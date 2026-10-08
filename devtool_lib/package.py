"""
The packages (`devtool package`): the Python wheels (PyPI fipc-python, one per platform, the native library in
fipc/_native/), the NuGet package (Fipc, every platform's native library in runtimes/{rid}/native/) and the
Maven artifact (io.github.fastipc:fipc, every platform's native library in the jar, with its sources, javadoc and
POM in a folder in the Maven repository layout), the Rust crates (fipc-sys with every platform's native library
in native/, and fipc), the Lua rock (fipc: a rockspec, and a source rock whose archive carries the module and
every platform's native library in native/<rid>/) and the npm package (fipc: the JavaScript binding, with every
platform's Node-API addon and native library in prebuilds/<platform-arch>/) and the Go module's tree (bindings/go
as its release tag holds it, with every platform's native library in fipc/native/<rid>/), built from the packaged
libraries of `zig build dist` or from libraries given by path, then checked; `smoke` installs a package the way a user
does (a fresh venv, a throwaway console project, a Gradle project that resolves the artifact from the folder, a Cargo
project that takes the crates from a vendored folder, LuaRocks into a fresh tree, npm into a fresh project, a Go
project that requires the module from a file proxy) and makes a round trip through it.

The release workflow (.github/workflows/release.yml) runs the same commands.
"""

import ast
import hashlib
import json
import os
import platform
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
import zipfile
from pathlib import Path
from typing import Dict, List, Optional

# rid -> (library file name, wheel platform tag): docs/platform-support.md's floors (glibc 2.34 on x86-64 and arm64,
# Windows x64, macOS 14.4 on arm64: a macOS wheel tag carries the major version only, so pip takes the wheel on
# 14.0-14.3 too)
PLATFORMS = {
    "linux-x64": ("libfastipc.so", "manylinux_2_34_x86_64"),
    "win-x64": ("fastipc.dll", "win_amd64"),
    "osx-arm64": ("libfastipc.dylib", "macosx_14_0_arm64"),
    "linux-arm64": ("libfastipc.so", "manylinux_2_34_aarch64"),
}
PYTHON_DIST = "fipc_python"  # the wheel's file-name form of the distribution fipc-python
NUGET_ID = "Fipc"
MAVEN_GROUP = "io.github.fastipc"
MAVEN_ARTIFACT = "fipc"
JAVA_MODULE = "io.github.fastipc"
# rid -> the jar's folder for its library (the binding's NativeLoader reads native/<platform>/<library>)
JAVA_PLATFORMS = {"linux-x64": "linux-x86_64", "win-x64": "windows-x86_64", "osx-arm64": "macos-aarch64",
                  "linux-arm64": "linux-aarch64"}
# The Rust crates, in publishing order: the raw FFI crate (with the libraries in native/<rid>/), then the safe API
RUST_SYS = "fipc-sys"
RUST_CRATE = "fipc"
RUST_CRATES = (RUST_SYS, RUST_CRATE)
CRATES_IO_LIMIT = 10 * 1024 * 1024
# The variables that point the loader at a library folder (Linux, macOS): a smoke run drops them, so that what it loads
# is the package's own library
LIBRARY_PATH_VARIABLES = ("LD_LIBRARY_PATH", "DYLD_LIBRARY_PATH", "DYLD_FALLBACK_LIBRARY_PATH")


def host_rid() -> str:
    if sys.platform == "win32":
        return "win-x64"
    if sys.platform == "darwin":
        return "osx-arm64"
    return "linux-arm64" if platform.machine().lower() in ("aarch64", "arm64") else "linux-x64"


def versions(repo: Path) -> Dict[str, str]:
    """The packages' versions: fipc.__version__ (the wheel's, via pyproject.toml), the csproj's <Version>, the
    Java binding's gradle.properties `version`, the Rust workspace's version (both crates'), the version of
    fipc-sys that fipc requires, the Lua module's _VERSION and the version of its one rockspec
    (bindings/lua/fipc-<version>-1.rockspec, whose `version` check_rock compares), and the JavaScript package's
    (bindings/js/package.json) and its module's VERSION (index.cjs)."""
    init = repo / "bindings" / "python" / "fipc" / "__init__.py"
    python = None
    for node in ast.parse(init.read_text(encoding="utf-8")).body:
        if isinstance(node, ast.Assign) and any(getattr(t, "id", None) == "__version__" for t in node.targets):
            python = ast.literal_eval(node.value)
    csproj = (repo / "bindings" / "csharp" / f"{NUGET_ID}.csproj").read_text(encoding="utf-8")
    match = re.search(r"<Version>([^<]+)</Version>", csproj)
    properties = (repo / "bindings" / "java" / "gradle.properties").read_text(encoding="utf-8")
    java = re.search(r"^version=(\S+)\s*$", properties, re.MULTILINE)
    workspace = (repo / "bindings" / "rust" / "Cargo.toml").read_text(encoding="utf-8")
    rust = re.search(r'^\[workspace\.package\][^\[]*?^version = "([^"]+)"', workspace, re.MULTILINE)
    wrapper = (repo / "bindings" / "rust" / RUST_CRATE / "Cargo.toml").read_text(encoding="utf-8")
    rust_sys = re.search(rf'^{RUST_SYS} = {{ version = "=?([^"]+)"', wrapper, re.MULTILINE)
    lua = repo / "bindings" / "lua"
    module = re.search(r'^M\._VERSION = "([^"]+)"', (lua / "fipc.lua").read_text(encoding="utf-8"),
                       re.MULTILINE)
    rockspecs = [p.name.removeprefix("fipc-").removesuffix("-1.rockspec") for p in lua.glob("*.rockspec")]
    js = repo / "bindings" / "js"
    js_package = json.loads((js / "package.json").read_text(encoding="utf-8")).get("version")
    js_module = re.search(r"^const VERSION = '([^']+)';", (js / "index.cjs").read_text(encoding="utf-8"), re.MULTILINE)
    return {"python": python, "csharp": match.group(1) if match else None, "java": java.group(1) if java else None,
            "rust": rust.group(1) if rust else None, "rust-sys-dependency": rust_sys.group(1) if rust_sys else None,
            "lua": module.group(1) if module else None,
            "lua-rockspec": rockspecs[0] if len(rockspecs) == 1 else None,
            "js": js_package, "js-module": js_module.group(1) if js_module else None}


def check_versions(repo: Path, expected: Optional[str]) -> str:
    """The one version every package carries (and `expected`'s, if given); exits otherwise."""
    found = versions(repo)
    wanted = {expected} if expected else {found["python"]}
    if set(found.values()) != wanted or None in found.values():
        print(f"❌ package versions {found}" + (f", expected {expected}" if expected else " differ"))
        sys.exit(1)
    version = found["python"]
    print(f"✅ version {version} (fipc.__version__, {NUGET_ID}.csproj, bindings/java/gradle.properties, "
          "bindings/rust/Cargo.toml, bindings/lua, bindings/js; the Go module's is its tag's)")
    return version


def _run(cmd: List[str], cwd: Optional[Path] = None, env: Optional[dict] = None) -> None:
    print(f"Running: {' '.join(str(c) for c in cmd)}", flush=True)
    subprocess.run([str(c) for c in cmd], cwd=cwd, env=env, check=True)


# ===== Python =====

def build_wheel(repo: Path, python: Path, rid: str, library: Path, out_dir: Path, work: Path) -> Path:
    """The wheel of platform `rid`: bindings/python staged in `work` with `library` in fipc/_native/ and the
    LICENSE, built by pip (setup.py tags it from the library)."""
    name, _ = PLATFORMS[rid]
    staging = work / f"python-{rid}"
    if staging.exists():
        shutil.rmtree(staging)
    shutil.copytree(
        repo / "bindings" / "python", staging,
        ignore=shutil.ignore_patterns("__pycache__", "*.pyc", "*.egg-info", "build", "dist", "*.so", "*.dll", "*.dylib"),
    )
    shutil.copy2(repo / "LICENSE", staging / "LICENSE")
    shutil.copy2(library, staging / "fipc" / "_native" / name)
    before = set(out_dir.glob("*.whl"))
    _run([python, "-m", "pip", "wheel", "--no-deps", "--disable-pip-version-check", "--wheel-dir", out_dir, staging])
    built = sorted(set(out_dir.glob("*.whl")) - before)
    if len(built) != 1:
        print(f"❌ expected one new wheel in {out_dir}, found {built}")
        sys.exit(1)
    return built[0]


def check_wheel(wheel: Path, rid: str, version: str, license_text: bytes) -> bool:
    """The wheel's name and tag, its native library, metadata and license."""
    name, platform_tag = PLATFORMS[rid]
    expected = f"{PYTHON_DIST}-{version}-py3-none-{platform_tag}.whl"
    problems = []
    if wheel.name != expected:
        problems.append(f"file name {wheel.name}, expected {expected}")
    info = f"{PYTHON_DIST}-{version}.dist-info"
    with zipfile.ZipFile(wheel) as z:
        names = set(z.namelist())
        for required in ("fipc/__init__.py", "fipc/_fipc.py", "fipc/py.typed",
                         f"fipc/_native/{name}", f"{info}/licenses/LICENSE"):
            if required not in names:
                problems.append(f"{required} missing")
        natives = [n for n in names if n.startswith("fipc/_native/")]
        if natives != [f"fipc/_native/{name}"]:
            problems.append(f"_native/ holds {natives}, expected only {name}")
        if f"{info}/licenses/LICENSE" in names and z.read(f"{info}/licenses/LICENSE") != license_text:
            problems.append("licenses/LICENSE differs from the repository's LICENSE")
        wheel_meta = z.read(f"{info}/WHEEL").decode() if f"{info}/WHEEL" in names else ""
        metadata = z.read(f"{info}/METADATA").decode() if f"{info}/METADATA" in names else ""
    for line in ("Root-Is-Purelib: false", f"Tag: py3-none-{platform_tag}"):
        if line not in wheel_meta.splitlines():
            problems.append(f"WHEEL lacks '{line}'")
    for line in ("Name: fipc-python", f"Version: {version}", "License-Expression: MIT", "Requires-Dist: cffi>=1.15.0",
                 "Description-Content-Type: text/markdown"):
        if line not in metadata.splitlines():
            problems.append(f"METADATA lacks '{line}'")
    for problem in problems:
        print(f"❌ {wheel.name}: {problem}")
    if not problems:
        print(f"✅ {wheel.name}: py3-none-{platform_tag}, fipc/_native/{name}, MIT, licenses/LICENSE")
    return not problems


PYTHON_SMOKE = r'''
# A server (this process) and a client (a second process, this script with the name as its argument)
import os, subprocess, sys
from pathlib import Path
import fipc
from fipc import Conn, Listener

if len(sys.argv) > 1:
    with Conn.connect(sys.argv[1], timeout_ms=5000) as conn:
        conn.send(b"ping", timeout_ms=5000)
        request_id = conn.rpc_submit(7, b"x" * 100_000, timeout_ms=5000)
        response = conn.rpc_recv(timeout_ms=5000)
        assert (response.id, response.payload) == (request_id, b"pong"), response
    sys.exit(0)

package = Path(fipc.__file__).resolve().parent
assert Path(sys.prefix).resolve() in package.parents, f"fipc imported from {package}, not the venv"
native = sorted(p.name for p in (package / "_native").iterdir())
name = f"fipc_smoke_py_{os.getpid()}"
with Listener(name, 1 << 16) as listener:
    client = subprocess.Popen([sys.executable, __file__, name])
    with listener.accept(timeout_ms=5000) as conn:
        assert conn.recv(timeout_ms=5000) == b"ping"
        request = conn.rpc_recv(timeout_ms=5000)
        assert (request.opcode, len(request.payload)) == (7, 100_000), request
        conn.rpc_respond(request.id, request.opcode, data=b"pong", timeout_ms=5000)
        assert client.wait(timeout=10) == 0, "the client failed"
print(f"round trip OK: fipc {fipc.__version__} from {package} (_native: {native})")
'''


def smoke_wheel(wheel: Path) -> None:
    """Installs `wheel` into a fresh venv in a temporary folder and makes a round trip through it there, outside the
    repository."""
    with tempfile.TemporaryDirectory(prefix="fipc-smoke-", ignore_cleanup_errors=True) as temp:
        work = Path(temp)
        venv = work / "venv"
        _run([sys.executable, "-m", "venv", venv])
        python = venv / ("Scripts/python.exe" if sys.platform == "win32" else "bin/python")
        _run([python, "-m", "pip", "install", "--disable-pip-version-check", "--quiet", wheel.resolve()])
        (work / "smoke.py").write_text(PYTHON_SMOKE, encoding="utf-8")
        env = {k: v for k, v in os.environ.items() if k != "PYTHONPATH" and k not in LIBRARY_PATH_VARIABLES}
        _run([python, "smoke.py"], cwd=work, env=env)
    print(f"✅ {wheel.name}: installed in a fresh venv, round trip OK")


# ===== NuGet =====

def build_nupkg(repo: Path, dotnet: str, libraries: Dict[str, Path], out_dir: Path, work: Path) -> Path:
    """The NuGet package with every platform's native library, staged in `work` in the runtimes/ layout (the csproj's
    FipcRuntimesDir), so the binding's own runtimes/ folder is left alone."""
    runtimes = work / "runtimes"
    if runtimes.exists():
        shutil.rmtree(runtimes)
    for rid, library in libraries.items():
        target = runtimes / rid / "native" / PLATFORMS[rid][0]
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(library, target)
    before = set(out_dir.glob("*.nupkg"))
    _run([dotnet, "pack", repo / "bindings" / "csharp" / f"{NUGET_ID}.csproj", "--configuration", "Release",
          "--output", out_dir, f"-p:FipcRuntimesDir={runtimes.resolve()}", "-nologo"])
    built = sorted(p for p in set(out_dir.glob("*.nupkg")) - before)
    if len(built) != 1:
        print(f"❌ expected one new .nupkg in {out_dir}, found {built}")
        sys.exit(1)
    return built[0]


def check_nupkg(nupkg: Path, version: str, libraries: Dict[str, Path]) -> bool:
    """The package's id, version, license expression, readme, assembly and every platform's native library (the ones
    given)."""
    problems = []
    expected = f"{NUGET_ID}.{version}.nupkg"
    if nupkg.name != expected:
        problems.append(f"file name {nupkg.name}, expected {expected}")
    with zipfile.ZipFile(nupkg) as z:
        names = set(z.namelist())
        for required in (f"{NUGET_ID}.nuspec", f"lib/netstandard2.1/{NUGET_ID}.dll", "README.md", "LICENSE",
                         *(f"runtimes/{rid}/native/{name}" for rid, (name, _) in PLATFORMS.items())):
            if required not in names:
                problems.append(f"{required} missing")
        for rid, library in libraries.items():
            packed = f"runtimes/{rid}/native/{PLATFORMS[rid][0]}"
            if packed in names and z.read(packed) != library.read_bytes():
                problems.append(f"{packed} differs from {library}")
        nuspec = z.read(f"{NUGET_ID}.nuspec").decode("utf-8-sig") if f"{NUGET_ID}.nuspec" in names else ""
    for pattern in (f"<id>{NUGET_ID}</id>", f"<version>{version}</version>", "<authors>Hayden Donnelly</authors>",
                    '<license type="expression">MIT</license>', "<readme>README.md</readme>",
                    '<repository type="git" url="https://github.com/fastipc/fastipc'):
        if pattern not in nuspec:
            problems.append(f"nuspec lacks {pattern}")
    for problem in problems:
        print(f"❌ {nupkg.name}: {problem}")
    if not problems:
        print(f"✅ {nupkg.name}: MIT, README.md, lib/netstandard2.1/{NUGET_ID}.dll, "
              + ", ".join(f"runtimes/{rid}/native/{name}" for rid, (name, _) in PLATFORMS.items()))
    return not problems


CSHARP_SMOKE_PROJECT = """<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <OutputType>Exe</OutputType>
    <TargetFramework>net9.0</TargetFramework>
    <Nullable>enable</Nullable>
  </PropertyGroup>
  <ItemGroup>
    <PackageReference Include="Fipc" Version="{version}" />
  </ItemGroup>
</Project>
"""

CSHARP_SMOKE_PROGRAM = """// A server (this process) and a client (a second process, this program with the name as its argument)
using System;
using System.Diagnostics;
using System.Text;
using FastIpc;

if (args.Length > 0)
{
    Check(FipcConnection.Connect(args[0], out FipcConnection? conn, 5000), "Connect");
    using (conn!)
    {
        Check(conn!.Send(Encoding.ASCII.GetBytes("ping"), 5000), "Send");
        Check(conn.RpcSubmit(7, new byte[100_000], out ulong id, 5000), "RpcSubmit");
        Check(conn.RpcReceive(out FipcRpcHeader response, out byte[] payload, 5000), "RpcReceive");
        if (response.Id != id || Encoding.ASCII.GetString(payload) != "pong") throw new Exception("bad response");
    }
    return;
}

string name = $"fipc_smoke_cs_{Environment.ProcessId}";
Check(FipcListener.Listen(name, 1 << 16, out FipcListener? listener), "Listen");
using (listener!)
{
    using Process client = Process.Start(Environment.ProcessPath!, name)!;
    Check(listener!.Accept(out FipcConnection? server, 5000), "Accept");
    using (server!)
    {
        Check(server!.Receive(out byte[] message, 5000), "Receive");
        if (Encoding.ASCII.GetString(message) != "ping") throw new Exception("bad message");
        Check(server.RpcReceive(out FipcRpcHeader request, out byte[] body, 5000), "RpcReceive");
        if (request.Opcode != 7 || body.Length != 100_000) throw new Exception("bad request");
        Check(server.RpcRespond(request.Id, request.Opcode, 0, Encoding.ASCII.GetBytes("pong"), 5000), "RpcRespond");
        if (!client.WaitForExit(10_000) || client.ExitCode != 0) throw new Exception("the client failed");
    }
}
Console.WriteLine($"round trip OK: {typeof(Fipc).Assembly.Location}");

static void Check(FipcResult result, string call)
{
    if (result != FipcResult.Ok) throw new Exception($"{call}: {result}");
}
"""


def smoke_nupkg(nupkg: Path, version: str, dotnet: str) -> None:
    """A throwaway console project in a temporary folder that references the package from its folder (with a NuGet
    cache of its own, so no earlier build of the same version is used) and makes a round trip through it on this
    OS."""
    with tempfile.TemporaryDirectory(prefix="fipc-smoke-", ignore_cleanup_errors=True) as temp:
        _smoke_nupkg(nupkg, version, dotnet, Path(temp))
    print(f"✅ {nupkg.name}: restored by a console project from {nupkg.parent}, round trip OK")


def _smoke_nupkg(nupkg: Path, version: str, dotnet: str, project: Path) -> None:
    (project / "Smoke.csproj").write_text(CSHARP_SMOKE_PROJECT.format(version=version), encoding="utf-8")
    (project / "Program.cs").write_text(CSHARP_SMOKE_PROGRAM, encoding="utf-8")
    (project / "nuget.config").write_text(
        '<?xml version="1.0" encoding="utf-8"?>\n<configuration>\n  <packageSources>\n    <clear />\n'
        f'    <add key="local" value="{nupkg.parent.resolve()}" />\n'
        '  </packageSources>\n</configuration>\n', encoding="utf-8")
    env = {k: v for k, v in os.environ.items() if k not in LIBRARY_PATH_VARIABLES}
    env["NUGET_PACKAGES"] = str((project / "packages").resolve())
    _run([dotnet, "run", "--configuration", "Release", "--"], cwd=project, env=env)


# ===== Maven (Java) =====

def gradlew(repo: Path) -> Path:
    """The Java binding's Gradle wrapper. It runs on a JDK 17+ (JAVA_HOME, or java on PATH); the build's JDK 25 is a
    toolchain, which the foojay resolver downloads when none is installed."""
    return repo / "bindings" / "java" / ("gradlew.bat" if sys.platform == "win32" else "gradlew")


def maven_folder(repository: Path, version: str) -> Path:
    """The artifact's folder in a Maven repository folder."""
    return repository.joinpath(*MAVEN_GROUP.split("."), MAVEN_ARTIFACT, version)


def build_maven(repo: Path, libraries: Dict[str, Path], out_dir: Path, work: Path, version: str) -> Path:
    """The jar (with every platform's native library), its sources and javadoc jars, its POM and Gradle module metadata,
    with checksums, published by Gradle into out_dir/maven, a folder in the Maven repository layout (what a Maven
    Central upload bundle holds). Signed when Gradle is given a key: the properties signingKey and signingPassword, as
    the environment variables ORG_GRADLE_PROJECT_signingKey and ORG_GRADLE_PROJECT_signingPassword. Returns the
    folder."""
    natives = work / "java-natives"
    if natives.exists():
        shutil.rmtree(natives)
    for rid, library in libraries.items():
        target = natives / JAVA_PLATFORMS[rid] / PLATFORMS[rid][0]
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(library, target)
    staging = out_dir / "maven"
    _run([gradlew(repo), "-p", repo / "bindings" / "java", "--no-daemon", "--console=plain", "--quiet",
          f"-PfipcNativeDir={natives.resolve()}", f"-PfipcStagingRepo={staging.resolve()}",
          "clean", "publishMavenPublicationToStagingRepository"])
    jar = maven_folder(staging, version) / f"{MAVEN_ARTIFACT}-{version}.jar"
    if not jar.exists():
        print(f"❌ Gradle published no {jar}")
        sys.exit(1)
    return staging


def check_maven(staging: Path, version: str, libraries: Dict[str, Path], license_text: bytes) -> bool:
    """The artifact's files and checksums; the jar's classes, module descriptor, every platform's native library (the
    ones given), license and manifest; the sources and javadoc jars; the POM's metadata (Maven Central's
    requirements)."""
    folder = maven_folder(staging, version)
    base = f"{MAVEN_ARTIFACT}-{version}"
    package_dir = JAVA_MODULE.replace(".", "/")
    problems = []
    for suffix in (".jar", "-sources.jar", "-javadoc.jar", ".pom", ".module"):
        name = base + suffix
        if not (folder / name).exists():
            problems.append(f"{name} missing")
            continue
        problems += [f"{name}{c} missing" for c in (".sha1", ".md5") if not (folder / (name + c)).exists()]
    jar = folder / f"{base}.jar"
    if jar.exists():
        with zipfile.ZipFile(jar) as z:
            names = set(z.namelist())
            classes = ("Connection", "Listener", "FipcException", "Result", "RpcMessage", "RpcKind", "Fipc")
            for required in ("module-info.class", "META-INF/MANIFEST.MF", "META-INF/LICENSE",
                             *(f"{package_dir}/{c}.class" for c in classes)):
                if required not in names:
                    problems.append(f"{jar.name}: {required} missing")
            natives = sorted(n for n in names if n.startswith(f"{package_dir}/native/") and not n.endswith("/"))
            expected = sorted(f"{package_dir}/native/{JAVA_PLATFORMS[rid]}/{name}"
                              for rid, (name, _) in PLATFORMS.items())
            if natives != expected:
                problems.append(f"{jar.name}: native libraries {natives}, expected {expected}")
            for rid, library in libraries.items():
                packed = f"{package_dir}/native/{JAVA_PLATFORMS[rid]}/{PLATFORMS[rid][0]}"
                if packed in names and z.read(packed) != library.read_bytes():
                    problems.append(f"{jar.name}: {packed} differs from {library}")
            if "META-INF/LICENSE" in names and z.read("META-INF/LICENSE") != license_text:
                problems.append(f"{jar.name}: META-INF/LICENSE differs from the repository's LICENSE")
            manifest = z.read("META-INF/MANIFEST.MF").decode() if "META-INF/MANIFEST.MF" in names else ""
            if f"Implementation-Version: {version}" not in manifest.splitlines():
                problems.append(f"{jar.name}: the manifest lacks Implementation-Version: {version}")
    for suffix, required in (("-sources.jar", ("module-info.java", f"{package_dir}/Connection.java")),
                             ("-javadoc.jar", ("index.html",))):
        path = folder / (base + suffix)
        if path.exists():
            with zipfile.ZipFile(path) as z:
                names = set(z.namelist())
                problems += [f"{path.name}: {r} missing" for r in required if r not in names]
    pom_path = folder / f"{base}.pom"
    pom = pom_path.read_text(encoding="utf-8") if pom_path.exists() else ""
    for pattern in (f"<groupId>{MAVEN_GROUP}</groupId>", f"<artifactId>{MAVEN_ARTIFACT}</artifactId>",
                    f"<version>{version}</version>", f"<name>{MAVEN_ARTIFACT}</name>", "<description>",
                    "<url>https://fastipc.github.io/fastipc/</url>", "<name>MIT</name>",
                    "<url>https://opensource.org/license/mit</url>", "<name>Hayden Donnelly</name>", "<email>",
                    "<organizationUrl>https://github.com/fastipc</organizationUrl>",
                    "<url>https://github.com/fastipc/fastipc</url>",
                    "<connection>scm:git:https://github.com/fastipc/fastipc.git</connection>"):
        if pattern not in pom:
            problems.append(f"{base}.pom lacks {pattern}")
    if "<dependencies>" in pom:
        problems.append(f"{base}.pom has dependencies; the artifact has none")
    for problem in problems:
        print(f"❌ {MAVEN_GROUP}:{MAVEN_ARTIFACT}:{version}: {problem}")
    if not problems:
        signed = "signed" if (folder / f"{base}.jar.asc").exists() else "unsigned"
        print(f"✅ {MAVEN_GROUP}:{MAVEN_ARTIFACT}:{version}: jar (module {JAVA_MODULE}, "
              + ", ".join(f"native/{JAVA_PLATFORMS[rid]}/{name}" for rid, (name, _) in PLATFORMS.items())
              + f", META-INF/LICENSE), sources, javadoc, POM (MIT), checksums, {signed}")
    return not problems


JAVA_SMOKE_SETTINGS = """plugins {
    id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
}
rootProject.name = "fipc-smoke"
"""

JAVA_SMOKE_BUILD = """plugins {
    application
}

repositories {
    maven { url = uri("@REPOSITORY@") }
}

dependencies {
    implementation("@COORDINATES@")
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(25)
    }
}

application {
    mainClass = "Smoke"
    applicationDefaultJvmArgs = listOf("--enable-native-access=ALL-UNNAMED")
}
"""

JAVA_SMOKE_PROGRAM = """// A server (this process, the artifact on the class path) and a client (a second JVM, the artifact on the module
// path as the module io.github.fastipc), each with the jar's library
import io.github.fastipc.Connection;
import io.github.fastipc.Fipc;
import io.github.fastipc.Listener;
import io.github.fastipc.RpcMessage;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Smoke {
    static final Duration TIMEOUT = Duration.ofSeconds(10);
    static final String MODULE = "io.github.fastipc";

    public static void main(String[] args) throws Exception {
        String library = Fipc.libraryPath();
        if (library.contains("zig-out") || !library.contains("fipc")) {
            throw new AssertionError("not the jar's library, extracted into the cache: " + library);
        }
        if (args.length > 0) {
            client(args[0]);
            return;
        }
        if (Listener.class.getModule().isNamed()) {
            throw new AssertionError("the server should have the artifact on the class path");
        }
        Path jar = Path.of(Listener.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        List<String> classPath = new ArrayList<>();
        for (String entry : System.getProperty("java.class.path").split(File.pathSeparator)) {
            if (!Path.of(entry).toAbsolutePath().equals(jar.toAbsolutePath())) {
                classPath.add(entry);
            }
        }
        String name = "fipc_smoke_java_" + ProcessHandle.current().pid();
        Path output = Files.createTempFile("fipc-smoke-client", ".txt");
        try (Listener listener = Listener.listen(name, 1 << 16)) {
            Process client = new ProcessBuilder(ProcessHandle.current().info().command().orElseThrow(),
                    "--enable-native-access=" + MODULE, "--module-path", jar.toString(), "--add-modules", MODULE,
                    "-cp", String.join(File.pathSeparator, classPath), "Smoke", name)
                .redirectErrorStream(true).redirectOutput(output.toFile()).start();
            try (Connection conn = listener.accept(TIMEOUT)) {
                String ping = new String(conn.receive(TIMEOUT), StandardCharsets.US_ASCII);
                if (!ping.equals("ping")) {
                    throw new AssertionError("bad message " + ping);
                }
                RpcMessage request = conn.rpcReceive(TIMEOUT);
                if (request.opcode() != 7 || request.payload().length != 100_000) {
                    throw new AssertionError("bad request " + request);
                }
                conn.rpcRespond(request.id(), request.opcode(), 0, "pong".getBytes(StandardCharsets.US_ASCII), TIMEOUT);
            }
            boolean exited = client.waitFor(20, TimeUnit.SECONDS);
            String text = Files.readString(output);
            System.out.print(text);
            if (!exited || client.exitValue() != 0) {
                throw new AssertionError("the client failed");
            }
            if (text.contains("WARNING")) {
                throw new AssertionError("the client warned (native access?)");
            }
        } finally {
            Files.deleteIfExists(output);
        }
        System.out.println("round trip OK: " + jar + " (library " + library + ")");
    }

    static void client(String name) {
        Module module = Listener.class.getModule();
        if (!module.isNamed()) {
            throw new AssertionError("the client should have the artifact on the module path");
        }
        try (Connection conn = Connection.connect(name, TIMEOUT)) {
            conn.send("ping".getBytes(StandardCharsets.US_ASCII), TIMEOUT);
            long id = conn.rpcSubmit(7, new byte[100_000], TIMEOUT);
            RpcMessage response = conn.rpcReceive(TIMEOUT);
            if (response.id() != id || !new String(response.payload(), StandardCharsets.US_ASCII).equals("pong")) {
                throw new AssertionError("bad response " + response);
            }
        }
        System.out.println("client OK: module " + module.getName() + " "
            + module.getDescriptor().rawVersion().orElse("without a version"));
    }
}
"""


def smoke_maven(repo: Path, staging: Path, version: str) -> None:
    """A throwaway Gradle project in a temporary folder that resolves the artifact from the Maven repository folder
    `staging` (through its POM) and makes a round trip between two JVMs on this OS: the server with the artifact on
    the class path, the client with it on the module path. Both must load the jar's library (extracted into the
    user's cache), and neither may warn about native access."""
    coordinates = f"{MAVEN_GROUP}:{MAVEN_ARTIFACT}:{version}"
    with tempfile.TemporaryDirectory(prefix="fipc-smoke-", ignore_cleanup_errors=True) as temp:
        project = Path(temp)
        (project / "settings.gradle.kts").write_text(JAVA_SMOKE_SETTINGS, encoding="utf-8")
        (project / "build.gradle.kts").write_text(
            JAVA_SMOKE_BUILD.replace("@REPOSITORY@", staging.resolve().as_uri()).replace("@COORDINATES@", coordinates),
            encoding="utf-8")
        source = project / "src" / "main" / "java"
        source.mkdir(parents=True)
        (source / "Smoke.java").write_text(JAVA_SMOKE_PROGRAM, encoding="utf-8")
        env = {k: v for k, v in os.environ.items() if k not in LIBRARY_PATH_VARIABLES}
        cmd = [str(gradlew(repo)), "-p", str(project), "--no-daemon", "--console=plain", "--quiet", "run"]
        print(f"Running: {' '.join(cmd)}", flush=True)
        result = subprocess.run(cmd, env=env, capture_output=True, text=True)
        print(result.stdout, end="")
        print(result.stderr, end="", file=sys.stderr, flush=True)
        if result.returncode != 0 or "round trip OK" not in result.stdout:
            print(f"❌ {coordinates}: the smoke project failed")
            sys.exit(1)
        if "WARNING" in result.stdout + result.stderr:
            print(f"❌ {coordinates}: the smoke project printed a warning")
            sys.exit(1)
    print(f"✅ {coordinates}: resolved by a Gradle project from {staging}, round trip OK (class path and module path)")


def maven_repository_of(path: Path) -> Optional[Path]:
    """The Maven repository folder `path` names: the folder itself (when the artifact is in it), or the one a jar of
    the artifact sits in (<repository>/io/github/fastipc/fipc/<version>/fipc-<version>.jar)."""
    path = path.resolve()
    if path.is_dir():
        return path if path.joinpath(*MAVEN_GROUP.split("."), MAVEN_ARTIFACT).is_dir() else None
    if path.suffix == ".jar" and len(path.parents) > 5 and path.parents[1].name == MAVEN_ARTIFACT:
        return path.parents[5]
    return None


def maven_versions(repository: Path) -> List[str]:
    """The versions of the artifact in a Maven repository folder."""
    folder = repository.joinpath(*MAVEN_GROUP.split("."), MAVEN_ARTIFACT)
    return sorted(p.name for p in folder.iterdir() if p.is_dir())


# ===== Rust (crates) =====

def cargo_env() -> dict:
    """The environment for cargo: the current stable toolchain (the workspace pins it with rust-toolchain.toml; a
    project outside it would get rustup's default), and no FASTIPC_LIB_DIR or LIBRARY_PATH_VARIABLES, so that the
    crates' own libraries are what links and loads."""
    env = {k: v for k, v in os.environ.items() if k != "FASTIPC_LIB_DIR" and k not in LIBRARY_PATH_VARIABLES}
    env.setdefault("RUSTUP_TOOLCHAIN", "stable")
    return env


def rust_library_path(rid: str) -> str:
    """Where fipc-sys carries the library of platform `rid` (its build script reads native/<rid>/<library>)."""
    return f"native/{rid}/{PLATFORMS[rid][0]}"


def build_crates(repo: Path, libraries: Dict[str, Path], out_dir: Path) -> List[Path]:
    """The two crates, packaged by `cargo package --workspace` from a copy of bindings/rust in out_dir/rust, with the
    LICENSE in each crate and every platform's library in fipc-sys/native/<rid>/; the .crate files are copied
    into out_dir. The copy stays: `cargo publish --workspace` runs there (the release workflow). Cargo verifies each
    crate by building it as a user would, fipc against the fipc-sys just packaged."""
    staging = out_dir / "rust"
    shutil.copytree(repo / "bindings" / "rust", staging, ignore=shutil.ignore_patterns("target", "native"))
    for crate in RUST_CRATES:
        shutil.copy2(repo / "LICENSE", staging / crate / "LICENSE")
    for rid, library in libraries.items():
        target = staging / RUST_SYS / rust_library_path(rid)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(library, target)
    target_dir = out_dir / "work" / "rust-target"
    _run(["cargo", "package", "--workspace", "--manifest-path", staging / "Cargo.toml", "--target-dir", target_dir],
         env=cargo_env())
    crates = []
    for crate in RUST_CRATES:
        built = sorted((target_dir / "package").glob(f"{crate}-[0-9]*.crate"))
        if len(built) != 1:
            print(f"❌ expected one {crate} crate in {target_dir / 'package'}, found {built}")
            sys.exit(1)
        crates.append(Path(shutil.copy2(built[0], out_dir / built[0].name)))
    return crates


def _crate_files(crate: Path) -> Dict[str, bytes]:
    """A .crate's files (a gzipped tar of <name>-<version>/...), by their path inside the crate."""
    with tarfile.open(crate, "r:gz") as tar:
        return {m.name.split("/", 1)[1]: tar.extractfile(m).read() for m in tar.getmembers() if m.isfile()}


def check_crates(crates: List[Path], version: str, libraries: Dict[str, Path], license_text: bytes) -> bool:
    """Each crate's name, version, size (crates.io's limit) and files, nothing more; the manifest's metadata (MIT,
    the author, the repository, the homepage, the README, the links key); fipc-sys's two libraries (the ones
    given) and fipc's dependency on that version of it."""
    problems = []
    expected_files = {
        RUST_SYS: {"Cargo.toml", "Cargo.toml.orig", "Cargo.lock", "LICENSE", "README.md", "build.rs", "src/lib.rs",
                   *(rust_library_path(rid) for rid in PLATFORMS)},
        RUST_CRATE: {"Cargo.toml", "Cargo.toml.orig", "Cargo.lock", "LICENSE", "README.md", "src/lib.rs",
                     "src/connection.rs", "src/error.rs", "src/listener.rs", "src/rpc.rs"},
    }
    for crate, name in zip(crates, RUST_CRATES):
        expected = f"{name}-{version}.crate"
        if crate.name != expected:
            problems.append(f"file name {crate.name}, expected {expected}")
        if crate.stat().st_size > CRATES_IO_LIMIT:
            problems.append(f"{crate.name}: {crate.stat().st_size} bytes, over crates.io's limit of 10 MiB")
        files = _crate_files(crate)
        names = {n for n in files if n != ".cargo_vcs_info.json"}
        extra = sorted(n for n in names - expected_files[name]
                       if not (name == RUST_CRATE and re.fullmatch(r"examples/\w+\.rs", n)))
        missing = sorted(expected_files[name] - names)
        problems += [f"{crate.name}: {n} missing" for n in missing] + [f"{crate.name}: unexpected {n}" for n in extra]
        if files.get("LICENSE", license_text) != license_text:
            problems.append(f"{crate.name}: LICENSE differs from the repository's LICENSE")
        manifest = files.get("Cargo.toml", b"").decode()
        for line in (f'name = "{name}"', f'version = "{version}"', 'license = "MIT"', 'authors = ["Hayden Donnelly"]',
                     'repository = "https://github.com/fastipc/fastipc"', 'readme = "README.md"',
                     'homepage = "https://fastipc.github.io/fastipc/"', 'edition = "2024"'):
            if line not in manifest.splitlines():
                problems.append(f"{crate.name}: Cargo.toml lacks '{line}'")
        if name == RUST_SYS:
            if 'links = "fastipc"' not in manifest.splitlines():
                problems.append(f"{crate.name}: Cargo.toml lacks 'links = \"fastipc\"'")
            for rid, library in libraries.items():
                if files.get(rust_library_path(rid)) not in (None, library.read_bytes()):
                    problems.append(f"{crate.name}: {rust_library_path(rid)} differs from {library}")
        else:
            dependency = f'[dependencies.{RUST_SYS}]\nversion = "{version}"'
            if dependency not in manifest:
                problems.append(f"{crate.name}: Cargo.toml lacks the dependency {RUST_SYS} {version}")
    for problem in problems:
        print(f"❌ {problem}")
    if not problems:
        sizes = ", ".join(f"{c.name} ({c.stat().st_size / 1024:.0f} KB)" for c in crates)
        print(f"✅ {sizes}: MIT, LICENSE, README.md, "
              + ", ".join(rust_library_path(rid) for rid in PLATFORMS) + f", {RUST_CRATE} -> {RUST_SYS} {version}")
    return not problems


RUST_SMOKE_MANIFEST = """[package]
name = "fipc-smoke"
version = "0.1.0"
edition = "2024"
publish = false

[dependencies]
fipc = "=@VERSION@"
"""

RUST_SMOKE_CONFIG = """[source.crates-io]
replace-with = "vendored"

[source.vendored]
directory = "@VENDOR@"

# As the README says for shipping a program on Linux and macOS: find the library next to the executable
[target.x86_64-unknown-linux-gnu]
rustflags = ["-C", "link-arg=-Wl,-rpath,$ORIGIN"]

[target.aarch64-apple-darwin]
rustflags = ["-C", "link-arg=-Wl,-rpath,@loader_path"]

[target.aarch64-unknown-linux-gnu]
rustflags = ["-C", "link-arg=-Wl,-rpath,$ORIGIN"]
"""

RUST_SMOKE_PROGRAM = """// A server (this process) and a client (a second process, this program with the name as its argument)
use fipc::{Connection, Listener};
use std::process::Command;
use std::time::Duration;

const TIMEOUT: Duration = Duration::from_secs(10);

fn main() -> fipc::Result<()> {
    if let Some(name) = std::env::args().nth(1) {
        let mut conn = Connection::connect(&name, TIMEOUT)?;
        conn.send(b"ping", TIMEOUT)?;
        let id = conn.rpc_submit(7, &[0; 100_000], TIMEOUT)?;
        let response = conn.rpc_receive(TIMEOUT)?;
        assert_eq!((response.id, &response.payload[..]), (id, &b"pong"[..]));
        return Ok(());
    }
    let name = format!("fipc_smoke_rs_{}", std::process::id());
    let mut listener = Listener::listen(&name, 1 << 16)?;
    let mut client = Command::new(std::env::current_exe().unwrap()).arg(&name).spawn().unwrap();
    let mut conn = listener.accept(TIMEOUT)?;
    assert_eq!(conn.receive(TIMEOUT)?, b"ping");
    let request = conn.rpc_receive(TIMEOUT)?;
    assert_eq!((request.opcode, request.payload.len()), (7, 100_000));
    conn.rpc_respond(request.id, request.opcode, 0, b"pong", TIMEOUT)?;
    assert!(client.wait().unwrap().success(), "the client failed");
    println!("round trip OK: {}", std::env::current_exe().unwrap().display());
    Ok(())
}
"""


def _vendor(crate: Path, vendor: Path) -> None:
    """`crate` unpacked into the directory source `vendor`, as `cargo vendor` lays it out."""
    with tarfile.open(crate, "r:gz") as tar:
        try:
            tar.extractall(vendor, filter="data")
        except TypeError:  # a Python without extraction filters
            tar.extractall(vendor)
    folder = vendor / crate.name.removesuffix(".crate")
    checksum = hashlib.sha256(crate.read_bytes()).hexdigest()
    (folder / ".cargo-checksum.json").write_text(json.dumps({"files": {}, "package": checksum}), encoding="utf-8")


def smoke_crates(crates: List[Path], version: str, library: Optional[Path]) -> None:
    """A throwaway Cargo project in a temporary folder that depends on fipc, the crates served by a vendored
    directory source (offline, as crates.io would serve them, checksums included), and makes a round trip between two
    processes on this OS: once through `cargo run`, then shipped as the README says (the executable and the library
    side by side in a folder of their own, on Linux with an rpath of $ORIGIN, on macOS of @loader_path) and run there
    directly. The library it
    linked must be the crate's copy (`library`'s bytes, when given)."""
    rid = host_rid()
    name = PLATFORMS[rid][0]
    with tempfile.TemporaryDirectory(prefix="fipc-smoke-", ignore_cleanup_errors=True) as temp:
        work = Path(temp)
        vendor = work / "vendor"
        for crate in crates:
            _vendor(crate, vendor)
        project = work / "project"
        (project / "src").mkdir(parents=True)
        (project / ".cargo").mkdir()
        (project / "Cargo.toml").write_text(RUST_SMOKE_MANIFEST.replace("@VERSION@", version), encoding="utf-8")
        (project / ".cargo" / "config.toml").write_text(
            RUST_SMOKE_CONFIG.replace("@VENDOR@", vendor.resolve().as_posix()), encoding="utf-8")
        (project / "src" / "main.rs").write_text(RUST_SMOKE_PROGRAM, encoding="utf-8")
        _run(["cargo", "run", "--release", "--offline", "--quiet"], cwd=project, env=cargo_env())

        built = sorted((project / "target" / "release" / "build").glob(f"{RUST_SYS}-*/out/{name}"))
        if len(built) != 1:
            print(f"❌ {RUST_SYS}: expected one {name} in its build folder, found {built}")
            sys.exit(1)
        if library is not None and built[0].read_bytes() != library.read_bytes():
            print(f"❌ {RUST_SYS}: the library it linked is not the crate's ({library})")
            sys.exit(1)
        shipped = work / "shipped"
        shipped.mkdir()
        exe = "fipc-smoke.exe" if rid == "win-x64" else "fipc-smoke"
        shutil.copy2(project / "target" / "release" / exe, shipped / exe)
        shutil.copy2(built[0], shipped / name)
        _run([shipped / exe], cwd=work, env={k: v for k, v in os.environ.items() if k not in LIBRARY_PATH_VARIABLES})
    print(f"✅ {', '.join(c.name for c in crates)}: built by a Cargo project from a vendored source, round trip OK "
          "(cargo run, and shipped next to its library)")


def crates_of(path: Path) -> Optional[List[Path]]:
    """The two crates a .crate file of either names: it and its sibling of the same version in its folder; None for
    another file."""
    for crate in RUST_CRATES:
        version = path.name.removeprefix(f"{crate}-").removesuffix(".crate")
        if path.name.startswith(f"{crate}-") and path.suffix == ".crate" and re.fullmatch(r"\d+\.\d+\.\d+\S*", version):
            crates = [path.parent / f"{c}-{version}.crate" for c in RUST_CRATES]
            missing = [str(c) for c in crates if not c.exists()]
            if missing:
                print(f"❌ the two crates go together; {', '.join(missing)} missing")
                sys.exit(1)
            return crates
    return None


# ===== Lua (the rock) =====

LUA_ROCK = "fipc"
LUA_MODULE = "fipc"
LUA_REVISION = "1"  # the rockspec's revision: a new rockspec of the same version (packaging only) raises it
# rid -> where the rock's archive carries the library of that platform (the rockspec installs it from there)
LUA_NATIVE = {rid: f"native/{rid}/{name}" for rid, (name, _) in PLATFORMS.items()}
# rid -> the folder in the tree's C module folder where the rockspec installs its library: LuaRocks picks an install
# by OS only, so on Linux it installs both Linux libraries, ARM64's in a subfolder (the module picks by ffi.arch)
LUA_INSTALLED_FOLDER = {rid: (LUA_MODULE, "arm64") if rid == "linux-arm64" else (LUA_MODULE,) for rid in PLATFORMS}


def lua_names(version: str) -> Dict[str, str]:
    """The Lua package's file names: the rockspec, the source rock (the rockspec and the archive, what luarocks.org
    serves) and the archive itself (a GitHub release asset, the rockspec's source.url), and the archive's folder."""
    folder = f"{LUA_ROCK}-lua-{version}"
    return {"rockspec": f"{LUA_ROCK}-{version}-{LUA_REVISION}.rockspec",
            "src_rock": f"{LUA_ROCK}-{version}-{LUA_REVISION}.src.rock",
            "archive": f"{folder}.tar.gz", "folder": folder}


def _user_tool(env_var: str, name: str, defaults: List[Path]) -> Optional[str]:
    """A tool: the path `env_var` names, else `name` on PATH, else the first of `defaults` that exists."""
    given = os.environ.get(env_var)
    if given:
        return given
    found = shutil.which(name)
    if found:
        return found
    return next((str(p) for p in defaults if p.exists()), None)


def find_luajit() -> Optional[str]:
    """LuaJIT: the LUAJIT environment variable, luajit on PATH, or the user-local build (~/luajit/bin); None if none
    is there."""
    exe = "luajit.exe" if sys.platform == "win32" else "luajit"
    return _user_tool("LUAJIT", "luajit", [Path.home() / "luajit" / "bin" / exe])


def luajit() -> str:
    """LuaJIT (find_luajit); exits if there is none."""
    found = find_luajit()
    if not found:
        print("❌ LuaJIT not found: set LUAJIT, put luajit on PATH, or install it in ~/luajit (bin/luajit)")
        sys.exit(1)
    return found


def luarocks() -> str:
    """LuaRocks: the LUAROCKS environment variable, luarocks on PATH, or the user-local install (~/luarocks: bin/
    on Linux, the standalone luarocks.exe on Windows)."""
    home = Path.home() / "luarocks"
    found = _user_tool("LUAROCKS", "luarocks", [home / "luarocks.exe", home / "bin" / "luarocks"])
    if not found:
        print("❌ LuaRocks not found: set LUAROCKS, put luarocks on PATH, or install it in ~/luarocks")
        sys.exit(1)
    return found


def build_rock(repo: Path, libraries: Dict[str, Path], out_dir: Path, work: Path, version: str) -> Dict[str, Path]:
    """The rock: bindings/lua's module, README and examples, the LICENSE and every platform's library (native/<rid>/) in
    the archive fipc-lua-<version>.tar.gz; the rockspec next to it; and the source rock (a zip of the two, as
    `luarocks pack` makes it), all in out_dir."""
    names = lua_names(version)
    binding = repo / "bindings" / "lua"
    staging = work / names["folder"]
    if staging.exists():
        shutil.rmtree(staging)
    (staging / "examples").mkdir(parents=True)
    shutil.copy2(binding / f"{LUA_MODULE}.lua", staging / f"{LUA_MODULE}.lua")
    shutil.copy2(binding / "README.md", staging / "README.md")
    shutil.copy2(repo / "LICENSE", staging / "LICENSE")
    for example in sorted((binding / "examples").glob("*.lua")):
        shutil.copy2(example, staging / "examples" / example.name)
    for rid, library in libraries.items():
        target = staging / LUA_NATIVE[rid]
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(library, target)
    archive = out_dir / names["archive"]
    with tarfile.open(archive, "w:gz") as tar:
        for path in [staging, *sorted(staging.rglob("*"))]:
            arcname = "/".join([names["folder"], *path.relative_to(staging).parts])
            info = tar.gettarinfo(path, arcname)
            info.uid = info.gid = 0
            info.uname = info.gname = ""
            info.mode = 0o755 if path.is_dir() or path.suffix in (".so", ".dll", ".dylib") else 0o644
            if path.is_file():
                with open(path, "rb") as f:
                    tar.addfile(info, f)
            else:
                tar.addfile(info)
    rockspec = Path(shutil.copy2(binding / names["rockspec"], out_dir / names["rockspec"]))
    src_rock = out_dir / names["src_rock"]
    with zipfile.ZipFile(src_rock, "w", zipfile.ZIP_DEFLATED) as rock:
        rock.write(rockspec, rockspec.name)
        rock.write(archive, archive.name)
    return {"rockspec": rockspec, "src_rock": src_rock, "archive": archive}


def _rockspec_fields(text: str) -> Dict[str, str]:
    """A rockspec's simple string fields (key = "value"), wherever they are."""
    return dict(re.findall(r'^\s*(\w+)\s*=\s*"([^"]*)"', text, re.MULTILINE))


def check_rock(files: Dict[str, Path], version: str, libraries: Dict[str, Path], license_text: bytes) -> bool:
    """The rockspec's package, version, source (the archive's name and folder, on the tag's GitHub release), license,
    homepage, dependencies and what it installs; the archive's files, nothing more (the module of this version, every
    platform's library as given, the LICENSE, README and examples); the source rock's two files, the same bytes."""
    names = lua_names(version)
    problems = []
    spec_name = files["rockspec"].name
    rockspec_text = files["rockspec"].read_text(encoding="utf-8")
    fields = _rockspec_fields(rockspec_text)
    url = f"https://github.com/fastipc/fastipc/releases/download/v{version}/{names['archive']}"
    expected = {"rockspec_format": "3.0", "package": LUA_ROCK, "version": f"{version}-{LUA_REVISION}", "url": url,
                "dir": names["folder"], "license": "MIT", "maintainer": "Hayden Donnelly",
                "homepage": "https://fastipc.github.io/fastipc/", "type": "builtin",
                LUA_MODULE: f"{LUA_MODULE}.lua"}
    for key, value in expected.items():
        if fields.get(key) != value:
            problems.append(f"{spec_name}: {key} = {fields.get(key)!r}, expected {value!r}")
    for dependency in ('"lua == 5.1"', '"luajit >= 2.1"'):
        if dependency not in rockspec_text:
            problems.append(f"{spec_name}: no dependency {dependency}")
    for path in LUA_NATIVE.values():
        if f'"{path}"' not in rockspec_text:
            problems.append(f"{spec_name}: doesn't install {path}")

    with tarfile.open(files["archive"], "r:gz") as tar:
        members = [m for m in tar.getmembers() if m.isfile()]
        contents = {m.name.split("/", 1)[1]: tar.extractfile(m).read() for m in members}
        folders = {m.name.split("/", 1)[0] for m in members}
    if folders != {names["folder"]}:
        problems.append(f"{files['archive'].name}: top folder {sorted(folders)}, expected {names['folder']}")
    expected_files = {f"{LUA_MODULE}.lua", "README.md", "LICENSE", *LUA_NATIVE.values()}
    found = {n for n in contents if not re.fullmatch(r"examples/\w+\.lua", n)}
    problems += [f"{files['archive'].name}: {n} missing" for n in sorted(expected_files - found)]
    problems += [f"{files['archive'].name}: unexpected {n}" for n in sorted(found - expected_files)]
    if not any(n.startswith("examples/") for n in contents):
        problems.append(f"{files['archive'].name}: no examples")
    if contents.get("LICENSE", license_text) != license_text:
        problems.append(f"{files['archive'].name}: LICENSE differs from the repository's LICENSE")
    if f'M._VERSION = "{version}"' not in contents.get(f"{LUA_MODULE}.lua", b"").decode():
        problems.append(f"{files['archive'].name}: {LUA_MODULE}.lua's _VERSION isn't {version}")
    for rid, library in libraries.items():
        if contents.get(LUA_NATIVE[rid]) not in (None, library.read_bytes()):
            problems.append(f"{files['archive'].name}: {LUA_NATIVE[rid]} differs from {library}")

    with zipfile.ZipFile(files["src_rock"]) as rock:
        inside = sorted(rock.namelist())
        if inside != sorted([names["rockspec"], names["archive"]]):
            problems.append(f"{files['src_rock'].name}: holds {inside}")
        else:
            for key in ("rockspec", "archive"):
                if rock.read(files[key].name) != files[key].read_bytes():
                    problems.append(f"{files['src_rock'].name}: its {files[key].name} differs")
    for problem in problems:
        print(f"❌ {problem}")
    if not problems:
        sizes = ", ".join(f"{p.name} ({p.stat().st_size / 1024:.0f} KB)" for p in files.values())
        print(f"✅ {sizes}: MIT, LICENSE, " + ", ".join(LUA_NATIVE.values()) + f", {LUA_MODULE} {version}")
    return not problems


LUA_SMOKE_PROGRAM = r'''-- A server (this process) and a client (a second process, this script with the name as its argument)
local fipc = require("fipc")
local ffi = require("ffi")

local name = arg[1]
if name then
    local conn = assert(fipc.connect(name, 10000))
    assert(conn:send("ping", 10000))
    local id = assert(conn:rpc_submit(7, string.rep("x", 100000), 10000))
    local response = assert(conn:rpc_recv(10000))
    assert(response.id == id and response.payload == "pong")
    conn:close()
    print("client OK")
    os.exit(0)
end

name = "fipc_smoke_lua_" .. tostring(os.time()) .. "_" .. tostring(math.random(1000000))
local listener = assert(fipc.listen(name, 65536))
-- The client: this interpreter (arg[-1]) running this script, its output read here
local command = '"' .. arg[-1] .. '" "' .. arg[0] .. '" ' .. name
if ffi.os == "Windows" then
    command = '"' .. command .. '"' -- cmd /c takes off the outer pair
end
local client = assert(io.popen(command, "r"))
local conn = assert(listener:accept(10000))
assert(conn:recv(10000) == "ping")
local request = assert(conn:rpc_recv(10000))
assert(request.opcode == 7 and #request.payload == 100000)
assert(conn:rpc_respond(request.id, request.opcode, 0, "pong", 10000))
local output = client:read("*a")
client:close()
assert(output:find("client OK", 1, true), "the client failed: " .. output)
conn:close()
listener:close()
print("round trip OK: fipc " .. fipc._VERSION .. ", library " .. fipc.library)
'''


def smoke_rock(src_rock: Path, version: str, library: Optional[Path]) -> None:
    """Installs the source rock with LuaRocks into a fresh tree in a temporary folder (for this machine's LuaJIT),
    and makes a round trip between two LuaJIT processes there, outside the repository, with the paths
    `luarocks path` gives. The module must load the library the rock installed (`library`'s bytes, when given)."""
    rid = host_rid()
    name = PLATFORMS[rid][0]
    interpreter = luajit()
    lua_dir = Path(interpreter).resolve().parent.parent  # <lua_dir>/bin/luajit
    with tempfile.TemporaryDirectory(prefix="fipc-smoke-", ignore_cleanup_errors=True) as temp:
        work = Path(temp)
        tree = work / "tree"
        base = [luarocks(), "--lua-dir", str(lua_dir), "--lua-version", "5.1", "--tree", str(tree)]
        _run(base + ["install", src_rock.resolve()], cwd=work)
        folder = LUA_INSTALLED_FOLDER[rid]
        installed = sorted(path for path in tree.rglob(name) if path.parent.parts[-len(folder):] == folder)
        if len(installed) != 1:
            print(f"❌ {src_rock.name}: expected one {'/'.join(folder)}/{name} in the tree, found {installed}")
            sys.exit(1)
        if library is not None and installed[0].read_bytes() != library.read_bytes():
            print(f"❌ {src_rock.name}: the library it installed is not the rock's ({library})")
            sys.exit(1)
        env = {k: v for k, v in os.environ.items() if k != "FASTIPC_LIB_DIR" and k not in LIBRARY_PATH_VARIABLES}
        for variable, option in (("LUA_PATH", "--lr-path"), ("LUA_CPATH", "--lr-cpath")):
            found = subprocess.run(base + ["path", option], capture_output=True, text=True, check=True).stdout
            env[variable] = found.strip() + ";;"
        (work / "smoke.lua").write_text(LUA_SMOKE_PROGRAM, encoding="utf-8")
        print(f"Running: {interpreter} smoke.lua", flush=True)
        result = subprocess.run([interpreter, "smoke.lua"], cwd=work, env=env, capture_output=True, text=True)
        print(result.stdout, end="")
        print(result.stderr, end="", file=sys.stderr, flush=True)
        if result.returncode != 0 or "round trip OK" not in result.stdout:
            print(f"❌ {src_rock.name}: the smoke program failed")
            sys.exit(1)
        loaded = Path(result.stdout.rsplit("library ", 1)[-1].strip())
        if not loaded.is_absolute():
            loaded = work / loaded
        if loaded.resolve() != installed[0].resolve():
            print(f"❌ {src_rock.name}: the module loaded {loaded}, not the rock's {installed[0]}")
            sys.exit(1)
    print(f"✅ {src_rock.name}: installed by LuaRocks into a fresh tree, round trip OK")


def rock_of(path: Path) -> Optional[Path]:
    """The source rock a path names (the .src.rock, or the rockspec beside it); None for another file."""
    if path.name.startswith(f"{LUA_ROCK}-") and path.name.endswith(".src.rock"):
        return path
    if path.name.startswith(f"{LUA_ROCK}-") and path.suffix == ".rockspec":
        rock = path.with_name(path.name.removesuffix(".rockspec") + ".src.rock")
        if not rock.exists():
            print(f"❌ {rock} missing: smoke takes the source rock")
            sys.exit(1)
        return rock
    return None


# ===== JavaScript (the npm package) =====

NPM_PACKAGE = "fipc"
# rid -> the npm package's folder for that platform (Node's process.platform-process.arch), where index.cjs looks for
# the addon and the library
NPM_PLATFORMS = {"linux-x64": "linux-x64", "win-x64": "win32-x64", "osx-arm64": "darwin-arm64",
                 "linux-arm64": "linux-arm64"}
NPM_ADDON = "fipc.node"
# The package's files besides package.json, LICENSE and prebuilds/
NPM_FILES = ("index.cjs", "index.mjs", "index.d.ts", "index.d.mts", "README.md")


def _executable(name: str) -> str:
    return f"{name}.exe" if sys.platform == "win32" else name


# The JavaScript binding's floor (bindings/js/package.json's engines)
NODE_MIN_MAJOR = 22


def _node_major(exe: str) -> int:
    """The major version of the Node.js `exe` ("v22.20.0": 22); 0 if it doesn't run or says something else."""
    try:
        out = subprocess.run([exe, "--version"], capture_output=True, text=True, timeout=30).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return 0
    match = re.match(r"v(\d+)\.", out)
    return int(match.group(1)) if match else 0


_found_node: List[Optional[str]] = []


def find_node() -> Optional[str]:
    """Node.js: the NODE environment variable; else node on PATH if it is Node.js 22 or newer; else the newest nvm
    install (~/.nvm/versions/node) of 22 or newer; None if none."""
    if not _found_node:
        given = os.environ.get("NODE")
        if given:
            _found_node.append(given)
        else:
            nvm = Path(os.environ.get("NVM_DIR", Path.home() / ".nvm")) / "versions" / "node"
            installs = sorted(nvm.glob("v*"), key=lambda p: [int(n) for n in re.findall(r"\d+", p.name)], reverse=True)
            candidates = [shutil.which("node")] + [str(p / "bin" / _executable("node")) for p in installs]
            _found_node.append(next((c for c in candidates if c and _node_major(c) >= NODE_MIN_MAJOR), None))
    return _found_node[0]


def node() -> str:
    """Node.js (find_node); exits if there is none."""
    found = find_node()
    if not found:
        print(f"❌ Node.js {NODE_MIN_MAJOR} or newer not found: set NODE, or put it on PATH")
        sys.exit(1)
    return found


def npm() -> str:
    """npm: the NPM environment variable, the one next to node (find_node), or npm on PATH; exits if there is none."""
    found = os.environ.get("NPM")
    if not found and find_node():
        folder = Path(find_node()).parent
        found = next((str(p) for p in (folder / "npm.cmd", folder / "npm") if p.exists()), None)
    found = found or shutil.which("npm")
    if not found:
        print("❌ npm not found: set NPM, or put npm on PATH")
        sys.exit(1)
    return found


def find_bun() -> Optional[str]:
    """Bun: the BUN environment variable, bun on PATH, or a user-local install (~/bun, ~/.bun/bin)."""
    exe = _executable("bun")
    return _user_tool("BUN", "bun", [Path.home() / "bun" / exe, Path.home() / ".bun" / "bin" / exe])


def find_deno() -> Optional[str]:
    """Deno: the DENO environment variable, deno on PATH, or a user-local install (~/deno, ~/.deno/bin)."""
    exe = _executable("deno")
    return _user_tool("DENO", "deno", [Path.home() / "deno" / exe, Path.home() / ".deno" / "bin" / exe])


def npm_tarball(version: str) -> str:
    """The file name `npm pack` gives the package."""
    return f"{NPM_PACKAGE}-{version}.tgz"


def build_npm(repo: Path, libraries: Dict[str, Path], addons: Dict[str, Path], out_dir: Path, work: Path) -> Path:
    """The npm package: bindings/js's entries, types and README, the LICENSE, and every platform's addon and library in
    prebuilds/<platform-arch>/, packed by `npm pack` into out_dir (fipc-<version>.tgz)."""
    binding = repo / "bindings" / "js"
    staging = work / "npm"
    if staging.exists():
        shutil.rmtree(staging)
    staging.mkdir(parents=True)
    for name in ("package.json", *NPM_FILES):
        shutil.copy2(binding / name, staging / name)
    shutil.copy2(repo / "LICENSE", staging / "LICENSE")
    for rid, library in libraries.items():
        folder = staging / "prebuilds" / NPM_PLATFORMS[rid]
        folder.mkdir(parents=True)
        shutil.copy2(library, folder / PLATFORMS[rid][0])
        shutil.copy2(addons[rid], folder / NPM_ADDON)
    result = subprocess.run([npm(), "pack", "--pack-destination", str(out_dir.resolve())], cwd=staging,
                            capture_output=True, text=True)
    if result.returncode != 0:
        print(f"❌ npm pack failed:\n{result.stdout}{result.stderr}")
        sys.exit(1)
    version = json.loads((binding / "package.json").read_text(encoding="utf-8"))["version"]
    return out_dir / npm_tarball(version)


def _binary_platform(data: bytes) -> Optional[str]:
    """The rid a shared library's header says it is for (ELF x86-64 or AArch64, PE x64, Mach-O arm64), or None."""
    if data[:4] == b"\x7fELF":
        return {62: "linux-x64", 183: "linux-arm64"}.get(int.from_bytes(data[18:20], "little"))
    if data[:2] == b"MZ":
        offset = int.from_bytes(data[0x3C:0x40], "little")
        if data[offset:offset + 4] == b"PE\0\0" and int.from_bytes(data[offset + 4:offset + 6], "little") == 0x8664:
            return "win-x64"
        return None
    if data[:4] == b"\xcf\xfa\xed\xfe" and int.from_bytes(data[4:8], "little") == 0x0100000C:  # MH_MAGIC_64, ARM64
        return "osx-arm64"
    return None


def check_npm(tarball: Path, version: str, libraries: Dict[str, Path], addons: Dict[str, Path],
              license_text: bytes) -> bool:
    """The tarball's package.json (name, version, license, entries, platforms, no dependencies or install script), its
    files and nothing more (the entries, types, README, LICENSE, every platform's addon and library as given), each
    binary for its platform, and index.cjs's VERSION."""
    problems = []
    with tarfile.open(tarball, "r:gz") as tar:
        contents = {m.name.removeprefix("package/"): tar.extractfile(m).read() for m in tar.getmembers() if m.isfile()}
    manifest = json.loads(contents.get("package.json", b"{}"))
    expected = {"name": NPM_PACKAGE, "version": version, "license": "MIT", "main": "./index.cjs",
                "types": "./index.d.ts", "author": "Hayden Donnelly",
                "homepage": "https://fastipc.github.io/fastipc/js.html"}
    for key, value in expected.items():
        if manifest.get(key) != value:
            problems.append(f"package.json: {key} = {manifest.get(key)!r}, expected {value!r}")
    if set(manifest.get("os", [])) != {"linux", "win32", "darwin"} or set(manifest.get("cpu", [])) != {"x64", "arm64"}:
        problems.append(f"package.json: os {manifest.get('os')}, cpu {manifest.get('cpu')}")
    if manifest.get("dependencies") or any(k in manifest.get("scripts", {}) for k in ("install", "postinstall")):
        problems.append("package.json: a dependency or an install script")
    binaries = {f"prebuilds/{NPM_PLATFORMS[rid]}/{name}": (rid, source)
                for rid in NPM_PLATFORMS
                for name, source in ((PLATFORMS[rid][0], libraries.get(rid)), (NPM_ADDON, addons.get(rid)))}
    expected_files = {"package.json", "LICENSE", *NPM_FILES, *binaries}
    problems += [f"{tarball.name}: {n} missing" for n in sorted(expected_files - set(contents))]
    problems += [f"{tarball.name}: unexpected {n}" for n in sorted(set(contents) - expected_files)]
    if contents.get("LICENSE", license_text) != license_text:
        problems.append(f"{tarball.name}: LICENSE differs from the repository's LICENSE")
    if f"const VERSION = '{version}';" not in contents.get("index.cjs", b"").decode():
        problems.append(f"{tarball.name}: index.cjs's VERSION isn't {version}")
    for name, (rid, source) in binaries.items():
        data = contents.get(name)
        if data is None:
            continue
        if source is not None and data != source.read_bytes():
            problems.append(f"{tarball.name}: {name} differs from {source}")
        if _binary_platform(data) != rid:
            problems.append(f"{tarball.name}: {name} is a binary for {_binary_platform(data)}, not {rid}")
    for problem in problems:
        print(f"❌ {problem}")
    if not problems:
        print(f"✅ {tarball.name} ({tarball.stat().st_size / 1024:.0f} KB): MIT, LICENSE, the addon and the library "
              f"for {', '.join(NPM_PLATFORMS.values())}, fipc {version}")
    return not problems


JS_SMOKE_PROGRAM = r"""// A server (this process) and a client (a second process, this script with the name as its argument)
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import fipc, { Connection, Listener } from 'fipc';

const name = process.argv[2];
if (name) {
  const conn = await Connection.connect(name, 10000);
  await conn.send('ping', 10000);
  const id = await conn.rpcSubmit(7, 'x'.repeat(100000), 10000);
  const response = await conn.rpcReceive(10000);
  if (response.id !== id || response.payload.toString() !== 'pong') throw new Error('a wrong response');
  conn.close();
  console.log('client OK');
  process.exit(0);
}

const channel = `fipc_smoke_js_${process.pid}_${Date.now()}`;
const listener = Listener.listen(channel, 65536);
// The client: this runtime running this script (Deno: deno run -A)
const runtime = typeof Deno !== 'undefined' ? [process.execPath, 'run', '-A'] : [process.execPath];
const args = [...runtime.slice(1), fileURLToPath(import.meta.url), channel];
const child = spawn(runtime[0], args, { stdio: ['ignore', 'pipe', 'inherit'] });
let output = '';
child.stdout.on('data', (chunk) => (output += chunk));
const exited = new Promise((resolve) => child.on('close', resolve));
const conn = await listener.accept(10000);
if ((await conn.receive(10000)).toString() !== 'ping') throw new Error('expected ping');
const request = await conn.rpcReceive(10000);
if (request.opcode !== 7 || request.payload.length !== 100000) throw new Error('a wrong request');
await conn.rpcRespond(request.id, request.opcode, 0, 'pong', 10000);
if ((await exited) !== 0 || !output.includes('client OK')) throw new Error(`the client failed: ${output}`);
conn.close();
listener.close();
console.log(`round trip OK: fipc ${fipc.version}, library ${fipc.library}`);
"""


def smoke_npm(tarball: Path, library: Optional[Path]) -> None:
    """Installs the tarball with npm into a fresh project in a temporary folder and makes a round trip between two
    processes there, outside the repository: with Node.js, and with Bun and Deno where they are installed. The module
    must load the library the package installed (`library`'s bytes, when given)."""
    rid = host_rid()
    with tempfile.TemporaryDirectory(prefix="fipc-smoke-", ignore_cleanup_errors=True) as temp:
        work = Path(temp)
        (work / "package.json").write_text(json.dumps({"name": "fipc-smoke", "private": True, "type": "module"}),
                                           encoding="utf-8")
        result = subprocess.run([npm(), "install", "--no-audit", "--no-fund", str(tarball.resolve())], cwd=work,
                                capture_output=True, text=True)
        if result.returncode != 0:
            print(f"❌ {tarball.name}: npm install failed:\n{result.stdout}{result.stderr}")
            sys.exit(1)
        installed = work / "node_modules" / NPM_PACKAGE / "prebuilds" / NPM_PLATFORMS[rid] / PLATFORMS[rid][0]
        if library is not None and installed.read_bytes() != library.read_bytes():
            print(f"❌ {tarball.name}: the library it installed is not the package's ({library})")
            sys.exit(1)
        (work / "smoke.mjs").write_text(JS_SMOKE_PROGRAM, encoding="utf-8")
        env = {k: v for k, v in os.environ.items() if k != "FASTIPC_LIB_DIR" and k not in LIBRARY_PATH_VARIABLES}
        runtimes = [("Node.js", [node()])]
        if find_bun():
            runtimes.append(("Bun", [find_bun()]))
        if find_deno():
            runtimes.append(("Deno", [find_deno(), "run", "-A"]))
        for title, command in runtimes:
            print(f"Running: {' '.join(command)} smoke.mjs", flush=True)
            run = subprocess.run([*command, "smoke.mjs"], cwd=work, env=env, capture_output=True, text=True)
            print(run.stdout, end="")
            print(run.stderr, end="", file=sys.stderr, flush=True)
            if run.returncode != 0 or "round trip OK" not in run.stdout:
                print(f"❌ {tarball.name}: the smoke program failed under {title}")
                sys.exit(1)
            loaded = Path(run.stdout.rsplit("library ", 1)[-1].strip())
            if loaded.resolve() != installed.resolve():
                print(f"❌ {tarball.name}: under {title} the module loaded {loaded}, not the package's {installed}")
                sys.exit(1)
        names = ", ".join(title for title, _ in runtimes)
    print(f"✅ {tarball.name}: installed by npm into a fresh project, round trip OK ({names})")


def npm_tarball_of(path: Path) -> Optional[Path]:
    """The npm package a path names (fipc-<version>.tgz); None for another file."""
    if path.name.startswith(f"{NPM_PACKAGE}-") and path.name.endswith(".tgz"):
        return path
    return None


# ===== Go (the module) =====

GO_MODULE = "github.com/fastipc/fastipc/bindings/go"
GO_BINDING = "bindings/go"  # the module's folder in the repository: its release tag is bindings/go/v<version>
# The binding's floor and toolchain (bindings/go/go.mod's go line)
GO_MIN = (1, 27)
# rid -> where the module carries the library of that platform (the package finds native/<rid>/ next to its source)
GO_NATIVE = {rid: f"fipc/native/{rid}/{name}" for rid, (name, _) in PLATFORMS.items()}
# Go's limit on a module's files' total size unzipped (golang.org/x/mod/zip)
GO_ZIP_LIMIT = 500 << 20


def go_env() -> dict:
    """The environment for the go command: the toolchain it is (GOTOOLCHAIN=local: it never downloads another), no cgo
    (the binding needs none), and no workspace file."""
    env = dict(os.environ)
    env.update({"GOTOOLCHAIN": "local", "CGO_ENABLED": "0", "GOWORK": "off"})
    return env


def _go_version(exe: str) -> tuple:
    """The version of the go command `exe` ((1, 27, 1) for go1.27.1); () if it doesn't run or says something else.
    Asked outside any module, with GOTOOLCHAIN=local, so an older go doesn't fetch the toolchain a go.mod names."""
    try:
        out = subprocess.run([exe, "env", "GOVERSION"], capture_output=True, text=True, timeout=60, env=go_env(),
                             cwd=tempfile.gettempdir()).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return ()
    match = re.match(r"go(\d+)\.(\d+)(?:\.(\d+))?", out)
    return tuple(int(n or 0) for n in match.groups()) if match else ()


_found_go: List[Optional[str]] = []


def find_go() -> Optional[str]:
    """The go command: the GO environment variable; else go on PATH if it is Go 1.27 or newer; else the newest
    user-local SDK of 1.27 or newer (~/go-sdk/go<version>/bin/go); None if none."""
    if not _found_go:
        given = os.environ.get("GO")
        if given:
            _found_go.append(given)
        else:
            sdks = [p / "bin" / _executable("go") for p in (Path.home() / "go-sdk").glob("go*")]
            sdks.sort(key=lambda p: [int(n) for n in re.findall(r"\d+", p.parent.parent.name)], reverse=True)
            candidates = [shutil.which("go")] + [str(p) for p in sdks if p.exists()]
            _found_go.append(next((c for c in candidates if c and _go_version(c)[:2] >= GO_MIN), None))
    return _found_go[0]


def go() -> str:
    """The go command (find_go); exits if there is none."""
    found = find_go()
    if not found:
        print(f"❌ Go {GO_MIN[0]}.{GO_MIN[1]} or newer not found: set GO, put go on PATH, or install it in "
              "~/go-sdk/go<version>")
        sys.exit(1)
    return found


def gofmt() -> Optional[str]:
    """gofmt: the one next to the go command (find_go), else gofmt on PATH; None if neither is there."""
    if find_go():
        beside = Path(find_go()).parent / _executable("gofmt")
        if beside.exists():
            return str(beside)
    return shutil.which("gofmt")


def _go_tracked(repo: Path) -> List[str]:
    """The module's files the repository tracks, relative to bindings/go (what the release tag's commit holds, besides
    the files build_go adds)."""
    out = subprocess.run(["git", "ls-files", "-z", "--", GO_BINDING], cwd=repo, capture_output=True, check=True).stdout
    return sorted(name.decode().removeprefix(f"{GO_BINDING}/") for name in out.split(b"\0") if name)


def build_go(repo: Path, libraries: Dict[str, Path], out_dir: Path) -> Path:
    """The module's tree as its release tag holds it, in out_dir/go: the files of bindings/go the repository tracks,
    the LICENSE (pkg.go.dev shows a module's documentation only with a license in it) and every platform's library in
    fipc/native/<rid>/. The release workflow commits the LICENSE and fipc/native/ into bindings/go, on a commit only
    the tag bindings/go/v<version> points to (fipc/native/ is ignored everywhere else)."""
    tree = out_dir / "go"
    for name in _go_tracked(repo):
        target = tree / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(repo / GO_BINDING / name, target)
    shutil.copy2(repo / "LICENSE", tree / "LICENSE")
    for rid, library in libraries.items():
        target = tree / GO_NATIVE[rid]
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(library, target)
    return tree


def _go_requirements(go_mod: str) -> List[tuple]:
    """The modules a go.mod requires: (path, version)."""
    return re.findall(r"^\s*(?:require\s+)?([\w.~-]+\.[\w.~/-]+)\s+(v\d\S*)", go_mod, re.MULTILINE)


def _go_escape(path: str) -> str:
    """A module path as the module cache and a proxy spell it (an upper-case letter becomes ! and the letter)."""
    return re.sub(r"[A-Z]", lambda m: "!" + m.group(0).lower(), path)


def _go_seed_proxy(tree: Path, proxy: Path) -> None:
    """The file proxy `proxy` (GOPROXY=file://...) with the modules the module's go.sum names (purego), copied from
    the user's module cache, where `go mod download` fetches what is missing: with that cache warm, no network."""
    exe = go()
    env = go_env()
    cache = Path(subprocess.run([exe, "env", "GOMODCACHE"], capture_output=True, text=True, check=True, env=env,
                                cwd=tempfile.gettempdir()).stdout.strip()) / "cache" / "download"
    modules: Dict[tuple, bool] = {}  # (path, version) -> whether its zip is needed (not only its go.mod)
    for line in (tree / "go.sum").read_text(encoding="utf-8").splitlines():
        if line.strip():
            path, version, _ = line.split()
            key = (path, version.removesuffix("/go.mod"))
            modules[key] = modules.get(key, False) or not version.endswith("/go.mod")
    for (path, version), need_zip in modules.items():
        source = cache / _go_escape(path) / "@v"
        kinds = (".info", ".mod", ".zip") if need_zip else (".info", ".mod")
        if not all((source / f"{version}{kind}").exists() for kind in kinds):
            _run([exe, "mod", "download", f"{path}@{version}"], cwd=Path(tempfile.gettempdir()), env=env)
        target = proxy / _go_escape(path) / "@v"
        target.mkdir(parents=True, exist_ok=True)
        for kind in kinds:
            shutil.copy2(source / f"{version}{kind}", target / f"{version}{kind}")
        with open(target / "list", "a", encoding="utf-8") as listing:
            listing.write(version + "\n")


def _go_module_env(work: Path, proxy: Path) -> dict:
    """go_env with a GOPATH (and module cache) of its own in `work` and the file proxy `proxy` as the only source of
    modules: offline, no checksum database, the module cache writable (so the folder can be deleted), and no
    FASTIPC_LIB_DIR or LIBRARY_PATH_VARIABLES, so that what a program loads is the module's own library."""
    env = {k: v for k, v in go_env().items() if k != "FASTIPC_LIB_DIR" and k not in LIBRARY_PATH_VARIABLES}
    gopath = work / "gopath"
    env.update({"GOPATH": str(gopath.resolve()), "GOMODCACHE": str((gopath / "pkg" / "mod").resolve()),
                "GOPROXY": f"{proxy.resolve().as_uri()},off", "GOSUMDB": "off", "GONOSUMDB": GO_MODULE,
                "GOPRIVATE": "", "GOFLAGS": "-mod=mod -modcacherw"})
    return env


def check_go(repo: Path, tree: Path, libraries: Dict[str, Path], license_text: bytes, work: Path) -> bool:
    """The tree's files and nothing more (the tracked files of bindings/go, the LICENSE, every platform's library as
    given, each a binary for its platform), its go.mod (the module path, the go line, purego the one requirement),
    Go's size limit for a module, and `go vet ./...` on it, its dependency from a file proxy (in `work`)."""
    problems = []
    expected = {*_go_tracked(repo), "LICENSE", *GO_NATIVE.values()}
    found = {p.relative_to(tree).as_posix() for p in tree.rglob("*") if p.is_file()}
    problems += [f"go/: {n} missing" for n in sorted(expected - found)]
    problems += [f"go/: unexpected {n}" for n in sorted(found - expected)]
    go_mod = (tree / "go.mod").read_text(encoding="utf-8") if (tree / "go.mod").exists() else ""
    for line in (f"module {GO_MODULE}", f"go {GO_MIN[0]}.{GO_MIN[1]}"):
        if line not in go_mod.splitlines():
            problems.append(f"go/go.mod lacks '{line}'")
    requirements = [path for path, _ in _go_requirements(go_mod)]
    if requirements != ["github.com/ebitengine/purego"]:
        problems.append(f"go/go.mod requires {requirements}, expected github.com/ebitengine/purego alone")
    if (tree / "LICENSE").exists() and (tree / "LICENSE").read_bytes() != license_text:
        problems.append("go/LICENSE differs from the repository's LICENSE")
    for rid, path in GO_NATIVE.items():
        if not (tree / path).exists():
            continue
        data = (tree / path).read_bytes()
        if rid in libraries and data != libraries[rid].read_bytes():
            problems.append(f"go/{path} differs from {libraries[rid]}")
        if _binary_platform(data) != rid:
            problems.append(f"go/{path} is a binary for {_binary_platform(data)}, not {rid}")
    size = sum((tree / n).stat().st_size for n in found)
    if size > GO_ZIP_LIMIT:
        problems.append(f"go/: {size} bytes, over Go's limit for a module of {GO_ZIP_LIMIT} bytes")
    if not problems:
        proxy = work / "go-proxy"
        _go_seed_proxy(tree, proxy)
        vet = subprocess.run([go(), "vet", "./..."], cwd=tree, env=_go_module_env(work, proxy), capture_output=True,
                             text=True)
        if vet.returncode != 0:
            problems.append(f"go vet ./... failed:\n{vet.stdout}{vet.stderr}")
    for problem in problems:
        print(f"❌ {problem}")
    if not problems:
        print(f"✅ go/ ({size / 1024:.0f} KB): {GO_MODULE}, LICENSE, " + ", ".join(GO_NATIVE.values())
              + ", go vet clean")
    return not problems


def _go_module_zip(tree: Path, version: str, target: Path) -> None:
    """The module's zip as a proxy serves it: every file of the tree under <module>@v<version>/."""
    prefix = f"{GO_MODULE}@v{version}/"
    with zipfile.ZipFile(target, "w", zipfile.ZIP_DEFLATED) as z:
        for path in sorted(p for p in tree.rglob("*") if p.is_file()):
            z.write(path, prefix + path.relative_to(tree).as_posix())


GO_SMOKE_MOD = """module fipcsmoke

go @GO@

require @MODULE@ v@VERSION@
"""

GO_SMOKE_PROGRAM = r"""// A server (this process) and a client (a second process, this program with the name as its argument)
package main

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"time"

	"github.com/fastipc/fastipc/bindings/go/fipc"
)

const timeout = 10 * time.Second

func check(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	if len(os.Args) > 1 {
		conn, err := fipc.Connect(os.Args[1], timeout)
		check(err)
		defer conn.Close()
		check(conn.Send([]byte("ping"), timeout))
		id, err := conn.RPCSubmit(7, make([]byte, 100_000), timeout)
		check(err)
		response, err := conn.RPCReceive(timeout)
		check(err)
		if response.ID != id || string(response.Payload) != "pong" {
			log.Fatalf("a wrong response: %+v", response)
		}
		return
	}
	name := fmt.Sprintf("fipc_smoke_go_%d", os.Getpid())
	listener, err := fipc.Listen(name, 1<<16)
	check(err)
	defer listener.Close()
	exe, err := os.Executable()
	check(err)
	client := exec.Command(exe, name)
	client.Stdout, client.Stderr = os.Stdout, os.Stderr
	check(client.Start())
	conn, err := listener.Accept(timeout)
	check(err)
	defer conn.Close()
	msg, err := conn.Receive(timeout)
	check(err)
	if string(msg) != "ping" {
		log.Fatalf("expected ping, got %q", msg)
	}
	request, err := conn.RPCReceive(timeout)
	check(err)
	if request.Opcode != 7 || len(request.Payload) != 100_000 {
		log.Fatalf("a wrong request: opcode %d, %d bytes", request.Opcode, len(request.Payload))
	}
	check(conn.RPCRespond(request.ID, request.Opcode, 0, []byte("pong"), timeout))
	check(client.Wait())
	library, err := fipc.LibraryPath()
	check(err)
	fmt.Printf("round trip OK: %s, library %s\n", exe, library)
}
"""


def _go_round_trip(exe: Path, cwd: Path, env: dict, expected: Path, what: str) -> None:
    """Runs the smoke program `exe`; exits unless it makes its round trip with the library `expected`."""
    print(f"Running: {exe}", flush=True)
    run = subprocess.run([str(exe)], cwd=cwd, env=env, capture_output=True, text=True)
    print(run.stdout, end="")
    print(run.stderr, end="", file=sys.stderr, flush=True)
    if run.returncode != 0 or "round trip OK" not in run.stdout:
        print(f"❌ the Go module: the smoke program failed ({what})")
        sys.exit(1)
    loaded = Path(run.stdout.rsplit("library ", 1)[-1].strip())
    if loaded.resolve() != expected.resolve():
        print(f"❌ the Go module: {what}, the program loaded {loaded}, not {expected}")
        sys.exit(1)


def smoke_go(tree: Path, version: str, library: Optional[Path], work: Path) -> None:
    """A throwaway Go project in a temporary folder that requires the module at `version`, served with its
    dependency by a file proxy (in `work`, with a module cache of its own there) as the module proxy would serve the
    release tag, and makes a round trip between two processes on this OS, built with CGO_ENABLED=0: once as built,
    loading the library from the module's copy in the module cache (`library`'s bytes, when given), then shipped as
    the README says (the executable and the library side by side in a folder of their own, the module cache gone) and
    run there."""
    rid = host_rid()
    name = PLATFORMS[rid][0]
    exe_name = _executable("fipc-smoke")
    proxy = work / "go-proxy"
    if proxy.exists():
        shutil.rmtree(proxy)
    _go_seed_proxy(tree, proxy)
    target = proxy / _go_escape(GO_MODULE) / "@v"
    target.mkdir(parents=True)
    (target / "list").write_text(f"v{version}\n", encoding="utf-8")
    (target / f"v{version}.info").write_text(json.dumps({"Version": f"v{version}", "Time": "2026-01-01T00:00:00Z"}),
                                             encoding="utf-8")
    shutil.copy2(tree / "go.mod", target / f"v{version}.mod")
    _go_module_zip(tree, version, target / f"v{version}.zip")
    env = _go_module_env(work, proxy)
    with tempfile.TemporaryDirectory(prefix="fipc-smoke-", ignore_cleanup_errors=True) as temp:
        project = Path(temp) / "project"
        project.mkdir()
        (project / "go.mod").write_text(GO_SMOKE_MOD.replace("@GO@", f"{GO_MIN[0]}.{GO_MIN[1]}")
                                        .replace("@MODULE@", GO_MODULE).replace("@VERSION@", version),
                                        encoding="utf-8")
        (project / "main.go").write_text(GO_SMOKE_PROGRAM, encoding="utf-8")
        _run([go(), "mod", "tidy"], cwd=project, env=env)
        _run([go(), "build", "-o", exe_name, "."], cwd=project, env=env)

        installed = Path(env["GOMODCACHE"]) / f"{_go_escape(GO_MODULE)}@v{version}" / GO_NATIVE[rid]
        if not installed.exists():
            print(f"❌ the Go module: no {installed} in the module cache")
            sys.exit(1)
        if library is not None and installed.read_bytes() != library.read_bytes():
            print(f"❌ the Go module: the library in the module cache is not the module's ({library})")
            sys.exit(1)
        _go_round_trip(project / exe_name, project, env, installed, "as built")

        shipped = Path(temp) / "shipped"
        shipped.mkdir()
        shutil.copy2(project / exe_name, shipped / exe_name)
        shutil.copy2(installed, shipped / name)
        _run([go(), "clean", "-modcache"], cwd=project, env=env)
        if installed.exists():
            print(f"❌ the Go module: {installed} is still there after go clean -modcache")
            sys.exit(1)
        _go_round_trip(shipped / exe_name, Path(temp), env, shipped / name, "shipped next to its library")
    print(f"✅ the Go module {GO_MODULE} v{version}: required by a Go project from a file proxy, built with "
          "CGO_ENABLED=0, round trip OK (as built, and shipped next to its library)")


def go_tree_of(path: Path) -> Optional[Path]:
    """The Go module's tree a path names (a folder whose go.mod is the module's: zig-out/packages/go); None for
    another path."""
    go_mod = path / "go.mod"
    if path.is_dir() and go_mod.exists() and f"module {GO_MODULE}" in go_mod.read_text(encoding="utf-8").splitlines():
        return path
    return None
