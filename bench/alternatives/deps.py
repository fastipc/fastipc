"""Fetches and builds the other IPC libraries the comparison runs, at pinned versions, into build/deps/<os>.

    python bench/alternatives/deps.py [--only zmq,nng,...] [--jobs N] [--clean]

Also builds alt_bench's Rust roles for iceoryx2 (rust/, "rust"). Needs git, CMake 3.30+ (Aeron's floor; CMAKE=<path> selects one), a C/C++ compiler (Windows: Visual Studio 2022) and, for iceoryx2, cargo (stable Rust). Each
library is cloned at its tag, built in Release and installed into the prefix; a library already installed there is
skipped (delete its folder to rebuild it). Boost is header-only: its headers are extracted from the release archive.
A library that fails to build is reported and left out (cpp-ipc has no macOS support); --clean deletes each build
tree once its library is installed, for a small disk.
"""

import argparse
import os
import platform
import shutil
import subprocess
import sys
import tarfile
import urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent
OS = {"Windows": "windows", "Linux": "linux", "Darwin": "macos"}[platform.system()]
ROOT = HERE / "build" / "deps" / OS
SRC = ROOT / "src"
PREFIX = ROOT / "prefix"

# name: (repository, tag, CMake options)
LIBS = {
    "zmq": (
        "https://github.com/zeromq/libzmq.git",
        "v4.3.5",
        [
            "-DBUILD_SHARED=OFF",
            "-DBUILD_STATIC=ON",
            "-DBUILD_TESTS=OFF",
            "-DWITH_PERF_TOOL=OFF",
            "-DENABLE_CURVE=OFF",
            "-DWITH_LIBSODIUM=OFF",
            "-DENABLE_DRAFTS=OFF",
            "-DWITH_DOCS=OFF",
            "-DCMAKE_POLICY_VERSION_MINIMUM=3.5",
        ],
    ),
    "nng": (
        "https://github.com/nanomsg/nng.git",
        "v2.0.0",
        ["-DBUILD_SHARED_LIBS=OFF", "-DNNG_TESTS=OFF", "-DNNG_TOOLS=OFF", "-DNNG_ENABLE_NNGCAT=OFF"],
    ),
    "cpp-ipc": (
        "https://github.com/mutouyun/cpp-ipc.git",
        "v1.4.1",
        ["-DLIBIPC_BUILD_SHARED_LIBS=OFF", "-DLIBIPC_BUILD_TESTS=OFF", "-DLIBIPC_BUILD_DEMOS=OFF"],
    ),
    "iceoryx2": (
        "https://github.com/eclipse-iceoryx/iceoryx2.git",
        "v0.10.0",
        ["-DBUILD_CXX=ON", "-DBUILD_EXAMPLES=OFF", "-DBUILD_TESTING=OFF"],
    ),
    "aeron": (
        "https://github.com/aeron-io/aeron.git",
        "1.53.3",
        [
            "-DBUILD_AERON_DRIVER=ON",
            "-DBUILD_AERON_ARCHIVE_API=OFF",
            "-DAERON_TESTS=OFF",
            "-DAERON_UNIT_TESTS=OFF",
            "-DAERON_SYSTEM_TESTS=OFF",
            "-DAERON_BUILD_SAMPLES=OFF",
            "-DAERON_BUILD_DOCUMENTATION=OFF",
            "-DAERON_INSTALL_TARGETS=ON",
        ],
    ),
}
CMAKE = os.environ.get("CMAKE", "cmake")
BOOST_URL = "https://github.com/boostorg/boost/releases/download/boost-1.92.0/boost-1.92.0-b2-nodocs.tar.xz"


def run(cmd, cwd=None, env=None):
    print("+", " ".join(str(c) for c in cmd), flush=True)
    subprocess.run([str(c) for c in cmd], cwd=cwd, env=env, check=True)


def build_cmake(name, jobs, clean):
    repo, tag, options = LIBS[name]
    stamp = PREFIX / f".{name}-{tag}"
    if stamp.exists():
        print(f"{name} {tag}: installed")
        return
    src = SRC / name
    if not src.exists():
        run(["git", "clone", "--quiet", "--depth", "1", "--branch", tag, repo, src])
    build = ROOT / "build" / name
    env = dict(os.environ)
    env.setdefault("RUSTUP_TOOLCHAIN", "stable")
    configure = [CMAKE, "-S", src, "-B", build, f"-DCMAKE_INSTALL_PREFIX={PREFIX}", f"-DCMAKE_PREFIX_PATH={PREFIX}"]
    if OS == "windows":
        configure += ["-G", "Visual Studio 17 2022", "-A", "x64"]
    else:
        configure += ["-G", "Ninja" if shutil.which("ninja") else "Unix Makefiles", "-DCMAKE_BUILD_TYPE=Release"]
    configure += ["-DCMAKE_POSITION_INDEPENDENT_CODE=ON", *options]
    run(configure, env=env)
    run([CMAKE, "--build", build, "--config", "Release", "--parallel", str(jobs)], env=env)
    run([CMAKE, "--install", build, "--config", "Release"], env=env)
    stamp.write_text(tag)
    if clean:
        shutil.rmtree(build, ignore_errors=True)
        shutil.rmtree(src, ignore_errors=True)


def build_rust_harness(clean):
    """alt_bench's roles for iceoryx2 in Rust (rust/), copied to build/rust/. CARGO_TARGET_DIR may move its build to
    a shorter path (Windows' linker fails on paths over 260 characters)."""
    target = Path(os.environ.get("CARGO_TARGET_DIR", HERE / "build" / "rust" / "target"))
    env = dict(os.environ, CARGO_TARGET_DIR=str(target))
    env.setdefault("RUSTUP_TOOLCHAIN", "stable")
    run(["cargo", "build", "--release", "--manifest-path", HERE / "rust" / "Cargo.toml"], env=env)
    exe = "alt_bench_iceoryx2" + (".exe" if OS == "windows" else "")
    shutil.copy2(target / "release" / exe, HERE / "build" / "rust" / exe)
    if clean:
        shutil.rmtree(target, ignore_errors=True)


def fetch_boost():
    stamp = PREFIX / ".boost-1.92.0"
    if stamp.exists():
        print("boost 1.92.0: installed")
        return
    archive = SRC / "boost-1.92.0.tar.xz"
    if not archive.exists():
        print("+ download", BOOST_URL, flush=True)
        SRC.mkdir(parents=True, exist_ok=True)
        urllib.request.urlretrieve(BOOST_URL, archive)
    include = PREFIX / "include"
    include.mkdir(parents=True, exist_ok=True)
    with tarfile.open(archive) as tar:
        for member in tar:
            parts = member.name.split("/", 1)
            if len(parts) == 2 and parts[1].startswith("boost/") and (member.isfile() or member.isdir()):
                member.name = parts[1]
                tar.extract(member, include, filter="data")
    stamp.write_text("1.92.0")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--only", help="comma-separated: " + ",".join([*LIBS, "boost", "rust"]))
    parser.add_argument("--jobs", type=int, default=os.cpu_count() or 4)
    parser.add_argument("--clean", action="store_true", help="delete each build tree once installed")
    args = parser.parse_args()
    names = args.only.split(",") if args.only else [*LIBS, "boost", "rust"]
    PREFIX.mkdir(parents=True, exist_ok=True)
    failed = []
    for name in names:
        try:
            if name == "boost":
                fetch_boost()
            elif name == "rust":
                (HERE / "build" / "rust").mkdir(parents=True, exist_ok=True)
                build_rust_harness(args.clean)
            elif name in LIBS:
                build_cmake(name, args.jobs, args.clean)
            else:
                sys.exit(f"unknown library: {name}")
        except subprocess.CalledProcessError as e:
            print(f"{name}: failed ({e}); left out", flush=True)
            failed.append(name)
    print(f"prefix: {PREFIX}")
    if failed:
        print("failed: " + ", ".join(failed))


if __name__ == "__main__":
    main()
