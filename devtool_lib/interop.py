"""
The cross-language matrix: `python devtool.py test interop [--server L] [--client L] [--client-first sample|all|none]
[--skip-missing]`.

Every language's server example runs against every language's client example, each in a process of its own: Zig, C,
C++, Python, C#, Java, Rust, Lua, JavaScript and Go, 10 x 10 = 100 ordered pairs, a language against itself included.
The examples are the programs the README and the website show (LANGUAGES below), and each pair makes their one
exchange: an RPC request with opcode 1 and the payload `ping`, which the server answers with `PING`. A pair passes
when both processes exit 0 and the client printed PING (Python prints the bytes, b'PING').

Each pair runs server first: the client starts once the server listens (its rendezvous name, docs/protocol.md
section 2, is there: a pipe on Windows, an abstract socket in /proc/net/unix on Linux, a socket file that accepts a
connection on macOS). With `--client-first sample`
(the default) ten more pairs start the client first, one second ahead of the server, so that its connect waits
for the server to listen: each language's server against the next language's client (Zig with C, C with C++, ...,
Go with Zig), so every language is once the client that waits and once the server that comes late.
`--client-first all` runs every pair in both orders.

Everything is built once, up front, with the bindings' own commands: `zig build` (the library in zig-out, which
every binding finds in a checkout) and `zig build examples` (examples/zig, examples/c-cpp), the repository's venv
(cffi), `dotnet build` of examples/csharp, the Java binding's jar (`gradlew jar`; the examples run with the source
launcher on the toolchain's JDK, as the README runs them), `cargo build` of the Rust examples, LuaJIT, which
needs no build, Node.js, whose addon `zig build` builds, and `go build` of the Go examples (without cgo). A missing
toolchain fails the run before anything is built, unless `--skip-missing` leaves that language out. A heavy, opt-in
check (it needs every toolchain): part of the pre-release gate, not of the test tiers.

`check_docs` (run by `test fast`, every edit, and before the matrix): each example is byte-for-byte what the
documentation shows. The README and a binding's README show the whole file, whose first line is a comment that names
it (a Rust example's crate doc comment, which the workspace's lints require, comes before that line and isn't shown, and
so does a Go example's description, the comment block before its first blank line); the website shows the file without
that line (its label names it). The overview's grid (website/index.html, #interop) lists the matrix's languages in its
order.
"""

import html
import os
import re
import shutil
import socket
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

from devtool_lib import package, rendezvous

# The name every example uses, and Windows' folder of named pipes
CHANNEL = "my_channel"
PIPES = "\\\\.\\pipe\\"
# How long a server may take to listen, a client to finish (its connect waits up to 5 s), and a server after its
# client is done
LISTEN_TIMEOUT_S = 30
CLIENT_TIMEOUT_S = 60
SERVER_TIMEOUT_S = 15
# With the client first: how long it runs alone before the server starts
CLIENT_HEAD_START_S = 1.0
# What a passing client prints (Python prints the payload as bytes)
REPLIES = ("PING", "b'PING'")


@dataclass
class Example:
    """One program of a pair: its file (relative to the repository) and the name it goes by in the docs."""
    path: str
    label: str


@dataclass
class Language:
    key: str  # devtool's name (--server, --client)
    title: str  # the website's and the grid's name
    server: Example
    client: Example
    page: str  # its page on the website
    readmes: List[str] = field(default_factory=list)  # Markdown files that show both files whole, besides README.md


LANGUAGES: List[Language] = [
    Language("zig", "Zig", Example("examples/zig/src/server.zig", "server.zig"),
             Example("examples/zig/src/client.zig", "client.zig"), "website/zig.html"),
    Language("c", "C", Example("examples/c-cpp/src/server.c", "server.c"),
             Example("examples/c-cpp/src/client.c", "client.c"), "website/c.html"),
    Language("cpp", "C++", Example("examples/c-cpp/src/server.cpp", "server.cpp"),
             Example("examples/c-cpp/src/client.cpp", "client.cpp"), "website/cpp.html"),
    Language("python", "Python", Example("examples/python/server.py", "server.py"),
             Example("examples/python/client.py", "client.py"), "website/python.html",
             ["bindings/python/README.md"]),
    Language("csharp", "C#", Example("examples/csharp/Server/Program.cs", "Server/Program.cs"),
             Example("examples/csharp/Client/Program.cs", "Client/Program.cs"), "website/csharp.html",
             ["bindings/csharp/README.md"]),
    Language("java", "Java", Example("examples/java/Server.java", "Server.java"),
             Example("examples/java/Client.java", "Client.java"), "website/java.html",
             ["bindings/java/README.md"]),
    Language("rust", "Rust", Example("bindings/rust/fipc/examples/server.rs", "server.rs"),
             Example("bindings/rust/fipc/examples/client.rs", "client.rs"), "website/rust.html",
             ["bindings/rust/README.md"]),
    Language("lua", "Lua", Example("bindings/lua/examples/server.lua", "server.lua"),
             Example("bindings/lua/examples/client.lua", "client.lua"), "website/lua.html",
             ["bindings/lua/README.md"]),
    Language("js", "JavaScript", Example("bindings/js/examples/server.mjs", "server.mjs"),
             Example("bindings/js/examples/client.mjs", "client.mjs"), "website/js.html",
             ["bindings/js/README.md"]),
    Language("go", "Go", Example("bindings/go/examples/server/main.go", "server.go"),
             Example("bindings/go/examples/client/main.go", "client.go"), "website/go.html",
             ["bindings/go/README.md"]),
]
KEYS = [language.key for language in LANGUAGES]
TITLES = {language.key: language.title for language in LANGUAGES}


# ===== The docs check =====

def _text(path: Path) -> str:
    return path.read_text(encoding="utf-8").replace("\r\n", "\n")


def _markdown_blocks(text: str) -> List[str]:
    return [m.group(1) for m in re.finditer(r"^```\w+\n(.*?)\n```$", text, re.MULTILINE | re.DOTALL)]


def _labelled_html_blocks(text: str) -> List[Tuple[str, str]]:
    """The website's code blocks with their labels: (label, code)."""
    pattern = r'<div class="label"><span>([^<]*)</span>.*?</div>\s*<pre><code class="language-\w+">(.*?)</code></pre>'
    return [(html.unescape(m.group(1)), html.unescape(m.group(2))) for m in re.finditer(pattern, text, re.DOTALL)]


def _grid_names(text: str) -> Tuple[List[str], List[List[str]]]:
    """The overview's grid (the table with id interop-grid): its column names, and each row's name and cells."""
    table = re.search(r'<table[^>]*id="interop-grid"[^>]*>(.*?)</table>', text, re.DOTALL)
    if not table:
        return [], []

    def cells(row: str) -> List[str]:
        found = re.findall(r"<t[hd][^>]*>(.*?)</t[hd]>", row, re.DOTALL)
        return [html.unescape(re.sub(r"<[^>]+>", "", c)).strip() for c in found]

    head = re.search(r"<thead>(.*?)</thead>", table.group(1), re.DOTALL)
    body = re.search(r"<tbody>(.*?)</tbody>", table.group(1), re.DOTALL)
    columns = cells(head.group(1))[1:] if head else []
    rows = [cells(r) for r in re.findall(r"<tr[^>]*>(.*?)</tr>", body.group(1), re.DOTALL)] if body else []
    return columns, rows


def check_docs(repo: Path) -> List[str]:
    """What differs between the examples and the documentation (empty when nothing does)."""
    problems: List[str] = []
    readme = _text(repo / "README.md")
    index = _text(repo / "website" / "index.html")
    for language in LANGUAGES:
        markdown = {"README.md": readme}
        markdown.update({name: _text(repo / name) for name in language.readmes})
        pages = {"website/index.html": index, language.page: _text(repo / language.page)}
        for example in (language.server, language.client):
            # What the docs show: the file from its first line that isn't a crate doc comment (//!) or blank, after a
            # Go example's description (a block of // lines that ends at a blank line)
            lines = _text(repo / example.path).rstrip("\n").split("\n")
            start = 0
            if example.path.endswith(".go"):
                start = next((i for i, line in enumerate(lines) if not line.startswith("//")), 0)
                start = start if start < len(lines) and not lines[start].strip() else 0
            start = next((i for i in range(start, len(lines)) if lines[i].strip() and not lines[i].startswith("//!")),
                         0)
            whole = "\n".join(lines[start:])
            first, _, body = whole.partition("\n")
            if example.label not in first:
                problems.append(f"{example.path}: its first line (after a crate doc comment) should name it "
                                f"({example.label})")
                continue
            for name, text in markdown.items():
                shown = [b for b in _markdown_blocks(text) if b.partition("\n")[0] == first]
                if not shown:
                    problems.append(f"{name}: no block shows {example.path} (a block whose first line is {first!r})")
                problems += [f"{name}: the block {first!r} differs from {example.path}" for b in shown if b != whole]
            for name, text in pages.items():
                shown = [code for label, code in _labelled_html_blocks(text) if label == example.label]
                if not shown:
                    problems.append(f"{name}: no block labelled {example.label} ({example.path})")
                problems += [f"{name}: the block labelled {example.label} differs from {example.path} without its "
                             "first line" for code in shown if code != body]
    titles = [language.title for language in LANGUAGES]
    columns, rows = _grid_names(index)
    if columns != titles or [row[0] for row in rows] != titles or any(len(row) != len(titles) + 1 for row in rows):
        problems.append(f"website/index.html: the grid (table#interop-grid) should have the columns and rows "
                        f"{titles} with a cell per pair; it has the columns {columns} and the rows "
                        f"{[row[:1] for row in rows]}")
    return problems


def report_docs(repo: Path) -> bool:
    problems = check_docs(repo)
    for problem in problems:
        print(f"❌ {problem}")
    if not problems:
        print(f"✅ the examples of {len(LANGUAGES)} languages are what the README, the bindings' READMEs and the "
              "website show")
    return not problems


# ===== The matrix =====

Command = Tuple[List[str], Path]  # argv, working directory


@dataclass
class Toolchains:
    """What the run needs, found up front."""
    repo: Path
    python: Path  # the repository's venv
    dotnet: Optional[str] = None
    java_runner: Optional[str] = None  # a JDK 17+ for the Gradle wrapper
    cargo: Optional[str] = None
    luajit: Optional[str] = None
    node: Optional[str] = None
    go: Optional[str] = None

    @staticmethod
    def find(repo: Path, python: Path, dotnet: str) -> "Toolchains":
        cargo_home = Path(os.environ.get("CARGO_HOME", Path.home() / ".cargo")) / "bin"
        java_home = os.environ.get("JAVA_HOME")
        java_exe = "java.exe" if sys.platform == "win32" else "java"
        return Toolchains(
            repo=repo,
            python=python,
            dotnet=shutil.which(dotnet),
            java_runner=shutil.which("java") or (str(Path(java_home) / "bin" / java_exe) if java_home and
                                                 (Path(java_home) / "bin" / java_exe).exists() else None),
            cargo=shutil.which("cargo") or next((str(p) for p in (cargo_home / "cargo", cargo_home / "cargo.exe")
                                                 if p.exists()), None),
            luajit=package.find_luajit(),
            node=package.find_node(),
            go=package.find_go(),
        )

    def missing(self, key: str) -> Optional[str]:
        """Why language `key` can't run here, or None."""
        return {
            "csharp": None if self.dotnet else "the .NET SDK (dotnet) isn't on PATH",
            "java": None if self.java_runner else "no JDK 17+ for the Gradle wrapper (JAVA_HOME, or java on PATH)",
            "rust": None if self.cargo else "cargo isn't on PATH or in ~/.cargo/bin (rustup)",
            "lua": None if self.luajit else "LuaJIT isn't found (LUAJIT, luajit on PATH, or ~/luajit/bin)",
            "js": None if self.node else "Node.js 22 or newer isn't found (NODE, node on PATH, or nvm)",
            "go": None if self.go else "Go 1.27 or newer isn't found (GO, go on PATH, or ~/go-sdk)",
        }.get(key)


def _run(cmd: List[str], cwd: Path, env: Optional[dict] = None) -> str:
    """Runs a build command; exits with its output if it fails. Its standard output."""
    print(f"   {' '.join(str(c) for c in cmd)}", flush=True)
    result = subprocess.run([str(c) for c in cmd], cwd=cwd, env=env, capture_output=True, text=True,
                            encoding="utf-8", errors="replace")
    if result.returncode != 0:
        print(f"❌ the build failed ({result.returncode}):\n{result.stdout[-3000:]}{result.stderr[-3000:]}")
        sys.exit(1)
    return result.stdout


Runner = Callable[[str], Command]  # role ("server" or "client") -> how to run it


def build(tools: Toolchains, keys: List[str], built_library: Callable[[], None]) -> Dict[str, Runner]:
    """Builds what languages `keys` need, once; for each, how to run its server or client (role "server" or
    "client"). `built_library` runs after `zig build` (devtool copies the library into the C# binding's runtimes/)."""
    repo = tools.repo
    exe = ".exe" if sys.platform == "win32" else ""
    runners: Dict[str, Runner] = {}
    started = time.monotonic()
    print("🔨 Building the library (zig build) and what the examples need", flush=True)
    _run(["zig", "build"], repo)
    built_library()
    if {"zig", "c", "cpp"} & set(keys):
        _run(["zig", "build", "examples"], repo)
    for key in keys:
        if key in ("zig", "c", "cpp"):
            folder = repo / "examples" / ("zig" if key == "zig" else "c-cpp") / "zig-out" / "bin"
            runners[key] = lambda role, folder=folder, key=key: ([str(folder / f"{key}_{role}{exe}")], folder)
        elif key == "python":
            runners[key] = lambda role: ([str(tools.python), f"{role}.py"], repo / "examples" / "python")
        elif key == "csharp":
            folder = repo / "examples" / "csharp"
            for project in ("Server", "Client"):
                _run([tools.dotnet, "build", "--configuration", "Release", "--nologo", "--verbosity", "quiet",
                      "--disable-build-servers", str(folder / project / f"{project}.csproj")], repo)
            runners[key] = lambda role, folder=folder: (
                [str(folder / role.title() / "bin" / "Release" / "net9.0" / f"{role.title()}{exe}")],
                folder / role.title())
        elif key == "java":
            out = _run([str(package.gradlew(repo)), "-p", str(repo / "bindings" / "java"), "--no-daemon",
                        "--console=plain", "--quiet", "jar", "printJavaHome"], repo)
            homes = [line.split("=", 1)[1].strip() for line in out.splitlines() if line.startswith("JAVA_HOME=")]
            if not homes:
                print(f"❌ the Java build printed no JAVA_HOME (printJavaHome):\n{out}")
                sys.exit(1)
            java = str(Path(homes[-1]) / "bin" / f"java{exe}")
            jar = repo / "bindings" / "java" / "build" / "libs" / f"fipc-{package.versions(repo)['java']}.jar"
            runners[key] = lambda role: ([java, "--enable-native-access=ALL-UNNAMED", "-cp", str(jar),
                                          f"{role.title()}.java"], repo / "examples" / "java")
        elif key == "rust":
            _run([tools.cargo, "build", "--quiet", "-p", "fipc", "--example", "server", "--example", "client"],
                 repo / "bindings" / "rust", package.cargo_env())
            folder = repo / "bindings" / "rust" / "target" / "debug" / "examples"
            runners[key] = lambda role, folder=folder: ([str(folder / f"{role}{exe}")], folder)
        elif key == "lua":
            runners[key] = lambda role: ([tools.luajit, f"examples/{role}.lua"], repo / "bindings" / "lua")
        elif key == "js":
            runners[key] = lambda role: ([tools.node, f"examples/{role}.mjs"], repo / "bindings" / "js")
        elif key == "go":
            # Without cgo, as the binding is used; the programs find zig-out's library, as in any checkout
            folder = repo / "zig-out" / "go" / "examples"
            _run([tools.go, "build", "-o", str(folder) + os.sep, "./examples/server", "./examples/client"],
                 repo / "bindings" / "go", package.go_env())
            runners[key] = lambda role, folder=folder: ([str(folder / f"{role}{exe}")], folder)
    print(f"⏱️  build: {time.monotonic() - started:.1f}s", flush=True)
    return runners


def run_env(repo: Path) -> dict:
    """The examples' environment: the library of zig-out where a program looks for it on its own (the Rust and C#
    programs run outside cargo and dotnet), and the Python binding of the checkout."""
    env = dict(os.environ)
    if sys.platform == "win32":
        env["PATH"] = str(repo / "zig-out" / "bin") + os.pathsep + env.get("PATH", "")
    elif sys.platform == "darwin":
        lib = str(repo / "zig-out" / "lib")
        env["DYLD_LIBRARY_PATH"] = lib + (os.pathsep + env["DYLD_LIBRARY_PATH"] if env.get("DYLD_LIBRARY_PATH") else "")
    else:
        lib = str(repo / "zig-out" / "lib")
        env["LD_LIBRARY_PATH"] = lib + (os.pathsep + env["LD_LIBRARY_PATH"] if env.get("LD_LIBRARY_PATH") else "")
    env["PYTHONPATH"] = str(repo / "bindings" / "python")
    env["PYTHONIOENCODING"] = "utf-8"
    env.pop("FASTIPC_LIB_DIR", None)
    return env


@dataclass
class Outcome:
    ok: bool
    seconds: float
    detail: str = ""


def _start(command: Command, env: dict) -> subprocess.Popen:
    argv, cwd = command
    return subprocess.Popen(argv, cwd=cwd, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                            encoding="utf-8", errors="replace")


def _finish(process: subprocess.Popen, timeout: float) -> Tuple[Optional[int], str, str]:
    """Waits for `process`; kills it after `timeout` (its exit code is then None)."""
    try:
        out, err = process.communicate(timeout=timeout)
        return process.returncode, out, err
    except subprocess.TimeoutExpired:
        process.kill()
        out, err = process.communicate()
        return None, out, err


def listening() -> bool:
    """Whether a server listens on CHANNEL: its rendezvous address exists (docs/protocol.md, "Names"), the pipe
    fastipc-<session>-my_channel on Windows, a listening abstract socket @fastipc-<euid>-my_channel on Linux, on
    macOS the socket file <user dir>fastipc/my_channel when a connection to it succeeds (a crashed listener leaves the
    file; a live one's accept drops the probe, which leaves at once without a word). The pairs run one at a time, so a
    server there is the pair's."""
    if sys.platform == "win32":
        try:
            return any(n.startswith("fastipc-") and n.endswith(f"-{CHANNEL}") for n in os.listdir(PIPES))
        except OSError:
            return False
    if sys.platform == "darwin":
        path = rendezvous.macos_socket_path(CHANNEL)
        if path is None or not os.path.exists(path):
            return False
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as probe:
            probe.settimeout(1.0)
            try:
                probe.connect(path)
                return True
            except OSError:
                return False
    try:
        lines = Path("/proc/net/unix").read_text().splitlines()[1:]
    except OSError:
        return False
    address = f"@fastipc-{os.geteuid()}-{CHANNEL}"
    # Num RefCount Protocol Flags Type St Inode Path: a listening socket has __SO_ACCEPTCON (0x10000) in its flags
    return any(len(f) >= 8 and f[7] == address and int(f[3], 16) & 0x10000 for f in (line.split() for line in lines))


def _wait(condition: Callable[[], bool], seconds: float, process: Optional[subprocess.Popen] = None) -> bool:
    """Polls `condition` until it holds (True), `seconds` pass or `process` exits (False)."""
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if condition():
            return True
        if process is not None and process.poll() is not None:
            return False
        time.sleep(0.005)
    return condition()


def run_pair(server: Command, client: Command, env: dict, client_first: bool) -> Outcome:
    if not _wait(lambda: not listening(), SERVER_TIMEOUT_S):
        return Outcome(False, 0.0, f"{CHANNEL} is still in use by another process")
    started = time.monotonic()
    if client_first:
        client_process = _start(client, env)
        time.sleep(CLIENT_HEAD_START_S)
        server_process = _start(server, env)
    else:
        server_process = _start(server, env)
        _wait(listening, LISTEN_TIMEOUT_S, server_process)  # if it never listens, the client says so
        client_process = _start(client, env)
    client_code, client_out, client_err = _finish(client_process, CLIENT_TIMEOUT_S)
    server_code, server_out, server_err = _finish(server_process, SERVER_TIMEOUT_S)
    seconds = time.monotonic() - started
    replied = any(line.strip() in REPLIES for line in (client_out + "\n" + client_err).splitlines())
    if client_code == 0 and server_code == 0 and replied:
        return Outcome(True, seconds)

    def describe(code: Optional[int], out: str, err: str) -> str:
        return (f"{'killed after its timeout' if code is None else f'exit {code}'}, stdout {out[-600:]!r}, "
                f"stderr {err[-1200:]!r}")

    detail = (f"client: {describe(client_code, client_out, client_err)}"
              + ("" if replied else " (no PING)") + f"\n      server: {describe(server_code, server_out, server_err)}")
    return Outcome(False, seconds, detail)


def _grid(title: str, results: Dict[Tuple[str, str], Outcome], servers: List[str], clients: List[str]) -> None:
    """Server rows x client columns: ok, FAIL, or . (not run)."""
    width = max(len(TITLES[k]) for k in KEYS) + 2
    print(f"\n{title}: rows are servers, columns clients")
    print(" " * (width + 2) + "".join(f"{TITLES[c]:>{width}}" for c in clients))
    for s in servers:
        cells = []
        for c in clients:
            outcome = results.get((s, c))
            cells.append("." if outcome is None else "ok" if outcome.ok else "FAIL")
        print(f"  {TITLES[s]:<{width}}" + "".join(f"{cell:>{width}}" for cell in cells))


def run(tools: Toolchains, servers: List[str], clients: List[str], client_first: str, skip_missing: bool,
        built_library: Callable[[], None]) -> bool:
    """The matrix: builds, runs the pairs, prints each and the grids. True if every pair passed."""
    started = time.monotonic()
    wanted = [k for k in KEYS if k in set(servers) | set(clients)]
    missing = {k: why for k in wanted if (why := tools.missing(k))}
    for key, why in missing.items():
        print(f"{'⚠️ ' if skip_missing else '❌'} {TITLES[key]}: {why}")
    if missing and not skip_missing:
        print("❌ toolchains are missing (above): install them, or pass --skip-missing to run the other languages")
        return False
    servers = [k for k in servers if k not in missing]
    clients = [k for k in clients if k not in missing]
    if not servers or not clients:
        print("❌ no pair left to run")
        return False

    runners = build(tools, [k for k in KEYS if k in set(servers) | set(clients)], built_library)
    env = run_env(tools.repo)
    pairs = [(s, c, False) for s in servers for c in clients]
    if client_first == "all":
        pairs += [(s, c, True) for s in servers for c in clients]
    elif client_first == "sample":
        pairs += [(s, clients[(i + 1) % len(clients)], True) for i, s in enumerate(servers)]
    print(f"\n🔀 {len(pairs)} pairs: {len(servers)} servers x {len(clients)} clients, server first"
          + {"all": ", then every pair client first", "sample": f", then {len(servers)} client first",
             "none": ""}[client_first], flush=True)

    results: Dict[bool, Dict[Tuple[str, str], Outcome]] = {False: {}, True: {}}
    pairs_started = time.monotonic()
    for s, c, first in pairs:
        outcome = run_pair(runners[s](role="server"), runners[c](role="client"), env, first)
        results[first][(s, c)] = outcome
        order = "client first" if first else "server first"
        print(f"{'✅' if outcome.ok else '❌'} {TITLES[s]} server, {TITLES[c]} client ({order}, "
              f"{outcome.seconds:.1f}s)" + (f"\n      {outcome.detail}" if outcome.detail else ""), flush=True)

    _grid("Server first", results[False], servers, clients)
    if results[True]:
        _grid("Client first (it waits for the server)", results[True], servers, clients)
    outcomes = [o for by_pair in results.values() for o in by_pair.values()]
    failed = sum(not o.ok for o in outcomes)
    print(f"\n{'✅' if not failed else '❌'} {len(outcomes) - failed}/{len(outcomes)} pairs passed"
          + (f"; skipped: {', '.join(TITLES[k] for k in missing)}" if missing else ""))
    now = time.monotonic()
    print(f"⏱️  pairs: {now - pairs_started:.1f}s, total: {now - started:.1f}s", flush=True)
    return failed == 0
