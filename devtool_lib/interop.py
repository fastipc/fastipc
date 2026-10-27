"""
The cross-language matrix: `python devtool.py test interop [--server L] [--client L] [--client-first sample|all|none]
[--pairs stream|broadcast|queue|service|all] [--skip-missing]`.

Every language's server example runs against every language's client example, each in a process of its own: Zig, C,
C++, Python, C#, Java, Rust, Lua, JavaScript and Go, 10 x 10 = 100 ordered pairs, a language against itself included.
The examples are the programs the README and the website show (LANGUAGES below), and each pair makes their one
exchange: the client sends the message `ping`, which the server answers with its uppercase, `PING`. A pair passes
when both processes exit 0 and the client printed PING (Python prints the bytes, b'PING').

Each pair runs server first: the client starts once the server listens (its rendezvous name, docs/protocol.md
section 2, is there: a pipe on Windows, an abstract socket in /proc/net/unix on Linux, a socket file that accepts a
connection on macOS). With `--client-first sample`
(the default) ten more pairs start the client first, one second ahead of the server, so that its connect waits
for the server to listen: each language's server against the next language's client (Zig with C, C with C++, ...,
Go with Zig), so every language is once the client that waits and once the server that comes late.
`--client-first all` runs every pair in both orders.

The broadcast's pairs come next, the same way: every language's publisher example against every language's
subscriber example, 100 more pairs, each run once. The publisher creates the broadcast `my_broadcast` (the library
admits its subscribers), waits for one, sends `tick 1` to `tick 3` and closes, which ends the broadcast; the gated
subscriber joins, prints the ticks and ends at DISCONNECTED. A pair passes when both exit 0 and the subscriber printed
the three ticks in order. The sample (every pair, with `--client-first all`) starts the subscriber first, one second
ahead of the publisher: its join waits for the publisher's broadcast (the examples wait up to 5 s).

The work queue's pairs come next, the same way: every language's producer example against every language's consumer
example, 100 more pairs and the sample. The producer creates the queue `my_queue` and joins it as its producer, sends
`job 1` to `job 3`, which wait in the queue, waits for a consumer, and closes its producer and the queue, which ends
it; the consumer joins, prints the jobs and ends at DISCONNECTED. A pair passes when both exit 0 and the consumer
printed each of the three jobs exactly once, in any order (the queue promises none). The sample (every pair, with
`--client-first all`) starts the consumer first, its join waiting for the producer's queue.

The service's pairs come last, the same way: every language's service server example against every language's
service client example, 100 more pairs and the sample. The server creates the service `my_service` and joins it as its
server, answers three requests, each with its uppercase, and closes its connection and the service, which ends it; the
client joins, sends `ping 1` to `ping 3`, each once the reply to the one before has come, and prints the replies. A
pair passes when both exit 0 and the client printed `PING 1`, `PING 2` and `PING 3`, in order (a client's replies come
in the order of its requests). The sample (every pair, with `--client-first all`) starts the client first, its join
waiting for the server's service. A language whose service examples aren't written yet (its `service_server` and
`service_client` below are None) is pending: the service's pairs leave it out and the run names it, passing without it.

`--server` and `--client` pick the publishers, the producers and the service servers, and the subscribers, the
consumers and the service clients, too; `--pairs stream`, `--pairs broadcast`, `--pairs queue` or `--pairs service`
runs one kind of pair alone.

Everything is built once, up front, with the bindings' own commands: `zig build` (the library in zig-out, which
every binding finds in a checkout) and `zig build examples` (examples/zig, examples/c-cpp), the repository's venv
(cffi), `dotnet build` of examples/csharp's projects, the Java binding's jar (`gradlew jar`; the examples run with
the source launcher on the toolchain's JDK, as the README runs them), `cargo build` of the Rust examples, LuaJIT,
which needs no build, Node.js, whose addon `zig build` builds, and `go build` of the Go examples (without cgo). A missing
toolchain fails the run before anything is built, unless `--skip-missing` leaves that language out. A heavy, opt-in
check (it needs every toolchain): part of the pre-release gate, not of the test tiers.

`check_docs` (run by `test fast` and before the matrix): each example is byte-for-byte what the documentation shows.
A file's first line is a comment that names it. The READMEs show the whole file from that line; the website shows it
without that line (its block's label names it). What comes before that line is not shown: a Rust example's crate doc
comment (the workspace's lints require one) and a Go example's description (the comment block before its first blank
line). The server and client examples appear in README.md, the binding's README, website/index.html and the
language's page; the broadcast, queue and service examples only in the binding's README and its page. The overview's
grid (website/index.html, #interop) lists the matrix's languages in its order.
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

from devtool_lib import package, rendezvous, site_assets

# The name the server and client examples use, the broadcast's, the work queue's, the service's, and Windows' folder
# of named pipes
CHANNEL = "my_channel"
BROADCAST = "my_broadcast"
QUEUE = "my_queue"
SERVICE = "my_service"
PIPES = "\\\\.\\pipe\\"
# How long a server (a publisher, a producer) may take to listen, a client (a subscriber, a consumer) to finish (its
# connect or join waits up to 5 s), and a server after its client is done
LISTEN_TIMEOUT_S = 30
CLIENT_TIMEOUT_S = 60
SERVER_TIMEOUT_S = 15
# With the client (the subscriber, the consumer) first: how long it runs alone before the server (the publisher, the
# producer) starts
CLIENT_HEAD_START_S = 1.0
# What a passing client prints (Python prints the payload as bytes), what a passing subscriber prints, in order, what
# a passing consumer prints, each once in any order, and what a passing service client prints, in order
REPLIES = ("PING", "b'PING'")
TICKS = ["tick 1", "tick 2", "tick 3"]
JOBS = ["job 1", "job 2", "job 3"]
ANSWERS = ["PING 1", "PING 2", "PING 3"]


def _printed(prefix: str, expected: List[str], in_order: bool) -> Callable[[str], bool]:
    """Whether the lines of an output that start with `prefix` are `expected`: in order, or each once in any order."""
    def check(out: str) -> bool:
        lines = [line.strip() for line in out.splitlines() if line.strip().startswith(prefix)]
        return lines == expected if in_order else sorted(lines) == sorted(expected)
    return check


@dataclass(frozen=True)
class Kind:
    """A kind of pair: its roles (the side that listens, the side that comes to it: each a field of Language, and
    the stem of its programs' names), the name both use, and whether the coming side's output (stdout and stderr) shows
    the exchange, or what it lacks."""
    key: str  # --pairs
    title: str
    listens: str
    comes: str
    name: str
    printed: Callable[[str], bool]
    missing: str


KINDS: List[Kind] = [
    Kind("stream", "Stream", "server", "client", CHANNEL,
         lambda out: any(line.strip() in REPLIES for line in out.splitlines()), "no PING"),
    Kind("broadcast", "Broadcast", "publisher", "subscriber", BROADCAST, _printed("tick", TICKS, in_order=True),
         "not tick 1, tick 2, tick 3"),
    Kind("queue", "Queue", "producer", "consumer", QUEUE, _printed("job", JOBS, in_order=False),
         "not job 1, job 2 and job 3, each once"),
    Kind("service", "Service", "service_server", "service_client", SERVICE, _printed("PING", ANSWERS, in_order=True),
         "not PING 1, PING 2, PING 3"),
]


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
    # The broadcast's examples (the broadcast my_broadcast: the publisher sends `tick 1` to `tick 3` once a subscriber
    # joined, then closes, which ends it), shown by the page and the readmes
    publisher: Optional[Example] = None
    subscriber: Optional[Example] = None
    # The work queue's examples (the queue my_queue: the producer creates it and joins it, sends `job 1` to `job 3`,
    # waits for a consumer and closes its producer and the queue, which ends it), shown by the page and the readmes
    producer: Optional[Example] = None
    consumer: Optional[Example] = None
    # The service's examples (the service my_service: the server creates it and joins it as its server, answers three
    # requests with their uppercase and closes its connection and the service, which ends it; the client sends `ping 1`
    # to `ping 3` and prints each reply), shown by the page and the readmes. None: pending, left out of the service's
    # pairs. A language adds them as its queue examples are: the two files, named like its producer and consumer with
    # the stem service_server and service_client (camel case where its names are: ServiceServer), set here, and shown
    # in its README and page next to the queue's; build() builds and runs them by that stem.
    service_server: Optional[Example] = None
    service_client: Optional[Example] = None

    def example(self, role: str) -> Optional[Example]:
        """The example of `role` (a Kind's listens or comes), or None."""
        return getattr(self, role)


LANGUAGES: List[Language] = [
    Language("zig", "Zig", Example("examples/zig/src/server.zig", "server.zig"),
             Example("examples/zig/src/client.zig", "client.zig"), "website/zig.html",
             publisher=Example("examples/zig/src/publisher.zig", "publisher.zig"),
             subscriber=Example("examples/zig/src/subscriber.zig", "subscriber.zig"),
             producer=Example("examples/zig/src/producer.zig", "producer.zig"),
             consumer=Example("examples/zig/src/consumer.zig", "consumer.zig"),
             service_server=Example("examples/zig/src/service_server.zig", "service_server.zig"),
             service_client=Example("examples/zig/src/service_client.zig", "service_client.zig")),
    Language("c", "C", Example("examples/c-cpp/src/server.c", "server.c"),
             Example("examples/c-cpp/src/client.c", "client.c"), "website/c.html",
             publisher=Example("examples/c-cpp/src/publisher.c", "publisher.c"),
             subscriber=Example("examples/c-cpp/src/subscriber.c", "subscriber.c"),
             producer=Example("examples/c-cpp/src/producer.c", "producer.c"),
             consumer=Example("examples/c-cpp/src/consumer.c", "consumer.c"),
             service_server=Example("examples/c-cpp/src/service_server.c", "service_server.c"),
             service_client=Example("examples/c-cpp/src/service_client.c", "service_client.c")),
    Language("cpp", "C++", Example("examples/c-cpp/src/server.cpp", "server.cpp"),
             Example("examples/c-cpp/src/client.cpp", "client.cpp"), "website/cpp.html",
             publisher=Example("examples/c-cpp/src/publisher.cpp", "publisher.cpp"),
             subscriber=Example("examples/c-cpp/src/subscriber.cpp", "subscriber.cpp"),
             producer=Example("examples/c-cpp/src/producer.cpp", "producer.cpp"),
             consumer=Example("examples/c-cpp/src/consumer.cpp", "consumer.cpp"),
             service_server=Example("examples/c-cpp/src/service_server.cpp", "service_server.cpp"),
             service_client=Example("examples/c-cpp/src/service_client.cpp", "service_client.cpp")),
    Language("python", "Python", Example("examples/python/server.py", "server.py"),
             Example("examples/python/client.py", "client.py"), "website/python.html",
             ["bindings/python/README.md"],
             publisher=Example("examples/python/publisher.py", "publisher.py"),
             subscriber=Example("examples/python/subscriber.py", "subscriber.py"),
             producer=Example("examples/python/producer.py", "producer.py"),
             consumer=Example("examples/python/consumer.py", "consumer.py"),
             service_server=Example("examples/python/service_server.py", "service_server.py"),
             service_client=Example("examples/python/service_client.py", "service_client.py")),
    Language("csharp", "C#", Example("examples/csharp/Server/Program.cs", "Server/Program.cs"),
             Example("examples/csharp/Client/Program.cs", "Client/Program.cs"), "website/csharp.html",
             ["bindings/csharp/README.md"],
             publisher=Example("examples/csharp/Publisher/Program.cs", "Publisher/Program.cs"),
             subscriber=Example("examples/csharp/Subscriber/Program.cs", "Subscriber/Program.cs"),
             producer=Example("examples/csharp/Producer/Program.cs", "Producer/Program.cs"),
             consumer=Example("examples/csharp/Consumer/Program.cs", "Consumer/Program.cs"),
             service_server=Example("examples/csharp/ServiceServer/Program.cs", "ServiceServer/Program.cs"),
             service_client=Example("examples/csharp/ServiceClient/Program.cs", "ServiceClient/Program.cs")),
    Language("java", "Java", Example("examples/java/Server.java", "Server.java"),
             Example("examples/java/Client.java", "Client.java"), "website/java.html",
             ["bindings/java/README.md"],
             publisher=Example("examples/java/Publisher.java", "Publisher.java"),
             subscriber=Example("examples/java/Subscriber.java", "Subscriber.java"),
             producer=Example("examples/java/Producer.java", "Producer.java"),
             consumer=Example("examples/java/Consumer.java", "Consumer.java"),
             service_server=Example("examples/java/ServiceServer.java", "ServiceServer.java"),
             service_client=Example("examples/java/ServiceClient.java", "ServiceClient.java")),
    Language("rust", "Rust", Example("bindings/rust/fipc/examples/server.rs", "server.rs"),
             Example("bindings/rust/fipc/examples/client.rs", "client.rs"), "website/rust.html",
             ["bindings/rust/README.md"],
             publisher=Example("bindings/rust/fipc/examples/publisher.rs", "publisher.rs"),
             subscriber=Example("bindings/rust/fipc/examples/subscriber.rs", "subscriber.rs"),
             producer=Example("bindings/rust/fipc/examples/producer.rs", "producer.rs"),
             consumer=Example("bindings/rust/fipc/examples/consumer.rs", "consumer.rs"),
             service_server=Example("bindings/rust/fipc/examples/service_server.rs", "service_server.rs"),
             service_client=Example("bindings/rust/fipc/examples/service_client.rs", "service_client.rs")),
    Language("lua", "Lua", Example("bindings/lua/examples/server.lua", "server.lua"),
             Example("bindings/lua/examples/client.lua", "client.lua"), "website/lua.html",
             ["bindings/lua/README.md"],
             publisher=Example("bindings/lua/examples/publisher.lua", "publisher.lua"),
             subscriber=Example("bindings/lua/examples/subscriber.lua", "subscriber.lua"),
             producer=Example("bindings/lua/examples/producer.lua", "producer.lua"),
             consumer=Example("bindings/lua/examples/consumer.lua", "consumer.lua"),
             service_server=Example("bindings/lua/examples/service_server.lua", "service_server.lua"),
             service_client=Example("bindings/lua/examples/service_client.lua", "service_client.lua")),
    Language("js", "JavaScript", Example("bindings/js/examples/server.mjs", "server.mjs"),
             Example("bindings/js/examples/client.mjs", "client.mjs"), "website/js.html",
             ["bindings/js/README.md"],
             publisher=Example("bindings/js/examples/publisher.mjs", "publisher.mjs"),
             subscriber=Example("bindings/js/examples/subscriber.mjs", "subscriber.mjs"),
             producer=Example("bindings/js/examples/producer.mjs", "producer.mjs"),
             consumer=Example("bindings/js/examples/consumer.mjs", "consumer.mjs"),
             service_server=Example("bindings/js/examples/service_server.mjs", "service_server.mjs"),
             service_client=Example("bindings/js/examples/service_client.mjs", "service_client.mjs")),
    Language("go", "Go", Example("bindings/go/examples/server/main.go", "server.go"),
             Example("bindings/go/examples/client/main.go", "client.go"), "website/go.html",
             ["bindings/go/README.md"],
             publisher=Example("bindings/go/examples/publisher/main.go", "publisher.go"),
             subscriber=Example("bindings/go/examples/subscriber/main.go", "subscriber.go"),
             producer=Example("bindings/go/examples/producer/main.go", "producer.go"),
             consumer=Example("bindings/go/examples/consumer/main.go", "consumer.go"),
             service_server=Example("bindings/go/examples/service_server/main.go", "service_server.go"),
             service_client=Example("bindings/go/examples/service_client/main.go", "service_client.go")),
]
KEYS = [language.key for language in LANGUAGES]
TITLES = {language.key: language.title for language in LANGUAGES}
BY_KEY = {language.key: language for language in LANGUAGES}


def _camel(role: str) -> str:
    """A role's name where a language names its programs in camel case (C#'s projects, Java's files): ServiceServer."""
    return "".join(part.title() for part in role.split("_"))


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
        readmes = {name: _text(repo / name) for name in language.readmes}
        page = {language.page: _text(repo / language.page)}
        shown_by = [(example, {"README.md": readme, **readmes}, {"website/index.html": index, **page})
                    for example in (language.server, language.client)]
        shown_by += [(example, readmes, page)
                     for example in (language.publisher, language.subscriber, language.producer, language.consumer,
                                     language.service_server, language.service_client)
                     if example]
        for example, markdown, pages in shown_by:
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
    # The website's stylesheets and scripts carry their content's hash in their URL
    problems += site_assets.problems(repo)
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


Runner = Callable[[str], Command]  # role (a Kind's listens or comes: "server", "consumer", ...) -> how to run it


def build(tools: Toolchains, keys: List[str], roles: List[str], built_library: Callable[[], None]) -> Dict[str, Runner]:
    """Builds the examples of `roles` (the Kinds' roles: "server", "client", "publisher", ...) that languages `keys`
    need and have (a pending one's are left out), once; for each language, how to run one of them. `built_library` runs
    after `zig build` (devtool copies the library into the C# binding's runtimes/)."""
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
        has = [role for role in roles if BY_KEY[key].example(role)]
        if key in ("zig", "c", "cpp"):
            folder = repo / "examples" / ("zig" if key == "zig" else "c-cpp") / "zig-out" / "bin"
            runners[key] = lambda role, folder=folder, key=key: ([str(folder / f"{key}_{role}{exe}")], folder)
        elif key == "python":
            runners[key] = lambda role: ([str(tools.python), f"{role}.py"], repo / "examples" / "python")
        elif key == "csharp":
            folder = repo / "examples" / "csharp"
            for project in (_camel(role) for role in has):
                _run([tools.dotnet, "build", "--configuration", "Release", "--nologo", "--verbosity", "quiet",
                      "--disable-build-servers", str(folder / project / f"{project}.csproj")], repo)
            runners[key] = lambda role, folder=folder: (
                [str(folder / _camel(role) / "bin" / "Release" / "net9.0" / f"{_camel(role)}{exe}")],
                folder / _camel(role))
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
                                          f"{_camel(role)}.java"], repo / "examples" / "java")
        elif key == "rust":
            _run([tools.cargo, "build", "--quiet", "-p", "fipc"] + [a for role in has for a in ("--example", role)],
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
            _run([tools.go, "build", "-o", str(folder) + os.sep] + [f"./examples/{role}" for role in has],
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


def listening(name: str) -> bool:
    """Whether something listens on `name`: a server, a broadcast's publisher or a queue's owner (docs/protocol.md,
    "Names").
    - Windows: the pipe fastipc-<session>-<name> exists.
    - Linux: the abstract socket @fastipc-<euid>-<name> is listening.
    - macOS: the socket file <user dir>fastipc/<name> accepts a connection. The file alone is not enough, since a
      crashed owner leaves it; a live one drops the probe at once.
    The pairs run one at a time, so a listener there is the pair's."""
    if sys.platform == "win32":
        try:
            return any(n.startswith("fastipc-") and n.endswith(f"-{name}") for n in os.listdir(PIPES))
        except OSError:
            return False
    if sys.platform == "darwin":
        path = rendezvous.macos_socket_path(name)
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
    address = f"@fastipc-{os.geteuid()}-{name}"
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


def run_pair(kind: Kind, server: Command, client: Command, env: dict, client_first: bool) -> Outcome:
    """One pair of `kind`: the side that listens (a server, publisher or producer) and the side that comes to it, the
    listening side first unless `client_first`."""
    if not _wait(lambda: not listening(kind.name), SERVER_TIMEOUT_S):
        return Outcome(False, 0.0, f"{kind.name} is still in use by another process")
    started = time.monotonic()
    if client_first:
        client_process = _start(client, env)
        time.sleep(CLIENT_HEAD_START_S)
        server_process = _start(server, env)
    else:
        server_process = _start(server, env)
        # If it never listens, the client says so
        _wait(lambda: listening(kind.name), LISTEN_TIMEOUT_S, server_process)
        client_process = _start(client, env)
    client_code, client_out, client_err = _finish(client_process, CLIENT_TIMEOUT_S)
    server_code, server_out, server_err = _finish(server_process, SERVER_TIMEOUT_S)
    seconds = time.monotonic() - started
    printed = kind.printed(client_out + "\n" + client_err)
    if client_code == 0 and server_code == 0 and printed:
        return Outcome(True, seconds)

    def describe(code: Optional[int], out: str, err: str) -> str:
        return (f"{'killed after its timeout' if code is None else f'exit {code}'}, stdout {out[-600:]!r}, "
                f"stderr {err[-1200:]!r}")

    detail = (f"{kind.comes}: {describe(client_code, client_out, client_err)}"
              + ("" if printed else f" ({kind.missing})")
              + f"\n      {kind.listens}: {describe(server_code, server_out, server_err)}")
    return Outcome(False, seconds, detail)


def _grid(title: str, kind: Kind, results: Dict[Tuple[str, str], Outcome], servers: List[str],
          clients: List[str]) -> None:
    """Rows: the kind's listening side; columns: its coming side. ok, FAIL, or . (not run)."""
    width = max(len(TITLES[k]) for k in KEYS) + 2
    print(f"\n{title}: rows are {kind.listens}s, columns {kind.comes}s")
    print(" " * (width + 2) + "".join(f"{TITLES[c]:>{width}}" for c in clients))
    for s in servers:
        cells = []
        for c in clients:
            outcome = results.get((s, c))
            cells.append("." if outcome is None else "ok" if outcome.ok else "FAIL")
        print(f"  {TITLES[s]:<{width}}" + "".join(f"{cell:>{width}}" for cell in cells))


def run(tools: Toolchains, servers: List[str], clients: List[str], client_first: str, kinds: List[str],
        skip_missing: bool, built_library: Callable[[], None]) -> bool:
    """The matrix: builds, runs the pairs of `kinds` ("stream", "broadcast", "queue", "service"), prints each and the
    grids. True if every pair passed. A kind's pairs leave out the languages that lack its examples (pending)."""
    started = time.monotonic()
    chosen = [kind for kind in KINDS if kind.key in kinds]
    roles = [role for kind in chosen for role in (kind.listens, kind.comes)]
    # The languages to build: those with an example of a chosen kind (a pending one needs no toolchain)
    wanted = [k for k in KEYS if k in set(servers) | set(clients) and any(BY_KEY[k].example(r) for r in roles)]
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

    runners = build(tools, [k for k in wanted if k not in missing], roles, built_library)
    env = run_env(tools.repo)

    outcomes: List[Outcome] = []
    pending: Dict[str, List[str]] = {}  # kind's title -> the languages without its examples
    for kind in chosen:
        listeners = [k for k in servers if BY_KEY[k].example(kind.listens)]
        comers = [k for k in clients if BY_KEY[k].example(kind.comes)]
        lacking = [k for k in KEYS if (k in servers and k not in listeners) or (k in clients and k not in comers)]
        if lacking:
            pending[kind.title] = lacking
            print(f"\n⏳ {kind.title}: pending (no {kind.listens} and {kind.comes} examples yet): "
                  f"{', '.join(TITLES[k] for k in lacking)}", flush=True)
        if not listeners or not comers:
            print(f"⏳ {kind.title}: no pair to run", flush=True)
            continue
        pairs = [(s, c, False) for s in listeners for c in comers]
        if client_first == "all":
            pairs += [(s, c, True) for s in listeners for c in comers]
        elif client_first == "sample":
            pairs += [(s, comers[(i + 1) % len(comers)], True) for i, s in enumerate(listeners)]
        print(f"\n🔀 {kind.title}: {len(pairs)} pairs, {len(listeners)} {kind.listens}s x {len(comers)} "
              f"{kind.comes}s, {kind.listens} first"
              + {"all": f", then every pair {kind.comes} first",
                 "sample": f", then {len(listeners)} {kind.comes} first", "none": ""}[client_first], flush=True)
        results: Dict[bool, Dict[Tuple[str, str], Outcome]] = {False: {}, True: {}}
        pairs_started = time.monotonic()
        for s, c, first in pairs:
            outcome = run_pair(kind, runners[s](kind.listens), runners[c](kind.comes), env, first)
            results[first][(s, c)] = outcome
            order = f"{kind.comes} first" if first else f"{kind.listens} first"
            print(f"{'✅' if outcome.ok else '❌'} {TITLES[s]} {kind.listens}, {TITLES[c]} {kind.comes} ({order}, "
                  f"{outcome.seconds:.1f}s)" + (f"\n      {outcome.detail}" if outcome.detail else ""), flush=True)
        _grid(f"{kind.title}, {kind.listens} first", kind, results[False], listeners, comers)
        if results[True]:
            _grid(f"{kind.title}, {kind.comes} first (it waits for the {kind.listens})", kind, results[True], listeners,
                  comers)
        kind_outcomes = [o for by_pair in results.values() for o in by_pair.values()]
        failed = sum(not o.ok for o in kind_outcomes)
        print(f"{'✅' if not failed else '❌'} {kind.title}: {len(kind_outcomes) - failed}/{len(kind_outcomes)} pairs "
              f"passed, {time.monotonic() - pairs_started:.1f}s", flush=True)
        outcomes += kind_outcomes

    failed = sum(not o.ok for o in outcomes)
    print(f"\n{'✅' if outcomes and not failed else '❌'} {len(outcomes) - failed}/{len(outcomes)} pairs passed"
          + (f"; skipped: {', '.join(TITLES[k] for k in missing)}" if missing else "")
          + "".join(f"; {title} pending: {', '.join(TITLES[k] for k in keys)}" for title, keys in pending.items()))
    print(f"⏱️  total: {time.monotonic() - started:.1f}s", flush=True)
    return bool(outcomes) and failed == 0
