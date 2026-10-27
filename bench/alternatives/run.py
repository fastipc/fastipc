"""Runs fastipc and other IPC libraries through the same cases (alt_bench, README.md) and prints their medians.

    python bench/alternatives/run.py [--libs fastipc,zeromq,...] [--cases stream,rtt,broadcast,queue,service,...]
                                     [--runs 3] [--duration-ms 1000] [--warmup-ms 300] [--buffer BYTES]
                                     [--batch N] [--out results.json]

Each role of a case runs in a process of its own (alt_bench <lib> <pattern> <role> ...): run.py starts them all,
waits until each is "ready", sends "go", waits for the senders' and callers' results, sends "stop" to the receivers
and collects theirs. A case a library lacks is skipped. Aeron's media driver (aeronmd from deps.py's prefix) runs in
a process of its own for the whole session.

Each library that takes a buffer size runs with its size for the case's pattern in BUFFERS, or without one there,
its usual size; --buffer gives every such library that many bytes instead (README.md). A size a library refuses is
skipped and recorded. Every result records the buffer its library had (`buffer_bytes`), and Aeron's its conductor
(`conductor`, AERON_CONDUCTOR or --aeron-conductor) and fragment limit (`fragment_limit`: AERON_FRAGMENT_LIMIT, 1, or
--aeron-fragment-limit).

--batch N runs the batched comparison: both ends of every case hand over N messages per call (a sender's batch, a
receiver's most per receive), with the libraries that have batch calls (BATCHING: fastipc's copying calls,
fipc_send_many and fipc_recv_many; Aeron's aeron_exclusive_publication_offer_block and its subscription poll with a
fragment limit of N), in the cases where both batch (BATCHED_CASES, unless --cases names others). Its results are a
series of their own (results/<platform>-batched.json for N = 16, the page's; -batched-<N> for another N).
"""

import argparse
import datetime
import json
import os
import platform
import queue
import shutil
import statistics
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
OS = {"Windows": "windows", "Linux": "linux", "Darwin": "macos"}[platform.system()]
# The platform a result belongs to, the series of results/ and the key of BUFFERS, AERON_CONDUCTOR and
# AERON_FRAGMENT_LIMIT: the OS, and for Linux on ARM64 its architecture too.
PLATFORM = "linux-arm64" if OS == "linux" and platform.machine() in ("aarch64", "arm64") else OS
PREFIX = HERE / "build" / "deps" / OS / "prefix"
EXE = HERE / "build" / "cmake" / ("Release/alt_bench.exe" if OS == "windows" else "alt_bench")
RUST_EXE = HERE / "build" / "rust" / ("alt_bench_iceoryx2.exe" if OS == "windows" else "alt_bench_iceoryx2")

LIBS = [
    "fastipc", "fastipc-zerocopy", "iceoryx2", "iceoryx2-cxx", "aeron", "zeromq", "zeromq-tcp", "nng", "nng-tcp", "cpp-ipc", "boost", "unix-socket",
    "tcp-loopback",
]
# Libraries whose receives never wait (their adapters poll), so --poll changes nothing for them.
POLLING = {"iceoryx2", "iceoryx2-cxx", "aeron"}


def command(lib):
    """The executable and library name of `lib`: iceoryx2 through its Rust API (rust/), iceoryx2-cxx through its C++
    binding (the C++ harness's "iceoryx2"), every other library through the C++ harness."""
    if lib == "iceoryx2":
        return [RUST_EXE, "iceoryx2"]
    if lib == "iceoryx2-cxx":
        return [EXE, "iceoryx2"]
    return [EXE, lib]

KiB = 1024
MiB = 1024 * KiB
# Libraries whose buffer isn't settable: cpp-ipc's queue is fixed, the sockets' buffers are the OS's.
UNSIZED = {"cpp-ipc", "unix-socket", "tcp-loopback"}
# Each library's configuration per platform (PLATFORM), the fastest for it there: the bytes of buffering it runs with by
# pattern (rtt cases are the stream's), {platform: {lib: {pattern: bytes}}}, and Aeron's conductor idle strategy, chosen
# by `sweep.py choose` from that platform's sweeps in sweeps/ (Aeron's at its fragment limit of 1). A library or pattern missing here runs at its usual size (`buffer_bytes`).
BUFFERS = {
    "windows": {
        "fastipc": {"stream": 4 * MiB, "broadcast": 4 * MiB, "queue": 4 * MiB, "service": 64 * KiB},
        "fastipc-zerocopy": {"stream": 4 * MiB, "broadcast": 512 * KiB, "queue": 4 * MiB, "service": 64 * KiB},
        "iceoryx2": {"stream": 4 * MiB, "broadcast": 64 * KiB},
        "iceoryx2-cxx": {"stream": 512 * KiB, "broadcast": 512 * KiB},
        "aeron": {"stream": 512 * KiB, "broadcast": 512 * KiB},
        "zeromq": {"stream": 4 * MiB, "broadcast": 4 * MiB, "queue": 512 * KiB, "service": 512 * KiB},
        "zeromq-tcp": {"stream": 4 * MiB, "broadcast": 512 * KiB, "queue": 512 * KiB, "service": 512 * KiB},
        "nng": {"stream": 64 * KiB, "broadcast": 64 * KiB, "queue": 64 * KiB, "service": 64 * KiB},
        "nng-tcp": {"stream": 64 * KiB, "broadcast": 64 * KiB, "queue": 64 * KiB, "service": 64 * KiB},
        "boost": {"stream": 4 * MiB, "queue": 4 * MiB},
    },
    "linux": {
        "fastipc": {"stream": 4 * MiB, "broadcast": 512 * KiB, "queue": 4 * MiB, "service": 512 * KiB},
        "fastipc-zerocopy": {"stream": 4 * MiB, "broadcast": 4 * MiB, "queue": 4 * MiB, "service": 512 * KiB},
        "iceoryx2": {"stream": 4 * MiB, "broadcast": 4 * MiB},
        "iceoryx2-cxx": {"stream": 4 * MiB, "broadcast": 512 * KiB},
        "aeron": {"stream": 512 * KiB, "broadcast": 4 * MiB},
        "zeromq": {"stream": 64 * KiB, "broadcast": 32 * MiB, "queue": 512 * KiB, "service": 4 * MiB},
        "zeromq-tcp": {"stream": 4 * MiB, "broadcast": 32 * MiB, "queue": 512 * KiB, "service": 32 * MiB},
        "nng": {"stream": 64 * KiB, "broadcast": 64 * KiB, "queue": 64 * KiB, "service": 64 * KiB},
        "nng-tcp": {"stream": 64 * KiB, "broadcast": 64 * KiB, "queue": 64 * KiB, "service": 64 * KiB},
        "boost": {"stream": 32 * MiB, "queue": 4 * MiB},
    },
    "macos": {
        "fastipc": {"stream": 512 * KiB, "broadcast": 32 * MiB, "queue": 4 * MiB, "service": 64 * KiB},
        "fastipc-zerocopy": {"stream": 512 * KiB, "broadcast": 32 * MiB, "queue": 4 * MiB, "service": 64 * KiB},
        "iceoryx2": {"stream": 512 * KiB, "broadcast": 4 * MiB},
        "iceoryx2-cxx": {"stream": 64 * KiB, "broadcast": 64 * KiB},
        "aeron": {"stream": 4 * MiB, "broadcast": 32 * MiB},
        "zeromq": {"stream": 512 * KiB, "broadcast": 32 * MiB, "queue": 32 * MiB, "service": 512 * KiB},
        "zeromq-tcp": {"stream": 4 * MiB, "broadcast": 32 * MiB, "queue": 512 * KiB, "service": 512 * KiB},
        "nng": {"stream": 64 * KiB, "broadcast": 64 * KiB, "queue": 64 * KiB, "service": 64 * KiB},
        "nng-tcp": {"stream": 64 * KiB, "broadcast": 64 * KiB, "queue": 64 * KiB, "service": 64 * KiB},
        "boost": {"stream": 32 * MiB, "queue": 32 * MiB},
    },
    "linux-arm64": {
        "fastipc": {"stream": 4 * MiB, "broadcast": 32 * MiB, "queue": 4 * MiB, "service": 64 * KiB},
        "fastipc-zerocopy": {"stream": 4 * MiB, "broadcast": 32 * MiB, "queue": 32 * MiB, "service": 4 * MiB},
        "iceoryx2": {"stream": 4 * MiB, "broadcast": 32 * MiB, "service": 64 * KiB},
        "iceoryx2-cxx": {"stream": 4 * MiB, "broadcast": 32 * MiB, "service": 512 * KiB},
        "aeron": {"stream": 4 * MiB, "broadcast": 32 * MiB},
        "zeromq": {"stream": 512 * KiB, "broadcast": 512 * KiB, "queue": 32 * MiB, "service": 512 * KiB},
        "zeromq-tcp": {"stream": 4 * MiB, "broadcast": 32 * MiB, "queue": 512 * KiB, "service": 32 * MiB},
        "nng": {"stream": 64 * KiB, "broadcast": 64 * KiB, "queue": 64 * KiB, "service": 64 * KiB},
        "nng-tcp": {"stream": 64 * KiB, "broadcast": 64 * KiB, "queue": 64 * KiB, "service": 64 * KiB},
        "boost": {"stream": 32 * MiB, "queue": 32 * MiB},
    },
}
AERON_CONDUCTOR = {"windows": "spin", "linux": "backoff", "macos": "backoff", "linux-arm64": "backoff"}
# Aeron's subscription polls take one message each (its fragment limit), as every other library's receive returns one
# message per call: the comparison measures each library's synchronization per message, not batching.
AERON_FRAGMENT_LIMIT = 1
# The batched comparison (--batch): the libraries with calls that send and receive messages in batches, and its cases
# (a stream's round trip has one message at a time to send).
BATCHING = ["fastipc", "aeron"]
BATCHED_CASES = ["stream-16", "stream-1024", "broadcast-1-lossless-16", "broadcast-4-lossless-16"]


def fragment_limit(args):
    """The most fragments one of Aeron's polls takes: --batch's N, --aeron-fragment-limit, else AERON_FRAGMENT_LIMIT."""
    if args.batch > 1:
        return args.batch
    return args.aeron_fragment_limit or AERON_FRAGMENT_LIMIT


def buffer_bytes(lib, size, buffer):
    """The bytes of buffering `lib` has for `size`-byte messages: --buffer's `buffer` (a library that counts messages
    holds buffer // size of them, at least 8), or without it its usual size (harness.hpp `depth_for`, each adapter's
    comment); None where it isn't settable."""
    if lib in UNSIZED:
        return None
    if buffer:
        return buffer if lib.startswith("fastipc") or lib == "aeron" else max(8, buffer // size) * size
    if lib.startswith("fastipc"):
        return 512 * KiB
    if lib == "aeron":
        return 32 * MiB  # its default term of 64 MiB: a window of half of it
    if lib.startswith("zeromq"):
        return 1000 * size  # its default high-water marks
    return min(max((512 * KiB) // size, 8), 1024) * size


def make_cases():
    """(id, pattern, {role: count}, params, kind) for every case, in the order they run."""
    cases = []
    for size in (16, 1 * KiB, 64 * KiB):
        cases.append((f"stream-{size}", "stream", {"rx": 1, "tx": 1}, {"size": size}, "throughput"))
    for size in (16, 1 * KiB):
        cases.append((f"rtt-{size}", "stream", {"echo": 1, "ping": 1}, {"size": size}, "latency"))
    for subs in (1, 4):
        for size in (16, 1 * KiB):
            for lossy in (False, True):
                tag = "lossy" if lossy else "lossless"
                cases.append((
                    f"broadcast-{subs}-{tag}-{size}", "broadcast", {"sub": subs, "pub": 1},
                    {"size": size, "subscribers": subs, "lossy": lossy}, "broadcast",
                ))
    for producers, consumers in ((1, 1), (4, 4)):
        for size in (16, 1 * KiB):
            cases.append((
                f"queue-{producers}x{consumers}-{size}", "queue", {"consumer": consumers, "producer": producers},
                {"size": size, "producers": producers, "consumers": consumers}, "queue",
            ))
    for clients, servers in ((1, 1), (4, 1), (4, 4)):
        for size in (16, 1 * KiB):
            cases.append((
                f"service-{clients}x{servers}-{size}", "service", {"server": servers, "client": clients},
                {"size": size, "clients": clients, "servers": servers}, "service",
            ))
    return cases


CASES = make_cases()
SENDING = {"tx", "pub", "producer", "ping", "client"}


def param_args(params, buffer=None):
    args = ["--buffer", str(buffer)] if buffer else []
    for key, value in params.items():
        if key == "lossy":
            if value:
                args.append("--lossy")
        else:
            args += [f"--{key}", str(value)]
    return args


def supports(lib, pattern, params, buffer=None, batch=1):
    batched = ["--batch", str(batch)] if batch > 1 else []
    return subprocess.run([*command(lib), "supports", pattern, *param_args(params, buffer), *batched]).returncode == 0


class Role:
    def __init__(self, cmd, env):
        self.cmd = cmd
        flags = subprocess.CREATE_NEW_PROCESS_GROUP if OS == "windows" else 0
        self.proc = subprocess.Popen(
            [str(c) for c in cmd], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, bufsize=1, env=env, creationflags=flags,
        )
        self.lines = queue.Queue()
        self.errors = []
        threading.Thread(target=self._read, args=(self.proc.stdout, self.lines), daemon=True).start()
        threading.Thread(target=self._read_errors, daemon=True).start()

    @staticmethod
    def _read(stream, sink):
        for line in stream:
            sink.put(line.strip())
        sink.put(None)

    def _read_errors(self):
        for line in self.proc.stderr:
            self.errors.append(line.rstrip())

    def expect(self, prefix, deadline):
        while True:
            left = deadline - time.monotonic()
            if left <= 0:
                raise TimeoutError(f"no '{prefix}' from {' '.join(map(str, self.cmd[1:4]))}")
            try:
                line = self.lines.get(timeout=left)
            except queue.Empty:
                continue
            if line is None:
                raise RuntimeError(f"{' '.join(map(str, self.cmd[1:4]))} ended: {' | '.join(self.errors[-5:])}")
            if line.startswith(prefix):
                return line[len(prefix):].strip()

    def tell(self, line):
        try:
            self.proc.stdin.write(line + "\n")
            self.proc.stdin.flush()
        except OSError:
            pass

    def finish(self, timeout):
        try:
            self.proc.stdin.close()
        except OSError:
            pass
        try:
            self.proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait()


class Session:
    def __init__(self, args):
        self.args = args
        self.counter = 0
        self.dir = Path(tempfile.mkdtemp(prefix="alt-"))
        self.env = dict(os.environ)
        # Aeron's log buffers are files in its directory: on Linux in /dev/shm (Aeron's default), memory, not disk.
        aeron_root = Path("/dev/shm") if OS == "linux" else self.dir
        self.aeron_dir = aeron_root / f"alt-aeron-{os.getpid()}"
        self.env["AERON_DIR"] = str(self.aeron_dir)
        # This checkout's library, whatever alt_bench's rpath says (a build of another checkout's).
        if OS != "windows":
            library_path = "DYLD_LIBRARY_PATH" if OS == "macos" else "LD_LIBRARY_PATH"
            self.env[library_path] = str(HERE.parent.parent / "zig-out" / "lib")
        self.driver = None

    def start_aeron(self):
        if self.driver:
            return
        # The static driver where the shared one can't find its libraries (Windows has no rpath; macOS's install has
        # none).
        exe = PREFIX / "bin" / {"windows": "aeronmd_s.exe", "macos": "aeronmd_s"}.get(OS, "aeronmd")
        env = dict(self.env)
        env["AERON_DIR_DELETE_ON_START"] = "1"
        # The driver's conductor, which moves an IPC publisher's flow-control limit: "spin" (Aeron's low-latency
        # setting, a core kept busy) or "backoff" (Aeron's default: it backs off into sleeps when idle).
        env["AERON_CONDUCTOR_IDLE_STRATEGY"] = self.args.aeron_conductor
        env["AERON_DIR_DELETE_ON_SHUTDOWN"] = "1"
        self.driver = subprocess.Popen([str(exe)], env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        cnc = self.aeron_dir / "cnc.dat"
        deadline = time.monotonic() + 10
        while not cnc.exists() and time.monotonic() < deadline:
            time.sleep(0.05)
        time.sleep(0.2)

    def close(self):
        if self.driver:
            self.driver.terminate()
            try:
                self.driver.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.driver.kill()
        shutil.rmtree(self.dir, ignore_errors=True)
        shutil.rmtree(self.aeron_dir, ignore_errors=True)

    def buffer(self, lib, pattern):
        """The --buffer to give `lib` for `pattern`: the command line's, else BUFFERS', else None (its usual)."""
        return self.args.buffer or BUFFERS.get(PLATFORM, {}).get(lib, {}).get(pattern)

    def run_once(self, lib, case, poll):
        case_id, pattern, roles, params, kind = case
        if lib == "aeron":
            self.start_aeron()
        self.counter += 1
        name = f"alt{os.getpid()}x{self.counter}"
        # TCP ports from 20000 to 48007: Windows reserves ranges of the dynamic ports above (Hyper-V, WSL) for itself
        port = 20000 + (os.getpid() * 8 + self.counter * 8) % 28000
        common = ["--name", name, "--dir", str(self.dir), "--port", str(port),
                  "--warmup-ms", str(self.args.warmup_ms), "--duration-ms", str(self.args.duration_ms),
                  *param_args(params, self.buffer(lib, pattern))]
        if poll:
            common.append("--poll")
        if self.args.batch > 1:
            common += ["--batch", str(self.args.batch)]
        started = []
        try:
            for role, count in roles.items():
                for index in range(count):
                    cmd = [*command(lib), pattern, role, "--index", str(index), *common]
                    started.append((role, Role(cmd, self.env)))
                    # Some libraries' setup races when processes start at the same instant (iceoryx2's node cleanup).
                    time.sleep(0.02)
            deadline = time.monotonic() + 30
            for _, r in started:
                r.expect("ready", deadline)
            for role, r in started:
                if role not in SENDING:
                    r.tell("go")
            for role, r in started:
                if role in SENDING:
                    r.tell("go")
            deadline = time.monotonic() + (self.args.warmup_ms + self.args.duration_ms) / 1000 + 60
            results = {}
            for role, r in started:
                if role in SENDING:
                    results.setdefault(role, []).append(json.loads(r.expect("result", deadline)))
            for role, r in started:
                if role not in SENDING:
                    r.tell("stop")
            deadline = time.monotonic() + 30
            for role, r in started:
                if role not in SENDING:
                    results.setdefault(role, []).append(json.loads(r.expect("result", deadline)))
            for role, r in started:
                if role in SENDING:
                    r.tell("quit")
            return metrics(kind, results)
        finally:
            for _, r in started:
                r.finish(timeout=5)


def seconds(r):
    return (r["t1"] - r["t0"]) / r["hz"]


def rate(r):
    span = seconds(r)
    return (r["count"] - 1) / span if span > 0 and r["count"] > 1 else 0.0


def metrics(kind, results):
    if kind == "throughput":
        return {"msgs_per_s": rate(results["rx"][0]), "lost": results["rx"][0]["lost"]}
    if kind == "latency":
        ping = results["ping"][0]
        return {"p50_ns": ping["p50_ns"], "p99_ns": ping["p99_ns"], "calls_per_s": ping["calls"] / seconds(ping)}
    if kind == "broadcast":
        subs = results["sub"]
        lost = max(s["lost"] / (s["lost"] + s["count"]) if s["count"] else 1.0 for s in subs)
        pub = results["pub"][0]
        return {"msgs_per_s": min(rate(s) for s in subs), "lost_pct": 100 * lost,
                "sent_per_s": pub["sent"] / seconds(pub)}
    if kind == "queue":
        consumers = [c for c in results["consumer"] if c["count"] > 0]
        hz = consumers[0]["hz"]
        span = (max(c["t1"] for c in consumers) - min(c["t0"] for c in consumers)) / hz
        return {"msgs_per_s": sum(c["count"] for c in consumers) / span}
    if kind == "service":
        clients = results["client"]
        return {"calls_per_s": sum(c["calls"] / seconds(c) for c in clients),
                "p50_ns": statistics.median(c["p50_ns"] for c in clients),
                "p99_ns": statistics.median(c["p99_ns"] for c in clients)}
    raise ValueError(kind)


def cpu_name():
    if OS == "windows":
        import winreg
        key = winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, r"HARDWARE\DESCRIPTION\System\CentralProcessor\0")
        return winreg.QueryValueEx(key, "ProcessorNameString")[0].strip()
    if OS == "linux":
        for line in Path("/proc/cpuinfo").read_text().splitlines():
            if line.startswith("model name"):
                return line.split(":", 1)[1].strip()
        return platform.machine()
    return subprocess.run(["sysctl", "-n", "machdep.cpu.brand_string"], capture_output=True, text=True).stdout.strip()


def fmt(metric, value):
    if value is None:
        return "-"
    if metric.endswith("_ns"):
        return f"{value / 1000:.1f}us" if value >= 10000 else f"{value:.0f}ns"
    if metric.endswith("_pct"):
        return f"{value:.1f}%"
    for unit, scale in (("M", 1e6), ("K", 1e3)):
        if value >= scale:
            return f"{value / scale:.2f}{unit}"
    return f"{value:.0f}"


def main():
    # Started from the background (an automated run), Windows throttles a process and puts it on the efficiency cores,
    # and the processes it starts each on its own: off for alt_bench and Aeron's media driver, as for devtool's
    # benchmarks (devtool_lib/throttle.py; on macOS it leaves the background state).
    sys.path.insert(0, str(HERE.parent.parent))
    from devtool_lib.throttle import unthrottle_descendants

    unthrottle_descendants()
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--libs", help="default: every library (with --batch, BATCHING's)")
    parser.add_argument("--cases", default="", help="case ids or prefixes (stream, rtt, broadcast, queue, service)")
    parser.add_argument("--skip", default="", help="case ids or prefixes to leave out")
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--duration-ms", type=int, default=1000)
    parser.add_argument("--warmup-ms", type=int, default=300)
    parser.add_argument("--poll", action="store_true", help="every receive looks once in a loop instead of waiting")
    parser.add_argument("--buffer", type=int, help="bytes of buffering for every library that takes a size")
    parser.add_argument("--aeron-conductor", choices=("spin", "backoff"),
                        help="Aeron's conductor idle strategy (default: AERON_CONDUCTOR's for this platform)")
    parser.add_argument("--aeron-fragment-limit", type=int, metavar="N",
                        help="the most fragments one of Aeron's polls takes, in every pattern (default: "
                             "AERON_FRAGMENT_LIMIT, 1)")
    parser.add_argument("--batch", type=int, default=1, metavar="N",
                        help="the batched comparison: both ends hand over N messages per call (a divisor of 64), "
                             "BATCHING's libraries in BATCHED_CASES")
    parser.add_argument("--out", type=Path)
    args = parser.parse_args()
    args.aeron_conductor = args.aeron_conductor or AERON_CONDUCTOR.get(PLATFORM, "spin")
    if args.aeron_fragment_limit is not None and args.aeron_fragment_limit < 1:
        parser.error("--aeron-fragment-limit must be at least 1")
    if args.batch < 1 or 64 % args.batch:
        parser.error("--batch is a divisor of 64")
    if args.aeron_fragment_limit and args.batch > 1:
        parser.error("--aeron-fragment-limit and --batch are exclusive (Aeron's polls take up to --batch's N)")
    if not EXE.exists():
        sys.exit(f"{EXE} is missing: build it first (README.md)")
    libs = args.libs.split(",") if args.libs else BATCHING if args.batch > 1 else LIBS
    if args.batch > 1 and set(libs) - set(BATCHING):
        parser.error(f"--batch runs only the libraries with batch calls: {', '.join(BATCHING)}")
    prefixes = [c for c in args.cases.split(",") if c] or (BATCHED_CASES if args.batch > 1 else [])
    skipped = [c for c in args.skip.split(",") if c]
    matches = lambda case_id, ids: any(case_id == p or case_id.startswith(p + "-") for p in ids)
    cases = [c for c in CASES if (not prefixes or matches(c[0], prefixes)) and not matches(c[0], skipped)]

    sys.path.insert(0, str(HERE))
    import deps

    def save():
        if args.out:
            args.out.write_text(json.dumps(report, indent=1))

    session = Session(args)
    report = {"os": OS, "cpu": cpu_name(), "platform": platform.platform(), "date": datetime.date.today().isoformat(),
              "versions": {name: tag for name, (_, tag, _) in deps.LIBS.items()} | {"boost": "1.92.0"},
              "runs": args.runs, "duration_ms": args.duration_ms, "warmup_ms": args.warmup_ms, "poll": args.poll,
              "buffer": args.buffer, "platform_key": PLATFORM, "aeron_conductor": args.aeron_conductor,
              "aeron_fragment_limit": fragment_limit(args), "batch": args.batch, "results": {}}
    try:
        for case in cases:
            case_id, pattern, _, params, _ = case
            for lib in libs:
                if not supports(lib, pattern, params, batch=args.batch):
                    continue
                buffer = session.buffer(lib, pattern)
                if buffer and not supports(lib, pattern, params, buffer, args.batch):
                    report["results"].setdefault(case_id, {})[lib] = {"runs": [], "skipped": "buffer",
                                                                     "buffer_bytes": buffer}
                    print(f"{case_id:28} {lib:13} skipped: doesn't take a buffer of {buffer} bytes", flush=True)
                    save()
                    continue
                limit = fragment_limit(args)
                # The most fragments one of Aeron's polls takes (src/lib_aeron.cpp); no other library reads it.
                session.env["ALT_AERON_FRAGMENT_LIMIT"] = str(limit)
                runs, error = [], None
                for _ in range(args.runs):
                    try:
                        runs.append(session.run_once(lib, case, args.poll))
                    except (TimeoutError, RuntimeError, KeyError, ValueError, ZeroDivisionError) as e:
                        error = str(e)
                        break
                entry = {"runs": runs, "buffer_bytes": buffer_bytes(lib, params["size"], buffer)}
                if args.batch > 1:
                    entry["batch"] = args.batch
                if lib == "aeron":
                    entry["conductor"] = args.aeron_conductor
                    entry["fragment_limit"] = limit
                if error:
                    entry["error"] = error
                if runs:
                    for metric in runs[0]:
                        entry[metric] = statistics.median(r[metric] for r in runs)
                report["results"].setdefault(case_id, {})[lib] = entry
                shown = "  ".join(f"{m}={fmt(m, entry.get(m))}" for m in runs[0]) if runs else ""
                print(f"{case_id:28} {lib:13} {shown}{'  ERROR: ' + error if error else ''}", flush=True)
                save()
    finally:
        session.close()


if __name__ == "__main__":
    main()
