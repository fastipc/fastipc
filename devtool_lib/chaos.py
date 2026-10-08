"""
Chaos (kill-and-restart) harness: `python devtool.py test chaos [--scenario S] [--cycles N] [--seed S] [--jobs N]`.

A heavy, opt-in check (CONTRIBUTING.md, "Tests"): not part of `devtool test all`, the PR gates or CI. Run it at the
end of a batch of risky changes and before releases. A smoke run (about 30-100 cycles per scenario) takes a few
minutes per OS; the full run (>= 1,000 cycles per scenario) is heavy validation.

The protocol (docs/protocol.md): a peer's end is reported when its process exits (EOF on the local control
connection), so DISCONNECTED arrives in milliseconds - no heartbeat, no timeout, and nothing left behind after a
crash. What the harness hunts: a survivor that never learns of a replacement, a stale ring index stored into a new
session, a newcomer that joins a live session, and a crash while connecting that wedges the next peer. None may
happen, so the xfail list (KNOWN) is empty and any failure fails the run.

Peers are the chaos peer (tests/chaos/chaos.c): one side each of a game platform's connection, through the public
header include/fipc.h - RPC only, 1 MiB rings, a producer thread on blocking `fipc_rpc_submit`, a consumer thread on
`fipc_rpc_recv(100 ms)`, recovery = a new connection. A send reports the peer's end at once and only stops the
producer; the consumer reports DISCONNECTED after it has received every message the peer completed, so every cycle
also checks that receives deliver those before the end (include/fipc.h). Roles are fixed: a `server` listens
(`fipc_listen`) and accepts (`fipc_accept`), keeping its listener until the connection ends; a `client` connects
(`fipc_connect`). Two policies: `unity` waits again after a timeout (accept or connect, 5000 ms) and makes a new
connection after a disconnect; `game` makes 3 attempts, then gives up (GAVE_UP). Both send and verify every message
(sender, session, sequence, request id, length, contents, checksum).

The driver kills peers hard (a peer's own thread calls `kill(SIGKILL)` / `TerminateProcess` at a seeded random point
of a chosen phase: init, handshake or cleanup 10 us - 5 ms after it starts, or traffic - which includes the middle
of messages of several pieces and RPC calls - 0-400 ms after the verified exchange), crashes them for real (a null
dereference or abort(), the OS's default crash handling left in place), ends them deterministically at a named
lifecycle step (the crash-at-step hook, FASTIPC_TEST_CRASH_AT), suspends them (SIGSTOP / NtSuspendProcess), restarts
them and checks each cycle:
- detection: a survivor that was connected to the dead (or paused, or departed) peer reports DISCONNECTED within
  DETECT_MS (1 s: EOF is near-instant, the margin covers a loaded machine); for a restart, the cycle must end in
  DISCONNECTED or a completed reconnect, never a silent wedge. The real-crash scenarios don't gate the detection
  time (they measure the OS crash handlers, not the library) - a slow or missing report there is a note, not a
  failure - but still forbid corruption, aborts and true hangs.
- reconnect: the restarted peer and the survivor exchange verified messages (6 each way, one of several pieces)
  within the accept/connect timeout + 3 s (8 s by default) of the restart;
- the watchdog: no peer spins (>= 0.5 cores for 2 s without receiving a message), aborts or exits unexpectedly, no
  message is lost, repeated, stale or corrupt;
- leaks: after all processes of a pair left gracefully, no named object of the test remains (nothing is named but
  the rendezvous: a per-user abstract socket that vanishes with the process on Linux, the fastipc-* named pipe on
  Windows, the socket file and its lock file in the user's fastipc/ directory on macOS; the segment is an anonymous
  memfd, a randomly-named section that dies with its last handle, or a POSIX shared memory object whose name is
  removed at once). On macOS a listener that is killed or crashes leaves its two files, which the name's next
  listener clears: there the check applies only when a listener that closed gracefully came after the last server
  that ended otherwise, and the harness removes whatever a pair left once all its processes are gone. A
  long-lived survivor's open handles (Windows, GetProcessHandleCount) or file descriptors (Linux, macOS) must not trend up
  across its reconnects: they move in a band (its process-wide Io and worker threads are rebuilt per connection), so
  the check compares the band's floor in the later half of the run with the earlier half, given at least 8 samples
  (a run of about 16+ cycles).

Scenarios (the victim is the peer killed, paused or leaving; the survivor has the other role):
- kill-client, kill-server: a long-lived survivor (unity, gated); each cycle starts a victim (game) with a random kill
  point, and the survivor starts its next connection 0-500 ms after it reported the death (or, when it never
  connected to the victim, after the detection window), before the next victim starts.
- restart-client-fast, restart-server-fast: a fresh pair per cycle; the victim is killed during traffic and
  restarted 200 ms later.
- crash-client, crash-server: like the fast scenarios, but the victim crashes for real (a null dereference; abort
  with crash=abort) instead of a hard kill; the time from the crash to the survivor's DISCONNECTED is measured and
  reported, not gated.
- crash-at-step: a server survivor; each cycle a fresh client victim is ended at a named client step (connected,
  segment_received, attached, ready_sent, watching) by the crash-at-step hook, then a healthy client connects. Proves
  every step's crash is recoverable through the RPC path (the exhaustive per-step matrix, both roles, is
  tests/zig/native_peer_death_test.zig's).
- pause-client, pause-server: one side is suspended (200-800 ms, a seeded tenth of the cycles 1-5 s: with no
  heartbeat and no timeout, a pause longer than the peers' 100 ms receive timeout tests what a long one does), then
  resumed. In half the cycles nothing else happens,
  and the protocol must not declare the paused side dead (docs/protocol.md §7): no DISCONNECTED, and traffic
  resumes. In the other half the other side is killed and restarted during the pause: the resumed side must report
  DISCONNECTED at once (its EOF was queued) and pair with the replacement.
- takeover-mid-stream: the client streams messages of several pieces while the server drains slowly; the client is
  killed mid-stream and restarted 0-60 ms later, so its replacement arrives while the server is still receiving a
  message (where a stale index store would land in the new session).
- pause-mid-stream: the server streams messages of several pieces to a client that drains without pause; the client,
  likely mid-copy, is suspended as in pause-client - in half the cycles the streaming server is killed and
  replaced meanwhile (no corrupt or phantom message may arrive).
- rejoin-client-fast, rejoin-server-fast: one side leaves gracefully and makes a new connection 200 ms later in the
  same process (a Play-mode restart or an in-process reconnect: the newcomer must not join the old session).
- start-together: a server and a client start at once with no gating; the client's connect waits for the server to
  listen, and they pair.
- three-parties: a second client starts on a live pair's name and tries for 200-800 ms (a tenth of the cycles
  1-5 s): it must never join the live
  session (the server serves one client at a time, docs/protocol.md §3.2), and the pair's traffic stays intact; then
  the first client is killed, the server reports it at once, and the second client pairs with it.

Seeds: cycle k of a run with --seed S uses seed S + k for all its random choices (kill phase and point, restart
delays, message sizes), so `--scenario X --seed <cycle seed> --cycles 1` repeats a cycle (the scheduling of the two
processes still varies from run to run). A kill-client/kill-server cycle also depends on how the previous victim
died: repeat it with `--seed <previous cycle's seed> --cycles 2`.

Lanes: --jobs N (default 4) runs N scenarios at once, each in a driver process of its own (its pair names carry
that process's pid), the longest first; a scenario prints its cycles as one block when it ends, and the summary is
the same. Every check is per peer process (a survivor's handles or descriptors, a peer's own CPU time), so lanes
don't disturb each other's; detection takes milliseconds against its 1 s bound. --jobs 1 runs the scenarios one
after another in one process, printing each line as it comes. With 4 lanes the default run (1,050 cycles) takes
about 3 min per OS (one after another, about 11 min), the full run (--cycles 1000, 15,000 cycles) about 45 min.

The xfail list (KNOWN) is empty: any failure fails the run (exit 1). It is kept so that a bug found here and accepted
for a while can be recorded rather than silenced. A failure prints both peers' last STATUS line (their phase,
traffic counters, CPU time), their last events and stderr.

Options (devtool.py test chaos): --scenario (default: all), --cycles (default: 200 for kill-*, 50 for the rest),
--seed, --jobs (scenarios at once, 4), --ready-ms (the accept/connect timeout, 5000), --observe-ms (how long a failing
cycle is watched after its first problem, 1000), --trace (print every line the peers print).
"""

import multiprocessing
import os
import queue
import random
import signal
import statistics
import subprocess
import sys
import threading
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

from devtool_lib import rendezvous

IS_WINDOWS = os.name == "nt"
IS_MACOS = sys.platform == "darwin"
KILLED_CODES = (137,) if IS_WINDOWS else (-signal.SIGKILL,)  # a peer's self-kill (chaos.c: EXIT_KILLED)
CRASH_STEP_CODE = 99  # the crash-at-step hook's exit code (control.zig test_hooks.crashIfAt)
RESOURCE_SLACK = 2  # how far the floor of a survivor's open handles/descriptors may rise (keep_resources)
MIN_RESOURCE_SAMPLES = 8  # steady samples (after two warm-up connections) the resource check needs to judge a trend
SPIN_CORES = 0.5  # CPU use that counts as a spin, when no message arrives for SPIN_WINDOW_MS
SPIN_WINDOW_MS = 2000
DETECT_MS = 1000  # a survivor must report DISCONNECTED within this of an EOF-visible death (near-instant)
CRASH_EXIT_S = 60  # how long a real crash may take to end its process, after the kill point (the OS's crash handling)
# Pauses and three-parties' wait: mostly short, a seeded share long. No heartbeat and no timeout end a session, so a
# pause tests the same thing at 200 ms as at 5 s once it outlasts the peers' 100 ms receive timeout.
SHORT_WAIT_MS = (200, 800)
LONG_WAIT_MS = (1000, 5000)
LONG_WAIT_SHARE = 0.1
DEFAULT_JOBS = 4  # scenarios run at once (--jobs)
# The client's lifecycle steps the crash-at-step scenario ends a client at (control.zig test_hooks.Step)
CRASH_STEPS = ("connected", "segment_received", "attached", "ready_sent", "watching")


# ===== The peer process =====

class Peer:
    """A chaos peer process; its stdout goes line by line to the shared event queue as
    (time.monotonic(), peer, line), then (time, peer, Peer.EXITED) when it ends. `env` overrides are added to the
    inherited environment (the crash-at-step scenario sets FASTIPC_TEST_CRASH_AT)."""

    EXITED = "<exited>"

    def __init__(self, exe: Path, args: List[str], label: str, events: "queue.Queue",
                 env: Optional[Dict[str, str]] = None):
        self.label = label
        self.events = events
        child_env = None
        if env:
            child_env = os.environ.copy()
            child_env.update(env)
        self.proc = subprocess.Popen(
            [str(exe), *args],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=child_env,
        )
        self.stdout: List[str] = []
        self.stderr: List[str] = []
        self._pumps = [
            threading.Thread(target=self._pump, args=(self.proc.stdout, self.stdout, True), daemon=True),
            threading.Thread(target=self._pump, args=(self.proc.stderr, self.stderr, False), daemon=True),
            threading.Thread(target=self._report_exit, daemon=True),
        ]
        for pump in self._pumps:
            pump.start()

    def _pump(self, stream, sink: List[str], to_events: bool):
        for line in stream:
            sink.append(line.rstrip())
            if to_events:
                self.events.put((time.monotonic(), self, line.strip()))

    def _report_exit(self):
        self.proc.wait()
        self.events.put((time.monotonic(), self, Peer.EXITED))

    def send(self, line: str) -> bool:
        """Writes a line to the peer's stdin; False if the peer is gone."""
        try:
            self.proc.stdin.write(line + "\n")
            self.proc.stdin.flush()
            return True
        except (OSError, ValueError):
            return False

    def finish(self, timeout_s: float) -> Optional[int]:
        """Exit code, or None if the peer had to be killed."""
        try:
            code = self.proc.wait(timeout=timeout_s)
        except subprocess.TimeoutExpired:
            self.kill()
            code = None
        for pump in self._pumps:
            pump.join(timeout=5)
        return code

    def kill(self):
        if self.proc.poll() is None:
            self.proc.kill()
            self.proc.wait()


# ===== Scenarios =====

@dataclass(frozen=True)
class Scenario:
    name: str
    family: str  # "restart", "fast", "crash", "crashstep", "pause", "takeover", "rejoin"
    victim: str  # the role of the peer that is killed, paused or leaves: "client" or "server"
    cycles: int  # default number of cycles
    summary: str
    victim_traffic: str = "mixed"  # chaos.c traffic=
    survivor_traffic: str = "mixed"
    gated_detection: bool = True  # a missing/slow DISCONNECTED is a failure (off for real crashes: it measures the OS)
    typical_s: float = 0.5  # a cycle's usual duration, to start the longest scenarios first when lanes run in parallel

    @property
    def survivor_role(self) -> str:
        return "server" if self.victim == "client" else "client"


SCENARIOS: Dict[str, Scenario] = {s.name: s for s in [
    Scenario("kill-client", "restart", "client", 200, "kill the client at a random point, restart it after the death was detected",
             typical_s=0.7),
    Scenario("kill-server", "restart", "server", 200, "kill the server at a random point, restart it after the death was detected",
             typical_s=0.8),
    Scenario("restart-client-fast", "fast", "client", 50, "kill the client during traffic, restart it 200 ms later"),
    Scenario("restart-server-fast", "fast", "server", 50, "kill the server during traffic, restart it 200 ms later"),
    Scenario("crash-client", "crash", "client", 50, "crash the client for real during traffic, restart it 200 ms later", gated_detection=False,
             typical_s=0.8),  # WSL captures a crash slowly (about 1 s a cycle)
    Scenario("crash-server", "crash", "server", 50, "crash the server for real during traffic, restart it 200 ms later", gated_detection=False,
             typical_s=0.8),
    Scenario("crash-at-step", "crashstep", "client", 50, "end a fresh client at each of its steps (the crash-at-step hook)",
             typical_s=0.1),
    Scenario("pause-client", "pause", "client", 50, "suspend the client (mostly 200-800 ms); in half the cycles the server restarts meanwhile",
             typical_s=1.3),
    Scenario("pause-server", "pause", "server", 50, "suspend the server (mostly 200-800 ms); in half the cycles the client restarts meanwhile",
             typical_s=1.3),
    Scenario("takeover-mid-stream", "takeover", "client", 50, "kill a client streaming messages of several pieces, restart it 0-60 ms later",
             victim_traffic="stream", survivor_traffic="drain"),
    Scenario("pause-mid-stream", "pause", "client", 50, "suspend a client draining a stream of several pieces; half the cycles the server restarts meanwhile",
             victim_traffic="sink", survivor_traffic="stream", typical_s=1.3),
    Scenario("rejoin-client-fast", "rejoin", "client", 50, "the client leaves gracefully and connects again 200 ms later in-process"),
    Scenario("rejoin-server-fast", "rejoin", "server", 50, "the server leaves gracefully and listens again 200 ms later in-process"),
    Scenario("start-together", "together", "client", 50, "a server and a client start at once: they pair",
             typical_s=0.1),
    Scenario("three-parties", "three", "client", 50, "a second client on a live pair's name never joins; it pairs once the first dies",
             typical_s=0.75),
]}


@dataclass
class Params:
    """One cycle's random choices, all from its seed."""
    seed: int
    kill_phase: str = "traffic"
    kill_us: int = 0
    leave_ms: int = -1    # cleanup kills and rejoins: traffic after the verified exchange before leaving
    restart_ms: int = 0   # delay from the death (or the survivor's report) to the restart
    pause_after_ms: int = 0
    pause_ms: int = 500   # how long a pause lasts (wait_ms: mostly 200-800 ms, a tenth 1-5 s)
    restart_other: bool = False  # pause: the other side is killed and restarted during the pause
    crash_method: str = ""  # chaos.c crash=: "segv" or "abort" for the real-crash scenarios
    crash_step: str = ""    # crash-at-step: the lifecycle step (FASTIPC_TEST_CRASH_AT) this cycle ends the victim at
    third_wait_ms: int = 0  # three-parties: how long the third stream tries the live pair's name before the victim dies

    def describe(self, sc: Scenario) -> str:
        if sc.family == "crashstep":
            return f"crash at {self.crash_step}"
        if sc.family == "together":
            return "both start at once"
        if sc.family == "three":
            return f"a second client tries for {self.third_wait_ms} ms, then the first is killed"
        if sc.family == "pause":
            other = ", the other side restarts during it" if self.restart_other else ""
            return f"pause {self.pause_ms} ms, {self.pause_after_ms} ms after the verified exchange{other}"
        if sc.family == "rejoin":
            return f"leave {self.leave_ms} ms after the verified exchange, rejoin after {self.restart_ms} ms"
        if sc.family == "crash":
            return f"crash={self.crash_method} +{self.kill_us}us, restart +{self.restart_ms} ms"
        text = f"kill={self.kill_phase}+{self.kill_us}us"
        if self.kill_phase == "cleanup":
            text += f" (leave after {self.leave_ms} ms)"
        return text + f", restart +{self.restart_ms} ms"


def wait_ms(rng: random.Random) -> int:
    """A pause's length, or three-parties' wait: SHORT_WAIT_MS, or LONG_WAIT_MS in a seeded LONG_WAIT_SHARE of
    the cycles, so long pauses keep their coverage."""
    low, high = LONG_WAIT_MS if rng.random() < LONG_WAIT_SHARE else SHORT_WAIT_MS
    return rng.randint(low, high)


def derive_params(sc: Scenario, seed: int) -> Params:
    rng = random.Random(seed)
    p = Params(seed=seed)
    if sc.family == "restart":
        p.kill_phase = rng.choices(["init", "handshake", "traffic", "cleanup"], weights=[15, 15, 55, 15])[0]
        # init, handshake and cleanup last from well under a millisecond (a Windows client whose server waits) to a
        # few (WSL): log-uniform delays of 10 us - 5 ms reach into all of them
        short_us = int(10 ** rng.uniform(1, 3.7))
        p.kill_us = rng.randint(0, 400_000) if p.kill_phase == "traffic" else short_us
        p.leave_ms = rng.randint(0, 300) if p.kill_phase == "cleanup" else -1
        p.restart_ms = rng.randint(0, 500)  # restart after 0-500 ms
    elif sc.family == "fast":
        p.kill_us = rng.randint(0, 400_000)
        p.restart_ms = 200
    elif sc.family == "crash":
        p.kill_us = rng.randint(0, 400_000)
        p.restart_ms = 200
        p.crash_method = rng.choice(["segv", "abort"])
    elif sc.family == "crashstep":
        p.crash_step = CRASH_STEPS[seed % len(CRASH_STEPS)]  # every step in turn across cycles
    elif sc.family == "takeover":
        p.kill_us = rng.randint(20_000, 300_000)
        p.restart_ms = rng.randint(0, 60)  # + process start: lands within the survivor's 100 ms receive timeout
    elif sc.family == "pause":
        p.pause_after_ms = rng.randint(0, 300)
        p.pause_ms = wait_ms(rng)
        p.restart_other = rng.random() < 0.5  # half the cycles: the plan's "while the other restarts"
    elif sc.family == "rejoin":
        p.leave_ms = rng.randint(0, 300)
        p.restart_ms = 200
    elif sc.family == "three":
        p.third_wait_ms = wait_ms(rng)
    return p


# ===== Platform helpers =====

if IS_WINDOWS:
    import ctypes
    from ctypes import wintypes

    _k32 = ctypes.WinDLL("kernel32", use_last_error=True)
    _ntdll = ctypes.WinDLL("ntdll")
    _k32.CloseHandle.argtypes = [wintypes.HANDLE]
    _k32.OpenProcess.restype = wintypes.HANDLE
    _k32.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    _k32.CreateFileW.restype = wintypes.HANDLE
    _k32.CreateFileW.argtypes = [wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD, ctypes.c_void_p,
                                 wintypes.DWORD, wintypes.DWORD, wintypes.HANDLE]
    _k32.ProcessIdToSessionId.argtypes = [wintypes.DWORD, ctypes.POINTER(wintypes.DWORD)]
    _ntdll.NtSuspendProcess.argtypes = [wintypes.HANDLE]
    _ntdll.NtResumeProcess.argtypes = [wintypes.HANDLE]
    PROCESS_SUSPEND_RESUME = 0x0800
    OPEN_EXISTING = 3
    INVALID_HANDLE = wintypes.HANDLE(-1).value
    ERROR_FILE_NOT_FOUND = 2


def suspend(pid: int, resume: bool = False):
    """Stops (or continues) every thread of the process: SIGSTOP/SIGCONT, NtSuspendProcess/NtResumeProcess."""
    if not IS_WINDOWS:
        os.kill(pid, signal.SIGCONT if resume else signal.SIGSTOP)
        return
    handle = _k32.OpenProcess(PROCESS_SUSPEND_RESUME, False, pid)
    if not handle:
        return
    try:
        (_ntdll.NtResumeProcess if resume else _ntdll.NtSuspendProcess)(handle)
    finally:
        _k32.CloseHandle(handle)


def leftover_names(name: str) -> List[str]:
    """Named objects of the pair `name` that still exist. The protocol leaves nothing behind: on Linux the segment is
    an anonymous memfd and the rendezvous an abstract socket, both gone with the process (so /dev/shm holds nothing);
    on Windows the section and events are randomly named and die with their last handle, and the one named object is
    the rendezvous pipe fastipc-<session>-<name>, gone once the last holder exits. On macOS the rendezvous is the
    socket file and its lock file (devtool_lib/rendezvous.py), which a listener removes when it closes and a killed
    one leaves behind (Board.listener_ended_last). Any other hit is a leak."""
    if IS_MACOS:
        return [path for path in rendezvous.macos_files(name) if os.path.lexists(path)]
    if not IS_WINDOWS:
        # A guard against a regression that resurrects a named segment; protocols 3 and 4 never create one.
        try:
            return [f"/dev/shm/{e}" for e in os.listdir("/dev/shm") if name in e]
        except OSError:
            return []
    session = wintypes.DWORD(0)
    if not _k32.ProcessIdToSessionId(os.getpid(), ctypes.byref(session)):
        return []
    pipe = f"\\\\.\\pipe\\fastipc-{session.value}-{name}"  # the un-hashed form (harness names are short)
    handle = _k32.CreateFileW(pipe, 0, 0, None, OPEN_EXISTING, 0, None)
    if handle != INVALID_HANDLE:
        _k32.CloseHandle(handle)
        return [pipe]
    # ERROR_PIPE_BUSY (231) means the instance exists but has a client; anything but "not found" means it is there
    return [] if ctypes.get_last_error() == ERROR_FILE_NOT_FOUND else [pipe]


# ===== Peers and their events =====

def parse_fields(line: str) -> Tuple[str, Dict[str, str]]:
    word, _, rest = line.partition(" ")
    fields = {}
    for token in rest.split():
        key, sep, value = token.partition("=")
        if sep:
            fields[key] = value
    return word, fields


class PeerState:
    """A chaos peer and what it reported."""

    def __init__(self, peer: Peer, role: str, policy: str):
        self.peer = peer
        self.label = peer.label
        self.role = role
        self.policy = policy
        self.pid = peer.proc.pid
        self.events: List[Tuple[float, str, Dict[str, str], str]] = []  # (driver time, word, fields, line)
        self.status: Dict[str, str] = {}
        self.status_line = ""
        self.samples: List[Tuple[int, int, int]] = []  # (peer clock ms, cpu ms, rx)
        self.spin: Optional[str] = None
        self.exit_time: Optional[float] = None
        self.exit_code: Optional[int] = None
        self.suspended = False
        self.resources: List[int] = []  # open handles / fds at each VERIFIED (the next STATUS after it)
        self._want_resources = False

    @property
    def alive(self) -> bool:
        return self.exit_time is None

    def in_connection(self) -> bool:
        """Whether it is inside a connection's life: it printed INIT (a server listens, a client connects) and hasn't
        printed CLOSED since."""
        for _, word, _, _ in reversed(self.events):
            if word == "INIT":
                return True
            if word in ("CLOSED", "START"):
                return False
        return False

    def find(self, word: str, after: float = 0.0, **match) -> Optional[Tuple[float, Dict[str, str], str]]:
        for t, w, fields, line in self.events:
            if w == word and t >= after and all(fields.get(k) == str(v) for k, v in match.items()):
                return t, fields, line
        return None

    def last(self, word: str) -> Optional[Tuple[float, Dict[str, str], str]]:
        for t, w, fields, line in reversed(self.events):
            if w == word:
                return t, fields, line
        return None

    def on_status(self, fields: Dict[str, str], line: str):
        self.status, self.status_line = fields, line
        try:
            sample = (int(fields["t"]), int(fields["cpu"]), int(fields["rx"]))
        except (KeyError, ValueError):
            return
        if self._want_resources and fields.get("res", "-1") != "-1":
            self.resources.append(int(fields["res"]))
            self._want_resources = False
        self.samples.append(sample)
        del self.samples[:-40]
        for t0, cpu0, rx0 in reversed(self.samples[:-1]):
            if sample[0] - t0 >= SPIN_WINDOW_MS:
                cores = (sample[1] - cpu0) / (sample[0] - t0)
                if cores >= SPIN_CORES and sample[2] == rx0 and not self.spin:
                    self.spin = f"{cores:.2f} cores for {sample[0] - t0} ms without receiving a message"
                break

    def describe(self) -> str:
        state = "alive" if self.alive else f"exited with {self.exit_code}"
        lines = [f"    {self.label} (pid {self.pid}, {self.role}, {self.policy}): {state}"
                 f"{', SUSPENDED' if self.suspended else ''}"]
        if self.status_line:
            lines.append(f"      last {self.status_line}")
        lines += [f"      event: {line}" for _, _, _, line in self.events[-8:]]
        lines += [f"      stderr: {line}" for line in self.peer.stderr[-6:]]
        return "\n".join(lines)


class Board:
    """The running peers of one pair, and one queue of all their events. `out` prints a line (Harness.out)."""

    def __init__(self, settings: "Settings", name: str, out: Callable[[str], None]):
        self.s = settings
        self.name = name
        self.out = out
        self.q: "queue.Queue" = queue.Queue()
        self.peers: List[PeerState] = []
        self._by_peer: Dict[Peer, PeerState] = {}

    def start(self, role: str, policy: str, label: str, env: Optional[Dict[str, str]] = None,
              **options) -> PeerState:
        exe = self.s.bin_dir / f"chaos_peer{self.s.ext}"
        args = [role, policy, self.name, f"ready_ms={self.s.ready_ms}", f"ring={self.s.ring}"]
        args += [f"{key}={value}" for key, value in options.items()]
        peer = Peer(exe, args, label, events=self.q, env=env)
        ps = PeerState(peer, role, policy)
        self.peers.append(ps)
        self._by_peer[peer] = ps
        return ps

    def pump(self, timeout_s: float):
        """Processes the events that arrive within timeout_s (at least one wait)."""
        try:
            item = self.q.get(timeout=max(timeout_s, 0.0))
        except queue.Empty:
            return
        while True:
            self._dispatch(*item)
            try:
                item = self.q.get_nowait()
            except queue.Empty:
                return

    def _dispatch(self, t: float, peer: Peer, line):
        ps = self._by_peer[peer]
        if self.s.trace:
            self.out(f"      {t % 1000:8.3f} {ps.label} [{ps.pid}]: {line if line != Peer.EXITED else 'exited'}"
                     f"{'' if line != Peer.EXITED else ' with ' + str(peer.proc.returncode)}")
        if line == Peer.EXITED:
            ps.exit_time, ps.exit_code = t, peer.proc.returncode
            return
        word, fields = parse_fields(line)
        if word == "STATUS":
            ps.on_status(fields, line)
            return
        if word == "VERIFIED":
            ps._want_resources = True
        ps.events.append((t, word, fields, line))

    def wait(self, predicate: Callable[[], bool], deadline: float) -> bool:
        while not predicate():
            now = time.monotonic()
            if now >= deadline:
                return predicate()
            self.pump(min(0.05, deadline - now))
        return True

    def sleep(self, seconds: float):
        end = time.monotonic() + seconds
        self.wait(lambda: False, end)

    def teardown(self) -> Tuple[bool, List[str]]:
        """Stops every peer: STOP, then a hard kill after 6.5 s (a peer may sit in a 5 s accept or connect). Returns
        whether the leak check applies - every live peer left gracefully and one of them was inside a connection's
        life, so the last one out left gracefully (on macOS, also: the name's files can't be a killed listener's) -
        and the named objects left behind."""
        live = [ps for ps in self.peers if ps.alive]
        holder = any(ps.in_connection() for ps in live)
        for ps in live:
            if ps.suspended:
                suspend(ps.pid, resume=True)
                ps.suspended = False
            ps.peer.send("STOP")
        self.wait(lambda: all(not ps.alive for ps in live), time.monotonic() + 6.5)
        graceful = True
        for ps in self.peers:
            if ps.alive:
                graceful = False
                ps.peer.kill()
            ps.peer.finish(5)  # joins its pumps: every event of every peer is queued now
        self.pump(0)
        graceful = graceful and all(ps.exit_code == 0 for ps in live)
        checked = graceful and holder
        if IS_MACOS and self.listener_ended_last():
            checked = False  # its socket and lock file may remain, for the name's next listener to clear
        leaks: List[str] = []
        if checked:
            # Windows may need a moment until the kernel closes a terminated process's handles
            deadline = time.monotonic() + 2
            while (leaks := leftover_names(self.name)) and time.monotonic() < deadline:
                time.sleep(0.1)
        if IS_MACOS:
            # Every process of the pair is gone, so no listener holds the lock: what is left can go
            for path in leftover_names(self.name):
                try:
                    os.unlink(path)
                except FileNotFoundError:
                    pass
        return checked, leaks

    def listener_ended_last(self) -> bool:
        """macOS: whether the name's files may be a killed or crashed listener's: a server ended other than gracefully
        (exit code not 0), and no server that ended gracefully started listening (INIT) after the last such end."""
        servers = [ps for ps in self.peers if ps.role == "server" and ps.exit_time is not None]
        ended = [ps.exit_time for ps in servers if ps.exit_code != 0]
        if not ended:
            return False
        listened = [t for ps in servers if ps.exit_code == 0 for t, word, _, _ in ps.events if word == "INIT"]
        return not listened or max(listened) < max(ended)


# ===== Problems and cycle results =====

@dataclass
class Problem:
    kind: str       # no-detection, late-detection, no-reconnect, bad-message, session-error, spin, exit, gave-up,
                    # victim-exit, setup, leak, false-death, no-progress, joined-live-session
    who: str
    detail: str
    stderr: str = ""

    def __str__(self) -> str:
        return f"{self.kind} ({self.who}): {self.detail}"


@dataclass
class CycleResult:
    scenario: str
    cycle: int
    seed: int
    params: str
    problems: List[Problem] = field(default_factory=list)
    known: List[str] = field(default_factory=list)
    detect_ms: Optional[float] = None
    reconnect_s: Optional[float] = None
    leak_checked: bool = False
    leaks: List[str] = field(default_factory=list)
    notes: List[str] = field(default_factory=list)
    report: str = ""
    duration_s: float = 0.0
    prev_seed: Optional[int] = None
    extra: bool = False  # not a cycle: the end of a run, or a survivor's resource check

    @property
    def outcome(self) -> str:
        if not self.problems:
            return "PASS"
        return "XFAIL" if self.known else "FAIL"


# ===== Known failures (the xfail list) =====

@dataclass(frozen=True)
class Known:
    """A finding that a problem is an accepted, already-explained failure: in these scenarios, of these kinds, when
    its evidence holds. Empty: any failure fails the run. Kept so that a bug found here and accepted for a while can be
    recorded, with its evidence, instead of being silenced."""
    finding: str
    scenarios: Tuple[str, ...]
    kinds: Tuple[str, ...]
    evidence: Callable[[Problem, CycleResult], bool]


KNOWN: List[Known] = []  # no expected failures


def classify(result: CycleResult):
    """A failure is known only when every problem is explained by a KNOWN entry for this scenario (none today)."""
    matched: List[str] = []
    for problem in result.problems:
        entry = next((k for k in KNOWN if result.scenario in k.scenarios and problem.kind in k.kinds
                      and k.evidence(problem, result)), None)
        if entry is None:
            result.known = []
            return
        if entry.finding not in matched:
            matched.append(entry.finding)
    result.known = matched


# ===== The harness =====

@dataclass
class Settings:
    bin_dir: Path
    ext: str
    ready_ms: int = 5000  # the peers' accept and connect timeout
    ring: int = 1 << 20
    observe_ms: int = 1000
    trace: bool = False  # print every event line (STATUS too) as it arrives

    @property
    def detect_s(self) -> float:
        return DETECT_MS / 1000  # EOF is near-instant; the margin covers a loaded machine

    @property
    def reconnect_s(self) -> float:
        return (self.ready_ms + 3000) / 1000


class Cycle:
    """Watches one cycle's peers: problems as they happen (bad messages, session errors, spins, unexpected exits)
    and at deadlines; after the first one it keeps observing for observe_ms, for evidence."""

    def __init__(self, harness: "Harness", board: Board, result: CycleResult):
        self.h, self.board, self.result = harness, board, result
        self.watched: Dict[PeerState, Tuple[int, ...]] = {}  # peer -> exit codes that are no problem
        self._seen: Dict[PeerState, int] = {}
        self.failed_at: Optional[float] = None

    def watch(self, ps: PeerState, exit_codes: Optional[Tuple[int, ...]] = ()):
        # exit_codes: codes that are no problem; None means any exit is expected (a real crash: the code varies)
        self.watched[ps] = exit_codes
        self._seen.setdefault(ps, len(ps.events))

    def problem(self, kind: str, who: PeerState, detail: str):
        stderr = "\n".join(who.peer.stderr[-20:])
        self.result.problems.append(Problem(kind, who.label, detail, stderr))
        if self.failed_at is None:
            self.failed_at = time.monotonic()

    def _has(self, kind: str, who: PeerState) -> bool:
        return any(p.kind == kind and p.who == who.label for p in self.result.problems)

    def scan(self):
        """Event-driven problems."""
        for ps, exit_codes in self.watched.items():
            for _, word, fields, line in ps.events[self._seen[ps]:]:
                if word == "BAD_MESSAGE":
                    self.problem("bad-message", ps, line)
                elif word == "SESSION_ERROR":
                    self.problem("session-error", ps, line)
                elif word == "GAVE_UP":
                    self.problem("gave-up", ps, line)
            self._seen[ps] = len(ps.events)
            if ps.spin and not self._has("spin", ps):
                self.problem("spin", ps, ps.spin)
            if exit_codes is not None and not ps.alive and ps.exit_code not in exit_codes \
                    and not self._has("exit", ps) and not self._has("gave-up", ps):
                self.problem("exit", ps, f"exited with {ps.exit_code}")

    def wait(self, predicate: Callable[[], bool], timeout_s: float) -> bool:
        """Waits for predicate until timeout_s, or until a problem was found and observed for observe_ms."""
        deadline = time.monotonic() + max(timeout_s, 0.0)

        def done() -> bool:
            self.scan()
            if self.failed_at is not None and time.monotonic() >= self.failed_at + self.h.s.observe_ms / 1000:
                return True
            return predicate()

        self.board.wait(done, deadline)
        return predicate()

    @property
    def failed(self) -> bool:
        return self.failed_at is not None

    def observe(self):
        """After a problem: collect evidence for observe_ms."""
        if self.failed_at is not None:
            self.wait(lambda: False, self.failed_at + self.h.s.observe_ms / 1000 - time.monotonic())


def verified_with(a: PeerState, b: PeerState, after: float) -> bool:
    return a.find("VERIFIED", after, peer=b.pid) is not None


def release(ps: PeerState):
    """Closes a dead peer's pipes once its output is read: a long-lived survivor's board would otherwise keep three
    descriptors per victim."""
    ps.peer.finish(5)
    for stream in (ps.peer.proc.stdin, ps.peer.proc.stdout, ps.peer.proc.stderr):
        try:
            if stream is not None:
                stream.close()
        except OSError:
            pass


class Harness:
    """Runs scenarios and records their cycles. `buffered`: lines are kept in `lines` (a lane's process prints its
    scenario as one block when it ends) instead of printed as they come."""

    def __init__(self, settings: Settings, seed: int, buffered: bool = False):
        self.s = settings
        self.seed = seed
        self.results: List[CycleResult] = []
        self.leak_checks: Dict[str, List[int]] = {}  # scenario -> [checked, leaked]
        self.survivor_resources: List[Tuple[str, List[int]]] = []  # (scenario, one survivor's counts)
        self._evidence_shown: set = set()
        self.lines: Optional[List[str]] = [] if buffered else None

    # ----- output -----

    def out(self, text: str):
        if self.lines is None:
            print(text, flush=True)
        else:
            self.lines.append(text)

    def record(self, result: CycleResult):
        classify(result)
        self.results.append(result)
        tag = {"PASS": "PASS ", "XFAIL": "XFAIL", "FAIL": "FAIL "}[result.outcome]
        extra = []
        if result.detect_ms is not None:
            extra.append(f"detected in {result.detect_ms:.0f} ms")
        if result.reconnect_s is not None:
            extra.append(f"reconnected in {result.reconnect_s * 1000:.0f} ms")
        extra += result.notes
        self.out(f"  {tag} {result.scenario:<21} #{result.cycle:<4} seed {result.seed:<8} {result.params}"
                 f"{'; ' + '; '.join(extra) if extra else ''}  ({result.duration_s:.1f} s)")
        if result.outcome == "XFAIL":
            self.out(f"        expected failure: {', '.join(result.known)}: " + "; ".join(map(str, result.problems)))
            key = (result.scenario, tuple(result.known))
            if key not in self._evidence_shown and result.report:  # the evidence, once per scenario and finding
                self._evidence_shown.add(key)
                self.out(result.report)
        elif result.outcome == "FAIL":
            for problem in result.problems:
                self.out(f"        {problem}")
            if result.report:
                self.out(result.report)
            repro = f"python devtool.py test chaos --scenario {result.scenario} --seed {result.seed} --cycles 1"
            if result.prev_seed is not None:
                repro += f"   (after the previous victim: --seed {result.prev_seed} --cycles 2)"
            self.out(f"        repeat: {repro}")

    def snapshot(self, board: Board) -> str:
        first = [ps for ps in board.peers if ps.label == "survivor"][:1] or board.peers[:1]
        shown = first + [ps for ps in board.peers[-3:] if ps not in first]
        return "\n".join(ps.describe() for ps in shown)

    def pair_name(self, sc: Scenario, cycle: int) -> str:
        """The pair's rendezvous name: unique to the driver's process (a lane runs in a process of its own), the
        scenario and the cycle, plus a random part."""
        return f"kr{os.getpid()}_{sc.name.replace('-', '')[:10]}_{cycle}_{random.randrange(1 << 16):x}"

    def teardown(self, sc: Scenario, board: Board, result: CycleResult):
        """Stops the pair; when every live peer left gracefully and one of them held the segment, nothing of it
        may remain."""
        if result.problems and not result.report:
            result.report = self.snapshot(board)
        checked, leaks = board.teardown()
        counts = self.leak_checks.setdefault(sc.name, [0, 0])
        if checked:
            result.leak_checked = True
            result.leaks = leaks
            counts[0] += 1
            if leaks:
                counts[1] += 1
                result.problems.append(Problem("leak", "pair", "left after every process exited: " + ", ".join(leaks)))

    def start(self, board: Board, role: str, policy: str, label: str, options: Dict[str, object],
              env: Optional[Dict[str, str]] = None) -> PeerState:
        return board.start(role, policy, label, env=env, **{k: v for k, v in options.items() if v is not None})

    # ----- restart scenarios: a long-lived survivor -----

    def run_restart(self, sc: Scenario, cycles: int):
        board: Optional[Board] = None
        survivor: Optional[PeerState] = None
        prev_seed: Optional[int] = None
        for cycle in range(cycles):
            seed = self.seed + cycle
            params = derive_params(sc, seed)
            started = time.monotonic()
            result = CycleResult(sc.name, cycle, seed, params.describe(sc), prev_seed=prev_seed)
            if board is None:
                board, result.prev_seed = Board(self.s, self.pair_name(sc, cycle), self.out), None
                survivor = self.start(board, sc.survivor_role, "unity", "survivor", {"seed": seed * 2 + 1, "gate": 1})
                if not board.wait(lambda: survivor.find("INIT") is not None, time.monotonic() + 10):
                    result.problems.append(Problem("setup", "survivor", "no INIT within 10 s"))
            if not result.problems:
                self.restart_cycle(sc, board, survivor, params, result)
            if result.problems:  # start over with a fresh pair
                self.teardown(sc, board, result)
                self.keep_resources(sc, survivor)
                board, survivor = None, None
            result.duration_s = time.monotonic() - started
            self.record(result)
            prev_seed = seed
        if board is not None:
            end = CycleResult(sc.name, cycles, self.seed + cycles, "end of run: the survivor leaves", extra=True)
            if survivor is not None and survivor.alive and not survivor.in_connection():
                # Let a survivor waiting at its gate start its next connection, so the last one out leaves gracefully
                gate = survivor.last("GATE")
                survivor.peer.send("GO")
                board.wait(lambda: gate is None or survivor.find("INIT", gate[0]) is not None, time.monotonic() + 20)
            self.teardown(sc, board, end)
            self.keep_resources(sc, survivor)
            if survivor is not None and survivor.exit_code != 0:
                end.problems.append(Problem("exit", "survivor", f"didn't leave on STOP (exit code {survivor.exit_code})"))
            if end.problems:
                self.record(end)

    def keep_resources(self, sc: Scenario, survivor: Optional[PeerState]):
        """A survivor's open handles or descriptors at its verified connections, after two warm-up connections. Its
        only connection closes every cycle, so the process-wide Io and its worker threads are rebuilt per connection
        (the process-wide Io's workers exit after the release), and a sample can catch a worker handle that closes a
        moment late: the counts move in a band (Windows: 74-84 over 20 connections). A leak grows with the connections, so it raises the
        band's floor: the check compares the minimum of the later half of the samples with the minimum of the earlier
        half (RESOURCE_SLACK allows for the C runtime), and judges only with MIN_RESOURCE_SAMPLES steady samples."""
        if survivor is None or not survivor.resources:
            return
        counts = list(survivor.resources)
        self.survivor_resources.append((sc.name, counts))
        steady = counts[2:]
        if len(steady) < MIN_RESOURCE_SAMPLES:
            return
        half = len(steady) // 2
        early, late = min(steady[:half]), min(steady[half:])
        if late - early > RESOURCE_SLACK:
            what = "handles" if IS_WINDOWS else "file descriptors"
            result = CycleResult(sc.name, len(counts), self.seed, "a survivor's resources", extra=True)
            result.problems.append(Problem("leak", survivor.label, f"the floor of the open {what} rose from {early} "
                                           f"to {late} over {len(counts)} connections: {counts}"))
            self.record(result)

    def restart_cycle(self, sc: Scenario, board: Board, survivor: PeerState, params: Params, result: CycleResult):
        s = self.s
        cyc = Cycle(self, board, result)
        cyc.watch(survivor)
        t0 = time.monotonic()  # before the start: the victim's first lines can arrive before Popen returns
        victim = self.start(board, sc.victim, "game", f"victim#{result.cycle}",
                            {"seed": params.seed * 2, "kill": f"{params.kill_phase}:{params.kill_us}",
                             "leave": params.leave_ms if params.leave_ms >= 0 else None})
        cyc.watch(victim, KILLED_CODES)

        # A traffic or cleanup kill comes after the verified exchange: the restarted victim must reconnect
        if params.kill_phase in ("traffic", "cleanup"):
            if cyc.wait(lambda: verified_with(victim, survivor, t0) and verified_with(survivor, victim, t0),
                        s.reconnect_s):
                done = max(victim.find("VERIFIED", t0, peer=survivor.pid)[0],
                           survivor.find("VERIFIED", t0, peer=victim.pid)[0])
                result.reconnect_s = done - t0
                connected = victim.find("CONNECTED", t0)
                if connected and connected[1].get("attempt") != "1":
                    result.notes.append(f"connected at attempt {connected[1].get('attempt')}")
            elif not cyc.failed:
                cyc.problem("no-reconnect", victim, f"no verified exchange within {s.reconnect_s:.0f} s of its start")

        # The victim dies by its own kill
        if not cyc.failed:
            limit = s.reconnect_s + params.kill_us / 1e6 + max(params.leave_ms, 0) / 1000 + 5
            if not cyc.wait(lambda: not victim.alive, limit) and not cyc.failed:
                cyc.problem("victim-exit", victim, "still alive after its kill point")
        if cyc.failed:
            cyc.observe()
            return

        # Detection: required if the survivor exchanged with this victim (the protocol sends no PID, so the verified
        # exchange, whose VERIFIED carries the peer's pid, is the reliable "was connected to this victim" signal).
        t_death = victim.exit_time
        cyc.wait(lambda: survivor.find("DISCONNECTED", t0) is not None, t_death + s.detect_s - time.monotonic())
        disconnected = survivor.find("DISCONNECTED", t0)
        required = verified_with(survivor, victim, t0)
        if disconnected:
            result.detect_ms = (disconnected[0] - t_death) * 1000
            if disconnected[0] - t_death > s.detect_s:
                cyc.problem("late-detection", survivor, f"DISCONNECTED {result.detect_ms:.0f} ms after the death")
        elif required and not cyc.failed:
            cyc.problem("no-detection", survivor,
                        f"no DISCONNECTED within {s.detect_s * 1000:.0f} ms of the death of its peer")
        if not required:
            result.notes.append("the survivor never connected to it")

        # After the survivor's report it closes its connection and waits at its gate; it starts its next connection
        # (a server listens again, a client connects) the restart delay later, and the next victim starts once it
        # has. (After an undetectable death the survivor still waits in its accept or connect.)
        if disconnected and not cyc.failed:
            if cyc.wait(lambda: survivor.find("GATE", disconnected[0]) is not None, 10):
                board.sleep(params.restart_ms / 1000)
                gate = survivor.find("GATE", disconnected[0])
                survivor.peer.send("GO")
                if not cyc.wait(lambda: survivor.find("INIT", gate[0]) is not None, 20):
                    cyc.problem("no-reconnect", survivor, "no INIT within 20 s of GO")
            elif not cyc.failed:
                cyc.problem("no-reconnect", survivor, "didn't close its connection after DISCONNECTED")
        elif not cyc.failed:
            board.sleep(params.restart_ms / 1000)
        cyc.observe()
        if not victim.alive:
            release(victim)

    # ----- fresh-pair scenarios -----

    def run_fresh(self, sc: Scenario, cycles: int):
        for cycle in range(cycles):
            seed = self.seed + cycle
            params = derive_params(sc, seed)
            started = time.monotonic()
            result = CycleResult(sc.name, cycle, seed, params.describe(sc))
            board = Board(self.s, self.pair_name(sc, cycle), self.out)
            try:
                self.fresh_cycle(sc, board, params, result)
            finally:
                self.teardown(sc, board, result)
            result.duration_s = time.monotonic() - started
            self.record(result)

    def fresh_cycle(self, sc: Scenario, board: Board, params: Params, result: CycleResult):
        s = self.s
        cyc = Cycle(self, board, result)
        family = sc.family
        crashing = family == "crash"  # a real crash: the victim's exit code varies, and detection isn't gated
        victim_policy = "game" if family in ("fast", "takeover", "crash") else "unity"
        victim_opts: Dict[str, object] = {"seed": params.seed * 2}
        survivor_opts: Dict[str, object] = {"seed": params.seed * 2 + 1}
        if family in ("fast", "takeover", "crash"):
            victim_opts["kill"] = f"traffic:{params.kill_us}"
        if crashing:
            victim_opts["crash"] = params.crash_method
        if sc.victim_traffic != "mixed":
            victim_opts["traffic"] = sc.victim_traffic
        if sc.survivor_traffic != "mixed":
            survivor_opts["traffic"] = sc.survivor_traffic
        if family == "rejoin":
            victim_opts["leave"], victim_opts["rejoin"] = params.leave_ms, params.restart_ms

        def start_victim(label: str, **overrides) -> PeerState:
            options = {**victim_opts, **overrides}
            # A real crash exits with a code that varies (segv/abort, per OS): don't flag its exit; a hard kill has
            # the fixed EXIT_KILLED, and a healthy restart exits 0 (watched as ()).
            if not options.get("kill"):
                codes: Optional[Tuple[int, ...]] = ()
            elif crashing:
                codes = None
            else:
                codes = KILLED_CODES
            ps = self.start(board, sc.victim, victim_policy, label, options)
            cyc.watch(ps, codes)
            return ps

        def start_survivor(label: str = "survivor", **overrides) -> PeerState:
            ps = self.start(board, sc.survivor_role, "unity", label, {**survivor_opts, **overrides})
            cyc.watch(ps)
            return ps

        if family == "together":
            # Both sides start at once, with no gating: the client's connect waits for the server to listen, and
            # they pair.
            t_start = time.monotonic()
            a = start_survivor("server")
            b = start_victim("client")
            if cyc.wait(lambda: verified_with(a, b, t_start) and verified_with(b, a, t_start), s.reconnect_s):
                result.reconnect_s = max(a.find("VERIFIED", t_start, peer=b.pid)[0],
                                         b.find("VERIFIED", t_start, peer=a.pid)[0]) - t_start
            elif not cyc.failed:
                cyc.problem("no-reconnect", a, f"no verified exchange within {s.reconnect_s:.0f} s of starting together")
            cyc.observe()
            return

        # The server starts first, the victim or the survivor
        if sc.victim == "server":
            victim = start_victim("victim")
            if not cyc.wait(lambda: victim.find("INIT") is not None, 10):
                cyc.problem("setup", victim, "no INIT within 10 s")
                return
            survivor = start_survivor()
        else:
            survivor = start_survivor()
            if not cyc.wait(lambda: survivor.find("INIT") is not None, 10):
                cyc.problem("setup", survivor, "no INIT within 10 s")
                return
            victim = start_victim("victim")
        if not cyc.wait(lambda: verified_with(victim, survivor, 0) and verified_with(survivor, victim, 0),
                        s.reconnect_s):
            if not cyc.failed:
                cyc.problem("setup", victim, "the first connection didn't reach its verified exchange")
            cyc.observe()
            return

        if family in ("fast", "takeover", "crash"):
            # A real crash ends the process when the OS's crash handling lets it (WSL's crash capture writes a core
            # of about 90 MB to the Windows disk first, sometimes for more than 5 s): its bound is longer, and a slow
            # end is a note
            exit_s = CRASH_EXIT_S if crashing else 5
            if not cyc.wait(lambda: not victim.alive, params.kill_us / 1e6 + exit_s):
                if not cyc.failed:
                    cyc.problem("victim-exit", victim, "still alive after its kill point")
                cyc.observe()
                return
            t_event = victim.exit_time
            armed = victim.find("KILL_ARMED")
            if crashing and armed and t_event - armed[0] > params.kill_us / 1e6 + 5:
                result.notes.append(f"the crash ended the process {t_event - armed[0]:.1f} s after the kill was armed")
            board.sleep(max(0.0, t_event + params.restart_ms / 1000 - time.monotonic()))
            t_back = time.monotonic()
            newcomer = start_victim("restarted", kill=None, crash=None)
            self.verdict(cyc, survivor, t_event, newcomer, t_back, gated=sc.gated_detection,
                         since=armed[0] if armed else None)
            init = newcomer.find("INIT")
            if init is not None:
                result.notes.append(f"the newcomer's INIT came {(init[0] - t_event) * 1000:.0f} ms after the death")
        elif family == "pause":
            board.sleep(params.pause_after_ms / 1000)
            suspend(victim.pid)
            victim.suspended = True
            t_pause = time.monotonic()
            replacement: Optional[PeerState] = None
            if params.restart_other:
                # The other side dies (a hard kill from outside) a third of the way into the pause and a fresh one
                # starts at once, while the victim is still suspended: its EOF queues and must reach it on resume
                # (docs/protocol.md §7). For pause-mid-stream: a drainer paused mid-copy while its peer is replaced
                # must not store a stale index into the new session.
                board.sleep(params.pause_ms / 3000)
                cyc.watch(survivor, None)  # its exit is expected now
                survivor.peer.kill()
                replacement = start_survivor("replacement")
                board.sleep(max(0.0, t_pause + params.pause_ms / 1000 - time.monotonic()))
            else:
                board.sleep(params.pause_ms / 1000)
            suspend(victim.pid, resume=True)
            victim.suspended = False
            if replacement is not None:
                # The resumed victim reads the queued EOF at once (DISCONNECTED), then pairs with the replacement
                t_resume = time.monotonic()
                self.verdict(cyc, victim, t_resume, replacement, t_resume)
            else:
                self.pause_verdict(cyc, survivor, victim, t_pause)
        elif family == "three":
            # A second client on the name of a live pair must never join its session: the server serves one
            # client at a time, so its connect waits. The pair's own traffic stays intact meanwhile (scan() turns any
            # stale or mixed message into a problem). Once the victim (the first client) dies, the server learns it at
            # once and the third client pairs with it.
            t_third = time.monotonic()
            third = start_victim("third", seed=params.seed * 2 + 5)
            board.sleep(params.third_wait_ms / 1000)
            if third.find("CONNECTED", t_third) is not None:
                cyc.problem("joined-live-session", third, "a second client connected while the pair's session was live")
            if not cyc.failed:
                cyc.watch(victim, None)  # killed from outside: its exit is expected
                t_event = time.monotonic()
                victim.peer.kill()  # waits for the exit, which the survivor may have reported already
                self.verdict(cyc, survivor, t_event, third, t_event)
        elif family == "rejoin":
            if not cyc.wait(lambda: victim.find("LEFT") is not None, params.leave_ms / 1000 + 5):
                if not cyc.failed:
                    cyc.problem("victim-exit", victim, "never left")
                cyc.observe()
                return
            t_event = victim.find("LEFT")[0]
            self.verdict(cyc, survivor, t_event, victim, t_event, since=victim.find("VERIFIED")[0])
        cyc.observe()

    def verdict(self, cyc: Cycle, survivor: PeerState, t_event: float, newcomer: PeerState, t_back: float,
                gated: bool = True, since: Optional[float] = None):
        """The survivor reports the death within the detection bound and the pair reconnects with a verified
        exchange. When `gated` is false (the real-crash scenarios) the detection and reconnect times are measured and
        reported, not gated: the OS crash handler, not the library, governs when the process exits and the peer sees
        EOF (docs/protocol.md §7). The survivor's DISCONNECTED counts from `since` (default: the event), the moment
        the victim's end became possible (its kill armed, its leave begun): the survivor's report and the driver's
        record of the event (the victim's exit, its LEFT line) arrive through different pipes, in either order, so a
        report can carry an earlier time than the event it answers; its detection time is then 0."""
        s, result = self.s, cyc.result
        since = t_event if since is None else since

        def miss(kind: str, who: PeerState, detail: str):
            (result.notes.append(detail) if not gated else cyc.problem(kind, who, detail))

        # Wait up to the reconnect bound (real crashes may be held by a dump handler) but gate at the detection bound
        cyc.wait(lambda: survivor.find("DISCONNECTED", since) is not None,
                 t_event + (s.reconnect_s if not gated else s.detect_s) - time.monotonic())
        disconnected = survivor.find("DISCONNECTED", since)
        if disconnected:
            result.detect_ms = max(0.0, disconnected[0] - t_event) * 1000
            if gated and disconnected[0] - t_event > s.detect_s:
                cyc.problem("late-detection", survivor, f"DISCONNECTED {result.detect_ms:.0f} ms after the event")
        elif not cyc.failed:
            miss("no-detection", survivor, "no DISCONNECTED within the bound after the event")
        if not cyc.failed:
            if cyc.wait(lambda: verified_with(newcomer, survivor, t_back) and verified_with(survivor, newcomer, t_back),
                        t_back + s.reconnect_s - time.monotonic()):
                done = max(newcomer.find("VERIFIED", t_back, peer=survivor.pid)[0],
                           survivor.find("VERIFIED", t_back, peer=newcomer.pid)[0])
                result.reconnect_s = done - t_back
            elif not cyc.failed:
                miss("no-reconnect", newcomer, f"no verified exchange within {s.reconnect_s:.0f} s")

    def pause_verdict(self, cyc: Cycle, survivor: PeerState, victim: PeerState, t_pause: float):
        """The protocol never declares a paused peer dead (docs/protocol.md §7): the survivor must not report DISCONNECTED while its
        peer is only suspended, and once the victim resumes both sides must keep exchanging (their rx counters climb),
        so the session survived the pause intact. A false death here would mean a timer ends sessions."""
        s, result = self.s, cyc.result
        if survivor.find("DISCONNECTED", t_pause) is not None:
            cyc.problem("false-death", survivor, "reported DISCONNECTED while its peer was only paused")

        def rx(ps: PeerState) -> int:
            try:
                return int(ps.status.get("rx", "0"))
            except ValueError:
                return 0

        base = {survivor: rx(survivor), victim: rx(victim)}
        t_resume = time.monotonic()
        if cyc.wait(lambda: rx(survivor) > base[survivor] and rx(victim) > base[victim],
                    t_resume + s.reconnect_s - time.monotonic()):
            result.reconnect_s = time.monotonic() - t_resume
        elif not cyc.failed:
            cyc.problem("no-progress", survivor, "traffic did not resume after the pause")

    # ----- crash-at-step: a fresh client ended at each of its steps -----

    def run_crash_step(self, sc: Scenario, cycles: int):
        for cycle in range(cycles):
            seed = self.seed + cycle
            params = derive_params(sc, seed)
            started = time.monotonic()
            result = CycleResult(sc.name, cycle, seed, params.describe(sc))
            board = Board(self.s, self.pair_name(sc, cycle), self.out)
            try:
                self.crash_step_cycle(sc, board, params, result)
            finally:
                self.teardown(sc, board, result)
            result.duration_s = time.monotonic() - started
            self.record(result)

    def crash_step_cycle(self, sc: Scenario, board: Board, params: Params, result: CycleResult):
        s = self.s
        cyc = Cycle(self, board, result)
        step = params.crash_step
        # The survivor listens (the server); the crash victim connects and the hook ends it at `step` (exit 99).
        survivor = self.start(board, "server", "unity", "survivor", {"seed": params.seed * 2 + 1})
        cyc.watch(survivor)
        if not cyc.wait(lambda: survivor.find("INIT") is not None, 10):
            cyc.problem("setup", survivor, "no INIT within 10 s")
            return
        t0 = time.monotonic()
        victim = self.start(board, "client", "game", "crash-victim", {"seed": params.seed * 2},
                            env={"FASTIPC_TEST_CRASH_AT": step})
        cyc.watch(victim, (CRASH_STEP_CODE,))
        if not cyc.wait(lambda: not victim.alive, s.reconnect_s):
            if not cyc.failed:
                cyc.problem("victim-exit", victim, f"still alive; never reached {step}")
            cyc.observe()
            return
        if victim.exit_code == CRASH_STEP_CODE:
            result.notes.append(f"ended at {step}")
        # A healthy client then connects and exchanges with the survivor, whatever the crash left behind.
        t_back = time.monotonic()
        healthy = self.start(board, "client", "game", "healthy", {"seed": params.seed * 2 + 7})
        cyc.watch(healthy)
        if cyc.wait(lambda: verified_with(healthy, survivor, t_back) and verified_with(survivor, healthy, t_back),
                    s.reconnect_s):
            done = max(healthy.find("VERIFIED", t_back, peer=survivor.pid)[0],
                       survivor.find("VERIFIED", t_back, peer=healthy.pid)[0])
            result.reconnect_s = done - t_back
        elif not cyc.failed:
            cyc.problem("no-reconnect", healthy, f"no verified exchange within {s.reconnect_s:.0f} s after a crash at {step}")
        cyc.observe()

    # ----- summary -----

    def summary(self, scenarios: List[Scenario]) -> bool:
        print("\nChaos summary"
              f" (detection bound {self.s.detect_s * 1000:.0f} ms, reconnect bound {self.s.reconnect_s:.0f} s,"
              f" base seed {self.seed}):")
        print("| Scenario | Cycles | Pass | Known failures | New failures | Detection ms (median / max) "
              "| Reconnect ms (median / max) | Leak checks |")
        print("|---|---|---|---|---|---|---|---|")
        new_failures = 0
        for sc in scenarios:
            rows = [r for r in self.results if r.scenario == sc.name]
            cycles = [r for r in rows if not r.extra]
            if not rows:
                continue
            known: Dict[str, int] = {}
            for r in cycles:
                if r.outcome == "XFAIL":
                    for k in r.known:
                        known[k] = known.get(k, 0) + 1
            new = sum(r.outcome == "FAIL" for r in rows)
            new_failures += new
            detect = [r.detect_ms for r in cycles if r.detect_ms is not None]
            reconnect = [r.reconnect_s * 1000 for r in cycles if r.reconnect_s is not None]
            checked, leaked = self.leak_checks.get(sc.name, [0, 0])
            print(f"| {sc.name} | {len(cycles)} | {sum(r.outcome == 'PASS' for r in cycles)} | "
                  f"{', '.join(f'{k}: {n}' for k, n in known.items()) or '0'} | {new} | "
                  f"{_median_max(detect, '{:.0f}')} | {_median_max(reconnect, '{:.0f}')} | "
                  f"{leaked} of {checked} leaked |")
        what = "handles" if IS_WINDOWS else "file descriptors"
        for name, counts in self.survivor_resources:
            if len(counts) >= 4:
                steady = counts[2:]
                print(f"  {name}: a survivor's open {what} at {len(counts)} verified connections: first {counts[0]}, "
                      f"after two warm-up connections {min(steady)}-{max(steady)}, last {counts[-1]}")
        return new_failures == 0


def _median_max(values: List[float], fmt: str) -> str:
    if not values:
        return "-"
    return f"{fmt.format(statistics.median(values))} / {fmt.format(max(values))}"


def run_scenario(harness: Harness, sc: Scenario, cycles: int):
    harness.out(f"\n{sc.name}: {sc.summary} ({cycles} cycles)")
    started = time.monotonic()
    if sc.family == "restart":
        harness.run_restart(sc, cycles)
    elif sc.family == "crashstep":
        harness.run_crash_step(sc, cycles)
    else:
        harness.run_fresh(sc, cycles)
    harness.out(f"  {sc.name}: {cycles} cycles in {time.monotonic() - started:.0f} s")


def run_lane(settings: Settings, seed: int, name: str, cycles: int):
    """One scenario in a lane's process: its output as one block, and what the summary needs."""
    harness = Harness(settings, seed, buffered=True)
    run_scenario(harness, SCENARIOS[name], cycles)
    return "\n".join(harness.lines), harness.results, harness.leak_checks, harness.survivor_resources


def run(bin_dir: Path, ext: str, scenarios: List[str], cycles: Optional[int], seed: Optional[int],
        ready_ms: int, observe_ms: int, trace: bool = False, jobs: int = DEFAULT_JOBS) -> bool:
    """Runs the scenarios, `jobs` at once; prints one line per cycle and a summary. True if no new failure.

    Each lane is a process of its own (its own driver, event threads and pair names) that runs one scenario at a
    time, the longest first; a scenario's lines print as one block when it ends. The checks are per peer process (a
    survivor's resources, a peer's CPU time), so lanes don't share them. jobs=1 runs every scenario in this process,
    one after another, printing each line as it comes."""
    if seed is None:
        seed = random.SystemRandom().randrange(1, 1_000_000)
    settings = Settings(bin_dir, ext, ready_ms=ready_ms, observe_ms=observe_ms, trace=trace)
    harness = Harness(settings, seed)
    chosen = [SCENARIOS[name] for name in scenarios]
    counts = {sc.name: cycles if cycles is not None else sc.cycles for sc in chosen}
    lanes = max(1, min(jobs, len(chosen)))
    print(f"\nChaos harness: {len(chosen)} scenario(s), base seed {seed}"
          + (f", {lanes} at once (each scenario prints as one block when it ends)" if lanes > 1 else ""), flush=True)
    if lanes == 1:
        for sc in chosen:
            run_scenario(harness, sc, counts[sc.name])
        return harness.summary(chosen)
    longest_first = sorted(chosen, key=lambda sc: counts[sc.name] * sc.typical_s, reverse=True)
    pool = ProcessPoolExecutor(max_workers=lanes, mp_context=multiprocessing.get_context("spawn"))
    try:
        futures = [pool.submit(run_lane, settings, seed, sc.name, counts[sc.name]) for sc in longest_first]
        for future in as_completed(futures):
            text, results, leak_checks, resources = future.result()
            print(text, flush=True)
            harness.results += results
            harness.leak_checks.update(leak_checks)
            harness.survivor_resources += resources
    finally:
        pool.shutdown(wait=True, cancel_futures=True)
    rank = {sc.name: i for i, sc in enumerate(chosen)}
    harness.survivor_resources.sort(key=lambda item: rank[item[0]])
    return harness.summary(chosen)
