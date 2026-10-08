#!/usr/bin/env python3
"""
What it costs a Python dispatcher to hand a small task to a worker process and get its result back, through FastIPC
and through the standard library's channels: the numbers behind the scaling model on website/python.html.

    python dispatch_bench.py overhead          # per transport: the round trip R, and the dispatcher's cost c at
                                                # each worker count
    python dispatch_bench.py scale [--task-us T] [--serial S] [--workers 1,2,4,8]
                                                # a real dispatcher and its workers
    python dispatch_bench.py grid [--out F] [--rounds N] [--budget-ms B]
                                                # scale over every workers x task time of the grid (serial 0),
                                                # N rounds (default 3), B ms per run (default 250), medians as
                                                # JSON
    python dispatch_bench.py website WINDOWS.json LINUX.json "WINDOWS LABEL" "LINUX LABEL"
                                                # writes website/assets/scaling-data.js from two grid files

Transports: fipc (Conn.send / Conn.recv, one connection per worker), pipe (multiprocessing.Pipe, send_bytes /
recv_bytes), tcp (a loopback socket per worker, TCP_NODELAY) and queue (multiprocessing.Queue: one queue of tasks
and one of results, shared by the workers). Each task and each result is 64 bytes.

- R, the round trip: one task in flight, the worker answers at once; the median of 20,000 round trips.
- c, the dispatcher's cost per task: P workers, each with one task at a time, that answer at once; the time per
  task, for each worker count P. It is what the dispatcher spends handing out a task and taking its result, in the
  pattern the speedups are measured in: a receive that finds nothing yet waits for the result there (a channel that
  blocks in the kernel pays for its wake-up). With many tasks in flight a channel could batch its wake-ups, and c
  would read several times lower than what a dispatcher of one task per worker pays.
- scale: tasks of T us each, a fraction S of each done by the dispatcher (busy-waiting), the rest by a worker; each
  worker has one task at a time, and the dispatcher takes the results in turn. It hands out tasks for a fixed time,
  then collects the last ones. Speedup = tasks done x T / the run's time (one process would take T a task).
"""

from __future__ import annotations

import multiprocessing as mp
import os
import socket
import statistics
import sys
import time
from pathlib import Path
from typing import Any, Callable, List

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "bindings" / "python"))

from fipc import Conn, Listener  # noqa: E402

SIZE = 64
END = b"E" * SIZE
CONNECT_MS = 15_000
TRANSPORTS = ["fipc", "pipe", "tcp", "queue"]


def spin(ns: int) -> None:
    """Busy work for `ns` nanoseconds."""
    end = time.perf_counter_ns() + ns
    while time.perf_counter_ns() < end:
        pass


def recv_exact(sock: socket.socket, n: int) -> bytes:
    data = b""
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        if not chunk:
            raise EOFError
        data += chunk
    return data


# The worker: receives tasks until END, does `work_ns` of busy work on each and sends the result back.


def worker(transport: str, address: Any, work_ns: int) -> None:
    """`work_ns` < 0: each task's first 8 bytes say how long its work is."""
    if transport == "fipc":
        conn = Conn.connect(address, CONNECT_MS)
        recv, send = conn.recv, conn.send
    elif transport == "pipe":
        recv, send = address.recv_bytes, address.send_bytes
    elif transport == "tcp":
        sock = socket.create_connection(("127.0.0.1", address))
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        recv, send = (lambda: recv_exact(sock, SIZE)), sock.sendall
    else:
        tasks, results = address
        recv, send = tasks.get, results.put
    while True:
        msg = recv()
        if msg == END:
            break
        if work_ns < 0:
            spin(int.from_bytes(msg[:8], "little"))
        elif work_ns:
            spin(work_ns)
        send(msg)
    if transport == "fipc":
        conn.close()


class Channels:
    """The dispatcher's side: send(i, task) to worker i, recv(i) a result from worker i (queue: from any worker)."""

    def __init__(self, transport: str, workers: int, work_ns: int):
        ctx = mp.get_context("spawn")
        self.transport = transport
        self.procs: List[Any] = []
        if transport == "fipc":
            base = f"dispatch_{os.getpid()}_{time.time_ns()}"
            self.listeners = [Listener(f"{base}_{i}", 1 << 20) for i in range(workers)]
            addresses: List[Any] = [f"{base}_{i}" for i in range(workers)]
        elif transport == "pipe":
            pairs = [ctx.Pipe() for _ in range(workers)]
            self.ends = [a for a, _ in pairs]
            addresses = [b for _, b in pairs]
        elif transport == "tcp":
            server = socket.socket()
            server.bind(("127.0.0.1", 0))
            server.listen(workers)
            addresses = [server.getsockname()[1]] * workers
        else:
            self.tasks, self.results = ctx.Queue(), ctx.Queue()
            addresses = [(self.tasks, self.results)] * workers
        for i in range(workers):
            p = ctx.Process(target=worker, args=(transport, addresses[i], work_ns))
            p.start()
            self.procs.append(p)
        if transport == "fipc":
            self.conns = [listener.accept(CONNECT_MS) for listener in self.listeners]
            self._send: Callable[[int, bytes], None] = lambda i, b: self.conns[i].send(b)
            self._recv: Callable[[int], bytes] = lambda i: self.conns[i].recv()
        elif transport == "pipe":
            self._send = lambda i, b: self.ends[i].send_bytes(b)
            self._recv = lambda i: self.ends[i].recv_bytes()
        elif transport == "tcp":
            self.socks = []
            for _ in range(workers):
                s, _ = server.accept()
                s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                self.socks.append(s)
            server.close()
            self._send = lambda i, b: self.socks[i].sendall(b)
            self._recv = lambda i: recv_exact(self.socks[i], SIZE)
        else:
            self._send = lambda i, b: self.tasks.put(b)
            self._recv = lambda i: self.results.get()
        self.send, self.recv = self._send, self._recv

    def close(self) -> None:
        for i in range(len(self.procs)):
            self.send(i, END)
        for p in self.procs:
            p.join(timeout=30)
        if self.transport == "fipc":
            for c in self.conns:
                c.close()
            for listener in self.listeners:
                listener.close()


def round_trip(transport: str) -> float:
    """R in microseconds: one worker that answers at once, one task in flight."""
    ch = Channels(transport, 1, 0)
    task = b"t" * SIZE
    try:
        for _ in range(2000):  # warm-up
            ch.send(0, task)
            ch.recv(0)
        samples = []
        clock = time.perf_counter_ns
        for _ in range(20_000):
            t0 = clock()
            ch.send(0, task)
            ch.recv(0)
            samples.append(clock() - t0)
    finally:
        ch.close()
    return statistics.median(samples) / 1000


def dispatch_cost(ch: Channels, workers: int, budget_ns: int) -> float:
    """c in microseconds: tasks without work handed to `workers` workers in turn, one each, for `budget_ns`."""
    task = (0).to_bytes(8, "little") + b"t" * (SIZE - 8)
    for i in range(workers):
        ch.send(i, task)
    clock = time.perf_counter_ns
    t0 = clock()
    stop = t0 + budget_ns
    done = 0
    while clock() < stop:
        i = done % workers
        ch.recv(i)
        ch.send(i, task)
        done += 1
    elapsed = clock() - t0
    for i in range(workers):
        ch.recv(i)
    return elapsed / done / 1000


def timed_run(ch: Channels, workers: int, task_ns: int, serial: float, budget_ns: int) -> float:
    """Hands out tasks of `task_ns` for `budget_ns` (each worker one at a time, in turn), then collects the last ones;
    returns the speedup over doing the same tasks in one process."""
    serial_ns = int(task_ns * serial)
    task = (task_ns - serial_ns).to_bytes(8, "little") + b"t" * (SIZE - 8)
    for _ in range(3):  # warm-up: every worker answers a few
        for i in range(workers):
            ch.send(i, task)
        for i in range(workers):
            ch.recv(i)
    clock = time.perf_counter_ns
    t0 = clock()
    stop = t0 + budget_ns
    for i in range(workers):
        ch.send(i, task)
    sent, done, k = workers, 0, 0
    while done < sent:
        i = k % workers
        k += 1
        ch.recv(i)
        done += 1
        spin(serial_ns)  # the dispatcher's serial share of the task
        if clock() < stop:
            ch.send(i, task)
            sent += 1
    return done * task_ns / (clock() - t0)


def scale(transport: str, workers: int, task_ns: int, serial: float) -> float:
    ch = Channels(transport, workers, -1)
    try:
        return timed_run(ch, workers, task_ns, serial, 1_000_000_000)
    finally:
        ch.close()


GRID_WORKERS = [1, 2, 4, 6, 8, 12]
GRID_TASK_US = [5, 10, 20, 50, 100, 200, 500, 1000]
GRID_SERIAL = [0.0]  # the dispatcher does no work of its own: what the chart shows is the cost of the IPC


def grid(out: str, rounds: int, budget_ms: int) -> None:
    """Every (transport, workers, serial, task time) of the grid and c at each worker count, `rounds` times
    round-robin, and each transport's R; writes the medians as JSON."""
    import json
    import platform

    cells: dict = {}
    begin = time.monotonic()
    for r in range(rounds):
        for t in TRANSPORTS:
            for p in GRID_WORKERS:
                ch = Channels(t, p, -1)
                try:
                    for s in GRID_SERIAL:
                        for us in GRID_TASK_US:
                            v = timed_run(ch, p, us * 1000, s, budget_ms * 1_000_000)
                            cells.setdefault((t, s, us, p), []).append(v)
                    cells.setdefault((t, "c", p), []).append(dispatch_cost(ch, p, budget_ms * 1_000_000))
                finally:
                    ch.close()
                print(f"round {r + 1}/{rounds}: {t} x {p} done ({time.monotonic() - begin:.0f} s)", flush=True)
    costs = {}
    for t in TRANSPORTS:
        costs[t] = {"R": round(statistics.median(round_trip(t) for _ in range(rounds)), 2),
                    "c": [round(statistics.median(cells[(t, "c", p)]), 2) for p in GRID_WORKERS]}
        print(f"{t}: R {costs[t]['R']} us, c {costs[t]['c']} us per worker count", flush=True)
    speedup = {t: {f"{s:g}": {str(us): [round(statistics.median(cells[(t, s, us, p)]), 3) for p in GRID_WORKERS]
                              for us in GRID_TASK_US} for s in GRID_SERIAL} for t in TRANSPORTS}
    result = {
        "system": platform.system(), "python": platform.python_version(), "cpus": os.cpu_count(),
        "date": time.strftime("%Y-%m-%d"), "rounds": rounds, "budget_ms": budget_ms,
        "workers": GRID_WORKERS, "task_us": GRID_TASK_US, "serial": GRID_SERIAL,
        "overhead": costs, "speedup": speedup,
    }
    Path(out).write_text(json.dumps(result, indent=1) + "\n")
    print(f"wrote {out}")


def main() -> None:
    args = sys.argv[1:]
    mode = args[0] if args else "overhead"
    if mode == "overhead":
        print(f"{'transport':<8} {'R (us)':>8}   c (us) at {GRID_WORKERS} workers; medians of 5 runs")
        for t in TRANSPORTS:
            r = statistics.median(round_trip(t) for _ in range(5))
            c = []
            for p in GRID_WORKERS:
                ch = Channels(t, p, -1)
                try:
                    c.append(statistics.median(dispatch_cost(ch, p, 250_000_000) for _ in range(5)))
                finally:
                    ch.close()
            print(f"{t:<8} {r:>8.2f}   " + " ".join(f"{x:6.2f}" for x in c), flush=True)
    elif mode == "scale":
        task_us = float(args[args.index("--task-us") + 1]) if "--task-us" in args else 100.0
        serial = float(args[args.index("--serial") + 1]) if "--serial" in args else 0.05
        counts = [int(x) for x in args[args.index("--workers") + 1].split(",")] if "--workers" in args else [1, 2, 4, 8]
        print(f"task {task_us} us, serial {serial:.0%}; speedup per worker count {counts}")
        for t in TRANSPORTS:
            speedups = [scale(t, p, int(task_us * 1000), serial) for p in counts]
            print(f"{t:<8} " + "  ".join(f"{s:6.2f}" for s in speedups), flush=True)
    elif mode == "grid":
        out = args[args.index("--out") + 1] if "--out" in args else "dispatch_grid.json"
        rounds = int(args[args.index("--rounds") + 1]) if "--rounds" in args else 3
        budget = int(args[args.index("--budget-ms") + 1]) if "--budget-ms" in args else 250
        grid(out, rounds, budget)
    elif mode == "website" and len(args) == 5:
        import json

        data = {}
        for key, path, label in (("windows", args[1], args[3]), ("linux", args[2], args[4])):
            data[key] = json.loads(Path(path).read_text())
            data[key]["label"] = label
            # the chart shows the runs without serial work
            data[key]["serial"] = [0.0]
            data[key]["speedup"] = {t: {"0": v["0"]} for t, v in data[key]["speedup"].items()}
        out = Path(__file__).resolve().parent.parent.parent / "website" / "assets" / "scaling-data.js"
        out.write_text("// Written by bench/python/dispatch_bench.py website, from its grid runs: the scaling chart's data.\n"
                       "window.FIPC_SCALING = " + json.dumps(data, separators=(",", ":")) + ";\n", newline="\n")
        print(f"wrote {out}")
    else:
        sys.exit(__doc__)


if __name__ == "__main__":
    main()
