#!/usr/bin/env python3
"""
FastIPC's Python benchmark, through the binding (bindings/python, fipc): one-way throughput between two
processes, a server that receives (and times) and a client that sends.

    python fipc_bench.py copy       # Conn.send / Conn.recv_into
    python fipc_bench.py zerocopy   # send_acquire + send_commit / recv_acquire + recv_release (a message of several
                                    # pieces through the copying calls); the receiver copies each message out
                                    # unless BENCH_TOUCH=0
    python fipc_bench.py rpc        # rpc_submit / rpc_recv

A fresh process starts slowly, so each case first sends messages untimed for 0.2 s (the warm-up; at least its count),
then a start marker, then its count, and the server times from the last start marker to the end marker (see client).
Each case prints "Test <i>/<n>: <name>" and "Throughput: <n> messages/sec" (devtool bench-compare reads them).
"""

from __future__ import annotations

import multiprocessing
import os
import sys
import time
from pathlib import Path
from typing import Any, Optional, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "bindings" / "python"))

from fipc import Conn, FipcError, Listener, Result  # noqa: E402

CONNECT_MS = 15_000
END = b"END"
GO = b"GO!"
END_OPCODE = 0xFB000001
GO_OPCODE = 0xFB000002
WARM_UP = 0.2  # seconds
MARKER_EVERY = 1000

# (messages, ring, message size, name): the cases of the other benchmarks
CASES = [
    (2_000_000, 512 * 1024, 16, "Tiny messages (16B)"),
    (2_000_000, 512 * 1024, 64, "Small messages (64B)"),
    (1_000_000, 512 * 1024, 256, "Medium messages (256B)"),
    (1000, 512 * 1024, 64 * 1024, "Large messages (64KB)"),
    (1000, 2 * 1024 * 1024, 512 * 1024, "Large messages (512KB)"),
    (10, 512 * 1024, 1024 * 1024, "Exceeds buffer (1MB msg, 512KB buffer)"),
]


def touch() -> bool:
    return os.getenv("BENCH_TOUCH", "1").strip() not in ("0", "false", "no")


def client(mode: str, name: str, ring: int, size: int, count: int, out: Any) -> None:
    """Connects; sends messages untimed for the warm-up (the count, and for at least WARM_UP), with a start marker
    every MARKER_EVERY messages, as the other benchmarks do; then the start marker, `count` messages of `size` bytes
    and the end marker; and closes (the server still receives everything sent before the close)."""
    try:
        conn = Conn.connect(name, CONNECT_MS)
        payload = b"x" * size
        one_piece = size <= conn.max_piece()

        def send_one() -> None:
            if mode == "rpc":
                conn.rpc_submit(1, payload)
            elif mode == "zerocopy" and one_piece:
                conn.send_acquire(size)[:] = payload
                conn.send_commit(size)
            else:
                conn.send(payload)

        def send_marker(marker: bytes, opcode: int) -> None:
            if mode == "rpc":
                conn.rpc_submit(opcode)
            else:
                conn.send(marker)

        warm = time.perf_counter() + WARM_UP
        i = 0
        while i < count or time.perf_counter() < warm:
            if i % MARKER_EVERY == 0:
                send_marker(GO, GO_OPCODE)
            send_one()
            i += 1
        send_marker(GO, GO_OPCODE)
        # The timed messages: each mode's loop inline, without send_one's call and branches
        if mode == "copy":
            for _ in range(count):
                conn.send(payload)
        elif mode == "zerocopy":
            for _ in range(count):
                if one_piece:
                    conn.send_acquire(size)[:] = payload
                    conn.send_commit(size)
                else:
                    conn.send(payload)
        else:
            for _ in range(count):
                conn.rpc_submit(1, payload)
        send_marker(END, END_OPCODE)
        conn.close()
        out.put(("ok",))
    except Exception as e:  # reported by the parent
        out.put((f"client: {e!r}",))


def server(mode: str, name: str, ring: int, size: int, out: Any) -> None:
    """Listens, accepts the client, skips the warm-up up to the last start marker and receives until the end marker;
    reports the messages, bytes and seconds."""
    try:
        with Listener(name, ring) as listener:
            conn = listener.accept(CONNECT_MS)
        messages = total = 0
        buf = bytearray(max(size, len(END)))
        sink = bytearray(size)
        copy_out = touch()
        start = time.perf_counter()
        # A start marker (the last one starts the timed messages) resets the count and the clock
        if mode == "copy":
            while True:
                n = conn.recv_into(buf)
                if n == len(END):
                    if buf[:n] == END:
                        break
                    if buf[:n] == GO:
                        messages = total = 0
                        start = time.perf_counter()
                        continue
                messages += 1
                total += n
        elif mode == "zerocopy":
            while True:
                try:
                    view = conn.recv_acquire()
                except FipcError as e:
                    if e.result != Result.TOO_LARGE:
                        raise
                    total += conn.recv_into(buf)  # several pieces
                    messages += 1
                    continue
                n = len(view)
                if n == len(END):
                    if view == END:
                        conn.recv_release()
                        break
                    if view == GO:
                        conn.recv_release()
                        messages = total = 0
                        start = time.perf_counter()
                        continue
                if copy_out:
                    sink[:n] = view
                conn.recv_release()
                messages += 1
                total += n
        else:
            while True:
                msg = conn.rpc_recv()
                if msg.opcode == END_OPCODE:
                    break
                if msg.opcode == GO_OPCODE:
                    messages = total = 0
                    start = time.perf_counter()
                    continue
                messages += 1
                total += len(msg.payload)
        seconds = time.perf_counter() - start
        conn.close()
        out.put((messages, total, seconds))
    except Exception as e:
        out.put((f"server: {e!r}", None, None))


def run_case(mode: str, count: int, ring: int, size: int) -> Optional[Tuple[int, int, float]]:
    name = f"pybench_{os.getpid()}_{time.time_ns()}"
    server_q: Any = multiprocessing.Queue()
    client_q: Any = multiprocessing.Queue()
    procs = [
        multiprocessing.Process(target=server, args=(mode, name, ring, size, server_q)),
        multiprocessing.Process(target=client, args=(mode, name, ring, size, count, client_q)),
    ]
    for p in procs:
        p.start()
    for p in procs:
        p.join(timeout=180)
    if any(p.is_alive() for p in procs):
        for p in procs:
            p.terminate()
        print("ERROR: timed out")
        return None
    result = server_q.get(timeout=5)
    sent = client_q.get(timeout=5)
    if sent[0] != "ok" or not isinstance(result[0], int):
        print(f"ERROR: {sent[0] if sent[0] != 'ok' else result[0]}")
        return None
    messages, total, seconds = result
    if messages != count or total != count * size:
        print(f"ERROR: expected {count} messages of {size} B, got {messages} ({total} B)")
        return None
    return messages, total, seconds


def main() -> int:
    mode = sys.argv[1] if len(sys.argv) > 1 else "copy"
    if mode not in ("copy", "zerocopy", "rpc"):
        print("usage: fipc_bench.py copy|zerocopy|rpc")
        return 2
    print(f"FastIPC Python benchmark: {mode}\n")
    passed = 0
    for i, (count, ring, size, what) in enumerate(CASES, 1):
        print(f"Test {i}/{len(CASES)}: {what}")
        result = run_case(mode, count, ring, size)
        if result is not None:
            messages, total, seconds = result
            print(f"Messages: {count} | Ring: {ring // 1024}KB | Size: {size}B")
            print(f"Duration: {seconds:.3f}s")
            print(f"Throughput: {messages / seconds:,.0f} messages/sec, {total / seconds / (1 << 20):.1f} MB/sec")
            passed += 1
        print()
    print(f"Summary: {passed}/{len(CASES)} tests passed")
    return 0 if passed == len(CASES) else 1


if __name__ == "__main__":
    sys.exit(main())
