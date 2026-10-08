# fipc

Messages and RPC between two processes on one machine, through shared memory: the Python binding of
[FastIPC](https://github.com/fastipc/fastipc), a small library written in Zig. A server listens on a name and
accepts one client at a time; a client connects to the name. `connect` waits for the server's `accept`, so a client and
its server run in different processes or on different threads. Each connection has a shared-memory segment with one ring
per direction, and reports the peer's end as soon as its process exits or closes the connection.

## Installation

```bash
pip install fipc-python
```

The wheels bundle the native library: Windows x64 (Windows 11 / Server 2022 or newer) and Linux x64 (glibc 2.34 or
newer: Ubuntu 22.04, Debian 12, RHEL 9 and later), which need an x86-64-v3 CPU (AVX2: Intel Haswell, AMD Zen or
later), Linux on ARM64 (`manylinux_2_34_aarch64`, glibc 2.34 or newer) and macOS 14.4 or newer on Apple Silicon. The
macOS wheel is tagged `macosx_14_0_arm64` (pip matches macOS 11
and later by the major version only), so pip also installs it on macOS 14.0 to 14.3, where the library doesn't load.
The binding loads it through CFFI's ABI mode, so one wheel per platform fits every Python 3.9+. A library of your own
comes first: set the environment variable `FASTIPC_LIB_DIR` to its folder (when it is set, the binding looks nowhere
else).

## Use

```python
# server.py
from fipc import Listener

# rings of 1 MiB each way
with Listener("my_channel", 1 << 20) as listener:
    with listener.accept() as conn:  # waits for a client
        request = conn.rpc_recv()
        conn.rpc_respond(request.id, request.opcode,
                         data=request.payload.upper())
```

```python
# client.py
from fipc import Conn

with Conn.connect("my_channel", timeout_ms=5000) as conn:
    conn.rpc_submit(1, b"ping")     # opcode 1
    print(conn.rpc_recv().payload)  # b'PING'
```

Run each in its own terminal: `python server.py`, then `python client.py`. The client sends an RPC request with opcode
1 and the payload `ping`, and the server answers with the payload in upper case. Every example of the
[repository's README](https://github.com/fastipc/fastipc#examples) makes this exchange, so its C, C#, Java,
Rust, Lua, JavaScript and Go clients work against this server too, and this client against their servers.

- **Plain messages:** `send` and `recv` (a message of any size: it travels in pieces and arrives whole), or
  `recv_into` a buffer of your own.
- **Zero-copy** (messages of one piece, up to `max_piece()` bytes): `send_acquire(length)`, write into the
  memoryview, `send_commit(length)`; `recv_acquire()`, read, `recv_release()`.
- **RPC:** `rpc_submit`, `rpc_respond` and `rpc_recv`, which returns an `RpcMessage` (id, kind, opcode, status,
  payload).

Timeouts are int milliseconds, the last parameter: `NO_WAIT` (0) doesn't wait, `FOREVER` (-1, the default) waits for
ever. A call that fails raises `FipcError`, whose `result` is a `Result` (`Result.TIMEOUT`, `Result.DISCONNECTED`,
...). On a connection, one thread at a time sends and one receives; `cancel` may be called from any thread, `close`
only when no other thread is inside a call on the connection. A disconnected connection is final: close it, and
accept or connect a new one.

## Documentation

- [The website](https://fastipc.github.io/fastipc/python.html): this binding's guide, with examples of every call.
- [`include/fipc.h`](https://github.com/fastipc/fastipc/blob/main/include/fipc.h): the C API under this binding, and every call's contract (results,
  timeouts, threads, messages in pieces, the peer's end).
- [Platform support](https://github.com/fastipc/fastipc/blob/main/docs/platform-support.md), the [protocol](https://github.com/fastipc/fastipc/blob/main/docs/protocol.md) and the
  [benchmarks](https://github.com/fastipc/fastipc/blob/main/docs/perf/bindings-baseline.md).
- [Changelog](https://github.com/fastipc/fastipc/blob/main/CHANGELOG.md), [issues](https://github.com/fastipc/fastipc/issues).

## License

MIT. Copyright (c) 2025-2026 Hayden Donnelly. See [LICENSE](https://github.com/fastipc/fastipc/blob/main/LICENSE).
