# Changelog

All notable changes to FastIPC. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
versions follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html): the version covers the C API
(`include/fipc.h`) and the bindings' APIs. The wire protocol has a version of its own
([`docs/protocol.md`](docs/protocol.md) §8): two builds interoperate exactly when their protocol versions match.

## [1.0.0] - 2026-10-04

The first public release.

### The library

- **The C API** ([`include/fipc.h`](include/fipc.h)): 18 functions, one public struct (`fipc_rpc_msg_t`) and the
  result codes, exported from `fastipc.dll` / `libfastipc.so` / `libfastipc.dylib` with the C calling convention.
- **Connections:** a server listens on a name and accepts one client at a time; a client connects to the name, and
  may connect before the server listens or while it serves another client (within its timeout). `fipc_accept` and
  `fipc_connect` set a connection up on the threads that call them, so a client and its server run on different
  threads or processes; nothing of a listener runs in the background, and `fipc_accept` with timeout 0 polls (a call
  that times out keeps a client's setup for the next). Each connection has its own shared-memory segment with one
  single-producer, single-consumer ring per direction, of a power-of-two capacity from 1 KiB to 2 GiB.
- **Messages:** the copying calls (`fipc_send`, `fipc_recv`) take messages of any size, which travel in pieces and
  arrive whole; the zero-copy calls (`fipc_send_acquire`/`fipc_send_commit`, `fipc_recv_acquire`/`fipc_recv_release`)
  write and read a message of one piece in place. No allocation per message.
- **RPC:** `fipc_rpc_submit`, `fipc_rpc_respond` and `fipc_rpc_recv`: requests and responses with ids, opcodes and a
  status, over a connection.
- **Waits:** timeouts in milliseconds on every call that can wait; `fipc_cancel` and `fipc_listener_cancel` end a
  wait from any thread. A waiting side spins, then sleeps on a futex (Linux), an `os_sync` address wait (macOS) or an
  event (Windows).
- **The stream pace:** a receiver that waits for a stream holds back its looks at the ring for its first spin pauses
  (a number measured per platform: 256 on arm64, 64 on Windows x64, 32 on Linux x64), so that it doesn't take the
  sender's cache lines after every message; one that waits for an answer looks at once. `zig build -Dstream_pace=N`
  builds another pace for other hardware.
- **The peer's end:** a local control connection (an abstract `AF_UNIX` socket on Linux, a named pipe on Windows, a
  socket file in the user's private directory on macOS) reports `FIPC_DISCONNECTED` as soon as the peer's process
  exits or it closes the connection; receives first deliver every message the peer completed. Nothing is left behind
  after a crash on Linux and Windows; on macOS a crashed listener's socket and lock files stay only until the same
  user's next listen removes them, and never stop a new listener or a connection.
- **Security:** only processes of the same user can connect (`SO_PEERCRED` on Linux, `getpeereid` and a private
  directory on macOS; owner and DACL checks on Windows), and every index and frame in shared memory is checked before use.
- **fork (Linux, macOS):** a child's inherited handles fail cleanly and never disturb the parent's connections.
- **The protocol, version 1** ([`docs/protocol.md`](docs/protocol.md)): names, control frames, the segment's layout,
  the rings' frames and the wake-up protocol, specified to the byte.
- **The native Zig API** (the module `fastipc`, [`src/zig/root.zig`](src/zig/root.zig)): listeners and connections
  over the caller's `std.Io`, with plain, zero-copy and RPC messages, Zig error sets and `std.Io.Timeout`;
  [`examples/zig`](examples/zig) builds a Zig server and client with it.

### Bindings and packages

- **C and C++:** an archive per OS on the GitHub release, with `include/fipc.h`, `include/fipc.hpp` and the library:
  `lib/libfastipc.so` (Linux x64, and Linux arm64 in `fastipc-1.0.0-linux-arm64.tar.gz`), `lib/libfastipc.dylib` (macOS arm64, `fastipc-1.0.0-osx-arm64.tar.gz`; ad hoc
  signed, not notarized), or `bin/fastipc.dll` and its import library `lib/fastipc.lib` (Windows x64); checksums in
  `SHA256SUMS`.
- **C++:** the header-only C++20 wrapper [`include/fipc.hpp`](include/fipc.hpp) over the 18 functions, adding no ABI:
  `fipc::listener` and `fipc::connection`, move-only and closed exactly once, with `fipc::result<T>` and
  `fipc::status` for the results (no exceptions or RTTI needed), `std::chrono` timeouts with `fipc::forever` and
  `fipc::no_wait`, sends of `std::span<const std::byte>` or `std::string_view`, receives into the caller's buffers,
  zero-copy slots that commit or release, RPC, and a `fipc::canceler` for other threads.
- **The Zig build system for C and C++:** a build that depends on the package (`zig fetch`, or by path) gets the
  artifact `fastipc`, the shared library with `fipc.h` and `fipc.hpp` on its include path, and the module `fastipc`;
  the package carries only what that build needs. [`examples/c-cpp`](examples/c-cpp) builds the C and
  C++ servers and clients that way, with no other build system.
- **Python:** the package `fipc-python` on PyPI (`import fipc`): `Listener` and `Conn` over CFFI's ABI mode, for
  Python 3.9+; wheels for Windows x64, Linux x64 and arm64 (manylinux, glibc 2.34) and macOS arm64 (`macosx_14_0_arm64`) with the library
  bundled.
- **C#:** the NuGet package `Fipc` (.NET Standard 2.1): `FipcListener` and `FipcConnection` on SafeHandles, safe
  to dispose from any thread, over the raw P/Invoke layer `Fipc`; the libraries for `win-x64`, `linux-x64`,
  `linux-arm64` and `osx-arm64` bundled under `runtimes/`.
- **Java:** the Maven artifact `io.github.fastipc:fipc` (the module `io.github.fastipc`, Java 22+): `Listener`
  and `Connection` over the FFM API, safe to close from any thread, with `FipcException` for the results, `Duration`
  timeouts, zero-copy `MemorySegment`s confined to their thread and RPC; the library for every platform bundled in the jar,
  extracted into the user's cache.
- **Rust:** the crates `fipc` (`use fipc`, Rust 1.85+) and `fipc-sys` (the raw FFI, declared by
  hand): `Listener` and `Connection`, `Send` and `Sync`, closed when dropped, with `Result<T, Error>`,
  `Option<Duration>` timeouts, sending and receiving halves, a `Canceler` for other threads, zero-copy slots that
  borrow the connection, and RPC; the library for every platform carried by `fipc-sys`, which links it.
- **Lua:** the rock `fipc` (`require("fipc")`, LuaJIT 2.1), one module over LuaJIT's FFI with no C
  module to compile: `fipc.listen` and `fipc.connect` give listeners and connections with plain messages (Lua
  strings, or a cdata pointer and its length), zero-copy pointers into the ring and RPC; a call that fails returns
  `nil` and the error's name, timeouts are milliseconds; handles close with `close()` or when the garbage collector
  frees them, and the library stays loaded; the library for every platform carried by the rock, whose
  archive (`fipc-lua-1.0.0.tar.gz`, the module and every platform's library) is on the GitHub release too.
- **JavaScript:** the npm package `fipc` for Node.js 22+, Bun and Deno 2 (`import { Listener, Connection } from
  "fipc"`, or `require`), one Node-API addon that all three runtimes load, with TypeScript types: `Listener` and
  `Connection` with plain, zero-copy and RPC messages. Every call that may wait returns a Promise and leaves the event
  loop running (a thread of the connection's own waits, one per direction, never libuv's pool) and has a `Sync` form;
  a call that fails rejects with a `FipcError` whose `code` names the result. Zero-copy views are `Uint8Array`s over
  the ring, detached when their bytes stop being valid. The addon and the library for every platform are carried in
  the package (`prebuilds/<platform>-<arch>/`); it has no dependencies and no install script.
- **Go:** the module `github.com/fastipc/fastipc/bindings/go` (Go 1.27+; the package `fipc`, `import
  "github.com/fastipc/fastipc/bindings/go/fipc"`, tagged `bindings/go/v1.0.0`), without cgo: it loads the library at
  run time through `github.com/ebitengine/purego`, its one dependency, so `CGO_ENABLED=0` builds and cross-compiling
  work. `Listener` and `Conn` with plain, zero-copy and RPC messages; every method is safe from any goroutine (a mutex
  per direction, `Close` while other calls wait), failures are `*Error` values for `errors.Is` (`ErrTimeout`, ...,
  `ErrTooLarge` with the length), timeouts are `time.Duration`s, `Cancel` stops calls that wait
  (`context.AfterFunc(ctx, conn.Cancel)`), and handles the program drops are closed by the garbage collector. The
  library for every platform is carried in the module (`fipc/native/<platform>/`); a shipped program finds it next to
  its executable, or in `FASTIPC_LIB_DIR`.

### Platforms

- Linux x86-64 and ARM64 (glibc 2.34+), Windows 11 x86-64, macOS 14.4+ on Apple Silicon: Linux with glibc 2.34 or
  newer, on x86-64-v3 CPUs (AVX2) or ARMv8.0-A (aarch64); Windows 11 and Windows Server 2022 or newer, x64, on
  x86-64-v3 CPUs; macOS 14.4 or newer on Apple Silicon ([`docs/platform-support.md`](docs/platform-support.md)).

### Verification

- Unit tests next to the code; the C-API conformance suite (`tests/zig`), in one process and across processes,
  peers killed at every step of the handshake included; both test tiers under ThreadSanitizer on Linux and macOS (the fast tier on ARM64 Linux too); C#
  integration tests; the C++ wrapper's tests in both tiers, and Java, Rust, Lua, JavaScript and Go integration tests (the
  JavaScript ones under Node.js, Bun and Deno), each with a peer against a Python one; a cross-language matrix that runs every language's server example against every language's
  client example (`devtool test interop`); chaos (kill-and-restart), soak and fuzz harnesses; and a benchmark suite
  with an A/B comparison between revisions ([`docs/perf/baseline.md`](docs/perf/baseline.md)).

[1.0.0]: https://github.com/fastipc/fastipc/releases/tag/v1.0.0
