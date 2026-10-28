# Changelog

All notable changes to FastIPC. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
versions follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html): the version covers the C API
(`include/fipc.h`) and the bindings' APIs. The wire protocol has a version of its own
([`docs/protocol.md`](docs/protocol.md) §8): two builds interoperate exactly when their protocol versions match.

## [1.0.0] - 2026-10-26

The first public release.

- **The C API** ([`include/fipc.h`](include/fipc.h)): 30 functions over four kinds of shared-memory IPC between
  processes of one user on one machine:
  - **streams:** a server and one client at a time, a ring each way, messages of any size;
  - **broadcasts:** one publisher and up to 1024 subscribers, each lossy (never holds the publisher back) or gated
    (misses nothing);
  - **work queues:** up to 63 producers and 64 consumers; a message goes to another consumer only if the one that took
    it ends before acknowledging it;
  - **services:** up to 64 clients and 64 servers; each request goes to one server, and its reply back to its client,
    in the order of the client's requests; a request whose server ends before replying is answered `FIPC_ABANDONED`
    (a new result code) and never sent again.
- **Batches:** `fipc_send_many` sends several messages in one call and publishes them together, with at most one wake
  of the receivers; `fipc_recv_many` takes every message already there into one buffer. Batch both ends: a batching
  sender whose receiver takes one message per call runs slower than without batches. The C++ wrapper, the Zig module
  and every binding have them (Python and Lua `send_many`, `recv_many`; C# and Go `SendMany`, `ReceiveMany`; Java and
  JavaScript `sendMany`, `receiveMany`; Rust `send_many`, `receive_many`).
- **Copied and zero-copy messages**, a timeout and a cancel on every wait, and the peer's end reported as soon as its
  process exits (no heartbeat). Nothing is left behind after a crash on Linux and Windows; on macOS a crashed owner's
  two files stay until the name's next create.
- **The C++20 wrapper** [`include/fipc.hpp`](include/fipc.hpp) (header-only, no ABI of its own) and the **native Zig
  module** `fastipc` ([`src/zig/root.zig`](src/zig/root.zig)).
- **Bindings and packages:** Python (`fipc-python` on PyPI), C# (`Fipc` on NuGet), Java (`io.github.fastipc:fipc` on
  Maven Central), Rust (`fipc` and `fipc-sys` on crates.io), Lua (`fipc` on LuaRocks), JavaScript (`fipc-js` on npm, for
  Node.js 22+, Bun and Deno 2) and Go (`github.com/fastipc/fastipc/bindings/go`), each bundling the library for every
  platform; C/C++ archives per OS on the GitHub release, with `SHA256SUMS`.
- **Platforms:** Linux x86-64 and ARM64 (glibc 2.34+), Windows 11 / Server 2022+ x64, macOS 14.4+ on Apple Silicon;
  x86-64 means x86-64-v3 with PREFETCHW ([`docs/platform-support.md`](docs/platform-support.md)).
- **The wire protocol** ([`docs/protocol.md`](docs/protocol.md)): streams version 1; broadcasts, work queues and
  services the channel protocol, version 1.
- **Examples** in every language (a stream's server and client, a broadcast's publisher and subscriber, a queue's
  producer and consumer, a service's server and client), each language's against every other's.

The README, the binding READMEs and the website describe each API;
[the architecture page](https://fastipc.github.io/fastipc/architecture.html) describes the design and its known limits.

[1.0.0]: https://github.com/fastipc/fastipc/releases/tag/v1.0.0
