# fipc for Rust

[![crates.io](https://img.shields.io/crates/v/fipc.svg)](https://crates.io/crates/fipc)
[![docs.rs](https://img.shields.io/docsrs/fipc)](https://docs.rs/fipc)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/fastipc/fastipc/blob/main/LICENSE)

Messages and RPC between two processes on one machine, through shared memory: the Rust binding of
[FastIPC](https://github.com/fastipc/fastipc), a small library written in Zig
([`include/fipc.h`](https://github.com/fastipc/fastipc/blob/main/include/fipc.h)). A server listens on a name and
accepts one client at a time; a client connects to the name. `connect` waits for the server's `accept`, so a client and
its server run in different processes or on different threads. Each connection has a shared-memory segment with one ring
per direction, and reports the peer's end as soon as its process exits or drops the connection.

The peer can be written in any language with a binding: a Rust process talks to a Zig, C, C++, Python, C#, Java, Lua, JavaScript or Go
process exactly as it talks to another Rust process (the protocol is the same, and [every pair of languages is
tested](https://fastipc.github.io/fastipc/#interop)). Use it where two local processes must talk at memory speed: a
game and its editor or tools, a UI and its engine, a Python front end and a Rust service.

## Installation

```toml
[dependencies]
fipc = "1.0"
```

Rust 1.85 or newer. Two crates: `fipc` (`use fipc::...`), the safe API, and
[`fipc-sys`](https://crates.io/crates/fipc-sys), the raw FFI under it (re-exported as
`fipc::sys`). No other dependencies. The `-sys` crate carries the native library for Windows x64 (Windows 11 /
Server 2022 or newer, MSVC toolchain) and Linux x64 (glibc 2.34 or newer: Ubuntu 22.04, Debian 12, RHEL 9 and later),
which need an x86-64-v3 CPU (AVX2: Intel Haswell, AMD Zen or later), for Linux on ARM64 (`aarch64-unknown-linux-gnu`,
glibc 2.34 or newer) and for macOS 14.4 or newer on Apple Silicon (`aarch64-apple-darwin`), and links it. `cargo run`
and `cargo test` find the library by themselves; a program run on its own needs it next to it (see
[Shipping your program](#shipping-your-program)).

## Use

```rust
// server.rs
use fipc::Listener;

fn main() -> fipc::Result<()> {
    let mut listener = Listener::listen("my_channel", 1 << 20)?; // rings of 1 MiB each way
    let mut conn = listener.accept(None)?; // waits for a client
    let request = conn.rpc_receive(None)?;
    conn.rpc_respond(request.id, request.opcode, 0, &request.payload.to_ascii_uppercase(), None)?;
    Ok(())
}
```

```rust
// client.rs
use fipc::Connection;
use std::time::Duration;

fn main() -> fipc::Result<()> {
    let mut conn = Connection::connect("my_channel", Duration::from_secs(5))?;
    conn.rpc_submit(1, b"ping", None)?; // opcode 1
    println!("{}", String::from_utf8_lossy(&conn.rpc_receive(None)?.payload)); // PING
    Ok(())
}
```

Run each in its own terminal; in a checkout of the repository they are the crate's examples: `cargo run --example
server` and `cargo run --example client`. The Zig, C, C++, Python, C#, Java, Lua, JavaScript and Go clients of the
[repository's README](https://github.com/fastipc/fastipc#examples) work against this server too, and this client
against their servers.

- **Plain messages:** `send(&[u8])` and `receive()`, which returns a new `Vec<u8>`, or `receive_into(&mut [u8])`,
  which fills your buffer and returns the length. A message of any size may be sent (at least 1 byte); it travels in
  pieces and arrives whole.
- **Zero-copy** (messages of one piece, up to `max_piece()` bytes): `send_acquire(len)` returns a `SendSlot` in the
  ring (it derefs to `[u8]`): write the message into it, then `commit(len)`. `receive_acquire()` returns a
  `ReceiveSlot` that derefs to the message in the ring and frees its room when dropped. A slot borrows its side of the
  connection, so the borrow checker keeps it from outliving its turn. Dropping a `SendSlot` without committing it
  sends nothing.
- **RPC:** `rpc_submit(opcode, payload)` returns the request's id; `rpc_respond(id, opcode, status, payload)`;
  `rpc_receive()` returns an `RpcMessage` (id, kind, opcode, status, payload), and `rpc_receive_into(&mut buf)` an
  `RpcHeader` with the payload in your buffer (no allocation per message). Use a connection for plain messages or for
  RPC, not both.

### Results and timeouts

Every call that can fail returns `fipc::Result<T>`; the `Error` enum mirrors the C API's results: `Timeout`,
`Disconnected` (the peer's end; final: drop the connection, and accept or connect a new one), `Cancelled`,
`TooLarge { len }` (a receive's `len` says how much room the message needs; it stays queued), `Invalid`, `NoMemory` and
`AddrInUse`. It implements `std::error::Error`, displays the C name (`FIPC_TIMEOUT: the timeout ran out`) and converts
into `std::io::Error`.

Every call that can wait takes its timeout last, as anything that converts into an `Option<Duration>`: a `Duration`,
`NO_WAIT` (doesn't wait), or `None` / `FOREVER` (waits as long as it takes).

```rust
match conn.receive(Duration::from_millis(100)) {
    Ok(message) => handle(&message),
    Err(Error::Timeout) => {} // nothing yet
    Err(e) => return Err(e),  // Error::Disconnected: the peer is gone
}
```

### Threads

`Listener`, `Connection` and its halves are `Send` and `Sync`. The calls that send or receive take `&mut self`: the C
API allows one thread at a time to send and one to receive, and the borrow checker keeps it so. To send and receive on
two threads at once, `split()` the connection into its `Sender` and `Receiver` (borrowed, for scoped threads) or
`into_split()` it (owned); the connection closes when the last half is dropped.

To stop a thread that waits, take a `Canceler` first (`conn.canceler()`, `listener.canceler()`; `Clone`, `Send`,
`Sync`) and call `cancel()` from any thread: every call that waits, then or later, returns `Error::Cancelled`. A
canceler doesn't keep the connection open; once it is dropped, cancelling does nothing.

```rust
let canceler = conn.canceler();
let reader = std::thread::spawn(move || {
    // until Error::Cancelled (or Error::Disconnected)
    while let Ok(message) = conn.receive(None) {
        handle(&message);
    }
});
// ...
canceler.cancel(); // wakes the reader: Cancelled
reader.join().unwrap();
```

Dropping a connection ends it: the peer gets `Error::Disconnected` once it has received the messages this side
completed. There is no heartbeat and no timeout: a paused or hung peer is not reported, a crashed or killed one as
soon as its process has ended.

### Games and frame loops

A frame loop shouldn't block on the connection. Either poll it once a frame with `NO_WAIT` (in Bevy, keep the
`Connection` in a `Resource` and poll it in a system), or give its `Receiver` to a thread that forwards each message
through a channel the frame loop drains:

```rust
// Sending on this thread, receiving on another: the halves of the connection
let (mut sender, mut receiver) = conn.into_split();
let reader = std::thread::spawn(move || {
    while let Ok(message) = receiver.receive(None) {
        if tx.send(message).is_err() {
            break; // the frame loop is gone
        }
    }
});
// Each frame: whatever has arrived, without waiting
sender.send(b"update", None)?;
```

The example `game_loop` polls an echo server once a frame: `cargo run --example echo_server`, then
`cargo run --example game_loop`.

### The native library

`fipc-sys` links the library dynamically (on Windows through `raw-dylib`, so no import library is needed) and
copies it into Cargo's build folder, where `cargo run` and `cargo test` find it. Its build script takes the library
from, in this order:

1. the folder the environment variable `FASTIPC_LIB_DIR` names (a library you built, or keep in your repository);
2. in a checkout of the FastIPC repository, its `zig-out` build;
3. the copy for the target platform that the crate carries.

On docs.rs nothing is linked. The library is never unloaded (on Linux and macOS it mustn't be:
[platform support](https://github.com/fastipc/fastipc/blob/main/docs/platform-support.md)).

### Shipping your program

A program started outside Cargo looks for the library itself:

- **Windows:** put `fastipc.dll` next to the `.exe` (Windows looks there first).
- **Linux:** put `libfastipc.so` next to the executable and link it with an rpath of `$ORIGIN`, for example in your
  project's `.cargo/config.toml`:

  ```toml
  [target.x86_64-unknown-linux-gnu]
  rustflags = ["-C", "link-arg=-Wl,-rpath,$ORIGIN"]
  ```

  (on ARM64, the same under `[target.aarch64-unknown-linux-gnu]`)

  or install it where the loader looks (`LD_LIBRARY_PATH`, a library folder).
- **macOS:** put `libfastipc.dylib` next to the executable and link it with an rpath of `@loader_path` (the library's
  install name is `@rpath/libfastipc.dylib`), for example in your project's `.cargo/config.toml`:

  ```toml
  [target.aarch64-apple-darwin]
  rustflags = ["-C", "link-arg=-Wl,-rpath,@loader_path"]
  ```

After a build, the library is in `target/<profile>/build/fipc-sys-<hash>/out/`; a build script of a crate that
depends on `fipc-sys` directly gets that folder as `DEP_FASTIPC_LIB_DIR`. It is the same file for every
program of a version (the crate's own copy, or the one `FASTIPC_LIB_DIR` names).

### Performance

Between two processes, copied 16-byte messages run at 46 to 58 million per second through the binding on the
baseline machine, and 64 KiB messages at 450,000 to 470,000 per second
(`bench/rust`; the numbers are in
[`docs/perf/bindings-baseline.md`](https://github.com/fastipc/fastipc/blob/main/docs/perf/bindings-baseline.md)).
The calls that take your buffer (`send`, `receive_into`, the zero-copy slots, `rpc_submit`, `rpc_respond`,
`rpc_receive_into`) add no allocation, lock or atomic operation to the C call: the safety comes from the borrow checker,
at compile time.

## Building from source

In a checkout of the repository, with the library built (`python devtool.py build`):

```bash
cd bindings/rust
cargo test           # unit, integration and doc tests against the repository's zig-out build, with peers in other
                     #   processes and a Python peer (the repository's venv) for the cross-language tests
cargo clippy --all-targets -- -D warnings
cargo doc --open
```

The workspace pins the current stable toolchain (`rust-toolchain.toml`); the crates build with Rust 1.85 or newer.
`python devtool.py package rust --smoke` packages both crates with every platform's library in `zig-out/packages`,
checks them, and makes a round trip through them from a fresh Cargo project, also run on its own next to its library.

## Documentation

- [The website](https://fastipc.github.io/fastipc/rust.html): this binding's guide, with examples of every call.
- [docs.rs/fipc](https://docs.rs/fipc): the API reference.
- [`include/fipc.h`](https://github.com/fastipc/fastipc/blob/main/include/fipc.h): the C API under this binding,
  and every call's contract (results, timeouts, threads, messages in pieces, the peer's end).
- [Platform support](https://github.com/fastipc/fastipc/blob/main/docs/platform-support.md) and the
  [protocol](https://github.com/fastipc/fastipc/blob/main/docs/protocol.md).
- [Changelog](https://github.com/fastipc/fastipc/blob/main/CHANGELOG.md),
  [issues](https://github.com/fastipc/fastipc/issues).

## License

MIT. Copyright (c) 2025-2026 Hayden Donnelly. See [LICENSE](https://github.com/fastipc/fastipc/blob/main/LICENSE).
