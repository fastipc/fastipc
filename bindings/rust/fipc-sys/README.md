# fipc-sys

[![crates.io](https://img.shields.io/crates/v/fipc-sys.svg)](https://crates.io/crates/fipc-sys)
[![docs.rs](https://img.shields.io/docsrs/fipc-sys)](https://docs.rs/fipc-sys)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/fastipc/fastipc/blob/main/LICENSE)

Raw FFI bindings to [FastIPC](https://github.com/fastipc/fastipc): shared-memory messages between two processes on
one machine, broadcasts from one publisher to many subscribers, work queues from many producers to many consumers, and
services from many clients to a pool of servers, with replies. The 30 functions of its streams, broadcasts, work queues
and services, the result codes and the members' roles of
[`include/fipc.h`](https://github.com/fastipc/fastipc/blob/main/include/fipc.h), declared by hand (no bindgen).

**For the safe API, use [`fipc`](https://crates.io/crates/fipc)**, which is built on this crate and
re-exports it as `fipc::sys`. This crate is for code that calls the C API itself; every call is `unsafe`, and
the header's rules on pointers, lifetimes and threads are the caller's to keep.

```toml
[dependencies]
fipc-sys = "1.0"
```

```rust
// server.rs
use fipc_sys::*;
use std::ptr;

fn main() {
    let mut listener = ptr::null_mut();
    let mut conn = ptr::null_mut();
    let mut buf = [0u8; 256];
    let mut len = 0;
    // SAFETY: the pointers are valid, and each handle is closed once, by the thread that uses it.
    unsafe {
        assert_eq!(fipc_stream_create(c"my_channel".as_ptr(), 1 << 20, &mut listener), FIPC_OK); // rings of 1 MiB each way
        assert_eq!(fipc_stream_accept(listener, &mut conn, FIPC_FOREVER), FIPC_OK); // waits for a client
        if fipc_recv(conn, buf.as_mut_ptr().cast(), buf.len(), &mut len, FIPC_FOREVER) == FIPC_OK {
            let message = &mut buf[..len];
            message.make_ascii_uppercase();
            fipc_send(conn, message.as_ptr().cast(), message.len(), FIPC_FOREVER);
        }
        fipc_close(conn);
        fipc_listener_close(listener);
    }
}
```

```rust
// client.rs
use fipc_sys::*;
use std::ptr;

fn main() {
    let mut conn = ptr::null_mut();
    let mut buf = [0u8; 256];
    let mut len = 0;
    // SAFETY: the pointers are valid, and the handle is closed once, by the thread that uses it.
    unsafe {
        assert_eq!(fipc_stream_connect(c"my_channel".as_ptr(), &mut conn, 5000), FIPC_OK);
        assert_eq!(fipc_send(conn, b"ping".as_ptr().cast(), 4, FIPC_FOREVER), FIPC_OK);
        assert_eq!(fipc_recv(conn, buf.as_mut_ptr().cast(), buf.len(), &mut len, FIPC_FOREVER), FIPC_OK);
        fipc_close(conn);
    }
    println!("{}", String::from_utf8_lossy(&buf[..len])); // PING
}
```

Run each in its own terminal (in a checkout of the repository, they are the `fipc` crate's examples
`sys_server` and `sys_client`). The client sends the message `ping`, and the server answers with its
uppercase, `PING`: the exchange every example of the
[repository's README](https://github.com/fastipc/fastipc#examples) makes, so a client or server in any language
with a binding works against these.

## The native library

The crate carries `fastipc.dll` (Windows x64), `libfastipc.so` (Linux x64, glibc 2.34+), both for x86-64-v3 CPUs,
`libfastipc.so` (Linux arm64, glibc 2.34+) and `libfastipc.dylib` (macOS 14.4+ on Apple Silicon), built from the
[FastIPC](https://github.com/fastipc/fastipc) repository. Its build script links the
library dynamically (on Windows through `raw-dylib`: no import library needed) and copies it into Cargo's build
folder, where `cargo run` and `cargo test` find it. The library comes from, in this order: the folder the environment
variable `FASTIPC_LIB_DIR` names; in a checkout of the repository, its `zig-out` build; the crate's own copy. The build
scripts of crates that depend on this one directly get the folder of the copy as `DEP_FASTIPC_LIB_DIR`. On docs.rs
nothing links.

A program started outside Cargo needs the library next to it (Windows) or on its library path (Linux, for example an
rpath of `$ORIGIN`; macOS, an rpath of `@loader_path`): see
[Shipping your program](https://github.com/fastipc/fastipc/tree/main/bindings/rust#shipping-your-program).

## License

MIT. Copyright (c) 2025-2026 Hayden Donnelly. See [LICENSE](https://github.com/fastipc/fastipc/blob/main/LICENSE).
