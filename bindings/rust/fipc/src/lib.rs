//! Fast messages and RPC between two processes on one machine, through shared memory: the Rust binding of
//! [FastIPC](https://github.com/fastipc/fastipc), a small library written in Zig.
//!
//! A server listens on a name and accepts one client at a time; a client connects to the name. Each
//! [`Connection`] has a shared-memory segment with one ring per direction, and reports the peer's end as soon as its
//! process exits or drops the connection. The peer can be written in any language with a binding: a Rust process
//! talks to a Zig, C, C++, Python, C#, Java, Lua, JavaScript or Go process exactly as it talks to another Rust
//! process.
//!
//! ```no_run
//! // server.rs
//! use fipc::Listener;
//!
//! fn main() -> fipc::Result<()> {
//!     let mut listener = Listener::listen("my_channel", 1 << 20)?; // rings of 1 MiB each way
//!     let mut conn = listener.accept(None)?; // waits for a client
//!     let request = conn.rpc_receive(None)?;
//!     conn.rpc_respond(request.id, request.opcode, 0, &request.payload.to_ascii_uppercase(), None)?;
//!     Ok(())
//! }
//! ```
//!
//! ```no_run
//! // client.rs
//! use fipc::Connection;
//! use std::time::Duration;
//!
//! fn main() -> fipc::Result<()> {
//!     let mut conn = Connection::connect("my_channel", Duration::from_secs(5))?;
//!     conn.rpc_submit(1, b"ping", None)?; // opcode 1
//!     println!("{}", String::from_utf8_lossy(&conn.rpc_receive(None)?.payload)); // PING
//!     Ok(())
//! }
//! ```
//!
//! Run each in a process of its own (in the repository, they are the crate's examples `server` and `client`). The
//! client sends an RPC request with opcode 1 and the payload `ping`, and the server answers with the payload in upper
//! case: the exchange every example of the repository makes, so a client or a server in any other language works
//! against these.
//!
//! # Messages
//!
//! - **Plain messages:** [`send`](Connection::send) a `&[u8]` (at least 1 byte, any size: a message longer than one
//!   piece of the ring travels in pieces and arrives whole); [`receive`](Connection::receive) returns a new
//!   `Vec<u8>`, [`receive_into`](Connection::receive_into) fills your buffer and returns the length.
//! - **Zero-copy** (messages of one piece, up to [`max_piece`](Connection::max_piece) bytes):
//!   [`send_acquire`](Connection::send_acquire) returns a [`SendSlot`] in the ring to write the message into, then
//!   [`commit`](SendSlot::commit) it; [`receive_acquire`](Connection::receive_acquire) returns a [`ReceiveSlot`] that
//!   reads the message in the ring and frees its room when dropped. The borrow checker keeps a slot from outliving
//!   its turn: while it lives, its side of the connection can't make another call.
//! - **RPC:** [`rpc_submit`](Connection::rpc_submit) returns the request's id,
//!   [`rpc_respond`](Connection::rpc_respond) answers one, [`rpc_receive`](Connection::rpc_receive) returns an
//!   [`RpcMessage`] and [`rpc_receive_into`](Connection::rpc_receive_into) an [`RpcHeader`] with the payload in your
//!   buffer. Use a connection for plain messages or for RPC, not both.
//!
//! Messages arrive in the order they were sent.
//!
//! # Timeouts and errors
//!
//! Every call that can wait takes its timeout last, as anything that converts into an `Option<Duration>`: a
//! [`Duration`], [`NO_WAIT`] (doesn't wait), or `None` / [`FOREVER`] (waits as long as it
//! takes). Waits are counted in whole milliseconds, rounded up. A call that doesn't succeed returns an [`Error`]:
//! [`Error::Timeout`] when the timeout ran out (the call changed nothing), [`Error::Disconnected`] at the peer's end,
//! and so on.
//!
//! ```
//! # use fipc::{Connection, Error, Listener, NO_WAIT};
//! # fn main() -> fipc::Result<()> {
//! # let mut listener = Listener::listen("fipc_doc_poll", 1 << 16)?;
//! # let client = std::thread::spawn(|| Connection::connect("fipc_doc_poll", None));
//! # let _server = listener.accept(None)?;
//! # let mut conn = client.join().unwrap()?;
//! // Polling, once a frame: whatever has arrived, without waiting
//! loop {
//!     match conn.receive(NO_WAIT) {
//!         Ok(message) => println!("{} bytes", message.len()),
//!         Err(Error::Timeout) => break, // nothing more for now
//!         Err(e) => return Err(e),      // Error::Disconnected: the peer is gone
//!     }
//! }
//! # Ok(())
//! # }
//! ```
//!
//! A copying call's timeout bounds its wait for the first piece; once a piece has moved, the call goes on until the
//! whole message has, and only a cancel or the peer's end stops it (the message is then dropped whole: the receiver
//! never sees part of one).
//!
//! # Threads
//!
//! [`Listener`], [`Connection`] and its halves are `Send` and `Sync`, so they can live in another thread or in a
//! shared resource (a Bevy `Resource`, an `Arc`). The calls that send or receive take `&mut self`, which is how the
//! C API's rule "one thread at a time sends, and one thread at a time receives" is kept: to send and receive at once
//! on two threads, [`split`](Connection::split) the connection into its [`Sender`] and [`Receiver`] (borrowed) or
//! [`into_split`](Connection::into_split) it (owned). The native connection closes when the last of them is
//! dropped.
//!
//! To stop a thread that waits in [`accept`](Listener::accept) or a receive, take a [`Canceler`] first
//! ([`Listener::canceler`], [`Connection::canceler`]) and call [`Canceler::cancel`] from any thread: every call that
//! waits, then or later, returns [`Error::Cancelled`]. Then join the thread and drop the handle. A canceler doesn't
//! keep the connection open; once it is closed, cancelling does nothing.
//!
//! # The peer's end
//!
//! [`Error::Disconnected`]: the peer dropped the connection, or its process ended, for any reason (crashed or
//! killed included). There is no heartbeat and no timeout: a paused or hung peer is not reported. It is final; to
//! talk again, accept or connect a new connection.
//!
//! # The native library
//!
//! The [`fipc-sys`](sys) crate links `fastipc.dll` / `libfastipc.so` / `libfastipc.dylib` (the published crate
//! carries all four, for Windows x64, Linux x64 and arm64 with glibc 2.34+ and macOS 14.4+ on Apple Silicon), and
//! `cargo run` and `cargo test` find it. A program started outside Cargo needs the library next to it (Windows) or on
//! its library path (Linux, macOS: an rpath):
//! [Shipping your program](https://github.com/fastipc/fastipc/tree/main/bindings/rust#shipping-your-program).
//! On x86-64 the CPU must support x86-64-v3 (AVX2).
//!
//! # Safety
//!
//! The API is safe. Zero-copy slots point into the shared ring, in a region the protocol gives to this side alone
//! until the commit or release; a peer that breaks the protocol can change those bytes while they are read, but it
//! can't reach the rest of this process's memory, and the library checks everything else it reads from the ring.

#![deny(unsafe_op_in_unsafe_fn)]

mod connection;
mod error;
mod listener;
mod rpc;

use std::ffi::{CString, c_int};
use std::time::Duration;

pub use connection::{Canceler, Connection, ReceiveSlot, Receiver, SendSlot, Sender};
pub use error::{Error, Result};
pub use fipc_sys as sys;
pub use listener::Listener;
pub use rpc::{RpcHeader, RpcKind, RpcMessage};

/// A timeout that doesn't wait: the call does what it can at once, or returns [`Error::Timeout`].
pub const NO_WAIT: Option<Duration> = Some(Duration::ZERO);

/// A timeout that waits as long as it takes (as `None` does).
pub const FOREVER: Option<Duration> = None;

/// `timeout` as the C API's milliseconds: whole milliseconds, rounded up; `None` and anything from `i32::MAX`
/// milliseconds (24.8 days) on wait for ever.
fn millis(timeout: impl Into<Option<Duration>>) -> c_int {
    match timeout.into() {
        None => sys::FIPC_FOREVER,
        Some(duration) => {
            let ms = duration.as_nanos().div_ceil(1_000_000);
            if ms >= c_int::MAX as u128 { sys::FIPC_FOREVER } else { ms as c_int }
        }
    }
}

/// `name` as a C string; [`Error::Invalid`] for one with a NUL character, which no name may hold.
fn c_name(name: &str) -> Result<CString> {
    CString::new(name).map_err(|_| Error::Invalid)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timeouts_in_milliseconds() {
        assert_eq!(millis(None), -1);
        assert_eq!(millis(FOREVER), -1);
        assert_eq!(millis(NO_WAIT), 0);
        assert_eq!(millis(Duration::ZERO), 0);
        assert_eq!(millis(Duration::from_nanos(1)), 1); // a partial millisecond waits a whole one
        assert_eq!(millis(Duration::from_micros(1500)), 2);
        assert_eq!(millis(Duration::from_secs(5)), 5000);
        assert_eq!(millis(Some(Duration::from_millis(i32::MAX as u64 - 1))), i32::MAX - 1);
        assert_eq!(millis(Duration::from_millis(i32::MAX as u64)), -1);
        assert_eq!(millis(Duration::MAX), -1);
    }

    #[test]
    fn handles_are_send_and_sync() {
        fn send_sync<T: Send + Sync>() {}
        send_sync::<Listener>();
        send_sync::<Connection>();
        send_sync::<Sender>();
        send_sync::<Receiver>();
        send_sync::<Canceler>();
        send_sync::<SendSlot<'static>>();
        send_sync::<ReceiveSlot<'static>>();
        send_sync::<Error>();
    }
}
