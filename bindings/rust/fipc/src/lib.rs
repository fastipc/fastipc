//! Fast messages between processes on one machine, through shared memory: the Rust binding of
//! [FastIPC](https://github.com/fastipc/fastipc), a small library written in Zig.
//!
//! Four kinds, each in a module of its own: a [stream] connects a server and one client at a time, a
//! [broadcast](#broadcasts) sends one publisher's messages to many subscribers, a [work queue](#work-queues) hands
//! each message of many producers to one of many consumers, and a [service](#services) hands each request of many
//! clients to one of a pool of servers, and its reply back to its client. Every [`Connection`] reports the peer's end
//! as soon as its process exits or drops the connection. The peer can be written in any language with a binding: a
//! Rust process talks to a Zig, C, C++, Python, C#, Java, Lua, JavaScript or Go process exactly as it talks to another
//! Rust process.
//!
//! ```no_run
//! // server.rs
//! fn main() -> fipc::Result<()> {
//!     let mut listener = fipc::stream::create("my_channel", 1 << 20)?; // rings of 1 MiB each way
//!     let mut conn = listener.accept(None)?; // waits for a client
//!     let message = conn.receive(None)?;
//!     conn.send(&message.to_ascii_uppercase(), None)?;
//!     Ok(())
//! }
//! ```
//!
//! ```no_run
//! // client.rs
//! use std::time::Duration;
//!
//! fn main() -> fipc::Result<()> {
//!     let mut conn = fipc::stream::connect("my_channel", Duration::from_secs(5))?;
//!     conn.send(b"ping", None)?;
//!     println!("{}", String::from_utf8_lossy(&conn.receive(None)?)); // PING
//!     Ok(())
//! }
//! ```
//!
//! Run each in a process of its own (in the repository, they are the crate's examples `server` and `client`). The
//! client sends the message `ping`, and the server answers with its uppercase, `PING`: the exchange every example of
//! the repository makes, so a client or a server in any other language works against these.
//!
//! # Messages
//!
//! - **Copied messages:** [`send`](Connection::send) a `&[u8]` (at least 1 byte, any size: a message longer than one
//!   piece of the ring travels in pieces and arrives whole); [`receive`](Connection::receive) returns a new
//!   `Vec<u8>`, [`receive_into`](Connection::receive_into) fills your buffer and returns the length.
//! - **Zero-copy** (messages of one piece, up to [`max_piece`](Connection::max_piece) bytes):
//!   [`send_acquire`](Connection::send_acquire) returns a [`SendSlot`] in the ring to write the message into, then
//!   [`commit`](SendSlot::commit) it; [`receive_acquire`](Connection::receive_acquire) returns a [`ReceiveSlot`] that
//!   reads the message in the ring and frees its room when dropped. The borrow checker keeps a slot from outliving
//!   its turn: while it lives, its side of the connection can't make another call.
//! - **Batches:** [`send_many`](Connection::send_many) sends a slice of messages, published together with at most one
//!   wake of the receivers; [`receive_many`](Connection::receive_many) takes every message already there in one call,
//!   packed into your buffer ([`batch_messages`] walks them). Batch both ends: a receiver that takes one message per
//!   call falls behind a batching sender.
//!
//! On a stream, messages arrive in the order they were sent.
//!
//! # Broadcasts
//!
//! A broadcast sends every message of one publisher to each of up to 1024 subscribers, in any processes of the user;
//! each message is stored once, in memory every subscriber maps, and is of one piece (up to a quarter of the capacity,
//! [`max_piece`](Connection::max_piece)). [`broadcast::create`] returns the publisher, a [`Connection`] whose sends go
//! to every subscriber, and the broadcast's one handle. A subscriber [`broadcast::join`]s as a [`SubscriberRole`]; the
//! publisher's process admits it on a thread of the library's:
//!
//! - [`SubscriberRole::Gated`] misses nothing (the publisher waits for it); one that holds the publisher back for the
//!   broadcast's `max_stall` is unsubscribed ([`Error::Unsubscribed`]). It may receive in place with the unsafe
//!   [`receive_acquire_gated`](Connection::receive_acquire_gated) (see [Safety](#safety)).
//! - [`SubscriberRole::Lossy`] never holds the publisher back: one that falls a lap behind gets [`Error::Overrun`]
//!   with the number of messages it lost, and goes on from a recent message.
//!
//! [`wait_for_members`](Connection::wait_for_members) counts the joined subscribers on the publisher, and waits for a
//! number of them, so that nothing is sent before they are there; a [`MembersError`] carries the count at a timeout.
//! Dropping the publisher ends the broadcast: each subscriber receives what was sent, then [`Error::Disconnected`].
//!
//! ```no_run
//! // publisher.rs
//! use std::time::Duration;
//!
//! fn main() -> fipc::Result<()> {
//!     // 64 KiB of messages, up to 8 subscribers; one that holds the publisher back 5 s is dropped
//!     let mut publisher = fipc::broadcast::create("my_broadcast", 1 << 16, 8, Duration::from_secs(5))?;
//!     publisher.wait_for_members(1, None)?; // waits for the first subscriber
//!     for i in 1..=3 {
//!         publisher.send(format!("tick {i}").as_bytes(), None)?;
//!     }
//!     Ok(())
//! } // dropping the publisher ends the broadcast: subscribers get Error::Disconnected after the ticks
//! ```
//!
//! ```no_run
//! // subscriber.rs
//! use fipc::{Error, SubscriberRole};
//! use std::time::Duration;
//!
//! fn main() -> fipc::Result<()> {
//!     // gated: misses nothing (the publisher waits for it)
//!     let mut subscriber = fipc::broadcast::join("my_broadcast", SubscriberRole::Gated, Duration::from_secs(5))?;
//!     loop {
//!         match subscriber.receive(None) {
//!             Ok(tick) => println!("{}", String::from_utf8_lossy(&tick)), // tick 1, tick 2, tick 3
//!             Err(Error::Disconnected) => return Ok(()),                  // the publisher closed
//!             Err(e) => return Err(e),
//!         }
//!     }
//! }
//! ```
//!
//! Run the publisher, then the subscriber (in the repository, they are the crate's examples `publisher` and
//! `subscriber`), or the other way round: `join` waits up to its timeout for the publisher.
//!
//! # Work queues
//!
//! A work queue takes messages from up to 63 producers and hands each to one of up to 64 consumers, in any processes
//! of the user. Each producer has a ring of its own in the queue's memory; a consumer takes a message of any producer:
//! the queue promises no order. Messages are of one piece (up to a quarter of the capacity,
//! [`max_piece`](Connection::max_piece)) and wait in the queue until a consumer takes them. A message goes again, to
//! another consumer, if the consumer that took it ends before acknowledging it: a copying receive acknowledges before
//! it returns (at most once), one in place when its slot is dropped (dropped after the work: at least once, so the work
//! must tolerate a duplicate).
//!
//! [`queue::create`] returns the [`Queue`], the owner's handle. Producers and consumers [`queue::join`] as a
//! [`QueueRole`] ([`QueueRole::Producer`] sends, [`QueueRole::Consumer`] receives, copied or in place), the owner too:
//! its process admits every joiner on a thread of the library's. [`Queue::wait_for_members`] counts them and waits
//! for a number of them. **A queue lives until its `Queue` is [closed](Queue::close) or dropped, or the owner's
//! process ends:** keep it for as long as the queue is to run. Producers then get [`Error::Disconnected`], and
//! consumers receive every message left in the queue, then [`Error::Disconnected`]. A producer's or a consumer's drop
//! never ends the queue.
//!
//! ```no_run
//! // producer.rs
//! use fipc::QueueRole;
//! use std::time::Duration;
//!
//! fn main() -> fipc::Result<()> {
//!     // 4 KiB of messages per producer, 1 producer and 1 consumer: this process owns the queue and is its producer
//!     let queue = fipc::queue::create("my_queue", 4096, 1, 1)?;
//!     let mut producer = fipc::queue::join("my_queue", QueueRole::Producer, Duration::from_secs(5))?;
//!     for i in 1..=3 {
//!         producer.send(format!("job {i}").as_bytes(), None)?; // waits in the queue until a consumer takes it
//!     }
//!     queue.wait_for_members(QueueRole::Consumer, 1, None)?; // waits for the consumer to join
//!     Ok(())
//! } // dropping the producer, then the queue, ends the queue: the consumer gets Error::Disconnected after the jobs
//! ```
//!
//! ```no_run
//! // consumer.rs
//! use fipc::{Error, QueueRole};
//! use std::time::Duration;
//!
//! fn main() -> fipc::Result<()> {
//!     let mut consumer = fipc::queue::join("my_queue", QueueRole::Consumer, Duration::from_secs(5))?;
//!     loop {
//!         match consumer.receive(None) {
//!             Ok(job) => println!("{}", String::from_utf8_lossy(&job)), // job 1, job 2, job 3
//!             Err(Error::Disconnected) => return Ok(()),                // the queue ended
//!             Err(e) => return Err(e),
//!         }
//!     }
//! }
//! ```
//!
//! Run the producer, then the consumer (in the repository, they are the crate's examples `producer` and `consumer`),
//! or the other way round: `join` waits up to its timeout for the owner, and the jobs wait in the queue for the
//! consumer.
//!
//! # Services
//!
//! A service takes requests from up to 64 clients and hands each to one of up to 64 servers, in any processes of the
//! user; the server that took a request replies to it, and the reply goes back to the client that sent it. A server
//! waits for the requests of every client at once (one receive), and the servers of a pool share the load. Each client
//! has a ring of its own in the service's memory, and a request takes room there for itself and for its reply, so a
//! server never waits for a client. Requests and replies are of one piece (up to the service's `max_message`,
//! [`max_piece`](Connection::max_piece)).
//!
//! [`service::create`] returns the [`Service`], the owner's handle. Clients and servers [`service::join`] as a
//! [`ServiceRole`], the owner too: its process admits every joiner on a thread of the library's.
//! [`ServiceRole::Client`] sends requests and receives their replies, in the order of its requests:
//! [`call`](Connection::call) does both, and never returns the reply to another request. [`ServiceRole::Server`]
//! receives the next request of any client and holds it until its next send, the reply, which never waits;
//! [`receive_request`](Connection::receive_request) holds it in place, as a [`Request`] that its reply consumes. A
//! request whose server ended before it replied is answered [`Error::Abandoned`]: it may or may not have been carried
//! out, and the service never sends it again. [`Service::wait_for_members`] counts the members and waits for a number
//! of them. **A service lives until its `Service` is [closed](Service::close) or dropped, or the owner's process
//! ends:** keep it for as long as the service is to run. Clients' sends and servers' receives then get
//! [`Error::Disconnected`], and a client receives the replies that came before the end, then
//! [`Error::Disconnected`]. A client's or a server's drop never ends the service.
//!
//! ```no_run
//! // service_server.rs
//! use fipc::ServiceRole;
//! use std::time::Duration;
//!
//! fn main() -> fipc::Result<()> {
//!     // 4 KiB per client, requests and replies of up to 256 bytes, 1 client and 1 server: this process owns the
//!     // service and is its server
//!     let service = fipc::service::create("my_service", 4096, 256, 1, 1)?;
//!     let mut server = fipc::service::join("my_service", ServiceRole::Server, Duration::from_secs(5))?;
//!     for _ in 0..3 {
//!         // three requests, then the end
//!         let request = server.receive(None)?; // the next request of any client
//!         server.send(&request.to_ascii_uppercase(), None)?; // the reply goes to its client, and never waits
//!     }
//!     service.close(); // ends the service: the client still receives the replies sent
//!     Ok(())
//! }
//! ```
//!
//! ```no_run
//! // service_client.rs
//! use fipc::ServiceRole;
//! use std::time::Duration;
//!
//! fn main() -> fipc::Result<()> {
//!     let mut client = fipc::service::join("my_service", ServiceRole::Client, Duration::from_secs(5))?;
//!     for i in 1..=3 {
//!         // sends the request and returns its reply; Error::Abandoned if the server ended before it replied
//!         let reply = client.call(format!("ping {i}").as_bytes(), None)?;
//!         println!("{}", String::from_utf8_lossy(&reply)); // PING 1, PING 2, PING 3
//!     }
//!     Ok(())
//! }
//! ```
//!
//! Run the server, then the client (in the repository, they are the crate's examples `service_server` and
//! `service_client`), or the other way round: `join` waits up to its timeout for the owner.
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
//! # use fipc::{Error, NO_WAIT};
//! # fn main() -> fipc::Result<()> {
//! # let mut listener = fipc::stream::create("fipc_doc_poll", 1 << 16)?;
//! # let client = std::thread::spawn(|| fipc::stream::connect("fipc_doc_poll", None));
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
//! [`Listener`], [`Connection`] and its halves, [`Queue`] and [`Service`] are `Send` and `Sync`, so they can live in
//! another thread or in a shared resource (a Bevy `Resource`, an `Arc`). The calls that send or receive take `&mut self`, which is how the
//! C API's rule "one thread at a time sends, and one thread at a time receives" is kept: to send and receive at once
//! on two threads, [`split`](Connection::split) the connection into its [`Sender`] and [`Receiver`] (borrowed) or
//! [`into_split`](Connection::into_split) it (owned). The native connection closes when the last of them is
//! dropped.
//!
//! To stop a thread that waits in [`accept`](Listener::accept), a receive, a call or a wait for members, take a
//! [`Canceler`] first ([`Listener::canceler`], [`Connection::canceler`], [`Queue::canceler`], [`Service::canceler`])
//! and call [`Canceler::cancel`] from any thread: every call that waits, then or later, returns [`Error::Cancelled`].
//! Then join the thread and drop the handle. A canceler doesn't keep its handle open; once it is closed, cancelling
//! does nothing.
//!
//! # The peer's end
//!
//! [`Error::Disconnected`]: the peer dropped the connection, or its process ended, for any reason (crashed or
//! killed included); on a broadcast, the publisher's end; on a work queue, the queue's; on a service, the service's
//! (a server's end answers the request it held [`Error::Abandoned`]). There is no heartbeat and no timeout: a paused
//! or hung peer is not reported. It is final; to talk again, accept, connect or join anew.
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
//! The API is safe, but for one call: [`Connection::receive_acquire_gated`]. Zero-copy slots point into the shared
//! ring, in a region the protocol gives to this side alone until the commit or release; a peer that breaks the
//! protocol can change those bytes while they are read, but it can't reach the rest of this process's memory, and the
//! library checks everything else it reads from the ring. A broadcast's gated subscriber is different: once it has
//! held the publisher back for the broadcast's `max_stall`, it is unsubscribed and the broadcast's memory becomes
//! inaccessible to it, at a moment the publisher's clock picks, so a slot it reads in place can vanish. Its in-place
//! receive is therefore `unsafe`, and the safe [`receive_acquire`](Connection::receive_acquire) refuses it.

#![deny(unsafe_op_in_unsafe_fn)]

pub mod broadcast;
mod connection;
mod error;
mod listener;
pub mod queue;
pub mod service;
pub mod stream;

use std::ffi::{CString, c_int};
use std::time::Duration;

pub use connection::{
    Canceler, Connection, QueueRole, ReceiveSlot, Receiver, Request, SendSlot, Sender, ServiceRole, SubscriberRole,
};
pub use error::{Error, MembersError, Result, SendManyError};
pub use fipc_sys as sys;
pub use listener::Listener;
pub use queue::Queue;
pub use service::Service;

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

/// The messages of a batch that [`Connection::receive_many`] packed into `buf`, in order: message i is `lens[i]`
/// bytes, at the sum of the lengths before it. Pass the lengths of the messages it took, `&lens[..count]`.
///
/// ```
/// let buf = b"onetwothree";
/// let lens = [3, 3, 5];
/// let messages: Vec<&[u8]> = fipc::batch_messages(buf, &lens).collect();
/// assert_eq!(messages, [&b"one"[..], b"two", b"three"]);
/// ```
pub fn batch_messages<'a>(buf: &'a [u8], lens: &'a [usize]) -> impl Iterator<Item = &'a [u8]> + 'a {
    lens.iter().scan(0, move |at: &mut usize, &len| {
        let message = &buf[*at..*at + len];
        *at += len;
        Some(message)
    })
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
        send_sync::<Queue>();
        send_sync::<Service>();
        send_sync::<Request<'static>>();
        send_sync::<Canceler>();
        send_sync::<SendSlot<'static>>();
        send_sync::<ReceiveSlot<'static>>();
        send_sync::<Error>();
        send_sync::<MembersError>();
    }

    #[test]
    fn broadcast_errors() {
        let overrun = Error::Overrun { lost: u64::MAX };
        assert_eq!(overrun.code(), sys::FIPC_OVERRUN);
        assert_eq!(overrun.name(), "FIPC_OVERRUN");
        assert!(overrun.to_string().contains("18446744073709551615 messages lost"), "{overrun}");
        assert_eq!(Error::from_code(sys::FIPC_OVERRUN, 7), Error::Overrun { lost: 7 });
        assert_eq!(Error::from_code(sys::FIPC_UNSUBSCRIBED, 0), Error::Unsubscribed);
        assert_eq!(Error::Unsubscribed.name(), "FIPC_UNSUBSCRIBED");
        assert_eq!(std::io::Error::from(Error::Unsubscribed).kind(), std::io::ErrorKind::ConnectionAborted);

        let timeout = MembersError { error: Error::Timeout, count: 3 };
        assert_eq!(timeout.to_string(), "FIPC_TIMEOUT: the timeout ran out (3 members joined)");
        assert_eq!(Error::from(timeout), Error::Timeout);
        assert_eq!(std::io::Error::from(timeout).kind(), std::io::ErrorKind::TimedOut);
    }

    #[test]
    fn service_errors() {
        assert_eq!(Error::Abandoned.code(), sys::FIPC_ABANDONED);
        assert_eq!(Error::from_code(sys::FIPC_ABANDONED, 0), Error::Abandoned);
        assert_eq!(Error::Abandoned.name(), "FIPC_ABANDONED");
        assert_eq!(
            Error::Abandoned.to_string(),
            "FIPC_ABANDONED: the server that took the request ended before it replied"
        );
        assert_eq!(std::io::Error::from(Error::Abandoned).kind(), std::io::ErrorKind::ConnectionAborted);
    }
}
