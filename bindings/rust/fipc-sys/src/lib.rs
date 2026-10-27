//! Raw FFI bindings to FastIPC: the 30 functions of its streams, broadcasts, work queues and services, the result codes
//! and the members' roles of [`include/fipc.h`](https://github.com/fastipc/fastipc/blob/main/include/fipc.h), declared
//! by hand.
//!
//! FastIPC connects processes of the same user on one machine through shared memory. A stream's server creates a name
//! and accepts one client at a time, a client connects to the name, and each connection has one ring per direction
//! ([`fipc_stream_create`], [`fipc_stream_accept`], [`fipc_stream_connect`]). A broadcast sends one publisher's
//! messages to many subscribers ([`fipc_broadcast_create`], [`fipc_broadcast_join`]); a work queue hands each message
//! of many producers to one of many consumers ([`fipc_queue_create`], [`fipc_queue_join`]); a service hands each
//! request of many clients to one of a pool of servers, and its reply back to its client ([`fipc_service_create`],
//! [`fipc_service_join`]).
//! The library is `fastipc.dll` (Windows x64), `libfastipc.so` (Linux x64 and arm64, glibc 2.34+) or `libfastipc.dylib`
//! (macOS 14.4+ on Apple Silicon), built from the [FastIPC](https://github.com/fastipc/fastipc) repository.
//!
//! **Most programs want the safe API instead: the [`fipc`](https://docs.rs/fipc) crate.** This crate
//! is its foundation, for code that calls the C API itself. Every function is `unsafe` (except
//! [`fipc_result_str`], which returns a pointer): the caller keeps the header's rules on pointers, lifetimes and threads, which are summed up
//! on each function here and given in full in the header.
//!
//! # Linking
//!
//! The build script links the shared library dynamically (on Windows with `raw-dylib`, so no import library is
//! needed) and copies it into Cargo's build folder, where `cargo run` and `cargo test` find it. It takes the library
//! from, in this order: the folder the environment variable `FASTIPC_LIB_DIR` names; in a checkout of the repository,
//! its `zig-out` build; the copy for this platform that the crate carries. A program run outside Cargo needs the
//! library next to it (Windows) or on its library path (Linux, macOS; for example an rpath of `$ORIGIN` or
//! `@loader_path`): see the
//! [`fipc` README](https://github.com/fastipc/fastipc/tree/main/bindings/rust#shipping-your-program).
//! Build scripts of crates that depend on this one directly get the folder of that copy as `DEP_FASTIPC_LIB_DIR`.
//!
//! # Example
//!
//! ```no_run
//! // server.rs
//! use fipc_sys::*;
//! use std::ptr;
//!
//! fn main() {
//!     let mut listener = ptr::null_mut();
//!     let mut conn = ptr::null_mut();
//!     let mut buf = [0u8; 256];
//!     let mut len = 0;
//!     // SAFETY: the pointers are valid, and each handle is closed once, by the thread that uses it.
//!     unsafe {
//!         assert_eq!(fipc_stream_create(c"my_channel".as_ptr(), 1 << 20, &mut listener), FIPC_OK); // rings of 1 MiB each way
//!         assert_eq!(fipc_stream_accept(listener, &mut conn, FIPC_FOREVER), FIPC_OK); // waits for a client
//!         if fipc_recv(conn, buf.as_mut_ptr().cast(), buf.len(), &mut len, FIPC_FOREVER) == FIPC_OK {
//!             let message = &mut buf[..len];
//!             message.make_ascii_uppercase();
//!             fipc_send(conn, message.as_ptr().cast(), message.len(), FIPC_FOREVER);
//!         }
//!         fipc_close(conn);
//!         fipc_listener_close(listener);
//!     }
//! }
//! ```
//!
//! ```no_run
//! // client.rs
//! use fipc_sys::*;
//! use std::ptr;
//!
//! fn main() {
//!     let mut conn = ptr::null_mut();
//!     let mut buf = [0u8; 256];
//!     let mut len = 0;
//!     // SAFETY: the pointers are valid, and the handle is closed once, by the thread that uses it.
//!     unsafe {
//!         assert_eq!(fipc_stream_connect(c"my_channel".as_ptr(), &mut conn, 5000), FIPC_OK);
//!         assert_eq!(fipc_send(conn, b"ping".as_ptr().cast(), 4, FIPC_FOREVER), FIPC_OK);
//!         assert_eq!(fipc_recv(conn, buf.as_mut_ptr().cast(), buf.len(), &mut len, FIPC_FOREVER), FIPC_OK);
//!         fipc_close(conn);
//!     }
//!     println!("{}", String::from_utf8_lossy(&buf[..len])); // PING
//! }
//! ```
//!
//! Run each in a process of its own. The client sends the message `ping`, and the server answers with its uppercase,
//! `PING`: the exchange every example of the repository makes, so a client or a server in any other language works
//! against these.

#![no_std]
#![allow(non_camel_case_types)]

use core::ffi::{c_char, c_int, c_void};
use core::marker::{PhantomData, PhantomPinned};

/// A stream's name, on which its server accepts clients (opaque; `fipc_listener_t*` in C).
#[repr(C)]
pub struct fipc_listener_t {
    _data: [u8; 0],
    _marker: PhantomData<(*mut u8, PhantomPinned)>,
}

/// One connection: a stream's, a publisher, or a member of a broadcast, a work queue or a service (opaque;
/// `fipc_conn_t*` in C).
#[repr(C)]
pub struct fipc_conn_t {
    _data: [u8; 0],
    _marker: PhantomData<(*mut u8, PhantomPinned)>,
}

/// A work queue, held by its owner (opaque; `fipc_queue_t*` in C).
#[repr(C)]
pub struct fipc_queue_t {
    _data: [u8; 0],
    _marker: PhantomData<(*mut u8, PhantomPinned)>,
}

/// A service, held by its owner (opaque; `fipc_service_t*` in C).
#[repr(C)]
pub struct fipc_service_t {
    _data: [u8; 0],
    _marker: PhantomData<(*mut u8, PhantomPinned)>,
}

/// The result of every call that can fail (a C enum: an `int`).
pub type fipc_result_t = c_int;

/// Success.
pub const FIPC_OK: fipc_result_t = 0;
/// The timeout ran out (with timeout 0: nothing to do without waiting).
pub const FIPC_TIMEOUT: fipc_result_t = 1;
/// The peer ended the connection: it closed it, or its process ended. Final.
pub const FIPC_DISCONNECTED: fipc_result_t = 2;
/// `fipc_cancel`, `fipc_listener_cancel`, `fipc_queue_cancel` or `fipc_service_cancel` was called, and the call would
/// have to wait.
pub const FIPC_CANCELLED: fipc_result_t = 3;
/// The message doesn't fit what the call can take: the caller's buffer, or one piece for zero-copy and on a
/// broadcast, a queue or a service. A receive reports the message's length and leaves the message queued.
pub const FIPC_TOO_LARGE: fipc_result_t = 4;
/// A bad argument or handle, a call out of order, a peer with another version or user, corrupt ring content.
pub const FIPC_INVALID: fipc_result_t = 5;
/// Memory or another OS resource ran out (setting a connection up).
pub const FIPC_NO_MEMORY: fipc_result_t = 6;
/// `fipc_stream_create`, `fipc_broadcast_create`, `fipc_queue_create`, `fipc_service_create`: another stream,
/// broadcast, queue or service holds the name.
pub const FIPC_ADDR_IN_USE: fipc_result_t = 7;
/// A lossy subscriber fell a lap behind: messages were lost. The receive returns this instead of a message and
/// reports how many were lost (at least 1) in `*out_len`; the next goes on from a recent message.
pub const FIPC_OVERRUN: fipc_result_t = 8;
/// A gated subscriber held the publisher back for `max_stall_ms` and was dropped from the broadcast: every later
/// receive on its connection returns it (close it).
pub const FIPC_UNSUBSCRIBED: fipc_result_t = 9;
/// A service's client: the server that took the request ended before it replied. The request is answered (the next
/// receive returns the next reply); it may or may not have been carried out.
pub const FIPC_ABANDONED: fipc_result_t = 10;

/// A timeout that doesn't wait.
pub const FIPC_NO_WAIT: c_int = 0;
/// A timeout that waits for ever (any negative value does).
pub const FIPC_FOREVER: c_int = -1;

/// [`fipc_broadcast_join`]'s role: a lossy subscriber, which never holds the publisher back.
pub const FIPC_SUBSCRIBER_LOSSY: c_int = 1;
/// [`fipc_broadcast_join`]'s role: a gated subscriber, which misses nothing (the publisher waits for the room it
/// holds).
pub const FIPC_SUBSCRIBER_GATED: c_int = 2;
/// [`fipc_queue_join`]'s and [`fipc_queue_wait_for_members`]' role: a work queue's producer.
pub const FIPC_PRODUCER: c_int = 3;
/// [`fipc_queue_join`]'s and [`fipc_queue_wait_for_members`]' role: a work queue's consumer.
pub const FIPC_CONSUMER: c_int = 4;
/// [`fipc_service_join`]'s and [`fipc_service_wait_for_members`]' role: a service's client.
pub const FIPC_CLIENT: c_int = 5;
/// [`fipc_service_join`]'s and [`fipc_service_wait_for_members`]' role: a service's server.
pub const FIPC_SERVER: c_int = 6;

// The layout include/fipc.h defines (x86_64 and aarch64, Linux = Windows = macOS)
const _: () = {
    use core::mem::size_of;
    assert!(size_of::<fipc_result_t>() == 4);
    assert!(size_of::<*mut fipc_conn_t>() == size_of::<usize>());
};

#[cfg_attr(windows, link(name = "fastipc", kind = "raw-dylib"))]
#[cfg_attr(not(windows), link(name = "fastipc"))]
unsafe extern "C" {
    /// A static, NUL-terminated string naming `result` (`"FIPC_OK"`, ...); `"FIPC_UNKNOWN"` for any other
    /// value. Safe to call; read it with `CStr::from_ptr`.
    pub safe fn fipc_result_str(result: fipc_result_t) -> *const c_char;

    /// Server: claims `name` (1-245 characters of `[A-Za-z0-9_.-]`, not starting with `.` or `-`) for a stream
    /// and listens on it, with rings of `capacity` bytes (a power of two from 1024 to 2^31) for each connection.
    /// Doesn't wait. [`FIPC_ADDR_IN_USE`] if another stream, broadcast, queue or service holds the name.
    ///
    /// # Safety
    /// `name` is a NUL-terminated string; `out_listener` is valid for a write.
    pub fn fipc_stream_create(
        name: *const c_char,
        capacity: usize,
        out_listener: *mut *mut fipc_listener_t,
    ) -> fipc_result_t;

    /// Server: waits up to `timeout_ms` for a client and returns its connection. One client at a time:
    /// [`FIPC_INVALID`] while the connection this listener returned last is open. [`FIPC_CANCELLED`] after
    /// [`fipc_listener_cancel`]. [`FIPC_NO_MEMORY`]: the listener has failed for good.
    ///
    /// # Safety
    /// `listener` is open; one thread at a time accepts on it; `out_conn` is valid for a write.
    pub fn fipc_stream_accept(
        listener: *mut fipc_listener_t,
        out_conn: *mut *mut fipc_conn_t,
        timeout_ms: c_int,
    ) -> fipc_result_t;

    /// Makes every [`fipc_stream_accept`] of the listener that waits, now or later, return [`FIPC_CANCELLED`]. Any
    /// thread. Final.
    ///
    /// # Safety
    /// `listener` is open (not yet passed to [`fipc_listener_close`]).
    pub fn fipc_listener_cancel(listener: *mut fipc_listener_t);

    /// Stops listening and frees the listener; the connections it accepted stay open. NULL is a no-op.
    ///
    /// # Safety
    /// `listener` is open or NULL, and no other thread is inside a call on it (the cancel included).
    pub fn fipc_listener_close(listener: *mut fipc_listener_t);

    /// Client: connects to the server of the stream on `name`, waiting up to `timeout_ms` for a server to create
    /// the stream and to set the connection up. [`FIPC_INVALID`] if a broadcast, a queue or a service
    /// holds the name.
    ///
    /// # Safety
    /// `name` is a NUL-terminated string; `out_conn` is valid for a write.
    pub fn fipc_stream_connect(
        name: *const c_char,
        out_conn: *mut *mut fipc_conn_t,
        timeout_ms: c_int,
    ) -> fipc_result_t;

    /// The longest message the zero-copy calls take on the connection: its capacity less 64 bytes (on a broadcast or
    /// a queue: a quarter of its capacity; on a service: its `max_message`). 0 for NULL. Any thread.
    ///
    /// # Safety
    /// `conn` is open or NULL.
    pub fn fipc_max_piece(conn: *const fipc_conn_t) -> usize;

    /// Makes every call of the connection that waits, now or later, return [`FIPC_CANCELLED`]; calls that
    /// needn't wait still work. The peer sees nothing until [`fipc_close`]. Any thread. Final.
    ///
    /// # Safety
    /// `conn` is open (not yet passed to [`fipc_close`]).
    pub fn fipc_cancel(conn: *mut fipc_conn_t);

    /// Ends the connection (the peer gets [`FIPC_DISCONNECTED`] once it has received the messages this side
    /// completed), unmaps its rings and frees the handle; every pointer the connection returned becomes
    /// invalid. NULL is a no-op.
    ///
    /// # Safety
    /// `conn` is open or NULL, and no other thread is inside a call on it (the cancel included).
    pub fn fipc_close(conn: *mut fipc_conn_t);

    /// Sends one message of `len` bytes (at least 1, any size): waits up to `timeout_ms` for room for its
    /// first piece, then copies it in, in pieces if it is longer than one. Drops a zero-copy reservation that
    /// was never committed.
    ///
    /// # Safety
    /// `conn` is open and one thread at a time sends on it; `data` is valid for `len` bytes of reads.
    pub fn fipc_send(conn: *mut fipc_conn_t, data: *const c_void, len: usize, timeout_ms: c_int) -> fipc_result_t;

    /// Receives one message of any size into `buf`: waits up to `timeout_ms` for its first piece, sets
    /// `*out_len` to its length and copies it out. [`FIPC_TOO_LARGE`] if `buf_len < *out_len`: nothing is
    /// taken. [`FIPC_OVERRUN`] (a lossy subscriber's): `*out_len` is the number of messages it lost. On a service's
    /// client: the next reply, or [`FIPC_ABANDONED`]; on a server: the next request, held until the reply.
    ///
    /// # Safety
    /// `conn` is open and one thread at a time receives on it; `buf` is valid for `buf_len` bytes of writes
    /// (or NULL with `buf_len` 0); `out_len` is valid for a write.
    pub fn fipc_recv(
        conn: *mut fipc_conn_t,
        buf: *mut c_void,
        buf_len: usize,
        out_len: *mut usize,
        timeout_ms: c_int,
    ) -> fipc_result_t;

    /// Sends `count` messages (at least 1) in order, message i being `lens[i]` bytes (at least 1) at `datas[i]`,
    /// and publishes them together, with at most one wake of the receivers for the batch; `timeout_ms` bounds the
    /// whole call. `*out_sent` is how many went, on every return. Every message is checked before any goes
    /// ([`FIPC_INVALID`], [`FIPC_TOO_LARGE`] over [`fipc_max_piece`] where messages are of one piece). Batch both
    /// ends: a receiver that takes one message per call falls behind a batching sender. `include/fipc.h` has the
    /// whole contract.
    ///
    /// # Safety
    /// `conn` is open and one thread at a time sends on it; `datas` and `lens` are valid for `count` reads, and each
    /// `datas[i]` for `lens[i]` bytes of reads; `out_sent` is valid for a write.
    pub fn fipc_send_many(
        conn: *mut fipc_conn_t,
        datas: *const *const c_void,
        lens: *const usize,
        count: usize,
        out_sent: *mut usize,
        timeout_ms: c_int,
    ) -> fipc_result_t;

    /// Waits up to `timeout_ms` for the next message, as [`fipc_recv`], then takes every further message already
    /// there that fits the rest of `buf`, up to `max_count`, packed back to back (message i is `lens[i]` bytes, at
    /// the sum of the lengths before it). `*out_count` is how many, on every return: 0 with any result but
    /// [`FIPC_OK`], which is the first message's, as [`fipc_recv`] returns it (`lens[0]` holds the length of
    /// [`FIPC_TOO_LARGE`]'s message and [`FIPC_OVERRUN`]'s count). `include/fipc.h` has the whole contract.
    ///
    /// # Safety
    /// `conn` is open and one thread at a time receives on it; `buf` is valid for `buf_len` bytes of writes (or NULL
    /// with `buf_len` 0); `lens` is valid for `max_count` writes; `out_count` is valid for a write.
    pub fn fipc_recv_many(
        conn: *mut fipc_conn_t,
        buf: *mut c_void,
        buf_len: usize,
        lens: *mut usize,
        max_count: usize,
        out_count: *mut usize,
        timeout_ms: c_int,
    ) -> fipc_result_t;

    /// Zero-copy send, step 1: waits up to `timeout_ms` for `len` (at least 1) contiguous bytes in the ring
    /// and sets `*out_buf` to them (16-byte aligned). [`FIPC_TOO_LARGE`] over [`fipc_max_piece`]. The room is
    /// contiguous: a reservation that doesn't fit before the ring's end waits until the receiver has read past
    /// the end, even in an empty ring, so a long one (near [`fipc_max_piece`]) can wait for the peer's next
    /// receive call.
    ///
    /// # Safety
    /// `conn` is open and one thread at a time sends on it; `out_buf` is valid for a write. The bytes are
    /// valid until the commit, the next send or acquire (which drops the reservation) or [`fipc_close`].
    pub fn fipc_send_acquire(
        conn: *mut fipc_conn_t,
        len: usize,
        out_buf: *mut *mut c_void,
        timeout_ms: c_int,
    ) -> fipc_result_t;

    /// Zero-copy send, step 2: publishes the first `len` bytes (1 to the acquired length) as one message.
    /// Doesn't wait. [`FIPC_INVALID`] without a reservation, or for a length out of range (the reservation
    /// stays for a valid one).
    ///
    /// # Safety
    /// `conn` is open and one thread at a time sends on it.
    pub fn fipc_send_commit(conn: *mut fipc_conn_t, len: usize) -> fipc_result_t;

    /// Zero-copy receive, step 1: waits up to `timeout_ms` for a message and sets `*out_data` (16-byte
    /// aligned; in the ring, read-only) and `*out_len`. [`FIPC_TOO_LARGE`] for a message in several pieces:
    /// `*out_len` is its length, and it stays for [`fipc_recv`]. [`FIPC_INVALID`] on a lossy subscriber. On a
    /// service's client: the next reply, or [`FIPC_ABANDONED`]; on a server: the next request, held until the reply.
    ///
    /// # Safety
    /// `conn` is open and one thread at a time receives on it; the out-pointers are valid for writes. The
    /// bytes are valid until [`fipc_recv_release`], the next receive call of any kind or [`fipc_close`]; a gated
    /// subscriber's, also until it is unsubscribed (the broadcast's `max_stall_ms`), after which reading them
    /// faults; a service's server's request, until the reply's [`fipc_send`] or [`fipc_send_commit`].
    pub fn fipc_recv_acquire(
        conn: *mut fipc_conn_t,
        out_data: *mut *const c_void,
        out_len: *mut usize,
        timeout_ms: c_int,
    ) -> fipc_result_t;

    /// Zero-copy receive, step 2: frees the acquired message's room. Without one, a no-op. Doesn't wait.
    ///
    /// # Safety
    /// `conn` is open and one thread at a time receives on it.
    pub fn fipc_recv_release(conn: *mut fipc_conn_t);

    /// Publisher: claims `name` for a broadcast of `capacity` bytes of messages (a power of two, 4096 to 2^31) and
    /// room for `max_subscribers` subscribers (1 to 1024), and returns the publisher's connection, the broadcast's
    /// one handle: its [`fipc_close`] ends the broadcast. Doesn't wait: the publisher's process admits joiners on a
    /// thread of the library's. A gated subscriber whose position stays still for `max_stall_ms` from the first send
    /// it held back is unsubscribed; [`FIPC_FOREVER`] never unsubscribes, [`FIPC_NO_WAIT`] refuses gated
    /// subscribers. [`FIPC_ADDR_IN_USE`] if another stream, broadcast, queue or service holds the name.
    ///
    /// # Safety
    /// `name` is a NUL-terminated string; `out_publisher` is valid for a write.
    pub fn fipc_broadcast_create(
        name: *const c_char,
        capacity: usize,
        max_subscribers: u32,
        max_stall_ms: c_int,
        out_publisher: *mut *mut fipc_conn_t,
    ) -> fipc_result_t;

    /// Subscriber: joins the broadcast on `name` as `role` ([`FIPC_SUBSCRIBER_LOSSY`] or [`FIPC_SUBSCRIBER_GATED`])
    /// and returns its connection, waiting up to `timeout_ms` for the publisher to create the broadcast, to have a
    /// free slot and to admit it. [`FIPC_INVALID`] for another role, a name a queue or a service holds, or a gated
    /// subscriber on a broadcast that refuses them.
    ///
    /// # Safety
    /// `name` is a NUL-terminated string; `out_conn` is valid for a write.
    pub fn fipc_broadcast_join(
        name: *const c_char,
        role: c_int,
        out_conn: *mut *mut fipc_conn_t,
        timeout_ms: c_int,
    ) -> fipc_result_t;

    /// Publisher: waits up to `timeout_ms` until at least `at_least` subscribers are joined, and stores in
    /// `*out_count` how many are, at every return but [`FIPC_INVALID`]. [`FIPC_CANCELLED`] after [`fipc_cancel`]
    /// of the publisher; [`FIPC_INVALID`] on a connection other than a publisher, or for `at_least` over the
    /// broadcast's `max_subscribers`. Any thread.
    ///
    /// # Safety
    /// `publisher` is open (not yet passed to [`fipc_close`]); `out_count` is valid for a write.
    pub fn fipc_broadcast_wait_for_members(
        publisher: *mut fipc_conn_t,
        at_least: u32,
        out_count: *mut u32,
        timeout_ms: c_int,
    ) -> fipc_result_t;

    /// Owner: claims `name` for a work queue of up to `max_producers` producers (1 to 63) and `max_consumers`
    /// consumers (1 to 64), each producer with a ring of `capacity` bytes (a power of two, 4096 to 2^31), and returns
    /// the queue's handle: its [`fipc_queue_close`] ends the queue. Doesn't wait: the owner's process admits joiners
    /// on a thread of the library's, and the owner joins with [`fipc_queue_join`] like any member.
    /// [`FIPC_ADDR_IN_USE`] if another stream, broadcast, queue or service holds the name.
    ///
    /// # Safety
    /// `name` is a NUL-terminated string; `out_queue` is valid for a write.
    pub fn fipc_queue_create(
        name: *const c_char,
        capacity: usize,
        max_producers: u32,
        max_consumers: u32,
        out_queue: *mut *mut fipc_queue_t,
    ) -> fipc_result_t;

    /// Producer or consumer: joins the queue on `name` as `role` ([`FIPC_PRODUCER`] or [`FIPC_CONSUMER`]) and
    /// returns its connection, waiting up to `timeout_ms` for the owner to create the queue, for a free place of
    /// the role and for the owner's process to admit it. [`FIPC_INVALID`] for another role or a name a broadcast or
    /// a service holds.
    ///
    /// # Safety
    /// `name` is a NUL-terminated string; `out_conn` is valid for a write.
    pub fn fipc_queue_join(
        name: *const c_char,
        role: c_int,
        out_conn: *mut *mut fipc_conn_t,
        timeout_ms: c_int,
    ) -> fipc_result_t;

    /// Owner: waits up to `timeout_ms` until at least `at_least` members of `role` ([`FIPC_PRODUCER`] or
    /// [`FIPC_CONSUMER`]; 0 counts both) are joined, and stores in `*out_count` how many are, at every return but
    /// [`FIPC_INVALID`]. [`FIPC_CANCELLED`] after [`fipc_queue_cancel`]; [`FIPC_DISCONNECTED`] once the queue has
    /// ended; [`FIPC_INVALID`] for another role, or for `at_least` over the role's maximum. Any thread.
    ///
    /// # Safety
    /// `queue` is open (not yet passed to [`fipc_queue_close`]); `out_count` is valid for a write.
    pub fn fipc_queue_wait_for_members(
        queue: *mut fipc_queue_t,
        role: c_int,
        at_least: u32,
        out_count: *mut u32,
        timeout_ms: c_int,
    ) -> fipc_result_t;

    /// Makes every [`fipc_queue_wait_for_members`] of the queue that waits, now or later, return
    /// [`FIPC_CANCELLED`]. The queue goes on. Any thread. Final.
    ///
    /// # Safety
    /// `queue` is open (not yet passed to [`fipc_queue_close`]).
    pub fn fipc_queue_cancel(queue: *mut fipc_queue_t);

    /// Ends the queue (producers' sends return [`FIPC_DISCONNECTED`]; consumers receive what is left, then
    /// [`FIPC_DISCONNECTED`]) and frees the handle; the name is free again at once. The members' connections stay
    /// open. NULL is a no-op.
    ///
    /// # Safety
    /// `queue` is open or NULL, and no other thread is inside a call on it (the cancel included).
    pub fn fipc_queue_close(queue: *mut fipc_queue_t);

    /// Owner: claims `name` for a service of up to `max_clients` clients and `max_servers` servers (1 to 64 each),
    /// each client with a ring of `capacity` bytes (a power of two, 4096 to 2^31) for its requests and their replies,
    /// each of 1 to `max_message` bytes (1 to `capacity / 4`), and returns the service's handle: its
    /// [`fipc_service_close`] ends the service. Doesn't wait: the owner's process admits joiners on a thread of the
    /// library's, and the owner joins with [`fipc_service_join`] like any member. [`FIPC_ADDR_IN_USE`] if another
    /// stream, broadcast, queue or service holds the name.
    ///
    /// # Safety
    /// `name` is a NUL-terminated string; `out_service` is valid for a write.
    pub fn fipc_service_create(
        name: *const c_char,
        capacity: usize,
        max_message: usize,
        max_clients: u32,
        max_servers: u32,
        out_service: *mut *mut fipc_service_t,
    ) -> fipc_result_t;

    /// Client or server: joins the service on `name` as `role` ([`FIPC_CLIENT`] or [`FIPC_SERVER`]) and returns its
    /// connection, waiting up to `timeout_ms` for the owner to create the service, for a free place of the role and
    /// for the owner's process to admit it. A client's sends are requests and its receives their replies, in the
    /// order of the requests ([`FIPC_ABANDONED`] for a request whose server ended before it replied); a server's
    /// receive takes the next request of any client and holds it until its send, the reply, which never waits.
    /// [`FIPC_INVALID`] for another role or a name a broadcast or a queue holds.
    ///
    /// # Safety
    /// `name` is a NUL-terminated string; `out_conn` is valid for a write.
    pub fn fipc_service_join(
        name: *const c_char,
        role: c_int,
        out_conn: *mut *mut fipc_conn_t,
        timeout_ms: c_int,
    ) -> fipc_result_t;

    /// Owner: waits up to `timeout_ms` until at least `at_least` members of `role` ([`FIPC_CLIENT`] or
    /// [`FIPC_SERVER`]; 0 counts both) are joined, and stores in `*out_count` how many are, at every return but
    /// [`FIPC_INVALID`]. [`FIPC_CANCELLED`] after [`fipc_service_cancel`]; [`FIPC_DISCONNECTED`] once the service
    /// has ended; [`FIPC_INVALID`] for another role, or for `at_least` over the role's maximum. Any thread.
    ///
    /// # Safety
    /// `service` is open (not yet passed to [`fipc_service_close`]); `out_count` is valid for a write.
    pub fn fipc_service_wait_for_members(
        service: *mut fipc_service_t,
        role: c_int,
        at_least: u32,
        out_count: *mut u32,
        timeout_ms: c_int,
    ) -> fipc_result_t;

    /// Makes every [`fipc_service_wait_for_members`] of the service that waits, now or later, return
    /// [`FIPC_CANCELLED`]. The service goes on. Any thread. Final.
    ///
    /// # Safety
    /// `service` is open (not yet passed to [`fipc_service_close`]).
    pub fn fipc_service_cancel(service: *mut fipc_service_t);

    /// Ends the service (clients' sends and servers' receives return [`FIPC_DISCONNECTED`]; clients receive the
    /// replies that came before the end, then [`FIPC_DISCONNECTED`]) and frees the handle; the name is free again at
    /// once. The members' connections stay open. NULL is a no-op.
    ///
    /// # Safety
    /// `service` is open or NULL, and no other thread is inside a call on it (the cancel included).
    pub fn fipc_service_close(service: *mut fipc_service_t);
}

#[cfg(test)]
mod tests {
    extern crate std;
    use super::*;
    use std::prelude::rust_2024::*;

    fn name_of(result: fipc_result_t) -> &'static str {
        // SAFETY: fipc_result_str returns a static, NUL-terminated string.
        unsafe { core::ffi::CStr::from_ptr(fipc_result_str(result)) }.to_str().unwrap()
    }

    #[test]
    fn result_names() {
        let names = [
            "FIPC_OK",
            "FIPC_TIMEOUT",
            "FIPC_DISCONNECTED",
            "FIPC_CANCELLED",
            "FIPC_TOO_LARGE",
            "FIPC_INVALID",
            "FIPC_NO_MEMORY",
            "FIPC_ADDR_IN_USE",
            "FIPC_OVERRUN",
            "FIPC_UNSUBSCRIBED",
            "FIPC_ABANDONED",
        ];
        for (code, name) in names.iter().enumerate() {
            assert_eq!(name_of(code as fipc_result_t), *name);
        }
        assert_eq!(name_of(99), "FIPC_UNKNOWN");
    }

    #[test]
    fn null_handles() {
        let mut out = core::ptr::null_mut();
        // SAFETY: the header defines NULL handles: FIPC_INVALID, 0, or a no-op.
        unsafe {
            assert_eq!(fipc_max_piece(core::ptr::null()), 0);
            assert_eq!(fipc_stream_accept(core::ptr::null_mut(), &mut out, 0), FIPC_INVALID);
            assert_eq!(fipc_send(core::ptr::null_mut(), b"x".as_ptr().cast(), 1, 0), FIPC_INVALID);
            let mut count = 0;
            assert_eq!(fipc_broadcast_wait_for_members(core::ptr::null_mut(), 0, &mut count, 0), FIPC_INVALID);
            assert_eq!(fipc_queue_wait_for_members(core::ptr::null_mut(), 0, 0, &mut count, 0), FIPC_INVALID);
            assert_eq!(fipc_service_wait_for_members(core::ptr::null_mut(), 0, 0, &mut count, 0), FIPC_INVALID);
            fipc_close(core::ptr::null_mut());
            fipc_listener_close(core::ptr::null_mut());
            fipc_queue_close(core::ptr::null_mut());
            fipc_service_close(core::ptr::null_mut());
        }
    }

    /// In a checkout of the repository: this crate declares exactly the header's functions, and its constants and
    /// values.
    #[test]
    fn matches_the_header() {
        let header = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../include/fipc.h");
        let Ok(header) = std::fs::read_to_string(header) else {
            return; // a published crate, outside the repository
        };
        let source = include_str!("lib.rs");
        let declared: Vec<String> = source
            .lines()
            .filter_map(|l| l.trim().strip_prefix("pub fn ").or_else(|| l.trim().strip_prefix("pub safe fn ")))
            .map(|l| l.split('(').next().unwrap().to_owned())
            .collect();
        // "FIPC_API <type> <name>(", the declarations' form (the #define has no space after FIPC_API)
        let exported: Vec<String> = header
            .split("FIPC_API ")
            .skip(1)
            .filter_map(|d| d.split('(').next()?.split_whitespace().last())
            .map(|f| f.trim_start_matches('*').to_owned())
            .collect();
        let functions: Vec<String> = exported.into_iter().filter(|f| f.starts_with("fipc_")).collect();
        assert_eq!(functions.len(), 30, "{functions:?}");
        for function in &functions {
            assert!(declared.contains(function), "{function} is not declared");
        }
        assert_eq!(declared.len(), 30, "{declared:?}");
        for (name, value) in [
            ("FIPC_OK", FIPC_OK),
            ("FIPC_TIMEOUT", FIPC_TIMEOUT),
            ("FIPC_DISCONNECTED", FIPC_DISCONNECTED),
            ("FIPC_CANCELLED", FIPC_CANCELLED),
            ("FIPC_TOO_LARGE", FIPC_TOO_LARGE),
            ("FIPC_INVALID", FIPC_INVALID),
            ("FIPC_NO_MEMORY", FIPC_NO_MEMORY),
            ("FIPC_ADDR_IN_USE", FIPC_ADDR_IN_USE),
            ("FIPC_OVERRUN", FIPC_OVERRUN),
            ("FIPC_UNSUBSCRIBED", FIPC_UNSUBSCRIBED),
            ("FIPC_ABANDONED", FIPC_ABANDONED),
        ] {
            assert!(header.contains(&std::format!("{name} = {value},")), "{name} = {value}");
        }
        for line in [
            "#define FIPC_NO_WAIT 0",
            "#define FIPC_FOREVER (-1)",
            "#define FIPC_SUBSCRIBER_LOSSY 1 ",
            "#define FIPC_SUBSCRIBER_GATED 2 ",
            "#define FIPC_PRODUCER 3 ",
            "#define FIPC_CONSUMER 4 ",
            "#define FIPC_CLIENT 5 ",
            "#define FIPC_SERVER 6 ",
        ] {
            assert!(header.contains(line), "{line}");
        }
        assert_eq!((FIPC_NO_WAIT, FIPC_FOREVER), (0, -1));
        assert_eq!((FIPC_SUBSCRIBER_LOSSY, FIPC_SUBSCRIBER_GATED, FIPC_PRODUCER, FIPC_CONSUMER), (1, 2, 3, 4));
        assert_eq!((FIPC_CLIENT, FIPC_SERVER), (5, 6));
    }
}
