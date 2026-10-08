//! Raw FFI bindings to FastIPC: the 18 functions, the result codes and the RPC header of
//! [`include/fipc.h`](https://github.com/fastipc/fastipc/blob/main/include/fipc.h), declared by hand.
//!
//! FastIPC connects two processes of the same user on one machine through shared memory: a server listens on a name
//! and accepts one client at a time, a client connects to the name, and each connection has one ring per direction.
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
//! library next to it (Windows) or on its library path (Linux; for example an rpath of `$ORIGIN`): see the
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
//!     let mut request = fipc_rpc_msg_t::default();
//!     // SAFETY: the pointers are valid, and each handle is closed once, by the thread that uses it.
//!     unsafe {
//!         assert_eq!(fipc_listen(c"my_channel".as_ptr(), 1 << 20, &mut listener), FIPC_OK); // rings of 1 MiB each way
//!         assert_eq!(fipc_accept(listener, &mut conn, FIPC_FOREVER), FIPC_OK); // waits for a client
//!         if fipc_rpc_recv(conn, buf.as_mut_ptr().cast(), buf.len(), &mut request, FIPC_FOREVER) == FIPC_OK {
//!             let payload = &mut buf[..request.len as usize];
//!             payload.make_ascii_uppercase();
//!             fipc_rpc_respond(conn, request.id, request.opcode, 0, payload.as_ptr().cast(), payload.len(), FIPC_FOREVER);
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
//!     let mut id = 0;
//!     let mut buf = [0u8; 256];
//!     let mut response = fipc_rpc_msg_t::default();
//!     // SAFETY: the pointers are valid, and the handle is closed once, by the thread that uses it.
//!     unsafe {
//!         assert_eq!(fipc_connect(c"my_channel".as_ptr(), &mut conn, 5000), FIPC_OK);
//!         assert_eq!(fipc_rpc_submit(conn, 1, b"ping".as_ptr().cast(), 4, &mut id, FIPC_FOREVER), FIPC_OK); // opcode 1
//!         assert_eq!(fipc_rpc_recv(conn, buf.as_mut_ptr().cast(), buf.len(), &mut response, FIPC_FOREVER), FIPC_OK);
//!         fipc_close(conn);
//!     }
//!     println!("{}", String::from_utf8_lossy(&buf[..response.len as usize])); // PING
//! }
//! ```
//!
//! Run each in a process of its own. The client sends an RPC request with opcode 1 and the payload `ping`, and the
//! server answers with the payload in upper case: the exchange every example of the repository makes, so a client or
//! a server in any other language works against these.

#![no_std]
#![allow(non_camel_case_types)]

use core::ffi::{c_char, c_int, c_void};
use core::marker::{PhantomData, PhantomPinned};

/// A name a server listens on (opaque; `fipc_listener_t*` in C).
#[repr(C)]
pub struct fipc_listener_t {
    _data: [u8; 0],
    _marker: PhantomData<(*mut u8, PhantomPinned)>,
}

/// One connection: two rings (opaque; `fipc_conn_t*` in C).
#[repr(C)]
pub struct fipc_conn_t {
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
/// `fipc_cancel` / `fipc_listener_cancel` was called, and the call would have to wait.
pub const FIPC_CANCELLED: fipc_result_t = 3;
/// The message doesn't fit what the call can take: the caller's buffer, or one piece for zero-copy. A receive
/// reports the message's length and leaves the message queued.
pub const FIPC_TOO_LARGE: fipc_result_t = 4;
/// A bad argument or handle, a call out of order, a peer with another version or user, corrupt ring content.
pub const FIPC_INVALID: fipc_result_t = 5;
/// Memory or another OS resource ran out (setting a connection up).
pub const FIPC_NO_MEMORY: fipc_result_t = 6;
/// `fipc_listen`: another listener holds the name.
pub const FIPC_ADDR_IN_USE: fipc_result_t = 7;

/// A timeout that doesn't wait.
pub const FIPC_NO_WAIT: c_int = 0;
/// A timeout that waits for ever (any negative value does).
pub const FIPC_FOREVER: c_int = -1;

/// [`fipc_rpc_msg_t::kind`] of a request.
pub const FIPC_RPC_REQUEST: u32 = 1;
/// [`fipc_rpc_msg_t::kind`] of a response.
pub const FIPC_RPC_RESPONSE: u32 = 2;

/// The header of an RPC message (32 bytes), which [`fipc_rpc_recv`] fills.
#[repr(C)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct fipc_rpc_msg_t {
    /// The request's id, echoed in its response.
    pub id: u64,
    /// [`FIPC_RPC_REQUEST`] or [`FIPC_RPC_RESPONSE`].
    pub kind: u32,
    /// The application's.
    pub opcode: u32,
    /// The application's; 0 in requests.
    pub status: i32,
    /// 0.
    pub reserved: u32,
    /// The payload's length.
    pub len: u64,
}

// The layout include/fipc.h defines (x86_64, Linux = Windows)
const _: () = {
    use core::mem::{align_of, offset_of, size_of};
    assert!(size_of::<fipc_rpc_msg_t>() == 32);
    assert!(align_of::<fipc_rpc_msg_t>() == 8);
    assert!(offset_of!(fipc_rpc_msg_t, id) == 0);
    assert!(offset_of!(fipc_rpc_msg_t, kind) == 8);
    assert!(offset_of!(fipc_rpc_msg_t, opcode) == 12);
    assert!(offset_of!(fipc_rpc_msg_t, status) == 16);
    assert!(offset_of!(fipc_rpc_msg_t, reserved) == 20);
    assert!(offset_of!(fipc_rpc_msg_t, len) == 24);
    assert!(size_of::<fipc_result_t>() == 4);
    assert!(size_of::<*mut fipc_conn_t>() == size_of::<usize>());
};

#[cfg_attr(windows, link(name = "fastipc", kind = "raw-dylib"))]
#[cfg_attr(not(windows), link(name = "fastipc"))]
unsafe extern "C" {
    /// A static, NUL-terminated string naming `result` (`"FIPC_OK"`, ...); `"FIPC_UNKNOWN"` for any other
    /// value. Safe to call; read it with `CStr::from_ptr`.
    pub safe fn fipc_result_str(result: fipc_result_t) -> *const c_char;

    /// Server: claims `name` (1-245 characters of `[A-Za-z0-9_.-]`, not starting with `.` or `-`) and listens
    /// on it, with rings of `capacity` bytes (a power of two from 1024 to 2^31) for each connection. Doesn't
    /// wait. [`FIPC_ADDR_IN_USE`] if another listener holds the name.
    ///
    /// # Safety
    /// `name` is a NUL-terminated string; `out_listener` is valid for a write.
    pub fn fipc_listen(name: *const c_char, capacity: usize, out_listener: *mut *mut fipc_listener_t) -> fipc_result_t;

    /// Server: waits up to `timeout_ms` for a client and returns its connection. One client at a time:
    /// [`FIPC_INVALID`] while the connection this listener returned last is open. [`FIPC_CANCELLED`] after
    /// [`fipc_listener_cancel`]. [`FIPC_NO_MEMORY`]: the listener has failed for good.
    ///
    /// # Safety
    /// `listener` is open; one thread at a time accepts on it; `out_conn` is valid for a write.
    pub fn fipc_accept(
        listener: *mut fipc_listener_t,
        out_conn: *mut *mut fipc_conn_t,
        timeout_ms: c_int,
    ) -> fipc_result_t;

    /// Makes every [`fipc_accept`] of the listener that waits, now or later, return [`FIPC_CANCELLED`]. Any
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

    /// Client: connects to the server listening on `name`, waiting up to `timeout_ms` for a server to listen
    /// and to set the connection up.
    ///
    /// # Safety
    /// `name` is a NUL-terminated string; `out_conn` is valid for a write.
    pub fn fipc_connect(name: *const c_char, out_conn: *mut *mut fipc_conn_t, timeout_ms: c_int) -> fipc_result_t;

    /// The longest message the zero-copy calls take on the connection: its capacity less 64 bytes. 0 for NULL.
    /// Any thread.
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
    /// taken.
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
    /// `*out_len` is its length, and it stays for [`fipc_recv`].
    ///
    /// # Safety
    /// `conn` is open and one thread at a time receives on it; the out-pointers are valid for writes. The
    /// bytes are valid until [`fipc_recv_release`], the next receive call of any kind or [`fipc_close`].
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

    /// Sends a request with a payload of any size, possibly empty (as [`fipc_send`]), and sets `*out_id` to
    /// its id: a connection numbers its requests from 1.
    ///
    /// # Safety
    /// As [`fipc_send`] (`data` may be NULL with `len` 0); `out_id` is valid for a write.
    pub fn fipc_rpc_submit(
        conn: *mut fipc_conn_t,
        opcode: u32,
        data: *const c_void,
        len: usize,
        out_id: *mut u64,
        timeout_ms: c_int,
    ) -> fipc_result_t;

    /// Sends the response to request `id` (as [`fipc_send`]; the payload may be empty).
    ///
    /// # Safety
    /// As [`fipc_send`] (`data` may be NULL with `len` 0).
    pub fn fipc_rpc_respond(
        conn: *mut fipc_conn_t,
        id: u64,
        opcode: u32,
        status: i32,
        data: *const c_void,
        len: usize,
        timeout_ms: c_int,
    ) -> fipc_result_t;

    /// Receives one request or response (as [`fipc_recv`]): fills `*msg` and copies the payload into `buf`.
    /// [`FIPC_TOO_LARGE`] if `buf_len < msg->len`: `*msg` is filled, nothing is taken. A message that isn't a
    /// well-formed RPC message is dropped, and the call returns [`FIPC_INVALID`].
    ///
    /// # Safety
    /// As [`fipc_recv`]; `msg` is valid for a write.
    pub fn fipc_rpc_recv(
        conn: *mut fipc_conn_t,
        buf: *mut c_void,
        buf_len: usize,
        msg: *mut fipc_rpc_msg_t,
        timeout_ms: c_int,
    ) -> fipc_result_t;
}

#[cfg(test)]
mod tests {
    extern crate std;
    use super::*;
    use std::prelude::rust_2024::*;

    #[test]
    fn rpc_header_layout() {
        let msg = fipc_rpc_msg_t { id: 1, kind: FIPC_RPC_REQUEST, opcode: 7, status: -1, reserved: 0, len: 9 };
        // SAFETY: fipc_rpc_msg_t is 32 bytes of plain integers without padding.
        let bytes: [u8; 32] = unsafe { core::mem::transmute(msg) };
        assert_eq!(&bytes[0..8], &1u64.to_le_bytes());
        assert_eq!(&bytes[8..12], &1u32.to_le_bytes());
        assert_eq!(&bytes[12..16], &7u32.to_le_bytes());
        assert_eq!(&bytes[16..20], &(-1i32).to_le_bytes());
        assert_eq!(&bytes[24..32], &9u64.to_le_bytes());
    }

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
            assert_eq!(fipc_accept(core::ptr::null_mut(), &mut out, 0), FIPC_INVALID);
            assert_eq!(fipc_send(core::ptr::null_mut(), b"x".as_ptr().cast(), 1, 0), FIPC_INVALID);
            fipc_close(core::ptr::null_mut());
            fipc_listener_close(core::ptr::null_mut());
        }
    }

    /// In a checkout of the repository: this crate declares exactly the header's functions, constants and values.
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
        assert_eq!(functions.len(), 18, "{functions:?}");
        for function in &functions {
            assert!(declared.contains(function), "{function} is not declared");
        }
        assert_eq!(declared.len(), 18, "{declared:?}");
        for (name, value) in [
            ("FIPC_OK", FIPC_OK),
            ("FIPC_TIMEOUT", FIPC_TIMEOUT),
            ("FIPC_DISCONNECTED", FIPC_DISCONNECTED),
            ("FIPC_CANCELLED", FIPC_CANCELLED),
            ("FIPC_TOO_LARGE", FIPC_TOO_LARGE),
            ("FIPC_INVALID", FIPC_INVALID),
            ("FIPC_NO_MEMORY", FIPC_NO_MEMORY),
            ("FIPC_ADDR_IN_USE", FIPC_ADDR_IN_USE),
        ] {
            assert!(header.contains(&std::format!("{name} = {value},")), "{name} = {value}");
        }
        for line in [
            "#define FIPC_NO_WAIT 0",
            "#define FIPC_FOREVER (-1)",
            "#define FIPC_RPC_REQUEST 1",
            "#define FIPC_RPC_RESPONSE 2",
        ] {
            assert!(header.contains(line), "{line}");
        }
        assert_eq!((FIPC_NO_WAIT, FIPC_FOREVER, FIPC_RPC_REQUEST, FIPC_RPC_RESPONSE), (0, -1, 1, 2));
    }
}
