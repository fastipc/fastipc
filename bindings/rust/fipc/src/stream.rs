//! Streams: a server and one client at a time, connected by two rings, one per direction.
//!
//! The server [`create`]s the stream's name and gets its [`Listener`], whose [`accept`](Listener::accept) sets up one
//! client's [`Connection`] at a time; a client [`connect`]s to the name.
//!
//! ```
//! use std::time::Duration;
//!
//! # fn main() -> fipc::Result<()> {
//! let mut listener = fipc::stream::create("fipc_doc_stream", 1 << 16)?; // rings of 64 KiB each way
//! // The client, on another thread (or in another process): connect waits for the listener's accept
//! let client = std::thread::spawn(|| fipc::stream::connect("fipc_doc_stream", Duration::from_secs(5)));
//! let mut server = listener.accept(Duration::from_secs(5))?;
//! let mut client = client.join().unwrap()?;
//! client.send(b"hello", None)?;
//! assert_eq!(server.receive(None)?, b"hello");
//! # Ok(())
//! # }
//! ```

use std::ptr::{self, NonNull};
use std::time::Duration;

use fipc_sys as sys;

use crate::connection::Kind;
use crate::error::check;
use crate::{Connection, Listener, Result, c_name, millis};

/// Server: claims `name` for a stream and listens on it, with rings of `capacity` bytes each way for every
/// connection. Doesn't wait: clients may [`connect`] from now on, and [`accept`](Listener::accept) sets each one up.
///
/// A name is 1-245 characters of `[A-Za-z0-9_.-]`, not starting with `.` or `-`, local to the user (Linux, macOS) or
/// to the desktop session (Windows). The capacity is a power of two from 1024 to 2^31; a client's connection has the
/// server's.
///
/// # Errors
///
/// [`Error::AddrInUse`](crate::Error::AddrInUse) if another stream, broadcast, queue or service holds the name (it is
/// freed when that one's handle is dropped or its process ends); [`Error::Invalid`](crate::Error::Invalid) for a bad
/// name or capacity.
pub fn create(name: &str, capacity: usize) -> Result<Listener> {
    let c_name = c_name(name)?;
    let mut out = ptr::null_mut();
    // SAFETY: c_name is NUL-terminated; out is valid for a write.
    check(unsafe { sys::fipc_stream_create(c_name.as_ptr(), capacity, &mut out) }, 0)?;
    let ptr = NonNull::new(out).expect("fipc_stream_create returned FIPC_OK without a listener");
    Ok(Listener::from_raw(ptr, name))
}

/// Client: connects to the server of the stream on `name` and sets the connection up on the calling thread, waiting
/// up to `timeout` for a server to create the stream (it may start later, or be serving another client) and to call
/// [`accept`](Listener::accept). It returns once the server's `accept` has offered the rings, which may be before that
/// `accept` returns: messages sent meanwhile wait in the ring. The client and the server run on different threads (of
/// one process or two). The connection's rings have the capacity the server chose.
///
/// There is no handle to cancel until it returns: use `None` ([`FOREVER`](crate::FOREVER)) to wait for a server
/// however late it starts, or a finite timeout to stay responsive.
///
/// # Errors
///
/// [`Error::Timeout`](crate::Error::Timeout) if no server's `accept` set a connection up in time;
/// [`Error::Invalid`](crate::Error::Invalid) for a bad name, a name a broadcast, a queue or a service holds, or a
/// server with another version of the protocol or running as another user;
/// [`Error::NoMemory`](crate::Error::NoMemory) if the rings can't be mapped.
pub fn connect(name: &str, timeout: impl Into<Option<Duration>>) -> Result<Connection> {
    let c_name = c_name(name)?;
    let mut out = ptr::null_mut();
    // SAFETY: c_name is NUL-terminated; out is valid for a write.
    check(unsafe { sys::fipc_stream_connect(c_name.as_ptr(), &mut out, millis(timeout)) }, 0)?;
    let ptr = NonNull::new(out).expect("fipc_stream_connect returned FIPC_OK without a connection");
    // SAFETY: a connection fipc_stream_connect just returned, owned by nothing else.
    Ok(unsafe { Connection::from_raw(ptr, name, Kind::Plain) })
}
