use std::ffi::CStr;
use std::fmt;
use std::io;

use fipc_sys as sys;

/// `Result<T, Error>`: what every call that can fail returns.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Why a call didn't succeed: the C API's `fipc_result_t`, without `FIPC_OK`.
///
/// [`Display`](fmt::Display) gives the C name (from `fipc_result_str`) and what it means. A timeout or the peer's end
/// is an ordinary result, not a failure of the library: match on it.
///
/// ```
/// # use fipc::{Error, NO_WAIT};
/// assert_eq!(fipc::stream::connect("fipc_doc_nobody_listens", NO_WAIT).unwrap_err(), Error::Timeout);
/// assert_eq!(Error::Timeout.to_string(), "FIPC_TIMEOUT: the timeout ran out");
/// ```
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Error {
    /// The timeout ran out (with [`NO_WAIT`](crate::NO_WAIT): there was nothing to do without waiting). The call
    /// changed nothing.
    Timeout,
    /// The peer ended the connection: it closed it, or its process ended, for any reason. Sends report it at once,
    /// receives only after they have delivered every message the peer completed. It is final: drop the connection;
    /// to talk again, accept or connect a new one. On a broadcast: the publisher was dropped, or its process ended.
    /// On a work queue: the queue ended (its [`Queue`](crate::Queue) was closed or dropped, or its owner's process
    /// ended); a consumer gets it once it has received every message left in the queue. On a service: the service
    /// ended (its [`Service`](crate::Service) was closed or dropped, or its owner's process ended); a client gets it
    /// once it has received the replies that came before the end.
    Disconnected,
    /// The listener, connection, queue or service was cancelled ([`Canceler::cancel`](crate::Canceler::cancel)), and
    /// the call would have to wait.
    Cancelled,
    /// The message doesn't fit what the call can take: the caller's buffer, or one piece for zero-copy and on a
    /// broadcast, a work queue or a service. `len` is the message's length. A receive leaves the message queued, so
    /// receive it again into `len` bytes (or with the copying calls, for a zero-copy receive); on a work queue another
    /// consumer may take it first.
    TooLarge {
        /// The message's length in bytes: the room a receive needs, or what a zero-copy send asked for.
        len: usize,
    },
    /// A bad argument (a name with a character the C API doesn't take, a capacity that isn't a power of two from
    /// 1024 to 2^31, an empty message), a call out of order (an accept while the listener's last connection is
    /// open), a call a broadcast's, a work queue's or a service's connection doesn't take (a receive on its publisher
    /// or a producer, a zero-copy receive on a lossy subscriber, a send on a consumer, a server's receive while it
    /// holds a request or its send with none held), a join on a name of another kind, a peer with another version or
    /// user, or corrupt ring content.
    Invalid,
    /// Memory or another OS resource ran out while setting a connection up.
    NoMemory,
    /// Another stream, broadcast, work queue or service holds the name.
    AddrInUse,
    /// A lossy subscriber of a broadcast fell a lap behind, and `lost` messages (at least 1) were lost: the
    /// receive returns this instead of a message, and the next one goes on from a recent message.
    Overrun {
        /// How many messages the subscriber lost.
        lost: u64,
    },
    /// A gated subscriber of a broadcast held the publisher back for the broadcast's `max_stall` and was dropped
    /// from it: every later receive returns this. Drop the connection.
    Unsubscribed,
    /// A service's client: the server that took the request ended before it replied. The request is answered (the
    /// next receive returns the next reply); it may or may not have been carried out.
    Abandoned,
}

impl Error {
    /// The C API's result code (`FIPC_TIMEOUT`, ...).
    pub fn code(self) -> sys::fipc_result_t {
        match self {
            Error::Timeout => sys::FIPC_TIMEOUT,
            Error::Disconnected => sys::FIPC_DISCONNECTED,
            Error::Cancelled => sys::FIPC_CANCELLED,
            Error::TooLarge { .. } => sys::FIPC_TOO_LARGE,
            Error::Invalid => sys::FIPC_INVALID,
            Error::NoMemory => sys::FIPC_NO_MEMORY,
            Error::AddrInUse => sys::FIPC_ADDR_IN_USE,
            Error::Overrun { .. } => sys::FIPC_OVERRUN,
            Error::Unsubscribed => sys::FIPC_UNSUBSCRIBED,
            Error::Abandoned => sys::FIPC_ABANDONED,
        }
    }

    /// The C API's name of the result (`"FIPC_TIMEOUT"`, ...), from `fipc_result_str`.
    pub fn name(self) -> &'static str {
        // SAFETY: fipc_result_str returns a static, NUL-terminated string.
        let name = unsafe { CStr::from_ptr(sys::fipc_result_str(self.code())) };
        name.to_str().unwrap_or("FIPC_UNKNOWN")
    }

    /// The error a call's result code stands for; `len` is the receive's `*out_len`: the message's length for
    /// `FIPC_TOO_LARGE`, the number of messages lost for `FIPC_OVERRUN`.
    pub(crate) fn from_code(code: sys::fipc_result_t, len: usize) -> Error {
        match code {
            sys::FIPC_TIMEOUT => Error::Timeout,
            sys::FIPC_DISCONNECTED => Error::Disconnected,
            sys::FIPC_CANCELLED => Error::Cancelled,
            sys::FIPC_TOO_LARGE => Error::TooLarge { len },
            sys::FIPC_NO_MEMORY => Error::NoMemory,
            sys::FIPC_ADDR_IN_USE => Error::AddrInUse,
            sys::FIPC_OVERRUN => Error::Overrun { lost: len as u64 },
            sys::FIPC_UNSUBSCRIBED => Error::Unsubscribed,
            sys::FIPC_ABANDONED => Error::Abandoned,
            _ => Error::Invalid, // FIPC_INVALID; the library returns no other code
        }
    }
}

/// `Ok(())` for `FIPC_OK`, else the error (`len` as in [`Error::from_code`]).
pub(crate) fn check(code: sys::fipc_result_t, len: usize) -> Result<()> {
    if code == sys::FIPC_OK { Ok(()) } else { Err(Error::from_code(code, len)) }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = self.name();
        match self {
            Error::Timeout => write!(f, "{name}: the timeout ran out"),
            Error::Disconnected => write!(f, "{name}: the peer ended the connection"),
            Error::Cancelled => write!(f, "{name}: the call was cancelled"),
            Error::TooLarge { len } => write!(f, "{name}: the message ({len} bytes) doesn't fit"),
            Error::Invalid => {
                write!(f, "{name}: a bad argument or handle, a call out of order, or an incompatible peer")
            }
            Error::NoMemory => write!(f, "{name}: memory or another OS resource ran out"),
            Error::AddrInUse => write!(f, "{name}: another stream, broadcast, queue or service holds the name"),
            Error::Overrun { lost } => write!(f, "{name}: {lost} messages lost (the subscriber fell a lap behind)"),
            Error::Unsubscribed => write!(f, "{name}: the gated subscriber held the publisher back and was dropped"),
            Error::Abandoned => write!(f, "{name}: the server that took the request ended before it replied"),
        }
    }
}

impl std::error::Error for Error {}

impl Error {
    /// The nearest [`io::ErrorKind`].
    fn io_kind(self) -> io::ErrorKind {
        match self {
            Error::Timeout => io::ErrorKind::TimedOut,
            Error::Disconnected => io::ErrorKind::ConnectionReset,
            Error::Cancelled => io::ErrorKind::Interrupted,
            Error::TooLarge { .. } | Error::Invalid => io::ErrorKind::InvalidInput,
            Error::NoMemory => io::ErrorKind::OutOfMemory,
            Error::AddrInUse => io::ErrorKind::AddrInUse,
            Error::Overrun { .. } => io::ErrorKind::Other,
            Error::Unsubscribed | Error::Abandoned => io::ErrorKind::ConnectionAborted,
        }
    }
}

/// For functions that return `std::io::Result`: the nearest [`io::ErrorKind`], with this error as the source.
impl From<Error> for io::Error {
    fn from(error: Error) -> io::Error {
        io::Error::new(error.io_kind(), error)
    }
}

/// Why a wait for members didn't succeed ([`Connection::wait_for_members`](crate::Connection::wait_for_members) on a
/// publisher, [`Queue::wait_for_members`](crate::Queue::wait_for_members)): the error, and how many members were
/// joined at that moment.
///
/// At [`Error::Timeout`] (fewer than `at_least` joined in time), [`Error::Cancelled`] and [`Error::Disconnected`] (the
/// queue ended), `count` is the number joined; at [`Error::Invalid`] (not a publisher, or `at_least` over the maximum)
/// it is 0. It converts into an [`Error`] (and so works with `?` in a function that returns [`Result`]) and into an
/// [`io::Error`].
///
/// ```
/// # use fipc::{Error, MembersError, NO_WAIT};
/// # fn main() -> fipc::Result<()> {
/// let publisher = fipc::broadcast::create("fipc_doc_members_error", 4096, 8, None)?;
/// assert_eq!(publisher.wait_for_members(1, NO_WAIT), Err(MembersError { error: Error::Timeout, count: 0 }));
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MembersError {
    /// Why the call didn't succeed: [`Error::Timeout`], [`Error::Cancelled`], [`Error::Disconnected`] (a queue's) or
    /// [`Error::Invalid`].
    pub error: Error,
    /// How many members were joined when the call returned (0 at [`Error::Invalid`]).
    pub count: u32,
}

impl MembersError {
    /// The result of a native wait for members: the count at `FIPC_OK`, else the error with the count it stored (none
    /// at `FIPC_INVALID`).
    pub(crate) fn check(code: sys::fipc_result_t, count: u32) -> std::result::Result<u32, MembersError> {
        match check(code, 0) {
            Ok(()) => Ok(count),
            Err(Error::Invalid) => Err(MembersError { error: Error::Invalid, count: 0 }),
            Err(error) => Err(MembersError { error, count }),
        }
    }
}

impl fmt::Display for MembersError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ({} members joined)", self.error, self.count)
    }
}

impl std::error::Error for MembersError {}

impl From<MembersError> for Error {
    fn from(error: MembersError) -> Error {
        error.error
    }
}

/// The [`io::ErrorKind`] of the error, with this error (and its count) as the source.
impl From<MembersError> for io::Error {
    fn from(error: MembersError) -> io::Error {
        io::Error::new(error.error.io_kind(), error)
    }
}

/// A batch's send that didn't send every message ([`Connection::send_many`](crate::Connection::send_many)): the error
/// of the first message that didn't go, as [`send`](crate::Connection::send) would return it, and how many went (all
/// those before it). It converts into an [`Error`] (and so works with `?` in a function that returns [`Result`]) and
/// into an [`io::Error`].
///
/// ```
/// # use fipc::{Error, SendManyError, NO_WAIT};
/// # fn main() -> fipc::Result<()> {
/// # let mut listener = fipc::stream::create("fipc_doc_send_many_error", 1 << 16)?;
/// # let client = std::thread::spawn(|| fipc::stream::connect("fipc_doc_send_many_error", None));
/// # let _server = listener.accept(None)?;
/// # let mut conn = client.join().unwrap()?;
/// let batch: [&[u8]; 2] = [b"one", b""];
/// assert_eq!(conn.send_many(&batch, NO_WAIT), Err(SendManyError { error: Error::Invalid, sent: 0 }));
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SendManyError {
    /// Why the first message that didn't go didn't: [`Error::Timeout`], [`Error::Cancelled`],
    /// [`Error::Disconnected`], [`Error::TooLarge`] or [`Error::Invalid`] (which every message is checked for before
    /// any goes).
    pub error: Error,
    /// How many of the batch's messages went: every one before the first that didn't.
    pub sent: usize,
}

impl fmt::Display for SendManyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ({} messages sent)", self.error, self.sent)
    }
}

impl std::error::Error for SendManyError {}

impl From<SendManyError> for Error {
    fn from(error: SendManyError) -> Error {
        error.error
    }
}

/// The [`io::ErrorKind`] of the error, with this error (and how many went) as the source.
impl From<SendManyError> for io::Error {
    fn from(error: SendManyError) -> io::Error {
        io::Error::new(error.error.io_kind(), error)
    }
}
