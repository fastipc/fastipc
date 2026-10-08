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
/// # use fipc::{Connection, Error, NO_WAIT};
/// assert_eq!(Connection::connect("fipc_doc_nobody_listens", NO_WAIT).unwrap_err(), Error::Timeout);
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
    /// to talk again, accept or connect a new one.
    Disconnected,
    /// The listener or connection was cancelled ([`Canceler::cancel`](crate::Canceler::cancel)), and the call would
    /// have to wait.
    Cancelled,
    /// The message doesn't fit what the call can take: the caller's buffer, or one piece for zero-copy. `len` is the
    /// message's length (for an RPC message, its payload's). A receive leaves the message queued, so receive it again
    /// into `len` bytes (or with the copying calls, for a zero-copy receive).
    TooLarge {
        /// The message's length in bytes: the room a receive needs, or what a zero-copy send asked for.
        len: usize,
    },
    /// A bad argument (a name with a character the C API doesn't take, a capacity that isn't a power of two from
    /// 1024 to 2^31, an empty plain message), a call out of order (an accept while the listener's last connection is
    /// open), a peer with another version or user, or corrupt ring content.
    Invalid,
    /// Memory or another OS resource ran out while setting a connection up.
    NoMemory,
    /// Another listener holds the name.
    AddrInUse,
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
        }
    }

    /// The C API's name of the result (`"FIPC_TIMEOUT"`, ...), from `fipc_result_str`.
    pub fn name(self) -> &'static str {
        // SAFETY: fipc_result_str returns a static, NUL-terminated string.
        let name = unsafe { CStr::from_ptr(sys::fipc_result_str(self.code())) };
        name.to_str().unwrap_or("FIPC_UNKNOWN")
    }

    /// The error a call's result code stands for; `len` is the message's length for `FIPC_TOO_LARGE`.
    pub(crate) fn from_code(code: sys::fipc_result_t, len: usize) -> Error {
        match code {
            sys::FIPC_TIMEOUT => Error::Timeout,
            sys::FIPC_DISCONNECTED => Error::Disconnected,
            sys::FIPC_CANCELLED => Error::Cancelled,
            sys::FIPC_TOO_LARGE => Error::TooLarge { len },
            sys::FIPC_NO_MEMORY => Error::NoMemory,
            sys::FIPC_ADDR_IN_USE => Error::AddrInUse,
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
            Error::AddrInUse => write!(f, "{name}: another listener holds the name"),
        }
    }
}

impl std::error::Error for Error {}

/// For functions that return `std::io::Result`: the nearest [`io::ErrorKind`], with this error as the source.
impl From<Error> for io::Error {
    fn from(error: Error) -> io::Error {
        let kind = match error {
            Error::Timeout => io::ErrorKind::TimedOut,
            Error::Disconnected => io::ErrorKind::ConnectionReset,
            Error::Cancelled => io::ErrorKind::Interrupted,
            Error::TooLarge { .. } | Error::Invalid => io::ErrorKind::InvalidInput,
            Error::NoMemory => io::ErrorKind::OutOfMemory,
            Error::AddrInUse => io::ErrorKind::AddrInUse,
        };
        io::Error::new(kind, error)
    }
}
