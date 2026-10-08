use std::fmt;
use std::ptr::{self, NonNull};
use std::sync::Arc;
use std::time::Duration;

use fipc_sys as sys;

use crate::connection::{Canceler, Connection};
use crate::error::check;
use crate::{Result, c_name, millis};

/// A server's name: it listens on the name, and [`accept`](Listener::accept) sets up one client's [`Connection`] at a
/// time, on the calling thread.
///
/// Dropping the listener stops listening and frees the name (on Windows, once the connections it accepted are
/// dropped too); those connections stay open. A client whose setup an `accept` began and didn't finish is dropped (if
/// its [`connect`](Connection::connect) returned, it gets [`Error::Disconnected`](crate::Error::Disconnected)).
///
/// ```
/// use fipc::{Connection, Listener};
/// use std::time::Duration;
///
/// # fn main() -> fipc::Result<()> {
/// let mut listener = Listener::listen("fipc_doc_listener", 1 << 16)?; // rings of 64 KiB each way
/// // The client, on another thread (or in another process): connect waits for the listener's accept
/// let client = std::thread::spawn(|| Connection::connect("fipc_doc_listener", Duration::from_secs(5)));
/// let mut server = listener.accept(Duration::from_secs(5))?;
/// let mut client = client.join().unwrap()?;
/// client.send(b"hello", None)?;
/// assert_eq!(server.receive(None)?, b"hello");
/// # Ok(())
/// # }
/// ```
pub struct Listener {
    inner: Arc<ListenerInner>,
}

/// The native listener; dropping it closes it, once no call holds it.
pub(crate) struct ListenerInner {
    ptr: NonNull<sys::fipc_listener_t>,
    name: Box<str>,
}

// SAFETY: the native listener may be used from any thread, one accept at a time (Listener::accept takes &mut self)
// and fipc_listener_cancel from any thread at any time before the close. The close runs in Drop, once no reference
// (and so no call) is left.
unsafe impl Send for ListenerInner {}
// SAFETY: as above: the only calls through a shared reference are the cancel and reading the name.
unsafe impl Sync for ListenerInner {}

impl ListenerInner {
    pub(crate) fn cancel(&self) {
        // SAFETY: the listener is open while self lives; fipc_listener_cancel may run on any thread.
        unsafe { sys::fipc_listener_cancel(self.ptr.as_ptr()) }
    }
}

impl Drop for ListenerInner {
    fn drop(&mut self) {
        // SAFETY: the last reference: no other thread is inside a call on the listener.
        unsafe { sys::fipc_listener_close(self.ptr.as_ptr()) }
    }
}

impl Listener {
    /// Claims `name` and listens on it, with rings of `capacity` bytes each way for every connection. Doesn't wait:
    /// clients may connect from now on, and [`accept`](Listener::accept) sets each one up.
    ///
    /// A name is 1-245 characters of `[A-Za-z0-9_.-]`, not starting with `.` or `-`, local to the user (Linux) or
    /// to the desktop session (Windows). The capacity is a power of two from 1024 to 2^31; a client's connection
    /// has the server's.
    ///
    /// # Errors
    ///
    /// [`Error::AddrInUse`](crate::Error::AddrInUse) if another listener holds the name (it is freed when that
    /// listener is dropped or its process ends); [`Error::Invalid`](crate::Error::Invalid) for a bad name or
    /// capacity.
    pub fn listen(name: &str, capacity: usize) -> Result<Listener> {
        let c_name = c_name(name)?;
        let mut out = ptr::null_mut();
        // SAFETY: c_name is NUL-terminated; out is valid for a write.
        check(unsafe { sys::fipc_listen(c_name.as_ptr(), capacity, &mut out) }, 0)?;
        let ptr = NonNull::new(out).expect("fipc_listen returned FIPC_OK without a listener");
        Ok(Listener { inner: Arc::new(ListenerInner { ptr, name: name.into() }) })
    }

    /// Waits up to `timeout` for a client, sets its connection up on the calling thread, and returns it; it may already
    /// hold the client's first messages. A call that times out in the middle of a client's setup keeps it for the next
    /// call, so [`NO_WAIT`](crate::NO_WAIT) polls (a client then takes one or two calls).
    ///
    /// One client at a time: [`Error::Invalid`](crate::Error::Invalid) while the connection this listener returned
    /// last is open (drop it first); a client that connects meanwhile waits, within its own timeout. A client with
    /// another version of the protocol, or running as another user, is refused, and the call goes on waiting.
    ///
    /// # Errors
    ///
    /// [`Error::Timeout`](crate::Error::Timeout); [`Error::Cancelled`](crate::Error::Cancelled) once the listener is
    /// cancelled; [`Error::Invalid`](crate::Error::Invalid) as above; [`Error::NoMemory`](crate::Error::NoMemory)
    /// once the listener couldn't get the memory for a client's rings: it has failed for good (drop it, and listen
    /// again to go on).
    pub fn accept(&mut self, timeout: impl Into<Option<Duration>>) -> Result<Connection> {
        let mut out = ptr::null_mut();
        // SAFETY: the listener is open; &mut self makes this the one thread accepting; out is valid for a write.
        check(unsafe { sys::fipc_accept(self.inner.ptr.as_ptr(), &mut out, millis(timeout)) }, 0)?;
        let ptr = NonNull::new(out).expect("fipc_accept returned FIPC_OK without a connection");
        // SAFETY: a connection fipc_accept just returned, owned by nothing else.
        Ok(unsafe { Connection::from_raw(ptr, &self.inner.name) })
    }

    /// Makes every [`accept`](Listener::accept) that waits, now or later, return
    /// [`Error::Cancelled`](crate::Error::Cancelled); a client whose setup an `accept` began is dropped. Final. To
    /// cancel from another thread, while this one waits in `accept`, use a [`canceler`](Listener::canceler).
    pub fn cancel(&self) {
        self.inner.cancel();
    }

    /// A handle that cancels this listener from any thread, as [`cancel`](Listener::cancel) does: the way to stop a
    /// thread that waits in [`accept`](Listener::accept).
    ///
    /// ```
    /// use fipc::{Error, Listener};
    ///
    /// # fn main() -> fipc::Result<()> {
    /// let mut listener = Listener::listen("fipc_doc_listener_cancel", 1 << 16)?;
    /// let canceler = listener.canceler();
    /// let server = std::thread::spawn(move || listener.accept(None).map(|_| ()));
    /// canceler.cancel();
    /// assert_eq!(server.join().unwrap(), Err(Error::Cancelled));
    /// # Ok(())
    /// # }
    /// ```
    pub fn canceler(&self) -> Canceler {
        Canceler::listener(Arc::downgrade(&self.inner))
    }

    /// The name this listener listens on.
    pub fn name(&self) -> &str {
        &self.inner.name
    }

    /// The native listener, for calls through [`sys`](crate::sys). It stays valid while this listener lives; the
    /// listener's rules apply (one thread at a time accepts; don't close it).
    pub fn as_raw(&self) -> *mut sys::fipc_listener_t {
        self.inner.ptr.as_ptr()
    }
}

impl fmt::Debug for Listener {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Listener").field("name", &self.inner.name).finish_non_exhaustive()
    }
}
