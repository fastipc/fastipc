//! Services: many clients, a pool of servers, each request to one server and its reply back to its client.
//!
//! The owner [`create`]s the service's name and gets its [`Service`], which ends the service when it is closed or
//! dropped. Clients and servers [`join`] as a [`ServiceRole`], the owner too: the owner's process admits each joiner on
//! a thread of the library's, so a member may join from any thread, the owner's included. A client's
//! [`call`](Connection::call) sends a request and returns its reply; a server's receive takes the next request of any
//! client, and its next send replies to it.
//!
//! ```
//! use fipc::{Error, ServiceRole};
//! use std::time::Duration;
//!
//! # fn main() -> fipc::Result<()> {
//! let service = fipc::service::create("fipc_doc_service", 4096, 256, 1, 1)?;
//! let mut client = fipc::service::join("fipc_doc_service", ServiceRole::Client, Duration::from_secs(5))?;
//! let mut server = fipc::service::join("fipc_doc_service", ServiceRole::Server, Duration::from_secs(5))?;
//! client.send(b"job 1", None)?; // a request: the next server's receive takes it
//! let request = server.receive(None)?;
//! server.send(&request.to_ascii_uppercase(), None)?; // the reply to the request it holds, which never waits
//! assert_eq!(client.receive(None)?, b"JOB 1"); // the replies come in the order of the requests
//! service.close(); // ends the service: the client receives the replies sent, then Disconnected
//! assert_eq!(client.receive(None), Err(Error::Disconnected));
//! assert_eq!(server.receive(None), Err(Error::Disconnected));
//! # Ok(())
//! # }
//! ```
//!
//! # A server's request and its reply
//!
//! A server holds one request at a time, from its receive until its reply. A request received in place stays valid
//! until the reply, and from then on its bytes are its client's again; the borrow checker keeps them from being read
//! after the reply. [`receive_request`](Connection::receive_request) returns a [`Request`](crate::Request), whose
//! reply consumes it; [`receive_acquire`](Connection::receive_acquire)'s slot borrows the whole connection, so the
//! reply waits for its drop:
//!
//! ```compile_fail
//! # fn main() -> fipc::Result<()> {
//! # let mut server = fipc::service::join("fipc_doc_slot_and_reply", fipc::ServiceRole::Server, None)?;
//! let slot = server.receive_acquire(None)?;
//! server.send(&slot.to_ascii_uppercase(), None)?; // the slot borrows the connection: drop it first
//! # Ok(())
//! # }
//! ```
//!
//! A server split into its [`Sender`](crate::Sender) and [`Receiver`](crate::Receiver) receives copies only: the
//! [`Receiver`](crate::Receiver)'s [`receive_acquire`](crate::Receiver::receive_acquire) returns
//! [`Error::Invalid`](crate::Error::Invalid), since the [`Sender`](crate::Sender) could reply while the slot lives.

use std::fmt;
use std::ptr::{self, NonNull};
use std::sync::Arc;
use std::time::Duration;

use fipc_sys as sys;

use crate::connection::{Canceler, Kind};
use crate::error::{MembersError, check};
use crate::{Connection, Result, ServiceRole, c_name, millis};

/// Owner: claims `name` for a service and returns its [`Service`]. Doesn't wait: clients and servers may [`join`] from
/// now on, the owner too, and the owner's process admits each one.
///
/// The service takes requests from up to `max_clients` clients and hands each to one of up to `max_servers` servers
/// (1 to 64 each), in any processes of the user; the server that took a request replies to it, and the reply goes back
/// to the client that sent it. Each client has a ring of `capacity` bytes (a power of two, 4096 to 2^31) in the
/// service's memory, which every member maps; a request takes room there for itself and for its reply, from its send
/// until its reply has been received, so a server never waits for a client. Requests and replies are of one piece, 1
/// to `max_message` bytes (1 to `capacity / 4`; [`max_piece`](Connection::max_piece)). A request takes
/// `round64(its length) + round64(max_message)` bytes of its client's ring (`round64` rounds up to a multiple of 64),
/// and a ring holds at most `capacity / 64` requests.
///
/// A client's receive returns the reply to its oldest request whose reply it hasn't received yet, so the n-th reply
/// it receives answers its n-th request, whichever server replies first. A request is answered once: by its reply, or
/// with [`Error::Abandoned`](crate::Error::Abandoned) if the server that took it ended before it replied (it was
/// dropped, or its process ended); an abandoned request may or may not have been carried out, and the service never
/// sends it again. Not covered: a server that hangs while its process runs (there is no timeout: its client waits,
/// within its own timeouts), and the owner's process, with which the service ends.
///
/// **A service lives until its [`Service`] is [closed](Service::close) or dropped, or the owner's process ends;**
/// nothing else ends it (a member's drop or end doesn't). Keep the `Service` for as long as the service is to run:
/// binding it to `_`, or not at all, drops it, and the service ends at once.
///
/// # Errors
///
/// [`Error::AddrInUse`](crate::Error::AddrInUse) if another stream, broadcast, queue or service holds the name;
/// [`Error::Invalid`](crate::Error::Invalid) for a bad name, capacity, `max_message`, `max_clients` or `max_servers`;
/// [`Error::NoMemory`](crate::Error::NoMemory) if the memory for the whole service can't be had (it is set up now,
/// not as members join).
#[must_use = "dropping the Service ends the service at once"]
pub fn create(name: &str, capacity: usize, max_message: usize, max_clients: u32, max_servers: u32) -> Result<Service> {
    let c_name = c_name(name)?;
    let mut out = ptr::null_mut();
    // SAFETY: c_name is NUL-terminated; out is valid for a write.
    let code =
        unsafe { sys::fipc_service_create(c_name.as_ptr(), capacity, max_message, max_clients, max_servers, &mut out) };
    check(code, 0)?;
    let ptr = NonNull::new(out).expect("fipc_service_create returned FIPC_OK without a service");
    Ok(Service { inner: Arc::new(ServiceInner { ptr, name: name.into() }) })
}

/// Client or server: joins the service on `name` as `role` and returns its connection, waiting up to `timeout` for
/// the owner to [`create`] the service, for a free place of the role (a client's place is free again once its last
/// client has left and every request of it that a server took has been answered) and for the owner's process to admit
/// it.
///
/// A client's sends are requests, and its receives their replies, in the order of its requests;
/// [`call`](Connection::call) does both. A server's receive takes the next request of any client and holds it until
/// its next send, the reply, which never waits ([`receive_request`](Connection::receive_request) holds it in place).
/// Clients wait while no server is joined, and servers while no client is. Dropping a client leaves the service: its
/// requests that no server took are dropped, and the replies to those already taken are lost. Dropping a server
/// leaves it too, and a request it still holds is answered [`Error::Abandoned`](crate::Error::Abandoned). Once the
/// service ends, clients' sends and servers' receives return [`Error::Disconnected`](crate::Error::Disconnected), a
/// client receives the replies that came before the end, then `Disconnected`, and a server's reply returns
/// `Disconnected`.
///
/// # Errors
///
/// [`Error::Timeout`](crate::Error::Timeout); [`Error::Invalid`](crate::Error::Invalid) for a bad name, a name a
/// broadcast or a queue holds, or an owner with another version of the protocol or running as another user;
/// [`Error::NoMemory`](crate::Error::NoMemory) if the service can't be mapped. On a name a stream holds, the join
/// learns the kind when the stream's server accepts it, which brings [`Error::Invalid`](crate::Error::Invalid); while
/// no accept runs, it waits and returns [`Error::Timeout`](crate::Error::Timeout).
pub fn join(name: &str, role: ServiceRole, timeout: impl Into<Option<Duration>>) -> Result<Connection> {
    let c_name = c_name(name)?;
    let mut out = ptr::null_mut();
    // SAFETY: c_name is NUL-terminated; out is valid for a write.
    check(unsafe { sys::fipc_service_join(c_name.as_ptr(), role.code(), &mut out, millis(timeout)) }, 0)?;
    let ptr = NonNull::new(out).expect("fipc_service_join returned FIPC_OK without a connection");
    let kind = match role {
        ServiceRole::Client => Kind::Client,
        ServiceRole::Server => Kind::Server,
    };
    // SAFETY: a connection fipc_service_join just returned, owned by nothing else: a client's or a server's, as role.
    Ok(unsafe { Connection::from_raw(ptr, name, kind) })
}

/// A service, held by its owner: [`create`] returns it. It counts the joined members
/// ([`wait_for_members`](Service::wait_for_members)) and ends the service when it is [closed](Service::close) or
/// dropped.
///
/// **A service lives until its `Service` is closed or dropped, or the owner's process ends;** nothing else ends it: no
/// garbage collector, a member's drop or end, or a [`Canceler`], which doesn't keep it alive. When it ends, clients'
/// sends and servers' receives return [`Error::Disconnected`](crate::Error::Disconnected), a client receives the
/// replies that came before the end, then `Disconnected`, and a server's reply returns `Disconnected`. The members'
/// connections, the owner's own included, stay open until they are dropped.
///
/// `Send` and `Sync`: [`wait_for_members`](Service::wait_for_members) and [`cancel`](Service::cancel) take `&self`, so
/// one thread may wait for members while another cancels.
///
/// A `Service` left unused as a statement's value is a warning (`unused_must_use`), since the service would end at
/// once; `let _ = ...` silences the warning and ends the service all the same:
///
/// ```compile_fail
/// #![deny(unused_must_use)]
/// fipc::service::create("fipc_doc_unused_service", 4096, 256, 1, 1).unwrap(); // dropped here: the service ends
/// ```
#[must_use = "dropping the Service ends the service at once"]
pub struct Service {
    inner: Arc<ServiceInner>,
}

/// The native service; dropping it closes it (which ends the service), once no call holds it.
pub(crate) struct ServiceInner {
    ptr: NonNull<sys::fipc_service_t>,
    name: Box<str>,
}

// SAFETY: fipc_service_wait_for_members and fipc_service_cancel may run on any thread at any time before the close,
// which runs in Drop, once no reference (and so no call) is left.
unsafe impl Send for ServiceInner {}
// SAFETY: as above: through a shared reference, only the wait for members, the cancel and the name are reachable.
unsafe impl Sync for ServiceInner {}

impl ServiceInner {
    pub(crate) fn cancel(&self) {
        // SAFETY: the service is open while self lives; fipc_service_cancel may run on any thread.
        unsafe { sys::fipc_service_cancel(self.ptr.as_ptr()) }
    }
}

impl Drop for ServiceInner {
    fn drop(&mut self) {
        // SAFETY: the last reference: no other thread is inside a call on the service.
        unsafe { sys::fipc_service_close(self.ptr.as_ptr()) }
    }
}

impl Service {
    /// Waits up to `timeout` until at least `at_least` members of `role` are joined, and returns how many are: `None`
    /// counts clients and servers together. Joined: admitted, and not yet left. A member that leaves, or whose process
    /// ends, is counted until the owner's process has seen its end, soon after. `at_least` 0 returns the count at
    /// once, and [`NO_WAIT`](crate::NO_WAIT) polls. Clients wait while no server is joined, and servers while no
    /// client is, so no member needs it; an owner may, to start its work once its servers are there. Any thread: while
    /// one thread waits here, another may [`cancel`](Service::cancel) the service.
    ///
    /// ```
    /// use fipc::ServiceRole;
    /// use std::time::Duration;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let service = fipc::service::create("fipc_doc_service_members", 4096, 256, 2, 2)?;
    /// assert_eq!(service.wait_for_members(None, 0, None)?, 0); // the count, at once
    /// let _client = fipc::service::join("fipc_doc_service_members", ServiceRole::Client, Duration::from_secs(5))?;
    /// # let _server = fipc::service::join("fipc_doc_service_members", ServiceRole::Server, Duration::from_secs(5))?;
    /// // ... a server joins
    /// assert_eq!(service.wait_for_members(ServiceRole::Server, 1, Duration::from_secs(5))?, 1);
    /// assert_eq!(service.wait_for_members(None, 0, None)?, 2); // both roles
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// A [`MembersError`] with the error and the count at that moment: [`Error::Timeout`](crate::Error::Timeout) if
    /// fewer than `at_least` joined in time; [`Error::Cancelled`](crate::Error::Cancelled) once the service is
    /// cancelled; [`Error::Disconnected`](crate::Error::Disconnected) once the service has ended;
    /// [`Error::Invalid`](crate::Error::Invalid) (count 0) for `at_least` over the role's maximum (`max_clients`,
    /// `max_servers`, or their sum for `None`).
    pub fn wait_for_members(
        &self,
        role: impl Into<Option<ServiceRole>>,
        at_least: u32,
        timeout: impl Into<Option<Duration>>,
    ) -> std::result::Result<u32, MembersError> {
        let role = role.into().map_or(0, ServiceRole::code);
        let mut count = 0;
        // SAFETY: the service is open while self lives (the close waits for every reference, this call's included);
        // fipc_service_wait_for_members may run on any thread; count is valid for a write.
        let code = unsafe {
            sys::fipc_service_wait_for_members(self.inner.ptr.as_ptr(), role, at_least, &mut count, millis(timeout))
        };
        MembersError::check(code, count)
    }

    /// Makes every [`wait_for_members`](Service::wait_for_members) that waits, now or later, return
    /// [`Error::Cancelled`](crate::Error::Cancelled). The service goes on: its members and their joins are unaffected.
    /// Final. To cancel from another thread, while this one waits, use a [`canceler`](Service::canceler).
    pub fn cancel(&self) {
        self.inner.cancel();
    }

    /// A handle that cancels this service from any thread, as [`cancel`](Service::cancel) does: the way to stop a
    /// thread that waits in [`wait_for_members`](Service::wait_for_members). It doesn't keep the service alive.
    pub fn canceler(&self) -> Canceler {
        Canceler::service(Arc::downgrade(&self.inner))
    }

    /// Ends the service, as dropping it does, and frees the name: clients' sends and servers' receives return
    /// [`Error::Disconnected`](crate::Error::Disconnected), a client receives the replies that came before the end,
    /// then `Disconnected`, and a server's reply returns `Disconnected`. The members' connections, the owner's own
    /// included, stay open until they are dropped.
    pub fn close(self) {}

    /// The service's name.
    pub fn name(&self) -> &str {
        &self.inner.name
    }

    /// The native service, for calls through [`sys`]. It stays valid while this service lives; the service's rules
    /// apply (don't close it).
    pub fn as_raw(&self) -> *mut sys::fipc_service_t {
        self.inner.ptr.as_ptr()
    }
}

impl fmt::Debug for Service {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Service").field("name", &self.inner.name).finish_non_exhaustive()
    }
}
