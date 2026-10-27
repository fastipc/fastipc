//! Work queues: many producers, many consumers, each message to one consumer.
//!
//! The owner [`create`]s the queue's name and gets its [`Queue`], which ends the queue when it is closed or dropped.
//! Producers and consumers [`join`] as a [`QueueRole`], the owner too: the owner's process admits each joiner on a
//! thread of the library's, so a member may join from any thread, the owner's included.
//!
//! ```
//! use fipc::{Error, QueueRole};
//! use std::time::Duration;
//!
//! # fn main() -> fipc::Result<()> {
//! let queue = fipc::queue::create("fipc_doc_queue", 4096, 1, 1)?;
//! let mut producer = fipc::queue::join("fipc_doc_queue", QueueRole::Producer, Duration::from_secs(5))?;
//! producer.send(b"job 1", None)?; // waits in the queue until a consumer takes it
//! let mut consumer = fipc::queue::join("fipc_doc_queue", QueueRole::Consumer, Duration::from_secs(5))?;
//! queue.close(); // ends the queue: the consumer receives what is left, then Disconnected
//! assert_eq!(consumer.receive(None)?, b"job 1");
//! assert_eq!(consumer.receive(None), Err(Error::Disconnected));
//! assert_eq!(producer.send(b"job 2", None), Err(Error::Disconnected));
//! # Ok(())
//! # }
//! ```

use std::fmt;
use std::ptr::{self, NonNull};
use std::sync::Arc;
use std::time::Duration;

use fipc_sys as sys;

use crate::connection::{Canceler, Kind};
use crate::error::{MembersError, check};
use crate::{Connection, QueueRole, Result, c_name, millis};

/// Owner: claims `name` for a work queue and returns its [`Queue`]. Doesn't wait: producers and consumers may
/// [`join`] from now on, the owner too, and the owner's process admits each one.
///
/// The queue takes messages from up to `max_producers` producers and hands each to one of up to `max_consumers`
/// consumers (1 to 63 producers, 1 to 64 consumers), in any processes of the user. Each producer has a ring of
/// `capacity` bytes (a power of two, 4096 to 2^31) in the queue's memory, which every member maps; it holds up to
/// `capacity / 64` messages. A message is of one piece, 1 to a quarter of the capacity
/// ([`max_piece`](Connection::max_piece)), and waits in the queue until a consumer takes it, so a producer may send
/// before any consumer has joined. A consumer takes a message of any producer: the queue promises no order. A message
/// goes again, to another consumer, if the process of the consumer that took it ends before acknowledging it: a copying
/// receive acknowledges before it returns (at most once), one in place when its slot is dropped (dropped after the
/// work: at least once, so the work must tolerate a duplicate).
///
/// **A queue lives until its [`Queue`] is [closed](Queue::close) or dropped, or the owner's process ends;** nothing
/// else ends it (a member's drop or end doesn't). Keep the `Queue` for as long as the queue is to run: binding it to
/// `_`, or not at all, drops it, and the queue ends at once.
///
/// # Errors
///
/// [`Error::AddrInUse`](crate::Error::AddrInUse) if another stream, broadcast, queue or service holds the name;
/// [`Error::Invalid`](crate::Error::Invalid) for a bad name, capacity, `max_producers` or `max_consumers`;
/// [`Error::NoMemory`](crate::Error::NoMemory) if the memory for the whole queue can't be had (it is set up now, not
/// as members join).
#[must_use = "dropping the Queue ends the queue at once"]
pub fn create(name: &str, capacity: usize, max_producers: u32, max_consumers: u32) -> Result<Queue> {
    let c_name = c_name(name)?;
    let mut out = ptr::null_mut();
    // SAFETY: c_name is NUL-terminated; out is valid for a write.
    let code = unsafe { sys::fipc_queue_create(c_name.as_ptr(), capacity, max_producers, max_consumers, &mut out) };
    check(code, 0)?;
    let ptr = NonNull::new(out).expect("fipc_queue_create returned FIPC_OK without a queue");
    Ok(Queue { inner: Arc::new(QueueInner { ptr, name: name.into() }) })
}

/// Producer or consumer: joins the work queue on `name` as `role` and returns its connection, waiting up to `timeout`
/// for the owner to [`create`] the queue, for a free place of the role (a producer's place is free again once every
/// message its last producer sent was taken and released) and for the owner's process to admit it.
///
/// A producer's sends add messages to the queue; a consumer's receives take them, each message going to one consumer.
/// Once the queue ends, a producer's sends return [`Error::Disconnected`](crate::Error::Disconnected), and a consumer
/// receives every message left in the queue, then `Disconnected`. Dropping a producer or a consumer leaves the queue,
/// which goes on: the messages a producer sent stay for the consumers.
///
/// # Errors
///
/// [`Error::Timeout`](crate::Error::Timeout); [`Error::Invalid`](crate::Error::Invalid) for a bad name, a name a
/// broadcast holds, or an owner with another version of the protocol or running as another user;
/// [`Error::NoMemory`](crate::Error::NoMemory) if the queue can't be mapped. On a name a stream holds, the join learns
/// the kind when the stream's server accepts it, which brings [`Error::Invalid`](crate::Error::Invalid); while no
/// accept runs, it waits and returns [`Error::Timeout`](crate::Error::Timeout).
pub fn join(name: &str, role: QueueRole, timeout: impl Into<Option<Duration>>) -> Result<Connection> {
    let c_name = c_name(name)?;
    let mut out = ptr::null_mut();
    // SAFETY: c_name is NUL-terminated; out is valid for a write.
    check(unsafe { sys::fipc_queue_join(c_name.as_ptr(), role.code(), &mut out, millis(timeout)) }, 0)?;
    let ptr = NonNull::new(out).expect("fipc_queue_join returned FIPC_OK without a connection");
    // SAFETY: a connection fipc_queue_join just returned, owned by nothing else: a producer's or a consumer's.
    Ok(unsafe { Connection::from_raw(ptr, name, Kind::Plain) })
}

/// A work queue, held by its owner: [`create`] returns it. It counts the joined members
/// ([`wait_for_members`](Queue::wait_for_members)) and ends the queue when it is [closed](Queue::close) or dropped.
///
/// **A queue lives until its `Queue` is closed or dropped, or the owner's process ends;** nothing else ends it: no
/// garbage collector, a member's drop or end, or a [`Canceler`], which doesn't keep it alive. When it ends, producers'
/// sends return [`Error::Disconnected`](crate::Error::Disconnected), and consumers receive every message left in the
/// queue, then `Disconnected`. The members' connections, the owner's own included, stay open until they are dropped.
///
/// `Send` and `Sync`: [`wait_for_members`](Queue::wait_for_members) and [`cancel`](Queue::cancel) take `&self`, so
/// one thread may wait for members while another cancels.
///
/// A `Queue` left unused as a statement's value is a warning (`unused_must_use`), since the queue would end at once;
/// `let _ = ...` silences the warning and ends the queue all the same:
///
/// ```compile_fail
/// #![deny(unused_must_use)]
/// fipc::queue::create("fipc_doc_unused_queue", 4096, 1, 1).unwrap(); // dropped here: the queue ends
/// ```
#[must_use = "dropping the Queue ends the queue at once"]
pub struct Queue {
    inner: Arc<QueueInner>,
}

/// The native queue; dropping it closes it (which ends the queue), once no call holds it.
pub(crate) struct QueueInner {
    ptr: NonNull<sys::fipc_queue_t>,
    name: Box<str>,
}

// SAFETY: fipc_queue_wait_for_members and fipc_queue_cancel may run on any thread at any time before the close, which
// runs in Drop, once no reference (and so no call) is left.
unsafe impl Send for QueueInner {}
// SAFETY: as above: through a shared reference, only the wait for members, the cancel and the name are reachable.
unsafe impl Sync for QueueInner {}

impl QueueInner {
    pub(crate) fn cancel(&self) {
        // SAFETY: the queue is open while self lives; fipc_queue_cancel may run on any thread.
        unsafe { sys::fipc_queue_cancel(self.ptr.as_ptr()) }
    }
}

impl Drop for QueueInner {
    fn drop(&mut self) {
        // SAFETY: the last reference: no other thread is inside a call on the queue.
        unsafe { sys::fipc_queue_close(self.ptr.as_ptr()) }
    }
}

impl Queue {
    /// Waits up to `timeout` until at least `at_least` members of `role` are joined, and returns how many are: `None`
    /// counts producers and consumers together. Joined: admitted, and not yet left. A member that leaves, or whose
    /// process ends, is counted until the owner's process has seen its end, soon after. `at_least` 0 returns the count
    /// at once, and [`NO_WAIT`](crate::NO_WAIT) polls. Any thread: while one thread waits here, another may
    /// [`cancel`](Queue::cancel) the queue.
    ///
    /// ```
    /// use fipc::QueueRole;
    /// use std::time::Duration;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let queue = fipc::queue::create("fipc_doc_queue_members", 4096, 2, 2)?;
    /// assert_eq!(queue.wait_for_members(None, 0, None)?, 0); // the count, at once
    /// let _producer = fipc::queue::join("fipc_doc_queue_members", QueueRole::Producer, Duration::from_secs(5))?;
    /// # let _consumer = fipc::queue::join("fipc_doc_queue_members", QueueRole::Consumer, Duration::from_secs(5))?;
    /// // ... a consumer joins
    /// assert_eq!(queue.wait_for_members(QueueRole::Consumer, 1, Duration::from_secs(5))?, 1);
    /// assert_eq!(queue.wait_for_members(None, 0, None)?, 2); // both roles
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// A [`MembersError`] with the error and the count at that moment: [`Error::Timeout`](crate::Error::Timeout) if
    /// fewer than `at_least` joined in time; [`Error::Cancelled`](crate::Error::Cancelled) once the queue is
    /// cancelled; [`Error::Disconnected`](crate::Error::Disconnected) once the queue has ended;
    /// [`Error::Invalid`](crate::Error::Invalid) (count 0) for `at_least` over the role's maximum (`max_producers`,
    /// `max_consumers`, or their sum for `None`).
    pub fn wait_for_members(
        &self,
        role: impl Into<Option<QueueRole>>,
        at_least: u32,
        timeout: impl Into<Option<Duration>>,
    ) -> std::result::Result<u32, MembersError> {
        let role = role.into().map_or(0, QueueRole::code);
        let mut count = 0;
        // SAFETY: the queue is open while self lives (the close waits for every reference, this call's included);
        // fipc_queue_wait_for_members may run on any thread; count is valid for a write.
        let code = unsafe {
            sys::fipc_queue_wait_for_members(self.inner.ptr.as_ptr(), role, at_least, &mut count, millis(timeout))
        };
        MembersError::check(code, count)
    }

    /// Makes every [`wait_for_members`](Queue::wait_for_members) that waits, now or later, return
    /// [`Error::Cancelled`](crate::Error::Cancelled). The queue goes on: its members and their joins are unaffected.
    /// Final. To cancel from another thread, while this one waits, use a [`canceler`](Queue::canceler).
    pub fn cancel(&self) {
        self.inner.cancel();
    }

    /// A handle that cancels this queue from any thread, as [`cancel`](Queue::cancel) does: the way to stop a thread
    /// that waits in [`wait_for_members`](Queue::wait_for_members). It doesn't keep the queue alive.
    pub fn canceler(&self) -> Canceler {
        Canceler::queue(Arc::downgrade(&self.inner))
    }

    /// Ends the queue, as dropping it does, and frees the name: producers' sends return
    /// [`Error::Disconnected`](crate::Error::Disconnected), and consumers receive every message left in the queue, then
    /// `Disconnected`. The members' connections, the owner's own included, stay open until they are dropped.
    pub fn close(self) {}

    /// The queue's name.
    pub fn name(&self) -> &str {
        &self.inner.name
    }

    /// The native queue, for calls through [`sys`]. It stays valid while this queue lives; the queue's
    /// rules apply (don't close it).
    pub fn as_raw(&self) -> *mut sys::fipc_queue_t {
        self.inner.ptr.as_ptr()
    }
}

impl fmt::Debug for Queue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Queue").field("name", &self.inner.name).finish_non_exhaustive()
    }
}
