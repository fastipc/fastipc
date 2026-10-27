use std::ffi::{c_int, c_void};
use std::fmt;
use std::ops::{Deref, DerefMut};
use std::ptr::{self, NonNull};
use std::slice;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use fipc_sys as sys;

use crate::error::{MembersError, SendManyError, check};
use crate::listener::ListenerInner;
use crate::queue::QueueInner;
use crate::service::ServiceInner;
use crate::{Error, NO_WAIT, Result, millis};

/// How a subscriber [joins](crate::broadcast::join) a broadcast: [`Lossy`](SubscriberRole::Lossy) or
/// [`Gated`](SubscriberRole::Gated).
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SubscriberRole {
    /// Never holds the publisher back (`FIPC_SUBSCRIBER_LOSSY`). One that falls a lap behind loses messages: a receive
    /// returns [`Error::Overrun`] with the number lost instead of a message, and the next goes on from a recent
    /// message (a message that a receive left queued with [`Error::TooLarge`] can be lost the same way). It receives
    /// copies only: a copying receive checks that the publisher didn't overwrite the message meanwhile, and
    /// [`receive_acquire`](Connection::receive_acquire) returns [`Error::Invalid`].
    Lossy,
    /// Misses nothing (`FIPC_SUBSCRIBER_GATED`): a send waits for room the subscriber still holds. One that holds the
    /// publisher back for the broadcast's `max_stall` is unsubscribed: its receives return [`Error::Unsubscribed`]. It
    /// may receive in place, with the unsafe [`receive_acquire_gated`](Connection::receive_acquire_gated) only: the
    /// message holds the publisher back until the slot is dropped, and once the subscriber is unsubscribed its memory
    /// is gone.
    Gated,
}

impl SubscriberRole {
    pub(crate) fn code(self) -> c_int {
        match self {
            SubscriberRole::Lossy => sys::FIPC_SUBSCRIBER_LOSSY,
            SubscriberRole::Gated => sys::FIPC_SUBSCRIBER_GATED,
        }
    }
}

/// How a member [joins](crate::queue::join) a work queue: [`Producer`](QueueRole::Producer) or
/// [`Consumer`](QueueRole::Consumer); also the role [`Queue::wait_for_members`](crate::Queue::wait_for_members)
/// counts.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum QueueRole {
    /// A work queue's producer (`FIPC_PRODUCER`): its sends add messages of one piece to its ring in the queue, where
    /// they wait until a consumer takes them; its receives return [`Error::Invalid`].
    Producer,
    /// A work queue's consumer (`FIPC_CONSUMER`): its receives take a message of any producer, which no other
    /// consumer gets, copied or in place ([`receive_acquire`](Connection::receive_acquire)); its sends return
    /// [`Error::Invalid`].
    Consumer,
}

impl QueueRole {
    pub(crate) fn code(self) -> c_int {
        match self {
            QueueRole::Producer => sys::FIPC_PRODUCER,
            QueueRole::Consumer => sys::FIPC_CONSUMER,
        }
    }
}

/// How a member [joins](crate::service::join) a service: [`Client`](ServiceRole::Client) or
/// [`Server`](ServiceRole::Server); also the role [`Service::wait_for_members`](crate::Service::wait_for_members)
/// counts.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ServiceRole {
    /// A service's client (`FIPC_CLIENT`): its sends are requests, into a ring of its own that holds the room for
    /// their replies too, and its receives return the replies in the order of its requests (copied or in place,
    /// [`receive_acquire`](Connection::receive_acquire)): each receive returns the reply to its oldest request whose
    /// reply it hasn't received yet, or [`Error::Abandoned`] if that request's server ended before it replied.
    /// [`call`](Connection::call) does both. Dropping it leaves the service: its requests that no server took are
    /// dropped.
    Client,
    /// A service's server (`FIPC_SERVER`): its receives take the next request of any client and hold it until its
    /// next send, which replies to it and never waits. While it holds a request its receives return
    /// [`Error::Invalid`], and with none held its sends do. [`receive_request`](Connection::receive_request) holds the
    /// request in place until the reply, which consumes it. Dropping it leaves the service; a request it still holds is
    /// answered [`Error::Abandoned`] (after [`into_split`](Connection::into_split), once both halves are dropped).
    Server,
}

impl ServiceRole {
    pub(crate) fn code(self) -> c_int {
        match self {
            ServiceRole::Client => sys::FIPC_CLIENT,
            ServiceRole::Server => sys::FIPC_SERVER,
        }
    }
}

/// What a connection is, as far as this crate's checks go.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Kind {
    /// A stream's, a publisher, a lossy subscriber, a work queue's member.
    Plain,
    /// A gated subscriber: a message received in place can become inaccessible (see `receive_acquire_gated`).
    Gated,
    /// A service's client: it keeps count of the replies owed to its requests (see `call`).
    Client,
    /// A service's server: a request held in place is valid until the reply (see `Receiver::receive_acquire`).
    Server,
}

/// One connection: a stream's two rings in shared memory, one per direction. A server gets one from
/// [`Listener::accept`](crate::Listener::accept), a client from [`stream::connect`](crate::stream::connect). A
/// broadcast's connections are of this type too: its publisher, from [`broadcast::create`](crate::broadcast::create),
/// only sends, and a subscriber, from [`broadcast::join`](crate::broadcast::join), only receives. So are a work
/// queue's members, from [`queue::join`](crate::queue::join): a producer only sends and a consumer only receives. And
/// a service's members, from [`service::join`](crate::service::join): a client sends requests and receives their
/// replies ([`call`](Connection::call) does both), and a server receives the next request of any client and replies
/// to it with its next send ([`receive_request`](Connection::receive_request) holds it in place until the reply).
///
/// The calls that send ([`send`](Connection::send), [`send_many`](Connection::send_many),
/// [`send_acquire`](Connection::send_acquire)) and the calls that receive ([`receive`](Connection::receive),
/// [`receive_into`](Connection::receive_into), [`receive_many`](Connection::receive_many),
/// [`receive_acquire`](Connection::receive_acquire)) take `&mut self`; to send and receive on two threads at once,
/// [`split`](Connection::split) or [`into_split`](Connection::into_split) the connection into its [`Sender`] and
/// [`Receiver`].
///
/// Dropping the connection ends it: the peer gets [`Error::Disconnected`] once it has received the messages this side
/// completed. After a split, that happens when both halves are dropped.
pub struct Connection {
    tx: Sender,
    rx: Receiver,
}

/// The sending half of a [`Connection`]: [`send`](Sender::send), [`send_many`](Sender::send_many) and
/// [`send_acquire`](Sender::send_acquire). The connection closes when both halves are dropped.
pub struct Sender {
    inner: Arc<ConnInner>,
}

/// The receiving half of a [`Connection`]: [`receive`](Receiver::receive), [`receive_into`](Receiver::receive_into),
/// [`receive_many`](Receiver::receive_many) and [`receive_acquire`](Receiver::receive_acquire). The connection closes
/// when both halves are dropped.
pub struct Receiver {
    inner: Arc<ConnInner>,
}

/// The native connection; dropping it closes it, once no half (and so no call) holds it.
pub(crate) struct ConnInner {
    ptr: NonNull<sys::fipc_conn_t>,
    name: Box<str>,
    max_piece: usize,
    kind: Kind,
    // A service's client keeps count, oldest first, of the requests whose replies it hasn't received yet, so that call
    // never returns the reply to another request: first those whose call gave up on its reply (it timed out or was
    // cancelled), which the next call receives and drops (at most one, as a call first drops what is owed), then those
    // sent with send or a slot's commit, whose replies are the caller's to receive. A request is counted before its
    // send, since a thread that receives may take its reply before the send returns, and uncounted if the send fails;
    // a reply or FIPC_ABANDONED takes the oldest off. The sending half adds to sends_owed while the receiving half
    // takes off, hence the atomics; calls_owed is call's (which holds both halves) and the receiving half's.
    calls_owed: AtomicU32,
    sends_owed: AtomicU32,
    // Set by a cancel (any thread) before the native cancel: a service client's call then sends nothing, as it could
    // never receive the reply; one that doesn't see it sent its request before the cancel.
    cancelled: AtomicBool,
}

// SAFETY: the C API lets a connection be used from any thread, one thread at a time per side: the Sender and Receiver
// methods that send or receive take &mut self, and there is one of each per connection. fipc_cancel, fipc_max_piece
// and fipc_broadcast_wait_for_members may run on any thread at any time before the close, which runs in Drop, once no
// reference (and so no call) is left.
unsafe impl Send for ConnInner {}
// SAFETY: as above: through a shared reference, only the cancel, the wait for members, and the name and max_piece
// (read once) are reachable.
unsafe impl Sync for ConnInner {}

impl ConnInner {
    fn conn(&self) -> *mut sys::fipc_conn_t {
        self.ptr.as_ptr()
    }

    pub(crate) fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release); // first: a call that doesn't see it sent before the cancel
        // SAFETY: the connection is open while self lives; fipc_cancel may run on any thread.
        unsafe { sys::fipc_cancel(self.conn()) }
    }

    fn wait_for_members(&self, at_least: u32, timeout: Option<Duration>) -> std::result::Result<u32, MembersError> {
        let mut count = 0;
        // SAFETY: the connection is open while self lives (the close waits for every reference, this call's
        // included); fipc_broadcast_wait_for_members may run on any thread; count is valid for a write.
        let code = unsafe { sys::fipc_broadcast_wait_for_members(self.conn(), at_least, &mut count, millis(timeout)) };
        MembersError::check(code, count)
    }

    /// A service's client is about to send a request: its reply is owed from before the send.
    fn count_request(&self) {
        if self.kind == Kind::Client {
            self.sends_owed.fetch_add(1, Ordering::AcqRel);
        }
    }

    /// The send of a request counted with `count_request` returned `code`: unless it went, nothing is owed to it.
    fn request_sent(&self, code: sys::fipc_result_t) {
        if self.kind == Kind::Client && code != sys::FIPC_OK {
            let mut owed = self.sends_owed.load(Ordering::Acquire);
            while owed > 0 {
                match self.sends_owed.compare_exchange_weak(owed, owed - 1, Ordering::AcqRel, Ordering::Acquire) {
                    Ok(_) => break,
                    Err(now) => owed = now,
                }
            }
        }
    }

    /// A service's client is about to send a batch of `count` requests: their replies are owed from before the send.
    fn count_requests(&self, count: usize) {
        if self.kind == Kind::Client {
            self.sends_owed.fetch_add(count as u32, Ordering::AcqRel);
        }
    }

    /// `unsent` of the requests counted with `count_requests` didn't go: nothing is owed to them.
    fn requests_unsent(&self, unsent: usize) {
        if self.kind != Kind::Client || unsent == 0 {
            return;
        }
        let mut owed = self.sends_owed.load(Ordering::Acquire);
        loop {
            let left = owed.saturating_sub(unsent as u32);
            match self.sends_owed.compare_exchange_weak(owed, left, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => break,
                Err(now) => owed = now,
            }
        }
    }

    /// A service client's receive returned `code`: a reply or FIPC_ABANDONED answers the oldest request owed one.
    fn answered(&self, code: sys::fipc_result_t) {
        if self.kind != Kind::Client || (code != sys::FIPC_OK && code != sys::FIPC_ABANDONED) {
            return;
        }
        if self.calls_owed.load(Ordering::Relaxed) > 0 {
            self.calls_owed.fetch_sub(1, Ordering::Relaxed);
        } else {
            let mut owed = self.sends_owed.load(Ordering::Acquire);
            while owed > 0 {
                match self.sends_owed.compare_exchange_weak(owed, owed - 1, Ordering::AcqRel, Ordering::Acquire) {
                    Ok(_) => break,
                    Err(now) => owed = now,
                }
            }
        }
    }
}

impl Drop for ConnInner {
    fn drop(&mut self) {
        // SAFETY: the last reference: no other thread is inside a call on the connection, and every slot (which
        // borrows a half) is gone.
        unsafe { sys::fipc_close(self.conn()) }
    }
}

impl Connection {
    /// Wraps a native connection that nothing else owns.
    ///
    /// # Safety
    /// `ptr` is an open connection, owned by the returned value from now on; `kind` says what it is (a gated
    /// subscriber's, a service's client or server, or another).
    pub(crate) unsafe fn from_raw(ptr: NonNull<sys::fipc_conn_t>, name: &str, kind: Kind) -> Connection {
        // SAFETY: ptr is open.
        let max_piece = unsafe { sys::fipc_max_piece(ptr.as_ptr()) };
        let inner = Arc::new(ConnInner {
            ptr,
            name: name.into(),
            max_piece,
            kind,
            calls_owed: AtomicU32::new(0),
            sends_owed: AtomicU32::new(0),
            cancelled: AtomicBool::new(false),
        });
        Connection { tx: Sender { inner: Arc::clone(&inner) }, rx: Receiver { inner } }
    }

    /// The name of the stream, broadcast, queue or service this connection belongs to.
    pub fn name(&self) -> &str {
        &self.tx.inner.name
    }

    /// The longest message the zero-copy calls take: the connection's capacity less 64 bytes. The copying calls take
    /// messages of any size. On a broadcast or a work queue, the longest message of any call: a quarter of its
    /// capacity; on a service, the longest request or reply: its `max_message`.
    pub fn max_piece(&self) -> usize {
        self.tx.inner.max_piece
    }

    /// A broadcast's publisher: waits up to `timeout` until at least `at_least` subscribers are joined, and returns
    /// how many are. Joined: admitted, and not yet left, dropped or unsubscribed; the subscribers the publisher's sends
    /// reach. To send nothing before N subscribers are there, call it with N before the first send. `at_least` 0
    /// returns the count at once, and [`NO_WAIT`](crate::NO_WAIT) polls. A subscriber that leaves, or whose process
    /// ends, is counted until the publisher's process has seen its end, soon after.
    ///
    /// It takes `&self`, and any thread may call it: while one thread waits here, another may
    /// [`cancel`](Connection::cancel) the publisher. After a [`split`](Connection::split), either half has it.
    ///
    /// ```
    /// # use fipc::SubscriberRole;
    /// # use std::time::Duration;
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut publisher = fipc::broadcast::create("fipc_doc_wait_for_members", 4096, 8, None)?;
    /// assert_eq!(publisher.wait_for_members(0, None)?, 0); // the count, at once
    /// # let subscriber = std::thread::spawn(|| {
    /// #     fipc::broadcast::join("fipc_doc_wait_for_members", SubscriberRole::Gated, Duration::from_secs(5))
    /// # });
    /// // ... a subscriber joins
    /// assert_eq!(publisher.wait_for_members(1, Duration::from_secs(5))?, 1); // waits for the first one
    /// publisher.send(b"hello", None)?;
    /// # assert_eq!(subscriber.join().unwrap()?.receive(None)?, b"hello");
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// A [`MembersError`] with the error and the count at that moment: [`Error::Timeout`] if fewer than `at_least`
    /// joined in time; [`Error::Cancelled`] once the publisher is cancelled; [`Error::Invalid`] (count 0) on a
    /// connection other than a publisher, or for `at_least` over the broadcast's `max_subscribers`.
    pub fn wait_for_members(
        &self,
        at_least: u32,
        timeout: impl Into<Option<Duration>>,
    ) -> std::result::Result<u32, MembersError> {
        self.tx.inner.wait_for_members(at_least, timeout.into())
    }

    /// Makes every call of this connection that waits, now or later, return [`Error::Cancelled`]; calls that needn't
    /// wait still work. A message cancelled halfway is dropped whole. The connection stays up: the peer sees nothing
    /// until it is dropped. Final. A service client's [`call`](Connection::call), which waits for its reply, returns
    /// [`Error::Cancelled`] at once from then on, sending nothing. To cancel from another thread, while this one waits,
    /// use a [`canceler`](Connection::canceler).
    pub fn cancel(&self) {
        self.tx.inner.cancel();
    }

    /// A handle that cancels this connection from any thread, as [`cancel`](Connection::cancel) does: the way to stop
    /// a thread that waits on it.
    ///
    /// ```
    /// use fipc::Error;
    ///
    /// # fn main() -> fipc::Result<()> {
    /// # let mut listener = fipc::stream::create("fipc_doc_cancel", 1 << 16)?;
    /// # let client = std::thread::spawn(|| fipc::stream::connect("fipc_doc_cancel", None));
    /// # let _server = listener.accept(None)?;
    /// # let mut conn = client.join().unwrap()?;
    /// let canceler = conn.canceler();
    /// let reader = std::thread::spawn(move || conn.receive(None)); // waits: nothing is sent
    /// canceler.cancel();
    /// assert_eq!(reader.join().unwrap(), Err(Error::Cancelled));
    /// # Ok(())
    /// # }
    /// ```
    pub fn canceler(&self) -> Canceler {
        self.tx.canceler()
    }

    /// The sending and receiving halves, borrowed: one thread may send while another receives, for example in
    /// [`std::thread::scope`].
    ///
    /// ```
    /// # fn main() -> fipc::Result<()> {
    /// let mut listener = fipc::stream::create("fipc_doc_split", 1 << 16)?;
    /// let client = std::thread::spawn(|| fipc::stream::connect("fipc_doc_split", None)); // connect waits for accept
    /// let mut server = listener.accept(None)?;
    /// let mut client = client.join().unwrap()?;
    /// let (tx, rx) = server.split();
    /// std::thread::scope(|s| {
    ///     s.spawn(|| tx.send(b"from the server", None));
    ///     s.spawn(|| rx.receive(None));
    ///     client.send(b"from the client", None)?;
    ///     client.receive(None).map(|_| ())
    /// })?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn split(&mut self) -> (&mut Sender, &mut Receiver) {
        (&mut self.tx, &mut self.rx)
    }

    /// The sending and receiving halves, owned: each can move to a thread of its own. The connection closes when
    /// both are dropped. A service's server leaves the service then too: a request it holds is answered
    /// [`Error::Abandoned`] only once both halves are dropped, so a half kept after the other was dropped keeps the
    /// request held, and its client waiting for the reply.
    pub fn into_split(self) -> (Sender, Receiver) {
        (self.tx, self.rx)
    }

    /// The native connection, for calls through [`sys`](crate::sys). It stays valid while this connection lives; the
    /// connection's rules apply (one thread at a time per side; don't close it).
    pub fn as_raw(&self) -> *mut sys::fipc_conn_t {
        self.tx.inner.conn()
    }

    // === Sending ===

    /// Sends one message (at least 1 byte, any size): waits up to `timeout` for room for its first piece, then copies
    /// it in, in pieces if it is longer than one piece of the ring ([`max_piece`](Connection::max_piece)). Once a
    /// piece has moved, the call goes on until the whole message has, past its timeout if the receiver is slow to
    /// make room; only a cancel or the peer's end stops it, and the message is then dropped whole.
    ///
    /// # Errors
    ///
    /// [`Error::Timeout`] if there was no room for the first piece in time; [`Error::Disconnected`];
    /// [`Error::Cancelled`]; [`Error::Invalid`] for an empty message. On a broadcast's publisher and a work
    /// queue's producer: [`Error::TooLarge`] over one piece ([`max_piece`](Connection::max_piece)); on a subscriber or
    /// a consumer: [`Error::Invalid`]. On a service, a client's send is a request, which waits for room in its ring
    /// (its replies not yet received hold room there too), and a server's is the reply to the request it holds, which
    /// never waits: [`Error::TooLarge`] over [`max_piece`](Connection::max_piece) on both, [`Error::Invalid`] on a
    /// server that holds no request, [`Error::Disconnected`] once the service has ended.
    pub fn send(&mut self, message: &[u8], timeout: impl Into<Option<Duration>>) -> Result<()> {
        self.tx.send(message, timeout)
    }

    /// Zero-copy send, step 1: waits up to `timeout` for `len` (at least 1, up to
    /// [`max_piece`](Connection::max_piece)) contiguous bytes in the ring and returns them as a [`SendSlot`] (16-byte
    /// aligned). Write the message into it, then [`commit`](SendSlot::commit) it. Dropping the slot without committing
    /// sends nothing.
    ///
    /// The room is contiguous: a slot that doesn't fit before the ring's end waits until the receiver has read past
    /// the end, even in an empty ring: a long slot (near `max_piece`) can wait for the peer's next receive call.
    ///
    /// ```
    /// # fn main() -> fipc::Result<()> {
    /// # let mut listener = fipc::stream::create("fipc_doc_send_acquire", 1 << 16)?;
    /// # let client = std::thread::spawn(|| fipc::stream::connect("fipc_doc_send_acquire", None));
    /// # let mut server = listener.accept(None)?;
    /// # let mut conn = client.join().unwrap()?;
    /// let mut slot = conn.send_acquire(64, None)?; // room for up to 64 bytes
    /// slot[..5].copy_from_slice(b"hello");
    /// slot.commit(5)?;                              // sends the first 5 as one message
    /// # assert_eq!(server.receive(None)?, b"hello");
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// [`Error::TooLarge`] over [`max_piece`](Connection::max_piece) (send such a message with
    /// [`send`](Connection::send)); [`Error::Timeout`]; [`Error::Disconnected`]; [`Error::Cancelled`];
    /// [`Error::Invalid`] for a length of 0, or on a service's server that holds no request.
    pub fn send_acquire(&mut self, len: usize, timeout: impl Into<Option<Duration>>) -> Result<SendSlot<'_>> {
        self.tx.send_acquire(len, timeout)
    }

    /// Sends `messages` (at least one, each of at least 1 byte) in order in one native call and publishes them
    /// together, with at most one wake of the receivers for the batch, and returns how many went: all of them.
    /// `timeout` bounds the whole call ([`NO_WAIT`](crate::NO_WAIT): only what fits now). Every message is checked
    /// first, before any goes. A service's client sends a batch of requests, whose replies come in their order and are
    /// owed as [`send`](Connection::send)'s are. Batch both ends: a receiver that takes one message per call falls
    /// behind a batching sender, and the two run slower than without batches.
    ///
    /// ```
    /// # fn main() -> fipc::Result<()> {
    /// # let mut listener = fipc::stream::create("fipc_doc_send_many", 1 << 16)?;
    /// # let client = std::thread::spawn(|| fipc::stream::connect("fipc_doc_send_many", None));
    /// # let mut server = listener.accept(None)?;
    /// # let mut conn = client.join().unwrap()?;
    /// let batch: [&[u8]; 3] = [b"one", b"two", b"three"];
    /// assert_eq!(conn.send_many(&batch, None)?, 3);
    /// # let mut buf = [0; 64];
    /// # let mut lens = [0; 8];
    /// # assert_eq!(server.receive_many(&mut buf, &mut lens, None)?, 3);
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// A [`SendManyError`] with the error of the first message that didn't go, as [`send`](Connection::send) would
    /// return it, and how many went (all those before it): [`Error::Timeout`], [`Error::Cancelled`],
    /// [`Error::Disconnected`]; [`Error::Invalid`] for an empty message or an empty batch, on a subscriber, a consumer
    /// or a service's server (which replies with [`send`](Connection::send)); [`Error::TooLarge`] over
    /// [`max_piece`](Connection::max_piece) on a broadcast, a work queue or a service (a stream sends a longer
    /// message in pieces).
    pub fn send_many<M: AsRef<[u8]>>(
        &mut self,
        messages: &[M],
        timeout: impl Into<Option<Duration>>,
    ) -> std::result::Result<usize, SendManyError> {
        self.tx.send_many(messages, timeout)
    }

    // === Receiving ===

    /// Receives one message of any size into a new `Vec`: waits up to `timeout` for its first piece. Once a piece has
    /// arrived, the call goes on until the whole message has, past its timeout if the sender is slow to send the
    /// rest; only a cancel or the peer's end stops it, and the message is then dropped whole.
    ///
    /// # Errors
    ///
    /// [`Error::Timeout`] if no message began in time; [`Error::Disconnected`] once every message the peer completed
    /// has been received; [`Error::Cancelled`]. On a broadcast: [`Error::Overrun`] (a lossy subscriber
    /// lost messages); [`Error::Unsubscribed`] (a gated one was dropped); [`Error::Invalid`] on the publisher. On a work
    /// queue: [`Error::Disconnected`] once the queue has ended and every message left in it was received;
    /// [`Error::Invalid`] on a producer. On a service: a client receives the reply to its oldest request whose reply
    /// it hasn't received yet, or [`Error::Abandoned`] if that request's server ended before it replied (the request
    /// counts as answered), and [`Error::Disconnected`] once the service has ended and the replies that came before
    /// were received (after a [`call`](Connection::call) that gave up, that call's late reply comes first); a server
    /// receives the next request of any client and holds it until its next send, the reply:
    /// [`Error::Invalid`] while it holds one, [`Error::Disconnected`] once the service has ended.
    pub fn receive(&mut self, timeout: impl Into<Option<Duration>>) -> Result<Vec<u8>> {
        self.rx.receive(timeout)
    }

    /// Receives one message into `buf` and returns its length, as [`receive`](Connection::receive) does, without
    /// allocating.
    ///
    /// ```
    /// # use fipc::Error;
    /// # fn main() -> fipc::Result<()> {
    /// # let mut listener = fipc::stream::create("fipc_doc_receive_into", 1 << 16)?;
    /// # let client = std::thread::spawn(|| fipc::stream::connect("fipc_doc_receive_into", None));
    /// # let mut server = listener.accept(None)?;
    /// # let mut conn = client.join().unwrap()?;
    /// # server.send(&[7; 300], None)?;
    /// let mut buf = vec![0; 256];
    /// let len = match conn.receive_into(&mut buf, None) {
    ///     Err(Error::TooLarge { len }) => {
    ///         buf.resize(len, 0);                 // the message stays queued: make room, then again
    ///         conn.receive_into(&mut buf, None)?
    ///     }
    ///     other => other?,
    /// };
    /// assert_eq!(len, 300);
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// As [`receive`](Connection::receive), and [`Error::TooLarge`] if the message is longer than `buf`: nothing is
    /// taken, and `len` is the room it needs (on a work queue another consumer may take that message first, so the
    /// next receive may find another; a buffer of [`max_piece`](Connection::max_piece) bytes takes every message).
    /// After an error, the contents of `buf` are unspecified.
    pub fn receive_into(&mut self, buf: &mut [u8], timeout: impl Into<Option<Duration>>) -> Result<usize> {
        self.rx.receive_into(buf, timeout)
    }

    /// Waits up to `timeout` for the next message, as [`receive`](Connection::receive), then takes every further
    /// message already there that fits the rest of `buf`, up to `lens.len()` (at least 1), in one native call, packed
    /// back to back: message i is `lens[i]` bytes, at the sum of the lengths before it
    /// ([`batch_messages`](crate::batch_messages) walks them). Returns how many it took. After the first it never
    /// waits, and stops before a message that isn't there yet, doesn't fit, is in pieces, or would bring another
    /// result: the next receive returns it. A broadcast's subscriber takes, after its first, only the messages its
    /// publisher has moved past (the newest come in the next calls); a work queue's consumer acknowledges each message
    /// before the call returns; a service's client receives its replies in its requests' order.
    ///
    /// ```
    /// # fn main() -> fipc::Result<()> {
    /// # let mut listener = fipc::stream::create("fipc_doc_receive_many", 1 << 16)?;
    /// # let client = std::thread::spawn(|| fipc::stream::connect("fipc_doc_receive_many", None));
    /// # let mut server = listener.accept(None)?;
    /// # let mut conn = client.join().unwrap()?;
    /// # server.send_many(&[&b"one"[..], b"two"], None)?;
    /// let mut buf = vec![0; 1 << 16];
    /// let mut lens = [0; 64];
    /// let count = conn.receive_many(&mut buf, &mut lens, None)?;
    /// for message in fipc::batch_messages(&buf, &lens[..count]) {
    ///     println!("{message:?}");
    /// }
    /// # assert_eq!(count, 2);
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// The first message's error, as [`receive_into`](Connection::receive_into) returns it, with nothing else taken:
    /// [`Error::TooLarge`] if it is longer than `buf` (`len` is the room it needs; it stays queued),
    /// [`Error::Overrun`], a service client's [`Error::Abandoned`], ...; [`Error::Invalid`] for an empty `lens`, on a
    /// publisher, a producer and a service's server (which holds one request at a time). After an error, the contents
    /// of `buf` and `lens` are unspecified.
    pub fn receive_many(
        &mut self,
        buf: &mut [u8],
        lens: &mut [usize],
        timeout: impl Into<Option<Duration>>,
    ) -> Result<usize> {
        self.rx.receive_many(buf, lens, timeout)
    }

    /// Zero-copy receive: waits up to `timeout` for a message and returns it in the ring, as a [`ReceiveSlot`]
    /// (16-byte aligned, read-only). Its room stays taken until the slot is dropped (or
    /// [`released`](ReceiveSlot::release)).
    ///
    /// A broadcast's gated subscriber receives in place with
    /// [`receive_acquire_gated`](Connection::receive_acquire_gated) instead, which is unsafe.
    ///
    /// ```
    /// # fn main() -> fipc::Result<()> {
    /// # let mut listener = fipc::stream::create("fipc_doc_receive_acquire", 1 << 16)?;
    /// # let client = std::thread::spawn(|| fipc::stream::connect("fipc_doc_receive_acquire", None));
    /// # let mut server = listener.accept(None)?;
    /// # let mut conn = client.join().unwrap()?;
    /// # server.send(b"hello", None)?;
    /// let message = conn.receive_acquire(None)?;
    /// assert_eq!(&*message, b"hello");
    /// drop(message); // frees its room in the ring
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// As [`receive`](Connection::receive), and [`Error::TooLarge`] for a message in several pieces: `len` is its
    /// length, and it stays queued for [`receive`](Connection::receive) or [`receive_into`](Connection::receive_into).
    /// On a broadcast: [`Error::Invalid`] on a subscriber, lossy (it receives copies only) or gated (see
    /// [`receive_acquire_gated`](Connection::receive_acquire_gated)), and on the publisher. On a work queue, a consumer
    /// holds the message until the slot is dropped, which acknowledges it (one whose process ends before has it go to
    /// another consumer); a producer gets [`Error::Invalid`]. On a service, a client's slot is a reply, and a server's
    /// a request, which the server holds until its reply: the slot borrows the whole connection, so the reply can't
    /// be sent while it lives (drop it first, or use [`receive_request`](Connection::receive_request), whose reply
    /// consumes the request); a server's split [`Receiver`] refuses it.
    pub fn receive_acquire(&mut self, timeout: impl Into<Option<Duration>>) -> Result<ReceiveSlot<'_>> {
        if self.rx.inner.kind == Kind::Gated {
            return Err(Error::Invalid);
        }
        let (data, len) = self.rx.acquire(timeout.into())?;
        Ok(ReceiveSlot { receiver: &mut self.rx, data, len })
    }

    /// Zero-copy receive on a gated subscriber of a broadcast (and on any connection, as
    /// [`receive_acquire`](Connection::receive_acquire)): returns the message in the broadcast's memory, as a
    /// [`ReceiveSlot`]. The slot holds the publisher back until it is dropped.
    ///
    /// A subscriber that holds the publisher back for the broadcast's `max_stall` is unsubscribed, and its own process
    /// then makes the broadcast's memory inaccessible, the slot's bytes included, at a moment the publisher's clock
    /// picks: reading the slot after that is undefined behavior (in practice, an access violation). No borrow can rule
    /// it out, hence `unsafe`.
    ///
    /// # Safety
    ///
    /// The subscriber is never unsubscribed while the slot lives: the publisher created the broadcast with a
    /// `max_stall` of `None` (which never unsubscribes), or the slot is dropped before it has held the publisher back
    /// for the broadcast's `max_stall`.
    ///
    /// # Errors
    ///
    /// As [`receive_acquire`](Connection::receive_acquire), without its [`Error::Invalid`] on a gated subscriber;
    /// [`Error::Disconnected`] on one whose publisher's process ended (receive the rest copied).
    pub unsafe fn receive_acquire_gated(&mut self, timeout: impl Into<Option<Duration>>) -> Result<ReceiveSlot<'_>> {
        // A gated subscriber's bytes stay valid while it isn't unsubscribed: the caller's contract
        let (data, len) = self.rx.acquire(timeout.into())?;
        Ok(ReceiveSlot { receiver: &mut self.rx, data, len })
    }

    // === A service: a client's request and its reply, a server's request held until its reply ===

    /// A service's client: sends `request` and returns its reply, waiting up to `timeout` in all. A call checks its
    /// request first: [`Error::Invalid`] for an empty one, [`Error::TooLarge`] over
    /// [`max_piece`](Connection::max_piece), at once, even while a reply is owed. Then a call on a cancelled
    /// connection returns [`Error::Cancelled`] at once, whatever its timeout, and sends nothing: it could never receive
    /// the reply. A call never returns the reply to another request: the connection keeps count of the replies still
    /// owed, and a call that timed out or was cancelled waiting for its reply leaves that reply owed, which the next
    /// call first receives and drops (at most one is owed, since every call drops it first; an [`Error::Abandoned`] in
    /// its place counts too), within its own timeout and before it sends: if the reply doesn't come in time, the
    /// call's request is never sent. A call that had to drop an owed reply first sends its request only if its timeout
    /// has time left: once a positive timeout has run out, it returns [`Error::Timeout`] without sending, so no server
    /// sees that request; with a zero timeout no step waits, and it sends if there is room. After a call that gave up,
    /// a plain
    /// [`receive`](Connection::receive) returns that call's late reply (or [`Error::Abandoned`]) first: replies come
    /// in request order. It takes `&mut self`, as it sends and receives.
    ///
    /// ```
    /// use fipc::ServiceRole;
    /// use std::time::Duration;
    ///
    /// # fn main() -> fipc::Result<()> {
    /// let service = fipc::service::create("fipc_doc_call", 4096, 256, 1, 1)?;
    /// let mut client = fipc::service::join("fipc_doc_call", ServiceRole::Client, Duration::from_secs(5))?;
    /// let mut server = fipc::service::join("fipc_doc_call", ServiceRole::Server, Duration::from_secs(5))?;
    /// let answering = std::thread::spawn(move || {
    ///     let request = server.receive(None)?;
    ///     server.send(&request.to_ascii_uppercase(), None)
    /// });
    /// assert_eq!(client.call(b"ping", Duration::from_secs(5))?, b"PING");
    /// answering.join().unwrap()?;
    /// # drop(service);
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// [`Error::Abandoned`] if the server that took the request ended before it replied (the request counts as
    /// answered; it may or may not have been carried out, and it is never sent again); [`Error::Timeout`] and
    /// [`Error::Cancelled`] (after a cancel, at once, sending nothing; else the request's reply, if it was sent, stays
    /// owed); [`Error::Disconnected`] once the
    /// service has ended; [`Error::TooLarge`] over [`max_piece`](Connection::max_piece); [`Error::Invalid`] for an
    /// empty request, while a request sent with [`send`](Connection::send) or a slot's
    /// [`commit`](SendSlot::commit) still has its reply to be received (that reply is the caller's: receive it first),
    /// or on any connection other than a service's client.
    pub fn call(&mut self, request: &[u8], timeout: impl Into<Option<Duration>>) -> Result<Vec<u8>> {
        let timeout = timeout.into();
        let deadline = timeout.and_then(|timeout| Instant::now().checked_add(timeout)); // None: for ever
        let left = || deadline.map(|deadline| deadline.saturating_duration_since(Instant::now()));
        let inner = &self.tx.inner;
        if inner.kind != Kind::Client {
            return Err(Error::Invalid);
        }
        if request.is_empty() {
            return Err(Error::Invalid);
        }
        if request.len() > inner.max_piece {
            return Err(Error::TooLarge { len: request.len() });
        }
        if inner.cancelled.load(Ordering::Acquire) {
            // Its reply could never be received: the request never goes
            return Err(Error::Cancelled);
        }
        if inner.sends_owed.load(Ordering::Acquire) != 0 {
            // A reply to a request of send's is the caller's, not the call's to drop
            return Err(Error::Invalid);
        }
        let mut dropped = false;
        while inner.calls_owed.load(Ordering::Relaxed) > 0 {
            // A reply owed to a call that gave up on it: dropped (the receive takes it off the count)
            match self.rx.receive(left()) {
                Ok(_) | Err(Error::Abandoned) => dropped = true,
                Err(e) => return Err(e),
            }
        }
        if dropped && timeout.is_some_and(|timeout| !timeout.is_zero()) && left() == Some(Duration::ZERO) {
            // Its timeout ran out while it dropped that reply: the request isn't sent, so no server sees it
            return Err(Error::Timeout);
        }
        if inner.cancelled.load(Ordering::Acquire) {
            return Err(Error::Cancelled); // cancelled while it dropped: the request never goes
        }
        inner.calls_owed.store(1, Ordering::Relaxed); // its reply is owed from before the send
        // SAFETY: &mut self makes this the one thread sending; request is valid for its length.
        let code = unsafe { sys::fipc_send(inner.conn(), request.as_ptr().cast(), request.len(), millis(left())) };
        if code != sys::FIPC_OK {
            inner.calls_owed.store(0, Ordering::Relaxed);
            return Err(Error::from_code(code, request.len()));
        }
        self.rx.receive(left()) // a reply or Abandoned takes it off the count; else it stays owed
    }

    /// A service's server: waits up to `timeout` for the next request of any client and returns it in place, as a
    /// [`Request`] (16-byte aligned, read-only), which the server holds until it replies with
    /// [`Request::reply`] or [`Request::reply_in_place`]. The reply consumes the request, and the request borrows the
    /// whole connection, so the borrow checker keeps its bytes from being read after the reply (from then on they
    /// are its client's again). The request may move to another thread for its reply.
    ///
    /// ```
    /// use fipc::ServiceRole;
    /// use std::time::Duration;
    ///
    /// # fn main() -> fipc::Result<()> {
    /// let service = fipc::service::create("fipc_doc_receive_request", 4096, 256, 1, 1)?;
    /// let mut server = fipc::service::join("fipc_doc_receive_request", ServiceRole::Server, Duration::from_secs(5))?;
    /// let mut client = fipc::service::join("fipc_doc_receive_request", ServiceRole::Client, Duration::from_secs(5))?;
    /// client.send(b"ping", None)?;
    /// let request = server.receive_request(None)?; // held in place until the reply
    /// assert_eq!(&*request, b"ping");
    /// let len = request.len();
    /// request.reply_in_place(len, |request, reply| {
    ///     reply.copy_from_slice(request); // the reply's room, apart from the request: zero-copy both ways
    ///     reply.make_ascii_uppercase();
    ///     len
    /// })?;
    /// assert_eq!(client.receive(None)?, b"PING");
    /// # drop(service);
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// A request's bytes can't be read after its reply:
    ///
    /// ```compile_fail
    /// # fn main() -> fipc::Result<()> {
    /// # let mut server = fipc::service::join("fipc_doc_after_the_reply", fipc::ServiceRole::Server, None)?;
    /// let request = server.receive_request(None)?;
    /// let bytes: &[u8] = &request;
    /// request.reply(b"done")?; // moves the request, which bytes borrows
    /// println!("{bytes:?}");
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// nor can the connection send while the request lives:
    ///
    /// ```compile_fail
    /// # fn main() -> fipc::Result<()> {
    /// # let mut server = fipc::service::join("fipc_doc_send_while_held", fipc::ServiceRole::Server, None)?;
    /// let request = server.receive_request(None)?;
    /// server.send(b"done", None)?; // the request borrows the connection
    /// println!("{:?}", &*request);
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// [`Error::Timeout`] if no request came in time; [`Error::Cancelled`]; [`Error::Disconnected`] once the service
    /// has ended; [`Error::Invalid`] while the server holds a request (one it received and didn't reply to), or on
    /// any connection other than a service's server.
    pub fn receive_request(&mut self, timeout: impl Into<Option<Duration>>) -> Result<Request<'_>> {
        if self.rx.inner.kind != Kind::Server {
            return Err(Error::Invalid);
        }
        let (data, len) = self.rx.acquire(timeout.into())?;
        Ok(Request { conn: self, data, len })
    }
}

impl Sender {
    /// See [`Connection::name`].
    pub fn name(&self) -> &str {
        &self.inner.name
    }

    /// See [`Connection::max_piece`].
    pub fn max_piece(&self) -> usize {
        self.inner.max_piece
    }

    /// Cancels the whole connection, both halves; see [`Connection::cancel`].
    pub fn cancel(&self) {
        self.inner.cancel();
    }

    /// A handle that cancels the whole connection from any thread; see [`Connection::canceler`].
    pub fn canceler(&self) -> Canceler {
        Canceler { target: Target::Connection(Arc::downgrade(&self.inner)) }
    }

    /// A publisher's wait for subscribers; see [`Connection::wait_for_members`].
    pub fn wait_for_members(
        &self,
        at_least: u32,
        timeout: impl Into<Option<Duration>>,
    ) -> std::result::Result<u32, MembersError> {
        self.inner.wait_for_members(at_least, timeout.into())
    }

    /// See [`Connection::send`].
    pub fn send(&mut self, message: &[u8], timeout: impl Into<Option<Duration>>) -> Result<()> {
        self.inner.count_request();
        // SAFETY: &mut self makes this the one thread sending; message is valid for its length.
        let code =
            unsafe { sys::fipc_send(self.inner.conn(), message.as_ptr().cast(), message.len(), millis(timeout)) };
        self.inner.request_sent(code);
        check(code, message.len())
    }

    /// See [`Connection::send_many`].
    pub fn send_many<M: AsRef<[u8]>>(
        &mut self,
        messages: &[M],
        timeout: impl Into<Option<Duration>>,
    ) -> std::result::Result<usize, SendManyError> {
        // Each message's address and length, on the stack for a batch of up to STACK messages
        const STACK: usize = 64;
        let count = messages.len();
        let mut stack_datas = [ptr::null::<c_void>(); STACK];
        let mut stack_lens = [0usize; STACK];
        let (mut heap_datas, mut heap_lens) = (Vec::new(), Vec::new());
        let (datas, lens) = if count <= STACK {
            (&mut stack_datas[..count], &mut stack_lens[..count])
        } else {
            heap_datas.resize(count, ptr::null());
            heap_lens.resize(count, 0);
            (&mut heap_datas[..], &mut heap_lens[..])
        };
        for (i, message) in messages.iter().enumerate() {
            let message = message.as_ref();
            datas[i] = message.as_ptr().cast();
            lens[i] = message.len();
        }
        self.inner.count_requests(count);
        let mut sent = 0;
        // SAFETY: &mut self makes this the one thread sending; datas and lens hold count entries, each datas[i]
        // valid for lens[i] bytes (the messages, borrowed for the call); sent is valid for a write.
        let code = unsafe {
            sys::fipc_send_many(self.inner.conn(), datas.as_ptr(), lens.as_ptr(), count, &mut sent, millis(timeout))
        };
        self.inner.requests_unsent(count - sent);
        if code == sys::FIPC_OK {
            return Ok(sent);
        }
        // TooLarge's length: the first message over one piece (the batch is checked before any goes)
        let len = lens.iter().copied().find(|&len| len > self.inner.max_piece).unwrap_or(0);
        Err(SendManyError { error: Error::from_code(code, len), sent })
    }

    /// See [`Connection::send_acquire`].
    pub fn send_acquire(&mut self, len: usize, timeout: impl Into<Option<Duration>>) -> Result<SendSlot<'_>> {
        let mut out = ptr::null_mut();
        // SAFETY: &mut self makes this the one thread sending; out is valid for a write.
        check(unsafe { sys::fipc_send_acquire(self.inner.conn(), len, &mut out, millis(timeout)) }, len)?;
        let ptr = NonNull::new(out.cast::<u8>()).expect("fipc_send_acquire returned FIPC_OK without room");
        Ok(SendSlot { sender: self, ptr, len })
    }
}

impl Receiver {
    /// See [`Connection::name`].
    pub fn name(&self) -> &str {
        &self.inner.name
    }

    /// See [`Connection::max_piece`].
    pub fn max_piece(&self) -> usize {
        self.inner.max_piece
    }

    /// Cancels the whole connection, both halves; see [`Connection::cancel`].
    pub fn cancel(&self) {
        self.inner.cancel();
    }

    /// A handle that cancels the whole connection from any thread; see [`Connection::canceler`].
    pub fn canceler(&self) -> Canceler {
        Canceler { target: Target::Connection(Arc::downgrade(&self.inner)) }
    }

    /// A publisher's wait for subscribers; see [`Connection::wait_for_members`].
    pub fn wait_for_members(
        &self,
        at_least: u32,
        timeout: impl Into<Option<Duration>>,
    ) -> std::result::Result<u32, MembersError> {
        self.inner.wait_for_members(at_least, timeout.into())
    }

    /// One fipc_recv into `buf` (`cap` bytes, which may be uninitialized): the result code and the length
    /// out-parameter (the message's length at FIPC_OK, the room it needs at FIPC_TOO_LARGE, the messages lost at
    /// FIPC_OVERRUN).
    fn recv_raw(&mut self, buf: *mut u8, cap: usize, ms: i32) -> (sys::fipc_result_t, usize) {
        let mut len = 0;
        // SAFETY: &mut self makes this the one thread receiving; buf is valid for cap bytes of writes (the callers'
        // contract); len is valid for a write.
        let code = unsafe { sys::fipc_recv(self.inner.conn(), buf.cast::<c_void>(), cap, &mut len, ms) };
        self.inner.answered(code);
        (code, len)
    }

    /// See [`Connection::receive`].
    pub fn receive(&mut self, timeout: impl Into<Option<Duration>>) -> Result<Vec<u8>> {
        // SAFETY: recv_raw is one fipc_recv, which writes the length it returns with FIPC_OK into the buffer.
        unsafe { receive_growing(timeout.into(), |buf, cap, ms| self.recv_raw(buf, cap, ms)) }
    }

    /// See [`Connection::receive_into`].
    pub fn receive_into(&mut self, buf: &mut [u8], timeout: impl Into<Option<Duration>>) -> Result<usize> {
        let (code, len) = self.recv_raw(buf.as_mut_ptr(), buf.len(), millis(timeout));
        check(code, len).map(|()| len)
    }

    /// See [`Connection::receive_many`].
    pub fn receive_many(
        &mut self,
        buf: &mut [u8],
        lens: &mut [usize],
        timeout: impl Into<Option<Duration>>,
    ) -> Result<usize> {
        let mut count = 0;
        // SAFETY: &mut self makes this the one thread receiving; buf and lens are valid for their lengths of writes;
        // count is valid for a write.
        let code = unsafe {
            sys::fipc_recv_many(
                self.inner.conn(),
                buf.as_mut_ptr().cast(),
                buf.len(),
                lens.as_mut_ptr(),
                lens.len(),
                &mut count,
                millis(timeout),
            )
        };
        if code != sys::FIPC_OK {
            self.inner.answered(code);
            return Err(Error::from_code(code, lens.first().copied().unwrap_or(0)));
        }
        for _ in 0..count {
            self.inner.answered(code);
        }
        Ok(count)
    }

    /// See [`Connection::receive_acquire`]. On a service's server, [`Error::Invalid`]: its sending half could reply
    /// while the slot lives, and the request's bytes are its client's again from the reply on (receive it copied, or
    /// in place on the whole [`Connection`]).
    pub fn receive_acquire(&mut self, timeout: impl Into<Option<Duration>>) -> Result<ReceiveSlot<'_>> {
        if self.inner.kind == Kind::Gated {
            return Err(Error::Invalid);
        }
        // SAFETY: not a gated subscriber, whose memory nothing revokes while the connection is open.
        unsafe { self.receive_acquire_gated(timeout) }
    }

    /// See [`Connection::receive_acquire_gated`]; on a service's server, [`Error::Invalid`], as
    /// [`receive_acquire`](Receiver::receive_acquire).
    ///
    /// # Safety
    ///
    /// As [`Connection::receive_acquire_gated`].
    pub unsafe fn receive_acquire_gated(&mut self, timeout: impl Into<Option<Duration>>) -> Result<ReceiveSlot<'_>> {
        if self.inner.kind == Kind::Server {
            return Err(Error::Invalid);
        }
        let (data, len) = self.acquire(timeout.into())?;
        Ok(ReceiveSlot { receiver: self, data, len })
    }

    /// One fipc_recv_acquire: the message's bytes in the ring and its length. They stay valid until the release, the
    /// next receive or the close (a gated subscriber's, also until it is unsubscribed; a service server's request,
    /// until the reply), which the callers' borrows keep.
    fn acquire(&mut self, timeout: Option<Duration>) -> Result<(NonNull<u8>, usize)> {
        let mut data = ptr::null();
        let mut len = 0;
        // SAFETY: &mut self makes this the one thread receiving; the out-pointers are valid for writes.
        let code = unsafe { sys::fipc_recv_acquire(self.inner.conn(), &mut data, &mut len, millis(timeout)) };
        self.inner.answered(code);
        check(code, len)?;
        let data = if len == 0 { NonNull::dangling() } else { NonNull::new(data.cast_mut().cast::<u8>()).unwrap() };
        Ok((data, len))
    }
}

/// Receives one message into a new `Vec` through `recv`, one native receive into a buffer of a capacity, with a
/// timeout in milliseconds. The first call, with no room, waits for a message and says its length; the next one takes
/// it. On a work queue another consumer may take that message first: the next call then finds another, maybe longer,
/// so the room grows until a message fits, and each call waits only for what is left of `timeout`.
///
/// # Safety
/// When `recv` returns `FIPC_OK` with a length, it has written that many bytes, at most the capacity, at the buffer.
unsafe fn receive_growing(
    timeout: Option<Duration>,
    mut recv: impl FnMut(*mut u8, usize, c_int) -> (sys::fipc_result_t, usize),
) -> Result<Vec<u8>> {
    let deadline = timeout.and_then(|timeout| Instant::now().checked_add(timeout)); // None: for ever
    let mut ms = millis(timeout);
    let mut buf = Vec::new();
    loop {
        let (code, len) = recv(buf.as_mut_ptr(), buf.capacity(), ms);
        match code {
            sys::FIPC_OK => {
                // SAFETY: recv wrote len bytes (at most the capacity) into the buffer (the caller's contract).
                unsafe { buf.set_len(len) };
                return Ok(buf);
            }
            sys::FIPC_TOO_LARGE => {
                buf.reserve_exact(len);
                if let Some(deadline) = deadline {
                    ms = millis(deadline.saturating_duration_since(Instant::now()));
                }
            }
            _ => return Err(Error::from_code(code, len)),
        }
    }
}

/// Room for one message in the ring, from [`Connection::send_acquire`]: write the message into it (it derefs to a
/// `[u8]` of the acquired length), then [`commit`](SendSlot::commit) it.
///
/// The slot borrows the sending side, so nothing else can be sent until it is committed or dropped. Dropping it
/// without committing abandons the message: nothing is sent, and the next send reuses the room.
#[must_use = "a SendSlot sends nothing until it is committed"]
pub struct SendSlot<'a> {
    sender: &'a mut Sender,
    ptr: NonNull<u8>,
    len: usize,
}

// SAFETY: the slot is the sending side's turn (it holds &mut Sender, which is Send); its bytes are this side's alone
// until the commit.
unsafe impl Send for SendSlot<'_> {}
// SAFETY: through a shared reference the bytes can only be read.
unsafe impl Sync for SendSlot<'_> {}

impl SendSlot<'_> {
    /// Publishes the first `len` bytes of the slot (1 to its length) as one message. Doesn't wait.
    ///
    /// # Errors
    ///
    /// [`Error::Invalid`] for a length of 0 or over the slot's; nothing is sent then.
    pub fn commit(self, len: usize) -> Result<()> {
        self.sender.inner.count_request();
        // SAFETY: the slot holds the sending side (&mut Sender); the reservation is the latest one.
        let code = unsafe { sys::fipc_send_commit(self.sender.inner.conn(), len) };
        self.sender.inner.request_sent(code);
        check(code, len)
    }
}

impl Deref for SendSlot<'_> {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        // SAFETY: fipc_send_acquire gave len bytes of the ring to this side until the commit, the next send (which
        // the borrow of the Sender rules out) or the close (ruled out as well).
        unsafe { slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }
}

impl DerefMut for SendSlot<'_> {
    fn deref_mut(&mut self) -> &mut [u8] {
        // SAFETY: as in deref; &mut self makes this the only reference to them.
        unsafe { slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }
}

impl fmt::Debug for SendSlot<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SendSlot").field("len", &self.len).finish_non_exhaustive()
    }
}

/// A message in the ring, from [`Connection::receive_acquire`]: it derefs to the message's bytes. Dropping it frees
/// the message's room in the ring.
///
/// The slot borrows the receiving side, so nothing else can be received until it is dropped. A gated subscriber's slot,
/// from the unsafe [`Connection::receive_acquire_gated`], also holds the publisher back until it is dropped.
pub struct ReceiveSlot<'a> {
    receiver: &'a mut Receiver,
    data: NonNull<u8>,
    len: usize,
}

// SAFETY: the slot is the receiving side's turn (it holds &mut Receiver, which is Send); its bytes are this side's
// alone until the release.
unsafe impl Send for ReceiveSlot<'_> {}
// SAFETY: the bytes can only be read.
unsafe impl Sync for ReceiveSlot<'_> {}

impl ReceiveSlot<'_> {
    /// Frees the message's room in the ring, as dropping the slot does.
    pub fn release(self) {}
}

impl Deref for ReceiveSlot<'_> {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        // SAFETY: fipc_recv_acquire gave len bytes of the ring to this side until the release (in Drop), the next
        // receive (which the borrow of the Receiver rules out) or the close (ruled out as well); on a gated subscriber,
        // also until it is unsubscribed, which receive_acquire_gated's caller rules out.
        unsafe { slice::from_raw_parts(self.data.as_ptr(), self.len) }
    }
}

impl Drop for ReceiveSlot<'_> {
    fn drop(&mut self) {
        // SAFETY: the slot holds the receiving side (&mut Receiver).
        unsafe { sys::fipc_recv_release(self.receiver.inner.conn()) }
    }
}

impl fmt::Debug for ReceiveSlot<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReceiveSlot").field("len", &self.len).finish_non_exhaustive()
    }
}

/// A request a service's server holds in place, from [`Connection::receive_request`]: it derefs to the request's
/// bytes (read-only, in its client's ring). The server holds it until it replies, which consumes it:
/// [`reply`](Request::reply) sends a copy, [`reply_in_place`](Request::reply_in_place) writes the reply in the ring,
/// reading the request meanwhile. A reply never waits.
///
/// The request borrows the whole connection, so nothing else can be sent or received on it while the request lives,
/// and its bytes can't be read after the reply. It is `Send`: another thread may reply (in [`std::thread::scope`], for
/// example). Dropping it without a reply leaves the request held: the connection's next [`send`](Connection::send)
/// replies to it, and its receives return [`Error::Invalid`] until then.
#[must_use = "a server holds its request until it replies"]
pub struct Request<'a> {
    conn: &'a mut Connection,
    data: NonNull<u8>,
    len: usize,
}

// SAFETY: the request is the server's turn (it holds &mut Connection, which is Send); its bytes are this side's until
// the reply.
unsafe impl Send for Request<'_> {}
// SAFETY: the bytes can only be read.
unsafe impl Sync for Request<'_> {}

impl Request<'_> {
    /// Replies to the request with a copy of `reply` (1 to [`max_piece`](Connection::max_piece) bytes), which goes to
    /// the request's client. Doesn't wait.
    ///
    /// # Errors
    ///
    /// [`Error::TooLarge`] over [`max_piece`](Connection::max_piece) and [`Error::Invalid`] for an empty reply:
    /// nothing is sent, and the request is still held, for the connection's next [`send`](Connection::send);
    /// [`Error::Disconnected`] once the service has ended (a reply that returns it just as the service ends may still
    /// have reached its client). A reply to a client that has left is dropped and returns `Ok`.
    pub fn reply(self, reply: &[u8]) -> Result<()> {
        self.conn.tx.send(reply, NO_WAIT)
    }

    /// Replies to the request in place: takes `len` bytes (1 to [`max_piece`](Connection::max_piece)) of room for the
    /// reply in the client's ring, apart from the request's, and calls `write` with the request and that room; `write`
    /// returns the reply's length (1 to `len`), and the reply goes to the request's client. Doesn't wait.
    ///
    /// # Errors
    ///
    /// As [`reply`](Request::reply), and [`Error::Invalid`] for a length `write` returns out of range, or a `len` of
    /// 0: nothing is sent, and the request is still held.
    pub fn reply_in_place(self, len: usize, write: impl FnOnce(&[u8], &mut [u8]) -> usize) -> Result<()> {
        let conn = self.conn.tx.inner.conn();
        let mut out = ptr::null_mut();
        // SAFETY: the request holds the whole connection (&mut Connection): this is the one thread sending; out is
        // valid for a write.
        check(unsafe { sys::fipc_send_acquire(conn, len, &mut out, millis(NO_WAIT)) }, len)?;
        let room = NonNull::new(out.cast::<u8>()).expect("fipc_send_acquire returned FIPC_OK without room");
        // SAFETY: the request's bytes stay valid until the reply's commit; the room is this side's alone until then,
        // len bytes long, and apart from the request's bytes (a request's reply has room of its own in the ring).
        let written = write(unsafe { slice::from_raw_parts(self.data.as_ptr(), self.len) }, unsafe {
            slice::from_raw_parts_mut(room.as_ptr(), len)
        });
        // SAFETY: as above; the reservation is the latest one.
        check(unsafe { sys::fipc_send_commit(conn, written) }, written)
    }
}

impl Deref for Request<'_> {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        // SAFETY: fipc_recv_acquire gave len bytes of the client's ring to this server until its reply, which consumes
        // the request; the request's borrow of the whole connection rules out any other send, receive or the close.
        unsafe { slice::from_raw_parts(self.data.as_ptr(), self.len) }
    }
}

impl fmt::Debug for Request<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Request").field("len", &self.len).finish_non_exhaustive()
    }
}

/// Cancels a [`Listener`](crate::Listener), a [`Connection`], a [`Queue`](crate::Queue) or a
/// [`Service`](crate::Service) from any thread: the way to stop a thread that waits in
/// [`accept`](crate::Listener::accept), a receive (or a send, for room), a [`call`](Connection::call) or a wait for
/// members. Cheap to clone, `Send` and `Sync`.
///
/// Cancelling is final: every call of the handle that waits, then or later, returns [`Error::Cancelled`]; calls that
/// needn't wait still work. A canceler doesn't keep its handle open: once the handle is dropped, cancelling does
/// nothing. To stop a thread: cancel, join the thread, drop the handle.
#[derive(Clone)]
pub struct Canceler {
    target: Target,
}

#[derive(Clone)]
enum Target {
    Listener(Weak<ListenerInner>),
    Connection(Weak<ConnInner>),
    Queue(Weak<QueueInner>),
    Service(Weak<ServiceInner>),
}

impl Canceler {
    pub(crate) fn listener(inner: Weak<ListenerInner>) -> Canceler {
        Canceler { target: Target::Listener(inner) }
    }

    pub(crate) fn queue(inner: Weak<QueueInner>) -> Canceler {
        Canceler { target: Target::Queue(inner) }
    }

    pub(crate) fn service(inner: Weak<ServiceInner>) -> Canceler {
        Canceler { target: Target::Service(inner) }
    }

    /// Cancels the listener, connection, queue or service, unless it is closed already.
    pub fn cancel(&self) {
        // The upgrade keeps the handle open for the call; if this was its last reference, the native close runs
        // here, after the cancel
        match &self.target {
            Target::Listener(weak) => {
                if let Some(inner) = weak.upgrade() {
                    inner.cancel();
                }
            }
            Target::Connection(weak) => {
                if let Some(inner) = weak.upgrade() {
                    inner.cancel();
                }
            }
            Target::Queue(weak) => {
                if let Some(inner) = weak.upgrade() {
                    inner.cancel();
                }
            }
            Target::Service(weak) => {
                if let Some(inner) = weak.upgrade() {
                    inner.cancel();
                }
            }
        }
    }
}

impl fmt::Debug for Canceler {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let what = match self.target {
            Target::Listener(_) => "listener",
            Target::Connection(_) => "connection",
            Target::Queue(_) => "queue",
            Target::Service(_) => "service",
        };
        f.debug_struct("Canceler").field("target", &what).finish()
    }
}

impl fmt::Debug for Connection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Connection")
            .field("name", &self.name())
            .field("max_piece", &self.max_piece())
            .finish_non_exhaustive()
    }
}

impl fmt::Debug for Sender {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sender").field("name", &self.name()).finish_non_exhaustive()
    }
}

impl fmt::Debug for Receiver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Receiver").field("name", &self.name()).finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn a_receive_that_grows_its_buffer_waits_only_for_what_is_left_of_its_timeout() {
        let mut calls = Vec::new();
        // A message of 100 bytes comes after 200 ms; another consumer takes it first, and the next is 300 bytes long
        // SAFETY: the fake writes the length it returns with FIPC_OK, within the capacity.
        let message = unsafe {
            receive_growing(Some(Duration::from_millis(1000)), |buf, cap, ms| {
                calls.push((cap, ms));
                match calls.len() {
                    1 => {
                        thread::sleep(Duration::from_millis(200));
                        (sys::FIPC_TOO_LARGE, 100)
                    }
                    2 => (sys::FIPC_TOO_LARGE, 300),
                    _ => {
                        assert!(cap >= 300);
                        ptr::write_bytes(buf, 7, 300);
                        (sys::FIPC_OK, 300)
                    }
                }
            })
        };
        assert_eq!(message, Ok(vec![7; 300]));
        assert_eq!(calls.len(), 3);
        assert_eq!(calls[0], (0, 1000));
        let (room, ms) = calls[1];
        assert!(room >= 100 && (0..=800).contains(&ms), "{calls:?}"); // at least the 200 ms slept are gone (0: all)
        assert!(calls[2].0 >= 300 && calls[2].1 <= ms, "{calls:?}");
    }

    #[test]
    fn a_receive_that_grows_its_buffer_keeps_no_wait_and_for_ever() {
        for (timeout, expected) in [(crate::NO_WAIT, 0), (crate::FOREVER, sys::FIPC_FOREVER)] {
            let mut waits = Vec::new();
            // SAFETY: the fake never returns FIPC_OK.
            let result = unsafe {
                receive_growing(timeout, |_, _, ms| {
                    waits.push(ms);
                    if waits.len() < 3 { (sys::FIPC_TOO_LARGE, 10 * waits.len()) } else { (sys::FIPC_TIMEOUT, 0) }
                })
            };
            assert_eq!(result, Err(Error::Timeout));
            assert_eq!(waits, [expected; 3]);
        }
    }
}
