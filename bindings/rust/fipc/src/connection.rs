use std::ffi::c_void;
use std::fmt;
use std::ops::{Deref, DerefMut};
use std::ptr::{self, NonNull};
use std::slice;
use std::sync::{Arc, Weak};
use std::time::Duration;

use fipc_sys as sys;

use crate::error::check;
use crate::listener::ListenerInner;
use crate::{Error, Result, RpcHeader, RpcMessage, c_name, millis};

/// One connection: two rings in shared memory, one per direction. A server gets one from
/// [`Listener::accept`](crate::Listener::accept), a client from [`Connection::connect`].
///
/// The calls that send ([`send`](Connection::send), [`send_acquire`](Connection::send_acquire),
/// [`rpc_submit`](Connection::rpc_submit), [`rpc_respond`](Connection::rpc_respond)) and the calls that receive
/// ([`receive`](Connection::receive), [`receive_into`](Connection::receive_into),
/// [`receive_acquire`](Connection::receive_acquire), [`rpc_receive`](Connection::rpc_receive),
/// [`rpc_receive_into`](Connection::rpc_receive_into)) take `&mut self`; to send and receive on two threads at once,
/// [`split`](Connection::split) or [`into_split`](Connection::into_split) the connection into its [`Sender`] and
/// [`Receiver`].
///
/// Dropping the connection ends it: the peer gets [`Error::Disconnected`] once it has received the messages this side
/// completed. After a split, that happens when both halves are dropped.
pub struct Connection {
    tx: Sender,
    rx: Receiver,
}

/// The sending half of a [`Connection`]: [`send`](Sender::send), [`send_acquire`](Sender::send_acquire),
/// [`rpc_submit`](Sender::rpc_submit) and [`rpc_respond`](Sender::rpc_respond). The connection closes when both
/// halves are dropped.
pub struct Sender {
    inner: Arc<ConnInner>,
}

/// The receiving half of a [`Connection`]: [`receive`](Receiver::receive), [`receive_into`](Receiver::receive_into),
/// [`receive_acquire`](Receiver::receive_acquire), [`rpc_receive`](Receiver::rpc_receive) and
/// [`rpc_receive_into`](Receiver::rpc_receive_into). The connection closes when both halves are dropped.
pub struct Receiver {
    inner: Arc<ConnInner>,
}

/// The native connection; dropping it closes it, once no half (and so no call) holds it.
pub(crate) struct ConnInner {
    ptr: NonNull<sys::fipc_conn_t>,
    name: Box<str>,
    max_piece: usize,
}

// SAFETY: the C API lets a connection be used from any thread, one thread at a time per side: the Sender and Receiver
// methods that send or receive take &mut self, and there is one of each per connection. fipc_cancel and
// fipc_max_piece may run on any thread at any time before the close, which runs in Drop, once no reference (and so no
// call) is left.
unsafe impl Send for ConnInner {}
// SAFETY: as above: through a shared reference, only the cancel and the name and max_piece (read once) are reachable.
unsafe impl Sync for ConnInner {}

impl ConnInner {
    fn conn(&self) -> *mut sys::fipc_conn_t {
        self.ptr.as_ptr()
    }

    pub(crate) fn cancel(&self) {
        // SAFETY: the connection is open while self lives; fipc_cancel may run on any thread.
        unsafe { sys::fipc_cancel(self.conn()) }
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
    /// `ptr` is an open connection, owned by the returned value from now on.
    pub(crate) unsafe fn from_raw(ptr: NonNull<sys::fipc_conn_t>, name: &str) -> Connection {
        // SAFETY: ptr is open.
        let max_piece = unsafe { sys::fipc_max_piece(ptr.as_ptr()) };
        let inner = Arc::new(ConnInner { ptr, name: name.into(), max_piece });
        Connection { tx: Sender { inner: Arc::clone(&inner) }, rx: Receiver { inner } }
    }

    /// Client: connects to the server listening on `name` and sets the connection up on the calling thread, waiting
    /// up to `timeout` for a server to listen (it may start later, or be serving another client) and to call
    /// [`accept`](crate::Listener::accept). It returns once the server's `accept` has offered the rings, which may be
    /// before that `accept` returns: messages sent meanwhile wait in the ring. The client and the server run on
    /// different threads (of one process or two). The connection's rings have the capacity the server chose.
    ///
    /// There is no handle to cancel until it returns: use `None` ([`FOREVER`](crate::FOREVER)) to wait for a server
    /// however late it starts, or a finite timeout to stay responsive.
    ///
    /// # Errors
    ///
    /// [`Error::Timeout`] if no server's `accept` set a connection up in time; [`Error::Invalid`] for a bad name, or a server
    /// with another version of the protocol or running as another user; [`Error::NoMemory`] if the rings can't be
    /// mapped.
    pub fn connect(name: &str, timeout: impl Into<Option<Duration>>) -> Result<Connection> {
        let c_name = c_name(name)?;
        let mut out = ptr::null_mut();
        // SAFETY: c_name is NUL-terminated; out is valid for a write.
        check(unsafe { sys::fipc_connect(c_name.as_ptr(), &mut out, millis(timeout)) }, 0)?;
        let ptr = NonNull::new(out).expect("fipc_connect returned FIPC_OK without a connection");
        // SAFETY: a connection fipc_connect just returned, owned by nothing else.
        Ok(unsafe { Connection::from_raw(ptr, name) })
    }

    /// The name of the listener this connection came through.
    pub fn name(&self) -> &str {
        &self.tx.inner.name
    }

    /// The longest message the zero-copy calls take: the connection's capacity less 64 bytes. The copying calls take
    /// messages of any size.
    pub fn max_piece(&self) -> usize {
        self.tx.inner.max_piece
    }

    /// Makes every call of this connection that waits, now or later, return [`Error::Cancelled`]; calls that needn't
    /// wait still work. A message cancelled halfway is dropped whole. The connection stays up: the peer sees nothing
    /// until it is dropped. Final. To cancel from another thread, while this one waits, use a
    /// [`canceler`](Connection::canceler).
    pub fn cancel(&self) {
        self.tx.inner.cancel();
    }

    /// A handle that cancels this connection from any thread, as [`cancel`](Connection::cancel) does: the way to stop
    /// a thread that waits on it.
    ///
    /// ```
    /// use fipc::{Connection, Error, Listener};
    ///
    /// # fn main() -> fipc::Result<()> {
    /// # let mut listener = Listener::listen("fipc_doc_cancel", 1 << 16)?;
    /// # let client = std::thread::spawn(|| Connection::connect("fipc_doc_cancel", None));
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
    /// use fipc::{Connection, Listener};
    ///
    /// # fn main() -> fipc::Result<()> {
    /// let mut listener = Listener::listen("fipc_doc_split", 1 << 16)?;
    /// let client = std::thread::spawn(|| Connection::connect("fipc_doc_split", None)); // connect waits for accept
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
    /// both are dropped.
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
    /// [`Error::Cancelled`]; [`Error::Invalid`] for an empty message.
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
    /// # use fipc::{Connection, Listener};
    /// # fn main() -> fipc::Result<()> {
    /// # let mut listener = Listener::listen("fipc_doc_send_acquire", 1 << 16)?;
    /// # let client = std::thread::spawn(|| Connection::connect("fipc_doc_send_acquire", None));
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
    /// [`Error::Invalid`] for a length of 0.
    pub fn send_acquire(&mut self, len: usize, timeout: impl Into<Option<Duration>>) -> Result<SendSlot<'_>> {
        self.tx.send_acquire(len, timeout)
    }

    /// Sends an RPC request with `payload` (any size, possibly empty) as [`send`](Connection::send) does, and returns
    /// its id: a connection numbers its requests from 1. The response arrives as an [`RpcMessage`] with that id.
    ///
    /// # Errors
    ///
    /// As [`send`](Connection::send).
    pub fn rpc_submit(&mut self, opcode: u32, payload: &[u8], timeout: impl Into<Option<Duration>>) -> Result<u64> {
        self.tx.rpc_submit(opcode, payload, timeout)
    }

    /// Sends the response to request `id`, with an `opcode` (by convention the request's), a `status` and a `payload`
    /// (any size, possibly empty), as [`send`](Connection::send) does.
    ///
    /// # Errors
    ///
    /// As [`send`](Connection::send).
    pub fn rpc_respond(
        &mut self,
        id: u64,
        opcode: u32,
        status: i32,
        payload: &[u8],
        timeout: impl Into<Option<Duration>>,
    ) -> Result<()> {
        self.tx.rpc_respond(id, opcode, status, payload, timeout)
    }

    // === Receiving ===

    /// Receives one message of any size into a new `Vec`: waits up to `timeout` for its first piece. Once a piece has
    /// arrived, the call goes on until the whole message has, past its timeout if the sender is slow to send the
    /// rest; only a cancel or the peer's end stops it, and the message is then dropped whole.
    ///
    /// # Errors
    ///
    /// [`Error::Timeout`] if no message began in time; [`Error::Disconnected`] once every message the peer completed
    /// has been received; [`Error::Cancelled`].
    pub fn receive(&mut self, timeout: impl Into<Option<Duration>>) -> Result<Vec<u8>> {
        self.rx.receive(timeout)
    }

    /// Receives one message into `buf` and returns its length, as [`receive`](Connection::receive) does, without
    /// allocating.
    ///
    /// ```
    /// # use fipc::{Connection, Error, Listener};
    /// # fn main() -> fipc::Result<()> {
    /// # let mut listener = Listener::listen("fipc_doc_receive_into", 1 << 16)?;
    /// # let client = std::thread::spawn(|| Connection::connect("fipc_doc_receive_into", None));
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
    /// taken, and `len` is the room it needs. After an error, the contents of `buf` are unspecified.
    pub fn receive_into(&mut self, buf: &mut [u8], timeout: impl Into<Option<Duration>>) -> Result<usize> {
        self.rx.receive_into(buf, timeout)
    }

    /// Zero-copy receive: waits up to `timeout` for a message and returns it in the ring, as a [`ReceiveSlot`]
    /// (16-byte aligned, read-only). Its room stays taken until the slot is dropped (or
    /// [`released`](ReceiveSlot::release)).
    ///
    /// ```
    /// # use fipc::{Connection, Listener};
    /// # fn main() -> fipc::Result<()> {
    /// # let mut listener = Listener::listen("fipc_doc_receive_acquire", 1 << 16)?;
    /// # let client = std::thread::spawn(|| Connection::connect("fipc_doc_receive_acquire", None));
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
    pub fn receive_acquire(&mut self, timeout: impl Into<Option<Duration>>) -> Result<ReceiveSlot<'_>> {
        self.rx.receive_acquire(timeout)
    }

    /// Receives one RPC request or response, its payload in a new `Vec`, as [`receive`](Connection::receive) does.
    ///
    /// # Errors
    ///
    /// As [`receive`](Connection::receive), and [`Error::Invalid`] for a message that isn't a well-formed RPC message
    /// (a plain message), which is dropped.
    pub fn rpc_receive(&mut self, timeout: impl Into<Option<Duration>>) -> Result<RpcMessage> {
        self.rx.rpc_receive(timeout)
    }

    /// Receives one RPC request or response with its payload in `buf`, and returns its header (the payload's length
    /// is [`RpcHeader::len`]), as [`rpc_receive`](Connection::rpc_receive) does, without allocating.
    ///
    /// # Errors
    ///
    /// As [`rpc_receive`](Connection::rpc_receive), and [`Error::TooLarge`] if the payload is longer than `buf`:
    /// nothing is taken, and `len` is the room it needs.
    pub fn rpc_receive_into(&mut self, buf: &mut [u8], timeout: impl Into<Option<Duration>>) -> Result<RpcHeader> {
        self.rx.rpc_receive_into(buf, timeout)
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

    /// See [`Connection::send`].
    pub fn send(&mut self, message: &[u8], timeout: impl Into<Option<Duration>>) -> Result<()> {
        // SAFETY: &mut self makes this the one thread sending; message is valid for its length.
        let code =
            unsafe { sys::fipc_send(self.inner.conn(), message.as_ptr().cast(), message.len(), millis(timeout)) };
        check(code, message.len())
    }

    /// See [`Connection::send_acquire`].
    pub fn send_acquire(&mut self, len: usize, timeout: impl Into<Option<Duration>>) -> Result<SendSlot<'_>> {
        let mut out = ptr::null_mut();
        // SAFETY: &mut self makes this the one thread sending; out is valid for a write.
        check(unsafe { sys::fipc_send_acquire(self.inner.conn(), len, &mut out, millis(timeout)) }, len)?;
        let ptr = NonNull::new(out.cast::<u8>()).expect("fipc_send_acquire returned FIPC_OK without room");
        Ok(SendSlot { sender: self, ptr, len })
    }

    /// See [`Connection::rpc_submit`].
    pub fn rpc_submit(&mut self, opcode: u32, payload: &[u8], timeout: impl Into<Option<Duration>>) -> Result<u64> {
        let mut id = 0;
        // SAFETY: &mut self makes this the one thread sending; payload is valid for its length; id for a write.
        let code = unsafe {
            sys::fipc_rpc_submit(
                self.inner.conn(),
                opcode,
                payload.as_ptr().cast(),
                payload.len(),
                &mut id,
                millis(timeout),
            )
        };
        check(code, payload.len()).map(|()| id)
    }

    /// See [`Connection::rpc_respond`].
    pub fn rpc_respond(
        &mut self,
        id: u64,
        opcode: u32,
        status: i32,
        payload: &[u8],
        timeout: impl Into<Option<Duration>>,
    ) -> Result<()> {
        // SAFETY: &mut self makes this the one thread sending; payload is valid for its length.
        let code = unsafe {
            sys::fipc_rpc_respond(
                self.inner.conn(),
                id,
                opcode,
                status,
                payload.as_ptr().cast(),
                payload.len(),
                millis(timeout),
            )
        };
        check(code, payload.len())
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

    /// One fipc_recv into `buf` (`cap` bytes, which may be uninitialized): Ok(length) or the result code with the
    /// length (the room a message needs for FIPC_TOO_LARGE).
    fn recv_raw(&mut self, buf: *mut u8, cap: usize, ms: i32) -> (sys::fipc_result_t, usize) {
        let mut len = 0;
        // SAFETY: &mut self makes this the one thread receiving; buf is valid for cap bytes of writes (the callers'
        // contract); len is valid for a write.
        let code = unsafe { sys::fipc_recv(self.inner.conn(), buf.cast::<c_void>(), cap, &mut len, ms) };
        (code, len)
    }

    /// See [`Connection::receive`].
    pub fn receive(&mut self, timeout: impl Into<Option<Duration>>) -> Result<Vec<u8>> {
        let ms = millis(timeout);
        let mut buf = Vec::new();
        loop {
            // The first call (no room) waits for a message and says its length; the next one takes it
            let (code, len) = self.recv_raw(buf.as_mut_ptr(), buf.capacity(), ms);
            match code {
                sys::FIPC_OK => {
                    // SAFETY: fipc_recv wrote len bytes (at most the capacity) into the buffer.
                    unsafe { buf.set_len(len) };
                    return Ok(buf);
                }
                sys::FIPC_TOO_LARGE => buf.reserve_exact(len),
                _ => return Err(Error::from_code(code, len)),
            }
        }
    }

    /// See [`Connection::receive_into`].
    pub fn receive_into(&mut self, buf: &mut [u8], timeout: impl Into<Option<Duration>>) -> Result<usize> {
        let (code, len) = self.recv_raw(buf.as_mut_ptr(), buf.len(), millis(timeout));
        check(code, len).map(|()| len)
    }

    /// See [`Connection::receive_acquire`].
    pub fn receive_acquire(&mut self, timeout: impl Into<Option<Duration>>) -> Result<ReceiveSlot<'_>> {
        let mut data = ptr::null();
        let mut len = 0;
        // SAFETY: &mut self makes this the one thread receiving; the out-pointers are valid for writes.
        let code = unsafe { sys::fipc_recv_acquire(self.inner.conn(), &mut data, &mut len, millis(timeout)) };
        check(code, len)?;
        let data = if len == 0 { NonNull::dangling() } else { NonNull::new(data.cast_mut().cast::<u8>()).unwrap() };
        Ok(ReceiveSlot { receiver: self, data, len })
    }

    /// One fipc_rpc_recv into `buf` (`cap` bytes, which may be uninitialized).
    fn rpc_recv_raw(&mut self, buf: *mut u8, cap: usize, ms: i32) -> (sys::fipc_result_t, sys::fipc_rpc_msg_t) {
        let mut msg = sys::fipc_rpc_msg_t::default();
        // SAFETY: &mut self makes this the one thread receiving; buf is valid for cap bytes of writes (the callers'
        // contract); msg is valid for a write.
        let code = unsafe { sys::fipc_rpc_recv(self.inner.conn(), buf.cast::<c_void>(), cap, &mut msg, ms) };
        (code, msg)
    }

    /// See [`Connection::rpc_receive`].
    pub fn rpc_receive(&mut self, timeout: impl Into<Option<Duration>>) -> Result<RpcMessage> {
        let ms = millis(timeout);
        let mut payload = Vec::new();
        loop {
            // The first call (no room) takes a message with an empty payload, or says the payload's length
            let (code, msg) = self.rpc_recv_raw(payload.as_mut_ptr(), payload.capacity(), ms);
            let len = usize::try_from(msg.len).unwrap_or(usize::MAX);
            match code {
                sys::FIPC_OK => {
                    let header = RpcHeader::from_sys(&msg)?;
                    // SAFETY: fipc_rpc_recv wrote the payload's len bytes (at most the capacity).
                    unsafe { payload.set_len(header.len) };
                    return Ok(header.with_payload(payload));
                }
                sys::FIPC_TOO_LARGE => payload.reserve_exact(len),
                _ => return Err(Error::from_code(code, len)),
            }
        }
    }

    /// See [`Connection::rpc_receive_into`].
    pub fn rpc_receive_into(&mut self, buf: &mut [u8], timeout: impl Into<Option<Duration>>) -> Result<RpcHeader> {
        let (code, msg) = self.rpc_recv_raw(buf.as_mut_ptr(), buf.len(), millis(timeout));
        check(code, usize::try_from(msg.len).unwrap_or(usize::MAX))?;
        RpcHeader::from_sys(&msg)
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
        // SAFETY: the slot holds the sending side (&mut Sender); the reservation is the latest one.
        check(unsafe { sys::fipc_send_commit(self.sender.inner.conn(), len) }, len)
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
/// The slot borrows the receiving side, so nothing else can be received until it is dropped.
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
        // receive (which the borrow of the Receiver rules out) or the close (ruled out as well).
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

/// Cancels a [`Listener`](crate::Listener) or a [`Connection`] from any thread: the way to stop a thread that waits
/// in [`accept`](crate::Listener::accept) or a receive (or a send, for room). Cheap to clone, `Send` and `Sync`.
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
}

impl Canceler {
    pub(crate) fn listener(inner: Weak<ListenerInner>) -> Canceler {
        Canceler { target: Target::Listener(inner) }
    }

    /// Cancels the listener or connection, unless it is closed already.
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
        }
    }
}

impl fmt::Debug for Canceler {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let what = match self.target {
            Target::Listener(_) => "listener",
            Target::Connection(_) => "connection",
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
