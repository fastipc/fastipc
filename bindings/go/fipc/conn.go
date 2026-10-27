package fipc

import (
	"runtime"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/ebitengine/purego"
)

// scratchSize is the room of a connection's receive buffer: Receive takes a message that fits it in one call of the
// library and copies it into a slice of its length; a longer one takes a second call, into a slice made for it.
const scratchSize = 4096

// Conn is one connection: a stream's two rings in shared memory, one per direction. A server gets one from
// [Listener.Accept], a client from [ConnectStream]. A broadcast's publisher ([CreateBroadcast]) and its subscribers
// ([JoinBroadcast]) are Conns too: the publisher sends to every subscriber, and each subscriber receives. So are a work
// queue's producers and consumers ([JoinQueue]): a producer only sends, and a consumer only receives. And so are a
// service's clients and servers ([JoinService]): a client sends requests and receives their replies ([Conn.Call] does
// both), and a server receives a request and replies to it.
//
// Every method is safe to call from any goroutine, as with a net.Conn: each direction has a mutex of its own, so
// concurrent sends (Send, SendMany, SendAcquire, SendCommit) run one at a time, and so do concurrent receives
// (Receive, ReceiveInto, ReceiveMany, ReceiveAcquire, ReceiveRelease), while a send and a receive run at once. Each call is whole: two
// goroutines' messages never interleave. A publisher's [Conn.WaitForMembers] runs beside all of them. A service
// client's Calls run one at a time, each holding both directions.
//
// Close ends the connection: the peer gets [ErrDisconnected] once it has received the messages this side completed.
// A Conn that becomes unreachable without Close is closed by the garbage collector, some time later: close it
// yourself to end it at once (a listener accepts its next client only once its last connection is closed).
type Conn struct {
	h          uintptr // the native connection, open until Close
	name       string
	maxPiece   int
	subscriber bool // a broadcast's subscriber (JoinBroadcast)
	client     bool // a service's client (JoinService): it keeps count of the replies owed to its requests

	closed   atomic.Bool
	cancelMu sync.RWMutex // Cancel and WaitForMembers hold it for reading, Close for writing: none runs into fipc_close
	cleanup  runtime.Cleanup

	// A service's client keeps count, oldest first, of the requests whose replies it hasn't received yet, so that
	// Call never returns the reply to another request: first those whose Call gave up on its reply (it timed out or
	// was cancelled), which the next Call receives and drops (at most one, as a Call first drops what is owed), then
	// those sent with Send or SendCommit, whose replies are the caller's to receive. A request is counted before its
	// send (a receiving goroutine may get its reply before the send returns) and uncounted if the send fails; a reply
	// or ErrAbandoned takes the oldest off. Sends add to sendsOwed while receives take off, hence the atomics.
	callsOwed atomic.Int32
	sendsOwed atomic.Int32
	// A client's Calls take turns: a Call holds the one slot of calls from its turn to its return, and one waiting
	// for its turn also wakes at its timeout and at cancelled, which the first Cancel or Close closes.
	calls      chan struct{}
	cancelled  chan struct{}
	cancelOnce sync.Once

	// Each direction's calls go through its own argument array, so that a call allocates nothing (see ffi). The
	// arguments hold the addresses of Go memory as uintptrs, which the GC neither traces nor keeps alive: the caller's
	// buffer is held in pin for the call (which also makes it escape to the heap, where nothing moves it), and the
	// out-parameters are this Conn's own fields. The two directions sit on cache lines of their own: a sender and a
	// receiver run at once.
	_  [64]byte
	tx struct {
		mu   sync.Mutex // one send at a time; Close holds it while it closes
		args [6]uintptr
		pin  unsafe.Pointer // the caller's message, during a call
		out  unsafe.Pointer // SendAcquire's out-parameter: room in the ring
		// SendMany's arrays, kept from one batch to the next: each message's bytes (pointers the GC sees, which keep the
		// messages on the heap and alive during the call; cleared after it) and length; sent, its out-parameter
		datas []unsafe.Pointer
		lens  []uintptr
		sent  uintptr
	}
	_  [64]byte
	rx struct {
		mu      sync.Mutex // one receive at a time; Close holds it while it closes
		args    [7]uintptr
		pin     unsafe.Pointer // the caller's buffer, during a call
		pinLens unsafe.Pointer // ReceiveMany's lengths, during a call
		len     uintptr        // the receives' out-parameter: the message's length
		data    unsafe.Pointer // ReceiveAcquire's out-parameter: the message in the ring
		scratch []byte         // Receive's buffer, made on its first call
	}
	_ [64]byte
}

// newConn wraps a native connection that nothing else owns.
func newConn(h uintptr, name string) *Conn {
	piece, _, _ := purego.SyscallN(lib.maxPiece, h)
	c := &Conn{h: h, name: name, maxPiece: int(piece)}
	c.cleanup = runtime.AddCleanup(c, closeConn, h)
	return c
}

// closeConn closes a native connection whose Conn became unreachable.
func closeConn(h uintptr) {
	purego.SyscallN(lib.close, h)
}

// ConnectStream connects to the stream's server listening on name and sets the connection up on the calling
// goroutine, waiting up to timeout for a server to listen (it may start later, or be serving another client) and to
// call [Listener.Accept]. It returns once the server's Accept has offered the rings, which may be before that Accept
// returns: messages sent meanwhile wait in the ring. A client and its server in one process run on different
// goroutines. The connection's rings have the capacity the server chose.
//
// There is no handle to cancel until it returns: use [Forever] to wait for a server however late it starts, or a
// finite timeout to stay responsive.
//
// Errors: [ErrTimeout] if no server's Accept set a connection up in time; [ErrInvalid] for a bad name, a server with
// another version of the protocol or running as another user, or a name a broadcast, a queue or a service holds;
// [ErrNoMemory] if the rings can't be mapped; an error wrapping [ErrNoLibrary] if the library can't be loaded.
func ConnectStream(name string, timeout time.Duration) (*Conn, error) {
	if err := load(); err != nil {
		return nil, err
	}
	c, ok := cName(name)
	if !ok {
		return nil, ErrInvalid
	}
	var out uintptr
	r, _, _ := purego.SyscallN(lib.streamConnect, uintptr(unsafe.Pointer(&c[0])), uintptr(unsafe.Pointer(&out)),
		uintptr(millis(timeout)))
	if err := result(r, 0); err != nil {
		return nil, err
	}
	return newConn(out, name), nil
}

// Name returns the name of the stream, broadcast, queue or service the connection belongs to.
func (c *Conn) Name() string { return c.name }

// MaxPiece returns the longest message the zero-copy calls take: the connection's capacity less 64 bytes. The copying
// calls take messages of any size. On a broadcast or a work queue, the longest message of any call: a quarter
// of its capacity. On a service, the longest request or reply: its maxMessage.
func (c *Conn) MaxPiece() int { return c.maxPiece }

// Cancel makes every call of the connection that waits, now or later, return [ErrCancelled]; calls that needn't wait
// still work. A message cancelled halfway is dropped whole. The connection stays up: the peer sees nothing until
// Close. Any goroutine, any time: the way to stop a goroutine that waits on the connection (with a context:
// context.AfterFunc(ctx, conn.Cancel)). Final. After Close, it does nothing.
//
// On a service's client it also ends the wait of a Call for its turn behind another Call, and a later Call sends
// nothing: it returns [ErrCancelled] at once.
func (c *Conn) Cancel() {
	c.cancelMu.RLock()
	defer c.cancelMu.RUnlock()
	if !c.closed.Load() {
		c.wakeCalls() // first: the cancel ends the Call that holds the turn, whose end must not hand it to a waiting one
		purego.SyscallN(lib.cancel, c.h)
	}
}

// wakeCalls ends the waits of a service client's Calls for their turn: from now on they return ErrCancelled.
func (c *Conn) wakeCalls() {
	if c.cancelled != nil {
		c.cancelOnce.Do(func() { close(c.cancelled) })
	}
}

// Close ends the connection: the peer gets [ErrDisconnected] once it has received the messages this side completed.
// The rings are unmapped: every slice SendAcquire or ReceiveAcquire returned becomes invalid.
//
// It may run while other goroutines are inside calls on the connection: it cancels the connection, so that the calls
// that wait return [ErrCancelled], waits for every call to return, then closes. Calls after Close return [ErrClosed]
// (ReceiveRelease does nothing; Name and MaxPiece still answer); so does a second Close.
//
// A broadcast's publisher's Close ends the broadcast and frees the name: its subscribers get [ErrDisconnected] once
// they have received what it published. A subscriber's Close leaves the broadcast. A work
// queue's producer's or consumer's Close leaves the queue, which goes on (a consumer's releases a message it holds in
// place); the messages a producer sent stay for the consumers. A service's client's Close leaves the service: its
// requests that no server took are dropped, and the replies to those already taken are lost. A server's Close leaves
// it too, and a request it still holds is answered [ErrAbandoned].
func (c *Conn) Close() error {
	if !c.closed.CompareAndSwap(false, true) {
		return ErrClosed
	}
	c.wakeCalls()                    // first, as in Cancel
	purego.SyscallN(lib.cancel, c.h) // wakes the calls that wait
	c.tx.mu.Lock()
	defer c.tx.mu.Unlock()
	c.rx.mu.Lock()
	defer c.rx.mu.Unlock()
	c.cancelMu.Lock()
	defer c.cancelMu.Unlock()
	c.cleanup.Stop()
	purego.SyscallN(lib.close, c.h)
	c.tx.out, c.rx.data, c.rx.scratch = nil, nil, nil
	return nil
}

// === Sending ===

// send calls fn with the first n of tx.args, after putting the connection in tx.args[0]. The caller holds tx.mu and has
// filled tx.args[1:n]; pin is the caller's message, kept reachable for the call.
func (c *Conn) send(fn uintptr, pin unsafe.Pointer, n int) uintptr {
	c.tx.pin = pin
	c.tx.args[0] = c.h
	r := ffi(fn, c.tx.args[:n])
	c.tx.pin = nil
	return r
}

// Send sends one message (at least 1 byte, any size): it waits up to timeout for room for its first piece, then
// copies it in, in pieces if it is longer than one piece of the ring ([Conn.MaxPiece]). Once a piece has moved, the
// call goes on until the whole message has, past its timeout if the receiver is slow to make room; only a cancel or
// the peer's end stops it, and the message is then dropped whole. It drops a SendAcquire reservation that was never
// committed.
//
// A broadcast's publisher and a work queue's producer send messages of one piece: [ErrTooLarge] over MaxPiece. A
// producer waits for room while consumers haven't taken (or, in place, released) the messages before it; once the
// queue has ended its sends return ErrDisconnected (one that returns it just as the queue ends may still have reached
// a consumer). On a subscriber or a consumer, [ErrInvalid].
//
// A service's client sends a request of one piece (ErrTooLarge over MaxPiece), waiting for room in its ring, which
// its requests hold until their replies have been received: a client that keeps sending without receiving waits for
// room its own replies hold. A server's Send replies to the request it holds and never waits: ErrInvalid with no
// request held; a reply to a client that has left is dropped and returns nil. Once the service has ended, both
// return ErrDisconnected.
//
// Errors: [ErrTimeout] if there was no room for the first piece in time; [ErrDisconnected]; [ErrCancelled];
// [ErrInvalid] for an empty message; [ErrClosed].
func (c *Conn) Send(msg []byte, timeout time.Duration) error {
	c.tx.mu.Lock()
	defer c.tx.mu.Unlock()
	if c.closed.Load() {
		return ErrClosed
	}
	p := unsafe.Pointer(unsafe.SliceData(msg))
	a := &c.tx.args
	a[1], a[2], a[3] = uintptr(p), uintptr(len(msg)), uintptr(millis(timeout))
	c.countRequest()
	r := c.send(lib.send, p, 4)
	c.requestSent(r)
	return result(r, len(msg))
}

// sendCopy is one fipc_send of msg (tx.mu held), as Send's.
func (c *Conn) sendCopy(msg []byte, ms int32) uintptr {
	p := unsafe.Pointer(unsafe.SliceData(msg))
	a := &c.tx.args
	a[1], a[2], a[3] = uintptr(p), uintptr(len(msg)), uintptr(ms)
	return c.send(lib.send, p, 4)
}

// SendMany sends msgs (at least one, each of at least 1 byte) in order in one call of the library and publishes them
// together, with at most one wake of the receivers for the batch, and returns how many went: all of them, with a nil
// error. The timeout bounds the whole call ([NoWait]: only what fits now). An error is the first message that didn't
// go, as Send would return it, and n is how many went, all those before it. Every message is checked first, before
// any goes: [ErrInvalid] for an empty one or an empty batch, [ErrTooLarge] over [Conn.MaxPiece] on a broadcast, a work
// queue or a service (a stream sends a longer message in pieces, as Send does). A service's client sends a batch of
// requests, whose replies come in their order and are owed as Send's are; on a server, ErrInvalid. A producer whose
// batch meets the queue's end returns [ErrDisconnected], and n leaves out what that publish held (it may still reach a
// consumer). Batch both ends: a receiver that takes one message per call falls behind a batching sender, and the two
// run slower than without batches.
//
// Errors: [ErrTimeout]; [ErrDisconnected]; [ErrCancelled]; [ErrInvalid]; [ErrTooLarge]; [ErrClosed].
func (c *Conn) SendMany(msgs [][]byte, timeout time.Duration) (n int, err error) {
	c.tx.mu.Lock()
	defer c.tx.mu.Unlock()
	if c.closed.Load() {
		return 0, ErrClosed
	}
	datas, lens := c.tx.datas[:0], c.tx.lens[:0]
	tooLarge := 0 // ErrTooLarge's length: the first message over one piece (the batch is checked before any goes)
	for _, msg := range msgs {
		datas = append(datas, unsafe.Pointer(unsafe.SliceData(msg)))
		lens = append(lens, uintptr(len(msg)))
		if tooLarge == 0 && len(msg) > c.maxPiece {
			tooLarge = len(msg)
		}
	}
	c.tx.datas, c.tx.lens, c.tx.sent = datas, lens, 0
	a := &c.tx.args
	a[1], a[2], a[3] = uintptr(unsafe.Pointer(unsafe.SliceData(datas))), uintptr(unsafe.Pointer(unsafe.SliceData(lens))),
		uintptr(len(msgs))
	a[4], a[5] = uintptr(unsafe.Pointer(&c.tx.sent)), uintptr(millis(timeout))
	c.countRequests(len(msgs))
	r := c.send(lib.sendMany, nil, 6)
	n = int(c.tx.sent)
	c.requestsUnsent(len(msgs) - n)
	clear(datas) // the messages are the caller's again: the array no longer keeps them alive
	return n, result(r, tooLarge)
}

// countRequests counts, on a service's client, a batch of requests about to be sent: their replies are owed from
// before the send.
func (c *Conn) countRequests(n int) {
	if c.client {
		c.sendsOwed.Add(int32(n))
	}
}

// requestsUnsent uncounts, on a service's client, the requests counted with countRequests that didn't go.
func (c *Conn) requestsUnsent(n int) {
	if c.client && n > 0 {
		c.sendsOwed.Add(-int32(n))
	}
}

// countRequest counts, on a service's client, a request about to be sent: its reply is owed from before the send.
func (c *Conn) countRequest() {
	if c.client {
		c.sendsOwed.Add(1)
	}
}

// requestSent uncounts, on a service's client, a request counted with countRequest whose send returned r, unless it
// went.
func (c *Conn) requestSent(r uintptr) {
	if c.client && int32(r) != codeOK {
		c.sendsOwed.Add(-1)
	}
}

// answered takes, on a service's client, the oldest request owed a reply off the count when a receive returned r: a
// reply or FIPC_ABANDONED answers it.
func (c *Conn) answered(r uintptr) {
	if !c.client || (int32(r) != codeOK && int32(r) != codeAbandoned) {
		return
	}
	if c.callsOwed.Load() > 0 {
		c.callsOwed.Add(-1)
		return
	}
	for owed := c.sendsOwed.Load(); owed > 0; owed = c.sendsOwed.Load() {
		if c.sendsOwed.CompareAndSwap(owed, owed-1) {
			return
		}
	}
}

// SendAcquire is a zero-copy send, step 1: it waits up to timeout for n (at least 1, up to [Conn.MaxPiece])
// contiguous bytes in the ring and returns them, 16-byte aligned. Write the message there, then call
// [Conn.SendCommit].
//
// The slice points into shared memory. It is valid until SendCommit, the next send of any kind on the connection
// (from any goroutine; it drops a reservation that was never committed) or Close; using it after that is undefined
// (it may crash the program). Keep the Conn reachable while the slice is in use: one the garbage collector closes
// unmaps it. Don't append beyond its length.
//
// The room is contiguous: a reservation that doesn't fit before the ring's end waits until the receiver has read past
// the end, even in an empty ring, so a long one (near MaxPiece) can wait for the peer's next receive call.
//
// On a service's client the room is for a request, which SendCommit sends as Send does: it waits for room in the
// client's ring, which its requests hold until their replies have been received. On a server it is for the reply to
// the request the server holds, which SendCommit sends, and it never waits: [ErrInvalid] with no request held. Once the
// service has ended, both return ErrDisconnected.
//
// Errors: [ErrTooLarge] over MaxPiece (send such a message with Send); [ErrTimeout]; [ErrDisconnected];
// [ErrCancelled]; [ErrInvalid] for n of 0; [ErrClosed].
func (c *Conn) SendAcquire(n int, timeout time.Duration) ([]byte, error) {
	c.tx.mu.Lock()
	defer c.tx.mu.Unlock()
	if c.closed.Load() {
		return nil, ErrClosed
	}
	if n < 0 {
		return nil, ErrInvalid
	}
	a := &c.tx.args
	a[1], a[2], a[3] = uintptr(n), uintptr(unsafe.Pointer(&c.tx.out)), uintptr(millis(timeout))
	if err := result(c.send(lib.sendAcquire, nil, 4), n); err != nil {
		return nil, err
	}
	return unsafe.Slice((*byte)(c.tx.out), n), nil
}

// SendCommit is a zero-copy send, step 2: it publishes the first n bytes of the room SendAcquire returned (1 to its
// length) as one message. It doesn't wait.
//
// On a service's client it sends a request; on a server it replies to the request held, which stays valid, in place,
// until the commit.
//
// Errors: [ErrInvalid] without a reservation, or for n out of that range (the reservation stays for a valid n);
// [ErrDisconnected] on a service that has ended; [ErrClosed].
func (c *Conn) SendCommit(n int) error {
	c.tx.mu.Lock()
	defer c.tx.mu.Unlock()
	if c.closed.Load() {
		return ErrClosed
	}
	if n < 0 {
		return ErrInvalid
	}
	c.tx.args[1] = uintptr(n)
	c.countRequest()
	r := c.send(lib.sendCommit, nil, 2)
	c.requestSent(r)
	return result(r, n)
}

// === Receiving ===

// recv is one fipc_recv into buf (rx.mu held); the message's length is in rx.len.
func (c *Conn) recv(buf []byte, ms int32) uintptr {
	c.rx.pin = unsafe.Pointer(unsafe.SliceData(buf))
	a := &c.rx.args
	a[0], a[1], a[2], a[3], a[4] = c.h, uintptr(c.rx.pin), uintptr(len(buf)), uintptr(unsafe.Pointer(&c.rx.len)),
		uintptr(ms)
	r := ffi(lib.recv, a[:5])
	c.rx.pin = nil
	return r
}

// Receive receives one message of any size into a new slice: it waits up to timeout for its first piece. Once a piece
// has arrived, the call goes on until the whole message has, past its timeout if the sender is slow to send the rest;
// only a cancel or the peer's end stops it, and the message is then dropped whole.
//
// Errors: [ErrTimeout] if no message began in time; [ErrDisconnected] once every message the peer completed has been
// received; [ErrCancelled]; [ErrClosed]. On a broadcast's subscriber also [ErrOverrun] (a lossy one: its Lost
// says how many messages were lost; the next receive goes on from a recent message) and [ErrUnsubscribed] (a gated
// one, final); on its publisher [ErrInvalid]. On a work queue's consumer, ErrDisconnected once the queue has ended and
// every message left in it was received; on a producer, ErrInvalid.
//
// On a service's client: the reply to its oldest request whose reply it hasn't received yet, waiting for it if it
// hasn't come, or [ErrAbandoned] if the server that took that request ended before it replied (the request counts as
// answered). A receive that times out or is cancelled leaves its reply to come: the next receive returns it. After a
// [Conn.Call] that gave up, Receive returns that Call's late reply (or ErrAbandoned) first: replies come in request
// order. Once the service has ended, the replies that came before the end, then ErrDisconnected. On a server: the next request of any
// client, which the server holds until its reply (ErrInvalid while it holds one); ErrDisconnected once the service
// has ended.
func (c *Conn) Receive(timeout time.Duration) ([]byte, error) {
	c.rx.mu.Lock()
	defer c.rx.mu.Unlock()
	if c.closed.Load() {
		return nil, ErrClosed
	}
	return c.receive(millis(timeout))
}

// receive is Receive's work (rx.mu held): one message into a new slice, waiting up to ms milliseconds.
func (c *Conn) receive(ms int32) ([]byte, error) {
	if c.rx.scratch == nil {
		c.rx.scratch = make([]byte, scratchSize)
	}
	var start instant
	if ms > 0 {
		start = now()
	}
	r := c.recv(c.rx.scratch, ms)
	if int32(r) == codeOK {
		c.answered(r)
		msg := make([]byte, c.rx.len)
		copy(msg, c.rx.scratch)
		return msg, nil
	}
	// Longer than the scratch buffer: the message stays queued, so take it into a slice of its length
	msg, r, n := receiveGrowing(r, int(c.rx.len), ms, start, func(buf []byte, ms int32) (uintptr, int) {
		r := c.recv(buf, ms)
		return r, int(c.rx.len)
	})
	c.answered(r)
	if int32(r) == codeOK {
		return msg, nil
	}
	return nil, result(r, n)
}

// receiveGrowing receives into a slice of the length n that a receive's FIPC_TOO_LARGE (r) reported, and again into a
// longer one while the message it finds is longer than the slice: on a work queue another consumer may take the
// message first, and the call then finds the next one. Each call waits only for what is left of the timeout of ms
// milliseconds that began at start. It returns the message, of exactly its length, or the last call's result and
// length.
func receiveGrowing(r uintptr, n int, ms int32, start instant,
	recv func(buf []byte, ms int32) (uintptr, int)) ([]byte, uintptr, int) {
	for int32(r) == codeTooLarge {
		msg := make([]byte, n)
		if r, n = recv(msg, remaining(ms, start)); int32(r) == codeOK {
			return msg[:n], r, n
		}
	}
	return nil, r, n
}

// remaining is what is left of a timeout of ms milliseconds that began at start: for ever (-1) and no wait (0) stay
// as they are, and a timeout that has run out polls.
func remaining(ms int32, start instant) int32 {
	if ms <= 0 {
		return ms
	}
	left := time.Duration(ms)*time.Millisecond - since(start)
	if left <= 0 {
		return 0
	}
	return millis(left)
}

// ReceiveInto receives one message into buf and returns its length, as Receive does, without allocating.
//
// Errors: as Receive's, and [ErrTooLarge] if the message is longer than buf: nothing is taken, and the error's Len is
// the room it needs (on a work queue another consumer may take that message first, and the next receive then finds
// another). After an error, the contents of buf are unspecified.
func (c *Conn) ReceiveInto(buf []byte, timeout time.Duration) (int, error) {
	c.rx.mu.Lock()
	defer c.rx.mu.Unlock()
	if c.closed.Load() {
		return 0, ErrClosed
	}
	r := c.recv(buf, millis(timeout))
	c.answered(r)
	if err := result(r, int(c.rx.len)); err != nil {
		return 0, err
	}
	return int(c.rx.len), nil
}

// Go's int is the C API's size_t, 8 bytes, on every platform the library supports: ReceiveMany passes lens as it is
var _ [unsafe.Sizeof(int(0)) - unsafe.Sizeof(uintptr(0))]byte
var _ [unsafe.Sizeof(uintptr(0)) - unsafe.Sizeof(int(0))]byte

// ReceiveMany waits up to timeout for the next message, as Receive does, then takes every further message already there
// that fits the rest of buf, up to len(lens) (at least 1), in one call of the library, packed back to back: message i
// is lens[i] bytes, at the sum of the lengths before it. It returns how many it took. After the first it never waits,
// and stops before a message that isn't there yet, doesn't fit, is in pieces, or would bring another result: the next
// receive returns it. A broadcast's subscriber takes, after its first, only the messages its publisher has moved past
// (the newest come in the next calls); a work queue's consumer acknowledges each message before the call returns; a
// service's client receives its replies in its requests' order.
//
// Errors: the first message's, as ReceiveInto returns it, with nothing else taken: [ErrTooLarge] if it is longer than
// buf (the error's Len is the room it needs; it stays queued), [ErrOverrun], a service client's [ErrAbandoned], ...;
// [ErrInvalid] for an empty lens, on a publisher, a producer and a service's server (which holds one request at a
// time); [ErrClosed]. After an error, the contents of buf and lens are unspecified.
func (c *Conn) ReceiveMany(buf []byte, lens []int, timeout time.Duration) (int, error) {
	c.rx.mu.Lock()
	defer c.rx.mu.Unlock()
	if c.closed.Load() {
		return 0, ErrClosed
	}
	c.rx.pin, c.rx.pinLens = unsafe.Pointer(unsafe.SliceData(buf)), unsafe.Pointer(unsafe.SliceData(lens))
	a := &c.rx.args
	a[0], a[1], a[2], a[3], a[4] = c.h, uintptr(c.rx.pin), uintptr(len(buf)), uintptr(c.rx.pinLens), uintptr(len(lens))
	a[5], a[6] = uintptr(unsafe.Pointer(&c.rx.len)), uintptr(millis(timeout))
	r := ffi(lib.recvMany, a[:7])
	c.rx.pin, c.rx.pinLens = nil, nil
	if int32(r) != codeOK {
		c.answered(r)
		first := 0 // ErrTooLarge's length, ErrOverrun's count
		if len(lens) > 0 {
			first = lens[0]
		}
		return 0, result(r, first)
	}
	n := int(c.rx.len)
	for range n {
		c.answered(r)
	}
	return n, nil
}

// ReceiveAcquire is a zero-copy receive: it waits up to timeout for a message and returns it in the ring, 16-byte
// aligned. Its room stays taken until [Conn.ReceiveRelease].
//
// The slice points into shared memory and is read-only: writing to it is undefined. It is valid until ReceiveRelease,
// the next receive of any kind on the connection (from any goroutine; it releases the message first) or Close; using
// it after that is undefined (it may crash the program). Keep the Conn reachable while the slice is in use: one the
// garbage collector closes unmaps it. Copy what must outlive it.
//
// A broadcast's subscriber receives copies only (Receive, ReceiveInto): on one, ReceiveAcquire returns
// [ErrInvalid]. A gated subscriber's message in place becomes inaccessible, and reading it crashes the program, at the
// moment the publisher's clock drops the subscriber for holding the publisher back, which no Go code can bound.
//
// A work queue's consumer receives in place too: the message is its own until ReceiveRelease, which acknowledges it.
// If the consumer's process ends before, the message goes again, to another consumer: release after the work for at
// least once (the work must tolerate a duplicate), before it for at most once.
//
// A service's client receives its reply in place, until ReceiveRelease. A server receives the request in place: it
// stays valid until the reply's Send or SendCommit (the reply may be written from it), and ReceiveRelease does
// nothing on a server.
//
// Errors: as Receive's, and [ErrTooLarge] for a message in several pieces: the error's Len is its length, and it stays
// queued for Receive or ReceiveInto; [ErrInvalid] on a broadcast's publisher or subscriber, or a queue's producer.
func (c *Conn) ReceiveAcquire(timeout time.Duration) ([]byte, error) {
	c.rx.mu.Lock()
	defer c.rx.mu.Unlock()
	if c.closed.Load() {
		return nil, ErrClosed
	}
	if c.subscriber {
		return nil, ErrInvalid
	}
	a := &c.rx.args
	a[0], a[1], a[2], a[3] = c.h, uintptr(unsafe.Pointer(&c.rx.data)), uintptr(unsafe.Pointer(&c.rx.len)),
		uintptr(millis(timeout))
	r := ffi(lib.recvAcquire, a[:4])
	c.answered(r)
	if err := result(r, int(c.rx.len)); err != nil {
		return nil, err
	}
	return unsafe.Slice((*byte)(c.rx.data), c.rx.len), nil
}

// ReceiveRelease is a zero-copy receive, step 2: it frees the room of the message ReceiveAcquire returned, whose
// slice becomes invalid. Without one, or after Close, it does nothing; on a service's server it does nothing (the
// request is held until the reply). It doesn't wait.
func (c *Conn) ReceiveRelease() {
	c.rx.mu.Lock()
	defer c.rx.mu.Unlock()
	if !c.closed.Load() {
		c.rx.args[0] = c.h
		ffi(lib.recvRelease, c.rx.args[:1])
	}
}

// === A service's calls ===

// Call is a service client's request and its reply in one: it sends request (1 to [Conn.MaxPiece] bytes) and returns
// its reply, a new slice. The timeout bounds the whole Call, not each of its steps: its waits for its turn, for a reply
// owed to an earlier Call, for room for its request and for its reply each take only what is left of it.
//
// A Call checks its request first: [ErrInvalid] for an empty one, [ErrTooLarge] over MaxPiece, at once, before it
// waits for anything (its turn included), even while a reply is owed. Then a Call on a cancelled connection returns
// [ErrCancelled] at once, whatever its timeout, and sends nothing: it could never receive the reply.
//
// The connection keeps count of the replies owed to its requests, so that a Call never returns the reply to another
// request. A Call whose receive timed out or was cancelled leaves its reply owed, and the next Call first receives and
// drops that reply (at most one is owed, since every Call drops it first; an ErrAbandoned in its place counts too),
// within its own timeout and before it sends. A reply owed to a request sent with Send or SendCommit is the caller's
// to receive: while one is, Call returns [ErrInvalid] (receive it first). After a Call that gave up, a plain Receive
// returns that Call's late reply (or ErrAbandoned) first: replies come in request order.
//
// Calls from several goroutines run one at a time, in turn, and each returns its own request's reply; a Call waits for
// its turn within its own timeout, and a Cancel or Close ends that wait with [ErrCancelled]. While a Call runs it holds
// both directions of the connection: the client's Sends and Receives on other goroutines wait for it to return (and a
// Call waits for one of theirs that is in progress, which can outlast the Call's timeout). A Call that had to drop an
// owed reply or wait for its turn (or for such a Send or Receive) first sends its request only if its timeout has time
// left: once a positive timeout has run out, it returns [ErrTimeout] without sending, so no server sees that request.
// With [NoWait] no step waits, and it sends if there is room.
//
// Errors: [ErrAbandoned] if the server that took the request ended before it replied (the request counts as answered:
// the next Call goes on); [ErrTimeout]; [ErrCancelled]; [ErrDisconnected] once the service has ended (after the
// replies that came before the end); [ErrTooLarge] over MaxPiece; [ErrInvalid] for an empty request, while a reply to
// a Send is owed, or on any connection but a service's client; [ErrClosed].
func (c *Conn) Call(request []byte, timeout time.Duration) ([]byte, error) {
	if !c.client {
		if c.closed.Load() {
			return nil, ErrClosed
		}
		return nil, ErrInvalid
	}
	if len(request) == 0 {
		return nil, ErrInvalid
	}
	if len(request) > c.maxPiece {
		return nil, result(codeTooLarge, len(request))
	}
	if c.isCancelled() { // its reply could never be received: the request never goes
		if c.closed.Load() {
			return nil, ErrClosed
		}
		return nil, ErrCancelled
	}
	ms := millis(timeout)
	var start instant
	if ms > 0 {
		start = now()
	}
	// waited: for its turn, a direction or an owed reply, after which it sends only with time left
	waited := false
	select { // its turn: one Call at a time
	case c.calls <- struct{}{}:
	default:
		if err := c.awaitTurn(ms); err != nil {
			return nil, err
		}
		waited = true
	}
	defer func() { <-c.calls }()
	// Both directions, so that no other send or receive runs into the call's
	if !c.tx.mu.TryLock() {
		c.tx.mu.Lock()
		waited = true
	}
	defer c.tx.mu.Unlock()
	if !c.rx.mu.TryLock() {
		c.rx.mu.Lock()
		waited = true
	}
	defer c.rx.mu.Unlock()
	if c.closed.Load() {
		return nil, ErrClosed
	}
	if c.sendsOwed.Load() != 0 {
		return nil, ErrInvalid // the reply to a request of Send's is the caller's, not the call's to drop
	}
	for c.callsOwed.Load() > 0 {
		// A reply owed to a Call that gave up on it: dropped (the receive takes it off the count)
		if _, err := c.receive(remaining(ms, start)); err != nil && err != ErrAbandoned {
			return nil, err
		}
		waited = true
	}
	if waited && ms > 0 && since(start) >= timeout {
		return nil, ErrTimeout // its time ran out before its request went: it never goes
	}
	if c.isCancelled() {
		return nil, ErrCancelled // cancelled while it waited: the request never goes
	}
	c.callsOwed.Store(1) // its reply is owed from before the send
	if r := c.sendCopy(request, remaining(ms, start)); int32(r) != codeOK {
		c.callsOwed.Store(0)
		return nil, result(r, len(request))
	}
	return c.receive(remaining(ms, start)) // a reply or ErrAbandoned takes it off the count; else it stays owed
}

// isCancelled reports whether a service client's connection is cancelled (or closed): its Calls send nothing.
func (c *Conn) isCancelled() bool {
	select {
	case <-c.cancelled:
		return true
	default:
		return false
	}
}

// awaitTurn waits up to ms milliseconds for a Call's turn, while another Call has it: ErrTimeout when that runs out,
// ErrCancelled once the connection is cancelled or closed, also when the turn comes with the cancel (the Call before
// it ended with it).
func (c *Conn) awaitTurn(ms int32) error {
	if ms == 0 {
		return ErrTimeout
	}
	var expired <-chan time.Time
	if ms > 0 {
		timer := time.NewTimer(time.Duration(ms) * time.Millisecond)
		defer timer.Stop()
		expired = timer.C
	}
	select {
	case c.calls <- struct{}{}:
		select { // a turn and the cancel at once: the cancel wins
		case <-c.cancelled:
			<-c.calls
			return ErrCancelled
		default:
			return nil
		}
	case <-c.cancelled:
		return ErrCancelled
	case <-expired:
		return ErrTimeout
	}
}
