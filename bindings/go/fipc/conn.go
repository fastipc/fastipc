package fipc

import (
	"runtime"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/ebitengine/purego"
)

// scratchSize is the room of a connection's receive buffer: Receive and RPCReceive take a message that fits it in one
// call of the library and copy it into a slice of its length; a longer one takes a second call, into a slice made for
// it.
const scratchSize = 4096

// Conn is one connection: two rings in shared memory, one per direction. A server gets one from [Listener.Accept], a
// client from [Connect].
//
// Every method is safe to call from any goroutine, as with a net.Conn: each direction has a mutex of its own, so
// concurrent sends (Send, SendAcquire, SendCommit, RPCSubmit, RPCRespond) run one at a time, and so do concurrent
// receives (Receive, ReceiveInto, ReceiveAcquire, ReceiveRelease, RPCReceive, RPCReceiveInto), while a send and a
// receive run at once. Each call is whole: two goroutines' messages never interleave.
//
// Close ends the connection: the peer gets [ErrDisconnected] once it has received the messages this side completed.
// A Conn that becomes unreachable without Close is closed by the garbage collector, some time later: close it
// yourself to end it at once (a listener accepts its next client only once its last connection is closed).
type Conn struct {
	h        uintptr // the native connection, open until Close
	name     string
	maxPiece int

	closed   atomic.Bool
	cancelMu sync.RWMutex // Cancel holds it for reading, Close for writing: no cancel runs into the native close
	cleanup  runtime.Cleanup

	// Each direction's calls go through its own argument array, so that a call allocates nothing (see ffi). The
	// arguments hold the addresses of Go memory as uintptrs, which the GC neither traces nor keeps alive: the caller's
	// buffer is held in pin for the call (which also makes it escape to the heap, where nothing moves it), and the
	// out-parameters are this Conn's own fields. The two directions sit on cache lines of their own: a sender and a
	// receiver run at once.
	_  [64]byte
	tx struct {
		mu   sync.Mutex // one send at a time; Close holds it while it closes
		args [7]uintptr
		pin  unsafe.Pointer // the caller's message, during a call
		out  unsafe.Pointer // SendAcquire's out-parameter: room in the ring
		id   uint64         // RPCSubmit's out-parameter
	}
	_  [64]byte
	rx struct {
		mu      sync.Mutex // one receive at a time; Close holds it while it closes
		args    [5]uintptr
		pin     unsafe.Pointer // the caller's buffer, during a call
		len     uintptr        // the receives' out-parameter: the message's length
		data    unsafe.Pointer // ReceiveAcquire's out-parameter: the message in the ring
		msg     rpcMsg         // the RPC receives' out-parameter
		scratch []byte         // Receive's and RPCReceive's buffer, made on their first call
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

// Connect connects to the server listening on name and sets the connection up on the calling goroutine, waiting up to
// timeout for a server to listen (it may start later, or be serving another client) and to call [Listener.Accept].
// It returns once the server's Accept has offered the rings, which may be before that Accept returns: messages sent
// meanwhile wait in the ring. A client and its server in one process run on different goroutines. The connection's
// rings have the capacity the server chose.
//
// There is no handle to cancel until it returns: use [Forever] to wait for a server however late it starts, or a
// finite timeout to stay responsive.
//
// Errors: [ErrTimeout] if no server's Accept set a connection up in time; [ErrInvalid] for a bad name, or a server
// with another version of the protocol or running as another user; [ErrNoMemory] if the rings can't be mapped; an
// error wrapping [ErrNoLibrary] if the library can't be loaded.
func Connect(name string, timeout time.Duration) (*Conn, error) {
	if err := load(); err != nil {
		return nil, err
	}
	c, ok := cName(name)
	if !ok {
		return nil, ErrInvalid
	}
	var out uintptr
	r, _, _ := purego.SyscallN(lib.connect, uintptr(unsafe.Pointer(&c[0])), uintptr(unsafe.Pointer(&out)),
		uintptr(millis(timeout)))
	if err := result(r, 0); err != nil {
		return nil, err
	}
	return newConn(out, name), nil
}

// Name returns the name of the listener the connection came through.
func (c *Conn) Name() string { return c.name }

// MaxPiece returns the longest message the zero-copy calls take: the connection's capacity less 64 bytes. The copying
// calls take messages of any size.
func (c *Conn) MaxPiece() int { return c.maxPiece }

// Cancel makes every call of the connection that waits, now or later, return [ErrCancelled]; calls that needn't wait
// still work. A message cancelled halfway is dropped whole. The connection stays up: the peer sees nothing until
// Close. Any goroutine, any time: the way to stop a goroutine that waits on the connection (with a context:
// context.AfterFunc(ctx, conn.Cancel)). Final. After Close, it does nothing.
func (c *Conn) Cancel() {
	c.cancelMu.RLock()
	defer c.cancelMu.RUnlock()
	if !c.closed.Load() {
		purego.SyscallN(lib.cancel, c.h)
	}
}

// Close ends the connection: the peer gets [ErrDisconnected] once it has received the messages this side completed.
// The rings are unmapped: every slice SendAcquire or ReceiveAcquire returned becomes invalid.
//
// It may run while other goroutines are inside calls on the connection: it cancels the connection, so that the calls
// that wait return [ErrCancelled], waits for every call to return, then closes. Calls after Close return [ErrClosed]
// (ReceiveRelease does nothing; Name and MaxPiece still answer); so does a second Close.
func (c *Conn) Close() error {
	if !c.closed.CompareAndSwap(false, true) {
		return ErrClosed
	}
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

// send makes one call of a sending function, its arguments after the connection in args (tx.mu held, tx.args
// filled from 1); pin is the caller's message, held for the call.
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
	return result(c.send(lib.send, p, 4), len(msg))
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
// Errors: [ErrInvalid] without a reservation, or for n out of that range (the reservation stays for a valid n);
// [ErrClosed].
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
	return result(c.send(lib.sendCommit, nil, 2), n)
}

// RPCSubmit sends an RPC request with payload (any size, possibly empty) as Send does, and returns its id: a
// connection numbers its requests from 1. The response arrives as an RPC message with that id.
//
// Errors: as Send's, an empty payload aside.
func (c *Conn) RPCSubmit(opcode uint32, payload []byte, timeout time.Duration) (uint64, error) {
	c.tx.mu.Lock()
	defer c.tx.mu.Unlock()
	if c.closed.Load() {
		return 0, ErrClosed
	}
	p := unsafe.Pointer(unsafe.SliceData(payload))
	a := &c.tx.args
	a[1], a[2], a[3], a[4], a[5] = uintptr(opcode), uintptr(p), uintptr(len(payload)),
		uintptr(unsafe.Pointer(&c.tx.id)), uintptr(millis(timeout))
	if err := result(c.send(lib.rpcSubmit, p, 6), len(payload)); err != nil {
		return 0, err
	}
	return c.tx.id, nil
}

// RPCRespond sends the response to request id, with an opcode (by convention the request's), a status and a payload
// (any size, possibly empty), as Send does.
//
// Errors: as Send's, an empty payload aside.
func (c *Conn) RPCRespond(id uint64, opcode uint32, status int32, payload []byte, timeout time.Duration) error {
	c.tx.mu.Lock()
	defer c.tx.mu.Unlock()
	if c.closed.Load() {
		return ErrClosed
	}
	p := unsafe.Pointer(unsafe.SliceData(payload))
	a := &c.tx.args
	a[1], a[2], a[3], a[4], a[5], a[6] = uintptr(id), uintptr(opcode), uintptr(status), uintptr(p),
		uintptr(len(payload)), uintptr(millis(timeout))
	return result(c.send(lib.rpcRespond, p, 7), len(payload))
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
// received; [ErrCancelled]; [ErrClosed].
func (c *Conn) Receive(timeout time.Duration) ([]byte, error) {
	c.rx.mu.Lock()
	defer c.rx.mu.Unlock()
	if c.closed.Load() {
		return nil, ErrClosed
	}
	if c.rx.scratch == nil {
		c.rx.scratch = make([]byte, scratchSize)
	}
	ms := millis(timeout)
	r := c.recv(c.rx.scratch, ms)
	if int32(r) == codeOK {
		msg := make([]byte, c.rx.len)
		copy(msg, c.rx.scratch)
		return msg, nil
	}
	// Longer than the scratch buffer: the message stays queued, so take it into a slice of its length
	for int32(r) == codeTooLarge {
		msg := make([]byte, c.rx.len)
		if r = c.recv(msg, ms); int32(r) == codeOK {
			return msg[:c.rx.len], nil
		}
	}
	return nil, result(r, int(c.rx.len))
}

// ReceiveInto receives one message into buf and returns its length, as Receive does, without allocating.
//
// Errors: as Receive's, and [ErrTooLarge] if the message is longer than buf: nothing is taken, and the error's Len is
// the room it needs. After an error, the contents of buf are unspecified.
func (c *Conn) ReceiveInto(buf []byte, timeout time.Duration) (int, error) {
	c.rx.mu.Lock()
	defer c.rx.mu.Unlock()
	if c.closed.Load() {
		return 0, ErrClosed
	}
	r := c.recv(buf, millis(timeout))
	if err := result(r, int(c.rx.len)); err != nil {
		return 0, err
	}
	return int(c.rx.len), nil
}

// ReceiveAcquire is a zero-copy receive: it waits up to timeout for a message and returns it in the ring, 16-byte
// aligned. Its room stays taken until [Conn.ReceiveRelease].
//
// The slice points into shared memory and is read-only: writing to it is undefined. It is valid until ReceiveRelease,
// the next receive of any kind on the connection (from any goroutine; it releases the message first) or Close; using
// it after that is undefined (it may crash the program). Keep the Conn reachable while the slice is in use: one the
// garbage collector closes unmaps it. Copy what must outlive it.
//
// Errors: as Receive's, and [ErrTooLarge] for a message in several pieces: the error's Len is its length, and it stays
// queued for Receive or ReceiveInto.
func (c *Conn) ReceiveAcquire(timeout time.Duration) ([]byte, error) {
	c.rx.mu.Lock()
	defer c.rx.mu.Unlock()
	if c.closed.Load() {
		return nil, ErrClosed
	}
	a := &c.rx.args
	a[0], a[1], a[2], a[3] = c.h, uintptr(unsafe.Pointer(&c.rx.data)), uintptr(unsafe.Pointer(&c.rx.len)),
		uintptr(millis(timeout))
	if err := result(ffi(lib.recvAcquire, a[:4]), int(c.rx.len)); err != nil {
		return nil, err
	}
	return unsafe.Slice((*byte)(c.rx.data), c.rx.len), nil
}

// ReceiveRelease is a zero-copy receive, step 2: it frees the room of the message ReceiveAcquire returned, whose
// slice becomes invalid. Without one, or after Close, it does nothing. It doesn't wait.
func (c *Conn) ReceiveRelease() {
	c.rx.mu.Lock()
	defer c.rx.mu.Unlock()
	if !c.closed.Load() {
		c.rx.args[0] = c.h
		ffi(lib.recvRelease, c.rx.args[:1])
	}
}

// rpcRecv is one fipc_rpc_recv into buf (rx.mu held); the header is in rx.msg.
func (c *Conn) rpcRecv(buf []byte, ms int32) uintptr {
	c.rx.pin = unsafe.Pointer(unsafe.SliceData(buf))
	a := &c.rx.args
	a[0], a[1], a[2], a[3], a[4] = c.h, uintptr(c.rx.pin), uintptr(len(buf)), uintptr(unsafe.Pointer(&c.rx.msg)),
		uintptr(ms)
	r := ffi(lib.rpcRecv, a[:5])
	c.rx.pin = nil
	return r
}

// RPCReceive receives one RPC request or response, its payload in a new slice, as Receive does.
//
// Errors: as Receive's, and [ErrInvalid] for a message that isn't a well-formed RPC message (a plain message), which
// is dropped.
func (c *Conn) RPCReceive(timeout time.Duration) (RPCMessage, error) {
	c.rx.mu.Lock()
	defer c.rx.mu.Unlock()
	if c.closed.Load() {
		return RPCMessage{}, ErrClosed
	}
	if c.rx.scratch == nil {
		c.rx.scratch = make([]byte, scratchSize)
	}
	ms := millis(timeout)
	r := c.rpcRecv(c.rx.scratch, ms)
	var payload []byte
	if int32(r) == codeOK {
		payload = make([]byte, c.rx.msg.lenOf())
		copy(payload, c.rx.scratch)
	}
	// Longer than the scratch buffer: the message stays queued, so take it into a slice of its length
	for int32(r) == codeTooLarge {
		payload = make([]byte, c.rx.msg.lenOf())
		r = c.rpcRecv(payload, ms)
	}
	if err := result(r, c.rx.msg.lenOf()); err != nil {
		return RPCMessage{}, err
	}
	h, err := c.rx.msg.header()
	if err != nil {
		return RPCMessage{}, err
	}
	return RPCMessage{ID: h.ID, Kind: h.Kind, Opcode: h.Opcode, Status: h.Status, Payload: payload[:h.Len]}, nil
}

// RPCReceiveInto receives one RPC request or response with its payload in buf, and returns its header (the payload's
// length is its Len), as RPCReceive does, without allocating.
//
// Errors: as RPCReceive's, and [ErrTooLarge] if the payload is longer than buf: nothing is taken, and the error's Len
// is the room it needs.
func (c *Conn) RPCReceiveInto(buf []byte, timeout time.Duration) (RPCHeader, error) {
	c.rx.mu.Lock()
	defer c.rx.mu.Unlock()
	if c.closed.Load() {
		return RPCHeader{}, ErrClosed
	}
	if err := result(c.rpcRecv(buf, millis(timeout)), c.rx.msg.lenOf()); err != nil {
		return RPCHeader{}, err
	}
	return c.rx.msg.header()
}
