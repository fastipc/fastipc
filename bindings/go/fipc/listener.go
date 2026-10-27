package fipc

import (
	"runtime"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/ebitengine/purego"
)

// Listener is a stream's server: it holds the stream's name, and [Listener.Accept] sets up one client's [Conn] at a
// time, on the calling goroutine.
//
// Every method is safe to call from any goroutine; Accepts run one at a time. Close stops listening and frees the name
// (on Windows, once the connections it accepted are closed too); those connections stay open. A Listener that becomes
// unreachable without Close is closed by the garbage collector, some time later: close it yourself to free the name
// at once.
type Listener struct {
	h    uintptr // the native listener, open until Close
	name string

	closed   atomic.Bool
	cancelMu sync.RWMutex // Cancel holds it for reading, Close for writing: no cancel runs into the native close
	mu       sync.Mutex   // one Accept at a time; Close holds it while it closes
	out      uintptr      // Accept's out-parameter (under mu)
	cleanup  runtime.Cleanup
}

// CreateStream claims name for a stream and listens on it, with rings of capacity bytes each way for every
// connection. It doesn't wait: clients may connect from now on, and Accept sets each one up.
//
// A name is 1-245 characters of [A-Za-z0-9_.-], not starting with '.' or '-', local to the user (Linux, macOS) or to
// the desktop session (Windows). The capacity is a power of two from 1024 to 2^31; a client's connection has the
// server's.
//
// Errors: [ErrAddrInUse] if another stream, broadcast, queue or service holds the name (it is freed when that one's
// handle is closed or its process ends); [ErrInvalid] for a bad name or capacity; an error wrapping [ErrNoLibrary] if
// the library can't be loaded.
func CreateStream(name string, capacity int) (*Listener, error) {
	if err := load(); err != nil {
		return nil, err
	}
	c, ok := cName(name)
	if !ok || capacity < 0 {
		return nil, ErrInvalid
	}
	var out uintptr
	r, _, _ := purego.SyscallN(lib.streamCreate, uintptr(unsafe.Pointer(&c[0])), uintptr(capacity),
		uintptr(unsafe.Pointer(&out)))
	if err := result(r, 0); err != nil {
		return nil, err
	}
	return newListener(out, name), nil
}

// newListener wraps a native listener that nothing else owns.
func newListener(h uintptr, name string) *Listener {
	l := &Listener{h: h, name: name}
	l.cleanup = runtime.AddCleanup(l, closeListener, h)
	return l
}

// closeListener closes a native listener whose Listener became unreachable.
func closeListener(h uintptr) {
	purego.SyscallN(lib.listenerClose, h)
}

// Accept waits up to timeout for a client, sets its connection up on the calling goroutine, and returns it; it may
// already hold the client's first messages. A call that times out in the middle of a client's setup keeps it for the
// next call, so [NoWait] polls (a client then takes one or two calls).
//
// One client at a time: [ErrInvalid] while the connection this listener returned last is open (close it first); a
// client that connects meanwhile waits, within its own timeout. A client with another version of the protocol, or
// running as another user, is refused, and the call goes on waiting.
//
// Errors: [ErrTimeout]; [ErrCancelled] once the listener is cancelled; [ErrInvalid] as above; [ErrNoMemory] once the
// listener couldn't get the memory for a client's rings: it has failed for good (close it, and listen again to go
// on); [ErrClosed].
func (l *Listener) Accept(timeout time.Duration) (*Conn, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed.Load() {
		return nil, ErrClosed
	}
	r, _, _ := purego.SyscallN(lib.streamAccept, l.h, uintptr(unsafe.Pointer(&l.out)), uintptr(millis(timeout)))
	if err := result(r, 0); err != nil {
		return nil, err
	}
	return newConn(l.out, l.name), nil
}

// Cancel makes every Accept that waits, now or later, return [ErrCancelled]; a client whose setup it began is dropped
// (it connects again, within its own timeout). Any goroutine, any time: the way to stop a goroutine that waits in
// Accept. Final. After Close, it does nothing.
func (l *Listener) Cancel() {
	l.cancelMu.RLock()
	defer l.cancelMu.RUnlock()
	if !l.closed.Load() {
		purego.SyscallN(lib.listenerCancel, l.h)
	}
}

// Close stops listening and frees the name (on Windows, once the connections the listener accepted are closed too);
// those connections stay open. A client whose setup an Accept began and didn't finish is dropped (if its ConnectStream
// returned, it gets [ErrDisconnected]).
//
// It may run while another goroutine waits in Accept: it cancels the listener, so that the call returns
// [ErrCancelled], waits for it to return, then closes. Calls after Close return [ErrClosed]; so does a second Close.
func (l *Listener) Close() error {
	if !l.closed.CompareAndSwap(false, true) {
		return ErrClosed
	}
	purego.SyscallN(lib.listenerCancel, l.h) // wakes an Accept that waits
	l.mu.Lock()
	defer l.mu.Unlock()
	l.cancelMu.Lock()
	defer l.cancelMu.Unlock()
	l.cleanup.Stop()
	purego.SyscallN(lib.listenerClose, l.h)
	return nil
}

// Name returns the name the listener listens on.
func (l *Listener) Name() string { return l.name }
