package fipc

import (
	"errors"
	"strconv"
)

// The C API's results (fipc_result_t).
const (
	codeOK           = 0
	codeTimeout      = 1
	codeDisconnected = 2
	codeCancelled    = 3
	codeTooLarge     = 4
	codeInvalid      = 5
	codeNoMemory     = 6
	codeAddrInUse    = 7
)

// Error is why a call of the C API didn't succeed: one of its results other than FIPC_OK. Every error of a call is
// one of the variables below ([ErrTimeout], [ErrDisconnected], ...) except [ErrTooLarge]'s, a new *Error that says
// the message's length ([Error.Len]); test them with [errors.Is], which matches an ErrTooLarge of any length:
//
//	var e *fipc.Error
//	if errors.As(err, &e) && e.Len() > 0 { // ErrTooLarge, and the message's length
//		buf = make([]byte, e.Len())
//	}
//
// A timeout or the peer's end is an ordinary result, not a failure of the library: test for it.
type Error struct {
	code int32
	len  int
}

var (
	// ErrTimeout: the timeout ran out (with [NoWait]: there was nothing to do without waiting). The call changed
	// nothing. Its Timeout method reports true.
	ErrTimeout = &Error{code: codeTimeout}
	// ErrDisconnected: the peer ended the connection: it closed it, or its process ended, for any reason. Sends
	// report it at once, receives only after they have delivered every message the peer completed. It is final:
	// close the connection; to talk again, accept or connect a new one.
	ErrDisconnected = &Error{code: codeDisconnected}
	// ErrCancelled: the listener or connection was cancelled (Cancel, or Close on another goroutine), and the call
	// would have to wait.
	ErrCancelled = &Error{code: codeCancelled}
	// ErrTooLarge: the message doesn't fit what the call can take: the caller's buffer, or one piece for zero-copy.
	// The error a call returns is a new *Error whose Len is the message's length (for an RPC message, its
	// payload's); a receive leaves the message queued, so receive it again into Len bytes (or with the copying
	// calls, for a zero-copy receive).
	ErrTooLarge = &Error{code: codeTooLarge}
	// ErrInvalid: a bad argument (a name with a character the C API doesn't take, a capacity that isn't a power of
	// two from 1024 to 2^31, an empty plain message), a call out of order (an accept while the listener's last
	// connection is open, a commit without a reservation), a peer with another version or user, or corrupt ring
	// content.
	ErrInvalid = &Error{code: codeInvalid}
	// ErrNoMemory: memory or another OS resource ran out while setting a connection up.
	ErrNoMemory = &Error{code: codeNoMemory}
	// ErrAddrInUse: another listener holds the name.
	ErrAddrInUse = &Error{code: codeAddrInUse}
)

// ErrClosed is the error of a call on a [Listener] or [Conn] that was closed (by Close, or a second Close). It is
// the binding's own, not a result of the C API.
var ErrClosed = errors.New("fipc: the listener or connection is closed")

// Code returns the C API's result code (FIPC_TIMEOUT is 1, ...).
func (e *Error) Code() int { return int(e.code) }

// Name returns the C API's name of the result ("FIPC_TIMEOUT", ...), as fipc_result_str gives it.
func (e *Error) Name() string {
	switch e.code {
	case codeTimeout:
		return "FIPC_TIMEOUT"
	case codeDisconnected:
		return "FIPC_DISCONNECTED"
	case codeCancelled:
		return "FIPC_CANCELLED"
	case codeTooLarge:
		return "FIPC_TOO_LARGE"
	case codeInvalid:
		return "FIPC_INVALID"
	case codeNoMemory:
		return "FIPC_NO_MEMORY"
	case codeAddrInUse:
		return "FIPC_ADDR_IN_USE"
	}
	return "FIPC_UNKNOWN"
}

// Len returns the message's length in bytes for an [ErrTooLarge] a call returned: the room a receive needs, or what a
// zero-copy send asked for. It is 0 for every other error.
func (e *Error) Len() int { return e.len }

// Timeout reports whether the error is [ErrTimeout], as the errors of package os and net do for theirs.
func (e *Error) Timeout() bool { return e.code == codeTimeout }

// Is reports whether target is an *Error with the same code: errors.Is(err, ErrTooLarge) holds whatever the
// message's length.
func (e *Error) Is(target error) bool {
	t, ok := target.(*Error)
	return ok && t.code == e.code
}

// Error returns the C name and what it means: "FIPC_TIMEOUT: the timeout ran out".
func (e *Error) Error() string {
	name := e.Name()
	switch e.code {
	case codeTimeout:
		return name + ": the timeout ran out"
	case codeDisconnected:
		return name + ": the peer ended the connection"
	case codeCancelled:
		return name + ": the call was cancelled"
	case codeTooLarge:
		if e.len > 0 {
			return name + ": the message (" + strconv.Itoa(e.len) + " bytes) doesn't fit"
		}
		return name + ": the message doesn't fit"
	case codeInvalid:
		return name + ": a bad argument or handle, a call out of order, or an incompatible peer"
	case codeNoMemory:
		return name + ": memory or another OS resource ran out"
	case codeAddrInUse:
		return name + ": another listener holds the name"
	}
	return name + " (" + strconv.Itoa(int(e.code)) + ")"
}

// result is the error a call's result code stands for, nil for FIPC_OK; n is the message's length for
// FIPC_TOO_LARGE. The codes come from a C int: only the low 32 bits of the register are the result.
func result(code uintptr, n int) error {
	switch int32(code) {
	case codeOK:
		return nil
	case codeTimeout:
		return ErrTimeout
	case codeDisconnected:
		return ErrDisconnected
	case codeCancelled:
		return ErrCancelled
	case codeTooLarge:
		return &Error{code: codeTooLarge, len: n}
	case codeNoMemory:
		return ErrNoMemory
	case codeAddrInUse:
		return ErrAddrInUse
	}
	return ErrInvalid // FIPC_INVALID; the library returns no other code
}
