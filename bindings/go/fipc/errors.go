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
	codeOverrun      = 8
	codeUnsubscribed = 9
	codeAbandoned    = 10
)

// Error is why a call of the C API didn't succeed: one of its results other than FIPC_OK. Every error of a call is
// one of the variables below ([ErrTimeout], [ErrDisconnected], ...) except [ErrTooLarge]'s, a new *Error that says
// the message's length ([Error.Len]), and [ErrOverrun]'s, a new *Error that says how many messages were lost
// ([Error.Lost]); test them with [errors.Is], which matches an ErrTooLarge of any length and an ErrOverrun of any
// count:
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
	lost uint64
}

var (
	// ErrTimeout: the timeout ran out (with [NoWait]: there was nothing to do without waiting). The call changed
	// nothing. Its Timeout method reports true.
	ErrTimeout = &Error{code: codeTimeout}
	// ErrDisconnected: the peer ended the connection: it closed it, or its process ended, for any reason. Sends
	// report it at once, receives only after they have delivered every message the peer completed. It is final:
	// close the connection; to talk again, accept or connect a new one.
	ErrDisconnected = &Error{code: codeDisconnected}
	// ErrCancelled: the listener, connection, queue or service was cancelled (Cancel, Queue.Cancel, Service.Cancel, or
	// Close on another goroutine), and the call would have to wait.
	ErrCancelled = &Error{code: codeCancelled}
	// ErrTooLarge: the message doesn't fit what the call can take: the caller's buffer, or one piece for zero-copy and
	// on a broadcast, a queue or a service.
	// The error a call returns is a new *Error whose Len is the message's length; a receive leaves the message
	// queued, so receive it again into Len bytes (or with the copying calls, for a zero-copy receive).
	ErrTooLarge = &Error{code: codeTooLarge}
	// ErrInvalid: a bad argument (a name with a character the C API doesn't take, a capacity that isn't a power of two
	// from 1024 (a stream's) or 4096 (a broadcast's, a queue's or a service's) to 2^31, an empty message), a call out
	// of order (an accept while the listener's last connection is open, a commit without a reservation, a server's
	// receive while it holds a request or its reply without one, a Call while a reply to a Send is owed), a call a
	// connection's kind doesn't take (a receive on a publisher or a producer, a send on a subscriber or a consumer, a
	// Call on anything but a service's client), a peer with another version or user, or corrupt ring content.
	ErrInvalid = &Error{code: codeInvalid}
	// ErrNoMemory: memory or another OS resource ran out while setting a connection up.
	ErrNoMemory = &Error{code: codeNoMemory}
	// ErrAddrInUse: another stream, broadcast, queue or service holds the name.
	ErrAddrInUse = &Error{code: codeAddrInUse}
	// ErrOverrun: a lossy subscriber of a broadcast fell a lap behind, and messages were lost. The error a
	// receive returns is a new *Error whose Lost is how many (at least 1); the next receive goes on from a recent
	// message.
	ErrOverrun = &Error{code: codeOverrun}
	// ErrUnsubscribed: a gated subscriber of a broadcast held the publisher back for the broadcast's maxStall and
	// was dropped from the broadcast. It is final: every later receive returns it; close the connection.
	ErrUnsubscribed = &Error{code: codeUnsubscribed}
	// ErrAbandoned: a service's client: the server that took the request ended before it replied. The request is
	// answered (the next receive returns the next reply); it may or may not have been carried out.
	ErrAbandoned = &Error{code: codeAbandoned}
)

// ErrClosed is the error of a call on a [Listener], [Conn], [Queue] or [Service] that was closed (by Close, or a second
// Close). It is the binding's own, not a result of the C API.
var ErrClosed = errors.New("fipc: the listener, connection, queue or service is closed")

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
	case codeOverrun:
		return "FIPC_OVERRUN"
	case codeUnsubscribed:
		return "FIPC_UNSUBSCRIBED"
	case codeAbandoned:
		return "FIPC_ABANDONED"
	}
	return "FIPC_UNKNOWN"
}

// Len returns the message's length in bytes for an [ErrTooLarge] a call returned: the room a receive needs, or what a
// zero-copy send asked for. It is 0 for every other error.
func (e *Error) Len() int { return e.len }

// Lost returns the number of messages a lossy subscriber lost for an [ErrOverrun] a receive returned (at least 1). It
// is 0 for every other error.
func (e *Error) Lost() uint64 { return e.lost }

// Timeout reports whether the error is [ErrTimeout], as the errors of package os and net do for theirs.
func (e *Error) Timeout() bool { return e.code == codeTimeout }

// Is reports whether target is an *Error with the same code: errors.Is(err, ErrTooLarge) holds whatever the
// message's length, errors.Is(err, ErrOverrun) whatever the number of messages lost.
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
		return name + ": another stream, broadcast, queue or service holds the name"
	case codeOverrun:
		if e.lost == 1 {
			return name + ": 1 message was lost"
		} else if e.lost > 1 {
			return name + ": " + strconv.FormatUint(e.lost, 10) + " messages were lost"
		}
		return name + ": messages were lost"
	case codeUnsubscribed:
		return name + ": the subscriber held the publisher back too long and was dropped"
	case codeAbandoned:
		return name + ": the server that took the request ended before it replied"
	}
	return name + " (" + strconv.Itoa(int(e.code)) + ")"
}

// result is the error a call's result code stands for, nil for FIPC_OK. n is the call's length out-parameter: the
// message's length for FIPC_TOO_LARGE, the number of messages lost for FIPC_OVERRUN (a size_t carried in an int;
// converting it back to uint64 keeps every bit). The C function returns an int, so only the low 32 bits of code are
// the result.
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
	case codeOverrun:
		return &Error{code: codeOverrun, lost: uint64(n)}
	case codeUnsubscribed:
		return ErrUnsubscribed
	case codeAbandoned:
		return ErrAbandoned
	}
	return ErrInvalid // FIPC_INVALID; the library returns no other code
}
