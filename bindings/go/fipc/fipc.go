package fipc

import (
	"math"
	"strconv"
	"strings"
	"structs"
	"time"
)

// Timeouts: every call that can wait takes one, last. A positive duration waits up to that long, counted in whole
// milliseconds, rounded up; durations from math.MaxInt32 milliseconds (24.8 days) on wait for ever.
const (
	// NoWait doesn't wait: the call does what it can at once, or returns ErrTimeout.
	NoWait time.Duration = 0
	// Forever waits as long as it takes; so does any negative duration.
	Forever time.Duration = -1
)

// millis is timeout as the C API's milliseconds.
func millis(timeout time.Duration) int32 {
	if timeout < 0 {
		return -1
	}
	ms := timeout / time.Millisecond
	if timeout%time.Millisecond != 0 {
		ms++
	}
	if ms >= math.MaxInt32 {
		return -1
	}
	return int32(ms)
}

// cName is name as a NUL-terminated C string; false for a name with a NUL character, which no name may hold.
func cName(name string) ([]byte, bool) {
	if strings.IndexByte(name, 0) >= 0 {
		return nil, false
	}
	c := make([]byte, len(name)+1)
	copy(c, name)
	return c, true
}

// RPCKind says whether an RPC message is a request or a response.
type RPCKind uint32

const (
	// RPCRequest is a request, sent by [Conn.RPCSubmit]: answer it with [Conn.RPCRespond].
	RPCRequest RPCKind = 1
	// RPCResponse is the response to a request, sent by [Conn.RPCRespond].
	RPCResponse RPCKind = 2
)

// String returns "request" or "response".
func (k RPCKind) String() string {
	switch k {
	case RPCRequest:
		return "request"
	case RPCResponse:
		return "response"
	}
	return "RPCKind(" + strconv.FormatUint(uint64(k), 10) + ")"
}

// RPCHeader is an RPC message's header without its payload: what [Conn.RPCReceiveInto] returns, the payload being in
// the caller's buffer.
type RPCHeader struct {
	ID     uint64  // the request's id, echoed in its response; a connection numbers the requests it submits from 1
	Kind   RPCKind // a request or a response
	Opcode uint32  // the application's: what the request asks for, by convention echoed in its response
	Status int32   // the application's: 0 in requests, a response's outcome
	Len    int     // the payload's length in bytes (it may be 0)
}

// RPCMessage is an RPC message with its payload: what [Conn.RPCReceive] returns.
type RPCMessage struct {
	ID      uint64  // the request's id, echoed in its response; a connection numbers the requests it submits from 1
	Kind    RPCKind // a request or a response
	Opcode  uint32  // the application's: what the request asks for, by convention echoed in its response
	Status  int32   // the application's: 0 in requests, a response's outcome
	Payload []byte  // the payload (it may be empty)
}

// rpcMsg is fipc_rpc_msg_t, 32 bytes.
type rpcMsg struct {
	_        structs.HostLayout
	id       uint64
	kind     uint32
	opcode   uint32
	status   int32
	reserved uint32
	len      uint64
}

// header is the header the C API filled; ErrInvalid for a kind it doesn't define (the library drops such a message
// itself, so this doesn't happen).
func (m *rpcMsg) header() (RPCHeader, error) {
	kind := RPCKind(m.kind)
	if kind != RPCRequest && kind != RPCResponse || m.len > math.MaxInt {
		return RPCHeader{}, ErrInvalid
	}
	return RPCHeader{ID: m.id, Kind: kind, Opcode: m.opcode, Status: m.status, Len: int(m.len)}, nil
}

// lenOf is an RPC payload's length as an int (FIPC_TOO_LARGE's room).
func (m *rpcMsg) lenOf() int {
	if m.len > math.MaxInt {
		return math.MaxInt
	}
	return int(m.len)
}
