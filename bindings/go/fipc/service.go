package fipc

import (
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/ebitengine/purego"
)

// ServiceRole is a service's member's part, chosen when it joins ([JoinService]): [Client] or [Server].
// [AnyServiceRole] counts both in [Service.WaitForMembers].
type ServiceRole int32

const (
	// AnyServiceRole counts clients and servers together in [Service.WaitForMembers]; no member joins as it.
	AnyServiceRole ServiceRole = 0
	// Client is a service's client: its sends are requests, and its receives their replies, in the order of its
	// requests; [Conn.Call] does both.
	Client ServiceRole = 5
	// Server is a service's server: its receive takes the next request of any client and holds it until its next
	// send, the reply, which never waits.
	Server ServiceRole = 6
)

// String returns "any", "client" or "server".
func (r ServiceRole) String() string {
	switch r {
	case AnyServiceRole:
		return "any"
	case Client:
		return "client"
	case Server:
		return "server"
	}
	return "ServiceRole(" + strconv.Itoa(int(r)) + ")"
}

// Service is a service, held by its owner ([CreateService]). Its clients and servers, the owner's own included, join
// with [JoinService]: the Service counts them ([Service.WaitForMembers]) and ends the service ([Service.Close]).
//
// A service lives until Close or the end of its process; dropping the Service doesn't end it: the garbage collector
// never closes a service, since its members, in this process and in others, use it without the Service. A forgotten
// Close keeps the name until the process ends, and a second CreateService of the name meanwhile returns
// [ErrAddrInUse].
//
// Every method is safe to call from any goroutine. Close cancels a WaitForMembers that waits and closes once it has
// returned.
type Service struct {
	h    uintptr // the native service, open until Close
	name string

	closed   atomic.Bool
	cancelMu sync.RWMutex // Cancel and WaitForMembers hold it for reading, Close for writing: none runs into the close
}

// CreateService claims name for a service and returns its handle: up to maxClients clients and maxServers servers (1
// to 64 each), in any processes of the user. Each request a client sends goes to one server, and the server that took
// it replies to it: the reply goes back to the client that sent it. A server waits for the requests of every client at
// once, and the servers share the load: each takes the next request of any client. It doesn't wait: clients and
// servers may join from now on ([JoinService], the owner's own members too), and this process admits each one by
// itself, on a thread of the library's, so no goroutine of the owner's need wait for them.
//
// The capacity is each client's ring, a power of two from 4096 to 2^31, which holds its requests and the room for their
// replies, so a server never waits for a client. Requests and replies are of one piece, 1 to maxMessage bytes (1 to a
// quarter of the capacity; [Conn.MaxPiece]). A request takes round64(its length) + round64(maxMessage) bytes of its
// client's ring, from its send until its reply has been received (round64 rounds up to a multiple of 64), and a ring
// holds at most capacity / 64 requests.
//
// A client's receive returns the reply to its oldest request whose reply it hasn't received yet, so the n-th reply it
// receives answers its n-th request, whichever server replies first. A request is answered once: by its reply, or with
// [ErrAbandoned] if the server that took it ended before it replied (it closed its connection, or its process ended);
// an abandoned request may or may not have been carried out, and the service never sends it again. Not covered: a
// server that hangs while its process runs (there is no timeout: its client waits, within its own timeouts), and the
// owner's process, with which the service ends.
//
// The service ends at [Service.Close], or when the owner's process ends: from then on clients' sends and servers'
// receives return [ErrDisconnected], a client receives the replies that came before the end, then ErrDisconnected, and
// a server's reply returns ErrDisconnected. A client's or a server's Close leaves the service, which goes on. A service
// lives until Close or the end of its process; dropping the Service doesn't end it.
//
// Errors: [ErrAddrInUse] if another stream, broadcast, queue or service holds the name; [ErrInvalid] for a bad name,
// capacity, maxMessage, maxClients or maxServers; [ErrNoMemory] if the memory for the whole service can't be had (it
// is set up now, not as members join); an error wrapping [ErrNoLibrary] if the library can't be loaded.
func CreateService(name string, capacity, maxMessage, maxClients, maxServers int) (*Service, error) {
	if err := load(); err != nil {
		return nil, err
	}
	c, ok := cName(name)
	if !ok || capacity < 0 || maxMessage < 0 || maxClients < 0 || maxClients > math.MaxUint32 || maxServers < 0 ||
		maxServers > math.MaxUint32 {
		return nil, ErrInvalid
	}
	var out uintptr
	r, _, _ := purego.SyscallN(lib.serviceCreate, uintptr(unsafe.Pointer(&c[0])), uintptr(capacity),
		uintptr(maxMessage), uintptr(maxClients), uintptr(maxServers), uintptr(unsafe.Pointer(&out)))
	if err := result(r, 0); err != nil {
		return nil, err
	}
	return &Service{h: out, name: name}, nil
}

// JoinService joins the service on name as a [Client] or [Server] and returns its connection, waiting up to timeout
// for the owner to create the service, for a free place of the role (a client's place is free again once its last
// client has left and every request of it that a server took has been answered) and for the owner's process to admit
// it.
//
// A client's sends (Send, SendAcquire/SendCommit) are requests, and its receives (Receive, ReceiveInto, ReceiveAcquire)
// their replies, in the order of its requests; [Conn.Call] does both. A server's receive takes the next request of any
// client and holds it until its next send, the reply, which never waits. Clients wait while no server is joined, and
// servers while no client is. A client's Close leaves the service: its requests that no server took are dropped, and
// the replies to those already taken are lost. A server's Close leaves it too, and a request it still holds is
// answered [ErrAbandoned]. Once the service ends, clients' sends and servers' receives return [ErrDisconnected], a
// client receives the replies that came before the end, then ErrDisconnected, and a server's reply returns
// ErrDisconnected.
//
// Errors: [ErrTimeout]; [ErrInvalid] for another role ([AnyServiceRole] included), a bad name, a name a broadcast or a
// queue holds, or an owner of another version or user; [ErrNoMemory] if the service can't be mapped; an error
// wrapping [ErrNoLibrary] if the library can't be loaded. On a name a stream holds, the join learns the kind when the
// stream's server accepts it, which brings ErrInvalid; while no Accept runs, it waits and returns ErrTimeout.
func JoinService(name string, role ServiceRole, timeout time.Duration) (*Conn, error) {
	if err := load(); err != nil {
		return nil, err
	}
	c, ok := cName(name)
	if !ok {
		return nil, ErrInvalid
	}
	var out uintptr
	r, _, _ := purego.SyscallN(lib.serviceJoin, uintptr(unsafe.Pointer(&c[0])), uintptr(role),
		uintptr(unsafe.Pointer(&out)), uintptr(millis(timeout)))
	if err := result(r, 0); err != nil {
		return nil, err
	}
	conn := newConn(out, name)
	if role == Client {
		conn.client = true
		conn.calls = make(chan struct{}, 1)
		conn.cancelled = make(chan struct{})
	}
	return conn, nil
}

// Name returns the service's name.
func (s *Service) Name() string { return s.name }

// WaitForMembers waits up to timeout until at least atLeast members of role ([Client] or [Server]; [AnyServiceRole]
// counts both) are joined, and returns how many are: with a nil error, and with [ErrTimeout], [ErrCancelled] and
// [ErrDisconnected] too. Joined: admitted, and not yet left. A member that leaves, or whose process ends, is counted
// until the owner's process has seen its end, soon after. An atLeast of 0 returns the count at once, and [NoWait]
// polls. Members join without it: no goroutine need admit them while it waits.
//
// It runs beside every other call, on any goroutine; Close cancels a call that waits, which returns ErrCancelled with
// the count.
//
// Errors: [ErrTimeout]; [ErrCancelled] once the service is cancelled; [ErrDisconnected] once the service has ended;
// [ErrInvalid] for another role, or atLeast over the role's maximum (maxClients, maxServers, or their sum for
// AnyServiceRole; the count is then 0); [ErrClosed].
func (s *Service) WaitForMembers(role ServiceRole, atLeast int, timeout time.Duration) (int, error) {
	s.cancelMu.RLock() // Close waits for it, after its cancel ended the wait
	defer s.cancelMu.RUnlock()
	if s.closed.Load() {
		return 0, ErrClosed
	}
	if atLeast < 0 || atLeast > math.MaxUint32 {
		return 0, ErrInvalid
	}
	var count uint32
	r, _, _ := purego.SyscallN(lib.serviceWaitForMembers, s.h, uintptr(role), uintptr(atLeast),
		uintptr(unsafe.Pointer(&count)), uintptr(millis(timeout)))
	err := result(r, 0)
	if err == ErrInvalid {
		return 0, err
	}
	return int(count), err
}

// Cancel makes every WaitForMembers that waits, now or later, return [ErrCancelled]. The service goes on: its members
// and their joins are unaffected. Any goroutine, any time: the way to stop a goroutine that waits in WaitForMembers.
// Final. After Close, it does nothing.
func (s *Service) Cancel() {
	s.cancelMu.RLock()
	defer s.cancelMu.RUnlock()
	if !s.closed.Load() {
		purego.SyscallN(lib.serviceCancel, s.h)
	}
}

// Close ends the service and frees the name at once: clients' sends and servers' receives return [ErrDisconnected]
// from then on, a client receives the replies that came before the end, then ErrDisconnected, and a server's reply
// returns ErrDisconnected. The members' connections, the owner's own included, stay open until their Close.
//
// It may run while other goroutines wait in WaitForMembers: it cancels the service, so that they return
// [ErrCancelled], waits for them to return, then closes. Calls after Close return [ErrClosed]; so does a second Close.
func (s *Service) Close() error {
	if !s.closed.CompareAndSwap(false, true) {
		return ErrClosed
	}
	purego.SyscallN(lib.serviceCancel, s.h) // wakes a WaitForMembers that waits
	s.cancelMu.Lock()
	defer s.cancelMu.Unlock()
	purego.SyscallN(lib.serviceClose, s.h)
	return nil
}
