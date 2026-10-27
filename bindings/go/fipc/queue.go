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

// QueueRole is a work queue's member's part, chosen when it joins ([JoinQueue]): [Producer] or [Consumer].
// [AnyQueueRole] counts both in [Queue.WaitForMembers].
type QueueRole int32

const (
	// AnyQueueRole counts producers and consumers together in [Queue.WaitForMembers]; no member joins as it.
	AnyQueueRole QueueRole = 0
	// Producer is a work queue's producer: its sends add messages to its ring in the queue, each taken by one
	// consumer. Its receives return [ErrInvalid].
	Producer QueueRole = 3
	// Consumer is a work queue's consumer: its receives (copies, or in place) take a message of any producer, which no
	// other consumer gets. Its sends return [ErrInvalid].
	Consumer QueueRole = 4
)

// String returns "any", "producer" or "consumer".
func (r QueueRole) String() string {
	switch r {
	case AnyQueueRole:
		return "any"
	case Producer:
		return "producer"
	case Consumer:
		return "consumer"
	}
	return "QueueRole(" + strconv.Itoa(int(r)) + ")"
}

// Queue is a work queue, held by its owner ([CreateQueue]). Its producers and consumers, the owner's own included,
// join with [JoinQueue]: the Queue counts them ([Queue.WaitForMembers]) and ends the queue ([Queue.Close]).
//
// A queue lives until Close or the end of its process; dropping the Queue doesn't end it: the garbage collector never
// closes a queue, since its members, in this process and in others, use it without the Queue. A forgotten Close keeps
// the name until the process ends, and a second CreateQueue of the name meanwhile returns [ErrAddrInUse].
//
// Every method is safe to call from any goroutine. Close cancels a WaitForMembers that waits and closes once it has
// returned.
type Queue struct {
	h    uintptr // the native queue, open until Close
	name string

	closed   atomic.Bool
	cancelMu sync.RWMutex // Cancel and WaitForMembers hold it for reading, Close for writing: none runs into the close
}

// CreateQueue claims name for a work queue and returns its handle: up to maxProducers producers and maxConsumers
// consumers (1 to 63 producers, 1 to 64 consumers), in any processes of the user. Each message a producer sends goes to
// one consumer; a consumer takes a message of any producer: the queue promises no order. Messages wait in the queue
// until a consumer takes them: a producer may send before any consumer has joined. It doesn't wait: producers and
// consumers may join from now on ([JoinQueue], the owner's own members too), and this process admits each one by
// itself, on a thread of the library's, so no goroutine of the owner's need wait for them.
//
// The capacity is each producer's ring, a power of two from 4096 to 2^31: a ring holds up to capacity / 64 messages of
// 1 to [Conn.MaxPiece] bytes (a quarter of the capacity) each, fewer when longer ones fill it first.
//
// A message goes again, to another consumer, if the process of the consumer that took it ends before acknowledging it:
// a copying receive acknowledges before it returns (at most once), one in place at its ReceiveRelease (released after
// the work: at least once, so the work must tolerate a duplicate).
//
// The queue ends at [Queue.Close], or when the owner's process ends: from then on a producer's sends return
// [ErrDisconnected], and a consumer receives every message still in the queue, then ErrDisconnected. A producer's or a
// consumer's Close leaves the queue, which goes on. A queue lives until Close or the end of its process; dropping the
// Queue doesn't end it.
//
// Errors: [ErrAddrInUse] if another stream, broadcast, queue or service holds the name; [ErrInvalid] for a bad name,
// capacity, maxProducers or maxConsumers; [ErrNoMemory] if the memory for the whole queue can't be had (it is set up
// now, not as members join); an error wrapping [ErrNoLibrary] if the library can't be loaded.
func CreateQueue(name string, capacity, maxProducers, maxConsumers int) (*Queue, error) {
	if err := load(); err != nil {
		return nil, err
	}
	c, ok := cName(name)
	if !ok || capacity < 0 || maxProducers < 0 || maxProducers > math.MaxUint32 || maxConsumers < 0 ||
		maxConsumers > math.MaxUint32 {
		return nil, ErrInvalid
	}
	var out uintptr
	r, _, _ := purego.SyscallN(lib.queueCreate, uintptr(unsafe.Pointer(&c[0])), uintptr(capacity),
		uintptr(maxProducers), uintptr(maxConsumers), uintptr(unsafe.Pointer(&out)))
	if err := result(r, 0); err != nil {
		return nil, err
	}
	return &Queue{h: out, name: name}, nil
}

// JoinQueue joins the work queue on name as a [Producer] or [Consumer] and returns its connection, waiting up to
// timeout for the owner to create the queue, for a free place of the role (a producer's place is free again once every
// message its last producer sent was taken and released) and for the owner's process to admit it. A producer's sends
// add messages to the queue; a consumer's receives take them, each message going to one consumer, copied (Receive,
// ReceiveInto) or in place (ReceiveAcquire). Once the queue ends, a producer's sends return [ErrDisconnected], and a
// consumer receives every message left in the queue, then ErrDisconnected. Close leaves the queue, which goes on: the
// messages a producer sent stay for the consumers.
//
// Errors: [ErrTimeout]; [ErrInvalid] for another role ([AnyQueueRole] included), a bad name, a name a broadcast or a
// service holds, or an owner of another version or user; [ErrNoMemory] if the queue can't be mapped; an error wrapping
// [ErrNoLibrary] if the library can't be loaded. On a name a stream holds, the join learns the kind when the stream's
// server accepts it, which brings ErrInvalid; while no Accept runs, it waits and returns ErrTimeout.
func JoinQueue(name string, role QueueRole, timeout time.Duration) (*Conn, error) {
	if err := load(); err != nil {
		return nil, err
	}
	c, ok := cName(name)
	if !ok {
		return nil, ErrInvalid
	}
	var out uintptr
	r, _, _ := purego.SyscallN(lib.queueJoin, uintptr(unsafe.Pointer(&c[0])), uintptr(role),
		uintptr(unsafe.Pointer(&out)), uintptr(millis(timeout)))
	if err := result(r, 0); err != nil {
		return nil, err
	}
	return newConn(out, name), nil
}

// Name returns the queue's name.
func (q *Queue) Name() string { return q.name }

// WaitForMembers waits up to timeout until at least atLeast members of role ([Producer] or [Consumer]; [AnyQueueRole]
// counts both) are joined, and returns how many are: with a nil error, and with [ErrTimeout], [ErrCancelled] and
// [ErrDisconnected] too. Joined: admitted, and not yet left. A member that leaves, or whose process ends, is counted
// until the owner's process has seen its end, soon after. An atLeast of 0 returns the count at once, and [NoWait]
// polls. Members join without it: no goroutine need admit them while it waits.
//
// It runs beside every other call, on any goroutine; Close cancels a call that waits, which returns ErrCancelled with
// the count.
//
// Errors: [ErrTimeout]; [ErrCancelled] once the queue is cancelled; [ErrDisconnected] once the queue has ended;
// [ErrInvalid] for another role, or atLeast over the role's maximum (maxProducers, maxConsumers, or their sum for
// AnyQueueRole; the count is then 0); [ErrClosed].
func (q *Queue) WaitForMembers(role QueueRole, atLeast int, timeout time.Duration) (int, error) {
	q.cancelMu.RLock() // Close waits for it, after its cancel ended the wait
	defer q.cancelMu.RUnlock()
	if q.closed.Load() {
		return 0, ErrClosed
	}
	if atLeast < 0 || atLeast > math.MaxUint32 {
		return 0, ErrInvalid
	}
	var count uint32
	r, _, _ := purego.SyscallN(lib.queueWaitForMembers, q.h, uintptr(role), uintptr(atLeast),
		uintptr(unsafe.Pointer(&count)), uintptr(millis(timeout)))
	err := result(r, 0)
	if err == ErrInvalid {
		return 0, err
	}
	return int(count), err
}

// Cancel makes every WaitForMembers that waits, now or later, return [ErrCancelled]. The queue goes on: its members
// and their joins are unaffected. Any goroutine, any time: the way to stop a goroutine that waits in WaitForMembers.
// Final. After Close, it does nothing.
func (q *Queue) Cancel() {
	q.cancelMu.RLock()
	defer q.cancelMu.RUnlock()
	if !q.closed.Load() {
		purego.SyscallN(lib.queueCancel, q.h)
	}
}

// Close ends the queue and frees the name at once: producers' sends return [ErrDisconnected] from then on, and
// consumers receive every message still in the queue, then ErrDisconnected. The members' connections, the owner's own
// included, stay open until their Close.
//
// It may run while other goroutines wait in WaitForMembers: it cancels the queue, so that they return [ErrCancelled],
// waits for them to return, then closes. Calls after Close return [ErrClosed]; so does a second Close.
func (q *Queue) Close() error {
	if !q.closed.CompareAndSwap(false, true) {
		return ErrClosed
	}
	purego.SyscallN(lib.queueCancel, q.h) // wakes a WaitForMembers that waits
	q.cancelMu.Lock()
	defer q.cancelMu.Unlock()
	purego.SyscallN(lib.queueClose, q.h)
	return nil
}
