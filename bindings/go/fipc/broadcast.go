package fipc

import (
	"math"
	"strconv"
	"time"
	"unsafe"

	"github.com/ebitengine/purego"
)

// SubscriberRole is how a broadcast's subscriber receives, chosen when it joins ([JoinBroadcast]): [Lossy] or
// [Gated].
type SubscriberRole int32

const (
	// Lossy never holds the publisher back. One that falls a lap behind loses messages: a receive returns an
	// [ErrOverrun] whose Lost says how many, and the next goes on from a recent message.
	Lossy SubscriberRole = 1
	// Gated misses nothing: the publisher's send waits for room the subscriber still holds. One that holds it back
	// for the broadcast's maxStall is dropped from the broadcast: its receives return [ErrUnsubscribed].
	Gated SubscriberRole = 2
)

// String returns "lossy" or "gated".
func (r SubscriberRole) String() string {
	switch r {
	case Lossy:
		return "lossy"
	case Gated:
		return "gated"
	}
	return "SubscriberRole(" + strconv.Itoa(int(r)) + ")"
}

// CreateBroadcast claims name for a broadcast and returns its publisher, the broadcast's one handle: one publisher, up
// to maxSubscribers subscribers (1 to 1024), in any processes of the user. Every message the publisher sends goes to
// every subscriber, in one order, and is stored once, in memory every subscriber maps. It doesn't wait: subscribers
// may join from now on ([JoinBroadcast]), and this process admits each one by itself, on a thread of the library's, so
// no goroutine of the publisher's need wait for them.
//
// The capacity is a power of two from 4096 to 2^31: the broadcast holds up to capacity / 64 messages, of 1 to
// [Conn.MaxPiece] bytes (a quarter of the capacity) each, fewer when longer ones fill the capacity first. A gated
// subscriber holds the publisher back, and a lossy one is lapped, once that much is unread. A gated subscriber that
// holds the publisher back for maxStall is unsubscribed and the publisher goes on; [Forever] (or any negative duration)
// never unsubscribes one, and [NoWait] refuses gated subscribers.
//
// The publisher sends with Send and SendAcquire/SendCommit: [ErrTooLarge] over MaxPiece, and a send waits only for
// gated subscribers; it never returns [ErrDisconnected]. Its receives return [ErrInvalid].
// [Conn.WaitForMembers] counts the joined subscribers, and waits for a number of them: before the first send, for
// those it must reach. The publisher's Close ends the broadcast and frees the name at once.
//
// Errors: [ErrAddrInUse] if another stream, broadcast, queue or service holds the name; [ErrInvalid] for a bad name,
// capacity or maxSubscribers; [ErrNoMemory] if the memory for the whole broadcast can't be had (it is set up now, not
// as subscribers join); an error wrapping [ErrNoLibrary] if the library can't be loaded.
func CreateBroadcast(name string, capacity, maxSubscribers int, maxStall time.Duration) (*Conn, error) {
	if err := load(); err != nil {
		return nil, err
	}
	c, ok := cName(name)
	if !ok || capacity < 0 || maxSubscribers < 0 || maxSubscribers > math.MaxUint32 {
		return nil, ErrInvalid
	}
	var publisher uintptr
	r, _, _ := purego.SyscallN(lib.broadcastCreate, uintptr(unsafe.Pointer(&c[0])), uintptr(capacity),
		uintptr(maxSubscribers), uintptr(millis(maxStall)), uintptr(unsafe.Pointer(&publisher)))
	if err := result(r, 0); err != nil {
		return nil, err
	}
	return newConn(publisher, name), nil
}

// JoinBroadcast joins the broadcast on name as a [Lossy] or [Gated] subscriber and returns its connection, waiting up
// to timeout for the publisher to create the broadcast, to have a free slot (while maxSubscribers are joined, a joiner
// waits for one to leave) and for the publisher's process to admit it. The subscriber receives the messages sent from
// its admission on, which comes before JoinBroadcast returns, with Receive or ReceiveInto (copies), until
// [ErrDisconnected] once the publisher is closed or its process ended, after the messages it published. Its sends
// return [ErrInvalid]; Close leaves the broadcast.
//
// Errors: [ErrTimeout]; [ErrInvalid] for another role, a bad name, a name a queue or a service holds, a publisher of
// another version or user, or a gated subscriber on a broadcast that refuses them; [ErrNoMemory] if the broadcast
// can't be mapped; an error wrapping [ErrNoLibrary] if the library can't be loaded. On a name a stream holds, the join
// learns the kind when the stream's server accepts it, which brings ErrInvalid; while no Accept runs, it waits and
// returns ErrTimeout.
func JoinBroadcast(name string, role SubscriberRole, timeout time.Duration) (*Conn, error) {
	if err := load(); err != nil {
		return nil, err
	}
	c, ok := cName(name)
	if !ok {
		return nil, ErrInvalid
	}
	var out uintptr
	r, _, _ := purego.SyscallN(lib.broadcastJoin, uintptr(unsafe.Pointer(&c[0])), uintptr(role),
		uintptr(unsafe.Pointer(&out)), uintptr(millis(timeout)))
	if err := result(r, 0); err != nil {
		return nil, err
	}
	conn := newConn(out, name)
	conn.subscriber = true
	return conn, nil
}

// WaitForMembers waits up to timeout until at least atLeast subscribers are joined to the broadcast whose publisher c
// is, and returns how many are: with a nil error, and with [ErrTimeout] and [ErrCancelled] too. Joined: admitted, and
// not yet left, dropped or unsubscribed; the subscribers the publisher's sends reach. To send nothing before n
// subscribers are there, call it with n before the first send. An atLeast of 0 returns the count at once, and
// [NoWait] polls. A subscriber that leaves, or whose process ends, is counted until the publisher's process has seen
// its end, soon after.
//
// It runs beside the publisher's sends and other WaitForMembers calls, on any goroutine; Close cancels a call that
// waits, which returns ErrCancelled with the count.
//
// Errors: [ErrTimeout]; [ErrCancelled] once the publisher is cancelled; [ErrInvalid] on a connection other than a
// broadcast's publisher, or for atLeast over the broadcast's maxSubscribers (the count is then 0); [ErrClosed].
func (c *Conn) WaitForMembers(atLeast int, timeout time.Duration) (int, error) {
	c.cancelMu.RLock() // Close waits for it, after its cancel ended the wait
	defer c.cancelMu.RUnlock()
	if c.closed.Load() {
		return 0, ErrClosed
	}
	if atLeast < 0 || atLeast > math.MaxUint32 {
		return 0, ErrInvalid
	}
	var count uint32
	r, _, _ := purego.SyscallN(lib.broadcastWaitForMembers, c.h, uintptr(atLeast), uintptr(unsafe.Pointer(&count)),
		uintptr(millis(timeout)))
	err := result(r, 0)
	if err == ErrInvalid {
		return 0, err
	}
	return int(count), err
}
