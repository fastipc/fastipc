package fipc_test

// The broadcast in one process: the publisher's calls and a broadcast's misuses, a lossy and a gated subscriber, the
// overrun's lost count, the subscriber count and its wait beside sends and other counts, an unsubscribed gated
// subscriber, a broadcast that refuses gated subscribers. Other processes: process_test.go (this
// binary as the publisher or a subscriber) and interop_test.go (the Python binding's broadcast_peer.py).

import (
	"encoding/binary"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/fastipc/fastipc/bindings/go/fipc"
)

// numbered is a message of size bytes that starts with i.
func numbered(i, size int) []byte {
	msg := make([]byte, size)
	binary.LittleEndian.PutUint32(msg, uint32(i))
	return msg
}

// lostOf is the number of messages lost that err reports: an ErrOverrun, or the test fails.
func lostOf(t *testing.T, err error) uint64 {
	t.Helper()
	var e *fipc.Error
	if !errors.As(err, &e) || !errors.Is(err, fipc.ErrOverrun) {
		t.Fatalf("got %v, want ErrOverrun", err)
	}
	return e.Lost()
}

// counted is a WaitForMembers call's result.
type counted struct {
	n   int
	err error
}

// countAside calls publisher.WaitForMembers on a goroutine of its own.
func countAside(publisher *fipc.Conn, atLeast int, timeout time.Duration) <-chan counted {
	ch := make(chan counted, 1)
	go func() {
		n, err := publisher.WaitForMembers(atLeast, timeout)
		ch <- counted{n, err}
	}()
	return ch
}

func TestABroadcastWithoutSubscribers(t *testing.T) {
	guard(t, 30*time.Second)
	name := uniqueName("bc_api")
	for _, c := range []struct{ capacity, maxSubscribers int }{{2048, 1}, {4096, 0}, {4096, 1025}, {5000, 1},
		{-1, 1}, {4096, -1}, {4096, 1 << 32}} {
		_, err := fipc.CreateBroadcast(name, c.capacity, c.maxSubscribers, fipc.Forever)
		is(t, err, fipc.ErrInvalid, fmt.Sprintf("CreateBroadcast(%d, %d)", c.capacity, c.maxSubscribers))
	}
	publisher, err := fipc.CreateBroadcast(name, 4096, 2, fipc.Forever)
	must(t, err)
	if publisher.Name() != name || publisher.MaxPiece() != 1024 {
		t.Fatalf("%s %d", publisher.Name(), publisher.MaxPiece())
	}
	_, err = fipc.CreateBroadcast(name, 4096, 2, fipc.Forever)
	is(t, err, fipc.ErrAddrInUse, "a second CreateBroadcast")
	_, err = fipc.CreateStream(name, 4096)
	is(t, err, fipc.ErrAddrInUse, "a CreateStream on the broadcast's name")
	_, err = fipc.CreateQueue(name, 4096, 1, 1)
	is(t, err, fipc.ErrAddrInUse, "a CreateQueue on the broadcast's name")

	n, err := publisher.WaitForMembers(0, fipc.NoWait)
	if n != 0 || err != nil {
		t.Fatalf("WaitForMembers(0) = %d, %v", n, err)
	}
	n, err = publisher.WaitForMembers(1, fipc.NoWait)
	if n != 0 || !errors.Is(err, fipc.ErrTimeout) {
		t.Fatalf("WaitForMembers(1, NoWait) = %d, %v", n, err)
	}
	started := time.Now()
	n, err = publisher.WaitForMembers(2, 50*time.Millisecond)
	if n != 0 || !errors.Is(err, fipc.ErrTimeout) || time.Since(started) < 40*time.Millisecond {
		t.Fatalf("WaitForMembers(2, 50 ms) = %d, %v after %v", n, err, time.Since(started))
	}
	for _, atLeast := range []int{3, -1, 1 << 32} { // over max_subscribers, or no uint32
		_, err = publisher.WaitForMembers(atLeast, fipc.NoWait)
		is(t, err, fipc.ErrInvalid, fmt.Sprintf("WaitForMembers(%d)", atLeast))
	}

	// What a publisher can't do, and joins a broadcast refuses
	_, err = publisher.Receive(fipc.NoWait)
	is(t, err, fipc.ErrInvalid, "the publisher's Receive")
	_, err = publisher.ReceiveAcquire(fipc.NoWait)
	is(t, err, fipc.ErrInvalid, "the publisher's ReceiveAcquire")
	err = publisher.Send(make([]byte, 1025), fipc.NoWait)
	var e *fipc.Error
	if !errors.As(err, &e) || !errors.Is(err, fipc.ErrTooLarge) || e.Len() != 1025 {
		t.Fatalf("a send over MaxPiece: %v", err)
	}
	_, err = fipc.JoinBroadcast(name, fipc.SubscriberRole(5), fipc.NoWait)
	is(t, err, fipc.ErrInvalid, "a JoinBroadcast with role 5")
	_, err = fipc.JoinBroadcast(name, fipc.SubscriberRole(fipc.Producer), tenSeconds)
	is(t, err, fipc.ErrInvalid, "a JoinBroadcast with a queue's role")
	_, err = fipc.JoinQueue(name, fipc.Consumer, tenSeconds)
	is(t, err, fipc.ErrInvalid, "a JoinQueue on a broadcast")
	_, err = fipc.JoinBroadcast("bad name", fipc.Lossy, fipc.NoWait)
	is(t, err, fipc.ErrInvalid, "a JoinBroadcast with a bad name")

	// The cancels: a wait, now or later, returns ErrCancelled, the count with it
	counting := countAside(publisher, 1, fipc.Forever)
	time.Sleep(50 * time.Millisecond) // let it wait
	publisher.Cancel()
	if c := <-counting; c.n != 0 || !errors.Is(c.err, fipc.ErrCancelled) {
		t.Fatalf("the waiting WaitForMembers: %d, %v", c.n, c.err)
	}
	_, err = publisher.WaitForMembers(1, fipc.Forever)
	is(t, err, fipc.ErrCancelled, "a later WaitForMembers")
	must(t, publisher.Close())
	_, err = publisher.WaitForMembers(0, fipc.NoWait)
	is(t, err, fipc.ErrClosed, "WaitForMembers after Close")
	is(t, publisher.Close(), fipc.ErrClosed, "a second Close")

	// The publisher's Close frees the name at once: a join waits for a new broadcast on it
	_, err = fipc.JoinBroadcast(name, fipc.Lossy, 50*time.Millisecond)
	is(t, err, fipc.ErrTimeout, "a JoinBroadcast after the publisher's Close")
	again, err := fipc.CreateBroadcast(name, 4096, 1, fipc.Forever)
	must(t, err)
	must(t, again.Close())
}

func TestLossyAndGatedSubscribersReceiveEveryMessage(t *testing.T) {
	guard(t, 30*time.Second)
	name := uniqueName("bc_roles")
	publisher, err := fipc.CreateBroadcast(name, 1<<16, 4, fipc.Forever)
	must(t, err)
	lossy, err := fipc.JoinBroadcast(name, fipc.Lossy, tenSeconds)
	must(t, err)
	defer lossy.Close()
	gated, err := fipc.JoinBroadcast(name, fipc.Gated, tenSeconds)
	must(t, err)
	defer gated.Close()
	n, err := publisher.WaitForMembers(2, tenSeconds)
	if n != 2 || err != nil {
		t.Fatalf("WaitForMembers(2) = %d, %v", n, err)
	}
	if lossy.MaxPiece() != 1<<14 || gated.MaxPiece() != publisher.MaxPiece() || lossy.Name() != name {
		t.Fatalf("MaxPiece %d %d %d", lossy.MaxPiece(), gated.MaxPiece(), publisher.MaxPiece())
	}

	words := []string{"hello", "shared", string(pattern(10_000))}
	for _, word := range words {
		must(t, publisher.Send([]byte(word), tenSeconds))
	}
	for _, word := range words {
		msg, err := lossy.Receive(tenSeconds)
		must(t, err)
		if string(msg) != word {
			t.Fatalf("lossy: %d bytes, want %d", len(msg), len(word))
		}
	}
	buf := make([]byte, 16)
	got, err := gated.ReceiveInto(buf, tenSeconds)
	must(t, err)
	if string(buf[:got]) != "hello" {
		t.Fatalf("gated: %q", buf[:got])
	}
	// Copies only: a gated subscriber's message in place could become inaccessible while the slice is read
	_, err = gated.ReceiveAcquire(fipc.NoWait)
	is(t, err, fipc.ErrInvalid, "a gated subscriber's ReceiveAcquire")
	_, err = lossy.ReceiveAcquire(fipc.NoWait)
	is(t, err, fipc.ErrInvalid, "a lossy subscriber's ReceiveAcquire")
	// A message too long for the buffer stays queued
	_, err = gated.ReceiveInto(buf[:4], tenSeconds)
	var e *fipc.Error
	if !errors.As(err, &e) || e.Len() != len("shared") {
		t.Fatalf("gated: %v", err)
	}
	for _, word := range words[1:] {
		msg, err := gated.Receive(tenSeconds)
		must(t, err)
		if string(msg) != word {
			t.Fatalf("gated: %d bytes, want %d", len(msg), len(word))
		}
	}

	// Zero-copy publishing; a broadcast's misuses
	room, err := publisher.SendAcquire(4, tenSeconds)
	must(t, err)
	copy(room, "tick")
	must(t, publisher.SendCommit(4))
	for _, subscriber := range []*fipc.Conn{lossy, gated} {
		msg, err := subscriber.Receive(tenSeconds)
		must(t, err)
		if string(msg) != "tick" {
			t.Fatalf("%q", msg)
		}
	}
	_, err = lossy.Receive(fipc.NoWait)
	is(t, err, fipc.ErrTimeout, "nothing more")
	is(t, gated.Send([]byte("x"), fipc.NoWait), fipc.ErrInvalid, "a subscriber's Send")
	_, err = gated.WaitForMembers(0, fipc.NoWait)
	is(t, err, fipc.ErrInvalid, "a subscriber's WaitForMembers")

	// The publisher's Close ends the broadcast: what it sent is received first
	must(t, publisher.Send([]byte("last"), tenSeconds))
	must(t, publisher.Close())
	for _, subscriber := range []*fipc.Conn{lossy, gated} {
		msg, err := subscriber.Receive(tenSeconds)
		must(t, err)
		if string(msg) != "last" {
			t.Fatalf("%q", msg)
		}
		_, err = subscriber.Receive(tenSeconds)
		is(t, err, fipc.ErrDisconnected, "after the publisher's Close")
	}
}

func TestALappedLossySubscriberGetsErrOverrun(t *testing.T) {
	guard(t, 30*time.Second)
	name := uniqueName("bc_overrun")
	publisher, err := fipc.CreateBroadcast(name, 4096, 1, fipc.Forever)
	must(t, err)
	defer publisher.Close()
	subscriber, err := fipc.JoinBroadcast(name, fipc.Lossy, tenSeconds)
	must(t, err)
	defer subscriber.Close()
	_, err = publisher.WaitForMembers(1, tenSeconds)
	must(t, err)
	// 144 messages of 48 bytes lap the broadcast's 64 cells twice; the subscriber goes on from message 140
	// (docs/protocol.md §9.6), so 0 to 139 are lost
	lap := func() {
		t.Helper()
		for i := range 144 {
			must(t, publisher.Send(numbered(i, 48), fipc.NoWait))
		}
	}
	lap()
	_, overrun := subscriber.Receive(tenSeconds)
	if lostOf(t, overrun) != 140 {
		t.Fatalf("Receive: %v", overrun)
	}
	var e *fipc.Error
	errors.As(overrun, &e)
	if e.Len() != 0 || e.Error() != "FIPC_OVERRUN: 140 messages were lost" {
		t.Fatalf("Len %d, %q", e.Len(), e.Error())
	}
	msg, err := subscriber.Receive(tenSeconds)
	must(t, err)
	if binary.LittleEndian.Uint32(msg) != 140 {
		t.Fatalf("the next message: %d", binary.LittleEndian.Uint32(msg))
	}
	buf := make([]byte, 64)
	for i := 141; i < 144; i++ {
		n, err := subscriber.ReceiveInto(buf, tenSeconds)
		must(t, err)
		if n != 48 || binary.LittleEndian.Uint32(buf) != uint32(i) {
			t.Fatalf("message %d: %d bytes, %d", i, n, binary.LittleEndian.Uint32(buf))
		}
	}
	if e.Lost() != 140 {
		t.Fatalf("the count, after the receives: %d", e.Lost())
	}
	_, err = subscriber.Receive(fipc.NoWait)
	is(t, err, fipc.ErrTimeout, "nothing more")
	if fipc.ErrTimeout.Lost() != 0 || fipc.ErrOverrun.Lost() != 0 {
		t.Fatal("the variables count no messages")
	}

	// ReceiveInto reports it too, and after its ErrTooLarge the message is still there
	lap()
	_, err = subscriber.ReceiveInto(buf, tenSeconds)
	if lostOf(t, err) != 140 {
		t.Fatalf("ReceiveInto: %v", err)
	}
	_, err = subscriber.ReceiveInto(nil, tenSeconds)
	if !errors.As(err, &e) || e.Len() != 48 {
		t.Fatalf("ReceiveInto(nil): %v", err)
	}
	n, err := subscriber.ReceiveInto(buf, tenSeconds)
	must(t, err)
	if n != 48 || binary.LittleEndian.Uint32(buf) != 140 {
		t.Fatalf("after the overrun: %d bytes, %d", n, binary.LittleEndian.Uint32(buf))
	}
}

func TestTheSubscriberCountAndItsWait(t *testing.T) {
	guard(t, 30*time.Second)
	name := uniqueName("bc_count")
	publisher, err := fipc.CreateBroadcast(name, 4096, 2, fipc.Forever)
	must(t, err)
	waiting := countAside(publisher, 1, tenSeconds)
	first, err := fipc.JoinBroadcast(name, fipc.Gated, tenSeconds)
	must(t, err)
	defer first.Close()
	if c := <-waiting; c.n != 1 || c.err != nil {
		t.Fatalf("WaitForMembers(1) = %d, %v", c.n, c.err)
	}
	n, err := publisher.WaitForMembers(2, 50*time.Millisecond)
	if n != 1 || !errors.Is(err, fipc.ErrTimeout) {
		t.Fatalf("WaitForMembers(2, 50 ms) = %d, %v", n, err)
	}

	// A count that waits holds back neither the publisher's sends nor other counts
	both := countAside(publisher, 2, tenSeconds)
	time.Sleep(20 * time.Millisecond)
	must(t, publisher.Send([]byte("x"), tenSeconds))
	if n, err := publisher.WaitForMembers(0, fipc.NoWait); n != 1 || err != nil {
		t.Fatalf("WaitForMembers(0) beside a wait = %d, %v", n, err)
	}
	second, err := fipc.JoinBroadcast(name, fipc.Lossy, tenSeconds)
	must(t, err)
	if c := <-both; c.n != 2 || c.err != nil {
		t.Fatalf("WaitForMembers(2) = %d, %v", c.n, c.err)
	}
	_, err = fipc.JoinBroadcast(name, fipc.Lossy, 50*time.Millisecond)
	is(t, err, fipc.ErrTimeout, "a JoinBroadcast while every slot is held")

	// A subscriber that leaves is counted until the publisher has seen its end, soon after
	must(t, second.Close())
	deadline := time.Now().Add(tenSeconds)
	for {
		n, err := publisher.WaitForMembers(0, fipc.NoWait)
		must(t, err)
		if n == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the subscriber that left is still counted")
		}
		time.Sleep(time.Millisecond)
	}

	// Closing the publisher cancels a count that waits, which returns the count
	pending := countAside(publisher, 2, fipc.Forever)
	time.Sleep(50 * time.Millisecond) // let it wait
	must(t, publisher.Close())
	if c := <-pending; !(c.n == 1 && errors.Is(c.err, fipc.ErrCancelled)) && !errors.Is(c.err, fipc.ErrClosed) {
		t.Fatalf("the count that waited: %d, %v", c.n, c.err)
	}
	msg, err := first.Receive(tenSeconds)
	must(t, err)
	if string(msg) != "x" {
		t.Fatalf("%q", msg)
	}
	_, err = first.Receive(tenSeconds)
	is(t, err, fipc.ErrDisconnected, "after the publisher's Close")
}

func TestAGatedSubscriberThatHoldsThePublisherBackIsUnsubscribed(t *testing.T) {
	guard(t, 30*time.Second)
	name := uniqueName("bc_stall")
	publisher, err := fipc.CreateBroadcast(name, 4096, 1, 100*time.Millisecond)
	must(t, err)
	defer publisher.Close()
	subscriber, err := fipc.JoinBroadcast(name, fipc.Gated, tenSeconds)
	must(t, err)
	defer subscriber.Close()
	_, err = publisher.WaitForMembers(1, tenSeconds)
	must(t, err)
	// The 65th message waits for room the subscriber holds; 100 ms later it is unsubscribed and the publisher goes on
	for i := range 100 {
		must(t, publisher.Send(numbered(i, 48), tenSeconds))
	}
	if n, err := publisher.WaitForMembers(0, fipc.NoWait); n != 0 || err != nil {
		t.Fatalf("WaitForMembers(0) = %d, %v", n, err)
	}
	_, err = subscriber.Receive(fipc.NoWait)
	is(t, err, fipc.ErrUnsubscribed, "Receive")
	_, err = subscriber.ReceiveInto(make([]byte, 64), fipc.NoWait)
	is(t, err, fipc.ErrUnsubscribed, "a later ReceiveInto") // final
	if err.Error() != "FIPC_UNSUBSCRIBED: the subscriber held the publisher back too long and was dropped" {
		t.Fatalf("%q", err.Error())
	}
}

func TestABroadcastThatRefusesGatedSubscribers(t *testing.T) {
	guard(t, 30*time.Second)
	name := uniqueName("bc_no_gated")
	publisher, err := fipc.CreateBroadcast(name, 4096, 1, fipc.NoWait)
	must(t, err)
	defer publisher.Close()
	_, err = fipc.JoinBroadcast(name, fipc.Gated, tenSeconds)
	is(t, err, fipc.ErrInvalid, "a gated JoinBroadcast")
	_, err = fipc.ConnectStream(name, tenSeconds)
	is(t, err, fipc.ErrInvalid, "a ConnectStream to a broadcast")
	lossy, err := fipc.JoinBroadcast(name, fipc.Lossy, tenSeconds)
	must(t, err)
	defer lossy.Close()
	if n, err := publisher.WaitForMembers(1, tenSeconds); n != 1 || err != nil {
		t.Fatalf("WaitForMembers(1) = %d, %v", n, err)
	}
	if fipc.Lossy.String() != "lossy" || fipc.Gated.String() != "gated" ||
		fipc.SubscriberRole(5).String() != "SubscriberRole(5)" {
		t.Fatal("SubscriberRole.String")
	}
}
