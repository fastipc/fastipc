package fipc_test

// The work queue in one process: producers and consumers on goroutines, each message delivered once; a producer and a
// consumer of the owner's and what each role refuses; the member count and its wait, its cancel, the queue's end at
// its Close; a queue the garbage collector never ends; consumers racing for messages longer than Receive's first
// buffer; a queue's misuses. Other processes: process_test.go (this binary as a producer or a consumer),
// interop_test.go (the Python binding's queue_peer.py) and examples_test.go.

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/fastipc/fastipc/bindings/go/fipc"
)

// queued is producer p's message i, length bytes (at least 16): the two numbers, little-endian, then a pattern.
func queued(p, i, length int) []byte {
	msg := pattern(length)
	binary.LittleEndian.PutUint64(msg, uint64(p))
	binary.LittleEndian.PutUint64(msg[8:], uint64(i))
	return msg
}

// checkEachOnce checks that the consumers received each of producers x count messages exactly once and whole
// (message i is length(i) bytes long), in any order: the queue promises none.
func checkEachOnce(t *testing.T, received [][][]byte, producers, count int, length func(int) int) {
	t.Helper()
	seen := make(map[[2]int]bool)
	for k, messages := range received {
		for _, msg := range messages {
			if len(msg) < 16 {
				t.Fatalf("consumer %d: a message of %d bytes", k, len(msg))
			}
			p, i := int(binary.LittleEndian.Uint64(msg)), int(binary.LittleEndian.Uint64(msg[8:]))
			switch {
			case p < 0 || p >= producers || i < 0 || i >= count:
				t.Fatalf("consumer %d: producer %d, message %d", k, p, i)
			case !bytes.Equal(msg, queued(p, i, length(i))):
				t.Fatalf("consumer %d: producer %d's message %d differs (%d bytes)", k, p, i, len(msg))
			case seen[[2]int{p, i}]:
				t.Fatalf("producer %d's message %d received twice", p, i)
			}
			seen[[2]int{p, i}] = true
		}
	}
	if len(seen) != producers*count {
		t.Fatalf("received %d of %d messages", len(seen), producers*count)
	}
}

// receiveAll receives on consumer until the queue ends, one of three ways: 0 Receive, 1 ReceiveInto a buffer of
// MaxPiece bytes, 2 ReceiveAcquire, copied, then ReceiveRelease. It returns the messages, and an error other than the
// end.
func receiveAll(consumer *fipc.Conn, how int) ([][]byte, error) {
	var messages [][]byte
	buf := make([]byte, consumer.MaxPiece())
	for {
		var msg []byte
		var err error
		switch how {
		case 0:
			msg, err = consumer.Receive(tenSeconds)
		case 1:
			var n int
			if n, err = consumer.ReceiveInto(buf, tenSeconds); err == nil {
				msg = bytes.Clone(buf[:n])
			}
		default:
			var held []byte
			if held, err = consumer.ReceiveAcquire(tenSeconds); err == nil {
				msg = bytes.Clone(held)
				consumer.ReceiveRelease()
			}
		}
		if errors.Is(err, fipc.ErrDisconnected) {
			return messages, nil
		} else if err != nil {
			return messages, fmt.Errorf("receive %d: %w", how, err)
		}
		messages = append(messages, msg)
	}
}

type consumed struct {
	messages [][]byte
	err      error
}

// consumeAside receives on each consumer on a goroutine of its own until the queue ends, consumer k the way k % 3
// says (receiveAll).
func consumeAside(consumers []*fipc.Conn) []chan consumed {
	results := make([]chan consumed, len(consumers))
	for k, consumer := range consumers {
		results[k] = make(chan consumed, 1)
		go func() {
			messages, err := receiveAll(consumer, k%3)
			results[k] <- consumed{messages, err}
		}()
	}
	return results
}

// joinMember joins the queue on name as role, closed when the test ends.
func joinMember(t *testing.T, name string, role fipc.QueueRole) *fipc.Conn {
	t.Helper()
	member, err := fipc.JoinQueue(name, role, tenSeconds)
	must(t, err)
	t.Cleanup(func() { member.Close() })
	return member
}

// joinConsumers joins n consumers to the queue on name, closed when the test ends.
func joinConsumers(t *testing.T, name string, n int) []*fipc.Conn {
	t.Helper()
	consumers := make([]*fipc.Conn, n)
	for k := range consumers {
		consumers[k] = joinMember(t, name, fipc.Consumer)
	}
	return consumers
}

// collect waits for each consumer's messages; a consumer's error fails the test.
func collect(t *testing.T, results []chan consumed) [][][]byte {
	t.Helper()
	received := make([][][]byte, len(results))
	for k, result := range results {
		r := <-result
		if r.err != nil {
			t.Fatalf("consumer %d: %v", k, r.err)
		}
		received[k] = r.messages
	}
	return received
}

// membersAside calls queue.WaitForMembers on a goroutine of its own.
func membersAside(queue *fipc.Queue, role fipc.QueueRole, atLeast int, timeout time.Duration) <-chan counted {
	ch := make(chan counted, 1)
	go func() {
		n, err := queue.WaitForMembers(role, atLeast, timeout)
		ch <- counted{n, err}
	}()
	return ch
}

func TestProducersAndConsumersOnGoroutinesReceiveEachMessageOnce(t *testing.T) {
	guard(t, 120*time.Second)
	const producers, count = 4, 2000
	name := uniqueName("q_goroutines")
	queue, err := fipc.CreateQueue(name, 1<<16, producers, 4)
	must(t, err)
	defer queue.Close()
	length := func(i int) int { return 16 + i*7%200 }
	results := consumeAside(joinConsumers(t, name, 4))
	sent := make(chan error, producers)
	for p := range producers {
		go func() {
			producer, err := fipc.JoinQueue(name, fipc.Producer, tenSeconds)
			if err != nil {
				sent <- err
				return
			}
			defer producer.Close() // leaves the queue; its messages stay for the consumers
			for i := range count {
				if err := producer.Send(queued(p, i, length(i)), tenSeconds); err != nil {
					sent <- fmt.Errorf("producer %d, message %d: %w", p, i, err)
					return
				}
			}
			sent <- nil
		}()
	}
	for range producers {
		must(t, <-sent)
	}
	must(t, queue.Close()) // ends the queue: the consumers receive what is left, then ErrDisconnected
	checkEachOnce(t, collect(t, results), producers, count, length)
}

func TestAProducerAndAConsumerOfTheOwner(t *testing.T) {
	guard(t, 30*time.Second)
	name := uniqueName("q_roles")
	queue, err := fipc.CreateQueue(name, 4096, 1, 1)
	must(t, err)
	defer queue.Close()
	consumer := joinMember(t, name, fipc.Consumer) // the owner's process admits it: no goroutine waits for it
	producer := joinMember(t, name, fipc.Producer)
	if producer.MaxPiece() != 1024 || consumer.MaxPiece() != 1024 { // a quarter of the capacity
		t.Fatalf("MaxPiece %d %d", producer.MaxPiece(), consumer.MaxPiece())
	}
	if producer.Name() != name || consumer.Name() != name || queue.Name() != name {
		t.Fatalf("names %q %q %q", producer.Name(), consumer.Name(), queue.Name())
	}
	_, err = consumer.Receive(fipc.NoWait)
	is(t, err, fipc.ErrTimeout, "Receive from an empty queue")

	must(t, producer.Send([]byte("hello"), tenSeconds))
	slot, err := producer.SendAcquire(9, tenSeconds)
	must(t, err)
	copy(slot, "zero-copy")
	must(t, producer.SendCommit(9))
	long := bytes.Repeat([]byte{3}, 1000)
	must(t, producer.Send(long, tenSeconds))
	// The queue promises no order: take the three, then check what came
	var got [][]byte
	msg, err := consumer.Receive(tenSeconds)
	must(t, err)
	got = append(got, msg)
	held, err := consumer.ReceiveAcquire(tenSeconds) // in place, until ReceiveRelease
	must(t, err)
	got = append(got, bytes.Clone(held))
	consumer.ReceiveRelease()
	buf := make([]byte, 16)
	n, err := consumer.ReceiveInto(buf, tenSeconds)
	var e *fipc.Error
	if errors.As(err, &e) && errors.Is(err, fipc.ErrTooLarge) { // the long one: left queued
		if e.Len() != len(long) {
			t.Fatalf("ReceiveInto a short buffer: %v", err)
		}
		msg, err = consumer.Receive(tenSeconds)
		must(t, err)
		got = append(got, msg)
	} else {
		must(t, err)
		got = append(got, bytes.Clone(buf[:n]))
	}
	want := map[string]bool{"hello": true, "zero-copy": true, string(long): true}
	for _, msg := range got {
		if !want[string(msg)] {
			t.Fatalf("received %d bytes %q, twice or not sent", len(msg), msg[:min(len(msg), 16)])
		}
		delete(want, string(msg))
	}

	// What each role refuses
	tooLong := make([]byte, producer.MaxPiece()+1)
	err = producer.Send(tooLong, fipc.NoWait)
	if !errors.As(err, &e) || !errors.Is(err, fipc.ErrTooLarge) || e.Len() != len(tooLong) {
		t.Fatalf("Send over MaxPiece: %v", err)
	}
	_, err = producer.SendAcquire(len(tooLong), fipc.NoWait)
	is(t, err, fipc.ErrTooLarge, "SendAcquire over MaxPiece")
	_, err = producer.Receive(fipc.NoWait)
	is(t, err, fipc.ErrInvalid, "a producer's Receive")
	_, err = producer.ReceiveAcquire(fipc.NoWait)
	is(t, err, fipc.ErrInvalid, "a producer's ReceiveAcquire")
	is(t, consumer.Send([]byte("x"), fipc.NoWait), fipc.ErrInvalid, "a consumer's Send")
	_, err = consumer.SendAcquire(1, fipc.NoWait)
	is(t, err, fipc.ErrInvalid, "a consumer's SendAcquire")
	for _, member := range []*fipc.Conn{producer, consumer} {
		n, err := member.WaitForMembers(0, fipc.NoWait)
		if n != 0 || !errors.Is(err, fipc.ErrInvalid) {
			t.Fatalf("a member's WaitForMembers: %d, %v", n, err)
		}
	}

	// The queue's Close ends it; the consumer receives what is left first
	must(t, producer.Send([]byte("last"), tenSeconds))
	must(t, queue.Close())
	is(t, producer.Send([]byte("after the end"), tenSeconds), fipc.ErrDisconnected, "Send after the end")
	if msg, err = consumer.Receive(tenSeconds); err != nil || string(msg) != "last" {
		t.Fatalf("Receive after the end: %q, %v", msg, err)
	}
	_, err = consumer.Receive(tenSeconds)
	is(t, err, fipc.ErrDisconnected, "Receive once the queue is empty")
}

// The examples' exchange in one goroutine: the owner's producer sends before any consumer has joined, the owner waits
// for a consumer, closes its producer and ends the queue; the consumer then receives the jobs.
func TestTheExamplesExchangeOnOneGoroutine(t *testing.T) {
	guard(t, 30*time.Second)
	name := uniqueName("q_exchange")
	queue, err := fipc.CreateQueue(name, 4096, 1, 1)
	must(t, err)
	defer queue.Close()
	producer := joinMember(t, name, fipc.Producer)
	for i := 1; i <= 3; i++ {
		must(t, producer.Send(queueJob(i), fipc.NoWait)) // waits in the queue
	}
	consumer := joinMember(t, name, fipc.Consumer)
	n, err := queue.WaitForMembers(fipc.Consumer, 1, tenSeconds)
	if n != 1 || err != nil {
		t.Fatalf("WaitForMembers(Consumer, 1) = %d, %v", n, err)
	}
	must(t, producer.Close())
	must(t, queue.Close())
	jobs, err := receiveJobs(consumer)
	must(t, err)
	must(t, checkJobs([][]string{jobs}, 1, 3))
}

func TestTheMemberCountItsWaitAndItsCancel(t *testing.T) {
	guard(t, 30*time.Second)
	name := uniqueName("q_count")
	queue, err := fipc.CreateQueue(name, 4096, 2, 3)
	must(t, err)
	defer queue.Close()
	for _, role := range []fipc.QueueRole{fipc.AnyQueueRole, fipc.Producer, fipc.Consumer} {
		n, err := queue.WaitForMembers(role, 0, fipc.NoWait)
		if n != 0 || err != nil {
			t.Fatalf("WaitForMembers(%v, 0) = %d, %v", role, n, err)
		}
	}
	n, err := queue.WaitForMembers(fipc.Consumer, 1, fipc.NoWait)
	if n != 0 || !errors.Is(err, fipc.ErrTimeout) {
		t.Fatalf("WaitForMembers(Consumer, 1, NoWait) = %d, %v", n, err)
	}
	for _, c := range []struct {
		role    fipc.QueueRole
		atLeast int
	}{{fipc.Producer, 3}, {fipc.Consumer, 4}, {fipc.AnyQueueRole, 6}, {fipc.Producer, -1}, {fipc.AnyQueueRole, 1 << 32},
		{fipc.QueueRole(fipc.Lossy), 1}, {fipc.QueueRole(fipc.Gated), 1}, {fipc.QueueRole(5), 1}} {
		n, err := queue.WaitForMembers(c.role, c.atLeast, fipc.NoWait)
		if n != 0 || !errors.Is(err, fipc.ErrInvalid) {
			t.Fatalf("WaitForMembers(%v, %d) = %d, %v", c.role, c.atLeast, n, err)
		}
	}

	// A count that waits on one goroutine sees the joins of another
	both := membersAside(queue, fipc.AnyQueueRole, 2, tenSeconds)
	producer := joinMember(t, name, fipc.Producer)
	consumer := joinMember(t, name, fipc.Consumer)
	if c := <-both; c.n != 2 || c.err != nil {
		t.Fatalf("WaitForMembers(AnyQueueRole, 2) = %d, %v", c.n, c.err)
	}
	for _, c := range []struct {
		role fipc.QueueRole
		want int
	}{{fipc.Producer, 1}, {fipc.Consumer, 1}, {fipc.AnyQueueRole, 2}} {
		if n, err := queue.WaitForMembers(c.role, 1, fipc.NoWait); n != c.want || err != nil {
			t.Fatalf("WaitForMembers(%v, 1) = %d, %v", c.role, n, err)
		}
	}

	// A member that leaves is counted until the owner's process has seen its end, soon after
	must(t, consumer.Close())
	deadline := time.Now().Add(tenSeconds)
	for {
		n, err := queue.WaitForMembers(fipc.Consumer, 0, fipc.NoWait)
		must(t, err)
		if n == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the consumer that left is still counted")
		}
		time.Sleep(time.Millisecond)
	}

	// Cancel: a wait, now or later, returns ErrCancelled with the count; the queue goes on
	waiting := membersAside(queue, fipc.Consumer, 3, fipc.Forever)
	time.Sleep(50 * time.Millisecond) // let it wait
	queue.Cancel()
	if c := <-waiting; c.n != 0 || !errors.Is(c.err, fipc.ErrCancelled) {
		t.Fatalf("the waiting WaitForMembers: %d, %v", c.n, c.err)
	}
	n, err = queue.WaitForMembers(fipc.Producer, 2, fipc.Forever)
	if n != 1 || !errors.Is(err, fipc.ErrCancelled) {
		t.Fatalf("a later WaitForMembers: %d, %v", n, err)
	}
	queue.Cancel() // final: a second one changes nothing
	again := joinMember(t, name, fipc.Consumer)
	must(t, producer.Send([]byte("the queue goes on"), tenSeconds))
	msg, err := again.Receive(tenSeconds)
	must(t, err)
	if string(msg) != "the queue goes on" {
		t.Fatalf("%q", msg)
	}

	// Close cancels a wait in progress, which returns; then every call returns ErrClosed
	other, err := fipc.CreateQueue(uniqueName("q_close"), 4096, 1, 1)
	must(t, err)
	pending := membersAside(other, fipc.AnyQueueRole, 1, fipc.Forever)
	time.Sleep(50 * time.Millisecond) // let it wait
	must(t, other.Close())
	if c := <-pending; !(c.n == 0 && errors.Is(c.err, fipc.ErrCancelled)) && !errors.Is(c.err, fipc.ErrClosed) {
		t.Fatalf("the wait that Close ended: %d, %v", c.n, c.err)
	}
	_, err = other.WaitForMembers(fipc.AnyQueueRole, 0, fipc.NoWait)
	is(t, err, fipc.ErrClosed, "WaitForMembers after Close")
	other.Cancel() // does nothing
	is(t, other.Close(), fipc.ErrClosed, "a second Close")
}

// Every message once, whole, through Receive's growing path under contention: Receive's first buffer is shorter than
// most messages, so it learns the message's length and takes it into a slice of that length; another consumer may
// take the message between the two calls, and the receive then takes the next one, growing again for a longer one,
// and returns exactly its length. That a retry waits only for what is left of the timeout is
// TestAReceiveThatGrowsItsBufferWaitsOnlyForWhatIsLeftOfItsTimeout (internal_test.go): a race can't show it reliably.
func TestTwoConsumersRaceForMessagesLongerThanTheirFirstBuffer(t *testing.T) {
	guard(t, 120*time.Second)
	const count = 3000
	name := uniqueName("q_race")
	queue, err := fipc.CreateQueue(name, 1<<16, 1, 2)
	must(t, err)
	defer queue.Close()
	producer := joinMember(t, name, fipc.Producer)
	maxPiece := producer.MaxPiece()
	length := func(i int) int { return 16 + i*997%(maxPiece-15) } // 16 to MaxPiece bytes
	consumers := joinConsumers(t, name, 2)
	results := make([]chan consumed, len(consumers))
	for k, consumer := range consumers {
		results[k] = make(chan consumed, 1)
		go func() {
			messages, err := receiveAll(consumer, 0) // Receive: its first buffer is 4 KiB
			results[k] <- consumed{messages, err}
		}()
	}
	for i := range count {
		must(t, producer.Send(queued(0, i, length(i)), tenSeconds))
	}
	must(t, queue.Close())
	checkEachOnce(t, collect(t, results), 1, count, length)
}

func TestAQueuesMisuses(t *testing.T) {
	guard(t, 60*time.Second)
	name := uniqueName("q_misuse")
	queue, err := fipc.CreateQueue(name, 4096, 1, 1)
	must(t, err)
	defer queue.Close()
	_, err = fipc.CreateQueue(name, 4096, 1, 1)
	is(t, err, fipc.ErrAddrInUse, "a second CreateQueue on the name")
	_, err = fipc.CreateBroadcast(name, 4096, 1, fipc.Forever)
	is(t, err, fipc.ErrAddrInUse, "CreateBroadcast on a queue's name")
	_, err = fipc.CreateStream(name, 4096)
	is(t, err, fipc.ErrAddrInUse, "CreateStream on a queue's name")
	for _, role := range []fipc.SubscriberRole{fipc.Lossy, fipc.Gated} {
		_, err = fipc.JoinBroadcast(name, role, tenSeconds)
		is(t, err, fipc.ErrInvalid, "a "+role.String()+" subscriber on a queue")
	}
	for _, role := range []fipc.QueueRole{fipc.AnyQueueRole, fipc.QueueRole(fipc.Lossy), fipc.QueueRole(fipc.Gated),
		-1, 5} {
		_, err = fipc.JoinQueue(name, role, tenSeconds)
		is(t, err, fipc.ErrInvalid, "JoinQueue as "+role.String())
	}
	_, err = fipc.ConnectStream(name, tenSeconds)
	is(t, err, fipc.ErrInvalid, "ConnectStream to a queue")
	_, err = fipc.JoinQueue("bad\x00name", fipc.Producer, fipc.NoWait)
	is(t, err, fipc.ErrInvalid, "JoinQueue with a NUL in the name")

	broadcast := uniqueName("q_broadcast")
	publisher, err := fipc.CreateBroadcast(broadcast, 4096, 1, fipc.Forever)
	must(t, err)
	defer publisher.Close()
	for _, role := range []fipc.QueueRole{fipc.Producer, fipc.Consumer} {
		_, err = fipc.JoinQueue(broadcast, role, tenSeconds)
		is(t, err, fipc.ErrInvalid, "a "+role.String()+" on a broadcast")
	}

	for _, bad := range []struct{ capacity, producers, consumers int }{
		{2048, 1, 1}, {5000, 1, 1}, {-1, 1, 1}, {4096, 0, 1}, {4096, 65, 1}, {4096, -1, 1}, {4096, 1, 0},
		{4096, 1, 65}, {4096, 1, -1}, {4096, 1 << 32, 1},
	} {
		_, err := fipc.CreateQueue(uniqueName("q_bad"), bad.capacity, bad.producers, bad.consumers)
		is(t, err, fipc.ErrInvalid, fmt.Sprintf("CreateQueue(%d, %d, %d)", bad.capacity, bad.producers, bad.consumers))
	}
	_, err = fipc.CreateQueue("bad\x00name", 4096, 1, 1)
	is(t, err, fipc.ErrInvalid, "CreateQueue with a NUL in the name")
	if fipc.Producer.String() != "producer" || fipc.Consumer.String() != "consumer" ||
		fipc.AnyQueueRole.String() != "any" || fipc.QueueRole(5).String() != "QueueRole(5)" {
		t.Fatal("QueueRole.String")
	}
}

// startQueue creates a queue on name, drops it, and returns a joined producer: the program keeps no reference to the
// Queue.
func startQueue(t *testing.T, name string) *fipc.Conn {
	t.Helper()
	if _, err := fipc.CreateQueue(name, 4096, 1, 1); err != nil {
		t.Fatal(err)
	}
	return joinMember(t, name, fipc.Producer)
}

// The garbage collector never closes a queue: one the program dropped goes on until its process ends, and holds its
// name meanwhile.
func TestTheGarbageCollectorNeverEndsAQueue(t *testing.T) {
	guard(t, 30*time.Second)
	name := uniqueName("gc_queue")
	producer := startQueue(t, name)
	for range 2 {
		runtime.GC()
		time.Sleep(20 * time.Millisecond) // time for cleanups, which run on a goroutine of their own
	}
	must(t, producer.Send([]byte("still here"), tenSeconds))
	consumer := joinMember(t, name, fipc.Consumer)
	msg, err := consumer.Receive(tenSeconds)
	must(t, err)
	if string(msg) != "still here" {
		t.Fatalf("%q", msg)
	}
	_, err = fipc.CreateQueue(name, 4096, 1, 1)
	is(t, err, fipc.ErrAddrInUse, "a second CreateQueue of the dropped queue's name")
}
