package fipc_test

// Batches (SendMany, ReceiveMany): on a stream, in order and packed back to back, a buffer that stops a batch and a
// first message too long for it, NoWait's partial send with how many went, an empty message refused before any goes;
// a broadcast's and a work queue's batches, and ErrTooLarge over MaxPiece before any goes; a service client's batch of
// requests, owed as its sends are, with their replies in order, and a server's batches refused.

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/fastipc/fastipc/bindings/go/fipc"
)

// receiveBatches receives count messages in batches (a broadcast's subscriber may take fewer than are there).
func receiveBatches(t testing.TB, conn *fipc.Conn, count int) [][]byte {
	t.Helper()
	received, err := batches(conn, count)
	must(t, err)
	return received
}

// batches is receiveBatches for another goroutine than the test's: the error, which the test checks.
func batches(conn *fipc.Conn, count int) ([][]byte, error) {
	buf := make([]byte, 1<<20)
	lens := make([]int, 16)
	var received [][]byte
	for len(received) < count {
		n, err := conn.ReceiveMany(buf, lens, tenSeconds)
		if err != nil {
			return received, err
		}
		if n < 1 || n > len(lens) {
			return received, fmt.Errorf("a batch of %d", n)
		}
		at := 0
		for _, l := range lens[:n] {
			received = append(received, bytes.Clone(buf[at:at+l]))
			at += l
		}
	}
	return received, nil
}

// received is what a goroutine's batches received, and its error.
type received struct {
	msgs [][]byte
	err  error
}

func (r received) wait(t testing.TB) [][]byte {
	t.Helper()
	must(t, r.err)
	return r.msgs
}

// receiveAside receives count messages in batches on a goroutine of its own.
func receiveAside(conn *fipc.Conn, count int) <-chan received {
	ch := make(chan received, 1)
	go func() {
		msgs, err := batches(conn, count)
		ch <- received{msgs, err}
	}()
	return ch
}

func sameMessages(t testing.TB, got, want [][]byte, what string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: %d messages, want %d", what, len(got), len(want))
	}
	for i := range want {
		if !bytes.Equal(got[i], want[i]) {
			t.Fatalf("%s: message %d differs", what, i)
		}
	}
}

func TestAStreamsBatchesComeInOrderPackedBackToBack(t *testing.T) {
	guard(t, 60*time.Second)
	p := newPair(t, "batch", 1024)
	var msgs [][]byte
	for i := range 6 {
		msgs = append(msgs, pattern(10+i*20))
	}
	n, err := p.client.SendMany(msgs, tenSeconds)
	if n != 6 || err != nil {
		t.Fatalf("SendMany: %d, %v", n, err)
	}
	buf := make([]byte, 4096)
	lens := make([]int, 16)
	n, err = p.server.ReceiveMany(buf, lens, tenSeconds)
	if n != 6 || err != nil {
		t.Fatalf("ReceiveMany: %d, %v", n, err)
	}
	at := 0
	for i, l := range lens[:n] {
		if !bytes.Equal(buf[at:at+l], msgs[i]) {
			t.Fatalf("message %d differs", i)
		}
		at += l
	}

	// A buffer that stops the batch; a first message too long for it stays queued, its length reported
	_, err = p.client.SendMany(msgs, tenSeconds)
	must(t, err)
	if n, err = p.server.ReceiveMany(buf[:10+30], lens, tenSeconds); n != 2 || err != nil {
		t.Fatalf("ReceiveMany into 40 bytes: %d, %v", n, err)
	}
	_, err = p.server.ReceiveMany(buf[:10], lens, tenSeconds)
	var e *fipc.Error
	if !errors.As(err, &e) || !errors.Is(err, fipc.ErrTooLarge) || e.Len() != 50 {
		t.Fatalf("ReceiveMany into 10 bytes: %v", err)
	}
	if n, err = p.server.ReceiveMany(buf, lens[:3], tenSeconds); n != 3 || err != nil {
		t.Fatalf("ReceiveMany of at most 3: %d, %v", n, err) // at most len(lens)
	}
	sameMessages(t, receiveBatches(t, p.server, 1), msgs[5:], "the last")

	// A message longer than one piece goes in pieces, and arrives whole
	long := pattern(100_000)
	batch := [][]byte{msgs[0], long, msgs[1]}
	got := receiveAside(p.server, 3)
	if n, err = p.client.SendMany(batch, tenSeconds); n != 3 || err != nil {
		t.Fatalf("SendMany with a long message: %d, %v", n, err)
	}
	sameMessages(t, (<-got).wait(t), batch, "with a long message")
	_, err = p.server.ReceiveMany(buf, lens, fipc.NoWait)
	is(t, err, fipc.ErrTimeout, "nothing left")
}

func TestAPartialSendSaysHowManyWentAndAnEmptyMessageSendsNone(t *testing.T) {
	guard(t, 60*time.Second)
	p := newPair(t, "partial", 1024)
	hundred := pattern(100)
	many := make([][]byte, 100)
	for i := range many {
		many[i] = hundred
	}
	// Frames of 128 bytes: the ring of 1 KiB holds 8, fewer where its end is padded
	n, err := p.client.SendMany(many, fipc.NoWait)
	is(t, err, fipc.ErrTimeout, "a partial send")
	if n < 1 || n > 8 {
		t.Fatalf("a partial send: %d went", n)
	}
	sameMessages(t, receiveBatches(t, p.server, n), many[:n], "what went")

	got := receiveAside(p.server, 100)
	if n, err = p.client.SendMany(many, tenSeconds); n != 100 || err != nil { // sent while it is received
		t.Fatalf("SendMany: %d, %v", n, err)
	}
	sameMessages(t, (<-got).wait(t), many, "the whole batch")

	n, err = p.client.SendMany([][]byte{hundred, {}}, fipc.NoWait)
	if n != 0 || !errors.Is(err, fipc.ErrInvalid) {
		t.Fatalf("an empty message: %d, %v", n, err)
	}
	_, err = p.client.SendMany(nil, fipc.NoWait)
	is(t, err, fipc.ErrInvalid, "an empty batch")
	_, err = p.server.ReceiveMany(make([]byte, 16), nil, fipc.NoWait)
	is(t, err, fipc.ErrInvalid, "no lengths")
	_, err = p.server.ReceiveMany(make([]byte, 16), make([]int, 4), fipc.NoWait)
	is(t, err, fipc.ErrTimeout, "nothing went")
	p.client.Close()
	_, err = p.client.SendMany(many, fipc.NoWait)
	is(t, err, fipc.ErrClosed, "after Close")
	_, err = p.client.ReceiveMany(make([]byte, 16), make([]int, 4), fipc.NoWait)
	is(t, err, fipc.ErrClosed, "after Close")
}

func TestABroadcastsBatchesReachEverySubscriber(t *testing.T) {
	guard(t, 60*time.Second)
	name := uniqueName("bc_batch")
	publisher, err := fipc.CreateBroadcast(name, 1<<16, 4, fipc.Forever)
	must(t, err)
	defer publisher.Close()
	lossy, err := fipc.JoinBroadcast(name, fipc.Lossy, tenSeconds)
	must(t, err)
	defer lossy.Close()
	gated, err := fipc.JoinBroadcast(name, fipc.Gated, tenSeconds)
	must(t, err)
	defer gated.Close()
	if n, err := publisher.WaitForMembers(2, tenSeconds); n != 2 || err != nil {
		t.Fatalf("WaitForMembers: %d, %v", n, err)
	}
	var ticks [][]byte
	for i := range 40 {
		ticks = append(ticks, []byte(fmt.Sprintf("tick %d", i)))
	}
	if n, err := publisher.SendMany(ticks, tenSeconds); n != 40 || err != nil {
		t.Fatalf("SendMany: %d, %v", n, err)
	}
	n, err := publisher.SendMany([][]byte{ticks[0], make([]byte, publisher.MaxPiece()+1)}, fipc.NoWait)
	var e *fipc.Error
	if n != 0 || !errors.As(err, &e) || !errors.Is(err, fipc.ErrTooLarge) || e.Len() != publisher.MaxPiece()+1 {
		t.Fatalf("a message over MaxPiece: %d, %v", n, err)
	}
	_, err = publisher.ReceiveMany(make([]byte, 64), make([]int, 4), fipc.NoWait)
	is(t, err, fipc.ErrInvalid, "a publisher's ReceiveMany")
	_, err = gated.SendMany(ticks, fipc.NoWait)
	is(t, err, fipc.ErrInvalid, "a subscriber's SendMany")
	publisher.Close() // what it sent is received first
	for _, subscriber := range []*fipc.Conn{lossy, gated} {
		sameMessages(t, receiveBatches(t, subscriber, 40), ticks, "a subscriber")
		_, err = subscriber.ReceiveMany(make([]byte, 64), make([]int, 4), tenSeconds)
		is(t, err, fipc.ErrDisconnected, "after the publisher's close")
	}
}

func TestAQueuesBatchesDeliverEveryMessageOnce(t *testing.T) {
	guard(t, 60*time.Second)
	name := uniqueName("q_batch")
	queue, err := fipc.CreateQueue(name, 4096, 1, 1)
	must(t, err)
	defer queue.Close()
	producer, err := fipc.JoinQueue(name, fipc.Producer, tenSeconds)
	must(t, err)
	defer producer.Close()
	consumer, err := fipc.JoinQueue(name, fipc.Consumer, tenSeconds)
	must(t, err)
	defer consumer.Close()
	var jobs [][]byte
	for i := range 200 {
		jobs = append(jobs, []byte(fmt.Sprintf("job %d", i)))
	}
	got := receiveAside(consumer, 200)
	for at := 0; at < 200; at += 50 {
		if n, err := producer.SendMany(jobs[at:at+50], tenSeconds); n != 50 || err != nil {
			t.Fatalf("SendMany: %d, %v", n, err)
		}
	}
	sameMessages(t, (<-got).wait(t), jobs, "one producer: its messages in order")
	_, err = producer.SendMany([][]byte{jobs[0], make([]byte, producer.MaxPiece()+1)}, fipc.NoWait)
	is(t, err, fipc.ErrTooLarge, "a message over MaxPiece")
	_, err = consumer.SendMany(jobs, fipc.NoWait)
	is(t, err, fipc.ErrInvalid, "a consumer's SendMany")
	_, err = producer.ReceiveMany(make([]byte, 64), make([]int, 4), fipc.NoWait)
	is(t, err, fipc.ErrInvalid, "a producer's ReceiveMany")
	must(t, queue.Close())
	_, err = consumer.ReceiveMany(make([]byte, 64), make([]int, 4), tenSeconds)
	is(t, err, fipc.ErrDisconnected, "after the queue's close")
	_, err = producer.SendMany(jobs, tenSeconds)
	is(t, err, fipc.ErrDisconnected, "after the queue's close")
}

func TestAServiceClientsBatchIsOwedAsItsSendsAreAndAServersBatchesAreInvalid(t *testing.T) {
	guard(t, 60*time.Second)
	name := uniqueName("s_batch")
	service, err := fipc.CreateService(name, 1<<14, 64, 1, 1)
	must(t, err)
	defer service.Close()
	server, err := fipc.JoinService(name, fipc.Server, tenSeconds)
	must(t, err)
	defer server.Close()
	client, err := fipc.JoinService(name, fipc.Client, tenSeconds)
	must(t, err)
	defer client.Close()
	answer := func() error {
		request, err := server.Receive(tenSeconds)
		if err != nil {
			return err
		}
		return server.Send(append(request, " done"...), fipc.NoWait)
	}
	requests := [][]byte{[]byte("a"), []byte("bb"), []byte("ccc")}
	if n, err := client.SendMany(requests, tenSeconds); n != 3 || err != nil {
		t.Fatalf("SendMany: %d, %v", n, err)
	}
	_, err = client.Call([]byte("d"), fipc.NoWait)
	is(t, err, fipc.ErrInvalid, "a Call while the batch's replies are owed")
	for range 3 {
		must(t, answer())
	}
	sameMessages(t, receiveBatches(t, client, 3), [][]byte{[]byte("a done"), []byte("bb done"), []byte("ccc done")},
		"the replies")
	answered := make(chan error, 1)
	go func() { answered <- answer() }()
	reply, err := client.Call([]byte("e"), tenSeconds) // nothing owed now
	must(t, err)
	must(t, <-answered)
	if string(reply) != "e done" {
		t.Fatalf("Call: %q", reply)
	}
	_, err = server.SendMany(requests, fipc.NoWait)
	is(t, err, fipc.ErrInvalid, "a server's SendMany")
	_, err = server.ReceiveMany(make([]byte, 64), make([]int, 4), fipc.NoWait)
	is(t, err, fipc.ErrInvalid, "a server's ReceiveMany")

	// A batch whose requests don't all fit counts only those that went: once their replies are received, a Call goes
	// again. Each request holds 64 + 64 bytes of the 16 KiB ring: at most 128 fit
	many := make([][]byte, 200)
	for i := range many {
		many[i] = []byte("f")
	}
	n, err := client.SendMany(many, fipc.NoWait)
	is(t, err, fipc.ErrTimeout, "a partial batch")
	if n < 1 || n > 128 {
		t.Fatalf("a partial batch: %d went", n)
	}
	for range n {
		request, err := server.Receive(tenSeconds)
		must(t, err)
		must(t, server.Send(request, fipc.NoWait))
	}
	if got := receiveBatches(t, client, n); len(got) != n {
		t.Fatalf("%d replies, want %d", len(got), n)
	}
	go func() { answered <- answer() }()
	reply, err = client.Call([]byte("g"), tenSeconds)
	must(t, err)
	must(t, <-answered)
	if string(reply) != "g done" {
		t.Fatalf("Call: %q", reply)
	}
}
