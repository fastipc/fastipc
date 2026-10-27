package fipc_test

// The code fragments of the documentation (the READMEs, the website), each run here inside the code it needs.
// TestEveryGoBlockOfTheDocsRuns checks that every Go block of the documentation is in this file or in ../examples.

import (
	"bytes"
	"context"
	"errors"
	"log"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fastipc/fastipc/bindings/go/fipc"
)

var handled atomic.Int64

// handle stands for what an application does with a message.
func handle(msg []byte) {
	if len(msg) == 0 {
		panic("an empty message")
	}
	handled.Add(1)
}

func TestSnippetABatchEachWay(t *testing.T) {
	guard(t, 30*time.Second)
	p := newPair(t, "snippet_batch", ring)
	conn, peer := p.client, p.server
	before := handled.Load()
	each := func() error {
		sent, err := conn.SendMany([][]byte{[]byte("one"), []byte("two"), []byte("three")}, fipc.Forever)
		if err != nil {
			return err // sent says how many went before the error
		}
		buf, lens := make([]byte, 1<<16), make([]int, 64)
		n, err := peer.ReceiveMany(buf, lens, fipc.Forever) // every message already there, up to 64
		if err != nil {
			return err
		}
		at := 0
		for _, l := range lens[:n] {
			handle(buf[at : at+l])
			at += l
		}
		if sent != 3 || n != 3 {
			t.Errorf("sent %d, received %d", sent, n)
		}
		return nil
	}
	must(t, each())
	if handled.Load()-before != 3 {
		t.Fatalf("handled %d", handled.Load()-before)
	}
}

func TestSnippetPollWithATimeout(t *testing.T) {
	guard(t, 30*time.Second)
	p := newPair(t, "snippet_poll", ring)
	conn := p.server
	must(t, p.client.Send([]byte("one"), fipc.Forever))
	poll := func() error {
		msg, err := conn.Receive(100 * time.Millisecond)
		switch {
		case err == nil:
			handle(msg)
		case errors.Is(err, fipc.ErrTimeout): // nothing yet
		default:
			return err // fipc.ErrDisconnected: the peer is gone
		}
		return nil
	}
	must(t, poll()) // the message
	must(t, poll()) // nothing yet
	must(t, p.client.Close())
	is(t, poll(), fipc.ErrDisconnected, "after the peer's end")
}

func TestSnippetReceiveIntoTooLarge(t *testing.T) {
	guard(t, 30*time.Second)
	p := newPair(t, "snippet_too_large", ring)
	conn := p.server
	must(t, p.client.Send(pattern(300), fipc.Forever))
	buf := make([]byte, 256)
	n, err := conn.ReceiveInto(buf, fipc.Forever)
	var e *fipc.Error
	if errors.As(err, &e) && e.Len() > 0 { // ErrTooLarge: make room, then again
		buf = make([]byte, e.Len())
		n, err = conn.ReceiveInto(buf, fipc.Forever)
	}
	must(t, err)
	if n != 300 {
		t.Fatalf("%d bytes", n)
	}
}

func TestSnippetZeroCopy(t *testing.T) {
	guard(t, 30*time.Second)
	p := newPair(t, "snippet_zero_copy", ring)
	send := func(conn *fipc.Conn) error {
		buf, err := conn.SendAcquire(5, fipc.Forever) // room in the ring
		if err != nil {
			return err
		}
		copy(buf, "hello")
		return conn.SendCommit(5) // sent; buf is no longer valid
	}
	receive := func(conn *fipc.Conn) error {
		msg, err := conn.ReceiveAcquire(fipc.Forever) // the message in the ring, read-only
		if err != nil {
			return err
		}
		handle(msg)           // read it in place
		conn.ReceiveRelease() // frees its room; msg is no longer valid
		return nil
	}
	before := handled.Load()
	must(t, send(p.client))
	must(t, receive(p.server))
	if handled.Load() != before+1 {
		t.Fatal("the message wasn't handled")
	}
}

func TestSnippetStopAReaderGoroutine(t *testing.T) {
	guard(t, 30*time.Second)
	p := newPair(t, "snippet_reader", ring)
	conn := p.server
	must(t, p.client.Send([]byte("before the cancel"), fipc.Forever))
	done := make(chan struct{})
	go func() {
		defer close(done)
		// until fipc.ErrCancelled (or fipc.ErrDisconnected)
		for {
			msg, err := conn.Receive(fipc.Forever)
			if err != nil {
				return
			}
			handle(msg)
		}
	}()
	// ...
	conn.Cancel() // wakes the reader: ErrCancelled
	<-done        // then close the connection, or go on with calls that needn't wait
}

func TestSnippetAContextCancels(t *testing.T) {
	guard(t, 30*time.Second)
	p := newPair(t, "snippet_context", ring)
	conn := p.server
	ctx, cancel := context.WithCancel(context.Background())
	stop := context.AfterFunc(ctx, conn.Cancel) // cancels the connection when ctx is done
	defer stop()
	cancel()
	_, err := conn.Receive(tenSeconds)
	is(t, err, fipc.ErrCancelled, "a Receive after the context's end")
}

func TestSnippetAReaderGoroutineFeedsAChannel(t *testing.T) {
	guard(t, 30*time.Second)
	p := newPair(t, "snippet_channel", ring)
	conn, peer := p.server, p.client
	must(t, peer.Send([]byte("update"), fipc.Forever))
	before := handled.Load()
	messages := make(chan []byte, 64)
	go func() {
		defer close(messages)
		for {
			msg, err := conn.Receive(fipc.Forever)
			if err != nil {
				return // ErrDisconnected, or ErrCancelled once the game closes the connection
			}
			messages <- msg
		}
	}()
	// Each frame: whatever has arrived, without waiting
	for len(messages) > 0 {
		handle(<-messages)
	}
	for handled.Load() == before { // the next frames, until the update has arrived
		time.Sleep(time.Millisecond)
		for len(messages) > 0 {
			handle(<-messages)
		}
	}
	must(t, conn.Close()) // the reader sees ErrCancelled and ends
	for range messages {
	}
}

// broadcast is a broadcast with one subscriber of role, joined and counted, closed when the test ends.
func broadcast(t *testing.T, prefix string, role fipc.SubscriberRole) (publisher, subscriber *fipc.Conn) {
	t.Helper()
	name := uniqueName(prefix)
	publisher, err := fipc.CreateBroadcast(name, 4096, 4, fipc.Forever)
	must(t, err)
	subscriber, err = fipc.JoinBroadcast(name, role, tenSeconds)
	must(t, err)
	_, err = publisher.WaitForMembers(1, tenSeconds)
	must(t, err)
	t.Cleanup(func() {
		subscriber.Close()
		publisher.Close()
	})
	return publisher, subscriber
}

func TestSnippetALossySubscriberThatFellBehind(t *testing.T) {
	guard(t, 30*time.Second)
	publisher, subscriber := broadcast(t, "snippet_overrun", fipc.Lossy)
	for i := range 144 { // laps the subscriber
		must(t, publisher.Send(numbered(i, 48), fipc.NoWait))
	}
	receive := func() error {
		msg, err := subscriber.Receive(fipc.Forever)
		var e *fipc.Error
		switch {
		case err == nil:
			handle(msg)
		case errors.As(err, &e) && e.Lost() > 0: // ErrOverrun: a lossy subscriber fell a lap behind
			log.Printf("%d messages lost", e.Lost()) // the next receive goes on from a recent message
		default:
			return err // fipc.ErrDisconnected: the broadcast ended
		}
		return nil
	}
	before := handled.Load()
	must(t, receive()) // the overrun
	must(t, receive())
	if handled.Load() != before+1 {
		t.Fatalf("%d messages", handled.Load()-before)
	}
	must(t, publisher.Close())
	for range 3 {
		must(t, receive())
	}
	is(t, receive(), fipc.ErrDisconnected, "after the publisher's Close")
}

func TestSnippetWaitForSubscribers(t *testing.T) {
	guard(t, 30*time.Second)
	publisher, first := broadcast(t, "snippet_count", fipc.Gated)
	joined := make(chan connected, 2)
	for range 2 { // two more subscribers join while the count waits
		go func() {
			subscriber, err := fipc.JoinBroadcast(first.Name(), fipc.Lossy, tenSeconds)
			joined <- connected{subscriber, err}
		}()
	}
	count := func() error {
		n, err := publisher.WaitForMembers(3, 5*time.Second) // waits up to 5 s for 3 subscribers
		if errors.Is(err, fipc.ErrTimeout) {
			log.Printf("only %d of 3 subscribers joined", n)
		} else if err != nil {
			return err
		}
		return nil
	}
	must(t, count())
	for range 2 {
		subscriber := <-joined
		must(t, subscriber.err)
		must(t, subscriber.conn.Close())
	}
}

// work stands for what a server does with a request: here, its uppercase.
func work(request []byte) []byte { return bytes.ToUpper(request) }

func TestSnippetACallWhoseServerEndedIsSentAgain(t *testing.T) {
	guard(t, 30*time.Second)
	_, name := newService(t, "snippet_abandoned", 4096, 256, 1, 2)
	client := joinService(t, name, fipc.Client)
	quitter := joinService(t, name, fipc.Server)
	answering := make(chan error, 1)
	go func() { // takes the first request and closes, holding it (ErrAbandoned); then another server answers
		if _, err := quitter.Receive(tenSeconds); err != nil {
			answering <- err
			return
		}
		quitter.Close()
		answering <- <-answerAside(t, name, 1)
	}()
	request := []byte("ping")
	call := func() ([]byte, error) {
		reply, err := client.Call(request, 5*time.Second)
		if errors.Is(err, fipc.ErrAbandoned) { // its server ended before it replied: send it again, if it may run twice
			reply, err = client.Call(request, 5*time.Second)
		}
		if err != nil {
			return nil, err // fipc.ErrDisconnected: the service ended
		}
		return reply, nil
	}
	reply, err := call()
	must(t, err)
	if string(reply) != "PING" {
		t.Fatalf("%q", reply)
	}
	must(t, <-answering)
}

func TestSnippetAPoolOfServerGoroutines(t *testing.T) {
	guard(t, 60*time.Second)
	service, name := newService(t, "snippet_pool", 4096, 256, 4, 4)
	for range 4 { // a pool of four servers on goroutines: each takes the next request of any client
		go func() {
			server, err := fipc.JoinService(name, fipc.Server, 5*time.Second)
			if err != nil {
				log.Fatal(err)
			}
			defer server.Close()
			for {
				request, err := server.Receive(fipc.Forever)
				if err != nil {
					return // fipc.ErrDisconnected: the service ended
				}
				if err := server.Send(work(request), fipc.Forever); err != nil {
					return
				}
			}
		}()
	}
	asked := make(chan error, 4)
	for range 4 {
		go func() {
			client, err := fipc.JoinService(name, fipc.Client, tenSeconds)
			if err != nil {
				asked <- err
				return
			}
			defer client.Close()
			asked <- callRequests(client, 50)
		}()
	}
	for range 4 {
		must(t, <-asked)
	}
	must(t, service.Close()) // the servers' receives return ErrDisconnected: they close and return
}

func TestSnippetAReplyInPlace(t *testing.T) {
	guard(t, 30*time.Second)
	_, name := newService(t, "snippet_in_place", 4096, 256, 1, 1)
	client := joinService(t, name, fipc.Client)
	server := joinService(t, name, fipc.Server)
	must(t, client.Send([]byte("hello"), tenSeconds))
	answer := func() error {
		request, err := server.ReceiveAcquire(fipc.Forever) // the request in place, valid until the reply
		if err != nil {
			return err
		}
		reply, err := server.SendAcquire(len(request), fipc.NoWait) // a reply never waits
		if err != nil {
			return err
		}
		copy(reply, bytes.ToUpper(request))    // written from the request in place
		return server.SendCommit(len(request)) // the reply; request is no longer valid
	}
	must(t, answer())
	expectReceive(t, client, "HELLO")
}
