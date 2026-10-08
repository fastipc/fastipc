package fipc_test

// The code fragments of the documentation (the READMEs, the website), each run here inside the code it needs.
// TestEveryGoBlockOfTheDocsRuns checks that every Go block of the documentation is in this file or in ../examples.

import (
	"context"
	"errors"
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
