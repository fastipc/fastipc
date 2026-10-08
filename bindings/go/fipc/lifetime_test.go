package fipc_test

// Goroutines, cancels, closes and the garbage collector: what ends a call, a connection and a listener, and what
// several goroutines may do at once (run these with -race where cgo is available: the race detector needs it).

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/fastipc/fastipc/bindings/go/fipc"
)

func TestCancelStopsAReceiveOnAnotherGoroutine(t *testing.T) {
	guard(t, 30*time.Second)
	p := newPair(t, "cancel_receive", ring)
	done := make(chan error, 1)
	go func() {
		_, err := p.server.Receive(fipc.Forever)
		done <- err
	}()
	time.Sleep(50 * time.Millisecond) // let it wait
	p.server.Cancel()
	is(t, <-done, fipc.ErrCancelled, "the waiting Receive")

	// Final: later waits are cancelled too, while calls that needn't wait still work, and the peer sees nothing
	_, err := p.server.Receive(time.Second)
	is(t, err, fipc.ErrCancelled, "a later Receive")
	must(t, p.client.Send([]byte("queued"), fipc.Forever))
	msg, err := p.server.Receive(tenSeconds)
	must(t, err)
	if string(msg) != "queued" {
		t.Fatalf("got %q", msg)
	}
	must(t, p.server.Send([]byte("still up"), fipc.NoWait))
	msg, err = p.client.Receive(tenSeconds)
	must(t, err)
	if string(msg) != "still up" {
		t.Fatalf("got %q", msg)
	}
	p.server.Cancel() // again: nothing more
}

func TestAContextCancelsAWait(t *testing.T) {
	guard(t, 30*time.Second)
	p := newPair(t, "context", ring)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	stop := context.AfterFunc(ctx, p.server.Cancel)
	defer stop()
	_, err := p.server.Receive(fipc.Forever)
	is(t, err, fipc.ErrCancelled, "a Receive past the context's deadline")
}

func TestCancelStopsAnAccept(t *testing.T) {
	guard(t, 30*time.Second)
	name := uniqueName("cancel_accept")
	listener, err := fipc.Listen(name, 1<<16)
	must(t, err)
	defer listener.Close()
	done := make(chan error, 1)
	go func() {
		_, err := listener.Accept(fipc.Forever)
		done <- err
	}()
	time.Sleep(50 * time.Millisecond)
	listener.Cancel()
	is(t, <-done, fipc.ErrCancelled, "the waiting Accept")
	_, err = listener.Accept(fipc.NoWait)
	is(t, err, fipc.ErrCancelled, "a later Accept")
	// A cancelled listener sets no new client up
	_, err = fipc.Connect(name, 100*time.Millisecond)
	is(t, err, fipc.ErrTimeout, "Connect to a cancelled listener")
}

func TestCloseWhileCallsWait(t *testing.T) {
	guard(t, 30*time.Second)
	p := newPair(t, "close_waiting", 1024)
	// The client's ring to the server is full, so a Send waits for room; a Receive waits for a message
	for p.client.Send(bytes.Repeat([]byte{1}, 100), fipc.NoWait) == nil {
	}
	var wg sync.WaitGroup
	results := make(chan error, 3)
	wg.Go(func() { results <- p.client.Send(bytes.Repeat([]byte{2}, 100), fipc.Forever) })
	wg.Go(func() {
		_, err := p.client.Receive(fipc.Forever)
		results <- err
	})
	wg.Go(func() {
		_, err := p.client.RPCReceive(fipc.Forever) // waits for the receive mutex, then sees the close
		results <- err
	})
	time.Sleep(50 * time.Millisecond) // let them wait
	must(t, p.client.Close())
	wg.Wait()
	close(results)
	for err := range results {
		if !errors.Is(err, fipc.ErrCancelled) && !errors.Is(err, fipc.ErrClosed) {
			t.Fatalf("a call that waited: %v", err)
		}
	}
	// The peer sees the end after the messages it can take
	for {
		_, err := p.server.Receive(tenSeconds)
		if errors.Is(err, fipc.ErrDisconnected) {
			break
		}
		must(t, err)
	}

	// A listener closed while its Accept waits
	name := uniqueName("close_accepting")
	listener, err := fipc.Listen(name, 1<<16)
	must(t, err)
	done := make(chan error, 1)
	go func() {
		_, err := listener.Accept(fipc.Forever)
		done <- err
	}()
	time.Sleep(50 * time.Millisecond)
	must(t, listener.Close())
	is(t, <-done, fipc.ErrCancelled, "the waiting Accept")
	again, err := fipc.Listen(name, 1<<16) // the name is free
	must(t, err)
	must(t, again.Close())
}

func TestCallsAfterCloseReturnErrClosed(t *testing.T) {
	guard(t, 30*time.Second)
	p := newPair(t, "after_close", ring)
	piece := p.client.MaxPiece()
	must(t, p.client.Close())
	is(t, p.client.Close(), fipc.ErrClosed, "a second Close")
	c := p.client
	is(t, c.Send([]byte("x"), fipc.NoWait), fipc.ErrClosed, "Send")
	_, err := c.SendAcquire(1, fipc.NoWait)
	is(t, err, fipc.ErrClosed, "SendAcquire")
	is(t, c.SendCommit(1), fipc.ErrClosed, "SendCommit")
	_, err = c.RPCSubmit(1, nil, fipc.NoWait)
	is(t, err, fipc.ErrClosed, "RPCSubmit")
	is(t, c.RPCRespond(1, 1, 0, nil, fipc.NoWait), fipc.ErrClosed, "RPCRespond")
	_, err = c.Receive(fipc.NoWait)
	is(t, err, fipc.ErrClosed, "Receive")
	_, err = c.ReceiveInto(make([]byte, 8), fipc.NoWait)
	is(t, err, fipc.ErrClosed, "ReceiveInto")
	_, err = c.ReceiveAcquire(fipc.NoWait)
	is(t, err, fipc.ErrClosed, "ReceiveAcquire")
	_, err = c.RPCReceive(fipc.NoWait)
	is(t, err, fipc.ErrClosed, "RPCReceive")
	_, err = c.RPCReceiveInto(nil, fipc.NoWait)
	is(t, err, fipc.ErrClosed, "RPCReceiveInto")
	c.ReceiveRelease() // does nothing
	c.Cancel()         // does nothing
	if c.MaxPiece() != piece || c.Name() != p.listener.Name() {
		t.Fatal("Name and MaxPiece answer after Close")
	}

	must(t, p.listener.Close())
	is(t, p.listener.Close(), fipc.ErrClosed, "a second Close of the listener")
	_, err = p.listener.Accept(fipc.NoWait)
	is(t, err, fipc.ErrClosed, "Accept")
	p.listener.Cancel() // does nothing
}

func TestCancelWhileClosingOnAnotherGoroutine(t *testing.T) {
	guard(t, 60*time.Second)
	// Cancel and Close race: the cancel runs before the native close or not at all
	for range 200 {
		p := newPair(t, "cancel_race", 1024)
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Go(func() {
			<-start
			p.client.Close()
			p.listener.Close()
		})
		wg.Go(func() {
			<-start
			p.client.Cancel()
			p.listener.Cancel()
		})
		close(start)
		wg.Wait()
		p.server.Close()
	}
}

func TestClosingAConnectionEndsItAfterItsMessages(t *testing.T) {
	guard(t, 30*time.Second)
	p := newPair(t, "close", ring)
	for i := range byte(3) {
		must(t, p.client.Send([]byte{i}, fipc.Forever))
	}
	must(t, p.client.Close())
	for i := range byte(3) {
		msg, err := p.server.Receive(tenSeconds) // every message the peer completed comes first
		must(t, err)
		if !bytes.Equal(msg, []byte{i}) {
			t.Fatalf("got %v", msg)
		}
	}
	_, err := p.server.Receive(tenSeconds)
	is(t, err, fipc.ErrDisconnected, "Receive")
	_, err = p.server.Receive(fipc.NoWait)
	is(t, err, fipc.ErrDisconnected, "Receive again") // final
	is(t, p.server.Send([]byte("x"), fipc.NoWait), fipc.ErrDisconnected, "Send")
	_, err = p.server.RPCSubmit(1, []byte("x"), fipc.NoWait)
	is(t, err, fipc.ErrDisconnected, "RPCSubmit")
}

func TestConcurrentSendersDontInterleave(t *testing.T) {
	guard(t, 60*time.Second)
	p := newPair(t, "senders", 1<<16)
	// Four goroutines send on one connection, messages in pieces among them; each message arrives whole
	const senders, each = 4, 50
	sizes := []int{1, 100, 5000, 100_000}
	var wg sync.WaitGroup
	for s := range senders {
		wg.Go(func() {
			for i := range each {
				msg := append([]byte{byte(s)}, bytes.Repeat([]byte{byte(s)}, sizes[i%len(sizes)])...)
				if err := p.client.Send(msg, tenSeconds); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	counts := make([]int, senders)
	for range senders * each {
		msg, err := p.server.Receive(tenSeconds)
		must(t, err)
		if !bytes.Equal(msg, bytes.Repeat(msg[:1], len(msg))) {
			t.Fatal("messages interleaved")
		}
		counts[msg[0]]++
	}
	wg.Wait()
	for s, n := range counts {
		if n != each {
			t.Fatalf("sender %d: %d messages", s, n)
		}
	}
}

func TestSendAndReceiveAtOnce(t *testing.T) {
	guard(t, 60*time.Second)
	const count = 20_000
	p := newPair(t, "at_once", 4096)
	// The server echoes on two goroutines; the client sends on one and receives on another
	echoes := make(chan []byte, 64)
	var wg sync.WaitGroup
	wg.Go(func() {
		defer close(echoes)
		for {
			msg, err := p.server.Receive(tenSeconds)
			if err != nil {
				return
			}
			echoes <- msg
		}
	})
	wg.Go(func() {
		for msg := range echoes {
			if err := p.server.Send(msg, tenSeconds); err != nil {
				t.Error(err)
				return
			}
		}
	})
	wg.Go(func() {
		for i := range uint32(count) {
			if err := p.client.Send(binary.LittleEndian.AppendUint32(nil, i), tenSeconds); err != nil {
				t.Error(err)
				return
			}
		}
	})
	buf := make([]byte, 4)
	for i := range uint32(count) {
		n, err := p.client.ReceiveInto(buf, tenSeconds)
		must(t, err)
		if n != 4 || binary.LittleEndian.Uint32(buf) != i {
			t.Fatalf("echo %d: %v", i, buf[:n])
		}
	}
	must(t, p.client.Close()) // the echo goroutines see the end
	wg.Wait()
}

func TestClosingTheListenerKeepsItsConnections(t *testing.T) {
	guard(t, 30*time.Second)
	p := newPair(t, "listener_close", ring)
	name := p.listener.Name()
	must(t, p.listener.Close())
	must(t, p.client.Send([]byte("still here"), fipc.Forever))
	msg, err := p.server.Receive(tenSeconds)
	must(t, err)
	if string(msg) != "still here" {
		t.Fatalf("got %q", msg)
	}
	// The name is free again (on Windows, once the accepted connections are closed)
	must(t, p.client.Close())
	must(t, p.server.Close())
	listenAgain(t, name)
}

// listenAgain listens on name once it is free, within ten seconds, and closes the listener.
func listenAgain(t *testing.T, name string) {
	t.Helper()
	deadline := time.Now().Add(tenSeconds)
	for {
		listener, err := fipc.Listen(name, 1<<16)
		if err == nil {
			must(t, listener.Close())
			return
		}
		if !errors.Is(err, fipc.ErrAddrInUse) || time.Now().After(deadline) {
			t.Fatalf("listen again: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestTheGarbageCollectorClosesAnUnreachableConn(t *testing.T) {
	guard(t, 30*time.Second)
	name := uniqueName("gc_conn")
	listener, err := fipc.Listen(name, 1<<16)
	must(t, err)
	defer listener.Close()
	connecting := connectAside(name)
	func() {
		server, err := listener.Accept(tenSeconds)
		must(t, err)
		must(t, server.Send([]byte("before"), fipc.Forever)) // the server's Conn is out of reach once this returns
	}()
	client := <-connecting
	must(t, client.err)
	defer client.conn.Close()
	msg, err := client.conn.Receive(tenSeconds)
	must(t, err)
	if string(msg) != "before" {
		t.Fatalf("got %q", msg)
	}
	for {
		runtime.GC()
		_, err := client.conn.Receive(10 * time.Millisecond)
		if errors.Is(err, fipc.ErrDisconnected) {
			break
		}
		is(t, err, fipc.ErrTimeout, "Receive while the server's Conn lives")
	}
}

func TestTheGarbageCollectorClosesAnUnreachableListener(t *testing.T) {
	guard(t, 30*time.Second)
	name := uniqueName("gc_listener")
	func() {
		_, err := fipc.Listen(name, 1<<16)
		must(t, err)
	}()
	for {
		runtime.GC()
		listener, err := fipc.Listen(name, 1<<16)
		if err == nil {
			must(t, listener.Close())
			break
		}
		is(t, err, fipc.ErrAddrInUse, "Listen while the first Listener lives")
		time.Sleep(10 * time.Millisecond)
	}
}

// An Accept that polls (NoWait) offers the rings and times out; the client's Connect returns. The listener is closed
// before another Accept takes the connection: the client gets ErrDisconnected.
func TestAListenerClosedBeforeItsAcceptReturnsDisconnectsTheClient(t *testing.T) {
	guard(t, 30*time.Second)
	name := uniqueName("unaccepted")
	listener, err := fipc.Listen(name, 1<<16)
	must(t, err)
	connecting := connectAside(name)
	var accepted *fipc.Conn
	var client connected
	for client.conn == nil && client.err == nil {
		if accepted == nil {
			accepted, _ = listener.Accept(fipc.NoWait) // the client may answer within the call that offered the rings
		}
		select {
		case client = <-connecting:
		case <-time.After(20 * time.Millisecond):
		}
	}
	must(t, client.err)
	defer client.conn.Close()
	if accepted != nil {
		must(t, accepted.Close())
	}
	must(t, listener.Close())
	_, err = client.conn.Receive(tenSeconds)
	is(t, err, fipc.ErrDisconnected, "the client of a closed listener")
}
