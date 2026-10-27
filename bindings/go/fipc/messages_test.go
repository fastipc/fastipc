package fipc_test

// Copied messages, zero-copy, timeouts and error results, between two connections in this process.

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/fastipc/fastipc/bindings/go/fipc"
)

func TestMessagesOfEverySizeArriveWhole(t *testing.T) {
	guard(t, 60*time.Second)
	p := newPair(t, "sizes", 1<<16)
	piece := p.server.MaxPiece()
	if piece != 1<<16-64 {
		t.Fatalf("MaxPiece = %d", piece)
	}
	sizes := []int{1, 15, 16, 17, 1000, 4095, 4096, 4097, piece - 1, piece, piece + 1, 200_000, 3_000_000}
	// A message longer than the ring needs a receiver at the same time: the client sends from a goroutine
	sent := make(chan error, 1)
	go func() {
		for _, size := range sizes {
			if err := p.client.Send(pattern(size), tenSeconds); err != nil {
				sent <- err
				return
			}
		}
		sent <- nil
	}()
	for _, size := range sizes {
		msg, err := p.server.Receive(tenSeconds)
		must(t, err)
		if !bytes.Equal(msg, pattern(size)) {
			t.Fatalf("size %d: got %d bytes, not the pattern", size, len(msg))
		}
	}
	must(t, <-sent)
}

func TestReceiveIntoReportsTheRoomAMessageNeeds(t *testing.T) {
	guard(t, 30*time.Second)
	p := newPair(t, "into", ring)
	must(t, p.client.Send(pattern(300), fipc.Forever))
	small := make([]byte, 100)
	for _, buf := range [][]byte{small, nil} { // nil: the length only; the message stays queued
		_, err := p.server.ReceiveInto(buf, tenSeconds)
		var e *fipc.Error
		if !errors.As(err, &e) || !errors.Is(err, fipc.ErrTooLarge) || e.Len() != 300 {
			t.Fatalf("ReceiveInto(%d bytes): %v", len(buf), err)
		}
	}
	buf := make([]byte, 512)
	n, err := p.server.ReceiveInto(buf, tenSeconds)
	must(t, err)
	if n != 300 || !bytes.Equal(buf[:n], pattern(300)) {
		t.Fatalf("ReceiveInto: %d bytes", n)
	}
	_, err = p.server.ReceiveInto(buf, fipc.NoWait)
	is(t, err, fipc.ErrTimeout, "nothing more")
}

func TestAnEmptyMessageIsInvalid(t *testing.T) {
	guard(t, 30*time.Second)
	p := newPair(t, "empty", ring)
	is(t, p.client.Send(nil, fipc.Forever), fipc.ErrInvalid, "Send(nil)")
	is(t, p.client.Send([]byte{}, fipc.Forever), fipc.ErrInvalid, "Send(empty)")
	_, err := p.server.Receive(fipc.NoWait)
	is(t, err, fipc.ErrTimeout, "Receive")
}

func TestMessagesArriveInOrderBothWays(t *testing.T) {
	guard(t, 30*time.Second)
	p := newPair(t, "order", ring)
	for i := range uint32(1000) {
		must(t, p.client.Send(binary.LittleEndian.AppendUint32(nil, i), fipc.Forever))
		must(t, p.server.Send(binary.LittleEndian.AppendUint32(nil, i+1), fipc.Forever))
	}
	for i := range uint32(1000) {
		msg, err := p.server.Receive(tenSeconds)
		must(t, err)
		if binary.LittleEndian.Uint32(msg) != i {
			t.Fatalf("server: message %d is %v", i, msg)
		}
		msg, err = p.client.Receive(tenSeconds)
		must(t, err)
		if binary.LittleEndian.Uint32(msg) != i+1 {
			t.Fatalf("client: message %d is %v", i, msg)
		}
	}
}

func TestZeroCopyRoundTrip(t *testing.T) {
	guard(t, 60*time.Second)
	p := newPair(t, "zerocopy", ring)
	piece := p.client.MaxPiece()
	sizes := []int{1, 16, 100, 4096, piece, 3, piece}
	// A frame never wraps: one that doesn't fit before the ring's end waits for the receiver to pass the end, so the
	// receiver runs at the same time
	sent := make(chan error, 1)
	go func() {
		for _, size := range sizes {
			room, err := p.client.SendAcquire(size, tenSeconds)
			if err != nil {
				sent <- err
				return
			}
			if len(room) != size || uintptrOf(room)%16 != 0 {
				sent <- errors.New("SendAcquire: wrong length or not 16-byte aligned")
				return
			}
			copy(room, pattern(size))
			if err := p.client.SendCommit(size); err != nil {
				sent <- err
				return
			}
		}
		sent <- nil
	}()
	for _, size := range sizes {
		msg, err := p.server.ReceiveAcquire(tenSeconds)
		must(t, err)
		if uintptrOf(msg)%16 != 0 {
			t.Fatal("ReceiveAcquire: not 16-byte aligned")
		}
		if !bytes.Equal(msg, pattern(size)) {
			t.Fatalf("size %d: got %d bytes, not the pattern", size, len(msg))
		}
		p.server.ReceiveRelease()
	}
	must(t, <-sent)

	// A commit of part of the room sends that part; the copying calls see zero-copy messages and vice versa
	room, err := p.client.SendAcquire(64, fipc.Forever)
	must(t, err)
	copy(room, "hello")
	must(t, p.client.SendCommit(5))
	must(t, p.client.Send([]byte("copied"), fipc.Forever))
	msg, err := p.server.Receive(tenSeconds)
	must(t, err)
	if string(msg) != "hello" {
		t.Fatalf("got %q", msg)
	}
	msg, err = p.server.ReceiveAcquire(tenSeconds)
	must(t, err)
	if string(msg) != "copied" {
		t.Fatalf("got %q", msg)
	}
	_, err = p.server.Receive(fipc.NoWait) // releases the acquired message first
	is(t, err, fipc.ErrTimeout, "nothing more")
	p.server.ReceiveRelease() // nothing acquired: does nothing
}

func TestAnAbandonedReservationSendsNothing(t *testing.T) {
	guard(t, 30*time.Second)
	p := newPair(t, "abandon", ring)
	room, err := p.client.SendAcquire(32, fipc.Forever)
	must(t, err)
	copy(room, bytes.Repeat([]byte("X"), 32))
	_, err = p.server.Receive(fipc.NoWait)
	is(t, err, fipc.ErrTimeout, "an uncommitted reservation")
	must(t, p.client.Send([]byte("after"), fipc.Forever)) // drops the reservation
	msg, err := p.server.Receive(tenSeconds)
	must(t, err)
	if string(msg) != "after" {
		t.Fatalf("got %q", msg)
	}
	_, err = p.server.Receive(fipc.NoWait)
	is(t, err, fipc.ErrTimeout, "nothing more")
	is(t, p.client.SendCommit(1), fipc.ErrInvalid, "a commit without a reservation")
}

func TestACommitOutOfRangeSendsNothing(t *testing.T) {
	guard(t, 30*time.Second)
	p := newPair(t, "commit", ring)
	_, err := p.client.SendAcquire(8, fipc.Forever)
	must(t, err)
	is(t, p.client.SendCommit(0), fipc.ErrInvalid, "commit 0")
	is(t, p.client.SendCommit(9), fipc.ErrInvalid, "commit 9")
	is(t, p.client.SendCommit(-1), fipc.ErrInvalid, "commit -1")
	_, err = p.server.Receive(fipc.NoWait)
	is(t, err, fipc.ErrTimeout, "nothing sent")
	must(t, p.client.SendCommit(8)) // the reservation stayed
	msg, err := p.server.Receive(tenSeconds)
	must(t, err)
	if len(msg) != 8 {
		t.Fatalf("got %d bytes", len(msg))
	}
}

func TestZeroCopyTakesOnePiece(t *testing.T) {
	guard(t, 30*time.Second)
	p := newPair(t, "zc_large", 1<<16)
	piece := p.client.MaxPiece()
	_, err := p.client.SendAcquire(piece+1, fipc.NoWait)
	var e *fipc.Error
	if !errors.As(err, &e) || !errors.Is(err, fipc.ErrTooLarge) || e.Len() != piece+1 {
		t.Fatalf("SendAcquire(MaxPiece+1): %v", err)
	}
	_, err = p.client.SendAcquire(0, fipc.NoWait)
	is(t, err, fipc.ErrInvalid, "SendAcquire(0)")
	_, err = p.client.SendAcquire(-1, fipc.NoWait)
	is(t, err, fipc.ErrInvalid, "SendAcquire(-1)")
	sent := make(chan error, 1)
	go func() { sent <- p.client.Send(pattern(100_000), tenSeconds) }()
	// A message in several pieces stays queued for the copying calls
	_, err = p.server.ReceiveAcquire(tenSeconds)
	if !errors.As(err, &e) || !errors.Is(err, fipc.ErrTooLarge) || e.Len() != 100_000 {
		t.Fatalf("ReceiveAcquire of a message in pieces: %v", err)
	}
	msg, err := p.server.Receive(tenSeconds)
	must(t, err)
	if !bytes.Equal(msg, pattern(100_000)) {
		t.Fatal("not the pattern")
	}
	must(t, <-sent)
}

func TestTimeouts(t *testing.T) {
	guard(t, 30*time.Second)
	p := newPair(t, "timeouts", 1024)
	_, err := p.server.Receive(fipc.NoWait)
	is(t, err, fipc.ErrTimeout, "Receive")
	_, err = p.server.ReceiveAcquire(fipc.NoWait)
	is(t, err, fipc.ErrTimeout, "ReceiveAcquire")
	_, err = p.listener.Accept(fipc.NoWait)
	is(t, err, fipc.ErrInvalid, "Accept while the last connection is open")

	start := time.Now()
	_, err = p.server.Receive(50 * time.Millisecond)
	is(t, err, fipc.ErrTimeout, "Receive(50 ms)")
	if elapsed := time.Since(start); elapsed < 45*time.Millisecond {
		t.Fatalf("Receive(50 ms) returned after %v", elapsed)
	}
	var timeout interface{ Timeout() bool }
	if !errors.As(err, &timeout) || !timeout.Timeout() {
		t.Fatal("ErrTimeout.Timeout() should be true")
	}

	// A full ring: the sender times out and changes nothing
	for p.client.Send(bytes.Repeat([]byte{1}, 100), fipc.NoWait) == nil {
	}
	is(t, p.client.Send(bytes.Repeat([]byte{1}, 100), 20*time.Millisecond), fipc.ErrTimeout, "Send to a full ring")
	_, err = p.client.SendAcquire(100, fipc.NoWait)
	is(t, err, fipc.ErrTimeout, "SendAcquire in a full ring")
	msg, err := p.server.Receive(tenSeconds)
	must(t, err)
	if !bytes.Equal(msg, bytes.Repeat([]byte{1}, 100)) {
		t.Fatal("not the first message")
	}
	must(t, p.client.Send(bytes.Repeat([]byte{2}, 100), fipc.NoWait))

	nobody := uniqueName("nobody")
	_, err = fipc.ConnectStream(nobody, fipc.NoWait)
	is(t, err, fipc.ErrTimeout, "ConnectStream(NoWait)")
	_, err = fipc.ConnectStream(nobody, 30*time.Millisecond)
	is(t, err, fipc.ErrTimeout, "ConnectStream(30 ms)")
}

func TestCreateStreamErrors(t *testing.T) {
	guard(t, 30*time.Second)
	for _, bad := range []string{"", ".hidden", "-dash", "a b", "slash/name", "nul\x00name", strings.Repeat("x", 246)} {
		_, err := fipc.CreateStream(bad, 1<<16)
		is(t, err, fipc.ErrInvalid, "CreateStream("+bad+")")
	}
	name := uniqueName("errors")
	for _, bad := range []int{-1, 0, 1000, 1023, 1025, 3 << 20} {
		_, err := fipc.CreateStream(name, bad)
		is(t, err, fipc.ErrInvalid, "a bad capacity")
	}
	listener, err := fipc.CreateStream(name, 1024)
	must(t, err)
	if listener.Name() != name {
		t.Fatalf("Name() = %q", listener.Name())
	}
	_, err = fipc.CreateStream(name, 1024)
	is(t, err, fipc.ErrAddrInUse, "a name in use")
	must(t, listener.Close())
	again, err := fipc.CreateStream(name, 1024) // free again
	must(t, err)
	must(t, again.Close())
	_, err = fipc.ConnectStream("bad name", fipc.NoWait)
	is(t, err, fipc.ErrInvalid, "ConnectStream(bad name)")
	_, err = fipc.ConnectStream("nul\x00name", fipc.NoWait)
	is(t, err, fipc.ErrInvalid, "ConnectStream(nul)")
}

func TestOneClientAtATime(t *testing.T) {
	guard(t, 30*time.Second)
	name := uniqueName("one_at_a_time")
	listener, err := fipc.CreateStream(name, 1<<16)
	must(t, err)
	defer listener.Close()
	connecting := connectAside(name)
	server, err := listener.Accept(tenSeconds)
	must(t, err)
	first := <-connecting
	must(t, first.err)
	if server.Name() != name || first.conn.Name() != name {
		t.Fatalf("names %q, %q", server.Name(), first.conn.Name())
	}
	_, err = listener.Accept(fipc.NoWait)
	is(t, err, fipc.ErrInvalid, "a second Accept")
	must(t, server.Close())
	must(t, first.conn.Close())
	connecting = connectAside(name)
	server, err = listener.Accept(tenSeconds)
	must(t, err)
	defer server.Close()
	second := <-connecting
	must(t, second.err)
	defer second.conn.Close()
	if second.conn.MaxPiece() != 1<<16-64 {
		t.Fatalf("MaxPiece = %d", second.conn.MaxPiece())
	}
}

func TestErrorsNameTheirResult(t *testing.T) {
	cases := []struct {
		err  *fipc.Error
		code int
		name string
	}{
		{fipc.ErrTimeout, 1, "FIPC_TIMEOUT"},
		{fipc.ErrDisconnected, 2, "FIPC_DISCONNECTED"},
		{fipc.ErrCancelled, 3, "FIPC_CANCELLED"},
		{fipc.ErrTooLarge, 4, "FIPC_TOO_LARGE"},
		{fipc.ErrInvalid, 5, "FIPC_INVALID"},
		{fipc.ErrNoMemory, 6, "FIPC_NO_MEMORY"},
		{fipc.ErrAddrInUse, 7, "FIPC_ADDR_IN_USE"},
		{fipc.ErrOverrun, 8, "FIPC_OVERRUN"},
		{fipc.ErrUnsubscribed, 9, "FIPC_UNSUBSCRIBED"},
		{fipc.ErrAbandoned, 10, "FIPC_ABANDONED"},
	}
	for _, c := range cases {
		if c.err.Code() != c.code || c.err.Name() != c.name || !strings.HasPrefix(c.err.Error(), c.name+": ") {
			t.Errorf("%d: %d %s %q", c.code, c.err.Code(), c.err.Name(), c.err.Error())
		}
		if c.err.Timeout() != (c.code == 1) || c.err.Len() != 0 || c.err.Lost() != 0 {
			t.Errorf("%s: Timeout() %v, Len() %d, Lost() %d", c.name, c.err.Timeout(), c.err.Len(), c.err.Lost())
		}
		for _, other := range cases {
			if errors.Is(c.err, other.err) != (c.code == other.code) {
				t.Errorf("errors.Is(%s, %s)", c.name, other.name)
			}
		}
	}
	if fipc.ErrTimeout.Error() != "FIPC_TIMEOUT: the timeout ran out" {
		t.Errorf("%q", fipc.ErrTimeout.Error())
	}
	if errors.Is(fipc.ErrClosed, fipc.ErrInvalid) || errors.Is(fipc.ErrInvalid, fipc.ErrClosed) {
		t.Error("ErrClosed is the binding's own")
	}
	if errors.Is(fipc.ErrTimeout, os.ErrDeadlineExceeded) {
		t.Error("ErrTimeout isn't os.ErrDeadlineExceeded")
	}

	// A TooLarge a call returned: its length, in its message too
	p := newPair(t, "too_large", ring)
	must(t, p.client.Send(pattern(7), fipc.Forever))
	_, err := p.server.ReceiveInto(nil, tenSeconds)
	var e *fipc.Error
	if !errors.As(err, &e) || e.Len() != 7 || e.Error() != "FIPC_TOO_LARGE: the message (7 bytes) doesn't fit" {
		t.Fatalf("%v", err)
	}
}

func TestTheLibraryIsFound(t *testing.T) {
	path, err := fipc.LibraryPath()
	must(t, err)
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("LibraryPath() = %q: %v", path, err)
	}
	t.Logf("the library: %s", path)
}
