package fipc

// The cost of a call through the binding: round trips of 16-byte messages between two goroutines, one goroutine's
// send and receive (no waiting) through the API, through purego.SyscallN alone and through functions purego.RegisterFunc
// made, and the uncontended direction mutex. Run with go test -run - -bench . -benchmem.

import (
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/ebitengine/purego"
)

var benchCounter atomic.Uint32

// benchPair is a connected server and client, closed when the benchmark ends.
func benchPair(b *testing.B) (server, client *Conn) {
	b.Helper()
	name := "fipc_go_bench_" + strconv.Itoa(int(benchCounter.Add(1))) + "_" + strconv.Itoa(time.Now().Nanosecond())
	listener, err := Listen(name, 1<<20)
	if err != nil {
		b.Fatal(err)
	}
	type connected struct {
		conn *Conn
		err  error
	}
	ch := make(chan connected, 1)
	go func() {
		conn, err := Connect(name, 10*time.Second)
		ch <- connected{conn, err}
	}()
	server, err = listener.Accept(10 * time.Second)
	if err != nil {
		b.Fatal(err)
	}
	c := <-ch
	if c.err != nil {
		b.Fatal(c.err)
	}
	b.Cleanup(func() {
		c.conn.Close()
		server.Close()
		listener.Close()
	})
	return server, c.conn
}

// echo answers every message on conn until it is closed.
func echo(conn *Conn, into bool, wg *sync.WaitGroup) {
	defer wg.Done()
	buf := make([]byte, 64)
	for {
		var msg []byte
		var err error
		if into {
			var n int
			n, err = conn.ReceiveInto(buf, Forever)
			msg = buf[:n]
		} else {
			msg, err = conn.Receive(Forever)
		}
		if err != nil || conn.Send(msg, Forever) != nil {
			return
		}
	}
}

// BenchmarkRoundTrip16: one round trip of a 16-byte message per op, to an echo on another goroutine.
func BenchmarkRoundTrip16(b *testing.B) {
	for _, into := range []bool{false, true} {
		name := "Receive"
		if into {
			name = "ReceiveInto"
		}
		b.Run(name, func(b *testing.B) {
			server, client := benchPair(b)
			var wg sync.WaitGroup
			wg.Add(1)
			go echo(server, into, &wg)
			msg := make([]byte, 16)
			buf := make([]byte, 64)
			b.ReportAllocs()
			for b.Loop() {
				if err := client.Send(msg, Forever); err != nil {
					b.Fatal(err)
				}
				if into {
					if _, err := client.ReceiveInto(buf, Forever); err != nil {
						b.Fatal(err)
					}
				} else if _, err := client.Receive(Forever); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			server.Cancel()
			wg.Wait()
		})
	}
}

// BenchmarkSendReceive16: one goroutine sends a 16-byte message and receives it on the other side, no waiting: the
// cost of two calls, through the API (direction mutexes included), through purego.SyscallN with the arguments one by
// one (as package unsafe's rules for uintptr arguments have it: each call copies them to the heap), and through functions
// purego.RegisterFunc made.
func BenchmarkSendReceive16(b *testing.B) {
	server, client := benchPair(b)
	msg := make([]byte, 16)
	buf := make([]byte, 64)

	b.Run("API", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if err := client.Send(msg, NoWait); err != nil {
				b.Fatal(err)
			}
			if _, err := server.ReceiveInto(buf, NoWait); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("APIReceive", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if err := client.Send(msg, NoWait); err != nil {
				b.Fatal(err)
			}
			if _, err := server.Receive(NoWait); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("SyscallN", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			r, _, _ := purego.SyscallN(lib.send, client.h, uintptr(unsafe.Pointer(&msg[0])), 16, 0)
			if int32(r) != codeOK {
				b.Fatal(result(r, 0))
			}
			r, _, _ = purego.SyscallN(lib.recv, server.h, uintptr(unsafe.Pointer(&buf[0])), 64,
				uintptr(unsafe.Pointer(&server.rx.len)), 0)
			if int32(r) != codeOK {
				b.Fatal(result(r, 0))
			}
		}
	})

	b.Run("RegisterFunc", func(b *testing.B) {
		var send func(conn uintptr, data *byte, n uintptr, ms int32) int32
		var recv func(conn uintptr, buf *byte, n uintptr, out *uintptr, ms int32) int32
		purego.RegisterFunc(&send, lib.send)
		purego.RegisterFunc(&recv, lib.recv)
		var n uintptr
		b.ReportAllocs()
		for b.Loop() {
			if r := send(client.h, &msg[0], 16, 0); r != codeOK {
				b.Fatal(result(uintptr(r), 0))
			}
			if r := recv(server.h, &buf[0], 64, &n, 0); r != codeOK {
				b.Fatal(result(uintptr(r), 0))
			}
		}
	})
}

// BenchmarkMaxPiece: the cheapest call of the library (a field read), through purego: the FFI's own cost.
func BenchmarkMaxPiece(b *testing.B) {
	_, client := benchPair(b)
	b.Run("SyscallN", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			purego.SyscallN(lib.maxPiece, client.h)
		}
	})
	b.Run("ffi", func(b *testing.B) {
		b.ReportAllocs()
		args := []uintptr{client.h}
		for b.Loop() {
			ffi(lib.maxPiece, args)
		}
	})
	b.Run("RegisterFunc", func(b *testing.B) {
		var maxPiece func(conn uintptr) uintptr
		purego.RegisterFunc(&maxPiece, lib.maxPiece)
		b.ReportAllocs()
		for b.Loop() {
			maxPiece(client.h)
		}
	})
}

// BenchmarkDirectionMutex: what each call adds for goroutine safety, uncontended: the direction mutex and the closed
// flag.
func BenchmarkDirectionMutex(b *testing.B) {
	var c Conn
	for b.Loop() {
		c.tx.mu.Lock()
		if c.closed.Load() {
			b.Fatal("closed")
		}
		c.tx.mu.Unlock()
	}
}
