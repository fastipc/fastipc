// FastIPC's Go benchmark, through the binding (bindings/go, package fipc): one-way throughput between this process, a
// server that receives and times, and a client process (this program again) that sends. The same cases, output and
// checks as the C, C++, Python, C#, Java, Rust, Lua and JavaScript benchmarks.
//
//	go run . copy|zerocopy|rpc   (in bench/go; devtool bench go-fastipc builds it and runs it)
//
//	copy      conn.Send / conn.ReceiveInto a buffer the server reuses
//	zerocopy  SendAcquire + copy + SendCommit / ReceiveAcquire, copied out, + ReceiveRelease (a message of several
//	          pieces through the copying calls)
//	rpc       RPCSubmit / RPCReceiveInto a buffer the server reuses
//
// Each case first sends messages untimed for 0.2 s (the warm-up; at least its count), then a start marker, then its
// count, and the server times from the last start marker to the end marker (see client). Each case prints
// "Test i/n: name" and "Throughput: n messages/sec" (devtool bench-compare reads them).
package main

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"time"

	"github.com/fastipc/fastipc/bindings/go/fipc"
)

const (
	connectTimeout = 15 * time.Second
	dataOpcode     = 1
	endOpcode      = 0xFB000001
	goOpcode       = 0xFB000002
	warmUp         = 200 * time.Millisecond
	markerEvery    = 1000
	atEnd          = -1
	atGo           = -2
)

var (
	end      = []byte("END")
	goMarker = []byte("GO!")
)

type benchCase struct {
	count, ring, size int
	name              string
}

var cases = []benchCase{
	{2_000_000, 512 * 1024, 16, "Tiny messages (16B)"},
	{2_000_000, 512 * 1024, 64, "Small messages (64B)"},
	{1_000_000, 512 * 1024, 256, "Medium messages (256B)"},
	{1000, 512 * 1024, 64 * 1024, "Large messages (64KB)"},
	{1000, 2 * 1024 * 1024, 512 * 1024, "Large messages (512KB)"},
	{10, 512 * 1024, 1024 * 1024, "Exceeds buffer (1MB msg, 512KB buffer)"},
}

func main() {
	mode := "copy"
	if len(os.Args) > 1 {
		mode = os.Args[1]
	}
	if mode == "client" {
		size, _ := strconv.Atoi(os.Args[4])
		count, _ := strconv.Atoi(os.Args[5])
		if err := client(os.Args[2], os.Args[3], size, count); err != nil {
			fmt.Fprintf(os.Stderr, "client: %v\n", err)
			os.Exit(1)
		}
		return
	}
	if mode != "copy" && mode != "zerocopy" && mode != "rpc" {
		fmt.Fprintln(os.Stderr, "usage: fipc_bench copy|zerocopy|rpc")
		os.Exit(2)
	}
	fmt.Printf("FastIPC Go benchmark: %s, %s\n\n", mode, runtime.Version())
	passed := 0
	for i, c := range cases {
		fmt.Printf("Test %d/%d: %s\n", i+1, len(cases), c.name)
		if runCase(mode, c, i+1) {
			passed++
		} else {
			fmt.Println("FAILED")
		}
		fmt.Println()
	}
	fmt.Printf("Summary: %d/%d tests passed\n", passed, len(cases))
	if passed != len(cases) {
		os.Exit(1)
	}
}

func runCase(mode string, c benchCase, index int) bool {
	name := fmt.Sprintf("gobench_%d_%d", os.Getpid(), index)
	listener, err := fipc.Listen(name, c.ring)
	if err != nil {
		fmt.Fprintf(os.Stderr, "listen: %v\n", err)
		return false
	}
	exe, err := os.Executable()
	if err != nil {
		fmt.Fprintf(os.Stderr, "the executable: %v\n", err)
		listener.Close()
		return false
	}
	cmd := exec.Command(exe, "client", mode, name, strconv.Itoa(c.size), strconv.Itoa(c.count))
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "the client: %v\n", err)
		listener.Close()
		return false
	}
	var messages, total int
	var seconds float64
	conn, err := listener.Accept(connectTimeout)
	if err == nil {
		messages, total, seconds, err = serve(mode, conn, c.size)
		conn.Close()
	}
	listener.Close()
	if err != nil {
		cmd.Process.Kill()
		cmd.Wait()
		fmt.Fprintf(os.Stderr, "server: %v\n", err)
		return false
	}
	if err := cmd.Wait(); err != nil {
		fmt.Fprintf(os.Stderr, "the client failed: %v\n", err)
		return false
	}
	if messages != c.count || total != c.count*c.size {
		fmt.Fprintf(os.Stderr, "expected %d messages of %d B, got %d (%d B)\n", c.count, c.size, messages, total)
		return false
	}
	fmt.Printf("Messages: %d | Ring: %dKB | Size: %dB\n", c.count, c.ring/1024, c.size)
	fmt.Printf("Duration: %.3fs\n", seconds)
	fmt.Printf("Throughput: %.0f messages/sec, %.1f MB/sec\n", float64(messages)/seconds,
		float64(total)/seconds/float64(1<<20))
	return true
}

// serve skips the warm-up up to the last start marker, then receives until the end marker; the messages, their bytes
// and the seconds.
func serve(mode string, conn *fipc.Conn, size int) (messages, total int, seconds float64, err error) {
	buf := make([]byte, max(size, len(end)))
	sink := make([]byte, size)
	timing := false
	var start time.Time
	for {
		n, err := receiveOne(mode, conn, buf, sink)
		switch {
		case err != nil:
			return 0, 0, 0, err
		case n == atEnd:
			return messages, total, time.Since(start).Seconds(), nil
		case n == atGo: // the last one starts the timed messages
			timing, messages, total, start = true, 0, 0, time.Now()
		case timing:
			messages++
			total += n
		}
	}
}

// receiveOne receives one message, as a consumer would: its length, or atGo or atEnd for the markers.
func receiveOne(mode string, conn *fipc.Conn, buf, sink []byte) (int, error) {
	switch mode {
	case "rpc":
		header, err := conn.RPCReceiveInto(buf, fipc.Forever)
		if err != nil {
			return 0, err
		}
		switch header.Opcode {
		case endOpcode:
			return atEnd, nil
		case goOpcode:
			return atGo, nil
		}
		return header.Len, nil
	case "zerocopy":
		msg, err := conn.ReceiveAcquire(fipc.Forever)
		if errors.Is(err, fipc.ErrTooLarge) { // several pieces
			return conn.ReceiveInto(buf, fipc.Forever)
		}
		if err != nil {
			return 0, err
		}
		n := marker(msg)
		if n == 0 {
			n = copy(sink, msg) // a consumer copies the message out
		}
		conn.ReceiveRelease()
		return n, nil
	default:
		n, err := conn.ReceiveInto(buf, fipc.Forever)
		if err != nil {
			return 0, err
		}
		if m := marker(buf[:n]); m != 0 {
			return m, nil
		}
		return n, nil
	}
}

// marker is atEnd or atGo for a marker, else 0.
func marker(msg []byte) int {
	switch {
	case bytes.Equal(msg, end):
		return atEnd
	case bytes.Equal(msg, goMarker):
		return atGo
	}
	return 0
}

// client connects and sends messages untimed for the warm-up (the count, and for at least warmUp), the start marker,
// the count and the end marker, then closes the connection (the server still receives everything sent before). The
// warm-up sends a start marker every markerEvery messages too, as the other benchmarks do.
func client(mode, name string, size, count int) error {
	conn, err := fipc.Connect(name, connectTimeout)
	if err != nil {
		return err
	}
	defer conn.Close()
	payload := bytes.Repeat([]byte{'x'}, size)
	onePiece := size <= conn.MaxPiece()
	warm := time.Now().Add(warmUp)
	for i := 0; i < count || time.Now().Before(warm); i++ {
		if i%markerEvery == 0 {
			if err := sendMarker(mode, conn, goMarker, goOpcode); err != nil {
				return err
			}
		}
		if err := sendOne(mode, conn, payload, onePiece); err != nil {
			return err
		}
	}
	if err := sendMarker(mode, conn, goMarker, goOpcode); err != nil {
		return err
	}
	for range count {
		if err := sendOne(mode, conn, payload, onePiece); err != nil {
			return err
		}
	}
	return sendMarker(mode, conn, end, endOpcode)
}

func sendMarker(mode string, conn *fipc.Conn, marker []byte, opcode uint32) error {
	if mode == "rpc" {
		_, err := conn.RPCSubmit(opcode, nil, fipc.Forever)
		return err
	}
	return conn.Send(marker, fipc.Forever)
}

func sendOne(mode string, conn *fipc.Conn, payload []byte, onePiece bool) error {
	switch {
	case mode == "rpc":
		_, err := conn.RPCSubmit(dataOpcode, payload, fipc.Forever)
		return err
	case mode == "zerocopy" && onePiece:
		slot, err := conn.SendAcquire(len(payload), fipc.Forever)
		if err != nil {
			return err
		}
		copy(slot, payload)
		return conn.SendCommit(len(payload))
	default:
		return conn.Send(payload, fipc.Forever)
	}
}
