package fipc_test

// What the tests share: unique names, test data, a connected pair, a watchdog per test, the repository, and the peer
// this test binary becomes when the environment names a part for it (process_test.go).

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/fastipc/fastipc/bindings/go/fipc"
)

const (
	ring       = 1 << 20
	tenSeconds = 10 * time.Second
	peerStart  = 30 * time.Second
	peerEnv    = "FIPC_GO_TEST_PEER"
)

func TestMain(m *testing.M) {
	if spec := os.Getenv(peerEnv); spec != "" {
		mode, name, _ := strings.Cut(spec, ":")
		if err := runPeer(mode, name); err != nil {
			fmt.Fprintf(os.Stderr, "peer %s: %v\n", mode, err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	m.Run()
}

var counter atomic.Uint32

// uniqueName is a name no other test (or run) uses.
func uniqueName(prefix string) string {
	return fmt.Sprintf("fipc_go_%s_%d_%d_%d", prefix, os.Getpid(), counter.Add(1), time.Now().Nanosecond()%1_000_000)
}

// pattern is n bytes of a pattern that differs at every offset of a ring's frame.
func pattern(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*31 + 7)
	}
	return b
}

// uintptrOf is the address of b's first byte.
func uintptrOf(b []byte) uintptr {
	return uintptr(unsafe.Pointer(unsafe.SliceData(b)))
}

// guard fails the whole run, with every goroutine's stack, if the test runs longer than limit: each test's own
// timeout.
func guard(t testing.TB, limit time.Duration) {
	t.Helper()
	name := t.Name()
	timer := time.AfterFunc(limit, func() {
		buf := make([]byte, 1<<20)
		buf = buf[:runtime.Stack(buf, true)]
		fmt.Fprintf(os.Stderr, "%s: still running after %v\n%s", name, limit, buf)
		os.Exit(2)
	})
	t.Cleanup(func() { timer.Stop() })
}

// must stops the test at an error.
func must(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// is checks that err is want.
func is(t testing.TB, err, want error, what string) {
	t.Helper()
	if !errors.Is(err, want) {
		t.Fatalf("%s: got %v, want %v", what, err, want)
	}
}

type connected struct {
	conn *fipc.Conn
	err  error
}

// connectAside connects to name on a goroutine of its own, within ten seconds, while the caller accepts: a Connect
// waits for the listener's Accept, so a client and its server in one process run on different goroutines.
func connectAside(name string) <-chan connected {
	ch := make(chan connected, 1)
	go func() {
		conn, err := fipc.Connect(name, tenSeconds)
		ch <- connected{conn, err}
	}()
	return ch
}

// pair is a listener and a server and client connection on one name, closed when the test ends.
type pair struct {
	listener *fipc.Listener
	client   *fipc.Conn
	server   *fipc.Conn
}

func newPair(t testing.TB, prefix string, capacity int) *pair {
	t.Helper()
	name := uniqueName(prefix)
	listener, err := fipc.Listen(name, capacity)
	must(t, err)
	connecting := connectAside(name)
	server, err := listener.Accept(tenSeconds)
	must(t, err)
	client := <-connecting
	must(t, client.err)
	p := &pair{listener, client.conn, server}
	t.Cleanup(func() {
		p.client.Close()
		p.server.Close()
		p.listener.Close()
	})
	return p
}

// repository is the repository's root (this package is bindings/go/fipc, and tests run in its folder).
func repository(t testing.TB) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	must(t, err)
	return root
}
