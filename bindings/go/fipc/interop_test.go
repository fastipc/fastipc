package fipc_test

// Cross-language: this binding against the Python binding (bindings/python, the package fipc) in another process,
// both ways, with plain messages and RPC. The interpreter is FIPC_TEST_PYTHON, else the repository's venv, else
// python3 or python; it needs cffi. Without one the tests skip, saying so, unless FIPC_REQUIRE_INTEROP is set.

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"testing"
	"time"

	"github.com/fastipc/fastipc/bindings/go/fipc"
)

// pythonPeer is the Python side: peer.py client|server <name> <bindings/python>.
const pythonPeer = `
import sys
sys.path.insert(0, sys.argv[3])  # the repository's bindings/python
from fipc import Conn, FipcError, Listener, Result, RPC_REQUEST

mode, name = sys.argv[1], sys.argv[2]
if mode == "client":
    # A plain connection: echo each message reversed until the server's end
    with Conn.connect(name, timeout_ms=20000) as conn:
        try:
            while True:
                conn.send(conn.recv(timeout_ms=20000)[::-1], timeout_ms=20000)
        except FipcError as e:
            if e.result != Result.DISCONNECTED:
                raise
    # An RPC connection: call the Go server, then answer its call
    with Conn.connect(name, timeout_ms=20000) as conn:
        request_id = conn.rpc_submit(7, "hello from Python".encode(), timeout_ms=20000)
        reply = conn.rpc_recv(timeout_ms=20000)
        assert (reply.id, reply.status, reply.payload) == (request_id, 42, b"HELLO FROM PYTHON"), reply
        request = conn.rpc_recv(timeout_ms=20000)
        assert request.kind == RPC_REQUEST and request.opcode == 9, request
        conn.rpc_respond(request.id, 9, status=len(request.payload), data=request.payload * 2, timeout_ms=20000)
        try:
            conn.rpc_recv(timeout_ms=20000)
            raise AssertionError("expected the server's end")
        except FipcError as e:
            if e.result != Result.DISCONNECTED:
                raise
else:
    with Listener(name, 1 << 16) as listener, listener.accept(timeout_ms=20000) as conn:
        while True:
            try:
                request = conn.rpc_recv(timeout_ms=20000)
            except FipcError as e:
                if e.result == Result.DISCONNECTED:
                    break
                raise
            conn.rpc_respond(request.id, request.opcode, status=-1, data=request.payload.upper(), timeout_ms=20000)
`

// python is an interpreter with cffi; the test skips without one, unless FIPC_REQUIRE_INTEROP is set.
func python(t *testing.T) string {
	t.Helper()
	venv := filepath.Join(repository(t), "venv", "bin", "python")
	if runtime.GOOS == "windows" {
		venv = filepath.Join(repository(t), "venv", "Scripts", "python.exe")
	}
	candidates := []string{os.Getenv("FIPC_TEST_PYTHON"), venv, "python3", "python"}
	for _, candidate := range candidates {
		if candidate != "" && exec.Command(candidate, "-c", "import cffi").Run() == nil {
			return candidate
		}
	}
	if os.Getenv("FIPC_REQUIRE_INTEROP") != "" {
		t.Fatal("no Python with cffi (set FIPC_TEST_PYTHON)")
	}
	t.Skip("no Python with cffi (set FIPC_TEST_PYTHON)")
	return ""
}

// startPython runs the Python peer as mode on name.
func startPython(t *testing.T, interpreter, mode, name string) *exec.Cmd {
	t.Helper()
	script := filepath.Join(t.TempDir(), "peer.py")
	must(t, os.WriteFile(script, []byte(pythonPeer), 0o644))
	cmd := exec.Command(interpreter, script, mode, name, filepath.Join(repository(t), "bindings", "python"))
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	must(t, cmd.Start())
	t.Cleanup(func() {
		if cmd.ProcessState == nil {
			cmd.Process.Kill()
			cmd.Wait()
		}
	})
	return cmd
}

// A Go server and a Python client: plain messages, then RPC in both directions.
func TestGoServerPythonClient(t *testing.T) {
	guard(t, 120*time.Second)
	interpreter := python(t)
	name := uniqueName("py_client")
	listener, err := fipc.Listen(name, 1<<16)
	must(t, err)
	defer listener.Close()
	peer := startPython(t, interpreter, "client", name)

	conn, err := listener.Accept(peerStart)
	must(t, err)
	for _, size := range []int{1, 100, 70_000, 200_000} {
		msg := pattern(size)
		sent := make(chan error, 1)
		go func() { sent <- conn.Send(msg, tenSeconds) }() // longer than the ring: send while receiving
		reversed, err := conn.Receive(tenSeconds)
		must(t, err)
		must(t, <-sent)
		slices.Reverse(msg)
		if !bytes.Equal(reversed, msg) {
			t.Fatalf("size %d: not the message reversed", size)
		}
	}
	must(t, conn.Close())

	conn, err = listener.Accept(peerStart)
	must(t, err)
	request, err := conn.RPCReceive(tenSeconds)
	must(t, err)
	if request.Kind != fipc.RPCRequest || request.Opcode != 7 || string(request.Payload) != "hello from Python" {
		t.Fatalf("request %+v", request)
	}
	must(t, conn.RPCRespond(request.ID, 7, 42, []byte("HELLO FROM PYTHON"), tenSeconds))
	id, err := conn.RPCSubmit(9, []byte("Go"), tenSeconds)
	must(t, err)
	reply, err := conn.RPCReceive(tenSeconds)
	must(t, err)
	if reply.Kind != fipc.RPCResponse || reply.ID != id || reply.Status != 2 || string(reply.Payload) != "GoGo" {
		t.Fatalf("reply %+v", reply)
	}
	must(t, conn.Close())
	if err := peer.Wait(); err != nil {
		t.Fatalf("the Python peer failed: %v", err)
	}
}

// A Python server and a Go client over RPC; the Go client's end ends the Python server.
func TestPythonServerGoClient(t *testing.T) {
	guard(t, 120*time.Second)
	interpreter := python(t)
	name := uniqueName("py_server")
	peer := startPython(t, interpreter, "server", name)

	conn, err := fipc.Connect(name, peerStart)
	must(t, err)
	first, err := conn.RPCSubmit(1, []byte("shared memory"), tenSeconds)
	must(t, err)
	big := pattern(300_000)
	submitted := make(chan uint64, 1)
	go func() {
		id, err := conn.RPCSubmit(2, big, tenSeconds)
		if err != nil {
			t.Error(err)
		}
		submitted <- id
	}()
	one, err := conn.RPCReceive(tenSeconds)
	must(t, err)
	if one.ID != first || one.Status != -1 || string(one.Payload) != "SHARED MEMORY" {
		t.Fatalf("first reply %+v", one)
	}
	two, err := conn.RPCReceive(tenSeconds)
	must(t, err)
	if two.ID != <-submitted || !bytes.Equal(two.Payload, asciiUpper(big)) {
		t.Fatalf("second reply: id %d, %d bytes", two.ID, len(two.Payload))
	}
	_, err = conn.RPCReceive(fipc.NoWait)
	is(t, err, fipc.ErrTimeout, "nothing more")
	must(t, conn.Close())
	if err := peer.Wait(); err != nil {
		t.Fatalf("the Python peer failed: %v", err)
	}
}

// asciiUpper is b with a-z in upper case, as Python's bytes.upper.
func asciiUpper(b []byte) []byte {
	upper := slices.Clone(b)
	for i, c := range upper {
		if 'a' <= c && c <= 'z' {
			upper[i] = c - 'a' + 'A'
		}
	}
	return upper
}
