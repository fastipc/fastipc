package fipc_test

// Cross-language: this binding against the Python binding (bindings/python, the package fipc) in another process,
// both ways, on a stream, on a broadcast (its tests/broadcast_peer.py), on a work queue (its tests/queue_peer.py) and
// on a service (its tests/service_peer.py). The interpreter is FIPC_TEST_PYTHON, else the repository's venv, else
// python3 or python; it needs cffi. Without one the tests skip, saying so, unless FIPC_REQUIRE_INTEROP is set.

import (
	"bufio"
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/fastipc/fastipc/bindings/go/fipc"
)

// pythonPeer is the Python side: peer.py client|server <name> <bindings/python>.
const pythonPeer = `
import sys
sys.path.insert(0, sys.argv[3])  # the repository's bindings/python
from fipc import FipcError, Result, stream_connect, stream_create

mode, name = sys.argv[1], sys.argv[2]
if mode == "client":
    # A stream's client: echo each message reversed until the server's end
    with stream_connect(name, timeout_ms=20000) as conn:
        try:
            while True:
                conn.send(conn.recv(timeout_ms=20000)[::-1], timeout_ms=20000)
        except FipcError as e:
            if e.result != Result.DISCONNECTED:
                raise
else:
    # Answer each message in upper case until the client's end
    with stream_create(name, 1 << 16) as listener, listener.accept(timeout_ms=20000) as conn:
        while True:
            try:
                message = conn.recv(timeout_ms=20000)
            except FipcError as e:
                if e.result == Result.DISCONNECTED:
                    break
                raise
            conn.send(message.upper(), timeout_ms=20000)
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

// A Go server and a Python client on a stream; the Go server's end ends the Python client.
func TestGoServerPythonClient(t *testing.T) {
	guard(t, 120*time.Second)
	interpreter := python(t)
	name := uniqueName("py_client")
	listener, err := fipc.CreateStream(name, 1<<16)
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
	if err := peer.Wait(); err != nil {
		t.Fatalf("the Python peer failed: %v", err)
	}
}

// A Python server and a Go client; the Go client's end ends the Python server.
func TestPythonServerGoClient(t *testing.T) {
	guard(t, 120*time.Second)
	interpreter := python(t)
	name := uniqueName("py_server")
	peer := startPython(t, interpreter, "server", name)

	conn, err := fipc.ConnectStream(name, peerStart)
	must(t, err)
	must(t, conn.Send([]byte("shared memory"), tenSeconds))
	big := pattern(300_000)
	sent := make(chan error, 1)
	go func() { sent <- conn.Send(big, tenSeconds) }() // longer than the ring: send while receiving
	one, err := conn.Receive(tenSeconds)
	must(t, err)
	if string(one) != "SHARED MEMORY" {
		t.Fatalf("first reply %q", one)
	}
	two, err := conn.Receive(tenSeconds)
	must(t, err)
	must(t, <-sent)
	if !bytes.Equal(two, asciiUpper(big)) {
		t.Fatalf("second reply: %d bytes", len(two))
	}
	_, err = conn.Receive(fipc.NoWait)
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

// startBroadcastPeer runs the Python binding's broadcast_peer.py with args.
func startBroadcastPeer(t *testing.T, interpreter string, args ...string) *exec.Cmd {
	t.Helper()
	return startPythonTest(t, interpreter, "broadcast_peer.py", args...)
}

// startPythonTest runs the Python binding's tests/<script> with args.
func startPythonTest(t *testing.T, interpreter, script string, args ...string) *exec.Cmd {
	t.Helper()
	script = filepath.Join(repository(t), "bindings", "python", "tests", script)
	cmd := exec.Command(interpreter, append([]string{script}, args...)...)
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

// A Go publisher and a gated and a lossy Python subscriber.
func TestGoPublisherPythonSubscribers(t *testing.T) {
	guard(t, 120*time.Second)
	interpreter := python(t)
	name := uniqueName("py_subscribers")
	count := strconv.Itoa(broadcastCount)
	gated := startBroadcastPeer(t, interpreter, "subscriber", name, "gated", count)
	lossy := startBroadcastPeer(t, interpreter, "subscriber", name, "lossy", count)
	must(t, publish(name, broadcastCount, 2))
	if err := gated.Wait(); err != nil {
		t.Fatalf("the gated Python subscriber failed: %v", err)
	}
	if err := lossy.Wait(); err != nil {
		t.Fatalf("the lossy Python subscriber failed: %v", err)
	}
}

// A Python publisher and a gated and a lossy Go subscriber.
func TestPythonPublisherGoSubscribers(t *testing.T) {
	guard(t, 120*time.Second)
	interpreter := python(t)
	name := uniqueName("py_publisher")
	peer := startBroadcastPeer(t, interpreter, "publisher", name, strconv.Itoa(broadcastCount), "2")
	lossy := make(chan error, 1)
	go func() { lossy <- subscribe(name, fipc.Lossy, broadcastCount) }()
	must(t, subscribe(name, fipc.Gated, broadcastCount))
	must(t, <-lossy)
	if err := peer.Wait(); err != nil {
		t.Fatalf("the Python publisher failed: %v", err)
	}
}

// A Python owner and producer and two Go consumers: each job goes to one of them.
func TestPythonQueueProducerGoConsumers(t *testing.T) {
	guard(t, 120*time.Second)
	interpreter := python(t)
	name := uniqueName("py_queue_producer")
	peer := startPythonTest(t, interpreter, "queue_peer.py", "producer", name, strconv.Itoa(queueCount), "2")
	type jobsOf struct {
		jobs []string
		err  error
	}
	results := make(chan jobsOf, 2)
	for range 2 {
		go func() {
			jobs, err := consumeJobs(name)
			results <- jobsOf{jobs, err}
		}()
	}
	var received [][]string
	for range 2 {
		r := <-results
		must(t, r.err)
		received = append(received, r.jobs)
	}
	must(t, checkJobs(received, 1, queueCount))
	if err := peer.Wait(); err != nil {
		t.Fatalf("the Python producer failed: %v", err)
	}
}

// A Go owner and producer and a Python consumer.
func TestGoQueueProducerPythonConsumer(t *testing.T) {
	guard(t, 120*time.Second)
	interpreter := python(t)
	name := uniqueName("py_queue_consumer")
	peer := startPythonTest(t, interpreter, "queue_peer.py", "consumer", name, strconv.Itoa(queueCount))
	must(t, produceJobs(name, queueCount, 1))
	if err := peer.Wait(); err != nil {
		t.Fatalf("the Python consumer failed: %v", err)
	}
}

// A Go owner and consumer and two Python producers that join.
func TestPythonQueueProducersJoinAGoOwner(t *testing.T) {
	guard(t, 120*time.Second)
	interpreter := python(t)
	name := uniqueName("py_queue_joined")
	var peers []*exec.Cmd
	for range 2 {
		peers = append(peers, startPythonTest(t, interpreter, "queue_peer.py", "joined-producer", name,
			strconv.Itoa(queueCount)))
	}
	jobs, err := ownAndConsume(name, queueCount, 2)
	must(t, err)
	must(t, checkJobs([][]string{jobs}, 2, queueCount))
	for _, peer := range peers {
		if err := peer.Wait(); err != nil {
			t.Fatalf("a Python producer failed: %v", err)
		}
	}
}

// A Python owner and consumer and two Go producers that join.
func TestGoQueueProducersJoinAPythonOwner(t *testing.T) {
	guard(t, 120*time.Second)
	interpreter := python(t)
	name := uniqueName("py_queue_owner")
	peer := startPythonTest(t, interpreter, "queue_peer.py", "owner-consumer", name, strconv.Itoa(queueCount), "2")
	sent := make(chan error, 2)
	for range 2 {
		go func() { sent <- joinAndProduce(name, queueCount) }()
	}
	must(t, <-sent)
	must(t, <-sent)
	if err := peer.Wait(); err != nil {
		t.Fatalf("the Python owner-consumer failed: %v", err)
	}
}

// startServicePeer runs the Python binding's service_peer.py with args.
func startServicePeer(t *testing.T, interpreter string, args ...string) *exec.Cmd {
	t.Helper()
	return startPythonTest(t, interpreter, "service_peer.py", args...)
}

// waitForPython is the Python peer's exit, which must succeed.
func waitForPython(t *testing.T, peer *exec.Cmd, what string) {
	t.Helper()
	if err := peer.Wait(); err != nil {
		t.Fatalf("the Python %s failed: %v", what, err)
	}
}

// A Python owner and server and a Go client.
func TestPythonServiceServerGoClient(t *testing.T) {
	guard(t, 120*time.Second)
	interpreter := python(t)
	name := uniqueName("py_s_server")
	peer := startServicePeer(t, interpreter, "server", name, strconv.Itoa(serviceCount))
	must(t, joinAndCall(name, serviceCount))
	waitForPython(t, peer, "server")
}

// A Go owner and server and a Python client.
func TestGoServiceServerPythonClient(t *testing.T) {
	guard(t, 120*time.Second)
	interpreter := python(t)
	name := uniqueName("py_s_client")
	peer := startServicePeer(t, interpreter, "client", name, strconv.Itoa(serviceCount))
	must(t, serve(name, serviceCount, 1))
	waitForPython(t, peer, "client")
}

// A Go owner and server and two Python clients.
func TestTwoPythonServiceClientsAndAGoServer(t *testing.T) {
	guard(t, 120*time.Second)
	interpreter := python(t)
	name := uniqueName("py_s_clients")
	var peers []*exec.Cmd
	for range 2 {
		peers = append(peers, startServicePeer(t, interpreter, "client", name, strconv.Itoa(serviceCount)))
	}
	must(t, serve(name, serviceCount, 2))
	for _, peer := range peers {
		waitForPython(t, peer, "client")
	}
}

// A Go owner and client and two Python servers that join.
func TestTwoPythonServersJoinAGoOwnersClient(t *testing.T) {
	guard(t, 120*time.Second)
	interpreter := python(t)
	name := uniqueName("py_s_joined")
	var peers []*exec.Cmd
	for range 2 {
		peers = append(peers, startServicePeer(t, interpreter, "joined-server", name))
	}
	must(t, ownAndCall(name, serviceCount, 2))
	for _, peer := range peers {
		waitForPython(t, peer, "server")
	}
}

// A Python owner and client and two Go servers that join, on goroutines.
func TestGoServersJoinAPythonOwnersClient(t *testing.T) {
	guard(t, 120*time.Second)
	interpreter := python(t)
	name := uniqueName("py_s_owner")
	peer := startServicePeer(t, interpreter, "owner-client", name, strconv.Itoa(serviceCount), "2")
	answered := make(chan error, 2)
	for range 2 {
		go func() {
			_, err := joinAndAnswer(name)
			answered <- err
		}()
	}
	must(t, <-answered)
	must(t, <-answered)
	waitForPython(t, peer, "owner-client")
}

// A Python server killed while it holds a Go client's request: the client's receive is ErrAbandoned, and a Call to
// another server goes on.
func TestAPythonServerKilledHoldingARequestAbandonsIt(t *testing.T) {
	guard(t, 120*time.Second)
	interpreter := python(t)
	name := uniqueName("py_s_killed")
	service, err := fipc.CreateService(name, 4096, 64, 1, 2)
	must(t, err)
	defer service.Close()
	client, err := fipc.JoinService(name, fipc.Client, tenSeconds)
	must(t, err)
	defer client.Close()
	script := filepath.Join(repository(t), "bindings", "python", "tests", "service_peer.py")
	peer := exec.Command(interpreter, script, "holding-server", name)
	peer.Stderr = os.Stderr
	out, err := peer.StdoutPipe()
	must(t, err)
	must(t, peer.Start())
	defer func() {
		peer.Process.Kill()
		peer.Wait()
	}()
	must(t, client.Send([]byte("doomed"), tenSeconds))
	line, err := bufio.NewReader(out).ReadString('\n')
	if err != nil || line != "holding\n" && line != "holding\r\n" { // Python's print ends a line with \r\n on Windows
		t.Fatalf("the Python peer said %q, %v", line, err)
	}
	must(t, peer.Process.Kill())
	peer.Wait()
	_, err = client.Receive(peerStart) // once the owner's process has seen its end
	is(t, err, fipc.ErrAbandoned, "the reply to the request the killed server held")
	answering := answerAside(t, name, 1)
	reply, err := client.Call([]byte("next"), tenSeconds)
	if err != nil || string(reply) != "NEXT" {
		t.Fatalf("the next call: %q, %v", reply, err)
	}
	must(t, <-answering)
}
