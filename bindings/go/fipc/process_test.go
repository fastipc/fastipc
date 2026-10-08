package fipc_test

// Peers in other processes: this test binary run again as a peer (TestMain runs runPeer when the environment names a
// part for it), echoing, exiting, killed, serving RPC.

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"slices"
	"testing"
	"time"

	"github.com/fastipc/fastipc/bindings/go/fipc"
)

// runPeer plays the part mode on name, in a process of its own.
func runPeer(mode, name string) error {
	switch mode {
	case "echo": // echoes every message until the server's end
		conn, err := fipc.Connect(name, peerStart)
		if err != nil {
			return err
		}
		defer conn.Close()
		for {
			msg, err := conn.Receive(fipc.Forever)
			if errors.Is(err, fipc.ErrDisconnected) {
				return nil
			} else if err != nil {
				return err
			}
			if err := conn.Send(msg, fipc.Forever); err != nil {
				return err
			}
		}
	case "exit": // sends one message, then the process ends without closing the connection
		conn, err := fipc.Connect(name, peerStart)
		if err != nil {
			return err
		}
		if err := conn.Send([]byte("bye"), fipc.Forever); err != nil {
			return err
		}
		os.Exit(0)
	case "hang": // says it is ready, then waits to be killed
		conn, err := fipc.Connect(name, peerStart)
		if err != nil {
			return err
		}
		if err := conn.Send([]byte("ready"), fipc.Forever); err != nil {
			return err
		}
		time.Sleep(10 * time.Minute)
		return conn.Close()
	case "rpc_server": // answers each request with its payload reversed and status = its length, until the end
		listener, err := fipc.Listen(name, 1<<16)
		if err != nil {
			return err
		}
		defer listener.Close()
		conn, err := listener.Accept(peerStart)
		if err != nil {
			return err
		}
		defer conn.Close()
		for {
			request, err := conn.RPCReceive(fipc.Forever)
			if errors.Is(err, fipc.ErrDisconnected) {
				return nil
			} else if err != nil {
				return err
			}
			reply := slices.Clone(request.Payload)
			slices.Reverse(reply)
			if err := conn.RPCRespond(request.ID, request.Opcode, int32(len(reply)), reply, fipc.Forever); err != nil {
				return err
			}
		}
	}
	return fmt.Errorf("unknown peer mode %q", mode)
}

// startPeer runs this test binary as a peer of mode on name.
func startPeer(t *testing.T, mode, name string) *exec.Cmd {
	t.Helper()
	exe, err := os.Executable()
	must(t, err)
	cmd := exec.Command(exe)
	cmd.Env = append(os.Environ(), peerEnv+"="+mode+":"+name)
	cmd.Stderr = os.Stderr
	must(t, cmd.Start())
	t.Cleanup(func() {
		if cmd.ProcessState == nil {
			cmd.Process.Kill()
			cmd.Wait()
		}
	})
	return cmd
}

// waitFor is the peer's exit, within 30 seconds.
func waitFor(t *testing.T, cmd *exec.Cmd) error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		return err
	case <-time.After(30 * time.Second):
		cmd.Process.Kill()
		t.Fatal("the peer didn't exit")
		return nil
	}
}

func TestAnEchoPeer(t *testing.T) {
	guard(t, 60*time.Second)
	name := uniqueName("echo")
	listener, err := fipc.Listen(name, 1<<16)
	must(t, err)
	defer listener.Close()
	peer := startPeer(t, "echo", name)
	conn, err := listener.Accept(peerStart)
	must(t, err)
	for _, size := range []int{1, 100, 65_472, 70_000, 1_000_000} {
		msg := pattern(size)
		// A message longer than the ring needs the echo to run at once: send from a goroutine
		sent := make(chan error, 1)
		go func() { sent <- conn.Send(msg, tenSeconds) }()
		echoed, err := conn.Receive(tenSeconds)
		must(t, err)
		must(t, <-sent)
		if !bytes.Equal(echoed, msg) {
			t.Fatalf("size %d: %d bytes back", size, len(echoed))
		}
	}
	must(t, conn.Close())
	must(t, waitFor(t, peer))
}

func TestAPeerThatExits(t *testing.T) {
	guard(t, 60*time.Second)
	name := uniqueName("exit")
	listener, err := fipc.Listen(name, 1<<16)
	must(t, err)
	defer listener.Close()
	peer := startPeer(t, "exit", name)
	conn, err := listener.Accept(peerStart)
	must(t, err)
	defer conn.Close()
	msg, err := conn.Receive(tenSeconds)
	must(t, err)
	if string(msg) != "bye" {
		t.Fatalf("got %q", msg)
	}
	_, err = conn.Receive(tenSeconds)
	is(t, err, fipc.ErrDisconnected, "Receive after the peer's exit")
	must(t, waitFor(t, peer))
}

func TestAKilledPeer(t *testing.T) {
	guard(t, 60*time.Second)
	name := uniqueName("killed")
	listener, err := fipc.Listen(name, 1<<16)
	must(t, err)
	defer listener.Close()
	peer := startPeer(t, "hang", name)
	conn, err := listener.Accept(peerStart)
	must(t, err)
	defer conn.Close()
	msg, err := conn.Receive(tenSeconds)
	must(t, err)
	if string(msg) != "ready" {
		t.Fatalf("got %q", msg)
	}
	must(t, peer.Process.Kill())
	_, err = conn.Receive(tenSeconds)
	is(t, err, fipc.ErrDisconnected, "Receive after the kill")
	is(t, conn.Send([]byte("anyone?"), fipc.Forever), fipc.ErrDisconnected, "Send after the kill")
	peer.Wait()
}

func TestAnRPCServerPeer(t *testing.T) {
	guard(t, 60*time.Second)
	name := uniqueName("rpc_server")
	peer := startPeer(t, "rpc_server", name)
	conn, err := fipc.Connect(name, peerStart)
	must(t, err)
	var ids []uint64
	for i := range 10 {
		id, err := conn.RPCSubmit(uint32(i), pattern(i*1000), tenSeconds)
		must(t, err)
		ids = append(ids, id)
	}
	if !slices.Equal(ids, []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}) {
		t.Fatalf("ids %v", ids)
	}
	for i, id := range ids {
		response, err := conn.RPCReceive(tenSeconds)
		must(t, err)
		want := pattern(i * 1000)
		slices.Reverse(want)
		if response.ID != id || response.Kind != fipc.RPCResponse || response.Opcode != uint32(i) ||
			response.Status != int32(len(want)) || !bytes.Equal(response.Payload, want) {
			t.Fatalf("response %d: %+v", i, response)
		}
	}
	must(t, conn.Close())
	must(t, waitFor(t, peer))
}
