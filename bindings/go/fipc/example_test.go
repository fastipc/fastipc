package fipc_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/fastipc/fastipc/bindings/go/fipc"
)

// A server and a client in one process, on two goroutines (in two processes, the same calls).
func Example() {
	listener, err := fipc.Listen("fipc_go_example", 1<<16) // rings of 64 KiB each way
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()

	// The client, on another goroutine: Connect waits for the listener's Accept
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := fipc.Connect("fipc_go_example", 5*time.Second)
		if err != nil {
			log.Fatal(err)
		}
		defer conn.Close()
		if err := conn.Send([]byte("hello"), fipc.Forever); err != nil {
			log.Fatal(err)
		}
		reply, err := conn.Receive(fipc.Forever)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("the client got", string(reply))
	}()

	conn, err := listener.Accept(5 * time.Second)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	msg, err := conn.Receive(fipc.Forever)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("the server got", string(msg))
	if err := conn.Send([]byte("HELLO"), fipc.Forever); err != nil {
		log.Fatal(err)
	}
	<-done
	// Output:
	// the server got hello
	// the client got HELLO
}

// connectPair is a server and client connection on name, as in the package example; closing them is the caller's.
func connectPair(name string) (listener *fipc.Listener, server, client *fipc.Conn) {
	listener, err := fipc.Listen(name, 1<<16)
	if err != nil {
		log.Fatal(err)
	}
	connecting := connectAside(name)
	if server, err = listener.Accept(5 * time.Second); err != nil {
		log.Fatal(err)
	}
	c := <-connecting
	if c.err != nil {
		log.Fatal(c.err)
	}
	return listener, server, c.conn
}

// Zero-copy: the message is written into the ring and read where it lies.
func ExampleConn_SendAcquire() {
	listener, server, conn := connectPair("fipc_go_example_zero_copy")
	defer listener.Close()
	defer server.Close()
	defer conn.Close()

	room, err := conn.SendAcquire(64, fipc.Forever) // room for up to 64 bytes, in the ring
	if err != nil {
		log.Fatal(err)
	}
	n := copy(room, "hello")
	if err := conn.SendCommit(n); err != nil { // sends the first 5 bytes as one message
		log.Fatal(err)
	}

	msg, err := server.ReceiveAcquire(fipc.Forever) // the message in the ring, read-only
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(msg)) // a copy: msg is valid until ReceiveRelease
	server.ReceiveRelease()  // frees its room
	// Output: hello
}

// RPC: a request and its response, correlated by id.
func ExampleConn_RPCSubmit() {
	listener, server, conn := connectPair("fipc_go_example_rpc")
	defer listener.Close()
	defer server.Close()
	defer conn.Close()

	id, err := conn.RPCSubmit(1, []byte("ping"), fipc.Forever) // opcode 1
	if err != nil {
		log.Fatal(err)
	}

	request, err := server.RPCReceive(fipc.Forever)
	if err != nil {
		log.Fatal(err)
	}
	err = server.RPCRespond(request.ID, request.Opcode, 0, []byte("PING"), fipc.Forever)
	if err != nil {
		log.Fatal(err)
	}

	reply, err := conn.RPCReceive(fipc.Forever)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(reply.Kind, reply.ID == id, reply.Status, string(reply.Payload))
	// Output: response true 0 PING
}

// Polling, once a frame: whatever has arrived, without waiting.
func ExampleConn_Receive_polling() {
	listener, server, conn := connectPair("fipc_go_example_poll")
	defer listener.Close()
	defer server.Close()
	defer conn.Close()
	for _, word := range []string{"one", "two"} {
		if err := server.Send([]byte(word), fipc.Forever); err != nil {
			log.Fatal(err)
		}
	}

	for {
		msg, err := conn.Receive(fipc.NoWait)
		if errors.Is(err, fipc.ErrTimeout) {
			break // nothing more for now
		} else if err != nil {
			log.Fatal(err) // ErrDisconnected: the peer is gone
		}
		fmt.Println(string(msg))
	}
	// Output:
	// one
	// two
}

// A receive into a buffer of yours: a message too long for it stays queued, and the error says its length.
func ExampleConn_ReceiveInto() {
	listener, server, conn := connectPair("fipc_go_example_receive_into")
	defer listener.Close()
	defer server.Close()
	defer conn.Close()
	if err := server.Send(make([]byte, 300), fipc.Forever); err != nil {
		log.Fatal(err)
	}

	buf := make([]byte, 256)
	n, err := conn.ReceiveInto(buf, fipc.Forever)
	var e *fipc.Error
	if errors.As(err, &e) && e.Len() > 0 { // ErrTooLarge: make room, then again
		buf = make([]byte, e.Len())
		n, err = conn.ReceiveInto(buf, fipc.Forever)
	}
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(n, "bytes")
	// Output: 300 bytes
}

// Stopping a goroutine that waits: Cancel, from any goroutine; with a context, context.AfterFunc calls it.
func ExampleConn_Cancel() {
	listener, server, conn := connectPair("fipc_go_example_cancel")
	defer listener.Close()
	defer server.Close()
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	stop := context.AfterFunc(ctx, conn.Cancel) // cancels the connection when ctx is done
	defer stop()

	_, err := conn.Receive(fipc.Forever) // nothing is sent: waits until the cancel
	fmt.Println(errors.Is(err, fipc.ErrCancelled))
	// Output: true
}
