package fipc_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/fastipc/fastipc/bindings/go/fipc"
)

// A server and a client in one process, on two goroutines (in two processes, the same calls).
func Example() {
	listener, err := fipc.CreateStream("fipc_go_example", 1<<16) // rings of 64 KiB each way
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()

	// The client, on another goroutine: ConnectStream waits for the listener's Accept
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := fipc.ConnectStream("fipc_go_example", 5*time.Second)
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
	listener, err := fipc.CreateStream(name, 1<<16)
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

// A broadcast in one process, on one goroutine: the publisher's process admits the subscriber by itself, so the
// publisher waits for its one subscriber, sends, and closes, which ends the broadcast (in other processes, the same
// calls).
func ExampleCreateBroadcast() {
	publisher, err := fipc.CreateBroadcast("fipc_go_example_broadcast", 1<<16, 8, 5*time.Second)
	if err != nil {
		log.Fatal(err)
	}
	subscriber, err := fipc.JoinBroadcast("fipc_go_example_broadcast", fipc.Gated, 5*time.Second)
	if err != nil {
		log.Fatal(err)
	}
	defer subscriber.Close()
	n, err := publisher.WaitForMembers(1, 5*time.Second)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(n, "subscriber")
	for _, tick := range []string{"tick 1", "tick 2"} {
		if err := publisher.Send([]byte(tick), 5*time.Second); err != nil {
			log.Fatal(err)
		}
	}
	publisher.Close() // ends the broadcast

	for {
		msg, err := subscriber.Receive(5 * time.Second)
		if errors.Is(err, fipc.ErrDisconnected) {
			break // the broadcast ended, after its messages
		} else if err != nil {
			log.Fatal(err)
		}
		fmt.Println(string(msg))
	}
	// Output:
	// 1 subscriber
	// tick 1
	// tick 2
}

// A work queue in one process, on one goroutine: the owner's producer sends jobs, which wait in the queue; a consumer
// joins (the owner's process admits it by itself), the owner waits for it, then closes its producer and the queue,
// which ends it; the consumer receives the jobs first, in no promised order (in other processes, the same calls).
func ExampleCreateQueue() {
	queue, err := fipc.CreateQueue("fipc_go_example_queue", 4096, 1, 1)
	if err != nil {
		log.Fatal(err)
	}
	producer, err := fipc.JoinQueue("fipc_go_example_queue", fipc.Producer, 5*time.Second)
	if err != nil {
		log.Fatal(err)
	}
	for i := 1; i <= 3; i++ {
		if err := producer.Send(fmt.Appendf(nil, "job %d", i), 5*time.Second); err != nil { // waits in the queue
			log.Fatal(err)
		}
	}

	consumer, err := fipc.JoinQueue("fipc_go_example_queue", fipc.Consumer, 5*time.Second)
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()
	if _, err := queue.WaitForMembers(fipc.Consumer, 1, 5*time.Second); err != nil {
		log.Fatal(err)
	}
	producer.Close()
	queue.Close() // ends the queue

	for {
		job, err := consumer.Receive(5 * time.Second)
		if errors.Is(err, fipc.ErrDisconnected) {
			break // the queue ended, after its jobs
		} else if err != nil {
			log.Fatal(err)
		}
		fmt.Println(string(job))
	}
	// Unordered output:
	// job 1
	// job 2
	// job 3
}

// A service in one process: the owner joins as its server and as a client (its process admits both by itself); the
// client's Call sends a request on one goroutine while the server answers on another; then the owner closes the
// service, which ends it (in other processes, the same calls).
func ExampleCreateService() {
	service, err := fipc.CreateService("fipc_go_example_service", 4096, 256, 1, 1)
	if err != nil {
		log.Fatal(err)
	}
	server, err := fipc.JoinService("fipc_go_example_service", fipc.Server, 5*time.Second)
	if err != nil {
		log.Fatal(err)
	}
	defer server.Close()
	client, err := fipc.JoinService("fipc_go_example_service", fipc.Client, 5*time.Second)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	done := make(chan struct{})
	go func() { // the server: answers each request with its uppercase until the service ends
		defer close(done)
		for {
			request, err := server.Receive(5 * time.Second)
			if errors.Is(err, fipc.ErrDisconnected) {
				return
			} else if err != nil {
				log.Fatal(err)
			}
			if err := server.Send(bytes.ToUpper(request), fipc.NoWait); err != nil { // a reply never waits
				log.Fatal(err)
			}
		}
	}()
	for i := 1; i <= 2; i++ {
		reply, err := client.Call(fmt.Appendf(nil, "ping %d", i), 5*time.Second)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(string(reply))
	}
	service.Close() // ends the service: the server's receive returns ErrDisconnected
	<-done
	// Output:
	// PING 1
	// PING 2
}

// A client's requests and replies in two steps: several requests on their way at once, and their replies in the order
// of the requests, whichever server replies first.
func ExampleConn_Receive_replies() {
	service, err := fipc.CreateService("fipc_go_example_service_order", 4096, 256, 1, 2)
	if err != nil {
		log.Fatal(err)
	}
	defer service.Close()
	client, err := fipc.JoinService("fipc_go_example_service_order", fipc.Client, 5*time.Second)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	var servers [2]*fipc.Conn
	for k := range servers {
		if servers[k], err = fipc.JoinService("fipc_go_example_service_order", fipc.Server, 5*time.Second); err != nil {
			log.Fatal(err)
		}
		defer servers[k].Close()
	}

	for _, request := range []string{"first", "second"} {
		if err := client.Send([]byte(request), 5*time.Second); err != nil {
			log.Fatal(err)
		}
	}
	var requests [2][]byte
	for k, server := range servers { // each server takes one request and holds it until its reply
		if requests[k], err = server.Receive(5 * time.Second); err != nil {
			log.Fatal(err)
		}
	}
	for _, k := range []int{1, 0} { // the second request's server replies first
		if err := servers[k].Send(bytes.ToUpper(requests[k]), fipc.NoWait); err != nil {
			log.Fatal(err)
		}
	}
	for range 2 {
		reply, err := client.Receive(5 * time.Second)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(string(reply))
	}
	// Output:
	// FIRST
	// SECOND
}
