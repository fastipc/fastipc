package fipc_test

// Peers in other processes: this test binary run again as a peer (TestMain runs runPeer when the environment names a
// part for it), echoing, exiting, killed, serving, publishing on a broadcast or subscribing to one, producing on
// a work queue or consuming from one, serving a service or calling it, or killed holding a request.

import (
	"bufio"
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
		conn, err := fipc.ConnectStream(name, peerStart)
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
		conn, err := fipc.ConnectStream(name, peerStart)
		if err != nil {
			return err
		}
		if err := conn.Send([]byte("bye"), fipc.Forever); err != nil {
			return err
		}
		os.Exit(0)
	case "hang": // says it is ready, then waits to be killed
		conn, err := fipc.ConnectStream(name, peerStart)
		if err != nil {
			return err
		}
		if err := conn.Send([]byte("ready"), fipc.Forever); err != nil {
			return err
		}
		time.Sleep(10 * time.Minute)
		return conn.Close()
	case "server": // answers each message with it reversed, until the end
		listener, err := fipc.CreateStream(name, 1<<16)
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
			msg, err := conn.Receive(fipc.Forever)
			if errors.Is(err, fipc.ErrDisconnected) {
				return nil
			} else if err != nil {
				return err
			}
			slices.Reverse(msg)
			if err := conn.Send(msg, fipc.Forever); err != nil {
				return err
			}
		}
	case "publisher": // publishes broadcastCount messages to one subscriber
		return publish(name, broadcastCount, 1)
	case "gated", "lossy": // subscribes, and checks what it received
		role := fipc.Gated
		if mode == "lossy" {
			role = fipc.Lossy
		}
		return subscribe(name, role, broadcastCount)
	case "queue_producer": // owns a queue and is its producer, sends the jobs, waits for one consumer and ends it
		return produceJobs(name, queueCount, 1)
	case "queue_consumer": // joins a queue and checks the jobs it received
		jobs, err := consumeJobs(name)
		if err != nil {
			return err
		}
		return checkJobs([][]string{jobs}, 1, queueCount)
	case "service_server": // owns a service and is its server, answers serviceCount requests and ends it
		return serve(name, serviceCount, 1)
	case "service_client": // joins a service and calls serviceCount times, checking each reply
		return joinAndCall(name, serviceCount)
	case "service_holding": // joins a service as a server, takes one request, says so and waits to be killed
		server, err := fipc.JoinService(name, fipc.Server, peerStart)
		if err != nil {
			return err
		}
		if _, err := server.Receive(peerStart); err != nil {
			return err
		}
		fmt.Println("holding")
		time.Sleep(10 * time.Minute)
		return server.Close()
	}
	return fmt.Errorf("unknown peer mode %q", mode)
}

// The work queue's peers, as the Python binding's tests/queue_peer.py: job i is "job <i>", from job 1; the owner's
// queue has rings of 4096 bytes (64 messages of these lengths) and room for 8 producers and 8 consumers.
const (
	queueCount    = 100
	queueCapacity = 4096
	queueMembers  = 8
)

func queueJob(i int) []byte { return fmt.Appendf(nil, "job %d", i) }

// sendJobs sends job 1 to job count on producer.
func sendJobs(producer *fipc.Conn, count int) error {
	for i := 1; i <= count; i++ {
		if err := producer.Send(queueJob(i), peerStart); err != nil {
			return fmt.Errorf("job %d: %w", i, err)
		}
	}
	return nil
}

// produceJobs creates the queue on name and joins it as a producer, as the examples' producer does, sends job 1 to job
// count (more than its ring holds wait for a consumer to take some), waits for consumers consumers to have joined, and
// closes the producer and the queue, which ends it.
func produceJobs(name string, count, consumers int) error {
	queue, err := fipc.CreateQueue(name, queueCapacity, queueMembers, queueMembers)
	if err != nil {
		return err
	}
	defer queue.Close()
	producer, err := fipc.JoinQueue(name, fipc.Producer, peerStart)
	if err != nil {
		return err
	}
	defer producer.Close()
	if err := sendJobs(producer, count); err != nil {
		return err
	}
	if _, err := queue.WaitForMembers(fipc.Consumer, consumers, peerStart); err != nil {
		return fmt.Errorf("waiting for %d consumers: %w", consumers, err)
	}
	return nil
}

// joinAndProduce joins name as a producer, sends job 1 to job count, and closes: the queue goes on.
func joinAndProduce(name string, count int) error {
	producer, err := fipc.JoinQueue(name, fipc.Producer, peerStart)
	if err != nil {
		return err
	}
	defer producer.Close()
	return sendJobs(producer, count)
}

// receiveJobs receives on consumer until the queue ends, and returns the jobs.
func receiveJobs(consumer *fipc.Conn) ([]string, error) {
	var jobs []string
	for {
		job, err := consumer.Receive(peerStart)
		if errors.Is(err, fipc.ErrDisconnected) {
			return jobs, nil
		} else if err != nil {
			return jobs, err
		}
		jobs = append(jobs, string(job))
	}
}

// consumeJobs joins name as a consumer and receives until the queue ends.
func consumeJobs(name string) ([]string, error) {
	consumer, err := fipc.JoinQueue(name, fipc.Consumer, peerStart)
	if err != nil {
		return nil, err
	}
	defer consumer.Close()
	return receiveJobs(consumer)
}

// ownAndConsume creates the queue on name and joins it as a consumer, receives count jobs from each of producers
// producers, and closes the consumer and the queue, which ends it.
func ownAndConsume(name string, count, producers int) ([]string, error) {
	queue, err := fipc.CreateQueue(name, queueCapacity, queueMembers, queueMembers)
	if err != nil {
		return nil, err
	}
	defer queue.Close()
	consumer, err := fipc.JoinQueue(name, fipc.Consumer, peerStart)
	if err != nil {
		return nil, err
	}
	defer consumer.Close()
	jobs := make([]string, 0, producers*count)
	for range producers * count {
		job, err := consumer.Receive(peerStart)
		if err != nil {
			return jobs, fmt.Errorf("after %d jobs: %w", len(jobs), err)
		}
		jobs = append(jobs, string(job))
	}
	return jobs, nil
}

// checkJobs checks the jobs consumers received from producers that each sent job 1 to job count: every job of every
// producer once, in any order (the queue promises none). The jobs don't name their producer, so each job is to come
// producers times in all.
func checkJobs(received [][]string, producers, count int) error {
	times := make(map[string]int)
	total := 0
	for _, jobs := range received {
		for _, job := range jobs {
			times[job]++
			total++
		}
	}
	for i := 1; i <= count; i++ {
		if n := times[string(queueJob(i))]; n != producers {
			return fmt.Errorf("received job %d %d times, expected %d (%d jobs in all)", i, n, producers, total)
		}
	}
	if total != producers*count {
		return fmt.Errorf("received %d jobs, expected %d", total, producers*count)
	}
	return nil
}

// The service's peers, as the Python binding's tests/service_peer.py: a client's request i is "ping <i>", from
// request 1, and its reply "PING <i>"; the owner's service has rings of 4096 bytes, requests and replies of up to 256
// bytes, and room for 8 clients and 8 servers.
const (
	serviceCount      = 300
	serviceCapacity   = 4096
	serviceMaxMessage = 256
	serviceMembers    = 8
)

func serviceRequest(i int) []byte { return fmt.Appendf(nil, "ping %d", i) }

// answerUntilTheEnd answers each request on server with its uppercase until the service ends, and returns how many
// it answered.
func answerUntilTheEnd(server *fipc.Conn) (int, error) {
	answered := 0
	for {
		request, err := server.Receive(peerStart)
		if errors.Is(err, fipc.ErrDisconnected) {
			return answered, nil
		} else if err != nil {
			return answered, err
		}
		if err := server.Send(bytes.ToUpper(request), peerStart); errors.Is(err, fipc.ErrDisconnected) {
			return answered, nil // the service ended between the request and its reply
		} else if err != nil {
			return answered, err
		}
		answered++
	}
}

// callRequests calls request 1 to request count on client, one at a time, and checks each reply.
func callRequests(client *fipc.Conn, count int) error {
	for i := 1; i <= count; i++ {
		reply, err := client.Call(serviceRequest(i), peerStart)
		if err != nil {
			return fmt.Errorf("request %d: %w", i, err)
		}
		if want := bytes.ToUpper(serviceRequest(i)); !bytes.Equal(reply, want) {
			return fmt.Errorf("the reply to %q is %q", serviceRequest(i), reply)
		}
	}
	return nil
}

// serve creates the service on name and joins it as a server, as the examples' server does, answers count requests
// from each of clients clients, and closes its server and the service, which ends it.
func serve(name string, count, clients int) error {
	service, err := fipc.CreateService(name, serviceCapacity, serviceMaxMessage, serviceMembers, serviceMembers)
	if err != nil {
		return err
	}
	defer service.Close()
	server, err := fipc.JoinService(name, fipc.Server, peerStart)
	if err != nil {
		return err
	}
	defer server.Close()
	for i := range count * clients {
		request, err := server.Receive(peerStart)
		if err != nil {
			return fmt.Errorf("after %d requests: %w", i, err)
		}
		if err := server.Send(bytes.ToUpper(request), peerStart); err != nil {
			return err
		}
	}
	return nil
}

// joinAndCall joins name as a client, calls count times and checks each reply, then closes.
func joinAndCall(name string, count int) error {
	client, err := fipc.JoinService(name, fipc.Client, peerStart)
	if err != nil {
		return err
	}
	defer client.Close()
	return callRequests(client, count)
}

// ownAndCall creates the service on name and joins it as a client, waits for servers servers to have joined, calls
// count times, checks each reply, and closes its client and the service, which ends it.
func ownAndCall(name string, count, servers int) error {
	service, err := fipc.CreateService(name, serviceCapacity, serviceMaxMessage, serviceMembers, serviceMembers)
	if err != nil {
		return err
	}
	defer service.Close()
	client, err := fipc.JoinService(name, fipc.Client, peerStart)
	if err != nil {
		return err
	}
	defer client.Close()
	if _, err := service.WaitForMembers(fipc.Server, servers, peerStart); err != nil {
		return fmt.Errorf("waiting for %d servers: %w", servers, err)
	}
	return callRequests(client, count)
}

// joinAndAnswer joins name as a server and answers requests until the service ends.
func joinAndAnswer(name string) (int, error) {
	server, err := fipc.JoinService(name, fipc.Server, peerStart)
	if err != nil {
		return 0, err
	}
	defer server.Close()
	return answerUntilTheEnd(server)
}

// The broadcast's peers, as the Python binding's tests/broadcast_peer.py: message i is "message <i>".
const broadcastCount = 100

func broadcastMessage(i int) []byte { return fmt.Appendf(nil, "message %d", i) }

// publish claims name for a broadcast of 4096 bytes and up to 8 subscribers, waits for subscribers of them to join,
// sends count messages and closes the publisher, which ends the broadcast.
func publish(name string, count, subscribers int) error {
	publisher, err := fipc.CreateBroadcast(name, 4096, 8, peerStart)
	if err != nil {
		return err
	}
	defer publisher.Close()
	if _, err := publisher.WaitForMembers(subscribers, peerStart); err != nil {
		return fmt.Errorf("waiting for %d subscribers: %w", subscribers, err)
	}
	for i := range count {
		if err := publisher.Send(broadcastMessage(i), peerStart); err != nil {
			return err
		}
	}
	return nil
}

// subscribe joins name as role and receives until the broadcast ends. A gated subscriber checks that it received every
// message in order; a lossy one that each message it received is the one after the last, or that many after it as an
// ErrOverrun between them reported lost, and that received and lost add up to count.
func subscribe(name string, role fipc.SubscriberRole, count int) error {
	subscriber, err := fipc.JoinBroadcast(name, role, peerStart)
	if err != nil {
		return err
	}
	defer subscriber.Close()
	next := 0 // the index of the next message
	for {
		msg, err := subscriber.Receive(peerStart)
		var e *fipc.Error
		switch {
		case err == nil:
			if !bytes.Equal(msg, broadcastMessage(next)) {
				return fmt.Errorf("%v: received %q, expected %q", role, msg, broadcastMessage(next))
			}
			next++
		case role == fipc.Lossy && errors.As(err, &e) && errors.Is(err, fipc.ErrOverrun):
			if e.Lost() < 1 {
				return fmt.Errorf("%v: %v", role, err)
			}
			next += int(e.Lost())
		case errors.Is(err, fipc.ErrDisconnected):
			if next != count {
				return fmt.Errorf("%v: the broadcast ended at message %d of %d", role, next, count)
			}
			return nil
		default:
			return fmt.Errorf("%v: %w", role, err)
		}
	}
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
	listener, err := fipc.CreateStream(name, 1<<16)
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
	listener, err := fipc.CreateStream(name, 1<<16)
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
	listener, err := fipc.CreateStream(name, 1<<16)
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

func TestSubscribersInOtherProcesses(t *testing.T) {
	guard(t, 60*time.Second)
	name := uniqueName("bc_subscribers")
	gated := startPeer(t, "gated", name)
	lossy := startPeer(t, "lossy", name)
	must(t, publish(name, broadcastCount, 2))
	must(t, waitFor(t, gated))
	must(t, waitFor(t, lossy))
}

func TestAPublisherInAnotherProcess(t *testing.T) {
	guard(t, 60*time.Second)
	name := uniqueName("bc_publisher")
	peer := startPeer(t, "publisher", name)
	must(t, subscribe(name, fipc.Gated, broadcastCount))
	must(t, waitFor(t, peer))
}

func TestAQueueProducerInAnotherProcess(t *testing.T) {
	guard(t, 60*time.Second)
	name := uniqueName("q_producer")
	peer := startPeer(t, "queue_producer", name)
	jobs, err := consumeJobs(name)
	must(t, err)
	must(t, checkJobs([][]string{jobs}, 1, queueCount))
	must(t, waitFor(t, peer))
}

func TestAQueueConsumerInAnotherProcess(t *testing.T) {
	guard(t, 60*time.Second)
	name := uniqueName("q_consumer")
	peer := startPeer(t, "queue_consumer", name)
	must(t, produceJobs(name, queueCount, 1))
	must(t, waitFor(t, peer))
}

func TestAServerPeer(t *testing.T) {
	guard(t, 60*time.Second)
	name := uniqueName("server")
	peer := startPeer(t, "server", name)
	conn, err := fipc.ConnectStream(name, peerStart)
	must(t, err)
	for i := range 10 {
		must(t, conn.Send(pattern(i*1000+1), tenSeconds))
	}
	for i := range 10 {
		reply, err := conn.Receive(tenSeconds)
		must(t, err)
		want := pattern(i*1000 + 1)
		slices.Reverse(want)
		if !bytes.Equal(reply, want) {
			t.Fatalf("reply %d: %d bytes", i, len(reply))
		}
	}
	must(t, conn.Close())
	must(t, waitFor(t, peer))
}

func TestAServiceServerInAnotherProcess(t *testing.T) {
	guard(t, 60*time.Second)
	name := uniqueName("s_server")
	peer := startPeer(t, "service_server", name)
	must(t, joinAndCall(name, serviceCount))
	must(t, waitFor(t, peer))
}

func TestServiceClientsInOtherProcesses(t *testing.T) {
	guard(t, 60*time.Second)
	name := uniqueName("s_clients")
	peers := []*exec.Cmd{startPeer(t, "service_client", name), startPeer(t, "service_client", name)}
	must(t, serve(name, serviceCount, len(peers)))
	for _, peer := range peers {
		must(t, waitFor(t, peer))
	}
}

// A server process killed while it holds a request: its client's receive is ErrAbandoned once the owner's process
// has seen its end, and the client goes on with another server.
func TestAServerProcessKilledHoldingARequestAbandonsIt(t *testing.T) {
	guard(t, 60*time.Second)
	name := uniqueName("s_killed")
	service, err := fipc.CreateService(name, 4096, 64, 1, 2)
	must(t, err)
	defer service.Close()
	client, err := fipc.JoinService(name, fipc.Client, tenSeconds)
	must(t, err)
	defer client.Close()
	exe, err := os.Executable()
	must(t, err)
	peer := exec.Command(exe)
	peer.Env = append(os.Environ(), peerEnv+"=service_holding:"+name)
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
	if err != nil || line != "holding\n" {
		t.Fatalf("the peer said %q, %v", line, err)
	}
	must(t, peer.Process.Kill())
	peer.Wait()
	_, err = client.Receive(tenSeconds)
	is(t, err, fipc.ErrAbandoned, "the reply to the request the killed server held")
	answering := answerAside(t, name, 1)
	reply, err := client.Call([]byte("next"), tenSeconds)
	if err != nil || string(reply) != "NEXT" {
		t.Fatalf("the next call: %q, %v", reply, err)
	}
	must(t, <-answering)
}
