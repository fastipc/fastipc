package fipc_test

// The service in one process: many clients and one server, a pool of server goroutines that hold requests at once,
// each client's replies in the order of its requests, ErrAbandoned when a server closes holding a request, what each
// role refuses and the zero-copy calls on both roles, a service's misuses, Call's owed replies (a call that gives up
// and the next one, a Send's reply owed, an ErrAbandoned owed), a Call whose time runs out before its send, a Call's
// checks of its request, Calls after the service's end, Calls after a Cancel (which send nothing), concurrent Calls on
// one client and their turns' timeouts and cancels, the count of members, its wait and its cancel, the end at the
// service's Close, a service the garbage collector never ends and a client it closes, requests longer than Receive's
// first buffer, and the examples' exchange in either order. Other processes: process_test.go (this binary as a server,
// a client, or a server killed holding a request), interop_test.go (the Python binding's service_peer.py) and
// examples_test.go.

import (
	"bytes"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/fastipc/fastipc/bindings/go/fipc"
)

// newService creates a service on a new name, closed when the test ends.
func newService(t *testing.T, prefix string, capacity, maxMessage, clients, servers int) (*fipc.Service, string) {
	t.Helper()
	name := uniqueName(prefix)
	service, err := fipc.CreateService(name, capacity, maxMessage, clients, servers)
	must(t, err)
	t.Cleanup(func() { service.Close() })
	return service, name
}

// joinService joins the service on name as role, closed when the test ends.
func joinService(t *testing.T, name string, role fipc.ServiceRole) *fipc.Conn {
	t.Helper()
	member, err := fipc.JoinService(name, role, tenSeconds)
	must(t, err)
	t.Cleanup(func() { member.Close() })
	return member
}

// answerAside joins name as a server on a goroutine of its own, answers n requests with their uppercase and closes.
func answerAside(t *testing.T, name string, n int) <-chan error {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		server, err := fipc.JoinService(name, fipc.Server, tenSeconds)
		if err != nil {
			done <- err
			return
		}
		defer server.Close()
		done <- answerN(server, n)
	}()
	return done
}

// answerN answers n requests on server with their uppercase.
func answerN(server *fipc.Conn, n int) error {
	for range n {
		request, err := server.Receive(tenSeconds)
		if err != nil {
			return err
		}
		if err := server.Send(bytes.ToUpper(request), tenSeconds); err != nil {
			return err
		}
	}
	return nil
}

// answerOnce answers one request on server with its uppercase, on a goroutine of its own.
func answerOnce(server *fipc.Conn) <-chan error {
	done := make(chan error, 1)
	go func() { done <- answerN(server, 1) }()
	return done
}

// expectCall checks that client's Call of request returns its uppercase.
func expectCall(t *testing.T, client *fipc.Conn, request string, timeout time.Duration) {
	t.Helper()
	reply, err := client.Call([]byte(request), timeout)
	if err != nil || !bytes.Equal(reply, bytes.ToUpper([]byte(request))) {
		t.Fatalf("Call(%q): %q, %v", request, reply, err)
	}
}

// expectReceive checks that conn receives want.
func expectReceive(t *testing.T, conn *fipc.Conn, want string) {
	t.Helper()
	msg, err := conn.Receive(tenSeconds)
	if err != nil || string(msg) != want {
		t.Fatalf("Receive: %q, %v; want %q", msg, err, want)
	}
}

func TestManyClientsAndOneServerEachGetTheirOwnRepliesInOrder(t *testing.T) {
	guard(t, 120*time.Second)
	const clients, count = 8, 300
	service, name := newService(t, "s_many", 4096, 64, clients, 1)
	members := make([]*fipc.Conn, clients)
	for k := range members {
		members[k] = joinService(t, name, fipc.Client)
	}
	server := joinService(t, name, fipc.Server)
	if n, err := service.WaitForMembers(fipc.AnyServiceRole, clients+1, tenSeconds); n != clients+1 || err != nil {
		t.Fatalf("WaitForMembers(AnyServiceRole, 9) = %d, %v", n, err)
	}
	for _, member := range append(members, server) {
		if member.MaxPiece() != 64 { // maxMessage
			t.Fatalf("MaxPiece %d", member.MaxPiece())
		}
	}
	type answered struct {
		n   int
		err error
	}
	answering := make(chan answered, 1)
	go func() {
		n, err := answerUntilTheEnd(server)
		answering <- answered{n, err}
	}()
	asked := make(chan error, clients)
	for k, client := range members {
		go func() {
			for i := range count {
				request := fmt.Sprintf("client %d request %d", k, i)
				reply, err := client.Call([]byte(request), tenSeconds)
				if err != nil {
					asked <- fmt.Errorf("%s: %w", request, err)
					return
				}
				if want := bytes.ToUpper([]byte(request)); !bytes.Equal(reply, want) {
					asked <- fmt.Errorf("the reply to %q is %q", request, reply)
					return
				}
			}
			asked <- nil
		}()
	}
	for range clients {
		must(t, <-asked)
	}
	must(t, service.Close()) // ends the service: the server's receive returns ErrDisconnected
	a := <-answering
	must(t, a.err)
	if a.n != clients*count {
		t.Fatalf("the server answered %d requests of %d", a.n, clients*count)
	}
}

// Four servers on goroutines and four clients that send at once: the servers each take a request and wait for the
// others before replying, so all four hold one at the same time; then they answer the rest, each request once, copied,
// into a buffer or in place, and each client receives its replies in the order of its requests (each sends ten before
// it receives).
func TestAPoolOfServerGoroutinesHoldsRequestsAtOnceAndAnswersEachOnce(t *testing.T) {
	guard(t, 120*time.Second)
	const servers, clients, count = 4, 4, 100
	service, name := newService(t, "s_pool", 1<<14, 64, clients, servers)
	members := make([]*fipc.Conn, servers)
	for k := range members {
		members[k] = joinService(t, name, fipc.Server)
	}
	if n, err := service.WaitForMembers(fipc.Server, servers, tenSeconds); n != servers || err != nil {
		t.Fatalf("WaitForMembers(Server, 4) = %d, %v", n, err)
	}
	var holding sync.WaitGroup
	holding.Add(servers)
	type answered struct {
		requests map[string]int
		err      error
	}
	results := make(chan answered, servers)
	for k, server := range members {
		go func() {
			requests := make(map[string]int)
			first, err := server.Receive(tenSeconds)
			holding.Done()
			if err != nil {
				results <- answered{requests, err}
				return
			}
			holding.Wait() // every server holds a request
			if err := server.Send(bytes.ToUpper(first), tenSeconds); err != nil {
				results <- answered{requests, err}
				return
			}
			requests[string(first)]++
			buf := make([]byte, server.MaxPiece())
			for {
				var request []byte
				switch k % 3 {
				case 0:
					request, err = server.Receive(tenSeconds)
					if err == nil {
						err = server.Send(bytes.ToUpper(request), tenSeconds)
					}
				case 1:
					var n int
					if n, err = server.ReceiveInto(buf, tenSeconds); err == nil {
						request = bytes.Clone(buf[:n])
						err = server.Send(bytes.ToUpper(request), tenSeconds)
					}
				default:
					var held []byte
					if held, err = server.ReceiveAcquire(tenSeconds); err == nil {
						request = bytes.Clone(held)
						var room []byte
						if room, err = server.SendAcquire(len(held), tenSeconds); err == nil {
							copy(room, bytes.ToUpper(held)) // the request in place stays valid until the commit
							err = server.SendCommit(len(held))
						}
					}
				}
				if errors.Is(err, fipc.ErrDisconnected) {
					results <- answered{requests, nil}
					return
				} else if err != nil {
					results <- answered{requests, err}
					return
				}
				requests[string(request)]++
			}
		}()
	}
	asked := make(chan error, clients)
	for k := range clients {
		client := joinService(t, name, fipc.Client)
		go func() {
			for start := 0; start < count; start += 10 { // ten requests on their way, then their ten replies, in order
				for i := start; i < start+10; i++ {
					if err := client.Send(fmt.Appendf(nil, "%d %d", k, i), tenSeconds); err != nil {
						asked <- err
						return
					}
				}
				for i := start; i < start+10; i++ {
					reply, err := client.Receive(tenSeconds)
					if err != nil {
						asked <- err
						return
					}
					if want := fmt.Sprintf("%d %d", k, i); string(reply) != want {
						asked <- fmt.Errorf("client %d: reply %q, want %q", k, reply, want)
						return
					}
				}
			}
			asked <- nil
		}()
	}
	for range clients {
		must(t, <-asked)
	}
	must(t, service.Close())
	all := make(map[string]int)
	for range servers {
		r := <-results
		must(t, r.err)
		if len(r.requests) == 0 {
			t.Fatal("a server answered nothing")
		}
		for request, n := range r.requests {
			all[request] += n
		}
	}
	for k := range clients {
		for i := range count {
			if n := all[fmt.Sprintf("%d %d", k, i)]; n != 1 {
				t.Fatalf("request %d %d answered %d times", k, i, n)
			}
		}
	}
	if len(all) != clients*count {
		t.Fatalf("%d requests answered, want %d", len(all), clients*count)
	}
}

func TestAClientsRepliesComeInTheOrderOfItsRequestsWhoeverRepliesFirst(t *testing.T) {
	guard(t, 30*time.Second)
	_, name := newService(t, "s_order", 4096, 64, 1, 2)
	client := joinService(t, name, fipc.Client)
	slow := joinService(t, name, fipc.Server)
	fast := joinService(t, name, fipc.Server)
	must(t, client.Send([]byte("first"), tenSeconds))
	must(t, client.Send([]byte("second"), tenSeconds))
	expectReceive(t, slow, "first") // each client's requests are taken in the order it sent them
	expectReceive(t, fast, "second")
	must(t, fast.Send([]byte("SECOND"), tenSeconds))
	_, err := client.Receive(50 * time.Millisecond) // the reply to the first request comes first: not there yet
	is(t, err, fipc.ErrTimeout, "Receive before the first reply")
	must(t, slow.Send([]byte("FIRST"), tenSeconds))
	expectReceive(t, client, "FIRST")
	expectReceive(t, client, "SECOND")
	_, err = client.Receive(fipc.NoWait)
	is(t, err, fipc.ErrTimeout, "nothing more")
}

func TestAServerThatClosesHoldingARequestAbandonsItAndTheNextIsAnswered(t *testing.T) {
	guard(t, 30*time.Second)
	_, name := newService(t, "s_abandoned", 4096, 64, 1, 2)
	client := joinService(t, name, fipc.Client)
	other := joinService(t, name, fipc.Server)
	quitter, err := fipc.JoinService(name, fipc.Server, tenSeconds)
	must(t, err)
	must(t, client.Send([]byte("one"), tenSeconds))
	expectReceive(t, quitter, "one")
	must(t, quitter.Close()) // holding it: the request is answered ErrAbandoned
	_, err = client.Receive(tenSeconds)
	var e *fipc.Error
	if !errors.As(err, &e) || !errors.Is(err, fipc.ErrAbandoned) || e.Len() != 0 || e.Code() != 10 ||
		e.Name() != "FIPC_ABANDONED" {
		t.Fatalf("Receive of the abandoned request: %v", err)
	}
	_, err = client.Receive(fipc.NoWait) // answered once: nothing more is owed
	is(t, err, fipc.ErrTimeout, "Receive after ErrAbandoned")

	// Call returns ErrAbandoned too, and the request counts as answered: the next Call gets its own reply
	quitter, err = fipc.JoinService(name, fipc.Server, tenSeconds)
	must(t, err)
	calling := make(chan error, 1)
	go func() {
		_, err := client.Call([]byte("two"), tenSeconds)
		calling <- err
	}()
	expectReceive(t, quitter, "two")
	must(t, quitter.Close())
	is(t, <-calling, fipc.ErrAbandoned, "the Call whose server closed")
	answering := answerOnce(other)
	expectCall(t, client, "three", tenSeconds)
	must(t, <-answering)
}

func TestWhatEachServiceRoleRefusesAndTheZeroCopyCalls(t *testing.T) {
	guard(t, 30*time.Second)
	service, name := newService(t, "s_roles", 1<<14, 1000, 1, 1)
	client := joinService(t, name, fipc.Client) // the owner's goroutine joins both: the library admits them
	server := joinService(t, name, fipc.Server)
	if client.MaxPiece() != 1000 || server.MaxPiece() != 1000 {
		t.Fatalf("MaxPiece %d %d", client.MaxPiece(), server.MaxPiece())
	}
	if client.Name() != name || server.Name() != name || service.Name() != name {
		t.Fatalf("names %q %q %q", client.Name(), server.Name(), service.Name())
	}
	var e *fipc.Error
	err := client.Send(make([]byte, 1001), fipc.NoWait)
	if !errors.As(err, &e) || !errors.Is(err, fipc.ErrTooLarge) || e.Len() != 1001 {
		t.Fatalf("a request over MaxPiece: %v", err)
	}
	is(t, client.Send(nil, fipc.NoWait), fipc.ErrInvalid, "an empty request")
	_, err = client.Call(nil, fipc.NoWait)
	is(t, err, fipc.ErrInvalid, "an empty Call")
	_, err = client.Call(make([]byte, 1001), fipc.NoWait)
	is(t, err, fipc.ErrTooLarge, "a Call over MaxPiece")
	_, err = client.SendAcquire(1001, fipc.NoWait)
	is(t, err, fipc.ErrTooLarge, "SendAcquire over MaxPiece")
	is(t, client.SendCommit(1), fipc.ErrInvalid, "SendCommit without a reservation")
	_, err = client.Receive(fipc.NoWait) // no reply to come
	is(t, err, fipc.ErrTimeout, "a client's Receive with nothing owed")
	client.ReceiveRelease() // nothing held: does nothing
	is(t, server.Send([]byte("x"), fipc.NoWait), fipc.ErrInvalid, "a reply with no request held")
	_, err = server.SendAcquire(1, fipc.NoWait)
	is(t, err, fipc.ErrInvalid, "a server's SendAcquire with no request held")
	is(t, server.SendCommit(1), fipc.ErrInvalid, "a server's SendCommit with no request held")
	_, err = server.Receive(fipc.NoWait)
	is(t, err, fipc.ErrTimeout, "a server's Receive with no request")
	_, err = server.Call([]byte("x"), fipc.NoWait)
	is(t, err, fipc.ErrInvalid, "a server's Call")
	for _, member := range []*fipc.Conn{client, server} {
		n, err := member.WaitForMembers(0, fipc.NoWait)
		if n != 0 || !errors.Is(err, fipc.ErrInvalid) {
			t.Fatalf("a member's WaitForMembers: %d, %v", n, err)
		}
	}

	// A server holds one request: its receives are ErrInvalid until it replies; ReceiveRelease does nothing
	must(t, client.Send(bytes.Repeat([]byte{7}, 1000), tenSeconds))
	_, err = server.ReceiveInto(make([]byte, 999), tenSeconds) // takes nothing
	if !errors.As(err, &e) || !errors.Is(err, fipc.ErrTooLarge) || e.Len() != 1000 {
		t.Fatalf("ReceiveInto a short buffer: %v", err)
	}
	held, err := server.ReceiveAcquire(tenSeconds)
	must(t, err)
	if !bytes.Equal(held, bytes.Repeat([]byte{7}, 1000)) {
		t.Fatalf("the request in place: %d bytes", len(held))
	}
	_, err = server.Receive(fipc.NoWait)
	is(t, err, fipc.ErrInvalid, "Receive while holding a request")
	_, err = server.ReceiveAcquire(fipc.NoWait)
	is(t, err, fipc.ErrInvalid, "ReceiveAcquire while holding a request")
	server.ReceiveRelease() // does nothing: the request is still held
	is(t, server.Send(make([]byte, 1001), fipc.NoWait), fipc.ErrTooLarge, "a reply over MaxPiece")
	room, err := server.SendAcquire(1000, fipc.NoWait) // a reply never waits
	must(t, err)
	copy(room, bytes.Repeat([]byte{3}, 1000))
	if !bytes.Equal(held, bytes.Repeat([]byte{7}, 1000)) { // the request in place stays whole until the commit
		t.Fatal("the request in place changed before the commit")
	}
	is(t, server.SendCommit(1001), fipc.ErrInvalid, "SendCommit over the reservation")
	must(t, server.SendCommit(1000))
	is(t, server.Send([]byte("x"), fipc.NoWait), fipc.ErrInvalid, "a second reply")
	_, err = client.ReceiveInto(make([]byte, 10), tenSeconds) // takes nothing
	is(t, err, fipc.ErrTooLarge, "a client's ReceiveInto a short buffer")
	expectReceive(t, client, string(bytes.Repeat([]byte{3}, 1000)))

	// The zero-copy calls the other way: a request committed in place, its reply received in place
	room, err = client.SendAcquire(5, tenSeconds)
	must(t, err)
	copy(room, "hello")
	must(t, client.SendCommit(5))
	expectReceive(t, server, "hello")
	must(t, server.Send([]byte("HELLO"), fipc.NoWait))
	reply, err := client.ReceiveAcquire(tenSeconds)
	if err != nil || string(reply) != "HELLO" {
		t.Fatalf("ReceiveAcquire of the reply: %q, %v", reply, err)
	}
	client.ReceiveRelease()
	answering := answerOnce(server)
	expectCall(t, client, "nothing owed", tenSeconds) // ErrInvalid if a request were still counted
	must(t, <-answering)
}

func TestAServicesMisuses(t *testing.T) {
	guard(t, 60*time.Second)
	for _, bad := range []struct{ capacity, maxMessage, clients, servers int }{
		{1000, 64, 1, 1}, {5000, 64, 1, 1}, {-1, 64, 1, 1}, {4096, 0, 1, 1}, {4096, 1025, 1, 1}, {4096, -1, 1, 1},
		{4096, 64, 0, 1}, {4096, 64, 65, 1}, {4096, 64, -1, 1}, {4096, 64, 1 << 32, 1}, {4096, 64, 1, 0},
		{4096, 64, 1, 65}, {4096, 64, 1, -1}, {4096, 64, 1, 1 << 32},
	} {
		_, err := fipc.CreateService(uniqueName("s_bad"), bad.capacity, bad.maxMessage, bad.clients, bad.servers)
		is(t, err, fipc.ErrInvalid, fmt.Sprintf("CreateService(%v)", bad))
	}
	_, err := fipc.CreateService("bad\x00name", 4096, 64, 1, 1)
	is(t, err, fipc.ErrInvalid, "CreateService with a NUL in the name")
	_, err = fipc.JoinService("bad\x00name", fipc.Client, fipc.NoWait)
	is(t, err, fipc.ErrInvalid, "JoinService with a NUL in the name")

	_, name := newService(t, "s_misuse", 4096, 64, 1, 1)
	_, err = fipc.CreateService(name, 4096, 64, 1, 1)
	is(t, err, fipc.ErrAddrInUse, "a second CreateService on the name")
	if err == nil || err.Error() != "FIPC_ADDR_IN_USE: another stream, broadcast, queue or service holds the name" {
		t.Fatalf("ErrAddrInUse's message: %v", err)
	}
	_, err = fipc.CreateQueue(name, 4096, 1, 1)
	is(t, err, fipc.ErrAddrInUse, "CreateQueue on a service's name")
	_, err = fipc.CreateBroadcast(name, 4096, 1, fipc.Forever)
	is(t, err, fipc.ErrAddrInUse, "CreateBroadcast on a service's name")
	_, err = fipc.CreateStream(name, 4096)
	is(t, err, fipc.ErrAddrInUse, "CreateStream on a service's name")
	for _, role := range []fipc.ServiceRole{fipc.AnyServiceRole, fipc.ServiceRole(fipc.Lossy),
		fipc.ServiceRole(fipc.Gated), fipc.ServiceRole(fipc.Producer), fipc.ServiceRole(fipc.Consumer), 7, -1} {
		_, err = fipc.JoinService(name, role, fipc.NoWait)
		is(t, err, fipc.ErrInvalid, "JoinService as "+role.String())
	}
	for _, role := range []fipc.SubscriberRole{fipc.Lossy, fipc.Gated} {
		_, err = fipc.JoinBroadcast(name, role, tenSeconds)
		is(t, err, fipc.ErrInvalid, "a "+role.String()+" subscriber on a service")
	}
	for _, role := range []fipc.QueueRole{fipc.Producer, fipc.Consumer, fipc.QueueRole(fipc.Client)} {
		_, err = fipc.JoinQueue(name, role, tenSeconds)
		is(t, err, fipc.ErrInvalid, "JoinQueue as "+role.String()+" on a service")
	}
	_, err = fipc.ConnectStream(name, tenSeconds)
	is(t, err, fipc.ErrInvalid, "ConnectStream to a service")

	queueName := uniqueName("s_a_queue")
	queue, err := fipc.CreateQueue(queueName, 4096, 1, 1)
	must(t, err)
	defer queue.Close()
	broadcastName := uniqueName("s_a_broadcast")
	publisher, err := fipc.CreateBroadcast(broadcastName, 4096, 1, fipc.Forever)
	must(t, err)
	defer publisher.Close()
	for _, other := range []string{queueName, broadcastName} {
		for _, role := range []fipc.ServiceRole{fipc.Client, fipc.Server} {
			_, err = fipc.JoinService(other, role, tenSeconds)
			is(t, err, fipc.ErrInvalid, "a "+role.String()+" on "+other)
		}
	}
	_, err = publisher.Call([]byte("x"), fipc.NoWait)
	is(t, err, fipc.ErrInvalid, "a publisher's Call")
	p := newPair(t, "s_stream", 4096)
	_, err = p.client.Call([]byte("x"), fipc.NoWait)
	is(t, err, fipc.ErrInvalid, "a stream's Call")
	must(t, p.client.Close())
	_, err = p.client.Call([]byte("x"), fipc.NoWait)
	is(t, err, fipc.ErrClosed, "a closed stream's Call")

	if fipc.Client.String() != "client" || fipc.Server.String() != "server" || fipc.AnyServiceRole.String() != "any" ||
		fipc.ServiceRole(7).String() != "ServiceRole(7)" {
		t.Fatal("ServiceRole.String")
	}
}

// A Call whose receive timed out or was cancelled leaves its reply to come; the next Call receives and drops it (within
// its own timeout) before it sends, so it returns the reply to its own request. A Call refuses (ErrInvalid) while a
// request sent with Send still has its reply to be received.
func TestACallThatGivesUpLeavesItsReplyOwedAndTheNextCallDropsIt(t *testing.T) {
	guard(t, 30*time.Second)
	_, name := newService(t, "s_owed", 4096, 64, 1, 1)
	client := joinService(t, name, fipc.Client)
	server := joinService(t, name, fipc.Server)
	_, err := client.Call([]byte("late"), 50*time.Millisecond) // the request went: no server replied in time
	is(t, err, fipc.ErrTimeout, "a Call no server answered in time")
	expectReceive(t, server, "late")
	must(t, server.Send([]byte("LATE"), fipc.NoWait)) // the late reply, owed to the call that gave up
	answering := answerOnce(server)
	expectCall(t, client, "next", tenSeconds) // not LATE
	must(t, <-answering)
	_, err = client.Receive(fipc.NoWait)
	is(t, err, fipc.ErrTimeout, "nothing more owed")

	// A Call whose owed reply hasn't come waits for it within its own timeout, and gives up before it sends
	_, err = client.Call([]byte("a"), fipc.NoWait) // a went; its reply is owed
	is(t, err, fipc.ErrTimeout, "Call(a, NoWait)")
	_, err = client.Call([]byte("b"), 50*time.Millisecond) // a's reply hasn't come: b never goes
	is(t, err, fipc.ErrTimeout, "Call(b) while a's reply is owed")
	expectReceive(t, server, "a")
	must(t, server.Send([]byte("A"), fipc.NoWait))
	_, err = server.Receive(fipc.NoWait)
	is(t, err, fipc.ErrTimeout, "no b")
	receiving := make(chan []byte, 1)
	go func() {
		request, _ := server.Receive(tenSeconds)
		receiving <- request
	}()
	_, err = client.Call([]byte("c"), 200*time.Millisecond) // drops A, sends c, and gives up on its reply
	is(t, err, fipc.ErrTimeout, "Call(c), unanswered")
	if request := <-receiving; string(request) != "c" {
		t.Fatalf("the server received %q", request)
	}
	must(t, server.Send([]byte("C"), fipc.NoWait)) // owed to the call that gave up
	expectReceive(t, client, "C")                  // a receive of the caller's own takes it, in request order
	_, err = client.Receive(fipc.NoWait)
	is(t, err, fipc.ErrTimeout, "nothing more owed")

	// A request of Send's whose reply hasn't been received: Call refuses, and leaves it for Receive
	must(t, client.Send([]byte("mine"), tenSeconds))
	_, err = client.Call([]byte("x"), tenSeconds)
	is(t, err, fipc.ErrInvalid, "Call while a Send's reply is owed")
	must(t, answerN(server, 1))
	expectReceive(t, client, "MINE")
	answering = answerOnce(server)
	expectCall(t, client, "x", tenSeconds)
	must(t, <-answering)

	// A request is counted before its send, and a failed send uncounts it: the Calls after them go on
	is(t, client.Send(nil, fipc.NoWait), fipc.ErrInvalid, "an empty request")
	room, err := client.SendAcquire(4, tenSeconds)
	must(t, err)
	copy(room, "abcd")
	is(t, client.SendCommit(5), fipc.ErrInvalid, "SendCommit over the reservation")
	must(t, client.SendCommit(4))
	must(t, answerN(server, 1))
	_, err = client.ReceiveAcquire(tenSeconds) // the reply in place: it counts at the receive
	must(t, err)
	client.ReceiveRelease()
	answering = answerOnce(server)
	expectCall(t, client, "after the sends", tenSeconds)
	must(t, <-answering)
}

// A Call that had to drop an owed reply, or wait for a direction another goroutine held, sends its request only if its
// timeout has time left: once it has run out, ErrTimeout, and no server sees that request.
func TestACallWhoseTimeRunsOutBeforeItsSendNeverSendsIt(t *testing.T) {
	guard(t, 30*time.Second)
	_, name := newService(t, "s_spent", 1<<16, 4096, 1, 1)
	client := joinService(t, name, fipc.Client)
	server := joinService(t, name, fipc.Server)
	_, err := client.Call([]byte("late"), 50*time.Millisecond)
	is(t, err, fipc.ErrTimeout, "a Call no server answered in time")
	expectReceive(t, server, "late")
	must(t, server.Send(bytes.Repeat([]byte("L"), 4096), fipc.NoWait)) // in the ring before the next Call
	// Its discard takes the owed reply at once, but the nanosecond has gone by: the request never goes
	_, err = client.Call([]byte("never"), time.Nanosecond)
	is(t, err, fipc.ErrTimeout, "a Call whose time ran out in its discard")
	_, err = server.Receive(50 * time.Millisecond)
	is(t, err, fipc.ErrTimeout, "the server never saw the request")
	answering := answerOnce(server)
	expectCall(t, client, "next", tenSeconds) // the owed reply was dropped: its own
	must(t, <-answering)

	// A Receive of another goroutine holds the receiving direction past the Call's timeout
	receiving := make(chan error, 1)
	go func() {
		_, err := client.Receive(time.Second)
		receiving <- err
	}()
	time.Sleep(200 * time.Millisecond) // let it take the direction and wait
	_, err = client.Call([]byte("blocked"), 100*time.Millisecond)
	is(t, err, fipc.ErrTimeout, "a Call whose time ran out waiting for the Receive")
	is(t, <-receiving, fipc.ErrTimeout, "the Receive with nothing owed")
	_, err = server.Receive(50 * time.Millisecond)
	is(t, err, fipc.ErrTimeout, "the server never saw the blocked request")
	answering = answerOnce(server)
	expectCall(t, client, "after", tenSeconds)
	must(t, <-answering)
}

// A Call checks its request before it waits for anything: an empty one and one over MaxPiece fail at once, even while a
// reply is owed, which stays owed.
func TestACallChecksItsRequestBeforeItDropsAnOwedReply(t *testing.T) {
	guard(t, 30*time.Second)
	_, name := newService(t, "s_check", 4096, 64, 1, 1)
	client := joinService(t, name, fipc.Client)
	server := joinService(t, name, fipc.Server)
	_, err := client.Call([]byte("owed"), 50*time.Millisecond)
	is(t, err, fipc.ErrTimeout, "a Call no server answered in time")
	expectReceive(t, server, "owed") // held: its reply hasn't come
	started := time.Now()
	_, err = client.Call(nil, fipc.Forever)
	is(t, err, fipc.ErrInvalid, "an empty request")
	_, err = client.Call([]byte{}, fipc.Forever)
	is(t, err, fipc.ErrInvalid, "a request of no bytes")
	_, err = client.Call(make([]byte, client.MaxPiece()+1), fipc.Forever)
	var e *fipc.Error
	if !errors.As(err, &e) || !errors.Is(err, fipc.ErrTooLarge) || e.Len() != client.MaxPiece()+1 {
		t.Fatalf("a request over MaxPiece: %v", err)
	}
	if waited := time.Since(started); waited > time.Second {
		t.Fatalf("the checks waited %v", waited)
	}
	must(t, server.Send([]byte("OWED"), fipc.NoWait))
	answering := answerOnce(server)
	expectCall(t, client, "own", tenSeconds) // OWED, still owed, was dropped first
	must(t, <-answering)
}

// An ErrAbandoned in place of the reply owed to a Call that gave up counts: the next Call drops it and returns its own
// request's reply.
func TestAnAbandonedReplyOwedToACallIsDroppedByTheNext(t *testing.T) {
	guard(t, 30*time.Second)
	_, name := newService(t, "s_owed_abandoned", 4096, 64, 1, 2)
	client := joinService(t, name, fipc.Client)
	other := joinService(t, name, fipc.Server)
	quitter, err := fipc.JoinService(name, fipc.Server, tenSeconds)
	must(t, err)
	// Only the quitter receives until it closes: the other server waits for its turn later
	calling := make(chan error, 1)
	go func() {
		_, err := client.Call([]byte("gives up"), 200*time.Millisecond)
		calling <- err
	}()
	expectReceive(t, quitter, "gives up")
	is(t, <-calling, fipc.ErrTimeout, "the Call that gave up")
	must(t, quitter.Close()) // holding it: ErrAbandoned, owed to the Call that gave up
	answering := answerOnce(other)
	expectCall(t, client, "next", tenSeconds)
	must(t, <-answering)
	_, err = client.Receive(fipc.NoWait)
	is(t, err, fipc.ErrTimeout, "nothing more owed")
}

// Once the service has ended, a Call returns ErrDisconnected from its discard (a reply owed that never came) or from
// its send (nothing owed).
func TestACallAfterTheServicesEnd(t *testing.T) {
	guard(t, 30*time.Second)
	name := uniqueName("s_call_end")
	service, err := fipc.CreateService(name, 4096, 64, 2, 1)
	must(t, err)
	gaveUp := joinService(t, name, fipc.Client)
	idle := joinService(t, name, fipc.Client)
	server := joinService(t, name, fipc.Server)
	_, err = gaveUp.Call([]byte("unanswered"), 50*time.Millisecond)
	is(t, err, fipc.ErrTimeout, "a Call no server answered in time")
	expectReceive(t, server, "unanswered")
	must(t, service.Close())
	_, err = gaveUp.Call([]byte("x"), tenSeconds)
	is(t, err, fipc.ErrDisconnected, "a Call whose owed reply never came, after the end")
	_, err = gaveUp.Call([]byte("x"), tenSeconds)
	is(t, err, fipc.ErrDisconnected, "and again")
	_, err = idle.Call([]byte("x"), tenSeconds)
	is(t, err, fipc.ErrDisconnected, "a Call with nothing owed, after the end")
}

// Sends on one goroutine and receives on another, at once: a reply may be received before its send has returned, and
// the counts end even, so a Call goes through after them.
func TestSendsAndReceivesOnTwoGoroutinesKeepTheCountEven(t *testing.T) {
	guard(t, 60*time.Second)
	const count = 2000
	service, name := newService(t, "s_halves", 1<<16, 64, 1, 1)
	client := joinService(t, name, fipc.Client)
	server := joinService(t, name, fipc.Server)
	answering := make(chan error, 1)
	go func() {
		n, err := answerUntilTheEnd(server)
		if err == nil && n != count+1 {
			err = fmt.Errorf("answered %d requests", n)
		}
		answering <- err
	}()
	received := make(chan error, 1)
	go func() {
		for i := range count {
			reply, err := client.Receive(tenSeconds)
			if err != nil {
				received <- err
				return
			}
			if want := fmt.Sprintf("REQUEST %d", i); string(reply) != want {
				received <- fmt.Errorf("reply %q, want %q", reply, want)
				return
			}
		}
		received <- nil
	}()
	for i := range count {
		must(t, client.Send(fmt.Appendf(nil, "request %d", i), tenSeconds))
	}
	must(t, <-received)
	expectCall(t, client, "and a call", tenSeconds)
	must(t, service.Close())
	must(t, <-answering)
}

// Calls from many goroutines on one client: each returns its own request's reply, whatever order their turns come in.
func TestConcurrentCallsOnOneClientEachGetTheirOwnReply(t *testing.T) {
	guard(t, 120*time.Second)
	const goroutines, count = 8, 200
	service, name := newService(t, "s_concurrent", 4096, 64, 1, 2)
	client := joinService(t, name, fipc.Client)
	answering := make(chan error, 2)
	for range 2 {
		server := joinService(t, name, fipc.Server)
		go func() {
			_, err := answerUntilTheEnd(server)
			answering <- err
		}()
	}
	asked := make(chan error, goroutines)
	for g := range goroutines {
		go func() {
			for i := range count {
				request := fmt.Sprintf("goroutine %d call %d", g, i)
				reply, err := client.Call([]byte(request), tenSeconds)
				if err != nil {
					asked <- fmt.Errorf("%s: %w", request, err)
					return
				}
				if want := bytes.ToUpper([]byte(request)); !bytes.Equal(reply, want) {
					asked <- fmt.Errorf("the reply to %q is %q", request, reply)
					return
				}
			}
			asked <- nil
		}()
	}
	for range goroutines {
		must(t, <-asked)
	}
	must(t, service.Close())
	must(t, <-answering)
	must(t, <-answering)
}

// A Call that waits for its turn behind another waits within its own timeout, and a Cancel or Close ends its wait; a
// Call that never had its turn sent nothing.
func TestACallWaitingForItsTurnTimesOutAndIsCancelled(t *testing.T) {
	guard(t, 30*time.Second)
	_, name := newService(t, "s_turns", 4096, 64, 2, 1)
	client := joinService(t, name, fipc.Client)
	server := joinService(t, name, fipc.Server)
	type called struct {
		reply []byte
		err   error
	}
	callAside := func(conn *fipc.Conn, request string, timeout time.Duration) <-chan called {
		ch := make(chan called, 1)
		go func() {
			reply, err := conn.Call([]byte(request), timeout)
			ch <- called{reply, err}
		}()
		return ch
	}
	first := callAside(client, "first", fipc.Forever)
	expectReceive(t, server, "first") // the first Call has its turn, and waits for its reply

	_, err := client.Call([]byte("no turn"), fipc.NoWait)
	is(t, err, fipc.ErrTimeout, "a Call with NoWait behind another")
	started := time.Now()
	_, err = client.Call([]byte("short"), 100*time.Millisecond)
	is(t, err, fipc.ErrTimeout, "a Call with a short timeout behind another")
	if waited := time.Since(started); waited < 100*time.Millisecond || waited > 5*time.Second {
		t.Fatalf("the Call waited %v for its turn", waited)
	}
	_, err = server.Receive(fipc.NoWait)
	is(t, err, fipc.ErrInvalid, "the server still holds first") // and no other request went
	must(t, server.Send([]byte("FIRST"), fipc.NoWait))
	if c := <-first; c.err != nil || string(c.reply) != "FIRST" {
		t.Fatalf("the first Call: %q, %v", c.reply, c.err)
	}
	_, err = server.Receive(fipc.NoWait)
	is(t, err, fipc.ErrTimeout, "no request of the Calls that never had their turn")

	// Cancel ends the Call that waits for its reply and the one that waits for its turn
	doomed := callAside(client, "doomed", fipc.Forever)
	expectReceive(t, server, "doomed")
	queued := callAside(client, "queued", fipc.Forever)
	time.Sleep(50 * time.Millisecond) // let it wait for its turn
	client.Cancel()
	is(t, (<-doomed).err, fipc.ErrCancelled, "the cancelled Call")
	is(t, (<-queued).err, fipc.ErrCancelled, "the cancelled Call that waited for its turn")
	must(t, server.Send([]byte("DOOMED"), fipc.NoWait)) // owed to the cancelled call
	expectReceive(t, client, "DOOMED")                  // a receive that needn't wait still works
	_, err = client.Receive(tenSeconds)
	is(t, err, fipc.ErrCancelled, "a Receive after the Cancel") // final
	_, err = server.Receive(fipc.NoWait)
	is(t, err, fipc.ErrTimeout, "no request of the queued Call")

	// Close ends a Call that waits for its turn behind one that waits for its reply
	other := joinService(t, name, fipc.Client)
	held := callAside(other, "held", fipc.Forever)
	expectReceive(t, server, "held")
	behind := callAside(other, "behind", fipc.Forever)
	time.Sleep(50 * time.Millisecond)
	must(t, other.Close())
	is(t, (<-held).err, fipc.ErrCancelled, "the Call Close cancelled")
	is(t, (<-behind).err, fipc.ErrCancelled, "the Call that waited for its turn at Close")
	_, err = other.Call([]byte("x"), fipc.NoWait)
	is(t, err, fipc.ErrClosed, "a Call after Close")
	must(t, server.Send([]byte("HELD"), fipc.NoWait)) // to a client that has left: dropped
}

func TestACallAfterCancelReturnsErrCancelledAndSendsNothing(t *testing.T) {
	guard(t, 30*time.Second)
	_, name := newService(t, "s_call_cancelled", 4096, 64, 1, 1)
	server := joinService(t, name, fipc.Server)
	client := joinService(t, name, fipc.Client)
	client.Cancel()
	_, err := client.Call(nil, tenSeconds)
	is(t, err, fipc.ErrInvalid, "an empty request after the Cancel") // the request's checks come first
	_, err = client.Call(make([]byte, 65), tenSeconds)
	is(t, err, fipc.ErrTooLarge, "a request over MaxPiece after the Cancel")
	for _, timeout := range []time.Duration{fipc.Forever, fipc.NoWait, tenSeconds} {
		_, err = client.Call([]byte("never sent"), timeout)
		is(t, err, fipc.ErrCancelled, fmt.Sprintf("a Call after the Cancel, timeout %v", timeout))
	}
	_, err = server.Receive(200 * time.Millisecond)
	is(t, err, fipc.ErrTimeout, "the server's Receive: no request of the Calls after the Cancel")
}

func TestACancelEndsAServersWaitAndTheServerGoesOn(t *testing.T) {
	guard(t, 30*time.Second)
	_, name := newService(t, "s_cancel", 4096, 64, 1, 1)
	server := joinService(t, name, fipc.Server)
	client := joinService(t, name, fipc.Client)
	waiting := make(chan error, 1)
	go func() {
		_, err := server.Receive(fipc.Forever)
		waiting <- err
	}()
	time.Sleep(50 * time.Millisecond) // let it wait
	server.Cancel()
	is(t, <-waiting, fipc.ErrCancelled, "the cancelled Receive")
	must(t, client.Send([]byte("after"), tenSeconds))
	expectReceive(t, server, "after")                  // a request there: the receive needn't wait
	must(t, server.Send([]byte("AFTER"), fipc.NoWait)) // a reply never waits
	expectReceive(t, client, "AFTER")
	_, err := server.Receive(tenSeconds)
	is(t, err, fipc.ErrCancelled, "a later Receive") // final
}

func TestTheServicesMemberCountItsWaitAndItsCancel(t *testing.T) {
	guard(t, 30*time.Second)
	service, name := newService(t, "s_count", 4096, 64, 2, 3)
	for _, role := range []fipc.ServiceRole{fipc.AnyServiceRole, fipc.Client, fipc.Server} {
		if n, err := service.WaitForMembers(role, 0, fipc.NoWait); n != 0 || err != nil {
			t.Fatalf("WaitForMembers(%v, 0) = %d, %v", role, n, err)
		}
	}
	n, err := service.WaitForMembers(fipc.AnyServiceRole, 1, fipc.NoWait)
	if n != 0 || !errors.Is(err, fipc.ErrTimeout) {
		t.Fatalf("WaitForMembers(AnyServiceRole, 1, NoWait) = %d, %v", n, err)
	}
	for _, c := range []struct {
		role    fipc.ServiceRole
		atLeast int
	}{{fipc.Client, 3}, {fipc.Server, 4}, {fipc.AnyServiceRole, 6}, {fipc.Client, -1},
		{fipc.AnyServiceRole, 1 << 32}, {fipc.ServiceRole(fipc.Producer), 1}, {fipc.ServiceRole(fipc.Gated), 1}} {
		n, err := service.WaitForMembers(c.role, c.atLeast, fipc.NoWait)
		if n != 0 || !errors.Is(err, fipc.ErrInvalid) {
			t.Fatalf("WaitForMembers(%v, %d) = %d, %v", c.role, c.atLeast, n, err)
		}
	}

	// A count that waits on one goroutine sees the joins of another
	type counted struct {
		n   int
		err error
	}
	aside := func(role fipc.ServiceRole, atLeast int, timeout time.Duration) <-chan counted {
		ch := make(chan counted, 1)
		go func() {
			n, err := service.WaitForMembers(role, atLeast, timeout)
			ch <- counted{n, err}
		}()
		return ch
	}
	servers := aside(fipc.Server, 1, tenSeconds)
	server := joinService(t, name, fipc.Server)
	client := joinService(t, name, fipc.Client)
	if c := <-servers; c.n < 1 || c.err != nil {
		t.Fatalf("WaitForMembers(Server, 1) = %d, %v", c.n, c.err)
	}
	if n, err := service.WaitForMembers(fipc.AnyServiceRole, 2, tenSeconds); n != 2 || err != nil {
		t.Fatalf("WaitForMembers(AnyServiceRole, 2) = %d, %v", n, err)
	}
	if n, err := service.WaitForMembers(fipc.Client, 2, 50*time.Millisecond); n != 1 ||
		!errors.Is(err, fipc.ErrTimeout) {
		t.Fatalf("WaitForMembers(Client, 2) = %d, %v", n, err)
	}

	// Cancel: a wait, now or later, returns ErrCancelled with the count; the service goes on
	waiting := aside(fipc.Server, 2, fipc.Forever)
	time.Sleep(50 * time.Millisecond) // let it wait
	service.Cancel()
	if c := <-waiting; c.n != 1 || !errors.Is(c.err, fipc.ErrCancelled) {
		t.Fatalf("the waiting WaitForMembers: %d, %v", c.n, c.err)
	}
	if n, err := service.WaitForMembers(fipc.AnyServiceRole, 3, fipc.Forever); n != 2 ||
		!errors.Is(err, fipc.ErrCancelled) {
		t.Fatalf("a later WaitForMembers: %d, %v", n, err)
	}
	service.Cancel() // final: a second one changes nothing
	answering := answerOnce(server)
	expectCall(t, client, "after the cancel", tenSeconds)
	must(t, <-answering)

	// Close cancels a wait in progress, which returns; then every call returns ErrClosed
	other, err := fipc.CreateService(uniqueName("s_close"), 4096, 64, 1, 1)
	must(t, err)
	pending := make(chan counted, 1)
	go func() {
		n, err := other.WaitForMembers(fipc.AnyServiceRole, 1, fipc.Forever)
		pending <- counted{n, err}
	}()
	time.Sleep(50 * time.Millisecond) // let it wait
	must(t, other.Close())
	if c := <-pending; !(c.n == 0 && errors.Is(c.err, fipc.ErrCancelled)) && !errors.Is(c.err, fipc.ErrClosed) {
		t.Fatalf("the wait that Close ended: %d, %v", c.n, c.err)
	}
	_, err = other.WaitForMembers(fipc.AnyServiceRole, 0, fipc.NoWait)
	is(t, err, fipc.ErrClosed, "WaitForMembers after Close")
	other.Cancel() // does nothing
	is(t, other.Close(), fipc.ErrClosed, "a second Close")
}

func TestTheServicesCloseEndsItForClientsAndServers(t *testing.T) {
	guard(t, 30*time.Second)
	name := uniqueName("s_end")
	service, err := fipc.CreateService(name, 4096, 64, 1, 2)
	must(t, err)
	client := joinService(t, name, fipc.Client)
	server := joinService(t, name, fipc.Server)
	holder := joinService(t, name, fipc.Server)
	must(t, client.Send([]byte("done"), tenSeconds))
	must(t, client.Send([]byte("held"), tenSeconds))
	must(t, answerN(server, 1))
	expectReceive(t, holder, "held")
	must(t, service.Close())
	is(t, service.Close(), fipc.ErrClosed, "a second Close")
	is(t, client.Send([]byte("after"), tenSeconds), fipc.ErrDisconnected, "a request after the end")
	_, err = server.Receive(tenSeconds)
	is(t, err, fipc.ErrDisconnected, "a server's Receive after the end")
	expectReceive(t, client, "DONE") // the replies that came before the end, then ErrDisconnected
	_, err = client.Receive(tenSeconds)
	is(t, err, fipc.ErrDisconnected, "a client's Receive after the end")
	is(t, holder.Send([]byte("HELD"), fipc.NoWait), fipc.ErrDisconnected, "a reply after the end")
	// The reply to held never came: it is still owed to a Send, so a Call refuses
	_, err = client.Call([]byte("x"), tenSeconds)
	is(t, err, fipc.ErrInvalid, "a Call after the end, with a Send's reply owed")
	again, err := fipc.CreateService(name, 4096, 64, 1, 1) // the name is free again at once
	must(t, err)
	must(t, again.Close())
}

// startService creates a service on name, drops it, and returns a joined server: the program keeps no reference to
// the Service.
func startService(t *testing.T, name string) *fipc.Conn {
	t.Helper()
	if _, err := fipc.CreateService(name, 4096, 64, 1, 1); err != nil {
		t.Fatal(err)
	}
	return joinService(t, name, fipc.Server)
}

// joinAndDrop joins name as a client and drops the connection without closing it.
func joinAndDrop(t *testing.T, name string) {
	t.Helper()
	if _, err := fipc.JoinService(name, fipc.Client, tenSeconds); err != nil {
		t.Fatal(err)
	}
}

// The garbage collector never closes a service: one the program dropped goes on until its process ends, and holds its
// name meanwhile. A client it drops is closed, and its place is free again.
func TestTheGarbageCollectorNeverEndsAServiceAndClosesADroppedClient(t *testing.T) {
	guard(t, 30*time.Second)
	name := uniqueName("gc_service")
	server := startService(t, name)
	joinAndDrop(t, name) // the service's one client place
	deadline := time.Now().Add(tenSeconds)
	var client *fipc.Conn
	for client == nil {
		runtime.GC()
		time.Sleep(20 * time.Millisecond) // time for cleanups, which run on a goroutine of their own
		var err error
		if client, err = fipc.JoinService(name, fipc.Client, 100*time.Millisecond); errors.Is(err, fipc.ErrTimeout) {
			if time.Now().After(deadline) {
				t.Fatal("the dropped client's place is still taken")
			}
		} else {
			must(t, err)
		}
	}
	defer client.Close()
	answering := answerOnce(server)
	expectCall(t, client, "still here", tenSeconds)
	must(t, <-answering)
	_, err := fipc.CreateService(name, 4096, 64, 1, 1)
	is(t, err, fipc.ErrAddrInUse, "a second CreateService of the dropped service's name")
}

// Requests and replies longer than Receive's first buffer (4 KiB) are received whole, on both sides, and counted once.
func TestRequestsLongerThanTheReceiveBufferAreReceivedWhole(t *testing.T) {
	guard(t, 30*time.Second)
	_, name := newService(t, "s_large", 1<<16, 1<<14, 1, 1)
	client := joinService(t, name, fipc.Client)
	server := joinService(t, name, fipc.Server)
	request := pattern(1 << 14)
	answering := make(chan error, 1)
	go func() {
		got, err := server.Receive(tenSeconds)
		if err == nil && !bytes.Equal(got, request) {
			err = fmt.Errorf("the request: %d bytes", len(got))
		}
		if err == nil {
			slices.Reverse(got)
			err = server.Send(got, tenSeconds)
		}
		answering <- err
	}()
	reply, err := client.Call(request, tenSeconds)
	must(t, err)
	must(t, <-answering)
	want := bytes.Clone(request)
	slices.Reverse(want)
	if !bytes.Equal(reply, want) {
		t.Fatalf("the reply: %d bytes", len(reply))
	}
	answered := answerOnce(server)
	expectCall(t, client, "nothing owed", tenSeconds)
	must(t, <-answered)
}

// The examples' exchange on goroutines, either way round: the client's JoinService waits for the service, and the
// server's Receive for a request.
func TestTheServiceExamplesExchangeInEitherOrder(t *testing.T) {
	guard(t, 60*time.Second)
	for _, clientFirst := range []bool{true, false} {
		name := uniqueName("s_example")
		asking := make(chan error, 1)
		if clientFirst {
			go func() { asking <- joinAndCall(name, 3) }()
			time.Sleep(200 * time.Millisecond) // likely, not certain, to join before the service exists
		}
		serving := make(chan error, 1)
		go func() { serving <- serve(name, 3, 1) }()
		if !clientFirst {
			go func() { asking <- joinAndCall(name, 3) }()
		}
		must(t, <-asking)
		must(t, <-serving)
	}
}
