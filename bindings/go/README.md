# fipc for Go

[![Go Reference](https://pkg.go.dev/badge/github.com/fastipc/fastipc/bindings/go/fipc.svg)](https://pkg.go.dev/github.com/fastipc/fastipc/bindings/go/fipc)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/fastipc/fastipc/blob/main/LICENSE)

Messages between processes on one machine, through shared memory: the Go binding of
[FastIPC](https://github.com/fastipc/fastipc), a small library written in Zig
([`include/fipc.h`](https://github.com/fastipc/fastipc/blob/main/include/fipc.h)). Four kinds, each created on a
name:

- **Streams:** a server creates a stream (`fipc.CreateStream`) and its `Listener` accepts one client at a time; a
  client connects to the name (`fipc.ConnectStream`). The connect waits for the server's `Accept`, so a client and its
  server run in different processes or on different goroutines. Each connection has a shared-memory segment with one
  ring per direction, and reports the peer's end as soon as its process exits or closes the connection.
- **[Broadcasts](#broadcasts):** one publisher (`fipc.CreateBroadcast`) sends every message to each of its
  subscribers (`fipc.JoinBroadcast`).
- **[Work queues](#work-queues):** producers and consumers join a queue (`fipc.CreateQueue`, `fipc.JoinQueue`), and
  each message goes to one consumer.
- **[Services](#services):** clients and a pool of servers join a service (`fipc.CreateService`, `fipc.JoinService`):
  each request goes to one server, and its reply back to its client (`conn.Call`).

The peer can be written in any language with a binding: a Go process talks to a Zig, C, C++, Python, C#, Java, Rust,
Lua or JavaScript process exactly as it talks to another Go process (the protocol is the same, and [every pair of
languages is tested](https://fastipc.github.io/fastipc/#interop)). Use it where two local processes must talk at memory
speed: a game and its editor or tools, a UI and its engine, a Go service and a Python front end.

## Installation

```bash
go get github.com/fastipc/fastipc/bindings/go/fipc@latest
```

Go 1.27 or newer. One package, `fipc` (`import "github.com/fastipc/fastipc/bindings/go/fipc"`), without cgo: it loads
the native library at run time through [purego](https://github.com/ebitengine/purego), the module's one dependency,
so `CGO_ENABLED=0` builds work, cross-compiling works, and no C compiler is needed. The module carries the native
library for Windows x64 (Windows 11 / Server 2022 or newer) and Linux x64 (glibc 2.34 or newer: Ubuntu 22.04, Debian
12, RHEL 9 and later), which need an x86-64-v3 CPU (AVX2: Intel Haswell, AMD Zen or later), for Linux on ARM64 (glibc
2.34 or newer) and for macOS 14.4 or newer on Apple Silicon. `go run`, `go test` and a program built on your machine
find it by themselves; a program run on another machine needs it next to its executable (see [Shipping your
program](#shipping-your-program)).

## Use

```go
// server.go
package main

import (
	"bytes"
	"log"

	"github.com/fastipc/fastipc/bindings/go/fipc"
)

func main() {
	listener, err := fipc.CreateStream("my_channel", 1<<20) // rings of 1 MiB each way
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()
	conn, err := listener.Accept(fipc.Forever) // waits for a client
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	message, err := conn.Receive(fipc.Forever)
	if err != nil {
		log.Fatal(err)
	}
	if err := conn.Send(bytes.ToUpper(message), fipc.Forever); err != nil {
		log.Fatal(err)
	}
}
```

```go
// client.go
package main

import (
	"fmt"
	"log"
	"time"

	"github.com/fastipc/fastipc/bindings/go/fipc"
)

func main() {
	conn, err := fipc.ConnectStream("my_channel", 5*time.Second) // waits up to 5 s for the server
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	if err := conn.Send([]byte("ping"), fipc.Forever); err != nil {
		log.Fatal(err)
	}
	reply, err := conn.Receive(fipc.Forever)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(reply)) // PING
}
```

Run each in its own terminal; in a checkout of the repository they are the module's examples, run in this folder: `go
run ./examples/server` and `go run ./examples/client`. The clients of the other languages in the [repository's
README](https://github.com/fastipc/fastipc#examples) work against this server too, and this client against their
servers.

- **Copied messages:** `conn.Send(msg)` and `conn.Receive()`, which returns the message in a new slice, or
  `conn.ReceiveInto(buf)`, which fills your buffer and returns the length. A message of any size may be sent (at
  least 1 byte); it travels in pieces and arrives whole.
- **Zero-copy** (messages of one piece, up to `conn.MaxPiece()` bytes): `conn.SendAcquire(n)` returns a slice over
  room in the ring: write the message into it, then `conn.SendCommit(n)`. `conn.ReceiveAcquire()` returns a slice over
  the message in the ring (read-only), and `conn.ReceiveRelease()` frees its room. These slices point into shared
  memory: one from `SendAcquire` is valid until `SendCommit`, the next send or `Close`, one from `ReceiveAcquire` until
  `ReceiveRelease`, the next receive or `Close`, and using one after that is undefined (it may crash the program).
  Keep the connection reachable while you use one.
- **Batches:** `conn.SendMany(msgs, timeout)` sends a `[][]byte`, published together with at most one wake of the
  receivers, and returns how many went with the first error; `conn.ReceiveMany(buf, lens, timeout)` waits for the next
  message, then takes every further one already there that fits `buf`, packed back to back, their lengths in `lens`,
  and returns how many. On every kind: a broadcast's subscriber takes those its publisher has moved past, and a
  service's client sends a batch of requests (owed as its sends are: `Call` returns `ErrInvalid` until their replies
  are received) and receives their replies in order; a server's batches return `ErrInvalid`. Batch both ends: a
  receiver that takes one message per call falls behind a batching sender.

```go
sent, err := conn.SendMany([][]byte{[]byte("one"), []byte("two"), []byte("three")}, fipc.Forever)
if err != nil {
	return err // sent says how many went before the error
}
buf, lens := make([]byte, 1<<16), make([]int, 64)
n, err := peer.ReceiveMany(buf, lens, fipc.Forever) // every message already there, up to 64
if err != nil {
	return err
}
at := 0
for _, l := range lens[:n] {
	handle(buf[at : at+l])
	at += l
}
```

### Errors and timeouts

A call that doesn't succeed returns one of the errors `fipc.ErrTimeout`, `fipc.ErrDisconnected` (the peer's end: final,
so close the connection, and accept or connect a new one), `fipc.ErrCancelled`, `fipc.ErrTooLarge`, `fipc.ErrInvalid`,
`fipc.ErrNoMemory`, `fipc.ErrAddrInUse`, on a broadcast `fipc.ErrOverrun` and `fipc.ErrUnsubscribed`, and on a
service's client `fipc.ErrAbandoned`, the C API's results, which are `*fipc.Error` values: test them with `errors.Is`. Their messages are the C names (`FIPC_TIMEOUT: the
timeout ran out`), and `ErrTimeout.Timeout()` is true, as for the timeouts of packages `os` and `net`. A call on a
closed listener, connection, queue or service returns `fipc.ErrClosed`; one that can't load the library, an error that wraps
`fipc.ErrNoLibrary` and says where it looked.

Every call that can wait takes its timeout last, as a `time.Duration`: `fipc.NoWait` (0) doesn't wait, and
`fipc.Forever` (or any negative duration) waits as long as it takes.

```go
msg, err := conn.Receive(100 * time.Millisecond)
switch {
case err == nil:
	handle(msg)
case errors.Is(err, fipc.ErrTimeout): // nothing yet
default:
	return err // fipc.ErrDisconnected: the peer is gone
}
```

A receive that is `ErrTooLarge` for your buffer leaves the message queued, and its error says the message's length:

```go
n, err := conn.ReceiveInto(buf, fipc.Forever)
var e *fipc.Error
if errors.As(err, &e) && e.Len() > 0 { // ErrTooLarge: make room, then again
	buf = make([]byte, e.Len())
	n, err = conn.ReceiveInto(buf, fipc.Forever)
}
```

### Broadcasts

A broadcast sends every message of one publisher to each of its subscribers (up to 1024, in any processes of the user);
each message is stored once, in memory every subscriber maps. `fipc.CreateBroadcast(name, capacity, maxSubscribers,
maxStall)` returns the publisher, a `*Conn` whose sends go to every subscriber, the broadcast's one handle: its `Close`
ends the broadcast and frees the name. Messages are of one piece, up to `publisher.MaxPiece()` bytes (a quarter of the
capacity). `fipc.JoinBroadcast(name, role, timeout)` joins the broadcast and returns the subscriber's `*Conn`, which
receives. The publisher's process admits each joiner by itself, on a thread of the library's: no goroutine waits for
them, and a publisher and its subscribers may share a goroutine.

```go
// publisher.go
package main

import (
	"fmt"
	"log"
	"time"

	"github.com/fastipc/fastipc/bindings/go/fipc"
)

func main() {
	// 64 KiB of messages, up to 8 subscribers; one that holds the publisher back 5 s is dropped
	publisher, err := fipc.CreateBroadcast("my_broadcast", 1<<16, 8, 5*time.Second)
	if err != nil {
		log.Fatal(err)
	}
	if _, err := publisher.WaitForMembers(1, fipc.Forever); err != nil { // waits for the first subscriber
		log.Fatal(err)
	}
	for i := 1; i <= 3; i++ {
		if err := publisher.Send(fmt.Appendf(nil, "tick %d", i), fipc.Forever); err != nil {
			log.Fatal(err)
		}
	}
	publisher.Close() // ends the broadcast: subscribers get fipc.ErrDisconnected after the ticks
}
```

```go
// subscriber.go
package main

import (
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/fastipc/fastipc/bindings/go/fipc"
)

func main() {
	// gated: misses nothing (the publisher waits for it)
	subscriber, err := fipc.JoinBroadcast("my_broadcast", fipc.Gated, 5*time.Second)
	if err != nil {
		log.Fatal(err)
	}
	defer subscriber.Close()
	for {
		tick, err := subscriber.Receive(fipc.Forever)
		if errors.Is(err, fipc.ErrDisconnected) {
			return // the publisher closed
		} else if err != nil {
			log.Fatal(err)
		}
		fmt.Println(string(tick)) // tick 1, tick 2, tick 3
	}
}
```

Run each in its own terminal (`go run ./examples/publisher`, then `go run ./examples/subscriber`); the subscribers of
the other languages work with this publisher too, and this subscriber with their publishers. The publisher waits for
its subscriber before it sends, so the subscriber gets every tick; closing the publisher ends the broadcast, and the
subscriber gets `ErrDisconnected` after the last message.

- **Gated** (`fipc.Gated`) misses nothing: the publisher's send waits for room the subscriber still holds. One that
  holds the publisher back for the broadcast's `maxStall` is dropped: its receives return `fipc.ErrUnsubscribed`, for
  good. `fipc.Forever` never drops one, and `fipc.NoWait` refuses gated subscribers.
- **Lossy** (`fipc.Lossy`) never holds the publisher back. One that falls a lap behind loses messages: a receive returns
  an `ErrOverrun` whose `Lost()` says how many, and the next receive goes on from a recent message.
- **Copies only:** subscribers receive with `Receive` and `ReceiveInto`; `ReceiveAcquire` returns `ErrInvalid` on them.
  The publisher may drop a gated subscriber at any moment, which makes a message held in place inaccessible, and
  reading its slice then would crash the program. The publisher may publish in place (`SendAcquire`, `SendCommit`).

```go
msg, err := subscriber.Receive(fipc.Forever)
var e *fipc.Error
switch {
case err == nil:
	handle(msg)
case errors.As(err, &e) && e.Lost() > 0: // ErrOverrun: a lossy subscriber fell a lap behind
	log.Printf("%d messages lost", e.Lost()) // the next receive goes on from a recent message
default:
	return err // fipc.ErrDisconnected: the broadcast ended
}
```

`publisher.WaitForMembers(atLeast, timeout)` returns the number of subscribers joined, once at least `atLeast` are;
with `ErrTimeout` or `ErrCancelled` (a cancel, or the publisher's `Close` on another goroutine) the count it returns
holds too. It runs beside the publisher's sends and other counts, on any goroutine.

```go
n, err := publisher.WaitForMembers(3, 5*time.Second) // waits up to 5 s for 3 subscribers
if errors.Is(err, fipc.ErrTimeout) {
	log.Printf("only %d of 3 subscribers joined", n)
} else if err != nil {
	return err
}
```

### Work queues

A work queue takes messages from up to 63 producers and hands each to one of up to 64 consumers, in any processes of
the user. Each producer has a ring of its own; a consumer takes a message of any producer: the queue promises no order.
Messages are of one piece (up to a quarter of the capacity, the connections' `MaxPiece()`), and wait in the queue until
a consumer takes them. `fipc.CreateQueue(name, capacity, maxProducers, maxConsumers)` returns the owner's `*fipc.Queue`;
everyone joins with `fipc.JoinQueue(name, role, timeout)`, the owner's own producers and consumers too.

```go
// producer.go
package main

import (
	"fmt"
	"log"
	"time"

	"github.com/fastipc/fastipc/bindings/go/fipc"
)

func main() {
	// 4 KiB of messages per producer, 1 producer and 1 consumer: this process owns the queue and is its producer
	queue, err := fipc.CreateQueue("my_queue", 4096, 1, 1)
	if err != nil {
		log.Fatal(err)
	}
	producer, err := fipc.JoinQueue("my_queue", fipc.Producer, 5*time.Second)
	if err != nil {
		log.Fatal(err)
	}
	for i := 1; i <= 3; i++ { // each job waits in the queue until a consumer takes it
		if err := producer.Send(fmt.Appendf(nil, "job %d", i), fipc.Forever); err != nil {
			log.Fatal(err)
		}
	}
	if _, err := queue.WaitForMembers(fipc.Consumer, 1, fipc.Forever); err != nil { // waits for the consumer to join
		log.Fatal(err)
	}
	producer.Close()
	queue.Close() // ends the queue: the consumer gets fipc.ErrDisconnected after the jobs
}
```

```go
// consumer.go
package main

import (
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/fastipc/fastipc/bindings/go/fipc"
)

func main() {
	consumer, err := fipc.JoinQueue("my_queue", fipc.Consumer, 5*time.Second)
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()
	for {
		job, err := consumer.Receive(fipc.Forever)
		if errors.Is(err, fipc.ErrDisconnected) {
			return // the queue ended
		} else if err != nil {
			log.Fatal(err)
		}
		fmt.Println(string(job)) // job 1, job 2, job 3
	}
}
```

Run the producer, then the consumer (`go run ./examples/producer`, then `go run ./examples/consumer`), or the other way
round: `JoinQueue` waits up to its timeout for the queue's owner. The consumer prints `job 1` to `job 3`, in no promised
order; the producers and consumers of the other languages work with these too. `capacity` is each producer's ring, a
power of two from 4 KiB to 2 GiB; `maxProducers` is 1 to 63 and `maxConsumers` 1 to 64.

- **Joining:** producers and consumers join with `fipc.JoinQueue(name, fipc.Producer, timeout)` or
  `fipc.JoinQueue(name, fipc.Consumer, timeout)`. The owner's process admits each one by itself, on a thread of the
  library's: no goroutine waits for them, and an owner and its members may share a goroutine. A joiner whose role has
  no free place waits, within its timeout, for one. A broadcast's roles are `ErrInvalid` on a queue, and a queue's on
  a broadcast.
- **Counting:** `queue.WaitForMembers(role, atLeast, timeout)` returns the number of members of `role` joined
  (`fipc.Producer`, `fipc.Consumer`, or `fipc.AnyQueueRole` for both), once at least `atLeast` are; with `ErrTimeout`,
  `ErrCancelled` (`queue.Cancel()`, or `queue.Close()` on another goroutine) or `ErrDisconnected` (the queue has
  ended) the count it returns holds too.
- **Sending and receiving:** a producer sends (`Send`, or `SendAcquire` and `SendCommit`), waiting for room in its
  ring while consumers haven't taken the messages in it; a consumer receives copied (`Receive`, `ReceiveInto`) or in
  place (`ReceiveAcquire`, until `ReceiveRelease`). The other role's calls return `ErrInvalid`. A `ReceiveInto` whose
  buffer is too short returns `ErrTooLarge` and leaves the message, which another consumer may take first.
- **The end:** `queue.Close()` ends the queue and frees the name, and so does the end of the owner's process:
  producers' sends then return `fipc.ErrDisconnected`, and consumers receive every message still in the queue, then
  `fipc.ErrDisconnected`. A producer's or a consumer's `Close` never ends the queue.
- **The garbage collector never closes a queue:** a queue lives until `Close` or the end of its process; dropping the
  `*fipc.Queue` doesn't end it. Its members, in this process and in others, use it without the `Queue`, so a queue
  that a collection ended would fail them at a random moment. A forgotten `Close` keeps the name until the process
  ends: a second `CreateQueue` of it returns `ErrAddrInUse`.

**Delivery.** Each message goes to one consumer. It goes again, to another consumer, only if the process of the consumer
that took it ends before acknowledging it, and when a consumer acknowledges is its own choice, not the queue's. A
message received in place is acknowledged at its release (or the consumer's next receive, or its close).

| How the consumer receives | When it acknowledges | If its process ends | What the application gets |
|---|---|---|---|
| `Receive` or `ReceiveInto` (copying) | inside the call, before it returns | inside the call: the message goes to another consumer, and this one never saw it; after the call returned: it doesn't go again | **at most once**: no duplicates |
| `ReceiveAcquire`, then `ReceiveRelease` at once, then the work | at the release, before the work | as above | **at most once**: no duplicates |
| `ReceiveAcquire`, the work, then `ReceiveRelease` | at the release, after the work | before the release: the message goes to another consumer | **at least once**: a duplicate is possible, so the work must be idempotent |

- **Choose by the work, per consumer.** Work that may safely run twice (rendering, a computation, re-indexing,
  converting a file) acknowledges after it; work whose effects must never repeat (a payment, an email, a motor command)
  acknowledges before it.
- **Not covered:** a consumer that hangs while its process runs (there are no timeouts, by design); the owner's process,
  with which the queue ends; a reboot. A consumer holds at most one unacknowledged message at a time, the one received
  in place.
- **A message that goes again comes in no set order,** after messages its producer sent later: the queue promises no
  order anyway.
- **A message held in place holds back its producer's room** until its release: a consumer that acknowledges after a
  long job keeps that message's room, and so the room of the producer's messages after it, until then.
- **A message that crashes every consumer that takes it,** acknowledged after the work, goes to each consumer in turn,
  with no limit: acknowledge before the work where that can happen.

The queue's contract is [`include/fipc.h`](https://github.com/fastipc/fastipc/blob/main/include/fipc.h)'s section on
work queues.

### Services

A service takes requests from up to 64 clients and hands each to one of up to 64 servers, in any processes of the
user; the server that took a request replies to it, and the reply goes back to the client that sent it. A server waits
for the requests of every client at once (one `Receive`), and the servers of a pool share the load: each takes the next
request of any client. Each client has a ring of its own, which holds its requests and the room for their replies, so
a server never waits for a client. Requests and replies are of one piece, 1 to `maxMessage` bytes, the owner's choice
(the connections' `MaxPiece()`).

```go
// service_server.go
package main

import (
	"bytes"
	"log"
	"time"

	"github.com/fastipc/fastipc/bindings/go/fipc"
)

func main() {
	// 4 KiB per client, requests and replies of up to 256 bytes, 1 client and 1 server: this process owns the service
	// and is its server
	service, err := fipc.CreateService("my_service", 4096, 256, 1, 1)
	if err != nil {
		log.Fatal(err)
	}
	server, err := fipc.JoinService("my_service", fipc.Server, 5*time.Second) // this process admits it, as any member
	if err != nil {
		log.Fatal(err)
	}
	for range 3 { // three requests, then the end
		request, err := server.Receive(fipc.Forever) // the next request of any client
		if err != nil {
			log.Fatal(err)
		}
		// the reply goes to its client, and never waits
		if err := server.Send(bytes.ToUpper(request), fipc.Forever); err != nil {
			log.Fatal(err)
		}
	}
	server.Close()
	service.Close() // ends the service: the client still receives the replies sent
}
```

```go
// service_client.go
package main

import (
	"fmt"
	"log"
	"time"

	"github.com/fastipc/fastipc/bindings/go/fipc"
)

func main() {
	client, err := fipc.JoinService("my_service", fipc.Client, 5*time.Second) // waits up to 5 s for the service
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	for i := 1; i <= 3; i++ {
		// sends the request and returns its reply; fipc.ErrAbandoned if the server ended before it replied
		reply, err := client.Call(fmt.Appendf(nil, "ping %d", i), fipc.Forever)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(string(reply)) // PING 1, PING 2, PING 3
	}
}
```

Run the server, then the client (`go run ./examples/service_server`, then `go run ./examples/service_client`), or the
other way round: `JoinService` waits up to its timeout for the service. The client prints `PING 1` to `PING 3`; the
service servers and clients of the other languages work with these too. `fipc.CreateService(name, capacity, maxMessage,
maxClients, maxServers)` returns the owner's `*fipc.Service`. `capacity` is each client's ring, a power of two from 4
KiB to 2 GiB; `maxMessage` is 1 to a quarter of it; `maxClients` and `maxServers` are 1 to 64.

- **Joining:** clients and servers join with `fipc.JoinService(name, fipc.Client, timeout)` or
  `fipc.JoinService(name, fipc.Server, timeout)`, the owner's members too. The owner's process admits each one by
  itself, on a thread of the library's: no goroutine waits for them, and an owner and its members may share a
  goroutine. Clients wait while no server is joined, and servers while no client is. The other kinds' roles are
  `ErrInvalid` on a service, and a service's on them.
- **The owner's service:** `service.WaitForMembers(role, atLeast, timeout)` returns the number of members of `role`
  joined (`fipc.Client`, `fipc.Server`, or `fipc.AnyServiceRole` for both), once at least `atLeast` are; with
  `ErrTimeout`, `ErrCancelled` (`service.Cancel()`, or `service.Close()` on another goroutine) or `ErrDisconnected`
  (the service has ended) the count it returns holds too.
- **A client:** `Send(request)` (or `SendAcquire` and `SendCommit`) sends a request, and a receive (`Receive`,
  `ReceiveInto`, or in place `ReceiveAcquire` until `ReceiveRelease`) returns the reply to its oldest request whose
  reply it hasn't received yet: the n-th reply answers the n-th request, whichever server replies first. A client may
  send several requests before it receives (as many as its ring has room for: each holds its length rounded up to 64
  bytes, plus `maxMessage` rounded up to 64, until its reply has been received), and the servers work on them at once.
  A receive that times out or is cancelled leaves its reply to come: the next receive returns it.
- **`client.Call(request, timeout)`** sends a request and returns its reply, a new slice, waiting up to `timeout` in
  all. A call checks its request first: `ErrInvalid` for an empty one, `ErrTooLarge` over `MaxPiece()`, at once, even
  while a reply is owed. Then a call on a cancelled connection returns `ErrCancelled` at once, whatever its timeout, and
  sends nothing: it could never receive the reply. The connection keeps count of the replies still owed: a call whose
  receive timed out or was cancelled leaves its reply owed, and the next call first receives and drops that reply (at
  most one is owed, since every call drops it first; an `ErrAbandoned` in its place counts too), within its own timeout
  and before it sends, so a call never returns the reply to another request. A call that had to drop an owed reply or
  wait for its turn first sends its request only if its timeout has time left: once a positive timeout has run out, it
  returns `ErrTimeout` without sending, so no server sees that request; with `fipc.NoWait` no step waits, and it sends
  if there is room. After a call that gave up, a plain `Receive` returns that call's late reply (or `ErrAbandoned`)
  first: replies come in request order. A call returns `ErrInvalid` while a request sent with `Send` (or `SendCommit`)
  still has its reply to be received: that reply is yours, so receive it first; on any connection other than a service's
  client it returns `ErrInvalid` too.
- **Calls from several goroutines run one at a time,** each returning its own request's reply. A call waits for its
  turn within its own timeout (`ErrTimeout` if another call holds the turn that long), and `client.Cancel()` or
  `client.Close()` ends the wait with `ErrCancelled`. While a call runs it holds both directions of the connection:
  the client's sends and receives on other goroutines wait for it to return, so that none takes the call's reply (and
  a call waits for one of theirs already in progress, which can outlast the call's timeout).
- **A server:** `Receive` (or `ReceiveInto`, or `ReceiveAcquire`) takes the next request of any client (each client's
  in the order it sent them), and `Send(reply)` (or `SendAcquire` and `SendCommit`) replies to it and never waits. A
  server holds one request at a time: its receives return `ErrInvalid` until it replies, and a reply with no request
  held returns `ErrInvalid`. A request received in place stays valid until the reply's `Send` or `SendCommit`, so the
  reply may be written from it; `ReceiveRelease` does nothing on a server. A server may receive on one goroutine and
  reply on another. A reply to a client that has left is dropped.
- **`fipc.ErrAbandoned`:** a client's receive (or call) returns `ErrAbandoned` when the server that took the request
  ended before it replied: it closed its connection, or its process ended. The request counts as answered, and the
  next receive returns the next reply. An abandoned request may or may not have been carried out; the service never
  sends it again, and the client decides whether to.

```go
reply, err := client.Call(request, 5*time.Second)
if errors.Is(err, fipc.ErrAbandoned) { // its server ended before it replied: send it again, if it may run twice
	reply, err = client.Call(request, 5*time.Second)
}
if err != nil {
	return nil, err // fipc.ErrDisconnected: the service ended
}
```

A pool of servers can be goroutines of one process, each with a connection of its own:

```go
for range 4 { // a pool of four servers on goroutines: each takes the next request of any client
	go func() {
		server, err := fipc.JoinService(name, fipc.Server, 5*time.Second)
		if err != nil {
			log.Fatal(err)
		}
		defer server.Close()
		for {
			request, err := server.Receive(fipc.Forever)
			if err != nil {
				return // fipc.ErrDisconnected: the service ended
			}
			if err := server.Send(work(request), fipc.Forever); err != nil {
				return
			}
		}
	}()
}
```

A server may reply in place, writing the reply from the request it holds in place:

```go
request, err := server.ReceiveAcquire(fipc.Forever) // the request in place, valid until the reply
if err != nil {
	return err
}
reply, err := server.SendAcquire(len(request), fipc.NoWait) // a reply never waits
if err != nil {
	return err
}
copy(reply, bytes.ToUpper(request))    // written from the request in place
return server.SendCommit(len(request)) // the reply; request is no longer valid
```

- **The end:** `service.Close()` ends the service and frees its name, and so does the end of the owner's process:
  clients' sends and servers' receives then return `fipc.ErrDisconnected`, a client receives the replies that came
  before the end, then `fipc.ErrDisconnected`, and a server's reply returns `fipc.ErrDisconnected`. A client's or a
  server's `Close` only leaves the service: a client's requests that no server took are dropped, and a server's close
  abandons the request it holds.
- **The garbage collector never closes a service:** a service lives until `Close` or the end of its process; dropping
  the `*fipc.Service` doesn't end it, as for a queue. A forgotten `Close` keeps the name until the process ends.
- **Not covered:** a server that hangs while its process runs (there are no timeouts, by design: its client waits,
  within its own timeouts); the owner's process, with which the service ends.

The service's contract is [`include/fipc.h`](https://github.com/fastipc/fastipc/blob/main/include/fipc.h)'s section on
services.

### Goroutines and cancel

Every method of a `Listener`, `Conn`, `Queue` or `Service` is safe to call from any goroutine, as with a `net.Conn`.
Each direction of a connection has a mutex of its own: concurrent sends run one at a time, each message whole, and so
do concurrent receives, while one goroutine sends and another receives at once; a service client's calls run one at a
time, each waiting for its turn within its own timeout. A call that waits holds its goroutine's thread in the
library, as a cgo call does.

To stop a goroutine that waits, call `conn.Cancel()` (or `listener.Cancel()`, `queue.Cancel()`, `service.Cancel()`)
from another one: every call that waits, then or later, returns `fipc.ErrCancelled`. With a context, `context.AfterFunc`
cancels the connection when the context is done:

```go
done := make(chan struct{})
go func() {
	defer close(done)
	// until fipc.ErrCancelled (or fipc.ErrDisconnected)
	for {
		msg, err := conn.Receive(fipc.Forever)
		if err != nil {
			return
		}
		handle(msg)
	}
}()
// ...
conn.Cancel() // wakes the reader: ErrCancelled
<-done        // then close the connection, or go on with calls that needn't wait
```

```go
stop := context.AfterFunc(ctx, conn.Cancel) // cancels the connection when ctx is done
defer stop()
```

`conn.Close()` ends the connection: the peer gets `ErrDisconnected` once it has received the messages this side
completed. It may run while other goroutines are inside calls: it cancels the connection (the calls that wait return
`ErrCancelled`), waits for them to return, then closes; later calls, and a second `Close`, return `ErrClosed`.
`listener.Close()` stops listening and leaves the connections it accepted open. A listener or connection that becomes
unreachable without `Close` is closed by the garbage collector, some time later; close it yourself to end it at once (a
listener accepts its next client only once its last connection is closed). A queue or a service never is: close it.
There is no heartbeat and no timeout: a paused or hung peer is not reported, a crashed or killed one as soon as its
process has ended.

### Games and frame loops

A frame loop shouldn't block on the connection. Either poll it once a frame with `fipc.NoWait`, or give the receiving
to a goroutine that forwards each message through a channel the frame loop drains:

```go
messages := make(chan []byte, 64)
go func() {
	defer close(messages)
	for {
		msg, err := conn.Receive(fipc.Forever)
		if err != nil {
			return // ErrDisconnected, or ErrCancelled once the game closes the connection
		}
		messages <- msg
	}
}()
// Each frame: whatever has arrived, without waiting
for len(messages) > 0 {
	handle(<-messages)
}
```

The example `game_loop` polls an echo server once a frame: `go run ./examples/echo_server`, then `go run
./examples/game_loop`.

### The native library

The binding loads the library on the first call that needs it (a create, connect or join, or `fipc.LibraryPath()`, which
says which file it loaded), looking in this order:

1. the folder the environment variable `FASTIPC_LIB_DIR` names (a library you built, or keep with your program; when
   it is set, nowhere else);
2. the folder of the running executable;
3. the copy the module carries, `fipc/native/<platform>/` in the module (`win-x64`, `linux-x64`, `linux-arm64`,
   `osx-arm64`), found from the path of the package's source as the build recorded it: this works for `go run`, `go
   test` and programs built on your machine, not for a program built with `-trimpath` or run on another machine;
4. in a checkout of the FastIPC repository, its `zig-out` build;
5. the system's own search (`LD_LIBRARY_PATH` and the loader's folders, `DYLD_LIBRARY_PATH`, Windows' DLL search
   order).

The library is never unloaded (on Linux and macOS it mustn't be:
[platform support](https://github.com/fastipc/fastipc/blob/main/docs/platform-support.md)).

### Shipping your program

Put the library next to the executable: `fastipc.dll` on Windows, `libfastipc.so` on Linux, `libfastipc.dylib` on
macOS. The binding loads it by its path, so no rpath or install step is needed. Take it from the module's copy
(`$(go env GOMODCACHE)/github.com/fastipc/fastipc/bindings/go@<version>/fipc/native/<platform>/`) or from the
[release's archives](https://github.com/fastipc/fastipc/releases). Elsewhere, set `FASTIPC_LIB_DIR` to its folder.
A program for another platform cross-compiles as any pure Go program (`GOOS=linux GOARCH=arm64 go build`), with that
platform's library shipped next to it.

### Performance

The calls that take your buffer (`Send`, `ReceiveInto`, the zero-copy calls) allocate nothing; `Receive` allocates the
slice it returns. Each call adds an uncontended mutex and purego's call into the library to the C call, which costs
more than copying a short message. Measured between two processes (Go 1.27, `CGO_ENABLED=0`, medians of 5 runs on an
Intel Core i9-13900H laptop), copied 16-byte messages run at about 10.5 million per second on Windows and 9 million on
Linux, and 64 KiB messages at 420,000 to 450,000 per second, as fast as the other languages. Zero-copy is slower
than copying for short messages in Go, 5.5 to 7.4 million per second at 16 B: it takes two calls into the library per
message on each side where a copy takes one. Use it for long messages you would otherwise copy again. The numbers,
and the other languages', are in
[`docs/perf/bindings-baseline.md`](https://github.com/fastipc/fastipc/blob/main/docs/perf/bindings-baseline.md).

## Building from source

In a checkout of the repository, with the library built (`python devtool.py build`):

```bash
cd bindings/go
go test ./...            # the API, peers in other processes, Python peers (the repository's venv), the examples
                         #   in pairs, every Go block of this README
go test -race ./...      # where cgo is available: the race detector needs it (the binding itself doesn't)
go vet ./... && gofmt -l .
go run ./examples/server # then, in another terminal: go run ./examples/client
```

## Documentation

- [The website](https://fastipc.github.io/fastipc/go.html): this binding's guide, with examples of every call.
- [pkg.go.dev](https://pkg.go.dev/github.com/fastipc/fastipc/bindings/go/fipc): the API reference.
- [`include/fipc.h`](https://github.com/fastipc/fastipc/blob/main/include/fipc.h): the C API under this binding,
  and every call's contract (results, timeouts, threads, messages in pieces, the peer's end).
- [Platform support](https://github.com/fastipc/fastipc/blob/main/docs/platform-support.md) and the
  [protocol](https://github.com/fastipc/fastipc/blob/main/docs/protocol.md).
- [Changelog](https://github.com/fastipc/fastipc/blob/main/CHANGELOG.md),
  [issues](https://github.com/fastipc/fastipc/issues).

## License

MIT. Copyright (c) 2025-2026 Hayden Donnelly. See [LICENSE](https://github.com/fastipc/fastipc/blob/main/LICENSE).
