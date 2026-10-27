// Package fipc sends messages between processes on one machine, through shared memory: the Go binding of
// FastIPC (https://github.com/fastipc/fastipc), a small library written in Zig.
//
// A stream is the basic kind: its server creates a name ([CreateStream]) and accepts one client at a time; a client
// connects to the name ([ConnectStream]). Each [Conn] has a shared-memory segment with one ring per direction, and
// reports the peer's end as soon as its process exits or closes the connection. Three more kinds share one segment
// among many processes: a broadcast, a work queue and a service. The peer can be written in any language with a
// binding: a Go process talks to a Zig, C, C++, Python, C#, Java, Rust, Lua or JavaScript process exactly as it talks
// to another Go process.
//
//	// server.go
//	listener, err := fipc.CreateStream("my_channel", 1<<20) // rings of 1 MiB each way
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer listener.Close()
//	conn, err := listener.Accept(fipc.Forever) // waits for a client
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer conn.Close()
//	message, err := conn.Receive(fipc.Forever)
//	if err != nil {
//		log.Fatal(err)
//	}
//	if err := conn.Send(bytes.ToUpper(message), fipc.Forever); err != nil {
//		log.Fatal(err)
//	}
//
//	// client.go
//	conn, err := fipc.ConnectStream("my_channel", 5*time.Second) // waits up to 5 s for the server
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer conn.Close()
//	if err := conn.Send([]byte("ping"), fipc.Forever); err != nil {
//		log.Fatal(err)
//	}
//	reply, err := conn.Receive(fipc.Forever)
//	if err != nil {
//		log.Fatal(err)
//	}
//	fmt.Println(string(reply)) // PING
//
// Run each in a process of its own (in the repository, they are the programs examples/server and examples/client).
// The client sends the message "ping", and the server answers with it in upper case: the exchange every example of
// the repository makes, so a client or a server in any other language works against these.
//
// # Messages
//
//   - Copied messages: [Conn.Send] a []byte (at least 1 byte, any size: a message longer than one piece of the ring
//     travels in pieces and arrives whole); [Conn.Receive] returns it in a new slice, [Conn.ReceiveInto] fills your
//     buffer and returns the length.
//   - Zero-copy (messages of one piece, up to [Conn.MaxPiece] bytes): [Conn.SendAcquire] returns room in the ring to
//     write the message into, then [Conn.SendCommit] sends it; [Conn.ReceiveAcquire] returns the message where it
//     lies in the ring, and [Conn.ReceiveRelease] frees its room. These slices point into shared memory: each method
//     says how long its slice stays valid, and using one after that is undefined (it may crash the program).
//   - Batches: [Conn.SendMany] sends several messages in one call, published together with at most one wake of the
//     receivers; [Conn.ReceiveMany] takes every message already there in one call, packed into your buffer. Batch
//     both ends: a receiver that takes one message per call falls behind a batching sender.
//
// Messages arrive in the order they were sent. The calls that take your buffer (Send, ReceiveInto, the zero-copy
// calls) allocate nothing.
//
// # Broadcasts
//
// A broadcast sends every message of one publisher to each of its subscribers, up to 1024, in any processes of the
// user; each message is stored once, in memory every subscriber maps. [CreateBroadcast] returns the publisher, a [Conn]
// whose sends go to every subscriber and whose Close ends the broadcast. [JoinBroadcast] returns a subscriber's Conn,
// which receives copies (Receive, ReceiveInto) until [ErrDisconnected] once the publisher is closed. The publisher's
// process admits the joiners by itself: no goroutine waits for them. [Conn.WaitForMembers] counts the joined
// subscribers and waits for a number of them: before the first send, for those it must reach.
//
// A [Gated] subscriber misses nothing: the publisher's send waits for room it still holds, and one that holds it back
// for the broadcast's maxStall is dropped ([ErrUnsubscribed]). A [Lossy] one never holds the publisher back: one that
// falls a lap behind gets [ErrOverrun], an [*Error] whose Lost says how many messages it lost, and goes on from a
// recent message.
//
// # Work queues
//
// A work queue takes messages from up to 63 producers and hands each to one of up to 64 consumers, in any processes of
// the user. Each producer has a ring of its own; a consumer takes a message of any producer: the queue promises no
// order. Messages are of one piece, up to [Conn.MaxPiece] bytes (a quarter of the capacity), and wait in the queue
// until a consumer takes them. [CreateQueue] returns the owner's [Queue]. [JoinQueue] with [Producer] or [Consumer]
// returns a member's Conn, the owner's own members too: a producer sends (Send, SendAcquire/SendCommit), and a
// consumer receives, copied (Receive, ReceiveInto) or in place (ReceiveAcquire). The owner's process admits the
// joiners by itself, and [Queue.WaitForMembers] counts them. A message goes again, to another consumer, if the consumer
// that took it ends before acknowledging it: a copying receive acknowledges before it returns (at most once), one in
// place at its ReceiveRelease (released after the work: at least once, so the work must tolerate a duplicate).
//
// The queue ends at [Queue.Close] or the end of the owner's process. From then on producers' sends return
// [ErrDisconnected], and consumers receive every message still in the queue, then ErrDisconnected. A producer's or a
// consumer's Close leaves the queue, which goes on. A queue lives until Close or the end of its process; dropping the
// Queue doesn't end it: the garbage collector never closes a queue.
//
// # Services
//
// A service takes requests from up to 64 clients and hands each to one of up to 64 servers, in any processes of the
// user; the server that took a request replies to it, and the reply goes back to the client that sent it. A server
// waits for the requests of every client at once (one Receive), and the servers share the load: each takes the next
// request of any client. Each client has a ring of its own, which holds its requests and the room for their replies, so
// a server never waits for a client. Requests and replies are of one piece, up to [Conn.MaxPiece] bytes (maxMessage,
// the owner's choice). [CreateService] returns the owner's [Service]. [JoinService] with [Client] or [Server] returns a
// member's Conn, the owner's own members too; the owner's process admits the joiners by itself, and
// [Service.WaitForMembers] counts them.
//
// A client's [Conn.Call] sends a request and returns its reply; its sends and receives do the same in two steps
// (several requests may be on their way at once), and a receive returns the reply to its oldest request whose reply it
// hasn't received yet, so the n-th reply answers the n-th request. A server's receive takes the next request of any
// client and holds it until its next send, the reply, which never waits. A request is answered once: by its reply, or
// with [ErrAbandoned] if the server that took it ended before it replied (it may or may not have been carried out, and
// the service never sends it again). A Call that gave up leaves its reply owed: the next Call drops it first, and a
// plain receive returns it first. A Call on a cancelled connection returns [ErrCancelled] at once and sends nothing.
//
// The service ends at [Service.Close] or the end of the owner's process. From then on clients' sends and servers'
// receives return [ErrDisconnected], a client receives the replies that came before the end, then ErrDisconnected, and
// a server's reply returns ErrDisconnected. A member's Close leaves the service, which goes on. As a queue, a service
// lives until Close or the end of its process: the garbage collector never closes a service.
//
// # Timeouts and errors
//
// Every call that can wait takes its timeout last, as a [time.Duration]: [NoWait] doesn't wait, [Forever] (or any
// negative duration) waits as long as it takes. Waits are counted in whole milliseconds, rounded up. A call that
// doesn't succeed returns one of the errors [ErrTimeout] (the timeout ran out; the call changed nothing),
// [ErrDisconnected] (the peer's end), [ErrCancelled], [ErrTooLarge] (an [*Error] whose Len says how much room the
// message needs), [ErrInvalid], [ErrNoMemory], [ErrAddrInUse], on a broadcast [ErrOverrun] and
// [ErrUnsubscribed], on a service's client [ErrAbandoned], or the binding's own [ErrClosed]; test them with
// [errors.Is].
//
// A copying call's timeout bounds its wait for the first piece; once a piece has moved, the call goes on until the
// whole message has, and only a cancel or the peer's end stops it (the message is then dropped whole: the receiver
// never sees part of one).
//
// # Goroutines
//
// Every method of a [Listener], [Conn], [Queue] or [Service] is safe to call from any goroutine, as with a net.Conn.
// Each direction of a connection has a mutex of its own: concurrent sends run one at a time, each whole, and so do
// concurrent receives, while a send and a receive run at once. A client and its server in one process run on
// different goroutines: [ConnectStream] waits for the server's [Listener.Accept]. A broadcast's, a queue's or a
// service's members may join on the owner's goroutine: its process admits them on a thread of the library's. A service
// client's Calls from several goroutines run one at a time, each waiting for its turn within its own timeout. A call
// that waits holds its goroutine's thread in the library, as a cgo call does.
//
// To stop a goroutine that waits, call [Conn.Cancel], [Listener.Cancel], [Queue.Cancel] or [Service.Cancel] from
// another goroutine: every call that waits, then or later, returns [ErrCancelled]. With a context,
// context.AfterFunc(ctx, conn.Cancel) cancels the connection when the context is done. [Conn.Close] may run while
// other goroutines are inside calls: it cancels the connection, waits for them to return, then closes it.
//
// # The peer's end
//
// [ErrDisconnected]: the peer closed the connection, or its process ended, for any reason (crashed or killed
// included). There is no heartbeat and no timeout: a paused or hung peer is not reported. It is final; to talk again,
// accept or connect a new connection. A [Listener] or [Conn] that becomes unreachable without Close is closed by the
// garbage collector, some time later; close it yourself to end it at once. A [Queue] or a [Service] never is: close
// it.
//
// # The native library
//
// The package needs fastipc.dll, libfastipc.so or libfastipc.dylib (Windows x64, Linux x64 and ARM64 with glibc 2.34
// or newer, macOS 14.4 or newer on Apple Silicon; on x86-64 the CPU must support x86-64-v3, AVX2). It loads it at
// run time, without cgo, through github.com/ebitengine/purego, the module's one dependency: CGO_ENABLED=0 builds work,
// cross-compiling works, and no C compiler is needed. [LibraryPath] says where it looks: a program run on another
// machine finds the library next to its executable. The library is never unloaded.
package fipc
