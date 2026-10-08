// Package fipc sends messages and RPC between two processes on one machine, through shared memory: the Go binding of
// FastIPC (https://github.com/fastipc/fastipc), a small library written in Zig.
//
// A server listens on a name and accepts one client at a time; a client connects to the name. Each [Conn] has a
// shared-memory segment with one ring per direction, and reports the peer's end as soon as its process exits or
// closes the connection. The peer can be written in any language with a binding: a Go process talks to a Zig, C,
// C++, Python, C#, Java, Rust, Lua or JavaScript process exactly as it talks to another Go process.
//
//	// server.go
//	listener, err := fipc.Listen("my_channel", 1<<20) // rings of 1 MiB each way
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer listener.Close()
//	conn, err := listener.Accept(fipc.Forever) // waits for a client
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer conn.Close()
//	request, err := conn.RPCReceive(fipc.Forever)
//	if err != nil {
//		log.Fatal(err)
//	}
//	err = conn.RPCRespond(request.ID, request.Opcode, 0, bytes.ToUpper(request.Payload), fipc.Forever)
//
//	// client.go
//	conn, err := fipc.Connect("my_channel", 5*time.Second) // waits up to 5 s for the server
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer conn.Close()
//	if _, err := conn.RPCSubmit(1, []byte("ping"), fipc.Forever); err != nil { // opcode 1
//		log.Fatal(err)
//	}
//	reply, err := conn.RPCReceive(fipc.Forever)
//	if err != nil {
//		log.Fatal(err)
//	}
//	fmt.Println(string(reply.Payload)) // PING
//
// Run each in a process of its own (in the repository, they are the programs examples/server and examples/client).
// The client sends an RPC request with opcode 1 and the payload "ping", and the server answers with the payload in
// upper case: the exchange every example of the repository makes, so a client or a server in any other language
// works against these.
//
// # Messages
//
//   - Plain messages: [Conn.Send] a []byte (at least 1 byte, any size: a message longer than one piece of the ring
//     travels in pieces and arrives whole); [Conn.Receive] returns it in a new slice, [Conn.ReceiveInto] fills your
//     buffer and returns the length.
//   - Zero-copy (messages of one piece, up to [Conn.MaxPiece] bytes): [Conn.SendAcquire] returns room in the ring to
//     write the message into, then [Conn.SendCommit] sends it; [Conn.ReceiveAcquire] returns the message where it
//     lies in the ring, and [Conn.ReceiveRelease] frees its room. These slices point into shared memory: each method
//     says how long its slice stays valid, and using one after that is undefined (it may crash the program).
//   - RPC: [Conn.RPCSubmit] sends a request and returns its id, [Conn.RPCRespond] answers one, [Conn.RPCReceive]
//     returns an [RPCMessage] and [Conn.RPCReceiveInto] an [RPCHeader] with the payload in your buffer. Use a
//     connection for plain messages or for RPC, not both.
//
// Messages arrive in the order they were sent. The calls that take your buffer (Send, ReceiveInto, the zero-copy
// calls, RPCSubmit, RPCRespond, RPCReceiveInto) allocate nothing.
//
// # Timeouts and errors
//
// Every call that can wait takes its timeout last, as a [time.Duration]: [NoWait] doesn't wait, [Forever] (or any
// negative duration) waits as long as it takes. Waits are counted in whole milliseconds, rounded up. A call that
// doesn't succeed returns one of the errors [ErrTimeout] (the timeout ran out; the call changed nothing),
// [ErrDisconnected] (the peer's end), [ErrCancelled], [ErrTooLarge] (an [*Error] whose Len says how much room the
// message needs), [ErrInvalid], [ErrNoMemory], [ErrAddrInUse], or the binding's own [ErrClosed]; test them with
// [errors.Is].
//
// A copying call's timeout bounds its wait for the first piece; once a piece has moved, the call goes on until the
// whole message has, and only a cancel or the peer's end stops it (the message is then dropped whole: the receiver
// never sees part of one).
//
// # Goroutines
//
// Every method of a [Listener] or [Conn] is safe to call from any goroutine, as with a net.Conn. Each direction of a
// connection has a mutex of its own: concurrent sends run one at a time, each whole, and so do concurrent receives,
// while a send and a receive run at once. A client and its server in one process run on different goroutines:
// [Connect] waits for the server's [Listener.Accept]. A call that waits holds its goroutine's thread in the library,
// as a cgo call does.
//
// To stop a goroutine that waits, call [Conn.Cancel] or [Listener.Cancel] from another goroutine: every call that
// waits, then or later, returns [ErrCancelled]. With a context, context.AfterFunc(ctx, conn.Cancel) cancels the
// connection when the context is done. [Conn.Close] may run while other goroutines are inside calls: it cancels the
// connection, waits for them to return, then closes it.
//
// # The peer's end
//
// [ErrDisconnected]: the peer closed the connection, or its process ended, for any reason (crashed or killed
// included). There is no heartbeat and no timeout: a paused or hung peer is not reported. It is final; to talk again,
// accept or connect a new connection. A [Listener] or [Conn] that becomes unreachable without Close is closed by the
// garbage collector, some time later; close it yourself to end it at once.
//
// # The native library
//
// The package needs fastipc.dll, libfastipc.so or libfastipc.dylib (Windows x64, Linux x64 and ARM64 with glibc 2.34
// or newer, macOS 14.4 or newer on Apple Silicon; on x86-64 the CPU must support x86-64-v3, AVX2). It loads it at
// run time, without cgo, through github.com/ebitengine/purego, the module's one dependency: CGO_ENABLED=0 builds work,
// cross-compiling works, and no C compiler is needed. [LibraryPath] says where it looks: a program run on another
// machine finds the library next to its executable. The library is never unloaded.
package fipc
