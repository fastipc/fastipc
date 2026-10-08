# fipc for Go

[![Go Reference](https://pkg.go.dev/badge/github.com/fastipc/fastipc/bindings/go/fipc.svg)](https://pkg.go.dev/github.com/fastipc/fastipc/bindings/go/fipc)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/fastipc/fastipc/blob/main/LICENSE)

Messages and RPC between two processes on one machine, through shared memory: the Go binding of
[FastIPC](https://github.com/fastipc/fastipc), a small library written in Zig
([`include/fipc.h`](https://github.com/fastipc/fastipc/blob/main/include/fipc.h)). A server listens on a name and
accepts one client at a time; a client connects to the name. `Connect` waits for the server's `Accept`, so a client and
its server run in different processes or on different goroutines. Each connection has a shared-memory segment with one
ring per direction, and reports the peer's end as soon as its process exits or closes the connection.

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
	listener, err := fipc.Listen("my_channel", 1<<20) // rings of 1 MiB each way
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()
	conn, err := listener.Accept(fipc.Forever) // waits for a client
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	request, err := conn.RPCReceive(fipc.Forever)
	if err != nil {
		log.Fatal(err)
	}
	err = conn.RPCRespond(request.ID, request.Opcode, 0, bytes.ToUpper(request.Payload), fipc.Forever)
	if err != nil {
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
	conn, err := fipc.Connect("my_channel", 5*time.Second) // waits up to 5 s for the server
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.RPCSubmit(1, []byte("ping"), fipc.Forever); err != nil { // opcode 1
		log.Fatal(err)
	}
	reply, err := conn.RPCReceive(fipc.Forever)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(reply.Payload)) // PING
}
```

Run each in its own terminal; in a checkout of the repository they are the module's examples, run in this folder: `go
run ./examples/server` and `go run ./examples/client`. The clients of the other languages in the [repository's
README](https://github.com/fastipc/fastipc#examples) work against this server too, and this client against their
servers.

- **Plain messages:** `conn.Send(msg)` and `conn.Receive()`, which returns the message in a new slice, or
  `conn.ReceiveInto(buf)`, which fills your buffer and returns the length. A message of any size may be sent (at
  least 1 byte); it travels in pieces and arrives whole.
- **Zero-copy** (messages of one piece, up to `conn.MaxPiece()` bytes): `conn.SendAcquire(n)` returns a slice over
  room in the ring: write the message into it, then `conn.SendCommit(n)`. `conn.ReceiveAcquire()` returns a slice over
  the message in the ring (read-only), and `conn.ReceiveRelease()` frees its room. These slices point into shared
  memory: one from `SendAcquire` is valid until `SendCommit`, the next send or `Close`, one from `ReceiveAcquire` until
  `ReceiveRelease`, the next receive or `Close`, and using one after that is undefined (it may crash the program).
  Keep the connection reachable while you use one.
- **RPC:** `conn.RPCSubmit(opcode, payload)` returns the request's id; `conn.RPCRespond(id, opcode, status, payload)`;
  `conn.RPCReceive()` returns an `RPCMessage` (`ID`, `Kind`, `Opcode`, `Status`, `Payload`), and
  `conn.RPCReceiveInto(buf)` an `RPCHeader` with the payload in your buffer (no allocation per message). A payload may
  be empty (`nil`). Use a connection for plain messages or for RPC, not both.

### Errors and timeouts

A call that doesn't succeed returns one of the errors `fipc.ErrTimeout`, `fipc.ErrDisconnected` (the peer's end:
final, so close the connection, and accept or connect a new one), `fipc.ErrCancelled`, `fipc.ErrTooLarge`,
`fipc.ErrInvalid`, `fipc.ErrNoMemory` and `fipc.ErrAddrInUse`, the C API's results, which are `*fipc.Error` values:
test them with `errors.Is`. Their messages are the C names (`FIPC_TIMEOUT: the timeout ran out`), and
`ErrTimeout.Timeout()` is true, as for the timeouts of packages `os` and `net`. A call on a closed listener or
connection returns `fipc.ErrClosed`; one that can't load the library, an error that wraps `fipc.ErrNoLibrary` and says
where it looked.

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

### Goroutines and cancel

Every method of a `Listener` or `Conn` is safe to call from any goroutine, as with a `net.Conn`. Each direction of a
connection has a mutex of its own: concurrent sends run one at a time, each message whole, and so do concurrent
receives, while one goroutine sends and another receives at once. A call that waits holds its goroutine's thread in
the library, as a cgo call does.

To stop a goroutine that waits, call `conn.Cancel()` (or `listener.Cancel()`) from another one: every call that waits,
then or later, returns `fipc.ErrCancelled`. With a context, `context.AfterFunc` cancels the connection when the context
is done:

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
unreachable without `Close` is closed by the garbage collector, some time later; close it yourself to end it at once
(a listener accepts its next client only once its last connection is closed). There is no heartbeat and no timeout: a
paused or hung peer is not reported, a crashed or killed one as soon as its process has ended.

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

The binding loads the library on the first call that needs it (`Listen`, `Connect` or `fipc.LibraryPath()`, which says
which file it loaded), looking in this order:

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

The calls that take your buffer (`Send`, `ReceiveInto`, the zero-copy calls, `RPCSubmit`, `RPCRespond`,
`RPCReceiveInto`) allocate nothing; `Receive` and `RPCReceive` allocate the slice they return. Each call adds an
uncontended mutex and purego's call into the library to the C call, which costs more than copying a short message.
Measured between two processes (Go 1.27, `CGO_ENABLED=0`, medians of 5 runs on an Intel Core i9-13900H laptop),
copied 16-byte messages run at about 11 million per second on Windows and Linux, RPC requests at 10 to 11 million,
and 64 KiB messages at 440,000 to 470,000 per second, as fast as the other languages. Zero-copy is slower than
copying for short messages in Go, 5.8 to 8.2 million per second at 16 B: it takes two calls into the library per message on each
side where a copy takes one. Use it for long messages you would otherwise copy again. The numbers, and the other
languages', are in
[`docs/perf/bindings-baseline.md`](https://github.com/fastipc/fastipc/blob/main/docs/perf/bindings-baseline.md).

## Building from source

In a checkout of the repository, with the library built (`python devtool.py build`):

```bash
cd bindings/go
go test ./...            # the API, peers in other processes, a Python peer (the repository's venv), the examples
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
