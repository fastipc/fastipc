<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="website/assets/brand/fastipc-logo-dark.svg">
    <img src="website/assets/brand/fastipc-logo-light.svg" alt="FastIPC: high-performance IPC" width="520">
  </picture>
</p>

<p align="center"><b>Website, guides and benchmarks: <a href="https://fastipc.github.io/fastipc/">fastipc.github.io/fastipc</a></b></p>

# FastIPC

[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Version 1.0.0](https://img.shields.io/badge/version-1.0.0-green.svg)](CHANGELOG.md)
[![Zig 0.17.0](https://img.shields.io/badge/zig-0.17.0-f7a41d.svg)](https://ziglang.org/download/)
[![Platforms: Linux x64 and arm64, Windows x64, macOS arm64](https://img.shields.io/badge/platforms-Linux%20x64%20%26%20arm64%20%7C%20Windows%20x64%20%7C%20macOS%20arm64-lightgrey.svg)](docs/platform-support.md)
[![Website](https://img.shields.io/badge/website-fastipc.github.io%2Ffastipc-a64b0c.svg)](https://fastipc.github.io/fastipc/)

**Fast messages and RPC between two processes on one machine, through shared memory.** FastIPC is a small C API
([`include/fipc.h`](include/fipc.h), 18 functions) implemented in Zig as `fastipc.dll` / `libfastipc.so` / `libfastipc.dylib`, with a
header-only C++20 wrapper ([`include/fipc.hpp`](include/fipc.hpp)), bindings for Python, C#, Java, Rust, Lua, JavaScript and
Go, and a native Zig module. They all speak one protocol, so a process in one language talks to a process in another.

Use it when two local processes (an editor and a game, a UI and its engine, a Python front end and a native service)
need to talk at memory speed: tens of millions of small messages per second, and each side learns at once when the
other one exits or crashes.

The scope is deliberately narrow: one server and one client per connection, which keeps the library small and simple.
Pub/sub or fan-out can be built on top, with a connection per subscriber.

## Highlights

- **Shared-memory rings.** Each connection has a segment of its own with one single-producer, single-consumer ring per
  direction. While both sides are busy, a message costs no system call: a waiting side spins briefly, then sleeps on a
  futex (Linux), an `os_sync` address wait (macOS) or an event (Windows), and a sender wakes it only if it sleeps.
- **No third-party dependencies.** The library links only the operating system's own libraries (the C library on
  Linux and macOS, system DLLs on Windows). The bindings call it through their language's built-in foreign-function
  interface (P/Invoke, Java's FFM API, Rust's `extern "C"`, LuaJIT's FFI, Node-API); the exceptions are Python's,
  which uses CFFI, and Go's, which uses [purego](https://github.com/ebitengine/purego) so that it needs no cgo. A build downloads nothing: what it needs at build time, Zig's official translate-c package and the
  Node-API headers, ships in [`vendor/`](vendor/README.md), and none of it ends up in the built library.
- **Messages of any size.** A message longer than the ring travels in pieces and arrives whole, copied straight into
  the caller's buffer; a message of one piece can also be written and read in place (zero-copy). The library
  allocates nothing per message.
- **RPC built in.** Requests and responses with ids and opcodes over a connection.
- **The peer's end, at once.** A local control connection (an abstract `AF_UNIX` socket on Linux, a named pipe on
  Windows, a socket file in the user's private directory on macOS) reports the peer's end as soon as its process
  exits, for any reason, or it closes the connection: no heartbeat, no timeouts, and nothing to clean up after a crash
  (on macOS a crashed listener's two files stay until the user's next listen, which removes them; they never get in
  the way).
- **Trusts nothing in shared memory.** Every index and frame the peer wrote is checked before use; a corrupt peer
  makes a call fail with a result code, never crash or hang (checked by a fuzz harness).
- **Small.** The library is about 3,400 lines of Zig code (5,200 with comments and blank lines), with about twice
  that again in tests.
- **Verified.** Unit tests, a C-API conformance suite run in-process and across processes (peers killed at every
  step of the handshake included), ThreadSanitizer runs, and chaos, soak and fuzz harnesses.

## Performance

Messages per second between two processes (one sends, one receives; 512 KiB rings), ReleaseFast, no CPU pinning;
Linux and Windows built for x86-64-v3 on an Intel Core i9-13900H laptop (Windows 11; Linux = WSL2 Ubuntu 24.04).
Method, machine and run-to-run noise: [`docs/perf/baseline.md`](docs/perf/baseline.md); C, C++ and the Python, C#,
Java, Rust, Lua, JavaScript and Go bindings: [`docs/perf/bindings-baseline.md`](docs/perf/bindings-baseline.md).

| Benchmark | Linux | Windows | macOS\* | Linux VM on the M1\*\* |
|---|---|---|---|---|
| Copy, 16 B | 47.9M | 48.2M | 54.1M | 49.6M |
| Copy, 1 KiB | 21.4M | 16.8M | 27.0M | 26.1M |
| Copy, 64 KiB | 488K (30 GiB/s) | 433K (26 GiB/s) | 653K (40 GiB/s) | 644K (39 GiB/s) |
| Zero-copy, 16 B | 57.5M | 47.9M | 56.2M | 54.7M |
| Zero-copy, 1 KiB | 23.1M | 21.2M | 25.3M | 21.8M |
| RPC, 16 B | 43.9M | 44.0M | 51.4M | 42.6M |
| RPC, 1 KiB | 17.8M | 15.7M | 25.9M | 25.3M |
| Round trip, both sides spinning (p50), fastest size | 237 ns (16 B) | 263 ns (8 B) | 167 ns (16 B) | 208 ns (16 B) |
| Connection setup and teardown | 2.6K/s | 7.4K/s | 3.5K/s | 2.3K/s |

\* macOS: MacBook Air (M1, 8 GB), macOS 26.5.1; different hardware from the Linux/Windows machine.

\*\* Linux (Ubuntu 24.04 arm64) in a Lima VM (Apple Virtualization framework, 4 vCPUs, 4 GB) on that MacBook Air.
Both M1 columns measured 2026-10-07, their 16 B round trip 2026-10-06.

Throughput depends on the receiver's pace as much as on the API: see "Short messages" in the baseline.

**Lowest latency: busy-polling.** A receiver waiting in `fipc_recv` spins for 100-250 µs, then sleeps, and a message
that comes later wakes it in tens of microseconds. For the lowest latency at any message rate, call `fipc_recv` (or
`fipc_rpc_recv`, `fipc_recv_acquire`) with timeout 0 in a loop: a call that finds nothing makes no system call and
reads no clock (about 20 ns), and the polling thread holds one core at 100%. With one 32 B message per millisecond, a
polling receiver gets it 257 ns (Linux), 268 ns (Windows) and 125 ns (macOS) after it was sent at the
median, a sleeping one 55 µs, 42 µs and 26 µs; the p99 on Linux and Windows, 18-71 µs, is the time the
OS takes the core away ([`docs/perf/baseline.md`](docs/perf/baseline.md)).

## Install

| Language | Get it | Docs |
|---|---|---|
| Python 3.9+ | `pip install fipc-python` ([PyPI](https://pypi.org/project/fipc-python/)), then `import fipc` | [`bindings/python`](bindings/python/README.md) |
| C# (.NET Standard 2.1) | `dotnet add package Fipc` ([NuGet](https://www.nuget.org/packages/Fipc)), namespace `FastIpc` | [`bindings/csharp`](bindings/csharp/README.md) |
| Java 22+ | `implementation("io.github.fastipc:fipc:1.0.0")` ([Maven Central](https://central.sonatype.com/artifact/io.github.fastipc/fipc)), module and package `io.github.fastipc` | [`bindings/java`](bindings/java/README.md) |
| Rust 1.85+ | `cargo add fipc` ([crates.io](https://crates.io/crates/fipc)), then `use fipc` | [`bindings/rust`](bindings/rust/README.md) |
| LuaJIT 2.1 | `luarocks install fipc` ([LuaRocks](https://luarocks.org/modules/fastipc/fipc)), then `require("fipc")` | [`bindings/lua`](bindings/lua/README.md) |
| JavaScript (Node.js 22+, Bun, Deno 2) | `npm install fipc` ([npm](https://www.npmjs.com/package/fipc)), then `import { Listener, Connection } from "fipc"` | [`bindings/js`](bindings/js/README.md) |
| Go 1.27+ | `go get github.com/fastipc/fastipc/bindings/go/fipc@latest` ([pkg.go.dev](https://pkg.go.dev/github.com/fastipc/fastipc/bindings/go/fipc)), then `import "github.com/fastipc/fastipc/bindings/go/fipc"` | [`bindings/go`](bindings/go/README.md) |
| C / C++ | The Zig build system (recommended): `zig fetch --save git+https://github.com/fastipc/fastipc`, then link the artifact `fastipc` ([below](#c-and-c)); or, with any compiler, the [GitHub release](https://github.com/fastipc/fastipc/releases/latest)'s archive per OS (headers and library, with `fastipc.lib` on Windows) | [`include/fipc.h`](include/fipc.h), [`include/fipc.hpp`](include/fipc.hpp) |
| Zig 0.17.0 | `zig fetch --save git+https://github.com/fastipc/fastipc`, then import the module `fastipc` ([below](#zig)) | [`src/zig/root.zig`](src/zig/root.zig) |

The Python wheels, the NuGet package, the Maven artifact, the crate `fipc-sys`, the Lua rock, the npm package and the Go module bundle the
native library for Windows x64, Linux x64 and ARM64 (glibc 2.34+) and macOS on Apple Silicon (14.4+).

**macOS:** the library is `libfastipc.dylib`, ad hoc signed and not notarized. Packages installed by pip, NuGet, Maven,
Cargo, LuaRocks, npm or Go need nothing more. The C/C++ archive (`fastipc-<version>-osx-arm64.tar.gz`), when a browser
downloaded it, is quarantined: clear the flag after unpacking, with `xattr -d com.apple.quarantine
lib/libfastipc.dylib`. An app signed with the hardened runtime and library validation signs the dylib it embeds
with its own identity.

### C and C++

The recommended way to build C and C++ against FastIPC is the [Zig build system](https://ziglang.org/learn/build-system/):
no CMake or other build system, and Zig 0.17.0 is a single download. It builds the library for your target and compiles
your C and C++ with its bundled Clang (`zig cc`, `zig c++`). After `zig fetch --save
git+https://github.com/fastipc/fastipc`:

```zig
// build.zig
const fastipc = b.dependency("fipc", .{ .target = target, .optimize = optimize }).artifact("fastipc");
exe.root_module.linkLibrary(fastipc); // the shared library; fipc.h and fipc.hpp on the include path
exe.root_module.addCMacro("FASTIPC_SHARED", "1"); // Windows: the functions are dllimport
b.installArtifact(fastipc); // fastipc.dll next to the programs, libfastipc.so / .dylib in zig-out/lib
```

[`examples/c-cpp`](examples/c-cpp) is such a project, with the C and C++ servers and clients below. With
another compiler, take the headers and the library from the GitHub release's archive for your OS (or from `zig build`
in a checkout: `zig-out/include`, `zig-out/lib`, `zig-out/bin`), put the headers on the include path and link the
library: `libfastipc.so` on Linux (found at run time through an rpath or `LD_LIBRARY_PATH`), `libfastipc.dylib` on
macOS (install name `@rpath/libfastipc.dylib`: an rpath such as `@loader_path/../lib`, or `DYLD_LIBRARY_PATH`),
`fastipc.lib` on Windows, with `fastipc.dll` next to the program and `FASTIPC_SHARED` defined. `fipc.hpp` needs C++20.

## Examples

Each language has a server file and a client file: run the server in one process, then the client in another. A
server listens on a name and accepts one client at a time; a client connects to the name. The examples all make the
same exchange, an RPC request with opcode 1 and the payload `ping`, which the server answers with the payload in
upper case, so any language's client works with any language's server: the repository's interop test, `python
devtool.py test interop`, runs every language's server below against every language's client (10 × 10 pairs, each in two
processes) on Linux, Windows and macOS. Timeouts are milliseconds: 0 doesn't wait, -1 waits for ever (in C++, a
`std::chrono` duration; in Java, a `Duration`; in Rust, an `Option<Duration>`; in Go, a `time.Duration`; in Zig, a `std.Io.Timeout`: `.none`
waits for ever, a `.duration` or a `.deadline` bounds the wait).

### Python

```python
# server.py
from fipc import Listener

# rings of 1 MiB each way
with Listener("my_channel", 1 << 20) as listener:
    with listener.accept() as conn:  # waits for a client
        request = conn.rpc_recv()
        conn.rpc_respond(request.id, request.opcode,
                         data=request.payload.upper())
```

```python
# client.py
from fipc import Conn

with Conn.connect("my_channel", timeout_ms=5000) as conn:
    conn.rpc_submit(1, b"ping")     # opcode 1
    print(conn.rpc_recv().payload)  # b'PING'
```

They are [`examples/python`](examples/python)'s: run `python server.py` there, then `python client.py` in another
terminal. Every call blocks by default (`timeout_ms=FOREVER`) and raises `FipcError` for any result but OK.

### C#

```csharp
// Server/Program.cs
using System.Text;
using FastIpc;

// rings of 1 MiB each way
FipcListener.Listen("my_channel", 1 << 20,
    out FipcListener? listener);
using (listener)
{
    // waits for a client
    listener!.Accept(out FipcConnection? conn, Fipc.Forever);
    using (conn)
    {
        if (conn!.RpcReceive(out FipcRpcHeader request,
                out byte[] payload, Fipc.Forever)
            == FipcResult.Ok)
        {
            string upper = Encoding.UTF8.GetString(payload)
                .ToUpperInvariant();
            conn.RpcRespond(request.Id, request.Opcode, 0,
                Encoding.UTF8.GetBytes(upper), Fipc.Forever);
        }
    }
}
```

```csharp
// Client/Program.cs
using System.Text;
using FastIpc;

if (FipcConnection.Connect("my_channel",
        out FipcConnection? conn, 5000) != FipcResult.Ok)
    return 1;
using (conn)
{
    conn!.RpcSubmit(opcode: 1, "ping"u8, out ulong id,
        Fipc.Forever);
    conn.RpcReceive(out FipcRpcHeader response,
        out byte[] reply, Fipc.Forever);
    Console.WriteLine(Encoding.UTF8.GetString(reply));  // PING
}
return 0;
```

Each is a console project (`dotnet new console`) with the `Fipc` package; [`examples/csharp`](examples/csharp)
has the two, with a reference to the binding's project in place of the package. Every call returns a `FipcResult`; a
timeout or the peer's end is a result, not an exception.

### Java

```java
// Server.java
import io.github.fastipc.Connection;
import io.github.fastipc.Listener;
import io.github.fastipc.RpcMessage;

public class Server {
    public static void main(String[] args) {
        // rings of 1 MiB each way; accept() waits for a client
        try (Listener listener =
                 Listener.listen("my_channel", 1 << 20);
             Connection conn = listener.accept()) {
            RpcMessage request = conn.rpcReceive();
            String text = new String(request.payload());
            conn.rpcRespond(request.id(), request.opcode(), 0,
                            text.toUpperCase().getBytes());
        }
    }
}
```

```java
// Client.java
import io.github.fastipc.Connection;
import java.time.Duration;

public class Client {
    public static void main(String[] args) {
        try (Connection conn = Connection.connect("my_channel",
                 Duration.ofSeconds(5))) {
            conn.rpcSubmit(1, "ping".getBytes());  // opcode 1
            byte[] reply = conn.rpcReceive().payload();
            System.out.println(new String(reply));  // PING
        }
    }
}
```

They are [`examples/java`](examples/java)'s. Run each with the jar on the class path, for example `java
--enable-native-access=ALL-UNNAMED -cp fipc-1.0.0.jar Server.java` (Java 22+). A call that doesn't succeed
throws `FipcException`, whose `result()` says why.

### Rust

```rust
// server.rs
use fipc::Listener;

fn main() -> fipc::Result<()> {
    let mut listener = Listener::listen("my_channel", 1 << 20)?; // rings of 1 MiB each way
    let mut conn = listener.accept(None)?; // waits for a client
    let request = conn.rpc_receive(None)?;
    conn.rpc_respond(request.id, request.opcode, 0, &request.payload.to_ascii_uppercase(), None)?;
    Ok(())
}
```

```rust
// client.rs
use fipc::Connection;
use std::time::Duration;

fn main() -> fipc::Result<()> {
    let mut conn = Connection::connect("my_channel", Duration::from_secs(5))?;
    conn.rpc_submit(1, b"ping", None)?; // opcode 1
    println!("{}", String::from_utf8_lossy(&conn.rpc_receive(None)?.payload)); // PING
    Ok(())
}
```

Each is a program of its own (in a checkout, `cargo run --example server` and `cargo run --example client` in
`bindings/rust`). A call that doesn't succeed returns an `Error` (`Error::Timeout`, `Error::Disconnected`, ...); `None`
as a timeout waits as long as it takes. `cargo run` finds the native library; a program run on its own needs it next
to it ([shipping](bindings/rust/README.md#shipping-your-program)).

### Lua

```lua
-- server.lua
local fipc = require("fipc")

local listener = assert(fipc.listen("my_channel", 1024 * 1024)) -- rings of 1 MiB each way
local conn = assert(listener:accept()) -- waits for a client
local request = assert(conn:rpc_recv())
assert(conn:rpc_respond(request.id, request.opcode, 0, request.payload:upper()))
conn:close()
listener:close()
```

```lua
-- client.lua
local fipc = require("fipc")

local conn = assert(fipc.connect("my_channel", 5000)) -- waits up to 5 s for the server
assert(conn:rpc_submit(1, "ping")) -- opcode 1
print(assert(conn:rpc_recv()).payload) -- PING
conn:close()
```

They are `examples/server.lua` and `examples/client.lua` of [`bindings/lua`](bindings/lua): run `luajit
examples/server.lua` there, then `luajit examples/client.lua` in another terminal. The binding is one module over
LuaJIT's FFI (LuaJIT 2.1; nothing is compiled). A call that fails returns `nil` and the error's name (`"timeout"`,
`"disconnected"`, ...), as Lua's `io` functions do, so `assert()` raises it; a call without a timeout waits as long as
it takes.

### JavaScript

```js
// server.mjs
import { Listener } from "fipc";

const listener = Listener.listen("my_channel", 1 << 20); // rings of 1 MiB each way
const conn = await listener.accept(); // waits for a client; the event loop runs meanwhile
const request = await conn.rpcReceive();
await conn.rpcRespond(request.id, request.opcode, 0, request.payload.toString().toUpperCase());
conn.close();
listener.close();
```

```js
// client.mjs
import { Connection } from "fipc";

const conn = await Connection.connect("my_channel", 5000); // waits up to 5 s for the server
await conn.rpcSubmit(1, "ping"); // opcode 1
console.log((await conn.rpcReceive()).payload.toString()); // PING
conn.close();
```

They are `examples/server.mjs` and `examples/client.mjs` of [`bindings/js`](bindings/js): run `node
examples/server.mjs` there, then `node examples/client.mjs` in another terminal (or `bun`, or `deno run -A`). The
binding is one Node-API addon for Node.js 22+, Bun and Deno 2, with TypeScript types. A call that may wait returns a
Promise and leaves the event loop running (a `Sync` form waits on the calling thread); a call that fails rejects with
a `FipcError` whose `code` names the result (`"FIPC_TIMEOUT"`, `"FIPC_DISCONNECTED"`, ...).

### Go

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

They are `examples/server` and `examples/client` of [`bindings/go`](bindings/go): run `go run ./examples/server`
there, then `go run ./examples/client` in another terminal. The binding is one package for Go 1.27+, without cgo: it
loads the library through purego, its one dependency, so `CGO_ENABLED=0` builds and cross-compiling work. Every method
is safe from any goroutine; a call that fails returns an error to test with `errors.Is` (`fipc.ErrTimeout`,
`fipc.ErrDisconnected`, ...), and timeouts are `time.Duration`s (`fipc.NoWait` doesn't wait, `fipc.Forever` waits as
long as it takes).

### C

```c
/* server.c */
#include <ctype.h>
#include <fipc.h>

int main(void)
{
    fipc_listener_t* listener;
    if (fipc_listen("my_channel", 1 << 20, &listener) != FIPC_OK) /* rings of 1 MiB each way */
        return 1;
    fipc_conn_t* conn;
    if (fipc_accept(listener, &conn, FIPC_FOREVER) == FIPC_OK) /* waits for a client */
    {
        char buf[256];
        fipc_rpc_msg_t request;
        if (fipc_rpc_recv(conn, buf, sizeof buf, &request, FIPC_FOREVER) == FIPC_OK)
        {
            for (size_t i = 0; i < request.len; i++)
                buf[i] = (char) toupper((unsigned char) buf[i]);
            fipc_rpc_respond(conn, request.id, request.opcode, 0, buf, request.len, FIPC_FOREVER);
        }
        fipc_close(conn);
    }
    fipc_listener_close(listener);
    return 0;
}
```

```c
/* client.c */
#include <fipc.h>
#include <stdio.h>

int main(void)
{
    fipc_conn_t* conn;
    fipc_result_t result = fipc_connect("my_channel", &conn, 5000);
    if (result != FIPC_OK)
    {
        fprintf(stderr, "connect: %s\n", fipc_result_str(result));
        return 1;
    }
    uint64_t id;
    char buf[256];
    fipc_rpc_msg_t response;
    if (fipc_rpc_submit(conn, 1, "ping", 4, &id, FIPC_FOREVER) == FIPC_OK /* opcode 1 */
        && fipc_rpc_recv(conn, buf, sizeof buf, &response, FIPC_FOREVER) == FIPC_OK)
        printf("%.*s\n", (int) response.len, buf); /* PING */
    fipc_close(conn);
    return 0;
}
```

They are `c_server` and `c_client` of [`examples/c-cpp`](examples/c-cpp): `zig build` there, then run
`zig-out/bin/c_server` and `zig-out/bin/c_client` ([C and C++](#c-and-c) says how to use another compiler). The header
documents every call's contract: results, timeouts, threads, messages in pieces, the peer's end.

### C++

```cpp
// server.cpp
#include <cctype>
#include <string>

#include <fipc.hpp>

int main()
{
    auto listener = fipc::listener::listen("my_channel", 1 << 20);  // rings of 1 MiB each way
    if (!listener)
        return 1;
    auto conn = listener->accept();  // waits for a client
    if (!conn)
        return 1;
    std::string payload;
    auto request = conn->rpc_receive(payload);
    if (!request)
        return 1;
    for (char& c : payload)
        c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
    return conn->rpc_respond(request->id, request->opcode, 0, payload) ? 0 : 1;
}
```

```cpp
// client.cpp
#include <chrono>
#include <cstdio>
#include <string>

#include <fipc.hpp>

int main()
{
    auto conn = fipc::connection::connect("my_channel", std::chrono::seconds(5));
    if (!conn)
    {
        std::fprintf(stderr, "connect: %s\n", conn.error().name());
        return 1;
    }
    std::string reply;
    if (!conn->rpc_submit(1, "ping") || !conn->rpc_receive(reply))  // opcode 1
        return 1;
    std::printf("%s\n", reply.c_str());  // PING
    return 0;
}
```

[`include/fipc.hpp`](include/fipc.hpp) is a header-only C++20 layer over the C API that adds no ABI and needs no
exceptions or RTTI: every call returns a `fipc::result<T>` (or `fipc::status`) that converts to `true` on success, and
`error()` says why not (`fipc::errc::timeout`, `fipc::errc::disconnected`, ...). `fipc::listener` and
`fipc::connection` are move-only and close when destroyed; sends take a `std::string_view` or a
`std::span<const std::byte>`, receives fill your buffer (a `std::string` or `std::vector` is resized to the message);
timeouts default to `fipc::forever`. The pair is `cpp_server` and `cpp_client` of
[`examples/c-cpp`](examples/c-cpp).

### Zig

```zig
// build.zig, after `zig fetch --save git+https://github.com/fastipc/fastipc`
const fastipc = b.dependency("fipc", .{ .target = target, .optimize = optimize });
exe.root_module.addImport("fastipc", fastipc.module("fastipc"));
```

```zig
// server.zig
const std = @import("std");
const fastipc = @import("fastipc");

pub fn main(init: std.process.Init) !void {
    // rings of 1 MiB each way
    const listener = try fastipc.Listener.listen(
        init.io,
        init.gpa,
        "my_channel",
        1 << 20,
    );
    defer listener.close();
    // waits for a client
    const conn = try listener.accept(.none);
    defer conn.close();

    var buf: [256]u8 = undefined;
    // .none: no timeout
    const req = try conn.rpcRecv(&buf, .none);
    const reply = std.ascii.upperString(&buf, req.payload);
    try conn.rpcRespond(req.id, req.opcode, 0, reply, .none);
}
```

```zig
// client.zig
const std = @import("std");
const fastipc = @import("fastipc");

pub fn main(init: std.process.Init) !void {
    const five_s: std.Io.Timeout = .{ .duration = .{
        .raw = .fromSeconds(5),
        .clock = .awake,
    } };
    const conn = try fastipc.Conn.connect(
        init.io,
        init.gpa,
        "my_channel",
        five_s,
    );
    defer conn.close();
    _ = try conn.rpcSubmit(1, "ping", .none); // opcode 1
    var buf: [256]u8 = undefined;
    const response = try conn.rpcRecv(&buf, .none);
    std.debug.print("{s}\n", .{response.payload}); // PING
}
```

They are `zig_server` and `zig_client` of [`examples/zig`](examples/zig): `zig build` there, then run
`zig-out/bin/zig_server` and `zig-out/bin/zig_client`. The native module runs on the caller's `std.Io` (it needs one
that runs concurrent tasks, such as `std.Io.Threaded`) and has listeners and connections with plain, zero-copy and RPC
messages. The website's [Zig page](https://fastipc.github.io/fastipc/zig.html) has its error sets, timeouts,
zero-copy calls, RPC and cancel.

## Startup and restarts

- **Any start order.** A client's `fipc_connect` waits for a server, within its timeout: the client may start first,
  or while the server serves another client. With `FIPC_FOREVER` it waits however late the server starts.
- **Restarts.** A client whose server ends gets `FIPC_DISCONNECTED`; it closes its connection and connects again, and
  gets in once the restarted server accepts it.
- **One client at a time per name.** The next client waits inside `fipc_connect` until the server has closed the
  connection it accepted last and calls `fipc_accept` again. A pool of workers needs one listener (and one name) per
  worker.
- **Different threads.** `fipc_accept` and `fipc_connect` set the connection up on the threads that call them, and
  `fipc_connect` returns once the server's `fipc_accept` has offered the rings: a client and its server run on
  different threads (of one process or two).
- **Polling.** A server's `fipc_accept` with timeout 0, once per frame of a game loop, lets a client in within two
  calls: a call that times out in the middle of a client's setup keeps it for the next.

## Platform support

Modern systems only ([`docs/platform-support.md`](docs/platform-support.md)):

- **Linux:** x86-64 (x86-64-v3, AVX2: Intel Haswell (2013), AMD Zen (2017) and later) and ARM64 (ARMv8.0-A: Raspberry
  Pi 3/4/5, AWS Graviton, Ampere, NVIDIA Jetson and later), glibc 2.34 or newer (Ubuntu 22.04, Debian 12, RHEL 9 and
  later).
- **Windows:** Windows 11, Windows Server 2022 and later, x64 (x86-64-v3).
- **macOS:** macOS 14.4 or newer, on Apple Silicon (M1 and later).

Not supported: 32-bit systems, Windows on ARM, Intel Macs, musl-based Linux, x86-64 CPUs without AVX2. Both processes of a connection run as the same user (on
Windows, in the same desktop session).

## Build from source

You need [Zig 0.17.0](https://ziglang.org/download/) and Python 3.9+ (for `devtool.py`, which wraps the build and
creates its own `venv/` on first use). The C# tests and benchmarks need the .NET 9 SDK; the Java binding's Gradle
wrapper needs a JDK 17+ to run (the build's JDK 25 is downloaded when none is installed); the Rust binding needs rustup
(its folder selects the stable toolchain); the Lua binding needs LuaJIT 2.1 (and LuaRocks to package it); the JavaScript binding needs Node.js 22+ (Bun and Deno
run its tests too); the Go binding needs Go 1.27+; C/H
formatting needs `clang-format-18`.

```bash
git clone https://github.com/fastipc/fastipc && cd fastipc
python devtool.py build                 # Debug build (zig build): zig-out/lib, zig-out/bin, zig-out/include
python devtool.py build --release       # ReleaseFast
python devtool.py dist                  # the release libraries (Linux x64 and arm64, Windows, macOS) in zig-out/dist
python devtool.py package --smoke       # the wheel(s), the NuGet package, the Maven artifact, the crates, the Lua
                                        #   rock, the npm package and the Go module in zig-out/packages, installed
                                        #   and tried
```

Every build targets x86-64-v3 (on macOS, `apple_m1` and macOS 14.4; on ARM64 Linux, generic ARMv8.0-A) unless `zig build -Dcpu=...` names another CPU. `zig build cdb` writes a
`compile_commands.json` for clangd.

A receiver that waits for a stream holds back its looks at the ring for its first few spin pauses, a number measured
per platform (`stream_paces` in `src/zig/data/ring_wait.zig`; [the stream pace](docs/perf/baseline.md#the-stream-pace-on-x86)).
`zig build -Dstream_pace=N` builds another for your hardware (1 turns the pacing off), and `python devtool.py
bench-pace` measures a set of them.

## Test

```bash
python devtool.py test fast             # unit tests, single-process C-API tests, the examples vs the docs (seconds)
python devtool.py test slow             # the multi-process C-API tests (minutes)
python devtool.py test all --filter rpc # fast, then slow; only tests whose name contains "rpc"
python devtool.py test chaos            # heavy, opt-in harnesses: peers killed, paused and restarted;
python devtool.py test soak             #   resources flat under churn;
python devtool.py test fuzz             #   a corrupt peer on the control connection and in the rings
python devtool.py test interop          # every language's server example against every language's client
                                        #   (needs every binding's toolchain; --skip-missing runs the rest)
python devtool.py bench zig-all         # the benchmark suite (bench/zig)
python devtool.py bench c-fastipc       # the C benchmark (bench/c; cpp-fastipc: the same loops through fipc.hpp)
dotnet test tests/csharp/Fipc.IntegrationTests -c Release   # the C# integration tests
bindings/java/gradlew -p bindings/java test                       # the Java integration tests (JUnit 5)
(cd bindings/rust && cargo test)                                  # the Rust tests (unit, integration, doc tests)
(cd bindings/lua && luajit tests/run.lua)                         # the Lua tests (LuaJIT alone runs them)
(cd bindings/js && node --expose-gc --test)                       # the JavaScript tests (also: bun test)
(cd bindings/go && go test ./...)                                 # the Go tests (-race where cgo is available)
```

On Linux and macOS, `zig build test test-slow -Denable_tsan=true` runs both tiers under ThreadSanitizer. [`CONTRIBUTING.md`](CONTRIBUTING.md) has the
test tiers, formatting and how to propose a change.

## Documentation

| Document | What |
|---|---|
| [The website](https://fastipc.github.io/fastipc/) | An overview, a quick start per language, and the benchmarks (its source: [`website/`](website/index.html)) |
| [`include/fipc.h`](include/fipc.h) | The API and its contract: results, timeouts, threads, messages, the peer's end, fork |
| [`docs/protocol.md`](docs/protocol.md) | The wire: names, control frames, the segment, ring frames, wake-ups (protocol version 1) |
| [`docs/design/architecture.md`](docs/design/architecture.md) | How the library is built: modules, threads, lifecycle, data path, invariants, known limits |
| [`docs/guide/code-tour.md`](docs/guide/code-tour.md) | Reading the code: each call's flow through the functions, and a reading plan |
| [`docs/platform-support.md`](docs/platform-support.md) | Supported systems, and loading and unloading the library |
| [`docs/perf/baseline.md`](docs/perf/baseline.md) | The benchmark suite, its method and the reference numbers |
| [`docs/roadmap.md`](docs/roadmap.md) | Open validation, known issues and possible directions |
| [`tests/zig/README.md`](tests/zig/README.md) | The C-API test suite and how to add a test |
| [`CHANGELOG.md`](CHANGELOG.md), [`CONTRIBUTING.md`](CONTRIBUTING.md), [`SECURITY.md`](SECURITY.md) | Releases, contributing, reporting a vulnerability |

## Repository layout

| Path | What |
|---|---|
| `include/fipc.h`, `include/fipc.hpp` | The public C header, the whole API, and the header-only C++20 wrapper over it |
| `src/zig/` | The library: `c_api.zig` (the exports), `root.zig` (the native Zig API), `lifecycle/` (listeners, connections, the handshake and the watcher), `session/` (the segment), `data/` (rings, waits, the data calls, RPC), `platform/` (Linux, Windows and macOS) |
| `bindings/` | The Python (CFFI) binding `fipc`, the C# binding `Fipc`, the Java (FFM) binding `io.github.fastipc`, the Rust crates `fipc` and `fipc-sys`, the Lua (LuaJIT FFI) rock `fipc`, the JavaScript (Node-API) npm package `fipc` and the Go module `github.com/fastipc/fastipc/bindings/go` (package `fipc`, without cgo), with their tests |
| `tests/` | The C-API suite (`zig`), the C++ wrapper's tests (`cpp`), the C# integration tests (`csharp`), and the chaos, soak and fuzz harnesses |
| `examples/` | The examples' servers and clients: downstream projects that depend on this repository, built with `zig build`, in C and C++ (`c-cpp`, no CMake) and Zig (`zig`); and the Python (`python`), C# (`csharp`) and Java (`java`) programs. The Rust, Lua, JavaScript and Go ones are in their bindings' `examples` folders |
| `bench/` | The benchmark suite (`bench/zig`), the C and C++ benchmark (`bench/c`) and the Python, C#, Java, Rust, Lua, JavaScript and Go benchmarks |
| `devtool.py`, `devtool_lib/` | The build, test, benchmark and packaging driver |
| `website/` | The project's website: static pages, no build step, published to GitHub Pages from `main` |
| `vendor/` | The build's one dependency, the official translate-c package, and Aro, which it uses ([`vendor/README.md`](vendor/README.md)): kept here so that no build downloads anything |

## License

MIT. Copyright (c) 2025-2026 Hayden Donnelly. See [LICENSE](LICENSE). The build-time packages in `vendor/` keep their
own licenses ([`vendor/README.md`](vendor/README.md)); the built library contains none of their code.
