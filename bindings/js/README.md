# fipc for JavaScript

[![npm](https://img.shields.io/npm/v/fipc)](https://www.npmjs.com/package/fipc)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/fastipc/fastipc/blob/main/LICENSE)

Messages and RPC between two processes on one machine, through shared memory: the JavaScript binding of
[FastIPC](https://github.com/fastipc/fastipc), a small library written in Zig
([`include/fipc.h`](https://github.com/fastipc/fastipc/blob/main/include/fipc.h)), for Node.js, Bun and Deno. A server
listens on a name and accepts one client at a time; a client connects to the name. Each connection has a shared-memory
segment with one ring per direction, and reports the peer's end as soon as its process exits or closes the connection.

The peer can be written in any language with a binding: a Node.js process talks to a Zig, C, C++, Python, C#, Java,
Rust, Lua or Go process exactly as it talks to another JavaScript process (the protocol is the same, and [every pair of
languages is tested](https://fastipc.github.io/fastipc/#interop)). Use it where a JavaScript process and another local
process must talk at memory speed: hand CPU-heavy work to a worker in another language (or take it from one), a UI and
its engine, a tool and a game, without a socket.

## Installation

```bash
npm install fipc
```

One package for Node.js 22 or newer, Bun and Deno (`import { Listener } from "npm:fipc"`, with `--allow-ffi`): the
binding is one Node-API addon, which all three runtimes load, so nothing is compiled when it is installed and there is
no install script. TypeScript declarations are included, for ES modules (`import`) and CommonJS (`require`). The
package carries the addon and the native library for Windows x64 (Windows 11 / Server 2022 or newer) and Linux x64
(glibc 2.34 or newer: Ubuntu 22.04, Debian 12, RHEL 9 and later), which need an x86-64-v3 CPU (AVX2: Intel Haswell,
AMD Zen or later), for Linux on ARM64 (glibc 2.34 or newer) and for macOS 14.4 or newer on Apple Silicon.

## Use

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

Run each in its own terminal (`node server.mjs`, then `node client.mjs`; `bun` runs them as they are, and `deno run
-A`); in a checkout of the repository they are `examples/server.mjs` and `examples/client.mjs`, run in this folder.
The Zig, C, C++, Python, C#, Java, Rust, Lua and Go clients of the [repository's
README](https://github.com/fastipc/fastipc#examples) work against this server too, and this client against their
servers. In CommonJS, `const { Listener, Connection } = require("fipc")`.

- **Plain messages:** `conn.send(data)` and `conn.receive()`, which resolves to the message as a new `Buffer`. A
  message of any size may be sent (at least 1 byte); it travels in pieces and arrives whole. `data` is a string (sent
  as UTF-8), a `Buffer` or any `TypedArray`, a `DataView` or an `ArrayBuffer`; `conn.receiveInto(buffer)` fills a
  buffer of yours and resolves to the message's length.
- **Zero-copy** (messages of one piece, up to `conn.maxPiece` bytes): `conn.sendAcquire(length)` resolves to a
  `Uint8Array` over the ring: write the message there, then `conn.sendCommit(length)`. `conn.receiveAcquire()`
  resolves to a `Uint8Array` over the message in the ring, valid until `conn.receiveRelease()`, the next receive or
  `close()`. When its bytes stop being valid the view is detached (its length becomes 0), so a stale view can never
  read or write the ring. In JavaScript, making a view costs more than copying a short message, so zero-copy pays off
  for long messages.
- **RPC:** `conn.rpcSubmit(opcode, payload)` resolves to the request's id; `conn.rpcRespond(id, opcode, status,
  payload)`; `conn.rpcReceive()` resolves to `{ id, kind, opcode, status, payload }` (`kind` is `RPC_REQUEST` or
  `RPC_RESPONSE`), and `conn.rpcReceiveInto(buffer)` to one with `length` in place of `payload`, the payload in your
  buffer. A payload may be empty (left out). Use a connection for plain messages or for RPC, not both.

### Async and Sync calls

Every call that may wait comes in two forms. The plain one (`accept`, `connect`, `send`, `receive`, `rpcReceive`, ...)
returns a Promise and leaves the event loop running: it makes the call at once if it needn't wait (a message is there,
the ring has room), else a thread of the connection's own (one per direction; a listener has one for accepting) waits
in the library and settles the promise. libuv's thread pool is never used, so files, DNS and crypto never wait behind
FastIPC. The `Sync` form (`acceptSync`, `connectSync`, `sendSync`, `receiveSync`, ...) waits on the calling thread,
blocking the event loop meanwhile: the fastest way to make many calls in a tight loop, a worker thread's way, and with
a timeout of 0 a way to poll.

```js
const timer = setInterval(() => console.log("the event loop runs"), 100);
const message = await conn.receive(); // waits without blocking the timer
clearInterval(timer);
```

Calls of one direction run in the order they were made, so sends need not be awaited one by one; a promise settles
when its call is done. Don't change a buffer you passed to `send` (or read one you passed to `receiveInto`) until its
promise settles, and don't resize or transfer its ArrayBuffer meanwhile: the call uses the memory in place. While an async call of a direction is pending, a `Sync` call of that direction, a commit or a release
fails with the code `ERR_FIPC_BUSY` (it would call the library from a second thread at once), and so does any call
beside a pending async acquire.

### Errors and timeouts

A call the library refuses rejects (or, in the `Sync` form, throws) a `FipcError`: its `code` names the result,
`"FIPC_TIMEOUT"`, `"FIPC_DISCONNECTED"` (the peer's end: final, so close the connection, and accept or connect a new
one), `"FIPC_CANCELLED"`, `"FIPC_TOO_LARGE"`, `"FIPC_INVALID"`, `"FIPC_NO_MEMORY"` or `"FIPC_ADDR_IN_USE"`, the C
API's results (`result` is the number, `Result.TIMEOUT` ...). A receive that is `"FIPC_TOO_LARGE"` for your buffer
carries the message's length in `error.length`, and leaves the message queued. A misuse is a `TypeError` or
`RangeError` (`ERR_INVALID_ARG_TYPE`, `ERR_OUT_OF_RANGE`), or an `Error` with the code `ERR_FIPC_CLOSED` (a closed
handle) or `ERR_FIPC_BUSY`.

Every call that can wait takes its timeout last, in milliseconds: `NO_WAIT` (0) doesn't wait, and leaving it out (or
`FOREVER`, -1) waits as long as it takes.

```js
try {
  handle(await conn.receive(100)); // waits at most 100 ms
} catch (error) {
  if (error.code !== "FIPC_TIMEOUT") throw error; // "FIPC_DISCONNECTED": the peer is gone
}
```

`conn.cancel()` makes every call of the connection that waits, then or later, fail with `"FIPC_CANCELLED"`.
`conn.close()` ends the connection (pending calls fail with `"FIPC_CANCELLED"`, the peer gets `"FIPC_DISCONNECTED"`
once it has received the messages this side sent) and detaches every view; `listener.close()` stops listening and
leaves the connections it accepted open. Both work with `using` (`Symbol.dispose`). A listener or connection the
program drops is closed when the garbage collector finalizes it (a zero-copy view that can still be reached keeps its
connection open), and those still open when a worker thread or the process ends are closed then; close them yourself
to end them at once. An idle connection doesn't keep the process alive; a pending call does, until it settles. There
is no heartbeat and no timeout: a paused or hung peer is not reported, a crashed or killed one as soon as its process
has ended.

### Threads and runtimes

A listener or connection belongs to the JavaScript thread (the main thread or a `Worker`) that made it: each worker
opens connections of its own. A client and its server may run in one process, as long as they don't wait on the same
thread: `await listener.accept()` and `await Connection.connect(name)` both wait on threads of their own, so a process
can even connect to itself.

The binding needs Node-API 8: Node.js 22 or newer, Bun and Deno 2 (which needs
`--allow-ffi` to load the addon, `--allow-read` and `--allow-env` for the binding to find it; `-A` grants all three).

### The native library

The binding looks for its addon (`fipc.node`) and the library in this order:

1. the folder the environment variable `FASTIPC_LIB_DIR` names (the library, and the addon if it is there too);
2. in a checkout of the FastIPC repository, its `zig-out` build;
3. the package's own copy, `prebuilds/<platform>-<arch>/` (`win32-x64`, `linux-x64`, `linux-arm64`, `darwin-arm64`);
4. for the library, the system's search (`PATH`, `LD_LIBRARY_PATH`, `DYLD_LIBRARY_PATH`).

`fipc.library` and `fipc.addon` say which files it loaded. The library is never unloaded (on Linux and macOS it
mustn't be: [platform support](https://github.com/fastipc/fastipc/blob/main/docs/platform-support.md)).

### Performance

Between two processes on the baseline machine (Node.js 22), copied 16-byte messages run at 2.6 to 3.3 million per
second through the async calls (each awaited) and about twice that through the `Sync` calls, RPC requests at 1.8 to 2.3
million, and 64 KiB messages at about 300,000 per second (`bench/js`; the numbers are in
[`docs/perf/bindings-baseline.md`](https://github.com/fastipc/fastipc/blob/main/docs/perf/bindings-baseline.md)). An
RPC round trip takes about 2 microseconds at the median through the async calls, as through the `Sync` ones: a call
whose answer comes within microseconds is taken on the JavaScript thread, and only a longer wait goes to the
connection's thread. A call that needn't wait costs no more than its Promise.

## Building from source

In a checkout of the repository, with the library and the addon built (`python devtool.py build`, which runs `zig
build`; it builds the addon for this platform as `zig-out/lib/fipc.node`, and `zig build dist` for every platform):

```bash
cd bindings/js
node --expose-gc --test      # the tests: the API, other processes, a Python peer (the repository's venv), the docs
bun test                     # the same tests under Bun
deno test -A --no-check --v8-flags=--expose-gc tests/   # and under Deno
node examples/server.mjs     # then, in another terminal: node examples/client.mjs
```

The tests need nothing but the runtime (`node:test`). `python devtool.py package js --smoke` packs the npm package with
every platform's addon and library in `zig-out/packages`, checks it, and installs it into a fresh project for a round
trip under Node.js (and Bun and Deno, where installed).

## Documentation

- [The website](https://fastipc.github.io/fastipc/js.html): this binding's guide, with examples of every call.
- [`index.d.ts`](index.d.ts): every class and call, with its types.
- [`include/fipc.h`](https://github.com/fastipc/fastipc/blob/main/include/fipc.h): the C API under this binding,
  and every call's contract (results, timeouts, threads, messages in pieces, the peer's end).
- [Platform support](https://github.com/fastipc/fastipc/blob/main/docs/platform-support.md) and the
  [protocol](https://github.com/fastipc/fastipc/blob/main/docs/protocol.md).
- [Changelog](https://github.com/fastipc/fastipc/blob/main/CHANGELOG.md),
  [issues](https://github.com/fastipc/fastipc/issues).

## License

MIT. Copyright (c) 2025-2026 Hayden Donnelly. See [LICENSE](https://github.com/fastipc/fastipc/blob/main/LICENSE).
