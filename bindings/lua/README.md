# fipc for Lua

[![LuaRocks](https://img.shields.io/luarocks/v/fastipc/fipc)](https://luarocks.org/modules/fastipc/fipc)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/fastipc/fastipc/blob/main/LICENSE)

Messages and RPC between two processes on one machine, through shared memory: the LuaJIT binding of
[FastIPC](https://github.com/fastipc/fastipc), a small library written in Zig
([`include/fipc.h`](https://github.com/fastipc/fastipc/blob/main/include/fipc.h)). A server listens on a name and
accepts one client at a time; a client connects to the name. `connect` waits for the server's `accept`, and a Lua state
has one thread, so in Lua a client and its server run in different processes. Each connection has a shared-memory
segment with one ring per direction, and reports the peer's end as soon as its process exits or closes the connection.

The peer can be written in any language with a binding: a Lua process talks to a Zig, C, C++, Python, C#, Java, Rust, JavaScript or Go
process exactly as it talks to another Lua process (the protocol is the same, and [every pair of languages is
tested](https://fastipc.github.io/fastipc/#interop)). Use it where two local processes must talk at memory speed: a
game or its editor and their tools, a UI and its engine, a script and a native service.

## Installation

```bash
luarocks install fipc
```

LuaJIT 2.1 (also OpenResty's, Neovim's and LÖVE's): the binding is one Lua module, `fipc`, over LuaJIT's FFI, so
nothing is compiled. PUC Lua (5.1 to 5.4) has no FFI and isn't supported. The rock carries the native library for
Windows x64 (Windows 11 / Server 2022 or newer) and Linux x64 (glibc 2.34 or newer: Ubuntu 22.04, Debian 12, RHEL 9
and later), which need an x86-64-v3 CPU (AVX2: Intel Haswell, AMD Zen or later), for Linux on ARM64 (glibc 2.34 or
newer; installed as `fipc/arm64/libfastipc.so`, beside the x64 one) and for macOS 14.4 or newer on Apple Silicon.
LuaRocks must be set up for LuaJIT
(for example `luarocks --lua-version 5.1 --lua-dir <your LuaJIT> install fipc`).

Without LuaRocks, put `fipc.lua` and the library for your platform (`fastipc.dll`, `libfastipc.so` or
`libfastipc.dylib`) in one folder on `package.path`, for example in your game's folder: the GitHub release's
`fipc-lua-1.0.0.tar.gz` holds the module and every platform's library.

## Use

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

Run each in its own terminal (`luajit server.lua`, then `luajit client.lua`); in a checkout of the repository they are
`examples/server.lua` and `examples/client.lua`, run in this folder. The Zig, C, C++, Python, C#, Java, Rust, JavaScript and Go
clients of the [repository's README](https://github.com/fastipc/fastipc#examples) work against this server too,
and this client against their servers.

- **Plain messages:** `conn:send(data)` and `conn:recv()`, which returns the message as a Lua string. A message of any
  size may be sent (at least 1 byte); it travels in pieces and arrives whole. `data` is a string, or a cdata pointer
  and its length (`conn:send(ptr, len)`); `conn:recv_into(buf, size)` fills a buffer of yours and returns the length.
- **Zero-copy** (messages of one piece, up to `conn:max_piece()` bytes): `conn:send_acquire(len)` returns a `uint8_t*`
  into the ring: write the message there, then `conn:send_commit(len)`. `conn:recv_acquire()` returns a
  `const uint8_t*` into the ring and the length, valid until `conn:recv_release()`, the next receive or `close()`.
- **RPC:** `conn:rpc_submit(opcode, payload)` returns the request's id; `conn:rpc_respond(id, opcode, status,
  payload)`; `conn:rpc_recv()` returns a table `{ id, kind, opcode, status, payload }` (`kind` is `fipc.RPC_REQUEST`
  or `fipc.RPC_RESPONSE`), and `conn:rpc_recv_into(buf, size)` one with `len` in place of `payload`, the payload in
  your buffer. A payload may be empty (`nil`). Use a connection for plain messages or for RPC, not both.

### Results and timeouts

A call that succeeds returns its value, or `true` when it has none. A call that doesn't returns `nil` and the error's
name, as Lua's `io` functions do: `"timeout"`, `"disconnected"` (the peer's end: final, so close the connection, and
accept or connect a new one), `"cancelled"`, `"too_large"`, `"invalid"`, `"no_memory"` or `"addr_in_use"`, the C API's
results (`fipc.TIMEOUT` ... are the same strings; `fipc.result_str(err)` gives the C name, `FIPC_TIMEOUT`). Wrap a call
in `assert()` to raise the error instead. A receive that is `"too_large"` for your buffer returns the message's length
third, and leaves the message queued. A misuse raises an error: a closed handle, or an argument of the wrong type.

Every call that can wait takes its timeout last, in milliseconds: `fipc.NO_WAIT` (0) doesn't wait, and `nil` (or
leaving it out) or `fipc.FOREVER` (-1) waits as long as it takes.

```lua
local message, err = conn:recv(100) -- waits at most 100 ms
if message then
    handle(message)
elseif err ~= fipc.TIMEOUT then -- "timeout": nothing yet, try again later
    error(err) -- "disconnected": the peer is gone
end
```

### Threads, coroutines and frame loops

A Lua state runs one thread at a time, so the C API's rule (on a connection, one thread sends and one receives) holds
by itself. A call that waits blocks the whole state, every coroutine in it: in a frame loop or a coroutine scheduler,
poll with `fipc.NO_WAIT` (or a short timeout) instead. In LÖVE, poll in `love.update`; in OpenResty, a call that
waits blocks the nginx worker, so poll and `ngx.sleep` between polls; in Neovim, poll from a `vim.uv` timer.

```lua
-- In a coroutine scheduler: poll, and yield until a message (or an error) comes
local function receive(conn)
    while true do
        local message, err = conn:recv(fipc.NO_WAIT)
        if message or err ~= fipc.TIMEOUT then
            return message, err
        end
        coroutine.yield()
    end
end
```

`conn:cancel()` makes every call of the connection that waits, then or later, return `nil, "cancelled"`. A listener or
connection belongs to the Lua state that made it; another state (another thread: `love.thread`, Lanes) opens a
connection of its own.

`conn:close()` ends the connection: the peer gets `"disconnected"` once it has received the messages this side sent.
A listener or connection the program drops is closed when the garbage collector frees it (an `ffi.gc` finalizer), so
close it yourself to end it at once. There is no heartbeat and no timeout: a paused or hung peer is not reported, a
crashed or killed one as soon as its process has ended.

### The native library

The module looks for the library in this order:

1. the folder the environment variable `FASTIPC_LIB_DIR` names;
2. in a checkout of the FastIPC repository, its `zig-out` build;
3. the module's own folder (a copy you ship next to `fipc.lua`);
4. the rock's copy, `fipc/fastipc.dll`, `fipc/libfastipc.so` or `fipc/libfastipc.dylib` on
   `package.cpath` (on macOS the module looks for `.dylib` where `package.cpath` says `.so`);
5. the system's search (`PATH`, `LD_LIBRARY_PATH`, `DYLD_LIBRARY_PATH`).

`fipc.library` says which file it loaded. The library is never unloaded, even when the Lua state closes (on Linux and
macOS it mustn't be: [platform support](https://github.com/fastipc/fastipc/blob/main/docs/platform-support.md)).
`fipc.C` is the library's FFI namespace, for code that calls the C API itself.

### Performance

Between two processes, copied 16-byte messages run at 49 to 56 million per second through the binding on the baseline
machine, and 64 KiB messages at 440,000 to 500,000 per second (`bench/lua`; the numbers are in
[`docs/perf/bindings-baseline.md`](https://github.com/fastipc/fastipc/blob/main/docs/perf/bindings-baseline.md)).
LuaJIT compiles the calls into direct C calls; `recv_into`, the zero-copy calls and `rpc_recv_into` allocate nothing
for the message, while `recv` and `rpc_recv` make a Lua string of it.

## Building from source

In a checkout of the repository, with the library built (`python devtool.py build`):

```bash
cd bindings/lua
luajit tests/run.lua         # the tests, against the repository's zig-out build, with peers in other processes and
                             #   a Python peer (the repository's venv) for the cross-language tests
luajit examples/server.lua   # then, in another terminal: luajit examples/client.lua
```

The tests need nothing but LuaJIT (their runner is `tests/run.lua`; `luajit tests/run.lua rpc` runs the tests whose
name contains `rpc`). `python devtool.py package lua --smoke` packages the rock with every platform's library in
`zig-out/packages`, checks it, and installs it with LuaRocks into a fresh tree for a round trip.

## Documentation

- [The website](https://fastipc.github.io/fastipc/lua.html): this binding's guide, with examples of every call.
- [`include/fipc.h`](https://github.com/fastipc/fastipc/blob/main/include/fipc.h): the C API under this binding,
  and every call's contract (results, timeouts, threads, messages in pieces, the peer's end).
- [Platform support](https://github.com/fastipc/fastipc/blob/main/docs/platform-support.md) and the
  [protocol](https://github.com/fastipc/fastipc/blob/main/docs/protocol.md).
- [Changelog](https://github.com/fastipc/fastipc/blob/main/CHANGELOG.md),
  [issues](https://github.com/fastipc/fastipc/issues).

## License

MIT. Copyright (c) 2025-2026 Hayden Donnelly. See [LICENSE](https://github.com/fastipc/fastipc/blob/main/LICENSE).
