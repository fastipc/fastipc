# fipc for Lua

[![LuaRocks](https://img.shields.io/luarocks/v/fastipc/fipc)](https://luarocks.org/modules/fastipc/fipc)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/fastipc/fastipc/blob/main/LICENSE)

Messages between processes on one machine, through shared memory: the LuaJIT binding of
[FastIPC](https://github.com/fastipc/fastipc), a small library written in Zig
([`include/fipc.h`](https://github.com/fastipc/fastipc/blob/main/include/fipc.h)). Four kinds, each created on a
name:

- **Streams:** a server creates a stream (`fipc.stream_create`) and accepts one client at a time; a client connects to
  the name (`fipc.stream_connect`). The connect waits for the server's `accept`, and a Lua state has one thread, so in
  Lua a client and its server run in different processes. Each connection has a shared-memory segment with one ring
  per direction, and reports the peer's end as soon as its process exits or closes the connection.
- **[Broadcasts](#broadcasts):** one publisher (`fipc.broadcast_create`) sends every message to each of its
  subscribers (`fipc.broadcast_join`).
- **[Work queues](#work-queues):** producers and consumers join a queue (`fipc.queue_create`, `fipc.queue_join`), and
  each message goes to one consumer.
- **[Services](#services):** clients and servers join a service (`fipc.service_create`, `fipc.service_join`): each
  request goes to one server, and its reply back to its client.

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

local listener = assert(fipc.stream_create("my_channel", 1024 * 1024)) -- rings of 1 MiB each way
local conn = assert(listener:accept()) -- waits for a client
local message = assert(conn:recv())
assert(conn:send(message:upper()))
conn:close()
listener:close()
```

```lua
-- client.lua
local fipc = require("fipc")

local conn = assert(fipc.stream_connect("my_channel", 5000)) -- waits up to 5 s for the server
assert(conn:send("ping"))
print(assert(conn:recv())) -- PING
conn:close()
```

Run each in its own terminal (`luajit server.lua`, then `luajit client.lua`); in a checkout of the repository they are
`examples/server.lua` and `examples/client.lua`, run in this folder. The Zig, C, C++, Python, C#, Java, Rust, JavaScript and Go
clients of the [repository's README](https://github.com/fastipc/fastipc#examples) work against this server too,
and this client against their servers.

- **Copied messages:** `conn:send(data)` and `conn:recv()`, which returns the message as a Lua string. A message of any
  size may be sent (at least 1 byte); it travels in pieces and arrives whole. `data` is a string, or a cdata pointer
  and its length (`conn:send(ptr, len)`); `conn:recv_into(buf, size)` fills a buffer of yours and returns the length.
- **Zero-copy** (messages of one piece, up to `conn:max_piece()` bytes): `conn:send_acquire(len)` returns a `uint8_t*`
  into the ring: write the message there, then `conn:send_commit(len)`. `conn:recv_acquire()` returns a
  `const uint8_t*` into the ring and the length, valid until `conn:recv_release()`, the next receive or `close()`.
- **Batches:** `conn:send_many(messages)` sends a table of strings, published together with at most one wake of the
  receivers, and returns how many went (a failure's third value says how many went before it);
  `conn:recv_many(max_count)` (default 16) waits for the next message, then takes every further one already there,
  and returns them as a table of strings. On every kind: a broadcast's subscriber takes those its publisher has moved
  past, and a service's client sends a batch of requests (owed as its sends are: `call` returns `nil, "invalid"` until
  their replies are received) and receives their replies in order; a server's batches return `nil, "invalid"`. Batch
  both ends: a receiver that takes one message per call falls behind a batching sender.

```lua
local sent = assert(conn:send_many({ "one", "two", "three" })) -- published together
local batch = assert(conn:recv_many(64)) -- every message already there, up to 64
for _, message in ipairs(batch) do
    handle(message)
end
```

### Results and timeouts

A call that succeeds returns its value, or `true` when it has none. A call that doesn't returns `nil` and the error's
name, as Lua's `io` functions do: `"timeout"`, `"disconnected"` (the peer's end: final, so close the connection, and
accept or connect a new one), `"cancelled"`, `"too_large"`, `"invalid"`, `"no_memory"`, `"addr_in_use"`, on a
broadcast `"overrun"` (a lossy subscriber lost messages) or `"unsubscribed"`, and on a service's client `"abandoned"`
(the server that took the request ended before it replied), the C API's results
(`fipc.TIMEOUT` ... are the same strings; `fipc.result_str(err)` gives the C name, `FIPC_TIMEOUT`). Wrap a call in
`assert()` to raise the error instead. A receive that is `"too_large"` for your buffer returns the message's length
third, and leaves the message queued: a later receive can take it, unless on a work queue another consumer takes it
first. One that is `"overrun"` returns the number of messages lost third, and `wait_for_members` at `"timeout"`,
`"cancelled"` or `"disconnected"` the count. A misuse raises an error: a closed handle, or an argument of the wrong
type.

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

`conn:cancel()` makes every call of the connection that waits, then or later, return `nil, "cancelled"`. A handle
belongs to the Lua state that made it; another state (another thread: `love.thread`, Lanes) opens a connection of its
own. A broadcast's subscribers and a queue's or a service's members may join from the owner's own Lua state: the
owner's process admits each joiner on a thread of the library's, so no call of the owner's waits for it.

`conn:close()` ends the connection: the peer gets `"disconnected"` once it has received the messages this side sent.
A listener or connection the program drops is closed when the garbage collector frees it (an `ffi.gc` finalizer), so
close it yourself to end it at once. A queue or a service is not: it lives until `close()` or the end of its process
([work queues](#work-queues), [services](#services)). There is no heartbeat and no timeout: a paused or hung peer is
not reported, a crashed or killed one as soon as its process has ended.

### Broadcasts

A broadcast sends every message of one publisher to each of its subscribers (up to 1024), in any processes of the
user: each message is stored once, in memory every subscriber maps, and is of one piece (up to a quarter of the
capacity, the connections' `max_piece()`).

```lua
-- publisher.lua
local fipc = require("fipc")

-- 64 KiB of messages, up to 8 subscribers; one that holds the publisher back 5 s is dropped
local publisher = assert(fipc.broadcast_create("my_broadcast", 64 * 1024, 8, 5000))
assert(publisher:wait_for_members(1)) -- waits for the first subscriber
for i = 1, 3 do
    assert(publisher:send("tick " .. i))
end
publisher:close() -- ends the broadcast: subscribers get "disconnected" after the ticks
```

```lua
-- subscriber.lua
local fipc = require("fipc")

-- gated: misses nothing (the publisher waits for it)
local subscriber = assert(fipc.broadcast_join("my_broadcast", fipc.SUBSCRIBER_GATED, 5000))
while true do
    local tick, err = subscriber:recv()
    if not tick then
        assert(err == fipc.DISCONNECTED, err) -- the publisher closed
        break
    end
    print(tick) -- tick 1, tick 2, tick 3
end
subscriber:close()
```

Run the publisher, then the subscriber (`luajit examples/publisher.lua`, then `luajit examples/subscriber.lua` in this
folder of a checkout), or the other way round: `broadcast_join` waits up to its timeout for the publisher. The
subscriber prints `tick 1` to `tick 3`. `fipc.broadcast_create(name, capacity, max_subscribers, max_stall)` returns
the publisher, a connection whose sends go to every joined subscriber and the broadcast's one handle.

- **Joining:** `fipc.broadcast_join(name, role, timeout)`; the publisher's process admits each subscriber by itself,
  so the publisher's own Lua state may join too. `publisher:wait_for_members(at_least, timeout)` returns the number of
  subscribers joined once there are at least `at_least` (default 0: at once); send nothing before they are there, as
  a send reaches only those joined. At `"timeout"` or `"cancelled"` it returns `nil`, the error's name and the count.
  In a frame loop, poll it with `fipc.NO_WAIT`.
- **Gated** (`fipc.SUBSCRIBER_GATED`): misses nothing, as the publisher waits for it. One that holds the publisher
  back for the broadcast's `max_stall` is unsubscribed: its receives return `nil, "unsubscribed"`. It may receive in
  place (`recv_acquire`, which holds the publisher back until `recv_release`), but once it is unsubscribed, at a
  moment the publisher's clock picks, the pointer's memory becomes inaccessible and reading it crashes the process:
  release it well within `max_stall`, or receive in place only on a broadcast created with `max_stall` `nil`
  (`fipc.FOREVER`), which never unsubscribes. `recv` and `recv_into` are always safe. A `max_stall` of 0 refuses gated
  subscribers.
- **Lossy** (`fipc.SUBSCRIBER_LOSSY`): never holds the publisher back, and receives copies only (`recv_acquire` is
  `"invalid"`). One that falls a lap behind gets `nil, "overrun"` and the number of messages it lost instead of a
  message, and the next receive goes on from a recent message. The number is a Lua number, exact up to 2^53, like
  every length the binding returns:

```lua
local message, err, lost = subscriber:recv()
if message then
    handle(message)
elseif err == fipc.OVERRUN then -- fell a lap behind: the next receive goes on from a recent message
    print(lost .. " messages lost")
else
    error(err) -- "disconnected": the broadcast ended
end
```

- **The end:** `publisher:close()` ends the broadcast and frees its name; each subscriber receives what was sent,
  then `"disconnected"`.

### Work queues

A work queue takes messages from up to 63 producers and hands each to one of up to 64 consumers, in any processes of
the user. Each producer has a ring of its own; a consumer takes a message of any producer, and the queue promises no
order. Messages are of one piece (up to a quarter of the capacity, the connections' `max_piece()`), and wait in the
queue until a consumer takes them.

```lua
-- producer.lua
local fipc = require("fipc")

-- 4 KiB of messages per producer, 1 producer and 1 consumer: this process owns the queue and is its producer
local queue = assert(fipc.queue_create("my_queue", 4096, 1, 1))
local producer = assert(fipc.queue_join("my_queue", fipc.PRODUCER))
for i = 1, 3 do
    assert(producer:send("job " .. i)) -- waits in the queue until a consumer takes it
end
assert(queue:wait_for_members(fipc.CONSUMER, 1)) -- waits for the consumer to join
producer:close()
queue:close() -- ends the queue: the consumer gets "disconnected" after the jobs
```

```lua
-- consumer.lua
local fipc = require("fipc")

local consumer = assert(fipc.queue_join("my_queue", fipc.CONSUMER, 5000))
while true do
    local job, err = consumer:recv()
    if not job then
        assert(err == fipc.DISCONNECTED, err) -- the queue ended
        break
    end
    print(job) -- job 1, job 2, job 3
end
consumer:close()
```

Run the producer, then the consumer (`luajit examples/producer.lua`, then `luajit examples/consumer.lua` in this
folder of a checkout), or the other way round: `queue_join` waits up to its timeout for the queue. The consumer prints
`job 1` to `job 3`, in any order. `fipc.queue_create(name, capacity, max_producers, max_consumers)` returns the queue
(a `fipc.Queue`). `capacity` is each producer's ring, a power of two from 4 KiB to 2 GiB; `max_producers` is 1 to 63
and `max_consumers` 1 to 64.

- **Joining:** `fipc.queue_join(name, fipc.PRODUCER, timeout)` or `fipc.queue_join(name, fipc.CONSUMER, timeout)`,
  the owner's members too: the owner's process admits each joiner by itself, so the owner's own Lua state joins as any
  other does. A joiner whose role has no free place waits, within its timeout, for one. A broadcast's roles are
  `"invalid"` on a queue, and a queue's on a broadcast. The example's producer sends first (the jobs wait in the
  queue), then waits for its consumer, so it works in either start order.
- **The owner's queue:** `queue:wait_for_members(role, at_least, timeout)` returns the number of members of `role`
  (`fipc.PRODUCER`, `fipc.CONSUMER`, or `nil` for both) joined once there are at least `at_least`; at `"timeout"`,
  `"cancelled"` or `"disconnected"` it returns `nil`, the error's name and the count. `queue:cancel()` makes its waits
  return `nil, "cancelled"`.
- **Sending and receiving:** a producer sends (`send`, or `send_acquire` and `send_commit`), waiting for room in its
  ring while consumers haven't taken the messages in it; a consumer receives copied (`recv`, `recv_into`) or in place
  (`recv_acquire`, until `recv_release`). The other role's calls are `"invalid"`.
- **The end:** `queue:close()` ends the queue, and so does the end of the owner's process: producers' sends then
  return `nil, "disconnected"`, and consumers receive every message still in the queue, then `"disconnected"`. A
  producer's or a consumer's close never ends the queue. The garbage collector never closes a queue: a queue lives
  until `close()` or the end of its process, and dropping the object doesn't end it (a later `queue_create` of its
  name in the same process returns `nil, "addr_in_use"`). Close it yourself.

**Delivery.** Each message goes to one consumer. It goes again, to another consumer, only if the process of the consumer
that took it ends before acknowledging it, and when a consumer acknowledges is its own choice, not the queue's. A
message received in place is acknowledged at its release (or the consumer's next receive, or its close).

| How the consumer receives | When it acknowledges | If its process ends | What the application gets |
|---|---|---|---|
| `recv()` or `recv_into()` (copying) | inside the call, before it returns | inside the call: the message goes to another consumer, and this one never saw it; after the call returned: it doesn't go again | **at most once**: no duplicates |
| `recv_acquire()`, then `recv_release()` at once, then the work | at the release, before the work | as above | **at most once**: no duplicates |
| `recv_acquire()`, the work, then `recv_release()` | at the release, after the work | before the release: the message goes to another consumer | **at least once**: a duplicate is possible, so the work must be idempotent |

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

### Services

A service takes requests from up to 64 clients and hands each to one of up to 64 servers, in any processes of the
user; the server that took a request replies to it, and the reply goes back to the client that sent it. A server waits
for the requests of every client at once (one `recv`), and the servers of a pool share the load: each takes the next
request of any client. Each client has a ring of its own, which holds its requests and the room for their replies, so
a server never waits for a client. Requests and replies are of one piece, 1 to `max_message` bytes, the owner's choice
(the connections' `max_piece()`).

```lua
-- service_server.lua
local fipc = require("fipc")

-- 4 KiB per client, requests and replies of up to 256 bytes, 1 client and 1 server: this process owns the service
-- and is its server
local service = assert(fipc.service_create("my_service", 4096, 256, 1, 1))
local server = assert(fipc.service_join("my_service", fipc.SERVER, 5000))
for _ = 1, 3 do -- three requests, then the end
    local request = assert(server:recv()) -- the next request of any client
    assert(server:send(request:upper())) -- the reply goes to its client, and never waits
end
server:close()
service:close() -- ends the service: the client still receives the replies sent
```

```lua
-- service_client.lua
local fipc = require("fipc")

local client = assert(fipc.service_join("my_service", fipc.CLIENT, 5000))
for i = 1, 3 do
    -- sends the request and returns its reply; nil, "abandoned" if the server ended before it replied
    print(assert(client:call("ping " .. i))) -- PING 1, PING 2, PING 3
end
client:close()
```

Run the server, then the client (`luajit examples/service_server.lua`, then `luajit examples/service_client.lua` in
this folder of a checkout), or the other way round: `service_join` waits up to its timeout for the service. The client
prints `PING 1` to `PING 3`. `fipc.service_create(name, capacity, max_message, max_clients, max_servers)` returns the
service (a `fipc.Service`). `capacity` is each client's ring, a power of two from 4 KiB to 2 GiB; `max_message` is 1
to a quarter of it; `max_clients` and `max_servers` are 1 to 64.

- **Joining:** `fipc.service_join(name, fipc.CLIENT, timeout)` or `fipc.service_join(name, fipc.SERVER, timeout)`,
  the owner's members too: the owner's process admits each joiner by itself, so the owner's own Lua state joins as any
  other does. Clients wait while no server is joined, and servers while no client is. The other kinds' roles are
  `"invalid"` on a service, and a service's on them.
- **The owner's service:** `service:wait_for_members(role, at_least, timeout)` returns the number of members of `role`
  (`fipc.CLIENT`, `fipc.SERVER`, or `nil` for both) joined once there are at least `at_least`; at `"timeout"`,
  `"cancelled"` or `"disconnected"` it returns `nil`, the error's name and the count. `service:cancel()` makes its
  waits return `nil, "cancelled"`. The garbage collector never closes a service: it lives until `close()` or the end
  of its process, and dropping the object doesn't end it.
- **A client:** `send(request)` (or `send_acquire` and `send_commit`) sends a request, and a receive (`recv`,
  `recv_into`, or in place `recv_acquire` until `recv_release`) returns the reply to its oldest request whose reply it
  hasn't received yet: the n-th reply answers the n-th request, whichever server replies first. A client may send
  several requests before it receives (as many as its ring has room for: each holds its length rounded up to 64 bytes,
  plus `max_message` rounded up to 64, until its reply has been received), and the servers work on them at once. A
  receive that times out or is cancelled leaves its reply to come: the next receive returns it.
- **`client:call(request, timeout)`** sends a request and returns its reply, waiting up to `timeout` in all. A call
  checks its request first: `nil, "invalid"` for an empty one, `nil, "too_large"` over `max_piece()`, at once, even
  while a reply is owed. Then a call on a cancelled connection returns `nil, "cancelled"` at once, whatever its timeout,
  and sends nothing: it could never receive the reply. The connection keeps count of the replies still owed: a call
  whose receive timed out or was cancelled leaves its reply owed, and the next call first receives and drops that reply
  (at most one is owed, since every call drops it first; an `"abandoned"` in its place counts too), within its own
  timeout and before it sends, so a call never returns the reply to another request. A call that had to drop an owed
  reply first sends its request only if its timeout has time left: once a positive timeout has run out, it returns
  `nil, "timeout"` without sending, so no server sees that request; with `fipc.NO_WAIT` (0) no step waits, and it sends
  if there is room. After a call that gave up, a plain `recv` returns that call's late reply (or `"abandoned"`) first:
  replies come in request order. A call returns `nil, "invalid"` while a request sent with `send` (or `send_commit`)
  still has its reply to be received: that reply is yours, so receive it first; on any connection other than a service's
  client it returns `nil, "invalid"` too.
- **A server:** `recv` (or `recv_into`, or `recv_acquire`) takes the next request of any client (each client's in the
  order it sent them), and `send(reply)` (or `send_acquire` and `send_commit`) replies to it and never waits. A server
  holds one request at a time: its receives return `nil, "invalid"` until it replies, and a reply with no request held
  returns `nil, "invalid"`. A request received in place stays valid until the reply's `send` or `send_commit`, so the
  reply may be written from it; `recv_release` does nothing. A reply to a client that has left is dropped. Its
  `close()` answers the request it holds `"abandoned"`; a server never closed holds its request, and its client waits
  for the reply, until the garbage collector frees the connection or the server's process exits.
- **`"abandoned"`:** a client's receive (or call) returns `nil, "abandoned"` when the server that took the request
  ended before it replied: it closed its connection, or its process ended. The request counts as answered, and the
  next receive returns the next reply. An abandoned request may or may not have been carried out; the service never
  sends it again, and the client decides whether to.
- **The end:** `service:close()` ends the service, and so does the end of the owner's process: clients' sends and
  servers' receives then return `nil, "disconnected"`, a client receives the replies that came before the end, then
  `"disconnected"`, and a server's reply returns `"disconnected"`. A client's or a server's close only leaves the
  service: a client's requests that no server took are dropped.
- **Not covered:** a server that hangs while its process runs (there are no timeouts, by design: its client waits,
  within its own timeouts); the owner's process, with which the service ends.

### The native library

The module looks for the library in this order:

1. the folder the environment variable `FASTIPC_LIB_DIR` names;
2. in a checkout of the FastIPC repository, its `zig-out` build;
3. the module's own folder (a copy you ship next to `fipc.lua`);
4. the rock's copy, `fipc/fastipc.dll`, `fipc/libfastipc.so` (on Linux arm64, `fipc/arm64/libfastipc.so`) or
   `fipc/libfastipc.dylib` on `package.cpath` (on macOS the module looks for `.dylib` where `package.cpath` says
   `.so`);
5. the system's search (`PATH`, `LD_LIBRARY_PATH`, `DYLD_LIBRARY_PATH`).

`fipc.library` says which file it loaded. The library is never unloaded, even when the Lua state closes (on Linux and
macOS it mustn't be: [platform support](https://github.com/fastipc/fastipc/blob/main/docs/platform-support.md)).
`fipc.C` is the library's FFI namespace, for code that calls the C API itself.

### Performance

Between two processes, copied 16-byte messages run at 49 to 51 million per second through the binding on the baseline
machine, and 64 KiB messages at 400,000 to 430,000 per second (`bench/lua`; the numbers are in
[`docs/perf/bindings-baseline.md`](https://github.com/fastipc/fastipc/blob/main/docs/perf/bindings-baseline.md)).
LuaJIT compiles the calls into direct C calls; `recv_into` and the zero-copy calls allocate nothing for the message,
while `recv` makes a Lua string of it.

## Building from source

In a checkout of the repository, with the library built (`python devtool.py build`):

```bash
cd bindings/lua
luajit tests/run.lua         # the tests, against the repository's zig-out build, with peers in other processes and
                             #   a Python peer (the repository's venv) for the cross-language tests
luajit examples/server.lua   # then, in another terminal: luajit examples/client.lua
```

The tests need nothing but LuaJIT (their runner is `tests/run.lua`; `luajit tests/run.lua queue` runs the tests whose
name contains `queue`). `python devtool.py package lua --smoke` packages the rock with every platform's library in
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
