# fipc for JavaScript

[![npm](https://img.shields.io/npm/v/fipc-js)](https://www.npmjs.com/package/fipc-js)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/fastipc/fastipc/blob/main/LICENSE)

Messages between processes on one machine, through shared memory: the JavaScript binding of
[FastIPC](https://github.com/fastipc/fastipc), a small library written in Zig
([`include/fipc.h`](https://github.com/fastipc/fastipc/blob/main/include/fipc.h)), for Node.js, Bun and Deno. Four
kinds, each created on a name:

- **Streams:** a server creates a stream (`createStream`) and its `Listener` accepts one client at a time; a client
  connects to the name (`connectStream`). Each connection has a shared-memory segment with one ring per direction, and
  reports the peer's end as soon as its process exits or closes the connection.
- **[Broadcasts](#broadcasts):** one publisher (`createBroadcast`) sends every message to each of its subscribers
  (`joinBroadcast`).
- **[Work queues](#work-queues):** producers and consumers join a queue (`createQueue`, `joinQueue`), and each message
  goes to one consumer.
- **[Services](#services):** clients and servers join a service (`createService`, `joinService`); each request of a
  client goes to one server, whose reply goes back to that client (`call`).

The peer can be written in any language with a binding: a Node.js process talks to a Zig, C, C++, Python, C#, Java,
Rust, Lua or Go process exactly as it talks to another JavaScript process (the protocol is the same, and [every pair of
languages is tested](https://fastipc.github.io/fastipc/#interop)). Use it where a JavaScript process and another local
process must talk at memory speed: hand CPU-heavy work to a worker in another language (or take it from one), a UI and
its engine, a tool and a game, without a socket.

## Installation

```bash
npm install fipc-js
```

One package for Node.js 22 or newer, Bun and Deno (`import { createStream } from "npm:fipc-js"`, with `--allow-ffi`): the
binding is one Node-API addon, which all three runtimes load, so nothing is compiled when it is installed and there is
no install script. TypeScript declarations are included, for ES modules (`import`) and CommonJS (`require`). The
package carries the addon and the native library for Windows x64 (Windows 11 / Server 2022 or newer) and Linux x64
(glibc 2.34 or newer: Ubuntu 22.04, Debian 12, RHEL 9 and later), which need an x86-64-v3 CPU (AVX2: Intel Haswell,
AMD Zen or later), for Linux on ARM64 (glibc 2.34 or newer) and for macOS 14.4 or newer on Apple Silicon.

## Use

```js
// server.mjs
import { createStream } from "fipc-js";

const listener = createStream("my_channel", 1 << 20); // rings of 1 MiB each way
const conn = await listener.accept(); // waits for a client; the event loop runs meanwhile
const message = await conn.receive();
await conn.send(message.toString().toUpperCase());
conn.close();
listener.close();
```

```js
// client.mjs
import { connectStream } from "fipc-js";

const conn = await connectStream("my_channel", 5000); // waits up to 5 s for the server
await conn.send("ping");
console.log((await conn.receive()).toString()); // PING
conn.close();
```

Run each in its own terminal (`node server.mjs`, then `node client.mjs`; `bun` runs them as they are, and `deno run
-A`); in a checkout of the repository they are `examples/server.mjs` and `examples/client.mjs`, run in this folder.
The Zig, C, C++, Python, C#, Java, Rust, Lua and Go clients of the [repository's
README](https://github.com/fastipc/fastipc#examples) work against this server too, and this client against their
servers. In CommonJS, `const { createStream, connectStream } = require("fipc-js")`.

The calls are grouped by kind. A **stream** (`createStream`, a listener's `accept`, `connectStream`) connects a server
and one client at a time. A **broadcast** (`createBroadcast`, `joinBroadcast`), a **work queue** (`createQueue`,
`joinQueue`) and a **service** (`createService`, `joinService`) are groups: their owner's process admits each member
that joins by itself, so no call of the owner's waits for them. The calls on a connection (`send`, `receive`, `sendAcquire`, `close`, ...) are the same for every
kind.

- **Copied messages:** `conn.send(data)` and `conn.receive()`, which resolves to the message as a new `Buffer`. A
  message of any size may be sent (at least 1 byte); it travels in pieces and arrives whole. `data` is a string (sent
  as UTF-8), a `Buffer` or any `TypedArray`, a `DataView` or an `ArrayBuffer`; `conn.receiveInto(buffer)` fills a
  buffer of yours and resolves to the message's length.
- **Zero-copy** (messages of one piece, up to `conn.maxPiece` bytes): `conn.sendAcquire(length)` resolves to a
  `Uint8Array` over the ring: write the message there, then `conn.sendCommit(length)`. `conn.receiveAcquire()`
  resolves to a `Uint8Array` over the message in the ring, valid until `conn.receiveRelease()`, the next receive or
  `close()`. When its bytes stop being valid the view is detached (its length becomes 0), so a stale view can never
  read or write the ring. In JavaScript, making a view costs more than copying a short message, so zero-copy pays off
  for long messages.
- **Batches:** `conn.sendMany(messages)` sends an array of messages, published together with at most one wake of the
  receivers, and resolves to how many went (a failure's `sent` says how many went before it);
  `conn.receiveMany(maxCount)` (16 if undefined) waits for the next message, then takes every further one already
  there, and resolves to an array of `Buffer`s; both have a `Sync` form. On every kind: a broadcast's subscriber takes
  those its publisher has moved past, and a service's client sends a batch of requests (owed as its sends are: `call`
  fails with `FIPC_INVALID` until their replies are received) and receives their replies in order; a server's batches
  fail with `FIPC_INVALID`. Batch both ends: a receiver that takes one message per call falls behind a batching
  sender.

```js
const sent = await conn.sendMany(["one", "two", "three"]); // published together
for (const message of await peer.receiveMany(64)) { // every message already there, up to 64
  handle(message);
}
```

### Async and Sync calls

Every call that may wait comes in two forms. The async one (`accept`, `connectStream`, `joinBroadcast`,
`joinQueue`, `joinService`, `waitForMembers`, `send`, `receive`, `receiveInto`, `call`, ...) returns a Promise and
leaves the event loop running: it makes the call at once if it needn't wait (a message is there, the ring has room),
else a thread of the handle's own (a connection has one per direction, a listener one for accepting, a queue or a
service one for counting its members, and each connect or join a thread of its own) waits in the library and settles
the promise. libuv's thread pool is never used, so files, DNS and crypto never wait behind FastIPC. The `Sync` form
(`acceptSync`, `connectStreamSync`, `joinQueueSync`, `sendSync`, `receiveSync`, `callSync`, ...) waits on the calling
thread, blocking the event loop meanwhile: the
fastest way to make many calls in a tight loop, a worker thread's way, and with a timeout of 0 a way to poll. A
broadcast's, a queue's or a service's examples below run on one thread with the `Sync` calls too, since the owner's
process admits its members by itself (and a service's reply never waits, so one thread can call and serve).

```js
const timer = setInterval(() => console.log("the event loop runs"), 100);
const message = await conn.receive(); // waits without blocking the timer
clearInterval(timer);
```

Calls of one direction run in the order they were made, so sends need not be awaited one by one; a promise settles
when its call is done. Don't change a buffer you passed to `send` (or read one you passed to `receiveInto`) until its
promise settles, and don't resize or transfer its ArrayBuffer meanwhile: the call uses the memory in place. While an
async call of a direction is pending, a `Sync` call of that direction, a commit or a release fails with the code
`ERR_FIPC_BUSY` (it would call the library from a second thread at once), and so does any call beside a pending async
acquire. A count of members (`waitForMembers`) is the exception: it may run beside any call, though an async count
that must wait queues behind one already waiting (so a short timeout can expire late). A service's client runs its
calls one at a time ([services](#services)).

### Errors and timeouts

A call the library refuses rejects (or, in the `Sync` form, throws) a `FipcError`: its `code` names the result,
`"FIPC_TIMEOUT"`, `"FIPC_DISCONNECTED"` (the peer's end: final, so close the connection, and accept or connect a new
one), `"FIPC_CANCELLED"`, `"FIPC_TOO_LARGE"`, `"FIPC_INVALID"`, `"FIPC_NO_MEMORY"`, `"FIPC_ADDR_IN_USE"`, on a
broadcast `"FIPC_OVERRUN"` and `"FIPC_UNSUBSCRIBED"`, and on a service's client `"FIPC_ABANDONED"` (the server that
took the request ended before it replied), the C API's results (`result` is the number, `Result.TIMEOUT` ...). A receive that is `"FIPC_TOO_LARGE"` for your buffer carries the message's length in
`error.length`, and leaves the message queued: a later receive can take it, unless on a work queue another consumer
takes it first. A misuse is a `TypeError` or `RangeError` (`ERR_INVALID_ARG_TYPE`,
`ERR_OUT_OF_RANGE`), or an `Error` with the code `ERR_FIPC_CLOSED` (a closed handle) or `ERR_FIPC_BUSY`.

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
once it has received the messages this side sent) and detaches every view; `listener.close()` stops listening, frees
the name and leaves the connections it accepted open. Both work with `using` (`Symbol.dispose`), as a queue and a
service do. A listener or connection the program drops is closed when the garbage collector finalizes it (a zero-copy
view that can still be reached keeps its connection open), and those still open when a worker thread or the process
ends are closed then; close them yourself to end them at once. A queue or a service is the exception: the collector
never closes it ([work queues](#work-queues), [services](#services)). An idle connection doesn't keep the process alive; a pending call does, until it
settles. There is no heartbeat and no timeout: a paused or hung peer is not reported, a crashed or killed one as soon as
its process has ended.

### Broadcasts

A broadcast sends every message of one publisher to each of its subscribers (up to 1024), in any processes of the
user: each message is stored once, in memory every subscriber maps, and is of one piece (up to a quarter of the
capacity, the connections' `maxPiece`).

```js
// publisher.mjs
import { createBroadcast } from "fipc-js";

// 64 KiB of messages, up to 8 subscribers; one that holds the publisher back 5 s is dropped
const publisher = createBroadcast("my_broadcast", 1 << 16, 8, 5000);
await publisher.waitForMembers(1); // waits for the first subscriber; the event loop runs meanwhile
for (let i = 1; i <= 3; i++) await publisher.send(`tick ${i}`);
publisher.close(); // ends the broadcast: subscribers get FIPC_DISCONNECTED after the ticks
```

```js
// subscriber.mjs
import { SUBSCRIBER_GATED, joinBroadcast } from "fipc-js";

// gated: misses nothing (the publisher waits for it)
const subscriber = await joinBroadcast("my_broadcast", SUBSCRIBER_GATED, 5000);
for (;;) {
  let tick;
  try {
    tick = await subscriber.receive();
  } catch (error) {
    if (error.code === "FIPC_DISCONNECTED") break; // the publisher closed
    throw error;
  }
  console.log(tick.toString()); // tick 1, tick 2, tick 3
}
subscriber.close();
```

Run the publisher, then the subscriber (`node publisher.mjs`, then `node subscriber.mjs`; in a checkout,
`examples/publisher.mjs` and `examples/subscriber.mjs`), or the other way round: `joinBroadcast` waits up to its
timeout for the publisher. `createBroadcast(name, capacity, maxSubscribers, maxStall)` returns the publisher, a
`Connection` whose sends go to every joined subscriber and the broadcast's one handle.

- **Joining:** `joinBroadcast(name, role, timeout)` (or `joinBroadcastSync`), on a thread of its own; the publisher's
  process admits each subscriber by itself, so the publisher's own thread may join too.
  `publisher.waitForMembers(atLeast, timeout)` resolves to the number of subscribers joined once there are at least
  `atLeast` (0 by default: at once); send nothing before they are there, as a send reaches only those joined. At
  `"FIPC_TIMEOUT"` or `"FIPC_CANCELLED"` the error's `count` is the count.
- **Gated** (`SUBSCRIBER_GATED`) misses nothing: a send waits for room it still holds. One that holds the publisher
  back for the broadcast's `maxStall` ms is unsubscribed: its receives fail with `"FIPC_UNSUBSCRIBED"` from then on
  (close it). `maxStall` left out (or negative) never unsubscribes; 0 refuses gated subscribers.
- **Lossy** (`SUBSCRIBER_LOSSY`) never holds the publisher back. One that falls a lap behind loses messages: a receive
  fails with `"FIPC_OVERRUN"` instead of a message, with the number of messages lost in `error.lost` (a number, exact
  up to 2<sup>53</sup>), and the next receive goes on from a recent message.

```js
try {
  handle(await subscriber.receive());
} catch (error) {
  if (error.code !== "FIPC_OVERRUN") throw error; // "FIPC_DISCONNECTED": the broadcast ended
  console.log(`${error.lost} messages lost`); // fell a lap behind: the next receive goes on from a recent message
}
```

Subscribers receive copies, with `receive` and `receiveInto`; `receiveAcquire` fails with `"FIPC_INVALID"` on them,
since the publisher may unsubscribe a gated one at any moment, which makes the memory of a message it holds in place
inaccessible (reading a view then would crash the process). The publisher may publish in place (`sendAcquire`,
`sendCommit`). `publisher.close()` ends the broadcast and frees its name; each subscriber receives what was sent, then
`"FIPC_DISCONNECTED"`. The broadcast's contract is
[`include/fipc.h`](https://github.com/fastipc/fastipc/blob/main/include/fipc.h)'s section on broadcasts.

### Work queues

A work queue takes messages from up to 63 producers and hands each to one of up to 64 consumers, in any processes of
the user. Each producer has a ring of its own; a consumer takes a message of any producer, and the queue promises no
order. Messages are of one piece (up to a quarter of the capacity, the connections' `maxPiece`), and wait in the
queue until a consumer takes them.

```js
// producer.mjs
import { CONSUMER, PRODUCER, createQueue, joinQueue } from "fipc-js";

// 4 KiB of messages per producer, 1 producer and 1 consumer: this process owns the queue and is its producer
const queue = createQueue("my_queue", 4096, 1, 1);
const producer = await joinQueue("my_queue", PRODUCER); // this process admits it, as any member
for (let i = 1; i <= 3; i++) await producer.send(`job ${i}`); // waits in the queue until a consumer takes it
await queue.waitForMembers(CONSUMER, 1); // waits for the consumer to join; the event loop runs meanwhile
producer.close();
queue.close(); // ends the queue: the consumer gets FIPC_DISCONNECTED after the jobs
```

```js
// consumer.mjs
import { CONSUMER, joinQueue } from "fipc-js";

const consumer = await joinQueue("my_queue", CONSUMER, 5000);
for (;;) {
  let job;
  try {
    job = await consumer.receive();
  } catch (error) {
    if (error.code === "FIPC_DISCONNECTED") break; // the queue ended
    throw error;
  }
  console.log(job.toString()); // job 1, job 2, job 3
}
consumer.close();
```

Run the producer, then the consumer (`node producer.mjs`, then `node consumer.mjs`; in a checkout,
`examples/producer.mjs` and `examples/consumer.mjs`), or the other way round: `joinQueue` waits up to its timeout for
the queue. The consumer prints `job 1` to `job 3`, in any order. `createQueue(name, capacity, maxProducers,
maxConsumers)` returns the owner's `Queue`. `capacity` is each producer's ring, a power of two from 4 KiB to 2 GiB;
`maxProducers` is 1 to 63 and `maxConsumers` 1 to 64.

- **Joining:** `joinQueue(name, PRODUCER, timeout)` or `joinQueue(name, CONSUMER, timeout)` (or `joinQueueSync`), the
  owner's members too, on a thread of their own: the owner's process admits each joiner by itself, so the owner's
  thread joins as any other does. A joiner whose role has no free place waits, within its timeout, for one. A
  broadcast's roles are `"FIPC_INVALID"` on a queue, and a queue's on a broadcast. The example's producer sends first
  (the jobs wait in the queue), then waits for its consumer, so it works in either start order.
- **The owner's queue:** `queue.waitForMembers(role, atLeast, timeout)` resolves to the number of members of `role`
  (`PRODUCER`, `CONSUMER`, or `undefined` or 0 for both) joined once there are at least `atLeast` (0 by default: at
  once); at `"FIPC_TIMEOUT"`, `"FIPC_CANCELLED"` or `"FIPC_DISCONNECTED"` the error's `count` is the count.
  `queue.cancel()` makes its waits fail with `"FIPC_CANCELLED"`.
- **Sending and receiving:** a producer sends (`send`, or `sendAcquire` and `sendCommit`), waiting for room in its
  ring while consumers haven't taken the messages in it; a consumer receives copied (`receive`, `receiveInto`) or in
  place (`receiveAcquire`, until `receiveRelease`). The other role's calls are `"FIPC_INVALID"`.
- **The end:** `queue.close()` ends the queue and frees its name, and so does the end of the owner's process (or of
  the worker thread that created it): producers' sends then fail with `"FIPC_DISCONNECTED"`, and consumers receive
  every message still in the queue, then `"FIPC_DISCONNECTED"`. A producer's or a consumer's close never ends the
  queue. The garbage collector never closes a queue: a queue lives until `close()` or the end of its process, and
  dropping the object doesn't end it (a later `createQueue` of its name in the same process fails with
  `"FIPC_ADDR_IN_USE"`). Close it yourself.

**Delivery.** Each message goes to one consumer. It goes again, to another consumer, only if the process of the consumer
that took it ends before acknowledging it, and when a consumer acknowledges is its own choice, not the queue's. A
message received in place is acknowledged at its release (or the consumer's next receive, or its close).

| How the consumer receives | When it acknowledges | If its process ends | What the application gets |
|---|---|---|---|
| `receive()` or `receiveInto()` (copying) | inside the call, before it returns | inside the call: the message goes to another consumer, and this one never saw it; after the call returned: it doesn't go again | **at most once**: no duplicates |
| `receiveAcquire()`, then `receiveRelease()` at once, then the work | at the release, before the work | as above | **at most once**: no duplicates |
| `receiveAcquire()`, the work, then `receiveRelease()` | at the release, after the work | before the release: the message goes to another consumer | **at least once**: a duplicate is possible, so the work must be idempotent |

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
for the requests of every client at once (one `receive`), and the servers of a pool share the load: each takes the next
request of any client. Each client has a ring of its own, which holds its requests and the room for their replies, so
a server never waits for a client. Requests and replies are of one piece, 1 to `maxMessage` bytes, the owner's choice
(the connections' `maxPiece`).

```js
// service_server.mjs
import { SERVER, createService, joinService } from "fipc-js";

// 4 KiB per client, requests and replies of up to 256 bytes, 1 client and 1 server: this process owns the service
// and is its server
const service = createService("my_service", 4096, 256, 1, 1);
const server = await joinService("my_service", SERVER, 5000); // this process admits it, as any member
for (let i = 0; i < 3; i++) { // three requests, then the end
  const request = await server.receive(); // the next request of any client
  await server.send(request.toString().toUpperCase()); // the reply goes to its client, and never waits
}
server.close();
service.close(); // ends the service: the client still receives the replies sent
```

```js
// service_client.mjs
import { CLIENT, joinService } from "fipc-js";

const client = await joinService("my_service", CLIENT, 5000);
for (let i = 1; i <= 3; i++) {
  // sends the request and resolves to its reply; FipcError FIPC_ABANDONED if the server ended before it replied
  const reply = await client.call(`ping ${i}`);
  console.log(reply.toString()); // PING 1, PING 2, PING 3
}
client.close();
```

Run the server, then the client (`node service_server.mjs`, then `node service_client.mjs`; in a checkout,
`examples/service_server.mjs` and `examples/service_client.mjs`), or the other way round: `joinService` waits up to its
timeout for the service. The client prints `PING 1` to `PING 3`. `createService(name, capacity, maxMessage,
maxClients, maxServers)` returns the owner's `Service`. `capacity` is each client's ring, a power of two from 4 KiB to
2 GiB; `maxMessage` is 1 to a quarter of it; `maxClients` and `maxServers` are 1 to 64.

- **Joining:** `joinService(name, CLIENT, timeout)` or `joinService(name, SERVER, timeout)` (or `joinServiceSync`),
  the owner's members too, on a thread of their own: the owner's process admits each joiner by itself, so the owner's
  thread joins as any other does. Clients wait while no server is joined, and servers while no client is. The other
  kinds' roles are `"FIPC_INVALID"` on a service, and a service's on them.
- **The owner's service:** `service.waitForMembers(role, atLeast, timeout)` resolves to the number of members of
  `role` (`CLIENT`, `SERVER`, or `undefined` or 0 for both) joined once there are at least `atLeast` (0 by default: at
  once); at `"FIPC_TIMEOUT"`, `"FIPC_CANCELLED"` or `"FIPC_DISCONNECTED"` the error's `count` is the count.
  `service.cancel()` makes its waits fail with `"FIPC_CANCELLED"`. The garbage collector never closes a service: it
  lives until `close()` or the end of its process (or of the worker thread that created it), and dropping the object
  doesn't end it.
- **A client:** `send(request)` (or `sendAcquire` and `sendCommit`) sends a request, and a receive (`receive`,
  `receiveInto`, or in place `receiveAcquire` until `receiveRelease`) resolves to the reply to its oldest request whose
  reply it hasn't received yet: the n-th reply answers the n-th request, whichever server replies first. A client may
  send several requests before it receives (as many as its ring has room for: each holds its length rounded up to 64
  bytes, plus `maxMessage` rounded up to 64, until its reply has been received), and the servers work on them at once.
  A receive that times out or is cancelled leaves its reply to come: the next receive returns it.
- **`client.call(request, timeout)`** (or `callSync`) sends a request and resolves to its reply, a new `Buffer`, waiting
  up to `timeout` in all. A call checks its request first: `"FIPC_INVALID"` for an empty one, `"FIPC_TOO_LARGE"` over
  `maxPiece`, a `TypeError` for another type, at once, even while a reply is owed. Then a call on a cancelled connection
  fails with `"FIPC_CANCELLED"` at once, whatever its timeout, and sends nothing: it could never receive the reply. The
  connection keeps count of the replies still owed: a call whose receive timed out or was cancelled leaves its reply
  owed, and the next call first receives and drops that reply (at most one is owed, since every call drops it first; an
  `"FIPC_ABANDONED"` in its place counts too), within its own timeout and before it sends, so a call never resolves to
  the reply to another request. A call that had to drop an owed reply or wait for its turn first sends its request only
  if its timeout has time left: once a positive timeout has run out, it fails with `"FIPC_TIMEOUT"` without sending, so
  no server sees that request; with `NO_WAIT` no step waits, and it sends if there is room. After a call that gave up, a
  plain `receive` returns that call's late reply (or `"FIPC_ABANDONED"`) first: replies come in request order. A call
  fails with `"FIPC_INVALID"` while a request sent with `send` (or `sendCommit`) still has its reply to be received:
  that reply is yours, so receive it first; on any connection other than a service's client it fails with
  `"FIPC_INVALID"` too.
- **Calls run one at a time,** in the order they were made: a call made while another is pending waits for its turn,
  its timeout counting from when it was made: it fails with `"FIPC_TIMEOUT"` if that runs out first (and with
  `"FIPC_CANCELLED"` at a `cancel` or a `close`), and the calls behind it keep their order. `Promise.all` of several
  calls resolves to each one's own reply. Meanwhile the client's other sends and receives (and `callSync`) fail with
  `ERR_FIPC_BUSY`, and so does a call while an async receive of yours is pending: either would race the call for its
  reply.
- **A server:** `receive` (or `receiveInto`, or `receiveAcquire`) takes the next request of any client (each client's
  in the order it sent them), and `send(reply)` (or `sendAcquire` and `sendCommit`) replies to it and never waits. A
  server holds one request at a time: its receives fail with `"FIPC_INVALID"` until it replies, and a reply with no
  request held fails with `"FIPC_INVALID"`. A request received in place stays valid until a reply (`send` or
  `sendCommit`) gives it up, which detaches its view, so the reply may be written from it; a reply refused with the
  request still held (empty, too large, or after the service's end) leaves the view until the close;
  `receiveRelease` does nothing. A reply
  while an async receive of the server's is pending fails with `ERR_FIPC_BUSY` (it would answer a request you haven't
  seen). A reply to a client that has left is dropped.
- **`"FIPC_ABANDONED"`:** a client's receive (or call) fails with `"FIPC_ABANDONED"` when the server that took the
  request ended before it replied: it closed its connection, or its process (or worker thread) ended. The request
  counts as answered, and the next receive returns the next reply. An abandoned request may or may not have been
  carried out; the service never sends it again, and the client decides whether to.

```js
let reply;
try {
  reply = await client.call(request);
} catch (error) {
  if (error.code !== "FIPC_ABANDONED") throw error; // "FIPC_DISCONNECTED": the service ended
  reply = await client.call(request); // its server ended before it replied: send it again, if it may run twice
}
```

- **The end:** `service.close()` ends the service and frees its name, and so does the end of the owner's process (or
  of the worker thread that created it): clients' sends and servers' receives then fail with `"FIPC_DISCONNECTED"`, a
  client receives the replies that came before the end, then `"FIPC_DISCONNECTED"`, and a server's reply fails with
  `"FIPC_DISCONNECTED"`. A client's or a server's close only leaves the service: a client's requests that no server
  took are dropped, and a server's close abandons the request it holds.
- **Not covered:** a server that hangs while its process runs (there are no timeouts, by design: its client waits,
  within its own timeouts); the owner's process, with which the service ends.

The service's contract is [`include/fipc.h`](https://github.com/fastipc/fastipc/blob/main/include/fipc.h)'s section on
services.

### Threads and runtimes

A listener, queue, service or connection belongs to the JavaScript thread (the main thread or a `Worker`) that made
it: each worker opens connections of its own. A client and its server may run in one process, as long as they don't
wait on the same thread: `await listener.accept()` and `await connectStream(name)` both wait on threads of their own,
so a process can even connect to itself.

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

Between two processes on the baseline machine (Node.js 22), copied 16-byte messages run at 2.1 to 2.5 million per
second through the async calls (each awaited) and about twice that through the `Sync` calls, and 64 KiB messages at
350,000 to 360,000 per second (`bench/js`; the numbers are in
[`docs/perf/bindings-baseline.md`](https://github.com/fastipc/fastipc/blob/main/docs/perf/bindings-baseline.md)). A
message that arrives within microseconds is taken on the JavaScript thread, and only a longer wait goes to the
connection's thread: a call that needn't wait costs no more than its Promise.

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
