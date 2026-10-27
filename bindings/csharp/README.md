# Fipc

Messages between processes on one machine, through shared memory: the C# binding of
[FastIPC](https://github.com/fastipc/fastipc), a small library written in Zig
([`include/fipc.h`](https://github.com/fastipc/fastipc/blob/main/include/fipc.h)). Four kinds, each created on a
name:

- **Streams:** a server creates a stream (`FipcStream.Create`) and its `FipcListener` accepts one client at a time; a
  client connects to the name (`FipcStream.Connect`). Each connection has a shared-memory segment with one ring per
  direction, and reports the peer's end as soon as its process exits or closes the connection.
- **[Broadcasts](#broadcasts):** one publisher (`FipcBroadcast.Create`) sends every message to each of its
  subscribers (`FipcBroadcast.Join`).
- **[Work queues](#work-queues):** producers and consumers join a queue (`FipcQueue.Create`, `FipcQueue.Join`), and
  each message goes to one consumer.
- **[Services](#services):** clients and servers join a service (`FipcService.Create`, `FipcService.Join`): each
  request goes to one server, and its reply back to its client.

The binding has two layers: `FipcStream`, `FipcBroadcast`, `FipcQueue` and `FipcService`, which make `FipcListener`,
`FipcConnection`, `FipcQueue` and `FipcService` objects that are safe to dispose from any thread, and underneath them
`Fipc`, the raw P/Invoke declarations over `IntPtr` handles.

## Installation

```bash
dotnet add package Fipc
```

The native libraries for Windows (x64, Windows 11 / Server 2022 or newer), Linux (x64 and arm64: `linux-arm64`, glibc
2.34 or newer) and macOS (Apple Silicon, macOS 14.4 or newer: `osx-arm64`) are included (`runtimes/{rid}/native/`);
the x64 ones need an x86-64-v3 CPU (AVX2: Intel Haswell, AMD Zen or later).

## Use

A name holds a stream, a broadcast, a work queue or a service. A stream's server creates the name (`FipcStream.Create`) and
accepts one client at a time on its listener; a client connects to the name (`FipcStream.Connect`). `Connect` waits
for the server's `Accept`, so a client and its server run in different processes or on different threads. Timeouts
are int milliseconds, the last parameter: 0 doesn't wait, -1 (`Fipc.Forever`, as `Timeout.Infinite`) waits for ever,
and is the default where a call waits.

Setup throws: a create, connect or join, and the listener's `Accept()`, return their object or throw a
`FipcException`, whose `Result` says why (`AddrInUse`, `Timeout`, ...). Everything after setup returns a `FipcResult`:
a timeout, the peer's end (`Disconnected`), `Cancelled` and `TooLarge` are normal results of a loop, never exceptions,
which would cost about a round trip each. A loop that polls for clients uses `Accept(out var conn, timeoutMs)`, which
returns its result too. A mistake throws as before, such as a call after `Dispose` (`ObjectDisposedException`).
The website's [In production](https://fastipc.github.io/fastipc/csharp.html#production) sections have fuller
programs: every result handled by a switch the compiler checks, shutdown through a `CancellationToken`, a frame
loop that never waits.

```csharp
// Server/Program.cs
using System.Text;
using FastIpc;

// rings of 1 MiB each way; Accept waits for a client
using var listener = FipcStream.Create("my_channel", 1 << 20);
using var conn = listener.Accept();
if (conn.Receive(out byte[] message) == FipcResult.Ok)
{
    string upper = Encoding.UTF8.GetString(message)
        .ToUpperInvariant();
    conn.Send(Encoding.UTF8.GetBytes(upper));
}
```

```csharp
// Client/Program.cs
using System.Text;
using FastIpc;

// waits up to 5 s for the server, else throws
using var conn = FipcStream.Connect("my_channel", 5000);
conn.Send("ping"u8);
conn.Receive(out byte[] reply);
Console.WriteLine(Encoding.UTF8.GetString(reply));  // PING
```

Each is a console project (`dotnet new console`) with the `Fipc` package; run the server, then the client in
another terminal. The client sends the message `ping`, and the server answers with its uppercase, `PING`. Every
example of the [repository's README](https://github.com/fastipc/fastipc#examples) makes this exchange, so its C,
Python, Java, Rust, Lua, JavaScript and Go clients work against this server too, and this client against their
servers.

- **Copied messages:** `Send` and `Receive` (into a span; `TooLarge` reports the length it needs and takes nothing, or
  `Receive(out byte[] message, ...)` for a message of any size). A message of any size may be sent; it travels in
  pieces and arrives whole.
- **Zero-copy** (messages of one piece, up to `MaxPiece` bytes): `SendAcquire(length, out Span<byte> buffer, ...)`,
  write, `SendCommit(length)`; `ReceiveAcquire(out ReadOnlySpan<byte> message, ...)`, read, `ReceiveRelease()`.
- **Batches:** `SendMany(messages, out int sent, timeoutMs)` sends a span of `byte[]` or `ReadOnlyMemory<byte>`,
  published together with at most one wake of the receivers; `sent` says how many went, at every result.
  `ReceiveMany(buffer, lengths, out int count, timeoutMs)` waits for the next message, then takes every further one
  already there that fits `buffer`, packed back to back, their lengths in `lengths`; `ReceiveMany(out byte[][]
  messages, maxCount, timeoutMs)` returns them as arrays. On every kind: a broadcast's subscriber takes those its
  publisher has moved past, and a service's client sends a batch of requests (owed as its sends are: `Call` returns
  `Invalid` until their replies are received) and receives their replies in order; a server's batches return
  `Invalid`. Batch both ends: a receiver that takes one message per call falls behind a batching sender.

Threads: on a connection one thread at a time sends and one receives; `Cancel` and `Dispose` may be called from any
thread, at any time. `Dispose` cancels the handle, so a call that waits returns `Cancelled`, and the native close runs
once the calls in progress have returned: no call ever runs on a closed connection, and a zero-copy span stays valid
until its commit or release (a service server's request held in place: until its reply, as below), when the close
then runs. So stopping an IO thread is: `Dispose`, then join the thread (with a timeout). A
`Disconnected` connection is final: dispose it, and accept or connect a new one.

## Broadcasts

A broadcast sends every message of one publisher to each of its subscribers, in any processes of the user: each
message is stored once, in memory every subscriber maps, and is of one piece (up to a quarter of the capacity, the
connections' `MaxPiece`).

```csharp
// Publisher/Program.cs
using System.Text;
using FastIpc;

// 64 KiB of messages, up to 8 subscribers; one that holds
// the publisher back 5 s is dropped
using (var publisher = FipcBroadcast.Create("my_broadcast",
           1 << 16, 8, 5000))
{
    // waits for the first subscriber
    if (publisher.WaitForMembers(1, out _) != FipcResult.Ok)
        return 1;
    for (int i = 1; i <= 3; i++)
        publisher.Send(Encoding.UTF8.GetBytes($"tick {i}"));
}
// disposing the publisher ends the broadcast: subscribers
// get Disconnected after the ticks
return 0;
```

```csharp
// Subscriber/Program.cs
using System.Text;
using FastIpc;

// gated: misses nothing (the publisher waits for it)
using var sub = FipcBroadcast.Join("my_broadcast",
    FipcSubscriberRole.Gated, 5000);
FipcResult result;
while ((result = sub.Receive(out byte[] tick))
       == FipcResult.Ok)
    Console.WriteLine(Encoding.UTF8.GetString(tick));
// tick 1, tick 2, tick 3, then Disconnected: the
// publisher closed
return result == FipcResult.Disconnected ? 0 : 1;
```

Run the publisher, then the subscriber (or in the other order: `Join` waits up to its timeout for the publisher),
which prints `tick 1` to `tick 3`. `FipcBroadcast.Create(name, capacity, maxSubscribers, maxStallMs, out publisher)`
returns the publisher, a `FipcConnection` whose sends go to every subscriber: the broadcast's one handle.

- **Joining:** `FipcBroadcast.Join(name, role, out subscriber, timeoutMs)`; the publisher's process admits each
  joiner by itself, on a thread of the library's, so no thread of the publisher's waits for them.
  `publisher.WaitForMembers(atLeast, out int count, timeoutMs)` waits until that many have joined (`count` holds the
  count at every result but `Invalid`: at a `Timeout` too), so that nothing is sent before they are there.
- **Gated** (`FipcSubscriberRole.Gated`): misses nothing, as the publisher waits for it, and may receive in place
  (`ReceiveAcquire`, `ReceiveRelease`). One that holds the publisher back for `maxStallMs` is unsubscribed: its
  receives return `Unsubscribed`.
- **Lossy** (`FipcSubscriberRole.Lossy`): never holds the publisher back. One that falls a lap behind gets `Overrun`
  instead of a message: the connection's `LostMessages` (a `long`) says how many messages it lost, and so does the
  span receive's `length`, up to `int.MaxValue`; the next receive goes on from a recent message.
- **The end:** disposing the publisher ends the broadcast and frees the name; each subscriber receives what was sent,
  then `Disconnected`. A publisher that is never disposed is closed by its finalizer once it is unreachable.

## Work queues

A work queue takes messages from up to 63 producers and hands each to one of up to 64 consumers, in any processes of
the user. Each producer has a ring of its own; a consumer takes a message of any producer, and the queue promises no
order. Messages are of one piece (up to a quarter of the capacity, the connections' `MaxPiece`) and wait in the queue
until a consumer takes them. Each message goes to one consumer, and again only if that consumer's process ends before
acknowledging it (**Delivery**, below).

```csharp
// Producer/Program.cs
using System.Text;
using FastIpc;

// 4 KiB of messages per producer, 1 producer and 1 consumer:
// this process owns the queue and is its producer
using (var queue = FipcQueue.Create("my_queue", 4096, 1, 1))
using (var producer = FipcQueue.Join("my_queue",
           FipcQueueRole.Producer, 5000))
{
    for (int i = 1; i <= 3; i++)
    {
        // waits in the queue until a consumer takes it
        if (producer.Send(Encoding.UTF8.GetBytes($"job {i}"))
            != FipcResult.Ok)
            return 1;
    }
    // waits for the consumer to join
    if (queue.WaitForMembers(FipcQueueRole.Consumer, 1, out _)
        != FipcResult.Ok)
        return 1;
}
// disposing the queue ends it: the consumer gets
// Disconnected after the jobs
return 0;
```

```csharp
// Consumer/Program.cs
using System.Text;
using FastIpc;

using var consumer = FipcQueue.Join("my_queue",
    FipcQueueRole.Consumer, 5000);
FipcResult result;
while ((result = consumer.Receive(out byte[] job))
       == FipcResult.Ok)
    Console.WriteLine(Encoding.UTF8.GetString(job));
// job 1, job 2, job 3, then Disconnected: the queue
// ended
return result == FipcResult.Disconnected ? 0 : 1;
```

Run the producer, then the consumer (or in the other order: `Join` waits up to its timeout for the queue), which
prints `job 1` to `job 3`. `FipcQueue.Create(name, capacity, maxProducers, maxConsumers, out queue)` returns the
`FipcQueue`, which the `using` disposes.

- **Joining:** `FipcQueue.Join(name, FipcQueueRole.Producer, ...)` or `FipcQueueRole.Consumer`, the owner's members
  too, from any thread: the owner's process admits each joiner by itself, on a thread of the library's. The example's
  producer sends first (the jobs wait in the queue), then waits for its consumer, so it works in either start order.
- **The owner's queue:** `queue.WaitForMembers(role, atLeast, out int count, timeoutMs)` waits until that many
  members of the role (`FipcQueueRole.Producer`, `FipcQueueRole.Consumer`, or `null` for both) have joined (`count`
  holds the count at every result but `Invalid`); `queue.Cancel()` makes its waits return `Cancelled`, from any
  thread.
- **Lifetime:** a queue lives until `Dispose` or the end of its process; dropping the object doesn't end it. Unlike
  this binding's other handles, a `FipcQueue` has no finalizer: its use lives in its members, in this process and in
  others, so the garbage collector never ends it. A queue that is never disposed holds its name until the process
  ends.
- **Receiving:** `Receive` into a span (`TooLarge` takes nothing: another consumer may take that message) or into a
  new array, or in place, `ReceiveAcquire` until `ReceiveRelease`.
- **The end:** after the queue's `Dispose`, a producer's sends return `Disconnected`, and each consumer receives what
  is left in the queue, then `Disconnected`. A producer's or a consumer's `Dispose` only leaves the queue.

**Delivery.** Each message goes to one consumer. It goes again, to another consumer, only if the process of the consumer
that took it ends before acknowledging it, and when a consumer acknowledges is its own choice, not the queue's. A
message received in place is acknowledged at its release (or the consumer's next receive, or its close).

| How the consumer receives | When it acknowledges | If its process ends | What the application gets |
|---|---|---|---|
| `Receive` (copying) | inside the call, before it returns | inside the call: the message goes to another consumer, and this one never saw it; after the call returned: it doesn't go again | **at most once**: no duplicates |
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

## Services

A service takes requests from up to 64 clients and hands each to one of up to 64 servers, in any processes of the
user; the server that took a request replies to it, and the reply goes back to the client that sent it. A server waits
for the requests of every client at once (one `Receive`), and the servers of a pool share the load: each takes the next
request of any client. Each client has a ring of its own, which holds its requests and the room for their replies, so a
server never waits for a client. Requests and replies are of one piece, 1 to `maxMessage` bytes, the owner's choice
(the connections' `MaxPiece`).

```csharp
// ServiceServer/Program.cs
using System.Text;
using FastIpc;

// 4 KiB per client, requests and replies of up to 256 bytes,
// 1 client and 1 server: this process owns the service and
// is its server
using (var service = FipcService.Create("my_service", 4096,
           256, 1, 1))
using (var server = FipcService.Join("my_service",
           FipcServiceRole.Server, 5000))
{
    // three requests, then the end
    for (int i = 1; i <= 3; i++)
    {
        // the next request of any client
        if (server.Receive(out byte[] request) != FipcResult.Ok)
            return 1;
        string upper = Encoding.UTF8.GetString(request)
            .ToUpperInvariant();
        // the reply goes to its client, and never waits
        if (server.Send(Encoding.UTF8.GetBytes(upper),
                Fipc.NoWait) != FipcResult.Ok)
            return 1;
    }
}
// disposing the service ends it: the client still receives
// the replies sent
return 0;
```

```csharp
// ServiceClient/Program.cs
using System.Text;
using FastIpc;

using var client = FipcService.Join("my_service",
    FipcServiceRole.Client, 5000);
for (int i = 1; i <= 3; i++)
{
    // sends the request and returns its reply; Abandoned if
    // the server ended before it replied
    if (client.Call(Encoding.UTF8.GetBytes($"ping {i}"),
            out byte[] reply) != FipcResult.Ok)
        return 1;
    // PING 1, PING 2, PING 3
    Console.WriteLine(Encoding.UTF8.GetString(reply));
}
return 0;
```

Run the server, then the client (or in the other order: `Join` waits up to its timeout for the service), which prints
`PING 1` to `PING 3`. `FipcService.Create(name, capacity, maxMessage, maxClients, maxServers, out service)` returns the
`FipcService`, which the `using` disposes.

- **Joining:** `FipcService.Join(name, FipcServiceRole.Client, ...)` or `FipcServiceRole.Server`, the owner's members
  too, from any thread: the owner's process admits each joiner by itself, on a thread of the library's. Clients wait
  while no server is joined, and servers while no client is.
- **The owner's service:** `service.WaitForMembers(role, atLeast, out int count, timeoutMs)` waits until that many
  members of the role (`FipcServiceRole.Client`, `FipcServiceRole.Server`, or `null` for both) have joined (`count`
  holds the count at every result but `Invalid`); `service.Cancel()` makes its waits return `Cancelled`, from any
  thread. A `FipcService` has no finalizer, as a `FipcQueue` has none: a service lives until `Dispose` or the end of
  its process, and dropping the object doesn't end it.
- **A client:** `Send(request, ...)` sends a request, and `Receive` (into a span or a new array, or in place
  `ReceiveAcquire` until `ReceiveRelease`) receives the reply to its oldest request whose reply it hasn't received yet:
  the n-th reply answers the n-th request, whichever server replies first. A client may send several requests before
  it receives (as many as its ring has room for: each holds its length rounded up to 64 bytes, plus `maxMessage`
  rounded up to 64, until its reply has been received), and the servers work on them at once. A receive that times out
  or is cancelled leaves its reply to come: the next receive returns it.
- **`Call(request, out byte[] reply, timeoutMs)`** sends a request and receives its reply. A call checks its request
  first: `Invalid` for an empty one, `TooLarge` over `MaxPiece`, at once, even while a reply is owed. Then a call on a
  cancelled connection returns `Cancelled` at once, whatever its timeout, and sends nothing: it could never receive the
  reply. The connection keeps count of the replies still owed: a call whose receive timed out or was cancelled leaves
  its reply owed, and the next call first receives and drops that reply (at most one is owed, since every call drops it
  first; an `Abandoned` in its place counts too), within its own timeout and before it sends, so a call never returns
  the reply to another request. A call that had to drop an owed reply first sends its request only if its timeout has
  time left: once a positive timeout has run out, it returns `Timeout` without sending, so no server sees that request;
  with `NoWait` (0) no step waits, and it sends if there is room. After a call that gave up, a plain `Receive` returns
  that call's late reply (or `Abandoned`) first: replies come in request order. A call returns `Invalid` while a request
  sent with `Send` (or `SendCommit`) still has its reply to be received: that reply is yours, so receive it first; on
  any connection other than a service's client it returns `Invalid` too. A call sends and receives, so no other thread
  may send or receive on the connection meanwhile.
- **A server:** `Receive` takes the next request of any client (each client's in the order it sent them), and
  `Send(reply, ...)` replies to it and never waits. A server holds one request at a time: its receives return `Invalid`
  until it replies, and a reply with no request held returns `Invalid`. A request received in place
  (`ReceiveAcquire`) stays valid until the reply's `Send` or `SendCommit`, which ends its span (so does
  `ReceiveRelease`, the next receive, or `Dispose` on the receiving thread; a `ReceiveRelease` leaves the request held
  until its reply). A span keeps the connection open: after a `Dispose` on another thread, the connection closes at the
  span's end. A reply to a
  client that has left is dropped. A server may receive on one thread and reply on another.
- **`Abandoned`:** a client's receive (or call) returns `FipcResult.Abandoned` when the server that took the request
  ended before it replied: it disposed its connection, or its process ended. The request counts as answered, and the
  next receive returns the next reply. An abandoned request may or may not have been carried out; the service never
  sends it again, and the client decides whether to.
- **The end:** after the service's `Dispose`, clients' sends and servers' receives return `Disconnected`; a client
  receives the replies that came before the end, then `Disconnected`, and a server's reply returns `Disconnected`. A
  client's or a server's `Dispose` only leaves the service: a client's requests that no server took are dropped, and
  the request a server holds, if it hasn't replied, is answered `Abandoned` (one held in place, at its span's end).
- **Not covered:** a server that hangs while its process runs (there are no timeouts, by design: its client waits,
  within its own timeouts); the owner's process, with which the service ends.

## The raw layer

`Fipc` declares the C API one to one: `IntPtr` handles, pointers or spans, and every call, setup's included, returns
its `FipcResult`. It costs nothing beyond the native call,
where each object-layer call holds its SafeHandle (two atomic operations, about 15 ns per call; see
[`docs/perf/bindings-baseline.md`](https://github.com/fastipc/fastipc/blob/main/docs/perf/bindings-baseline.md)), so it is the escape hatch for the hottest loops. It enforces nothing either: `Close`
needs the handle to itself (cancel, join the threads that use it, then close), and closing under a running call frees
memory the call still uses.

```csharp
Fipc.StreamConnect("my_channel", out IntPtr conn, 5000);
Fipc.Send(conn, "hello"u8, Fipc.Forever);
Fipc.Close(conn);
```

## Documentation

- [The website](https://fastipc.github.io/fastipc/csharp.html): this binding's guide, with examples of every call.
- [`include/fipc.h`](https://github.com/fastipc/fastipc/blob/main/include/fipc.h): the C API under this binding, and every call's contract (results,
  timeouts, threads, messages in pieces, the peer's end).
- [Platform support](https://github.com/fastipc/fastipc/blob/main/docs/platform-support.md) and the [protocol](https://github.com/fastipc/fastipc/blob/main/docs/protocol.md).
- [Changelog](https://github.com/fastipc/fastipc/blob/main/CHANGELOG.md), [issues](https://github.com/fastipc/fastipc/issues).

## License

MIT. Copyright (c) 2025-2026 Hayden Donnelly. See the LICENSE file in the package.
