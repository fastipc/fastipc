# fipc

Messages between processes on one machine, through shared memory: the Python binding of
[FastIPC](https://github.com/fastipc/fastipc), a small library written in Zig. Four kinds, each created on a name:

- **Streams:** a server creates a stream (`fipc.stream_create`) and its `Listener` accepts one client at a time; a
  client connects to the name (`fipc.stream_connect`). Each connection has a shared-memory segment with one ring per
  direction, and reports the peer's end as soon as its process exits or closes the connection.
- **[Broadcasts](#broadcasts):** one publisher (`fipc.broadcast_create`) sends every message to each of its
  subscribers (`fipc.broadcast_join`).
- **[Work queues](#work-queues):** producers and consumers join a queue (`fipc.queue_create`, `fipc.queue_join`), and
  each message goes to one consumer.
- **[Services](#services):** clients and servers join a service (`fipc.service_create`, `fipc.service_join`): each
  request goes to one server, and its reply back to its client.

## Installation

```bash
pip install fipc-python
```

The wheels bundle the native library: Windows x64 (Windows 11 / Server 2022 or newer) and Linux x64 (glibc 2.34 or
newer: Ubuntu 22.04, Debian 12, RHEL 9 and later), which need an x86-64-v3 CPU (AVX2: Intel Haswell, AMD Zen or
later), Linux on ARM64 (`manylinux_2_34_aarch64`, glibc 2.34 or newer) and macOS 14.4 or newer on Apple Silicon. The
macOS wheel is tagged `macosx_14_0_arm64` (pip matches macOS 11
and later by the major version only), so pip also installs it on macOS 14.0 to 14.3, where the library doesn't load.
The binding loads it through CFFI's ABI mode, so one wheel per platform fits every Python 3.9+. A library of your own
comes first: set the environment variable `FASTIPC_LIB_DIR` to its folder (when it is set, the binding looks nowhere
else).

## Use

```python
# server.py
import fipc

# rings of 1 MiB each way
with fipc.stream_create("my_channel", 1 << 20) as listener:
    with listener.accept() as conn:  # waits for a client
        message = conn.recv()
        conn.send(message.upper())
```

```python
# client.py
import fipc

with fipc.stream_connect("my_channel", timeout_ms=5000) as conn:
    conn.send(b"ping")
    print(conn.recv())  # b'PING'
```

Run each in its own terminal: `python server.py`, then `python client.py`. The client sends the message `ping`, and the
server answers with its uppercase, `PING`. Every example of the
[repository's README](https://github.com/fastipc/fastipc#examples) makes this exchange, so its Zig, C, C++, C#, Java,
Rust, Lua, JavaScript and Go clients work against this server too, and this client against their servers.

A name holds a stream, a broadcast, a work queue or a service, and each kind has its functions: `stream_create`,
`stream_connect`; `broadcast_create`, `broadcast_join`; `queue_create`, `queue_join`; `service_create`,
`service_join`. They are the only way to make a `Listener`, a `Conn`, a `Queue` or a `Service`.

- **Streams:** `fipc.stream_create(name, capacity)` claims the name and returns its `Listener`, whose `accept()` sets
  one client at a time up on the calling thread (`NO_WAIT` polls); `fipc.stream_connect(name)` connects to it.
  `stream_connect` waits for the server's `accept`, so a client and its server run in different processes or on
  different threads.
- **Copied messages:** `send` and `recv` (a message of any size: it travels in pieces and arrives whole), or
  `recv_into` a buffer of your own.
- **Zero-copy** (messages of one piece, up to `max_piece()` bytes): `send_acquire(length)`, write into the
  memoryview, `send_commit(length)`; `recv_acquire()`, read, `recv_release()`.
- **Batches:** `send_many(messages)` sends a list of messages, published together with at most one wake of the
  receivers, and returns how many went (an error's `sent` says how many went before it); `recv_many(max_count=16)`
  waits for the next message, then takes every further one already there, and returns a list of `bytes`. On every
  kind: a broadcast's subscriber takes those its publisher has moved past, and a service's client sends a batch of
  requests (owed as its sends are: `call` waits until their replies are received) and receives their replies in
  order; a server's batches raise `INVALID`. Batch both ends: a receiver that takes one message per call falls
  behind a batching sender.

Timeouts are int milliseconds, the last parameter: `NO_WAIT` (0) doesn't wait, `FOREVER` (-1, the default) waits for
ever. A call that fails raises `FipcError`, whose `result` is a `Result` (`Result.TIMEOUT`, `Result.DISCONNECTED`,
...). On a connection, one thread at a time sends and one receives; `cancel` may be called from any thread, `close`
only when no other thread is inside a call on the connection. A disconnected connection is final: close it, and
accept or connect a new one. A `Conn` left unclosed is closed when it is garbage-collected (a thread inside a call on
it, or a memoryview of `send_acquire` or `recv_acquire`, keeps it referenced), and so is a `Listener`, which frees its
name; a `Queue` and a `Service` never are.

## Broadcasts

A broadcast sends every message of one publisher to each of its subscribers, in any processes of the user: each
message is stored once, in memory every subscriber maps, and is of one piece (up to a quarter of the capacity).

```python
# publisher.py
import fipc

# 64 KiB of messages, up to 8 subscribers; one that holds the publisher back 5 s is dropped
with fipc.broadcast_create("my_broadcast", 1 << 16, 8, 5000) as publisher:
    publisher.wait_for_members(1)  # waits for the first subscriber
    for i in (1, 2, 3):
        publisher.send(f"tick {i}".encode())
# closing the publisher ends the broadcast: subscribers get DISCONNECTED after the ticks
```

```python
# subscriber.py
import fipc

# gated: misses nothing (the publisher waits for it)
with fipc.broadcast_join("my_broadcast", fipc.SUBSCRIBER_GATED, timeout_ms=5000) as subscriber:
    try:
        while True:
            print(subscriber.recv().decode())  # tick 1, tick 2, tick 3
    except fipc.FipcError as e:
        if e.result != fipc.Result.DISCONNECTED:  # the publisher closed
            raise
```

Run `python publisher.py`, then `python subscriber.py` (or in the other order: `broadcast_join` waits up to its
timeout for the publisher), which prints `tick 1` to `tick 3`. `fipc.broadcast_create(name, capacity,
max_subscribers, max_stall_ms)` returns the publisher, a `Conn` and the broadcast's one handle, which the `with` block
closes.

- **Joining:** `fipc.broadcast_join(name, role)`; the publisher's process admits each subscriber by itself, on a
  thread of the library's. `publisher.wait_for_members(at_least)` waits until that many have joined (at a timeout or
  a cancel, the error's `count` holds the count), so that nothing is sent before they are there.
- **Gated** (`SUBSCRIBER_GATED`): misses nothing, as the publisher waits for it, and may receive in place
  (`recv_acquire`, `recv_release`). One that holds the publisher back for `max_stall_ms` is unsubscribed: its
  receives raise `UNSUBSCRIBED`.
- **Lossy** (`SUBSCRIBER_LOSSY`): never holds the publisher back. One that falls a lap behind gets `OVERRUN` instead
  of a message, and the error's `lost` says how many messages it lost; the next receive goes on from a recent message.
- **The end:** closing the publisher ends the broadcast and frees its name; each subscriber receives what was sent,
  then `DISCONNECTED`. The publisher's garbage collection closes it too: keep it referenced while the broadcast is in
  use.

## Work queues

A work queue takes messages from up to 63 producers and hands each to one of up to 64 consumers, in any processes of
the user. Each producer has a ring of its own; a consumer takes a message of any producer, and the queue promises no
order. Messages are of one piece (up to a quarter of the capacity) and wait in the queue until a consumer takes them.
Each message goes to one consumer, and again only if that consumer's process ends before acknowledging it
(**Delivery**, below).

```python
# producer.py
import fipc

# 4 KiB of messages per producer, 1 producer and 1 consumer: this process owns the queue and is its producer
with fipc.queue_create("my_queue", 4096, 1, 1) as queue:
    with fipc.queue_join("my_queue", fipc.PRODUCER) as producer:
        for i in (1, 2, 3):
            producer.send(f"job {i}".encode())  # waits in the queue until a consumer takes it
        queue.wait_for_members(fipc.CONSUMER, 1)  # waits for the consumer to join
# closing the queue ends it: the consumer gets DISCONNECTED after the jobs
```

```python
# consumer.py
import fipc

with fipc.queue_join("my_queue", fipc.CONSUMER, timeout_ms=5000) as consumer:
    try:
        while True:
            print(consumer.recv().decode())  # job 1, job 2, job 3
    except fipc.FipcError as e:
        if e.result != fipc.Result.DISCONNECTED:  # the queue ended
            raise
```

Run `python producer.py`, then `python consumer.py` (or in the other order: `queue_join` waits up to its timeout for
the queue), which prints `job 1` to `job 3`. `fipc.queue_create(name, capacity, max_producers, max_consumers)`
returns the `Queue`, which the `with` block closes.

- **Joining:** `fipc.queue_join(name, PRODUCER)` or `fipc.queue_join(name, CONSUMER)`, the owner's members too, from
  any thread: the owner's process admits each joiner by itself, on a thread of the library's. The example's producer
  sends first (the jobs wait in the queue), then waits for its consumer, so it works in either start order.
- **The owner's queue:** `queue.wait_for_members(role, at_least)` waits until that many members of the role
  (`PRODUCER`, `CONSUMER`, or `None` for both) have joined (at a timeout or a cancel, the error's `count` holds the
  count); `queue.cancel()` makes its waits raise `CANCELLED`, from any thread. A `Queue` never closes itself (it has
  no finalizer): a queue lives until `close()` or the end of its process, and dropping the object doesn't end it.
- **Receiving:** `recv()`, `recv_into(buf)` (`TOO_LARGE` takes nothing: another consumer may take that message) or in
  place, `recv_acquire()` until `recv_release()`.
- **The end:** after the queue's close, a producer's sends raise `DISCONNECTED`, and each consumer receives what is
  left in the queue, then `DISCONNECTED`. A producer's or a consumer's close only leaves the queue.

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

## Services

A service takes requests from up to 64 clients and hands each to one of up to 64 servers, in any processes of the
user; the server that took a request replies to it, and the reply goes back to the client that sent it. A server waits
for the requests of every client at once (one `recv`), and the servers of a pool share the load: each takes the next
request of any client. Each client has a ring of its own, which holds its requests and the room for their replies, so a
server never waits for a client. Requests and replies are of one piece, 1 to `max_message` bytes, the owner's choice.

```python
# service_server.py
import fipc

# 4 KiB per client, requests and replies of up to 256 bytes, 1 client and 1 server: this process owns the service
# and is its server
with fipc.service_create("my_service", 4096, 256, 1, 1):
    with fipc.service_join("my_service", fipc.SERVER, timeout_ms=5000) as server:
        for _ in range(3):  # three requests, then the end
            request = server.recv()  # the next request of any client
            server.send(request.upper())  # the reply goes to its client, and never waits
# closing the service ends it: the client still receives the replies sent
```

```python
# service_client.py
import fipc

with fipc.service_join("my_service", fipc.CLIENT, timeout_ms=5000) as client:
    for i in (1, 2, 3):
        # sends the request and returns its reply; FipcError (ABANDONED) if the server ended before it replied
        reply = client.call(f"ping {i}".encode())
        print(reply.decode())  # PING 1, PING 2, PING 3
```

Run `python service_server.py`, then `python service_client.py` (or in the other order: `service_join` waits up to its
timeout for the service), which prints `PING 1` to `PING 3`. `fipc.service_create(name, capacity, max_message,
max_clients, max_servers)` returns the `Service`, which the `with` block closes.

- **Joining:** `fipc.service_join(name, CLIENT)` or `fipc.service_join(name, SERVER)`, the owner's members too, from
  any thread: the owner's process admits each joiner by itself, on a thread of the library's. Clients wait while no
  server is joined, and servers while no client is.
- **The owner's service:** `service.wait_for_members(role, at_least)` waits until that many members of the role
  (`CLIENT`, `SERVER`, or `None` for both) have joined (at a timeout or a cancel, the error's `count` holds the count);
  `service.cancel()` makes its waits raise `CANCELLED`, from any thread. A `Service` never closes itself (it has no
  finalizer): a service lives until `close()` or the end of its process, and dropping the object doesn't end it.
- **A client:** `send(request)` sends a request, and `recv()` (`recv_into(buf)`, or in place `recv_acquire()` until
  `recv_release()`) receives the reply to its oldest request whose reply it hasn't received yet: the n-th reply answers
  the n-th request, whichever server replies first. A client may send several requests before it receives (as many as
  its ring has room for: each holds its length rounded up to 64 bytes, plus `max_message` rounded up to 64, until its
  reply has been received), and the servers work on them at once. A receive that times out or is cancelled leaves its
  reply to come: the next receive returns it.
- **`call(request)`** sends a request and returns its reply. A call checks its request first: `INVALID` for an empty
  one, `TOO_LARGE` over `max_piece()`, at once, even while a reply is owed. Then a call on a cancelled connection raises
  `CANCELLED` at once, whatever its timeout, and sends nothing: it could never receive the reply. The connection keeps
  count of the replies still owed: a call whose receive timed out or was cancelled leaves its reply owed, and the next
  call first receives and drops that reply (at most one is owed, since every call drops it first; an `ABANDONED` in its
  place counts too), within its own timeout and before it sends, so a call never returns the reply to another request. A
  call that had to drop an owed reply first sends its request only if its timeout has time left: once a positive timeout
  has run out, it raises `TIMEOUT` without sending, so no server sees that request; with `NO_WAIT` (0) no step waits,
  and it sends if there is room. After a call that gave up, a plain `recv` returns that call's late reply (or
  `ABANDONED`) first: replies come in request order. A call raises `INVALID` while a request sent with `send` still has
  its reply to be received: that reply is yours, so receive it first. A call sends and receives, so no other thread may
  send or receive on the connection meanwhile.
- **A server:** `recv()` takes the next request of any client (each client's in the order it sent them), and
  `send(reply)` replies to it and never waits. A server holds one request at a time: its receives raise `INVALID` until
  it replies, and a reply with no request held raises `INVALID`. A request received in place (`recv_acquire()`) stays
  valid until the reply's `send` or `send_commit`; `recv_release()` does nothing. A reply to a client that has left is
  dropped. A server may receive on one thread and reply on another. Its `close()` answers the request it holds
  `ABANDONED`, and so does the garbage collection of a server's `Conn` left unclosed: a server holds its request, and
  its client waits for the reply, until it is closed (with `with` or `close()`) or garbage-collected (or its process
  ends).
- **`ABANDONED`:** a client's receive (or call) raises `FipcError` with `Result.ABANDONED` when the server that took
  the request ended before it replied: it closed its connection, or its process ended. The request counts as answered,
  and the next receive returns the next reply. An abandoned request may or may not have been carried out; the service
  never sends it again, and the client decides whether to.
- **The end:** after the service's close, clients' sends and servers' receives raise `DISCONNECTED`; a client receives
  the replies that came before the end, then `DISCONNECTED`, and a server's reply raises `DISCONNECTED`. A client's or
  a server's close only leaves the service: a client's requests that no server took are dropped.
- **Not covered:** a server that hangs while its process runs (there are no timeouts, by design: its client waits,
  within its own timeouts); the owner's process, with which the service ends.

## Documentation

- [The website](https://fastipc.github.io/fastipc/python.html): this binding's guide, with examples of every call.
- [`include/fipc.h`](https://github.com/fastipc/fastipc/blob/main/include/fipc.h): the C API under this binding, and every call's contract (results,
  timeouts, threads, messages in pieces, the peer's end).
- [Platform support](https://github.com/fastipc/fastipc/blob/main/docs/platform-support.md), the [protocol](https://github.com/fastipc/fastipc/blob/main/docs/protocol.md) and the
  [benchmarks](https://github.com/fastipc/fastipc/blob/main/docs/perf/bindings-baseline.md).
- [Changelog](https://github.com/fastipc/fastipc/blob/main/CHANGELOG.md), [issues](https://github.com/fastipc/fastipc/issues).

## License

MIT. Copyright (c) 2025-2026 Hayden Donnelly. See [LICENSE](https://github.com/fastipc/fastipc/blob/main/LICENSE).
