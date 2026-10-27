# fipc for Java

Messages between processes on one machine, through shared memory: the Java binding of
[FastIPC](https://github.com/fastipc/fastipc), a small library written in Zig
([`include/fipc.h`](https://github.com/fastipc/fastipc/blob/main/include/fipc.h)). Four kinds, each created on a name
by a static method of `Fipc`:

- **Streams:** a server creates a stream (`Fipc.createStream`) and its `Listener` accepts one client at a time; a
  client connects to the name (`Fipc.connectStream`). The connect waits for the server's `accept`, so a client and its
  server run in different processes or on different threads. Each connection has a shared-memory segment with one ring
  per direction, and reports the peer's end as soon as its process exits or closes the connection.
- **Broadcasts:** one publisher (`Fipc.createBroadcast`) sends every message to each of its subscribers
  (`Fipc.joinBroadcast`).
- **Work queues:** producers and consumers join a queue (`Fipc.createQueue`, `Fipc.joinQueue`), and each message goes
  to one consumer.
- **Services:** clients and servers join a service (`Fipc.createService`, `Fipc.joinService`): each request goes to
  one server, and its reply back to its client.

The peer can be written in any language with a binding: a Java process talks to a Zig, C, C++, Python, C#, Rust, Lua, JavaScript or Go
process exactly as it talks to another Java process (the protocol is the same, and [every pair of languages is
tested](https://fastipc.github.io/fastipc/#interop)).

## Installation

Java 22 or newer (the binding uses the FFM API, `java.lang.foreign`; no JNI, no other dependency).

```kotlin
// build.gradle.kts
dependencies {
    implementation("io.github.fastipc:fipc:1.0.0")
}
```

```xml
<!-- pom.xml -->
<dependency>
  <groupId>io.github.fastipc</groupId>
  <artifactId>fipc</artifactId>
  <version>1.0.0</version>
</dependency>
```

The jar carries the native library for Windows x64 (Windows 11 / Server 2022 or newer) and Linux x64 (glibc 2.34 or
newer: Ubuntu 22.04, Debian 12, RHEL 9 and later), which need an x86-64-v3 CPU (AVX2: Intel Haswell, AMD Zen or
later), for Linux on ARM64 (aarch64, glibc 2.34 or newer) and for macOS 14.4 or newer on Apple Silicon.
It is the module and package `io.github.fastipc`, on the module path or the class path.

### Native access

The binding calls the library through restricted methods of the FFM API, so grant it native access, or the JVM prints
a warning on the first call (and a future JVM may refuse):

| The jar is on | JVM option |
|---|---|
| the class path | `--enable-native-access=ALL-UNNAMED` |
| the module path | `--enable-native-access=io.github.fastipc` |
| the class path of an executable jar | `Enable-Native-Access: ALL-UNNAMED` in that jar's manifest |

With Gradle's `application` plugin: `application { applicationDefaultJvmArgs = listOf("--enable-native-access=ALL-UNNAMED") }`.

## Use

```java
// Server.java
import io.github.fastipc.Connection;
import io.github.fastipc.Fipc;
import io.github.fastipc.Listener;

public class Server {
    public static void main(String[] args) {
        // rings of 1 MiB each way; accept() waits for a client
        try (Listener listener =
                 Fipc.createStream("my_channel", 1 << 20);
             Connection conn = listener.accept()) {
            String text = new String(conn.receive());
            conn.send(text.toUpperCase().getBytes());
        }
    }
}
```

```java
// Client.java
import io.github.fastipc.Connection;
import io.github.fastipc.Fipc;
import java.time.Duration;

public class Client {
    public static void main(String[] args) {
        try (Connection conn = Fipc.connectStream("my_channel",
                 Duration.ofSeconds(5))) {
            conn.send("ping".getBytes());
            byte[] reply = conn.receive();
            System.out.println(new String(reply));  // PING
        }
    }
}
```

Run each in its own terminal, for example with the source launcher:
`java --enable-native-access=ALL-UNNAMED -cp fipc-1.0.0.jar Server.java`. The Python client of the
[repository's README](https://github.com/fastipc/fastipc#examples) works against this server too.

- **Copied messages:** `send` (a `byte[]`, a range of one, a `ByteBuffer` or a `MemorySegment`) and `receive`, which
  returns a new `byte[]`, or fills your `ByteBuffer` or `MemorySegment` and returns the length. A message of any size
  may be sent (at least 1 byte); it travels in pieces and arrives whole. Native memory (a direct `ByteBuffer`, a
  native `MemorySegment`) goes to the library as it is; heap memory is copied through a native buffer.
- **Zero-copy** (messages of one piece, up to `maxPiece()` bytes): `sendAcquire(length)` returns a `MemorySegment`
  in the ring, write the message into it, `sendCommit(length)`; `receiveAcquire()` returns the message in the ring,
  read-only, read it, `receiveRelease()`.
- **Batches:** `sendMany(messages, [timeout])` sends a `List<byte[]>`, published together with at most one wake of the
  receivers, and returns how many went (a `FipcException`'s `sentMessages()` says how many went before it);
  `receiveMany(maxCount, [timeout])` waits for the next message, then takes every further one already there, and
  returns a `List<byte[]>` (`receiveMany(segment, lengths, timeout)` packs them into your `MemorySegment`). On every
  kind: a broadcast's subscriber takes those its publisher has moved past, and a service's client sends a batch of
  requests (owed as its sends are: `call` throws `INVALID` until their replies are received) and receives their
  replies in order; a server's batches throw `INVALID`. Batch both ends: a receiver that takes one message per call
  falls behind a batching sender.

### Broadcasts

A broadcast sends every message of one publisher to each of its subscribers, in any processes of the user: each
message is stored once, in memory every subscriber maps, and is of one piece (up to a quarter of the capacity, the
connections' `maxPiece()`).

```java
// Publisher.java
import io.github.fastipc.Connection;
import io.github.fastipc.Fipc;
import java.time.Duration;

public class Publisher {
    public static void main(String[] args) {
        // 64 KiB of messages, up to 8 subscribers; one that
        // holds the publisher back 5 s is dropped
        try (Connection publisher = Fipc.createBroadcast(
                 "my_broadcast", 1 << 16, 8, Duration.ofSeconds(5))) {
            publisher.waitForMembers(1);  // waits for the first one
            for (int i = 1; i <= 3; i++) {
                publisher.send(("tick " + i).getBytes());
            }
        }
        // closing the publisher ends the broadcast: subscribers
        // get DISCONNECTED after the ticks
    }
}
```

```java
// Subscriber.java
import io.github.fastipc.Connection;
import io.github.fastipc.Fipc;
import io.github.fastipc.FipcException;
import io.github.fastipc.Result;
import io.github.fastipc.SubscriberRole;
import java.time.Duration;

public class Subscriber {
    public static void main(String[] args) {
        // gated: misses nothing (the publisher waits for it)
        try (Connection sub = Fipc.joinBroadcast("my_broadcast",
                 SubscriberRole.GATED, Duration.ofSeconds(5))) {
            while (true) {
                byte[] tick = sub.receive();
                System.out.println(new String(tick));  // tick 1..3
            }
        } catch (FipcException e) {
            // DISCONNECTED: the publisher closed
            if (e.result() != Result.DISCONNECTED) throw e;
        }
    }
}
```

Run the publisher, then the subscriber (or in the other order: `joinBroadcast` waits up to its timeout for the
publisher), which prints `tick 1` to `tick 3`. `Fipc.createBroadcast(name, capacity, maxSubscribers, maxStall)` returns
the publisher's `Connection`, the broadcast's one handle, whose sends go to every subscriber.

- **Joining:** subscribers join with `Fipc.joinBroadcast(name, role)`; the publisher's process admits each one by
  itself, on a thread of the library's, so no thread of yours waits for them. `publisher.waitForMembers(atLeast,
  timeout)` waits until that many have joined and returns the count, so that nothing is sent before they are there; at
  a `TIMEOUT` or `CANCELLED` the exception's `memberCount()` holds the count. `publisher.waitForMembers()` returns it at
  once.
- **Gated** (`SubscriberRole.GATED`): misses nothing, as the publisher waits for it, and may receive in place
  (`receiveAcquire()`, `receiveRelease()`). One that holds the publisher back for `maxStall` is unsubscribed: its
  receives throw `UNSUBSCRIBED`, and a message it holds in place is gone (release it promptly).
- **Lossy** (`SubscriberRole.LOSSY`): never holds the publisher back, and receives copies only. One that falls a lap
  behind gets `OVERRUN` instead of a message: the exception's `lostMessages()` (a `long`) says how many messages it
  lost, and the next receive goes on from a recent message.
- **The end:** closing the publisher ends the broadcast and frees the name; each subscriber receives what was sent,
  then `DISCONNECTED`.

### Work queues

A work queue hands each message of up to 63 producers to one of up to 64 consumers, in any processes of the user. Each
producer has a ring of its own; a consumer takes a message of any producer, and the queue promises no order. Messages
are of one piece (up to a quarter of the capacity, `maxPiece()`) and wait in the
queue until a consumer takes them.

```java
// Producer.java
import io.github.fastipc.Connection;
import io.github.fastipc.Fipc;
import io.github.fastipc.QueueRole;
import io.github.fastipc.WorkQueue;

public class Producer {
    public static void main(String[] args) {
        // 4 KiB of messages per producer, 1 producer and 1
        // consumer: this process owns the queue and is its
        // producer
        try (WorkQueue queue = Fipc.createQueue("my_queue", 4096,
                 1, 1)) {
            try (Connection producer = Fipc.joinQueue("my_queue",
                     QueueRole.PRODUCER)) {
                for (int i = 1; i <= 3; i++) {
                    // waits in the queue until a consumer takes it
                    producer.send(("job " + i).getBytes());
                }
                // waits for the consumer to join
                queue.waitForMembers(QueueRole.CONSUMER, 1);
            }
        }
        // closing the queue ends it: the consumer gets
        // DISCONNECTED after the jobs
    }
}
```

```java
// Consumer.java
import io.github.fastipc.Connection;
import io.github.fastipc.Fipc;
import io.github.fastipc.FipcException;
import io.github.fastipc.QueueRole;
import io.github.fastipc.Result;
import java.time.Duration;

public class Consumer {
    public static void main(String[] args) {
        try (Connection consumer = Fipc.joinQueue("my_queue",
                 QueueRole.CONSUMER, Duration.ofSeconds(5))) {
            while (true) {
                byte[] job = consumer.receive();
                System.out.println(new String(job));  // job 1..3
            }
        } catch (FipcException e) {
            // DISCONNECTED: the queue ended
            if (e.result() != Result.DISCONNECTED) throw e;
        }
    }
}
```

Run the producer, then the consumer (or in the other order: `joinQueue` waits up to its timeout for the queue), which
prints `job 1` to `job 3`. `Fipc.createQueue(name, capacity, maxProducers, maxConsumers)` returns the `WorkQueue`.

- **Joining:** producers and consumers join with `Fipc.joinQueue(name, QueueRole.PRODUCER)` or `QueueRole.CONSUMER`,
  the owner's own too, from any thread: the owner's process admits each one by itself, on a thread of the library's.
  `queue.waitForMembers(role, atLeast, timeout)` waits until that many members of the role have joined (without a
  role, producers and consumers together) and returns the count; at a `TIMEOUT`, `CANCELLED` or `DISCONNECTED` the
  exception's `memberCount()` holds it. `queue.cancel()` makes a wait throw `CANCELLED`, from any thread.
- **Producers** send (`send`, or in place with `sendAcquire`/`sendCommit`), waiting for room while the consumers
  haven't taken their earlier messages. **Consumers** receive (`receive`, or in place with
  `receiveAcquire`/`receiveRelease`). A producer's receives and a consumer's sends throw `INVALID`.
- **The end:** `queue.close()` ends the queue and frees the name, and so does the end of the owner's process:
  producers' sends throw `DISCONNECTED`, and consumers receive what is left, then `DISCONNECTED`. A producer's or a
  consumer's close never ends it.
- **Lifetime:** a queue lives until `close()` or the end of its process; dropping the object doesn't end it. Unlike the
  binding's other handles, a `WorkQueue` has no `Cleaner`: its use lives in its members, in this process and in
  others, so the garbage collector never ends it. One that is never closed holds its name until the process ends, and
  another `createQueue` of the name then throws `ADDR_IN_USE`.

**Delivery.** Each message goes to one consumer. It goes again, to another consumer, only if the process of the consumer
that took it ends before acknowledging it, and when a consumer acknowledges is its own choice, not the queue's. A
message received in place is acknowledged at its release (or the consumer's next receive, or its close).

| How the consumer receives | When it acknowledges | If its process ends | What the application gets |
|---|---|---|---|
| `receive()` (copying) | inside the call, before it returns | inside the call: the message goes to another consumer, and this one never saw it; after the call returned: it doesn't go again | **at most once**: no duplicates |
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

### Services

A service takes requests from up to 64 clients and hands each to one of up to 64 servers, in any processes of the
user; the server that took a request replies to it, and the reply goes back to the client that sent it. A server waits
for the requests of every client at once (one `receive`), and the servers of a pool share the load: each takes the
next request of any client. Each client has a ring of its own, which holds its requests and the room for their replies,
so a server never waits for a client. Requests and replies are of one piece, 1 to `maxMessage` bytes, the owner's
choice (the connections' `maxPiece()`).

```java
// ServiceServer.java
import io.github.fastipc.Connection;
import io.github.fastipc.Fipc;
import io.github.fastipc.Service;
import io.github.fastipc.ServiceRole;
import java.time.Duration;

public class ServiceServer {
    public static void main(String[] args) {
        // 4 KiB per client, requests and replies of up to 256
        // bytes, 1 client and 1 server: this process owns the
        // service and is its server
        try (Service service = Fipc.createService("my_service",
                 4096, 256, 1, 1)) {
            try (Connection server = Fipc.joinService("my_service",
                     ServiceRole.SERVER, Duration.ofSeconds(5))) {
                // three requests, then the end
                for (int i = 1; i <= 3; i++) {
                    // the next request of any client
                    String request = new String(server.receive());
                    // the reply goes to its client, and never waits
                    server.send(request.toUpperCase().getBytes());
                }
            }
        }
        // closing the service ends it: the client still
        // receives the replies sent
    }
}
```

```java
// ServiceClient.java
import io.github.fastipc.Connection;
import io.github.fastipc.Fipc;
import io.github.fastipc.ServiceRole;
import java.time.Duration;

public class ServiceClient {
    public static void main(String[] args) {
        try (Connection client = Fipc.joinService("my_service",
                 ServiceRole.CLIENT, Duration.ofSeconds(5))) {
            for (int i = 1; i <= 3; i++) {
                // sends the request and returns its reply; throws
                // FipcException (ABANDONED) if the server ended
                // before it replied
                byte[] reply = client.call(("ping " + i).getBytes());
                System.out.println(new String(reply));  // PING 1..3
            }
        }
    }
}
```

Run the server, then the client (or in the other order: `joinService` waits up to its timeout for the service), which
prints `PING 1` to `PING 3`. `Fipc.createService(name, capacity, maxMessage, maxClients, maxServers)` returns the
`Service`.

- **Joining:** clients and servers join with `Fipc.joinService(name, ServiceRole.CLIENT)` or `ServiceRole.SERVER`, the
  owner's own too, from any thread: the owner's process admits each joiner by itself, on a thread of the library's.
  Clients wait while no server is joined, and servers while no client is.
- **The owner's service:** `service.waitForMembers(role, atLeast, timeout)` waits until that many members of the role
  have joined (without a role, clients and servers together) and returns the count; at a `TIMEOUT`, `CANCELLED` or
  `DISCONNECTED` the exception's `memberCount()` holds it. `service.cancel()` makes a wait throw `CANCELLED`, from any
  thread. A `Service`, as a `WorkQueue`, has no `Cleaner`: a service lives until `close()` or the end of its process,
  and dropping the object doesn't end it.
- **A client:** `send(request)` sends a request, and `receive` (into a new array, a buffer or a segment, or in place
  `receiveAcquire` until `receiveRelease`) receives the reply to its oldest request whose reply it hasn't received yet:
  the n-th reply answers the n-th request, whichever server replies first. A client may send several requests before
  it receives (as many as its ring has room for: each holds its length rounded up to 64 bytes, plus `maxMessage`
  rounded up to 64, until its reply has been received), and the servers work on them at once. A receive that times out
  or is cancelled leaves its reply to come: the next receive returns it.
- **`call(request, timeout)`** sends a request and returns its reply. A call checks its request first: `INVALID` for an
  empty one, `TOO_LARGE` over `maxPiece()`, at once, even while a reply is owed. Then a call on a cancelled connection
  throws `CANCELLED` at once, whatever its timeout, and sends nothing: it could never receive the reply. The connection
  keeps count of the replies still owed: a call whose receive timed out or was cancelled leaves its reply owed, and the
  next call first receives and drops that reply (at most one is owed, since every call drops it first; an `ABANDONED` in
  its place counts too), within its own timeout and before it sends, so a call never returns the reply to another
  request. A call that had to drop an owed reply first sends its request only if its timeout has time left: once a
  positive timeout has run out, it throws `TIMEOUT` without sending, so no server sees that request; with a zero timeout
  no step waits, and it sends if there is room. After a call that gave up, a plain receive returns that call's late
  reply (or `ABANDONED`) first: replies come in request order. A call throws `INVALID` while a request sent with `send`
  (or `sendCommit`) still has its reply to be received: that reply is yours, so receive it first; on any connection
  other than a service's client it throws `INVALID` too. A call sends and receives, so no other thread may send or
  receive on the connection meanwhile.
- **A server:** `receive` takes the next request of any client (each client's in the order it sent them), and
  `send(reply)` replies to it and never waits. A server holds one request at a time: its receives throw `INVALID`
  until it replies, and a reply with no request held throws `INVALID`. A request received in place (`receiveAcquire`)
  stays the server's until the reply's `send` or `sendCommit`, and the reply may be written from it in place. The
  reply invalidates the segment, as do `receiveRelease` (which leaves the request held until its reply), the next
  receive and `close()` on the receiving thread. Another thread can't invalidate a segment confined to the receiving
  thread, so its reply throws `WrongThreadException`, sending nothing, while the segment is valid: to reply on another
  thread, take what the reply needs from the segment and `receiveRelease()` it first. A held segment keeps the
  connection open: after a `close()` on another thread, the connection closes at the segment's end. A reply to a client
  that has left is dropped. A server may receive on one thread and reply on another.
- **`ABANDONED`:** a client's receive (or call) throws `FipcException` with `Result.ABANDONED` when the server that
  took the request ended before it replied: it closed its connection, or its process ended. The request counts as
  answered, and the next receive returns the next reply. An abandoned request may or may not have been carried out;
  the service never sends it again, and the client decides whether to.
- **The end:** after the service's `close()`, clients' sends and servers' receives throw `DISCONNECTED`; a client
  receives the replies that came before the end, then `DISCONNECTED`, and a server's reply throws `DISCONNECTED`. A
  client's or a server's close only leaves the service: a client's requests that no server took are dropped, and the
  request a server holds, if it hasn't replied, is answered `ABANDONED` (one held in place, at its segment's end).
- **Not covered:** a server that hangs while its process runs (there are no timeouts, by design: its client waits,
  within its own timeouts); the owner's process, with which the service ends.

### Results and timeouts

A call that doesn't succeed throws `FipcException`, an unchecked exception whose `result()` is a `Result`: `TIMEOUT`,
`DISCONNECTED` (the peer's end; final: close the connection, and accept or connect a new one), `CANCELLED`, `TOO_LARGE`
(a receive's `messageLength()` says how much room the message needs; it stays queued), `INVALID`, `NO_MEMORY`,
`ADDR_IN_USE`, on a broadcast `OVERRUN` (`lostMessages()`: the messages a lossy subscriber lost) or `UNSUBSCRIBED`,
and on a service's client `ABANDONED`. A call on a closed listener, connection, queue or service throws
`IllegalStateException`.

Every call that can wait has an overload without a timeout, which waits for ever, and one with a `java.time.Duration`:
`Fipc.NO_WAIT` (`Duration.ZERO`) doesn't wait, `Fipc.FOREVER` waits for ever.

```java
byte[] msg;
try {
    msg = conn.receive(Duration.ofMillis(100));
} catch (FipcException e) {
    if (e.result() != Result.TIMEOUT) throw e;
    msg = null;                    // nothing yet
}
```

### Threads and close

On a connection, one thread at a time sends and one receives; the two may run at once. `cancel()` and `close()` may
be called from any thread, at any time. `close()` cancels the connection, so a call that waits throws `CANCELLED`, and
the native close runs once the calls in progress have returned: no call ever runs on a closed connection. So stopping
a reader thread is: `close()`, then join the thread. A listener or connection that is never closed is closed by a
`Cleaner` once it is unreachable (a work queue or a service never is); close them explicitly, with
try-with-resources.

A zero-copy segment is valid until its commit or release (or the next call on its side, which drops or releases it),
holds the connection open until then, and is confined to the thread that acquired it: touching it afterwards, or from
another thread, throws instead of reaching memory that is no longer the caller's.

Waits are not interruptible (`Thread.interrupt()` doesn't wake a call; `cancel()` and `close()` do), and a call that
waits blocks its thread in native code, which pins a virtual thread to its carrier: use platform threads for calls
that wait long.

### The native library

The binding loads, in this order: the file the system property `fastipc.library.path` names; in a development
checkout of the repository, its `zig-out` build; the jar's library, extracted once per content into a cache folder of
the user (`%LOCALAPPDATA%\fipc` on Windows, `$XDG_CACHE_HOME/fipc` or `~/.cache/fipc` on Linux,
`~/Library/Caches/fipc` on macOS) and checked against the jar's copy before each load; else the system's search
(`PATH`, `LD_LIBRARY_PATH`, `DYLD_LIBRARY_PATH`).
`Fipc.libraryPath()` says which one was loaded. The library is never unloaded.

### Performance

Between two processes, copied 16-byte messages run at 14 to 15 million per second through the binding on the
baseline machine, and 64 KiB messages at about 420,000 to 430,000 per second (`bench/java`; the numbers are in
[`docs/perf/bindings-baseline.md`](https://github.com/fastipc/fastipc/blob/main/docs/perf/bindings-baseline.md)).
In one process with no waiting, a 16-byte message sent and received costs about 85 ns on the baseline machine
(Windows and Linux), against about 40 ns for the C API alone: each call holds the handle (two atomic operations) and
goes through an FFM downcall. A zero-copy message costs about a third more at that size (its segments' lifetimes).

The JIT compiles the hot path only after a few hundred milliseconds of use, so measure after a warm-up, and keep the
warm-up on the same paths as the measurement: a branch the compiled code hasn't seen before deoptimizes it, and the
messages after it run in the interpreter until it is compiled again. `python devtool.py bench java-fastipc` shows how.

## Building from source

In a checkout of the repository, with the library built (`python devtool.py build`):

```bash
cd bindings/java
./gradlew test       # the integration tests (JUnit 5), against the repository's zig-out build
./gradlew jar        # build/libs/fipc-1.0.0.jar, without native libraries (it loads zig-out's in the checkout)
```

The wrapper runs on any JDK 17 or newer; the build compiles for Java 22 with a JDK 25 toolchain, which Gradle downloads
when none is installed. `python devtool.py package java --smoke` builds the jar with every platform's native library,
its sources, javadoc and POM in `zig-out/packages/maven` (a folder in the Maven repository layout), checks them, and
makes a round trip through them from a fresh Gradle project.

## Documentation

- [The website](https://fastipc.github.io/fastipc/java.html): this binding's guide, with examples of every call.
- The javadoc, in the `-javadoc.jar` next to the artifact.
- [`include/fipc.h`](https://github.com/fastipc/fastipc/blob/main/include/fipc.h): the C API under this binding,
  and every call's contract (results, timeouts, threads, messages in pieces, the peer's end).
- [Platform support](https://github.com/fastipc/fastipc/blob/main/docs/platform-support.md) and the
  [protocol](https://github.com/fastipc/fastipc/blob/main/docs/protocol.md).
- [Changelog](https://github.com/fastipc/fastipc/blob/main/CHANGELOG.md),
  [issues](https://github.com/fastipc/fastipc/issues).

## License

MIT. Copyright (c) 2025-2026 Hayden Donnelly. See [LICENSE](https://github.com/fastipc/fastipc/blob/main/LICENSE).
