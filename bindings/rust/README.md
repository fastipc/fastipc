# fipc for Rust

[![crates.io](https://img.shields.io/crates/v/fipc.svg)](https://crates.io/crates/fipc)
[![docs.rs](https://img.shields.io/docsrs/fipc)](https://docs.rs/fipc)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/fastipc/fastipc/blob/main/LICENSE)

Messages between processes on one machine, through shared memory: the Rust binding of
[FastIPC](https://github.com/fastipc/fastipc), a small library written in Zig
([`include/fipc.h`](https://github.com/fastipc/fastipc/blob/main/include/fipc.h)). Four kinds, each a module:
a [stream](#use) (`fipc::stream`) connects a server and its clients: the server creates a name and accepts
one client at a time, a client connects to the name, and `connect` waits for the server's `accept`, so a client and
its server run in different processes or on different threads. Each connection has a shared-memory segment with one
ring per direction, and reports the peer's end as soon as its process exits or drops the connection. A
[broadcast](#broadcasts) (`fipc::broadcast`) sends one publisher's messages to many subscribers, a
[work queue](#work-queues) (`fipc::queue`) hands each message of many producers to one of many consumers, and a
[service](#services) (`fipc::service`) hands each request of many clients to one of a pool of servers, and its reply
back to its client.

The peer can be written in any language with a binding: a Rust process talks to a Zig, C, C++, Python, C#, Java, Lua, JavaScript or Go
process exactly as it talks to another Rust process (the protocol is the same, and [every pair of languages is
tested](https://fastipc.github.io/fastipc/#interop)). Use it where two local processes must talk at memory speed: a
game and its editor or tools, a UI and its engine, a Python front end and a Rust service.

## Installation

```toml
[dependencies]
fipc = "1.0"
```

Rust 1.85 or newer. Two crates: `fipc` (`use fipc::...`), the safe API, and
[`fipc-sys`](https://crates.io/crates/fipc-sys), the raw FFI under it (re-exported as
`fipc::sys`). No other dependencies. The `-sys` crate carries the native library for Windows x64 (Windows 11 /
Server 2022 or newer, MSVC toolchain) and Linux x64 (glibc 2.34 or newer: Ubuntu 22.04, Debian 12, RHEL 9 and later),
which need an x86-64-v3 CPU (AVX2: Intel Haswell, AMD Zen or later), for Linux on ARM64 (`aarch64-unknown-linux-gnu`,
glibc 2.34 or newer) and for macOS 14.4 or newer on Apple Silicon (`aarch64-apple-darwin`), and links it. `cargo run`
and `cargo test` find the library by themselves; a program run on its own needs it next to it (see
[Shipping your program](#shipping-your-program)).

## Use

```rust
// server.rs
fn main() -> fipc::Result<()> {
    let mut listener = fipc::stream::create("my_channel", 1 << 20)?; // rings of 1 MiB each way
    let mut conn = listener.accept(None)?; // waits for a client
    let message = conn.receive(None)?;
    conn.send(&message.to_ascii_uppercase(), None)?;
    Ok(())
}
```

```rust
// client.rs
use std::time::Duration;

fn main() -> fipc::Result<()> {
    let mut conn = fipc::stream::connect("my_channel", Duration::from_secs(5))?;
    conn.send(b"ping", None)?;
    println!("{}", String::from_utf8_lossy(&conn.receive(None)?)); // PING
    Ok(())
}
```

Run each in its own terminal; in a checkout of the repository they are the crate's examples: `cargo run --example
server` and `cargo run --example client`. The Zig, C, C++, Python, C#, Java, Lua, JavaScript and Go clients of the
[repository's README](https://github.com/fastipc/fastipc#examples) work against this server too, and this client
against their servers.

- **Copied messages:** `send(&[u8])` and `receive()`, which returns a new `Vec<u8>`, or `receive_into(&mut [u8])`,
  which fills your buffer and returns the length. A message of any size may be sent (at least 1 byte); it travels in
  pieces and arrives whole.
- **Zero-copy** (messages of one piece, up to `max_piece()` bytes): `send_acquire(len)` returns a `SendSlot` in the
  ring (it derefs to `[u8]`): write the message into it, then `commit(len)`. `receive_acquire()` returns a
  `ReceiveSlot` that derefs to the message in the ring and frees its room when dropped. A slot borrows its side of the
  connection, so the borrow checker keeps it from outliving its turn. Dropping a `SendSlot` without committing it
  sends nothing.
- **Batches:** `send_many(&messages, timeout)` sends a slice of messages, published together with at most one wake of
  the receivers, and returns how many went (a `SendManyError` holds the first message's `error` and how many were
  `sent`); `receive_many(&mut buf, &mut lens, timeout)` waits for the next message, then takes every further one
  already there that fits `buf`, packed back to back, their lengths in `lens`, and returns how many
  (`fipc::batch_messages` walks them). On every kind: a broadcast's subscriber takes those its publisher has moved
  past, and a service's client sends a batch of requests (owed as its sends are: `call` returns `Error::Invalid` until
  their replies are received) and receives their replies in order; a server's batches return `Error::Invalid`. Batch
  both ends: a receiver that takes one message per call falls behind a batching sender.

```rust
let batch: [&[u8]; 3] = [b"one", b"two", b"three"];
let sent = conn.send_many(&batch, None)?; // published together, at most one wake
let mut buf = vec![0; 1 << 16];
let mut lens = [0; 64];
let count = peer.receive_many(&mut buf, &mut lens, None)?; // every message already there
for message in fipc::batch_messages(&buf, &lens[..count]) {
    handle(message);
}
```

### Results and timeouts

Every call that can fail returns `fipc::Result<T>`; the `Error` enum mirrors the C API's results: `Timeout`,
`Disconnected` (the peer's end; final: drop the connection, and accept or connect a new one), `Cancelled`,
`TooLarge { len }` (a receive's `len` says how much room the message needs; it stays queued), `Invalid`, `NoMemory`,
`AddrInUse`, on a broadcast `Overrun { lost }` (the messages a lossy subscriber lost) and `Unsubscribed`, and on a
service's client `Abandoned` (the server that took the request ended before it replied). On a broadcast,
`Disconnected` is the publisher's end; on a work queue, the queue's; on a service, the service's.
It implements `std::error::Error`, displays the C name (`FIPC_TIMEOUT: the timeout ran out`) and converts
into `std::io::Error`.

Every call that can wait takes its timeout last, as anything that converts into an `Option<Duration>`: a `Duration`,
`NO_WAIT` (doesn't wait), or `None` / `FOREVER` (waits as long as it takes).

```rust
match conn.receive(Duration::from_millis(100)) {
    Ok(message) => handle(&message),
    Err(Error::Timeout) => {} // nothing yet
    Err(e) => return Err(e),  // Error::Disconnected: the peer is gone
}
```

### Threads

`Listener`, `Connection` and its halves, `Queue` and `Service` are `Send` and `Sync`. The calls that send or receive take `&mut self`: the C
API allows one thread at a time to send and one to receive, and the borrow checker keeps it so. To send and receive on
two threads at once, `split()` the connection into its `Sender` and `Receiver` (borrowed, for scoped threads) or
`into_split()` it (owned); the connection closes when the last half is dropped.

To stop a thread that waits, take a `Canceler` first (`conn.canceler()`, `listener.canceler()`, `queue.canceler()`,
`service.canceler()`; `Clone`, `Send`, `Sync`) and call `cancel()` from any thread: every call that waits, then or later, returns
`Error::Cancelled`. A canceler doesn't keep its handle open; once that is dropped, cancelling does nothing.

```rust
let canceler = conn.canceler();
let reader = std::thread::spawn(move || {
    // until Error::Cancelled (or Error::Disconnected)
    while let Ok(message) = conn.receive(None) {
        handle(&message);
    }
});
// ...
canceler.cancel(); // wakes the reader: Cancelled
reader.join().unwrap();
```

Dropping a connection ends it: the peer gets `Error::Disconnected` once it has received the messages this side
completed. There is no heartbeat and no timeout: a paused or hung peer is not reported, a crashed or killed one as
soon as its process has ended.

### Games and frame loops

A frame loop shouldn't block on the connection. Either poll it once a frame with `NO_WAIT` (in Bevy, keep the
`Connection` in a `Resource` and poll it in a system), or give its `Receiver` to a thread that forwards each message
through a channel the frame loop drains:

```rust
// Sending on this thread, receiving on another: the halves of the connection
let (mut sender, mut receiver) = conn.into_split();
let reader = std::thread::spawn(move || {
    while let Ok(message) = receiver.receive(None) {
        if tx.send(message).is_err() {
            break; // the frame loop is gone
        }
    }
});
// Each frame: whatever has arrived, without waiting
sender.send(b"update", None)?;
```

The example `game_loop` polls an echo server once a frame: `cargo run --example echo_server`, then
`cargo run --example game_loop`.

### Broadcasts

A broadcast sends every message of one publisher to each of its subscribers (up to 1024), in any processes of the
user: each message is stored once, in memory every subscriber maps, and is of one piece (up to a quarter of the
capacity, the connections' `max_piece()`).

```rust
// publisher.rs
use std::time::Duration;

fn main() -> fipc::Result<()> {
    // 64 KiB of messages, up to 8 subscribers; one that holds the publisher back 5 s is dropped
    let mut publisher = fipc::broadcast::create("my_broadcast", 1 << 16, 8, Duration::from_secs(5))?;
    publisher.wait_for_members(1, None)?; // waits for the first subscriber
    for i in 1..=3 {
        publisher.send(format!("tick {i}").as_bytes(), None)?;
    }
    Ok(())
} // dropping the publisher ends the broadcast: subscribers get Error::Disconnected after the ticks
```

```rust
// subscriber.rs
use fipc::{Error, SubscriberRole};
use std::time::Duration;

fn main() -> fipc::Result<()> {
    // gated: misses nothing (the publisher waits for it)
    let mut subscriber = fipc::broadcast::join("my_broadcast", SubscriberRole::Gated, Duration::from_secs(5))?;
    loop {
        match subscriber.receive(None) {
            Ok(tick) => println!("{}", String::from_utf8_lossy(&tick)), // tick 1, tick 2, tick 3
            Err(Error::Disconnected) => return Ok(()),                  // the publisher closed
            Err(e) => return Err(e),
        }
    }
}
```

Run the publisher, then the subscriber (`cargo run --example publisher`, then `cargo run --example subscriber` in a
checkout), or the other way round: `join` waits up to its timeout for the publisher. The subscriber prints `tick 1`
to `tick 3`. `fipc::broadcast::create(name, capacity, max_subscribers, max_stall)` returns the publisher, a
`Connection` whose sends go to every subscriber, and the broadcast's one handle.

- **Joining:** a subscriber joins with `fipc::broadcast::join(name, SubscriberRole::Gated or Lossy, timeout)`; the
  publisher's process admits it on a thread of the library's, so no thread of the publisher's waits for it.
  `publisher.wait_for_members(at_least, timeout)` waits until that many have joined and returns the count, so that
  nothing is sent before they are there; it takes `&self`, so any thread may call it. It returns `Result<u32,
  MembersError>`: at a `Timeout` or `Cancelled`, the error's `count` is the number joined; it converts into an `Error`
  for `?`.
- **Gated** (`SubscriberRole::Gated`): misses nothing, as the publisher waits for it. One that holds the publisher back
  for the broadcast's `max_stall` is unsubscribed: its receives return `Error::Unsubscribed`, and the broadcast's
  memory becomes inaccessible to it, at a moment the publisher's clock picks. So its in-place receive is `unsafe`:
  `receive_acquire_gated()`, whose caller promises the slot is dropped before it has held the publisher back for
  `max_stall`, or that the broadcast was created with a `max_stall` of `None`, which never unsubscribes (the safe
  `receive_acquire()` returns `Error::Invalid` on a subscriber). `NO_WAIT` as `max_stall` refuses gated subscribers.
- **Lossy** (`SubscriberRole::Lossy`): never holds the publisher back, and receives copies only. One that falls a lap
  behind gets `Error::Overrun { lost }` instead of a message, `lost` (a `u64`) being the number of messages it lost,
  and the next receive goes on from a recent message:

```rust
match subscriber.receive(None) {
    Ok(message) => handle(&message),
    // Fell a lap behind: the next receive goes on from a recent message
    Err(Error::Overrun { lost }) => println!("{lost} messages lost"),
    Err(e) => return Err(e), // Error::Disconnected: the broadcast ended
}
```

- **The end:** dropping the publisher ends the broadcast and frees the name; each subscriber receives what was sent,
  then `Error::Disconnected`.

### Work queues

A work queue takes messages from up to 63 producers and hands each to one of up to 64 consumers, in any processes of
the user. Each producer has a ring of its own in the queue's memory; a consumer takes a message of any producer: the
queue promises no order. Messages are of one piece (up to a quarter of the capacity, the connections' `max_piece()`)
and wait in the queue until a consumer takes them, so a producer may send before any consumer has joined.

```rust
// producer.rs
use fipc::QueueRole;
use std::time::Duration;

fn main() -> fipc::Result<()> {
    // 4 KiB of messages per producer, 1 producer and 1 consumer: this process owns the queue and is its producer
    let queue = fipc::queue::create("my_queue", 4096, 1, 1)?;
    let mut producer = fipc::queue::join("my_queue", QueueRole::Producer, Duration::from_secs(5))?;
    for i in 1..=3 {
        producer.send(format!("job {i}").as_bytes(), None)?; // waits in the queue until a consumer takes it
    }
    queue.wait_for_members(QueueRole::Consumer, 1, None)?; // waits for the consumer to join
    Ok(())
} // dropping the producer, then the queue, ends the queue: the consumer gets Error::Disconnected after the jobs
```

```rust
// consumer.rs
use fipc::{Error, QueueRole};
use std::time::Duration;

fn main() -> fipc::Result<()> {
    let mut consumer = fipc::queue::join("my_queue", QueueRole::Consumer, Duration::from_secs(5))?;
    loop {
        match consumer.receive(None) {
            Ok(job) => println!("{}", String::from_utf8_lossy(&job)), // job 1, job 2, job 3
            Err(Error::Disconnected) => return Ok(()),                // the queue ended
            Err(e) => return Err(e),
        }
    }
}
```

Run the producer, then the consumer (`cargo run --example producer`, then `cargo run --example consumer` in a
checkout), or the other way round: `join` waits up to its timeout for the owner. The consumer prints `job 1` to
`job 3`. `fipc::queue::create(name, capacity, max_producers, max_consumers)` returns the queue's handle, a `Queue`.

- **Joining:** producers and consumers join with `fipc::queue::join(name, QueueRole::Producer, timeout)` or
  `QueueRole::Consumer`, from any process or thread, the owner's included: the owner's process admits each one on a
  thread of the library's. A producer only sends (copied, or zero-copy with `send_acquire`), and a consumer only
  receives: copied (`receive`, `receive_into`) or in place (`receive_acquire`, the message held until the slot is
  dropped). The other calls return `Error::Invalid`.
- **Counting:** `queue.wait_for_members(role, at_least, timeout)` waits until that many members of the role
  (`QueueRole::Producer`, `QueueRole::Consumer`, or `None` for both) have joined and returns the count, with a
  `MembersError` as a publisher's; `queue.cancel()` or a `queue.canceler()` ends such a wait from any thread.
- **Too short a buffer:** a `receive_into` whose buffer is too short returns `TooLarge { len }` and takes nothing:
  another consumer may take that message first (a buffer of `max_piece()` bytes takes every message). `receive` grows
  its buffer and receives again, within what is left of its timeout.
- **A queue lives until its `Queue` is closed or dropped, or the owner's process ends.** Then producers' sends return
  `Error::Disconnected`, and consumers receive every message left in the queue, then `Error::Disconnected`. Keep the
  `Queue` for as long as the queue is to run: `queue.close()` or its drop ends it, at a place you see, and nothing
  else does (no garbage collector; a `Canceler` doesn't keep it alive). Binding it to `_`, or not at all, drops it,
  and the queue ends at once. A producer's or a consumer's drop, or the end of its process, never
  ends the queue; the members' connections stay open after it ends.

**Delivery.** Each message goes to one consumer. It goes again, to another consumer, only if the process of the consumer
that took it ends before acknowledging it, and when a consumer acknowledges is its own choice, not the queue's. A
message received in place is acknowledged at its release (or the consumer's next receive, or its close).

| How the consumer receives | When it acknowledges | If its process ends | What the application gets |
|---|---|---|---|
| `receive()` or `receive_into()` (copying) | inside the call, before it returns | inside the call: the message goes to another consumer, and this one never saw it; after the call returned: it doesn't go again | **at most once**: no duplicates |
| `receive_acquire()`, then the slot's `release()` (or its drop) at once, then the work | at the release, before the work | as above | **at most once**: no duplicates |
| `receive_acquire()`, the work, then the slot's `release()` (or its drop) | at the release, after the work | before the release: the message goes to another consumer | **at least once**: a duplicate is possible, so the work must be idempotent |

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
so a server never waits for a client. Requests and replies are of one piece, 1 to `max_message` bytes, the owner's
choice (the connections' `max_piece()`).

```rust
// service_server.rs
use fipc::ServiceRole;
use std::time::Duration;

fn main() -> fipc::Result<()> {
    // 4 KiB per client, requests and replies of up to 256 bytes, 1 client and 1 server: this process owns the
    // service and is its server
    let service = fipc::service::create("my_service", 4096, 256, 1, 1)?;
    let mut server = fipc::service::join("my_service", ServiceRole::Server, Duration::from_secs(5))?;
    for _ in 0..3 {
        // three requests, then the end
        let request = server.receive(None)?; // the next request of any client
        server.send(&request.to_ascii_uppercase(), None)?; // the reply goes to its client, and never waits
    }
    service.close(); // ends the service: the client still receives the replies sent
    Ok(())
}
```

```rust
// service_client.rs
use fipc::ServiceRole;
use std::time::Duration;

fn main() -> fipc::Result<()> {
    let mut client = fipc::service::join("my_service", ServiceRole::Client, Duration::from_secs(5))?;
    for i in 1..=3 {
        // sends the request and returns its reply; Error::Abandoned if the server ended before it replied
        let reply = client.call(format!("ping {i}").as_bytes(), None)?;
        println!("{}", String::from_utf8_lossy(&reply)); // PING 1, PING 2, PING 3
    }
    Ok(())
}
```

Run the server, then the client (`cargo run --example service_server`, then `cargo run --example service_client` in
a checkout), or the other way round: `join` waits up to its timeout for the owner. The client prints `PING 1` to
`PING 3`. `fipc::service::create(name, capacity, max_message, max_clients, max_servers)` returns the service's
handle, a `Service`.

- **Joining:** clients and servers join with `fipc::service::join(name, ServiceRole::Client, timeout)` or
  `ServiceRole::Server`, the owner's own too, from any process or thread: the owner's process admits each one on a
  thread of the library's. Clients wait while no server is joined, and servers while no client is.
- **Counting:** `service.wait_for_members(role, at_least, timeout)` waits until that many members of the role
  (`ServiceRole::Client`, `ServiceRole::Server`, or `None` for both) have joined and returns the count, with a
  `MembersError` as a publisher's; `service.cancel()` or a `service.canceler()` ends such a wait from any thread.
- **A client:** `send(request)` sends a request (or `send_acquire` and the slot's `commit`), and a receive (`receive`,
  `receive_into`, or in place `receive_acquire`) returns the reply to its oldest request whose reply it hasn't
  received yet: the n-th reply answers the n-th request, whichever server replies first. A client may send several
  requests before it receives (as many as its ring has room for: each holds its length rounded up to 64 bytes, plus
  `max_message` rounded up to 64, until its reply has been received), and the servers work on them at once. A receive
  that times out or is cancelled leaves its reply to come: the next receive returns it.
- **`call(request, timeout)`** sends a request and returns its reply, in a new `Vec`. A call checks its request first:
  `Error::Invalid` for an empty one, `Error::TooLarge` over `max_piece()`, at once, even while a reply is owed. Then a
  call on a cancelled connection returns `Error::Cancelled` at once, whatever its timeout, and sends nothing: it could
  never receive the reply. The connection keeps count of the replies still owed: a call that timed out or was cancelled
  waiting for its reply leaves that reply owed, and the next call first receives and drops that reply (at most one is
  owed, since every call drops it first; an `Error::Abandoned` in its place counts too), within its own timeout and
  before it sends, so a call never returns the reply to another request. A call that had to drop an owed reply first
  sends its request only if its timeout has time left: once a positive timeout has run out, it returns `Error::Timeout`
  without sending, so no server sees that request; with a zero timeout no step waits, and it sends if there is room.
  After a call that gave up, a plain receive returns that call's late reply (or `Error::Abandoned`) first: replies come
  in request order. A call returns `Error::Invalid` while a request sent with `send` (or a slot's `commit`) still has
  its reply to be received: that reply is yours, so receive it first; on any connection other than a service's client it
  returns `Error::Invalid` too. It takes `&mut self`: it sends and receives, and the borrow checker keeps another thread
  from doing either meanwhile.
- **A server:** `receive` takes the next request of any client (each client's in the order it sent them), and
  `send(reply)` replies to it and never waits. A server holds one request at a time: its receives return
  `Error::Invalid` until it replies, and a reply with no request held returns `Error::Invalid`. A reply to a client
  that has left is dropped. A server may receive on one thread and reply on another (`into_split()`), and it receives
  its next request once it has replied.
- **A request in place:** a request held in place stays valid until the reply, and from then on its bytes are its
  client's again, so the borrow checker keeps them from being read after the reply. `receive_request()` returns a
  `Request`, which derefs to the request in the ring and borrows the whole connection; its reply consumes it:
  `reply(&[u8])` sends a copy, and `reply_in_place(len, write)` hands `write` the request and `len` bytes of room for
  the reply in the ring (apart from the request's), and sends the length `write` returns. A `Request` is `Send`: it
  may move to another thread for its reply. A `ReceiveSlot` from `receive_acquire()` borrows the whole connection as
  well, so the reply waits for its drop; a server's split `Receiver` refuses `receive_acquire()` (`Error::Invalid`),
  since its `Sender` could reply while the slot lives.

```rust
let request = server.receive_request(None)?; // held in place until the reply
let len = request.len();
request.reply_in_place(len, |request, reply| {
    reply.copy_from_slice(request); // the reply's room, apart from the request: zero-copy both ways
    reply.make_ascii_uppercase();
    len
})?;
```

- **`Error::Abandoned`:** a client's receive (or call) returns it when the server that took the request ended before
  it replied: it was dropped, or its process ended. The request counts as answered, and the next receive returns the
  next reply. An abandoned request may or may not have been carried out; the service never sends it again, and the
  client decides whether to:

```rust
match client.call(b"charge 42", Duration::from_secs(5)) {
    Ok(reply) => handle(&reply),
    // Its server ended before it replied: the charge may or may not have been made
    Err(Error::Abandoned) => println!("ask the ledger before trying again"),
    Err(e) => return Err(e), // Error::Disconnected: the service ended
}
```

- **A service lives until its `Service` is closed or dropped, or the owner's process ends.** Then clients' sends and
  servers' receives return `Error::Disconnected`; a client receives the replies that came before the end, then
  `Error::Disconnected`, and a server's reply returns `Error::Disconnected`. Keep the `Service` for as long as the
  service is to run: binding it to `_`, or not at all, drops it, and the service ends at once. A client's or a
  server's drop only leaves the service: a client's requests that no server took are dropped, and a request a server
  holds is answered `Error::Abandoned`.
- **Not covered:** a server that hangs while its process runs (there are no timeouts, by design: its client waits,
  within its own timeouts); the owner's process, with which the service ends.

### The native library

`fipc-sys` links the library dynamically (on Windows through `raw-dylib`, so no import library is needed) and
copies it into Cargo's build folder, where `cargo run` and `cargo test` find it. Its build script takes the library
from, in this order:

1. the folder the environment variable `FASTIPC_LIB_DIR` names (a library you built, or keep in your repository);
2. in a checkout of the FastIPC repository, its `zig-out` build;
3. the copy for the target platform that the crate carries.

On docs.rs nothing is linked. The library is never unloaded (on Linux and macOS it mustn't be:
[platform support](https://github.com/fastipc/fastipc/blob/main/docs/platform-support.md)).

### Shipping your program

A program started outside Cargo looks for the library itself:

- **Windows:** put `fastipc.dll` next to the `.exe` (Windows looks there first).
- **Linux:** put `libfastipc.so` next to the executable and link it with an rpath of `$ORIGIN`, for example in your
  project's `.cargo/config.toml`:

  ```toml
  [target.x86_64-unknown-linux-gnu]
  rustflags = ["-C", "link-arg=-Wl,-rpath,$ORIGIN"]
  ```

  (on ARM64, the same under `[target.aarch64-unknown-linux-gnu]`)

  or install it where the loader looks (`LD_LIBRARY_PATH`, a library folder).
- **macOS:** put `libfastipc.dylib` next to the executable and link it with an rpath of `@loader_path` (the library's
  install name is `@rpath/libfastipc.dylib`), for example in your project's `.cargo/config.toml`:

  ```toml
  [target.aarch64-apple-darwin]
  rustflags = ["-C", "link-arg=-Wl,-rpath,@loader_path"]
  ```

After a build, the library is in `target/<profile>/build/fipc-sys-<hash>/out/`; a build script of a crate that
depends on `fipc-sys` directly gets that folder as `DEP_FASTIPC_LIB_DIR`. It is the same file for every
program of a version (the crate's own copy, or the one `FASTIPC_LIB_DIR` names).

### Performance

Between two processes, copied 16-byte messages run at about 41 to 42 million per second through the binding on the
baseline machine, and 64 KiB messages at 435,000 to 460,000 per second
(`bench/rust`; the numbers are in
[`docs/perf/bindings-baseline.md`](https://github.com/fastipc/fastipc/blob/main/docs/perf/bindings-baseline.md)).
The calls that take your buffer (`send`, `receive_into`, the zero-copy slots) add no allocation, lock or atomic
operation to the C call: the safety comes from the borrow checker, at compile time.

## Building from source

In a checkout of the repository, with the library built (`python devtool.py build`):

```bash
cd bindings/rust
cargo test           # unit, integration and doc tests against the repository's zig-out build, with peers in other
                     #   processes and a Python peer (the repository's venv) for the cross-language tests
cargo clippy --all-targets -- -D warnings
cargo doc --open
```

The workspace pins the current stable toolchain (`rust-toolchain.toml`); the crates build with Rust 1.85 or newer.
`python devtool.py package rust --smoke` packages both crates with every platform's library in `zig-out/packages`,
checks them, and makes a round trip through them from a fresh Cargo project, also run on its own next to its library.

## Documentation

- [The website](https://fastipc.github.io/fastipc/rust.html): this binding's guide, with examples of every call.
- [docs.rs/fipc](https://docs.rs/fipc): the API reference.
- [`include/fipc.h`](https://github.com/fastipc/fastipc/blob/main/include/fipc.h): the C API under this binding,
  and every call's contract (results, timeouts, threads, messages in pieces, the peer's end).
- [Platform support](https://github.com/fastipc/fastipc/blob/main/docs/platform-support.md) and the
  [protocol](https://github.com/fastipc/fastipc/blob/main/docs/protocol.md).
- [Changelog](https://github.com/fastipc/fastipc/blob/main/CHANGELOG.md),
  [issues](https://github.com/fastipc/fastipc/issues).

## License

MIT. Copyright (c) 2025-2026 Hayden Donnelly. See [LICENSE](https://github.com/fastipc/fastipc/blob/main/LICENSE).
