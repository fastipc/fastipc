# fipc for Java

Messages and RPC between two processes on one machine, through shared memory: the Java binding of
[FastIPC](https://github.com/fastipc/fastipc), a small library written in Zig
([`include/fipc.h`](https://github.com/fastipc/fastipc/blob/main/include/fipc.h)). A server listens on a name and
accepts one client at a time; a client connects to the name. `connect` waits for the server's `accept`, so a client and
its server run in different processes or on different threads. Each connection has a shared-memory segment with one ring
per direction, and reports the peer's end as soon as its process exits or closes the connection.

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

Run each in its own terminal, for example with the source launcher:
`java --enable-native-access=ALL-UNNAMED -cp fipc-1.0.0.jar Server.java`. The Python client of the
[repository's README](https://github.com/fastipc/fastipc#examples) works against this server too.

- **Plain messages:** `send` (a `byte[]`, a range of one, a `ByteBuffer` or a `MemorySegment`) and `receive`, which
  returns a new `byte[]`, or fills your `ByteBuffer` or `MemorySegment` and returns the length. A message of any size
  may be sent (at least 1 byte); it travels in pieces and arrives whole. Native memory (a direct `ByteBuffer`, a
  native `MemorySegment`) goes to the library as it is; heap memory is copied through a native buffer.
- **Zero-copy** (messages of one piece, up to `maxPiece()` bytes): `sendAcquire(length)` returns a `MemorySegment`
  in the ring, write the message into it, `sendCommit(length)`; `receiveAcquire()` returns the message in the ring,
  read-only, read it, `receiveRelease()`.
- **RPC:** `rpcSubmit(opcode, payload)` returns the request's id; `rpcRespond(id, opcode, status, payload)`;
  `rpcReceive()` returns an `RpcMessage` (id, kind, opcode, status, payload), and `rpcReceive(buffer)` an
  `RpcHeader` with the payload in your buffer (no allocation per message). Use a connection for plain messages or
  for RPC, not both.

### Results and timeouts

A call that doesn't succeed throws `FipcException`, an unchecked exception whose `result()` is a `Result`: `TIMEOUT`,
`DISCONNECTED` (the peer's end; final: close the connection, and accept or connect a new one), `CANCELLED`, `TOO_LARGE`
(a receive's `messageLength()` says how much room the message needs; it stays queued), `INVALID`, `NO_MEMORY` or
`ADDR_IN_USE`. A call on a closed listener or connection throws `IllegalStateException`.

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
`Cleaner` once it is unreachable; close them explicitly, with try-with-resources.

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

Between two processes, copied 16-byte messages run at about 16 million per second through the binding, and
64 KiB messages at 430,000 to 450,000 per second (`bench/java`; the numbers are in
[`docs/perf/bindings-baseline.md`](https://github.com/fastipc/fastipc/blob/main/docs/perf/bindings-baseline.md)).
In one process with no waiting, a 16-byte message sent and received costs about 105 ns on the Windows baseline
machine, against about 40 ns for the C API alone: each call holds the handle (two atomic operations) and goes through
an FFM downcall. A zero-copy message costs a little more than a copied one at that size (its segment's lifetime).

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

The wrapper runs on any JDK 17 or newer; the build compiles for Java 22 with a JDK 25 toolchain, which Gradle
downloads when none is installed. `python devtool.py package java --smoke` builds the jar with both native libraries,
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
