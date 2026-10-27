/**
 * FastIPC for Java: messages between processes on one machine, through shared memory, over the C API
 * of <a href="https://github.com/fastipc/fastipc/blob/main/include/fipc.h">include/fipc.h</a>.
 *
 * <p>Four kinds of IPC, each on a name the creator claims, in any processes of the user. The peers may be written in
 * any language the library has a binding for (Zig, C, C++, Python, C#, Java, Rust, Lua, JavaScript, Go): the protocol
 * is the same. Every handle comes from {@link io.github.fastipc.Fipc}'s static methods.
 *
 * <h2>Streams</h2>
 * A server creates a stream ({@link io.github.fastipc.Fipc#createStream(String, long) Fipc.createStream}) and its
 * {@link io.github.fastipc.Listener} accepts one client at a time; a client connects to the name
 * ({@link io.github.fastipc.Fipc#connectStream(String, java.time.Duration) Fipc.connectStream}). Each connection has a
 * shared-memory segment with one ring per direction, and reports the peer's end as soon as its process exits or it
 * closes the connection.
 *
 * <pre>{@code
 * // Server
 * try (Listener listener = Fipc.createStream("my_channel", 1 << 20);  // rings of 1 MiB each way
 *      Connection conn = listener.accept()) {                         // waits for a client
 *     String text = new String(conn.receive(), UTF_8);
 *     conn.send(text.toUpperCase().getBytes(UTF_8));
 * }
 *
 * // Client (another process)
 * try (Connection conn = Fipc.connectStream("my_channel", Duration.ofSeconds(5))) {
 *     conn.send("ping".getBytes(UTF_8));
 *     byte[] reply = conn.receive();                                  // "PING"
 * }
 * }</pre>
 *
 * <h2>Broadcasts</h2>
 * A broadcast sends every message of one publisher to each of its subscribers.
 * {@link io.github.fastipc.Fipc#createBroadcast(String, long, int, java.time.Duration) Fipc.createBroadcast} returns
 * the publisher's {@link io.github.fastipc.Connection}, the broadcast's one handle: its sends go to every subscriber,
 * and {@link io.github.fastipc.Connection#waitForMembers(int, java.time.Duration) waitForMembers} waits for a number of
 * subscribers. A subscriber joins with {@link io.github.fastipc.Fipc#joinBroadcast(String,
 * io.github.fastipc.SubscriberRole, java.time.Duration) Fipc.joinBroadcast}, and the publisher's process admits it by
 * itself: {@link io.github.fastipc.SubscriberRole#GATED GATED} misses nothing (the publisher waits for it),
 * {@link io.github.fastipc.SubscriberRole#LOSSY LOSSY} never holds the publisher back and may lose messages
 * ({@link io.github.fastipc.Result#OVERRUN OVERRUN}, with
 * {@link io.github.fastipc.FipcException#lostMessages() lostMessages()}). Closing the publisher ends the broadcast.
 *
 * <h2>Work queues</h2>
 * A work queue hands each message of up to 63 producers to one of up to 64 consumers, in no promised order.
 * {@link io.github.fastipc.Fipc#createQueue(String, long, int, int) Fipc.createQueue} returns the
 * {@link io.github.fastipc.WorkQueue}; producers and consumers, the owner's own too, join with
 * {@link io.github.fastipc.Fipc#joinQueue(String, io.github.fastipc.QueueRole, java.time.Duration) Fipc.joinQueue} as
 * a {@link io.github.fastipc.QueueRole#PRODUCER PRODUCER} or a {@link io.github.fastipc.QueueRole#CONSUMER CONSUMER},
 * and the owner's process admits them by itself. {@link io.github.fastipc.WorkQueue#close() WorkQueue.close} ends the
 * queue: consumers receive what is left, then {@link io.github.fastipc.Result#DISCONNECTED DISCONNECTED}. A queue lives
 * until {@code close()} or the end of its process; dropping the object doesn't end it.
 *
 * <h2>Services</h2>
 * A service hands each request of up to 64 clients to one of up to 64 servers, and the server's reply back to the
 * client that sent it. {@link io.github.fastipc.Fipc#createService(String, long, int, int, int) Fipc.createService}
 * returns the {@link io.github.fastipc.Service}; clients and servers, the owner's own too, join with
 * {@link io.github.fastipc.Fipc#joinService(String, io.github.fastipc.ServiceRole, java.time.Duration)
 * Fipc.joinService} as a {@link io.github.fastipc.ServiceRole#CLIENT CLIENT} or a
 * {@link io.github.fastipc.ServiceRole#SERVER SERVER}, and the owner's process admits them by itself. A client's
 * replies come in the order of its requests, and
 * {@link io.github.fastipc.Connection#call(byte[], java.time.Duration) call} sends a request and returns its reply; a
 * server receives the next request of any client and replies with its next send, which never waits. A request whose
 * server ended before it replied is answered {@link io.github.fastipc.Result#ABANDONED ABANDONED}.
 * {@link io.github.fastipc.Service#close() Service.close} ends the service. A service lives until {@code close()} or
 * the end of its process; dropping the object doesn't end it.
 *
 * <h2>Results</h2>
 * A call that doesn't succeed throws {@link io.github.fastipc.FipcException}, an unchecked exception whose
 * {@link io.github.fastipc.FipcException#result() result()} says why:
 * {@link io.github.fastipc.Result#TIMEOUT TIMEOUT},
 * {@link io.github.fastipc.Result#DISCONNECTED DISCONNECTED} (the peer's end; final),
 * {@link io.github.fastipc.Result#CANCELLED CANCELLED}, and so on. A call on a closed listener, connection, queue
 * or service throws {@link java.lang.IllegalStateException}.
 *
 * <h2>Timeouts</h2>
 * Every call that can wait has an overload without a timeout, which waits for ever, and one that takes a
 * {@link java.time.Duration}: {@link io.github.fastipc.Fipc#NO_WAIT} ({@code Duration.ZERO}) doesn't
 * wait, {@link io.github.fastipc.Fipc#FOREVER} waits for ever. A timeout is rounded up to whole
 * milliseconds; one of {@code Integer.MAX_VALUE} milliseconds (about 24.8 days) or longer waits for ever. A call that
 * would have to wait longer throws {@code TIMEOUT}, having changed nothing. Waits are not interruptible: a thread's
 * interrupt doesn't wake a call; {@code cancel()} and {@code close()} do. A waiting call blocks its thread in native
 * code, which pins a virtual thread to its carrier: use platform threads for calls that wait long.
 *
 * <h2>Threads</h2>
 * On a connection, one thread at a time sends (the sends, the zero-copy send) and one thread at a time receives (the
 * receives, the zero-copy receive); the two may be different threads, running at once. On a listener, one thread at a
 * time accepts; the waits for members run on any thread. {@code cancel()} and {@code close()} may be called from any
 * thread, at any time: {@code close()} cancels the handle, so a call that waits throws {@code CANCELLED}, and the
 * native close runs once the calls in progress have returned. No call ever runs on a closed handle, so stopping a
 * thread that waits is: close, then join the thread. A listener or connection that is never closed is closed once it is
 * unreachable, by a {@link java.lang.ref.Cleaner}; close them explicitly (try-with-resources). A
 * {@link io.github.fastipc.WorkQueue} or a {@link io.github.fastipc.Service} never is: its use lives in its members, so
 * it lives until {@code close()} or the end of its process.
 *
 * <h2>Native access</h2>
 * The binding calls restricted methods of the FFM API. Grant it native access, or the JVM prints a warning (and a
 * future JVM may refuse): {@code --enable-native-access=io.github.fastipc} when the jar is on the module
 * path, {@code --enable-native-access=ALL-UNNAMED} on the class path, or the manifest attribute
 * {@code Enable-Native-Access: ALL-UNNAMED} in an executable jar's manifest.
 *
 * <h2>The native library</h2>
 * The jar carries {@code fastipc.dll} (Windows x64), {@code libfastipc.so} (Linux x64 and arm64, glibc 2.34+) and
 * {@code libfastipc.dylib} (macOS 14.4+ on Apple Silicon); the x64 libraries need an x86-64-v3 CPU. The binding
 * loads, in this order: the file the system property {@code fastipc.library.path} names; in a development checkout
 * of the repository, its {@code zig-out} build; the jar's library, extracted once per content into a cache folder of
 * the user ({@code %LOCALAPPDATA%\fipc} on Windows, {@code ~/Library/Caches/fipc} on macOS,
 * {@code $XDG_CACHE_HOME/fipc} or {@code ~/.cache/fipc} on Linux) and checked against the jar's copy before each
 * load; else the system's search ({@code PATH}, {@code LD_LIBRARY_PATH}, {@code DYLD_LIBRARY_PATH}).
 * {@link io.github.fastipc.Fipc#libraryPath()} says which one was loaded. The library is never unloaded.
 */
package io.github.fastipc;
