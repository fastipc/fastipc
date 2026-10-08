/**
 * FastIPC for Java: messages and RPC between two processes on one machine, through shared memory, over the C API
 * of <a href="https://github.com/fastipc/fastipc/blob/main/include/fipc.h">include/fipc.h</a>.
 *
 * <p>A server listens on a name and accepts one client at a time; a client connects to the name. Each connection has
 * a shared-memory segment with one ring per direction, and reports the peer's end as soon as its process exits or it
 * closes the connection. The peer may be written in any language the library has a binding for (Zig, C, C++, Python,
 * C#, Java, Rust, Lua, JavaScript, Go): the protocol is the same.
 *
 * <pre>{@code
 * // Server
 * try (Listener listener = Listener.listen("my_channel", 1 << 20);   // rings of 1 MiB each way
 *      Connection conn = listener.accept()) {                        // waits for a client
 *     RpcMessage request = conn.rpcReceive();
 *     conn.rpcRespond(request.id(), request.opcode(), 0, "pong".getBytes(UTF_8));
 * }
 *
 * // Client (another process)
 * try (Connection conn = Connection.connect("my_channel", Duration.ofSeconds(5))) {
 *     conn.rpcSubmit(1, "ping".getBytes(UTF_8));
 *     byte[] reply = conn.rpcReceive().payload();                    // "pong"
 * }
 * }</pre>
 *
 * <h2>Results</h2>
 * A call that doesn't succeed throws {@link io.github.fastipc.FipcException}, an unchecked exception whose
 * {@link io.github.fastipc.FipcException#result() result()} says why:
 * {@link io.github.fastipc.Result#TIMEOUT TIMEOUT},
 * {@link io.github.fastipc.Result#DISCONNECTED DISCONNECTED} (the peer's end; final),
 * {@link io.github.fastipc.Result#CANCELLED CANCELLED}, and so on. A call on a closed listener or
 * connection throws {@link java.lang.IllegalStateException}.
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
 * On a connection, one thread at a time sends (the sends, the zero-copy send, {@code rpcSubmit},
 * {@code rpcRespond}) and one thread at a time receives (the receives, the zero-copy receive, {@code rpcReceive});
 * the two may be different threads, running at once. On a listener, one thread at a time accepts. {@code cancel()} and
 * {@code close()} may be called from any thread, at any time: {@code close()} cancels the handle, so a call that waits
 * throws {@code CANCELLED}, and the native close runs once the calls in progress have returned. No call ever runs on
 * a closed handle, so stopping a thread that waits is: close, then join the thread. A listener or connection that is
 * never closed is closed once it is unreachable, by a {@link java.lang.ref.Cleaner}; close them explicitly
 * (try-with-resources).
 *
 * <h2>Native access</h2>
 * The binding calls restricted methods of the FFM API. Grant it native access, or the JVM prints a warning (and a
 * future JVM may refuse): {@code --enable-native-access=io.github.fastipc} when the jar is on the module
 * path, {@code --enable-native-access=ALL-UNNAMED} on the class path, or the manifest attribute
 * {@code Enable-Native-Access: ALL-UNNAMED} in an executable jar's manifest.
 *
 * <h2>The native library</h2>
 * The jar carries {@code fastipc.dll} (Windows x64) and {@code libfastipc.so} (Linux x64, glibc 2.34+); they need an
 * x86-64-v3 CPU. The binding loads, in this order: the file the system property {@code fastipc.library.path} names; in
 * a development checkout of the repository, its {@code zig-out} build; the jar's library, extracted once per content
 * into a cache folder of the user ({@code %LOCALAPPDATA%\fipc} on Windows, {@code $XDG_CACHE_HOME/fipc}
 * or {@code ~/.cache/fipc} on Linux) and checked against the jar's copy before each load; else the system's
 * search ({@code PATH}, {@code LD_LIBRARY_PATH}). {@link io.github.fastipc.Fipc#libraryPath()} says which
 * one was loaded. The library is never unloaded.
 */
package io.github.fastipc;
