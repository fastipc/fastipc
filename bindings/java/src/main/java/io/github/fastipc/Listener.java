package io.github.fastipc;

import static java.lang.foreign.ValueLayout.ADDRESS;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.ref.Cleaner;
import java.lang.ref.Reference;
import java.time.Duration;

/**
 * A stream's listener, from {@link Fipc#createStream(String, long)}: it holds the stream's name, and {@link #accept()}
 * sets up one client's connection at a time, on the calling thread. The connections it accepted are independent of it:
 * they stay open when it is closed.
 *
 * <p>Threads: one thread at a time accepts; {@link #cancel()} and {@link #close()} may be called from any thread, at
 * any time. {@code close()} cancels the listener, so an accept that waits throws {@link Result#CANCELLED}, and the
 * native close runs once the call in progress has returned.
 *
 * <p>Lifetime: a listener that is never closed is closed by a {@link Cleaner} once it is unreachable, which frees the
 * name.
 */
public final class Listener implements AutoCloseable {
    private final Handle handle;
    private final Cleaner.Cleanable cleanable;
    private final String name;

    /** Owns {@code pointer} once it returns; if it throws, nothing does. */
    private Listener(MemorySegment pointer, String name) {
        this.name = name;
        this.handle = new Handle(pointer, "listener", Native::listenerCancel, () -> Native.listenerClose(pointer));
        this.cleanable = Handle.CLEANER.register(this, handle::close); // last: the cleaner owns the pointer
    }

    /** The listener that owns the native {@code pointer}; if it can't be made (out of memory), closes the pointer. */
    static Listener adopt(MemorySegment pointer, String name) {
        try {
            return new Listener(pointer, name);
        } catch (RuntimeException | Error e) {
            Native.listenerClose(pointer);
            throw e;
        }
    }

    /** {@return the name of the stream this listener holds} */
    public String name() {
        return name;
    }

    /**
     * Waits for a client, for ever; see {@link #accept(Duration)}.
     *
     * @return the client's connection
     */
    public Connection accept() {
        return accept(Fipc.FOREVER);
    }

    /**
     * Waits up to {@code timeout} for a client, sets its connection up on the calling thread, and returns it; it may
     * already hold the client's first messages. A call that times out in the middle of a client's setup keeps it for
     * the next call, so {@link Fipc#NO_WAIT} polls (a client then takes one or two calls).
     *
     * <p>One client at a time: {@link Result#INVALID} while the connection this listener returned last is open (close
     * it first; its native close runs once no call on it is in progress). A client that connects meanwhile waits,
     * within its own timeout. A client whose version differs, or which runs as another user, is refused, and the call
     * goes on waiting.
     *
     * @param timeout how long to wait; {@link Fipc#NO_WAIT} doesn't, {@link Fipc#FOREVER} waits for ever
     * @return the client's connection
     * @throws FipcException {@link Result#TIMEOUT}; {@link Result#CANCELLED} after {@link #cancel()} or
     *                       {@link #close()}; {@link Result#INVALID} while the last accepted connection is open;
     *                       {@link Result#NO_MEMORY} once the listener couldn't get the memory for a client's rings:
     *                       it has failed for good (close it, and create the stream again to go on)
     * @throws IllegalStateException if the listener is closed
     */
    public Connection accept(Duration timeout) {
        int ms = Fipc.millis(timeout);
        MemorySegment listener = handle.acquire();
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment out = arena.allocate(ADDRESS);
            int result = Native.streamAccept(listener, out, ms);
            if (result != Native.OK) {
                throw new FipcException(result, "fipc_stream_accept");
            }
            return Connection.adopt(out.get(ADDRESS, 0), name);
        } finally {
            handle.release();
            Reference.reachabilityFence(this);
        }
    }

    /**
     * Makes every accept that waits, now or later, throw {@link Result#CANCELLED}; a client whose setup an accept began
     * is dropped (it connects again, within its own timeout). Final. Any thread; a no-op once closed.
     */
    public void cancel() {
        handle.cancel();
        Reference.reachabilityFence(this);
    }

    /**
     * Stops listening: cancels the listener, and closes it once no accept is in progress. The name is free again then
     * (on Windows, once the connections it accepted are closed too). The connections it accepted stay open; one it set
     * up that {@code accept} hasn't returned is closed (its client gets {@link Result#DISCONNECTED}). Any thread; a
     * no-op once closed. Later calls throw {@link IllegalStateException}.
     */
    @Override
    public void close() {
        cleanable.clean();
    }
}
