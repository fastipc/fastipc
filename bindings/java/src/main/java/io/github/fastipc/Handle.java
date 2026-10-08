package io.github.fastipc;

import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.ref.Cleaner;
import java.util.function.Consumer;

/**
 * A native handle (a listener or a connection) with a reference count, so that no call ever runs on a closed handle
 * (include/fipc.h, "Threads": the close needs the handle to itself). The owner holds one reference; every call holds
 * one for its duration, and a zero-copy reservation or acquired message one until its commit or release. close()
 * marks the handle closed (new calls throw IllegalStateException), cancels it (a call that waits returns
 * FIPC_CANCELLED) and drops the owner's reference; the native close runs on the thread that drops the last one.
 */
final class Handle {
    /** Every listener and connection that is never closed is closed by this cleaner once it is unreachable. */
    static final Cleaner CLEANER = Cleaner.create();

    private static final int CLOSED = 1;
    private static final int ONE = 2;
    private static final VarHandle STATE;

    static {
        try {
            STATE = MethodHandles.lookup().findVarHandle(Handle.class, "state", int.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /** The native pointer. */
    final MemorySegment pointer;
    private final String what;
    private final Consumer<MemorySegment> cancel;
    private final Runnable close;
    /** The reference count times ONE, plus CLOSED once closed; starts with the owner's reference. */
    @SuppressWarnings("unused") // through STATE
    private volatile int state = ONE;

    /**
     * @param what   "listener" or "connection", for the exceptions
     * @param cancel the native cancel
     * @param close  the native close and whatever else is freed with it
     */
    Handle(MemorySegment pointer, String what, Consumer<MemorySegment> cancel, Runnable close) {
        this.pointer = pointer;
        this.what = what;
        this.cancel = cancel;
        this.close = close;
    }

    /** Takes a reference for a call: the pointer, valid until the matching release. */
    MemorySegment acquire() {
        for (;;) {
            int s = state;
            if ((s & CLOSED) != 0) {
                throw closed();
            }
            if (STATE.compareAndSet(this, s, s + ONE)) {
                return pointer;
            }
        }
    }

    /** Drops a reference; the last one closes the handle. */
    void release() {
        int s = (int) STATE.getAndAdd(this, -ONE) - ONE;
        if (s == CLOSED) {
            close.run();
        }
    }

    boolean isClosed() {
        return (state & CLOSED) != 0;
    }

    /** The native cancel, unless the handle is closed. Any thread. */
    void cancel() {
        for (;;) {
            int s = state;
            if ((s & CLOSED) != 0) {
                return;
            }
            if (STATE.compareAndSet(this, s, s + ONE)) {
                break;
            }
        }
        try {
            cancel.accept(pointer);
        } finally {
            release();
        }
    }

    /** Closes the handle: cancels it, and the native close runs once no call holds it. Any thread; once. */
    void close() {
        for (;;) {
            int s = state;
            if ((s & CLOSED) != 0) {
                return;
            }
            if (STATE.compareAndSet(this, s, s | CLOSED)) {
                break;
            }
        }
        try {
            cancel.accept(pointer); // the owner's reference still holds the handle
        } finally {
            release();
        }
    }

    IllegalStateException closed() {
        return new IllegalStateException("the " + what + " is closed");
    }
}
