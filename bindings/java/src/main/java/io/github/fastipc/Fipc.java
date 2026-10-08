package io.github.fastipc;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Objects;

/** The binding's constants and the native library it loaded. */
public final class Fipc {
    /** A timeout that doesn't wait ({@code FIPC_NO_WAIT}): {@link Duration#ZERO}. */
    public static final Duration NO_WAIT = Duration.ZERO;

    /**
     * A timeout that waits for ever ({@code FIPC_FOREVER}), as the overloads without a timeout do: the longest
     * {@link Duration} ({@link ChronoUnit#FOREVER}). Any timeout of {@code Integer.MAX_VALUE} milliseconds or longer
     * waits for ever too.
     */
    public static final Duration FOREVER = ChronoUnit.FOREVER.getDuration();

    private Fipc() {
    }

    /**
     * {@return where the native library was loaded from: a file's path, or the library's name when the system's search
     * found it} Loads the library if no call has yet.
     *
     * @throws UnsatisfiedLinkError if the library can't be found or loaded
     */
    public static String libraryPath() {
        return Native.LIBRARY;
    }

    /** {@code timeout} as the C API's int milliseconds: 0 doesn't wait, -1 waits for ever. */
    static int millis(Duration timeout) {
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.isNegative()) {
            throw new IllegalArgumentException("a negative timeout: " + timeout);
        }
        if (timeout.isZero()) {
            return 0;
        }
        long seconds = timeout.getSeconds();
        if (seconds > Integer.MAX_VALUE / 1000) {
            return -1;
        }
        long ms = seconds * 1000 + (timeout.getNano() + 999_999) / 1_000_000; // a partial millisecond waits a whole one
        return ms >= Integer.MAX_VALUE ? -1 : (int) ms;
    }
}
