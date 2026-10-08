package io.github.fastipc;

import java.io.Serial;

/**
 * A call that didn't succeed: its {@link #result()} says why. Unchecked: a timeout, the peer's end and a cancel are
 * ordinary events of IPC, so catch the exception where they are handled, for example:
 *
 * <pre>{@code
 * try {
 *     byte[] message = conn.receive(Duration.ofMillis(100));
 *     handle(message);
 * } catch (FipcException e) {
 *     switch (e.result()) {
 *         case TIMEOUT -> { }                     // nothing yet
 *         case DISCONNECTED -> conn.close();      // the peer is gone: final
 *         default -> throw e;
 *     }
 * }
 * }</pre>
 */
public final class FipcException extends RuntimeException {
    @Serial
    private static final long serialVersionUID = 1L;

    /** Why the call failed. */
    private final Result result;
    /** The C function that returned the result. */
    private final String call;
    /** For TOO_LARGE from a receive, the message's length; -1 otherwise. */
    private final long messageLength;

    FipcException(int code, String call) {
        this(code, call, -1);
    }

    FipcException(int code, String call, long messageLength) {
        super(call + ": " + Native.resultStr(code)
            + (messageLength >= 0 ? " (the message is " + messageLength + " bytes)" : ""));
        this.result = Result.of(code);
        this.call = call;
        this.messageLength = messageLength;
    }

    /** {@return why the call failed} Never {@link Result#OK}. */
    public Result result() {
        return result;
    }

    /** {@return the C function that returned the result, such as {@code "fipc_recv"}} */
    public String call() {
        return call;
    }

    /**
     * {@return for {@link Result#TOO_LARGE} from a receive, the length of the message that didn't fit; -1 otherwise}
     * The message stays queued: receive it into a buffer this long, or with a receive that returns a new array.
     */
    public long messageLength() {
        return messageLength;
    }
}
