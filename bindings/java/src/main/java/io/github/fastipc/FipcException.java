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
    /** For OVERRUN, the number of messages lost; -1 otherwise. */
    private final long lostMessages;
    /** For every result but INVALID from a wait for members, the member count; -1 otherwise. */
    private final int memberCount;
    /** For a batch's send, how many of its messages went; -1 otherwise. */
    private final int sentMessages;

    FipcException(int code, String call) {
        this(code, call, -1, -1, -1, -1);
    }

    private FipcException(int code, String call, long messageLength, long lostMessages, int memberCount,
                          int sentMessages) {
        super(call + ": " + Native.resultStr(code)
            + (messageLength >= 0 ? " (the message is " + messageLength + " bytes)" : "")
            + (lostMessages >= 0 ? " (" + lostMessages + " messages lost)" : "")
            + (memberCount >= 0 ? " (" + memberCount + " members joined)" : "")
            + (sentMessages >= 0 ? " (" + sentMessages + " messages sent)" : ""));
        this.result = Result.of(code);
        this.call = call;
        this.messageLength = messageLength;
        this.lostMessages = lostMessages;
        this.memberCount = memberCount;
        this.sentMessages = sentMessages;
    }

    /**
     * A receive's failure, with what its length out-parameter holds: TOO_LARGE's message length, OVERRUN's number of
     * messages lost (a size_t, which no count takes past 2^63).
     */
    static FipcException receive(int code, String call, long length) {
        return new FipcException(code, call, code == Native.TOO_LARGE ? length : -1,
            code == Native.OVERRUN ? length : -1, -1, -1);
    }

    /**
     * A wait for members' failure (fipc_broadcast_wait_for_members, fipc_queue_wait_for_members,
     * fipc_service_wait_for_members), with the count it stored (at every result but INVALID).
     */
    static FipcException members(int code, String call, int count) {
        return new FipcException(code, call, -1, -1, code == Native.INVALID ? -1 : count, -1);
    }

    /** A batch's send failure (fipc_send_many), with how many of its messages went. */
    static FipcException sendMany(int code, int sent) {
        return new FipcException(code, "fipc_send_many", -1, -1, -1, sent);
    }

    /** {@return why the call failed} Never {@link Result#OK}. */
    public Result result() {
        return result;
    }

    /**
     * {@return the C function that returned the result, such as {@code "fipc_recv"}} {@code "call"} when
     * {@link Connection#call(byte[], java.time.Duration)} refused to send ({@link Result#INVALID}).
     */
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

    /**
     * {@return for {@link Result#OVERRUN}, the number of messages the lossy subscriber lost (at least 1); -1
     * otherwise} The next receive goes on from a recent message.
     */
    public long lostMessages() {
        return lostMessages;
    }

    /**
     * {@return for {@link Result#TIMEOUT}, {@link Result#CANCELLED} or {@link Result#DISCONNECTED} from a wait for
     * members ({@link Connection#waitForMembers(int, java.time.Duration)}, {@link WorkQueue#waitForMembers(QueueRole,
     * int, java.time.Duration)}, {@link Service#waitForMembers(ServiceRole, int, java.time.Duration)}), how many members
     * were joined; -1 otherwise}
     */
    public int memberCount() {
        return memberCount;
    }

    /**
     * {@return for a failure of {@link Connection#sendMany(java.util.List, java.time.Duration)}, how many of its
     * messages went (all those before the one that didn't); -1 otherwise}
     */
    public int sentMessages() {
        return sentMessages;
    }
}
