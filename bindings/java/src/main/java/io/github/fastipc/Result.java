package io.github.fastipc;

/**
 * The result of a call: {@code fipc_result_t}. Every result but {@link #OK} reaches the caller as a
 * {@link FipcException}.
 */
public enum Result {
    /** The call succeeded. */
    OK(0),
    /** The timeout ran out (with {@link Fipc#NO_WAIT}: nothing to do without waiting). The call changed nothing. */
    TIMEOUT(1),
    /**
     * The peer ended the connection: it closed it, or its process ended, for any reason. Receives report it only after
     * they have delivered every message the peer completed. Final: close the connection; to talk again, accept or
     * connect a new one.
     */
    DISCONNECTED(2),
    /** {@code cancel()} or {@code close()} was called, and the call would have to wait. */
    CANCELLED(3),
    /**
     * The message doesn't fit what the call can take: the caller's buffer, or one piece for zero-copy and on a
     * broadcast, a queue or a service. A receive leaves the message queued and reports its length
     * ({@link FipcException#messageLength()}).
     */
    TOO_LARGE(4),
    /**
     * A bad argument (a name, a capacity, an empty message), a call out of order (an accept while the last accepted
     * connection is open, a commit without a reservation, a service server's receive while it holds a request or its
     * reply while it holds none), a call the connection's role can't make, a peer with another version or user, or
     * corrupt ring content.
     */
    INVALID(5),
    /** Memory or another OS resource ran out (setting a connection up). */
    NO_MEMORY(6),
    /**
     * {@link Fipc#createStream}, {@link Fipc#createBroadcast}, {@link Fipc#createQueue}, {@link Fipc#createService}:
     * another stream, broadcast, queue or service holds the name.
     */
    ADDR_IN_USE(7),
    /**
     * A lossy subscriber of a broadcast fell a lap behind: messages were lost. The receive throws it instead
     * of a message, with the number lost ({@link FipcException#lostMessages()}, at least 1); the next receive goes on
     * from a recent message.
     */
    OVERRUN(8),
    /**
     * A gated subscriber of a broadcast held the publisher back for the broadcast's longest stall and was dropped
     * from it: every later receive throws it. Final: close the connection.
     */
    UNSUBSCRIBED(9),
    /**
     * A service's client: the server that took the request ended before it replied (it closed its connection, or its
     * process ended). A receive, or {@link Connection#call(byte[], java.time.Duration) call}, throws it instead of the
     * reply. The request is answered (the next receive returns the next reply); it may or may not have been carried
     * out, and the service never sends it again: the client decides whether to.
     */
    ABANDONED(10);

    private static final Result[] BY_CODE = values();

    private final int code;

    Result(int code) {
        this.code = code;
    }

    /** {@return the C value} {@code FIPC_OK} is 0, {@code FIPC_TIMEOUT} 1, and so on. */
    public int code() {
        return code;
    }

    /** {@return the library's name for this result, such as {@code "FIPC_TIMEOUT"}} The text of {@code fipc_result_str}. */
    public String libraryName() {
        return Native.resultStr(code);
    }

    /** The result of C value {@code code}; {@link #INVALID} for a value the library doesn't define. */
    static Result of(int code) {
        return code >= 0 && code < BY_CODE.length ? BY_CODE[code] : INVALID;
    }
}
