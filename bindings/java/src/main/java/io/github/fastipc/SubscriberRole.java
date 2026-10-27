package io.github.fastipc;

/** How a subscriber joins a broadcast: {@link Fipc#joinBroadcast(String, SubscriberRole, java.time.Duration)}. */
public enum SubscriberRole {
    /**
     * {@code FIPC_SUBSCRIBER_LOSSY}: never holds the publisher back. One that falls a lap behind loses messages: a
     * receive throws {@link Result#OVERRUN} instead of a message, with the number lost, and the next goes on from a
     * recent message. It receives copies, checked against the publisher's overwriting them meanwhile: its
     * {@link Connection#receiveAcquire()} throws {@link Result#INVALID}.
     */
    LOSSY(1),
    /**
     * {@code FIPC_SUBSCRIBER_GATED}: misses nothing, as a send waits for room the subscriber still holds. It may
     * receive in place ({@link Connection#receiveAcquire()}): the message holds the publisher back until its
     * release. One that holds the publisher back for the broadcast's longest stall is unsubscribed
     * ({@link Result#UNSUBSCRIBED}).
     */
    GATED(2);

    private final int code;

    SubscriberRole(int code) {
        this.code = code;
    }

    /** The C value: fipc_broadcast_join's role. */
    int code() {
        return code;
    }
}
