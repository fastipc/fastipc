package io.github.fastipc;

/**
 * A work queue's member: how one joins a queue ({@link Fipc#joinQueue(String, QueueRole, java.time.Duration)}), and
 * which members a count takes ({@link WorkQueue#waitForMembers(QueueRole, int, java.time.Duration)}).
 */
public enum QueueRole {
    /**
     * {@code FIPC_PRODUCER}: sends ({@code send}, or in place with {@code sendAcquire}/{@code sendCommit}) into a ring
     * of its own, waiting for room while the consumers haven't taken its earlier messages. Its receives throw
     * {@link Result#INVALID}. Its close leaves the queue; the messages it sent stay for the consumers.
     */
    PRODUCER(3),
    /**
     * {@code FIPC_CONSUMER}: takes the next message of any producer ({@code receive}, or in place with
     * {@code receiveAcquire}/{@code receiveRelease}); each message goes to one consumer. Its sends throw
     * {@link Result#INVALID}. Once the queue has ended it receives what is left, then {@link Result#DISCONNECTED}.
     */
    CONSUMER(4);

    private final int code;

    QueueRole(int code) {
        this.code = code;
    }

    /** The C value: fipc_queue_join's and fipc_queue_wait_for_members' role. */
    int code() {
        return code;
    }
}
