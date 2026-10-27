package io.github.fastipc;

import static java.lang.foreign.ValueLayout.JAVA_INT;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.time.Duration;
import java.util.Objects;

/**
 * A work queue, held by its owner, from {@link Fipc#createQueue(String, long, int, int)}: it takes messages from up to
 * 63 producers and hands each to one of up to 64 consumers, in any processes of the user. Each producer has a ring of
 * its own in the queue's memory, which every member maps; a consumer takes a message of any producer, and the queue
 * promises no order. Producers and consumers join with {@link Fipc#joinQueue(String, QueueRole, Duration)}, the
 * owner's own too: the owner's process admits each joiner by itself, on a thread of the library's, so a member may join
 * from any thread, the owner's included. Messages are of one piece, up to {@link Connection#maxPiece()} bytes (a
 * quarter of the capacity); they wait in the queue until a consumer takes them, each delivered to one consumer at most
 * (none if that consumer's process ends while it holds it).
 *
 * <p>The queue ends at {@link #close()}, or when the owner's process ends: producers' sends then throw
 * {@link Result#DISCONNECTED}, and consumers receive what is left in the queue, then {@link Result#DISCONNECTED}. A
 * member's close, or the end of its process, doesn't end it.
 *
 * <p>Lifetime: a queue lives until {@link #close()} or the end of its process; dropping the object doesn't end it.
 * Unlike the binding's other handles, it is never closed by a {@link java.lang.ref.Cleaner}: its use lives in its
 * members, in this process and in others, so the garbage collector never ends it. A queue that is never closed holds
 * its name until the process ends, and another {@code createQueue} of the name then throws
 * {@link Result#ADDR_IN_USE}.
 *
 * <p>Threads: {@link #waitForMembers}, {@link #cancel()} and {@link #close()} may be called from any thread, at any
 * time. {@code close()} cancels the queue, so a {@code waitForMembers} that waits throws {@link Result#CANCELLED}, and
 * the native close runs once the call in progress has returned.
 */
public final class WorkQueue implements AutoCloseable {
    private final Handle handle;
    private final String name;

    /** Owns {@code pointer} once it returns; if it throws, nothing does. */
    private WorkQueue(MemorySegment pointer, String name) {
        this.name = name;
        this.handle = new Handle(pointer, "queue", Native::queueCancel, () -> Native.queueClose(pointer));
    }

    /** The queue that owns the native {@code pointer}; if it can't be made (out of memory), closes the pointer. */
    static WorkQueue adopt(MemorySegment pointer, String name) {
        try {
            return new WorkQueue(pointer, name);
        } catch (RuntimeException | Error e) {
            Native.queueClose(pointer);
            throw e;
        }
    }

    /** {@return the queue's name} */
    public String name() {
        return name;
    }

    /**
     * Waits for ever until at least {@code atLeast} members, producers and consumers together, are joined; see
     * {@link #waitForMembers(QueueRole, int, Duration)}.
     *
     * @param atLeast the number of members to wait for: 0 to {@code maxProducers + maxConsumers}
     * @return how many members are joined: at least {@code atLeast}
     */
    public int waitForMembers(int atLeast) {
        return count(0, atLeast, Fipc.FOREVER);
    }

    /**
     * Waits up to {@code timeout} until at least {@code atLeast} members, producers and consumers together, are joined;
     * see {@link #waitForMembers(QueueRole, int, Duration)}.
     *
     * @param atLeast the number of members to wait for: 0 to {@code maxProducers + maxConsumers}
     * @param timeout how long to wait; {@link Fipc#NO_WAIT} polls
     * @return how many members are joined: at least {@code atLeast}
     */
    public int waitForMembers(int atLeast, Duration timeout) {
        return count(0, atLeast, timeout);
    }

    /**
     * Waits for ever until at least {@code atLeast} members of {@code role} are joined; see
     * {@link #waitForMembers(QueueRole, int, Duration)}.
     *
     * @param role    the role to count
     * @param atLeast the number of members to wait for: 0 to the role's maximum
     * @return how many members of the role are joined: at least {@code atLeast}
     */
    public int waitForMembers(QueueRole role, int atLeast) {
        return waitForMembers(role, atLeast, Fipc.FOREVER);
    }

    /**
     * Waits up to {@code timeout} until at least {@code atLeast} members of {@code role} are joined, and returns how
     * many are. Joined: admitted, and not yet left; a member that leaves, or whose process ends, is counted until the
     * owner's process has seen its end, soon after. {@code atLeast} 0 returns the count at once. For example, a
     * producer that is to close only once a consumer is there waits for one consumer. Any thread.
     *
     * @param role    the role to count: {@link QueueRole#PRODUCER} or {@link QueueRole#CONSUMER}
     * @param atLeast the number of members to wait for: 0 to the role's maximum ({@code maxProducers} or
     *                {@code maxConsumers})
     * @param timeout how long to wait; {@link Fipc#NO_WAIT} polls
     * @return how many members of the role are joined: at least {@code atLeast}
     * @throws FipcException {@link Result#TIMEOUT}, {@link Result#CANCELLED} (after {@link #cancel()} or
     *                       {@link #close()}), {@link Result#DISCONNECTED} once the queue has ended, each with the
     *                       count in {@link FipcException#memberCount()}; {@link Result#INVALID} for {@code atLeast}
     *                       over the role's maximum
     * @throws IllegalArgumentException for a negative {@code atLeast}
     * @throws IllegalStateException    if the queue is closed
     */
    public int waitForMembers(QueueRole role, int atLeast, Duration timeout) {
        Objects.requireNonNull(role, "role");
        return count(role.code(), atLeast, timeout);
    }

    /** fipc_queue_wait_for_members for the role of C value {@code role} (0: both). */
    private int count(int role, int atLeast, Duration timeout) {
        if (atLeast < 0) {
            throw new IllegalArgumentException("a negative number of members: " + atLeast);
        }
        int ms = Fipc.millis(timeout);
        MemorySegment queue = handle.acquire();
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment count = arena.allocate(JAVA_INT);
            int result = Native.queueWaitForMembers(queue, role, atLeast, count, ms);
            if (result != Native.OK) {
                throw FipcException.members(result, "fipc_queue_wait_for_members", count.get(JAVA_INT, 0));
            }
            return count.get(JAVA_INT, 0);
        } finally {
            handle.release();
        }
    }

    /**
     * Makes every {@link #waitForMembers} that waits, now or later, throw {@link Result#CANCELLED}. The queue goes on:
     * its members and their joins are unaffected. Final. Any thread; a no-op once closed.
     */
    public void cancel() {
        handle.cancel();
    }

    /**
     * Ends the queue: producers' sends throw {@link Result#DISCONNECTED} from then on, and consumers receive what is
     * left in the queue, then {@link Result#DISCONNECTED}. Cancels the queue, and closes it once no
     * {@link #waitForMembers} is in progress; the name is free again then. The members' connections, the owner's own
     * included, stay open until their close. Any thread; a no-op once closed. Later calls throw
     * {@link IllegalStateException}.
     */
    @Override
    public void close() {
        handle.close();
    }
}
