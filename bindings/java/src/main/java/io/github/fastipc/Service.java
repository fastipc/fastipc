package io.github.fastipc;

import static java.lang.foreign.ValueLayout.JAVA_INT;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.time.Duration;
import java.util.Objects;

/**
 * A service, held by its owner, from {@link Fipc#createService(String, long, int, int, int)}: it takes requests from up
 * to 64 clients and hands each to one of up to 64 servers, in any processes of the user; the server that took a
 * request replies to it, and the reply goes back to the client that sent it. A server waits for the requests of every
 * client at once (one receive), and the servers of a pool share the load: each takes the next request of any client.
 * Each client has a ring of its own in the service's memory, which every member maps, and a request takes room there
 * for itself and for its reply, so a server never waits for a client. Clients and servers join with
 * {@link Fipc#joinService(String, ServiceRole, Duration)}, the owner's own too: the owner's process admits each joiner
 * by itself, on a thread of the library's, so a member may join from any thread, the owner's included. Requests and
 * replies are of one piece, 1 to {@code maxMessage} bytes (the connections' {@link Connection#maxPiece()}).
 *
 * <p>Order: a client's receive returns the reply to its oldest request whose reply it hasn't received yet, so the
 * n-th reply it receives answers its n-th request, whichever server replies first. A request is answered once: by its
 * reply, or with {@link Result#ABANDONED} if the server that took it ended before it replied (it closed its connection,
 * or its process ended); an abandoned request may or may not have been carried out, and the service never sends it
 * again. Not covered: a server that hangs while its process runs (there are no timeouts: its client waits, within its
 * own timeouts), and the owner's process, with which the service ends.
 *
 * <p>The service ends at {@link #close()}, or when the owner's process ends: clients' sends and servers' receives then
 * throw {@link Result#DISCONNECTED}, a client receives the replies that came before the end, then
 * {@link Result#DISCONNECTED}, and a server's reply throws {@link Result#DISCONNECTED}. A member's close, or the end of
 * its process, doesn't end it.
 *
 * <p>Lifetime: a service lives until {@link #close()} or the end of its process; dropping the object doesn't end it.
 * As a {@link WorkQueue}, it is never closed by a {@link java.lang.ref.Cleaner}: its use lives in its members, in this
 * process and in others, so the garbage collector never ends it. A service that is never closed holds its name until
 * the process ends, and another {@code createService} of the name then throws {@link Result#ADDR_IN_USE}.
 *
 * <p>Threads: {@link #waitForMembers}, {@link #cancel()} and {@link #close()} may be called from any thread, at any
 * time. {@code close()} cancels the service, so a {@code waitForMembers} that waits throws {@link Result#CANCELLED},
 * and the native close runs once the call in progress has returned.
 */
public final class Service implements AutoCloseable {
    private final Handle handle;
    private final String name;

    /** Owns {@code pointer} once it returns; if it throws, nothing does. */
    private Service(MemorySegment pointer, String name) {
        this.name = name;
        this.handle = new Handle(pointer, "service", Native::serviceCancel, () -> Native.serviceClose(pointer));
    }

    /** The service that owns the native {@code pointer}; if it can't be made (out of memory), closes the pointer. */
    static Service adopt(MemorySegment pointer, String name) {
        try {
            return new Service(pointer, name);
        } catch (RuntimeException | Error e) {
            Native.serviceClose(pointer);
            throw e;
        }
    }

    /** {@return the service's name} */
    public String name() {
        return name;
    }

    /**
     * Waits for ever until at least {@code atLeast} members, clients and servers together, are joined; see
     * {@link #waitForMembers(ServiceRole, int, Duration)}.
     *
     * @param atLeast the number of members to wait for: 0 to {@code maxClients + maxServers}
     * @return how many members are joined: at least {@code atLeast}
     */
    public int waitForMembers(int atLeast) {
        return count(0, atLeast, Fipc.FOREVER);
    }

    /**
     * Waits up to {@code timeout} until at least {@code atLeast} members, clients and servers together, are joined;
     * see {@link #waitForMembers(ServiceRole, int, Duration)}.
     *
     * @param atLeast the number of members to wait for: 0 to {@code maxClients + maxServers}
     * @param timeout how long to wait; {@link Fipc#NO_WAIT} polls
     * @return how many members are joined: at least {@code atLeast}
     */
    public int waitForMembers(int atLeast, Duration timeout) {
        return count(0, atLeast, timeout);
    }

    /**
     * Waits for ever until at least {@code atLeast} members of {@code role} are joined; see
     * {@link #waitForMembers(ServiceRole, int, Duration)}.
     *
     * @param role    the role to count
     * @param atLeast the number of members to wait for: 0 to the role's maximum
     * @return how many members of the role are joined: at least {@code atLeast}
     */
    public int waitForMembers(ServiceRole role, int atLeast) {
        return waitForMembers(role, atLeast, Fipc.FOREVER);
    }

    /**
     * Waits up to {@code timeout} until at least {@code atLeast} members of {@code role} are joined, and returns how
     * many are. Joined: admitted, and not yet left; a member that leaves, or whose process ends, is counted until the
     * owner's process has seen its end, soon after. {@code atLeast} 0 returns the count at once. Clients wait while no
     * server is joined, and servers while no client is, so no member needs it; an owner may, to start its work once its
     * servers are there. Any thread.
     *
     * @param role    the role to count: {@link ServiceRole#CLIENT} or {@link ServiceRole#SERVER}
     * @param atLeast the number of members to wait for: 0 to the role's maximum ({@code maxClients} or
     *                {@code maxServers})
     * @param timeout how long to wait; {@link Fipc#NO_WAIT} polls
     * @return how many members of the role are joined: at least {@code atLeast}
     * @throws FipcException {@link Result#TIMEOUT}, {@link Result#CANCELLED} (after {@link #cancel()} or
     *                       {@link #close()}), {@link Result#DISCONNECTED} once the service has ended, each with the
     *                       count in {@link FipcException#memberCount()}; {@link Result#INVALID} for {@code atLeast}
     *                       over the role's maximum
     * @throws IllegalArgumentException for a negative {@code atLeast}
     * @throws IllegalStateException    if the service is closed
     */
    public int waitForMembers(ServiceRole role, int atLeast, Duration timeout) {
        Objects.requireNonNull(role, "role");
        return count(role.code(), atLeast, timeout);
    }

    /** fipc_service_wait_for_members for the role of C value {@code role} (0: both). */
    private int count(int role, int atLeast, Duration timeout) {
        if (atLeast < 0) {
            throw new IllegalArgumentException("a negative number of members: " + atLeast);
        }
        int ms = Fipc.millis(timeout);
        MemorySegment service = handle.acquire();
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment count = arena.allocate(JAVA_INT);
            int result = Native.serviceWaitForMembers(service, role, atLeast, count, ms);
            if (result != Native.OK) {
                throw FipcException.members(result, "fipc_service_wait_for_members", count.get(JAVA_INT, 0));
            }
            return count.get(JAVA_INT, 0);
        } finally {
            handle.release();
        }
    }

    /**
     * Makes every {@link #waitForMembers} that waits, now or later, throw {@link Result#CANCELLED}. The service goes
     * on: its members and their joins are unaffected. Final. Any thread; a no-op once closed.
     */
    public void cancel() {
        handle.cancel();
    }

    /**
     * Ends the service: clients' sends and servers' receives throw {@link Result#DISCONNECTED} from then on, a client
     * receives the replies that came before the end, then {@link Result#DISCONNECTED}, and a server's reply throws
     * {@link Result#DISCONNECTED}. Cancels the service, and closes it once no {@link #waitForMembers} is in progress;
     * the name is free again then. The members' connections, the owner's own included, stay open until their close.
     * Any thread; a no-op once closed. Later calls throw {@link IllegalStateException}.
     */
    @Override
    public void close() {
        handle.close();
    }
}
