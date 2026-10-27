package io.github.fastipc;

import static java.lang.foreign.ValueLayout.ADDRESS;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Objects;

/**
 * Where every handle comes from: each kind's create, and its connect or join. A <b>stream</b> connects one client at a
 * time to a server ({@link #createStream}, {@link Listener#accept()}, {@link #connectStream}); a <b>broadcast</b> sends
 * every message of one publisher to each of its subscribers ({@link #createBroadcast}, {@link #joinBroadcast}); a
 * <b>work queue</b> hands each message of its producers to one of its consumers ({@link #createQueue},
 * {@link #joinQueue}); a <b>service</b> hands each request of its clients to one of its servers, and the reply back
 * to its client ({@link #createService}, {@link #joinService}). Also the timeouts' constants and the native library the
 * binding loaded.
 */
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

    // === Streams ===

    /**
     * Server: claims {@code name} for a stream and returns its listener, with rings of {@code capacity} bytes each way
     * for every connection. Doesn't wait: clients may connect from now on ({@link #connectStream}), and
     * {@link Listener#accept()} sets each one up, on the calling thread.
     *
     * @param name     1-245 characters of {@code [A-Za-z0-9_.-]}, not starting with '.' or '-'; local to the user
     *                 (Linux, macOS) or to the desktop session (Windows)
     * @param capacity the size of each ring in bytes: a power of two from 1024 to 2^31
     * @return the listener; close it to free the name
     * @throws FipcException {@link Result#ADDR_IN_USE} if another stream, broadcast, queue or service holds the name
     *                       (a stream's is freed when its listener is closed or its process ends; on Windows, once the
     *                       connections it accepted are closed too); {@link Result#INVALID} for a bad name or capacity
     */
    public static Listener createStream(String name, long capacity) {
        Objects.requireNonNull(name, "name");
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment out = arena.allocate(ADDRESS);
            int result = Native.streamCreate(arena.allocateFrom(name), capacity, out);
            if (result != Native.OK) {
                throw new FipcException(result, "fipc_stream_create");
            }
            return Listener.adopt(out.get(ADDRESS, 0), name);
        }
    }

    /**
     * Connects to the stream on {@code name}, waiting for ever; see {@link #connectStream(String, Duration)}.
     *
     * @param name the stream's name
     * @return the connection
     */
    public static Connection connectStream(String name) {
        return connectStream(name, FOREVER);
    }

    /**
     * Client: connects to the stream on {@code name} and sets the connection up on the calling thread, waiting up to
     * {@code timeout} for its server to create it (it may start later, or be serving another client) and to call
     * {@link Listener#accept()}. It returns once the server's {@code accept} has offered the rings, which may be before
     * that {@code accept} returns: messages sent meanwhile wait in the ring. The client and the server run on different
     * threads (of one process or two). The connection's rings have the capacity the server chose. There is no handle
     * to cancel until it returns: use {@link #FOREVER} to wait for a server however late it starts, or a finite timeout
     * to stay responsive.
     *
     * @param name    the stream's name
     * @param timeout how long to wait; {@link #NO_WAIT} doesn't, {@link #FOREVER} waits for ever
     * @return the connection
     * @throws FipcException {@link Result#TIMEOUT} if no server's accept set a connection up in time;
     *                       {@link Result#INVALID} for a bad name, a name a broadcast, a queue or a service holds, or
     *                       a server with another version or user; {@link Result#NO_MEMORY} if the rings can't be
     *                       mapped
     */
    public static Connection connectStream(String name, Duration timeout) {
        Objects.requireNonNull(name, "name");
        int ms = millis(timeout);
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment out = arena.allocate(ADDRESS);
            int result = Native.streamConnect(arena.allocateFrom(name), out, ms);
            if (result != Native.OK) {
                throw new FipcException(result, "fipc_stream_connect");
            }
            return Connection.adopt(out.get(ADDRESS, 0), name);
        }
    }

    // === Broadcasts ===

    /**
     * Publisher: claims {@code name} for a broadcast and returns the publisher's connection, the broadcast's one
     * handle: its sends go to every joined subscriber, and {@link Connection#waitForMembers(int, Duration)} waits for a
     * number of subscribers. Doesn't wait: subscribers may join from now on ({@link #joinBroadcast}), and this process
     * admits each one by itself, on a thread of the library's. The broadcast holds up to {@code capacity / 64}
     * messages, each in its length rounded up to 64 bytes of the capacity (fewer when longer ones fill it first): a
     * gated subscriber holds the publisher back, and a lossy one is lapped, once that much is unread. Its memory, one
     * segment every subscriber maps, is set up now: for 4096 bytes and one subscriber 132 KiB, for 1 MiB and 64
     * subscribers about 1.6 MiB.
     *
     * <p>A gated subscriber whose position stays still for {@code maxStall} from the first send it held back (sends
     * that timed out meanwhile count too) is unsubscribed: its receives throw {@link Result#UNSUBSCRIBED}, and the
     * publisher goes on. A subscriber whose process ended is dropped at once; one whose whole process is paused isn't
     * unsubscribed (sends time out until it resumes or ends).
     *
     * <p>Closing the publisher ends the broadcast and frees the name: each subscriber receives what the publisher sent,
     * then {@link Result#DISCONNECTED}. A publisher that is never closed is closed once it is unreachable, which ends
     * the broadcast too.
     *
     * @param name           as {@link #createStream(String, long)}'s
     * @param capacity       the bytes of messages: a power of two from 4096 to 2^31; a message takes 1 byte to a
     *                       quarter of it ({@link Connection#maxPiece()})
     * @param maxSubscribers how many subscribers may be joined at once: 1 to 1024
     * @param maxStall       how long a gated subscriber may hold the publisher back; {@link #FOREVER} never
     *                       unsubscribes, {@link #NO_WAIT} refuses gated subscribers
     * @return the publisher's connection; close it to end the broadcast and free the name
     * @throws FipcException {@link Result#ADDR_IN_USE} if another stream, broadcast, queue or service holds the
     *                       name; {@link Result#INVALID} for a bad name, or a capacity or {@code maxSubscribers} out of
     *                       range; {@link Result#NO_MEMORY} if the memory for the whole broadcast can't be had
     */
    public static Connection createBroadcast(String name, long capacity, int maxSubscribers, Duration maxStall) {
        Objects.requireNonNull(name, "name");
        int stallMs = millis(maxStall);
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment out = arena.allocate(ADDRESS);
            int result = Native.broadcastCreate(arena.allocateFrom(name), capacity, maxSubscribers, stallMs, out);
            if (result != Native.OK) {
                throw new FipcException(result, "fipc_broadcast_create");
            }
            return Connection.adopt(out.get(ADDRESS, 0), name);
        }
    }

    /**
     * Joins the broadcast on {@code name} as a subscriber, waiting for ever; see
     * {@link #joinBroadcast(String, SubscriberRole, Duration)}.
     *
     * @param name the broadcast's name
     * @param role {@link SubscriberRole#GATED}, which misses nothing, or {@link SubscriberRole#LOSSY}
     * @return the subscriber's connection
     */
    public static Connection joinBroadcast(String name, SubscriberRole role) {
        return joinBroadcast(name, role, FOREVER);
    }

    /**
     * Subscriber: joins the broadcast on {@code name} as {@code role} and returns its connection, waiting up to
     * {@code timeout} for the publisher to create the broadcast, for a free slot (while {@code maxSubscribers} are
     * joined, a joiner waits for one to leave) and for the publisher's process to admit it. No thread of the
     * publisher's waits for it. It receives the messages sent from its admission on, then {@link Result#DISCONNECTED}
     * once the publisher is closed or its process ended. Closing it leaves the broadcast.
     *
     * @param name    the broadcast's name
     * @param role    {@link SubscriberRole#GATED}, which misses nothing, or {@link SubscriberRole#LOSSY}
     * @param timeout how long to wait; {@link #NO_WAIT} doesn't, {@link #FOREVER} waits for ever
     * @return the subscriber's connection
     * @throws FipcException {@link Result#TIMEOUT}; {@link Result#INVALID} for a name a queue or a service holds, a
     *                       publisher of another version or user, or a gated subscriber on a broadcast that refuses
     *                       them;
     *                       {@link Result#NO_MEMORY} if the broadcast can't be mapped. On a name a stream holds, the
     *                       join learns the kind when the stream's server accepts it, which brings
     *                       {@link Result#INVALID}; while no accept runs, it waits and throws {@link Result#TIMEOUT}
     */
    public static Connection joinBroadcast(String name, SubscriberRole role, Duration timeout) {
        Objects.requireNonNull(role, "role");
        return join(name, role.code(), timeout, Native::broadcastJoin, "fipc_broadcast_join", false, false);
    }

    // === Work queues ===

    /**
     * Owner: claims {@code name} for a work queue and returns it. Doesn't wait: producers and consumers may join from
     * now on ({@link #joinQueue}), the owner's own too, and this process admits each one by itself, on a thread of the
     * library's, so a member may join from any thread, this one included.
     *
     * <p>Up to {@code maxProducers} producers and {@code maxConsumers} consumers may be joined at once. Each producer
     * has a ring of {@code capacity} bytes: up to {@code capacity / 64} messages, each in its length rounded up to 64
     * bytes (fewer when longer ones fill it first), each of one piece, at most a quarter of the capacity
     * ({@link Connection#maxPiece()}). Messages wait in the queue until a consumer takes them; each goes to one
     * consumer at most (none if that consumer's process ends while it holds it), and the queue promises no order. The
     * queue's memory, one segment every member maps, is set up now: for 4096 bytes, one producer and one consumer
     * 12 KiB; for 1 MiB, 8 producers and 8 consumers about 12 MiB.
     *
     * <p>A queue lives until {@link WorkQueue#close()} or the end of its process; dropping the object doesn't end it.
     *
     * @param name         as {@link #createStream(String, long)}'s
     * @param capacity     the bytes of each producer's ring: a power of two from 4096 to 2^31
     * @param maxProducers how many producers may be joined at once: 1 to 63
     * @param maxConsumers how many consumers may be joined at once: 1 to 64
     * @return the queue; close it to end the queue and free the name
     * @throws FipcException {@link Result#ADDR_IN_USE} if another stream, broadcast, queue or service holds the
     *                       name; {@link Result#INVALID} for a bad name, or a capacity, {@code maxProducers} or
     *                       {@code maxConsumers} out of range; {@link Result#NO_MEMORY} if the queue's memory can't be
     *                       had
     */
    public static WorkQueue createQueue(String name, long capacity, int maxProducers, int maxConsumers) {
        Objects.requireNonNull(name, "name");
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment out = arena.allocate(ADDRESS);
            int result = Native.queueCreate(arena.allocateFrom(name), capacity, maxProducers, maxConsumers, out);
            if (result != Native.OK) {
                throw new FipcException(result, "fipc_queue_create");
            }
            return WorkQueue.adopt(out.get(ADDRESS, 0), name);
        }
    }

    /**
     * Joins the work queue on {@code name} as a producer or a consumer, waiting for ever; see
     * {@link #joinQueue(String, QueueRole, Duration)}.
     *
     * @param name the queue's name
     * @param role {@link QueueRole#PRODUCER} or {@link QueueRole#CONSUMER}
     * @return the member's connection
     */
    public static Connection joinQueue(String name, QueueRole role) {
        return joinQueue(name, role, FOREVER);
    }

    /**
     * Producer or consumer: joins the work queue on {@code name} as {@code role} and returns its connection, waiting up
     * to {@code timeout} for the owner to create the queue, for a free place of the role (a producer's place is free
     * again once every message its last producer sent was taken and released) and for the owner's process to admit
     * it. No thread of the owner's waits for it. A producer sends: its messages wait in the queue until a consumer
     * takes them. A consumer takes each message of the queue's producers that no other consumer took, then
     * {@link Result#DISCONNECTED} once the queue has ended. Closing it leaves the queue, which goes on: a producer's
     * messages stay for the consumers.
     *
     * @param name    the queue's name
     * @param role    {@link QueueRole#PRODUCER} or {@link QueueRole#CONSUMER}
     * @param timeout how long to wait; {@link #NO_WAIT} doesn't, {@link #FOREVER} waits for ever
     * @return the member's connection
     * @throws FipcException {@link Result#TIMEOUT}; {@link Result#INVALID} for a name a broadcast or a service
     *                       holds, or an owner of another version or user; {@link Result#NO_MEMORY} if the queue can't
     *                       be mapped. On a
     *                       name a stream holds, the join learns the kind when the stream's server accepts it, which
     *                       brings {@link Result#INVALID}; while no accept runs, it waits and throws
     *                       {@link Result#TIMEOUT}
     */
    public static Connection joinQueue(String name, QueueRole role, Duration timeout) {
        Objects.requireNonNull(role, "role");
        return join(name, role.code(), timeout, Native::queueJoin, "fipc_queue_join", false, false);
    }

    // === Services ===

    /**
     * Owner: claims {@code name} for a service and returns it. Doesn't wait: clients and servers may join from now on
     * ({@link #joinService}), the owner's own too, and this process admits each one by itself, on a thread of the
     * library's, so a member may join from any thread, this one included.
     *
     * <p>Up to {@code maxClients} clients and {@code maxServers} servers may be joined at once. Each client has a ring
     * of {@code capacity} bytes for its requests and their replies, each of one piece, 1 to {@code maxMessage} bytes
     * ({@link Connection#maxPiece()}). A request holds its length rounded up to 64 bytes, plus {@code maxMessage}
     * rounded up to 64, of its client's ring, from its send until its reply has been received, so a server never waits
     * for a client; a ring holds at most {@code capacity / 64} requests. The service's memory, one segment every member
     * maps, is set up now, whole: for 4096 bytes, one client and one server 12 KiB; for 64 KiB, 64 clients and 8
     * servers about 6.3 MiB.
     *
     * <p>A service lives until {@link Service#close()} or the end of its process; dropping the object doesn't end it.
     *
     * @param name       as {@link #createStream(String, long)}'s
     * @param capacity   the bytes of each client's ring: a power of two from 4096 to 2^31
     * @param maxMessage the longest request or reply: 1 to a quarter of the capacity
     * @param maxClients how many clients may be joined at once: 1 to 64
     * @param maxServers how many servers may be joined at once: 1 to 64
     * @return the service; close it to end the service and free the name
     * @throws FipcException {@link Result#ADDR_IN_USE} if another stream, broadcast, queue or service holds the
     *                       name; {@link Result#INVALID} for a bad name, or a capacity, {@code maxMessage},
     *                       {@code maxClients} or {@code maxServers} out of range; {@link Result#NO_MEMORY} if the
     *                       service's memory can't be had
     */
    public static Service createService(String name, long capacity, int maxMessage, int maxClients, int maxServers) {
        Objects.requireNonNull(name, "name");
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment out = arena.allocate(ADDRESS);
            int result = Native.serviceCreate(arena.allocateFrom(name), capacity, maxMessage, maxClients, maxServers,
                out);
            if (result != Native.OK) {
                throw new FipcException(result, "fipc_service_create");
            }
            return Service.adopt(out.get(ADDRESS, 0), name);
        }
    }

    /**
     * Joins the service on {@code name} as a client or a server, waiting for ever; see
     * {@link #joinService(String, ServiceRole, Duration)}.
     *
     * @param name the service's name
     * @param role {@link ServiceRole#CLIENT} or {@link ServiceRole#SERVER}
     * @return the member's connection
     */
    public static Connection joinService(String name, ServiceRole role) {
        return joinService(name, role, FOREVER);
    }

    /**
     * Client or server: joins the service on {@code name} as {@code role} and returns its connection, waiting up to
     * {@code timeout} for the owner to create the service, for a free place of the role (a client's place is free
     * again once its last client has left and every request of it that a server took has been answered) and for the
     * owner's process to admit it. No thread of the owner's waits for it. Clients wait while no server is joined, and
     * servers while no client is.
     *
     * <p>A client's {@code send} (or {@code sendAcquire}/{@code sendCommit}) sends a request, waiting for room in its
     * ring, which its replies not yet received hold too; its receives return the reply to its oldest request whose
     * reply it hasn't received yet, or throw {@link Result#ABANDONED} if that request's server ended before it replied
     * (the request counts as answered). A receive that times out or is cancelled leaves the reply to come, for the next
     * receive. {@link Connection#call(byte[], Duration)} does both: it sends a request and returns its reply. A
     * server's receives take the next request of any client, and its next send replies to it and never waits; a server
     * holds one request at a time, so its receives throw {@link Result#INVALID} until it replies, and a reply with no
     * request held throws {@link Result#INVALID}. Closing the connection leaves the service, which goes on: a client's
     * requests that no server took are dropped, and the replies to those taken are lost; the request a server holds is
     * answered {@link Result#ABANDONED}.
     *
     * @param name    the service's name
     * @param role    {@link ServiceRole#CLIENT} or {@link ServiceRole#SERVER}
     * @param timeout how long to wait; {@link #NO_WAIT} doesn't, {@link #FOREVER} waits for ever
     * @return the member's connection
     * @throws FipcException {@link Result#TIMEOUT}; {@link Result#INVALID} for a name a broadcast or a queue holds,
     *                       or an owner of another version or user; {@link Result#NO_MEMORY} if the service can't be
     *                       mapped. On a name a stream holds, the join learns the kind when the stream's server
     *                       accepts it, which brings {@link Result#INVALID}; while no accept runs, it waits and throws
     *                       {@link Result#TIMEOUT}
     */
    public static Connection joinService(String name, ServiceRole role, Duration timeout) {
        Objects.requireNonNull(role, "role");
        return join(name, role.code(), timeout, Native::serviceJoin, "fipc_service_join", role == ServiceRole.CLIENT,
            role == ServiceRole.SERVER);
    }

    /** A native join: fipc_broadcast_join, fipc_queue_join or fipc_service_join. */
    @FunctionalInterface
    private interface Join {
        int join(MemorySegment name, int role, MemorySegment outConn, int timeoutMs);
    }

    /**
     * {@code join} as the role of C value {@code role}: the member's connection ({@code serviceClient}: a service's
     * client's, which keeps count of the replies owed; {@code serviceServer}: a service's server's).
     */
    private static Connection join(String name, int role, Duration timeout, Join join, String call,
                                   boolean serviceClient, boolean serviceServer) {
        Objects.requireNonNull(name, "name");
        int ms = millis(timeout);
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment out = arena.allocate(ADDRESS);
            int result = join.join(arena.allocateFrom(name), role, out, ms);
            if (result != Native.OK) {
                throw new FipcException(result, call);
            }
            return Connection.adopt(out.get(ADDRESS, 0), name, serviceClient, serviceServer);
        }
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

    /**
     * The start of a wait of {@code ms} (as {@link #millis}) for {@link #remaining}: {@code nanoTime()} for a positive
     * timeout, else 0, as no other needs it, so that a receive without one doesn't pay for reading the clock.
     */
    static long started(int ms) {
        return ms > 0 ? System.nanoTime() : 0;
    }

    /** What is left of a timeout of {@code ms} (as {@link #millis}) since {@code started}, a {@code nanoTime()}. */
    static int remaining(int ms, long started) {
        if (ms <= 0) {
            return ms;
        }
        long elapsed = (System.nanoTime() - started) / 1_000_000;
        return (int) Math.max(0, ms - elapsed);
    }
}
