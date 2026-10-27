package io.github.fastipc;

import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.ref.Cleaner;
import java.lang.ref.Reference;
import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * One connection: two rings in shared memory, one per direction. A stream's server gets one from
 * {@link Listener#accept()}, its client from {@link Fipc#connectStream(String, Duration)}.
 *
 * <p><b>Broadcasts.</b> A broadcast's publisher ({@link Fipc#createBroadcast(String, long, int, Duration)}) and its
 * subscribers ({@link Fipc#joinBroadcast(String, SubscriberRole, Duration)}) are connections too. The publisher sends,
 * and every message goes to each subscriber; a subscriber receives. A broadcast's messages are of one piece (1 to
 * {@link #maxPiece()} bytes, a quarter of the capacity), and a publisher's receives and a subscriber's sends throw
 * {@link Result#INVALID}. Closing the publisher ends the broadcast.
 *
 * <p><b>Work queues.</b> A work queue's producers and consumers ({@link Fipc#joinQueue(String, QueueRole, Duration)},
 * the owner's own too) are connections too: a producer sends, and a consumer receives, each message going to one
 * consumer. A queue's messages are of one piece, as a broadcast's; a producer's receives and a consumer's sends throw
 * {@link Result#INVALID}. Once the queue has ended ({@link WorkQueue#close()}), a producer's sends throw
 * {@link Result#DISCONNECTED}, and a consumer receives what is left, then {@link Result#DISCONNECTED}.
 *
 * <p><b>Services.</b> A service's clients and servers ({@link Fipc#joinService(String, ServiceRole, Duration)}, the
 * owner's own too) are connections too: a client sends requests and receives their replies, in the order of its
 * requests ({@link #call(byte[], Duration)} does both), and a server receives the next request of any client and
 * replies to it with its next send, which never waits. A service's requests and replies are of one piece (1 to
 * {@link #maxPiece()} bytes, the service's {@code maxMessage}). A client's receive throws {@link Result#ABANDONED} for
 * a request whose server ended before it replied; a server's receives throw {@link Result#INVALID} while it holds a
 * request, and its sends while it holds none. Once the service has ended ({@link Service#close()}), a client's sends
 * and a server's receives throw {@link Result#DISCONNECTED}, a client receives the replies that came before the end,
 * then {@link Result#DISCONNECTED}, and a server's reply throws {@link Result#DISCONNECTED}.
 *
 * <p><b>Messages.</b> The copying calls ({@link #send(byte[])}, {@link #receive()}) take messages of any size: a
 * message longer than one piece ({@link #maxPiece()} bytes) travels in pieces and arrives whole. A copying call's
 * timeout bounds its wait for the first piece; once a piece has moved, the call goes on until the whole message has
 * (only {@link #cancel()}, {@link #close()} or the peer's end stops it, and the message is then dropped whole).
 * Messages arrive in the order they were sent; a message holds at least 1 byte. The zero-copy calls write and read a
 * message of one piece in place.
 *
 * <p><b>Buffers.</b> A {@link MemorySegment} of native memory goes to the library as it is. A byte array, a heap
 * {@link ByteBuffer} or a heap segment is copied through a native buffer of the connection (up to 1 MiB; a longer
 * message through a temporary one), so direct buffers and native segments save a copy.
 *
 * <p><b>Batches.</b> {@link #sendMany(List, Duration)} sends several messages in one native call, published together
 * with at most one wake of the receivers, and {@link #receiveMany(int, Duration)} takes every message already there in
 * one: batch both ends, as a receiver that takes one message per call falls behind a batching sender.
 *
 * <p><b>Threads.</b> One thread at a time sends ({@code send}, {@code sendMany}, {@code sendAcquire}/{@code sendCommit})
 * and one thread at a time receives ({@code receive}, {@code receiveMany}, {@code receiveAcquire}/{@code
 * receiveRelease}); the two may run at once.
 * {@link #cancel()}, {@link #close()}, {@link #maxPiece()} and {@link #name()} may be called from any thread, at any
 * time.
 *
 * <p><b>Lifetime.</b> {@link #close()} cancels the connection, so a call that waits throws {@link Result#CANCELLED},
 * and the native close runs once the calls in progress have returned: no call ever runs on a closed connection. Later
 * calls throw {@link IllegalStateException}. A zero-copy segment holds the connection open the same way, until its
 * commit or release (a service's server's request held in place: see {@link #receiveAcquire(Duration)}); it is
 * confined to the thread that acquired it, and accessing it after its commit or release throws
 * {@link IllegalStateException} instead of touching memory that is no longer the caller's.
 *
 * <p><b>The peer's end.</b> {@link Result#DISCONNECTED}: the peer closed the connection, or its process ended, for any
 * reason. Sends report it at once; receives only after they have delivered every message the peer completed. There is
 * no heartbeat and no timeout: a paused or hung peer is not reported. It is final: close the connection; to talk
 * again, accept or connect a new one.
 */
public final class Connection implements AutoCloseable {
    /**
     * The native buffers the copying calls use for heap data start at this size and grow, as messages need, up to
     * SCRATCH_MAX; a longer message goes through a temporary buffer of its own.
     */
    private static final long SCRATCH_MIN = 4096;
    private static final long SCRATCH_MAX = 1 << 20;
    /** The room {@link #receiveMany(int, Duration)} receives into first; a longer first message gets its own. */
    private static final long BATCH_BYTES = 1 << 16;
    /** The longest array a JVM allocates. */
    private static final long MAX_ARRAY = Integer.MAX_VALUE - 8;
    private static final VarHandle SENDS_OWED;

    static {
        try {
            SENDS_OWED = MethodHandles.lookup().findVarHandle(Connection.class, "sendsOwed", int.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final Handle handle;
    private final Cleaner.Cleanable cleanable;
    private final String name;
    private final int maxPiece;
    /** The out-parameters; closed with the native connection. */
    private final Arena arena;
    /** The sending thread's out-parameter: fipc_send_acquire's buffer. */
    private final MemorySegment sendOut;
    /** The receiving thread's out-parameters: the length at 0, fipc_recv_acquire's data at 8. */
    private final MemorySegment receiveOut;
    /** receiveOut's slice at 8: fipc_recv_acquire's data pointer. */
    private final MemorySegment receiveData;

    // A service's client keeps count, oldest first, of the requests whose replies it hasn't received yet, so that call
    // never returns the reply to another request: first those whose call gave up on its reply (it timed out or was
    // cancelled), which the next call receives and drops (at most one, as a call first drops what is owed), then those
    // sent with send or sendCommit, whose replies are the caller's to receive. A request is counted before its send,
    // since a thread that receives may take its reply before the send returns, and uncounted if the send fails; a
    // reply or ABANDONED takes the oldest off. The sending thread adds to sendsOwed while the receiving thread takes
    // off, hence the atomic operations (SENDS_OWED); callsOwed is call's, which sends and receives with no other thread
    // doing either, and the receiving thread's.
    private final boolean serviceClient;
    /** A service's server's connection: its reply, and its close, end the request it holds in place. */
    private final boolean serviceServer;
    private int callsOwed;
    @SuppressWarnings("unused") // through SENDS_OWED
    private volatile int sendsOwed;
    /**
     * Set by {@link #cancel()} (any thread) before the native cancel: a call then sends nothing, as it could never
     * receive the reply; one that doesn't see it sent its request before the cancel.
     */
    private volatile boolean cancelled;

    // The sending thread's state
    private MemorySegment sendScratch;
    private Arena sendTemporary;
    private MemorySegment sendHeld;
    private Arena sendHeldArena;

    // The receiving thread's state
    private MemorySegment receiveScratch;
    /** receiveMany's lengths, fipc_recv_many's size_t array. */
    private MemorySegment receiveLengths;
    private Arena receiveTemporary;
    private Arena receiveHeldArena;
    /**
     * A server's: the thread that received the request held in place, while its segment is valid; that thread alone
     * can invalidate the segment, so only it may reply meanwhile. Other threads read it to see that they aren't that
     * thread: one that replies has learnt of the request, or of its release, from the receiving thread, which set this
     * or cleared it before.
     */
    private Thread receiveHolder;

    /** Owns {@code pointer} once it returns; if it throws, nothing does, and it has freed what it took. */
    private Connection(MemorySegment pointer, String name, boolean serviceClient, boolean serviceServer) {
        Arena arena = Arena.ofShared();
        this.arena = arena;
        this.serviceClient = serviceClient;
        this.serviceServer = serviceServer;
        try {
            this.sendOut = arena.allocate(16, 16);
            this.receiveOut = arena.allocate(16, 16);
            this.receiveData = receiveOut.asSlice(8, 8);
            this.name = name;
            this.maxPiece = (int) Native.maxPiece(pointer);
            this.handle = new Handle(pointer, "connection", Native::cancel, () -> {
                Native.close(pointer);
                arena.close();
            });
            this.cleanable = Handle.CLEANER.register(this, handle::close); // last: the cleaner owns the pointer
        } catch (RuntimeException | Error e) {
            arena.close();
            throw e;
        }
    }

    /** The connection that owns the native {@code pointer}; if it can't be made (out of memory), closes the pointer. */
    static Connection adopt(MemorySegment pointer, String name) {
        return adopt(pointer, name, false, false);
    }

    /**
     * As {@link #adopt(MemorySegment, String)}; {@code serviceClient}: a service's client's connection, which keeps
     * count of the replies owed; {@code serviceServer}: a service's server's, whose reply ends the request it holds in
     * place.
     */
    static Connection adopt(MemorySegment pointer, String name, boolean serviceClient, boolean serviceServer) {
        try {
            return new Connection(pointer, name, serviceClient, serviceServer);
        } catch (RuntimeException | Error e) {
            Native.close(pointer);
            throw e;
        }
    }

    /** {@return the name of the stream, broadcast, queue or service this connection belongs to} */
    public String name() {
        return name;
    }

    /**
     * {@return the longest message the zero-copy calls take: on a stream the ring's capacity less 64 bytes; on a
     * broadcast or a work queue a quarter of its capacity, the longest message it takes; on a service its
     * {@code maxMessage}, the longest request or reply} Any thread.
     */
    public int maxPiece() {
        return maxPiece;
    }

    /**
     * {@return how many subscribers are joined to this broadcast's publisher} Doesn't wait; see
     * {@link #waitForMembers(int, Duration)}.
     */
    public int waitForMembers() {
        return waitForMembers(0, Fipc.NO_WAIT);
    }

    /**
     * A broadcast's publisher: waits for ever until at least {@code atLeast} subscribers are joined; see
     * {@link #waitForMembers(int, Duration)}.
     *
     * @param atLeast the number of subscribers to wait for: 0 to the broadcast's {@code maxSubscribers}
     * @return how many subscribers are joined: at least {@code atLeast}
     */
    public int waitForMembers(int atLeast) {
        return waitForMembers(atLeast, Fipc.FOREVER);
    }

    /**
     * A broadcast's publisher: waits up to {@code timeout} until at least {@code atLeast} subscribers are joined, and
     * returns how many are. Joined: admitted, and not yet left, dropped or unsubscribed; the subscribers the
     * publisher's sends reach. To send nothing before N subscribers are there, call it with N before the first send:
     * the publisher's process admits them by itself meanwhile. {@code atLeast} 0 returns the count at once. A
     * subscriber that leaves, or whose process ends, is counted until the publisher's process has seen its end, soon
     * after. Any thread.
     *
     * @param atLeast the number of subscribers to wait for: 0 to the broadcast's {@code maxSubscribers}
     * @param timeout how long to wait; {@link Fipc#NO_WAIT} polls
     * @return how many subscribers are joined: at least {@code atLeast}
     * @throws FipcException {@link Result#TIMEOUT}, {@link Result#CANCELLED} (after {@link #cancel()}), each with the
     *                       count in {@link FipcException#memberCount()}; {@link Result#INVALID} on a connection other
     *                       than a broadcast's publisher, or for {@code atLeast} over its {@code maxSubscribers}
     * @throws IllegalArgumentException for a negative {@code atLeast}
     * @throws IllegalStateException    if the connection is closed
     */
    public int waitForMembers(int atLeast, Duration timeout) {
        if (atLeast < 0) {
            throw new IllegalArgumentException("a negative number of subscribers: " + atLeast);
        }
        int ms = Fipc.millis(timeout);
        MemorySegment publisher = handle.acquire();
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment count = arena.allocate(JAVA_INT);
            int result = Native.broadcastWaitForMembers(publisher, atLeast, count, ms);
            if (result != Native.OK) {
                throw FipcException.members(result, "fipc_broadcast_wait_for_members", count.get(JAVA_INT, 0));
            }
            return count.get(JAVA_INT, 0);
        } finally {
            handle.release();
            Reference.reachabilityFence(this);
        }
    }

    // === Messages ===

    /**
     * Sends one message, waiting for ever for room; see {@link #send(MemorySegment, Duration)}.
     *
     * @param message the message: at least 1 byte, any size
     */
    public void send(byte[] message) {
        send(MemorySegment.ofArray(message), Fipc.FOREVER);
    }

    /**
     * Sends one message; see {@link #send(MemorySegment, Duration)}.
     *
     * @param message the message: at least 1 byte, any size
     * @param timeout how long to wait for room for the first piece
     */
    public void send(byte[] message, Duration timeout) {
        send(MemorySegment.ofArray(message), timeout);
    }

    /**
     * Sends {@code length} bytes of {@code message} from {@code offset} as one message; see
     * {@link #send(MemorySegment, Duration)}.
     *
     * @param message the bytes
     * @param offset  where the message starts in {@code message}
     * @param length  its length: at least 1
     * @param timeout how long to wait for room for the first piece
     */
    public void send(byte[] message, int offset, int length, Duration timeout) {
        Objects.checkFromIndexSize(offset, length, message.length);
        send(MemorySegment.ofArray(message).asSlice(offset, length), timeout);
    }

    /**
     * Sends the buffer's remaining bytes (from its position to its limit) as one message, and moves its position to
     * its limit; see {@link #send(MemorySegment, Duration)}.
     *
     * @param message the message
     * @param timeout how long to wait for room for the first piece
     */
    public void send(ByteBuffer message, Duration timeout) {
        send(MemorySegment.ofBuffer(message), timeout);
        message.position(message.limit());
    }

    /**
     * Sends one message, waiting for ever for room; see {@link #send(MemorySegment, Duration)}.
     *
     * @param message the message: at least 1 byte, any size
     */
    public void send(MemorySegment message) {
        send(message, Fipc.FOREVER);
    }

    /**
     * Sends one message of any size (at least 1 byte): waits up to {@code timeout} for room for its first piece, then
     * copies it in, in pieces if it is longer than one (going on past the timeout until the whole message is in).
     * Drops a zero-copy reservation that was never committed. On a broadcast, a work queue or a service, a message is
     * of one piece: {@link Result#TOO_LARGE} over {@link #maxPiece()}. A work queue's producer waits for room while the
     * consumers haven't taken its earlier messages. A service's client sends a request, waiting for room in its ring
     * (which its replies not yet received hold too); a server replies to the request it holds, and never waits. A
     * server's reply (one that succeeds or throws {@link Result#DISCONNECTED}) invalidates the segment of the request
     * it holds in place; on another thread than the one that received it in place, the reply is refused until that
     * segment's release ({@link #receiveAcquire(Duration)}).
     *
     * @param message the message: at least 1 byte
     * @param timeout how long to wait for room for the first piece
     * @throws FipcException {@link Result#TIMEOUT}, {@link Result#CANCELLED}, {@link Result#DISCONNECTED};
     *                       {@link Result#INVALID} for an empty message, or a service's server that holds no request;
     *                       {@link Result#TOO_LARGE} over {@link #maxPiece()} on a broadcast, a work queue or a service
     * @throws IllegalStateException if the connection is closed
     * @throws WrongThreadException  a service's server's reply on another thread than the one that holds the request's
     *                               segment in place, until its release there (nothing is sent)
     */
    public void send(MemorySegment message, Duration timeout) {
        int ms = Fipc.millis(timeout);
        if (!serviceClient) {
            if (serviceServer) {
                checkReplyThread();
            }
            int result = sendNative(message, ms);
            if (serviceServer) {
                replied(result);
            }
            check(result, "fipc_send");
            return;
        }
        SENDS_OWED.getAndAdd(this, 1); // its reply is owed from before the send
        int result = Native.INVALID;
        try {
            result = sendNative(message, ms);
        } finally {
            if (result != Native.OK) {
                SENDS_OWED.getAndAdd(this, -1);
            }
        }
        check(result, "fipc_send");
    }

    /** fipc_send of {@code message}, waiting up to {@code ms} (as {@link Fipc#millis}): the result. */
    private int sendNative(MemorySegment message, int ms) {
        MemorySegment conn = enterSend();
        try {
            return Native.send(conn, outgoing(message), message.byteSize(), ms);
        } finally {
            exitSend();
        }
    }

    /**
     * Receives one message of any size into a new array, waiting for ever; see {@link #receive(Duration)}.
     *
     * @return the message
     */
    public byte[] receive() {
        return receive(Fipc.FOREVER);
    }

    /**
     * Receives one message of any size into a new array of its length: waits up to {@code timeout} for its first
     * piece, then goes on until the whole message is in. A message longer than the connection's buffer is received
     * again into a longer one; on a work queue another consumer may take it first, and the call then receives the next
     * message, within what is left of {@code timeout}. A service's client receives the reply to its oldest request
     * whose reply it hasn't received yet; a receive that times out or is cancelled leaves that reply to come, for the
     * next receive, and after a {@link #call(byte[], Duration) call} that gave up, that call's late reply comes first.
     * A service's server takes the next request of any client and holds it until it replies.
     *
     * @param timeout how long to wait for the first piece
     * @return the message
     * @throws FipcException {@link Result#TIMEOUT}, {@link Result#CANCELLED}, {@link Result#DISCONNECTED} (once every
     *                       message the peer completed has been received); {@link Result#TOO_LARGE} for a message
     *                       longer than an array can be, which stays queued for
     *                       {@link #receive(MemorySegment, Duration)}; a broadcast's subscriber:
     *                       {@link Result#OVERRUN} (lossy; the number of messages lost in
     *                       {@link FipcException#lostMessages()}), {@link Result#UNSUBSCRIBED} (gated); a service's
     *                       client: {@link Result#ABANDONED} if the server that took the request ended before it
     *                       replied (the request counts as answered: the next receive returns the next reply); a
     *                       service's server: {@link Result#INVALID} while it holds a request
     * @throws IllegalStateException if the connection is closed
     */
    public byte[] receive(Duration timeout) {
        return receiveArray(Fipc.millis(timeout));
    }

    /** {@link #receive(Duration)}, waiting up to {@code ms} (as {@link Fipc#millis}). */
    private byte[] receiveArray(int ms) {
        long started = Fipc.started(ms);
        MemorySegment conn = enterReceive();
        try {
            MemorySegment buffer = receiveScratch(SCRATCH_MIN);
            int result = Native.recv(conn, buffer, buffer.byteSize(), receiveOut, ms);
            long length = receiveOut.get(JAVA_LONG, 0);
            while (result == Native.TOO_LARGE && length <= MAX_ARRAY) {
                // The message stays queued, so this receive doesn't wait for it, unless another consumer of a work
                // queue took it first: then it waits for the next, which may be longer still
                buffer = incoming(length);
                result = Native.recv(conn, buffer, length, receiveOut, Fipc.remaining(ms, started));
                length = receiveOut.get(JAVA_LONG, 0);
            }
            answered(result);
            if (result != Native.OK) {
                throw FipcException.receive(result, "fipc_recv", length);
            }
            return buffer.asSlice(0, length).toArray(JAVA_BYTE);
        } finally {
            exitReceive();
        }
    }

    /**
     * Receives one message into the buffer's remaining bytes, waiting for ever; see
     * {@link #receive(ByteBuffer, Duration)}.
     *
     * @param buffer where the message goes, from its position
     * @return the message's length
     */
    public int receive(ByteBuffer buffer) {
        return receive(buffer, Fipc.FOREVER);
    }

    /**
     * Receives one message into the buffer, from its position up to its limit, and moves its position past the
     * message; see {@link #receive(MemorySegment, Duration)}.
     *
     * @param buffer  where the message goes, from its position; not read-only
     * @param timeout how long to wait for the first piece
     * @return the message's length
     * @throws FipcException {@link Result#TOO_LARGE} if the message doesn't fit the buffer's remaining bytes: nothing
     *                       is taken, and {@link FipcException#messageLength()} says how much room it needs
     */
    public int receive(ByteBuffer buffer, Duration timeout) {
        if (buffer.isReadOnly()) {
            throw new ReadOnlyBufferException();
        }
        int length = (int) receive(MemorySegment.ofBuffer(buffer), timeout);
        buffer.position(buffer.position() + length);
        return length;
    }

    /**
     * Receives one message into the segment, waiting for ever; see {@link #receive(MemorySegment, Duration)}.
     *
     * @param buffer where the message goes
     * @return the message's length
     */
    public long receive(MemorySegment buffer) {
        return receive(buffer, Fipc.FOREVER);
    }

    /**
     * Receives one message of any size into {@code buffer}: waits up to {@code timeout} for its first piece, then
     * copies it out, going on until the whole message is in, and frees its room. A heap segment's message goes through
     * a native buffer, received again into a longer one if it doesn't fit; on a work queue another consumer may take it
     * first, and the call then receives the next message, within what is left of {@code timeout}.
     *
     * @param buffer  where the message goes, from its start; writable
     * @param timeout how long to wait for the first piece
     * @return the message's length
     * @throws FipcException {@link Result#TOO_LARGE} if the message is longer than the buffer: nothing is taken, and
     *                       {@link FipcException#messageLength()} says how much room it needs; {@link Result#TIMEOUT},
     *                       {@link Result#CANCELLED}, {@link Result#DISCONNECTED}; a broadcast's subscriber:
     *                       {@link Result#OVERRUN} (lossy; the number of messages lost in
     *                       {@link FipcException#lostMessages()}), {@link Result#UNSUBSCRIBED} (gated); a service's
     *                       client: {@link Result#ABANDONED}; a service's server: {@link Result#INVALID} while it holds
     *                       a request. After any of them the buffer's contents are unspecified.
     * @throws IllegalStateException if the connection is closed
     */
    public long receive(MemorySegment buffer, Duration timeout) {
        if (buffer.isReadOnly()) {
            throw new IllegalArgumentException("a read-only segment");
        }
        int ms = Fipc.millis(timeout);
        long started = Fipc.started(ms);
        MemorySegment conn = enterReceive();
        try {
            long capacity = buffer.byteSize();
            MemorySegment target = buffer.isNative() ? buffer : receiveScratch(Math.min(capacity, SCRATCH_MAX));
            int result = Native.recv(conn, target, Math.min(capacity, target.byteSize()), receiveOut, ms);
            long length = receiveOut.get(JAVA_LONG, 0);
            while (result == Native.TOO_LARGE && target != buffer && length <= capacity) {
                // As in receive(Duration): another consumer of a work queue may take the message first
                target = incoming(length);
                result = Native.recv(conn, target, length, receiveOut, Fipc.remaining(ms, started));
                length = receiveOut.get(JAVA_LONG, 0);
            }
            answered(result);
            if (result != Native.OK) {
                throw FipcException.receive(result, "fipc_recv", length);
            }
            if (target != buffer) {
                MemorySegment.copy(target, 0, buffer, 0, length);
            }
            return length;
        } finally {
            exitReceive();
        }
    }

    // === Batches ===

    /**
     * Sends {@code messages} in order, published together, waiting for ever for room; see
     * {@link #sendMany(List, Duration)}.
     *
     * @param messages the messages: at least one, each of at least 1 byte
     * @return how many went: all of them
     */
    public int sendMany(List<byte[]> messages) {
        return sendMany(messages, Fipc.FOREVER);
    }

    /**
     * Sends {@code messages} (at least one, each of at least 1 byte) in order in one native call (fipc_send_many) and
     * publishes them together, with at most one wake of the receivers for the batch, and returns how many went: all of
     * them. {@code timeout} bounds the whole call ({@link Fipc#NO_WAIT}: only what fits now). The messages go through a
     * native buffer of the connection (up to 1 MiB in all; a longer batch through a temporary one). Every message is
     * checked first, before any goes. A service's client sends a batch of requests, whose replies come in their order
     * and are owed as {@code send}'s are. Batch both ends: a receiver that takes one message per call falls behind a
     * batching sender, and the two run slower than without batches.
     *
     * @param messages the messages: at least one, each of at least 1 byte
     * @param timeout  how long the whole call may wait for room
     * @return how many went: all of them
     * @throws FipcException the first message that didn't go, as {@code send} would throw it, with how many went (all
     *                       those before it) in {@link FipcException#sentMessages()}: {@link Result#TIMEOUT},
     *                       {@link Result#CANCELLED}, {@link Result#DISCONNECTED}; {@link Result#INVALID} for an empty
     *                       message or an empty batch, on a subscriber, a consumer or a service's server (which
     *                       replies with {@code send}); {@link Result#TOO_LARGE} over {@link #maxPiece()} on a
     *                       broadcast, a work queue or a service (a stream sends a longer message in pieces)
     * @throws IllegalStateException if the connection is closed
     */
    public int sendMany(List<byte[]> messages, Duration timeout) {
        int ms = Fipc.millis(timeout);
        int count = messages.size();
        for (byte[] message : messages) {
            Objects.requireNonNull(message, "message");
        }
        if (serviceClient) {
            SENDS_OWED.getAndAdd(this, count); // their replies are owed from before the send
        }
        int result = Native.INVALID;
        int sent = 0;
        try {
            MemorySegment conn = enterSend();
            try {
                result = sendManyNative(conn, messages, ms);
                sent = (int) sendOut.get(JAVA_LONG, 0);
            } finally {
                exitSend();
            }
        } finally {
            if (serviceClient && sent < count) {
                SENDS_OWED.getAndAdd(this, sent - count); // nothing is owed to those that didn't go
            }
        }
        if (result != Native.OK) {
            throw FipcException.sendMany(result, sent);
        }
        return sent;
    }

    /**
     * fipc_send_many of {@code messages}, copied into native memory: each message's address, then each one's length,
     * then the messages; the count that went in {@code sendOut}.
     */
    private int sendManyNative(MemorySegment conn, List<byte[]> messages, int ms) {
        int count = messages.size();
        long total = 0;
        for (byte[] message : messages) {
            total += message.length;
        }
        long table = 16L * count;
        MemorySegment room = sendRoom(table + total);
        long at = table;
        for (int i = 0; i < count; i++) {
            byte[] message = messages.get(i);
            room.setAtIndex(JAVA_LONG, i, room.address() + at);
            room.setAtIndex(JAVA_LONG, count + i, message.length);
            MemorySegment.copy(message, 0, room, JAVA_BYTE, at, message.length);
            at += message.length;
        }
        return Native.sendMany(conn, room, room.asSlice(8L * count), count, sendOut, ms);
    }

    /**
     * Receives a batch of up to {@code maxCount} messages, waiting for ever for the first; see
     * {@link #receiveMany(int, Duration)}.
     *
     * @param maxCount the most messages to take: at least 1
     * @return the messages, in order: at least one
     */
    public List<byte[]> receiveMany(int maxCount) {
        return receiveMany(maxCount, Fipc.FOREVER);
    }

    /**
     * Waits up to {@code timeout} for the next message, as {@link #receive(Duration)}, then takes every further message
     * already there, up to {@code maxCount} and as many as fit the connection's buffer (64 KiB; a longer first message
     * is received alone into one of its length), in one native call (fipc_recv_many), and returns each in a new array,
     * in order. After the first it never waits, and stops before a message that isn't there yet, doesn't fit, is in
     * pieces, or would bring another result: the next receive returns it. A broadcast's subscriber takes, after its
     * first, only the messages its publisher has moved past (the newest come in the next calls); a work queue's
     * consumer acknowledges each message before the call returns; a service's client receives its replies in its
     * requests' order.
     *
     * @param maxCount the most messages to take: at least 1
     * @param timeout  how long to wait for the first message
     * @return the messages, in order: at least one
     * @throws FipcException the first message's result, as {@link #receive(Duration)} throws it, with nothing else
     *                       taken: {@link Result#TIMEOUT}, {@link Result#CANCELLED}, {@link Result#DISCONNECTED},
     *                       {@link Result#OVERRUN} (the number lost in {@link FipcException#lostMessages()}),
     *                       {@link Result#UNSUBSCRIBED}, a service client's {@link Result#ABANDONED};
     *                       {@link Result#INVALID} for a {@code maxCount} of 0, on a publisher, a producer and a
     *                       service's server (which holds one request at a time)
     * @throws IllegalArgumentException for a negative {@code maxCount}
     * @throws IllegalStateException    if the connection is closed
     */
    public List<byte[]> receiveMany(int maxCount, Duration timeout) {
        if (maxCount < 0) {
            throw new IllegalArgumentException("a negative number of messages: " + maxCount);
        }
        int ms = Fipc.millis(timeout);
        long started = Fipc.started(ms);
        MemorySegment conn = enterReceive();
        try {
            MemorySegment lengths = receiveLengths(maxCount);
            MemorySegment buffer = receiveScratch(BATCH_BYTES);
            int result = Native.recvMany(conn, buffer, buffer.byteSize(), lengths, maxCount, receiveOut, ms);
            long first = lengths.get(JAVA_LONG, 0);
            while (result == Native.TOO_LARGE && first <= MAX_ARRAY) {
                // The first message stays queued, so this receive doesn't wait for it, unless another consumer of a
                // work queue took it first: then it waits for the next, which may be longer still
                buffer = incoming(first);
                result = Native.recvMany(conn, buffer, buffer.byteSize(), lengths, maxCount, receiveOut,
                    Fipc.remaining(ms, started));
                first = lengths.get(JAVA_LONG, 0);
            }
            int count = (int) receiveOut.get(JAVA_LONG, 0);
            answeredMany(result, count);
            if (result != Native.OK) {
                throw FipcException.receive(result, "fipc_recv_many", first);
            }
            List<byte[]> messages = new ArrayList<>(count);
            long at = 0;
            for (int i = 0; i < count; i++) {
                long length = lengths.getAtIndex(JAVA_LONG, i);
                messages.add(buffer.asSlice(at, length).toArray(JAVA_BYTE));
                at += length;
            }
            return messages;
        } finally {
            exitReceive();
        }
    }

    /**
     * Receives a batch into {@code buffer}, as {@link #receiveMany(int, Duration)}, of up to {@code lengths.length}
     * messages packed back to back: message i is {@code lengths[i]} bytes, at the sum of the lengths before it. A heap
     * segment's batch goes through a native buffer.
     *
     * @param buffer  where the messages go, from its start; writable
     * @param lengths where their lengths go: at least one entry, the most messages to take
     * @param timeout how long to wait for the first message
     * @return how many it took: at least one
     * @throws FipcException as {@link #receiveMany(int, Duration)}, and {@link Result#TOO_LARGE} if the first message
     *                       is longer than the buffer: nothing is taken, and {@link FipcException#messageLength()}
     *                       says how much room it needs. After any of them the buffer's contents are unspecified.
     * @throws IllegalStateException if the connection is closed
     */
    public int receiveMany(MemorySegment buffer, long[] lengths, Duration timeout) {
        if (buffer.isReadOnly()) {
            throw new IllegalArgumentException("a read-only segment");
        }
        int ms = Fipc.millis(timeout);
        long started = Fipc.started(ms);
        MemorySegment conn = enterReceive();
        try {
            int maxCount = lengths.length;
            MemorySegment lens = receiveLengths(maxCount);
            long capacity = buffer.byteSize();
            MemorySegment target = buffer.isNative() ? buffer : receiveScratch(Math.min(capacity, SCRATCH_MAX));
            int result = Native.recvMany(conn, target, Math.min(capacity, target.byteSize()), lens, maxCount,
                receiveOut, ms);
            long first = lens.get(JAVA_LONG, 0);
            while (result == Native.TOO_LARGE && target != buffer && first <= capacity) {
                // As in receiveMany(int, Duration): another consumer of a work queue may take the message first
                target = incoming(first);
                result = Native.recvMany(conn, target, Math.min(capacity, target.byteSize()), lens, maxCount,
                    receiveOut, Fipc.remaining(ms, started));
                first = lens.get(JAVA_LONG, 0);
            }
            int count = (int) receiveOut.get(JAVA_LONG, 0);
            answeredMany(result, count);
            if (result != Native.OK) {
                throw FipcException.receive(result, "fipc_recv_many", first);
            }
            long total = 0;
            for (int i = 0; i < count; i++) {
                lengths[i] = lens.getAtIndex(JAVA_LONG, i);
                total += lengths[i];
            }
            if (target != buffer) {
                MemorySegment.copy(target, 0, buffer, 0, total);
            }
            return count;
        } finally {
            exitReceive();
        }
    }

    /** A service client's batched receive returned {@code result} with {@code count} replies: each answers one. */
    private void answeredMany(int result, int count) {
        if (result != Native.OK) {
            answered(result);
            return;
        }
        for (int i = 0; i < count; i++) {
            answered(result);
        }
    }

    // === A service's client: a request and its reply ===

    /**
     * A service's client: sends {@code request} and returns its reply, waiting for ever; see
     * {@link #call(byte[], Duration)}.
     *
     * @param request the request: 1 to {@link #maxPiece()} bytes
     * @return the reply
     */
    public byte[] call(byte[] request) {
        return call(request, Fipc.FOREVER);
    }

    /**
     * A service's client: sends {@code request} and returns its reply, in a new array, waiting up to {@code timeout}
     * in all. A call checks its request first: {@link Result#INVALID} for an empty one, {@link Result#TOO_LARGE}
     * over {@link #maxPiece()}, at once, even while a reply is owed. Then a call on a cancelled connection throws
     * {@link Result#CANCELLED} at once, whatever its timeout, and sends nothing: it could never receive the reply. A
     * call never returns the reply to another request: the connection keeps count of the replies still owed, and a
     * call whose receive timed out or was cancelled leaves its reply owed, which the next call first receives and
     * drops (at most one is owed, since every call drops it first; an {@link Result#ABANDONED} in its place counts
     * too), within its own timeout and before it sends: if it doesn't come in time, the call's request is never sent.
     * A call that had to drop an owed reply first sends its request only if its timeout has time left: once a positive
     * timeout has run out, it throws {@link Result#TIMEOUT} without sending, so no server sees that request; with a
     * zero timeout no step waits, and it sends if there is room. After a call that gave up, a plain
     * {@link #receive(Duration)} returns that call's late reply (or {@link Result#ABANDONED}) first: replies come in
     * request order. A call sends and receives, so no other thread may send or receive on the connection meanwhile.
     *
     * @param request the request: 1 to {@link #maxPiece()} bytes
     * @param timeout how long to wait in all: for a reply owed to an earlier call, for room for the request, and for
     *                its reply
     * @return the reply
     * @throws FipcException {@link Result#ABANDONED} if the server that took the request ended before it replied (the
     *                       request counts as answered; it may or may not have been carried out, and it is never sent
     *                       again); {@link Result#TIMEOUT}, {@link Result#CANCELLED} (after {@link #cancel()}, at
     *                       once, sending nothing; else the request's reply, if it was sent, stays owed),
     *                       {@link Result#DISCONNECTED}; {@link Result#TOO_LARGE} over {@link #maxPiece()};
     *                       {@link Result#INVALID} for an empty request, while a request sent with {@code send} or
     *                       {@code sendCommit} still has its reply to be received (that reply is the caller's: receive
     *                       it first), or on any connection other than a service's client
     * @throws IllegalStateException if the connection is closed
     */
    public byte[] call(byte[] request, Duration timeout) {
        Objects.requireNonNull(request, "request");
        int ms = Fipc.millis(timeout);
        long started = Fipc.started(ms);
        if (!serviceClient) {
            if (handle.isClosed()) {
                throw handle.closed();
            }
            throw new FipcException(Native.INVALID, "call");
        }
        if (request.length == 0) {
            throw new FipcException(Native.INVALID, "call");
        }
        if (request.length > maxPiece) {
            throw new FipcException(Native.TOO_LARGE, "call");
        }
        if (cancelled) {
            if (handle.isClosed()) {
                throw handle.closed();
            }
            // Its reply could never be received: the request isn't sent
            throw new FipcException(Native.CANCELLED, "call");
        }
        if ((int) SENDS_OWED.getVolatile(this) != 0) {
            if (handle.isClosed()) {
                throw handle.closed();
            }
            // A reply to a request of send's is the caller's, not the call's to drop
            throw new FipcException(Native.INVALID, "call");
        }
        boolean dropped = false;
        while (callsOwed > 0) { // replies owed to calls that gave up on them: dropped
            try {
                receiveArray(Fipc.remaining(ms, started));
            } catch (FipcException e) {
                if (e.result() != Result.ABANDONED) {
                    throw e;
                }
            }
            dropped = true;
        }
        if (dropped && ms > 0 && System.nanoTime() - started >= timeout.toNanos()) {
            // Its timeout ran out while it dropped that reply: the request isn't sent, so no server sees it
            throw new FipcException(Native.TIMEOUT, "call");
        }
        if (cancelled) {
            throw new FipcException(Native.CANCELLED, "call"); // cancelled while it dropped: the request isn't sent
        }
        callsOwed = 1; // its reply is owed from before the send
        int result = Native.INVALID;
        try {
            result = sendNative(MemorySegment.ofArray(request), Fipc.remaining(ms, started));
        } finally {
            if (result != Native.OK) {
                callsOwed = 0;
            }
        }
        check(result, "fipc_send");
        return receiveArray(Fipc.remaining(ms, started)); // a reply or ABANDONED takes it off; else it stays owed
    }

    /** A service client's receive returned {@code result}: a reply or ABANDONED answers the oldest request owed one. */
    private void answered(int result) {
        if (!serviceClient || (result != Native.OK && result != Native.ABANDONED)) {
            return;
        }
        if (callsOwed > 0) {
            callsOwed--;
        } else if ((int) SENDS_OWED.getVolatile(this) > 0) {
            SENDS_OWED.getAndAdd(this, -1);
        }
    }

    // === Zero-copy: messages of one piece, up to maxPiece() bytes ===

    /**
     * Zero-copy send, step 1, waiting for ever for room; see {@link #sendAcquire(int, Duration)}.
     *
     * @param length the bytes to reserve: 1 to {@link #maxPiece()}
     * @return the reserved bytes in the ring
     */
    public MemorySegment sendAcquire(int length) {
        return sendAcquire(length, Fipc.FOREVER);
    }

    /**
     * Zero-copy send, step 1: waits up to {@code timeout} for {@code length} contiguous bytes in the ring and returns
     * them (16-byte aligned). Write the message there, then {@link #sendCommit(int)}. The segment is valid until the
     * commit, or the next send of any kind (which drops a reservation that was never committed); it is confined to
     * this thread, and holds the connection open until then. The room is contiguous: a reservation that doesn't fit
     * before the ring's end waits until the receiver has read past the end, even in an empty ring, so a long one (near
     * {@link #maxPiece()}) can wait for the peer's next receive call.
     *
     * @param length  the bytes to reserve: 1 to {@link #maxPiece()}
     * @param timeout how long to wait for room
     * @return the reserved bytes in the ring
     * @throws FipcException {@link Result#TOO_LARGE} over {@link #maxPiece()} (send it with {@code send});
     *                       {@link Result#TIMEOUT}, {@link Result#CANCELLED}, {@link Result#DISCONNECTED};
     *                       {@link Result#INVALID} for a length of 0
     * @throws IllegalStateException if the connection is closed
     */
    public MemorySegment sendAcquire(int length, Duration timeout) {
        if (length < 0) {
            throw new IllegalArgumentException("a negative length: " + length);
        }
        int ms = Fipc.millis(timeout);
        MemorySegment conn = enterSend();
        boolean held = false;
        try {
            int result = Native.sendAcquire(conn, length, sendOut, ms);
            check(result, "fipc_send_acquire");
            Arena reservation = Arena.ofConfined();
            MemorySegment room = sendOut.get(ADDRESS, 0).reinterpret(length, reservation, null);
            sendHeld = room;
            sendHeldArena = reservation;
            held = true; // the reservation keeps the call's reference until it is committed or dropped
            return room;
        } finally {
            if (!held) {
                exitSend();
            }
        }
    }

    /**
     * Zero-copy send, step 2: sends the first {@code length} bytes (1 to the acquired length) of the reservation as
     * one message. Doesn't wait. The reservation's segment is invalid afterwards, whatever the result but
     * {@link Result#INVALID}. Call it on the thread that acquired the reservation. A service's client sends a request
     * so, and a server its reply, which, as {@code send}'s, invalidates the segment of the request it holds in place,
     * and is refused on another thread until that segment's release.
     *
     * @param length the message's length: 1 to the acquired length
     * @throws FipcException {@link Result#INVALID} without a reservation, or for a length out of range (the
     *                       reservation stays, for a valid one); {@link Result#DISCONNECTED} (the reservation is used
     *                       up)
     * @throws IllegalStateException if the connection is closed (the reservation is dropped)
     * @throws WrongThreadException  on another thread than the one that acquired the reservation; a service's server's
     *                               reply on another thread than the one that holds the request's segment in place,
     *                               until its release there (the reservation stays)
     */
    public void sendCommit(int length) {
        MemorySegment room = sendHeld;
        if (room == null) {
            if (handle.isClosed()) {
                throw handle.closed();
            }
            throw new FipcException(Native.INVALID, "fipc_send_commit");
        }
        if (!room.isAccessibleBy(Thread.currentThread())) {
            throw new WrongThreadException("the zero-copy reservation belongs to another thread");
        }
        if (handle.isClosed()) {
            dropSendHeld();
            exitSend();
            endHeldRequest();
            throw handle.closed();
        }
        if (length < 0) {
            throw new FipcException(Native.INVALID, "fipc_send_commit");
        }
        if (serviceServer) {
            checkReplyThread();
        }
        if (serviceClient) {
            SENDS_OWED.getAndAdd(this, 1); // its reply is owed from before the commit
        }
        int result = Native.sendCommit(handle.pointer, length);
        if (serviceClient && result != Native.OK) {
            SENDS_OWED.getAndAdd(this, -1);
        }
        if (result != Native.INVALID) { // the library used the reservation up (DISCONNECTED too)
            dropSendHeld();
            exitSend();
        }
        if (serviceServer) {
            replied(result);
        }
        check(result, "fipc_send_commit"); // the reservation stays for a valid length
    }

    /**
     * Zero-copy receive, step 1, waiting for ever; see {@link #receiveAcquire(Duration)}.
     *
     * @return the message in the ring, read-only
     */
    public MemorySegment receiveAcquire() {
        return receiveAcquire(Fipc.FOREVER);
    }

    /**
     * Zero-copy receive, step 1: waits up to {@code timeout} for a message and returns it in the ring (read-only,
     * 16-byte aligned). The bytes stay valid, and their room stays taken, until {@link #receiveRelease()} or the next
     * receive of any kind (which releases them first); the segment is confined to this thread, and holds the
     * connection open until then (a {@link #close()} meanwhile closes it at the release).
     *
     * <p>A broadcast's gated subscriber receives in place too: the message holds the publisher back until its
     * release. Release it within the broadcast's longest stall: a subscriber that holds the publisher back longer is
     * unsubscribed, and the memory of the message it holds goes away, so reading the segment then crashes the process.
     *
     * <p>A service's client takes its next reply in place, or throws {@link Result#ABANDONED}, as {@code receive}
     * does. A service's server takes the next request in place: the request is its until the reply's {@code send} or
     * {@code sendCommit}, which may be written from it in place. The reply (one that succeeds or throws
     * {@link Result#DISCONNECTED}) invalidates the segment, as do {@link #receiveRelease()} (which leaves the request
     * held until its reply), the next receive, {@link #close()} on this thread, and a call on this thread that throws
     * {@link IllegalStateException} because the connection is closed. Another thread can't invalidate a segment
     * confined to this one, so its reply throws {@link WrongThreadException}, sending nothing, while the segment is
     * valid: to reply on another thread, take what the reply needs from the segment, release it here, then reply
     * there. The segment holds the connection open: a close on another thread closes it, and answers a request not
     * replied to {@link Result#ABANDONED}, once the segment is invalid; a connection the garbage collector finds
     * unreachable while it holds a segment is never closed.
     *
     * @param timeout how long to wait for a message
     * @return the message in the ring, read-only
     * @throws FipcException {@link Result#TOO_LARGE} for a message in several pieces: it stays queued for
     *                       {@code receive}, and {@link FipcException#messageLength()} is its length;
     *                       {@link Result#TIMEOUT}, {@link Result#CANCELLED}, {@link Result#DISCONNECTED};
     *                       {@link Result#UNSUBSCRIBED} (a gated subscriber); {@link Result#INVALID} for a lossy
     *                       subscriber, which receives copies only, or a service's server that holds a request;
     *                       {@link Result#ABANDONED} (a service's client)
     * @throws IllegalStateException if the connection is closed
     */
    public MemorySegment receiveAcquire(Duration timeout) {
        int ms = Fipc.millis(timeout);
        MemorySegment conn = enterReceive();
        boolean held = false;
        try {
            int result = Native.recvAcquire(conn, receiveData, receiveOut, ms);
            long length = receiveOut.get(JAVA_LONG, 0);
            answered(result);
            if (result != Native.OK) {
                throw FipcException.receive(result, "fipc_recv_acquire", length);
            }
            Arena message = Arena.ofConfined();
            MemorySegment data = receiveData.get(ADDRESS, 0).reinterpret(length, message, null).asReadOnly();
            receiveHeldArena = message;
            if (serviceServer) {
                receiveHolder = Thread.currentThread();
            }
            held = true; // the message keeps the call's reference until it is released
            return data;
        } finally {
            if (!held) {
                exitReceive();
            }
        }
    }

    /**
     * Zero-copy receive, step 2: frees the acquired message's room; its segment is invalid afterwards. Without an
     * acquired message, a no-op (also once the connection is closed). Doesn't wait. Call it on the thread that acquired
     * the message. On a service's server the request stays held until the reply (the native release does nothing):
     * only the segment is invalid, and its hold on the connection ends, and another thread may reply. A server's reply
     * invalidates the segment by itself.
     *
     * @throws WrongThreadException on another thread than the one that acquired the message
     */
    public void receiveRelease() {
        Arena message = receiveHeldArena;
        if (message == null) {
            return;
        }
        message.close(); // first: WrongThreadException on another thread, with nothing changed
        receiveHeldArena = null;
        receiveHolder = null;
        if (!handle.isClosed()) {
            Native.recvRelease(handle.pointer);
        }
        exitReceive();
    }

    // === Lifetime ===

    /**
     * Makes every call of the connection that waits, now or later, throw {@link Result#CANCELLED}; calls that needn't
     * wait still work. A message cancelled halfway is dropped whole. The connection stays up: the peer sees nothing
     * until it is closed. Final. Any thread; a no-op once closed. A service client's {@link #call(byte[], Duration)},
     * which waits for its reply, throws {@link Result#CANCELLED} at once from then on, sending nothing.
     */
    public void cancel() {
        cancelled = true; // first: a call that doesn't see it sent its request before the cancel
        handle.cancel();
        Reference.reachabilityFence(this);
    }

    /**
     * Ends the connection: cancels it (a call that waits throws {@link Result#CANCELLED}) and closes it once no call is
     * in progress and no zero-copy segment is held; the peer then gets {@link Result#DISCONNECTED}, after the messages
     * this side completed. A work queue's producer or consumer leaves the queue, which goes on (the queue's
     * {@link WorkQueue#close()} ends it). A service's client or server leaves the service, which goes on (the
     * service's {@link Service#close()} ends it): a client's requests that no server took are dropped, and the request
     * a server holds is answered {@link Result#ABANDONED}. A server's request held in place on this thread: its
     * segment is invalid afterwards; held on another thread, the close waits for its segment's end (see
     * {@link #receiveAcquire(Duration)}). Any thread; a no-op once closed. Later calls throw
     * {@link IllegalStateException}.
     */
    @Override
    public void close() {
        cleanable.clean();
        endHeldRequest(); // a server's request held in place by this thread: its reference goes, so the close goes on
    }

    // === Calls ===
    //
    // A call holds a reference on the handle from its enter to its exit. A zero-copy reservation or acquired message
    // keeps its call's reference until its commit or release; the next call on the same side takes that reference
    // over (the library drops the reservation, or releases the message, first), after invalidating the segment.

    /** The sending side's reference: a reservation's, which this call takes over, or a new one. */
    private MemorySegment enterSend() {
        if (sendHeld != null) {
            dropSendHeld();
            if (handle.isClosed()) {
                exitSend();
                endHeldRequest();
                throw handle.closed();
            }
            return handle.pointer;
        }
        try {
            return handle.acquire();
        } catch (IllegalStateException closed) {
            endHeldRequest(); // a server's reply after the close: the request it held in place ends with it
            throw closed;
        }
    }

    /** A server's reply returned {@code result}: one made, or met by the service's end, ends the request held in place. */
    private void replied(int result) {
        if (result == Native.OK || result == Native.DISCONNECTED) {
            endHeldRequest();
        }
    }

    /**
     * A service's server about to reply: on another thread than the one that holds the request's segment in place,
     * throws WrongThreadException, with nothing changed, since only that thread can invalidate the segment, and the
     * reply gives the request's room back to its client.
     */
    private void checkReplyThread() {
        Thread holder = receiveHolder;
        if (holder != null && holder != Thread.currentThread()) {
            throw new WrongThreadException("the request held in place belongs to another thread: release it first");
        }
    }

    /**
     * A service's server, on the thread that received the request it holds in place: invalidates the request's segment
     * and drops its reference, so that the connection can close. On another thread, or with nothing held, a no-op:
     * the segment is confined to the receiving thread, whose next receive, release or close ends it.
     */
    private void endHeldRequest() {
        Arena message = receiveHeldArena;
        if (!serviceServer || message == null || receiveHolder != Thread.currentThread()) {
            return;
        }
        message.close();
        receiveHeldArena = null;
        receiveHolder = null;
        exitReceive();
    }

    /** Invalidates the reservation's segment: WrongThreadException on another thread, with nothing changed. */
    private void dropSendHeld() {
        sendHeldArena.close();
        sendHeld = null;
        sendHeldArena = null;
    }

    private void exitSend() {
        Arena temporary = sendTemporary;
        if (temporary != null) {
            sendTemporary = null;
            temporary.close();
        }
        handle.release();
        Reference.reachabilityFence(this);
    }

    /** The receiving side's reference: an acquired message's, which this call takes over, or a new one. */
    private MemorySegment enterReceive() {
        Arena message = receiveHeldArena;
        if (message != null) {
            message.close(); // WrongThreadException on another thread, with nothing changed
            receiveHeldArena = null;
            receiveHolder = null;
            if (handle.isClosed()) {
                exitReceive();
                throw handle.closed();
            }
            return handle.pointer;
        }
        return handle.acquire();
    }

    private void exitReceive() {
        Arena temporary = receiveTemporary;
        if (temporary != null) {
            receiveTemporary = null;
            temporary.close();
        }
        handle.release();
        Reference.reachabilityFence(this);
    }

    /** {@code data} in native memory: itself, or a copy in the sending side's buffer or a temporary one. */
    private MemorySegment outgoing(MemorySegment data) {
        long length = data.byteSize();
        if (data.isNative() || length == 0) {
            return length == 0 ? MemorySegment.NULL : data;
        }
        MemorySegment copy = sendRoom(length);
        MemorySegment.copy(data, 0, copy, 0, length);
        return copy;
    }

    /** At least {@code length} bytes of native memory for a send: the sending side's buffer, or a temporary one. */
    private MemorySegment sendRoom(long length) {
        if (length <= SCRATCH_MAX) {
            MemorySegment scratch = sendScratch;
            if (scratch == null || scratch.byteSize() < length) {
                scratch = Arena.ofAuto().allocate(scratchSize(length), 16); // the one it replaces is freed by the GC
                sendScratch = scratch;
            }
            return scratch;
        }
        sendTemporary = Arena.ofConfined();
        return sendTemporary.allocate(length, 16);
    }

    /** The receiving side's buffer, at least {@code length} bytes (up to SCRATCH_MAX). */
    private MemorySegment receiveScratch(long length) {
        MemorySegment scratch = receiveScratch;
        if (scratch == null || scratch.byteSize() < length) {
            scratch = Arena.ofAuto().allocate(scratchSize(length), 16); // the one it replaces is freed by the GC
            receiveScratch = scratch;
        }
        return scratch;
    }

    /**
     * Room for a message of {@code length} bytes: the receiving side's buffer, or a temporary one, which replaces the
     * call's earlier one.
     */
    private MemorySegment incoming(long length) {
        if (length <= SCRATCH_MAX) {
            return receiveScratch(length);
        }
        Arena earlier = receiveTemporary;
        if (earlier != null) {
            receiveTemporary = null;
            earlier.close();
        }
        receiveTemporary = Arena.ofConfined();
        return receiveTemporary.allocate(length, 16);
    }

    /** The receiving side's array of {@code count} lengths (at least one, so that its address is never NULL). */
    private MemorySegment receiveLengths(int count) {
        MemorySegment lengths = receiveLengths;
        long size = JAVA_LONG.byteSize() * Math.max(count, 16);
        if (lengths == null || lengths.byteSize() < size) {
            lengths = Arena.ofAuto().allocate(size, 8); // the one it replaces is freed by the GC
            receiveLengths = lengths;
        }
        return lengths;
    }

    private static long scratchSize(long length) {
        return Math.max(SCRATCH_MIN, Long.highestOneBit(Math.max(1, length - 1)) << 1);
    }

    private static void check(int result, String call) {
        if (result != Native.OK) {
            throw new FipcException(result, call);
        }
    }
}
