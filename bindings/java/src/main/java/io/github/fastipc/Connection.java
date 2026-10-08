package io.github.fastipc;

import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.ref.Cleaner;
import java.lang.ref.Reference;
import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;
import java.time.Duration;
import java.util.Objects;

/**
 * One connection: two rings in shared memory, one per direction. A server gets one from {@link Listener#accept()}, a
 * client from {@link #connect(String, Duration)}.
 *
 * <p><b>Messages.</b> The copying calls ({@link #send(byte[])}, {@link #receive()}, the RPC calls) take messages of any
 * size: a message longer than one piece ({@link #maxPiece()} bytes) travels in pieces and arrives whole. A copying
 * call's timeout bounds its wait for the first piece; once a piece has moved, the call goes on until the whole message
 * has (only {@link #cancel()}, {@link #close()} or the peer's end stops it, and the message is then dropped whole).
 * Messages arrive in the order they were sent; a plain message holds at least 1 byte, an RPC payload may be empty. The
 * zero-copy calls write and read a message of one piece in place. Use a connection for plain messages or for RPC, not
 * both: an RPC message is a plain message with a 32-byte header in front.
 *
 * <p><b>Buffers.</b> A {@link MemorySegment} of native memory goes to the library as it is. A byte array, a heap
 * {@link ByteBuffer} or a heap segment is copied through a native buffer of the connection (up to 1 MiB; a longer
 * message through a temporary one), so direct buffers and native segments save a copy.
 *
 * <p><b>Threads.</b> One thread at a time sends ({@code send}, {@code sendAcquire}/{@code sendCommit},
 * {@code rpcSubmit}, {@code rpcRespond}) and one thread at a time receives ({@code receive},
 * {@code receiveAcquire}/{@code receiveRelease}, {@code rpcReceive}); the two may run at once. {@link #cancel()},
 * {@link #close()}, {@link #maxPiece()} and {@link #name()} may be called from any thread, at any time.
 *
 * <p><b>Lifetime.</b> {@link #close()} cancels the connection, so a call that waits throws {@link Result#CANCELLED},
 * and the native close runs once the calls in progress have returned: no call ever runs on a closed connection. Later
 * calls throw {@link IllegalStateException}. A zero-copy segment holds the connection open the same way, until its
 * commit or release; it is confined to the thread that acquired it, and accessing it after its commit or release
 * throws {@link IllegalStateException} instead of touching memory that is no longer the caller's.
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
    /** The longest array a JVM allocates. */
    private static final long MAX_ARRAY = Integer.MAX_VALUE - 8;
    private static final byte[] EMPTY = new byte[0];

    private final Handle handle;
    private final Cleaner.Cleanable cleanable;
    private final String name;
    private final int maxPiece;
    /** The out-parameters; closed with the native connection. */
    private final Arena arena;
    /** The sending thread's out-parameter: fipc_send_acquire's buffer, fipc_rpc_submit's id. */
    private final MemorySegment sendOut;
    /** The receiving thread's out-parameters: the length at 0, fipc_recv_acquire's data at 8. */
    private final MemorySegment receiveOut;
    /** The receiving thread's out-parameter fipc_recv_acquire's data (receiveOut at 8). */
    private final MemorySegment receiveData;
    /** The receiving thread's fipc_rpc_msg_t. */
    private final MemorySegment rpcHeader;

    // The sending thread's state
    private MemorySegment sendScratch;
    private Arena sendTemporary;
    private MemorySegment sendHeld;
    private Arena sendHeldArena;

    // The receiving thread's state
    private MemorySegment receiveScratch;
    private Arena receiveTemporary;
    private Arena receiveHeldArena;

    Connection(MemorySegment pointer, String name) {
        Arena arena = Arena.ofShared();
        this.arena = arena;
        this.handle = new Handle(pointer, "connection", Native::cancel, () -> {
            Native.close(pointer);
            arena.close();
        });
        this.cleanable = Handle.CLEANER.register(this, handle::close);
        this.name = name;
        this.maxPiece = (int) Native.maxPiece(pointer);
        this.sendOut = arena.allocate(16, 16);
        this.receiveOut = arena.allocate(48, 16);
        this.receiveData = receiveOut.asSlice(8, 8);
        this.rpcHeader = receiveOut.asSlice(16, 32);
    }

    /**
     * Connects to the server listening on {@code name}, waiting for ever; see {@link #connect(String, Duration)}.
     *
     * @param name the server's name
     * @return the connection
     */
    public static Connection connect(String name) {
        return connect(name, Fipc.FOREVER);
    }

    /**
     * Client: connects to the server listening on {@code name} and sets the connection up on the calling thread,
     * waiting up to {@code timeout} for a server to listen (it may start later, or be serving another client) and to
     * call {@code accept}. It returns once the server's {@code accept} has offered the rings, which may be before that
     * {@code accept} returns: messages sent meanwhile wait in the ring. The client and the server run on different
     * threads (of one process or two). The connection's rings have the capacity the server chose. There is no handle
     * to cancel until it returns: use {@link Fipc#FOREVER} to wait for a server however late it starts, or a finite
     * timeout to stay responsive.
     *
     * @param name    the server's name
     * @param timeout how long to wait; {@link Fipc#NO_WAIT} doesn't, {@link Fipc#FOREVER} waits for ever
     * @return the connection
     * @throws FipcException {@link Result#TIMEOUT} if no server's accept set a connection up in time; {@link Result#INVALID}
     *                       for a bad name, or a server with another version or user; {@link Result#NO_MEMORY} if the
     *                       rings can't be mapped
     */
    public static Connection connect(String name, Duration timeout) {
        Objects.requireNonNull(name, "name");
        int ms = Fipc.millis(timeout);
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment out = arena.allocate(ADDRESS);
            int result = Native.connect(arena.allocateFrom(name), out, ms);
            if (result != Native.OK) {
                throw new FipcException(result, "fipc_connect");
            }
            return new Connection(out.get(ADDRESS, 0), name);
        }
    }

    /** {@return the name of the listener this connection came through} */
    public String name() {
        return name;
    }

    /** {@return the longest message the zero-copy calls take: the connection's capacity less 64 bytes} Any thread. */
    public int maxPiece() {
        return maxPiece;
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
     * Drops a zero-copy reservation that was never committed.
     *
     * @param message the message: at least 1 byte
     * @param timeout how long to wait for room for the first piece
     * @throws FipcException {@link Result#TIMEOUT}, {@link Result#CANCELLED}, {@link Result#DISCONNECTED};
     *                       {@link Result#INVALID} for an empty message
     * @throws IllegalStateException if the connection is closed
     */
    public void send(MemorySegment message, Duration timeout) {
        int ms = Fipc.millis(timeout);
        MemorySegment conn = enterSend();
        int result;
        try {
            result = Native.send(conn, outgoing(message), message.byteSize(), ms);
        } finally {
            exitSend();
        }
        check(result, "fipc_send");
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
     * piece, then goes on until the whole message is in.
     *
     * @param timeout how long to wait for the first piece
     * @return the message
     * @throws FipcException {@link Result#TIMEOUT}, {@link Result#CANCELLED}, {@link Result#DISCONNECTED} (once every
     *                       message the peer completed has been received); {@link Result#TOO_LARGE} for a message
     *                       longer than an array can be, which stays queued for {@link #receive(MemorySegment, Duration)}
     * @throws IllegalStateException if the connection is closed
     */
    public byte[] receive(Duration timeout) {
        int ms = Fipc.millis(timeout);
        MemorySegment conn = enterReceive();
        try {
            MemorySegment buffer = receiveScratch(SCRATCH_MIN);
            int result = Native.recv(conn, buffer, buffer.byteSize(), receiveOut, ms);
            long length = receiveOut.get(JAVA_LONG, 0);
            if (result == Native.TOO_LARGE && length <= MAX_ARRAY) {
                buffer = incoming(length); // the message stays queued, so this call doesn't wait for its first piece
                result = Native.recv(conn, buffer, length, receiveOut, ms);
                length = receiveOut.get(JAVA_LONG, 0);
            }
            if (result != Native.OK) {
                throw new FipcException(result, "fipc_recv", result == Native.TOO_LARGE ? length : -1);
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
     * copies it out, going on until the whole message is in, and frees its room.
     *
     * @param buffer  where the message goes, from its start; writable
     * @param timeout how long to wait for the first piece
     * @return the message's length
     * @throws FipcException {@link Result#TOO_LARGE} if the message is longer than the buffer: nothing is taken, and
     *                       {@link FipcException#messageLength()} says how much room it needs; {@link Result#TIMEOUT},
     *                       {@link Result#CANCELLED}, {@link Result#DISCONNECTED}. After any of them the buffer's
     *                       contents are unspecified.
     * @throws IllegalStateException if the connection is closed
     */
    public long receive(MemorySegment buffer, Duration timeout) {
        if (buffer.isReadOnly()) {
            throw new IllegalArgumentException("a read-only segment");
        }
        int ms = Fipc.millis(timeout);
        MemorySegment conn = enterReceive();
        try {
            long capacity = buffer.byteSize();
            MemorySegment target = buffer.isNative() ? buffer : receiveScratch(Math.min(capacity, SCRATCH_MAX));
            int result = Native.recv(conn, target, Math.min(capacity, target.byteSize()), receiveOut, ms);
            long length = receiveOut.get(JAVA_LONG, 0);
            if (result == Native.TOO_LARGE && target != buffer && length <= capacity) {
                target = incoming(length); // the message stays queued, so this call doesn't wait for its first piece
                result = Native.recv(conn, target, length, receiveOut, ms);
                length = receiveOut.get(JAVA_LONG, 0);
            }
            if (result != Native.OK) {
                throw new FipcException(result, "fipc_recv", result == Native.TOO_LARGE ? length : -1);
            }
            if (target != buffer) {
                MemorySegment.copy(target, 0, buffer, 0, length);
            }
            return length;
        } finally {
            exitReceive();
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
     * one message. Doesn't wait. The reservation's segment is invalid afterwards. Call it on the thread that acquired
     * the reservation.
     *
     * @param length the message's length: 1 to the acquired length
     * @throws FipcException {@link Result#INVALID} without a reservation, or for a length out of range (the
     *                       reservation stays, for a valid one)
     * @throws IllegalStateException if the connection is closed (the reservation is dropped)
     * @throws WrongThreadException  on another thread than the one that acquired the reservation
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
            throw handle.closed();
        }
        if (length < 0) {
            throw new FipcException(Native.INVALID, "fipc_send_commit");
        }
        int result = Native.sendCommit(handle.pointer, length);
        check(result, "fipc_send_commit"); // the reservation stays for a valid length
        dropSendHeld();
        exitSend();
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
     * @param timeout how long to wait for a message
     * @return the message in the ring, read-only
     * @throws FipcException {@link Result#TOO_LARGE} for a message in several pieces: it stays queued for
     *                       {@code receive}, and {@link FipcException#messageLength()} is its length;
     *                       {@link Result#TIMEOUT}, {@link Result#CANCELLED}, {@link Result#DISCONNECTED}
     * @throws IllegalStateException if the connection is closed
     */
    public MemorySegment receiveAcquire(Duration timeout) {
        int ms = Fipc.millis(timeout);
        MemorySegment conn = enterReceive();
        boolean held = false;
        try {
            int result = Native.recvAcquire(conn, receiveData, receiveOut, ms);
            long length = receiveOut.get(JAVA_LONG, 0);
            if (result != Native.OK) {
                throw new FipcException(result, "fipc_recv_acquire", result == Native.TOO_LARGE ? length : -1);
            }
            Arena message = Arena.ofConfined();
            MemorySegment data = receiveData.get(ADDRESS, 0).reinterpret(length, message, null).asReadOnly();
            receiveHeldArena = message;
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
     * the message.
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
        if (!handle.isClosed()) {
            Native.recvRelease(handle.pointer);
        }
        exitReceive();
    }

    // === RPC ===

    /**
     * Sends a request, waiting for ever for room; see {@link #rpcSubmit(int, MemorySegment, Duration)}.
     *
     * @param opcode  the application's
     * @param payload the payload, possibly empty
     * @return the request's id
     */
    public long rpcSubmit(int opcode, byte[] payload) {
        return rpcSubmit(opcode, MemorySegment.ofArray(payload), Fipc.FOREVER);
    }

    /**
     * Sends a request; see {@link #rpcSubmit(int, MemorySegment, Duration)}.
     *
     * @param opcode  the application's
     * @param payload the payload, possibly empty
     * @param timeout how long to wait for room for the first piece
     * @return the request's id
     */
    public long rpcSubmit(int opcode, byte[] payload, Duration timeout) {
        return rpcSubmit(opcode, MemorySegment.ofArray(payload), timeout);
    }

    /**
     * Sends a request with a payload of any size, possibly empty (as {@link #send(MemorySegment, Duration)}), and
     * returns its id: a connection numbers its requests from 1. Correlate the responses by it.
     *
     * @param opcode  the application's (an unsigned 32-bit value)
     * @param payload the payload, possibly empty
     * @param timeout how long to wait for room for the first piece
     * @return the request's id
     * @throws FipcException {@link Result#TIMEOUT}, {@link Result#CANCELLED}, {@link Result#DISCONNECTED}
     * @throws IllegalStateException if the connection is closed
     */
    public long rpcSubmit(int opcode, MemorySegment payload, Duration timeout) {
        int ms = Fipc.millis(timeout);
        MemorySegment conn = enterSend();
        try {
            int result = Native.rpcSubmit(conn, opcode, outgoing(payload), payload.byteSize(), sendOut, ms);
            check(result, "fipc_rpc_submit");
            return sendOut.get(JAVA_LONG, 0);
        } finally {
            exitSend();
        }
    }

    /**
     * Sends the response to request {@code id}, waiting for ever for room; see
     * {@link #rpcRespond(long, int, int, MemorySegment, Duration)}.
     *
     * @param id      the request's id
     * @param opcode  the application's
     * @param status  the application's
     * @param payload the payload, possibly empty
     */
    public void rpcRespond(long id, int opcode, int status, byte[] payload) {
        rpcRespond(id, opcode, status, MemorySegment.ofArray(payload), Fipc.FOREVER);
    }

    /**
     * Sends the response to request {@code id}; see {@link #rpcRespond(long, int, int, MemorySegment, Duration)}.
     *
     * @param id      the request's id
     * @param opcode  the application's
     * @param status  the application's
     * @param payload the payload, possibly empty
     * @param timeout how long to wait for room for the first piece
     */
    public void rpcRespond(long id, int opcode, int status, byte[] payload, Duration timeout) {
        rpcRespond(id, opcode, status, MemorySegment.ofArray(payload), timeout);
    }

    /**
     * Sends the response to request {@code id} (as {@link #send(MemorySegment, Duration)}; the payload may be empty).
     *
     * @param id      the request's id ({@link RpcMessage#id()})
     * @param opcode  the application's (an unsigned 32-bit value)
     * @param status  the application's
     * @param payload the payload, possibly empty
     * @param timeout how long to wait for room for the first piece
     * @throws FipcException {@link Result#TIMEOUT}, {@link Result#CANCELLED}, {@link Result#DISCONNECTED}
     * @throws IllegalStateException if the connection is closed
     */
    public void rpcRespond(long id, int opcode, int status, MemorySegment payload, Duration timeout) {
        int ms = Fipc.millis(timeout);
        MemorySegment conn = enterSend();
        int result;
        try {
            result = Native.rpcRespond(conn, id, opcode, status, outgoing(payload), payload.byteSize(), ms);
        } finally {
            exitSend();
        }
        check(result, "fipc_rpc_respond");
    }

    /**
     * Receives one request or response, waiting for ever; see {@link #rpcReceive(Duration)}.
     *
     * @return the message
     */
    public RpcMessage rpcReceive() {
        return rpcReceive(Fipc.FOREVER);
    }

    /**
     * Receives one request or response with a payload of any size (as {@link #receive(Duration)}).
     *
     * @param timeout how long to wait for the first piece
     * @return the message, its payload in a new array
     * @throws FipcException {@link Result#INVALID} for a message that isn't a well-formed RPC message (it is dropped);
     *                       {@link Result#TIMEOUT}, {@link Result#CANCELLED}, {@link Result#DISCONNECTED}
     * @throws IllegalStateException if the connection is closed
     */
    public RpcMessage rpcReceive(Duration timeout) {
        int ms = Fipc.millis(timeout);
        MemorySegment conn = enterReceive();
        try {
            MemorySegment buffer = receiveScratch(SCRATCH_MIN);
            int result = Native.rpcRecv(conn, buffer, buffer.byteSize(), rpcHeader, ms);
            long length = rpcHeader.get(JAVA_LONG, 24);
            if (result == Native.TOO_LARGE && length <= MAX_ARRAY) {
                buffer = incoming(length); // the message stays queued, so this call doesn't wait for its first piece
                result = Native.rpcRecv(conn, buffer, length, rpcHeader, ms);
                length = rpcHeader.get(JAVA_LONG, 24);
            }
            if (result != Native.OK) {
                throw new FipcException(result, "fipc_rpc_recv", result == Native.TOO_LARGE ? length : -1);
            }
            return new RpcMessage(rpcHeader.get(JAVA_LONG, 0), RpcKind.of(rpcHeader.get(JAVA_INT, 8)),
                rpcHeader.get(JAVA_INT, 12), rpcHeader.get(JAVA_INT, 16),
                length == 0 ? EMPTY : buffer.asSlice(0, length).toArray(JAVA_BYTE));
        } finally {
            exitReceive();
        }
    }

    /**
     * Receives one request or response into the buffer's remaining bytes, waiting for ever; see
     * {@link #rpcReceive(ByteBuffer, Duration)}.
     *
     * @param buffer where the payload goes, from its position
     * @return the message's header
     */
    public RpcHeader rpcReceive(ByteBuffer buffer) {
        return rpcReceive(buffer, Fipc.FOREVER);
    }

    /**
     * Receives one request or response, its payload into the buffer from its position up to its limit, and moves the
     * position past the payload; see {@link #rpcReceive(MemorySegment, Duration)}.
     *
     * @param buffer  where the payload goes, from its position; not read-only
     * @param timeout how long to wait for the first piece
     * @return the message's header
     */
    public RpcHeader rpcReceive(ByteBuffer buffer, Duration timeout) {
        if (buffer.isReadOnly()) {
            throw new ReadOnlyBufferException();
        }
        RpcHeader header = rpcReceive(MemorySegment.ofBuffer(buffer), timeout);
        buffer.position(buffer.position() + (int) header.length());
        return header;
    }

    /**
     * Receives one request or response into the segment, waiting for ever; see
     * {@link #rpcReceive(MemorySegment, Duration)}.
     *
     * @param buffer where the payload goes
     * @return the message's header
     */
    public RpcHeader rpcReceive(MemorySegment buffer) {
        return rpcReceive(buffer, Fipc.FOREVER);
    }

    /**
     * Receives one request or response into the caller's buffer, which a loop can reuse (no allocation per message):
     * returns its header, and copies its payload (any size, as {@link #receive(MemorySegment, Duration)}) into
     * {@code buffer} from its start.
     *
     * @param buffer  where the payload goes, from its start; writable
     * @param timeout how long to wait for the first piece
     * @return the message's header; its {@link RpcHeader#length()} is the payload's
     * @throws FipcException {@link Result#TOO_LARGE} if the payload is longer than the buffer: nothing is taken, and
     *                       {@link FipcException#messageLength()} says how much room it needs; {@link Result#INVALID}
     *                       for a message that isn't a well-formed RPC message (it is dropped); {@link Result#TIMEOUT},
     *                       {@link Result#CANCELLED}, {@link Result#DISCONNECTED}
     * @throws IllegalStateException if the connection is closed
     */
    public RpcHeader rpcReceive(MemorySegment buffer, Duration timeout) {
        if (buffer.isReadOnly()) {
            throw new IllegalArgumentException("a read-only segment");
        }
        int ms = Fipc.millis(timeout);
        MemorySegment conn = enterReceive();
        try {
            long capacity = buffer.byteSize();
            MemorySegment target = buffer.isNative() ? buffer : receiveScratch(Math.min(capacity, SCRATCH_MAX));
            int result = Native.rpcRecv(conn, target, Math.min(capacity, target.byteSize()), rpcHeader, ms);
            long length = rpcHeader.get(JAVA_LONG, 24);
            if (result == Native.TOO_LARGE && target != buffer && length <= capacity) {
                target = incoming(length); // the message stays queued, so this call doesn't wait for its first piece
                result = Native.rpcRecv(conn, target, length, rpcHeader, ms);
                length = rpcHeader.get(JAVA_LONG, 24);
            }
            if (result != Native.OK) {
                throw new FipcException(result, "fipc_rpc_recv", result == Native.TOO_LARGE ? length : -1);
            }
            if (target != buffer && length > 0) {
                MemorySegment.copy(target, 0, buffer, 0, length);
            }
            return new RpcHeader(rpcHeader.get(JAVA_LONG, 0), RpcKind.of(rpcHeader.get(JAVA_INT, 8)),
                rpcHeader.get(JAVA_INT, 12), rpcHeader.get(JAVA_INT, 16), length);
        } finally {
            exitReceive();
        }
    }

    // === Lifetime ===

    /**
     * Makes every call of the connection that waits, now or later, throw {@link Result#CANCELLED}; calls that needn't
     * wait still work. A message cancelled halfway is dropped whole. The connection stays up: the peer sees nothing
     * until it is closed. Final. Any thread; a no-op once closed.
     */
    public void cancel() {
        handle.cancel();
        Reference.reachabilityFence(this);
    }

    /**
     * Ends the connection: cancels it (a call that waits throws {@link Result#CANCELLED}) and closes it once no call is
     * in progress and no zero-copy segment is held; the peer then gets {@link Result#DISCONNECTED}, after the messages
     * this side completed. Any thread; a no-op once closed. Later calls throw {@link IllegalStateException}.
     */
    @Override
    public void close() {
        cleanable.clean();
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
                throw handle.closed();
            }
            return handle.pointer;
        }
        return handle.acquire();
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
        MemorySegment copy;
        if (length <= SCRATCH_MAX) {
            MemorySegment scratch = sendScratch;
            if (scratch == null || scratch.byteSize() < length) {
                scratch = Arena.ofAuto().allocate(scratchSize(length), 16); // the one it replaces is freed by the GC
                sendScratch = scratch;
            }
            copy = scratch;
        } else {
            sendTemporary = Arena.ofConfined();
            copy = sendTemporary.allocate(length, 16);
        }
        MemorySegment.copy(data, 0, copy, 0, length);
        return copy;
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

    /** Room for a message of {@code length} bytes: the receiving side's buffer, or a temporary one. */
    private MemorySegment incoming(long length) {
        if (length <= SCRATCH_MAX) {
            return receiveScratch(length);
        }
        receiveTemporary = Arena.ofConfined();
        return receiveTemporary.allocate(length, 16);
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
