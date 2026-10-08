package io.github.fastipc;

import static io.github.fastipc.TestSupport.FIVE_SECONDS;
import static io.github.fastipc.TestSupport.RING;
import static io.github.fastipc.TestSupport.TEN_SECONDS;
import static io.github.fastipc.TestSupport.assertFipc;
import static io.github.fastipc.TestSupport.pattern;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** The data calls in one process: plain messages through every kind of buffer, zero-copy and RPC. */
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class MessagesTest {
    private static final ExecutorService SENDERS = Executors.newCachedThreadPool();

    @AfterAll
    static void stopSenders() {
        SENDERS.shutdownNow();
    }

    @Test
    void connectAcceptAndMaxPiece() {
        try (TestSupport.Pair pair = new TestSupport.Pair("pair")) {
            assertEquals(RING - 64, pair.client.maxPiece());
            assertEquals(RING - 64, pair.server.maxPiece());
            assertEquals(pair.listener.name(), pair.client.name());
            assertEquals(pair.listener.name(), pair.server.name());
            assertTrue(Fipc.libraryPath().contains("fastipc"), Fipc.libraryPath());
        }
    }

    @Test
    void sendAndReceiveArrays() {
        try (TestSupport.Pair pair = new TestSupport.Pair("arrays")) {
            pair.client.send(pattern(100));
            assertArrayEquals(pattern(100), pair.server.receive(FIVE_SECONDS));

            byte[] bytes = pattern(50);
            pair.server.send(bytes, 10, 20, FIVE_SECONDS);
            assertArrayEquals(Arrays.copyOfRange(bytes, 10, 30), pair.client.receive());
            assertThrows(IndexOutOfBoundsException.class, () -> pair.server.send(bytes, 40, 20, FIVE_SECONDS));

            // An empty message is the library's INVALID
            assertFipc(Result.INVALID, () -> pair.client.send(new byte[0]));
            assertFipc(Result.TIMEOUT, () -> pair.server.receive(Fipc.NO_WAIT));
        }
    }

    @Test
    void sendAndReceiveByteBuffers() {
        try (TestSupport.Pair pair = new TestSupport.Pair("buffers")) {
            for (boolean directOut : new boolean[] {false, true}) {
                for (boolean directIn : new boolean[] {false, true}) {
                    ByteBuffer out = directOut ? ByteBuffer.allocateDirect(64).put(pattern(64)).flip()
                        : ByteBuffer.wrap(pattern(64));
                    out.position(4).limit(36); // sends bytes 4..36
                    pair.client.send(out, FIVE_SECONDS);
                    assertEquals(36, out.position());

                    ByteBuffer in = directIn ? ByteBuffer.allocateDirect(100) : ByteBuffer.allocate(100);
                    in.position(10); // receives into 10..
                    assertEquals(32, pair.server.receive(in, FIVE_SECONDS));
                    assertEquals(42, in.position());
                    byte[] got = new byte[32];
                    in.get(10, got);
                    assertArrayEquals(Arrays.copyOfRange(pattern(64), 4, 36), got);
                }
            }

            // A message longer than the buffer's remaining bytes: TOO_LARGE with its length, and it stays queued
            pair.client.send(pattern(200));
            ByteBuffer small = ByteBuffer.allocate(300).position(150);
            FipcException e = assertFipc(Result.TOO_LARGE, () -> pair.server.receive(small, FIVE_SECONDS));
            assertEquals(200, e.messageLength());
            assertEquals(150, small.position());
            ByteBuffer direct = ByteBuffer.allocateDirect(100);
            assertEquals(200, assertFipc(Result.TOO_LARGE, () -> pair.server.receive(direct, FIVE_SECONDS)).messageLength());
            assertArrayEquals(pattern(200), pair.server.receive(FIVE_SECONDS));

            assertThrows(ReadOnlyBufferException.class,
                () -> pair.server.receive(ByteBuffer.allocate(10).asReadOnlyBuffer(), Fipc.NO_WAIT));
        }
    }

    @Test
    void sendAndReceiveSegments() {
        try (TestSupport.Pair pair = new TestSupport.Pair("segments"); Arena arena = Arena.ofConfined()) {
            MemorySegment nativeOut = arena.allocate(1000);
            nativeOut.copyFrom(MemorySegment.ofArray(pattern(1000)));
            pair.client.send(nativeOut);
            pair.client.send(MemorySegment.ofArray(pattern(70_000)), FIVE_SECONDS); // longer than the copy buffer

            MemorySegment nativeIn = arena.allocate(2000);
            assertEquals(1000, pair.server.receive(nativeIn, FIVE_SECONDS));
            assertArrayEquals(pattern(1000), nativeIn.asSlice(0, 1000).toArray(ValueLayout.JAVA_BYTE));
            byte[] heap = new byte[80_000];
            assertEquals(70_000, pair.server.receive(MemorySegment.ofArray(heap)));
            assertArrayEquals(pattern(70_000), Arrays.copyOf(heap, 70_000));

            assertThrows(IllegalArgumentException.class,
                () -> pair.server.receive(nativeIn.asReadOnly(), Fipc.NO_WAIT));
        }
    }

    /** Messages longer than the binding's copy buffer and than the ring travel in pieces and arrive whole. */
    @Test
    void messagesOfAnySize() throws Exception {
        String name = TestSupport.uniqueName("sizes");
        int ring = 1 << 16;
        int[] sizes = {1, 4096, 4097, 65_536, 65_537, 3 * ring + 5, 1 << 21};
        try (TestSupport.Pair pair = new TestSupport.Pair(name, ring)) {
            CompletableFuture<Void> sender = CompletableFuture.runAsync(() -> {
                for (int size : sizes) {
                    pair.client.send(pattern(size), TEN_SECONDS);
                }
                for (int size : sizes) {
                    pair.client.send(ByteBuffer.allocateDirect(size).put(pattern(size)).flip(), TEN_SECONDS);
                }
            }, SENDERS);
            for (int size : sizes) {
                assertArrayEquals(pattern(size), pair.server.receive(TEN_SECONDS), "size " + size);
            }
            for (int size : sizes) {
                ByteBuffer in = ByteBuffer.allocateDirect(size);
                assertEquals(size, pair.server.receive(in, TEN_SECONDS));
                assertEquals(ByteBuffer.wrap(pattern(size)), in.flip(), "size " + size);
            }
            sender.get(30, TimeUnit.SECONDS);
            assertFipc(Result.TIMEOUT, () -> pair.server.receive(Fipc.NO_WAIT));
        }
    }

    @Test
    void zeroCopy() {
        try (TestSupport.Pair pair = new TestSupport.Pair("zerocopy")) {
            // Write in the ring, commit fewer bytes than acquired, read in the ring
            MemorySegment room = pair.client.sendAcquire(64, FIVE_SECONDS);
            assertEquals(64, room.byteSize());
            assertEquals(0, room.address() % 16);
            room.copyFrom(MemorySegment.ofArray(pattern(40)));
            pair.client.sendCommit(40);
            assertThrows(IllegalStateException.class, () -> room.get(ValueLayout.JAVA_BYTE, 0)); // committed
            assertFipc(Result.INVALID, () -> pair.client.sendCommit(40)); // no reservation left

            MemorySegment message = pair.server.receiveAcquire(FIVE_SECONDS);
            assertArrayEquals(pattern(40), message.toArray(ValueLayout.JAVA_BYTE));
            assertTrue(message.isReadOnly());
            assertThrows(IllegalArgumentException.class, () -> message.set(ValueLayout.JAVA_BYTE, 0, (byte) 1));
            pair.server.receiveRelease();
            assertThrows(IllegalStateException.class, () -> message.get(ValueLayout.JAVA_BYTE, 0)); // released
            pair.server.receiveRelease(); // a no-op without a message

            // A commit's length out of range leaves the reservation for a valid one
            MemorySegment again = pair.client.sendAcquire(8);
            again.fill((byte) 9);
            assertFipc(Result.INVALID, () -> pair.client.sendCommit(9));
            assertFipc(Result.INVALID, () -> pair.client.sendCommit(0));
            pair.client.sendCommit(8);
            assertArrayEquals(new byte[] {9, 9, 9, 9, 9, 9, 9, 9}, pair.server.receive(FIVE_SECONDS));

            // The next send drops a reservation that was never committed
            MemorySegment dropped = pair.client.sendAcquire(16);
            pair.client.send(pattern(5));
            assertThrows(IllegalStateException.class, () -> dropped.get(ValueLayout.JAVA_BYTE, 0));
            assertArrayEquals(pattern(5), pair.server.receive(FIVE_SECONDS));

            // Over max_piece: TOO_LARGE for zero-copy; a message of several pieces is TOO_LARGE for receiveAcquire
            // (with its length) and stays for the copying receive
            assertFipc(Result.TOO_LARGE, () -> pair.client.sendAcquire(pair.client.maxPiece() + 1, Fipc.NO_WAIT));
            pair.client.send(pattern(1000));
            CompletableFuture<Void> sender = CompletableFuture.runAsync(
                () -> pair.client.send(pattern((int) (2 * RING)), TEN_SECONDS), SENDERS);
            MemorySegment first = pair.server.receiveAcquire(FIVE_SECONDS);
            assertEquals(1000, first.byteSize());
            // The next receive releases the acquired message first
            FipcException e = assertFipc(Result.TOO_LARGE, () -> pair.server.receiveAcquire(FIVE_SECONDS));
            assertEquals(2 * RING, e.messageLength());
            assertThrows(IllegalStateException.class, () -> first.get(ValueLayout.JAVA_BYTE, 0));
            assertArrayEquals(pattern((int) (2 * RING)), pair.server.receive(TEN_SECONDS));
            sender.join();

            assertThrows(IllegalArgumentException.class, () -> pair.client.sendAcquire(-1));
            assertFipc(Result.INVALID, () -> pair.client.sendAcquire(0, Fipc.NO_WAIT));
        }
    }

    /** A zero-copy segment is confined to the thread that acquired it, and so are its commit and release. */
    @Test
    void zeroCopySegmentsAreConfined() throws Exception {
        try (TestSupport.Pair pair = new TestSupport.Pair("confined")) {
            MemorySegment room = pair.client.sendAcquire(4);
            CompletableFuture<Throwable> other = CompletableFuture.supplyAsync(() -> {
                Throwable[] thrown = new Throwable[3];
                try {
                    room.fill((byte) 1);
                } catch (Throwable t) {
                    thrown[0] = t;
                }
                try {
                    pair.client.sendCommit(4);
                } catch (Throwable t) {
                    thrown[1] = t;
                }
                try {
                    pair.client.send(pattern(4));
                } catch (Throwable t) {
                    thrown[2] = t;
                }
                for (Throwable t : thrown) {
                    if (!(t instanceof WrongThreadException)) {
                        return new AssertionError("expected WrongThreadException, got " + t);
                    }
                }
                return null;
            }, SENDERS);
            Throwable failure = other.get(10, TimeUnit.SECONDS);
            if (failure != null) {
                throw new AssertionError(failure);
            }
            room.fill((byte) 2);
            pair.client.sendCommit(4); // still this thread's reservation
            MemorySegment message = pair.server.receiveAcquire(FIVE_SECONDS);
            assertArrayEquals(new byte[] {2, 2, 2, 2}, message.toArray(ValueLayout.JAVA_BYTE));
            assertThrows(WrongThreadException.class,
                () -> {
                    try {
                        CompletableFuture.runAsync(pair.server::receiveRelease, SENDERS).join();
                    } catch (java.util.concurrent.CompletionException ce) {
                        throw ce.getCause();
                    }
                });
            pair.server.receiveRelease();
        }
    }

    @Test
    void rpc() {
        try (TestSupport.Pair pair = new TestSupport.Pair("rpc")) {
            long id = pair.client.rpcSubmit(7, pattern(16), FIVE_SECONDS);
            assertEquals(1, id);

            RpcMessage request = pair.server.rpcReceive(FIVE_SECONDS);
            assertTrue(request.isRequest());
            assertFalse(request.isResponse());
            assertEquals(RpcKind.REQUEST, request.kind());
            assertEquals(id, request.id());
            assertEquals(7, request.opcode());
            assertEquals(0, request.status());
            assertArrayEquals(pattern(16), request.payload());

            pair.server.rpcRespond(request.id(), 8, -3, new byte[0], FIVE_SECONDS);
            RpcMessage response = pair.client.rpcReceive();
            assertEquals(RpcKind.RESPONSE, response.kind());
            assertEquals(id, response.id());
            assertEquals(8, response.opcode());
            assertEquals(-3, response.status());
            assertEquals(0, response.payload().length);

            // Ids count up; opcodes are unsigned 32-bit values; payloads of any size, also from native memory
            try (Arena arena = Arena.ofShared()) { // the sending thread reads it
                MemorySegment payload = arena.allocate(3 * RING);
                payload.copyFrom(MemorySegment.ofArray(pattern((int) (3 * RING))));
                CompletableFuture<Long> sender = CompletableFuture.supplyAsync(() -> {
                    pair.client.rpcSubmit(0xFFFF_FFFE, new byte[0]);
                    return pair.client.rpcSubmit(9, payload, TEN_SECONDS);
                }, SENDERS);
                RpcMessage empty = pair.server.rpcReceive(FIVE_SECONDS);
                assertEquals(2, empty.id());
                assertEquals(0xFFFF_FFFEL, Integer.toUnsignedLong(empty.opcode()));
                RpcMessage large = pair.server.rpcReceive(TEN_SECONDS);
                assertEquals(3, large.id());
                assertEquals(3, (long) sender.join());
                assertArrayEquals(pattern((int) (3 * RING)), large.payload());
                pair.server.rpcRespond(large.id(), 9, 0, MemorySegment.ofArray(pattern(70_000)), FIVE_SECONDS);
                assertArrayEquals(pattern(70_000), pair.client.rpcReceive(FIVE_SECONDS).payload());
            }

            // Into the caller's buffer: a segment (native or heap) or a ByteBuffer; TOO_LARGE takes nothing
            pair.client.rpcSubmit(11, pattern(100));
            pair.client.rpcSubmit(12, pattern(5000));
            pair.client.rpcSubmit(13, new byte[0]);
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment small = arena.allocate(50);
                assertEquals(100, assertFipc(Result.TOO_LARGE, () -> pair.server.rpcReceive(small, FIVE_SECONDS))
                    .messageLength());
                assertEquals(100, assertFipc(Result.TOO_LARGE,
                    () -> pair.server.rpcReceive(MemorySegment.ofArray(new byte[99]), FIVE_SECONDS)).messageLength());
                MemorySegment big = arena.allocate(200);
                RpcHeader header = pair.server.rpcReceive(big, FIVE_SECONDS);
                assertEquals(new RpcHeader(4, RpcKind.REQUEST, 11, 0, 100), header);
                assertTrue(header.isRequest());
                assertArrayEquals(pattern(100), big.asSlice(0, 100).toArray(ValueLayout.JAVA_BYTE));
            }
            ByteBuffer heap = ByteBuffer.allocate(6000).position(10);
            RpcHeader second = pair.server.rpcReceive(heap, FIVE_SECONDS);
            assertEquals(12, second.opcode());
            assertEquals(5000, second.length());
            assertEquals(5010, heap.position());
            byte[] got = new byte[5000];
            heap.get(10, got);
            assertArrayEquals(pattern(5000), got);
            RpcHeader empty = pair.server.rpcReceive(ByteBuffer.allocateDirect(0));
            assertEquals(13, empty.opcode());
            assertEquals(0, empty.length());

            // A plain message isn't an RPC message: rpcReceive drops it and reports INVALID
            pair.client.send(pattern(3));
            assertFipc(Result.INVALID, () -> pair.server.rpcReceive(FIVE_SECONDS));
            assertFipc(Result.TIMEOUT, () -> pair.server.rpcReceive(Fipc.NO_WAIT));
            assertTrue(request.toString().contains("payload=16 bytes"), request.toString());
        }
    }
}
