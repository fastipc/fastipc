package io.github.fastipc;

import static io.github.fastipc.TestSupport.FIVE_SECONDS;
import static io.github.fastipc.TestSupport.TEN_SECONDS;
import static io.github.fastipc.TestSupport.assertFipc;
import static io.github.fastipc.TestSupport.startPeer;
import static io.github.fastipc.TestSupport.uniqueName;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * The service: many clients and one server, a pool of server threads that hold requests at once, each client's replies
 * in the order of its requests, ABANDONED when a server closes or its process ends holding a request, the calls each
 * role refuses, the zero-copy calls on both roles, {@link Connection#call} refused on other connections, timeouts and
 * cancels (a call that gives up and the next one, which drops the late reply; a call after a cancel, which sends
 * nothing), a client that sends on one thread and receives on another, the owner's wait for members and its cancel,
 * the end at the service's close, a service the garbage collector finds unreachable (it lives on), long requests and
 * replies, the examples' exchange in this process and as two processes (examples/java's files), and clients and
 * servers in other JVMs ({@link Peer}). InteropTest runs the Python binding's clients and servers against this
 * binding.
 */
@Timeout(value = 120, unit = TimeUnit.SECONDS)
class ServiceTest {
    /** A thread for each member that waits (QueueTest says why not the common pool). */
    private static final ExecutorService MEMBERS = Executors.newCachedThreadPool();

    @AfterAll
    static void stopMembers() {
        MEMBERS.shutdownNow();
    }

    private static byte[] bytes(String text) {
        return text.getBytes(UTF_8);
    }

    private static String text(byte[] bytes) {
        return new String(bytes, UTF_8);
    }

    /** Answers one request with its uppercase, on another thread. */
    private static CompletableFuture<Void> answerAside(Connection server) {
        return CompletableFuture.runAsync(() -> Peer.answer(server, 1), MEMBERS);
    }

    /**
     * Answers each request with its uppercase until DISCONNECTED: copied (way 0), into a buffer (1) or in place (2,
     * replying in place too); returns how often it answered each request.
     */
    private static Map<String, Integer> answerUntilTheEnd(Connection server, int way) {
        Map<String, Integer> answered = new HashMap<>();
        ByteBuffer buffer = ByteBuffer.allocate(server.maxPiece());
        for (;;) {
            byte[] request;
            try {
                switch (way) {
                    case 0 -> {
                        request = server.receive(TEN_SECONDS);
                        server.send(Peer.upper(request), Fipc.NO_WAIT);
                    }
                    case 1 -> {
                        buffer.clear();
                        int length = server.receive(buffer, TEN_SECONDS);
                        request = Arrays.copyOf(buffer.array(), length);
                        server.send(Peer.upper(request), Fipc.NO_WAIT);
                    }
                    default -> {
                        MemorySegment held = server.receiveAcquire(TEN_SECONDS);
                        request = held.toArray(ValueLayout.JAVA_BYTE);
                        byte[] reply = Peer.upper(request);
                        MemorySegment room = server.sendAcquire(reply.length, Fipc.NO_WAIT);
                        room.copyFrom(MemorySegment.ofArray(reply)); // the request held stays valid until the commit
                        server.sendCommit(reply.length);
                    }
                }
            } catch (FipcException e) {
                if (e.result() != Result.DISCONNECTED) {
                    throw e;
                }
                return answered;
            }
            answered.merge(text(request), 1, Integer::sum);
        }
    }

    /** Eight clients on threads of their own call one server: each reply goes to its own client, in order. */
    @Test
    void manyClientsAndOneServerEachReplyToItsOwnClient() throws Exception {
        String name = uniqueName("s_many");
        int count = 300;
        List<Connection> clients = new ArrayList<>();
        Connection server;
        CompletableFuture<Map<String, Integer>> answered;
        try (Service service = Fipc.createService(name, 4096, 64, 8, 1)) {
            for (int k = 0; k < 8; k++) {
                clients.add(Fipc.joinService(name, ServiceRole.CLIENT, TEN_SECONDS));
            }
            server = Fipc.joinService(name, ServiceRole.SERVER, TEN_SECONDS);
            assertEquals(9, service.waitForMembers(9, TEN_SECONDS));
            for (Connection member : clients) {
                assertEquals(64, member.maxPiece());
            }
            assertEquals(64, server.maxPiece());
            answered = CompletableFuture.supplyAsync(() -> answerUntilTheEnd(server, 0), MEMBERS);
            List<CompletableFuture<Void>> asking = new ArrayList<>();
            for (int k = 0; k < 8; k++) {
                Connection client = clients.get(k);
                int number = k;
                asking.add(CompletableFuture.runAsync(() -> {
                    try (client) {
                        for (int i = 0; i < count; i++) {
                            byte[] reply = client.call(bytes("client " + number + " request " + i), TEN_SECONDS);
                            assertEquals("CLIENT " + number + " REQUEST " + i, text(reply));
                        }
                    }
                }, MEMBERS));
            }
            for (CompletableFuture<Void> client : asking) {
                client.get(60, TimeUnit.SECONDS);
            }
        } // the service's close ends it: the server's receive throws DISCONNECTED
        assertEquals(8 * count, answered.get(60, TimeUnit.SECONDS).values().stream().mapToInt(Integer::intValue).sum());
        server.close();
    }

    /**
     * Four servers on threads and four clients that send at once: the servers each take a request and wait at a
     * barrier before replying, so all four hold one at the same time; then they answer the rest (copied, into a
     * buffer, in place), each request once, and each client receives its replies in the order of its requests (each
     * sends ten before it receives).
     */
    @Test
    void aPoolOfServerThreadsHoldsRequestsAtOnceAndAnswersEachOnce() throws Exception {
        String name = uniqueName("s_pool");
        int count = 100;
        List<Connection> servers = new ArrayList<>();
        List<CompletableFuture<Map<String, Integer>>> answering = new ArrayList<>();
        try (Service service = Fipc.createService(name, 1 << 14, 64, 4, 4)) {
            for (int k = 0; k < 4; k++) {
                servers.add(Fipc.joinService(name, ServiceRole.SERVER, TEN_SECONDS));
            }
            List<Connection> clients = new ArrayList<>();
            for (int k = 0; k < 4; k++) {
                clients.add(Fipc.joinService(name, ServiceRole.CLIENT, TEN_SECONDS));
            }
            assertEquals(4, service.waitForMembers(ServiceRole.SERVER, 4, TEN_SECONDS));
            CyclicBarrier barrier = new CyclicBarrier(4);
            for (int k = 0; k < 4; k++) {
                Connection server = servers.get(k);
                int way = k % 3;
                answering.add(CompletableFuture.supplyAsync(() -> {
                    byte[] request = server.receive(TEN_SECONDS);
                    try {
                        barrier.await(10, TimeUnit.SECONDS); // every server holds a request
                    } catch (Exception e) {
                        throw new IllegalStateException(e);
                    }
                    server.send(Peer.upper(request), Fipc.NO_WAIT);
                    Map<String, Integer> answered = answerUntilTheEnd(server, way);
                    answered.merge(text(request), 1, Integer::sum);
                    return answered;
                }, MEMBERS));
            }
            List<CompletableFuture<Void>> asking = new ArrayList<>();
            for (int k = 0; k < 4; k++) {
                Connection client = clients.get(k);
                int number = k;
                asking.add(CompletableFuture.runAsync(() -> {
                    try (client) {
                        for (int start = 0; start < count; start += 10) { // ten in flight, then their ten replies
                            for (int i = start; i < start + 10; i++) {
                                client.send(bytes(number + " " + i), TEN_SECONDS);
                            }
                            for (int i = start; i < start + 10; i++) {
                                assertEquals(number + " " + i, text(client.receive(TEN_SECONDS)));
                            }
                        }
                    }
                }, MEMBERS));
            }
            for (CompletableFuture<Void> client : asking) {
                client.get(60, TimeUnit.SECONDS);
            }
        }
        Map<String, Integer> all = new HashMap<>();
        for (CompletableFuture<Map<String, Integer>> server : answering) {
            Map<String, Integer> answered = server.get(60, TimeUnit.SECONDS);
            assertTrue(!answered.isEmpty(), "a server answered nothing");
            answered.forEach((request, times) -> all.merge(request, times, Integer::sum));
        }
        for (Connection server : servers) {
            server.close();
        }
        assertEquals(4 * count, all.size());
        for (int k = 0; k < 4; k++) {
            for (int i = 0; i < count; i++) {
                assertEquals(1, all.getOrDefault(k + " " + i, 0), "request " + k + " " + i);
            }
        }
    }

    @Test
    void aClientsRepliesComeInTheOrderOfItsRequestsWhoeverRepliesFirst() {
        String name = uniqueName("s_order");
        try (Service _ = Fipc.createService(name, 4096, 64, 1, 2);
             Connection client = Fipc.joinService(name, ServiceRole.CLIENT, TEN_SECONDS);
             Connection slow = Fipc.joinService(name, ServiceRole.SERVER, TEN_SECONDS);
             Connection fast = Fipc.joinService(name, ServiceRole.SERVER, TEN_SECONDS)) {
            client.send(bytes("first"), TEN_SECONDS);
            client.send(bytes("second"), TEN_SECONDS);
            assertEquals("first", text(slow.receive(TEN_SECONDS))); // a client's requests are taken in its order
            assertEquals("second", text(fast.receive(TEN_SECONDS)));
            fast.send(bytes("SECOND"), Fipc.NO_WAIT);
            assertFipc(Result.TIMEOUT, () -> client.receive(Duration.ofMillis(50))); // the first reply comes first
            slow.send(bytes("FIRST"), Fipc.NO_WAIT);
            assertEquals("FIRST", text(client.receive(TEN_SECONDS)));
            assertEquals("SECOND", text(client.receive(TEN_SECONDS)));
            assertFipc(Result.TIMEOUT, () -> client.receive(Fipc.NO_WAIT));
        }
    }

    /**
     * A server that closes its connection holding a request abandons it: the client's receive (copied, in place) or
     * its call throws ABANDONED, the request counts as answered, and the next request gets its own reply.
     */
    @Test
    void aServerThatClosesHoldingARequestAbandonsIt() throws Exception {
        String name = uniqueName("s_abandoned");
        try (Service _ = Fipc.createService(name, 4096, 64, 1, 2);
             Connection client = Fipc.joinService(name, ServiceRole.CLIENT, TEN_SECONDS);
             Connection other = Fipc.joinService(name, ServiceRole.SERVER, TEN_SECONDS)) {
            Connection quitter = Fipc.joinService(name, ServiceRole.SERVER, TEN_SECONDS);
            client.send(bytes("one"), TEN_SECONDS);
            assertEquals("one", text(quitter.receive(TEN_SECONDS)));
            quitter.close(); // holding it: the request is answered ABANDONED
            FipcException e = assertFipc(Result.ABANDONED, () -> client.receive(TEN_SECONDS));
            assertEquals("fipc_recv", e.call());
            assertEquals(-1, e.messageLength());
            assertEquals(-1, e.lostMessages());
            assertFipc(Result.TIMEOUT, () -> client.receive(Fipc.NO_WAIT)); // answered once: nothing more is owed

            // In place too
            Connection inPlace = Fipc.joinService(name, ServiceRole.SERVER, TEN_SECONDS);
            client.send(bytes("two"), TEN_SECONDS);
            assertEquals("two", text(inPlace.receiveAcquire(TEN_SECONDS).toArray(ValueLayout.JAVA_BYTE)));
            inPlace.close(); // on the receiving thread: the segment ends, and the close runs, abandoning it
            inPlace.receiveRelease(); // nothing held any more: does nothing
            assertEquals("fipc_recv_acquire",
                assertFipc(Result.ABANDONED, () -> client.receiveAcquire(TEN_SECONDS)).call());

            // call throws ABANDONED too, and the request counts as answered: the next call gets its own reply
            Connection last = Fipc.joinService(name, ServiceRole.SERVER, TEN_SECONDS);
            CompletableFuture<byte[]> calling =
                CompletableFuture.supplyAsync(() -> client.call(bytes("three"), TEN_SECONDS), MEMBERS);
            assertEquals("three", text(last.receive(TEN_SECONDS)));
            last.close();
            ExecutionException failed = assertThrows(ExecutionException.class,
                () -> calling.get(10, TimeUnit.SECONDS));
            assertEquals(Result.ABANDONED, ((FipcException) failed.getCause()).result());
            CompletableFuture<Void> answering = answerAside(other);
            assertEquals("FOUR", text(client.call(bytes("four"), TEN_SECONDS)));
            answering.get(10, TimeUnit.SECONDS);
        }
    }

    /**
     * A server that received a request in place and replied (copied, or in place) closes at its close, with no
     * receiveRelease: the reply invalidated the request's segment and dropped its hold. Its place, the service's only
     * one, is free for the next server.
     */
    @Test
    void aServerThatRepliedToARequestHeldInPlaceClosesAtItsClose() {
        String name = uniqueName("s_replied");
        try (Service _ = Fipc.createService(name, 4096, 64, 1, 1);
             Connection client = Fipc.joinService(name, ServiceRole.CLIENT, TEN_SECONDS)) {
            for (int way = 0; way < 2; way++) {
                try (Connection server = Fipc.joinService(name, ServiceRole.SERVER, TEN_SECONDS)) {
                    client.send(bytes("in place"), TEN_SECONDS);
                    MemorySegment request = server.receiveAcquire(TEN_SECONDS);
                    byte[] reply = Peer.upper(request.toArray(ValueLayout.JAVA_BYTE));
                    if (way == 0) {
                        server.send(MemorySegment.ofArray(reply), Fipc.NO_WAIT);
                    } else {
                        MemorySegment room = server.sendAcquire(reply.length, Fipc.NO_WAIT);
                        room.copyFrom(MemorySegment.ofArray(reply));
                        server.sendCommit(reply.length);
                    }
                    // The client may reuse the request's room now: its segment is invalid
                    assertThrows(IllegalStateException.class, () -> request.get(ValueLayout.JAVA_BYTE, 0));
                    assertEquals("IN PLACE", text(client.receive(TEN_SECONDS)));
                    server.receiveRelease(); // nothing held any more: does nothing
                }
                // Closed: the place is free again (a server that stayed open would keep it, and this join time out)
                Fipc.joinService(name, ServiceRole.SERVER, TEN_SECONDS).close();
            }
        }
    }

    /**
     * A request received in place on this thread and replied to on another (copied, or in place): the reply is refused
     * (WrongThreadException, nothing sent; the reservation stays) while the segment is valid here, since the other
     * thread can't invalidate it, and the client may reuse the request's room once replied to; after the release
     * here, the reply goes, and the segment stays invalid. The server closes at its close.
     */
    @Test
    void aReplyOnAnotherThreadIsRefusedUntilTheRequestHeldInPlaceIsReleased() throws Exception {
        String name = uniqueName("s_replied_aside");
        try (Service _ = Fipc.createService(name, 4096, 64, 1, 1);
             Connection client = Fipc.joinService(name, ServiceRole.CLIENT, TEN_SECONDS)) {
            for (int way = 0; way < 2; way++) {
                try (Connection server = Fipc.joinService(name, ServiceRole.SERVER, TEN_SECONDS)) {
                    client.send(bytes("aside"), TEN_SECONDS);
                    MemorySegment request = server.receiveAcquire(TEN_SECONDS);
                    byte[] reply = Peer.upper(request.toArray(ValueLayout.JAVA_BYTE));
                    boolean inPlace = way == 1;
                    CountDownLatch refused = new CountDownLatch(1);
                    CountDownLatch released = new CountDownLatch(1);
                    CompletableFuture<Void> replying = CompletableFuture.runAsync(() -> {
                        try {
                            if (inPlace) {
                                MemorySegment room = server.sendAcquire(reply.length, Fipc.NO_WAIT);
                                room.copyFrom(MemorySegment.ofArray(reply));
                                assertThrows(WrongThreadException.class, () -> server.sendCommit(reply.length));
                                refused.countDown();
                                assertTrue(released.await(10, TimeUnit.SECONDS));
                                server.sendCommit(reply.length); // the reservation stayed
                            } else {
                                assertThrows(WrongThreadException.class,
                                    () -> server.send(MemorySegment.ofArray(reply), Fipc.NO_WAIT));
                                refused.countDown();
                                assertTrue(released.await(10, TimeUnit.SECONDS));
                                server.send(MemorySegment.ofArray(reply), Fipc.NO_WAIT);
                            }
                        } catch (InterruptedException e) {
                            throw new IllegalStateException(e);
                        }
                    }, MEMBERS);
                    assertTrue(refused.await(10, TimeUnit.SECONDS));
                    assertEquals("aside", text(request.toArray(ValueLayout.JAVA_BYTE))); // still held, still readable
                    assertFipc(Result.TIMEOUT, () -> client.receive(Duration.ofMillis(50))); // nothing sent
                    server.receiveRelease(); // the request stays held, for the other thread's reply
                    assertThrows(IllegalStateException.class, () -> request.get(ValueLayout.JAVA_BYTE, 0));
                    released.countDown();
                    replying.get(10, TimeUnit.SECONDS);
                    assertEquals("ASIDE", text(client.receive(TEN_SECONDS)));
                    assertThrows(IllegalStateException.class, () -> request.get(ValueLayout.JAVA_BYTE, 0));
                }
                // Closed: the place is free again
                Fipc.joinService(name, ServiceRole.SERVER, TEN_SECONDS).close();
            }
        }
    }

    /**
     * A server closed while it holds a request in place, with no reply: on the thread that received it, the close
     * invalidates the segment and closes, and the client gets ABANDONED; on another thread, the segment stays readable
     * and holds the connection open until it ends: at the receiving thread's receiveRelease, or at its reply, which
     * throws (the connection is closed); the request is ABANDONED then.
     */
    @Test
    void aServerClosedHoldingARequestInPlaceAbandonsIt() throws Exception {
        String name = uniqueName("s_closed_held");
        try (Service _ = Fipc.createService(name, 4096, 64, 1, 1);
             Connection client = Fipc.joinService(name, ServiceRole.CLIENT, TEN_SECONDS)) {
            // On the receiving thread
            Connection server = Fipc.joinService(name, ServiceRole.SERVER, TEN_SECONDS);
            client.send(bytes("one"), TEN_SECONDS);
            MemorySegment request = server.receiveAcquire(TEN_SECONDS);
            server.close();
            assertThrows(IllegalStateException.class, () -> request.get(ValueLayout.JAVA_BYTE, 0));
            assertFipc(Result.ABANDONED, () -> client.receive(TEN_SECONDS));

            // On another thread: the close waits for the segment's end, a receiveRelease
            Connection second = Fipc.joinService(name, ServiceRole.SERVER, TEN_SECONDS); // the place is free again
            client.send(bytes("two"), TEN_SECONDS);
            MemorySegment held = second.receiveAcquire(TEN_SECONDS);
            CompletableFuture.runAsync(second::close, MEMBERS).get(10, TimeUnit.SECONDS);
            assertFipc(Result.TIMEOUT, () -> client.receive(Duration.ofMillis(100))); // still held, not answered
            assertEquals("two", text(held.toArray(ValueLayout.JAVA_BYTE))); // still readable
            second.receiveRelease();
            assertThrows(IllegalStateException.class, () -> held.get(ValueLayout.JAVA_BYTE, 0));
            assertFipc(Result.ABANDONED, () -> client.receive(TEN_SECONDS));

            // On another thread, then the reply, which throws: that ends the segment too
            Connection third = Fipc.joinService(name, ServiceRole.SERVER, TEN_SECONDS);
            client.send(bytes("three"), TEN_SECONDS);
            MemorySegment last = third.receiveAcquire(TEN_SECONDS);
            CompletableFuture.runAsync(third::close, MEMBERS).get(10, TimeUnit.SECONDS);
            assertThrows(IllegalStateException.class, () -> third.send(bytes("THREE"), Fipc.NO_WAIT));
            assertThrows(IllegalStateException.class, () -> last.get(ValueLayout.JAVA_BYTE, 0));
            assertFipc(Result.ABANDONED, () -> client.receive(TEN_SECONDS));
            Fipc.joinService(name, ServiceRole.SERVER, TEN_SECONDS).close(); // every server closed: the place is free
        }
    }

    /** Joins a server, takes a request in place and replies to it; the server is unreachable once this returns. */
    private static void droppedServer(String name, Connection client) {
        Connection server = Fipc.joinService(name, ServiceRole.SERVER, TEN_SECONDS);
        client.send(bytes("dropped"), TEN_SECONDS);
        server.send(MemorySegment.ofArray(Peer.upper(server.receiveAcquire(TEN_SECONDS).toArray(ValueLayout.JAVA_BYTE))),
            Fipc.NO_WAIT);
    }

    /**
     * A server that replied to a request held in place and was never closed is closed by the cleaner once the garbage
     * collector finds it unreachable: the reply ended the segment's hold, so the service's only place frees.
     */
    @Test
    void aServerDroppedAfterItsReplyToARequestHeldInPlaceIsClosedByTheCleaner() throws Exception {
        String name = uniqueName("s_dropped");
        try (Service _ = Fipc.createService(name, 4096, 64, 1, 1);
             Connection client = Fipc.joinService(name, ServiceRole.CLIENT, TEN_SECONDS)) {
            droppedServer(name, client);
            assertEquals("DROPPED", text(client.receive(TEN_SECONDS)));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            for (;;) {
                System.gc();
                try {
                    Fipc.joinService(name, ServiceRole.SERVER, Duration.ofMillis(100)).close();
                    break;
                } catch (FipcException e) {
                    assertEquals(Result.TIMEOUT, e.result());
                    assertTrue(System.nanoTime() < deadline, "the dropped server was never closed");
                }
            }
        }
    }

    /**
     * A call that gives up leaves its reply owed; when the server that took its request closes holding it, the next
     * call's drop of the owed reply receives ABANDONED instead, and the call then gets its own reply.
     */
    @Test
    void aCallThatGivesUpWhoseRequestIsAbandonedLetsTheNextCallGetItsOwnReply() throws Exception {
        String name = uniqueName("s_call_abandoned");
        try (Service _ = Fipc.createService(name, 4096, 64, 1, 2);
             Connection client = Fipc.joinService(name, ServiceRole.CLIENT, TEN_SECONDS);
             Connection other = Fipc.joinService(name, ServiceRole.SERVER, TEN_SECONDS)) {
            Connection quitter = Fipc.joinService(name, ServiceRole.SERVER, TEN_SECONDS);
            assertFipc(Result.TIMEOUT, () -> client.call(bytes("given up"), Duration.ofMillis(50))); // stays owed
            assertEquals("given up", text(quitter.receive(TEN_SECONDS))); // the only request: this server takes it
            quitter.close(); // holding it: ABANDONED is what the next call drops
            CompletableFuture<Void> answering = answerAside(other);
            assertEquals("NEXT", text(client.call(bytes("next"), TEN_SECONDS)));
            answering.get(10, TimeUnit.SECONDS);
            assertFipc(Result.TIMEOUT, () -> client.receive(Fipc.NO_WAIT)); // nothing more owed
        }
    }

    /**
     * Each role's calls that it can't use are INVALID, maxPiece() is maxMessage, a server holds one request at a time
     * (receiveRelease doesn't release it), and the zero-copy calls work both ways.
     */
    @Test
    void eachRolesRefusedCallsTheZeroCopyCallsAndMaxPieceIsMaxMessage() {
        String name = uniqueName("s_roles");
        try (Service service = Fipc.createService(name, 1 << 14, 1000, 1, 1);
             Connection client = Fipc.joinService(name, ServiceRole.CLIENT, TEN_SECONDS);
             Connection server = Fipc.joinService(name, ServiceRole.SERVER, TEN_SECONDS)) { // joined from this thread
            assertEquals(name, service.name());
            assertEquals(name, client.name());
            assertEquals(1000, client.maxPiece());
            assertEquals(1000, server.maxPiece());
            assertFipc(Result.TOO_LARGE, () -> client.send(new byte[1001], Fipc.NO_WAIT));
            assertFipc(Result.INVALID, () -> client.send(new byte[0], Fipc.NO_WAIT));
            assertFipc(Result.TOO_LARGE, () -> client.sendAcquire(1001, Fipc.NO_WAIT));
            assertFipc(Result.INVALID, () -> client.sendCommit(1)); // no reservation
            assertFipc(Result.TIMEOUT, () -> client.receive(Fipc.NO_WAIT)); // no reply to come
            assertFipc(Result.INVALID, () -> client.waitForMembers()); // a broadcast publisher's call
            client.receiveRelease(); // nothing held: does nothing
            assertFipc(Result.INVALID, () -> server.send(bytes("x"), Fipc.NO_WAIT)); // no request held
            assertFipc(Result.INVALID, () -> server.sendAcquire(1, Fipc.NO_WAIT));
            assertFipc(Result.TIMEOUT, () -> server.receive(Fipc.NO_WAIT));
            assertFipc(Result.INVALID, () -> server.waitForMembers());
            assertEquals("call", assertFipc(Result.INVALID, () -> server.call(bytes("x"), Fipc.NO_WAIT)).call());
            assertFipc(Result.ADDR_IN_USE, () -> Fipc.createService(name, 4096, 64, 1, 1));
            assertFipc(Result.ADDR_IN_USE, () -> Fipc.createQueue(name, 4096, 1, 1));
            assertFipc(Result.ADDR_IN_USE, () -> Fipc.createStream(name, 4096));
            assertFipc(Result.ADDR_IN_USE, () -> Fipc.createBroadcast(name, 4096, 1, Fipc.FOREVER));

            // A server holds one request: its receives are INVALID until it replies; receiveRelease doesn't release it
            byte[] sevens = new byte[1000];
            Arrays.fill(sevens, (byte) 7);
            client.send(sevens, TEN_SECONDS);
            assertEquals(1000, assertFipc(Result.TOO_LARGE, () -> server.receive(ByteBuffer.allocate(999),
                TEN_SECONDS)).messageLength()); // takes nothing
            MemorySegment held = server.receiveAcquire(TEN_SECONDS);
            assertTrue(held.isReadOnly());
            assertEquals(1000, held.byteSize());
            assertFipc(Result.INVALID, () -> server.receive(Fipc.NO_WAIT));
            server.receiveRelease(); // invalidates the segment; the request is still held
            assertThrows(IllegalStateException.class, () -> held.get(ValueLayout.JAVA_BYTE, 0));
            assertFipc(Result.INVALID, () -> server.receiveAcquire(Fipc.NO_WAIT));
            assertFipc(Result.TOO_LARGE, () -> server.send(new byte[1001], Fipc.NO_WAIT));
            MemorySegment room = server.sendAcquire(1000, Fipc.NO_WAIT);
            room.fill((byte) 3);
            assertFipc(Result.INVALID, () -> server.sendCommit(1001));
            server.sendCommit(1000);
            assertFipc(Result.INVALID, () -> server.send(bytes("x"), Fipc.NO_WAIT)); // nothing held any more
            assertEquals(1000, assertFipc(Result.TOO_LARGE, () -> client.receive(ByteBuffer.allocate(10),
                TEN_SECONDS)).messageLength()); // takes nothing
            byte[] threes = new byte[1000];
            Arrays.fill(threes, (byte) 3);
            assertArrayEquals(threes, client.receive(TEN_SECONDS));

            // The zero-copy calls the other way: a request committed in place, read in place and answered from it
            MemorySegment request = client.sendAcquire(5, TEN_SECONDS);
            request.copyFrom(MemorySegment.ofArray(bytes("hello")));
            client.sendCommit(5);
            MemorySegment taken = server.receiveAcquire(TEN_SECONDS);
            MemorySegment reply = server.sendAcquire(5, Fipc.NO_WAIT);
            reply.copyFrom(MemorySegment.ofArray(Peer.upper(taken.toArray(ValueLayout.JAVA_BYTE))));
            server.sendCommit(5);
            server.receiveRelease();
            MemorySegment answer = client.receiveAcquire(TEN_SECONDS);
            assertTrue(answer.isReadOnly());
            assertEquals("HELLO", text(answer.toArray(ValueLayout.JAVA_BYTE)));
            client.receiveRelease();

            // Into a native segment and a heap one
            client.send(bytes("seg"), TEN_SECONDS);
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment target = arena.allocate(16);
                assertEquals(3, server.receive(target, TEN_SECONDS));
                server.send(Peer.upper(target.asSlice(0, 3).toArray(ValueLayout.JAVA_BYTE)), Fipc.NO_WAIT);
            }
            MemorySegment heap = MemorySegment.ofArray(new byte[16]);
            assertEquals(3, client.receive(heap, TEN_SECONDS));
            assertEquals("SEG", text(heap.asSlice(0, 3).toArray(ValueLayout.JAVA_BYTE)));
            assertEquals(1, service.waitForMembers(ServiceRole.CLIENT, 0));
            assertEquals(1, service.waitForMembers(ServiceRole.SERVER, 0));
        }
    }

    /** call is a service client's: INVALID on a stream's connection, a queue's members and a broadcast's publisher. */
    @Test
    void callIsInvalidOnOtherConnectionsAndThrowsOnceClosed() {
        try (TestSupport.Pair pair = new TestSupport.Pair("s_call_stream")) {
            assertFipc(Result.INVALID, () -> pair.client.call(bytes("x"), Fipc.NO_WAIT));
            assertFipc(Result.INVALID, () -> pair.server.call(bytes("x")));
        }
        String name = uniqueName("s_call_queue");
        try (WorkQueue _ = Fipc.createQueue(name, 4096, 1, 1);
             Connection producer = Fipc.joinQueue(name, QueueRole.PRODUCER, TEN_SECONDS);
             Connection consumer = Fipc.joinQueue(name, QueueRole.CONSUMER, TEN_SECONDS)) {
            assertFipc(Result.INVALID, () -> producer.call(bytes("x"), Fipc.NO_WAIT));
            assertFipc(Result.INVALID, () -> consumer.call(bytes("x"), Fipc.NO_WAIT));
        }
        try (Connection publisher = Fipc.createBroadcast(uniqueName("s_call_broadcast"), 4096, 1, Fipc.FOREVER)) {
            assertFipc(Result.INVALID, () -> publisher.call(bytes("x"), Fipc.NO_WAIT));
        }
        String service = uniqueName("s_call_closed");
        try (Service _ = Fipc.createService(service, 4096, 64, 1, 1)) {
            Connection client = Fipc.joinService(service, ServiceRole.CLIENT, TEN_SECONDS);
            assertThrows(NullPointerException.class, () -> client.call(null, Fipc.NO_WAIT));
            assertFipc(Result.INVALID, () -> client.call(new byte[0], Fipc.NO_WAIT)); // an empty request
            assertFipc(Result.TOO_LARGE, () -> client.call(new byte[65], Fipc.NO_WAIT));
            client.close();
            assertThrows(IllegalStateException.class, () -> client.call(bytes("x"), Fipc.NO_WAIT));
        }
    }

    /** createService refuses what is out of range; other kinds' roles are refused on a service, and its on theirs. */
    @Test
    void outOfRangeArgumentsAndJoinersOfOtherKindsAreRefused() {
        String name = uniqueName("s_range");
        long[][] invalid = {{1000, 64, 1, 1}, {4096, 0, 1, 1}, {4096, 1025, 1, 1}, {4096, 64, 0, 1}, {4096, 64, 65, 1},
            {4096, 64, 1, 0}, {4096, 64, 1, 65}, {4096, -1, 1, 1}};
        for (long[] args : invalid) {
            assertFipc(Result.INVALID, () -> Fipc.createService(name, args[0], (int) args[1], (int) args[2],
                (int) args[3]));
        }
        assertThrows(NullPointerException.class, () -> Fipc.createService(null, 4096, 64, 1, 1));
        assertThrows(NullPointerException.class, () -> Fipc.joinService(name, null, FIVE_SECONDS));
        assertThrows(NullPointerException.class, () -> Fipc.joinService(null, ServiceRole.CLIENT, FIVE_SECONDS));
        try (Service _ = Fipc.createService(name, 4096, 64, 1, 1)) { // the other kinds' roles, on a service
            assertFipc(Result.INVALID, () -> Fipc.joinBroadcast(name, SubscriberRole.GATED, FIVE_SECONDS));
            assertFipc(Result.INVALID, () -> Fipc.joinBroadcast(name, SubscriberRole.LOSSY, FIVE_SECONDS));
            assertFipc(Result.INVALID, () -> Fipc.joinQueue(name, QueueRole.PRODUCER, FIVE_SECONDS));
            assertFipc(Result.INVALID, () -> Fipc.joinQueue(name, QueueRole.CONSUMER, FIVE_SECONDS));
            assertFipc(Result.INVALID, () -> Fipc.connectStream(name, FIVE_SECONDS));
        }
        String queue = uniqueName("s_a_queue");
        try (WorkQueue _ = Fipc.createQueue(queue, 4096, 1, 1)) { // a service's roles, on a queue
            assertFipc(Result.INVALID, () -> Fipc.joinService(queue, ServiceRole.CLIENT, FIVE_SECONDS));
            assertFipc(Result.INVALID, () -> Fipc.joinService(queue, ServiceRole.SERVER, FIVE_SECONDS));
        }
        String broadcast = uniqueName("s_a_broadcast");
        try (Connection _ = Fipc.createBroadcast(broadcast, 4096, 1, Fipc.FOREVER)) { // and on a broadcast
            assertFipc(Result.INVALID, () -> Fipc.joinService(broadcast, ServiceRole.CLIENT, FIVE_SECONDS));
        }
        assertFipc(Result.TIMEOUT, () -> Fipc.joinService(uniqueName("s_nobody"), ServiceRole.CLIENT, Fipc.NO_WAIT));
    }

    /**
     * A call whose receive timed out leaves its reply to come; the next call receives and drops it (within its own
     * timeout) before it sends, so it returns the reply to its own request. A call refuses (INVALID) while a request
     * sent with send still has its reply to be received.
     */
    @Test
    void aCallThatGivesUpLeavesItsReplyOwedAndTheNextCallDropsIt() throws Exception {
        String name = uniqueName("s_owed");
        try (Service _ = Fipc.createService(name, 4096, 64, 1, 1);
             Connection client = Fipc.joinService(name, ServiceRole.CLIENT, TEN_SECONDS);
             Connection server = Fipc.joinService(name, ServiceRole.SERVER, TEN_SECONDS)) {
            assertFipc(Result.TIMEOUT, () -> client.call(bytes("late"), Duration.ofMillis(50))); // the request went
            assertEquals("late", text(server.receive(TEN_SECONDS)));
            server.send(bytes("LATE"), Fipc.NO_WAIT); // owed to the call that gave up

            CompletableFuture<Void> answering = answerAside(server);
            assertEquals("NEXT", text(client.call(bytes("next"), TEN_SECONDS))); // not LATE
            answering.get(10, TimeUnit.SECONDS);
            assertFipc(Result.TIMEOUT, () -> client.receive(Fipc.NO_WAIT)); // nothing more owed

            // A call whose owed reply hasn't come waits for it within its own timeout, and gives up before it sends
            assertFipc(Result.TIMEOUT, () -> client.call(bytes("a"), Fipc.NO_WAIT)); // a went; its reply is owed
            assertFipc(Result.TIMEOUT, () -> client.call(bytes("b"), Duration.ofMillis(50))); // a's hasn't come
            assertEquals("a", text(server.receive(TEN_SECONDS)));
            server.send(bytes("A"), Fipc.NO_WAIT);
            assertFipc(Result.TIMEOUT, () -> server.receive(Fipc.NO_WAIT)); // b never went
            CompletableFuture<byte[]> taking =
                CompletableFuture.supplyAsync(() -> server.receive(TEN_SECONDS), MEMBERS);
            assertFipc(Result.TIMEOUT, () -> client.call(bytes("c"), Duration.ofMillis(200))); // drops A, sends c
            assertEquals("c", text(taking.get(10, TimeUnit.SECONDS)));
            server.send(bytes("C"), Fipc.NO_WAIT); // owed to the call that gave up
            assertEquals("C", text(client.receive(TEN_SECONDS))); // the caller's own receive takes it, in order
            assertFipc(Result.TIMEOUT, () -> client.receive(Fipc.NO_WAIT));

            // A request of send's whose reply hasn't been received: call refuses, and leaves it for receive
            client.send(bytes("mine"), TEN_SECONDS);
            assertFipc(Result.INVALID, () -> client.call(bytes("x"), TEN_SECONDS));
            Peer.answer(server, 1);
            assertEquals("MINE", text(client.receive(TEN_SECONDS)));
            // So does one committed in place
            client.sendAcquire(4, TEN_SECONDS).copyFrom(MemorySegment.ofArray(bytes("held")));
            client.sendCommit(4);
            assertFipc(Result.INVALID, () -> client.call(bytes("x"), TEN_SECONDS));
            Peer.answer(server, 1);
            assertEquals("HELD", text(client.receiveAcquire(TEN_SECONDS).toArray(ValueLayout.JAVA_BYTE)));
            client.receiveRelease();
            answering = answerAside(server);
            assertEquals("X", text(client.call(bytes("x"), TEN_SECONDS)));
            answering.get(10, TimeUnit.SECONDS);

            // A send that fails isn't owed a reply
            assertFipc(Result.TOO_LARGE, () -> client.send(new byte[65], Fipc.NO_WAIT));
            answering = answerAside(server);
            assertEquals("Y", text(client.call(bytes("y"), TEN_SECONDS)));
            answering.get(10, TimeUnit.SECONDS);
        }
    }

    /**
     * A call whose timeout runs out while it drops an owed reply never sends its request; with a zero timeout no step
     * waits, and the call sends.
     */
    @Test
    void aCallWhoseTimeoutRunsOutWhileItDropsAnOwedReplyNeverSendsItsRequest() throws Exception {
        String name = uniqueName("s_deadline");
        try (Service _ = Fipc.createService(name, 65536, 8192, 1, 1);
             Connection client = Fipc.joinService(name, ServiceRole.CLIENT, TEN_SECONDS);
             Connection server = Fipc.joinService(name, ServiceRole.SERVER, TEN_SECONDS)) {
            assertFipc(Result.TIMEOUT, () -> client.call(bytes("late"), Duration.ofMillis(50))); // the request went
            assertEquals("late", text(server.receive(TEN_SECONDS)));
            byte[] late = new byte[8192];
            Arrays.fill(late, (byte) 'L');
            server.send(late, Fipc.NO_WAIT); // the late reply has come once the send returns
            // A timeout of 1 ns: the call drops the owed reply (there, so its receive needn't wait), and by then the
            // timeout has run out, so its request isn't sent
            assertFipc(Result.TIMEOUT, () -> client.call(bytes("never"), Duration.ofNanos(1)));
            assertFipc(Result.TIMEOUT, () -> server.receive(Duration.ofMillis(50))); // the server never saw it
            assertFipc(Result.TIMEOUT, () -> client.receive(Fipc.NO_WAIT)); // nothing owed: the late reply was dropped
            CompletableFuture<Void> answering = answerAside(server);
            assertEquals("NEXT", text(client.call(bytes("next"), TEN_SECONDS)));
            answering.get(10, TimeUnit.SECONDS);

            // A zero timeout waits at no step: with the owed reply there, the call drops it and sends
            assertFipc(Result.TIMEOUT, () -> client.call(bytes("a"), Fipc.NO_WAIT)); // a went; its reply is owed
            assertEquals("a", text(server.receive(TEN_SECONDS)));
            server.send(bytes("A"), Fipc.NO_WAIT);
            assertFipc(Result.TIMEOUT, () -> client.call(bytes("b"), Fipc.NO_WAIT)); // drops A, sends b
            assertEquals("b", text(server.receive(TEN_SECONDS)));
            server.send(bytes("B"), Fipc.NO_WAIT);
            assertEquals("B", text(client.receive(TEN_SECONDS))); // a plain receive takes the late reply first
        }
    }

    /** A call checks its request at once, even while a reply is owed: INVALID when empty, TOO_LARGE when too long. */
    @Test
    void aCallChecksItsRequestAtOnceEvenWhileAReplyIsOwed() throws Exception {
        String name = uniqueName("s_check");
        try (Service _ = Fipc.createService(name, 4096, 64, 1, 1);
             Connection client = Fipc.joinService(name, ServiceRole.CLIENT, TEN_SECONDS);
             Connection server = Fipc.joinService(name, ServiceRole.SERVER, TEN_SECONDS)) {
            assertFipc(Result.TIMEOUT, () -> client.call(bytes("held"), Duration.ofMillis(50))); // its reply is owed
            assertEquals("held", text(server.receive(TEN_SECONDS))); // held: the owed reply doesn't come
            long started = System.nanoTime();
            assertFipc(Result.INVALID, () -> client.call(new byte[0]));
            assertFipc(Result.TOO_LARGE, () -> client.call(new byte[client.maxPiece() + 1]));
            long elapsedMs = (System.nanoTime() - started) / 1_000_000;
            assertTrue(elapsedMs < 2000, elapsedMs + " ms"); // neither waited
            server.send(bytes("HELD"), Fipc.NO_WAIT); // still owed: the next call drops it and returns its own reply
            CompletableFuture<Void> answering = answerAside(server);
            assertEquals("NEXT", text(client.call(bytes("next"), TEN_SECONDS)));
            answering.get(10, TimeUnit.SECONDS);
            assertFipc(Result.TIMEOUT, () -> client.receive(Fipc.NO_WAIT));
        }
    }

    /**
     * A call after the service's end: DISCONNECTED from the drop of an owed reply that never came and from the send,
     * INVALID while a reply to a request of send's is owed.
     */
    @Test
    void aCallAfterTheServicesEnd() {
        String name = uniqueName("s_end_owed");
        Service service = Fipc.createService(name, 4096, 64, 1, 1);
        try (Connection client = Fipc.joinService(name, ServiceRole.CLIENT, TEN_SECONDS);
             Connection server = Fipc.joinService(name, ServiceRole.SERVER, TEN_SECONDS)) {
            assertFipc(Result.TIMEOUT, () -> client.call(bytes("held"), Duration.ofMillis(50))); // its reply is owed
            assertEquals("held", text(server.receive(TEN_SECONDS)));
            service.close();
            assertFipc(Result.DISCONNECTED, () -> client.call(bytes("x"), TEN_SECONDS));
        }

        String sendName = uniqueName("s_end_send");
        service = Fipc.createService(sendName, 4096, 64, 1, 1);
        try (Connection client = Fipc.joinService(sendName, ServiceRole.CLIENT, TEN_SECONDS);
             Connection _ = Fipc.joinService(sendName, ServiceRole.SERVER, TEN_SECONDS)) {
            service.close();
            assertFipc(Result.DISCONNECTED, () -> client.call(bytes("x"), TEN_SECONDS)); // nothing owed: the send
            assertFipc(Result.INVALID, () -> client.call(new byte[0], TEN_SECONDS)); // checked first, end or not
        }

        String sentName = uniqueName("s_end_sent");
        service = Fipc.createService(sentName, 4096, 64, 1, 1);
        try (Connection client = Fipc.joinService(sentName, ServiceRole.CLIENT, TEN_SECONDS);
             Connection _ = Fipc.joinService(sentName, ServiceRole.SERVER, TEN_SECONDS)) {
            client.send(bytes("mine"), TEN_SECONDS);
            service.close();
            assertFipc(Result.INVALID, () -> client.call(bytes("x"), TEN_SECONDS));
            assertFipc(Result.DISCONNECTED, () -> client.receive(TEN_SECONDS)); // that reply never comes
        }
    }

    /**
     * A cancel ends a client's wait (a cancelled call leaves its reply owed, and a receive that needn't wait still
     * works) and a server's.
     */
    @Test
    void aCancelEndsAClientsAndAServersWaitAndACancelledCallLeavesItsReplyOwed() throws Exception {
        String name = uniqueName("s_cancel");
        try (Service _ = Fipc.createService(name, 4096, 64, 2, 1);
             Connection server = Fipc.joinService(name, ServiceRole.SERVER, TEN_SECONDS)) {
            Connection client = Fipc.joinService(name, ServiceRole.CLIENT, TEN_SECONDS);
            CompletableFuture<byte[]> calling =
                CompletableFuture.supplyAsync(() -> client.call(bytes("cancelled")), MEMBERS);
            assertEquals("cancelled", text(server.receive(TEN_SECONDS)));
            client.cancel(); // from another thread: the call's wait for its reply throws CANCELLED
            ExecutionException failed = assertThrows(ExecutionException.class,
                () -> calling.get(10, TimeUnit.SECONDS));
            assertEquals(Result.CANCELLED, ((FipcException) failed.getCause()).result());
            server.send(bytes("CANCELLED"), Fipc.NO_WAIT); // owed to the cancelled call
            assertEquals("CANCELLED", text(client.receive(Fipc.NO_WAIT))); // a receive that needn't wait works
            assertFipc(Result.CANCELLED, () -> client.receive(TEN_SECONDS)); // final
            client.close();

            try (Connection other = Fipc.joinService(name, ServiceRole.CLIENT, TEN_SECONDS)) { // the server goes on
                CompletableFuture<byte[]> waiting = CompletableFuture.supplyAsync(() -> server.receive(), MEMBERS);
                server.cancel();
                failed = assertThrows(ExecutionException.class, () -> waiting.get(10, TimeUnit.SECONDS));
                assertEquals(Result.CANCELLED, ((FipcException) failed.getCause()).result());
                other.send(bytes("after"), TEN_SECONDS);
                assertEquals("after", text(server.receive(TEN_SECONDS))); // a request there: needn't wait
                server.send(bytes("AFTER")); // a reply never waits
                assertEquals("AFTER", text(other.receive(TEN_SECONDS)));
                assertFipc(Result.CANCELLED, () -> server.receive(TEN_SECONDS)); // final
            }
        }
    }

    /**
     * A call on a cancelled connection throws CANCELLED at once, whatever its timeout, after its request's checks, and
     * sends nothing: the server, which listens, receives no request.
     */
    @Test
    void aCallAfterCancelThrowsCancelledAndSendsNothing() throws Exception {
        String name = uniqueName("s_call_cancelled");
        try (Service _ = Fipc.createService(name, 4096, 64, 1, 1);
             Connection server = Fipc.joinService(name, ServiceRole.SERVER, TEN_SECONDS);
             Connection client = Fipc.joinService(name, ServiceRole.CLIENT, TEN_SECONDS)) {
            client.cancel();
            assertFipc(Result.INVALID, () -> client.call(new byte[0], TEN_SECONDS)); // the request's checks come first
            assertFipc(Result.TOO_LARGE, () -> client.call(new byte[65], TEN_SECONDS));
            for (Duration timeout : List.of(Fipc.FOREVER, Fipc.NO_WAIT, TEN_SECONDS)) {
                assertFipc(Result.CANCELLED, () -> client.call(bytes("never sent"), timeout));
            }
            assertFipc(Result.TIMEOUT, () -> server.receive(Duration.ofMillis(200))); // no request went
        }
    }

    /**
     * A client that sends on one thread and receives on another: each request is counted before its send, so the
     * receiving thread may take its reply first; once every reply has been received, nothing is owed and call works.
     */
    @Test
    void aClientSendingOnOneThreadAndReceivingOnAnotherOwesNothingOnceDone() throws Exception {
        String name = uniqueName("s_threads");
        int count = 2000;
        Service service = Fipc.createService(name, 1 << 14, 64, 1, 1);
        try (Connection client = Fipc.joinService(name, ServiceRole.CLIENT, TEN_SECONDS);
             Connection server = Fipc.joinService(name, ServiceRole.SERVER, TEN_SECONDS)) {
            CompletableFuture<Integer> answered =
                CompletableFuture.supplyAsync(() -> Peer.answerUntilTheEnd(server), MEMBERS);
            CompletableFuture<Void> receiving = CompletableFuture.runAsync(() -> {
                for (int i = 0; i < count; i++) {
                    assertEquals("R " + i, text(client.receive(TEN_SECONDS)));
                }
            }, MEMBERS);
            for (int i = 0; i < count; i++) {
                client.send(bytes("r " + i), TEN_SECONDS);
            }
            receiving.get(60, TimeUnit.SECONDS);
            assertEquals("LAST", text(client.call(bytes("last"), TEN_SECONDS)));
            service.close(); // ends the service: the server's receive throws DISCONNECTED
            assertEquals(count + 1, answered.get(60, TimeUnit.SECONDS));
        } finally {
            service.close();
        }
    }

    @Test
    void theOwnersWaitForMembersAndItsCancel() throws Exception {
        String name = uniqueName("s_members");
        try (Service service = Fipc.createService(name, 4096, 64, 2, 3)) {
            assertEquals(0, service.waitForMembers(0));
            assertEquals(0, service.waitForMembers(ServiceRole.CLIENT, 0));
            assertEquals(0, service.waitForMembers(ServiceRole.SERVER, 0, Fipc.NO_WAIT));
            FipcException timeout = assertFipc(Result.TIMEOUT, () -> service.waitForMembers(1, Fipc.NO_WAIT));
            assertEquals(0, timeout.memberCount());
            assertEquals("fipc_service_wait_for_members", timeout.call());

            CompletableFuture<Integer> waiting = CompletableFuture.supplyAsync(
                () -> service.waitForMembers(ServiceRole.SERVER, 1, TEN_SECONDS), MEMBERS); // while one joins
            try (Connection _ = Fipc.joinService(name, ServiceRole.SERVER, TEN_SECONDS);
                 Connection _ = Fipc.joinService(name, ServiceRole.CLIENT, TEN_SECONDS)) {
                assertTrue(waiting.get(10, TimeUnit.SECONDS) >= 1);
                assertEquals(2, service.waitForMembers(2, TEN_SECONDS));
                assertEquals(1, service.waitForMembers(ServiceRole.CLIENT, 0));
                assertEquals(1, service.waitForMembers(ServiceRole.SERVER, 0));
                assertEquals(1, assertFipc(Result.TIMEOUT,
                    () -> service.waitForMembers(ServiceRole.CLIENT, 2, Duration.ofMillis(50))).memberCount());

                // Over the role's maximum (maxClients, maxServers, their sum for both)
                assertEquals(-1, assertFipc(Result.INVALID,
                    () -> service.waitForMembers(ServiceRole.CLIENT, 3, Fipc.NO_WAIT)).memberCount());
                assertFipc(Result.INVALID, () -> service.waitForMembers(ServiceRole.SERVER, 4, Fipc.NO_WAIT));
                assertFipc(Result.INVALID, () -> service.waitForMembers(6, Fipc.NO_WAIT));
                assertThrows(IllegalArgumentException.class, () -> service.waitForMembers(-1, Fipc.NO_WAIT));
                assertThrows(NullPointerException.class, () -> service.waitForMembers(null, 1, Fipc.NO_WAIT));

                CompletableFuture<Integer> cancelled = CompletableFuture.supplyAsync(
                    () -> service.waitForMembers(ServiceRole.SERVER, 2, TEN_SECONDS), MEMBERS);
                service.cancel(); // from this thread: the wait, now or about to start, throws CANCELLED
                ExecutionException failed = assertThrows(ExecutionException.class,
                    () -> cancelled.get(10, TimeUnit.SECONDS));
                FipcException e = (FipcException) failed.getCause();
                assertEquals(Result.CANCELLED, e.result());
                assertEquals(1, e.memberCount());
                assertEquals(2, assertFipc(Result.CANCELLED, () -> service.waitForMembers(3, TEN_SECONDS))
                    .memberCount()); // final

                // The service goes on after its cancel: members join, and a call goes through
                try (Connection client = Fipc.joinService(name, ServiceRole.CLIENT, TEN_SECONDS);
                     Connection server = Fipc.joinService(name, ServiceRole.SERVER, TEN_SECONDS)) {
                    CompletableFuture<Void> answering = answerAside(server);
                    assertEquals("AFTER THE CANCEL", text(client.call(bytes("after the cancel"), TEN_SECONDS)));
                    answering.get(10, TimeUnit.SECONDS);
                }
            }
        }
    }

    /**
     * The service's close ends it: clients' sends and servers' receives throw DISCONNECTED, a client receives the
     * replies that came before the end, then DISCONNECTED, a server's reply throws DISCONNECTED, and the name is free
     * again at once.
     */
    @Test
    void theServicesCloseEndsItForClientsAndServers() {
        String name = uniqueName("s_end");
        Service service = Fipc.createService(name, 4096, 64, 1, 2);
        try (Connection client = Fipc.joinService(name, ServiceRole.CLIENT, TEN_SECONDS);
             Connection server = Fipc.joinService(name, ServiceRole.SERVER, TEN_SECONDS);
             Connection holder = Fipc.joinService(name, ServiceRole.SERVER, TEN_SECONDS)) {
            client.send(bytes("done"), TEN_SECONDS);
            client.send(bytes("held"), TEN_SECONDS);
            Peer.answer(server, 1);
            assertEquals("held", text(holder.receive(TEN_SECONDS)));
            service.close();
            service.close(); // a second close does nothing
            assertThrows(IllegalStateException.class, () -> service.waitForMembers(0));
            service.cancel(); // a no-op once closed
            assertFipc(Result.DISCONNECTED, () -> client.send(bytes("after"), TEN_SECONDS));
            assertFipc(Result.DISCONNECTED, () -> server.receive(TEN_SECONDS));
            assertEquals("DONE", text(client.receive(TEN_SECONDS))); // the replies before the end, then DISCONNECTED
            assertFipc(Result.DISCONNECTED, () -> client.receive(TEN_SECONDS));
            assertFipc(Result.DISCONNECTED, () -> holder.send(bytes("HELD"), Fipc.NO_WAIT)); // a reply after the end
            Fipc.createService(name, 4096, 64, 1, 1).close(); // the name is free again at once
        }
    }

    /**
     * A service has no cleaner: one created in a method that keeps only a joined server goes on after the garbage
     * collector has collected its object, and holds its name until the end of the process (this test's JVM).
     */
    @Test
    void aServiceTheGarbageCollectorCollectsLivesOn() throws Exception {
        String name = uniqueName("s_gc");
        List<WeakReference<Service>> services = new ArrayList<>();
        try (Connection server = startService(name, services)) {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (services.getFirst().get() != null) {
                assertTrue(System.nanoTime() < deadline, "the garbage collector didn't collect the service's object");
                System.gc();
                Thread.sleep(10);
            }
            try (Connection client = Fipc.joinService(name, ServiceRole.CLIENT, TEN_SECONDS)) {
                client.send(bytes("still here"), TEN_SECONDS);
                Peer.answer(server, 1);
                assertEquals("STILL HERE", text(client.receive(TEN_SECONDS)));
            }
            assertFipc(Result.ADDR_IN_USE, () -> Fipc.createService(name, 4096, 64, 1, 1));
        }
    }

    /** Creates a service on {@code name}, keeps it only in a weak reference in {@code services}, and joins a server. */
    private static Connection startService(String name, List<WeakReference<Service>> services) {
        Service service = Fipc.createService(name, 4096, 64, 1, 1);
        services.add(new WeakReference<>(service));
        return Fipc.joinService(name, ServiceRole.SERVER, TEN_SECONDS);
    }

    /**
     * Requests and replies longer than the copying receives' first native buffer (4 KiB) and than its longest (1 MiB)
     * arrive whole on both sides, into new arrays, heap segments and native ones; one too long for a buffer takes
     * nothing.
     */
    @Test
    void longRequestsAndRepliesArriveWhole() {
        String name = uniqueName("s_long");
        try (Service _ = Fipc.createService(name, 8 << 20, 2 << 20, 1, 1);
             Connection client = Fipc.joinService(name, ServiceRole.CLIENT, TEN_SECONDS);
             Connection server = Fipc.joinService(name, ServiceRole.SERVER, TEN_SECONDS)) {
            for (int length : new int[] {5000, 1536 << 10, 2 << 20}) {
                byte[] request = TestSupport.pattern(length);
                client.send(request, TEN_SECONDS);
                assertArrayEquals(request, server.receive(TEN_SECONDS), "length " + length);
                byte[] reply = request.clone();
                Arrays.fill(reply, 0, 4, (byte) 1);
                server.send(reply, Fipc.NO_WAIT);
                assertEquals(length, assertFipc(Result.TOO_LARGE,
                    () -> client.receive(ByteBuffer.allocateDirect(length - 1), TEN_SECONDS)).messageLength());
                MemorySegment heap = MemorySegment.ofArray(new byte[length]);
                assertEquals(length, client.receive(heap, TEN_SECONDS));
                assertArrayEquals(reply, heap.toArray(ValueLayout.JAVA_BYTE), "length " + length);

                client.send(reply, TEN_SECONDS);
                MemorySegment into = MemorySegment.ofArray(new byte[length]);
                assertEquals(length, server.receive(into, TEN_SECONDS));
                assertArrayEquals(reply, into.toArray(ValueLayout.JAVA_BYTE), "length " + length);
                server.send(request, Fipc.NO_WAIT);
                try (Arena arena = Arena.ofConfined()) {
                    MemorySegment target = arena.allocate(length);
                    assertEquals(length, client.receive(target, TEN_SECONDS));
                    assertArrayEquals(request, target.toArray(ValueLayout.JAVA_BYTE), "length " + length);
                }

                CompletableFuture<Void> echoing = CompletableFuture.runAsync(
                    () -> server.send(server.receive(TEN_SECONDS), Fipc.NO_WAIT), MEMBERS);
                assertArrayEquals(reply, client.call(reply, TEN_SECONDS), "length " + length);
                echoing.join();
            }
        }
    }

    /**
     * The examples' exchange in this process: the server creates the service, joins it as its server, answers three
     * requests and closes its server and the service; the client calls ping 1 to ping 3 and checks each reply. Client
     * first (its join waits for the service) and client last (it joins once the server waits for a request).
     */
    @Test
    void theExamplesExchangeInEitherOrder() throws Exception {
        for (boolean clientFirst : new boolean[] {true, false}) {
            String name = uniqueName("s_example");
            CompletableFuture<Void> asking = null;
            if (clientFirst) {
                asking = CompletableFuture.runAsync(() -> Peer.joinAndCall(name, 3), MEMBERS);
                Thread.sleep(200);
            }
            try (Service _ = Fipc.createService(name, 4096, 256, 1, 1);
                 Connection server = Fipc.joinService(name, ServiceRole.SERVER)) {
                if (!clientFirst) {
                    asking = CompletableFuture.runAsync(() -> Peer.joinAndCall(name, 3), MEMBERS);
                }
                for (int i = 1; i <= 3; i++) {
                    server.send(Peer.upper(server.receive()));
                }
            }
            asking.get(30, TimeUnit.SECONDS);
        }
    }

    /**
     * examples/java's ServiceServer.java and ServiceClient.java, compiled here and each run in a JVM of its own, client
     * first and server first: the client prints PING 1 to PING 3, in order, and both exit 0. The start order is
     * likely, not certain (QueueTest's example programs say why, and why it holds either way).
     */
    @Test
    void theExampleProgramsInTwoProcessesInEitherOrder(@TempDir Path classes) throws Exception {
        TestSupport.runExamplePair(classes, "ServiceServer", "ServiceClient", List.of("PING 1", "PING 2", "PING 3"),
            false);
    }

    private static void assertExited(Process peer, String what) throws InterruptedException {
        assertTrue(peer.waitFor(60, TimeUnit.SECONDS), what + " didn't exit");
        assertEquals(0, peer.exitValue(), what + " failed");
    }

    /** A server (the service's owner's) in another JVM, a client here. */
    @Test
    void aServerInAnotherProcessAndAClientHere() throws Exception {
        String name = uniqueName("s_server_peer");
        Process peer = startPeer("service-server", name, "300", "1");
        Peer.joinAndCall(name, 300);
        assertExited(peer, "the server peer");
    }

    /** Two clients in other JVMs, the owner's server here. */
    @Test
    void clientsInOtherProcessesAndTheOwnersServerHere() throws Exception {
        String name = uniqueName("s_client_peers");
        List<Process> peers = List.of(startPeer("service-client", name, "300"),
            startPeer("service-client", name, "300"));
        Peer.serve(name, 300, 2);
        for (Process peer : peers) {
            assertExited(peer, "a client peer");
        }
    }

    /** Two joined servers in other JVMs, the owner's client here. */
    @Test
    void joinedServersInOtherProcessesAndTheOwnersClientHere() throws Exception {
        String name = uniqueName("s_joined_peers");
        List<Process> peers = List.of(startPeer("service-joined-server", name),
            startPeer("service-joined-server", name));
        Peer.ownAndCall(name, 300, 2);
        for (Process peer : peers) {
            assertExited(peer, "a server peer");
        }
    }

    /** The owner's client in another JVM, two joined servers here. */
    @Test
    void theOwnersClientInAnotherProcessAndJoinedServersHere() throws Exception {
        String name = uniqueName("s_owner_peer");
        Process peer = startPeer("service-owner-client", name, "300", "2");
        List<CompletableFuture<Void>> servers = List.of(
            CompletableFuture.runAsync(() -> Peer.joinAndAnswer(name), MEMBERS),
            CompletableFuture.runAsync(() -> Peer.joinAndAnswer(name), MEMBERS));
        for (CompletableFuture<Void> server : servers) {
            server.get(60, TimeUnit.SECONDS);
        }
        assertExited(peer, "the owner's client peer");
    }

    /**
     * A server in another JVM takes a request and holds it until it is killed: the client's receive here throws
     * ABANDONED once the owner's process (this one) has seen its end, and the next call gets its own reply.
     */
    @Test
    void aServerProcessKilledHoldingARequestAbandonsIt() throws Exception {
        String name = uniqueName("s_killed");
        try (Service _ = Fipc.createService(name, 4096, 64, 1, 2);
             Connection client = Fipc.joinService(name, ServiceRole.CLIENT, TEN_SECONDS)) {
            Process peer = TestSupport.startPeerReadingItsOutput("service-holding-server", name);
            try (BufferedReader output = new BufferedReader(new InputStreamReader(peer.getInputStream(), UTF_8))) {
                client.send(bytes("doomed"), TEN_SECONDS);
                assertEquals("holding", output.readLine());
                peer.destroyForcibly();
                assertTrue(peer.waitFor(60, TimeUnit.SECONDS));
                assertFipc(Result.ABANDONED, () -> client.receive(TEN_SECONDS));
                try (Connection server = Fipc.joinService(name, ServiceRole.SERVER, TEN_SECONDS)) {
                    CompletableFuture<Void> answering = answerAside(server);
                    assertEquals("NEXT", text(client.call(bytes("next"), TEN_SECONDS)));
                    answering.get(10, TimeUnit.SECONDS);
                }
            } finally {
                peer.destroyForcibly();
            }
        }
    }
}
