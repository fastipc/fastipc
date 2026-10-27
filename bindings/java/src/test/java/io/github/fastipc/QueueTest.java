package io.github.fastipc;

import static io.github.fastipc.TestSupport.FIVE_SECONDS;
import static io.github.fastipc.TestSupport.TEN_SECONDS;
import static io.github.fastipc.TestSupport.assertFipc;
import static io.github.fastipc.TestSupport.startPeer;
import static io.github.fastipc.TestSupport.uniqueName;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.ref.WeakReference;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * The work queue: producers and consumers in this process, with every message delivered once, the owner's members,
 * the calls a queue's roles refuse, the owner's wait for members and its cancel, the end at the queue's close, a queue
 * the garbage collector finds unreachable (it lives on), two consumers racing for messages longer than the receive's
 * buffer, the examples' exchange in this process and as two processes (examples/java's files), and producers and
 * consumers in other JVMs ({@link Peer}). InteropTest runs the Python binding's producers and consumers against this
 * binding.
 */
@Timeout(value = 120, unit = TimeUnit.SECONDS)
class QueueTest {
    /**
     * A thread for each member that waits: the common pool has one thread fewer than the CPUs, so on 4 CPUs three
     * consumers waiting in their receives would hold every thread and leave no producer one.
     */
    private static final ExecutorService MEMBERS = Executors.newCachedThreadPool();

    @AfterAll
    static void stopMembers() {
        MEMBERS.shutdownNow();
    }

    private static byte[] bytes(String text) {
        return text.getBytes(UTF_8);
    }

    /** Joins a member of each role to the queue on {@code name}: the owner's process admits each one by itself. */
    private static List<Connection> joinAll(String name, QueueRole... roles) {
        List<Connection> members = new ArrayList<>();
        for (QueueRole role : roles) {
            members.add(Fipc.joinQueue(name, role, TEN_SECONDS));
        }
        return members;
    }

    /** Every message the consumer takes until DISCONNECTED: copied (way 0), into a buffer (1) or in place (2). */
    private static List<byte[]> receiveUntilTheEnd(Connection consumer, int way) {
        List<byte[]> received = new ArrayList<>();
        ByteBuffer buffer = ByteBuffer.allocate(consumer.maxPiece());
        for (;;) {
            try {
                switch (way) {
                    case 0 -> received.add(consumer.receive(TEN_SECONDS));
                    case 1 -> {
                        buffer.clear();
                        int length = consumer.receive(buffer, TEN_SECONDS);
                        received.add(Arrays.copyOf(buffer.array(), length));
                    }
                    default -> {
                        received.add(consumer.receiveAcquire(TEN_SECONDS).toArray(ValueLayout.JAVA_BYTE));
                        consumer.receiveRelease();
                    }
                }
            } catch (FipcException e) {
                if (e.result() != Result.DISCONNECTED) {
                    throw e;
                }
                return received;
            }
        }
    }

    @Test
    void fourProducersAndFourConsumersDeliverEveryMessageOnce() throws Exception {
        String name = uniqueName("q_many");
        int count = 500;
        List<Connection> producers;
        List<Connection> consumers;
        List<CompletableFuture<List<byte[]>>> taken = new ArrayList<>();
        try (WorkQueue queue = Fipc.createQueue(name, 4096, 4, 4)) {
            producers = joinAll(name, QueueRole.PRODUCER, QueueRole.PRODUCER, QueueRole.PRODUCER, QueueRole.PRODUCER);
            consumers = joinAll(name, QueueRole.CONSUMER, QueueRole.CONSUMER, QueueRole.CONSUMER, QueueRole.CONSUMER);
            assertEquals(8, queue.waitForMembers(8, TEN_SECONDS));
            for (Connection member : producers) {
                assertEquals(1024, member.maxPiece());
            }
            for (int k = 0; k < consumers.size(); k++) {
                Connection consumer = consumers.get(k);
                int way = k % 3;
                taken.add(CompletableFuture.supplyAsync(() -> receiveUntilTheEnd(consumer, way), MEMBERS));
            }
            List<CompletableFuture<Void>> producing = new ArrayList<>();
            for (int k = 0; k < producers.size(); k++) {
                Connection producer = producers.get(k);
                int number = k;
                producing.add(CompletableFuture.runAsync(() -> {
                    try (producer) { // its close leaves the queue; its messages stay
                        for (int i = 0; i < count; i++) {
                            producer.send(bytes(number + " " + i), TEN_SECONDS);
                        }
                    }
                }, MEMBERS));
            }
            for (CompletableFuture<Void> producer : producing) {
                producer.get(60, TimeUnit.SECONDS);
            }
        } // the queue's close ends it: the consumers take what is left, then DISCONNECTED
        boolean[][] seen = new boolean[4][count];
        for (CompletableFuture<List<byte[]>> consumer : taken) {
            for (byte[] message : consumer.get(60, TimeUnit.SECONDS)) {
                String[] parts = new String(message, UTF_8).split(" ");
                int k = Integer.parseInt(parts[0]);
                int i = Integer.parseInt(parts[1]);
                assertFalse(seen[k][i], "message " + k + " " + i + " delivered twice");
                seen[k][i] = true;
            }
        }
        for (Connection consumer : consumers) {
            consumer.close();
        }
        for (int k = 0; k < 4; k++) {
            for (int i = 0; i < count; i++) {
                assertTrue(seen[k][i], "message " + k + " " + i + " lost");
            }
        }
    }

    /**
     * The owner joins its queue as a producer and as a consumer, from this thread. One message at a time, so that no
     * check depends on an order the queue doesn't promise.
     */
    @Test
    void theOwnersProducerAndConsumerAndTheCallsTheirRolesRefuse() {
        String name = uniqueName("q_roles");
        try (WorkQueue queue = Fipc.createQueue(name, 4096, 1, 1);
             Connection consumer = Fipc.joinQueue(name, QueueRole.CONSUMER, TEN_SECONDS)) {
            Connection producer = Fipc.joinQueue(name, QueueRole.PRODUCER, TEN_SECONDS); // closed by the test
            assertEquals(name, queue.name());
            assertEquals(1024, producer.maxPiece());
            assertEquals(1024, consumer.maxPiece());
            assertEquals(name, consumer.name());
            assertEquals(name, producer.name());

            // A message waits in the queue; the consumer takes it copied, into a buffer, into a segment or in place
            producer.send(bytes("one"), TEN_SECONDS);
            assertArrayEquals(bytes("one"), consumer.receive(TEN_SECONDS));
            producer.send(bytes("two"), TEN_SECONDS);
            FipcException tooLarge = assertFipc(Result.TOO_LARGE, () -> consumer.receive(ByteBuffer.allocate(2),
                TEN_SECONDS)); // takes nothing
            assertEquals(3, tooLarge.messageLength());
            ByteBuffer buffer = ByteBuffer.allocate(16);
            assertEquals(3, consumer.receive(buffer, TEN_SECONDS));
            assertArrayEquals(bytes("two"), Arrays.copyOf(buffer.array(), 3));
            producer.send(bytes("three"), TEN_SECONDS);
            MemorySegment held = consumer.receiveAcquire(TEN_SECONDS);
            assertTrue(held.isReadOnly());
            assertArrayEquals(bytes("three"), held.toArray(ValueLayout.JAVA_BYTE));
            consumer.receiveRelease();
            producer.send(bytes("four"), TEN_SECONDS);
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment target = arena.allocate(16);
                assertEquals(4, consumer.receive(target, TEN_SECONDS));
                assertArrayEquals(bytes("four"), target.asSlice(0, 4).toArray(ValueLayout.JAVA_BYTE));
            }
            MemorySegment room = producer.sendAcquire(4, TEN_SECONDS);
            room.copyFrom(MemorySegment.ofArray(bytes("five")));
            producer.sendCommit(4);
            assertArrayEquals(bytes("five"), consumer.receive(TEN_SECONDS));

            assertFipc(Result.INVALID, () -> producer.receive(Fipc.NO_WAIT));
            assertFipc(Result.INVALID, () -> producer.receiveAcquire(Fipc.NO_WAIT));
            assertFipc(Result.INVALID, () -> consumer.send(bytes("x"), Fipc.NO_WAIT));
            assertFipc(Result.INVALID, () -> consumer.sendAcquire(1, Fipc.NO_WAIT));
            assertFipc(Result.INVALID, () -> producer.waitForMembers()); // a broadcast publisher's call
            assertFipc(Result.TOO_LARGE, () -> producer.send(new byte[producer.maxPiece() + 1], Fipc.NO_WAIT));
            assertFipc(Result.ADDR_IN_USE, () -> Fipc.createQueue(name, 4096, 1, 1));
            assertFipc(Result.ADDR_IN_USE, () -> Fipc.createStream(name, 4096));
            assertFipc(Result.ADDR_IN_USE, () -> Fipc.createBroadcast(name, 4096, 1, Fipc.FOREVER));

            // A producer's close doesn't end the queue: the consumer waits for more
            producer.send(bytes("last"), TEN_SECONDS);
            producer.close();
            assertArrayEquals(bytes("last"), consumer.receive(TEN_SECONDS));
            assertFipc(Result.TIMEOUT, () -> consumer.receive(Fipc.NO_WAIT));
        }
    }

    @Test
    void theOwnersWaitForMembersAndItsCancel() throws Exception {
        String name = uniqueName("q_members");
        try (WorkQueue queue = Fipc.createQueue(name, 4096, 2, 3)) {
            assertEquals(0, queue.waitForMembers(0));
            assertEquals(0, queue.waitForMembers(QueueRole.PRODUCER, 0));
            assertEquals(0, queue.waitForMembers(QueueRole.CONSUMER, 0, Fipc.NO_WAIT));
            FipcException timeout = assertFipc(Result.TIMEOUT, () -> queue.waitForMembers(1, Fipc.NO_WAIT));
            assertEquals(0, timeout.memberCount());
            assertEquals(-1, timeout.lostMessages());
            assertTrue(timeout.getMessage().contains("0 members joined"), timeout.getMessage());
            assertEquals("fipc_queue_wait_for_members", timeout.call());

            // On another thread, while one joins
            CompletableFuture<Integer> waiting =
                CompletableFuture.supplyAsync(() -> queue.waitForMembers(QueueRole.CONSUMER, 1, TEN_SECONDS));
            try (Connection _ = Fipc.joinQueue(name, QueueRole.CONSUMER, TEN_SECONDS);
                 Connection _ = Fipc.joinQueue(name, QueueRole.PRODUCER, TEN_SECONDS)) {
                assertTrue(waiting.get(10, TimeUnit.SECONDS) >= 1);
                assertEquals(2, queue.waitForMembers(2, TEN_SECONDS));
                assertEquals(1, queue.waitForMembers(QueueRole.PRODUCER, 0));
                assertEquals(1, queue.waitForMembers(QueueRole.CONSUMER, 0));
                assertEquals(1, assertFipc(Result.TIMEOUT,
                    () -> queue.waitForMembers(QueueRole.PRODUCER, 2, Duration.ofMillis(50))).memberCount());

                // Over the role's maximum (maxProducers, maxConsumers, their sum for both)
                assertEquals(-1, assertFipc(Result.INVALID,
                    () -> queue.waitForMembers(QueueRole.PRODUCER, 3, Fipc.NO_WAIT)).memberCount());
                assertFipc(Result.INVALID, () -> queue.waitForMembers(QueueRole.CONSUMER, 4, Fipc.NO_WAIT));
                assertFipc(Result.INVALID, () -> queue.waitForMembers(6, Fipc.NO_WAIT));
                assertThrows(IllegalArgumentException.class, () -> queue.waitForMembers(-1, Fipc.NO_WAIT));
                assertThrows(NullPointerException.class, () -> queue.waitForMembers(null, 1, Fipc.NO_WAIT));

                CompletableFuture<Integer> cancelled =
                    CompletableFuture.supplyAsync(() -> queue.waitForMembers(QueueRole.CONSUMER, 2, TEN_SECONDS));
                queue.cancel(); // from this thread: the wait, now or about to start, throws CANCELLED
                ExecutionException failed = assertThrows(ExecutionException.class,
                    () -> cancelled.get(10, TimeUnit.SECONDS));
                FipcException e = (FipcException) failed.getCause();
                assertEquals(Result.CANCELLED, e.result());
                assertEquals(1, e.memberCount());
                assertEquals(2, assertFipc(Result.CANCELLED, () -> queue.waitForMembers(3, TEN_SECONDS))
                    .memberCount()); // final

                // The queue goes on after its cancel: members join, and a message goes through
                try (Connection producer = Fipc.joinQueue(name, QueueRole.PRODUCER, TEN_SECONDS);
                     Connection consumer = Fipc.joinQueue(name, QueueRole.CONSUMER, TEN_SECONDS)) {
                    producer.send(bytes("after the cancel"), TEN_SECONDS);
                    assertArrayEquals(bytes("after the cancel"), consumer.receive(TEN_SECONDS));
                }
            }
        }
    }

    @Test
    void theQueuesCloseEndsItForProducersAndConsumers() {
        String name = uniqueName("q_end");
        WorkQueue queue = Fipc.createQueue(name, 4096, 1, 1);
        try (Connection producer = Fipc.joinQueue(name, QueueRole.PRODUCER, TEN_SECONDS);
             Connection consumer = Fipc.joinQueue(name, QueueRole.CONSUMER, TEN_SECONDS)) {
            producer.send(bytes("left"), TEN_SECONDS);
            queue.close();
            queue.close(); // a second close does nothing
            assertThrows(IllegalStateException.class, () -> queue.waitForMembers(0));
            queue.cancel(); // a no-op once closed
            assertFipc(Result.DISCONNECTED, () -> producer.send(bytes("after"), TEN_SECONDS));
            assertArrayEquals(bytes("left"), consumer.receive(TEN_SECONDS));
            assertFipc(Result.DISCONNECTED, () -> consumer.receive(TEN_SECONDS));
            Fipc.createQueue(name, 4096, 1, 1).close(); // the name is free again at once
        }
    }

    /**
     * A queue has no cleaner: one created in a method that keeps only a joined producer goes on after the garbage
     * collector has collected its object, and holds its name until the end of the process (this test's JVM).
     */
    @Test
    void aQueueTheGarbageCollectorCollectsLivesOn() throws Exception {
        String name = uniqueName("q_gc");
        List<WeakReference<WorkQueue>> queues = new ArrayList<>();
        try (Connection producer = startQueue(name, queues)) {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (queues.getFirst().get() != null) {
                assertTrue(System.nanoTime() < deadline, "the garbage collector didn't collect the queue's object");
                System.gc();
                Thread.sleep(10);
            }
            producer.send(bytes("still here"), TEN_SECONDS);
            try (Connection consumer = Fipc.joinQueue(name, QueueRole.CONSUMER, TEN_SECONDS)) {
                assertArrayEquals(bytes("still here"), consumer.receive(TEN_SECONDS));
            }
            assertFipc(Result.ADDR_IN_USE, () -> Fipc.createQueue(name, 4096, 1, 1));
        }
    }

    /** Creates a queue on {@code name}, keeps it only in a weak reference in {@code queues}, and joins a producer. */
    private static Connection startQueue(String name, List<WeakReference<WorkQueue>> queues) {
        WorkQueue queue = Fipc.createQueue(name, 4096, 1, 1);
        queues.add(new WeakReference<>(queue));
        return Fipc.joinQueue(name, QueueRole.PRODUCER, TEN_SECONDS);
    }

    @Test
    void outOfRangeArgumentsAndJoinersOfTheOtherKindAreRefused() {
        String name = uniqueName("q_range");
        long[][] invalid = {{1000, 1, 1}, {4096, 0, 1}, {4096, 65, 1}, {4096, 1, 0}, {4096, 1, 65}};
        for (long[] args : invalid) {
            assertFipc(Result.INVALID, () -> Fipc.createQueue(name, args[0], (int) args[1], (int) args[2]));
        }
        assertThrows(NullPointerException.class, () -> Fipc.createQueue(null, 4096, 1, 1));
        assertThrows(NullPointerException.class, () -> Fipc.joinQueue(name, null, FIVE_SECONDS));
        try (WorkQueue _ = Fipc.createQueue(name, 4096, 1, 1)) { // a broadcast's roles, on a queue
            assertFipc(Result.INVALID, () -> Fipc.joinBroadcast(name, SubscriberRole.LOSSY, FIVE_SECONDS));
            assertFipc(Result.INVALID, () -> Fipc.joinBroadcast(name, SubscriberRole.GATED, FIVE_SECONDS));
            assertFipc(Result.INVALID, () -> Fipc.connectStream(name, FIVE_SECONDS));
        }
        try (Connection _ = Fipc.createBroadcast(name, 4096, 1, Fipc.FOREVER)) { // a queue's roles, on a broadcast
            assertFipc(Result.INVALID, () -> Fipc.joinQueue(name, QueueRole.PRODUCER, FIVE_SECONDS));
            assertFipc(Result.INVALID, () -> Fipc.joinQueue(name, QueueRole.CONSUMER, FIVE_SECONDS));
        }
    }

    /**
     * Two consumers race with the copying receives for messages longer than the native buffer they receive into first
     * (the connection's, which starts at 4 KiB and grows to at most 1 MiB, so a message over 1 MiB always overflows
     * it): each one that finds a message too long for it receives again into a longer buffer, where another consumer
     * may have taken that message first; it then receives the next one, which may be longer still. Every message
     * arrives once, whole.
     */
    @Test
    void twoConsumersRaceForMessagesLongerThanTheReceivesBuffer() throws Exception {
        String name = uniqueName("q_race");
        int count = 300;
        List<CompletableFuture<List<Integer>>> taken = new ArrayList<>();
        List<Connection> consumers;
        try (WorkQueue _ = Fipc.createQueue(name, 8 << 20, 1, 2);
             Connection producer = Fipc.joinQueue(name, QueueRole.PRODUCER, TEN_SECONDS)) {
            consumers = joinAll(name, QueueRole.CONSUMER, QueueRole.CONSUMER);
            assertEquals(2 << 20, consumers.getFirst().maxPiece());
            for (int k = 0; k < 2; k++) {
                Connection consumer = consumers.get(k);
                boolean intoSegment = k == 1;
                taken.add(CompletableFuture.supplyAsync(() -> {
                    List<Integer> received = new ArrayList<>(); // the messages' indexes, each checked whole
                    MemorySegment buffer = MemorySegment.ofArray(new byte[consumer.maxPiece()]); // heap: copied through
                    for (;;) {
                        try {
                            if (intoSegment) {
                                long length = consumer.receive(buffer, TEN_SECONDS);
                                received.add(checkIndexed(buffer.asSlice(0, length).toArray(ValueLayout.JAVA_BYTE),
                                    count));
                            } else {
                                received.add(checkIndexed(consumer.receive(TEN_SECONDS), count));
                            }
                        } catch (FipcException e) {
                            if (e.result() != Result.DISCONNECTED) {
                                throw e;
                            }
                            return received;
                        }
                    }
                }));
            }
            for (int i = 0; i < count; i++) {
                producer.send(indexed(i), TEN_SECONDS);
            }
        } // the queue's close ends it: the consumers take what is left, then DISCONNECTED
        boolean[] seen = new boolean[count];
        int received = 0;
        for (CompletableFuture<List<Integer>> consumer : taken) {
            for (int i : consumer.get(60, TimeUnit.SECONDS)) {
                assertFalse(seen[i], "message " + i + " arrived twice");
                seen[i] = true;
                received++;
            }
        }
        for (Connection consumer : consumers) {
            consumer.close();
        }
        assertEquals(count, received);
    }

    /**
     * Message {@code i} of the race: 100 bytes, 1.5 MiB or 2 MiB (the longer ones past the copying receives' native
     * buffer, at most 1 MiB), that start with {@code i}, little-endian, followed by a pattern of {@code i}.
     */
    private static byte[] indexed(int i) {
        byte[] message = new byte[new int[] {100, 1536 << 10, 2 << 20}[i % 3]];
        ByteBuffer.wrap(message).order(ByteOrder.LITTLE_ENDIAN).putInt(0, i);
        for (int j = 4; j < message.length; j++) {
            message[j] = (byte) (i + j);
        }
        return message;
    }

    /** {@return the index of {@code message}, one of the race's {@code count} messages, which must be whole} */
    private static int checkIndexed(byte[] message, int count) {
        int i = ByteBuffer.wrap(message).order(ByteOrder.LITTLE_ENDIAN).getInt(0);
        assertTrue(i >= 0 && i < count, "message " + i);
        assertArrayEquals(indexed(i), message, "message " + i);
        return i;
    }

    /**
     * The examples' exchange in this process: the producer creates the queue, joins it, sends job 1 to job 3, waits
     * for a consumer and closes its producer and the queue; the consumer receives the three jobs, then DISCONNECTED.
     * Consumer first (its join waits for the queue) and consumer last (it joins after the jobs were sent).
     */
    @Test
    void theExamplesExchangeInEitherOrder() throws Exception {
        for (boolean consumerFirst : new boolean[] {true, false}) {
            String name = uniqueName("q_example");
            CompletableFuture<Void> consumed = null;
            if (consumerFirst) {
                consumed = CompletableFuture.runAsync(() -> Peer.consume(name, 3));
                Thread.sleep(200);
            }
            try (WorkQueue queue = Fipc.createQueue(name, 4096, 1, 1);
                 Connection producer = Fipc.joinQueue(name, QueueRole.PRODUCER)) {
                for (int i = 1; i <= 3; i++) {
                    producer.send(Peer.job(i));
                }
                if (!consumerFirst) {
                    consumed = CompletableFuture.runAsync(() -> Peer.consume(name, 3));
                }
                assertEquals(1, queue.waitForMembers(QueueRole.CONSUMER, 1, Fipc.FOREVER));
            }
            consumed.get(30, TimeUnit.SECONDS);
        }
    }

    /**
     * examples/java's Producer.java and Consumer.java, compiled here and each run in a JVM of its own, consumer first
     * and producer first: the consumer prints job 1 to job 3, in any order, and both exit 0.
     *
     * <p>The start order is likely, not certain: the second process starts after a fixed pause, since nothing outside
     * the processes shows that the consumer waits in its join or that the producer has created the queue. The result
     * holds either way as long as the producer creates the queue within the consumer's 5 s join:
     * <ul>
     *   <li>Consumer first: the producer starts 0.5 s later and takes about as long again to create the queue
     *   (compiled classes, no source launcher), which leaves the join seconds of slack.</li>
     *   <li>Producer first: the consumer starts 2 s later and waits up to 5 s more.</li>
     * </ul>
     */
    @Test
    void theExampleProgramsInTwoProcessesInEitherOrder(@TempDir Path classes) throws Exception {
        TestSupport.runExamplePair(classes, "Producer", "Consumer", List.of("job 1", "job 2", "job 3"), true);
    }

    /** A producer (the queue's owner's) in another JVM, a consumer here. */
    @Test
    void aProducerInAnotherProcessAndAConsumerHere() throws Exception {
        String name = uniqueName("q_producer_peer");
        Process peer = startPeer("producer", name, "300", "1");
        Peer.consume(name, 300);
        assertTrue(peer.waitFor(60, TimeUnit.SECONDS), "the producer peer didn't exit");
        assertEquals(0, peer.exitValue(), "the producer peer failed");
    }

    /** A producer (the queue's owner's) here, a consumer in another JVM. */
    @Test
    void aProducerHereAndAConsumerInAnotherProcess() throws Exception {
        String name = uniqueName("q_consumer_peer");
        Process peer = startPeer("consumer", name, "300");
        Peer.produce(name, 300, 1);
        assertTrue(peer.waitFor(60, TimeUnit.SECONDS), "the consumer peer didn't exit");
        assertEquals(0, peer.exitValue(), "the consumer peer failed");
    }

    /** Two joined producers in other JVMs, the owner's consumer here. */
    @Test
    void joinedProducersInOtherProcessesAndTheOwnersConsumerHere() throws Exception {
        String name = uniqueName("q_joined_peers");
        List<Process> peers = List.of(startPeer("joined-producer", name, "300"),
            startPeer("joined-producer", name, "300"));
        Peer.ownAndConsume(name, 300, 2);
        for (Process peer : peers) {
            assertTrue(peer.waitFor(60, TimeUnit.SECONDS), "a producer peer didn't exit");
            assertEquals(0, peer.exitValue(), "a producer peer failed");
        }
    }
}
