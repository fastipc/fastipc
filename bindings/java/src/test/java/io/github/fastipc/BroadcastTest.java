package io.github.fastipc;

import static io.github.fastipc.TestSupport.TEN_SECONDS;
import static io.github.fastipc.TestSupport.assertFipc;
import static io.github.fastipc.TestSupport.startPeer;
import static io.github.fastipc.TestSupport.uniqueName;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * The broadcast: a publisher and its lossy and gated subscribers in this process, the overrun's lost count, the
 * publisher's wait for members, an unsubscribed gated subscriber, the publisher's close freeing the name, the examples'
 * exchange in this process and as two processes (examples/java's files), and the publisher and its subscribers in other
 * JVMs ({@link Peer}). InteropTest runs the Python binding's publisher and subscribers against this binding.
 */
@Timeout(value = 120, unit = TimeUnit.SECONDS)
class BroadcastTest {
    private static byte[] bytes(String text) {
        return text.getBytes(UTF_8);
    }

    /** {@code length} bytes that start with {@code i}, little-endian. */
    private static byte[] numbered(long i, int length) {
        return ByteBuffer.allocate(length).order(ByteOrder.LITTLE_ENDIAN).putLong(0, i).array();
    }

    private static long number(byte[] message) {
        return ByteBuffer.wrap(message).order(ByteOrder.LITTLE_ENDIAN).getLong(0);
    }

    @Test
    void lossyAndGatedSubscribersReceiveEveryMessageThenDisconnected() {
        String name = uniqueName("bc_roles");
        Connection publisher = Fipc.createBroadcast(name, 1 << 16, 4, Fipc.FOREVER); // closed by the test
        try (Connection lossy = Fipc.joinBroadcast(name, SubscriberRole.LOSSY, TEN_SECONDS);
             Connection gated = Fipc.joinBroadcast(name, SubscriberRole.GATED, TEN_SECONDS)) {
            assertEquals(2, publisher.waitForMembers(2, TEN_SECONDS));
            assertEquals((1 << 16) / 4, publisher.maxPiece());
            assertEquals(publisher.maxPiece(), lossy.maxPiece());
            assertEquals(name, lossy.name());
            byte[][] words = {bytes("hello"), bytes("shared"), bytes("memory".repeat(1000))};
            for (byte[] word : words) {
                publisher.send(word, TEN_SECONDS);
            }
            for (byte[] word : words) {
                assertArrayEquals(word, lossy.receive(TEN_SECONDS));
            }
            ByteBuffer buffer = ByteBuffer.allocate(16);
            assertEquals(5, gated.receive(buffer, TEN_SECONDS));
            assertArrayEquals(bytes("hello"), Arrays.copyOf(buffer.array(), 5));
            MemorySegment held = gated.receiveAcquire(TEN_SECONDS); // in place: holds the publisher back until released
            assertTrue(held.isReadOnly());
            assertArrayEquals(bytes("shared"), held.toArray(ValueLayout.JAVA_BYTE));
            gated.receiveRelease();
            assertArrayEquals(words[2], gated.receive(TEN_SECONDS));

            // Zero-copy publishing; a broadcast's misuses
            MemorySegment room = publisher.sendAcquire(4, TEN_SECONDS);
            room.copyFrom(MemorySegment.ofArray(bytes("tick")));
            publisher.sendCommit(4);
            assertArrayEquals(bytes("tick"), lossy.receive(TEN_SECONDS));
            assertArrayEquals(bytes("tick"), gated.receive(TEN_SECONDS));
            assertFipc(Result.TIMEOUT, () -> lossy.receive(Fipc.NO_WAIT));
            assertFipc(Result.INVALID, () -> lossy.receiveAcquire(Fipc.NO_WAIT)); // a lossy subscriber copies
            assertFipc(Result.INVALID, () -> publisher.receive(Fipc.NO_WAIT));
            assertFipc(Result.INVALID, () -> gated.send(bytes("x"), Fipc.NO_WAIT));
            assertFipc(Result.INVALID, () -> lossy.waitForMembers()); // not the publisher
            assertFipc(Result.TOO_LARGE, () -> publisher.send(new byte[publisher.maxPiece() + 1], Fipc.NO_WAIT));

            // The publisher's close ends the broadcast: what it sent is received first
            publisher.send(bytes("last"), TEN_SECONDS);
            publisher.close();
            for (Connection subscriber : List.of(lossy, gated)) {
                assertArrayEquals(bytes("last"), subscriber.receive(TEN_SECONDS));
                assertFipc(Result.DISCONNECTED, () -> subscriber.receive(TEN_SECONDS));
            }
        } finally {
            publisher.close();
        }
    }

    /**
     * A subscriber joining a stream's name learns the kind once the stream's accept runs: the joiner refuses the stream
     * and gets INVALID, and the accept goes on waiting.
     */
    @Test
    void aSubscriberRefusesAStreamsName() throws Exception {
        String name = uniqueName("bc_stream");
        try (Listener listener = Fipc.createStream(name, 1 << 16)) {
            CompletableFuture<FipcException> accepting = CompletableFuture.supplyAsync(
                () -> assertFipc(Result.TIMEOUT, () -> listener.accept(Duration.ofSeconds(2))));
            assertFipc(Result.INVALID, () -> Fipc.joinBroadcast(name, SubscriberRole.LOSSY, TEN_SECONDS));
            accepting.get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void aLappedLossySubscriberGetsOverrunWithTheNumberLost() {
        String name = uniqueName("bc_overrun");
        try (Connection publisher = Fipc.createBroadcast(name, 4096, 1, Fipc.FOREVER);
             Connection subscriber = Fipc.joinBroadcast(name, SubscriberRole.LOSSY, TEN_SECONDS)) {
            assertEquals(1, publisher.waitForMembers(1, TEN_SECONDS));
            // 144 messages of 48 bytes lap the broadcast's 64 cells twice; the subscriber goes on from message 140
            // (docs/protocol.md §9.6), so 0 to 139 are lost
            for (int i = 0; i < 144; i++) {
                publisher.send(numbered(i, 48), Fipc.NO_WAIT);
            }
            FipcException overrun = assertFipc(Result.OVERRUN, () -> subscriber.receive(TEN_SECONDS));
            assertEquals(140, overrun.lostMessages());
            assertEquals(-1, overrun.messageLength());
            assertEquals(-1, overrun.memberCount());
            assertTrue(overrun.getMessage().contains("140 messages lost"), overrun.getMessage());
            assertEquals(140, number(subscriber.receive(TEN_SECONDS)));
            ByteBuffer buffer = ByteBuffer.allocate(64);
            for (int i = 141; i < 144; i++) {
                buffer.clear();
                assertEquals(48, subscriber.receive(buffer, TEN_SECONDS));
                assertEquals(i, number(buffer.array()));
            }
            assertEquals(140, overrun.lostMessages()); // the exception keeps its count after the receives that follow
            assertEquals(-1, assertFipc(Result.TIMEOUT, () -> subscriber.receive(Fipc.NO_WAIT)).lostMessages());
        }
    }

    /** The receives into the caller's memory report the overrun too: a native segment, as the library takes it. */
    @Test
    void anOverrunIntoANativeSegment() {
        String name = uniqueName("bc_overrun_native");
        try (Connection publisher = Fipc.createBroadcast(name, 4096, 1, Fipc.FOREVER);
             Connection subscriber = Fipc.joinBroadcast(name, SubscriberRole.LOSSY, TEN_SECONDS);
             Arena arena = Arena.ofConfined()) {
            assertEquals(1, publisher.waitForMembers(1, TEN_SECONDS));
            for (int i = 0; i < 144; i++) {
                publisher.send(numbered(i, 48), Fipc.NO_WAIT);
            }
            MemorySegment buffer = arena.allocate(64);
            assertEquals(140, assertFipc(Result.OVERRUN, () -> subscriber.receive(buffer, TEN_SECONDS)).lostMessages());
            assertEquals(48, subscriber.receive(buffer, TEN_SECONDS));
            assertEquals(140, buffer.get(ValueLayout.JAVA_LONG.withOrder(ByteOrder.LITTLE_ENDIAN), 0));
        }
    }

    @Test
    void thePublishersWaitForMembers() throws Exception {
        String name = uniqueName("bc_count");
        try (Connection publisher = Fipc.createBroadcast(name, 4096, 2, Fipc.FOREVER)) {
            assertEquals(0, publisher.waitForMembers());
            FipcException timeout = assertFipc(Result.TIMEOUT, () -> publisher.waitForMembers(1, Fipc.NO_WAIT));
            assertEquals(0, timeout.memberCount());
            assertEquals(-1, timeout.lostMessages());
            assertTrue(timeout.getMessage().contains("0 members joined"), timeout.getMessage());
            assertEquals("fipc_broadcast_wait_for_members", timeout.call());

            CompletableFuture<Integer> waiting =
                CompletableFuture.supplyAsync(() -> publisher.waitForMembers(1, TEN_SECONDS));
            try (Connection _ = Fipc.joinBroadcast(name, SubscriberRole.GATED, TEN_SECONDS)) {
                assertEquals(1, waiting.get(10, TimeUnit.SECONDS));
                assertEquals(1, publisher.waitForMembers());
                assertEquals(1, assertFipc(Result.TIMEOUT, () -> publisher.waitForMembers(2, Duration.ofMillis(50)))
                    .memberCount());

                CompletableFuture<Integer> cancelled =
                    CompletableFuture.supplyAsync(() -> publisher.waitForMembers(2, TEN_SECONDS));
                publisher.cancel(); // the wait, now or about to start, throws CANCELLED
                ExecutionException failed = assertThrows(ExecutionException.class,
                    () -> cancelled.get(10, TimeUnit.SECONDS));
                FipcException e = (FipcException) failed.getCause();
                assertEquals(Result.CANCELLED, e.result());
                assertEquals(1, e.memberCount());
            }
            assertEquals(-1, assertFipc(Result.INVALID, () -> publisher.waitForMembers(3, Fipc.NO_WAIT)).memberCount());
            assertThrows(IllegalArgumentException.class, () -> publisher.waitForMembers(-1, Fipc.NO_WAIT));
        }
    }

    @Test
    void aGatedSubscriberThatHoldsThePublisherBackIsUnsubscribed() {
        String name = uniqueName("bc_stall");
        try (Connection publisher = Fipc.createBroadcast(name, 4096, 1, Duration.ofMillis(100));
             Connection subscriber = Fipc.joinBroadcast(name, SubscriberRole.GATED, TEN_SECONDS)) {
            assertEquals(1, publisher.waitForMembers(1, TEN_SECONDS));
            // The 65th message waits for room the subscriber holds; 100 ms later it is unsubscribed, and the publisher
            // goes on
            for (int i = 0; i < 100; i++) {
                publisher.send(new byte[48], TEN_SECONDS);
            }
            assertFipc(Result.UNSUBSCRIBED, () -> subscriber.receive(Fipc.NO_WAIT));
            assertFipc(Result.UNSUBSCRIBED, () -> subscriber.receive(Fipc.NO_WAIT)); // final
        }
    }

    @Test
    void thePublishersCloseFreesTheNameAndCreateRefusesWhatIsOutOfRange() {
        String name = uniqueName("bc_name");
        assertFipc(Result.INVALID, () -> Fipc.createBroadcast(name, 1000, 1, Fipc.FOREVER));
        assertFipc(Result.INVALID, () -> Fipc.createBroadcast(name, 4096, 0, Fipc.FOREVER));
        assertFipc(Result.INVALID, () -> Fipc.createBroadcast(name, 4096, 1025, Fipc.FOREVER));
        assertThrows(NullPointerException.class, () -> Fipc.joinBroadcast(name, null, TEN_SECONDS));
        Connection publisher = Fipc.createBroadcast(name, 4096, 1, Fipc.NO_WAIT); // closed by the test
        try {
            assertFipc(Result.ADDR_IN_USE, () -> Fipc.createBroadcast(name, 4096, 1, Fipc.FOREVER));
            // NO_WAIT refuses gated subscribers
            assertFipc(Result.INVALID, () -> Fipc.joinBroadcast(name, SubscriberRole.GATED, TEN_SECONDS));
            for (QueueRole role : QueueRole.values()) { // a queue's roles, on a broadcast
                assertFipc(Result.INVALID, () -> Fipc.joinQueue(name, role, TEN_SECONDS));
            }
            try (Connection subscriber = Fipc.joinBroadcast(name, SubscriberRole.LOSSY, TEN_SECONDS)) {
                assertEquals(1, publisher.waitForMembers(1, TEN_SECONDS));
                publisher.close(); // ends the broadcast and frees the name at once
                assertFipc(Result.DISCONNECTED, () -> subscriber.receive(TEN_SECONDS));
                Fipc.createBroadcast(name, 4096, 1, Fipc.FOREVER).close();
            }
        } finally {
            publisher.close();
        }
    }

    /**
     * The examples' exchange in this process: the publisher creates the broadcast, waits for a subscriber, sends
     * tick 1 to tick 3 and closes; the subscriber receives the three ticks, then DISCONNECTED. Subscriber first (its
     * join waits for the broadcast) and subscriber last (it joins while the publisher waits for it).
     */
    @Test
    void theExamplesExchangeInEitherOrder() throws Exception {
        for (boolean subscriberFirst : new boolean[] {true, false}) {
            String name = uniqueName("bc_example");
            CompletableFuture<List<String>> received = null;
            if (subscriberFirst) {
                received = CompletableFuture.supplyAsync(() -> subscribeToTheEnd(name));
                Thread.sleep(200);
            }
            try (Connection publisher = Fipc.createBroadcast(name, 1 << 16, 8, Duration.ofSeconds(5))) {
                if (!subscriberFirst) {
                    received = CompletableFuture.supplyAsync(() -> subscribeToTheEnd(name));
                }
                assertEquals(1, publisher.waitForMembers(1));
                for (int i = 1; i <= 3; i++) {
                    publisher.send(bytes("tick " + i));
                }
            }
            assertEquals(List.of("tick 1", "tick 2", "tick 3"), received.get(30, TimeUnit.SECONDS));
        }
    }

    /** A gated subscriber of the broadcast on {@code name}: what it receives until DISCONNECTED. */
    private static List<String> subscribeToTheEnd(String name) {
        List<String> received = new ArrayList<>();
        try (Connection subscriber = Fipc.joinBroadcast(name, SubscriberRole.GATED, TEN_SECONDS)) {
            for (;;) {
                try {
                    received.add(new String(subscriber.receive(TEN_SECONDS), UTF_8));
                } catch (FipcException e) {
                    if (e.result() != Result.DISCONNECTED) {
                        throw e;
                    }
                    return received;
                }
            }
        }
    }

    /**
     * examples/java's Publisher.java and Subscriber.java, compiled here and each run in a JVM of its own, subscriber
     * first and publisher first: the subscriber prints tick 1 to tick 3, and both exit 0. The order is likely, not
     * certain, as in QueueTest's run of the producer and consumer examples.
     */
    @Test
    void theExampleProgramsInTwoProcessesInEitherOrder(@TempDir Path classes) throws Exception {
        TestSupport.runExamplePair(classes, "Publisher", "Subscriber", List.of("tick 1", "tick 2", "tick 3"), false);
    }

    /** A publisher in another JVM, a gated and a lossy subscriber here. */
    @Test
    void aPublisherInAnotherProcess() throws Exception {
        String name = uniqueName("bc_pub_peer");
        Process peer = startPeer("publisher", name, "300", "2");
        List<CompletableFuture<Void>> subscribers = new ArrayList<>();
        for (boolean gated : new boolean[] {true, false}) {
            subscribers.add(CompletableFuture.runAsync(() -> Peer.subscribe(name, gated, 300)));
        }
        for (CompletableFuture<Void> subscriber : subscribers) {
            subscriber.get(60, TimeUnit.SECONDS);
        }
        assertTrue(peer.waitFor(60, TimeUnit.SECONDS), "the publisher peer didn't exit");
        assertEquals(0, peer.exitValue(), "the publisher peer failed");
    }

    /** A publisher here, a gated and a lossy subscriber in other JVMs. */
    @Test
    void subscribersInOtherProcesses() throws Exception {
        String name = uniqueName("bc_sub_peer");
        List<Process> peers = List.of(startPeer("subscriber", name, "gated", "300"),
            startPeer("subscriber", name, "lossy", "300"));
        Peer.publish(name, 300, 2);
        for (Process peer : peers) {
            assertTrue(peer.waitFor(60, TimeUnit.SECONDS), "a subscriber peer didn't exit");
            assertEquals(0, peer.exitValue(), "a subscriber peer failed");
        }
    }
}
