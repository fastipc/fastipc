package io.github.fastipc;

import static io.github.fastipc.TestSupport.TEN_SECONDS;
import static io.github.fastipc.TestSupport.assertFipc;
import static io.github.fastipc.TestSupport.pattern;
import static io.github.fastipc.TestSupport.uniqueName;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Batches (sendMany, receiveMany): on a stream, in order and packed back to back, a buffer that stops a batch and a
 * first message too long for it, NO_WAIT's partial send with how many went, an empty message refused before any goes;
 * a broadcast's and a work queue's batches, and TOO_LARGE over maxPiece before any goes; a service client's batch of
 * requests, owed as its sends are, with their replies in order, and a server's batches refused.
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class BatchTest {
    private static byte[] bytes(String text) {
        return text.getBytes(UTF_8);
    }

    /** {@code count} messages, received in batches (a broadcast's subscriber may take fewer than are there). */
    private static List<byte[]> receiveAll(Connection conn, int count) {
        List<byte[]> received = new ArrayList<>();
        while (received.size() < count) {
            List<byte[]> batch = conn.receiveMany(16, TEN_SECONDS);
            assertTrue(batch.size() >= 1 && batch.size() <= 16, "batch of " + batch.size());
            received.addAll(batch);
        }
        return received;
    }

    private static void assertMessages(List<byte[]> expected, List<byte[]> actual) {
        assertEquals(expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++) {
            assertArrayEquals(expected.get(i), actual.get(i), "message " + i);
        }
    }

    @Test
    void streamBatchesComeInOrderPackedBackToBack() {
        try (TestSupport.Pair pair = new TestSupport.Pair(uniqueName("batch"), 1024)) {
            List<byte[]> messages = new ArrayList<>();
            for (int i = 0; i < 6; i++) {
                messages.add(pattern(10 + i * 20));
            }
            assertEquals(6, pair.client.sendMany(messages, TEN_SECONDS));
            assertMessages(messages, pair.server.receiveMany(16, TEN_SECONDS));

            // Into a segment: a buffer that stops the batch, and a first message too long for it, which stays queued
            assertEquals(6, pair.client.sendMany(messages));
            long[] lengths = new long[16];
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment buffer = arena.allocate(4096);
                assertEquals(2, pair.server.receiveMany(buffer.asSlice(0, 10 + 30), lengths, TEN_SECONDS));
                assertEquals(10, lengths[0]);
                assertEquals(30, lengths[1]);
                assertArrayEquals(messages.get(1), buffer.asSlice(10, 30).toArray(ValueLayout.JAVA_BYTE));
                FipcException tooLarge = assertFipc(Result.TOO_LARGE,
                    () -> pair.server.receiveMany(buffer.asSlice(0, 10), lengths, TEN_SECONDS));
                assertEquals(50, tooLarge.messageLength());
            }
            byte[] heap = new byte[4096];
            assertEquals(3, pair.server.receiveMany(MemorySegment.ofArray(heap), new long[3], TEN_SECONDS));
            assertMessages(messages.subList(5, 6), pair.server.receiveMany(16));

            // A message longer than one piece goes in pieces, and arrives whole
            byte[] longMessage = pattern(100_000);
            CompletableFuture<List<byte[]>> receiving = CompletableFuture.supplyAsync(() -> receiveAll(pair.server, 3));
            List<byte[]> withLong = List.of(messages.get(0), longMessage, messages.get(1));
            assertEquals(3, pair.client.sendMany(withLong, TEN_SECONDS));
            assertMessages(withLong, receiving.join());
            assertFipc(Result.TIMEOUT, () -> pair.server.receiveMany(16, Fipc.NO_WAIT));
        }
    }

    @Test
    void partialSendReportsHowManyWent() {
        try (TestSupport.Pair pair = new TestSupport.Pair(uniqueName("partial"), 1024)) {
            byte[] hundred = pattern(100);
            List<byte[]> many = Collections.nCopies(100, hundred);
            // Frames of 128 bytes: the ring of 1 KiB holds 8, fewer where its end is padded
            FipcException partial = assertFipc(Result.TIMEOUT, () -> pair.client.sendMany(many, Fipc.NO_WAIT));
            assertTrue(partial.sentMessages() >= 1 && partial.sentMessages() <= 8, partial.getMessage());
            assertMessages(Collections.nCopies(partial.sentMessages(), hundred),
                receiveAll(pair.server, partial.sentMessages()));

            CompletableFuture<List<byte[]>> receiving = CompletableFuture.supplyAsync(() -> receiveAll(pair.server, 100));
            assertEquals(100, pair.client.sendMany(many, TEN_SECONDS)); // sent while it is received
            assertMessages(many, receiving.join());

            FipcException empty = assertFipc(Result.INVALID,
                () -> pair.client.sendMany(List.of(hundred, new byte[0]), Fipc.NO_WAIT));
            assertEquals(0, empty.sentMessages());
            assertFipc(Result.INVALID, () -> pair.client.sendMany(List.of(), Fipc.NO_WAIT));
            assertFipc(Result.INVALID, () -> pair.server.receiveMany(0, Fipc.NO_WAIT));
            assertFipc(Result.TIMEOUT, () -> pair.server.receiveMany(16, Fipc.NO_WAIT)); // nothing went
        }
    }

    @Test
    void broadcastBatchesReachEverySubscriber() {
        String name = uniqueName("bc_batch");
        Connection publisher = Fipc.createBroadcast(name, 1 << 16, 4, Fipc.FOREVER);
        try (Connection lossy = Fipc.joinBroadcast(name, SubscriberRole.LOSSY, TEN_SECONDS);
             Connection gated = Fipc.joinBroadcast(name, SubscriberRole.GATED, TEN_SECONDS)) {
            assertEquals(2, publisher.waitForMembers(2, TEN_SECONDS));
            List<byte[]> ticks = IntStream.range(0, 40).mapToObj(i -> bytes("tick " + i)).toList();
            assertEquals(40, publisher.sendMany(ticks, TEN_SECONDS));
            FipcException tooLarge = assertFipc(Result.TOO_LARGE,
                () -> publisher.sendMany(List.of(ticks.get(0), new byte[publisher.maxPiece() + 1]), Fipc.NO_WAIT));
            assertEquals(0, tooLarge.sentMessages());
            assertFipc(Result.INVALID, () -> publisher.receiveMany(16, Fipc.NO_WAIT));
            assertFipc(Result.INVALID, () -> gated.sendMany(ticks, Fipc.NO_WAIT));
            publisher.close(); // what it sent is received first
            for (Connection subscriber : List.of(lossy, gated)) {
                assertMessages(ticks, receiveAll(subscriber, 40));
                assertFipc(Result.DISCONNECTED, () -> subscriber.receiveMany(16, TEN_SECONDS));
            }
        } finally {
            publisher.close();
        }
    }

    @Test
    void queueBatchesDeliverEveryMessageOnce() {
        String name = uniqueName("q_batch");
        WorkQueue queue = Fipc.createQueue(name, 4096, 1, 1);
        try (Connection producer = Fipc.joinQueue(name, QueueRole.PRODUCER, TEN_SECONDS);
             Connection consumer = Fipc.joinQueue(name, QueueRole.CONSUMER, TEN_SECONDS)) {
            List<byte[]> jobs = IntStream.range(0, 200).mapToObj(i -> bytes("job " + i)).toList();
            CompletableFuture<List<byte[]>> receiving = CompletableFuture.supplyAsync(() -> receiveAll(consumer, 200));
            for (int at = 0; at < 200; at += 50) {
                assertEquals(50, producer.sendMany(jobs.subList(at, at + 50), TEN_SECONDS));
            }
            assertMessages(jobs, receiving.join()); // one producer: its messages in order
            assertFipc(Result.TOO_LARGE,
                () -> producer.sendMany(List.of(jobs.get(0), new byte[producer.maxPiece() + 1]), Fipc.NO_WAIT));
            assertFipc(Result.INVALID, () -> consumer.sendMany(jobs, Fipc.NO_WAIT));
            assertFipc(Result.INVALID, () -> producer.receiveMany(16, Fipc.NO_WAIT));
            queue.close();
            assertFipc(Result.DISCONNECTED, () -> consumer.receiveMany(16, TEN_SECONDS));
            assertFipc(Result.DISCONNECTED, () -> producer.sendMany(jobs, TEN_SECONDS));
        } finally {
            queue.close();
        }
    }

    @Test
    void serviceClientsBatchIsOwedAsItsSendsAre() {
        String name = uniqueName("s_batch");
        try (Service service = Fipc.createService(name, 1 << 14, 64, 1, 1);
             Connection server = Fipc.joinService(name, ServiceRole.SERVER, TEN_SECONDS);
             Connection client = Fipc.joinService(name, ServiceRole.CLIENT, TEN_SECONDS)) {
            assertEquals(1, service.waitForMembers(ServiceRole.CLIENT, 1, TEN_SECONDS));
            List<byte[]> requests = List.of(bytes("a"), bytes("bb"), bytes("ccc"));
            assertEquals(3, client.sendMany(requests, TEN_SECONDS));
            assertFipc(Result.INVALID, () -> client.call(bytes("d"), Fipc.NO_WAIT)); // those replies are the caller's
            for (byte[] request : requests) {
                byte[] got = server.receive(TEN_SECONDS);
                assertArrayEquals(request, got);
                server.send(bytes(new String(got, UTF_8) + " done"), Fipc.NO_WAIT);
            }
            assertMessages(List.of(bytes("a done"), bytes("bb done"), bytes("ccc done")), receiveAll(client, 3));

            CompletableFuture<Void> answering = CompletableFuture.runAsync(() -> {
                byte[] got = server.receive(TEN_SECONDS);
                server.send(bytes(new String(got, UTF_8) + " done"), Fipc.NO_WAIT);
            });
            assertArrayEquals(bytes("e done"), client.call(bytes("e"), TEN_SECONDS)); // nothing owed now
            answering.join();
            assertFipc(Result.INVALID, () -> server.sendMany(requests, Fipc.NO_WAIT));
            assertFipc(Result.INVALID, () -> server.receiveMany(16, Fipc.NO_WAIT));

            // A batch whose requests don't all fit counts only those that went: once their replies are received, a
            // call goes again. Each request holds 64 + 64 bytes of the 16 KiB ring: at most 128 fit
            FipcException partial = assertFipc(Result.TIMEOUT,
                () -> client.sendMany(Collections.nCopies(200, bytes("f")), Fipc.NO_WAIT));
            int sent = partial.sentMessages();
            assertTrue(sent >= 1 && sent <= 128, partial.getMessage());
            for (int i = 0; i < sent; i++) {
                server.send(server.receive(TEN_SECONDS), Fipc.NO_WAIT);
            }
            assertEquals(sent, receiveAll(client, sent).size());
            answering = CompletableFuture.runAsync(() -> {
                byte[] got = server.receive(TEN_SECONDS);
                server.send(bytes(new String(got, UTF_8) + " done"), Fipc.NO_WAIT);
            });
            assertArrayEquals(bytes("g done"), client.call(bytes("g"), TEN_SECONDS));
            answering.join();
        }
    }
}
