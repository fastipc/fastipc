package io.github.fastipc;

import static io.github.fastipc.TestSupport.FIVE_SECONDS;
import static io.github.fastipc.TestSupport.TEN_SECONDS;
import static io.github.fastipc.TestSupport.assertFipc;
import static io.github.fastipc.TestSupport.connectAside;
import static io.github.fastipc.TestSupport.pattern;
import static io.github.fastipc.TestSupport.uniqueName;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Timeouts, cancel and close, and the errors the library reports. */
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class LifetimeTest {
    private static final ExecutorService WAITERS = Executors.newCachedThreadPool();

    @AfterAll
    static void stopWaiters() {
        WAITERS.shutdownNow();
    }

    /** What {@code call}, run on another thread, threw: it must throw a FipcException. */
    private static <T> CompletableFuture<FipcException> inBackground(Supplier<T> call) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                T value = call.get();
                throw new AssertionError("expected a FipcException, got " + value);
            } catch (FipcException e) {
                return e;
            }
        }, WAITERS);
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            throw new AssertionError(e);
        }
    }

    @Test
    void timeoutsWaitAndChangeNothing() {
        try (TestSupport.Pair pair = new TestSupport.Pair("timeouts")) {
            assertFipc(Result.TIMEOUT, () -> pair.server.receive(Fipc.NO_WAIT));
            long start = System.nanoTime();
            assertFipc(Result.TIMEOUT, () -> pair.server.receive(Duration.ofMillis(100)));
            assertFipc(Result.TIMEOUT, () -> pair.server.receiveAcquire(Duration.ofMillis(100)));
            assertFipc(Result.TIMEOUT, () -> pair.server.rpcReceive(Duration.ofMillis(100)));
            long elapsedMs = (System.nanoTime() - start) / 1_000_000;
            assertTrue(elapsedMs >= 280, "three 100 ms timeouts took " + elapsedMs + " ms");

            // A sub-millisecond timeout waits a whole millisecond, and still times out
            assertFipc(Result.TIMEOUT, () -> pair.server.receive(Duration.ofNanos(1)));
            try (Listener idle = Listener.listen(uniqueName("idle"), 4096)) {
                assertFipc(Result.TIMEOUT, () -> idle.accept(Duration.ofMillis(50)));
            }
            assertThrows(IllegalArgumentException.class, () -> pair.server.receive(Duration.ofMillis(-1)));
            assertThrows(NullPointerException.class, () -> pair.server.receive((Duration) null));

            // A send times out when the ring is full, having sent nothing
            String name = uniqueName("full");
            try (TestSupport.Pair small = new TestSupport.Pair(name, 1024)) {
                byte[] piece = pattern(small.client.maxPiece());
                small.client.send(piece, FIVE_SECONDS);
                assertFipc(Result.TIMEOUT, () -> small.client.send(pattern(100), Duration.ofMillis(50)));
                assertFipc(Result.TIMEOUT, () -> small.client.sendAcquire(100, Fipc.NO_WAIT));
                assertArrayEquals(piece, small.server.receive(FIVE_SECONDS));
                assertFipc(Result.TIMEOUT, () -> small.server.receive(Fipc.NO_WAIT));
            }
        }
    }

    @Test
    void connectTimesOutWithoutAServer() {
        long start = System.nanoTime();
        assertFipc(Result.TIMEOUT, () -> Connection.connect(uniqueName("nobody"), Duration.ofMillis(200)));
        assertTrue((System.nanoTime() - start) / 1_000_000 >= 150);
    }

    @Test
    void timeoutConversion() {
        assertEquals(0, Fipc.millis(Fipc.NO_WAIT));
        assertEquals(-1, Fipc.millis(Fipc.FOREVER));
        assertEquals(1, Fipc.millis(Duration.ofNanos(1)));
        assertEquals(1500, Fipc.millis(Duration.ofMillis(1500)));
        assertEquals(1501, Fipc.millis(Duration.ofMillis(1500).plusNanos(1)));
        assertEquals(Integer.MAX_VALUE - 1, Fipc.millis(Duration.ofMillis(Integer.MAX_VALUE - 1)));
        assertEquals(-1, Fipc.millis(Duration.ofMillis(Integer.MAX_VALUE)));
        assertEquals(-1, Fipc.millis(Duration.ofDays(365)));
    }

    @Test
    void cancelWakesWaitingCalls() throws Exception {
        try (TestSupport.Pair pair = new TestSupport.Pair("cancel")) {
            Listener idle = Listener.listen(uniqueName("idle"), 4096);
            CompletableFuture<FipcException> receiver = inBackground(() -> pair.server.receive());
            CompletableFuture<FipcException> acceptor = inBackground(() -> idle.accept());
            sleep(200);
            assertTrue(!receiver.isDone() && !acceptor.isDone());
            pair.server.cancel();
            idle.cancel();
            assertEquals(Result.CANCELLED, receiver.get(5, TimeUnit.SECONDS).result());
            assertEquals(Result.CANCELLED, acceptor.get(5, TimeUnit.SECONDS).result());
            assertFipc(Result.CANCELLED, () -> idle.accept(Fipc.FOREVER));
            idle.close();

            // Final, for calls that would wait; calls that needn't wait still work
            assertFipc(Result.CANCELLED, () -> pair.server.receive(FIVE_SECONDS));
            pair.server.send(pattern(10), FIVE_SECONDS);
            assertArrayEquals(pattern(10), pair.client.receive(FIVE_SECONDS));
            pair.client.send(pattern(11), FIVE_SECONDS);
            assertArrayEquals(pattern(11), pair.server.receive(FIVE_SECONDS));
            pair.server.cancel(); // again: a no-op
        }
    }

    /** A close from another thread wakes a waiting receive (CANCELLED); the peer then sees the end. */
    @Test
    void closeFromAnotherThreadWakesTheCall() throws Exception {
        try (TestSupport.Pair pair = new TestSupport.Pair("close")) {
            CompletableFuture<FipcException> receiver = inBackground(() -> pair.server.receive());
            sleep(200);
            pair.server.close();
            assertEquals(Result.CANCELLED, receiver.get(5, TimeUnit.SECONDS).result());
            assertFipc(Result.DISCONNECTED, () -> pair.client.receive(FIVE_SECONDS));
            assertFipc(Result.DISCONNECTED, () -> pair.client.send(pattern(1), FIVE_SECONDS));

            // After close: every call throws IllegalStateException; cancel and close are no-ops
            assertThrows(IllegalStateException.class, () -> pair.server.receive(Fipc.NO_WAIT));
            assertThrows(IllegalStateException.class, () -> pair.server.send(pattern(1)));
            assertThrows(IllegalStateException.class, () -> pair.server.sendAcquire(1));
            assertThrows(IllegalStateException.class, () -> pair.server.sendCommit(1));
            assertThrows(IllegalStateException.class, () -> pair.server.rpcSubmit(1, new byte[0]));
            pair.server.receiveRelease();
            pair.server.cancel();
            pair.server.close();
            assertEquals(pair.listener.name(), pair.server.name());

            pair.listener.close();
            assertThrows(IllegalStateException.class, () -> pair.listener.accept(Fipc.NO_WAIT));
            pair.listener.cancel();
        }
    }

    /** The native close waits for what holds the connection: an acquired message stays readable until its release. */
    @Test
    void closeWhileAMessageIsAcquired() throws Exception {
        try (TestSupport.Pair pair = new TestSupport.Pair("held")) {
            pair.client.send(pattern(1000), FIVE_SECONDS);
            MemorySegment message = pair.server.receiveAcquire(FIVE_SECONDS);
            CompletableFuture.runAsync(pair.server::close, WAITERS).get(5, TimeUnit.SECONDS);

            // Still open natively: the peer sees nothing yet, and the message is readable
            assertFipc(Result.TIMEOUT, () -> pair.client.receive(Duration.ofMillis(100)));
            assertArrayEquals(pattern(1000), message.toArray(ValueLayout.JAVA_BYTE));
            pair.server.receiveRelease(); // the last reference: the native close
            assertThrows(IllegalStateException.class, () -> message.get(ValueLayout.JAVA_BYTE, 0));
            assertFipc(Result.DISCONNECTED, () -> pair.client.receive(FIVE_SECONDS));
        }
    }

    /** The same for a reservation: the commit after a close drops it and throws; then the connection closes. */
    @Test
    void closeWhileASendIsReserved() throws Exception {
        try (TestSupport.Pair pair = new TestSupport.Pair("reserved")) {
            MemorySegment room = pair.client.sendAcquire(8, FIVE_SECONDS);
            CompletableFuture.runAsync(pair.client::close, WAITERS).get(5, TimeUnit.SECONDS);
            room.fill((byte) 1); // still the caller's
            assertThrows(IllegalStateException.class, () -> pair.client.sendCommit(8));
            assertThrows(IllegalStateException.class, () -> room.get(ValueLayout.JAVA_BYTE, 0));
            assertFipc(Result.DISCONNECTED, () -> pair.server.receive(FIVE_SECONDS));
        }
    }

    /** A message sent before the close is delivered before the end. */
    @Test
    void messagesBeforeTheCloseArrive() {
        try (TestSupport.Pair pair = new TestSupport.Pair("drain")) {
            pair.client.send(pattern(1));
            pair.client.send(pattern(2));
            pair.client.close();
            assertArrayEquals(pattern(1), pair.server.receive(FIVE_SECONDS));
            assertArrayEquals(pattern(2), pair.server.receive(FIVE_SECONDS));
            assertFipc(Result.DISCONNECTED, () -> pair.server.receive(FIVE_SECONDS));
        }
    }

    @Test
    void errorResults() {
        String name = uniqueName("errors");
        try (Listener listener = Listener.listen(name, 4096)) {
            FipcException inUse = assertFipc(Result.ADDR_IN_USE, () -> Listener.listen(name, 4096));
            assertEquals("fipc_listen", inUse.call());
            assertEquals("fipc_listen: FIPC_ADDR_IN_USE", inUse.getMessage());
            assertEquals(-1, inUse.messageLength());

            // One client at a time: INVALID while the last accepted connection is open
            CompletableFuture<Connection> connecting = connectAside(name);
            try (Connection server = listener.accept(TEN_SECONDS);
                 Connection client = connecting.join()) {
                assertFipc(Result.INVALID, () -> listener.accept(Fipc.NO_WAIT));
                client.send(pattern(1));
                assertArrayEquals(pattern(1), server.receive(FIVE_SECONDS));
            }
            assertFipc(Result.TIMEOUT, () -> listener.accept(Fipc.NO_WAIT));
        }
        assertFipc(Result.INVALID, () -> Listener.listen("-bad", 4096));
        assertFipc(Result.INVALID, () -> Listener.listen("bad/name", 4096));
        assertFipc(Result.INVALID, () -> Listener.listen("", 4096));
        assertFipc(Result.INVALID, () -> Listener.listen(uniqueName("capacity"), 1000));
        assertFipc(Result.INVALID, () -> Listener.listen(uniqueName("capacity"), 512));
        assertFipc(Result.INVALID, () -> Connection.connect("bad name", Fipc.NO_WAIT));
        assertThrows(NullPointerException.class, () -> Listener.listen(null, 4096));

        for (Result result : Result.values()) {
            assertEquals("FIPC_" + result.name(), result.libraryName());
        }
        assertEquals(Result.INVALID, Result.of(99));
    }

    /** The listener's close frees the name; the connections it accepted stay open. */
    @Test
    void listenerCloseKeepsItsConnections() {
        TestSupport.Pair pair = new TestSupport.Pair("independent");
        try {
            pair.listener.close();
            pair.client.send(pattern(7), FIVE_SECONDS);
            assertArrayEquals(pattern(7), pair.server.receive(FIVE_SECONDS));
            if (!System.getProperty("os.name").startsWith("Windows")) {
                // Linux frees the name at once; Windows once the accepted connections are closed too
                Listener.listen(pair.listener.name(), 4096).close();
            }
        } finally {
            pair.close();
        }
        Listener.listen(pair.listener.name(), 4096).close();
    }
}
