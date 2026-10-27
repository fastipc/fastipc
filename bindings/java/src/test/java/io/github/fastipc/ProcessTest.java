package io.github.fastipc;

import static io.github.fastipc.TestSupport.TEN_SECONDS;
import static io.github.fastipc.TestSupport.assertFipc;
import static io.github.fastipc.TestSupport.pattern;
import static io.github.fastipc.TestSupport.startPeer;
import static io.github.fastipc.TestSupport.uniqueName;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Two processes: the peer is another JVM ({@link Peer}). */
@Timeout(value = 120, unit = TimeUnit.SECONDS)
class ProcessTest {
    private static final Duration PEER_START = Duration.ofSeconds(30);

    @Test
    void echoAcrossProcesses() throws Exception {
        String name = uniqueName("echo");
        try (Listener listener = Fipc.createStream(name, 1 << 16)) {
            Process peer = startPeer("echo", name);
            try (Connection conn = listener.accept(PEER_START)) {
                for (int size : new int[] {1, 16, 1000, 60_000, 300_000}) {
                    conn.send(pattern(size), TEN_SECONDS);
                    assertArrayEquals(pattern(size), conn.receive(TEN_SECONDS), "size " + size);
                }
            }
            assertTrue(peer.waitFor(20, TimeUnit.SECONDS), "the peer didn't exit");
            assertEquals(0, peer.exitValue());
        }
    }

    /** A peer that exits without closing: its messages arrive, then DISCONNECTED. */
    @Test
    void peerExitIsDisconnected() throws Exception {
        String name = uniqueName("exit");
        try (Listener listener = Fipc.createStream(name, 1 << 16)) {
            Process peer = startPeer("send-and-exit", name);
            try (Connection conn = listener.accept(PEER_START)) {
                for (int i = 1; i <= 3; i++) {
                    assertArrayEquals(new byte[] {(byte) i}, conn.receive(TEN_SECONDS));
                }
                assertFipc(Result.DISCONNECTED, () -> conn.receive(TEN_SECONDS));
                assertFipc(Result.DISCONNECTED, () -> conn.send(pattern(1), TEN_SECONDS));
            }
            assertTrue(peer.waitFor(20, TimeUnit.SECONDS));
            assertEquals(0, peer.exitValue());
        }
    }

    /** A killed peer: a receive that waits wakes with DISCONNECTED; the next client is accepted. */
    @Test
    void killedPeerIsDisconnected() throws Exception {
        String name = uniqueName("kill");
        try (Listener listener = Fipc.createStream(name, 1 << 16)) {
            Process peer = startPeer("connect-and-wait", name);
            try (Connection conn = listener.accept(PEER_START)) {
                assertArrayEquals("ready".getBytes(), conn.receive(TEN_SECONDS));
                long start = System.nanoTime();
                peer.destroyForcibly();
                assertFipc(Result.DISCONNECTED, () -> conn.receive(TEN_SECONDS));
                long ms = (System.nanoTime() - start) / 1_000_000;
                assertTrue(ms < 5000, "DISCONNECTED took " + ms + " ms after the kill");
            }
            Process next = startPeer("echo", name);
            try (Connection conn = listener.accept(PEER_START)) {
                conn.send(pattern(5), TEN_SECONDS);
                assertArrayEquals(pattern(5), conn.receive(TEN_SECONDS));
            }
            assertTrue(next.waitFor(20, TimeUnit.SECONDS));
        }
    }

    /** This process as the client of a server in another process; two messages pipelined, one longer than the ring. */
    @Test
    void clientOfAnotherProcess() throws Exception {
        String name = uniqueName("server");
        Process peer = startPeer("reverse-server", name);
        try (Connection conn = Fipc.connectStream(name, PEER_START)) {
            conn.send("abc".getBytes(), TEN_SECONDS);
            conn.send(pattern(200_000), TEN_SECONDS);
            assertArrayEquals("cba".getBytes(), conn.receive(TEN_SECONDS));
            byte[] two = conn.receive(TEN_SECONDS);
            byte[] reversed = pattern(200_000);
            for (int i = 0; i < reversed.length / 2; i++) {
                byte b = reversed[i];
                reversed[i] = reversed[reversed.length - 1 - i];
                reversed[reversed.length - 1 - i] = b;
            }
            assertArrayEquals(reversed, two);
        }
        assertTrue(peer.waitFor(20, TimeUnit.SECONDS));
        assertEquals(0, peer.exitValue());
    }
}
