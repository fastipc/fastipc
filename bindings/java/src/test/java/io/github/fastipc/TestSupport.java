package io.github.fastipc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.function.Executable;

/** What the tests share: unique names, test data, a connected pair, and peers in other processes. */
final class TestSupport {
    static final long RING = 1 << 20;
    static final Duration FIVE_SECONDS = Duration.ofSeconds(5);
    static final Duration TEN_SECONDS = Duration.ofSeconds(10);
    private static final AtomicInteger COUNTER = new AtomicInteger();

    private TestSupport() {
    }

    /** A name no other test (or run) uses. */
    static String uniqueName(String prefix) {
        return "fipc_java_" + prefix + "_" + ProcessHandle.current().pid() + "_" + COUNTER.incrementAndGet() + "_"
            + System.nanoTime() % 1_000_000;
    }

    /** {@code length} bytes of a pattern that differs at every offset of a ring's frame. */
    static byte[] pattern(int length) {
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; i++) {
            bytes[i] = (byte) (i * 31 + 7);
        }
        return bytes;
    }

    /** The exception {@code call} throws, which must carry {@code expected}. */
    static FipcException assertFipc(Result expected, Executable call) {
        FipcException e = assertThrows(FipcException.class, call);
        assertEquals(expected, e.result(), e.getMessage());
        return e;
    }

    /**
     * A listener and a server and client connection on one name. The client connects on another thread: a connect
     * waits for the listener's accept, so a client and its server in one process run on different threads.
     */
    static final class Pair implements AutoCloseable {
        final Listener listener;
        final Connection client;
        final Connection server;

        Pair(String name, long ring) {
            listener = Listener.listen(name, ring);
            CompletableFuture<Connection> connecting = connectAside(name);
            server = listener.accept(TEN_SECONDS);
            client = connecting.join();
        }

        Pair(String prefix) {
            this(uniqueName(prefix), RING);
        }

        @Override
        public void close() {
            client.close();
            server.close();
            listener.close();
        }
    }

    /** Connects to {@code name} on another thread, within ten seconds, while the caller accepts. */
    static CompletableFuture<Connection> connectAside(String name) {
        return CompletableFuture.supplyAsync(() -> Connection.connect(name, TEN_SECONDS));
    }

    /** The repository's root (Gradle passes it). */
    static Path repository() {
        return Path.of(System.getProperty("fastipc.test.repo"));
    }

    /** A JVM running {@link Peer} with {@code args}, on the tests' class path; its output goes to this process's. */
    static Process startPeer(String... args) throws IOException {
        List<String> command = new ArrayList<>();
        command.add(ProcessHandle.current().info().command().orElseThrow());
        command.add("--enable-native-access=ALL-UNNAMED");
        command.add("-cp");
        command.add(System.getProperty("fastipc.test.classpath"));
        command.add(Peer.class.getName());
        command.addAll(List.of(args));
        return new ProcessBuilder(command).inheritIO().start();
    }
}
