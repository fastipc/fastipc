package io.github.fastipc;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.tools.ToolProvider;
import org.junit.jupiter.api.function.Executable;

/**
 * What the tests share: unique names, test data, a connected pair, peers in other processes, and the example programs
 * in pairs.
 */
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
     * A stream's listener and a server and client connection on its name. The client connects on another thread: a
     * connect waits for the listener's accept, so a client and its server in one process run on different threads.
     */
    static final class Pair implements AutoCloseable {
        final Listener listener;
        final Connection client;
        final Connection server;

        Pair(String name, long ring) {
            listener = Fipc.createStream(name, ring);
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
        return CompletableFuture.supplyAsync(() -> Fipc.connectStream(name, TEN_SECONDS));
    }

    /** The repository's root (Gradle passes it). */
    static Path repository() {
        return Path.of(System.getProperty("fastipc.test.repo"));
    }

    /** A JVM running {@link Peer} with {@code args}, on the tests' class path; its output goes to this process's. */
    static Process startPeer(String... args) throws IOException {
        return new ProcessBuilder(peerCommand(args)).inheritIO().start();
    }

    /** As {@link #startPeer}, with the peer's output in a pipe the caller reads ({@code getInputStream()}). */
    static Process startPeerReadingItsOutput(String... args) throws IOException {
        return new ProcessBuilder(peerCommand(args)).redirectError(ProcessBuilder.Redirect.INHERIT).start();
    }

    private static List<String> peerCommand(String... args) {
        List<String> command = new ArrayList<>();
        command.add(ProcessHandle.current().info().command().orElseThrow());
        command.add("--enable-native-access=ALL-UNNAMED");
        command.add("-cp");
        command.add(System.getProperty("fastipc.test.classpath"));
        command.add(Peer.class.getName());
        command.addAll(List.of(args));
        return command;
    }

    /**
     * examples/java's {@code sender}.java and {@code receiver}.java (a publisher and a subscriber, a producer and a
     * consumer, or a service's server and client), compiled into {@code classes} and each run in a JVM of its own,
     * receiver first and sender first: the receiver prints {@code printed} (in any order if {@code anyOrder}: a queue
     * promises none), and both exit 0.
     * Receiver first, the sender starts 0.5 s after it; sender first, the receiver starts 2 s after it and waits up to
     * 5 s more in its join.
     */
    static void runExamplePair(Path classes, String sender, String receiver, List<String> printed, boolean anyOrder)
            throws Exception {
        Path examples = repository().resolve("examples").resolve("java");
        int compiled = ToolProvider.getSystemJavaCompiler().run(null, null, null, "-d", classes.toString(), "-cp",
            System.getProperty("fastipc.test.classpath"), examples.resolve(sender + ".java").toString(),
            examples.resolve(receiver + ".java").toString());
        assertEquals(0, compiled, "the examples didn't compile");
        for (boolean receiverFirst : new boolean[] {true, false}) {
            Process first = startExample(classes, receiverFirst ? receiver : sender);
            Thread.sleep(receiverFirst ? 500 : 2000);
            Process second = startExample(classes, receiverFirst ? sender : receiver);
            Process receiving = receiverFirst ? first : second;
            Process sending = receiverFirst ? second : first;
            try {
                assertTrue(receiving.waitFor(60, TimeUnit.SECONDS), "the " + receiver + " example didn't exit");
                assertTrue(sending.waitFor(60, TimeUnit.SECONDS), "the " + sender + " example didn't exit");
                String output = new String(receiving.getInputStream().readAllBytes(), UTF_8);
                assertEquals(0, receiving.exitValue(), "the " + receiver + " example failed: " + output);
                assertEquals(0, sending.exitValue(), "the " + sender + " example failed");
                List<String> lines = output.lines().toList();
                assertEquals(printed, anyOrder ? lines.stream().sorted().toList() : lines);
            } finally {
                first.destroyForcibly();
                second.destroyForcibly();
            }
        }
    }

    /** A JVM running the example {@code program}, compiled in {@code classes}, with the tests' class path. */
    private static Process startExample(Path classes, String program) throws IOException {
        String classPath = classes + File.pathSeparator + System.getProperty("fastipc.test.classpath");
        return new ProcessBuilder(ProcessHandle.current().info().command().orElseThrow(),
            "--enable-native-access=ALL-UNNAMED", "-cp", classPath, program)
            .redirectError(ProcessBuilder.Redirect.INHERIT).start();
    }
}
