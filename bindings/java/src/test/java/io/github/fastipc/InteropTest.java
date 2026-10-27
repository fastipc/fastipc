package io.github.fastipc;

import static io.github.fastipc.TestSupport.TEN_SECONDS;
import static io.github.fastipc.TestSupport.assertFipc;
import static io.github.fastipc.TestSupport.pattern;
import static io.github.fastipc.TestSupport.uniqueName;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Cross-language: this binding against the Python binding (bindings/python, fipc) in another process, both ways,
 * on a stream, a broadcast (bindings/python/tests/broadcast_peer.py), a work queue
 * (bindings/python/tests/queue_peer.py) and a service (bindings/python/tests/service_peer.py). The Python interpreter is Gradle's fastipc.test.python (the repository's venv
 * by default); it needs cffi. Without one the tests are skipped.
 */
@Timeout(value = 120, unit = TimeUnit.SECONDS)
class InteropTest {
    private static final Duration PEER_START = Duration.ofSeconds(30);

    /** The Python peer: {@code peer.py client|server <name>}. */
    private static final String PYTHON_PEER = """
        import sys
        sys.path.insert(0, sys.argv[3])  # the repository's bindings/python
        from fipc import FipcError, Result, stream_connect, stream_create

        mode, name = sys.argv[1], sys.argv[2]
        if mode == "client":
            # A stream's client: echo each message reversed until the server's end
            with stream_connect(name, timeout_ms=20000) as conn:
                try:
                    while True:
                        conn.send(conn.recv(timeout_ms=20000)[::-1], timeout_ms=20000)
                except FipcError as e:
                    if e.result != Result.DISCONNECTED:
                        raise
            # A second connection: ask the Java server, then answer its question
            with stream_connect(name, timeout_ms=20000) as conn:
                conn.send("hello from Python".encode(), timeout_ms=20000)
                reply = conn.recv(timeout_ms=20000)
                assert reply == b"HELLO FROM PYTHON", reply
                question = conn.recv(timeout_ms=20000)
                conn.send(question * 2, timeout_ms=20000)
                try:
                    conn.recv(timeout_ms=20000)
                    raise AssertionError("expected the server's end")
                except FipcError as e:
                    if e.result != Result.DISCONNECTED:
                        raise
        else:
            with stream_create(name, 1 << 16) as listener, listener.accept(timeout_ms=20000) as conn:
                while True:
                    try:
                        message = conn.recv(timeout_ms=20000)
                    except FipcError as e:
                        if e.result == Result.DISCONNECTED:
                            break
                        raise
                    conn.send(message.upper(), timeout_ms=20000)
        """;

    private static String python;

    @BeforeAll
    static void findPython() throws Exception {
        python = System.getProperty("fastipc.test.python", "python");
        Process probe;
        try {
            probe = new ProcessBuilder(python, "-c", "import cffi").redirectErrorStream(true).start();
        } catch (IOException e) {
            probe = null;
        }
        boolean usable = probe != null && probe.waitFor(30, TimeUnit.SECONDS) && probe.exitValue() == 0;
        Assumptions.assumeTrue(usable, "no Python with cffi at " + python + " (set -PfipcPython=...)");
    }

    private static Process startPython(Path dir, String mode, String name) throws IOException {
        Path script = dir.resolve("peer.py");
        Files.writeString(script, PYTHON_PEER, UTF_8);
        String bindings = TestSupport.repository().resolve("bindings").resolve("python").toString();
        return new ProcessBuilder(List.of(python, script.toString(), mode, name, bindings)).inheritIO().start();
    }

    /** The Python binding's broadcast peer: {@code broadcast_peer.py publisher|subscriber <name> ...}. */
    private static Process startPythonBroadcast(String... args) throws IOException {
        Path peer = TestSupport.repository().resolve("bindings").resolve("python").resolve("tests")
            .resolve("broadcast_peer.py");
        List<String> command = new ArrayList<>(List.of(python, peer.toString()));
        command.addAll(List.of(args));
        return new ProcessBuilder(command).inheritIO().start();
    }

    /** The Python binding's queue peer: {@code queue_peer.py producer|consumer|owner-consumer|joined-producer ...}. */
    private static Process startPythonQueue(String... args) throws IOException {
        Path peer = TestSupport.repository().resolve("bindings").resolve("python").resolve("tests")
            .resolve("queue_peer.py");
        List<String> command = new ArrayList<>(List.of(python, peer.toString()));
        command.addAll(List.of(args));
        return new ProcessBuilder(command).inheritIO().start();
    }

    /**
     * The Python binding's service peer: {@code service_peer.py server|client|owner-client|joined-server|holding-server
     * ...}; its output goes to this process's, or into a pipe ({@code readOutput}).
     */
    private static Process startPythonService(boolean readOutput, String... args) throws IOException {
        Path peer = TestSupport.repository().resolve("bindings").resolve("python").resolve("tests")
            .resolve("service_peer.py");
        List<String> command = new ArrayList<>(List.of(python, peer.toString()));
        command.addAll(List.of(args));
        ProcessBuilder builder = new ProcessBuilder(command);
        return (readOutput ? builder.redirectError(ProcessBuilder.Redirect.INHERIT) : builder.inheritIO()).start();
    }

    /** A Python server (the service's owner's), a Java client. */
    @Test
    void pythonServiceServerJavaClient() throws Exception {
        String name = uniqueName("py_service_server");
        Process peer = startPythonService(false, "server", name, "300", "1");
        Peer.joinAndCall(name, 300);
        assertExited(peer, "the Python server");
    }

    /** Two Python clients, the Java owner's server. */
    @Test
    void pythonServiceClientsJavaOwnersServer() throws Exception {
        String name = uniqueName("py_service_clients");
        List<Process> peers = List.of(startPythonService(false, "client", name, "300"),
            startPythonService(false, "client", name, "300"));
        Peer.serve(name, 300, 2);
        for (Process peer : peers) {
            assertExited(peer, "a Python client");
        }
    }

    /** The Python owner's client, two joined Java servers. */
    @Test
    void pythonServiceOwnersClientJavaJoinedServers() throws Exception {
        String name = uniqueName("py_service_owner");
        Process peer = startPythonService(false, "owner-client", name, "300", "2");
        List<CompletableFuture<Void>> servers = new ArrayList<>();
        for (int k = 0; k < 2; k++) {
            servers.add(CompletableFuture.runAsync(() -> Peer.joinAndAnswer(name)));
        }
        for (CompletableFuture<Void> server : servers) {
            server.get(60, TimeUnit.SECONDS);
        }
        assertExited(peer, "the Python owner's client");
    }

    /** Two joined Python servers, the Java owner's client. */
    @Test
    void pythonServiceJoinedServersJavaOwnersClient() throws Exception {
        String name = uniqueName("py_service_joined");
        List<Process> peers = List.of(startPythonService(false, "joined-server", name),
            startPythonService(false, "joined-server", name));
        Peer.ownAndCall(name, 300, 2);
        for (Process peer : peers) {
            assertExited(peer, "a Python server");
        }
    }

    /**
     * A Python server takes a request and holds it until it is killed: the Java client's receive throws ABANDONED
     * once the owner's process (this one) has seen its end, and the next call gets its own reply.
     */
    @Test
    void pythonServiceServerKilledHoldingARequestAbandonsIt() throws Exception {
        String name = uniqueName("py_service_killed");
        try (Service _ = Fipc.createService(name, 4096, 64, 1, 2);
             Connection client = Fipc.joinService(name, ServiceRole.CLIENT, TEN_SECONDS)) {
            Process peer = startPythonService(true, "holding-server", name);
            try (BufferedReader output = new BufferedReader(new InputStreamReader(peer.getInputStream(), UTF_8))) {
                client.send("doomed".getBytes(UTF_8), TEN_SECONDS);
                assertEquals("holding", output.readLine());
                peer.destroyForcibly();
                assertTrue(peer.waitFor(60, TimeUnit.SECONDS));
                assertFipc(Result.ABANDONED, () -> client.receive(TEN_SECONDS));
                try (Connection server = Fipc.joinService(name, ServiceRole.SERVER, TEN_SECONDS)) {
                    CompletableFuture<Void> answering = CompletableFuture.runAsync(() -> Peer.answer(server, 1));
                    assertEquals("NEXT", new String(client.call("next".getBytes(UTF_8), TEN_SECONDS), UTF_8));
                    answering.get(10, TimeUnit.SECONDS);
                }
            } finally {
                peer.destroyForcibly();
            }
        }
    }

    private static void assertExited(Process peer, String what) throws InterruptedException {
        assertTrue(peer.waitFor(60, TimeUnit.SECONDS), what + " didn't exit");
        assertEquals(0, peer.exitValue(), what + " failed");
    }

    /** A Python producer (the queue's owner's), a Java consumer. */
    @Test
    void pythonProducerJavaConsumer() throws Exception {
        String name = uniqueName("py_producer");
        Process peer = startPythonQueue("producer", name, "300", "1");
        Peer.consume(name, 300);
        assertExited(peer, "the Python producer");
    }

    /** A Java producer (the queue's owner's), a Python consumer. */
    @Test
    void javaProducerPythonConsumer() throws Exception {
        String name = uniqueName("py_consumer");
        Process peer = startPythonQueue("consumer", name, "300");
        Peer.produce(name, 300, 1);
        assertExited(peer, "the Python consumer");
    }

    /** Two joined Python producers, the Java owner's consumer. */
    @Test
    void pythonJoinedProducersJavaOwnersConsumer() throws Exception {
        String name = uniqueName("py_joined_producers");
        List<Process> peers = List.of(startPythonQueue("joined-producer", name, "300"),
            startPythonQueue("joined-producer", name, "300"));
        Peer.ownAndConsume(name, 300, 2);
        for (Process peer : peers) {
            assertExited(peer, "a Python producer");
        }
    }

    /** The Python owner's consumer, a joined Java producer. */
    @Test
    void pythonOwnersConsumerJavaJoinedProducer() throws Exception {
        String name = uniqueName("py_owner_consumer");
        Process peer = startPythonQueue("owner-consumer", name, "300", "1");
        Peer.joinAndProduce(name, 300);
        assertExited(peer, "the Python owner's consumer");
    }

    /** A Python publisher, a gated and a lossy Java subscriber. */
    @Test
    void pythonPublisherJavaSubscribers() throws Exception {
        String name = uniqueName("py_publisher");
        Process peer = startPythonBroadcast("publisher", name, "300", "2");
        List<CompletableFuture<Void>> subscribers = new ArrayList<>();
        for (boolean gated : new boolean[] {true, false}) {
            subscribers.add(CompletableFuture.runAsync(() -> Peer.subscribe(name, gated, 300)));
        }
        for (CompletableFuture<Void> subscriber : subscribers) {
            subscriber.get(60, TimeUnit.SECONDS);
        }
        assertTrue(peer.waitFor(60, TimeUnit.SECONDS), "the Python publisher didn't exit");
        assertEquals(0, peer.exitValue(), "the Python publisher failed");
    }

    /** A Java publisher, a gated and a lossy Python subscriber. */
    @Test
    void javaPublisherPythonSubscribers() throws Exception {
        String name = uniqueName("py_subscribers");
        List<Process> peers = List.of(startPythonBroadcast("subscriber", name, "gated", "300"),
            startPythonBroadcast("subscriber", name, "lossy", "300"));
        Peer.publish(name, 300, 2);
        for (Process peer : peers) {
            assertTrue(peer.waitFor(60, TimeUnit.SECONDS), "a Python subscriber didn't exit");
            assertEquals(0, peer.exitValue(), "a Python subscriber failed");
        }
    }

    /** A Java server and a Python client: echoed messages, then a second connection that asks in both directions. */
    @Test
    void javaServerPythonClient(@TempDir Path dir) throws Exception {
        String name = uniqueName("py_client");
        try (Listener listener = Fipc.createStream(name, 1 << 16)) {
            Process peer = startPython(dir, "client", name);
            try (Connection conn = listener.accept(PEER_START)) {
                for (int size : new int[] {1, 100, 70_000, 200_000}) {
                    byte[] message = pattern(size);
                    conn.send(message, TEN_SECONDS);
                    byte[] reversed = new byte[size];
                    for (int i = 0; i < size; i++) {
                        reversed[i] = message[size - 1 - i];
                    }
                    assertArrayEquals(reversed, conn.receive(TEN_SECONDS), "size " + size);
                }
            }
            try (Connection conn = listener.accept(PEER_START)) {
                assertEquals("hello from Python", new String(conn.receive(TEN_SECONDS), UTF_8));
                conn.send("HELLO FROM PYTHON".getBytes(UTF_8), TEN_SECONDS);

                conn.send("Java".getBytes(UTF_8), TEN_SECONDS);
                assertEquals("JavaJava", new String(conn.receive(TEN_SECONDS), UTF_8));
            }
            assertTrue(peer.waitFor(30, TimeUnit.SECONDS), "the Python peer didn't exit");
            assertEquals(0, peer.exitValue(), "the Python peer failed");
        }
    }

    /**
     * A Python server that answers each message in upper case and a Java client, its messages pipelined, one longer
     * than the ring; the Java client's close ends the Python server.
     */
    @Test
    void pythonServerJavaClient(@TempDir Path dir) throws Exception {
        String name = uniqueName("py_server");
        Process peer = startPython(dir, "server", name);
        try (Connection conn = Fipc.connectStream(name, PEER_START)) {
            conn.send("shared memory".getBytes(UTF_8), TEN_SECONDS);
            conn.send(pattern(300_000), TEN_SECONDS);
            assertEquals("SHARED MEMORY", new String(conn.receive(TEN_SECONDS), UTF_8));
            assertEquals(300_000, conn.receive(TEN_SECONDS).length);
            assertFipc(Result.TIMEOUT, () -> conn.receive(Fipc.NO_WAIT));
        }
        assertTrue(peer.waitFor(30, TimeUnit.SECONDS), "the Python peer didn't exit");
        assertEquals(0, peer.exitValue(), "the Python peer failed");
    }
}
