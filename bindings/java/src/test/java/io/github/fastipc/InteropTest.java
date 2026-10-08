package io.github.fastipc;

import static io.github.fastipc.TestSupport.TEN_SECONDS;
import static io.github.fastipc.TestSupport.assertFipc;
import static io.github.fastipc.TestSupport.pattern;
import static io.github.fastipc.TestSupport.uniqueName;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Cross-language: this binding against the Python binding (bindings/python, fipc) in another process, both
 * ways, with plain messages and RPC. The Python interpreter is Gradle's fastipc.test.python (the repository's venv by
 * default); it needs cffi. Without one the tests are skipped.
 */
@Timeout(value = 120, unit = TimeUnit.SECONDS)
class InteropTest {
    private static final Duration PEER_START = Duration.ofSeconds(30);

    /** The Python peer: {@code peer.py client|server <name>}. */
    private static final String PYTHON_PEER = """
        import sys
        sys.path.insert(0, sys.argv[3])  # the repository's bindings/python
        from fipc import Conn, FipcError, Listener, Result, RPC_REQUEST

        mode, name = sys.argv[1], sys.argv[2]
        if mode == "client":
            # A plain connection: echo each message reversed until the server's end
            with Conn.connect(name, timeout_ms=20000) as conn:
                try:
                    while True:
                        conn.send(conn.recv(timeout_ms=20000)[::-1], timeout_ms=20000)
                except FipcError as e:
                    if e.result != Result.DISCONNECTED:
                        raise
            # An RPC connection: call the Java server, then answer its call
            with Conn.connect(name, timeout_ms=20000) as conn:
                request_id = conn.rpc_submit(7, "hello from Python".encode(), timeout_ms=20000)
                reply = conn.rpc_recv(timeout_ms=20000)
                assert (reply.id, reply.status, reply.payload) == (request_id, 42, b"HELLO FROM PYTHON"), reply
                request = conn.rpc_recv(timeout_ms=20000)
                assert request.kind == RPC_REQUEST and request.opcode == 9, request
                conn.rpc_respond(request.id, 9, status=len(request.payload), data=request.payload * 2,
                                 timeout_ms=20000)
                try:
                    conn.rpc_recv(timeout_ms=20000)
                    raise AssertionError("expected the server's end")
                except FipcError as e:
                    if e.result != Result.DISCONNECTED:
                        raise
        else:
            with Listener(name, 1 << 16) as listener, listener.accept(timeout_ms=20000) as conn:
                while True:
                    try:
                        request = conn.rpc_recv(timeout_ms=20000)
                    except FipcError as e:
                        if e.result == Result.DISCONNECTED:
                            break
                        raise
                    conn.rpc_respond(request.id, request.opcode, status=-1, data=request.payload.upper(),
                                     timeout_ms=20000)
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

    /** A Java server and a Python client: plain messages, then RPC in both directions. */
    @Test
    void javaServerPythonClient(@TempDir Path dir) throws Exception {
        String name = uniqueName("py_client");
        try (Listener listener = Listener.listen(name, 1 << 16)) {
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
                RpcMessage request = conn.rpcReceive(TEN_SECONDS);
                assertEquals(RpcKind.REQUEST, request.kind());
                assertEquals(7, request.opcode());
                assertEquals("hello from Python", new String(request.payload(), UTF_8));
                conn.rpcRespond(request.id(), 7, 42, "HELLO FROM PYTHON".getBytes(UTF_8), TEN_SECONDS);

                long id = conn.rpcSubmit(9, "Java".getBytes(UTF_8), TEN_SECONDS);
                RpcMessage reply = conn.rpcReceive(TEN_SECONDS);
                assertEquals(RpcKind.RESPONSE, reply.kind());
                assertEquals(id, reply.id());
                assertEquals(4, reply.status());
                assertEquals("JavaJava", new String(reply.payload(), UTF_8));
            }
            assertTrue(peer.waitFor(30, TimeUnit.SECONDS), "the Python peer didn't exit");
            assertEquals(0, peer.exitValue(), "the Python peer failed");
        }
    }

    /** A Python server and a Java client over RPC; the Java client's close ends the Python server. */
    @Test
    void pythonServerJavaClient(@TempDir Path dir) throws Exception {
        String name = uniqueName("py_server");
        Process peer = startPython(dir, "server", name);
        try (Connection conn = Connection.connect(name, PEER_START)) {
            long first = conn.rpcSubmit(1, "shared memory".getBytes(UTF_8), TEN_SECONDS);
            long second = conn.rpcSubmit(2, pattern(300_000), TEN_SECONDS);
            RpcMessage one = conn.rpcReceive(TEN_SECONDS);
            assertEquals(first, one.id());
            assertEquals(-1, one.status());
            assertEquals("SHARED MEMORY", new String(one.payload(), UTF_8));
            RpcMessage two = conn.rpcReceive(TEN_SECONDS);
            assertEquals(second, two.id());
            assertEquals(300_000, two.payload().length);
            assertFipc(Result.TIMEOUT, () -> conn.rpcReceive(Fipc.NO_WAIT));
        }
        assertTrue(peer.waitFor(30, TimeUnit.SECONDS), "the Python peer didn't exit");
        assertEquals(0, peer.exitValue(), "the Python peer failed");
    }
}
