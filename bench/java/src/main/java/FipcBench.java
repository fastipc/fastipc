import io.github.fastipc.Connection;
import io.github.fastipc.Fipc;
import io.github.fastipc.FipcException;
import io.github.fastipc.Listener;
import io.github.fastipc.Result;
import io.github.fastipc.RpcHeader;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * FastIPC's Java benchmark, through the binding (io.github.fastipc): one-way throughput between this
 * process, a server that receives and times, and a client process (this program again, in a new JVM) that sends.
 *
 * <pre>
 *   copy      Connection.send / receive into a native segment the server reuses
 *   zerocopy  sendAcquire + sendCommit / receiveAcquire + receiveRelease (a message of several pieces through the
 *             copying calls); the server copies each message out, as a consumer would
 *   rpc       rpcSubmit / rpcReceive into a native segment the server reuses
 * </pre>
 *
 * The JIT compiles the code only after it has run a while, so each case first sends messages untimed for half a
 * second (the warm-up; at least its count), then a start marker, then its count, and the server times from the last
 * start marker to the end marker (see {@link #client}). The client
 * sends from native memory. Each case prints "Test i/n: name" and "Throughput: n messages/sec" (devtool bench-compare
 * reads them).
 */
public final class FipcBench {
    private static final Duration CONNECT = Duration.ofSeconds(15);
    private static final long WARM_UP_NS = 500_000_000;
    private static final int MARKER_EVERY = 1000;
    private static final int DATA_OPCODE = 1;
    private static final int GO_OPCODE = 0xFB000002;
    private static final int END_OPCODE = 0xFB000001;
    private static final byte[] GO = "GO!".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] END = "END".getBytes(StandardCharsets.US_ASCII);

    private record Case(int count, int ring, int size, String name) {
    }

    private static final Case[] CASES = {
        new Case(2_000_000, 512 * 1024, 16, "Tiny messages (16B)"),
        new Case(2_000_000, 512 * 1024, 64, "Small messages (64B)"),
        new Case(1_000_000, 512 * 1024, 256, "Medium messages (256B)"),
        new Case(1000, 512 * 1024, 64 * 1024, "Large messages (64KB)"),
        new Case(1000, 2 * 1024 * 1024, 512 * 1024, "Large messages (512KB)"),
        new Case(10, 512 * 1024, 1024 * 1024, "Exceeds buffer (1MB msg, 512KB buffer)"),
    };

    private FipcBench() {
    }

    public static void main(String[] args) throws Exception {
        String mode = args.length > 0 ? args[0] : "copy";
        if (mode.equals("client")) {
            client(args[1], args[2], Integer.parseInt(args[3]), Integer.parseInt(args[4]));
            return;
        }
        if (!List.of("copy", "zerocopy", "rpc").contains(mode)) {
            System.err.println("usage: FipcBench copy|zerocopy|rpc");
            System.exit(2);
        }
        System.out.println("FastIPC Java benchmark: " + mode + "\n");
        int passed = 0;
        for (int i = 0; i < CASES.length; i++) {
            Case c = CASES[i];
            System.out.println("Test " + (i + 1) + "/" + CASES.length + ": " + c.name());
            if (runCase(mode, c)) {
                passed++;
            } else {
                System.out.println("FAILED");
            }
            System.out.println();
        }
        System.out.println("Summary: " + passed + "/" + CASES.length + " tests passed");
        System.exit(passed == CASES.length ? 0 : 1);
    }

    private static boolean runCase(String mode, Case c) throws Exception {
        String name = "javabench_" + ProcessHandle.current().pid() + "_" + System.currentTimeMillis();
        List<String> command = new ArrayList<>(List.of(ProcessHandle.current().info().command().orElseThrow(),
            "--enable-native-access=ALL-UNNAMED", "-cp", System.getProperty("java.class.path"), "FipcBench",
            "client", mode, name, String.valueOf(c.size()), String.valueOf(c.count())));
        String library = System.getProperty("fastipc.library.path");
        if (library != null) {
            command.add(1, "-Dfastipc.library.path=" + library); // the client measures the same library
        }
        long[] result;
        Process client;
        try (Listener listener = Listener.listen(name, c.ring())) {
            client = new ProcessBuilder(command).redirectOutput(ProcessBuilder.Redirect.DISCARD)
                .redirectError(ProcessBuilder.Redirect.INHERIT).start();
            try (Connection conn = listener.accept(CONNECT)) {
                result = serve(mode, conn, c.size());
            } catch (FipcException e) {
                client.destroyForcibly();
                System.err.println("server: " + e.getMessage());
                return false;
            }
        }
        if (!client.waitFor(180, TimeUnit.SECONDS)) {
            client.destroyForcibly();
            System.err.println("the client timed out");
            return false;
        }
        if (client.exitValue() != 0) {
            System.err.println("the client failed: exit " + client.exitValue());
            return false;
        }
        long messages = result[0];
        long bytes = result[1];
        double seconds = result[2] / 1e9;
        if (messages != c.count() || bytes != (long) c.count() * c.size()) {
            System.err.printf("expected %d messages of %d B, got %d (%d B)%n", c.count(), c.size(), messages, bytes);
            return false;
        }
        System.out.printf("Messages: %d | Ring: %dKB | Size: %dB%n", c.count(), c.ring() / 1024, c.size());
        System.out.printf("Duration: %.3fs%n", seconds);
        System.out.printf("Throughput: %.0f messages/sec, %.1f MB/sec%n", messages / seconds, bytes / seconds / (1 << 20));
        return true;
    }

    /** The server: skips the warm-up up to the marker, then receives until the end marker; messages, bytes, ns. */
    private static long[] serve(String mode, Connection conn, int size) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment buffer = arena.allocate(Math.max(size, END.length));
            MemorySegment sink = arena.allocate(size);
            boolean timing = false;
            long messages = 0;
            long bytes = 0;
            long start = 0;
            while (true) {
                long length = receiveOne(mode, conn, buffer, sink);
                if (length == AT_END) {
                    break;
                }
                if (length == AT_GO) { // the last one starts the timed messages
                    timing = true;
                    messages = 0;
                    bytes = 0;
                    start = System.nanoTime();
                } else if (timing) {
                    messages++;
                    bytes += length;
                }
            }
            return new long[] {messages, bytes, System.nanoTime() - start};
        }
    }

    private static final long AT_END = -1;
    private static final long AT_GO = -2;

    /**
     * One message received, as a consumer would: its length, or AT_GO or AT_END for the markers. A method of its
     * own, so the JIT compiles it after its first thousands of calls (a loop in a method called once waits for an
     * on-stack replacement).
     */
    private static long receiveOne(String mode, Connection conn, MemorySegment buffer, MemorySegment sink) {
        switch (mode) {
            case "rpc" -> {
                RpcHeader message = conn.rpcReceive(buffer);
                return message.opcode() == END_OPCODE ? AT_END : message.opcode() == GO_OPCODE ? AT_GO : message.length();
            }
            case "zerocopy" -> {
                MemorySegment message;
                try {
                    message = conn.receiveAcquire();
                } catch (FipcException e) {
                    if (e.result() != Result.TOO_LARGE) {
                        throw e;
                    }
                    return conn.receive(buffer); // several pieces
                }
                long length = message.byteSize();
                long marker = marker(message);
                if (marker == 0) {
                    MemorySegment.copy(message, 0, sink, 0, length);
                }
                conn.receiveRelease();
                return marker == 0 ? length : marker;
            }
            default -> {
                long length = conn.receive(buffer);
                long marker = marker(buffer.asSlice(0, length));
                return marker == 0 ? length : marker;
            }
        }
    }

    /** AT_END or AT_GO for a marker, else 0. */
    private static long marker(MemorySegment message) {
        if (message.byteSize() != END.length) {
            return 0;
        }
        MemorySegment bytes = message.asSlice(0, END.length);
        if (MemorySegment.mismatch(bytes, 0, END.length, MemorySegment.ofArray(END), 0, END.length) == -1) {
            return AT_END;
        }
        return MemorySegment.mismatch(bytes, 0, GO.length, MemorySegment.ofArray(GO), 0, GO.length) == -1 ? AT_GO : 0;
    }

    /**
     * The client: messages untimed for the warm-up (the count, and for at least WARM_UP_NS: the JIT compiles the hot
     * path in the background, which takes longer than the shorter cases' count), the start marker, the count, the end
     * marker. The warm-up sends a start marker every MARKER_EVERY messages too, so that the real one takes no path the
     * JIT hasn't seen (one would deoptimize the compiled code, and the timed messages would run before it is compiled
     * again); the markers are native segments, as the messages are, for the same reason.
     */
    private static void client(String mode, String name, int size, int count) {
        try (Connection conn = Connection.connect(name, CONNECT); Arena arena = Arena.ofConfined()) {
            MemorySegment payload = arena.allocate(size).fill((byte) 'x');
            MemorySegment go = arena.allocate(GO.length).copyFrom(MemorySegment.ofArray(GO));
            MemorySegment end = arena.allocate(END.length).copyFrom(MemorySegment.ofArray(END));
            boolean onePiece = size <= conn.maxPiece();
            long warm = System.nanoTime() + WARM_UP_NS;
            for (int i = 0; i < count || System.nanoTime() < warm; i++) {
                if (i % MARKER_EVERY == 0) {
                    sendMarker(mode, conn, go, GO_OPCODE);
                }
                sendOne(mode, conn, payload, onePiece);
            }
            sendMarker(mode, conn, go, GO_OPCODE);
            for (int i = 0; i < count; i++) {
                sendOne(mode, conn, payload, onePiece);
            }
            sendMarker(mode, conn, end, END_OPCODE);
        } // the server still receives everything sent before the close
        System.exit(0);
    }

    private static void sendMarker(String mode, Connection conn, MemorySegment marker, int opcode) {
        if (mode.equals("rpc")) {
            conn.rpcSubmit(opcode, marker.asSlice(0, 0), Fipc.FOREVER);
        } else {
            conn.send(marker);
        }
    }

    /** One message sent (a method of its own, for the JIT, as receiveOne). */
    private static void sendOne(String mode, Connection conn, MemorySegment payload, boolean onePiece) {
        switch (mode) {
            case "rpc" -> conn.rpcSubmit(DATA_OPCODE, payload, Fipc.FOREVER);
            case "zerocopy" -> {
                if (onePiece) {
                    int size = (int) payload.byteSize();
                    MemorySegment room = conn.sendAcquire(size);
                    MemorySegment.copy(payload, 0, room, 0, size);
                    conn.sendCommit(size);
                } else {
                    conn.send(payload);
                }
            }
            default -> conn.send(payload);
        }
    }
}
