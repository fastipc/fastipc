package io.github.fastipc;

import java.time.Duration;
import java.util.Arrays;

/**
 * The other process of the two-process tests: {@code Peer <mode> <name>}. Exits 0 when its part went as expected.
 *
 * <ul>
 *   <li>{@code echo}: connects, echoes plain messages until the server's end, exits.</li>
 *   <li>{@code send-and-exit}: connects, sends three messages, exits without closing.</li>
 *   <li>{@code connect-and-wait}: connects, sends "ready" and waits (the test kills it).</li>
 *   <li>{@code rpc-server}: listens, answers RPC requests with the payload reversed (status = the payload's length)
 *       until the client's end.</li>
 * </ul>
 */
public final class Peer {
    private static final Duration TIMEOUT = Duration.ofSeconds(20);

    private Peer() {
    }

    public static void main(String[] args) throws InterruptedException {
        String mode = args[0];
        String name = args[1];
        switch (mode) {
            case "echo" -> {
                try (Connection conn = Connection.connect(name, TIMEOUT)) {
                    for (;;) {
                        byte[] message;
                        try {
                            message = conn.receive(TIMEOUT);
                        } catch (FipcException e) {
                            if (e.result() == Result.DISCONNECTED) {
                                return;
                            }
                            throw e;
                        }
                        conn.send(message, TIMEOUT);
                    }
                }
            }
            case "send-and-exit" -> {
                Connection conn = Connection.connect(name, TIMEOUT);
                for (int i = 1; i <= 3; i++) {
                    conn.send(new byte[] {(byte) i}, TIMEOUT);
                }
                System.exit(0); // no close: the process's end is the connection's
            }
            case "connect-and-wait" -> {
                Connection conn = Connection.connect(name, TIMEOUT);
                conn.send("ready".getBytes(), TIMEOUT);
                Thread.sleep(60_000);
                System.exit(1); // the test kills it before
            }
            case "rpc-server" -> {
                try (Listener listener = Listener.listen(name, 1 << 16);
                     Connection conn = listener.accept(TIMEOUT)) {
                    for (;;) {
                        RpcMessage request;
                        try {
                            request = conn.rpcReceive(TIMEOUT);
                        } catch (FipcException e) {
                            if (e.result() == Result.DISCONNECTED) {
                                return;
                            }
                            throw e;
                        }
                        byte[] reply = request.payload().clone();
                        for (int i = 0; i < reply.length / 2; i++) {
                            byte b = reply[i];
                            reply[i] = reply[reply.length - 1 - i];
                            reply[reply.length - 1 - i] = b;
                        }
                        conn.rpcRespond(request.id(), request.opcode(), reply.length, reply, TIMEOUT);
                    }
                }
            }
            default -> throw new IllegalArgumentException("unknown mode " + mode + " " + Arrays.toString(args));
        }
    }
}
