package io.github.fastipc;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * The other process of the two-process tests: {@code Peer <mode> <name>}. Exits 0 when its part went as expected.
 *
 * <ul>
 *   <li>{@code echo}: connects, echoes messages until the server's end, exits.</li>
 *   <li>{@code send-and-exit}: connects, sends three messages, exits without closing.</li>
 *   <li>{@code connect-and-wait}: connects, sends "ready" and waits (the test kills it).</li>
 *   <li>{@code reverse-server}: creates a stream, answers each message with it reversed until the client's end.</li>
 *   <li>{@code publisher <name> <count> <subscribers>} and {@code subscriber <name> gated|lossy <count>}: a broadcast
 *       publisher and subscriber, as bindings/python/tests/broadcast_peer.py's ({@link #publish},
 *       {@link #subscribe}).</li>
 *   <li>{@code producer <name> <count> <consumers>}, {@code consumer <name> <count>},
 *       {@code owner-consumer <name> <count> <producers>} and {@code joined-producer <name> <count>}: a work queue's
 *       members, as bindings/python/tests/queue_peer.py's ({@link #produce}, {@link #consume}, {@link #ownAndConsume},
 *       {@link #joinAndProduce}).</li>
 *   <li>{@code service-server <name> <count> <clients>}, {@code service-client <name> <count>},
 *       {@code service-owner-client <name> <count> <servers>}, {@code service-joined-server <name>} and
 *       {@code service-holding-server <name>}: a service's members, as bindings/python/tests/service_peer.py's
 *       ({@link #serve}, {@link #joinAndCall}, {@link #ownAndCall}, {@link #joinAndAnswer}, {@link #joinAndHold}).</li>
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
                try (Connection conn = Fipc.connectStream(name, TIMEOUT)) {
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
                Connection conn = Fipc.connectStream(name, TIMEOUT);
                for (int i = 1; i <= 3; i++) {
                    conn.send(new byte[] {(byte) i}, TIMEOUT);
                }
                System.exit(0); // no close: the process's end is the connection's
            }
            case "connect-and-wait" -> {
                Connection conn = Fipc.connectStream(name, TIMEOUT);
                conn.send("ready".getBytes(), TIMEOUT);
                Thread.sleep(60_000);
                System.exit(1); // the test kills it before
            }
            case "reverse-server" -> {
                try (Listener listener = Fipc.createStream(name, 1 << 16);
                     Connection conn = listener.accept(TIMEOUT)) {
                    for (;;) {
                        byte[] reply;
                        try {
                            reply = conn.receive(TIMEOUT);
                        } catch (FipcException e) {
                            if (e.result() == Result.DISCONNECTED) {
                                return;
                            }
                            throw e;
                        }
                        for (int i = 0; i < reply.length / 2; i++) {
                            byte b = reply[i];
                            reply[i] = reply[reply.length - 1 - i];
                            reply[reply.length - 1 - i] = b;
                        }
                        conn.send(reply, TIMEOUT);
                    }
                }
            }
            case "publisher" -> publish(name, Integer.parseInt(args[2]), Integer.parseInt(args[3]));
            case "subscriber" -> subscribe(name, args[2].equals("gated"), Integer.parseInt(args[3]));
            case "producer" -> produce(name, Integer.parseInt(args[2]), Integer.parseInt(args[3]));
            case "consumer" -> consume(name, Integer.parseInt(args[2]));
            case "owner-consumer" -> ownAndConsume(name, Integer.parseInt(args[2]), Integer.parseInt(args[3]));
            case "joined-producer" -> joinAndProduce(name, Integer.parseInt(args[2]));
            case "service-server" -> serve(name, Integer.parseInt(args[2]), Integer.parseInt(args[3]));
            case "service-client" -> joinAndCall(name, Integer.parseInt(args[2]));
            case "service-owner-client" -> ownAndCall(name, Integer.parseInt(args[2]), Integer.parseInt(args[3]));
            case "service-joined-server" -> joinAndAnswer(name);
            case "service-holding-server" -> joinAndHold(name);
            default -> throw new IllegalArgumentException("unknown mode " + mode + " " + Arrays.toString(args));
        }
    }

    /** A broadcast message: message {@code i} is the UTF-8 text {@code message <i>}, as broadcast_peer.py's. */
    static byte[] message(long i) {
        return ("message " + i).getBytes(UTF_8);
    }

    /**
     * A broadcast's publisher: claims {@code name} for a broadcast of 4096 bytes and up to 8 subscribers, waits for
     * {@code subscribers} to join, sends {@code count} messages and closes, which ends the broadcast.
     */
    static void publish(String name, int count, int subscribers) {
        try (Connection publisher = Fipc.createBroadcast(name, 4096, 8, TIMEOUT)) {
            int joined = publisher.waitForMembers(subscribers, TIMEOUT);
            check(joined >= subscribers, joined + " subscribers joined, expected " + subscribers);
            for (int i = 0; i < count; i++) {
                publisher.send(message(i), TIMEOUT);
            }
        }
    }

    /**
     * A broadcast's subscriber: joins {@code name} and receives until DISCONNECTED. A gated one checks that it
     * received every message in order; a lossy one that each message it received is the one after the last, or that
     * many after it as an OVERRUN between them reported lost, and that received and lost add up to {@code count}.
     */
    static void subscribe(String name, boolean gated, int count) {
        String role = gated ? "gated" : "lossy";
        long expected = 0; // the index of the next message
        long received = 0;
        long lost = 0;
        try (Connection subscriber = Fipc.joinBroadcast(name, gated ? SubscriberRole.GATED : SubscriberRole.LOSSY,
                 TIMEOUT)) {
            for (;;) {
                byte[] data;
                try {
                    data = subscriber.receive(TIMEOUT);
                } catch (FipcException e) {
                    if (e.result() == Result.DISCONNECTED) {
                        break;
                    }
                    if (e.result() != Result.OVERRUN || gated) {
                        throw e;
                    }
                    check(e.lostMessages() >= 1, role + ": " + e.getMessage());
                    lost += e.lostMessages();
                    expected += e.lostMessages();
                    continue;
                }
                check(Arrays.equals(data, message(expected)),
                    role + ": received " + new String(data, UTF_8) + ", expected message " + expected);
                received++;
                expected++;
            }
        }
        check(expected == count, role + ": received " + received + " and lost " + lost + " of " + count + " messages");
        check(!gated || received == count, "a gated subscriber lost messages");
    }

    /** A work queue's message: job {@code i} is the UTF-8 text {@code job <i>}, as queue_peer.py's. */
    static byte[] job(long i) {
        return ("job " + i).getBytes(UTF_8);
    }

    private static void sendJobs(Connection producer, int count) {
        for (int i = 1; i <= count; i++) {
            producer.send(job(i), TIMEOUT);
        }
    }

    /**
     * That {@code received} is the jobs of {@code producers} producers, each sending job 1 to job {@code count}, in any
     * order (the queue promises none): each job {@code producers} times.
     */
    static void checkJobs(List<byte[]> received, int producers, int count) {
        Map<String, Integer> seen = new TreeMap<>();
        for (byte[] data : received) {
            seen.merge(new String(data, UTF_8), 1, Integer::sum);
        }
        Map<String, Integer> expected = new TreeMap<>();
        for (int i = 1; i <= count; i++) {
            expected.put(new String(job(i), UTF_8), producers);
        }
        check(seen.equals(expected), "received " + received.size() + " of " + producers * count + " jobs, or a job "
            + "twice: " + seen);
    }

    /**
     * A work queue's owner and its producer, as the examples' producer: claims {@code name} for a queue of rings of
     * 4096 bytes and up to 8 producers and 8 consumers, joins it as a producer, sends job 1 to job {@code count}, waits
     * for {@code consumers} consumers to have joined, and closes its producer and the queue, which ends it.
     */
    static void produce(String name, int count, int consumers) {
        try (WorkQueue queue = Fipc.createQueue(name, 4096, 8, 8)) {
            try (Connection producer = Fipc.joinQueue(name, QueueRole.PRODUCER, TIMEOUT)) {
                sendJobs(producer, count);
                int joined = queue.waitForMembers(QueueRole.CONSUMER, consumers, TIMEOUT);
                check(joined >= consumers, joined + " consumers joined, expected " + consumers);
            }
        }
    }

    /**
     * A work queue's consumer: joins {@code name}, receives until DISCONNECTED, and checks that it received job 1 to
     * {@code count}, each once.
     */
    static void consume(String name, int count) {
        List<byte[]> received = new ArrayList<>();
        try (Connection consumer = Fipc.joinQueue(name, QueueRole.CONSUMER, TIMEOUT)) {
            for (;;) {
                try {
                    received.add(consumer.receive(TIMEOUT));
                } catch (FipcException e) {
                    if (e.result() != Result.DISCONNECTED) {
                        throw e;
                    }
                    break;
                }
            }
        }
        checkJobs(received, 1, count);
    }

    /**
     * A work queue's owner and its consumer: claims {@code name} as {@link #produce} does, joins it as a consumer,
     * receives {@code count} jobs from each of {@code producers} producers and checks that each job of each producer
     * came once, then closes its consumer and the queue, which ends it.
     */
    static void ownAndConsume(String name, int count, int producers) {
        List<byte[]> received = new ArrayList<>();
        try (WorkQueue _ = Fipc.createQueue(name, 4096, 8, 8);
             Connection consumer = Fipc.joinQueue(name, QueueRole.CONSUMER, TIMEOUT)) {
            for (int i = 0; i < producers * count; i++) {
                received.add(consumer.receive(TIMEOUT));
            }
        }
        checkJobs(received, producers, count);
    }

    /** A work queue's joined producer: sends job 1 to {@code count} and closes; the queue goes on. */
    static void joinAndProduce(String name, int count) {
        try (Connection producer = Fipc.joinQueue(name, QueueRole.PRODUCER, TIMEOUT)) {
            sendJobs(producer, count);
        }
    }

    /** A service's request: request {@code i} is the UTF-8 text {@code ping <i>}, as service_peer.py's. */
    static byte[] request(long i) {
        return ("ping " + i).getBytes(UTF_8);
    }

    /** {@code text}'s uppercase, as the servers answer: ASCII only, as the requests are. */
    static byte[] upper(byte[] text) {
        return new String(text, UTF_8).toUpperCase(java.util.Locale.ROOT).getBytes(UTF_8);
    }

    /** The peers' service on {@code name}, as service_peer.py's: rings of 4096 bytes, 256-byte messages, 8 and 8. */
    static Service createService(String name) {
        return Fipc.createService(name, 4096, 256, 8, 8);
    }

    /** A service's server: answers {@code count} requests, each with its uppercase. */
    static void answer(Connection server, int count) {
        for (int i = 0; i < count; i++) {
            server.send(upper(server.receive(TIMEOUT)), Fipc.NO_WAIT);
        }
    }

    /** A service's server: answers requests until the service ends, and returns how many it answered. */
    static int answerUntilTheEnd(Connection server) {
        int answered = 0;
        for (;;) {
            try {
                server.send(upper(server.receive(TIMEOUT)), Fipc.NO_WAIT);
            } catch (FipcException e) { // the end, at a receive or between a request and its reply
                if (e.result() != Result.DISCONNECTED) {
                    throw e;
                }
                return answered;
            }
            answered++;
        }
    }

    /** A service's client: calls ping 1 to ping {@code count}, one at a time, and checks each reply. */
    static void call(Connection client, int count) {
        for (int i = 1; i <= count; i++) {
            byte[] reply = client.call(request(i), TIMEOUT);
            check(Arrays.equals(reply, upper(request(i))),
                "the reply to ping " + i + " is " + new String(reply, UTF_8));
        }
    }

    /**
     * A service's owner and its server, as the examples' server: claims {@code name} for a service of rings of 4096
     * bytes, requests and replies of up to 256 bytes and up to 8 clients and 8 servers, joins it as a server, answers
     * {@code count} requests from each of {@code clients} clients and closes its server and the service, which ends it.
     */
    static void serve(String name, int count, int clients) {
        try (Service _ = createService(name);
             Connection server = Fipc.joinService(name, ServiceRole.SERVER, TIMEOUT)) {
            answer(server, count * clients);
        }
    }

    /** A service's client: joins {@code name}, calls {@code count} times and checks each reply, then closes. */
    static void joinAndCall(String name, int count) {
        try (Connection client = Fipc.joinService(name, ServiceRole.CLIENT, TIMEOUT)) {
            call(client, count);
        }
    }

    /**
     * A service's owner and its client: claims {@code name} as {@link #serve} does, joins it as a client, waits for
     * {@code servers} servers to have joined, calls {@code count} times, checks each reply, and closes its client and
     * the service, which ends it.
     */
    static void ownAndCall(String name, int count, int servers) {
        try (Service service = createService(name);
             Connection client = Fipc.joinService(name, ServiceRole.CLIENT, TIMEOUT)) {
            int joined = service.waitForMembers(ServiceRole.SERVER, servers, TIMEOUT);
            check(joined >= servers, joined + " servers joined, expected " + servers);
            call(client, count);
        }
    }

    /** A service's joined server: answers requests until the service's end. */
    static void joinAndAnswer(String name) {
        try (Connection server = Fipc.joinService(name, ServiceRole.SERVER, TIMEOUT)) {
            answerUntilTheEnd(server);
        }
    }

    /**
     * A service's joined server that takes one request, prints {@code holding} and waits, never replying, until it is
     * killed: its client's receive throws ABANDONED once the process has ended.
     */
    static void joinAndHold(String name) throws InterruptedException {
        Connection server = Fipc.joinService(name, ServiceRole.SERVER, TIMEOUT);
        server.receive(TIMEOUT);
        System.out.println("holding");
        System.out.flush();
        Thread.sleep(60_000);
        java.lang.ref.Reference.reachabilityFence(server); // no cleaner closes it meanwhile
        System.exit(1); // the test kills it before
    }

    private static void check(boolean condition, String failure) {
        if (!condition) {
            throw new AssertionError(failure);
        }
    }
}
