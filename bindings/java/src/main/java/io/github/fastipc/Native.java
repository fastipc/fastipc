package io.github.fastipc;

import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandle;

/**
 * Every function of include/fipc.h as an FFM downcall, one to one (x86-64 and aarch64: size_t is a long, the enums,
 * the roles and the timeouts are ints, uint32_t values are ints with their bits). Handles are zero-length segments of
 * the native pointers. The calls that never wait and do little (fipc_max_piece, fipc_send_commit,
 * fipc_recv_release) are critical downcalls, without the thread-state transition of the others.
 */
final class Native {
    static final int OK = 0;
    static final int TIMEOUT = 1;
    static final int DISCONNECTED = 2;
    static final int CANCELLED = 3;
    static final int TOO_LARGE = 4;
    static final int INVALID = 5;
    static final int OVERRUN = 8;
    static final int ABANDONED = 10;

    /** Where the library came from (Fipc.libraryPath). */
    static final String LIBRARY;

    private static final MethodHandle RESULT_STR;
    private static final MethodHandle STREAM_CREATE;
    private static final MethodHandle STREAM_ACCEPT;
    private static final MethodHandle STREAM_CONNECT;
    private static final MethodHandle LISTENER_CANCEL;
    private static final MethodHandle LISTENER_CLOSE;
    private static final MethodHandle MAX_PIECE;
    private static final MethodHandle CANCEL;
    private static final MethodHandle CLOSE;
    private static final MethodHandle SEND;
    private static final MethodHandle RECV;
    private static final MethodHandle SEND_MANY;
    private static final MethodHandle RECV_MANY;
    private static final MethodHandle SEND_ACQUIRE;
    private static final MethodHandle SEND_COMMIT;
    private static final MethodHandle RECV_ACQUIRE;
    private static final MethodHandle RECV_RELEASE;
    private static final MethodHandle BROADCAST_CREATE;
    private static final MethodHandle BROADCAST_JOIN;
    private static final MethodHandle BROADCAST_WAIT_FOR_MEMBERS;
    private static final MethodHandle QUEUE_CREATE;
    private static final MethodHandle QUEUE_JOIN;
    private static final MethodHandle QUEUE_WAIT_FOR_MEMBERS;
    private static final MethodHandle QUEUE_CANCEL;
    private static final MethodHandle QUEUE_CLOSE;
    private static final MethodHandle SERVICE_CREATE;
    private static final MethodHandle SERVICE_JOIN;
    private static final MethodHandle SERVICE_WAIT_FOR_MEMBERS;
    private static final MethodHandle SERVICE_CANCEL;
    private static final MethodHandle SERVICE_CLOSE;

    static {
        NativeLoader.Loaded library = NativeLoader.load();
        LIBRARY = library.description();
        Linker linker = Linker.nativeLinker();
        Linker.Option critical = Linker.Option.critical(false);
        class Bind {
            MethodHandle of(String name, FunctionDescriptor descriptor, Linker.Option... options) {
                MemorySegment symbol = library.symbols().find(name)
                    .orElseThrow(() -> new UnsatisfiedLinkError(name + " not found in " + LIBRARY));
                return linker.downcallHandle(symbol, descriptor, options);
            }
        }
        Bind bind = new Bind();
        RESULT_STR = bind.of("fipc_result_str", FunctionDescriptor.of(ADDRESS, JAVA_INT));
        STREAM_CREATE = bind.of("fipc_stream_create", FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_LONG, ADDRESS));
        STREAM_ACCEPT = bind.of("fipc_stream_accept", FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, JAVA_INT));
        STREAM_CONNECT = bind.of("fipc_stream_connect", FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, JAVA_INT));
        LISTENER_CANCEL = bind.of("fipc_listener_cancel", FunctionDescriptor.ofVoid(ADDRESS));
        LISTENER_CLOSE = bind.of("fipc_listener_close", FunctionDescriptor.ofVoid(ADDRESS));
        MAX_PIECE = bind.of("fipc_max_piece", FunctionDescriptor.of(JAVA_LONG, ADDRESS), critical);
        CANCEL = bind.of("fipc_cancel", FunctionDescriptor.ofVoid(ADDRESS));
        CLOSE = bind.of("fipc_close", FunctionDescriptor.ofVoid(ADDRESS));
        SEND = bind.of("fipc_send", FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, JAVA_LONG, JAVA_INT));
        RECV = bind.of("fipc_recv", FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, JAVA_LONG, ADDRESS, JAVA_INT));
        SEND_MANY = bind.of("fipc_send_many",
            FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, ADDRESS, JAVA_LONG, ADDRESS, JAVA_INT));
        RECV_MANY = bind.of("fipc_recv_many",
            FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, JAVA_LONG, ADDRESS, JAVA_LONG, ADDRESS, JAVA_INT));
        SEND_ACQUIRE = bind.of("fipc_send_acquire", FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_LONG, ADDRESS, JAVA_INT));
        SEND_COMMIT = bind.of("fipc_send_commit", FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_LONG), critical);
        RECV_ACQUIRE = bind.of("fipc_recv_acquire", FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, ADDRESS, JAVA_INT));
        RECV_RELEASE = bind.of("fipc_recv_release", FunctionDescriptor.ofVoid(ADDRESS), critical);
        BROADCAST_CREATE = bind.of("fipc_broadcast_create",
            FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_LONG, JAVA_INT, JAVA_INT, ADDRESS));
        BROADCAST_JOIN = bind.of("fipc_broadcast_join",
            FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_INT, ADDRESS, JAVA_INT));
        BROADCAST_WAIT_FOR_MEMBERS = bind.of("fipc_broadcast_wait_for_members",
            FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_INT, ADDRESS, JAVA_INT));
        QUEUE_CREATE = bind.of("fipc_queue_create",
            FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_LONG, JAVA_INT, JAVA_INT, ADDRESS));
        QUEUE_JOIN = bind.of("fipc_queue_join", FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_INT, ADDRESS, JAVA_INT));
        QUEUE_WAIT_FOR_MEMBERS = bind.of("fipc_queue_wait_for_members",
            FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_INT, JAVA_INT, ADDRESS, JAVA_INT));
        QUEUE_CANCEL = bind.of("fipc_queue_cancel", FunctionDescriptor.ofVoid(ADDRESS));
        QUEUE_CLOSE = bind.of("fipc_queue_close", FunctionDescriptor.ofVoid(ADDRESS));
        SERVICE_CREATE = bind.of("fipc_service_create",
            FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_LONG, JAVA_LONG, JAVA_INT, JAVA_INT, ADDRESS));
        SERVICE_JOIN = bind.of("fipc_service_join",
            FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_INT, ADDRESS, JAVA_INT));
        SERVICE_WAIT_FOR_MEMBERS = bind.of("fipc_service_wait_for_members",
            FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_INT, JAVA_INT, ADDRESS, JAVA_INT));
        SERVICE_CANCEL = bind.of("fipc_service_cancel", FunctionDescriptor.ofVoid(ADDRESS));
        SERVICE_CLOSE = bind.of("fipc_service_close", FunctionDescriptor.ofVoid(ADDRESS));
    }

    private Native() {
    }

    /** A downcall's exception, as an unchecked one (a downcall throws only for a bad argument, such as a closed segment). */
    private static RuntimeException unexpected(Throwable t) {
        if (t instanceof RuntimeException e) {
            return e;
        }
        if (t instanceof Error e) {
            throw e;
        }
        return new IllegalStateException("a downcall threw", t);
    }

    /** fipc_result_str: a static C string. */
    static String resultStr(int result) {
        try {
            MemorySegment text = (MemorySegment) RESULT_STR.invokeExact(result);
            return text.reinterpret(64).getString(0);
        } catch (Throwable t) {
            throw unexpected(t);
        }
    }

    static int streamCreate(MemorySegment name, long capacity, MemorySegment outListener) {
        try {
            return (int) STREAM_CREATE.invokeExact(name, capacity, outListener);
        } catch (Throwable t) {
            throw unexpected(t);
        }
    }

    static int streamAccept(MemorySegment listener, MemorySegment outConn, int timeoutMs) {
        try {
            return (int) STREAM_ACCEPT.invokeExact(listener, outConn, timeoutMs);
        } catch (Throwable t) {
            throw unexpected(t);
        }
    }

    static void listenerCancel(MemorySegment listener) {
        try {
            LISTENER_CANCEL.invokeExact(listener);
        } catch (Throwable t) {
            throw unexpected(t);
        }
    }

    static void listenerClose(MemorySegment listener) {
        try {
            LISTENER_CLOSE.invokeExact(listener);
        } catch (Throwable t) {
            throw unexpected(t);
        }
    }

    static int streamConnect(MemorySegment name, MemorySegment outConn, int timeoutMs) {
        try {
            return (int) STREAM_CONNECT.invokeExact(name, outConn, timeoutMs);
        } catch (Throwable t) {
            throw unexpected(t);
        }
    }

    static long maxPiece(MemorySegment conn) {
        try {
            return (long) MAX_PIECE.invokeExact(conn);
        } catch (Throwable t) {
            throw unexpected(t);
        }
    }

    static void cancel(MemorySegment conn) {
        try {
            CANCEL.invokeExact(conn);
        } catch (Throwable t) {
            throw unexpected(t);
        }
    }

    static void close(MemorySegment conn) {
        try {
            CLOSE.invokeExact(conn);
        } catch (Throwable t) {
            throw unexpected(t);
        }
    }

    static int send(MemorySegment conn, MemorySegment data, long len, int timeoutMs) {
        try {
            return (int) SEND.invokeExact(conn, data, len, timeoutMs);
        } catch (Throwable t) {
            throw unexpected(t);
        }
    }

    static int recv(MemorySegment conn, MemorySegment buf, long bufLen, MemorySegment outLen, int timeoutMs) {
        try {
            return (int) RECV.invokeExact(conn, buf, bufLen, outLen, timeoutMs);
        } catch (Throwable t) {
            throw unexpected(t);
        }
    }

    static int sendMany(MemorySegment conn, MemorySegment datas, MemorySegment lens, long count, MemorySegment outSent,
                        int timeoutMs) {
        try {
            return (int) SEND_MANY.invokeExact(conn, datas, lens, count, outSent, timeoutMs);
        } catch (Throwable t) {
            throw unexpected(t);
        }
    }

    static int recvMany(MemorySegment conn, MemorySegment buf, long bufLen, MemorySegment lens, long maxCount,
                        MemorySegment outCount, int timeoutMs) {
        try {
            return (int) RECV_MANY.invokeExact(conn, buf, bufLen, lens, maxCount, outCount, timeoutMs);
        } catch (Throwable t) {
            throw unexpected(t);
        }
    }

    static int sendAcquire(MemorySegment conn, long len, MemorySegment outBuf, int timeoutMs) {
        try {
            return (int) SEND_ACQUIRE.invokeExact(conn, len, outBuf, timeoutMs);
        } catch (Throwable t) {
            throw unexpected(t);
        }
    }

    static int sendCommit(MemorySegment conn, long len) {
        try {
            return (int) SEND_COMMIT.invokeExact(conn, len);
        } catch (Throwable t) {
            throw unexpected(t);
        }
    }

    static int recvAcquire(MemorySegment conn, MemorySegment outData, MemorySegment outLen, int timeoutMs) {
        try {
            return (int) RECV_ACQUIRE.invokeExact(conn, outData, outLen, timeoutMs);
        } catch (Throwable t) {
            throw unexpected(t);
        }
    }

    static void recvRelease(MemorySegment conn) {
        try {
            RECV_RELEASE.invokeExact(conn);
        } catch (Throwable t) {
            throw unexpected(t);
        }
    }

    static int broadcastCreate(MemorySegment name, long capacity, int maxSubscribers, int maxStallMs,
                               MemorySegment outPublisher) {
        try {
            return (int) BROADCAST_CREATE.invokeExact(name, capacity, maxSubscribers, maxStallMs, outPublisher);
        } catch (Throwable t) {
            throw unexpected(t);
        }
    }

    static int broadcastJoin(MemorySegment name, int role, MemorySegment outConn, int timeoutMs) {
        try {
            return (int) BROADCAST_JOIN.invokeExact(name, role, outConn, timeoutMs);
        } catch (Throwable t) {
            throw unexpected(t);
        }
    }

    static int broadcastWaitForMembers(MemorySegment publisher, int atLeast, MemorySegment outCount, int timeoutMs) {
        try {
            return (int) BROADCAST_WAIT_FOR_MEMBERS.invokeExact(publisher, atLeast, outCount, timeoutMs);
        } catch (Throwable t) {
            throw unexpected(t);
        }
    }

    static int queueCreate(MemorySegment name, long capacity, int maxProducers, int maxConsumers,
                           MemorySegment outQueue) {
        try {
            return (int) QUEUE_CREATE.invokeExact(name, capacity, maxProducers, maxConsumers, outQueue);
        } catch (Throwable t) {
            throw unexpected(t);
        }
    }

    static int queueJoin(MemorySegment name, int role, MemorySegment outConn, int timeoutMs) {
        try {
            return (int) QUEUE_JOIN.invokeExact(name, role, outConn, timeoutMs);
        } catch (Throwable t) {
            throw unexpected(t);
        }
    }

    static int queueWaitForMembers(MemorySegment queue, int role, int atLeast, MemorySegment outCount,
                                   int timeoutMs) {
        try {
            return (int) QUEUE_WAIT_FOR_MEMBERS.invokeExact(queue, role, atLeast, outCount, timeoutMs);
        } catch (Throwable t) {
            throw unexpected(t);
        }
    }

    static void queueCancel(MemorySegment queue) {
        try {
            QUEUE_CANCEL.invokeExact(queue);
        } catch (Throwable t) {
            throw unexpected(t);
        }
    }

    static void queueClose(MemorySegment queue) {
        try {
            QUEUE_CLOSE.invokeExact(queue);
        } catch (Throwable t) {
            throw unexpected(t);
        }
    }

    static int serviceCreate(MemorySegment name, long capacity, long maxMessage, int maxClients, int maxServers,
                             MemorySegment outService) {
        try {
            return (int) SERVICE_CREATE.invokeExact(name, capacity, maxMessage, maxClients, maxServers, outService);
        } catch (Throwable t) {
            throw unexpected(t);
        }
    }

    static int serviceJoin(MemorySegment name, int role, MemorySegment outConn, int timeoutMs) {
        try {
            return (int) SERVICE_JOIN.invokeExact(name, role, outConn, timeoutMs);
        } catch (Throwable t) {
            throw unexpected(t);
        }
    }

    static int serviceWaitForMembers(MemorySegment service, int role, int atLeast, MemorySegment outCount,
                                     int timeoutMs) {
        try {
            return (int) SERVICE_WAIT_FOR_MEMBERS.invokeExact(service, role, atLeast, outCount, timeoutMs);
        } catch (Throwable t) {
            throw unexpected(t);
        }
    }

    static void serviceCancel(MemorySegment service) {
        try {
            SERVICE_CANCEL.invokeExact(service);
        } catch (Throwable t) {
            throw unexpected(t);
        }
    }

    static void serviceClose(MemorySegment service) {
        try {
            SERVICE_CLOSE.invokeExact(service);
        } catch (Throwable t) {
            throw unexpected(t);
        }
    }
}
