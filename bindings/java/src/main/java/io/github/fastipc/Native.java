package io.github.fastipc;

import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandle;

/**
 * The 18 functions of include/fipc.h as FFM downcalls, one to one (x86-64: size_t is a long, the enums and the
 * timeouts are ints, uint32_t values are ints with their bits). Handles are zero-length segments of the native
 * pointers. The calls that never wait and do little (fipc_max_piece, fipc_send_commit, fipc_recv_release) are
 * critical downcalls, without the thread-state transition of the others.
 */
final class Native {
    static final int OK = 0;
    static final int TOO_LARGE = 4;
    static final int INVALID = 5;

    /** Where the library came from (Fipc.libraryPath). */
    static final String LIBRARY;

    private static final MethodHandle RESULT_STR;
    private static final MethodHandle LISTEN;
    private static final MethodHandle ACCEPT;
    private static final MethodHandle LISTENER_CANCEL;
    private static final MethodHandle LISTENER_CLOSE;
    private static final MethodHandle CONNECT;
    private static final MethodHandle MAX_PIECE;
    private static final MethodHandle CANCEL;
    private static final MethodHandle CLOSE;
    private static final MethodHandle SEND;
    private static final MethodHandle RECV;
    private static final MethodHandle SEND_ACQUIRE;
    private static final MethodHandle SEND_COMMIT;
    private static final MethodHandle RECV_ACQUIRE;
    private static final MethodHandle RECV_RELEASE;
    private static final MethodHandle RPC_SUBMIT;
    private static final MethodHandle RPC_RESPOND;
    private static final MethodHandle RPC_RECV;

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
        LISTEN = bind.of("fipc_listen", FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_LONG, ADDRESS));
        ACCEPT = bind.of("fipc_accept", FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, JAVA_INT));
        LISTENER_CANCEL = bind.of("fipc_listener_cancel", FunctionDescriptor.ofVoid(ADDRESS));
        LISTENER_CLOSE = bind.of("fipc_listener_close", FunctionDescriptor.ofVoid(ADDRESS));
        CONNECT = bind.of("fipc_connect", FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, JAVA_INT));
        MAX_PIECE = bind.of("fipc_max_piece", FunctionDescriptor.of(JAVA_LONG, ADDRESS), critical);
        CANCEL = bind.of("fipc_cancel", FunctionDescriptor.ofVoid(ADDRESS));
        CLOSE = bind.of("fipc_close", FunctionDescriptor.ofVoid(ADDRESS));
        SEND = bind.of("fipc_send", FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, JAVA_LONG, JAVA_INT));
        RECV = bind.of("fipc_recv", FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, JAVA_LONG, ADDRESS, JAVA_INT));
        SEND_ACQUIRE = bind.of("fipc_send_acquire", FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_LONG, ADDRESS, JAVA_INT));
        SEND_COMMIT = bind.of("fipc_send_commit", FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_LONG), critical);
        RECV_ACQUIRE = bind.of("fipc_recv_acquire", FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, ADDRESS, JAVA_INT));
        RECV_RELEASE = bind.of("fipc_recv_release", FunctionDescriptor.ofVoid(ADDRESS), critical);
        RPC_SUBMIT = bind.of("fipc_rpc_submit",
            FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_INT, ADDRESS, JAVA_LONG, ADDRESS, JAVA_INT));
        RPC_RESPOND = bind.of("fipc_rpc_respond",
            FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_LONG, JAVA_INT, JAVA_INT, ADDRESS, JAVA_LONG, JAVA_INT));
        RPC_RECV = bind.of("fipc_rpc_recv", FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, JAVA_LONG, ADDRESS, JAVA_INT));
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

    static int listen(MemorySegment name, long capacity, MemorySegment outListener) {
        try {
            return (int) LISTEN.invokeExact(name, capacity, outListener);
        } catch (Throwable t) {
            throw unexpected(t);
        }
    }

    static int accept(MemorySegment listener, MemorySegment outConn, int timeoutMs) {
        try {
            return (int) ACCEPT.invokeExact(listener, outConn, timeoutMs);
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

    static int connect(MemorySegment name, MemorySegment outConn, int timeoutMs) {
        try {
            return (int) CONNECT.invokeExact(name, outConn, timeoutMs);
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

    static int rpcSubmit(MemorySegment conn, int opcode, MemorySegment data, long len, MemorySegment outId, int timeoutMs) {
        try {
            return (int) RPC_SUBMIT.invokeExact(conn, opcode, data, len, outId, timeoutMs);
        } catch (Throwable t) {
            throw unexpected(t);
        }
    }

    static int rpcRespond(MemorySegment conn, long id, int opcode, int status, MemorySegment data, long len, int timeoutMs) {
        try {
            return (int) RPC_RESPOND.invokeExact(conn, id, opcode, status, data, len, timeoutMs);
        } catch (Throwable t) {
            throw unexpected(t);
        }
    }

    static int rpcRecv(MemorySegment conn, MemorySegment buf, long bufLen, MemorySegment msg, int timeoutMs) {
        try {
            return (int) RPC_RECV.invokeExact(conn, buf, bufLen, msg, timeoutMs);
        } catch (Throwable t) {
            throw unexpected(t);
        }
    }
}
