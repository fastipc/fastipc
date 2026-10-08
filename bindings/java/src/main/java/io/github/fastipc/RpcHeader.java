package io.github.fastipc;

import java.util.Objects;

/**
 * A received RPC message's header ({@code fipc_rpc_msg_t}), from a receive into the caller's buffer
 * ({@link Connection#rpcReceive(java.lang.foreign.MemorySegment, java.time.Duration)}), which holds the payload.
 *
 * @param id     the request's id, echoed in its response; a connection numbers its requests from 1
 * @param kind   {@link RpcKind#REQUEST} or {@link RpcKind#RESPONSE}
 * @param opcode the application's; an unsigned 32-bit value ({@link Integer#toUnsignedLong(int)} reads it as one)
 * @param status the application's; 0 in requests
 * @param length the payload's length, in the caller's buffer from its start
 */
public record RpcHeader(long id, RpcKind kind, int opcode, int status, long length) {
    /** Checks the components: the kind isn't null. */
    public RpcHeader {
        Objects.requireNonNull(kind, "kind");
    }

    /** {@return whether this is a request} Answer it with {@link Connection#rpcRespond(long, int, int, byte[])}. */
    public boolean isRequest() {
        return kind == RpcKind.REQUEST;
    }

    /** {@return whether this is a response} It answers the request whose id is {@link #id()}. */
    public boolean isResponse() {
        return kind == RpcKind.RESPONSE;
    }
}
