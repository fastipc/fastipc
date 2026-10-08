package io.github.fastipc;

import java.util.Objects;

/**
 * A received RPC message: a request or a response ({@code fipc_rpc_msg_t} and its payload).
 *
 * @param id      the request's id, echoed in its response; a connection numbers its requests from 1
 * @param kind    {@link RpcKind#REQUEST} or {@link RpcKind#RESPONSE}
 * @param opcode  the application's; an unsigned 32-bit value ({@link Integer#toUnsignedLong(int)} reads it as one)
 * @param status  the application's; 0 in requests
 * @param payload the payload, possibly empty; the message's own array
 */
public record RpcMessage(long id, RpcKind kind, int opcode, int status, byte[] payload) {
    /** Checks the components: the kind and the payload aren't null. */
    public RpcMessage {
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(payload, "payload");
    }

    /** {@return whether this is a request} Answer it with {@link Connection#rpcRespond(long, int, int, byte[])}. */
    public boolean isRequest() {
        return kind == RpcKind.REQUEST;
    }

    /** {@return whether this is a response} It answers the request whose id is {@link #id()}. */
    public boolean isResponse() {
        return kind == RpcKind.RESPONSE;
    }

    /** The message's header and the payload's length (not its bytes). */
    @Override
    public String toString() {
        return "RpcMessage[id=" + id + ", kind=" + kind + ", opcode=" + Integer.toUnsignedString(opcode)
            + ", status=" + status + ", payload=" + payload.length + " bytes]";
    }
}
