package io.github.fastipc;

/** An RPC message's kind: {@code FIPC_RPC_REQUEST} or {@code FIPC_RPC_RESPONSE}. */
public enum RpcKind {
    /** A request, sent by {@link Connection#rpcSubmit(int, byte[])}. */
    REQUEST(1),
    /** A response, sent by {@link Connection#rpcRespond(long, int, int, byte[])}. */
    RESPONSE(2);

    private final int code;

    RpcKind(int code) {
        this.code = code;
    }

    /** {@return the C value: 1 for a request, 2 for a response} */
    public int code() {
        return code;
    }

    static RpcKind of(int code) {
        return switch (code) {
            case 1 -> REQUEST;
            case 2 -> RESPONSE;
            default -> throw new FipcException(Result.INVALID.code(), "fipc_rpc_recv");
        };
    }
}
