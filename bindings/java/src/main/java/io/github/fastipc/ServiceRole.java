package io.github.fastipc;

/**
 * A service's member: how one joins a service ({@link Fipc#joinService(String, ServiceRole, java.time.Duration)}),
 * and which members a count takes ({@link Service#waitForMembers(ServiceRole, int, java.time.Duration)}).
 */
public enum ServiceRole {
    /**
     * {@code FIPC_CLIENT}: sends requests ({@code send}, or in place with {@code sendAcquire}/{@code sendCommit}) into
     * a ring of its own, which holds the room for their replies too, and receives the replies ({@code receive}, or in
     * place with {@code receiveAcquire}/{@code receiveRelease}) in the order of its requests: each receive returns the
     * reply to its oldest request whose reply it hasn't received yet, or throws {@link Result#ABANDONED} if that
     * request's server ended before it replied. {@link Connection#call(byte[], java.time.Duration)} does both. Its
     * close leaves the service: its requests that no server took are dropped.
     */
    CLIENT(5),
    /**
     * {@code FIPC_SERVER}: takes the next request of any client ({@code receive}, or in place with
     * {@code receiveAcquire}) and holds it until its next send ({@code send}, or
     * {@code sendAcquire}/{@code sendCommit}), which replies to it and never waits. While it holds a request its
     * receives throw {@link Result#INVALID}, and with none held its sends do. Its close leaves the service; a request
     * it still holds is answered {@link Result#ABANDONED}.
     */
    SERVER(6);

    private final int code;

    ServiceRole(int code) {
        this.code = code;
    }

    /** The C value: fipc_service_join's and fipc_service_wait_for_members' role. */
    int code() {
        return code;
    }
}
