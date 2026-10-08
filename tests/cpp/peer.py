"""The Python peer of the C++ wrapper's interop test (fipc_hpp_test.cpp): `peer.py client|server <name>
<bindings/python>`, the Python binding of this repository against the C++ process."""

import sys

sys.path.insert(0, sys.argv[3])  # the repository's bindings/python
from fipc import Conn, FipcError, Listener, Result, RPC_REQUEST  # noqa: E402

TIMEOUT_MS = 20000
mode, name = sys.argv[1], sys.argv[2]


def until_disconnected(call):
    """Calls `call` until the peer's end; FIPC_DISCONNECTED ends the loop, any other error fails."""
    try:
        while True:
            call()
    except FipcError as e:
        if e.result != Result.DISCONNECTED:
            raise


if mode == "client":
    # Calls the C++ server, then answers its call, then waits for its end
    with Conn.connect(name, timeout_ms=TIMEOUT_MS) as conn:
        request_id = conn.rpc_submit(7, b"hello from Python", timeout_ms=TIMEOUT_MS)
        reply = conn.rpc_recv(timeout_ms=TIMEOUT_MS)
        assert (reply.id, reply.status, reply.payload) == (request_id, 42, b"HELLO FROM PYTHON"), reply
        request = conn.rpc_recv(timeout_ms=TIMEOUT_MS)
        assert request.kind == RPC_REQUEST and request.opcode == 9, request
        conn.rpc_respond(request.id, 9, status=len(request.payload), data=request.payload * 2, timeout_ms=TIMEOUT_MS)
        until_disconnected(lambda: conn.rpc_recv(timeout_ms=TIMEOUT_MS))
else:
    # Answers each request with its payload in upper case and status -1, until the C++ client's end
    with Listener(name, 1 << 16) as listener, listener.accept(timeout_ms=TIMEOUT_MS) as conn:

        def answer():
            request = conn.rpc_recv(timeout_ms=TIMEOUT_MS)
            conn.rpc_respond(request.id, request.opcode, status=-1, data=request.payload.upper(), timeout_ms=TIMEOUT_MS)

        until_disconnected(answer)
