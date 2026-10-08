"""
FastIPC: shared-memory messages and RPC between two processes, over the C API of include/fipc.h.

    from fipc import Listener, Conn

    with Listener("my_name", 1 << 20) as listener:      # the server, in one process
        conn = listener.accept()
    conn = Conn.connect("my_name", timeout_ms=5000)     # the client, in another (or on another thread)

connect waits for the server's accept, so a client and its server run on different threads or processes.
"""

from fipc._fipc import (
    FOREVER,
    NO_WAIT,
    RPC_REQUEST,
    RPC_RESPONSE,
    Conn,
    FipcError,
    Listener,
    Result,
    RpcMessage,
    ffi,
    lib,
    result_str,
)

__version__ = "1.0.0"
__all__ = [
    "FOREVER",
    "NO_WAIT",
    "RPC_REQUEST",
    "RPC_RESPONSE",
    "Conn",
    "FipcError",
    "Listener",
    "Result",
    "RpcMessage",
    "ffi",
    "lib",
    "result_str",
]
