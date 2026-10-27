"""
FastIPC: shared-memory messages between two processes (streams), broadcasts from one publisher to many
subscribers, work queues from many producers to many consumers, and services from many clients to a pool of servers,
with replies, over the C API of include/fipc.h.

    import fipc

    with fipc.stream_create("my_name", 1 << 20) as listener:      # the server, in one process
        conn = listener.accept()
    conn = fipc.stream_connect("my_name", timeout_ms=5000)        # the client, in another (or on another thread)

stream_connect waits for the server's accept, so a client and its server run on different threads or processes. A
broadcast: `broadcast_create` returns the publisher's Conn, and `broadcast_join` subscribes (SUBSCRIBER_LOSSY or
SUBSCRIBER_GATED). A work queue: `queue_create` returns a Queue, and `queue_join` joins it as a PRODUCER or a CONSUMER
(the owner too). A service: `service_create` returns a Service, and `service_join` joins it as a CLIENT, whose `call`
sends a request and returns its reply, or a SERVER (the owner too). The library admits a broadcast's, a queue's or a
service's joiners itself, in the owner's process.
"""

from fipc._fipc import (
    CLIENT,
    CONSUMER,
    FOREVER,
    NO_WAIT,
    PRODUCER,
    SERVER,
    SUBSCRIBER_GATED,
    SUBSCRIBER_LOSSY,
    Conn,
    FipcError,
    Listener,
    Queue,
    Result,
    Service,
    broadcast_create,
    broadcast_join,
    ffi,
    lib,
    queue_create,
    queue_join,
    result_str,
    service_create,
    service_join,
    stream_connect,
    stream_create,
)

__version__ = "1.0.0"
__all__ = [
    "CLIENT",
    "CONSUMER",
    "FOREVER",
    "NO_WAIT",
    "PRODUCER",
    "SERVER",
    "SUBSCRIBER_GATED",
    "SUBSCRIBER_LOSSY",
    "Conn",
    "FipcError",
    "Listener",
    "Queue",
    "Result",
    "Service",
    "broadcast_create",
    "broadcast_join",
    "ffi",
    "lib",
    "queue_create",
    "queue_join",
    "result_str",
    "service_create",
    "service_join",
    "stream_connect",
    "stream_create",
]
