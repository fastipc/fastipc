"""
The FastIPC C API (include/fipc.h) for Python, through CFFI's ABI mode: the shared library is loaded at run time,
with no build step.

A stream's server creates a name (`stream_create`) and accepts one client at a time; a client connects to the name
(`stream_connect`). A broadcast's publisher (`broadcast_create`) sends every message to each of its subscribers
(`broadcast_join`). A work queue's owner creates it (`queue_create`), and its producers and consumers join it
(`queue_join`, the owner too): each message a producer sends goes to one consumer. A service's owner creates it
(`service_create`), and its clients and servers join it (`service_join`, the owner too): each request a client sends
goes to one server, whose reply goes back to that client. The library admits a broadcast's, a queue's or a service's
joiners itself, in the owner's process. Timeouts are int milliseconds: NO_WAIT (0) doesn't wait, FOREVER
(-1) waits for ever. A call that fails raises FipcError, whose `result` is the library's Result (Result.TIMEOUT when the
timeout ran out). The header's rules hold: on a connection, one thread at a time sends and one receives; `cancel` may
be called from any thread; `close` only when no other thread is inside a call on the handle (cancel, join the
threads, close). A Conn or a Listener left unclosed is closed when it is garbage-collected; a Queue and a Service never
are: they live until their close or the end of the process.
"""

from __future__ import annotations

import os
import platform
import time
from collections import deque
from enum import IntEnum
from pathlib import Path
from typing import Any, Deque, List, Optional, Sequence, Union

from cffi import FFI

ffi = FFI()
ffi.cdef("""
    typedef struct fipc_listener fipc_listener_t;
    typedef struct fipc_conn fipc_conn_t;
    typedef struct fipc_queue fipc_queue_t;
    typedef struct fipc_service fipc_service_t;
    typedef int fipc_result_t;

    const char* fipc_result_str(fipc_result_t result);

    fipc_result_t fipc_stream_create(const char* name, size_t capacity, fipc_listener_t** out_listener);
    fipc_result_t fipc_stream_accept(fipc_listener_t* listener, fipc_conn_t** out_conn, int timeout_ms);
    void fipc_listener_cancel(fipc_listener_t* listener);
    void fipc_listener_close(fipc_listener_t* listener);
    fipc_result_t fipc_stream_connect(const char* name, fipc_conn_t** out_conn, int timeout_ms);
    size_t fipc_max_piece(const fipc_conn_t* conn);
    void fipc_cancel(fipc_conn_t* conn);
    void fipc_close(fipc_conn_t* conn);

    fipc_result_t fipc_send(fipc_conn_t* conn, const void* data, size_t len, int timeout_ms);
    fipc_result_t fipc_recv(fipc_conn_t* conn, void* buf, size_t buf_len, size_t* out_len, int timeout_ms);
    fipc_result_t fipc_send_acquire(fipc_conn_t* conn, size_t len, void** out_buf, int timeout_ms);
    fipc_result_t fipc_send_commit(fipc_conn_t* conn, size_t len);
    fipc_result_t fipc_recv_acquire(fipc_conn_t* conn, const void** out_data, size_t* out_len, int timeout_ms);
    void fipc_recv_release(fipc_conn_t* conn);
    fipc_result_t fipc_send_many(fipc_conn_t* conn, const void* const* datas, const size_t* lens, size_t count,
                                 size_t* out_sent, int timeout_ms);
    fipc_result_t fipc_recv_many(fipc_conn_t* conn, void* buf, size_t buf_len, size_t* lens, size_t max_count,
                                 size_t* out_count, int timeout_ms);

    fipc_result_t fipc_broadcast_create(const char* name, size_t capacity, uint32_t max_subscribers, int max_stall_ms,
                                        fipc_conn_t** out_publisher);
    fipc_result_t fipc_broadcast_join(const char* name, int role, fipc_conn_t** out_conn, int timeout_ms);
    fipc_result_t fipc_broadcast_wait_for_members(fipc_conn_t* publisher, uint32_t at_least, uint32_t* out_count,
                                                  int timeout_ms);

    fipc_result_t fipc_queue_create(const char* name, size_t capacity, uint32_t max_producers, uint32_t max_consumers,
                                    fipc_queue_t** out_queue);
    fipc_result_t fipc_queue_join(const char* name, int role, fipc_conn_t** out_conn, int timeout_ms);
    fipc_result_t fipc_queue_wait_for_members(fipc_queue_t* queue, int role, uint32_t at_least, uint32_t* out_count,
                                              int timeout_ms);
    void fipc_queue_cancel(fipc_queue_t* queue);
    void fipc_queue_close(fipc_queue_t* queue);

    fipc_result_t fipc_service_create(const char* name, size_t capacity, size_t max_message, uint32_t max_clients,
                                      uint32_t max_servers, fipc_service_t** out_service);
    fipc_result_t fipc_service_join(const char* name, int role, fipc_conn_t** out_conn, int timeout_ms);
    fipc_result_t fipc_service_wait_for_members(fipc_service_t* service, int role, uint32_t at_least,
                                                uint32_t* out_count, int timeout_ms);
    void fipc_service_cancel(fipc_service_t* service);
    void fipc_service_close(fipc_service_t* service);
""")


# Linux and macOS: the library is never unloaded (docs/platform-support.md), but CFFI unloads it when the interpreter
# exits and frees the handle, and a library thread on its way out of the library's code at that moment crashes the
# exit. RTLD_NODELETE keeps it loaded for the life of the process; Windows has no such flag and needs none.
_PIN = getattr(ffi, "RTLD_NODELETE", 0)


def _load() -> Any:
    """The shared library, kept loaded (`_PIN`): in the folder the environment variable FASTIPC_LIB_DIR names, when it
    is set (nowhere else); in a development checkout, the repository's zig-out build (ahead of any copy in _native/,
    which may be older); else the one bundled in the package's _native/ folder (an installed wheel); else the
    system's search (LD_LIBRARY_PATH, DYLD_LIBRARY_PATH, PATH)."""
    package = Path(__file__).parent
    repo = package.parent.parent.parent  # fipc -> python -> bindings -> the repository
    names = {"Windows": ["fastipc.dll"], "Darwin": ["libfastipc.dylib"]}.get(platform.system(), ["libfastipc.so"])
    directory = os.environ.get("FASTIPC_LIB_DIR")
    if directory:
        for name in names:
            if (Path(directory) / name).exists():
                return ffi.dlopen(str((Path(directory) / name).resolve()), _PIN)
        raise OSError(f"FastIPC library not found: no {names[0]} in {directory}, the folder FASTIPC_LIB_DIR names")
    folders = [repo / "zig-out" / "lib", repo / "zig-out" / "bin"] if (repo / "build.zig").exists() else []
    for folder in (*folders, package / "_native"):
        for name in names:
            if (folder / name).exists():
                return ffi.dlopen(str(folder / name), _PIN)
    for name in names:
        try:
            return ffi.dlopen(name, _PIN)
        except OSError:
            pass
    raise OSError(f"FastIPC library not found: {names} in {package / '_native'}, zig-out or the system's search")


lib = _load()

NO_WAIT = 0
FOREVER = -1
# broadcast_join's roles: a lossy subscriber never holds the publisher back (it may lose messages: OVERRUN); a gated
# one misses nothing (the publisher waits for it)
SUBSCRIBER_LOSSY = 1
SUBSCRIBER_GATED = 2
# queue_join's and Queue.wait_for_members' roles: a producer sends, a consumer receives
PRODUCER = 3
CONSUMER = 4
# service_join's and Service.wait_for_members' roles: a client sends requests and receives their replies, a server
# receives requests and replies to them
CLIENT = 5
SERVER = 6


class Result(IntEnum):
    """fipc_result_t."""
    OK = 0
    TIMEOUT = 1
    DISCONNECTED = 2
    CANCELLED = 3
    TOO_LARGE = 4
    INVALID = 5
    NO_MEMORY = 6
    ADDR_IN_USE = 7
    OVERRUN = 8
    UNSUBSCRIBED = 9
    ABANDONED = 10


def result_str(result: int) -> str:
    """The library's name for a result ("FIPC_OK", ...)."""
    return ffi.string(lib.fipc_result_str(result)).decode()


class FipcError(Exception):
    """A call returned `result`, not OK. `lost`: on OVERRUN, how many messages the lossy subscriber lost (at least 1),
    else None. `count`: when a `wait_for_members` raised TIMEOUT, CANCELLED or DISCONNECTED, how many members were
    joined, else None (INVALID included). `sent`: when a `send_many` raised, how many of its messages went (all those
    before the one that didn't), else None. `why`, if given, says why in the message (a call's INVALID while a reply to
    a send is owed)."""

    def __init__(self, result: int, call: str, lost: Optional[int] = None, count: Optional[int] = None,
                 why: Optional[str] = None, sent: Optional[int] = None) -> None:
        detail = f" ({lost} lost)" if lost is not None else f" ({count} joined)" if count is not None else ""
        detail = f" ({sent} sent)" if sent is not None else detail
        detail = f" ({why})" if why is not None else detail
        super().__init__(f"{call}: {result_str(result)}{detail}")
        self.result = Result(result) if result in Result._value2member_map_ else result
        self.lost = lost
        self.count = count
        self.sent = sent


def _check(result: int, call: str) -> None:
    if result != Result.OK:
        raise FipcError(result, call)


def _check_recv(result: int, length: int, call: str = "fipc_recv") -> None:
    """As _check for fipc_recv (or fipc_recv_many), whose length out-parameter holds OVERRUN's count of the messages
    lost."""
    if result == Result.OVERRUN:
        raise FipcError(result, call, lost=length)
    _check(result, call)


def _left(timeout_ms: int, deadline: float) -> int:
    """What is left of timeout_ms at `deadline` (FOREVER stays FOREVER), in whole milliseconds, at least 0."""
    return timeout_ms if timeout_ms < 0 else max(0, int((deadline - time.monotonic()) * 1000))


def _check_count(result: int, count: int, call: str) -> None:
    """As _check for a wait for members, whose count out-parameter holds the count at every result but INVALID."""
    if result not in (Result.OK, Result.INVALID):
        raise FipcError(result, call, count=count)
    _check(result, call)


Bytes = Union[bytes, bytearray, memoryview]

# The least room recv_many receives into: a batch takes the messages that fit it (it grows for a longer first message)
_BATCH_BYTES = 1 << 16


class Conn:
    """One connection: a stream's (`stream_connect`, a client; `Listener.accept`, a server), with two rings and one
    session; a broadcast's publisher (`broadcast_create`), whose sends go to every subscriber, or one of its subscribers
    (`broadcast_join`), which receives; a work queue's producer or consumer (`queue_join`): a producer sends, and a
    consumer receives, each message going to one consumer; or a service's client or server (`service_join`): a client
    sends requests and receives their replies, in the order of its requests (or `call`s), and a server receives the
    next request of any client and replies to it with its next send.

    A Conn left unclosed is closed when it is garbage-collected, as by `close` (a publisher's ends its broadcast, a
    server's answers the request it holds ABANDONED). A memoryview that `send_acquire` or `recv_acquire` returned keeps
    its Conn referenced, so the connection's memory stays mapped while the view exists; and a thread inside a call
    holds the Conn, so the collection never closes it under a call."""

    def __init__(self, handle: Any) -> None:
        self._handle = ffi.gc(handle, lib.fipc_close)  # the finalizer, which close() disarms
        # Out-parameters and the receive buffer, per connection (one receiving thread at a time)
        self._len = ffi.new("size_t*")
        self._ptr = ffi.new("void**")
        self._cptr = ffi.new("const void**")
        self._buf = ffi.new("char[]", 4096)
        self._lens = ffi.new("size_t[]", 16)  # recv_many's lengths, grown for a larger max_count

    def __enter__(self) -> "Conn":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def max_piece(self) -> int:
        """The largest message the zero-copy calls take on this connection: its capacity less 64 bytes (a broadcast's or
        a work queue's: a quarter of its capacity, the largest message it takes; a service's: its max_message, the
        largest request or reply)."""
        return lib.fipc_max_piece(self._handle)

    def cancel(self) -> None:
        """Every call of this connection that waits, now or later, raises CANCELLED (a service client's `call` too,
        sending nothing). Any thread."""
        lib.fipc_cancel(self._handle)

    def wait_for_members(self, at_least: int = 0, timeout_ms: int = FOREVER) -> int:
        """A broadcast's publisher: waits up to timeout_ms until at least `at_least` subscribers are joined (0 returns
        the count at once), and returns how many are. To send nothing before N subscribers are there, call it with N
        before the first send. TIMEOUT and CANCELLED (after `cancel`) carry the count in the error's `count`; INVALID
        on any other connection, or for at_least over the broadcast's max_subscribers. Any thread."""
        count = ffi.new("uint32_t*")
        result = lib.fipc_broadcast_wait_for_members(self._handle, at_least, count, timeout_ms)
        _check_count(result, count[0], "fipc_broadcast_wait_for_members")
        return count[0]

    def close(self) -> None:
        """Ends the connection; the peer gets DISCONNECTED. A publisher's close ends the broadcast and frees its name:
        its subscribers receive what it sent, then DISCONNECTED. A subscriber, a producer or a consumer leaves (a
        producer's messages stay for the consumers). A service's client or server leaves: a client's requests that no
        server took are dropped, and the replies to those taken are lost; the request a server holds is answered
        ABANDONED. No other thread may be inside a call on it. Closing it again does nothing."""
        if self._handle is not None:
            handle, self._handle = self._handle, None
            ffi.gc(handle, None)  # disarms the finalizer: closed once
            lib.fipc_close(handle)

    def _view(self, pointer: Any, length: int) -> memoryview:
        """A memoryview of the `length` bytes at `pointer`, in the connection's shared memory, that keeps the Conn
        referenced while it exists (through the destructor of the pointer it holds), so the finalizer can't unmap that
        memory under it."""
        return memoryview(ffi.buffer(ffi.gc(pointer, self._keep), length))

    def _keep(self, pointer: Any) -> None:
        """The destructor of a view's pointer: nothing to free; being a bound method, it holds the Conn for the view."""

    # === Messages ===

    def send(self, data: Bytes, timeout_ms: int = FOREVER) -> None:
        """Sends one message of any size (at least 1 byte; on a broadcast, a work queue or a service, at most
        `max_piece()`: else TOO_LARGE). A work queue's producer waits for room while the consumers haven't taken its
        earlier messages. A service's client sends a request, waiting for room in its ring (which its replies not yet
        received hold too); a server replies to the request it holds, and never waits (INVALID with none held)."""
        _check(lib.fipc_send(self._handle, ffi.from_buffer(data), len(data), timeout_ms), "fipc_send")

    def recv(self, timeout_ms: int = FOREVER) -> bytes:
        """Receives one message of any size. A lossy subscriber that fell a lap behind gets OVERRUN instead, with the
        number of messages lost in the error's `lost`; its next receive goes on from a recent message. A work queue's
        consumer takes a message of any producer. A service's client receives the reply to its oldest request whose
        reply it hasn't received, or ABANDONED if that request's server ended before it replied (the request counts as
        answered; it may or may not have been carried out); a receive that times out or is cancelled leaves the reply to
        come, for the next receive, and after a `call` that gave up, the next `recv` returns that call's late reply
        first. A server takes the next request of any client and holds it until it replies: its receives raise INVALID
        until then."""
        length = self._recv(timeout_ms)  # may replace the buffer with a bigger one
        return ffi.buffer(self._buf, length)[:]

    def recv_into(self, buf: Union[bytearray, memoryview], timeout_ms: int = FOREVER) -> int:
        """Receives one message into `buf` and returns its length; TOO_LARGE (the message stays) if it doesn't fit: on
        a work queue another consumer may take it first. OVERRUN as `recv`."""
        _check_recv(lib.fipc_recv(self._handle, ffi.from_buffer(buf), len(buf), self._len, timeout_ms), self._len[0])
        return self._len[0]

    def _recv(self, timeout_ms: int) -> int:
        """Receives into the connection's buffer, growing it for a message that doesn't fit (on a work queue another
        consumer may take that one first, and the next may be longer still); returns the length. Every receive waits
        only for what is left of timeout_ms."""
        deadline = time.monotonic() + timeout_ms / 1000
        result = lib.fipc_recv(self._handle, self._buf, len(self._buf), self._len, timeout_ms)
        while result == Result.TOO_LARGE:
            self._buf = ffi.new("char[]", self._len[0])
            result = lib.fipc_recv(self._handle, self._buf, len(self._buf), self._len, _left(timeout_ms, deadline))
        _check_recv(result, self._len[0])
        return self._len[0]

    def send_many(self, messages: Sequence[Bytes], timeout_ms: int = FOREVER) -> int:
        """Sends `messages` (at least one, each of at least 1 byte) in order and publishes them together, with at most
        one wake of the receivers for the batch, and returns how many went: all of them. timeout_ms bounds the whole
        call (NO_WAIT: only what fits now). An error is the first message that didn't go, as `send` raises it, and its
        `sent` is how many went, all those before it. Every message is checked first, before any goes: INVALID for an
        empty one or an empty batch, TOO_LARGE over `max_piece()` on a broadcast, a work queue or a service; on a stream
        a longer message goes in pieces, as `send` sends it. A service's client sends a batch of requests, whose replies
        come in their order and are owed as `send`'s are; a server's batch is INVALID. Batch both ends: a receiver that
        takes one message per call falls behind a batching sender, and the two run slower than without batches."""
        count = len(messages)
        datas = ffi.new("const void*[]", count)
        lens = ffi.new("size_t[]", count)
        keep = [ffi.from_buffer(message) for message in messages]  # referenced until the call returns
        for i, data in enumerate(keep):
            datas[i] = data
            lens[i] = len(data)
        sent = ffi.new("size_t*")
        result = lib.fipc_send_many(self._handle, datas, lens, count, sent, timeout_ms)
        if result != Result.OK:
            raise FipcError(result, "fipc_send_many", sent=sent[0])
        return sent[0]

    def recv_many(self, max_count: int = 16, timeout_ms: int = FOREVER) -> List[bytes]:
        """Waits up to timeout_ms for the next message, as `recv`, then takes every further message already there, up to
        `max_count` (at least 1) and as many as fit the connection's receive buffer (64 KiB, grown for a longer first
        message), and returns them in order. An error is the first message's, as `recv` raises it, with nothing else
        taken: OVERRUN with the number lost in `lost`, a service client's ABANDONED. After the first it never waits,
        and stops before a message that isn't there yet, doesn't fit, is in pieces, or would bring another result: the
        next receive returns it. A broadcast's subscriber takes, after its first, only the messages its publisher has
        moved past (the newest come in the next calls); a work queue's consumer acknowledges each message before the
        call returns, as `recv` does; a service's client receives its replies in its requests' order. INVALID on a
        publisher, a producer and a service's server, which holds one request at a time."""
        if len(self._buf) < _BATCH_BYTES:
            self._buf = ffi.new("char[]", _BATCH_BYTES)
        if len(self._lens) < max_count:
            self._lens = ffi.new("size_t[]", max_count)
        deadline = time.monotonic() + timeout_ms / 1000
        result = lib.fipc_recv_many(self._handle, self._buf, len(self._buf), self._lens, max_count, self._len,
                                    timeout_ms)
        while result == Result.TOO_LARGE:  # the first message stays: make room for it (or the next, maybe longer)
            self._buf = ffi.new("char[]", self._lens[0])
            result = lib.fipc_recv_many(self._handle, self._buf, len(self._buf), self._lens, max_count, self._len,
                                        _left(timeout_ms, deadline))
        _check_recv(result, self._lens[0], "fipc_recv_many")
        received = ffi.buffer(self._buf)
        messages = []
        at = 0
        for i in range(self._len[0]):
            messages.append(received[at:at + self._lens[i]])
            at += self._lens[i]
        return messages

    def send_acquire(self, length: int, timeout_ms: int = FOREVER) -> memoryview:
        """Zero-copy send, step 1: `length` bytes (at most `max_piece()`) in the ring to write the message into. The
        memoryview keeps the Conn referenced (it is never collected under it)."""
        _check(lib.fipc_send_acquire(self._handle, length, self._ptr, timeout_ms), "fipc_send_acquire")
        return self._view(self._ptr[0], length)

    def send_commit(self, length: int) -> None:
        """Zero-copy send, step 2: sends the first `length` bytes of the acquired room as one message."""
        _check(lib.fipc_send_commit(self._handle, length), "fipc_send_commit")

    def recv_acquire(self, timeout_ms: int = FOREVER) -> memoryview:
        """Zero-copy receive, step 1: the next message in the ring (read-only), valid until `recv_release`, the next
        receive or `close` (the memoryview keeps the Conn referenced: it is never collected under it). TOO_LARGE for a
        message of several pieces: `recv` it. A gated subscriber's message holds the publisher back until its release; a
        lossy subscriber's acquire is INVALID. A work queue's consumer takes the message: it holds its producer back
        once the producer is a ring ahead of it. Once a receive raised UNSUBSCRIBED, never read a memoryview of a
        message held in place: its memory is gone, and reading it crashes the process. A service's client takes its next
        reply in place (or ABANDONED, as `recv`); a server takes the next request, which stays valid until its reply's
        `send` or `send_commit`."""
        _check(lib.fipc_recv_acquire(self._handle, self._cptr, self._len, timeout_ms), "fipc_recv_acquire")
        return self._view(self._cptr[0], self._len[0]).toreadonly()

    def recv_release(self) -> None:
        """Zero-copy receive, step 2: frees the acquired message's room. On a service's server it does nothing: the
        request stays until the reply."""
        lib.fipc_recv_release(self._handle)

    def call(self, request: Bytes, timeout_ms: int = FOREVER) -> bytes:
        """A service's client: sends `request` (1 to `max_piece()` bytes) and returns its reply, waiting up to
        timeout_ms in all; CANCELLED, sending nothing, after `cancel`; INVALID on any other connection. See
        `service_join`."""
        raise FipcError(Result.INVALID, "call")


class _ServiceClient(Conn):
    """A service's client (`service_join` with CLIENT), a Conn that keeps, oldest first, the requests whose replies it
    hasn't received yet, so that `call` never takes the reply to another request: True for a request whose `call`
    gave up on its reply (it timed out or was cancelled), which the next `call` receives and drops (at most one, as a
    call first drops what is owed); False for one sent with `send` or `send_commit`, whose reply is the caller's to
    receive. A request's entry is added before it is sent, since a thread that receives may take its reply before the
    send returns, and removed unless the send returned: whatever it raised (FipcError, a TypeError for an argument that
    isn't bytes, KeyboardInterrupt, ...). `_cancelled` once `cancel` has run: a call then sends nothing, as it could
    never receive the reply."""

    def __init__(self, handle: Any) -> None:
        super().__init__(handle)
        self._owed: Deque[bool] = deque()
        self._cancelled = False

    def cancel(self) -> None:
        self._cancelled = True  # before the native cancel: a call that doesn't see it sent its request before it
        super().cancel()

    def _counted(self, entry: bool, send: Any, *args: Any) -> None:
        """Calls `send` with `entry` added for its request, and removes it unless the send returned (the sending thread
        is the one that adds, so the newest entry is this one)."""
        self._owed.append(entry)
        sent = False
        try:
            send(*args)
            sent = True
        finally:
            if not sent:
                self._owed.pop()

    def send(self, data: Bytes, timeout_ms: int = FOREVER) -> None:
        self._counted(False, super().send, data, timeout_ms)

    def send_commit(self, length: int) -> None:
        self._counted(False, super().send_commit, length)

    def send_many(self, messages: Sequence[Bytes], timeout_ms: int = FOREVER) -> int:
        """As Conn.send_many, with an entry added for each request before the batch goes, and removed for each one that
        didn't go (the newest entries: the sending thread is the one that adds)."""
        count = len(messages)
        self._owed.extend([False] * count)
        sent = 0
        try:
            sent = super().send_many(messages, timeout_ms)
        except FipcError as e:
            sent = e.sent or 0
            raise
        finally:
            for _ in range(count - sent):
                self._owed.pop()
        return sent

    def _answered(self, receive: Any, *args: Any) -> Any:
        """Calls `receive`, and takes the oldest request off the list when it returned a reply or raised ABANDONED (an
        answer too)."""
        try:
            value = receive(*args)
        except FipcError as e:
            if e.result == Result.ABANDONED and self._owed:
                self._owed.popleft()
            raise
        if self._owed:
            self._owed.popleft()
        return value

    def recv(self, timeout_ms: int = FOREVER) -> bytes:
        return self._answered(super().recv, timeout_ms)

    def recv_into(self, buf: Union[bytearray, memoryview], timeout_ms: int = FOREVER) -> int:
        return self._answered(super().recv_into, buf, timeout_ms)

    def recv_acquire(self, timeout_ms: int = FOREVER) -> memoryview:
        return self._answered(super().recv_acquire, timeout_ms)

    def recv_many(self, max_count: int = 16, timeout_ms: int = FOREVER) -> List[bytes]:
        """As Conn.recv_many, taking a request off the list for each reply, or for an ABANDONED."""
        try:
            replies = super().recv_many(max_count, timeout_ms)
        except FipcError as e:
            if e.result == Result.ABANDONED and self._owed:
                self._owed.popleft()
            raise
        for _ in replies:
            if self._owed:
                self._owed.popleft()
        return replies

    def call(self, request: Bytes, timeout_ms: int = FOREVER) -> bytes:
        deadline = time.monotonic() + timeout_ms / 1000
        data = ffi.from_buffer(request)  # a TypeError for a request that isn't bytes, before anything is counted
        length = len(data)
        if length == 0:
            raise FipcError(Result.INVALID, "call")
        if length > self.max_piece():
            raise FipcError(Result.TOO_LARGE, "call")
        if self._cancelled:
            raise FipcError(Result.CANCELLED, "call")  # its reply could never be received: the request never goes
        if not all(self._owed):  # a reply to a request of send(): the caller's, not ours to drop
            raise FipcError(Result.INVALID, "call", why="a reply to a request of send() is owed: receive it first")
        dropped = False
        while self._owed:  # the reply to a call that gave up on it: dropped
            dropped = True
            try:
                self._answered(self._recv, _left(timeout_ms, deadline))
            except FipcError as e:
                if e.result != Result.ABANDONED:
                    raise
        if dropped and timeout_ms > 0 and time.monotonic() >= deadline:
            raise FipcError(Result.TIMEOUT, "call")  # its time went on the drop: the request never goes
        if self._cancelled:
            raise FipcError(Result.CANCELLED, "call")  # cancelled while it dropped
        self._counted(True, _check_send, self, data, length, _left(timeout_ms, deadline))
        length = self._answered(self._recv, _left(timeout_ms, deadline))
        return ffi.buffer(self._buf, length)[:]


def _check_send(conn: Conn, data: Any, length: int, timeout_ms: int) -> None:
    _check(lib.fipc_send(conn._handle, data, length, timeout_ms), "fipc_send")


class Listener:
    """A stream's name, held by its server (`stream_create`): `accept` one client at a time. Its close frees the name;
    the connections it accepted stay open.

    A Listener left unclosed is closed when it is garbage-collected, as by `close`, which frees the name; a thread
    inside `accept` holds the Listener, so the collection never closes it under a call."""

    def __init__(self, handle: Any) -> None:
        self._handle: Optional[Any] = ffi.gc(handle, lib.fipc_listener_close)  # the finalizer, which close() disarms

    def __enter__(self) -> "Listener":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def accept(self, timeout_ms: int = FOREVER) -> Conn:
        """Waits for a client and sets its connection up on the calling thread; a call that times out in the middle
        of a client's setup keeps it for the next call, so NO_WAIT polls. INVALID while the connection it returned
        last is open, NO_MEMORY once the listener has failed for good."""
        out = ffi.new("fipc_conn_t**")
        _check(lib.fipc_stream_accept(self._handle, out, timeout_ms), "fipc_stream_accept")
        return Conn(out[0])

    def cancel(self) -> None:
        """Every accept that waits, now or later, raises CANCELLED; a client whose setup an accept began is dropped. Any
        thread."""
        lib.fipc_listener_cancel(self._handle)

    def close(self) -> None:
        """Stops listening and frees the name (on Windows once the connections it accepted are closed too); the
        connections it accepted stay open. No other thread may be inside a call on it."""
        if self._handle is not None:
            handle, self._handle = self._handle, None
            ffi.gc(handle, None)  # disarms the finalizer: closed once
            lib.fipc_listener_close(handle)


class Queue:
    """A work queue, held by its owner (`queue_create`). Its producers and consumers, the owner's own included, join
    with `queue_join`. A queue lives until `close()` (or the end of its `with` block) or the end of its process;
    dropping the object doesn't end it (it has no finalizer)."""

    def __init__(self, handle: Any) -> None:
        self._handle: Optional[Any] = handle

    def __enter__(self) -> "Queue":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def wait_for_members(self, role: Optional[int] = None, at_least: int = 0, timeout_ms: int = FOREVER) -> int:
        """Waits up to timeout_ms until at least `at_least` members of `role` (PRODUCER or CONSUMER; None counts both)
        are joined (0 returns the count at once), and returns how many are. TIMEOUT, CANCELLED and DISCONNECTED (the
        queue has ended) carry the count in the error's `count`; INVALID for another role, or for at_least over the
        role's maximum (max_producers, max_consumers, or their sum for None). Any thread."""
        count = ffi.new("uint32_t*")
        result = lib.fipc_queue_wait_for_members(self._handle, role or 0, at_least, count, timeout_ms)
        _check_count(result, count[0], "fipc_queue_wait_for_members")
        return count[0]

    def cancel(self) -> None:
        """Every `wait_for_members` that waits, now or later, raises CANCELLED. The queue goes on. Any thread."""
        lib.fipc_queue_cancel(self._handle)

    def close(self) -> None:
        """Ends the queue and frees its name: producers' sends raise DISCONNECTED, and consumers receive what is left,
        then DISCONNECTED. The members' connections, the owner's own included, stay open until their close. No other
        thread may be inside a call on it."""
        if self._handle is not None:
            lib.fipc_queue_close(self._handle)
            self._handle = None


class Service:
    """A service, held by its owner (`service_create`). Its clients and servers, the owner's own included, join with
    `service_join`. A service lives until `close()` (or the end of its `with` block) or the end of its process;
    dropping the object doesn't end it (it has no finalizer)."""

    def __init__(self, handle: Any) -> None:
        self._handle: Optional[Any] = handle

    def __enter__(self) -> "Service":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def wait_for_members(self, role: Optional[int] = None, at_least: int = 0, timeout_ms: int = FOREVER) -> int:
        """Waits up to timeout_ms until at least `at_least` members of `role` (CLIENT or SERVER; None counts both) are
        joined (0 returns the count at once), and returns how many are. TIMEOUT, CANCELLED and DISCONNECTED (the
        service has ended) carry the count in the error's `count`; INVALID for another role, or for at_least over the
        role's maximum (max_clients, max_servers, or their sum for None). Any thread."""
        count = ffi.new("uint32_t*")
        result = lib.fipc_service_wait_for_members(self._handle, role or 0, at_least, count, timeout_ms)
        _check_count(result, count[0], "fipc_service_wait_for_members")
        return count[0]

    def cancel(self) -> None:
        """Every `wait_for_members` that waits, now or later, raises CANCELLED. The service goes on. Any thread."""
        lib.fipc_service_cancel(self._handle)

    def close(self) -> None:
        """Ends the service and frees its name: clients' sends and servers' receives raise DISCONNECTED, a client
        receives the replies that came before the end, then DISCONNECTED, and a server's reply raises DISCONNECTED.
        The members' connections, the owner's own included, stay open until their close. No other thread may be inside
        a call on it."""
        if self._handle is not None:
            lib.fipc_service_close(self._handle)
            self._handle = None


# === Streams ===


def stream_create(name: str, capacity: int) -> Listener:
    """A stream's server: claims `name` and listens on it, with rings of `capacity` bytes (a power of two, 1 KiB to
    2 GiB) for each connection, and returns its Listener. Doesn't wait: clients may connect from now on, and
    `Listener.accept` sets each one up. ADDR_IN_USE if another stream, broadcast, queue or service holds the name."""
    out = ffi.new("fipc_listener_t**")
    _check(lib.fipc_stream_create(name.encode(), capacity, out), "fipc_stream_create")
    return Listener(out[0])


def stream_connect(name: str, timeout_ms: int = FOREVER) -> Conn:
    """A stream's client: connects to the server of the stream on `name` and sets the connection up on the calling
    thread, waiting up to timeout_ms for the server to create the stream and to call accept: the server runs on another
    thread or in another process. FOREVER waits for a server however late it starts. INVALID if a broadcast, a queue or
    a service holds the name."""
    out = ffi.new("fipc_conn_t**")
    _check(lib.fipc_stream_connect(name.encode(), out, timeout_ms), "fipc_stream_connect")
    return Conn(out[0])


# === Broadcasts ===


def broadcast_create(name: str, capacity: int, max_subscribers: int, max_stall_ms: int) -> Conn:
    """A broadcast's publisher: claims `name` for a broadcast of `capacity` bytes of messages (a power of two, 4 KiB to
    2 GiB; messages of up to a quarter of it) and up to `max_subscribers` subscribers (1 to 1024), and returns the
    publisher's connection, the broadcast's one handle: its sends go to every subscriber, and its close ends the
    broadcast, as does its garbage collection: keep it referenced while the broadcast is in use. Doesn't wait:
    subscribers may join from now on, and this process admits them by itself. A gated subscriber that holds the
    publisher back for max_stall_ms is unsubscribed; FOREVER never, NO_WAIT refuses gated subscribers. ADDR_IN_USE if
    another stream, broadcast, queue or service holds the name."""
    out = ffi.new("fipc_conn_t**")
    _check(lib.fipc_broadcast_create(name.encode(), capacity, max_subscribers, max_stall_ms, out),
           "fipc_broadcast_create")
    return Conn(out[0])


def broadcast_join(name: str, role: int, timeout_ms: int = FOREVER) -> Conn:
    """A subscriber: joins the broadcast on `name` as `role` (SUBSCRIBER_LOSSY, which never holds the publisher back;
    SUBSCRIBER_GATED, which misses nothing), waiting up to timeout_ms for the publisher to create the broadcast, for a
    free slot, and for the publisher's process to admit it. It receives the messages sent from its admission on, then
    DISCONNECTED once the publisher closed. INVALID for another role, a name a queue or a service holds, or a gated
    subscriber on a broadcast that refuses them. On a name a stream holds, the join learns the kind when the stream's
    server accepts it, which brings INVALID; while no accept runs, it waits and raises TIMEOUT."""
    out = ffi.new("fipc_conn_t**")
    _check(lib.fipc_broadcast_join(name.encode(), role, out, timeout_ms), "fipc_broadcast_join")
    return Conn(out[0])


# === Work queues ===


def queue_create(name: str, capacity: int, max_producers: int, max_consumers: int) -> Queue:
    """A work queue's owner: claims `name` for a work queue of up to `max_producers` producers (1 to 63) and
    `max_consumers` consumers (1 to 64), each producer with a ring of `capacity` bytes of messages (a power of two,
    4 KiB to 2 GiB; messages of up to a quarter of it), and returns the Queue. Doesn't wait: members may join from now
    on, and this process admits them by itself; the owner joins with `queue_join` too, from any thread. A queue lives
    until `close()` or the end of its process; dropping the object doesn't end it. ADDR_IN_USE if another stream,
    broadcast, queue or service holds the name."""
    out = ffi.new("fipc_queue_t**")
    _check(lib.fipc_queue_create(name.encode(), capacity, max_producers, max_consumers, out), "fipc_queue_create")
    return Queue(out[0])


def queue_join(name: str, role: int, timeout_ms: int = FOREVER) -> Conn:
    """A producer or a consumer: joins the work queue on `name` as `role` (PRODUCER or CONSUMER), waiting up to
    timeout_ms for the owner to create the queue, for a free place of the role, and for the owner's process to admit
    it. A consumer takes each message of the queue's producers that no other consumer took, in no promised order, then
    DISCONNECTED once the queue ended; a producer's sends raise DISCONNECTED from then on. INVALID for another role, or
    a name a broadcast or a service holds. On a name a stream holds, the join learns the kind when the stream's server
    accepts it, which brings INVALID; while no accept runs, it waits and raises TIMEOUT."""
    out = ffi.new("fipc_conn_t**")
    _check(lib.fipc_queue_join(name.encode(), role, out, timeout_ms), "fipc_queue_join")
    return Conn(out[0])


# === Services ===


def service_create(name: str, capacity: int, max_message: int, max_clients: int, max_servers: int) -> Service:
    """A service's owner: claims `name` for a service of up to `max_clients` clients (1 to 64) and `max_servers`
    servers (1 to 64), each client with a ring of `capacity` bytes (a power of two, 4 KiB to 2 GiB) for its requests and
    their replies, each of 1 to `max_message` bytes (1 to capacity / 4), and returns the Service. A request holds its
    length rounded up to 64 bytes, plus max_message rounded up to 64, of its client's ring from its send until its
    reply has been received. Doesn't wait: members may join from now on, and this process admits them by itself; the
    owner joins with `service_join` too, from any thread. A service lives until `close()` or the end of its process;
    dropping the object doesn't end it. ADDR_IN_USE if another stream, broadcast, queue or service holds the name."""
    out = ffi.new("fipc_service_t**")
    _check(lib.fipc_service_create(name.encode(), capacity, max_message, max_clients, max_servers, out),
           "fipc_service_create")
    return Service(out[0])


def service_join(name: str, role: int, timeout_ms: int = FOREVER) -> Conn:
    """A client or a server: joins the service on `name` as `role` (CLIENT or SERVER), waiting up to timeout_ms for the
    owner to create the service, for a free place of the role, and for the owner's process to admit it. INVALID for
    another role, or a name a broadcast or a queue holds. On a name a stream holds, the join learns the kind when the
    stream's server accepts it, which brings INVALID; while no accept runs, it waits and raises TIMEOUT.

    A client's `send` sends a request, and its `recv` (`recv_into`, `recv_acquire`) receives the reply to its oldest
    request whose reply it hasn't received yet, so the n-th reply answers the n-th request; or ABANDONED, if the server
    that took that request ended before it replied (the request counts as answered; it may or may not have been carried
    out, and it is never sent again). `call(request)` does both: it sends the request and returns its reply. A call
    checks its request first: INVALID for an empty one, TOO_LARGE over `max_piece()`, at once, even while a reply is
    owed. Then a call on a cancelled connection raises CANCELLED, whatever its timeout, and sends nothing, as it could
    never receive the reply. A call whose receive timed out or was cancelled leaves its reply owed: the connection keeps
    count, and the next call first receives and drops that reply (at most one is owed, since every call drops it first;
    an ABANDONED in its place counts too), within its own timeout and before it sends, so it never returns another
    request's reply. A call that had to drop an owed reply first sends its request only if its timeout has time left:
    once a positive timeout has run out, it raises TIMEOUT without sending, so no server sees that request; with NO_WAIT
    no step waits, and it sends if there is room. After a call that gave up, a plain `recv` returns that call's late
    reply (or ABANDONED) first: replies come in request order. A call raises INVALID while a request sent with `send`
    still has its reply to be received: receive it first. A call sends and receives, so no other thread may send or
    receive on the connection meanwhile.

    A server's `recv` takes the next request of any client, and its `send` replies to it, never waiting; a server holds
    one request at a time, so its receives raise INVALID until it replies, and its replies raise INVALID with no request
    held. A request taken in place (`recv_acquire`) stays valid until the reply's `send` or `send_commit`;
    `recv_release` does nothing. A reply to a client that has left is dropped. A server's close answers the request it
    holds ABANDONED, and so does the garbage collection of a server's `Conn` left unclosed: a server holds its request,
    and its client waits for the reply, until the server is closed or garbage-collected (or its process ends). Once the
    service has ended, clients' sends and servers' receives raise DISCONNECTED; a client receives the replies that came
    before the end, then DISCONNECTED."""
    out = ffi.new("fipc_conn_t**")
    _check(lib.fipc_service_join(name.encode(), role, out, timeout_ms), "fipc_service_join")
    return _ServiceClient(out[0]) if role == CLIENT else Conn(out[0])
