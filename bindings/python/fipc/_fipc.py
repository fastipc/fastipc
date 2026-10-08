"""
The FastIPC C API (include/fipc.h) for Python, through CFFI's ABI mode: the shared library is loaded at run time,
with no build step.

A server listens on a name and accepts one client at a time; a client connects to the name. Timeouts are int
milliseconds: NO_WAIT (0) doesn't wait, FOREVER (-1) waits for ever. A call that fails raises FipcError, whose
`result` is the library's Result (Result.TIMEOUT when the timeout ran out). The header's rules hold: on a
connection, one thread at a time sends and one receives; `cancel` may be called from any thread; `close` only when
no other thread is inside a call on the handle (cancel, join the threads, close).
"""

from __future__ import annotations

import os
import platform
from enum import IntEnum
from pathlib import Path
from typing import Any, NamedTuple, Optional, Union

from cffi import FFI

ffi = FFI()
ffi.cdef("""
    typedef struct fipc_listener fipc_listener_t;
    typedef struct fipc_conn fipc_conn_t;
    typedef int fipc_result_t;

    typedef struct {
        uint64_t id;
        uint32_t kind;
        uint32_t opcode;
        int32_t status;
        uint32_t reserved;
        uint64_t len;
    } fipc_rpc_msg_t;

    const char* fipc_result_str(fipc_result_t result);

    fipc_result_t fipc_listen(const char* name, size_t capacity, fipc_listener_t** out_listener);
    fipc_result_t fipc_accept(fipc_listener_t* listener, fipc_conn_t** out_conn, int timeout_ms);
    void fipc_listener_cancel(fipc_listener_t* listener);
    void fipc_listener_close(fipc_listener_t* listener);
    fipc_result_t fipc_connect(const char* name, fipc_conn_t** out_conn, int timeout_ms);
    size_t fipc_max_piece(const fipc_conn_t* conn);
    void fipc_cancel(fipc_conn_t* conn);
    void fipc_close(fipc_conn_t* conn);

    fipc_result_t fipc_send(fipc_conn_t* conn, const void* data, size_t len, int timeout_ms);
    fipc_result_t fipc_recv(fipc_conn_t* conn, void* buf, size_t buf_len, size_t* out_len, int timeout_ms);
    fipc_result_t fipc_send_acquire(fipc_conn_t* conn, size_t len, void** out_buf, int timeout_ms);
    fipc_result_t fipc_send_commit(fipc_conn_t* conn, size_t len);
    fipc_result_t fipc_recv_acquire(fipc_conn_t* conn, const void** out_data, size_t* out_len, int timeout_ms);
    void fipc_recv_release(fipc_conn_t* conn);

    fipc_result_t fipc_rpc_submit(fipc_conn_t* conn, uint32_t opcode, const void* data, size_t len, uint64_t* out_id,
                                  int timeout_ms);
    fipc_result_t fipc_rpc_respond(fipc_conn_t* conn, uint64_t id, uint32_t opcode, int32_t status, const void* data,
                                   size_t len, int timeout_ms);
    fipc_result_t fipc_rpc_recv(fipc_conn_t* conn, void* buf, size_t buf_len, fipc_rpc_msg_t* msg, int timeout_ms);
""")


def _load() -> Any:
    """The shared library: in the folder the environment variable FASTIPC_LIB_DIR names, when it is set (nowhere
    else); in a development checkout, the repository's zig-out build (it beats a copy staged earlier); else the one
    bundled in the package's _native/ folder (an installed wheel); else the system's search (LD_LIBRARY_PATH,
    DYLD_LIBRARY_PATH, PATH)."""
    package = Path(__file__).parent
    repo = package.parent.parent.parent  # fipc -> python -> bindings -> the repository
    names = {"Windows": ["fastipc.dll"], "Darwin": ["libfastipc.dylib"]}.get(platform.system(), ["libfastipc.so"])
    directory = os.environ.get("FASTIPC_LIB_DIR")
    if directory:
        for name in names:
            if (Path(directory) / name).exists():
                return ffi.dlopen(str((Path(directory) / name).resolve()))
        raise OSError(f"FastIPC library not found: no {names[0]} in {directory}, the folder FASTIPC_LIB_DIR names")
    folders = [repo / "zig-out" / "lib", repo / "zig-out" / "bin"] if (repo / "build.zig").exists() else []
    for folder in (*folders, package / "_native"):
        for name in names:
            if (folder / name).exists():
                return ffi.dlopen(str(folder / name))
    for name in names:
        try:
            return ffi.dlopen(name)
        except OSError:
            pass
    raise OSError(f"FastIPC library not found: {names} in {package / '_native'}, zig-out or the system's search")


lib = _load()

NO_WAIT = 0
FOREVER = -1
RPC_REQUEST = 1
RPC_RESPONSE = 2


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


def result_str(result: int) -> str:
    """The library's name for a result ("FIPC_OK", ...)."""
    return ffi.string(lib.fipc_result_str(result)).decode()


class FipcError(Exception):
    """A call returned `result`, not OK."""

    def __init__(self, result: int, call: str) -> None:
        super().__init__(f"{call}: {result_str(result)}")
        self.result = Result(result) if result in Result._value2member_map_ else result


def _check(result: int, call: str) -> None:
    if result != Result.OK:
        raise FipcError(result, call)


Bytes = Union[bytes, bytearray, memoryview]


class RpcMessage(NamedTuple):
    """A received request (kind RPC_REQUEST) or response (RPC_RESPONSE)."""
    id: int
    kind: int
    opcode: int
    status: int
    payload: bytes


class Conn:
    """One connection: two rings, one session. `Conn.connect` (a client) or `Listener.accept` (a server) makes it."""

    def __init__(self, handle: Any) -> None:
        self._handle = handle
        # Out-parameters and the receive buffer, per connection (one receiving thread at a time)
        self._len = ffi.new("size_t*")
        self._ptr = ffi.new("void**")
        self._cptr = ffi.new("const void**")
        self._id = ffi.new("uint64_t*")
        self._msg = ffi.new("fipc_rpc_msg_t*")
        self._buf = ffi.new("char[]", 4096)

    @classmethod
    def connect(cls, name: str, timeout_ms: int = FOREVER) -> "Conn":
        """Connects to the server listening on `name` and sets the connection up on the calling thread, waiting up
        to timeout_ms for the server to listen and to call accept: the server runs on another thread or in another
        process. FOREVER waits for a server however late it starts."""
        out = ffi.new("fipc_conn_t**")
        _check(lib.fipc_connect(name.encode(), out, timeout_ms), "fipc_connect")
        return cls(out[0])

    def __enter__(self) -> "Conn":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def max_piece(self) -> int:
        """The largest message the zero-copy calls take on this connection: its capacity less 64 bytes."""
        return lib.fipc_max_piece(self._handle)

    def cancel(self) -> None:
        """Every call of this connection that waits, now or later, raises CANCELLED. Any thread."""
        lib.fipc_cancel(self._handle)

    def close(self) -> None:
        """Ends the connection; the peer gets DISCONNECTED. No other thread may be inside a call on it."""
        if self._handle is not None:
            lib.fipc_close(self._handle)
            self._handle = None

    # === Messages ===

    def send(self, data: Bytes, timeout_ms: int = FOREVER) -> None:
        """Sends one message of any size (at least 1 byte)."""
        _check(lib.fipc_send(self._handle, ffi.from_buffer(data), len(data), timeout_ms), "fipc_send")

    def recv(self, timeout_ms: int = FOREVER) -> bytes:
        """Receives one message of any size."""
        length = self._recv(timeout_ms)  # may replace the buffer with a bigger one
        return ffi.buffer(self._buf, length)[:]

    def recv_into(self, buf: Union[bytearray, memoryview], timeout_ms: int = FOREVER) -> int:
        """Receives one message into `buf` and returns its length; TOO_LARGE (the message stays) if it doesn't fit."""
        _check(lib.fipc_recv(self._handle, ffi.from_buffer(buf), len(buf), self._len, timeout_ms), "fipc_recv")
        return self._len[0]

    def _recv(self, timeout_ms: int) -> int:
        """Receives into the connection's buffer, growing it for a message that doesn't fit; returns the length."""
        result = lib.fipc_recv(self._handle, self._buf, len(self._buf), self._len, timeout_ms)
        if result == Result.TOO_LARGE:
            self._buf = ffi.new("char[]", self._len[0])
            result = lib.fipc_recv(self._handle, self._buf, len(self._buf), self._len, NO_WAIT)
        _check(result, "fipc_recv")
        return self._len[0]

    def send_acquire(self, length: int, timeout_ms: int = FOREVER) -> memoryview:
        """Zero-copy send, step 1: `length` bytes (at most `max_piece()`) in the ring to write the message into."""
        _check(lib.fipc_send_acquire(self._handle, length, self._ptr, timeout_ms), "fipc_send_acquire")
        return memoryview(ffi.buffer(self._ptr[0], length))

    def send_commit(self, length: int) -> None:
        """Zero-copy send, step 2: sends the first `length` bytes of the acquired room as one message."""
        _check(lib.fipc_send_commit(self._handle, length), "fipc_send_commit")

    def recv_acquire(self, timeout_ms: int = FOREVER) -> memoryview:
        """Zero-copy receive, step 1: the next message in the ring (read-only), valid until `recv_release`, the next
        receive or `close`. TOO_LARGE for a message of several pieces: `recv` it."""
        _check(lib.fipc_recv_acquire(self._handle, self._cptr, self._len, timeout_ms), "fipc_recv_acquire")
        return memoryview(ffi.buffer(self._cptr[0], self._len[0])).toreadonly()

    def recv_release(self) -> None:
        """Zero-copy receive, step 2: frees the acquired message's room."""
        lib.fipc_recv_release(self._handle)

    # === RPC ===

    def rpc_submit(self, opcode: int, data: Bytes = b"", timeout_ms: int = FOREVER) -> int:
        """Sends a request; returns its id."""
        _check(lib.fipc_rpc_submit(self._handle, opcode, ffi.from_buffer(data), len(data), self._id, timeout_ms),
               "fipc_rpc_submit")
        return self._id[0]

    def rpc_respond(self, id: int, opcode: int, status: int = 0, data: Bytes = b"", timeout_ms: int = FOREVER) -> None:
        """Sends the response to request `id`."""
        _check(lib.fipc_rpc_respond(self._handle, id, opcode, status, ffi.from_buffer(data), len(data), timeout_ms),
               "fipc_rpc_respond")

    def rpc_recv(self, timeout_ms: int = FOREVER) -> RpcMessage:
        """Receives one request or response."""
        msg = self._msg
        result = lib.fipc_rpc_recv(self._handle, self._buf, len(self._buf), msg, timeout_ms)
        if result == Result.TOO_LARGE:
            self._buf = ffi.new("char[]", msg.len)
            result = lib.fipc_rpc_recv(self._handle, self._buf, len(self._buf), msg, NO_WAIT)
        _check(result, "fipc_rpc_recv")
        return RpcMessage(msg.id, msg.kind, msg.opcode, msg.status, ffi.buffer(self._buf, msg.len)[:])


class Listener:
    """A server's name: `accept` one client at a time."""

    def __init__(self, name: str, capacity: int) -> None:
        """Claims `name` and listens on it, with rings of `capacity` bytes (a power of two, 1 KiB to 2 GiB);
        ADDR_IN_USE if another listener holds it."""
        out = ffi.new("fipc_listener_t**")
        _check(lib.fipc_listen(name.encode(), capacity, out), "fipc_listen")
        self._handle: Optional[Any] = out[0]

    def __enter__(self) -> "Listener":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def accept(self, timeout_ms: int = FOREVER) -> Conn:
        """Waits for a client and sets its connection up on the calling thread; a call that times out in the middle
        of a client's setup keeps it for the next call, so NO_WAIT polls. INVALID while the connection it returned
        last is open, NO_MEMORY once the listener has failed for good."""
        out = ffi.new("fipc_conn_t**")
        _check(lib.fipc_accept(self._handle, out, timeout_ms), "fipc_accept")
        return Conn(out[0])

    def cancel(self) -> None:
        """Every accept that waits, now or later, raises CANCELLED; a client whose setup an accept began is
        dropped. Any thread."""
        lib.fipc_listener_cancel(self._handle)

    def close(self) -> None:
        """Stops listening; the connections it accepted stay open."""
        if self._handle is not None:
            lib.fipc_listener_close(self._handle)
            self._handle = None
