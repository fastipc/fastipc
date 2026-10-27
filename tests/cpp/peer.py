"""The Python peer of the C++ wrapper's interop test (fipc_hpp_test.cpp): `peer.py client|server|pinned <name>
<bindings/python>`, the Python binding of this repository against the C++ process; `pinned` checks on its own that
the binding keeps the library loaded."""

import platform
import sys

sys.path.insert(0, sys.argv[3])  # the repository's bindings/python
from fipc import FipcError, Result, stream_connect, stream_create  # noqa: E402
from fipc._fipc import ffi, lib  # noqa: E402

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
    # Sends to the C++ server and receives its answer, then answers its message (doubled), then waits for its end
    with stream_connect(name, timeout_ms=TIMEOUT_MS) as conn:
        conn.send(b"hello from Python", timeout_ms=TIMEOUT_MS)
        reply = conn.recv(timeout_ms=TIMEOUT_MS)
        assert reply == b"HELLO FROM PYTHON", reply
        message = conn.recv(timeout_ms=TIMEOUT_MS)
        conn.send(message * 2, timeout_ms=TIMEOUT_MS)
        until_disconnected(lambda: conn.recv(timeout_ms=TIMEOUT_MS))
elif mode == "pinned":
    # Uses the library (a listener, which registers its fork handlers) and closes it, then closes CFFI's handle, as the
    # interpreter's exit does. On Linux and macOS the library must stay loaded, so a call into it still works; Windows
    # may unload it, so nothing is called there.
    stream_create(name, 1 << 12).close()
    result_str = lib.fipc_result_str
    ffi.dlclose(lib)
    if platform.system() != "Windows":
        assert ffi.string(result_str(Result.TIMEOUT)) == b"FIPC_TIMEOUT"
else:
    # Answers each message with its upper case, until the C++ client's end
    with stream_create(name, 1 << 16) as listener, listener.accept(timeout_ms=TIMEOUT_MS) as conn:

        def answer():
            conn.send(conn.recv(timeout_ms=TIMEOUT_MS).upper(), timeout_ms=TIMEOUT_MS)

        until_disconnected(answer)
