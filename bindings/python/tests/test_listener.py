"""A stream's Listener through the binding: one left unclosed is closed when it is garbage-collected, which frees its
name (on Windows once the connection it accepted is closed too, as fipc_listener_close says), and that connection
stays open; one closed first is closed once, so its collection closes nothing again."""

import gc
import itertools
import os
import sys
import threading

import pytest

from fipc import FipcError, Result, stream_connect, stream_create

TIMEOUT_MS = 10000
_names = itertools.count()


def unique_name(what: str) -> str:
    return f"py_l_{what}_{os.getpid()}_{next(_names)}"


def test_a_listener_collected_unclosed_frees_its_name_and_its_connection_stays_open():
    name = unique_name("collected")
    listener = stream_create(name, 1 << 16)
    with pytest.raises(FipcError) as caught:
        stream_create(name, 1 << 16)
    assert caught.value.result == Result.ADDR_IN_USE

    connected = []
    connecting = threading.Thread(target=lambda: connected.append(stream_connect(name, TIMEOUT_MS)))
    connecting.start()
    server = listener.accept(TIMEOUT_MS)
    connecting.join()
    (client,) = connected
    del listener
    gc.collect()
    if sys.platform != "win32":
        with stream_create(name, 1 << 16):  # the name is free again at once
            pass
    with server, client:  # the accepted connection stays open
        client.send(b"ping", TIMEOUT_MS)
        assert server.recv(TIMEOUT_MS) == b"ping"
    with stream_create(name, 1 << 16):  # free on Windows too, now that the accepted connection is closed
        pass


def test_a_listener_closed_first_is_closed_once():
    name = unique_name("closed")
    closed = stream_create(name, 1 << 16)
    closed.close()
    closed.close()  # again: nothing
    with stream_create(name, 1 << 16):
        del closed
        gc.collect()  # its collection closes nothing, so the new listener keeps the name
        with pytest.raises(FipcError) as caught:
            stream_create(name, 1 << 16)
        assert caught.value.result == Result.ADDR_IN_USE
