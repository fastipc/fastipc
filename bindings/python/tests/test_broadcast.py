"""The broadcast through the binding: a publisher and its lossy and gated subscribers in this process, the overrun's
lost count, the publisher's wait for members, an unsubscribed gated subscriber, the publisher's close freeing the
name, the other kind's roles, and the publisher and its subscribers in other processes (broadcast_peer.py, which the
other bindings' tests run as their Python side)."""

import itertools
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

import broadcast_peer
from fipc import (CONSUMER, FOREVER, NO_WAIT, PRODUCER, SUBSCRIBER_GATED, SUBSCRIBER_LOSSY, Conn, FipcError, Result,
                  broadcast_create, broadcast_join, queue_join)

PEER = Path(__file__).resolve().parent / "broadcast_peer.py"
TIMEOUT_MS = 10000
_names = itertools.count()


def unique_name(what: str) -> str:
    return f"py_bc_{what}_{os.getpid()}_{next(_names)}"


def raises(result: Result, call, *args) -> FipcError:
    with pytest.raises(FipcError) as caught:
        call(*args)
    assert caught.value.result == result, caught.value
    return caught.value


def test_a_lossy_and_a_gated_subscriber_receive_every_message_then_disconnected():
    name = unique_name("roles")
    publisher = broadcast_create(name, 1 << 16, 4, FOREVER)
    assert isinstance(publisher, Conn)
    with publisher:
        with broadcast_join(name, SUBSCRIBER_LOSSY, TIMEOUT_MS) as lossy, \
                broadcast_join(name, SUBSCRIBER_GATED, TIMEOUT_MS) as gated:
            assert publisher.wait_for_members(2, TIMEOUT_MS) == 2
            assert publisher.max_piece() == lossy.max_piece() == (1 << 16) // 4
            words = [b"hello", b"shared", b"memory" * 1000]
            for word in words:
                publisher.send(word, TIMEOUT_MS)
            assert [lossy.recv(TIMEOUT_MS) for _ in words] == words
            buffer = bytearray(16)
            assert gated.recv_into(buffer, TIMEOUT_MS) == 5 and buffer[:5] == b"hello"
            view = gated.recv_acquire(TIMEOUT_MS)  # in place: the message holds the publisher back until its release
            assert view.readonly and bytes(view) == b"shared"
            gated.recv_release()
            assert gated.recv(TIMEOUT_MS) == words[2]

            # Zero-copy publishing; a broadcast's misuses
            room = publisher.send_acquire(4, TIMEOUT_MS)
            room[:] = b"tick"
            publisher.send_commit(4)
            assert lossy.recv(TIMEOUT_MS) == gated.recv(TIMEOUT_MS) == b"tick"
            raises(Result.TIMEOUT, lossy.recv, NO_WAIT)
            raises(Result.INVALID, lossy.recv_acquire, NO_WAIT)  # a lossy subscriber copies and checks
            raises(Result.INVALID, publisher.recv, NO_WAIT)
            raises(Result.INVALID, gated.send, b"x")
            raises(Result.TOO_LARGE, publisher.send, bytes(publisher.max_piece() + 1))
            raises(Result.INVALID, lossy.wait_for_members)  # a publisher's call

            # The publisher's close ends the broadcast: what it sent is received first
            publisher.send(b"last", TIMEOUT_MS)
            publisher.close()
            for subscriber in (lossy, gated):
                assert subscriber.recv(TIMEOUT_MS) == b"last"
                raises(Result.DISCONNECTED, subscriber.recv, TIMEOUT_MS)


def test_a_lapped_lossy_subscriber_gets_overrun_with_the_number_lost():
    name = unique_name("overrun")
    with broadcast_create(name, 4096, 1, FOREVER) as publisher, \
            broadcast_join(name, SUBSCRIBER_LOSSY, TIMEOUT_MS) as subscriber:
        assert publisher.wait_for_members(1, TIMEOUT_MS) == 1
        # 144 messages of 48 bytes lap the broadcast's 64 cells twice; the subscriber goes on from message 140
        # (docs/protocol.md §9.6), so 0 to 139 are lost
        for i in range(144):
            publisher.send(i.to_bytes(8, "little") + bytes(40), NO_WAIT)
        error = raises(Result.OVERRUN, subscriber.recv, TIMEOUT_MS)
        assert error.lost == 140 and error.count is None
        assert "140 lost" in str(error)
        assert int.from_bytes(subscriber.recv(TIMEOUT_MS)[:8], "little") == 140
        buffer = bytearray(64)
        for i in range(141, 144):
            assert subscriber.recv_into(buffer, TIMEOUT_MS) == 48
            assert int.from_bytes(buffer[:8], "little") == i
        assert raises(Result.TIMEOUT, subscriber.recv, NO_WAIT).lost is None


def test_the_publishers_wait_for_members():
    name = unique_name("count")
    with broadcast_create(name, 4096, 2, FOREVER) as publisher, ThreadPoolExecutor(1) as pool:
        assert publisher.wait_for_members() == 0
        error = raises(Result.TIMEOUT, publisher.wait_for_members, 1, NO_WAIT)
        assert error.count == 0 and error.lost is None

        waiting = pool.submit(publisher.wait_for_members, 1, TIMEOUT_MS)  # on another thread, while one joins
        with broadcast_join(name, SUBSCRIBER_GATED, TIMEOUT_MS):
            assert waiting.result(timeout=TIMEOUT_MS / 1000) == 1
            assert publisher.wait_for_members() == 1
            assert raises(Result.TIMEOUT, publisher.wait_for_members, 2, 50).count == 1

            waiting = pool.submit(publisher.wait_for_members, 2, TIMEOUT_MS)
            publisher.cancel()  # from this thread: the wait, now or about to start, raises CANCELLED
            with pytest.raises(FipcError) as caught:
                waiting.result(timeout=TIMEOUT_MS / 1000)
            assert caught.value.result == Result.CANCELLED and caught.value.count == 1
        error = raises(Result.INVALID, publisher.wait_for_members, 3)  # over max_subscribers
        assert error.count is None
    raises(Result.INVALID, broadcast_join, name, 5, NO_WAIT)  # 5 is no role


def test_a_gated_subscriber_that_holds_the_publisher_back_is_unsubscribed():
    name = unique_name("stall")
    with broadcast_create(name, 4096, 1, 100) as publisher, \
            broadcast_join(name, SUBSCRIBER_GATED, TIMEOUT_MS) as subscriber:
        assert publisher.wait_for_members(1, TIMEOUT_MS) == 1
        # The 65th message waits for room the subscriber holds; 100 ms later it is unsubscribed and the publisher
        # goes on
        for _ in range(100):
            publisher.send(bytes(48), TIMEOUT_MS)
        raises(Result.UNSUBSCRIBED, subscriber.recv, NO_WAIT)
        raises(Result.UNSUBSCRIBED, subscriber.recv, NO_WAIT)  # final


def test_the_publishers_close_frees_the_name_and_create_refuses_what_is_out_of_range():
    name = unique_name("name")
    for args in ((1000, 1, FOREVER), (4096, 0, FOREVER), (4096, 1025, FOREVER)):
        raises(Result.INVALID, broadcast_create, name, *args)
    with broadcast_create(name, 4096, 1, NO_WAIT) as publisher:
        raises(Result.ADDR_IN_USE, broadcast_create, name, 4096, 1, FOREVER)
        raises(Result.INVALID, broadcast_join, name, SUBSCRIBER_GATED, TIMEOUT_MS)  # NO_WAIT refuses gated ones
        for role in (PRODUCER, CONSUMER):  # a queue's roles, on a broadcast and on a join of a broadcast
            raises(Result.INVALID, queue_join, name, role, TIMEOUT_MS)
            raises(Result.INVALID, broadcast_join, name, role, TIMEOUT_MS)
        with broadcast_join(name, SUBSCRIBER_LOSSY, TIMEOUT_MS) as subscriber:
            assert publisher.wait_for_members(1, TIMEOUT_MS) == 1
            publisher.close()  # ends the broadcast and frees the name at once
            raises(Result.DISCONNECTED, subscriber.recv, TIMEOUT_MS)
            with broadcast_create(name, 4096, 1, FOREVER):
                pass


def test_a_publisher_in_another_process_and_two_subscribers_here():
    name = unique_name("pub_peer")
    peer = subprocess.Popen([sys.executable, str(PEER), "publisher", name, "300", "2"])
    try:
        with ThreadPoolExecutor(2) as pool:
            done = [pool.submit(broadcast_peer.subscribe, name, gated, 300) for gated in (True, False)]
            for future in done:
                future.result(timeout=60)
    finally:
        assert peer.wait(timeout=60) == 0, "the publisher peer failed"


def test_a_publisher_here_and_two_subscribers_in_other_processes():
    name = unique_name("sub_peer")
    peers = [subprocess.Popen([sys.executable, str(PEER), "subscriber", name, role, "300"])
             for role in ("gated", "lossy")]
    try:
        broadcast_peer.publish(name, 300, 2)
    finally:
        for peer in peers:
            assert peer.wait(timeout=60) == 0, "a subscriber peer failed"
