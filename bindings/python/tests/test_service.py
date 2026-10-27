"""The service through the binding: many clients and one server, a pool of server threads that hold requests at once,
each client's replies in the order of its requests, ABANDONED when a server closes or its process ends holding a
request, the calls each role refuses, the zero-copy calls on both roles, timeouts and cancels (a client's call that
gives up and the next one, which drops the late reply), the owner's wait for members and its cancel, the end at the
service's close, a service that outlives its dropped object, a server's Conn that the garbage collector closes and
the views in place that keep their Conn, the examples' exchange, and clients and servers in other
processes (service_peer.py, which the other bindings' tests run as their Python side)."""

import gc
import itertools
import os
import subprocess
import sys
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

import service_peer
from fipc import (CLIENT, CONSUMER, FOREVER, NO_WAIT, PRODUCER, SERVER, SUBSCRIBER_GATED, SUBSCRIBER_LOSSY, Conn,
                  FipcError, Result, Service, broadcast_join, lib, queue_create, queue_join, service_create,
                  service_join)

PEER = Path(__file__).resolve().parent / "service_peer.py"
TIMEOUT_MS = 10000
_names = itertools.count()


def unique_name(what: str) -> str:
    return f"py_s_{what}_{os.getpid()}_{next(_names)}"


def raises(result: Result, call, *args) -> FipcError:
    with pytest.raises(FipcError) as caught:
        call(*args)
    assert caught.value.result == result, caught.value
    return caught.value


def answer_until_the_end(server: Conn, way: int = 0) -> Counter:
    """Answers each request with its uppercase until DISCONNECTED: copied (way 0), into a buffer (1) or in place (2,
    replying in place too); returns the requests it answered."""
    answered: Counter = Counter()
    buffer = bytearray(server.max_piece())
    while True:
        try:
            if way == 0:
                request = server.recv(TIMEOUT_MS)
                server.send(request.upper())
            elif way == 1:
                request = bytes(buffer[:server.recv_into(buffer, TIMEOUT_MS)])
                server.send(request.upper())
            else:
                view = server.recv_acquire(TIMEOUT_MS)
                request = bytes(view)
                room = server.send_acquire(len(view))
                room[:] = request.upper()  # the request in place stays valid until the reply's commit
                server.send_commit(len(view))
        except FipcError as e:
            if e.result != Result.DISCONNECTED:
                raise
            return answered
        answered[request] += 1


def test_many_clients_and_one_server_each_reply_to_its_own_client_in_order():
    name = unique_name("many")
    count = 300
    with service_create(name, 4096, 64, 8, 1) as service:
        assert isinstance(service, Service)
        clients = [service_join(name, CLIENT, TIMEOUT_MS) for _ in range(8)]
        server = service_join(name, SERVER, TIMEOUT_MS)
        assert service.wait_for_members(None, 9, TIMEOUT_MS) == 9
        assert all(conn.max_piece() == 64 for conn in clients + [server])

        def ask(k: int) -> None:
            with clients[k] as client:
                for i in range(count):
                    assert client.call(f"client {k} request {i}".encode(), TIMEOUT_MS) == \
                        f"CLIENT {k} REQUEST {i}".encode()

        with ThreadPoolExecutor(9) as pool:
            answered = pool.submit(answer_until_the_end, server)
            for future in [pool.submit(ask, k) for k in range(8)]:
                future.result(timeout=60)
            service.close()  # ends the service: the server's receive raises DISCONNECTED
            assert sum(answered.result(timeout=60).values()) == 8 * count
    server.close()


def test_a_pool_of_server_threads_holds_requests_at_once_and_answers_each_once():
    """Four servers on threads and four clients that send at once: the servers each take a request and wait at a
    barrier before replying, so all four hold one at the same time; then they answer the rest, each request once, and
    each client receives its replies in the order of its requests (each sends several before it receives)."""
    name = unique_name("pool")
    count = 100
    with service_create(name, 1 << 14, 64, 4, 4) as service:
        servers = [service_join(name, SERVER, TIMEOUT_MS) for _ in range(4)]
        clients = [service_join(name, CLIENT, TIMEOUT_MS) for _ in range(4)]
        assert service.wait_for_members(SERVER, 4, TIMEOUT_MS) == 4
        barrier = threading.Barrier(4, timeout=TIMEOUT_MS / 1000)

        def serve(server: Conn, way: int) -> Counter:
            request = server.recv(TIMEOUT_MS)
            barrier.wait()  # every server holds a request
            server.send(request.upper())
            answered = answer_until_the_end(server, way)
            answered[request] += 1
            return answered

        def ask(k: int) -> None:
            with clients[k] as client:
                for start in range(0, count, 10):  # ten requests in flight, then their ten replies, in order
                    for i in range(start, start + 10):
                        client.send(f"{k} {i}".encode(), TIMEOUT_MS)
                    for i in range(start, start + 10):
                        assert client.recv(TIMEOUT_MS) == f"{k} {i}".encode().upper()

        with ThreadPoolExecutor(8) as pool:
            answering = [pool.submit(serve, server, k % 3) for k, server in enumerate(servers)]
            for future in [pool.submit(ask, k) for k in range(4)]:
                future.result(timeout=60)
            service.close()
            answered = [future.result(timeout=60) for future in answering]
    for server in servers:
        server.close()
    assert sum(answered, Counter()) == Counter(f"{k} {i}".encode() for k in range(4) for i in range(count))
    assert all(answered), "a server answered nothing"


def test_a_clients_replies_come_in_the_order_of_its_requests_whoever_replies_first():
    name = unique_name("order")
    with service_create(name, 4096, 64, 1, 2), service_join(name, CLIENT, TIMEOUT_MS) as client, \
            service_join(name, SERVER, TIMEOUT_MS) as slow, service_join(name, SERVER, TIMEOUT_MS) as fast:
        client.send(b"first", TIMEOUT_MS)
        client.send(b"second", TIMEOUT_MS)
        assert slow.recv(TIMEOUT_MS) == b"first"  # each client's requests are taken in the order it sent them
        assert fast.recv(TIMEOUT_MS) == b"second"
        fast.send(b"SECOND")
        raises(Result.TIMEOUT, client.recv, 50)  # the reply to the first request comes first: it isn't there yet
        slow.send(b"FIRST")
        assert client.recv(TIMEOUT_MS) == b"FIRST"
        assert client.recv(TIMEOUT_MS) == b"SECOND"
        raises(Result.TIMEOUT, client.recv, NO_WAIT)


def test_a_server_that_closes_holding_a_request_abandons_it_and_the_next_request_is_answered():
    name = unique_name("abandoned")
    with service_create(name, 4096, 64, 1, 2), service_join(name, CLIENT, TIMEOUT_MS) as client, \
            service_join(name, SERVER, TIMEOUT_MS) as other:
        quitter = service_join(name, SERVER, TIMEOUT_MS)
        client.send(b"one", TIMEOUT_MS)
        assert quitter.recv(TIMEOUT_MS) == b"one"
        quitter.close()  # holding it: the request is answered ABANDONED
        error = raises(Result.ABANDONED, client.recv, TIMEOUT_MS)
        assert error.lost is None and error.count is None
        raises(Result.TIMEOUT, client.recv, NO_WAIT)  # answered once: nothing more is owed

        # call() raises ABANDONED too, and the request counts as answered: the next call gets its own reply
        quitter = service_join(name, SERVER, TIMEOUT_MS)
        with ThreadPoolExecutor(1) as pool:
            calling = pool.submit(client.call, b"two", TIMEOUT_MS)
            assert quitter.recv(TIMEOUT_MS) == b"two"
            quitter.close()
            with pytest.raises(FipcError) as caught:
                calling.result(timeout=TIMEOUT_MS / 1000)
            assert caught.value.result == Result.ABANDONED
            answering = pool.submit(lambda: other.send(other.recv(TIMEOUT_MS).upper()))
            assert client.call(b"three", TIMEOUT_MS) == b"THREE"
            answering.result(timeout=TIMEOUT_MS / 1000)


def test_each_roles_calls_what_it_cant_use_is_invalid_and_max_piece_is_max_message():
    name = unique_name("roles")
    with service_create(name, 1 << 14, 1000, 1, 1) as service, service_join(name, CLIENT, TIMEOUT_MS) as client, \
            service_join(name, SERVER, TIMEOUT_MS) as server:  # the owner's thread joins both: the library admits them
        assert client.max_piece() == server.max_piece() == 1000
        raises(Result.TOO_LARGE, client.send, bytes(1001))
        raises(Result.INVALID, client.send, b"")
        raises(Result.TOO_LARGE, client.send_acquire, 1001)
        raises(Result.INVALID, client.send_commit, 1)  # no reservation
        raises(Result.TIMEOUT, client.recv, NO_WAIT)  # no reply to come
        raises(Result.INVALID, client.wait_for_members)  # a broadcast publisher's call
        client.recv_release()  # nothing held: does nothing
        raises(Result.INVALID, server.send, b"x")  # no request held
        raises(Result.INVALID, server.send_acquire, 1)
        raises(Result.INVALID, server.send_commit, 1)
        raises(Result.TIMEOUT, server.recv, NO_WAIT)
        raises(Result.INVALID, server.wait_for_members)
        raises(Result.INVALID, server.call, b"x")  # call is a client's
        raises(Result.ADDR_IN_USE, service_create, name, 4096, 64, 1, 1)
        raises(Result.ADDR_IN_USE, queue_create, name, 4096, 1, 1)

        # A server holds one request: its receives are INVALID until it replies; recv_release does nothing
        client.send(bytes([7]) * 1000, TIMEOUT_MS)
        raises(Result.TOO_LARGE, server.recv_into, bytearray(999), TIMEOUT_MS)  # takes nothing
        view = server.recv_acquire(TIMEOUT_MS)
        assert view.readonly and len(view) == 1000
        raises(Result.INVALID, server.recv, NO_WAIT)
        raises(Result.INVALID, server.recv_acquire, NO_WAIT)
        server.recv_release()  # does nothing: the request is still held
        raises(Result.TOO_LARGE, server.send, bytes(1001))
        room = server.send_acquire(1000)
        room[:] = bytes([3]) * 1000
        assert bytes(view) == bytes([7]) * 1000  # the request held in place stays whole until the commit
        raises(Result.INVALID, server.send_commit, 1001)
        server.send_commit(1000)
        raises(Result.INVALID, server.send, b"x")  # nothing held any more
        raises(Result.TOO_LARGE, client.recv_into, bytearray(10), TIMEOUT_MS)  # takes nothing
        assert client.recv(TIMEOUT_MS) == bytes([3]) * 1000

        # The zero-copy calls the other way: a request committed in place, its reply received in place
        room = client.send_acquire(5, TIMEOUT_MS)
        room[:] = b"hello"
        client.send_commit(5)
        assert server.recv(TIMEOUT_MS) == b"hello"
        server.send(b"HELLO")
        view = client.recv_acquire(TIMEOUT_MS)
        assert view.readonly and bytes(view) == b"HELLO"
        client.recv_release()
        assert service.wait_for_members(CLIENT) == service.wait_for_members(SERVER) == 1


def test_service_create_refuses_what_is_out_of_range_and_the_other_kinds_roles_are_refused():
    name = unique_name("range")
    for args in ((1000, 64, 1, 1), (4096, 0, 1, 1), (4096, 1025, 1, 1), (4096, 64, 0, 1), (4096, 64, 65, 1),
                 (4096, 64, 1, 0), (4096, 64, 1, 65)):
        raises(Result.INVALID, service_create, name, *args)
    with service_create(name, 4096, 64, 1, 1):
        for role in (SUBSCRIBER_LOSSY, SUBSCRIBER_GATED, PRODUCER, CONSUMER, 7):  # no role of a service's
            raises(Result.INVALID, service_join, name, role, NO_WAIT)
        raises(Result.INVALID, broadcast_join, name, SUBSCRIBER_GATED, TIMEOUT_MS)  # a service's name, other kinds
        raises(Result.INVALID, queue_join, name, CONSUMER, TIMEOUT_MS)
        raises(Result.INVALID, queue_join, name, CLIENT, TIMEOUT_MS)
    queue_name = unique_name("a_queue")
    with queue_create(queue_name, 4096, 1, 1):
        for role in (CLIENT, SERVER):
            raises(Result.INVALID, service_join, queue_name, role, TIMEOUT_MS)


def test_a_call_that_gives_up_leaves_its_reply_owed_and_the_next_call_drops_it():
    """A call whose receive timed out or was cancelled leaves its reply to come; the next call receives and drops it
    (within its own timeout) before it sends, so it returns the reply to its own request. A call refuses (INVALID) while
    a request sent with send() still has its reply to be received."""
    name = unique_name("owed")
    with service_create(name, 4096, 64, 1, 1), service_join(name, CLIENT, TIMEOUT_MS) as client, \
            service_join(name, SERVER, TIMEOUT_MS) as server:
        raises(Result.TIMEOUT, client.call, b"late", 50)  # the request went: no server replied in time
        assert server.recv(TIMEOUT_MS) == b"late"
        server.send(b"LATE")  # the late reply, owed to the call that gave up

        with ThreadPoolExecutor(1) as pool:
            answering = pool.submit(lambda: server.send(server.recv(TIMEOUT_MS).upper()))
            assert client.call(b"next", TIMEOUT_MS) == b"NEXT"  # not LATE
            answering.result(timeout=TIMEOUT_MS / 1000)
        raises(Result.TIMEOUT, client.recv, NO_WAIT)  # nothing more owed

        # A call whose owed reply hasn't come waits for it within its own timeout, and gives up before it sends
        raises(Result.TIMEOUT, client.call, b"a", NO_WAIT)  # a went; its reply is owed
        raises(Result.TIMEOUT, client.call, b"b", 50)  # a's reply hasn't come: b never goes
        assert server.recv(TIMEOUT_MS) == b"a"
        server.send(b"A")
        raises(Result.TIMEOUT, server.recv, NO_WAIT)  # no b
        with ThreadPoolExecutor(1) as pool:
            answering = pool.submit(lambda: server.recv(TIMEOUT_MS))
            raises(Result.TIMEOUT, client.call, b"c", 200)  # drops A, sends c, and gives up on its reply
            assert answering.result(timeout=TIMEOUT_MS / 1000) == b"c"
        server.send(b"C")  # owed to the call that gave up
        assert client.recv(TIMEOUT_MS) == b"C"  # a receive of the caller's own takes it, in request order
        raises(Result.TIMEOUT, client.recv, NO_WAIT)

        # A request of send() whose reply hasn't been received: call refuses, and leaves it for recv
        client.send(b"mine", TIMEOUT_MS)
        raises(Result.INVALID, client.call, b"x", TIMEOUT_MS)
        server.send(server.recv(TIMEOUT_MS).upper())
        assert client.recv(TIMEOUT_MS) == b"MINE"
        with ThreadPoolExecutor(1) as pool:
            answering = pool.submit(lambda: server.send(server.recv(TIMEOUT_MS).upper()))
            assert client.call(b"x", TIMEOUT_MS) == b"X"
            answering.result(timeout=TIMEOUT_MS / 1000)


def answer_one(pool: ThreadPoolExecutor, server: Conn):
    """Answers the next request with its uppercase, on a thread of the pool."""
    return pool.submit(lambda: server.send(server.recv(TIMEOUT_MS).upper()))


def test_a_call_checks_its_request_first_even_while_a_reply_is_owed():
    """An empty request is INVALID and one over max_piece() TOO_LARGE at once, before the call waits for the reply
    owed to a call that gave up (which hasn't come); a request that isn't bytes raises TypeError at once. None of them
    counts: the owed reply stays owed, and the next call drops it."""
    name = unique_name("check")
    with service_create(name, 4096, 64, 1, 1), service_join(name, CLIENT, TIMEOUT_MS) as client, \
            service_join(name, SERVER, TIMEOUT_MS) as server:
        raises(Result.TIMEOUT, client.call, b"late", 50)
        assert server.recv(TIMEOUT_MS) == b"late"  # held: its reply hasn't come
        started = time.monotonic()
        raises(Result.INVALID, client.call, b"", FOREVER)
        raises(Result.TOO_LARGE, client.call, bytes(65), FOREVER)
        with pytest.raises(TypeError):
            client.call("not bytes", FOREVER)  # type: ignore[arg-type]
        assert time.monotonic() - started < 1
        server.send(b"LATE")
        with ThreadPoolExecutor(1) as pool:
            answering = answer_one(pool, server)
            assert client.call(bytes(64), TIMEOUT_MS) == bytes(64)  # drops LATE; max_piece() bytes go
            answering.result(timeout=TIMEOUT_MS / 1000)
        raises(Result.TIMEOUT, client.recv, NO_WAIT)


def test_a_call_whose_time_runs_out_on_the_owed_reply_never_sends_its_request():
    """A call that drops an owed reply and has no time left then raises TIMEOUT without sending: the server never sees
    that request. With NO_WAIT it drops a reply that is in and sends. (The drop is made slow here by a receive that
    sleeps after it returns.)"""
    name = unique_name("spent")
    with service_create(name, 4096, 64, 1, 1), service_join(name, CLIENT, TIMEOUT_MS) as client, \
            service_join(name, SERVER, TIMEOUT_MS) as server:
        raises(Result.TIMEOUT, client.call, b"late", 50)
        assert server.recv(TIMEOUT_MS) == b"late"
        server.send(b"LATE")  # in the client's ring: the next call's drop needn't wait for it
        receive = client._recv

        def slow(timeout_ms: int) -> int:
            length = receive(timeout_ms)
            time.sleep(0.1)
            return length

        client._recv = slow  # type: ignore[method-assign]
        raises(Result.TIMEOUT, client.call, b"lost", 50)  # LATE dropped, its 50 ms gone: lost never goes
        del client._recv
        raises(Result.TIMEOUT, server.recv, 100)  # no request
        raises(Result.TIMEOUT, client.recv, NO_WAIT)  # nothing owed: LATE was dropped

        raises(Result.TIMEOUT, client.call, b"a", NO_WAIT)  # a went; its reply is owed
        assert server.recv(TIMEOUT_MS) == b"a"
        server.send(b"A")
        raises(Result.TIMEOUT, client.call, b"b", NO_WAIT)  # drops A (in), sends b without waiting
        assert server.recv(TIMEOUT_MS) == b"b"
        server.send(b"B")
        with ThreadPoolExecutor(1) as pool:
            answering = answer_one(pool, server)
            assert client.call(b"c", TIMEOUT_MS) == b"C"  # drops B
            answering.result(timeout=TIMEOUT_MS / 1000)


def test_an_abandoned_owed_to_a_call_that_gave_up_is_dropped_too():
    """A call gives up, and the server holding its request closes: the request's ABANDONED is owed in its reply's
    place, and the next call drops it and returns its own reply."""
    name = unique_name("owed_abandoned")
    with service_create(name, 4096, 64, 1, 2), service_join(name, CLIENT, TIMEOUT_MS) as client, \
            service_join(name, SERVER, TIMEOUT_MS) as other, ThreadPoolExecutor(1) as pool:
        quitter = service_join(name, SERVER, TIMEOUT_MS)
        raises(Result.TIMEOUT, client.call, b"one", 50)
        assert quitter.recv(TIMEOUT_MS) == b"one"  # the other server doesn't receive
        quitter.close()  # holding it: ABANDONED, owed to the call that gave up
        answering = answer_one(pool, other)
        assert client.call(b"two", TIMEOUT_MS) == b"TWO"
        answering.result(timeout=TIMEOUT_MS / 1000)
        raises(Result.TIMEOUT, client.recv, NO_WAIT)


def test_a_call_after_the_services_end():
    """DISCONNECTED from the drop of a reply that never came, DISCONNECTED from the send with nothing owed, and INVALID
    while a reply to a request of send() is owed (the reply is the caller's; its receive raises DISCONNECTED)."""
    name = unique_name("call_end")
    service = service_create(name, 4096, 64, 3, 1)
    owing, idle, sender = (service_join(name, CLIENT, TIMEOUT_MS) for _ in range(3))
    with service_join(name, SERVER, TIMEOUT_MS) as server:
        raises(Result.TIMEOUT, owing.call, b"held", 50)
        assert server.recv(TIMEOUT_MS) == b"held"  # never answered
        sender.send(b"mine", TIMEOUT_MS)
        service.close()
        raises(Result.DISCONNECTED, owing.call, b"after", TIMEOUT_MS)  # the drop
        raises(Result.DISCONNECTED, idle.call, b"after", TIMEOUT_MS)  # the send
        error = raises(Result.INVALID, sender.call, b"after", TIMEOUT_MS)
        assert "receive it first" in str(error)
        raises(Result.DISCONNECTED, sender.recv, TIMEOUT_MS)
    for client in (owing, idle, sender):
        client.close()


def test_a_send_or_call_whose_argument_isnt_bytes_counts_nothing():
    """A TypeError from a send's or a call's argument leaves nothing owed: the next call goes on."""
    name = unique_name("not_bytes")
    with service_create(name, 4096, 64, 1, 1), service_join(name, CLIENT, TIMEOUT_MS) as client, \
            service_join(name, SERVER, TIMEOUT_MS) as server, ThreadPoolExecutor(1) as pool:
        with pytest.raises(TypeError):
            client.send("not bytes", TIMEOUT_MS)  # type: ignore[arg-type]
        with pytest.raises(TypeError):
            client.call("not bytes", TIMEOUT_MS)  # type: ignore[arg-type]
        answering = answer_one(pool, server)
        assert client.call(b"bytes", TIMEOUT_MS) == b"BYTES"
        answering.result(timeout=TIMEOUT_MS / 1000)
        raises(Result.TIMEOUT, server.recv, NO_WAIT)


def test_a_cancel_ends_a_clients_and_a_servers_wait_and_a_cancelled_call_leaves_its_reply_owed():
    name = unique_name("cancel")
    with service_create(name, 4096, 64, 2, 1), service_join(name, SERVER, TIMEOUT_MS) as server, \
            ThreadPoolExecutor(1) as pool:
        client = service_join(name, CLIENT, TIMEOUT_MS)
        calling = pool.submit(client.call, b"cancelled", FOREVER)
        assert server.recv(TIMEOUT_MS) == b"cancelled"
        client.cancel()  # from another thread: the call's wait for its reply raises CANCELLED
        with pytest.raises(FipcError) as caught:
            calling.result(timeout=TIMEOUT_MS / 1000)
        assert caught.value.result == Result.CANCELLED
        server.send(b"CANCELLED")  # owed to the cancelled call
        assert client.recv(NO_WAIT) == b"CANCELLED"  # a receive that needn't wait still works
        raises(Result.CANCELLED, client.recv, TIMEOUT_MS)  # final
        client.close()

        with service_join(name, CLIENT, TIMEOUT_MS) as client:  # the server goes on
            waiting = pool.submit(server.recv, FOREVER)
            server.cancel()
            with pytest.raises(FipcError) as caught:
                waiting.result(timeout=TIMEOUT_MS / 1000)
            assert caught.value.result == Result.CANCELLED
            client.send(b"after", TIMEOUT_MS)
            assert server.recv(TIMEOUT_MS) == b"after"  # a request there: the receive needn't wait
            server.send(b"AFTER")  # a reply never waits
            assert client.recv(TIMEOUT_MS) == b"AFTER"
            raises(Result.CANCELLED, server.recv, TIMEOUT_MS)  # final


def test_a_call_after_cancel_raises_cancelled_and_sends_nothing():
    """A call on a cancelled connection raises CANCELLED, whatever its timeout, after its request's checks, and sends
    nothing: its reply could never be received, so the server, which listens, sees no request."""
    name = unique_name("call_cancelled")
    with service_create(name, 4096, 64, 1, 1), service_join(name, SERVER, TIMEOUT_MS) as server, \
            service_join(name, CLIENT, TIMEOUT_MS) as client:
        client.cancel()
        raises(Result.INVALID, client.call, b"", TIMEOUT_MS)  # the request's checks come first
        raises(Result.TOO_LARGE, client.call, b"x" * 65, TIMEOUT_MS)
        for timeout_ms in (FOREVER, NO_WAIT, TIMEOUT_MS):
            raises(Result.CANCELLED, client.call, b"never sent", timeout_ms)
        raises(Result.TIMEOUT, server.recv, 200)


def test_the_owners_wait_for_members_and_its_cancel():
    name = unique_name("members")
    with service_create(name, 4096, 64, 2, 3) as service, ThreadPoolExecutor(1) as pool:
        assert service.wait_for_members() == 0
        assert service.wait_for_members(CLIENT) == service.wait_for_members(SERVER) == 0
        error = raises(Result.TIMEOUT, service.wait_for_members, None, 1, NO_WAIT)
        assert error.count == 0 and error.lost is None

        waiting = pool.submit(service.wait_for_members, SERVER, 1, TIMEOUT_MS)  # on another thread, while one joins
        with service_join(name, SERVER, TIMEOUT_MS), service_join(name, CLIENT, TIMEOUT_MS):
            assert waiting.result(timeout=TIMEOUT_MS / 1000) >= 1
            assert service.wait_for_members(None, 2, TIMEOUT_MS) == 2
            assert service.wait_for_members(CLIENT) == service.wait_for_members(SERVER) == 1
            assert raises(Result.TIMEOUT, service.wait_for_members, CLIENT, 2, 50).count == 1

            # Over the role's maximum (max_clients, max_servers, their sum for both), or another role
            for role, at_least in ((CLIENT, 3), (SERVER, 4), (None, 6), (PRODUCER, 1)):
                assert raises(Result.INVALID, service.wait_for_members, role, at_least).count is None

            waiting = pool.submit(service.wait_for_members, SERVER, 2, TIMEOUT_MS)
            service.cancel()  # from this thread: the wait, now or about to start, raises CANCELLED
            with pytest.raises(FipcError) as caught:
                waiting.result(timeout=TIMEOUT_MS / 1000)
            assert caught.value.result == Result.CANCELLED and caught.value.count == 1
            assert raises(Result.CANCELLED, service.wait_for_members, None, 3, TIMEOUT_MS).count == 2  # final

            # The service goes on after its cancel: members join, and a call goes through
            with service_join(name, CLIENT, TIMEOUT_MS) as client, service_join(name, SERVER, TIMEOUT_MS) as server:
                answering = pool.submit(lambda: server.send(server.recv(TIMEOUT_MS).upper()))
                assert client.call(b"after the cancel", TIMEOUT_MS) == b"AFTER THE CANCEL"
                answering.result(timeout=TIMEOUT_MS / 1000)


def test_the_services_close_ends_it_for_clients_and_servers():
    name = unique_name("end")
    service = service_create(name, 4096, 64, 1, 2)
    client = service_join(name, CLIENT, TIMEOUT_MS)
    server = service_join(name, SERVER, TIMEOUT_MS)
    holder = service_join(name, SERVER, TIMEOUT_MS)
    client.send(b"done", TIMEOUT_MS)
    client.send(b"held", TIMEOUT_MS)
    server.send(server.recv(TIMEOUT_MS).upper())
    assert holder.recv(TIMEOUT_MS) == b"held"
    service.close()
    service.close()  # a second close does nothing
    raises(Result.DISCONNECTED, client.send, b"after", TIMEOUT_MS)
    raises(Result.DISCONNECTED, server.recv, TIMEOUT_MS)
    assert client.recv(TIMEOUT_MS) == b"DONE"  # the replies that came before the end, then DISCONNECTED
    raises(Result.DISCONNECTED, client.recv, TIMEOUT_MS)
    raises(Result.DISCONNECTED, holder.send, b"HELD")  # a reply after the end
    with service_create(name, 4096, 64, 1, 1):  # the name is free again at once
        pass
    for conn in (client, server, holder):
        conn.close()


def test_a_service_lives_on_when_its_object_is_dropped():
    """The service has no finalizer: one created in a function that keeps only a joined server goes on through the
    garbage collector's runs, and holds its name, until its close (here through the native handle, which the test
    kept) or the end of the process."""
    name = unique_name("dropped")
    handles = []

    def start() -> Conn:
        service = service_create(name, 4096, 64, 1, 1)
        handles.append(service._handle)
        return service_join(name, SERVER, TIMEOUT_MS)

    server = start()
    for _ in range(2):
        gc.collect()
    with service_join(name, CLIENT, TIMEOUT_MS) as client:
        client.send(b"still here", TIMEOUT_MS)
        server.send(server.recv(TIMEOUT_MS).upper())
        assert client.recv(TIMEOUT_MS) == b"STILL HERE"
        raises(Result.ADDR_IN_USE, service_create, name, 4096, 64, 1, 1)
        lib.fipc_service_close(handles[0])
        raises(Result.DISCONNECTED, client.recv, TIMEOUT_MS)
    server.close()


def test_a_server_conn_collected_unclosed_abandons_the_request_it_holds():
    """A Conn left unclosed is closed when it is garbage-collected: a server's answers the request it holds ABANDONED,
    as its close does. A Conn closed first is closed once: its collection closes nothing again."""
    name = unique_name("collected")
    with service_create(name, 4096, 64, 1, 1), service_join(name, CLIENT, TIMEOUT_MS) as client:
        server = service_join(name, SERVER, TIMEOUT_MS)
        client.send(b"held", TIMEOUT_MS)
        request = server.recv(TIMEOUT_MS)
        assert request == b"held"
        raises(Result.TIMEOUT, client.recv, NO_WAIT)  # held: no answer yet
        del server
        gc.collect()
        raises(Result.ABANDONED, client.recv, TIMEOUT_MS)

        closed = service_join(name, SERVER, TIMEOUT_MS)
        closed.close()
        with service_join(name, SERVER, TIMEOUT_MS) as server:
            del closed
            gc.collect()
            client.send(b"after", TIMEOUT_MS)
            server.send(server.recv(TIMEOUT_MS).upper())
            assert client.recv(TIMEOUT_MS) == b"AFTER"


def test_a_view_in_place_keeps_its_conn_open_after_the_conns_variable_is_dropped():
    """A memoryview of a request held in place, or of a reply's reserved room, keeps its Conn referenced: the
    connection is not collected, and its memory stays mapped, until the last view (a slice of one too) is gone."""
    name = unique_name("views")
    with service_create(name, 4096, 64, 1, 1), service_join(name, CLIENT, TIMEOUT_MS) as client:
        server = service_join(name, SERVER, TIMEOUT_MS)
        client.send(b"in place", TIMEOUT_MS)
        request = server.recv_acquire(TIMEOUT_MS)
        room = server.send_acquire(len(request))
        del server
        gc.collect()
        assert bytes(request) == b"in place"  # still mapped, and still held
        room[:] = bytes(request).upper()  # still writable
        raises(Result.TIMEOUT, client.recv, NO_WAIT)  # the server is open, holding the request
        tail = request[3:]
        del request
        gc.collect()
        assert bytes(tail) == b"place"
        raises(Result.TIMEOUT, client.recv, NO_WAIT)
        del tail
        gc.collect()
        assert bytes(room) == b"IN PLACE"
        raises(Result.TIMEOUT, client.recv, NO_WAIT)  # the room still holds the Conn
        del room
        gc.collect()
        raises(Result.ABANDONED, client.recv, TIMEOUT_MS)  # the last view gone, the Conn is collected


def test_requests_longer_than_the_receive_buffer_are_received_whole_on_both_sides():
    name = unique_name("large")
    with service_create(name, 1 << 16, 1 << 14, 1, 1), service_join(name, CLIENT, TIMEOUT_MS) as client, \
            service_join(name, SERVER, TIMEOUT_MS) as server:
        request = bytes(range(256)) * 64  # more than the receive buffer's first 4 KiB
        client.send(request, TIMEOUT_MS)
        assert server.recv(TIMEOUT_MS) == request
        server.send(request[::-1])
        assert client.recv(TIMEOUT_MS) == request[::-1]


def test_the_examples_exchange_in_either_order():
    """The server creates the service, joins it as its server, answers three requests and closes; the client calls
    ping 1 to ping 3 and checks each reply. Client first: its thread calls join before the service is created, which
    happens 200 ms later (nothing outside the join shows that it is waiting, so the pause makes that likely, not
    certain). Client last: it joins once the server waits for a request."""
    for client_first in (True, False):
        name = unique_name("example")
        joining = threading.Event()

        def ask() -> None:
            joining.set()
            service_peer.join_and_call(name, 3)

        with ThreadPoolExecutor(1) as pool:
            if client_first:
                asking = pool.submit(ask)
                assert joining.wait(TIMEOUT_MS / 1000)
                time.sleep(0.2)
            with service_create(name, 4096, 256, 1, 1), service_join(name, SERVER) as server:
                if not client_first:
                    asking = pool.submit(ask)
                for _ in range(3):
                    server.send(server.recv().upper())
            asking.result(timeout=TIMEOUT_MS / 1000)


def test_a_server_in_another_process_and_a_client_here():
    name = unique_name("server_peer")
    peer = subprocess.Popen([sys.executable, str(PEER), "server", name, "300"])
    try:
        service_peer.join_and_call(name, 300)
    finally:
        assert peer.wait(timeout=60) == 0, "the server peer failed"


def test_a_server_here_and_a_client_in_another_process():
    name = unique_name("client_peer")
    peer = subprocess.Popen([sys.executable, str(PEER), "client", name, "300"])
    try:
        service_peer.serve(name, 300, 1)
    finally:
        assert peer.wait(timeout=60) == 0, "the client peer failed"


def test_two_joined_servers_in_other_processes_and_the_owners_client_here():
    name = unique_name("joined_peer")
    peers = [subprocess.Popen([sys.executable, str(PEER), "joined-server", name]) for _ in range(2)]
    try:
        service_peer.own_and_call(name, 300, 2)
    finally:
        for peer in peers:
            assert peer.wait(timeout=60) == 0, "a server peer failed"


def test_two_client_processes_and_the_owners_server_here():
    name = unique_name("clients_peer")
    peers = [subprocess.Popen([sys.executable, str(PEER), "client", name, "300"]) for _ in range(2)]
    try:
        service_peer.serve(name, 300, 2)
    finally:
        for peer in peers:
            assert peer.wait(timeout=60) == 0, "a client peer failed"


def test_a_server_process_killed_holding_a_request_abandons_it():
    name = unique_name("killed")
    with service_create(name, 4096, 64, 1, 2), service_join(name, CLIENT, TIMEOUT_MS) as client:
        peer = subprocess.Popen([sys.executable, str(PEER), "holding-server", name], stdout=subprocess.PIPE, text=True)
        try:
            client.send(b"doomed", TIMEOUT_MS)
            assert peer.stdout.readline().strip() == "holding"
            peer.kill()
            peer.wait(timeout=60)
            raises(Result.ABANDONED, client.recv, TIMEOUT_MS)  # once the owner's process has seen its end
            with service_join(name, SERVER, TIMEOUT_MS) as server, ThreadPoolExecutor(1) as pool:
                answering = pool.submit(lambda: server.send(server.recv(TIMEOUT_MS).upper()))
                assert client.call(b"next", TIMEOUT_MS) == b"NEXT"
                answering.result(timeout=TIMEOUT_MS / 1000)
        finally:
            peer.kill()
            peer.wait(timeout=60)
            peer.stdout.close()
