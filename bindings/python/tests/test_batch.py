"""Batches through the binding (send_many, recv_many): on a stream, in order and whole, a buffer that grows for a first
message longer than it, max_count, NO_WAIT's partial send with how many went, an empty message refused before any
goes; a broadcast's and a work queue's batches; a service client's batch of requests, owed as its sends are, with
their replies in order, and a server's batches refused."""

import itertools
import os
import threading

import pytest

from fipc import (CLIENT, CONSUMER, FOREVER, NO_WAIT, PRODUCER, SERVER, SUBSCRIBER_GATED, SUBSCRIBER_LOSSY, Conn,
                  FipcError, Result, broadcast_create, broadcast_join, queue_create, queue_join, service_create,
                  service_join, stream_connect, stream_create)

TIMEOUT_MS = 10000
_names = itertools.count()


def unique_name(what: str) -> str:
    return f"py_b_{what}_{os.getpid()}_{next(_names)}"


def raises(result: Result, call, *args) -> FipcError:
    with pytest.raises(FipcError) as caught:
        call(*args)
    assert caught.value.result == result, caught.value
    return caught.value


def stream_pair(name: str, capacity: int):
    listener = stream_create(name, capacity)
    connected = []
    connecting = threading.Thread(target=lambda: connected.append(stream_connect(name, TIMEOUT_MS)))
    connecting.start()
    server = listener.accept(TIMEOUT_MS)
    connecting.join()
    listener.close()
    return server, connected[0]


def receive_all(conn: Conn, count: int, max_count: int = 16) -> list:
    """`count` messages, received in batches (a broadcast's subscriber may take fewer than are there)."""
    received = []
    while len(received) < count:
        batch = conn.recv_many(max_count, TIMEOUT_MS)
        assert 1 <= len(batch) <= max_count
        received += batch
    return received


def test_a_streams_batches_come_in_order_and_whole():
    server, client = stream_pair(unique_name("stream"), 1024)
    with server, client:
        messages = [bytes([i]) * (10 + i * 20) for i in range(6)]
        assert client.send_many(messages, TIMEOUT_MS) == 6
        assert server.recv_many(16, TIMEOUT_MS) == messages
        assert client.send_many(messages, TIMEOUT_MS) == 6
        assert server.recv_many(2, TIMEOUT_MS) == messages[:2]  # max_count
        assert server.recv_many(16, TIMEOUT_MS) == messages[2:]

        # A message longer than one piece goes in pieces and arrives whole, in a buffer grown for it
        long = bytes(range(256)) * 1000
        received = []
        receiving = threading.Thread(target=lambda: received.extend(receive_all(server, 3)))
        receiving.start()
        assert client.send_many([b"a", long, b"b"], TIMEOUT_MS) == 3
        receiving.join()
        assert received == [b"a", long, b"b"]
        raises(Result.TIMEOUT, server.recv_many, 16, NO_WAIT)


def test_a_partial_send_reports_how_many_went_and_an_empty_message_sends_none():
    server, client = stream_pair(unique_name("partial"), 1024)
    with server, client:
        hundred = bytes(range(100))
        # Frames of 128 bytes: the ring of 1 KiB holds 8, fewer where its end is padded
        partial = raises(Result.TIMEOUT, client.send_many, [hundred] * 100, NO_WAIT)
        assert 1 <= partial.sent <= 8
        assert receive_all(server, partial.sent) == [hundred] * partial.sent

        received = []
        receiving = threading.Thread(target=lambda: received.extend(receive_all(server, 100)))
        receiving.start()
        assert client.send_many([hundred] * 100, TIMEOUT_MS) == 100  # sent while it is received
        receiving.join()
        assert received == [hundred] * 100

        empty = raises(Result.INVALID, client.send_many, [hundred, b""], NO_WAIT)
        assert empty.sent == 0
        raises(Result.INVALID, client.send_many, [], NO_WAIT)
        raises(Result.INVALID, server.recv_many, 0, NO_WAIT)
        raises(Result.TIMEOUT, server.recv_many, 16, NO_WAIT)  # nothing went


def test_a_broadcasts_batches_reach_every_subscriber():
    name = unique_name("broadcast")
    with broadcast_create(name, 1 << 16, 4, FOREVER) as publisher:
        with broadcast_join(name, SUBSCRIBER_LOSSY, TIMEOUT_MS) as lossy, \
                broadcast_join(name, SUBSCRIBER_GATED, TIMEOUT_MS) as gated:
            assert publisher.wait_for_members(2, TIMEOUT_MS) == 2
            messages = [f"tick {i}".encode() for i in range(40)]
            assert publisher.send_many(messages, TIMEOUT_MS) == 40
            publisher.close()  # what it sent is received first
            for subscriber in (lossy, gated):
                assert receive_all(subscriber, 40) == messages
                raises(Result.DISCONNECTED, subscriber.recv_many, 16, TIMEOUT_MS)
            raises(Result.INVALID, gated.send_many, [b"x"], NO_WAIT)


def test_a_broadcasts_and_a_queues_batches_refuse_a_message_over_max_piece():
    name = unique_name("too_large")
    with broadcast_create(name, 1 << 16, 1, FOREVER) as publisher:
        sent = raises(Result.TOO_LARGE, publisher.send_many, [b"a", bytes(publisher.max_piece() + 1)], NO_WAIT)
        assert sent.sent == 0
        raises(Result.INVALID, publisher.recv_many, 16, NO_WAIT)
    name = unique_name("queue_too_large")
    with queue_create(name, 4096, 1, 1), queue_join(name, PRODUCER, TIMEOUT_MS) as producer:
        raises(Result.TOO_LARGE, producer.send_many, [b"a", bytes(producer.max_piece() + 1)], NO_WAIT)


def test_a_queues_batches_deliver_every_message_once():
    name = unique_name("queue")
    with queue_create(name, 4096, 1, 1) as queue, queue_join(name, PRODUCER, TIMEOUT_MS) as producer, \
            queue_join(name, CONSUMER, TIMEOUT_MS) as consumer:
        messages = [f"job {i}".encode() for i in range(200)]
        received = []
        receiving = threading.Thread(target=lambda: received.extend(receive_all(consumer, 200)))
        receiving.start()
        for at in range(0, 200, 50):
            assert producer.send_many(messages[at:at + 50], TIMEOUT_MS) == 50
        receiving.join()
        assert received == messages  # one producer: its messages in order
        raises(Result.INVALID, consumer.send_many, [b"x"], NO_WAIT)
        raises(Result.INVALID, producer.recv_many, 16, NO_WAIT)
        queue.close()
        raises(Result.DISCONNECTED, consumer.recv_many, 16, TIMEOUT_MS)
        raises(Result.DISCONNECTED, producer.send_many, [b"x"], TIMEOUT_MS)


def test_a_service_clients_batch_is_owed_as_its_sends_are_and_a_servers_batches_are_invalid():
    name = unique_name("service")
    with service_create(name, 1 << 14, 64, 1, 1), service_join(name, SERVER, TIMEOUT_MS) as server, \
            service_join(name, CLIENT, TIMEOUT_MS) as client:
        requests = [b"a", b"bb", b"ccc"]
        assert client.send_many(requests, TIMEOUT_MS) == 3
        raises(Result.INVALID, client.call, b"d", NO_WAIT)  # those replies are the caller's
        for request in requests:
            assert server.recv(TIMEOUT_MS) == request
            server.send(request + b" done")
        assert receive_all(client, 3) == [b"a done", b"bb done", b"ccc done"]

        answering = threading.Thread(target=lambda: server.send(server.recv(TIMEOUT_MS) + b" done"))
        answering.start()
        assert client.call(b"e", TIMEOUT_MS) == b"e done"  # nothing owed now
        answering.join()
        raises(Result.INVALID, server.send_many, requests, NO_WAIT)
        raises(Result.INVALID, server.recv_many, 16, NO_WAIT)

        # A batch whose requests don't all fit counts only those that went: once their replies are received, a call
        # goes again. Each request holds 64 + 64 bytes of the 16 KiB ring: at most 128 fit
        partial = raises(Result.TIMEOUT, client.send_many, [b"f"] * 200, NO_WAIT)
        assert 1 <= partial.sent <= 128
        for _ in range(partial.sent):
            server.send(server.recv(TIMEOUT_MS))
        assert receive_all(client, partial.sent) == [b"f"] * partial.sent
        answering = threading.Thread(target=lambda: server.send(server.recv(TIMEOUT_MS) + b" done"))
        answering.start()
        assert client.call(b"g", TIMEOUT_MS) == b"g done"
        answering.join()
