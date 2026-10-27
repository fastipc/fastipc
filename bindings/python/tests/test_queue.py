"""The work queue through the binding: producers and consumers in this process, with every message delivered once,
the owner's own members, the calls a queue's roles refuse, the owner's wait for members and its cancel, the end at
the queue's close, a queue that outlives its dropped object, consumers racing for messages longer than the receive
buffer, the examples' exchange, and producers and consumers in other processes (queue_peer.py, which the other
bindings' tests run as their Python side)."""

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

import queue_peer
from fipc import (CONSUMER, FOREVER, NO_WAIT, PRODUCER, SUBSCRIBER_GATED, SUBSCRIBER_LOSSY, Conn, FipcError, Queue,
                  Result, broadcast_join, lib, queue_create, queue_join)

PEER = Path(__file__).resolve().parent / "queue_peer.py"
TIMEOUT_MS = 10000
_names = itertools.count()


def unique_name(what: str) -> str:
    return f"py_q_{what}_{os.getpid()}_{next(_names)}"


def raises(result: Result, call, *args) -> FipcError:
    with pytest.raises(FipcError) as caught:
        call(*args)
    assert caught.value.result == result, caught.value
    return caught.value


def receive_until_the_end(consumer: Conn, way: int) -> list:
    """Every message the consumer takes until DISCONNECTED: copied (way 0), into a buffer (1) or in place (2)."""
    received = []
    buffer = bytearray(consumer.max_piece())
    while True:
        try:
            if way == 0:
                received.append(consumer.recv(TIMEOUT_MS))
            elif way == 1:
                received.append(bytes(buffer[:consumer.recv_into(buffer, TIMEOUT_MS)]))
            else:
                received.append(bytes(consumer.recv_acquire(TIMEOUT_MS)))
                consumer.recv_release()
        except FipcError as e:
            if e.result != Result.DISCONNECTED:
                raise
            return received


def test_four_producers_and_four_consumers_deliver_every_message_once():
    name = unique_name("many")
    count = 500
    with queue_create(name, 4096, 4, 4) as queue:
        assert isinstance(queue, Queue)
        producers = [queue_join(name, PRODUCER, TIMEOUT_MS) for _ in range(4)]
        consumers = [queue_join(name, CONSUMER, TIMEOUT_MS) for _ in range(4)]
        assert queue.wait_for_members(None, 8, TIMEOUT_MS) == 8
        assert all(conn.max_piece() == 1024 for conn in producers + consumers)

        def produce(k: int) -> None:
            with producers[k] as producer:  # its close leaves the queue; its messages stay
                for i in range(count):
                    producer.send(f"{k} {i}".encode(), TIMEOUT_MS)

        with ThreadPoolExecutor(8) as pool:
            taken = [pool.submit(receive_until_the_end, consumer, k % 3) for k, consumer in enumerate(consumers)]
            for future in [pool.submit(produce, k) for k in range(4)]:
                future.result(timeout=60)
            queue.close()  # ends the queue: the consumers take what is left, then DISCONNECTED
            received = [[tuple(map(int, data.split())) for data in future.result(timeout=60)] for future in taken]
    for consumer in consumers:
        consumer.close()
    everything = sorted(itertools.chain.from_iterable(received))
    assert everything == [(k, i) for k in range(4) for i in range(count)], "a message was lost or delivered twice"


def test_the_owners_members_and_the_calls_their_roles_refuse():
    name = unique_name("roles")
    with queue_create(name, 4096, 1, 1) as queue, queue_join(name, CONSUMER, TIMEOUT_MS) as consumer:
        producer = queue_join(name, PRODUCER, TIMEOUT_MS)  # the owner's thread joins both: the library admits them
        assert producer.max_piece() == consumer.max_piece() == 1024

        # Messages wait in the queue; a consumer takes them copied, into a buffer or in place
        for word in (b"one", b"two", b"three", b"four"):
            producer.send(word, TIMEOUT_MS)
        received = [consumer.recv(TIMEOUT_MS)]
        small = bytearray(2)
        raises(Result.TOO_LARGE, consumer.recv_into, small, TIMEOUT_MS)  # takes nothing
        buffer = bytearray(16)
        length = consumer.recv_into(buffer, TIMEOUT_MS)
        received.append(bytes(buffer[:length]))
        view = consumer.recv_acquire(TIMEOUT_MS)
        assert view.readonly
        received.append(bytes(view))
        consumer.recv_release()
        room = producer.send_acquire(4, TIMEOUT_MS)
        room[:] = b"five"
        producer.send_commit(4)
        received += [consumer.recv(TIMEOUT_MS) for _ in range(2)]
        assert sorted(received) == sorted([b"one", b"two", b"three", b"four", b"five"])

        raises(Result.INVALID, producer.recv, NO_WAIT)
        raises(Result.INVALID, producer.recv_acquire, NO_WAIT)
        raises(Result.INVALID, consumer.send, b"x")
        raises(Result.INVALID, producer.wait_for_members)  # a broadcast publisher's call
        raises(Result.TOO_LARGE, producer.send, bytes(producer.max_piece() + 1))
        raises(Result.ADDR_IN_USE, queue_create, name, 4096, 1, 1)

        # A producer's close doesn't end the queue: the consumer waits for more
        producer.send(b"last", TIMEOUT_MS)
        producer.close()
        assert consumer.recv(TIMEOUT_MS) == b"last"
        raises(Result.TIMEOUT, consumer.recv, NO_WAIT)


def test_the_owners_wait_for_members_and_its_cancel():
    name = unique_name("members")
    with queue_create(name, 4096, 2, 3) as queue, ThreadPoolExecutor(1) as pool:
        assert queue.wait_for_members() == 0
        assert queue.wait_for_members(PRODUCER) == queue.wait_for_members(CONSUMER) == 0
        error = raises(Result.TIMEOUT, queue.wait_for_members, None, 1, NO_WAIT)
        assert error.count == 0 and error.lost is None

        waiting = pool.submit(queue.wait_for_members, CONSUMER, 1, TIMEOUT_MS)  # on another thread, while one joins
        with queue_join(name, CONSUMER, TIMEOUT_MS), queue_join(name, PRODUCER, TIMEOUT_MS):
            assert waiting.result(timeout=TIMEOUT_MS / 1000) >= 1
            assert queue.wait_for_members(None, 2, TIMEOUT_MS) == 2
            assert queue.wait_for_members(PRODUCER) == queue.wait_for_members(CONSUMER) == 1
            assert raises(Result.TIMEOUT, queue.wait_for_members, PRODUCER, 2, 50).count == 1

            # Over the role's maximum (max_producers, max_consumers, their sum for both), or another role
            for role, at_least in ((PRODUCER, 3), (CONSUMER, 4), (None, 6), (SUBSCRIBER_LOSSY, 1)):
                assert raises(Result.INVALID, queue.wait_for_members, role, at_least).count is None

            waiting = pool.submit(queue.wait_for_members, CONSUMER, 2, TIMEOUT_MS)
            queue.cancel()  # from this thread: the wait, now or about to start, raises CANCELLED
            with pytest.raises(FipcError) as caught:
                waiting.result(timeout=TIMEOUT_MS / 1000)
            assert caught.value.result == Result.CANCELLED and caught.value.count == 1
            assert raises(Result.CANCELLED, queue.wait_for_members, None, 3, TIMEOUT_MS).count == 2  # final

            # The queue goes on after its cancel: a member joins, and a message goes through
            with queue_join(name, PRODUCER, TIMEOUT_MS) as producer, queue_join(name, CONSUMER, TIMEOUT_MS) as consumer:
                producer.send(b"after the cancel", TIMEOUT_MS)
                assert consumer.recv(TIMEOUT_MS) == b"after the cancel"


def test_the_queues_close_ends_it_for_producers_and_consumers():
    name = unique_name("end")
    queue = queue_create(name, 4096, 1, 1)
    producer = queue_join(name, PRODUCER, TIMEOUT_MS)
    consumer = queue_join(name, CONSUMER, TIMEOUT_MS)
    producer.send(b"left", TIMEOUT_MS)
    queue.close()
    queue.close()  # a second close does nothing
    raises(Result.DISCONNECTED, producer.send, b"after", TIMEOUT_MS)
    assert consumer.recv(TIMEOUT_MS) == b"left"
    raises(Result.DISCONNECTED, consumer.recv, TIMEOUT_MS)
    with queue_create(name, 4096, 1, 1):  # the name is free again at once
        pass
    producer.close()
    consumer.close()


def test_queue_create_refuses_what_is_out_of_range_and_the_other_kinds_roles_are_refused():
    name = unique_name("range")
    for args in ((1000, 1, 1), (4096, 0, 1), (4096, 65, 1), (4096, 1, 0), (4096, 1, 65)):
        raises(Result.INVALID, queue_create, name, *args)
    with queue_create(name, 4096, 1, 1):
        for role in (SUBSCRIBER_LOSSY, SUBSCRIBER_GATED):  # a broadcast's roles, on a queue and on a join of a queue
            raises(Result.INVALID, broadcast_join, name, role, TIMEOUT_MS)
            raises(Result.INVALID, queue_join, name, role, TIMEOUT_MS)
        raises(Result.INVALID, queue_join, name, 5, NO_WAIT)  # 5 is no role


def test_a_queue_lives_on_when_its_object_is_dropped():
    """The queue has no finalizer: one created in a function that keeps only a joined producer goes on through the
    garbage collector's runs, and holds its name, until its close (here through the native handle, which the test
    kept) or the end of the process."""
    name = unique_name("dropped")
    handles = []

    def start() -> Conn:
        queue = queue_create(name, 4096, 1, 1)
        handles.append(queue._handle)
        return queue_join(name, PRODUCER, TIMEOUT_MS)

    producer = start()
    for _ in range(2):
        gc.collect()
    producer.send(b"still here", TIMEOUT_MS)
    with queue_join(name, CONSUMER, TIMEOUT_MS) as consumer:
        assert consumer.recv(TIMEOUT_MS) == b"still here"
        raises(Result.ADDR_IN_USE, queue_create, name, 4096, 1, 1)
        lib.fipc_queue_close(handles[0])
        raises(Result.DISCONNECTED, consumer.recv, TIMEOUT_MS)
    producer.close()


def test_consumers_racing_for_messages_longer_than_the_receive_buffer_get_each_whole_and_once():
    """recv grows the buffer and receives again after TOO_LARGE, by which time another consumer may have taken that
    message: each receive must return the message it took, whole, at its own length."""
    name = unique_name("race")
    count = 200
    with queue_create(name, 1 << 16, 1, 4) as queue, queue_join(name, PRODUCER, TIMEOUT_MS) as producer:
        consumers = [queue_join(name, CONSUMER, TIMEOUT_MS) for _ in range(4)]
        assert queue.wait_for_members(CONSUMER, 4, TIMEOUT_MS) == 4

        def message(i: int) -> bytes:  # 4 KiB and longer: past the first buffer, and longer as they go
            return i.to_bytes(4, "little") * (1024 + 37 * i % 3000)

        with ThreadPoolExecutor(4) as pool:
            taken = [pool.submit(receive_until_the_end, consumer, 0) for consumer in consumers]
            for i in range(count):
                producer.send(message(i), TIMEOUT_MS)
            queue.close()
            received = list(itertools.chain.from_iterable(future.result(timeout=60) for future in taken))
    for consumer in consumers:
        consumer.close()
    for data in received:
        assert data == message(int.from_bytes(data[:4], "little")), f"a message of {len(data)} bytes isn't whole"
    assert Counter(int.from_bytes(data[:4], "little") for data in received) == Counter(range(count))


def test_the_examples_exchange_in_either_order():
    """The producer creates the queue, joins it, sends job 1 to job 3, waits for a consumer and closes its producer and
    the queue; the consumer receives the three jobs, then DISCONNECTED. Consumer first: its thread calls join before
    the queue is created, which happens 200 ms later (nothing outside the join shows that it is waiting, so the pause
    makes that likely, not certain). Consumer last: it joins after the jobs were sent."""
    for consumer_first in (True, False):
        name = unique_name("example")
        joining = threading.Event()

        def consume() -> None:
            joining.set()
            queue_peer.consume(name, 3)

        with ThreadPoolExecutor(1) as pool:
            if consumer_first:
                consumed = pool.submit(consume)
                assert joining.wait(TIMEOUT_MS / 1000)
                time.sleep(0.2)
            with queue_create(name, 4096, 1, 1) as queue, queue_join(name, PRODUCER) as producer:
                for i in (1, 2, 3):
                    producer.send(f"job {i}".encode())
                if not consumer_first:
                    consumed = pool.submit(consume)
                assert queue.wait_for_members(CONSUMER, 1, FOREVER) == 1
            consumed.result(timeout=TIMEOUT_MS / 1000)


def test_a_producer_in_another_process_and_a_consumer_here():
    name = unique_name("producer_peer")
    peer = subprocess.Popen([sys.executable, str(PEER), "producer", name, "300"])
    try:
        queue_peer.consume(name, 300)
    finally:
        assert peer.wait(timeout=60) == 0, "the producer peer failed"


def test_a_producer_here_and_a_consumer_in_another_process():
    name = unique_name("consumer_peer")
    peer = subprocess.Popen([sys.executable, str(PEER), "consumer", name, "300"])
    try:
        queue_peer.produce(name, 300, 1)
    finally:
        assert peer.wait(timeout=60) == 0, "the consumer peer failed"


def test_two_joined_producers_in_other_processes_and_the_owners_consumer_here():
    name = unique_name("joined_peer")
    peers = [subprocess.Popen([sys.executable, str(PEER), "joined-producer", name, "300"]) for _ in range(2)]
    try:
        queue_peer.own_and_consume(name, 300, 2)
    finally:
        for peer in peers:
            assert peer.wait(timeout=60) == 0, "a producer peer failed"


def test_an_owners_consumer_in_another_process_and_a_joined_producer_here():
    name = unique_name("owner_peer")
    peer = subprocess.Popen([sys.executable, str(PEER), "owner-consumer", name, "300"])
    try:
        queue_peer.join_and_produce(name, 300)
    finally:
        assert peer.wait(timeout=60) == 0, "the consumer peer failed"


def test_a_message_too_large_for_the_buffer_is_received_by_recv_at_any_size():
    name = unique_name("large")
    with queue_create(name, 1 << 16, 1, 1) as queue, queue_join(name, PRODUCER, TIMEOUT_MS) as producer, \
            queue_join(name, CONSUMER, TIMEOUT_MS) as consumer:
        assert queue.wait_for_members(None, 2, NO_WAIT) == 2
        producer.send(bytes(range(256)) * 64, TIMEOUT_MS)  # more than the receive buffer's first 4 KiB
        assert consumer.recv(TIMEOUT_MS) == bytes(range(256)) * 64
