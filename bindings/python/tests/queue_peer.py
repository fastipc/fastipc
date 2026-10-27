"""The Python side of a work queue across processes, for this binding's tests and the other bindings':

    python queue_peer.py producer <name> [count] [consumers]
    python queue_peer.py consumer <name> [count]
    python queue_peer.py owner-consumer <name> [count] [producers]
    python queue_peer.py joined-producer <name> [count]

It loads the binding of this checkout (bindings/python, which loads zig-out's library). A producer sends `count` (100
by default) messages, job 1 to job <count>: the UTF-8 texts `job <i>`. The owner's queue has rings of 4096 bytes (64
messages of these lengths before a producer waits for room) and room for 8 producers and 8 consumers.

- producer: creates the queue on `name` and joins it as a producer, as the examples' producer does, sends the jobs
  (more than its ring holds wait for a consumer to take some), waits for `consumers` (1 by default) consumers to have
  joined, and closes its producer and the queue, which ends it: its consumers receive what is left, then
  FIPC_DISCONNECTED.
- consumer: joins `name` as a consumer and receives until FIPC_DISCONNECTED; it checks that it received job 1 to job
  <count>, each once (one producer, and no other consumer; the queue promises no order).
- owner-consumer: creates the queue on `name` and joins it as a consumer, receives count jobs from each of
  `producers` (1 by default) producers, checks that each job of each producer came once, and closes its consumer and
  the queue, which ends it.
- joined-producer: joins `name` as a producer, sends the jobs and closes; the queue goes on until its owner ends it.

Each step waits at most 20 s. Exit code 0 when everything checked out; a failure raises (exit code 1).
"""

import sys
from collections import Counter
from pathlib import Path
from typing import List

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # bindings/python
import fipc  # noqa: E402

TIMEOUT_MS = 20000
CAPACITY = 4096
MEMBERS = 8


def job(i: int) -> bytes:
    return f"job {i}".encode()


def send_jobs(producer: fipc.Conn, count: int) -> None:
    for i in range(1, count + 1):
        producer.send(job(i), timeout_ms=TIMEOUT_MS)


def check_jobs(received: List[bytes], producers: int, count: int) -> None:
    """That `received` is the jobs of `producers` producers, each sending job 1 to job <count>, in any order: each job
    `producers` times."""
    expected = Counter({job(i): producers for i in range(1, count + 1)})
    assert Counter(received) == expected, f"received {len(received)} of {producers * count} jobs, or a job twice"


def produce(name: str, count: int, consumers: int) -> None:
    with fipc.queue_create(name, CAPACITY, MEMBERS, MEMBERS) as queue:
        with fipc.queue_join(name, fipc.PRODUCER, timeout_ms=TIMEOUT_MS) as producer:
            send_jobs(producer, count)
            assert queue.wait_for_members(fipc.CONSUMER, consumers, timeout_ms=TIMEOUT_MS) >= consumers


def consume(name: str, count: int) -> None:
    received = []
    with fipc.queue_join(name, fipc.CONSUMER, timeout_ms=TIMEOUT_MS) as consumer:
        while True:
            try:
                received.append(consumer.recv(timeout_ms=TIMEOUT_MS))
            except fipc.FipcError as e:
                if e.result != fipc.Result.DISCONNECTED:
                    raise
                break
    check_jobs(received, 1, count)


def own_and_consume(name: str, count: int, producers: int) -> None:
    with fipc.queue_create(name, CAPACITY, MEMBERS, MEMBERS):
        with fipc.queue_join(name, fipc.CONSUMER, timeout_ms=TIMEOUT_MS) as consumer:
            received = [consumer.recv(timeout_ms=TIMEOUT_MS) for _ in range(producers * count)]
    check_jobs(received, producers, count)


def join_and_produce(name: str, count: int) -> None:
    with fipc.queue_join(name, fipc.PRODUCER, timeout_ms=TIMEOUT_MS) as producer:
        send_jobs(producer, count)


if __name__ == "__main__":
    mode, name = sys.argv[1], sys.argv[2]
    count = int(sys.argv[3]) if len(sys.argv) > 3 else 100
    members = int(sys.argv[4]) if len(sys.argv) > 4 else 1
    if mode == "producer":
        produce(name, count, members)
    elif mode == "consumer":
        consume(name, count)
    elif mode == "owner-consumer":
        own_and_consume(name, count, members)
    elif mode == "joined-producer":
        join_and_produce(name, count)
    else:
        sys.exit(f"unknown mode {mode}: producer, consumer, owner-consumer or joined-producer")
