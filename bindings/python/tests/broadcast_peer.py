"""The Python side of a broadcast across processes, for this binding's tests and the other bindings':

    python broadcast_peer.py publisher <name> [count] [subscribers]
    python broadcast_peer.py subscriber <name> [gated|lossy] [count]

It loads the binding of this checkout (bindings/python, which loads zig-out's library). Message i, from 0 to count - 1
(100 by default), is the UTF-8 text `message <i>`.

- publisher: creates a broadcast on `name` of 4096 bytes (64 messages of these lengths before a gated subscriber holds
  the publisher back) and up to 8 subscribers, waits for `subscribers` (1 by default) to join, sends the count messages
  and closes, which ends the broadcast.
- subscriber: joins `name` as a gated (the default) or lossy subscriber and receives until FIPC_DISCONNECTED. A gated
  one checks that it received every message in order. A lossy one checks that each message it received is the one
  after the last, or that many after it as an FIPC_OVERRUN between them reported lost, and that received and lost
  add up to the count. Its publisher must wait for it to join before sending (the publisher's wait for members).

Each step waits at most 20 s. Exit code 0 when everything checked out; a failure raises (exit code 1).
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # bindings/python
import fipc  # noqa: E402

TIMEOUT_MS = 20000


def message(i: int) -> bytes:
    return f"message {i}".encode()


def publish(name: str, count: int, subscribers: int) -> None:
    with fipc.broadcast_create(name, 4096, 8, TIMEOUT_MS) as publisher:
        assert publisher.wait_for_members(subscribers, timeout_ms=TIMEOUT_MS) >= subscribers
        for i in range(count):
            publisher.send(message(i), timeout_ms=TIMEOUT_MS)


def subscribe(name: str, gated: bool, count: int) -> None:
    role = "gated" if gated else "lossy"
    received = []  # each message's index
    lost = 0
    expected = 0  # the index of the next message
    with fipc.broadcast_join(name, fipc.SUBSCRIBER_GATED if gated else fipc.SUBSCRIBER_LOSSY,
                             timeout_ms=TIMEOUT_MS) as subscriber:
        while True:
            try:
                data = subscriber.recv(timeout_ms=TIMEOUT_MS)
            except fipc.FipcError as e:
                if e.result == fipc.Result.DISCONNECTED:
                    break
                if e.result != fipc.Result.OVERRUN or gated:
                    raise
                assert e.lost >= 1, e
                lost += e.lost
                expected += e.lost
                continue
            assert data == message(expected), f"{role}: received {data!r}, expected {message(expected)!r}"
            received.append(expected)
            expected += 1
    assert expected == count, f"{role}: received {len(received)} and lost {lost} of {count} messages"
    assert not gated or received == list(range(count)), "a gated subscriber lost messages"


if __name__ == "__main__":
    mode, name = sys.argv[1], sys.argv[2]
    if mode == "publisher":
        publish(name, int(sys.argv[3]) if len(sys.argv) > 3 else 100, int(sys.argv[4]) if len(sys.argv) > 4 else 1)
    elif mode == "subscriber":
        role = sys.argv[3] if len(sys.argv) > 3 else "gated"
        assert role in ("gated", "lossy"), f"unknown role {role}"
        subscribe(name, role == "gated", int(sys.argv[4]) if len(sys.argv) > 4 else 100)
    else:
        sys.exit(f"unknown mode {mode}: publisher or subscriber")
