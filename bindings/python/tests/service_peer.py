"""The Python side of a service across processes, for this binding's tests and the other bindings':

    python service_peer.py server <name> [count] [clients]
    python service_peer.py client <name> [count]
    python service_peer.py owner-client <name> [count] [servers]
    python service_peer.py joined-server <name>
    python service_peer.py holding-server <name>

It loads the binding of this checkout (bindings/python, which loads zig-out's library). A client sends `count` (100 by
default) requests, ping 1 to ping <count> (the UTF-8 texts `ping <i>`), one at a time (`call`), and checks that each
reply is its request's uppercase, PING <i>. The owner's service has rings of 4096 bytes, requests and replies of up to
256 bytes, and room for 8 clients and 8 servers.

- server: creates the service on `name` and joins it as a server, as the examples' server does, answers count
  requests from each of `clients` (1 by default) clients with their uppercase, and closes its server and the service,
  which ends it: its clients receive the replies sent, then FIPC_DISCONNECTED.
- client: joins `name` as a client, calls count times and checks each reply, then closes.
- owner-client: creates the service on `name` and joins it as a client, waits for `servers` (1 by default) servers to
  have joined, calls count times, checks each reply, and closes its client and the service, which ends it.
- joined-server: joins `name` as a server and answers requests until FIPC_DISCONNECTED (the service's end).
- holding-server: joins `name` as a server, takes one request, prints `holding` and waits, never replying, until it is
  killed: its client's receive is FIPC_ABANDONED once the process has ended.

Each step waits at most 20 s. Exit code 0 when everything checked out; a failure raises (exit code 1).
"""

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # bindings/python
import fipc  # noqa: E402

TIMEOUT_MS = 20000
CAPACITY = 4096
MAX_MESSAGE = 256
MEMBERS = 8


def request(i: int) -> bytes:
    return f"ping {i}".encode()


def answer(server: fipc.Conn, count: int) -> None:
    for _ in range(count):
        server.send(server.recv(timeout_ms=TIMEOUT_MS).upper())


def answer_until_the_end(server: fipc.Conn) -> int:
    """Answers requests until the service ends, and returns how many it answered."""
    answered = 0
    while True:
        try:
            message = server.recv(timeout_ms=TIMEOUT_MS)
        except fipc.FipcError as e:
            if e.result != fipc.Result.DISCONNECTED:
                raise
            return answered
        try:
            server.send(message.upper())
        except fipc.FipcError as e:  # the service ended between the request and its reply
            if e.result != fipc.Result.DISCONNECTED:
                raise
            return answered
        answered += 1


def call(client: fipc.Conn, count: int) -> None:
    for i in range(1, count + 1):
        reply = client.call(request(i), timeout_ms=TIMEOUT_MS)
        assert reply == request(i).upper(), f"the reply to {request(i)!r} is {reply!r}"


def serve(name: str, count: int, clients: int) -> None:
    with fipc.service_create(name, CAPACITY, MAX_MESSAGE, MEMBERS, MEMBERS):
        with fipc.service_join(name, fipc.SERVER, timeout_ms=TIMEOUT_MS) as server:
            answer(server, count * clients)


def join_and_call(name: str, count: int) -> None:
    with fipc.service_join(name, fipc.CLIENT, timeout_ms=TIMEOUT_MS) as client:
        call(client, count)


def own_and_call(name: str, count: int, servers: int) -> None:
    with fipc.service_create(name, CAPACITY, MAX_MESSAGE, MEMBERS, MEMBERS) as service:
        with fipc.service_join(name, fipc.CLIENT, timeout_ms=TIMEOUT_MS) as client:
            assert service.wait_for_members(fipc.SERVER, servers, timeout_ms=TIMEOUT_MS) >= servers
            call(client, count)


def join_and_answer(name: str) -> None:
    with fipc.service_join(name, fipc.SERVER, timeout_ms=TIMEOUT_MS) as server:
        answer_until_the_end(server)


def join_and_hold(name: str) -> None:
    server = fipc.service_join(name, fipc.SERVER, timeout_ms=TIMEOUT_MS)
    server.recv(timeout_ms=TIMEOUT_MS)
    print("holding", flush=True)
    time.sleep(60)
    sys.exit("holding-server: not killed within 60 s")


if __name__ == "__main__":
    mode, name = sys.argv[1], sys.argv[2]
    count = int(sys.argv[3]) if len(sys.argv) > 3 else 100
    members = int(sys.argv[4]) if len(sys.argv) > 4 else 1
    if mode == "server":
        serve(name, count, members)
    elif mode == "client":
        join_and_call(name, count)
    elif mode == "owner-client":
        own_and_call(name, count, members)
    elif mode == "joined-server":
        join_and_answer(name)
    elif mode == "holding-server":
        join_and_hold(name)
    else:
        sys.exit(f"unknown mode {mode}: server, client, owner-client, joined-server or holding-server")
