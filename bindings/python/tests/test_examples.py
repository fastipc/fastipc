"""The examples (examples/python) in pairs, each program a process of its own, in both start orders: the server and
the client, the publisher and the subscriber, the producer and the consumer, the service's server and client. They use
the binding of this checkout (bindings/python on PYTHONPATH, which loads zig-out's library), and their fixed names
(`my_channel`, `my_broadcast`, `my_queue`, `my_service`), so no two of these tests run at once."""

import os
import subprocess
import sys
import time
from pathlib import Path
from typing import List

import pytest

EXAMPLES = Path(__file__).resolve().parents[3] / "examples" / "python"
BINDING = Path(__file__).resolve().parents[1]
# Each pair: its programs, the lines the second prints, and whether in any order (the queue promises none)
PAIRS = [("server.py", "client.py", ["b'PING'"], False),
         ("publisher.py", "subscriber.py", ["tick 1", "tick 2", "tick 3"], False),
         ("producer.py", "consumer.py", ["job 1", "job 2", "job 3"], True),
         ("service_server.py", "service_client.py", ["PING 1", "PING 2", "PING 3"], False)]


def start(example: str) -> subprocess.Popen:
    path = [str(BINDING)] + ([os.environ["PYTHONPATH"]] if os.environ.get("PYTHONPATH") else [])
    environment = {**os.environ, "PYTHONPATH": os.pathsep.join(path)}
    return subprocess.Popen([sys.executable, str(EXAMPLES / example)], env=environment, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, text=True)


@pytest.mark.parametrize("server, client, printed, any_order", PAIRS)
@pytest.mark.parametrize("client_first", [False, True])
def test_the_pair_in_two_processes(server: str, client: str, printed: List[str], any_order: bool,
                                   client_first: bool) -> None:
    """The second program starts half a second after the first: a client first waits in its join or connect (up to
    its 5 s timeout) for the server, publisher, producer or service."""
    first, second = (client, server) if client_first else (server, client)
    processes = {first: start(first)}
    time.sleep(0.5)
    processes[second] = start(second)
    outputs = {}
    try:
        for example, process in processes.items():
            outputs[example] = process.communicate(timeout=30)
    finally:
        for process in processes.values():
            process.kill()
    for example, process in processes.items():
        assert process.returncode == 0, f"{example} failed: {outputs[example][1]}"
    lines = outputs[client][0].splitlines()
    assert (sorted(lines) if any_order else lines) == printed
