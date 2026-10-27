# Python examples

The Python server and client of the repository's [README](../../README.md#python), a broadcast's publisher and
subscriber, a work queue's producer and consumer, and a service's server and client, using the binding `fipc`
(`pip install fipc-python`).

```bash
python server.py                # in one terminal
python client.py                # in another: b'PING'
python publisher.py             # in one terminal
python subscriber.py            # in another: tick 1, tick 2, tick 3
python producer.py              # in one terminal
python consumer.py              # in another: job 1, job 2, job 3
python service_server.py        # in one terminal
python service_client.py        # in another: PING 1, PING 2, PING 3
```

The pair works with every other language's server or client: the message `ping`, answered with its uppercase,
`PING`. The publisher and subscriber make the broadcast's exchange on the broadcast `my_broadcast`:
the publisher waits for a subscriber, sends `tick 1` to `tick 3` and closes. The producer and consumer make the work
queue's exchange on the queue `my_queue`: the producer creates the queue, joins it as its producer, sends `job 1` to
`job 3` (they wait in the queue), waits for a consumer, and closes its producer and the queue, which ends it; the
consumer prints the three jobs, then sees the end (`DISCONNECTED`). The service's server and client make the
service's exchange on the service `my_service`: the server creates the service, joins it as its server, answers three
requests with their uppercase and closes its connection and the service, which ends it; the client joins, sends
`ping 1` to `ping 3` (`call`: each request, then its reply) and prints `PING 1` to `PING 3`. In a checkout, put
[`bindings/python`](../../bindings/python) on `PYTHONPATH` instead of installing the package: the binding then loads
the checkout's `zig-out` build. The repository's interop test (`python devtool.py test interop`) runs the server,
client, publisher, subscriber, producer, consumer and the service's server and client against every language's
examples.
