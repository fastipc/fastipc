# C# examples

The C# server and client of the repository's [README](../../README.md#c), a broadcast's publisher and
subscriber, a work queue's producer and consumer, and a service's server and client, each a console project of its own
(`dotnet new console`) using the binding `Fipc`.

```bash
dotnet run --project Server     # in one terminal
dotnet run --project Client     # in another: PING
dotnet run --project Publisher  # in one terminal
dotnet run --project Subscriber # in another: tick 1, tick 2, tick 3
dotnet run --project Producer   # in one terminal
dotnet run --project Consumer   # in another: job 1, job 2, job 3
dotnet run --project ServiceServer  # in one terminal
dotnet run --project ServiceClient  # in another: PING 1, PING 2, PING 3
```

The pair works with every other language's server or client: the message `ping`, answered with its uppercase,
`PING`. The publisher and subscriber make the broadcast's exchange on the broadcast `my_broadcast`: the
publisher waits for a subscriber, sends `tick 1` to `tick 3` and closes. The producer and consumer make the work
queue's exchange on the queue `my_queue`: the producer creates the queue, joins it as its producer, sends `job 1` to
`job 3` (they wait in the queue), waits for a consumer, and disposes its producer and the queue, which ends it; the
consumer prints the three jobs, then sees the end (`Disconnected`). The service's server and client make the
service's exchange on the service `my_service`: the server creates the service, joins it as its server, answers three
requests with their uppercase and disposes its connection and the service, which ends it; the client joins, sends
`ping 1` to `ping 3` (`Call`: each request, then its reply) and prints `PING 1` to `PING 3`. Here the projects reference the binding's
project ([`bindings/csharp`](../../bindings/csharp)), which takes the library from the checkout's build (`python
devtool.py build` copies it); in a project of your own, `dotnet add package Fipc` takes the reference's place. On Linux
the program finds `libfastipc.so` through `LD_LIBRARY_PATH` (the checkout's `zig-out/lib`); on Windows and macOS the
build copies the library next to the program. The repository's interop test (`python devtool.py test interop`) runs the
server, client, publisher, subscriber, producer, consumer and the service's server and client against every language's
examples.
