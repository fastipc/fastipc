# Zig examples

Zig programs using FastIPC through its native module, `fastipc`, built with `zig build`. This folder is a
downstream project of its own. The module is the library's source, compiled into each program for its target, so
there is no shared library to ship next to them.

```bash
zig build                       # zig-out/bin: zig_server, zig_client, zig_publisher, zig_subscriber,
                                #   zig_producer, zig_consumer, zig_service_server, zig_service_client
zig-out/bin/zig_server          # in one terminal
zig-out/bin/zig_client          # in another: PING
zig-out/bin/zig_publisher       # in one terminal
zig-out/bin/zig_subscriber      # in another: tick 1, tick 2, tick 3
zig-out/bin/zig_producer        # in one terminal
zig-out/bin/zig_consumer        # in another: job 1, job 2, job 3
zig-out/bin/zig_service_server  # in one terminal
zig-out/bin/zig_service_client  # in another: PING 1, PING 2, PING 3
```

- [`build.zig.zon`](build.zig.zon) depends on this repository by path (`../..`). In a project of your own, `zig fetch
  --save git+https://github.com/fastipc/fastipc` adds it by URL instead.
- [`build.zig`](build.zig) imports the dependency's module `fastipc` into each program, built with the program's target
  and optimize mode.
- [`src`](src): the server and client of the repository's [README](../../README.md#zig), a broadcast's publisher and
  subscriber, a work queue's producer and consumer, and a service's server and client. The server and client work with every other language's server
  or client: the message `ping`, answered with its uppercase, `PING`. The publisher and subscriber make the
  broadcast's exchange on the broadcast `my_broadcast`: the publisher waits for a subscriber, sends `tick 1` to
  `tick 3` and closes, which ends the broadcast. The producer and consumer make the work queue's exchange on the queue
  `my_queue`: the producer creates the queue and joins it as its producer, sends `job 1` to `job 3` (they wait in the
  queue), waits for a consumer, and closes its producer and then the queue, which ends it; the consumer prints the
  three jobs, then sees the end (`error.Disconnected`). The service server and client make the service's exchange on
  the service `my_service`: the server creates the service and joins it as its server, answers three requests with
  their uppercase, and closes its connection and then the service, which ends it; the client sends `ping 1` to
  `ping 3` and prints each reply, `PING 1` to `PING 3`. Each runs on one thread: the owner's process admits the
  joiners by itself.

The repository's slow test tier builds this project (`zig build examples`).
