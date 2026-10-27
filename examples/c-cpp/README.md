# C and C++ examples

C and C++ programs using FastIPC, built by `zig build` alone (no CMake): this folder is a downstream project of its
own. Zig 0.17.0 builds the library for the target and compiles the C and C++ with its bundled Clang.

```bash
zig build                       # zig-out/bin: c_server, c_client, cpp_server, cpp_client, c_publisher,
                                #   c_subscriber, cpp_publisher, cpp_subscriber, c_producer, c_consumer,
                                #   cpp_producer, cpp_consumer, c_service_server, c_service_client,
                                #   cpp_service_server, cpp_service_client (and fastipc.dll on Windows)
zig-out/bin/cpp_server          # in one terminal
zig-out/bin/c_client            # in another: PING
zig-out/bin/c_publisher         # in one terminal
zig-out/bin/cpp_subscriber      # in another: tick 1, tick 2, tick 3
zig-out/bin/c_producer          # in one terminal
zig-out/bin/cpp_consumer        # in another: job 1, job 2, job 3
zig-out/bin/cpp_service_server  # in one terminal
zig-out/bin/c_service_client    # in another: PING 1, PING 2, PING 3
```

- [`build.zig.zon`](build.zig.zon) depends on this repository by path (`../..`). In a project of your own, `zig fetch
  --save git+https://github.com/fastipc/fastipc` adds it by URL instead.
- [`build.zig`](build.zig) takes the dependency's artifact `fastipc`, the shared library: linking it puts `fipc.h` and
  `fipc.hpp` on the include path. Installing it puts `fastipc.dll` next to the programs (Windows) or `libfastipc.so` /
  `libfastipc.dylib` in `zig-out/lib`, which the programs find through their rpath `$ORIGIN/../lib` (Linux) or
  `@loader_path/../lib` (macOS).
- [`src`](src): the servers and clients of the repository's [README](../../README.md#examples), in C (`fipc.h`) and
  C++20 (`fipc.hpp`). Every pair works with every other language's server or client: the message `ping`, answered with
  its uppercase, `PING`. The publishers and subscribers make the broadcast's exchange on the broadcast `my_broadcast`:
  the publisher waits for a subscriber, sends `tick 1` to `tick 3` and closes, which ends the broadcast. The producers
  and consumers make the work queue's exchange on the queue `my_queue`: the producer creates the queue and joins it as
  its producer, sends `job 1` to `job 3` (they wait in the queue), waits for a consumer, and closes its producer and
  then the queue, which ends it; the consumer prints the three jobs, then sees the end (`FIPC_DISCONNECTED`). The service
  servers and clients make the service's exchange on the service `my_service`: the server creates the service and
  joins it as its server, answers three requests with their uppercase, and closes its connection and then the
  service, which ends it; the client sends `ping 1` to `ping 3` and prints each reply, `PING 1` to `PING 3`. Each runs
  on one thread: the owner's process admits the joiners by itself.

The repository's slow test tier builds this project (`zig build examples`).
