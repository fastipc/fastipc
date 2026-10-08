# Zig examples

Zig programs using FastIPC through its native module, `fastipc`, built with `zig build`. This folder is a
downstream project of its own. The module is the library's source, compiled into each program for its target, so
there is no shared library to ship next to them.

```bash
zig build                       # zig-out/bin: zig_server, zig_client
zig-out/bin/zig_server          # in one terminal
zig-out/bin/zig_client          # in another: PING
```

- [`build.zig.zon`](build.zig.zon) depends on this repository by path (`../..`). In a project of your own, `zig fetch
  --save git+https://github.com/fastipc/fastipc` adds it by URL instead.
- [`build.zig`](build.zig) imports the dependency's module `fastipc` into each program, built with the program's target
  and optimize mode.
- [`src`](src): the server and client of the repository's [README](../../README.md#zig). The pair works with every
  other language's server or client: an RPC request with opcode 1 and the payload `ping`, answered with `PING`.

The repository's slow test tier builds this project (`zig build examples`).
