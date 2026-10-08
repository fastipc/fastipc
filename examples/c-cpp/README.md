# C and C++ examples

C and C++ programs using FastIPC, built with `zig build` (no CMake). This folder is a downstream project of its
own: the Zig build system and nothing else, no other build tool. Zig 0.17.0 builds the library for the target and
compiles the C and C++ with its bundled Clang.

```bash
zig build                       # zig-out/bin: c_server, c_client, cpp_server, cpp_client (and fastipc.dll on Windows)
zig-out/bin/cpp_server          # in one terminal
zig-out/bin/c_client            # in another: PING
```

- [`build.zig.zon`](build.zig.zon) depends on this repository by path (`../..`). In a project of your own, `zig fetch
  --save git+https://github.com/fastipc/fastipc` adds it by URL instead.
- [`build.zig`](build.zig) takes the dependency's artifact `fastipc`, the shared library: linking it puts `fipc.h` and
  `fipc.hpp` on the include path. Installing it puts `fastipc.dll` next to the programs (Windows) or `libfastipc.so` /
  `libfastipc.dylib` in `zig-out/lib`, which the programs find through their rpath `$ORIGIN/../lib` (Linux) or
  `@loader_path/../lib` (macOS).
- [`src`](src): the servers and clients of the repository's [README](../../README.md#examples), in C (`fipc.h`) and
  C++20 (`fipc.hpp`). Every pair works with every other language's server or client: an RPC request with opcode 1 and
  the payload `ping`, answered with `PING`.

The repository's slow test tier builds this project (`zig build examples`).
