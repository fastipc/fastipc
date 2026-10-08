# C# examples

The C# server and client of the repository's [README](../../README.md#c), each a console project of its own
(`dotnet new console`) using the binding `Fipc`.

```bash
dotnet run --project Server     # in one terminal
dotnet run --project Client     # in another: PING
```

The pair works with every other language's server or client: an RPC request with opcode 1 and the payload `ping`,
answered with `PING`. Here the projects reference the binding's project
([`bindings/csharp`](../../bindings/csharp)), which takes the library from the checkout's build (`python devtool.py
build` copies it); in a project of your own, `dotnet add package Fipc` takes the reference's place. On Linux the
program finds `libfastipc.so` through `LD_LIBRARY_PATH` (the checkout's `zig-out/lib`); on Windows and macOS the build
copies the library next to the program. The repository's interop test
(`python devtool.py test interop`) runs both against every language's examples.
