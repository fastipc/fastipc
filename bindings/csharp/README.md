# Fipc

Messages and RPC between two processes on one machine, through shared memory: the C# binding of
[FastIPC](https://github.com/fastipc/fastipc), a small library written in Zig ([`include/fipc.h`](https://github.com/fastipc/fastipc/blob/main/include/fipc.h)). It has two
layers: `FipcListener` and `FipcConnection`, objects that are safe to dispose from any thread, and underneath them
`Fipc`, the raw P/Invoke declarations over `IntPtr` handles.

## Installation

```bash
dotnet add package Fipc
```

The native libraries for Windows (x64, Windows 11 / Server 2022 or newer), Linux (x64 and arm64: `linux-arm64`, glibc
2.34 or newer) and macOS (Apple Silicon, macOS 14.4 or newer: `osx-arm64`) are included (`runtimes/{rid}/native/`);
the x64 ones need an x86-64-v3 CPU (AVX2: Intel Haswell, AMD Zen or later).

## Use

A server listens on a name and accepts one client at a time; a client connects to the name. `Connect` waits for the
server's `Accept`, so a client and its server run in different processes or on different threads. Timeouts are int
milliseconds, the last parameter: 0 doesn't wait, -1 (`Fipc.Forever`, as `Timeout.Infinite`) waits for ever. Every call
returns a `FipcResult`: a timeout, the peer's end (`Disconnected`), `Cancelled` and `TooLarge` are normal results, never
exceptions. Only a mistake throws, such as a call after `Dispose` (`ObjectDisposedException`).

```csharp
// Server/Program.cs
using System.Text;
using FastIpc;

// rings of 1 MiB each way
FipcListener.Listen("my_channel", 1 << 20,
    out FipcListener? listener);
using (listener)
{
    // waits for a client
    listener!.Accept(out FipcConnection? conn, Fipc.Forever);
    using (conn)
    {
        if (conn!.RpcReceive(out FipcRpcHeader request,
                out byte[] payload, Fipc.Forever)
            == FipcResult.Ok)
        {
            string upper = Encoding.UTF8.GetString(payload)
                .ToUpperInvariant();
            conn.RpcRespond(request.Id, request.Opcode, 0,
                Encoding.UTF8.GetBytes(upper), Fipc.Forever);
        }
    }
}
```

```csharp
// Client/Program.cs
using System.Text;
using FastIpc;

if (FipcConnection.Connect("my_channel",
        out FipcConnection? conn, 5000) != FipcResult.Ok)
    return 1;
using (conn)
{
    conn!.RpcSubmit(opcode: 1, "ping"u8, out ulong id,
        Fipc.Forever);
    conn.RpcReceive(out FipcRpcHeader response,
        out byte[] reply, Fipc.Forever);
    Console.WriteLine(Encoding.UTF8.GetString(reply));  // PING
}
return 0;
```

Each is a console project (`dotnet new console`) with the `Fipc` package; run the server, then the client in
another terminal. The client sends an RPC request with opcode 1 and the payload `ping`, and the server answers with the
payload in upper case. Every example of the [repository's README](https://github.com/fastipc/fastipc#examples)
makes this exchange, so its C, Python, Java, Rust, Lua, JavaScript and Go clients work against this server too, and this client against
their servers.

- **Plain messages:** `Send` and `Receive` (into a span; `TooLarge` reports the length it needs and takes nothing, or
  `Receive(out byte[] message, ...)` for a message of any size). A message of any size may be sent; it travels in
  pieces and arrives whole.
- **Zero-copy** (messages of one piece, up to `MaxPiece` bytes): `SendAcquire(length, out Span<byte> buffer, ...)`,
  write, `SendCommit(length)`; `ReceiveAcquire(out ReadOnlySpan<byte> message, ...)`, read, `ReceiveRelease()`.
- **RPC:** `RpcSubmit`, `RpcRespond`, and `RpcReceive` into a span or a new array; `FipcRpcHeader` has the id, the
  kind (`FipcRpcKind.Request` or `Response`), the opcode, the status and the payload's length.

Threads: on a connection one thread at a time sends and one receives; `Cancel` and `Dispose` may be called from any
thread, at any time. `Dispose` cancels the handle, so a call that waits returns `Cancelled`, and the native close runs
once the calls in progress have returned: no call ever runs on a closed connection, and a zero-copy span stays valid
until its commit or release. So stopping an IO thread is: `Dispose`, then join the thread (with a timeout). A
`Disconnected` connection is final: dispose it, and accept or connect a new one.

## The raw layer

`Fipc` declares the C API one to one: `IntPtr` handles, pointers or spans. It costs nothing beyond the native call,
where each object-layer call holds its SafeHandle (two atomic operations, about 15 ns per call; see
[`docs/perf/bindings-baseline.md`](https://github.com/fastipc/fastipc/blob/main/docs/perf/bindings-baseline.md)), so it is the escape hatch for the hottest loops. It enforces nothing either: `Close`
needs the handle to itself (cancel, join the threads that use it, then close), and closing under a running call frees
memory the call still uses.

```csharp
Fipc.Connect("my_channel", out IntPtr conn, 5000);
Fipc.Send(conn, "hello"u8, Fipc.Forever);
Fipc.Close(conn);
```

## Documentation

- [The website](https://fastipc.github.io/fastipc/csharp.html): this binding's guide, with examples of every call.
- [`include/fipc.h`](https://github.com/fastipc/fastipc/blob/main/include/fipc.h): the C API under this binding, and every call's contract (results,
  timeouts, threads, messages in pieces, the peer's end).
- [Platform support](https://github.com/fastipc/fastipc/blob/main/docs/platform-support.md) and the [protocol](https://github.com/fastipc/fastipc/blob/main/docs/protocol.md).
- [Changelog](https://github.com/fastipc/fastipc/blob/main/CHANGELOG.md), [issues](https://github.com/fastipc/fastipc/issues).

## License

MIT. Copyright (c) 2025-2026 Hayden Donnelly. See the LICENSE file in the package.
