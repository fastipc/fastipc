using System;
using System.Diagnostics;
using System.Text;
using FastIpc;

namespace FastIpc.Bench
{
    /// <summary>
    /// FastIPC's C# benchmark, through the binding (FastIpc.Fipc): one-way throughput between this process, a
    /// server that receives and times, and a client subprocess (this program again) that sends.
    ///
    ///   copy      Send / Recv into the server's buffer
    ///   zerocopy  SendAcquire + SendCommit / RecvAcquire + RecvRelease (a message of several pieces through the
    ///             copying calls); the server copies each message out, as a consumer would
    ///   rpc       RpcSubmit / RpcRecv
    ///
    /// Each case first sends messages untimed for half a second (the warm-up, for the JIT; at least its count), then a
    /// start marker, then its count, and the server times from the last start marker to the end marker (see
    /// <see cref="Client"/>). Each case prints "Test i/n: name" and "Throughput: n messages/sec" (devtool bench-compare reads them).
    /// The mode "layers" compares the binding's raw layer with its object layer instead (<see cref="Layers"/>).
    /// </summary>
    public static unsafe class Program
    {
        private const int ConnectMs = 15_000;
        private const uint DataOpcode = 1;
        private const uint EndOpcode = 0xFB000001;
        private const uint GoOpcode = 0xFB000002;
        private const long WarmUpTicks = 500 * TimeSpan.TicksPerMillisecond;
        private const int MarkerEvery = 1000;
        private const long AtEnd = -1;
        private const long AtGo = -2;
        private static readonly byte[] End = Encoding.ASCII.GetBytes("END");
        private static readonly byte[] Go = Encoding.ASCII.GetBytes("GO!");

        private static readonly (int Count, int Ring, int Size, string Name)[] Cases =
        {
            (2_000_000, 512 * 1024, 16, "Tiny messages (16B)"),
            (2_000_000, 512 * 1024, 64, "Small messages (64B)"),
            (1_000_000, 512 * 1024, 256, "Medium messages (256B)"),
            (1000, 512 * 1024, 64 * 1024, "Large messages (64KB)"),
            (1000, 2 * 1024 * 1024, 512 * 1024, "Large messages (512KB)"),
            (10, 512 * 1024, 1024 * 1024, "Exceeds buffer (1MB msg, 512KB buffer)"),
        };

        public static int Main(string[] args)
        {
            string mode = args.Length > 0 ? args[0] : "copy";
            if (mode == "client")
                return Client(args[1], args[2], int.Parse(args[3]), int.Parse(args[4]), int.Parse(args[5]));
            if (mode == "layers")
                return Layers.Run();
            if (mode != "copy" && mode != "zerocopy" && mode != "rpc")
            {
                Console.Error.WriteLine("usage: FastIpc.Bench copy|zerocopy|rpc|layers");
                return 2;
            }

            Console.WriteLine($"FastIPC C# benchmark: {mode}\n");
            int passed = 0;
            for (int i = 0; i < Cases.Length; i++)
            {
                var c = Cases[i];
                Console.WriteLine($"Test {i + 1}/{Cases.Length}: {c.Name}");
                if (RunCase(mode, c.Count, c.Ring, c.Size))
                    passed++;
                else
                    Console.WriteLine("FAILED");
                Console.WriteLine();
            }
            Console.WriteLine($"Summary: {passed}/{Cases.Length} tests passed");
            return passed == Cases.Length ? 0 : 1;
        }

        private static bool RunCase(string mode, int count, int ring, int size)
        {
            string name = $"csbench_{Environment.ProcessId}_{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}";
            FipcResult listened = Fipc.Listen(name, (nuint)ring, out IntPtr listener);
            if (listened != FipcResult.Ok)
            {
                Console.Error.WriteLine($"listen: {Fipc.ResultStr(listened)}");
                return false;
            }

            string self = System.Reflection.Assembly.GetEntryAssembly()!.Location;
            var start = new ProcessStartInfo("dotnet", $"\"{self}\" client {mode} {name} {ring} {size} {count}")
            {
                UseShellExecute = false,
                RedirectStandardError = true,
            };
            using var client = Process.Start(start)!;
            var clientErrors = new StringBuilder();
            client.ErrorDataReceived += (_, e) => { if (e.Data != null) clientErrors.AppendLine(e.Data); };
            client.BeginErrorReadLine();

            FipcResult accepted = Fipc.Accept(listener, out IntPtr conn, ConnectMs);
            Fipc.ListenerClose(listener);
            if (accepted != FipcResult.Ok)
            {
                Console.Error.WriteLine($"accept: {Fipc.ResultStr(accepted)}");
                client.Kill();
                return false;
            }

            (long messages, long bytes, double seconds, FipcResult result) = Serve(mode, conn, size);
            Fipc.Close(conn);
            if (!client.WaitForExit(180_000))
            {
                client.Kill();
                Console.Error.WriteLine("the client timed out");
                return false;
            }
            if (result != FipcResult.Ok || client.ExitCode != 0)
            {
                Console.Error.WriteLine($"server: {Fipc.ResultStr(result)}; client: exit {client.ExitCode} {clientErrors}");
                return false;
            }
            if (messages != count || bytes != (long)count * size)
            {
                Console.Error.WriteLine($"expected {count} messages of {size} B, got {messages} ({bytes} B)");
                return false;
            }

            Console.WriteLine($"Messages: {count} | Ring: {ring / 1024}KB | Size: {size}B");
            Console.WriteLine($"Duration: {seconds:F3}s");
            Console.WriteLine($"Throughput: {messages / seconds:F0} messages/sec, {bytes / seconds / (1 << 20):F1} MB/sec");
            return true;
        }

        /// <summary>
        /// The server: skips the warm-up up to the last start marker, then receives until the end marker; the messages,
        /// their bytes, the seconds and the result.
        /// </summary>
        private static (long, long, double, FipcResult) Serve(string mode, IntPtr conn, int size)
        {
            var buf = new byte[Math.Max(size, End.Length)];
            var sink = new byte[size];
            bool timing = false;
            long messages = 0, bytes = 0, started = 0;
            fixed (byte* b = buf)
            fixed (byte* s = sink)
            {
                while (true)
                {
                    long len = ReceiveOne(mode, conn, b, buf.Length, s, sink.Length, out FipcResult r);
                    if (r != FipcResult.Ok) return (messages, bytes, 0, r);
                    if (len == AtEnd) break;
                    if (len == AtGo) // the last one starts the timed messages
                    {
                        timing = true;
                        messages = 0;
                        bytes = 0;
                        started = Stopwatch.GetTimestamp();
                    }
                    else if (timing)
                    {
                        messages++;
                        bytes += len;
                    }
                }
            }
            return (messages, bytes, Stopwatch.GetElapsedTime(started).TotalSeconds, FipcResult.Ok);
        }

        /// <summary>One message received, as a consumer would: its length, or AtGo or AtEnd for the markers.</summary>
        private static long ReceiveOne(string mode, IntPtr conn, byte* b, int bufLen, byte* s, int sinkLen, out FipcResult r)
        {
            nuint len;
            if (mode == "rpc")
            {
                r = Fipc.RpcRecv(conn, b, (nuint)bufLen, out FipcRpcMsg msg, Fipc.Forever);
                if (r != FipcResult.Ok) return 0;
                return msg.Opcode == EndOpcode ? AtEnd : msg.Opcode == GoOpcode ? AtGo : (long)msg.Len;
            }
            if (mode == "zerocopy")
            {
                r = Fipc.RecvAcquire(conn, out byte* data, out len, Fipc.Forever);
                if (r == FipcResult.TooLarge)
                {
                    r = Fipc.Recv(conn, b, (nuint)bufLen, out len, Fipc.Forever); // several pieces
                    return (long)len;
                }
                if (r != FipcResult.Ok) return 0;
                long marker = Marker(data, len);
                if (marker == 0)
                    Buffer.MemoryCopy(data, s, sinkLen, (long)len);
                Fipc.RecvRelease(conn);
                return marker == 0 ? (long)len : marker;
            }
            r = Fipc.Recv(conn, b, (nuint)bufLen, out len, Fipc.Forever);
            if (r != FipcResult.Ok) return 0;
            long m = Marker(b, len);
            return m == 0 ? (long)len : m;
        }

        /// <summary>AtEnd or AtGo for a marker, else 0.</summary>
        private static long Marker(byte* data, nuint len)
        {
            if (len != (nuint)End.Length) return 0;
            var bytes = new ReadOnlySpan<byte>(data, End.Length);
            return bytes.SequenceEqual(End) ? AtEnd : bytes.SequenceEqual(Go) ? AtGo : 0;
        }

        /// <summary>
        /// The client subprocess: messages untimed for the warm-up (the count, and for at least half a second: the JIT
        /// compiles the hot path after it has run a while, which takes longer than the shorter cases' count), the start
        /// marker, the count, the end marker; then it closes. The warm-up sends a start marker every MarkerEvery
        /// messages too, so that the real one takes no path the JIT hasn't seen; the markers go through the same calls
        /// as the messages.
        /// </summary>
        private static int Client(string mode, string name, int ring, int size, int count)
        {
            FipcResult r = Fipc.Connect(name, out IntPtr conn, ConnectMs);
            if (r != FipcResult.Ok)
            {
                Console.Error.WriteLine($"connect: {Fipc.ResultStr(r)}");
                return 1;
            }
            var payload = new byte[size];
            payload.AsSpan().Fill((byte)'x');
            bool onePiece = (ulong)size <= (ulong)Fipc.MaxPiece(conn);
            fixed (byte* p = payload)
            fixed (byte* go = Go)
            fixed (byte* end = End)
            {
                long warm = Stopwatch.GetTimestamp() + WarmUpTicks * Stopwatch.Frequency / TimeSpan.TicksPerSecond;
                for (int i = 0; r == FipcResult.Ok && (i < count || Stopwatch.GetTimestamp() < warm); i++)
                {
                    if (i % MarkerEvery == 0)
                        r = SendMarker(mode, conn, go, GoOpcode);
                    if (r == FipcResult.Ok)
                        r = SendOne(mode, conn, p, size, onePiece);
                }
                if (r == FipcResult.Ok)
                    r = SendMarker(mode, conn, go, GoOpcode);
                for (int i = 0; i < count && r == FipcResult.Ok; i++)
                    r = SendOne(mode, conn, p, size, onePiece);
                if (r == FipcResult.Ok)
                    r = SendMarker(mode, conn, end, EndOpcode);
            }
            Fipc.Close(conn); // the server still receives everything sent before the close
            if (r != FipcResult.Ok)
            {
                Console.Error.WriteLine($"send: {Fipc.ResultStr(r)}");
                return 1;
            }
            return 0;
        }

        private static FipcResult SendMarker(string mode, IntPtr conn, byte* marker, uint opcode) =>
            mode == "rpc" ? Fipc.RpcSubmit(conn, opcode, marker, 0, out _, Fipc.Forever)
                          : Fipc.Send(conn, marker, (nuint)End.Length, Fipc.Forever);

        /// <summary>One message sent (a method of its own, for the JIT, as ReceiveOne).</summary>
        private static FipcResult SendOne(string mode, IntPtr conn, byte* p, int size, bool onePiece)
        {
            if (mode == "rpc")
                return Fipc.RpcSubmit(conn, DataOpcode, p, (nuint)size, out _, Fipc.Forever);
            if (mode == "zerocopy" && onePiece)
            {
                FipcResult r = Fipc.SendAcquire(conn, (nuint)size, out byte* room, Fipc.Forever);
                if (r != FipcResult.Ok) return r;
                Buffer.MemoryCopy(p, room, size, size);
                return Fipc.SendCommit(conn, (nuint)size);
            }
            return Fipc.Send(conn, p, (nuint)size, Fipc.Forever);
        }
    }
}
