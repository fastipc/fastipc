using System;
using System.Diagnostics;
using FastIpc;

namespace FastIpc.Bench
{
    /// <summary>
    /// The binding's two layers side by side, in one process and one thread: the raw layer (<see cref="Fipc"/>, IntPtr
    /// handles) and the object layer (<see cref="FipcConnection"/>, a SafeHandle that each call holds). Each round
    /// sends a batch of messages that fits the ring and then receives it, so no call waits and what differs between
    /// the layers is their own cost per call. Rounds alternate between the layers; each case prints the median ns per
    /// message (its send and its receive: 2 calls, 4 for zero-copy) of each layer and the difference.
    /// </summary>
    internal static unsafe class Layers
    {
        private const int Ring = 1 << 20;
        private const int Batch = 256;
        private const int MessagesPerRound = 100_000;
        private const int Rounds = 21;
        private static readonly int[] Sizes = { 16, 64, 1024 };
        private static readonly string[] Ops = { "copy", "zerocopy", "rpc" };

        public static int Run()
        {
            string prefix = $"cslayers_{Environment.ProcessId}_{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}";
            Check(Fipc.Listen(prefix + "_raw", Ring, out IntPtr rawListener), "listen");
            Check(Fipc.Connect(prefix + "_raw", out IntPtr rawClient, 10_000), "connect");
            Check(Fipc.Accept(rawListener, out IntPtr rawServer, 10_000), "accept");
            Check(FipcListener.Listen(prefix + "_obj", Ring, out FipcListener? listener), "listen");
            Check(FipcConnection.Connect(prefix + "_obj", out FipcConnection? client, 10_000), "connect");
            Check(listener!.Accept(out FipcConnection? server, 10_000), "accept");

            Console.WriteLine("FastIPC C# benchmark: layers (ns per message: a send and a receive, one thread)\n");
            Console.WriteLine($"{"case",-16} {"raw",8} {"object",8} {"difference",11}");
            foreach (string op in Ops)
            {
                foreach (int size in Sizes)
                {
                    var payload = new byte[size];
                    payload.AsSpan().Fill((byte)'x');
                    var buf = new byte[size];
                    Round(op, true, rawClient, rawServer, client!, server!, payload, buf); // warm-up
                    Round(op, false, rawClient, rawServer, client!, server!, payload, buf);
                    var raw = new double[Rounds];
                    var obj = new double[Rounds];
                    for (int r = 0; r < Rounds; r++)
                    {
                        bool rawFirst = r % 2 == 0;
                        if (rawFirst) raw[r] = Round(op, true, rawClient, rawServer, client!, server!, payload, buf);
                        obj[r] = Round(op, false, rawClient, rawServer, client!, server!, payload, buf);
                        if (!rawFirst) raw[r] = Round(op, true, rawClient, rawServer, client!, server!, payload, buf);
                    }
                    double rawNs = Median(raw), objNs = Median(obj);
                    Console.WriteLine($"{op + " " + size + "B",-16} {rawNs,8:F1} {objNs,8:F1} {objNs - rawNs,8:+0.0;-0.0} ns");
                }
            }

            client!.Dispose();
            server!.Dispose();
            listener.Dispose();
            Fipc.Close(rawClient);
            Fipc.Close(rawServer);
            Fipc.ListenerClose(rawListener);
            return 0;
        }

        /// <summary>One round of MessagesPerRound messages in batches; ns per message.</summary>
        private static double Round(string op, bool raw, IntPtr rawClient, IntPtr rawServer, FipcConnection client,
            FipcConnection server, byte[] payload, byte[] buf)
        {
            int size = payload.Length;
            var clock = Stopwatch.StartNew();
            fixed (byte* p = payload)
            fixed (byte* b = buf)
            {
                for (int sent = 0; sent < MessagesPerRound; sent += Batch)
                {
                    if (raw)
                    {
                        switch (op)
                        {
                            case "copy":
                                for (int i = 0; i < Batch; i++) Check(Fipc.Send(rawClient, p, (nuint)size, 0), "send");
                                for (int i = 0; i < Batch; i++) Check(Fipc.Recv(rawServer, b, (nuint)size, out _, 0), "recv");
                                break;
                            case "zerocopy":
                                for (int i = 0; i < Batch; i++)
                                {
                                    Check(Fipc.SendAcquire(rawClient, (nuint)size, out byte* room, 0), "send acquire");
                                    Buffer.MemoryCopy(p, room, size, size);
                                    Check(Fipc.SendCommit(rawClient, (nuint)size), "send commit");
                                }
                                for (int i = 0; i < Batch; i++)
                                {
                                    Check(Fipc.RecvAcquire(rawServer, out byte* data, out nuint len, 0), "recv acquire");
                                    Buffer.MemoryCopy(data, b, size, (long)len);
                                    Fipc.RecvRelease(rawServer);
                                }
                                break;
                            default:
                                for (int i = 0; i < Batch; i++) Check(Fipc.RpcSubmit(rawClient, 1, p, (nuint)size, out _, 0), "submit");
                                for (int i = 0; i < Batch; i++) Check(Fipc.RpcRecv(rawServer, b, (nuint)size, out _, 0), "rpc recv");
                                break;
                        }
                    }
                    else
                    {
                        switch (op)
                        {
                            case "copy":
                                for (int i = 0; i < Batch; i++) Check(client.Send(payload, 0), "send");
                                for (int i = 0; i < Batch; i++) Check(server.Receive(buf, out _, 0), "receive");
                                break;
                            case "zerocopy":
                                for (int i = 0; i < Batch; i++)
                                {
                                    Check(client.SendAcquire(size, out Span<byte> room, 0), "send acquire");
                                    payload.AsSpan().CopyTo(room);
                                    Check(client.SendCommit(size), "send commit");
                                }
                                for (int i = 0; i < Batch; i++)
                                {
                                    Check(server.ReceiveAcquire(out ReadOnlySpan<byte> data, 0), "receive acquire");
                                    data.CopyTo(buf);
                                    server.ReceiveRelease();
                                }
                                break;
                            default:
                                for (int i = 0; i < Batch; i++) Check(client.RpcSubmit(1, payload, out _, 0), "submit");
                                for (int i = 0; i < Batch; i++) Check(server.RpcReceive(buf, out _, 0), "rpc receive");
                                break;
                        }
                    }
                }
            }
            int messages = (MessagesPerRound + Batch - 1) / Batch * Batch;
            return clock.Elapsed.TotalMilliseconds * 1e6 / messages;
        }

        private static void Check(FipcResult result, string call)
        {
            if (result != FipcResult.Ok)
                throw new InvalidOperationException($"{call}: {Fipc.ResultStr(result)}");
        }

        private static double Median(double[] values)
        {
            var sorted = (double[])values.Clone();
            Array.Sort(sorted);
            return sorted[sorted.Length / 2];
        }
    }
}
