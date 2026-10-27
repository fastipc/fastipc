using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;

#nullable enable

namespace FastIpc.IntegrationTests
{
    /// <summary>
    /// The C# page's code (website/csharp.html), compiled here: every block of the page is a "// page:" region of these
    /// files, line for line (<see cref="DocsTests"/> checks it), and <see cref="PatternTests"/> runs them in pairs. What
    /// the page leaves to the application (Respond, Dispatch, Show, ...) is a stub here that records what it saw. The
    /// project's CS8509 setting makes every switch expression over a FipcResult handle each result.
    /// </summary>
    public static partial class Snippets
    {
        // page: unexpected
        static Exception Unexpected(string call, FipcResult result) =>
            new UnreachableException($"{call}: {Fipc.ResultStr(result)}");
        // end

        /// <summary>What the stubs saw, in order.</summary>
        static readonly ConcurrentQueue<string> Seen = new();

        static string Text(ReadOnlySpan<byte> bytes) => Encoding.UTF8.GetString(bytes);

        static bool Record(string what)
        {
            Seen.Enqueue(what);
            return true;
        }

        static void Handle(ReadOnlySpan<byte> message) => Record(Text(message));

        // === Messages ===

        static void EchoServer()
        {
            // page: echo-server
            // rings of 1 MiB each way
            using var listener = FipcStream.Create("demo", 1 << 20);
            using var conn = listener.Accept();
            while (conn.Receive(out byte[] msg) == FipcResult.Ok)
                conn.Send(msg);   // echo
            // Disconnected: the client closed or exited
            // end
        }

        static void EchoClient()
        {
            // page: echo-client
            using var conn = FipcStream.Connect("demo", 5000);
            conn.Send("hello"u8);
            if (conn.Receive(out byte[] reply) == FipcResult.Ok)
                Console.WriteLine(Encoding.UTF8.GetString(reply));
            // end
            Record(Encoding.UTF8.GetString(reply));
        }

        // === Zero-copy ===

        /// <summary>A message the application encodes straight into the ring.</summary>
        sealed class Frame
        {
            private readonly byte[] _bytes;

            public Frame(byte[] bytes) => _bytes = bytes;

            public int Size => _bytes.Length;

            public int EncodeInto(Span<byte> room)
            {
                _bytes.CopyTo(room);
                return _bytes.Length;
            }

            public byte[] ToArray() => _bytes;
        }

        // page: send-frame
        static FipcResult SendFrame(FipcConnection conn, Frame frame)
        {
            FipcResult result = conn.SendAcquire(frame.Size, out Span<byte> room);   // room in the ring
            if (result == FipcResult.Ok)
                return conn.SendCommit(frame.EncodeInto(room));   // publishes what it wrote; never waits
            if (result == FipcResult.TooLarge)
                return conn.Send(frame.ToArray());   // over MaxPiece: the copying Send takes it in pieces
            return result;   // Timeout, Disconnected or Cancelled: the caller's to decide
        }
        // end

        static void ReceiveInPlace(FipcConnection conn)
        {
            // page: receive-in-place
            if (conn.ReceiveAcquire(out ReadOnlySpan<byte> msg)
                == FipcResult.Ok)
            {
                Handle(msg);            // read-only, in the ring
                conn.ReceiveRelease();  // frees its room
            }
            // end
        }

        // === Batches ===

        static void BatchEachWay(FipcConnection conn, FipcConnection peer)
        {
            // page: batch
            byte[][] batch = { "one"u8.ToArray(), "two"u8.ToArray() };
            conn.SendMany(batch, out int sent);

            var buf = new byte[65536];
            var lengths = new int[64];
            if (peer.ReceiveMany(buf, lengths, out int count)
                == FipcResult.Ok)
            {
                int at = 0;
                for (int i = 0; i < count; at += lengths[i++])
                    Handle(buf.AsSpan(at, lengths[i]));
            }
            // end
            Record($"sent {sent}");
        }

        // === The raw layer ===

        static void RawCalls()
        {
            // page: raw
            Fipc.StreamConnect("my_channel", out IntPtr conn, 5000);
            Fipc.Send(conn, "hello"u8, Fipc.Forever);
            Fipc.Close(conn);
            // end
        }
    }
}
