using System;
using System.Text;
using System.Threading;

#nullable enable

namespace FastIpc.IntegrationTests
{
    public static partial class Snippets
    {
        static byte[] Respond(ReadOnlySpan<byte> request) => Encoding.UTF8.GetBytes(Text(request).ToUpperInvariant());

        static bool Dispatch(ReadOnlySpan<byte> message) => Record($"{message.Length} bytes");

        // page: server
        static void RunServer(CancellationToken token)
        {
            using var listener = FipcStream.Create("orders", 1 << 20);
            using var stopListening = token.Register(listener.Cancel);
            try
            {
                while (true)
                {
                    using var conn = listener.Accept();   // the next client
                    using var stopServing = token.Register(conn.Cancel);
                    Serve(conn);
                }
            }
            catch (FipcException e) when (e.Result == FipcResult.Cancelled)
            {
                // the token cancelled the listener: shutting down
            }
        }
        // end

        // page: serve
        static void Serve(FipcConnection conn)
        {
            var buffer = new byte[64 * 1024];   // reused: no allocation per message
            while (true)
            {
                FipcResult result = conn.Receive(buffer, out int length);
                bool more = result switch
                {
                    FipcResult.Ok => Reply(conn, buffer.AsSpan(0, length)),
                    FipcResult.TooLarge => Grow(ref buffer, length),   // nothing was taken: again
                    FipcResult.Disconnected => false,   // the client left: accept the next
                    FipcResult.Cancelled => false,      // shutting down
                    FipcResult.Invalid => throw Unexpected("Receive", result),   // a bug: two receiving threads
                    FipcResult.Timeout or FipcResult.NoMemory or FipcResult.AddrInUse or FipcResult.Overrun
                        or FipcResult.Unsubscribed or FipcResult.Abandoned
                        => throw Unexpected("Receive", result),   // never on a stream, waiting for ever
                };
                if (!more)
                    return;
            }
        }

        static bool Grow(ref byte[] buffer, int needed)
        {
            buffer = new byte[needed];
            return true;
        }

        static bool Reply(FipcConnection conn, ReadOnlySpan<byte> request)
        {
            FipcResult result = conn.Send(Respond(request));
            return result switch
            {
                FipcResult.Ok => true,
                FipcResult.Disconnected or FipcResult.Cancelled => false,
                FipcResult.Timeout or FipcResult.TooLarge or FipcResult.Invalid or FipcResult.NoMemory
                    or FipcResult.AddrInUse or FipcResult.Overrun or FipcResult.Unsubscribed
                    or FipcResult.Abandoned => throw Unexpected("Send", result),
            };
        }
        // end

        // page: client
        static int Ping()
        {
            FipcConnection conn;
            try
            {
                conn = FipcStream.Connect("orders", timeoutMs: 5000);
            }
            catch (FipcException e) when (e.Result == FipcResult.Timeout)
            {
                Console.Error.WriteLine("no server on 'orders' within 5 s");
                return 2;
            }
            // any other FipcException (Invalid: a server of another version or user) propagates
            using (conn)
            {
                if (conn.Send("ping"u8) != FipcResult.Ok || conn.Receive(out byte[] reply) != FipcResult.Ok)
                    return 1;   // the server left
                Console.WriteLine(Encoding.UTF8.GetString(reply));
            }
            return 0;
        }
        // end

        // page: frame-loop
        sealed class GameEndpoint : IDisposable
        {
            private readonly FipcListener _listener = FipcStream.Create("game", 1 << 22);
            private FipcConnection? _conn;
            private byte[] _buffer = new byte[64 * 1024];

            public void Tick()   // once per frame: takes what came, never waits
            {
                if (_conn is null && !TryAccept())
                    return;
                while (true)
                {
                    FipcResult result = _conn!.Receive(_buffer, out int length, Fipc.NoWait);
                    bool more = result switch
                    {
                        FipcResult.Ok => Dispatch(_buffer.AsSpan(0, length)),
                        FipcResult.TooLarge => Grow(ref _buffer, length),   // stays queued: read it now
                        FipcResult.Timeout => false,        // nothing more this frame
                        FipcResult.Disconnected => Drop(),  // the client left: free the slot for the next
                        FipcResult.Cancelled => false,      // shutting down
                        FipcResult.Invalid or FipcResult.NoMemory or FipcResult.AddrInUse or FipcResult.Overrun
                            or FipcResult.Unsubscribed or FipcResult.Abandoned
                            => throw Unexpected("Receive", result),
                    };
                    if (!more)
                        return;
                }
            }

            private bool TryAccept()
            {
                FipcResult result = _listener.Accept(out _conn, Fipc.NoWait);   // the polling form: never throws
                return result switch
                {
                    FipcResult.Ok => true,
                    FipcResult.Timeout or FipcResult.Cancelled => false,   // no client yet, or shutting down
                    FipcResult.NoMemory => throw Unexpected("Accept", result),   // failed for good: create it again
                    FipcResult.Invalid or FipcResult.Disconnected or FipcResult.TooLarge or FipcResult.AddrInUse
                        or FipcResult.Overrun or FipcResult.Unsubscribed or FipcResult.Abandoned
                        => throw Unexpected("Accept", result),
                };
            }

            private bool Drop()
            {
                _conn!.Dispose();
                _conn = null;
                return false;
            }

            public void Dispose()
            {
                _conn?.Dispose();
                _listener.Dispose();
            }
        }
        // end
    }
}
