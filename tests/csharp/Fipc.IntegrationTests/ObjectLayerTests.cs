using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;
using FastIpc;

#nullable enable

namespace FastIpc.IntegrationTests
{
    /// <summary>
    /// The binding's object layer (FipcListener, FipcConnection) against the library: the round trips, the
    /// array-returning receives, the next client, and the lifetime rules of its SafeHandles (a Dispose from another
    /// thread wakes a waiting call, and the native close waits for it).
    /// </summary>
    public class ObjectLayerTests
    {
        private const int Ring = 1 << 20;
        private static int _counter;
        private readonly ITestOutputHelper _output;
        private readonly string _name;

        public ObjectLayerTests(ITestOutputHelper output)
        {
            _output = output;
            _name = $"fipc_obj_{Environment.ProcessId}_{Interlocked.Increment(ref _counter)}_{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}";
            RpcReconnectTests.SetLibraryPath();
        }

        /// <summary>A listener and a server and client connection on one name.</summary>
        private sealed class Pair : IDisposable
        {
            public FipcListener Listener = null!;
            public FipcConnection Server = null!;
            public FipcConnection Client = null!;

            public static Pair Open(string name, int ring = Ring)
            {
                var pair = new Pair();
                Assert.Equal(FipcResult.Ok, FipcListener.Listen(name, ring, out FipcListener? listener));
                pair.Listener = listener!;
                // The client connects on another thread: its connect waits for the listener's accept
                var connecting = Task.Run(() => (FipcConnection.Connect(name, out FipcConnection? c, 10_000), c));
                Assert.Equal(FipcResult.Ok, pair.Listener.Accept(out FipcConnection? server, 10_000));
                pair.Server = server!;
                var (connected, client) = connecting.GetAwaiter().GetResult();
                Assert.Equal(FipcResult.Ok, connected);
                pair.Client = client!;
                return pair;
            }

            public void Dispose()
            {
                Client?.Dispose();
                Server?.Dispose();
                Listener?.Dispose();
            }
        }

        private static byte[] Pattern(int length)
        {
            var bytes = new byte[length];
            for (int i = 0; i < length; i++)
                bytes[i] = (byte)(i * 31 + 7);
            return bytes;
        }

        private static Task Run(Action body) => Task.Run(body);

        [Fact(Timeout = 30000)]
        public Task CopyAndZeroCopy_RoundTrip() => Run(() =>
        {
            using var pair = Pair.Open(_name);
            Assert.Equal(Ring - 64, pair.Client.MaxPiece);

            byte[] message = Pattern(100);
            Assert.Equal(FipcResult.Ok, pair.Client.Send(message, 5000));
            var buf = new byte[256];
            Assert.Equal(FipcResult.Ok, pair.Server.Receive(buf, out int length, 5000));
            Assert.Equal(message, buf.AsSpan(0, length).ToArray());

            // Zero-copy: write in the ring, commit fewer bytes than acquired, read in the ring
            Assert.Equal(FipcResult.Ok, pair.Client.SendAcquire(64, out Span<byte> room, 5000));
            Assert.Equal(64, room.Length);
            message.AsSpan(0, 40).CopyTo(room);
            Assert.Equal(FipcResult.Ok, pair.Client.SendCommit(40));
            Assert.Equal(FipcResult.Invalid, pair.Client.SendCommit(40)); // no reservation left
            Assert.Equal(FipcResult.Ok, pair.Server.ReceiveAcquire(out ReadOnlySpan<byte> data, 5000));
            Assert.Equal(message.AsSpan(0, 40).ToArray(), data.ToArray());
            pair.Server.ReceiveRelease();
            pair.Server.ReceiveRelease(); // a no-op without a message

            // A message of several pieces: TooLarge for zero-copy, then received whole by a copying call
            Assert.Equal(FipcResult.TooLarge, pair.Client.SendAcquire(Ring, out _, 0));
            Assert.Equal(FipcResult.Ok, pair.Client.Send(Pattern(1000), 5000));
            var sender = Task.Run(() => pair.Client.Send(Pattern(2 * Ring), 10_000));
            Assert.Equal(FipcResult.Ok, pair.Server.ReceiveAcquire(out data, 5000));
            Assert.Equal(1000, data.Length);
            // The next receive releases the acquired message first
            Assert.Equal(FipcResult.TooLarge, pair.Server.ReceiveAcquire(out _, 5000));
            Assert.Equal(FipcResult.TooLarge, pair.Server.Receive(buf, out length, 5000));
            Assert.Equal(2 * Ring, length);
            Assert.Equal(FipcResult.Ok, pair.Server.Receive(out byte[] whole, 5000));
            Assert.Equal(Pattern(2 * Ring), whole);
            Assert.Equal(FipcResult.Ok, sender.Result);
            Assert.Equal(FipcResult.Timeout, pair.Server.Receive(buf, out _, 0));
        });

        [Fact(Timeout = 30000)]
        public Task Rpc_RoundTrip() => Run(() =>
        {
            using var pair = Pair.Open(_name);
            byte[] request = Pattern(16);
            Assert.Equal(FipcResult.Ok, pair.Client.RpcSubmit(7, request, out ulong id, 5000));
            Assert.Equal(1UL, id);

            var buf = new byte[64];
            Assert.Equal(FipcResult.Ok, pair.Server.RpcReceive(buf, out FipcRpcHeader header, 5000));
            Assert.Equal(FipcRpcKind.Request, header.Kind);
            Assert.Equal(id, header.Id);
            Assert.Equal(7u, header.Opcode);
            Assert.Equal(0, header.Status);
            Assert.Equal(request, buf.AsSpan(0, header.Length).ToArray());

            Assert.Equal(FipcResult.Ok, pair.Server.RpcRespond(header.Id, 8, -3, ReadOnlySpan<byte>.Empty, 5000));
            Assert.Equal(FipcResult.Ok, pair.Client.RpcReceive(out FipcRpcHeader reply, out byte[] payload, 5000));
            Assert.Equal(FipcRpcKind.Response, reply.Kind);
            Assert.Equal(id, reply.Id);
            Assert.Equal(8u, reply.Opcode);
            Assert.Equal(-3, reply.Status);
            Assert.Empty(payload);

            // TooLarge fills the header and takes nothing
            Assert.Equal(FipcResult.Ok, pair.Client.RpcSubmit(9, Pattern(100), out _, 5000));
            Assert.Equal(FipcResult.TooLarge, pair.Server.RpcReceive(buf, out header, 5000));
            Assert.Equal(100, header.Length);
            Assert.Equal(FipcResult.Ok, pair.Server.RpcReceive(out header, out payload, 5000));
            Assert.Equal(9u, header.Opcode);
            Assert.Equal(Pattern(100), payload);
        });

        /// <summary>The array-returning receives take a message longer than their scratch buffer, and longer than the ring.</summary>
        [Fact(Timeout = 30000)]
        public Task ReceiveArray_LargerThanScratchAndRing() => Run(() =>
        {
            using var pair = Pair.Open(_name);
            int[] sizes = { 1, 4096, 4097, 100_000, 3 * Ring + 5 };
            var sender = Task.Run(() =>
            {
                foreach (int size in sizes)
                    Assert.Equal(FipcResult.Ok, pair.Client.Send(Pattern(size), 10_000));
                foreach (int size in sizes)
                    Assert.Equal(FipcResult.Ok, pair.Client.RpcSubmit((uint)size, Pattern(size), out _, 10_000));
            });
            foreach (int size in sizes)
            {
                Assert.Equal(FipcResult.Ok, pair.Server.Receive(out byte[] message, 10_000));
                Assert.Equal(Pattern(size), message);
            }
            foreach (int size in sizes)
            {
                Assert.Equal(FipcResult.Ok, pair.Server.RpcReceive(out FipcRpcHeader header, out byte[] payload, 10_000));
                Assert.Equal((uint)size, header.Opcode);
                Assert.Equal(size, header.Length);
                Assert.Equal(Pattern(size), payload);
            }
            sender.Wait();
            Assert.Equal(FipcResult.Timeout, pair.Server.Receive(out byte[] none, 0));
            Assert.Empty(none);
        });

        /// <summary>A Dispose from another thread wakes a receive that waits; it returns Cancelled, and the peer then sees the end.</summary>
        [Fact(Timeout = 30000)]
        public Task Dispose_DuringBlockedReceive_ReturnsCancelled() => Run(() =>
        {
            using var pair = Pair.Open(_name);
            FipcResult received = FipcResult.Ok;
            var blocked = new Thread(() => received = pair.Server.Receive(new byte[64], out _, Fipc.Forever));
            blocked.Start();
            Thread.Sleep(200);

            var sw = Stopwatch.StartNew();
            pair.Server.Dispose();
            Assert.True(blocked.Join(5000), "the blocked receive did not return within 5 s of the Dispose");
            Assert.Equal(FipcResult.Cancelled, received);
            Assert.Equal(FipcResult.Disconnected, pair.Client.Receive(new byte[64], out _, 5000));
            Assert.Throws<ObjectDisposedException>(() => pair.Server.Receive(new byte[64], out _, 0));
            _output.WriteLine($"Dispose woke the blocked receive in {sw.ElapsedMilliseconds} ms");
        });

        /// <summary>The native close waits for what holds the connection: here an acquired message, read after the Dispose.</summary>
        [Fact(Timeout = 30000)]
        public Task Dispose_WhileMessageAcquired_ClosesOnRelease() => Run(() =>
        {
            using var pair = Pair.Open(_name);
            byte[] message = Pattern(1000);
            Assert.Equal(FipcResult.Ok, pair.Client.Send(message, 5000));
            Assert.Equal(FipcResult.Ok, pair.Server.ReceiveAcquire(out ReadOnlySpan<byte> data, 5000));

            var disposer = new Thread(() => pair.Server.Dispose());
            disposer.Start();
            Assert.True(disposer.Join(5000));

            // Still mapped and open: the peer sees no end until the release
            Assert.Equal(message, data.ToArray());
            Assert.Equal(FipcResult.Timeout, pair.Client.Receive(new byte[64], out _, 300));
            pair.Server.ReceiveRelease();
            Assert.Equal(FipcResult.Disconnected, pair.Client.Receive(new byte[64], out _, 5000));
        });

        /// <summary>A Dispose from another thread stops a send of many pieces that waits for room; the receiver never sees part of it.</summary>
        [Fact(Timeout = 30000)]
        public Task Dispose_DuringLargeSend_ReturnsCancelled() => Run(() =>
        {
            using var pair = Pair.Open(_name);
            FipcResult sent = FipcResult.Ok;
            var sender = new Thread(() => sent = pair.Client.Send(Pattern(8 * Ring), Fipc.Forever));
            sender.Start();
            Thread.Sleep(300); // the ring is full: the send waits for room

            pair.Client.Dispose();
            Assert.True(sender.Join(5000), "the blocked send did not return within 5 s of the Dispose");
            Assert.Equal(FipcResult.Cancelled, sent);
            var buf = new byte[8 * Ring];
            Assert.Equal(FipcResult.Disconnected, pair.Server.Receive(buf, out _, 5000));
        });

        [Fact(Timeout = 30000)]
        public Task CallsAfterDispose_Throw() => Run(() =>
        {
            using var pair = Pair.Open(_name);
            FipcConnection conn = pair.Client;
            conn.Dispose();
            var buf = new byte[16];
            Assert.Throws<ObjectDisposedException>(() => conn.Send(buf, 0));
            Assert.Throws<ObjectDisposedException>(() => conn.Receive(buf, out _, 0));
            Assert.Throws<ObjectDisposedException>(() => conn.Receive(out _, 0));
            Assert.Throws<ObjectDisposedException>(() => conn.SendAcquire(1, out _, 0));
            Assert.Throws<ObjectDisposedException>(() => conn.SendCommit(1));
            Assert.Throws<ObjectDisposedException>(() => conn.ReceiveAcquire(out _, 0));
            Assert.Throws<ObjectDisposedException>(() => conn.RpcSubmit(1, buf, out _, 0));
            Assert.Throws<ObjectDisposedException>(() => conn.RpcRespond(1, 1, 0, buf, 0));
            Assert.Throws<ObjectDisposedException>(() => conn.RpcReceive(buf, out _, 0));
            Assert.Throws<ObjectDisposedException>(() => conn.RpcReceive(out _, out _, 0));
            conn.ReceiveRelease();
            conn.Cancel();
            conn.Dispose();

            pair.Listener.Dispose();
            Assert.Throws<ObjectDisposedException>(() => pair.Listener.Accept(out _, 0));
            pair.Listener.Cancel();
            pair.Listener.Dispose();
        });

        /// <summary>The listener accepts one client at a time: the next one waits until the connection accepted last is disposed.</summary>
        [Fact(Timeout = 30000)]
        public Task Listener_AcceptsTheNextClient() => Run(() =>
        {
            Assert.Equal(FipcResult.Ok, FipcListener.Listen(_name, Ring, out FipcListener? listening));
            using (FipcListener listener = listening!)
            {
                Assert.Equal(FipcResult.AddrInUse, FipcListener.Listen(_name, Ring, out FipcListener? second));
                Assert.Null(second);
                Assert.Equal(FipcResult.Timeout, listener.Accept(out FipcConnection? none, 0));
                Assert.Null(none);

                var connecting1 = Task.Run(() => (FipcConnection.Connect(_name, out FipcConnection? c, 10_000), c));
                Assert.Equal(FipcResult.Ok, listener.Accept(out FipcConnection? server1, 10_000));
                var (connected1, client1) = connecting1.GetAwaiter().GetResult();
                Assert.Equal(FipcResult.Ok, connected1);

                FipcResult connected2 = FipcResult.Invalid;
                FipcConnection? client2 = null;
                var connecting = new Thread(() => connected2 = FipcConnection.Connect(_name, out client2, 10_000));
                connecting.Start();
                Assert.Equal(FipcResult.Invalid, listener.Accept(out _, 0)); // server1 is open

                Assert.Equal(FipcResult.Ok, client1!.Send(new byte[] { 1 }, 5000));
                Assert.Equal(FipcResult.Ok, server1!.Receive(out byte[] first, 5000));
                Assert.Equal(new byte[] { 1 }, first);
                server1.Dispose();
                client1.Dispose();

                Assert.Equal(FipcResult.Ok, listener.Accept(out FipcConnection? server2, 10_000));
                Assert.True(connecting.Join(10_000));
                Assert.Equal(FipcResult.Ok, connected2);
                using (FipcConnection s2 = server2!)
                using (FipcConnection c2 = client2!)
                {
                    Assert.Equal(FipcResult.Ok, c2.Send(new byte[] { 2 }, 5000));
                    Assert.Equal(FipcResult.Ok, s2.Receive(out byte[] next, 5000));
                    Assert.Equal(new byte[] { 2 }, next);
                }

                // A Dispose from another thread wakes an Accept that waits
                FipcResult accepted = FipcResult.Ok;
                var accepting = new Thread(() => accepted = listener.Accept(out _, Fipc.Forever));
                accepting.Start();
                Thread.Sleep(200);
                listener.Dispose();
                Assert.True(accepting.Join(5000), "the blocked accept did not return within 5 s of the Dispose");
                Assert.Equal(FipcResult.Cancelled, accepted);
            }
            // The name is free again
            Assert.Equal(FipcResult.Ok, FipcListener.Listen(_name, Ring, out FipcListener? again));
            again!.Dispose();
        });
    }
}
