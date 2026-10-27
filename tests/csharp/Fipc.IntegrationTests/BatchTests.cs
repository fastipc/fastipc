using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using FastIpc;

#nullable enable

namespace FastIpc.IntegrationTests
{
    /// <summary>
    /// Batches through the binding (SendMany, ReceiveMany): on a stream, in order and packed back to back, a buffer that
    /// stops a batch and a first message too long for it, NoWait's partial send with how many went, an empty message
    /// refused before any goes, the raw layer's calls; a broadcast's and a work queue's batches, and TooLarge over
    /// MaxPiece before any goes; a service client's batch of requests, owed as its sends are, with their replies in
    /// order, and a server's batches refused.
    /// </summary>
    public class BatchTests
    {
        private const int TimeoutMs = 10_000;
        private static int _counter;
        private readonly string _name;

        public BatchTests()
        {
            _name = $"fipc_b_{Environment.ProcessId}_{Interlocked.Increment(ref _counter)}_{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}";
            RawLayerTests.SetLibraryPath();
        }

        private static Task Run(Action body) => Task.Run(body);

        private static byte[] Bytes(string text) => Encoding.UTF8.GetBytes(text);

        private static string Text(ReadOnlySpan<byte> bytes) => Encoding.UTF8.GetString(bytes);

        private static byte[] Pattern(int length, int seed)
        {
            var bytes = new byte[length];
            for (int i = 0; i < length; i++)
                bytes[i] = (byte)(i * 31 + seed);
            return bytes;
        }

        private static (FipcListener, FipcConnection, FipcConnection) Stream(string name, int ring)
        {
            FipcListener listener = FipcStream.Create(name, ring);
            var connecting = Task.Run(() => FipcStream.Connect(name, TimeoutMs));
            FipcConnection server = listener.Accept(TimeoutMs);
            FipcConnection client = connecting.GetAwaiter().GetResult();
            return (listener, server, client);
        }

        /// <summary><paramref name="count"/> messages, received in batches (a broadcast's subscriber may take fewer than are there).</summary>
        private static List<byte[]> ReceiveAll(FipcConnection conn, int count, int maxCount = 16)
        {
            var received = new List<byte[]>();
            while (received.Count < count)
            {
                Assert.Equal(FipcResult.Ok, conn.ReceiveMany(out byte[][] batch, maxCount, TimeoutMs));
                Assert.InRange(batch.Length, 1, maxCount);
                received.AddRange(batch);
            }
            return received;
        }

        [Fact(Timeout = 30000)]
        public Task Stream_BatchesComeInOrderPackedBackToBack() => Run(() =>
        {
            var (listener, server, client) = Stream(_name, 1024);
            using var l = listener;
            using var s = server;
            using var c = client;
            byte[][] messages = Enumerable.Range(0, 6).Select(i => Pattern(10 + i * 20, i)).ToArray();
            Assert.Equal(FipcResult.Ok, client.SendMany(messages, out int sent, TimeoutMs));
            Assert.Equal(6, sent);
            var buf = new byte[4096];
            var lengths = new int[16];
            Assert.Equal(FipcResult.Ok, server.ReceiveMany(buf, lengths, out int count, TimeoutMs));
            Assert.Equal(6, count);
            int at = 0;
            for (int i = 0; i < 6; i++)
            {
                Assert.Equal(messages[i], buf.AsSpan(at, lengths[i]).ToArray());
                at += lengths[i];
            }

            // A buffer that stops the batch; a first message too long for it stays queued, its length reported
            var memories = messages.Select(m => (ReadOnlyMemory<byte>)m).ToArray();
            Assert.Equal(FipcResult.Ok, client.SendMany(memories, out sent, TimeoutMs));
            Assert.Equal(FipcResult.Ok, server.ReceiveMany(buf.AsSpan(0, 10 + 30), lengths, out count, TimeoutMs));
            Assert.Equal(2, count);
            Assert.Equal(FipcResult.TooLarge, server.ReceiveMany(buf.AsSpan(0, 10), lengths, out count, TimeoutMs));
            Assert.Equal(0, count);
            Assert.Equal(50, lengths[0]);
            Assert.Equal(FipcResult.Ok, server.ReceiveMany(buf, lengths.AsSpan(0, 3), out count, TimeoutMs));
            Assert.Equal(3, count); // at most lengths.Length
            Assert.Equal(FipcResult.Ok, server.ReceiveMany(out byte[][] last, 16, TimeoutMs));
            Assert.Equal(messages[5], Assert.Single(last));

            // A message longer than one piece goes in pieces, and the array-returning receive takes it whole
            byte[] longMessage = Pattern(100_000, 3);
            List<byte[]>? received = null;
            var receiving = Task.Run(() => received = ReceiveAll(server, 3));
            Assert.Equal(FipcResult.Ok, client.SendMany(new[] { messages[0], longMessage, messages[1] }, out sent, TimeoutMs));
            receiving.GetAwaiter().GetResult();
            Assert.Equal(new[] { messages[0], longMessage, messages[1] }, received);
            Assert.Equal(FipcResult.Timeout, server.ReceiveMany(buf, lengths, out count, Fipc.NoWait));
        });

        [Fact(Timeout = 30000)]
        public Task Stream_PartialSendReportsHowManyWent() => Run(() =>
        {
            var (listener, server, client) = Stream(_name, 1024);
            using var l = listener;
            using var s = server;
            using var c = client;
            byte[] hundred = Pattern(100, 1);
            byte[][] many = Enumerable.Repeat(hundred, 100).ToArray();
            // Frames of 128 bytes: the ring of 1 KiB holds 8, fewer where its end is padded
            Assert.Equal(FipcResult.Timeout, client.SendMany(many, out int sent, Fipc.NoWait));
            Assert.InRange(sent, 1, 8);
            Assert.All(ReceiveAll(server, sent), m => Assert.Equal(hundred, m));

            List<byte[]>? received = null;
            var receiving = Task.Run(() => received = ReceiveAll(server, 100));
            Assert.Equal(FipcResult.Ok, client.SendMany(many, out sent, TimeoutMs)); // sent while it is received
            Assert.Equal(100, sent);
            receiving.GetAwaiter().GetResult();
            Assert.Equal(100, received!.Count);

            Assert.Equal(FipcResult.Invalid, client.SendMany(new[] { hundred, Array.Empty<byte>() }, out sent, Fipc.NoWait));
            Assert.Equal(0, sent);
            Assert.Equal(FipcResult.Invalid, client.SendMany(Array.Empty<byte[]>(), out sent, Fipc.NoWait));
            Assert.Equal(FipcResult.Invalid, server.ReceiveMany(new byte[16], Span<int>.Empty, out int count, Fipc.NoWait));
            Assert.Equal(FipcResult.Timeout, server.ReceiveMany(out byte[][] _, 16, Fipc.NoWait)); // nothing went
        });

        [Fact(Timeout = 30000)]
        public Task RawLayer_SendManyAndRecvMany() => Run(() =>
        {
            Assert.Equal(FipcResult.Ok, Fipc.StreamCreate(_name, 1 << 16, out IntPtr listener));
            var connecting = Task.Run(() => (Fipc.StreamConnect(_name, out IntPtr c, TimeoutMs), c));
            Assert.Equal(FipcResult.Ok, Fipc.StreamAccept(listener, out IntPtr server, TimeoutMs));
            var (connected, client) = connecting.GetAwaiter().GetResult();
            Assert.Equal(FipcResult.Ok, connected);
            try
            {
                unsafe
                {
                    byte[] a = Bytes("one"), b = Bytes("three");
                    fixed (byte* pa = a)
                    fixed (byte* pb = b)
                    {
                        byte** datas = stackalloc byte*[2] { pa, pb };
                        nuint* lens = stackalloc nuint[2] { 3, 5 };
                        Assert.Equal(FipcResult.Ok, Fipc.SendMany(client, datas, lens, 2, out nuint sent, TimeoutMs));
                        Assert.Equal((nuint)2, sent);
                    }
                    byte* buf = stackalloc byte[64];
                    nuint* got = stackalloc nuint[4];
                    Assert.Equal(FipcResult.Ok, Fipc.RecvMany(server, buf, 64, got, 4, out nuint count, TimeoutMs));
                    Assert.Equal((nuint)2, count);
                    Assert.Equal((nuint)3, got[0]);
                    Assert.Equal((nuint)5, got[1]);
                    Assert.Equal("onethree", Text(new ReadOnlySpan<byte>(buf, 8)));
                }
            }
            finally
            {
                Fipc.Close(client);
                Fipc.Close(server);
                Fipc.ListenerClose(listener);
            }
        });

        [Fact(Timeout = 30000)]
        public Task Broadcast_BatchesReachEverySubscriber() => Run(() =>
        {
            using var publisher = FipcBroadcast.Create(_name, 1 << 16, 4, Fipc.Forever);
            using var lossy = FipcBroadcast.Join(_name, FipcSubscriberRole.Lossy, TimeoutMs);
            using var gated = FipcBroadcast.Join(_name, FipcSubscriberRole.Gated, TimeoutMs);
            Assert.Equal(FipcResult.Ok, publisher.WaitForMembers(2, out _, TimeoutMs));
            byte[][] ticks = Enumerable.Range(0, 40).Select(i => Bytes($"tick {i}")).ToArray();
            Assert.Equal(FipcResult.Ok, publisher.SendMany(ticks, out int sent, TimeoutMs));
            Assert.Equal(40, sent);
            Assert.Equal(FipcResult.TooLarge,
                publisher.SendMany(new[] { ticks[0], new byte[publisher.MaxPiece + 1] }, out sent, Fipc.NoWait));
            Assert.Equal(0, sent);
            Assert.Equal(FipcResult.Invalid, publisher.ReceiveMany(out byte[][] _, 16, Fipc.NoWait));
            Assert.Equal(FipcResult.Invalid, gated.SendMany(ticks, out sent, Fipc.NoWait));
            publisher.Dispose(); // what it sent is received first
            foreach (FipcConnection subscriber in new[] { lossy, gated })
            {
                Assert.Equal(ticks, ReceiveAll(subscriber, 40));
                Assert.Equal(FipcResult.Disconnected, subscriber.ReceiveMany(out byte[][] _, 16, TimeoutMs));
            }
        });

        [Fact(Timeout = 30000)]
        public Task Queue_BatchesDeliverEveryMessageOnce() => Run(() =>
        {
            using var queue = FipcQueue.Create(_name, 4096, 1, 1);
            using var producer = FipcQueue.Join(_name, FipcQueueRole.Producer, TimeoutMs);
            using var consumer = FipcQueue.Join(_name, FipcQueueRole.Consumer, TimeoutMs);
            byte[][] jobs = Enumerable.Range(0, 200).Select(i => Bytes($"job {i}")).ToArray();
            List<byte[]>? received = null;
            var receiving = Task.Run(() => received = ReceiveAll(consumer, 200));
            for (int at = 0; at < 200; at += 50)
            {
                Assert.Equal(FipcResult.Ok, producer.SendMany(jobs.AsSpan(at, 50), out int sent, TimeoutMs));
                Assert.Equal(50, sent);
            }
            receiving.GetAwaiter().GetResult();
            Assert.Equal(jobs, received); // one producer: its messages in order
            Assert.Equal(FipcResult.TooLarge,
                producer.SendMany(new[] { jobs[0], new byte[producer.MaxPiece + 1] }, out int none, Fipc.NoWait));
            Assert.Equal(FipcResult.Invalid, consumer.SendMany(jobs, out none, Fipc.NoWait));
            Assert.Equal(FipcResult.Invalid, producer.ReceiveMany(out byte[][] _, 16, Fipc.NoWait));
            queue.Dispose();
            Assert.Equal(FipcResult.Disconnected, consumer.ReceiveMany(out byte[][] _, 16, TimeoutMs));
            Assert.Equal(FipcResult.Disconnected, producer.SendMany(jobs, out none, TimeoutMs));
        });

        [Fact(Timeout = 30000)]
        public Task Service_ClientsBatchIsOwedAsItsSendsAre() => Run(() =>
        {
            using var service = FipcService.Create(_name, 1 << 14, 64, 1, 1);
            using var server = FipcService.Join(_name, FipcServiceRole.Server, TimeoutMs);
            using var client = FipcService.Join(_name, FipcServiceRole.Client, TimeoutMs);
            byte[][] requests = { Bytes("a"), Bytes("bb"), Bytes("ccc") };
            Assert.Equal(FipcResult.Ok, client.SendMany(requests, out int sent, TimeoutMs));
            Assert.Equal(3, sent);
            Assert.Equal(FipcResult.Invalid, client.Call(Bytes("d"), out byte[] _, Fipc.NoWait)); // the caller's replies
            foreach (byte[] request in requests)
            {
                Assert.Equal(FipcResult.Ok, server.Receive(out byte[] got, TimeoutMs));
                Assert.Equal(request, got);
                Assert.Equal(FipcResult.Ok, server.Send(Bytes(Text(got) + " done"), Fipc.NoWait));
            }
            Assert.Equal(new[] { "a done", "bb done", "ccc done" }, ReceiveAll(client, 3).Select(r => Text(r)));

            var answering = Task.Run(() =>
            {
                Assert.Equal(FipcResult.Ok, server.Receive(out byte[] got, TimeoutMs));
                Assert.Equal(FipcResult.Ok, server.Send(Bytes(Text(got) + " done"), Fipc.NoWait));
            });
            Assert.Equal(FipcResult.Ok, client.Call(Bytes("e"), out byte[] reply, TimeoutMs)); // nothing owed now
            Assert.Equal("e done", Text(reply));
            answering.GetAwaiter().GetResult();
            Assert.Equal(FipcResult.Invalid, server.SendMany(requests, out sent, Fipc.NoWait));
            Assert.Equal(FipcResult.Invalid, server.ReceiveMany(out byte[][] _, 16, Fipc.NoWait));

            // A batch whose requests don't all fit counts only those that went: once their replies are received, a
            // call goes again. Each request holds 64 + 64 bytes of the 16 KiB ring: at most 128 fit
            byte[][] many = Enumerable.Repeat(Bytes("f"), 200).ToArray();
            Assert.Equal(FipcResult.Timeout, client.SendMany(many, out sent, Fipc.NoWait));
            Assert.InRange(sent, 1, 128);
            for (int i = 0; i < sent; i++)
            {
                Assert.Equal(FipcResult.Ok, server.Receive(out byte[] got, TimeoutMs));
                Assert.Equal(FipcResult.Ok, server.Send(got, Fipc.NoWait));
            }
            Assert.Equal(sent, ReceiveAll(client, sent).Count);
            answering = Task.Run(() =>
            {
                Assert.Equal(FipcResult.Ok, server.Receive(out byte[] got, TimeoutMs));
                Assert.Equal(FipcResult.Ok, server.Send(Bytes(Text(got) + " done"), Fipc.NoWait));
            });
            Assert.Equal(FipcResult.Ok, client.Call(Bytes("g"), out reply, TimeoutMs));
            Assert.Equal("g done", Text(reply));
            answering.GetAwaiter().GetResult();
        });
    }
}
