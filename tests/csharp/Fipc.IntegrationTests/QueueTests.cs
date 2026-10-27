using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;
using FastIpc;

#nullable enable

namespace FastIpc.IntegrationTests
{
    /// <summary>
    /// The work queue through the binding: producers and consumers in this process (FipcQueue.Create, FipcQueue.Join,
    /// the owner's members joined from its own thread), with every message delivered exactly once (the queue promises
    /// no order), the queue's WaitForMembers and Cancel, the calls a queue's roles refuse, the end at the queue's
    /// Dispose, a queue that outlives its dropped object (it has no finalizer), the examples' exchange, in this process
    /// and as the example projects' two processes, the raw layer, and the Python binding's producers and consumers in
    /// other processes (bindings/python/tests/queue_peer.py, found as BroadcastTests finds its peer).
    /// </summary>
    public class QueueTests
    {
        private const int Capacity = 4096;
        private const int TimeoutMs = 10_000;
        private static int _counter;
        private readonly ITestOutputHelper _output;
        private readonly string _name;

        public QueueTests(ITestOutputHelper output)
        {
            _output = output;
            _name = $"fipc_q_{Environment.ProcessId}_{Interlocked.Increment(ref _counter)}_{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}";
            RawLayerTests.SetLibraryPath();
        }

        private static Task Run(Action body) => Task.Run(body);

        private static byte[] Bytes(string text) => Encoding.UTF8.GetBytes(text);

        private static string Job(int i) => $"job {i}";

        private static FipcQueue Create(string name, int maxProducers, int maxConsumers, int capacity = Capacity)
        {
            return FipcQueue.Create(name, capacity, maxProducers, maxConsumers);
        }

        private static FipcConnection Join(string name, FipcQueueRole role, int timeoutMs = TimeoutMs)
        {
            return FipcQueue.Join(name, role, timeoutMs);
        }

        /// <summary>
        /// Every message the consumer takes until Disconnected: into a new array (way 0), into a span (1) or in place
        /// (2).
        /// </summary>
        private static List<string> ReceiveUntilTheEnd(FipcConnection consumer, int way)
        {
            var received = new List<string>();
            var buffer = new byte[consumer.MaxPiece];
            for (;;)
            {
                FipcResult result;
                if (way == 0)
                {
                    result = consumer.Receive(out byte[] message, TimeoutMs);
                    if (result == FipcResult.Ok)
                        received.Add(Encoding.UTF8.GetString(message));
                }
                else if (way == 1)
                {
                    result = consumer.Receive(buffer, out int length, TimeoutMs);
                    if (result == FipcResult.Ok)
                        received.Add(Encoding.UTF8.GetString(buffer, 0, length));
                }
                else
                {
                    result = consumer.ReceiveAcquire(out ReadOnlySpan<byte> message, TimeoutMs);
                    if (result == FipcResult.Ok)
                    {
                        received.Add(Encoding.UTF8.GetString(message));
                        consumer.ReceiveRelease();
                    }
                }
                if (result == FipcResult.Disconnected)
                    return received;
                Assert.Equal(FipcResult.Ok, result);
            }
        }

        /// <summary>
        /// That <paramref name="received"/> is the jobs of <paramref name="producers"/> producers, each sending job 1
        /// to job <paramref name="count"/>, in any order: each job <paramref name="producers"/> times.
        /// </summary>
        private static void CheckJobs(List<string> received, int producers, int count)
        {
            var expected = Enumerable.Range(1, count).SelectMany(i => Enumerable.Repeat(Job(i), producers))
                .OrderBy(job => job, StringComparer.Ordinal);
            Assert.True(expected.SequenceEqual(received.OrderBy(job => job, StringComparer.Ordinal)),
                $"received {received.Count} of {producers * count} jobs, or a job twice");
        }

        /// <summary>
        /// Four producers and four consumers on threads of their own: every message is delivered exactly once.
        /// </summary>
        [Fact(Timeout = 60000)]
        public Task FourProducersAndFourConsumers_EveryMessageOnce() => Run(() =>
        {
            const int Count = 500;
            using FipcQueue queue = Create(_name, 4, 4);
            List<FipcConnection> producers = Enumerable.Range(0, 4).Select(_ => Join(_name, FipcQueueRole.Producer)).ToList();
            List<FipcConnection> consumers = Enumerable.Range(0, 4).Select(_ => Join(_name, FipcQueueRole.Consumer)).ToList();
            Assert.Equal(FipcResult.Ok, queue.WaitForMembers(null, 8, out int count, TimeoutMs));
            Assert.Equal(8, count);
            Assert.All(producers.Concat(consumers), member => Assert.Equal(Capacity / 4, member.MaxPiece));

            Task<List<string>>[] taken = consumers
                .Select((consumer, k) => Task.Run(() => ReceiveUntilTheEnd(consumer, k % 3))).ToArray();
            Task[] produced = producers.Select((producer, k) => Task.Run(() =>
            {
                using (producer) // its Dispose leaves the queue; its messages stay
                {
                    for (int i = 0; i < Count; i++)
                        Assert.Equal(FipcResult.Ok, producer.Send(Bytes($"{k} {i}"), TimeoutMs));
                }
            })).ToArray();
            Assert.True(Task.WaitAll(produced, 60_000), "the producers did not finish within 60 s");
            queue.Dispose(); // ends the queue: the consumers take what is left, then Disconnected
            var everything = taken
                .SelectMany(task => task.GetAwaiter().GetResult())
                .Select(text => text.Split(' '))
                .Select(parts => (int.Parse(parts[0]), int.Parse(parts[1])))
                .OrderBy(m => m).ToList();
            foreach (FipcConnection consumer in consumers)
                consumer.Dispose();

            var expected = Enumerable.Range(0, 4).SelectMany(k => Enumerable.Range(0, Count).Select(i => (k, i))).ToList();
            Assert.True(expected.SequenceEqual(everything), "a message was lost or delivered twice");
        });

        /// <summary>
        /// The owner's consumer and producer, joined from its own thread: copied, span, in-place and zero-copy
        /// messages, and the calls each role refuses.
        /// </summary>
        [Fact(Timeout = 30000)]
        public Task OwnersMembers_AndTheCallsTheirRolesRefuse() => Run(() =>
        {
            using FipcQueue queue = Create(_name, 1, 1);
            using FipcConnection consumer = Join(_name, FipcQueueRole.Consumer);
            using FipcConnection producer = Join(_name, FipcQueueRole.Producer); // the library admits both
            Assert.Equal(Capacity / 4, producer.MaxPiece);
            Assert.Equal(Capacity / 4, consumer.MaxPiece);

            // Messages wait in the queue; a consumer takes them copied, into a span or in place
            foreach (string word in new[] { "one", "two", "three", "four" })
                Assert.Equal(FipcResult.Ok, producer.Send(Bytes(word), TimeoutMs));
            var received = new List<string>();
            Assert.Equal(FipcResult.Ok, consumer.Receive(out byte[] first, TimeoutMs));
            received.Add(Encoding.UTF8.GetString(first));
            var small = new byte[2];
            Assert.Equal(FipcResult.TooLarge, consumer.Receive(small, out int needed, TimeoutMs)); // takes nothing
            Assert.InRange(needed, 3, 5);
            var buffer = new byte[16];
            Assert.Equal(FipcResult.Ok, consumer.Receive(buffer, out int length, TimeoutMs));
            received.Add(Encoding.UTF8.GetString(buffer, 0, length));
            Assert.Equal(FipcResult.Ok, consumer.ReceiveAcquire(out ReadOnlySpan<byte> inPlace, TimeoutMs));
            received.Add(Encoding.UTF8.GetString(inPlace));
            consumer.ReceiveRelease();
            Assert.Equal(FipcResult.Ok, producer.SendAcquire(4, out Span<byte> room, TimeoutMs));
            Bytes("five").CopyTo(room);
            Assert.Equal(FipcResult.Ok, producer.SendCommit(4));
            for (int i = 0; i < 2; i++)
            {
                Assert.Equal(FipcResult.Ok, consumer.Receive(out byte[] message, TimeoutMs));
                received.Add(Encoding.UTF8.GetString(message));
            }
            Assert.Equal(new[] { "five", "four", "one", "three", "two" }, received.OrderBy(word => word));

            Assert.Equal(FipcResult.Invalid, producer.Receive(buffer, out _, Fipc.NoWait));
            Assert.Equal(FipcResult.Invalid, producer.ReceiveAcquire(out _, Fipc.NoWait));
            Assert.Equal(FipcResult.Invalid, consumer.Send(Bytes("x"), Fipc.NoWait));
            Assert.Equal(FipcResult.Invalid, consumer.SendAcquire(1, out _, Fipc.NoWait));
            Assert.Equal(FipcResult.Invalid, producer.WaitForMembers(0, out _, Fipc.NoWait)); // a publisher's call
            Assert.Equal(FipcResult.TooLarge, producer.Send(new byte[producer.MaxPiece + 1], Fipc.NoWait));
            Assert.Equal(FipcResult.AddrInUse,
                Assert.Throws<FipcException>(() => FipcQueue.Create(_name, Capacity, 1, 1)).Result);

            // A producer's Dispose doesn't end the queue: the consumer waits for more
            Assert.Equal(FipcResult.Ok, producer.Send(Bytes("last"), TimeoutMs));
            producer.Dispose();
            Assert.Equal(FipcResult.Ok, consumer.Receive(out byte[] last, TimeoutMs));
            Assert.Equal("last", Encoding.UTF8.GetString(last));
            Assert.Equal(FipcResult.Timeout, consumer.Receive(out _, Fipc.NoWait));
        });

        /// <summary>
        /// The queue's count of members, per role and of both, a wait for them on another thread, Invalid over each
        /// maximum, and its Cancel: final, and the queue goes on.
        /// </summary>
        [Fact(Timeout = 30000)]
        public Task WaitForMembers_AndCancel() => Run(() =>
        {
            using FipcQueue queue = Create(_name, 2, 3);
            Assert.Equal(FipcResult.Ok, queue.WaitForMembers(null, 0, out int count, Fipc.NoWait));
            Assert.Equal(0, count);
            Assert.Equal(FipcResult.Ok, queue.WaitForMembers(FipcQueueRole.Producer, 0, out count, Fipc.NoWait));
            Assert.Equal(0, count);
            Assert.Equal(FipcResult.Timeout, queue.WaitForMembers(null, 1, out count, Fipc.NoWait));
            Assert.Equal(0, count);
            Assert.Throws<ArgumentOutOfRangeException>(() => queue.WaitForMembers(null, -1, out _, 0));

            // Another thread waits for a consumer while one joins
            Task<(FipcResult, int)> waiting = Task.Run(() =>
                (queue.WaitForMembers(FipcQueueRole.Consumer, 1, out int n, TimeoutMs), n));
            using (FipcConnection consumer = Join(_name, FipcQueueRole.Consumer))
            using (FipcConnection producer = Join(_name, FipcQueueRole.Producer))
            {
                (FipcResult waited, int waitedCount) = waiting.GetAwaiter().GetResult();
                Assert.Equal(FipcResult.Ok, waited);
                Assert.True(waitedCount >= 1, $"counted {waitedCount} consumers");
                Assert.Equal(FipcResult.Ok, queue.WaitForMembers(null, 2, out count, TimeoutMs));
                Assert.Equal(2, count);
                foreach (FipcQueueRole role in new[] { FipcQueueRole.Producer, FipcQueueRole.Consumer })
                {
                    Assert.Equal(FipcResult.Ok, queue.WaitForMembers(role, 0, out count, Fipc.NoWait));
                    Assert.Equal(1, count);
                }
                Assert.Equal(FipcResult.Timeout, queue.WaitForMembers(FipcQueueRole.Producer, 2, out count, 50));
                Assert.Equal(1, count);

                // Over the role's maximum (maxProducers, maxConsumers, their sum for both), or another role
                foreach ((FipcQueueRole? role, int atLeast) in new (FipcQueueRole?, int)[]
                    { (FipcQueueRole.Producer, 3), (FipcQueueRole.Consumer, 4), (null, 6), ((FipcQueueRole)1, 1) })
                    Assert.Equal(FipcResult.Invalid, queue.WaitForMembers(role, atLeast, out _, Fipc.NoWait));

                // A cancel wakes a wait, with the count, and later waits return Cancelled
                waiting = Task.Run(() => (queue.WaitForMembers(FipcQueueRole.Consumer, 2, out int n, TimeoutMs), n));
                Thread.Sleep(100);
                queue.Cancel();
                (waited, waitedCount) = waiting.GetAwaiter().GetResult();
                Assert.Equal(FipcResult.Cancelled, waited);
                Assert.Equal(1, waitedCount);
                Assert.Equal(FipcResult.Cancelled, queue.WaitForMembers(null, 3, out count, TimeoutMs));
                Assert.Equal(2, count);

                // The queue goes on after its cancel: members join, and a message goes through
                using FipcConnection secondProducer = Join(_name, FipcQueueRole.Producer);
                Assert.Equal(FipcResult.Ok, secondProducer.Send(Bytes("after the cancel"), TimeoutMs));
                Assert.Equal(FipcResult.Ok, consumer.Receive(out byte[] message, TimeoutMs));
                Assert.Equal("after the cancel", Encoding.UTF8.GetString(message));
            }
        });

        /// <summary>
        /// The queue's Dispose ends it: the producer's sends return Disconnected; the consumer receives what is left,
        /// then Disconnected; the name is free at once.
        /// </summary>
        [Fact(Timeout = 30000)]
        public Task QueuesDispose_EndsTheQueue() => Run(() =>
        {
            FipcQueue queue = Create(_name, 1, 1);
            using FipcConnection producer = Join(_name, FipcQueueRole.Producer);
            using FipcConnection consumer = Join(_name, FipcQueueRole.Consumer);
            Assert.Equal(FipcResult.Ok, producer.Send(Bytes("left"), TimeoutMs));
            queue.Dispose();
            queue.Dispose(); // a second Dispose does nothing
            Assert.Equal(FipcResult.Disconnected, producer.Send(Bytes("after"), TimeoutMs));
            Assert.Equal(FipcResult.Ok, consumer.Receive(out byte[] left, TimeoutMs));
            Assert.Equal("left", Encoding.UTF8.GetString(left));
            Assert.Equal(FipcResult.Disconnected, consumer.Receive(out _, TimeoutMs));
            Assert.Throws<ObjectDisposedException>(() => queue.WaitForMembers(null, 0, out _, Fipc.NoWait));
            queue.Cancel(); // a no-op once disposed
            Create(_name, 1, 1).Dispose();
        });

        /// <summary>
        /// Creates a queue and joins its producer; the queue's object is unreachable once this returns.
        /// <paramref name="native"/> is the queue's native handle, for the test to close, and
        /// <paramref name="dropped"/> a weak reference to its object.
        /// </summary>
        [MethodImpl(MethodImplOptions.NoInlining)]
        private static FipcConnection DroppedQueue(string name, out IntPtr native, out WeakReference dropped)
        {
            FipcQueue queue = Create(name, 1, 1);
            var handle = (SafeHandle)typeof(FipcQueue).GetField("_handle", BindingFlags.NonPublic | BindingFlags.Instance)!
                .GetValue(queue)!;
            native = handle.DangerousGetHandle();
            dropped = new WeakReference(queue);
            return Join(name, FipcQueueRole.Producer);
        }

        /// <summary>
        /// The queue has no finalizer: one whose object was dropped, with only a joined producer kept, goes on through
        /// the garbage collector's runs and holds its name, until its close (here through the native handle) or the end
        /// of the process.
        /// </summary>
        [Fact(Timeout = 30000)]
        public Task QueueOutlivesItsDroppedObject() => Run(() =>
        {
            using FipcConnection producer = DroppedQueue(_name, out IntPtr native, out WeakReference dropped);
            for (int i = 0; i < 2; i++)
            {
                GC.Collect();
                GC.WaitForPendingFinalizers();
            }
            Assert.False(dropped.IsAlive, "the queue's object was not collected");
            Assert.Equal(FipcResult.Ok, producer.Send(Bytes("still here"), TimeoutMs));
            using FipcConnection consumer = Join(_name, FipcQueueRole.Consumer);
            Assert.Equal(FipcResult.Ok, consumer.Receive(out byte[] message, TimeoutMs));
            Assert.Equal("still here", Encoding.UTF8.GetString(message));
            Assert.Equal(FipcResult.AddrInUse,
                Assert.Throws<FipcException>(() => FipcQueue.Create(_name, Capacity, 1, 1)).Result);
            Fipc.QueueClose(native);
            Assert.Equal(FipcResult.Disconnected, consumer.Receive(out _, TimeoutMs));
        });

        /// <summary>
        /// Create refuses what is out of range; a broadcast's roles are refused on a queue, a queue's on a broadcast,
        /// and a role of neither.
        /// </summary>
        [Fact(Timeout = 30000)]
        public Task Create_RefusesWhatIsOutOfRange_AndJoinersOfTheOtherKind() => Run(() =>
        {
            foreach ((int capacity, int producers, int consumers) in new[] { (1000, 1, 1), (Capacity, 0, 1), (Capacity, 65, 1), (Capacity, 1, 0), (Capacity, 1, 65) })
            {
                Assert.Equal(FipcResult.Invalid, Assert.Throws<FipcException>(
                    () => FipcQueue.Create(_name, capacity, producers, consumers)).Result);
            }
            Assert.Throws<ArgumentOutOfRangeException>(() => FipcQueue.Create(_name, Capacity, -1, 1));

            using (Create(_name, 1, 1))
            {
                foreach (FipcSubscriberRole role in new[] { FipcSubscriberRole.Lossy, FipcSubscriberRole.Gated })
                {
                    Assert.Equal(FipcResult.Invalid,
                        Assert.Throws<FipcException>(() => FipcBroadcast.Join(_name, role, TimeoutMs)).Result);
                    Assert.Equal(FipcResult.Invalid, Assert.Throws<FipcException>(
                        () => FipcQueue.Join(_name, (FipcQueueRole)role, TimeoutMs)).Result);
                }
                Assert.Equal(FipcResult.Invalid, Assert.Throws<FipcException>(
                    () => FipcQueue.Join(_name, (FipcQueueRole)5, Fipc.NoWait)).Result);
            }
            using (FipcBroadcast.Create(_name, Capacity, 1, 0))
            {
                foreach (FipcQueueRole role in new[] { FipcQueueRole.Producer, FipcQueueRole.Consumer })
                {
                    Assert.Equal(FipcResult.Invalid,
                        Assert.Throws<FipcException>(() => FipcQueue.Join(_name, role, TimeoutMs)).Result);
                }
            }
        });

        /// <summary>The examples' consumer: joins, receives until Disconnected.</summary>
        private static List<string> Consume(string name, int timeoutMs)
        {
            using FipcConnection consumer = Join(name, FipcQueueRole.Consumer, timeoutMs);
            return ReceiveUntilTheEnd(consumer, 0);
        }

        /// <summary>
        /// The examples' exchange: the producer creates the queue, joins it, sends job 1 to job 3, waits for a consumer
        /// and disposes its producer and the queue; the consumer receives the three jobs, then Disconnected. Consumer
        /// first: its thread calls Join before the queue is created, 200 ms later (nothing outside the Join shows that
        /// it waits, so the pause makes that likely, not certain). Consumer last: it joins after the jobs were sent.
        /// </summary>
        [Fact(Timeout = 30000)]
        public Task ExamplesExchange_InEitherOrder() => Run(() =>
        {
            foreach (bool consumerFirst in new[] { true, false })
            {
                string name = $"{_name}_{consumerFirst}";
                using var joining = new ManualResetEventSlim();
                Func<List<string>> consume = () =>
                {
                    joining.Set();
                    return Consume(name, TimeoutMs);
                };
                Task<List<string>>? consumed = null;
                if (consumerFirst)
                {
                    consumed = Task.Run(consume);
                    Assert.True(joining.Wait(TimeoutMs));
                    Thread.Sleep(200);
                }
                using (FipcQueue queue = Create(name, 1, 1))
                {
                    using (FipcConnection producer = Join(name, FipcQueueRole.Producer))
                    {
                        for (int i = 1; i <= 3; i++)
                            Assert.Equal(FipcResult.Ok, producer.Send(Bytes(Job(i)), Fipc.Forever));
                        if (!consumerFirst)
                            consumed = Task.Run(consume);
                        Assert.Equal(FipcResult.Ok, queue.WaitForMembers(FipcQueueRole.Consumer, 1, out _, TimeoutMs));
                    }
                }
                Assert.Equal(new[] { Job(1), Job(2), Job(3) }, consumed!.GetAwaiter().GetResult());
            }
        });

        /// <summary>
        /// A message longer than the array receive's 4 KiB buffer arrives whole; one too long for a span takes nothing.
        /// </summary>
        [Fact(Timeout = 30000)]
        public Task LongMessage_ReceivedIntoAnArrayOfItsLength() => Run(() =>
        {
            using FipcQueue queue = Create(_name, 1, 1, capacity: 1 << 16);
            using FipcConnection producer = Join(_name, FipcQueueRole.Producer);
            using FipcConnection consumer = Join(_name, FipcQueueRole.Consumer);
            byte[] longMessage = Enumerable.Range(0, 16384).Select(i => (byte)i).ToArray();
            Assert.Equal(FipcResult.Ok, producer.Send(longMessage, TimeoutMs));
            Assert.Equal(FipcResult.TooLarge, consumer.Receive(new byte[4096], out int needed, TimeoutMs));
            Assert.Equal(longMessage.Length, needed);
            Assert.Equal(FipcResult.Ok, consumer.Receive(out byte[] message, TimeoutMs));
            Assert.Equal(longMessage, message);
        });

        /// <summary>
        /// Two consumers race for messages up to 12 KiB, longer than the 4 KiB buffer the array receive tries first: a
        /// receive that finds a longer message takes it into an array of its length, or, when the other consumer took
        /// it first, the next message, trimmed or grown to its length. Every message arrives once, whole.
        /// </summary>
        [Fact(Timeout = 60000)]
        public Task TwoConsumers_RaceForLongMessages_EachArrivesOnceWhole() => Run(() =>
        {
            const int Count = 3000;
            static byte[] Indexed(int i)
            {
                var message = new byte[(i % 3) switch { 0 => 100, 1 => 5000, _ => 12000 }];
                BitConverter.TryWriteBytes(message, i);
                for (int j = 4; j < message.Length; j++)
                    message[j] = (byte)(i + j);
                return message;
            }
            static List<byte[]> TakeAll(FipcConnection consumer)
            {
                var received = new List<byte[]>();
                for (;;)
                {
                    FipcResult result = consumer.Receive(out byte[] message, TimeoutMs);
                    if (result == FipcResult.Disconnected)
                        return received;
                    Assert.Equal(FipcResult.Ok, result);
                    received.Add(message);
                }
            }

            FipcQueue queue = Create(_name, 1, 2, capacity: 1 << 16);
            List<FipcConnection> consumers = new[] { 0, 1 }.Select(_ => Join(_name, FipcQueueRole.Consumer)).ToList();
            Assert.Equal(1 << 14, consumers[0].MaxPiece);
            Task<List<byte[]>>[] taken = consumers.Select(consumer => Task.Run(() => TakeAll(consumer))).ToArray();
            using (FipcConnection producer = Join(_name, FipcQueueRole.Producer))
            {
                for (int i = 0; i < Count; i++)
                    Assert.Equal(FipcResult.Ok, producer.Send(Indexed(i), TimeoutMs));
            }
            queue.Dispose(); // ends the queue: the consumers take what is left, then Disconnected
            List<byte[]> everything = taken.SelectMany(task => task.GetAwaiter().GetResult()).ToList();
            foreach (FipcConnection consumer in consumers)
                consumer.Dispose();

            Assert.Equal(Count, everything.Count);
            var seen = new bool[Count];
            foreach (byte[] message in everything)
            {
                int i = BitConverter.ToInt32(message, 0);
                Assert.InRange(i, 0, Count - 1);
                Assert.False(seen[i], $"message {i} arrived twice");
                seen[i] = true;
                Assert.Equal(Indexed(i), message);
            }
        });

        /// <summary>
        /// The examples' producer and consumer (examples/csharp), each in a process of its own, in both start orders:
        /// the consumer prints job 1 to job 3, in any order, and both exit with 0. The examples use the name my_queue.
        /// </summary>
        [Fact(Timeout = 60000)]
        public Task ExampleProcesses_ProducerAndConsumer_InEitherOrder() =>
            Run(() => ExamplePairs.Run("Producer", "Consumer", new[] { Job(1), Job(2), Job(3) }, anyOrder: true));

        /// <summary>
        /// The raw layer: Fipc.QueueCreate, a producer and a consumer joined from this thread, the count, the end at
        /// QueueClose.
        /// </summary>
        [Fact(Timeout = 30000)]
        public Task RawLayer_CreateJoinAndTheEnd() => Run(() =>
        {
            Assert.Equal(FipcResult.Ok, Fipc.QueueCreate(_name, Capacity, 1, 2, out IntPtr queue));
            Assert.Equal(FipcResult.Ok, Fipc.QueueJoin(_name, FipcQueueRole.Producer, out IntPtr producer, TimeoutMs));
            Assert.Equal(FipcResult.Ok, Fipc.QueueJoin(_name, FipcQueueRole.Consumer, out IntPtr consumer, TimeoutMs));
            Assert.Equal(FipcResult.Ok, Fipc.QueueWaitForMembers(queue, default, 2, out uint count, TimeoutMs));
            Assert.Equal(2u, count);
            Assert.Equal((nuint)(Capacity / 4), Fipc.MaxPiece(consumer));
            Assert.Equal(FipcResult.AddrInUse, Fipc.QueueCreate(_name, Capacity, 1, 1, out _));

            Assert.Equal(FipcResult.Ok, Fipc.Send(producer, Bytes("raw"), TimeoutMs));
            Fipc.Close(producer);
            Fipc.QueueCancel(queue);
            Assert.Equal(FipcResult.Cancelled, Fipc.QueueWaitForMembers(queue, FipcQueueRole.Consumer, 2, out count, TimeoutMs));
            Fipc.QueueClose(queue);
            var buffer = new byte[64];
            Assert.Equal(FipcResult.Ok, Fipc.Recv(consumer, buffer, out int length, TimeoutMs));
            Assert.Equal("raw", Encoding.UTF8.GetString(buffer, 0, length));
            Assert.Equal(FipcResult.Disconnected, Fipc.Recv(consumer, buffer, out _, TimeoutMs));
            Fipc.Close(consumer);
        });

        // === Other processes: the Python binding's peer ===

        private const int PeerCount = 300;
        private const int PeerTimeoutMs = 30_000;

        private Process StartPeer(string python, params string[] args) =>
            BroadcastTests.StartPythonPeer(_output, python, "queue_peer.py", args);

        /// <summary>A Python producer that owns the queue, in another process, and a consumer here.</summary>
        [Fact(Timeout = 60000)]
        public Task PythonProducer_SendsToThisConsumer() => Run(() =>
        {
            string? python = BroadcastTests.Python(_output);
            if (python == null)
                return;
            using Process peer = StartPeer(python, "producer", _name, PeerCount.ToString());
            List<string> received = Consume(_name, PeerTimeoutMs); // the peer's interpreter takes a moment to create it
            CheckJobs(received, 1, PeerCount);
            BroadcastTests.ExpectSuccess(peer, "producer");
        });

        /// <summary>A producer that owns the queue here, and a Python consumer in another process.</summary>
        [Fact(Timeout = 60000)]
        public Task PythonConsumer_ReceivesFromThisProducer() => Run(() =>
        {
            string? python = BroadcastTests.Python(_output);
            if (python == null)
                return;
            using Process peer = StartPeer(python, "consumer", _name, PeerCount.ToString());
            using (FipcQueue queue = Create(_name, 8, 8))
            {
                using (FipcConnection producer = Join(_name, FipcQueueRole.Producer))
                {
                    for (int i = 1; i <= PeerCount; i++)
                        Assert.Equal(FipcResult.Ok, producer.Send(Bytes(Job(i)), PeerTimeoutMs));
                    Assert.Equal(FipcResult.Ok, queue.WaitForMembers(FipcQueueRole.Consumer, 1, out _, PeerTimeoutMs));
                }
            }
            BroadcastTests.ExpectSuccess(peer, "consumer");
        });

        /// <summary>A Python owner's consumer in another process, and two producers here that join its queue.</summary>
        [Fact(Timeout = 60000)]
        public Task PythonOwnersConsumer_TakesFromTheseProducers() => Run(() =>
        {
            string? python = BroadcastTests.Python(_output);
            if (python == null)
                return;
            using Process peer = StartPeer(python, "owner-consumer", _name, PeerCount.ToString(), "2");
            Task[] producing = Enumerable.Range(0, 2).Select(_ => Task.Run(() =>
            {
                using FipcConnection producer = Join(_name, FipcQueueRole.Producer, PeerTimeoutMs);
                for (int i = 1; i <= PeerCount; i++)
                    Assert.Equal(FipcResult.Ok, producer.Send(Bytes(Job(i)), PeerTimeoutMs));
            })).ToArray();
            foreach (Task task in producing)
                task.GetAwaiter().GetResult();
            BroadcastTests.ExpectSuccess(peer, "owner's consumer");
        });

        /// <summary>
        /// Two Python producers in other processes that join the queue, and the owner's consumer here.
        /// </summary>
        [Fact(Timeout = 60000)]
        public Task PythonJoinedProducers_SendToThisOwnersConsumer() => Run(() =>
        {
            string? python = BroadcastTests.Python(_output);
            if (python == null)
                return;
            using FipcQueue queue = Create(_name, 8, 8);
            using FipcConnection consumer = Join(_name, FipcQueueRole.Consumer);
            using Process first = StartPeer(python, "joined-producer", _name, PeerCount.ToString());
            using Process second = StartPeer(python, "joined-producer", _name, PeerCount.ToString());
            var received = new List<string>();
            for (int i = 0; i < 2 * PeerCount; i++)
            {
                Assert.Equal(FipcResult.Ok, consumer.Receive(out byte[] message, PeerTimeoutMs));
                received.Add(Encoding.UTF8.GetString(message));
            }
            CheckJobs(received, 2, PeerCount);
            BroadcastTests.ExpectSuccess(first, "producer");
            BroadcastTests.ExpectSuccess(second, "producer");
        });
    }
}
