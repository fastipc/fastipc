using System;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

#nullable enable

namespace FastIpc.IntegrationTests
{
    public static partial class Snippets
    {
        /// <summary>
        /// The page's patterns, run in pairs. They use the page's fixed names ("orders", "game", ...), so they run one
        /// at a time (one collection); no other test uses those names.
        /// </summary>
        [Collection("Snippets")]
        public sealed class PatternTests
        {
            private const int TimeoutMs = 10_000;

            public PatternTests()
            {
                RawLayerTests.SetLibraryPath();
                Seen.Clear();
            }

            private static string[] SeenWith(string prefix) => Seen.Where(s => s.StartsWith(prefix)).ToArray();

            private static void WaitUntil(Func<bool> condition, Action? meanwhile = null)
            {
                var sw = Stopwatch.StartNew();
                while (!condition())
                {
                    Assert.True(sw.ElapsedMilliseconds < TimeoutMs, "the condition didn't come within 10 s");
                    meanwhile?.Invoke();
                    Thread.Sleep(1);
                }
            }

            /// <summary>
            /// The server serves one client after another (a message longer than its buffer too), and its token stops
            /// it while a client is connected: the waiting receive returns Cancelled, then Accept throws Cancelled.
            /// </summary>
            [Fact(Timeout = 60000)]
            public Task Server_ServesOneClientAfterAnother_UntilCancelled() => Task.Run(() =>
            {
                using var cts = new CancellationTokenSource();
                Task server = Task.Run(() => RunServer(cts.Token));
                byte[] large = Enumerable.Repeat((byte)'a', 100_000).ToArray();
                for (int i = 0; i < 2; i++)
                {
                    using var conn = FipcStream.Connect("orders", TimeoutMs);
                    Assert.Equal(FipcResult.Ok, conn.Send("ping"u8));
                    Assert.Equal(FipcResult.Ok, conn.Receive(out byte[] reply, TimeoutMs));
                    Assert.Equal("PING", Text(reply));
                    Assert.Equal(FipcResult.Ok, conn.Send(large));
                    Assert.Equal(FipcResult.Ok, conn.Receive(out reply, TimeoutMs));
                    Assert.Equal(Enumerable.Repeat((byte)'A', 100_000).ToArray(), reply);
                }
                using (FipcStream.Connect("orders", TimeoutMs))
                {
                    Thread.Sleep(200); // the server waits in its receive
                    cts.Cancel();
                    Assert.True(server.Wait(TimeoutMs), "the server didn't stop within 10 s of the cancel");
                }
            });

            /// <summary>The client returns 2 when no server comes within 5 s, and 0 after a round trip with one.</summary>
            [Fact(Timeout = 60000)]
            public Task Client_ReturnsTwoWithoutServer_ThenPings() => Task.Run(() =>
            {
                Assert.Equal(2, Ping());
                using var cts = new CancellationTokenSource();
                Task server = Task.Run(() => RunServer(cts.Token));
                Assert.Equal(0, Ping());
                cts.Cancel();
                Assert.True(server.Wait(TimeoutMs));
            });

            /// <summary>
            /// The frame loop never waits: it lets each client in by polling, takes every message that came (one longer
            /// than its buffer too), and frees the slot when the client leaves, for the next.
            /// </summary>
            [Fact(Timeout = 60000)]
            public Task FrameLoop_PollsAndNeverWaits() => Task.Run(() =>
            {
                using var endpoint = new GameEndpoint();
                var sw = Stopwatch.StartNew();
                for (int i = 0; i < 100; i++)
                    endpoint.Tick();
                Assert.True(sw.ElapsedMilliseconds < 1000, $"100 ticks without a client took {sw.ElapsedMilliseconds} ms");
                for (int round = 0; round < 2; round++)
                {
                    Seen.Clear();
                    var connecting = Task.Run(() => FipcStream.Connect("game", TimeoutMs));
                    WaitUntil(() => connecting.IsCompleted, endpoint.Tick);
                    using (FipcConnection conn = connecting.Result)
                    {
                        Assert.Equal(FipcResult.Ok, conn.Send(new byte[10]));
                        Assert.Equal(FipcResult.Ok, conn.Send(new byte[200_000]));
                        WaitUntil(() => Seen.Count == 2, endpoint.Tick);
                        Assert.Equal(new[] { "10 bytes", "200000 bytes" }, Seen.ToArray());
                    }
                }
            });

            /// <summary>The publisher waits for its two subscribers; the lossy one shows every tick, the gated one audits every one in order.</summary>
            [Fact(Timeout = 60000)]
            public Task Broadcast_PublisherLossyAndGated() => Task.Run(() =>
            {
                Task lossy = Task.Run(WatchTicks);
                Task gated = Task.Run(AuditTicks);
                string[] ticks = Enumerable.Range(1, 100).Select(i => $"tick {i}").ToArray();
                Publish(ticks.Select(t => Encoding.UTF8.GetBytes(t)));
                Assert.True(lossy.Wait(TimeoutMs) && gated.Wait(TimeoutMs), "a subscriber didn't see the end");
                Assert.Equal(ticks.Select(t => $"audit {t}"), SeenWith("audit "));
                Assert.Equal(ticks.Select(t => $"show {t}"), SeenWith("show "));
                Assert.Empty(SeenWith("missed "));
            });

            /// <summary>
            /// The consumer takes every job of the producer's batch once, then sees the queue's end; a consumer stops
            /// when its token is cancelled.
            /// </summary>
            [Fact(Timeout = 60000)]
            public Task Queue_ProducerAndConsumers() => Task.Run(() =>
            {
                using var cts = new CancellationTokenSource();
                // One consumer: Produce waits for one, and a second that joined after the queue ended would wait for ever
                Task consumer = Task.Run(() => Consume(cts.Token));
                string[] jobs = Enumerable.Range(1, 50).Select(i => $"{i}").ToArray();
                Produce(jobs.Select(j => Encoding.UTF8.GetBytes(j)).ToArray());
                Assert.True(consumer.Wait(TimeoutMs), "the consumer didn't see the queue's end");
                Assert.Equal(jobs.Select(j => $"job {j}").Order(), SeenWith("job ").Order());

                using var queue = FipcQueue.Create("jobs", 1 << 16, 1, 1);
                Task stopped = Task.Run(() => Consume(cts.Token));
                Assert.Equal(FipcResult.Ok, queue.WaitForMembers(FipcQueueRole.Consumer, 1, out _, TimeoutMs));
                cts.Cancel();
                Assert.True(stopped.Wait(TimeoutMs), "the consumer didn't stop within 10 s of the cancel");
            });

            /// <summary>
            /// The pool's servers answer the client's quotes; the token ends the service, the servers return, and the
            /// client's next quote is null (Disconnected).
            /// </summary>
            [Fact(Timeout = 60000)]
            public Task Service_PoolAndClient() => Task.Run(() =>
            {
                using var cts = new CancellationTokenSource();
                Task owner = Task.Run(() => RunPricing(cts.Token));
                using var client = FipcService.Join("pricing", FipcServiceRole.Client, timeoutMs: TimeoutMs);
                Assert.Equal(3.75m, Quote(client, "ABC"));
                Assert.Equal(5m, Quote(client, "ABCD"));
                Assert.Throws<ArgumentException>(() => Quote(client, ""));
                cts.Cancel();
                Assert.True(owner.Wait(TimeoutMs), "the servers didn't return within 10 s of the cancel");
                Assert.Null(Quote(client, "ABC"));
            });

            /// <summary>
            /// The page's small blocks: the echo pair, a frame sent in place or, over MaxPiece, in pieces, a message
            /// received in place, a batch each way, the raw calls.
            /// </summary>
            [Fact(Timeout = 60000)]
            public Task SmallBlocks_Run() => Task.Run(() =>
            {
                Task echo = Task.Run(EchoServer);
                EchoClient();
                Assert.True(echo.Wait(TimeoutMs));
                Assert.Equal(new[] { "hello" }, Seen.ToArray());
                Seen.Clear();

                using (var listener = FipcStream.Create("my_channel", 1 << 16))
                {
                    var connecting = Task.Run(() => FipcStream.Connect("my_channel", TimeoutMs));
                    using FipcConnection server = listener.Accept(TimeoutMs);
                    using FipcConnection client = connecting.GetAwaiter().GetResult();

                    Assert.Equal(FipcResult.Ok, SendFrame(client, new Frame(Encoding.UTF8.GetBytes("in place"))));
                    ReceiveInPlace(server);
                    Assert.Equal(new[] { "in place" }, Seen.ToArray());
                    var receiving = Task.Run(() => (server.Receive(out byte[] whole, TimeoutMs), whole));
                    Assert.Equal(FipcResult.Ok, SendFrame(client, new Frame(new byte[100_000])));
                    var (received, message) = receiving.GetAwaiter().GetResult();
                    Assert.Equal(FipcResult.Ok, received);
                    Assert.Equal(100_000, message.Length);
                    Seen.Clear();

                    BatchEachWay(client, server);
                    Assert.Equal(new[] { "one", "two", "sent 2" }, Seen.ToArray());
                }

                using (var listener = FipcStream.Create("my_channel", 1 << 16))
                {
                    var accepting = Task.Run(() => listener.Accept(TimeoutMs));
                    RawCalls();
                    using FipcConnection server = accepting.GetAwaiter().GetResult();
                    Assert.Equal(FipcResult.Ok, server.Receive(out byte[] hello, TimeoutMs));
                    Assert.Equal("hello", Text(hello));
                }
            });
        }
    }
}
