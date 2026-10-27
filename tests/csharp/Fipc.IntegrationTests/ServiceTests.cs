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
    /// The service through the binding: many clients and one server, a pool of server threads that hold requests at
    /// once, each client's replies in the order of its requests, Abandoned when a server disposes its connection or its
    /// process ends holding a request, the calls each role refuses, the zero-copy calls on both roles, timeouts and
    /// cancels (a Call that gives up and the next one, which drops the late reply; a Call's deadline and its checks of
    /// the request; a Call after a Cancel, which sends nothing), a client that sends on one thread and receives on
    /// another, the owner's WaitForMembers and Cancel, the end at the service's Dispose (and Call after it), a service
    /// that outlives its dropped object (it has no finalizer), the examples' exchange, in this process and as the
    /// example projects' two processes, the raw layer, and the Python binding's clients and servers in other processes
    /// (bindings/python/tests/service_peer.py, found as BroadcastTests finds its peer).
    /// </summary>
    public class ServiceTests
    {
        private const int Capacity = 4096;
        private const int TimeoutMs = 10_000;
        private static int _counter;
        private readonly ITestOutputHelper _output;
        private readonly string _name;

        public ServiceTests(ITestOutputHelper output)
        {
            _output = output;
            _name = $"fipc_s_{Environment.ProcessId}_{Interlocked.Increment(ref _counter)}_{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}";
            RawLayerTests.SetLibraryPath();
        }

        private static Task Run(Action body) => Task.Run(body);

        private static byte[] Bytes(string text) => Encoding.UTF8.GetBytes(text);

        private static string Text(ReadOnlySpan<byte> bytes) => Encoding.UTF8.GetString(bytes);

        private static byte[] Upper(ReadOnlySpan<byte> bytes) => Bytes(Text(bytes).ToUpperInvariant());

        private static FipcService Create(string name, int maxMessage, int maxClients, int maxServers,
            int capacity = Capacity)
        {
            return FipcService.Create(name, capacity, maxMessage, maxClients, maxServers);
        }

        private static FipcConnection Join(string name, FipcServiceRole role, int timeoutMs = TimeoutMs)
        {
            return FipcService.Join(name, role, timeoutMs);
        }

        /// <summary>The next message (a request or a reply), which must come within the timeout.</summary>
        private static string Receive(FipcConnection conn, int timeoutMs = TimeoutMs)
        {
            Assert.Equal(FipcResult.Ok, conn.Receive(out byte[] message, timeoutMs));
            return Text(message);
        }

        /// <summary>A server's answer to one request: its uppercase.</summary>
        private static void AnswerOne(FipcConnection server)
        {
            Assert.Equal(FipcResult.Ok, server.Receive(out byte[] request, TimeoutMs));
            Assert.Equal(FipcResult.Ok, server.Send(Upper(request), Fipc.NoWait));
        }

        private static string CallOk(FipcConnection client, string request, int timeoutMs = TimeoutMs)
        {
            Assert.Equal(FipcResult.Ok, client.Call(Bytes(request), out byte[] reply, timeoutMs));
            return Text(reply);
        }

        /// <summary>
        /// Answers each request with its uppercase until Disconnected: copied (way 0), into a span (1) or in place (2,
        /// replying in place too); returns how often it answered each request.
        /// </summary>
        private static Dictionary<string, int> AnswerUntilTheEnd(FipcConnection server, int way = 0)
        {
            var answered = new Dictionary<string, int>();
            var buffer = new byte[server.MaxPiece];
            for (;;)
            {
                FipcResult result;
                string request = "";
                if (way == 0)
                {
                    result = server.Receive(out byte[] message, TimeoutMs);
                    if (result == FipcResult.Ok)
                    {
                        request = Text(message);
                        result = server.Send(Upper(message), Fipc.NoWait);
                    }
                }
                else if (way == 1)
                {
                    result = server.Receive(buffer, out int length, TimeoutMs);
                    if (result == FipcResult.Ok)
                    {
                        request = Text(buffer.AsSpan(0, length));
                        result = server.Send(Upper(buffer.AsSpan(0, length)), Fipc.NoWait);
                    }
                }
                else
                {
                    result = server.ReceiveAcquire(out ReadOnlySpan<byte> inPlace, TimeoutMs);
                    if (result == FipcResult.Ok)
                    {
                        request = Text(inPlace);
                        result = server.SendAcquire(inPlace.Length, out Span<byte> room, Fipc.NoWait);
                        if (result == FipcResult.Ok)
                        {
                            Upper(inPlace).CopyTo(room); // the request in place stays valid until the reply's commit
                            result = server.SendCommit(inPlace.Length);
                        }
                    }
                }
                if (result == FipcResult.Disconnected)
                    return answered;
                Assert.Equal(FipcResult.Ok, result);
                answered[request] = answered.GetValueOrDefault(request) + 1;
            }
        }

        /// <summary>
        /// Eight clients on threads of their own call one server: each reply goes to its own client, in order, and the
        /// server answers each request once.
        /// </summary>
        [Fact(Timeout = 60000)]
        public Task ManyClientsAndOneServer_EachReplyToItsOwnClient() => Run(() =>
        {
            const int Count = 300;
            FipcService service = Create(_name, 64, 8, 1);
            List<FipcConnection> clients = Enumerable.Range(0, 8).Select(_ => Join(_name, FipcServiceRole.Client)).ToList();
            using FipcConnection server = Join(_name, FipcServiceRole.Server);
            Assert.Equal(FipcResult.Ok, service.WaitForMembers(null, 9, out int count, TimeoutMs));
            Assert.Equal(9, count);
            Assert.All(clients.Append(server), member => Assert.Equal(64, member.MaxPiece));

            Task<Dictionary<string, int>> answering = Task.Run(() => AnswerUntilTheEnd(server));
            Task[] asking = clients.Select((client, k) => Task.Run(() =>
            {
                using (client)
                {
                    for (int i = 0; i < Count; i++)
                        Assert.Equal($"CLIENT {k} REQUEST {i}", CallOk(client, $"client {k} request {i}"));
                }
            })).ToArray();
            Assert.True(Task.WaitAll(asking, 60_000), "the clients did not finish within 60 s");
            service.Dispose(); // ends the service: the server's receive returns Disconnected
            Dictionary<string, int> answered = answering.GetAwaiter().GetResult();
            Assert.Equal(8 * Count, answered.Count);
            Assert.All(answered.Values, n => Assert.Equal(1, n));
        });

        /// <summary>
        /// Four servers on threads and four clients that send at once: the servers each take a request and wait at a
        /// barrier before replying, so all four hold one at the same time; then they answer the rest (copied, into a
        /// span, in place), each request once, and each client receives its replies in the order of its requests (each
        /// sends ten before it receives).
        /// </summary>
        [Fact(Timeout = 60000)]
        public Task APoolOfServerThreads_HoldsRequestsAtOnce_AndAnswersEachOnce() => Run(() =>
        {
            const int Count = 100;
            FipcService service = Create(_name, 64, 4, 4, capacity: 1 << 14);
            List<FipcConnection> servers = Enumerable.Range(0, 4).Select(_ => Join(_name, FipcServiceRole.Server)).ToList();
            List<FipcConnection> clients = Enumerable.Range(0, 4).Select(_ => Join(_name, FipcServiceRole.Client)).ToList();
            Assert.Equal(FipcResult.Ok, service.WaitForMembers(FipcServiceRole.Server, 4, out _, TimeoutMs));
            using var barrier = new Barrier(4);

            Task<Dictionary<string, int>>[] serving = servers.Select((server, k) => Task.Run(() =>
            {
                Assert.Equal(FipcResult.Ok, server.Receive(out byte[] request, TimeoutMs));
                Assert.True(barrier.SignalAndWait(TimeoutMs), "the servers did not all hold a request at once");
                Assert.Equal(FipcResult.Ok, server.Send(Upper(request), Fipc.NoWait));
                Dictionary<string, int> answered = AnswerUntilTheEnd(server, k % 3);
                answered[Text(request)] = answered.GetValueOrDefault(Text(request)) + 1;
                return answered;
            })).ToArray();
            Task[] asking = clients.Select((client, k) => Task.Run(() =>
            {
                using (client)
                {
                    for (int start = 0; start < Count; start += 10) // ten requests in flight, then their ten replies
                    {
                        for (int i = start; i < start + 10; i++)
                            Assert.Equal(FipcResult.Ok, client.Send(Bytes($"r {k} {i}"), TimeoutMs));
                        for (int i = start; i < start + 10; i++)
                            Assert.Equal($"R {k} {i}", Receive(client));
                    }
                }
            })).ToArray();
            Assert.True(Task.WaitAll(asking, 60_000), "the clients did not finish within 60 s");
            service.Dispose();
            List<Dictionary<string, int>> answered = serving.Select(task => task.GetAwaiter().GetResult()).ToList();
            foreach (FipcConnection server in servers)
                server.Dispose();

            Assert.All(answered, a => Assert.NotEmpty(a)); // every server answered
            var all = answered.SelectMany(a => a).ToList();
            Assert.All(all, pair => Assert.Equal(1, pair.Value));
            var expected = Enumerable.Range(0, 4).SelectMany(k => Enumerable.Range(0, Count).Select(i => $"r {k} {i}"));
            Assert.Equal(expected.OrderBy(r => r, StringComparer.Ordinal),
                all.Select(pair => pair.Key).OrderBy(r => r, StringComparer.Ordinal));
        });

        /// <summary>A client's replies come in the order of its requests, whichever server replies first.</summary>
        [Fact(Timeout = 30000)]
        public Task AClientsReplies_ComeInTheOrderOfItsRequests_WhoeverRepliesFirst() => Run(() =>
        {
            using FipcService service = Create(_name, 64, 1, 2);
            using FipcConnection client = Join(_name, FipcServiceRole.Client);
            using FipcConnection slow = Join(_name, FipcServiceRole.Server);
            using FipcConnection fast = Join(_name, FipcServiceRole.Server);
            Assert.Equal(FipcResult.Ok, client.Send(Bytes("first"), TimeoutMs));
            Assert.Equal(FipcResult.Ok, client.Send(Bytes("second"), TimeoutMs));
            Assert.Equal("first", Receive(slow)); // each client's requests are taken in the order it sent them
            Assert.Equal("second", Receive(fast));
            Assert.Equal(FipcResult.Ok, fast.Send(Bytes("SECOND"), Fipc.NoWait));
            Assert.Equal(FipcResult.Timeout, client.Receive(out _, 50)); // the first request's reply comes first
            Assert.Equal(FipcResult.Ok, slow.Send(Bytes("FIRST"), Fipc.NoWait));
            Assert.Equal("FIRST", Receive(client));
            Assert.Equal("SECOND", Receive(client));
            Assert.Equal(FipcResult.Timeout, client.Receive(out _, Fipc.NoWait));
        });

        /// <summary>
        /// A server that disposes its connection holding a request abandons it: the client's receive, or its Call,
        /// returns Abandoned, the request counts as answered, and the next request gets its own reply.
        /// </summary>
        [Fact(Timeout = 30000)]
        public Task AServerThatLeavesHoldingARequest_AbandonsIt() => Run(() =>
        {
            using FipcService service = Create(_name, 64, 1, 2);
            using FipcConnection client = Join(_name, FipcServiceRole.Client);
            using FipcConnection other = Join(_name, FipcServiceRole.Server);
            FipcConnection quitter = Join(_name, FipcServiceRole.Server);
            Assert.Equal(FipcResult.Ok, client.Send(Bytes("one"), TimeoutMs));
            Assert.Equal("one", Receive(quitter));
            quitter.Dispose(); // holding it: the request is answered Abandoned
            var buffer = new byte[64];
            Assert.Equal(FipcResult.Abandoned, client.Receive(buffer, out int length, TimeoutMs));
            Assert.Equal(0, length);
            Assert.Equal(FipcResult.Timeout, client.Receive(out _, Fipc.NoWait)); // answered once: nothing more owed

            // Call returns Abandoned too, and the request counts as answered: the next call gets its own reply
            quitter = Join(_name, FipcServiceRole.Server);
            Task<FipcResult> calling = Task.Run(() => client.Call(Bytes("two"), out _, TimeoutMs));
            Assert.Equal("two", Receive(quitter));
            quitter.Dispose();
            Assert.Equal(FipcResult.Abandoned, calling.GetAwaiter().GetResult());
            Task answering = Task.Run(() => AnswerOne(other));
            Assert.Equal("THREE", CallOk(client, "three"));
            answering.GetAwaiter().GetResult();

            // And in place
            quitter = Join(_name, FipcServiceRole.Server);
            Assert.Equal(FipcResult.Ok, client.Send(Bytes("four"), TimeoutMs));
            Assert.Equal("four", Receive(quitter));
            quitter.Dispose();
            Assert.Equal(FipcResult.Abandoned, client.ReceiveAcquire(out ReadOnlySpan<byte> none, TimeoutMs));
            Assert.Equal(0, none.Length);
            answering = Task.Run(() => AnswerOne(other));
            Assert.Equal("FIVE", CallOk(client, "five"));
            answering.GetAwaiter().GetResult();
        });

        /// <summary>
        /// A server that received a request in place and replied (copied, or in place) closes at its Dispose, with no
        /// ReceiveRelease: the reply ended the request's span. Its place, the service's only one, is free for the next
        /// server.
        /// </summary>
        [Fact(Timeout = 30000)]
        public Task AServerThatRepliedToARequestHeldInPlace_ClosesAtItsDispose() => Run(() =>
        {
            using FipcService service = Create(_name, 64, 1, 1);
            using FipcConnection client = Join(_name, FipcServiceRole.Client);
            for (int way = 0; way < 2; way++)
            {
                using (FipcConnection server = Join(_name, FipcServiceRole.Server))
                {
                    Assert.Equal(FipcResult.Ok, client.Send(Bytes("in place"), TimeoutMs));
                    Assert.Equal(FipcResult.Ok, server.ReceiveAcquire(out ReadOnlySpan<byte> request, TimeoutMs));
                    if (way == 0)
                    {
                        Assert.Equal(FipcResult.Ok, server.Send(Upper(request), Fipc.NoWait));
                    }
                    else
                    {
                        Assert.Equal(FipcResult.Ok, server.SendAcquire(request.Length, out Span<byte> room, Fipc.NoWait));
                        Upper(request).CopyTo(room);
                        Assert.Equal(FipcResult.Ok, server.SendCommit(request.Length));
                    }
                    Assert.Equal("IN PLACE", Receive(client));
                }
                // Disposed: the place is free again (a server that stayed open would keep it, and this join time out)
                using FipcConnection next = Join(_name, FipcServiceRole.Server);
            }
        });

        /// <summary>
        /// A server disposed while it holds a request in place, with no reply: on the thread that received it, the
        /// Dispose ends the span and closes, and the client gets Abandoned; on another thread, the span stays valid
        /// and holds the connection open until it ends: at the receiving thread's ReceiveRelease, and the request is
        /// Abandoned then; or at its reply, which still goes (it never waits), and the connection closes after it.
        /// </summary>
        [Fact(Timeout = 30000)]
        public Task AServerDisposedHoldingARequestInPlace_AbandonsIt() => Run(() =>
        {
            using FipcService service = Create(_name, 64, 1, 1);
            using FipcConnection client = Join(_name, FipcServiceRole.Client);

            // On the receiving thread
            FipcConnection server = Join(_name, FipcServiceRole.Server);
            Assert.Equal(FipcResult.Ok, client.Send(Bytes("one"), TimeoutMs));
            Assert.Equal(FipcResult.Ok, server.ReceiveAcquire(out ReadOnlySpan<byte> request, TimeoutMs));
            Assert.Equal("one", Text(request));
            server.Dispose();
            Assert.Equal(FipcResult.Abandoned, client.Receive(out _, TimeoutMs));

            // On another thread: the close waits for the span's end, a ReceiveRelease
            server = Join(_name, FipcServiceRole.Server); // the place is free again
            Assert.Equal(FipcResult.Ok, client.Send(Bytes("two"), TimeoutMs));
            Assert.Equal(FipcResult.Ok, server.ReceiveAcquire(out request, TimeoutMs));
            FipcConnection disposed = server;
            OnAnotherThread(disposed.Dispose);
            Assert.Equal(FipcResult.Timeout, client.Receive(out _, 100)); // still held, not yet answered
            Assert.Equal("two", Text(request)); // the span is still valid
            server.ReceiveRelease();
            Assert.Equal(FipcResult.Abandoned, client.Receive(out _, TimeoutMs));

            // On another thread, then the reply, which still goes: that ends the span too
            server = Join(_name, FipcServiceRole.Server);
            Assert.Equal(FipcResult.Ok, client.Send(Bytes("three"), TimeoutMs));
            Assert.Equal(FipcResult.Ok, server.ReceiveAcquire(out request, TimeoutMs));
            disposed = server;
            OnAnotherThread(disposed.Dispose);
            Assert.Equal(FipcResult.Ok, server.Send(Upper(request), Fipc.NoWait));
            Assert.Equal("THREE", Receive(client));
            Assert.Throws<ObjectDisposedException>(() => server.Send(Bytes("x"), Fipc.NoWait)); // closed now
            using FipcConnection last = Join(_name, FipcServiceRole.Server); // every server closed: the place is free
        });

        /// <summary>Runs <paramref name="body"/> on a thread of its own (a task's wait could run it on this one) and waits for it.</summary>
        private static void OnAnotherThread(Action body)
        {
            var thread = new Thread(() => body());
            thread.Start();
            thread.Join();
        }

        /// <summary>
        /// Joins a server, takes a request in place and replies to it; the server's object is unreachable once this
        /// returns, undisposed.
        /// </summary>
        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void DroppedServer(string name, FipcConnection client)
        {
            FipcConnection server = Join(name, FipcServiceRole.Server);
            Assert.Equal(FipcResult.Ok, client.Send(Bytes("dropped"), TimeoutMs));
            Assert.Equal(FipcResult.Ok, server.ReceiveAcquire(out ReadOnlySpan<byte> request, TimeoutMs));
            Assert.Equal(FipcResult.Ok, server.Send(Upper(request), Fipc.NoWait));
        }

        /// <summary>
        /// A server that replied to a request held in place and was dropped undisposed is closed by its finalizer: the
        /// reply ended the span's hold on the connection, so the service's only place is free for the next server.
        /// </summary>
        [Fact(Timeout = 30000)]
        public Task AServerDroppedAfterItsReplyToARequestHeldInPlace_IsClosedByItsFinalizer() => Run(() =>
        {
            using FipcService service = Create(_name, 64, 1, 1);
            using FipcConnection client = Join(_name, FipcServiceRole.Client);
            DroppedServer(_name, client);
            Assert.Equal("DROPPED", Receive(client));
            for (int i = 0; i < 2; i++)
            {
                GC.Collect();
                GC.WaitForPendingFinalizers();
            }
            using FipcConnection next = Join(_name, FipcServiceRole.Server);
        });

        /// <summary>
        /// A call that gives up leaves its reply owed; when the server that took its request leaves holding it, the
        /// next call's drop of the owed reply receives Abandoned instead, and the call then gets its own reply.
        /// </summary>
        [Fact(Timeout = 30000)]
        public Task ACallThatGivesUp_ItsRequestAbandoned_TheNextCallGetsItsOwnReply() => Run(() =>
        {
            using FipcService service = Create(_name, 64, 1, 2);
            using FipcConnection client = Join(_name, FipcServiceRole.Client);
            using FipcConnection other = Join(_name, FipcServiceRole.Server);
            FipcConnection quitter = Join(_name, FipcServiceRole.Server);
            Assert.Equal(FipcResult.Timeout, client.Call(Bytes("given up"), out _, 50)); // its reply stays owed
            Assert.Equal("given up", Receive(quitter)); // the only request: this server takes it
            quitter.Dispose(); // holding it: Abandoned is what the next call drops
            Task answering = Task.Run(() => AnswerOne(other));
            Assert.Equal("NEXT", CallOk(client, "next"));
            answering.GetAwaiter().GetResult();
            Assert.Equal(FipcResult.Timeout, client.Receive(out _, Fipc.NoWait)); // nothing more owed
        });

        /// <summary>
        /// Each role's calls that it can't use are Invalid, MaxPiece is maxMessage, a server holds one request at a
        /// time (ReceiveRelease does nothing), and the zero-copy calls work both ways.
        /// </summary>
        [Fact(Timeout = 30000)]
        public Task EachRolesRefusedCalls_ZeroCopy_AndMaxPieceIsMaxMessage() => Run(() =>
        {
            using FipcService service = Create(_name, 1000, 1, 1, capacity: 1 << 14);
            using FipcConnection client = Join(_name, FipcServiceRole.Client); // the owner's thread joins both
            using FipcConnection server = Join(_name, FipcServiceRole.Server);
            Assert.Equal(1000, client.MaxPiece);
            Assert.Equal(1000, server.MaxPiece);
            Assert.Equal(FipcResult.TooLarge, client.Send(new byte[1001], Fipc.NoWait));
            Assert.Equal(FipcResult.Invalid, client.Send(ReadOnlySpan<byte>.Empty, Fipc.NoWait));
            Assert.Equal(FipcResult.Invalid, client.Call(ReadOnlySpan<byte>.Empty, out _, Fipc.NoWait));
            Assert.Equal(FipcResult.TooLarge, client.SendAcquire(1001, out _, Fipc.NoWait));
            Assert.Equal(FipcResult.Invalid, client.SendCommit(1)); // no reservation
            Assert.Equal(FipcResult.Timeout, client.Receive(out _, Fipc.NoWait)); // no reply to come
            Assert.Equal(FipcResult.Invalid, client.WaitForMembers(0, out _, Fipc.NoWait)); // a publisher's call
            client.ReceiveRelease(); // nothing held: does nothing
            Assert.Equal(FipcResult.Invalid, server.Send(Bytes("x"), Fipc.NoWait)); // no request held
            Assert.Equal(FipcResult.Invalid, server.SendAcquire(1, out _, Fipc.NoWait));
            Assert.Equal(FipcResult.Invalid, server.SendCommit(1));
            Assert.Equal(FipcResult.Timeout, server.Receive(out _, Fipc.NoWait));
            Assert.Equal(FipcResult.Invalid, server.WaitForMembers(0, out _, Fipc.NoWait));
            Assert.Equal(FipcResult.Invalid, server.Call(Bytes("x"), out byte[] none, Fipc.NoWait)); // a client's call
            Assert.Empty(none);
            Assert.Equal(FipcResult.AddrInUse,
                Assert.Throws<FipcException>(() => FipcService.Create(_name, Capacity, 64, 1, 1)).Result);
            Assert.Equal(FipcResult.AddrInUse,
                Assert.Throws<FipcException>(() => FipcQueue.Create(_name, Capacity, 1, 1)).Result);

            // A server holds one request: its receives are Invalid until it replies; ReceiveRelease does nothing
            byte[] sevens = Enumerable.Repeat((byte)7, 1000).ToArray();
            Assert.Equal(FipcResult.Ok, client.Send(sevens, TimeoutMs));
            Assert.Equal(FipcResult.TooLarge, server.Receive(new byte[999], out int needed, TimeoutMs)); // takes nothing
            Assert.Equal(1000, needed);
            Assert.Equal(FipcResult.Ok, server.ReceiveAcquire(out ReadOnlySpan<byte> request, TimeoutMs));
            Assert.Equal(1000, request.Length);
            Assert.Equal(FipcResult.Invalid, server.Receive(out _, Fipc.NoWait));
            server.ReceiveRelease(); // does nothing: the request is still held
            Assert.Equal(FipcResult.Invalid, server.ReceiveAcquire(out _, Fipc.NoWait));
            Assert.Equal(FipcResult.TooLarge, server.Send(new byte[1001], Fipc.NoWait));
            Assert.Equal(FipcResult.Ok, server.SendAcquire(1000, out Span<byte> room, Fipc.NoWait));
            room.Fill(3);
            Assert.Equal(FipcResult.Invalid, server.SendCommit(1001));
            Assert.Equal(FipcResult.Ok, server.SendCommit(1000));
            Assert.Equal(FipcResult.Invalid, server.Send(Bytes("x"), Fipc.NoWait)); // nothing held any more
            Assert.Equal(FipcResult.TooLarge, client.Receive(new byte[10], out _, TimeoutMs)); // takes nothing
            Assert.Equal(FipcResult.Ok, client.Receive(out byte[] reply, TimeoutMs));
            Assert.Equal(Enumerable.Repeat((byte)3, 1000), reply);

            // The zero-copy calls the other way: a request committed in place, its reply received in place. Call
            // refuses while the committed request's reply is owed: it is the caller's
            Assert.Equal(FipcResult.Ok, client.SendAcquire(5, out Span<byte> requestRoom, TimeoutMs));
            Bytes("hello").CopyTo(requestRoom);
            Assert.Equal(FipcResult.Ok, client.SendCommit(5));
            Assert.Equal(FipcResult.Invalid, client.Call(Bytes("x"), out _, Fipc.NoWait));
            Assert.Equal("hello", Receive(server));
            Assert.Equal(FipcResult.Ok, server.Send(Bytes("HELLO"), Fipc.NoWait));
            Assert.Equal(FipcResult.Ok, client.ReceiveAcquire(out ReadOnlySpan<byte> inPlace, TimeoutMs));
            Assert.Equal("HELLO", Text(inPlace));
            client.ReceiveRelease();
            Task answering = Task.Run(() => AnswerOne(server));
            Assert.Equal("X", CallOk(client, "x")); // nothing owed any more
            answering.GetAwaiter().GetResult();
            Assert.Equal(FipcResult.Ok, service.WaitForMembers(FipcServiceRole.Client, 0, out int clients, Fipc.NoWait));
            Assert.Equal(FipcResult.Ok, service.WaitForMembers(FipcServiceRole.Server, 0, out int servers, Fipc.NoWait));
            Assert.Equal((1, 1), (clients, servers));
        });

        /// <summary>
        /// Call is a service client's: Invalid on a work queue's members and a broadcast's publisher (as on a stream's
        /// connection and a service's server); after Dispose it throws.
        /// </summary>
        [Fact(Timeout = 30000)]
        public Task Call_IsInvalidOnOtherConnections_AndThrowsAfterDispose() => Run(() =>
        {
            using (FipcQueue queue = Create_Queue(_name))
            {
                foreach (FipcQueueRole role in new[] { FipcQueueRole.Producer, FipcQueueRole.Consumer })
                {
                    using (FipcConnection member = FipcQueue.Join(_name, role, TimeoutMs))
                        Assert.Equal(FipcResult.Invalid, member.Call(Bytes("x"), out _, Fipc.NoWait));
                }
            }
            using (FipcConnection publisher = FipcBroadcast.Create(_name, Capacity, 1, 0))
                Assert.Equal(FipcResult.Invalid, publisher.Call(Bytes("x"), out _, Fipc.NoWait));

            using FipcService service = Create(_name + "_s", 64, 1, 1);
            FipcConnection client = Join(_name + "_s", FipcServiceRole.Client);
            FipcConnection server = Join(_name + "_s", FipcServiceRole.Server);
            client.Dispose();
            server.Dispose();
            Assert.Throws<ObjectDisposedException>(() => client.Call(Bytes("x"), out _, Fipc.NoWait));
            Assert.Throws<ObjectDisposedException>(() => server.Call(Bytes("x"), out _, Fipc.NoWait));
        });

        private static FipcQueue Create_Queue(string name)
        {
            return FipcQueue.Create(name, Capacity, 1, 1);
        }

        /// <summary>
        /// Create refuses what is out of range; the other kinds' roles are refused on a service, and a service's on a
        /// broadcast's or a queue's name.
        /// </summary>
        [Fact(Timeout = 30000)]
        public Task Create_RefusesWhatIsOutOfRange_AndJoinersOfOtherKinds() => Run(() =>
        {
            foreach ((int capacity, int maxMessage, int clients, int servers) in new[]
                { (1000, 64, 1, 1), (Capacity, 0, 1, 1), (Capacity, 1025, 1, 1), (Capacity, 64, 0, 1),
                  (Capacity, 64, 65, 1), (Capacity, 64, 1, 0), (Capacity, 64, 1, 65) })
            {
                Assert.Equal(FipcResult.Invalid, Assert.Throws<FipcException>(
                    () => FipcService.Create(_name, capacity, maxMessage, clients, servers)).Result);
            }
            Assert.Throws<ArgumentOutOfRangeException>(() => FipcService.Create(_name, Capacity, -1, 1, 1));
            Assert.Throws<ArgumentNullException>(() => FipcService.Join(null!, FipcServiceRole.Client, 0));

            using (Create(_name, 64, 1, 1))
            {
                foreach (int role in new[] { 1, 2, 3, 4, 7 }) // no role of a service's
                {
                    Assert.Equal(FipcResult.Invalid, Assert.Throws<FipcException>(
                        () => FipcService.Join(_name, (FipcServiceRole)role, Fipc.NoWait)).Result);
                }
                Assert.Equal(FipcResult.Invalid, Assert.Throws<FipcException>(
                    () => FipcBroadcast.Join(_name, FipcSubscriberRole.Gated, TimeoutMs)).Result);
                Assert.Equal(FipcResult.Invalid, Assert.Throws<FipcException>(
                    () => FipcQueue.Join(_name, FipcQueueRole.Consumer, TimeoutMs)).Result);
                Assert.Equal(FipcResult.Invalid, Assert.Throws<FipcException>(
                    () => FipcQueue.Join(_name, (FipcQueueRole)FipcServiceRole.Client, TimeoutMs)).Result);
            }
            using (Create_Queue(_name))
            {
                foreach (FipcServiceRole role in new[] { FipcServiceRole.Client, FipcServiceRole.Server })
                {
                    Assert.Equal(FipcResult.Invalid,
                        Assert.Throws<FipcException>(() => FipcService.Join(_name, role, TimeoutMs)).Result);
                }
            }
        });

        /// <summary>
        /// A call whose receive timed out leaves its reply to come; the next call receives and drops it (within its
        /// own timeout) before it sends, so it returns the reply to its own request. A call refuses (Invalid) while a
        /// request sent with Send still has its reply to be received.
        /// </summary>
        [Fact(Timeout = 30000)]
        public Task ACallThatGivesUp_LeavesItsReplyOwed_AndTheNextCallDropsIt() => Run(() =>
        {
            using FipcService service = Create(_name, 64, 1, 1);
            using FipcConnection client = Join(_name, FipcServiceRole.Client);
            using FipcConnection server = Join(_name, FipcServiceRole.Server);
            Assert.Equal(FipcResult.Timeout, client.Call(Bytes("late"), out byte[] reply, 50)); // no reply in time
            Assert.Empty(reply);
            Assert.Equal("late", Receive(server));
            Assert.Equal(FipcResult.Ok, server.Send(Bytes("LATE"), Fipc.NoWait)); // owed to the call that gave up

            Task answering = Task.Run(() => AnswerOne(server));
            Assert.Equal("NEXT", CallOk(client, "next")); // not LATE
            answering.GetAwaiter().GetResult();
            Assert.Equal(FipcResult.Timeout, client.Receive(out _, Fipc.NoWait)); // nothing more owed

            // A call whose owed reply hasn't come waits for it within its own timeout, and gives up before it sends
            Assert.Equal(FipcResult.Timeout, client.Call(Bytes("a"), out _, Fipc.NoWait)); // a went; its reply is owed
            Assert.Equal(FipcResult.Timeout, client.Call(Bytes("b"), out _, 50)); // a's reply hasn't come: b never goes
            Assert.Equal("a", Receive(server));
            Assert.Equal(FipcResult.Ok, server.Send(Bytes("A"), Fipc.NoWait));
            Assert.Equal(FipcResult.Timeout, server.Receive(out _, Fipc.NoWait)); // no b
            Task<string> taken = Task.Run(() => Receive(server));
            Assert.Equal(FipcResult.Timeout, client.Call(Bytes("c"), out _, 200)); // drops A, sends c, gives up
            Assert.Equal("c", taken.GetAwaiter().GetResult());
            Assert.Equal(FipcResult.Ok, server.Send(Bytes("C"), Fipc.NoWait)); // owed to the call that gave up
            Assert.Equal("C", Receive(client)); // a receive of the caller's own takes it, in request order
            Assert.Equal(FipcResult.Timeout, client.Receive(out _, Fipc.NoWait));

            // A request of Send's whose reply hasn't been received: Call refuses, and leaves it for Receive
            Assert.Equal(FipcResult.Ok, client.Send(Bytes("mine"), TimeoutMs));
            Assert.Equal(FipcResult.Invalid, client.Call(Bytes("x"), out _, TimeoutMs));
            AnswerOne(server);
            Assert.Equal("MINE", Receive(client));
            answering = Task.Run(() => AnswerOne(server));
            Assert.Equal("X", CallOk(client, "x"));
            answering.GetAwaiter().GetResult();

            // A send that fails is not owed a reply: Call still works after it
            Assert.Equal(FipcResult.TooLarge, client.Send(new byte[65], Fipc.NoWait));
            answering = Task.Run(() => AnswerOne(server));
            Assert.Equal("Y", CallOk(client, "y"));
            answering.GetAwaiter().GetResult();
        });

        /// <summary>
        /// A call that had to drop an owed reply sends only if its timeout has time left: one whose deadline ran out
        /// while it dropped returns Timeout and its request never goes (the internal overload's start, five seconds
        /// ago, spends its 1000 ms before the drop returns). With NoWait a call drops the reply and still sends.
        /// </summary>
        [Fact(Timeout = 30000)]
        public Task ACallWhoseDeadlineRanOutWhileItDroppedAnOwedReply_NeverSends() => Run(() =>
        {
            using FipcService service = Create(_name, 64, 1, 1);
            using FipcConnection client = Join(_name, FipcServiceRole.Client);
            using FipcConnection server = Join(_name, FipcServiceRole.Server);
            Assert.Equal(FipcResult.Timeout, client.Call(Bytes("first"), out _, 50)); // the request went: no reply in time
            Assert.Equal("first", Receive(server));
            Assert.Equal(FipcResult.Ok, server.Send(Bytes("FIRST"), Fipc.NoWait)); // in: the next drop needn't wait
            long past = Stopwatch.GetTimestamp() - 5 * Stopwatch.Frequency;
            Assert.Equal(FipcResult.Timeout, client.Call(Bytes("second"), out byte[] reply, 1000, past));
            Assert.Empty(reply);
            Assert.Equal(FipcResult.Timeout, server.Receive(out _, 50)); // no server saw second
            Task answering = Task.Run(() => AnswerOne(server));
            Assert.Equal("THIRD", CallOk(client, "third")); // nothing of second's is owed
            answering.GetAwaiter().GetResult();

            Assert.Equal(FipcResult.Timeout, client.Call(Bytes("fourth"), out _, 50));
            Assert.Equal("fourth", Receive(server));
            Assert.Equal(FipcResult.Ok, server.Send(Bytes("FOURTH"), Fipc.NoWait));
            Assert.Equal(FipcResult.Timeout, client.Call(Bytes("fifth"), out _, Fipc.NoWait)); // drops FOURTH, sends
            Assert.Equal("fifth", Receive(server));
            Assert.Equal(FipcResult.Ok, server.Send(Bytes("FIFTH"), Fipc.NoWait));
            Assert.Equal("FIFTH", Receive(client)); // a plain receive takes the late reply of the call that gave up
            Assert.Equal(FipcResult.Timeout, client.Receive(out _, Fipc.NoWait));
        });

        /// <summary>
        /// A call checks its request first: Invalid for an empty one and TooLarge over MaxPiece, at once, even while a
        /// reply that never comes is owed (with no limit); that reply stays owed, for the next call to drop.
        /// </summary>
        [Fact(Timeout = 30000)]
        public Task ACallChecksItsRequestFirst_EvenWhileAReplyIsOwed() => Run(() =>
        {
            using FipcService service = Create(_name, 64, 1, 1);
            using FipcConnection client = Join(_name, FipcServiceRole.Client);
            using FipcConnection server = Join(_name, FipcServiceRole.Server);
            Assert.Equal(FipcResult.Timeout, client.Call(Bytes("held"), out _, 50));
            Assert.Equal("held", Receive(server)); // held, without a reply yet
            var watch = Stopwatch.StartNew();
            Assert.Equal(FipcResult.Invalid, client.Call(ReadOnlySpan<byte>.Empty, out _, Fipc.Forever));
            Assert.Equal(FipcResult.TooLarge, client.Call(new byte[client.MaxPiece + 1], out _, Fipc.Forever));
            Assert.True(watch.ElapsedMilliseconds < 1000, $"the checks took {watch.ElapsedMilliseconds} ms");
            Assert.Equal(FipcResult.Ok, server.Send(Bytes("HELD"), Fipc.NoWait)); // still owed: the next call drops it
            Task answering = Task.Run(() => AnswerOne(server));
            Assert.Equal("AFTER", CallOk(client, "after"));
            answering.GetAwaiter().GetResult();
            Assert.Equal(FipcResult.Timeout, client.Receive(out _, Fipc.NoWait)); // nothing more owed
        });

        /// <summary>
        /// A cancel ends a client's wait (a cancelled call leaves its reply owed, and a receive that needn't wait still
        /// works) and a server's.
        /// </summary>
        [Fact(Timeout = 30000)]
        public Task Cancel_EndsAClientsAndAServersWait_AndACancelledCallLeavesItsReplyOwed() => Run(() =>
        {
            using FipcService service = Create(_name, 64, 2, 1);
            using FipcConnection server = Join(_name, FipcServiceRole.Server);
            using (FipcConnection client = Join(_name, FipcServiceRole.Client))
            {
                Task<FipcResult> calling = Task.Run(() => client.Call(Bytes("cancelled"), out _, Fipc.Forever));
                Assert.Equal("cancelled", Receive(server));
                client.Cancel(); // from another thread: the call's wait for its reply returns Cancelled
                Assert.Equal(FipcResult.Cancelled, calling.GetAwaiter().GetResult());
                Assert.Equal(FipcResult.Ok, server.Send(Bytes("CANCELLED"), Fipc.NoWait)); // owed to the cancelled call
                Assert.Equal("CANCELLED", Receive(client, Fipc.NoWait)); // a receive that needn't wait still works
                Assert.Equal(FipcResult.Cancelled, client.Receive(out _, TimeoutMs)); // final
            }

            using (FipcConnection client = Join(_name, FipcServiceRole.Client)) // the server goes on
            {
                Task<FipcResult> waiting = Task.Run(() => server.Receive(out _, Fipc.Forever));
                Thread.Sleep(100);
                server.Cancel();
                Assert.Equal(FipcResult.Cancelled, waiting.GetAwaiter().GetResult());
                Assert.Equal(FipcResult.Ok, client.Send(Bytes("after"), TimeoutMs));
                Assert.Equal("after", Receive(server)); // a request there: the receive needn't wait
                Assert.Equal(FipcResult.Ok, server.Send(Bytes("AFTER"), Fipc.NoWait)); // a reply never waits
                Assert.Equal("AFTER", Receive(client));
                Assert.Equal(FipcResult.Cancelled, server.Receive(out _, TimeoutMs)); // final
            }
        });

        /// <summary>
        /// A call on a cancelled connection returns Cancelled at once, whatever its timeout, after its request's
        /// checks, and sends nothing: the server, which listens, receives no request.
        /// </summary>
        [Fact(Timeout = 30000)]
        public Task Call_AfterCancel_ReturnsCancelledAndSendsNothing() => Run(() =>
        {
            using FipcService service = Create(_name, 64, 1, 1);
            using FipcConnection server = Join(_name, FipcServiceRole.Server);
            using FipcConnection client = Join(_name, FipcServiceRole.Client);
            client.Cancel();
            Assert.Equal(FipcResult.Invalid, client.Call(ReadOnlySpan<byte>.Empty, out _, TimeoutMs)); // checks first
            Assert.Equal(FipcResult.TooLarge, client.Call(new byte[65], out _, TimeoutMs));
            foreach (int timeoutMs in new[] { Fipc.Forever, Fipc.NoWait, TimeoutMs })
                Assert.Equal(FipcResult.Cancelled, client.Call(Bytes("never sent"), out _, timeoutMs));
            Assert.Equal(FipcResult.Timeout, server.Receive(out _, 200)); // no request went
        });

        /// <summary>
        /// A client that sends on one thread and receives on another: each request is counted before its send, so the
        /// receiving thread may take its reply first; once every reply has been received, nothing is owed and Call
        /// works.
        /// </summary>
        [Fact(Timeout = 60000)]
        public Task AClientSendingOnOneThreadAndReceivingOnAnother_OwesNothingOnceDone() => Run(() =>
        {
            const int Count = 2000;
            using FipcService service = Create(_name, 64, 1, 1);
            using FipcConnection client = Join(_name, FipcServiceRole.Client);
            using FipcConnection server = Join(_name, FipcServiceRole.Server);
            Task answering = Task.Run(() =>
            {
                for (int i = 0; i < Count + 1; i++)
                    AnswerOne(server);
            });
            Task receiving = Task.Run(() =>
            {
                for (int i = 0; i < Count; i++)
                    Assert.Equal($"R {i}", Receive(client));
            });
            for (int i = 0; i < Count; i++)
                Assert.Equal(FipcResult.Ok, client.Send(Bytes($"r {i}"), TimeoutMs));
            receiving.GetAwaiter().GetResult();
            Assert.Equal("LAST", CallOk(client, "last"));
            answering.GetAwaiter().GetResult();
        });

        /// <summary>
        /// The service's count of members, per role and of both, a wait for them on another thread, Invalid over each
        /// maximum, and its Cancel: final, and the service goes on.
        /// </summary>
        [Fact(Timeout = 30000)]
        public Task WaitForMembers_AndCancel() => Run(() =>
        {
            using FipcService service = Create(_name, 64, 2, 3);
            Assert.Equal(FipcResult.Ok, service.WaitForMembers(null, 0, out int count, Fipc.NoWait));
            Assert.Equal(0, count);
            Assert.Equal(FipcResult.Ok, service.WaitForMembers(FipcServiceRole.Client, 0, out count, Fipc.NoWait));
            Assert.Equal(0, count);
            Assert.Equal(FipcResult.Timeout, service.WaitForMembers(null, 1, out count, Fipc.NoWait));
            Assert.Equal(0, count);
            Assert.Throws<ArgumentOutOfRangeException>(() => service.WaitForMembers(null, -1, out _, 0));

            // Another thread waits for a server while one joins
            Task<(FipcResult, int)> waiting = Task.Run(() =>
                (service.WaitForMembers(FipcServiceRole.Server, 1, out int n, TimeoutMs), n));
            using (FipcConnection server = Join(_name, FipcServiceRole.Server))
            using (FipcConnection client = Join(_name, FipcServiceRole.Client))
            {
                (FipcResult waited, int waitedCount) = waiting.GetAwaiter().GetResult();
                Assert.Equal(FipcResult.Ok, waited);
                Assert.True(waitedCount >= 1, $"counted {waitedCount} servers");
                Assert.Equal(FipcResult.Ok, service.WaitForMembers(null, 2, out count, TimeoutMs));
                Assert.Equal(2, count);
                foreach (FipcServiceRole role in new[] { FipcServiceRole.Client, FipcServiceRole.Server })
                {
                    Assert.Equal(FipcResult.Ok, service.WaitForMembers(role, 0, out count, Fipc.NoWait));
                    Assert.Equal(1, count);
                }
                Assert.Equal(FipcResult.Timeout, service.WaitForMembers(FipcServiceRole.Client, 2, out count, 50));
                Assert.Equal(1, count);

                // Over the role's maximum (maxClients, maxServers, their sum for both), or another role
                foreach ((FipcServiceRole? role, int atLeast) in new (FipcServiceRole?, int)[]
                    { (FipcServiceRole.Client, 3), (FipcServiceRole.Server, 4), (null, 6), ((FipcServiceRole)3, 1) })
                    Assert.Equal(FipcResult.Invalid, service.WaitForMembers(role, atLeast, out _, Fipc.NoWait));

                // A cancel wakes a wait, with the count, and later waits return Cancelled
                waiting = Task.Run(() => (service.WaitForMembers(FipcServiceRole.Server, 2, out int n, TimeoutMs), n));
                Thread.Sleep(100);
                service.Cancel();
                (waited, waitedCount) = waiting.GetAwaiter().GetResult();
                Assert.Equal(FipcResult.Cancelled, waited);
                Assert.Equal(1, waitedCount);
                Assert.Equal(FipcResult.Cancelled, service.WaitForMembers(null, 3, out count, TimeoutMs));
                Assert.Equal(2, count);

                // The service goes on after its cancel: members join, and a call goes through
                using FipcConnection secondClient = Join(_name, FipcServiceRole.Client);
                Task answering = Task.Run(() => AnswerOne(server));
                Assert.Equal("AFTER THE CANCEL", CallOk(secondClient, "after the cancel"));
                answering.GetAwaiter().GetResult();
            }
        });

        /// <summary>
        /// The service's Dispose ends it: clients' sends and servers' receives return Disconnected, a client receives
        /// the replies that came before the end, then Disconnected, a server's reply returns Disconnected, and the name
        /// is free at once.
        /// </summary>
        [Fact(Timeout = 30000)]
        public Task ServicesDispose_EndsTheService() => Run(() =>
        {
            FipcService service = Create(_name, 64, 1, 2);
            using FipcConnection client = Join(_name, FipcServiceRole.Client);
            using FipcConnection server = Join(_name, FipcServiceRole.Server);
            using FipcConnection holder = Join(_name, FipcServiceRole.Server);
            Assert.Equal(FipcResult.Ok, client.Send(Bytes("done"), TimeoutMs));
            Assert.Equal(FipcResult.Ok, client.Send(Bytes("held"), TimeoutMs));
            AnswerOne(server);
            Assert.Equal("held", Receive(holder));
            service.Dispose();
            service.Dispose(); // a second Dispose does nothing
            Assert.Equal(FipcResult.Disconnected, client.Send(Bytes("after"), TimeoutMs));
            Assert.Equal(FipcResult.Disconnected, server.Receive(out _, TimeoutMs));
            Assert.Equal("DONE", Receive(client)); // the replies that came before the end, then Disconnected
            Assert.Equal(FipcResult.Disconnected, client.Receive(out _, TimeoutMs));
            Assert.Equal(FipcResult.Disconnected, holder.Send(Bytes("HELD"), Fipc.NoWait)); // a reply after the end
            Assert.Throws<ObjectDisposedException>(() => service.WaitForMembers(null, 0, out _, Fipc.NoWait));
            service.Cancel(); // a no-op once disposed
            Create(_name, 64, 1, 1).Dispose(); // the name is free again at once
        });

        /// <summary>
        /// Call after the service's end: Disconnected from the drop of a reply that never came and from the send of a
        /// client that owes nothing; Invalid while a request of Send's has its reply owed, and for an empty request.
        /// </summary>
        [Fact(Timeout = 30000)]
        public Task CallAfterTheServicesEnd_DisconnectedFromTheDropAndTheSend() => Run(() =>
        {
            FipcService service = Create(_name, 64, 3, 1);
            using FipcConnection gaveUp = Join(_name, FipcServiceRole.Client);
            using FipcConnection idle = Join(_name, FipcServiceRole.Client);
            using FipcConnection sender = Join(_name, FipcServiceRole.Client);
            using FipcConnection server = Join(_name, FipcServiceRole.Server);
            Assert.Equal(FipcResult.Timeout, gaveUp.Call(Bytes("held"), out _, 50));
            Assert.Equal("held", Receive(server)); // held, never replied to
            Assert.Equal(FipcResult.Ok, sender.Send(Bytes("sent"), TimeoutMs)); // its reply is the caller's
            service.Dispose();
            Assert.Equal(FipcResult.Disconnected, gaveUp.Call(Bytes("next"), out _, TimeoutMs)); // its drop
            Assert.Equal(FipcResult.Disconnected, idle.Call(Bytes("next"), out _, TimeoutMs)); // its send
            Assert.Equal(FipcResult.Invalid, sender.Call(Bytes("next"), out _, TimeoutMs));
            Assert.Equal(FipcResult.Invalid, idle.Call(ReadOnlySpan<byte>.Empty, out _, TimeoutMs)); // checked first
        });

        /// <summary>
        /// Creates a service and joins its server; the service's object is unreachable once this returns.
        /// <paramref name="native"/> is the service's native handle, for the test to close, and
        /// <paramref name="dropped"/> a weak reference to its object.
        /// </summary>
        [MethodImpl(MethodImplOptions.NoInlining)]
        private static FipcConnection DroppedService(string name, out IntPtr native, out WeakReference dropped)
        {
            FipcService service = Create(name, 64, 1, 1);
            var handle = (SafeHandle)typeof(FipcService).GetField("_handle", BindingFlags.NonPublic | BindingFlags.Instance)!
                .GetValue(service)!;
            native = handle.DangerousGetHandle();
            dropped = new WeakReference(service);
            return Join(name, FipcServiceRole.Server);
        }

        /// <summary>
        /// The service has no finalizer: one whose object was dropped, with only a joined server kept, goes on through
        /// the garbage collector's runs and holds its name, until its close (here through the native handle) or the
        /// end of the process.
        /// </summary>
        [Fact(Timeout = 30000)]
        public Task ServiceOutlivesItsDroppedObject() => Run(() =>
        {
            using FipcConnection server = DroppedService(_name, out IntPtr native, out WeakReference dropped);
            for (int i = 0; i < 2; i++)
            {
                GC.Collect();
                GC.WaitForPendingFinalizers();
            }
            Assert.False(dropped.IsAlive, "the service's object was not collected");
            using FipcConnection client = Join(_name, FipcServiceRole.Client);
            Assert.Equal(FipcResult.Ok, client.Send(Bytes("still here"), TimeoutMs));
            AnswerOne(server);
            Assert.Equal("STILL HERE", Receive(client));
            Assert.Equal(FipcResult.AddrInUse,
                Assert.Throws<FipcException>(() => FipcService.Create(_name, Capacity, 64, 1, 1)).Result);
            Fipc.ServiceClose(native);
            Assert.Equal(FipcResult.Disconnected, client.Receive(out _, TimeoutMs));
        });

        /// <summary>
        /// Requests and replies longer than the array receive's 4 KiB buffer arrive whole on both sides; one too long
        /// for a span takes nothing.
        /// </summary>
        [Fact(Timeout = 30000)]
        public Task LongRequestsAndReplies_ReceivedIntoArraysOfTheirLength() => Run(() =>
        {
            using FipcService service = Create(_name, 1 << 14, 1, 1, capacity: 1 << 16);
            using FipcConnection client = Join(_name, FipcServiceRole.Client);
            using FipcConnection server = Join(_name, FipcServiceRole.Server);
            byte[] request = Enumerable.Range(0, 16384).Select(i => (byte)i).ToArray();
            Assert.Equal(FipcResult.Ok, client.Send(request, TimeoutMs));
            Assert.Equal(FipcResult.TooLarge, server.Receive(new byte[4096], out int needed, TimeoutMs));
            Assert.Equal(request.Length, needed);
            Assert.Equal(FipcResult.Ok, server.Receive(out byte[] received, TimeoutMs));
            Assert.Equal(request, received);
            Assert.Equal(FipcResult.Ok, server.Send(request.Reverse().ToArray(), Fipc.NoWait));
            Assert.Equal(FipcResult.Ok, client.Receive(out byte[] reply, TimeoutMs));
            Assert.Equal(request.Reverse(), reply);

            Task answering = Task.Run(() =>
            {
                Assert.Equal(FipcResult.Ok, server.Receive(out byte[] r, TimeoutMs));
                Assert.Equal(FipcResult.Ok, server.Send(r.Reverse().ToArray(), Fipc.NoWait));
            });
            Assert.Equal(FipcResult.Ok, client.Call(request, out reply, TimeoutMs)); // a call's reply too
            Assert.Equal(request.Reverse(), reply);
            answering.GetAwaiter().GetResult();
        });

        /// <summary>The examples' client: joins, calls ping 1 to ping 3 and checks each reply.</summary>
        private static void JoinAndCall(string name, int count, int timeoutMs)
        {
            using FipcConnection client = Join(name, FipcServiceRole.Client, timeoutMs);
            for (int i = 1; i <= count; i++)
                Assert.Equal($"PING {i}", CallOk(client, $"ping {i}", timeoutMs));
        }

        /// <summary>
        /// The examples' exchange: the server creates the service, joins it as its server, answers three requests and
        /// disposes its server and the service; the client calls ping 1 to ping 3. Client first: its thread calls Join
        /// before the service is created, 200 ms later (nothing outside the Join shows that it waits, so the pause
        /// makes that likely, not certain). Client last: it joins once the server waits for a request.
        /// </summary>
        [Fact(Timeout = 30000)]
        public Task ExamplesExchange_InEitherOrder() => Run(() =>
        {
            foreach (bool clientFirst in new[] { true, false })
            {
                string name = $"{_name}_{clientFirst}";
                using var joining = new ManualResetEventSlim();
                Action ask = () =>
                {
                    joining.Set();
                    JoinAndCall(name, 3, TimeoutMs);
                };
                Task? asking = null;
                if (clientFirst)
                {
                    asking = Task.Run(ask);
                    Assert.True(joining.Wait(TimeoutMs));
                    Thread.Sleep(200);
                }
                using (FipcService service = Create(name, 256, 1, 1))
                using (FipcConnection server = Join(name, FipcServiceRole.Server))
                {
                    if (!clientFirst)
                        asking = Task.Run(ask);
                    for (int i = 0; i < 3; i++)
                        AnswerOne(server);
                }
                asking!.GetAwaiter().GetResult();
            }
        });

        /// <summary>
        /// The examples' server and client (examples/csharp), each in a process of its own, in both start orders: the
        /// client prints PING 1 to PING 3, in order, and both exit with 0. The examples use the name my_service.
        /// </summary>
        [Fact(Timeout = 60000)]
        public Task ExampleProcesses_ServiceServerAndClient_InEitherOrder() =>
            Run(() => ExamplePairs.Run("ServiceServer", "ServiceClient", new[] { "PING 1", "PING 2", "PING 3" },
                anyOrder: false));

        /// <summary>
        /// The raw layer: Fipc.ServiceCreate, a client and a server joined from this thread, the count, a request and
        /// its reply, Abandoned, the end at ServiceClose.
        /// </summary>
        [Fact(Timeout = 30000)]
        public Task RawLayer_CreateJoinAndTheEnd() => Run(() =>
        {
            Assert.Equal(FipcResult.Ok, Fipc.ServiceCreate(_name, Capacity, 64, 1, 2, out IntPtr service));
            Assert.Equal(FipcResult.Ok, Fipc.ServiceJoin(_name, FipcServiceRole.Client, out IntPtr client, TimeoutMs));
            Assert.Equal(FipcResult.Ok, Fipc.ServiceJoin(_name, FipcServiceRole.Server, out IntPtr server, TimeoutMs));
            Assert.Equal(FipcResult.Ok, Fipc.ServiceWaitForMembers(service, default, 2, out uint count, TimeoutMs));
            Assert.Equal(2u, count);
            Assert.Equal((nuint)64, Fipc.MaxPiece(client));
            Assert.Equal(FipcResult.AddrInUse, Fipc.ServiceCreate(_name, Capacity, 64, 1, 1, out _));

            var buffer = new byte[64];
            Assert.Equal(FipcResult.Ok, Fipc.Send(client, Bytes("raw"), TimeoutMs));
            Assert.Equal(FipcResult.Ok, Fipc.Recv(server, buffer, out int length, TimeoutMs));
            Assert.Equal("raw", Encoding.UTF8.GetString(buffer, 0, length));
            Assert.Equal(FipcResult.Ok, Fipc.Send(server, Bytes("RAW"), Fipc.NoWait));
            Assert.Equal(FipcResult.Ok, Fipc.Recv(client, buffer, out length, TimeoutMs));
            Assert.Equal("RAW", Encoding.UTF8.GetString(buffer, 0, length));

            Assert.Equal(FipcResult.Ok, Fipc.Send(client, Bytes("held"), TimeoutMs));
            Assert.Equal(FipcResult.Ok, Fipc.Recv(server, buffer, out _, TimeoutMs));
            Fipc.Close(server); // holding it: Abandoned
            Assert.Equal(FipcResult.Abandoned, Fipc.Recv(client, buffer, out length, TimeoutMs));
            Assert.Equal(0, length);
            Assert.Equal("FIPC_ABANDONED", Fipc.ResultStr(FipcResult.Abandoned));

            Fipc.ServiceCancel(service);
            Assert.Equal(FipcResult.Cancelled,
                Fipc.ServiceWaitForMembers(service, FipcServiceRole.Server, 2, out count, TimeoutMs));
            Fipc.ServiceClose(service);
            Assert.Equal(FipcResult.Disconnected, Fipc.Send(client, Bytes("after"), TimeoutMs));
            Fipc.Close(client);
        });

        // === Other processes: the Python binding's peer ===

        private const int PeerCount = 300;
        private const int PeerTimeoutMs = 30_000;

        private Process StartPeer(string python, params string[] args) =>
            BroadcastTests.StartPythonPeer(_output, python, "service_peer.py", args);

        /// <summary>A Python server that owns the service, in another process, and a client here.</summary>
        [Fact(Timeout = 60000)]
        public Task PythonServer_AnswersThisClient() => Run(() =>
        {
            string? python = BroadcastTests.Python(_output);
            if (python == null)
                return;
            using Process peer = StartPeer(python, "server", _name, PeerCount.ToString());
            JoinAndCall(_name, PeerCount, PeerTimeoutMs); // the peer's interpreter takes a moment to create it
            BroadcastTests.ExpectSuccess(peer, "server");
        });

        /// <summary>A server that owns the service here, and two Python clients in other processes.</summary>
        [Fact(Timeout = 60000)]
        public Task PythonClients_CallThisOwnersServer() => Run(() =>
        {
            string? python = BroadcastTests.Python(_output);
            if (python == null)
                return;
            using (FipcService service = Create(_name, 256, 8, 8))
            using (FipcConnection server = Join(_name, FipcServiceRole.Server))
            {
                using Process first = StartPeer(python, "client", _name, PeerCount.ToString());
                using Process second = StartPeer(python, "client", _name, PeerCount.ToString());
                for (int i = 0; i < 2 * PeerCount; i++)
                {
                    Assert.Equal(FipcResult.Ok, server.Receive(out byte[] request, PeerTimeoutMs));
                    Assert.Equal(FipcResult.Ok, server.Send(Upper(request), Fipc.NoWait));
                }
                BroadcastTests.ExpectSuccess(first, "client");
                BroadcastTests.ExpectSuccess(second, "client");
            }
        });

        /// <summary>A Python owner's client in another process, and two servers here that join its service.</summary>
        [Fact(Timeout = 60000)]
        public Task PythonOwnersClient_CallsTheseServers() => Run(() =>
        {
            string? python = BroadcastTests.Python(_output);
            if (python == null)
                return;
            using Process peer = StartPeer(python, "owner-client", _name, PeerCount.ToString(), "2");
            Task<Dictionary<string, int>>[] serving = Enumerable.Range(0, 2).Select(_ => Task.Run(() =>
            {
                using FipcConnection server = Join(_name, FipcServiceRole.Server, PeerTimeoutMs);
                return AnswerUntilTheEnd(server); // until the owner's close ends the service
            })).ToArray();
            int answered = serving.Sum(task => task.GetAwaiter().GetResult().Values.Sum());
            Assert.Equal(PeerCount, answered);
            BroadcastTests.ExpectSuccess(peer, "owner's client");
        });

        /// <summary>Two Python servers in other processes that join the service, and the owner's client here.</summary>
        [Fact(Timeout = 60000)]
        public Task PythonJoinedServers_AnswerThisOwnersClient() => Run(() =>
        {
            string? python = BroadcastTests.Python(_output);
            if (python == null)
                return;
            FipcService service = Create(_name, 256, 8, 8);
            using Process first = StartPeer(python, "joined-server", _name);
            using Process second = StartPeer(python, "joined-server", _name);
            using (service)
            {
                Assert.Equal(FipcResult.Ok, service.WaitForMembers(FipcServiceRole.Server, 2, out _, PeerTimeoutMs));
                JoinAndCall(_name, PeerCount, PeerTimeoutMs);
            } // ends the service: the servers' receives return Disconnected
            BroadcastTests.ExpectSuccess(first, "server");
            BroadcastTests.ExpectSuccess(second, "server");
        });

        /// <summary>
        /// A Python server in another process takes a request and holds it until it is killed: the client's receive
        /// here returns Abandoned once the owner's process has seen its end, and the next call gets its own reply.
        /// </summary>
        [Fact(Timeout = 60000)]
        public Task PythonServerKilledHoldingARequest_AbandonsIt() => Run(() =>
        {
            string? python = BroadcastTests.Python(_output);
            if (python == null)
                return;
            using FipcService service = Create(_name, 64, 1, 2);
            using FipcConnection client = Join(_name, FipcServiceRole.Client);
            using Process peer = StartPeer(python, "holding-server", _name);
            using var holding = new ManualResetEventSlim();
            peer.OutputDataReceived += (_, e) => { if (e.Data?.Trim() == "holding") holding.Set(); };
            try
            {
                Assert.Equal(FipcResult.Ok, client.Send(Bytes("doomed"), TimeoutMs)); // before the peer can take it
                Assert.True(holding.Wait(PeerTimeoutMs), "the Python server did not take the request");
                peer.Kill();
                Assert.True(peer.WaitForExit(PeerTimeoutMs));
                Assert.Equal(FipcResult.Abandoned, client.Receive(out _, TimeoutMs));
                using FipcConnection server = Join(_name, FipcServiceRole.Server);
                Task answering = Task.Run(() => AnswerOne(server));
                Assert.Equal("NEXT", CallOk(client, "next"));
                answering.GetAwaiter().GetResult();
            }
            finally
            {
                if (!peer.HasExited)
                    peer.Kill();
            }
        });
    }
}
