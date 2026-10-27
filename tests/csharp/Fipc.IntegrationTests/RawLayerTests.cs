using System;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;
using FastIpc;

namespace FastIpc.IntegrationTests
{
    /// <summary>
    /// The binding's raw layer (FastIpc.Fipc) against the library: messages over one connection, a server that listens
    /// and accepts and a client that connects, both in this process, on different threads (a connect waits for the
    /// listener's accept); reconnecting means closing and making a new connection on the same name.
    /// </summary>
    public class RawLayerTests
    {
        private const int Ring = 1 << 20;
        private readonly ITestOutputHelper _output;
        private readonly string _name;

        public RawLayerTests(ITestOutputHelper output)
        {
            _output = output;
            _name = $"fipc_test_{Environment.ProcessId}_{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}";
            SetLibraryPath();
        }

        /// <summary>A server's listener and connection and a client's connection on one name.</summary>
        private sealed class Pair : IDisposable
        {
            public IntPtr Listener, Server, Client;

            public static Pair Open(string name)
            {
                var pair = new Pair();
                Assert.Equal(FipcResult.Ok, Fipc.StreamCreate(name, Ring, out pair.Listener));
                // The client connects on another thread: its connect returns once the listener's accept set the
                // connection up
                var connecting = Task.Run(() => (Fipc.StreamConnect(name, out IntPtr c, 10_000), c));
                Assert.Equal(FipcResult.Ok, Fipc.StreamAccept(pair.Listener, out pair.Server, 10_000));
                var (connected, client) = connecting.GetAwaiter().GetResult();
                Assert.Equal(FipcResult.Ok, connected);
                pair.Client = client;
                return pair;
            }

            public void CloseServer()
            {
                Fipc.Close(Server);
                Server = IntPtr.Zero;
                Fipc.ListenerClose(Listener);
                Listener = IntPtr.Zero;
            }

            public void CloseClient()
            {
                Fipc.Close(Client);
                Client = IntPtr.Zero;
            }

            public void Dispose()
            {
                if (Client != IntPtr.Zero) CloseClient();
                if (Server != IntPtr.Zero || Listener != IntPtr.Zero)
                {
                    if (Server != IntPtr.Zero) Fipc.Close(Server);
                    if (Listener != IntPtr.Zero) Fipc.ListenerClose(Listener);
                    Server = Listener = IntPtr.Zero;
                }
            }
        }

        /// <summary>
        /// The client sends a message, the server receives it and answers, the client receives the answer.
        /// </summary>
        private static void RoundTrip(Pair pair, string request, string response)
        {
            var buf = new byte[256];
            Assert.Equal(FipcResult.Ok, Fipc.Send(pair.Client, Encoding.UTF8.GetBytes(request), 5000));
            Assert.Equal(FipcResult.Ok, Fipc.Recv(pair.Server, buf, out int length, 5000));
            Assert.Equal(request, Encoding.UTF8.GetString(buf, 0, length));

            Assert.Equal(FipcResult.Ok, Fipc.Send(pair.Server, Encoding.UTF8.GetBytes(response), 5000));
            Assert.Equal(FipcResult.Ok, Fipc.Recv(pair.Client, buf, out length, 5000));
            Assert.Equal(response, Encoding.UTF8.GetString(buf, 0, length));
        }

        /// <summary>A server waits for its client: accept times out while nobody connects, then connects.</summary>
        private static void DoServerWaitsForClient(string name, ITestOutputHelper output)
        {
            Assert.Equal(FipcResult.Ok, Fipc.StreamCreate(name, Ring, out IntPtr listener));
            try
            {
                Assert.Equal(FipcResult.Timeout, Fipc.StreamAccept(listener, out _, 0));
                var connecting = Task.Run(() => (Fipc.StreamConnect(name, out IntPtr c, 10_000), c));
                Assert.Equal(FipcResult.Ok, Fipc.StreamAccept(listener, out IntPtr server, 10_000));
                var (connected, client) = connecting.GetAwaiter().GetResult();
                Assert.Equal(FipcResult.Ok, connected);
                Fipc.Close(client);
                Fipc.Close(server);
                output.WriteLine("Accept: TIMEOUT without a client, then OK");
            }
            finally
            {
                Fipc.ListenerClose(listener);
            }
        }

        /// <summary>Both sides close and make new connections on the same name, and exchange again.</summary>
        private static void DoReconnectBothSides(string name, ITestOutputHelper output)
        {
            using (var first = Pair.Open(name))
                RoundTrip(first, "phase1", "ok1");
            output.WriteLine("Phase 1: connection and exchange OK; both sides closed");

            using var second = Pair.Open(name);
            RoundTrip(second, "phase2", "ok2");
            output.WriteLine("Phase 2: a new connection on the same name and exchange OK");
        }

        /// <summary>
        /// The server goes: the client learns it at once (Disconnected), closes, and connects to a new server.
        /// </summary>
        private static void DoClientSurvivesServerCrash(string name, ITestOutputHelper output)
        {
            using var pair = Pair.Open(name);
            pair.CloseServer();

            var buf = new byte[64];
            var sw = Stopwatch.StartNew();
            FipcResult result = Fipc.Recv(pair.Client, buf, out _, 10_000);
            output.WriteLine($"Client learned of the server's end in {sw.ElapsedMilliseconds} ms ({result})");
            Assert.Equal(FipcResult.Disconnected, result);
            Assert.Equal(FipcResult.Disconnected, Fipc.Send(pair.Client, "x"u8, 0));
            pair.CloseClient();

            using var next = Pair.Open(name);
            RoundTrip(next, "after_server_crash", "ok");
            output.WriteLine("The client reconnected to a new server");
        }

        /// <summary>Cancel wakes a thread blocked in a receive.</summary>
        private static void DoCancelWakesBlocked(string name, ITestOutputHelper output)
        {
            using var pair = Pair.Open(name);
            FipcResult received = FipcResult.Ok;
            IntPtr server = pair.Server;
            var blocked = new Thread(() =>
            {
                var buf = new byte[64];
                received = Fipc.Recv(server, buf, out _, 30_000);
            });
            blocked.Start();
            Thread.Sleep(200);

            var sw = Stopwatch.StartNew();
            Fipc.Cancel(pair.Server);
            Assert.True(blocked.Join(5000), "the blocked thread did not wake within 5 s of the cancel");
            Assert.Equal(FipcResult.Cancelled, received);
            output.WriteLine($"Cancel woke the blocked thread in {sw.ElapsedMilliseconds} ms");
        }

        /// <summary>Every function of the binding resolves in the library, and checks its handle.</summary>
        private static unsafe void DoAllFunctionsAccessible(string name, ITestOutputHelper output)
        {
            Assert.Equal("FIPC_OK", Fipc.ResultStr(FipcResult.Ok));
            Assert.Equal("FIPC_ADDR_IN_USE", Fipc.ResultStr(FipcResult.AddrInUse));
            var buf = new byte[16];
            Assert.Equal(FipcResult.Invalid, Fipc.StreamAccept(IntPtr.Zero, out _, 0));
            Assert.Equal(FipcResult.Invalid, Fipc.Send(IntPtr.Zero, buf, 0));
            Assert.Equal(FipcResult.Invalid, Fipc.Recv(IntPtr.Zero, buf, out _, 0));
            Assert.Equal(FipcResult.Invalid, Fipc.SendAcquire(IntPtr.Zero, 1, out byte* _, 0));
            Assert.Equal(FipcResult.Invalid, Fipc.SendCommit(IntPtr.Zero, 1));
            Assert.Equal(FipcResult.Invalid, Fipc.RecvAcquire(IntPtr.Zero, out byte* _, out _, 0));
            Fipc.RecvRelease(IntPtr.Zero);
            Fipc.Cancel(IntPtr.Zero);
            Fipc.Close(IntPtr.Zero);
            Fipc.ListenerCancel(IntPtr.Zero);
            Fipc.ListenerClose(IntPtr.Zero);
            Assert.Equal((nuint)0, Fipc.MaxPiece(IntPtr.Zero));
            Assert.Equal(FipcResult.Invalid, Fipc.BroadcastWaitForMembers(IntPtr.Zero, 0, out _, 0));
            Assert.Equal(FipcResult.Invalid, Fipc.BroadcastJoin(name, (FipcSubscriberRole)5, out _, 0));
            Assert.Equal(FipcResult.Invalid, Fipc.BroadcastCreate(name, 1000, 1, 0, out _));
            Assert.Equal(FipcResult.Invalid, Fipc.QueueWaitForMembers(IntPtr.Zero, default, 0, out _, 0));
            Assert.Equal(FipcResult.Invalid, Fipc.QueueJoin(name, (FipcQueueRole)5, out _, 0));
            Assert.Equal(FipcResult.Invalid, Fipc.QueueCreate(name, 1000, 1, 1, out _));
            Fipc.QueueCancel(IntPtr.Zero);
            Fipc.QueueClose(IntPtr.Zero);

            Assert.Equal(FipcResult.Ok, Fipc.StreamCreate(name, Ring, out IntPtr listener));
            Assert.Equal(FipcResult.AddrInUse, Fipc.StreamCreate(name, Ring, out _));
            Fipc.ListenerCancel(listener);
            Assert.Equal(FipcResult.Cancelled, Fipc.StreamAccept(listener, out _, Fipc.Forever));
            Fipc.ListenerClose(listener);
            Assert.Equal(FipcResult.Timeout, Fipc.StreamConnect(name, out _, 0));
            output.WriteLine("Every function the binding declares is accessible from the library");
        }

        [Fact(Timeout = 30000)]
        public async Task Accept_WaitsForTheClient()
        {
            string name = _name;
            var output = _output;
            await Task.Run(() => DoServerWaitsForClient(name, output));
        }

        [Fact(Timeout = 30000)]
        public async Task Reconnect_BothSidesOnTheSameName()
        {
            string name = _name;
            var output = _output;
            await Task.Run(() => DoReconnectBothSides(name, output));
        }

        [Fact(Timeout = 30000)]
        public async Task Reconnect_ClientSurvivesServerCrash()
        {
            string name = _name;
            var output = _output;
            await Task.Run(() => DoClientSurvivesServerCrash(name, output));
        }

        [Fact(Timeout = 30000)]
        public async Task Cancel_WakesBlockedThread()
        {
            string name = _name;
            var output = _output;
            await Task.Run(() => DoCancelWakesBlocked(name, output));
        }

        [Fact(Timeout = 30000)]
        public async Task DllLoad_AllFunctionsAccessible()
        {
            string name = _name;
            var output = _output;
            await Task.Run(() => DoAllFunctionsAccessible(name, output));
        }

        internal static void SetLibraryPath()
        {
            string searchDir = Environment.CurrentDirectory;
            for (int i = 0; i < 10; i++)
            {
                string candidate = Path.Combine(searchDir, "zig-out", "bin");
                if (Directory.Exists(candidate))
                {
                    string currentPath = Environment.GetEnvironmentVariable("PATH") ?? "";
                    if (!currentPath.Contains(candidate))
                    {
                        Environment.SetEnvironmentVariable("PATH", $"{candidate};{currentPath}");
                    }
                    return;
                }

#nullable enable
                string? parent = Directory.GetParent(searchDir)?.FullName;
#nullable restore
                if (parent == null || parent == searchDir) break;
                searchDir = parent;
            }
        }
    }
}
