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
    /// The C# binding (FastIpc.Fipc) against the library, as a game platform uses it: RPC over one connection, a
    /// server that listens and accepts and a client that connects, both in this process, on different threads (a
    /// connect waits for the listener's accept); reconnecting means closing and making a new connection on the same
    /// name.
    /// </summary>
    public class RpcReconnectTests
    {
        private const int Ring = 1 << 20;
        private readonly ITestOutputHelper _output;
        private readonly string _name;

        public RpcReconnectTests(ITestOutputHelper output)
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
                Assert.Equal(FipcResult.Ok, Fipc.Listen(name, Ring, out pair.Listener));
                // The client connects on another thread: its connect returns once the listener's accept set the
                // connection up
                var connecting = Task.Run(() => (Fipc.Connect(name, out IntPtr c, 10_000), c));
                Assert.Equal(FipcResult.Ok, Fipc.Accept(pair.Listener, out pair.Server, 10_000));
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

        /// <summary>The client submits a request, the server receives it and responds, the client receives the response.</summary>
        private static void RoundTrip(Pair pair, uint opcode, string request, string response)
        {
            byte[] requestBytes = Encoding.UTF8.GetBytes(request);
            Assert.Equal(FipcResult.Ok, Fipc.RpcSubmit(pair.Client, opcode, requestBytes, out ulong id, 5000));

            var buf = new byte[256];
            Assert.Equal(FipcResult.Ok, Fipc.RpcRecv(pair.Server, buf, out FipcRpcMsg msg, 5000));
            Assert.Equal(Fipc.RpcRequest, msg.Kind);
            Assert.Equal(opcode, msg.Opcode);
            Assert.Equal(id, msg.Id);
            Assert.Equal(request, Encoding.UTF8.GetString(buf, 0, (int)msg.Len));

            Assert.Equal(FipcResult.Ok, Fipc.RpcRespond(pair.Server, msg.Id, opcode, 0, Encoding.UTF8.GetBytes(response), 5000));
            Assert.Equal(FipcResult.Ok, Fipc.RpcRecv(pair.Client, buf, out FipcRpcMsg reply, 5000));
            Assert.Equal(Fipc.RpcResponse, reply.Kind);
            Assert.Equal(id, reply.Id);
            Assert.Equal(response, Encoding.UTF8.GetString(buf, 0, (int)reply.Len));
        }

        private static void DoRpcRoundTrip(string name, ITestOutputHelper output)
        {
            using var pair = Pair.Open(name);
            RoundTrip(pair, 42, "hello", "world");
            output.WriteLine("RPC round trip completed");
        }

        /// <summary>A server waits for its client: accept times out while nobody connects, then connects.</summary>
        private static void DoServerWaitsForClient(string name, ITestOutputHelper output)
        {
            Assert.Equal(FipcResult.Ok, Fipc.Listen(name, Ring, out IntPtr listener));
            try
            {
                Assert.Equal(FipcResult.Timeout, Fipc.Accept(listener, out _, 0));
                var connecting = Task.Run(() => (Fipc.Connect(name, out IntPtr c, 10_000), c));
                Assert.Equal(FipcResult.Ok, Fipc.Accept(listener, out IntPtr server, 10_000));
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
                RoundTrip(first, 1, "phase1", "ok1");
            output.WriteLine("Phase 1: connection and exchange OK; both sides closed");

            using var second = Pair.Open(name);
            RoundTrip(second, 99, "phase2", "ok2");
            output.WriteLine("Phase 2: a new connection on the same name and exchange OK");
        }

        /// <summary>A reconnecting consumer's pattern: a full RPC cycle, both sides go, new ones connect, and a second cycle succeeds.</summary>
        private static void DoGamePlatformReconnect(string name, ITestOutputHelper output)
        {
            using (var session1 = Pair.Open(name))
                RoundTrip(session1, 1, "GetPlayerStats", "{\"level\":42}");
            output.WriteLine("Session 1: full RPC cycle OK");

            using var session2 = Pair.Open(name);
            RoundTrip(session2, 2, "GetInventory", "{\"items\":[]}");
            output.WriteLine("Session 2: full RPC cycle after reconnect OK");
        }

        /// <summary>The server goes: the client learns it at once (Disconnected), closes, and connects to a new server.</summary>
        private static void DoClientSurvivesServerCrash(string name, ITestOutputHelper output)
        {
            using var pair = Pair.Open(name);
            pair.CloseServer();

            var buf = new byte[64];
            var sw = Stopwatch.StartNew();
            FipcResult result = Fipc.RpcRecv(pair.Client, buf, out _, 10_000);
            output.WriteLine($"Client learned of the server's end in {sw.ElapsedMilliseconds} ms ({result})");
            Assert.Equal(FipcResult.Disconnected, result);
            Assert.Equal(FipcResult.Disconnected, Fipc.RpcSubmit(pair.Client, 1, ReadOnlySpan<byte>.Empty, out _, 0));
            pair.CloseClient();

            using var next = Pair.Open(name);
            RoundTrip(next, 77, "after_server_crash", "ok");
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
                received = Fipc.RpcRecv(server, buf, out _, 30_000);
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
            Assert.Equal(FipcResult.Invalid, Fipc.Accept(IntPtr.Zero, out _, 0));
            Assert.Equal(FipcResult.Invalid, Fipc.Send(IntPtr.Zero, buf, 0));
            Assert.Equal(FipcResult.Invalid, Fipc.Recv(IntPtr.Zero, buf, out _, 0));
            Assert.Equal(FipcResult.Invalid, Fipc.SendAcquire(IntPtr.Zero, 1, out byte* _, 0));
            Assert.Equal(FipcResult.Invalid, Fipc.SendCommit(IntPtr.Zero, 1));
            Assert.Equal(FipcResult.Invalid, Fipc.RecvAcquire(IntPtr.Zero, out byte* _, out _, 0));
            Assert.Equal(FipcResult.Invalid, Fipc.RpcSubmit(IntPtr.Zero, 1, buf, out _, 0));
            Assert.Equal(FipcResult.Invalid, Fipc.RpcRespond(IntPtr.Zero, 1, 1, 0, buf, 0));
            Assert.Equal(FipcResult.Invalid, Fipc.RpcRecv(IntPtr.Zero, buf, out _, 0));
            Fipc.RecvRelease(IntPtr.Zero);
            Fipc.Cancel(IntPtr.Zero);
            Fipc.Close(IntPtr.Zero);
            Fipc.ListenerCancel(IntPtr.Zero);
            Fipc.ListenerClose(IntPtr.Zero);

            Assert.Equal(FipcResult.Ok, Fipc.Listen(name, Ring, out IntPtr listener));
            Assert.Equal(FipcResult.AddrInUse, Fipc.Listen(name, Ring, out _));
            Fipc.ListenerCancel(listener);
            Assert.Equal(FipcResult.Cancelled, Fipc.Accept(listener, out _, Fipc.Forever));
            Fipc.ListenerClose(listener);
            Assert.Equal(FipcResult.Timeout, Fipc.Connect(name, out _, 0));
            output.WriteLine("All 17 functions are accessible from the library");
        }

        [Fact(Timeout = 30000)]
        public async Task RpcRoundTrip_BasicMessage()
        {
            string name = _name;
            var output = _output;
            await Task.Run(() => DoRpcRoundTrip(name, output));
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
        public async Task GamePlatformReconnect_FullRpcCycleAfterCrash()
        {
            string name = _name;
            var output = _output;
            await Task.Run(() => DoGamePlatformReconnect(name, output));
        }

        [Fact(Timeout = 30000)]
        public async Task GamePlatformReconnect_ClientSurvivesServerCrash()
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
