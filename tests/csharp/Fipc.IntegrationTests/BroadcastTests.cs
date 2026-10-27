using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
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
    /// The broadcast through the binding: a publisher (FipcBroadcast.Create, its WaitForMembers) and lossy and gated
    /// subscribers (FipcBroadcast.Join) in this process, admitted by the library with no thread of the test's waiting
    /// for them, the overrun's lost count, an unsubscribed gated subscriber, the end at the publisher's Dispose (or its
    /// finalizer), the example projects' publisher and subscriber as two processes, and the Python binding's publisher
    /// and subscribers in other processes (bindings/python/tests/broadcast_peer.py). The interpreter is
    /// FIPC_TEST_PYTHON, else the repository's venv, else python; it needs cffi. Without one the Python tests say so
    /// and pass, unless FIPC_REQUIRE_INTEROP is set.
    /// </summary>
    public class BroadcastTests
    {
        /// <summary>
        /// The smallest broadcast: 64 messages of up to 64 bytes before a subscriber falls a lap behind.
        /// </summary>
        private const int Capacity = 4096;
        private const int Slots = Capacity / 64;
        private static int _counter;
        private readonly ITestOutputHelper _output;
        private readonly string _name;

        public BroadcastTests(ITestOutputHelper output)
        {
            _output = output;
            _name = $"fipc_bc_{Environment.ProcessId}_{Interlocked.Increment(ref _counter)}_{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}";
            RawLayerTests.SetLibraryPath();
        }

        private static FipcConnection Create(string name, int maxSubscribers = 8, int maxStallMs = 5000)
        {
            return FipcBroadcast.Create(name, Capacity, maxSubscribers, maxStallMs);
        }

        private static FipcConnection Join(string name, FipcSubscriberRole role)
        {
            return FipcBroadcast.Join(name, role, 10_000);
        }

        private static byte[] Message(int i) => Encoding.UTF8.GetBytes($"message {i}");

        private static Task Run(Action body) => Task.Run(body);

        /// <summary>
        /// A lossy and a gated subscriber each receive every message, the gated one also in place, then the end, which
        /// frees the name.
        /// </summary>
        [Fact(Timeout = 30000)]
        public Task Publisher_LossyAndGatedSubscribers() => Run(() =>
        {
            using FipcConnection publisher = Create(_name);
            Assert.Equal(Capacity / 4, publisher.MaxPiece);
            using FipcConnection lossy = Join(_name, FipcSubscriberRole.Lossy);
            using FipcConnection gated = Join(_name, FipcSubscriberRole.Gated);
            Assert.Equal(Capacity / 4, gated.MaxPiece);
            Assert.Equal(FipcResult.Ok, publisher.WaitForMembers(2, out int count, 10_000));
            Assert.Equal(2, count);

            // Each side's calls of the other side are Invalid
            Assert.Equal(FipcResult.Invalid, publisher.Receive(new byte[64], out _, 0));
            Assert.Equal(FipcResult.Invalid, lossy.Send(Message(0), 0));
            Assert.Equal(FipcResult.Invalid, gated.Send(Message(0), 0));
            Assert.Equal(FipcResult.Invalid, lossy.WaitForMembers(0, out _, 0));
            Assert.Equal(FipcResult.TooLarge, publisher.Send(new byte[publisher.MaxPiece + 1], 0));

            for (int i = 0; i < 10; i++)
                Assert.Equal(FipcResult.Ok, publisher.Send(Message(i), 5000));
            Assert.Equal(FipcResult.Ok, publisher.SendAcquire(Message(10).Length, out Span<byte> room, 5000));
            Message(10).CopyTo(room);
            Assert.Equal(FipcResult.Ok, publisher.SendCommit(Message(10).Length));

            for (int i = 0; i <= 10; i++)
            {
                Assert.Equal(FipcResult.Ok, lossy.Receive(out byte[] message, 5000));
                Assert.Equal(Message(i), message);
                if (i % 2 == 0)
                {
                    Assert.Equal(FipcResult.Ok, gated.ReceiveAcquire(out ReadOnlySpan<byte> inPlace, 5000));
                    Assert.Equal(Message(i), inPlace.ToArray());
                    gated.ReceiveRelease();
                }
                else
                {
                    Assert.Equal(FipcResult.Ok, gated.Receive(out byte[] message2, 5000));
                    Assert.Equal(Message(i), message2);
                }
            }
            Assert.Equal(FipcResult.Invalid, lossy.ReceiveAcquire(out _, 0)); // a lossy subscriber copies
            Assert.Equal(FipcResult.Timeout, gated.Receive(out _, 0));
            Assert.Equal(0, lossy.LostMessages);

            // The publisher's Dispose ends the broadcast: the subscribers get what it sent, then Disconnected, and the
            // name is free at once
            Assert.Equal(FipcResult.Ok, publisher.Send(Message(11), 5000));
            publisher.Dispose();
            foreach (FipcConnection subscriber in new[] { lossy, gated })
            {
                Assert.Equal(FipcResult.Ok, subscriber.Receive(out byte[] last, 5000));
                Assert.Equal(Message(11), last);
                Assert.Equal(FipcResult.Disconnected, subscriber.Receive(out _, 5000));
            }
            Assert.Throws<ObjectDisposedException>(() => publisher.WaitForMembers(0, out _, 0));
            Create(_name).Dispose();
        });

        /// <summary>
        /// The names of the kinds don't mix: a stream's calls are Invalid on a broadcast's name, a broadcast's on a
        /// stream's, and every create of a held name is AddrInUse.
        /// </summary>
        [Fact(Timeout = 30000)]
        public Task StreamsAndBroadcasts_DontMix() => Run(() =>
        {
            using (Create(_name))
            {
                Assert.Equal(FipcResult.Invalid,
                    Assert.Throws<FipcException>(() => FipcStream.Connect(_name, 5000)).Result);
                Assert.Equal(FipcResult.AddrInUse,
                    Assert.Throws<FipcException>(() => FipcStream.Create(_name, 1 << 16)).Result);
                Assert.Equal(FipcResult.AddrInUse,
                    Assert.Throws<FipcException>(() => FipcQueue.Create(_name, Capacity, 1, 1)).Result);
                Assert.Equal(FipcResult.AddrInUse,
                    Assert.Throws<FipcException>(() => FipcBroadcast.Create(_name, Capacity, 1, 0)).Result);
            }
            using (FipcListener listener = FipcStream.Create(_name, 1 << 16))
            {
                // The server's Accept refuses the joiner and goes on waiting
                var accepting = Task.Run(() => listener.Accept(out _, Fipc.Forever));
                Assert.Equal(FipcResult.Invalid, Assert.Throws<FipcException>(
                    () => FipcBroadcast.Join(_name, FipcSubscriberRole.Lossy, 5000)).Result);
                listener.Cancel();
                Assert.Equal(FipcResult.Cancelled, accepting.GetAwaiter().GetResult());
            }
            Assert.Equal(FipcResult.Invalid,
                Assert.Throws<FipcException>(() => FipcBroadcast.Create(_name, 1000, 1, 0)).Result);
            Assert.Throws<ArgumentOutOfRangeException>(() => FipcBroadcast.Create(_name, Capacity, -1, 0));
        });

        /// <summary>
        /// A lossy subscriber that falls a lap behind gets one Overrun with the number of messages lost, then recent
        /// ones.
        /// </summary>
        [Fact(Timeout = 30000)]
        public Task LossySubscriber_Overrun_ReportsTheLostCount() => Run(() =>
        {
            // 144 messages of 48 bytes lap the broadcast's 64 cells twice before the subscriber reads; it goes on from
            // message 140 (docs/protocol.md §9.6), so 0 to 139 are lost
            const int Count = 144, Lost = 140;
            static byte[] Indexed(int i)
            {
                var message = new byte[48];
                BitConverter.TryWriteBytes(message, (long)i); // little-endian on every supported platform
                return message;
            }
            using FipcConnection publisher = Create(_name);
            using FipcConnection lossy = Join(_name, FipcSubscriberRole.Lossy);
            Assert.Equal(FipcResult.Ok, publisher.WaitForMembers(1, out _, 10_000));
            for (int i = 0; i < Count; i++)
                Assert.Equal(FipcResult.Ok, publisher.Send(Indexed(i), Fipc.NoWait)); // never waits for a lossy one

            // The receive into a span reports the lost count as its length, and in LostMessages
            var buffer = new byte[64];
            Assert.Equal(FipcResult.Overrun, lossy.Receive(buffer, out int lost, 5000));
            Assert.Equal(Lost, lost);
            Assert.Equal(Lost, lossy.LostMessages);
            Assert.Equal(FipcResult.Ok, lossy.Receive(out byte[] next, 5000));
            Assert.Equal(Indexed(Lost), next);
            Assert.Equal(Lost, lossy.LostMessages); // until the next Overrun
            for (int i = Lost + 1; i < Count; i++)
            {
                Assert.Equal(FipcResult.Ok, lossy.Receive(buffer, out int length, 5000));
                Assert.Equal(48, length);
                Assert.Equal(Indexed(i), buffer.AsSpan(0, length).ToArray());
            }
            Assert.Equal(FipcResult.Timeout, lossy.Receive(buffer, out _, Fipc.NoWait));
            Assert.Equal(Lost, lossy.LostMessages);
        });

        /// <summary>
        /// The publisher's count of subscribers: at once, a wait for a number of them, a timeout, a cancel, a leave.
        /// </summary>
        [Fact(Timeout = 30000)]
        public Task WaitForMembers_CountAndWait() => Run(() =>
        {
            using FipcConnection publisher = Create(_name, maxSubscribers: 4);
            Assert.Equal(FipcResult.Ok, publisher.WaitForMembers(0, out int count, 0));
            Assert.Equal(0, count);
            Assert.Equal(FipcResult.Timeout, publisher.WaitForMembers(1, out count, 100));
            Assert.Equal(0, count);
            Assert.Equal(FipcResult.Invalid, publisher.WaitForMembers(5, out _, 0)); // over maxSubscribers
            Assert.Throws<ArgumentOutOfRangeException>(() => publisher.WaitForMembers(-1, out _, 0));

            // Another thread waits for two; they join
            FipcResult waited = FipcResult.Invalid;
            int waitedCount = -1;
            var waiter = new Thread(() => waited = publisher.WaitForMembers(2, out waitedCount, Fipc.Forever));
            waiter.Start();
            using FipcConnection first = Join(_name, FipcSubscriberRole.Lossy);
            FipcConnection second = Join(_name, FipcSubscriberRole.Gated);
            Assert.True(waiter.Join(10_000), "the wait for two subscribers did not return");
            Assert.Equal(FipcResult.Ok, waited);
            Assert.Equal(2, waitedCount);

            // One leaves: counted until the publisher's process has seen its end, soon after
            second.Dispose();
            var deadline = Stopwatch.StartNew();
            do
            {
                Assert.Equal(FipcResult.Ok, publisher.WaitForMembers(0, out count, 0));
                if (count == 1)
                    break;
                Thread.Sleep(10);
            } while (deadline.ElapsedMilliseconds < 5000);
            Assert.Equal(1, count);

            // A cancel wakes a wait, with the count
            waiter = new Thread(() => waited = publisher.WaitForMembers(4, out waitedCount, Fipc.Forever));
            waiter.Start();
            Thread.Sleep(100);
            publisher.Cancel();
            Assert.True(waiter.Join(5000), "the cancel did not wake the wait");
            Assert.Equal(FipcResult.Cancelled, waited);
            Assert.Equal(1, waitedCount);
        });

        /// <summary>
        /// A gated subscriber that holds the publisher back for maxStallMs is unsubscribed; a broadcast with 0 refuses
        /// gated ones.
        /// </summary>
        [Fact(Timeout = 30000)]
        public Task GatedSubscriber_Unsubscribed() => Run(() =>
        {
            using (FipcConnection publisher = Create(_name, maxStallMs: 200))
            {
                using FipcConnection gated = Join(_name, FipcSubscriberRole.Gated);
                Assert.Equal(FipcResult.Ok, publisher.WaitForMembers(1, out _, 10_000));
                var sw = Stopwatch.StartNew();
                for (int i = 0; i < 2 * Slots; i++) // past a full broadcast: a send waits, then the subscriber is dropped
                    Assert.Equal(FipcResult.Ok, publisher.Send(Message(i), 10_000));
                Assert.InRange(sw.ElapsedMilliseconds, 150, 10_000);
                Assert.Equal(FipcResult.Unsubscribed, gated.Receive(out _, 5000));
                Assert.Equal(FipcResult.Unsubscribed, gated.ReceiveAcquire(out _, 0));
                Assert.Equal(FipcResult.Ok, publisher.WaitForMembers(0, out int count, 0));
                Assert.Equal(0, count);
            }

            using (Create(_name + "_r", maxStallMs: Fipc.NoWait))
            {
                Assert.Equal(FipcResult.Invalid, Assert.Throws<FipcException>(
                    () => FipcBroadcast.Join(_name + "_r", FipcSubscriberRole.Gated, 10_000)).Result);
                using FipcConnection lossy = Join(_name + "_r", FipcSubscriberRole.Lossy);
            }
        });

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static FipcConnection AbandonedPublisher(string name)
        {
            FipcConnection publisher = Create(name);
            FipcConnection subscriber = Join(name, FipcSubscriberRole.Gated);
            Assert.Equal(FipcResult.Ok, publisher.Send(Message(0), 5000));
            return subscriber; // the publisher is unreachable once this returns
        }

        /// <summary>
        /// A publisher that is never disposed is closed by its finalizer once unreachable, which ends the broadcast and
        /// frees the name.
        /// </summary>
        [Fact(Timeout = 30000)]
        public Task PublisherCollectedByTheGarbageCollector_EndsTheBroadcast() => Run(() =>
        {
            using FipcConnection subscriber = AbandonedPublisher(_name);
            for (int i = 0; i < 2; i++)
            {
                GC.Collect();
                GC.WaitForPendingFinalizers();
            }
            Assert.Equal(FipcResult.Ok, subscriber.Receive(out byte[] message, 5000));
            Assert.Equal(Message(0), message);
            Assert.Equal(FipcResult.Disconnected, subscriber.Receive(out _, 5000));
            // Disconnected comes as the close begins, the name's release at its end: a collection other than these
            // (another test's) may run the finalizer, so its close is waited for before the name is created again
            GC.WaitForPendingFinalizers();
            Create(_name).Dispose();
        });

        /// <summary>
        /// The examples' publisher and subscriber (examples/csharp), each in a process of its own, in both start
        /// orders: the subscriber prints tick 1 to tick 3, and both exit with 0. The examples use the name
        /// my_broadcast.
        /// </summary>
        [Fact(Timeout = 60000)]
        public Task ExampleProcesses_PublisherAndSubscriber_InEitherOrder() =>
            Run(() => ExamplePairs.Run("Publisher", "Subscriber", new[] { "tick 1", "tick 2", "tick 3" },
                anyOrder: false));

        // === Other processes: the Python binding's peer ===

        /// <summary>A C# publisher and Python subscribers, a gated and a lossy one, in other processes.</summary>
        [Fact(Timeout = 60000)]
        public Task PythonSubscribers_ReceiveFromThisPublisher() => Run(() =>
        {
            string? python = Python(_output);
            if (python == null)
                return;
            const int Count = 300;
            using FipcConnection publisher = Create(_name, maxStallMs: 20_000);
            using Process gated = StartPeer(python, "subscriber", _name, "gated", Count.ToString());
            using Process lossy = StartPeer(python, "subscriber", _name, "lossy", Count.ToString());
            Assert.Equal(FipcResult.Ok, publisher.WaitForMembers(2, out _, 30_000));
            for (int i = 0; i < Count; i++)
                Assert.Equal(FipcResult.Ok, publisher.Send(Message(i), 20_000));
            publisher.Dispose(); // ends the broadcast
            ExpectSuccess(gated, "gated subscriber");
            ExpectSuccess(lossy, "lossy subscriber");
        });

        /// <summary>
        /// A Python publisher in another process, and C# subscribers: a gated one (in place, then copied) and a lossy
        /// one.
        /// </summary>
        [Fact(Timeout = 60000)]
        public Task PythonPublisher_SendsToTheseSubscribers() => Run(() =>
        {
            string? python = Python(_output);
            if (python == null)
                return;
            const int Count = 300;
            using Process peer = StartPeer(python, "publisher", _name, Count.ToString(), "2");
            using FipcConnection gated = JoinPeer(_name, FipcSubscriberRole.Gated);
            using FipcConnection lossy = JoinPeer(_name, FipcSubscriberRole.Lossy);

            var lossyCount = Task.Run(() =>
            {
                long next = 0, lost = 0;
                for (;;)
                {
                    FipcResult result = lossy.Receive(out byte[] message, 20_000);
                    if (result == FipcResult.Disconnected)
                        return (next, lost);
                    if (result == FipcResult.Overrun)
                    {
                        next += lossy.LostMessages;
                        lost += lossy.LostMessages;
                        continue;
                    }
                    Assert.Equal(FipcResult.Ok, result);
                    Assert.Equal(Encoding.UTF8.GetString(Message((int)next)), Encoding.UTF8.GetString(message));
                    next++;
                }
            });
            for (int i = 0; i < Count; i++)
            {
                if (i % 2 == 0)
                {
                    Assert.Equal(FipcResult.Ok, gated.ReceiveAcquire(out ReadOnlySpan<byte> inPlace, 20_000));
                    Assert.Equal(Message(i), inPlace.ToArray());
                    gated.ReceiveRelease();
                }
                else
                {
                    Assert.Equal(FipcResult.Ok, gated.Receive(out byte[] message, 20_000));
                    Assert.Equal(Message(i), message);
                }
            }
            Assert.Equal(FipcResult.Disconnected, gated.Receive(out _, 20_000));
            var (total, lostTotal) = lossyCount.GetAwaiter().GetResult();
            Assert.Equal(Count, total);
            _output.WriteLine($"the lossy subscriber lost {lostTotal} of {Count}");
            ExpectSuccess(peer, "publisher");
        });

        private static FipcConnection JoinPeer(string name, FipcSubscriberRole role)
        {
            // The peer's interpreter takes a moment to start and create the broadcast
            return FipcBroadcast.Join(name, role, 30_000);
        }

        /// <summary>The repository: the first folder up from here with build.zig.zon.</summary>
        internal static string Repository()
        {
            for (DirectoryInfo? dir = new DirectoryInfo(AppContext.BaseDirectory); dir != null; dir = dir.Parent)
            {
                if (File.Exists(Path.Combine(dir.FullName, "build.zig.zon")))
                    return dir.FullName;
            }
            throw new InvalidOperationException("the repository (build.zig.zon) isn't above " + AppContext.BaseDirectory);
        }

        /// <summary>
        /// A Python interpreter with cffi, or null (the test then passes, unless FIPC_REQUIRE_INTEROP is set).
        /// </summary>
        internal static string? Python(ITestOutputHelper output)
        {
            string venv = Path.Combine(Repository(), "venv");
            var candidates = new List<string?>
            {
                Environment.GetEnvironmentVariable("FIPC_TEST_PYTHON"),
                OperatingSystem.IsWindows()
                    ? Path.Combine(venv, "Scripts", "python.exe")
                    : Path.Combine(venv, "bin", "python"),
                OperatingSystem.IsWindows() ? "python" : "python3",
            };
            foreach (string? python in candidates)
            {
                if (string.IsNullOrEmpty(python))
                    continue;
                try
                {
                    using var probe = Process.Start(new ProcessStartInfo(python, "-c \"import cffi\"")
                    {
                        UseShellExecute = false,
                        RedirectStandardError = true,
                    })!;
                    probe.StandardError.ReadToEnd();
                    if (probe.WaitForExit(30_000) && probe.ExitCode == 0)
                        return python;
                }
                catch (System.ComponentModel.Win32Exception)
                {
                    // not there
                }
            }
            Assert.True(Environment.GetEnvironmentVariable("FIPC_REQUIRE_INTEROP") == null,
                "no Python with cffi (set FIPC_TEST_PYTHON)");
            output.WriteLine("skipped: no Python with cffi (set FIPC_TEST_PYTHON)");
            return null;
        }

        private Process StartPeer(string python, params string[] args) =>
            StartPythonPeer(_output, python, "broadcast_peer.py", args);

        /// <summary>
        /// Starts one of the Python binding's test peers (bindings/python/tests/<paramref name="script"/>), its output
        /// in the test's.
        /// </summary>
        internal static Process StartPythonPeer(ITestOutputHelper output, string python, string script,
            params string[] args)
        {
            var start = new ProcessStartInfo(python)
            {
                UseShellExecute = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
            };
            start.ArgumentList.Add(Path.Combine(Repository(), "bindings", "python", "tests", script));
            foreach (string arg in args)
                start.ArgumentList.Add(arg);
            Process peer = Process.Start(start)!;
            peer.OutputDataReceived += (_, e) => { if (e.Data != null) output.WriteLine($"peer: {e.Data}"); };
            peer.ErrorDataReceived += (_, e) => { if (e.Data != null) output.WriteLine($"peer: {e.Data}"); };
            peer.BeginOutputReadLine();
            peer.BeginErrorReadLine();
            return peer;
        }

        internal static void ExpectSuccess(Process peer, string what)
        {
            if (!peer.WaitForExit(30_000))
            {
                peer.Kill();
                Assert.Fail($"the Python {what} did not exit within 30 s");
            }
            peer.WaitForExit(); // the output's end
            Assert.True(peer.ExitCode == 0, $"the Python {what} failed: exit code {peer.ExitCode}");
        }
    }
}
