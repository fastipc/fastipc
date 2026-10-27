using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using System.Threading;

#nullable enable

namespace FastIpc.IntegrationTests
{
    public static partial class Snippets
    {
        static bool Show(ReadOnlySpan<byte> tick) => Record($"show {Text(tick)}");

        static bool Missed(long lost) => Record($"missed {lost}");

        static bool Append(ReadOnlySpan<byte> tick) => Record($"audit {Text(tick)}");

        static bool Dropped() => !Record("unsubscribed");

        static bool Work(byte[] job) => Record($"job {Text(job)}");

        static bool Requeue(ReadOnlySpan<byte[]> jobs) => Record($"requeue {jobs.Length}");

        static int Price(ReadOnlySpan<byte> request, Span<byte> reply) =>
            Encoding.UTF8.GetBytes($"{Text(request)}={(Text(request).Length * 1.25m).ToString(CultureInfo.InvariantCulture)}",
                reply);

        static decimal ParsePrice(byte[] reply) =>
            decimal.Parse(Text(reply).Split('=')[1], CultureInfo.InvariantCulture);

        // === Broadcast ===

        // page: publisher
        static void Publish(IEnumerable<byte[]> ticks)
        {
            // the publisher's connection is the broadcast: disposing it ends the broadcast
            using var publisher = FipcBroadcast.Create("ticks", 1 << 20, maxSubscribers: 16, maxStallMs: 500);
            if (publisher.WaitForMembers(atLeast: 2, out int joined, timeoutMs: 10_000) != FipcResult.Ok)
                Console.Error.WriteLine($"starting with {joined} subscribers");
            foreach (byte[] tick in ticks)
            {
                FipcResult result = publisher.Send(tick);   // to every subscriber joined now
                bool more = result switch
                {
                    FipcResult.Ok => true,
                    FipcResult.Cancelled => false,   // shutting down
                    FipcResult.TooLarge or FipcResult.Invalid    // over publisher.MaxPiece, or empty
                        => throw new ArgumentException(Fipc.ResultStr(result), nameof(ticks)),
                    FipcResult.Timeout or FipcResult.Disconnected or FipcResult.NoMemory or FipcResult.AddrInUse
                        or FipcResult.Overrun or FipcResult.Unsubscribed or FipcResult.Abandoned
                        => throw Unexpected("Send", result),
                };
                if (!more)
                    return;
            }
        }
        // end

        // page: lossy
        static void WatchTicks()
        {
            // lossy: never holds the publisher back, and may miss ticks
            using var sub = FipcBroadcast.Join("ticks", FipcSubscriberRole.Lossy);
            var buffer = new byte[sub.MaxPiece];   // the longest message: never TooLarge
            while (true)
            {
                FipcResult result = sub.Receive(buffer, out int length);
                bool more = result switch
                {
                    FipcResult.Ok => Show(buffer.AsSpan(0, length)),
                    FipcResult.Overrun => Missed(sub.LostMessages),   // a lap behind: goes on from a recent tick
                    FipcResult.Disconnected => false,   // the publisher ended the broadcast
                    FipcResult.Cancelled => false,      // shutting down
                    FipcResult.Timeout or FipcResult.TooLarge or FipcResult.Invalid or FipcResult.NoMemory
                        or FipcResult.AddrInUse or FipcResult.Unsubscribed or FipcResult.Abandoned
                        => throw Unexpected("Receive", result),
                };
                if (!more)
                    return;
            }
        }
        // end

        // page: gated
        static void AuditTicks()
        {
            // gated: misses nothing (the publisher waits for it), so it may read in place, with no copy
            using var audit = FipcBroadcast.Join("ticks", FipcSubscriberRole.Gated);
            while (true)
            {
                FipcResult result = audit.ReceiveAcquire(out ReadOnlySpan<byte> tick);
                bool more = result switch
                {
                    FipcResult.Ok => Append(tick),   // holds the publisher back until the release: be quick
                    FipcResult.Unsubscribed => Dropped(),   // held it back past maxStallMs: out for good
                    FipcResult.Disconnected => false,   // the publisher ended the broadcast
                    FipcResult.Cancelled => false,      // shutting down
                    FipcResult.Timeout or FipcResult.TooLarge or FipcResult.Invalid or FipcResult.NoMemory
                        or FipcResult.AddrInUse or FipcResult.Overrun or FipcResult.Abandoned
                        => throw Unexpected("ReceiveAcquire", result),
                };
                audit.ReceiveRelease();   // frees the tick for the publisher; a no-op without one
                if (!more)
                    return;
            }
        }
        // end

        // === Work queue ===

        // page: producer
        static void Produce(byte[][] jobs)
        {
            using var queue = FipcQueue.Create("jobs", 1 << 20, maxProducers: 4, maxConsumers: 8);
            using var producer = FipcQueue.Join("jobs", FipcQueueRole.Producer);   // the owner joins as any member
            queue.WaitForMembers(FipcQueueRole.Consumer, atLeast: 1, out _);
            FipcResult result = producer.SendMany(jobs, out int sent);   // one wake for the whole batch
            _ = result switch
            {
                FipcResult.Ok => true,
                FipcResult.Cancelled => Requeue(jobs.AsSpan(sent)),   // `sent` went, at every result
                FipcResult.TooLarge or FipcResult.Invalid    // a job over MaxPiece, or empty: checked before any went
                    => throw new ArgumentException(Fipc.ResultStr(result), nameof(jobs)),
                FipcResult.Timeout or FipcResult.Disconnected or FipcResult.NoMemory or FipcResult.AddrInUse
                    or FipcResult.Overrun or FipcResult.Unsubscribed or FipcResult.Abandoned
                    => throw Unexpected("SendMany", result),
            };
            // disposing the queue ends it: the consumers take what is left, then Disconnected
        }
        // end

        // page: consumer
        static void Consume(CancellationToken token)
        {
            using var consumer = FipcQueue.Join("jobs", FipcQueueRole.Consumer);
            using var stop = token.Register(consumer.Cancel);   // stops a waiting receive, from any thread
            while (true)
            {
                FipcResult result = consumer.Receive(out byte[] job);
                bool more = result switch
                {
                    FipcResult.Ok => Work(job),   // each job goes to one consumer
                    FipcResult.Disconnected => false,   // the queue ended, and nothing is left in it
                    FipcResult.Cancelled => false,      // the token stopped it
                    FipcResult.Timeout or FipcResult.TooLarge or FipcResult.Invalid or FipcResult.NoMemory
                        or FipcResult.AddrInUse or FipcResult.Overrun or FipcResult.Unsubscribed
                        or FipcResult.Abandoned => throw Unexpected("Receive", result),
                };
                if (!more)
                    return;
            }
        }
        // end

        // === Service ===

        // page: service-owner
        static void RunPricing(CancellationToken token)
        {
            using var service = FipcService.Create("pricing", 1 << 20, maxMessage: 4096, maxClients: 64,
                maxServers: 4);
            var servers = new Thread[4];
            for (int i = 0; i < servers.Length; i++)
                (servers[i] = new Thread(() => ServeQuotes(token))).Start();
            token.WaitHandle.WaitOne();
            service.Dispose();   // ends the service: each server's receive returns Disconnected
            foreach (Thread server in servers)
                server.Join();
        }
        // end

        // page: service-server
        static void ServeQuotes(CancellationToken token)
        {
            FipcConnection server;
            while (true)
            {
                try
                {
                    server = FipcService.Join("pricing", FipcServiceRole.Server, timeoutMs: 500);
                    break;
                }
                catch (FipcException e) when (e.Result == FipcResult.Timeout)
                {
                    // a server's join waits for a client: look at the token now and then
                    if (token.IsCancellationRequested)
                        return;
                }
            }
            using (server)
            {
                Span<byte> reply = stackalloc byte[4096];
                while (true)
                {
                    // the request is read in place, in its client's ring, until the reply
                    FipcResult result = server.ReceiveAcquire(out ReadOnlySpan<byte> request);
                    bool more = result switch
                    {
                        FipcResult.Ok => server.Send(reply[..Price(request, reply)]) == FipcResult.Ok,
                        FipcResult.Disconnected => false,   // the service ended
                        FipcResult.Cancelled => false,      // shutting down
                        FipcResult.Invalid => throw Unexpected("ReceiveAcquire", result),   // a request not replied to
                        FipcResult.Timeout or FipcResult.TooLarge or FipcResult.NoMemory or FipcResult.AddrInUse
                            or FipcResult.Overrun or FipcResult.Unsubscribed or FipcResult.Abandoned
                            => throw Unexpected("ReceiveAcquire", result),
                    };
                    if (!more)
                        return;
                }
            }
        }
        // end

        // page: service-client
        static decimal? Quote(FipcConnection client, string symbol)
        {
            for (int attempt = 0; attempt < 3; attempt++)
            {
                FipcResult result = client.Call(Encoding.UTF8.GetBytes(symbol), out byte[] reply, timeoutMs: 200);
                bool again = result switch
                {
                    FipcResult.Ok => false,
                    FipcResult.Abandoned => true,   // its server ended first: a quote is safe to ask again
                    FipcResult.Timeout => false,    // the reply stays owed: the next Call drops it first
                    FipcResult.Disconnected or FipcResult.Cancelled => false,   // the service ended, or stopping
                    FipcResult.TooLarge or FipcResult.Invalid    // checked before anything is sent
                        => throw new ArgumentException(Fipc.ResultStr(result), nameof(symbol)),
                    FipcResult.NoMemory or FipcResult.AddrInUse or FipcResult.Overrun or FipcResult.Unsubscribed
                        => throw Unexpected("Call", result),
                };
                if (result == FipcResult.Ok)
                    return ParsePrice(reply);
                if (!again)
                    return null;
            }
            return null;
        }
        // end
    }
}
