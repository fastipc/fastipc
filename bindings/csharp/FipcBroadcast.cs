using System;

#nullable enable

namespace FastIpc
{
    /// <summary>
    /// Broadcasts: one publisher and up to 1024 subscribers, in any processes of the user. Every message goes to every
    /// subscriber, in one order, and is stored once, in memory every subscriber maps. The publisher creates the name
    /// (<see cref="Create"/>), which returns its connection, the broadcast's one handle; subscribers join it
    /// (<see cref="Join"/>). The publisher's process admits each joiner by itself, on a thread of the library's: no
    /// thread of the publisher's waits for them, and <see cref="FipcConnection.WaitForMembers"/> waits for a number of
    /// them before the first send. A broadcast's messages are of one piece, up to <see cref="FipcConnection.MaxPiece"/>
    /// bytes (a quarter of the capacity).
    ///
    /// Setup throws: Create and Join throw <see cref="FipcException"/>, whose <see cref="FipcException.Result"/> says
    /// why. Everything after setup returns a <see cref="FipcResult"/>. A caller's mistake (a null name, a negative
    /// count, a call after Dispose) throws as .NET's own calls do.
    /// </summary>
    public static class FipcBroadcast
    {
        /// <summary>
        /// Publisher: claims <paramref name="name"/> for a broadcast, and returns the publisher's connection, whose
        /// sends go to every joined subscriber. Doesn't wait: subscribers may join from now on.
        /// <paramref name="capacity"/> is the bytes of messages it holds (a power of two from 4096 to 2^30): up to
        /// capacity / 64 messages, each of one piece, at most a quarter of it (the publisher's
        /// <see cref="FipcConnection.MaxPiece"/>). <paramref name="maxSubscribers"/> (1 to 1024) may be joined at once.
        /// A gated subscriber that holds the publisher back for <paramref name="maxStallMs"/> is unsubscribed
        /// (<see cref="Fipc.Forever"/>: never; <see cref="Fipc.NoWait"/>: gated subscribers are refused). Disposing the
        /// publisher ends the broadcast and frees the name: its subscribers get Disconnected after what it sent.
        /// Throws <see cref="FipcException"/>: AddrInUse if another stream, broadcast, queue or service holds the name;
        /// Invalid for a bad name, capacity or maxSubscribers; NoMemory if the broadcast's memory can't be had.
        /// </summary>
        public static FipcConnection Create(string name, int capacity, int maxSubscribers, int maxStallMs)
        {
            if (name == null) throw new ArgumentNullException(nameof(name));
            if (capacity < 0) throw new ArgumentOutOfRangeException(nameof(capacity));
            if (maxSubscribers < 0) throw new ArgumentOutOfRangeException(nameof(maxSubscribers));
            FipcResult result = Fipc.BroadcastCreate(name, (nuint)capacity, (uint)maxSubscribers, maxStallMs,
                out IntPtr handle);
            if (result != FipcResult.Ok)
                throw new FipcException("FipcBroadcast.Create", name, result);
            return FipcConnection.Wrap(handle);
        }

        /// <summary>
        /// Subscriber: joins the broadcast on <paramref name="name"/> as <paramref name="role"/> and returns its
        /// connection, which receives the messages sent from its admission on, waiting up to
        /// <paramref name="timeoutMs"/> for the publisher to create the broadcast and to have a free slot, and for the
        /// publisher's process to admit it. Throws <see cref="FipcException"/>: Timeout if none did; Invalid for
        /// another role, a name a queue or a service holds, a publisher of another version or user, or
        /// <see cref="FipcSubscriberRole.Gated"/> on a broadcast that refuses gated subscribers. On a name a stream
        /// holds, the join learns the kind when the stream's server accepts it, which brings Invalid; while no accept
        /// runs, it waits until it times out. Disposing the connection leaves the broadcast.
        /// </summary>
        public static FipcConnection Join(string name, FipcSubscriberRole role, int timeoutMs = Fipc.Forever)
        {
            if (name == null) throw new ArgumentNullException(nameof(name));
            FipcResult result = Fipc.BroadcastJoin(name, role, out IntPtr conn, timeoutMs);
            if (result != FipcResult.Ok)
                throw new FipcException("FipcBroadcast.Join", name, result);
            return FipcConnection.Wrap(conn);
        }
    }
}
