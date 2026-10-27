using System;
using System.Runtime.InteropServices;

namespace FastIpc
{
    /// <summary>fipc_result_t: the result of every call.</summary>
    public enum FipcResult
    {
        Ok = 0,
        /// <summary>The timeout ran out (with timeout 0: nothing to do without waiting).</summary>
        Timeout = 1,
        /// <summary>The peer ended the connection. Final: close it, then accept or connect a new one.</summary>
        Disconnected = 2,
        /// <summary>Cancel was called (on the connection, the listener, the queue or the service), and the call would have to wait.</summary>
        Cancelled = 3,
        /// <summary>The message doesn't fit what the call can take; it stays queued, and its length is reported.</summary>
        TooLarge = 4,
        /// <summary>A bad argument or handle, a call out of order, a peer with another version or user.</summary>
        Invalid = 5,
        /// <summary>Memory or another OS resource ran out (setting a connection up).</summary>
        NoMemory = 6,
        /// <summary>A create (StreamCreate, BroadcastCreate, QueueCreate, ServiceCreate): another stream, broadcast, queue or service holds the name.</summary>
        AddrInUse = 7,
        /// <summary>A lossy subscriber fell a lap behind: messages were lost. Instead of a message, the receive reports how many (at least 1); the next goes on from a recent message.</summary>
        Overrun = 8,
        /// <summary>A gated subscriber held the publisher back for the broadcast's maxStallMs and was dropped from it: every later receive returns this. Final: close it.</summary>
        Unsubscribed = 9,
        /// <summary>A service's client: the server that took the request ended before it replied. The request is answered (the next receive returns the next reply); it may or may not have been carried out.</summary>
        Abandoned = 10,
    }

    /// <summary>A broadcast subscriber's role, chosen when it joins (<see cref="FipcBroadcast.Join"/>).</summary>
    public enum FipcSubscriberRole
    {
        /// <summary>Never holds the publisher back. One that falls a lap behind gets <see cref="FipcResult.Overrun"/> instead of a message.</summary>
        Lossy = 1,
        /// <summary>Misses nothing, as the publisher waits for it, and may receive in place. One that holds the publisher back for the broadcast's maxStallMs is unsubscribed.</summary>
        Gated = 2,
    }

    /// <summary>A work queue member's role, chosen when it joins (<see cref="FipcQueue.Join"/>).</summary>
    public enum FipcQueueRole
    {
        /// <summary>Its sends add messages to the queue, each taken by one consumer.</summary>
        Producer = 3,
        /// <summary>Each receive takes a message of any producer, which no other consumer gets.</summary>
        Consumer = 4,
    }

    /// <summary>A service member's role, chosen when it joins (<see cref="FipcService.Join"/>).</summary>
    public enum FipcServiceRole
    {
        /// <summary>Its sends are requests, each taken by one server, and its receives their replies, in the order of its requests.</summary>
        Client = 5,
        /// <summary>Each receive takes the next request of any client, which it holds until its send replies to it.</summary>
        Server = 6,
    }

    /// <summary>
    /// The FastIPC C API (include/fipc.h), as thin P/Invoke declarations: the raw layer, with nothing between the
    /// caller and the library. <see cref="FipcStream"/>, <see cref="FipcBroadcast"/>, <see cref="FipcQueue"/> and
    /// <see cref="FipcService"/> are the safe layer over it, with <see cref="FipcListener"/> and
    /// <see cref="FipcConnection"/>; this one is the escape hatch for the hottest loops. The safe layer's setup (create,
    /// connect, join, accept) throws <see cref="FipcException"/>; nothing here throws: every call, setup included,
    /// returns its <see cref="FipcResult"/>.
    ///
    /// A stream's server creates a name and accepts one client at a time; a client connects to the name. A broadcast's
    /// publisher creates a name, and subscribers join it; a work queue's owner creates a name, and producers and
    /// consumers join it, the owner's own too; a service's owner creates a name, and clients and servers join it, the
    /// owner's own too. The owner's process admits joiners by itself. Handles are opaque pointers (a listener, a
    /// connection, a queue, a service); timeouts are int milliseconds, the last parameter: <see cref="NoWait"/>
    /// (0) or <see cref="Forever"/> (-1). On a connection one thread at a time sends and one receives; Cancel may be
    /// called from any thread; Close only when no other thread is inside a call on the handle: cancel, join the
    /// threads, close. Nothing here enforces that: closing under a running call frees memory the call still uses.
    ///
    /// The library is "fastipc": NuGet packages load it from runtimes/{rid}/native/; development builds find it on
    /// LD_LIBRARY_PATH (Linux) or next to the executable (Windows, macOS).
    /// </summary>
    public static unsafe class Fipc
    {
        private const string Library = "fastipc";

        public const int NoWait = 0;
        public const int Forever = -1;

        /// <summary>The library's name for a result ("FIPC_OK", ...).</summary>
        public static string ResultStr(FipcResult result) => Marshal.PtrToStringAnsi(fipc_result_str(result)) ?? "";

        [DllImport(Library, ExactSpelling = true)]
        private static extern IntPtr fipc_result_str(FipcResult result);

        // === Streams: a server, and one client at a time ===

        /// <summary>Server: claims <paramref name="name"/> for a stream and listens on it, with rings of <paramref name="capacity"/> bytes. Doesn't wait. AddrInUse if another stream, broadcast, queue or service holds the name.</summary>
        [DllImport(Library, EntryPoint = "fipc_stream_create", ExactSpelling = true)]
        public static extern FipcResult StreamCreate([MarshalAs(UnmanagedType.LPUTF8Str)] string name, nuint capacity, out IntPtr listener);

        /// <summary>Server: waits for a client and sets its connection up on the calling thread; a call that times out in the middle of a client's setup keeps it for the next call, so a timeout of 0 polls. Invalid while the connection it accepted last is open; NoMemory once the listener has failed for good.</summary>
        [DllImport(Library, EntryPoint = "fipc_stream_accept", ExactSpelling = true)]
        public static extern FipcResult StreamAccept(IntPtr listener, out IntPtr conn, int timeoutMs);

        /// <summary>Every StreamAccept that waits, now or later, returns Cancelled; a client whose setup had begun is dropped. Any thread.</summary>
        [DllImport(Library, EntryPoint = "fipc_listener_cancel", ExactSpelling = true)]
        public static extern void ListenerCancel(IntPtr listener);

        /// <summary>Stops listening and frees the listener; the connections it accepted stay open.</summary>
        [DllImport(Library, EntryPoint = "fipc_listener_close", ExactSpelling = true)]
        public static extern void ListenerClose(IntPtr listener);

        /// <summary>Client: connects to the stream on <paramref name="name"/> and sets the connection up on the calling thread, waiting for its server to create it and to call StreamAccept: the server runs on another thread or in another process.</summary>
        [DllImport(Library, EntryPoint = "fipc_stream_connect", ExactSpelling = true)]
        public static extern FipcResult StreamConnect([MarshalAs(UnmanagedType.LPUTF8Str)] string name, out IntPtr conn, int timeoutMs);

        // === Connections ===

        /// <summary>The largest message the zero-copy calls take on the connection: its capacity less 64 bytes (a broadcast's or a queue's: a quarter of its capacity, the largest message it takes; a service's: its maxMessage). Any thread.</summary>
        [DllImport(Library, EntryPoint = "fipc_max_piece", ExactSpelling = true)]
        public static extern nuint MaxPiece(IntPtr conn);

        /// <summary>Every call of the connection that waits, now or later, returns Cancelled. Any thread.</summary>
        [DllImport(Library, EntryPoint = "fipc_cancel", ExactSpelling = true)]
        public static extern void Cancel(IntPtr conn);

        /// <summary>Ends the connection (the peer gets Disconnected) and frees it. No other thread may be inside a call on it.</summary>
        [DllImport(Library, EntryPoint = "fipc_close", ExactSpelling = true)]
        public static extern void Close(IntPtr conn);

        // === Messages ===

        /// <summary>Sends one message of any size (at least 1 byte).</summary>
        [DllImport(Library, EntryPoint = "fipc_send", ExactSpelling = true)]
        public static extern FipcResult Send(IntPtr conn, byte* data, nuint len, int timeoutMs);

        /// <summary>Receives one message into <paramref name="buf"/>. TooLarge (the message stays) if it doesn't fit: <paramref name="len"/> is its length. Overrun (a lossy subscriber's): <paramref name="len"/> is the number of messages lost.</summary>
        [DllImport(Library, EntryPoint = "fipc_recv", ExactSpelling = true)]
        public static extern FipcResult Recv(IntPtr conn, byte* buf, nuint bufLen, out nuint len, int timeoutMs);

        /// <summary>Sends <paramref name="count"/> messages (at least 1) in order, message i being <paramref name="lens"/>[i] bytes (at least 1) at <paramref name="datas"/>[i], and publishes them together, with at most one wake of the receivers; the timeout bounds the whole call. <paramref name="sent"/> is how many went, on every return: any result but Ok is the first message that didn't go, as Send would return it. Every message is checked before any goes (Invalid for an empty one, TooLarge over MaxPiece on a broadcast, a queue or a service). Invalid on a service's server. Batch both ends.</summary>
        [DllImport(Library, EntryPoint = "fipc_send_many", ExactSpelling = true)]
        public static extern FipcResult SendMany(IntPtr conn, byte** datas, nuint* lens, nuint count, out nuint sent, int timeoutMs);

        /// <summary>Waits for the next message, as Recv, then takes every further message already there that fits the rest of <paramref name="buf"/>, up to <paramref name="maxCount"/> (at least 1), packed back to back, message i being <paramref name="lens"/>[i] bytes; <paramref name="count"/> is how many it took (0 unless Ok). Any result but Ok is the first message's, as Recv returns it: TooLarge (lens[0] its length, nothing taken), Overrun (lens[0] the number lost), Abandoned. Invalid on a publisher, a producer and a service's server.</summary>
        [DllImport(Library, EntryPoint = "fipc_recv_many", ExactSpelling = true)]
        public static extern FipcResult RecvMany(IntPtr conn, byte* buf, nuint bufLen, nuint* lens, nuint maxCount, out nuint count, int timeoutMs);

        /// <summary>Zero-copy send, step 1: <paramref name="len"/> bytes (at most MaxPiece) in the ring to write the message into.</summary>
        [DllImport(Library, EntryPoint = "fipc_send_acquire", ExactSpelling = true)]
        public static extern FipcResult SendAcquire(IntPtr conn, nuint len, out byte* buf, int timeoutMs);

        /// <summary>Zero-copy send, step 2: sends the first <paramref name="len"/> bytes of the acquired room as one message.</summary>
        [DllImport(Library, EntryPoint = "fipc_send_commit", ExactSpelling = true)]
        public static extern FipcResult SendCommit(IntPtr conn, nuint len);

        /// <summary>Zero-copy receive, step 1: the next message in the ring, valid until RecvRelease, the next receive or Close. TooLarge for a message of several pieces.</summary>
        [DllImport(Library, EntryPoint = "fipc_recv_acquire", ExactSpelling = true)]
        public static extern FipcResult RecvAcquire(IntPtr conn, out byte* data, out nuint len, int timeoutMs);

        /// <summary>Zero-copy receive, step 2: frees the acquired message's room.</summary>
        [DllImport(Library, EntryPoint = "fipc_recv_release", ExactSpelling = true)]
        public static extern void RecvRelease(IntPtr conn);

        // === Broadcasts: one publisher, many subscribers ===

        /// <summary>Publisher: claims <paramref name="name"/> for a broadcast of <paramref name="capacity"/> bytes of messages (a power of two, 4096 to 2^31) and up to <paramref name="maxSubscribers"/> subscribers (1 to 1024), and returns the publisher's connection, the broadcast's one handle: its Close ends the broadcast. A gated subscriber that holds the publisher back for <paramref name="maxStallMs"/> is unsubscribed (Forever: never; NoWait: gated subscribers are refused). The publisher's process admits joiners by itself. AddrInUse if another stream, broadcast, queue or service holds the name.</summary>
        [DllImport(Library, EntryPoint = "fipc_broadcast_create", ExactSpelling = true)]
        public static extern FipcResult BroadcastCreate([MarshalAs(UnmanagedType.LPUTF8Str)] string name, nuint capacity, uint maxSubscribers, int maxStallMs, out IntPtr publisher);

        /// <summary>Subscriber: joins the broadcast on <paramref name="name"/> as <paramref name="role"/>, waiting for the publisher to create it, to have a free slot and for its process to admit the subscriber. Invalid for another role, a name a queue or a service holds, or a gated subscriber on a broadcast that refuses them; on a name a stream holds, Invalid once the stream's server accepts the joiner, Timeout while no accept runs.</summary>
        [DllImport(Library, EntryPoint = "fipc_broadcast_join", ExactSpelling = true)]
        public static extern FipcResult BroadcastJoin([MarshalAs(UnmanagedType.LPUTF8Str)] string name, FipcSubscriberRole role, out IntPtr conn, int timeoutMs);

        /// <summary>Publisher: waits until at least <paramref name="atLeast"/> subscribers are joined; <paramref name="count"/> is how many are, at every return but Invalid. Ok, Timeout, Cancelled (after Cancel of the publisher); Invalid on another connection than a publisher, or for atLeast over its maxSubscribers. Any thread.</summary>
        [DllImport(Library, EntryPoint = "fipc_broadcast_wait_for_members", ExactSpelling = true)]
        public static extern FipcResult BroadcastWaitForMembers(IntPtr publisher, uint atLeast, out uint count, int timeoutMs);

        // === Work queues: many producers, many consumers ===

        /// <summary>Owner: claims <paramref name="name"/> for a work queue of up to <paramref name="maxProducers"/> producers (1 to 63) and <paramref name="maxConsumers"/> consumers (1 to 64), each producer with a ring of <paramref name="capacity"/> bytes of messages (a power of two, 4096 to 2^31), and returns the queue's handle: QueueClose ends the queue. The owner's process admits joiners by itself, the owner's own members too. Invalid for a capacity or member count out of range; NoMemory; AddrInUse if another stream, broadcast, queue or service holds the name.</summary>
        [DllImport(Library, EntryPoint = "fipc_queue_create", ExactSpelling = true)]
        public static extern FipcResult QueueCreate([MarshalAs(UnmanagedType.LPUTF8Str)] string name, nuint capacity, uint maxProducers, uint maxConsumers, out IntPtr queue);

        /// <summary>Producer or consumer: joins the work queue on <paramref name="name"/> as <paramref name="role"/>, waiting for the owner to create it, for a free place of the role and for the owner's process to admit the member. Invalid for another role, or a name a broadcast or a service holds; on a name a stream holds, Invalid once the stream's server accepts the joiner, Timeout while no accept runs.</summary>
        [DllImport(Library, EntryPoint = "fipc_queue_join", ExactSpelling = true)]
        public static extern FipcResult QueueJoin([MarshalAs(UnmanagedType.LPUTF8Str)] string name, FipcQueueRole role, out IntPtr conn, int timeoutMs);

        /// <summary>Owner: waits until at least <paramref name="atLeast"/> members of <paramref name="role"/> are joined (default(FipcQueueRole), 0, counts both roles); <paramref name="count"/> is how many are, at every return but Invalid. Ok, Timeout, Cancelled (after QueueCancel), Disconnected once the queue has ended; Invalid for another role, or for atLeast over the role's maximum (their sum for both). Any thread.</summary>
        [DllImport(Library, EntryPoint = "fipc_queue_wait_for_members", ExactSpelling = true)]
        public static extern FipcResult QueueWaitForMembers(IntPtr queue, FipcQueueRole role, uint atLeast, out uint count, int timeoutMs);

        /// <summary>Every QueueWaitForMembers of the queue that waits, now or later, returns Cancelled; the queue goes on. Any thread.</summary>
        [DllImport(Library, EntryPoint = "fipc_queue_cancel", ExactSpelling = true)]
        public static extern void QueueCancel(IntPtr queue);

        /// <summary>Ends the queue and frees the handle: producers' sends return Disconnected, and consumers receive what is left, then Disconnected. The members' connections stay open until their Close. No other thread may be inside a call on the queue.</summary>
        [DllImport(Library, EntryPoint = "fipc_queue_close", ExactSpelling = true)]
        public static extern void QueueClose(IntPtr queue);

        // === Services: many clients, a pool of servers, with replies ===

        /// <summary>Owner: claims <paramref name="name"/> for a service of up to <paramref name="maxClients"/> clients (1 to 64) and <paramref name="maxServers"/> servers (1 to 64), each client with a ring of <paramref name="capacity"/> bytes (a power of two, 4096 to 2^31) for its requests and their replies, each of 1 to <paramref name="maxMessage"/> bytes (1 to capacity / 4), and returns the service's handle: ServiceClose ends the service. The owner's process admits joiners by itself, the owner's own members too. Invalid for a capacity, maxMessage or member count out of range; NoMemory; AddrInUse if another stream, broadcast, queue or service holds the name.</summary>
        [DllImport(Library, EntryPoint = "fipc_service_create", ExactSpelling = true)]
        public static extern FipcResult ServiceCreate([MarshalAs(UnmanagedType.LPUTF8Str)] string name, nuint capacity, nuint maxMessage, uint maxClients, uint maxServers, out IntPtr service);

        /// <summary>Client or server: joins the service on <paramref name="name"/> as <paramref name="role"/>, waiting for the owner to create it, for a free place of the role and for the owner's process to admit the member. Invalid for another role, or a name a broadcast or a queue holds; on a name a stream holds, Invalid once the stream's server accepts the joiner, Timeout while no accept runs.</summary>
        [DllImport(Library, EntryPoint = "fipc_service_join", ExactSpelling = true)]
        public static extern FipcResult ServiceJoin([MarshalAs(UnmanagedType.LPUTF8Str)] string name, FipcServiceRole role, out IntPtr conn, int timeoutMs);

        /// <summary>Owner: waits until at least <paramref name="atLeast"/> members of <paramref name="role"/> are joined (default(FipcServiceRole), 0, counts both roles); <paramref name="count"/> is how many are, at every return but Invalid. Ok, Timeout, Cancelled (after ServiceCancel), Disconnected once the service has ended; Invalid for another role, or for atLeast over the role's maximum (their sum for both). Any thread.</summary>
        [DllImport(Library, EntryPoint = "fipc_service_wait_for_members", ExactSpelling = true)]
        public static extern FipcResult ServiceWaitForMembers(IntPtr service, FipcServiceRole role, uint atLeast, out uint count, int timeoutMs);

        /// <summary>Every ServiceWaitForMembers of the service that waits, now or later, returns Cancelled; the service goes on. Any thread.</summary>
        [DllImport(Library, EntryPoint = "fipc_service_cancel", ExactSpelling = true)]
        public static extern void ServiceCancel(IntPtr service);

        /// <summary>Ends the service and frees the handle: clients' sends and servers' receives return Disconnected, a client receives the replies that came before the end, then Disconnected, and a server's reply returns Disconnected. The members' connections stay open until their Close. No other thread may be inside a call on the service.</summary>
        [DllImport(Library, EntryPoint = "fipc_service_close", ExactSpelling = true)]
        public static extern void ServiceClose(IntPtr service);

        // === Span overloads: lengths are the spans' ===

        public static FipcResult Send(IntPtr conn, ReadOnlySpan<byte> data, int timeoutMs)
        {
            fixed (byte* p = data)
                return Send(conn, p, (nuint)data.Length, timeoutMs);
        }

        /// <summary>Receives one message into <paramref name="buf"/>. TooLarge (the message stays) if it doesn't fit: <paramref name="len"/> is its length. Overrun: <paramref name="len"/> is the number of messages lost, at most int.MaxValue (the pointer overload's nuint holds the whole count).</summary>
        public static FipcResult Recv(IntPtr conn, Span<byte> buf, out int len, int timeoutMs)
        {
            FipcResult result;
            nuint n;
            fixed (byte* p = buf)
                result = Recv(conn, p, (nuint)buf.Length, out n, timeoutMs);
            len = result == FipcResult.Overrun ? (int)Math.Min((ulong)n, int.MaxValue) : checked((int)n);
            return result;
        }
    }
}
