using System;
using System.Buffers;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

#nullable enable

namespace FastIpc
{
    /// <summary>
    /// One FastIPC connection. A stream's: two rings in shared memory, one per direction; a server gets one from
    /// <see cref="FipcListener.Accept(int)"/>, a client from <see cref="FipcStream.Connect"/>. A broadcast's publisher
    /// (<see cref="FipcBroadcast.Create"/>) and its subscribers (<see cref="FipcBroadcast.Join"/>) are connections too:
    /// the publisher sends, a subscriber receives. So are a work queue's producers and consumers
    /// (<see cref="FipcQueue.Join"/>): a producer sends, and a consumer receives, each message going to one consumer.
    /// So are a service's clients and servers (<see cref="FipcService.Join"/>): a client sends requests and receives
    /// their replies, in the order of its requests (or <see cref="Call"/>s), and a server receives the next request of
    /// any client and replies to it with its next send.
    ///
    /// Results: setup throws (the create, connect, join or <see cref="FipcListener.Accept(int)"/> that made the
    /// connection throws <see cref="FipcException"/>), and everything after it returns a <see cref="FipcResult"/>:
    /// every call here does. Timeouts, the peer's end (Disconnected, final), Cancelled, TooLarge, a subscriber's
    /// Overrun and Unsubscribed, and a service client's Abandoned are normal events, returned, never thrown. Only a
    /// caller's mistake throws: a call after <see cref="Dispose"/> (<see cref="ObjectDisposedException"/>) or a
    /// negative length. No call allocates, except the receives that return a new array. The calls that wait take
    /// their timeout last, in milliseconds, <see cref="Fipc.Forever"/> unless given.
    ///
    /// Threads: one thread at a time sends (Send, SendMany, SendAcquire/SendCommit) and one thread at a time receives
    /// (Receive, ReceiveMany, ReceiveAcquire/ReceiveRelease); the two may run at once.
    /// <see cref="Cancel"/> and <see cref="Dispose"/> may be called from any thread, at any time.
    ///
    /// Lifetime: the connection is a <see cref="SafeHandle"/> underneath, and every call holds it for its duration.
    /// <see cref="Dispose"/> cancels the connection, which wakes any call that waits (it returns Cancelled), and the
    /// native close runs once the last call in progress has returned: no call ever runs on a closed connection. A
    /// zero-copy span holds the connection the same way, until its commit or release (a service's server's request
    /// held in place: see <see cref="ReceiveAcquire"/>). A connection that is never disposed is closed by its
    /// finalizer once it is unreachable (and holds no span): a publisher's ends its broadcast so.
    ///
    /// Cost: holding the handle is two uncontended atomic operations per call (a zero-copy message's pair of calls
    /// shares them), about 15 ns on the baseline machine; docs/perf/bindings-baseline.md has the numbers. For the
    /// hottest loops the raw layer, <see cref="Fipc"/>, has none of it, and none of its safety.
    /// </summary>
    public sealed unsafe class FipcConnection : IDisposable
    {
        /// <summary>The buffer the array-returning receives try first; a longer message is received straight into its own array.</summary>
        private const int ScratchBytes = 4096;

        /// <summary>The buffer the array-returning batched receive tries first.</summary>
        private const int BatchBytes = 1 << 16;

        private readonly Handle _handle;
        // A zero-copy reservation (sender's) or message (receiver's) holds a reference on the handle until it is
        // committed or released; each flag is used by its own side's thread only, except on a service's server, whose
        // reply (on any thread) also ends the request it holds in place: there _receiveHeld (1 while held) is taken with
        // an interlocked exchange (TakeReceiveHeld), so that one side alone drops its reference. _receiveThread: the
        // thread that received a server's request in place, whose Dispose ends it.
        private bool _sendHeld;
        private int _receiveHeld;
        private int _receiveThread;
        private readonly bool _serviceServer;
        private byte[]? _scratch;
        private byte[]? _batchScratch;
        private long _lostMessages;
        // A service's client keeps count, oldest first, of the requests whose replies it hasn't received yet, so that
        // Call never takes the reply to another request: first those whose Call gave up on its reply (it timed out or
        // was cancelled), which the next Call receives and drops (at most one, as a Call first drops what is owed),
        // then those sent with Send or SendCommit, whose replies are the caller's to receive. A request is counted
        // before its send, since a thread that receives may take its reply before the send returns, and uncounted if
        // the send fails; a reply or Abandoned takes the oldest off. The sending thread adds to _sendsOwed while the
        // receiving thread takes off, hence the interlocked operations; _callsOwed is Call's, which sends and receives
        // with no other thread doing either, and the receiving thread's.
        private readonly bool _serviceClient;
        private int _callsOwed;
        private int _sendsOwed;
        // Set by Cancel (any thread) before the native cancel: a Call then sends nothing, as it could never receive
        // the reply; one that doesn't see it sent its request before the cancel.
        private volatile bool _cancelled;

        private FipcConnection(IntPtr conn, bool serviceClient, bool serviceServer)
        {
            _handle = new Handle(conn);
            _serviceClient = serviceClient;
            _serviceServer = serviceServer;
            MaxPiece = (int)Fipc.MaxPiece(conn);
        }

        /// <summary>
        /// The object over a connection the library returned (<paramref name="serviceClient"/>: a service's client's,
        /// which keeps count of the replies owed; <paramref name="serviceServer"/>: a service's server's, whose reply
        /// ends the request it holds in place); it closes the connection if the object can't be made.
        /// </summary>
        internal static FipcConnection Wrap(IntPtr conn, bool serviceClient = false, bool serviceServer = false)
        {
            try
            {
                return new FipcConnection(conn, serviceClient, serviceServer);
            }
            catch
            {
                Fipc.Close(conn); // out of memory: no object owns it
                throw;
            }
        }

        /// <summary>
        /// The longest message the zero-copy calls take: the connection's capacity less 64 bytes. A broadcast's or a
        /// work queue's: a quarter of its capacity, the longest message it takes. A service's: its maxMessage, the
        /// longest request or reply.
        /// </summary>
        public int MaxPiece { get; }

        /// <summary>
        /// A lossy subscriber's: how many messages it lost at the last receive that returned
        /// <see cref="FipcResult.Overrun"/> (at least 1); 0 before any. A long: a stalled subscriber of a busy broadcast
        /// can lose more than int.MaxValue. Read it on the receiving thread.
        /// </summary>
        public long LostMessages => _lostMessages;

        /// <summary>
        /// A broadcast's publisher: waits up to <paramref name="timeoutMs"/> until at least <paramref name="atLeast"/>
        /// subscribers are joined (0 returns at once, and a timeout of 0 polls); <paramref name="count"/> is how many
        /// are, at every result but Invalid. Joined: admitted, and not yet left, dropped or unsubscribed. Call it before
        /// the first send, so that nothing is sent before they are there: the publisher's process admits them by
        /// itself. Ok once the count is reached; Timeout; Cancelled after <see cref="Cancel"/>; Invalid on a connection
        /// other than a publisher, or for atLeast over the broadcast's maxSubscribers. A subscriber that left is counted
        /// until the publisher's process has seen its end, soon after. Any thread.
        /// </summary>
        public FipcResult WaitForMembers(int atLeast, out int count, int timeoutMs = Fipc.Forever)
        {
            if (atLeast < 0) throw new ArgumentOutOfRangeException(nameof(atLeast));
            bool added = false;
            try
            {
                _handle.DangerousAddRef(ref added); // ObjectDisposedException once disposed
                FipcResult result = Fipc.BroadcastWaitForMembers(_handle.DangerousGetHandle(), (uint)atLeast, out uint n,
                    timeoutMs);
                count = (int)n;
                return result;
            }
            finally
            {
                if (added) _handle.DangerousRelease();
            }
        }

        // === Messages ===

        /// <summary>
        /// Sends one message (at least 1 byte, any size): waits up to <paramref name="timeoutMs"/> for room for its
        /// first piece, then goes on until the whole message is in (see include/fipc.h, "A message in pieces"). On a
        /// broadcast, a work queue or a service, a message is of one piece: TooLarge over <see cref="MaxPiece"/>. A
        /// work queue's producer waits for room while the consumers haven't taken its earlier messages. A service's
        /// client sends a request, waiting for room in its ring (which its replies not yet received hold too); a
        /// server replies to the request it holds, and never waits (Invalid with none held). A server's reply that
        /// returns Ok or Disconnected ends the request it holds in place (<see cref="ReceiveAcquire"/>).
        /// </summary>
        [MethodImpl(MethodImplOptions.NoInlining)] // see "Calls" below
        public FipcResult Send(ReadOnlySpan<byte> message, int timeoutMs = Fipc.Forever)
        {
            IntPtr conn = EnterSend();
            if (_serviceClient)
                Interlocked.Increment(ref _sendsOwed); // its reply is owed from before the send
            FipcResult result;
            fixed (byte* p = message)
                result = Fipc.Send(conn, p, (nuint)message.Length, timeoutMs);
            _handle.DangerousRelease();
            if (_serviceClient && result != FipcResult.Ok)
                Interlocked.Decrement(ref _sendsOwed);
            if (_serviceServer && (result == FipcResult.Ok || result == FipcResult.Disconnected))
                Replied();
            return result;
        }

        /// <summary>
        /// Receives one message into <paramref name="buffer"/>, waiting up to <paramref name="timeoutMs"/> for it;
        /// <paramref name="length"/> is its length. TooLarge if it doesn't fit: nothing is taken, and
        /// <paramref name="length"/> says how much room it needs. Overrun (a lossy subscriber that fell a lap behind):
        /// no message, and <paramref name="length"/> is how many were lost, at most int.MaxValue (the whole count is
        /// <see cref="LostMessages"/>); the next receive goes on from a recent message. A work queue's consumer takes
        /// the next message of any producer; after TooLarge another consumer may take that message first. A
        /// service's client receives the reply to its oldest request whose reply it hasn't received, or Abandoned (and
        /// <paramref name="length"/> 0) if that request's server ended before it replied: the request counts as
        /// answered, and it may or may not have been carried out. A receive that times out or is cancelled leaves the
        /// reply to come, for the next receive; after a <see cref="Call(ReadOnlySpan{byte}, out byte[], int)"/> that
        /// gave up, that call's late reply (or Abandoned) comes first, as replies come in request order. A server takes
        /// the next request of any client and holds it until it replies: its receives return Invalid until then.
        /// </summary>
        [MethodImpl(MethodImplOptions.NoInlining)] // see "Calls" below
        public FipcResult Receive(Span<byte> buffer, out int length, int timeoutMs = Fipc.Forever)
        {
            IntPtr conn = EnterReceive();
            FipcResult result;
            nuint n;
            fixed (byte* p = buffer)
                result = Fipc.Recv(conn, p, (nuint)buffer.Length, out n, timeoutMs);
            _handle.DangerousRelease();
            if (_serviceClient && (result == FipcResult.Ok || result == FipcResult.Abandoned))
                Answered();
            if (result == FipcResult.Overrun)
            {
                _lostMessages = (long)Math.Min((ulong)n, long.MaxValue);
                length = (int)Math.Min((ulong)n, int.MaxValue);
            }
            else
            {
                length = checked((int)n);
            }
            return result;
        }

        /// <summary>
        /// Receives one message of any size into a new array of its length, waiting up to
        /// <paramref name="timeoutMs"/> for it. A message up to 4 KiB is copied out of a buffer the connection reuses;
        /// a longer one is received straight into its array. <paramref name="message"/> is empty unless the result
        /// is Ok. Overrun: <see cref="LostMessages"/> says how many messages were lost. On a work queue another
        /// consumer may take a longer message first: the call then receives the next one, within what is left of
        /// <paramref name="timeoutMs"/>. A service's client receives its next reply, or Abandoned, as the span
        /// receive does.
        /// </summary>
        public FipcResult Receive(out byte[] message, int timeoutMs = Fipc.Forever)
        {
            long started = Stopwatch.GetTimestamp();
            byte[] scratch = _scratch ??= new byte[ScratchBytes];
            FipcResult result = Receive(scratch, out int length, timeoutMs);
            if (result == FipcResult.Ok)
            {
                message = scratch.AsSpan(0, length).ToArray();
                return result;
            }
            while (result == FipcResult.TooLarge)
            {
                // The message stays queued, so this receive doesn't wait for it, unless another consumer of a work
                // queue took it first: then it waits for the next, which may be longer still
                var whole = new byte[length];
                result = Receive(whole, out length, Left(timeoutMs, started));
                if (result == FipcResult.Ok)
                {
                    message = length == whole.Length ? whole : whole.AsSpan(0, length).ToArray();
                    return result;
                }
            }
            message = Array.Empty<byte>();
            return result;
        }

        // === Batches ===

        /// <summary>
        /// Sends <paramref name="messages"/> (at least one, each of at least 1 byte) in order in one native call and
        /// publishes them together, with at most one wake of the receivers for the batch; <paramref name="timeoutMs"/>
        /// bounds the whole call (NoWait: only what fits now). <paramref name="sent"/> is how many went, on every
        /// return: Ok once all went; any other result is the first message that didn't go, as <see cref="Send"/>
        /// would return it, and every one before it went. Every message is checked first, before any goes: Invalid for
        /// an empty one or an empty batch, TooLarge over <see cref="MaxPiece"/> on a broadcast, a work queue or a
        /// service; on a stream a longer message goes in pieces, as Send sends it. A service's client sends a batch of
        /// requests, whose replies come in their order and are owed as Send's are; a server's batch is Invalid. Batch
        /// both ends: a receiver that takes one message per call falls behind a batching sender, and the two run slower
        /// than without batches.
        /// </summary>
        public FipcResult SendMany(ReadOnlySpan<ReadOnlyMemory<byte>> messages, out int sent,
            int timeoutMs = Fipc.Forever)
        {
            var batch = new Batch(messages.Length);
            try
            {
                for (int i = 0; i < messages.Length; i++)
                    batch.Pin(i, messages[i]);
                return SendBatch(batch, out sent, timeoutMs);
            }
            finally
            {
                batch.Dispose();
            }
        }

        /// <summary>
        /// <see cref="SendMany(ReadOnlySpan{ReadOnlyMemory{byte}}, out int, int)"/> for arrays (a byte[][], or a
        /// List of byte[] through CollectionsMarshal.AsSpan); a null array is an empty message.
        /// </summary>
        public FipcResult SendMany(ReadOnlySpan<byte[]> messages, out int sent, int timeoutMs = Fipc.Forever)
        {
            var batch = new Batch(messages.Length);
            try
            {
                for (int i = 0; i < messages.Length; i++)
                    batch.Pin(i, messages[i]);
                return SendBatch(batch, out sent, timeoutMs);
            }
            finally
            {
                batch.Dispose();
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)] // see "Calls" below
        private FipcResult SendBatch(in Batch batch, out int sent, int timeoutMs)
        {
            IntPtr conn = EnterSend();
            if (_serviceClient)
                Interlocked.Add(ref _sendsOwed, batch.Count); // their replies are owed from before the send
            FipcResult result;
            nuint went;
            fixed (IntPtr* datas = batch.Datas)
            fixed (nuint* lens = batch.Lens)
                result = Fipc.SendMany(conn, (byte**)datas, lens, (nuint)batch.Count, out went, timeoutMs);
            _handle.DangerousRelease();
            sent = (int)went;
            if (_serviceClient && sent < batch.Count)
                Interlocked.Add(ref _sendsOwed, sent - batch.Count); // nothing is owed to those that didn't go
            return result;
        }

        /// <summary>
        /// Waits up to <paramref name="timeoutMs"/> for the next message, as <see cref="Receive(Span{byte}, out int, int)"/>,
        /// then takes every further message already there that fits the rest of <paramref name="buffer"/>, up to
        /// <paramref name="lengths"/>.Length (at least 1), packed back to back: message i is lengths[i] bytes, at the sum
        /// of the lengths before it. <paramref name="count"/> is how many it took: 1 or more with Ok, 0 with any other
        /// result, which is the first message's, as Receive returns it: TooLarge (lengths[0] is the room it needs; it
        /// stays queued), Overrun (lengths[0] is how many were lost, at most int.MaxValue; the whole count is
        /// <see cref="LostMessages"/>), a service client's Abandoned (lengths[0] is 0). After the first it never waits,
        /// and stops before a message that isn't there yet, doesn't fit, is in pieces, or would bring another result:
        /// the next receive returns it. A broadcast's subscriber takes, after its first, only the messages its publisher
        /// has moved past (the newest come in the next calls); a work queue's consumer acknowledges each message before
        /// the call returns; a service's client receives its replies in its requests' order. Invalid on a publisher, a
        /// producer and a service's server, which holds one request at a time.
        /// </summary>
        [MethodImpl(MethodImplOptions.NoInlining)] // see "Calls" below
        public FipcResult ReceiveMany(Span<byte> buffer, Span<int> lengths, out int count, int timeoutMs = Fipc.Forever)
        {
            nuint[]? rented = null;
            Span<nuint> lens = lengths.Length <= 64
                ? stackalloc nuint[64]
                : (rented = ArrayPool<nuint>.Shared.Rent(lengths.Length));
            IntPtr conn = EnterReceive();
            FipcResult result;
            nuint n;
            fixed (byte* p = buffer)
            fixed (nuint* l = lens)
                result = Fipc.RecvMany(conn, p, (nuint)buffer.Length, l, (nuint)lengths.Length, out n, timeoutMs);
            _handle.DangerousRelease();
            count = (int)n;
            for (int i = 0; i < count; i++)
            {
                lengths[i] = (int)lens[i];
                if (_serviceClient)
                    Answered();
            }
            if (result == FipcResult.Overrun)
            {
                _lostMessages = (long)Math.Min((ulong)lens[0], long.MaxValue);
                lengths[0] = (int)Math.Min((ulong)lens[0], int.MaxValue);
            }
            else if (result == FipcResult.TooLarge || result == FipcResult.Abandoned)
            {
                lengths[0] = checked((int)lens[0]);
                if (_serviceClient && result == FipcResult.Abandoned)
                    Answered();
            }
            if (rented != null)
                ArrayPool<nuint>.Shared.Return(rented);
            return result;
        }

        /// <summary>
        /// Receives a batch, as <see cref="ReceiveMany(Span{byte}, Span{int}, out int, int)"/>, of up to
        /// <paramref name="maxCount"/> messages (at least 1), each into a new array of its length, in order
        /// (<paramref name="messages"/> is empty unless the result is Ok). The batch takes the messages that fit a
        /// 64 KiB buffer the connection reuses; a longer first message is received straight into its own array, alone.
        /// </summary>
        public FipcResult ReceiveMany(out byte[][] messages, int maxCount, int timeoutMs = Fipc.Forever)
        {
            if (maxCount < 0) throw new ArgumentOutOfRangeException(nameof(maxCount));
            long started = Stopwatch.GetTimestamp();
            byte[] buffer = _batchScratch ??= new byte[BatchBytes];
            Span<int> lengths = maxCount <= 64 ? stackalloc int[64] : new int[maxCount];
            lengths = lengths.Slice(0, maxCount);
            FipcResult result = ReceiveMany(buffer, lengths, out int count, timeoutMs);
            while (result == FipcResult.TooLarge)
            {
                // The first message stays queued, so this receive doesn't wait for it, unless another consumer of a
                // work queue took it first: then it waits for the next, which may be longer still
                buffer = new byte[lengths[0]];
                result = ReceiveMany(buffer, lengths, out count, Left(timeoutMs, started));
            }
            if (result != FipcResult.Ok)
            {
                messages = Array.Empty<byte[]>();
                return result;
            }
            messages = new byte[count][];
            int at = 0;
            for (int i = 0; i < count; i++)
            {
                messages[i] = buffer.AsSpan(at, lengths[i]).ToArray();
                at += lengths[i];
            }
            return result;
        }

        /// <summary>
        /// A batch's native arrays, rented: each message's address and length, and the pin that holds it in place for
        /// the call.
        /// </summary>
        private readonly struct Batch
        {
            public readonly int Count;
            public readonly IntPtr[] Datas;
            public readonly nuint[] Lens;
            private readonly MemoryHandle[] _pins;

            public Batch(int count)
            {
                Count = count;
                Datas = ArrayPool<IntPtr>.Shared.Rent(count);
                Lens = ArrayPool<nuint>.Shared.Rent(count);
                _pins = ArrayPool<MemoryHandle>.Shared.Rent(count);
                Array.Clear(_pins, 0, count); // a rented array may hold another renter's handles
            }

            public void Pin(int i, ReadOnlyMemory<byte> message)
            {
                _pins[i] = message.Pin();
                Datas[i] = (IntPtr)_pins[i].Pointer;
                Lens[i] = (nuint)message.Length;
            }

            /// <summary>Unpins the messages (a pin never made is a default handle, whose Dispose does nothing) and returns the arrays.</summary>
            public void Dispose()
            {
                for (int i = 0; i < Count; i++)
                    _pins[i].Dispose();
                ArrayPool<MemoryHandle>.Shared.Return(_pins, clearArray: true);
                ArrayPool<nuint>.Shared.Return(Lens);
                ArrayPool<IntPtr>.Shared.Return(Datas);
            }
        }

        /// <summary>
        /// A service's client: sends <paramref name="request"/> and receives its reply into a new array
        /// (<paramref name="reply"/>, empty unless the result is Ok), waiting up to <paramref name="timeoutMs"/> in
        /// all; Invalid on any other connection. Abandoned if the server that took the request ended before it
        /// replied (the request counts as answered; it may or may not have been carried out, and it is never sent
        /// again). A call checks its request first: Invalid for an empty one, TooLarge over <see cref="MaxPiece"/>,
        /// at once, even while a reply is owed. Then a call on a cancelled connection returns Cancelled at once,
        /// whatever its timeout, and sends nothing: it could never receive the reply. A call never returns the reply
        /// to another request: the connection keeps count of the replies still owed, and a call whose receive timed
        /// out or was cancelled leaves its reply owed, which the next call first receives and drops (at most one is
        /// owed, since every call drops it first; an Abandoned in its place counts too), within its own timeout and
        /// before it sends. A call that had to drop an owed reply first sends its request only if its timeout has time
        /// left: once a positive timeout has run out, it returns Timeout without sending, so no server sees that
        /// request; with NoWait (0) no step waits, and it sends if there is room. After a call that gave up, a plain
        /// <see cref="Receive(Span{byte}, out int, int)"/> returns that call's late reply (or Abandoned) first:
        /// replies come in request order. A call returns Invalid while a request sent with <see cref="Send"/> or
        /// <see cref="SendCommit"/> still has its reply to be received: that reply is the caller's, so receive it
        /// first. A call sends and receives, so no other thread may send or receive on the connection meanwhile.
        /// </summary>
        public FipcResult Call(ReadOnlySpan<byte> request, out byte[] reply, int timeoutMs = Fipc.Forever) =>
            Call(request, out reply, timeoutMs, Stopwatch.GetTimestamp());

        /// <summary><see cref="Call(ReadOnlySpan{byte}, out byte[], int)"/>, its timeout counted from
        /// <paramref name="started"/>, a <see cref="Stopwatch.GetTimestamp"/> (the tests' way to a deadline already
        /// past).</summary>
        internal FipcResult Call(ReadOnlySpan<byte> request, out byte[] reply, int timeoutMs, long started)
        {
            reply = Array.Empty<byte>();
            if (_handle.IsClosed) throw new ObjectDisposedException(nameof(FipcConnection));
            if (!_serviceClient)
                return FipcResult.Invalid;
            if (request.Length == 0)
                return FipcResult.Invalid;
            if (request.Length > MaxPiece)
                return FipcResult.TooLarge;
            if (_cancelled)
                return FipcResult.Cancelled; // its reply could never be received: the request never goes
            if (Volatile.Read(ref _sendsOwed) != 0)
                return FipcResult.Invalid; // a reply to a request of Send's is the caller's, not the call's to drop
            bool dropped = false;
            while (_callsOwed > 0) // replies owed to calls that gave up on them: dropped
            {
                FipcResult drop = Receive(out byte[] _, Left(timeoutMs, started));
                if (drop != FipcResult.Ok && drop != FipcResult.Abandoned)
                    return drop;
                dropped = true;
            }
            // Its deadline ran out while it dropped: the request never goes (a server that saw it would do its work
            // for a call that has already returned Timeout)
            if (dropped && timeoutMs > 0 && Left(timeoutMs, started) == 0)
                return FipcResult.Timeout;
            if (_cancelled)
                return FipcResult.Cancelled; // cancelled while it dropped: the request never goes
            IntPtr conn = EnterSend();
            _callsOwed = 1; // its reply is owed from before the send
            FipcResult result;
            fixed (byte* p = request)
                result = Fipc.Send(conn, p, (nuint)request.Length, Left(timeoutMs, started));
            _handle.DangerousRelease();
            if (result != FipcResult.Ok)
            {
                _callsOwed = 0;
                return result;
            }
            return Receive(out reply, Left(timeoutMs, started)); // a reply or Abandoned takes it off; else it stays owed
        }

        /// <summary>A service client's receive returned a reply or Abandoned: the oldest request owed one is answered.</summary>
        private void Answered()
        {
            if (_callsOwed > 0)
                _callsOwed--;
            else if (Volatile.Read(ref _sendsOwed) > 0)
                Interlocked.Decrement(ref _sendsOwed);
        }

        /// <summary>What is left of <paramref name="timeoutMs"/> since <paramref name="started"/> (Forever stays Forever), at least 0.</summary>
        private static int Left(int timeoutMs, long started)
        {
            if (timeoutMs < 0)
                return timeoutMs;
            long elapsedMs = (Stopwatch.GetTimestamp() - started) * 1000 / Stopwatch.Frequency;
            return (int)Math.Max(0, timeoutMs - elapsedMs);
        }

        // === Zero-copy: messages of one piece (up to MaxPiece bytes) ===

        /// <summary>
        /// Zero-copy send, step 1: waits up to <paramref name="timeoutMs"/> for <paramref name="length"/> (1 to
        /// <see cref="MaxPiece"/>) contiguous bytes in the ring; write the message into <paramref name="buffer"/>,
        /// then <see cref="SendCommit"/>. The next send of any kind drops a reservation that was never committed.
        /// TooLarge over MaxPiece: send it with <see cref="Send"/>. The room is contiguous: a reservation that doesn't
        /// fit before the ring's end waits until the receiver has read past the end, even in an empty ring, so a long
        /// one (near MaxPiece) can wait for the peer's next receive call.
        /// </summary>
        [MethodImpl(MethodImplOptions.NoInlining)] // see "Calls" below
        public FipcResult SendAcquire(int length, out Span<byte> buffer, int timeoutMs = Fipc.Forever)
        {
            if (length < 0) throw new ArgumentOutOfRangeException(nameof(length));
            IntPtr conn = EnterSend();
            FipcResult result = Fipc.SendAcquire(conn, (nuint)length, out byte* room, timeoutMs);
            if (result == FipcResult.Ok)
            {
                _sendHeld = true; // the reservation keeps the reference until it is committed or dropped
                buffer = new Span<byte>(room, length);
            }
            else
            {
                _handle.DangerousRelease();
                buffer = default;
            }
            return result;
        }

        /// <summary>
        /// Zero-copy send, step 2: sends the first <paramref name="length"/> bytes (1 to the acquired length) of the
        /// acquired buffer as one message. Doesn't wait. Invalid without a reservation, or for a length out of that
        /// range (the reservation stays for a valid one); any other result, Disconnected included, uses the
        /// reservation up. A service's client sends a request so, and a server its reply, which, if it returns Ok or
        /// Disconnected, ends the request the server holds in place.
        /// </summary>
        [MethodImpl(MethodImplOptions.NoInlining)] // see "Calls" below
        public FipcResult SendCommit(int length)
        {
            if (length < 0) throw new ArgumentOutOfRangeException(nameof(length));
            if (!_sendHeld)
            {
                if (_handle.IsClosed) throw new ObjectDisposedException(nameof(FipcConnection));
                return FipcResult.Invalid;
            }
            if (_serviceClient)
                Interlocked.Increment(ref _sendsOwed); // its reply is owed from before the commit
            FipcResult result = Fipc.SendCommit(_handle.DangerousGetHandle(), (nuint)length);
            if (result != FipcResult.Invalid) // the library used the reservation up (Disconnected too): its reference goes
            {
                _sendHeld = false;
                _handle.DangerousRelease();
            }
            if (_serviceClient && result != FipcResult.Ok)
                Interlocked.Decrement(ref _sendsOwed);
            if (_serviceServer && (result == FipcResult.Ok || result == FipcResult.Disconnected))
                Replied();
            return result;
        }

        /// <summary>
        /// Zero-copy receive, step 1: waits up to <paramref name="timeoutMs"/> for a message and sets
        /// <paramref name="message"/> to it, in the ring (read-only). It stays valid until <see cref="ReceiveRelease"/>
        /// or the next receive of any kind, even if another thread disposes the connection meanwhile. TooLarge for a
        /// message in several pieces: it stays for <see cref="Receive(Span{byte}, out int, int)"/>. On a broadcast, a
        /// gated subscriber's only (Invalid for a lossy one): the message holds the publisher back until its
        /// release, and becomes unreadable (reading it faults) if the subscriber is unsubscribed meanwhile. A work
        /// queue's consumer takes the message: it holds its producer back once the producer is a ring ahead of it. A
        /// service's client takes its next reply in place, or Abandoned, as the span receive does.
        ///
        /// A service's server takes the next request in place, and the span ends at the first of: the reply (a
        /// <see cref="Send"/> or <see cref="SendCommit"/> that returns Ok or Disconnected, on any thread),
        /// <see cref="ReceiveRelease"/>, the next receive, or <see cref="Dispose"/> on this thread. Don't read it after
        /// that: the client reuses the room, and a disposed connection's memory goes away. The span holds the
        /// connection open until then, as every span does: after a Dispose on another thread the calls still work
        /// (cancelled: one that would wait returns Cancelled; the reply, which never waits, still goes), and the
        /// connection closes at the span's end (the request, if not replied to, is answered Abandoned then). A
        /// connection dropped while it holds a request in place is never closed.
        /// </summary>
        [MethodImpl(MethodImplOptions.NoInlining)] // see "Calls" below
        public FipcResult ReceiveAcquire(out ReadOnlySpan<byte> message, int timeoutMs = Fipc.Forever)
        {
            IntPtr conn = EnterReceive();
            FipcResult result = Fipc.RecvAcquire(conn, out byte* data, out nuint length, timeoutMs);
            if (result == FipcResult.Ok)
            {
                if (_serviceServer)
                    _receiveThread = Environment.CurrentManagedThreadId;
                _receiveHeld = 1; // the message keeps the reference until it is released
                message = new ReadOnlySpan<byte>(data, (int)length);
            }
            else
            {
                _handle.DangerousRelease();
                message = default;
            }
            if (_serviceClient && (result == FipcResult.Ok || result == FipcResult.Abandoned))
                Answered();
            return result;
        }

        /// <summary>
        /// Zero-copy receive, step 2: frees the acquired message's room, and the span ends. Without one, a no-op. On a
        /// service's server the span ends (and with it the hold on the connection) but the request stays held until its
        /// reply, which a copying Send can still make; a reply ends the span by itself.
        /// </summary>
        [MethodImpl(MethodImplOptions.NoInlining)] // see "Calls" below
        public void ReceiveRelease()
        {
            if (!TakeReceiveHeld())
                return;
            Fipc.RecvRelease(_handle.DangerousGetHandle());
            _handle.DangerousRelease();
        }

        // === Lifetime ===

        /// <summary>
        /// Makes every call of the connection that waits, now or later, return Cancelled (a publisher's
        /// <see cref="WaitForMembers"/> too); calls that needn't wait still work. A message cancelled halfway is dropped
        /// whole. The peer sees nothing until the connection is disposed. Final. Any thread; a no-op once disposed. A
        /// service client's <see cref="Call(ReadOnlySpan{byte}, out byte[], int)"/>, which waits for its reply,
        /// returns Cancelled at once from then on, sending nothing.
        /// </summary>
        public void Cancel()
        {
            _cancelled = true; // first: a Call that doesn't see it sent its request before the cancel
            bool added = false;
            try
            {
                _handle.DangerousAddRef(ref added);
                Fipc.Cancel(_handle.DangerousGetHandle());
            }
            catch (ObjectDisposedException)
            {
            }
            finally
            {
                if (added) _handle.DangerousRelease();
            }
        }

        /// <summary>
        /// Ends the connection: cancels it (a call that waits returns Cancelled) and closes it once no call is in
        /// progress; the peer then gets Disconnected, after the messages this side completed. A broadcast's publisher
        /// ends the broadcast so, and frees its name; a subscriber leaves it. A work queue's producer or consumer leaves
        /// the queue, which goes on (the queue's <see cref="FipcQueue.Dispose"/> ends it). A service's client or server
        /// leaves the service, which goes on (the service's <see cref="FipcService.Dispose"/> ends it): a client's
        /// requests that no server took are dropped, and the replies to those taken are lost; the request a server
        /// holds is answered Abandoned. A span still held keeps the connection open until its end (see
        /// <see cref="ReceiveAcquire"/>), except a server's request held in place on the thread that disposes: that
        /// span ends here. Any thread; later calls throw <see cref="ObjectDisposedException"/>.
        /// </summary>
        public void Dispose()
        {
            Cancel();
            _handle.Dispose();
            // A server's request held in place by this thread: its span ends here, so the close goes on (Abandoned)
            if (_serviceServer && Volatile.Read(ref _receiveHeld) != 0 && _receiveThread == Environment.CurrentManagedThreadId)
                Replied();
        }

        // Calls. A call takes a reference on the handle before its native call and releases it after, without
        // try/finally: on x64 the JIT inlines a P/Invoke only outside a try region, and the native calls never throw.
        // A call that threw anyway would leave the handle referenced: the connection would stay open, never closed
        // under a call. The calls are never inlined into their callers either: inlined into a caller's loop, a
        // P/Invoke can go through its marshalling stub instead, which costs about 90 ns per call on Linux (.NET 9).

        /// <summary>A send-side call's reference on the handle: a zero-copy reservation's, which the call drops, or a new one.</summary>
        private IntPtr EnterSend()
        {
            if (_sendHeld)
                _sendHeld = false;
            else
                AddRef();
            return _handle.DangerousGetHandle();
        }

        /// <summary>A receive-side call's reference on the handle: an acquired message's, which the call releases, or a new one.</summary>
        private IntPtr EnterReceive()
        {
            if (!TakeReceiveHeld())
                AddRef();
            return _handle.DangerousGetHandle();
        }

        /// <summary>
        /// Takes the acquired message's reference, if one is held: true if this call took it. On a server, an
        /// interlocked exchange: its reply, on another thread, may take it at the same time.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool TakeReceiveHeld()
        {
            if (_receiveHeld == 0)
                return false;
            if (_serviceServer)
                return Interlocked.Exchange(ref _receiveHeld, 0) != 0;
            _receiveHeld = 0;
            return true;
        }

        /// <summary>
        /// A server's reply was made (or met the service's end), or its Dispose came: the request it held in place is no
        /// longer the receive side's, and its reference goes, so that the connection can close.
        /// </summary>
        private void Replied()
        {
            if (TakeReceiveHeld())
                _handle.DangerousRelease();
        }

        private void AddRef()
        {
            bool added = false;
            _handle.DangerousAddRef(ref added); // ObjectDisposedException once disposed
        }

        /// <summary>fipc_conn_t*: closed when the last reference is released.</summary>
        private sealed class Handle : SafeHandle
        {
            public Handle(IntPtr conn) : base(IntPtr.Zero, ownsHandle: true) => SetHandle(conn);

            public override bool IsInvalid => handle == IntPtr.Zero;

            protected override bool ReleaseHandle()
            {
                Fipc.Close(handle);
                return true;
            }
        }
    }
}
