using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

#nullable enable

namespace FastIpc
{
    /// <summary>An RPC message's kind.</summary>
    public enum FipcRpcKind : uint
    {
        Request = 1,
        Response = 2,
    }

    /// <summary>A received RPC message's header; its payload is in the caller's buffer.</summary>
    public readonly struct FipcRpcHeader
    {
        /// <summary>The request's id, echoed in its response.</summary>
        public ulong Id { get; }
        public FipcRpcKind Kind { get; }
        /// <summary>The application's.</summary>
        public uint Opcode { get; }
        /// <summary>The application's; 0 in requests.</summary>
        public int Status { get; }
        /// <summary>The payload's length.</summary>
        public int Length { get; }

        internal FipcRpcHeader(in FipcRpcMsg msg)
        {
            Id = msg.Id;
            Kind = (FipcRpcKind)msg.Kind;
            Opcode = msg.Opcode;
            Status = msg.Status;
            Length = checked((int)msg.Len);
        }
    }

    /// <summary>
    /// One FastIPC connection: two rings in shared memory, one per direction. A server gets one from
    /// <see cref="FipcListener.Accept"/>, a client from <see cref="Connect"/>.
    ///
    /// Results: every call returns a <see cref="FipcResult"/>. Timeouts, the peer's end (Disconnected, final),
    /// Cancelled and TooLarge are normal events, returned, never thrown. Only a caller's mistake throws: a call after
    /// <see cref="Dispose"/> (<see cref="ObjectDisposedException"/>) or a negative length. No call allocates, except
    /// the receives that return a new array.
    ///
    /// Threads: one thread at a time sends (Send, SendAcquire/SendCommit, RpcSubmit, RpcRespond) and one thread at a
    /// time receives (Receive, ReceiveAcquire/ReceiveRelease, RpcReceive); the two may run at once.
    /// <see cref="Cancel"/> and <see cref="Dispose"/> may be called from any thread, at any time.
    ///
    /// Lifetime: the connection is a <see cref="SafeHandle"/> underneath, and every call holds it for its duration.
    /// <see cref="Dispose"/> cancels the connection, which wakes any call that waits (it returns Cancelled), and the
    /// native close runs once the last call in progress has returned: no call ever runs on a closed connection. A
    /// zero-copy span holds the connection the same way, until its commit or release. A connection that is never
    /// disposed is closed by its finalizer.
    ///
    /// Cost: holding the handle is two uncontended atomic operations per call (a zero-copy message's pair of calls
    /// shares them), about 15 ns on the baseline machine; docs/perf/bindings-baseline.md has the numbers. For the
    /// hottest loops the raw layer, <see cref="Fipc"/>, has none of it, and none of its safety.
    /// </summary>
    public sealed unsafe class FipcConnection : IDisposable
    {
        /// <summary>The buffer the array-returning receives try first; a longer message is received straight into its own array.</summary>
        private const int ScratchBytes = 4096;

        private readonly Handle _handle;
        // A zero-copy reservation (sender's) or message (receiver's) holds a reference on the handle until it is
        // committed or released; each flag is used by its own side's thread only.
        private bool _sendHeld;
        private bool _receiveHeld;
        private byte[]? _scratch;

        internal FipcConnection(IntPtr conn)
        {
            _handle = new Handle(conn);
            MaxPiece = (int)Fipc.MaxPiece(conn);
        }

        /// <summary>The longest message the zero-copy calls take: the connection's capacity less 64 bytes.</summary>
        public int MaxPiece { get; }

        /// <summary>
        /// Client: connects to the server listening on <paramref name="name"/> and sets the connection up on the
        /// calling thread, waiting up to <paramref name="timeoutMs"/> for a server to listen (it may start later, or be
        /// serving another client) and to call Accept; the server runs on another thread or in another process.
        /// Timeout if none did; Invalid if the server speaks another version or runs as another user.
        /// </summary>
        /// <param name="connection">The connection when the result is Ok, otherwise null.</param>
        public static FipcResult Connect(string name, out FipcConnection? connection, int timeoutMs)
        {
            if (name == null) throw new ArgumentNullException(nameof(name));
            FipcResult result = Fipc.Connect(name, out IntPtr conn, timeoutMs);
            connection = result == FipcResult.Ok ? new FipcConnection(conn) : null;
            return result;
        }

        // === Messages ===

        /// <summary>
        /// Sends one message (at least 1 byte, any size): waits up to <paramref name="timeoutMs"/> for room for its
        /// first piece, then goes on until the whole message is in (see include/fipc.h, "A message in pieces").
        /// </summary>
        [MethodImpl(MethodImplOptions.NoInlining)] // see "Calls" below
        public FipcResult Send(ReadOnlySpan<byte> message, int timeoutMs)
        {
            IntPtr conn = EnterSend();
            FipcResult result;
            fixed (byte* p = message)
                result = Fipc.Send(conn, p, (nuint)message.Length, timeoutMs);
            _handle.DangerousRelease();
            return result;
        }

        /// <summary>
        /// Receives one message into <paramref name="buffer"/>, waiting up to <paramref name="timeoutMs"/> for it;
        /// <paramref name="length"/> is its length. TooLarge if it doesn't fit: nothing is taken, and
        /// <paramref name="length"/> says how much room it needs.
        /// </summary>
        [MethodImpl(MethodImplOptions.NoInlining)] // see "Calls" below
        public FipcResult Receive(Span<byte> buffer, out int length, int timeoutMs)
        {
            IntPtr conn = EnterReceive();
            FipcResult result;
            nuint n;
            fixed (byte* p = buffer)
                result = Fipc.Recv(conn, p, (nuint)buffer.Length, out n, timeoutMs);
            _handle.DangerousRelease();
            length = checked((int)n);
            return result;
        }

        /// <summary>
        /// Receives one message of any size into a new array of its length, waiting up to
        /// <paramref name="timeoutMs"/> for it. A message up to 4 KiB is copied out of a buffer the connection reuses;
        /// a longer one is received straight into its array. <paramref name="message"/> is empty unless the result
        /// is Ok.
        /// </summary>
        public FipcResult Receive(out byte[] message, int timeoutMs)
        {
            byte[] scratch = _scratch ??= new byte[ScratchBytes];
            FipcResult result = Receive(scratch, out int length, timeoutMs);
            if (result == FipcResult.Ok)
            {
                message = scratch.AsSpan(0, length).ToArray();
                return result;
            }
            if (result == FipcResult.TooLarge)
            {
                // The message stays queued, so this call doesn't wait for its first piece
                var whole = new byte[length];
                result = Receive(whole, out _, timeoutMs);
                if (result == FipcResult.Ok)
                {
                    message = whole;
                    return result;
                }
            }
            message = Array.Empty<byte>();
            return result;
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
        public FipcResult SendAcquire(int length, out Span<byte> buffer, int timeoutMs)
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
        /// range (the reservation stays for a valid one).
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
            FipcResult result = Fipc.SendCommit(_handle.DangerousGetHandle(), (nuint)length);
            if (result == FipcResult.Ok)
            {
                _sendHeld = false;
                _handle.DangerousRelease();
            }
            return result;
        }

        /// <summary>
        /// Zero-copy receive, step 1: waits up to <paramref name="timeoutMs"/> for a message and sets
        /// <paramref name="message"/> to it, in the ring (read-only). It stays valid until <see cref="ReceiveRelease"/>
        /// or the next receive of any kind, even if another thread disposes the connection meanwhile. TooLarge for a
        /// message in several pieces: it stays for <see cref="Receive(Span{byte}, out int, int)"/>.
        /// </summary>
        [MethodImpl(MethodImplOptions.NoInlining)] // see "Calls" below
        public FipcResult ReceiveAcquire(out ReadOnlySpan<byte> message, int timeoutMs)
        {
            IntPtr conn = EnterReceive();
            FipcResult result = Fipc.RecvAcquire(conn, out byte* data, out nuint length, timeoutMs);
            if (result == FipcResult.Ok)
            {
                _receiveHeld = true; // the message keeps the reference until it is released
                message = new ReadOnlySpan<byte>(data, (int)length);
            }
            else
            {
                _handle.DangerousRelease();
                message = default;
            }
            return result;
        }

        /// <summary>Zero-copy receive, step 2: frees the acquired message's room. Without one, a no-op.</summary>
        [MethodImpl(MethodImplOptions.NoInlining)] // see "Calls" below
        public void ReceiveRelease()
        {
            if (!_receiveHeld)
                return;
            _receiveHeld = false;
            Fipc.RecvRelease(_handle.DangerousGetHandle());
            _handle.DangerousRelease();
        }

        // === RPC ===

        /// <summary>
        /// Sends a request with a payload of any size, possibly empty (as <see cref="Send"/>); <paramref name="id"/>
        /// is its id (a connection numbers its requests from 1).
        /// </summary>
        [MethodImpl(MethodImplOptions.NoInlining)] // see "Calls" below
        public FipcResult RpcSubmit(uint opcode, ReadOnlySpan<byte> payload, out ulong id, int timeoutMs)
        {
            IntPtr conn = EnterSend();
            FipcResult result;
            fixed (byte* p = payload)
                result = Fipc.RpcSubmit(conn, opcode, p, (nuint)payload.Length, out id, timeoutMs);
            _handle.DangerousRelease();
            return result;
        }

        /// <summary>Sends the response to request <paramref name="id"/> (as <see cref="Send"/>; the payload may be empty).</summary>
        [MethodImpl(MethodImplOptions.NoInlining)] // see "Calls" below
        public FipcResult RpcRespond(ulong id, uint opcode, int status, ReadOnlySpan<byte> payload, int timeoutMs)
        {
            IntPtr conn = EnterSend();
            FipcResult result;
            fixed (byte* p = payload)
                result = Fipc.RpcRespond(conn, id, opcode, status, p, (nuint)payload.Length, timeoutMs);
            _handle.DangerousRelease();
            return result;
        }

        /// <summary>
        /// Receives one request or response, waiting up to <paramref name="timeoutMs"/>: fills
        /// <paramref name="header"/> and copies the payload into <paramref name="buffer"/>. TooLarge if it doesn't
        /// fit: the header is filled (its Length says how much room the payload needs) and nothing is taken. Invalid
        /// (the message dropped) for a message that isn't a well-formed RPC message.
        /// </summary>
        [MethodImpl(MethodImplOptions.NoInlining)] // see "Calls" below
        public FipcResult RpcReceive(Span<byte> buffer, out FipcRpcHeader header, int timeoutMs)
        {
            IntPtr conn = EnterReceive();
            FipcResult result;
            FipcRpcMsg msg;
            fixed (byte* p = buffer)
                result = Fipc.RpcRecv(conn, p, (nuint)buffer.Length, out msg, timeoutMs);
            _handle.DangerousRelease();
            header = new FipcRpcHeader(msg);
            return result;
        }

        /// <summary>
        /// Receives one request or response with a payload of any size, into a new array of its length (as
        /// <see cref="Receive(out byte[], int)"/>). <paramref name="payload"/> is empty unless the result is Ok.
        /// </summary>
        public FipcResult RpcReceive(out FipcRpcHeader header, out byte[] payload, int timeoutMs)
        {
            byte[] scratch = _scratch ??= new byte[ScratchBytes];
            FipcResult result = RpcReceive(scratch, out header, timeoutMs);
            if (result == FipcResult.Ok)
            {
                payload = header.Length == 0 ? Array.Empty<byte>() : scratch.AsSpan(0, header.Length).ToArray();
                return result;
            }
            if (result == FipcResult.TooLarge)
            {
                // The message stays queued, so this call doesn't wait for its first piece
                var whole = new byte[header.Length];
                result = RpcReceive(whole, out header, timeoutMs);
                if (result == FipcResult.Ok)
                {
                    payload = whole;
                    return result;
                }
            }
            payload = Array.Empty<byte>();
            return result;
        }

        // === Lifetime ===

        /// <summary>
        /// Makes every call of the connection that waits, now or later, return Cancelled; calls that needn't wait
        /// still work. A message cancelled halfway is dropped whole. The peer sees nothing until the connection is
        /// disposed. Final. Any thread; a no-op once disposed.
        /// </summary>
        public void Cancel()
        {
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
        /// progress; the peer then gets Disconnected, after the messages this side completed. Any thread; later calls
        /// throw <see cref="ObjectDisposedException"/>.
        /// </summary>
        public void Dispose()
        {
            Cancel();
            _handle.Dispose();
        }

        // Calls. A call takes a reference on the handle before its native call and releases it after, without
        // try/finally: on x64 the JIT inlines a P/Invoke only outside a try region, and the native calls never throw.
        // A call that threw anyway would leave the handle referenced: the connection would stay open, never closed
        // under a call. The calls are never inlined into their callers either: inlined into a caller's loop, a
        // P/Invoke can go through its marshalling stub instead, which cost about 90 ns per call on Linux (.NET 9).

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
            if (_receiveHeld)
                _receiveHeld = false;
            else
                AddRef();
            return _handle.DangerousGetHandle();
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
