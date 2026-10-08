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
        /// <summary>Cancel was called, and the call would have to wait.</summary>
        Cancelled = 3,
        /// <summary>The message doesn't fit what the call can take; it stays queued, and its length is reported.</summary>
        TooLarge = 4,
        /// <summary>A bad argument or handle, a call out of order, a peer with another version or user.</summary>
        Invalid = 5,
        /// <summary>Memory or another OS resource ran out (setting a connection up).</summary>
        NoMemory = 6,
        /// <summary>Listen: another listener holds the name.</summary>
        AddrInUse = 7,
    }

    /// <summary>fipc_rpc_msg_t: a received RPC message (32 bytes); its payload is in the caller's buffer.</summary>
    [StructLayout(LayoutKind.Sequential)]
    public struct FipcRpcMsg
    {
        /// <summary>The request's id, echoed in its response.</summary>
        public ulong Id;
        /// <summary><see cref="Fipc.RpcRequest"/> or <see cref="Fipc.RpcResponse"/>.</summary>
        public uint Kind;
        public uint Opcode;
        public int Status;
        public uint Reserved;
        /// <summary>The payload's length.</summary>
        public ulong Len;
    }

    /// <summary>
    /// The FastIPC C API (include/fipc.h), as thin P/Invoke declarations: the raw layer, with nothing between the
    /// caller and the library. <see cref="FipcListener"/> and <see cref="FipcConnection"/> are the safe layer over it;
    /// this one is the escape hatch for the hottest loops.
    ///
    /// A server listens on a name and accepts one client at a time; a client connects to the name. Handles are opaque
    /// pointers (a listener, a connection); timeouts are int milliseconds, the last parameter: <see cref="NoWait"/>
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
        public const uint RpcRequest = 1;
        public const uint RpcResponse = 2;

        /// <summary>The library's name for a result ("FIPC_OK", ...).</summary>
        public static string ResultStr(FipcResult result) => Marshal.PtrToStringAnsi(fipc_result_str(result)) ?? "";

        [DllImport(Library, ExactSpelling = true)]
        private static extern IntPtr fipc_result_str(FipcResult result);

        // === Connections ===

        /// <summary>Server: claims <paramref name="name"/> and listens on it, with rings of <paramref name="capacity"/> bytes. AddrInUse if another listener holds it.</summary>
        [DllImport(Library, EntryPoint = "fipc_listen", ExactSpelling = true)]
        public static extern FipcResult Listen([MarshalAs(UnmanagedType.LPUTF8Str)] string name, nuint capacity, out IntPtr listener);

        /// <summary>Server: waits for a client and sets its connection up on the calling thread; a call that times out in the middle of a client's setup keeps it for the next call, so a timeout of 0 polls. Invalid while the connection it accepted last is open; NoMemory once the listener has failed for good.</summary>
        [DllImport(Library, EntryPoint = "fipc_accept", ExactSpelling = true)]
        public static extern FipcResult Accept(IntPtr listener, out IntPtr conn, int timeoutMs);

        /// <summary>Every Accept that waits, now or later, returns Cancelled; a client whose setup an Accept began is dropped. Any thread.</summary>
        [DllImport(Library, EntryPoint = "fipc_listener_cancel", ExactSpelling = true)]
        public static extern void ListenerCancel(IntPtr listener);

        /// <summary>Stops listening; the connections it accepted stay open.</summary>
        [DllImport(Library, EntryPoint = "fipc_listener_close", ExactSpelling = true)]
        public static extern void ListenerClose(IntPtr listener);

        /// <summary>Client: connects to the server on <paramref name="name"/> and sets the connection up on the calling thread, waiting for it to listen and to call Accept: the server runs on another thread or in another process.</summary>
        [DllImport(Library, EntryPoint = "fipc_connect", ExactSpelling = true)]
        public static extern FipcResult Connect([MarshalAs(UnmanagedType.LPUTF8Str)] string name, out IntPtr conn, int timeoutMs);

        /// <summary>The largest message the zero-copy calls take on the connection: its capacity less 64 bytes. Any thread.</summary>
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

        /// <summary>Receives one message into <paramref name="buf"/>. TooLarge (the message stays) if it doesn't fit: <paramref name="len"/> is its length.</summary>
        [DllImport(Library, EntryPoint = "fipc_recv", ExactSpelling = true)]
        public static extern FipcResult Recv(IntPtr conn, byte* buf, nuint bufLen, out nuint len, int timeoutMs);

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

        // === RPC ===

        /// <summary>Sends a request with a payload of any size, possibly empty; <paramref name="id"/> is its id.</summary>
        [DllImport(Library, EntryPoint = "fipc_rpc_submit", ExactSpelling = true)]
        public static extern FipcResult RpcSubmit(IntPtr conn, uint opcode, byte* data, nuint len, out ulong id, int timeoutMs);

        /// <summary>Sends the response to request <paramref name="id"/>.</summary>
        [DllImport(Library, EntryPoint = "fipc_rpc_respond", ExactSpelling = true)]
        public static extern FipcResult RpcRespond(IntPtr conn, ulong id, uint opcode, int status, byte* data, nuint len, int timeoutMs);

        /// <summary>Receives one request or response, its payload into <paramref name="buf"/>. TooLarge (nothing taken) if msg.Len exceeds bufLen.</summary>
        [DllImport(Library, EntryPoint = "fipc_rpc_recv", ExactSpelling = true)]
        public static extern FipcResult RpcRecv(IntPtr conn, byte* buf, nuint bufLen, out FipcRpcMsg msg, int timeoutMs);

        // === Span overloads: lengths are the spans' ===

        public static FipcResult Send(IntPtr conn, ReadOnlySpan<byte> data, int timeoutMs)
        {
            fixed (byte* p = data)
                return Send(conn, p, (nuint)data.Length, timeoutMs);
        }

        /// <summary>Receives one message into <paramref name="buf"/>. TooLarge (the message stays) if it doesn't fit: <paramref name="len"/> is its length.</summary>
        public static FipcResult Recv(IntPtr conn, Span<byte> buf, out int len, int timeoutMs)
        {
            FipcResult result;
            nuint n;
            fixed (byte* p = buf)
                result = Recv(conn, p, (nuint)buf.Length, out n, timeoutMs);
            len = checked((int)n);
            return result;
        }

        public static FipcResult RpcSubmit(IntPtr conn, uint opcode, ReadOnlySpan<byte> data, out ulong id, int timeoutMs)
        {
            fixed (byte* p = data)
                return RpcSubmit(conn, opcode, p, (nuint)data.Length, out id, timeoutMs);
        }

        public static FipcResult RpcRespond(IntPtr conn, ulong id, uint opcode, int status, ReadOnlySpan<byte> data, int timeoutMs)
        {
            fixed (byte* p = data)
                return RpcRespond(conn, id, opcode, status, p, (nuint)data.Length, timeoutMs);
        }

        public static FipcResult RpcRecv(IntPtr conn, Span<byte> buf, out FipcRpcMsg msg, int timeoutMs)
        {
            fixed (byte* p = buf)
                return RpcRecv(conn, p, (nuint)buf.Length, out msg, timeoutMs);
        }
    }
}
