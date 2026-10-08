using System;
using System.Runtime.InteropServices;

#nullable enable

namespace FastIpc
{
    /// <summary>
    /// A server's listener: it holds a name, and <see cref="Accept"/> sets up one client's connection at a time, on the
    /// calling thread. The connections it accepted are independent of it: they stay open when it is disposed.
    ///
    /// Results are returned as <see cref="FipcResult"/>s, as on <see cref="FipcConnection"/>. Threads: one thread at
    /// a time accepts; <see cref="Cancel"/> and <see cref="Dispose"/> may be called from any thread, at any time.
    /// Lifetime: a <see cref="SafeHandle"/> underneath, as a connection's; Dispose cancels the listener, and the native
    /// close runs once an Accept in progress has returned.
    /// </summary>
    public sealed class FipcListener : IDisposable
    {
        private readonly Handle _handle;

        private FipcListener(IntPtr listener) => _handle = new Handle(listener);

        /// <summary>
        /// Claims <paramref name="name"/> and listens on it, with rings of <paramref name="capacity"/> bytes (a power
        /// of two from 1024 to 2^30) for each connection. Doesn't wait. AddrInUse if another listener holds the name;
        /// Invalid for a bad name or capacity.
        /// </summary>
        /// <param name="listener">The listener when the result is Ok, otherwise null.</param>
        public static FipcResult Listen(string name, int capacity, out FipcListener? listener)
        {
            if (name == null) throw new ArgumentNullException(nameof(name));
            if (capacity < 0) throw new ArgumentOutOfRangeException(nameof(capacity));
            FipcResult result = Fipc.Listen(name, (nuint)capacity, out IntPtr handle);
            listener = result == FipcResult.Ok ? new FipcListener(handle) : null;
            return result;
        }

        /// <summary>
        /// Waits up to <paramref name="timeoutMs"/> for a client, sets its connection up on the calling thread and
        /// returns it; it may already hold the client's first messages. A call that times out in the middle of a
        /// client's setup keeps it for the next call, so a timeout of 0 polls (a client then takes one or two calls).
        /// One client at a time: Invalid while the connection accepted last is open (dispose it first; its close runs
        /// once no call on it is in progress). Cancelled after <see cref="Cancel"/>. NoMemory once the listener has
        /// failed for good: dispose it and listen again.
        /// </summary>
        /// <param name="connection">The connection when the result is Ok, otherwise null.</param>
        public FipcResult Accept(out FipcConnection? connection, int timeoutMs)
        {
            bool added = false;
            FipcResult result;
            IntPtr conn;
            try
            {
                _handle.DangerousAddRef(ref added); // ObjectDisposedException once disposed
                result = Fipc.Accept(_handle.DangerousGetHandle(), out conn, timeoutMs);
            }
            finally
            {
                if (added) _handle.DangerousRelease();
            }
            connection = result == FipcResult.Ok ? new FipcConnection(conn) : null;
            return result;
        }

        /// <summary>
        /// Makes every Accept that waits, now or later, return Cancelled; a client whose setup an Accept began is
        /// dropped. Final. Any thread; a no-op once disposed.
        /// </summary>
        public void Cancel()
        {
            bool added = false;
            try
            {
                _handle.DangerousAddRef(ref added);
                Fipc.ListenerCancel(_handle.DangerousGetHandle());
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
        /// Stops listening: cancels the listener and closes it once no Accept is in progress. The name is free again
        /// then (on Windows, once the connections it accepted are closed too). Any thread; a later Accept throws
        /// <see cref="ObjectDisposedException"/>.
        /// </summary>
        public void Dispose()
        {
            Cancel();
            _handle.Dispose();
        }

        /// <summary>fipc_listener_t*: closed when the last reference is released.</summary>
        private sealed class Handle : SafeHandle
        {
            public Handle(IntPtr listener) : base(IntPtr.Zero, ownsHandle: true) => SetHandle(listener);

            public override bool IsInvalid => handle == IntPtr.Zero;

            protected override bool ReleaseHandle()
            {
                Fipc.ListenerClose(handle);
                return true;
            }
        }
    }
}
