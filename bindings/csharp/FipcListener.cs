using System;
using System.Runtime.InteropServices;

#nullable enable

namespace FastIpc
{
    /// <summary>
    /// A stream's listener (<see cref="FipcStream.Create"/>): it holds the stream's name, and <see cref="Accept(int)"/>
    /// sets up one client's connection at a time, on the calling thread. The connections it accepted are independent of
    /// it: they stay open when it is disposed.
    ///
    /// Setup throws: <see cref="Accept(int)"/> throws <see cref="FipcException"/>, whose
    /// <see cref="FipcException.Result"/> says why, Timeout and Cancelled included. A loop that polls for clients uses
    /// <see cref="Accept(out FipcConnection?, int)"/>, which returns its <see cref="FipcResult"/> instead. Everything
    /// after setup returns a <see cref="FipcResult"/>, as on <see cref="FipcConnection"/>. A call after Dispose throws
    /// <see cref="ObjectDisposedException"/>. Threads: one thread at a time accepts; <see cref="Cancel"/> and
    /// <see cref="Dispose"/> may be called from any thread, at any time.
    /// Lifetime: a <see cref="SafeHandle"/> underneath, as a connection's; Dispose cancels the listener, and the native
    /// close runs once an Accept in progress has returned. A listener that is never disposed is closed by its
    /// finalizer once it is unreachable, which frees the name.
    /// </summary>
    public sealed class FipcListener : IDisposable
    {
        private readonly Handle _handle;

        private FipcListener(IntPtr listener) => _handle = new Handle(listener);

        /// <summary>The object over a listener that <see cref="Fipc.StreamCreate"/> returned; it closes the listener if the object can't be made.</summary>
        internal static FipcListener Wrap(IntPtr listener)
        {
            try
            {
                return new FipcListener(listener);
            }
            catch
            {
                Fipc.ListenerClose(listener); // out of memory: no object owns it
                throw;
            }
        }

        /// <summary>
        /// Waits up to <paramref name="timeoutMs"/> for a client, sets its connection up on the calling thread and
        /// returns it; it may already hold the client's first messages. Throws <see cref="FipcException"/>: Timeout if
        /// no client came; Invalid while the connection accepted last is open (one client at a time: dispose it first;
        /// its close runs once no call on it is in progress); Cancelled after <see cref="Cancel"/>; NoMemory once the
        /// listener has failed for good (dispose it and create the stream again). To poll for clients, use
        /// <see cref="Accept(out FipcConnection?, int)"/>.
        /// </summary>
        public FipcConnection Accept(int timeoutMs = Fipc.Forever)
        {
            FipcResult result = Accept(out FipcConnection? connection, timeoutMs);
            if (result != FipcResult.Ok)
                throw new FipcException("FipcListener.Accept", result);
            return connection!;
        }

        /// <summary>
        /// <see cref="Accept(int)"/> for a loop that polls: it returns its result instead of throwing, and
        /// <paramref name="connection"/> is the connection when the result is Ok, otherwise null. A call that times out
        /// in the middle of a client's setup keeps it for the next call, so a timeout of 0 polls (a client then takes
        /// one or two calls).
        /// </summary>
        public FipcResult Accept(out FipcConnection? connection, int timeoutMs)
        {
            bool added = false;
            FipcResult result;
            IntPtr conn;
            try
            {
                _handle.DangerousAddRef(ref added); // ObjectDisposedException once disposed
                result = Fipc.StreamAccept(_handle.DangerousGetHandle(), out conn, timeoutMs);
            }
            finally
            {
                if (added) _handle.DangerousRelease();
            }
            connection = result == FipcResult.Ok ? FipcConnection.Wrap(conn) : null;
            return result;
        }

        /// <summary>
        /// Makes every Accept that waits, now or later, return Cancelled; a client whose setup had begun is dropped.
        /// Final. Any thread; a no-op once disposed.
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
