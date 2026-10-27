using System;

#nullable enable

namespace FastIpc
{
    /// <summary>
    /// Streams: a server and one client at a time, each connection a pair of rings, one per direction. The server
    /// creates the stream's name (<see cref="Create"/>) and accepts each client on its listener
    /// (<see cref="FipcListener.Accept(int)"/>); a client connects to the name (<see cref="Connect"/>).
    ///
    /// Setup throws: Create, Connect and Accept throw <see cref="FipcException"/>, whose
    /// <see cref="FipcException.Result"/> says why. Everything after setup returns a <see cref="FipcResult"/>. A
    /// caller's mistake (a null name, a negative capacity, a call after Dispose) throws as .NET's own calls do.
    /// </summary>
    public static class FipcStream
    {
        /// <summary>
        /// Server: claims <paramref name="name"/> for a stream and listens on it, with rings of
        /// <paramref name="capacity"/> bytes (a power of two from 1024 to 2^30) for each connection. Doesn't wait:
        /// clients may connect from now on, and <see cref="FipcListener.Accept(int)"/> sets each one up. Throws
        /// <see cref="FipcException"/>: AddrInUse if another stream, broadcast, queue or service holds the name;
        /// Invalid for a bad name or capacity.
        /// </summary>
        public static FipcListener Create(string name, int capacity)
        {
            if (name == null) throw new ArgumentNullException(nameof(name));
            if (capacity < 0) throw new ArgumentOutOfRangeException(nameof(capacity));
            FipcResult result = Fipc.StreamCreate(name, (nuint)capacity, out IntPtr handle);
            if (result != FipcResult.Ok)
                throw new FipcException("FipcStream.Create", name, result);
            return FipcListener.Wrap(handle);
        }

        /// <summary>
        /// Client: connects to the server of the stream on <paramref name="name"/> and sets the connection up on the
        /// calling thread, waiting up to <paramref name="timeoutMs"/> for a server to create the stream (it may start
        /// later, or be serving another client) and to call Accept; the server runs on another thread or in another
        /// process. Throws <see cref="FipcException"/>: Timeout if none did; Invalid if the server speaks another
        /// version or runs as another user, or a broadcast, a queue or a service holds the name.
        /// </summary>
        public static FipcConnection Connect(string name, int timeoutMs = Fipc.Forever)
        {
            if (name == null) throw new ArgumentNullException(nameof(name));
            FipcResult result = Fipc.StreamConnect(name, out IntPtr conn, timeoutMs);
            if (result != FipcResult.Ok)
                throw new FipcException("FipcStream.Connect", name, result);
            return FipcConnection.Wrap(conn);
        }
    }
}
