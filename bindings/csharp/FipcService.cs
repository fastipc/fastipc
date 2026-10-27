using System;
using System.Runtime.InteropServices;

#nullable enable

namespace FastIpc
{
    /// <summary>
    /// A service, held by its owner: it takes requests from up to 64 clients and hands each to one of up to 64
    /// servers, in any processes of the user; the server that took a request replies to it, and the reply goes back to
    /// the client that sent it. A server waits for the requests of every client at once (one receive), and the servers
    /// of a pool share the load: each takes the next request of any client. Each client has a ring of its own in the
    /// service's memory, which every member maps, and a request takes room there for itself and for its reply, so a
    /// server never waits for a client. The owner creates the name (<see cref="Create"/>), which returns this object;
    /// clients and servers join it (<see cref="Join"/>), the owner's own too: the owner's process admits each joiner
    /// by itself, on a thread of the library's, so a member may join from any thread, the owner's included. Requests
    /// and replies are of one piece, 1 to maxMessage bytes (the connections' <see cref="FipcConnection.MaxPiece"/>).
    ///
    /// Order: a client's receive returns the reply to its oldest request whose reply it hasn't received yet, so the
    /// n-th reply it receives answers its n-th request, whichever server replies first. A request is answered once:
    /// by its reply, or with <see cref="FipcResult.Abandoned"/> if the server that took it ended before it replied (it
    /// disposed its connection, or its process ended); an abandoned request may or may not have been carried out, and
    /// the service never sends it again. Not covered: a server that hangs while its process runs (there are no
    /// timeouts: its client waits, within its own timeouts), and the owner's process, with which the service ends.
    ///
    /// The service ends at <see cref="Dispose"/>, or when the owner's process ends: clients' sends and servers'
    /// receives then return Disconnected, a client receives the replies that came before the end, then Disconnected,
    /// and a server's reply returns Disconnected. A member's Dispose, or the end of its process, doesn't end it.
    ///
    /// Lifetime: a service lives until <see cref="Dispose"/> or the end of its process; dropping the object doesn't
    /// end it. It has no finalizer, as a <see cref="FipcQueue"/> has none: its use lives in its members, in this
    /// process and in others, so the garbage collector never ends it. A service that is never disposed holds its name
    /// until the process ends, and another <see cref="Create"/> of the name then throws AddrInUse. Threads:
    /// <see cref="WaitForMembers"/>, <see cref="Cancel"/> and <see cref="Dispose"/> may be called from any thread, at
    /// any time; Dispose cancels the service, and the native close runs once a WaitForMembers in progress has
    /// returned.
    ///
    /// Setup throws: Create and Join throw <see cref="FipcException"/>, whose <see cref="FipcException.Result"/> says
    /// why. Everything after setup returns a <see cref="FipcResult"/>: the service's WaitForMembers and every call on a
    /// member's connection. A caller's mistake (a null name, a negative count, a call after Dispose) throws as .NET's
    /// own calls do.
    /// </summary>
    public sealed class FipcService : IDisposable
    {
        private readonly Handle _handle;

        private FipcService(IntPtr service)
        {
            _handle = new Handle(service);
            GC.SuppressFinalize(_handle); // the garbage collector never ends a service
        }

        /// <summary>
        /// Owner: claims <paramref name="name"/> for a service and returns it. Doesn't wait: members may join from now
        /// on. Up to <paramref name="maxClients"/> clients (1 to 64) and <paramref name="maxServers"/> servers (1 to
        /// 64) may be joined at once; each client has a ring of <paramref name="capacity"/> bytes (a power of two from
        /// 4096 to 2^30: the library takes up to 2^31, which an int can't hold, so a ring of 2^31 bytes needs the raw
        /// <see cref="Fipc.ServiceCreate"/>) for its requests and their replies, each of 1 to <paramref name="maxMessage"/> bytes (1 to
        /// capacity / 4). A request holds its length rounded up to 64 bytes, plus maxMessage rounded up to 64, of its
        /// client's ring, from its send until its reply has been received. The service's memory is set up now, whole.
        /// A service lives until <see cref="Dispose"/> or the end of its process; dropping the object doesn't end it.
        /// Throws <see cref="FipcException"/>: AddrInUse if another stream, broadcast, queue or service holds the
        /// name; Invalid for a bad name, capacity, maxMessage or member count; NoMemory if the service's memory can't
        /// be had.
        /// </summary>
        public static FipcService Create(string name, int capacity, int maxMessage, int maxClients, int maxServers)
        {
            if (name == null) throw new ArgumentNullException(nameof(name));
            if (capacity < 0) throw new ArgumentOutOfRangeException(nameof(capacity));
            if (maxMessage < 0) throw new ArgumentOutOfRangeException(nameof(maxMessage));
            if (maxClients < 0) throw new ArgumentOutOfRangeException(nameof(maxClients));
            if (maxServers < 0) throw new ArgumentOutOfRangeException(nameof(maxServers));
            FipcResult result = Fipc.ServiceCreate(name, (nuint)capacity, (nuint)maxMessage, (uint)maxClients,
                (uint)maxServers, out IntPtr handle);
            if (result != FipcResult.Ok)
                throw new FipcException("FipcService.Create", name, result);
            try
            {
                return new FipcService(handle);
            }
            catch
            {
                Fipc.ServiceClose(handle); // out of memory: no object owns it
                throw;
            }
        }

        /// <summary>
        /// Client or server: joins the service on <paramref name="name"/> as <paramref name="role"/> and returns its
        /// connection, waiting up to <paramref name="timeoutMs"/> for the owner to create the service, for a free place
        /// of the role (a client's place is free again once its last client has left and every request of it that a
        /// server took has been answered), and for the owner's process to admit it. Clients wait while no server is
        /// joined, and servers while no client is. Throws <see cref="FipcException"/>: Timeout if the join didn't
        /// complete within timeoutMs; Invalid for another role, a name a broadcast or a queue holds, or an owner of
        /// another version or user. On a name a stream holds, the join learns the kind when the stream's server accepts
        /// it, which brings Invalid; while no accept runs, it waits until it times out.
        ///
        /// A client's <see cref="FipcConnection.Send"/> (or SendAcquire and SendCommit) sends a request, waiting for
        /// room in its ring, which its replies not yet received hold too; its receives return the reply to its oldest
        /// request whose reply it hasn't received yet, or <see cref="FipcResult.Abandoned"/> if that request's server
        /// ended before it replied (the request counts as answered). A receive that times out or is cancelled leaves
        /// the reply to come, for the next receive. <see cref="FipcConnection.Call"/> does both: it sends a request and
        /// returns its reply. A server's receives take the next request of any client, and its next send replies to it
        /// and never waits; a server holds one request at a time, so its receives return Invalid until it replies, and
        /// a reply with no request held returns Invalid. A request received in place (ReceiveAcquire) stays valid until
        /// the reply's Send or SendCommit, and the reply ends its span; a ReceiveRelease ends the span only, and the
        /// request stays held until its reply. A reply to a client that has left is dropped and returns Ok.
        /// Disposing the connection leaves the service, which goes on: a client's requests that no server took are
        /// dropped, and the replies to those taken are lost; the request a server holds is answered Abandoned (one
        /// held in place by another thread once its span ends: see <see cref="FipcConnection.ReceiveAcquire"/>).
        /// </summary>
        public static FipcConnection Join(string name, FipcServiceRole role, int timeoutMs = Fipc.Forever)
        {
            if (name == null) throw new ArgumentNullException(nameof(name));
            FipcResult result = Fipc.ServiceJoin(name, role, out IntPtr conn, timeoutMs);
            if (result != FipcResult.Ok)
                throw new FipcException("FipcService.Join", name, result);
            return FipcConnection.Wrap(conn, serviceClient: role == FipcServiceRole.Client,
                serviceServer: role == FipcServiceRole.Server);
        }

        /// <summary>
        /// Waits up to <paramref name="timeoutMs"/> until at least <paramref name="atLeast"/> members of
        /// <paramref name="role"/> (null: of both roles) are joined (0 returns at once, and a timeout of 0 polls);
        /// <paramref name="count"/> is how many are, at every result but Invalid. A member that leaves, or whose process
        /// ends, is counted until the owner's process has seen its end, soon after. Ok once the count is reached;
        /// Timeout; Cancelled after <see cref="Cancel"/>; Disconnected once the service has ended; Invalid for atLeast
        /// over the role's maximum (maxClients, maxServers, or their sum for null). Any thread.
        /// </summary>
        public FipcResult WaitForMembers(FipcServiceRole? role, int atLeast, out int count,
            int timeoutMs = Fipc.Forever)
        {
            if (atLeast < 0) throw new ArgumentOutOfRangeException(nameof(atLeast));
            bool added = false;
            try
            {
                _handle.DangerousAddRef(ref added); // ObjectDisposedException once disposed
                FipcResult result = Fipc.ServiceWaitForMembers(_handle.DangerousGetHandle(), role ?? default,
                    (uint)atLeast, out uint n, timeoutMs);
                count = (int)n;
                return result;
            }
            finally
            {
                if (added) _handle.DangerousRelease();
            }
        }

        /// <summary>
        /// Makes every <see cref="WaitForMembers"/> that waits, now or later, return Cancelled. The service goes on:
        /// its members and their joins are unaffected. Final. Any thread; a no-op once disposed.
        /// </summary>
        public void Cancel()
        {
            bool added = false;
            try
            {
                _handle.DangerousAddRef(ref added);
                Fipc.ServiceCancel(_handle.DangerousGetHandle());
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
        /// Ends the service: cancels it, and closes it once no <see cref="WaitForMembers"/> is in progress. Clients'
        /// sends and servers' receives then return Disconnected, a client receives the replies that came before the
        /// end, then Disconnected, and a server's reply returns Disconnected; the name is free again. The members'
        /// connections, the owner's own included, stay open until their Dispose. Any thread; a later WaitForMembers
        /// throws <see cref="ObjectDisposedException"/>.
        /// </summary>
        public void Dispose()
        {
            Cancel();
            _handle.Dispose();
        }

        /// <summary>fipc_service_t*: closed when the last reference is released, never by a finalizer (it is suppressed).</summary>
        private sealed class Handle : SafeHandle
        {
            public Handle(IntPtr service) : base(IntPtr.Zero, ownsHandle: true) => SetHandle(service);

            public override bool IsInvalid => handle == IntPtr.Zero;

            protected override bool ReleaseHandle()
            {
                Fipc.ServiceClose(handle);
                return true;
            }
        }
    }
}
