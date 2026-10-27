using System;
using System.Runtime.InteropServices;

#nullable enable

namespace FastIpc
{
    /// <summary>
    /// A work queue, held by its owner: it takes messages from up to 63 producers and hands each to one of up to 64
    /// consumers, in any processes of the user. Each producer has a ring of its own in the queue's memory, which every
    /// member maps; a consumer takes a message of any producer, and the queue promises no order. The owner creates the
    /// name (<see cref="Create"/>), which returns this object; producers and consumers join it (<see cref="Join"/>),
    /// the owner's own too: the owner's process admits each joiner by itself, on a thread of the library's, so a member
    /// may join from any thread, the owner's included. Messages are of one piece, up to
    /// <see cref="FipcConnection.MaxPiece"/> bytes (a quarter of the capacity); they wait in the queue until a consumer
    /// takes them. Each message goes to one consumer, and again, to another consumer, only if the process of the
    /// consumer that took it ends before acknowledging it (see the README's Delivery).
    ///
    /// The queue ends at <see cref="Dispose"/>, or when the owner's process ends: producers' sends then return
    /// Disconnected, and consumers receive what is left in the queue, then Disconnected. A member's Dispose, or the end
    /// of its process, doesn't end it.
    ///
    /// Lifetime: a queue lives until <see cref="Dispose"/> or the end of its process; dropping the object doesn't end
    /// it. It has no finalizer, unlike this binding's other handles: its use lives in its members, in this process and
    /// in others, so the garbage collector never ends it. A queue that is never disposed holds its name until the
    /// process ends, and another <see cref="Create"/> of the name then throws AddrInUse. Threads:
    /// <see cref="WaitForMembers"/>, <see cref="Cancel"/> and <see cref="Dispose"/> may be called from any thread, at
    /// any time; Dispose cancels the queue, and the native close runs once a WaitForMembers in progress has returned.
    ///
    /// Setup throws: Create and Join throw <see cref="FipcException"/>, whose <see cref="FipcException.Result"/> says
    /// why. Everything after setup returns a <see cref="FipcResult"/>: the queue's WaitForMembers and every call on a
    /// member's connection. A caller's mistake (a null name, a negative count, a call after Dispose) throws as .NET's
    /// own calls do.
    /// </summary>
    public sealed class FipcQueue : IDisposable
    {
        private readonly Handle _handle;

        private FipcQueue(IntPtr queue)
        {
            _handle = new Handle(queue);
            GC.SuppressFinalize(_handle); // the garbage collector never ends a queue
        }

        /// <summary>
        /// Owner: claims <paramref name="name"/> for a work queue and returns it. Doesn't wait: members may join from
        /// now on. Up to <paramref name="maxProducers"/> producers (1 to 63) and <paramref name="maxConsumers"/>
        /// consumers (1 to 64) may be joined at once; each producer has a ring of <paramref name="capacity"/> bytes (a
        /// power of two from 4096 to 2^30): up to capacity / 64 messages, each of one piece, at most a quarter of it.
        /// The queue's memory is set up now, whole. A queue lives until <see cref="Dispose"/> or the end of its
        /// process; dropping the object doesn't end it. Throws <see cref="FipcException"/>: AddrInUse if another
        /// stream, broadcast, queue or service holds the name; Invalid for a bad name, capacity or member count;
        /// NoMemory if the queue's memory can't be had.
        /// </summary>
        public static FipcQueue Create(string name, int capacity, int maxProducers, int maxConsumers)
        {
            if (name == null) throw new ArgumentNullException(nameof(name));
            if (capacity < 0) throw new ArgumentOutOfRangeException(nameof(capacity));
            if (maxProducers < 0) throw new ArgumentOutOfRangeException(nameof(maxProducers));
            if (maxConsumers < 0) throw new ArgumentOutOfRangeException(nameof(maxConsumers));
            FipcResult result = Fipc.QueueCreate(name, (nuint)capacity, (uint)maxProducers, (uint)maxConsumers,
                out IntPtr handle);
            if (result != FipcResult.Ok)
                throw new FipcException("FipcQueue.Create", name, result);
            try
            {
                return new FipcQueue(handle);
            }
            catch
            {
                Fipc.QueueClose(handle); // out of memory: no object owns it
                throw;
            }
        }

        /// <summary>
        /// Producer or consumer: joins the work queue on <paramref name="name"/> as <paramref name="role"/> and returns
        /// its connection, waiting up to <paramref name="timeoutMs"/> for the owner to create the queue, for a free
        /// place of the role (a producer's place is free again once every message its last producer sent was taken and
        /// released), and for the owner's process to admit it. A producer sends; a consumer receives each message of
        /// the queue's producers that no other consumer took, then Disconnected once the queue has ended, and a
        /// producer's sends return Disconnected from then on. Throws <see cref="FipcException"/>: Timeout if the join
        /// didn't complete within timeoutMs; Invalid for another role, a name a broadcast or a service holds, or an
        /// owner of another version or user. On a name a stream holds, the join learns the kind when the stream's
        /// server accepts it, which brings Invalid; while no accept runs, it waits until it times out. Disposing the
        /// connection leaves the queue, which goes on: a producer's messages stay for the consumers.
        /// </summary>
        public static FipcConnection Join(string name, FipcQueueRole role, int timeoutMs = Fipc.Forever)
        {
            if (name == null) throw new ArgumentNullException(nameof(name));
            FipcResult result = Fipc.QueueJoin(name, role, out IntPtr conn, timeoutMs);
            if (result != FipcResult.Ok)
                throw new FipcException("FipcQueue.Join", name, result);
            return FipcConnection.Wrap(conn);
        }

        /// <summary>
        /// Waits up to <paramref name="timeoutMs"/> until at least <paramref name="atLeast"/> members of
        /// <paramref name="role"/> (null: of both roles) are joined (0 returns at once, and a timeout of 0 polls);
        /// <paramref name="count"/> is how many are, at every result but Invalid. A member that leaves, or whose process
        /// ends, is counted until the owner's process has seen its end, soon after. Ok once the count is reached;
        /// Timeout; Cancelled after <see cref="Cancel"/>; Disconnected once the queue has ended; Invalid for atLeast
        /// over the role's maximum (maxProducers, maxConsumers, or their sum for null). Any thread.
        /// </summary>
        public FipcResult WaitForMembers(FipcQueueRole? role, int atLeast, out int count, int timeoutMs = Fipc.Forever)
        {
            if (atLeast < 0) throw new ArgumentOutOfRangeException(nameof(atLeast));
            bool added = false;
            try
            {
                _handle.DangerousAddRef(ref added); // ObjectDisposedException once disposed
                FipcResult result = Fipc.QueueWaitForMembers(_handle.DangerousGetHandle(), role ?? default,
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
        /// Makes every <see cref="WaitForMembers"/> that waits, now or later, return Cancelled. The queue goes on: its
        /// members and their joins are unaffected. Final. Any thread; a no-op once disposed.
        /// </summary>
        public void Cancel()
        {
            bool added = false;
            try
            {
                _handle.DangerousAddRef(ref added);
                Fipc.QueueCancel(_handle.DangerousGetHandle());
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
        /// Ends the queue: cancels it, and closes it once no <see cref="WaitForMembers"/> is in progress. Producers'
        /// sends then return Disconnected, and consumers receive what is left, then Disconnected; the name is free
        /// again. The members' connections, the owner's own included, stay open until their Dispose. Any thread; a
        /// later WaitForMembers throws <see cref="ObjectDisposedException"/>.
        /// </summary>
        public void Dispose()
        {
            Cancel();
            _handle.Dispose();
        }

        /// <summary>fipc_queue_t*: closed when the last reference is released, never by a finalizer (it is suppressed).</summary>
        private sealed class Handle : SafeHandle
        {
            public Handle(IntPtr queue) : base(IntPtr.Zero, ownsHandle: true) => SetHandle(queue);

            public override bool IsInvalid => handle == IntPtr.Zero;

            protected override bool ReleaseHandle()
            {
                Fipc.QueueClose(handle);
                return true;
            }
        }
    }
}
