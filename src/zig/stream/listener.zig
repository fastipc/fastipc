//! A stream's listener (docs/protocol.md §3.2): the process-local object behind a `fipc_listener_t*`. It holds the
//! name, through a listening handle, and the cancel signal that every wait of `accept` watches. Nothing of the listener
//! runs in the background: `fipc_stream_create` claims the name (`claimName`) and returns, and `fipc_stream_accept`
//! sets each client up on the caller's thread (server.zig `accept`). Between two calls of `accept` the listener keeps
//! the setup of a client that a call began (`pending`), which the next call resumes.
//!
//! The listener's memory lives while its handle or the connection `accept` returned does (`refs`): an accepted
//! connection may outlive `close`, and tells the listener when it is closed (`connectionClosed`). The name is free once
//! `close` has closed the listening handle, and where a client connects on the listening handle itself (a Windows pipe
//! instance), once the accepted connection is closed too.
//!
//! Threads: one thread at a time accepts, and `close` has the listener to itself (fipc.h). `cancel` may run on any
//! thread: it sets `cancelled`, then the cancel signal.

const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;
const control = @import("../core/control.zig");
const endpoint = @import("../core/endpoint.zig");
const platform = @import("../platform.zig");
const process_state = @import("../core/process_state.zig");
const shared_memory = @import("../core/shared_memory.zig");
const wire = @import("../core/wire.zig");

const Endpoint = endpoint.Endpoint;
const Session = shared_memory.Session;

pub const Listener = struct {
    /// Set by `cancel` (any thread), never cleared.
    cancelled: std.atomic.Value(bool) = .init(false),
    /// The connection `accept` returned last is open: set by `accept`, cleared by that connection's `close`.
    client_open: std.atomic.Value(bool) = .init(false),
    /// The handle's reference, and one while the connection `accept` returned lives.
    refs: std.atomic.Value(u32) = .init(1),
    /// The listener couldn't get the memory for a client's segment: it has failed for good (`accept`'s thread only).
    failed: bool = false,
    /// The setup of a client that a call of `accept` began and the next resumes (`accept`'s thread only).
    pending: ?Pending = null,
    /// The back-off delay after a failed accept, 1 ms again once a client is served.
    backoff: control.Backoff = .{},
    io: Io,
    gpa: Allocator,
    io_ref: process_state.Ref,
    /// The listening handle and the cancel signal; while a client is set up, its connection, the segment's handle and
    /// the next listening handle.
    handles: process_state.Handles,
    capacity: u32,
    address: platform.Address,
    /// The security of the listening handles and the segments (docs/protocol.md §4.2).
    security: platform.Security,

    /// `fipc_stream_create`'s steps: claims the name (AddrInUse if another stream, broadcast, queue or service holds
    /// it) and creates the cancel signal; the caller has validated the name and the capacity. Doesn't wait.
    pub fn claimName(gpa: Allocator, name: []const u8, capacity: u32) endpoint.CreateError!*Listener {
        const ref = process_state.acquire() catch return error.NoMemory;
        errdefer process_state.release(ref);
        const listener = gpa.create(Listener) catch return error.NoMemory;
        errdefer gpa.destroy(listener);
        listener.* = .{
            .io = ref.io,
            .gpa = gpa,
            .io_ref = ref,
            .handles = .{},
            .capacity = capacity,
            .address = platform.addressOf(name) catch return error.NoMemory,
            .security = platform.Security.init() catch return error.NoMemory,
        };
        errdefer listener.security.deinit();
        listener.handles.join();
        errdefer listener.handles.leave();
        {
            process_state.lockHandles();
            defer process_state.unlockHandles();
            listener.handles.register(.cancel, platform.createCancelSignal() catch return error.NoMemory);
        }
        errdefer listener.handles.close(.cancel);
        try control.claimName(&listener.handles, &listener.address, &listener.security);
        return listener;
    }

    /// `fipc_listener_cancel`: every `accept` that waits, now or later, returns Cancelled, and drops a setup in
    /// progress.
    pub fn cancel(listener: *Listener) void {
        listener.cancelled.store(true, .release);
        if (listener.handles.forked) return; // an inherited listener (a forked child's) only takes the flag: its handles are closed
        platform.setCancelSignal(listener.handles.get(.cancel).?);
    }

    /// `fipc_listener_close`: drops a setup in progress (its client reads EOF), closes the listening handle (the name
    /// is free again, but not while an accepted connection that holds it is open) and the cancel signal, and releases
    /// the handle's reference. The connection `accept` returned stays open. Precondition: no other thread inside a call
    /// on this listener.
    pub fn close(listener: *Listener) void {
        if (listener.handles.forked) {
            // A forked child's: the child handler closed the handles; the rest belongs to the parent's threads
            listener.handles.leave();
            return;
        }
        listener.dropPending();
        listener.handles.close(.listening);
        listener.handles.close(.cancel);
        listener.release();
    }

    /// An accepted connection's `close` (core/endpoint.zig): the listener's one client is gone, so `accept` may set the
    /// next up; then the connection's reference goes.
    pub fn connectionClosed(listener: *Listener) void {
        listener.client_open.store(false, .release);
        listener.release();
    }

    /// Ends the setup in progress, if any. It unmaps and frees its segment (the client keeps its own mapping and gets a
    /// new segment on its next attempt: docs/protocol.md §4.3), frees its connection object, and drops the client on
    /// the listener's side (`dropClient`).
    pub fn dropPending(listener: *Listener) void {
        const pending = listener.pending orelse return;
        listener.pending = null;
        pending.session.unmap();
        listener.gpa.destroy(pending.session);
        endpoint.free(pending.conn);
        listener.dropClient();
    }

    /// Ends a client's setup on the listener's side: the segment's handle, the next listening handle, and the
    /// connection itself (control.zig `dropClient`, which keeps the name held).
    pub fn dropClient(listener: *Listener) void {
        listener.handles.close(.segment);
        listener.handles.close(.spare);
        control.dropClient(&listener.handles, &listener.handles, false);
    }

    /// Drops a reference; the last frees the listener.
    fn release(listener: *Listener) void {
        if (listener.refs.fetchSub(1, .acq_rel) != 1) return;
        listener.security.deinit();
        listener.handles.leave();
        const gpa = listener.gpa;
        const ref = listener.io_ref;
        gpa.destroy(listener);
        process_state.release(ref);
    }
};

/// A client's setup between the two waits of `accept` (server.zig): SEGMENT sent; READY not read yet, or only in part.
/// The client's connection is the listener's `.connected` handle, with the segment's handle (`.segment`, where it
/// travels with the frame) and the next listening handle (`.spare`, where a client connects on the listening handle
/// itself).
pub const Pending = struct {
    /// The connection that will own the session.
    conn: *Endpoint,
    /// The client's segment, created and filled.
    session: *Session,
    /// READY's bytes so far.
    frame: wire.Frame = undefined,
    got: usize = 0,
};
