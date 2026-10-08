//! A listener (docs/protocol.md §3.2): the process-local object behind a `fipc_listener_t*`. `listen` claims the name
//! (a listening handle, platform.zig `claim`) and returns; nothing of the listener runs in the background. `accept`
//! sets each client up on the caller's thread: it waits for a client and accepts it, creates the client's segment and
//! the connection that will own it, sends SEGMENT, waits for READY, then hands the connection (its handle and the
//! session) to that connection, starts its watcher (control.zig) and returns it.
//!
//! The setup has two waits: for a client, and for its READY. A call's timeout ends it only inside a wait; everything
//! between two waits runs to completion, and the progress before the second is kept in the listener (`pending`), so
//! the next call resumes there. A client whose setup a call began is never dropped because that call timed out, and a
//! server that polls (`accept` with timeout 0) lets a client in within two calls. Kernel objects hold everything else
//! between calls: a client that connects while no `accept` runs waits in the backlog (or on the next pipe instance),
//! and its READY in the connection's buffer.
//!
//! One client at a time: while the connection `accept` returned last is open, `accept` is Invalid at once, and a
//! client that connects meanwhile waits, within its own timeout. The listener's memory lives while its handle or that
//! connection does (`refs`): an accepted connection may outlive `close`, and tells the listener when it is closed
//! (`connectionClosed`). The name is free once `close` has closed the listening handle, and where a client connects on
//! the listening handle itself (a Windows pipe instance), once the accepted connection is closed too.
//!
//! Threads: one thread at a time accepts, and `close` has the listener to itself (fipc.h). `cancel` may run on any
//! thread: it sets `cancelled`, then the cancel signal, which every wait of `accept` watches.

const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;
const build_options = @import("build_options");
const control = @import("control.zig");
const endpoint = @import("endpoint.zig");
const log = @import("../log.zig");
const names = @import("names.zig");
const platform = @import("../platform.zig");
const process_state = @import("../process_state.zig");
const segment = @import("../session/segment.zig");
const wire = @import("wire.zig");

const Endpoint = endpoint.Endpoint;
const Session = segment.Session;

pub const Listener = struct {
    /// Set by `cancel` (any thread), never cleared.
    cancelled: std.atomic.Value(bool) = .init(false),
    /// The connection `accept` returned last is open: set by `accept`, cleared by that connection's `close`.
    client_open: std.atomic.Value(bool) = .init(false),
    /// The handle's reference, and one while the connection `accept` returned lives.
    refs: std.atomic.Value(u32) = .init(1),
    /// The listener couldn't get the memory for a client's segment: it has failed for good (`accept`'s thread only).
    failed: bool = false,
    /// A client's setup that a call of `accept` began and the next resumes (`accept`'s thread only).
    pending: Pending = .none,
    /// The back-off delay after a failed accept, 1 ms again once a client is served.
    backoff: control.Backoff = .{},
    io: Io,
    io_source: endpoint.IoSource,
    gpa: Allocator,
    io_ref: ?process_state.Ref,
    /// The listening handle and the cancel signal; while a client is set up, its connection, the segment's handle and
    /// the next listening handle.
    handles: process_state.Handles,
    capacity: u32,
    address: platform.Address,
    /// The security of the listening handles and the segments (docs/protocol.md §4.2).
    security: platform.Security,

    /// An accepted connection's `close` (endpoint.zig): the listener's one client is gone, so `accept` may set the
    /// next up; then the connection's reference goes.
    pub fn connectionClosed(l: *Listener) void {
        l.client_open.store(false, .release);
        release(l);
    }
};

/// The setup of a client between the two waits.
const Pending = union(enum) {
    none,
    /// SEGMENT sent; READY not read yet, or only in part. The client's connection is the listener's `.connected` handle,
    /// with the segment's handle (`.segment`, where it travels with the frame) and the next listening handle
    /// (`.spare`, where a client connects on the listening handle itself).
    awaiting_ready: AwaitingReady,
};

const AwaitingReady = struct {
    /// The connection that will own the session.
    conn: *Endpoint,
    /// The client's segment, created and filled.
    session: *Session,
    /// READY's bytes so far.
    frame: wire.Frame = undefined,
    got: usize = 0,
};

/// The listener's errors: each is the `fipc_result_t` code of the same name.
pub const Error = error{ Invalid, NoMemory, Timeout, Cancelled, AddrInUse };

// === The application side ===

/// `fipc_listen`: validates the name and capacity, claims the name (AddrInUse if another listener holds it) and
/// creates the cancel signal; doesn't wait.
pub fn listen(io_source: endpoint.IoSource, gpa: Allocator, name: []const u8, capacity: usize) Error!*Listener {
    if (!names.isValid(name) or !segment.validCapacity(capacity)) return error.Invalid;
    const io, const ref = try io_source.acquire();
    errdefer if (ref) |r| process_state.release(r);
    const l = gpa.create(Listener) catch return error.NoMemory;
    errdefer gpa.destroy(l);
    l.* = .{
        .io = io,
        .io_source = io_source,
        .gpa = gpa,
        .io_ref = ref,
        .handles = .{},
        .capacity = @intCast(capacity),
        .address = platform.addressOf(name) catch return error.NoMemory,
        .security = platform.Security.init() catch return error.NoMemory,
    };
    errdefer l.security.deinit();
    l.handles.join();
    errdefer l.handles.leave();
    {
        process_state.lockHandles();
        defer process_state.unlockHandles();
        l.handles.register(.cancel, platform.createCancelSignal() catch return error.NoMemory);
    }
    errdefer l.handles.close(.cancel);
    process_state.lockHandles();
    defer process_state.unlockHandles();
    const h = (platform.claim(&l.address, &l.security) catch return error.NoMemory) orelse return error.AddrInUse;
    l.handles.register(.listening, h);
    return l;
}

/// `fipc_accept`: waits up to `timeout` for a client and sets it up on the caller's thread (this file's top comment),
/// resuming a setup an earlier call began. Invalid while the connection it returned last is open (one client at a
/// time); NoMemory once the listener failed; Cancelled after `cancel` (a setup in progress is dropped); Timeout, with
/// the progress kept.
pub fn accept(l: *Listener, timeout: Io.Timeout) Error!*Endpoint {
    if (l.handles.forked) {
        process_state.reportInherited("listener");
        return error.Invalid;
    }
    if (l.client_open.load(.acquire)) return error.Invalid;
    if (l.failed) return error.NoMemory;
    const deadline = timeout.toTimestamp(l.io);
    while (true) {
        if (l.cancelled.load(.acquire)) {
            dropPending(l);
            return error.Cancelled;
        }
        const wait = control.waitMs(l.io, deadline);
        const step = switch (l.pending) {
            .none => acceptClient(l, wait),
            .awaiting_ready => |*p| awaitReady(l, p, wait),
        };
        switch (step) {
            .progress => {},
            .idle => if (control.passed(l.io, deadline)) return error.Timeout,
            .back_off => _ = platform.waitCancelSignal(l.handles.get(.cancel).?, l.backoff.next(control.waitMs(l.io, deadline))),
            .failed => return error.NoMemory,
            .accepted => |conn| return conn,
        }
    }
}

/// `fipc_listener_cancel`: every `accept` that waits, now or later, returns Cancelled, and drops a setup in progress.
pub fn cancel(l: *Listener) void {
    l.cancelled.store(true, .release);
    if (l.handles.forked) return; // an inherited listener (a forked child's) only takes the flag: its handles are closed
    platform.setCancelSignal(l.handles.get(.cancel).?);
}

/// `fipc_listener_close`: drops a setup in progress (its client reads EOF), closes the listening handle (the name is
/// free again, but not while an accepted connection that holds it is open) and the cancel signal, and releases the
/// handle's reference. The connection `accept` returned stays open. Precondition: no other thread inside a call on
/// this listener.
pub fn close(l: *Listener) void {
    if (l.handles.forked) {
        // A forked child's: the child handler closed the handles; the rest belongs to the parent's threads
        l.handles.leave();
        return;
    }
    dropPending(l);
    l.handles.close(.listening);
    l.handles.close(.cancel);
    release(l);
}

/// Drops a reference; the last frees the listener.
fn release(l: *Listener) void {
    if (l.refs.fetchSub(1, .acq_rel) != 1) return;
    l.security.deinit();
    l.handles.leave();
    const gpa = l.gpa;
    const ref = l.io_ref;
    gpa.destroy(l);
    if (ref) |r| process_state.release(r);
}

// === The setup ===

/// What a step of `accept` came to.
const Step = union(enum) {
    /// Something changed: look again (the cancel, the next wait).
    progress,
    /// The wait ended with nothing: Timeout if the deadline has passed, else wait again.
    idle,
    /// An accept failed, or handles ran out: wait out the back-off, then look again.
    back_off,
    /// No memory for a client's segment: the listener has failed for good.
    failed,
    /// The client's connection, set up.
    accepted: *Endpoint,
};

/// The first wait, for a client (docs/protocol.md §3.2), then the steps up to SEGMENT: the accept, under the handle lock
/// the fork check (the accepted connection is the one handle not born under the lock), the peer's user, the client's
/// segment and connection, SEGMENT.
fn acceptClient(l: *Listener, wait: ?u32) Step {
    var forks = process_state.forkCount();
    const handle = switch (platform.acceptWithin(l.handles.get(.listening).?, l.handles.get(.cancel).?, wait, &forks)) {
        .idle => return .idle,
        .cancelled => return .progress,
        .done => |outcome| switch (outcome) {
            .connection => |h| h,
            .closing => return .progress,
            .transient, .other => return .back_off,
        },
    };
    const forked = forked: {
        process_state.lockHandles();
        defer process_state.unlockHandles();
        // A client that connects on the listening handle itself takes it: `newSetup` makes the next one
        if (l.handles.get(.listening) == handle) _ = l.handles.unregister(.listening);
        l.handles.register(.connected, handle);
        break :forked process_state.forkCount() != forks;
    };
    if (build_options.test_hooks) control.test_hooks.crashIfAt(.accepted);
    if (forked) {
        dropConnection(l, true);
        return .progress;
    }
    const peer = platform.peer(handle, .listener) orelse {
        dropConnection(l, false);
        return .back_off;
    };
    if (!peer.same_user) {
        log.warn(@src(), "a process of another user connected; dropped", .{});
        dropConnection(l, false);
        return .progress;
    }
    const setup = newSetup(l) catch |err| {
        abandon(l, null);
        return switch (err) {
            error.OutOfMemory => {
                l.failed = true;
                return .failed;
            },
            error.SystemResources => .back_off,
        };
    };
    if (build_options.test_hooks) control.test_hooks.crashIfAt(.segment_created);
    const frame = wire.encodeSegment(.{
        .pid = platform.pid(),
        .capacity = setup.session.cap,
        .segment_size = segment.size(setup.session.cap),
        .session_id = setup.session.id,
    });
    if (!control.sendFrame(l.io, handle, &frame, l.handles.get(.segment))) {
        abandon(l, setup);
        return .progress;
    }
    if (build_options.test_hooks) control.test_hooks.crashIfAt(.segment_sent);
    l.pending = .{ .awaiting_ready = .{ .conn = setup.conn, .session = setup.session } };
    return .progress;
}

/// The second wait, for READY (docs/protocol.md §3.2), into the bytes an earlier call may have read; then the
/// hand-over. EOF, a read error or anything but READY drops the client.
fn awaitReady(l: *Listener, p: *AwaitingReady, wait: ?u32) Step {
    switch (platform.readWithin(l.handles.get(.connected).?, p.frame[p.got..], l.handles.get(.cancel).?, wait)) {
        .idle => return .idle,
        .cancelled => return .progress,
        .done => |n| {
            if (n == 0) {
                dropPending(l);
                return .progress;
            }
            p.got += n;
            if (p.got < p.frame.len) return .progress;
            if (!wire.isReady(&p.frame)) {
                dropPending(l);
                return .progress;
            }
            if (build_options.test_hooks) control.test_hooks.crashIfAt(.ready_read);
            return handOver(l);
        },
    }
}

/// A client's connection and segment, before SEGMENT is sent.
const Setup = struct {
    conn: *Endpoint,
    session: *Session,
};

/// The client's connection, its segment, created and filled with no lock held (a segment handle that travels with
/// SEGMENT is created and registered under the handle lock first), and the next listening handle where the accept
/// took this one (docs/protocol.md §4.2). On failure the caller abandons the client (`abandon(null)`).
fn newSetup(l: *Listener) segment.CreateError!Setup {
    const conn = endpoint.create(l.io_source, l.gpa) catch return error.OutOfMemory;
    errdefer endpoint.free(conn);
    const session = try l.gpa.create(Session);
    errdefer l.gpa.destroy(session);
    session.* = try newSegment(l);
    errdefer session.unmap();
    try nextListening(l);
    return .{ .conn = conn, .session = session };
}

fn newSegment(l: *Listener) segment.CreateError!Session {
    if (platform.passes_segment_handle) {
        process_state.lockHandles();
        defer process_state.unlockHandles();
        l.handles.register(.segment, platform.createSegmentHandle() catch return error.SystemResources);
    }
    return segment.create(l.io, l.handles.get(.segment), &l.security, l.capacity) catch |err| {
        l.handles.close(.segment);
        return err;
    };
}

fn nextListening(l: *Listener) error{SystemResources}!void {
    process_state.lockHandles();
    defer process_state.unlockHandles();
    const next = platform.nextListening(&l.address, &l.security) catch return error.SystemResources;
    if (next) |h| l.handles.register(.spare, h);
}

/// The setup in progress, if any, ends: the one way out of `awaiting_ready` but the hand-over.
fn dropPending(l: *Listener) void {
    switch (l.pending) {
        .none => {},
        .awaiting_ready => |p| abandon(l, .{ .conn = p.conn, .session = p.session }),
    }
    l.pending = .none;
}

/// Ends a client's setup: its segment (the handle and the mapping; the client's copy goes with the client, which
/// gets a new one next time, docs/protocol.md §4.3), its connection object, the next listening handle, and the
/// connection itself.
fn abandon(l: *Listener, setup: ?Setup) void {
    l.handles.close(.segment);
    l.handles.close(.spare);
    if (setup) |s| {
        s.session.unmap();
        l.gpa.destroy(s.session);
        endpoint.free(s.conn);
    }
    dropConnection(l, false);
}

/// The accepted connection ends (`forked`: a child forked during the accept may hold a copy, docs/protocol.md §7).
/// A handle the client connected on itself stays, to accept the next client on: closing it could give up the name.
fn dropConnection(l: *Listener, forked: bool) void {
    process_state.lockHandles();
    defer process_state.unlockHandles();
    const handle = l.handles.unregister(.connected) orelse return;
    if (platform.dropClient(handle, forked)) |again| l.handles.register(.listening, again);
}

/// The handshake is complete: the connection gets the accepted handle (moved between the nodes under the handle
/// lock) and the session, and its watcher starts; the segment's handle is closed (the client holds its own mapping),
/// and a next listening handle becomes the one to accept on.
fn handOver(l: *Listener) Step {
    const p = l.pending.awaiting_ready;
    l.pending = .none;
    l.backoff = .{};
    l.handles.close(.segment);
    {
        process_state.lockHandles();
        defer process_state.unlockHandles();
        p.conn.handles.register(.connected, l.handles.unregister(.connected).?);
        if (l.handles.unregister(.spare)) |next| l.handles.register(.listening, next);
    }
    endpoint.startAccepted(p.conn, p.session) catch {
        log.warn(@src(), "no task for an accepted connection; the client is dropped", .{});
        return .progress;
    };
    p.conn.listener = l;
    _ = l.refs.fetchAdd(1, .monotonic);
    l.client_open.store(true, .release);
    return .{ .accepted = p.conn };
}
