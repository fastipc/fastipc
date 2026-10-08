//! A connection's endpoint: the process-local object behind a `fipc_conn_t*`, which the application's threads and the
//! connection's watcher share, and the application side of its lifecycle: `connect`, `cancel` and `close`. A
//! listener's `accept` creates the endpoints of the connections it accepts (listener.zig, `create`). Nothing of the
//! lifecycle is in shared memory.
//!
//! A connection has one session: `connect` sets it up on the caller's thread (control.zig `handshake`) and returns
//! once it is, an accepted connection has it from the start, and once it ended the application closes the connection
//! and accepts or connects a new one. Once the session is published, the connection's watcher (control.zig
//! `runWatcher`) reads the control connection until the peer's end, which it publishes.
//!
//! Who writes what: `status` and the view: the thread that sets the session up, before the watcher starts; then the
//! watcher (PEER_DEAD); `close` once the watcher has stopped. `requests`: application threads only (`cancel` and
//! `shutdown` set with fetch-or; never cleared).
//!
//! Locks, outermost first: the reference-count lock (process_state.zig), the endpoint mutex, the handle lock
//! (process_state.zig). None is held across a blocking call, and no path here holds two.
//!
//! Two import cycles are deliberate. With data/ring_wait.zig: the local interrupt (`interrupt`, docs/protocol.md §6.2)
//! runs the one statement of the wake rule, `wakeOwnWaiters`, and the ring waits read this endpoint's `Io` and hot line
//! (ring.zig and stream.zig import this file for the hot line and the view, one way). With control.zig: `connect` runs
//! the handshake and starts the watcher (`control.runWatcher`), which work on the endpoint. Zig analyzes both cycles
//! lazily.

const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;
const abi = @import("../abi.zig");
const control = @import("control.zig");
const listener = @import("listener.zig");
const names = @import("names.zig");
const platform = @import("../platform.zig");
const process_state = @import("../process_state.zig");
const segment = @import("../session/segment.zig");
const ring_wait = @import("../data/ring_wait.zig");

const Session = segment.Session;

/// The `requests` word: written only by application threads, never cleared. `cancel` and `shutdown` are set with
/// fetch-or.
pub const Requests = packed struct(u32) {
    cancel: bool = false,
    shutdown: bool = false,
    reserved: u30 = 0,
};

/// The public phase: `handshake` until the session is published, then `connected` until the peer's end.
pub const Phase = enum(u2) {
    handshake,
    connected,
    peer_dead,
};

/// The `status` word: the public phase.
pub const Status = packed struct(u32) {
    phase: Phase = .handshake,
    reserved: u30 = 0,
};

/// Cache line 0, which every data call reads; it changes only at lifecycle events. The view (`view_*`) is the
/// data path's only source for the rings: whoever publishes the session writes its fields (.monotonic), then the
/// session slot and the number (.release), under the endpoint mutex; a reader loads the number (.acquire) and, if it
/// isn't 0, the fields. The view changes while data calls may run only from a session to none, in a forked child.
pub const Hot = extern struct {
    /// 1 once the session is published, 0 before it and in a forked child: the view's publication point.
    view_number: std.atomic.Value(u64),
    view_header: std.atomic.Value(?*abi.SegmentHeader),
    view_data: std.atomic.Value(?[*]u8),
    /// The session slot: set (.release) only once the session is complete, cleared before its segment is unmapped,
    /// so a forked child can free what it finds there (docs/protocol.md §7).
    view_session: std.atomic.Value(?*Session),
    /// The ring capacity, this process's own copy: never read again from the shared header (docs/protocol.md §4.1).
    view_cap: std.atomic.Value(u32),
    /// 1: this side is the listener's, which writes s2c.
    view_server: std.atomic.Value(u32),
    requests: std.atomic.Value(Requests),
    /// The phase.
    status: std.atomic.Value(Status),
    reserved: [16]u8,
};

/// Cache line 1, written only by the receiving thread (data/stream.zig): the message the last zero-copy receive
/// acquired, which `recvRelease` consumes: the tail its frame begins at, and the frame's length (0: none); and the
/// receive ring's head as this side last loaded it (data/ring_wait.zig; 0, where a new segment's indices start, until then).
pub const RecvLine = extern struct {
    acquired_tail: u64 = 0,
    acquired_frame: u64 = 0,
    head_seen: u64 = 0,
    /// The send ring's head as the last receive wait saw it: whether this side sent since (ring_wait.zig).
    send_head_seen: u64 = 0,
    pad: [32]u8 = @splat(0),
};

/// Cache line 2, written only by the sending thread (data/stream.zig): the zero-copy reservation the last
/// `sendAcquire` made for its commit: the head it begins at, and the message's length (0: none); the send ring's
/// tail as this side last loaded it (data/ring_wait.zig; 0 until then); the next RPC request's id (data/rpc.zig:
/// never 0, taken only by a request that was sent); and the head the last message published, which a receive wait
/// reads to tell whether this side sent since its last wait (data/ring_wait.zig).
pub const SendLine = extern struct {
    reserved_head: u64 = 0,
    reserved_len: u64 = 0,
    tail_seen: u64 = 0,
    rpc_next_id: u64 = 1,
    head_sent: std.atomic.Value(u64) = .init(0),
    pad: [24]u8 = @splat(0),
};

pub const Endpoint = struct {
    hot: Hot align(64),
    recv: RecvLine align(64) = .{},
    send: SendLine align(64) = .{},
    // Cold. Zig orders these as it likes; none can share the three lines above.
    io: Io,
    gpa: Allocator,
    /// The process-wide `Io`'s reference (the C boundary), released when the endpoint is freed on an application
    /// thread; null with the caller's `Io` (the native API).
    io_ref: ?process_state.Ref,
    /// The view's changes and the unmaps, and the ring waiters' interrupt (`cancel`, the watcher). Always taken
    /// uncancelably.
    mutex: Io.Mutex = .init,
    /// The watcher, once it runs.
    task: ?Io.Future(void) = null,
    /// The connection's handles and the forked mark (process_state.zig).
    handles: process_state.Handles,
    /// A client's: where it connects.
    address: platform.Address = undefined,
    /// A client's: the security its connect checks the listener against (docs/protocol.md §3.3).
    security: ?platform.Security = null,
    /// An accepted connection's listener, which `close` tells that its one client is gone (listener.zig).
    listener: ?*listener.Listener = null,

    pub fn loadStatus(ep: *const Endpoint) Status {
        return ep.hot.status.load(.seq_cst);
    }

    pub fn loadRequests(ep: *const Endpoint) Requests {
        return ep.hot.requests.load(.seq_cst);
    }

    /// Wakes this side's ring waiters (docs/protocol.md §6.2), after the caller published what they must see. Under the
    /// endpoint mutex, which `close` holds while it unmaps the segment, so the flags are mapped.
    pub fn interrupt(ep: *Endpoint) void {
        ep.mutex.lockUncancelable(ep.io);
        defer ep.mutex.unlock(ep.io);
        if (ep.hot.view_session.load(.monotonic) != null) ring_wait.wakeOwnWaiters(&ep.hot);
    }

    /// The publication of the complete session, under the endpoint mutex: the view's fields, then the session
    /// slot and the number (.release).
    pub fn publishSession(ep: *Endpoint, session: *Session) void {
        ep.mutex.lockUncancelable(ep.io);
        defer ep.mutex.unlock(ep.io);
        ep.hot.view_header.store(session.header(), .monotonic);
        ep.hot.view_data.store(session.data(), .monotonic);
        ep.hot.view_cap.store(session.cap, .monotonic);
        ep.hot.view_server.store(@intFromBool(session.server), .monotonic);
        ep.hot.view_session.store(session, .release);
        ep.hot.view_number.store(1, .release);
    }

    /// Clears the view and the session slot, then unmaps and frees the session, under the endpoint mutex: `close`
    /// once the watcher has stopped.
    fn dropSession(ep: *Endpoint) void {
        ep.mutex.lockUncancelable(ep.io);
        defer ep.mutex.unlock(ep.io);
        const session = ep.hot.view_session.load(.monotonic) orelse return;
        ep.hot.view_number.store(0, .release);
        ep.hot.view_session.store(null, .release);
        ep.hot.view_header.store(null, .monotonic);
        ep.hot.view_data.store(null, .monotonic);
        ep.hot.view_cap.store(0, .monotonic);
        ep.hot.view_server.store(0, .monotonic);
        session.unmap();
        ep.gpa.destroy(session);
    }
};

comptime {
    // Each hot line is a whole cache line of its own, whatever order Zig gives the fields
    std.debug.assert(@sizeOf(Hot) == 64 and @sizeOf(RecvLine) == 64 and @sizeOf(SendLine) == 64);
    std.debug.assert(@offsetOf(Endpoint, "hot") % 64 == 0 and @offsetOf(Endpoint, "recv") % 64 == 0);
    std.debug.assert(@offsetOf(Endpoint, "send") % 64 == 0);
    std.debug.assert(@alignOf(Endpoint) >= 64);
    const offsets = .{
        .{ "view_number", 0 }, .{ "view_header", 8 },  .{ "view_data", 16 }, .{ "view_session", 24 },
        .{ "view_cap", 32 },   .{ "view_server", 36 }, .{ "requests", 40 },  .{ "status", 44 },
        .{ "reserved", 48 },
    };
    for (offsets) |field| std.debug.assert(@offsetOf(Hot, field[0]) == field[1]);
}

/// The lifecycle's errors: each is the `fipc_result_t` code of the same name. `Cancelled` is a listener's own
/// `cancel` (a connection has no handle to cancel until `connect` returns).
pub const Error = error{ Invalid, NoMemory, Timeout, Cancelled };

/// Where an endpoint's `Io` comes from.
pub const IoSource = union(enum) {
    /// The caller's (the native API): it must run concurrent tasks (as `Io.Threaded` does), and the caller must not
    /// cancel the library's tasks.
    caller: Io,
    /// The process-wide instance (the C boundary, process_state.zig), acquired here and released when the endpoint is
    /// freed.
    process,

    /// The `Io` of an object on this source, and the process-wide reference taken for it (null with the caller's
    /// `Io`), which the object releases when it is freed.
    pub fn acquire(source: IoSource) error{NoMemory}!struct { Io, ?process_state.Ref } {
        return switch (source) {
            .caller => |io| .{ io, null },
            .process => {
                const ref = process_state.acquire() catch return error.NoMemory;
                return .{ ref.io, ref };
            },
        };
    }
};

// === The application side ===

/// `fipc_connect`: validates the name, creates the endpoint and sets the session up on the caller's thread, within
/// `timeout` (control.zig `handshake`: it connects to the listener again and again while nobody listens, and waits for
/// the listener's `accept` to offer the segment); then starts the watcher. OK with the session published (the peer may
/// have ended it since: the data calls say so); the failure's error (INVALID: another protocol version, or another
/// user; NO_MEMORY); Timeout, and nothing is left behind.
pub fn connect(io_source: IoSource, gpa: Allocator, name: []const u8, timeout: Io.Timeout) Error!*Endpoint {
    if (!names.isValid(name)) return error.Invalid;
    const ep = try create(io_source, gpa);
    errdefer free(ep);
    ep.address = platform.addressOf(name) catch return error.NoMemory;
    ep.security = platform.Security.init() catch return error.NoMemory;
    const session = try control.handshake(ep, timeout.toTimestamp(ep.io));
    try start(ep, session);
    return ep;
}

/// A new endpoint, before its session: `connect`'s, or an accepted connection's, which the listener's `accept` creates
/// with the client's segment (listener.zig), on the listener's `Io` source and allocator, and either starts
/// (`startAccepted`) or frees (`free`).
pub fn create(io_source: IoSource, gpa: Allocator) error{NoMemory}!*Endpoint {
    const io, const ref = try io_source.acquire();
    errdefer if (ref) |r| process_state.release(r);
    const ep = gpa.create(Endpoint) catch return error.NoMemory;
    ep.* = .{
        .hot = std.mem.zeroes(Hot), // HANDSHAKE, no session, no request
        .io = io,
        .gpa = gpa,
        .io_ref = ref,
        .handles = .{ .view_number = &ep.hot.view_number },
    };
    ep.handles.join();
    return ep;
}

/// Gives an accepted connection its session (the listener has moved the connection into its handles), publishes
/// CONNECTED and starts its watcher. On failure (no task) the connection is freed with what it was given, and the
/// client reads EOF.
pub fn startAccepted(ep: *Endpoint, session: *Session) error{NoMemory}!void {
    start(ep, session) catch |err| {
        free(ep);
        return err;
    };
}

/// Publishes the session, then CONNECTED (so a data call that sees the phase finds the view), and starts the watcher.
/// On failure (no task) the connection is closed and the session dropped; the caller frees the endpoint.
fn start(ep: *Endpoint, session: *Session) error{NoMemory}!void {
    ep.publishSession(session);
    ep.hot.status.store(Status{ .phase = .connected }, .seq_cst);
    ep.task = ep.io.concurrent(control.runWatcher, .{ep}) catch {
        ep.handles.close(.connected);
        ep.dropSession();
        return error.NoMemory;
    };
}

/// `fipc_cancel`: from now until `close`, every call of this connection that would wait returns Cancelled; calls
/// that needn't wait still work. The session goes on, and the peer sees nothing.
pub fn cancel(ep: *Endpoint) void {
    _ = ep.hot.requests.fetchOr(Requests{ .cancel = true }, .seq_cst);
    // An inherited endpoint (a forked child's) only takes the bit: no `Io` call, no lock
    if (ep.handles.forked) return;
    ep.interrupt();
}

/// `fipc_close`: stops the watcher (the peer's calls then report the end), unmaps the session and frees the endpoint;
/// an accepted connection then tells its listener that its one client is gone. Precondition: no other thread inside a
/// call on this connection.
pub fn close(ep: *Endpoint) void {
    if (ep.handles.forked) return freeInherited(ep);
    teardown(ep);
}

/// The connection's session has ended (the peer is gone), as the native API reports it.
pub fn ended(ep: *const Endpoint) bool {
    return ep.loadStatus().phase == .peer_dead;
}

// === Teardown ===

fn teardown(ep: *Endpoint) void {
    if (ep.task) |*task| {
        requestStop(ep);
        platform.endTask(ep.io, task);
    }
    ep.dropSession();
    const l = ep.listener;
    free(ep);
    if (l) |x| x.connectionClosed();
}

/// The stop rule's first steps, before `platform.endTask`: `shutdown`, which the watcher reads once its read returns,
/// and `unblock` of the connection under the handle lock, which ends a blocked read where the OS can (end of stream;
/// elsewhere `endTask` cancels it).
fn requestStop(ep: *Endpoint) void {
    _ = ep.hot.requests.fetchOr(Requests{ .shutdown = true }, .seq_cst);
    process_state.lockHandles();
    defer process_state.unlockHandles();
    if (ep.handles.get(.connected)) |h| platform.unblock(h);
}

/// The last step: with the watcher gone (or never started), the security, the registry entry, the memory and the `Io`
/// reference.
pub fn free(ep: *Endpoint) void {
    if (ep.security) |*s| s.deinit();
    ep.handles.leave();
    const gpa = ep.gpa;
    const ref = ep.io_ref;
    gpa.destroy(ep);
    if (ref) |r| process_state.release(r);
}

/// A forked child's `close` of an inherited connection (docs/protocol.md §7): frees what is provably complete, the
/// session in the slot, and leaks whatever a parent thread was changing at the fork, among them its listener's count.
/// No endpoint mutex (a parent thread may have held it), no `Io` call, no handle (the child handler closed its copies),
/// no await, and the `Io` reference is not released: it belongs to the abandoned instance.
fn freeInherited(ep: *Endpoint) void {
    if (ep.hot.view_session.load(.acquire)) |session| {
        session.unmap();
        ep.gpa.destroy(session);
    }
    ep.handles.leave();
    ep.gpa.destroy(ep);
}
