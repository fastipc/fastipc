//! An endpoint is one process's end of a stream connection, or its place in a broadcast, a work queue or a service:
//! `fipc_conn_t`
//! in C, `Conn` in the Zig module and `Endpoint` here are one object, which the application's threads and the
//! connection's watcher share. This file is the part of its lifecycle every kind shares: `create`, the start (`start`,
//! `startWatching`), `cancel`, `close` and `free`. Each kind creates its endpoints: a stream's client
//! (stream/client.zig `connect`) and server (stream/server.zig `accept`), a broadcast's publisher
//! (broadcast/server.zig `create`) and subscribers (broadcast/client.zig `join`), a work queue's producers and
//! consumers (queue/client.zig `join`), a service's clients and servers (service/client.zig `join`). Nothing of the
//! lifecycle is in shared memory.
//!
//! A stream connection has one session: `connect` sets it up on the caller's thread and returns once it is, an
//! accepted connection has it from the start, and once it ended the application closes the connection and accepts or
//! connects a new one. Once the session is published, the connection's watcher (control.zig `runWatcher`) reads the
//! control connection until the peer's end, which it publishes.
//!
//! What the endpoint is, is its view kind (`ViewKind`): a stream connection's side, a broadcast's publisher or
//! subscriber, a work queue's producer or consumer, or a service's client or server. A subscriber and every member of a
//! queue or a service have their view of the segment, their own state and a watcher as a connection has; the publisher
//! has the owner's mapping instead, and no control connection or watcher. The calls that depend on the kind branch on
//! it once, in core/calls.zig: the data calls, and the kinds' sides of `interrupt` and `close`.
//!
//! Who writes what:
//!   - `phase` and the view: the thread that sets the session up, before the watcher starts. After that the watcher
//!     writes `phase` (PEER_DEAD, or a subscriber's UNSUBSCRIBED), and `close` clears the view once the watcher has
//!     stopped.
//!   - `requests`: application threads only. `cancel` and `shutdown` are set with fetch-or and never cleared.
//!
//! Locks, outermost first: the reference-count lock (process_state.zig), the endpoint mutex, the handle lock
//! (process_state.zig). None is held across a blocking call, and no path here holds two.
//!
//! The import cycles with calls.zig, control.zig and the kinds are deliberate: this file starts the watcher
//! (`control.runWatcher`), calls the kinds through calls.zig and tells an accepted connection's listener of its close
//! (stream/listener.zig), and they work on the endpoint. Zig analyzes them lazily.

const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;
const wire_protocol = @import("../wire_protocol.zig");
const calls = @import("calls.zig");
const control = @import("control.zig");
const names = @import("names.zig");
const platform = @import("../platform.zig");
const process_state = @import("process_state.zig");

const Listener = @import("../stream/listener.zig").Listener;
const Session = @import("shared_memory.zig").Session;

/// The bytes each hot line takes: 128 on x86-64 (its spatial prefetcher fetches 64-byte lines in pairs) and on aarch64
/// (Apple's cores' line), so that no thread's stores take another's line.
const line = std.atomic.cache_line;

/// The `requests` word: written only by application threads, never cleared. `cancel` and `shutdown` are set with
/// fetch-or.
pub const Requests = packed struct(u8) {
    cancel: bool = false,
    shutdown: bool = false,
    reserved: u6 = 0,
};

/// The public phase (`Hot.phase`): `handshake` until the session is published, then `connected` until the peer's end.
/// A gated broadcast subscriber dropped from its channel ends in `unsubscribed` instead, for good (broadcast/reader.zig).
pub const Phase = enum(u8) {
    handshake,
    connected,
    peer_dead,
    unsubscribed,
};

/// Cache line 0, which every data call reads; it changes only at lifecycle events. The view (`stream`, `view_*`) is
/// the data path's only source for the rings: whoever publishes the session writes its fields (.monotonic), then the
/// session slot and `view_published` (.release), under the endpoint mutex; a reader loads `view_published` (.acquire)
/// and, if it is set, the fields. While data calls may run, the view changes only one way: from a session to none, in a
/// forked child. The fields go largest first, so that the padding is at the end.
pub const Hot = extern struct {
    /// A stream's rings; null for the other kinds.
    stream: StreamRings,
    /// The session slot: set (.release) only once the session is complete, cleared before its segment is unmapped,
    /// so a forked child can free what it finds there (docs/protocol.md §7).
    view_session: std.atomic.Value(?*Session),
    /// The kind's own object, where it has one: a publisher's channel (broadcast/server.zig), a subscriber's
    /// receive state (broadcast/reader.zig), a queue member's state (queue/client.zig), a service member's
    /// (service/client.zig).
    view_object: std.atomic.Value(?*anyopaque),
    /// The ring capacity, this process's own copy: never read again from the shared header (docs/protocol.md §4.1).
    view_capacity: std.atomic.Value(u32),
    requests: std.atomic.Value(Requests),
    phase: std.atomic.Value(Phase),
    /// What the connection is: a connection's side (`stream`), a broadcast's publisher or subscriber, a queue's or a
    /// service's member. The data calls branch on it once, after `view_published` (core/calls.zig).
    view_kind: std.atomic.Value(ViewKind),
    /// Set once the session is published, clear before it and in a forked child: the view's publication point.
    view_published: std.atomic.Value(bool),
    /// A stream's: set on the listener's side, which writes s2c.
    view_server: std.atomic.Value(bool),
    pad: [line - 41]u8,
};

/// A stream's rings, in its session's mapping (stream/ring.zig).
pub const StreamRings = extern struct {
    header: std.atomic.Value(?*wire_protocol.SegmentHeader),
    data: std.atomic.Value(?[*]u8),
};

/// What a connection is (`Hot.view_kind`): the list of kinds, which core/calls.zig switches on.
pub const ViewKind = enum(u8) {
    /// One side of a connection: two rings (stream/data_path.zig).
    stream,
    /// A broadcast's publisher (broadcast/server.zig): no session in the view, the channel's mapping instead.
    broadcast_publisher,
    /// A broadcast's subscriber, lossy or gated (its receive state, broadcast/reader.zig `Reader`, says).
    broadcast_subscriber,
    /// A work queue's producer and consumer (queue/client.zig `State`), with its own mapping.
    queue_producer,
    queue_consumer,
    /// A service's client and server (service/client.zig `State`), with its own mapping.
    service_client,
    service_server,
};

/// A connection's view, as `Endpoint.publishView` publishes it: a stream connection's side, its complete session with
/// its `header` and its `rings` (stream/segment.zig), flat in the hot line; or any other kind's object and capacity,
/// with its session (a subscriber's, a queue member's or a service member's mapping; none for a publisher, whose
/// mapping is the channel's).
pub const View = union(enum) {
    stream: struct { session: *Session, header: *wire_protocol.SegmentHeader, rings: [*]u8 },
    object: struct { kind: ViewKind, session: ?*Session, object: *anyopaque, capacity: u32 },

    fn session(view: View) ?*Session {
        return switch (view) {
            .stream => |stream| stream.session,
            .object => |object| object.session,
        };
    }
};

/// Cache line 1, written only by the receiving thread (stream/data_path.zig).
pub const RecvLine = extern struct {
    /// Where the frame the last zero-copy receive acquired begins, for `recvRelease`.
    acquired_tail: u64 = 0,
    /// That frame's length; 0: none acquired.
    acquired_frame: u64 = 0,
    /// The receive ring's head as this side last loaded it (stream/ring_wait.zig); 0 until then, where a new segment's
    /// indices start.
    head_seen: u64 = 0,
    /// The send ring's head as the last receive wait, or the last poll that took a frame after this side sent, saw it:
    /// whether this side sent since (ring_wait.zig), and whether a send follows an answer (data_path.zig
    /// `prefetchNextFrame`), which the sending thread reads.
    send_head_seen: std.atomic.Value(u64) = .init(0),
    /// The receive ring's tail plus 1 where a poll last found the frame unwritten and its stamp slot without its stamp:
    /// the next poll at that tail may take the frame by its stamp (ring_wait.zig); 0: none.
    unstamped_tail: u64 = 0,
    /// The receive ring's tail as this side last published it (stream/ring_wait.zig `consume`): only this side moves
    /// it, so its waits read it here instead of loading it back from the ring. 0 until then.
    tail: u64 = 0,
    /// The hints an empty poll pauses before it looks once more (stream/ring_wait.zig `pollPaced`).
    poll_pace: u32 = 1,
    /// Whether the last frame a receive wait's spin, or a poll, found was short enough to carry a stamp: the next spins
    /// and polls watch for stamps (ring_wait.zig).
    poll_stamps: bool = false,
    pad: [line - 53]u8 = @splat(0),
};

/// Cache line 2, written only by the sending thread (stream/data_path.zig).
pub const SendLine = extern struct {
    /// Where the reservation of the last `sendAcquire` begins, for its commit.
    reserved_head: u64 = 0,
    /// The reserved message's length; 0: none reserved.
    reserved_len: u64 = 0,
    /// The send ring's tail as this side last loaded it (stream/ring_wait.zig); 0 until then.
    tail_seen: u64 = 0,
    /// The head after the last message this side published. A receive wait reads it to tell whether this side sent
    /// since its last wait (stream/ring_wait.zig).
    head_sent: std.atomic.Value(u64) = .init(0),
    /// The send ring's head as this side last published it, frames and PADs (stream/data_path.zig): only this side
    /// moves it, so its waits read it here instead of loading it back from the ring. 0 until then.
    head: u64 = 0,
    pad: [line - 40]u8 = @splat(0),
};

/// A service client's requests whose replies it hasn't received yet, oldest first, as the native Zig module counts them
/// (root.zig `Conn.call`; the C API's calls leave them alone), so that a call never returns the reply to another
/// request: first the one whose call gave up on its reply (it timed out or was cancelled), which the next call
/// receives and drops (at most one, as a call first drops what is owed), then those sent with `send` or `commit`,
/// whose replies are the caller's to receive. A request is counted before its send, since a thread that receives may
/// take its reply before the send returns, and uncounted if the send fails; a reply or Abandoned takes the oldest off.
pub const Owed = struct {
    /// The receiving thread's (a call is both sides): 0 or 1.
    calls: u32 = 0,
    /// The sending thread adds, the receiving thread takes off.
    sends: std.atomic.Value(u32) = .init(0),
};

pub const Endpoint = struct {
    hot: Hot align(line),
    recv: RecvLine align(line) = .{},
    send: SendLine align(line) = .{},
    // Cold. Zig orders these as it likes; none can share the three lines above.
    io: Io,
    gpa: Allocator,
    /// The process-wide `Io`'s reference, released when the endpoint is freed on an application thread.
    io_ref: process_state.Ref,
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
    /// An accepted connection's listener, which `close` tells that its one client is gone (stream/listener.zig).
    listener: ?*Listener = null,
    /// A service client's replies still owed, which only the native Zig module counts (root.zig `Conn.call`).
    owed: Owed = .{},

    pub fn loadPhase(endpoint: *const Endpoint) Phase {
        return endpoint.hot.phase.load(.seq_cst);
    }

    pub fn loadRequests(endpoint: *const Endpoint) Requests {
        return endpoint.hot.requests.load(.seq_cst);
    }

    /// Wakes this side's waits (core/calls.zig `interrupt`: a stream connection's ring waiters, docs/protocol.md §6.2,
    /// or a channel connection's waits), after the caller published what they must see. Under the endpoint mutex, which
    /// `close` holds while it unmaps the segment, so the flags are mapped.
    pub fn interrupt(endpoint: *Endpoint) void {
        endpoint.mutex.lockUncancelable(endpoint.io);
        defer endpoint.mutex.unlock(endpoint.io);
        calls.interrupt(endpoint);
    }

    /// The publication of a connection's view under the endpoint mutex: its fields, then the session slot and
    /// `view_published` (.release).
    pub fn publishView(endpoint: *Endpoint, view: View) void {
        endpoint.mutex.lockUncancelable(endpoint.io);
        defer endpoint.mutex.unlock(endpoint.io);
        switch (view) {
            .stream => |stream| {
                endpoint.hot.view_kind.store(.stream, .monotonic);
                endpoint.hot.stream.header.store(stream.header, .monotonic);
                endpoint.hot.stream.data.store(stream.rings, .monotonic);
                endpoint.hot.view_capacity.store(stream.session.capacity, .monotonic);
                endpoint.hot.view_server.store(stream.session.server, .monotonic);
            },
            .object => |object| {
                std.debug.assert(object.kind != .stream);
                endpoint.hot.view_kind.store(object.kind, .monotonic);
                endpoint.hot.view_object.store(object.object, .monotonic);
                endpoint.hot.view_capacity.store(object.capacity, .monotonic);
            },
        }
        endpoint.hot.view_session.store(view.session(), .release);
        endpoint.hot.view_published.store(true, .release);
    }

    /// Clears the view and the session slot, then unmaps and frees the session and its kind's object (a subscriber's
    /// receive state, a queue member's state), under the endpoint mutex: `close` once the watcher has stopped.
    fn dropView(endpoint: *Endpoint) void {
        endpoint.mutex.lockUncancelable(endpoint.io);
        defer endpoint.mutex.unlock(endpoint.io);
        const session = endpoint.hot.view_session.load(.monotonic) orelse return;
        const kind = endpoint.hot.view_kind.load(.monotonic);
        const object = endpoint.hot.view_object.load(.monotonic);
        endpoint.hot.view_published.store(false, .release);
        endpoint.hot.view_session.store(null, .release);
        endpoint.hot.stream.header.store(null, .monotonic);
        endpoint.hot.stream.data.store(null, .monotonic);
        endpoint.hot.view_capacity.store(0, .monotonic);
        endpoint.hot.view_server.store(false, .monotonic);
        endpoint.hot.view_kind.store(.stream, .monotonic);
        endpoint.hot.view_object.store(null, .monotonic);
        freeView(endpoint.gpa, session, kind, object);
    }
};

comptime {
    // Each hot line is a whole cache line of its own, whatever order Zig gives the fields
    std.debug.assert(@sizeOf(Hot) == line and @sizeOf(RecvLine) == line and @sizeOf(SendLine) == line);
    std.debug.assert(@offsetOf(Endpoint, "hot") % line == 0 and @offsetOf(Endpoint, "recv") % line == 0);
    std.debug.assert(@offsetOf(Endpoint, "send") % line == 0);
    std.debug.assert(@alignOf(Endpoint) >= line);
    const offsets = .{
        .{ "stream", 0 },       .{ "view_session", 16 }, .{ "view_object", 24 }, .{ "view_capacity", 32 },
        .{ "requests", 36 },    .{ "phase", 37 },        .{ "view_kind", 38 },   .{ "view_published", 39 },
        .{ "view_server", 40 }, .{ "pad", 41 },
    };
    for (offsets) |field| std.debug.assert(@offsetOf(Hot, field[0]) == field[1]);
}

/// The lifecycle's errors (a stream's accept and connect, the joins): each is the `fipc_result_t` code of the same
/// name. `Cancelled` is a listener's own `cancel` (a connection has no handle to cancel until `connect` returns).
pub const Error = error{ Invalid, NoMemory, Timeout, Cancelled };

/// The creates' errors (a stream's, a broadcast's, a queue's, a service's): each is the `fipc_result_t` code of the
/// same name. None waits, so none times out; AddrInUse: another stream, broadcast, queue or service holds the name.
pub const CreateError = error{ Invalid, NoMemory, AddrInUse };

/// The data calls' errors (core/calls.zig): each is the `fipc_result_t` code of the same name. Overrun and
/// Unsubscribed are a channel subscriber's, Abandoned a service client's.
pub const DataError = error{ Invalid, Timeout, TooLarge, Disconnected, Cancelled, Overrun, Unsubscribed, Abandoned };

// === The application side ===

/// The start of a client's `connect` and `join`: the name validated, a new endpoint with the name's address and the
/// security.
pub fn newClient(gpa: Allocator, name: []const u8) Error!*Endpoint {
    if (!names.isValid(name)) return error.Invalid;
    const endpoint = try create(gpa);
    errdefer free(endpoint);
    endpoint.address = platform.addressOf(name) catch return error.NoMemory;
    endpoint.security = platform.Security.init() catch return error.NoMemory;
    return endpoint;
}

/// A new endpoint, before its session, on the process-wide `Io` and `gpa`. Its callers:
///   - a client (`newClient`);
///   - a listener's `accept`, which creates the accepted connection with the client's segment (stream/server.zig) and
///     then starts it (`startAccepted`) or frees it (`free`);
///   - a broadcast's publisher (broadcast/server.zig).
pub fn create(gpa: Allocator) error{NoMemory}!*Endpoint {
    const ref = process_state.acquire() catch return error.NoMemory;
    errdefer process_state.release(ref);
    const endpoint = gpa.create(Endpoint) catch return error.NoMemory;
    endpoint.* = .{
        .hot = std.mem.zeroes(Hot), // HANDSHAKE, no session, no request
        .io = ref.io,
        .gpa = gpa,
        .io_ref = ref,
        .handles = .{ .view_published = &endpoint.hot.view_published },
    };
    endpoint.handles.join();
    return endpoint;
}

/// Gives an accepted connection its session (the listener has moved the connection into its handles), publishes
/// CONNECTED and starts its watcher. On failure (no task) the connection is freed with what it was given, and the
/// client reads EOF.
pub fn startAccepted(endpoint: *Endpoint, session: *Session, header: *wire_protocol.SegmentHeader, rings: [*]u8) error{NoMemory}!void {
    start(endpoint, session, header, rings) catch |err| {
        free(endpoint);
        return err;
    };
}

/// Publishes a stream connection's session, then CONNECTED (so a data call that sees the phase finds the view), and
/// starts the watcher. On failure (no task) the connection is closed and the session dropped; the caller frees the
/// endpoint.
pub fn start(endpoint: *Endpoint, session: *Session, header: *wire_protocol.SegmentHeader, rings: [*]u8) error{NoMemory}!void {
    endpoint.publishView(.{ .stream = .{ .session = session, .header = header, .rings = rings } });
    try startWatching(endpoint);
}

/// CONNECTED, then the watcher, once the view is published. On failure (no task) the kind leaves (core/calls.zig
/// `leave`), the connection is closed and the session dropped.
pub fn startWatching(endpoint: *Endpoint) error{NoMemory}!void {
    endpoint.hot.phase.store(.connected, .seq_cst);
    endpoint.task = endpoint.io.concurrent(control.runWatcher, .{endpoint}) catch {
        calls.leave(endpoint);
        endpoint.handles.close(.connected);
        endpoint.dropView();
        return error.NoMemory;
    };
}

/// `fipc_cancel`: from now until `close`, every call of this connection that would wait returns Cancelled; calls
/// that needn't wait still work. The session goes on, and the peer sees nothing.
pub fn cancel(endpoint: *Endpoint) void {
    _ = endpoint.hot.requests.fetchOr(Requests{ .cancel = true }, .seq_cst);
    // An inherited endpoint (a forked child's) only takes the bit: no `Io` call, no lock
    if (endpoint.handles.forked) return;
    endpoint.interrupt();
}

/// `fipc_close`: stops the watcher (the peer's calls then report the end), unmaps the session and frees the endpoint;
/// an accepted connection then tells its listener that its one client is gone. First the kind leaves (core/calls.zig
/// `leave`): a gated subscriber leaves its broadcast (the publisher stops waiting for it at once), a publisher ends the
/// broadcast (broadcast/server.zig `closePublisher`), a queue consumer releases the message it holds, and a service's
/// server abandons the request it holds.
/// Precondition: no other thread inside a call on this connection.
pub fn close(endpoint: *Endpoint) void {
    if (endpoint.handles.forked) return freeInherited(endpoint);
    teardown(endpoint);
}

/// The session has ended (the peer is gone), or a subscriber's broadcast (its publisher closed it), or a queue
/// member's queue, or a service member's service, as the native API reports it.
pub fn ended(endpoint: *Endpoint) bool {
    if (endpoint.loadPhase() == .peer_dead) return true;
    return endpoint.hot.view_published.load(.acquire) and calls.channelEnded(endpoint);
}

// === Teardown ===

fn teardown(endpoint: *Endpoint) void {
    calls.leave(endpoint);
    if (endpoint.task) |*task| {
        requestStop(endpoint);
        platform.endTask(endpoint.io, task);
    }
    endpoint.dropView();
    const accepted_by = endpoint.listener;
    free(endpoint);
    if (accepted_by) |listener| listener.connectionClosed();
}

/// The stop rule's first steps, before `platform.endTask`: `shutdown`, which the watcher reads once its read returns,
/// and `unblock` of the connection under the handle lock, which ends a blocked read where the OS can (end of stream;
/// elsewhere `endTask` cancels it).
fn requestStop(endpoint: *Endpoint) void {
    _ = endpoint.hot.requests.fetchOr(Requests{ .shutdown = true }, .seq_cst);
    process_state.lockHandles();
    defer process_state.unlockHandles();
    if (endpoint.handles.get(.connected)) |h| platform.unblock(h);
}

/// The last step: with the watcher gone (or never started), the security, the registry entry, the memory and the `Io`
/// reference.
pub fn free(endpoint: *Endpoint) void {
    if (endpoint.security) |*s| s.deinit();
    endpoint.handles.leave();
    const gpa = endpoint.gpa;
    const ref = endpoint.io_ref;
    gpa.destroy(endpoint);
    process_state.release(ref);
}

/// A forked child's `close` of an inherited connection (docs/protocol.md §7): frees what is provably complete, the
/// session in the slot (and its kind's object), and leaks whatever a parent thread was changing at the
/// fork, among them its listener's count, and a publisher's whole channel.
/// No endpoint mutex (a parent thread may have held it), no `Io` call, no handle (the child handler closed its copies),
/// no await, and the `Io` reference is not released: it belongs to the abandoned instance.
fn freeInherited(endpoint: *Endpoint) void {
    if (endpoint.hot.view_session.load(.acquire)) |session| {
        freeView(endpoint.gpa, session, endpoint.hot.view_kind.load(.monotonic), endpoint.hot.view_object.load(.monotonic));
    }
    endpoint.handles.leave();
    endpoint.gpa.destroy(endpoint);
}

/// Unmaps and frees a view's session, and its kind's object (`object`: a subscriber's receive state, a queue member's
/// or a service member's state).
fn freeView(gpa: Allocator, session: *Session, kind: ViewKind, object: ?*anyopaque) void {
    session.unmap();
    gpa.destroy(session);
    calls.freeObject(gpa, kind, object);
}
