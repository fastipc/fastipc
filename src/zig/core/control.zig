//! A connection's control connection (docs/protocol.md §3.3, §7), and what every kind does with it:
//!   - the name: its claim (`claimName`) and the accept of its clients (`acceptClient`, `dropClient`,
//!     `nextListening`);
//!   - the frame reads, and a handshake's back-off (`Backoff`; the deadlines are core/wait.zig's);
//!   - the client's steps that every kind's handshake takes: `retrying`, `connectOnce`, `readOffered`, `awaitOffer`
//!     (a broadcast's, a queue's or a service's) and `attach`;
//!   - the watcher (`runWatcher`): a task on the endpoint's `Io`, started once the session is published, that reads
//!     the control connection until the peer's end.
//!
//! The handshakes themselves live with their kinds: a stream's (stream/client.zig `handshake`, stream/server.zig
//! `accept`), a broadcast's (broadcast/client.zig `join`, broadcast/server.zig), a queue's (queue/client.zig `join`,
//! queue/server.zig) and a service's (service/client.zig `join`, service/server.zig). A broadcast's, a queue's or a
//! service's owner admits its joiners on its poller, through core/admission.zig.
//!
//! A client's handshake owns the handles it opens until it returns: on any failure it closes them, under the handle
//! lock (process_state.zig), so `connect` leaves nothing behind. The watcher then owns the connection, and is the only
//! one that closes it: `close` stops the watcher first (endpoint.zig `requestStop`, then `platform.endTask`).
//!
//! This file, endpoint.zig, calls.zig and the kinds import each other: the kinds' handshakes take the steps here, and
//! the watcher reads by the endpoint's kind (calls.zig `readControl`). The cycles are deliberate (endpoint.zig's top
//! comment).

const std = @import("std");
const Io = std.Io;
const build_options = @import("build_options");
const calls = @import("calls.zig");
const log = @import("log.zig");
const platform = @import("../platform.zig");
const process_state = @import("process_state.zig");
const shared_memory = @import("shared_memory.zig");
const test_hooks = @import("test_hooks.zig");
const wait = @import("wait.zig");
const wire = @import("wire.zig");
const wire_protocol = @import("../wire_protocol.zig");

const Endpoint = @import("endpoint.zig").Endpoint;
const Error = @import("endpoint.zig").Error;
const Session = shared_memory.Session;

// === The name and its clients ===

/// Claims a name for a stream, a broadcast, a queue or a service (docs/protocol.md §3.2): its listening handle,
/// registered in `handles` (`.listening`) under the handle lock. AddrInUse if another stream, broadcast, queue or
/// service holds the name.
pub fn claimName(handles: *process_state.Handles, address: *const platform.Address, security: *const platform.Security) error{ NoMemory, AddrInUse }!void {
    process_state.lockHandles();
    defer process_state.unlockHandles();
    const h = (platform.claim(address, security) catch return error.NoMemory) orelse return error.AddrInUse;
    handles.register(.listening, h);
}

/// What `acceptClient` came to.
pub const Accepted = union(enum) {
    /// A client's connection, registered as the client's node's `.connected`.
    connection: platform.Handle,
    /// No client within the wait.
    idle,
    /// No client after all (the wait was cancelled, or the client left before the accept), or one that was dropped (a
    /// fork during the accept, a process of another user): look again at once.
    retry_now,
    /// The accept failed, or the OS couldn't tell the client's user: back off, then look again.
    back_off,
};

/// Waits up to `wait_ms` milliseconds (null: no limit; 0: only looks) for a client on `server`'s listening handle, or
/// for the cancel signal `cancel`, and accepts the client into `client`'s `.connected` (a stream's listener passes its
/// own node twice). Two checks then drop the client:
///   - a fork during the accept: the accepted handle is the one handle not opened under the handle lock, so a forked
///     child may hold a copy of it (the check runs under the lock);
///   - a peer of another user.
/// A client that connected on the listening handle itself (a Windows pipe instance) takes that handle out of
/// `server`, and the caller makes the next one (`nextListening`).
pub fn acceptClient(server: *process_state.Handles, client: *process_state.Handles, cancel: ?platform.Handle, wait_ms: ?u32) Accepted {
    var forks = process_state.forkCount();
    const handle = switch (platform.acceptWithin(server.get(.listening).?, cancel, wait_ms, &forks)) {
        .idle => return .idle,
        .cancelled => return .retry_now,
        .done => |outcome| switch (outcome) {
            .connection => |h| h,
            .closing => return .retry_now,
            .transient, .other => return .back_off,
        },
    };
    const forked = forked: {
        process_state.lockHandles();
        defer process_state.unlockHandles();
        if (server.get(.listening) == handle) _ = server.unregister(.listening);
        client.register(.connected, handle);
        break :forked process_state.forkCount() != forks;
    };
    if (build_options.test_hooks) test_hooks.crashIfAt(.accepted);
    if (forked) {
        dropClient(server, client, true);
        return .retry_now;
    }
    const peer = platform.peer(handle, .listener) orelse {
        dropClient(server, client, false);
        return .back_off;
    };
    if (!peer.same_user) {
        log.warn(@src(), "a process of another user connected; dropped", .{});
        dropClient(server, client, false);
        return .retry_now;
    }
    return .{ .connection = handle };
}

/// Ends an accepted client's connection, `client`'s `.connected` (`forked`: a child forked during the accept may hold
/// a copy, which is ended too, docs/protocol.md §7). A handle the client connected on itself (a Windows pipe instance)
/// becomes `server`'s listening handle again, unless another one listens: closing it could give up the name.
pub fn dropClient(server: *process_state.Handles, client: *process_state.Handles, forked: bool) void {
    process_state.lockHandles();
    defer process_state.unlockHandles();
    const handle = client.unregister(.connected) orelse return;
    const again = platform.dropClient(handle, forked) orelse return;
    if (server.get(.listening) == null) server.register(.listening, again) else platform.close(again);
}

/// Where the accept took the listening handle (a Windows pipe instance), the next listening handle for the name
/// (docs/protocol.md §4.2), into `server`'s `slot`: `.listening` to accept on at once, or `.spare` to keep until the
/// client's hand-over. Nothing where the listening handle stays (a socket).
pub fn nextListening(server: *process_state.Handles, slot: process_state.Handles.Slot, address: *const platform.Address, security: *const platform.Security) error{SystemResources}!void {
    process_state.lockHandles();
    defer process_state.unlockHandles();
    const next = platform.nextListening(address, security) catch return error.SystemResources;
    if (next) |h| server.register(slot, h);
}

// === The back-off ===

/// A handshake's back-off delay (docs/protocol.md §3.2, §3.3): 1 ms, doubled by each back-off up to 100 ms, and never
/// past the call's deadline.
pub const Backoff = struct {
    delay_ms: u32 = 1,

    /// The next delay, at most `wait_ms` (the time left before the deadline, as core/wait.zig `msLeft` gives it; null:
    /// no deadline). Then doubles the delay, up to 100 ms.
    pub fn next(b: *Backoff, wait_ms: ?u32) u32 {
        const delay = if (wait_ms) |w| @min(w, b.delay_ms) else b.delay_ms;
        b.delay_ms = @min(2 * b.delay_ms, 100);
        return delay;
    }
};

// === The client's steps ===

/// `attemptFn(endpoint, deadline, args...)` (stream/client.zig `attempt`, broadcast/client.zig and queue/client.zig
/// `attemptJoin`) until it is done: after each Retry a back-off, until `deadline` (Timeout).
pub fn retrying(comptime T: type, endpoint: *Endpoint, deadline: ?Io.Clock.Timestamp, comptime attemptFn: anytype, args: anytype) Error!T {
    var backoff: Backoff = .{};
    while (true) {
        if (@call(.auto, attemptFn, .{ endpoint, deadline } ++ args)) |done| return done else |err| switch (err) {
            error.Retry => {},
            else => |e| return e,
        }
        if (wait.passed(endpoint.io, deadline)) return error.Timeout;
        platform.sleep(backoff.next(wait.msLeft(endpoint.io, deadline)));
    }
}

/// Connecting (docs/protocol.md §3.3): the listener, which must run as this user. `kind`: a stream's client warns of a
/// server in its own process (`warnSameProcess`); a broadcast's, a queue's or a service's joiner doesn't, since an
/// owner that joins its own broadcast, queue or service is the normal case.
pub fn connectOnce(endpoint: *Endpoint, comptime kind: enum { stream, member }) (Error || error{Retry})!void {
    const handle = handle: {
        process_state.lockHandles();
        defer process_state.unlockHandles();
        switch (platform.connect(&endpoint.address, &endpoint.security.?) catch return error.Retry) {
            .connected => |h| {
                endpoint.handles.register(.connected, h);
                break :handle h;
            },
            .no_listener, .busy => return error.Retry,
            .foreign => return foreign(),
        }
    };
    const peer = platform.peer(handle, .client) orelse return error.Retry;
    if (!peer.same_user) return foreign();
    if (kind == .stream and peer.pid == platform.pid()) warnSameProcess();
}

fn foreign() error{Invalid} {
    log.warn(@src(), "a process of another user holds the connection's name; the connection fails", .{});
    return error.Invalid;
}

/// A stream within one process is legal, as long as client and server run on different threads: `connect` waits for
/// the listener's `accept`. It is also the symptom of a listener that is never closed: it holds its name, and a later
/// connection of its process to that name pairs with it. Logged once per process, so that a process that pairs with
/// itself on purpose isn't flooded.
fn warnSameProcess() void {
    if (warned_same_process.swap(true, .monotonic)) return;
    log.warn(@src(), "a connection paired with a listener of this process: fipc_stream_connect waits for that listener's " ++
        "fipc_stream_accept, so client and server must run on different threads (or a listener never closed keeps its name)", .{});
}

var warned_same_process = std.atomic.Value(bool).init(false);

/// The listener's first frame, SEGMENT or OFFER (`what`, for the log), into `frame`, with the segment's handle where
/// it comes with the frame (into `.received`): the client waits until the connection is readable, then takes the frame
/// and its handle at once, under the handle lock. Retry: the listener dropped the attempt or left (EOF), or sent
/// something else than a frame and a handle.
pub fn readOffered(endpoint: *Endpoint, deadline: ?Io.Clock.Timestamp, frame: *wire.Frame, comptime what: []const u8) (Error || error{Retry})!void {
    const connection = endpoint.handles.get(.connected).?;
    if (platform.passes_segment_handle) {
        while (true) switch (platform.readableWithin(connection, wait.msLeft(endpoint.io, deadline))) {
            .done => break,
            .idle => if (wait.passed(endpoint.io, deadline)) return error.Timeout,
            .cancelled => unreachable, // nothing to cancel it
        };
        process_state.lockHandles();
        const received = platform.recvSegment(connection, frame);
        if (received) |h| endpoint.handles.register(.received, h) else |_| {}
        process_state.unlockHandles();
        _ = received catch |err| {
            if (err != error.EndOfStream) log.warn(@src(), "protocol error: " ++ what ++ ": {s}", .{@errorName(err)});
            return error.Retry;
        };
    } else {
        if (!try readFrameWithin(endpoint, connection, frame, deadline)) return error.Retry;
    }
}

/// AwaitOffer (docs/protocol.md §9.3, §10.3, §11.3): a broadcast's, a queue's or a service's joiner reads its first
/// frame (`readOffered`) and classifies it against the OFFER of a channel of `kind` (core/wire.zig `classifyOffer`),
/// which the kind then decodes (its frames.zig `decodeOffer`). Invalid for a stream's server, another channel protocol
/// version, or another kind, whose join the log names; Retry for a malformed frame.
pub fn awaitOffer(endpoint: *Endpoint, deadline: ?Io.Clock.Timestamp, comptime kind: wire_protocol.Kind) (Error || error{Retry})!wire.Frame {
    var frame: wire.Frame = undefined;
    try readOffered(endpoint, deadline, &frame, "OFFER");
    switch (wire.classifyOffer(&frame, kind)) {
        .offer => return frame,
        .stream_listener => {
            log.warn(@src(), "the name is a stream's: use fipc_stream_connect; the join fails", .{});
            return error.Invalid;
        },
        .other_version => {
            log.warn(@src(), "the owner runs another channel protocol version; the join fails", .{});
            return error.Invalid;
        },
        .other_kind => {
            const found = channelNoun(wire.headerOf(&frame).kind) orelse {
                log.warn(@src(), "the name is a channel's of a kind this build doesn't know; the join fails", .{});
                return error.Invalid;
            };
            log.warn(@src(), "the name is a {s}'s: use fipc_{s}_join; the join fails", .{ found, found });
            return error.Invalid;
        },
        .rejected => {
            log.warn(@src(), "protocol error: a malformed OFFER frame", .{});
            return error.Retry;
        },
    }
}

/// The name of a channel `kind` as its calls' prefix spells it ("broadcast", "queue", "service"); null for a stream's,
/// and for a kind this build doesn't know.
fn channelNoun(kind: wire_protocol.Kind) ?[]const u8 {
    return switch (kind) {
        .broadcast, .queue, .service => @tagName(kind),
        .stream, _ => null,
    };
}

/// Reads one whole frame within `deadline`: false at EOF, also in the middle of the frame (docs/protocol.md §3.1).
pub fn readFrameWithin(endpoint: *Endpoint, connection: platform.Handle, frame: *wire.Frame, deadline: ?Io.Clock.Timestamp) error{Timeout}!bool {
    var got: usize = 0;
    while (got < frame.len) switch (platform.readWithin(connection, frame[got..], null, wait.msLeft(endpoint.io, deadline))) {
        .done => |n| {
            if (n == 0) return false;
            got += n;
        },
        .idle => if (wait.passed(endpoint.io, deadline)) return error.Timeout,
        .cancelled => unreachable, // nothing to cancel it
    };
    return true;
}

/// Attach (docs/protocol.md §4.3): maps the offered segment and checks it with `attachFn` (stream/segment.zig,
/// broadcast/segment.zig, queue/segment.zig `attach`). A received segment handle is closed right after, whatever the
/// result. Retry: the segment is malformed, or gone (the listener left after sending its offer).
pub fn attach(endpoint: *Endpoint, comptime attachFn: anytype, offer: anytype) (Error || error{Retry})!*Session {
    const session = endpoint.gpa.create(Session) catch return error.NoMemory;
    const attached = attachFn(endpoint.handles.get(.received), offer);
    endpoint.handles.close(.received);
    session.* = attached catch |err| {
        endpoint.gpa.destroy(session);
        return switch (err) {
            error.Rejected, error.Gone => error.Retry,
            error.AccessDenied => accessDenied(),
            error.OutOfMemory => error.NoMemory,
        };
    };
    return session;
}

/// The peer's objects refused this process.
pub fn accessDenied() error{Invalid} {
    log.warn(@src(), "the peer runs as another user or at an incompatible elevation; the connection fails", .{});
    return error.Invalid;
}

// === The watcher ===

/// A connection's watcher task, started once its session is published (docs/protocol.md §7). It reads the control
/// connection by kind (calls.zig `readControl`) until the peer's end or `close`'s stop.
///   - On a stop it only closes the connection.
///   - At the peer's end it lets the kind apply the end first (calls.zig `applyPeerEnd`), then closes the connection.
///     For a stream connection it then publishes PEER_DEAD and wakes this side's ring waiters.
pub fn runWatcher(endpoint: *Endpoint) void {
    if (build_options.test_hooks) test_hooks.crashIfAt(.watching);
    const connection = endpoint.handles.get(.connected).?;
    const stopped = calls.readControl(endpoint, connection);
    if (stopped or endpoint.loadRequests().shutdown) return endpoint.handles.close(.connected);
    const end = calls.applyPeerEnd(endpoint);
    endpoint.handles.close(.connected);
    switch (end) {
        .published => {},
        .publish_after_close => {
            endpoint.hot.phase.store(.peer_dead, .seq_cst);
            endpoint.interrupt();
        },
    }
}

/// Reads until the peer's end, on a control connection where nothing more arrives (a stream connection's). True:
/// `close` stopped the read (error.Canceled comes only from it, platform.zig `endTask`).
pub fn readUntilEnd(endpoint: *Endpoint, connection: platform.Handle) bool {
    var buf: wire.Frame = undefined;
    return if (platform.read(endpoint.io, connection, &buf)) |_| false else |err| err == error.Canceled;
}

/// Reads one whole frame through `io` (the watchers' reads): false at the end of the stream (`platform.read`), also in
/// the middle of a frame. ReadFailed: the read failed otherwise; Canceled: the watcher's stop (platform.zig `endTask`).
pub fn readFrame(io: Io, connection: platform.Handle, frame: *wire.Frame) platform.ReadError!bool {
    var got: usize = 0;
    while (got < frame.len) {
        const n = try platform.read(io, connection, frame[got..]);
        if (n == 0) return false;
        got += n;
    }
    return true;
}
