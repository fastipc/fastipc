//! A connection's control connection (docs/protocol.md §3.3, §7): the client's side of the handshake, which `connect`
//! runs on the caller's thread (`handshake`), and the watcher every connection runs once its session is published
//! (`runWatcher`), a task on the endpoint's `Io` that reads the control connection until the peer's end. The frame and
//! deadline helpers here serve the listener's `accept` too (listener.zig).
//!
//! The handshake owns the handles it opens until it returns: on any failure it closes them, under the handle lock
//! (process_state.zig), so `connect` leaves nothing behind. The watcher then owns the connection, and is the only one
//! that closes it: `close` stops the watcher first (endpoint.zig `requestStop`, then `platform.endTask`).
//!
//! This file and endpoint.zig import each other: `endpoint.connect` runs the handshake and starts the watcher, which
//! work on the endpoint. The cycle is deliberate (endpoint.zig's top comment).

const std = @import("std");
const builtin = @import("builtin");
const Io = std.Io;
const build_options = @import("build_options");
const endpoint = @import("endpoint.zig");
const log = @import("../log.zig");
const platform = @import("../platform.zig");
const process_state = @import("../process_state.zig");
const segment = @import("../session/segment.zig");
const wire = @import("wire.zig");

const Endpoint = endpoint.Endpoint;
const Session = segment.Session;

// === Deadlines and back-off ===

/// What a wait may take before `deadline` (null: no deadline): null waits for ever, 0 only looks; rounded up to the
/// next millisecond.
pub fn waitMs(io: Io, deadline: ?Io.Clock.Timestamp) ?u32 {
    const at = deadline orelse return null;
    const left = at.durationFromNow(io).raw.nanoseconds;
    if (left <= 0) return 0;
    return @intCast(@min(@divFloor(left + std.time.ns_per_ms - 1, std.time.ns_per_ms), std.math.maxInt(i32)));
}

/// Whether `deadline` (null: none) has passed.
pub fn passed(io: Io, deadline: ?Io.Clock.Timestamp) bool {
    return (waitMs(io, deadline) orelse return false) == 0;
}

/// A handshake's back-off delay (docs/protocol.md §3.2, §3.3): 1 ms, doubled by each back-off up to 100 ms, and never
/// past the call's deadline.
pub const Backoff = struct {
    delay_ms: u32 = 1,

    /// The next delay, cut to what is left before the deadline (`wait`: `waitMs`'s); then doubles the delay.
    pub fn next(b: *Backoff, wait: ?u32) u32 {
        const delay = if (wait) |w| @min(w, b.delay_ms) else b.delay_ms;
        b.delay_ms = @min(2 * b.delay_ms, 100);
        return delay;
    }
};

// === The client's handshake ===

/// Sets a client's session up on the caller's thread (docs/protocol.md §3.3): connects to the listener, waits for its
/// SEGMENT, maps the offered segment and answers READY. While nobody listens, the listener is busy, or an attempt
/// ends early (the listener dropped it or left), it backs off and tries again, until `deadline` (null: for ever). The
/// session, attached and not published; Invalid (another user, another protocol version); NoMemory; Timeout. On
/// failure the endpoint holds no handle.
pub fn handshake(ep: *Endpoint, deadline: ?Io.Clock.Timestamp) endpoint.Error!*Session {
    var backoff: Backoff = .{};
    while (true) {
        if (attempt(ep, deadline)) |session| return session else |err| switch (err) {
            error.Retry => {},
            else => |e| return e,
        }
        if (passed(ep.io, deadline)) return error.Timeout;
        platform.sleep(backoff.next(waitMs(ep.io, deadline)));
    }
}

/// One attempt of the handshake. Retry: try again after a back-off. On any error the attempt's handles are closed.
fn attempt(ep: *Endpoint, deadline: ?Io.Clock.Timestamp) (endpoint.Error || error{Retry})!*Session {
    errdefer {
        ep.handles.close(.received);
        ep.handles.close(.connected);
    }
    try connectOnce(ep);
    if (build_options.test_hooks) test_hooks.crashIfAt(.connected);
    var offer = try awaitSegment(ep, deadline);
    if (build_options.test_hooks) test_hooks.crashIfAt(.segment_received);
    const session = try attach(ep, &offer);
    errdefer {
        session.unmap();
        ep.gpa.destroy(session);
    }
    if (build_options.test_hooks) test_hooks.crashIfAt(.attached);
    // From here the offered segment belongs to this connection, and to no other (docs/protocol.md §4.3)
    const ready = wire.encodeReady(platform.pid());
    if (!sendFrame(ep.io, ep.handles.get(.connected).?, &ready, null)) return error.Retry;
    if (build_options.test_hooks) test_hooks.crashIfAt(.ready_sent);
    return session;
}

/// Connecting (docs/protocol.md §3.3): the listener, which must run as this user.
fn connectOnce(ep: *Endpoint) (endpoint.Error || error{Retry})!void {
    const handle = handle: {
        process_state.lockHandles();
        defer process_state.unlockHandles();
        switch (platform.connect(&ep.address, &ep.security.?) catch return error.Retry) {
            .connected => |h| {
                ep.handles.register(.connected, h);
                break :handle h;
            },
            .no_listener, .busy => return error.Retry,
            .foreign => return foreign(),
        }
    };
    const peer = platform.peer(handle, .client) orelse return error.Retry;
    if (!peer.same_user) return foreign();
    if (peer.pid == platform.pid()) warnSameProcess();
}

fn foreign() error{Invalid} {
    log.warn(@src(), "a process of another user holds the connection's name; the connection fails", .{});
    return error.Invalid;
}

/// AwaitSegment (docs/protocol.md §3.3): SEGMENT, and the frame's checks (wire.zig), then the capacity the listener
/// chose and the segment size it implies. Where the segment's handle comes with the frame, the client waits until the
/// connection is readable, then takes the frame and its handle at once, under the handle lock. Retry: the listener
/// dropped the attempt or left (EOF), or sent a malformed frame.
fn awaitSegment(ep: *Endpoint, deadline: ?Io.Clock.Timestamp) (endpoint.Error || error{Retry})!wire.Offer {
    const connection = ep.handles.get(.connected).?;
    var frame: wire.Frame = undefined;
    if (platform.passes_segment_handle) {
        while (true) switch (platform.readableWithin(connection, waitMs(ep.io, deadline))) {
            .done => break,
            .idle => if (passed(ep.io, deadline)) return error.Timeout,
            .cancelled => unreachable, // nothing to cancel it
        };
        process_state.lockHandles();
        const received = platform.recvSegment(connection, &frame);
        if (received) |h| ep.handles.register(.received, h) else |_| {}
        process_state.unlockHandles();
        _ = received catch |err| {
            if (err != error.EndOfStream) log.warn(@src(), "protocol error: SEGMENT: {s}", .{@errorName(err)});
            return error.Retry;
        };
    } else {
        var got: usize = 0;
        while (got < frame.len) switch (platform.readWithin(connection, frame[got..], null, waitMs(ep.io, deadline))) {
            .done => |n| {
                if (n == 0) return error.Retry; // EOF, also in the middle of the frame (docs/protocol.md §3.1)
                got += n;
            },
            .idle => if (passed(ep.io, deadline)) return error.Timeout,
            .cancelled => unreachable,
        };
    }
    switch (wire.checkSegment(&frame)) {
        .valid => |offer| {
            if (!segment.validCapacity(offer.capacity) or offer.segment_size != segment.size(@intCast(offer.capacity))) {
                log.warn(@src(), "protocol error: a SEGMENT frame with a bad capacity or segment size", .{});
                return error.Retry;
            }
            return offer;
        },
        .mismatch => {
            log.warn(@src(), "the listener runs another protocol version; the connection fails", .{});
            return error.Invalid;
        },
        .rejected => {
            log.warn(@src(), "protocol error: a malformed SEGMENT frame", .{});
            return error.Retry;
        },
    }
}

/// Attach (docs/protocol.md §4.3): maps the offered segment and checks it (session/segment.zig). A received segment
/// handle is closed right after, whatever the result. Retry: the segment is malformed, or gone (the listener left after
/// sending SEGMENT).
fn attach(ep: *Endpoint, offer: *const wire.Offer) (endpoint.Error || error{Retry})!*Session {
    const session = ep.gpa.create(Session) catch return error.NoMemory;
    const attached = segment.attach(ep.handles.get(.received), offer);
    ep.handles.close(.received);
    session.* = attached catch |err| {
        ep.gpa.destroy(session);
        return switch (err) {
            error.Rejected, error.Gone => error.Retry,
            error.AccessDenied => {
                log.warn(@src(), "the peer runs as another user or at an incompatible elevation; the connection fails", .{});
                return error.Invalid;
            },
            error.OutOfMemory => error.NoMemory,
        };
    };
    return session;
}

// === The watcher ===

/// A connection's task once its session is published (docs/protocol.md §7): reads the control connection, where
/// nothing more arrives, so the read returns only at the peer's end (EOF, an error, or any frame) or when `close` stops
/// it. At the peer's end it closes the connection, publishes PEER_DEAD and wakes this side's ring waiters; on a stop it
/// only closes the connection.
pub fn runWatcher(ep: *Endpoint) void {
    if (build_options.test_hooks) test_hooks.crashIfAt(.watching);
    var buf: wire.Frame = undefined;
    // error.Canceled comes only from `close` (platform.zig `endTask`)
    const stopped = if (platform.read(ep.io, ep.handles.get(.connected).?, &buf)) |_| false else |_| true;
    ep.handles.close(.connected);
    if (stopped or ep.loadRequests().shutdown) return;
    ep.hot.status.store(endpoint.Status{ .phase = .peer_dead }, .seq_cst);
    ep.interrupt();
}

// === The control connection's frames (both sides) ===

/// Sends one frame, with the segment's handle `attached` where it travels with the frame: instant, a frame fits an
/// empty buffer. False: the peer is gone, or the send failed.
pub fn sendFrame(io: Io, connection: platform.Handle, frame: *const wire.Frame, attached: ?platform.Handle) bool {
    return platform.sendFrame(io, connection, frame, attached) catch false;
}

/// A pair within one process is legal, as long as client and server run on different threads: `connect` waits for
/// the listener's `accept`. It is also the symptom of a listener that is never closed: it holds its name, and a later
/// connection of its process to that name pairs with it. Logged once per process, so that a process that pairs with
/// itself on purpose isn't flooded.
pub fn warnSameProcess() void {
    if (warned_same_process.swap(true, .monotonic)) return;
    log.warn(@src(), "a connection paired with a listener of this process: fipc_connect waits for that listener's " ++
        "fipc_accept, so client and server must run on different threads (or a listener never closed keeps its name)", .{});
}

var warned_same_process = std.atomic.Value(bool).init(false);

/// Test hooks: compiled in only with `build_options.test_hooks` (the static library and the tests).
pub const test_hooks = struct {
    /// A step of the handshake on either side, or the watcher's start: the point right after it.
    pub const Step = enum {
        /// The listener accepted a client (its connection).
        accepted,
        /// The listener created the client's segment and its connection.
        segment_created,
        /// The listener sent SEGMENT.
        segment_sent,
        /// The listener read READY, before it hands the connection over.
        ready_read,
        /// The client connected to the listener.
        connected,
        /// The client received SEGMENT.
        segment_received,
        /// The client mapped the offered segment.
        attached,
        /// The client sent READY, before `connect` returns.
        ready_sent,
        /// A connection's watcher, before its first read (either side).
        watching,
    };

    /// When set, the process ends at once (exit code 99), as a crash would, when it reaches this step: nothing is
    /// cleaned up, and the kernel closes what the process held (the crash tests, and the chaos harness's crash-at-step
    /// scenarios). Set it before the process listens or connects, in Zig through this variable, or from any language
    /// through the environment (`armFromEnv`).
    pub var crash_at: ?Step = null;

    var env_checked = std.atomic.Value(bool).init(false);

    /// Arms `crash_at` from `FASTIPC_TEST_CRASH_AT` (a harness that drives the library through the C API can't reach
    /// `crash_at` directly). Read once per process, at the first step; an unset variable or an unknown step name
    /// leaves `crash_at` unchanged. test_hooks only, so the shipped library never reads the environment for this.
    pub fn armFromEnv() void {
        if (env_checked.swap(true, .monotonic)) return;
        const value = std.c.getenv("FASTIPC_TEST_CRASH_AT") orelse return;
        if (std.meta.stringToEnum(Step, std.mem.span(value))) |step| crash_at = step;
    }

    /// std's Windows `Io` maps a read of a pipe that its server disconnected (STATUS_PIPE_DISCONNECTED, what a client
    /// reads when a listener drops the connection) to an unexpected status, whose stack trace, in a Debug build that
    /// keeps tracing on, loads the debug info and keeps its files open. A test that counts handles calls this first, so
    /// that the process's first such trace isn't in its count. Nothing elsewhere.
    pub fn primeStackTraces(io: Io) void {
        if (builtin.os.tag != .windows) return; // only std's Windows `Io` traces that status
        var security = platform.Security.init() catch return;
        defer security.deinit();
        var name_buf: [64]u8 = undefined;
        const name = std.mem.print(&name_buf, "t-prime-{d}", .{platform.pid()}) catch return;
        const address = platform.addressOf(name) catch return;
        const listening = (platform.claim(&address, &security) catch return) orelse return;
        defer platform.close(listening);
        const client = switch (platform.connect(&address, &security) catch return) {
            .connected => |client| client,
            else => return,
        };
        defer platform.close(client);
        switch (platform.acceptWithin(listening, null, 5000, null)) {
            .done => |outcome| switch (outcome) {
                .connection => |accepted| _ = platform.dropClient(accepted, false),
                else => return,
            },
            else => return,
        }
        std.debug.print("test_hooks.primeStackTraces: a PIPE_DISCONNECTED trace follows, on purpose\n", .{});
        var byte: [1]u8 = undefined;
        _ = platform.read(io, client, &byte) catch {};
    }

    pub fn crashIfAt(step: Step) void {
        armFromEnv();
        if (crash_at != step) return;
        platform.crash(99);
    }
};
