//! A stream's server (docs/protocol.md §3.2): `create` claims the name (listener.zig `claimName`), and `accept` sets
//! each client up on the caller's thread: it waits for a client and accepts it, creates the client's segment and the
//! connection that will own it, sends SEGMENT, waits for READY, then gives the accepted handle and the session to the
//! new connection, starts its watcher (core/control.zig) and returns it. The client's side is client.zig.
//!
//! A setup has two waits: for a client (`acceptClient`), and for its answer (`awaitReady`). A call's timeout ends it
//! only inside a wait; everything between two waits runs to completion, and the progress before the second is kept in
//! the listener (`Listener.pending`), so the next call resumes there. A client whose setup a call began is never
//! dropped because that call timed out, and a server that polls (timeout 0) lets a client in within two calls. Kernel
//! objects hold everything else between calls: a client that connects while no call runs waits in the backlog (or on
//! the next pipe instance), and its answer in the connection's buffer.
//!
//! One client at a time: while the connection `accept` returned last is open, `accept` is Invalid at once, and a
//! client that connects meanwhile waits, within its own timeout.

const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;
const build_options = @import("build_options");
const control = @import("../core/control.zig");
const endpoint = @import("../core/endpoint.zig");
const frames = @import("frames.zig");
const stream_listener = @import("listener.zig");
const log = @import("../core/log.zig");
const names = @import("../core/names.zig");
const platform = @import("../platform.zig");
const process_state = @import("../core/process_state.zig");
const segment = @import("segment.zig");
const shared_memory = @import("../core/shared_memory.zig");
const test_hooks = @import("../core/test_hooks.zig");
const wait = @import("../core/wait.zig");

const Endpoint = endpoint.Endpoint;
const Error = endpoint.Error;
const Listener = stream_listener.Listener;
const Pending = stream_listener.Pending;
const Session = shared_memory.Session;

/// `fipc_stream_create`: validates the name and capacity, claims the name (AddrInUse if another stream, broadcast,
/// queue or service holds it) and creates the cancel signal; doesn't wait.
pub fn create(gpa: Allocator, name: []const u8, capacity: usize) endpoint.CreateError!*Listener {
    if (!names.isValid(name) or !segment.validCapacity(capacity)) return error.Invalid;
    return Listener.claimName(gpa, name, @intCast(capacity));
}

/// `fipc_stream_accept`: waits up to `timeout` for a client and sets it up on the caller's thread (this file's top
/// comment), resuming a setup an earlier call began. Invalid while the connection it returned last is open (one client
/// at a time); NoMemory once the listener failed; Cancelled after `cancel` (a setup in progress is dropped); Timeout,
/// with the progress kept.
///
/// Each round checks for cancel, then runs the next step of the setup, which may wait for the time left before the
/// deadline. The call ends when a step finishes it, or when a step finds nothing after the deadline has passed.
pub fn accept(listener: *Listener, timeout: Io.Timeout) Error!*Endpoint {
    if (listener.handles.forked) {
        process_state.reportInherited("listener");
        return error.Invalid;
    }
    if (listener.client_open.load(.acquire)) return error.Invalid;
    if (listener.failed) return error.NoMemory;
    const deadline = timeout.toTimestamp(listener.io);
    while (true) {
        if (listener.cancelled.load(.acquire)) {
            listener.dropPending();
            return error.Cancelled;
        }
        const wait_ms = wait.msLeft(listener.io, deadline);
        const step = if (listener.pending) |*pending| awaitReady(listener, pending, wait_ms) else acceptClient(listener, wait_ms);
        switch (step) {
            .progress => {},
            .idle => if (wait.passed(listener.io, deadline)) return error.Timeout,
            .back_off => _ = platform.waitCancelSignal(listener.handles.get(.cancel).?, listener.backoff.next(wait.msLeft(listener.io, deadline))),
            .done => |result| return result,
        }
    }
}

/// What a step of `accept` came to.
const Step = union(enum) {
    /// Something changed: look again (the cancel, the next wait).
    progress,
    /// The wait ended with nothing: Timeout if the deadline has passed, else wait again.
    idle,
    /// An accept failed, or handles ran out: wait out the back-off, then look again.
    back_off,
    /// `accept` returns this.
    done: Error!*Endpoint,
};

/// The first wait, for a client (docs/protocol.md §3.2), then the steps up to SEGMENT: the accept (core/control.zig
/// `acceptClient`: the fork check, the peer's user), the client's segment and connection, SEGMENT.
fn acceptClient(listener: *Listener, wait_ms: ?u32) Step {
    const handle = switch (control.acceptClient(&listener.handles, &listener.handles, listener.handles.get(.cancel).?, wait_ms)) {
        .connection => |h| h,
        .idle => return .idle,
        .retry_now => return .progress,
        .back_off => return .back_off,
    };
    listener.pending = newSetup(listener) catch |err| {
        listener.dropClient();
        switch (err) {
            error.OutOfMemory => {
                // No memory for a client's segment: the listener has failed for good
                listener.failed = true;
                return .{ .done = error.NoMemory };
            },
            error.SystemResources => return .back_off,
        }
    };
    if (build_options.test_hooks) test_hooks.crashIfAt(.segment_created);
    const session = listener.pending.?.session;
    const frame = frames.encodeSegment(platform.pid(), .{
        .capacity = session.capacity,
        .segment_size = segment.size(session.capacity),
        .session_id = session.id,
    });
    if (!platform.sendFrame(handle, &frame, listener.handles.get(.segment))) {
        listener.dropPending();
        return .progress;
    }
    if (build_options.test_hooks) test_hooks.crashIfAt(.segment_sent);
    return .progress;
}

/// The second wait, for READY (docs/protocol.md §3.2), into the bytes an earlier call may have read; then the
/// hand-over. EOF, a read error or anything but READY drops the client.
fn awaitReady(listener: *Listener, pending: *Pending, wait_ms: ?u32) Step {
    const connection = listener.handles.get(.connected).?;
    switch (platform.readWithin(connection, pending.frame[pending.got..], listener.handles.get(.cancel).?, wait_ms)) {
        .idle => return .idle,
        .cancelled => return .progress,
        .done => |n| {
            if (n == 0) {
                listener.dropPending();
                return .progress;
            }
            pending.got += n;
        },
    }
    if (pending.got < pending.frame.len) return .progress;
    if (!frames.isReady(&pending.frame)) {
        listener.dropPending();
        return .progress;
    }
    if (build_options.test_hooks) test_hooks.crashIfAt(.ready_read);
    return handOver(listener);
}

/// The client's connection, its segment, created and filled with no lock held (a segment handle that travels with
/// SEGMENT is created and registered under the handle lock first), and the next listening handle where the accept
/// took this one (docs/protocol.md §4.2). On failure the caller drops the client (`Listener.dropClient`).
fn newSetup(listener: *Listener) shared_memory.CreateError!Pending {
    const conn = endpoint.create(listener.gpa) catch return error.OutOfMemory;
    errdefer endpoint.free(conn);
    const session = try listener.gpa.create(Session);
    errdefer listener.gpa.destroy(session);
    session.* = try newSegment(listener);
    errdefer session.unmap();
    try control.nextListening(&listener.handles, .spare, &listener.address, &listener.security);
    return .{ .conn = conn, .session = session };
}

fn newSegment(listener: *Listener) shared_memory.CreateError!Session {
    if (platform.passes_segment_handle) {
        process_state.lockHandles();
        defer process_state.unlockHandles();
        listener.handles.register(.segment, platform.createSegmentHandle() catch return error.SystemResources);
    }
    return segment.create(listener.io, listener.handles.get(.segment), &listener.security, listener.capacity) catch |err| {
        listener.handles.close(.segment);
        return err;
    };
}

/// The handshake is complete: the connection gets the accepted handle (moved between the nodes under the handle
/// lock) and the session, and its watcher starts; the segment's handle is closed (the client holds its own mapping),
/// and a next listening handle becomes the one to accept on.
fn handOver(listener: *Listener) Step {
    const pending = listener.pending.?;
    listener.pending = null;
    listener.backoff = .{};
    listener.handles.close(.segment);
    {
        process_state.lockHandles();
        defer process_state.unlockHandles();
        pending.conn.handles.register(.connected, listener.handles.unregister(.connected).?);
        if (listener.handles.unregister(.spare)) |next| listener.handles.register(.listening, next);
    }
    const session = pending.session;
    endpoint.startAccepted(pending.conn, session, segment.header(session), segment.data(session)) catch {
        log.warn(@src(), "no task for an accepted connection; the client is dropped", .{});
        return .progress;
    };
    pending.conn.listener = listener;
    _ = listener.refs.fetchAdd(1, .monotonic);
    listener.client_open.store(true, .release);
    return .{ .done = pending.conn };
}
