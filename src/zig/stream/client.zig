//! A stream connection's client (docs/protocol.md §3.3): `connect`, which sets its session up on the caller's thread
//! (`handshake`): it connects to the listener, waits for its SEGMENT, maps the offered segment and answers READY. The
//! steps every kind's client takes are core/control.zig's; the listener's side is server.zig.

const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;
const build_options = @import("build_options");
const control = @import("../core/control.zig");
const endpoint_module = @import("../core/endpoint.zig");
const frames = @import("frames.zig");
const log = @import("../core/log.zig");
const platform = @import("../platform.zig");
const segment = @import("segment.zig");
const test_hooks = @import("../core/test_hooks.zig");
const wire = @import("../core/wire.zig");

const Endpoint = endpoint_module.Endpoint;
const Error = endpoint_module.Error;
const Session = @import("../core/shared_memory.zig").Session;

/// `fipc_stream_connect`: validates the name, creates the endpoint and sets the session up on the caller's thread,
/// within `timeout`, then starts the watcher. The handshake (`handshake`) connects again and again while nobody
/// listens, and waits for the listener's `accept` to offer the segment.
///   - On success it returns the connection with its session published. The peer may already have ended it, which the
///     data calls report.
///   - On failure it returns Invalid (another protocol version, or another user), NoMemory or Timeout, and leaves
///     nothing behind.
pub fn connect(gpa: Allocator, name: []const u8, timeout: Io.Timeout) Error!*Endpoint {
    const endpoint = try endpoint_module.newClient(gpa, name);
    errdefer endpoint_module.free(endpoint);
    const session = try handshake(endpoint, timeout.toTimestamp(endpoint.io));
    try endpoint_module.start(endpoint, session, segment.header(session), segment.data(session));
    return endpoint;
}

/// Sets a client's session up on the caller's thread (docs/protocol.md §3.3): connects to the listener, waits for its
/// SEGMENT, maps the offered segment and answers READY. While nobody listens, the listener is busy, or an attempt
/// ends early (the listener dropped it or left), it backs off and tries again, until `deadline` (null: for ever). The
/// session, attached and not published; Invalid (another user, another protocol version); NoMemory; Timeout. On
/// failure the endpoint holds no handle.
pub fn handshake(endpoint: *Endpoint, deadline: ?Io.Clock.Timestamp) Error!*Session {
    return control.retrying(*Session, endpoint, deadline, attempt, .{});
}

/// One attempt of the handshake. Retry: try again after a back-off. On any error the attempt's handles are closed.
fn attempt(endpoint: *Endpoint, deadline: ?Io.Clock.Timestamp) (Error || error{Retry})!*Session {
    errdefer {
        endpoint.handles.close(.received);
        endpoint.handles.close(.connected);
    }
    try control.connectOnce(endpoint, .stream);
    if (build_options.test_hooks) test_hooks.crashIfAt(.connected);
    var offer = try awaitSegment(endpoint, deadline);
    if (build_options.test_hooks) test_hooks.crashIfAt(.segment_received);
    const session = try control.attach(endpoint, segment.attach, &offer);
    errdefer {
        session.unmap();
        endpoint.gpa.destroy(session);
    }
    if (build_options.test_hooks) test_hooks.crashIfAt(.attached);
    // From here the offered segment belongs to this connection, and to no other (docs/protocol.md §4.3)
    const ready = frames.encodeReady(platform.pid());
    if (!platform.sendFrame(endpoint.handles.get(.connected).?, &ready, null)) return error.Retry;
    if (build_options.test_hooks) test_hooks.crashIfAt(.ready_sent);
    return session;
}

/// AwaitSegment (docs/protocol.md §3.3): SEGMENT, and the frame's checks (frames.zig), then the capacity the listener
/// chose and the segment size it implies. Where the segment's handle comes with the frame, the client waits until the
/// connection is readable, then takes the frame and its handle at once, under the handle lock. Retry: the listener
/// dropped the attempt or left (EOF), or sent a malformed frame.
fn awaitSegment(endpoint: *Endpoint, deadline: ?Io.Clock.Timestamp) (Error || error{Retry})!frames.Offer {
    var frame: wire.Frame = undefined;
    try control.readOffered(endpoint, deadline, &frame, "SEGMENT");
    switch (frames.checkSegment(&frame)) {
        .valid => |offer| {
            if (!segment.validCapacity(offer.capacity) or offer.segment_size != segment.size(offer.capacity)) {
                log.warn(@src(), "protocol error: a SEGMENT frame with a bad capacity or segment size", .{});
                return error.Retry;
            }
            return offer;
        },
        .other_version => {
            log.warn(@src(), "the listener runs another protocol version; the connection fails", .{});
            return error.Invalid;
        },
        .other_kind => {
            log.warn(@src(), "the name is a broadcast's, a queue's or a service's: use fipc_broadcast_join, fipc_queue_join or fipc_service_join; the connection fails", .{});
            return error.Invalid;
        },
        .rejected => {
            log.warn(@src(), "protocol error: a malformed SEGMENT frame", .{});
            return error.Retry;
        },
    }
}
