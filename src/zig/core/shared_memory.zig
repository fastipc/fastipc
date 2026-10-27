//! A session's shared segment, whatever its kind's layout (stream/segment.zig, broadcast/segment.zig,
//! queue/segment.zig): the server creates it under a new random id (`createUnderRandomId`) and offers it, and the
//! client maps what was offered (`openOffered`; a broadcast's, a queue's or a service's, `attachDescribed`). A mapped
//! segment is a `Session`, which a connection's view holds and its `close` unmaps.
//!
//! Every function here runs without a lock: a segment is created and filled outside every lock, since committing
//! gigabytes takes seconds. Where the segment's handle travels with the offer, the caller creates and registers it
//! under the handle lock (process_state.zig) and gives it to the creation, and closes a received one after the attach.

const std = @import("std");
const Io = std.Io;
const log = @import("log.zig");
const platform = @import("../platform.zig");

/// One session's mapped segment, with its ring events. A handshake builds it, and it is published in a connection's
/// view; the connection's `close` unmaps it. Its `capacity` is a ring's: each of a stream connection's two, a channel's
/// data ring.
pub const Session = struct {
    id: [16]u8,
    capacity: u32,
    /// This side is the server, which writes the s2c ring (the client writes c2s); of a channel, its owner.
    server: bool,
    mapping: platform.Mapping,

    pub fn unmap(s: *Session) void {
        s.mapping.unmap();
    }
};

pub const CreateError = error{
    /// The segment can't get its memory: NO_MEMORY (docs/protocol.md §4.2).
    OutOfMemory,
    /// Descriptor, handle or kernel-pool exhaustion, or an unexpected failure (logged): retried after a back-off.
    SystemResources,
};

/// Creates a segment of `len` bytes under a new random id, with the user-only security, and returns the id and the
/// mapping. Its memory is committed now, so a shortage is NO_MEMORY here rather than a fault later. `handle`: the
/// segment's handle where it travels with the offer (created and registered by the caller), else null.
pub fn createUnderRandomId(
    io: Io,
    handle: ?platform.Handle,
    len: usize,
    security: *const platform.Security,
    kind: platform.SegmentKind,
) CreateError!struct { [16]u8, platform.Mapping } {
    // A random 128-bit id collides with a live one only if the system's randomness is broken: a few draws, then
    // the attempt is treated as exhaustion
    for (0..8) |_| {
        const id = randomId(io);
        const mapping = platform.createSegment(handle, &id, len, security, kind) catch |err| switch (err) {
            error.NameCollision => continue,
            error.OutOfMemory => return error.OutOfMemory,
            error.SystemResources, error.Unexpected => return error.SystemResources,
        };
        return .{ id, mapping };
    }
    log.warn(@src(), "eight random session ids collided", .{});
    return error.SystemResources;
}

fn randomId(io: Io) [16]u8 {
    var id: [16]u8 = undefined;
    while (true) {
        io.random(&id);
        if (!std.mem.allEqual(u8, &id, 0)) return id;
    }
}

pub const AttachError = error{
    /// Not the offered segment: not sealed, another size, or a header that doesn't match the frame (a protocol
    /// error, logged; retried after a back-off); also handle exhaustion.
    Rejected,
    /// The segment doesn't exist: the server left after sending its offer.
    Gone,
    /// The segment's security refused this process: another user, or an incompatible elevation.
    AccessDenied,
    /// No address space for the mapping: NO_MEMORY.
    OutOfMemory,
};

/// The offered segment of `kind`, mapped (`platform.openSegment`), with its failures as an attach's. `received`: the
/// segment's handle, where it travels with the offer (the caller closes it afterwards, whatever the result), else
/// null: the offer's session id `id` names the segment.
pub fn openOffered(received: ?platform.Handle, id: *const [16]u8, len: usize, kind: platform.SegmentKind) AttachError!platform.Mapping {
    return platform.openSegment(received, id, len, kind) catch |err| switch (err) {
        error.Gone => error.Gone,
        error.AccessDenied => error.AccessDenied,
        error.OutOfMemory => error.OutOfMemory,
        error.NotSealed => rejected("the offered segment isn't sealed"),
        error.WrongSize => rejected("the offered segment has another size"),
        error.SystemResources, error.Unexpected => error.Rejected,
    };
}

/// The attach of a broadcast's, a queue's or a service's segment (broadcast/segment.zig, queue/segment.zig `attach`),
/// whose start is a description of `kind`'s layout, `Description` (`wire_protocol.BroadcastInfo`,
/// `wire_protocol.QueueInfo`). `expected` is the description the offer implies: the kind's `describe` of the offer's
/// sizes, with the offer's session id (and flags). Checks the offered `segment_size` against it, maps the segment, and
/// compares the description at its start with `expected` byte for byte, the reserved bytes aside; a mismatch unmaps it
/// again.
pub fn attachDescribed(
    comptime Description: type,
    comptime kind: platform.SegmentKind,
    received: ?platform.Handle,
    expected: Description,
    segment_size: u64,
) AttachError!Session {
    if (segment_size != expected.segment_size) return rejected("the offered segment size isn't the " ++ @tagName(kind) ++ " layout's");
    const mapping = try openOffered(received, &expected.session_id, expected.segment_size, kind);
    var session: Session = .{ .id = expected.session_id, .capacity = expected.capacity, .server = false, .mapping = mapping };
    const description: *const Description = @ptrCast(@alignCast(mapping.base));
    var compared = expected;
    compared.reserved = description.reserved;
    if (!std.mem.eql(u8, std.mem.asBytes(&compared), std.mem.asBytes(description))) {
        session.unmap();
        return rejected("the offered " ++ @tagName(kind) ++ " segment's description doesn't match the frame");
    }
    return session;
}

/// A protocol error of the offered segment, logged: Rejected.
pub fn rejected(comptime why: []const u8) AttachError {
    log.warn(@src(), "protocol error: " ++ why, .{});
    return error.Rejected;
}
