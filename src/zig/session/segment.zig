//! A session's shared segment (docs/protocol.md §4): a 384-byte header (`abi.SegmentHeader`: the magic, the ring
//! indices, the capacity, the header's size and the session id), then the s2c ring and the c2s ring, `cap` bytes each,
//! `align_up(384 + 2·cap, 4096)` bytes in all. The server creates a segment for each connection it accepts and writes
//! the header once, before it offers the segment; the client checks it against the offer before any use and never
//! writes it. Neither side reads the capacity again afterwards: the connection's view holds it.
//!
//! Every function here runs without a lock: a segment is created and filled outside every lock, since committing
//! gigabytes takes seconds. Where the segment's handle travels with SEGMENT, the caller creates and registers
//! it under the handle lock (process_state.zig) and gives it to `create`, and closes a received one after `attach`.

const std = @import("std");
const Io = std.Io;
const abi = @import("../abi.zig");
const log = @import("../log.zig");
const platform = @import("../platform.zig");
const wire = @import("../lifecycle/wire.zig");

/// The segment's magic ("FIPCSEG1"), in `Header.magic`.
pub const magic = abi.segment_magic;
pub const header_size = 384;

/// The segment's first 384 bytes.
pub const Header = abi.SegmentHeader;

comptime {
    std.debug.assert(@sizeOf(Header) == header_size);
}

/// A ring's capacity: a power of two from 1 KiB to 2 GiB, the same in both directions.
pub fn validCapacity(capacity: u64) bool {
    return capacity >= abi.min_capacity and capacity <= 1 << 31 and std.math.isPowerOfTwo(capacity);
}

/// The segment's size for rings of `cap` bytes.
pub fn size(cap: u32) usize {
    return std.mem.alignForward(usize, header_size + 2 * @as(usize, cap), 4096);
}

/// One session's mapped segment, with its ring events. A handshake builds it, and it is published in a connection's
/// view; the connection's `close` unmaps it.
pub const Session = struct {
    id: [16]u8,
    cap: u32,
    /// This side is the server, which writes the s2c ring (the client writes c2s).
    server: bool,
    mapping: platform.Mapping,

    pub fn header(s: *const Session) *Header {
        return @ptrCast(@alignCast(s.mapping.base));
    }

    /// The rings: s2c, then c2s.
    pub fn data(s: *const Session) [*]u8 {
        return @as([*]u8, @ptrCast(s.mapping.base)) + header_size;
    }

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

/// A new session's segment under a new random id, with its memory committed now (NO_MEMORY rather than a fault
/// later) and the user-only security, and its header written. `handle`: the segment's handle, where it travels with
/// SEGMENT (created and registered by the caller), else null.
pub fn create(io: Io, handle: ?platform.Handle, security: *const platform.Security, cap: u32) CreateError!Session {
    const len = size(cap);
    // A random 128-bit id collides with a live one only if the system's randomness is broken: a few draws, then
    // the attempt is treated as exhaustion
    for (0..8) |_| {
        const id = randomId(io);
        const mapping = platform.createSegment(handle, &id, len, security) catch |err| switch (err) {
            error.NameCollision => continue,
            error.OutOfMemory => return error.OutOfMemory,
            error.SystemResources, error.Unexpected => return error.SystemResources,
        };
        const session: Session = .{ .id = id, .cap = cap, .server = true, .mapping = mapping };
        writeHeader(session.header(), cap, id);
        return session;
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

/// The header of a new segment, whose memory the kernel zeroed.
fn writeHeader(h: *Header, cap: u32, id: [16]u8) void {
    h.magic = magic;
    h.capacity = cap;
    h.header_size = header_size;
    h.session_id = id;
}

pub const AttachError = error{
    /// Not the offered segment: not sealed, another size, or a header that doesn't match the frame (a protocol
    /// error, logged; retried after a back-off); also handle exhaustion.
    Rejected,
    /// The segment doesn't exist: the server left after sending SEGMENT.
    Gone,
    /// The segment's security refused this process: another user, or an incompatible elevation.
    AccessDenied,
    /// No address space for the mapping: NO_MEMORY.
    OutOfMemory,
};

/// The client's attach (docs/protocol.md §4.3): maps the offered segment, checks it, and returns it as this side's
/// session. The offer's capacity is the server's, which `wire.checkSegment` has checked. `received`: the segment's
/// handle, where it travels with SEGMENT (the caller closes it afterwards, whatever the result), else null: the offer's
/// session id names the segment.
pub fn attach(received: ?platform.Handle, offer: *const wire.Offer) AttachError!Session {
    const cap: u32 = @intCast(offer.capacity);
    const len = size(cap);
    const mapping = platform.openSegment(received, &offer.session_id, len) catch |err| return switch (err) {
        error.Gone => error.Gone,
        error.AccessDenied => error.AccessDenied,
        error.OutOfMemory => error.OutOfMemory,
        error.NotSealed => rejected("the offered segment isn't sealed"),
        error.WrongSize => rejected("the offered segment has another size"),
        error.SystemResources, error.Unexpected => error.Rejected,
    };
    var session: Session = .{ .id = offer.session_id, .cap = cap, .server = false, .mapping = mapping };
    if (!matches(session.header(), cap, offer.session_id)) {
        session.unmap();
        return rejected("the offered segment's header doesn't match the frame");
    }
    return session;
}

/// The header's check at attach (docs/protocol.md §4.3): the magic, the capacity, the header size and the session id
/// the frame named.
fn matches(h: *const Header, cap: u32, id: [16]u8) bool {
    return h.magic == magic and h.capacity == cap and h.header_size == header_size and std.mem.eql(u8, &h.session_id, &id);
}

fn rejected(comptime why: []const u8) AttachError {
    log.warn(@src(), "protocol error: " ++ why, .{});
    return error.Rejected;
}

// === Tests (real OS objects, in this process) ===

const testing = std.testing;

test "the segment's size: the header and both rings, rounded up to a page" {
    try testing.expectEqual(@as(usize, 4096), size(1024));
    try testing.expectEqual(@as(usize, 12288), size(4096));
    try testing.expectEqual(@as(usize, 384 + 2 * (1 << 20) + 3712), size(1 << 20));
    try testing.expect(validCapacity(1024) and validCapacity(1 << 31));
    try testing.expect(!validCapacity(512) and !validCapacity(3000) and !validCapacity(1 << 32));
}

/// A server's segment and the client's attach of it, in one process.
const Pair = struct {
    created: Session,
    handle: ?platform.Handle,
    security: platform.Security,

    fn init(cap: u32) !Pair {
        var security = try platform.Security.init();
        errdefer security.deinit();
        const handle: ?platform.Handle = if (platform.passes_segment_handle) try platform.createSegmentHandle() else null;
        errdefer if (handle) |h| platform.close(h);
        return .{ .created = try segment.create(testing.io, handle, &security, cap), .handle = handle, .security = security };
    }

    fn offer(p: *const Pair) wire.Offer {
        return .{ .pid = 0, .capacity = p.created.cap, .segment_size = size(p.created.cap), .session_id = p.created.id };
    }

    fn attachTo(p: *const Pair, o: *const wire.Offer) AttachError!Session {
        return segment.attach(p.handle, o);
    }

    fn deinit(p: *Pair) void {
        p.created.unmap();
        p.security.deinit();
        if (p.handle) |h| platform.close(h);
    }
};
const segment = @This();

test "a new segment has its header, and the client's attach sees the same memory" {
    var pair = try Pair.init(4096);
    defer pair.deinit();
    const h = pair.created.header();
    try testing.expectEqual(magic, h.magic);
    try testing.expectEqual(@as(u32, 4096), h.capacity);
    try testing.expectEqual(@as(u32, header_size), h.header_size);
    try testing.expect(!std.mem.allEqual(u8, &h.session_id, 0));
    // The rings start zeroed, indices and data
    try testing.expectEqual(@as(u64, 0), h.s2c.writer.head.load(.monotonic));
    try testing.expect(std.mem.allEqual(u8, pair.created.data()[0 .. 2 * 4096], 0));

    const o = pair.offer();
    var attached = try pair.attachTo(&o);
    defer attached.unmap();
    try testing.expect(!attached.server and attached.cap == 4096);
    pair.created.data()[4096 + 7] = 0x5a; // the c2s ring
    try testing.expectEqual(@as(u8, 0x5a), attached.data()[4096 + 7]);
}

test "the attach refuses a header that doesn't match the offer" {
    var pair = try Pair.init(4096);
    defer pair.deinit();
    const h = pair.created.header();
    var o = pair.offer();
    o.session_id[5] +%= 1;
    // A passed segment is the one received, whose header then doesn't match; a named one isn't found under another id
    try testing.expectError(if (platform.passes_segment_handle) error.Rejected else error.Gone, pair.attachTo(&o));

    o = pair.offer();
    const fields = [_]*u32{ &h.capacity, &h.header_size };
    for (fields) |field| {
        const saved = field.*;
        field.* = saved + 1;
        try testing.expectError(error.Rejected, pair.attachTo(&o));
        field.* = saved;
    }
    h.magic = std.mem.readInt(u64, "FIPCSEG2", .little); // another version's
    try testing.expectError(error.Rejected, pair.attachTo(&o));
    h.magic = magic;
    var again = try pair.attachTo(&o);
    again.unmap();
}
