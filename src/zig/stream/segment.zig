//! A stream connection's shared segment (docs/protocol.md §4): a 640-byte header (`wire_protocol.SegmentHeader`: the magic, the
//! capacity, the header's size and the session id, then the ring indices), then the s2c ring and the c2s ring, `capacity`
//! bytes each, `align_up(640 + 2·capacity, 4096)` bytes in all. The server creates a segment for each connection it
//! accepts and writes the header once, before it offers the segment; the client checks it against the offer before any
//! use and never writes it. Neither side reads the capacity again afterwards: the connection's view holds it.
//!
//! As every segment (core/shared_memory.zig), it is created and attached without a lock.

const std = @import("std");
const Io = std.Io;
const wire_protocol = @import("../wire_protocol.zig");
const frames = @import("frames.zig");
const platform = @import("../platform.zig");
const shared_memory = @import("../core/shared_memory.zig");

const Session = shared_memory.Session;

/// The segment's magic ("FIPCSEG1"), in `Header.magic`.
pub const magic = wire_protocol.segment_magic;
/// The segment's first 640 bytes (wire_protocol.zig checks the layout).
pub const Header = wire_protocol.SegmentHeader;
pub const header_size = @sizeOf(Header);

/// A ring's capacity: a power of two from 1 KiB to 2 GiB, the same in both directions.
pub fn validCapacity(capacity: u64) bool {
    return capacity >= wire_protocol.min_capacity and capacity <= 1 << 31 and std.math.isPowerOfTwo(capacity);
}

/// The segment's size for rings of `capacity` bytes.
pub fn size(capacity: u32) usize {
    return std.mem.alignForward(usize, header_size + 2 * @as(usize, capacity), 4096);
}

pub fn header(session: *const Session) *Header {
    return @ptrCast(@alignCast(session.mapping.base));
}

/// A session's rings: s2c, then c2s.
pub fn data(session: *const Session) [*]u8 {
    return @as([*]u8, @ptrCast(session.mapping.base)) + header_size;
}

/// A new session's segment under a new random id (core/shared_memory.zig `createUnderRandomId`), and its header
/// written. `handle`: the segment's handle, where it travels with SEGMENT (created and registered by the caller), else
/// null.
pub fn create(io: Io, handle: ?platform.Handle, security: *const platform.Security, capacity: u32) shared_memory.CreateError!Session {
    const id, const mapping = try shared_memory.createUnderRandomId(io, handle, size(capacity), security, .stream);
    const session: Session = .{ .id = id, .capacity = capacity, .server = true, .mapping = mapping };
    writeHeader(header(&session), capacity, id);
    return session;
}

/// The header of a new segment, whose memory the kernel zeroed.
fn writeHeader(h: *Header, capacity: u32, id: [16]u8) void {
    h.magic = magic;
    h.capacity = capacity;
    h.header_size = header_size;
    h.session_id = id;
}

/// The client's attach (docs/protocol.md §4.3): maps the offered segment, checks it, and returns it as this side's
/// session. The offer's capacity is the server's, which the client has checked (stream/client.zig `awaitSegment`).
/// `received`: the segment's handle, where it travels with SEGMENT (the caller closes it afterwards, whatever the
/// result), else null: the offer's session id names the segment.
pub fn attach(received: ?platform.Handle, offer: *const frames.Offer) shared_memory.AttachError!Session {
    const mapping = try shared_memory.openOffered(received, &offer.session_id, size(offer.capacity), .stream);
    var session: Session = .{ .id = offer.session_id, .capacity = offer.capacity, .server = false, .mapping = mapping };
    if (!matches(header(&session), offer.capacity, offer.session_id)) {
        session.unmap();
        return shared_memory.rejected("the offered segment's header doesn't match the frame");
    }
    return session;
}

/// The header's check at attach (docs/protocol.md §4.3): the magic, the capacity, the header size and the session id
/// the frame named.
fn matches(h: *const Header, capacity: u32, id: [16]u8) bool {
    return h.magic == magic and h.capacity == capacity and h.header_size == header_size and std.mem.eql(u8, &h.session_id, &id);
}

// === Tests (real OS objects, in this process) ===

const testing = std.testing;

test "the segment's size: the header and both rings, rounded up to a page" {
    try testing.expectEqual(@as(usize, 4096), size(1024));
    try testing.expectEqual(@as(usize, 12288), size(4096));
    try testing.expectEqual(@as(usize, 640 + 2 * (1 << 20) + 3456), size(1 << 20));
    try testing.expect(validCapacity(1024) and validCapacity(1 << 31));
    try testing.expect(!validCapacity(512) and !validCapacity(3000) and !validCapacity(1 << 32));
}

/// A server's segment and the client's attach of it, in one process.
const Pair = struct {
    created: Session,
    handle: ?platform.Handle,
    security: platform.Security,

    fn init(capacity: u32) !Pair {
        var security = try platform.Security.init();
        errdefer security.deinit();
        const handle: ?platform.Handle = if (platform.passes_segment_handle) try platform.createSegmentHandle() else null;
        errdefer if (handle) |h| platform.close(h);
        return .{ .created = try create(testing.io, handle, &security, capacity), .handle = handle, .security = security };
    }

    fn offer(p: *const Pair) frames.Offer {
        return .{ .capacity = p.created.capacity, .segment_size = size(p.created.capacity), .session_id = p.created.id };
    }

    fn attachTo(p: *const Pair, o: *const frames.Offer) shared_memory.AttachError!Session {
        return attach(p.handle, o);
    }

    fn deinit(p: *Pair) void {
        p.created.unmap();
        p.security.deinit();
        if (p.handle) |h| platform.close(h);
    }
};

test "a new segment has its header, and the client's attach sees the same memory" {
    var pair = try Pair.init(4096);
    defer pair.deinit();
    const h = header(&pair.created);
    try testing.expectEqual(magic, h.magic);
    try testing.expectEqual(@as(u32, 4096), h.capacity);
    try testing.expectEqual(@as(u32, header_size), h.header_size);
    try testing.expect(!std.mem.allEqual(u8, &h.session_id, 0));
    // The rings start zeroed, indices and data
    try testing.expectEqual(@as(u64, 0), h.s2c.writer.head.load(.monotonic));
    try testing.expect(std.mem.allEqual(u8, data(&pair.created)[0 .. 2 * 4096], 0));

    const o = pair.offer();
    var attached = try pair.attachTo(&o);
    defer attached.unmap();
    try testing.expect(!attached.server and attached.capacity == 4096);
    data(&pair.created)[4096 + 7] = 0x5a; // the c2s ring
    try testing.expectEqual(@as(u8, 0x5a), data(&attached)[4096 + 7]);
}

test "the attach refuses a header that doesn't match the offer" {
    var pair = try Pair.init(4096);
    defer pair.deinit();
    const h = header(&pair.created);
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
