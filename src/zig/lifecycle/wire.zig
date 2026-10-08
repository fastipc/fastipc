//! The two frames of the control connection (docs/protocol.md §3.1): SEGMENT, which the server sends once it has
//! accepted a client and created its segment, and READY, the client's answer. Each is 64 bytes, little-endian, on a
//! byte stream. Reserved fields are written as 0 and ignored on read. The magic never changes, so that each side tells
//! a peer of another protocol version (another `version`) from a corrupt one. On Linux the SEGMENT frame carries the
//! segment's memfd as its one `SCM_RIGHTS` descriptor (platform/linux.zig). The layout is `abi.ControlFrame`, whose
//! offsets abi.zig checks at compile time:
//!
//! | Offset | Size | Field                              | SEGMENT | READY |
//! |--------|------|------------------------------------|---------|-------|
//! | 0      | 4    | `magic` = "FIPC"                   | yes     | yes   |
//! | 4      | 1    | `type`: 1 SEGMENT, 2 READY         | 1       | 2     |
//! | 5      | 1    | `version` = 1                      | yes     | yes   |
//! | 8      | 4    | `pid` of the sender (logs only)    | yes     | yes   |
//! | 16     | 8    | `capacity`                         | yes     |       |
//! | 24     | 8    | `segment_size`                     | yes     |       |
//! | 32     | 16   | `session_id`                       | yes     |       |

const std = @import("std");
const abi = @import("../abi.zig");

pub const frame_len = 64;
pub const Frame = [frame_len]u8;

pub const magic: u32 = std.mem.readInt(u32, "FIPC", .little);
pub const version: u8 = abi.protocol_version;

const Type = enum(u8) { segment = 1, ready = 2 };

comptime {
    std.debug.assert(@sizeOf(abi.ControlFrame) == frame_len);
}

/// What a SEGMENT frame offers: the client's segment.
pub const Offer = struct {
    pid: u32,
    capacity: u64,
    segment_size: u64,
    session_id: [16]u8,
};

pub fn encodeSegment(offer: Offer) Frame {
    return std.mem.toBytes(abi.ControlFrame{
        .magic = magic,
        .frame_type = @backingInt(Type.segment),
        .version = version,
        .pid = offer.pid,
        .capacity = offer.capacity,
        .segment_size = offer.segment_size,
        .session_id = offer.session_id,
    });
}

pub fn encodeReady(pid: u32) Frame {
    return std.mem.toBytes(abi.ControlFrame{ .magic = magic, .frame_type = @backingInt(Type.ready), .version = version, .pid = pid });
}

/// The frame's fields, from bytes received in any alignment.
fn fields(frame: *const Frame) abi.ControlFrame {
    return std.mem.bytesToValue(abi.ControlFrame, frame);
}

/// The client's checks of a SEGMENT frame (docs/protocol.md §3.3), in this order: magic and type (else `rejected`:
/// not a SEGMENT of this protocol), the version (else `mismatch`: another protocol version, a permanent INVALID, never
/// retried). The client takes the capacity the server chose from the frame, and checks it and the segment size it
/// implies itself (control.zig; a corrupt peer is `rejected`, retried after a back-off); the attach checks the segment
/// (session/segment.zig).
pub const Check = union(enum) {
    valid: Offer,
    mismatch,
    rejected,
};

pub fn checkSegment(frame: *const Frame) Check {
    const f = fields(frame);
    if (f.magic != magic or f.frame_type != @backingInt(Type.segment)) return .rejected;
    if (f.version != version) return .mismatch;
    return .{ .valid = .{ .pid = f.pid, .capacity = f.capacity, .segment_size = f.segment_size, .session_id = f.session_id } };
}

/// Whether `frame` is a READY of this protocol. Anything else in its place (another type, another version, garbage)
/// ends the handshake like the end of the stream.
pub fn isReady(frame: *const Frame) bool {
    const f = fields(frame);
    return f.magic == magic and f.frame_type == @backingInt(Type.ready) and f.version == version;
}

// === Tests ===

const testing = std.testing;

const example: Offer = .{
    .pid = 4242,
    .capacity = 4096,
    .segment_size = 12288,
    .session_id = .{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 },
};

test "a SEGMENT frame round-trips, with the layout of the protocol" {
    const frame = encodeSegment(example);
    try testing.expectEqualSlices(u8, "FIPC", frame[0..4]);
    try testing.expectEqual(@as(u8, 1), frame[4]);
    try testing.expectEqual(@as(u8, 1), frame[5]);
    try testing.expectEqualSlices(u8, &.{ 0, 0 }, frame[6..8]);
    try testing.expectEqualSlices(u8, &@as([4]u8, @splat(0)), frame[12..16]);
    try testing.expectEqualSlices(u8, &@as([16]u8, @splat(0)), frame[48..64]);
    try testing.expectEqualDeep(Check{ .valid = example }, checkSegment(&frame));
}

test "a SEGMENT frame is refused for its magic or type, and is a mismatch for its version" {
    var frame = encodeSegment(example);
    frame[0] = 'X';
    try testing.expectEqual(Check.rejected, checkSegment(&frame));

    frame = encodeSegment(example);
    frame[4] = 2;
    try testing.expectEqual(Check.rejected, checkSegment(&frame));

    // Another version's SEGMENT
    frame = encodeSegment(example);
    frame[5] = 2;
    try testing.expectEqual(Check.mismatch, checkSegment(&frame));

    // Reserved bytes are ignored
    frame = encodeSegment(example);
    frame[6] = 0xff;
    frame[63] = 0xff;
    try testing.expect(checkSegment(&frame) == .valid);
}

test "READY: only a READY frame of this version counts" {
    const ready = encodeReady(7);
    try testing.expect(isReady(&ready));
    try testing.expectEqual(@as(u32, 7), std.mem.readInt(u32, ready[8..12], .little));
    try testing.expect(!isReady(&encodeSegment(example)));
    var other = ready;
    other[5] = 2;
    try testing.expect(!isReady(&other));
    other = ready;
    other[1] = 0;
    try testing.expect(!isReady(&other));
}
