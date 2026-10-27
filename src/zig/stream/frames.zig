//! A stream connection's two frames on the control connection (docs/protocol.md §3.1): SEGMENT, which the server sends
//! once it has accepted a client and created its segment, and READY, the client's answer. Each is a 64-byte frame
//! (core/wire.zig) of kind `.stream`. On Linux and macOS the SEGMENT frame carries the segment (a memfd on
//! Linux, a POSIX shared memory object on macOS) as its one `SCM_RIGHTS` descriptor (platform/posix.zig `sendMsg`,
//! `recvSegment`). Their layouts are `wire_protocol.ControlStreamSegment` and `wire_protocol.ControlStreamReady`, whose offsets
//! wire_protocol.zig checks at compile time:
//!
//! | Offset | Size | Field                              | SEGMENT | READY |
//! |--------|------|------------------------------------|---------|-------|
//! | 0      | 12   | the header (`wire_protocol.ControlHeader`)   | type 1  | type 2|
//! | 12     | 4    | `capacity`                         | yes     |       |
//! | 16     | 8    | `segment_size`                     | yes     |       |
//! | 24     | 16   | `session_id`                       | yes     |       |

const std = @import("std");
const wire_protocol = @import("../wire_protocol.zig");
const wire = @import("../core/wire.zig");

const Frame = wire.Frame;

pub const version: u8 = wire_protocol.protocol_version;

const Type = wire_protocol.StreamFrameType;

/// What a SEGMENT frame offers: the client's segment.
pub const Offer = struct {
    capacity: u32,
    segment_size: u64,
    session_id: [16]u8,
};

fn newHeader(frame_type: Type, pid: u32) wire_protocol.ControlHeader {
    return wire.newHeader(.stream, @backingInt(frame_type), version, pid);
}

/// SEGMENT, from process `pid`.
pub fn encodeSegment(pid: u32, offer: Offer) Frame {
    return wire.encode(wire_protocol.ControlStreamSegment{
        .header = newHeader(.segment, pid),
        .capacity = offer.capacity,
        .segment_size = offer.segment_size,
        .session_id = offer.session_id,
    });
}

/// READY, from process `pid`.
pub fn encodeReady(pid: u32) Frame {
    return wire.encode(wire_protocol.ControlStreamReady{ .header = newHeader(.ready, pid) });
}

/// What the client's check of a SEGMENT frame found (docs/protocol.md §3.3).
pub const SegmentCheck = union(enum) {
    valid: Offer,
    other_version,
    other_kind,
    rejected,
};

/// Checks a SEGMENT frame, in this order:
///   1. magic and type, else `rejected`: not a SEGMENT of this protocol (the client retries after a back-off);
///   2. the kind, else `other_kind`: a broadcast's, a queue's or a service's OFFER;
///   3. the version, else `other_version`: another protocol version.
/// The last two fail the connect for good (Invalid), never retried. The client checks the capacity the server chose
/// and the segment size it implies itself (stream/client.zig `awaitSegment`, a Retry if they are bad), and the attach
/// checks the segment (stream/segment.zig).
pub fn checkSegment(frame: *const Frame) SegmentCheck {
    const header = wire.headerOf(frame);
    if (header.magic != wire.magic or header.frame_type != @backingInt(Type.segment)) return .rejected;
    if (header.kind != .stream) return .other_kind;
    if (header.version != version) return .other_version;
    const segment = wire.decode(wire_protocol.ControlStreamSegment, frame);
    return .{ .valid = .{
        .capacity = segment.capacity,
        .segment_size = segment.segment_size,
        .session_id = segment.session_id,
    } };
}

/// Whether `frame` is a READY of this protocol. Anything else in its place (another type, kind or version, garbage)
/// ends the handshake like the end of the stream.
pub fn isReady(frame: *const Frame) bool {
    return wire.isFrame(wire.headerOf(frame), .stream, @backingInt(Type.ready), version);
}

// === Tests ===

const testing = std.testing;

pub const example: Offer = .{
    .capacity = 4096,
    .segment_size = 12288,
    .session_id = .{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 },
};

/// `frame`'s fields, to change one in place.
fn fieldsOf(comptime Control: type, frame: *Frame) *align(1) Control {
    return @ptrCast(frame);
}

test "a SEGMENT frame round-trips, with the layout of the protocol" {
    const frame = encodeSegment(4242, example);
    try testing.expectEqualSlices(u8, "FIPC", frame[0..4]);
    try testing.expectEqual(@as(u32, 4242), std.mem.readInt(u32, frame[4..8], .little));
    try testing.expectEqual(@as(u8, 1), frame[8]); // type
    try testing.expectEqual(@as(u8, 1), frame[9]); // version
    try testing.expectEqual(@as(u8, 0), frame[10]); // a stream's kind
    try testing.expectEqual(@as(u8, 0), frame[11]);
    try testing.expectEqual(@as(u32, 4096), std.mem.readInt(u32, frame[12..16], .little));
    try testing.expectEqual(@as(u64, 12288), std.mem.readInt(u64, frame[16..24], .little));
    try testing.expectEqualSlices(u8, &example.session_id, frame[24..40]);
    try testing.expectEqualSlices(u8, &@as([24]u8, @splat(0)), frame[40..64]);
    try testing.expectEqualDeep(SegmentCheck{ .valid = example }, checkSegment(&frame));
}

test "a SEGMENT frame is refused for its magic or type, is a channel's for its kind, and a mismatch for its version" {
    var frame = encodeSegment(4242, example);
    fieldsOf(wire_protocol.ControlStreamSegment, &frame).header.magic = std.mem.readInt(u32, "XIPC", .little);
    try testing.expectEqual(SegmentCheck.rejected, checkSegment(&frame));

    frame = encodeSegment(4242, example);
    fieldsOf(wire_protocol.ControlStreamSegment, &frame).header.frame_type = @backingInt(Type.ready);
    try testing.expectEqual(SegmentCheck.rejected, checkSegment(&frame));

    // A broadcast's, a queue's or a service's OFFER, or another channel kind's
    for ([_]wire_protocol.Kind{ .broadcast, .queue, .service, @fromBackingInt(4) }) |kind| {
        frame = encodeSegment(4242, example);
        fieldsOf(wire_protocol.ControlStreamSegment, &frame).header.kind = kind;
        try testing.expectEqual(SegmentCheck.other_kind, checkSegment(&frame));
    }

    // Another version's SEGMENT
    frame = encodeSegment(4242, example);
    fieldsOf(wire_protocol.ControlStreamSegment, &frame).header.version = version + 1;
    try testing.expectEqual(SegmentCheck.other_version, checkSegment(&frame));

    // Reserved bytes are ignored
    frame = encodeSegment(4242, example);
    fieldsOf(wire_protocol.ControlStreamSegment, &frame).header.reserved = 0xff;
    frame[63] = 0xff;
    try testing.expect(checkSegment(&frame) == .valid);
}

test "READY: only a READY frame of this version counts" {
    const ready = encodeReady(7);
    try testing.expect(isReady(&ready));
    try testing.expectEqual(@as(u32, 7), std.mem.readInt(u32, ready[4..8], .little));
    try testing.expectEqual(@as(u8, 2), ready[8]);
    try testing.expectEqualSlices(u8, &@as([52]u8, @splat(0)), ready[12..64]);
    try testing.expect(!isReady(&encodeSegment(4242, example)));
    var other = ready;
    fieldsOf(wire_protocol.ControlStreamReady, &other).header.version = version + 1;
    try testing.expect(!isReady(&other));
    other = ready;
    fieldsOf(wire_protocol.ControlStreamReady, &other).header.kind = .broadcast;
    try testing.expect(!isReady(&other));
    other = ready;
    other[1] = 0;
    try testing.expect(!isReady(&other));
}
