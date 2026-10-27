//! A work queue's frames on its control connections (docs/protocol.md §10.2): OFFER (type 1, as SEGMENT, of kind
//! `.queue`) and the segment's descriptor where it is passed, JOIN, WELCOME. Each is a 64-byte frame
//! (core/wire.zig): `wire_protocol.ControlQueueOffer`, `wire_protocol.ControlQueueJoin`, `wire_protocol.ControlQueueWelcome`. A joiner
//! classifies its first frame (core/control.zig `awaitOffer`) before it decodes OFFER here; a client of a stream
//! connection and a broadcast's joiner read OFFER's kind and refuse the name at once. Nothing travels on a member's
//! control connection after WELCOME.

const std = @import("std");
const wire_protocol = @import("../wire_protocol.zig");
const wire = @import("../core/wire.zig");

const Frame = wire.Frame;
const Role = wire_protocol.QueueRole;

/// What an OFFER frame offers: the queue's segment. Its sizes are the peer's: the attach checks them (segment.zig
/// `attach`).
pub const Offer = struct {
    capacity: u32,
    segment_size: u64,
    session_id: [16]u8,
    max_producers: u32,
    max_consumers: u32,
};

fn newHeader(frame_type: wire_protocol.ChannelFrameType, pid: u32) wire_protocol.ControlHeader {
    return wire.newHeader(.queue, @backingInt(frame_type), wire_protocol.channel_protocol_version, pid);
}

/// OFFER, of a work queue, from process `pid`.
pub fn encodeOffer(pid: u32, offer: Offer) Frame {
    return wire.encode(wire_protocol.ControlQueueOffer{
        .header = newHeader(.offer, pid),
        .capacity = offer.capacity,
        .segment_size = offer.segment_size,
        .session_id = offer.session_id,
        .max_producers = offer.max_producers,
        .max_consumers = offer.max_consumers,
    });
}

/// The offer of a frame that core/wire.zig `classifyOffer` found to be a queue's OFFER.
pub fn decodeOffer(frame: *const Frame) Offer {
    const offer = wire.decode(wire_protocol.ControlQueueOffer, frame);
    return .{
        .capacity = offer.capacity,
        .segment_size = offer.segment_size,
        .session_id = offer.session_id,
        .max_producers = offer.max_producers,
        .max_consumers = offer.max_consumers,
    };
}

/// JOIN: the joiner's answer to OFFER, with its role.
pub fn encodeJoin(pid: u32, role: Role) Frame {
    return wire.encode(wire_protocol.ControlQueueJoin{ .header = newHeader(.join, pid), .role = @backingInt(role) });
}

/// WELCOME: the member's role, echoed, and its slot and the slot's generation.
pub fn encodeWelcome(pid: u32, role: Role, slot: u32, generation: u32) Frame {
    return wire.encode(wire_protocol.ControlQueueWelcome{
        .header = newHeader(.welcome, pid),
        .role = @backingInt(role),
        .slot = slot,
        .slot_generation = generation,
    });
}

/// The owner's check of JOIN: magic, type, version and kind, and a role that is a queue's, which it returns. Null for
/// anything else: the owner drops the joiner.
pub fn checkJoin(frame: *const Frame) ?Role {
    const join = wire.decode(wire_protocol.ControlQueueJoin, frame);
    if (!isFrame(join.header, .join)) return null;
    return std.enums.fromInt(Role, join.role);
}

/// What a valid WELCOME says: the member's slot, and its generation.
pub const Welcome = struct { slot: u32, generation: u32 };

/// The joiner's check of WELCOME: magic, type, version and kind, its own `role` echoed, and a slot below that role's
/// maximum (`max_slots`). Null for anything else.
pub fn checkWelcome(frame: *const Frame, role: Role, max_slots: u32) ?Welcome {
    const welcome = wire.decode(wire_protocol.ControlQueueWelcome, frame);
    if (!isFrame(welcome.header, .welcome) or welcome.role != @backingInt(role) or welcome.slot >= max_slots) return null;
    return .{ .slot = welcome.slot, .generation = welcome.slot_generation };
}

fn isFrame(header: wire_protocol.ControlHeader, frame_type: wire_protocol.ChannelFrameType) bool {
    return wire.isFrame(header, .queue, @backingInt(frame_type), wire_protocol.channel_protocol_version);
}

// === Tests ===

const testing = std.testing;
// The frames a queue's must be told apart from: a stream connection's
const stream_frames = @import("../stream/frames.zig");

const example_offer: Offer = .{
    .capacity = 1 << 20,
    .segment_size = 4096 + 9 * (528384 + (1 << 20)), // 8 producers' rings and the return ring
    .session_id = .{ 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1 },
    .max_producers = 8,
    .max_consumers = 8,
};

/// `frame`'s fields, to change one in place.
fn fieldsOf(comptime Control: type, frame: *Frame) *align(1) Control {
    return @ptrCast(frame);
}

test "a queue's OFFER frame round-trips, with the layout of the protocol" {
    const frame = encodeOffer(4242, example_offer);
    try testing.expectEqualSlices(u8, "FIPC", frame[0..4]);
    try testing.expectEqual(@as(u32, 4242), std.mem.readInt(u32, frame[4..8], .little));
    try testing.expectEqual(@as(u8, 1), frame[8]); // SEGMENT's type
    try testing.expectEqual(@as(u8, 1), frame[9]); // the channel protocol's version
    try testing.expectEqual(@as(u8, 2), frame[10]); // a work queue
    try testing.expectEqual(@as(u8, 0), frame[11]);
    try testing.expectEqual(@as(u32, 1 << 20), std.mem.readInt(u32, frame[12..16], .little));
    try testing.expectEqual(example_offer.segment_size, std.mem.readInt(u64, frame[16..24], .little));
    try testing.expectEqualSlices(u8, &example_offer.session_id, frame[24..40]);
    try testing.expectEqual(@as(u32, 8), std.mem.readInt(u32, frame[40..44], .little));
    try testing.expectEqual(@as(u32, 8), std.mem.readInt(u32, frame[44..48], .little));
    try testing.expectEqualSlices(u8, &@as([16]u8, @splat(0)), frame[48..64]);
    try testing.expectEqual(wire.OfferClass.offer, wire.classifyOffer(&frame, .queue));
    try testing.expectEqualDeep(example_offer, decodeOffer(&frame));
    try testing.expectEqualSlices(u8, &frame, &encodeOffer(4242, decodeOffer(&frame)));
}

test "the joiner tells a SEGMENT, a broadcast channel, another version and a malformed frame from a queue's OFFER" {
    try testing.expectEqual(wire.OfferClass.stream_listener, wire.classifyOffer(&stream_frames.encodeSegment(4242, stream_frames.example), .queue));
    var frame = encodeOffer(4242, example_offer);
    fieldsOf(wire_protocol.ControlQueueOffer, &frame).header.version = wire_protocol.channel_protocol_version + 1;
    try testing.expectEqual(wire.OfferClass.other_version, wire.classifyOffer(&frame, .queue));
    frame = encodeOffer(4242, example_offer);
    fieldsOf(wire_protocol.ControlQueueOffer, &frame).header.kind = @fromBackingInt(4);
    try testing.expectEqual(wire.OfferClass.other_kind, wire.classifyOffer(&frame, .queue));
    frame = encodeOffer(4242, example_offer);
    frame[0] = 'X';
    try testing.expectEqual(wire.OfferClass.rejected, wire.classifyOffer(&frame, .queue));
    // A broadcast channel's OFFER is another kind to a queue's joiner, and a queue's to a subscriber
    var broadcast = encodeOffer(4242, example_offer);
    fieldsOf(wire_protocol.ControlQueueOffer, &broadcast).header.kind = .broadcast;
    try testing.expectEqual(wire.OfferClass.other_kind, wire.classifyOffer(&broadcast, .queue));
    try testing.expectEqual(wire.OfferClass.other_kind, wire.classifyOffer(&encodeOffer(4242, example_offer), .broadcast));
    // A client of fipc_stream_connect reads it as another kind's
    try testing.expectEqual(stream_frames.SegmentCheck.other_kind, stream_frames.checkSegment(&encodeOffer(4242, example_offer)));
    // Reserved bytes are ignored
    frame = encodeOffer(4242, example_offer);
    fieldsOf(wire_protocol.ControlQueueOffer, &frame).header.reserved = 0xff;
    frame[63] = 0xff;
    try testing.expectEqual(wire.OfferClass.offer, wire.classifyOffer(&frame, .queue));
    try testing.expectEqualDeep(example_offer, decodeOffer(&frame));
}

test "JOIN: a queue's role only; WELCOME: the role echoed and a slot in range" {
    const join = encodeJoin(7, .consumer);
    try testing.expectEqual(@as(u8, 3), join[8]);
    try testing.expectEqual(@as(u8, 2), join[10]);
    try testing.expectEqual(@as(u32, 4), std.mem.readInt(u32, join[12..16], .little));
    try testing.expectEqual(@as(?Role, .consumer), checkJoin(&join));
    try testing.expectEqual(@as(?Role, .producer), checkJoin(&encodeJoin(7, .producer)));
    var other = join;
    for ([_]u32{ 0, 1, 2, 5 }) |role| {
        fieldsOf(wire_protocol.ControlQueueJoin, &other).role = role;
        try testing.expectEqual(@as(?Role, null), checkJoin(&other));
    }
    other = join;
    fieldsOf(wire_protocol.ControlQueueJoin, &other).header.kind = .broadcast;
    try testing.expectEqual(@as(?Role, null), checkJoin(&other));
    other = join;
    fieldsOf(wire_protocol.ControlQueueJoin, &other).header.version = wire_protocol.channel_protocol_version + 1;
    try testing.expectEqual(@as(?Role, null), checkJoin(&other));
    try testing.expectEqual(@as(?Role, null), checkJoin(&encodeWelcome(7, .consumer, 0, 1)));

    const welcome = encodeWelcome(7, .producer, 63, 0xffff_ffff);
    try testing.expectEqual(@as(u8, 4), welcome[8]);
    try testing.expectEqual(@as(?Welcome, .{ .slot = 63, .generation = 0xffff_ffff }), checkWelcome(&welcome, .producer, 64));
    try testing.expectEqual(@as(?Welcome, null), checkWelcome(&welcome, .consumer, 64)); // another role
    try testing.expectEqual(@as(?Welcome, null), checkWelcome(&welcome, .producer, 63)); // past the maximum
    try testing.expectEqual(@as(?Welcome, null), checkWelcome(&join, .consumer, 64));
    other = welcome;
    fieldsOf(wire_protocol.ControlQueueWelcome, &other).header.kind = .broadcast;
    try testing.expectEqual(@as(?Welcome, null), checkWelcome(&other, .producer, 64));
    other = welcome;
    other[3] = 'D';
    try testing.expectEqual(@as(?Welcome, null), checkWelcome(&other, .producer, 64));
}
