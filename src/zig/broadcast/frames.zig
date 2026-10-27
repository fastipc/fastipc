//! A broadcast's frames on its control connections (docs/protocol.md §9): OFFER (type 1, as SEGMENT, of kind
//! `.broadcast`) and the segment's descriptor where it is passed, JOIN, WELCOME, and later UNSUBSCRIBE and
//! UNSUBSCRIBED. Each is a 64-byte frame (core/wire.zig): `wire_protocol.ControlBroadcastOffer`, `wire_protocol.ControlBroadcastJoin`, and
//! `wire_protocol.ControlBroadcastSlot` for the last three. A joiner classifies its first frame (core/control.zig
//! `awaitOffer`) before it decodes OFFER here; a client of a stream connection and a queue's joiner read OFFER's kind
//! and refuse the name at once.

const std = @import("std");
const wire_protocol = @import("../wire_protocol.zig");
const wire = @import("../core/wire.zig");

const Frame = wire.Frame;
const Role = wire_protocol.SubscriberRole;

/// What an OFFER frame offers: the channel's segment, and the subscriber's slot in it. Its sizes and slot are the
/// peer's: the attach checks them (segment.zig `attach`).
pub const Offer = struct {
    /// `wire_protocol.offer_flag_gated`.
    flags: u32,
    capacity: u32,
    segment_size: u64,
    session_id: [16]u8,
    slot: u32,
    generation: u32,
    max_subscribers: u32,
};

fn newHeader(frame_type: wire_protocol.ChannelFrameType, pid: u32) wire_protocol.ControlHeader {
    return wire.newHeader(.broadcast, @backingInt(frame_type), wire_protocol.channel_protocol_version, pid);
}

/// OFFER, of a broadcast channel, from process `pid`.
pub fn encodeOffer(pid: u32, offer: Offer) Frame {
    return wire.encode(wire_protocol.ControlBroadcastOffer{
        .header = newHeader(.offer, pid),
        .capacity = offer.capacity,
        .segment_size = offer.segment_size,
        .session_id = offer.session_id,
        .slot = offer.slot,
        .slot_generation = offer.generation,
        .max_subscribers = offer.max_subscribers,
        .flags = offer.flags,
    });
}

/// The offer of a frame that core/wire.zig `classifyOffer` found to be a broadcast's OFFER.
pub fn decodeOffer(frame: *const Frame) Offer {
    const offer = wire.decode(wire_protocol.ControlBroadcastOffer, frame);
    return .{
        .flags = offer.flags,
        .capacity = offer.capacity,
        .segment_size = offer.segment_size,
        .session_id = offer.session_id,
        .slot = offer.slot,
        .generation = offer.slot_generation,
        .max_subscribers = offer.max_subscribers,
    };
}

/// JOIN: the joiner's answer to OFFER, with its role and the offer's slot and generation echoed.
pub fn encodeJoin(pid: u32, role: Role, slot: u32, generation: u32) Frame {
    return wire.encode(wire_protocol.ControlBroadcastJoin{
        .header = newHeader(.join, pid),
        .role = @backingInt(role),
        .slot = slot,
        .slot_generation = generation,
    });
}

/// WELCOME, UNSUBSCRIBE and UNSUBSCRIBED: a frame of `frame_type` that names the subscriber's slot and generation.
pub fn encodeSlotFrame(frame_type: wire_protocol.ChannelFrameType, pid: u32, slot: u32, generation: u32) Frame {
    std.debug.assert(frame_type == .welcome or frame_type == .unsubscribe or frame_type == .unsubscribed);
    return wire.encode(wire_protocol.ControlBroadcastSlot{ .header = newHeader(frame_type, pid), .slot = slot, .slot_generation = generation });
}

/// The owner's check of JOIN: magic, type, version and kind, the slot and generation it offered, and a role that is a
/// subscriber's, which it returns. Null for anything else: the owner drops the joiner.
pub fn checkJoin(frame: *const Frame, slot: u32, generation: u32) ?Role {
    const join = wire.decode(wire_protocol.ControlBroadcastJoin, frame);
    if (!isFrame(join.header, .join) or join.slot != slot or join.slot_generation != generation) return null;
    return std.enums.fromInt(Role, join.role);
}

/// Whether `frame` is a broadcast's frame of `frame_type` (WELCOME, UNSUBSCRIBE, UNSUBSCRIBED) of this channel
/// protocol for `slot` and `generation`. Anything else in its place ends the exchange like the end of the stream.
pub fn checkSlotFrame(frame: *const Frame, frame_type: wire_protocol.ChannelFrameType, slot: u32, generation: u32) bool {
    const decoded = wire.decode(wire_protocol.ControlBroadcastSlot, frame);
    return isFrame(decoded.header, frame_type) and decoded.slot == slot and decoded.slot_generation == generation;
}

fn isFrame(header: wire_protocol.ControlHeader, frame_type: wire_protocol.ChannelFrameType) bool {
    return wire.isFrame(header, .broadcast, @backingInt(frame_type), wire_protocol.channel_protocol_version);
}

// === Tests ===

const testing = std.testing;
// The frames a channel's must be told apart from: a stream connection's
const stream_frames = @import("../stream/frames.zig");

const example_offer: Offer = .{
    .flags = wire_protocol.offer_flag_gated,
    .capacity = 65536,
    .segment_size = 266240,
    .session_id = .{ 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1 },
    .slot = 1023,
    .generation = 77,
    .max_subscribers = 1024,
};

/// `frame`'s fields, to change one in place.
fn fieldsOf(comptime Control: type, frame: *Frame) *align(1) Control {
    return @ptrCast(frame);
}

test "an OFFER frame round-trips, with the layout of the protocol" {
    const frame = encodeOffer(4242, example_offer);
    try testing.expectEqualSlices(u8, "FIPC", frame[0..4]);
    try testing.expectEqual(@as(u32, 4242), std.mem.readInt(u32, frame[4..8], .little));
    try testing.expectEqual(@as(u8, 1), frame[8]); // SEGMENT's type
    try testing.expectEqual(@as(u8, 1), frame[9]); // the channel protocol's version
    try testing.expectEqual(@as(u8, 1), frame[10]); // broadcast
    try testing.expectEqual(@as(u8, 0), frame[11]);
    try testing.expectEqual(@as(u32, 65536), std.mem.readInt(u32, frame[12..16], .little));
    try testing.expectEqual(@as(u64, 266240), std.mem.readInt(u64, frame[16..24], .little));
    try testing.expectEqualSlices(u8, &example_offer.session_id, frame[24..40]);
    try testing.expectEqual(@as(u32, 1023), std.mem.readInt(u32, frame[40..44], .little));
    try testing.expectEqual(@as(u32, 77), std.mem.readInt(u32, frame[44..48], .little));
    try testing.expectEqual(@as(u32, 1024), std.mem.readInt(u32, frame[48..52], .little));
    try testing.expectEqual(@as(u32, 1), std.mem.readInt(u32, frame[52..56], .little)); // gated allowed
    try testing.expectEqualSlices(u8, &@as([8]u8, @splat(0)), frame[56..64]);
    // Every field read back as encoded
    try testing.expectEqual(wire.OfferClass.offer, wire.classifyOffer(&frame, .broadcast));
    try testing.expectEqualDeep(example_offer, decodeOffer(&frame));
    try testing.expectEqualSlices(u8, &frame, &encodeOffer(4242, decodeOffer(&frame)));
}

test "a client of fipc_stream_connect refuses an OFFER at once, for its kind" {
    const frame = encodeOffer(4242, example_offer);
    try testing.expectEqual(stream_frames.SegmentCheck.other_kind, stream_frames.checkSegment(&frame));
    // Of any channel protocol version
    var other = frame;
    fieldsOf(wire_protocol.ControlBroadcastOffer, &other).header.version = wire_protocol.channel_protocol_version + 1;
    try testing.expectEqual(stream_frames.SegmentCheck.other_kind, stream_frames.checkSegment(&other));
}

test "the joiner tells a SEGMENT, another channel version or kind and a malformed frame from an OFFER" {
    try testing.expectEqual(wire.OfferClass.stream_listener, wire.classifyOffer(&stream_frames.encodeSegment(4242, stream_frames.example), .broadcast));
    var frame = stream_frames.encodeSegment(4242, stream_frames.example);
    fieldsOf(wire_protocol.ControlStreamSegment, &frame).header.version = wire_protocol.protocol_version + 1; // a SEGMENT of a later stream protocol
    try testing.expectEqual(wire.OfferClass.stream_listener, wire.classifyOffer(&frame, .broadcast));

    frame = encodeOffer(4242, example_offer);
    fieldsOf(wire_protocol.ControlBroadcastOffer, &frame).header.version = wire_protocol.channel_protocol_version + 1;
    try testing.expectEqual(wire.OfferClass.other_version, wire.classifyOffer(&frame, .broadcast));
    frame = encodeOffer(4242, example_offer);
    fieldsOf(wire_protocol.ControlBroadcastOffer, &frame).header.kind = .queue;
    try testing.expectEqual(wire.OfferClass.other_kind, wire.classifyOffer(&frame, .broadcast));
    frame = encodeOffer(4242, example_offer);
    frame[0] = 'X';
    try testing.expectEqual(wire.OfferClass.rejected, wire.classifyOffer(&frame, .broadcast));
    frame = encodeOffer(4242, example_offer);
    fieldsOf(wire_protocol.ControlBroadcastOffer, &frame).header.frame_type = @backingInt(wire_protocol.ChannelFrameType.join);
    try testing.expectEqual(wire.OfferClass.rejected, wire.classifyOffer(&frame, .broadcast));
    // Reserved bytes are ignored
    frame = encodeOffer(4242, example_offer);
    fieldsOf(wire_protocol.ControlBroadcastOffer, &frame).header.reserved = 0xff;
    frame[63] = 0xff;
    try testing.expectEqual(wire.OfferClass.offer, wire.classifyOffer(&frame, .broadcast));
    try testing.expectEqualDeep(example_offer, decodeOffer(&frame));
}

test "JOIN: the offered slot and generation echoed, and a role that is one" {
    const join = encodeJoin(7, .gated, 3, 9);
    try testing.expectEqual(@as(u8, 3), join[8]);
    try testing.expectEqual(@as(u32, 2), std.mem.readInt(u32, join[12..16], .little));
    try testing.expectEqual(@as(?Role, .gated), checkJoin(&join, 3, 9));
    try testing.expectEqual(@as(?Role, .lossy), checkJoin(&encodeJoin(7, .lossy, 3, 9), 3, 9));
    // Another slot or generation: a joiner of another attempt
    try testing.expectEqual(@as(?Role, null), checkJoin(&join, 4, 9));
    try testing.expectEqual(@as(?Role, null), checkJoin(&join, 3, 8));
    var other = join;
    for ([_]u32{ 0, 3, 4, 5 }) |role| {
        fieldsOf(wire_protocol.ControlBroadcastJoin, &other).role = role;
        try testing.expectEqual(@as(?Role, null), checkJoin(&other, 3, 9));
    }
    other = join;
    fieldsOf(wire_protocol.ControlBroadcastJoin, &other).header.kind = .queue;
    try testing.expectEqual(@as(?Role, null), checkJoin(&other, 3, 9));
    other = join;
    fieldsOf(wire_protocol.ControlBroadcastJoin, &other).header.version = wire_protocol.channel_protocol_version + 1;
    try testing.expectEqual(@as(?Role, null), checkJoin(&other, 3, 9));
    try testing.expectEqual(@as(?Role, null), checkJoin(&encodeSlotFrame(.welcome, 7, 3, 9), 3, 9));
}

test "WELCOME, UNSUBSCRIBE, UNSUBSCRIBED: their type, for the slot and generation" {
    for ([_]wire_protocol.ChannelFrameType{ .welcome, .unsubscribe, .unsubscribed }) |frame_type| {
        const frame = encodeSlotFrame(frame_type, 7, 1023, 0xffff_ffff);
        try testing.expectEqual(@backingInt(frame_type), frame[8]);
        try testing.expectEqual(@as(u8, 1), frame[10]); // broadcast
        try testing.expectEqual(@as(u32, 1023), std.mem.readInt(u32, frame[12..16], .little));
        try testing.expect(checkSlotFrame(&frame, frame_type, 1023, 0xffff_ffff));
        try testing.expect(!checkSlotFrame(&frame, frame_type, 1022, 0xffff_ffff));
        try testing.expect(!checkSlotFrame(&frame, frame_type, 1023, 0));
        var other = frame;
        fieldsOf(wire_protocol.ControlBroadcastSlot, &other).header.version = wire_protocol.channel_protocol_version + 1;
        try testing.expect(!checkSlotFrame(&other, frame_type, 1023, 0xffff_ffff));
        other = frame;
        fieldsOf(wire_protocol.ControlBroadcastSlot, &other).header.kind = .queue;
        try testing.expect(!checkSlotFrame(&other, frame_type, 1023, 0xffff_ffff));
        other = frame;
        other[3] = 'D';
        try testing.expect(!checkSlotFrame(&other, frame_type, 1023, 0xffff_ffff));
    }
    const welcome = encodeSlotFrame(.welcome, 7, 1, 1);
    try testing.expect(!checkSlotFrame(&welcome, .unsubscribe, 1, 1));
    try testing.expect(!checkSlotFrame(&stream_frames.encodeReady(7), .welcome, 0, 0));
}
