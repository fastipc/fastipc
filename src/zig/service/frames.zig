//! A service's frames on its control connections (docs/protocol.md §11.2): OFFER (type 1, as SEGMENT, of kind
//! `.service`) and the segment's descriptor where it is passed, JOIN, WELCOME. Each is a 64-byte frame (core/wire.zig):
//! `wire_protocol.ControlServiceOffer`, `wire_protocol.ControlServiceJoin`, `wire_protocol.ControlServiceWelcome` (a
//! queue's JOIN and WELCOME, of the service's kind). A joiner classifies its first frame (core/control.zig
//! `awaitOffer`) before it decodes OFFER here; a client of a stream connection and a broadcast's or a queue's joiner
//! read OFFER's kind and refuse the name at once. Nothing travels on a member's control connection after WELCOME.

const std = @import("std");
const wire_protocol = @import("../wire_protocol.zig");
const wire = @import("../core/wire.zig");

const Frame = wire.Frame;
const Role = wire_protocol.ServiceRole;

/// What an OFFER frame offers: the service's segment. Its sizes are the peer's: the attach checks them (segment.zig
/// `attach`).
pub const Offer = struct {
    capacity: u32,
    segment_size: u64,
    session_id: [16]u8,
    max_clients: u32,
    max_servers: u32,
    max_message: u32,
};

fn newHeader(frame_type: wire_protocol.ChannelFrameType, pid: u32) wire_protocol.ControlHeader {
    return wire.newHeader(.service, @backingInt(frame_type), wire_protocol.channel_protocol_version, pid);
}

/// OFFER, of a service, from process `pid`.
pub fn encodeOffer(pid: u32, offer: Offer) Frame {
    return wire.encode(wire_protocol.ControlServiceOffer{
        .header = newHeader(.offer, pid),
        .capacity = offer.capacity,
        .segment_size = offer.segment_size,
        .session_id = offer.session_id,
        .max_clients = offer.max_clients,
        .max_servers = offer.max_servers,
        .max_message = offer.max_message,
    });
}

/// The offer of a frame that core/wire.zig `classifyOffer` found to be a service's OFFER.
pub fn decodeOffer(frame: *const Frame) Offer {
    const offer = wire.decode(wire_protocol.ControlServiceOffer, frame);
    return .{
        .capacity = offer.capacity,
        .segment_size = offer.segment_size,
        .session_id = offer.session_id,
        .max_clients = offer.max_clients,
        .max_servers = offer.max_servers,
        .max_message = offer.max_message,
    };
}

/// JOIN: the joiner's answer to OFFER, with its role.
pub fn encodeJoin(pid: u32, role: Role) Frame {
    return wire.encode(wire_protocol.ControlServiceJoin{ .header = newHeader(.join, pid), .role = @backingInt(role) });
}

/// WELCOME: the member's role, echoed, and its slot and the slot's generation.
pub fn encodeWelcome(pid: u32, role: Role, slot: u32, generation: u32) Frame {
    return wire.encode(wire_protocol.ControlServiceWelcome{
        .header = newHeader(.welcome, pid),
        .role = @backingInt(role),
        .slot = slot,
        .slot_generation = generation,
    });
}

/// The owner's check of JOIN: magic, type, version and kind, and a role that is a service's, which it returns. Null for
/// anything else: the owner drops the joiner.
pub fn checkJoin(frame: *const Frame) ?Role {
    const join = wire.decode(wire_protocol.ControlServiceJoin, frame);
    if (!isFrame(join.header, .join)) return null;
    return std.enums.fromInt(Role, join.role);
}

/// What a valid WELCOME says: the member's slot, and its generation.
pub const Welcome = struct { slot: u32, generation: u32 };

/// The joiner's check of WELCOME: magic, type, version and kind, its own `role` echoed, and a slot below that role's
/// maximum (`max_slots`). Null for anything else.
pub fn checkWelcome(frame: *const Frame, role: Role, max_slots: u32) ?Welcome {
    const welcome = wire.decode(wire_protocol.ControlServiceWelcome, frame);
    if (!isFrame(welcome.header, .welcome) or welcome.role != @backingInt(role) or welcome.slot >= max_slots) return null;
    return .{ .slot = welcome.slot, .generation = welcome.slot_generation };
}

fn isFrame(header: wire_protocol.ControlHeader, frame_type: wire_protocol.ChannelFrameType) bool {
    return wire.isFrame(header, .service, @backingInt(frame_type), wire_protocol.channel_protocol_version);
}

// === Tests ===

const testing = std.testing;
// The frames a service's must be told apart from: a stream connection's, a queue's
const stream_frames = @import("../stream/frames.zig");
const queue_frames = @import("../queue/frames.zig");

const example_offer: Offer = .{
    .capacity = 1 << 16,
    .segment_size = 4096 + 8 * (36864 + (1 << 16)), // 8 clients' rings
    .session_id = .{ 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1 },
    .max_clients = 8,
    .max_servers = 4,
    .max_message = 256,
};

/// `frame`'s fields, to change one in place.
fn fieldsOf(comptime Control: type, frame: *Frame) *align(1) Control {
    return @ptrCast(frame);
}

test "a service's OFFER frame round-trips, with the layout of the protocol" {
    const frame = encodeOffer(4242, example_offer);
    try testing.expectEqualSlices(u8, "FIPC", frame[0..4]);
    try testing.expectEqual(@as(u32, 4242), std.mem.readInt(u32, frame[4..8], .little));
    try testing.expectEqual(@as(u8, 1), frame[8]); // SEGMENT's type
    try testing.expectEqual(@as(u8, 1), frame[9]); // the channel protocol's version
    try testing.expectEqual(@as(u8, 3), frame[10]); // a service
    try testing.expectEqual(@as(u8, 0), frame[11]);
    try testing.expectEqual(@as(u32, 1 << 16), std.mem.readInt(u32, frame[12..16], .little));
    try testing.expectEqual(example_offer.segment_size, std.mem.readInt(u64, frame[16..24], .little));
    try testing.expectEqualSlices(u8, &example_offer.session_id, frame[24..40]);
    try testing.expectEqual(@as(u32, 8), std.mem.readInt(u32, frame[40..44], .little));
    try testing.expectEqual(@as(u32, 4), std.mem.readInt(u32, frame[44..48], .little));
    try testing.expectEqual(@as(u32, 256), std.mem.readInt(u32, frame[48..52], .little));
    try testing.expectEqualSlices(u8, &@as([12]u8, @splat(0)), frame[52..64]);
    try testing.expectEqual(wire.OfferClass.offer, wire.classifyOffer(&frame, .service));
    try testing.expectEqualDeep(example_offer, decodeOffer(&frame));
    try testing.expectEqualSlices(u8, &frame, &encodeOffer(4242, decodeOffer(&frame)));
}

test "the joiner tells a SEGMENT, a queue's and a broadcast's OFFER, another version and a malformed frame from a service's OFFER" {
    try testing.expectEqual(wire.OfferClass.stream_listener, wire.classifyOffer(&stream_frames.encodeSegment(4242, stream_frames.example), .service));
    var frame = encodeOffer(4242, example_offer);
    fieldsOf(wire_protocol.ControlServiceOffer, &frame).header.version = wire_protocol.channel_protocol_version + 1;
    try testing.expectEqual(wire.OfferClass.other_version, wire.classifyOffer(&frame, .service));
    frame = encodeOffer(4242, example_offer);
    fieldsOf(wire_protocol.ControlServiceOffer, &frame).header.version = 0; // a version below this one
    try testing.expectEqual(wire.OfferClass.other_version, wire.classifyOffer(&frame, .service));
    for ([_]wire_protocol.Kind{ .broadcast, .queue, @fromBackingInt(4) }) |kind| {
        frame = encodeOffer(4242, example_offer);
        fieldsOf(wire_protocol.ControlServiceOffer, &frame).header.kind = kind;
        try testing.expectEqual(wire.OfferClass.other_kind, wire.classifyOffer(&frame, .service));
    }
    frame = encodeOffer(4242, example_offer);
    frame[0] = 'X';
    try testing.expectEqual(wire.OfferClass.rejected, wire.classifyOffer(&frame, .service));
    // A service's OFFER is another kind to a queue's and a broadcast's joiner, and a queue's to a service's
    for ([_]wire_protocol.Kind{ .queue, .broadcast }) |kind| {
        try testing.expectEqual(wire.OfferClass.other_kind, wire.classifyOffer(&encodeOffer(4242, example_offer), kind));
    }
    const queue_offer = queue_frames.encodeOffer(4242, .{ .capacity = 4096, .segment_size = 20480, .session_id = example_offer.session_id, .max_producers = 1, .max_consumers = 1 });
    try testing.expectEqual(wire.OfferClass.other_kind, wire.classifyOffer(&queue_offer, .service));
    // A client of fipc_stream_connect reads it as another kind's
    try testing.expectEqual(stream_frames.SegmentCheck.other_kind, stream_frames.checkSegment(&encodeOffer(4242, example_offer)));
    // Reserved bytes are ignored
    frame = encodeOffer(4242, example_offer);
    fieldsOf(wire_protocol.ControlServiceOffer, &frame).header.reserved = 0xff;
    frame[63] = 0xff;
    try testing.expectEqual(wire.OfferClass.offer, wire.classifyOffer(&frame, .service));
    try testing.expectEqualDeep(example_offer, decodeOffer(&frame));
}

test "JOIN: a service's role only, and a queue's JOIN isn't one; WELCOME: the role echoed and a slot in range" {
    const join = encodeJoin(7, .server);
    try testing.expectEqual(@as(u8, 3), join[8]);
    try testing.expectEqual(@as(u8, 3), join[10]);
    try testing.expectEqual(@as(u32, 6), std.mem.readInt(u32, join[12..16], .little));
    try testing.expectEqual(@as(?Role, .server), checkJoin(&join));
    try testing.expectEqual(@as(?Role, .client), checkJoin(&encodeJoin(7, .client)));
    var other = join;
    for ([_]u32{ 0, 1, 2, 3, 4, 7 }) |role| {
        fieldsOf(wire_protocol.ControlServiceJoin, &other).role = role;
        try testing.expectEqual(@as(?Role, null), checkJoin(&other));
    }
    try testing.expectEqual(@as(?Role, null), checkJoin(&queue_frames.encodeJoin(7, .consumer)));
    // A service's role in a queue's JOIN is no queue's
    try testing.expectEqual(@as(?wire_protocol.QueueRole, null), queue_frames.checkJoin(&join));
    other = join;
    fieldsOf(wire_protocol.ControlServiceJoin, &other).header.kind = .queue;
    try testing.expectEqual(@as(?Role, null), checkJoin(&other));
    other = join;
    fieldsOf(wire_protocol.ControlServiceJoin, &other).header.version = wire_protocol.channel_protocol_version + 1;
    try testing.expectEqual(@as(?Role, null), checkJoin(&other));
    try testing.expectEqual(@as(?Role, null), checkJoin(&encodeWelcome(7, .server, 0, 1)));

    const welcome = encodeWelcome(7, .client, 63, 0xffff_ffff);
    try testing.expectEqual(@as(u8, 4), welcome[8]);
    try testing.expectEqual(@as(?Welcome, .{ .slot = 63, .generation = 0xffff_ffff }), checkWelcome(&welcome, .client, 64));
    try testing.expectEqual(@as(?Welcome, null), checkWelcome(&welcome, .server, 64)); // another role
    try testing.expectEqual(@as(?Welcome, null), checkWelcome(&welcome, .client, 63)); // past the maximum
    try testing.expectEqual(@as(?Welcome, null), checkWelcome(&join, .server, 64));
    try testing.expectEqual(@as(?Welcome, null), checkWelcome(&queue_frames.encodeWelcome(7, .producer, 0, 1), .client, 64));
    other = welcome;
    fieldsOf(wire_protocol.ControlServiceWelcome, &other).header.kind = .queue;
    try testing.expectEqual(@as(?Welcome, null), checkWelcome(&other, .client, 64));
    other = welcome;
    other[3] = 'D';
    try testing.expectEqual(@as(?Welcome, null), checkWelcome(&other, .client, 64));
}
