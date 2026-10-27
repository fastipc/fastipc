//! The control connection's frame (docs/protocol.md §3.1): 64 bytes, little-endian, on a byte stream. Every frame of
//! every kind begins with the same 12-byte header, `wire_protocol.ControlHeader`: the magic, "FIPC", the sender's pid, then the
//! frame's type, version and kind. Reserved fields are written as 0 and ignored on read. The magic never changes, so
//! that each side tells a peer of another kind or protocol version (another `kind` or `version`) from a corrupt one.
//!
//! The kinds' frames: a stream connection's SEGMENT and READY (stream/frames.zig, `wire_protocol.ControlStream*`), a broadcast's
//! OFFER, JOIN, WELCOME, UNSUBSCRIBE and UNSUBSCRIBED (broadcast/frames.zig, `wire_protocol.ControlBroadcast*`, docs/protocol.md
//! §9), a work queue's OFFER, JOIN and WELCOME (queue/frames.zig, `wire_protocol.ControlQueue*`, §10), and a service's
//! OFFER, JOIN and WELCOME (service/frames.zig, `wire_protocol.ControlService*`, §11).

const std = @import("std");
const wire_protocol = @import("../wire_protocol.zig");

pub const frame_len = 64;
pub const Frame = [frame_len]u8;

pub const magic: u32 = std.mem.readInt(u32, "FIPC", .little);

comptime {
    for ([_]type{
        wire_protocol.ControlStreamSegment,
        wire_protocol.ControlStreamReady,
        wire_protocol.ControlBroadcastOffer,
        wire_protocol.ControlBroadcastJoin,
        wire_protocol.ControlBroadcastSlot,
        wire_protocol.ControlQueueOffer,
        wire_protocol.ControlQueueJoin,
        wire_protocol.ControlQueueWelcome,
        wire_protocol.ControlServiceOffer,
    }) |Control| std.debug.assert(@sizeOf(Control) == frame_len);
}

/// The header of a frame this side sends: the magic, and this process's `pid`.
pub fn newHeader(kind: wire_protocol.Kind, frame_type: u8, version: u8, pid: u32) wire_protocol.ControlHeader {
    return .{ .magic = magic, .pid = pid, .frame_type = frame_type, .version = version, .kind = kind };
}

/// The bytes of `control`, one of the `wire_protocol.Control*` frames.
pub fn encode(control: anytype) Frame {
    return std.mem.toBytes(control);
}

/// `frame` as a `Control` frame, from bytes received in any alignment.
pub fn decode(comptime Control: type, frame: *const Frame) Control {
    return std.mem.bytesToValue(Control, frame);
}

/// The header of `frame`, from bytes received in any alignment.
pub fn headerOf(frame: *const Frame) wire_protocol.ControlHeader {
    return std.mem.bytesToValue(wire_protocol.ControlHeader, frame[0..@sizeOf(wire_protocol.ControlHeader)]);
}

/// Whether `header` is a frame of `kind`, of type `frame_type` and of protocol `version`.
pub fn isFrame(header: wire_protocol.ControlHeader, kind: wire_protocol.Kind, frame_type: u8, version: u8) bool {
    return header.magic == magic and header.kind == kind and header.frame_type == frame_type and header.version == version;
}

/// What a joiner's first frame is, against the OFFER of a channel of `kind` (docs/protocol.md §9.1, §10.1, §11.1), in
/// this order: magic and type 1 (else `rejected`: a malformed frame, retried); a stream's kind (`stream_listener`: a
/// stream's server, of any version); another channel protocol version (`other_version`); another kind (`other_kind`).
/// The three are a permanent INVALID. Each kind decodes and checks the rest.
pub const OfferClass = enum { offer, stream_listener, other_version, other_kind, rejected };

pub fn classifyOffer(frame: *const Frame, kind: wire_protocol.Kind) OfferClass {
    const header = headerOf(frame);
    if (header.magic != magic or header.frame_type != @backingInt(wire_protocol.ChannelFrameType.offer)) return .rejected;
    if (header.kind == .stream) return .stream_listener;
    if (header.version != wire_protocol.channel_protocol_version) return .other_version;
    if (header.kind != kind) return .other_kind;
    return .offer;
}
