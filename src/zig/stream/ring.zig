//! One direction of a session's segment: its indices in the header, its capacity and its data bytes, and the frames in
//! it. Head and tail are monotonic byte counts; a position's offset in the data is `pos % capacity` (capacities are
//! powers of two).
//!
//! A frame is a 16-byte header (`FrameHeader`) and a piece of a message, padded to a multiple of 16 bytes
//! (`frameLen`), and it never wraps: a writer whose next frame doesn't fit before the ring's end fills the end with a
//! PAD frame first. So every header and every piece lies in one piece of memory, copies are single copies, and a
//! piece's bytes are 16-byte aligned (the data starts at offset 640 of a page-aligned mapping).
//!
//! A ring is built from the endpoint's view, never from the header: its capacity is the local one, checked once at
//! attach. The view is the endpoint's hot line (`Hot`), which is why this file imports core/endpoint.zig.

const std = @import("std");
const wire_protocol = @import("../wire_protocol.zig");
const Hot = @import("../core/endpoint.zig").Hot;

pub const FrameHeader = wire_protocol.FrameHeader;
pub const frame_header_len = @sizeOf(FrameHeader);

/// `fipc_max_piece` of a connection whose rings hold `capacity` bytes: the capacity less the framing's share.
pub fn maxPieceOf(capacity: u64) usize {
    return @intCast(capacity - wire_protocol.piece_overhead);
}

/// The bytes a frame of a piece of `piece_len` bytes takes in the ring: its header and the piece, padded to
/// `wire_protocol.frame_align`.
pub fn frameLen(piece_len: u64) u64 {
    return std.mem.alignForward(u64, frame_header_len + piece_len, wire_protocol.frame_align);
}

pub const Ring = struct {
    indices: *wire_protocol.RingIndices,
    capacity: u32,
    data: [*]u8,
    /// The server-to-client direction (selects the Windows events).
    is_s2c: bool,

    /// The direction this side writes: s2c for the server, c2s for the client. The caller has seen `view_published`
    /// set (.acquire), so the view's fields are the session's.
    pub fn forSend(hot: *const Hot) Ring {
        return direction(hot, hot.view_server.load(.monotonic));
    }

    /// The direction this side reads: c2s for the server, s2c for the client.
    pub fn forRecv(hot: *const Hot) Ring {
        return direction(hot, !hot.view_server.load(.monotonic));
    }

    fn direction(hot: *const Hot, s2c: bool) Ring {
        const header = hot.stream.header.load(.monotonic).?;
        const data = hot.stream.data.load(.monotonic).?;
        const capacity = hot.view_capacity.load(.monotonic);
        return if (s2c)
            .{ .indices = &header.s2c, .capacity = capacity, .data = data, .is_s2c = true }
        else
            .{ .indices = &header.c2s, .capacity = capacity, .data = data + capacity, .is_s2c = false };
    }

    /// Bytes published by the writer.
    pub fn head(ring: Ring) u64 {
        return ring.indices.writer.head.load(.acquire);
    }

    /// Bytes consumed by the reader.
    pub fn tail(ring: Ring) u64 {
        return ring.indices.reader.tail.load(.acquire);
    }

    /// Offset of `pos` in the data.
    pub fn offset(ring: Ring, pos: u64) u32 {
        return @truncate(pos & (ring.capacity - 1));
    }

    /// The longest piece (`maxPieceOf`).
    pub fn maxPiece(ring: Ring) usize {
        return maxPieceOf(ring.capacity);
    }

    /// Whether the frame `header` heads at `pos` ends before the ring's end, as every frame must. The header itself was
    /// read at `pos` before it could be checked: at most 15 bytes past the ring's end if a corrupt peer moved the
    /// index off a frame's boundary, which is still the mapping (the other ring, or the segment's last page, follows).
    pub fn fits(ring: Ring, pos: u64, header: FrameHeader) bool {
        return @as(u64, ring.offset(pos)) + frameLen(header.frag_len) <= ring.capacity;
    }

    /// The piece's bytes, or a part of them, at `pos`: `len` bytes that the caller has checked lie in the ring.
    pub inline fn bytes(ring: Ring, pos: u64, len: usize) []u8 {
        return ring.data[ring.offset(pos)..][0..len];
    }

    pub inline fn readHeader(ring: Ring, pos: u64) FrameHeader {
        return @as(*align(1) const FrameHeader, @ptrCast(ring.data + ring.offset(pos))).*;
    }

    /// A store of each field. Building the header in memory first and copying its bytes would make the copy's loads
    /// wait on the stores just before them. `total_len` is stored atomically: a reader may be watching it for a stamp
    /// (`stamped`).
    pub inline fn writeHeader(ring: Ring, pos: u64, header: FrameHeader) void {
        @as(*align(1) Lead, @ptrCast(ring.data + ring.offset(pos))).* = .{ .flags = header.flags, .frag_len = header.frag_len };
        @atomicStore(u64, ring.stampSlot(pos), header.total_len, .monotonic);
    }

    /// The header of a message of one piece of `len` bytes, in a frame shorter than `stamped_frame_max`, at `pos`: all
    /// of it but `total_len`, which `stamp` fills once the piece's bytes are written.
    pub inline fn writeUnstamped(ring: Ring, pos: u64, len: usize) void {
        @as(*align(1) Lead, @ptrCast(ring.data + ring.offset(pos))).* = .{ .flags = .{ .start = true, .end = true }, .frag_len = @intCast(len) };
    }

    /// Stamps the frame at `pos` (docs/protocol.md §5.2): its position, complemented, in its `total_len`, released
    /// after its header and bytes, so that a reader that sees the stamp sees the whole frame. A zeroed ring holds no
    /// stamp, since a position reaches 2^63 bytes only after centuries.
    pub inline fn stamp(ring: Ring, pos: u64) void {
        @atomicStore(u64, ring.stampSlot(pos), ~pos, .release);
    }

    /// Whether the frame at `pos` carries its stamp: a message of one piece, written whole.
    pub inline fn stamped(ring: Ring, pos: u64) bool {
        return @atomicLoad(u64, ring.stampSlot(pos), .acquire) == ~pos;
    }

    /// The `total_len` of the frame at `pos`, aligned to 8 bytes whatever `pos` is: a corrupt peer may move the index
    /// off a frame's boundary, and the word there is still in the mapping, as the header read there is (`fits`).
    inline fn stampSlot(ring: Ring, pos: u64) *u64 {
        const at = (ring.offset(pos) & ~@as(u32, 7)) + @offsetOf(FrameHeader, "total_len");
        return @ptrCast(@alignCast(ring.data + at));
    }
};

/// A frame header's first 8 bytes, which are written apart from `total_len`.
const Lead = extern struct { flags: wire_protocol.FrameFlags, frag_len: u32 };

/// The frames that carry a stamp are shorter than this (wire_protocol.zig).
pub const stamped_frame_max = wire_protocol.stamped_frame_max;

test "a frame is its header and its piece, padded to 16 bytes" {
    try std.testing.expectEqual(@as(u64, 16), frameLen(0));
    try std.testing.expectEqual(@as(u64, 32), frameLen(1));
    try std.testing.expectEqual(@as(u64, 32), frameLen(16));
    try std.testing.expectEqual(@as(u64, 48), frameLen(17));
    try std.testing.expectEqual(@as(u64, 1040), frameLen(1024));
    try std.testing.expectEqual(@as(u64, 16 + 0xFFFF_FFF0), frameLen(0xFFFF_FFF0));
}

test "a frame fits only if it ends before the ring's end" {
    var indices = std.mem.zeroes(wire_protocol.RingIndices);
    var data: [64]u8 = @splat(0);
    const ring: Ring = .{ .indices = &indices, .capacity = 64, .data = &data, .is_s2c = true };
    const header = struct {
        fn of(frag_len: u32) FrameHeader {
            return .{ .flags = .{ .start = true, .end = true }, .frag_len = frag_len, .total_len = frag_len };
        }
    }.of;
    try std.testing.expect(ring.fits(0, header(48)) and ring.fits(48, header(0)) and ring.fits(64 * 3 + 32, header(16)));
    try std.testing.expect(!ring.fits(0, header(49)) and !ring.fits(48, header(1)) and !ring.fits(8, header(40)));
    try std.testing.expect(!ring.fits(0, header(std.math.maxInt(u32))));
}

test "a ring comes from the view: the server writes s2c and reads c2s, with the view's capacity" {
    var header = std.mem.zeroes(wire_protocol.SegmentHeader);
    header.capacity = 1 << 20; // never read
    var data: [128]u8 = @splat(0);
    var hot = std.mem.zeroes(Hot);
    hot.stream.header.store(&header, .monotonic);
    hot.stream.data.store(&data, .monotonic);
    hot.view_capacity.store(64, .monotonic);
    hot.view_server.store(true, .monotonic);
    const send = Ring.forSend(&hot);
    const recv = Ring.forRecv(&hot);
    try std.testing.expect(send.is_s2c and send.indices == &header.s2c and send.data == @as([*]u8, &data) and send.capacity == 64);
    try std.testing.expect(!recv.is_s2c and recv.indices == &header.c2s and recv.data == data[64..].ptr and recv.capacity == 64);
    hot.view_server.store(false, .monotonic);
    try std.testing.expect(Ring.forSend(&hot).indices == &header.c2s and Ring.forRecv(&hot).indices == &header.s2c);
}
