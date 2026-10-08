//! One direction of a session's segment: its indices in the header, its capacity and its data bytes, and the frames in
//! it. Head and tail are monotonic byte counts; a position's offset in the data is `pos % capacity` (capacities are
//! powers of two).
//!
//! A frame is a 16-byte header (`FragHdr`) and a piece of a message, padded to a multiple of 16 bytes
//! (`frameLen`), and it never wraps: a writer whose next frame doesn't fit before the ring's end fills the end with a
//! PAD frame first. So every header and every piece lies in one piece of memory, copies are single copies, and a
//! piece's bytes are 16-byte aligned (the data starts at offset 384 of a page-aligned mapping).
//!
//! A ring is built from the endpoint's view, never from the header: its capacity is the local one, checked once at
//! attach. The view is the endpoint's hot line (`Hot`), which is why this file imports lifecycle/endpoint.zig.

const std = @import("std");
const abi = @import("../abi.zig");
const Hot = @import("../lifecycle/endpoint.zig").Hot;

pub const FragHdr = abi.FragHdr;
pub const frag_hdr_len = @sizeOf(FragHdr);

/// The bytes a frame of a piece of `piece_len` bytes takes in the ring: its header and the piece, padded to
/// `abi.frame_align`.
pub fn frameLen(piece_len: u64) u64 {
    return std.mem.alignForward(u64, frag_hdr_len + piece_len, abi.frame_align);
}

pub const Ring = struct {
    ctrl: *abi.RingCtrl,
    cap: u32,
    data: [*]u8,
    /// The server-to-client direction (selects the Windows events).
    is_s2c: bool,

    /// The direction this side writes: s2c for the server, c2s for the client. The caller has seen the view's
    /// number non-zero (.acquire), so the view's fields are the session's.
    pub fn forSend(hot: *const Hot) Ring {
        return direction(hot, hot.view_server.load(.monotonic) != 0);
    }

    /// The direction this side reads: c2s for the server, s2c for the client.
    pub fn forRecv(hot: *const Hot) Ring {
        return direction(hot, hot.view_server.load(.monotonic) == 0);
    }

    fn direction(hot: *const Hot, s2c: bool) Ring {
        const header = hot.view_header.load(.monotonic).?;
        const data = hot.view_data.load(.monotonic).?;
        const cap = hot.view_cap.load(.monotonic);
        return if (s2c)
            .{ .ctrl = &header.s2c, .cap = cap, .data = data, .is_s2c = true }
        else
            .{ .ctrl = &header.c2s, .cap = cap, .data = data + cap, .is_s2c = false };
    }

    /// Bytes published by the writer.
    pub fn head(r: Ring) u64 {
        return r.ctrl.writer.head.load(.acquire);
    }

    /// Bytes consumed by the reader.
    pub fn tail(r: Ring) u64 {
        return r.ctrl.reader.tail.load(.acquire);
    }

    /// Offset of `pos` in the data.
    pub fn offset(r: Ring, pos: u64) u32 {
        return @truncate(pos & (r.cap - 1));
    }

    /// The longest piece: `fipc_max_piece`, the capacity less the framing's share.
    pub fn maxPiece(r: Ring) usize {
        return r.cap - abi.piece_overhead;
    }

    /// Whether the frame `hdr` heads at `pos` ends before the ring's end, as every frame must. The header itself was
    /// read at `pos` before it could be checked: at most 15 bytes past the ring's end if a corrupt peer moved the
    /// index off a frame's boundary, which is still the mapping (the other ring, or the segment's last page, follows).
    pub fn fits(r: Ring, pos: u64, hdr: FragHdr) bool {
        return @as(u64, r.offset(pos)) + frameLen(hdr.frag_len) <= r.cap;
    }

    /// The piece's bytes, or a part of them, at `pos`: `len` bytes that the caller has checked lie in the ring.
    pub inline fn bytes(r: Ring, pos: u64, len: usize) []u8 {
        return r.data[r.offset(pos)..][0..len];
    }

    pub inline fn readHeader(r: Ring, pos: u64) FragHdr {
        return @as(*align(1) const FragHdr, @ptrCast(r.data + r.offset(pos))).*;
    }

    /// Stored field by field: a header assembled in memory and then copied as bytes stalls the copy's loads on the
    /// stores before them.
    pub inline fn writeHeader(r: Ring, pos: u64, hdr: FragHdr) void {
        @as(*align(1) FragHdr, @ptrCast(r.data + r.offset(pos))).* = hdr;
    }
};

/// Copies a piece of a message into or out of the ring. A piece of 64 bytes or more goes through `copyLong`'s 32-byte
/// vectors (AVX2: x86-64-v3 is the platform's floor), whichever `memcpy` the library links: compiler-rt's, which both
/// libraries link, copies RPC's 64-byte and 1 KiB payloads 10-12% slower than glibc's on Linux. A shorter piece goes
/// through `@memcpy`: a few overlapping loads and stores in any `memcpy`, and a call keeps the calling loops small.
pub inline fn copy(dst: []u8, src: []const u8) void {
    if (src.len >= 64) return copyLong(dst.ptr, src.ptr, src.len);
    @memcpy(dst, src);
}

/// At least 64 bytes: the first 32, then 32-byte blocks from the destination's first 32-byte boundary on, four at a
/// time, and the last 32 (each overlapping its neighbour). Aligned stores: a store that splits a cache line costs more
/// than a load that does, and a piece in the ring lies 16 bytes past a 32-byte boundary as often as not.
noinline fn copyLong(d: [*]u8, s: [*]const u8, n: usize) void {
    store(d, load(s));
    var i: usize = 32 - @intFromPtr(d) % 32;
    while (i + 128 <= n) : (i += 128) {
        const a = load(s + i);
        const b = load(s + i + 32);
        const c = load(s + i + 64);
        const e = load(s + i + 96);
        store(d + i, a);
        store(d + i + 32, b);
        store(d + i + 64, c);
        store(d + i + 96, e);
    }
    while (i + 32 < n) : (i += 32) store(d + i, load(s + i));
    store(d + n - 32, load(s + n - 32));
}

const Block = @Vector(32, u8);

inline fn load(p: [*]const u8) Block {
    return @as(*align(1) const Block, @ptrCast(p)).*;
}

inline fn store(p: [*]u8, v: Block) void {
    @as(*align(1) Block, @ptrCast(p)).* = v;
}

test "a copy of any length copies every byte" {
    var src: [9000]u8 = undefined;
    for (&src, 0..) |*byte, i| byte.* = @truncate(i *% 31);
    for (0..600) |len| for (0..3) |at| try expectCopies(&src, len, at);
    for ([_]usize{ 4095, 4096, 4097, 4096 + 127, 9000 }) |len| try expectCopies(&src, len, 1);
}

/// A copy of `len` bytes to `at` bytes past a 32-byte boundary leaves the bytes around it alone.
fn expectCopies(src: []const u8, len: usize, at: usize) !void {
    var dst: [9040]u8 align(32) = @splat(0);
    copy(dst[32 + at ..][0..len], src[0..len]);
    try std.testing.expectEqualSlices(u8, src[0..len], dst[32 + at ..][0..len]);
    try std.testing.expect(std.mem.allEqual(u8, dst[0 .. 32 + at], 0) and std.mem.allEqual(u8, dst[32 + at + len ..], 0));
}

test "a frame is its header and its piece, padded to 16 bytes" {
    try std.testing.expectEqual(@as(u64, 16), frameLen(0));
    try std.testing.expectEqual(@as(u64, 32), frameLen(1));
    try std.testing.expectEqual(@as(u64, 32), frameLen(16));
    try std.testing.expectEqual(@as(u64, 48), frameLen(17));
    try std.testing.expectEqual(@as(u64, 1040), frameLen(1024));
    try std.testing.expectEqual(@as(u64, 16 + 0xFFFF_FFF0), frameLen(0xFFFF_FFF0));
}

test "a frame fits only if it ends before the ring's end" {
    var ctrl = std.mem.zeroes(abi.RingCtrl);
    var data: [64]u8 = @splat(0);
    const r: Ring = .{ .ctrl = &ctrl, .cap = 64, .data = &data, .is_s2c = true };
    const hdr = struct {
        fn of(frag_len: u32) FragHdr {
            return .{ .flags = abi.flag_start | abi.flag_end, .frag_len = frag_len, .total_len = frag_len };
        }
    }.of;
    try std.testing.expect(r.fits(0, hdr(48)) and r.fits(48, hdr(0)) and r.fits(64 * 3 + 32, hdr(16)));
    try std.testing.expect(!r.fits(0, hdr(49)) and !r.fits(48, hdr(1)) and !r.fits(8, hdr(40)));
    try std.testing.expect(!r.fits(0, hdr(std.math.maxInt(u32))));
}

test "a ring comes from the view: the server writes s2c and reads c2s, with the view's capacity" {
    var ctrl = std.mem.zeroes(abi.SegmentHeader);
    ctrl.capacity = 1 << 20; // never read
    var data: [128]u8 = @splat(0);
    var hot = std.mem.zeroes(Hot);
    hot.view_header.store(&ctrl, .monotonic);
    hot.view_data.store(&data, .monotonic);
    hot.view_cap.store(64, .monotonic);
    hot.view_server.store(1, .monotonic);
    const send = Ring.forSend(&hot);
    const recv = Ring.forRecv(&hot);
    try std.testing.expect(send.is_s2c and send.ctrl == &ctrl.s2c and send.data == @as([*]u8, &data) and send.cap == 64);
    try std.testing.expect(!recv.is_s2c and recv.ctrl == &ctrl.c2s and recv.data == data[64..].ptr and recv.cap == 64);
    hot.view_server.store(0, .monotonic);
    try std.testing.expect(Ring.forSend(&hot).ctrl == &ctrl.c2s and Ring.forRecv(&hot).ctrl == &ctrl.s2c);
}
