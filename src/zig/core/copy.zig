//! The copy of a piece of a message into or out of shared memory: a stream's ring, a channel's data ring.

const std = @import("std");

/// Copies a piece of a message.
///   - A piece of 64 bytes or more goes through `copyLong`'s 32-byte vectors (AVX2 on x86-64-v3, the platform's
///     floor), whatever `memcpy` the library links. The shared and the static library both link compiler-rt's
///     `memcpy`, which on Linux copies 64-byte and 1 KiB pieces more slowly than glibc's.
///   - A shorter piece goes through `@memcpy`. Any `memcpy` copies it in a few overlapping loads and stores, and a call
///     keeps the calling loops small.
pub inline fn copy(dst: []u8, src: []const u8) void {
    if (src.len >= 64) return copyLong(dst.ptr, src.ptr, src.len);
    @memcpy(dst, src);
}

/// At least 64 bytes: the first 32, then 32-byte blocks from the destination's first 32-byte boundary on, four at a
/// time, and the last 32 (each overlapping its neighbour). Aligned stores: a store that splits a cache line costs more
/// than a load that does, and a piece in the ring lies 16 bytes past a 32-byte boundary as often as not.
noinline fn copyLong(destination: [*]u8, source: [*]const u8, len: usize) void {
    store(destination, load(source));
    var i: usize = 32 - @intFromPtr(destination) % 32;
    while (i + 128 <= len) : (i += 128) {
        const block0 = load(source + i);
        const block1 = load(source + i + 32);
        const block2 = load(source + i + 64);
        const block3 = load(source + i + 96);
        store(destination + i, block0);
        store(destination + i + 32, block1);
        store(destination + i + 64, block2);
        store(destination + i + 96, block3);
    }
    while (i + 32 < len) : (i += 32) store(destination + i, load(source + i));
    store(destination + len - 32, load(source + len - 32));
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
