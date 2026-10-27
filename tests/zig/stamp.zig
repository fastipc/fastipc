//! A checked message of a broadcast stream (the tests of broadcast/ and their peers): its sequence number, its length
//! and a checksum of the rest, then bytes of the sequence. Every message is at least `min` bytes.

const std = @import("std");

pub const min = 16;

/// Fills `buf` (at least `min` bytes) as message `seq`.
pub fn fill(buf: []u8, seq: u64) void {
    std.debug.assert(buf.len >= min);
    std.mem.writeInt(u64, buf[0..8], seq, .little);
    std.mem.writeInt(u32, buf[8..12], @intCast(buf.len), .little);
    for (buf[min..], 0..) |*byte, i| byte.* = @truncate(seq *% 31 +% i);
    std.mem.writeInt(u32, buf[12..16], std.hash.Crc32.hash(buf[min..]), .little);
}

/// The sequence number of a whole message; null for a torn or foreign one.
pub fn check(msg: []const u8) ?u64 {
    if (msg.len < min or std.mem.readInt(u32, msg[8..12], .little) != msg.len) return null;
    if (std.mem.readInt(u32, msg[12..16], .little) != std.hash.Crc32.hash(msg[min..])) return null;
    return std.mem.readInt(u64, msg[0..8], .little);
}

/// Message `seq`'s length, from `min` to `max` bytes, skewed small.
pub fn sizeOf(seq: u64, max: usize) usize {
    const hash = std.hash.Wyhash.hash(0, std.mem.asBytes(&seq));
    const limit = if (hash & 7 == 0) max else @min(max, 256);
    return min + @as(usize, @intCast((hash >> 8) % (limit - min + 1)));
}

/// What a subscriber checks of the stream it receives: every message whole; one after the other, but right after an
/// OVERRUN (a lossy subscriber's); the count and the last one.
pub const Tally = struct {
    received: u64 = 0,
    overruns: u64 = 0,
    bad: u64 = 0,
    last: ?u64 = null,
    after_overrun: bool = false,

    /// A message received, its sequence number or null if it wasn't whole.
    pub fn message(tally: *Tally, seq: ?u64) void {
        const number = seq orelse {
            tally.bad += 1;
            return;
        };
        if (tally.last) |last| {
            if (number <= last or (number != last + 1 and !tally.after_overrun)) tally.bad += 1;
        } else if (number != 0 and !tally.after_overrun and tally.overruns == 0) {
            // A subscriber that joined before the first send starts at message 0
            tally.bad += 1;
        }
        tally.last = number;
        tally.received += 1;
        tally.after_overrun = false;
    }

    pub fn overrun(tally: *Tally) void {
        tally.overruns += 1;
        tally.after_overrun = true;
    }

    /// The stream of `count` messages, sent after the subscriber joined, came whole: every message for a subscriber
    /// that never overran.
    pub fn complete(tally: *const Tally, count: u64) bool {
        if (tally.bad != 0 or (tally.last orelse return count == 0) != count - 1) return false;
        return tally.overruns != 0 or tally.received == count;
    }
};
