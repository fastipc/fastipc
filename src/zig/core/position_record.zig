//! A writer's own record of where each message's payload starts in its data ring: what its room is computed from,
//! instead of the cells' `payload_position`, which a peer that maps the segment writable could change. A broadcast
//! channel's writer, each producer of a work queue and each client of a service keep one, in their own process's
//! memory (a client's receiving thread finds each reply there too).
//!
//! It keeps the low 32 bits of each position, one entry per cell (a power of two), at `index % entries`: an entry stays
//! until the message a lap of cells later replaces it. A writer writes data only below the position of its oldest
//! message still unread plus the ring's capacity, so every payload whose entry is still there starts at most a ring
//! (at most 2^31 bytes) below the end of the last payload, and its distance below that end fits the 32 bits kept.

const std = @import("std");

pub const PositionRecord = struct {
    /// One per cell: a power of two.
    entries: []u32,

    /// Message `index`'s payload starts at `position`.
    pub fn record(positions: PositionRecord, index: u64, position: u64) void {
        positions.entries[positions.slot(index)] = @truncate(position);
    }

    /// Message `index`'s payload position, for an index that no later message has replaced (at most a lap of cells
    /// below `next`, the next message's index); at or past `next`, `end`, the end of the last payload.
    pub fn positionOf(positions: PositionRecord, index: u64, next: u64, end: u64) u64 {
        if (index >= next) return end;
        const low = positions.entries[positions.slot(index)];
        return end - (@as(u32, @truncate(end)) -% low);
    }

    /// Message `index`'s payload position modulo 2^32, for an index that no later message has replaced: where it is in
    /// a data ring (of at most 2^31 bytes, so its capacity divides 2^32), without the end of the last payload.
    pub fn lowOf(positions: PositionRecord, index: u64) u32 {
        return positions.entries[positions.slot(index)];
    }

    fn slot(positions: PositionRecord, index: u64) usize {
        return @intCast(index & (positions.entries.len - 1));
    }
};

test "positions come back whole from their low 32 bits, across a multiple of 2^32 and the ring's laps" {
    var entries: [4]u32 = undefined;
    const positions: PositionRecord = .{ .entries = &entries };
    // A 2 GiB ring past several laps: the payloads straddle 2^33
    const capacity: u64 = 1 << 31;
    var payloads: [6]u64 = undefined;
    var end: u64 = 3 * capacity - 4096;
    for (&payloads, 0..) |*at, index| {
        at.* = end;
        positions.record(index, at.*);
        end += if (index % 2 == 0) 4096 else 1 << 30;
    }
    // Only the last lap of cells is kept: indices 2 to 5
    for (2..6) |index| try std.testing.expectEqual(payloads[index], positions.positionOf(index, 6, end));
    try std.testing.expectEqual(end, positions.positionOf(6, 6, end));
    try std.testing.expectEqual(end, positions.positionOf(9, 6, end));
}

test "a skip to the ring's start leaves a gap the record keeps" {
    var entries: [8]u32 = undefined;
    const positions: PositionRecord = .{ .entries = &entries };
    const capacity: u64 = 4096;
    // 1024-byte payloads, then one that doesn't fit before the end skips to the next lap's start
    positions.record(0, 0);
    positions.record(1, 1024);
    positions.record(2, 2048);
    positions.record(3, capacity); // skipped 1024 bytes
    const end = capacity + 2048;
    try std.testing.expectEqual(@as(u64, 2048), positions.positionOf(2, 4, end));
    try std.testing.expectEqual(capacity, positions.positionOf(3, 4, end));
    try std.testing.expectEqual(@as(u64, 0), positions.positionOf(0, 4, end));
}
