//! A data ring of payloads, as a broadcast's writer (broadcast/writer.zig) and a ring set's sender (core/sender.zig, a
//! queue's producer) write it: each payload at a monotonic byte position, after the last one, in a whole number of
//! alignment units, never wrapping (a payload that wouldn't fit before the ring's end starts at the ring's start); a
//! zero-copy reservation for the commit that follows it; and the slices of a wait for room.

const std = @import("std");

/// The first and the longest slice of a wait for room: also the longest a missed wake delays the waiter.
pub const first_slice_ms: u32 = 1;
pub const room_slice_ms: u32 = 10;

/// Where a payload goes: its position, and the position after its room.
pub const Placement = struct { at: u64, end: u64 };

/// Where a payload of `len` bytes goes in a ring of `capacity` bytes (a power of two) whose last payload ended at
/// `payload_end`: there, or at the ring's start where its `alignment`-rounded room wouldn't fit before the end.
pub inline fn place(payload_end: u64, capacity: u64, len: usize, comptime alignment: u64) Placement {
    std.debug.assert(std.math.isPowerOfTwo(capacity));
    const room = std.mem.alignForward(u64, len, alignment);
    var at = payload_end;
    const offset = at & (capacity - 1);
    if (offset + room > capacity) at += capacity - offset;
    return .{ .at = at, .end = at + room };
}

/// The zero-copy reservation of the last `fipc_send_acquire`, for its commit: its position and length (0: none).
pub const Reservation = struct {
    at: u64 = 0,
    len: u64 = 0,

    pub inline fn set(reservation: *Reservation, at: u64, len: usize) void {
        reservation.at = at;
        reservation.len = len;
    }

    pub inline fn drop(reservation: *Reservation) void {
        reservation.len = 0;
    }

    /// The position of the reservation, used up by a commit of `len` bytes. Invalid without one, or for a `len` of 0
    /// or past it (the reservation stays).
    pub inline fn commit(reservation: *Reservation, len: usize) error{Invalid}!u64 {
        if (len == 0 or len > reservation.len) return error.Invalid;
        reservation.len = 0;
        return reservation.at;
    }
};

test "a payload goes after the last one, or at the ring's start where it wouldn't fit before the end" {
    try std.testing.expectEqual(Placement{ .at = 0, .end = 64 }, place(0, 256, 1, 64));
    try std.testing.expectEqual(Placement{ .at = 64, .end = 192 }, place(64, 256, 65, 64));
    try std.testing.expectEqual(Placement{ .at = 192, .end = 256 }, place(192, 256, 64, 64));
    try std.testing.expectEqual(Placement{ .at = 256, .end = 384 }, place(192, 256, 65, 64));
    try std.testing.expectEqual(Placement{ .at = 512 + 64, .end = 512 + 128 }, place(512 + 64, 256, 64, 64));
}

test "a reservation is committed once, for 1 to its length" {
    var reservation: Reservation = .{};
    try std.testing.expectError(error.Invalid, reservation.commit(1));
    reservation.set(128, 10);
    try std.testing.expectError(error.Invalid, reservation.commit(0));
    try std.testing.expectError(error.Invalid, reservation.commit(11));
    try std.testing.expectEqual(@as(u64, 128), try reservation.commit(10));
    try std.testing.expectError(error.Invalid, reservation.commit(10));
    reservation.set(64, 10);
    reservation.drop();
    try std.testing.expectError(error.Invalid, reservation.commit(1));
}
