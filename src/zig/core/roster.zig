//! A channel owner's record of its slots (`Roster`): process-local memory, written under the owner's lock, whose
//! atomics the owner's data path may read (a broadcast channel's writer, broadcast/writer.zig), so that a bit a corrupt
//! member sets in the segment can't make the owner wait for a slot nobody holds, or wake through a wake it doesn't
//! have. A broadcast channel keeps one for its subscribers' slots (broadcast/roster.zig), a work queue one per role
//! (core/members.zig).

const std = @import("std");
const platform = @import("../platform.zig");

const Atomic = std.atomic.Value;

/// A slot the owner picked (`Roster.claim`): its index, and the generation `hold` gives it.
pub const Claim = struct { slot: u32, generation: u32 };

/// The record of up to `max_slots` slots (a multiple of 64). The owner picks a slot (`claim`) and holds it once its
/// wake is there (`hold`, before it tells the joiner), and lets it go when the member is gone (`release`), or retires
/// it for good where the member may still write it (`retire`). A slot's generation grows with every claim, so that a
/// frame or a report naming an older member of the slot is told apart from the current one. Slots are claimed round
/// robin: a slot just freed is the last to be claimed again.
pub fn Roster(comptime max_slots: u32) type {
    std.debug.assert(max_slots % 64 == 0);
    return struct {
        const Self = @This();

        /// One bit per slot, in words of 64.
        pub const Bits = [max_slots / 64]Atomic(u64);

        /// Slots `0..slot_count` exist.
        slot_count: u32 = max_slots,
        /// Bit s: slot s is held.
        held: Bits = @splat(.init(0)),
        /// Each slot's generation, as it was held last (0: never).
        generations: [max_slots]Atomic(u32) = @splat(.init(0)),
        /// Each slot's wake (where `platform.wakes_each_slot`): created for the slot's first member, before the slot
        /// is first held, and kept for its later ones until the channel's teardown, so that the owner can set the wake
        /// of any slot it saw held, whenever it acts on that.
        wakes: [max_slots]?platform.SlotWake = @splat(null),
        /// Bit s: slot s is retired (the owner's only).
        retired: std.bit_set.Static(max_slots) = .empty,
        /// Where the next claim starts looking (the owner's only).
        next: u32 = 0,

        /// The first slot neither held nor retired from the one after the last held, and the generation `hold` gives
        /// it; null if there is none.
        pub fn claim(roster: *const Self) ?Claim {
            for (0..roster.slot_count) |offset| {
                const slot: u32 = @intCast((roster.next + offset) % roster.slot_count);
                if (roster.isHeld(slot) or roster.retired.isSet(slot)) continue;
                return .{ .slot = slot, .generation = roster.generations[slot].load(.monotonic) +% 1 };
            }
            return null;
        }

        /// Slot `slot`, of generation `generation`, is held; where `wakes_each_slot` its wake is set already.
        pub fn hold(roster: *Self, slot: u32, generation: u32) void {
            if (platform.wakes_each_slot) std.debug.assert(roster.wakes[slot] != null);
            roster.generations[slot].store(generation, .monotonic);
            roster.next = (slot + 1) % roster.slot_count;
            _ = roster.held[slot / 64].fetchOr(bitOf(slot), .release);
        }

        /// Lets slot `slot` go if it is held under `generation`: true; false (nothing changes) for a slot not held,
        /// or held by a later claim.
        pub fn release(roster: *Self, slot: u32, generation: u32) bool {
            if (!roster.holds(slot, generation)) return false;
            roster.free(slot);
            return true;
        }

        /// Retires slot `slot` if it is held under `generation`, as `release`: it is never claimed again.
        pub fn retire(roster: *Self, slot: u32, generation: u32) bool {
            if (!roster.holds(slot, generation)) return false;
            roster.retired.set(slot);
            roster.free(slot);
            return true;
        }

        /// Whether slot `slot` is held under `generation`.
        pub fn holds(roster: *const Self, slot: u32, generation: u32) bool {
            return slot < roster.slot_count and roster.isHeld(slot) and roster.generations[slot].load(.monotonic) == generation;
        }

        pub fn isHeld(roster: *const Self, slot: u32) bool {
            return roster.held[slot / 64].load(.monotonic) & bitOf(slot) != 0;
        }

        /// Slot `slot` is held no more. Its wake stays.
        pub fn free(roster: *Self, slot: u32) void {
            _ = roster.held[slot / 64].fetchAnd(~bitOf(slot), .release);
        }

        /// Sets the wake of each slot of `bits`, word `word_index` of a bitmap, that has one.
        pub fn setWakes(roster: *const Self, word_index: usize, bits: u64) void {
            var rest = bits;
            while (rest != 0) : (rest &= rest - 1) {
                if (roster.wakes[word_index * 64 + @ctz(rest)]) |wake| platform.setSlotWake(wake);
            }
        }

        /// Closes every slot's wake: the channel's teardown.
        pub fn closeWakes(roster: *Self) void {
            if (!platform.wakes_each_slot) return;
            for (&roster.wakes) |*wake| if (wake.*) |handle| {
                platform.closeSlotWake(handle);
                wake.* = null;
            };
        }
    };
}

fn bitOf(slot: u32) u64 {
    return @as(u64, 1) << @intCast(slot % 64);
}

// === Tests ===

const testing = std.testing;

/// A roster of `slot_count` slots on its own, without wakes: where each slot has one, a placeholder that nothing
/// sets, for `hold`'s check.
fn rosterOf(comptime max_slots: u32, slot_count: u32) Roster(max_slots) {
    var roster: Roster(max_slots) = .{ .slot_count = slot_count };
    if (platform.wakes_each_slot) roster.wakes = @splat(@as(platform.SlotWake, @ptrFromInt(0x10)));
    return roster;
}

test "the roster: slots claimed round robin until full, let go only under their generation, which grows" {
    var roster = rosterOf(1024, 3);
    for (0..3) |i| {
        const claim = roster.claim().?;
        try testing.expectEqual(Claim{ .slot = @intCast(i), .generation = 1 }, claim);
        roster.hold(claim.slot, claim.generation);
    }
    try testing.expect(roster.claim() == null);
    try testing.expect(!roster.release(1, 2)); // a later generation's: not this one
    try testing.expect(!roster.release(5, 1)); // out of range
    try testing.expect(roster.release(1, 1));
    try testing.expect(!roster.release(1, 1)); // free already
    try testing.expectEqual(@as(u64, 0b101), roster.held[0].load(.monotonic));

    const again = roster.claim().?;
    try testing.expectEqual(Claim{ .slot = 1, .generation = 2 }, again);
    roster.hold(again.slot, again.generation);
    try testing.expect(!roster.release(1, 1)); // the slot's former member
    try testing.expect(roster.release(1, 2));

    // Round robin: the slot after the last held comes first, a slot just freed last
    try testing.expect(roster.release(0, 1));
    try testing.expectEqual(@as(u32, 0), roster.claim().?.slot); // after slot 1: 2 is held, so 0
    roster.hold(0, 2);
    try testing.expect(roster.release(2, 1));
    try testing.expectEqual(@as(u32, 1), roster.claim().?.slot);

    // A full roster of 1024, and one of 64
    var full = rosterOf(1024, 1024);
    for (0..1024) |i| {
        const claim = full.claim().?;
        try testing.expectEqual(@as(u32, @intCast(i)), claim.slot);
        full.hold(claim.slot, claim.generation);
    }
    try testing.expect(full.claim() == null);
    try testing.expect(full.release(700, 1));
    try testing.expectEqual(Claim{ .slot = 700, .generation = 2 }, full.claim().?);
    var small = rosterOf(64, 64);
    for (0..64) |_| {
        const claim = small.claim().?;
        small.hold(claim.slot, claim.generation);
    }
    try testing.expect(small.claim() == null);
    try testing.expectEqual(~@as(u64, 0), small.held[0].load(.monotonic));
}

test "the roster: a retired slot is held by nobody, and never claimed again" {
    var roster = rosterOf(64, 2);
    roster.hold(0, 1);
    try testing.expect(!roster.retire(0, 2)); // not its generation
    try testing.expect(roster.retire(0, 1));
    try testing.expectEqual(@as(u64, 0), roster.held[0].load(.monotonic));
    try testing.expect(!roster.release(0, 1) and !roster.retire(0, 1)); // nothing names it again
    for (0..4) |_| {
        const claim = roster.claim().?;
        try testing.expectEqual(@as(u32, 1), claim.slot);
        roster.hold(claim.slot, claim.generation);
        try testing.expect(roster.claim() == null);
        try testing.expect(roster.release(claim.slot, claim.generation));
    }
}
