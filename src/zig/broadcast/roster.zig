//! The owner's record of a broadcast's slots (`Roster`): the slot table every channel owner keeps (core/roster.zig),
//! plus which slots belong to gated subscribers. It is process-local memory, written under the channel lock
//! (server.zig) and read by the writer through atomics (writer.zig). So a bit that a corrupt subscriber sets in the
//! segment's bitmaps can't make the writer wait for a slot nobody holds, or wake through a slot wake it doesn't have.

const std = @import("std");
const wire_protocol = @import("../wire_protocol.zig");
const platform = @import("../platform.zig");
const core_roster = @import("../core/roster.zig");
const segment = @import("segment.zig");

const SlotBits = segment.SlotBits;
const setBit = segment.setBit;
const clearBit = segment.clearBit;

/// The slots (`slots`: claimed, held, released and retired as core/roster.zig says), and the gated mark of each held
/// one (`holdGated`, before the subscriber's gated bit is set in the segment, writer.zig `startSlot`), which a slot
/// loses whenever it is let go.
pub const Roster = struct {
    slots: Slots = .{},
    /// Bit s: slot s is a gated subscriber's, whom the writer waits for.
    gated: SlotBits = @splat(.init(0)),

    pub const Slots = core_roster.Roster(wire_protocol.max_channel_subscribers);
    pub const Claim = core_roster.Claim;

    pub fn claim(roster: *const Roster) ?Claim {
        return roster.slots.claim();
    }

    pub fn hold(roster: *Roster, slot: u32, generation: u32) void {
        roster.slots.hold(slot, generation);
    }

    /// Slot `slot`'s subscriber joined as a gated one.
    pub fn holdGated(roster: *Roster, slot: u32) void {
        setBit(&roster.gated, slot, .release);
    }

    /// Lets slot `slot` go if it is held under `generation`, its gated mark first: true; false (nothing changes)
    /// otherwise.
    pub fn release(roster: *Roster, slot: u32, generation: u32) bool {
        if (!roster.slots.holds(slot, generation)) return false;
        clearBit(&roster.gated, slot, .release);
        return roster.slots.release(slot, generation);
    }

    /// Retires slot `slot` if it is held under `generation`, as `release`: it is never claimed again.
    pub fn retire(roster: *Roster, slot: u32, generation: u32) bool {
        if (!roster.slots.holds(slot, generation)) return false;
        clearBit(&roster.gated, slot, .release);
        return roster.slots.retire(slot, generation);
    }

    /// Slot `slot` is held no more, nor gated: the writer neither waits for it nor wakes it. Its wake stays.
    pub fn free(roster: *Roster, slot: u32) void {
        clearBit(&roster.gated, slot, .release);
        roster.slots.free(slot);
    }
};

// === Tests ===

const testing = std.testing;

test "the roster: a gated slot's mark goes with its release or its retirement, and only under its generation" {
    var roster: Roster = .{ .slots = .{ .slot_count = 2 } };
    if (platform.wakes_each_slot) roster.slots.wakes = @splat(@as(platform.SlotWake, @ptrFromInt(0x10)));
    roster.hold(0, 1);
    roster.holdGated(0);
    try testing.expect(!roster.retire(0, 2)); // not its generation
    try testing.expectEqual(@as(u64, 1), roster.gated[0].load(.monotonic));
    try testing.expect(roster.retire(0, 1));
    try testing.expectEqual(@as(u64, 0), roster.slots.held[0].load(.monotonic) | roster.gated[0].load(.monotonic));
    try testing.expect(!roster.release(0, 1) and !roster.retire(0, 1)); // nothing names it again
    const claim = roster.claim().?;
    try testing.expectEqual(@as(u32, 1), claim.slot);
    roster.hold(claim.slot, claim.generation);
    roster.holdGated(claim.slot);
    try testing.expect(!roster.release(1, claim.generation + 1));
    try testing.expectEqual(@as(u64, 0b10), roster.gated[0].load(.monotonic));
    try testing.expect(roster.release(1, claim.generation));
    try testing.expectEqual(@as(u64, 0), roster.slots.held[0].load(.monotonic) | roster.gated[0].load(.monotonic));
}
