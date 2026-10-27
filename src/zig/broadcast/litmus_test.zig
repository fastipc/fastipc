//! Litmus tests of the broadcast channel's orderings (broadcast/writer.zig, broadcast/reader.zig), on core/litmus.zig's
//! harness: each races the library's two halves of one pattern, round after round, and counts the forbidden outcomes,
//! which must be none. Each also races a weaker variant (orderings the library doesn't use) that must show the
//! forbidden outcome on this machine: if it doesn't, the zero says nothing here and the test is skipped. On x86-64 the
//! store-buffering cases (sleeps, the room's sleep, the guard) have informative weak variants; the message-passing
//! cases (the cell seqlock, the frontier) need a weakly ordered machine, an aarch64 one, to say anything.

const std = @import("std");
const testing = std.testing;
const wire_protocol = @import("../wire_protocol.zig");
const reader_module = @import("reader.zig");
const segment = @import("segment.zig");
const writer_module = @import("writer.zig");
const endpoint = @import("../core/endpoint.zig");
const fence = @import("../core/fence.zig");
const harness = @import("writer_reader_test.zig");
const litmus = @import("../core/litmus.zig");

const Atomic = std.atomic.Value;
const Endpoint = endpoint.Endpoint;

const race = litmus.race;
const expectNoneLost = litmus.expectNoneLost;

/// A channel with one subscriber in slot 0 (broadcast/writer_reader_test.zig's harness: on Windows with its slot wake),
/// and what the cases touch: the owner's view, the writer, the subscriber's receive state, their endpoints.
const Rig = struct {
    channel: *harness.Channel,
    subscriber: *harness.Subscriber,
    layout: segment.Layout,
    writer: *writer_module.Writer,
    reader: *reader_module.Reader,
    writer_endpoint: *Endpoint,
    reader_endpoint: *Endpoint,

    fn create(gated: bool) !Rig {
        const channel = try harness.Channel.create(4096, 1, -1);
        errdefer channel.destroy();
        const subscriber = try channel.subscribe(0, gated);
        return .{
            .channel = channel,
            .subscriber = subscriber,
            .layout = channel.layout,
            .writer = &channel.writer,
            .reader = &subscriber.reader,
            .writer_endpoint = channel.writer_endpoint,
            .reader_endpoint = subscriber.endpoint,
        };
    }

    fn destroy(rig: *Rig) void {
        rig.subscriber.destroy();
        rig.channel.destroy();
    }
};

// === 1. A subscriber's sleep against the writer's publish (store buffering) ===

/// a: the writer sends a message. b: the subscriber, with nothing to read, announces its sleep. Lost: it decided to
/// sleep, and its armed bit is still there once the send is done: no take, so no wake, for that message.
const SleepCase = struct {
    rig: *Rig,
    weak: bool,
    sleeping: bool = false,

    pub fn reset(case: *SleepCase) void {
        const shared = case.rig.layout.shared;
        shared.armed[0].store(0, .monotonic);
        shared.armed_any.store(0, .monotonic);
        case.rig.reader.pos = case.rig.writer.next;
        case.sleeping = false;
    }

    pub fn a(case: *SleepCase) void {
        if (!case.weak) return case.rig.writer.send(case.rig.writer_endpoint, "litmus", 0) catch unreachable;
        // A `.release` store of `next`, then an `.acquire` look at `armed_any`
        const writer = case.rig.writer;
        writer.next += 1;
        case.rig.layout.header.next.store(writer.next, .release);
        if (case.rig.layout.shared.armed_any.load(.acquire) != 0) {
            _ = case.rig.layout.shared.armed_any.swap(0, .acq_rel);
            _ = case.rig.layout.shared.armed[0].swap(0, .acq_rel);
        }
    }

    pub fn b(case: *SleepCase) void {
        if (!case.weak) {
            case.sleeping = (case.rig.reader.announceSleep(case.rig.reader_endpoint) catch unreachable) != null;
            return;
        }
        // Plain `.release` stores of the armed bits, then an `.acquire` look at `next`
        const shared = case.rig.layout.shared;
        shared.armed[0].store(1, .release);
        shared.armed_any.store(1, .release);
        case.sleeping = case.rig.layout.header.next.load(.acquire) <= case.rig.reader.pos;
    }

    pub fn lost(case: *SleepCase) bool {
        return case.sleeping and case.rig.layout.shared.armed[0].load(.monotonic) != 0;
    }
};

test "litmus: a subscriber's sleep against the writer's publish: no wake is lost" {
    var rig = try Rig.create(false);
    defer rig.destroy();
    var library: SleepCase = .{ .rig = &rig, .weak = false };
    const strong = race(&library);
    var weak: SleepCase = .{ .rig = &rig, .weak = true };
    try expectNoneLost("sleep vs publish", strong, race(&weak));
}

// === 2. The writer's sleep for room against a gated subscriber's cursor (store buffering) ===

/// a: the gated subscriber, about to sleep, publishes its cursor past the writer's need. b: the writer announces its
/// sleep for room. Lost: the writer decided to sleep and its flag is still set once the subscriber is done. The weaker
/// pair is the subscriber's per-receive publish against a plain flag store.
const RoomCase = struct {
    rig: *Rig,
    weak: bool,
    sleeping: bool = false,
    const need = 5;

    pub fn reset(case: *RoomCase) void {
        case.rig.layout.slots[0].cursor.store(need - 1, .monotonic);
        case.rig.layout.shared.writer_sleeps.store(0, .monotonic);
        case.rig.reader.pos = need;
        case.sleeping = false;
    }

    pub fn a(case: *RoomCase) void {
        if (!case.weak) {
            _ = case.rig.reader.announceSleep(case.rig.reader_endpoint) catch unreachable;
            return;
        }
        // The cursor's per-receive publish: a `.release` store, then a `.monotonic` look at the flag (the race the
        // writer's growing slices bound)
        case.rig.layout.slots[0].cursor.store(need, .release);
        if (case.rig.layout.shared.writer_sleeps.load(.monotonic) != 0) segment.wakeWriter(&case.rig.layout);
    }

    pub fn b(case: *RoomCase) void {
        if (!case.weak) {
            case.sleeping = case.rig.writer.announceRoomSleep(case.rig.writer_endpoint, 0, need) == .sleep;
            return;
        }
        // A `.release` store of the flag, then an `.acquire` look at the cursor
        case.rig.layout.shared.writer_sleeps.store(1, .release);
        case.sleeping = case.rig.layout.slots[0].cursor.load(.acquire) < need;
    }

    pub fn lost(case: *RoomCase) bool {
        return case.sleeping and case.rig.layout.shared.writer_sleeps.load(.monotonic) == 1;
    }
};

test "litmus: the writer's sleep for room against a gated subscriber's cursor before its sleep: no wake is lost" {
    var rig = try Rig.create(true);
    defer rig.destroy();
    rig.writer.next = 10; // the need below it
    var library: RoomCase = .{ .rig = &rig, .weak = false };
    const strong = race(&library);
    var weak: RoomCase = .{ .rig = &rig, .weak = true };
    try expectNoneLost("room sleep vs cursor", strong, race(&weak));
}

// === 3. The cell seqlock (message passing) ===

/// a: the writer rewrites cell 0 for the message a lap later. b: a subscriber at message 0 reads the cell under the
/// seqlock. Lost: it accepted fields of the new message as message 0's.
const CellCase = struct {
    rig: *Rig,
    weak: bool,
    accepted: ?reader_module.Piece = null,
    const old: reader_module.Piece = .{ .payload_position = 0, .len = 100 };
    const new: reader_module.Piece = .{ .payload_position = 4096, .len = 200 };

    pub fn reset(case: *CellCase) void {
        writer_module.writeCell(case.rig.layout.cell(0), 0, old.payload_position, old.len);
        case.accepted = null;
    }

    pub fn a(case: *CellCase) void {
        const cell = case.rig.layout.cell(0);
        const lap = case.rig.layout.cellCount();
        if (!case.weak) return writer_module.writeCell(cell, lap, new.payload_position, new.len);
        cell.seq.store((lap + 1) | wire_protocol.broadcast_busy, .monotonic);
        cell.payload_position.store(new.payload_position, .monotonic);
        cell.len.store(new.len, .monotonic);
        cell.seq.store(lap + 1, .release);
    }

    pub fn b(case: *CellCase) void {
        const cell = case.rig.layout.cell(0);
        if (cell.seq.load(.acquire) != 1) return;
        if (!case.weak) {
            case.accepted = reader_module.readCell(cell, 1);
            return;
        }
        const piece: reader_module.Piece = .{ .payload_position = cell.payload_position.load(.monotonic), .len = cell.len.load(.monotonic) };
        if (cell.seq.load(.monotonic) == 1) case.accepted = piece;
    }

    pub fn lost(case: *CellCase) bool {
        const piece = case.accepted orelse return false;
        return piece.payload_position != old.payload_position or piece.len != old.len;
    }
};

test "litmus: the cell seqlock: a reader never accepts fields of the cell's next message" {
    var rig = try Rig.create(false);
    defer rig.destroy();
    var library: CellCase = .{ .rig = &rig, .weak = false };
    const strong = race(&library);
    var weak: CellCase = .{ .rig = &rig, .weak = true };
    try expectNoneLost("cell seqlock", strong, race(&weak));
}

// === 4. The frontier against the payload (message passing) ===

/// The writer's frontier and a payload, modelled with atomics (the library's copy is a plain one): a: the writer
/// announces a frontier a ring later, then overwrites the payload. b: a reader copies the payload, then checks the
/// frontier. Lost: it accepted a copy with bytes of the new payload.
const FrontierCase = struct {
    weak: bool,
    frontier: Atomic(u64) align(64) = .init(0),
    payload: [8]Atomic(u64) align(64) = @splat(.init(0)),
    accepted_new: bool = false,
    const capacity = 4096;

    pub fn reset(case: *FrontierCase) void {
        case.frontier.store(capacity, .monotonic);
        for (&case.payload) |*word| word.store(1, .monotonic);
        case.accepted_new = false;
    }

    pub fn a(case: *FrontierCase) void {
        case.frontier.store(2 * capacity, .monotonic);
        if (!case.weak) fence.storeFence();
        for (&case.payload) |*word| word.store(2, .monotonic);
    }

    pub fn b(case: *FrontierCase) void {
        var saw_new = false;
        for (&case.payload) |*word| saw_new = saw_new or word.load(.monotonic) == 2;
        if (!case.weak) fence.loadFence();
        if (case.frontier.load(.monotonic) -% 0 > capacity) return; // overrun: the copy is dropped
        case.accepted_new = saw_new;
    }

    pub fn lost(case: *FrontierCase) bool {
        return case.accepted_new;
    }
};

test "litmus: the frontier: a reader never accepts a copy the writer overwrote meanwhile" {
    var library: FrontierCase = .{ .weak = false };
    const strong = race(&library);
    var weak: FrontierCase = .{ .weak = true };
    try expectNoneLost("frontier", strong, race(&weak));
}

// === 5. The segment guard against the drop (store buffering) ===

/// a: the subscriber's watcher drops it and looks at `in_segment`. b: a receive enters the segment guard. Lost: the
/// receive entered while the watcher saw it outside, and would revoke the mapping under it.
const GuardCase = struct {
    rig: *Rig,
    weak: bool,
    entered: bool = false,
    may_revoke: bool = false,

    pub fn reset(case: *GuardCase) void {
        case.rig.reader_endpoint.hot.phase.store(.connected, .monotonic);
        case.rig.reader.in_segment.store(0, .monotonic);
        case.entered = false;
        case.may_revoke = false;
    }

    pub fn a(case: *GuardCase) void {
        if (!case.weak) {
            case.may_revoke = case.rig.reader.drop(case.rig.reader_endpoint, .unsubscribed);
            return;
        }
        case.rig.reader_endpoint.hot.phase.store(.unsubscribed, .release);
        case.may_revoke = case.rig.reader.in_segment.load(.acquire) == 0;
    }

    pub fn b(case: *GuardCase) void {
        if (!case.weak) {
            case.entered = if (case.rig.reader.enterSegment(case.rig.reader_endpoint)) true else |_| false;
            return;
        }
        case.rig.reader.in_segment.store(1, .release);
        case.entered = case.rig.reader_endpoint.hot.phase.load(.acquire) != .unsubscribed;
    }

    pub fn lost(case: *GuardCase) bool {
        return case.entered and case.may_revoke;
    }
};

test "litmus: the segment guard against the drop: never a revoke while a receive is inside" {
    var rig = try Rig.create(true);
    defer rig.destroy();
    var library: GuardCase = .{ .rig = &rig, .weak = false };
    const strong = race(&library);
    var weak: GuardCase = .{ .rig = &rig, .weak = true };
    try expectNoneLost("guard vs drop", strong, race(&weak));
}
