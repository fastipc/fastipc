//! Litmus tests of the work queue's orderings (core/sender.zig, core/taker.zig, queue/returns.zig), on
//! core/litmus.zig's harness: each races the library's two halves of one pattern, round after round, and counts the
//! forbidden outcomes, which must be none. Each also races a weaker variant (orderings the library doesn't use) that
//! must show the forbidden outcome on this machine: if it doesn't, the zero says nothing here and the test is skipped.
//! On x86-64 the claim's compare-and-swap and the store-buffering cases (the two Dekker handshakes, the end's pair)
//! have informative weak variants; the message-passing cases (a payload published, then claimed; read, then reused
//! once done; the same through the owner's return) need a weakly ordered machine, an aarch64 one, to say anything.

const std = @import("std");
const testing = std.testing;
const returns_module = @import("returns.zig");
const ring_set = @import("../core/ring_set.zig");
const segment = @import("segment.zig");
const harness = @import("producer_consumer_test.zig");
const litmus = @import("../core/litmus.zig");

const Atomic = std.atomic.Value;
const race = litmus.race;
const expectNoneLost = litmus.expectNoneLost;

/// A queue of one ring with its producer and two consumers (producer_consumer_test.zig's harness: on Windows with each
/// slot's event).
const Rig = struct {
    queue: *harness.Queue,
    producer: *harness.ProducerEnd,
    consumers: [2]*harness.ConsumerEnd,

    fn create() !Rig {
        const queue = try harness.Queue.create(4096, 1, 2);
        errdefer queue.destroy();
        const producer = try queue.producer(0);
        errdefer producer.destroy();
        const first = try queue.consumer(0);
        errdefer first.destroy();
        return .{ .queue = queue, .producer = producer, .consumers = .{ first, try queue.consumer(1) } };
    }

    fn destroy(rig: *Rig) void {
        for (rig.consumers) |consumer| consumer.destroy();
        rig.producer.destroy();
        rig.queue.destroy();
    }

    fn layout(rig: *const Rig) *const segment.Layout {
        return &rig.queue.layout;
    }

    /// Takes every message left, so that the next round starts on an empty ring with room.
    fn drain(rig: *Rig) void {
        var buf: [1024]u8 = undefined;
        while (true) _ = rig.consumers[0].recv(&buf, 0) catch return;
    }

    /// The ring has a message to take, but its ready bit is clear: no consumer would look.
    fn stranded(rig: *const Rig) bool {
        const ring = rig.layout().ring(0);
        return ring.tail.load(.seq_cst) > ring.head.load(.seq_cst) and rig.layout().header.ready.load(.seq_cst) & 1 == 0;
    }
};

// === 1. The claim (compare-and-swap) ===

/// a and b: a consumer each takes the message published at head. Lost: both took it, or neither.
const ClaimCase = struct {
    rig: *Rig,
    weak: bool,
    index: u64 = 0,
    took: [2]bool = .{ false, false },

    pub fn reset(case: *ClaimCase) void {
        const layout = case.rig.layout();
        for (case.rig.consumers) |consumer| consumer.consumer.held = null;
        case.index += 1;
        const cell = layout.cell(0, case.index);
        cell.payload_position.store(case.index % layout.cellCount() * 64, .monotonic);
        cell.len.store(8, .monotonic);
        cell.state.store(ring_set.published(case.index), .release);
        layout.ring(0).head.store(case.index, .seq_cst);
        layout.ring(0).tail.store(case.index + 1, .seq_cst);
        layout.header.ready.store(1, .seq_cst);
        case.took = .{ false, false };
    }

    fn take(case: *ClaimCase, consumer: u32) bool {
        if (!case.weak) return if (case.rig.consumers[consumer].recvAcquire(0)) |_| true else |_| false;
        // A look, then a plain store: not one atomic step
        const cell = case.rig.layout().cell(0, case.index);
        if (cell.state.load(.acquire) != ring_set.published(case.index)) return false;
        cell.state.store(ring_set.claimed(case.index, consumer), .release);
        return true;
    }

    pub fn a(case: *ClaimCase) void {
        case.took[0] = case.take(0);
    }

    pub fn b(case: *ClaimCase) void {
        case.took[1] = case.take(1);
    }

    pub fn lost(case: *ClaimCase) bool {
        return case.took[0] == case.took[1];
    }
};

test "litmus: the claim: of two consumers racing for one message, exactly one takes it" {
    var rig = try Rig.create();
    defer rig.destroy();
    var library: ClaimCase = .{ .rig = &rig, .weak = false };
    const strong = race(&library);
    var weak: ClaimCase = .{ .rig = &rig, .weak = true };
    try expectNoneLost("claim", strong, race(&weak));
}

// === 2. A publish against a consumer's clear of the ready bit (store buffering) ===

/// a: the producer publishes onto its ring, whose ready bit is set. b: a consumer that found the ring empty clears the
/// bit and looks again. Lost: a message left with its bit clear, which no consumer looks for. The weaker pair: a
/// `.release` store of `tail`, then an `.acquire` look at the bit; a plain store of the bit, then an `.acquire` look at
/// `tail`.
const ReadyCase = struct {
    rig: *Rig,
    weak: bool,

    pub fn reset(case: *ReadyCase) void {
        const layout = case.rig.layout();
        if (case.weak) {
            layout.ring(0).tail.store(0, .monotonic);
            layout.ring(0).head.store(0, .monotonic);
        } else case.rig.drain();
        layout.header.ready.store(1, .seq_cst);
    }

    pub fn a(case: *ReadyCase) void {
        if (!case.weak) return case.rig.producer.send("litmus", 0) catch unreachable;
        const layout = case.rig.layout();
        layout.ring(0).tail.store(1, .release);
        if (layout.header.ready.load(.acquire) & 1 == 0) _ = layout.header.ready.fetchOr(1, .seq_cst);
    }

    pub fn b(case: *ReadyCase) void {
        if (!case.weak) {
            _ = case.rig.consumers[0].consumer.taker.clearReady(0);
            return;
        }
        const layout = case.rig.layout();
        layout.header.ready.store(0, .monotonic);
        if (layout.ring(0).tail.load(.acquire) > layout.ring(0).head.load(.acquire)) _ = layout.header.ready.fetchOr(1, .seq_cst);
    }

    pub fn lost(case: *ReadyCase) bool {
        return case.rig.stranded();
    }
};

test "litmus: a publish against a consumer's clear of the ready bit: no message is left without its bit" {
    var rig = try Rig.create();
    defer rig.destroy();
    var library: ReadyCase = .{ .rig = &rig, .weak = false };
    const strong = race(&library);
    var weak_rig = try Rig.create();
    defer weak_rig.destroy();
    var weak: ReadyCase = .{ .rig = &weak_rig, .weak = true };
    try expectNoneLost("publish vs clear of ready", strong, race(&weak));
}

// === 3. A consumer's park against a publish's wake (store buffering) ===

/// a: the producer publishes onto its empty ring: its ready bit, then a wake of an idle consumer. b: a consumer with
/// nothing to take announces its park. Lost: it decided to sleep, and its flag is still set once the publish is done:
/// no wake. The weaker pair: plain stores of the bits and the flag, each followed by an `.acquire` look.
const ParkCase = struct {
    rig: *Rig,
    weak: bool,
    sleeping: bool = false,

    pub fn reset(case: *ParkCase) void {
        const layout = case.rig.layout();
        if (!case.weak) case.rig.drain();
        layout.header.ready.store(0, .seq_cst);
        layout.header.idle.store(0, .seq_cst);
        layout.takerSlot(0).sleeps.store(0, .seq_cst);
        case.sleeping = false;
    }

    pub fn a(case: *ParkCase) void {
        if (!case.weak) return case.rig.producer.send("litmus", 0) catch unreachable;
        const layout = case.rig.layout();
        layout.header.ready.store(1, .release);
        if (layout.header.idle.load(.acquire) & 1 != 0) layout.takerSlot(0).sleeps.store(0, .release);
    }

    pub fn b(case: *ParkCase) void {
        if (!case.weak) {
            const end = case.rig.consumers[0];
            case.sleeping = end.consumer.taker.announcePark(end.endpoint) == .sleep;
            return;
        }
        const layout = case.rig.layout();
        layout.takerSlot(0).sleeps.store(1, .release);
        layout.header.idle.store(1, .release);
        case.sleeping = layout.header.ready.load(.acquire) == 0;
    }

    pub fn lost(case: *ParkCase) bool {
        return case.sleeping and case.rig.layout().takerSlot(0).sleeps.load(.seq_cst) == 1;
    }
};

test "litmus: a consumer's park against a publish's wake: no wake is lost" {
    var rig = try Rig.create();
    defer rig.destroy();
    var library: ParkCase = .{ .rig = &rig, .weak = false };
    const strong = race(&library);
    var weak_rig = try Rig.create();
    defer weak_rig.destroy();
    var weak: ParkCase = .{ .rig = &weak_rig, .weak = true };
    try expectNoneLost("park vs publish's wake", strong, race(&weak));
}

// === 4. The end against a send (store buffering) ===

/// a: the producer sends. b: the queue's end is marked, and then a consumer receives what is left. Lost: the send
/// returned OK, and the consumer didn't get the message. The weaker pair: `.release` stores of `tail` and of the mark,
/// each followed by an `.acquire` look at the other.
const EndCase = struct {
    rig: *Rig,
    weak: bool,
    sent: bool = false,
    got: bool = false,

    pub fn reset(case: *EndCase) void {
        const layout = case.rig.layout();
        layout.header.ended.store(0, .seq_cst);
        if (case.weak) {
            layout.ring(0).tail.store(0, .monotonic);
        } else {
            case.rig.consumers[0].consumer.taker.draining = false;
            case.rig.drain();
        }
        case.sent = false;
        case.got = false;
    }

    pub fn a(case: *EndCase) void {
        if (!case.weak) {
            case.sent = if (case.rig.producer.send("litmus", 0)) true else |_| false;
            return;
        }
        const layout = case.rig.layout();
        layout.ring(0).tail.store(1, .release);
        case.sent = layout.header.ended.load(.acquire) == 0;
    }

    pub fn b(case: *EndCase) void {
        const layout = case.rig.layout();
        if (!case.weak) {
            layout.header.ended.store(1, .seq_cst);
            var buf: [64]u8 = undefined;
            case.got = if (case.rig.consumers[0].recv(&buf, 0)) |_| true else |_| false;
            return;
        }
        layout.header.ended.store(1, .release);
        case.got = layout.ring(0).tail.load(.acquire) > 0;
    }

    pub fn lost(case: *EndCase) bool {
        return case.sent and !case.got;
    }
};

test "litmus: the end against a send: a send that returned OK is drained by the consumer that saw the end" {
    var rig = try Rig.create();
    defer rig.destroy();
    var library: EndCase = .{ .rig = &rig, .weak = false };
    const strong = race(&library);
    var weak_rig = try Rig.create();
    defer weak_rig.destroy();
    var weak: EndCase = .{ .rig = &weak_rig, .weak = true };
    try expectNoneLost("end vs send", strong, race(&weak));
}

// === 5. A publish, then a claim (message passing) ===

/// a: the producer sends a message of its round's bytes. b: a consumer receives. Lost: it took the message with bytes
/// of an earlier one. The weaker pair, modelled with atomics: the payload's stores, then a `.monotonic` store of the
/// state; a `.monotonic` compare-and-swap, then the payload's loads.
const PublishCase = struct {
    rig: *Rig,
    weak: bool,
    round: u8 = 0,
    torn: bool = false,
    payload: [8]Atomic(u64) align(64) = @splat(.init(0)),
    state: Atomic(u64) align(64) = .init(0),

    pub fn reset(case: *PublishCase) void {
        if (!case.weak) case.rig.drain();
        case.round +%= 1;
        case.state.store(0, .monotonic);
        case.torn = false;
    }

    pub fn a(case: *PublishCase) void {
        if (!case.weak) {
            const bytes: [256]u8 = @splat(case.round);
            return case.rig.producer.send(&bytes, 0) catch unreachable;
        }
        for (&case.payload) |*word| word.store(case.round, .monotonic);
        case.state.store(ring_set.published(case.round), .monotonic);
    }

    pub fn b(case: *PublishCase) void {
        if (!case.weak) {
            var buf: [256]u8 = undefined;
            const msg = case.rig.consumers[0].recv(&buf, 0) catch return;
            case.torn = !std.mem.allEqual(u8, msg, case.round);
            return;
        }
        if (case.state.cmpxchgStrong(ring_set.published(case.round), ring_set.claimed(case.round, 0), .monotonic, .monotonic) != null) return;
        for (&case.payload) |*word| case.torn = case.torn or word.load(.monotonic) != case.round;
    }

    pub fn lost(case: *PublishCase) bool {
        return case.torn;
    }
};

test "litmus: a publish, then a claim: the claimer sees the whole payload" {
    var rig = try Rig.create();
    defer rig.destroy();
    var library: PublishCase = .{ .rig = &rig, .weak = false };
    const strong = race(&library);
    var weak: PublishCase = .{ .rig = &rig, .weak = true };
    try expectNoneLost("publish vs claim", strong, race(&weak));
}

// === 6. A read, then a done, then the producer's reuse (message passing) ===

/// The ring is full of 64-byte messages, so that the producer's next one takes the room of the message at head. a: a
/// consumer receives the message at head (its copy, then its done). b: the producer sends the next one, which
/// overwrites that payload once it sees the done. Lost: the consumer's copy has bytes of the new message. The weaker
/// pair, modelled with atomics: the copy's loads, then a `.monotonic` done; a `.monotonic` look at it, then the
/// overwrite.
const ReuseCase = struct {
    rig: *Rig,
    weak: bool,
    /// The library's: the message at head this round, and whether the producer's next one went out in the round.
    head: u64 = 0,
    sent_next: bool = true,
    torn: bool = false,
    /// The weaker pair's model.
    round: u64 = 0,
    payload: [8]Atomic(u64) align(64) = @splat(.init(0)),
    done: Atomic(u64) align(64) = .init(0),

    fn message(index: u64) [64]u8 {
        var bytes: [64]u8 = @splat(@truncate(index *% 7 +% 1));
        std.mem.writeInt(u64, bytes[0..8], index, .little);
        return bytes;
    }

    /// The library's start: the ring full, its oldest message 0.
    fn fill(case: *ReuseCase) !void {
        for (0..case.rig.layout().cellCount()) |index| try case.rig.producer.send(&message(index), 0);
    }

    pub fn reset(case: *ReuseCase) void {
        if (case.weak) {
            case.round += 1;
            for (&case.payload) |*word| word.store(case.round, .monotonic);
            case.done.store(0, .monotonic);
        } else if (!case.sent_next) {
            // The last round's send came before the done: its message now, so that the ring is full again
            case.rig.producer.send(&message(case.head - 1 + case.rig.layout().cellCount()), 0) catch unreachable;
        }
        case.sent_next = false;
        case.torn = false;
    }

    pub fn a(case: *ReuseCase) void {
        if (!case.weak) {
            var buf: [64]u8 = undefined;
            const msg = case.rig.consumers[0].recv(&buf, 0) catch unreachable;
            case.torn = !std.mem.eql(u8, msg, &message(case.head));
            return;
        }
        for (&case.payload) |*word| case.torn = case.torn or word.load(.monotonic) != case.round;
        case.done.store(1, .monotonic);
    }

    pub fn b(case: *ReuseCase) void {
        if (!case.weak) {
            case.sent_next = if (case.rig.producer.send(&message(case.head + case.rig.layout().cellCount()), 0)) true else |_| false;
            return;
        }
        if (case.done.load(.monotonic) == 0) return;
        for (&case.payload) |*word| word.store(case.round + 1000, .monotonic);
    }

    pub fn lost(case: *ReuseCase) bool {
        if (!case.weak) case.head += 1;
        return case.torn;
    }
};

test "litmus: a read, then a done, then the producer's reuse: the consumer's copy is never overwritten" {
    var rig = try Rig.create();
    defer rig.destroy();
    var library: ReuseCase = .{ .rig = &rig, .weak = false };
    try library.fill();
    const strong = race(&library);
    var weak: ReuseCase = .{ .rig = &rig, .weak = true };
    try expectNoneLost("done vs reuse", strong, race(&weak));
}

// === 7. A return, then a claim (message passing through the owner) ===

/// The rig's returns, on the owner's view (returns.zig).
fn returnsOf(rig: *Rig) !returns_module.Returns {
    return .init(testing.allocator, rig.queue.layout, rig.queue.session.id);
}

/// Consumer 1 takes the next message in place and ends: its claim stays in the segment, nothing released.
fn endHolding(rig: *Rig) void {
    _ = rig.consumers[1].recvAcquire(0) catch unreachable;
    rig.consumers[1].consumer.held = null;
}

/// Each round consumer 1 ends holding a message of the round's bytes. a: the owner returns it (its copy onto the
/// return ring, its publish, then the original's done). b: consumer 0 receives. Lost: it took the returned message
/// with other bytes. The weaker pair is the publish case's (5): the same message passing.
const ReturnCase = struct {
    rig: *Rig,
    returns: *returns_module.Returns,
    round: u8 = 0,
    torn: bool = false,

    pub fn reset(case: *ReturnCase) void {
        case.rig.drain();
        case.round +%= 1;
        const bytes: [256]u8 = @splat(case.round);
        case.rig.producer.send(&bytes, 0) catch unreachable;
        endHolding(case.rig);
        case.torn = false;
    }

    pub fn a(case: *ReturnCase) void {
        std.debug.assert(case.returns.recover(1) == .free);
    }

    pub fn b(case: *ReturnCase) void {
        var buf: [256]u8 = undefined;
        const msg = case.rig.consumers[0].recv(&buf, 0) catch return;
        case.torn = !std.mem.allEqual(u8, msg, case.round);
    }

    pub fn lost(case: *ReturnCase) bool {
        return case.torn;
    }
};

test "litmus: a return, then a claim: the consumer of a returned message sees the whole payload" {
    var rig = try Rig.create();
    defer rig.destroy();
    var returns = try returnsOf(&rig);
    defer returns.deinit(testing.allocator);
    var library: ReturnCase = .{ .rig = &rig, .returns = &returns };
    const strong = race(&library);
    var weak: PublishCase = .{ .rig = &rig, .weak = true };
    try expectNoneLost("return vs claim", strong, race(&weak));
}

// === 8. A return's copy, then the original's done, then the producer's reuse (message passing) ===

/// The ring is full of 64-byte messages, its oldest held by consumer 1, which ended. a: the owner returns it (its
/// copy, its publish, then the original's done). b: the producer sends the next one, which overwrites the original's
/// payload once it sees the done. Lost: the returned copy has bytes of the new message. The weaker pair is the reuse
/// case's (6).
const ReturnReuseCase = struct {
    rig: *Rig,
    returns: *returns_module.Returns,
    /// The message held this round, and whether the producer's next one went out in the round.
    head: u64 = 0,
    sent_next: bool = true,

    fn fill(case: *ReturnReuseCase) !void {
        for (0..case.rig.layout().cellCount()) |index| try case.rig.producer.send(&ReuseCase.message(index), 0);
    }

    pub fn reset(case: *ReturnReuseCase) void {
        const cells = case.rig.layout().cellCount();
        // The last round's send came before the done: its message now, so that the ring is full again
        if (!case.sent_next) case.rig.producer.send(&ReuseCase.message(case.head - 1 + cells), 0) catch unreachable;
        case.sent_next = false;
        endHolding(case.rig);
    }

    pub fn a(case: *ReturnReuseCase) void {
        std.debug.assert(case.returns.recover(1) == .free);
    }

    pub fn b(case: *ReturnReuseCase) void {
        case.sent_next = if (case.rig.producer.send(&ReuseCase.message(case.head + case.rig.layout().cellCount()), 0)) true else |_| false;
    }

    /// Reads the returned copy, then takes it off the return ring as a consumer would (done, `head` past it).
    pub fn lost(case: *ReturnReuseCase) bool {
        const layout = case.rig.layout();
        const back = segment.returnRing(layout);
        const index = case.returns.producer.next - 1;
        const cell = layout.cell(back, index);
        const copied = layout.bytes(back, cell.payload_position.load(.monotonic), cell.len.load(.monotonic));
        const torn = !std.mem.eql(u8, copied, &ReuseCase.message(case.head));
        cell.state.store(ring_set.done(index, 0), .release);
        layout.ring(back).head.store(index + 1, .seq_cst);
        case.head += 1;
        return torn;
    }
};

test "litmus: a return's copy, then the original's done, then the producer's reuse: the copy is never overwritten" {
    var rig = try Rig.create();
    defer rig.destroy();
    var returns = try returnsOf(&rig);
    defer returns.deinit(testing.allocator);
    var library: ReturnReuseCase = .{ .rig = &rig, .returns = &returns };
    try library.fill();
    const strong = race(&library);
    var weak: ReuseCase = .{ .rig = &rig, .weak = true };
    try expectNoneLost("return vs reuse", strong, race(&weak));
}
