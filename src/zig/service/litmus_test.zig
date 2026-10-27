//! Litmus tests of the service's orderings (core/ring_set.zig `answer` and `abandonHeld`, caller.zig, responder.zig),
//! on core/litmus.zig's harness: each races the library's two halves of one pattern, round after round, and counts the
//! forbidden outcomes, which must be none. Each also races a weaker variant (orderings the library doesn't use) that
//! must show the forbidden outcome on this machine: if it doesn't, the zero says nothing here and the test is skipped.
//! On x86-64 the store-buffering cases (the reply's wake, the end's pair) have informative weak variants; the
//! message-passing case (an abandon's fields, then its done) needs a weakly ordered machine, an aarch64 one, to say
//! anything. The claim, the ready bits and the park are the work queue's, whose litmus tests cover them
//! (queue/litmus_test.zig).

const std = @import("std");
const testing = std.testing;
const wire_protocol = @import("../wire_protocol.zig");
const ring_set = @import("../core/ring_set.zig");
const segment = @import("segment.zig");
const harness = @import("caller_responder_test.zig");
const litmus = @import("../core/litmus.zig");

const Atomic = std.atomic.Value;
const Rings = segment.Rings;
const race = litmus.race;
const expectNoneLost = litmus.expectNoneLost;

/// A service of one client and one server (caller_responder_test.zig's harness: on Windows with each slot's events).
const Rig = struct {
    service: *harness.Service,
    client: *harness.ClientEnd,
    server: *harness.ServerEnd,

    fn create() !Rig {
        const service = try harness.Service.create(4096, 64, 1, 1);
        errdefer service.destroy();
        const client = try service.client(0);
        errdefer client.destroy();
        return .{ .service = service, .client = client, .server = try service.server(0) };
    }

    fn destroy(rig: *Rig) void {
        rig.server.destroy();
        rig.client.destroy();
        rig.service.destroy();
    }

    fn layout(rig: *const Rig) *const segment.Layout {
        return &rig.service.layout;
    }

    /// The service goes on (no mark, and the client's receives as a new client's, which no Disconnected ended), every
    /// answer the client is owed comes, and it consumes them; then it sends a new request, which the server takes and
    /// holds: the round's request, the client's next.
    fn freshRequest(rig: *Rig) void {
        rig.layout().header.ended.store(0, .seq_cst);
        rig.client.caller.replies.disconnected = false;
        var buf: [64]u8 = undefined;
        if (rig.server.responder.held.request() != null) rig.server.reply("old") catch unreachable;
        while (rig.client.caller.replies.next.load(.seq_cst) != rig.client.caller.sender.next) {
            _ = rig.client.recv(&buf, 0) catch |err| switch (err) {
                error.Abandoned => {},
                else => unreachable,
            };
        }
        rig.client.send("litmus", 0) catch unreachable;
        _ = rig.server.recv(&buf, 0) catch unreachable;
    }

    /// The round's request: the client's next.
    fn index(rig: *const Rig) u64 {
        return rig.client.caller.replies.next.load(.seq_cst);
    }
};

// === 1. A reply's done against its client's sleep (store buffering) ===

/// a: the server replies to the request it holds: its done, then the look at the client's `reply_sleeps` (swapped
/// and woken if it read 1). b: the client, with no answer, announces its sleep. Lost: it decided to sleep, and its
/// flag is still set once the reply is done: no wake. The weaker pair: a `.release` store of the done, then a relaxed
/// look at the flag; a plain store of the flag, then an `.acquire` look at the state.
const ReplyCase = struct {
    rig: *Rig,
    weak: bool,
    sleeping: bool = false,

    pub fn reset(case: *ReplyCase) void {
        const layout = case.rig.layout();
        if (case.weak) {
            layout.cell(0, 0).state.store(ring_set.claimed(0, 0), .seq_cst);
        } else case.rig.freshRequest();
        layout.ring(0).reply_sleeps.store(0, .seq_cst);
        case.sleeping = false;
    }

    pub fn a(case: *ReplyCase) void {
        if (!case.weak) return case.rig.server.reply("re:litmus") catch unreachable;
        const ring = case.rig.layout().ring(0);
        case.rig.layout().cell(0, 0).state.store(ring_set.done(0, 0), .release);
        if (ring.reply_sleeps.load(.monotonic) == 1) ring.reply_sleeps.store(0, .monotonic);
    }

    pub fn b(case: *ReplyCase) void {
        const layout = case.rig.layout();
        if (!case.weak) {
            const client = case.rig.client;
            const state = &layout.cell(0, case.rig.index()).state;
            case.sleeping = client.caller.replies.announceSleep(client.endpoint, state, case.rig.index()) == .sleep;
            return;
        }
        layout.ring(0).reply_sleeps.store(1, .monotonic);
        case.sleeping = layout.cell(0, 0).state.load(.acquire) != ring_set.done(0, 0);
    }

    pub fn lost(case: *ReplyCase) bool {
        return case.sleeping and case.rig.layout().ring(0).reply_sleeps.load(.seq_cst) == 1;
    }
};

test "litmus: a reply's done against its client's sleep: no reply's wake is lost" {
    var rig = try Rig.create();
    defer rig.destroy();
    var library: ReplyCase = .{ .rig = &rig, .weak = false };
    const strong = race(&library);
    var weak_rig = try Rig.create();
    defer weak_rig.destroy();
    var weak: ReplyCase = .{ .rig = &weak_rig, .weak = true };
    try expectNoneLost("reply vs client's sleep", strong, race(&weak));
}

// === 2. The end against a reply (store buffering) ===

/// a: the server replies. b: the service's end is marked, and then the client receives. Lost: the reply returned OK,
/// and the client didn't get it. The weaker pair: `.release` stores of the done and of the mark, each followed by an
/// `.acquire` look at the other.
const EndCase = struct {
    rig: *Rig,
    weak: bool,
    replied: bool = false,
    got: bool = false,

    pub fn reset(case: *EndCase) void {
        const layout = case.rig.layout();
        if (case.weak) {
            layout.header.ended.store(0, .seq_cst);
            layout.cell(0, 0).state.store(ring_set.claimed(0, 0), .seq_cst);
        } else case.rig.freshRequest();
        case.replied = false;
        case.got = false;
    }

    pub fn a(case: *EndCase) void {
        if (!case.weak) {
            case.replied = if (case.rig.server.reply("re:litmus")) true else |_| false;
            return;
        }
        const layout = case.rig.layout();
        layout.cell(0, 0).state.store(ring_set.done(0, 0), .release);
        case.replied = layout.header.ended.load(.acquire) == 0;
    }

    pub fn b(case: *EndCase) void {
        const layout = case.rig.layout();
        if (!case.weak) {
            layout.header.ended.store(1, .seq_cst);
            var buf: [64]u8 = undefined;
            case.got = if (case.rig.client.recv(&buf, 0)) |_| true else |_| false;
            return;
        }
        layout.header.ended.store(1, .release);
        case.got = layout.cell(0, 0).state.load(.acquire) == ring_set.done(0, 0);
    }

    pub fn lost(case: *EndCase) bool {
        return case.replied and !case.got;
    }
};

test "litmus: the end against a reply: a reply that returned OK is received by the client that saw the end" {
    var rig = try Rig.create();
    defer rig.destroy();
    var library: EndCase = .{ .rig = &rig, .weak = false };
    const strong = race(&library);
    var weak_rig = try Rig.create();
    defer weak_rig.destroy();
    var weak: EndCase = .{ .rig = &weak_rig, .weak = true };
    try expectNoneLost("end vs reply", strong, race(&weak));
}

// === 3. An abandon, then the client's receive (message passing) ===

/// a: the owner abandons the request an ended server held: the answer's fields, then the done. b: the client
/// receives. Lost: it saw the done with the fields of an earlier answer (a reply). The weaker pair, modelled with
/// atomics: the fields' stores, then a `.monotonic` store of the state; a `.monotonic` load of the state, then the
/// fields' loads.
const AbandonCase = struct {
    rig: *Rig,
    weak: bool,
    stale: bool = false,
    outcome: Atomic(u32) align(64) = .init(0),
    state: Atomic(u64) align(64) = .init(0),

    pub fn reset(case: *AbandonCase) void {
        case.stale = false;
        if (case.weak) {
            case.outcome.store(@backingInt(wire_protocol.ServiceOutcome.replied), .monotonic);
            case.state.store(0, .seq_cst);
            return;
        }
        case.rig.freshRequest();
        // The server ended holding it; the cell keeps the fields of the answer before, a reply's
        case.rig.server.responder.held.holding.store(false, .seq_cst);
        const cell = case.rig.layout().cell(0, case.rig.index());
        cell.outcome.store(@backingInt(wire_protocol.ServiceOutcome.replied), .seq_cst);
        cell.reply_len.store(5, .seq_cst);
    }

    pub fn a(case: *AbandonCase) void {
        if (!case.weak) {
            _ = case.rig.service.serverEnded(0);
            return;
        }
        case.outcome.store(@backingInt(wire_protocol.ServiceOutcome.abandoned), .monotonic);
        case.state.store(1, .monotonic);
    }

    pub fn b(case: *AbandonCase) void {
        if (!case.weak) {
            var buf: [64]u8 = undefined;
            // A reply here is the earlier answer's fields under the abandon's done
            case.stale = if (case.rig.client.recv(&buf, 0)) |_| true else |_| false;
            return;
        }
        if (case.state.load(.monotonic) == 1) case.stale = case.outcome.load(.monotonic) != @backingInt(wire_protocol.ServiceOutcome.abandoned);
    }

    pub fn lost(case: *AbandonCase) bool {
        return case.stale;
    }
};

test "litmus: an abandon, then the client's receive: a done abandon is never read as a reply" {
    var rig = try Rig.create();
    defer rig.destroy();
    var library: AbandonCase = .{ .rig = &rig, .weak = false };
    const strong = race(&library);
    var weak_rig = try Rig.create();
    defer weak_rig.destroy();
    var weak: AbandonCase = .{ .rig = &weak_rig, .weak = true };
    try expectNoneLost("abandon vs receive", strong, race(&weak));
}
