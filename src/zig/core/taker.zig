//! A ring set's takers (core/ring_set.zig): the pass that takes each message of any ring for one taker, and its park. A
//! work queue's consumers are its takers (queue/consumer.zig, whose receives copy the message out or hold it in place),
//! and so are a service's servers (service/responder.zig, which hold the request until they reply). Everything here
//! works on a mapped ring set (`Layout`) and the taker's endpoint, whose `Io`, `requests` and `phase` it reads; the
//! member's lifecycle sets it up. How senders publish is core/sender.zig.
//!
//! A pass. A take looks at the rings whose `ready` bit is set (every ring after a slice's timeout, and while it
//! drains), in order from the ring it starts at, and the first message it can take ends the pass. A taker keeps to a
//! ring while it wins its claims there at the first look, for up to `stay_limit` claims in a row, then starts at the
//! next ring, so that every ring is visited; after a claim it had to contend for (another taker takes from that ring
//! too) it starts at its home ring, the one its slot maps to. So takers settle on rings of their own, and each ring's
//! `head` and cells stay in one taker's cache instead of moving between the takers at every message (`nextRing`).
//!
//! In a ring it loads `tail`, then `head`, so that `tail - head` is at most a ring of cells whatever happens between
//! the loads (a larger gap is a corrupt peer's). While `head` is below the `tail` it saw last, it reuses that `tail`
//! instead of loading it again, so that `tail`'s cache line moves once per run of messages, not once per message
//! (`takeFrom`). Then it looks at the cell of message `head`:
//!   - `published(head)` is claimed by a compare-and-swap to `claimed(head, slot)`, which only one taker wins (a
//!     queue's only consumer slot's taker stores it, and moves `head` with a store too: `sole`);
//!   - a cell claimed or done is helped: after a pause (`helpPause`), if `head` is still at it (its claimer moves it
//!     next), `head` is moved past it, by compare-and-swap.
//! The claimer stores its intent (its slot's `claim`) before its claim, so that the owner can find the message of a
//! taker that ended holding it (core/ring_set.zig `heldClaim`). Then it moves `head` on itself. Claims follow `head`,
//! which passes a message only once it is claimed, so the messages of one sender are claimed in the order it sent
//! them. A pass treats the rings past the senders' (a queue's return ring) as any other.
//!
//! Ready bits. A taker that found a ring empty, or took its last message, clears the ring's bit and then looks at the
//! ring again: if a message came meanwhile it sets the bit again, and wakes one parked taker unless it takes the
//! message itself (its sender saw the bit set and woke nobody). So does a take that leaves a message it found: one
//! too large for its buffer (TooLarge), or the messages after a corrupt one it skipped (Invalid). With the sender's
//! store of `tail` and load of `ready` it is a Dekker handshake (core/sender.zig), every access sequentially
//! consistent. A claim that leaves messages behind wakes one more parked taker, if any (chaining): only for
//! parallelism, never needed to make progress.
//!
//! Park. A taker with nothing to take sets its slot's `sleeps`, then its `idle` bit, then looks at `ready`, its
//! requests, its phase and `ended`, and sleeps only if none changed: a sender's wake sets a ready bit and then takes an
//! idle bit (core/ring_set.zig `wakeOneIdle`), so either the taker sees the ready bit or the waker sees its idle bit
//! and swaps its flag back. A sleep lasts a slice at most, after which the taker looks at every ring. A pass that keeps
//! finding a ready bit set and nothing to take naps 1 ms a round instead of looping.
//!
//! The end. Where the kind's takers drain (`drains`: a queue's consumers do), the end (`ended`, set before the phase)
//! lets a taker take what is left, in passes over every ring whose loads are sequentially consistent, then
//! Disconnected; elsewhere the end is Disconnected at once. Its own connection's failure (PEER_DEAD without the mark)
//! is Disconnected at once.
//!
//! A corrupt peer can write anything in the segment. Every bitmap is masked to the set's members, a ring's indices
//! are checked (`validRing`: else the ring is skipped and logged once), a cell at `head` that isn't message `head`'s is
//! skipped (Invalid), and a claimed message's payload fields are checked (`validPiece`: else it is dropped, Invalid;
//! where messages carry replies, dropped means answered as abandoned, so that its sender's wait for it ends).
//! Each ring gets at most `take_attempts` rounds per pass, and every wait ends within its slice.

const std = @import("std");
const builtin = @import("builtin");
const build_options = @import("build_options");
const platform = @import("../platform.zig");
const test_hooks = @import("test_hooks.zig");
const fence = @import("fence.zig");
const log = @import("log.zig");
const ring_set = @import("ring_set.zig");
const wait_module = @import("wait.zig");

const Atomic = std.atomic.Value;
const AtomicOrder = std.lang.AtomicOrder;
const Claim = ring_set.Claim;
const Endpoint = @import("endpoint.zig").Endpoint;
const Deadline = wait_module.Deadline;

const DataError = @import("endpoint.zig").DataError;

/// The rounds of one pass at one ring before it goes on to the next: a ring whose messages other takers keep winning
/// is their progress, not this taker's to wait for.
const take_attempts = 64;

/// How long a taker that finds the message at `head` claimed or done pauses before it helps `head` on, at its
/// `round`-th look at the ring in this pass: 1 << min(round, `help_backoff`) hints. On x86 one hint (a `pause`, tens
/// of nanoseconds) mostly lets the claimer move `head` first. On aarch64 a hint is an `isb`, a few nanoseconds on an
/// M1, so a taker that keeps finding `head` claimed waits twice as long at each look, up to 64 hints: with eight
/// consumers on one producer's ring (queue-1x8, 16-64 B) one hint ran at 4-5 M msg/s on the M1 under macOS and 10-12 M
/// under Linux, and this backoff at 22-27 M on both, the losers no longer taking `head`'s line from the claimer.
const help_backoff: u6 = switch (builtin.cpu.arch) {
    .aarch64 => 6,
    else => 0,
};

inline fn helpPause(round: usize) usize {
    return @as(usize, 1) << @intCast(@min(round, help_backoff));
}

/// The most claims in a row a taker makes from one ring, each won at the first look, before its next pass starts at
/// the next ring (`nextRing`): every ring is visited, and a busy ring holds a taker for a bounded run of claims (tens
/// of microseconds on x86, a few hundred on aarch64). Per architecture: each move on displaces the next ring's taker,
/// which then contends and goes home, and on aarch64 (an M1, under macOS and Linux) 4096 measured 1.3 to 1.4 times
/// 256's rate at queue-8x8 and service-8x1-pipelined (16-64 B), and queue-4x4 no lower. On x86, 256 kept nearly all
/// of an unbounded stay's rate at queue-4x4.
pub const stay_limit: u32 = switch (builtin.cpu.arch) {
    .aarch64 => 4096,
    else => 256,
};

/// Whether the servers of a service's pool no larger than its clients pace their spins as a queue's consumers do
/// (`Taker.spinMode`), per architecture. On x86 they do: unpaced, service-4x4 at 1 KiB lost 5-10% (#242). On aarch64
/// they look after every hint, as a lone server does: the pace's top there is `stream_pace` (512 hints, about 4 µs on
/// an M1), and paced servers ran service-4x4 at 0.8-0.9 of the unpaced rate under macOS and a third to a half of it
/// under Linux.
pub const pool_paced = builtin.cpu.arch != .aarch64;

/// What a pass found.
pub const Taken = union(enum) { claimed: Claim, too_large: usize, nothing };

/// What a look at one ring found.
const From = union(enum) { claimed: Claim, too_large: usize, empty, corrupt_ring, contended };

/// A taker of the ring set `Rings` (core/ring_set.zig `RingSet`); `drains`: whether it takes what is left once the
/// kind has ended (the module's comment).
pub fn Taker(comptime Rings: type, comptime drains: bool) type {
    return struct {
        const Self = @This();
        const Layout = Rings.Layout;

        layout: Layout,
        slot: u32,
        /// The ring the next pass starts at (`nextRing`).
        next_ring: u32,
        /// The ring its slot maps to, where a pass starts after a contended claim.
        home: u32,
        /// The claims in a row from `next_ring`, each won at the first look.
        stay: u32 = 0,
        /// The last claim of `takeFrom` was won at its first look at the ring.
        first_look: bool = false,
        /// Each ring's `tail` as this taker loaded it last: every message below it is published (`takeFrom`).
        seen_tails: [Rings.max_rings]u64 = @splat(0),
        /// It saw the kind's end: every pass looks at every ring, with sequentially consistent loads.
        draining: bool = false,
        /// The next pass looks at every ring, whatever the ready bits: after a slice ran out, or a nap.
        full_scan: bool = false,
        /// The last wait ended on its spin's hit: the next wait, if the pass in between took nothing, doesn't spin.
        spun: bool = false,
        /// Rings logged as corrupt, once each.
        warned_rings: u64 = 0,
        /// What wakes this taker's park besides its flag (Windows: its slot's event).
        own_event: platform.RingEvent,
        /// The other slots' wakes, for the takers and senders this taker wakes.
        wakes: Rings.Wakes,
        /// The hints the next spin leaves before its first look (`spin`).
        pace: u32 = 1,
        /// The spin before a park, and the longest park: the defaults, which only tests change.
        spin_iters: u32 = wait_module.spin_iters,
        longest_sleep_ms: u32 = wait_module.slice_ms,
        /// The parks begun, counted in the tests' builds only: a test that sees it move knows the take is past every
        /// look before its sleep, so that only a wake ends the sleep before its time.
        sleeps: Atomic(u32) = .init(0),

        /// The taker of slot `slot`: its first pass starts at the ring its slot maps to, its home, so that takers start
        /// spread over the rings.
        pub fn init(layout: Layout, slot: u32, own_event: platform.RingEvent, session_id: [16]u8) Self {
            std.debug.assert(slot < layout.max_takers);
            const home: u32 = @intCast(@as(u64, slot) * layout.max_senders / layout.max_takers);
            return .{
                .layout = layout,
                .slot = slot,
                .next_ring = home,
                .home = home,
                .own_event = own_event,
                .wakes = .init(session_id),
            };
        }

        /// Closes the wakes it opened. The caller let go of a message it held first.
        pub fn deinit(taker: *Self) void {
            taker.wakes.deinit();
        }

        /// Disconnected after this taker's own failure: its connection ended while the kind goes on (no end's mark,
        /// which its watcher stores before the phase), and at the end where takers don't drain, from the mark on. It
        /// takes nothing more; the other takers do.
        pub fn open(taker: *const Self, endpoint: *const Endpoint) DataError!void {
            if (!drains and taker.layout.header.ended.load(.acquire) != 0) return error.Disconnected;
            if (endpoint.hot.phase.load(.acquire) == .connected) return;
            if (!drains or taker.layout.header.ended.load(.acquire) == 0) return error.Disconnected;
        }

        /// One pass (the module's comment): a message claimed, the length of one too large for `fit` bytes, or
        /// nothing. Invalid for a corrupt message, which it skipped or dropped.
        pub fn take(taker: *Self, fit: usize) DataError!Taken {
            const all = taker.full_scan or taker.draining;
            taker.full_scan = false;
            const header = taker.layout.header;
            const rings = if (all) taker.layout.ringMask() else header.ready.load(.acquire) & taker.layout.ringMask();
            // A ring this pass leaves with a message it doesn't take, and that no other taker may be looking for: one
            // whose bit this pass set again (its sender saw the bit set and woke nobody), one whose next message is too
            // large for this call, or one whose corrupt message it skipped or dropped (that message may have used up
            // its sender's one wake). On every way out, the pass wakes one parked taker for it
            var wake_for: ?u32 = null;
            defer if (wake_for) |ring| Rings.wakeOneIdle(&taker.layout, &taker.wakes, ring);
            var left = rings;
            while (left != 0) {
                const ring = ring_set.firstSetBitFrom(left, taker.next_ring);
                left &= ~ring_set.bitOf(ring);
                var looked_again = false;
                while (true) {
                    const from = (if (taker.draining) taker.takeFrom(ring, fit, .seq_cst) else taker.takeFrom(ring, fit, .acquire)) catch |err| {
                        wake_for = ring;
                        return err;
                    };
                    switch (from) {
                        .claimed => |claim| {
                            if (wake_for == ring) wake_for = null; // `afterClaim` wakes for it
                            taker.nextRing(ring);
                            return .{ .claimed = claim };
                        },
                        .too_large => |found| {
                            wake_for = ring;
                            return .{ .too_large = found };
                        },
                        .empty => if (taker.clearReady(ring)) {
                            wake_for = ring;
                            if (!looked_again) {
                                looked_again = true;
                                continue;
                            }
                        },
                        .corrupt_ring => {
                            taker.warnCorrupt(ring);
                            _ = taker.clearReady(ring);
                        },
                        .contended => {},
                    }
                    break;
                }
            }
            return .nothing;
        }

        /// At most `take_attempts` rounds at ring `ring` (the module's comment), its indices loaded with `order`
        /// (`.seq_cst` while draining: the taker's half of the end's pair).
        fn takeFrom(taker: *Self, ring: u32, fit: usize, comptime order: AtomicOrder) DataError!From {
            const layout = &taker.layout;
            const control = layout.ring(ring);
            for (0..take_attempts) |round| {
                taker.first_look = round == 0;
                // `tail` before `head`: `tail <= reclaimed + cell_count <= head + cell_count` held when it was loaded,
                // and `head` only grows, so a pair that isn't valid is a corrupt peer's, not a delay between the loads.
                // The `tail` this taker saw last will do while `head` is below it (its messages stay published): the
                // line that the sender writes at every publish moves here once per run of messages, not once per
                // message. Draining loads it every time (the end's pair)
                var tail = taker.seen_tails[ring];
                var head = control.head.load(order);
                if (head >= tail or order == .seq_cst) {
                    tail = taker.loadTail(ring, order);
                    head = control.head.load(order);
                }
                if (!ring_set.validRing(head, tail, layout.cellCount())) {
                    taker.seen_tails[ring] = 0;
                    return .corrupt_ring;
                }
                if (head >= tail) return .empty;
                // Message `head` was published before `tail` passed it (this `tail`'s `.acquire` load, in this pass or
                // an earlier one), and its cell isn't reused while `head` is at most its index (core/sender.zig
                // `reclaim`); a stale `head` is harmless: the claim expects message `head`'s exact state, which no
                // later message's equals
                const cell = layout.cell(ring, head);
                const state = cell.state.load(.acquire);
                if (state == ring_set.published(head)) {
                    if (fit < layout.maxPiece()) {
                        // The length before the claim, under the cell's seqlock: only a buffer that may be short looks.
                        // The only taker's message can't be taken meanwhile (`sole`)
                        const len = cell.len.load(.monotonic);
                        if (!taker.sole()) {
                            fence.loadFence();
                            if (cell.state.load(.monotonic) != ring_set.published(head)) continue; // taken meanwhile
                        }
                        if (len > fit and len <= layout.maxPiece()) return .{ .too_large = len };
                    }
                    // The intent before the claim (`.release`: the owner that reads `head` past it sees it)
                    layout.takerSlot(taker.slot).claim.store(ring_set.intent(ring, head), .release);
                    if (taker.sole()) {
                        // The only taker: the claim and the move of `head` are stores (`sole`)
                        cell.state.store(ring_set.claimed(head, taker.slot), .release);
                        if (build_options.test_hooks) test_hooks.crashIfAt(Rings.claimed_step);
                        control.head.store(head + 1, .release);
                    } else {
                        if (cell.state.cmpxchgStrong(ring_set.published(head), ring_set.claimed(head, taker.slot), .acq_rel, .monotonic) != null) continue;
                        if (build_options.test_hooks) test_hooks.crashIfAt(Rings.claimed_step);
                        // Fails only where a helper moved it already
                        _ = control.head.cmpxchgStrong(head, head + 1, .release, .monotonic);
                    }
                    const claim: Claim = .{ .ring = ring, .index = head, .position = cell.payload_position.load(.monotonic), .len = cell.len.load(.monotonic) };
                    if (!layout.validPiece(claim.position, claim.len)) {
                        taker.drop(claim);
                        return error.Invalid;
                    }
                    taker.afterClaim(ring, head, tail);
                    return .{ .claimed = claim };
                }
                if (ring_set.ofMessage(state, head) and (ring_set.phaseOf(state) == .claimed or ring_set.phaseOf(state) == .done)) {
                    // Claimed or done, and `head` not past it yet: its claimer moves `head` next, so a look after a
                    // pause mostly finds it moved. Help it on only if it didn't: takers that all lost a claim to one
                    // winner and then raced it to move `head` took the line from each other for every message
                    for (0..helpPause(round)) |_| std.atomic.spinLoopHint();
                    if (control.head.load(.monotonic) == head) _ = control.head.cmpxchgStrong(head, head + 1, .release, .monotonic);
                    continue;
                }
                if (control.head.load(.acquire) != head) continue; // this `head` was stale
                // `head` is message `head`'s, below `tail`, and its cell isn't: a corrupt peer's. Skip it, so that
                // nobody loops on it
                _ = control.head.cmpxchgStrong(head, head + 1, .release, .monotonic);
                return error.Invalid;
            }
            return .contended;
        }

        /// Where the next pass starts after a claim from `ring` (the module's comment): `ring` again while its claims
        /// are won at the first look, up to `stay_limit` in a row; then the next ring (without a division); after a
        /// contended claim, the taker's home.
        pub fn nextRing(taker: *Self, ring: u32) void {
            if (!taker.first_look) {
                taker.next_ring = taker.home;
                taker.stay = 0;
            } else if (ring == taker.next_ring and taker.stay < stay_limit) {
                taker.stay += 1;
            } else if (ring != taker.next_ring) {
                taker.next_ring = ring;
                taker.stay = 1;
            } else {
                taker.next_ring = if (ring + 1 == taker.layout.ringCount()) 0 else ring + 1;
                taker.stay = 0;
            }
        }

        /// After a claim of message `head` of `ring`, whose pass saw `tail`: the ready bit and wake chaining (the
        /// module's comment).
        fn afterClaim(taker: *Self, ring: u32, head: u64, tail: u64) void {
            if (tail == head + 1 and taker.loadTail(ring, .acquire) == head + 1) {
                // It took the last message (`tail` loaded again: the pass's may be one it saw earlier): a later publish
                // must find the bit clear. Set again (a message came meanwhile, whose sender saw the bit set and woke
                // nobody): this taker is busy, so it wakes one
                if (taker.clearReady(ring)) Rings.wakeOneIdle(&taker.layout, &taker.wakes, ring);
            } else if (taker.layout.header.idle.load(.monotonic) & taker.layout.takerMask() != 0) {
                Rings.wakeOneIdle(&taker.layout, &taker.wakes, ring);
            }
        }

        /// Whether this taker is the set's only one: a queue of one consumer slot. While it holds the slot nobody
        /// else claims a message or moves `head` (the owner writes them for a taker that ended, before it gives the
        /// slot out again), so it claims with a store of the state where takers race with a compare-and-swap, and
        /// moves `head` with a store too: the same words, without two locked instructions per message. Where
        /// messages carry replies (a service's) the owner drops an ended sender's unclaimed messages while the
        /// takers run (core/ring_set.zig `dropUnclaimed`), so a sole server still races it.
        pub inline fn sole(taker: *const Self) bool {
            return !Rings.replies and taker.layout.max_takers == 1;
        }

        /// Ring `ring`'s `tail`, loaded with `order`, and kept as the one this taker saw last.
        fn loadTail(taker: *Self, ring: u32, comptime order: AtomicOrder) u64 {
            const tail = taker.layout.ring(ring).tail.load(order);
            taker.seen_tails[ring] = tail;
            return tail;
        }

        /// Clears `ring`'s ready bit, then looks at the ring again (`tail` first) and sets it again if a message is
        /// there to take: true then. The taker's half of the ready handshake (the module's comment).
        pub fn clearReady(taker: *Self, ring: u32) bool {
            const header = taker.layout.header;
            const bit = ring_set.bitOf(ring);
            _ = header.ready.fetchAnd(~bit, .seq_cst);
            const control = taker.layout.ring(ring);
            const tail = control.tail.load(.seq_cst);
            if (!ring_set.claimable(control.head.load(.seq_cst), tail, taker.layout.cellCount())) return false;
            _ = header.ready.fetchOr(bit, .seq_cst);
            return true;
        }

        /// Message `claim` is done: its sender may reuse its room (`.release`: this taker's reads of the payload come
        /// first), and is woken if it sleeps for room (not exact: core/sender.zig says what that costs).
        pub fn finish(taker: *Self, claim: Claim) void {
            taker.layout.cell(claim.ring, claim.index).state.store(ring_set.done(claim.index, taker.slot), .release);
            if (taker.layout.ring(claim.ring).room_sleeps.load(.monotonic) != 0) Rings.wakeSender(&taker.layout, &taker.wakes, claim.ring);
        }

        /// A claimed message that is a corrupt peer's, dropped: done (`finish`), or where messages carry replies,
        /// answered as abandoned, with no reply (core/ring_set.zig `answer`).
        pub fn drop(taker: *Self, claim: Claim) void {
            if (Rings.replies) Rings.answer(&taker.layout, &taker.wakes, taker.slot, claim, .abandoned, 0) else taker.finish(claim);
        }

        fn warnCorrupt(taker: *Self, ring: u32) void {
            const bit = ring_set.bitOf(ring);
            if (taker.warned_rings & bit != 0) return;
            taker.warned_rings |= bit;
            log.warn(@src(), "the " ++ Rings.noun ++ "'s ring {d} has corrupt indices: skipped", .{ring});
        }

        /// Waits after a pass that took nothing (the module's comment). Each round, in this order: spin, unless the
        /// call doesn't wait or the last spin's hit led to nothing; cancelled → Cancelled; the kind's end → one
        /// draining pass, then Disconnected (where takers drain; else Disconnected at once); this taker's own failure
        /// → Disconnected; the call's timeout 0 or past → Timeout; a surplus server's spin after a lost race
        /// (`spinMode`); then announce the park, and sleep a slice at most, or nap 1 ms if a ready bit is set but the
        /// passes take nothing.
        pub fn wait(taker: *Self, endpoint: *Endpoint, deadline: *Deadline) DataError!void {
            const fruitless = taker.spun;
            taker.spun = false;
            if (deadline.timeout_ms != 0 and !fruitless and taker.spin()) {
                taker.spun = true;
                return;
            }
            if (endpoint.hot.requests.load(.acquire).cancel) return error.Cancelled;
            // `.seq_cst`: the taker's half of the end's pair, this load, then the drain pass's loads of `tail`
            const ended = taker.layout.header.ended.load(.seq_cst) != 0;
            if (ended or endpoint.hot.phase.load(.acquire) != .connected) {
                // The watcher stores the end's mark before the phase: a phase without the mark is this member's failure
                if (!ended and taker.layout.header.ended.load(.seq_cst) == 0) return error.Disconnected;
                if (!drains or taker.draining) return error.Disconnected; // a draining pass found nothing
                taker.draining = true;
                return;
            }
            const sleep_ms = try wait_module.sleepMsWithin(endpoint.io, deadline, taker.longest_sleep_ms);
            // A lost race of a surplus server (`spinMode`), the call's deadline still ahead: the winner took the
            // request, so with no ready bit set it spins again for the next one instead of parking, where a client's
            // publish would have to wake it
            if (fruitless and taker.spinMode() == .surplus and taker.layout.header.ready.load(.monotonic) & taker.layout.ringMask() == 0 and taker.spin()) {
                taker.spun = true;
                return;
            }
            const slot = taker.layout.takerSlot(taker.slot);
            defer {
                // On every way out, a cancel through `Io` too; a waker may have taken them already
                Rings.clearIdle(&taker.layout, taker.slot);
                _ = slot.sleeps.swap(0, .seq_cst);
            }
            switch (taker.announcePark(endpoint)) {
                .look => {},
                .sleep => {
                    if (build_options.test_hooks) _ = taker.sleeps.fetchAdd(1, .seq_cst);
                    // A timeout means a missed or corrupt ready bit: the next pass looks at every ring
                    if (platform.ringWait(&slot.sleeps, taker.own_event, sleep_ms)) taker.full_scan = true;
                    try wait_module.backInIo(endpoint.io);
                },
                .work => if (fruitless) {
                    // A ready bit stays set, but the passes take nothing (a peer that keeps setting it, or others
                    // winning every message): a nap, within the call's deadline, instead of a loop
                    platform.sleep(@min(sleep_ms, 1));
                    taker.full_scan = true;
                },
            }
        }

        /// The taker's side of its park (the module's comment): its flag, then its idle bit, then the looks at
        /// `ready`, `requests`, the phase and `ended`, all sequentially consistent. `.sleep` only if none changed and
        /// no ready bit is set; `.work` if one is; `.look` if a request, the phase or the end changed. The caller
        /// clears the flag and the bit afterwards.
        pub fn announcePark(taker: *Self, endpoint: *const Endpoint) enum { sleep, work, look } {
            const header = taker.layout.header;
            _ = taker.layout.takerSlot(taker.slot).sleeps.swap(1, .seq_cst);
            _ = header.idle.fetchOr(ring_set.bitOf(taker.slot), .seq_cst);
            const work = header.ready.load(.seq_cst) & taker.layout.ringMask() != 0;
            if (endpoint.hot.requests.load(.seq_cst).cancel or endpoint.hot.phase.load(.seq_cst) != .connected or
                header.ended.load(.seq_cst) != 0) return .look;
            return if (work) .work else .sleep;
        }

        /// A taker's spin: true once a ready bit is set. Paced as a broadcast subscriber's (broadcast/reader.zig
        /// `Reader.spin`): it leaves the first `pace` hints without a look, then looks after every hint; a first look
        /// that hits doubles the pace, up to `stream_pace`, and a later one sets it back to 1.
        ///
        /// A service's servers pace it only as a pool no larger than its clients (`spinMode`).
        pub fn spin(taker: *Self) bool {
            const ready = &taker.layout.header.ready;
            const mask = taker.layout.ringMask();
            const paced = taker.spinMode() == .paced;
            if (!paced) taker.pace = 1;
            const first = @min(taker.pace, taker.spin_iters);
            for (0..first) |_| std.atomic.spinLoopHint();
            for (first..taker.spin_iters) |i| {
                if (ready.load(.monotonic) & mask != 0) {
                    if (paced) taker.pace = if (i == first) @min(taker.pace *| 2, wait_module.stream_pace) else 1;
                    return true;
                }
                std.atomic.spinLoopHint();
            }
            taker.pace = 1;
            return false;
        }

        /// How a taker waits for its next message (`spin`, `wait`), by the members the owner shows joined where the
        /// kind shows them (a service's: core/ring_set.zig `joinedShown`):
        ///   - `paced`: a queue's consumers, and the servers of a pool no larger than its clients where pools are
        ///     paced (`pool_paced`). Those that wait look at `ready` less often, which the clients' publishes and the
        ///     other servers' takes need;
        ///   - `alone`: a service's one server, and where pools aren't paced the servers of a pool no larger than its
        ///     clients. It waits for the next call of a client that waits for its reply, as a stream's reader that
        ///     sent waits for an answer (stream/ring_wait.zig `spinPace`), so it looks after every hint. Paced, a call
        ///     that comes within the paced hints waits for the rest of them, and a round trip shorter than those hints
        ///     keeps the first looks hitting and the pace at its top;
        ///   - `surplus`: the servers of a pool larger than its clients, which always has servers waiting. They look
        ///     after every hint too, and one that lost a request to another spins again rather than parking: parked
        ///     servers would make every request's publish wake one, which then loses to the servers that spin.
        pub fn spinMode(taker: *const Self) enum { paced, alone, surplus } {
            if (!Rings.replies) return .paced;
            if (taker.layout.max_takers == 1) return .alone;
            const joined = Rings.joinedShown(&taker.layout);
            if (joined.takers <= 1) return .alone;
            if (joined.takers > joined.senders) return .surplus;
            return if (pool_paced) .paced else .alone;
        }

        /// The local interrupt of this taker's park (cancel, the end), after the caller published what the park must
        /// see (`.seq_cst`): the park's looks come after its flag's swap to 1, so either they see what was published
        /// or this swap sees the 1 and wakes it.
        pub fn interrupt(taker: *const Self) void {
            const flag = &taker.layout.takerSlot(taker.slot).sleeps;
            if (flag.swap(0, .seq_cst) == 1) platform.ringWake(flag, taker.own_event);
        }
    };
}
