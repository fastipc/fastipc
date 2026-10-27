//! A ring set's senders (core/ring_set.zig): the sends into a sender's own ring, and the room it waits for while takers
//! haven't taken its messages. A work queue's producers are its senders (queue/producer.zig), and so is its owner, on
//! the return ring (queue/returns.zig): it publishes there as a producer does, but never waits for room (`reserveNow`).
//! A service's clients are its senders too (service/caller.zig), whose messages carry replies. Everything here works on
//! a mapped ring set (`Layout`) and the sender's endpoint, whose `Io`, `requests` and `phase` it reads; the member's
//! lifecycle sets it up. How takers take the messages is core/taker.zig.
//!
//! Publish. The sender writes the payload into its data ring and the cell's payload fields, then the cell's state
//! `published(i)` (`.release`, which a claimer's acquire pairs with), then `tail = i + 1` (`.seq_cst`). Then it loads
//! `ready` and, if its bit is clear, sets it and wakes one parked taker: with the takers' clearing of the bit
//! (core/taker.zig `clearReady`) it is a Dekker handshake, every access sequentially consistent, so a ring with a
//! message to take always ends with its bit set by someone who then claims or wakes. Last, where takers drain (a
//! queue's), it loads `ended`: the end's mark is stored before the takers' drain (queue/server.zig `close`,
//! queue/client.zig `endMembership`), so a send that returns OK is drained by a taker that receives until DISCONNECTED;
//! one that returns DISCONNECTED may or may not have been. Where messages carry replies (a service's) the takers don't
//! drain, so that load would promise nothing: a published request returns OK, and its answer, or DISCONNECTED once the
//! service ended, is the sender's receive's (service/caller.zig). A send returns DISCONNECTED only before it publishes.
//!
//! Room. The sender reuses a cell, and the data a payload took, only once that message is done and every message
//! before it too: `reclaim` walks its own messages from `reclaimed`, as far as `head` (a done cell at or past `head`
//! may be done before `head` moved past it, and the takers at `head` must never see a later message's state there),
//! and stops at the first one not done. The payload positions come from its own record (core/position_record.zig),
//! never from the cells. A message held in place holds room back, not work: takers go on taking the messages after it
//! until the sender is a ring ahead of it.
//!
//! Replies. Where messages carry replies (`Rings.replies`), a message's room is its reply's room (`Layout.replyRoom`),
//! then its payload: the position record keeps the reservation's start, the cell the payload's. Its room is free only
//! once the sender's own receiving thread has consumed its answer, so the reclaim stops at `consumed` too (the
//! receiving thread's count, stored `.release` after its reads of the reply). Nor does it walk the cells there: the
//! receive saw each consumed answer's done, so everything below `consumed` (and `head`) is free at once. A walk over a
//! lap of cells, one line each, would make one call in every lap slower by hundreds of nanoseconds. The takers never
//! wake the wait for room: the receiving thread does, once it consumed an answer (service/caller.zig), with the same
//! inexact pair.
//!
//! The room's spin. A sender that finds no room spins, reclaiming every 64 hints, until a share of its ring is free past
//! the room it needs (`spin_share`), then sends into that room without reclaiming again.
//!
//! The room's sleep. A sender waiting for room sets its ring's `room_sleeps`, reclaims again and sleeps if nothing
//! changed; a taker marking a message done (`.release`) wakes it if it sees the flag (`.monotonic`). That pair is not
//! exact: a sender that announces its sleep just as a taker marks done can miss it and sleeps out its slice, which
//! grows from 1 ms to `payload_ring.room_slice_ms` (10 ms) and so bounds the delay. The exact pair would cost a locked
//! instruction per message. The sender's own interrupts (cancel, the end) are exact: they swap the flag after their
//! stores.
//!
//! A corrupt peer can write anything in the segment but the sender's own records: it writes only at positions it
//! chose, frees room only on a done state of its own message's exact index (a forged one frees room early: a
//! disturbance, nothing else), and bounds its walk by its own `next`.

const std = @import("std");
const build_options = @import("build_options");
const wire_protocol = @import("../wire_protocol.zig");
const platform = @import("../platform.zig");
const test_hooks = @import("test_hooks.zig");
const copy = @import("copy.zig").copy;
const log = @import("log.zig");
const wait_module = @import("wait.zig");
const payload_ring = @import("payload_ring.zig");
const ring_set = @import("ring_set.zig");

const Atomic = std.atomic.Value;
const AtomicOrder = std.lang.AtomicOrder;
const Endpoint = @import("endpoint.zig").Endpoint;
const Deadline = wait_module.Deadline;
const PositionRecord = @import("position_record.zig").PositionRecord;

const DataError = @import("endpoint.zig").DataError;

/// A sender that waits for room spins until this share of its ring is free past the room it needs, then sends into that
/// room without reclaiming again. Resuming for every message the takers free would reclaim, and move the lines of
/// `head` and of the cells the takers mark done between the cores, for every message again (as a stream's writer:
/// stream/ring_wait.zig `writer_spin_share`). Only the spins look for it: after a spin that doesn't find it, and for a
/// call that doesn't wait, the room the message needs is enough.
const spin_share = 8;

/// A sender of the ring set `Rings` (core/ring_set.zig `RingSet`).
pub fn Sender(comptime Rings: type) type {
    return struct {
        const Self = @This();
        const Layout = Rings.Layout;

        layout: Layout,
        /// Its slot: the ring it writes.
        ring: u32,
        /// Each message's payload position: `cell_count` entries in this process's memory, a sixteenth of the
        /// capacity.
        positions: PositionRecord,
        /// What wakes this sender's sleep for room besides its flag (Windows: its slot's event).
        own_event: platform.RingEvent,
        /// The other slots' wakes, for the taker this sender wakes.
        wakes: Rings.Wakes,
        /// The next message's index.
        next: u64,
        /// Every message below it is done and its room free.
        reclaimed: u64,
        /// The data ring's position after the last payload.
        payload_end: u64 = 0,
        /// The sender may write cells below this (`reclaim` moves it).
        cell_limit: u64,
        /// The sender may write data below this (`reclaim` moves it).
        data_limit: u64,
        reservation: payload_ring.Reservation = .{},
        /// The first and the longest slice of a wait for room: the defaults, which only tests change.
        first_slice_ms: u32 = payload_ring.first_slice_ms,
        longest_slice_ms: u32 = payload_ring.room_slice_ms,
        /// The sleeps for room begun, counted in the tests' builds only: a test that sees it move knows the sender is
        /// past every look before its sleep.
        sleeps: Atomic(u32) = .init(0),
        /// A corrupt peer's state stalled `reclaim`, and was logged (once).
        warned_stalled: bool = false,
        /// Where messages carry replies: the index of the first message whose answer the sender's receiving thread
        /// hasn't consumed, its count on its own lines, which bounds `reclaim`. Set by the kind once the sender is in
        /// place.
        consumed: if (Rings.replies) *const Atomic(u64) else void = if (Rings.replies) undefined else {},

        /// The sender of ring `ring`, which starts where the ring's last sender left it: an empty ring at `tail`, all
        /// its messages claimed (the owner gives a slot out only then). `positions`: `cell_count` entries, the
        /// caller's. Invalid: the ring's indices are a corrupt peer's.
        pub fn init(layout: Layout, ring: u32, positions: []u32, own_event: platform.RingEvent, session_id: [16]u8) DataError!Self {
            std.debug.assert(positions.len == layout.cellCount());
            const control = layout.ring(ring);
            const tail = control.tail.load(.acquire);
            const head = control.head.load(.acquire);
            if (tail >= wire_protocol.queue_max_index or head != tail) return error.Invalid;
            return .{
                .layout = layout,
                .ring = ring,
                .positions = .{ .entries = positions },
                .own_event = own_event,
                .wakes = .init(session_id),
                .next = tail,
                .reclaimed = tail,
                .cell_limit = tail + layout.cellCount(),
                .data_limit = layout.capacity,
            };
        }

        /// Closes the wakes it opened.
        pub fn deinit(sender: *Self) void {
            sender.wakes.deinit();
        }

        /// `fipc_send` on a sender: copies `payload` into its ring and publishes it, after waiting, within
        /// `timeout_ms`, for room.
        pub fn send(sender: *Self, endpoint: *Endpoint, payload: []const u8, timeout_ms: c_int) DataError!void {
            try sender.open(endpoint);
            const at = try sender.reserve(endpoint, payload.len, timeout_ms);
            copy(sender.layout.bytes(sender.ring, at, payload.len), payload);
            return sender.publish(at, payload.len);
        }

        /// `fipc_send_many` on a sender: sends the messages of `batch` (core/calls.zig checked them: 1 to `maxPiece`
        /// bytes each) in order, each written as `send` writes it, and publishes them together: one store of `tail`, one
        /// look at the ready bit and, where takers drain, one look at the end's mark for the run (`publishWritten`),
        /// after its last message, and before any wait for room, so that the takers can free what it wrote. The call's
        /// timeout bounds its waits together. `sent` is how many went: a message a wait stopped isn't counted, and
        /// neither is a run whose publish met the end (Disconnected; it may still reach a taker, as a send's may).
        pub fn sendMany(sender: *Self, endpoint: *Endpoint, batch: anytype, timeout_ms: c_int, sent: *usize) DataError!void {
            sender.reservation.drop();
            try sender.open(endpoint);
            var deadline: Deadline = .init(timeout_ms);
            // Messages written since the last publish; a failure publishes them, uncounted
            var unpublished = false;
            errdefer if (unpublished) sender.publishTail();
            for (0..batch.count) |i| {
                const payload = batch.at(i);
                const end = sender.place(sender.layout.replyRoom() + payload.len).end;
                if (!sender.fits(end)) {
                    if (unpublished) {
                        unpublished = false;
                        try sender.publishWritten();
                        sent.* = i;
                    }
                    try sender.room(endpoint, end, &deadline);
                }
                // The room is there: the reserve doesn't wait
                const at = try sender.reserve(endpoint, payload.len, timeout_ms);
                copy(sender.layout.bytes(sender.ring, at, payload.len), payload);
                sender.writeMessage(at, payload.len);
                unpublished = true;
            }
            unpublished = false;
            try sender.publishWritten();
            sent.* = batch.count;
        }

        /// `fipc_send_acquire` on a sender: room for a message of up to `len` bytes, in place in its data ring (64-byte
        /// aligned), for `sendCommit`. A later acquire or send drops the reservation.
        pub fn sendAcquire(sender: *Self, endpoint: *Endpoint, len: usize, timeout_ms: c_int) DataError![]u8 {
            try sender.open(endpoint);
            const at = try sender.reserve(endpoint, len, timeout_ms);
            sender.reservation.set(at, len);
            return sender.layout.bytes(sender.ring, at, len);
        }

        /// `fipc_send_commit` on a sender: publishes the first `len` bytes of the reservation. Invalid without one, or
        /// for a `len` of 0 or past it (the reservation stays); Disconnected once the kind ended (the reservation is
        /// dropped).
        pub fn sendCommit(sender: *Self, endpoint: *Endpoint, len: usize) DataError!void {
            const at = try sender.reservation.commit(len);
            try sender.open(endpoint);
            return sender.publish(at, len);
        }

        /// Disconnected once the kind ended or this sender's connection did: not exact (the exact pair is `publish`'s),
        /// it only saves writing a message after the end.
        fn open(sender: *const Self, endpoint: *const Endpoint) DataError!void {
            if (endpoint.hot.phase.load(.acquire) != .connected) return error.Disconnected;
            if (sender.layout.header.ended.load(.acquire) != 0) return error.Disconnected;
        }

        /// Where a payload of `len` bytes goes. No state changes.
        inline fn place(sender: *const Self, len: usize) payload_ring.Placement {
            return payload_ring.place(sender.payload_end, sender.layout.capacity, len, wire_protocol.queue_payload_align);
        }

        /// The room for the next message's payload of `len` bytes, after it dropped a reservation: waits, within
        /// `timeout_ms`, if a limit is in the way, and takes the room (the skip to the ring's start too).
        fn reserve(sender: *Self, endpoint: *Endpoint, len: usize, timeout_ms: c_int) DataError!u64 {
            sender.reservation.drop();
            if (len == 0) return error.Invalid;
            if (len > sender.layout.maxPiece()) return error.TooLarge;
            if (sender.next >= wire_protocol.queue_max_index - 1) return error.Invalid; // decades of messages away
            // The reply's room first, where messages carry replies (0 elsewhere)
            const reply_room = sender.layout.replyRoom();
            const placement = sender.place(reply_room + len);
            if (!sender.fits(placement.end)) {
                @branchHint(.unlikely);
                var deadline: Deadline = .init(timeout_ms);
                try sender.room(endpoint, placement.end, &deadline);
            }
            sender.payload_end = placement.end;
            return placement.at + reply_room;
        }

        /// Whether the next message's cell, and a payload ending at `end`, are free.
        fn fits(sender: *const Self, end: u64) bool {
            return sender.next < sender.cell_limit and end <= sender.data_limit;
        }

        /// Whether `fits`, and a share of the ring past it too (`spin_share`): an eighth of the cells and of the data.
        fn fitsShare(sender: *const Self, end: u64) bool {
            return sender.next + sender.layout.cellCount() / spin_share < sender.cell_limit and
                end + sender.layout.capacity / spin_share <= sender.data_limit;
        }

        /// The owner's reserve on a ring past the senders' (a queue's return ring, queue/returns.zig), which never
        /// waits: the room for a payload of `len` bytes (1 to `maxPiece`), after a reclaim if a limit is in the way,
        /// taken; null if there is none.
        pub fn reserveNow(sender: *Self, len: usize) ?u64 {
            comptime std.debug.assert(!Rings.replies);
            std.debug.assert(len != 0 and len <= sender.layout.maxPiece());
            if (sender.next >= wire_protocol.queue_max_index - 1) return null;
            const placement = sender.place(len);
            if (!sender.fits(placement.end)) {
                sender.reclaim(.acquire);
                if (!sender.fits(placement.end)) return null;
            }
            sender.payload_end = placement.end;
            return placement.at;
        }

        /// Publishes the next message, whose payload of `len` bytes is at `at`, then, where takers drain, loads the
        /// end's mark (the module's comment).
        fn publish(sender: *Self, at: u64, len: usize) DataError!void {
            sender.writeMessage(at, len);
            return sender.publishWritten();
        }

        /// Publishes the next message, whose payload of `len` bytes is at `at`: its cell, `tail`, and its ring's ready
        /// bit with a wake if the ring looked empty (the module's comment).
        pub fn publishMessage(sender: *Self, at: u64, len: usize) void {
            sender.writeMessage(at, len);
            sender.publishTail();
        }

        /// The messages written since the last publish (`writeMessage`), published together (`publishTail`), then,
        /// where takers drain, the end's mark loaded (the module's comment).
        fn publishWritten(sender: *Self) DataError!void {
            sender.publishTail();
            if (Rings.replies) return;
            if (sender.layout.header.ended.load(.seq_cst) != 0) return error.Disconnected;
        }

        /// The next message, whose payload of `len` bytes is at `at`: its cell, published (`.release`), and its
        /// reservation's start in the sender's record; `tail` isn't stored (`publishTail`).
        inline fn writeMessage(sender: *Self, at: u64, len: usize) void {
            const index = sender.next;
            const cell = sender.layout.cell(sender.ring, index);
            cell.payload_position.store(at, .monotonic);
            cell.len.store(@intCast(len), .monotonic);
            Rings.clearExtra(cell);
            // `.release`: the payload and the fields before the state, for the claimer's compare-and-swap that
            // acquires it
            cell.state.store(ring_set.published(index), .release);
            // The reservation's start: its reply's room, where messages carry replies
            sender.positions.record(index, at - sender.layout.replyRoom());
            sender.next = index + 1;
        }

        /// Stores `tail` past the messages written, then sets the ring's ready bit, with a wake, if the ring looked
        /// empty (the module's comment).
        inline fn publishTail(sender: *Self) void {
            const header = sender.layout.header;
            // `.seq_cst`: with the loads below, the sender's half of two Dekker pairs, tail and ready (a taker's
            // `clearReady`), tail and ended (a taker's drain)
            sender.layout.ring(sender.ring).tail.store(sender.next, .seq_cst);
            if (build_options.test_hooks) test_hooks.crashIfAt(Rings.published_step);
            const bit = ring_set.bitOf(sender.ring);
            if (header.ready.load(.seq_cst) & bit == 0) {
                // The ring looked empty to the takers: its bit, and a wake of one that parked
                if (header.ready.fetchOr(bit, .seq_cst) & bit == 0) Rings.wakeOneIdle(&sender.layout, &sender.wakes, sender.ring);
            }
        }

        /// Frees the room of the run of done messages from `reclaimed`, as far as `head` (and where messages carry
        /// replies, as far as `consumed`), and moves the limits. `.acquire` (every taker's reads of a payload, and the
        /// receiving thread's of a reply, before the sender's reuse of it), or `.seq_cst` after the sender announced
        /// its sleep.
        fn reclaim(sender: *Self, comptime order: AtomicOrder) void {
            const head = sender.layout.ring(sender.ring).head.load(order);
            var limit = @min(head, sender.next); // a head past `next` is a corrupt peer's
            if (Rings.replies) {
                // Every message below `consumed` is done: its receive saw the done before it consumed the answer
                limit = @min(limit, sender.consumed.load(order));
                sender.reclaimed = @max(sender.reclaimed, limit);
            }
            while (sender.reclaimed < limit) {
                const state = sender.layout.cell(sender.ring, sender.reclaimed).state.load(order);
                const ours = ring_set.ofMessage(state, sender.reclaimed);
                if (ours and ring_set.phaseOf(state) == .done) {
                    sender.reclaimed += 1;
                    continue;
                }
                // Below `head` a cell holds its message claimed or done: anything else is a corrupt peer's state, which
                // holds this sender's room back for good
                if (!ours or ring_set.phaseOf(state) != .claimed) sender.warnStalled();
                break;
            }
            sender.cell_limit = sender.reclaimed + sender.layout.cellCount();
            sender.data_limit = sender.positions.positionOf(sender.reclaimed, sender.next, sender.payload_end) + sender.layout.capacity;
        }

        fn warnStalled(sender: *Self) void {
            if (sender.warned_stalled) return;
            sender.warned_stalled = true;
            log.warn(@src(), "message {d} of the " ++ Rings.noun ++ "'s ring {d} has a corrupt peer's state: its room is never freed", .{ sender.reclaimed, sender.ring });
        }

        /// Waits until a payload ending at `end`, and the next message's cell, are free (the module's comment). Each
        /// round: reclaim; spin, unless the call doesn't wait, reclaiming every 64 hints (until a share of the ring is
        /// free past `end` too, until a spin doesn't see it: `spin_share`); cancelled → Cancelled; the end →
        /// Disconnected; the call's timeout 0 or past → Timeout; then announce the sleep and sleep a slice at most.
        noinline fn room(sender: *Self, endpoint: *Endpoint, end: u64, deadline: *Deadline) DataError!void {
            var slice_ms = sender.first_slice_ms;
            // The spins wait for a share of the ring past `end` too, until one doesn't see it (`spin_share`)
            var share = true;
            while (true) {
                sender.reclaim(.acquire);
                if (sender.fits(end)) return;
                if (deadline.timeout_ms != 0) {
                    for (0..wait_module.spin_iters / 64) |_| {
                        for (0..64) |_| std.atomic.spinLoopHint();
                        sender.reclaim(.acquire);
                        if (if (share) sender.fitsShare(end) else sender.fits(end)) return;
                    }
                    share = false;
                    if (sender.fits(end)) return;
                }
                if (endpoint.hot.requests.load(.acquire).cancel) return error.Cancelled;
                try sender.open(endpoint);
                const sleep_ms = try wait_module.sleepMsWithin(endpoint.io, deadline, slice_ms);
                const reclaimed = sender.reclaimed;
                if (sender.announceRoomSleep(endpoint, end) == .sleep) {
                    if (build_options.test_hooks) _ = sender.sleeps.fetchAdd(1, .seq_cst);
                    _ = platform.ringWait(&sender.layout.ring(sender.ring).room_sleeps, sender.own_event, sleep_ms);
                    try wait_module.backInIo(endpoint.io);
                }
                // The slices start again from the first once room moved
                slice_ms = if (sender.reclaimed != reclaimed) sender.first_slice_ms else @min(slice_ms *| 2, sender.longest_slice_ms);
            }
        }

        /// The sender's side of its sleep for room: sets `room_sleeps`, then reclaims and looks at `requests`, the
        /// phase and `ended` again. `.sleep` only if nothing changed; otherwise the flag is swapped back (a 1 a waker
        /// took meanwhile only ends the next sleep early).
        pub fn announceRoomSleep(sender: *Self, endpoint: *const Endpoint, end: u64) enum { ready, sleep } {
            const flag = &sender.layout.ring(sender.ring).room_sleeps;
            _ = flag.swap(1, .seq_cst);
            sender.reclaim(.seq_cst);
            if (sender.fits(end) or endpoint.hot.requests.load(.seq_cst).cancel or
                endpoint.hot.phase.load(.seq_cst) != .connected or sender.layout.header.ended.load(.seq_cst) != 0)
            {
                _ = flag.swap(0, .seq_cst);
                return .ready;
            }
            return .sleep;
        }

        /// The local interrupt of this sender's wait for room (cancel, the end), after the caller published what the
        /// wait must see (`.seq_cst`): the wait's looks come after its flag's swap to 1, so either they see what was
        /// published or this swap sees the 1 and wakes it.
        pub fn interrupt(sender: *const Self) void {
            const flag = &sender.layout.ring(sender.ring).room_sleeps;
            if (flag.swap(0, .seq_cst) == 1) platform.ringWake(flag, sender.own_event);
        }
    };
}
