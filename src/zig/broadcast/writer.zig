//! A broadcast channel's writer (docs/protocol.md §9.5, §9.6): its publish, and the room it waits for while gated
//! subscribers hold messages. Everything here works on a mapped channel segment (segment.zig `Layout`) and the writer's
//! endpoint, whose `Io` and `requests` it reads; the owner (server.zig) sets it up. The subscribers' side is
//! reader.zig.
//!
//! Messages. Message `i` has cell `i % cell_count` and a payload in the data ring at its position
//! `payload_position`, a monotonic byte count; a payload never wraps (the writer skips to the ring's start instead)
//! and takes a multiple of 64 bytes. The cell is a seqlock: the writer stores `seq = (i + 1) | busy`, a store fence,
//! `payload_position` and `len`, then `seq = i + 1` (`.release`). Then it stores the published `next = i + 1`
//! (`.seq_cst`), which the owner starts joiners at and sleepers watch. The writer announces `frontier`, the end of the
//! sixteenth of the data ring it may write next, before it writes there (a store fence between them), which a lossy
//! subscriber's copy is checked against (reader.zig).
//!
//! Room. The writer never writes cell `i` before every gated cursor is past `i - cell_count + 1`, nor data a ring past
//! a cursor's message. It keeps `free_index`, an index every gated cursor has reached, and from it the limits it may
//! write up to: cells below `free_index + cell_count`, data below `payloadPositionOf(free_index) + capacity`. It takes
//! payload positions from its own record (core/position_record.zig), not from the cells a subscriber could rewrite.
//! Most sends check only the two limits. Past one, `room` finds the oldest message the send needs gone (`need`) and
//! sweeps the gated bitmap's set bits for their cursors (`sweep`); a sweep that meets a cursor below `need` stops
//! there, the writer waits for that subscriber (`waitForLaggard`), and the next sweep resumes at it. While its spins
//! find it, the writer waits for an eighth of the messages in flight past `need` too (`writer_spin_share`), so that it
//! sweeps once per eighth, not once per message. With no gated subscriber a sweep reads one line, once per lap. Every gated subscriber held still past the channel's `max_stall_ms`
//! from the first send it held back (the waits of later sends add up) is reported to the owner (`on_stall`), once, and
//! the owner drops it.
//!
//! Joining. The owner starts a subscriber before WELCOME, on its admitting thread (`startSlot`): it stores the
//! published `next` as the slot's cursor, which the subscriber reads after WELCOME and starts at (reader.zig
//! `Reader.init`). So every message sent once the owner counts the subscriber reaches it, however late its process
//! reads the start.
//!
//! A gated subscriber needs more, because a sweep may run while the owner sets its bit. The owner stores `next` as its
//! cursor, sets its gated bit, loads `next` again and stores that as the start (all `.seq_cst`). Then every sweep is
//! safe:
//!   - a sweep that saw the bit read a cursor no later than the start;
//!   - a sweep that didn't see the bit loaded its bitmap words before the bit was set, and after the writer's last
//!     store of `next` before the sweep, so the start is at or after the `next` that sweep caps `free_index` with.
//!
//! Sleeps. After each send the writer takes the armed bits and wakes their subscribers (`wakeSubscribers`;
//! docs/protocol.md §9.5 gives the argument). A writer waiting for room sleeps on `writer_sleeps` as a ring's writer
//! sleeps on its flag (docs/protocol.md §6.1): the laggard's cursor store and then the flag's swap wake it. A gated
//! subscriber stores its cursor on every receive (`.release`) and wakes the writer if its flag is set; only before its
//! own sleep is that pair sequentially consistent. So the writer can go to sleep just as a cursor moved, its swap and
//! look racing the subscriber's store and look, and miss it: then it sleeps out its slice with room there, and as it
//! never saw the move the slice doesn't start again from 1 ms. The slices grow from 1 ms to
//! `payload_ring.room_slice_ms` (10 ms), which bounds that delay; the exact pair on every receive would cost a locked
//! instruction per message.
//!
//! A corrupt peer can write anything in the segment but what the writer's own records hold: the writer only ever
//! writes at positions it chose, reads the bitmaps' bits against the owner's own record of its slots (roster.zig), and
//! computes its room from the cursors only (a corrupt cursor holds back or lets go no more than that subscriber's own
//! messages).

const std = @import("std");
const Io = std.Io;
const wire_protocol = @import("../wire_protocol.zig");
const platform = @import("../platform.zig");
const copy = @import("../core/copy.zig").copy;
const wait_module = @import("../core/wait.zig");
const fence = @import("../core/fence.zig");
const payload_ring = @import("../core/payload_ring.zig");
const segment = @import("segment.zig");

const Atomic = std.atomic.Value;
const Endpoint = @import("../core/endpoint.zig").Endpoint;
const Deadline = wait_module.Deadline;
const Layout = segment.Layout;
const PositionRecord = @import("../core/position_record.zig").PositionRecord;
const Roster = @import("roster.zig").Roster;
const busy = wire_protocol.broadcast_busy;
const hasBit = segment.hasBit;
const setBit = segment.setBit;
const wakeAll = segment.wakeAll;

const DataError = @import("../core/endpoint.zig").DataError;

/// The owner's half of a join (docs/protocol.md §9.3), before WELCOME and before it counts the subscriber: stores the
/// published `next` as slot `slot`'s cursor, where the subscriber starts (`Reader.init`). A gated subscriber's bit is
/// set here too, by the double store (the module's comment), after the roster's mark (`Roster.holdGated`), so that the
/// writer overwrites nothing from the start on, however late the subscriber reads it.
pub fn startSlot(layout: *const Layout, slot: u32, gated: bool) void {
    const cursor = &layout.slots[slot].cursor;
    if (gated) {
        // `.monotonic`: the bit's `.seq_cst` OR after it releases it to a sweep that sees the bit
        cursor.store(layout.header.next.load(.seq_cst), .monotonic);
        setBit(&layout.shared.gated, slot, .seq_cst);
    }
    cursor.store(layout.header.next.load(.seq_cst), .release);
}

/// A writer that waits for room spins until every gated subscriber has consumed this share of the messages from `need`
/// to `next` too, not just up to `need`, and then sends into that room without a sweep. Resuming for every message a
/// laggard consumes would sweep, and move the cursors' cache lines between the cores, for every message again (as a
/// stream's writer: stream/ring_wait.zig `writer_spin_share`). Only the spins look for it: after a spin that doesn't
/// find it, and for a call that doesn't wait, `need` is enough.
const writer_spin_share = 8;

/// While it spins for that share, the writer looks at the laggard's cursor once every this many hints: each look pulls
/// the cursor's line from the subscriber, which must win it back for its next store, and a writer that looked after
/// every hint slowed its laggard, four gated subscribers' 16 B messages by about a fifth. A spin for `need` alone looks
/// after every hint.
const writer_spin_pace = 16;

/// The writer's state: private to the thread that sends, in the owner's channel.
pub const Writer = struct {
    layout: Layout,
    roster: *const Roster,
    /// Each message's payload position (core/position_record.zig): what `room` reads instead of the cells'
    /// `payload_position`, which a subscriber that maps the segment writable could change. Owner-process memory,
    /// `cell_count` entries: a sixteenth of the capacity (128 MiB for a channel of 2 GiB), allocated with the channel
    /// and touched as the writer first goes round.
    payload_positions: PositionRecord,
    /// The channel's `max_stall_ms`: a gated subscriber held still this long from the first send it held back is
    /// reported to `on_stall`; never when not positive.
    max_stall_ms: i32,
    /// Called on the writer's thread, once per stall of a subscriber (its slot and the generation the roster had): the
    /// owner drops that subscriber (server.zig) and returns true; false: reported again at the next wait
    /// (the owner's frame couldn't be sent). The writer goes on waiting within its send's timeout.
    on_stall: *const fn (context: *anyopaque, slot: u32, generation: u32) bool,
    context: *anyopaque,
    /// The next message's index.
    next: u64 = 0,
    /// The data ring's position after the last payload.
    payload_end: u64 = 0,
    /// The frontier announced last.
    announced_frontier: u64 = 0,
    /// Every gated cursor is at least this (`sweep` moves it).
    free_index: u64 = 0,
    /// The writer may write cells below this.
    cell_limit: u64,
    /// The writer may write data below this.
    data_limit: u64,
    reservation: payload_ring.Reservation = .{},
    scan: Scan = .{},
    /// Each slot's stall, so that every gated subscriber that holds the writer back is reported once it has been still
    /// `max_stall_ms`, not one laggard at a time.
    stalls: [wire_protocol.max_channel_subscribers]Stall = @splat(.{}),
    /// The room's round: a sweep that succeeds ends it, and a stall of an earlier round counts as none.
    round: u32 = 1,
    /// The first slice of a wait for room: the default, which only tests change.
    first_slice_ms: u32 = payload_ring.first_slice_ms,
    /// The longest slice of a wait for room: the default, which only tests change.
    longest_slice_ms: u32 = payload_ring.room_slice_ms,

    /// The writer of a new channel, whose segment is still all zeroes but its description: an empty channel, its
    /// first lap free.
    pub fn init(
        layout: Layout,
        roster: *const Roster,
        payload_positions: []u32,
        max_stall_ms: i32,
        on_stall: *const fn (context: *anyopaque, slot: u32, generation: u32) bool,
        context: *anyopaque,
    ) Writer {
        std.debug.assert(payload_positions.len == layout.cellCount());
        return .{
            .layout = layout,
            .roster = roster,
            .payload_positions = .{ .entries = payload_positions },
            .max_stall_ms = max_stall_ms,
            .on_stall = on_stall,
            .context = context,
            .cell_limit = layout.cellCount(),
            .data_limit = layout.capacity,
        };
    }

    /// `fipc_send` on the writer: copies `payload` into the data ring and publishes it to every subscriber, after
    /// waiting, within `timeout_ms`, for the room gated subscribers hold.
    pub fn send(writer: *Writer, endpoint: *Endpoint, payload: []const u8, timeout_ms: c_int) DataError!void {
        const at = try writer.reserve(endpoint, payload.len, timeout_ms);
        copy(writer.layout.bytes(at, payload.len), payload);
        writer.publish(at, payload.len);
    }

    /// `fipc_send_acquire` on the writer: room for a message of up to `len` bytes, in place in the data ring (64-byte
    /// aligned), for `sendCommit`. A later acquire or send drops the reservation.
    pub fn sendAcquire(writer: *Writer, endpoint: *Endpoint, len: usize, timeout_ms: c_int) DataError![]u8 {
        const at = try writer.reserve(endpoint, len, timeout_ms);
        writer.reservation.set(at, len);
        return writer.layout.bytes(at, len);
    }

    /// `fipc_send_commit` on the writer: publishes the first `len` bytes of the reservation. Invalid without one, or
    /// for a `len` of 0 or past it (the reservation stays).
    pub fn sendCommit(writer: *Writer, len: usize) DataError!void {
        writer.publish(try writer.reservation.commit(len), len);
    }

    /// The room for the next message's payload of `len` bytes, after it dropped a reservation: waits, within
    /// `timeout_ms`, for gated subscribers if a limit is in the way, announces the frontier before the payload is
    /// written, and takes the room (the skip to the ring's start too).
    fn reserve(writer: *Writer, endpoint: *Endpoint, len: usize, timeout_ms: c_int) DataError!u64 {
        writer.reservation.drop();
        if (len == 0) return error.Invalid;
        if (len > writer.layout.maxPiece()) return error.TooLarge;
        const placement = payload_ring.place(writer.payload_end, writer.layout.capacity, len, wire_protocol.broadcast_payload_align);
        if (writer.next >= writer.cell_limit or placement.end > writer.data_limit) {
            @branchHint(.unlikely);
            var deadline: Deadline = .init(timeout_ms);
            try writer.room(endpoint, placement.end, &deadline);
        }
        if (placement.end > writer.announced_frontier) {
            const header = writer.layout.header;
            writer.announced_frontier = std.mem.alignForward(u64, placement.end, writer.layout.capacity / wire_protocol.broadcast_chunks);
            header.frontier.store(writer.announced_frontier, .monotonic);
            header.next_mark.store(writer.next, .monotonic);
            // The frontier before the payload: a reader that copied any byte the payload overwrites sees it
            fence.storeFence();
        }
        writer.payload_end = placement.end;
        return placement.at;
    }

    /// `fipc_send_many` on the writer: sends the messages of `batch` (core/calls.zig checked them: 1 to `maxPiece`
    /// bytes each) in order, each written as `send` writes it, and publishes them together: one store of `next` and
    /// one look at the armed bits for the batch (`publishNext`), after its last message, and before any wait for room,
    /// so that the subscribers it waits for see what it wrote, and the sweep's `next` is the published one (the
    /// module's comment). The call's timeout bounds its waits together. `sent` is how many went: the message a wait
    /// stopped isn't counted, and every message before it is published.
    pub fn sendMany(writer: *Writer, endpoint: *Endpoint, batch: anytype, timeout_ms: c_int, sent: *usize) DataError!void {
        writer.reservation.drop();
        var deadline: Deadline = .init(timeout_ms);
        // Messages written since the last store of `next`
        var unpublished = false;
        defer if (unpublished) writer.publishNext();
        for (0..batch.count) |i| {
            const payload = batch.at(i);
            const placement = payload_ring.place(writer.payload_end, writer.layout.capacity, payload.len, wire_protocol.broadcast_payload_align);
            if (writer.next >= writer.cell_limit or placement.end > writer.data_limit) {
                if (unpublished) writer.publishNext();
                unpublished = false;
                try writer.room(endpoint, placement.end, &deadline);
            }
            // The room is there: the reserve doesn't wait
            const at = try writer.reserve(endpoint, payload.len, timeout_ms);
            copy(writer.layout.bytes(at, payload.len), payload);
            writer.writeMessage(at, payload.len);
            unpublished = true;
            sent.* = i + 1;
        }
    }

    /// Publishes the message whose payload is at `at`: its cell (the seqlock), then `next`, then the wakes the
    /// subscribers asked for.
    fn publish(writer: *Writer, at: u64, len: usize) void {
        writer.writeMessage(at, len);
        writer.publishNext();
    }

    /// The message whose payload is at `at`: its cell (the seqlock), its payload position in the writer's record, and
    /// the next message's request; `next` isn't stored (`publishNext`).
    inline fn writeMessage(writer: *Writer, at: u64, len: usize) void {
        writeCell(writer.layout.cell(writer.next), writer.next, at, @intCast(len));
        writer.payload_positions.record(writer.next, at);
        writer.next += 1;
        if (segment.prefetchesNext(len)) writer.prefetchNext(len);
    }

    /// Stores the published `next`, then wakes the subscribers that asked for it.
    inline fn publishNext(writer: *Writer) void {
        // `.seq_cst`: the owner starts joiners at it, and with the load of `armed_any` it is the writer's half of the
        // subscribers' sleep protocol (store, then load, against the subscriber's arm, then look)
        writer.layout.header.next.store(writer.next, .seq_cst);
        if (writer.layout.shared.armed_any.load(.seq_cst) != 0) writer.wakeSubscribers();
    }

    /// The writer's request, for writing, of the next message's lines, taken to be as long as this one (`len`): its
    /// cell, and its payload's room where `payload_ring.place` would put it; only below the limits, so that no line a
    /// gated subscriber hasn't read yet is taken from it. The subscribers read those lines last, and the store of
    /// `next` (a locked instruction on x86) waits for every store before it: requested one message early, they are the
    /// writer's by the time it writes them. Requested once this message is written, before that store, as the stream's
    /// sender requests its next frame (stream/data_path.zig `prefetchNextFrame`); for the payloads
    /// `segment.prefetchesNext` names. Out of line, so that a short message's publish stays as short as without it.
    noinline fn prefetchNext(writer: *const Writer, len: usize) void {
        if (writer.next < writer.cell_limit) @prefetch(writer.layout.cell(writer.next), .{ .rw = .write, .locality = 3, .cache = .data });
        const placement = payload_ring.place(writer.payload_end, writer.layout.capacity, len, wire_protocol.broadcast_payload_align);
        if (placement.end <= writer.data_limit) writer.layout.prefetchPayload(placement.at, placement.end - placement.at);
    }

    /// The writer's close (server.zig `closePublisher`), after its last message: marks the channel ended, then wakes
    /// every subscriber once (Linux, macOS: bumps `wake_gen` and wakes all its waiters; Windows: sets the wake of every
    /// slot held). Each subscriber then ends on its own (`stopped`), and its watcher's interrupt at the control
    /// connection's end needs no wake of its own (`wakeOwn`). A publisher that dies sets no mark: each subscriber's
    /// watcher reads the end of its control connection and wakes its own subscriber.
    pub fn endChannel(writer: *Writer) void {
        // `.seq_cst`, before the wake: a subscriber reads `wake_gen`, then arms and looks at the mark
        // (`announceSleep`); a slot wake set after the mark stays set for a wait that begins later
        writer.layout.header.ended.store(1, .seq_cst);
        if (!platform.wakes_each_slot) return wakeAll(writer.layout.shared);
        for (0..writer.layout.bitmapWords()) |word_index| writer.roster.slots.setWakes(word_index, writer.roster.slots.held[word_index].load(.acquire));
    }

    /// Takes the armed bits and wakes their subscribers (docs/protocol.md §9.5): on Windows each slot's wake, elsewhere
    /// every sleeper of `wake_gen` at once, after the bump. Bits of slots the owner doesn't hold are taken and dropped.
    noinline fn wakeSubscribers(writer: *Writer) void {
        const shared = writer.layout.shared;
        const words = writer.layout.bitmapWords();
        var any = shared.armed_any.swap(0, .seq_cst);
        var woken = false;
        while (any != 0) : (any &= any - 1) {
            const word_index: u32 = @ctz(any);
            if (word_index >= words) break;
            const taken = shared.armed[word_index].swap(0, .seq_cst) & writer.roster.slots.held[word_index].load(.acquire);
            if (taken == 0) continue;
            woken = true;
            if (platform.wakes_each_slot) writer.roster.slots.setWakes(word_index, taken);
        }
        if (!platform.wakes_each_slot and woken) wakeAll(shared);
    }

    /// Message `index`'s payload position, from `free_index` on (whose entries no later message has replaced: `next` is
    /// at most `free_index + cell_count`); at or past `next`, `payload_end`.
    fn payloadPositionOf(writer: *const Writer, index: u64) u64 {
        return writer.payload_positions.positionOf(index, writer.next, writer.payload_end);
    }

    /// Waits until every gated subscriber has passed the oldest message whose cell or payload room the send needs
    /// (the payload ends at `end`), and moves the limits.
    noinline fn room(writer: *Writer, endpoint: *Endpoint, end: u64, deadline: *Deadline) DataError!void {
        // The first message whose payload starts at most a ring before `end`: the ones before it hold room the
        // payload needs. `free_index` only grows, so this walk does too, across sends.
        var j = writer.free_index;
        while (j < writer.next and writer.payloadPositionOf(j) + writer.layout.capacity < end) j += 1;
        const need = @max(j, (writer.next + 1) -| writer.layout.cellCount());
        // What the sweeps wait for: more than `need` while the spins find it (`writer_spin_share`), `need` from the
        // first spin that doesn't, and for a call that doesn't wait
        var target = if (deadline.timeout_ms != 0) need + (writer.next - need) / writer_spin_share else need;
        var slice_ms = writer.first_slice_ms;
        var seen: struct { slot: u32, cursor: u64 } = .{ .slot = std.math.maxInt(u32), .cursor = 0 };
        while (writer.sweep(target)) |cursor| {
            // The slices start again from the first whenever the laggard or its cursor moves
            if (writer.scan.slot != seen.slot or cursor != seen.cursor) {
                seen = .{ .slot = writer.scan.slot, .cursor = cursor };
                slice_ms = writer.first_slice_ms;
            }
            try writer.waitForLaggard(endpoint, need, &target, deadline, &slice_ms);
        }
    }

    /// One pass of the sweep for `need`, resumed where the last one stopped: null once every gated cursor is at least
    /// `need`, with `free_index` and the limits moved; else the cursor of the laggard it stopped at, which `scan.slot`
    /// names.
    pub fn sweep(writer: *Writer, need: u64) ?u64 {
        writer.scan.prepare(need, writer.next);
        while (writer.nextGatedSlot(writer.scan.slot)) |slot| {
            const cursor = writer.layout.slots[slot].cursor.load(.seq_cst);
            if (cursor < need) {
                writer.scan.slot = slot;
                return cursor;
            }
            writer.scan.least_cursor = @min(writer.scan.least_cursor, cursor);
            writer.scan.slot = slot + 1;
        }
        writer.free_index = @min(writer.scan.least_cursor, writer.scan.published_next.?);
        writer.cell_limit = writer.free_index + writer.layout.cellCount();
        writer.data_limit = writer.payloadPositionOf(writer.free_index) + writer.layout.capacity;
        writer.scan = .{};
        // Nobody holds the writer back now: stalls start again at the next wait (round 0 is no round's)
        writer.round +%= 1;
        if (writer.round == 0) writer.round = 1;
        return null;
    }

    /// The first slot from `from` on whose gated bit is set in the segment and in the roster.
    fn nextGatedSlot(writer: *const Writer, from: u32) ?u32 {
        const max_subscribers = writer.layout.max_subscribers;
        var slot = from;
        while (slot < max_subscribers) {
            const word_index = slot / 64;
            // The segment's bit first (.seq_cst, in the order the join argument needs), then the owner's
            const in_segment = writer.layout.shared.gated[word_index].load(.seq_cst);
            const word = in_segment & writer.roster.gated[word_index].load(.acquire);
            const rest = word & (~@as(u64, 0) << @intCast(slot % 64));
            if (rest != 0) {
                const found = word_index * 64 + @ctz(rest);
                return if (found < max_subscribers) found else null;
            }
            slot = (word_index + 1) * 64;
        }
        return null;
    }

    /// Whether slot `slot` is still a gated subscriber the writer waits for.
    fn isGated(writer: *const Writer, slot: u32) bool {
        // Both loads, the segment's first, as `nextGatedSlot`
        const in_segment = hasBit(&writer.layout.shared.gated, slot, .seq_cst);
        const in_roster = hasBit(&writer.roster.gated, slot, .acquire);
        return in_segment and in_roster;
    }

    /// The writer's wait for the laggard the last sweep stopped at (docs/protocol.md §9.6): spin until its cursor
    /// reaches `target` (paced: `writer_spin_pace`), unless the call doesn't wait (a spin that doesn't lowers `target`
    /// to `need`); cancelled → Cancelled; each subscriber held still past `max_stall_ms` → reported, once; the call's
    /// timeout 0 or past → Timeout; then announce the sleep on `writer_sleeps` and sleep a slice at most.
    fn waitForLaggard(writer: *Writer, endpoint: *Endpoint, need: u64, target: *u64, deadline: *Deadline, slice_ms: *u32) DataError!void {
        const slot = writer.scan.slot;
        const cursor = &writer.layout.slots[slot].cursor;
        if (deadline.timeout_ms != 0) {
            const pace: usize = if (target.* > need) writer_spin_pace else 1;
            for (0..wait_module.spin_iters) |i| {
                if (i % pace == 0 and (cursor.load(.monotonic) >= target.* or !writer.isGated(slot))) return;
                std.atomic.spinLoopHint();
            }
            target.* = need;
        }
        if (endpoint.hot.requests.load(.acquire).cancel) return error.Cancelled;
        const left_ms = if (writer.max_stall_ms > 0) writer.checkStalls(need, Io.Clock.awake.now(endpoint.io)) else std.math.maxInt(u32);
        const sleep_ms = @min(try wait_module.sleepMsWithin(endpoint.io, deadline, slice_ms.*), left_ms);
        if (writer.announceRoomSleep(endpoint, slot, need) == .ready) return;
        _ = platform.ringWait(writer.layout.writerSleeps(), writer.layout.room_event, sleep_ms);
        try wait_module.backInIo(endpoint.io);
        slice_ms.* = @min(slice_ms.* *| 2, writer.longest_slice_ms);
    }

    /// The stall of every gated subscriber below `need`, the laggard and any other, as a wait for room sees it at
    /// `now`: it goes on, or starts with this wait; one still for `max_stall_ms` is reported to `on_stall`, once (at
    /// the next wait again if the owner couldn't act on it). The milliseconds until the next stall is due (at least 1),
    /// so that the writer's sleep ends then.
    fn checkStalls(writer: *Writer, need: u64, now: Io.Timestamp) u32 {
        var left: u32 = std.math.maxInt(u32);
        var from: u32 = 0;
        while (writer.nextGatedSlot(from)) |slot| : (from = slot + 1) {
            const cursor = writer.layout.slots[slot].cursor.load(.monotonic);
            if (cursor >= need) continue;
            const stall = &writer.stalls[slot];
            stall.observe(writer.round, writer.roster.slots.generations[slot].load(.monotonic), cursor, now);
            if (stall.reported) continue;
            if (stall.leftMs(now, writer.max_stall_ms)) |ms| {
                left = @min(left, ms);
            } else stall.reported = writer.on_stall(writer.context, slot, stall.generation);
        }
        return left;
    }

    /// The writer's side of its sleep protocol: sets `writer_sleeps`, then looks at the laggard's bit and cursor, and
    /// at `requests`, again. `.sleep` only if nothing changed; otherwise the flag is swapped back (a 1 a waker took
    /// meanwhile only ends the next sleep early).
    pub fn announceRoomSleep(writer: *const Writer, endpoint: *const Endpoint, slot: u32, need: u64) enum { ready, sleep } {
        const flag = writer.layout.writerSleeps();
        _ = flag.swap(1, .seq_cst);
        if (writer.layout.slots[slot].cursor.load(.seq_cst) >= need or !writer.isGated(slot) or endpoint.hot.requests.load(.seq_cst).cancel) {
            _ = flag.swap(0, .seq_cst);
            return .ready;
        }
        return .sleep;
    }
};

/// A sweep's progress, kept between its passes while a send waits: the next slot to look at, the least cursor seen,
/// the `next` it caps `free_index` with (null: not taken yet), the need it was made for.
const Scan = struct {
    slot: u32 = 0,
    least_cursor: u64 = std.math.maxInt(u64),
    published_next: ?u64 = null,
    need: u64 = 0,

    /// Readies the scan for `need`, `next` being the writer's. The cursors it visited still count for another need only
    /// if each is past it, and the `next` it took only while the need hasn't passed it (a smaller send got through
    /// meanwhile); else it starts over. That `next` (for joiners: the module's comment) is taken here, before the
    /// scan's first bitmap load, after the last `.seq_cst` store of `next`.
    fn prepare(scan: *Scan, need: u64, next: u64) void {
        if (scan.need != need and scan.least_cursor < need) scan.* = .{};
        if (scan.published_next) |published| {
            if (published < need) scan.* = .{};
        }
        scan.need = need;
        if (scan.published_next == null) scan.published_next = next;
    }
};

/// How long a slot's gated subscriber has been still while it held the writer back: since the first wait of a room
/// round (`Writer.round`) that found it below the need, at this cursor. It lasts across sends (sends with a timeout of
/// 0 that keep failing add up) until a sweep succeeds; the clock is read only by waits.
const Stall = struct {
    round: u32 = 0,
    generation: u32 = 0,
    cursor: u64 = 0,
    since: Io.Timestamp = .zero,
    /// The owner acted on the report: none again for this stall.
    reported: bool = false,

    /// A wait at `now` found the slot's subscriber (of generation `generation`) at `cursor`: another round, subscriber
    /// or cursor starts the stall again.
    fn observe(stall: *Stall, round: u32, generation: u32, cursor: u64, now: Io.Timestamp) void {
        if (stall.round == round and stall.generation == generation and stall.cursor == cursor) return;
        stall.* = .{ .round = round, .generation = generation, .cursor = cursor, .since = now };
    }

    /// Milliseconds until the subscriber has been still `max_stall_ms` (positive), so that the writer's sleep ends
    /// then; null once it has, by `now`.
    fn leftMs(stall: *const Stall, now: Io.Timestamp, max_stall_ms: i32) ?u32 {
        const left = @as(i96, max_stall_ms) * std.time.ns_per_ms - stall.since.durationTo(now).nanoseconds;
        if (left <= 0) return null;
        return wait_module.msRoundedUp(left, std.math.maxInt(u32));
    }
};

/// The writer's cell seqlock for message `index`: busy, a store fence, the fields, then the sequence (`.release`, so
/// that the payload and the fields come before it).
pub fn writeCell(cell: *wire_protocol.BroadcastCell, index: u64, at: u64, len: u32) void {
    const sequence = index + 1;
    cell.seq.store(sequence | busy, .monotonic);
    fence.storeFence();
    cell.payload_position.store(at, .monotonic);
    cell.len.store(len, .monotonic);
    cell.flags.store(0, .monotonic);
    cell.seq.store(sequence, .release);
}

// === Tests (more in writer_reader_test.zig and litmus_test.zig) ===

test "the stall tracker: another round, subscriber or cursor starts it again; due after max_stall" {
    const t0: Io.Timestamp = .fromNanoseconds(std.time.ns_per_s);
    const at = struct {
        fn ms(base: Io.Timestamp, n: i64) Io.Timestamp {
            return base.addDuration(.fromMilliseconds(n));
        }
    }.ms;
    var stall: Stall = .{};
    stall.observe(1, 1, 10, t0); // the first wait starts it
    try std.testing.expectEqual(@as(?u32, 1), stall.leftMs(at(t0, 99), 100));
    try std.testing.expectEqual(@as(?u32, 60), stall.leftMs(at(t0, 40), 100));
    stall.observe(1, 1, 10, at(t0, 50)); // the same: it goes on
    try std.testing.expectEqual(@as(?u32, null), stall.leftMs(at(t0, 100), 100));
    try std.testing.expectEqual(@as(?u32, null), stall.leftMs(at(t0, 500), 100));
    stall.reported = true;
    stall.observe(1, 1, 10, at(t0, 600));
    try std.testing.expect(stall.reported); // reported once per stall
    stall.observe(1, 1, 11, at(t0, 600)); // the cursor moved
    try std.testing.expect(!stall.reported and stall.leftMs(at(t0, 699), 100) != null and stall.leftMs(at(t0, 700), 100) == null);
    stall.observe(1, 2, 11, at(t0, 800)); // the slot's next subscriber
    try std.testing.expect(stall.leftMs(at(t0, 899), 100) != null);
    stall.observe(2, 2, 11, at(t0, 900)); // a sweep succeeded meanwhile: a new round
    try std.testing.expect(stall.leftMs(at(t0, 999), 100) != null and stall.leftMs(at(t0, 1000), 100) == null);
}
