//! A broadcast channel's subscribers' receives (docs/protocol.md §9.5): lossy (copy and check) and gated (in place,
//! with a cursor the writer waits for), and their sleep protocol. Everything here works on a mapped channel segment
//! (segment.zig `Layout`) and the subscriber's endpoint, whose `Io`, `requests` and `phase` it reads; the subscriber's
//! lifecycle (client.zig) sets it up. The writer's side, and how messages are published, is writer.zig.
//!
//! Lossy receive. A subscriber at `pos` loads the cell's `seq` (`.acquire`): `pos + 1` is its message; a larger one
//! (the busy bit aside) means the writer lapped it: `Overrun`, and it goes on from `next_mark`, a recent message. It
//! loads `payload_position` and `len`, a load fence, and `seq` again: changed, the writer rewrote the cell meanwhile
//! (`Overrun`). Then it copies the payload and checks, after a load fence, that the writer's `frontier` is still within
//! a ring of `payload_position`. The writer announces `frontier` before it writes there (writer.zig): a byte of the
//! payload can only be overwritten by a write at least a ring later, which comes after a frontier past
//! `payload_position + capacity`. So a frontier within `capacity` of `payload_position`, loaded after the copy, proves
//! the copy whole; otherwise `Overrun`. The copy reads memory the writer may be writing: under TSan it is the one
//! access the sanitizer is told to ignore (`seqlockCopy`).
//!
//! Gated receive. A gated subscriber's cursor (its slot's `cursor`) is the first index it hasn't consumed. The writer
//! never writes cell `i` before every gated cursor is past `i - cell_count + 1`, nor data a ring past a cursor's
//! message. So the subscriber reads its message in place, and stores its cursor (`.release`) once done.
//!
//! A receive takes the segment guard (`enterSegment`/`leaveSegment`) first, and holds it while it sleeps. The watcher
//! ends the subscription (client.zig `endSubscription`): it moves the phase off CONNECTED, wakes a sleeping receive,
//! and waits until no receive is inside the guard. Only then does it let the control connection end (the owner may give
//! the slot to the next joiner) or, if the owner dropped the subscriber (`unsubscribed`), make the segment
//! inaccessible. So:
//!   - a call that entered the guard while CONNECTED may write the slot (`live`); one that entered later writes nothing
//!     there; once the subscriber is dropped, a call touches nothing;
//!   - in place, the subscriber reads only while the writer protects the messages: while the subscription runs, or if
//!     it ended after the writer's close marked the channel ended (nothing writes it again);
//!   - after any other end (the owner's process ended, or the owner dropped the subscriber while its writer goes on),
//!     it drains what was published the way a lossy subscriber does, each message copied and checked
//!     (`recvChecked`), and reads nothing in place.
//!
//! Sleeps. A subscriber with nothing to read arms its slot in the armed bitmap, then looks at `next` again, and sleeps
//! only if it hasn't moved; after each send the writer takes the armed bits and wakes their subscribers (writer.zig
//! `wakeSubscribers`; docs/protocol.md §9.5 gives the argument). A subscriber's own interrupt (`wakeOwn`) acts only
//! when one of its receives announced a sleep (`sleeping`), because on Linux and macOS it can only wake every sleeper
//! of the channel at once. A gated subscriber stores its cursor on every receive (`.release`) and wakes the writer if
//! its flag is set; only before its own sleep is that pair sequentially consistent (writer.zig says what that costs the
//! writer).
//!
//! A subscriber's view has what only the writer writes read-only (segment.zig `attach`): nothing here writes
//! there. A corrupt peer can still write anything in the segment. Whatever is read from it and indexes or sizes
//! anything is checked here: a cell's `payload_position` and `len`, `next` and `next_mark` (`max_index`). A bad value
//! fails that subscriber's call (`Invalid`), never an access out of the segment.

const std = @import("std");
const builtin = @import("builtin");
const build_options = @import("build_options");
const wire_protocol = @import("../wire_protocol.zig");
const platform = @import("../platform.zig");
const copy = @import("../core/copy.zig").copy;
const wait_module = @import("../core/wait.zig");
const fence = @import("../core/fence.zig");
const segment = @import("segment.zig");

const Atomic = std.atomic.Value;
const Endpoint = @import("../core/endpoint.zig").Endpoint;
const Phase = @import("../core/endpoint.zig").Phase;
const Deadline = wait_module.Deadline;
const Layout = segment.Layout;
const busy = wire_protocol.broadcast_busy;
const bitOf = segment.bitOf;
const setBit = segment.setBit;
const clearBit = segment.clearBit;
const wakeWriter = segment.wakeWriter;
const wakeAll = segment.wakeAll;

const DataError = @import("../core/endpoint.zig").DataError;

/// Indices from here on are a corrupt peer's. A subscriber that finds `next` or `next_mark` there fails (Invalid) and
/// never takes its position from them, so its position stays below it; it is computed with wrapping arithmetic all the
/// same.
const max_index = wire_protocol.broadcast_max_index;

/// What the cell of message `pos` shows, by its sequence word: the message (`here`), a later one (`lapped`), or neither
/// yet (`pending`: an earlier message, or the message while the writer writes it).
fn cellState(seq: u64, pos: u64) enum { pending, here, lapped } {
    const expected_sequence = pos +% 1;
    if (seq == expected_sequence) return .here;
    return if (seq & ~busy > expected_sequence) .lapped else .pending;
}

/// One subscriber's receive state: private to its receiving thread, but for `in_segment`, which its watcher reads.
pub const Reader = struct {
    /// The segment guard (gated subscribers): 1 while a call of the subscriber may touch the segment.
    in_segment: Atomic(u32) align(64) = .init(0),
    /// Linux, macOS: 1 from a receive's announcement of its sleep until it woke, for `wakeOwn`, which reads it on
    /// another thread.
    sleeping: Atomic(u32) = .init(0),
    layout: Layout,
    /// The next message's index.
    pos: u64,
    /// The number of messages the last Overrun skipped (`overrun`), for the native module's `Conn.lostMessages`.
    lost: u64 = 0,
    slot: u32,
    /// The slot's generation the owner offered, which its frames to this subscriber name (the join sets it).
    generation: u32 = 0,
    gated: bool,
    /// A gated subscriber holds message `pos` in place (`recvAcquire`) until `release`.
    acquired: bool = false,
    /// The phase was CONNECTED when the current call entered the segment guard (`enterSegment`): it may write the slot.
    live: bool = false,
    /// For a gated subscriber whose subscription ended without the writer's close: where its drain ends. It is set to
    /// the published `next` when the watcher ends the subscription (`drop`), then to `pos` once nothing more checks out
    /// (`recvChecked`).
    drain_end: Atomic(u64) = .init(0),
    /// The writer's close had marked the channel ended when the subscription ended (`drop`): what was published stays
    /// as it was, and a gated subscriber drains it in place. A mark set later protects nothing the writer may have
    /// overwritten meanwhile.
    channel_ended_first: Atomic(bool) = .init(false),
    /// The owner dropped this subscriber, and its watcher made the mapping inaccessible (client.zig `unsubscribe`):
    /// nothing touches the segment any more. Under the endpoint mutex.
    revoked: bool = false,
    /// The subscriber's slot wake (Windows), which its waits wait on.
    own_wake: platform.SlotWake,
    /// The hints the next spin leaves before its first look (`spin`).
    pace: u32 = 1,
    /// The hints of the spin before a sleep: the default, which only tests change.
    spin_iters: u32 = wait_module.spin_iters,
    /// The longest sleep: the default, which only tests change.
    longest_sleep_ms: u32 = wait_module.slice_ms,
    /// The sleeps the receives began, counted in the tests' builds only: a test that sees it move knows the receive
    /// is past every look before its sleep, so that only a wake ends the sleep before its time.
    sleeps: Atomic(u32) = .init(0),

    /// A subscriber of slot `slot`, lossy or `gated`, from the start the owner stored in its slot before WELCOME
    /// (writer.zig `startSlot`), which a gated subscriber's cursor and bit protect already. Invalid: the start is a
    /// corrupt peer's (a gated subscriber's bit is cleared again).
    pub fn init(layout: Layout, slot: u32, gated: bool, own_wake: platform.SlotWake) DataError!Reader {
        const start = layout.slots[slot].cursor.load(.acquire);
        if (start >= max_index) {
            if (gated) clearBit(&layout.shared.gated, slot, .seq_cst);
            return error.Invalid;
        }
        return .{ .layout = layout, .pos = start, .slot = slot, .gated = gated, .own_wake = own_wake };
    }

    /// A gated subscriber's leave (its `close`): its bit, then a wake of the writer, which may wait for it; under the
    /// guard, and nothing once its subscription ended (its slot may be another's, its mapping revoked).
    pub fn leaveChannel(reader: *Reader, endpoint: *const Endpoint) void {
        if (!reader.gated) return;
        reader.enterSegment(endpoint) catch return;
        defer reader.leaveSegment();
        // Its subscription ended: the slot may be another subscriber's already
        if (!reader.live) return;
        clearBit(&reader.layout.shared.gated, reader.slot, .seq_cst);
        wakeWriter(&reader.layout);
    }

    /// `fipc_recv`: the next message, copied into `buf`, its length in `len`. TooLarge (and its length in `len`)
    /// for a `buf` too small: nothing is consumed. Overrun (and the number of messages lost in `len`): a lossy
    /// subscriber fell a lap behind.
    pub fn recv(reader: *Reader, endpoint: *Endpoint, buf: []u8, timeout_ms: c_int, len: *usize) DataError!void {
        var deadline: Deadline = .init(timeout_ms);
        if (!reader.gated) return reader.recvLossy(endpoint, buf, &deadline, len);
        try reader.enterSegment(endpoint);
        defer reader.leaveSegment();
        const piece = (try reader.nextInPlace(endpoint, &deadline)) orelse return reader.recvChecked(buf, len);
        len.* = piece.len;
        if (piece.len > buf.len) return error.TooLarge;
        copy(buf[0..piece.len], piece);
        reader.pos +%= 1;
        reader.publishCursor();
    }

    /// `fipc_recv_many`: the next message as `recv` takes it (its TooLarge and Overrun too), then the further messages
    /// the writer has moved past that fit the rest of `buf`, up to `lens.len`, packed back to back, each one's length
    /// in `lens`; `count` is how many. Moved past: below `next_mark` (`movedPast`), the first message of the sixteenth
    /// of the data ring the writer writes now, whose cells and payloads before it the writer doesn't write again for a
    /// lap. So the subscriber reads only lines the writer has left. A look at the newest messages (at `next`, or at
    /// the cells after the first) takes the lines the writer is writing from it: with four subscribers of 16 B on
    /// Linux x86 such a batched receive ran at half the rate of one message per call, and this one runs faster than
    /// that. The newest messages come in the next calls. It stops, without waiting, at anything else: a message too
    /// long, one a lossy subscriber's copy finds overwritten (the next receive's Overrun). A gated subscriber publishes
    /// its cursor once, after the last; one whose in-place reads lost their protection drains one message per call,
    /// as `recv` does.
    pub fn recvMany(reader: *Reader, endpoint: *Endpoint, buf: []u8, lens: []usize, timeout_ms: c_int, count: *usize) DataError!void {
        var deadline: Deadline = .init(timeout_ms);
        if (!reader.gated) {
            try reader.recvLossy(endpoint, buf, &deadline, &lens[0]);
            count.* = reader.takeFurtherLossy(buf, lens, reader.movedPast());
            return;
        }
        try reader.enterSegment(endpoint);
        defer reader.leaveSegment();
        const piece = (try reader.nextInPlace(endpoint, &deadline)) orelse {
            try reader.recvChecked(buf, &lens[0]);
            count.* = 1;
            return;
        };
        lens[0] = piece.len;
        if (piece.len > buf.len) return error.TooLarge;
        copy(buf[0..piece.len], piece);
        reader.pos +%= 1;
        var used = piece.len;
        var taken: usize = 1;
        const end = reader.movedPast();
        while (taken < lens.len and reader.pos < end) {
            const cell = reader.layout.cell(reader.pos);
            if (cellState(cell.seq.load(.acquire), reader.pos) != .here) break;
            // The writer doesn't write this cell or payload again before the cursor has passed it
            const payload_position = cell.payload_position.load(.monotonic);
            const len = cell.len.load(.monotonic);
            if (!reader.layout.validPiece(payload_position, len) or len > buf.len - used) break;
            copy(buf[used..][0..len], reader.layout.bytes(payload_position, len));
            reader.pos +%= 1;
            lens[taken] = len;
            used += len;
            taken += 1;
        }
        reader.publishCursor();
        count.* = taken;
    }

    /// A lossy subscriber's further messages after the first of a `recvMany`, whose length is `lens[0]`: each one there
    /// below `end`, copied and checked as `recvLossy` does, while it fits; the count, the first included.
    fn takeFurtherLossy(reader: *Reader, buf: []u8, lens: []usize, end: u64) usize {
        var used = lens[0];
        var taken: usize = 1;
        while (taken < lens.len and reader.pos < end) {
            const cell = reader.layout.cell(reader.pos);
            if (cellState(cell.seq.load(.acquire), reader.pos) != .here) break;
            var len: usize = undefined;
            const outcome = reader.copyChecked(cell, buf[used..], &len) catch break; // too long for the rest
            if (outcome != .whole) break;
            lens[taken] = len;
            used += len;
            taken += 1;
        }
        return taken;
    }

    /// The end of the messages the writer has moved past, for `recvMany`: `next_mark`, which it stores as it moves into
    /// the next sixteenth of the data ring (writer.zig `reserve`); `pos` (none) if it is a corrupt peer's. The writer
    /// stores the frontier beside it, on the line a lossy subscriber's check reads anyway, and rarely writes either.
    fn movedPast(reader: *const Reader) u64 {
        const mark = reader.layout.header.next_mark.load(.acquire);
        return if (mark < max_index) mark else reader.pos;
    }

    /// `fipc_recv_acquire` (gated subscribers; Invalid on a lossy one): the next message in place, which the
    /// subscriber holds, and so holds the writer back, until `release` or its next receive. Disconnected once the
    /// writer no longer protects what it would hold (`nextInPlace`).
    pub fn recvAcquire(reader: *Reader, endpoint: *Endpoint, timeout_ms: c_int) DataError![]const u8 {
        if (!reader.gated) return error.Invalid;
        var deadline: Deadline = .init(timeout_ms);
        try reader.enterSegment(endpoint);
        defer reader.leaveSegment();
        const piece = (try reader.nextInPlace(endpoint, &deadline)) orelse return error.Disconnected;
        reader.acquired = true;
        return piece;
    }

    /// `fipc_recv_release`: lets the held message go. Nothing without one, or once the subscriber was dropped.
    pub fn release(reader: *Reader, endpoint: *const Endpoint) void {
        if (!reader.acquired) return;
        reader.enterSegment(endpoint) catch {
            reader.acquired = false;
            return;
        };
        defer reader.leaveSegment();
        reader.releaseHeld();
    }

    fn releaseHeld(reader: *Reader) void {
        if (!reader.acquired) return;
        reader.acquired = false;
        reader.pos +%= 1;
        reader.publishCursor();
    }

    /// The gated subscriber's progress, on every receive and release, so that a writer never waits for one that has
    /// moved on: its cursor, then a wake of the writer if its flag is set. The flag load is `.monotonic`: the
    /// sequentially consistent pair of cursor and flag runs only before a sleep (`announceSleep`; the module's
    /// comment). Nothing once its subscription ended (`live`): the slot may be another subscriber's.
    inline fn publishCursor(reader: *const Reader) void {
        if (!reader.live) return;
        reader.layout.slots[reader.slot].cursor.store(reader.pos, .release);
        if (reader.layout.writerSleeps().load(.monotonic) != 0) wakeWriter(&reader.layout);
    }

    /// The segment guard's entry (the module's comment): sets `in_segment`, then looks at the phase: a dropped
    /// subscriber touches nothing (Unsubscribed), and only a call that sees CONNECTED may write the slot (`live`) until
    /// it leaves. Sequentially consistent against the subscriber's watcher, which sets the phase, then looks at
    /// `in_segment`, and lets the subscription's end go further only once it is 0. Lossy subscribers write nothing a
    /// later subscriber of the slot depends on, and take no guard.
    pub fn enterSegment(reader: *Reader, endpoint: *const Endpoint) DataError!void {
        if (!reader.gated) return;
        reader.in_segment.store(1, .seq_cst);
        const phase = endpoint.hot.phase.load(.seq_cst);
        if (phase == .unsubscribed) {
            reader.in_segment.store(0, .release);
            return error.Unsubscribed;
        }
        reader.live = phase == .connected;
    }

    pub fn leaveSegment(reader: *Reader) void {
        if (reader.gated) reader.in_segment.store(0, .release);
    }

    /// Ends the subscription, as the watcher applies it (client.zig `endSubscription`). For PEER_DEAD it first records
    /// where the drain ends (`drain_end`: the current `next`) and whether the channel was already marked ended
    /// (`channel_ended_first`). Then it stores the phase (`.seq_cst`; a call's look at the phase acquires it) and looks
    /// at `in_segment`.
    ///   - True: no call is inside the segment guard, and none that enters later writes the segment (or, once
    ///     unsubscribed, touches it).
    ///   - False: a call is inside, perhaps asleep (the caller wakes it); `outside` tells when it has left.
    pub fn drop(reader: *Reader, endpoint: *Endpoint, phase: Phase) bool {
        if (phase == .peer_dead) {
            reader.channel_ended_first.store(reader.channelEnded(), .monotonic);
            const next = reader.layout.header.next.load(.acquire);
            reader.drain_end.store(if (next < max_index) next else 0, .monotonic);
        }
        endpoint.hot.phase.store(phase, .seq_cst);
        return reader.outside();
    }

    /// After `drop`: no call is inside the segment guard.
    pub fn outside(reader: *const Reader) bool {
        return reader.in_segment.load(.seq_cst) == 0;
    }

    /// The lossy receive (the module's comment).
    fn recvLossy(reader: *Reader, endpoint: *Endpoint, buf: []u8, deadline: *Deadline, len: *usize) DataError!void {
        while (true) {
            const cell = reader.layout.cell(reader.pos);
            switch (cellState(cell.seq.load(.acquire), reader.pos)) {
                .here => return switch (try reader.copyChecked(cell, buf, len)) {
                    .whole => {},
                    .torn => reader.overrun(len),
                    .corrupt => reader.corrupt(),
                },
                .lapped => return reader.overrun(len),
                .pending => _ = try reader.wait(endpoint, deadline), // a lossy subscriber's waits end `.look`
            }
        }
    }

    /// The gated subscriber's next message, in place, after it let a held one go; under the guard. Null once the writer
    /// may no longer protect it: the subscription ended without the writer's close (the owner's process ended, or the
    /// owner dropped this subscriber while its writer may go on).
    fn nextInPlace(reader: *Reader, endpoint: *Endpoint, deadline: *Deadline) DataError!?[]u8 {
        reader.releaseHeld();
        if (!reader.live and !reader.channel_ended_first.load(.monotonic)) return null;
        while (true) {
            const cell = reader.layout.cell(reader.pos);
            switch (cellState(cell.seq.load(.acquire), reader.pos)) {
                .here => {
                    // The writer doesn't write this cell or payload again before the cursor has passed it
                    const payload_position = cell.payload_position.load(.monotonic);
                    const len = cell.len.load(.monotonic);
                    if (!reader.layout.validPiece(payload_position, len)) return error.Invalid;
                    return reader.layout.bytes(payload_position, len);
                },
                // A gated subscriber is never lapped: a later message in its cell is a corrupt peer's
                .lapped => return error.Invalid,
                .pending => if (try reader.wait(endpoint, deadline) == .unprotected) return null,
            }
        }
    }

    /// `fipc_recv` of a gated subscriber whose in-place reads lost their protection (`nextInPlace`): the message at
    /// `pos`, if it was published before the subscription ended (`drain_end`), copied and checked as a lossy
    /// subscriber's (the module's comment). Anything else (none there, one the writer overwrote, a corrupt cell) is
    /// Disconnected, from then on.
    fn recvChecked(reader: *Reader, buf: []u8, len: *usize) DataError!void {
        if (reader.pos < reader.drain_end.load(.monotonic)) {
            const cell = reader.layout.cell(reader.pos);
            if (cell.seq.load(.acquire) == reader.pos +% 1 and try reader.copyChecked(cell, buf, len) == .whole) return;
        }
        reader.drain_end.store(reader.pos, .monotonic);
        return error.Disconnected;
    }

    /// Copies message `pos`, whose cell `cell` showed it (`.acquire`), into `buf`, and checks it (the module's
    /// comment):
    ///   - `whole`: copied, and `pos` moves to the next message;
    ///   - `torn`: the writer rewrote the cell, or may have overwritten the payload, meanwhile;
    ///   - `corrupt`: payload fields no writer writes.
    /// TooLarge, with the length in `len`, for a `buf` too small.
    fn copyChecked(reader: *Reader, cell: *const wire_protocol.BroadcastCell, buf: []u8, len: *usize) DataError!enum { whole, torn, corrupt } {
        const expected_sequence = reader.pos +% 1;
        const piece = readCell(cell, expected_sequence) orelse return .torn;
        if (!reader.layout.validPiece(piece.payload_position, piece.len)) return .corrupt;
        len.* = piece.len;
        if (piece.len > buf.len) return error.TooLarge;
        seqlockCopy(buf[0..piece.len], reader.layout.bytes(piece.payload_position, piece.len));
        // After the copy's loads: a frontier past a ring from the payload means the writer may have overwritten part
        // of what was copied
        fence.loadFence();
        if (reader.layout.header.frontier.load(.monotonic) -% piece.payload_position > reader.layout.capacity) return .torn;
        reader.pos = expected_sequence;
        return .whole;
    }

    /// Overrun: the subscriber goes on from `next_mark`, a recent message (`resync`), and `len` (and `lost`) is the
    /// number of messages it skipped, the one at `pos` included: at least 1. Invalid instead where `next` or
    /// `next_mark` is a corrupt peer's.
    fn overrun(reader: *Reader, len: *usize) DataError {
        const before = reader.pos;
        if (!reader.resync()) return error.Invalid;
        reader.lost = reader.pos - before;
        len.* = @intCast(reader.lost);
        return error.Overrun;
    }

    /// A cell or payload that no writer writes: Invalid, and the subscriber goes on as after an overrun.
    fn corrupt(reader: *Reader) DataError {
        _ = reader.resync();
        return error.Invalid;
    }

    /// Moves `pos` to `next_mark`, clamped to the published messages after `pos`: a subscriber that is a lap behind
    /// goes on from a recent message at once, not one lap at a time. If `next` or `next_mark` is past `max_index`, they
    /// are a corrupt peer's: it returns false and moves `pos` on by one only, so `pos` stays below `max_index` whatever
    /// the segment says.
    fn resync(reader: *Reader) bool {
        const mark = reader.layout.header.next_mark.load(.acquire);
        const next = reader.layout.header.next.load(.acquire);
        const least = reader.pos +% 1;
        if (next >= max_index or mark >= max_index) {
            reader.pos = least;
            return false;
        }
        reader.pos = std.math.clamp(mark, least, @max(least, next));
        return true;
    }

    /// Whether the message at `pos` is there to look at: complete, or lapped.
    fn arrived(reader: *const Reader) bool {
        return cellState(reader.layout.cell(reader.pos).seq.load(.acquire), reader.pos) != .pending;
    }

    /// How a wait ended: look at the message again, or (gated) the writer may no longer protect it (`nextInPlace`).
    const Wake = enum { look, unprotected };

    /// Waits until the message at `pos` may be there. Each round, in order:
    ///   - spin, or, for a call that doesn't wait, the poll's pause (`pollPaced`);
    ///   - what ends the wait without a sleep (`stopped`): Unsubscribed, Cancelled, Disconnected, `.unprotected`, or
    ///     the message there after all (what the writer published before it ended is drained);
    ///   - the call's timeout is 0 or past: Timeout;
    ///   - arm, look again, and sleep.
    /// A gated subscriber sleeps inside the segment guard: the watcher that ends its subscription wakes it before it
    /// waits for the guard (client.zig `endSubscription`).
    fn wait(reader: *Reader, endpoint: *Endpoint, deadline: *Deadline) DataError!Wake {
        if (if (deadline.timeout_ms != 0) reader.spin() else reader.pollPaced()) return .look;
        switch (try reader.stopped(endpoint)) {
            .runs => {},
            .look => return .look,
            .unprotected => return .unprotected,
        }
        const sleep_ms = try wait_module.sleepMsWithin(endpoint.io, deadline, reader.longest_sleep_ms);
        defer if (!platform.wakes_each_slot) reader.sleeping.store(0, .monotonic);
        const wake_generation = (try reader.announceSleep(endpoint)) orelse return .look;
        if (build_options.test_hooks) _ = reader.sleeps.fetchAdd(1, .seq_cst);
        _ = platform.broadcastWait(&reader.layout.shared.wake_gen, wake_generation, reader.own_wake, sleep_ms);
        try wait_module.backInIo(endpoint.io);
        // Dropped meanwhile: nothing more, not even a message that came
        if (reader.gated and endpoint.hot.phase.load(.acquire) == .unsubscribed) return error.Unsubscribed;
        return .look;
    }

    /// A subscriber's spin: true once the message at `pos` may be there. It spends the first `pace` hints without a
    /// look, then looks after every hint. A look pulls the cell's line from the writer, which must win it back to
    /// publish, so a subscriber that has caught up with a stream of small messages and looks after every hint slows its
    /// writer (stream/ring_wait.zig `spin`).
    ///
    /// A ring's reader learns from its own sends whether it waits for a stream: after it sent, it waits for an answer
    /// and spins unpaced. A subscriber sends nothing, so it learns from its spins:
    ///   - if the first look finds the message, the writer is publishing steadily, and the next spin waits twice as
    ///     long before its first look, up to `stream_pace` (core/wait.zig);
    ///   - if the spin had to look again, it was waiting for the writer (an answer, an occasional message), and the
    ///     next spin looks after one hint.
    /// So a stream is paced after a few messages, and a message that comes a while into a wait is seen within a hint of
    /// its publish. Looking after every hint in a long wait costs nothing: the line stays in this core's cache until
    /// the writer writes it.
    pub fn spin(reader: *Reader) bool {
        const cell = reader.layout.cell(reader.pos);
        const first = @min(reader.pace, reader.spin_iters);
        for (0..first) |_| std.atomic.spinLoopHint();
        for (first..reader.spin_iters) |i| {
            if (cellState(cell.seq.load(.monotonic), reader.pos) != .pending) {
                reader.pace = if (i == first) @min(reader.pace *| 2, wait_module.stream_pace) else 1;
                return true;
            }
            std.atomic.spinLoopHint();
        }
        reader.pace = 1;
        return false;
    }

    /// A call that doesn't wait, with nothing at `pos`: true once a short pause finds the message there after all. Its
    /// hints are the spin's `pace`, learned the same way: a pause that finds the message means a steady stream, and the
    /// next one waits twice as long, up to `stream_pace`; one that finds nothing means none, and the next takes one
    /// hint. Subscribers that poll a stream of small messages in a loop would otherwise pull the cell's line from the
    /// writer on every empty poll, as an unpaced spin does (on x86, the polling broadcast of 16 B to 4 subscribers runs
    /// 20-30% faster paced). So an empty poll returns after one hint while nothing comes, and after up to `stream_pace`
    /// hints (1 to 2.5 us) in a steady stream.
    fn pollPaced(reader: *Reader) bool {
        for (0..reader.pace) |_| std.atomic.spinLoopHint();
        if (reader.arrived()) {
            reader.pace = @min(reader.pace *| 2, wait_module.stream_pace);
            return true;
        }
        reader.pace = 1;
        return false;
    }

    /// What ends the wait without a sleep, in order:
    ///   - Unsubscribed once dropped; Cancelled after `fipc_cancel`;
    ///   - once the publisher has ended (its watcher saw the end, or the writer's close marked the channel ended):
    ///     `.unprotected` for a gated subscriber whose subscription ended before that mark (`channel_ended_first`,
    ///     `nextInPlace`); otherwise `.look` if the message is there after all (looked at after the phase and the
    ///     mark), else Disconnected;
    ///   - `.runs` while the channel runs.
    fn stopped(reader: *const Reader, endpoint: *const Endpoint) DataError!enum { runs, look, unprotected } {
        const phase = endpoint.hot.phase.load(.acquire);
        if (phase == .unsubscribed) return error.Unsubscribed;
        if (endpoint.hot.requests.load(.acquire).cancel) return error.Cancelled;
        if (phase == .peer_dead) {
            if (reader.gated and !reader.channel_ended_first.load(.monotonic)) return .unprotected;
        } else if (!reader.channelEnded()) return .runs;
        return if (reader.arrived()) .look else error.Disconnected;
    }

    /// The writer's close marked the channel ended (`Writer.endChannel`): after its last message, which this load
    /// (.acquire) makes visible.
    pub fn channelEnded(reader: *const Reader) bool {
        return reader.layout.header.ended.load(.acquire) != 0;
    }

    /// The subscriber's side of its sleep protocol (docs/protocol.md §9.5), all `.seq_cst`. In order:
    ///   1. announce the sleep to `wakeOwn`;
    ///   2. read `wake_gen`;
    ///   3. arm the slot: its bit, then its word's bit in `armed_any`;
    ///   4. a gated subscriber publishes its cursor and wakes a sleeping writer (the exact pair; its call is CONNECTED,
    ///      as `stopped` checked);
    ///   5. look again at `next`, `requests`, the phase and the ended mark.
    /// Returns the `wake_gen` to sleep on, or null when something changed and the caller looks again (the slot stays
    /// armed; a later send takes it). A `next` past `pos` with no message there is a corrupt peer's: Invalid.
    pub fn announceSleep(reader: *Reader, endpoint: *const Endpoint) DataError!?u32 {
        const shared = reader.layout.shared;
        // First (.seq_cst, as the looks below): an interrupt that missed it published what those looks see
        if (!platform.wakes_each_slot) reader.sleeping.store(1, .seq_cst);
        const wake_generation = shared.wake_gen.load(.seq_cst);
        setBit(&shared.armed, reader.slot, .seq_cst);
        _ = shared.armed_any.fetchOr(bitOf(reader.slot / 64), .seq_cst);
        if (reader.gated) {
            reader.layout.slots[reader.slot].cursor.store(reader.pos, .seq_cst);
            if (reader.layout.writerSleeps().load(.seq_cst) != 0) wakeWriter(&reader.layout);
        }
        if (reader.layout.header.next.load(.seq_cst) > reader.pos) {
            // Its cell was stored before `next`: it shows the message, or a later one
            if (!reader.arrived()) return if (reader.gated) error.Invalid else reader.corrupt();
            return null;
        }
        if (endpoint.hot.requests.load(.seq_cst).cancel) return null;
        if (endpoint.hot.phase.load(.seq_cst) != .connected) return null;
        // After the arm, as `next`: a mark stored after this look comes before the writer's bump of `wake_gen`
        if (reader.layout.header.ended.load(.seq_cst) != 0) return null;
        return wake_generation;
    }

    /// The local interrupt of this subscriber's wait (cancel, the subscription's end), after the caller published what
    /// the wait must see (`.seq_cst`). On Windows it sets the slot's own wake. Elsewhere it can only bump `wake_gen`
    /// and wake every sleeper of the channel (the others look again and sleep on), so it does that only when both
    /// hold:
    ///   - the receiving thread announced its sleep (`sleeping`, which `announceSleep` sets before its looks: either
    ///     this load sees it, or those looks see what the caller published);
    ///   - the writer's close hasn't marked the channel ended (that close woke every sleeper after the mark, and no
    ///     receive sleeps again once it has seen the mark: `Writer.endChannel`).
    /// The caller never calls it on a revoked mapping.
    pub fn wakeOwn(reader: *const Reader) void {
        if (platform.wakes_each_slot) return platform.setSlotWake(reader.own_wake);
        if (reader.sleeping.load(.seq_cst) == 0 or reader.channelEnded()) return;
        wakeAll(reader.layout.shared);
    }
};

/// A cell's payload fields, read under its seqlock.
pub const Piece = struct { payload_position: u64, len: u32 };

/// The fields of a cell whose `seq` was just loaded as `expected_sequence` (`.acquire`), and loaded again after them:
/// null if the writer started rewriting the cell meanwhile.
pub fn readCell(cell: *const wire_protocol.BroadcastCell, expected_sequence: u64) ?Piece {
    const piece: Piece = .{ .payload_position = cell.payload_position.load(.monotonic), .len = cell.len.load(.monotonic) };
    fence.loadFence();
    if (cell.seq.load(.monotonic) != expected_sequence) return null;
    return piece;
}

/// The lossy receive's copy, of memory the writer may be writing meanwhile (the frontier check tells): under TSan the
/// one race of the library meant to be, which the sanitizer is told to ignore. Its bracket ignores every access of
/// this thread inside it, interceptors' too; the cells, the frontier, the cursors and the wake words outside it are
/// atomics, and stay checked.
fn seqlockCopy(dst: []u8, src: []const u8) void {
    if (builtin.sanitize_thread) {
        const here = @src();
        tsan.AnnotateIgnoreReadsBegin(here.file, @intCast(here.line));
        defer tsan.AnnotateIgnoreReadsEnd(here.file, @intCast(here.line));
        copy(dst, src);
    } else copy(dst, src);
}

/// TSan's annotations (its runtime's `tsan_interface_ann.cpp`), linked only into a TSan build.
const tsan = struct {
    extern fn AnnotateIgnoreReadsBegin(file: [*:0]const u8, line: c_int) void;
    extern fn AnnotateIgnoreReadsEnd(file: [*:0]const u8, line: c_int) void;
};
