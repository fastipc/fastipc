//! Ring waits and wake-ups (docs/protocol.md §6): how a side that must wait for its ring sleeps, and how the
//! other side, or this side's own session end or cancel, wakes it. The one place for both; stream/data_path.zig calls
//! `until` and `publishHead`/`consume`, the endpoint `wakeOwnWaiters`.
//!
//! Each ring index has a sleep flag beside it: the reader sleeps on `reader_sleeps` while it waits for `head` to move,
//! the writer on `writer_sleeps` while it waits for `tail` to move. The two sides pair up through the flag, and while a
//! session runs every change of a flag is an atomic swap (`.acq_rel`):
//!   - the publisher stores the index (`.release`), then swaps the flag to 0, and wakes the sleeper if it was 1;
//!   - the waiter swaps its flag to 1, then loads the index and the endpoint's `requests` and `phase` (`.acquire`),
//!     and sleeps only if none of them changed; otherwise it swaps the flag back to 0 and acts on what it saw.
//!
//! Why no wake-up is lost, in the language memory model. The swaps of one flag are totally ordered (its modification
//! order), and each swap reads the value of the one just before it. Two cases:
//!   - The publisher's swap comes after the waiter's. It reads the waiter's 1 and wakes the waiter. Or it reads the 0
//!     of whoever took that 1 first, which woke the waiter (or was the waiter itself, giving up its sleep).
//!   - The publisher's swap comes before the waiter's. The waiter's swap reads from it, or from a later swap that
//!     continues its release sequence, so the publisher's swap synchronizes with the waiter's. The index store then
//!     happens before the waiter's index load, which sees it, and the waiter doesn't sleep.
//! The kernel covers the last step: `FUTEX_WAIT(flag, 1)` doesn't sleep once the flag is 0, and an auto-reset event
//! stays signaled until a wait consumes it. The waiter must set its flag with a swap, not a plain `.release` store: on
//! x86 its index load could pass such a store, and a receive would sleep a whole slice with its data waiting (the
//! litmus test below shows it).
//!
//! A batch of sends stores the head after each frame and swaps the flag once, after the last and before any wait for
//! room (`storeHead`, `wakeReader`): the swap that follows a run of stores covers every one of them, as it covers one.
//!
//! A publish of `head` costs one locked instruction, the flag's `xchg` after a plain store of the index. A `.seq_cst`
//! store of the index (an `xchg`) followed by a plain flag load measures 5-9% slower with 1 KiB messages. The waiter's
//! `xchg` is on its slow path.
//!
//! The room's wake is inexact, as a ring set's is (core/sender.zig): a reader's publish of `tail` loads the writer's
//! flag (`.monotonic`) and swaps it only if it read 1. A locked instruction for every message the reader consumes
//! costs a quarter of the small-message rate on x86, and its round trip some 30-40 ns, while a writer waits for room
//! only on a full ring. A writer that announces its sleep just as the reader's look passes can miss its wake; the
//! reader's exact swap closes that gap on its own slow path, after a spin for data finds none and before it sleeps
//! (`sleepUntil`), so the writer sleeps at most until the reader next waits. A reader that stops receiving altogether
//! leaves it to the writer's slice, which starts at 1 ms and doubles up to `wait.slice_ms` (`space_first_slice_ms`).
//!
//! The same rule wakes this side's own sleepers after `fipc_cancel` or the session's end (docs/protocol.md §6.2): the
//! change is published (`requests`, `phase`), then the two flags are swapped to 0 as a publisher does. The peer's
//! sleepers are never touched.
//!
//! A sleep lasts one slice at most, and a call's timeout bounds all its waits (core/wait.zig).
//!
//! A ring is ready only while it holds at most `capacity` bytes: the peer's indices are shared memory, and a tail past
//! its head (a commit too long, a corrupt peer) would otherwise make every wait ready and a receive walk the ring for
//! ever. A wait on such a ring ends with Invalid (but for a reader one stamped frame ahead of its head: below).
//!
//! Each side keeps the other's index as it last loaded it, on its endpoint's own cache line (`head_seen`, `tail_seen`),
//! and loads the shared index again only once what it saw runs out: a reader has the bytes up to the head it saw, a
//! writer the room up to the tail it saw, since indices only grow. So an index's line stays with its writer while the
//! ring has data and room, instead of moving between the two cores for every message (about 2-4 times the small-message
//! rate). A corrupt peer's index is checked where it is loaded: a cached one only ever says less.
//!
//! **A short message's stamp** (docs/protocol.md §6.5). A message of one piece whose frame is shorter than
//! `stamped_frame_max` carries a stamp, its position complemented, in its header's `total_len`: the writer stores it
//! (`.release`) after the frame's bytes, and then the head as always (data_path.zig, ring.zig `stamp`). A reader whose
//! spin watches for stamps (`spin`) learns that the frame at its tail is whole from that frame's own cache line, which
//! it must take anyway, instead of from the head's line first: one transfer between the cores per message instead of
//! two, about 30% of a 16 B round trip on x86. It watches only in a wait for an answer (pace 1, `spinPace`): a
//! reader that keeps up with a one-way stream would take the line of the frame being written from the writer, as it
//! fills it, and that stream runs 7-15% slower on x86. It watches only after a frame short enough to carry a stamp
//! arrived (`RecvLine.poll_stamps`), so a conversation of long frames waits as before; and it loads the head every
//! `head_every` looks, for a long frame. The sleep protocol is unchanged: the head is stored and the flag swapped as
//! before, and a waiter's last look before it sleeps is at the head. A stamp only ends a spin early.
//!
//! A stamp the reader trusts is its frame's:
//!   - this lap's other frames hold a length or 0 in that slot, below 2^63, and a stamp is above it;
//!   - an earlier lap's bytes may hold anything there, so a wait trusts stamps only if the slot at its tail holds none
//!     as it begins (the stale-slot guard). The reader waits at its tail P only once the head it loaded says the frame
//!     at P isn't written, and the slot at P changes only when the writer writes the frame at P, so a stamp that
//!     appears later is that frame's. One there already is an earlier lap's bytes or the frame that landed that
//!     instant: the wait then uses the head alone. The writer pays nothing for the guard.
//!
//! **A poll** (a receive that doesn't wait, timeout 0) never spins, so it looks for the stamp at its tail before it
//! loads the head (`poll`): a caller that polls in its own loop then learns of an answer from the frame's line alone,
//! as a spin does. It looks only where an earlier poll at the same tail found the frame unwritten and its slot without
//! the stamp (`RecvLine.unstamped_tail`, noted by `notePollClear`): the stale-slot guard across calls. Positions never
//! repeat, only the reader moves its tail, and while its tail is P the slot at P changes only when the writer writes
//! the frame at P, so a stamp that appears after a look found none there is that frame's. A poll notes the slot only
//! for an answer, after a short frame: this side has sent since it last took a frame or began a wait
//! (`send_head_seen`, which an empty poll doesn't move), so a reader that polls a one-way stream leaves the line of the
//! frame being written to its writer, as a wait does.
//!
//! A reader that takes a frame by its stamp may get ahead of the head: if the writer is preempted between its two
//! stores, the reader's next wait finds its tail up to a stamped frame past the head. That ring isn't corrupt
//! (`corrupt`), and no wait is ready on it, so the reader waits, sleeps if it must, and the head's store and swap wake
//! it. The writer reads its own head, which is never behind its stores, so its room is unaffected.
//!
//! **Each side's own index** (aarch64: `own_indices`). Only a ring's writer moves its head, and only its reader its
//! tail, so on aarch64 each side keeps its own on its endpoint's line (`SendLine.head`, `RecvLine.tail`), stores it
//! there with every publish, and reads it there (`readerTail`, `writerHead`): never back from the ring. On Apple's M1 a
//! load of the index this thread had just stored, with a release store, cost about a third of a zero-copy 16 B stream
//! (the release publishes the tail, and the next receive loads it at once) and 8-15% of a 1 KiB one. The indices are
//! published as before, the tail with every consume; a corrupt peer that writes this side's own index in the ring
//! changes nothing this side reads. On x86-64 each side loads its own index from the ring, as before: there the own
//! copies made a 16 B stream up to a third slower (Windows, polling), though they took 6-22% off a 16 B round trip.
//!
//! This file and core/endpoint.zig import each other, through core/calls.zig: the waits read the endpoint's `Io` and
//! hot line, and the endpoint's local interrupt calls `wakeOwnWaiters` (calls.zig `interrupt`; endpoint.zig's top
//! comment says why the cycle is there).

const std = @import("std");
const builtin = @import("builtin");
const wait = @import("../core/wait.zig");
const wire_protocol = @import("../wire_protocol.zig");
const names = @import("../core/names.zig");
const Ring = @import("ring.zig").Ring;
const frameLen = @import("ring.zig").frameLen;
const stamped_frame_max = @import("ring.zig").stamped_frame_max;

const Endpoint = @import("../core/endpoint.zig").Endpoint;
const Hot = @import("../core/endpoint.zig").Hot;
const Requests = @import("../core/endpoint.zig").Requests;
const Flag = std.atomic.Value(u32);
const platform = @import("../platform.zig");
const build_options = @import("build_options");
const wake_diagnostics = @import("wake_diagnostics.zig");

/// How a wait ends without its bytes. Invalid: the ring holds more than `capacity` bytes, so its indices are corrupt.
pub const Error = error{ Timeout, Disconnected, Cancelled, Invalid };

/// What a waiter needs: bytes to read, or free bytes to write into.
pub const Need = enum { data, space };

/// A writer that finds its ring full spins until this share of the ring is free, not just its frame, and then writes
/// into that room without loading the tail again. Resuming for every frame the reader frees would load the tail, and
/// move its cache line between the two cores, for every message again. Only the spin looks for it: after the spin, and
/// for a call that doesn't wait, the frame's room is enough. On aarch64 a writer whose reload of the tail finds its
/// frame's room but less than this share, while its reader moves, spins for it too, once (`spinForBatch`): with the
/// faster writer its own head makes (`own_indices`), a reader that keeps freeing a frame or two per reload would
/// otherwise take the tail's line back for every message.
const writer_spin_share = 8;

/// A writer's one spin for an eighth of the ring after a reload of the tail found its frame's room but less than an
/// eighth (data_path.zig `reserveSlow`), with its own head `head`. It never sleeps, and it ends once that much is free or
/// the spin runs out: the writer then writes into the room it has.
pub noinline fn spinForBatch(ring: Ring, head: u64) void {
    _ = spin(.space, ring, head, roomBatch(ring), 1, false);
}

/// The room a writer's spin waits for: an eighth of the ring (`writer_spin_share`).
pub inline fn roomBatch(ring: Ring) u64 {
    return ring.capacity / writer_spin_share;
}

/// A writer's first sleep for room, in ms; each next one is twice as long, up to `wait.slice_ms` (the inexact room
/// wake: the module's comment).
const space_first_slice_ms = 1;

// === Waiting ===

/// Waits until `ring` has `amount` bytes (at most `capacity`) to read (`.data`) or free (`.space`). Each round, in
/// order:
///   - spin, unless the call doesn't wait (a writer spins for more room than it needs: `writer_spin_share`);
///   - corrupt indices: Invalid; cancelled: Cancelled;
///   - the session ended: Disconnected, unless the ring is ready by then (what the peer completed before it died is
///     drained, docs/protocol.md §7);
///   - the call's timeout is 0 or past: Timeout;
///   - announce the sleep, and sleep one slice at most.
///
/// The first look is made inline: the peer is usually ahead, and calling the loop (eight saved registers, the ring view
/// spilled to the stack) would then be the whole cost. A reader looks at the head it saw before it loads the head; a
/// poll has its own first look (`poll`).
pub inline fn until(comptime need: Need, endpoint: *Endpoint, ring: Ring, amount: u64, deadline: *wait.Deadline) Error!void {
    if (need == .data) {
        const tail = readerTail(endpoint, ring);
        if (enough(.data, ring.capacity, endpoint.recv.head_seen -% tail, amount)) return;
        if (deadline.timeout_ms == 0) return poll(endpoint, ring, tail, amount, deadline);
        endpoint.recv.head_seen = ring.head();
        if (enough(.data, ring.capacity, endpoint.recv.head_seen -% tail, amount)) return;
    } else if (ready(need, ring, writerHead(endpoint, ring), amount)) return;
    return sleepUntil(need, endpoint, ring, amount, deadline);
}

/// A poll's look, once the head it saw ran out: the stamp of the frame at `tail`, where an earlier poll found that frame
/// unwritten (`takeStamped`), then the head. A frame the head shows, to a poll that has sent since the last frame it
/// took (one that waits for an answer), sets whether the next polls look for stamps, as a spin's does (`noteArrival`);
/// a poll of a one-way stream never looks for them, and skips that work on every message.
inline fn poll(endpoint: *Endpoint, ring: Ring, tail: u64, amount: u64, deadline: *wait.Deadline) Error!void {
    if (takeStamped(endpoint, ring, tail) and enough(.data, ring.capacity, endpoint.recv.head_seen -% tail, amount)) return;
    endpoint.recv.head_seen = ring.head();
    if (enough(.data, ring.capacity, endpoint.recv.head_seen -% tail, amount)) {
        const sent = endpoint.send.head_sent.load(.monotonic);
        if (sent == endpoint.recv.send_head_seen.load(.monotonic)) return;
        endpoint.recv.send_head_seen.store(sent, .monotonic);
        return noteArrival(endpoint, ring, false);
    }
    return sleepUntil(.data, endpoint, ring, amount, deadline);
}

/// A poll that found less than `amount` bytes at `tail`: true once a short pause finds them after all. Its hints are
/// learned as a broadcast subscriber's are (broadcast/reader.zig `pollPaced`): a pause that finds the bytes means a
/// steady stream, and the next one waits twice as long, up to `wait.stream_pace`; one that finds nothing means none,
/// and the next takes one hint. A poll for an answer (this side sent since the last frame it took: `spinPace`'s
/// signal) takes one hint and leaves the pace alone, so a polling round trip doesn't wait out a stream's pause. A
/// reader that polls in a loop would otherwise pull the head's line from the writer on every empty poll, as an unpaced
/// spin does: on Apple's M1 (where each side reads its own index, `own_indices`, and an empty poll is short) a polling
/// 1 KiB stream ran a third as fast as a waiting one, and on x86 a polling round trip's p50 was 25-35% slower than a
/// waiting one's at 16 B and 45-60% at 1 KiB. An empty poll still never sleeps.
fn pollPaced(endpoint: *Endpoint, ring: Ring, tail: u64, amount: u64) bool {
    const sent = endpoint.send.head_sent.load(.monotonic);
    const answer = sent != endpoint.recv.send_head_seen.load(.monotonic);
    for (0..if (answer) 1 else endpoint.recv.poll_pace) |_| std.atomic.spinLoopHint();
    endpoint.recv.head_seen = ring.head();
    if (enough(.data, ring.capacity, endpoint.recv.head_seen -% tail, amount)) {
        if (answer) {
            // As `poll` does with a frame the head shows
            endpoint.recv.send_head_seen.store(sent, .monotonic);
            noteArrival(endpoint, ring, false);
        } else endpoint.recv.poll_pace = @min(endpoint.recv.poll_pace *| 2, wait.stream_pace);
        return true;
    }
    if (!answer) endpoint.recv.poll_pace = 1;
    return false;
}

/// Whether a poll takes the frame at `tail` by its stamp: an earlier poll at this tail found its slot without the stamp
/// (the guard: the module's comment), the slot holds it now, and the frame is short enough to carry one. The frame is
/// then published as far as this side is concerned, as after a spin (`noteArrival`), and the answer this side waited
/// for has come (`send_head_seen`).
inline fn takeStamped(endpoint: *Endpoint, ring: Ring, tail: u64) bool {
    const clear = endpoint.recv.unstamped_tail;
    if (clear == 0 or clear != tail +% 1 or !ring.stamped(tail)) return false;
    const frame = frameLen(ring.readHeader(tail).frag_len);
    const end = tail +% frame;
    if (frame >= stamped_frame_max or end -% endpoint.recv.head_seen > ring.capacity) return false;
    endpoint.recv.head_seen = end;
    endpoint.recv.send_head_seen.store(endpoint.send.head_sent.load(.monotonic), .monotonic);
    return true;
}

/// A poll that found the frame at the tail unwritten: notes the tail if its slot holds no stamp, for the next poll's
/// `takeStamped`. Only in a wait for an answer after a short frame (the module's comment): this side has sent since it
/// last took a frame by its stamp or began a wait.
fn notePollClear(endpoint: *Endpoint, ring: Ring) void {
    if (!endpoint.recv.poll_stamps or endpoint.send.head_sent.load(.monotonic) == endpoint.recv.send_head_seen.load(.monotonic)) return;
    const tail = readerTail(endpoint, ring);
    if (!ring.stamped(tail)) endpoint.recv.unstamped_tail = tail +% 1;
}

/// Out of line, so that the callers' per-message code keeps its registers: inlined into `send`, it makes the fragment
/// loop spill, and 64 B messages run slower.
noinline fn sleepUntil(comptime need: Need, endpoint: *Endpoint, ring: Ring, amount: u64, deadline: *wait.Deadline) Error!void {
    const io = endpoint.io;
    const spin_amount = if (need == .space) @max(amount, ring.capacity / writer_spin_share) else amount;
    // A call that doesn't wait doesn't spin; a reader's poll notes its stamp slot for the next poll instead, and
    // leaves `send_head_seen` to the waits and to the polls that take a frame
    const polls = deadline.timeout_ms == 0;
    if (need == .data and polls) notePollClear(endpoint, ring);
    // A reader paces its spin (`spinPace`); unpaced, the pace is 1, a look after every hint
    const pace: u32 = if (need == .data and !polls and wait.stream_pace > 1) spinPace(endpoint) else 1;
    // A receiver that keeps up with its sender comes here for most messages and leaves after a few spins, so what
    // comes before the first spin stays light: the clock is read only once a spin has ended, and the sleeper's
    // flag and event are looked up only before a sleep.
    var space_slice_ms: u32 = space_first_slice_ms;
    // This side's own index, which only this thread moves: a reader's tail, a writer's head
    const own = switch (need) {
        .data => readerTail(endpoint, ring),
        .space => writerHead(endpoint, ring),
    };
    // A reader watches for stamps only in a wait for an answer, and only if the slot at its tail holds none as the
    // wait begins (the stale-slot guard: the module's comment)
    const stamps = need == .data and !polls and pace == 1 and endpoint.recv.poll_stamps and !ring.stamped(own);
    while (true) {
        if (!polls and spin(need, ring, own, spin_amount, pace, stamps)) {
            if (need == .data) noteArrival(endpoint, ring, stamps);
            return;
        }
        // A poll that found nothing pauses and looks once more (`pollPaced`)
        if (need == .data and polls and pollPaced(endpoint, ring, own, amount)) return;
        // A reader about to sleep: the exact swap of the room's wake, which its publishes only look at
        if (need == .data) wakeIfAsleep(&endpoint.hot, sleeper(ring, .space));
        if (try checkStop(need, &endpoint.hot, ring, own, amount) == .ready) return;
        const sleep_ms = switch (need) {
            .data => try wait.nextSleepMs(io, deadline),
            .space => try wait.sleepMsWithin(io, deadline, space_slice_ms),
        };
        space_slice_ms = @min(space_slice_ms * 2, wait.slice_ms);
        if (try announceSleep(need, &endpoint.hot, ring, own, amount) == .ready) return;
        const waiter = sleeper(ring, need);
        const event = eventOf(&endpoint.hot, waiter.object);
        if (build_options.test_hooks) {
            const watched: wake_diagnostics.Sleep = .begin(waiter.flag);
            const timed_out = platform.ringWait(waiter.flag, event, sleep_ms);
            watched.end(@tagName(need), timed_out, sleep_ms, ready(need, ring, own, amount));
        } else _ = platform.ringWait(waiter.flag, event, sleep_ms);
        try wait.backInIo(io);
    }
}

/// The hints a reader's spin leaves without a look after its first (`spin`): 1 for a reader that sent since its last
/// wait (or its last poll that took a frame by its stamp: `takeStamped`), which waits for an answer;
/// `wait.stream_pace` for one that only receives, which waits for its stream to go on.
/// What it sent is its own endpoint's (`SendLine.head_sent`), not the send ring's head in the segment: the peer spins
/// on that line, and taking it back after every send cost a round trip some 20-30 ns across x86 cores.
fn spinPace(endpoint: *Endpoint) u32 {
    const sent = endpoint.send.head_sent.load(.monotonic);
    defer endpoint.recv.send_head_seen.store(sent, .monotonic);
    return if (sent != endpoint.recv.send_head_seen.load(.monotonic)) 1 else wait.stream_pace;
}

/// A spin: true once `ring` has `amount` bytes to read or free, with this side's own index `own` (a reader's tail, a
/// writer's head). It looks once, then spends `pace` hints without a look, then looks after every hint, up to
/// `spin_iters` hints in all. Each look loads the other side's index. A writer's pace is 1.
///
/// A reader paces itself because its look pulls the line of `head` from the writer, which must win it back to publish.
/// A reader that has caught up with a stream of small frames and looks after every hint slows its writer so much that
/// it stays caught up: on an M1, 1 KiB messages run at 8 M/s that way, at 25 M/s with no look for 256 hints and at
/// 30 M/s for 512 (`wait.stream_pace` there). A wait that lasts past the paced hints is for a big frame, whose writer
/// publishes once after all its copying, so looking after every hint costs that writer nothing (a look only every 256
/// hints made a 1.5 MiB message, three pieces of a 512 KiB ring, about 7% slower).
///
/// A reader that watches for `stamps` looks at the stamp slot of the frame at its tail first, and loads the head only
/// every `head_every` looks: a stamp tells it that frame is whole, and every wait for data is for the frame at the
/// tail, its header or all of it.
inline fn spin(comptime need: Need, ring: Ring, own: u64, amount: u64, pace: u32, stamps: bool) bool {
    if (stamps and ring.stamped(own)) return true;
    if (ready(need, ring, own, amount)) return true;
    for (0..pace) |_| std.atomic.spinLoopHint();
    for (pace..wait.spin_iters) |i| {
        if (stamps) {
            if (ring.stamped(own)) return true;
            if (i % head_every == 0 and ready(need, ring, own, amount)) return true;
        } else if (ready(need, ring, own, amount)) return true;
        std.atomic.spinLoopHint();
    }
    return false;
}

/// A spin that watches for stamps loads the head once every this many looks: for a frame too long to carry a stamp,
/// which the head alone announces.
const head_every = 64;

/// After a reader's spin found the next frame: the next spins watch for stamps only if this frame is short enough to
/// carry one; and a frame found by its stamp, whose head may not be stored yet, is published as far as this side is
/// concerned, so the receive's wait for the whole frame doesn't load the head.
fn noteArrival(endpoint: *Endpoint, ring: Ring, stamps: bool) void {
    const tail = readerTail(endpoint, ring);
    const frame = frameLen(ring.readHeader(tail).frag_len);
    endpoint.recv.poll_stamps = frame < stamped_frame_max;
    if (!stamps or !endpoint.recv.poll_stamps or !ring.stamped(tail)) return;
    const end = tail +% frame;
    if (end -% endpoint.recv.head_seen <= ring.capacity) endpoint.recv.head_seen = end;
}

/// The waiter's side of the sleep protocol (docs/protocol.md §6.1): sets the sleep flag, then looks at the ring,
/// `requests` and `phase` again. `.sleep` only if nothing changed; otherwise the flag is cleared again (a swap too, so
/// that every change of the flag is one) and the change reported.
fn announceSleep(comptime need: Need, hot: *const Hot, ring: Ring, own: u64, amount: u64) Error!enum { ready, sleep } {
    const flag = sleeper(ring, need).flag;
    _ = flag.swap(1, .acq_rel);
    errdefer _ = flag.swap(0, .acq_rel);
    if (!ready(need, ring, own, amount) and try checkStop(need, hot, ring, own, amount) == .waiting) return .sleep;
    _ = flag.swap(0, .acq_rel);
    return .ready;
}

/// Whether `ring` has `amount` (at most `capacity`) bytes to read or free, with this side's own index `own` (a reader's
/// tail, a writer's head): loads the other side's. Never while the ring holds more than `capacity` bytes: `head -%
/// tail` is the bytes in the ring as long as the indices are sound.
inline fn ready(comptime need: Need, ring: Ring, own: u64, amount: u64) bool {
    return enough(need, ring.capacity, held(need, ring, own), amount);
}

/// The bytes in `ring`, `head -% tail`, with this side's own index `own`: loads the other side's.
inline fn held(comptime need: Need, ring: Ring, own: u64) u64 {
    return switch (need) {
        .data => ring.head() -% own,
        .space => own -% ring.tail(),
    };
}

/// Whether a ring of `capacity` bytes that holds `used` has `amount` bytes to read or free.
inline fn enough(comptime need: Need, capacity: u32, used: u64, amount: u64) bool {
    return switch (need) {
        // amount <= used <= capacity, in one comparison: below `amount`, `used -% amount` wraps past `capacity`
        .data => used -% amount <= capacity - amount,
        .space => used <= capacity - amount,
    };
}

/// Whether the ring holds more than `capacity` bytes: its tail is past its head, or its indices are garbage. Except,
/// for its reader, a tail less than `stamped_frame_max` past the head: the reader took a frame by its stamp before its
/// head was stored (the module's comment). Its writer, which loads its own head, never sees that.
fn corrupt(comptime need: Need, ring: Ring, own: u64) bool {
    const in_ring = held(need, ring, own);
    return in_ring > ring.capacity and (need == .space or 0 -% in_ring >= stamped_frame_max);
}

/// Whether the wait goes on to its sleep: Invalid on a corrupt ring; Cancelled after `fipc_cancel`; once the session
/// has ended (PEER_DEAD), `.ready` if the ring is ready after all (looked at after the phase, so it holds everything
/// the peer published before it died), else Disconnected; `.waiting` while the session runs.
fn checkStop(comptime need: Need, hot: *const Hot, ring: Ring, own: u64, amount: u64) Error!enum { waiting, ready } {
    if (corrupt(need, ring, own)) return error.Invalid;
    if (hot.requests.load(.acquire).cancel) return error.Cancelled;
    return switch (hot.phase.load(.acquire)) {
        .handshake, .connected => .waiting,
        // A connection is never unsubscribed (a broadcast subscriber's phase): as an ended session
        .peer_dead, .unsubscribed => if (ready(need, ring, own, amount)) .ready else error.Disconnected,
    };
}

// === Waking ===

/// The writer's publish (docs/protocol.md §6.1): stores `head`, then wakes the ring's reader if it sleeps.
pub inline fn publishHead(hot: *const Hot, ring: Ring, head: u64) void {
    storeHead(ring, head);
    wakeReader(hot, ring);
}

/// The publish's first step alone: stores `head`. A batch of sends (data_path.zig `sendMany`) stores it after every
/// frame, and wakes the reader once (`wakeReader`), after its last frame and before any wait for room.
pub inline fn storeHead(ring: Ring, head: u64) void {
    ring.indices.writer.head.store(head, .release);
}

/// The publish's second step: wakes the ring's reader if it sleeps, for every head stored before it.
pub inline fn wakeReader(hot: *const Hot, ring: Ring) void {
    wakeIfAsleep(hot, sleeper(ring, .data));
}

/// The reader's consume: it has read the ring up to `tail`, which it keeps on aarch64 (`RecvLine.tail`: the module's
/// comment) and publishes.
pub inline fn consume(endpoint: *Endpoint, ring: Ring, tail: u64) void {
    if (own_indices) endpoint.recv.tail = tail;
    publishTail(&endpoint.hot, ring, tail);
}

/// Whether each side reads its own index from its endpoint, not from the ring (the module's comment): on aarch64.
pub const own_indices = builtin.cpu.arch == .aarch64;

/// The receive ring's tail, which only this side's reader moves: its own copy on aarch64, the ring's on x86-64.
pub inline fn readerTail(endpoint: *const Endpoint, ring: Ring) u64 {
    return if (own_indices) endpoint.recv.tail else ring.tail();
}

/// The send ring's head, which only this side's writer moves: its own copy on aarch64, the ring's on x86-64.
pub inline fn writerHead(endpoint: *const Endpoint, ring: Ring) u64 {
    return if (own_indices) endpoint.send.head else ring.head();
}

/// The reader's publish (the space it consumed): stores `tail`, then wakes the ring's writer if it saw its flag set
/// (the inexact room wake: the module's comment).
pub inline fn publishTail(hot: *const Hot, ring: Ring, tail: u64) void {
    ring.indices.reader.tail.store(tail, .release);
    const writer = sleeper(ring, .space);
    if (writer.flag.load(.monotonic) != 0) wakeIfAsleep(hot, writer);
}

/// The local interrupt (docs/protocol.md §6.2): after the caller published what they must see (`requests.cancel`, or
/// PEER_DEAD in `phase`), wakes this side's two sleepers of the view's session, the reader of its inbound ring and the
/// writer of its outbound ring. The peer's waits don't look at this side's changes and are left alone: waking them
/// would only make them spin. The caller holds the endpoint mutex and has seen the
/// view's session slot set, so the flags are mapped (`close` holds the mutex while it unmaps).
pub fn wakeOwnWaiters(hot: *const Hot) void {
    wakeIfAsleep(hot, sleeper(Ring.forRecv(hot), .data));
    wakeIfAsleep(hot, sleeper(Ring.forSend(hot), .space));
}

/// The publisher's second step: a sleeper may be asleep only while its flag is set, and whoever takes its 1 wakes it.
inline fn wakeIfAsleep(hot: *const Hot, who: Sleeper) void {
    if (who.flag.swap(0, .acq_rel) == 1) wake(hot, who.flag, who.object);
}

/// Out of line, to keep the inlined publish small: this is the path of the system call, taken only when the flag was
/// set (its owner is then usually asleep). The sleeper comes in two registers rather than as a struct, which the ABI
/// would pass in memory, built on every publish.
noinline fn wake(hot: *const Hot, flag: *Flag, object: names.Object) void {
    if (build_options.test_hooks) wake_diagnostics.noteWake(flag);
    platform.ringWake(flag, eventOf(hot, object));
}

// === Sleepers and events ===

/// A ring side that may sleep: its flag and, on Windows, the flag's event in the session's objects.
const Sleeper = struct { flag: *Flag, object: names.Object };

/// The side of `ring` that waits for `need`: the reader (waits for data) sleeps on `reader_sleeps`, in the writer's
/// half of the indices; the writer (waits for space) on `writer_sleeps`, in the reader's half.
fn sleeper(ring: Ring, comptime need: Need) Sleeper {
    return switch (need) {
        .data => .{ .flag = &ring.indices.writer.reader_sleeps, .object = if (ring.is_s2c) .s2c_data else .c2s_data },
        .space => .{ .flag = &ring.indices.reader.writer_sleeps, .object = if (ring.is_s2c) .s2c_space else .c2s_space },
    };
}

/// A sleeper's ring event, `object` of the view's session.
fn eventOf(hot: *const Hot, object: names.Object) platform.RingEvent {
    if (@sizeOf(platform.RingEvent) == 0) return platform.no_ring_event; // the flag is all a sleeper needs
    const session = hot.view_session.load(.monotonic) orelse return platform.no_ring_event;
    return session.mapping.ringEvent(object);
}

// === Tests ===

const litmus = @import("../core/litmus.zig");

/// A view over a header and rings in this process's memory, for the tests below: no session slot, so no events.
const TestView = struct {
    header: wire_protocol.SegmentHeader,
    data: [64]u8,
    hot: Hot,

    fn init(v: *TestView, listener: bool) void {
        v.header = std.mem.zeroes(wire_protocol.SegmentHeader);
        v.data = @splat(0);
        v.hot = std.mem.zeroes(Hot);
        v.hot.stream.header.store(&v.header, .monotonic);
        v.hot.stream.data.store(&v.data, .monotonic);
        v.hot.view_capacity.store(32, .monotonic);
        v.hot.view_server.store(listener, .monotonic);
    }
};

test "a ring is ready only while it holds at most its capacity" {
    var v: TestView = undefined;
    v.init(true); // capacity 32
    const ring = Ring.forSend(&v.hot);
    const head = &v.header.s2c.writer.head;
    const tail = &v.header.s2c.reader.tail;
    head.store(16, .monotonic);
    try std.testing.expect(ready(.data, ring, ring.tail(), 16) and !ready(.data, ring, ring.tail(), 17));
    try std.testing.expect(ready(.space, ring, ring.head(), 16) and !ready(.space, ring, ring.head(), 17));
    head.store(32, .monotonic); // full
    try std.testing.expect(ready(.data, ring, ring.tail(), 32) and !ready(.space, ring, ring.head(), 1) and !corrupt(.data, ring, ring.tail()) and !corrupt(.space, ring, ring.head()));
    // The tail past the head: never ready, and corrupt for the writer. For the reader, a tail less than a stamped
    // frame past the head is a frame taken by its stamp before its head was stored: a wait
    for ([_]u64{ 40, 32 + stamped_frame_max - 16 }) |past| {
        tail.store(past, .monotonic);
        try std.testing.expect(!ready(.data, ring, ring.tail(), 16) and !ready(.space, ring, ring.head(), 16) and corrupt(.space, ring, ring.head()) and !corrupt(.data, ring, ring.tail()));
        try std.testing.expectEqual(.waiting, try checkStop(.data, &v.hot, ring, ring.tail(), 16));
        try std.testing.expectError(error.Invalid, checkStop(.space, &v.hot, ring, ring.head(), 16));
    }
    // A stamped frame or more past the head, or more than a ring between them: corrupt for both
    tail.store(32 + stamped_frame_max, .monotonic);
    try std.testing.expect(!ready(.data, ring, ring.tail(), 16) and corrupt(.data, ring, ring.tail()) and corrupt(.space, ring, ring.head()));
    try std.testing.expectError(error.Invalid, checkStop(.data, &v.hot, ring, ring.tail(), 16));
    tail.store(0, .monotonic);
    head.store(33, .monotonic);
    try std.testing.expect(!ready(.data, ring, ring.tail(), 16) and !ready(.space, ring, ring.head(), 0) and corrupt(.data, ring, ring.tail()) and corrupt(.space, ring, ring.head()));
    try std.testing.expectError(error.Invalid, checkStop(.data, &v.hot, ring, ring.tail(), 16));
    // Indices wrap: a ring across 2^64 is ready as any other
    tail.store(std.math.maxInt(u64) - 7, .monotonic);
    head.store(8, .monotonic);
    try std.testing.expect(ready(.data, ring, ring.tail(), 16) and !ready(.data, ring, ring.tail(), 17) and !corrupt(.data, ring, ring.tail()) and !corrupt(.space, ring, ring.head()));
}

test "the local interrupt clears only this side's two sleep flags" {
    var v: TestView = undefined;
    v.init(true); // the server: it reads c2s and writes s2c
    const header = &v.header;
    for ([_]*Flag{ &header.s2c.writer.reader_sleeps, &header.s2c.reader.writer_sleeps, &header.c2s.writer.reader_sleeps, &header.c2s.reader.writer_sleeps }) |flag| flag.store(1, .monotonic);
    wakeOwnWaiters(&v.hot);
    try std.testing.expectEqual(@as(u32, 0), header.c2s.writer.reader_sleeps.load(.monotonic));
    try std.testing.expectEqual(@as(u32, 0), header.s2c.reader.writer_sleeps.load(.monotonic));
    try std.testing.expectEqual(@as(u32, 1), header.s2c.writer.reader_sleeps.load(.monotonic));
    try std.testing.expectEqual(@as(u32, 1), header.c2s.reader.writer_sleeps.load(.monotonic));
}

test "after the session ends a waiter drains what was published, then gets Disconnected; cancel comes first" {
    var v: TestView = undefined;
    v.init(false); // the client: it reads s2c
    const ring = Ring.forRecv(&v.hot);
    try std.testing.expectEqual(.waiting, try checkStop(.data, &v.hot, ring, ring.tail(), 16));
    v.hot.phase.store(.peer_dead, .monotonic);
    try std.testing.expectError(error.Disconnected, checkStop(.data, &v.hot, ring, ring.tail(), 16));
    v.header.s2c.writer.head.store(16, .monotonic); // a message the peer completed before it died
    try std.testing.expectEqual(.ready, try checkStop(.data, &v.hot, ring, ring.tail(), 16));
    try std.testing.expectEqual(.ready, try announceSleep(.data, &v.hot, ring, ring.tail(), 16));
    try std.testing.expectError(error.Disconnected, announceSleep(.data, &v.hot, ring, ring.tail(), 32));
    try std.testing.expectEqual(@as(u32, 0), v.header.s2c.writer.reader_sleeps.load(.monotonic)); // given up, not left set
    v.hot.requests.store(Requests{ .cancel = true }, .monotonic);
    try std.testing.expectError(error.Cancelled, checkStop(.data, &v.hot, ring, ring.tail(), 16));
}

/// The litmus test's race (core/litmus.zig) over one ring: a, the writer, publishes 16 bytes; b, the reader, announces
/// its sleep.
///
/// Lost: the reader decided to sleep, and its flag is still set after the writer finished. So the writer's swap came
/// first, yet the reader's index loads missed the new head, and nobody will wake the reader.
///
/// The weaker pair: the writer stores the index (`.release`), then CASes the flag (`.acq_rel`); the reader stores its
/// flag (`.release`), then loads the indices (`.acquire`). That flag store can still sit in the reader's store buffer
/// while its loads run.
const SleepCase = struct {
    view: TestView,
    weak: bool,
    sleeping: bool = false,

    fn ring(case: *SleepCase) Ring {
        return Ring.forSend(&case.view.hot);
    }

    pub fn reset(case: *SleepCase) void {
        case.ring().indices.writer.head.store(0, .monotonic);
        case.ring().indices.writer.reader_sleeps.store(0, .monotonic);
        case.sleeping = false;
    }

    pub fn a(case: *SleepCase) void {
        const indices = case.ring().indices;
        if (!case.weak) return publishHead(&case.view.hot, case.ring(), 16);
        indices.writer.head.store(16, .release);
        _ = indices.writer.reader_sleeps.cmpxchgStrong(1, 0, .acq_rel, .acquire);
    }

    pub fn b(case: *SleepCase) void {
        if (!case.weak) {
            case.sleeping = (announceSleep(.data, &case.view.hot, case.ring(), case.ring().tail(), 16) catch unreachable) == .sleep;
            return;
        }
        const indices = case.ring().indices;
        indices.writer.reader_sleeps.store(1, .release);
        const head = indices.writer.head.load(.acquire);
        const tail = indices.reader.tail.load(.acquire);
        case.sleeping = head -% tail < 16;
        if (!case.sleeping) indices.writer.reader_sleeps.store(0, .release);
    }

    pub fn lost(case: *SleepCase) bool {
        return case.sleeping and case.ring().indices.writer.reader_sleeps.load(.monotonic) == 1;
    }
};

test "no wake-up is lost: store-buffering litmus of the publisher and the waiter" {
    var library: SleepCase = .{ .view = undefined, .weak = false };
    library.view.init(true); // the server writes s2c
    const strong = litmus.race(&library);
    var weak: SleepCase = .{ .view = undefined, .weak = true };
    weak.view.init(true);
    try litmus.expectNoneLost("ring sleep vs publish", strong, litmus.race(&weak));
}
