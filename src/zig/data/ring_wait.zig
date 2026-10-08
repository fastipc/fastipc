//! Ring waits and wake-ups (docs/protocol.md §6): how a side that must wait for its ring sleeps, and how the
//! other side, or this side's own session end or cancel, wakes it. The one place for both; stream.zig calls `until`
//! and `publishHead`/`publishTail`, the endpoint `wakeOwnWaiters`.
//!
//! Each ring index has a sleep flag beside it: the reader sleeps on `reader_sleeps` while it waits for `head` to move,
//! the writer on `writer_sleeps` while it waits for `tail` to move. The two sides pair up through the flag, and while a
//! session runs every change of a flag is an atomic swap (`.acq_rel`):
//!   - the publisher stores the index (`.release`), then swaps the flag to 0, and wakes the sleeper if it was 1;
//!   - the waiter swaps its flag to 1, then loads the index and the endpoint's `requests` and `status` (`.acquire`),
//!     and sleeps only if none of them changed; otherwise it swaps the flag back to 0 and acts on what it saw.
//! Why no wake-up is lost, in the language memory model: the swaps of one flag are totally ordered (its modification
//! order), and each swap reads the value of the one just before it. If the publisher's swap comes after the waiter's,
//! it reads the waiter's 1, or the 0 of whoever took that 1 first and so woke the waiter (or of the waiter itself,
//! giving the sleep up), and it wakes. If it comes before, the waiter's swap reads from it or from a later swap, which
//! continues its release sequence, so the publisher's swap synchronizes with the waiter's: the index store happens
//! before the waiter's index load, which sees it, and the waiter doesn't sleep. The kernel keeps the last step:
//! `FUTEX_WAIT(flag, 1)` doesn't sleep once the flag is 0, and an auto-reset event stays signaled until a wait
//! consumes it. The waiter's flag must be set by a swap, not a plain `.release` store, which its index load could
//! pass on x86: a receive would sleep a whole slice with its data waiting (the litmus test below shows it).
//!
//! A publish costs one locked instruction, the flag's `xchg` after a plain store of the index. A `.seq_cst` store of
//! the index (an `xchg`) followed by a plain flag load measures 5-9% slower with 1 KiB messages. The waiter's `xchg` is
//! on its slow path.
//!
//! The same rule wakes this side's own sleepers after `fipc_cancel` or the session's end (docs/protocol.md §6.2): the
//! change is published (`requests`, `status`), then the two flags are swapped to 0 as a publisher does. The peer's
//! sleepers are never touched.
//!
//! A sleep lasts one slice (100 ms) at most. Between slices the waiter looks again and returns to `Io` through its
//! cancelation check, so a task canceled through its `Io` ends its wait within a slice (docs/protocol.md §6.1; Zig
//! 0.17's `Io` has no yield to offer besides). Every wake-up the protocol needs is explicit; the slice is only a
//! backstop.
//!
//! A ring is ready only while it holds at most `cap` bytes: the peer's indices are shared memory, and a tail past its
//! head (a commit too long, a corrupt peer) would otherwise make every wait ready and a receive walk the ring for ever.
//! A wait on such a ring ends with Invalid.
//!
//! Each side keeps the other's index as it last loaded it, on its endpoint's own cache line (`head_seen`, `tail_seen`),
//! and loads the shared index again only once what it saw runs out: a reader has the bytes up to the head it saw, a
//! writer the room up to the tail it saw, since indices only grow. So an index's line stays with its writer while the
//! ring has data and room, instead of moving between the two cores for every message (about 2-4 times the small-message
//! rate). A corrupt peer's index is checked where it is loaded: a cached one only ever says less.
//!
//! This file and lifecycle/endpoint.zig import each other: the waits read the endpoint's `Io` and hot line, and the
//! endpoint's local interrupt calls `wakeOwnWaiters` (endpoint.zig's top comment says why the cycle is there).

const std = @import("std");
const builtin = @import("builtin");
const abi = @import("../abi.zig");
const endpoint = @import("../lifecycle/endpoint.zig");
const names = @import("../lifecycle/names.zig");
const Ring = @import("ring.zig").Ring;

const Endpoint = endpoint.Endpoint;
const Hot = endpoint.Hot;
const Flag = std.atomic.Value(u32);
const platform = @import("../platform.zig");
const build_options = @import("build_options");

/// How a wait ends without its bytes. Invalid: the ring holds more than `cap` bytes, so its indices are corrupt.
pub const Error = error{ Timeout, Disconnected, Cancelled, Invalid };

/// What a waiter needs: bytes to read, or free bytes to write into.
pub const Need = enum { data, space };

/// A data call's timeout, which bounds the whole call, however many waits it makes: C's milliseconds (0: the
/// call doesn't wait; negative: no limit) and the deadline, taken by the first wait that doesn't find its bytes in its
/// first spin, so a call that never waits reads no clock. Zero means not yet: a deadline is at least a
/// millisecond after the clock's start.
pub const Deadline = struct {
    timeout_ms: c_int,
    at: std.Io.Timestamp = .zero,

    pub fn init(timeout_ms: c_int) Deadline {
        return .{ .timeout_ms = timeout_ms };
    }
};

/// Spins before sleeping, to catch a fast peer without a system call.
const spin_iters = 4096;
/// A reader that waits for a stream, not for an answer, looks at the ring once, then again only after this many hints
/// of its spin, and then after every hint (`spinData`); 1 turns the pacing off. Per platform (`stream_paces`), since a
/// hint's length differs: 256 of an M1's `isb`s take some 2 µs, 256 of x86's `pause`s some 10 µs. The build option
/// `stream_pace` overrides it, for tuning.
const stream_pace: u32 = build_options.stream_pace orelse streamPaceOf(builtin.os.tag, builtin.cpu.arch);
/// The measured paces (`devtool bench-pace`); a platform not listed, or listed with 1, isn't paced. On x86 (an
/// i9-13900H, unpinned) the 16-256 B streams gain more the larger the pace, up to 64 (two to two and a half times at
/// 64 B), 1 KiB and up nothing; what limits the pace is the round trip. Windows 64: copy and RPC of 64 B 2.0 and 2.5
/// times, the round trip unchanged. Linux 32: 2.1 and 1.7 times, the round trip's p50 some 3% slower (64: 2-9%), its
/// p99 unchanged. `zig build -Dstream_pace=N` builds another pace, for other hardware.
const stream_paces = [_]struct { os: std.Target.Os.Tag, arch: std.Target.Cpu.Arch, pace: u32 }{
    .{ .os = .macos, .arch = .aarch64, .pace = 256 },
    .{ .os = .linux, .arch = .aarch64, .pace = 256 },
    .{ .os = .linux, .arch = .x86_64, .pace = 32 },
    .{ .os = .windows, .arch = .x86_64, .pace = 64 },
};

fn streamPaceOf(os: std.Target.Os.Tag, arch: std.Target.Cpu.Arch) u32 {
    for (stream_paces) |entry| if (entry.os == os and entry.arch == arch) return entry.pace;
    return 1;
}

/// A writer that finds its ring full spins until this share of the ring is free, not just its frame, and then writes
/// into that room without loading the tail again. Resuming for every frame the reader frees would load the tail, and
/// move its cache line between the two cores, for every message again. Only the spin looks for it: after the spin, and
/// for a call that doesn't wait, the frame's room is enough.
const writer_spin_share = 8;
/// The longest sleep between two looks at the ring (docs/protocol.md §6.1).
const slice_ms = 100;

// === Waiting ===

/// Waits until `r` has `amount` bytes (at most `cap`) to read (`.data`) or free (`.space`). Each round, in this order:
/// spin, unless the call doesn't wait, a writer for more room than it needs (`writer_spin_share`); corrupt indices →
/// Invalid; cancelled → Cancelled; the session ended → Disconnected, unless the ring is ready by then (what the peer
/// completed before it died is drained, docs/protocol.md §7); the call's timeout 0 or past → Timeout; announce the
/// sleep and sleep one slice at most.
///
/// The first look is made inline: the peer is usually ahead, and calling the loop (eight saved registers, the ring view
/// spilled to the stack) would then be the whole cost. A reader looks at the head it saw before it loads the head.
pub inline fn until(comptime need: Need, ep: *Endpoint, r: Ring, amount: u64, deadline: *Deadline) Error!void {
    if (need == .data) {
        const tail = r.tail();
        if (enough(.data, r.cap, ep.recv.head_seen -% tail, amount)) return;
        ep.recv.head_seen = r.head();
        if (enough(.data, r.cap, ep.recv.head_seen -% tail, amount)) return;
    } else if (ready(need, r, amount)) return;
    return sleepUntil(need, ep, r, amount, deadline);
}

/// Out of line, so that the callers' per-message code keeps its registers: inlined into `send`, it makes the fragment
/// loop spill, and RPC's 64 B messages run 2-4% slower.
noinline fn sleepUntil(comptime need: Need, ep: *Endpoint, r: Ring, amount: u64, deadline: *Deadline) Error!void {
    const io = ep.io;
    const spin_amount = if (need == .space) @max(amount, r.cap / writer_spin_share) else amount;
    // A reader that sent since its last wait waits for an answer, and looks after every hint; one that only receives
    // waits for its stream to go on, and leaves the first `stream_pace` hints without a look (`spinData`). What it sent
    // is its own endpoint's (`SendLine.head_sent`), not the send ring's head in the segment: the peer spins on that
    // line, and taking it back after every send cost a round trip some 20-30 ns across x86 cores. Unpaced, none of
    // this is compiled.
    const paced = need == .data and stream_pace > 1;
    const pace: u32 = if (paced) pace: {
        const sent = ep.send.head_sent.load(.monotonic);
        defer ep.recv.send_head_seen = sent;
        break :pace if (sent != ep.recv.send_head_seen) 1 else stream_pace;
    } else 1;
    // A receiver that keeps up with its sender comes here for most messages and leaves after a few spins, so what
    // comes before the first spin stays light: the clock is read only once a spin has ended, and the sleeper's
    // flag and event are looked up only before a sleep.
    while (true) {
        if (deadline.timeout_ms != 0) {
            if (paced) {
                if (spinData(r, spin_amount, pace)) return;
            } else for (0..spin_iters) |_| {
                if (ready(need, r, spin_amount)) return;
                std.atomic.spinLoopHint();
            }
        }
        if (try stopped(need, &ep.hot, r, amount)) return;
        const sleep_ms = try nextSleepMs(io, deadline);
        if (try announceSleep(need, &ep.hot, r, amount) == .ready) return;
        const waiter = sleeper(r, need);
        if (build_options.test_hooks) {
            diagnosed(need, r, amount, waiter.flag, eventOf(&ep.hot, waiter), sleep_ms);
        } else _ = platform.ringWait(waiter.flag, eventOf(&ep.hot, waiter), sleep_ms);
        // Back in `Io` between slices. A task canceled through it ends its wait as a cancel would, and the cancelation
        // stays pending for the task's next cancelation point.
        io.checkCancel() catch {
            io.recancel();
            return error.Cancelled;
        };
    }
}

/// A reader's spin: true once `r` has `amount` bytes to read; `spin_iters` hints at most, with a look at the head
/// before the first, then none until `pace` hints have passed, then after every hint. The tail is the reader's own, so
/// it is loaded once. Each look pulls the line of `head` (and of `tail`, which shares it on 128-byte lines) from the
/// writer, which must win it back for its next publish: a reader that has caught up with a stream of small frames and
/// looks after every hint slows its writer so much that it stays caught up (on an M1, 1 KiB messages ran at 8 M/s that
/// way, against 25 M/s with no look for 256 hints). A wait that outlasts the pause is for a big frame, whose writer
/// publishes once in all its copying: looking after every hint costs that writer nothing, and a look every 256 hints
/// made a 1.5 MiB message, three pieces of a 512 KiB ring, some 7% slower.
fn spinData(r: Ring, amount: u64, pace: u32) bool {
    const tail = r.tail();
    if (enough(.data, r.cap, r.head() -% tail, amount)) return true;
    for (0..pace) |_| std.atomic.spinLoopHint();
    for (pace..spin_iters) |_| {
        if (enough(.data, r.cap, r.head() -% tail, amount)) return true;
        std.atomic.spinLoopHint();
    }
    return false;
}

/// The waiter's side of the sleep protocol (docs/protocol.md §6.1): sets the sleep flag, then looks at the ring,
/// `requests` and `status` again. `.sleep` only if nothing changed; otherwise the flag is cleared again (a swap too, so
/// that every change of the flag is one) and the change reported.
fn announceSleep(comptime need: Need, hot: *const Hot, r: Ring, amount: u64) Error!enum { ready, sleep } {
    const flag = sleeper(r, need).flag;
    _ = flag.swap(1, .acq_rel);
    const is_ready = ready(need, r, amount) or (stopped(need, hot, r, amount) catch |err| {
        _ = flag.swap(0, .acq_rel);
        return err;
    });
    if (!is_ready) return .sleep;
    _ = flag.swap(0, .acq_rel);
    return .ready;
}

/// Whether `r` has `amount` (at most `cap`) bytes to read or free. Never while the ring holds more than `cap` bytes:
/// `head -% tail` is the bytes in the ring as long as the indices are sound.
inline fn ready(comptime need: Need, r: Ring, amount: u64) bool {
    return enough(need, r.cap, r.head() -% r.tail(), amount);
}

/// Whether a ring of `cap` bytes that holds `used` has `amount` bytes to read or free.
inline fn enough(comptime need: Need, cap: u32, used: u64, amount: u64) bool {
    return switch (need) {
        // amount <= used <= cap, in one comparison: below `amount`, `used -% amount` wraps past `cap`
        .data => used -% amount <= cap - amount,
        .space => used <= cap - amount,
    };
}

/// Whether the ring holds more than `cap` bytes: its tail is past its head, or its indices are garbage.
fn corrupt(r: Ring) bool {
    return r.head() -% r.tail() > r.cap;
}

/// Whether the wait ends without its sleep: Invalid on a corrupt ring; Cancelled after `fipc_cancel`; once the
/// session has ended (PEER_DEAD), true if the ring is ready after all (looked at after the phase, so it holds
/// everything the peer published before it died), else Disconnected; false while the session runs.
fn stopped(comptime need: Need, hot: *const Hot, r: Ring, amount: u64) Error!bool {
    if (corrupt(r)) return error.Invalid;
    if (hot.requests.load(.acquire).cancel) return error.Cancelled;
    return switch (hot.status.load(.acquire).phase) {
        .handshake, .connected => false,
        .peer_dead => if (ready(need, r, amount)) true else error.Disconnected,
    };
}

/// How long the next sleep may last: a slice, or the rest of the call's timeout if that is shorter (the first call
/// takes the deadline). Timeout at once for a timeout of 0, and once the deadline has passed; a negative timeout waits
/// forever.
fn nextSleepMs(io: std.Io, deadline: *Deadline) Error!u32 {
    if (deadline.timeout_ms == 0) return error.Timeout;
    if (deadline.timeout_ms < 0) return slice_ms;
    const now = std.Io.Clock.awake.now(io);
    if (deadline.at.nanoseconds == 0) deadline.at = now.addDuration(.fromMilliseconds(deadline.timeout_ms));
    const left_ns = now.durationTo(deadline.at).nanoseconds;
    if (left_ns <= 0) return error.Timeout;
    return sliceMs(left_ns);
}

/// The sleep for `left_ns` (> 0) nanoseconds of timeout: rounded up to whole milliseconds, at most a slice.
fn sliceMs(left_ns: i96) u32 {
    return @intCast(@divCeil(@min(left_ns, slice_ms * std.time.ns_per_ms), std.time.ns_per_ms));
}

// === Waking ===

/// The writer's publish (docs/protocol.md §6.1): stores `head`, then wakes the ring's reader if it sleeps.
pub inline fn publishHead(hot: *const Hot, r: Ring, head: u64) void {
    r.ctrl.writer.head.store(head, .release);
    wakeIfAsleep(hot, sleeper(r, .data));
}

/// The reader's publish (the space it consumed): stores `tail`, then wakes the ring's writer if it sleeps.
pub inline fn publishTail(hot: *const Hot, r: Ring, tail: u64) void {
    r.ctrl.reader.tail.store(tail, .release);
    wakeIfAsleep(hot, sleeper(r, .space));
}

/// The local interrupt (docs/protocol.md §6.2): after the caller published what they must see (`requests.cancel`, or
/// PEER_DEAD in `status`), wakes this side's two sleepers of the view's session, the reader of its inbound ring and the
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

/// Out of line: the system call, for the flag's owner is usually asleep when it is set. The sleeper comes in two
/// registers, not as a struct, which the ABI would pass in memory, built on every publish.
noinline fn wake(hot: *const Hot, flag: *Flag, object: names.Object) void {
    if (build_options.test_hooks) {
        _ = wakeCount(flag).fetchAdd(1, .monotonic);
        wakeTime(flag).store(nowNs(), .monotonic);
    }
    platform.ringWake(flag, eventOf(hot, .{ .flag = flag, .object = object }));
}

// === Diagnostics (test hooks only) ===

/// The wakes sent per flag and the last one's time, in slots by the flag's address (a collision only over-counts).
var wake_counts: [64]std.atomic.Value(u32) = @splat(.init(0));
var wake_times: [64]std.atomic.Value(u64) = @splat(.init(0));

fn wakeCount(flag: *const Flag) *std.atomic.Value(u32) {
    return &wake_counts[(@intFromPtr(flag) >> 2) % wake_counts.len];
}

fn wakeTime(flag: *const Flag) *std.atomic.Value(u64) {
    return &wake_times[(@intFromPtr(flag) >> 2) % wake_times.len];
}

/// The monotonic clock in nanoseconds, without `Io` (the publisher has none at hand); 0 on Windows.
fn nowNs() u64 {
    if (builtin.os.tag == .windows) return 0;
    var ts: std.c.timespec = undefined;
    _ = std.c.clock_gettime(.MONOTONIC, &ts);
    return @as(u64, @intCast(ts.sec)) * std.time.ns_per_s + @as(u64, @intCast(ts.nsec));
}

/// `platform.ringWait`, reporting a sleep that ended 90 ms or more after it began with the bytes already there, unless a
/// wake ended it at once: whether it timed out, the flag's value, and how many wakes were sent for the flag while it
/// slept. A timeout after a wake
/// means the OS lost it; a timeout with none means the publisher never woke the sleeper; no timeout means it was woken
/// and ran late.
fn diagnosed(comptime need: Need, r: Ring, amount: u64, flag: *Flag, event: platform.RingEvent, sleep_ms: u32) void {
    const wakes_before = wakeCount(flag).load(.monotonic);
    const start_ns = nowNs();
    const timed_out = platform.ringWait(flag, event, sleep_ms);
    const end_ns = nowNs();
    if (end_ns -% start_ns < 90 * std.time.ns_per_ms or !ready(need, r, amount)) return;
    const wakes = wakeCount(flag).load(.monotonic) -% wakes_before;
    const wake_ns = wakeTime(flag).load(.monotonic);
    // A wake that came late (a slow publisher) is no fault of the wait: only a timeout after a wake, a timeout with no
    // wake, or a return long after its wake is reported
    if (!timed_out and wakes != 0 and end_ns -% wake_ns < 10 * std.time.ns_per_ms) return;
    // The last wake's time since the sleep began, and the return's since that wake (signed: a wake may come after)
    const wake_ms: i64 = if (wakes == 0) -1 else @divTrunc(@as(i64, @bitCast(wake_ns -% start_ns)), std.time.ns_per_ms);
    const after_ms: i64 = if (wakes == 0) -1 else @divTrunc(@as(i64, @bitCast(end_ns -% wake_ns)), std.time.ns_per_ms);
    std.debug.print("ring wait ({s}, flag 0x{x}): slept {d} ms of {d}, timed out: {}, flag now {d}, wakes sent meanwhile: {d}, " ++
        "the last at +{d} ms, the return {d} ms after it\n", .{
        @tagName(need),
        @intFromPtr(flag),
        (end_ns -% start_ns) / std.time.ns_per_ms,
        sleep_ms,
        timed_out,
        flag.load(.monotonic),
        wakes,
        wake_ms,
        after_ms,
    });
}

// === Sleepers and events ===

/// A ring side that may sleep: its flag and, on Windows, the flag's event in the session's objects.
const Sleeper = struct { flag: *Flag, object: names.Object };

/// The side of `r` that waits for `need`: the reader (waits for data) sleeps on `reader_sleeps`, in the writer's half
/// of the indices; the writer (waits for space) on `writer_sleeps`, in the reader's half.
fn sleeper(r: Ring, comptime need: Need) Sleeper {
    return switch (need) {
        .data => .{ .flag = &r.ctrl.writer.reader_sleeps, .object = if (r.is_s2c) .s2c_data else .c2s_data },
        .space => .{ .flag = &r.ctrl.reader.writer_sleeps, .object = if (r.is_s2c) .s2c_space else .c2s_space },
    };
}

/// The sleeper's ring event, from the view's session.
fn eventOf(hot: *const Hot, who: Sleeper) platform.RingEvent {
    if (@sizeOf(platform.RingEvent) == 0) return platform.no_ring_event; // the flag is all a sleeper needs
    const session = hot.view_session.load(.monotonic) orelse return platform.no_ring_event;
    return session.mapping.ringEvent(who.object);
}

// === Tests ===

test "a sleep is the rest of the timeout, rounded up to a millisecond, at most a slice" {
    try std.testing.expectEqual(@as(u32, 1), sliceMs(1));
    try std.testing.expectEqual(@as(u32, 1), sliceMs(std.time.ns_per_ms));
    try std.testing.expectEqual(@as(u32, 2), sliceMs(std.time.ns_per_ms + 1));
    try std.testing.expectEqual(@as(u32, 100), sliceMs(100 * std.time.ns_per_ms));
    try std.testing.expectEqual(@as(u32, 100), sliceMs(250 * std.time.ns_per_ms));
}

/// A view over a header and rings in this process's memory, for the tests below: no session slot, so no events.
const TestView = struct {
    ctrl: abi.SegmentHeader,
    data: [64]u8,
    hot: Hot,

    fn init(v: *TestView, listener: bool) void {
        v.ctrl = std.mem.zeroes(abi.SegmentHeader);
        v.data = @splat(0);
        v.hot = std.mem.zeroes(Hot);
        v.hot.view_header.store(&v.ctrl, .monotonic);
        v.hot.view_data.store(&v.data, .monotonic);
        v.hot.view_cap.store(32, .monotonic);
        v.hot.view_server.store(@intFromBool(listener), .monotonic);
    }
};

test "a ring is ready only while it holds at most its capacity" {
    var v: TestView = undefined;
    v.init(true); // cap 32
    const r = Ring.forSend(&v.hot);
    const head = &v.ctrl.s2c.writer.head;
    const tail = &v.ctrl.s2c.reader.tail;
    head.store(16, .monotonic);
    try std.testing.expect(ready(.data, r, 16) and !ready(.data, r, 17));
    try std.testing.expect(ready(.space, r, 16) and !ready(.space, r, 17));
    head.store(32, .monotonic); // full
    try std.testing.expect(ready(.data, r, 32) and !ready(.space, r, 1) and !corrupt(r));
    // The tail past the head, or more than a ring between them: never ready, and corrupt
    tail.store(40, .monotonic);
    try std.testing.expect(!ready(.data, r, 16) and !ready(.space, r, 16) and corrupt(r));
    tail.store(0, .monotonic);
    head.store(33, .monotonic);
    try std.testing.expect(!ready(.data, r, 16) and !ready(.space, r, 0) and corrupt(r));
    try std.testing.expectError(error.Invalid, stopped(.data, &v.hot, r, 16));
    // Indices wrap: a ring across 2^64 is ready as any other
    tail.store(std.math.maxInt(u64) - 7, .monotonic);
    head.store(8, .monotonic);
    try std.testing.expect(ready(.data, r, 16) and !ready(.data, r, 17) and !corrupt(r));
}

test "a call's timeout: 0 doesn't wait, negative waits a slice at a time, positive is one deadline for the call" {
    const io = std.testing.io;
    var none: Deadline = .init(0);
    try std.testing.expectError(error.Timeout, nextSleepMs(io, &none));
    var forever: Deadline = .init(-1);
    try std.testing.expectEqual(@as(u32, slice_ms), try nextSleepMs(io, &forever));
    try std.testing.expectEqual(@as(i96, 0), forever.at.nanoseconds);
    var call: Deadline = .init(250);
    try std.testing.expectEqual(@as(u32, slice_ms), try nextSleepMs(io, &call)); // takes the deadline
    const at = call.at;
    try std.testing.expect(at.nanoseconds != 0);
    _ = try nextSleepMs(io, &call);
    try std.testing.expectEqual(at, call.at); // the next wait of the call keeps it
    call.at = std.Io.Clock.awake.now(io); // past
    try std.testing.expectError(error.Timeout, nextSleepMs(io, &call));
}

test "the local interrupt clears only this side's two sleep flags" {
    var v: TestView = undefined;
    v.init(true); // the server: it reads c2s and writes s2c
    const ctrl = &v.ctrl;
    for ([_]*Flag{ &ctrl.s2c.writer.reader_sleeps, &ctrl.s2c.reader.writer_sleeps, &ctrl.c2s.writer.reader_sleeps, &ctrl.c2s.reader.writer_sleeps }) |flag| flag.store(1, .monotonic);
    wakeOwnWaiters(&v.hot);
    try std.testing.expectEqual(@as(u32, 0), ctrl.c2s.writer.reader_sleeps.load(.monotonic));
    try std.testing.expectEqual(@as(u32, 0), ctrl.s2c.reader.writer_sleeps.load(.monotonic));
    try std.testing.expectEqual(@as(u32, 1), ctrl.s2c.writer.reader_sleeps.load(.monotonic));
    try std.testing.expectEqual(@as(u32, 1), ctrl.c2s.reader.writer_sleeps.load(.monotonic));
}

test "after the session ends a waiter drains what was published, then gets Disconnected; cancel comes first" {
    var v: TestView = undefined;
    v.init(false); // the client: it reads s2c
    const r = Ring.forRecv(&v.hot);
    try std.testing.expectEqual(false, try stopped(.data, &v.hot, r, 16));
    v.hot.status.store(endpoint.Status{ .phase = .peer_dead }, .monotonic);
    try std.testing.expectError(error.Disconnected, stopped(.data, &v.hot, r, 16));
    v.ctrl.s2c.writer.head.store(16, .monotonic); // a message the peer completed before it died
    try std.testing.expectEqual(true, try stopped(.data, &v.hot, r, 16));
    try std.testing.expectEqual(.ready, try announceSleep(.data, &v.hot, r, 16));
    try std.testing.expectError(error.Disconnected, announceSleep(.data, &v.hot, r, 32));
    try std.testing.expectEqual(@as(u32, 0), v.ctrl.s2c.writer.reader_sleeps.load(.monotonic)); // given up, not left set
    v.hot.requests.store(endpoint.Requests{ .cancel = true }, .monotonic);
    try std.testing.expectError(error.Cancelled, stopped(.data, &v.hot, r, 16));
}

test "no wake-up is lost: store-buffering litmus of the publisher and the waiter" {
    // Races the two halves of the sleep protocol (docs/protocol.md §6.1) over one ring, round after round: a writer
    // publishes 16 bytes while the reader announces its sleep. A lost wake-up is a reader that decides to sleep while
    // its flag is still set once the writer is done: the writer's swap came first, yet the reader's index loads missed
    // the index, and nobody will wake the reader. The same race with a weaker pair (`publishWeak`: a `.release`
    // index store, then an `.acq_rel` CAS; `announceWeak`: a `.release` flag store, then `.acquire` index loads)
    // shows that the harness catches the reordering on this machine: the reader's flag store can still sit in its
    // store buffer while its index loads run.
    const rounds = 200_000;
    const budget_ns = 2 * std.time.ns_per_s;
    const library = Litmus.run(publishLitmusHead, announceLitmusSleep, rounds, budget_ns);
    const weak = Litmus.run(publishWeak, announceWeak, rounds, budget_ns);
    std.debug.print("  litmus: {d} of {d} wake-ups lost with a plain flag store, {d} of {d} with the library's swaps\n", .{
        weak.lost, weak.rounds, library.lost, library.rounds,
    });
    try std.testing.expectEqual(@as(u32, 0), library.lost);
    // Without a reordering to catch, the zero above says nothing about this machine (TSan, for example, may order the
    // race itself)
    if (weak.lost == 0) return error.SkipZigTest;
}

/// The harness of the litmus test: one ring, a writer thread and a reader thread, each round started together by a
/// spin barrier and staggered by a random spin, so that the writer's publish and the reader's announcement overlap.
const Litmus = struct {
    view: TestView,
    arrivals: std.atomic.Value(u32) align(64),
    /// Set by the reader before a round's start barrier: that round doesn't run, and both loops end.
    stop: bool,

    const Outcome = struct { lost: u32, rounds: u32 };
    const Publish = fn (*Litmus) void;
    /// True: the reader decides to sleep.
    const Announce = fn (*Litmus) bool;

    fn run(comptime publish: Publish, comptime announce: Announce, rounds: u32, budget_ns: u64) Outcome {
        const l = std.testing.allocator.create(Litmus) catch @panic("OOM");
        defer std.testing.allocator.destroy(l);
        l.view.init(true); // the server writes s2c
        l.arrivals = .init(0);
        l.stop = false;

        const writer = std.Thread.spawn(.{}, writerLoop, .{ l, publish }) catch @panic("thread");
        const outcome = l.readerLoop(announce, rounds, budget_ns);
        writer.join();
        return outcome;
    }

    fn ring(l: *Litmus) Ring {
        return Ring.forSend(&l.view.hot);
    }

    /// Two-party barrier number `n` (1, 2, ...).
    fn barrier(l: *Litmus, n: u32) void {
        _ = l.arrivals.fetchAdd(1, .acq_rel);
        while (l.arrivals.load(.acquire) < 2 * n) std.atomic.spinLoopHint();
    }

    fn stagger(state: *u64) void {
        state.* = state.* *% 6364136223846793005 +% 1442695040888963407;
        for (0..(state.* >> 33) % 64) |_| std.atomic.spinLoopHint();
    }

    fn writerLoop(l: *Litmus, comptime publish: Publish) void {
        var random: u64 = 0x5eed;
        var n: u32 = 0;
        while (true) {
            n += 1;
            l.barrier(n); // start
            if (l.stop) return;
            stagger(&random);
            publish(l);
            n += 1;
            l.barrier(n); // end
        }
    }

    fn readerLoop(l: *Litmus, comptime announce: Announce, rounds: u32, budget_ns: u64) Outcome {
        const io = std.testing.io;
        const start = std.Io.Clock.awake.now(io);
        var random: u64 = 0xfeed;
        var outcome: Outcome = .{ .lost = 0, .rounds = 0 };
        var n: u32 = 0;
        while (true) {
            const r = l.ring();
            r.ctrl.writer.head.store(0, .monotonic);
            r.ctrl.writer.reader_sleeps.store(0, .monotonic);
            l.stop = outcome.rounds == rounds or
                (outcome.rounds % 1024 == 0 and start.durationTo(std.Io.Clock.awake.now(io)).nanoseconds > budget_ns);
            n += 1;
            l.barrier(n); // start
            if (l.stop) return outcome;
            stagger(&random);
            const sleeping = announce(l);
            n += 1;
            l.barrier(n); // end
            if (sleeping and r.ctrl.writer.reader_sleeps.load(.monotonic) == 1) outcome.lost += 1;
            outcome.rounds += 1;
        }
    }
};

fn publishLitmusHead(l: *Litmus) void {
    publishHead(&l.view.hot, l.ring(), 16);
}

fn announceLitmusSleep(l: *Litmus) bool {
    return (announceSleep(.data, &l.view.hot, l.ring(), 16) catch unreachable) == .sleep;
}

/// The weaker pair's publish: a `.release` store of the index, then an unconditional CAS of the flag.
fn publishWeak(l: *Litmus) void {
    const r = l.ring();
    r.ctrl.writer.head.store(16, .release);
    _ = r.ctrl.writer.reader_sleeps.cmpxchgStrong(1, 0, .acq_rel, .acquire);
}

/// The weaker pair's announcement: a `.release` flag store, then `.acquire` index loads.
fn announceWeak(l: *Litmus) bool {
    const r = l.ring();
    r.ctrl.writer.reader_sleeps.store(1, .release);
    const head = r.ctrl.writer.head.load(.acquire);
    const tail = r.ctrl.reader.tail.load(.acquire);
    if (head -% tail >= 16) {
        r.ctrl.writer.reader_sleeps.store(0, .release);
        return false;
    }
    return true;
}
