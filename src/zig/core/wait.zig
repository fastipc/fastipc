//! The waits every kind's data calls share: a call's deadline, the spin before a sleep, and the sleep itself. A sleep
//! lasts one slice (100 ms) at most. Between slices the waiter looks again and returns to `Io` through its
//! cancelation check, so a task canceled through its `Io` ends its wait within a slice (docs/protocol.md §6.1; Zig
//! 0.17's `Io` has no yield to offer besides). Every wake-up the protocol needs is explicit; the slice is only a
//! backstop. What a wait waits for, and how the peer wakes it, is up to the kind that calls these.
//!
//! Also here: the deadlines of the waits that take one deadline for the whole call (a handshake's, an owner's wait for
//! members, its poller's), the milliseconds left before one (`msLeft`), and the one rounding of nanoseconds to
//! milliseconds that every wait uses (`msRoundedUp`).

const std = @import("std");
const builtin = @import("builtin");
const build_options = @import("build_options");

/// A data call's timeout, which bounds the whole call, however many waits it makes: C's milliseconds (0: the
/// call doesn't wait; negative: no limit) and the deadline, taken by the first wait that doesn't find its bytes in its
/// first spin (null until then), so a call that never waits reads no clock.
pub const Deadline = struct {
    timeout_ms: c_int,
    at: ?std.Io.Timestamp = null,

    pub fn init(timeout_ms: c_int) Deadline {
        return .{ .timeout_ms = timeout_ms };
    }
};

/// Spins before sleeping, to catch a fast peer without a system call.
pub const spin_iters = 4096;
/// How often a reader that waits for a stream of data (not for an answer) looks during its kind's spin: once at the
/// start, then only after this many spin hints, then after every hint. 1 turns the pacing off. It is set per platform
/// (`stream_paces`), because a hint's length differs: 256 `isb`s on an M1 take about 2 µs, 256 `pause`s on x86 about
/// 10 µs. The build option `stream_pace` overrides it, for tuning.
pub const stream_pace: u32 = build_options.stream_pace orelse streamPaceOf(builtin.os.tag, builtin.cpu.arch);
/// The paces measured with `devtool bench-pace`. A platform not listed, or listed with 1, isn't paced. Each is the
/// largest pace that speeds up small copying streams (16-256 B; about twice as fast at 64 B) without slowing the round
/// trip noticeably. On x86, Linux takes 32 because 64 slows the round trip's median by 2-9%. On arm64, where 1 KiB
/// gains too, the largest pace that is fastest at 1 KiB without slowing 1.5 MiB or the round trip: 512 (1024 slows
/// 1.5 MiB). The measurements are in docs/perf/baseline.md ("The stream pace on x86", "The stream pace on arm64").
/// `zig build -Dstream_pace=N` builds another pace, for other hardware.
const stream_paces = [_]struct { os: std.Target.Os.Tag, arch: std.Target.Cpu.Arch, pace: u32 }{
    .{ .os = .macos, .arch = .aarch64, .pace = 512 },
    .{ .os = .linux, .arch = .aarch64, .pace = 512 },
    .{ .os = .linux, .arch = .x86_64, .pace = 32 },
    .{ .os = .windows, .arch = .x86_64, .pace = 64 },
};

fn streamPaceOf(os: std.Target.Os.Tag, arch: std.Target.Cpu.Arch) u32 {
    for (stream_paces) |entry| if (entry.os == os and entry.arch == arch) return entry.pace;
    return 1;
}

/// The longest sleep between two looks (docs/protocol.md §6.1).
pub const slice_ms = 100;

/// How long the next sleep may last: a slice, or the rest of the call's timeout if that is shorter (the first call
/// takes the deadline). Timeout at once for a timeout of 0, and once the deadline has passed; a negative timeout waits
/// forever.
pub fn nextSleepMs(io: std.Io, deadline: *Deadline) error{Timeout}!u32 {
    return sleepMsWithin(io, deadline, slice_ms);
}

/// `nextSleepMs` with a slice of `longest_ms` (at least 1).
pub fn sleepMsWithin(io: std.Io, deadline: *Deadline, longest_ms: u32) error{Timeout}!u32 {
    if (deadline.timeout_ms == 0) return error.Timeout;
    if (deadline.timeout_ms < 0) return longest_ms;
    const now = std.Io.Clock.awake.now(io);
    if (deadline.at == null) deadline.at = now.addDuration(.fromMilliseconds(deadline.timeout_ms));
    const left_ns = now.durationTo(deadline.at.?).nanoseconds;
    if (left_ns <= 0) return error.Timeout;
    return msRoundedUp(left_ns, longest_ms);
}

/// What is left of a call's `deadline`, as C's milliseconds, for a call made within it (a batch's message that goes
/// through a call of its own): the call's timeout until one of its waits took the deadline, 0 and negative ones
/// included; then the milliseconds left, rounded up, 0 once it has passed.
pub fn msLeftOf(io: std.Io, deadline: *const Deadline) c_int {
    const at = deadline.at orelse return deadline.timeout_ms;
    return @intCast(msRoundedUp(std.Io.Clock.awake.now(io).durationTo(at).nanoseconds, std.math.maxInt(c_int)));
}

/// Back in `Io` between a wait's slices: a task canceled through it ends its wait as a cancel would, and the
/// cancelation stays pending for the task's next cancelation point.
pub fn backInIo(io: std.Io) error{Cancelled}!void {
    io.checkCancel() catch {
        io.recancel();
        return error.Cancelled;
    };
}

// === Deadlines of the waits that take one whole ===

/// The deadline of a C timeout of `timeout_ms` milliseconds from now; null for a negative timeout, which has none.
pub fn deadlineFromMs(io: std.Io, timeout_ms: c_int) ?std.Io.Clock.Timestamp {
    if (timeout_ms < 0) return null;
    return .fromNow(io, .{ .raw = .fromMilliseconds(timeout_ms), .clock = .awake });
}

/// What a wait may take before `deadline` (null: no deadline), as the platform's waits take it: null waits for ever,
/// 0 only looks.
pub fn msLeft(io: std.Io, deadline: ?std.Io.Clock.Timestamp) ?u32 {
    const at = deadline orelse return null;
    return msRoundedUp(at.durationFromNow(io).raw.nanoseconds, std.math.maxInt(i32));
}

/// Whether `deadline` (null: none) has passed.
pub fn passed(io: std.Io, deadline: ?std.Io.Clock.Timestamp) bool {
    return (msLeft(io, deadline) orelse return false) == 0;
}

/// `ns` nanoseconds as whole milliseconds, rounded up (a wait never ends before its time), at most `max_ms`; 0 when
/// nothing is left (`ns` at most 0).
pub fn msRoundedUp(ns: i96, max_ms: u32) u32 {
    if (ns <= 0) return 0;
    return @intCast(@divCeil(@min(ns, @as(i96, max_ms) * std.time.ns_per_ms), std.time.ns_per_ms));
}

// === Tests ===

test "a sleep is the rest of the timeout, rounded up to a millisecond, at most a slice" {
    try std.testing.expectEqual(@as(u32, 1), msRoundedUp(1, slice_ms));
    try std.testing.expectEqual(@as(u32, 1), msRoundedUp(std.time.ns_per_ms, slice_ms));
    try std.testing.expectEqual(@as(u32, 2), msRoundedUp(std.time.ns_per_ms + 1, slice_ms));
    try std.testing.expectEqual(@as(u32, 100), msRoundedUp(100 * std.time.ns_per_ms, slice_ms));
    try std.testing.expectEqual(@as(u32, 100), msRoundedUp(250 * std.time.ns_per_ms, slice_ms));
    try std.testing.expectEqual(@as(u32, 0), msRoundedUp(0, slice_ms));
    try std.testing.expectEqual(@as(u32, 0), msRoundedUp(-1, slice_ms));
}

test "a call's timeout: 0 doesn't wait, negative waits a slice at a time, positive is one deadline for the call" {
    const io = std.testing.io;
    var none: Deadline = .init(0);
    try std.testing.expectError(error.Timeout, nextSleepMs(io, &none));
    var forever: Deadline = .init(-1);
    try std.testing.expectEqual(@as(u32, slice_ms), try nextSleepMs(io, &forever));
    try std.testing.expectEqual(null, forever.at);
    var call: Deadline = .init(250);
    try std.testing.expectEqual(@as(u32, slice_ms), try nextSleepMs(io, &call)); // takes the deadline
    const at = call.at;
    try std.testing.expect(at != null);
    _ = try nextSleepMs(io, &call);
    try std.testing.expectEqual(at, call.at); // the next wait of the call keeps it
    call.at = std.Io.Clock.awake.now(io); // past
    try std.testing.expectError(error.Timeout, nextSleepMs(io, &call));
}
