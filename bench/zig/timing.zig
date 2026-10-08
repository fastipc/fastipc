//! Clocks and statistics.
//!
//! Durations come from the OS monotonic clock through `std.Io` (`Io.Clock.awake`: CLOCK_MONOTONIC,
//! QueryPerformanceCounter). Latency samples come from the CPU's time-stamp counter: its ticks resolve
//! the sub-microsecond round trips that QueryPerformanceCounter's 100 ns steps would quantize. The
//! counter is invariant and synchronized across cores on every supported CPU (x86-64-v3,
//! docs/platform-support.md), so a reading taken in one process can be compared with another's; ticks
//! become nanoseconds at the rate measured against the monotonic clock over the same run. On Apple Silicon
//! (aarch64-macos) the samples come from the generic timer's virtual counter (`cntvct_el0`), shared by every
//! core, which ticks at 24 MHz: a latency resolves to about 42 ns. ARM64 Linux reads the same counter (the kernel
//! lets user space read it); its rate is the board's (`cntfrq_el0`, often 19.2 to 1000 MHz), measured as on x86-64.

const std = @import("std");
const builtin = @import("builtin");
const Io = std.Io;

comptime {
    if (builtin.cpu.arch != .x86_64 and !(builtin.cpu.arch == .aarch64 and (builtin.os.tag == .macos or builtin.os.tag == .linux)))
        @compileError("FastIPC supports x86-64, and aarch64 on Linux and macOS (docs/platform-support.md)");
}

pub const ns_per_s = std.time.ns_per_s;

/// Nanoseconds on the monotonic clock.
pub fn now(io: Io) u64 {
    return @intCast(Io.Clock.awake.now(io).nanoseconds);
}

/// The time-stamp counter. `rdtscp` waits for the instructions before it to complete.
pub inline fn ticks() u64 {
    if (builtin.cpu.arch == .aarch64) return virtualCount();
    var low: u32 = undefined;
    var high: u32 = undefined;
    asm volatile ("rdtscp"
        : [low] "={eax}" (low),
          [high] "={edx}" (high),
        :
        : .{ .rcx = true });
    return @as(u64, high) << 32 | low;
}

/// aarch64: the virtual counter, after an `isb`, so the read waits for the instructions before it.
inline fn virtualCount() u64 {
    return asm volatile (
        \\isb
        \\mrs %[count], cntvct_el0
        : [count] "=r" (-> u64),
    );
}

/// A monotonic-clock reading and a tick reading taken together.
pub const Stamp = struct {
    ns: u64,
    ticks: u64,

    pub fn take(io: Io) Stamp {
        const before = ticks();
        const ns = now(io);
        const after = ticks();
        return .{ .ns = ns, .ticks = before + (after - before) / 2 };
    }

    /// Nanoseconds per tick from `from` to `to`: the counter's rate over that interval.
    pub fn nsPerTick(from: Stamp, to: Stamp) f64 {
        return seconds(to.ns - from.ns) * ns_per_s / @as(f64, @floatFromInt(to.ticks - from.ticks));
    }
};

pub fn seconds(ns: u64) f64 {
    return @as(f64, @floatFromInt(ns)) / ns_per_s;
}

pub fn nanoseconds(s: f64) u64 {
    return @intFromFloat(s * ns_per_s);
}

/// Busy-waits until the monotonic clock reaches `deadline_ns`.
pub fn spinUntil(io: Io, deadline_ns: u64) void {
    while (now(io) < deadline_ns) std.atomic.spinLoopHint();
}

/// The median and range of a case's runs.
pub const Spread = struct {
    median: f64,
    min: f64,
    max: f64,

    pub fn of(gpa: std.mem.Allocator, values: []const f64) !Spread {
        std.debug.assert(values.len > 0);
        const sorted = try gpa.dupe(f64, values);
        defer gpa.free(sorted);
        std.mem.sort(f64, sorted, {}, std.sort.asc(f64));
        const mid = sorted.len / 2;
        return .{
            .median = if (sorted.len % 2 == 1) sorted[mid] else (sorted[mid - 1] + sorted[mid]) / 2,
            .min = sorted[0],
            .max = sorted[sorted.len - 1],
        };
    }
};

/// The nearest-rank quantile `q` (0 < q <= 1) of sorted samples: the smallest sample that at least
/// q of all samples don't exceed.
pub fn quantile(sorted: []const u64, q: f64) u64 {
    std.debug.assert(sorted.len > 0);
    const rank: usize = @intFromFloat(@ceil(q * @as(f64, @floatFromInt(sorted.len))));
    return sorted[@min(sorted.len, @max(rank, 1)) - 1];
}

test "the median of an even count is the mean of the middle two" {
    const gpa = std.testing.allocator;
    try std.testing.expectEqual(Spread{ .median = 2, .min = 1, .max = 3 }, try Spread.of(gpa, &.{ 3, 1, 2 }));
    try std.testing.expectEqual(Spread{ .median = 2.5, .min = 1, .max = 4 }, try Spread.of(gpa, &.{ 4, 1, 3, 2 }));
}

test "quantiles use the nearest rank" {
    const samples = [_]u64{ 10, 20, 30, 40, 50, 60, 70, 80, 90, 100 };
    try std.testing.expectEqual(@as(u64, 50), quantile(&samples, 0.5));
    try std.testing.expectEqual(@as(u64, 100), quantile(&samples, 0.99));
    try std.testing.expectEqual(@as(u64, 10), quantile(&samples, 0.01));
    try std.testing.expectEqual(@as(u64, 7), quantile(&[_]u64{7}, 0.999));
}
