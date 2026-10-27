//! Lost-wake diagnostics of the ring waits (ring_wait.zig), in the tests' builds only (`build_options.test_hooks`):
//! the wakes sent per sleep flag and the last one's time (`noteWake`), and a report of a sleep that ended 90 ms or more
//! after it began with its bytes already there (`Sleep`). tests/zig/stream/wake_test.zig provokes such sleeps.

const std = @import("std");
const platform = @import("../platform.zig");

const Flag = std.atomic.Value(u32);

/// The wakes sent per flag and the last one's time, in entries by the flag's address (a collision only over-counts).
var wake_counts: [64]std.atomic.Value(u32) = @splat(.init(0));
var wake_times: [64]std.atomic.Value(u64) = @splat(.init(0));

fn wakeCount(flag: *const Flag) *std.atomic.Value(u32) {
    return &wake_counts[(@intFromPtr(flag) >> 2) % wake_counts.len];
}

fn wakeTime(flag: *const Flag) *std.atomic.Value(u64) {
    return &wake_times[(@intFromPtr(flag) >> 2) % wake_times.len];
}

/// A wake of `flag`'s sleeper is being sent.
pub fn noteWake(flag: *const Flag) void {
    _ = wakeCount(flag).fetchAdd(1, .monotonic);
    wakeTime(flag).store(platform.monotonicNs(), .monotonic);
}

/// One sleep on a flag, watched: `begin` just before `platform.ringWait`, `end` just after it.
pub const Sleep = struct {
    flag: *const Flag,
    wakes_before: u32,
    start_ns: u64,

    pub fn begin(flag: *const Flag) Sleep {
        return .{ .flag = flag, .wakes_before = wakeCount(flag).load(.monotonic), .start_ns = platform.monotonicNs() };
    }

    /// Reports the sleep if it ended 90 ms or more after it began with the bytes already there (`bytes_there`),
    /// unless a wake ended it at once: whether it timed out, the flag's value, and how many wakes were sent for the flag
    /// while it slept. A timeout after a wake means the OS lost it; a timeout with none means the publisher never woke
    /// the sleeper; no timeout means it was woken and ran late. `need`: what the sleeper waited for.
    pub fn end(sleep: Sleep, need: []const u8, timed_out: bool, sleep_ms: u32, bytes_there: bool) void {
        const end_ns = platform.monotonicNs();
        if (end_ns -% sleep.start_ns < 90 * std.time.ns_per_ms or !bytes_there) return;
        const wakes = wakeCount(sleep.flag).load(.monotonic) -% sleep.wakes_before;
        const wake_ns = wakeTime(sleep.flag).load(.monotonic);
        // A wake that came late (a slow publisher) is no fault of the wait: only a timeout after a wake, a timeout with
        // no wake, or a return long after its wake is reported
        if (!timed_out and wakes != 0 and end_ns -% wake_ns < 10 * std.time.ns_per_ms) return;
        // The last wake's time since the sleep began, and the return's since that wake (signed: a wake may come after)
        const wake_ms: i64 = if (wakes == 0) -1 else @divTrunc(@as(i64, @bitCast(wake_ns -% sleep.start_ns)), std.time.ns_per_ms);
        const after_ms: i64 = if (wakes == 0) -1 else @divTrunc(@as(i64, @bitCast(end_ns -% wake_ns)), std.time.ns_per_ms);
        std.debug.print("ring wait ({s}, flag 0x{x}): slept {d} ms of {d}, timed out: {}, flag now {d}, wakes sent meanwhile: {d}, " ++
            "the last at +{d} ms, the return {d} ms after it\n", .{
            need,
            @intFromPtr(sleep.flag),
            (end_ns -% sleep.start_ns) / std.time.ns_per_ms,
            sleep_ms,
            timed_out,
            sleep.flag.load(.monotonic),
            wakes,
            wake_ms,
            after_ms,
        });
    }
};
