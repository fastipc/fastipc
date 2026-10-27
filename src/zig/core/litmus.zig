//! The litmus tests' harness, which every kind's litmus tests share (broadcast/litmus_test.zig,
//! queue/litmus_test.zig): it races the library's two halves of one pattern, round after round, two threads started
//! together by a spin barrier and staggered by a random spin, and counts the forbidden outcomes, which must be none.
//! Each test also races a weaker variant (orderings the library doesn't use) that must show the forbidden outcome on
//! this machine: if it doesn't, the zero says nothing here and the test is skipped. On x86-64 the store-buffering
//! cases have informative weak variants; the message-passing cases need a weakly ordered machine, an aarch64 one, to
//! say anything.

const std = @import("std");
const testing = std.testing;

const Atomic = std.atomic.Value;

/// Each variant's time.
const budget_ns = 150 * std.time.ns_per_ms;
const max_rounds = 200_000;

pub const Outcome = struct { lost: u32 = 0, rounds: u32 = 0 };

/// Races `case.a` (on a thread of its own) against `case.b`, after `case.reset`, until the budget or `max_rounds`;
/// `case.lost` judges each round once both are done.
pub fn race(case: anytype) Outcome {
    const Case = @TypeOf(case.*);
    const Shared = struct {
        arrivals: Atomic(u32) align(64) = .init(0),
        stop: Atomic(bool) = .init(false),

        fn barrier(shared: *@This(), n: u32) void {
            _ = shared.arrivals.fetchAdd(1, .acq_rel);
            while (shared.arrivals.load(.acquire) < 2 * n) std.atomic.spinLoopHint();
        }

        fn stagger(state: *u64) void {
            state.* = state.* *% 6364136223846793005 +% 1442695040888963407;
            for (0..(state.* >> 33) % 64) |_| std.atomic.spinLoopHint();
        }

        fn other(shared: *@This(), raced: *Case) void {
            var random: u64 = 0x5eed;
            var n: u32 = 0;
            while (true) {
                n += 1;
                shared.barrier(n);
                if (shared.stop.load(.acquire)) return;
                stagger(&random);
                raced.a();
                n += 1;
                shared.barrier(n);
            }
        }
    };
    var shared: Shared = .{};
    const thread = std.Thread.spawn(.{}, Shared.other, .{ &shared, case }) catch @panic("thread");
    const io = testing.io;
    const start = std.Io.Clock.awake.now(io);
    var random: u64 = 0xfeed;
    var outcome: Outcome = .{};
    var n: u32 = 0;
    while (true) {
        case.reset();
        const stop = outcome.rounds == max_rounds or
            (outcome.rounds % 1024 == 0 and start.durationTo(std.Io.Clock.awake.now(io)).nanoseconds > budget_ns);
        shared.stop.store(stop, .release);
        n += 1;
        shared.barrier(n);
        if (stop) break;
        Shared.stagger(&random);
        case.b();
        n += 1;
        shared.barrier(n);
        if (case.lost()) outcome.lost += 1;
        outcome.rounds += 1;
    }
    thread.join();
    return outcome;
}

/// Reports both outcomes. Fails on a lost round of the library's pair, and skips the test when the weaker pair lost
/// none (the zero then says nothing on this machine).
pub fn expectNoneLost(comptime what: []const u8, library: Outcome, weak: Outcome) !void {
    std.debug.print("  litmus " ++ what ++ ": {d} of {d} lost with the weaker pair, {d} of {d} with the library's\n", .{
        weak.lost, weak.rounds, library.lost, library.rounds,
    });
    try testing.expectEqual(@as(u32, 0), library.lost);
    if (weak.lost == 0) return error.SkipZigTest;
}
