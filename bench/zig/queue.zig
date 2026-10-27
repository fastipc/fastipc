//! A work queue: `producers` producer threads send messages of one size as fast as they can, and `consumers` consumer
//! threads take them, each producer and consumer a member joined through `fipc_queue_join` (its own mapping of the
//! queue, as in another process), the queue owned by this thread (`fipc_queue_create`).
//!
//! The producers warm up (run 0, `warmup_ns`), then send `runs` runs of about `duration_ns` each (with `messages`
//! set, that many between them), copying (`fipc_send`) or in place (`fipc_send_acquire`/`_commit`). The consumers
//! receive with `fipc_recv`, or in place (`fipc_recv_acquire`/`_release`, each message copied out unless --no-touch).
//! A run ends once the consumers have taken every message it sent: its rate is the messages over the time from its
//! start to then. A run's metrics: that rate, and the rate per producer.
//!
//! Each member counts what it sent or took in a line of its own, so the counts add no shared write to the hot path.

const std = @import("std");
const fipc = @import("fipc.zig");
const scenarios = @import("scenarios.zig");
const throughput = @import("throughput.zig");
const timing = @import("timing.zig");

const Ctx = scenarios.Ctx;
const Results = scenarios.Results;
const Scenario = scenarios.Scenario;

const join_timeout_ms = throughput.connect_timeout_ms;
/// The most producers and consumers a scenario starts.
const max_members = 8;

/// A member's count, alone in its cache line.
const Count = struct {
    value: std.atomic.Value(u64) align(std.atomic.cache_line) = .init(0),
};

/// What the owner's thread and the members share.
const Shared = struct {
    ctx: *const Ctx,
    zero_copy: bool,
    /// The run the producers send now: 0 the warm-up, then 1 to `runs`; `runs + 1` ends them.
    run: std.atomic.Value(u32) = .init(std.math.maxInt(u32)),
    /// The producers stop sending this run.
    stop: std.atomic.Value(bool) = .init(false),
    /// Producers done with the run.
    producers_done: std.atomic.Value(u32) = .init(0),
    /// Messages per producer per run, if counted (`messages`); else 0.
    per_producer: u64,
    sent: [max_members]Count = @splat(.{}),
    taken: [max_members]Count = @splat(.{}),
    /// A member's failure: the run stops, and the case fails.
    failed: std.atomic.Value(bool) = .init(false),

    fn total(counts: []const Count) u64 {
        var sum: u64 = 0;
        for (counts) |*count| sum += count.value.load(.acquire);
        return sum;
    }
};

/// The owner: claims the queue, starts the members and waits until they are joined, times the runs, and ends the queue
/// (its close), after which the consumers drain and leave.
pub fn run(ctx: *const Ctx, scenario: *const Scenario, results: *Results) !void {
    std.debug.assert(scenario.producers <= max_members and scenario.consumers <= max_members);
    const io = ctx.io;
    const queue = try fipc.queueCreate(ctx.name, ctx.ring, scenario.producers, scenario.consumers);
    var listening = true;
    defer if (listening) fipc.closeQueue(queue);

    var shared: Shared = .{
        .ctx = ctx,
        .zero_copy = scenario.zero_copy,
        .per_producer = if (ctx.messages > 0) std.math.divCeil(u64, ctx.messages, scenario.producers) catch unreachable else 0,
    };
    var threads: [2 * max_members]std.Thread = undefined;
    var started: usize = 0;
    defer for (threads[0..started]) |thread| thread.join();
    defer if (listening) {
        shared.failed.store(true, .release);
        shared.run.store(ctx.runs + 1, .release);
        fipc.closeQueue(queue);
        listening = false;
    };
    for (0..scenario.consumers) |k| {
        threads[started] = try std.Thread.spawn(.{}, consumer, .{ &shared, k });
        started += 1;
    }
    for (0..scenario.producers) |k| {
        threads[started] = try std.Thread.spawn(.{}, producer, .{ &shared, k });
        started += 1;
    }
    try fipc.waitForMembers(queue, scenario.producers + scenario.consumers, join_timeout_ms);

    const producers = shared.sent[0..scenario.producers];
    const consumers = shared.taken[0..scenario.consumers];
    var r: u32 = if (ctx.warmup_ns > 0) 0 else 1;
    while (r <= ctx.runs) : (r += 1) {
        const budget_ns = if (r == 0) ctx.warmup_ns else ctx.duration_ns;
        const sent_before = Shared.total(producers);
        shared.stop.store(false, .release);
        shared.producers_done.store(0, .release);
        const start = timing.now(io);
        shared.run.store(r, .release);
        if (shared.per_producer == 0 or r == 0) {
            timing.spinUntil(io, start + budget_ns);
            shared.stop.store(true, .release);
        }
        while (shared.producers_done.load(.acquire) < scenario.producers) {
            if (shared.failed.load(.acquire)) return error.MemberFailed;
            std.Thread.yield() catch {};
        }
        const sent = Shared.total(producers);
        while (Shared.total(consumers) < sent) {
            if (shared.failed.load(.acquire)) return error.MemberFailed;
            std.Thread.yield() catch {};
        }
        const elapsed = timing.now(io) - start;
        if (Shared.total(consumers) != sent) return error.UnexpectedMessage;
        if (r == 0) continue;
        const rate = @as(f64, @floatFromInt(sent - sent_before)) / timing.seconds(elapsed);
        try results.record(ctx.gpa, &.{ rate, rate / @as(f64, @floatFromInt(scenario.producers)) });
    }
    shared.run.store(ctx.runs + 1, .release);
    fipc.closeQueue(queue); // the end: each consumer drains, then leaves
    listening = false;
}

fn producer(shared: *Shared, k: usize) void {
    sendRuns(shared, k) catch |err| {
        std.log.err("producer {d}: {s}", .{ k, @errorName(err) });
        shared.failed.store(true, .release);
    };
}

fn sendRuns(shared: *Shared, k: usize) !void {
    const ctx = shared.ctx;
    const conn = try fipc.queueJoin(ctx.name, fipc.producer, join_timeout_ms);
    defer fipc.close(conn);
    const payload = try throughput.payloadFor(ctx);
    defer ctx.gpa.free(payload);
    var sent: u64 = 0;
    var done: u32 = std.math.maxInt(u32);
    while (true) {
        const r = shared.run.load(.acquire);
        if (r == done or r == std.math.maxInt(u32)) {
            std.Thread.yield() catch {};
            continue;
        }
        if (r > ctx.runs) return;
        const count = if (r > 0) shared.per_producer else 0;
        var n: u64 = 0;
        while (if (count > 0) n < count else (n % 64 != 0 or !shared.stop.load(.acquire))) : (n += 1) {
            if (shared.zero_copy) {
                const room = try fipc.sendAcquire(conn, payload.len, fipc.forever);
                @memcpy(room, payload);
                try fipc.sendCommit(conn, payload.len);
            } else try fipc.send(conn, payload, fipc.forever);
        }
        sent += n;
        shared.sent[k].value.store(sent, .release);
        done = r;
        _ = shared.producers_done.fetchAdd(1, .acq_rel);
    }
}

fn consumer(shared: *Shared, k: usize) void {
    takeAll(shared, k) catch |err| {
        std.log.err("consumer {d}: {s}", .{ k, @errorName(err) });
        shared.failed.store(true, .release);
    };
}

fn takeAll(shared: *Shared, k: usize) !void {
    const ctx = shared.ctx;
    const conn = try fipc.queueJoin(ctx.name, fipc.consumer, join_timeout_ms);
    defer fipc.close(conn);
    const buffer = try throughput.hotBuffer(ctx, ctx.size);
    defer ctx.gpa.free(buffer);
    var taken: u64 = 0;
    while (true) {
        if (shared.zero_copy) {
            const bytes = fipc.recvAcquire(conn, fipc.forever) catch |err| switch (err) {
                error.Disconnected => return,
                else => return err,
            };
            defer fipc.recvRelease(conn);
            if (bytes.len != buffer.len) return error.UnexpectedMessage;
            if (ctx.touch) @memcpy(buffer, bytes);
        } else {
            const bytes = fipc.recv(conn, buffer, fipc.forever) catch |err| switch (err) {
                error.Disconnected => return,
                else => return err,
            };
            if (bytes.len != buffer.len) return error.UnexpectedMessage;
        }
        taken += 1;
        shared.taken[k].value.store(taken, .release);
    }
}
