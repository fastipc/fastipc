//! The scenarios, their cases (message size, ring capacity) and what a case measures.
//!
//! One-way throughput over the copying and the zero-copy calls and over RPC, at the sizes of the performance
//! gates (16 B to a message of three rings); ping-pong round trips with a spinning and with a sleeping receiver; the
//! wake-up of an idle receiver, and the same messages to a receiver that polls; and connection setup. `gate` marks
//! the cases of the performance gates (`fipc_bench gates`, docs/perf/baseline.md).

const std = @import("std");
const Io = std.Io;

const KiB = 1024;
const MiB = 1024 * KiB;

pub const Kind = enum {
    /// One-way throughput: send / recv, or the zero-copy calls.
    stream,
    /// One-way RPC throughput: submit / recv.
    rpc,
    /// Round trips of plain messages.
    ping_pong,
    /// One message per millisecond to a receiver that sleeps in between, or polls.
    wakeup,
    /// listen + accept / connect + close cycles.
    setup,
};

pub const Case = struct {
    size: usize,
    ring: u32,
    /// One of the performance gates' cases.
    gate: bool = false,
};

pub const Scenario = struct {
    name: []const u8,
    what: []const u8,
    kind: Kind,
    /// Stream: the zero-copy calls (send_acquire / send_commit, recv_acquire / recv_release) in place of the copying
    /// ones.
    zero_copy: bool = false,
    /// Ping-pong: the peer thinks before it replies, long enough for this side to fall asleep.
    thinks: bool = false,
    /// Wake-up: the receiver polls, calling `recv` with timeout 0 in a loop, in place of waiting in it.
    polls: bool = false,
    cases: []const Case,
};

const ring = 512 * KiB;

/// The one-way throughput cases: the gates' sizes, from 16 B to a message of three rings, and two others.
const throughput_cases = [_]Case{
    .{ .size = 16, .ring = ring, .gate = true },
    .{ .size = 64, .ring = ring, .gate = true },
    .{ .size = 256, .ring = ring },
    .{ .size = 1 * KiB, .ring = ring, .gate = true },
    .{ .size = 64 * KiB, .ring = ring, .gate = true },
    .{ .size = 512 * KiB, .ring = 2 * MiB },
    .{ .size = 3 * ring, .ring = ring, .gate = true },
};

/// 32-byte messages, as a game platform's small RPC calls.
const small_message_case = [_]Case{.{ .size = 32, .ring = ring, .gate = true }};
/// The same, outside the performance gates.
const small_message_info = [_]Case{.{ .size = 32, .ring = ring }};
/// A typical RPC ring (1 MiB): each connection maps and clears both rings.
const setup_case = [_]Case{.{ .size = 0, .ring = 1 * MiB, .gate = true }};

pub const all = [_]Scenario{
    .{ .name = "fastipc", .what = "copy: send / recv", .kind = .stream, .cases = &throughput_cases },
    .{ .name = "fastipc-zerocopy", .what = "zero-copy: send_acquire / recv_acquire", .kind = .stream, .zero_copy = true, .cases = &throughput_cases },
    .{ .name = "rpc", .what = "RPC: submit / recv", .kind = .rpc, .cases = &throughput_cases },
    .{ .name = "latency-spin", .what = "round trip, the peer replies at once", .kind = .ping_pong, .cases = &small_message_case },
    .{ .name = "latency-sleep", .what = "round trip, the peer thinks first (this side sleeps)", .kind = .ping_pong, .thinks = true, .cases = &small_message_case },
    .{ .name = "wakeup", .what = "one message per ms to a sleeping receiver", .kind = .wakeup, .cases = &small_message_case },
    .{ .name = "wakeup-poll", .what = "one message per ms to a receiver polling with timeout 0", .kind = .wakeup, .polls = true, .cases = &small_message_info },
    .{ .name = "setup", .what = "listen + accept / connect + close cycles", .kind = .setup, .cases = &setup_case },
};

pub fn find(name: []const u8) ?*const Scenario {
    for (&all) |*scenario| {
        if (std.mem.eql(u8, scenario.name, name)) return scenario;
    }
    return null;
}

/// The alignment of the hot path's buffers (payloads, sinks, work queues): the start of a page. A copy's
/// speed depends on where its buffers sit in their pages (4K aliasing, cache-line splits: about 10% at
/// 1 KiB on WSL), and malloc places a buffer after everything allocated before it, the command line
/// included. The library's own buffers can't be aligned from here, so an A/B also gives both sides
/// command lines of the same length.
pub const hot_align: std.mem.Alignment = .fromByteUnits(std.heap.page_size_min);
pub const HotBuffer = []align(hot_align.toByteUnits()) u8;

/// What a case runs with, the same in both processes.
pub const Ctx = struct {
    io: Io,
    gpa: std.mem.Allocator,
    /// The name the server listens on.
    name: [:0]const u8,
    size: usize,
    ring: u32,
    runs: u32,
    duration_ns: u64,
    warmup_ns: u64,
    /// Messages per run; 0: as many as fill `duration_ns` at the warm-up's rate.
    messages: u64,
    /// Zero-copy receivers copy each message out, as a consumer would.
    touch: bool,
    /// Ping-pong: the peer's think time before it replies.
    think_ns: u64,
};

/// A metric of a case's runs.
pub const Metric = struct {
    name: []const u8,
    unit: []const u8,
    higher_is_better: bool,
};

pub const throughput_metrics = [_]Metric{
    .{ .name = "msgs_per_s", .unit = "msg/s", .higher_is_better = true },
    .{ .name = "mib_per_s", .unit = "MiB/s", .higher_is_better = true },
};
pub const latency_metrics = [_]Metric{
    .{ .name = "p50_ns", .unit = "ns", .higher_is_better = false },
    .{ .name = "p99_ns", .unit = "ns", .higher_is_better = false },
    .{ .name = "p999_ns", .unit = "ns", .higher_is_better = false },
};
pub const setup_metrics = [_]Metric{
    .{ .name = "cycles_per_s", .unit = "cycle/s", .higher_is_better = true },
    .{ .name = "cycle_p50_ns", .unit = "ns", .higher_is_better = false },
    .{ .name = "cycle_p99_ns", .unit = "ns", .higher_is_better = false },
};

pub fn metricsOf(kind: Kind) []const Metric {
    return switch (kind) {
        .stream, .rpc => &throughput_metrics,
        .ping_pong, .wakeup => &latency_metrics,
        .setup => &setup_metrics,
    };
}

pub const max_metrics = 3;

/// A case's measured runs: one value per metric per run.
pub const Results = struct {
    metrics: []const Metric,
    values: [max_metrics]std.ArrayList(f64) = @splat(.empty),
    /// Latency samples per run, or cycles per run (for the report).
    samples: u64 = 0,

    pub fn init(kind: Kind) Results {
        return .{ .metrics = metricsOf(kind) };
    }

    pub fn deinit(results: *Results, gpa: std.mem.Allocator) void {
        for (&results.values) |*values| values.deinit(gpa);
    }

    /// Records one run: a value per metric, in `metrics` order.
    pub fn record(results: *Results, gpa: std.mem.Allocator, run: []const f64) !void {
        std.debug.assert(run.len == results.metrics.len);
        for (run, results.values[0..run.len]) |value, *values| try values.append(gpa, value);
    }

    /// A throughput run: `messages` of `size` bytes in `ns` nanoseconds.
    pub fn recordThroughput(results: *Results, gpa: std.mem.Allocator, messages: u64, size: usize, ns: u64) !void {
        const s = @as(f64, @floatFromInt(ns)) / std.time.ns_per_s;
        const msgs_per_s = @as(f64, @floatFromInt(messages)) / s;
        try results.record(gpa, &.{ msgs_per_s, msgs_per_s * @as(f64, @floatFromInt(size)) / MiB });
    }

    /// A latency run: `samples` sorted, in ticks of `ns_per_tick` nanoseconds.
    pub fn recordLatency(results: *Results, gpa: std.mem.Allocator, sorted: []const u64, ns_per_tick: f64) !void {
        const timing = @import("timing.zig");
        var run: [3]f64 = undefined;
        for (&run, [_]f64{ 0.5, 0.99, 0.999 }) |*value, q| {
            value.* = @as(f64, @floatFromInt(timing.quantile(sorted, q))) * ns_per_tick;
        }
        try results.record(gpa, &run);
        results.samples = sorted.len;
    }
};
