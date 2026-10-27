//! The scenarios, their cases (message size, ring capacity) and what a case measures.
//!
//! One-way throughput over the copying and the zero-copy calls, at the sizes of the performance gates (16 B to a
//! message of three rings); ping-pong round trips with a spinning and with a sleeping receiver; the wake-up of an idle
//! receiver, and the same messages to a receiver that polls; connection setup; a broadcast's publisher with lossy or
//! gated subscriber processes; a work queue's producer and consumer threads; and a service's client and server
//! threads, the clients with one request in flight each, or many. `gate` marks the cases of the performance gates
//! (`fipc_bench gates`, docs/perf/baseline.md).

const std = @import("std");
const Io = std.Io;

const KiB = 1024;
const MiB = 1024 * KiB;

pub const Kind = enum {
    /// One-way throughput: send / recv, or the zero-copy calls.
    stream,
    /// Round trips on a stream.
    ping_pong,
    /// One message per millisecond to a receiver that sleeps in between, or polls.
    wakeup,
    /// listen + accept / connect + close cycles.
    setup,
    /// A broadcast: this process's publisher, `subscribers` peer processes.
    broadcast,
    /// Round trips over two broadcasts, one each way, each with one lossy subscriber.
    broadcast_ping_pong,
    /// A work queue: `producers` producer and `consumers` consumer threads, each a joined member.
    queue,
    /// A service: `clients` client and `servers` server threads, each a joined member, each client with up to
    /// `in_flight` requests awaiting their replies.
    service,
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
    /// Stream, queue, service: the zero-copy calls (send_acquire / send_commit, recv_acquire / recv_release) in place of the
    /// copying ones.
    zero_copy: bool = false,
    /// Ping-pong: the peer thinks before it replies, long enough for this side to fall asleep.
    thinks: bool = false,
    /// Wake-up: the receiver polls, calling `recv` with timeout 0 in a loop, in place of waiting in it.
    polls: bool = false,
    /// Broadcast: the subscriber processes, and whether they are gated (receiving in place) rather than lossy.
    subscribers: u32 = 0,
    gated: bool = false,
    /// Queue: the producer and consumer threads.
    producers: u32 = 0,
    consumers: u32 = 0,
    /// Service: the client and server threads, and the requests each client keeps awaiting their replies.
    clients: u32 = 0,
    servers: u32 = 0,
    in_flight: u32 = 1,
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

/// 32-byte messages, as a game platform's small requests.
const small_message_case = [_]Case{.{ .size = 32, .ring = ring, .gate = true }};
/// The same, outside the performance gates.
const small_message_info = [_]Case{.{ .size = 32, .ring = ring }};
/// A typical ring (1 MiB): each connection maps and clears both rings.
const setup_case = [_]Case{.{ .size = 0, .ring = 1 * MiB, .gate = true }};
/// A broadcast's small messages, in a broadcast of 512 KiB (8192 cells; a piece is up to 128 KiB).
const broadcast_cases = [_]Case{
    .{ .size = 16, .ring = ring },
    .{ .size = 64, .ring = ring },
    .{ .size = 1 * KiB, .ring = ring },
};

/// A work queue's small messages, in a ring of 512 KiB per producer (8192 cells; a message is up to 128 KiB).
const queue_cases = broadcast_cases;
/// A service's small requests and replies of the same size (its max_message), in a ring of 512 KiB per client.
const service_cases = broadcast_cases;

pub const all = [_]Scenario{
    .{ .name = "fastipc", .what = "copy: send / recv", .kind = .stream, .cases = &throughput_cases },
    .{ .name = "fastipc-zerocopy", .what = "zero-copy: send_acquire / recv_acquire", .kind = .stream, .zero_copy = true, .cases = &throughput_cases },
    .{ .name = "latency-spin", .what = "round trip, the peer replies at once", .kind = .ping_pong, .cases = &small_message_case },
    .{ .name = "latency-sleep", .what = "round trip, the peer thinks first (this side sleeps)", .kind = .ping_pong, .thinks = true, .cases = &small_message_case },
    .{ .name = "wakeup", .what = "one message per ms to a sleeping receiver", .kind = .wakeup, .cases = &small_message_case },
    .{ .name = "wakeup-poll", .what = "one message per ms to a receiver polling with timeout 0", .kind = .wakeup, .polls = true, .cases = &small_message_info },
    .{ .name = "setup", .what = "listen + accept / connect + close cycles", .kind = .setup, .cases = &setup_case },
    .{ .name = "broadcast-1", .what = "broadcast to 1 lossy subscriber process: send / recv", .kind = .broadcast, .subscribers = 1, .cases = &broadcast_cases },
    .{ .name = "broadcast-4", .what = "broadcast to 4 lossy subscriber processes: send / recv", .kind = .broadcast, .subscribers = 4, .cases = &broadcast_cases },
    .{ .name = "broadcast-gated-1", .what = "broadcast to 1 gated subscriber process: send / recv_acquire", .kind = .broadcast, .subscribers = 1, .gated = true, .cases = &broadcast_cases },
    .{ .name = "broadcast-gated-4", .what = "broadcast to 4 gated subscriber processes: send / recv_acquire", .kind = .broadcast, .subscribers = 4, .gated = true, .cases = &broadcast_cases },
    .{ .name = "broadcast-latency", .what = "round trip over two broadcasts, one each way (lossy), the peer replies at once", .kind = .broadcast_ping_pong, .cases = &small_message_info },
    .{ .name = "queue-1x1", .what = "work queue, 1 producer and 1 consumer thread: send / recv", .kind = .queue, .producers = 1, .consumers = 1, .cases = &queue_cases },
    .{ .name = "queue-4x4", .what = "work queue, 4 producer and 4 consumer threads: send / recv", .kind = .queue, .producers = 4, .consumers = 4, .cases = &queue_cases },
    .{ .name = "queue-1x8", .what = "work queue, 1 producer and 8 consumer threads: send / recv", .kind = .queue, .producers = 1, .consumers = 8, .cases = &queue_cases },
    .{ .name = "queue-8x8", .what = "work queue, 8 producer and 8 consumer threads: send / recv", .kind = .queue, .producers = 8, .consumers = 8, .cases = &queue_cases },
    .{ .name = "queue-1x1-zerocopy", .what = "work queue, 1 producer and 1 consumer thread: send_acquire / recv_acquire", .kind = .queue, .zero_copy = true, .producers = 1, .consumers = 1, .cases = &queue_cases },
    .{ .name = "service-1x1", .what = "service, 1 client and 1 server thread, 1 request in flight (calls): send / recv", .kind = .service, .clients = 1, .servers = 1, .cases = &service_cases },
    .{ .name = "service-1x1-pipelined", .what = "service, 1 client and 1 server thread, 64 requests in flight: send / recv", .kind = .service, .clients = 1, .servers = 1, .in_flight = 64, .cases = &service_cases },
    .{ .name = "service-4x4", .what = "service, 4 client and 4 server threads, 1 request in flight per client (calls): send / recv", .kind = .service, .clients = 4, .servers = 4, .cases = &service_cases },
    .{ .name = "service-1x4", .what = "service, 1 client and 4 server threads, 1 request in flight (calls): send / recv", .kind = .service, .clients = 1, .servers = 4, .cases = &service_cases },
    .{ .name = "service-2x4", .what = "service, 2 client and 4 server threads, 1 request in flight per client (calls): send / recv", .kind = .service, .clients = 2, .servers = 4, .cases = &service_cases },
    .{ .name = "service-8x1-pipelined", .what = "service, 8 client threads and 1 server thread, 64 requests in flight per client: send / recv", .kind = .service, .clients = 8, .servers = 1, .in_flight = 64, .cases = &service_cases },
    .{ .name = "service-1x1-pipelined-zerocopy", .what = "service, 1 client and 1 server thread, 64 requests in flight: send_acquire / recv_acquire", .kind = .service, .zero_copy = true, .clients = 1, .servers = 1, .in_flight = 64, .cases = &service_cases },
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
/// The writer's rate, the slowest subscriber's (what it received, over its own time from the run's first message to
/// its last), and the most any subscriber missed (a lossy one's lapped messages; a gated one misses none).
pub const broadcast_metrics = [_]Metric{
    .{ .name = "msgs_per_s", .unit = "msg/s", .higher_is_better = true },
    .{ .name = "slowest_msgs_per_s", .unit = "msg/s received (slowest)", .higher_is_better = true },
    .{ .name = "lost_pct", .unit = "% lost (most)", .higher_is_better = false },
};
/// The messages every consumer took, per second of the run (to the moment the last was taken), and that rate per
/// producer.
pub const queue_metrics = [_]Metric{
    .{ .name = "msgs_per_s", .unit = "msg/s", .higher_is_better = true },
    .{ .name = "msgs_per_s_per_producer", .unit = "msg/s per producer", .higher_is_better = true },
};
/// The replies every client received, per second of the run (to the moment the last was received), and that rate per
/// client: with one request in flight, calls per second.
pub const service_metrics = [_]Metric{
    .{ .name = "msgs_per_s", .unit = "reply/s", .higher_is_better = true },
    .{ .name = "msgs_per_s_per_client", .unit = "reply/s per client", .higher_is_better = true },
};
pub const setup_metrics = [_]Metric{
    .{ .name = "cycles_per_s", .unit = "cycle/s", .higher_is_better = true },
    .{ .name = "cycle_p50_ns", .unit = "ns", .higher_is_better = false },
    .{ .name = "cycle_p99_ns", .unit = "ns", .higher_is_better = false },
};

pub fn metricsOf(kind: Kind) []const Metric {
    return switch (kind) {
        .stream => &throughput_metrics,
        .ping_pong, .wakeup, .broadcast_ping_pong => &latency_metrics,
        .setup => &setup_metrics,
        .broadcast => &broadcast_metrics,
        .queue => &queue_metrics,
        .service => &service_metrics,
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

    /// A latency run: the `sorted` samples, in ticks of `ns_per_tick` nanoseconds.
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
