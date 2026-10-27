//! A broadcast: this process, its publisher, publishes messages of one size as fast as it can to `subscribers`
//! subscriber processes (this executable again), lossy (`fipc_recv`: the publisher never waits for them) or gated
//! (`fipc_recv_acquire`, each message copied out unless --no-touch: the publisher waits for the slowest).
//!
//! Every message starts with its run and its number in the run (`Stamp`, 16 bytes: the cases' smallest size). The
//! publisher warms up (run 0, `warmup_ns`), then sends `runs` runs of about `duration_ns` each (with `messages` set,
//! that many), timed by its own clock, and pauses `pause_ms` after each, so that a lossy subscriber that fell behind
//! catches up with the run's last message before the next run laps it. A subscriber counts per run the messages it
//! received, those it missed (the gaps in the numbers: a lossy subscriber lapped by the publisher), and its time from
//! the run's first message to the moment it caught up with the last (a receive that finds nothing). At the publisher's
//! close (DISCONNECTED) it prints one line per run, which this process reads from its stdout. A run's metrics: the
//! publisher's rate, the slowest subscriber's, and the largest share any subscriber missed.
//!
//! The round trip (broadcast-latency) is latency-spin's over two broadcasts: this process publishes the pings on the
//! case's name, the peer publishes the replies on `<name>_back`, and each is the other's one lossy subscriber.

const std = @import("std");
const fipc = @import("fipc.zig");
const latency = @import("latency.zig");
const peer = @import("peer.zig");
const scenarios = @import("scenarios.zig");
const throughput = @import("throughput.zig");
const timing = @import("timing.zig");

const Ctx = scenarios.Ctx;
const Results = scenarios.Results;
const Scenario = scenarios.Scenario;

const join_timeout_ms = throughput.connect_timeout_ms;
/// The writer's pause after each run.
const pause_ms = 20;
/// The most subscribers a scenario starts.
const max_subscribers = 4;
/// The most runs a subscriber keeps count of: the warm-up and up to 63 measured.
const max_runs = 64;

/// What starts every message: its run and its number in the run.
const Stamp = struct {
    run: u32,
    seq: u64,

    const len = 16;

    fn write(bytes: []u8, run: u32, seq: u64) void {
        std.mem.writeInt(u32, bytes[0..4], run, .little);
        std.mem.writeInt(u64, bytes[8..16], seq, .little);
    }

    fn read(bytes: []const u8) Stamp {
        return .{ .run = std.mem.readInt(u32, bytes[0..4], .little), .seq = std.mem.readInt(u64, bytes[8..16], .little) };
    }
};

/// The publisher: starts the subscriber processes (`argv`, the peer's command line), waits until they are joined, sends
/// the runs, closes the broadcast and records each measured run with what the subscribers report.
pub fn publisher(ctx: *const Ctx, scenario: *const Scenario, argv: []const []const u8, results: *Results) !void {
    if (ctx.size < Stamp.len) return error.MessageTooSmall;
    if (ctx.runs >= max_runs) return error.TooManyRuns;
    std.debug.assert(scenario.subscribers <= max_subscribers);
    const io = ctx.io;
    const writer = try fipc.broadcastCreate(ctx.name, ctx.ring, scenario.subscribers, fipc.forever);
    var writer_open = true;
    defer if (writer_open) fipc.close(writer);

    var peers: [max_subscribers]peer.Peer = undefined;
    var started: usize = 0;
    defer for (peers[0..started]) |*p| p.kill(io);
    while (started < scenario.subscribers) : (started += 1) peers[started] = try peer.Peer.spawnReporting(io, argv);
    try fipc.waitForSubscribers(writer, scenario.subscribers, join_timeout_ms);

    const payload = try throughput.payloadFor(ctx);
    defer ctx.gpa.free(payload);
    var rates: [max_runs]f64 = @splat(0);
    var run: u32 = if (ctx.warmup_ns > 0) 0 else 1;
    while (run <= ctx.runs) : (run += 1) {
        const by_count = ctx.messages > 0 and run > 0;
        const budget_ns = if (run == 0) ctx.warmup_ns else ctx.duration_ns;
        const start = timing.now(io);
        var seq: u64 = 0;
        while (true) {
            Stamp.write(payload, run, seq);
            try fipc.send(writer, payload, fipc.forever);
            seq += 1;
            if (by_count) {
                if (seq == ctx.messages) break;
            } else if (seq % 64 == 0 and timing.now(io) - start >= budget_ns) break;
        }
        rates[run] = @as(f64, @floatFromInt(seq)) / timing.seconds(timing.now(io) - start);
        try io.sleep(.fromMilliseconds(pause_ms), .awake);
    }
    fipc.close(writer); // the end: each subscriber reports and exits
    writer_open = false;

    var slowest: [max_runs]f64 = @splat(std.math.inf(f64));
    var lost: [max_runs]f64 = @splat(0);
    var reports: [max_runs]u32 = @splat(0);
    for (peers[0..started]) |*p| {
        var buffer: [4096]u8 = undefined;
        const text = try p.readAll(io, &buffer);
        try p.finish(io);
        var lines = std.mem.tokenizeAny(u8, text, "\r\n");
        while (lines.next()) |line| {
            const seen = Seen.parse(line) orelse return error.UnexpectedReport;
            if (seen.run >= max_runs) return error.UnexpectedReport;
            reports[seen.run] += 1;
            slowest[seen.run] = @min(slowest[seen.run], seen.rate());
            lost[seen.run] = @max(lost[seen.run], seen.lostPercent());
        }
    }
    started = 0;
    for (1..ctx.runs + 1) |r| {
        if (reports[r] != scenario.subscribers) {
            std.log.err("run {d}: {d} of {d} subscribers received anything", .{ r, reports[r], scenario.subscribers });
            return error.MissingReport;
        }
        try results.record(ctx.gpa, &.{ rates[r], slowest[r], lost[r] });
    }
}

/// One run as a subscriber saw it; a report line is "R <run> <received> <missed> <ns>".
const Seen = struct {
    run: u32 = 0,
    received: u64 = 0,
    missed: u64 = 0,
    first_ns: u64 = 0,
    last_ns: u64 = 0,
    caught_up: bool = false,

    fn rate(seen: Seen) f64 {
        return @as(f64, @floatFromInt(seen.received)) / timing.seconds(@max(seen.last_ns - seen.first_ns, 1));
    }

    fn lostPercent(seen: Seen) f64 {
        return 100 * @as(f64, @floatFromInt(seen.missed)) / @as(f64, @floatFromInt(seen.received + seen.missed));
    }

    fn print(seen: Seen, w: *std.Io.Writer) !void {
        try w.print("R {d} {d} {d} {d}\n", .{ seen.run, seen.received, seen.missed, seen.last_ns - seen.first_ns });
    }

    fn parse(line: []const u8) ?Seen {
        var fields = std.mem.tokenizeScalar(u8, line, ' ');
        if (!std.mem.eql(u8, fields.next() orelse return null, "R")) return null;
        var values: [4]u64 = undefined;
        for (&values) |*value| value.* = std.fmt.parseInt(u64, fields.next() orelse return null, 10) catch return null;
        if (fields.next() != null or values[0] >= max_runs) return null;
        return .{ .run = @intCast(values[0]), .received = values[1], .missed = values[2], .first_ns = 0, .last_ns = values[3] };
    }
};

/// A subscriber process: joins, counts every run's messages until the writer's close, then reports them on stdout.
pub fn subscriber(ctx: *const Ctx, gated: bool) !void {
    const io = ctx.io;
    const conn = try fipc.broadcastJoin(ctx.name, if (gated) fipc.subscriber_gated else fipc.subscriber_lossy, join_timeout_ms);
    defer fipc.close(conn);
    const buffer = try throughput.hotBuffer(ctx, ctx.size);
    defer ctx.gpa.free(buffer);
    const receiver: Receiver = .{ .conn = conn, .gated = gated, .buffer = buffer, .touch = ctx.touch };

    var seen: [max_runs]Seen = @splat(.{});
    var current: ?u32 = null;
    var expected: u64 = 0;
    while (true) {
        const stamp = (receiver.next(fipc.no_wait) catch |err| switch (err) {
            error.Timeout => wait: {
                // Caught up: the message received last is the newest
                if (current) |run| {
                    seen[run].last_ns = timing.now(io);
                    seen[run].caught_up = true;
                }
                break :wait receiver.next(fipc.forever) catch |late| switch (late) {
                    error.Disconnected => break,
                    else => return late,
                };
            },
            error.Disconnected => break,
            else => return err,
        }) orelse continue; // lapped (OVERRUN): the gap shows in the next message's number
        if (stamp.run >= max_runs) return error.UnexpectedMessage;
        if (current != stamp.run) {
            if (current) |run| {
                if (!seen[run].caught_up) seen[run].last_ns = timing.now(io);
            }
            current = stamp.run;
            const now = timing.now(io);
            seen[stamp.run] = .{ .run = stamp.run, .first_ns = now, .last_ns = now };
            expected = stamp.seq;
        }
        const s = &seen[stamp.run];
        if (stamp.seq > expected) s.missed += stamp.seq - expected;
        s.received += 1;
        expected = stamp.seq + 1;
    }
    if (current) |run| {
        if (!seen[run].caught_up) seen[run].last_ns = timing.now(io);
    }

    var out_buffer: [4096]u8 = undefined;
    var stdout = std.Io.File.stdout().writer(io, &out_buffer);
    for (seen) |s| {
        if (s.received > 0) try s.print(&stdout.interface);
    }
    try stdout.interface.flush();
}

const Receiver = struct {
    conn: *fipc.Conn,
    gated: bool,
    buffer: []u8,
    touch: bool,

    /// The next message's stamp; null after OVERRUN (a lossy subscriber was lapped).
    inline fn next(receiver: Receiver, timeout_ms: c_int) !?Stamp {
        if (receiver.gated) {
            const bytes = try fipc.recvAcquire(receiver.conn, timeout_ms);
            defer fipc.recvRelease(receiver.conn);
            if (bytes.len != receiver.buffer.len) return error.UnexpectedMessage;
            if (receiver.touch) @memcpy(receiver.buffer, bytes);
            return Stamp.read(bytes);
        }
        const bytes = fipc.recv(receiver.conn, receiver.buffer, timeout_ms) catch |err| switch (err) {
            error.Overrun => return null,
            else => return err,
        };
        if (bytes.len != receiver.buffer.len) return error.UnexpectedMessage;
        return Stamp.read(bytes);
    }
};

// === Round trips ===

/// The peer's broadcast: the case's name with "_back".
fn backName(buffer: []u8, name: [:0]const u8) ![:0]const u8 {
    return std.mem.printSentinel(buffer, "{s}_back", .{name}, 0);
}

/// This process: publishes the pings, subscribes to the replies, and records each round trip minus the peer's hold
/// time (as latency.pinger).
pub fn pinger(ctx: *const Ctx, results: *Results) !void {
    const pings = try fipc.broadcastCreate(ctx.name, ctx.ring, 1, fipc.forever);
    defer fipc.close(pings);
    try fipc.waitForSubscribers(pings, 1, join_timeout_ms);
    var name_buffer: [128]u8 = undefined;
    const replies = try fipc.broadcastJoin(try backName(&name_buffer, ctx.name), fipc.subscriber_lossy, join_timeout_ms);
    defer fipc.close(replies);
    const ping = try latency.messageBuffer(ctx);
    defer ctx.gpa.free(ping);
    const reply = try latency.messageBuffer(ctx);
    defer ctx.gpa.free(reply);

    var seq: u64 = 0;
    const warmup_end = timing.now(ctx.io) + ctx.warmup_ns;
    while (timing.now(ctx.io) < warmup_end) {
        seq += 1;
        _ = try roundTrip(pings, replies, ping, reply, seq);
    }
    var samples: std.ArrayList(u64) = .empty;
    defer samples.deinit(ctx.gpa);
    for (0..ctx.runs) |_| {
        samples.clearRetainingCapacity();
        const start = timing.Stamp.take(ctx.io);
        const end_ns = start.ns + ctx.duration_ns;
        while (timing.now(ctx.io) < end_ns) {
            try samples.ensureUnusedCapacity(ctx.gpa, 16);
            for (0..16) |_| {
                seq += 1;
                samples.appendAssumeCapacity(try roundTrip(pings, replies, ping, reply, seq));
            }
        }
        const end = timing.Stamp.take(ctx.io);
        std.mem.sort(u64, samples.items, {}, std.sort.asc(u64));
        try results.recordLatency(ctx.gpa, samples.items, start.nsPerTick(end));
    }
    (latency.Message{ .stop = true, .seq = @truncate(seq + 1), .stamp = 0 }).write(ping);
    try fipc.send(pings, ping, fipc.forever);
    _ = try fipc.recv(replies, reply, join_timeout_ms);
}

/// One ping and its reply: the round trip in ticks, minus the peer's hold time.
inline fn roundTrip(writer: *fipc.Conn, replies: *fipc.Conn, ping: []u8, reply: []u8, seq: u64) !u64 {
    (latency.Message{ .seq = @truncate(seq), .stamp = 0 }).write(ping);
    const sent = timing.ticks();
    try fipc.send(writer, ping, fipc.forever);
    const reply_bytes = try fipc.recv(replies, reply, fipc.forever);
    const received = timing.ticks();
    const answer = try latency.Message.read(reply_bytes);
    if (!answer.is(seq) or reply_bytes.len != ping.len) return error.UnexpectedMessage;
    return (received - sent) -| answer.stamp;
}

/// The peer: publishes the replies, subscribes to the pings, and answers each at once with its hold time.
pub fn ponger(ctx: *const Ctx) !void {
    var name_buffer: [128]u8 = undefined;
    const replies = try fipc.broadcastCreate(try backName(&name_buffer, ctx.name), ctx.ring, 1, fipc.forever);
    defer fipc.close(replies);
    const pings = try fipc.broadcastJoin(ctx.name, fipc.subscriber_lossy, join_timeout_ms);
    defer fipc.close(pings);
    try fipc.waitForSubscribers(replies, 1, join_timeout_ms);
    const ping_buffer = try latency.messageBuffer(ctx);
    defer ctx.gpa.free(ping_buffer);
    const reply = try latency.messageBuffer(ctx);
    defer ctx.gpa.free(reply);
    while (true) {
        const ping_bytes = try fipc.recv(pings, ping_buffer, fipc.forever);
        const received = timing.ticks();
        const ping = try latency.Message.read(ping_bytes);
        (latency.Message{ .stop = ping.stop, .seq = ping.seq, .stamp = @truncate(timing.ticks() - received) }).write(reply);
        try fipc.send(replies, reply, fipc.forever);
        if (ping.stop) return;
    }
}

test "a report line survives its round trip, and other lines aren't one" {
    var buffer: [64]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buffer);
    const seen: Seen = .{ .run = 3, .received = 1000, .missed = 25, .first_ns = 5, .last_ns = 2_000_005 };
    try seen.print(&w);
    const parsed = Seen.parse(std.mem.trimEnd(u8, w.buffered(), "\n")).?;
    try std.testing.expectEqual(@as(u32, 3), parsed.run);
    try std.testing.expectEqual(seen.rate(), parsed.rate());
    try std.testing.expectEqual(seen.lostPercent(), parsed.lostPercent());
    try std.testing.expectEqual(@as(?Seen, null), Seen.parse("R 1 2 3"));
    try std.testing.expectEqual(@as(?Seen, null), Seen.parse("X 1 2 3 4"));
    try std.testing.expectEqual(@as(?Seen, null), Seen.parse("R 64 2 3 4"));
}
