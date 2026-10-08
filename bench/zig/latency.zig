//! Latency and connection cost:
//!
//! - Ping-pong: this process sends a message and waits for the peer's reply; a sample is the round
//!   trip minus the peer's hold time, which the reply carries. With think time 0 the peer replies at
//!   once and both sides catch each message while they spin (latency-spin). With a think time longer
//!   than the library's spin phase, this side falls asleep before each reply arrives, so a sample is a
//!   message to a spinning receiver plus one to a sleeping one (latency-sleep).
//! - Wake-up: the peer sends a message every millisecond, stamped with its time-stamp counter; this
//!   process sleeps in `recv` in between and records each message's age on arrival (wakeup), or polls
//!   with `recv` and timeout 0 in a loop, never sleeping (wakeup-poll).
//! - Setup: a connection's whole life, over and over: this process listens, the peer connects, this
//!   process accepts, both close, and this process stops listening. A second, long-lived connection
//!   paces the cycles.
//!
//! This process is the server and the peer the client. Samples are counter ticks (timing.zig),
//! converted at the rate measured over their run.

const std = @import("std");
const fipc = @import("fipc.zig");
const scenarios = @import("scenarios.zig");
const throughput = @import("throughput.zig");
const timing = @import("timing.zig");

const Ctx = scenarios.Ctx;
const Results = scenarios.Results;

const connect_timeout_ms = throughput.connect_timeout_ms;
const goodbye_timeout_ms = 30_000;

/// A message of these scenarios, in its first 8 bytes: a tag (the top bit says stop, the other 31 bits are the sequence
/// number's low bits) and the low 32 bits of a counter value (the peer's hold time in a reply, the send time in a
/// wake-up message), then 'x' bytes up to the case's size. Eight bytes, so a case can measure messages as short as
/// that; a hold time or a message's age is far shorter than 2^32 ticks (over a second on every supported CPU).
const Message = struct {
    stop: bool = false,
    seq: u31,
    stamp: u32,

    const len = 8;
    const stop_bit: u32 = 1 << 31;

    fn write(msg: Message, bytes: []u8) void {
        std.mem.writeInt(u32, bytes[0..4], (if (msg.stop) stop_bit else 0) | msg.seq, .little);
        std.mem.writeInt(u32, bytes[4..8], msg.stamp, .little);
    }

    fn read(bytes: []const u8) !Message {
        if (bytes.len < len) return error.UnexpectedMessage;
        const tag = std.mem.readInt(u32, bytes[0..4], .little);
        return .{ .stop = tag & stop_bit != 0, .seq = @truncate(tag), .stamp = std.mem.readInt(u32, bytes[4..8], .little) };
    }

    /// Whether this is message `seq` of its case.
    fn is(msg: Message, seq: u64) bool {
        return msg.seq == @as(u31, @truncate(seq));
    }
};

fn messageBuffer(ctx: *const Ctx) !scenarios.HotBuffer {
    const bytes = try throughput.payloadFor(ctx);
    if (bytes.len < Message.len) return error.MessageTooShort;
    return bytes;
}

/// This process's end of the case's connection.
fn serve(ctx: *const Ctx) !*fipc.Conn {
    return fipc.serve(ctx.name, ctx.ring, connect_timeout_ms);
}

/// The peer's end.
fn connect(ctx: *const Ctx) !*fipc.Conn {
    return fipc.connect(ctx.name, connect_timeout_ms);
}

// === Ping-pong ===

pub fn pinger(ctx: *const Ctx, results: *Results) !void {
    const conn = try serve(ctx);
    defer fipc.close(conn);
    const ping = try messageBuffer(ctx);
    defer ctx.gpa.free(ping);
    const reply = try messageBuffer(ctx);
    defer ctx.gpa.free(reply);

    var seq: u64 = 0;
    const warmup_end = timing.now(ctx.io) + ctx.warmup_ns;
    while (timing.now(ctx.io) < warmup_end) {
        seq += 1;
        _ = try roundTrip(conn, ping, reply, seq);
    }

    var samples: std.ArrayList(u64) = .empty;
    defer samples.deinit(ctx.gpa);
    for (0..ctx.runs) |_| {
        samples.clearRetainingCapacity();
        const start = timing.Stamp.take(ctx.io);
        const end_ns = start.ns + ctx.duration_ns;
        while (timing.now(ctx.io) < end_ns) {
            // The clock is read between batches only, never inside a sample
            try samples.ensureUnusedCapacity(ctx.gpa, 16);
            for (0..16) |_| {
                seq += 1;
                samples.appendAssumeCapacity(try roundTrip(conn, ping, reply, seq));
            }
        }
        const end = timing.Stamp.take(ctx.io);
        std.mem.sort(u64, samples.items, {}, std.sort.asc(u64));
        try results.recordLatency(ctx.gpa, samples.items, start.nsPerTick(end));
    }

    (Message{ .stop = true, .seq = @truncate(seq + 1), .stamp = 0 }).write(ping);
    try fipc.send(conn, ping, fipc.forever);
    _ = try fipc.recv(conn, reply, goodbye_timeout_ms);
}

/// One ping and its reply (into `reply`): the round trip in ticks, minus the peer's hold time.
inline fn roundTrip(conn: *fipc.Conn, ping: []u8, reply: []u8, seq: u64) !u64 {
    (Message{ .seq = @truncate(seq), .stamp = 0 }).write(ping);
    const sent = timing.ticks();
    try fipc.send(conn, ping, fipc.forever);
    const reply_bytes = try fipc.recv(conn, reply, fipc.forever);
    const received = timing.ticks();
    const answer = try Message.read(reply_bytes);
    if (!answer.is(seq) or reply_bytes.len != ping.len) return error.UnexpectedMessage;
    return (received - sent) -| answer.stamp;
}

/// The peer: replies to every ping after `ctx.think_ns`, busy-waiting (a sleep would be too coarse),
/// and puts its hold time in the reply.
pub fn ponger(ctx: *const Ctx) !void {
    const conn = try connect(ctx);
    defer fipc.close(conn);
    const ping_buffer = try messageBuffer(ctx);
    defer ctx.gpa.free(ping_buffer);
    const reply = try messageBuffer(ctx);
    defer ctx.gpa.free(reply);

    while (true) {
        const ping_bytes = try fipc.recv(conn, ping_buffer, fipc.forever);
        const received = timing.ticks();
        if (ctx.think_ns > 0) timing.spinUntil(ctx.io, timing.now(ctx.io) + ctx.think_ns);
        const ping = try Message.read(ping_bytes);
        (Message{ .stop = ping.stop, .seq = ping.seq, .stamp = @truncate(timing.ticks() - received) }).write(reply);
        try fipc.send(conn, reply, fipc.forever);
        if (ping.stop) return;
    }
}

// === Wake-up ===

/// The sending period.
const wakeup_period_ns = std.time.ns_per_ms;

/// Messages of the warm-up and of each run, the same on both sides.
fn wakeupCounts(ctx: *const Ctx) struct { warmup: u64, run: u64 } {
    return .{ .warmup = ctx.warmup_ns / wakeup_period_ns, .run = @max(1, ctx.duration_ns / wakeup_period_ns) };
}

/// This process: receives the messages and records their ages, waiting in `recv` or, if `polls`, calling it with timeout
/// 0 until each message is there.
pub fn wakeupReceiver(ctx: *const Ctx, comptime polls: bool, results: *Results) !void {
    const conn = try serve(ctx);
    defer fipc.close(conn);
    const buffer = try messageBuffer(ctx);
    defer ctx.gpa.free(buffer);

    const counts = wakeupCounts(ctx);
    var seq: u64 = 0;
    for (0..counts.warmup) |_| {
        seq += 1;
        _ = try receiveStamped(conn, buffer, seq, polls);
    }
    const samples = try ctx.gpa.alloc(u64, counts.run);
    defer ctx.gpa.free(samples);
    for (0..ctx.runs) |_| {
        const start = timing.Stamp.take(ctx.io);
        for (samples) |*sample| {
            seq += 1;
            sample.* = try receiveStamped(conn, buffer, seq, polls);
        }
        const end = timing.Stamp.take(ctx.io);
        std.mem.sort(u64, samples, {}, std.sort.asc(u64));
        try results.recordLatency(ctx.gpa, samples, start.nsPerTick(end));
    }
    try fipc.send(conn, throughput.goodbye, fipc.forever);
}

/// Waits for message `seq` (into `buffer`, the message's size) and returns its age in ticks.
fn receiveStamped(conn: *fipc.Conn, buffer: []u8, seq: u64, comptime polls: bool) !u64 {
    const bytes = try if (polls) poll(conn, buffer) else fipc.recv(conn, buffer, fipc.forever);
    const received = timing.ticks();
    const msg = try Message.read(bytes);
    if (!msg.is(seq) or bytes.len != buffer.len) return error.UnexpectedMessage;
    return @as(u32, @truncate(received)) -% msg.stamp;
}

/// The next message, by calling `recv` with timeout 0 until one is there: the loop a caller writes who wants the
/// lowest latency at any message rate, and gives a core to it.
inline fn poll(conn: *fipc.Conn, buffer: []u8) ![]u8 {
    while (true) {
        return fipc.recv(conn, buffer, fipc.no_wait) catch |err| switch (err) {
            error.Timeout => continue,
            else => return err,
        };
    }
}

/// The peer: one message per period, on time (busy-waiting between them), until the receiver has all
/// it needs.
pub fn wakeupSender(ctx: *const Ctx) !void {
    const conn = try connect(ctx);
    defer fipc.close(conn);
    const msg = try messageBuffer(ctx);
    defer ctx.gpa.free(msg);

    const counts = wakeupCounts(ctx);
    var next = timing.now(ctx.io);
    for (1..counts.warmup + ctx.runs * counts.run + 1) |seq| {
        next += wakeup_period_ns;
        timing.spinUntil(ctx.io, next);
        (Message{ .seq = @truncate(seq), .stamp = @truncate(timing.ticks()) }).write(msg);
        try fipc.send(conn, msg, fipc.forever);
    }
    var reply: [throughput.goodbye.len]u8 = undefined;
    if (!std.mem.eql(u8, try fipc.recv(conn, &reply, goodbye_timeout_ms), throughput.goodbye)) return error.UnexpectedMessage;
}

// === Setup ===

/// The long-lived connection that paces the cycles.
const control_ring = 64 * 1024;

/// The pacing connection's name.
pub fn controlName(name: []const u8, buffer: []u8) ![:0]const u8 {
    return std.mem.printSentinel(buffer, "{s}_ctl", .{name}, 0);
}

/// This process: listens, tells the peer to connect ("G"), accepts, closes the connection and the listener, and waits
/// for the peer's confirmation that it closed its end too ("K"). A cycle is the time from one confirmation to the next.
pub fn setupLeader(ctx: *const Ctx, results: *Results) !void {
    var name_buffer: [128]u8 = undefined;
    const control = try fipc.serve(try controlName(ctx.name, &name_buffer), control_ring, connect_timeout_ms);
    defer fipc.close(control);

    const warmup_end = timing.now(ctx.io) + ctx.warmup_ns;
    while (timing.now(ctx.io) < warmup_end) try leadCycle(ctx, control);

    var cycle_ns: std.ArrayList(u64) = .empty;
    defer cycle_ns.deinit(ctx.gpa);
    for (0..ctx.runs) |_| {
        cycle_ns.clearRetainingCapacity();
        const start = timing.now(ctx.io);
        var last = start;
        while (last - start < ctx.duration_ns) {
            try leadCycle(ctx, control);
            const now = timing.now(ctx.io);
            try cycle_ns.append(ctx.gpa, now - last);
            last = now;
        }
        std.mem.sort(u64, cycle_ns.items, {}, std.sort.asc(u64));
        try results.record(ctx.gpa, &.{
            @as(f64, @floatFromInt(cycle_ns.items.len)) / timing.seconds(last - start),
            @floatFromInt(timing.quantile(cycle_ns.items, 0.5)),
            @floatFromInt(timing.quantile(cycle_ns.items, 0.99)),
        });
        results.samples = cycle_ns.items.len;
    }
    try fipc.send(control, "S", fipc.forever); // the peer stops
}

fn leadCycle(ctx: *const Ctx, control: *fipc.Conn) !void {
    const listener = try fipc.listen(ctx.name, ctx.ring);
    {
        defer fipc.closeListener(listener);
        try fipc.send(control, "G", fipc.forever);
        fipc.close(try fipc.accept(listener, connect_timeout_ms));
    }
    var confirmation: [1]u8 = undefined;
    _ = try fipc.recv(control, &confirmation, fipc.forever);
}

/// The peer: connects and closes at each "G", and confirms with "K"; stops at "S".
pub fn setupFollower(ctx: *const Ctx) !void {
    var name_buffer: [128]u8 = undefined;
    const control = try fipc.connect(try controlName(ctx.name, &name_buffer), connect_timeout_ms);
    defer fipc.close(control);

    var command: [1]u8 = undefined;
    while (true) {
        _ = try fipc.recv(control, &command, fipc.forever);
        if (command[0] == 'S') return;
        fipc.close(try fipc.connect(ctx.name, connect_timeout_ms));
        try fipc.send(control, "K", fipc.forever);
    }
}
