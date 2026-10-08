//! One-way throughput: the peer (the client) sends messages of one size as fast as the API lets it, as plain
//! messages or RPC requests, and this process (the server) receives them.
//!
//! The sender warms up for `warmup_ns`, measures its rate and announces a plan in a control message: `runs` runs of
//! `count` messages, about `duration_ns` each (with `messages` set, that many). The receiver times each run's messages
//! as one window. The windows follow each other, so every run measures steady traffic. Then the receiver says goodbye
//! through the other direction and both close. Every message's length is checked; payloads are 'x' bytes.
//!
//! The zero-copy scenario sends a message of one piece in place (send_acquire, then send_commit) and receives it in
//! place (recv_acquire, then recv_release); a message longer than one piece goes through the copying calls, as the
//! header says to (TOO_LARGE).

const std = @import("std");
const fipc = @import("fipc.zig");
const scenarios = @import("scenarios.zig");
const timing = @import("timing.zig");

const Ctx = scenarios.Ctx;
const Results = scenarios.Results;

/// How long a side waits for the other to connect.
pub const connect_timeout_ms = 15_000;
const goodbye_timeout_ms = 30_000;
pub const goodbye = "BYE";

const data_opcode: u32 = 1;
/// RPC control messages (the plan, the goodbye).
const control_opcode: u32 = 0xFB00_0001;

/// The sender's announcement: `runs` runs of `count` messages follow.
pub const Plan = struct {
    runs: u32,
    count: u64,

    const magic: u32 = 0xFB1C_5A7E;
    pub const len = 16;

    /// Enough messages for `ctx.duration_ns` at the warm-up's rate (`sent` in `elapsed_ns`), or `ctx.messages` if set.
    pub fn forRate(ctx: *const Ctx, sent: u64, elapsed_ns: u64) Plan {
        if (ctx.messages > 0) return .{ .runs = ctx.runs, .count = ctx.messages };
        const count = @as(f64, @floatFromInt(sent)) * @as(f64, @floatFromInt(ctx.duration_ns)) / @as(f64, @floatFromInt(elapsed_ns));
        return .{ .runs = ctx.runs, .count = @max(1, @as(u64, @intFromFloat(@ceil(count)))) };
    }

    pub fn total(plan: Plan) u64 {
        return plan.count * plan.runs;
    }

    pub fn encode(plan: Plan) [len]u8 {
        var bytes: [len]u8 = undefined;
        std.mem.writeInt(u32, bytes[0..4], magic, .little);
        std.mem.writeInt(u32, bytes[4..8], plan.runs, .little);
        std.mem.writeInt(u64, bytes[8..16], plan.count, .little);
        return bytes;
    }

    /// The plan in `bytes`, or null if they aren't one.
    pub fn decode(bytes: []const u8) ?Plan {
        if (bytes.len != len or std.mem.readInt(u32, bytes[0..4], .little) != magic) return null;
        return .{ .runs = std.mem.readInt(u32, bytes[4..8], .little), .count = std.mem.readInt(u64, bytes[8..16], .little) };
    }
};

/// Where the receiver's first window starts: at the plan (the warm-up just ended), or without a warm-up when the
/// connection was made, so a run of few messages includes the sender's start-up.
fn firstWindowStart(ctx: *const Ctx, connected_ns: u64) u64 {
    return if (ctx.warmup_ns == 0) connected_ns else timing.now(ctx.io);
}

/// Messages sent between two clock readings of a warm-up: about 64 KiB, at most 4096 messages.
pub fn batchSize(size: usize) usize {
    return std.math.clamp(64 * 1024 / @max(size, 1), 1, 4096);
}

/// Sends for `ctx.warmup_ns` (none without a warm-up), then returns the plan for the runs.
pub fn warmUp(ctx: *const Ctx, sender: anytype) !Plan {
    if (ctx.warmup_ns == 0) return .{ .runs = ctx.runs, .count = @max(ctx.messages, 1) };
    const batch = batchSize(ctx.size);
    const start = timing.now(ctx.io);
    var sent: u64 = 0;
    while (true) {
        for (0..batch) |_| try sender.send();
        sent += batch;
        const elapsed = timing.now(ctx.io) - start;
        if (elapsed >= ctx.warmup_ns) return .forRate(ctx, sent, elapsed);
    }
}

/// A buffer of `len` bytes at the start of a page (the caller frees it).
pub fn hotBuffer(ctx: *const Ctx, len: usize) !scenarios.HotBuffer {
    return ctx.gpa.alignedAlloc(u8, scenarios.hot_align, len);
}

/// A payload of `ctx.size` 'x' bytes at the start of a page (the caller frees it).
pub fn payloadFor(ctx: *const Ctx) !scenarios.HotBuffer {
    const payload = try hotBuffer(ctx, ctx.size);
    @memset(payload, 'x');
    return payload;
}

// === Plain messages ===

/// The peer: sends the warm-up and the plan's messages.
pub fn streamSender(ctx: *const Ctx, comptime zero_copy: bool) !void {
    const conn = try fipc.connect(ctx.name, connect_timeout_ms);
    defer fipc.close(conn);

    const payload = try payloadFor(ctx);
    defer ctx.gpa.free(payload);
    const sender: StreamSender(zero_copy) = .{ .conn = conn, .payload = payload, .max_piece = fipc.maxPiece(conn) };
    const plan = try warmUp(ctx, sender);
    try fipc.send(conn, &plan.encode(), fipc.forever);
    for (0..plan.total()) |_| try sender.send();

    var reply: [goodbye.len]u8 = undefined;
    if (!std.mem.eql(u8, try fipc.recv(conn, &reply, goodbye_timeout_ms), goodbye)) return error.UnexpectedMessage;
}

fn StreamSender(comptime zero_copy: bool) type {
    return struct {
        conn: *fipc.Conn,
        payload: []const u8,
        max_piece: usize,

        inline fn send(sender: @This()) !void {
            if (zero_copy and sender.payload.len <= sender.max_piece) {
                const buffer = try fipc.sendAcquire(sender.conn, sender.payload.len, fipc.forever);
                @memcpy(buffer, sender.payload);
                return fipc.sendCommit(sender.conn, buffer.len);
            }
            return fipc.send(sender.conn, sender.payload, fipc.forever);
        }
    };
}

/// This process: receives and times the plan's runs.
pub fn streamReceiver(ctx: *const Ctx, comptime zero_copy: bool, results: *Results) !void {
    const conn = try fipc.serve(ctx.name, ctx.ring, connect_timeout_ms);
    defer fipc.close(conn);
    const connected = timing.now(ctx.io);

    // The copying receive's buffer (the plan fits too); a zero-copy receiver copies each message out of the ring into
    // it, as a consumer would (unless --no-touch), and receives a message of several pieces into it
    const buffer = try hotBuffer(ctx, @max(ctx.size, Plan.len));
    defer ctx.gpa.free(buffer);
    const receiver: StreamReceiver(zero_copy) = .{ .conn = conn, .size = ctx.size, .buffer = buffer, .touch = ctx.touch };

    const plan = while (true) {
        const msg = try receiver.next();
        if (Plan.decode(msg.bytes)) |plan| {
            receiver.release(msg);
            break plan;
        }
        try receiver.consume(msg);
    };
    var start = firstWindowStart(ctx, connected);
    for (0..plan.runs) |_| {
        for (0..plan.count) |_| try receiver.consume(try receiver.next());
        const end = timing.now(ctx.io);
        try results.recordThroughput(ctx.gpa, plan.count, ctx.size, end - start);
        start = end;
    }
    try fipc.send(conn, goodbye, fipc.forever);
}

fn StreamReceiver(comptime zero_copy: bool) type {
    return struct {
        conn: *fipc.Conn,
        size: usize,
        buffer: []u8,
        touch: bool,

        const Msg = struct { bytes: []const u8, acquired: bool };

        inline fn next(receiver: @This()) !Msg {
            if (zero_copy) {
                if (fipc.recvAcquire(receiver.conn, fipc.forever)) |bytes| {
                    return .{ .bytes = bytes, .acquired = true };
                } else |err| switch (err) {
                    error.TooLarge => {}, // several pieces: receive it with the copying call
                    else => return err,
                }
            }
            return .{ .bytes = try fipc.recv(receiver.conn, receiver.buffer, fipc.forever), .acquired = false };
        }

        /// Checks a data message, copies it out (zero-copy with touch) and consumes it.
        inline fn consume(receiver: @This(), msg: Msg) !void {
            if (msg.bytes.len != receiver.size) return error.UnexpectedMessage;
            if (msg.acquired and receiver.touch) @memcpy(receiver.buffer[0..msg.bytes.len], msg.bytes);
            receiver.release(msg);
        }

        inline fn release(receiver: @This(), msg: Msg) void {
            if (msg.acquired) fipc.recvRelease(receiver.conn);
        }
    };
}

// === RPC ===

/// The peer (the client): submits the warm-up and the plan's requests.
pub fn rpcSender(ctx: *const Ctx) !void {
    const conn = try fipc.connect(ctx.name, connect_timeout_ms);
    defer fipc.close(conn);

    const payload = try payloadFor(ctx);
    defer ctx.gpa.free(payload);
    const sender: RpcSender = .{ .conn = conn, .payload = payload };
    const plan = try warmUp(ctx, sender);
    _ = try fipc.submit(conn, control_opcode, &plan.encode(), fipc.forever);
    for (0..plan.total()) |_| try sender.send();

    var reply: [goodbye.len]u8 = undefined;
    const msg = try fipc.rpcRecv(conn, &reply, goodbye_timeout_ms);
    if (msg.opcode != control_opcode or !std.mem.eql(u8, reply[0..msg.len], goodbye)) return error.UnexpectedMessage;
}

const RpcSender = struct {
    conn: *fipc.Conn,
    payload: []const u8,

    inline fn send(sender: RpcSender) !void {
        _ = try fipc.submit(sender.conn, data_opcode, sender.payload, fipc.forever);
    }
};

/// This process (the server): receives the requests and times the plan's runs.
pub fn rpcReceiver(ctx: *const Ctx, results: *Results) !void {
    const conn = try fipc.serve(ctx.name, ctx.ring, connect_timeout_ms);
    defer fipc.close(conn);
    const connected = timing.now(ctx.io);

    const buffer = try hotBuffer(ctx, @max(ctx.size, Plan.len));
    defer ctx.gpa.free(buffer);

    const plan = while (true) {
        const msg = try fipc.rpcRecv(conn, buffer, fipc.forever);
        if (msg.opcode == control_opcode) break Plan.decode(buffer[0..msg.len]) orelse return error.UnexpectedMessage;
        if (msg.len != ctx.size) return error.UnexpectedMessage;
    };
    var start = firstWindowStart(ctx, connected);
    for (0..plan.runs) |_| {
        for (0..plan.count) |_| {
            const msg = try fipc.rpcRecv(conn, buffer, fipc.forever);
            if (msg.len != ctx.size) return error.UnexpectedMessage;
        }
        const end = timing.now(ctx.io);
        try results.recordThroughput(ctx.gpa, plan.count, ctx.size, end - start);
        start = end;
    }
    try fipc.respond(conn, 0, control_opcode, goodbye, fipc.forever);
}

test "a plan survives its encoding, and data isn't mistaken for one" {
    const plan: Plan = .{ .runs = 5, .count = 12_345_678 };
    try std.testing.expectEqual(plan, Plan.decode(&plan.encode()).?);
    try std.testing.expectEqual(@as(?Plan, null), Plan.decode(&@as([16]u8, @splat('x'))));
    try std.testing.expectEqual(@as(?Plan, null), Plan.decode(plan.encode()[0..15]));
}
