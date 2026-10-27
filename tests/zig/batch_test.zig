//! Batches, `fipc_send_many` and `fipc_recv_many`, on every kind: the argument checks, done before anything is sent; a
//! batch arrives whole, in order, packed back to back; a send that can't wait sends what fits, a timeout or cancel stops
//! a batch partway and `*out_sent` says how far it went; a receive takes what is there, stops before a message that
//! doesn't fit, and returns any other result only for its first message (TOO_LARGE, a lossy subscriber's OVERRUN, a
//! service client's ABANDONED); the connections that don't batch refuse both calls. In the slow tier, batches from
//! other processes (a stream's client, a queue's producers).

const std = @import("std");
const support = @import("support.zig");
const broadcast_shared = @import("broadcast/shared.zig");
const queue_shared = @import("queue/shared.zig");
const service_shared = @import("service/shared.zig");
const c = support.c;

/// The most messages a test sends or receives in one call.
const max_batch = 256;

/// Message `seq` of `len` bytes: each byte a function of the sequence number and its place, so that a receiver can
/// check every message by its length and number.
fn fill(buf: []u8, seq: usize) void {
    for (buf, 0..) |*byte, i| byte.* = @truncate(seq *% 131 +% i *% 7 +% 1);
}

fn matches(message: []const u8, seq: usize) bool {
    for (message, 0..) |byte, i| if (byte != @as(u8, @truncate(seq *% 131 +% i *% 7 +% 1))) return false;
    return true;
}

/// A batch of messages to send, each filled as `fill` fills message `first + i`.
const Outgoing = struct {
    storage: []u8,
    datas: [max_batch]?*const anyopaque = undefined,
    lens: [max_batch]usize = undefined,
    count: usize = 0,

    /// Messages `first` to `first + lens.len - 1`, of the lengths `lens`.
    fn init(lens: []const usize, first: usize) !Outgoing {
        var total: usize = 0;
        for (lens) |len| total += len;
        var out: Outgoing = .{ .storage = try std.testing.allocator.alloc(u8, @max(total, 1)), .count = lens.len };
        var at: usize = 0;
        for (lens, 0..) |len, i| {
            const message = out.storage[at..][0..len];
            fill(message, first + i);
            out.datas[i] = message.ptr;
            out.lens[i] = len;
            at += len;
        }
        return out;
    }

    /// `count` messages of `len` bytes each, from `first` on.
    fn same(count: usize, len: usize, first: usize) !Outgoing {
        var lens: [max_batch]usize = undefined;
        @memset(lens[0..count], len);
        return init(lens[0..count], first);
    }

    fn deinit(out: *Outgoing) void {
        std.testing.allocator.free(out.storage);
    }

    /// `fipc_send_many` of the first `count` messages (all of them by default), `*sent` set.
    fn send(out: *const Outgoing, conn: support.Conn, timeout_ms: c_int, sent: *usize) c.fipc_result_t {
        return c.fipc_send_many(conn, &out.datas, &out.lens, out.count, sent, timeout_ms);
    }
};

/// A batch received: `fipc_recv_many` into `buf`, the messages packed back to back, each one's length in `lens`.
const Incoming = struct {
    buf: [16384]u8 = undefined,
    lens: [max_batch]usize = undefined,
    count: usize = 0,
    result: c.fipc_result_t = 99,

    /// Receives into the first `buf_len` bytes, at most `max_count` messages.
    fn recv(in: *Incoming, conn: support.Conn, buf_len: usize, max_count: usize, timeout_ms: c_int) c.fipc_result_t {
        in.count = 12345;
        in.result = c.fipc_recv_many(conn, &in.buf, buf_len, &in.lens, max_count, &in.count, timeout_ms);
        return in.result;
    }

    /// Message `i` of the batch, at the sum of the lengths before it.
    fn message(in: *const Incoming, i: usize) []const u8 {
        var at: usize = 0;
        for (in.lens[0..i]) |len| at += len;
        return in.buf[at..][0..in.lens[i]];
    }

    /// Whether the batch is messages `first` to `first + count - 1`, each of its length in `lens` (if given) and whole.
    fn holds(in: *const Incoming, first: usize, lens: ?[]const usize) bool {
        for (0..in.count) |i| {
            if (lens) |expected| if (in.lens[i] != expected[first + i]) return false;
            if (!matches(in.message(i), first + i)) return false;
        }
        return true;
    }
};

/// Receives `count` messages, from `first` on, in batches of up to `max_count` into `buf_len` bytes, each within
/// `timeout_ms`: whether they all came whole and in order.
fn recvAll(t: *support.Test, conn: support.Conn, first: usize, count: usize, buf_len: usize, max_count: usize, timeout_ms: c_int) bool {
    var in: Incoming = .{};
    var next = first;
    while (next < first + count) {
        if (!t.expectEqual(in.recv(conn, buf_len, max_count, timeout_ms), c.FIPC_OK, "a batch received")) return false;
        if (!t.expect(in.count >= 1 and in.count <= max_count and next + in.count <= first + count, "a batch's count")) return false;
        if (!t.expect(in.holds(next, null), "a batch's messages whole and in order")) return false;
        next += in.count;
    }
    return true;
}

// === Every kind's argument checks ===

test "fast: batches: NULL pointers, empty batches and empty messages are INVALID, checked before anything is sent" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var pair: support.Pair = .{};
    try support.openPair(&t, &pair, "batch_args", 4096);
    defer pair.close();

    var out = try Outgoing.same(4, 8, 0);
    defer out.deinit();
    var sent: usize = 99;
    _ = t.expectEqual(c.fipc_send_many(pair.server, &out.datas, &out.lens, 4, null, 0), c.FIPC_INVALID, "no out_sent");
    _ = t.expectEqual(c.fipc_send_many(null, &out.datas, &out.lens, 4, &sent, 0), c.FIPC_INVALID, "no connection");
    _ = t.expectEqual(sent, 0, "*out_sent 0");
    sent = 99;
    _ = t.expectEqual(c.fipc_send_many(pair.server, null, &out.lens, 4, &sent, 0), c.FIPC_INVALID, "no datas");
    _ = t.expectEqual(sent, 0, "*out_sent 0");
    _ = t.expectEqual(c.fipc_send_many(pair.server, &out.datas, null, 4, &sent, 0), c.FIPC_INVALID, "no lens");
    _ = t.expectEqual(c.fipc_send_many(pair.server, &out.datas, &out.lens, 0, &sent, 0), c.FIPC_INVALID, "an empty batch");
    // An empty message, or one without its data, anywhere in the batch: nothing goes, not even the messages before it
    out.lens[2] = 0;
    sent = 99;
    _ = t.expectEqual(out.send(pair.server, 0, &sent), c.FIPC_INVALID, "an empty message");
    _ = t.expectEqual(sent, 0, "nothing sent");
    out.lens[2] = 8;
    out.datas[3] = null;
    _ = t.expectEqual(out.send(pair.server, 0, &sent), c.FIPC_INVALID, "a message without its data");
    _ = t.expectEqual(sent, 0, "nothing sent");
    var len: usize = 0;
    var buf: [16]u8 = undefined;
    _ = t.expectEqual(support.recv(pair.client, &buf, &len, 0), c.FIPC_TIMEOUT, "nothing arrived");

    var in: Incoming = .{};
    var count: usize = 99;
    _ = t.expectEqual(c.fipc_recv_many(pair.client, &in.buf, in.buf.len, &in.lens, 4, null, 0), c.FIPC_INVALID, "no out_count");
    _ = t.expectEqual(c.fipc_recv_many(null, &in.buf, in.buf.len, &in.lens, 4, &count, 0), c.FIPC_INVALID, "no connection");
    _ = t.expectEqual(count, 0, "*out_count 0");
    count = 99;
    _ = t.expectEqual(c.fipc_recv_many(pair.client, &in.buf, in.buf.len, null, 4, &count, 0), c.FIPC_INVALID, "no lens");
    _ = t.expectEqual(count, 0, "*out_count 0");
    _ = t.expectEqual(c.fipc_recv_many(pair.client, &in.buf, in.buf.len, &in.lens, 0, &count, 0), c.FIPC_INVALID, "max_count 0");
    _ = t.expectEqual(c.fipc_recv_many(pair.client, null, 16, &in.lens, 4, &count, 0), c.FIPC_INVALID, "no buffer, a length");
    // A buffer of 0 bytes asks for the next message's length, as fipc_recv's does
    try t.checkEqual(support.send(pair.server, "four", 0), c.FIPC_OK, "a send");
    count = 99;
    _ = t.expectEqual(c.fipc_recv_many(pair.client, null, 0, &in.lens, 4, &count, 0), c.FIPC_TOO_LARGE, "its length");
    _ = t.expect(count == 0 and in.lens[0] == 4, "TOO_LARGE: *out_count 0, lens[0] the length");
    _ = t.expectEqual(in.recv(pair.client, in.buf.len, 4, 0), c.FIPC_OK, "then the message");
    _ = t.expect(in.count == 1 and std.mem.eql(u8, in.message(0), "four"), "it, whole");
    return t.done();
}

// === Streams ===

test "fast: stream: a batch arrives whole and in order, packed back to back, across the stamped frames' edge" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var pair: support.Pair = .{};
    try support.openPair(&t, &pair, "batch_stream", 64 * 1024);
    defer pair.close();

    // Lengths on both sides of the stamped frames' edge (frames shorter than 256 bytes carry a stamp)
    var lens: [40]usize = undefined;
    for (&lens, 0..) |*len, i| len.* = 1 + (i * 37) % 300;
    var out = try Outgoing.init(&lens, 0);
    defer out.deinit();
    var sent: usize = 0;
    try t.checkEqual(out.send(pair.server, 0, &sent), c.FIPC_OK, "the batch");
    _ = t.expectEqual(sent, lens.len, "*out_sent: all of them");

    var in: Incoming = .{};
    try t.checkEqual(in.recv(pair.client, in.buf.len, 64, 0), c.FIPC_OK, "one receive");
    _ = t.expectEqual(in.count, lens.len, "takes every message there");
    _ = t.expect(in.holds(0, &lens), "each whole, in order");
    _ = t.expectEqual(in.recv(pair.client, in.buf.len, 64, 0), c.FIPC_TIMEOUT, "nothing more");
    _ = t.expectEqual(in.count, 0, "*out_count 0");

    // A receive takes at most max_count, and stops before a message too long for the rest of its buffer
    try t.checkEqual(out.send(pair.server, 0, &sent), c.FIPC_OK, "the batch again");
    try t.checkEqual(in.recv(pair.client, in.buf.len, 3, 0), c.FIPC_OK, "three");
    _ = t.expect(in.count == 3 and in.holds(0, &lens), "the first three");
    const room = lens[3] + lens[4] + lens[5] - 1;
    try t.checkEqual(in.recv(pair.client, room, 64, 0), c.FIPC_OK, "as many as fit");
    _ = t.expect(in.count == 2 and in.holds(3, &lens), "the two that fit");
    try t.checkEqual(in.recv(pair.client, in.buf.len, 64, 0), c.FIPC_OK, "the rest");
    _ = t.expect(in.count == lens.len - 5 and in.holds(5, &lens), "the rest, in order");
    // Plain receives take a batch's messages as any others
    try t.checkEqual(out.send(pair.server, 0, &sent), c.FIPC_OK, "the batch once more");
    for (0..lens.len) |i| {
        var buf: [512]u8 = undefined;
        var len: usize = 0;
        if (!t.expectEqual(support.recv(pair.client, &buf, &len, 0), c.FIPC_OK, "fipc_recv")) break;
        _ = t.expect(len == lens[i] and matches(buf[0..len], i), "fipc_recv: a batch's message");
    }
    return t.done();
}

test "fast: stream: a message too long for the buffer is TOO_LARGE only as the first, which stays" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var pair: support.Pair = .{};
    try support.openPair(&t, &pair, "batch_stream_too_large", 4096);
    defer pair.close();
    const lens = [_]usize{ 100, 10, 200, 10 };
    var out = try Outgoing.init(&lens, 0);
    defer out.deinit();
    var sent: usize = 0;
    try t.checkEqual(out.send(pair.server, 0, &sent), c.FIPC_OK, "the batch");
    var in: Incoming = .{};
    _ = t.expectEqual(in.recv(pair.client, 50, 8, 0), c.FIPC_TOO_LARGE, "the first doesn't fit");
    _ = t.expect(in.count == 0 and in.lens[0] == 100, "*out_count 0, lens[0] its length");
    try t.checkEqual(in.recv(pair.client, 150, 8, 0), c.FIPC_OK, "then it fits");
    _ = t.expect(in.count == 2 and in.holds(0, &lens), "it and the next; the third doesn't fit the rest");
    _ = t.expectEqual(in.recv(pair.client, 150, 8, 0), c.FIPC_TOO_LARGE, "the third, first now");
    _ = t.expect(in.count == 0 and in.lens[0] == 200, "its length");
    try t.checkEqual(in.recv(pair.client, 300, 8, 0), c.FIPC_OK, "it and the last");
    _ = t.expect(in.count == 2 and in.holds(2, &lens), "whole");
    return t.done();
}

/// A receiver of `count` messages, in batches, on a thread of its own: what it received (lengths, and whether each was
/// whole and in order).
const BatchReceiver = struct {
    conn: support.Conn,
    count: usize,
    lens: []const usize,
    max_count: usize = 8,
    received: usize = 0,
    calls: usize = 0,
    bad: bool = false,
    result: c.fipc_result_t = c.FIPC_OK,
    thread: std.Thread = undefined,

    fn start(receiver: *BatchReceiver) !void {
        receiver.thread = try std.Thread.spawn(.{}, run, .{receiver});
    }

    fn run(receiver: *BatchReceiver) void {
        var in: Incoming = .{};
        while (receiver.received < receiver.count) {
            receiver.result = in.recv(receiver.conn, in.buf.len, receiver.max_count, 5000);
            if (receiver.result != c.FIPC_OK) return;
            receiver.calls += 1;
            if (receiver.received + in.count > receiver.count or !in.holds(receiver.received, receiver.lens)) receiver.bad = true;
            receiver.received += in.count;
        }
    }

    fn join(receiver: *BatchReceiver) void {
        receiver.thread.join();
    }
};

test "fast: stream: a batch with messages in pieces: each whole, in order, the batch's timeout bounding its waits" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var pair: support.Pair = .{};
    try support.openPair(&t, &pair, "batch_stream_pieces", 4096);
    defer pair.close();
    // Through a ring of 4 KiB: 10000 bytes go in three pieces, 4033 in two, the others in one frame each
    const lens = [_]usize{ 10, 10000, 10, 300, 4033, 1, 2000, 7 };
    var out = try Outgoing.init(&lens, 0);
    defer out.deinit();
    var receiver: BatchReceiver = .{ .conn = pair.client, .count = lens.len, .lens = &lens };
    try receiver.start();
    var sent: usize = 0;
    _ = t.expectEqual(out.send(pair.server, 5000, &sent), c.FIPC_OK, "the batch");
    _ = t.expectEqual(sent, lens.len, "all of it");
    receiver.join();
    _ = t.expectEqual(receiver.result, c.FIPC_OK, "the receives");
    _ = t.expect(receiver.received == lens.len and !receiver.bad, "every message whole, in order");
    return t.done();
}

test "fast: stream: NO_WAIT sends what fits; a timeout and cancel stop a batch partway, and *out_sent says how far" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var pair: support.Pair = .{};
    try support.openPair(&t, &pair, "batch_stream_partial", 1024);
    defer pair.close();
    // Frames of 128 bytes: a ring of 1 KiB holds 8
    var out = try Outgoing.same(20, 100, 0);
    defer out.deinit();
    var sent: usize = 99;
    _ = t.expectEqual(out.send(pair.server, c.FIPC_NO_WAIT, &sent), c.FIPC_TIMEOUT, "NO_WAIT: what fits");
    _ = t.expectEqual(sent, 8, "eight went");
    _ = t.expect(recvAll(&t, pair.client, 0, 8, 16384, 64, 0), "the eight, whole");

    // A timeout stops it once the ring is full again, after its timeout
    const start = support.nowMs();
    _ = t.expectEqual(out.send(pair.server, 50, &sent), c.FIPC_TIMEOUT, "a timeout of 50 ms");
    const elapsed = support.nowMs() - start;
    _ = t.expectEqual(sent, 8, "eight went");
    _ = t.expect(elapsed >= 45 and elapsed < 3000, "after its timeout");
    _ = t.expect(recvAll(&t, pair.client, 0, 8, 16384, 64, 0), "the eight, whole");

    // After cancel, a batch that needn't wait still goes; one that would wait goes as far as it can
    c.fipc_cancel(pair.server);
    var few = try Outgoing.same(8, 100, 0);
    defer few.deinit();
    _ = t.expectEqual(few.send(pair.server, c.FIPC_FOREVER, &sent), c.FIPC_OK, "a batch that fits, after cancel");
    _ = t.expect(recvAll(&t, pair.client, 0, 8, 16384, 64, 0), "it arrived");
    _ = t.expectEqual(out.send(pair.server, c.FIPC_FOREVER, &sent), c.FIPC_CANCELLED, "one that must wait");
    _ = t.expectEqual(sent, 8, "eight went");
    _ = t.expect(recvAll(&t, pair.client, 0, 8, 16384, 64, 0), "the eight, whole");
    // And a receive that would wait
    c.fipc_cancel(pair.client);
    var in: Incoming = .{};
    _ = t.expectEqual(in.recv(pair.client, in.buf.len, 8, c.FIPC_FOREVER), c.FIPC_CANCELLED, "a receive that must wait");
    _ = t.expectEqual(in.count, 0, "*out_count 0");
    return t.done();
}

test "fast: stream: the peer's end: a batch is DISCONNECTED, and a receive drains what the peer sent first" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var pair: support.Pair = .{};
    try support.openPair(&t, &pair, "batch_stream_end", 4096);
    defer pair.close();
    var out = try Outgoing.same(5, 33, 0);
    defer out.deinit();
    var sent: usize = 0;
    try t.checkEqual(out.send(pair.client, 0, &sent), c.FIPC_OK, "the client's batch");
    c.fipc_close(pair.client);
    pair.client = null;
    try t.check(support.waitForEnd(pair.server, 5000), "the server sees the end");
    sent = 99;
    _ = t.expectEqual(out.send(pair.server, 0, &sent), c.FIPC_DISCONNECTED, "a batch to a peer that left");
    _ = t.expectEqual(sent, 0, "nothing went");
    _ = t.expect(recvAll(&t, pair.server, 0, 5, 16384, 2, 0), "what the client sent, drained");
    var in: Incoming = .{};
    _ = t.expectEqual(in.recv(pair.server, in.buf.len, 8, 1000), c.FIPC_DISCONNECTED, "then DISCONNECTED");
    _ = t.expectEqual(in.count, 0, "*out_count 0");
    return t.done();
}

test "fast: stream: batches both ways on threads, against plain sends and receives" {
    var t = try support.begin(@src(), 60);
    defer t.end();
    var pair: support.Pair = .{};
    try support.openPair(&t, &pair, "batch_stream_threads", 4096);
    defer pair.close();
    // 2000 messages of 1 to 600 bytes through a ring of 4 KiB, in batches of 1 to 16, to a receiver that takes up to 5
    // per call
    const count = 2000;
    var lens: [count]usize = undefined;
    for (&lens, 0..) |*len, i| len.* = 1 + (i * 7919) % 600;
    var receiver: BatchReceiver = .{ .conn = pair.client, .count = count, .lens = &lens, .max_count = 5 };
    try receiver.start();
    var next: usize = 0;
    var batch: usize = 1;
    while (next < count) : (batch = batch % 16 + 1) {
        const n = @min(batch, count - next);
        var out = try Outgoing.init(lens[next..][0..n], next);
        defer out.deinit();
        var sent: usize = 0;
        if (!t.expectEqual(out.send(pair.server, 5000, &sent), c.FIPC_OK, "a batch")) break;
        next += n;
    }
    receiver.join();
    _ = t.expectEqual(receiver.result, c.FIPC_OK, "the receives");
    _ = t.expect(receiver.received == count and !receiver.bad, "every message whole, in order");
    return t.done();
}

// === Broadcasts ===

test "fast: broadcast: a publisher's batch reaches its lossy and gated subscribers' batched receives; the others refuse" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var name_buf: [64]u8 = undefined;
    var broadcast: support.Broadcast = .{};
    const name = try support.openBroadcast(&t, &broadcast, &name_buf, "batch_broadcast", 64 * 1024, 2, -1);
    defer broadcast.close();
    const lossy = try broadcast_shared.joined(&t, name, broadcast_shared.lossy);
    defer c.fipc_close(lossy);
    const gated = try broadcast_shared.joined(&t, name, broadcast_shared.gated);
    defer c.fipc_close(gated);

    var lens: [30]usize = undefined;
    for (&lens, 0..) |*len, i| len.* = 1 + (i * 53) % 500;
    var out = try Outgoing.init(&lens, 0);
    defer out.deinit();
    var sent: usize = 0;
    try t.checkEqual(out.send(broadcast.publisher, 0, &sent), c.FIPC_OK, "the batch");
    _ = t.expectEqual(sent, lens.len, "all of it");
    for ([_]support.Conn{ lossy, gated }) |subscriber| {
        _ = t.expect(recvAll(&t, subscriber, 0, lens.len, 16384, 64, 0), "every message, whole, in order");
        var in: Incoming = .{};
        _ = t.expectEqual(in.recv(subscriber, in.buf.len, 64, 0), c.FIPC_TIMEOUT, "nothing more");
    }
    // After its first message a receive takes the further messages the publisher has moved past: 200 messages of 16 B
    // fill three sixteenths of the capacity and part of a fourth
    var many = try Outgoing.same(200, 16, 0);
    defer many.deinit();
    try t.checkEqual(many.send(broadcast.publisher, 0, &sent), c.FIPC_OK, "200 messages");
    for ([_]support.Conn{ lossy, gated }) |subscriber| {
        var in: Incoming = .{};
        try t.checkEqual(in.recv(subscriber, in.buf.len, max_batch, 0), c.FIPC_OK, "a batched receive");
        _ = t.expect(in.count > 1 and in.count < 200 and in.holds(0, null), "more than its first, in order");
        _ = t.expect(recvAll(&t, subscriber, in.count, 200 - in.count, 16384, max_batch, 0), "the newest, in the next calls");
    }
    // Over one piece: nothing goes
    var long = try Outgoing.init(&.{ 10, c.fipc_max_piece(broadcast.publisher) + 1 }, 0);
    defer long.deinit();
    sent = 99;
    _ = t.expectEqual(long.send(broadcast.publisher, 0, &sent), c.FIPC_TOO_LARGE, "a message over one piece");
    _ = t.expectEqual(sent, 0, "nothing went");
    var in: Incoming = .{};
    _ = t.expectEqual(in.recv(gated, in.buf.len, 8, 0), c.FIPC_TIMEOUT, "nothing arrived");
    // The calls a role doesn't make
    _ = t.expectEqual(out.send(lossy, 0, &sent), c.FIPC_INVALID, "a subscriber's batch");
    _ = t.expectEqual(in.recv(broadcast.publisher, in.buf.len, 8, 0), c.FIPC_INVALID, "a publisher's receive");
    return t.done();
}

test "fast: broadcast: a lapped lossy subscriber's batched receive returns OVERRUN once, then a recent run" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var name_buf: [64]u8 = undefined;
    var broadcast: support.Broadcast = .{};
    const name = try support.openBroadcast(&t, &broadcast, &name_buf, "batch_broadcast_overrun", 4096, 1, -1);
    defer broadcast.close();
    const lossy = try broadcast_shared.joined(&t, name, broadcast_shared.lossy);
    defer c.fipc_close(lossy);
    // 200 messages through 64 cells
    var out = try Outgoing.same(200, 16, 0);
    defer out.deinit();
    var sent: usize = 0;
    try t.checkEqual(out.send(broadcast.publisher, 0, &sent), c.FIPC_OK, "the batch: lossy subscribers hold nothing back");
    var in: Incoming = .{};
    _ = t.expectEqual(in.recv(lossy, in.buf.len, max_batch, 0), c.FIPC_OVERRUN, "the first is lapped");
    _ = t.expect(in.count == 0 and in.lens[0] >= 1, "*out_count 0, lens[0] the number lost");
    const lost = in.lens[0];
    // Then the run from where it went on (message `lost`, as it was at message 0) to the last, in as many calls as
    // it takes
    _ = t.expect(lost < 200 and recvAll(&t, lossy, lost, 200 - lost, 16384, max_batch, 0), "the rest, whole, in order");
    _ = t.expectEqual(in.recv(lossy, in.buf.len, max_batch, 0), c.FIPC_TIMEOUT, "nothing more");
    return t.done();
}

test "fast: broadcast: a gated subscriber holds a batch back: NO_WAIT sends what fits, a timeout stops it" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var name_buf: [64]u8 = undefined;
    var broadcast: support.Broadcast = .{};
    const name = try support.openBroadcast(&t, &broadcast, &name_buf, "batch_broadcast_gated", 4096, 1, -1);
    defer broadcast.close();
    const gated = try broadcast_shared.joined(&t, name, broadcast_shared.gated);
    defer c.fipc_close(gated);
    // 64 cells of messages of up to 64 bytes
    var out = try Outgoing.same(100, 16, 0);
    defer out.deinit();
    var sent: usize = 99;
    _ = t.expectEqual(out.send(broadcast.publisher, c.FIPC_NO_WAIT, &sent), c.FIPC_TIMEOUT, "NO_WAIT: what fits");
    _ = t.expectEqual(sent, 64, "64 went");
    const start = support.nowMs();
    var more = try Outgoing.same(10, 16, 64);
    defer more.deinit();
    _ = t.expectEqual(more.send(broadcast.publisher, 50, &sent), c.FIPC_TIMEOUT, "a timeout of 50 ms");
    _ = t.expect(sent == 0 and support.nowMs() - start >= 45, "nothing went, after its timeout");
    _ = t.expect(recvAll(&t, gated, 0, 64, 16384, 20, 0), "the 64, whole, in order");
    try t.checkEqual(more.send(broadcast.publisher, 0, &sent), c.FIPC_OK, "room again");
    _ = t.expect(recvAll(&t, gated, 64, 10, 16384, 64, 0), "the rest");
    return t.done();
}

// === Work queues ===

test "fast: queue: a producer's batch, a consumer's batched receive; TOO_LARGE, NO_WAIT; the others refuse" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var name_buf: [64]u8 = undefined;
    var queue: queue_shared.Queue = .{};
    const name = try queue_shared.openQueue(&t, &queue, &name_buf, "batch_queue", 4096, 1, 1, queue_shared.producer);
    defer queue.close();
    const consumer = try queue_shared.joined(&t, name, queue_shared.consumer);
    defer c.fipc_close(consumer);

    var lens: [20]usize = undefined;
    for (&lens, 0..) |*len, i| len.* = 1 + (i * 61) % 150;
    var out = try Outgoing.init(&lens, 0);
    defer out.deinit();
    var sent: usize = 0;
    try t.checkEqual(out.send(queue.own, 0, &sent), c.FIPC_OK, "the batch");
    _ = t.expectEqual(sent, lens.len, "all of it");
    var in: Incoming = .{};
    try t.checkEqual(in.recv(consumer, in.buf.len, 64, 0), c.FIPC_OK, "a batched receive");
    // One producer's messages, one consumer: in the order sent (the implementation keeps it)
    _ = t.expect(in.count == lens.len and in.holds(0, &lens), "every message, whole");
    _ = t.expectEqual(in.recv(consumer, in.buf.len, 64, 0), c.FIPC_TIMEOUT, "nothing more");

    // The ring holds 64 messages of up to 64 bytes
    var many = try Outgoing.same(100, 64, 0);
    defer many.deinit();
    _ = t.expectEqual(many.send(queue.own, c.FIPC_NO_WAIT, &sent), c.FIPC_TIMEOUT, "NO_WAIT: what fits");
    _ = t.expectEqual(sent, 64, "64 went");
    _ = t.expectEqual(in.recv(consumer, 100, 64, 0), c.FIPC_OK, "a receive into 100 bytes");
    _ = t.expect(in.count == 1 and in.holds(0, null), "one fits");
    _ = t.expect(recvAll(&t, consumer, 1, 63, 16384, 64, 0), "the rest");

    var long = try Outgoing.init(&.{ 10, c.fipc_max_piece(queue.own) + 1 }, 0);
    defer long.deinit();
    sent = 99;
    _ = t.expectEqual(long.send(queue.own, 0, &sent), c.FIPC_TOO_LARGE, "a message over one piece");
    _ = t.expectEqual(sent, 0, "nothing went");
    _ = t.expectEqual(in.recv(consumer, in.buf.len, 8, 0), c.FIPC_TIMEOUT, "nothing arrived");
    _ = t.expectEqual(out.send(consumer, 0, &sent), c.FIPC_INVALID, "a consumer's batch");
    _ = t.expectEqual(in.recv(queue.own, in.buf.len, 8, 0), c.FIPC_INVALID, "a producer's receive");
    return t.done();
}

test "fast: queue: the end: a producer's batch is DISCONNECTED, and a consumer drains in batches, then DISCONNECTED" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var name_buf: [64]u8 = undefined;
    var queue: queue_shared.Queue = .{};
    const name = try queue_shared.openQueue(&t, &queue, &name_buf, "batch_queue_end", 4096, 1, 1, 0);
    const producer = try queue_shared.joined(&t, name, queue_shared.producer);
    defer c.fipc_close(producer);
    const consumer = try queue_shared.joined(&t, name, queue_shared.consumer);
    defer c.fipc_close(consumer);
    var out = try Outgoing.same(10, 40, 0);
    defer out.deinit();
    var sent: usize = 0;
    try t.checkEqual(out.send(producer, 0, &sent), c.FIPC_OK, "the batch");
    queue.close();
    sent = 99;
    _ = t.expectEqual(out.send(producer, 0, &sent), c.FIPC_DISCONNECTED, "a batch after the end");
    _ = t.expectEqual(sent, 0, "nothing went");
    _ = t.expect(recvAll(&t, consumer, 0, 10, 16384, 3, 1000), "what was sent, drained");
    var in: Incoming = .{};
    _ = t.expectEqual(in.recv(consumer, in.buf.len, 8, 1000), c.FIPC_DISCONNECTED, "then DISCONNECTED");
    return t.done();
}

test "fast: queue: producers' batches and consumers' batched receives on threads: each message to exactly one consumer" {
    var t = try support.begin(@src(), 60);
    defer t.end();
    var name_buf: [64]u8 = undefined;
    var queue: queue_shared.Queue = .{};
    const name = try queue_shared.openQueue(&t, &queue, &name_buf, "batch_queue_threads", 4096, 3, 3, 0);
    defer queue.close();
    const producers = 3;
    const consumers = 3;
    const count = 3000;
    var ledger = try queue_shared.Ledger.init(producers, count);
    defer ledger.deinit();

    const Producer = struct {
        conn: support.Conn,
        index: u64,
        result: c.fipc_result_t = c.FIPC_OK,

        fn run(p: *@This()) void {
            var storage: [16][256]u8 = undefined;
            var datas: [16]?*const anyopaque = undefined;
            var lens: [16]usize = undefined;
            var next: usize = 0;
            var batch: usize = 1;
            while (next < count) : (batch = batch % 16 + 1) {
                const n = @min(batch, count - next);
                for (0..n) |i| {
                    const len = support.Stamp.sizeOf(next + i, storage[i].len);
                    support.Stamp.fill(storage[i][0..len], queue_shared.seqOf(p.index, next + i));
                    datas[i] = &storage[i];
                    lens[i] = len;
                }
                var sent: usize = 0;
                p.result = c.fipc_send_many(p.conn, &datas, &lens, n, &sent, 5000);
                if (p.result != c.FIPC_OK) return;
                next += n;
            }
        }
    };
    const Consumer = struct {
        conn: support.Conn,
        ledger: *queue_shared.Ledger,
        result: c.fipc_result_t = c.FIPC_OK,

        fn run(r: *@This()) void {
            var last: [64]?u64 = @splat(null);
            var in: Incoming = .{};
            while (true) {
                // Buffers of 600 bytes: a batch often stops at a message that doesn't fit the rest
                r.result = in.recv(r.conn, 600, 16, 1000);
                if (r.result != c.FIPC_OK) return;
                for (0..in.count) |i| r.ledger.record(&last, support.Stamp.check(in.message(i)));
            }
        }
    };
    var producer_states: [producers]Producer = undefined;
    var consumer_states: [consumers]Consumer = undefined;
    for (&producer_states, 0..) |*p, i| p.* = .{ .conn = try queue_shared.joined(&t, name, queue_shared.producer), .index = i };
    defer for (producer_states) |p| c.fipc_close(p.conn);
    for (&consumer_states) |*r| r.* = .{ .conn = try queue_shared.joined(&t, name, queue_shared.consumer), .ledger = &ledger };
    defer for (consumer_states) |r| c.fipc_close(r.conn);
    var threads: [producers + consumers]std.Thread = undefined;
    for (&producer_states, 0..) |*p, i| threads[i] = try std.Thread.spawn(.{}, Producer.run, .{p});
    for (&consumer_states, 0..) |*r, i| threads[producers + i] = try std.Thread.spawn(.{}, Consumer.run, .{r});
    for (threads) |thread| thread.join();
    for (producer_states) |p| _ = t.expectEqual(p.result, c.FIPC_OK, "a producer's batches");
    // The consumers end once nothing comes for 1 s
    for (consumer_states) |r| _ = t.expectEqual(r.result, c.FIPC_TIMEOUT, "a consumer's end");
    if (!t.expect(ledger.complete(), "each message once, whole, each producer's in order")) ledger.report();
    return t.done();
}

// === Services ===

/// Server `server` answers the next request: its reply is the request reversed.
fn answer(t: *support.Test, server: support.Conn) bool {
    var buf: [256]u8 = undefined;
    var len: usize = 0;
    if (!t.expectEqual(support.recv(server, &buf, &len, 5000), c.FIPC_OK, "a server's receive")) return false;
    std.mem.reverse(u8, buf[0..len]);
    return t.expectEqual(support.send(server, buf[0..len], 0), c.FIPC_OK, "its reply");
}

/// Whether reply `reply` answers request `seq` of `len` bytes (`answer`: the request reversed).
fn answers(reply: []const u8, seq: usize, len: usize) bool {
    if (reply.len != len) return false;
    var request: [256]u8 = undefined;
    @memcpy(request[0..len], reply);
    std.mem.reverse(u8, request[0..len]);
    return matches(request[0..len], seq);
}

test "fast: service: a client's batch of requests, its replies in order through batched receives; a server refuses both" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var name_buf: [64]u8 = undefined;
    var service: service_shared.Service = .{};
    _ = try service_shared.openService(&t, &service, &name_buf, "batch_service", 4096, 64, 1, 1, .{ .client = true, .server = true });
    defer service.close();
    const client = service.own_client;
    const server = service.own_server;

    var lens: [12]usize = undefined;
    for (&lens, 0..) |*len, i| len.* = 1 + (i * 17) % 64;
    var out = try Outgoing.init(&lens, 0);
    defer out.deinit();
    var sent: usize = 0;
    try t.checkEqual(out.send(client, 0, &sent), c.FIPC_OK, "the batch of requests");
    _ = t.expectEqual(sent, lens.len, "all of them");
    // Half answered: a batched receive takes those, and stops at the first without an answer
    for (0..6) |_| _ = answer(&t, server);
    var in: Incoming = .{};
    try t.checkEqual(in.recv(client, in.buf.len, 64, 0), c.FIPC_OK, "the replies there");
    _ = t.expectEqual(in.count, 6, "the six answered");
    for (0..in.count) |i| _ = t.expect(answers(in.message(i), i, lens[i]), "a reply, in its request's order");
    for (0..6) |_| _ = answer(&t, server);
    const room = 60;
    try t.checkEqual(in.recv(client, room, 64, 0), c.FIPC_OK, "a receive into 60 bytes");
    var taken: usize = 0;
    for (0..in.count) |i| taken += in.lens[i];
    _ = t.expect(in.count >= 1 and taken <= room and taken + lens[6 + in.count] > room, "as many replies as fit");
    for (0..in.count) |i| _ = t.expect(answers(in.message(i), 6 + i, lens[6 + i]), "a reply, in order");
    const next = 6 + in.count;
    try t.checkEqual(in.recv(client, in.buf.len, 64, 0), c.FIPC_OK, "the rest");
    _ = t.expectEqual(next + in.count, lens.len, "all of them");
    for (0..in.count) |i| _ = t.expect(answers(in.message(i), next + i, lens[next + i]), "a reply, in order");
    _ = t.expectEqual(in.recv(client, in.buf.len, 64, 0), c.FIPC_TIMEOUT, "none owed");

    // Over max_message: nothing goes
    var long = try Outgoing.init(&.{ 10, 65 }, 0);
    defer long.deinit();
    sent = 99;
    _ = t.expectEqual(long.send(client, 0, &sent), c.FIPC_TOO_LARGE, "a request over max_message");
    _ = t.expectEqual(sent, 0, "nothing went");
    var buf: [64]u8 = undefined;
    var len: usize = 0;
    _ = t.expectEqual(support.recv(server, &buf, &len, 0), c.FIPC_TIMEOUT, "no request came");
    // A server replies to the one request it holds
    _ = t.expectEqual(out.send(server, 0, &sent), c.FIPC_INVALID, "a server's batch");
    _ = t.expectEqual(in.recv(server, in.buf.len, 8, 0), c.FIPC_INVALID, "a server's batched receive");
    try t.checkEqual(support.send(client, "x", 0), c.FIPC_OK, "a request");
    try t.checkEqual(support.recv(server, &buf, &len, 1000), c.FIPC_OK, "the server holds it");
    _ = t.expectEqual(out.send(server, 0, &sent), c.FIPC_INVALID, "a server's batch, holding a request");
    try t.checkEqual(support.send(server, "y", 0), c.FIPC_OK, "its reply");
    _ = t.expect(support.recvIs(client, 1000, "y"), "the reply");
    return t.done();
}

test "fast: service: a batched receive returns an abandoned request only as its first" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var name_buf: [64]u8 = undefined;
    var service: service_shared.Service = .{};
    const name = try service_shared.openService(&t, &service, &name_buf, "batch_service_abandoned", 4096, 64, 1, 2, .{ .client = true, .server = true });
    defer service.close();
    const client = service.own_client;
    const keeper = service.own_server;
    const leaver = try service_shared.joined(&t, name, service_shared.server);
    var out = try Outgoing.same(3, 20, 0);
    defer out.deinit();
    var sent: usize = 0;
    try t.checkEqual(out.send(client, 0, &sent), c.FIPC_OK, "three requests");
    // The keeper takes request 0, the leaver request 1, then the keeper request 2
    var buf: [64]u8 = undefined;
    var len: usize = 0;
    try t.checkEqual(support.recv(keeper, &buf, &len, 1000), c.FIPC_OK, "request 0");
    _ = t.expect(len == 20 and matches(buf[0..len], 0), "request 0");
    try t.checkEqual(support.recv(leaver, &buf, &len, 1000), c.FIPC_OK, "request 1");
    try t.checkEqual(support.send(keeper, "reply 0", 0), c.FIPC_OK, "reply 0");
    c.fipc_close(leaver); // abandons request 1
    try t.checkEqual(support.recv(keeper, &buf, &len, 1000), c.FIPC_OK, "request 2");
    try t.checkEqual(support.send(keeper, "reply 2", 0), c.FIPC_OK, "reply 2");
    var in: Incoming = .{};
    try t.checkEqual(in.recv(client, in.buf.len, 8, 0), c.FIPC_OK, "reply 0");
    _ = t.expect(in.count == 1 and std.mem.eql(u8, in.message(0), "reply 0"), "it alone: the next is abandoned");
    _ = t.expectEqual(in.recv(client, in.buf.len, 8, 0), c.FIPC_ABANDONED, "the abandoned request, first");
    _ = t.expect(in.count == 0 and in.lens[0] == 0, "*out_count 0, lens[0] 0");
    try t.checkEqual(in.recv(client, in.buf.len, 8, 0), c.FIPC_OK, "reply 2");
    _ = t.expect(in.count == 1 and std.mem.eql(u8, in.message(0), "reply 2"), "it");
    return t.done();
}

test "fast: service: a client's batch waits for room its replies hold: NO_WAIT sends what fits" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var name_buf: [64]u8 = undefined;
    var service: service_shared.Service = .{};
    _ = try service_shared.openService(&t, &service, &name_buf, "batch_service_room", 4096, 64, 1, 1, .{ .client = true, .server = true });
    defer service.close();
    // Each request takes 64 bytes for itself and 64 for its reply: 32 fit a ring of 4 KiB
    var out = try Outgoing.same(40, 50, 0);
    defer out.deinit();
    var sent: usize = 99;
    _ = t.expectEqual(out.send(service.own_client, c.FIPC_NO_WAIT, &sent), c.FIPC_TIMEOUT, "NO_WAIT: what fits");
    _ = t.expectEqual(sent, 32, "32 went");
    for (0..32) |_| if (!answer(&t, service.own_server)) break;
    var in: Incoming = .{};
    try t.checkEqual(in.recv(service.own_client, in.buf.len, max_batch, 0), c.FIPC_OK, "the replies");
    _ = t.expectEqual(in.count, 32, "all 32");
    for (0..in.count) |i| _ = t.expect(answers(in.message(i), i, 50), "a reply, in order");
    var rest = try Outgoing.same(8, 50, 32);
    defer rest.deinit();
    try t.checkEqual(rest.send(service.own_client, 0, &sent), c.FIPC_OK, "room again");
    return t.done();
}

// === Other processes ===

test "slow: stream: batches from another process arrive whole and in order through batched receives" {
    var t = try support.begin(@src(), 120);
    defer t.end();
    var name_buf: [64]u8 = undefined;
    const name = support.uniqueName(&name_buf, "batch_stream_process");
    var listener: support.Listener = null;
    try t.checkEqual(c.fipc_stream_create(name, 4096, &listener), c.FIPC_OK, "a listener");
    defer c.fipc_listener_close(listener);
    const count = 20000;
    const peer = try support.Peer.spawn(&.{ "send-batches", name, "20000", "13" });
    defer peer.deinit();
    var conn: support.Conn = null;
    try t.checkEqual(c.fipc_stream_accept(listener, &conn, 10000), c.FIPC_OK, "the peer connects");
    defer c.fipc_close(conn);
    // Up to 6000 bytes each, some in pieces, into buffers of 8 KiB, up to 7 per call
    var tally: support.Stamp.Tally = .{};
    var in: Incoming = .{};
    while (tally.received < count) {
        if (!t.expectEqual(in.recv(conn, 8192, 7, 10000), c.FIPC_OK, "a batched receive")) break;
        for (0..in.count) |i| tally.message(support.Stamp.check(in.message(i)));
    }
    _ = t.expect(tally.complete(count), "every message, whole, in order");
    _ = t.expect(peer.waitFor("S", 10000), "the peer sent them all");
    peer.send("x");
    _ = t.expect(peer.waitTimeout(10000) and peer.exitedWith(0), "the peer closed");
    return t.done();
}

test "slow: queue: producer processes' batches, consumers' batched receives on threads: each message to exactly one consumer" {
    var t = try support.begin(@src(), 120);
    defer t.end();
    var name_buf: [64]u8 = undefined;
    var queue: queue_shared.Queue = .{};
    const name = try queue_shared.openQueue(&t, &queue, &name_buf, "batch_queue_processes", 4096, 3, 2, 0);
    defer queue.close();
    const count = 5000;
    var ledger = try queue_shared.Ledger.init(3, count);
    defer ledger.deinit();
    const Consumer = struct {
        conn: support.Conn,
        ledger: *queue_shared.Ledger,
        result: c.fipc_result_t = c.FIPC_OK,

        fn run(r: *@This()) void {
            var last: [64]?u64 = @splat(null);
            var in: Incoming = .{};
            while (true) {
                r.result = in.recv(r.conn, 1024, 9, 3000);
                if (r.result != c.FIPC_OK) return;
                for (0..in.count) |i| r.ledger.record(&last, support.Stamp.check(in.message(i)));
            }
        }
    };
    var consumers: [2]Consumer = undefined;
    for (&consumers) |*r| r.* = .{ .conn = try queue_shared.joined(&t, name, queue_shared.consumer), .ledger = &ledger };
    defer for (consumers) |r| c.fipc_close(r.conn);
    var peers: [3]*support.Peer = undefined;
    var spawned: usize = 0;
    defer for (peers[0..spawned]) |peer| peer.deinit();
    for (&peers, 0..) |*peer, i| {
        var index_buf: [4]u8 = undefined;
        peer.* = try support.Peer.spawn(&.{ "queue-produce-batches", name, try std.fmt.bufPrint(&index_buf, "{d}", .{i}), "5000", "11" });
        spawned += 1;
    }
    var producers: u32 = 0;
    try t.checkEqual(c.fipc_queue_wait_for_members(queue.handle, queue_shared.producer, 3, &producers, 30_000), c.FIPC_OK, "the producer processes join");
    var threads: [2]std.Thread = undefined;
    for (&consumers, &threads) |*r, *thread| thread.* = try std.Thread.spawn(.{}, Consumer.run, .{r});
    for (peers) |peer| _ = t.expect(peer.waitFor("SENT", 60_000), "a producer sent its stream");
    // The consumers end once nothing more comes for 3 s
    for (threads) |thread| thread.join();
    for (consumers) |r| _ = t.expectEqual(r.result, c.FIPC_TIMEOUT, "a consumer's end");
    if (!t.expect(ledger.complete(), "each message once, whole, each producer's in order")) ledger.report();
    for (peers) |peer| peer.send("x");
    for (peers) |peer| _ = t.expect(peer.waitTimeout(10_000) and peer.exitedWith(0), "a producer closed");
    return t.done();
}
