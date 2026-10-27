//! Gated subscribers (`FIPC_SUBSCRIBER_GATED`): the writer held back until they receive, for `fipc_send` and
//! `fipc_send_acquire`, the data ring's room within the call's timeout, the writer's cancel, a broadcast that refuses
//! them (FIPC_NO_WAIT), their receives (TOO_LARGE, in place, copied), and gated and lossy subscribers on threads.

const std = @import("std");
const fastipc = @import("fastipc");
const support = @import("../support.zig");
const shared = @import("shared.zig");
const c = support.c;

const lossy = shared.lossy;
const gated = shared.gated;
const joined = shared.joined;
const sendStamp = shared.sendStamp;
const recvStamp = shared.recvStamp;
const recvStampInPlace = shared.recvStampInPlace;
const Receiver = shared.Receiver;
const sleepsOf = shared.sleepsOf;
const awaitAsleep = shared.awaitAsleep;
const recvForever = shared.recvForever;

/// Waits until `writer` has announced that it sleeps for room. A flag left set by an earlier send that slept would end
/// this wait at once, so each test first has that flag taken (or sends only without waiting).
fn awaitWriterAsleep(writer: support.Conn) void {
    const layout = fastipc.test_hooks.channelLayout(writer.?);
    while (layout.shared.writer_sleeps.load(.seq_cst) == 0) std.Thread.yield() catch {};
}

fn sendForever(writer: support.Conn, result: *c.fipc_result_t) void {
    result.* = sendStamp(writer, 1 << 40, 64, -1);
}

test "fast: a broadcast of max_stall_ms 0 (FIPC_NO_WAIT) refuses a gated subscriber at once, and takes a lossy one" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    var broadcast: support.Broadcast = .{};
    const name = try support.openBroadcast(&t, &broadcast, &buf, "bc_nogated", 4096, 2, c.FIPC_NO_WAIT);
    defer broadcast.close();
    var none: support.Conn = null;
    var joiner: support.Joiner = .{};
    try t.check(joiner.start(name, gated, 5000), "a gated joiner");
    _ = t.expectEqual(joiner.join(&none), c.FIPC_INVALID, "a gated join on a broadcast of max_stall 0");
    _ = t.expect(joiner.elapsed_ms < 1500, "at once");
    const ok = try joined(&t, name, lossy);
    c.fipc_close(ok);
    return t.done();
}

test "fast: a gated subscriber holds the writer back until it receives; its close lets it go; cancel ends the wait" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    var broadcast: support.Broadcast = .{};
    const name = try support.openBroadcast(&t, &broadcast, &buf, "bc_gated", 4096, 2, c.FIPC_FOREVER);
    defer broadcast.close();
    const subscriber = try joined(&t, name, gated);
    const watcher = try joined(&t, name, lossy);
    defer c.fipc_close(watcher);
    // 64 cells: the 65th message waits for the gated subscriber; the lossy one holds nothing back
    for (0..64) |i| try t.checkEqual(sendStamp(broadcast.publisher, i, 64, 0), c.FIPC_OK, "send");
    _ = t.expectEqual(sendStamp(broadcast.publisher, 64, 64, 0), c.FIPC_TIMEOUT, "the writer waits for the gated subscriber");
    _ = t.expectEqual(sendStamp(broadcast.publisher, 64, 64, 20), c.FIPC_TIMEOUT, "for 20 ms too");
    // In place: the message stays the writer's limit until it is released
    var in: ?*const anyopaque = null;
    var len: usize = 0;
    try t.checkEqual(c.fipc_recv_acquire(subscriber, &in, &len, 0), c.FIPC_OK, "recv_acquire");
    const held: [*]const u8 = @ptrCast(in.?);
    _ = t.expectEqual(support.Stamp.check(held[0..len]) orelse 999, 0, "the first message, in place");
    _ = t.expectEqual(sendStamp(broadcast.publisher, 64, 64, 0), c.FIPC_TIMEOUT, "held: still no room");
    c.fipc_recv_release(subscriber);
    _ = t.expectEqual(sendStamp(broadcast.publisher, 64, 64, 0), c.FIPC_OK, "released: room for one");
    var result: c.fipc_result_t = 0;
    _ = t.expectEqual(recvStamp(subscriber, 0, &result) orelse 999, 1, "the next, copied");
    _ = t.expectEqual(sendStamp(broadcast.publisher, 65, 64, 0), c.FIPC_OK, "room for one more");
    // A send that waits ends at the subscriber's close
    var sent: c.fipc_result_t = c.FIPC_INVALID;
    var sender = try std.Thread.spawn(.{}, sendForever, .{ broadcast.publisher, &sent });
    awaitWriterAsleep(broadcast.publisher); // the release took the flag the 20 ms send left
    const begun = support.nowMs();
    c.fipc_close(subscriber);
    sender.join();
    _ = t.expectEqual(sent, c.FIPC_OK, "the waiting send, once the gated subscriber left");
    _ = t.expect(support.nowMs() - begun < 1000, "at once");
    // And at the writer's cancel
    const again = try joined(&t, name, gated);
    defer c.fipc_close(again);
    var i: u64 = 0;
    while (sendStamp(broadcast.publisher, 100 + i, 64, 0) == c.FIPC_OK) : (i += 1) {}
    sender = try std.Thread.spawn(.{}, sendForever, .{ broadcast.publisher, &sent });
    awaitWriterAsleep(broadcast.publisher);
    c.fipc_cancel(broadcast.publisher);
    sender.join();
    _ = t.expectEqual(sent, c.FIPC_CANCELLED, "the writer's waiting send");
    _ = t.expectEqual(recvStamp(again, 0, &result) orelse 999, 100, "the gated subscriber's first message");
    _ = t.expectEqual(sendStamp(broadcast.publisher, 200, 64, 0), c.FIPC_OK, "a send that needn't wait, after the cancel");
    return t.done();
}

fn acquireForever(writer: support.Conn, result: *c.fipc_result_t) void {
    var out: ?*anyopaque = null;
    result.* = c.fipc_send_acquire(writer, 1024, &out, -1);
}

test "fast: the writer's cancel ends a send_acquire that waits for a gated subscriber; one that needn't wait works" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    var broadcast: support.Broadcast = .{};
    const name = try support.openBroadcast(&t, &broadcast, &buf, "bc_acq_cancel", 4096, 1, c.FIPC_FOREVER);
    defer broadcast.close();
    const subscriber = try joined(&t, name, gated);
    defer c.fipc_close(subscriber);
    // Messages of 1 KiB: the 4 KiB data ring holds 4
    for (0..4) |i| try t.checkEqual(sendStamp(broadcast.publisher, i, 1024, 0), c.FIPC_OK, "send");
    var result: c.fipc_result_t = c.FIPC_OK;
    const acquirer = try std.Thread.spawn(.{}, acquireForever, .{ broadcast.publisher, &result });
    awaitWriterAsleep(broadcast.publisher);
    const begun = support.nowMs();
    c.fipc_cancel(broadcast.publisher);
    acquirer.join();
    _ = t.expectEqual(result, c.FIPC_CANCELLED, "the waiting send_acquire");
    _ = t.expect(support.nowMs() - begun < 1000, "at once");
    _ = t.expectEqual(c.fipc_send_commit(broadcast.publisher, 1), c.FIPC_INVALID, "it reserved nothing");
    var received: c.fipc_result_t = 0;
    _ = t.expectEqual(recvStamp(subscriber, 0, &received) orelse 999, 0, "the subscriber receives the first");
    var out: ?*anyopaque = null;
    try t.checkEqual(c.fipc_send_acquire(broadcast.publisher, 1024, &out, 0), c.FIPC_OK, "room for one: send_acquire after the cancel");
    support.Stamp.fill(@as([*]u8, @ptrCast(out.?))[0..1024], 4);
    try t.checkEqual(c.fipc_send_commit(broadcast.publisher, 1024), c.FIPC_OK, "commit");
    for (1..5) |i| _ = t.expectEqual(recvStamp(subscriber, 0, &received) orelse 999, i, "every message, in order");
    return t.done();
}

/// A send of message `seq` (`len` bytes) on a thread, which may wait 5 s, and how long it took.
const TimedSend = struct {
    result: c.fipc_result_t = c.FIPC_INVALID,
    took_ms: u64 = 0,

    fn run(timed: *TimedSend, writer: support.Conn, seq: u64, len: usize) void {
        const begun = support.nowMs();
        timed.result = sendStamp(writer, seq, len, 5000);
        timed.took_ms = support.nowMs() - begun;
    }
};

test "fast: a gated subscriber holds the data ring too, for send_acquire as for send, within the call's timeout" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    var broadcast: support.Broadcast = .{};
    const name = try support.openBroadcast(&t, &broadcast, &buf, "bc_room", 4096, 1, c.FIPC_FOREVER);
    defer broadcast.close();
    const subscriber = try joined(&t, name, gated);
    defer c.fipc_close(subscriber);
    // Messages of 1 KiB: the 4 KiB data ring holds 4, long before the 64 cells are taken
    for (0..4) |i| try t.checkEqual(sendStamp(broadcast.publisher, i, 1024, 0), c.FIPC_OK, "send");
    _ = t.expectEqual(sendStamp(broadcast.publisher, 4, 1024, 0), c.FIPC_TIMEOUT, "the data ring is full");
    var out: ?*anyopaque = null;
    _ = t.expectEqual(c.fipc_send_acquire(broadcast.publisher, 1024, &out, 0), c.FIPC_TIMEOUT, "send_acquire waits too");
    const begun = support.nowMs();
    _ = t.expectEqual(c.fipc_send_acquire(broadcast.publisher, 1024, &out, 50), c.FIPC_TIMEOUT, "send_acquire, 50 ms");
    const elapsed = support.nowMs() - begun;
    _ = t.expect(elapsed >= 45 and elapsed < 1000, "it waited its timeout");
    _ = t.expectEqual(c.fipc_send_commit(broadcast.publisher, 1), c.FIPC_INVALID, "a failed send_acquire reserved nothing");
    var result: c.fipc_result_t = 0;
    _ = t.expectEqual(recvStamp(subscriber, 0, &result) orelse 999, 0, "the subscriber receives the first");
    try t.checkEqual(c.fipc_send_acquire(broadcast.publisher, 1024, &out, 0), c.FIPC_OK, "room for one: send_acquire");
    support.Stamp.fill(@as([*]u8, @ptrCast(out.?))[0..1024], 4);
    try t.checkEqual(c.fipc_send_commit(broadcast.publisher, 1024), c.FIPC_OK, "commit");
    // A send that waits goes through as soon as the subscriber receives
    var timed: TimedSend = .{};
    const sender = try std.Thread.spawn(.{}, TimedSend.run, .{ &timed, broadcast.publisher, 5, 1024 });
    awaitWriterAsleep(broadcast.publisher); // the first receive took the flag the 50 ms send_acquire left
    _ = t.expectEqual(recvStamp(subscriber, 0, &result) orelse 999, 1, "the next");
    sender.join();
    _ = t.expectEqual(timed.result, c.FIPC_OK, "the waiting send");
    _ = t.expect(timed.took_ms < 2000, "once the subscriber received");
    for (2..6) |i| _ = t.expectEqual(recvStamp(subscriber, 1000, &result) orelse 999, i, "every message, in order");
    _ = t.expect(recvStamp(subscriber, 0, &result) == null and result == c.FIPC_TIMEOUT, "nothing more");
    return t.done();
}

test "fast: a gated subscriber's receives: TOO_LARGE keeps the message, a receive lets a held one go, then the end" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    var broadcast: support.Broadcast = .{};
    const name = try support.openBroadcast(&t, &broadcast, &buf, "bc_gated_recv", 4096, 2, c.FIPC_FOREVER);
    defer broadcast.close();
    const subscriber = try joined(&t, name, gated);
    defer c.fipc_close(subscriber);
    const other = try joined(&t, name, gated);
    defer c.fipc_close(other);
    var result: c.fipc_result_t = 0;
    try t.checkEqual(support.send(broadcast.publisher, "a message of 26 bytes long", 0), c.FIPC_OK, "send");
    var small: [8]u8 = undefined;
    var len: usize = 0;
    _ = t.expectEqual(support.recv(subscriber, &small, &len, 1000), c.FIPC_TOO_LARGE, "a buffer too small");
    _ = t.expectEqual(len, 26, "the message's length");
    _ = t.expect(support.recvIs(subscriber, 0, "a message of 26 bytes long"), "the message, still there");
    _ = t.expect(support.recvIs(other, 0, "a message of 26 bytes long"), "the other's");
    // A receive lets the message held in place go; a release without one is nothing
    for (1..4) |i| try t.checkEqual(sendStamp(broadcast.publisher, i, 40, 0), c.FIPC_OK, "send");
    var at: ?*const anyopaque = null;
    try t.checkEqual(c.fipc_recv_acquire(subscriber, &at, &len, 0), c.FIPC_OK, "recv_acquire");
    _ = t.expectEqual(support.Stamp.check(@as([*]const u8, @ptrCast(at.?))[0..len]) orelse 999, 1, "held in place");
    _ = t.expectEqual(recvStamp(subscriber, 0, &result) orelse 999, 2, "a receive goes on after it");
    c.fipc_recv_release(subscriber);
    c.fipc_recv_release(subscriber);
    _ = t.expectEqual(recvStampInPlace(subscriber, 0, &result) orelse 999, 3, "and nothing was skipped");
    // Cancel ends a waiting receive; what is there is still received
    var waited: c.fipc_result_t = c.FIPC_OK;
    const since = sleepsOf(subscriber);
    const thread = try std.Thread.spawn(.{}, recvForever, .{ subscriber, &waited });
    awaitAsleep(subscriber, since);
    c.fipc_cancel(subscriber);
    thread.join();
    _ = t.expectEqual(waited, c.FIPC_CANCELLED, "the waiting receive");
    try t.checkEqual(sendStamp(broadcast.publisher, 4, 40, 0), c.FIPC_OK, "send");
    _ = t.expectEqual(recvStamp(subscriber, 0, &result) orelse 999, 4, "a message that is there");
    _ = t.expectEqual(recvStamp(subscriber, 0, &result) orelse 999, 999, "nothing more");
    _ = t.expectEqual(result, c.FIPC_CANCELLED, "CANCELLED, where a receive would wait");
    // The writer's close: the other receives what was sent, in place, then DISCONNECTED
    try t.checkEqual(sendStamp(broadcast.publisher, 5, 40, 0), c.FIPC_OK, "send");
    c.fipc_close(broadcast.publisher);
    broadcast.publisher = null;
    for (1..6) |i| _ = t.expectEqual(recvStampInPlace(other, 2000, &result) orelse 999, i, "what was sent");
    const begun = support.nowMs();
    _ = t.expect(recvStampInPlace(other, 5000, &result) == null and result == c.FIPC_DISCONNECTED, "then DISCONNECTED");
    _ = t.expect(support.nowMs() - begun < 2000, "promptly");
    _ = t.expect(recvStamp(other, 0, &result) == null and result == c.FIPC_DISCONNECTED, "a copying receive too");
    return t.done();
}

test "fast: gated and lossy subscribers on threads: the gated ones receive every message in order, copied or in place" {
    var t = try support.begin(@src(), 60);
    defer t.end();
    var buf: [64]u8 = undefined;
    var broadcast: support.Broadcast = .{};
    // A small broadcast, so that the writer waits for the gated subscribers again and again
    const name = try support.openBroadcast(&t, &broadcast, &buf, "bc_threads_gated", 16384, 4, c.FIPC_FOREVER);
    defer broadcast.close();
    var receivers = [_]Receiver{
        .{ .subscriber = try joined(&t, name, lossy) },
        .{ .subscriber = try joined(&t, name, lossy) },
        .{ .subscriber = try joined(&t, name, gated) },
        .{ .subscriber = try joined(&t, name, gated), .in_place = true },
    };
    for (&receivers) |*receiver| receiver.thread = try std.Thread.spawn(.{}, Receiver.run, .{receiver});
    const count = 20_000;
    for (0..count) |i| try t.checkEqual(sendStamp(broadcast.publisher, i, support.Stamp.sizeOf(i, 2048), 5000), c.FIPC_OK, "send");
    c.fipc_close(broadcast.publisher);
    broadcast.publisher = null;
    for (&receivers, 0..) |*receiver, i| {
        receiver.thread.join();
        c.fipc_close(receiver.subscriber);
        _ = t.expectEqual(receiver.end, c.FIPC_DISCONNECTED, "the stream ends with DISCONNECTED");
        _ = t.expectEqual(receiver.tally.bad, 0, "every message whole and in order");
        _ = t.expect(receiver.tally.complete(count), "up to the last message");
        if (i >= 2) _ = t.expect(receiver.tally.overruns == 0 and receiver.tally.received == count, "a gated subscriber: every one");
    }
    return t.done();
}
