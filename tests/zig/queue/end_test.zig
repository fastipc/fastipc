//! A work queue's end, and a member's, through the C API, in one process: the queue's close, the end's mark a
//! member's watcher stores, a member's own failure (a frame from the owner), a member the owner drops, and the
//! timeouts.

const std = @import("std");
const support = @import("../support.zig");
const shared = @import("shared.zig");
const c = support.c;

const producer = shared.producer;
const consumer = shared.consumer;
const joined = shared.joined;
const sendStamp = shared.sendStamp;
const recvStamp = shared.recvStamp;

fn recvWaiting(conn: support.Conn, result: *c.fipc_result_t) void {
    var buf: [64]u8 = undefined;
    var len: usize = 0;
    result.* = support.recv(conn, &buf, &len, c.FIPC_FOREVER);
}

test "fast: the queue's close ends it: producers DISCONNECTED, consumers drain what is left, then DISCONNECTED" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    var queue: shared.Queue = .{};
    const name = try shared.openQueue(&t, &queue, &buf, "mq_end", 4096, 2, 3, consumer);
    defer c.fipc_close(queue.own);
    const giver = try joined(&t, name, producer);
    defer c.fipc_close(giver);
    const parked = try joined(&t, name, consumer);
    defer c.fipc_close(parked);
    const drainer = try joined(&t, name, consumer);
    defer c.fipc_close(drainer);
    // A consumer parked with nothing to take, woken by the end
    var parked_result: c.fipc_result_t = 0;
    const since = shared.sleepsOf(parked);
    const thread = try std.Thread.spawn(.{}, recvWaiting, .{ parked, &parked_result });
    shared.awaitAsleep(parked, since);
    // The owner's consumer parked too: the end wakes it as any member
    var own_result: c.fipc_result_t = 0;
    const own_since = shared.sleepsOf(queue.own);
    const own_thread = try std.Thread.spawn(.{}, recvWaiting, .{ queue.own, &own_result });
    shared.awaitAsleep(queue.own, own_since);
    c.fipc_queue_close(queue.handle);
    queue.handle = null;
    thread.join();
    own_thread.join();
    _ = t.expectEqual(parked_result, c.FIPC_DISCONNECTED, "the parked consumer: DISCONNECTED");
    _ = t.expectEqual(own_result, c.FIPC_DISCONNECTED, "the owner's consumer: DISCONNECTED");
    _ = t.expect(support.waitForEnd(giver, 5000), "the producer sees the end");
    _ = t.expectEqual(sendStamp(giver, 0, 32, 0), c.FIPC_DISCONNECTED, "its sends");
    var receive_buffer: [64]u8 = undefined;
    var len: usize = 0;
    _ = t.expectEqual(support.recv(drainer, &receive_buffer, &len, 1000), c.FIPC_DISCONNECTED, "nothing left: DISCONNECTED");
    try t.check(support.freeAtOnce(name), "the name is free");
    return t.done();
}

test "fast: after the end, a consumer drains every producer's messages, copied or in place, then DISCONNECTED" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    var queue: shared.Queue = .{};
    const name = try shared.openQueue(&t, &queue, &buf, "mq_drain", 4096, 3, 1, producer);
    defer c.fipc_close(queue.own);
    const taker = try joined(&t, name, consumer);
    defer c.fipc_close(taker);
    const second = try joined(&t, name, producer);
    defer c.fipc_close(second);
    for (0..10) |i| {
        try t.checkEqual(sendStamp(queue.own, shared.seqOf(0, i), 48, 0), c.FIPC_OK, "the owner's producer's");
        try t.checkEqual(sendStamp(second, shared.seqOf(1, i), 48, 0), c.FIPC_OK, "a joined producer's");
    }
    c.fipc_queue_close(queue.handle);
    queue.handle = null;
    _ = t.expectEqual(sendStamp(queue.own, shared.seqOf(0, 10), 48, 0), c.FIPC_DISCONNECTED, "the owner's producer: DISCONNECTED at once");
    var ledger = try shared.Ledger.init(2, 10);
    defer ledger.deinit();
    var last: [64]?u64 = @splat(null);
    var result: c.fipc_result_t = c.FIPC_OK;
    var i: usize = 0;
    while (result == c.FIPC_OK) : (i += 1) {
        const seq = recvStamp(taker, i % 2 == 0, 1000, &result);
        if (result == c.FIPC_OK) ledger.record(&last, seq);
    }
    _ = t.expectEqual(result, c.FIPC_DISCONNECTED, "then DISCONNECTED");
    if (!t.expect(ledger.complete(), "every message, once, each producer's in order")) ledger.report();
    var receive_buffer: [64]u8 = undefined;
    _ = t.expectEqual(support.recv(taker, &receive_buffer, &i, 0), c.FIPC_DISCONNECTED, "and from then on");
    return t.done();
}

test "fast: once the queue's end is marked in its segment, the producers' sends are DISCONNECTED and no joiner gets in" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    var queue: shared.Queue = .{};
    const name = try shared.openQueue(&t, &queue, &buf, "mq_marked", 4096, 1, 2, producer);
    defer queue.close();
    const taker = try joined(&t, name, consumer);
    defer c.fipc_close(taker);
    try t.checkEqual(sendStamp(queue.own, 0, 32, 0), c.FIPC_OK, "a message");
    // What a member's watcher stores when its control connection to the owner fails
    shared.layoutOf(queue.own).header.ended.store(1, .seq_cst);
    var count: u32 = 0;
    _ = t.expectEqual(c.fipc_queue_wait_for_members(queue.handle, 0, 3, &count, 1000), c.FIPC_DISCONNECTED, "the owner's count");
    _ = t.expectEqual(sendStamp(queue.own, 1, 32, 0), c.FIPC_DISCONNECTED, "a send");
    var late: support.Conn = null;
    _ = t.expectEqual(c.fipc_queue_join(name, consumer, &late, 300), c.FIPC_TIMEOUT, "a joiner afterwards never gets a place");
    var result: c.fipc_result_t = 0;
    _ = t.expect(recvStamp(taker, false, 1000, &result) == 0, "the consumer drains");
    _ = t.expect(recvStamp(taker, false, 1000, &result) == null, "then");
    _ = t.expectEqual(result, c.FIPC_DISCONNECTED, "DISCONNECTED");
    return t.done();
}

test "fast: a member's own failure (a frame from its owner): its calls are DISCONNECTED at once; the queue goes on; its close frees its place" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    var queue: shared.Queue = .{};
    const name = try shared.openQueue(&t, &queue, &buf, "mq_own_failure", 4096, 2, 1, 0);
    defer queue.close();
    const taker = try joined(&t, name, consumer);
    const giver = try joined(&t, name, producer);
    for (0..3) |i| try t.checkEqual(sendStamp(giver, i, 32, 0), c.FIPC_OK, "a message");
    // The owner sends a frame on every member's connection: each member takes it for its own failure
    @import("fastipc").test_hooks.sendQueueStray(queue.handle.?);
    _ = t.expect(support.waitForEnd(giver, 5000), "the producer's sends are DISCONNECTED");
    var receive_buffer: [64]u8 = undefined;
    var len: usize = 0;
    const begun = support.nowMs();
    var result = support.recv(taker, &receive_buffer, &len, 5000);
    while (result == c.FIPC_OK and support.nowMs() - begun < 5000) result = support.recv(taker, &receive_buffer, &len, 5000);
    _ = t.expectEqual(result, c.FIPC_DISCONNECTED, "the consumer's receive: DISCONNECTED, without draining");
    _ = t.expectEqual(shared.layoutOf(taker).header.ended.load(.seq_cst), 0, "the queue hasn't ended");
    // A new producer joins and sends: the queue goes on
    const next_giver = try joined(&t, name, producer);
    defer c.fipc_close(next_giver);
    try t.checkEqual(sendStamp(next_giver, 3, 32, 0), c.FIPC_OK, "a new producer sends");
    // The failed members' closes free their places: a new consumer gets one, and the messages left
    c.fipc_close(taker);
    c.fipc_close(giver);
    const next = try joined(&t, name, consumer);
    defer c.fipc_close(next);
    var got: usize = 0;
    while (recvStamp(next, false, 200, &result) != null) got += 1;
    _ = t.expect(got >= 1 and got <= 4, "the messages the failed consumer didn't take");
    try t.checkEqual(sendStamp(next_giver, 7, 32, 0), c.FIPC_OK, "the next producer sends");
    _ = t.expect(recvStamp(next, false, 1000, &result) == 7, "received");
    return t.done();
}

test "fast: a member the owner drops for a frame keeps its place out of use while the queue lasts, and still works" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    var queue: shared.Queue = .{};
    const name = try shared.openQueue(&t, &queue, &buf, "mq_retired", 4096, 1, 1, producer);
    defer queue.close();
    const taker = try joined(&t, name, consumer);
    defer c.fipc_close(taker);
    @import("fastipc").test_hooks.sendStray(taker.?);
    // Its place is retired: no consumer joins in its stead
    var other: support.Conn = null;
    _ = t.expectEqual(c.fipc_queue_join(name, consumer, &other, 400), c.FIPC_TIMEOUT, "another consumer finds no place");
    // The dropped consumer may still run: it does
    try t.checkEqual(sendStamp(queue.own, 1, 32, 0), c.FIPC_OK, "a message");
    var result: c.fipc_result_t = 0;
    _ = t.expect(recvStamp(taker, false, 1000, &result) == 1, "the dropped consumer receives it");
    return t.done();
}

test "fast: the timeouts of send, receive and join" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    var queue: shared.Queue = .{};
    const name = try shared.openQueue(&t, &queue, &buf, "mq_timeouts", 4096, 1, 1, producer);
    defer queue.close();
    const taker = try joined(&t, name, consumer);
    defer c.fipc_close(taker);
    var receive_buffer: [64]u8 = undefined;
    var len: usize = 0;
    for ([_]c_int{ 0, 50 }) |timeout_ms| {
        const begun = support.nowMs();
        _ = t.expectEqual(support.recv(taker, &receive_buffer, &len, timeout_ms), c.FIPC_TIMEOUT, "a receive with nothing to take");
        _ = t.expect(support.nowMs() - begun >= @as(u64, @intCast(timeout_ms)) and support.nowMs() - begun < 2000, "after its timeout");
    }
    for (0..64) |i| try t.checkEqual(sendStamp(queue.own, i, 32, 0), c.FIPC_OK, "a send");
    for ([_]c_int{ 0, 50 }) |timeout_ms| {
        const begun = support.nowMs();
        _ = t.expectEqual(sendStamp(queue.own, 64, 32, timeout_ms), c.FIPC_TIMEOUT, "a send without room");
        _ = t.expect(support.nowMs() - begun >= @as(u64, @intCast(timeout_ms)) and support.nowMs() - begun < 2000, "after its timeout");
    }
    var conn: support.Conn = null;
    const begun = support.nowMs();
    _ = t.expectEqual(c.fipc_queue_join(name, consumer, &conn, 100), c.FIPC_TIMEOUT, "a join with every place held");
    _ = t.expect(support.nowMs() - begun >= 100, "after its timeout");
    return t.done();
}
