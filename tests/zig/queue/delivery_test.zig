//! A work queue's delivery through the C API, in one process: producers and consumers on threads (each message to
//! exactly one consumer, each producer's in order), TOO_LARGE, in place, the places of each role (full, freed, a
//! producer's settled), and the cancels.

const std = @import("std");
const support = @import("../support.zig");
const shared = @import("shared.zig");
const c = support.c;

const producer = shared.producer;
const consumer = shared.consumer;
const joined = shared.joined;
const sendStamp = shared.sendStamp;
const recvStamp = shared.recvStamp;

test "fast: producers and consumers on threads: each message to exactly one consumer, each producer's in order" {
    var t = try support.begin(@src(), 60);
    defer t.end();
    var buf: [64]u8 = undefined;
    var queue: shared.Queue = .{};
    // Small rings, so that the producers wait for room too; consumers copy or take in place
    const name = try shared.openQueue(&t, &queue, &buf, "mq_threads", 4096, 3, 4, producer);
    defer queue.close();
    const producers = 3;
    const count = 3000;
    var ledger = try shared.Ledger.init(producers, count);
    defer ledger.deinit();
    var receivers: [4]shared.Receiver = undefined;
    for (&receivers, 0..) |*receiver, i| {
        receiver.* = .{ .conn = try joined(&t, name, consumer), .ledger = &ledger, .in_place = i % 2 == 1 };
        try receiver.start();
    }
    defer for (receivers) |receiver| c.fipc_close(receiver.conn);
    var senders: [producers]shared.Sender = undefined;
    for (&senders, 0..) |*sender, i| {
        const conn = if (i == 0) queue.own else try joined(&t, name, producer);
        sender.* = .{ .conn = conn, .producer_index = i, .count = count, .max_len = 1024 };
        try sender.start();
    }
    defer for (senders[1..]) |sender| c.fipc_close(sender.conn);
    for (&senders) |*sender| _ = t.expectEqual(sender.join(), c.FIPC_OK, "every send");
    // The end: what is left is drained, then DISCONNECTED
    c.fipc_queue_close(queue.handle);
    queue.handle = null;
    for (&receivers) |*receiver| _ = t.expectEqual(receiver.join(), c.FIPC_DISCONNECTED, "a consumer's end");
    if (!t.expect(ledger.complete(), "each message once, whole, each producer's in order")) ledger.report();
    return t.done();
}

test "fast: TOO_LARGE takes nothing: the length of the message found, which stays for any consumer" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    var queue: shared.Queue = .{};
    const name = try shared.openQueue(&t, &queue, &buf, "mq_too_large", 4096, 1, 2, producer);
    defer queue.close();
    const small = try joined(&t, name, consumer);
    defer c.fipc_close(small);
    const large = try joined(&t, name, consumer);
    defer c.fipc_close(large);
    var message: [600]u8 = undefined;
    support.pattern(&message, 3);
    try t.checkEqual(support.send(queue.own, &message, 0), c.FIPC_OK, "a message of 600 bytes");
    var len: usize = 0;
    var receive_buffer: [1024]u8 = undefined;
    _ = t.expectEqual(c.fipc_recv(small, null, 0, &len, 0), c.FIPC_TOO_LARGE, "a NULL buffer asks for the length");
    _ = t.expectEqual(len, 600, "its length");
    _ = t.expectEqual(support.recv(small, receive_buffer[0..100], &len, 1000), c.FIPC_TOO_LARGE, "a buffer too small");
    _ = t.expectEqual(len, 600, "its length");
    // Another consumer takes it
    _ = t.expectEqual(support.recv(large, &receive_buffer, &len, 1000), c.FIPC_OK, "a buffer large enough");
    _ = t.expect(len == 600 and std.mem.eql(u8, receive_buffer[0..len], &message), "the message, whole");
    _ = t.expectEqual(support.recv(small, receive_buffer[0..100], &len, 0), c.FIPC_TIMEOUT, "nothing is left");
    // A buffer of max_piece bytes never looks: it takes every message
    try t.checkEqual(support.send(queue.own, &message, 0), c.FIPC_OK, "again");
    var piece: [1024]u8 = undefined;
    _ = t.expectEqual(support.recv(small, &piece, &len, 1000), c.FIPC_OK, "a buffer of max_piece");
    _ = t.expectEqual(len, 600, "the message");
    return t.done();
}

test "fast: a message held in place until its release, the next receive or the close; its producer waits a ring ahead of it" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    var queue: shared.Queue = .{};
    // 64 cells: a held message holds the producer back once it is 64 messages ahead
    const name = try shared.openQueue(&t, &queue, &buf, "mq_in_place", 4096, 1, 2, producer);
    defer queue.close();
    const holder = try joined(&t, name, consumer);
    const other = try joined(&t, name, consumer);
    defer c.fipc_close(other);
    try t.checkEqual(sendStamp(queue.own, 0, 32, 0), c.FIPC_OK, "message 0");
    var result: c.fipc_result_t = 0;
    var at: ?*const anyopaque = null;
    var len: usize = 0;
    try t.checkEqual(c.fipc_recv_acquire(holder, &at, &len, 1000), c.FIPC_OK, "held in place");
    _ = t.expect(support.Stamp.check(@as([*]const u8, @ptrCast(at.?))[0..len]) == 0, "message 0");
    // The other consumer takes the next 63 as they come; the producer then waits for the held one
    for (1..64) |i| {
        try t.checkEqual(sendStamp(queue.own, i, 32, 0), c.FIPC_OK, "a send");
        _ = t.expect(recvStamp(other, false, 1000, &result) == i, "taken by the other consumer");
    }
    _ = t.expectEqual(sendStamp(queue.own, 64, 32, 0), c.FIPC_TIMEOUT, "a ring ahead of the held message");
    _ = t.expectEqual(sendStamp(queue.own, 64, 32, 50), c.FIPC_TIMEOUT, "within its timeout too");
    c.fipc_recv_release(holder);
    _ = t.expectEqual(sendStamp(queue.own, 64, 32, 0), c.FIPC_OK, "free once released");
    // The next receive releases a held message; so does the close
    try t.checkEqual(c.fipc_recv_acquire(holder, &at, &len, 1000), c.FIPC_OK, "message 64, held");
    for (65..128) |i| try t.checkEqual(sendStamp(queue.own, i, 32, 0), c.FIPC_OK, "a send");
    _ = t.expectEqual(sendStamp(queue.own, 128, 32, 0), c.FIPC_TIMEOUT, "held back");
    _ = t.expect(recvStamp(holder, false, 1000, &result) == 65, "the next receive");
    _ = t.expectEqual(sendStamp(queue.own, 128, 32, 0), c.FIPC_OK, "the held one is released");
    try t.checkEqual(c.fipc_recv_acquire(holder, &at, &len, 1000), c.FIPC_OK, "message 66, held");
    for (67..129) |i| _ = t.expect(recvStamp(other, false, 1000, &result) == i, "the rest");
    for (129..130) |i| try t.checkEqual(sendStamp(queue.own, i, 32, 0), c.FIPC_OK, "a send");
    _ = t.expectEqual(sendStamp(queue.own, 130, 32, 0), c.FIPC_TIMEOUT, "held back");
    c.fipc_close(holder);
    _ = t.expectEqual(sendStamp(queue.own, 130, 32, 0), c.FIPC_OK, "the close released it");
    return t.done();
}

test "fast: a role full: its joiner waits, and joins once a place is free; the other role still joins" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    var queue: shared.Queue = .{};
    const name = try shared.openQueue(&t, &queue, &buf, "mq_full", 4096, 1, 1, 0);
    defer queue.close();
    const first = try joined(&t, name, consumer);
    var none: support.Conn = null;
    _ = t.expectEqual(c.fipc_queue_join(name, consumer, &none, 300), c.FIPC_TIMEOUT, "no place: a second consumer waits");
    var joiner: support.Joiner = .{};
    try t.check(joiner.start(name, consumer, 5000), "a second consumer joins");
    // The producer's place is free: a producer gets in meanwhile
    const giver = try joined(&t, name, producer);
    defer c.fipc_close(giver);
    c.fipc_close(first);
    var second: support.Conn = null;
    try t.checkEqual(joiner.join(&second), c.FIPC_OK, "the first consumer's close frees its place: the second joins");
    defer c.fipc_close(second);
    try t.checkEqual(sendStamp(giver, 9, 32, 0), c.FIPC_OK, "a send");
    var result: c.fipc_result_t = 0;
    _ = t.expect(recvStamp(second, false, 1000, &result) == 9, "received by the second consumer");
    return t.done();
}

test "fast: a producer's place is free again only once its ring settled: a consumer holding its last message delays it" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    var queue: shared.Queue = .{};
    const name = try shared.openQueue(&t, &queue, &buf, "mq_settle", 4096, 1, 1, consumer);
    defer queue.close();
    const first = try joined(&t, name, producer);
    for (0..2) |i| try t.checkEqual(sendStamp(first, i, 32, 0), c.FIPC_OK, "a send");
    c.fipc_close(first);
    var result: c.fipc_result_t = 0;
    _ = t.expect(recvStamp(queue.own, false, 1000, &result) == 0, "the first");
    var at: ?*const anyopaque = null;
    var len: usize = 0;
    try t.checkEqual(c.fipc_recv_acquire(queue.own, &at, &len, 1000), c.FIPC_OK, "the last, held in place");
    // The place drains: its producer's end is seen (the poller), but the held message keeps it busy
    var none: support.Conn = null;
    _ = t.expectEqual(c.fipc_queue_join(name, producer, &none, 400), c.FIPC_TIMEOUT, "the next producer waits: its place is busy");
    c.fipc_recv_release(queue.own);
    const next = try joined(&t, name, producer);
    defer c.fipc_close(next);
    // It starts where the ring is: its messages follow
    try t.checkEqual(sendStamp(next, 5, 32, 0), c.FIPC_OK, "the next producer's send");
    _ = t.expect(recvStamp(queue.own, false, 1000, &result) == 5, "received");
    return t.done();
}

fn recvWaiting(conn: support.Conn, result: *c.fipc_result_t) void {
    var buf: [64]u8 = undefined;
    var len: usize = 0;
    result.* = support.recv(conn, &buf, &len, c.FIPC_FOREVER);
}

fn sendWaiting(conn: support.Conn, result: *c.fipc_result_t) void {
    result.* = sendStamp(conn, 99, 32, c.FIPC_FOREVER);
}

test "fast: cancel ends a consumer's waiting receive and a producer's waiting send; calls that needn't wait still work" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    var queue: shared.Queue = .{};
    const name = try shared.openQueue(&t, &queue, &buf, "mq_cancel", 4096, 2, 2, consumer);
    defer queue.close();
    const giver = try joined(&t, name, producer);
    defer c.fipc_close(giver);
    const taker = try joined(&t, name, consumer);
    defer c.fipc_close(taker);
    // Consumers parked, the own and a joined one, each cancelled
    for ([_]support.Conn{ queue.own, taker }) |conn| {
        var result: c.fipc_result_t = 0;
        const since = shared.sleepsOf(conn);
        const thread = try std.Thread.spawn(.{}, recvWaiting, .{ conn, &result });
        shared.awaitAsleep(conn, since);
        c.fipc_cancel(conn);
        thread.join();
        _ = t.expectEqual(result, c.FIPC_CANCELLED, "the parked receive");
    }
    // A producer waiting for room: the cancelled consumers take nothing
    for (0..64) |i| try t.checkEqual(sendStamp(giver, i, 32, 0), c.FIPC_OK, "a send");
    var result: c.fipc_result_t = 0;
    const since = shared.sleepsOf(giver);
    const thread = try std.Thread.spawn(.{}, sendWaiting, .{ giver, &result });
    shared.awaitAsleep(giver, since);
    c.fipc_cancel(giver);
    thread.join();
    _ = t.expectEqual(result, c.FIPC_CANCELLED, "the waiting send");
    _ = t.expectEqual(sendStamp(giver, 64, 32, 0), c.FIPC_CANCELLED, "a send that would have to wait, even for 0 ms");
    var receive_buffer: [64]u8 = undefined;
    var len: usize = 0;
    _ = t.expectEqual(support.recv(taker, &receive_buffer, &len, 0), c.FIPC_OK, "a receive that needn't wait works");
    _ = t.expectEqual(sendStamp(giver, 64, 32, 0), c.FIPC_OK, "a send that needn't wait works");
    _ = t.expectEqual(sendStamp(giver, 65, 32, 1000), c.FIPC_CANCELLED, "one that would wait is CANCELLED");
    return t.done();
}
