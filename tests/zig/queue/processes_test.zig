//! A work queue's members in other processes (peer.zig), in the slow tier: producer processes and consumers on
//! threads, a producer or a consumer process killed (or crashed in its data path), and what goes again after a
//! consumer's end, by when it acknowledged; the owner's process killed, and a crash at each step of a join on either
//! side.

const std = @import("std");
const support = @import("../support.zig");
const shared = @import("shared.zig");
const c = support.c;

const producer = shared.producer;
const consumer = shared.consumer;
const joined = shared.joined;
const sendStamp = shared.sendStamp;
const recvStamp = shared.recvStamp;

/// Waits until `count` members of `role` are joined (processes that join), each within 10 s.
fn awaitMembers(t: *support.Test, queue: *shared.Queue, role: c_int, count: u32) !void {
    var joined_count: u32 = 0;
    const result = c.fipc_queue_wait_for_members(queue.handle, role, count, &joined_count, @intCast(10_000 * count));
    try t.checkEqual(result, c.FIPC_OK, "the member processes join");
}

test "slow: producer processes and consumers on threads: each message to exactly one consumer, each producer's in order" {
    var t = try support.begin(@src(), 120);
    defer t.end();
    var buf: [64]u8 = undefined;
    var queue: shared.Queue = .{};
    const name = try shared.openQueue(&t, &queue, &buf, "mq_processes", 4096, 3, 3, consumer);
    defer queue.close();
    const count = 5000;
    var ledger = try shared.Ledger.init(3, count);
    defer ledger.deinit();
    var receivers: [3]shared.Receiver = undefined;
    for (&receivers, 0..) |*receiver, i| {
        const conn = if (i == 0) queue.own else try joined(&t, name, consumer);
        receiver.* = .{ .conn = conn, .ledger = &ledger, .in_place = i == 1, .timeout_ms = 3000 };
    }
    defer for (receivers[1..]) |receiver| c.fipc_close(receiver.conn);
    var peers: [3]*support.Peer = undefined;
    var spawned: usize = 0;
    defer for (peers[0..spawned]) |peer| peer.deinit();
    for (&peers, 0..) |*peer, i| {
        var index_buf: [4]u8 = undefined;
        peer.* = try support.Peer.spawn(&.{ "queue-produce", name, try std.fmt.bufPrint(&index_buf, "{d}", .{i}), "5000" });
        spawned += 1;
    }
    try awaitMembers(&t, &queue, producer, 3);
    for (&receivers) |*receiver| try receiver.start();
    for (peers) |peer| _ = t.expect(peer.waitFor("SENT", 60_000), "a producer sent its stream");
    // The consumers end once nothing more comes for 3 s
    for (&receivers) |*receiver| _ = t.expectEqual(receiver.join(), c.FIPC_TIMEOUT, "a consumer's end");
    if (!t.expect(ledger.complete(), "each message once, whole, each producer's in order")) ledger.report();
    for (peers) |peer| peer.send("x");
    for (peers) |peer| _ = t.expect(peer.waitTimeout(10_000) and peer.exitedWith(0), "a producer closed");
    return t.done();
}

test "slow: a producer process killed: the consumers drain what it sent; the next producer gets its place once it settled" {
    var t = try support.begin(@src(), 60);
    defer t.end();
    var buf: [64]u8 = undefined;
    var queue: shared.Queue = .{};
    const name = try shared.openQueue(&t, &queue, &buf, "mq_kill_producer", 65536, 1, 1, consumer);
    defer queue.close();
    const giver = try support.Peer.spawn(&.{ "queue-produce", name, "0", "50" });
    defer giver.deinit();
    try awaitMembers(&t, &queue, producer, 1);
    try t.check(giver.waitFor("SENT", 10_000), "the producer sent");
    giver.kill();
    // Its place drains: no producer joins while its messages wait
    var none: support.Conn = null;
    _ = t.expectEqual(c.fipc_queue_join(name, producer, &none, 400), c.FIPC_TIMEOUT, "the next producer waits: its place is busy");
    var ledger = try shared.Ledger.init(1, 50);
    defer ledger.deinit();
    var last: [64]?u64 = @splat(null);
    for (0..50) |_| {
        var result: c.fipc_result_t = 0;
        const seq = recvStamp(queue.own, false, 5000, &result);
        if (!t.expectEqual(result, c.FIPC_OK, "a message the killed producer sent")) break;
        ledger.record(&last, seq);
    }
    if (!t.expect(ledger.complete(), "all of them, in order")) ledger.report();
    const next = try joined(&t, name, producer);
    defer c.fipc_close(next);
    try t.checkEqual(sendStamp(next, 99, 32, 0), c.FIPC_OK, "the next producer sends");
    var result: c.fipc_result_t = 0;
    _ = t.expect(recvStamp(queue.own, false, 5000, &result) == 99, "received");
    return t.done();
}

test "slow: a consumer process killed between fipc_recv_acquire and fipc_recv_release: its message goes again, once; its producer goes on; the next consumer gets its place" {
    var t = try support.beginTimed(@src(), 60);
    defer t.end();
    var buf: [64]u8 = undefined;
    var queue: shared.Queue = .{};
    const name = try shared.openQueue(&t, &queue, &buf, "mq_kill_consumer", 4096, 1, 1, producer);
    defer queue.close();
    const holder = try support.Peer.spawn(&.{ "queue-hold", name, "consumer-inplace" });
    defer holder.deinit();
    try awaitMembers(&t, &queue, consumer, 1);
    try t.checkEqual(sendStamp(queue.own, 0, 32, 0), c.FIPC_OK, "message 0");
    try t.check(holder.waitFor("R", 10_000), "the consumer holds it in place");
    for (1..64) |i| try t.checkEqual(sendStamp(queue.own, i, 32, 0), c.FIPC_OK, "a send");
    _ = t.expectEqual(sendStamp(queue.own, 64, 32, 0), c.FIPC_TIMEOUT, "the held message holds the producer back");
    const Sender = struct {
        fn run(conn: support.Conn, result: *c.fipc_result_t) void {
            result.* = sendStamp(conn, 64, 32, 5000);
        }
    };
    var sent: c.fipc_result_t = 0;
    const thread = try std.Thread.spawn(.{}, Sender.run, .{ queue.own, &sent });
    support.sleepMs(100);
    const killed = support.nowMs();
    holder.kill();
    thread.join();
    _ = t.expectEqual(sent, c.FIPC_OK, "the waiting send goes through: the held message was copied for another consumer");
    _ = t.expect(support.nowMs() - killed < 1000, "within a second of the kill");
    const next = try joined(&t, name, consumer);
    defer c.fipc_close(next);
    // Each once: the held one again, and the rest, these in their order
    var result: c.fipc_result_t = 0;
    var held_again: u32 = 0;
    var expected: u64 = 1;
    for (0..65) |_| {
        const seq = recvStamp(next, false, 5000, &result) orelse break;
        if (seq == 0) {
            held_again += 1;
        } else {
            _ = t.expectEqual(seq, expected, "the next consumer: the rest, in order");
            expected = seq + 1;
        }
    }
    _ = t.expectEqual(held_again, 1, "the held message goes again, once");
    _ = t.expectEqual(expected, 65, "and every other message");
    _ = t.expect(recvStamp(next, false, 200, &result) == null and result == c.FIPC_TIMEOUT, "nothing more");
    return t.done();
}

test "slow: a consumer process killed after it released its message, before its work: nothing goes again" {
    var t = try support.begin(@src(), 60);
    defer t.end();
    var buf: [64]u8 = undefined;
    var queue: shared.Queue = .{};
    const name = try shared.openQueue(&t, &queue, &buf, "mq_kill_released", 4096, 1, 1, producer);
    defer queue.close();
    const worker = try support.Peer.spawn(&.{ "queue-hold", name, "consumer-released" });
    defer worker.deinit();
    try awaitMembers(&t, &queue, consumer, 1);
    try t.checkEqual(sendStamp(queue.own, 0, 32, 0), c.FIPC_OK, "message 0");
    try t.check(worker.waitFor("R", 10_000), "the consumer took it and released it");
    worker.kill();
    const next = try joined(&t, name, consumer);
    defer c.fipc_close(next);
    var result: c.fipc_result_t = 0;
    _ = t.expect(recvStamp(next, false, 300, &result) == null and result == c.FIPC_TIMEOUT, "the message doesn't go again");
    try t.checkEqual(sendStamp(queue.own, 1, 32, 0), c.FIPC_OK, "message 1");
    _ = t.expect(recvStamp(next, false, 5000, &result) == 1, "the next consumer's");
    return t.done();
}

test "slow: a consumer process killed holding a message that went again: it goes again, once more" {
    var t = try support.begin(@src(), 60);
    defer t.end();
    var buf: [64]u8 = undefined;
    var queue: shared.Queue = .{};
    const name = try shared.openQueue(&t, &queue, &buf, "mq_kill_twice", 4096, 1, 1, producer);
    defer queue.close();
    const first = try support.Peer.spawn(&.{ "queue-hold", name, "consumer-inplace" });
    defer first.deinit();
    try awaitMembers(&t, &queue, consumer, 1);
    try t.checkEqual(sendStamp(queue.own, 0, 32, 0), c.FIPC_OK, "message 0");
    try t.check(first.waitFor("R", 10_000), "the first consumer holds it");
    first.kill();
    // The place is free once the message went again: the second consumer takes the copy, the only message
    const second = try support.Peer.spawn(&.{ "queue-hold", name, "consumer-inplace" });
    defer second.deinit();
    try t.check(second.waitFor("R", 10_000), "the second consumer holds the message that went again");
    second.kill();
    const next = try joined(&t, name, consumer);
    defer c.fipc_close(next);
    var result: c.fipc_result_t = 0;
    _ = t.expect(recvStamp(next, false, 5000, &result) == 0, "it goes again");
    _ = t.expect(recvStamp(next, false, 200, &result) == null and result == c.FIPC_TIMEOUT, "once");
    // The queue goes on: a ring's worth more
    for (1..129) |i| {
        try t.checkEqual(sendStamp(queue.own, i, 32, 5000), c.FIPC_OK, "a send");
        _ = t.expect(recvStamp(next, true, 5000, &result) == i, "the next consumer's, in order");
    }
    return t.done();
}

/// A copying consumer that crashes inside fipc_recv at `step`, holding message 0, which the application never got:
/// the next consumer receives it, once (no duplicate), then a ring's worth more in order.
fn crashInsideRecv(t: *support.Test, comptime prefix: []const u8, step: []const u8) !void {
    var buf: [64]u8 = undefined;
    var queue: shared.Queue = .{};
    const name = try shared.openQueue(t, &queue, &buf, prefix, 4096, 1, 1, producer);
    defer queue.close();
    const crashing = try support.Peer.spawn(&.{ "native-queue-crash", "consumer", name, step });
    defer crashing.deinit();
    try t.check(crashing.waitFor("READY", 10_000), "the consumer joined");
    try t.checkEqual(sendStamp(queue.own, 0, 32, 0), c.FIPC_OK, "message 0");
    try t.check(crashing.waitTimeout(10_000) and crashing.exitedWith(99), "the consumer crashed holding it");
    // Its place is free once the message went again: the next consumer receives it, the crashed one never returned it
    const next = try joined(t, name, consumer);
    defer c.fipc_close(next);
    var result: c.fipc_result_t = 0;
    _ = t.expect(recvStamp(next, false, 5000, &result) == 0, "the message the crashed consumer took goes to the next");
    // A ring's worth more: the message holds nothing back once it went again, and comes no second time
    for (1..129) |i| {
        try t.checkEqual(sendStamp(queue.own, i, 32, 5000), c.FIPC_OK, "a send");
        _ = t.expect(recvStamp(next, false, 5000, &result) == i, "the next consumer's, in order");
    }
}

test "slow: a consumer crashed inside fipc_recv between its claim and its move of head: its message goes to another consumer, once" {
    var t = try support.begin(@src(), 60);
    defer t.end();
    try crashInsideRecv(&t, "mq_crash_claim", "queue_claimed");
    return t.done();
}

test "slow: a consumer crashed inside fipc_recv between its copy and its done: its message goes to another consumer, once" {
    var t = try support.begin(@src(), 60);
    defer t.end();
    try crashInsideRecv(&t, "mq_crash_copied", "queue_copied");
    return t.done();
}

test "slow: a consumer killed holding a message, then the queue's close: a live consumer receives the message, then DISCONNECTED" {
    var t = try support.begin(@src(), 60);
    defer t.end();
    var buf: [64]u8 = undefined;
    var queue: shared.Queue = .{};
    const name = try shared.openQueue(&t, &queue, &buf, "mq_kill_close", 4096, 1, 2, producer);
    defer queue.close(); // a second close does nothing
    const holder = try support.Peer.spawn(&.{ "queue-hold", name, "consumer-inplace" });
    defer holder.deinit();
    try awaitMembers(&t, &queue, consumer, 1);
    try t.checkEqual(sendStamp(queue.own, 0, 32, 0), c.FIPC_OK, "message 0");
    try t.check(holder.waitFor("R", 10_000), "the consumer holds it in place");
    // The live consumer joins once the message is held: nothing else is in the queue for it
    const live = try joined(&t, name, consumer);
    defer c.fipc_close(live);
    holder.kill();
    // The owner saw the end once its count drops to the live consumer; the message went again at that end
    var count: u32 = 2;
    const since = support.nowMs();
    while (count != 1 and support.nowMs() - since < 10_000) {
        _ = c.fipc_queue_wait_for_members(queue.handle, consumer, 0, &count, 0);
        support.sleepMs(1);
    }
    try t.checkEqual(count, 1, "the owner saw the killed consumer's end");
    queue.close();
    var result: c.fipc_result_t = 0;
    _ = t.expect(recvStamp(live, false, 5000, &result) == 0, "the live consumer receives the message after the close");
    _ = t.expect(recvStamp(live, false, 5000, &result) == null and result == c.FIPC_DISCONNECTED, "then DISCONNECTED");
    return t.done();
}

test "slow: a producer crashed between its tail and its ready bit: its message is still received" {
    var t = try support.begin(@src(), 60);
    defer t.end();
    var buf: [64]u8 = undefined;
    var queue: shared.Queue = .{};
    const name = try shared.openQueue(&t, &queue, &buf, "mq_crash_publish", 4096, 1, 1, consumer);
    defer queue.close();
    const crashing = try support.Peer.spawn(&.{ "native-queue-crash", "producer", name, "queue_published" });
    defer crashing.deinit();
    try t.check(crashing.waitTimeout(10_000) and crashing.exitedWith(99), "the producer crashed after publishing");
    var receive_buffer: [64]u8 = undefined;
    var len: usize = 0;
    _ = t.expectEqual(support.recv(queue.own, &receive_buffer, &len, 5000), c.FIPC_OK, "the message");
    _ = t.expect(std.mem.eql(u8, receive_buffer[0..len], "crash"), "whole");
    return t.done();
}

test "slow: the owner's process killed: producers DISCONNECTED, consumers drain what is left, then DISCONNECTED" {
    var t = try support.begin(@src(), 60);
    defer t.end();
    var buf: [64]u8 = undefined;
    const name = support.uniqueName(&buf, "mq_kill_owner");
    defer support.clearName(name);
    const owner = try support.Peer.spawn(&.{ "queue-own", name, "100" });
    defer owner.deinit();
    try t.check(owner.waitFor("SENT", 10_000), "the owner sent");
    var taker: support.Conn = null;
    try t.checkEqual(c.fipc_queue_join(name, consumer, &taker, 10_000), c.FIPC_OK, "a consumer joins");
    defer c.fipc_close(taker);
    var giver: support.Conn = null;
    try t.checkEqual(c.fipc_queue_join(name, producer, &giver, 10_000), c.FIPC_OK, "a producer joins");
    defer c.fipc_close(giver);
    var result: c.fipc_result_t = 0;
    _ = t.expect(recvStamp(taker, false, 5000, &result) == 0, "the first message");
    owner.kill();
    _ = t.expect(support.waitForEnd(giver, 5000), "the producer sees the end");
    var ledger = try shared.Ledger.init(1, 100);
    defer ledger.deinit();
    var last: [64]?u64 = @splat(null);
    ledger.record(&last, 0);
    while (true) {
        const seq = recvStamp(taker, false, 5000, &result);
        if (result != c.FIPC_OK) break;
        ledger.record(&last, seq);
    }
    _ = t.expectEqual(result, c.FIPC_DISCONNECTED, "then DISCONNECTED");
    if (!t.expect(ledger.complete(), "every message the owner sent, in order")) ledger.report();
    return t.done();
}

test "slow: an owner crashed at each step of an admission (on its poller): the joiner fails or reads the end, and the name is free" {
    var t = try support.begin(@src(), 120);
    defer t.end();
    for ([_][]const u8{ "accepted", "offer_sent", "join_read", "welcome_sent", "member_watching" }) |step| {
        var name_buf: [64]u8 = undefined;
        const name = try std.mem.printSentinel(&name_buf, "mq-crash-own-{s}-{d}", .{ step, support.pid() }, 0);
        defer support.clearName(name);
        const crashing = try support.Peer.spawn(&.{ "native-queue-crash", "own", name, step });
        defer crashing.deinit();
        try t.check(crashing.waitFor("OPEN", 10_000), "the crashing owner listens");
        var member: support.Conn = null;
        const result = c.fipc_queue_join(name, consumer, &member, 500);
        std.debug.print("  crash at {s}: the join returned {s}\n", .{ step, support.resultName(result) });
        if (result == c.FIPC_OK) {
            var receive_buffer: [16]u8 = undefined;
            var len: usize = 0;
            _ = t.expectEqual(support.recv(member, &receive_buffer, &len, 5000), c.FIPC_DISCONNECTED, "the consumer reads the queue's end");
            c.fipc_close(member);
        } else _ = t.expectEqual(result, c.FIPC_TIMEOUT, "or the join times out");
        try t.check(crashing.waitTimeout(10_000) and crashing.exitedWith(99), "the owner ended at its crash point");
        var queue: shared.Queue = .{};
        try t.check(queue.open(name, 4096, 1, 1, producer), "the next queue on the name");
        defer queue.close();
        const next = try joined(&t, name, consumer);
        c.fipc_close(next);
    }
    return t.done();
}

test "slow: a member crashed at each step of its join: its place is freed, the next gets it" {
    var t = try support.begin(@src(), 180);
    defer t.end();
    var buf: [64]u8 = undefined;
    var queue: shared.Queue = .{};
    // One producer place and, besides the owner's consumer's, one consumer place
    const name = try shared.openQueue(&t, &queue, &buf, "mq_crash_member", 4096, 1, 2, consumer);
    defer queue.close();
    for ([_][]const u8{ "producer", "consumer" }) |role| {
        for ([_][]const u8{ "connected", "offer_received", "join_sent", "welcomed", "start_taken", "watching" }) |step| {
            const crashing = try support.Peer.spawn(&.{ "native-queue-crash", role, name, step });
            defer crashing.deinit();
            try t.check(crashing.waitTimeout(10_000) and crashing.exitedWith(99), "the member ended at its crash point");
            // A producer that crashed only in its watcher may have sent its message: taken, its place settles
            var receive_buffer: [64]u8 = undefined;
            var len: usize = 0;
            while (support.recv(queue.own, &receive_buffer, &len, 0) == c.FIPC_OK) {}
            // One place of each role: the next gets in only once the crashed one's is free
            const next = try joined(&t, name, if (std.mem.eql(u8, role, "producer")) producer else consumer);
            c.fipc_close(next);
        }
    }
    return t.done();
}
