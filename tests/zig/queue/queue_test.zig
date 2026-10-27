//! A work queue through the C API, in one process: `fipc_queue_create` and its arguments, the owner's members (joined
//! on its own thread: its process admits them by itself), `fipc_queue_join` on the test's thread or on threads of
//! their own (`support.Joiner`), each role's calls, the kinds a joiner refuses, the owner's count of its members and
//! its cancel, and nothing left after a queue's close.

const std = @import("std");
const fastipc = @import("fastipc");
const support = @import("../support.zig");
const shared = @import("shared.zig");
const c = support.c;

const producer = shared.producer;
const consumer = shared.consumer;
const joined = shared.joined;
const sendStamp = shared.sendStamp;
const recvStamp = shared.recvStamp;

test "fast: queue_create checks its arguments, and its name is held against any kind" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    const name = support.uniqueName(&buf, "mq_args");
    var queue: support.Queue = null;
    for ([_]usize{ 0, 2048, 4097, 1 << 32, 1 << 33 }) |capacity| {
        _ = t.expectEqual(c.fipc_queue_create(name, capacity, 1, 1, &queue), c.FIPC_INVALID, "a capacity out of range");
    }
    for ([_]u32{ 0, 64, 65 }) |max| {
        _ = t.expectEqual(c.fipc_queue_create(name, 4096, max, 1, &queue), c.FIPC_INVALID, "max_producers out of range (1 to 63)");
    }
    for ([_]u32{ 0, 65 }) |max| {
        _ = t.expectEqual(c.fipc_queue_create(name, 4096, 1, max, &queue), c.FIPC_INVALID, "max_consumers out of range (1 to 64)");
    }
    _ = t.expectEqual(c.fipc_queue_create(null, 4096, 1, 1, &queue), c.FIPC_INVALID, "a NULL name");
    _ = t.expectEqual(c.fipc_queue_create(name, 4096, 1, 1, null), c.FIPC_INVALID, "a NULL out-pointer");
    _ = t.expectEqual(c.fipc_queue_create("has space", 4096, 1, 1, &queue), c.FIPC_INVALID, "a name that isn't valid");
    _ = t.expect(queue == null, "nothing was created");
    try t.check(support.freeAtOnce(name), "the name is free");

    try t.checkEqual(c.fipc_queue_create(name, 4096, 63, 64, &queue), c.FIPC_OK, "a queue of 63 producers and 64 consumers");
    var other: support.Listener = null;
    var publisher: support.Conn = null;
    var other_queue: support.Queue = null;
    _ = t.expectEqual(c.fipc_stream_create(name, 4096, &other), c.FIPC_ADDR_IN_USE, "a stream on a queue's name");
    _ = t.expectEqual(c.fipc_broadcast_create(name, 4096, 1, 0, &publisher), c.FIPC_ADDR_IN_USE, "a broadcast on a queue's name");
    _ = t.expectEqual(c.fipc_queue_create(name, 4096, 1, 1, &other_queue), c.FIPC_ADDR_IN_USE, "a second queue on the name");
    c.fipc_queue_close(queue);
    queue = null;
    try t.check(support.freeAtOnce(name), "the name is free again after the close");
    var broadcast: support.Broadcast = .{};
    try t.check(broadcast.open(name, 4096, 1, 0), "a broadcast takes the name");
    _ = t.expectEqual(c.fipc_queue_create(name, 4096, 1, 1, &other_queue), c.FIPC_ADDR_IN_USE, "a queue on a broadcast's name");
    broadcast.close();
    return t.done();
}

test "fast: joiners of another kind are INVALID at once" {
    var t = try support.begin(@src(), 60);
    defer t.end();
    var buf: [64]u8 = undefined;
    var queue: shared.Queue = .{};
    const name = try shared.openQueue(&t, &queue, &buf, "mq_kinds", 4096, 1, 1, 0);
    defer queue.close();
    var conn: support.Conn = null;

    // A subscriber, and a client of fipc_stream_connect, of the queue: refused at once (the queue goes on admitting)
    for ([_]c_int{ c.FIPC_SUBSCRIBER_LOSSY, c.FIPC_SUBSCRIBER_GATED }) |role| {
        const begun = support.nowMs();
        _ = t.expectEqual(c.fipc_broadcast_join(name, role, &conn, 5000), c.FIPC_INVALID, "a subscriber of a queue");
        _ = t.expect(support.nowMs() - begun < 4000, "at once");
    }
    var connector: support.Connector = .{};
    try t.check(connector.start(name, 5000), "a client starts");
    _ = t.expectEqual(connector.join(null), c.FIPC_INVALID, "a client of fipc_stream_connect to a queue");

    // A producer or consumer of a broadcast, or of a stream: refused at once
    var other_buf: [64]u8 = undefined;
    var broadcast: support.Broadcast = .{};
    const broadcast_name = try support.openBroadcast(&t, &broadcast, &other_buf, "mq_kinds_bc", 4096, 1, c.FIPC_FOREVER);
    defer broadcast.close();
    for ([_]c_int{ producer, consumer }) |role| {
        const begun = support.nowMs();
        _ = t.expectEqual(c.fipc_queue_join(broadcast_name, role, &conn, 5000), c.FIPC_INVALID, "a queue's role on a broadcast");
        _ = t.expect(support.nowMs() - begun < 4000, "at once");
    }
    var server_buf: [64]u8 = undefined;
    const server_name = support.uniqueName(&server_buf, "mq_kinds_server");
    var server: support.Listener = null;
    try t.checkEqual(c.fipc_stream_create(server_name, 4096, &server), c.FIPC_OK, "a server listens");
    defer c.fipc_listener_close(server);
    var joiner: support.Joiner = .{};
    try t.check(joiner.start(server_name, consumer, 5000), "a joiner starts");
    _ = t.expectEqual(c.fipc_stream_accept(server, &conn, 1000), c.FIPC_TIMEOUT, "the server's accept drops it");
    _ = t.expectEqual(joiner.join(null), c.FIPC_INVALID, "a consumer of a stream");

    // Nobody on the name: the join waits out its timeout
    var nobody_buf: [64]u8 = undefined;
    const begun = support.nowMs();
    _ = t.expectEqual(c.fipc_queue_join(support.uniqueName(&nobody_buf, "mq_nobody"), producer, &conn, 200), c.FIPC_TIMEOUT, "no owner");
    _ = t.expect(support.nowMs() - begun >= 190, "after its timeout");

    // The queue still admits its own kind
    const member = try joined(&t, name, consumer);
    c.fipc_close(member);
    return t.done();
}

test "fast: each role's calls: what it can't use is INVALID; max_piece is a quarter of the capacity, TOO_LARGE above" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    var queue: shared.Queue = .{};
    const name = try shared.openQueue(&t, &queue, &buf, "mq_roles", 16384, 2, 2, producer);
    defer queue.close();
    const own = queue.own;
    const taker = try joined(&t, name, consumer);
    defer c.fipc_close(taker);
    const giver = try joined(&t, name, producer);
    defer c.fipc_close(giver);

    for ([_]support.Conn{ own, taker, giver }) |member| _ = t.expectEqual(c.fipc_max_piece(member), 4096, "max_piece: a quarter of the capacity");
    var big: [4097]u8 = @splat(7);
    var out: ?*anyopaque = null;
    for ([_]support.Conn{ own, giver }) |sender| {
        _ = t.expectEqual(support.send(sender, &big, 0), c.FIPC_TOO_LARGE, "a send over one piece");
        _ = t.expectEqual(c.fipc_send(sender, &big, 0, 0), c.FIPC_INVALID, "an empty send");
        _ = t.expectEqual(c.fipc_send_acquire(sender, 4097, &out, 0), c.FIPC_TOO_LARGE, "a reservation over one piece");
        _ = t.expectEqual(c.fipc_send_commit(sender, 1), c.FIPC_INVALID, "a commit without a reservation");
    }
    var len: usize = 0;
    var in: ?*const anyopaque = null;
    var count: u32 = 0;
    var receive_buffer: [8192]u8 = undefined;
    for ([_]support.Conn{ own, giver }) |sender| {
        _ = t.expectEqual(support.recv(sender, &receive_buffer, &len, 0), c.FIPC_INVALID, "recv on a producer");
        _ = t.expectEqual(c.fipc_recv_acquire(sender, &in, &len, 0), c.FIPC_INVALID, "recv_acquire on a producer");
        _ = t.expectEqual(c.fipc_broadcast_wait_for_members(sender, 0, &count, 0), c.FIPC_INVALID, "broadcast_wait_for_members on a producer");
        c.fipc_recv_release(sender); // nothing
    }
    _ = t.expectEqual(support.send(taker, "x", 0), c.FIPC_INVALID, "send on a consumer");
    _ = t.expectEqual(c.fipc_send_acquire(taker, 1, &out, 0), c.FIPC_INVALID, "send_acquire on a consumer");
    _ = t.expectEqual(c.fipc_send_commit(taker, 1), c.FIPC_INVALID, "send_commit on a consumer");
    _ = t.expectEqual(c.fipc_broadcast_wait_for_members(taker, 0, &count, 0), c.FIPC_INVALID, "broadcast_wait_for_members on a consumer");
    _ = t.expectEqual(support.recv(taker, &receive_buffer, &len, 0), c.FIPC_TIMEOUT, "nothing to receive");
    c.fipc_recv_release(taker); // nothing held

    // A message of one piece from each producer, and a zero-copy one, each received once, in place or copied
    _ = t.expectEqual(support.send(own, big[0..4096], 0), c.FIPC_OK, "a send of one piece");
    _ = t.expectEqual(c.fipc_send_acquire(giver, 100, &out, 0), c.FIPC_OK, "a reservation");
    _ = t.expect(@intFromPtr(out.?) % 64 == 0, "64-byte aligned");
    @memcpy(@as([*]u8, @ptrCast(out.?))[0..5], "zc-ok");
    _ = t.expectEqual(c.fipc_send_commit(giver, 101), c.FIPC_INVALID, "a commit past the reservation");
    _ = t.expectEqual(c.fipc_send_commit(giver, 5), c.FIPC_OK, "the commit of 5 of its 100 bytes");
    _ = t.expectEqual(c.fipc_send_commit(giver, 5), c.FIPC_INVALID, "a second commit");
    var got_big = false;
    var got_zero_copy = false;
    for (0..2) |i| {
        var result: c.fipc_result_t = c.FIPC_OK;
        if (i == 0) {
            result = support.recv(taker, &receive_buffer, &len, 1000);
            if (result == c.FIPC_OK) {
                got_big = got_big or (len == 4096 and std.mem.allEqual(u8, receive_buffer[0..len], 7));
                got_zero_copy = got_zero_copy or std.mem.eql(u8, receive_buffer[0..len], "zc-ok");
            }
        } else {
            result = c.fipc_recv_acquire(taker, &in, &len, 1000);
            if (result == c.FIPC_OK) {
                _ = t.expect(@intFromPtr(in.?) % 16 == 0, "16-byte aligned");
                const bytes = @as([*]const u8, @ptrCast(in.?))[0..len];
                got_big = got_big or (len == 4096 and std.mem.allEqual(u8, bytes, 7));
                got_zero_copy = got_zero_copy or std.mem.eql(u8, bytes, "zc-ok");
                c.fipc_recv_release(taker);
                c.fipc_recv_release(taker); // once
            }
        }
        _ = t.expectEqual(result, c.FIPC_OK, "a receive");
    }
    _ = t.expect(got_big and got_zero_copy, "both messages, each once");
    _ = t.expectEqual(support.recv(taker, &receive_buffer, &len, 0), c.FIPC_TIMEOUT, "nothing more");
    _ = t.expectEqual(c.fipc_send(giver, "x", 1, 0), c.FIPC_OK, "a send still works");
    return t.done();
}

test "fast: the owner's producer and consumer, joined on its own thread: one thread owns a queue, its member closed before or after the queue" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    // An own producer: messages wait in its ring before any consumer joins
    var queue: shared.Queue = .{};
    const name = try shared.openQueue(&t, &queue, &buf, "mq_own", 4096, 1, 1, producer);
    for (0..3) |i| try t.checkEqual(sendStamp(queue.own, i, 64, 0), c.FIPC_OK, "a job before any consumer");
    const taker = try joined(&t, name, consumer);
    defer c.fipc_close(taker);
    // Its close before the queue's: the queue goes on, the place drains (its three messages are still to take)
    c.fipc_close(queue.own);
    queue.own = null;
    var result: c.fipc_result_t = 0;
    for (0..3) |i| _ = t.expect(recvStamp(taker, false, 1000, &result) == i, "the jobs, in order");
    _ = t.expect(recvStamp(taker, false, 50, &result) == null, "nothing more");
    _ = t.expectEqual(result, c.FIPC_TIMEOUT, "the queue goes on without producers");
    // The queue's close ends it
    c.fipc_queue_close(queue.handle);
    queue.handle = null;
    _ = t.expect(recvStamp(taker, false, 1000, &result) == null, "the end");
    _ = t.expectEqual(result, c.FIPC_DISCONNECTED, "DISCONNECTED");

    // The owner's consumer, closed after the queue: it drains what is left first
    var second: shared.Queue = .{};
    try t.check(second.open(name, 4096, 1, 1, consumer), "a queue, and the owner's consumer");
    const giver = try joined(&t, name, producer);
    defer c.fipc_close(giver);
    for (0..3) |i| try t.checkEqual(sendStamp(giver, i, 64, 0), c.FIPC_OK, "a job");
    _ = t.expect(recvStamp(second.own, true, 1000, &result) == 0, "the first, in place");
    c.fipc_queue_close(second.handle);
    second.handle = null;
    _ = t.expect(support.waitForEnd(giver, 5000), "the producer sees the end");
    _ = t.expectEqual(sendStamp(giver, 3, 64, 0), c.FIPC_DISCONNECTED, "its send is DISCONNECTED");
    for (1..3) |i| _ = t.expect(recvStamp(second.own, false, 1000, &result) == i, "the rest");
    _ = t.expect(recvStamp(second.own, false, 1000, &result) == null, "then");
    _ = t.expectEqual(result, c.FIPC_DISCONNECTED, "DISCONNECTED");
    second.close();
    try t.check(support.freeAtOnce(name), "the name is free");
    return t.done();
}

test "fast: the shared example: the producer owns the queue; the consumer receives three jobs, then DISCONNECTED" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    const name = support.uniqueName(&buf, "mq_example");
    const Consumer = struct {
        fn run(queue_name: [*:0]const u8, jobs: *[3][16]u8, received: *usize, end: *c.fipc_result_t) void {
            var conn: support.Conn = null;
            end.* = c.fipc_queue_join(queue_name, consumer, &conn, 5000);
            if (end.* != c.FIPC_OK) return;
            defer c.fipc_close(conn);
            while (true) {
                var job: [64]u8 = undefined;
                var len: usize = 0;
                end.* = c.fipc_recv(conn, &job, job.len, &len, 5000);
                if (end.* != c.FIPC_OK) return;
                if (received.* < 3 and len <= 16) @memcpy(jobs[received.*][0..len], job[0..len]);
                received.* += 1;
            }
        }
    };
    // The consumer first: its join waits for the queue
    var jobs: [3][16]u8 = @splat(@splat(0));
    var received: usize = 0;
    var end: c.fipc_result_t = 0;
    const thread = try std.Thread.spawn(.{}, Consumer.run, .{ name.ptr, &jobs, &received, &end });
    support.sleepMs(50);
    var queue: support.Queue = null;
    var own: support.Conn = null;
    try t.checkEqual(c.fipc_queue_create(name, 4096, 1, 1, &queue), c.FIPC_OK, "queue_create");
    try t.checkEqual(c.fipc_queue_join(name, producer, &own, c.FIPC_FOREVER), c.FIPC_OK, "the owner joins as the producer");
    for ([_][]const u8{ "job 1", "job 2", "job 3" }) |job| _ = t.expectEqual(support.send(own, job, c.FIPC_FOREVER), c.FIPC_OK, "a job");
    var count: u32 = 0;
    _ = t.expectEqual(c.fipc_queue_wait_for_members(queue, consumer, 1, &count, c.FIPC_FOREVER), c.FIPC_OK, "the consumer joins");
    c.fipc_close(own);
    c.fipc_queue_close(queue);
    thread.join();
    _ = t.expectEqual(end, c.FIPC_DISCONNECTED, "the consumer's last receive");
    _ = t.expectEqual(received, 3, "three jobs");
    // Each job once, in any order: the queue promises none
    for ([_][]const u8{ "job 1", "job 2", "job 3" }) |expected| {
        var times: usize = 0;
        for (jobs) |job| times += @intFromBool(std.mem.eql(u8, job[0..5], expected));
        _ = t.expectEqual(times, 1, "each job once");
    }
    return t.done();
}

test "fast: queue_wait_for_members counts each role and both: at_least 0, a poll, a wait a join ends, INVALID, the leave" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    var queue: shared.Queue = .{};
    const name = try shared.openQueue(&t, &queue, &buf, "mq_count", 4096, 2, 3, 0);
    defer queue.close();
    var count: u32 = 99;
    for ([_]c_int{ 0, producer, consumer }) |role| {
        count = 99;
        _ = t.expectEqual(c.fipc_queue_wait_for_members(queue.handle, role, 0, &count, 0), c.FIPC_OK, "at_least 0 returns at once");
        _ = t.expectEqual(count, 0, "nobody yet");
    }
    count = 99;
    _ = t.expectEqual(c.fipc_queue_wait_for_members(queue.handle, producer, 1, &count, 0), c.FIPC_TIMEOUT, "a poll for a producer");
    _ = t.expectEqual(count, 0, "the count at the poll");
    const begun = support.nowMs();
    _ = t.expectEqual(c.fipc_queue_wait_for_members(queue.handle, 0, 1, &count, 100), c.FIPC_TIMEOUT, "a wait of 100 ms");
    _ = t.expect(support.nowMs() - begun >= 90, "after its timeout");
    // INVALID for another role, or at_least over the role's maximum
    count = 99;
    for ([_]c_int{ c.FIPC_SUBSCRIBER_LOSSY, c.FIPC_SUBSCRIBER_GATED, 5, -1 }) |role| {
        _ = t.expectEqual(c.fipc_queue_wait_for_members(queue.handle, role, 0, &count, 0), c.FIPC_INVALID, "a role that isn't a member's");
    }
    _ = t.expectEqual(c.fipc_queue_wait_for_members(queue.handle, producer, 3, &count, 0), c.FIPC_INVALID, "over max_producers");
    _ = t.expectEqual(c.fipc_queue_wait_for_members(queue.handle, consumer, 4, &count, 0), c.FIPC_INVALID, "over max_consumers");
    _ = t.expectEqual(c.fipc_queue_wait_for_members(queue.handle, 0, 6, &count, 0), c.FIPC_INVALID, "over their sum");
    _ = t.expectEqual(c.fipc_queue_wait_for_members(queue.handle, 0, 0, null, 0), c.FIPC_INVALID, "a NULL out_count");
    _ = t.expectEqual(count, 99, "INVALID stores nothing");

    // A wait on another thread that the joins end; each join is counted before it returns
    var waited_count: u32 = 0;
    var waited: c.fipc_result_t = c.FIPC_OK;
    const waiter = try std.Thread.spawn(.{}, waitForMembers, .{ queue.handle, consumer, 2, &waited_count, &waited });
    const giver = try joined(&t, name, producer);
    defer c.fipc_close(giver);
    _ = t.expectEqual(c.fipc_queue_wait_for_members(queue.handle, producer, 1, &count, 0), c.FIPC_OK, "the producer is counted");
    var takers: [2]support.Conn = .{ try joined(&t, name, consumer), try joined(&t, name, consumer) };
    defer for (takers) |taker| c.fipc_close(taker);
    waiter.join();
    _ = t.expectEqual(waited, c.FIPC_OK, "the wait for two consumers");
    _ = t.expectEqual(waited_count, 2, "its count");
    _ = t.expectEqual(c.fipc_queue_wait_for_members(queue.handle, 0, 3, &count, 0), c.FIPC_OK, "both roles: three");
    _ = t.expectEqual(count, 3, "the count");
    // A leave: counted until the owner's poller has seen it
    c.fipc_close(takers[0]);
    takers[0] = null;
    const left = support.nowMs();
    while (support.nowMs() - left < 5000) {
        _ = c.fipc_queue_wait_for_members(queue.handle, consumer, 0, &count, 0);
        if (count == 1) break;
        support.sleepMs(1);
    }
    _ = t.expectEqual(count, 1, "the leave is uncounted");
    return t.done();
}

fn waitForMembers(queue: support.Queue, role: c_int, at_least: u32, count: *u32, result: *c.fipc_result_t) void {
    result.* = c.fipc_queue_wait_for_members(queue, role, at_least, count, c.FIPC_FOREVER);
}

test "fast: queue_cancel ends a waiting queue_wait_for_members at once, and every later one; the queue goes on" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    var queue: shared.Queue = .{};
    const name = try shared.openQueue(&t, &queue, &buf, "mq_cancel", 4096, 1, 1, producer);
    defer queue.close();
    var count: u32 = 99;
    var result: c.fipc_result_t = c.FIPC_OK;
    const waiter = try std.Thread.spawn(.{}, waitForMembers, .{ queue.handle, consumer, 1, &count, &result });
    support.sleepMs(50);
    const begun = support.nowMs();
    c.fipc_queue_cancel(queue.handle);
    waiter.join();
    _ = t.expectEqual(result, c.FIPC_CANCELLED, "the waiting count is CANCELLED");
    _ = t.expect(support.nowMs() - begun < 1000, "at once");
    _ = t.expectEqual(count, 0, "with the count");
    _ = t.expectEqual(c.fipc_queue_wait_for_members(queue.handle, consumer, 1, &count, c.FIPC_FOREVER), c.FIPC_CANCELLED, "a later wait too");
    _ = t.expectEqual(c.fipc_queue_wait_for_members(queue.handle, producer, 1, &count, c.FIPC_FOREVER), c.FIPC_OK, "a count already reached");
    c.fipc_queue_cancel(queue.handle);
    // The queue goes on: a consumer joins and takes a job
    const taker = try joined(&t, name, consumer);
    defer c.fipc_close(taker);
    try t.checkEqual(sendStamp(queue.own, 1, 64, 0), c.FIPC_OK, "a job");
    var received: c.fipc_result_t = 0;
    _ = t.expect(recvStamp(taker, false, 1000, &received) == 1, "received");
    return t.done();
}

test "fast: a queue's end: a waiting queue_wait_for_members is DISCONNECTED, and so is every later one" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    var queue: shared.Queue = .{};
    const name = try shared.openQueue(&t, &queue, &buf, "mq_count_end", 4096, 2, 1, producer);
    defer queue.close();
    const taker = try joined(&t, name, consumer);
    defer c.fipc_close(taker);
    // The queue ends as its owner's process would: a member's watcher marks the segment, and the owner's poller sees
    // it at a member's end
    var count: u32 = 99;
    var result: c.fipc_result_t = c.FIPC_OK;
    const waiter = try std.Thread.spawn(.{}, waitForMembers, .{ queue.handle, producer, 2, &count, &result });
    support.sleepMs(50);
    shared.layoutOf(taker).header.ended.store(1, .seq_cst);
    c.fipc_close(queue.own);
    queue.own = null;
    waiter.join();
    _ = t.expectEqual(result, c.FIPC_DISCONNECTED, "the waiting count is DISCONNECTED");
    _ = t.expectEqual(c.fipc_queue_wait_for_members(queue.handle, producer, 2, &count, c.FIPC_FOREVER), c.FIPC_DISCONNECTED, "a later wait too");
    return t.done();
}

test "fast: a joined producer's close doesn't end the queue: the consumer's next receive times out" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    var queue: shared.Queue = .{};
    const name = try shared.openQueue(&t, &queue, &buf, "mq_producer_close", 4096, 2, 1, consumer);
    defer queue.close();
    const giver = try joined(&t, name, producer);
    for (0..3) |i| try t.checkEqual(sendStamp(giver, i, 64, 0), c.FIPC_OK, "a job");
    c.fipc_close(giver);
    var result: c.fipc_result_t = 0;
    for (0..3) |i| _ = t.expect(recvStamp(queue.own, false, 1000, &result) == i, "the jobs, after the producer's close");
    _ = t.expect(recvStamp(queue.own, false, 200, &result) == null, "then");
    _ = t.expectEqual(result, c.FIPC_TIMEOUT, "the queue goes on: TIMEOUT");
    // Another producer joins and sends
    const next = try joined(&t, name, producer);
    defer c.fipc_close(next);
    try t.checkEqual(sendStamp(next, 7, 64, 0), c.FIPC_OK, "the next producer's job");
    _ = t.expect(recvStamp(queue.own, false, 1000, &result) == 7, "received");
    return t.done();
}

/// A queue's whole life through the native API: the owner's producer and a consumer, both joined on the owner's thread,
/// a message through, every close.
fn nativeCycle(t: *support.Test, name: []const u8) !void {
    const queue = try fastipc.queueCreate(std.testing.allocator, name, 4096, 2, 2);
    const giver = try fastipc.queueJoin(std.testing.allocator, name, .producer, support.within(5000));
    const taker = try fastipc.queueJoin(std.testing.allocator, name, .consumer, support.within(5000));
    _ = t.expectEqual(try queue.waitForMembers(null, 2, support.within(0)), 2, "both are counted");
    try giver.send("job", support.within(1000));
    var buf: [16]u8 = undefined;
    try t.check(std.mem.eql(u8, buf[0..try taker.recv(&buf, support.within(1000))], "job"), "the job");
    _ = t.expect(!taker.ended(), "not ended");
    queue.close();
    _ = t.expect(giver.ended(), "the producer sees the end at once");
    try t.check(if (taker.recv(&buf, support.within(5000))) |_| false else |err| err == error.Disconnected, "the consumer's end");
    taker.close();
    giver.close();
}

test "fast: nothing is left after a queue's close: its name is free at once, its memory and handles are back" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    const name = support.uniqueName(&buf, "mq_cycle");
    try nativeCycle(&t, name);
    try t.check(support.freeAtOnce(name), "the name is free at once after the close");
    const handles = support.handleCount();
    for (0..3) |_| {
        try nativeCycle(&t, name);
        try t.check(support.freeAtOnce(name), "the name is free at once after the close");
    }
    _ = t.expect(support.handlesBack(handles), "the descriptors or handles are back");
    return t.done();
}
