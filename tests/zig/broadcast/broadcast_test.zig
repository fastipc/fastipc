//! A broadcast through the C API, in one process: `fipc_broadcast_create`, and `fipc_broadcast_join` on the test's
//! thread (the publisher's process admits the joiners by itself) or on threads of their own (`support.Joiner`). Each
//! call's arguments and each role's calls, what a subscriber receives after it joined, the publisher's close, a full
//! broadcast, cancel and the timeouts, a broadcast to `fipc_stream_connect` and a stream to `fipc_broadcast_join`, and
//! nothing left after a broadcast's close.

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

test "fast: broadcast_create checks its capacity, subscribers and name, and its name is held against any kind" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    const name = support.uniqueName(&buf, "bc_args");
    var publisher: support.Conn = null;
    for ([_]usize{ 0, 2048, 4097, 1 << 32, 1 << 33 }) |capacity| {
        _ = t.expectEqual(c.fipc_broadcast_create(name, capacity, 1, c.FIPC_FOREVER, &publisher), c.FIPC_INVALID, "a capacity out of range");
    }
    for ([_]u32{ 0, 1025 }) |max| {
        _ = t.expectEqual(c.fipc_broadcast_create(name, 4096, max, c.FIPC_FOREVER, &publisher), c.FIPC_INVALID, "max_subscribers out of range");
    }
    _ = t.expectEqual(c.fipc_broadcast_create("has space", 4096, 1, c.FIPC_FOREVER, &publisher), c.FIPC_INVALID, "a name that isn't valid");
    _ = t.expect(publisher == null, "nothing was created");
    try t.check(support.freeAtOnce(name), "the name is free");

    var broadcast: support.Broadcast = .{};
    try t.check(broadcast.open(name, 4096, 1024, c.FIPC_FOREVER), "a broadcast of 1024 subscribers");
    var other: support.Listener = null;
    var queue: support.Queue = null;
    _ = t.expectEqual(c.fipc_stream_create(name, 4096, &other), c.FIPC_ADDR_IN_USE, "a stream on a broadcast's name");
    _ = t.expectEqual(c.fipc_broadcast_create(name, 4096, 1, 0, &publisher), c.FIPC_ADDR_IN_USE, "a second broadcast on the name");
    _ = t.expectEqual(c.fipc_queue_create(name, 4096, 1, 1, &queue), c.FIPC_ADDR_IN_USE, "a queue on the name");
    broadcast.close();
    try t.check(support.freeAtOnce(name), "the name is free again after the close");
    try t.check(c.fipc_stream_create(name, 4096, &other) == c.FIPC_OK, "a server listens");
    _ = t.expectEqual(c.fipc_broadcast_create(name, 4096, 1, 0, &publisher), c.FIPC_ADDR_IN_USE, "a broadcast on a stream's name");
    c.fipc_listener_close(other);
    return t.done();
}

test "fast: each role's calls: what it can't use is INVALID; max_piece is a quarter of the capacity, TOO_LARGE above" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    var broadcast: support.Broadcast = .{};
    const name = try support.openBroadcast(&t, &broadcast, &buf, "bc_roles", 16384, 2, c.FIPC_FOREVER);
    defer broadcast.close();
    const subscriber = try joined(&t, name, lossy);
    defer c.fipc_close(subscriber);
    const writer = broadcast.publisher;

    _ = t.expectEqual(c.fipc_max_piece(writer), 4096, "the writer's max_piece");
    _ = t.expectEqual(c.fipc_max_piece(subscriber), 4096, "the subscriber's max_piece");
    var big: [4097]u8 = @splat(7);
    _ = t.expectEqual(support.send(writer, &big, 0), c.FIPC_TOO_LARGE, "a send over one piece");
    _ = t.expectEqual(support.send(writer, big[0..4096], 0), c.FIPC_OK, "a send of one piece");
    var out: ?*anyopaque = null;
    _ = t.expectEqual(c.fipc_send_acquire(writer, 4097, &out, 0), c.FIPC_TOO_LARGE, "a reservation over one piece");
    _ = t.expectEqual(c.fipc_send(writer, &big, 0, 0), c.FIPC_INVALID, "an empty send");
    _ = t.expectEqual(c.fipc_send_commit(writer, 1), c.FIPC_INVALID, "a commit without a reservation");

    var len: usize = 0;
    var in: ?*const anyopaque = null;
    var receive_buffer: [8192]u8 = undefined;
    // The writer receives nothing
    _ = t.expectEqual(support.recv(writer, &receive_buffer, &len, 0), c.FIPC_INVALID, "recv on the writer");
    _ = t.expectEqual(c.fipc_recv_acquire(writer, &in, &len, 0), c.FIPC_INVALID, "recv_acquire on the writer");
    c.fipc_recv_release(writer);
    // A subscriber sends nothing; a lossy one doesn't receive in place
    _ = t.expectEqual(support.send(subscriber, "x", 0), c.FIPC_INVALID, "send on a subscriber");
    _ = t.expectEqual(c.fipc_send_acquire(subscriber, 1, &out, 0), c.FIPC_INVALID, "send_acquire on a subscriber");
    _ = t.expectEqual(c.fipc_send_commit(subscriber, 1), c.FIPC_INVALID, "send_commit on a subscriber");
    _ = t.expectEqual(c.fipc_recv_acquire(subscriber, &in, &len, 0), c.FIPC_INVALID, "recv_acquire on a lossy subscriber");
    c.fipc_recv_release(subscriber);
    // The message sent above is still the subscriber's next
    _ = t.expectEqual(support.recv(subscriber, &receive_buffer, &len, 0), c.FIPC_OK, "the subscriber's message");
    _ = t.expectEqual(len, 4096, "of one piece");
    return t.done();
}

test "fast: a gated subscriber's sends are INVALID; the message it has stays" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    var broadcast: support.Broadcast = .{};
    const name = try support.openBroadcast(&t, &broadcast, &buf, "bc_gated_sends", 4096, 1, c.FIPC_FOREVER);
    defer broadcast.close();
    const subscriber = try joined(&t, name, gated);
    defer c.fipc_close(subscriber);
    try t.checkEqual(sendStamp(broadcast.publisher, 1, 40, 0), c.FIPC_OK, "send");
    var out: ?*anyopaque = null;
    _ = t.expectEqual(support.send(subscriber, "x", 0), c.FIPC_INVALID, "send on a gated subscriber");
    _ = t.expectEqual(c.fipc_send_acquire(subscriber, 1, &out, 0), c.FIPC_INVALID, "send_acquire on a gated subscriber");
    _ = t.expectEqual(c.fipc_send_commit(subscriber, 1), c.FIPC_INVALID, "send_commit on a gated subscriber");
    var result: c.fipc_result_t = 0;
    _ = t.expectEqual(recvStampInPlace(subscriber, 0, &result) orelse 999, 1, "the message, still there");
    return t.done();
}

test "fast: a receive into a buffer too small is TOO_LARGE and keeps the message" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    var broadcast: support.Broadcast = .{};
    const name = try support.openBroadcast(&t, &broadcast, &buf, "bc_small", 4096, 1, c.FIPC_FOREVER);
    defer broadcast.close();
    const subscriber = try joined(&t, name, lossy);
    defer c.fipc_close(subscriber);
    try t.checkEqual(support.send(broadcast.publisher, "a message of 26 bytes long", 0), c.FIPC_OK, "send");
    var small: [8]u8 = undefined;
    var len: usize = 0;
    _ = t.expectEqual(support.recv(subscriber, &small, &len, 1000), c.FIPC_TOO_LARGE, "a buffer too small");
    _ = t.expectEqual(len, 26, "the message's length");
    _ = t.expectEqual(c.fipc_recv(subscriber, null, 0, &len, 0), c.FIPC_TOO_LARGE, "no buffer at all");
    _ = t.expect(support.recvIs(subscriber, 0, "a message of 26 bytes long"), "the message, still there");
    return t.done();
}

test "fast: a subscriber receives only the messages sent after it joined" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    var broadcast: support.Broadcast = .{};
    const name = try support.openBroadcast(&t, &broadcast, &buf, "bc_late", 4096, 2, c.FIPC_FOREVER);
    defer broadcast.close();
    const early = try joined(&t, name, lossy);
    defer c.fipc_close(early);
    for (0..5) |i| try t.checkEqual(sendStamp(broadcast.publisher, i, 32, 0), c.FIPC_OK, "send");
    const late = try joined(&t, name, lossy);
    defer c.fipc_close(late);
    try t.checkEqual(sendStamp(broadcast.publisher, 5, 32, 0), c.FIPC_OK, "send");
    var result: c.fipc_result_t = 0;
    _ = t.expectEqual(recvStamp(late, 1000, &result) orelse 999, 5, "the late subscriber's first message");
    for (0..6) |i| _ = t.expectEqual(recvStamp(early, 1000, &result) orelse 999, i, "the early subscriber's");
    return t.done();
}

test "fast: subscribers on threads receive one checked stream, then DISCONNECTED once the publisher closed" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    var broadcast: support.Broadcast = .{};
    const name = try support.openBroadcast(&t, &broadcast, &buf, "bc_threads", 1 << 20, 4, c.FIPC_FOREVER);
    defer broadcast.close();
    var receivers: [3]Receiver = undefined;
    for (&receivers) |*receiver| receiver.* = .{ .subscriber = try joined(&t, name, lossy) };
    for (&receivers) |*receiver| receiver.thread = try std.Thread.spawn(.{}, Receiver.run, .{receiver});
    const count = 20_000;
    for (0..count) |i| try t.checkEqual(sendStamp(broadcast.publisher, i, support.Stamp.sizeOf(i, 4096), 0), c.FIPC_OK, "send");
    c.fipc_close(broadcast.publisher);
    broadcast.publisher = null;
    for (&receivers) |*receiver| {
        receiver.thread.join();
        c.fipc_close(receiver.subscriber);
        _ = t.expectEqual(receiver.end, c.FIPC_DISCONNECTED, "the stream ends with DISCONNECTED");
        _ = t.expectEqual(receiver.tally.bad, 0, "every message whole and in order");
        _ = t.expect(receiver.tally.complete(count), "up to the last message");
    }
    return t.done();
}

test "fast: the publisher's close: a subscriber receives what was sent, then DISCONNECTED; the name is free at once" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    var broadcast: support.Broadcast = .{};
    const name = try support.openBroadcast(&t, &broadcast, &buf, "bc_wclose", 4096, 2, c.FIPC_FOREVER);
    defer broadcast.close();
    const subscriber = try joined(&t, name, lossy);
    defer c.fipc_close(subscriber);
    for (0..5) |i| try t.checkEqual(sendStamp(broadcast.publisher, i, 40, 0), c.FIPC_OK, "send");
    c.fipc_close(broadcast.publisher);
    broadcast.publisher = null;
    var result: c.fipc_result_t = 0;
    for (0..5) |i| _ = t.expectEqual(recvStamp(subscriber, 2000, &result) orelse 999, i, "what was sent");
    const begun = support.nowMs();
    _ = t.expect(recvStamp(subscriber, 5000, &result) == null and result == c.FIPC_DISCONNECTED, "then DISCONNECTED");
    _ = t.expect(support.nowMs() - begun < 2000, "promptly");
    _ = t.expectEqual(recvStamp(subscriber, 0, &result) orelse 0, 0, "and again");
    _ = t.expectEqual(result, c.FIPC_DISCONNECTED, "DISCONNECTED again");
    // Nobody holds the name: a joiner meanwhile waits out its timeout
    var joiner: support.Joiner = .{};
    try t.check(joiner.start(name, lossy, 300), "a joiner");
    _ = t.expectEqual(joiner.join(null), c.FIPC_TIMEOUT, "the joiner times out");
    try t.check(support.freeAtOnce(name), "the name is free at once");
    return t.done();
}

test "fast: a joiner that waits while the publisher closes joins the next broadcast on the name, within its timeout" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    var broadcast: support.Broadcast = .{};
    const name = try support.openBroadcast(&t, &broadcast, &buf, "bc_next", 4096, 1, c.FIPC_FOREVER);
    defer broadcast.close();
    const first = try joined(&t, name, lossy);
    defer c.fipc_close(first);
    // The only slot is held: the joiner waits
    var joiner: support.Joiner = .{};
    try t.check(joiner.start(name, lossy, 5000), "a joiner");
    support.sleepMs(100);
    c.fipc_close(broadcast.publisher);
    broadcast.publisher = null;
    try t.check(broadcast.open(name, 4096, 1, c.FIPC_FOREVER), "a new broadcast on the name, at once");
    var second: support.Conn = null;
    try t.checkEqual(joiner.join(&second), c.FIPC_OK, "the joiner joins the new broadcast");
    defer c.fipc_close(second);
    try t.checkEqual(support.send(broadcast.publisher, "new", 0), c.FIPC_OK, "send");
    _ = t.expect(support.recvIs(second, 1000, "new"), "it receives the new publisher's messages");
    return t.done();
}

test "fast: a full broadcast takes nobody until a subscriber's close frees its slot" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    var broadcast: support.Broadcast = .{};
    const name = try support.openBroadcast(&t, &broadcast, &buf, "bc_full", 4096, 1, c.FIPC_FOREVER);
    defer broadcast.close();
    const first = try joined(&t, name, lossy);
    var none: support.Conn = null;
    _ = t.expectEqual(c.fipc_broadcast_join(name, lossy, &none, 200), c.FIPC_TIMEOUT, "a join, with every slot held");
    var joiner: support.Joiner = .{};
    try t.check(joiner.start(name, lossy, 5000), "a second joiner");
    support.sleepMs(50);
    c.fipc_close(first);
    var second: support.Conn = null;
    try t.checkEqual(joiner.join(&second), c.FIPC_OK, "the slot is free: the second subscriber joined");
    _ = t.expectEqual(sendStamp(broadcast.publisher, 7, 20, 0), c.FIPC_OK, "send");
    var result: c.fipc_result_t = 0;
    _ = t.expectEqual(recvStamp(second, 1000, &result) orelse 0, 7, "the slot's new subscriber receives");
    c.fipc_close(second);
    return t.done();
}

test "fast: cancel ends a subscriber's waiting receive; what is there is still received" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    var broadcast: support.Broadcast = .{};
    const name = try support.openBroadcast(&t, &broadcast, &buf, "bc_cancel", 4096, 2, c.FIPC_FOREVER);
    defer broadcast.close();
    const subscriber = try joined(&t, name, lossy);
    defer c.fipc_close(subscriber);
    const other = try joined(&t, name, lossy);
    defer c.fipc_close(other);
    var result: c.fipc_result_t = c.FIPC_OK;
    const since = sleepsOf(subscriber);
    const thread = try std.Thread.spawn(.{}, recvForever, .{ subscriber, &result });
    awaitAsleep(subscriber, since);
    const begun = support.nowMs();
    c.fipc_cancel(subscriber);
    thread.join();
    _ = t.expectEqual(result, c.FIPC_CANCELLED, "the waiting receive");
    _ = t.expect(support.nowMs() - begun < 1000, "at once");
    try t.checkEqual(support.send(broadcast.publisher, "there", 0), c.FIPC_OK, "send");
    _ = t.expect(support.recvIs(subscriber, 0, "there"), "a message that is there");
    _ = t.expect(support.recvIs(other, 1000, "there"), "the other subscriber isn't cancelled");
    return t.done();
}

test "fast: the timeouts of receive and join" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    var broadcast: support.Broadcast = .{};
    const name = try support.openBroadcast(&t, &broadcast, &buf, "bc_timeouts", 4096, 2, c.FIPC_FOREVER);
    defer broadcast.close();
    const subscriber = try joined(&t, name, lossy);
    defer c.fipc_close(subscriber);
    var receive_buffer: [16]u8 = undefined;
    var len: usize = 0;
    var begun = support.nowMs();
    _ = t.expectEqual(support.recv(subscriber, &receive_buffer, &len, 0), c.FIPC_TIMEOUT, "recv, 0");
    _ = t.expectEqual(support.recv(subscriber, &receive_buffer, &len, 50), c.FIPC_TIMEOUT, "recv, 50 ms");
    var elapsed = support.nowMs() - begun;
    _ = t.expect(elapsed >= 45 and elapsed < 1000, "recv waited its timeout");
    var buf2: [64]u8 = undefined;
    var none: support.Conn = null;
    begun = support.nowMs();
    _ = t.expectEqual(c.fipc_broadcast_join(support.uniqueName(&buf2, "bc_nobody"), lossy, &none, 100), c.FIPC_TIMEOUT, "join with nobody listening");
    elapsed = support.nowMs() - begun;
    _ = t.expect(elapsed >= 95 and elapsed < 2000, "join waited its timeout");
    return t.done();
}

test "fast: fipc_stream_connect and fipc_queue_join to a broadcast, fipc_broadcast_join to a stream, are INVALID at once" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    var broadcast: support.Broadcast = .{};
    const name = try support.openBroadcast(&t, &broadcast, &buf, "bc_mixed", 4096, 2, c.FIPC_FOREVER);
    defer broadcast.close();
    const begun = support.nowMs();
    var connector: support.Connector = .{};
    try t.check(connector.start(name, 5000), "a client of fipc_stream_connect");
    _ = t.expectEqual(connector.join(null), c.FIPC_INVALID, "fipc_stream_connect to a broadcast");
    _ = t.expect(support.nowMs() - begun < 2500, "at once, not at its timeout");
    var member: support.Conn = null;
    _ = t.expectEqual(c.fipc_queue_join(name, c.FIPC_PRODUCER, &member, 5000), c.FIPC_INVALID, "fipc_queue_join to a broadcast");
    _ = t.expect(support.nowMs() - begun < 2500, "at once too");
    const subscriber = try joined(&t, name, lossy);
    c.fipc_close(subscriber);

    var buf2: [64]u8 = undefined;
    const server_name = support.uniqueName(&buf2, "bc_mixed_srv");
    var listener: support.Listener = null;
    try t.checkEqual(c.fipc_stream_create(server_name, 4096, &listener), c.FIPC_OK, "a server");
    defer c.fipc_listener_close(listener);
    var joiner: support.Joiner = .{};
    try t.check(joiner.start(server_name, lossy, 5000), "a joiner");
    var conn: support.Conn = null;
    _ = t.expectEqual(c.fipc_stream_accept(listener, &conn, 1000), c.FIPC_TIMEOUT, "accept doesn't take it");
    _ = t.expectEqual(joiner.join(null), c.FIPC_INVALID, "fipc_broadcast_join to a stream");
    _ = t.expect(joiner.elapsed_ms < 1500, "at once, not at its timeout");
    var client: support.Conn = null;
    var server: support.Conn = null;
    try t.check(support.connectAccept(listener, server_name, &client, &server, 5000), "the server's next client");
    c.fipc_close(client);
    c.fipc_close(server);
    return t.done();
}

/// One broadcast's life through the native API: a lossy and a gated subscriber, joined on the publisher's thread, a
/// message to both, every close.
fn nativeCycle(t: *support.Test, name: []const u8) !void {
    const publisher = try fastipc.broadcastCreate(std.testing.allocator, name, 65536, 8, -1);
    const lossy_subscriber = try fastipc.broadcastJoin(std.testing.allocator, name, .lossy, support.within(5000));
    const gated_subscriber = try fastipc.broadcastJoin(std.testing.allocator, name, .gated, support.within(5000));
    try publisher.send("x", .none);
    var receive_buffer: [8]u8 = undefined;
    _ = t.expect(std.mem.eql(u8, receive_buffer[0..try lossy_subscriber.recv(&receive_buffer, support.within(1000))], "x"), "the lossy subscriber receives");
    _ = t.expect(std.mem.eql(u8, receive_buffer[0..try gated_subscriber.recv(&receive_buffer, support.within(1000))], "x"), "the gated one too");
    lossy_subscriber.close();
    publisher.close();
    gated_subscriber.close();
}

test "fast: nothing is left after a broadcast's close: its name is free at once, its memory and handles are back" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    const name = support.uniqueName(&buf, "bc_cycle");
    // A first cycle before counting: what the process creates once and keeps for every later broadcast isn't counted
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
