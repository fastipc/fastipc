//! A service through the C API, in one process: `fipc_service_create` and its arguments, the owner's members (joined
//! on its own thread: its process admits them by itself), `fipc_service_join` on the test's thread or on threads of
//! their own (`support.Joiner`), each role's calls, the kinds a joiner refuses, the owner's count of its members and
//! its cancel, and nothing left after a service's close.

const std = @import("std");
const fastipc = @import("fastipc");
const support = @import("../support.zig");
const shared = @import("shared.zig");
const c = support.c;

const client = shared.client;
const server = shared.server;
const joined = shared.joined;
const sendStamp = shared.sendStamp;
const recvStamp = shared.recvStamp;
const replyOf = shared.replyOf;

test "fast: service_create checks its arguments, and its name is held against any kind" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    const name = support.uniqueName(&buf, "sv_args");
    var service: support.Service = null;
    for ([_]usize{ 0, 2048, 4097, 1 << 32, 1 << 33 }) |capacity| {
        _ = t.expectEqual(c.fipc_service_create(name, capacity, 64, 1, 1, &service), c.FIPC_INVALID, "a capacity out of range");
    }
    for ([_]usize{ 0, 1025, 1 << 40 }) |max_message| {
        _ = t.expectEqual(c.fipc_service_create(name, 4096, max_message, 1, 1, &service), c.FIPC_INVALID, "max_message out of range (1 to capacity / 4)");
    }
    for ([_]u32{ 0, 65 }) |max| {
        _ = t.expectEqual(c.fipc_service_create(name, 4096, 64, max, 1, &service), c.FIPC_INVALID, "max_clients out of range (1 to 64)");
        _ = t.expectEqual(c.fipc_service_create(name, 4096, 64, 1, max, &service), c.FIPC_INVALID, "max_servers out of range (1 to 64)");
    }
    _ = t.expectEqual(c.fipc_service_create(null, 4096, 64, 1, 1, &service), c.FIPC_INVALID, "a NULL name");
    _ = t.expectEqual(c.fipc_service_create(name, 4096, 64, 1, 1, null), c.FIPC_INVALID, "a NULL out-pointer");
    _ = t.expectEqual(c.fipc_service_create("has space", 4096, 64, 1, 1, &service), c.FIPC_INVALID, "a name that isn't valid");
    _ = t.expect(service == null, "nothing was created");
    try t.check(support.freeAtOnce(name), "the name is free");

    try t.checkEqual(c.fipc_service_create(name, 4096, 1024, 64, 64, &service), c.FIPC_OK, "a service of 64 clients and 64 servers");
    var other: support.Listener = null;
    var publisher: support.Conn = null;
    var queue: support.Queue = null;
    var other_service: support.Service = null;
    _ = t.expectEqual(c.fipc_stream_create(name, 4096, &other), c.FIPC_ADDR_IN_USE, "a stream on a service's name");
    _ = t.expectEqual(c.fipc_broadcast_create(name, 4096, 1, 0, &publisher), c.FIPC_ADDR_IN_USE, "a broadcast on a service's name");
    _ = t.expectEqual(c.fipc_queue_create(name, 4096, 1, 1, &queue), c.FIPC_ADDR_IN_USE, "a queue on a service's name");
    _ = t.expectEqual(c.fipc_service_create(name, 4096, 64, 1, 1, &other_service), c.FIPC_ADDR_IN_USE, "a second service on the name");
    c.fipc_service_close(service);
    service = null;
    try t.check(support.freeAtOnce(name), "the name is free again after the close");
    try t.checkEqual(c.fipc_queue_create(name, 4096, 1, 1, &queue), c.FIPC_OK, "a queue takes the name");
    _ = t.expectEqual(c.fipc_service_create(name, 4096, 64, 1, 1, &other_service), c.FIPC_ADDR_IN_USE, "a service on a queue's name");
    c.fipc_queue_close(queue);
    var broadcast: support.Broadcast = .{};
    try t.check(broadcast.open(name, 4096, 1, 0), "a broadcast takes the name");
    _ = t.expectEqual(c.fipc_service_create(name, 4096, 64, 1, 1, &other_service), c.FIPC_ADDR_IN_USE, "a service on a broadcast's name");
    broadcast.close();
    return t.done();
}

test "fast: joiners of another kind are INVALID at once, both ways" {
    var t = try support.begin(@src(), 60);
    defer t.end();
    var buf: [64]u8 = undefined;
    var service: shared.Service = .{};
    const name = try shared.openService(&t, &service, &buf, "sv_kinds", 4096, 64, 1, 1, .{});
    defer service.close();
    var conn: support.Conn = null;

    // A subscriber, a queue's member, and a client of fipc_stream_connect, of the service: refused at once
    for ([_]c_int{ c.FIPC_SUBSCRIBER_LOSSY, c.FIPC_SUBSCRIBER_GATED, c.FIPC_PRODUCER, c.FIPC_CONSUMER }) |role| {
        const begun = support.nowMs();
        _ = t.expectEqual(support.joinAs(name, role, &conn, 5000), c.FIPC_INVALID, "another kind's member of a service");
        _ = t.expect(support.nowMs() - begun < 4000, "at once");
    }
    var connector: support.Connector = .{};
    try t.check(connector.start(name, 5000), "a client starts");
    _ = t.expectEqual(connector.join(null), c.FIPC_INVALID, "a client of fipc_stream_connect to a service");

    // A service's member of a broadcast, of a queue, or of a stream: refused at once
    var broadcast_buf: [64]u8 = undefined;
    var broadcast: support.Broadcast = .{};
    const broadcast_name = try support.openBroadcast(&t, &broadcast, &broadcast_buf, "sv_kinds_bc", 4096, 1, c.FIPC_FOREVER);
    defer broadcast.close();
    var queue_buf: [64]u8 = undefined;
    const queue_name = support.uniqueName(&queue_buf, "sv_kinds_mq");
    var queue: support.Queue = null;
    try t.checkEqual(c.fipc_queue_create(queue_name, 4096, 1, 1, &queue), c.FIPC_OK, "a queue");
    defer c.fipc_queue_close(queue);
    for ([_][:0]const u8{ broadcast_name, queue_name }) |other_name| {
        for ([_]c_int{ client, server }) |role| {
            const begun = support.nowMs();
            _ = t.expectEqual(c.fipc_service_join(other_name, role, &conn, 5000), c.FIPC_INVALID, "a service's role on another kind");
            _ = t.expect(support.nowMs() - begun < 4000, "at once");
        }
    }
    var server_buf: [64]u8 = undefined;
    const server_name = support.uniqueName(&server_buf, "sv_kinds_server");
    var listener: support.Listener = null;
    try t.checkEqual(c.fipc_stream_create(server_name, 4096, &listener), c.FIPC_OK, "a server listens");
    defer c.fipc_listener_close(listener);
    var joiner: support.Joiner = .{};
    try t.check(joiner.start(server_name, server, 5000), "a joiner starts");
    _ = t.expectEqual(c.fipc_stream_accept(listener, &conn, 1000), c.FIPC_TIMEOUT, "the server's accept drops it");
    _ = t.expectEqual(joiner.join(null), c.FIPC_INVALID, "a service's server of a stream");

    // Nobody on the name: the join waits out its timeout; a role that isn't a service's: INVALID at once
    var nobody_buf: [64]u8 = undefined;
    const begun = support.nowMs();
    _ = t.expectEqual(c.fipc_service_join(support.uniqueName(&nobody_buf, "sv_nobody"), client, &conn, 200), c.FIPC_TIMEOUT, "no owner");
    _ = t.expect(support.nowMs() - begun >= 190, "after its timeout");
    for ([_]c_int{ 0, c.FIPC_SUBSCRIBER_LOSSY, c.FIPC_PRODUCER, c.FIPC_CONSUMER, 7, -1 }) |role| {
        _ = t.expectEqual(c.fipc_service_join(name, role, &conn, 5000), c.FIPC_INVALID, "a role that isn't a service's");
    }
    _ = t.expectEqual(c.fipc_service_join(null, client, &conn, 0), c.FIPC_INVALID, "a NULL name");
    _ = t.expectEqual(c.fipc_service_join(name, client, null, 0), c.FIPC_INVALID, "a NULL out-pointer");

    // The service still admits its own kind
    for ([_]c_int{ client, server }) |role| {
        const member = try joined(&t, name, role);
        c.fipc_close(member);
    }
    return t.done();
}

test "fast: each role's calls: what it can't use is INVALID; max_piece is max_message, TOO_LARGE above" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    var service: shared.Service = .{};
    const name = try shared.openService(&t, &service, &buf, "sv_roles", 16384, 1000, 2, 2, .{ .client = true, .server = true });
    defer service.close();
    const asker = try joined(&t, name, client);
    defer c.fipc_close(asker);
    const answerer = try joined(&t, name, server);
    defer c.fipc_close(answerer);

    for ([_]support.Conn{ service.own_client, service.own_server, asker, answerer }) |member| _ = t.expectEqual(c.fipc_max_piece(member), 1000, "max_piece: max_message");
    var big: [1001]u8 = @splat(7);
    var out: ?*anyopaque = null;
    var in: ?*const anyopaque = null;
    var len: usize = 0;
    var count: u32 = 0;
    var receive_buffer: [2048]u8 = undefined;
    for ([_]support.Conn{ service.own_client, asker }) |member| {
        _ = t.expectEqual(support.send(member, &big, 0), c.FIPC_TOO_LARGE, "a request over max_message");
        _ = t.expectEqual(c.fipc_send(member, &big, 0, 0), c.FIPC_INVALID, "an empty request");
        _ = t.expectEqual(c.fipc_send_acquire(member, 1001, &out, 0), c.FIPC_TOO_LARGE, "a reservation over max_message");
        _ = t.expectEqual(c.fipc_send_commit(member, 1), c.FIPC_INVALID, "a commit without a reservation");
        _ = t.expectEqual(support.recv(member, &receive_buffer, &len, 0), c.FIPC_TIMEOUT, "no reply to come");
        _ = t.expectEqual(c.fipc_broadcast_wait_for_members(member, 0, &count, 0), c.FIPC_INVALID, "broadcast_wait_for_members on a client");
        c.fipc_recv_release(member); // nothing held
    }
    for ([_]support.Conn{ service.own_server, answerer }) |member| {
        _ = t.expectEqual(support.send(member, "x", 0), c.FIPC_INVALID, "a reply with no request held");
        _ = t.expectEqual(c.fipc_send_acquire(member, 1, &out, 0), c.FIPC_INVALID, "a reply's room with no request held");
        _ = t.expectEqual(c.fipc_send_commit(member, 1), c.FIPC_INVALID, "a commit with no request held");
        _ = t.expectEqual(support.recv(member, &receive_buffer, &len, 0), c.FIPC_TIMEOUT, "no request");
        _ = t.expectEqual(c.fipc_broadcast_wait_for_members(member, 0, &count, 0), c.FIPC_INVALID, "broadcast_wait_for_members on a server");
        c.fipc_recv_release(member); // nothing
    }

    // A request of max_message bytes, held by a server: its receives are INVALID until it replies
    _ = t.expectEqual(support.send(asker, big[0..1000], 0), c.FIPC_OK, "a request of max_message bytes");
    _ = t.expectEqual(c.fipc_recv(answerer, &receive_buffer, 999, &len, 1000), c.FIPC_TOO_LARGE, "a buffer too short takes nothing");
    _ = t.expectEqual(len, 1000, "and reports its length");
    _ = t.expectEqual(c.fipc_recv_acquire(answerer, &in, &len, 1000), c.FIPC_OK, "the request in place");
    _ = t.expect(@intFromPtr(in.?) % 64 == 0 and len == 1000, "64-byte aligned, whole");
    _ = t.expectEqual(support.recv(answerer, &receive_buffer, &len, 0), c.FIPC_INVALID, "a receive while holding one");
    _ = t.expectEqual(c.fipc_recv_acquire(answerer, &in, &len, 0), c.FIPC_INVALID, "in place too");
    c.fipc_recv_release(answerer); // does nothing: the request is still held
    _ = t.expectEqual(support.send(answerer, &big, 0), c.FIPC_TOO_LARGE, "a reply over max_message");
    _ = t.expectEqual(c.fipc_send_acquire(answerer, 1001, &out, 0), c.FIPC_TOO_LARGE, "a reply's room over max_message");
    _ = t.expectEqual(c.fipc_send_acquire(answerer, 1000, &out, 0), c.FIPC_OK, "the reply's room");
    _ = t.expect(@intFromPtr(out.?) % 64 == 0, "64-byte aligned");
    @memset(@as([*]u8, @ptrCast(out.?))[0..1000], 3);
    _ = t.expect(std.mem.allEqual(u8, @as([*]const u8, @ptrCast(in.?))[0..1000], 7), "the request held in place stays whole");
    _ = t.expectEqual(c.fipc_send_commit(answerer, 1001), c.FIPC_INVALID, "a commit past the reservation");
    _ = t.expectEqual(c.fipc_send_commit(answerer, 1000), c.FIPC_OK, "the reply");
    _ = t.expectEqual(support.send(answerer, "x", 0), c.FIPC_INVALID, "nothing held any more");
    _ = t.expectEqual(c.fipc_recv(asker, &receive_buffer, 10, &len, 1000), c.FIPC_TOO_LARGE, "a client's buffer too short takes nothing");
    _ = t.expectEqual(len, 1000, "and reports the reply's length");
    _ = t.expectEqual(support.recv(asker, &receive_buffer, &len, 1000), c.FIPC_OK, "the reply");
    _ = t.expect(len == 1000 and std.mem.allEqual(u8, receive_buffer[0..len], 3), "whole");
    _ = t.expectEqual(support.recv(asker, &receive_buffer, &len, 0), c.FIPC_TIMEOUT, "nothing more");
    return t.done();
}

test "fast: the owner's client and server, joined on its own thread: one thread runs a call round, and its members outlive the service" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    var service: shared.Service = .{};
    _ = try shared.openService(&t, &service, &buf, "sv_own", 4096, 256, 1, 1, .{ .client = true, .server = true });
    defer service.close();
    // Send, take, reply, receive, all on this thread: a reply never waits, and the admission ran on the owner's poller
    var result: c.fipc_result_t = 0;
    for (0..100) |i| {
        try t.checkEqual(sendStamp(service.own_client, i, 256, 0), c.FIPC_OK, "a request");
        try t.check(shared.answerOne(service.own_server, 256, i % 2 == 0, 0, &result) == i, "the server takes it and replies");
        try t.check(recvStamp(service.own_client, i % 3 == 0, 0, &result) == replyOf(i), "the client receives its reply");
    }
    // Requests wait while no server takes them: three in flight, answered in turn, received in order
    for (0..3) |i| try t.checkEqual(sendStamp(service.own_client, 100 + i, 256, 0), c.FIPC_OK, "a request");
    for (0..3) |i| try t.check(shared.answerOne(service.own_server, 256, false, 0, &result) == 100 + i, "taken in order");
    for (0..3) |i| try t.check(recvStamp(service.own_client, false, 0, &result) == replyOf(100 + i), "received in order");
    // The service's close: the members stay open until their own close, and report the end
    try t.checkEqual(sendStamp(service.own_client, 200, 256, 0), c.FIPC_OK, "a request before the close");
    c.fipc_service_close(service.handle);
    service.handle = null;
    _ = t.expectEqual(sendStamp(service.own_client, 201, 256, 0), c.FIPC_DISCONNECTED, "a request after it");
    _ = t.expect(recvStamp(service.own_server, false, 0, &result) == null and result == c.FIPC_DISCONNECTED, "the server's receive");
    _ = t.expect(recvStamp(service.own_client, false, 1000, &result) == null and result == c.FIPC_DISCONNECTED, "the client's: no reply came");
    return t.done();
}

test "fast: the shared example: the server owns the service; the client's pings get their pongs, in order, then the close" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    const name = support.uniqueName(&buf, "sv_example");
    const Client = struct {
        fn run(service_name: [*:0]const u8, replies: *[3][16]u8, end: *c.fipc_result_t) void {
            var conn: support.Conn = null;
            end.* = c.fipc_service_join(service_name, client, &conn, 5000);
            if (end.* != c.FIPC_OK) return;
            defer c.fipc_close(conn);
            for (0..3) |i| {
                var ping: [16]u8 = undefined;
                const request = std.fmt.bufPrint(&ping, "ping {d}", .{i + 1}) catch unreachable;
                end.* = support.send(conn, request, 5000);
                if (end.* != c.FIPC_OK) return;
            }
            for (replies) |*reply| {
                var len: usize = 0;
                end.* = c.fipc_recv(conn, reply, reply.len, &len, 5000);
                if (end.* != c.FIPC_OK) return;
            }
            var len: usize = 0;
            var more: [16]u8 = undefined;
            end.* = c.fipc_recv(conn, &more, more.len, &len, 5000);
        }
    };
    // The client first: its join waits for the service
    var replies: [3][16]u8 = @splat(@splat(0));
    var end: c.fipc_result_t = 0;
    const thread = try std.Thread.spawn(.{}, Client.run, .{ name.ptr, &replies, &end });
    support.sleepMs(50);
    var service: support.Service = null;
    var own: support.Conn = null;
    try t.checkEqual(c.fipc_service_create(name, 4096, 64, 1, 1, &service), c.FIPC_OK, "service_create");
    try t.checkEqual(c.fipc_service_join(name, server, &own, c.FIPC_FOREVER), c.FIPC_OK, "the owner joins as the server");
    for (0..3) |i| {
        var request: [64]u8 = undefined;
        var len: usize = 0;
        _ = t.expectEqual(c.fipc_recv(own, &request, request.len, &len, c.FIPC_FOREVER), c.FIPC_OK, "a ping");
        var expected: [16]u8 = undefined;
        _ = t.expect(std.mem.eql(u8, request[0..len], std.fmt.bufPrint(&expected, "ping {d}", .{i + 1}) catch unreachable), "in order");
        var pong: [16]u8 = undefined;
        _ = t.expectEqual(support.send(own, std.fmt.bufPrint(&pong, "pong {d}", .{i + 1}) catch unreachable, 0), c.FIPC_OK, "its pong");
    }
    // The replies survive the close: the client receives them all, then DISCONNECTED
    c.fipc_close(own);
    c.fipc_service_close(service);
    thread.join();
    _ = t.expectEqual(end, c.FIPC_DISCONNECTED, "the client's last receive");
    for (replies, 0..) |reply, i| {
        var expected: [16]u8 = undefined;
        _ = t.expect(std.mem.eql(u8, reply[0..6], std.fmt.bufPrint(&expected, "pong {d}", .{i + 1}) catch unreachable), "each pong, in order");
    }
    return t.done();
}

test "fast: service_wait_for_members counts each role and both: at_least 0, a poll, a wait a join ends, INVALID, the leave" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    var service: shared.Service = .{};
    const name = try shared.openService(&t, &service, &buf, "sv_count", 4096, 64, 2, 3, .{});
    defer service.close();
    var count: u32 = 99;
    for ([_]c_int{ 0, client, server }) |role| {
        count = 99;
        _ = t.expectEqual(c.fipc_service_wait_for_members(service.handle, role, 0, &count, 0), c.FIPC_OK, "at_least 0 returns at once");
        _ = t.expectEqual(count, 0, "nobody yet");
    }
    count = 99;
    _ = t.expectEqual(c.fipc_service_wait_for_members(service.handle, client, 1, &count, 0), c.FIPC_TIMEOUT, "a poll for a client");
    _ = t.expectEqual(count, 0, "the count at the poll");
    const begun = support.nowMs();
    _ = t.expectEqual(c.fipc_service_wait_for_members(service.handle, 0, 1, &count, 100), c.FIPC_TIMEOUT, "a wait of 100 ms");
    _ = t.expect(support.nowMs() - begun >= 90, "after its timeout");
    // INVALID for another role, or at_least over the role's maximum
    count = 99;
    for ([_]c_int{ c.FIPC_SUBSCRIBER_LOSSY, c.FIPC_PRODUCER, c.FIPC_CONSUMER, 7, -1 }) |role| {
        _ = t.expectEqual(c.fipc_service_wait_for_members(service.handle, role, 0, &count, 0), c.FIPC_INVALID, "a role that isn't a member's");
    }
    _ = t.expectEqual(c.fipc_service_wait_for_members(service.handle, client, 3, &count, 0), c.FIPC_INVALID, "over max_clients");
    _ = t.expectEqual(c.fipc_service_wait_for_members(service.handle, server, 4, &count, 0), c.FIPC_INVALID, "over max_servers");
    _ = t.expectEqual(c.fipc_service_wait_for_members(service.handle, 0, 6, &count, 0), c.FIPC_INVALID, "over their sum");
    _ = t.expectEqual(c.fipc_service_wait_for_members(service.handle, 0, 0, null, 0), c.FIPC_INVALID, "a NULL out_count");
    _ = t.expectEqual(c.fipc_service_wait_for_members(null, 0, 0, &count, 0), c.FIPC_INVALID, "a NULL service");
    _ = t.expectEqual(count, 99, "INVALID stores nothing");

    // A wait on another thread that the joins end; each join is counted before it returns
    var waited_count: u32 = 0;
    var waited: c.fipc_result_t = c.FIPC_OK;
    const waiter = try std.Thread.spawn(.{}, waitForMembers, .{ service.handle, server, 2, &waited_count, &waited });
    const asker = try joined(&t, name, client);
    defer c.fipc_close(asker);
    _ = t.expectEqual(c.fipc_service_wait_for_members(service.handle, client, 1, &count, 0), c.FIPC_OK, "the client is counted");
    var answerers: [2]support.Conn = .{ try joined(&t, name, server), try joined(&t, name, server) };
    defer for (answerers) |answerer| c.fipc_close(answerer);
    waiter.join();
    _ = t.expectEqual(waited, c.FIPC_OK, "the wait for two servers");
    _ = t.expectEqual(waited_count, 2, "its count");
    _ = t.expectEqual(c.fipc_service_wait_for_members(service.handle, 0, 3, &count, 0), c.FIPC_OK, "both roles: three");
    _ = t.expectEqual(count, 3, "the count");
    // A leave: counted until the owner's poller has seen it
    c.fipc_close(answerers[0]);
    answerers[0] = null;
    _ = t.expect(shared.awaitCount(service.handle, server, 1, 5000), "the leave is uncounted");
    return t.done();
}

test "fast: the segment shows the members the owner counts joined, for the servers' spin: each join, each leave" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    var service: shared.Service = .{};
    const name = try shared.openService(&t, &service, &buf, "sv_shown", 4096, 64, 2, 3, .{});
    defer service.close();
    const asker = try joined(&t, name, client);
    defer c.fipc_close(asker);
    var answerers: [2]support.Conn = .{ try joined(&t, name, server), try joined(&t, name, server) };
    defer for (answerers) |answerer| c.fipc_close(answerer);
    const header = shared.layoutOf(asker).header;
    _ = t.expectEqual(header.clients_joined.load(.monotonic), 1, "one client joined");
    _ = t.expectEqual(header.servers_joined.load(.monotonic), 2, "two servers joined");
    c.fipc_close(answerers[0]);
    answerers[0] = null;
    // The owner's poller sees the leave, then shows its counts
    const begun = support.nowMs();
    while (header.servers_joined.load(.monotonic) != 1 and support.nowMs() - begun < 5000) std.Thread.yield() catch {};
    _ = t.expectEqual(header.servers_joined.load(.monotonic), 1, "the leave is shown");
    _ = t.expectEqual(header.clients_joined.load(.monotonic), 1, "the client is still shown");
    return t.done();
}

fn waitForMembers(service: support.Service, role: c_int, at_least: u32, count: *u32, result: *c.fipc_result_t) void {
    result.* = c.fipc_service_wait_for_members(service, role, at_least, count, c.FIPC_FOREVER);
}

test "fast: service_cancel ends a waiting service_wait_for_members at once, and every later one; the service goes on" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    var service: shared.Service = .{};
    const name = try shared.openService(&t, &service, &buf, "sv_cancel", 4096, 64, 1, 1, .{ .client = true });
    defer service.close();
    var count: u32 = 99;
    var result: c.fipc_result_t = c.FIPC_OK;
    const waiter = try std.Thread.spawn(.{}, waitForMembers, .{ service.handle, server, 1, &count, &result });
    support.sleepMs(50);
    const begun = support.nowMs();
    c.fipc_service_cancel(service.handle);
    waiter.join();
    _ = t.expectEqual(result, c.FIPC_CANCELLED, "the waiting count is CANCELLED");
    _ = t.expect(support.nowMs() - begun < 1000, "at once");
    _ = t.expectEqual(count, 0, "with the count");
    _ = t.expectEqual(c.fipc_service_wait_for_members(service.handle, server, 1, &count, c.FIPC_FOREVER), c.FIPC_CANCELLED, "a later wait too");
    _ = t.expectEqual(c.fipc_service_wait_for_members(service.handle, client, 1, &count, c.FIPC_FOREVER), c.FIPC_OK, "a count already reached");
    c.fipc_service_cancel(service.handle);
    c.fipc_service_cancel(null);
    // The service goes on: a server joins and answers
    const answerer = try joined(&t, name, server);
    defer c.fipc_close(answerer);
    try t.checkEqual(sendStamp(service.own_client, 1, 64, 0), c.FIPC_OK, "a request");
    var received: c.fipc_result_t = 0;
    _ = t.expect(shared.answerOne(answerer, 64, false, 1000, &received) == 1, "answered");
    _ = t.expect(recvStamp(service.own_client, false, 1000, &received) == replyOf(1), "received");
    return t.done();
}

test "fast: a service's end: a waiting service_wait_for_members is DISCONNECTED, and so is every later one" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    var service: shared.Service = .{};
    const name = try shared.openService(&t, &service, &buf, "sv_count_end", 4096, 64, 2, 1, .{ .client = true });
    defer service.close();
    const answerer = try joined(&t, name, server);
    defer c.fipc_close(answerer);
    // The service ends as its owner's process would: a member's watcher marks the segment, and the owner's poller sees
    // it at a member's end
    var count: u32 = 99;
    var result: c.fipc_result_t = c.FIPC_OK;
    const waiter = try std.Thread.spawn(.{}, waitForMembers, .{ service.handle, client, 2, &count, &result });
    support.sleepMs(50);
    shared.layoutOf(answerer).header.ended.store(1, .seq_cst);
    c.fipc_close(service.own_client);
    service.own_client = null;
    waiter.join();
    _ = t.expectEqual(result, c.FIPC_DISCONNECTED, "the waiting count is DISCONNECTED");
    _ = t.expectEqual(c.fipc_service_wait_for_members(service.handle, client, 2, &count, c.FIPC_FOREVER), c.FIPC_DISCONNECTED, "a later wait too");
    return t.done();
}

/// A service's whole life through the native API: the owner's client and server, both joined on the owner's thread, a
/// call through, every close.
fn nativeCycle(t: *support.Test, name: []const u8) !void {
    const service = try fastipc.serviceCreate(std.testing.allocator, name, 4096, 128, 2, 2);
    const asker = try fastipc.serviceJoin(std.testing.allocator, name, .client, support.within(5000));
    const answerer = try fastipc.serviceJoin(std.testing.allocator, name, .server, support.within(5000));
    _ = t.expectEqual(try service.waitForMembers(null, 2, support.within(0)), 2, "both are counted");
    _ = t.expectEqual(asker.maxPiece(), 128, "max_message");
    try asker.send("call", support.within(1000));
    var buf: [16]u8 = undefined;
    try t.check(std.mem.eql(u8, buf[0..try answerer.recv(&buf, support.within(1000))], "call"), "the request");
    try answerer.send("answer", support.within(0));
    try t.check(std.mem.eql(u8, buf[0..try asker.recv(&buf, support.within(1000))], "answer"), "the reply");
    _ = t.expect(!asker.ended(), "not ended");
    service.close();
    _ = t.expect(asker.ended() and answerer.ended(), "both see the end at once");
    try t.check(if (asker.recv(&buf, support.within(5000))) |_| false else |err| err == error.Disconnected, "the client's end");
    try t.check(if (answerer.recv(&buf, support.within(5000))) |_| false else |err| err == error.Disconnected, "the server's end");
    answerer.close();
    asker.close();
}

test "fast: nothing is left after a service's close: its name is free at once, its memory and handles are back" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    const name = support.uniqueName(&buf, "sv_cycle");
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
