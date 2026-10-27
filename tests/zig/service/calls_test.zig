//! A service's calls through the C API, in one process: clients and servers on threads (each request answered once,
//! each client's replies in the order of its requests), a pool of servers, requests in flight, a client's room and a
//! server that never waits for it, a server that receives on one thread and replies on another, the places of each
//! role, and the cancels.

const std = @import("std");
const support = @import("../support.zig");
const shared = @import("shared.zig");
const c = support.c;

const client = shared.client;
const server = shared.server;
const joined = shared.joined;
const sendStamp = shared.sendStamp;
const recvStamp = shared.recvStamp;
const replyOf = shared.replyOf;
const seqOf = shared.seqOf;

/// `clients` client threads, each asking `count` requests with `depth` in flight, and `servers` server threads, on a
/// service of `capacity`-byte rings and `max_message`; every reply its request's, in order, every request answered once.
fn callsOnThreads(t: *support.Test, comptime prefix: []const u8, capacity: usize, max_message: usize, clients: u32, servers: u32, count: u64, depth: u64) !void {
    var buf: [64]u8 = undefined;
    var service: shared.Service = .{};
    const name = try shared.openService(t, &service, &buf, prefix, capacity, max_message, clients, servers, .{});
    defer service.close();
    var answerers: [8]shared.Answerer = undefined;
    for (answerers[0..servers], 0..) |*answerer, i| answerer.* = .{ .conn = try joined(t, name, server), .max_message = max_message, .in_place = i % 2 == 1 };
    defer for (answerers[0..servers]) |answerer| c.fipc_close(answerer.conn);
    var askers: [8]shared.Asker = undefined;
    for (askers[0..clients], 0..) |*asker, i| {
        asker.* = .{ .conn = try joined(t, name, client), .client_index = i, .count = count, .depth = depth, .max_message = max_message, .in_place = i % 2 == 0 };
    }
    defer for (askers[0..clients]) |asker| c.fipc_close(asker.conn);
    for (answerers[0..servers]) |*answerer| try answerer.start();
    for (askers[0..clients]) |*asker| try asker.start();
    for (askers[0..clients]) |*asker| _ = t.expectEqual(asker.join(), c.FIPC_OK, "a client's calls");
    // The servers end at the service's close
    c.fipc_service_close(service.handle);
    service.handle = null;
    var answered: u64 = 0;
    for (answerers[0..servers]) |*answerer| {
        _ = t.expectEqual(answerer.join(), c.FIPC_DISCONNECTED, "a server's end");
        _ = t.expectEqual(answerer.bad.load(.monotonic), 0, "every request whole");
        answered += answerer.answered.load(.monotonic);
    }
    for (askers[0..clients]) |asker| {
        _ = t.expectEqual(asker.received, count, "every reply");
        _ = t.expectEqual(asker.wrong, 0, "each its request's, in order");
    }
    _ = t.expectEqual(answered, clients * count, "every request answered once");
}

test "fast: one server and client threads: each request answered once, each client's replies in order" {
    var t = try support.begin(@src(), 60);
    defer t.end();
    try callsOnThreads(&t, "sv_one_server", 65536, 512, 4, 1, 2000, 1);
    return t.done();
}

test "fast: a pool of servers: clients with requests in flight, the servers sharing them" {
    var t = try support.begin(@src(), 60);
    defer t.end();
    try callsOnThreads(&t, "sv_pool", 65536, 256, 4, 4, 2000, 16);
    // Small rings: the clients wait for room their own replies hold
    try callsOnThreads(&t, "sv_pool_small", 4096, 256, 3, 3, 2000, 4);
    return t.done();
}

test "fast: requests in flight: as many as a client's ring holds, then its sends wait for its own replies" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    var service: shared.Service = .{};
    // 4 KiB rings, max_message 64: each request takes 64 + 64 bytes, so 32 fit
    _ = try shared.openService(&t, &service, &buf, "sv_depth", 4096, 64, 1, 1, .{ .client = true, .server = true });
    defer service.close();
    var in_flight: u64 = 0;
    while (support.send(service.own_client, "q", 0) == c.FIPC_OK) in_flight += 1;
    _ = t.expectEqual(in_flight, 32, "32 requests in flight");
    var len: usize = 0;
    var receive_buffer: [64]u8 = undefined;
    for (0..32) |_| {
        _ = t.expectEqual(support.recv(service.own_server, &receive_buffer, &len, 0), c.FIPC_OK, "a request");
        _ = t.expectEqual(support.send(service.own_server, "a", 0), c.FIPC_OK, "its reply");
    }
    _ = t.expectEqual(support.send(service.own_client, "q", 0), c.FIPC_TIMEOUT, "answered, the replies still hold the room");
    const begun = support.nowMs();
    _ = t.expectEqual(support.send(service.own_client, "q", 50), c.FIPC_TIMEOUT, "a send waits out its timeout");
    _ = t.expect(support.nowMs() - begun >= 50, "after its timeout");
    _ = t.expectEqual(support.recv(service.own_client, &receive_buffer, &len, 0), c.FIPC_OK, "a reply received");
    _ = t.expectEqual(support.send(service.own_client, "q", 0), c.FIPC_OK, "makes room for one more");
    return t.done();
}

test "fast: a server never waits for a client: one that never receives fills its ring, and the others are served" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    var service: shared.Service = .{};
    const name = try shared.openService(&t, &service, &buf, "sv_slow_client", 4096, 512, 2, 1, .{ .server = true });
    defer service.close();
    const stuck = try joined(&t, name, client);
    defer c.fipc_close(stuck);
    const other = try joined(&t, name, client);
    defer c.fipc_close(other);
    var result: c.fipc_result_t = 0;
    var sent: u64 = 0;
    while (sendStamp(stuck, sent, 512, 0) == c.FIPC_OK) sent += 1;
    _ = t.expect(sent >= 3, "the stuck client fills its ring");
    for (0..sent) |i| _ = t.expect(shared.answerOne(service.own_server, 512, i % 2 == 0, 1000, &result) == i, "its requests answered");
    for (0..100) |i| {
        try t.checkEqual(sendStamp(other, seqOf(1, i), 512, 0), c.FIPC_OK, "the other client's request");
        try t.check(shared.answerOne(service.own_server, 512, false, 1000, &result) == seqOf(1, i), "answered at once");
        try t.check(recvStamp(other, false, 1000, &result) == replyOf(seqOf(1, i)), "and received");
    }
    _ = t.expect(recvStamp(stuck, false, 1000, &result) == replyOf(0), "the stuck client's first reply is still there");
    return t.done();
}

test "fast: a server receives on one thread and replies on another" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    var service: shared.Service = .{};
    const name = try shared.openService(&t, &service, &buf, "sv_two_threads", 65536, 256, 1, 1, .{ .client = true });
    defer service.close();
    const answerer = try joined(&t, name, server);
    defer c.fipc_close(answerer);
    // The receiving thread hands each request's sequence number to the replying thread
    const Pipe = struct {
        conn: support.Conn,
        seqs: [256]std.atomic.Value(u64) = @splat(.init(0)),
        handed: std.atomic.Value(u64) = .init(0),
        answered: std.atomic.Value(u64) = .init(0),

        fn receive(pipe: *@This(), count: u64) void {
            for (0..count) |i| {
                // One held at a time: the next receive waits for the reply on the other thread
                while (pipe.answered.load(.acquire) != i) std.Thread.yield() catch {};
                var result: c.fipc_result_t = 0;
                const seq = recvStamp(pipe.conn, i % 2 == 0, 5000, &result) orelse return;
                pipe.seqs[i % 256].store(seq, .monotonic);
                pipe.handed.store(i + 1, .release);
            }
        }

        fn reply(pipe: *@This(), count: u64) void {
            for (0..count) |i| {
                while (pipe.handed.load(.acquire) != i + 1) std.Thread.yield() catch {};
                const seq = pipe.seqs[i % 256].load(.monotonic);
                if (sendStamp(pipe.conn, replyOf(seq), 256, 0) != c.FIPC_OK) return;
                pipe.answered.store(i + 1, .release);
            }
        }
    };
    const count = 500;
    var pipe: Pipe = .{ .conn = answerer };
    const receiving = try std.Thread.spawn(.{}, Pipe.receive, .{ &pipe, count });
    const replying = try std.Thread.spawn(.{}, Pipe.reply, .{ &pipe, count });
    var result: c.fipc_result_t = 0;
    for (0..count) |i| {
        try t.checkEqual(sendStamp(service.own_client, i, 256, 5000), c.FIPC_OK, "a request");
        _ = t.expect(recvStamp(service.own_client, false, 5000, &result) == replyOf(i), "its reply");
    }
    receiving.join();
    replying.join();
    _ = t.expectEqual(pipe.answered.load(.acquire), count, "every request answered");
    return t.done();
}

test "fast: the places of each role: full, then free again at a leave; a client's place once its requests were answered" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    var service: shared.Service = .{};
    const name = try shared.openService(&t, &service, &buf, "sv_places", 4096, 64, 1, 1, .{ .server = true });
    defer service.close();
    var conn: support.Conn = null;
    _ = t.expectEqual(c.fipc_service_join(name, server, &conn, 200), c.FIPC_TIMEOUT, "no second server's place");
    const asker = try joined(&t, name, client);
    _ = t.expectEqual(c.fipc_service_join(name, client, &conn, 200), c.FIPC_TIMEOUT, "no second client's place");
    // A client that leaves with a request a server holds: its place drains until the reply
    for (0..3) |i| try t.checkEqual(sendStamp(asker, i, 64, 0), c.FIPC_OK, "a request");
    var result: c.fipc_result_t = 0;
    var len: usize = 0;
    var request: [64]u8 = undefined;
    try t.checkEqual(support.recv(service.own_server, &request, &len, 1000), c.FIPC_OK, "the server takes one");
    c.fipc_close(asker);
    _ = t.expectEqual(c.fipc_service_join(name, client, &conn, 300), c.FIPC_TIMEOUT, "the place drains while a request is held");
    _ = t.expectEqual(support.send(service.own_server, "late", 0), c.FIPC_OK, "a reply to a client that has left: OK");
    const next = try joined(&t, name, client);
    defer c.fipc_close(next);
    // The requests no server took were dropped: the server has nothing more, and the next client starts afresh
    _ = t.expect(recvStamp(service.own_server, false, 100, &result) == null and result == c.FIPC_TIMEOUT, "nothing more");
    try t.checkEqual(sendStamp(next, 7, 64, 0), c.FIPC_OK, "the next client's request");
    _ = t.expect(shared.answerOne(service.own_server, 64, false, 1000, &result) == 7, "answered");
    _ = t.expect(recvStamp(next, false, 1000, &result) == replyOf(7), "received");
    // A server's leave frees its place at once
    c.fipc_close(service.own_server);
    service.own_server = null;
    const other = try joined(&t, name, server);
    defer c.fipc_close(other);
    try t.checkEqual(sendStamp(next, 8, 64, 0), c.FIPC_OK, "a request");
    _ = t.expect(shared.answerOne(other, 64, true, 1000, &result) == 8, "the new server answers");
    _ = t.expect(recvStamp(next, true, 1000, &result) == replyOf(8), "received");
    return t.done();
}

test "fast: a cancel ends a client's wait for a reply and for room, and a server's wait for a request; the calls that needn't wait still work" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    var service: shared.Service = .{};
    const name = try shared.openService(&t, &service, &buf, "sv_cancel_calls", 4096, 1024, 2, 2, .{ .client = true, .server = true });
    defer service.close();
    const Waiting = struct {
        fn recv(conn: support.Conn, result: *c.fipc_result_t) void {
            var receive_buffer: [1024]u8 = undefined;
            var len: usize = 0;
            result.* = support.recv(conn, &receive_buffer, &len, c.FIPC_FOREVER);
        }

        fn send(conn: support.Conn, result: *c.fipc_result_t) void {
            result.* = support.send(conn, "q", c.FIPC_FOREVER);
        }
    };
    // A client waiting for a reply, and a server for a request
    const asker = try joined(&t, name, client);
    defer c.fipc_close(asker);
    try t.checkEqual(support.send(asker, "q", 0), c.FIPC_OK, "a request nobody takes");
    const answerer = try joined(&t, name, server);
    defer c.fipc_close(answerer);
    var results: [3]c.fipc_result_t = .{ 0, 0, 0 };
    const client_since = shared.sleepsOf(service.own_client);
    try t.checkEqual(support.send(service.own_client, "q", 0), c.FIPC_OK, "the owner's client's request");
    var len: usize = 0;
    var receive_buffer: [1024]u8 = undefined;
    // Its requests taken by the owner's server and held: the client waits for the reply
    _ = t.expectEqual(support.recv(service.own_server, &receive_buffer, &len, 1000), c.FIPC_OK, "taken");
    const client_thread = try std.Thread.spawn(.{}, Waiting.recv, .{ service.own_client, &results[0] });
    shared.awaitAsleep(service.own_client, client_since);
    c.fipc_cancel(service.own_client);
    client_thread.join();
    _ = t.expectEqual(results[0], c.FIPC_CANCELLED, "the client's wait for a reply");
    // A server whose receive waits: the asker's request is the other server's to take, so take it first
    _ = t.expectEqual(support.recv(answerer, &receive_buffer, &len, 1000), c.FIPC_OK, "the other server takes the asker's request");
    _ = t.expectEqual(support.send(answerer, "a", 0), c.FIPC_OK, "and answers it");
    const server_since = shared.sleepsOf(answerer);
    const server_thread = try std.Thread.spawn(.{}, Waiting.recv, .{ answerer, &results[1] });
    shared.awaitAsleep(answerer, server_since);
    c.fipc_cancel(answerer);
    server_thread.join();
    _ = t.expectEqual(results[1], c.FIPC_CANCELLED, "the server's wait for a request");
    // A client waiting for room
    for (0..2) |_| _ = support.send(asker, "q", 0);
    _ = t.expectEqual(support.send(asker, "q", 0), c.FIPC_TIMEOUT, "its ring is full");
    const room_since = shared.roomSleepsOf(asker);
    const room_thread = try std.Thread.spawn(.{}, Waiting.send, .{ asker, &results[2] });
    while (shared.roomSleepsOf(asker) == room_since) std.Thread.yield() catch {};
    c.fipc_cancel(asker);
    room_thread.join();
    _ = t.expectEqual(results[2], c.FIPC_CANCELLED, "the client's wait for room");
    // What needn't wait still works after the cancel: the owner's server replies, its client receives the reply there
    _ = t.expectEqual(support.send(service.own_server, "a", 0), c.FIPC_OK, "a reply after the server's own client's cancel");
    _ = t.expectEqual(support.recv(service.own_client, &receive_buffer, &len, 0), c.FIPC_OK, "a reply that is there");
    _ = t.expectEqual(support.recv(service.own_client, &receive_buffer, &len, 1000), c.FIPC_CANCELLED, "one that would wait");
    _ = t.expectEqual(support.recv(asker, &receive_buffer, &len, 0), c.FIPC_OK, "the asker's reply");
    return t.done();
}
