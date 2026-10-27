//! A service's end, and a member's, through the C API, in one process: the service's close, the end's mark a member's
//! watcher stores, a server's close holding a request, a client's close with requests pending, a member's own failure
//! (a frame from the owner), a member the owner drops (a server holding a request too), and the timeouts.

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

fn recvWaiting(conn: support.Conn, result: *c.fipc_result_t) void {
    var buf: [256]u8 = undefined;
    var len: usize = 0;
    result.* = support.recv(conn, &buf, &len, c.FIPC_FOREVER);
}

/// Polls a receive of `holder`, a server holding a request, every 5 ms for up to `timeout_ms`: INVALID while it holds
/// the request and works, DISCONNECTED once it has seen its own failure. The last result.
fn awaitHolderFailure(holder: support.Conn, timeout_ms: u64) c.fipc_result_t {
    const deadline = support.nowMs() + timeout_ms;
    var result: c.fipc_result_t = 0;
    while (true) {
        _ = recvStamp(holder, false, 0, &result);
        if (result != c.FIPC_INVALID or support.nowMs() >= deadline) return result;
        support.sleepMs(5);
    }
}

test "fast: the service's close ends it: clients receive the replies done, in order, then DISCONNECTED; servers DISCONNECTED at once" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    var service: shared.Service = .{};
    const name = try shared.openService(&t, &service, &buf, "sv_end", 4096, 128, 3, 3, .{ .client = true, .server = true });
    defer c.fipc_close(service.own_client);
    defer c.fipc_close(service.own_server);
    const asker = try joined(&t, name, client);
    defer c.fipc_close(asker);
    const holder = try joined(&t, name, server);
    defer c.fipc_close(holder);
    const parked = try joined(&t, name, server);
    defer c.fipc_close(parked);
    // Three requests: the first answered, the second held unanswered, the third answered after it
    var result: c.fipc_result_t = 0;
    try t.checkEqual(sendStamp(asker, 0, 128, 0), c.FIPC_OK, "request 0");
    try t.check(shared.answerOne(service.own_server, 128, false, 1000, &result) == 0, "answered");
    try t.checkEqual(sendStamp(asker, 1, 128, 0), c.FIPC_OK, "request 1");
    try t.check(recvStamp(holder, false, 1000, &result) == 1, "held");
    try t.checkEqual(sendStamp(asker, 2, 128, 0), c.FIPC_OK, "request 2");
    try t.check(shared.answerOne(service.own_server, 128, true, 1000, &result) == 2, "answered");
    try t.checkEqual(sendStamp(asker, 3, 128, 0), c.FIPC_OK, "request 3");
    try t.check(recvStamp(service.own_server, false, 1000, &result) == 3, "held by the owner's server");
    // A server parked with nothing to take, and the owner's client waiting for a reply to a request it never sent:
    // the end wakes both
    var parked_result: c.fipc_result_t = 0;
    const since = shared.sleepsOf(parked);
    const thread = try std.Thread.spawn(.{}, recvWaiting, .{ parked, &parked_result });
    shared.awaitAsleep(parked, since);
    var own_result: c.fipc_result_t = 0;
    const own_since = shared.sleepsOf(service.own_client);
    const own_thread = try std.Thread.spawn(.{}, recvWaiting, .{ service.own_client, &own_result });
    shared.awaitAsleep(service.own_client, own_since);
    c.fipc_service_close(service.handle);
    service.handle = null;
    thread.join();
    own_thread.join();
    _ = t.expectEqual(parked_result, c.FIPC_DISCONNECTED, "the parked server: DISCONNECTED");
    _ = t.expectEqual(own_result, c.FIPC_DISCONNECTED, "the owner's client: DISCONNECTED");
    // The asker: reply 0, then DISCONNECTED at request 1 (reply 2 waits behind it, for nothing)
    _ = t.expect(recvStamp(asker, false, 1000, &result) == replyOf(0), "the reply done before the end");
    _ = t.expect(recvStamp(asker, false, 1000, &result) == null and result == c.FIPC_DISCONNECTED, "then DISCONNECTED");
    _ = t.expectEqual(sendStamp(asker, 4, 128, 0), c.FIPC_DISCONNECTED, "its sends");
    // The servers: their replies and receives
    _ = t.expectEqual(sendStamp(holder, replyOf(1), 128, 0), c.FIPC_DISCONNECTED, "a reply after the end");
    _ = t.expect(recvStamp(holder, false, 0, &result) == null and result == c.FIPC_DISCONNECTED, "a receive after the end");
    try t.check(support.freeAtOnce(name), "the name is free");
    return t.done();
}

test "fast: once the service's end is marked in its segment, sends and receives are DISCONNECTED and no joiner gets in" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    var service: shared.Service = .{};
    const name = try shared.openService(&t, &service, &buf, "sv_marked", 4096, 64, 2, 2, .{ .client = true, .server = true });
    defer service.close();
    var result: c.fipc_result_t = 0;
    try t.checkEqual(sendStamp(service.own_client, 0, 64, 0), c.FIPC_OK, "a request");
    try t.check(shared.answerOne(service.own_server, 64, false, 1000, &result) == 0, "answered");
    try t.checkEqual(sendStamp(service.own_client, 1, 64, 0), c.FIPC_OK, "another, not taken");
    // What a member's watcher stores when its control connection to the owner fails
    shared.layoutOf(service.own_client).header.ended.store(1, .seq_cst);
    var count: u32 = 0;
    _ = t.expectEqual(c.fipc_service_wait_for_members(service.handle, 0, 4, &count, 1000), c.FIPC_DISCONNECTED, "the owner's count");
    _ = t.expectEqual(sendStamp(service.own_client, 2, 64, 0), c.FIPC_DISCONNECTED, "a send");
    _ = t.expect(recvStamp(service.own_server, false, 1000, &result) == null and result == c.FIPC_DISCONNECTED, "a server's receive: no drain");
    var late: support.Conn = null;
    _ = t.expectEqual(c.fipc_service_join(name, server, &late, 300), c.FIPC_TIMEOUT, "a joiner afterwards never gets a place");
    _ = t.expect(recvStamp(service.own_client, false, 1000, &result) == replyOf(0), "the client receives the reply done");
    _ = t.expect(recvStamp(service.own_client, false, 1000, &result) == null and result == c.FIPC_DISCONNECTED, "then DISCONNECTED");
    return t.done();
}

test "fast: a server's close abandons the request it holds: its client's receive is ABANDONED, and the next request is answered" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    var service: shared.Service = .{};
    const name = try shared.openService(&t, &service, &buf, "sv_abandon_close", 4096, 64, 1, 2, .{ .client = true });
    defer service.close();
    var result: c.fipc_result_t = 0;
    for (0..2) |in_place| {
        const leaving = try joined(&t, name, server);
        try t.checkEqual(sendStamp(service.own_client, in_place, 64, 0), c.FIPC_OK, "a request");
        try t.check(recvStamp(leaving, in_place == 1, 1000, &result) == in_place, "the server takes it");
        c.fipc_close(leaving);
        var len: usize = 99;
        var receive_buffer: [64]u8 = undefined;
        if (in_place == 0) {
            _ = t.expectEqual(support.recv(service.own_client, &receive_buffer, &len, 1000), c.FIPC_ABANDONED, "ABANDONED");
        } else {
            var at: ?*const anyopaque = null;
            _ = t.expectEqual(c.fipc_recv_acquire(service.own_client, &at, &len, 1000), c.FIPC_ABANDONED, "ABANDONED, in place too");
            c.fipc_recv_release(service.own_client); // nothing held
        }
        _ = t.expectEqual(len, 0, "with no reply");
    }
    const answerer = try joined(&t, name, server);
    defer c.fipc_close(answerer);
    try t.checkEqual(sendStamp(service.own_client, 7, 64, 0), c.FIPC_OK, "the next request");
    _ = t.expect(shared.answerOne(answerer, 64, false, 1000, &result) == 7, "answered");
    _ = t.expect(recvStamp(service.own_client, false, 1000, &result) == replyOf(7), "its reply, the next received");
    return t.done();
}

test "fast: a member's own failure (a frame from its owner): its calls are DISCONNECTED at once; the service goes on; its close frees its place" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    var service: shared.Service = .{};
    const name = try shared.openService(&t, &service, &buf, "sv_own_failure", 4096, 64, 2, 2, .{});
    defer service.close();
    const asker = try joined(&t, name, client);
    const answerer = try joined(&t, name, server);
    var result: c.fipc_result_t = 0;
    try t.checkEqual(sendStamp(asker, 0, 64, 0), c.FIPC_OK, "a request");
    try t.check(shared.answerOne(answerer, 64, false, 1000, &result) == 0, "answered");
    try t.checkEqual(sendStamp(asker, 1, 64, 0), c.FIPC_OK, "a second, held");
    try t.check(recvStamp(answerer, false, 1000, &result) == 1, "taken");
    // The owner sends a frame on every member's connection: each member's watcher takes it for its own failure, each
    // in its own time
    fastipc.test_hooks.sendServiceStray(service.handle.?);
    _ = t.expect(support.waitForEnd(asker, 5000), "the client's sends are DISCONNECTED");
    _ = t.expect(recvStamp(asker, false, 1000, &result) == null and result == c.FIPC_DISCONNECTED, "its receive too, its reply done or not");
    _ = t.expectEqual(awaitHolderFailure(answerer, 5000), c.FIPC_DISCONNECTED, "the server's receive");
    _ = t.expectEqual(sendStamp(answerer, replyOf(1), 64, 0), c.FIPC_DISCONNECTED, "the server's reply");
    _ = t.expectEqual(shared.layoutOf(asker).header.ended.load(.seq_cst), 0, "the service hasn't ended");
    // New members join and call: the service goes on
    const next_asker = try joined(&t, name, client);
    defer c.fipc_close(next_asker);
    const next_answerer = try joined(&t, name, server);
    defer c.fipc_close(next_answerer);
    try t.checkEqual(sendStamp(next_asker, 5, 64, 0), c.FIPC_OK, "a new client's request");
    _ = t.expect(shared.answerOne(next_answerer, 64, false, 1000, &result) == 5, "a new server answers");
    _ = t.expect(recvStamp(next_asker, false, 1000, &result) == replyOf(5), "received");
    // The failed members' closes free their places (the server's close abandons the request it held): new ones join
    c.fipc_close(asker);
    c.fipc_close(answerer);
    _ = t.expect(shared.awaitCount(service.handle, 0, 2, 5000), "the owner saw both ends");
    const third = try joined(&t, name, server);
    c.fipc_close(third);
    const fourth = try joined(&t, name, client);
    c.fipc_close(fourth);
    return t.done();
}

test "fast: a member the owner drops for a frame keeps its place out of use while the service lasts, and still works" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    var service: shared.Service = .{};
    const name = try shared.openService(&t, &service, &buf, "sv_retired", 4096, 64, 1, 1, .{ .client = true });
    defer service.close();
    const answerer = try joined(&t, name, server);
    defer c.fipc_close(answerer);
    fastipc.test_hooks.sendStray(answerer.?);
    // Its place is retired: no server joins in its stead
    var other: support.Conn = null;
    _ = t.expectEqual(c.fipc_service_join(name, server, &other, 400), c.FIPC_TIMEOUT, "another server finds no place");
    // The dropped server may still run: it does
    var result: c.fipc_result_t = 0;
    try t.checkEqual(sendStamp(service.own_client, 1, 64, 0), c.FIPC_OK, "a request");
    _ = t.expect(shared.answerOne(answerer, 64, false, 1000, &result) == 1, "the dropped server answers it");
    _ = t.expect(recvStamp(service.own_client, false, 1000, &result) == replyOf(1), "received");
    return t.done();
}

test "fast: a server the owner drops for a frame while it holds a request: the request stays its own, and its reply is received" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    var service: shared.Service = .{};
    const name = try shared.openService(&t, &service, &buf, "sv_retired_holding", 4096, 64, 1, 1, .{ .client = true });
    defer service.close();
    var holder = try joined(&t, name, server);
    defer c.fipc_close(holder);
    try t.checkEqual(sendStamp(service.own_client, 1, 64, 0), c.FIPC_OK, "a request");
    var len: usize = 0;
    var request: [64]u8 = undefined;
    try t.checkEqual(support.recv(holder, &request, &len, 1000), c.FIPC_OK, "the server holds it");
    fastipc.test_hooks.sendStray(holder.?);
    try t.check(shared.awaitCount(service.handle, server, 0, 5000), "the owner dropped it");
    // The server may still run, and reply: the owner abandons nothing while its connection is open
    var result: c.fipc_result_t = 0;
    _ = t.expect(recvStamp(service.own_client, false, 200, &result) == null and result == c.FIPC_TIMEOUT, "nothing abandoned");
    try t.checkEqual(sendStamp(holder, replyOf(support.Stamp.check(request[0..len]).?), 64, 0), c.FIPC_OK, "its reply");
    _ = t.expect(recvStamp(service.own_client, false, 1000, &result) == replyOf(1), "received");
    // Its close abandons a request it holds, and the owner, at the end of its connection, finds nothing to abandon
    try t.checkEqual(sendStamp(service.own_client, 2, 64, 0), c.FIPC_OK, "the next request");
    try t.checkEqual(support.recv(holder, &request, &len, 1000), c.FIPC_OK, "the server holds it");
    c.fipc_close(holder);
    holder = null;
    _ = t.expectEqual(support.recv(service.own_client, &request, &len, 1000), c.FIPC_ABANDONED, "ABANDONED once");
    _ = t.expect(recvStamp(service.own_client, false, 200, &result) == null and result == c.FIPC_TIMEOUT, "nothing more");
    return t.done();
}

test "fast: the timeouts of send, receive and join" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    var service: shared.Service = .{};
    const name = try shared.openService(&t, &service, &buf, "sv_timeouts", 4096, 1024, 1, 1, .{ .client = true, .server = true });
    defer service.close();
    var receive_buffer: [1024]u8 = undefined;
    var len: usize = 0;
    for ([_]support.Conn{ service.own_client, service.own_server }) |member| {
        for ([_]c_int{ 0, 50 }) |timeout_ms| {
            const begun = support.nowMs();
            _ = t.expectEqual(support.recv(member, &receive_buffer, &len, timeout_ms), c.FIPC_TIMEOUT, "a receive with nothing to take");
            _ = t.expect(support.nowMs() - begun >= @as(u64, @intCast(timeout_ms)) and support.nowMs() - begun < 2000, "after its timeout");
        }
    }
    // Each request takes 64 bytes and its reply's room of 1024: three fit
    for (0..3) |_| try t.checkEqual(support.send(service.own_client, "q", 0), c.FIPC_OK, "a request");
    for ([_]c_int{ 0, 50 }) |timeout_ms| {
        const begun = support.nowMs();
        _ = t.expectEqual(support.send(service.own_client, "q", timeout_ms), c.FIPC_TIMEOUT, "a send without room");
        _ = t.expect(support.nowMs() - begun >= @as(u64, @intCast(timeout_ms)) and support.nowMs() - begun < 2000, "after its timeout");
    }
    var conn: support.Conn = null;
    const begun = support.nowMs();
    _ = t.expectEqual(c.fipc_service_join(name, client, &conn, 100), c.FIPC_TIMEOUT, "a join with every place held");
    _ = t.expect(support.nowMs() - begun >= 100, "after its timeout");
    return t.done();
}
