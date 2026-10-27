//! A service's members in other processes (peer.zig), in the slow tier: client processes and servers on threads; a
//! server process killed holding a request (its client's receive is ABANDONED, once, and its connection goes on),
//! also after the owner dropped it for a frame, or crashed inside its receive or its reply (its client asleep, which
//! the owner wakes); a client process killed with requests pending (the ones no server took go with it), or crashed
//! in its send; the owner's process killed; and a crash at each step of a join on either side.

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

/// Waits until `count` members of `role` are joined (processes that join), each within 10 s.
fn awaitMembers(t: *support.Test, service: *shared.Service, role: c_int, count: u32) !void {
    var joined_count: u32 = 0;
    const result = c.fipc_service_wait_for_members(service.handle, role, count, &joined_count, @intCast(10_000 * count));
    try t.checkEqual(result, c.FIPC_OK, "the member processes join");
}

test "slow: client processes and servers on threads: each client gets every reply, in the order of its requests" {
    var t = try support.begin(@src(), 120);
    defer t.end();
    var buf: [64]u8 = undefined;
    var service: shared.Service = .{};
    const name = try shared.openService(&t, &service, &buf, "sv_processes", 16384, 256, 3, 3, .{ .server = true });
    defer service.close();
    var answerers: [3]shared.Answerer = undefined;
    for (&answerers, 0..) |*answerer, i| {
        const conn = if (i == 0) service.own_server else try joined(&t, name, server);
        answerer.* = .{ .conn = conn, .max_message = 256, .in_place = i == 1, .timeout_ms = 3000 };
    }
    defer for (answerers[1..]) |answerer| c.fipc_close(answerer.conn);
    for (&answerers) |*answerer| try answerer.start();
    var peers: [3]*support.Peer = undefined;
    var spawned: usize = 0;
    defer for (peers[0..spawned]) |peer| peer.deinit();
    for (&peers, 0..) |*peer, i| {
        var index_buf: [4]u8 = undefined;
        peer.* = try support.Peer.spawn(&.{ "service-ask", name, try std.fmt.bufPrint(&index_buf, "{d}", .{i}), "3000", "8" });
        spawned += 1;
    }
    for (peers) |peer| _ = t.expect(peer.waitFor("DONE", 60_000), "a client got every reply, in order");
    for (peers) |peer| peer.send("x");
    for (peers) |peer| _ = t.expect(peer.waitTimeout(10_000) and peer.exitedWith(0), "a client closed");
    // The servers end once nothing more comes for 3 s
    var answered: u64 = 0;
    for (&answerers) |*answerer| {
        _ = t.expectEqual(answerer.join(), c.FIPC_TIMEOUT, "a server's end");
        _ = t.expectEqual(answerer.bad.load(.monotonic), 0, "every request whole");
        answered += answerer.answered.load(.monotonic);
    }
    _ = t.expectEqual(answered, 9000, "every request answered once");
    return t.done();
}

test "slow: a server process killed holding a request: its client's receive is ABANDONED, once, and the next reply follows" {
    var t = try support.begin(@src(), 60);
    defer t.end();
    var buf: [64]u8 = undefined;
    var service: shared.Service = .{};
    const name = try shared.openService(&t, &service, &buf, "sv_kill_server", 4096, 64, 1, 2, .{ .client = true });
    defer service.close();
    for ([_][]const u8{ "server", "server-inplace" }) |how| {
        const holder = try support.Peer.spawn(&.{ "service-hold", name, how });
        defer holder.deinit();
        try awaitMembers(&t, &service, server, 1);
        try t.checkEqual(sendStamp(service.own_client, 0, 64, 0), c.FIPC_OK, "a request");
        try t.check(holder.waitFor("R", 10_000), "the server process holds it");
        // A second server takes the next request and replies: its reply waits behind the held request's answer
        const answerer = try joined(&t, name, server);
        defer c.fipc_close(answerer);
        var result: c.fipc_result_t = 0;
        try t.checkEqual(sendStamp(service.own_client, 1, 64, 0), c.FIPC_OK, "the next request");
        try t.check(shared.answerOne(answerer, 64, false, 5000, &result) == 1, "answered");
        _ = t.expect(recvStamp(service.own_client, false, 200, &result) == null and result == c.FIPC_TIMEOUT, "the held request comes first");
        holder.kill();
        var len: usize = 99;
        var receive_buffer: [64]u8 = undefined;
        _ = t.expectEqual(support.recv(service.own_client, &receive_buffer, &len, 5000), c.FIPC_ABANDONED, "the held request: ABANDONED");
        _ = t.expectEqual(len, 0, "with no reply");
        _ = t.expect(recvStamp(service.own_client, false, 1000, &result) == replyOf(1), "then the next reply");
        _ = t.expect(recvStamp(service.own_client, false, 100, &result) == null and result == c.FIPC_TIMEOUT, "nothing more");
        // The killed server's place is free again
        try t.check(shared.awaitCount(service.handle, server, 1, 5000), "the owner saw its end");
    }
    return t.done();
}

test "slow: a server process the owner drops for a frame while it holds a request, then killed: its client's receive is ABANDONED, and the next reply follows" {
    var t = try support.begin(@src(), 60);
    defer t.end();
    var buf: [64]u8 = undefined;
    var service: shared.Service = .{};
    const name = try shared.openService(&t, &service, &buf, "sv_kill_dropped", 4096, 64, 1, 2, .{ .client = true });
    defer service.close();
    const holder = try support.Peer.spawn(&.{ "service-hold", name, "server-stray" });
    defer holder.deinit();
    try awaitMembers(&t, &service, server, 1);
    try t.checkEqual(sendStamp(service.own_client, 0, 64, 0), c.FIPC_OK, "a request");
    try t.check(holder.waitFor("R", 10_000), "the server process holds it, and sent a frame");
    try t.check(shared.awaitCount(service.handle, server, 0, 5000), "the owner dropped it");
    // A second server, in the other place, answers the next request: its reply waits behind the held request's answer
    const answerer = try joined(&t, name, server);
    defer c.fipc_close(answerer);
    var result: c.fipc_result_t = 0;
    try t.checkEqual(sendStamp(service.own_client, 1, 64, 0), c.FIPC_OK, "the next request");
    try t.check(shared.answerOne(answerer, 64, false, 5000, &result) == 1, "answered");
    _ = t.expect(recvStamp(service.own_client, false, 200, &result) == null and result == c.FIPC_TIMEOUT, "the held request comes first");
    holder.kill();
    var len: usize = 99;
    var receive_buffer: [64]u8 = undefined;
    _ = t.expectEqual(support.recv(service.own_client, &receive_buffer, &len, 5000), c.FIPC_ABANDONED, "the held request: ABANDONED");
    _ = t.expectEqual(len, 0, "with no reply");
    _ = t.expect(recvStamp(service.own_client, false, 1000, &result) == replyOf(1), "then the next reply");
    // The dropped server's place stays out of use
    var other: support.Conn = null;
    _ = t.expectEqual(c.fipc_service_join(name, server, &other, 400), c.FIPC_TIMEOUT, "no server gets its place");
    return t.done();
}

/// A client's receive on a thread of its own, and when it returned.
const Receive = struct {
    conn: support.Conn,
    timeout_ms: c_int,
    thread: std.Thread = undefined,
    result: c.fipc_result_t = 0,
    buf: [64]u8 = undefined,
    len: usize = 0,
    returned_ms: u64 = 0,

    fn start(receive: *Receive) !void {
        receive.thread = try std.Thread.spawn(.{}, run, .{receive});
    }

    fn run(receive: *Receive) void {
        receive.result = support.recv(receive.conn, &receive.buf, &receive.len, receive.timeout_ms);
        receive.returned_ms = support.nowMs();
    }
};

/// A server that crashes at `step` while it answers the client's request "crash": the client's receive returns
/// `expected` (ABANDONED, or the reply when the crash came after its done), then the service goes on. A crash in the
/// reply comes once the client's receive is asleep, in sleeps of a minute unless woken: it returns at once only if the
/// owner, at the server's end, woke it (the server that crashed after its done never did; its timeout is 15 s).
fn crashWhileAnswering(t: *support.Test, comptime prefix: []const u8, step: []const u8, expected: c.fipc_result_t) !void {
    var buf: [64]u8 = undefined;
    var service: shared.Service = .{};
    const name = try shared.openService(t, &service, &buf, prefix, 4096, 64, 1, 1, .{ .client = true });
    defer service.close();
    const crashing = try support.Peer.spawn(&.{ "native-service-crash", "server", name, step });
    defer crashing.deinit();
    try t.check(crashing.waitFor("READY", 10_000), "the server joined");
    try t.checkEqual(support.send(service.own_client, "ask", 0), c.FIPC_OK, "a request");
    if (std.mem.eql(u8, step, "service_claimed")) {
        try t.check(crashing.waitTimeout(10_000) and crashing.exitedWith(99), "the server crashed at its claim");
        var len: usize = 0;
        var receive_buffer: [64]u8 = undefined;
        _ = t.expectEqual(support.recv(service.own_client, &receive_buffer, &len, 5000), expected, "the client's receive");
    } else {
        try t.check(crashing.waitFor("R", 10_000), "the server took the request");
        fastipc.test_hooks.setServiceReplySleepMs(service.own_client.?, 60_000);
        var receive: Receive = .{ .conn = service.own_client, .timeout_ms = 15_000 };
        const since = shared.sleepsOf(service.own_client);
        try receive.start();
        shared.awaitAsleep(service.own_client, since);
        const replying_ms = support.nowMs();
        crashing.send("x");
        try t.check(crashing.waitTimeout(10_000) and crashing.exitedWith(99), "the server crashed at its step");
        receive.thread.join();
        _ = t.expectEqual(receive.result, expected, "the client's receive");
        if (expected == c.FIPC_OK) _ = t.expect(std.mem.eql(u8, receive.buf[0..receive.len], "crash"), "the reply, whole");
        _ = t.expect(receive.returned_ms - replying_ms < 5000, "the owner woke the client's receive at the server's end");
    }
    // The next server gets the place, and answers the next request
    const next = try joined(t, name, server);
    defer c.fipc_close(next);
    var result: c.fipc_result_t = 0;
    try t.checkEqual(sendStamp(service.own_client, 7, 64, 0), c.FIPC_OK, "the next request");
    _ = t.expect(shared.answerOne(next, 64, false, 5000, &result) == 7, "answered");
    _ = t.expect(recvStamp(service.own_client, false, 1000, &result) == replyOf(7), "received");
}

test "slow: a server crashed inside fipc_recv after its claim: its client's receive is ABANDONED" {
    var t = try support.begin(@src(), 60);
    defer t.end();
    try crashWhileAnswering(&t, "sv_crash_claimed", "service_claimed", c.FIPC_ABANDONED);
    return t.done();
}

test "slow: a server crashed after its reply's bytes, before its done: ABANDONED; after its done, before its wake: the reply, which the owner wakes" {
    var t = try support.begin(@src(), 60);
    defer t.end();
    try crashWhileAnswering(&t, "sv_crash_replying", "service_replying", c.FIPC_ABANDONED);
    try crashWhileAnswering(&t, "sv_crash_replied", "service_replied", c.FIPC_OK);
    return t.done();
}

test "slow: a client process killed with requests pending: the servers go on, and its place is the next client's once they answered" {
    var t = try support.begin(@src(), 60);
    defer t.end();
    var buf: [64]u8 = undefined;
    var service: shared.Service = .{};
    const name = try shared.openService(&t, &service, &buf, "sv_kill_client", 65536, 256, 1, 1, .{ .server = true });
    defer service.close();
    const asker = try support.Peer.spawn(&.{ "service-hold", name, "client" });
    defer asker.deinit();
    try t.check(asker.waitFor("SENT", 10_000), "the client sent three requests");
    var result: c.fipc_result_t = 0;
    try t.check(recvStamp(service.own_server, false, 5000, &result) == 0, "the server holds the first");
    asker.kill();
    try t.check(shared.awaitCount(service.handle, client, 0, 5000), "the owner saw the client's end");
    var none: support.Conn = null;
    _ = t.expectEqual(c.fipc_service_join(name, client, &none, 300), c.FIPC_TIMEOUT, "its place drains while a request is held");
    _ = t.expectEqual(sendStamp(service.own_server, replyOf(0), 256, 0), c.FIPC_OK, "the reply to the killed client: OK");
    // The requests no server took were dropped
    _ = t.expect(recvStamp(service.own_server, false, 200, &result) == null and result == c.FIPC_TIMEOUT, "nothing more to take");
    const next = try joined(&t, name, client);
    defer c.fipc_close(next);
    try t.checkEqual(sendStamp(next, 9, 256, 0), c.FIPC_OK, "the next client's request");
    _ = t.expect(shared.answerOne(service.own_server, 256, true, 5000, &result) == 9, "answered");
    _ = t.expect(recvStamp(next, false, 5000, &result) == replyOf(9), "received");
    return t.done();
}

test "slow: a client crashed between its tail and its ready bit: its request goes with it, and its place is the next client's" {
    var t = try support.begin(@src(), 60);
    defer t.end();
    var buf: [64]u8 = undefined;
    var service: shared.Service = .{};
    const name = try shared.openService(&t, &service, &buf, "sv_crash_publish", 4096, 64, 1, 1, .{ .server = true });
    defer service.close();
    const crashing = try support.Peer.spawn(&.{ "native-service-crash", "client", name, "service_published" });
    defer crashing.deinit();
    try t.check(crashing.waitTimeout(10_000) and crashing.exitedWith(99), "the client crashed after publishing");
    // At the client's end the owner drops the requests no server took: nobody waits for their replies
    try t.check(shared.awaitCount(service.handle, client, 0, 5000), "the owner saw the client's end");
    var result: c.fipc_result_t = 0;
    _ = t.expect(recvStamp(service.own_server, false, 200, &result) == null and result == c.FIPC_TIMEOUT, "the request was dropped");
    const next = try joined(&t, name, client);
    defer c.fipc_close(next);
    try t.checkEqual(sendStamp(next, 3, 64, 0), c.FIPC_OK, "the next client's request");
    _ = t.expect(shared.answerOne(service.own_server, 64, false, 5000, &result) == 3, "answered");
    _ = t.expect(recvStamp(next, false, 5000, &result) == replyOf(3), "received");
    return t.done();
}

test "slow: the owner's process killed: clients receive the replies done, then DISCONNECTED; servers DISCONNECTED" {
    var t = try support.begin(@src(), 60);
    defer t.end();
    var buf: [64]u8 = undefined;
    const name = support.uniqueName(&buf, "sv_kill_owner");
    defer support.clearName(name);
    const owner = try support.Peer.spawn(&.{ "service-own", name });
    defer owner.deinit();
    try t.check(owner.waitFor("L", 10_000), "the owner listens");
    var asker: support.Conn = null;
    try t.checkEqual(c.fipc_service_join(name, client, &asker, 10_000), c.FIPC_OK, "a client joins");
    defer c.fipc_close(asker);
    var answerer: support.Conn = null;
    try t.checkEqual(c.fipc_service_join(name, server, &answerer, 10_000), c.FIPC_OK, "a server joins");
    defer c.fipc_close(answerer);
    var result: c.fipc_result_t = 0;
    for (0..2) |i| try t.checkEqual(sendStamp(asker, i, 256, 0), c.FIPC_OK, "a request");
    try t.check(shared.answerOne(answerer, 256, false, 5000, &result) == 0, "the first answered");
    try t.check(recvStamp(answerer, false, 5000, &result) == 1, "the second held");
    owner.kill();
    _ = t.expect(support.waitForEnd(asker, 5000), "the client sees the end");
    _ = t.expectEqual(sendStamp(answerer, replyOf(1), 256, 0), c.FIPC_DISCONNECTED, "the server's reply after the end");
    _ = t.expect(recvStamp(asker, false, 5000, &result) == replyOf(0), "the reply done before the end");
    _ = t.expect(recvStamp(asker, false, 5000, &result) == null and result == c.FIPC_DISCONNECTED, "then DISCONNECTED");
    _ = t.expect(recvStamp(answerer, false, 5000, &result) == null and result == c.FIPC_DISCONNECTED, "the server's receive");
    return t.done();
}

test "slow: an owner crashed at each step of an admission (on its poller): the joiner fails or reads the end, and the name is free" {
    var t = try support.begin(@src(), 120);
    defer t.end();
    for ([_][]const u8{ "accepted", "offer_sent", "join_read", "welcome_sent", "member_watching" }) |step| {
        var name_buf: [64]u8 = undefined;
        const name = try std.mem.printSentinel(&name_buf, "sv-crash-own-{s}-{d}", .{ step, support.pid() }, 0);
        defer support.clearName(name);
        const crashing = try support.Peer.spawn(&.{ "native-service-crash", "own", name, step });
        defer crashing.deinit();
        try t.check(crashing.waitFor("OPEN", 10_000), "the crashing owner listens");
        var member: support.Conn = null;
        const result = c.fipc_service_join(name, server, &member, 500);
        std.debug.print("  crash at {s}: the join returned {s}\n", .{ step, support.resultName(result) });
        if (result == c.FIPC_OK) {
            var receive_buffer: [16]u8 = undefined;
            var len: usize = 0;
            _ = t.expectEqual(support.recv(member, &receive_buffer, &len, 5000), c.FIPC_DISCONNECTED, "the server reads the service's end");
            c.fipc_close(member);
        } else _ = t.expectEqual(result, c.FIPC_TIMEOUT, "or the join times out");
        try t.check(crashing.waitTimeout(10_000) and crashing.exitedWith(99), "the owner ended at its crash point");
        var service: shared.Service = .{};
        try t.check(service.open(name, 4096, 64, 1, 1, .{ .client = true }), "the next service on the name");
        defer service.close();
        const next = try joined(&t, name, server);
        c.fipc_close(next);
    }
    return t.done();
}

test "slow: a member crashed at each step of its join: its place is freed, the next gets it" {
    var t = try support.begin(@src(), 180);
    defer t.end();
    var buf: [64]u8 = undefined;
    var service: shared.Service = .{};
    // Besides the owner's client and server, one place of each role
    const name = try shared.openService(&t, &service, &buf, "sv_crash_member", 4096, 64, 2, 2, .{ .client = true, .server = true });
    defer service.close();
    for ([_][]const u8{ "client", "server" }) |role| {
        for ([_][]const u8{ "connected", "offer_received", "join_sent", "welcomed", "start_taken", "watching" }) |step| {
            const crashing = try support.Peer.spawn(&.{ "native-service-crash", role, name, step });
            defer crashing.deinit();
            try t.check(crashing.waitTimeout(10_000) and crashing.exitedWith(99), "the member ended at its crash point");
            // A client that crashed only in its watcher may have sent its request: answered, its place settles
            var result: c.fipc_result_t = 0;
            while (shared.answerOne(service.own_server, 64, false, 0, &result) != null or result == c.FIPC_OK) {}
            // One place of each role: the next gets in only once the crashed one's is free
            const next = try joined(&t, name, if (std.mem.eql(u8, role, "client")) client else server);
            c.fipc_close(next);
        }
    }
    return t.done();
}
