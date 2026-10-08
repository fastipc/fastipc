//! A session's end, as the side that stays sees it: once the peer closes or its process exits, sends return
//! DISCONNECTED at once, receives deliver what the peer completed, in order, then DISCONNECTED; a message the peer left
//! unfinished is never delivered; and no message of a session arrives in the next one. (A peer killed: peer_death_test.zig.)

const std = @import("std");
const ts = @import("support.zig");
const c = ts.c;
const Peer = ts.Peer;

const RING_BYTES = 4096;

// After the peer closes, sends and zero-copy reservations return DISCONNECTED at once, and receives drain what the
// peer completed, then return DISCONNECTED. The first message is acquired zero-copy before the peer leaves; the
// receive after it releases it first, so the drain takes the other 99.
test "fast: after the peer closes, sends fail and receives drain what it sent" {
    var t = ts.begin(@src(), 30);
    defer t.end();
    var p: ts.Pair = .{};
    try ts.openPair(&t, &p, "drain_after_close", RING_BYTES);
    defer p.close();

    // 100 small messages from the server's side; the client acquires the first
    var msg: [16]u8 = undefined;
    for (0..100) |i| {
        @memset(&msg, @truncate(i));
        try t.checkEq(ts.send(p.server, &msg, 1000), c.FIPC_OK, "Send before the peer left");
    }
    var first: ?*const anyopaque = null;
    var first_len: usize = 0;
    try t.checkEq(c.fipc_recv_acquire(p.client, &first, &first_len, 1000), c.FIPC_OK, "acquire before the peer left");
    _ = t.expect(first_len == 16 and @as([*]const u8, @ptrCast(first.?))[0] == 0, "the first message");

    c.fipc_close(p.server);
    p.server = null;
    _ = t.expect(ts.waitForEnd(p.client, 5000), "The client learns that the server's side left");

    // Nothing more is written for a peer that is gone
    _ = t.expectEq(ts.send(p.client, &msg, 0), c.FIPC_DISCONNECTED, "send after the peer left returns DISCONNECTED");
    var wb: ?*anyopaque = null;
    _ = t.expectEq(c.fipc_send_acquire(p.client, 16, &wb, 0), c.FIPC_DISCONNECTED, "send_acquire after the peer left returns DISCONNECTED");

    // What the peer completed is delivered, in order, then the end
    var drained: usize = 0;
    var in_order = true;
    while (drained < 99) : (drained += 1) {
        var got: [16]u8 = undefined;
        var len: usize = 0;
        if (ts.recv(p.client, &got, &len, 0) != c.FIPC_OK) break;
        if (len != 16 or got[0] != @as(u8, @truncate(drained + 1))) in_order = false;
    }
    var got: [16]u8 = undefined;
    var len: usize = 0;
    _ = t.expectEq(drained, 99, "recv drains the other 99 messages the peer completed");
    _ = t.expect(in_order, "they arrive intact and in order");
    _ = t.expectEq(ts.recv(p.client, &got, &len, 0), c.FIPC_DISCONNECTED, "then recv returns DISCONNECTED");
    return t.done();
}

// The peer's close in the middle of a message (its send cancelled after the first piece): the receive that starts it
// returns DISCONNECTED, and nothing of it is delivered.
test "fast: the peer's close in the middle of a message: nothing of it arrives" {
    var t = ts.begin(@src(), 30);
    defer t.end();
    var p: ts.Pair = .{};
    try ts.openPair(&t, &p, "close_mid_message", RING_BYTES);
    defer p.close();
    var long: [16384]u8 = undefined;
    ts.pattern(&long, 5);

    const Send = struct {
        fn run(conn: ts.Conn, bytes: []const u8) void {
            _ = ts.send(conn, bytes, -1);
        }
    };
    const sender = try std.Thread.spawn(.{}, Send.run, .{ p.server, @as([]const u8, &long) });
    var ptr: ?*const anyopaque = null;
    var len: usize = 0;
    _ = t.expectEq(c.fipc_recv_acquire(p.client, &ptr, &len, 5000), c.FIPC_TOO_LARGE, "the message's first piece is there");
    c.fipc_cancel(p.server);
    sender.join();
    c.fipc_close(p.server);
    p.server = null;

    var got: [16384]u8 = undefined;
    _ = t.expectEq(ts.recv(p.client, &got, &len, 5000), c.FIPC_DISCONNECTED, "the receive that starts it: DISCONNECTED");
    _ = t.expectEq(ts.recv(p.client, &got, &len, 5000), c.FIPC_DISCONNECTED, "and the next one");
    return t.done();
}

// The peer sends 100 messages and exits without closing anything: all 100 arrive, then DISCONNECTED; a send after the
// exit and a zero-copy reservation return DISCONNECTED at once.
test "slow: after the peer's process exits, receives drain all it sent" {
    var t = ts.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    const name = ts.name(&buf, "drain_after_exit");
    var listener: ts.Listener = null;
    try t.checkEq(c.fipc_listen(name, RING_BYTES, &listener), c.FIPC_OK, "listen");
    defer c.fipc_listener_close(listener);

    const peer = Peer.spawn(&.{ "send-exit", name, "100" }) catch return t.fail("Failed to launch the peer");
    defer peer.deinit();
    var conn: ts.Conn = null;
    try t.checkEq(c.fipc_accept(listener, &conn, 10000), c.FIPC_OK, "they pair");
    defer c.fipc_close(conn);
    try t.check(peer.waitFor("S", 10000), "the peer sends its messages");
    try t.check(peer.exitedWith(0), "the peer exits");
    try t.check(ts.waitForEnd(conn, 5000), "this side learns of the exit");

    const start = ts.nowMs();
    const send_rc = ts.send(conn, "late", -1);
    var wb: ?*anyopaque = null;
    const wb_rc = c.fipc_send_acquire(conn, 16, &wb, -1);
    const refused_ms = ts.nowMs() - start;
    _ = t.expectEq(send_rc, c.FIPC_DISCONNECTED, "A send after the exit: DISCONNECTED");
    _ = t.expectEq(wb_rc, c.FIPC_DISCONNECTED, "A zero-copy reservation after the exit: DISCONNECTED");
    _ = t.expect(refused_ms < 100, "at once");

    var received: usize = 0;
    var intact = true;
    while (received < 100) : (received += 1) {
        var got: [16]u8 = undefined;
        var len: usize = 0;
        if (ts.recv(conn, &got, &len, 1000) != c.FIPC_OK) break;
        if (len != 16 or got[0] != @as(u8, @truncate(received)) or got[15] != @as(u8, @truncate(received))) intact = false;
    }
    var got: [16]u8 = undefined;
    var len: usize = 0;
    const end_rc = ts.recv(conn, &got, &len, 1000);
    _ = t.expectEq(received, 100, "All 100 messages arrive");
    _ = t.expect(intact, "intact and in order");
    _ = t.expectEq(end_rc, c.FIPC_DISCONNECTED, "then DISCONNECTED");
    return t.done();
}

// No message of a session arrives in the next:
// 1. A client peer connects to this process's listener, sends 100 messages and exits without closing anything; this
//    process accepts it and doesn't receive.
// 2. This process learns of the end and closes that connection; a new client connects, with a fresh segment, and this
//    process accepts it.
// 3. recv gets none of the old messages; the new client's 5 fresh messages all arrive intact.
test "slow: no message of an old session arrives in the next" {
    var t = ts.begin(@src(), 60);
    defer t.end();
    var buf: [64]u8 = undefined;
    const name = ts.name(&buf, "no_stale");
    var listener: ts.Listener = null;
    try t.checkEq(c.fipc_listen(name, RING_BYTES, &listener), c.FIPC_OK, "listen");
    defer c.fipc_listener_close(listener);

    {
        const client1 = Peer.spawn(&.{ "send-exit", name, "100" }) catch return t.fail("Failed to launch the first client");
        defer client1.deinit();
        var conn: ts.Conn = null;
        try t.checkEq(c.fipc_accept(listener, &conn, 10000), c.FIPC_OK, "accept (the first client)");
        defer c.fipc_close(conn);
        try t.check(client1.exitedWith(0), "The first client sends its messages and exits");
        _ = t.expect(ts.waitForEnd(conn, 5000), "This side learns of the end");
    }

    const client2 = Peer.spawn(&.{ "send-fresh", name }) catch return t.fail("Failed to launch the second client");
    defer client2.deinit();
    var conn: ts.Conn = null;
    try t.checkEq(c.fipc_accept(listener, &conn, 10000), c.FIPC_OK, "accept (the second client)");
    defer c.fipc_close(conn);
    _ = client2.waitFor("C", 10000);

    // The old messages stayed in the old session's segment, which the new session doesn't share. Try more than 100 so
    // no old message can leak.
    var msgs_received: usize = 0;
    for (0..110) |_| {
        var got: [16]u8 = undefined;
        var len: usize = 0;
        const rc = ts.recv(conn, &got, &len, 1000);
        if (rc != c.FIPC_OK) break;
        msgs_received += 1;
        const expected_byte: u8 = 0xAA +% @as(u8, @truncate(msgs_received - 1));
        if (len != 16 or got[0] != expected_byte) {
            std.debug.print("  Message {d}: expected 0x{X:0>2}, got 0x{X:0>2}\n", .{ msgs_received, expected_byte, got[0] });
            return t.fail("Received a stale message from the old session");
        }
        if (msgs_received == 5) break;
    }
    _ = t.expectEq(msgs_received, 5, "Exactly the 5 new messages arrive");
    _ = t.expect(client2.exitedWith(0), "The new client closes and exits");
    return t.done();
}
