//! Messages through `fipc_send` and `fipc_recv`: they arrive whole, both ways, in order, of any size through any ring;
//! a receive into a buffer too small leaves the message; a call's timeout bounds its wait for the first piece, and a
//! started message completes. (A ping-pong that hunts lost wake-ups: wake_test.zig.)

const std = @import("std");
const ts = @import("support.zig");
const c = ts.c;

/// A receive on another thread, after `delay_ms`.
const Recv = struct {
    conn: ts.Conn,
    buf: []u8,
    delay_ms: u64 = 0,
    len: usize = 0,
    result: c.fipc_result_t = 99,
    thread: std.Thread = undefined,

    fn start(r: *Recv) !void {
        r.thread = try std.Thread.spawn(.{}, run, .{r});
    }

    fn run(r: *Recv) void {
        ts.sleepMs(r.delay_ms);
        r.result = ts.recv(r.conn, r.buf, &r.len, 5000);
    }
};

test "fast: a message arrives intact" {
    var t = ts.begin(@src(), 30);
    defer t.end();
    var p: ts.Pair = .{};
    try ts.openPair(&t, &p, "small_message", 64 * 1024);
    defer p.close();

    var message: [256]u8 = undefined;
    ts.pattern(&message, 1);
    try t.checkEq(ts.send(p.server, &message, 0), c.FIPC_OK, "Send");
    var got: [message.len]u8 = undefined;
    var len: usize = 0;
    const result = ts.recv(p.client, &got, &len, 0);
    if (result != c.FIPC_OK or len != message.len or !std.mem.eql(u8, &got, &message)) return t.fail("Receive failed or data corrupted");
    return t.done();
}

// A plain message holds at least one byte: an empty send is refused.
test "fast: an empty message is refused" {
    var t = ts.begin(@src(), 30);
    defer t.end();
    var p: ts.Pair = .{};
    try ts.openPair(&t, &p, "empty_message", 1024);
    defer p.close();
    if (c.fipc_send(p.server, null, 0, 0) != c.FIPC_INVALID) return t.fail("Zero-length send should fail");
    return t.done();
}

test "fast: messages in a row arrive intact" {
    var t = ts.begin(@src(), 30);
    defer t.end();
    var p: ts.Pair = .{};
    try ts.openPair(&t, &p, "messages_in_a_row", 64 * 1024);
    defer p.close();

    const message_size: usize = 128;
    for (0..5) |i| {
        var message: [message_size]u8 = undefined;
        for (&message, 0..) |*byte, j| byte.* = @truncate(i + j * 7);
        try t.checkEq(ts.send(p.server, &message, 0), c.FIPC_OK, "Send");
        var got: [message_size]u8 = undefined;
        var len: usize = 0;
        const result = ts.recv(p.client, &got, &len, 0);
        if (result != c.FIPC_OK or len != message_size or !std.mem.eql(u8, &got, &message)) return t.fail("a message of the row");
    }
    return t.done();
}

// A connection is duplex: the server's side and the client's each send and receive.
test "fast: messages go both ways" {
    var t = ts.begin(@src(), 30);
    defer t.end();
    var p: ts.Pair = .{};
    try ts.openPair(&t, &p, "both_ways", 4 * 1024);
    defer p.close();
    try t.checkEq(ts.send(p.server, "Hello!", 0), c.FIPC_OK, "Send from the server's side");
    _ = t.expect(ts.recvIs(p.client, 0, "Hello!"), "The client receives it");
    try t.checkEq(ts.send(p.client, "And back!", 0), c.FIPC_OK, "Send from the client's side");
    _ = t.expect(ts.recvIs(p.server, 0, "And back!"), "The server receives it");
    return t.done();
}

// A receive into a buffer too small for the message returns TOO_LARGE with the message's length and takes nothing:
// the next receive with room gets it. A NULL buffer of length 0 is the way to ask for the length. The same for a
// message of several pieces, longer than the ring, whose sender waits for the receiver's room.
test "fast: a receive into a small buffer is TOO_LARGE and leaves the message" {
    var t = ts.begin(@src(), 30);
    defer t.end();
    var p: ts.Pair = .{};
    try ts.openPair(&t, &p, "small_buffer", 4096);
    defer p.close();
    var message: [100]u8 = undefined;
    for (&message, 0..) |*byte, i| byte.* = @truncate(i);
    try t.checkEq(ts.send(p.server, &message, 0), c.FIPC_OK, "a message of 100 bytes");
    var small: [10]u8 = undefined;
    var len: usize = 0;
    _ = t.expectEq(ts.recv(p.client, &small, &len, 100), c.FIPC_TOO_LARGE, "into 10 bytes: TOO_LARGE");
    _ = t.expectEq(len, 100, "with the message's length");
    len = 0;
    _ = t.expectEq(c.fipc_recv(p.client, null, 0, &len, 0), c.FIPC_TOO_LARGE, "a NULL buffer: TOO_LARGE");
    _ = t.expectEq(len, 100, "with the length");
    var got: [100]u8 = undefined;
    _ = t.expectEq(ts.recv(p.client, &got, &len, 0), c.FIPC_OK, "then with room: OK");
    _ = t.expect(len == 100 and std.mem.eql(u8, &got, &message), "the whole message");

    const long = try std.testing.allocator.alloc(u8, 10000);
    defer std.testing.allocator.free(long);
    for (long, 0..) |*byte, i| byte.* = @truncate(i *% 5);
    const Sender = struct {
        fn run(conn: ts.Conn, bytes: []const u8, rc: *c.fipc_result_t) void {
            rc.* = ts.send(conn, bytes, 5000);
        }
    };
    var sent: c.fipc_result_t = 99;
    const thread = try std.Thread.spawn(.{}, Sender.run, .{ p.server, long, &sent });
    const too_large = ts.recv(p.client, &small, &len, 5000);
    const long_len = len;
    const room = try std.testing.allocator.alloc(u8, 10000);
    defer std.testing.allocator.free(room);
    const received = ts.recv(p.client, room, &len, 0);
    thread.join();
    _ = t.expectEq(too_large, c.FIPC_TOO_LARGE, "a message of 10000 bytes into 10: TOO_LARGE");
    _ = t.expectEq(long_len, 10000, "with its length");
    _ = t.expectEq(received, c.FIPC_OK, "then with room: OK");
    _ = t.expect(len == 10000 and std.mem.eql(u8, room, long), "intact");
    _ = t.expectEq(sent, c.FIPC_OK, "the send completes");
    return t.done();
}

// A message of a thousand pieces goes through a 1 KiB ring and arrives whole, copied and through RPC: nothing bounds a
// message's length but memory, and the library allocates nothing for it.
test "fast: a 1 MiB message goes through a 1 KiB ring" {
    var t = ts.begin(@src(), 60);
    defer t.end();
    var p: ts.Pair = .{};
    try ts.openPair(&t, &p, "large_small_ring", 1024);
    defer p.close();
    const len = 1 << 20;
    const message = try std.testing.allocator.alloc(u8, len);
    defer std.testing.allocator.free(message);
    ts.pattern(message, 1);
    const got = try std.testing.allocator.alloc(u8, len);
    defer std.testing.allocator.free(got);

    var receive: Recv = .{ .conn = p.client, .buf = got };
    try receive.start();
    const sent = ts.send(p.server, message, 5000);
    receive.thread.join();
    _ = t.expectEq(sent, c.FIPC_OK, "a message of 1 MiB through a 1 KiB ring");
    _ = t.expectEq(receive.result, c.FIPC_OK, "received");
    _ = t.expect(receive.len == len and std.mem.eql(u8, got, message), "whole");

    const Submit = struct {
        fn run(conn: ts.Conn, bytes: []const u8, rc: *c.fipc_result_t) void {
            var id: u64 = 0;
            rc.* = c.fipc_rpc_submit(conn, 3, bytes.ptr, bytes.len, &id, 5000);
        }
    };
    var submitted: c.fipc_result_t = 99;
    const submitter = try std.Thread.spawn(.{}, Submit.run, .{ p.client, message[0 .. 300 * 1024], &submitted });
    var msg: c.fipc_rpc_msg_t = undefined;
    const received = c.fipc_rpc_recv(p.server, got.ptr, got.len, &msg, 5000);
    submitter.join();
    _ = t.expectEq(submitted, c.FIPC_OK, "a request of 300 KiB through a 1 KiB ring");
    _ = t.expectEq(received, c.FIPC_OK, "received");
    _ = t.expect(msg.len == 300 * 1024 and msg.opcode == 3 and std.mem.eql(u8, got[0 .. 300 * 1024], message[0 .. 300 * 1024]), "whole");
    return t.done();
}

// A send's timeout bounds its wait for room for the first piece. Once that piece is in, the rest follows however long
// the receiver takes, past the timeout, also with a timeout of 0. (Each case on a fresh ring, whose first piece has
// room at once.)
test "fast: a send's timeout bounds only its wait for the first piece" {
    var t = ts.begin(@src(), 30);
    defer t.end();
    var long: [10000]u8 = undefined;
    ts.pattern(&long, 2);
    var got: [10000]u8 = undefined;

    for ([_]c_int{ 50, 0 }) |timeout_ms| {
        var p: ts.Pair = .{};
        try ts.openPair(&t, &p, "send_past_timeout", 1024);
        defer p.close();
        var receive: Recv = .{ .conn = p.client, .buf = &got, .delay_ms = 200 };
        try receive.start();
        const start = ts.nowMs();
        const sent = ts.send(p.server, &long, timeout_ms);
        const elapsed = ts.nowMs() - start;
        receive.thread.join();
        _ = t.expectEq(sent, c.FIPC_OK, "a message of 11 pieces, whose receiver comes after the timeout");
        _ = t.expect(elapsed >= 150, "the send waited for the receiver, past its timeout");
        _ = t.expectEq(receive.result, c.FIPC_OK, "received");
        _ = t.expect(receive.len == long.len and std.mem.eql(u8, &got, &long), "whole");
    }
    // With no room for the first piece, the timeout applies
    var p: ts.Pair = .{};
    try ts.openPair(&t, &p, "send_no_room", 1024);
    defer p.close();
    try t.checkEq(ts.send(p.server, long[0..500], 0), c.FIPC_OK, "a message that leaves no room for a first piece");
    const start = ts.nowMs();
    _ = t.expectEq(ts.send(p.server, &long, 50), c.FIPC_TIMEOUT, "no room for the first piece: TIMEOUT");
    _ = t.expect(ts.nowMs() - start < 1000, "at its timeout");
    _ = t.expect(ts.recvIs(p.client, 1000, long[0..500]), "nothing of the timed-out message was sent");
    var len: usize = 0;
    _ = t.expectEq(ts.recv(p.client, &got, &len, 0), c.FIPC_TIMEOUT, "nothing else");
    return t.done();
}

// A receive with timeout 0 on an empty ring returns TIMEOUT after one look, without spinning first: the fastest of
// five batches of 100 such receives takes well under 2 ms.
test "fast: a receive with timeout 0 looks once" {
    var t = ts.begin(@src(), 30);
    defer t.end();
    var p: ts.Pair = .{};
    try ts.openPair(&t, &p, "recv_look_once", 4096);
    defer p.close();
    var fastest: u64 = std.math.maxInt(u64);
    for (0..5) |_| {
        const start = ts.nowNs();
        for (0..100) |_| {
            var got: [16]u8 = undefined;
            var len: usize = 0;
            if (ts.recv(p.client, &got, &len, 0) != c.FIPC_TIMEOUT) return t.fail("recv(0) on an empty ring: TIMEOUT");
        }
        fastest = @min(fastest, ts.nowNs() - start);
    }
    std.debug.print("  100 receives with timeout 0: {d} us at best\n", .{fastest / 1000});
    _ = t.expect(fastest < 2 * std.time.ns_per_ms, "100 receives with timeout 0 take under 2 ms");
    return t.done();
}
