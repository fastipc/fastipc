//! The zero-copy calls: `fipc_send_acquire` reserves one piece of the ring, waiting for room up to its timeout, and
//! `fipc_send_commit` sends what the caller wrote into it; `fipc_recv_acquire` hands out one piece in place, and
//! `fipc_recv_release` frees it. A commit must follow a reservation and fit it, and a message holds a byte at least.

const std = @import("std");
const ts = @import("support.zig");
const c = ts.c;

/// A frame's header in the ring.
const hdr_len = 16;

// A zero-copy send waits for its room, up to its timeout, and takes one piece at most (fipc_max_piece, the same on both
// sides of a connection); a release without an acquire does nothing.
test "fast: a send reservation waits for room and takes one piece at most" {
    var t = ts.begin(@src(), 30);
    defer t.end();
    var p: ts.Pair = .{};
    try ts.openPair(&t, &p, "acquire_room", 1024);
    defer p.close();
    _ = t.expectEq(c.fipc_max_piece(p.server), 1024 - 64, "fipc_max_piece: the capacity less 64");
    _ = t.expectEq(c.fipc_max_piece(p.client), 1024 - 64, "the client's too: the server's capacity");
    var out: ?*anyopaque = null;
    _ = t.expectEq(c.fipc_send_acquire(p.server, 1024 - 63, &out, 0), c.FIPC_TOO_LARGE, "one byte over fipc_max_piece: TOO_LARGE");
    _ = t.expectEq(c.fipc_send_acquire(p.server, 0, &out, 0), c.FIPC_INVALID, "an empty reservation: INVALID");
    try t.checkEq(c.fipc_send_acquire(p.server, 1024 - 64, &out, 0), c.FIPC_OK, "fipc_max_piece fits an empty ring");
    try t.checkEq(c.fipc_send_commit(p.server, 1024 - 64), c.FIPC_OK, "committed");
    const start = ts.nowMs();
    _ = t.expectEq(c.fipc_send_acquire(p.server, 100, &out, 50), c.FIPC_TIMEOUT, "no room: TIMEOUT at its timeout");
    _ = t.expect(ts.nowMs() - start < 1000, "promptly");
    const Receiver = struct {
        fn run(conn: ts.Conn) void {
            ts.sleepMs(100);
            var buf: [1024]u8 = undefined;
            var len: usize = 0;
            _ = ts.recv(conn, &buf, &len, 5000);
        }
    };
    const thread = try std.Thread.spawn(.{}, Receiver.run, .{p.client});
    const waited = c.fipc_send_acquire(p.server, 100, &out, 5000);
    thread.join();
    _ = t.expectEq(waited, c.FIPC_OK, "it waits until the receiver makes room");
    @memcpy(@as([*]u8, @ptrCast(out.?))[0..5], "later");
    try t.checkEq(c.fipc_send_commit(p.server, 5), c.FIPC_OK, "committed");
    c.fipc_recv_release(p.client); // nothing acquired: nothing happens
    _ = t.expect(ts.recvIs(p.client, 1000, "later"), "the message arrives");
    return t.done();
}

// A zero-copy receive takes one piece only: a message of several pieces is TOO_LARGE with its length, and stays for
// `fipc_recv`.
test "fast: a zero-copy receive of a message in several pieces is TOO_LARGE" {
    var t = ts.begin(@src(), 30);
    defer t.end();
    var p: ts.Pair = .{};
    try ts.openPair(&t, &p, "acquire_pieces", 1024);
    defer p.close();
    const long = try std.testing.allocator.alloc(u8, 3000);
    defer std.testing.allocator.free(long);
    for (long, 0..) |*byte, i| byte.* = @truncate(i *% 3);
    const Sender = struct {
        fn run(conn: ts.Conn, bytes: []const u8, rc: *c.fipc_result_t) void {
            rc.* = ts.send(conn, bytes, 5000);
        }
    };
    var sent: c.fipc_result_t = 99;
    const thread = try std.Thread.spawn(.{}, Sender.run, .{ p.server, long, &sent });
    var ptr: ?*const anyopaque = null;
    var len: usize = 0;
    const acquired = c.fipc_recv_acquire(p.client, &ptr, &len, 5000);
    const got = try std.testing.allocator.alloc(u8, 3000);
    defer std.testing.allocator.free(got);
    var got_len: usize = 0;
    const received = ts.recv(p.client, got, &got_len, 5000);
    thread.join();
    _ = t.expectEq(acquired, c.FIPC_TOO_LARGE, "a zero-copy receive of a message in pieces: TOO_LARGE");
    _ = t.expectEq(len, 3000, "with its length");
    _ = t.expectEq(received, c.FIPC_OK, "fipc_recv then takes it");
    _ = t.expect(got_len == 3000 and std.mem.eql(u8, got, long), "whole");
    _ = t.expectEq(sent, c.FIPC_OK, "the send completes");
    return t.done();
}

// A commit longer than the reservation is refused (it would send bytes nobody wrote); the reservation stays, and a
// commit that fits succeeds.
test "fast: a commit longer than its reservation is refused" {
    var t = ts.begin(@src(), 30);
    defer t.end();
    var p: ts.Pair = .{};
    try ts.openPair(&t, &p, "commit_too_long", 4096);
    defer p.close();

    var buf: ?*anyopaque = null;
    try t.checkEq(c.fipc_send_acquire(p.server, 64, &buf, 0), c.FIPC_OK, "send_acquire(64)");
    try t.check(buf != null, "a buffer");
    _ = t.expectEq(c.fipc_send_commit(p.server, 4096 - hdr_len + 1), c.FIPC_INVALID, "a commit past the reservation is FIPC_INVALID");
    @memset(@as([*]u8, @ptrCast(buf.?))[0..64], 0xAB);
    _ = t.expectEq(c.fipc_send_commit(p.server, 64), c.FIPC_OK, "send_commit(64) after send_acquire(64) succeeds");
    return t.done();
}

test "fast: a commit without a reservation is refused" {
    var t = ts.begin(@src(), 30);
    defer t.end();
    var p: ts.Pair = .{};
    try ts.openPair(&t, &p, "commit_alone", 4096);
    defer p.close();

    _ = t.expectEq(c.fipc_send_commit(p.server, 16), c.FIPC_INVALID, "send_commit without send_acquire is FIPC_INVALID");
    var buf: ?*anyopaque = null;
    try t.checkEq(c.fipc_send_acquire(p.server, 16, &buf, 0), c.FIPC_OK, "send_acquire");
    @memset(@as([*]u8, @ptrCast(buf.?))[0..16], 0xCD);
    _ = t.expectEq(c.fipc_send_commit(p.server, 16), c.FIPC_OK, "send_commit after send_acquire succeeds");
    return t.done();
}

// A plain message holds at least a byte, so the sender refuses an empty commit, and the message after it arrives. (A
// faulty peer's empty frame is a unit test: src/zig/data/stream_test.zig.)
test "fast: an empty commit is refused, and the next message arrives" {
    var t = ts.begin(@src(), 30);
    defer t.end();
    var p: ts.Pair = .{};
    try ts.openPair(&t, &p, "empty_commit", 4096);
    defer p.close();

    var buffer: ?*anyopaque = null;
    try t.checkEq(c.fipc_send_acquire(p.server, 16, &buffer, 0), c.FIPC_OK, "a reservation");
    _ = t.expectEq(c.fipc_send_commit(p.server, 0), c.FIPC_INVALID, "an empty commit is refused");
    try t.checkEq(ts.send(p.server, "after", 1000), c.FIPC_OK, "the next message");
    _ = t.expect(ts.recvIs(p.client, 1000, "after"), "The message after the empty commit arrives intact");
    return t.done();
}
