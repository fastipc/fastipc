//! `fipc_cancel` and the waits it ends: a cancel releases every call of its connection that waits, with CANCELLED,
//! and from then on only the calls that would wait return CANCELLED. A send stopped in the middle of a message drops it
//! whole. A blocked wait sleeps: neither the peer's cancel nor a session before it makes it spin.

const std = @import("std");
const ts = @import("support.zig");
const c = ts.c;
const win = ts.win;

const RING_BYTES = 4096;

/// A send or receive without a timeout on its own thread, and when it returned.
const BlockedCall = struct {
    conn: ts.Conn,
    send: bool,
    result: std.atomic.Value(i32) = .init(-1),
    returned_ns: std.atomic.Value(u64) = .init(0),

    fn run(b: *BlockedCall) void {
        var buf: [16]u8 = undefined;
        var len: usize = 0;
        const rc = if (b.send) ts.send(b.conn, &@as([1000]u8, @splat('x')), -1) else ts.recv(b.conn, &buf, &len, -1);
        b.returned_ns.store(ts.nowNs(), .release);
        b.result.store(@intCast(rc), .release);
    }
};

// A receive(-1) on an empty ring and a send(-1) on a full one, on one connection, are released by a cancel from
// another thread. After a cancel, a send that needn't wait still succeeds, while a receive that would wait returns
// CANCELLED.
test "fast: cancel releases a waiting send and receive, then only calls that would wait" {
    var t = ts.begin(@src(), 30);
    defer t.end();
    var p: ts.Pair = .{};
    try ts.openPair(&t, &p, "cancel", RING_BYTES);
    defer p.close();

    // The client's send ring full: 4 messages of 1000 bytes and their headers
    for (0..4) |_| try t.checkEq(ts.send(p.client, &@as([1000]u8, @splat('x')), 0), c.FIPC_OK, "filling the ring");
    var calls = [_]BlockedCall{ .{ .conn = p.client, .send = false }, .{ .conn = p.client, .send = true } };
    var threads: [2]std.Thread = undefined;
    for (&threads, &calls) |*thread, *call| thread.* = std.Thread.spawn(.{}, BlockedCall.run, .{call}) catch return t.fail("thread");
    ts.sleepMs(100); // both wait
    const cancel_ns = ts.nowNs();
    c.fipc_cancel(p.client);
    for (threads) |thread| thread.join();
    for (calls) |call| {
        const took_ms = (call.returned_ns.load(.acquire) -| cancel_ns) / std.time.ns_per_ms;
        std.debug.print("  {s} returned {s} {d} ms after the cancel\n", .{ if (call.send) "send(-1)" else "recv(-1)", ts.resultName(@intCast(call.result.load(.acquire))), took_ms });
        _ = t.expectEq(call.result.load(.acquire), c.FIPC_CANCELLED, "The waiting call returns CANCELLED");
        _ = t.expect(took_ms < 1000, "promptly");
    }

    var got: [1000]u8 = undefined;
    var len: usize = 0;
    try t.checkEq(ts.recv(p.server, &got, &len, 0), c.FIPC_OK, "the server makes room");
    _ = t.expectEq(ts.send(p.client, "x", -1), c.FIPC_OK, "A send with room in the ring succeeds after cancel");
    _ = t.expectEq(ts.recv(p.client, &got, &len, -1), c.FIPC_CANCELLED, "A receive that would wait returns CANCELLED");
    return t.done();
}

/// A send on another thread, whose result the test looks at once it has joined it.
const Send = struct {
    conn: ts.Conn,
    bytes: []const u8,
    result: c.fipc_result_t = 99,
    thread: std.Thread = undefined,

    fn run(s: *Send) void {
        s.result = ts.send(s.conn, s.bytes, -1);
    }
};

/// A receive on another thread.
const Recv = struct {
    conn: ts.Conn,
    buf: []u8,
    len: usize = 0,
    result: c.fipc_result_t = 99,

    fn run(r: *Recv) void {
        r.result = ts.recv(r.conn, r.buf, &r.len, 5000);
    }
};

// A send that cancel stops in the middle of a message drops it whole: the receiver, which started it, drops it when
// the sender's next message begins, and never sees a part of it. After cancel, the calls that needn't wait work, the
// zero-copy calls included.
test "fast: a send cancelled in the middle drops its message whole" {
    var t = ts.begin(@src(), 30);
    defer t.end();
    var p: ts.Pair = .{};
    try ts.openPair(&t, &p, "cancelled_send", RING_BYTES);
    defer p.close();
    var long: [16384]u8 = undefined;
    ts.pattern(&long, 4);

    var send: Send = .{ .conn = p.server, .bytes = &long };
    send.thread = try std.Thread.spawn(.{}, Send.run, .{&send});
    var ptr: ?*const anyopaque = null;
    var len: usize = 0;
    const acquired = c.fipc_recv_acquire(p.client, &ptr, &len, 5000);
    _ = t.expectEq(acquired, c.FIPC_TOO_LARGE, "the message's first piece is there");
    _ = t.expectEq(len, long.len, "with its length");
    ts.sleepMs(50);
    c.fipc_cancel(p.server);
    send.thread.join();
    _ = t.expectEq(send.result, c.FIPC_CANCELLED, "the send, stopped after its first piece: CANCELLED");

    var got: [16384]u8 = undefined;
    var receive: Recv = .{ .conn = p.client, .buf = &got };
    const receiver = try std.Thread.spawn(.{}, Recv.run, .{&receive});
    const deadline = ts.nowMs() + 5000;
    var next = ts.send(p.server, "next", 0);
    while (next == c.FIPC_CANCELLED and ts.nowMs() < deadline) : (next = ts.send(p.server, "next", 0)) ts.sleepMs(5);
    receiver.join();
    _ = t.expectEq(next, c.FIPC_OK, "a send that needn't wait works after cancel");
    _ = t.expectEq(receive.result, c.FIPC_OK, "the receiver drops the unfinished message");
    _ = t.expect(receive.len == 4 and std.mem.eql(u8, got[0..4], "next"), "and takes the next one");

    var out: ?*anyopaque = null;
    try t.checkEq(c.fipc_send_acquire(p.server, 2, &out, 0), c.FIPC_OK, "a zero-copy send after it");
    @memcpy(@as([*]u8, @ptrCast(out.?))[0..2], "zc");
    try t.checkEq(c.fipc_send_commit(p.server, 2), c.FIPC_OK, "committed");
    _ = t.expect(ts.recvIs(p.client, 0, "zc"), "arrives");
    return t.done();
}

/// An `fipc_rpc_recv` without a timeout on its own thread.
const BlockedRpcRecv = struct {
    conn: ts.Conn,
    result: c.fipc_result_t = c.FIPC_OK,
    elapsed_ms: u64 = 0,

    fn run(a: *BlockedRpcRecv) void {
        const t0 = ts.nowMs();
        var msg: c.fipc_rpc_msg_t = undefined;
        var buf: [64]u8 = undefined;
        a.result = c.fipc_rpc_recv(a.conn, &buf, buf.len, &msg, -1);
        a.elapsed_ms = ts.nowMs() - t0;
    }
};

// With a peer process on the other side, as the server (this process listens) and as the client: a thread blocked in
// `fipc_rpc_recv` returns FIPC_CANCELLED within 1 s of another thread's `fipc_cancel`.
test "slow: cancel releases a blocked RPC receive, on either side" {
    var t = ts.begin(@src(), 60);
    defer t.end();
    for ([_]bool{ true, false }) |server| {
        var buf: [64]u8 = undefined;
        const name = ts.name(&buf, if (server) "cancel_server" else "cancel_client");
        var listener: ts.Listener = null;
        if (server) try t.checkEq(c.fipc_listen(name, 1 << 20, &listener), c.FIPC_OK, "listen");
        defer c.fipc_listener_close(listener);

        const peer = ts.Peer.spawn(&.{ if (server) "connect-hold" else "listen-hold", name }) catch return t.fail("Failed to launch the peer");
        defer peer.deinit();
        var conn: ts.Conn = null;
        const rc = if (server) c.fipc_accept(listener, &conn, 10000) else c.fipc_connect(name, &conn, 10000);
        try t.checkEq(rc, c.FIPC_OK, "the connection");
        defer c.fipc_close(conn);

        var blocked: BlockedRpcRecv = .{ .conn = conn };
        const thread = std.Thread.spawn(.{}, BlockedRpcRecv.run, .{&blocked}) catch return t.fail("Failed to start the thread");
        ts.sleepMs(200); // it settles into its wait
        c.fipc_cancel(conn);
        thread.join();
        peer.kill();

        const side = if (server) "the server's" else "the client's";
        if (!t.expect(blocked.result == c.FIPC_CANCELLED, "the receive returns FIPC_CANCELLED after cancel"))
            std.debug.print("  {s}: {s} ({d}), elapsed: {d} ms\n", .{ side, ts.resultName(blocked.result), blocked.result, blocked.elapsed_ms });
        if (!t.expect(blocked.elapsed_ms <= 1000, "the receive returns within 1 second of cancel"))
            std.debug.print("  {s}: elapsed {d} ms\n", .{ side, blocked.elapsed_ms });
    }
    return t.done();
}

// ---------------------------------------------------------------------------------------------------------------
// Windows only: a blocked wait doesn't spin (Linux's futex waits keep no state that could)
// ---------------------------------------------------------------------------------------------------------------

/// CPU time a blocked recv on `conn` spends in 500 ms, after `disturb` ran; `result` gets the recv's result.
fn blockedRecvCpuMs(conn: ts.Conn, disturb: *const fn (ts.Conn) void, arg: ts.Conn, result: *i32) f64 {
    const Blocked = struct {
        conn: ts.Conn,
        result: std.atomic.Value(i32) = .init(-1),
        fn run(r: *@This()) void {
            var buf: [64]u8 = undefined;
            var len: usize = 0;
            r.result.store(@intCast(ts.recv(r.conn, &buf, &len, -1)), .seq_cst);
        }
    };
    var recv: Blocked = .{ .conn = conn };
    const thread = std.Thread.spawn(.{}, Blocked.run, .{&recv}) catch return -1.0;
    ts.sleepMs(200); // the recv spins briefly, then blocks
    disturb(arg);
    const before = threadCpuMs(thread);
    ts.sleepMs(500);
    const spent = threadCpuMs(thread) - before;
    c.fipc_cancel(conn); // ends the recv with CANCELLED
    thread.join();
    result.* = recv.result.load(.seq_cst);
    return spent;
}

fn threadCpuMs(thread: std.Thread) f64 {
    var created: win.FILETIME = undefined;
    var exited: win.FILETIME = undefined;
    var kernel: win.FILETIME = undefined;
    var user: win.FILETIME = undefined;
    if (win.GetThreadTimes(thread.getHandle(), &created, &exited, &kernel, &user) == 0) return -1.0;
    return @as(f64, @floatFromInt(win.filetime(kernel) + win.filetime(user))) / 10000.0; // 100 ns units
}

fn cancelOf(conn: ts.Conn) void {
    c.fipc_cancel(conn);
}

fn nothing(conn: ts.Conn) void {
    _ = conn;
}

// The peer's cancel signals nothing this side waits on: a blocked receive stays blocked, and uses no CPU to speak of.
test "fast: the peer's cancel doesn't make a blocked receive spin" {
    if (!ts.windows) return error.SkipZigTest;
    var t = ts.begin(@src(), 30);
    defer t.end();
    var p: ts.Pair = .{};
    try ts.openPair(&t, &p, "cancel_spin", RING_BYTES);
    defer p.close();
    var result: i32 = -1;
    const spent = blockedRecvCpuMs(p.client, cancelOf, p.server, &result);

    std.debug.print("  blocked recv used {d:.0} ms of CPU in 500 ms after the peer's cancel\n", .{spent});
    if (result != c.FIPC_CANCELLED) return t.checkEq(result, c.FIPC_CANCELLED, "The recv ends with its own cancel");
    _ = t.expect(spent >= 0.0 and spent < 100.0, "A blocked recv stays blocked after the peer cancels");
    return t.done();
}

// Each session has its own ring events: after a peer's departure and a reconnect, a blocked receive of the new session
// stays blocked.
test "fast: a blocked receive doesn't spin in the session after a peer's end" {
    if (!ts.windows) return error.SkipZigTest;
    var t = ts.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    const name = ts.name(&buf, "spin_after_end");
    var p: ts.Pair = .{};
    try t.check(p.open(name, RING_BYTES), "Failed to open the pair");
    defer p.close();

    // The server's side leaves; the client learns of it and connects again, and the server accepts it
    c.fipc_close(p.server);
    p.server = null;
    _ = t.expect(ts.waitForEnd(p.client, 5000), "The client learns that the server's side left");
    c.fipc_close(p.client);
    p.client = null;
    try t.check(p.reconnect(name), "The client connects again, and the server accepts it");

    var result: i32 = -1;
    const spent = blockedRecvCpuMs(p.client, nothing, null, &result);
    std.debug.print("  blocked recv used {d:.0} ms of CPU in 500 ms in the session after a peer's end\n", .{spent});
    if (result != c.FIPC_CANCELLED) return t.checkEq(result, c.FIPC_CANCELLED, "The recv ends with its own cancel");
    _ = t.expect(spent >= 0.0 and spent < 100.0, "A blocked recv stays blocked in the new session");
    return t.done();
}
