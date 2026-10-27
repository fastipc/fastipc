//! A peer's process killed at each point of a connection's life (before, during and after the handshake, between
//! exchanges, while a receive waits for an answer, before a commit, in the middle of a message, during a reconnect): the side that
//! stays learns of the death at once, from its control connection's end, nothing half-written reaches it, nothing of the
//! dead process is left behind, and a new peer pairs with it. The peer process is the server (it listens) unless a test
//! says otherwise.

const std = @import("std");
const support = @import("../support.zig");
const c = support.c;
const Peer = support.Peer;

const RING_BYTES = 4096;
/// A bound for learning of a death: it reaches the other side at once; this leaves room for a loaded machine.
const DEATH_MS = 5000;
/// The timeout of a connect to a server that dies before or during the handshake, which the connect waits out.
const DEAD_CONNECT_MS = 1000;

/// Sends a message of `len` bytes of `fill`.
fn sendFilled(conn: support.Conn, fill: u8, comptime len: usize) c.fipc_result_t {
    var message: [len]u8 = undefined;
    @memset(&message, fill);
    return c.fipc_send(conn, &message, len, 5000);
}

/// Receives a message of up to 64 bytes.
fn receive(conn: support.Conn, timeout_ms: c_int) c.fipc_result_t {
    var buf: [64]u8 = undefined;
    var len: usize = 0;
    return c.fipc_recv(conn, &buf, buf.len, &len, timeout_ms);
}

/// Connects this process to the peer's listener within `timeout_ms`; null if it didn't.
fn connect(name: [:0]const u8, timeout_ms: c_int) support.Conn {
    var conn: support.Conn = null;
    if (c.fipc_stream_connect(name, &conn, timeout_ms) != c.FIPC_OK) return null;
    return conn;
}

/// A server that answers one message and then closes, the client that connects to it and exchanges a message and its
/// answer, and the server's clean exit: a recovery.
fn recovers(t: *support.Test, name: [:0]const u8, fill: u8, comptime what: []const u8) !void {
    const server = Peer.spawn(&.{ "serve", name, "1", "close" }) catch return t.fail("Failed to launch the new server");
    defer server.deinit();
    const conn = connect(name, 10000) orelse return t.fail("The client connects to the new server (" ++ what ++ ")");
    defer c.fipc_close(conn);
    if (t.expect(sendFilled(conn, fill, 16) == c.FIPC_OK, "Send after " ++ what ++ " should succeed")) {
        _ = t.expect(receive(conn, 5000) == c.FIPC_OK, "Recv after " ++ what ++ " should succeed");
    }
    _ = t.expect(server.exitedWith(0), "The new server answers and exits");
}

// ---------------------------------------------------------------------------------------------------------------
// A waiting receive, and pairing again
// ---------------------------------------------------------------------------------------------------------------

/// An `fipc_recv` without a timeout on its own thread, and when it returned.
const BlockedRecv = struct {
    conn: support.Conn,
    result: std.atomic.Value(i32) = .init(-1),
    returned_ns: std.atomic.Value(u64) = .init(0),

    fn run(blocked: *BlockedRecv) void {
        const code = receive(blocked.conn, -1);
        blocked.returned_ns.store(support.nowNs(), .release);
        blocked.result.store(@intCast(code), .release);
    }
};

/// The survivor's bound: the peer's end reaches it through the kernel (EOF on the control connection) as the process
/// exits, so it is a matter of scheduling, not a timeout; this leaves room for a loaded 2-CPU machine.
const SURVIVOR_MS = 250;

/// This side's connection with a peer process: as the server, this process listens and the peer connects; as the
/// client, the peer listens and this process connects. `listener` is this side's, kept open between the peers.
fn pairWith(t: *support.Test, name: [:0]const u8, survivor_listens: bool, listener: support.Listener) !struct { *Peer, support.Conn } {
    const peer = Peer.spawn(&.{ if (survivor_listens) "connect-hold" else "listen-hold", name }) catch return t.fail("Failed to launch the peer");
    errdefer peer.deinit();
    var conn: support.Conn = null;
    const result = if (survivor_listens) c.fipc_stream_accept(listener, &conn, 10000) else c.fipc_stream_connect(name, &conn, 10000);
    if (result != c.FIPC_OK or !peer.waitFor(if (survivor_listens) "C" else "A", 10000)) {
        c.fipc_close(conn);
        return t.fail("They pair");
    }
    return .{ peer, conn };
}

/// One cycle: this process and a peer pair (this side listening, or connecting to the peer's listener); this side waits
/// in `fipc_recv`; the peer's process is killed; the wait returns DISCONNECTED at once; this side closes its
/// connection, a new peer starts `restart_ms` later, and they pair again.
fn survivorCycle(t: *support.Test, name: [:0]const u8, survivor_listens: bool, restart_ms: u32) error{TestFailed}!void {
    var listener: support.Listener = null;
    if (survivor_listens and c.fipc_stream_create(name, RING_BYTES, &listener) != c.FIPC_OK) return t.fail("The survivor listens");
    defer c.fipc_listener_close(listener);
    const peer, const conn = try pairWith(t, name, survivor_listens, listener);
    defer peer.deinit();
    {
        defer c.fipc_close(conn);
        var recv: BlockedRecv = .{ .conn = conn };
        const thread = std.Thread.spawn(.{}, BlockedRecv.run, .{&recv}) catch return t.fail("Failed to start the receive thread");
        support.sleepMs(50); // the receive waits
        peer.kill(); // killed and reaped: its process has exited
        const dead_ns = support.nowNs();
        thread.join();
        const returned_ns = recv.returned_ns.load(.acquire);
        const took_ms = if (returned_ns > dead_ns) (returned_ns - dead_ns) / std.time.ns_per_ms else 0;
        std.debug.print("  survivor {s}, restart after {d} ms: recv returned {s} {d} ms after the peer's death\n", .{
            if (survivor_listens) "listening" else "connected", restart_ms, support.resultName(@intCast(recv.result.load(.acquire))), took_ms,
        });
        try t.check(recv.result.load(.acquire) == c.FIPC_DISCONNECTED, "The blocked recv returns DISCONNECTED");
        try t.check(took_ms < SURVIVOR_MS, "at once");
    }
    // The survivor closed its connection; a new peer starts
    support.sleepMs(restart_ms);
    const again, const conn2 = try pairWith(t, name, survivor_listens, listener);
    defer again.deinit();
    c.fipc_close(conn2);
}

// As the server and as the client, with the new peer starting 0, 50 and 200 ms after the death.
test "slow: a waiting receive learns of the peer's death at once, and they pair again" {
    var t = try support.beginTimed(@src(), 60);
    defer t.end();
    var buf: [64]u8 = undefined;
    const name = support.uniqueName(&buf, "survivor");
    defer support.clearName(name); // macOS: the files a killed listener of the name left
    for ([_]bool{ true, false }) |survivor_listens| {
        for ([_]u32{ 0, 50, 200 }) |restart_ms| try survivorCycle(&t, name, survivor_listens, restart_ms);
    }
    return t.done();
}

// ---------------------------------------------------------------------------------------------------------------
// Deaths at each point of a connection
// ---------------------------------------------------------------------------------------------------------------

// The server listens and dies before any client: the client's connect times out, without hanging.
test "slow: a connect to a server that died before accepting times out" {
    var t = try support.begin(@src(), 60);
    defer t.end();
    var buf: [64]u8 = undefined;
    const name = support.uniqueName(&buf, "died_listening");
    defer support.clearName(name); // macOS: the files a killed listener of the name left
    {
        const server = Peer.spawn(&.{ "listen-exit", name }) catch return t.fail("Failed to launch the server");
        defer server.deinit();
        try t.check(server.exitedWith(0), "The server listens and dies");
    }
    const start = support.nowMs();
    var conn: support.Conn = null;
    const result = c.fipc_stream_connect(name, &conn, DEAD_CONNECT_MS);
    c.fipc_close(conn);
    if (!t.expect(result == c.FIPC_TIMEOUT, "connect should fail when the server died before the handshake"))
        std.debug.print("  Got: {s}\n", .{support.resultName(result)});
    _ = t.expect(support.nowMs() - start < DEAD_CONNECT_MS + 2000, "at its timeout, without hanging");
    return t.done();
}

// The server dies during or right after the handshake: either the connect fails, or the client learns of the death.
test "slow: a server killed during the handshake: the connect fails or the session ends" {
    var t = try support.begin(@src(), 60);
    defer t.end();
    var buf: [64]u8 = undefined;
    const name = support.uniqueName(&buf, "died_handshaking");
    defer support.clearName(name); // macOS: the files a killed listener of the name left
    const server = Peer.spawn(&.{ "listen-hold", name }) catch return t.fail("Failed to launch the server");
    defer server.deinit();
    try t.check(server.waitFor("L", 10000), "The server listens");

    const Connect = struct {
        name: [:0]const u8,
        conn: support.Conn = null,
        result: c.fipc_result_t = c.FIPC_OK,
        fn run(client: *@This()) void {
            client.result = c.fipc_stream_connect(client.name, &client.conn, DEAD_CONNECT_MS);
        }
    };
    var client: Connect = .{ .name = name };
    const thread = std.Thread.spawn(.{}, Connect.run, .{&client}) catch return t.fail("Failed to start the client");
    server.kill(); // during or after the handshake
    thread.join();
    defer c.fipc_close(client.conn);
    if (client.result == c.FIPC_OK) {
        _ = t.expect(support.waitForEnd(client.conn, DEATH_MS), "The client learns of the server's death");
    } else {
        _ = t.expectEqual(client.result, c.FIPC_TIMEOUT, "The handshake fails");
    }
    return t.done();
}

// The server is killed after some exchanges of a message and its answer: the client's next receive reports the end.
test "slow: a server killed between exchanges: the next receive reports the end" {
    var t = try support.begin(@src(), 60);
    defer t.end();
    var buf: [64]u8 = undefined;
    const name = support.uniqueName(&buf, "died_between");
    defer support.clearName(name); // macOS: the files a killed listener of the name left
    const server = Peer.spawn(&.{ "serve", name, "100", "hold" }) catch return t.fail("Failed to launch the server");
    defer server.deinit();
    const conn = connect(name, 10000) orelse return t.fail("The client connects");
    defer c.fipc_close(conn);
    for (0..5) |_| {
        if (sendFilled(conn, 0xCC, 32) != c.FIPC_OK) break;
        if (receive(conn, 5000) != c.FIPC_OK) break;
    }
    server.kill();
    const result = receive(conn, -1);
    if (!t.expect(result == c.FIPC_DISCONNECTED or result == c.FIPC_CANCELLED, "recv after mid-exchange kill should return DISCONNECTED or CANCELLED"))
        std.debug.print("  Got: {s}\n", .{support.resultName(result)});
    return t.done();
}

// The client waits for an answer when the server is killed: its receive returns DISCONNECTED.
test "slow: a server killed while a message waits for its answer: the receive reports the end" {
    var t = try support.begin(@src(), 60);
    defer t.end();
    var buf: [64]u8 = undefined;
    const name = support.uniqueName(&buf, "died_asked");
    defer support.clearName(name); // macOS: the files a killed listener of the name left
    const server = Peer.spawn(&.{ "listen-hold", name }) catch return t.fail("Failed to launch the server");
    defer server.deinit();
    const conn = connect(name, 10000) orelse return t.fail("The client connects");
    defer c.fipc_close(conn);
    try t.checkEqual(sendFilled(conn, 0xDD, 16), c.FIPC_OK, "Send");
    server.kill();
    const result = receive(conn, -1);
    if (!t.expect(result == c.FIPC_DISCONNECTED, "recv should return DISCONNECTED after the server was killed mid-recv"))
        std.debug.print("  Got: {s}\n", .{support.resultName(result)});
    return t.done();
}

// The server reserves room in its send ring and dies before the commit: the client learns of the end, and its receive
// finds nothing (DISCONNECTED or TIMEOUT: nothing half-written arrives).
test "slow: a server killed before its commit: nothing half-written arrives" {
    var t = try support.begin(@src(), 60);
    defer t.end();
    var buf: [64]u8 = undefined;
    const name = support.uniqueName(&buf, "died_writing");
    defer support.clearName(name); // macOS: the files a killed listener of the name left
    const server = Peer.spawn(&.{ "partial-write", name }) catch return t.fail("Failed to launch the server");
    defer server.deinit();
    const conn = connect(name, 10000) orelse return t.fail("The client connects");
    defer c.fipc_close(conn);
    _ = server.waitTimeout(10000);
    _ = t.expect(support.waitForEnd(conn, DEATH_MS), "The client detects the end after the partial-write kill");
    const result = receive(conn, 1000);
    _ = t.expect(result == c.FIPC_DISCONNECTED or result == c.FIPC_TIMEOUT, "nothing half-written arrives");
    return t.done();
}

// The peer (a client) dies in the middle of a message, its send waiting for room for the second piece: the receive
// returns DISCONNECTED, and nothing of the message is delivered.
test "slow: the peer's death in the middle of a message: nothing of it arrives" {
    var t = try support.begin(@src(), 60);
    defer t.end();
    var buf: [64]u8 = undefined;
    const name = support.uniqueName(&buf, "died_mid_message");
    var listener: support.Listener = null;
    try t.checkEqual(c.fipc_stream_create(name, RING_BYTES, &listener), c.FIPC_OK, "listen");
    defer c.fipc_listener_close(listener);
    const peer = Peer.spawn(&.{ "send-long", name }) catch return t.fail("Failed to launch the peer");
    defer peer.deinit();
    var conn: support.Conn = null;
    try t.checkEqual(c.fipc_stream_accept(listener, &conn, 10000), c.FIPC_OK, "they pair");
    defer c.fipc_close(conn);

    var ptr: ?*const anyopaque = null;
    var len: usize = 0;
    _ = t.expectEqual(c.fipc_recv_acquire(conn, &ptr, &len, 10000), c.FIPC_TOO_LARGE, "the message's first piece is there");
    _ = t.expectEqual(len, 16384, "with its length");
    peer.kill();
    var got: [16384]u8 = undefined;
    _ = t.expectEqual(support.recv(conn, &got, &len, 10000), c.FIPC_DISCONNECTED, "the receive that starts it: DISCONNECTED");
    _ = t.expectEqual(support.recv(conn, &got, &len, 1000), c.FIPC_DISCONNECTED, "and the next one");
    return t.done();
}

// ---------------------------------------------------------------------------------------------------------------
// Nothing left behind, and the next peer
// ---------------------------------------------------------------------------------------------------------------

// A listener's process killed while it serves a client leaves nothing behind: a new listener claims the name at once,
// and this process listens on it and sets up a session of its own, with its own ring events: a message crosses it.
test "slow: a killed listener leaves nothing behind" {
    var t = try support.beginTimed(@src(), 30);
    defer t.end();
    var buf: [128]u8 = undefined;
    const name = support.uniqueName(&buf, "killed_listener");

    const child = Peer.spawn(&.{ "listen-hold", name }) catch return t.fail("Failed to launch the child");
    defer child.deinit();
    try t.check(child.waitFor("L", 10000), "The child listens");
    child.kill();
    _ = t.expect(support.freeAtOnce(name), "a new listener claims the name at once");

    var pair: support.Pair = .{};
    const start = support.nowMs();
    const opened = pair.open(name, RING_BYTES);
    const elapsed = support.nowMs() - start;
    defer pair.close();
    try t.check(opened, "this process listens and a client connects after the child's crash");
    _ = t.expect(elapsed < 500, "at once");
    _ = t.expectEqual(support.send(pair.server, "hello", 1000), c.FIPC_OK, "a message on the new session");
    _ = t.expect(support.recvIs(pair.client, 1000, "hello"), "crosses it: its ring events work");
    return t.done();
}

// A server listens and exits without closing anything: nothing of it is left (a new listener claims the name at
// once), and a new server and the client exchange a message and its answer.
test "slow: a listener that exits without closing leaves nothing behind" {
    var t = try support.begin(@src(), 60);
    defer t.end();
    var buf: [64]u8 = undefined;
    const name = support.uniqueName(&buf, "exited_listener");
    {
        const child = Peer.spawn(&.{ "listen-exit", name }) catch return t.fail("Failed to launch the child");
        defer child.deinit();
        try t.check(child.exitedWith(0), "The child listens and exits");
    }
    if (!t.expect(support.freeAtOnce(name), "Nothing is left after the child's death: the name is free")) return t.done();
    try recovers(&t, name, 0xBB, "the exited listener");
    return t.done();
}

// A server and a client (both peer processes) connect, and both are killed; then a new server and this process's
// client connect and exchange a message and its answer.
test "slow: after both sides are killed, a new pair works" {
    var t = try support.begin(@src(), 60);
    defer t.end();
    var buf: [64]u8 = undefined;
    const name = support.uniqueName(&buf, "both_killed");
    {
        const server1 = Peer.spawn(&.{ "listen-hold", name }) catch return t.fail("Failed to launch the first server");
        defer server1.deinit();
        const client1 = Peer.spawn(&.{ "connect-hold", name }) catch return t.fail("Failed to launch the first client");
        defer client1.deinit();
        try t.check(server1.waitFor("A", 10000) and client1.waitFor("C", 10000), "The first pair connects");
        server1.terminate();
        client1.terminate();
        _ = server1.wait();
        _ = client1.wait();
    }
    try recovers(&t, name, 0xAB, "both sides' restart");
    return t.done();
}

// After a first connection's server is killed, a new server dies while this process reconnects (the connect fails, or
// its session ends); a stable reconnect afterwards works.
test "slow: a server that dies during a reconnect doesn't stop the next one" {
    var t = try support.begin(@src(), 60);
    defer t.end();
    var buf: [64]u8 = undefined;
    const name = support.uniqueName(&buf, "died_reconnecting");
    {
        const server1 = Peer.spawn(&.{ "listen-hold", name }) catch return t.fail("Failed to launch the first server");
        defer server1.deinit();
        const conn1 = connect(name, 10000) orelse return t.fail("The client connects");
        defer c.fipc_close(conn1);
        server1.kill();
        _ = t.expect(support.waitForEnd(conn1, DEATH_MS), "The client learns of the death");
    }
    {
        const server2 = Peer.spawn(&.{ "listen-exit", name }) catch return t.fail("Failed to launch the second server");
        defer server2.deinit();
        const conn2 = connect(name, 500);
        defer c.fipc_close(conn2);
        _ = server2.waitTimeout(5000);
        if (conn2 != null) _ = t.expect(support.waitForEnd(conn2, DEATH_MS), "A connection to the dying server ends");
    }
    try recovers(&t, name, 0x77, "the kill-during-reconnect recovery");
    return t.done();
}

// ---------------------------------------------------------------------------------------------------------------
// Many cycles
// ---------------------------------------------------------------------------------------------------------------

const SOAK_CYCLES = 50;
const SOAK_CYCLE_TIMEOUT_SEC = 30;
const SOAK_MIN_MSG_SIZE = 16;
const SOAK_MAX_MSG_SIZE = 65536;
const SOAK_MAX_DELAY_MS = 100;
const SOAK_MSG_EXCHANGE_COUNT = 3;

const Cycle = struct {
    index: usize,
    name: [:0]const u8,
    random: std.Random,
    err_buf: [256]u8 = undefined,
    err: []const u8 = "",

    fn fail(cycle: *Cycle, comptime fmt: []const u8, args: anytype) error{CycleFailed} {
        cycle.err = std.mem.print(&cycle.err_buf, "cycle {d}: " ++ fmt, .{cycle.index} ++ args) catch "cycle failed";
        return error.CycleFailed;
    }
};

/// Sends and receives SOAK_MSG_EXCHANGE_COUNT echoes.
fn exchange(cycle: *Cycle, conn: support.Conn, send_buf: []const u8, recv_buf: []u8, comptime phase: []const u8) error{CycleFailed}!void {
    for (0..SOAK_MSG_EXCHANGE_COUNT) |i| {
        var result = c.fipc_send(conn, send_buf.ptr, send_buf.len, 5000);
        if (result != c.FIPC_OK) return cycle.fail(phase ++ " send[{d}] failed: {s}", .{ i, support.resultName(result) });
        var len: usize = 0;
        result = c.fipc_recv(conn, recv_buf.ptr, recv_buf.len, &len, 5000);
        if (result != c.FIPC_OK) return cycle.fail(phase ++ " recv[{d}] failed: {s}", .{ i, support.resultName(result) });
        if (len != send_buf.len or !std.mem.eql(u8, recv_buf[0..len], send_buf))
            return cycle.fail(phase ++ " echo[{d}] doesn't match", .{i});
    }
}

/// Connects this process's client to the echo server peer, whose accept says "A".
fn connectTo(cycle: *Cycle, server: *Peer) error{CycleFailed}!support.Conn {
    var conn: support.Conn = null;
    const result = c.fipc_stream_connect(cycle.name, &conn, 10000);
    if (result != c.FIPC_OK) return cycle.fail("connect failed: {s}", .{support.resultName(result)});
    if (!server.waitFor("A", 10000)) {
        c.fipc_close(conn);
        return cycle.fail("the server's accept timed out", .{});
    }
    return conn;
}

/// One cycle: a server peer and this process's client connect and exchange echoes; one side goes (the server killed,
/// or the client closed); after a random delay a new server and a new client connect and exchange echoes.
/// `reconnect_ms` gets the time from the restart to both sides being connected again.
fn runCycle(cycle: *Cycle, reconnect_ms: *u64) error{CycleFailed}!void {
    const kill_server = cycle.random.uintLessThan(u32, 2) == 1;
    const msg_size = cycle.random.intRangeAtMost(u32, SOAK_MIN_MSG_SIZE, SOAK_MAX_MSG_SIZE);
    const restart_delay_ms = cycle.random.intRangeAtMost(u32, 0, SOAK_MAX_DELAY_MS);
    reconnect_ms.* = 0;

    const send_buf = std.testing.allocator.alloc(u8, msg_size) catch return cycle.fail("malloc({d}) failed", .{msg_size});
    defer std.testing.allocator.free(send_buf);
    for (send_buf, 0..) |*byte, i| byte.* = @truncate(i ^ cycle.index);
    const recv_buf = std.testing.allocator.alloc(u8, msg_size) catch return cycle.fail("malloc({d}) failed", .{msg_size});
    defer std.testing.allocator.free(recv_buf);

    {
        const server = Peer.spawn(&.{ "echo", cycle.name }) catch return cycle.fail("server launch failed", .{});
        defer server.deinit();
        const conn = try connectTo(cycle, server);
        exchange(cycle, conn, send_buf, recv_buf, "before the end") catch |err| {
            c.fipc_close(conn);
            return err;
        };
        if (kill_server) {
            server.kill();
            const ended = support.waitForEnd(conn, 5000);
            c.fipc_close(conn);
            if (!ended) return cycle.fail("expected DISCONNECTED after the server's kill", .{});
        } else {
            // The client goes: this process closes its connection; the server sees the end at once and exits
            c.fipc_close(conn);
            if (!server.waitTimeout(5000)) return cycle.fail("the server didn't see the client's end", .{});
        }
    }

    if (restart_delay_ms > 0) support.sleepMs(restart_delay_ms);
    const reconnect_start = support.nowMs();
    const server2 = Peer.spawn(&.{ "echo", cycle.name }) catch return cycle.fail("server2 launch failed", .{});
    defer server2.deinit();
    const conn2 = try connectTo(cycle, server2);
    defer c.fipc_close(conn2);
    reconnect_ms.* = support.nowMs() - reconnect_start;
    try exchange(cycle, conn2, send_buf, recv_buf, "after the reconnect");
}

// 50 cycles of a side going and a reconnect, with random message sizes (16 B to 64 KiB) and restart delays (up to
// 100 ms): every cycle passes, and none takes over 30 s.
test "slow: fifty cycles of a side going and a reconnect" {
    var t = try support.begin(@src(), 300);
    defer t.end();
    var name_buf: [64]u8 = undefined;
    const name = support.uniqueName(&name_buf, "cycles");
    defer support.clearName(name); // macOS: the files a killed listener of the name left

    // A PRNG seeded with the PID and the time, for variety (printed, to repeat a run)
    const seed: u64 = support.pid() ^ @as(u64, @intCast(std.Io.Clock.real.now(support.io()).toSeconds()));
    var prng: std.Random.DefaultPrng = .init(seed);
    const random = prng.random();

    var cycles_completed: usize = 0;
    var failures: usize = 0;
    var total_reconnect_ms: u64 = 0;
    std.debug.print("  {d} cycles, messages of {d}-{d} B, seed {d}\n", .{ SOAK_CYCLES, SOAK_MIN_MSG_SIZE, SOAK_MAX_MSG_SIZE, seed });

    for (0..SOAK_CYCLES) |index| {
        const cycle_start = support.nowMs();
        var cycle: Cycle = .{ .index = index, .name = name, .random = random };
        var reconnect_ms: u64 = 0;
        const result = runCycle(&cycle, &reconnect_ms);
        const cycle_elapsed_sec = (support.nowMs() - cycle_start) / 1000;
        if (cycle_elapsed_sec > SOAK_CYCLE_TIMEOUT_SEC) {
            std.debug.print("  TIMEOUT: cycle {d} took {d} sec (limit {d})\n", .{ index, cycle_elapsed_sec, SOAK_CYCLE_TIMEOUT_SEC });
            failures += 1;
            continue;
        }
        if (result) |_| {
            cycles_completed += 1;
            total_reconnect_ms += reconnect_ms;
        } else |_| {
            failures += 1;
            std.debug.print("  FAIL: {s}\n", .{cycle.err});
            // Go on: the run shows every cycle's result
        }
        if ((index + 1) % 10 == 0)
            std.debug.print("  ... {d}/{d} cycles done ({d} ok, {d} fail)\n", .{ index + 1, SOAK_CYCLES, cycles_completed, failures });
    }

    const avg_reconnect = if (cycles_completed > 0) total_reconnect_ms / cycles_completed else 0;
    std.debug.print("  {d}/{d} cycles completed, {d} failures, avg reconnect {d}ms\n", .{ cycles_completed, SOAK_CYCLES, failures, avg_reconnect });
    _ = t.expectEqual(failures, 0, "Every cycle passes");
    return t.done();
}
