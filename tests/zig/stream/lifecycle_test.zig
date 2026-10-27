//! A connection's life through the C API: a listener accepts one client at a time, a client may start before its
//! server, an accept that polls, the listener's cancel and close, a session's end as the other side sees it,
//! reconnecting to a restarted server, pairs across processes and within one (on two threads: `fipc_stream_connect` waits for
//! the listener's `fipc_stream_accept`), and the library's unload on Windows (Linux and macOS: it keeps itself loaded).

const std = @import("std");
const builtin = @import("builtin");
const options = @import("test_options");
const support = @import("../support.zig");
const c = support.c;
const Peer = support.Peer;

const RING_BYTES = 4096;
/// A side's end reaches the other at once; this leaves room for a loaded machine.
const END_MS = 5000;

// One client at a time: `fipc_stream_accept` is INVALID while the connection it returned is open; a second client waits (its
// connect times out); once the first connection is closed, the listener accepts the next client.
test "fast: a listener accepts one client at a time" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    const name = support.uniqueName(&buf, "one_client");
    var pair: support.Pair = .{};
    try t.check(pair.open(name, RING_BYTES), "the first client");
    defer pair.close();
    var conn: support.Conn = null;
    _ = t.expectEqual(c.fipc_stream_accept(pair.listener, &conn, 0), c.FIPC_INVALID, "accept while the connection is open: INVALID");
    _ = t.expectEqual(c.fipc_stream_accept(pair.listener, &conn, 1000), c.FIPC_INVALID, "also with a timeout: at once");
    var second: support.Conn = null;
    _ = t.expectEqual(c.fipc_stream_connect(name, &second, 200), c.FIPC_TIMEOUT, "a second client waits: its connect times out");
    c.fipc_close(pair.client);
    pair.client = null;
    c.fipc_close(pair.server);
    pair.server = null;
    try t.check(pair.reconnect(name), "once the first is closed, the next client connects and is accepted");
    _ = t.expectEqual(support.send(pair.client, "next", 1000), c.FIPC_OK, "a message");
    _ = t.expect(support.recvIs(pair.server, 1000, "next"), "crosses the new connection");
    return t.done();
}

// A client may start before its server: `fipc_stream_connect` with FIPC_FOREVER waits for the listener and its accept.
test "fast: connect waits for a server that starts later" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    const name = support.uniqueName(&buf, "late_server");
    const Late = struct {
        fn run(n: [*:0]const u8, listener: *support.Listener, server: *support.Conn, result: *c.fipc_result_t) void {
            support.sleepMs(200);
            result.* = c.fipc_stream_create(n, RING_BYTES, listener);
            if (result.* == c.FIPC_OK) result.* = c.fipc_stream_accept(listener.*, server, 5000);
        }
    };
    var listener: support.Listener = null;
    var server: support.Conn = null;
    var accepted: c.fipc_result_t = c.FIPC_TIMEOUT;
    const thread = try std.Thread.spawn(.{}, Late.run, .{ name, &listener, &server, &accepted });
    var client: support.Conn = null;
    const result = c.fipc_stream_connect(name, &client, c.FIPC_FOREVER);
    thread.join();
    defer c.fipc_listener_close(listener);
    defer c.fipc_close(client);
    defer c.fipc_close(server);
    try t.checkEqual(result, c.FIPC_OK, "the client connects once the server listens and accepts");
    try t.checkEqual(accepted, c.FIPC_OK, "the server accepts it");
    return t.done();
}

// A server that polls (`fipc_stream_accept` with timeout 0, as a game loop does once per frame) lets a client in: a call that
// times out in the middle of the client's setup keeps it, and a later call finishes it.
test "fast: accept polled with timeout 0 lets a client in" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    const name = support.uniqueName(&buf, "polled");
    var listener: support.Listener = null;
    try t.checkEqual(c.fipc_stream_create(name, RING_BYTES, &listener), c.FIPC_OK, "listen");
    defer c.fipc_listener_close(listener);
    var connector: support.Connector = .{};
    try t.check(connector.start(name, 5000), "the client's thread");
    var server: support.Conn = null;
    var calls: usize = 0;
    var result: c.fipc_result_t = c.FIPC_TIMEOUT;
    while (result == c.FIPC_TIMEOUT and calls < 5000) : (calls += 1) {
        result = c.fipc_stream_accept(listener, &server, 0);
        if (result == c.FIPC_TIMEOUT) support.sleepMs(1);
    }
    var client: support.Conn = null;
    const connected = connector.join(&client);
    defer c.fipc_close(client);
    defer c.fipc_close(server);
    try t.checkEqual(result, c.FIPC_OK, "a polling accept returns the client");
    try t.checkEqual(connected, c.FIPC_OK, "the client connects");
    // `calls` counts every poll, those made before the client's thread connects included, then the one or two that
    // let the client in (fipc.h): one only where the client's side was done before the first poll, so the count has no
    // floor beyond the one call the loop always makes
    std.debug.print("  the client got in after {d} calls\n", .{calls});
    _ = t.expectEqual(support.send(client, "hi", 1000), c.FIPC_OK, "a message");
    _ = t.expect(support.recvIs(server, 1000, "hi"), "crosses the connection");
    return t.done();
}

// `fipc_listener_cancel` releases an accept that waits, and every later accept that would wait returns CANCELLED.
test "fast: listener cancel releases a waiting accept" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    var listener: support.Listener = null;
    try t.checkEqual(c.fipc_stream_create(support.uniqueName(&buf, "listener_cancel"), RING_BYTES, &listener), c.FIPC_OK, "listen");
    defer c.fipc_listener_close(listener);
    const Accept = struct {
        listener: support.Listener,
        result: c.fipc_result_t = c.FIPC_OK,
        fn run(accept: *@This()) void {
            var conn: support.Conn = null;
            accept.result = c.fipc_stream_accept(accept.listener, &conn, -1);
        }
    };
    var waiter: Accept = .{ .listener = listener };
    const thread = try std.Thread.spawn(.{}, Accept.run, .{&waiter});
    support.sleepMs(50);
    const start = support.nowMs();
    c.fipc_listener_cancel(listener);
    thread.join();
    _ = t.expectEqual(waiter.result, c.FIPC_CANCELLED, "the waiting accept returns CANCELLED");
    _ = t.expect(support.nowMs() - start < 1000, "at once");
    var conn: support.Conn = null;
    _ = t.expectEqual(c.fipc_stream_accept(listener, &conn, -1), c.FIPC_CANCELLED, "a later accept that would wait: CANCELLED");
    return t.done();
}

/// A client's `fipc_stream_connect` on a thread of its own that also says when it returned, for the tests that act between
/// two calls of a polling `fipc_stream_accept`.
const FlaggedConnector = struct {
    result: c.fipc_result_t = c.FIPC_TIMEOUT,
    conn: support.Conn = null,
    connected: std.atomic.Value(bool) = .init(false),
    thread: ?std.Thread = null,

    fn start(connector: *FlaggedConnector, n: [*:0]const u8) bool {
        connector.thread = std.Thread.spawn(.{}, run, .{ connector, n }) catch return false;
        return true;
    }

    fn run(connector: *FlaggedConnector, n: [*:0]const u8) void {
        connector.result = c.fipc_stream_connect(n, &connector.conn, 5000);
        connector.connected.store(true, .release);
    }

    /// Polls `fipc_stream_accept` with timeout 0 every 20 ms until the client's connect has returned. Normally the call
    /// that sent SEGMENT times out and the client's READY waits for the next call; if READY came within that call, the
    /// call finished the setup and `server` gets the connection. Returns the client's result, with its connection in
    /// `client`.
    fn betweenAccepts(connector: *FlaggedConnector, listener: support.Listener, server: *support.Conn, client: *support.Conn) c.fipc_result_t {
        while (!connector.connected.load(.acquire)) {
            if (server.* == null and c.fipc_stream_accept(listener, server, 0) != c.FIPC_OK) server.* = null;
            support.sleepMs(20);
        }
        if (connector.thread) |thread| thread.join();
        client.* = connector.conn;
        return connector.result;
    }
};

// A client whose setup `fipc_stream_accept` began (a call that sent SEGMENT and timed out) has its `fipc_stream_connect` return; if
// the listener is closed before a call finishes the setup, the client sees the end (DISCONNECTED).
test "fast: closing a listener ends a client's connection whose setup accept began" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    const name = support.uniqueName(&buf, "untaken");
    var listener: support.Listener = null;
    try t.checkEqual(c.fipc_stream_create(name, RING_BYTES, &listener), c.FIPC_OK, "listen");
    var connector: FlaggedConnector = .{};
    if (!connector.start(name)) {
        c.fipc_listener_close(listener);
        return t.fail("the client's thread");
    }
    var server: support.Conn = null;
    var client: support.Conn = null;
    const connected = connector.betweenAccepts(listener, &server, &client);
    defer c.fipc_close(client);
    // A call that read READY at once returned the connection: its close ends the client's session all the same
    c.fipc_close(server);
    c.fipc_listener_close(listener);
    try t.checkEqual(connected, c.FIPC_OK, "the client connects once an accept offered the rings");
    _ = t.expect(support.waitForEnd(client, END_MS), "the listener's close ends the client's session");
    var got: [8]u8 = undefined;
    var len: usize = 0;
    _ = t.expectEqual(support.recv(client, &got, &len, 1000), c.FIPC_DISCONNECTED, "its receive reports DISCONNECTED");
    return t.done();
}

// A client that leaves between the accept that sent SEGMENT and the one that returns: its READY came first, so the
// next `fipc_stream_accept` returns its connection, whose calls then report the session's end (DISCONNECTED).
test "fast: a client that leaves between two accepts is accepted, with its session ended" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    const name = support.uniqueName(&buf, "leaves_at_once");
    var listener: support.Listener = null;
    try t.checkEqual(c.fipc_stream_create(name, RING_BYTES, &listener), c.FIPC_OK, "listen");
    defer c.fipc_listener_close(listener);
    var connector: FlaggedConnector = .{};
    try t.check(connector.start(name), "the client's thread");
    var server: support.Conn = null;
    var client: support.Conn = null;
    const connected = connector.betweenAccepts(listener, &server, &client);
    try t.checkEqual(connected, c.FIPC_OK, "The client completes the handshake");
    // It leaves at once, before the server's next call
    c.fipc_close(client);
    const accepted: c.fipc_result_t = if (server != null) c.FIPC_OK else c.fipc_stream_accept(listener, &server, 5000);
    defer c.fipc_close(server);
    _ = t.expectEqual(accepted, c.FIPC_OK, "The server still accepts the connection");
    if (accepted != c.FIPC_OK) return t.done();
    var msg: [8]u8 = undefined;
    var len: usize = 0;
    _ = t.expectEqual(support.recv(server, &msg, &len, 5000), c.FIPC_DISCONNECTED, "Then its calls report the session's end");
    return t.done();
}

// Each session reports its own end: the server learns that the first client left, accepts the next one, and learns
// that it left too.
test "fast: the server learns of each client's end in turn" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    const name = support.uniqueName(&buf, "each_end");
    var pair: support.Pair = .{};
    try t.check(pair.open(name, RING_BYTES), "Failed to open the pair");
    defer pair.close();

    c.fipc_close(pair.client);
    pair.client = null;
    _ = t.expect(support.waitForEnd(pair.server, END_MS), "The server detects the first client's departure");
    c.fipc_close(pair.server);
    pair.server = null;
    try t.check(pair.reconnect(name), "A new client connects, and the server accepts it");

    c.fipc_close(pair.client);
    pair.client = null;
    _ = t.expect(support.waitForEnd(pair.server, END_MS), "The server detects the new client's departure");
    return t.done();
}

// The server's side and its listener close: the client learns of the end (DISCONNECTED), the restarted server listens
// on the name, and the client connects again (a connection has one session); a message crosses the new session.
test "fast: a client connects again to a restarted server" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    const name = support.uniqueName(&buf, "server_restart");
    var pair: support.Pair = .{};
    try t.check(pair.open(name, RING_BYTES), "Failed to open the pair");
    defer pair.close();

    c.fipc_close(pair.server);
    pair.server = null;
    c.fipc_listener_close(pair.listener);
    pair.listener = null;
    _ = t.expect(support.waitForEnd(pair.client, END_MS), "The client detects the server's departure");

    try t.checkEqual(c.fipc_stream_create(name, RING_BYTES, &pair.listener), c.FIPC_OK, "The restarted server listens");
    c.fipc_close(pair.client);
    pair.client = null;
    try t.check(pair.reconnect(name), "The client connects again, and the restarted server accepts it");
    try t.checkEqual(support.send(pair.client, "again", 1000), c.FIPC_OK, "A message on the new session");
    _ = t.expect(support.recvIs(pair.server, 1000, "again"), "A message crosses the new session");
    return t.done();
}

// A call with timeout 0 looks once: `fipc_stream_connect` with nobody listening, or a listener busy with another client,
// `fipc_recv` without a message and `fipc_stream_accept` without a client return TIMEOUT at once (and `fipc_stream_accept`
// while its client is connected, INVALID). After the session ended, a receive returns DISCONNECTED at once, and so does
// `fipc_recv(0)`.
test "fast: calls with timeout 0 look once, before and after the end" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    const name = support.uniqueName(&buf, "look_once");

    var conn: support.Conn = null;
    var start = support.nowMs();
    _ = t.expectEqual(c.fipc_stream_connect(name, &conn, 0), c.FIPC_TIMEOUT, "connect(0) with nobody listening: TIMEOUT");
    _ = t.expect(support.nowMs() - start < 100, "a single check");

    var pair: support.Pair = .{};
    try t.check(pair.open(name, RING_BYTES), "a pair");
    defer pair.close();
    start = support.nowMs();
    _ = t.expectEqual(c.fipc_stream_accept(pair.listener, &conn, 0), c.FIPC_INVALID, "accept(0) while its client is connected: INVALID (one at a time)");
    _ = t.expectEqual(c.fipc_stream_connect(name, &conn, 0), c.FIPC_TIMEOUT, "connect(0) to a listener busy with a client: TIMEOUT");
    var got: [16]u8 = undefined;
    var len: usize = 0;
    _ = t.expectEqual(c.fipc_recv(pair.server, &got, got.len, &len, 0), c.FIPC_TIMEOUT, "recv(0) without a message: TIMEOUT");
    _ = t.expect(support.nowMs() - start < 100, "single checks");

    c.fipc_close(pair.client);
    pair.client = null;
    _ = t.expect(support.waitForEnd(pair.server, END_MS), "The server learns that the client left");
    start = support.nowMs();
    _ = t.expectEqual(support.recv(pair.server, &got, &len, 5000), c.FIPC_DISCONNECTED, "a receive after the end returns DISCONNECTED");
    _ = t.expect(support.nowMs() - start < 500, "at once");
    _ = t.expectEqual(c.fipc_recv(pair.server, &got, got.len, &len, 0), c.FIPC_DISCONNECTED, "recv(0) after the end: DISCONNECTED");
    c.fipc_close(pair.server);
    pair.server = null;
    start = support.nowMs();
    _ = t.expectEqual(c.fipc_stream_accept(pair.listener, &conn, 0), c.FIPC_TIMEOUT, "accept(0) without a client: TIMEOUT");
    _ = t.expect(support.nowMs() - start < 100, "a single check");
    return t.done();
}

// A client process connects to this process's listener: once `connect` and `accept` returned, a message goes each way
// (the peer's "ping", this side's "pong"), and both sides then close.
test "slow: a client process and this process's listener exchange messages" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    const name = support.uniqueName(&buf, "cross_process");

    var listener: support.Listener = null;
    try t.checkEqual(c.fipc_stream_create(name, RING_BYTES, &listener), c.FIPC_OK, "listen");
    defer c.fipc_listener_close(listener);
    const peer = Peer.spawn(&.{ "connect-check", name }) catch return t.fail("Failed to launch the client");
    defer peer.deinit();

    var server: support.Conn = null;
    try t.checkEqual(c.fipc_stream_accept(listener, &server, 10000), c.FIPC_OK, "accept returns the connected client");
    defer c.fipc_close(server);
    _ = t.expect(support.recvIs(server, 5000, "ping"), "the client's message arrives");
    _ = t.expectEqual(support.send(server, "pong", 5000), c.FIPC_OK, "and the answer goes");
    _ = t.expect(peer.waitFor("S", 10000), "the client receives it: both sides are connected");
    peer.send("x");
    _ = t.expect(peer.exitedWith(0), "the client closes and exits");
    return t.done();
}

/// The racers' stacks. Windows commits a thread's whole stack at its creation (`std.Thread.spawn` passes the size as
/// the committed size), so 200 threads get small ones; Linux only reserves it, and refuses a small one here (EINVAL:
/// the test binary's static TLS goes on the stack too).
const racer_stack: usize = if (support.windows) 256 * 1024 else std.Thread.SpawnConfig.default_stack_size;

/// One side of one of many pairs, on its own thread: a listener that accepts and sends "hi", or a client that
/// connects and receives it.
const PairRacer = struct {
    name: [:0]const u8,
    start: *std.atomic.Value(u32),
    listens: bool,
    result: i32 = -1,

    fn run(racer: *PairRacer) void {
        // A gate that sleeps, not spins: the main thread spawns the racers meanwhile
        while (racer.start.load(.acquire) == 0) support.io().futexWaitUncancelable(u32, &racer.start.raw, 0);
        if (racer.listens) {
            var listener: support.Listener = null;
            racer.result = @intCast(c.fipc_stream_create(racer.name, RING_BYTES, &listener));
            if (racer.result != c.FIPC_OK) return;
            defer c.fipc_listener_close(listener);
            var conn: support.Conn = null;
            racer.result = @intCast(c.fipc_stream_accept(listener, &conn, 10000));
            if (racer.result == c.FIPC_OK) racer.result = @intCast(support.send(conn, "hi", 5000));
            // Its client receives the message before this side closes
            if (racer.result == c.FIPC_OK) _ = support.waitForEnd(conn, 10000);
            c.fipc_close(conn);
        } else {
            var conn: support.Conn = null;
            racer.result = @intCast(c.fipc_stream_connect(racer.name, &conn, 10000));
            if (racer.result == c.FIPC_OK and !support.recvIs(conn, 5000, "hi")) racer.result = -2;
            c.fipc_close(conn);
        }
    }
};

// Of each pair's listener and client, started together with every other pair's (a client may start before its
// listener: `fipc_stream_connect` waits for it), every pair connects and a message crosses.
test "slow: many pairs connect at once" {
    var t = try support.begin(@src(), 120);
    defer t.end();
    const pairs = 100;
    const rounds = 10;
    const gpa = std.testing.allocator;
    const names = gpa.alloc([32]u8, pairs) catch return t.fail("out of memory");
    defer gpa.free(names);
    const racers = gpa.alloc(PairRacer, 2 * pairs) catch return t.fail("out of memory");
    defer gpa.free(racers);
    const threads = gpa.alloc(std.Thread, 2 * pairs) catch return t.fail("out of memory");
    defer gpa.free(threads);

    var failures: usize = 0;
    for (0..rounds) |round| {
        var start = std.atomic.Value(u32).init(0);
        for (racers, 0..) |*racer, i| {
            const name = std.mem.printSentinel(&names[i / 2], "many_{d}_{d}_{d}", .{ support.pid(), round, i / 2 }, 0) catch unreachable;
            racer.* = .{ .name = name, .start = &start, .listens = i % 2 == 0 };
        }
        var started: usize = 0;
        for (threads, racers) |*thread, *racer| {
            thread.* = std.Thread.spawn(.{ .stack_size = racer_stack }, PairRacer.run, .{racer}) catch break;
            started += 1;
        }
        start.store(1, .release);
        support.io().futexWake(u32, &start.raw, std.math.maxInt(u32));
        for (threads[0..started]) |thread| thread.join();
        if (started != threads.len) return t.fail("Failed to start the racer threads");
        for (racers) |racer| {
            if (racer.result != c.FIPC_OK) failures += 1;
        }
    }
    std.debug.print("  {d} pairs x {d} rounds: {d} sides that didn't connect\n", .{ pairs, rounds, failures });
    _ = t.expectEqual(failures, 0, "Every pair connects");
    return t.done();
}

// A client that connects to a listener of its own process logs a warning, the symptom of a listener never closed
// (once per process). The peer pairs with itself, with its log sent to its stdout.
test "slow: a pair within one process is logged" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    const name = support.uniqueName(&buf, "self_pair");
    const peer = Peer.spawn(&.{ "self-pair", name }) catch return t.fail("Failed to launch the peer");
    defer peer.deinit();
    _ = t.expect(peer.waitFor("a connection paired with a listener of this process", 10000), "The peer logs the same-process warning");
    _ = t.expect(peer.waitFor("DONE", 10000), "Its connections pair");
    _ = t.expect(peer.exitedWith(0), "It exits cleanly");
    return t.done();
}

// A client that connects with FIPC_FOREVER before its server starts, in another process, gets in once the server
// listens and accepts.
test "slow: a client process that connects before the server listens, without a timeout, gets in once it does" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    const name = support.uniqueName(&buf, "client_first");
    const peer = Peer.spawn(&.{ "connect-forever", name }) catch return t.fail("Failed to launch the client");
    defer peer.deinit();
    support.sleepMs(300); // the client waits for a server
    var listener: support.Listener = null;
    try t.checkEqual(c.fipc_stream_create(name, RING_BYTES, &listener), c.FIPC_OK, "listen");
    defer c.fipc_listener_close(listener);
    var server: support.Conn = null;
    try t.checkEqual(c.fipc_stream_accept(listener, &server, 10000), c.FIPC_OK, "accept returns the waiting client");
    defer c.fipc_close(server);
    _ = t.expect(peer.waitFor("C", 10000), "the client's connect returned");
    _ = t.expect(support.recvIs(server, 5000, "ping"), "its message arrives");
    peer.send("x");
    _ = t.expect(peer.exitedWith(0), "the client closes and exits");
    return t.done();
}

// Processes that listen, close the listener and exit (or find the name in use and exit) all terminate by themselves:
// nothing of the library keeps a process alive.
test "slow: processes that listen and close exit by themselves" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    const name = support.uniqueName(&buf, "listen_close");
    var peers: [5]*Peer = undefined;
    var launched: usize = 0;
    defer for (peers[0..launched]) |peer| peer.deinit();
    for (0..5) |_| {
        peers[launched] = Peer.spawn(&.{ "listen-close", name }) catch return t.fail("Failed to launch a child");
        launched += 1;
    }
    var reaped: usize = 0;
    for (peers) |peer| {
        if (peer.waitTimeout(10000)) reaped += 1 else peer.terminate();
    }
    if (!t.expect(reaped == 5, "All 5 child processes should terminate and be reaped (no zombies, no orphans)"))
        std.debug.print("  Reaped: {d}/5\n", .{reaped});
    return t.done();
}

// ---------------------------------------------------------------------------------------------------------------
// A reconnect's cost (Windows), and the library's unload (Windows) or staying loaded (Linux, macOS)
// ---------------------------------------------------------------------------------------------------------------

/// The process's CPU time (user + kernel) in milliseconds.
fn processCpuMs() u64 {
    var creation: support.win.FILETIME = undefined;
    var exit_ft: support.win.FILETIME = undefined;
    var kernel: support.win.FILETIME = undefined;
    var user: support.win.FILETIME = undefined;
    _ = support.win.GetProcessTimes(support.win.GetCurrentProcess(), &creation, &exit_ft, &kernel, &user);
    return (support.win.filetime(kernel) + support.win.filetime(user)) / 10000; // 100 ns units
}

/// Starts an echo server peer and connects to it: a client connected to a (re)started server.
fn connectToNewServer(t: *support.Test, name: [:0]const u8) !struct { *Peer, support.Conn } {
    const server = Peer.spawn(&.{ "echo", name }) catch return t.fail("Server launch failed");
    errdefer server.deinit();
    var client: support.Conn = null;
    if (c.fipc_stream_connect(name, &client, 10000) != c.FIPC_OK) return t.fail("connect failed");
    if (!server.waitFor("A", 10000)) {
        c.fipc_close(client);
        return t.fail("The server's accept");
    }
    return .{ server, client };
}

// A reconnect to a restarted server is quick and costs little CPU: over 5 cycles of a server killed and a new one
// started, the client's reconnect takes under 2 s on average (process creation included) and no cycle spends 100 ms
// of CPU (nothing spins while it waits).
test "slow: a reconnect to a restarted server is quick and doesn't spin" {
    if (!support.windows) return error.SkipZigTest;
    var t = try support.beginTimed(@src(), 60);
    defer t.end();
    var buf: [64]u8 = undefined;
    const name = support.uniqueName(&buf, "reconnect_cost");
    const cycles = 5;

    var total_wall_ms: u64 = 0;
    var max_cycle_cpu_ms: u64 = 0;
    for (0..cycles) |cycle| {
        {
            const server, const client = try connectToNewServer(&t, name);
            defer server.deinit();
            defer c.fipc_close(client);
            var message: [64]u8 = undefined;
            @memset(&message, 0xAB);
            try t.checkEqual(c.fipc_send(client, &message, message.len, 5000), c.FIPC_OK, "send");
            var echo: [64]u8 = undefined;
            var len: usize = 0;
            try t.checkEqual(c.fipc_recv(client, &echo, echo.len, &len, 5000), c.FIPC_OK, "the echo");
            server.kill();
            _ = t.expect(support.waitForEnd(client, END_MS), "the client learns of the server's death");
        }

        const cpu_before = processCpuMs();
        const wall_before = support.nowMs();
        const server, const client = try connectToNewServer(&t, name);
        defer server.deinit();
        defer c.fipc_close(client);
        const wall_ms = support.nowMs() - wall_before;
        const cpu_ms = processCpuMs() - cpu_before;
        total_wall_ms += wall_ms;
        max_cycle_cpu_ms = @max(max_cycle_cpu_ms, cpu_ms);
        std.debug.print("  cycle {d}: reconnect {d} ms, {d} ms of CPU\n", .{ cycle, wall_ms, cpu_ms });
    }
    _ = t.expect(total_wall_ms / cycles < 2000, "a reconnect takes under 2 s on average");
    _ = t.expect(max_cycle_cpu_ms < 100, "a reconnect spends under 100 ms of CPU (no busy-spin)");
    return t.done();
}

/// The shipped library's exports these tests call, from a module LoadLibraryW or dlopen loaded.
const Api = struct {
    stream_create: *const @TypeOf(c.fipc_stream_create),
    stream_accept: *const @TypeOf(c.fipc_stream_accept),
    stream_connect: *const @TypeOf(c.fipc_stream_connect),
    send: *const @TypeOf(c.fipc_send),
    recv: *const @TypeOf(c.fipc_recv),
    close: *const @TypeOf(c.fipc_close),
    listener_close: *const @TypeOf(c.fipc_listener_close),

    const Module = if (support.windows) win.HMODULE else *anyopaque;

    fn load(module: Module) ?Api {
        var api: Api = undefined;
        inline for (@typeInfo(Api).@"struct".field_names) |field_name| {
            @field(api, field_name) = @ptrCast(@alignCast(symbol(module, "fipc_" ++ field_name) orelse return null));
        }
        return api;
    }

    fn symbol(module: Module, symbol_name: [*:0]const u8) ?*anyopaque {
        return if (support.windows) win.GetProcAddress(module, symbol_name) else std.c.dlsym(module, symbol_name);
    }

    fn connectInto(api: Api, name: [:0]const u8, client: *support.Conn, result: *c.fipc_result_t) void {
        result.* = api.stream_connect(name, client, 5000);
    }

    /// A listener and a client (on another thread) through the library: they connect and a message crosses.
    fn pair(api: Api, name: [:0]const u8) bool {
        var listener: support.Listener = null;
        if (api.stream_create(name, RING_BYTES, &listener) != c.FIPC_OK) return false;
        defer api.listener_close(listener);
        var client: support.Conn = null;
        var connected: c.fipc_result_t = c.FIPC_TIMEOUT;
        const thread = std.Thread.spawn(.{}, connectInto, .{ api, name, &client, &connected }) catch return false;
        var server: support.Conn = null;
        const accepted = api.stream_accept(listener, &server, 5000);
        thread.join();
        defer api.close(client);
        defer api.close(server);
        if (accepted != c.FIPC_OK or connected != c.FIPC_OK) return false;
        var buf: [8]u8 = undefined;
        var len: usize = 0;
        return api.send(server, "hello", 5, 1000) == c.FIPC_OK and api.recv(client, &buf, buf.len, &len, 1000) == c.FIPC_OK and len == 5;
    }
};

// Once every connection and listener is closed on an application thread, the library has no thread left (and its
// handles are closed), so Windows can unload it: FreeLibrary really unloads it, and loaded again it works. Linux and
// macOS never unload the library (it keeps itself loaded: the next test), so the test is Windows only.
test "slow: Windows unloads the library once everything is closed, and it loads again" {
    if (!support.windows) return error.SkipZigTest;
    if (options.shared_lib.len == 0) return error.SkipZigTest;
    var t = try support.begin(@src(), 60);
    defer t.end();
    var buf: [64]u8 = undefined;
    const name = support.uniqueName(&buf, "unload");
    const gpa = std.testing.allocator;
    const path = std.unicode.utf8ToUtf16LeAllocZ(gpa, options.shared_lib) catch return t.fail("out of memory");
    defer gpa.free(path);
    const file_name = std.unicode.utf8ToUtf16LeAllocZ(gpa, std.Io.Dir.path.basename(options.shared_lib)) catch return t.fail("out of memory");
    defer gpa.free(file_name);

    // Round 0 settles what a first load leaves in the process for good (the loader's own bookkeeping)
    var threads_base: usize = 0;
    var handles_base: u32 = 0;
    for (0..3) |round| {
        const module = win.LoadLibraryW(path) orelse return t.fail("LoadLibraryW");
        const api = Api.load(module) orelse {
            _ = win.FreeLibrary(module);
            return t.fail("The library's exports");
        };
        const paired = api.pair(name);
        _ = win.FreeLibrary(module);
        const unloaded = win.GetModuleHandleW(file_name) == null;
        const threads = threadCount();
        var handles: u32 = 0;
        _ = support.win.GetProcessHandleCount(support.win.GetCurrentProcess(), &handles);
        std.debug.print("  round {d}: paired {}, unloaded {}, {d} threads, {d} handles\n", .{ round, paired, unloaded, threads, handles });
        try t.check(paired, "A pair connects through the loaded library");
        try t.check(unloaded, "FreeLibrary unloads the library: nothing of it is left running");
        if (round == 0) {
            threads_base = threads;
            handles_base = handles;
            continue;
        }
        _ = t.expect(threads <= threads_base, "No library thread is left");
        // A test process's handle count moves by a handle or two on its own
        _ = t.expect(handles <= handles_base + 2, "No handle of the library is left");
    }
    return t.done();
}

// Linux and macOS: the library keeps itself loaded for the life of the process, since its fork handlers can't be
// unregistered and the last close's join lets a worker leave the library's code after it. A dlclose of the only handle
// that loaded it, right after the last close, leaves it loaded: the loader still finds it, a call into it works, and a
// fork, which runs its fork handlers, succeeds. Without the pin the loader unloads it: the first check fails, and the
// call and the fork would crash.
test "slow: Linux and macOS keep the library loaded after its last close and a dlclose; a fork still works" {
    if (support.windows) return error.SkipZigTest;
    // A TSan build's library can't be loaded at run time (glibc: its thread-locals need the static TLS block)
    if (builtin.sanitize_thread) return error.SkipZigTest;
    if (options.shared_lib.len == 0) return error.SkipZigTest;
    var t = try support.begin(@src(), 60);
    defer t.end();
    var buf: [64]u8 = undefined;
    const name = support.uniqueName(&buf, "keep_loaded");
    const gpa = std.testing.allocator;
    const path = gpa.dupeSentinel(u8, options.shared_lib, 0) catch return t.fail("out of memory");
    defer gpa.free(path);

    const module = std.c.dlopen(path, .{ .NOW = true }) orelse return t.fail("dlopen");
    const api = Api.load(module) orelse {
        _ = std.c.dlclose(module);
        return t.fail("The library's exports");
    };
    const result_str: *const @TypeOf(c.fipc_result_str) = @ptrCast(@alignCast(Api.symbol(module, "fipc_result_str") orelse {
        _ = std.c.dlclose(module);
        return t.fail("fipc_result_str");
    }));
    const paired = api.pair(name);
    _ = std.c.dlclose(module);
    try t.check(paired, "A pair connects through the loaded library");

    // RTLD_NOLOAD finds a loaded library only
    const still_loaded = std.c.dlopen(path, .{ .NOW = true, .NOLOAD = true });
    if (still_loaded) |again| _ = std.c.dlclose(again);
    try t.check(still_loaded != null, "After its last close and a dlclose the library is still loaded");
    _ = t.expect(std.mem.eql(u8, std.mem.span(result_str(c.FIPC_TIMEOUT)), "FIPC_TIMEOUT"), "A call into it works");
    const pid = std.c.fork();
    if (pid == 0) std.c._exit(0);
    try t.check(pid > 0, "fork");
    var status: c_int = -1;
    _ = std.c.waitpid(pid, &status, 0);
    _ = t.expectEqual(status, 0, "The forked child exits normally");
    return t.done();
}

/// The threads of this process (Windows).
fn threadCount() usize {
    const snapshot = win.CreateToolhelp32Snapshot(win.TH32CS_SNAPTHREAD, 0);
    if (snapshot == std.os.windows.INVALID_HANDLE_VALUE) return 0;
    defer _ = support.win.CloseHandle(snapshot);
    const self = support.win.GetCurrentProcessId();
    var entry: win.THREADENTRY32 = .{};
    var count: usize = 0;
    var more = win.Thread32First(snapshot, &entry) != 0;
    while (more) : (more = win.Thread32Next(snapshot, &entry) != 0) {
        if (entry.th32OwnerProcessID == self) count += 1;
    }
    return count;
}

const win = struct {
    const HMODULE = std.os.windows.HMODULE;
    const TH32CS_SNAPTHREAD: u32 = 0x4;
    const THREADENTRY32 = extern struct {
        dwSize: u32 = @sizeOf(THREADENTRY32),
        cntUsage: u32 = 0,
        th32ThreadID: u32 = 0,
        th32OwnerProcessID: u32 = 0,
        tpBasePri: i32 = 0,
        tpDeltaPri: i32 = 0,
        dwFlags: u32 = 0,
    };
    extern "kernel32" fn LoadLibraryW(name: [*:0]const u16) callconv(.winapi) ?HMODULE;
    extern "kernel32" fn FreeLibrary(module: HMODULE) callconv(.winapi) c_int;
    extern "kernel32" fn GetProcAddress(module: HMODULE, name: [*:0]const u8) callconv(.winapi) ?*anyopaque;
    extern "kernel32" fn GetModuleHandleW(name: [*:0]const u16) callconv(.winapi) ?HMODULE;
    extern "kernel32" fn CreateToolhelp32Snapshot(flags: u32, pid: u32) callconv(.winapi) std.os.windows.HANDLE;
    extern "kernel32" fn Thread32First(snapshot: std.os.windows.HANDLE, entry: *THREADENTRY32) callconv(.winapi) c_int;
    extern "kernel32" fn Thread32Next(snapshot: std.os.windows.HANDLE, entry: *THREADENTRY32) callconv(.winapi) c_int;
};
