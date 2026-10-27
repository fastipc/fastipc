//! A peer's death through the native Zig API (the module "fastipc": `Listener` and `Conn`): a stream's listener or a
//! client of this process against the test peer's (peer.zig, the "native-" scenarios) in another process. The peer
//! crashes at each step of the handshake, the listener's and the client's, and at its watcher's start (through the
//! crash hook), and is paused. The pairs within one process, the races and the fork tests are the endpoint's
//! (src/zig/core/endpoint_test.zig). Each test ends with the descriptors or handles it started with, and the testing
//! allocator checks the endpoints and sessions. The connections of this process run on the process-wide `Io`, whose
//! workers are joined when its last object closes: on Windows each of them holds a handle to its thread until then.

const std = @import("std");
const fastipc = @import("fastipc");
const support = @import("support.zig");

const Listener = fastipc.Listener;
const Conn = fastipc.Conn;

/// The process's handle count when the test began, which it must be back to once its connections are closed
/// (`support.handlesBack`).
const Caller = struct {
    handles: usize,

    var primed = false;

    fn init(caller: *Caller) !void {
        // Windows: the first stack trace of an expected pipe status loads debug info and keeps its files open; take it
        // once, before any test counts handles
        if (!primed) {
            primed = true;
            fastipc.test_hooks.primeStackTraces(support.io());
        }
        // Spawn and reap one peer before counting: the first spawn leaves handles in `std.testing.io` (on Windows,
        // one to the named-pipe device) that belong to no connection. The step "none" is unknown, so this peer exits
        // at once
        var name_buf: [64]u8 = undefined;
        const peer = try support.Peer.spawn(&.{ "native-crash", "listen", try std.mem.print(&name_buf, "warm-{d}", .{support.pid()}), "none" });
        _ = peer.waitTimeout(10_000);
        peer.deinit();
        caller.handles = support.handleCount();
    }

    /// True if the process is back to its handles, allowing the process-wide `Io`'s workers a moment to close their
    /// thread handles (the join lets a worker go just before it does).
    fn finish(caller: *Caller) bool {
        return support.handlesBack(caller.handles);
    }
};

const within = support.within;

/// Checks a session between this process's connection and the next peer's: the peer is READY, and closes and exits on
/// this side's byte.
fn pairedWith(t: *support.Test, next: *support.Peer) !void {
    try t.check(next.waitFor("READY", 10_000), "the next peer connects");
    next.send("x");
    try t.check(next.exitedWith(0), "the next peer closes and exits");
}

/// The name of the listener crash test's step.
fn listenerName(buf: *[64]u8, step: []const u8) [:0]const u8 {
    return std.mem.printSentinel(buf, "listener-{s}-{d}", .{ step, support.pid() }, 0) catch unreachable;
}

test "slow: the listener's process crashes at each step: the client connects to the next listener, and nothing leaks" {
    var t = try support.begin(@src(), 120);
    defer t.end();
    var caller: Caller = undefined;
    try caller.init();
    const steps = [_][]const u8{ "accepted", "segment_created", "segment_sent", "ready_read", "watching" };
    // macOS: remove the socket and lock files that each name's last listener (a peer that exits without closing it)
    // leaves behind; deferred, so it runs after the handle count
    defer for (steps) |step| {
        var name_buf: [64]u8 = undefined;
        support.clearName(listenerName(&name_buf, step));
    };
    for (steps) |step| {
        var name_buf: [64]u8 = undefined;
        const name = listenerName(&name_buf, step);
        const crashing = try support.Peer.spawn(&.{ "native-crash", "listen", name, step });
        defer crashing.deinit();
        try t.check(crashing.waitFor("OPEN", 10_000), "the crashing peer listens");
        // This client connects; the peer crashes on the way (or once the session is set up)
        const first = fastipc.streamConnect(std.testing.allocator, name, within(500));
        if (first) |conn| {
            // The session's end reaches it
            var buf: [8]u8 = undefined;
            _ = t.expect(conn.recv(&buf, within(5000)) == error.Disconnected, "the client's session ends with the peer");
            conn.close();
        } else |_| {}
        try t.check(crashing.waitTimeout(10_000), "the crashing peer exits");
        try t.check(crashing.exitedWith(99), "the peer ended at its crash point");
        // The next listener
        const next = try support.Peer.spawn(&.{ "native-pair", "listen", name });
        defer next.deinit();
        try t.check(next.waitFor("OPEN", 10_000), "the next peer listens");
        const conn = try fastipc.streamConnect(std.testing.allocator, name, within(10_000));
        defer conn.close();
        try pairedWith(&t, next);
    }
    _ = t.expect(caller.finish(), "the descriptors or handles are back");
    return t.done();
}

test "slow: the client's process crashes at each step: the listener accepts the next client, and nothing leaks" {
    var t = try support.begin(@src(), 120);
    defer t.end();
    var caller: Caller = undefined;
    try caller.init();
    for ([_][]const u8{ "connected", "segment_received", "attached", "ready_sent", "watching" }) |step| {
        var name_buf: [64]u8 = undefined;
        const name = try std.mem.print(&name_buf, "client-{s}-{d}", .{ step, support.pid() });
        const listener = try fastipc.streamCreate(std.testing.allocator, name, 4096);
        defer listener.close();
        // The listener accepts on a thread of its own: the crashing client's handshake goes as far as its crash point
        var acceptor: Acceptor = .{ .listener = listener };
        const thread = try std.Thread.spawn(.{}, Acceptor.run, .{&acceptor});
        const crashing = try support.Peer.spawn(&.{ "native-crash", "connect", name, step });
        defer crashing.deinit();
        const crashed = crashing.waitTimeout(10_000) and crashing.exitedWith(99);
        const next = try support.Peer.spawn(&.{ "native-pair", "connect", name });
        defer next.deinit();
        thread.join();
        try t.check(crashed, "the peer ended at its crash point");
        const conn = try acceptor.result;
        defer conn.close();
        try pairedWith(&t, next);
    }
    _ = t.expect(caller.finish(), "the descriptors or handles are back");
    return t.done();
}

/// Accepts until a client's session goes on: a client that crashed after its READY has a connection, which the
/// listener returns first: its session ends at once (its receive reads the end within 2 s, a margin for TSan on a slow
/// runner), and it is closed; a client that crashed before is dropped inside the accept, which goes on waiting. The
/// next client's first byte (peer.zig's `native-pair`) returns the receive at once.
const Acceptor = struct {
    listener: Listener,
    result: fastipc.AcceptError!Conn = error.Timeout,

    fn run(acceptor: *Acceptor) void {
        acceptor.result = while (true) {
            const accepted = acceptor.listener.accept(within(20_000)) catch |err| break err;
            var buf: [8]u8 = undefined;
            if (accepted.recv(&buf, within(2000))) |_| {} else |err| if (err == error.Disconnected) {
                accepted.close();
                continue;
            }
            break accepted;
        };
    }
};

test "slow: a paused peer isn't declared dead, and an end during its pause reaches it as soon as it resumes" {
    var t = try support.beginTimed(@src(), 60);
    defer t.end();
    var caller: Caller = undefined;
    try caller.init();
    {
        var name_buf: [64]u8 = undefined;
        const name = try std.mem.print(&name_buf, "pause-{d}", .{support.pid()});
        const listener = try fastipc.streamCreate(std.testing.allocator, name, 4096);
        defer listener.close();
        const peer = try support.Peer.spawn(&.{ "native-pair", "connect", name });
        defer peer.deinit();
        const conn = try listener.accept(within(10_000));
        try t.check(peer.waitFor("READY", 10_000), "the peer connects");
        // No timer ends a session: 3 s of pause
        peer.pause();
        for (0..3) |_| {
            support.sleepMs(1000);
            _ = t.expect(!conn.ended(), "connected while the peer is paused");
        }
        peer.unpause();
        _ = t.expect(!conn.ended(), "connected once it resumed");
        // This side ends the session while the peer is paused: the peer reads the end as soon as it runs again
        peer.pause();
        conn.close();
        support.sleepMs(500);
        peer.unpause();
        _ = t.expect(peer.waitFor("DEAD", 1000), "the peer reports the end within 1 s of resuming");
        peer.send("x");
        _ = t.expect(peer.exitedWith(0), "the peer closes and exits");
    }
    _ = t.expect(caller.finish(), "the descriptors or handles are back");
    return t.done();
}

test "slow: a broadcast through the native API: subscribers of both roles, the publisher's close, and nothing leaks" {
    var t = try support.begin(@src(), 60);
    defer t.end();
    var caller: Caller = undefined;
    try caller.init();
    for (0..3) |_| {
        var name_buf: [64]u8 = undefined;
        const name = try std.mem.print(&name_buf, "native-broadcast-{d}", .{support.pid()});
        const publisher = try fastipc.broadcastCreate(std.testing.allocator, name, 65536, 4, -1);
        try t.check(publisher.maxPiece() == 16384, "max_piece: a quarter of the capacity");
        // The publisher's thread joins: its process admits the subscribers on a task of the library's
        const lossy = try fastipc.broadcastJoin(std.testing.allocator, name, .lossy, within(10_000));
        defer lossy.close();
        const gated = try fastipc.broadcastJoin(std.testing.allocator, name, .gated, within(10_000));
        defer gated.close();
        _ = t.expectEqual(try publisher.waitForMembers(2, within(5000)), 2, "both are counted");
        try publisher.send("one", .none);
        const room = try publisher.acquire(5, .none);
        @memcpy(room, "two!!");
        try publisher.commit(3);
        var buf: [16]u8 = undefined;
        _ = t.expect(std.mem.eql(u8, buf[0..try lossy.recv(&buf, within(1000))], "one"), "the lossy subscriber's first");
        _ = t.expect(std.mem.eql(u8, buf[0..try lossy.recv(&buf, within(1000))], "two"), "and second");
        _ = t.expect(std.mem.eql(u8, try gated.recvAcquire(within(1000)), "one"), "the gated subscriber's, in place");
        gated.release();
        _ = t.expect(std.mem.eql(u8, buf[0..try gated.recv(&buf, within(1000))], "two"), "and copied");
        _ = t.expect(lossy.send("x", .none) == error.Invalid, "a subscriber's send");
        _ = t.expect(publisher.recv(&buf, .none) == error.Invalid, "the publisher's receive");
        publisher.close();
        _ = t.expect(lossy.recv(&buf, within(5000)) == error.Disconnected, "the publisher's close ends the broadcast");
        _ = t.expect(gated.recv(&buf, within(5000)) == error.Disconnected, "for every subscriber");
        _ = t.expect(lossy.ended() and gated.ended(), "ended");
        // The name is free at once
        const again = try fastipc.broadcastCreate(std.testing.allocator, name, 4096, 1, -1);
        again.close();
    }
    _ = t.expect(caller.finish(), "the descriptors or handles are back");
    return t.done();
}
