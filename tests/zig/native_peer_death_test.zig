//! A peer's death through the native Zig API (the module "fastipc": `Listener` and `Conn`): a listener or a client of
//! this process against the test peer's (peer.zig, the "native-" scenarios) in another process. The peer crashes at
//! each step of the handshake, the listener's and the client's, and at its watcher's start (through the crash hook),
//! and is paused. The pairs within one process, the races and the fork tests are the endpoint's
//! (src/zig/lifecycle/endpoint_test.zig). Each test ends with the descriptors or handles it started with, and the
//! testing allocator checks the endpoints and sessions. The connections of this process run on an `Io` of the test's
//! own, which it deinitializes before it counts: on Windows each of an `Io.Threaded`'s workers holds a handle to its
//! thread until the pool goes.

const std = @import("std");
const fastipc = @import("fastipc");
const ts = @import("support.zig");

const Listener = fastipc.Listener;
const Conn = fastipc.Conn;

/// The caller's `Io` of a test, and the handles to give back: those of the process when it began, once its workers
/// are gone. On Windows the count may end lower, or up to two higher: parts of the process outside the library open
/// or close a handle late.
const Caller = struct {
    threaded: std.Io.Threaded,
    handles: usize,

    var primed = false;

    fn init(c: *Caller) !void {
        // The process's first stack trace of an expected pipe status belongs to no test
        if (!primed) {
            primed = true;
            fastipc.test_hooks.primeStackTraces(ts.io());
        }
        // What a peer's first spawn and wait create in this test's `std.testing.io` (on Windows, a handle to the named
        // pipe device that it keeps) belongs to no connection. An unknown step: the peer exits at once
        var name_buf: [64]u8 = undefined;
        const peer = try ts.Peer.spawn(&.{ "native-crash", "listen", try std.mem.print(&name_buf, "warm-{d}", .{ts.pid()}), "none" });
        _ = peer.waitTimeout(10_000);
        peer.deinit();
        c.threaded = .init_single_threaded;
        c.threaded.allocator = std.heap.c_allocator;
        c.threaded.concurrent_limit = .unlimited;
        c.threaded.async_limit = .unlimited;
        c.handles = handleCount();
    }

    fn io(c: *Caller) std.Io {
        return c.threaded.io();
    }

    /// Joins the workers, then allows them a moment to close their thread handles (the join lets a worker go just
    /// before it does); true if the process is back to its handles.
    fn finish(c: *Caller) bool {
        c.threaded.deinit();
        for (0..100) |_| {
            if (handleCount() <= c.handles) break;
            ts.sleepMs(10);
        }
        return if (ts.windows) handleCount() <= c.handles + 2 else handleCount() == c.handles;
    }
};

fn within(ms: i64) std.Io.Timeout {
    return .{ .duration = .{ .raw = .fromMilliseconds(ms), .clock = .awake } };
}

/// The process's descriptors (Linux, macOS) or handles (Windows).
fn handleCount() usize {
    if (ts.windows) {
        var count: u32 = 0;
        std.debug.assert(ts.win.GetProcessHandleCount(ts.win.GetCurrentProcess(), &count) != 0);
        return count;
    }
    if (ts.macos) {
        var count: usize = 0;
        for (0..1024) |fd| {
            if (std.c.fcntl(@intCast(fd), std.c.F.GETFD) != -1) count += 1;
        }
        return count;
    }
    var count: usize = 0;
    for (0..1024) |fd| {
        if (std.os.linux.errno(std.os.linux.fcntl(@intCast(fd), std.os.linux.F.GETFD, 0)) == .SUCCESS) count += 1;
    }
    return count;
}

extern "ntdll" fn NtSuspendProcess(process: std.os.windows.HANDLE) callconv(.winapi) i32;
extern "ntdll" fn NtResumeProcess(process: std.os.windows.HANDLE) callconv(.winapi) i32;

/// Stops every thread of the peer (SIGSTOP; Windows: NtSuspendProcess, SuspendThread on each thread).
fn pause(peer: *ts.Peer) void {
    if (ts.windows) _ = NtSuspendProcess(peer.child.id.?) else _ = std.c.kill(peer.child.id.?, .STOP);
}

fn unpause(peer: *ts.Peer) void {
    if (ts.windows) _ = NtResumeProcess(peer.child.id.?) else _ = std.c.kill(peer.child.id.?, .CONT);
}

/// Checks a session between this process's connection and the next peer's: the peer is READY, and closes and exits on
/// this side's byte.
fn pairedWith(t: *ts.Test, next: *ts.Peer) !void {
    try t.check(next.waitFor("READY", 10_000), "the next peer connects");
    next.send("x");
    try t.check(next.exitedWith(0), "the next peer closes and exits");
}

test "slow: the listener's process crashes at each step: the client connects to the next listener, and nothing leaks" {
    var t = ts.begin(@src(), 120);
    defer t.end();
    var caller: Caller = undefined;
    try caller.init();
    for ([_][]const u8{ "accepted", "segment_created", "segment_sent", "ready_read", "watching" }) |step| {
        var name_buf: [64]u8 = undefined;
        const name = try std.mem.print(&name_buf, "listener-{s}-{d}", .{ step, ts.pid() });
        const crashing = try ts.Peer.spawn(&.{ "native-crash", "listen", name, step });
        defer crashing.deinit();
        try t.check(crashing.waitFor("OPEN", 10_000), "the crashing peer listens");
        // This client connects; the peer crashes on the way (or once the session is set up)
        const first = Conn.connect(caller.io(), std.testing.allocator, name, within(2000));
        if (first) |conn| {
            // The session's end reaches it
            var buf: [8]u8 = undefined;
            _ = t.expect(conn.recv(&buf, within(5000)) == error.Disconnected, "the client's session ends with the peer");
            conn.close();
        } else |_| {}
        try t.check(crashing.waitTimeout(10_000), "the crashing peer exits");
        try t.check(crashing.exitedWith(99), "the peer ended at its crash point");
        // The next listener
        const next = try ts.Peer.spawn(&.{ "native-pair", "listen", name });
        defer next.deinit();
        try t.check(next.waitFor("OPEN", 10_000), "the next peer listens");
        const conn = try Conn.connect(caller.io(), std.testing.allocator, name, within(10_000));
        defer conn.close();
        try pairedWith(&t, next);
    }
    _ = t.expect(caller.finish(), "the descriptors or handles are back");
    return t.done();
}

test "slow: the client's process crashes at each step: the listener accepts the next client, and nothing leaks" {
    var t = ts.begin(@src(), 120);
    defer t.end();
    var caller: Caller = undefined;
    try caller.init();
    for ([_][]const u8{ "connected", "segment_received", "attached", "ready_sent", "watching" }) |step| {
        var name_buf: [64]u8 = undefined;
        const name = try std.mem.print(&name_buf, "client-{s}-{d}", .{ step, ts.pid() });
        const l = try Listener.listen(caller.io(), std.testing.allocator, name, 4096);
        defer l.close();
        // The listener accepts on a thread of its own: the crashing client's handshake goes as far as its crash point
        var acceptor: Acceptor = .{ .l = l };
        const thread = try std.Thread.spawn(.{}, Acceptor.run, .{&acceptor});
        const crashing = try ts.Peer.spawn(&.{ "native-crash", "connect", name, step });
        defer crashing.deinit();
        const crashed = crashing.waitTimeout(10_000) and crashing.exitedWith(99);
        const next = try ts.Peer.spawn(&.{ "native-pair", "connect", name });
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
/// listener returns first: its session ends at once (its receive reads the end, within 2 s: under TSan on a slow
/// runner the end has come later than 200 ms), and it is closed; a client that crashed before is dropped inside the
/// accept, which goes on waiting.
const Acceptor = struct {
    l: Listener,
    result: fastipc.ListenError!Conn = error.Timeout,

    fn run(a: *Acceptor) void {
        a.result = while (true) {
            const accepted = a.l.accept(within(20_000)) catch |err| break err;
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
    var t = ts.begin(@src(), 60);
    defer t.end();
    var caller: Caller = undefined;
    try caller.init();
    {
        var name_buf: [64]u8 = undefined;
        const name = try std.mem.print(&name_buf, "pause-{d}", .{ts.pid()});
        const l = try Listener.listen(caller.io(), std.testing.allocator, name, 4096);
        defer l.close();
        const peer = try ts.Peer.spawn(&.{ "native-pair", "connect", name });
        defer peer.deinit();
        const conn = try l.accept(within(10_000));
        try t.check(peer.waitFor("READY", 10_000), "the peer connects");
        // No timer ends a session: 3 s of pause
        pause(peer);
        for (0..3) |_| {
            ts.sleepMs(1000);
            _ = t.expect(!conn.ended(), "connected while the peer is paused");
        }
        unpause(peer);
        _ = t.expect(!conn.ended(), "connected once it resumed");
        // This side ends the session while the peer is paused: the peer reads the end as soon as it runs again
        pause(peer);
        conn.close();
        ts.sleepMs(500);
        unpause(peer);
        _ = t.expect(peer.waitFor("DEAD", 1000), "the peer reports the end within 1 s of resuming");
        peer.send("x");
        _ = t.expect(peer.exitedWith(0), "the peer closes and exits");
    }
    _ = t.expect(caller.finish(), "the descriptors or handles are back");
    return t.done();
}
