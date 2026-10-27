//! Listeners, endpoints and their lifecycle on real OS objects, through a stream connection's server and client: a
//! listener and a client paired within this process, on two threads (`connect` waits for the listener's `accept`),
//! the name's claim, a session's end, a cancel and a close. The slow tier (`slow_tests`, compiled only into the slow
//! tier's unit tests) runs, on Linux and macOS, the fork tests, which fork this process. The stream connection's
//! handshake has its own tests (stream/handshake_test.zig), on this file's harness. Every test ends with the
//! descriptors or handles and the segment mappings it started with, and the testing allocator checks that no endpoint,
//! listener or session is left. They use the C boundary's process-wide `Io` (the native API's tests, in tests/zig,
//! pass a caller's `Io`).

const std = @import("std");
const builtin = @import("builtin");
const Io = std.Io;
const build_options = @import("build_options");
const control = @import("control.zig");
const endpoint_module = @import("endpoint.zig");
const platform = @import("../platform.zig");
const process_state = @import("process_state.zig");
const stream_client = @import("../stream/client.zig");
const stream_listener = @import("../stream/listener.zig");
const stream_server = @import("../stream/server.zig");
const test_hooks = @import("test_hooks.zig");

const testing = std.testing;
const linux = if (builtin.os.tag == .macos) @import("../platform/macos/test_sys.zig") else std.os.linux;

const Endpoint = endpoint_module.Endpoint;
const Listener = stream_listener.Listener;

pub const forever: Io.Timeout = .none;

pub fn within(ms: i64) Io.Timeout {
    return .{ .duration = .{ .raw = .fromMilliseconds(ms), .clock = .awake } };
}

pub fn listen(name: []const u8) endpoint_module.CreateError!*Listener {
    return stream_server.create(testing.allocator, name, 4096);
}

pub fn connect(name: []const u8, timeout: Io.Timeout) endpoint_module.Error!*Endpoint {
    return stream_client.connect(testing.allocator, name, timeout);
}

/// A name unique to this process and call.
pub fn uniqueName(buf: *[64]u8, comptime tag: []const u8) []const u8 {
    const counter = struct {
        var next = std.atomic.Value(u32).init(0);
    };
    return std.mem.print(buf, "t-" ++ tag ++ "-{d}-{d}", .{ processId(), counter.next.fetchAdd(1, .monotonic) }) catch unreachable;
}

const processId = platform.pid;

pub fn sleepMs(ms: i64) void {
    testing.io.sleep(.fromMilliseconds(ms), .awake) catch {};
}

pub fn nowMs() i64 {
    return Io.Clock.awake.now(testing.io).toMilliseconds();
}

/// A client that connects on a thread of its own, while the caller accepts: `connect` waits for the listener's
/// `accept`.
pub const Connector = struct {
    thread: std.Thread = undefined,
    result: endpoint_module.Error!*Endpoint = error.Timeout,
    /// Closes the connection as soon as `connect` returns (`brief`).
    close_at_once: bool = false,

    pub fn start(connector: *Connector, name: []const u8, timeout: Io.Timeout) !void {
        connector.thread = try std.Thread.spawn(.{}, run, .{ connector, name, timeout });
    }

    fn run(connector: *Connector, name: []const u8, timeout: Io.Timeout) void {
        connector.result = connect(name, timeout);
        if (connector.close_at_once) if (connector.result) |conn| endpoint_module.close(conn) else |_| {};
    }

    pub fn join(connector: *Connector) endpoint_module.Error!*Endpoint {
        connector.thread.join();
        return connector.result;
    }
};

/// A connected pair on `listener` (named `name`): the client's connection (`b`) and the one `accept` returned (`a`).
pub fn pair(listener: *Listener, name: []const u8) !struct { *Endpoint, *Endpoint } {
    var connector: Connector = .{};
    try connector.start(name, within(5000));
    const accepted = stream_server.accept(listener, within(5000));
    const b = try connector.join();
    errdefer endpoint_module.close(b);
    return .{ b, try accepted };
}

/// What a test must give back: the process's descriptors (Linux, macOS) or handles (Windows), and its segment mappings. The
/// first baseline warms up what a process creates once, including std's first stack trace of an expected pipe status.
/// On Windows parts of the process outside the library open or close a handle late, so a baseline waits for the count
/// to settle, the check allows a moment, and a test may end with fewer handles than it began with, or with
/// `windows_noise` more. A leak of the library's grows with its sessions, far past that, and the
/// platform tests count its objects exactly.
pub const Baseline = struct {
    handles: usize,
    mappings: usize,

    var warmed = false;

    pub fn take() !Baseline {
        if (!warmed) {
            warmed = true;
            var name_buf: [64]u8 = undefined;
            const name = uniqueName(&name_buf, "warm");
            const listener = try listen(name);
            defer listener.close();
            const b, const a = try pair(listener, name);
            endpoint_module.close(b);
            endpoint_module.close(a);
            test_hooks.primeStackTraces(testing.io);
        }
        var handles = handleCount();
        while (true) {
            sleepMs(5);
            const again = handleCount();
            if (again == handles) break;
            handles = again;
        }
        return .{ .handles = handles, .mappings = mappingCount() };
    }

    pub fn expectSame(b: Baseline) !void {
        for (0..100) |_| {
            if (handleCount() <= b.handles) break;
            sleepMs(10);
        }
        // Windows's count moves by itself (above); Linux's and macOS's descriptors are the library's own
        if (builtin.os.tag == .windows) try testing.expect(handleCount() <= b.handles + windows_noise) else try testing.expectEqual(b.handles, handleCount());
        try testing.expectEqual(b.mappings, mappingCount());
    }
};

/// The handles that parts of the process outside the library may open or close late (Windows).
const windows_noise = 2;

const handleCount = platform.openHandleCount;

/// Linux: the mappings of the segments (`/memfd:fastipc`). macOS: the shared, read-write regions with no VM tag.
/// Windows: every mapped view (`MEM_MAPPED`); the sections themselves are handles.
const mappingCount = platform.segmentMappingCount;

/// Waits until the endpoint's phase is `phase`: the peer's end reaches its watcher a moment after the peer's close.
pub fn awaitPhase(endpoint: *Endpoint, phase: endpoint_module.Phase) !void {
    const deadline = nowMs() + 5000;
    while (endpoint.loadPhase() != phase) {
        if (nowMs() > deadline) return error.TestUnexpectedResult;
        sleepMs(1);
    }
}

pub fn acceptResult(listener: *Listener, result: *?endpoint_module.Error!*Endpoint) void {
    result.* = stream_server.accept(listener, forever);
}

// === Fast: pairs within this process ===

test "a listener and a client on two threads: connect returns connected, accept returns the other end, one segment" {
    const base = try Baseline.take();
    var name_buf: [64]u8 = undefined;
    const name = uniqueName(&name_buf, "pair");
    const listener = try listen(name);
    errdefer listener.close();
    // Nobody connected: accept times out, having changed nothing
    try testing.expectError(error.Timeout, stream_server.accept(listener, within(0)));
    const b, const a = try pair(listener, name);
    try testing.expectEqual(endpoint_module.Phase.connected, b.loadPhase());
    try testing.expectEqual(@as(u32, 4096), b.hot.view_capacity.load(.monotonic)); // the listener's capacity
    try testing.expectEqual(endpoint_module.Phase.connected, a.loadPhase());
    try testing.expect(a.hot.view_server.load(.monotonic) and !b.hot.view_server.load(.monotonic));
    // One segment: what the listener's side writes into the s2c ring, the client sees
    a.hot.stream.data.load(.monotonic).?[5] = 0xa5;
    try testing.expectEqual(@as(u8, 0xa5), b.hot.stream.data.load(.monotonic).?[5]);
    endpoint_module.close(a);
    endpoint_module.close(b);
    listener.close();
    try base.expectSame();
}

test "the peer's close ends the session, and the listener accepts the next client" {
    const base = try Baseline.take();
    var name_buf: [64]u8 = undefined;
    const name = uniqueName(&name_buf, "dead");
    const listener = try listen(name);
    errdefer listener.close();
    for (0..3) |round| {
        // A connection has one session: to talk again, the client connects anew (docs/protocol.md §7)
        const b, const a = try pair(listener, name);
        if (round % 2 == 0) {
            endpoint_module.close(b);
            try awaitPhase(a, .peer_dead);
            endpoint_module.close(a);
        } else {
            endpoint_module.close(a);
            try awaitPhase(b, .peer_dead);
            endpoint_module.close(b);
        }
    }
    listener.close();
    try base.expectSame();
}

test "a connection outlives its listener: its session goes on, and the name is free for a new listener" {
    const base = try Baseline.take();
    var name_buf: [64]u8 = undefined;
    const name = uniqueName(&name_buf, "outlive");
    const listener = try listen(name);
    const b, const a = try pair(listener, name);
    listener.close();
    // The session goes on
    try testing.expectEqual(endpoint_module.Phase.connected, b.loadPhase());
    try testing.expectEqual(endpoint_module.Phase.connected, a.loadPhase());
    // A real difference of the OSes' rendezvous: a Windows client connects on the pipe instance itself
    if (builtin.os.tag != .windows) {
        // Linux, macOS: the name is free at once
        const l2 = try listen(name);
        l2.close();
    } else {
        // Windows: the accepted connection's pipe instance holds the name until it is closed
        try testing.expectError(error.AddrInUse, listen(name));
    }
    endpoint_module.close(a);
    try awaitPhase(b, .peer_dead);
    const l3 = try listen(name);
    l3.close();
    endpoint_module.close(b);
    try base.expectSame();
}

test "a name another listener holds is AddrInUse, and free again once it is closed" {
    const base = try Baseline.take();
    var name_buf: [64]u8 = undefined;
    const name = uniqueName(&name_buf, "inuse");
    const listener = try listen(name);
    try testing.expectError(error.AddrInUse, listen(name));
    listener.close();
    const again = try listen(name);
    again.close();
    try base.expectSame();
}

test "names and capacities that aren't valid are Invalid, before anything is created" {
    for ([_][]const u8{ "", ".hidden", "-flag", "a/b", "a b", &@as([300]u8, @splat('x')), &@as([246]u8, @splat('x')) }) |name| {
        try testing.expectError(error.Invalid, stream_server.create(testing.allocator, name, 4096));
        try testing.expectError(error.Invalid, connect(name, within(0)));
    }
    for ([_]usize{ 0, 512, 1000, 4097, 1 << 32 }) |capacity| {
        try testing.expectError(error.Invalid, stream_server.create(testing.allocator, "valid", capacity));
    }
}

test "a connection's cancel ends the waits of its data calls only; close is final" {
    const base = try Baseline.take();
    var name_buf: [64]u8 = undefined;
    const name = uniqueName(&name_buf, "conn-cancel");
    const listener = try listen(name);
    errdefer listener.close();
    const b, const a = try pair(listener, name);
    endpoint_module.cancel(b);
    try testing.expectEqual(endpoint_module.Phase.connected, b.loadPhase()); // the session goes on
    try testing.expectEqual(endpoint_module.Phase.connected, a.loadPhase());
    endpoint_module.close(a);
    endpoint_module.close(b);
    listener.close();
    try base.expectSame();
}

/// A client that closes as soon as it connects: accept returns its connection (its READY came before its end), whose
/// session ends, every time.
pub fn briefPeers(comptime tag: []const u8, rounds: usize) !void {
    const base = try Baseline.take();
    var name_buf: [64]u8 = undefined;
    const name = uniqueName(&name_buf, tag);
    const listener = try listen(name);
    errdefer listener.close();
    for (0..rounds) |_| {
        var connector: Connector = .{ .close_at_once = true };
        try connector.start(name, within(5000));
        const accepted = stream_server.accept(listener, within(5000));
        _ = try connector.join();
        const a = try accepted;
        try awaitPhase(a, .peer_dead);
        endpoint_module.close(a);
    }
    listener.close();
    try base.expectSame();
}

test {
    if (build_options.slow_tests) _ = slow_tests;
}

// === Slow: fork (Linux, macOS) ===

// The child runs only its checks and leaves with `_exit`. Under TSan the tests are skipped: TSan ends a child that
// starts a thread after forking from a parent with threads. Objects that a child touches use the C allocator, which is
// fork-safe; the testing allocator's lock may be inherited held.
const slow_tests = struct {
    fn listenC(name: []const u8) endpoint_module.CreateError!*Listener {
        return stream_server.create(std.heap.c_allocator, name, 4096);
    }

    fn connectC(name: []const u8, timeout: Io.Timeout) endpoint_module.Error!*Endpoint {
        return stream_client.connect(std.heap.c_allocator, name, timeout);
    }

    /// macOS: listens on the name once and closes, which removes the socket file and lock file that a killed listener
    /// of it left (docs/protocol.md §3.2); a name still held is left alone. Elsewhere nothing of a dead listener is
    /// left: nothing to do.
    fn clearName(name: []const u8) void {
        if (builtin.os.tag != .macos) return;
        (listenC(name) catch return).close();
    }

    /// Comptime-known, so that the rest of a fork test isn't even analyzed where it can't run.
    const fork_unsupported = !platform.has_fork or builtin.sanitize_thread;

    /// Sleeps without `Io`: a forked child makes no call on its parent's instances.
    fn childSleepMs(ms: u32) void {
        const ts: linux.timespec = .{ .sec = @intCast(ms / 1000), .nsec = @intCast(@as(u64, ms % 1000) * std.time.ns_per_ms) };
        _ = linux.nanosleep(&ts, null);
    }

    fn childNowMs() i64 {
        var ts: linux.timespec = undefined;
        _ = linux.clock_gettime(.MONOTONIC, &ts);
        return @as(i64, ts.sec) * 1000 + @divTrunc(ts.nsec, std.time.ns_per_ms);
    }

    fn makePipe() ![2]linux.fd_t {
        var fds: [2]linux.fd_t = undefined;
        if (linux.errno(linux.pipe2(&fds, .{ .CLOEXEC = true })) != .SUCCESS) return error.TestUnexpectedResult;
        return fds;
    }

    /// One byte from `fd` within `timeout_ms`; null at EOF or the deadline.
    fn readByte(fd: linux.fd_t, timeout_ms: i32) ?u8 {
        var fds = [1]std.posix.pollfd{.{ .fd = fd, .events = std.posix.POLL.IN, .revents = 0 }};
        const ready = std.posix.poll(&fds, timeout_ms) catch return null;
        if (ready != 1) return null;
        var byte: [1]u8 = undefined;
        return if (linux.read(fd, &byte, 1) == 1) byte[0] else null;
    }

    /// The checks of a forked child on a connection and a listener it inherited (docs/protocol.md §7): 0, or the number
    /// of the failed check.
    fn inheritedChecks(conn: *Endpoint, listener: *Listener) u8 {
        if (conn.hot.view_published.load(.monotonic)) return 1; // no session: every data call is Invalid
        if (!conn.handles.forked or !listener.handles.forked) return 2;
        if (stream_server.accept(listener, forever)) |_| return 3 else |err| if (err != error.Invalid) return 4;
        endpoint_module.cancel(conn); // only its bit
        listener.cancel();
        endpoint_module.close(conn); // frees what is complete, as memory only
        listener.close();
        return 0;
    }

    test "slow: fork: the child's inherited connection and listener fail and close, and its new connection gets in; the parent's death ends its session at once" {
        if (fork_unsupported) return error.SkipZigTest;
        var first_buf: [64]u8 = undefined;
        var second_buf: [64]u8 = undefined;
        var own_buf: [64]u8 = undefined;
        const first = uniqueName(&first_buf, "fork-a");
        const second = uniqueName(&second_buf, "fork-b");
        const own = uniqueName(&own_buf, "fork-own");
        defer clearName(own); // the killed forked parent's listener
        const l1 = try listenC(first);
        defer l1.close();
        const l2 = try listenC(second);
        defer l2.close();
        const report = try makePipe(); // the grandchild's result
        const go = try makePipe(); // the grandchild's leave to exit
        const pid = std.c.fork();
        if (pid == 0) forkedParent(first, second, own, report, go);
        _ = linux.close(report[1]);
        _ = linux.close(go[0]);
        defer _ = linux.close(report[0]);
        defer _ = linux.close(go[1]);
        const a1 = try stream_server.accept(l1, within(5000)); // the forked parent's connection, made before its fork
        defer endpoint_module.close(a1);
        const a2 = try stream_server.accept(l2, within(10_000)); // the grandchild's new connection
        defer endpoint_module.close(a2);
        const code = readByte(report[0], 10_000);
        _ = std.c.kill(pid, .KILL);
        _ = process_state.waitChild(pid, 5000);
        const killed = nowMs();
        const ended = awaitPhase(a1, .peer_dead);
        const elapsed = nowMs() - killed;
        const grandchild_connected = a2.loadPhase() == .connected; // it still runs
        _ = linux.write(go[1], "x", 1);
        try testing.expectEqual(@as(?u8, 0), code);
        try ended;
        try testing.expect(elapsed < 1000); // the grandchild's copy of the socket was closed at its fork
        try testing.expect(grandchild_connected);
        try awaitPhase(a2, .peer_dead); // the grandchild exited
    }

    /// The forked parent of the first fork test: a connection to the test's first listener, whose watcher runs on a
    /// worker of the process-wide `Io`, and a listener of its own on `own`. Then it forks the grandchild and waits to be
    /// killed.
    fn forkedParent(first: []const u8, second: []const u8, own: []const u8, report: [2]linux.fd_t, go: [2]linux.fd_t) noreturn {
        const a = connectC(first, within(5000)) catch std.c._exit(11);
        const listener = listenC(own) catch std.c._exit(12);
        if (std.c.fork() == 0) forkedGrandchild(a, listener, second, report, go);
        while (true) childSleepMs(1000);
    }

    fn forkedGrandchild(a: *Endpoint, own: *Listener, second: []const u8, report: [2]linux.fd_t, go: [2]linux.fd_t) noreturn {
        const code: u8 = code: {
            const inherited = inheritedChecks(a, own);
            if (inherited != 0) break :code inherited;
            const n = connectC(second, within(5000)) catch break :code 20;
            _ = n; // held until the grandchild exits
            break :code 0;
        };
        _ = linux.write(report[1], &[1]u8{code}, 1);
        var byte: [1]u8 = undefined;
        _ = linux.read(go[0], &byte, 1);
        std.c._exit(code);
    }

    test "slow: fork: a child forked while the listener waits in accept costs at most that connection; the client gets in" {
        if (fork_unsupported) return error.SkipZigTest;
        var name_buf: [64]u8 = undefined;
        const name = uniqueName(&name_buf, "fork-accept");
        const listener = try listenC(name);
        defer listener.close();
        const Waiting = struct {
            fn run(accepting_listener: *Listener, out: *?endpoint_module.Error!*Endpoint) void {
                out.* = stream_server.accept(accepting_listener, within(5000));
            }
        };
        var result: ?endpoint_module.Error!*Endpoint = null;
        const waiter = try std.Thread.spawn(.{}, Waiting.run, .{ listener, &result });
        sleepMs(10); // it waits in accept
        const pid = std.c.fork();
        if (pid == 0) std.c._exit(0);
        try testing.expectEqual(@as(?u8, 0), process_state.waitChild(pid, 5000));
        // The accept that was waiting at the fork drops the next connection; the client retries and gets in
        const b = connectC(name, within(5000));
        waiter.join();
        endpoint_module.close(try b);
        endpoint_module.close(try result.?);
    }

    const Churn = struct {
        stop: *std.atomic.Value(bool),
        failures: std.atomic.Value(u32) = .init(0),

        /// Sets pairs up, waits for each session's end, and closes.
        fn run(churn: *Churn) void {
            while (!churn.stop.load(.acquire)) {
                once() catch {
                    _ = churn.failures.fetchAdd(1, .monotonic);
                };
            }
        }

        fn once() !void {
            var name_buf: [64]u8 = undefined;
            const name = uniqueName(&name_buf, "churn");
            const listener = try listenC(name);
            defer listener.close();
            const Client = struct {
                fn run(n: []const u8, out: *?endpoint_module.Error!*Endpoint) void {
                    out.* = connectC(n, within(5000));
                }
            };
            var client: ?endpoint_module.Error!*Endpoint = null;
            const thread = try std.Thread.spawn(.{}, Client.run, .{ name, &client });
            const accepted = stream_server.accept(listener, within(5000));
            thread.join();
            const b = try client.?;
            const a = accepted catch |err| {
                endpoint_module.close(b);
                return err;
            };
            defer endpoint_module.close(a);
            endpoint_module.close(b);
            try awaitPhase(a, .peer_dead);
        }
    };

    test "slow: fork stress: 1,000 forks while other threads set connections up and close them; every child's listen and close return at once" {
        if (fork_unsupported) return error.SkipZigTest;
        // The process's first call comes before any fork, as it must on macOS (docs/platform-support.md)
        var warm_buf: [64]u8 = undefined;
        (try listenC(uniqueName(&warm_buf, "warm"))).close();
        var stop = std.atomic.Value(bool).init(false);
        var churns: [3]Churn = undefined;
        var threads: [3]std.Thread = undefined;
        for (&churns, &threads) |*churn, *thread| {
            churn.* = .{ .stop = &stop };
            thread.* = try std.Thread.spawn(.{}, Churn.run, .{churn});
        }
        var failed_children: usize = 0;
        for (0..1000) |_| {
            const pid = std.c.fork();
            if (pid == 0) {
                const start = childNowMs();
                var name_buf: [64]u8 = undefined;
                const n = listenC(uniqueName(&name_buf, "forked")) catch std.c._exit(1);
                n.close();
                std.c._exit(if (childNowMs() - start < 1000) 0 else 2);
            }
            if (process_state.waitChild(pid, 5000) != 0) failed_children += 1;
        }
        stop.store(true, .release);
        for (threads) |thread| thread.join();
        try testing.expectEqual(@as(usize, 0), failed_children);
        for (churns) |churn| try testing.expectEqual(@as(u32, 0), churn.failures.load(.monotonic));
        // The parent's connections still work
        try briefPeers("after-forks", 3);
    }
};
