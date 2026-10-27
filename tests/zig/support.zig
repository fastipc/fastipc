//! Shared setup of the C-API test suite (tests/zig): the C API under test, a per-test context with assertions that
//! go on and a per-test timeout, timing and naming helpers, the helpers for a listener and its two connections in this
//! process, and `Peer`, the other process of the multi-process tests.
//!
//! The tests call the library only through its exported C API (module "c", translated from include/fipc.h). The tests
//! that play a faulty peer, writing a ring's frames by hand, are the library's unit tests
//! (src/zig/stream/data_path_test.zig).

const std = @import("std");
const builtin = @import("builtin");
const options = @import("test_options");

pub const c = @import("c");

pub const windows = builtin.os.tag == .windows;
pub const macos = builtin.os.tag == .macos;

/// The `Io` of the running test (std's test runner sets one up per test).
pub fn io() std.Io {
    return std.testing.io;
}

// ---------------------------------------------------------------------------------------------------------------
// A test's context: assertions and the per-test timeout
// ---------------------------------------------------------------------------------------------------------------

/// A failed assertion prints its description and marks the test failed, and the test goes on, so a run reports every
/// failed check; `fail` and `check` also end the test. Every test starts with `begin` (or `beginTimed`) and ends with
/// `defer t.end()` and `return t.done()`.
pub const Test = struct {
    name: []const u8,
    failed: bool = false,

    /// Returns `ok`; a false `ok` prints `description` and fails the test at its end.
    pub fn expect(t: *Test, ok: bool, description: []const u8) bool {
        if (!ok) {
            t.failed = true;
            std.debug.print("ASSERTION FAILED: {s}\n", .{description});
        }
        return ok;
    }

    /// Whether two integers are equal; if not, prints both and `description`, and fails the test at its end.
    pub fn expectEqual(t: *Test, actual: anytype, expected: anytype, description: []const u8) bool {
        const a: i128 = @intCast(actual);
        const e: i128 = @intCast(expected);
        if (a != e) {
            t.failed = true;
            std.debug.print("ASSERTION FAILED - Expected {d}, got {d}: {s}\n", .{ e, a, description });
            return false;
        }
        return true;
    }

    /// Fails the test and ends it: `return t.fail(description)`.
    pub fn fail(t: *Test, description: []const u8) error{TestFailed} {
        _ = t.expect(false, description);
        return error.TestFailed;
    }

    /// `expect`, and ends the test if `ok` is false: `try t.check(ok, description)`.
    pub fn check(t: *Test, ok: bool, description: []const u8) error{TestFailed}!void {
        if (!t.expect(ok, description)) return error.TestFailed;
    }

    /// `expectEqual`, and ends the test if the integers differ.
    pub fn checkEqual(t: *Test, actual: anytype, expected: anytype, description: []const u8) error{TestFailed}!void {
        if (!t.expectEqual(actual, expected, description)) return error.TestFailed;
    }

    /// The end of the test: fails it if any assertion failed on the way.
    pub fn done(t: *Test) error{TestFailed}!void {
        if (t.failed) return error.TestFailed;
    }

    /// Stops the test's timeout (`defer t.end()`).
    pub fn end(t: *Test) void {
        _ = t;
        watchdog.stop();
    }
};

/// Starts a test that may take `timeout_s` seconds: past it, the test process prints the test's name and exits with
/// 124, and `zig build` reports that test as failed and goes on with the next one in a new process. In a run of one
/// shard (`FIPC_TEST_SHARD`), a test of another shard is skipped.
pub fn begin(comptime src: std.lang.SourceLocation, timeout_s: u32) error{SkipZigTest}!Test {
    if (!inShard(src, false)) return error.SkipZigTest;
    watchdog.start(src.fn_name, timeout_s);
    return .{ .name = src.fn_name };
}

/// `begin` for a test whose checks measure time (a death seen within a bound, a wake-up, the CPU a wait spends): in a
/// sharded run it belongs to shard 1, which the slow tier runs after its other runs, beside only the slow unit tests,
/// so no other shard's or stress test's processes or threads load the machine meanwhile.
pub fn beginTimed(comptime src: std.lang.SourceLocation, timeout_s: u32) error{SkipZigTest}!Test {
    if (!inShard(src, true)) return error.SkipZigTest;
    watchdog.start(src.fn_name, timeout_s);
    return .{ .name = src.fn_name };
}

/// Whether the test at `src` belongs to this run. `zig build test-slow` runs its test executable once per shard, at
/// the same time, with `FIPC_TEST_SHARD=<k>/<n>` (build/tests.zig): shard 1 has the timed tests (`beginTimed`), and
/// the others are dealt to shards 2 to n by their place in the executable's list of tests. Without the variable a run
/// has every test.
fn inShard(comptime src: std.lang.SourceLocation, timed: bool) bool {
    const value = std.testing.environ.getAlloc(std.heap.page_allocator, "FIPC_TEST_SHARD") catch return true;
    defer std.heap.page_allocator.free(value);
    const slash = std.mem.findScalar(u8, value, '/') orelse return true;
    const shard = std.fmt.parseInt(usize, value[0..slash], 10) catch return true;
    const shards = std.fmt.parseInt(usize, value[slash + 1 ..], 10) catch return true;
    if (shards < 2) return true;
    if (timed) return shard == 1;
    // The test's full name in the list: its file's path (with `\` on Windows), its folders and its name joined by dots
    const full_name = comptime name: {
        var path = src.file[0 .. src.file.len - ".zig".len].*;
        for (&path) |*char| {
            if (char.* == '/' or char.* == '\\') char.* = '.';
        }
        const dotted = path;
        break :name dotted ++ "." ++ src.fn_name;
    };
    const index = for (builtin.test_functions, 0..) |test_fn, i| {
        if (std.mem.eql(u8, test_fn.name, full_name)) break i;
    } else std.debug.panic("support.inShard: no test named \"{s}\" in the executable's list", .{full_name});
    return shard == 2 + index % (shards - 1);
}

const watchdog = struct {
    var stopped: std.Io.Event = .unset;
    var thread: ?std.Thread = null;

    fn start(test_name: []const u8, timeout_s: u32) void {
        stop();
        stopped = .unset;
        thread = std.Thread.spawn(.{}, run, .{ test_name, timeout_s }) catch null;
    }

    fn stop() void {
        const t = thread orelse return;
        stopped.set(io());
        t.join();
        thread = null;
    }

    fn run(test_name: []const u8, timeout_s: u32) void {
        const deadline: std.Io.Clock.Timestamp = .fromNow(io(), .{ .clock = .awake, .raw = .fromSeconds(timeout_s) });
        while (!stopped.isSet()) {
            if (deadline.durationFromNow(io()).raw.nanoseconds <= 0) {
                std.debug.print("\n*** TEST TIMEOUT: {s} exceeded its {d} s timeout and was killed ***\n", .{ test_name, timeout_s });
                hardExit(124);
            }
            stopped.waitTimeout(io(), .{ .deadline = deadline }) catch {};
        }
    }
};

/// Ends the process at once, without cleanup.
fn hardExit(code: u8) noreturn {
    if (windows) {
        win.ExitProcess(code);
    } else {
        std.c._exit(code);
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Time, names, atomics, result names
// ---------------------------------------------------------------------------------------------------------------

pub fn sleepMs(ms: u64) void {
    io().sleep(.fromMilliseconds(@intCast(ms)), .awake) catch {};
}

/// A monotonic clock in milliseconds, for elapsed times.
pub fn nowMs() u64 {
    return @intCast(std.Io.Clock.awake.now(io()).toMilliseconds());
}

/// The same clock in nanoseconds.
pub fn nowNs() u64 {
    return @intCast(std.Io.Clock.awake.now(io()).nanoseconds);
}

/// A native call's timeout of `ms` milliseconds.
pub fn within(ms: i64) std.Io.Timeout {
    return .{ .duration = .{ .raw = .fromMilliseconds(ms), .clock = .awake } };
}

/// Busy-waits until `deadline_ns` (`nowNs`): finer than a sleep.
pub fn spinUntil(deadline_ns: u64) void {
    while (nowNs() < deadline_ns) std.atomic.spinLoopHint();
}

// ---------------------------------------------------------------------------------------------------------------
// CPU pinning
// ---------------------------------------------------------------------------------------------------------------

/// Two CPUs this process may run on, for the two sides of a pinned test: the first allowed one and the one halfway
/// through the allowed list (with the usual numbering, not two hyperthreads of one core). Null with fewer than two, and
/// on macOS, which has no CPU affinity.
pub fn twoCpus() ?[2]u32 {
    if (macos) return null;
    var allowed: [256]u32 = undefined;
    var count: usize = 0;
    if (windows) {
        var process_mask: usize = 0;
        var system_mask: usize = 0;
        if (win.GetProcessAffinityMask(win.GetCurrentProcess(), &process_mask, &system_mask) == 0) return null;
        for (0..@bitSizeOf(usize)) |cpu| {
            if (process_mask >> @intCast(cpu) & 1 != 0) {
                allowed[count] = @intCast(cpu);
                count += 1;
            }
        }
    } else {
        var set: std.os.linux.cpu_set_t = @splat(0);
        if (std.os.linux.sched_getaffinity(0, @sizeOf(std.os.linux.cpu_set_t), &set) != 0) return null;
        for (set, 0..) |word, i| {
            for (0..@bitSizeOf(usize)) |bit| {
                if (word >> @intCast(bit) & 1 != 0 and count < allowed.len) {
                    allowed[count] = @intCast(i * @bitSizeOf(usize) + bit);
                    count += 1;
                }
            }
        }
    }
    if (count < 2) return null;
    return .{ allowed[0], allowed[count / 2] };
}

/// Pins the calling thread to `cpu`; false if the system refused, and on macOS, which has no CPU affinity.
pub fn pinThread(cpu: u32) bool {
    if (macos) return false;
    if (windows) return win.SetThreadAffinityMask(std.os.windows.GetCurrentThread(), @as(usize, 1) << @intCast(cpu)) != 0;
    var set: std.os.linux.cpu_set_t = @splat(0);
    set[cpu / @bitSizeOf(usize)] |= @as(usize, 1) << @intCast(cpu % @bitSizeOf(usize));
    std.os.linux.sched_setaffinity(0, &set) catch return false;
    return true;
}

/// The process's descriptors (Linux, macOS) or handles (Windows).
pub fn handleCount() usize {
    if (windows) {
        var count: u32 = 0;
        std.debug.assert(win.GetProcessHandleCount(win.GetCurrentProcess(), &count) != 0);
        return count;
    }
    if (macos) {
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

/// Whether the process is back to `before` descriptors or handles, waiting up to a second for them to close. On Windows
/// the count may end lower, or up to two higher: parts of the process outside the library open or close a handle late.
pub fn handlesBack(before: usize) bool {
    for (0..100) |_| {
        if (handleCount() <= before) break;
        sleepMs(10);
    }
    const now = handleCount();
    return if (windows) now <= before + 2 else now == before;
}

/// Where the OS limits a process's descriptors (Linux, macOS), raises the soft limit to `count`; false if the hard
/// limit is lower.
pub fn allowHandles(count: usize) bool {
    if (windows) return true;
    var limit = std.posix.getrlimit(.NOFILE) catch return false;
    if (limit.cur >= count) return true;
    if (limit.max < count) return false;
    limit.cur = count;
    std.posix.setrlimit(.NOFILE, limit) catch return false;
    return true;
}

pub fn pid() u32 {
    return if (windows) win.GetCurrentProcessId() else @intCast(std.c.getpid());
}

/// "<prefix>_<pid>": the tests' per-process names.
pub fn uniqueName(buf: []u8, prefix: []const u8) [:0]const u8 {
    return std.mem.printSentinel(buf, "{s}_{d}", .{ prefix, pid() }, 0) catch unreachable;
}

pub fn resultName(result: c.fipc_result_t) []const u8 {
    return std.mem.span(c.fipc_result_str(result));
}

pub const Listener = ?*c.fipc_listener_t;
pub const Queue = ?*c.fipc_queue_t;
pub const Service = ?*c.fipc_service_t;
pub const Conn = ?*c.fipc_conn_t;

// ---------------------------------------------------------------------------------------------------------------
// A listener and its two connections in this process
// ---------------------------------------------------------------------------------------------------------------

/// A client's `fipc_stream_connect` on a thread of its own: `connect` waits for the listener's `fipc_stream_accept`, so a client and
/// its server in one process run on different threads. `start`, then accept on the calling thread, then `join`.
pub const Connector = struct {
    thread: ?std.Thread = null,
    result: c.fipc_result_t = c.FIPC_TIMEOUT,
    conn: Conn = null,

    /// Starts `fipc_stream_connect(name, timeout_ms)` on a new thread; false if no thread could be started.
    pub fn start(connector: *Connector, name: [*:0]const u8, timeout_ms: c_int) bool {
        connector.* = .{};
        connector.thread = std.Thread.spawn(.{}, run, .{ connector, name, timeout_ms }) catch return false;
        return true;
    }

    fn run(connector: *Connector, name: [*:0]const u8, timeout_ms: c_int) void {
        connector.result = c.fipc_stream_connect(name, &connector.conn, timeout_ms);
    }

    /// Waits for the connect to return; its result, with the connection in `conn` (into `out` if given).
    pub fn join(connector: *Connector, out: ?*Conn) c.fipc_result_t {
        if (connector.thread) |thread| thread.join();
        connector.thread = null;
        if (out) |o| o.* = connector.conn;
        return connector.result;
    }
};

/// Connects a client to `name` on another thread while `listener` accepts on this one: both connections, or false with
/// nothing left open.
pub fn connectAccept(listener: Listener, name: [*:0]const u8, client: *Conn, server: *Conn, timeout_ms: c_int) bool {
    var connector: Connector = .{};
    if (!connector.start(name, timeout_ms)) return false;
    const accepted = c.fipc_stream_accept(listener, server, timeout_ms);
    const connected = connector.join(client);
    if (accepted == c.FIPC_OK and connected == c.FIPC_OK) return true;
    if (connected == c.FIPC_OK) c.fipc_close(client.*);
    if (accepted == c.FIPC_OK) c.fipc_close(server.*);
    client.* = null;
    server.* = null;
    return false;
}

/// A listener, the connection it accepted (`server`, which writes the s2c ring) and the client's (`client`), in this
/// process: the client connects on another thread (`connectAccept`).
pub const Pair = struct {
    listener: Listener = null,
    server: Conn = null,
    client: Conn = null,

    /// Listens on `name` with rings of `capacity` bytes, connects and accepts; false (nothing left open) on a failure.
    pub fn open(pair: *Pair, name: [*:0]const u8, capacity: usize) bool {
        pair.* = .{};
        if (c.fipc_stream_create(name, capacity, &pair.listener) != c.FIPC_OK) return false;
        if (!connectAccept(pair.listener, name, &pair.client, &pair.server, 5000)) {
            pair.close();
            return false;
        }
        return true;
    }

    /// Connects a new client and accepts it, once the previous connections are closed; false on a failure.
    pub fn reconnect(pair: *Pair, name: [*:0]const u8) bool {
        return connectAccept(pair.listener, name, &pair.client, &pair.server, 5000);
    }

    /// Closes what is open: the client, the server's connection, the listener.
    pub fn close(pair: *Pair) void {
        c.fipc_close(pair.client);
        c.fipc_close(pair.server);
        c.fipc_listener_close(pair.listener);
        pair.* = .{};
    }
};

/// `Pair.open` on the per-process name "<prefix>_<pid>"; ends the test if it fails.
pub fn openPair(t: *Test, pair: *Pair, comptime prefix: []const u8, capacity: usize) !void {
    var buf: [64]u8 = undefined;
    try t.check(pair.open(uniqueName(&buf, prefix), capacity), "a listener and its two connections");
}

// ---------------------------------------------------------------------------------------------------------------
// Broadcasts, work queues and services in this process
// ---------------------------------------------------------------------------------------------------------------

/// Joins `name` as `role`: a broadcast's subscriber (`FIPC_SUBSCRIBER_LOSSY`, `FIPC_SUBSCRIBER_GATED`) by
/// `fipc_broadcast_join`, a work queue's member (`FIPC_PRODUCER`, `FIPC_CONSUMER`) by `fipc_queue_join`, a service's
/// (`FIPC_CLIENT`, `FIPC_SERVER`) by `fipc_service_join`.
pub fn joinAs(name: [*:0]const u8, role: c_int, conn: *Conn, timeout_ms: c_int) c.fipc_result_t {
    return switch (role) {
        c.FIPC_PRODUCER, c.FIPC_CONSUMER => c.fipc_queue_join(name, role, conn, timeout_ms),
        c.FIPC_CLIENT, c.FIPC_SERVER => c.fipc_service_join(name, role, conn, timeout_ms),
        else => c.fipc_broadcast_join(name, role, conn, timeout_ms),
    };
}

/// A join (`joinAs`) on a thread of its own, for a test that does something else while the join waits. `start`, then
/// `join`.
pub const Joiner = struct {
    thread: ?std.Thread = null,
    result: c.fipc_result_t = c.FIPC_TIMEOUT,
    conn: Conn = null,
    /// How long the join took.
    elapsed_ms: u64 = 0,

    /// Starts `joinAs(name, role, timeout_ms)` on a new thread; false if no thread could be started.
    pub fn start(joiner: *Joiner, name: [*:0]const u8, role: c_int, timeout_ms: c_int) bool {
        joiner.* = .{};
        joiner.thread = std.Thread.spawn(.{}, run, .{ joiner, name, role, timeout_ms }) catch return false;
        return true;
    }

    fn run(joiner: *Joiner, name: [*:0]const u8, role: c_int, timeout_ms: c_int) void {
        const begun = nowMs();
        joiner.result = joinAs(name, role, &joiner.conn, timeout_ms);
        joiner.elapsed_ms = nowMs() - begun;
    }

    /// Waits for the join to return; its result, with the connection in `conn` (into `out` if given).
    pub fn join(joiner: *Joiner, out: ?*Conn) c.fipc_result_t {
        if (joiner.thread) |thread| thread.join();
        joiner.thread = null;
        if (out) |o| o.* = joiner.conn;
        return joiner.result;
    }
};

/// Joins `name` as `role` on this thread (the owner's process admits the joiner by itself): its connection, or false
/// with nothing left open.
pub fn joinNow(name: [*:0]const u8, role: c_int, member: *Conn, timeout_ms: c_int) bool {
    const joined = joinAs(name, role, member, timeout_ms);
    if (joined == c.FIPC_OK) return true;
    std.debug.print("  join: {s}\n", .{resultName(joined)});
    member.* = null;
    return false;
}

/// A broadcast's publisher in this process.
pub const Broadcast = struct {
    publisher: Conn = null,

    /// `fipc_broadcast_create(name, ...)`; false (nothing left open) on a failure.
    pub fn open(broadcast: *Broadcast, name: [*:0]const u8, capacity: usize, max_subscribers: u32, max_stall_ms: c_int) bool {
        broadcast.* = .{};
        const result = c.fipc_broadcast_create(name, capacity, max_subscribers, max_stall_ms, &broadcast.publisher);
        if (result != c.FIPC_OK) std.debug.print("  broadcast_create on '{s}': {s}\n", .{ name, resultName(result) });
        return result == c.FIPC_OK;
    }

    /// Closes the publisher: the broadcast ends.
    pub fn close(broadcast: *Broadcast) void {
        c.fipc_close(broadcast.publisher);
        broadcast.* = .{};
    }
};

/// `Broadcast.open` on the per-process name "<prefix>_<pid>" (into `name_buf`); ends the test if it fails.
pub fn openBroadcast(t: *Test, broadcast: *Broadcast, name_buf: []u8, comptime prefix: []const u8, capacity: usize, max_subscribers: u32, max_stall_ms: c_int) ![:0]const u8 {
    const name = uniqueName(name_buf, prefix);
    try t.check(broadcast.open(name, capacity, max_subscribers, max_stall_ms), "a broadcast");
    return name;
}

/// A checked message of a broadcast stream (stamp.zig).
pub const Stamp = @import("stamp.zig");

/// Fills `buf` with bytes that depend on their position and on `seed`.
pub fn pattern(buf: []u8, seed: u8) void {
    for (buf, 0..) |*byte, i| byte.* = seed +% @as(u8, @truncate(i *% 7));
}

/// Receives the next message of `conn` into `buf`, as `fipc_recv`.
pub fn recv(conn: Conn, buf: []u8, len: *usize, timeout_ms: c_int) c.fipc_result_t {
    return c.fipc_recv(conn, buf.ptr, buf.len, len, timeout_ms);
}

/// Whether the next message of `conn`, within `timeout_ms`, is `expected`.
pub fn recvIs(conn: Conn, timeout_ms: c_int, expected: []const u8) bool {
    var buf: [512]u8 = undefined;
    var len: usize = 0;
    return recv(conn, &buf, &len, timeout_ms) == c.FIPC_OK and std.mem.eql(u8, buf[0..len], expected);
}

/// Sends `bytes` on `conn`, as `fipc_send`.
pub fn send(conn: Conn, bytes: []const u8, timeout_ms: c_int) c.fipc_result_t {
    return c.fipc_send(conn, bytes.ptr, bytes.len, timeout_ms);
}

/// Whether a listener claims `name` at once (nobody holds it): `fipc_stream_create` returns OK well within 500 ms, and
/// the listener is closed again.
pub fn freeAtOnce(name: [*:0]const u8) bool {
    var listener: Listener = null;
    const start = nowMs();
    const result = c.fipc_stream_create(name, 4096, &listener);
    const elapsed = nowMs() - start;
    c.fipc_listener_close(listener);
    if (result != c.FIPC_OK or elapsed >= 500) std.debug.print("  listen on '{s}': {s} after {d} ms\n", .{ name, resultName(result), elapsed });
    return result == c.FIPC_OK and elapsed < 500;
}

/// macOS: listens on the name once and closes, which removes the socket file and lock file that a killed or crashed
/// listener of it left (docs/protocol.md §3.2), so a test leaves nothing in the user's directory. A name still held
/// is left alone. Elsewhere nothing of a dead listener is left: nothing to do.
pub fn clearName(name: [*:0]const u8) void {
    if (!macos) return;
    var listener: Listener = null;
    if (c.fipc_stream_create(name, 4096, &listener) == c.FIPC_OK) c.fipc_listener_close(listener);
}

/// The session's end, as a send reports it: `fipc_send_acquire` of one byte with timeout 0 returns DISCONNECTED once
/// the peer ended the session, whatever is still queued for this side (it receives nothing), and otherwise reserves a
/// byte, which the next send drops.
pub fn ended(conn: Conn) bool {
    var buf: ?*anyopaque = null;
    return c.fipc_send_acquire(conn, 1, &buf, 0) == c.FIPC_DISCONNECTED;
}

/// Polls `ended` every 5 ms for up to `timeout_ms`.
pub fn waitForEnd(conn: Conn, timeout_ms: u32) bool {
    const deadline = nowMs() + timeout_ms;
    while (!ended(conn)) {
        if (nowMs() >= deadline) return false;
        sleepMs(5);
    }
    return true;
}

// ---------------------------------------------------------------------------------------------------------------
// The test peer
// ---------------------------------------------------------------------------------------------------------------

/// Another process running one scenario of the test peer (tests/zig/peer.zig). It reports progress as tokens on its stdout ("G",
/// "READY", ...), which `waitFor` reads, and takes bytes on its stdin (`send`); it exits by itself when this process
/// dies (its stdin closes), so a test that fails or times out leaves no peer behind.
pub const Peer = struct {
    child: std.process.Child,
    stdout: std.Io.File,
    reader: ?std.Thread = null,
    /// The peer's output: written by the reader thread up to `len`, read by the test from `consumed`.
    data: [4096]u8 = undefined,
    len: std.atomic.Value(u32) = .init(0),
    eof: std.atomic.Value(bool) = .init(false),
    /// Bumped by the reader thread after each change; the futex word `waitFor` sleeps on.
    changes: std.atomic.Value(u32) = .init(0),
    consumed: usize = 0,
    term: ?std.process.Child.Term = null,

    /// Starts `test_peer <scenario> <args...>` (`args[0]` is the scenario).
    pub fn spawn(args: []const []const u8) !*Peer {
        std.debug.assert(options.peer_exe.len > 0); // a multi-process test in a binary built without the peer
        var argv: [16][]const u8 = undefined;
        argv[0] = options.peer_exe;
        @memcpy(argv[1 .. 1 + args.len], args);
        const peer = try std.testing.allocator.create(Peer);
        errdefer std.testing.allocator.destroy(peer);
        var child = try std.process.spawn(io(), .{
            .argv = argv[0 .. 1 + args.len],
            .stdin = .pipe,
            .stdout = .pipe,
            .stderr = .inherit,
        });
        const stdout = child.stdout.?;
        child.stdout = null; // the reader thread owns and closes it
        peer.* = .{ .child = child, .stdout = stdout };
        peer.reader = std.Thread.spawn(.{}, readLoop, .{peer}) catch |err| {
            peer.kill();
            stdout.close(io());
            return err;
        };
        return peer;
    }

    fn readLoop(peer: *Peer) void {
        while (true) {
            const len = peer.len.load(.monotonic);
            var sink: [256]u8 = undefined;
            const buffer: []u8 = if (len < peer.data.len) peer.data[len..] else &sink;
            const n = peer.stdout.readStreaming(io(), &.{buffer}) catch break;
            if (len < peer.data.len) peer.len.store(len + @as(u32, @intCast(n)), .release);
            peer.notify();
        }
        peer.stdout.close(io());
        peer.eof.store(true, .release);
        peer.notify();
    }

    fn notify(peer: *Peer) void {
        _ = peer.changes.fetchAdd(1, .release);
        io().futexWake(u32, &peer.changes.raw, 1);
    }

    /// Waits until the peer printed `token` (after the tokens already consumed) and consumes it; false if the peer's
    /// output ended or `timeout_ms` passed first. `null`: no timeout (the per-test timeout still applies).
    pub fn waitFor(peer: *Peer, token: []const u8, timeout_ms: ?u32) bool {
        const deadline: ?std.Io.Clock.Timestamp = if (timeout_ms) |ms|
            .fromNow(io(), .{ .clock = .awake, .raw = .fromMilliseconds(ms) })
        else
            null;
        while (true) {
            const seen = peer.changes.load(.acquire);
            const output_ended = peer.eof.load(.acquire);
            const len = peer.len.load(.acquire);
            if (std.mem.find(u8, peer.data[peer.consumed..len], token)) |at| {
                peer.consumed += at + token.len;
                return true;
            }
            if (output_ended) return false;
            if (deadline) |d| {
                if (d.durationFromNow(io()).raw.nanoseconds <= 0) return false;
                io().futexWaitTimeout(u32, &peer.changes.raw, seen, .{ .deadline = d }) catch {};
            } else {
                io().futexWaitUncancelable(u32, &peer.changes.raw, seen);
            }
        }
    }

    /// Writes bytes to the peer's stdin.
    pub fn send(peer: *Peer, bytes: []const u8) void {
        if (peer.child.stdin) |stdin| stdin.writeStreamingAll(io(), bytes) catch {};
    }

    /// Kills the peer hard (SIGKILL, TerminateProcess) without waiting for it.
    pub fn terminate(peer: *Peer) void {
        const id = peer.child.id orelse return;
        if (windows) {
            _ = win.TerminateProcess(id, 1);
        } else {
            _ = std.c.kill(id, .KILL);
        }
    }

    /// Stops every thread of the peer (SIGSTOP; Windows: NtSuspendProcess).
    pub fn pause(peer: *Peer) void {
        if (windows) _ = win.NtSuspendProcess(peer.child.id.?) else _ = std.c.kill(peer.child.id.?, .STOP);
    }

    /// Lets a paused peer run again (SIGCONT; Windows: NtResumeProcess).
    pub fn unpause(peer: *Peer) void {
        if (windows) _ = win.NtResumeProcess(peer.child.id.?) else _ = std.c.kill(peer.child.id.?, .CONT);
    }

    /// Waits for the peer to exit and reaps it (waitpid; WaitForSingleObject + CloseHandle). Idempotent.
    pub fn wait(peer: *Peer) std.process.Child.Term {
        if (peer.term) |term| return term;
        peer.term = peer.child.wait(io()) catch .{ .unknown = 0 };
        return peer.term.?;
    }

    /// Waits up to `timeout_ms` for the peer to exit; true (and reaped) if it did.
    pub fn waitTimeout(peer: *Peer, timeout_ms: u32) bool {
        if (peer.term != null) return true;
        const id = peer.child.id.?;
        if (windows) {
            if (win.WaitForSingleObject(id, timeout_ms) != 0) return false;
        } else if (macos) {
            if (!exitedWithin(id, timeout_ms)) return false;
        } else {
            const fd_rc = std.os.linux.pidfd_open(id, 0);
            if (std.os.linux.errno(fd_rc) != .SUCCESS) return false;
            const fd: i32 = @intCast(fd_rc);
            defer _ = std.c.close(fd);
            var fds = [_]std.c.pollfd{.{ .fd = fd, .events = std.c.POLL.IN, .revents = 0 }};
            if (std.c.poll(&fds, 1, @intCast(timeout_ms)) <= 0) return false;
        }
        _ = peer.wait();
        return true;
    }

    /// macOS: whether the child `id` exits within `timeout_ms`, without reaping it: a kqueue's `NOTE_EXIT` on it. A
    /// child that has already exited reports at once, or ESRCH in the moment it ends; it isn't reaped, so its pid is
    /// still its own.
    fn exitedWithin(id: std.c.pid_t, timeout_ms: u32) bool {
        const kq = std.c.kqueue();
        if (kq < 0) return false;
        defer _ = std.c.close(kq);
        const watch = [1]std.c.Kevent{.{
            .ident = @intCast(id),
            .filter = std.c.EVFILT.PROC,
            .flags = std.c.EV.ADD | std.c.EV.ONESHOT,
            .fflags = std.c.NOTE.EXIT,
            .data = 0,
            .udata = 0,
        }};
        var got: [1]std.c.Kevent = undefined;
        const timeout: std.c.timespec = .{
            .sec = @intCast(timeout_ms / 1000),
            .nsec = @intCast(@as(u64, timeout_ms % 1000) * std.time.ns_per_ms),
        };
        if (std.c.kevent(kq, &watch, 1, &got, 1, &timeout) != 1) return false;
        if (got[0].flags & std.c.EV.ERROR != 0) return got[0].data == @backingInt(std.c.E.SRCH);
        return true;
    }

    /// Kills the peer and reaps it (SIGKILL and waitpid; TerminateProcess and WaitForSingleObject).
    pub fn kill(peer: *Peer) void {
        if (peer.term != null) return;
        peer.terminate();
        _ = peer.wait();
    }

    /// Whether the peer exited normally with `code` (WIFEXITED && WEXITSTATUS == code).
    pub fn exitedWith(peer: *Peer, code: u8) bool {
        const term = peer.wait();
        return term == .exited and term.exited == code;
    }

    /// Kills the peer if it still runs, and frees it.
    pub fn deinit(peer: *Peer) void {
        peer.kill();
        if (peer.reader) |reader| reader.join();
        std.testing.allocator.destroy(peer);
    }
};

// ---------------------------------------------------------------------------------------------------------------
// The Windows API the tests use
// ---------------------------------------------------------------------------------------------------------------

pub const win = struct {
    pub const HANDLE = std.os.windows.HANDLE;
    pub const FILETIME = extern struct { low: u32, high: u32 };

    pub extern "kernel32" fn GetCurrentProcessId() callconv(.winapi) u32;
    pub extern "kernel32" fn GetCurrentProcess() callconv(.winapi) HANDLE;
    pub extern "kernel32" fn ExitProcess(code: u32) callconv(.winapi) noreturn;
    pub extern "kernel32" fn CloseHandle(handle: HANDLE) callconv(.winapi) c_int;
    pub extern "kernel32" fn WaitForSingleObject(handle: HANDLE, ms: u32) callconv(.winapi) u32;
    pub extern "kernel32" fn TerminateProcess(handle: HANDLE, code: u32) callconv(.winapi) c_int;
    pub extern "kernel32" fn GetProcessTimes(process: HANDLE, creation: *FILETIME, exit: *FILETIME, kernel: *FILETIME, user: *FILETIME) callconv(.winapi) c_int;
    pub extern "kernel32" fn GetThreadTimes(thread: HANDLE, creation: *FILETIME, exit: *FILETIME, kernel: *FILETIME, user: *FILETIME) callconv(.winapi) c_int;
    pub extern "kernel32" fn GetProcessHandleCount(process: HANDLE, count: *u32) callconv(.winapi) c_int;
    pub extern "kernel32" fn GetProcessAffinityMask(process: HANDLE, process_mask: *usize, system_mask: *usize) callconv(.winapi) c_int;
    pub extern "kernel32" fn SetThreadAffinityMask(thread: HANDLE, mask: usize) callconv(.winapi) usize;
    pub extern "ntdll" fn NtSuspendProcess(process: HANDLE) callconv(.winapi) i32;
    pub extern "ntdll" fn NtResumeProcess(process: HANDLE) callconv(.winapi) i32;

    pub fn filetime(ft: FILETIME) u64 {
        return (@as(u64, ft.high) << 32) | ft.low;
    }
};
