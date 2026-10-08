//! Listeners, connections and their handshakes on real OS objects. The fast tier pairs a listener and a client within
//! this process, on two threads (`connect` waits for the listener's `accept`), and paces the handshake with a client
//! played by hand on the platform layer (`RawClient`), to reach every state a resumed `accept` can be in. The slow tier
//! (`slow_tests`, compiled only into the slow tier's unit tests) runs the races (clients that race for one listener,
//! clients that connect and close at once, cancels of waiting accepts, accepts with random short timeouts), and on
//! Linux and macOS the fork tests, which fork this process. Every test ends with the descriptors or handles and the segment
//! mappings it started with, and the testing allocator checks that no endpoint, listener or session is left. They use
//! the C boundary's process-wide `Io` (the native API's tests, in tests/zig, pass a caller's `Io`).

const std = @import("std");
const builtin = @import("builtin");
const Io = std.Io;
const build_options = @import("build_options");
const control = @import("control.zig");
const endpoint = @import("endpoint.zig");
const listener = @import("listener.zig");
const segment = @import("../session/segment.zig");
const platform = @import("../platform.zig");
const process_state = @import("../process_state.zig");
const wire = @import("wire.zig");

const testing = std.testing;
const linux = if (builtin.os.tag == .macos) @import("../platform/macos_test_sys.zig") else std.os.linux;

const Endpoint = endpoint.Endpoint;
const Listener = listener.Listener;

const forever: Io.Timeout = .none;

fn within(ms: i64) Io.Timeout {
    return .{ .duration = .{ .raw = .fromMilliseconds(ms), .clock = .awake } };
}

fn listen(name: []const u8) listener.Error!*Listener {
    return listener.listen(.process, testing.allocator, name, 4096);
}

fn connect(name: []const u8, timeout: Io.Timeout) endpoint.Error!*Endpoint {
    return endpoint.connect(.process, testing.allocator, name, timeout);
}

/// A name unique to this process and call.
fn uniqueName(buf: *[64]u8, comptime tag: []const u8) []const u8 {
    const counter = struct {
        var next = std.atomic.Value(u32).init(0);
    };
    return std.mem.print(buf, "t-" ++ tag ++ "-{d}-{d}", .{ processId(), counter.next.fetchAdd(1, .monotonic) }) catch unreachable;
}

const processId = platform.pid;

fn sleepMs(ms: i64) void {
    testing.io.sleep(.fromMilliseconds(ms), .awake) catch {};
}

fn nowMs() i64 {
    return Io.Clock.awake.now(testing.io).toMilliseconds();
}

/// A client that connects on a thread of its own, while the caller accepts: `connect` waits for the listener's
/// `accept`.
const Connector = struct {
    thread: std.Thread = undefined,
    result: endpoint.Error!*Endpoint = error.Timeout,
    /// Closes the connection as soon as `connect` returns (`brief`).
    close_at_once: bool = false,

    fn start(c: *Connector, name: []const u8, timeout: Io.Timeout) !void {
        c.thread = try std.Thread.spawn(.{}, run, .{ c, name, timeout });
    }

    fn run(c: *Connector, name: []const u8, timeout: Io.Timeout) void {
        c.result = connect(name, timeout);
        if (c.close_at_once) if (c.result) |conn| endpoint.close(conn) else |_| {};
    }

    fn join(c: *Connector) endpoint.Error!*Endpoint {
        c.thread.join();
        return c.result;
    }
};

/// A connected pair on listener `l` (named `name`): the client's connection (`b`) and the one `accept` returned (`a`).
fn pair(l: *Listener, name: []const u8) !struct { *Endpoint, *Endpoint } {
    var connector: Connector = .{};
    try connector.start(name, within(5000));
    const accepted = listener.accept(l, within(5000));
    const b = try connector.join();
    errdefer endpoint.close(b);
    return .{ b, try accepted };
}

/// What a test must give back: the process's descriptors (Linux, macOS) or handles (Windows), and its segment mappings. The
/// first baseline warms up what a process creates once, including std's first stack trace of an expected pipe status.
/// On Windows parts of the process outside the library open or close a handle late, so a baseline waits for the count
/// to settle, the check allows a moment, and a test may end with fewer handles than it began with, or with
/// `windows_noise` more. A leak of the library's grows with its sessions, far past that, and the
/// platform tests count its objects exactly.
const Baseline = struct {
    handles: usize,
    mappings: usize,

    var warmed = false;

    fn take() !Baseline {
        if (!warmed) {
            warmed = true;
            var name_buf: [64]u8 = undefined;
            const name = uniqueName(&name_buf, "warm");
            const l = try listen(name);
            defer listener.close(l);
            const b, const a = try pair(l, name);
            endpoint.close(b);
            endpoint.close(a);
            control.test_hooks.primeStackTraces(testing.io);
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

    fn expectSame(b: Baseline) !void {
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
fn awaitPhase(ep: *Endpoint, phase: endpoint.Phase) !void {
    const deadline = nowMs() + 5000;
    while (ep.loadStatus().phase != phase) {
        if (nowMs() > deadline) return error.TestUnexpectedResult;
        sleepMs(1);
    }
}

fn acceptResult(l: *Listener, result: *?listener.Error!*Endpoint) void {
    result.* = listener.accept(l, forever);
}

/// A client played by hand on the platform layer, to pace the handshake: it connects, takes SEGMENT, and sends READY,
/// whole or in pieces, or anything else, when the test says.
const RawClient = struct {
    handle: platform.Handle,
    security: platform.Security,

    fn connectTo(name: []const u8) !RawClient {
        var security = try platform.Security.init();
        errdefer security.deinit();
        const address = try platform.addressOf(name);
        for (0..2000) |_| switch (try platform.connect(&address, &security)) {
            .connected => |h| return .{ .handle = h, .security = security },
            .busy, .no_listener => sleepMs(1),
            .foreign => return error.TestUnexpectedResult,
        };
        return error.TestUnexpectedResult;
    }

    /// Waits up to `ms` for SEGMENT (Linux: its descriptor is closed at once). False: none within `ms`.
    fn awaitSegment(c: *RawClient, ms: u32) !bool {
        var frame: wire.Frame = undefined;
        if (platform.passes_segment_handle) {
            if (platform.readableWithin(c.handle, ms) != .done) return false;
            platform.close(try platform.recvSegment(c.handle, &frame));
        } else {
            var got: usize = 0;
            while (got < frame.len) switch (platform.readWithin(c.handle, frame[got..], null, ms)) {
                .done => |n| {
                    if (n == 0) return error.EndOfStream;
                    got += n;
                },
                .idle => if (got == 0) return false,
                .cancelled => unreachable,
            };
        }
        if (wire.checkSegment(&frame) != .valid) return error.TestUnexpectedResult;
        return true;
    }

    fn send(c: *RawClient, bytes: []const u8) !void {
        if (!(platform.sendFrame(testing.io, c.handle, bytes, null) catch false)) return error.TestUnexpectedResult;
    }

    /// Whether the listener dropped it: the end of the stream within `ms`.
    fn dropped(c: *RawClient, ms: u32) bool {
        var byte: [1]u8 = undefined;
        return switch (platform.readWithin(c.handle, &byte, null, ms)) {
            .done => |n| n == 0,
            else => false,
        };
    }

    fn close(c: *RawClient) void {
        platform.close(c.handle);
        c.security.deinit();
    }
};

/// Polls `accept` with timeout 0 until it returns a connection or an error other than Timeout, at most `calls` times;
/// the number of calls it took goes to `count`.
fn pollAccept(l: *Listener, calls: usize, count: *usize) listener.Error!*Endpoint {
    count.* = 0;
    while (count.* < calls) {
        count.* += 1;
        return listener.accept(l, within(0)) catch |err| {
            if (err != error.Timeout) return err;
            sleepMs(1);
            continue;
        };
    }
    return error.Timeout;
}

// === Fast: pairs within this process ===

test "a listener and a client on two threads: connect returns connected, accept returns the other end, one segment" {
    const base = try Baseline.take();
    var name_buf: [64]u8 = undefined;
    const name = uniqueName(&name_buf, "pair");
    const l = try listen(name);
    errdefer listener.close(l);
    // Nobody connected: accept times out, having changed nothing
    try testing.expectError(error.Timeout, listener.accept(l, within(0)));
    const b, const a = try pair(l, name);
    try testing.expectEqual(endpoint.Phase.connected, b.loadStatus().phase);
    try testing.expectEqual(@as(u32, 4096), b.hot.view_cap.load(.monotonic)); // the listener's capacity
    try testing.expectEqual(endpoint.Phase.connected, a.loadStatus().phase);
    try testing.expect(a.hot.view_server.load(.monotonic) == 1 and b.hot.view_server.load(.monotonic) == 0);
    // One segment: what the listener's side writes into the s2c ring, the client sees
    a.hot.view_data.load(.monotonic).?[5] = 0xa5;
    try testing.expectEqual(@as(u8, 0xa5), b.hot.view_data.load(.monotonic).?[5]);
    endpoint.close(a);
    endpoint.close(b);
    listener.close(l);
    try base.expectSame();
}

test "the peer's close ends the session, and the listener accepts the next client" {
    const base = try Baseline.take();
    var name_buf: [64]u8 = undefined;
    const name = uniqueName(&name_buf, "dead");
    const l = try listen(name);
    errdefer listener.close(l);
    for (0..3) |round| {
        // A connection has one session: to talk again, the client connects anew (docs/protocol.md §7)
        const b, const a = try pair(l, name);
        if (round % 2 == 0) {
            endpoint.close(b);
            try awaitPhase(a, .peer_dead);
            endpoint.close(a);
        } else {
            endpoint.close(a);
            try awaitPhase(b, .peer_dead);
            endpoint.close(b);
        }
    }
    listener.close(l);
    try base.expectSame();
}

test "a connection outlives its listener: its session goes on, and the name is free for a new listener" {
    const base = try Baseline.take();
    var name_buf: [64]u8 = undefined;
    const name = uniqueName(&name_buf, "outlive");
    const l = try listen(name);
    const b, const a = try pair(l, name);
    listener.close(l);
    // The session goes on
    try testing.expectEqual(endpoint.Phase.connected, b.loadStatus().phase);
    try testing.expectEqual(endpoint.Phase.connected, a.loadStatus().phase);
    // A real difference of the OSes' rendezvous: a Windows client connects on the pipe instance itself
    if (builtin.os.tag != .windows) {
        // Linux: the name is free at once
        const l2 = try listen(name);
        listener.close(l2);
    } else {
        // Windows: the accepted connection's pipe instance holds the name until it is closed
        try testing.expectError(error.AddrInUse, listen(name));
    }
    endpoint.close(a);
    try awaitPhase(b, .peer_dead);
    const l3 = try listen(name);
    listener.close(l3);
    endpoint.close(b);
    try base.expectSame();
}

test "a name another listener holds is AddrInUse, and free again once it is closed" {
    const base = try Baseline.take();
    var name_buf: [64]u8 = undefined;
    const name = uniqueName(&name_buf, "inuse");
    const l = try listen(name);
    try testing.expectError(error.AddrInUse, listen(name));
    listener.close(l);
    const again = try listen(name);
    listener.close(again);
    try base.expectSame();
}

test "names and capacities that aren't valid are Invalid, before anything is created" {
    for ([_][]const u8{ "", ".hidden", "-flag", "a/b", "a b", &@as([300]u8, @splat('x')), &@as([246]u8, @splat('x')) }) |name| {
        try testing.expectError(error.Invalid, listener.listen(.process, testing.allocator, name, 4096));
        try testing.expectError(error.Invalid, connect(name, within(0)));
    }
    for ([_]usize{ 0, 512, 1000, 4097, 1 << 32 }) |capacity| {
        try testing.expectError(error.Invalid, listener.listen(.process, testing.allocator, "valid", capacity));
    }
}

test "connect waits for a late listener and its accept, and times out while nobody listens or accepts, leaving nothing behind" {
    const base = try Baseline.take();
    var name_buf: [64]u8 = undefined;
    const name = uniqueName(&name_buf, "late");
    try testing.expectError(error.Timeout, connect(name, within(0)));
    try testing.expectError(error.Timeout, connect(name, within(50)));
    // A listener that never accepts: the connect times out too, connect(0) as well
    {
        const idle = try listen(name);
        defer listener.close(idle);
        try testing.expectError(error.Timeout, connect(name, within(0)));
        try testing.expectError(error.Timeout, connect(name, within(50)));
    }
    const Late = struct {
        fn run(n: []const u8, out: *?listener.Error!*Listener, accepted: *?listener.Error!*Endpoint) void {
            sleepMs(100);
            out.* = listen(n);
            const l = (out.* orelse return) catch return;
            accepted.* = listener.accept(l, within(5000));
        }
    };
    var late: ?listener.Error!*Listener = null;
    var accepted: ?listener.Error!*Endpoint = null;
    const thread = try std.Thread.spawn(.{}, Late.run, .{ name, &late, &accepted });
    const b = connect(name, within(5000));
    thread.join();
    const l = try late.?;
    errdefer listener.close(l);
    const conn = try b;
    errdefer endpoint.close(conn);
    const a = try accepted.?;
    endpoint.close(a);
    endpoint.close(conn);
    listener.close(l);
    try base.expectSame();
}

test "one client at a time: accept is Invalid while its connection is open; the next client gets in once it is closed" {
    const base = try Baseline.take();
    var name_buf: [64]u8 = undefined;
    const name = uniqueName(&name_buf, "one");
    const l = try listen(name);
    errdefer listener.close(l);
    const b, const a = try pair(l, name);
    try testing.expectError(error.Invalid, listener.accept(l, within(0)));
    // A second client waits in the backlog (or on the next pipe instance): no session while the first is open
    try testing.expectError(error.Timeout, connect(name, within(200)));
    var second: Connector = .{};
    try second.start(name, within(5000));
    sleepMs(50);
    endpoint.close(b);
    try testing.expectError(error.Invalid, listener.accept(l, within(0))); // still open on this side
    endpoint.close(a);
    const c = try listener.accept(l, within(5000));
    const d = try second.join();
    endpoint.close(d);
    endpoint.close(c);
    listener.close(l);
    try base.expectSame();
}

test "cancel releases an accept that waits; later accepts return Cancelled, and no client gets in after it" {
    const base = try Baseline.take();
    var name_buf: [64]u8 = undefined;
    const name = uniqueName(&name_buf, "cancel");
    const l = try listen(name);
    errdefer listener.close(l);
    var result: ?listener.Error!*Endpoint = null;
    const waiter = try std.Thread.spawn(.{}, acceptResult, .{ l, &result });
    sleepMs(10);
    listener.cancel(l);
    waiter.join();
    try testing.expectError(error.Cancelled, result.?);
    try testing.expectError(error.Cancelled, listener.accept(l, within(0)));
    // The cancelled listener sets no new client up
    try testing.expectError(error.Timeout, connect(name, within(200)));
    listener.close(l);
    try base.expectSame();
}

test "a connection's cancel ends the waits of its data calls only; close is final" {
    const base = try Baseline.take();
    var name_buf: [64]u8 = undefined;
    const name = uniqueName(&name_buf, "conn-cancel");
    const l = try listen(name);
    errdefer listener.close(l);
    const b, const a = try pair(l, name);
    endpoint.cancel(b);
    try testing.expectEqual(endpoint.Phase.connected, b.loadStatus().phase); // the session goes on
    try testing.expectEqual(endpoint.Phase.connected, a.loadStatus().phase);
    endpoint.close(a);
    endpoint.close(b);
    listener.close(l);
    try base.expectSame();
}

// A listener of another protocol version (a SEGMENT frame of version 3), as far as the client's first check: the
// client refuses it for good (INVALID), and doesn't retry.
test "a listener of another protocol version: connect fails with Invalid" {
    const base = try Baseline.take();
    {
        var name_buf: [64]u8 = undefined;
        const name = uniqueName(&name_buf, "version");
        const ref = try process_state.acquire();
        defer process_state.release(ref);
        const address = try platform.addressOf(name);
        var security = try platform.Security.init();
        defer security.deinit();
        const held = (try platform.claim(&address, &security)) orelse return error.TestUnexpectedResult;
        defer platform.close(held);
        const Old = struct {
            fn serve(io: Io, listening: platform.Handle) void {
                var frame = wire.encodeSegment(.{ .pid = 1, .capacity = 4096, .segment_size = segment.size(4096), .session_id = @splat(7) });
                frame[5] = 3;
                const connection = switch (platform.acceptWithin(listening, null, 5000, null)) {
                    .done => |outcome| switch (outcome) {
                        .connection => |h| h,
                        else => return,
                    },
                    else => return,
                };
                const attached: ?platform.Handle = if (platform.passes_segment_handle) platform.createSegmentHandle() catch null else null;
                defer if (attached) |h| platform.close(h);
                _ = platform.sendFrame(io, connection, &frame, attached) catch {};
                var byte: [1]u8 = undefined;
                _ = platform.read(io, connection, &byte) catch {}; // until the client leaves
                // A handle the client connected on itself is `held`, closed by the test
                _ = platform.dropClient(connection, false);
            }
        };
        var old = try ref.io.concurrent(Old.serve, .{ ref.io, held });
        try testing.expectError(error.Invalid, connect(name, within(5000)));
        old.await(ref.io);
    }
    try base.expectSame();
}

/// A client that closes as soon as it connects: accept returns its connection (its READY came before its end), whose
/// session ends, every time.
fn briefPeers(comptime tag: []const u8, rounds: usize) !void {
    const base = try Baseline.take();
    var name_buf: [64]u8 = undefined;
    const name = uniqueName(&name_buf, tag);
    const l = try listen(name);
    errdefer listener.close(l);
    for (0..rounds) |_| {
        var connector: Connector = .{ .close_at_once = true };
        try connector.start(name, within(5000));
        const accepted = listener.accept(l, within(5000));
        _ = try connector.join();
        const a = try accepted;
        try awaitPhase(a, .peer_dead);
        endpoint.close(a);
    }
    listener.close(l);
    try base.expectSame();
}

test "a client that closes as soon as it connects: accept returns it, and its session ends, every time" {
    try briefPeers("brief", 20);
}

// The resume rule: a call of `accept` that times out keeps the setup it began, and the next call goes on from there.
// The client pauses at each step: before it connects (state none), once it got SEGMENT (awaiting READY), and between
// READY's bytes, which it sends one at a time (a READY read in part).
test "accept polled with timeout 0 lets in a client that pauses at each step and sends READY a byte at a time" {
    const base = try Baseline.take();
    var name_buf: [64]u8 = undefined;
    const name = uniqueName(&name_buf, "polled");
    const l = try listen(name);
    errdefer listener.close(l);
    var calls: usize = 0;
    try testing.expectError(error.Timeout, pollAccept(l, 3, &calls)); // nobody yet
    var client = try RawClient.connectTo(name);
    var client_open = true;
    defer if (client_open) client.close();
    try testing.expectError(error.Timeout, listener.accept(l, within(0))); // sends SEGMENT, then finds no READY
    try testing.expect(try client.awaitSegment(5000));
    try testing.expectError(error.Timeout, listener.accept(l, within(0))); // still no READY
    const ready = wire.encodeReady(platform.pid());
    for (ready[0 .. ready.len - 1]) |*byte| {
        try client.send(byte[0..1]);
        try testing.expectError(error.Timeout, listener.accept(l, within(0)));
    }
    try client.send(ready[ready.len - 1 ..]);
    const a = try pollAccept(l, 1000, &calls);
    try testing.expectEqual(endpoint.Phase.connected, a.loadStatus().phase);
    try testing.expectError(error.Invalid, listener.accept(l, within(0)));
    // The client's end reaches the accepted connection
    client.close();
    client_open = false;
    try awaitPhase(a, .peer_dead);
    endpoint.close(a);
    listener.close(l);
    try base.expectSame();
}

// A setup that fails while `accept` waits for READY (the client leaves, or answers with something else) drops that
// client and goes on: the next client gets in, through the same call or a later one, and nothing leaks.
test "a client that leaves or answers garbage while accept waits for its READY is dropped, and the next gets in" {
    const base = try Baseline.take();
    var name_buf: [64]u8 = undefined;
    const name = uniqueName(&name_buf, "dropped");
    const l = try listen(name);
    errdefer listener.close(l);
    for ([_]bool{ false, true }) |garbage| {
        var bad = try RawClient.connectTo(name);
        try testing.expectError(error.Timeout, listener.accept(l, within(0)));
        try testing.expect(try bad.awaitSegment(5000));
        if (garbage) {
            var frame = wire.encodeReady(platform.pid());
            frame[4] = 9; // another frame type
            try bad.send(&frame);
            try testing.expectError(error.Timeout, listener.accept(l, within(50)));
            try testing.expect(bad.dropped(5000));
            bad.close();
        } else {
            bad.close();
            try testing.expectError(error.Timeout, listener.accept(l, within(50)));
        }
        // A real client, accepted by a call that blocks
        const b, const a = try pair(l, name);
        endpoint.close(b);
        endpoint.close(a);
    }
    listener.close(l);
    try base.expectSame();
}

// Cancel while a setup is in progress: an accept that polls, or one that blocks, returns Cancelled and drops the
// client, which reads the end of the stream; a close drops it too.
test "cancel or close while a client's setup is in progress drops the client" {
    const base = try Baseline.take();
    var name_buf: [64]u8 = undefined;
    // An accept that polls
    {
        const name = uniqueName(&name_buf, "cancel-polled");
        const l = try listen(name);
        defer listener.close(l);
        var client = try RawClient.connectTo(name);
        defer client.close();
        try testing.expectError(error.Timeout, listener.accept(l, within(0)));
        try testing.expect(try client.awaitSegment(5000));
        listener.cancel(l);
        try testing.expectError(error.Cancelled, listener.accept(l, within(0)));
        try testing.expect(client.dropped(5000));
    }
    // An accept that blocks, waiting for READY
    {
        const name = uniqueName(&name_buf, "cancel-blocked");
        const l = try listen(name);
        defer listener.close(l);
        var client = try RawClient.connectTo(name);
        defer client.close();
        var result: ?listener.Error!*Endpoint = null;
        const waiter = try std.Thread.spawn(.{}, acceptResult, .{ l, &result });
        const segment_came = client.awaitSegment(5000);
        listener.cancel(l);
        waiter.join();
        try testing.expect(try segment_came);
        try testing.expectError(error.Cancelled, result.?);
        try testing.expect(client.dropped(5000));
    }
    // A close
    {
        const name = uniqueName(&name_buf, "close-pending");
        const l = try listen(name);
        var client = try RawClient.connectTo(name);
        defer client.close();
        try testing.expectError(error.Timeout, listener.accept(l, within(0)));
        try testing.expect(try client.awaitSegment(5000));
        listener.close(l);
        try testing.expect(client.dropped(5000));
    }
    try base.expectSame();
}

test {
    if (build_options.slow_tests) _ = slow_tests;
}

// === Slow: races, and fork (Linux, macOS) ===

const slow_tests = struct {
    /// The rounds of the races: 10,000, and 2,000 on Windows, where setting a connection up (a pipe, a section, four
    /// events, their security) costs several times what it does on Linux, so that the slow tier fits its timeout on a
    /// 2-CPU runner.
    const race_rounds: usize = if (builtin.os.tag == .windows) 2_000 else 10_000;

    test "slow: a client that closes as soon as it connects, 1,000 times: accept returns it, and its session ends" {
        try briefPeers("brief-1000", 1000);
    }

    test "slow: cancel releases an accept that waits for ever within 100 ms, 10,000 rounds, 2,000 on Windows" {
        const base = try Baseline.take();
        var name_buf: [64]u8 = undefined;
        const name = uniqueName(&name_buf, "cancels");
        for (0..race_rounds) |_| {
            const l = try listen(name);
            defer listener.close(l);
            var result: ?listener.Error!*Endpoint = null;
            const waiter = try std.Thread.spawn(.{}, acceptResult, .{ l, &result });
            const start = nowMs();
            listener.cancel(l);
            waiter.join();
            try testing.expect(nowMs() - start < 100);
            try testing.expectError(error.Cancelled, result.?);
        }
        try base.expectSame();
    }

    test "slow: clients race for one listener: each gets in once the one before it is closed" {
        const base = try Baseline.take();
        var name_buf: [64]u8 = undefined;
        const name = uniqueName(&name_buf, "race");
        const l = try listen(name);
        errdefer listener.close(l);
        const Client = struct {
            fn run(n: []const u8, done: *std.atomic.Value(u32)) void {
                const conn = connect(n, within(20_000)) catch return;
                // The session ends once the server closes its side
                awaitPhase(conn, .peer_dead) catch {};
                endpoint.close(conn);
                _ = done.fetchAdd(1, .monotonic);
            }
        };
        var done = std.atomic.Value(u32).init(0);
        var threads: [4]std.Thread = undefined;
        for (&threads) |*thread| thread.* = try std.Thread.spawn(.{}, Client.run, .{ name, &done });
        for (0..threads.len) |_| {
            const a = try listener.accept(l, within(10_000));
            try testing.expectError(error.Invalid, listener.accept(l, within(0)));
            endpoint.close(a);
        }
        for (threads) |thread| thread.join();
        try testing.expectEqual(@as(u32, threads.len), done.load(.monotonic));
        listener.close(l);
        try base.expectSame();
    }

    // Clients one after another, each connecting as soon as the one before is closed, against an accept that keeps
    // timing out after 0, 1 or 2 ms at random: every client gets in, whatever step a timeout cuts. On Windows a listen
    // or a READY read that completes just as its wait times out is kept (the cancel-and-drain path).
    test "slow: accept with random timeouts of 0 to 2 ms lets every one of 300 clients in, and nothing leaks" {
        const base = try Baseline.take();
        var name_buf: [64]u8 = undefined;
        const name = uniqueName(&name_buf, "random");
        const l = try listen(name);
        errdefer listener.close(l);
        const clients = 300;
        const Clients = struct {
            fn run(n: []const u8, connected: *std.atomic.Value(u32)) void {
                for (0..clients) |_| {
                    const conn = connect(n, within(10_000)) catch return;
                    awaitPhase(conn, .peer_dead) catch {};
                    endpoint.close(conn);
                    _ = connected.fetchAdd(1, .monotonic);
                }
            }
        };
        var connected = std.atomic.Value(u32).init(0);
        const thread = try std.Thread.spawn(.{}, Clients.run, .{ name, &connected });
        var prng: std.Random.DefaultPrng = .init(0x5eed);
        var accepted: usize = 0;
        var timeouts: usize = 0;
        const deadline = nowMs() + 60_000;
        while (accepted < clients and nowMs() < deadline) {
            const a = listener.accept(l, within(prng.random().uintAtMost(u8, 2))) catch |err| {
                try testing.expectEqual(error.Timeout, err);
                timeouts += 1;
                continue;
            };
            accepted += 1;
            endpoint.close(a);
        }
        thread.join();
        std.debug.print("  {d} clients in, {d} accepts timed out\n", .{ accepted, timeouts });
        try testing.expectEqual(@as(usize, clients), accepted);
        try testing.expectEqual(@as(u32, clients), connected.load(.monotonic));
        listener.close(l);
        try base.expectSame();
    }

    // --- Fork (Linux, macOS): the child runs only its checks and leaves with `_exit`. Under TSan the tests are skipped: TSan
    // ends a child that starts a thread after forking from a parent with threads. Objects that a child touches use the
    // C allocator, which is fork-safe; the testing allocator's lock may be inherited held. ---

    fn listenC(name: []const u8) listener.Error!*Listener {
        return listener.listen(.process, std.heap.c_allocator, name, 4096);
    }

    fn connectC(name: []const u8, timeout: Io.Timeout) endpoint.Error!*Endpoint {
        return endpoint.connect(.process, std.heap.c_allocator, name, timeout);
    }

    /// Comptime-known, so that the rest of a fork test isn't even analyzed where it can't run.
    const fork_unsupported = !platform.has_fork or builtin.sanitize_thread;

    /// Waits up to `timeout_ms` for the child to exit, and returns its exit code; kills it at the deadline (null: it
    /// hung).
    fn waitChild(pid: std.c.pid_t, timeout_ms: u32) ?u8 {
        var status: c_int = 0;
        for (0..timeout_ms) |_| {
            if (std.c.waitpid(pid, &status, std.c.W.NOHANG) == pid) {
                const s: u32 = @bitCast(status);
                return if (s & 0x7f == 0) @truncate(s >> 8) else 128 + @as(u8, @truncate(s & 0x7f));
            }
            childSleepMs(1);
        }
        _ = std.c.kill(pid, .KILL);
        _ = std.c.waitpid(pid, &status, 0);
        return null;
    }

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
    fn inheritedChecks(conn: *Endpoint, l: *Listener) u8 {
        if (conn.hot.view_number.load(.monotonic) != 0) return 1; // no session: every data call is Invalid
        if (!conn.handles.forked or !l.handles.forked) return 2;
        if (listener.accept(l, forever)) |_| return 3 else |err| if (err != error.Invalid) return 4;
        endpoint.cancel(conn); // only its bit
        listener.cancel(l);
        endpoint.close(conn); // frees what is complete, as memory only
        listener.close(l);
        return 0;
    }

    test "slow: fork: the child's inherited connection and listener fail and close, and its new connection gets in; the parent's death ends its session at once" {
        if (fork_unsupported) return error.SkipZigTest;
        var first_buf: [64]u8 = undefined;
        var second_buf: [64]u8 = undefined;
        const first = uniqueName(&first_buf, "fork-a");
        const second = uniqueName(&second_buf, "fork-b");
        const l1 = try listenC(first);
        defer listener.close(l1);
        const l2 = try listenC(second);
        defer listener.close(l2);
        const report = try makePipe(); // the grandchild's result
        const go = try makePipe(); // the grandchild's leave to exit
        const pid = std.c.fork();
        if (pid == 0) forkedParent(first, second, report, go);
        _ = linux.close(report[1]);
        _ = linux.close(go[0]);
        defer _ = linux.close(report[0]);
        defer _ = linux.close(go[1]);
        const a1 = try listener.accept(l1, within(5000)); // the forked parent's connection, made before its fork
        defer endpoint.close(a1);
        const a2 = try listener.accept(l2, within(10_000)); // the grandchild's new connection
        defer endpoint.close(a2);
        const code = readByte(report[0], 10_000);
        _ = std.c.kill(pid, .KILL);
        _ = waitChild(pid, 5000);
        const killed = nowMs();
        const ended = awaitPhase(a1, .peer_dead);
        const elapsed = nowMs() - killed;
        const grandchild_connected = a2.loadStatus().phase == .connected; // it still runs
        _ = linux.write(go[1], "x", 1);
        try testing.expectEqual(@as(?u8, 0), code);
        try ended;
        try testing.expect(elapsed < 1000); // the grandchild's copy of the socket was closed at its fork
        try testing.expect(grandchild_connected);
        try awaitPhase(a2, .peer_dead); // the grandchild exited
    }

    /// The forked parent of the first fork test: a connection to the test's first listener, whose watcher runs on a
    /// worker of the process-wide `Io`, and a listener of its own. Then it forks the grandchild and waits to be killed.
    fn forkedParent(first: []const u8, second: []const u8, report: [2]linux.fd_t, go: [2]linux.fd_t) noreturn {
        const a = connectC(first, within(5000)) catch std.c._exit(11);
        var name_buf: [64]u8 = undefined;
        const own = listenC(uniqueName(&name_buf, "fork-own")) catch std.c._exit(12);
        if (std.c.fork() == 0) forkedGrandchild(a, own, second, report, go);
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
        const l = try listenC(name);
        defer listener.close(l);
        const Waiting = struct {
            fn run(lis: *Listener, out: *?listener.Error!*Endpoint) void {
                out.* = listener.accept(lis, within(5000));
            }
        };
        var result: ?listener.Error!*Endpoint = null;
        const waiter = try std.Thread.spawn(.{}, Waiting.run, .{ l, &result });
        sleepMs(10); // it waits in accept
        const pid = std.c.fork();
        if (pid == 0) std.c._exit(0);
        try testing.expectEqual(@as(?u8, 0), waitChild(pid, 5000));
        // The accept that was waiting at the fork drops the next connection; the client retries and gets in
        const b = connectC(name, within(5000));
        waiter.join();
        endpoint.close(try b);
        endpoint.close(try result.?);
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
            const l = try listenC(name);
            defer listener.close(l);
            const Client = struct {
                fn run(n: []const u8, out: *?endpoint.Error!*Endpoint) void {
                    out.* = connectC(n, within(5000));
                }
            };
            var client: ?endpoint.Error!*Endpoint = null;
            const thread = try std.Thread.spawn(.{}, Client.run, .{ name, &client });
            const accepted = listener.accept(l, within(5000));
            thread.join();
            const b = try client.?;
            const a = accepted catch |err| {
                endpoint.close(b);
                return err;
            };
            defer endpoint.close(a);
            endpoint.close(b);
            try awaitPhase(a, .peer_dead);
        }
    };

    test "slow: fork stress: 1,000 forks while other threads set connections up and close them; every child's listen and close return at once" {
        if (fork_unsupported) return error.SkipZigTest;
        // The process's first call comes before any fork, as it must on macOS (docs/platform-support.md)
        var warm_buf: [64]u8 = undefined;
        listener.close(try listenC(uniqueName(&warm_buf, "warm")));
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
                listener.close(n);
                std.c._exit(if (childNowMs() - start < 1000) 0 else 2);
            }
            if (waitChild(pid, 5000) != 0) failed_children += 1;
        }
        stop.store(true, .release);
        for (threads) |thread| thread.join();
        try testing.expectEqual(@as(usize, 0), failed_children);
        for (churns) |churn| try testing.expectEqual(@as(u32, 0), churn.failures.load(.monotonic));
        // The parent's connections still work
        try briefPeers("after-forks", 3);
    }
};
