//! A stream connection's handshake on real OS objects (server.zig `accept`, client.zig `connect`), on the harness of
//! core/endpoint_test.zig. The fast tier paces the handshake with a client played by hand on the platform layer
//! (`RawClient`), to reach every state a resumed `accept` can be in, and checks the client's waits and refusals. The
//! slow tier (`slow_tests`, compiled only into the slow tier's unit tests) runs the races: clients that race for one
//! listener, clients that connect and close at once, cancels of waiting accepts, accepts with random short timeouts.

const std = @import("std");
const builtin = @import("builtin");
const Io = std.Io;
const build_options = @import("build_options");
const wire_protocol = @import("../wire_protocol.zig");
const endpoint_module = @import("../core/endpoint.zig");
const frames = @import("frames.zig");
const harness = @import("../core/endpoint_test.zig");
const platform = @import("../platform.zig");
const process_state = @import("../core/process_state.zig");
const segment = @import("segment.zig");
const stream_listener = @import("listener.zig");
const stream_server = @import("server.zig");
const wire = @import("../core/wire.zig");

const testing = std.testing;

const Endpoint = endpoint_module.Endpoint;
const Listener = stream_listener.Listener;
const Baseline = harness.Baseline;
const Connector = harness.Connector;
const acceptResult = harness.acceptResult;
const awaitPhase = harness.awaitPhase;
const briefPeers = harness.briefPeers;
const connect = harness.connect;
const forever = harness.forever;
const listen = harness.listen;
const nowMs = harness.nowMs;
const pair = harness.pair;
const sleepMs = harness.sleepMs;
const uniqueName = harness.uniqueName;
const within = harness.within;

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

    /// Waits up to `ms` for SEGMENT. Where the segment's handle travels with the frame (Linux, macOS), the received
    /// descriptor is closed at once. False: none within `ms`.
    fn awaitSegment(client: *RawClient, ms: u32) !bool {
        var frame: wire.Frame = undefined;
        if (platform.passes_segment_handle) {
            if (platform.readableWithin(client.handle, ms) != .done) return false;
            platform.close(try platform.recvSegment(client.handle, &frame));
        } else {
            var got: usize = 0;
            while (got < frame.len) switch (platform.readWithin(client.handle, frame[got..], null, ms)) {
                .done => |n| {
                    if (n == 0) return error.EndOfStream;
                    got += n;
                },
                .idle => if (got == 0) return false,
                .cancelled => unreachable,
            };
        }
        if (frames.checkSegment(&frame) != .valid) return error.TestUnexpectedResult;
        return true;
    }

    fn send(client: *RawClient, bytes: []const u8) !void {
        if (!platform.sendFrame(client.handle, bytes, null)) return error.TestUnexpectedResult;
    }

    /// Whether the listener dropped it: the end of the stream within `ms`.
    fn dropped(client: *RawClient, ms: u32) bool {
        var byte: [1]u8 = undefined;
        return switch (platform.readWithin(client.handle, &byte, null, ms)) {
            .done => |n| n == 0,
            else => false,
        };
    }

    fn close(client: *RawClient) void {
        platform.close(client.handle);
        client.security.deinit();
    }
};

/// Polls `accept` with timeout 0 until it returns a connection or an error other than Timeout, at most `calls` times;
/// the number of calls it took goes to `count`.
fn pollAccept(listener: *Listener, calls: usize, count: *usize) endpoint_module.Error!*Endpoint {
    count.* = 0;
    while (count.* < calls) {
        count.* += 1;
        return stream_server.accept(listener, within(0)) catch |err| {
            if (err != error.Timeout) return err;
            sleepMs(1);
            continue;
        };
    }
    return error.Timeout;
}

// === Fast: the handshake within this process ===

test "connect waits for a late listener and its accept, and times out while nobody listens or accepts, leaving nothing behind" {
    const base = try Baseline.take();
    var name_buf: [64]u8 = undefined;
    const name = uniqueName(&name_buf, "late");
    try testing.expectError(error.Timeout, connect(name, within(0)));
    try testing.expectError(error.Timeout, connect(name, within(50)));
    // A listener that never accepts: the connect times out too, connect(0) as well
    {
        const idle = try listen(name);
        defer idle.close();
        try testing.expectError(error.Timeout, connect(name, within(0)));
        try testing.expectError(error.Timeout, connect(name, within(50)));
    }
    const Late = struct {
        fn run(n: []const u8, out: *?endpoint_module.CreateError!*Listener, accepted: *?endpoint_module.Error!*Endpoint) void {
            sleepMs(100);
            out.* = listen(n);
            const listener = (out.* orelse return) catch return;
            accepted.* = stream_server.accept(listener, within(5000));
        }
    };
    var late: ?endpoint_module.CreateError!*Listener = null;
    var accepted: ?endpoint_module.Error!*Endpoint = null;
    const thread = try std.Thread.spawn(.{}, Late.run, .{ name, &late, &accepted });
    const b = connect(name, within(5000));
    thread.join();
    const listener = try late.?;
    errdefer listener.close();
    const conn = try b;
    errdefer endpoint_module.close(conn);
    const a = try accepted.?;
    endpoint_module.close(a);
    endpoint_module.close(conn);
    listener.close();
    try base.expectSame();
}

test "one client at a time: accept is Invalid while its connection is open; the next client gets in once it is closed" {
    const base = try Baseline.take();
    var name_buf: [64]u8 = undefined;
    const name = uniqueName(&name_buf, "one");
    const listener = try listen(name);
    errdefer listener.close();
    const b, const a = try pair(listener, name);
    try testing.expectError(error.Invalid, stream_server.accept(listener, within(0)));
    // A second client waits in the backlog (or on the next pipe instance): no session while the first is open
    try testing.expectError(error.Timeout, connect(name, within(200)));
    var second: Connector = .{};
    try second.start(name, within(5000));
    sleepMs(50);
    endpoint_module.close(b);
    try testing.expectError(error.Invalid, stream_server.accept(listener, within(0))); // still open on this side
    endpoint_module.close(a);
    const c = try stream_server.accept(listener, within(5000));
    const d = try second.join();
    endpoint_module.close(d);
    endpoint_module.close(c);
    listener.close();
    try base.expectSame();
}

test "cancel releases an accept that waits; later accepts return Cancelled, and no client gets in after it" {
    const base = try Baseline.take();
    var name_buf: [64]u8 = undefined;
    const name = uniqueName(&name_buf, "cancel");
    const listener = try listen(name);
    errdefer listener.close();
    var result: ?endpoint_module.Error!*Endpoint = null;
    const waiter = try std.Thread.spawn(.{}, acceptResult, .{ listener, &result });
    sleepMs(10);
    listener.cancel();
    waiter.join();
    try testing.expectError(error.Cancelled, result.?);
    try testing.expectError(error.Cancelled, stream_server.accept(listener, within(0)));
    // The cancelled listener sets no new client up
    try testing.expectError(error.Timeout, connect(name, within(200)));
    listener.close();
    try base.expectSame();
}

// A listener of another protocol version (a SEGMENT frame with another version), as far as the client's first check:
// the client refuses it for good (INVALID), and doesn't retry.
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
        const Other = struct {
            fn serve(io: Io, listening: platform.Handle) void {
                var frame = frames.encodeSegment(1, .{ .capacity = 4096, .segment_size = segment.size(4096), .session_id = @splat(7) });
                const fields: *align(1) wire_protocol.ControlStreamSegment = @ptrCast(&frame);
                fields.header.version = wire_protocol.protocol_version + 1;
                const connection = switch (platform.acceptWithin(listening, null, 5000, null)) {
                    .done => |outcome| switch (outcome) {
                        .connection => |h| h,
                        else => return,
                    },
                    else => return,
                };
                const attached: ?platform.Handle = if (platform.passes_segment_handle) platform.createSegmentHandle() catch null else null;
                defer if (attached) |h| platform.close(h);
                _ = platform.sendFrame(connection, &frame, attached);
                var byte: [1]u8 = undefined;
                _ = platform.read(io, connection, &byte) catch {}; // until the client leaves
                // A handle the client connected on itself is `held`, closed by the test
                _ = platform.dropClient(connection, false);
            }
        };
        var other = try ref.io.concurrent(Other.serve, .{ ref.io, held });
        try testing.expectError(error.Invalid, connect(name, within(5000)));
        other.await(ref.io);
    }
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
    const listener = try listen(name);
    errdefer listener.close();
    var calls: usize = 0;
    try testing.expectError(error.Timeout, pollAccept(listener, 3, &calls)); // nobody yet
    var client = try RawClient.connectTo(name);
    var client_open = true;
    defer if (client_open) client.close();
    try testing.expectError(error.Timeout, stream_server.accept(listener, within(0))); // sends SEGMENT, then finds no READY
    try testing.expect(try client.awaitSegment(5000));
    try testing.expectError(error.Timeout, stream_server.accept(listener, within(0))); // still no READY
    const ready = frames.encodeReady(platform.pid());
    for (ready[0 .. ready.len - 1]) |*byte| {
        try client.send(byte[0..1]);
        try testing.expectError(error.Timeout, stream_server.accept(listener, within(0)));
    }
    try client.send(ready[ready.len - 1 ..]);
    const a = try pollAccept(listener, 1000, &calls);
    try testing.expectEqual(endpoint_module.Phase.connected, a.loadPhase());
    try testing.expectError(error.Invalid, stream_server.accept(listener, within(0)));
    // The client's end reaches the accepted connection
    client.close();
    client_open = false;
    try awaitPhase(a, .peer_dead);
    endpoint_module.close(a);
    listener.close();
    try base.expectSame();
}

// A setup that fails while `accept` waits for READY (the client leaves, or answers with something else) drops that
// client and goes on: the next client gets in, through the same call or a later one, and nothing leaks.
test "a client that leaves or answers garbage while accept waits for its READY is dropped, and the next gets in" {
    const base = try Baseline.take();
    var name_buf: [64]u8 = undefined;
    const name = uniqueName(&name_buf, "dropped");
    const listener = try listen(name);
    errdefer listener.close();
    for ([_]bool{ false, true }) |garbage| {
        var bad = try RawClient.connectTo(name);
        try testing.expectError(error.Timeout, stream_server.accept(listener, within(0)));
        try testing.expect(try bad.awaitSegment(5000));
        if (garbage) {
            var frame = frames.encodeReady(platform.pid());
            const fields: *align(1) wire_protocol.ControlStreamReady = @ptrCast(&frame);
            fields.header.frame_type = 9; // another frame type
            try bad.send(&frame);
            try testing.expectError(error.Timeout, stream_server.accept(listener, within(50)));
            try testing.expect(bad.dropped(5000));
            bad.close();
        } else {
            bad.close();
            try testing.expectError(error.Timeout, stream_server.accept(listener, within(50)));
        }
        // A real client, accepted by a call that blocks
        const b, const a = try pair(listener, name);
        endpoint_module.close(b);
        endpoint_module.close(a);
    }
    listener.close();
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
        const listener = try listen(name);
        defer listener.close();
        var client = try RawClient.connectTo(name);
        defer client.close();
        try testing.expectError(error.Timeout, stream_server.accept(listener, within(0)));
        try testing.expect(try client.awaitSegment(5000));
        listener.cancel();
        try testing.expectError(error.Cancelled, stream_server.accept(listener, within(0)));
        try testing.expect(client.dropped(5000));
    }
    // An accept that blocks, waiting for READY
    {
        const name = uniqueName(&name_buf, "cancel-blocked");
        const listener = try listen(name);
        defer listener.close();
        var client = try RawClient.connectTo(name);
        defer client.close();
        var result: ?endpoint_module.Error!*Endpoint = null;
        const waiter = try std.Thread.spawn(.{}, acceptResult, .{ listener, &result });
        const segment_came = client.awaitSegment(5000);
        listener.cancel();
        waiter.join();
        try testing.expect(try segment_came);
        try testing.expectError(error.Cancelled, result.?);
        try testing.expect(client.dropped(5000));
    }
    // A close
    {
        const name = uniqueName(&name_buf, "close-pending");
        const listener = try listen(name);
        var client = try RawClient.connectTo(name);
        defer client.close();
        try testing.expectError(error.Timeout, stream_server.accept(listener, within(0)));
        try testing.expect(try client.awaitSegment(5000));
        listener.close();
        try testing.expect(client.dropped(5000));
    }
    try base.expectSame();
}

test {
    if (build_options.slow_tests) _ = slow_tests;
}

// === Slow: races ===

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
            const listener = try listen(name);
            defer listener.close();
            var result: ?endpoint_module.Error!*Endpoint = null;
            const waiter = try std.Thread.spawn(.{}, acceptResult, .{ listener, &result });
            const start = nowMs();
            listener.cancel();
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
        const listener = try listen(name);
        errdefer listener.close();
        const Client = struct {
            fn run(n: []const u8, done: *std.atomic.Value(u32)) void {
                const conn = connect(n, within(20_000)) catch return;
                // The session ends once the server closes its side
                awaitPhase(conn, .peer_dead) catch {};
                endpoint_module.close(conn);
                _ = done.fetchAdd(1, .monotonic);
            }
        };
        var done = std.atomic.Value(u32).init(0);
        var threads: [4]std.Thread = undefined;
        for (&threads) |*thread| thread.* = try std.Thread.spawn(.{}, Client.run, .{ name, &done });
        for (0..threads.len) |_| {
            const a = try stream_server.accept(listener, within(10_000));
            try testing.expectError(error.Invalid, stream_server.accept(listener, within(0)));
            endpoint_module.close(a);
        }
        for (threads) |thread| thread.join();
        try testing.expectEqual(@as(u32, threads.len), done.load(.monotonic));
        listener.close();
        try base.expectSame();
    }

    // Clients one after another, each connecting as soon as the one before is closed, against an accept that keeps
    // timing out after 0, 1 or 2 ms at random: every client gets in, whatever step a timeout cuts. On Windows a listen
    // or a READY read that completes just as its wait times out is kept (the cancel-and-drain path).
    test "slow: accept with random timeouts of 0 to 2 ms lets every one of 300 clients in, and nothing leaks" {
        const base = try Baseline.take();
        var name_buf: [64]u8 = undefined;
        const name = uniqueName(&name_buf, "random");
        const listener = try listen(name);
        errdefer listener.close();
        const clients = 300;
        const Clients = struct {
            fn run(n: []const u8, connected: *std.atomic.Value(u32)) void {
                for (0..clients) |_| {
                    const conn = connect(n, within(10_000)) catch return;
                    awaitPhase(conn, .peer_dead) catch {};
                    endpoint_module.close(conn);
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
            const a = stream_server.accept(listener, within(prng.random().uintAtMost(u8, 2))) catch |err| {
                try testing.expectEqual(error.Timeout, err);
                timeouts += 1;
                continue;
            };
            accepted += 1;
            endpoint_module.close(a);
        }
        thread.join();
        std.debug.print("  {d} clients in, {d} accepts timed out\n", .{ accepted, timeouts });
        try testing.expectEqual(@as(usize, clients), accepted);
        try testing.expectEqual(@as(u32, clients), connected.load(.monotonic));
        listener.close();
        try base.expectSame();
    }
};
