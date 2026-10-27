//! The publisher's count of joined subscribers, `fipc_broadcast_wait_for_members`: after each join and each leave, a
//! joiner the count includes receives every message sent afterwards, a wait that a join on another thread ends, its
//! timeout and its cancel.

const std = @import("std");
const fastipc = @import("fastipc");
const support = @import("../support.zig");
const shared = @import("shared.zig");
const c = support.c;

const lossy = shared.lossy;
const gated = shared.gated;
const joined = shared.joined;
const NativeJoiner = shared.NativeJoiner;
const countBecomes = shared.countBecomes;

test "fast: a subscriber the publisher's count includes receives every message sent afterwards, however late it starts" {
    // The native API: the joiner's hook (`hold_welcomed`) is this module's, not the static library's
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    const name = support.uniqueName(&buf, "bc_counted");
    const publisher = try fastipc.broadcastCreate(std.testing.allocator, name, 4096, 2, -1);
    defer publisher.close();
    const hooks = fastipc.test_hooks;
    defer hooks.hold_welcomed.store(false, .release);
    var seq: u64 = 0;
    for ([_]fastipc.SubscriberRole{ .lossy, .gated }) |role| {
        // The last one's close is counted once the publisher has seen its end
        const begun = support.nowMs();
        while (try publisher.waitForMembers(0, .none) != 0 and support.nowMs() - begun < 5000) support.sleepMs(1);
        try t.checkEqual(try publisher.waitForMembers(0, .none), 0, "nobody is joined");
        // The joiner waits between WELCOME and its start, counted by the publisher
        hooks.welcomed_held.store(0, .monotonic);
        hooks.hold_welcomed.store(true, .release);
        var joiner: NativeJoiner = .{ .name = name, .role = role };
        joiner.thread = try std.Thread.spawn(.{}, NativeJoiner.run, .{&joiner});
        const counted = publisher.waitForMembers(1, support.within(5000));
        while (hooks.welcomed_held.load(.acquire) == 0 and counted != error.Timeout) std.Thread.yield() catch {};
        // Three messages; to a gated joiner, as many as it lets the publisher send from its start on: at most its 64
        // cells
        const first = seq;
        const gated_role = role == .gated;
        var held = false;
        var payload: [48]u8 = @splat(0);
        while (seq - first < @as(u64, if (gated_role) 65 else 3)) : (seq += 1) {
            std.mem.writeInt(u64, payload[0..8], seq, .little);
            publisher.send(&payload, support.within(0)) catch |err| {
                held = err == error.Timeout and seq - first >= 3;
                break;
            };
        }
        _ = t.expect(held == gated_role, "only the gated joiner holds the publisher back, from its start on");
        hooks.hold_welcomed.store(false, .release);
        joiner.thread.join();
        try t.checkEqual(try counted, 1, "the publisher's count includes the joiner");
        const subscriber = try joiner.result;
        for (first..seq) |expected| {
            const len = subscriber.recv(&payload, support.within(1000)) catch |err| {
                std.debug.print("  message {d}: {t}\n", .{ expected, err });
                _ = t.expect(false, "every message sent after the count is received");
                break;
            };
            if (!t.expectEqual(std.mem.readInt(u64, payload[0..8], .little), expected, "the messages in order") or len != payload.len) break;
        }
        _ = t.expect(subscriber.recv(&payload, support.within(0)) == error.Timeout, "nothing more");
        subscriber.close();
    }
    return t.done();
}

test "fast: fipc_broadcast_wait_for_members counts the joined subscribers after each join and each leave; INVALID but on a publisher" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    var broadcast: support.Broadcast = .{};
    const name = try support.openBroadcast(&t, &broadcast, &buf, "bc_count", 4096, 3, c.FIPC_FOREVER);
    defer broadcast.close();
    var count: u32 = 99;
    _ = t.expectEqual(c.fipc_broadcast_wait_for_members(broadcast.publisher, 0, &count, 0), c.FIPC_OK, "the count of an empty broadcast");
    _ = t.expectEqual(count, 0, "none");
    var subscribers: [3]support.Conn = @splat(null);
    defer for (subscribers) |subscriber| c.fipc_close(subscriber);
    for (&subscribers, 1..) |*subscriber, n| {
        subscriber.* = try joined(&t, name, if (n == 2) gated else lossy);
        count = 99;
        _ = t.expectEqual(c.fipc_broadcast_wait_for_members(broadcast.publisher, @intCast(n), &count, 0), c.FIPC_OK, "counted once its join returned");
        _ = t.expectEqual(count, n, "the count");
    }
    count = 99;
    _ = t.expectEqual(c.fipc_broadcast_wait_for_members(broadcast.publisher, 4, &count, 0), c.FIPC_INVALID, "at_least over max_subscribers");
    _ = t.expectEqual(c.fipc_broadcast_wait_for_members(subscribers[0], 0, &count, 0), c.FIPC_INVALID, "on a lossy subscriber");
    _ = t.expectEqual(c.fipc_broadcast_wait_for_members(subscribers[1], 0, &count, 0), c.FIPC_INVALID, "on a gated subscriber");
    _ = t.expectEqual(c.fipc_broadcast_wait_for_members(broadcast.publisher, 0, null, 0), c.FIPC_INVALID, "a NULL out_count");
    _ = t.expectEqual(count, 99, "INVALID stores nothing");
    // Each leave, lossy and gated: counted until the poller has seen the end of its control connection
    for (&subscribers, 1..) |*subscriber, n| {
        c.fipc_close(subscriber.*);
        subscriber.* = null;
        _ = t.expect(countBecomes(broadcast.publisher, @intCast(3 - n), 5000), "the count drops after a leave");
    }
    // A freed slot's next subscriber is counted
    subscribers[0] = try joined(&t, name, lossy);
    _ = t.expectEqual(c.fipc_broadcast_wait_for_members(broadcast.publisher, 1, &count, 0), c.FIPC_OK, "a new subscriber is counted");
    _ = t.expectEqual(count, 1, "the count");
    return t.done();
}

/// Joins `count` lossy subscribers to a broadcast on a thread of its own, each after a pause of `pause_ms`.
const Joiners = struct {
    thread: ?std.Thread = null,
    subscribers: [4]support.Conn = @splat(null),
    joined: usize = 0,

    fn start(joiners: *Joiners, name: [*:0]const u8, count: usize, pause_ms: u64) !void {
        joiners.thread = try std.Thread.spawn(.{}, run, .{ joiners, name, count, pause_ms });
    }

    fn run(joiners: *Joiners, name: [*:0]const u8, count: usize, pause_ms: u64) void {
        for (joiners.subscribers[0..count]) |*subscriber| {
            support.sleepMs(pause_ms);
            if (!support.joinNow(name, lossy, subscriber, 5000)) return;
            joiners.joined += 1;
        }
    }

    /// Waits for the thread, then closes the subscribers; how many joined.
    fn finish(joiners: *Joiners) usize {
        if (joiners.thread) |thread| thread.join();
        joiners.thread = null;
        for (joiners.subscribers) |subscriber| c.fipc_close(subscriber);
        return joiners.joined;
    }
};

test "fast: a wait for subscribers ends at the join on another thread that reaches the count: 1, then 3" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    var broadcast: support.Broadcast = .{};
    const name = try support.openBroadcast(&t, &broadcast, &buf, "bc_count_wait", 4096, 4, c.FIPC_FOREVER);
    defer broadcast.close();
    {
        var joiners: Joiners = .{};
        try joiners.start(name, 1, 100);
        var count: u32 = 99;
        const result = c.fipc_broadcast_wait_for_members(broadcast.publisher, 1, &count, 10_000);
        _ = t.expectEqual(joiners.finish(), 1, "one joined");
        _ = t.expectEqual(result, c.FIPC_OK, "the wait for one");
        _ = t.expectEqual(count, 1, "its count");
    }
    try t.check(countBecomes(broadcast.publisher, 0, 5000), "the subscriber left");
    {
        var joiners: Joiners = .{};
        try joiners.start(name, 3, 30);
        var count: u32 = 99;
        const result = c.fipc_broadcast_wait_for_members(broadcast.publisher, 3, &count, 10_000);
        _ = t.expectEqual(joiners.finish(), 3, "three joined");
        _ = t.expectEqual(result, c.FIPC_OK, "the wait for three, past the first two");
        _ = t.expectEqual(count, 3, "its count");
    }
    return t.done();
}

test "fast: fipc_broadcast_wait_for_members' timeout stores the current count; a timeout of 0 polls" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    var broadcast: support.Broadcast = .{};
    const name = try support.openBroadcast(&t, &broadcast, &buf, "bc_count_timeout", 4096, 2, c.FIPC_FOREVER);
    defer broadcast.close();
    const subscriber = try joined(&t, name, lossy);
    defer c.fipc_close(subscriber);
    var count: u32 = 99;
    _ = t.expectEqual(c.fipc_broadcast_wait_for_members(broadcast.publisher, 2, &count, 0), c.FIPC_TIMEOUT, "a poll for two");
    _ = t.expectEqual(count, 1, "the count at the poll");
    count = 99;
    const begun = support.nowMs();
    _ = t.expectEqual(c.fipc_broadcast_wait_for_members(broadcast.publisher, 2, &count, 100), c.FIPC_TIMEOUT, "a wait of 100 ms for two");
    _ = t.expect(support.nowMs() - begun >= 90, "after its timeout");
    _ = t.expectEqual(count, 1, "the count at the timeout");
    _ = t.expectEqual(c.fipc_broadcast_wait_for_members(broadcast.publisher, 1, &count, 0), c.FIPC_OK, "a poll for one");
    return t.done();
}

fn countWaiting(publisher: support.Conn, at_least: u32, count: *u32, result: *c.fipc_result_t) void {
    result.* = c.fipc_broadcast_wait_for_members(publisher, at_least, count, c.FIPC_FOREVER);
}

test "fast: the publisher's cancel ends a wait for subscribers at once; a count that needn't wait still works" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    var broadcast: support.Broadcast = .{};
    const name = try support.openBroadcast(&t, &broadcast, &buf, "bc_count_cancel", 4096, 2, c.FIPC_FOREVER);
    defer broadcast.close();
    const subscriber = try joined(&t, name, lossy);
    defer c.fipc_close(subscriber);
    var count: u32 = 99;
    var result: c.fipc_result_t = c.FIPC_OK;
    const waiter = try std.Thread.spawn(.{}, countWaiting, .{ broadcast.publisher, 2, &count, &result });
    support.sleepMs(50);
    const begun = support.nowMs();
    c.fipc_cancel(broadcast.publisher);
    waiter.join();
    _ = t.expectEqual(result, c.FIPC_CANCELLED, "the waiting count is CANCELLED");
    _ = t.expect(support.nowMs() - begun < 1000, "at once");
    _ = t.expectEqual(count, 1, "with the count");
    _ = t.expectEqual(c.fipc_broadcast_wait_for_members(broadcast.publisher, 2, &count, c.FIPC_FOREVER), c.FIPC_CANCELLED, "a later wait too");
    _ = t.expectEqual(c.fipc_broadcast_wait_for_members(broadcast.publisher, 1, &count, c.FIPC_FOREVER), c.FIPC_OK, "a count already reached");
    _ = t.expectEqual(count, 1, "its count");
    return t.done();
}
