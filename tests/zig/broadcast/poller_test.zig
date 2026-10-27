//! The publisher's poller, one thread that admits and watches every subscriber: a full broadcast's subscribers join,
//! half of them leave and their slots are taken again, join-and-leave churn, and the publisher's close racing its
//! subscribers' leaves; nothing is left.

const std = @import("std");
const fastipc = @import("fastipc");
const support = @import("../support.zig");
const shared = @import("shared.zig");
const c = support.c;

const lossy = shared.lossy;
const gated = shared.gated;
const joined = shared.joined;
const sendStamp = shared.sendStamp;
const recvStamp = shared.recvStamp;

/// Joins one subscriber of `role` to the broadcast on `name` into each of `subscribers`, a batch at a time, each on a
/// thread of its own, all at once: the publisher's poller sets several joiners up together.
fn joinMany(t: *support.Test, name: [*:0]const u8, role: c_int, subscribers: []support.Conn) !void {
    var joiners: [64]support.Joiner = undefined;
    var done: usize = 0;
    while (done < subscribers.len) {
        const batch = @min(joiners.len, subscribers.len - done);
        var started: usize = 0;
        while (started < batch and joiners[started].start(name, role, 10_000)) started += 1;
        var joins: usize = 0;
        for (joiners[0..started], subscribers[done..][0..started]) |*joiner, *subscriber| {
            if (joiner.join(subscriber) == c.FIPC_OK) joins += 1;
        }
        try t.checkEqual(started, batch, "a thread for each joiner");
        try t.checkEqual(joins, batch, "every joiner joins");
        done += batch;
    }
}

/// Every subscriber of `subscribers` receives the message `seq`.
fn allReceive(subscribers: []const support.Conn, seq: u64) bool {
    for (subscribers) |subscriber| {
        var result: c.fipc_result_t = 0;
        if (recvStamp(subscriber, 5000, &result) != seq) return false;
    }
    return true;
}

test "fast: a full broadcast's subscribers join, half of them leave and their slots are taken again; the publisher takes one thread" {
    var t = try support.begin(@src(), 120);
    defer t.end();
    // On Windows each subscriber's own watcher thread takes about 20 ms to start here, so 1024 in this process would
    // take this tier 25 s; the poller's unit test watches 1024 pipes there
    const count = if (support.windows) 128 else 1024;
    // Both ends of each subscriber's control connection are in this process
    if (!support.allowHandles(2 * count + 512)) return error.SkipZigTest;
    var buf: [64]u8 = undefined;
    const threads_before = fastipc.test_hooks.threadCount();
    var broadcast: support.Broadcast = .{};
    const name = try support.openBroadcast(&t, &broadcast, &buf, "bc_poller", 4096, count, c.FIPC_FOREVER);
    defer broadcast.close();
    const threads_open = fastipc.test_hooks.threadCount();
    _ = t.expect(threads_open <= threads_before + 1, "the broadcast's poller is one thread");

    const subscribers = try std.testing.allocator.alloc(support.Conn, count);
    defer std.testing.allocator.free(subscribers);
    @memset(subscribers, null);
    defer for (subscribers) |subscriber| c.fipc_close(subscriber);
    try joinMany(&t, name, lossy, subscribers);
    const threads_joined = fastipc.test_hooks.threadCount();
    std.debug.print("  threads: {d} before, {d} with the broadcast, {d} with its {d} subscribers\n", .{ threads_before, threads_open, threads_joined, count });
    // Each subscriber has a watcher of its own, in this process too; the publisher takes no more for them
    _ = t.expect(threads_joined <= threads_open + count + 16, "the publisher watches its subscribers on its one thread");
    _ = t.expectEqual(sendStamp(broadcast.publisher, 1, 32, 1000), c.FIPC_OK, "send");
    _ = t.expect(allReceive(subscribers, 1), "every subscriber receives");

    // Half leave; their slots are free again once the poller has seen each end
    for (subscribers[0 .. count / 2]) |*subscriber| {
        c.fipc_close(subscriber.*);
        subscriber.* = null;
    }
    try joinMany(&t, name, gated, subscribers[0 .. count / 2]);
    _ = t.expectEqual(sendStamp(broadcast.publisher, 2, 32, 1000), c.FIPC_OK, "send");
    _ = t.expect(allReceive(subscribers, 2), "every subscriber receives, old and new");
    return t.done();
}

/// Joins and leaves `name`'s broadcast `cycles` times as `role`, receiving once each time.
const Churner = struct {
    name: [*:0]const u8,
    role: c_int,
    cycles: u32,
    joined: u32 = 0,
    done: std.atomic.Value(bool) = .init(false),
    thread: std.Thread = undefined,

    fn run(churner: *Churner) void {
        defer churner.done.store(true, .release);
        for (0..churner.cycles) |_| {
            var subscriber: support.Conn = null;
            if (c.fipc_broadcast_join(churner.name, churner.role, &subscriber, 10_000) != c.FIPC_OK) continue;
            churner.joined += 1;
            var buf: [64]u8 = undefined;
            var len: usize = 0;
            _ = c.fipc_recv(subscriber, &buf, buf.len, &len, 0);
            c.fipc_close(subscriber);
        }
    }
};

fn allDone(churners: []Churner) bool {
    for (churners) |*churner| if (!churner.done.load(.acquire)) return false;
    return true;
}

test "fast: join and leave churn on a 4-slot broadcast: 1000 subscribers on 4 threads, then every slot is free, nothing is left" {
    var t = try support.begin(@src(), 120);
    defer t.end();
    var buf: [64]u8 = undefined;
    const name = support.uniqueName(&buf, "bc_churn");
    // A first cycle before counting: what the process creates once and keeps for every later broadcast isn't counted
    {
        var broadcast: support.Broadcast = .{};
        try t.check(broadcast.open(name, 4096, 4, c.FIPC_FOREVER), "a broadcast");
        c.fipc_close(try joined(&t, name, gated));
        broadcast.close();
    }
    const handles = support.handleCount();
    {
        var broadcast: support.Broadcast = .{};
        try t.check(broadcast.open(name, 4096, 4, c.FIPC_FOREVER), "a broadcast of 4 subscribers");
        defer broadcast.close();
        var churners: [4]Churner = undefined;
        for (&churners, 0..) |*churner, i| {
            churner.* = .{ .name = name, .role = if (i % 2 == 0) lossy else gated, .cycles = 250 };
            churner.thread = try std.Thread.spawn(.{}, Churner.run, .{churner});
        }
        // The publisher goes on meanwhile; a gated subscriber that hasn't received yet may hold it back
        var seq: u64 = 0;
        while (!allDone(&churners)) : (seq += 1) {
            _ = sendStamp(broadcast.publisher, seq, 32, 0);
            std.Thread.yield() catch {};
        }
        var total: u32 = 0;
        for (&churners) |*churner| {
            churner.thread.join();
            total += churner.joined;
        }
        _ = t.expectEqual(total, 1000, "every join succeeds");
        // No slot is left held or retired
        var subscribers: [4]support.Conn = @splat(null);
        defer for (subscribers) |subscriber| c.fipc_close(subscriber);
        try joinMany(&t, name, gated, &subscribers);
    }
    try t.check(support.freeAtOnce(name), "the name is free at once after the close");
    _ = t.expect(support.handlesBack(handles), "the descriptors or handles are back");
    return t.done();
}

/// Closes `subscriber` once `go` is set.
fn closeAtGo(subscriber: support.Conn, go: *std.atomic.Value(bool)) void {
    while (!go.load(.acquire)) std.atomic.spinLoopHint();
    c.fipc_close(subscriber);
}

test "fast: the publisher's close racing its subscribers' leaves: each end is handled once, nothing is left" {
    var t = try support.begin(@src(), 120);
    defer t.end();
    var buf: [64]u8 = undefined;
    const name = support.uniqueName(&buf, "bc_close_race");
    var handles: usize = 0;
    for (0..30) |round| {
        // The first round settles what this process creates once
        if (round == 1) handles = support.handleCount();
        var broadcast: support.Broadcast = .{};
        try t.check(broadcast.open(name, 4096, 8, c.FIPC_FOREVER), "a broadcast of 8 subscribers");
        var subscribers: [8]support.Conn = @splat(null);
        joinMany(&t, name, lossy, subscribers[0..4]) catch |err| {
            for (subscribers) |subscriber| c.fipc_close(subscriber);
            broadcast.close();
            return err;
        };
        joinMany(&t, name, gated, subscribers[4..]) catch |err| {
            for (subscribers) |subscriber| c.fipc_close(subscriber);
            broadcast.close();
            return err;
        };
        _ = sendStamp(broadcast.publisher, round, 32, 0);
        var go: std.atomic.Value(bool) = .init(false);
        var closers: [8]std.Thread = undefined;
        var started: usize = 0;
        while (started < closers.len) : (started += 1) {
            closers[started] = std.Thread.spawn(.{}, closeAtGo, .{ subscribers[started], &go }) catch break;
        }
        go.store(true, .release);
        c.fipc_close(broadcast.publisher);
        broadcast.publisher = null;
        for (closers[0..started]) |closer| closer.join();
        for (subscribers[started..]) |subscriber| c.fipc_close(subscriber);
        broadcast.close();
        try t.check(started == closers.len, "a thread for each subscriber's close");
        try t.check(support.freeAtOnce(name), "the name is free at once after the close");
    }
    _ = t.expect(support.handlesBack(handles), "the descriptors or handles are back");
    return t.done();
}
