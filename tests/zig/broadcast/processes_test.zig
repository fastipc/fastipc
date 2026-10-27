//! Subscriber processes (peer.zig), in the slow tier: the checked stream (lossy subscribers whole messages in order,
//! gated ones all of it, also with twice as many processes as CPUs), and a gated subscriber process that stops
//! receiving, reads a message it held in place after its unsubscribe, or is paused.

const std = @import("std");
const support = @import("../support.zig");
const shared = @import("shared.zig");
const c = support.c;

const sendStamp = shared.sendStamp;
const awaitSubscribers = shared.awaitSubscribers;

/// The checksummed stress: `lossy_count` lossy subscriber processes and four gated ones (two receiving in place) on a
/// broadcast of `capacity` bytes receive `count` checked messages (`support.Stamp`, up to 1 KiB) until the writer's close:
/// each gated one every message in order, each lossy one only whole messages, in order, with gaps only right after an
/// OVERRUN, up to the last (peer.zig `broadcastSub`).
fn stress(t: *support.Test, comptime prefix: []const u8, capacity: usize, lossy_count: usize, count: u64) !void {
    var buf: [64]u8 = undefined;
    var broadcast: support.Broadcast = .{};
    const name = try support.openBroadcast(t, &broadcast, &buf, prefix, capacity, 128, c.FIPC_FOREVER);
    defer broadcast.close();
    var count_buf: [24]u8 = undefined;
    const count_arg = try std.mem.print(&count_buf, "{d}", .{count});
    const gated_roles = [_][]const u8{ "gated", "gated", "gated-inplace", "gated-inplace" };
    var peers: [68]*support.Peer = undefined;
    const total = lossy_count + gated_roles.len;
    std.debug.assert(total <= peers.len);
    var spawned: usize = 0;
    defer for (peers[0..spawned]) |peer| peer.deinit();
    while (spawned < total) : (spawned += 1) {
        const role = if (spawned < gated_roles.len) gated_roles[spawned] else "lossy";
        peers[spawned] = try support.Peer.spawn(&.{ "broadcast-sub", name, role, count_arg });
    }
    try awaitSubscribers(t, &broadcast, total);
    for (peers[0..total]) |peer| try t.check(peer.waitFor("J", 10_000), "the subscriber process joined");
    for (0..count) |i| try t.checkEqual(sendStamp(broadcast.publisher, i, support.Stamp.sizeOf(i, 1024), 10_000), c.FIPC_OK, "send");
    c.fipc_close(broadcast.publisher);
    broadcast.publisher = null;
    for (peers[0..total]) |peer| {
        _ = t.expect(peer.waitTimeout(30_000), "the subscriber process ends");
        _ = t.expect(peer.exitedWith(0), "with the whole stream");
    }
}

test "slow: subscriber processes receive one checked stream: lossy ones whole and in order, gated ones all of it" {
    var t = try support.begin(@src(), 120);
    defer t.end();
    try stress(&t, "bc_procs", 65536, 4, 20_000);
    return t.done();
}

test "slow: the checked stream with twice as many subscriber processes as CPUs: gated ones still get all of it" {
    var t = try support.begin(@src(), 180);
    defer t.end();
    const cpus = std.Thread.getCpuCount() catch 4;
    try stress(&t, "bc_procs_over", 65536, @min(2 * cpus, 64), 5_000);
    return t.done();
}

/// A broadcast of 4 KiB with `max_stall_ms` and one subscriber process of `broadcast-stall` in `mode`, which received
/// (or holds in place) message 0 and receives nothing more; the writer filled every cell it may: the next message,
/// `seq`, waits for that subscriber.
const Stalled = struct {
    broadcast: support.Broadcast = .{},
    peer: *support.Peer = undefined,
    seq: u64 = 0,
    /// When the fill began: the stall's clock starts with its last send, which waits for the subscriber.
    fill_begun: u64 = 0,

    fn open(stalled: *Stalled, t: *support.Test, name_buf: []u8, comptime prefix: []const u8, mode: []const u8, max_stall_ms: c_int) !void {
        const name = try support.openBroadcast(t, &stalled.broadcast, name_buf, prefix, 4096, 1, max_stall_ms);
        errdefer stalled.broadcast.close();
        stalled.peer = try support.Peer.spawn(&.{ "broadcast-stall", name, mode });
        errdefer stalled.peer.deinit();
        try awaitSubscribers(t, &stalled.broadcast, 1);
        try t.check(stalled.peer.waitFor("J", 10_000), "the subscriber process joined");
        try t.checkEqual(sendStamp(stalled.broadcast.publisher, 0, 48, 1000), c.FIPC_OK, "send");
        try t.check(stalled.peer.waitFor("R", 10_000), "the subscriber process received it");
        stalled.seq = 1;
        stalled.fill_begun = support.nowMs();
        while (sendStamp(stalled.broadcast.publisher, stalled.seq, 48, 0) == c.FIPC_OK) stalled.seq += 1;
        try t.check(stalled.seq == 64 or stalled.seq == 65, "the writer filled the cells the subscriber left it");
    }

    fn close(stalled: *Stalled) void {
        stalled.peer.deinit();
        stalled.broadcast.close();
    }
};

test "slow: a gated subscriber process that stops receiving is unsubscribed: the writer goes on, its calls are UNSUBSCRIBED" {
    var t = try support.beginTimed(@src(), 60);
    defer t.end();
    for ([_][]const u8{ "copy", "inplace" }) |mode| {
        var buf: [64]u8 = undefined;
        var stalled: Stalled = .{};
        try stalled.open(&t, &buf, "bc_stall_proc", mode, 200);
        defer stalled.close();
        try t.checkEqual(sendStamp(stalled.broadcast.publisher, stalled.seq, 48, 5000), c.FIPC_OK, "the send that waited for it");
        const took = support.nowMs() - stalled.fill_begun;
        _ = t.expect(took >= 190 and took < 2000, "about max_stall_ms");
        for (stalled.seq + 1..stalled.seq + 200) |i| try t.checkEqual(sendStamp(stalled.broadcast.publisher, i, 48, 0), c.FIPC_OK, "the writer waits for nobody");
        stalled.peer.send("x");
        _ = t.expect(stalled.peer.waitFor("U", 10_000), "its calls are UNSUBSCRIBED, and it closes");
        _ = t.expect(stalled.peer.waitTimeout(10_000) and stalled.peer.exitedWith(0), "it exits");
    }
    return t.done();
}

test "slow: an unsubscribed subscriber process that reads the message it held in place dies of an access fault" {
    var t = try support.begin(@src(), 60);
    defer t.end();
    var buf: [64]u8 = undefined;
    var stalled: Stalled = .{};
    try stalled.open(&t, &buf, "bc_stall_touch", "touch", 100);
    defer stalled.close();
    try t.checkEqual(sendStamp(stalled.broadcast.publisher, stalled.seq, 48, 5000), c.FIPC_OK, "the writer goes on once it unsubscribed it");
    stalled.peer.send("x");
    try t.check(stalled.peer.waitFor("TOUCH", 10_000), "the subscriber process reads its pointer");
    try t.check(stalled.peer.waitTimeout(30_000), "it ends");
    _ = t.expect(!stalled.peer.waitFor("SURVIVED", 5000), "not after the read");
    const term = stalled.peer.wait();
    // POSIX: SIGSEGV (or, under ThreadSanitizer, its report's exit code); Windows: std's handler's exit code
    _ = t.expect(!(term == .exited and term.exited == 0), "it died of the fault");
    for (stalled.seq + 1..stalled.seq + 200) |i| try t.checkEqual(sendStamp(stalled.broadcast.publisher, i, 48, 0), c.FIPC_OK, "the writer is unaffected");
    return t.done();
}

test "slow: a paused gated subscriber process: the writer's sends time out; resumed, it is unsubscribed and the writer goes on" {
    var t = try support.beginTimed(@src(), 60);
    defer t.end();
    var buf: [64]u8 = undefined;
    var stalled: Stalled = .{};
    try stalled.open(&t, &buf, "bc_stall_paused", "copy", 100);
    defer stalled.close();
    stalled.peer.pause();
    // UNSUBSCRIBE goes out after 100 ms, but a process that doesn't run can't answer: nothing frees its slot
    _ = t.expectEqual(sendStamp(stalled.broadcast.publisher, stalled.seq, 48, 500), c.FIPC_TIMEOUT, "a send times out");
    _ = t.expectEqual(sendStamp(stalled.broadcast.publisher, stalled.seq, 48, 300), c.FIPC_TIMEOUT, "and the next");
    const resumed = support.nowMs();
    stalled.peer.unpause();
    try t.checkEqual(sendStamp(stalled.broadcast.publisher, stalled.seq, 48, 5000), c.FIPC_OK, "resumed, it is unsubscribed: the send goes through");
    _ = t.expect(support.nowMs() - resumed < 2000, "soon after");
    stalled.peer.send("x");
    _ = t.expect(stalled.peer.waitFor("U", 10_000), "its calls are UNSUBSCRIBED, and it closes");
    _ = t.expect(stalled.peer.waitTimeout(10_000) and stalled.peer.exitedWith(0), "it exits");
    return t.done();
}
