//! A broadcast's peer process killed or crashed (peer.zig), in the slow tier: a subscriber process killed mid-stream,
//! many at once, the publisher's count after a kill, the publisher's process killed, and a crash at each step of a join
//! on either side.

const std = @import("std");
const builtin = @import("builtin");
const fastipc = @import("fastipc");
const support = @import("../support.zig");
const shared = @import("shared.zig");
const c = support.c;

const lossy = shared.lossy;
const gated = shared.gated;
const torn = shared.torn;
const joined = shared.joined;
const sendStamp = shared.sendStamp;
const recvStamp = shared.recvStamp;
const Receiver = shared.Receiver;
const countBecomes = shared.countBecomes;
const awaitSubscribers = shared.awaitSubscribers;

fn sendWaiting(writer: support.Conn, seq: u64, result: *c.fipc_result_t) void {
    result.* = sendStamp(writer, seq, 48, 5000);
}

test "slow: a subscriber process killed mid-stream: the writer goes on at once, its slot is free, the others go on" {
    var t = try support.beginTimed(@src(), 60);
    defer t.end();
    var buf: [64]u8 = undefined;
    var broadcast: support.Broadcast = .{};
    const name = try support.openBroadcast(&t, &broadcast, &buf, "bc_kill_sub", 65536, 2, c.FIPC_FOREVER);
    defer broadcast.close();
    const count = 3000;
    const subscriber = try support.Peer.spawn(&.{ "broadcast-sub", name, "lossy", "3000" });
    defer subscriber.deinit();
    const holder = try support.Peer.spawn(&.{ "broadcast-hold", name, "gated" });
    defer holder.deinit();
    try awaitSubscribers(&t, &broadcast, 2);
    try t.check(subscriber.waitFor("J", 10_000) and holder.waitFor("J", 10_000), "both joined");
    // The gated subscriber, which never receives, holds the writer back after one lap of the 1024 cells
    var seq: u64 = 0;
    while (seq < 1024) : (seq += 1) try t.checkEqual(sendStamp(broadcast.publisher, seq, 48, 0), c.FIPC_OK, "send");
    _ = t.expectEqual(sendStamp(broadcast.publisher, seq, 48, 0), c.FIPC_TIMEOUT, "the writer waits for the gated subscriber");
    var result: c.fipc_result_t = c.FIPC_INVALID;
    const sender = try std.Thread.spawn(.{}, sendWaiting, .{ broadcast.publisher, seq, &result });
    support.sleepMs(100);
    const killed = support.nowMs();
    holder.kill();
    sender.join();
    _ = t.expectEqual(result, c.FIPC_OK, "the waiting send goes through");
    _ = t.expect(support.nowMs() - killed < 1000, "within a second of the kill");
    seq += 1;
    // The slot is free again: a new subscriber gets it
    const next = try support.Peer.spawn(&.{ "broadcast-hold", name, "lossy" });
    defer next.deinit();
    try t.check(next.waitFor("J", 10_000), "the next subscriber gets the slot, and joins");
    while (seq < count) : (seq += 1) try t.checkEqual(sendStamp(broadcast.publisher, seq, 48, 5000), c.FIPC_OK, "send");
    c.fipc_close(broadcast.publisher);
    broadcast.publisher = null;
    _ = t.expect(subscriber.waitTimeout(30_000) and subscriber.exitedWith(0), "the lossy subscriber received the whole stream");
    return t.done();
}

test "slow: a subscriber process killed: the publisher's count drops once it has seen the end" {
    var t = try support.begin(@src(), 60);
    defer t.end();
    var buf: [64]u8 = undefined;
    var broadcast: support.Broadcast = .{};
    const name = try support.openBroadcast(&t, &broadcast, &buf, "bc_count_kill", 4096, 2, c.FIPC_FOREVER);
    defer broadcast.close();
    const lossy_peer = try support.Peer.spawn(&.{ "broadcast-hold", name, "lossy" });
    defer lossy_peer.deinit();
    const gated_peer = try support.Peer.spawn(&.{ "broadcast-hold", name, "gated" });
    defer gated_peer.deinit();
    try awaitSubscribers(&t, &broadcast, 2);
    try t.check(lossy_peer.waitFor("J", 10_000) and gated_peer.waitFor("J", 10_000), "both joined");
    var count: u32 = 0;
    _ = t.expectEqual(c.fipc_broadcast_wait_for_members(broadcast.publisher, 2, &count, 0), c.FIPC_OK, "two joined");
    gated_peer.kill();
    _ = t.expect(countBecomes(broadcast.publisher, 1, 5000), "the count drops after the gated subscriber's kill");
    lossy_peer.kill();
    _ = t.expect(countBecomes(broadcast.publisher, 0, 5000), "and after the lossy one's");
    return t.done();
}

test "slow: many subscriber processes killed at once: the send that waits for them goes through, and every slot is free again" {
    var t = try support.beginTimed(@src(), 180);
    defer t.end();
    // 64, or 16 under TSan on macOS: there each TSan process holds about 75 MB of wired kernel memory (its VM map
    // entries), freed only seconds after its death, and two rounds of 64 exhaust an 8 GB Mac
    const count = if (builtin.sanitize_thread and support.macos) 16 else 64;
    var buf: [64]u8 = undefined;
    var broadcast: support.Broadcast = .{};
    const name = try support.openBroadcast(&t, &broadcast, &buf, "bc_kill_many", 65536, count, c.FIPC_FOREVER);
    defer broadcast.close();
    var peers: [count]*support.Peer = undefined;
    var spawned: usize = 0;
    defer for (peers[0..spawned]) |peer| peer.deinit();
    while (spawned < count) : (spawned += 1) {
        peers[spawned] = try support.Peer.spawn(&.{ "broadcast-hold", name, if (spawned % 2 == 0) "gated" else "lossy" });
    }
    try awaitSubscribers(&t, &broadcast, count);
    for (peers) |peer| try t.check(peer.waitFor("J", 10_000), "the subscriber process joined");
    // The gated subscribers, which never receive, hold the writer back after one lap of the 1024 cells
    var seq: u64 = 0;
    while (seq < 1024) : (seq += 1) try t.checkEqual(sendStamp(broadcast.publisher, seq, 48, 0), c.FIPC_OK, "send");
    _ = t.expectEqual(sendStamp(broadcast.publisher, seq, 48, 0), c.FIPC_TIMEOUT, "the writer waits for the gated subscribers");
    var result: c.fipc_result_t = c.FIPC_INVALID;
    const sender = try std.Thread.spawn(.{}, sendWaiting, .{ broadcast.publisher, seq, &result });
    support.sleepMs(100);
    const killed = support.nowMs();
    for (peers) |peer| peer.terminate();
    sender.join();
    const waited = support.nowMs() - killed;
    std.debug.print("  the waiting send returned {d} ms after the kills\n", .{waited});
    _ = t.expectEqual(result, c.FIPC_OK, "the waiting send goes through");
    _ = t.expect(waited < 2000, "within two seconds of the kills");
    for (peers) |peer| _ = peer.wait();

    // Every slot is free again: as many new subscribers get in
    var next: [count]*support.Peer = undefined;
    var respawned: usize = 0;
    defer for (next[0..respawned]) |peer| peer.deinit();
    while (respawned < count) : (respawned += 1) next[respawned] = try support.Peer.spawn(&.{ "broadcast-hold", name, "lossy" });
    try awaitSubscribers(&t, &broadcast, count);
    for (next) |peer| try t.check(peer.waitFor("J", 10_000), "the next subscriber process joined");
    return t.done();
}

test "slow: the publisher's process killed: its subscribers receive what it sent, then DISCONNECTED" {
    var t = try support.beginTimed(@src(), 60);
    defer t.end();
    var buf: [64]u8 = undefined;
    const name = support.uniqueName(&buf, "bc_kill_pub");
    defer support.clearName(name); // macOS: the files the killed publisher's listener left
    const publisher = try support.Peer.spawn(&.{ "broadcast-pub", name, "2", "500" });
    defer publisher.deinit();
    try t.check(publisher.waitFor("L", 10_000), "the publisher listens");
    var receivers: [2]Receiver = undefined;
    for (&receivers) |*receiver| {
        receiver.* = .{ .subscriber = null };
        try t.checkEqual(c.fipc_broadcast_join(name, lossy, &receiver.subscriber, 10_000), c.FIPC_OK, "a subscriber of this process joins");
    }
    try t.check(publisher.waitFor("ALL", 10_000), "the publisher admitted both");
    for (&receivers) |*receiver| receiver.thread = try std.Thread.spawn(.{}, Receiver.run, .{receiver});
    publisher.send("x");
    try t.check(publisher.waitFor("SENT", 10_000), "the publisher sent its stream");
    const killed = support.nowMs();
    publisher.kill();
    for (&receivers) |*receiver| {
        receiver.thread.join();
        c.fipc_close(receiver.subscriber);
        _ = t.expectEqual(receiver.end, c.FIPC_DISCONNECTED, "DISCONNECTED");
        _ = t.expect(receiver.tally.complete(500) and receiver.tally.overruns == 0, "after every message");
    }
    _ = t.expect(support.nowMs() - killed < 2000, "promptly after the kill");
    return t.done();
}

/// A subscriber's end, as the native API reports it (`Conn.ended`).
fn subscriberEnded(subscriber: support.Conn) bool {
    const conn: fastipc.Conn = .{ .endpoint = @ptrCast(@alignCast(subscriber.?)) };
    return conn.ended();
}

test "slow: the publisher's process killed: a gated subscriber receives what it sent, copied and checked; in place, DISCONNECTED" {
    var t = try support.begin(@src(), 60);
    defer t.end();
    var buf: [64]u8 = undefined;
    const name = support.uniqueName(&buf, "bc_kill_pub_gated");
    defer support.clearName(name); // macOS: the files the killed publisher's listener left
    const publisher = try support.Peer.spawn(&.{ "broadcast-pub", name, "2", "500" });
    defer publisher.deinit();
    try t.check(publisher.waitFor("L", 10_000), "the publisher listens");
    var subscribers: [2]support.Conn = .{ null, null };
    for (&subscribers) |*subscriber| try t.checkEqual(c.fipc_broadcast_join(name, gated, subscriber, 10_000), c.FIPC_OK, "a gated subscriber of this process joins");
    defer for (subscribers) |subscriber| c.fipc_close(subscriber);
    try t.check(publisher.waitFor("ALL", 10_000), "the publisher admitted both");
    publisher.send("x");
    try t.check(publisher.waitFor("SENT", 10_000), "the publisher sent its stream");
    publisher.kill();
    // Its end comes without the writer's close: nothing protects the messages in place any more
    const begun = support.nowMs();
    for (subscribers) |subscriber| {
        while (!subscriberEnded(subscriber) and support.nowMs() - begun < 5000) support.sleepMs(5);
        try t.check(subscriberEnded(subscriber), "the subscriber sees the end");
    }
    var at: ?*const anyopaque = null;
    var len: usize = 0;
    _ = t.expectEqual(c.fipc_recv_acquire(subscribers[1], &at, &len, 0), c.FIPC_DISCONNECTED, "in place: DISCONNECTED, with 500 messages there");
    for (subscribers) |subscriber| {
        var tally: support.Stamp.Tally = .{};
        var result: c.fipc_result_t = 0;
        while (true) {
            const seq = recvStamp(subscriber, 5000, &result);
            if (result != c.FIPC_OK and result != torn) break;
            tally.message(seq);
        }
        _ = t.expectEqual(result, c.FIPC_DISCONNECTED, "DISCONNECTED");
        _ = t.expect(tally.complete(500) and tally.overruns == 0, "after every message, whole and in order");
    }
    return t.done();
}

test "slow: a publisher crashed at each step of a join: the joiner fails or reads the end, and the name is free" {
    var t = try support.begin(@src(), 120);
    defer t.end();
    for ([_][]const u8{ "accepted", "offer_sent", "join_read", "welcome_sent", "member_watching" }) |step| {
        var name_buf: [64]u8 = undefined;
        const name = try std.mem.printSentinel(&name_buf, "bc-crash-pub-{s}-{d}", .{ step, support.pid() }, 0);
        const crashing = try support.Peer.spawn(&.{ "native-broadcast-crash", "publish", name, step });
        defer crashing.deinit();
        try t.check(crashing.waitFor("OPEN", 10_000), "the crashing publisher listens");
        var subscriber: support.Conn = null;
        const result = c.fipc_broadcast_join(name, lossy, &subscriber, 500);
        std.debug.print("  crash at {s}: the join returned {s}\n", .{ step, support.resultName(result) });
        if (result == c.FIPC_OK) {
            var receive_buffer: [16]u8 = undefined;
            var len: usize = 0;
            _ = t.expectEqual(support.recv(subscriber, &receive_buffer, &len, 5000), c.FIPC_DISCONNECTED, "the subscriber reads the publisher's end");
            c.fipc_close(subscriber);
        } else _ = t.expectEqual(result, c.FIPC_TIMEOUT, "or the join times out");
        try t.check(crashing.waitTimeout(10_000) and crashing.exitedWith(99), "the publisher ended at its crash point");
        // A broadcast of this process takes the name, and a subscriber joins it
        var broadcast: support.Broadcast = .{};
        try t.check(broadcast.open(name, 4096, 1, c.FIPC_FOREVER), "the next broadcast on the name");
        defer broadcast.close();
        const next = try joined(&t, name, lossy);
        c.fipc_close(next);
    }
    return t.done();
}

test "slow: a subscriber crashed at each step of its join, lossy or gated: its slot is freed, the next gets it" {
    var t = try support.begin(@src(), 180);
    defer t.end();
    var buf: [64]u8 = undefined;
    var broadcast: support.Broadcast = .{};
    const name = try support.openBroadcast(&t, &broadcast, &buf, "bc_crash_sub", 4096, 1, c.FIPC_FOREVER);
    defer broadcast.close();
    const Case = struct { role: []const u8, step: []const u8 };
    var cases: [11]Case = undefined;
    for ([_][]const u8{ "connected", "offer_received", "join_sent", "welcomed", "watching" }, 0..) |step, i| {
        cases[i] = .{ .role = "join", .step = step };
    }
    for ([_][]const u8{ "connected", "offer_received", "join_sent", "welcomed", "start_taken", "watching" }, 5..) |step, i| {
        cases[i] = .{ .role = "join-gated", .step = step };
    }
    var seq: u64 = 0;
    for (cases) |case| {
        const step = case.step;
        const crashing = try support.Peer.spawn(&.{ "native-broadcast-crash", case.role, name, step });
        defer crashing.deinit();
        // The crashing subscriber's join goes as far as its crash point
        try t.check(crashing.waitTimeout(10_000) and crashing.exitedWith(99), "the subscriber ended at its crash point");
        // One slot: the next subscriber gets in only once the crashed one's is free
        const next = try support.Peer.spawn(&.{ "broadcast-hold", name, "lossy" });
        defer next.deinit();
        try t.check(next.waitFor("J", 10_000), "the next subscriber gets the slot, and joins");
        // Two laps of the 64 cells, none waited for: a crashed gated subscriber left nothing that holds the writer
        for (0..128) |_| {
            try t.checkEqual(sendStamp(broadcast.publisher, seq, 48, 0), c.FIPC_OK, "the writer sends");
            seq += 1;
        }
        next.kill();
    }
    return t.done();
}
