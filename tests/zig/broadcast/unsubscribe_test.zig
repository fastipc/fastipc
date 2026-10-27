//! The unsubscribe of a gated subscriber that stops receiving: after `max_stall_ms` the writer goes on (FIPC_FOREVER:
//! never), several at once, one that holds a message in place, its close and the writer's count around its answer, and
//! nothing left behind. A subscription that ends while its process runs on (a stray frame either way,
//! `fastipc.test_hooks.sendStray`): its slot's next holder is safe from it.

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

/// The cursor of slot `slot`, as the writer `writer` reads it.
fn slotCursor(writer: *anyopaque, slot: u32) u64 {
    return fastipc.test_hooks.channelLayout(writer).slots[slot].cursor.load(.monotonic);
}

/// A gated subscriber on a thread that receives `count` messages, then nothing until `go`, then tries once more.
const Staller = struct {
    subscriber: support.Conn,
    count: u64,
    go: std.atomic.Value(bool) = .init(false),
    thread: std.Thread = undefined,
    received: u64 = 0,
    after: c.fipc_result_t = c.FIPC_OK,

    fn run(staller: *Staller) void {
        var result: c.fipc_result_t = 0;
        while (staller.received < staller.count) : (staller.received += 1) {
            if ((recvStamp(staller.subscriber, 5000, &result) orelse break) != staller.received) break;
        }
        while (!staller.go.load(.acquire)) support.sleepMs(1);
        _ = recvStamp(staller.subscriber, 1000, &result);
        staller.after = result;
    }
};

test "fast: a gated subscriber that stops receiving is unsubscribed after max_stall_ms: the writer goes on" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    var broadcast: support.Broadcast = .{};
    const name = try support.openBroadcast(&t, &broadcast, &buf, "bc_unsub", 4096, 3, 300);
    defer broadcast.close();
    var stalled: Staller = .{ .subscriber = try joined(&t, name, gated), .count = 10 };
    const keeps = try joined(&t, name, gated);
    defer c.fipc_close(keeps);
    const idle = try joined(&t, name, lossy);
    defer c.fipc_close(idle);
    stalled.thread = try std.Thread.spawn(.{}, Staller.run, .{&stalled});
    var result: c.fipc_result_t = 0;
    // The 64 cells: the subscriber that stops after 10 messages holds the 75th back
    var seq: u64 = 0;
    while (seq < 74) : (seq += 1) {
        try t.checkEqual(sendStamp(broadcast.publisher, seq, 48, 5000), c.FIPC_OK, "send");
        try t.checkEqual(recvStamp(keeps, 1000, &result) orelse 999, seq, "the other gated subscriber keeps up");
    }
    const begun = support.nowMs();
    try t.checkEqual(sendStamp(broadcast.publisher, seq, 48, 5000), c.FIPC_OK, "the send that waited for it");
    const took = support.nowMs() - begun;
    _ = t.expect(took >= 290 and took < 3000, "about max_stall_ms");
    _ = t.expectEqual(recvStamp(keeps, 1000, &result) orelse 999, seq, "the other's");
    seq += 1;
    // Laps of the broadcast, waited for by nobody but the subscriber that keeps up
    while (seq < 400) : (seq += 1) {
        try t.checkEqual(sendStamp(broadcast.publisher, seq, 48, 0), c.FIPC_OK, "send");
        _ = t.expectEqual(recvStamp(keeps, 1000, &result) orelse 999, seq, "the other's");
    }
    stalled.go.store(true, .release);
    stalled.thread.join();
    _ = t.expectEqual(stalled.received, 10, "the unsubscribed subscriber received its 10");
    _ = t.expectEqual(stalled.after, c.FIPC_UNSUBSCRIBED, "then UNSUBSCRIBED");
    // Every later call on it: UNSUBSCRIBED where it would touch the broadcast; max_piece and cancel as before
    const subscriber = stalled.subscriber;
    var at: ?*const anyopaque = null;
    var len: usize = 0;
    _ = t.expectEqual(recvStamp(subscriber, 0, &result) orelse 999, 999, "recv");
    _ = t.expectEqual(result, c.FIPC_UNSUBSCRIBED, "recv: UNSUBSCRIBED");
    _ = t.expectEqual(c.fipc_recv_acquire(subscriber, &at, &len, 0), c.FIPC_UNSUBSCRIBED, "recv_acquire");
    c.fipc_recv_release(subscriber);
    _ = t.expectEqual(c.fipc_max_piece(subscriber), 1024, "max_piece");
    _ = t.expectEqual(support.send(subscriber, "x", 0), c.FIPC_INVALID, "a subscriber still sends nothing");
    c.fipc_cancel(subscriber);
    _ = t.expectEqual(c.fipc_recv_acquire(subscriber, &at, &len, -1), c.FIPC_UNSUBSCRIBED, "UNSUBSCRIBED before CANCELLED");
    c.fipc_close(subscriber);
    // A lossy subscriber holds nothing back and is never unsubscribed: lapped, it goes on
    _ = t.expect(recvStamp(idle, 0, &result) == null and result == c.FIPC_OVERRUN, "the idle lossy subscriber: OVERRUN");
    var last: u64 = 0;
    while (recvStamp(idle, 0, &result)) |stamp| last = stamp;
    _ = t.expectEqual(result, c.FIPC_TIMEOUT, "then messages, until there is nothing more");
    _ = t.expectEqual(last, seq - 1, "up to the last");
    // Its slot is free again
    const next = try joined(&t, name, gated);
    c.fipc_close(next);
    return t.done();
}

test "fast: a negative max_stall_ms (FIPC_FOREVER) never unsubscribes: the writer's send times out instead" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    for ([_]c_int{ c.FIPC_FOREVER, -2 }) |max_stall| {
        var buf: [64]u8 = undefined;
        var broadcast: support.Broadcast = .{};
        const name = try support.openBroadcast(&t, &broadcast, &buf, "bc_forever", 4096, 1, max_stall);
        defer broadcast.close();
        const subscriber = try joined(&t, name, gated);
        defer c.fipc_close(subscriber);
        for (0..64) |i| try t.checkEqual(sendStamp(broadcast.publisher, i, 48, 0), c.FIPC_OK, "send");
        const begun = support.nowMs();
        _ = t.expectEqual(sendStamp(broadcast.publisher, 64, 48, 300), c.FIPC_TIMEOUT, "the send times out");
        _ = t.expect(support.nowMs() - begun >= 295, "at its own timeout");
        var result: c.fipc_result_t = 0;
        _ = t.expectEqual(recvStamp(subscriber, 0, &result) orelse 999, 0, "the subscriber is still subscribed");
        _ = t.expectEqual(sendStamp(broadcast.publisher, 64, 48, 0), c.FIPC_OK, "and its receive made room");
    }
    return t.done();
}

test "fast: unsubscribed while it holds a message in place: release is nothing, receives are UNSUBSCRIBED" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    var broadcast: support.Broadcast = .{};
    const name = try support.openBroadcast(&t, &broadcast, &buf, "bc_unsub_held", 4096, 1, 50);
    defer broadcast.close();
    const subscriber = try joined(&t, name, gated);
    try t.checkEqual(sendStamp(broadcast.publisher, 0, 48, 0), c.FIPC_OK, "send");
    var at: ?*const anyopaque = null;
    var len: usize = 0;
    try t.checkEqual(c.fipc_recv_acquire(subscriber, &at, &len, 0), c.FIPC_OK, "recv_acquire, held");
    for (1..64) |i| try t.checkEqual(sendStamp(broadcast.publisher, i, 48, 0), c.FIPC_OK, "send");
    // Sends that don't wait add the stall up: the writer unsubscribes the subscriber once it reaches max_stall_ms
    const begun = support.nowMs();
    const through = while (support.nowMs() - begun < 5000) {
        const result = sendStamp(broadcast.publisher, 64, 48, 0);
        if (result == c.FIPC_OK) break true;
        if (result != c.FIPC_TIMEOUT) break false;
        support.sleepMs(5);
    } else false;
    _ = t.expect(through, "a send with a timeout of 0 goes through, once the subscriber is unsubscribed");
    _ = t.expect(support.nowMs() - begun >= 45, "after max_stall_ms");
    // The held pointer faults from here on (processes_test.zig checks that in another process); the library touches
    // nothing
    c.fipc_recv_release(subscriber);
    c.fipc_recv_release(subscriber);
    var receive_buffer: [64]u8 = undefined;
    _ = t.expectEqual(support.recv(subscriber, &receive_buffer, &len, 0), c.FIPC_UNSUBSCRIBED, "recv");
    _ = t.expectEqual(c.fipc_recv_acquire(subscriber, &at, &len, 100), c.FIPC_UNSUBSCRIBED, "recv_acquire");
    c.fipc_close(subscriber);
    for (65..300) |i| try t.checkEqual(sendStamp(broadcast.publisher, i, 48, 0), c.FIPC_OK, "the writer waits for nobody");
    return t.done();
}

test "fast: several gated subscribers held still are unsubscribed together, once max_stall_ms has passed" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    var broadcast: support.Broadcast = .{};
    const name = try support.openBroadcast(&t, &broadcast, &buf, "bc_unsub_many", 4096, 3, 300);
    defer broadcast.close();
    var subscribers: [3]support.Conn = undefined;
    for (&subscribers) |*subscriber| subscriber.* = try joined(&t, name, gated);
    for (0..64) |i| try t.checkEqual(sendStamp(broadcast.publisher, i, 48, 0), c.FIPC_OK, "send");
    const begun = support.nowMs();
    try t.checkEqual(sendStamp(broadcast.publisher, 64, 48, 5000), c.FIPC_OK, "the send that waited for all three");
    const took = support.nowMs() - begun;
    // One after the other would take three times max_stall_ms
    _ = t.expect(took >= 290 and took < 750, "about max_stall_ms, once");
    for (subscribers) |subscriber| {
        var len: usize = 0;
        _ = t.expectEqual(c.fipc_recv(subscriber, null, 0, &len, 0), c.FIPC_UNSUBSCRIBED, "each was unsubscribed");
        c.fipc_close(subscriber);
    }
    return t.done();
}

/// A subscriber of `role` joined to the broadcast on `name` through the native API: its watcher runs this module's
/// code, so this module's test hooks reach it (`hold_answer`; `sendStray` under the handle lock that its watcher takes),
/// which they don't through the C API (the static library has hooks and a handle lock of its own).
fn nativeJoined(name: []const u8, role: fastipc.SubscriberRole) !fastipc.Conn {
    return fastipc.broadcastJoin(std.testing.allocator, name, role, support.within(5000));
}

/// Sends a message with a timeout of 0, again and again, until `subscriber` sees that it was unsubscribed (a receive
/// into no buffer: it consumes nothing, so the stall goes on).
fn nativeStallUntilUnsubscribed(writer: fastipc.Conn, subscriber: fastipc.Conn) bool {
    const message: [48]u8 = @splat('m');
    var none: [0]u8 = .{};
    const begun = support.nowMs();
    while (support.nowMs() - begun < 5000) {
        writer.send(&message, support.within(0)) catch {};
        _ = subscriber.recv(&none, support.within(0)) catch |err| if (err == error.Unsubscribed) return true;
        support.sleepMs(2);
    }
    return false;
}

/// `countBecomes` through the native API.
fn nativeCountBecomes(writer: fastipc.Conn, expected: u32, timeout_ms: u64) bool {
    const begun = support.nowMs();
    while (true) {
        const count = writer.waitForMembers(0, .none) catch return false;
        if (count == expected) return true;
        if (support.nowMs() - begun > timeout_ms) {
            std.debug.print("  count: {d}, expected {d}\n", .{ count, expected });
            return false;
        }
        support.sleepMs(1);
    }
}

test "fast: a subscriber's close right after UNSUBSCRIBED doesn't wait for its answer to the publisher" {
    // The native API, for the hook `hold_answer` (`nativeJoined`)
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    const name = support.uniqueName(&buf, "bc_unsub_close");
    const publisher = try fastipc.broadcastCreate(std.testing.allocator, name, 4096, 1, 50);
    defer publisher.close();
    const subscriber = try nativeJoined(name, .gated);
    const message: [48]u8 = @splat('m');
    for (0..64) |_| try publisher.send(&message, support.within(0));
    // The subscriber's watcher holds its answer until the close below stops it: the close meets the answer's send
    fastipc.test_hooks.hold_answer.store(true, .monotonic);
    defer fastipc.test_hooks.hold_answer.store(false, .monotonic);
    try t.check(nativeStallUntilUnsubscribed(publisher, subscriber), "the subscriber is unsubscribed");
    const begun = support.nowMs();
    subscriber.close();
    _ = t.expect(support.nowMs() - begun < 1000, "the close returns at once");
    publisher.send(&message, support.within(2000)) catch |err| {
        std.debug.print("  send: {t}\n", .{err});
        _ = t.expect(false, "the subscriber's end freed its slot");
    };
    return t.done();
}

test "fast: an unsubscribed subscriber leaves the writer's count when UNSUBSCRIBE is sent, before its answer" {
    // The native API, for the hook `hold_answer` (`nativeJoined`)
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    const name = support.uniqueName(&buf, "bc_unsub_count");
    const publisher = try fastipc.broadcastCreate(std.testing.allocator, name, 4096, 2, 50);
    defer publisher.close();
    const held = try nativeJoined(name, .gated);
    const other = try nativeJoined(name, .lossy);
    defer other.close();
    _ = t.expectEqual(publisher.waitForMembers(2, support.within(0)) catch 0, 2, "two joined");
    const message: [48]u8 = @splat('m');
    for (0..64) |_| try publisher.send(&message, support.within(0));
    // The subscriber's watcher holds its answer until its close: only the UNSUBSCRIBE sent can uncount it
    fastipc.test_hooks.hold_answer.store(true, .monotonic);
    defer fastipc.test_hooks.hold_answer.store(false, .monotonic);
    try t.check(nativeStallUntilUnsubscribed(publisher, held), "the gated subscriber is unsubscribed");
    _ = t.expect(nativeCountBecomes(publisher, 1, 1000), "it isn't counted, though it hasn't answered");
    held.close();
    // A new subscriber gets the slot only once the poller has handled the end, which mustn't uncount it again
    const next = try nativeJoined(name, .lossy);
    defer next.close();
    _ = t.expectEqual(publisher.waitForMembers(0, .none) catch 0, 2, "the other and the next: the unsubscribed one uncounted once, not twice");
    return t.done();
}

/// Joins a gated subscriber, holds or receives message `seq`, lets the writer unsubscribe it, and closes it.
fn unsubscribeCycle(t: *support.Test, broadcast: *support.Broadcast, name: [*:0]const u8, seq: *u64, in_place: bool) !void {
    const subscriber = try joined(t, name, gated);
    try t.checkEqual(sendStamp(broadcast.publisher, seq.*, 48, 0), c.FIPC_OK, "send");
    seq.* += 1;
    var at: ?*const anyopaque = null;
    var len: usize = 0;
    var receive_buffer: [64]u8 = undefined;
    const first = if (in_place) c.fipc_recv_acquire(subscriber, &at, &len, 0) else support.recv(subscriber, &receive_buffer, &len, 0);
    try t.checkEqual(first, c.FIPC_OK, "the subscriber's message");
    while (sendStamp(broadcast.publisher, seq.*, 48, 0) == c.FIPC_OK) seq.* += 1;
    const begun = support.nowMs();
    const through = while (support.nowMs() - begun < 5000) {
        if (sendStamp(broadcast.publisher, seq.*, 48, 0) == c.FIPC_OK) break true;
        support.sleepMs(2);
    } else false;
    try t.check(through, "the writer goes on once the subscriber is unsubscribed");
    seq.* += 1;
    try t.checkEqual(support.recv(subscriber, &receive_buffer, &len, 0), c.FIPC_UNSUBSCRIBED, "UNSUBSCRIBED");
    c.fipc_recv_release(subscriber);
    c.fipc_close(subscriber);
}

test "fast: unsubscribed subscribers leave nothing behind: handles and mappings are back after their close" {
    var t = try support.begin(@src(), 60);
    defer t.end();
    var buf: [64]u8 = undefined;
    var broadcast: support.Broadcast = .{};
    const name = try support.openBroadcast(&t, &broadcast, &buf, "bc_unsub_leak", 4096, 1, 20);
    defer broadcast.close();
    var seq: u64 = 0;
    // A first cycle before counting: what the process creates once and keeps for every later subscriber isn't counted
    try unsubscribeCycle(&t, &broadcast, name, &seq, true);
    const platform = fastipc.internal.platform;
    const handles = support.handleCount();
    const mappings = platform.segmentMappingCount();
    for (0..10) |i| try unsubscribeCycle(&t, &broadcast, name, &seq, i % 2 == 0);
    _ = t.expect(support.handlesBack(handles), "the descriptors or handles are back");
    _ = t.expectEqual(platform.segmentMappingCount(), mappings, "the mappings are back");
    return t.done();
}

test "fast: a gated subscriber whose own watcher ends its subscription writes its slot no more: the slot's next subscriber is held to" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    var broadcast: support.Broadcast = .{};
    const name = try support.openBroadcast(&t, &broadcast, &buf, "bc_stray_sub", 4096, 1, c.FIPC_FOREVER);
    defer broadcast.close();
    const old = try joined(&t, name, gated);
    for (0..10) |i| try t.checkEqual(sendStamp(broadcast.publisher, i, 48, 0), c.FIPC_OK, "send");
    var result: c.fipc_result_t = 0;
    for (0..9) |i| _ = t.expectEqual(recvStamp(old, 0, &result) orelse 999, i, "the subscriber receives");
    var in: ?*const anyopaque = null;
    var len: usize = 0;
    try t.checkEqual(c.fipc_recv_acquire(old, &in, &len, 0), c.FIPC_OK, "and holds message 9 in place");
    // The publisher sends it a frame that doesn't belong: its watcher ends the subscription and closes its end, while
    // its process runs on; the publisher reads that end and frees the slot, and the next joiner gets it (one slot)
    fastipc.test_hooks.sendStray(broadcast.publisher.?);
    const next = try joined(&t, name, gated);
    defer c.fipc_close(next);
    var seq: u64 = 10;
    while (sendStamp(broadcast.publisher, seq, 48, 0) == c.FIPC_OK) seq += 1;
    _ = t.expectEqual(seq, 10 + 64, "the writer fills the cells the slot's next subscriber leaves it");
    // The next subscriber moves on, so its cursor (13) differs from what the old one's release would store (10)
    for (10..13) |i| _ = t.expectEqual(recvStamp(next, 0, &result) orelse 999, i, "the next subscriber receives");
    while (sendStamp(broadcast.publisher, seq, 48, 0) == c.FIPC_OK) seq += 1;
    _ = t.expectEqual(seq, 13 + 64, "the writer fills the cells it left");
    // The old subscriber's calls touch nothing of the slot: its release, its receives (DISCONNECTED, in place too,
    // though messages are there) and its close
    c.fipc_recv_release(old);
    _ = t.expectEqual(slotCursor(broadcast.publisher.?, 0), 13, "the old subscriber's release stores no cursor");
    _ = t.expectEqual(c.fipc_recv_acquire(old, &in, &len, 0), c.FIPC_DISCONNECTED, "in place: DISCONNECTED");
    _ = recvStamp(old, 0, &result);
    _ = t.expectEqual(result, c.FIPC_DISCONNECTED, "a copy: DISCONNECTED, what came after its end isn't its");
    _ = t.expectEqual(c.fipc_recv_acquire(old, &in, &len, 0), c.FIPC_DISCONNECTED, "again");
    c.fipc_close(old);
    _ = t.expectEqual(sendStamp(broadcast.publisher, seq, 48, 0), c.FIPC_TIMEOUT, "the slot's next subscriber still holds the writer back");
    _ = t.expectEqual(slotCursor(broadcast.publisher.?, 0), 13, "nor do its receives and its close");
    for (13..seq) |i| _ = t.expectEqual(recvStamp(next, 0, &result) orelse 999, i, "and receives every message whole");
    return t.done();
}

test "fast: a gated subscriber the publisher drops for a frame that doesn't belong keeps its slot out of use while the broadcast lasts" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    var broadcast: support.Broadcast = .{};
    const name = try support.openBroadcast(&t, &broadcast, &buf, "bc_stray_pub", 4096, 2, c.FIPC_FOREVER);
    defer broadcast.close();
    // The native API (`nativeJoined`): the hook that sends the stray frame and the subscriber's watcher, which closes
    // the connection it is sent on, are then the same copy of the library, with the same handle lock
    const dropped = try nativeJoined(name, .gated);
    try t.checkEqual(sendStamp(broadcast.publisher, 0, 48, 0), c.FIPC_OK, "send");
    try t.check(if (dropped.recvAcquire(support.within(0))) |_| true else |_| false, "the subscriber holds message 0 in place");
    // Its process sends the publisher a frame that doesn't belong: the publisher drops it and closes its end, not
    // knowing whether it still runs; its receive sees the end
    fastipc.test_hooks.sendStray(dropped.endpoint);
    _ = t.expect(if (dropped.recvAcquire(support.within(5000))) |_| false else |err| err == error.Disconnected, "the dropped subscriber's end");
    // The writer no longer waits for it: two laps go through at once
    for (1..129) |i| try t.checkEqual(sendStamp(broadcast.publisher, i, 48, 0), c.FIPC_OK, "the writer sends");
    // Its slot stays out of use: the next joiner gets the other, and a third finds the broadcast full, before and after
    // the dropped subscriber's close
    const next = try joined(&t, name, gated);
    defer c.fipc_close(next);
    var third: support.Conn = null;
    _ = t.expectEqual(c.fipc_broadcast_join(name, lossy, &third, 300), c.FIPC_TIMEOUT, "a third joiner: no slot is free");
    dropped.close();
    _ = t.expectEqual(c.fipc_broadcast_join(name, lossy, &third, 300), c.FIPC_TIMEOUT, "nor after the dropped subscriber's close");
    // The next subscriber holds the writer back as its own
    var seq: u64 = 129;
    while (sendStamp(broadcast.publisher, seq, 48, 0) == c.FIPC_OK) seq += 1;
    _ = t.expectEqual(seq, 129 + 64, "the next subscriber holds the writer back");
    var result: c.fipc_result_t = 0;
    for (129..seq) |i| _ = t.expectEqual(recvStamp(next, 0, &result) orelse 999, i, "and receives every message whole");
    return t.done();
}
