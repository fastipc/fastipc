//! The broadcast tests' shared helpers: a subscriber joined, checked messages (`support.Stamp`) sent and received,
//! whole or in place, a subscriber's receives on a thread of its own, its sleeps, a subscriber's native join on a
//! thread, the publisher's count of subscribers, and the wait for subscriber processes.

const std = @import("std");
const fastipc = @import("fastipc");
const support = @import("../support.zig");
const c = support.c;

pub const lossy = c.FIPC_SUBSCRIBER_LOSSY;
pub const gated = c.FIPC_SUBSCRIBER_GATED;

/// `recvStamp`'s result for a message that isn't whole (no result code is this).
pub const torn: c.fipc_result_t = 100;

/// A subscriber joined to the broadcast on `name` as `role`, or the test ends.
pub fn joined(t: *support.Test, name: [*:0]const u8, role: c_int) !support.Conn {
    var subscriber: support.Conn = null;
    try t.check(support.joinNow(name, role, &subscriber, 5000), "a subscriber joins");
    return subscriber;
}

/// Sends message `seq` of `len` bytes (`support.Stamp`) on the writer.
pub fn sendStamp(writer: support.Conn, seq: u64, len: usize, timeout_ms: c_int) c.fipc_result_t {
    var buf: [4096]u8 = undefined;
    support.Stamp.fill(buf[0..len], seq);
    return support.send(writer, buf[0..len], timeout_ms);
}

/// Receives the next message, which must be whole: its sequence number; null on any other result (in `result`).
pub fn recvStamp(subscriber: support.Conn, timeout_ms: c_int, result: *c.fipc_result_t) ?u64 {
    var buf: [65536]u8 = undefined;
    var len: usize = 0;
    result.* = support.recv(subscriber, &buf, &len, timeout_ms);
    if (result.* != c.FIPC_OK) return null;
    return support.Stamp.check(buf[0..len]) orelse {
        result.* = torn;
        return null;
    };
}

/// `recvStamp` in place (a gated subscriber's `fipc_recv_acquire`), the message released once checked.
pub fn recvStampInPlace(subscriber: support.Conn, timeout_ms: c_int, result: *c.fipc_result_t) ?u64 {
    var at: ?*const anyopaque = null;
    var len: usize = 0;
    result.* = c.fipc_recv_acquire(subscriber, &at, &len, timeout_ms);
    if (result.* != c.FIPC_OK) return null;
    defer c.fipc_recv_release(subscriber);
    return support.Stamp.check(@as([*]const u8, @ptrCast(at.?))[0..len]) orelse {
        result.* = torn;
        return null;
    };
}

pub const Receiver = struct {
    subscriber: support.Conn,
    /// A gated subscriber's receives in place.
    in_place: bool = false,
    thread: std.Thread = undefined,
    tally: support.Stamp.Tally = .{},
    end: c.fipc_result_t = 0,

    /// Receives until the broadcast ends (`support.Stamp.Tally` checks the stream).
    pub fn run(receiver: *Receiver) void {
        while (true) {
            var result: c.fipc_result_t = 0;
            const seq = if (receiver.in_place) recvStampInPlace(receiver.subscriber, 5000, &result) else recvStamp(receiver.subscriber, 5000, &result);
            switch (result) {
                c.FIPC_OK, torn => receiver.tally.message(seq),
                c.FIPC_OVERRUN => receiver.tally.overrun(),
                else => {
                    receiver.end = result;
                    return;
                },
            }
        }
    }
};

/// How many sleeps the receives of `subscriber` have begun (a test hook), for `awaitAsleep`.
pub fn sleepsOf(subscriber: support.Conn) u32 {
    return fastipc.test_hooks.subscriberSleeps(subscriber.?);
}

/// Waits until a receive of `subscriber` has begun a new sleep since `sleepsOf` returned `since`. That receive has made
/// its last look, so only a wake-up ends the sleep before its timeout.
pub fn awaitAsleep(subscriber: support.Conn, since: u32) void {
    while (sleepsOf(subscriber) == since) std.Thread.yield() catch {};
}

pub fn recvForever(subscriber: support.Conn, result: *c.fipc_result_t) void {
    var buf: [64]u8 = undefined;
    var len: usize = 0;
    result.* = support.recv(subscriber, &buf, &len, -1);
}

/// A subscriber's `broadcastJoin` on a thread of its own, for a test that does something else while it joins.
pub const NativeJoiner = struct {
    name: []const u8,
    role: fastipc.SubscriberRole,
    thread: std.Thread = undefined,
    result: fastipc.Error!fastipc.Conn = error.Timeout,

    pub fn run(joiner: *NativeJoiner) void {
        joiner.result = fastipc.broadcastJoin(std.testing.allocator, joiner.name, joiner.role, support.within(5000));
    }
};

/// Whether the writer's count of joined subscribers becomes `expected` within `timeout_ms` (a subscriber's leave is
/// counted once the publisher's poller has seen its end).
pub fn countBecomes(writer: support.Conn, expected: u32, timeout_ms: u64) bool {
    const begun = support.nowMs();
    while (true) {
        var count: u32 = 0;
        if (c.fipc_broadcast_wait_for_members(writer, 0, &count, 0) != c.FIPC_OK) return false;
        if (count == expected) return true;
        if (support.nowMs() - begun > timeout_ms) {
            std.debug.print("  count: {d}, expected {d}\n", .{ count, expected });
            return false;
        }
        support.sleepMs(1);
    }
}

/// Waits until `count` subscribers are joined (processes that join), each within 10 s.
pub fn awaitSubscribers(t: *support.Test, broadcast: *support.Broadcast, count: usize) !void {
    var joined_count: u32 = 0;
    const result = c.fipc_broadcast_wait_for_members(broadcast.publisher, @intCast(count), &joined_count, @intCast(10_000 * count));
    try t.checkEqual(result, c.FIPC_OK, "the subscriber processes join");
}
