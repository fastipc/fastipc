//! The work queue tests' shared helpers: a queue's owner (its handle, and a member it joined), a member joined, checked
//! messages (`support.Stamp`) whose sequence number names their producer, and a ledger of what the consumers received:
//! each message once, each producer's in order on each consumer.

const std = @import("std");
const fastipc = @import("fastipc");
const support = @import("../support.zig");
const c = support.c;

pub const producer = c.FIPC_PRODUCER;
pub const consumer = c.FIPC_CONSUMER;

/// A work queue's owner in this process: its handle, and with a role the member the owner joined on this thread.
pub const Queue = struct {
    handle: support.Queue = null,
    own: support.Conn = null,

    /// `fipc_queue_create(name, ...)`, then with `role` (not 0) `fipc_queue_join(name, role)`; false (nothing left
    /// open) on a failure.
    pub fn open(queue: *Queue, name: [*:0]const u8, capacity: usize, max_producers: u32, max_consumers: u32, role: c_int) bool {
        queue.* = .{};
        const result = c.fipc_queue_create(name, capacity, max_producers, max_consumers, &queue.handle);
        if (result != c.FIPC_OK) {
            std.debug.print("  queue_create on '{s}': {s}\n", .{ name, support.resultName(result) });
            return false;
        }
        if (role == 0) return true;
        const joined_own = c.fipc_queue_join(name, role, &queue.own, 5000);
        if (joined_own == c.FIPC_OK) return true;
        std.debug.print("  the owner's queue_join on '{s}': {s}\n", .{ name, support.resultName(joined_own) });
        queue.close();
        return false;
    }

    /// Closes what is open: the owner's member, then the queue (it ends).
    pub fn close(queue: *Queue) void {
        c.fipc_close(queue.own);
        c.fipc_queue_close(queue.handle);
        queue.* = .{};
    }
};

/// `Queue.open` on the per-process name "<prefix>_<pid>" (into `name_buf`); ends the test if it fails.
pub fn openQueue(t: *support.Test, queue: *Queue, name_buf: []u8, comptime prefix: []const u8, capacity: usize, max_producers: u32, max_consumers: u32, role: c_int) ![:0]const u8 {
    const name = support.uniqueName(name_buf, prefix);
    try t.check(queue.open(name, capacity, max_producers, max_consumers, role), "a work queue");
    return name;
}

/// A member of `role` joined to the queue on `name`, or the test ends.
pub fn joined(t: *support.Test, name: [*:0]const u8, role: c_int) !support.Conn {
    var member: support.Conn = null;
    try t.check(support.joinNow(name, role, &member, 5000), "a member joins");
    return member;
}

/// Message `index` of producer `producer_index`'s stream: its stamp's sequence number.
pub fn seqOf(producer_index: u64, index: u64) u64 {
    return producer_index << 32 | index;
}

/// Sends message `seq` of `len` bytes (`support.Stamp`).
pub fn sendStamp(conn: support.Conn, seq: u64, len: usize, timeout_ms: c_int) c.fipc_result_t {
    var buf: [4096]u8 = undefined;
    support.Stamp.fill(buf[0..len], seq);
    return support.send(conn, buf[0..len], timeout_ms);
}

/// Receives the next message, copied or in place (released once checked): its sequence number; null on any other
/// result (in `result`), or for a message that isn't whole (`result` OK).
pub fn recvStamp(conn: support.Conn, in_place: bool, timeout_ms: c_int, result: *c.fipc_result_t) ?u64 {
    var buf: [4096]u8 = undefined;
    var len: usize = 0;
    if (in_place) {
        var at: ?*const anyopaque = null;
        result.* = c.fipc_recv_acquire(conn, &at, &len, timeout_ms);
        if (result.* != c.FIPC_OK) return null;
        defer c.fipc_recv_release(conn);
        return support.Stamp.check(@as([*]const u8, @ptrCast(at.?))[0..len]);
    }
    result.* = support.recv(conn, &buf, &len, timeout_ms);
    if (result.* != c.FIPC_OK) return null;
    return support.Stamp.check(buf[0..len]);
}

/// What the consumers of a test received, of `producers` streams of `count` messages each: each message at most once
/// (`duplicates`), whole (`bad`), and each producer's in order on each consumer (`out_of_order`).
pub const Ledger = struct {
    producers: usize,
    count: usize,
    seen: []std.atomic.Value(u8),
    received: std.atomic.Value(u64) = .init(0),
    duplicates: std.atomic.Value(u32) = .init(0),
    bad: std.atomic.Value(u32) = .init(0),
    out_of_order: std.atomic.Value(u32) = .init(0),

    pub fn init(producers: usize, count: usize) !Ledger {
        const seen = try std.testing.allocator.alloc(std.atomic.Value(u8), producers * count);
        @memset(seen, .init(0));
        return .{ .producers = producers, .count = count, .seen = seen };
    }

    pub fn deinit(ledger: *Ledger) void {
        std.testing.allocator.free(ledger.seen);
    }

    /// A message one consumer received (null: not whole), with that consumer's last index of each producer.
    pub fn record(ledger: *Ledger, last: *[64]?u64, seq: ?u64) void {
        const number = seq orelse {
            _ = ledger.bad.fetchAdd(1, .monotonic);
            return;
        };
        const producer_index = number >> 32;
        const index = number & 0xffff_ffff;
        if (producer_index >= ledger.producers or index >= ledger.count) {
            _ = ledger.bad.fetchAdd(1, .monotonic);
            return;
        }
        if (last[producer_index]) |previous| {
            if (index <= previous) _ = ledger.out_of_order.fetchAdd(1, .monotonic);
        }
        last[producer_index] = index;
        if (ledger.seen[producer_index * ledger.count + index].swap(1, .monotonic) != 0) _ = ledger.duplicates.fetchAdd(1, .monotonic);
        _ = ledger.received.fetchAdd(1, .monotonic);
    }

    /// Every message received once, whole, in order.
    pub fn complete(ledger: *const Ledger) bool {
        for (ledger.seen) |*flag| if (flag.load(.monotonic) == 0) return false;
        return ledger.duplicates.load(.monotonic) == 0 and ledger.bad.load(.monotonic) == 0 and ledger.out_of_order.load(.monotonic) == 0;
    }

    pub fn report(ledger: *const Ledger) void {
        var missing: usize = 0;
        for (ledger.seen) |*flag| missing += @intFromBool(flag.load(.monotonic) == 0);
        std.debug.print("  ledger: {d} received, {d} missing, {d} duplicates, {d} bad, {d} out of order\n", .{
            ledger.received.load(.monotonic), missing, ledger.duplicates.load(.monotonic), ledger.bad.load(.monotonic), ledger.out_of_order.load(.monotonic),
        });
    }
};

/// A consumer receiving on a thread of its own until a receive returns anything but OK (`end`), into a `Ledger`.
pub const Receiver = struct {
    conn: support.Conn,
    ledger: *Ledger,
    in_place: bool = false,
    timeout_ms: c_int = 5000,
    thread: std.Thread = undefined,
    end: c.fipc_result_t = 0,
    last: [64]?u64 = @splat(null),

    pub fn start(receiver: *Receiver) !void {
        receiver.thread = try std.Thread.spawn(.{}, run, .{receiver});
    }

    fn run(receiver: *Receiver) void {
        while (true) {
            var result: c.fipc_result_t = 0;
            const seq = recvStamp(receiver.conn, receiver.in_place, receiver.timeout_ms, &result);
            if (result != c.FIPC_OK) {
                receiver.end = result;
                return;
            }
            receiver.ledger.record(&receiver.last, seq);
        }
    }

    pub fn join(receiver: *Receiver) c.fipc_result_t {
        receiver.thread.join();
        return receiver.end;
    }
};

/// Sends producer `producer_index`'s stream of `count` checked messages (up to `max_len` bytes) on a thread of its
/// own; `result`: the first send that failed, else OK.
pub const Sender = struct {
    conn: support.Conn,
    producer_index: u64,
    count: usize,
    max_len: usize = 256,
    thread: std.Thread = undefined,
    result: c.fipc_result_t = c.FIPC_OK,

    pub fn start(sender: *Sender) !void {
        sender.thread = try std.Thread.spawn(.{}, run, .{sender});
    }

    fn run(sender: *Sender) void {
        for (0..sender.count) |i| {
            const result = sendStamp(sender.conn, seqOf(sender.producer_index, i), support.Stamp.sizeOf(i, sender.max_len), 5000);
            if (result != c.FIPC_OK) {
                sender.result = result;
                return;
            }
        }
    }

    pub fn join(sender: *Sender) c.fipc_result_t {
        sender.thread.join();
        return sender.result;
    }
};

/// The parks a consumer's receives began (or a producer's sleeps for room), for `awaitAsleep`.
pub fn sleepsOf(member: support.Conn) u32 {
    return fastipc.test_hooks.queueSleeps(member.?);
}

/// Until a wait of `member` has begun a sleep since `sleepsOf(member)` was `since`.
pub fn awaitAsleep(member: support.Conn, since: u32) void {
    while (sleepsOf(member) == since) std.Thread.yield() catch {};
}

/// The queue's segment, as `member` maps it.
pub fn layoutOf(member: support.Conn) *const fastipc.test_hooks.QueueLayout {
    return fastipc.test_hooks.queueLayout(member.?);
}
