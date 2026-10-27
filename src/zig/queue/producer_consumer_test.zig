//! The work queue's data path on a queue segment in this process, without the lifecycle: the owner's segment (real OS
//! objects: the mapping, and on Windows each slot's event), and producers and consumers on endpoints of their own,
//! each with its own view of the segment as a member attaches it, which tests drive from threads. Tests that play a
//! corrupt or ended peer write through the owner's view. The queue's end is what each member's watcher would publish:
//! the mark, the member's phase PEER_DEAD, then its local interrupt.
//!
//! The stress's messages carry their producer, their sequence number and a checksum (`Message`): the consumers' union
//! must be exactly the set sent, each message once, and each consumer must see each producer's messages in order.

const std = @import("std");
const builtin = @import("builtin");
const testing = std.testing;
const wire_protocol = @import("../wire_protocol.zig");
const platform = @import("../platform.zig");
const consumer_module = @import("consumer.zig");
const producer_module = @import("producer.zig");
const returns_module = @import("returns.zig");
const frames = @import("frames.zig");
const ring_set = @import("../core/ring_set.zig");
const segment = @import("segment.zig");
const taker_module = @import("../core/taker.zig");

const Atomic = std.atomic.Value;
const Endpoint = @import("../core/endpoint.zig").Endpoint;
const Hot = @import("../core/endpoint.zig").Hot;
const Session = @import("../core/shared_memory.zig").Session;
const DataError = @import("../core/endpoint.zig").DataError;

// === The harness ===

fn newEndpoint() !*Endpoint {
    const endpoint = try testing.allocator.create(Endpoint);
    endpoint.* = .{ .hot = std.mem.zeroes(Hot), .io = testing.io, .gpa = testing.allocator, .io_ref = undefined, .handles = undefined };
    endpoint.hot.phase.store(.connected, .monotonic);
    return endpoint;
}

/// A queue as its owner sets it up; producers and consumers come from `producer` and `consumer`.
pub const Queue = struct {
    security: platform.Security,
    handle: ?platform.Handle,
    session: Session,
    layout: segment.Layout,
    /// Each slot's event (Windows), as the owner creates it at the slot's first claim and keeps it.
    producer_events: [wire_protocol.max_queue_producers]?platform.SlotWake = @splat(null),
    consumer_events: [wire_protocol.max_queue_consumers]?platform.SlotWake = @splat(null),
    /// The owner's wakes, for `rescan`.
    wakes: segment.Wakes,

    pub fn create(capacity: u32, max_producers: u32, max_consumers: u32) !*Queue {
        var security = try platform.Security.init();
        errdefer security.deinit();
        const handle: ?platform.Handle = if (platform.passes_segment_handle) try platform.createSegmentHandle() else null;
        errdefer if (handle) |segment_handle| platform.close(segment_handle);
        var session = try segment.create(testing.io, handle, &security, capacity, max_producers, max_consumers);
        errdefer session.unmap();
        const queue = try testing.allocator.create(Queue);
        queue.* = .{
            .security = security,
            .handle = handle,
            .session = session,
            .layout = .of(&session, max_producers, max_consumers),
            .wakes = .init(session.id),
        };
        return queue;
    }

    pub fn destroy(queue: *Queue) void {
        queue.wakes.deinit();
        if (platform.wakes_each_slot) {
            for (queue.producer_events) |event| if (event) |handle| platform.closeSlotWake(handle);
            for (queue.consumer_events) |event| if (event) |handle| platform.closeSlotWake(handle);
        }
        queue.session.unmap();
        if (queue.handle) |segment_handle| platform.close(segment_handle);
        queue.security.deinit();
        testing.allocator.destroy(queue);
    }

    fn offer(queue: *const Queue) frames.Offer {
        return .{
            .capacity = queue.session.capacity,
            .segment_size = queue.session.mapping.size,
            .session_id = queue.session.id,
            .max_producers = queue.layout.max_senders,
            .max_consumers = queue.layout.max_takers,
        };
    }

    /// The member's own slot's event: created by the owner at the slot's first claim, opened by the member.
    fn ownWake(queue: *Queue, events: []?platform.SlotWake, suffix: []const u8, slot: u32) !platform.SlotWake {
        if (!platform.wakes_each_slot) return {};
        if (events[slot] == null) events[slot] = try platform.createSlotWake(&queue.session.id, suffix, slot, &queue.security);
        return platform.openSlotWake(&queue.session.id, suffix, slot);
    }

    /// The producer of slot `slot`, joined now.
    pub fn producer(queue: *Queue, slot: u32) !*ProducerEnd {
        const end = try testing.allocator.create(ProducerEnd);
        errdefer testing.allocator.destroy(end);
        end.own_wake = try queue.ownWake(&queue.producer_events, segment.producer_wake_suffix, slot);
        errdefer if (platform.wakes_each_slot) platform.closeSlotWake(end.own_wake);
        queue.layout.ring(slot).room_sleeps.store(0, .monotonic);
        const offered = queue.offer();
        end.session = try segment.attach(queue.handle, &offered);
        errdefer end.session.unmap();
        end.positions = try testing.allocator.alloc(u32, queue.layout.cellCount());
        errdefer testing.allocator.free(end.positions);
        end.endpoint = try newEndpoint();
        errdefer testing.allocator.destroy(end.endpoint);
        const view: segment.Layout = .of(&end.session, queue.layout.max_senders, queue.layout.max_takers);
        end.producer = try .init(view, slot, end.positions, platform.slotRingEvent(end.own_wake), queue.session.id);
        return end;
    }

    /// The consumer of slot `slot`, joined now.
    pub fn consumer(queue: *Queue, slot: u32) !*ConsumerEnd {
        const end = try testing.allocator.create(ConsumerEnd);
        errdefer testing.allocator.destroy(end);
        end.own_wake = try queue.ownWake(&queue.consumer_events, segment.consumer_wake_suffix, slot);
        errdefer if (platform.wakes_each_slot) platform.closeSlotWake(end.own_wake);
        const words = queue.layout.takerSlot(slot);
        words.sleeps.store(0, .monotonic);
        words.claim.store(0, .monotonic);
        segment.Rings.clearIdle(&queue.layout, slot);
        const offered = queue.offer();
        end.session = try segment.attach(queue.handle, &offered);
        errdefer end.session.unmap();
        end.endpoint = try newEndpoint();
        const view: segment.Layout = .of(&end.session, queue.layout.max_senders, queue.layout.max_takers);
        end.consumer = .init(view, slot, platform.slotRingEvent(end.own_wake), queue.session.id);
        return end;
    }

    /// The queue's end, as the owner's close (server.zig `close`) marks it: every member's watcher then reads its
    /// connection's end.
    pub fn markEnded(queue: *Queue) void {
        queue.layout.header.ended.store(1, .seq_cst);
    }

    pub fn readyBits(queue: *const Queue) u64 {
        return queue.layout.header.ready.load(.seq_cst);
    }
};

/// A producer: its endpoint, its own view of the segment, its payload record and its state.
pub const ProducerEnd = struct {
    endpoint: *Endpoint,
    session: Session,
    positions: []u32,
    own_wake: platform.SlotWake,
    producer: producer_module.Producer,

    pub fn destroy(end: *ProducerEnd) void {
        end.producer.deinit();
        if (platform.wakes_each_slot) platform.closeSlotWake(end.own_wake);
        testing.allocator.destroy(end.endpoint);
        testing.allocator.free(end.positions);
        end.session.unmap();
        testing.allocator.destroy(end);
    }

    pub fn send(end: *ProducerEnd, payload: []const u8, timeout_ms: c_int) DataError!void {
        return end.producer.send(end.endpoint, payload, timeout_ms);
    }

    /// What its watcher does at the end of its control connection: PEER_DEAD, then the local interrupt (the queue's
    /// end also has the mark, stored first).
    pub fn connectionEnded(end: *ProducerEnd) void {
        end.endpoint.hot.phase.store(.peer_dead, .seq_cst);
        end.producer.interrupt();
    }

    pub fn cancel(end: *ProducerEnd) void {
        _ = end.endpoint.hot.requests.fetchOr(.{ .cancel = true }, .seq_cst);
        end.producer.interrupt();
    }
};

/// A consumer: its endpoint, its own view of the segment and its state.
pub const ConsumerEnd = struct {
    endpoint: *Endpoint,
    session: Session,
    own_wake: platform.SlotWake,
    consumer: consumer_module.Consumer,

    /// Its close: a held message is released first.
    pub fn destroy(end: *ConsumerEnd) void {
        end.consumer.release();
        end.destroyHolding();
    }

    /// A consumer whose process ended: nothing released.
    fn destroyHolding(end: *ConsumerEnd) void {
        end.consumer.deinit();
        if (platform.wakes_each_slot) platform.closeSlotWake(end.own_wake);
        testing.allocator.destroy(end.endpoint);
        end.session.unmap();
        testing.allocator.destroy(end);
    }

    pub fn recv(end: *ConsumerEnd, buf: []u8, timeout_ms: c_int) DataError![]u8 {
        var len: usize = undefined;
        try end.consumer.recv(end.endpoint, buf, timeout_ms, &len);
        return buf[0..len];
    }

    pub fn recvAcquire(end: *ConsumerEnd, timeout_ms: c_int) DataError![]const u8 {
        return end.consumer.recvAcquire(end.endpoint, timeout_ms);
    }

    pub fn connectionEnded(end: *ConsumerEnd) void {
        end.endpoint.hot.phase.store(.peer_dead, .seq_cst);
        end.consumer.taker.interrupt();
    }

    pub fn cancel(end: *ConsumerEnd) void {
        _ = end.endpoint.hot.requests.fetchOr(.{ .cancel = true }, .seq_cst);
        end.consumer.taker.interrupt();
    }
};

/// A stress message: its sequence number (8 bytes), its producer (4), its length (4), a checksum of the rest (4), 4
/// bytes of 0, then bytes made from the sequence number.
const Message = struct {
    const min = 24;
    const max = 16 * 1024;

    fn fill(buf: []u8, producer: u32, seq: u64) void {
        std.debug.assert(buf.len >= min);
        std.mem.writeInt(u64, buf[0..8], seq, .little);
        std.mem.writeInt(u32, buf[8..12], producer, .little);
        std.mem.writeInt(u32, buf[12..16], @intCast(buf.len), .little);
        std.mem.writeInt(u32, buf[20..24], 0, .little);
        var state = (seq ^ @as(u64, producer) << 48) *% 0x9e3779b97f4a7c15 +% 1;
        for (buf[min..]) |*byte| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            byte.* = @truncate(state);
        }
        std.mem.writeInt(u32, buf[16..20], sum(buf), .little);
    }

    fn sum(buf: []const u8) u32 {
        var hasher = std.hash.Wyhash.init(0x5eed);
        hasher.update(buf[0..16]);
        hasher.update(buf[20..]);
        return @truncate(hasher.final());
    }

    const Id = struct { producer: u32, seq: u64 };

    /// The message's producer and sequence number, if it is whole.
    fn check(buf: []const u8) ?Id {
        if (buf.len < min) return null;
        if (std.mem.readInt(u32, buf[12..16], .little) != buf.len) return null;
        if (std.mem.readInt(u32, buf[16..20], .little) != sum(buf)) return null;
        return .{ .producer = std.mem.readInt(u32, buf[8..12], .little), .seq = std.mem.readInt(u64, buf[0..8], .little) };
    }

    /// Message `seq`'s size, from 24 to `max_piece`, most of them small.
    fn sizeOf(seq: u64, max_piece: usize) usize {
        const bits = (seq *% 0x2545f4914f6cdd1d) >> 40;
        const top = @min(max_piece, max);
        if (bits % 8 == 0) return min + @as(usize, @intCast(bits / 8 % (top - min + 1)));
        return min + @as(usize, @intCast(bits % 113));
    }
};

fn nowMs() i64 {
    return std.Io.Clock.awake.now(testing.io).toMilliseconds();
}

/// Until a receive of `end` has begun a park since its count of parks was `since`: it is past every look, so only a
/// wake ends that park before its time.
fn awaitParked(end: *const ConsumerEnd, since: u32) void {
    while (end.consumer.taker.sleeps.load(.seq_cst) == since) std.Thread.yield() catch {};
}

/// Until a send of `end` has begun a sleep for room since its count was `since`.
fn awaitRoomSleep(end: *const ProducerEnd, since: u32) void {
    while (end.producer.sleeps.load(.seq_cst) == since) std.Thread.yield() catch {};
}

/// Parks and sleeps for room last a minute unless woken: a test of a wake then fails if it doesn't come.
fn longParks(end: *ConsumerEnd) void {
    end.consumer.taker.longest_sleep_ms = 60_000;
    end.consumer.taker.spin_iters = 0;
}

fn longRoomSleeps(end: *ProducerEnd) void {
    end.producer.first_slice_ms = 60_000;
    end.producer.longest_slice_ms = 60_000;
}

/// Byte `offset` of the single-thread tests' message `index`.
fn patternByte(index: u64, offset: usize) u8 {
    return @truncate(index *% 31 +% offset *% 7);
}

fn sendPattern(end: *ProducerEnd, index: u64, size: usize, timeout_ms: c_int) DataError!void {
    var buf: [4096]u8 = undefined;
    for (buf[0..size], 0..) |*byte, offset| byte.* = patternByte(index, offset);
    return end.send(buf[0..size], timeout_ms);
}

fn expectPattern(index: u64, size: usize, msg: []const u8) !void {
    try testing.expectEqual(size, msg.len);
    for (msg, 0..) |byte, offset| try testing.expectEqual(patternByte(index, offset), byte);
}

/// A thread's result: the first failure, as an error name.
const Failure = struct {
    first: Atomic(?[*:0]const u8) = .init(null),

    fn set(failure: *Failure, comptime what: [:0]const u8) void {
        _ = failure.first.cmpxchgStrong(null, what.ptr, .seq_cst, .seq_cst);
    }

    fn check(failure: *const Failure) !void {
        if (failure.first.load(.seq_cst)) |what| {
            std.debug.print("failure: {s}\n", .{what});
            return error.TestFailed;
        }
    }
};

// === One thread ===

test "one message at a time, 1 byte to max_piece, copied and in place, across many laps of cells and data" {
    const queue = try Queue.create(4096, 1, 1);
    defer queue.destroy();
    const producer = try queue.producer(0);
    defer producer.destroy();
    const consumer = try queue.consumer(0);
    defer consumer.destroy();
    const max_piece = queue.layout.maxPiece();
    try testing.expectEqual(@as(usize, 1024), max_piece);
    const ring = queue.layout.ring(0);
    const sizes = [_]usize{ 1, 2, 63, 64, 65, 127, 128, 500, 1000, 1023, 1024, 17, 3 };
    var buf: [1024]u8 = undefined;
    var index: u64 = 0;
    // 64 cells and 4 KiB of data: 1000 messages are over 15 laps of cells and many more of data
    while (index < 1000) : (index += 1) {
        const size = sizes[index % sizes.len];
        if (index % 3 == 0) {
            // Zero-copy: room for more than is committed
            const room = try producer.producer.sendAcquire(producer.endpoint, @min(size + 100, max_piece), 0);
            try testing.expectEqual(@as(usize, 0), @intFromPtr(room.ptr) % 64);
            for (room[0..size], 0..) |*byte, offset| byte.* = patternByte(index, offset);
            try producer.producer.sendCommit(producer.endpoint, size);
        } else try sendPattern(producer, index, size, 0);
        try testing.expectEqual(index + 1, ring.tail.load(.seq_cst));
        try testing.expectEqual(@as(u64, 1), queue.readyBits());
        if (index % 2 == 0) {
            const held = try consumer.recvAcquire(0);
            try testing.expectEqual(@as(usize, 0), @intFromPtr(held.ptr) % 64);
            try expectPattern(index, size, held);
            try testing.expectEqual(ring_set.claimed(index, 0), queue.layout.cell(0, index).state.load(.seq_cst));
            if (index % 4 == 0) consumer.consumer.release(); // else the next receive lets it go
        } else try expectPattern(index, size, try consumer.recv(&buf, 0));
        // The last message taken: head past it, the ready bit clear
        try testing.expectEqual(index + 1, ring.head.load(.seq_cst));
        try testing.expectEqual(@as(u64, 0), queue.readyBits());
        try testing.expectError(error.Timeout, consumer.recv(&buf, 0));
    }
    try testing.expectEqual(ring_set.done(index - 1, 0), queue.layout.cell(0, index - 1).state.load(.seq_cst));
}

test "each producer's messages are taken in the order it sent them, whichever ring a pass starts at" {
    const queue = try Queue.create(8192, 3, 2);
    defer queue.destroy();
    var producers: [3]*ProducerEnd = undefined;
    for (&producers, 0..) |*producer, producer_index| producer.* = try queue.producer(@intCast(producer_index));
    defer for (producers) |producer| producer.destroy();
    const consumer = try queue.consumer(1);
    defer consumer.destroy();
    // Each message: its producer, then its number
    for (0..20) |index| for (producers, 0..) |producer, producer_index| try producer.send(&.{ @intCast(producer_index), @intCast(index) }, 0);
    try testing.expectEqual(@as(u64, 0b111), queue.readyBits());
    var next: [3]u8 = @splat(0);
    var buf: [1024]u8 = undefined;
    for (0..60) |_| {
        const msg = try consumer.recv(&buf, 0);
        try testing.expectEqual(@as(usize, 2), msg.len);
        try testing.expectEqual(next[msg[0]], msg[1]);
        next[msg[0]] += 1;
    }
    try testing.expectEqual([3]u8{ 20, 20, 20 }, next);
    try testing.expectEqual(@as(u64, 0), queue.readyBits());
    try testing.expectError(error.Timeout, consumer.recv(&buf, 0));
}

test "a pass starts at the ring of its last claim while those are won at the first look, the next one after stay_limit, home after a contended one" {
    const queue = try Queue.create(4096, 4, 2);
    defer queue.destroy();
    const consumer = try queue.consumer(1);
    defer consumer.destroy();
    const taker = &consumer.consumer.taker;
    try testing.expectEqual(@as(u32, 2), taker.home); // slot 1 of 2 over 4 producers' rings
    try testing.expectEqual(@as(u32, 2), taker.next_ring);
    taker.first_look = true;
    taker.nextRing(0); // its start ring was empty: it moves to the ring it took from
    try testing.expectEqual(@as(u32, 0), taker.next_ring);
    for (1..taker_module.stay_limit) |_| {
        taker.nextRing(0);
        try testing.expectEqual(@as(u32, 0), taker.next_ring);
    }
    taker.nextRing(0); // stay_limit claims in a row: the next ring
    try testing.expectEqual(@as(u32, 1), taker.next_ring);
    taker.nextRing(1);
    taker.first_look = false;
    taker.nextRing(1); // another taker took from it too: home
    try testing.expectEqual(@as(u32, 2), taker.home);
    try testing.expectEqual(@as(u32, 2), taker.next_ring);
    // The return ring, the last, is followed by the first
    taker.first_look = true;
    for (0..taker_module.stay_limit + 1) |_| taker.nextRing(4);
    try testing.expectEqual(@as(u32, 0), taker.next_ring);
}

test "a producer's calls refuse an empty message, one over max_piece, and a commit without its reservation" {
    const queue = try Queue.create(8192, 1, 1);
    defer queue.destroy();
    const producer = try queue.producer(0);
    defer producer.destroy();
    const max_piece = queue.layout.maxPiece();
    var big: [2049]u8 = @splat(1);
    try testing.expectError(error.Invalid, producer.send(big[0..0], 0));
    try testing.expectError(error.TooLarge, producer.send(big[0 .. max_piece + 1], 0));
    try testing.expectError(error.TooLarge, producer.producer.sendAcquire(producer.endpoint, max_piece + 1, 0));
    try testing.expectError(error.Invalid, producer.producer.sendAcquire(producer.endpoint, 0, 0));
    try testing.expectError(error.Invalid, producer.producer.sendCommit(producer.endpoint, 1));
    _ = try producer.producer.sendAcquire(producer.endpoint, 100, 0);
    try testing.expectError(error.Invalid, producer.producer.sendCommit(producer.endpoint, 0));
    try testing.expectError(error.Invalid, producer.producer.sendCommit(producer.endpoint, 101));
    try producer.producer.sendCommit(producer.endpoint, 100); // the reservation stayed
    try testing.expectError(error.Invalid, producer.producer.sendCommit(producer.endpoint, 100));
    try testing.expectEqual(@as(u64, 1), queue.layout.ring(0).tail.load(.seq_cst));
}

test "a producer starts where its ring's last producer left it, and refuses a ring whose indices are a corrupt peer's" {
    const queue = try Queue.create(4096, 1, 1);
    defer queue.destroy();
    const ring = queue.layout.ring(0);
    ring.tail.store(1000, .seq_cst);
    ring.head.store(1000, .seq_cst);
    {
        const producer = try queue.producer(0);
        defer producer.destroy();
        const consumer = try queue.consumer(0);
        defer consumer.destroy();
        try sendPattern(producer, 1000, 50, 0);
        var buf: [64]u8 = undefined;
        try expectPattern(1000, 50, try consumer.recv(&buf, 0));
        try testing.expectEqual(@as(u64, 1001), ring.head.load(.seq_cst));
    }
    ring.head.store(1000, .seq_cst); // a message not taken
    try testing.expectError(error.Invalid, queue.producer(0));
    ring.tail.store(wire_protocol.queue_max_index, .seq_cst);
    ring.head.store(wire_protocol.queue_max_index, .seq_cst);
    try testing.expectError(error.Invalid, queue.producer(0));
}

// === Room ===

test "room: a message held stops the producer's reclaim at it, and out-of-order dones free only the run before it" {
    const queue = try Queue.create(4096, 1, 2);
    defer queue.destroy();
    const producer = try queue.producer(0);
    defer producer.destroy();
    const holder = try queue.consumer(0);
    defer holder.destroy();
    const other = try queue.consumer(1);
    defer other.destroy();
    // 64 cells: the 65th message waits
    for (0..64) |index| try sendPattern(producer, index, 32, 0);
    try testing.expectError(error.Timeout, sendPattern(producer, 64, 32, 0));
    try expectPattern(0, 32, try holder.recvAcquire(0));
    var buf: [64]u8 = undefined;
    for (1..64) |index| try expectPattern(index, 32, try other.recv(&buf, 0));
    // Every message but the first is done: no room until it is
    try testing.expectError(error.Timeout, sendPattern(producer, 64, 32, 0));
    holder.consumer.release();
    for (64..128) |index| try sendPattern(producer, index, 32, 0);
    try testing.expectError(error.Timeout, sendPattern(producer, 128, 32, 0));
}

test "room: reclaim stops at head, even at a cell done before head moved past it" {
    const queue = try Queue.create(4096, 1, 1);
    defer queue.destroy();
    const producer = try queue.producer(0);
    defer producer.destroy();
    for (0..64) |index| try sendPattern(producer, index, 32, 0);
    // A consumer between its claim and its move of head, done already: head stays at 0
    const cell = queue.layout.cell(0, 0);
    try testing.expectEqual(null, cell.state.cmpxchgStrong(ring_set.published(0), ring_set.done(0, 0), .seq_cst, .seq_cst));
    try testing.expectError(error.Timeout, sendPattern(producer, 64, 32, 0));
    queue.layout.ring(0).head.store(1, .seq_cst);
    try sendPattern(producer, 64, 32, 0);
}

test "room: payloads of a ring's data, and the skip to its start, wait for the messages whose room they need" {
    const queue = try Queue.create(8192, 1, 1);
    defer queue.destroy();
    const producer = try queue.producer(0);
    defer producer.destroy();
    const consumer = try queue.consumer(0);
    defer consumer.destroy();
    // Payloads at 0, 1024, 2048, 4096 and 6144 (to 7680); then one of 2000 bytes that doesn't fit before the ring's
    // end: it goes at the next lap's start, 8192, and its 2048 bytes need the room of the first two payloads
    const sizes = [_]usize{ 1024, 1024, 2048, 2048, 1536 };
    for (sizes, 0..) |size, index| try sendPattern(producer, index, size, 0);
    try testing.expectError(error.Timeout, sendPattern(producer, 5, 2000, 0));
    var buf: [2048]u8 = undefined;
    try expectPattern(0, sizes[0], try consumer.recv(&buf, 0));
    try testing.expectError(error.Timeout, sendPattern(producer, 5, 2000, 0));
    try expectPattern(1, sizes[1], try consumer.recv(&buf, 0));
    try sendPattern(producer, 5, 2000, 0);
    try testing.expectEqual(@as(u64, 8192), queue.layout.cell(0, 5).payload_position.load(.seq_cst));
    for (2..5) |index| try expectPattern(index, sizes[index], try consumer.recv(&buf, 0));
    const last = try consumer.recvAcquire(0);
    try expectPattern(5, 2000, last);
    try testing.expectEqual(@intFromPtr(queue.layout.bytes(0, 0, 1).ptr) - @intFromPtr(queue.session.mapping.base), @intFromPtr(last.ptr) - @intFromPtr(consumer.session.mapping.base));
}

test "room: a done wakes a producer asleep for room; a cancel or the queue's end ends its wait" {
    const queue = try Queue.create(4096, 1, 1);
    defer queue.destroy();
    const producer = try queue.producer(0);
    defer producer.destroy();
    const consumer = try queue.consumer(0);
    defer consumer.destroy();
    longRoomSleeps(producer);
    for (0..64) |index| try sendPattern(producer, index, 32, 0);
    const Sending = struct {
        fn run(end: *ProducerEnd, index: u64, result: *?DataError) void {
            result.* = null;
            sendPattern(end, index, 32, 30_000) catch |err| {
                result.* = err;
            };
        }
    };
    var result: ?DataError = null;
    var buf: [64]u8 = undefined;
    // A done
    var since = producer.producer.sleeps.load(.seq_cst);
    var thread = try std.Thread.spawn(.{}, Sending.run, .{ producer, 64, &result });
    awaitRoomSleep(producer, since);
    const start_ms = nowMs();
    try expectPattern(0, 32, try consumer.recv(&buf, 0));
    thread.join();
    try testing.expectEqual(@as(?DataError, null), result);
    try testing.expect(nowMs() - start_ms < 5000);
    // A cancel
    since = producer.producer.sleeps.load(.seq_cst);
    thread = try std.Thread.spawn(.{}, Sending.run, .{ producer, 65, &result });
    awaitRoomSleep(producer, since);
    producer.cancel();
    thread.join();
    try testing.expectEqual(@as(?DataError, error.Cancelled), result);
    producer.endpoint.hot.requests.store(.{}, .seq_cst);
    // The queue's end
    since = producer.producer.sleeps.load(.seq_cst);
    thread = try std.Thread.spawn(.{}, Sending.run, .{ producer, 65, &result });
    awaitRoomSleep(producer, since);
    queue.markEnded();
    producer.connectionEnded();
    thread.join();
    try testing.expectEqual(@as(?DataError, error.Disconnected), result);
}

// === Claims ===

test "a cell claimed at a head left behind is helped on, and the next message taken" {
    const queue = try Queue.create(4096, 1, 2);
    defer queue.destroy();
    const producer = try queue.producer(0);
    defer producer.destroy();
    const consumer = try queue.consumer(0);
    defer consumer.destroy();
    for (0..3) |index| try sendPattern(producer, index, 40, 0);
    // Consumer 1 claimed message 0 and stopped before its move of head
    try testing.expectEqual(null, queue.layout.cell(0, 0).state.cmpxchgStrong(ring_set.published(0), ring_set.claimed(0, 1), .seq_cst, .seq_cst));
    var buf: [64]u8 = undefined;
    try expectPattern(1, 40, try consumer.recv(&buf, 0));
    try testing.expectEqual(@as(u64, 2), queue.layout.ring(0).head.load(.seq_cst));
    try expectPattern(2, 40, try consumer.recv(&buf, 0));
    try testing.expectError(error.Timeout, consumer.recv(&buf, 0));
}

test "a corrupt cell at head is skipped (Invalid) and the next message taken; a corrupt payload is dropped" {
    const queue = try Queue.create(4096, 1, 1);
    defer queue.destroy();
    const producer = try queue.producer(0);
    defer producer.destroy();
    const consumer = try queue.consumer(0);
    defer consumer.destroy();
    for (0..4) |index| try sendPattern(producer, index, 40, 0);
    queue.layout.cell(0, 0).state.store(ring_set.published(77), .seq_cst);
    var buf: [1024]u8 = undefined;
    try testing.expectError(error.Invalid, consumer.recv(&buf, 0));
    try testing.expectEqual(@as(u64, 1), queue.layout.ring(0).head.load(.seq_cst));
    try expectPattern(1, 40, try consumer.recv(&buf, 0));
    // Payload fields out of range: claimed, done and dropped
    queue.layout.cell(0, 2).len.store(@intCast(queue.layout.maxPiece() + 1), .seq_cst);
    try testing.expectError(error.Invalid, consumer.recv(&buf, 0));
    try testing.expectEqual(ring_set.done(2, 0), queue.layout.cell(0, 2).state.load(.seq_cst));
    queue.layout.cell(0, 3).payload_position.store(4096 - 32, .seq_cst); // past the ring's end
    try testing.expectError(error.Invalid, consumer.recvAcquire(0));
    try testing.expectError(error.Timeout, consumer.recv(&buf, 0));
}

test "a ring with corrupt indices is skipped, its bit cleared, and the other rings served" {
    const queue = try Queue.create(4096, 2, 1);
    defer queue.destroy();
    const producer = try queue.producer(1);
    defer producer.destroy();
    const consumer = try queue.consumer(0);
    defer consumer.destroy();
    const corrupt = queue.layout.ring(0);
    corrupt.head.store(10, .seq_cst);
    corrupt.tail.store(10 + queue.layout.cellCount() + 1, .seq_cst);
    _ = queue.layout.header.ready.fetchOr(1, .seq_cst);
    try sendPattern(producer, 5, 40, 0);
    var buf: [64]u8 = undefined;
    try expectPattern(5, 40, try consumer.recv(&buf, 0));
    try testing.expectEqual(@as(u64, 0), queue.readyBits());
    _ = queue.layout.header.ready.fetchOr(1, .seq_cst);
    try testing.expectError(error.Timeout, consumer.recv(&buf, 0));
    try testing.expectEqual(@as(u64, 0), queue.readyBits());
    corrupt.tail.store(wire_protocol.queue_max_index, .seq_cst);
    corrupt.head.store(wire_protocol.queue_max_index - 1, .seq_cst);
    consumer.consumer.taker.full_scan = true;
    try testing.expectError(error.Timeout, consumer.recv(&buf, 0));
    try testing.expectEqual(@as(u64, 0), queue.layout.cell(0, wire_protocol.queue_max_index - 1).state.load(.seq_cst));
}

test "TooLarge takes nothing: the message stays for any consumer; a NULL buffer asks for its length" {
    const queue = try Queue.create(4096, 1, 2);
    defer queue.destroy();
    const producer = try queue.producer(0);
    defer producer.destroy();
    const small = try queue.consumer(0);
    defer small.destroy();
    const big = try queue.consumer(1);
    defer big.destroy();
    try sendPattern(producer, 9, 500, 0);
    var buf: [1024]u8 = undefined;
    var len: usize = 0;
    try testing.expectError(error.TooLarge, small.consumer.recv(small.endpoint, buf[0..100], 0, &len));
    try testing.expectEqual(@as(usize, 500), len);
    len = 0;
    try testing.expectError(error.TooLarge, small.consumer.recv(small.endpoint, buf[0..0], 0, &len));
    try testing.expectEqual(@as(usize, 500), len);
    try testing.expectEqual(ring_set.published(0), queue.layout.cell(0, 0).state.load(.seq_cst));
    try testing.expectEqual(@as(u64, 0), queue.layout.ring(0).head.load(.seq_cst));
    try expectPattern(9, 500, try big.recv(&buf, 0));
    try testing.expectError(error.Timeout, small.recv(buf[0..0], 0));
}

test "a queue of one consumer slot: its consumer claims and moves head with stores, the same words as a claim's" {
    const queue = try Queue.create(4096, 2, 1);
    defer queue.destroy();
    const producer = try queue.producer(1);
    defer producer.destroy();
    const consumer = try queue.consumer(0);
    defer consumer.destroy();
    try testing.expect(consumer.consumer.taker.sole());
    for (0..3) |index| try sendPattern(producer, index, 40, 0);
    try expectPattern(0, 40, try consumer.recvAcquire(0));
    // Held: its intent, its claim and head past it, as the owner's look at an ended consumer reads them
    const layout = &queue.layout;
    try testing.expectEqual(ring_set.claimed(0, 0), layout.cell(1, 0).state.load(.seq_cst));
    try testing.expectEqual(@as(u64, 1), layout.ring(1).head.load(.seq_cst));
    const held = segment.Rings.heldClaim(layout, 0).?;
    try testing.expectEqual(@as(u32, 1), held.ring);
    try testing.expectEqual(@as(u64, 0), held.index);
    var buf: [40]u8 = undefined;
    try expectPattern(1, 40, try consumer.recv(&buf, 0));
    try testing.expectEqual(ring_set.done(0, 0), layout.cell(1, 0).state.load(.seq_cst));
    try testing.expectEqual(ring_set.done(1, 0), layout.cell(1, 1).state.load(.seq_cst));
    // A short buffer still finds a message too large, and takes nothing
    var len: usize = 0;
    try testing.expectError(error.TooLarge, consumer.consumer.recv(consumer.endpoint, buf[0..10], 0, &len));
    try testing.expectEqual(@as(usize, 40), len);
    try testing.expectEqual(@as(u64, 2), layout.ring(1).head.load(.seq_cst));
    try expectPattern(2, 40, try consumer.recv(&buf, 0));

    // Two consumer slots: the claim is a compare-and-swap
    const pool = try Queue.create(4096, 1, 2);
    defer pool.destroy();
    const member = try pool.consumer(0);
    defer member.destroy();
    try testing.expect(!member.consumer.taker.sole());
}

test "in place: held until release, the next receive or the close, each of which marks it done" {
    const queue = try Queue.create(4096, 1, 1);
    defer queue.destroy();
    const producer = try queue.producer(0);
    defer producer.destroy();
    const consumer = try queue.consumer(0);
    for (0..3) |index| try sendPattern(producer, index, 70, 0);
    consumer.consumer.release(); // none held: nothing
    try expectPattern(0, 70, try consumer.recvAcquire(0));
    try testing.expectEqual(ring_set.claimed(0, 0), queue.layout.cell(0, 0).state.load(.seq_cst));
    consumer.consumer.release();
    try testing.expectEqual(ring_set.done(0, 0), queue.layout.cell(0, 0).state.load(.seq_cst));
    consumer.consumer.release();
    try expectPattern(1, 70, try consumer.recvAcquire(0));
    var buf: [128]u8 = undefined;
    try expectPattern(2, 70, try consumer.recv(&buf, 0));
    try testing.expectEqual(ring_set.done(1, 0), queue.layout.cell(0, 1).state.load(.seq_cst));
    try sendPattern(producer, 3, 70, 0);
    try expectPattern(3, 70, try consumer.recvAcquire(0));
    consumer.destroy();
    try testing.expectEqual(ring_set.done(3, 0), queue.layout.cell(0, 3).state.load(.seq_cst));
}

test "ready bits: set by a publish onto an empty ring, cleared on the last claim, set again for a message that came" {
    const queue = try Queue.create(4096, 2, 1);
    defer queue.destroy();
    const producer = try queue.producer(1);
    defer producer.destroy();
    const consumer = try queue.consumer(0);
    defer consumer.destroy();
    try testing.expect(!consumer.consumer.taker.clearReady(1));
    try sendPattern(producer, 0, 30, 0);
    try sendPattern(producer, 1, 30, 0);
    try testing.expectEqual(@as(u64, 0b10), queue.readyBits());
    // A clear that finds a message sets the bit again
    try testing.expect(consumer.consumer.taker.clearReady(1));
    try testing.expectEqual(@as(u64, 0b10), queue.readyBits());
    var buf: [64]u8 = undefined;
    _ = try consumer.recv(&buf, 0);
    try testing.expectEqual(@as(u64, 0b10), queue.readyBits()); // one left
    _ = try consumer.recv(&buf, 0);
    try testing.expectEqual(@as(u64, 0), queue.readyBits());
    // A stale bit is cleared by the pass that finds the ring empty
    _ = queue.layout.header.ready.fetchOr(0b11, .seq_cst);
    try testing.expectError(error.Timeout, consumer.recv(&buf, 0));
    try testing.expectEqual(@as(u64, 0), queue.readyBits());
}

// === Waits ===

test "a parked consumer is woken by a publish onto an empty ring, and by its cancel" {
    const queue = try Queue.create(4096, 1, 1);
    defer queue.destroy();
    const producer = try queue.producer(0);
    defer producer.destroy();
    const consumer = try queue.consumer(0);
    defer consumer.destroy();
    longParks(consumer);
    const Receiving = struct {
        fn run(end: *ConsumerEnd, result: *?DataError, got: *usize) void {
            var buf: [64]u8 = undefined;
            result.* = null;
            const msg = end.recv(&buf, 30_000) catch |err| {
                result.* = err;
                return;
            };
            got.* = msg.len;
        }
    };
    var result: ?DataError = null;
    var got: usize = 0;
    var since = consumer.consumer.taker.sleeps.load(.seq_cst);
    var thread = try std.Thread.spawn(.{}, Receiving.run, .{ consumer, &result, &got });
    awaitParked(consumer, since);
    const start_ms = nowMs();
    try sendPattern(producer, 0, 33, 0);
    thread.join();
    try testing.expect(result == null and got == 33 and nowMs() - start_ms < 5000);
    try testing.expectEqual(@as(u64, 0), queue.layout.header.idle.load(.seq_cst));
    try testing.expectEqual(@as(u32, 0), queue.layout.takerSlot(0).sleeps.load(.seq_cst));

    since = consumer.consumer.taker.sleeps.load(.seq_cst);
    thread = try std.Thread.spawn(.{}, Receiving.run, .{ consumer, &result, &got });
    awaitParked(consumer, since);
    consumer.cancel();
    thread.join();
    try testing.expectEqual(@as(?DataError, error.Cancelled), result);
}

test "chaining: a claim that leaves a message behind wakes a second parked consumer" {
    const queue = try Queue.create(4096, 1, 2);
    defer queue.destroy();
    const producer = try queue.producer(0);
    defer producer.destroy();
    const consumers: [2]*ConsumerEnd = .{ try queue.consumer(0), try queue.consumer(1) };
    defer for (consumers) |consumer| consumer.destroy();
    for (consumers) |consumer| longParks(consumer);
    // Each takes one message in place and holds it: the other must get the second without its slice
    const Holding = struct {
        fn run(end: *ConsumerEnd, got: *Atomic(u32), failure: *Failure) void {
            const msg = end.recvAcquire(30_000) catch return failure.set("a receive failed");
            if (msg.len != 33) return failure.set("a message was wrong");
            _ = got.fetchAdd(1, .seq_cst);
        }
    };
    for (0..20) |_| {
        var got: Atomic(u32) = .init(0);
        var failure: Failure = .{};
        var threads: [2]std.Thread = undefined;
        for (&threads, consumers) |*thread, consumer| {
            const since = consumer.consumer.taker.sleeps.load(.seq_cst);
            thread.* = try std.Thread.spawn(.{}, Holding.run, .{ consumer, &got, &failure });
            awaitParked(consumer, since);
        }
        const start_ms = nowMs();
        try sendPattern(producer, 0, 33, 0);
        try sendPattern(producer, 1, 33, 0);
        for (threads) |thread| thread.join();
        try failure.check();
        try testing.expect(got.load(.seq_cst) == 2 and nowMs() - start_ms < 5000);
        for (consumers) |consumer| consumer.consumer.release();
    }
}

test "a consumer woken for a message too large for its buffer wakes another parked consumer for it" {
    const queue = try Queue.create(4096, 1, 2);
    defer queue.destroy();
    const producer = try queue.producer(0);
    defer producer.destroy();
    const consumers: [2]*ConsumerEnd = .{ try queue.consumer(0), try queue.consumer(1) };
    defer for (consumers) |consumer| consumer.destroy();
    for (consumers) |consumer| longParks(consumer);
    const Receiving = struct {
        fn run(end: *ConsumerEnd, buf_len: usize, result: *?DataError, got: *usize) void {
            var buf: [1024]u8 = undefined;
            result.* = null;
            var len: usize = 0;
            end.consumer.recv(end.endpoint, buf[0..buf_len], 30_000, &len) catch |err| {
                result.* = err;
            };
            got.* = len;
        }
    };
    var results: [2]?DataError = .{ null, null };
    var got: [2]usize = .{ 0, 0 };
    var threads: [2]std.Thread = undefined;
    for (&threads, consumers, [_]usize{ 100, 1024 }, &results, &got) |*thread, consumer, buf_len, *result, *len| {
        const since = consumer.consumer.taker.sleeps.load(.seq_cst);
        thread.* = try std.Thread.spawn(.{}, Receiving.run, .{ consumer, buf_len, result, len });
        awaitParked(consumer, since);
    }
    // The publish wakes consumer 0 (the first idle one from ring 0's slot), whose buffer is too short
    const start_ms = nowMs();
    try sendPattern(producer, 0, 500, 0);
    for (threads) |thread| thread.join();
    try testing.expectEqual(@as(?DataError, error.TooLarge), results[0]);
    try testing.expectEqual([2]usize{ 500, 500 }, got);
    try testing.expectEqual(@as(?DataError, null), results[1]);
    try testing.expect(nowMs() - start_ms < 5000);
}

test "a park's slice ends in a pass over every ring: a message whose ready bit is missing is found" {
    const queue = try Queue.create(4096, 2, 1);
    defer queue.destroy();
    const producer = try queue.producer(1);
    defer producer.destroy();
    const consumer = try queue.consumer(0);
    defer consumer.destroy();
    consumer.consumer.taker.longest_sleep_ms = 20;
    consumer.consumer.taker.spin_iters = 0;
    try sendPattern(producer, 0, 33, 0);
    queue.layout.header.ready.store(0, .seq_cst);
    const since = consumer.consumer.taker.sleeps.load(.seq_cst);
    var buf: [64]u8 = undefined;
    try expectPattern(0, 33, try consumer.recv(&buf, 2000));
    try testing.expect(consumer.consumer.taker.sleeps.load(.seq_cst) != since);
}

test "rescan: a message left without its ready bit gets it, and a parked consumer is woken" {
    const queue = try Queue.create(4096, 3, 1);
    defer queue.destroy();
    const producer = try queue.producer(2);
    defer producer.destroy();
    const consumer = try queue.consumer(0);
    defer consumer.destroy();
    longParks(consumer);
    try sendPattern(producer, 0, 33, 0);
    queue.layout.header.ready.store(0, .seq_cst); // a producer that ended before its ready bit
    const Receiving = struct {
        fn run(end: *ConsumerEnd, failure: *Failure) void {
            var buf: [64]u8 = undefined;
            const msg = end.recv(&buf, 30_000) catch return failure.set("a receive failed");
            if (msg.len != 33) failure.set("a message was wrong");
        }
    };
    var failure: Failure = .{};
    const since = consumer.consumer.taker.sleeps.load(.seq_cst);
    const thread = try std.Thread.spawn(.{}, Receiving.run, .{ consumer, &failure });
    awaitParked(consumer, since);
    const start_ms = nowMs();
    segment.Rings.rescan(&queue.layout, &queue.wakes);
    thread.join();
    try failure.check();
    try testing.expect(nowMs() - start_ms < 5000);
}

// === The end ===

test "the queue's end: a producer is Disconnected before writing; a consumer drains every ring, then Disconnected" {
    const queue = try Queue.create(4096, 2, 1);
    defer queue.destroy();
    const producers: [2]*ProducerEnd = .{ try queue.producer(0), try queue.producer(1) };
    defer for (producers) |producer| producer.destroy();
    const consumer = try queue.consumer(0);
    defer consumer.destroy();
    for (0..3) |index| for (producers, 0..) |producer, producer_index| try sendPattern(producer, 10 * producer_index + index, 20, 0);
    queue.markEnded();
    for (producers) |producer| producer.connectionEnded();
    consumer.connectionEnded();
    try testing.expectError(error.Disconnected, sendPattern(producers[0], 99, 20, 0));
    try testing.expectEqual(@as(u64, 3), queue.layout.ring(0).tail.load(.seq_cst));
    queue.layout.header.ready.store(0, .seq_cst); // the drain looks at every ring all the same
    var buf: [64]u8 = undefined;
    for (0..6) |_| _ = try consumer.recv(&buf, -1);
    try testing.expectError(error.Disconnected, consumer.recv(&buf, -1));
    try testing.expectError(error.Disconnected, consumer.recvAcquire(-1));
}

test "the end wakes a parked consumer; its own failure (no mark) is Disconnected at once, for a producer too" {
    const queue = try Queue.create(4096, 1, 2);
    defer queue.destroy();
    const producer = try queue.producer(0);
    defer producer.destroy();
    const ended = try queue.consumer(0);
    defer ended.destroy();
    const failed = try queue.consumer(1);
    defer failed.destroy();
    longParks(ended);
    var buf: [64]u8 = undefined;
    // Its own failure: it takes nothing more, the others do
    try sendPattern(producer, 0, 20, 0);
    failed.connectionEnded();
    try testing.expectError(error.Disconnected, failed.recv(&buf, -1));
    try testing.expectError(error.Disconnected, failed.recvAcquire(-1));
    try expectPattern(0, 20, try ended.recv(&buf, 0));
    // The queue's end, while parked
    const Receiving = struct {
        fn run(end: *ConsumerEnd, result: *?DataError) void {
            var b: [64]u8 = undefined;
            result.* = null;
            _ = end.recv(&b, -1) catch |err| {
                result.* = err;
            };
        }
    };
    var result: ?DataError = null;
    const since = ended.consumer.taker.sleeps.load(.seq_cst);
    const thread = try std.Thread.spawn(.{}, Receiving.run, .{ ended, &result });
    awaitParked(ended, since);
    queue.markEnded();
    ended.connectionEnded();
    thread.join();
    try testing.expectEqual(@as(?DataError, error.Disconnected), result);
    // A producer's own failure
    queue.layout.header.ended.store(0, .seq_cst);
    producer.connectionEnded();
    try testing.expectError(error.Disconnected, sendPattern(producer, 0, 20, -1));
}

// === The owner's recovery and settling ===

test "an ended consumer's message: the one its claim names while its cell holds the claim, done once; nothing else" {
    const queue = try Queue.create(4096, 2, 3);
    defer queue.destroy();
    const producer = try queue.producer(1);
    defer producer.destroy();
    const live = try queue.consumer(0);
    defer live.destroy();
    for (0..4) |index| try sendPattern(producer, index, 20, 0);
    const layout = &queue.layout;
    // Consumer 2 stored its intent, claimed message 0, and ended before its move of head
    layout.takerSlot(2).claim.store(ring_set.intent(1, 0), .seq_cst);
    try testing.expectEqual(null, layout.cell(1, 0).state.cmpxchgStrong(ring_set.published(0), ring_set.claimed(0, 2), .seq_cst, .seq_cst));
    const held = segment.Rings.heldClaim(layout, 2).?;
    try testing.expectEqual(@as(u32, 1), held.ring);
    try testing.expectEqual(@as(u64, 0), held.index);
    try testing.expectEqual(@as(u32, 20), held.len);
    try testing.expect(segment.Rings.finishHeld(layout, 2, held));
    try testing.expectEqual(ring_set.done(0, 2), layout.cell(1, 0).state.load(.seq_cst));
    try testing.expectEqual(@as(u64, 1), layout.ring(1).head.load(.seq_cst));
    try testing.expectEqual(null, segment.Rings.heldClaim(layout, 2)); // once
    try testing.expect(!segment.Rings.finishHeld(layout, 2, held));

    // Consumer 1 ended holding message 1 in place
    {
        const holder = try queue.consumer(1);
        _ = try holder.recvAcquire(0);
        holder.destroyHolding();
    }
    try testing.expectEqual(ring_set.claimed(1, 1), layout.cell(1, 1).state.load(.seq_cst));
    try testing.expectEqual(@as(u64, 1), segment.Rings.heldClaim(layout, 1).?.index);

    // A live consumer's claim is never taken for another's: an intent naming it, of another consumer, holds nothing
    _ = try live.recvAcquire(0);
    layout.takerSlot(2).claim.store(ring_set.intent(1, 2), .seq_cst);
    try testing.expectEqual(null, segment.Rings.heldClaim(layout, 2));
    try testing.expectEqual(ring_set.claimed(2, 0), layout.cell(1, 2).state.load(.seq_cst));
    // An intent stored before a claim that never came: the message stays for the others
    layout.takerSlot(2).claim.store(ring_set.intent(1, 3), .seq_cst);
    try testing.expectEqual(null, segment.Rings.heldClaim(layout, 2));
    var buf: [64]u8 = undefined;
    try expectPattern(3, 20, try live.recv(&buf, 0));
    // Garbage intents: no ring, a ring past the return ring (ring number 4 of 3), no message, all ones
    for ([_]u64{ 0, 0xff, ring_set.intent(1, 0) & ~@as(u64, 0xff) | 4, 0x2, ~@as(u64, 0) }) |garbage| {
        layout.takerSlot(2).claim.store(garbage, .seq_cst);
        try testing.expectEqual(null, segment.Rings.heldClaim(layout, 2));
    }
}

/// The owner's returns of `queue` (returns.zig), on its view.
fn returnsOf(queue: *Queue) !returns_module.Returns {
    return .init(testing.allocator, queue.layout, queue.session.id);
}

/// Consumer `slot` takes the next message in place, and its process ends: nothing released.
fn endHolding(queue: *Queue, slot: u32) !void {
    const holder = try queue.consumer(slot);
    defer holder.destroyHolding();
    _ = try holder.recvAcquire(0);
}

/// Fills the return ring with `count` messages of 64 bytes, numbered from `first`, as the owner publishes them.
fn fillReturnRing(returns: *returns_module.Returns, first: u64, count: u64) !void {
    const back = segment.returnRing(&returns.layout);
    for (first..first + count) |index| {
        const at = returns.producer.reserveNow(64) orelse return error.NoRoom;
        for (returns.layout.bytes(back, at, 64), 0..) |*byte, offset| byte.* = patternByte(index, offset);
        returns.producer.publishMessage(at, 64);
    }
}

/// The number of a pattern message (`sendPattern`, `fillReturnRing`) below 256: its first byte tells.
fn patternIndex(msg: []const u8) u64 {
    for (0..256) |index| {
        if (patternByte(index, 0) == msg[0]) return index;
    }
    unreachable;
}

test "returns: a held message of a producer's ring is published once on the return ring, then its original done; another consumer takes it" {
    const queue = try Queue.create(4096, 2, 3);
    defer queue.destroy();
    var returns = try returnsOf(queue);
    defer returns.deinit(testing.allocator);
    const producer = try queue.producer(1);
    defer producer.destroy();
    for (0..3) |index| try sendPattern(producer, index, 64 + index, 0);
    try endHolding(queue, 2);
    const layout = &queue.layout;
    const back = segment.returnRing(layout);
    try testing.expectEqual(@as(u32, 2), back);
    try testing.expectEqual(ring_set.claimed(0, 2), layout.cell(1, 0).state.load(.seq_cst));
    try testing.expectEqual(.free, returns.recover(2));
    try testing.expectEqual(ring_set.done(0, 2), layout.cell(1, 0).state.load(.seq_cst));
    try testing.expectEqual(@as(u64, 1), layout.ring(back).tail.load(.seq_cst));
    try testing.expectEqual(ring_set.published(0), layout.cell(back, 0).state.load(.seq_cst));
    try testing.expect(queue.readyBits() & ring_set.bitOf(back) != 0);
    try testing.expect(!returns.pending());
    // Once: the slot holds nothing now
    try testing.expectEqual(.free, returns.recover(2));
    try testing.expectEqual(@as(u64, 1), layout.ring(back).tail.load(.seq_cst));
    // A consumer takes each message once, the returned one among them
    const live = try queue.consumer(0);
    defer live.destroy();
    var seen: [3]bool = @splat(false);
    var buf: [128]u8 = undefined;
    for (0..3) |_| {
        const msg = try live.recv(&buf, 0);
        const index = msg.len - 64;
        try expectPattern(index, msg.len, msg);
        try testing.expect(!seen[index]);
        seen[index] = true;
    }
    try testing.expectError(error.Timeout, live.recv(&buf, 0));
    // The original's room is its producer's again: a ring's worth of sends
    for (3..3 + layout.cellCount()) |index| try sendPattern(producer, index, 20, 0);
}

test "returns: no room on the return ring: the message stays claimed and its slot waits; the copy goes once a done made room" {
    const queue = try Queue.create(4096, 1, 3);
    defer queue.destroy();
    var returns = try returnsOf(queue);
    defer returns.deinit(testing.allocator);
    const layout = &queue.layout;
    const back = segment.returnRing(layout);
    try fillReturnRing(&returns, 100, layout.cellCount());
    try testing.expectEqual(null, returns.producer.reserveNow(64));
    const producer = try queue.producer(0);
    defer producer.destroy();
    try sendPattern(producer, 7, 30, 0);
    try endHolding(queue, 0); // its pass starts at ring 0, the producer's
    try testing.expectEqual(ring_set.claimed(0, 0), layout.cell(0, 0).state.load(.seq_cst));
    try testing.expectEqual(.waiting, returns.recover(0));
    try testing.expect(returns.pending());
    try testing.expectEqual(@as(u64, 0), returns.tick().mask);
    try testing.expectEqual(ring_set.claimed(0, 0), layout.cell(0, 0).state.load(.seq_cst));
    try testing.expectEqual(layout.cellCount(), layout.ring(back).tail.load(.seq_cst));
    // A consumer takes the return ring's oldest message: its done frees its room, and the next tick copies
    const live = try queue.consumer(1);
    defer live.destroy();
    var buf: [64]u8 = undefined;
    try expectPattern(100, 64, try live.recv(&buf, 0));
    try testing.expectEqual(@as(u64, 1), returns.tick().mask);
    try testing.expect(!returns.pending());
    try testing.expectEqual(ring_set.done(0, 0), layout.cell(0, 0).state.load(.seq_cst));
    try testing.expectEqual(layout.cellCount() + 1, layout.ring(back).tail.load(.seq_cst));
    const copied = layout.cell(back, layout.cellCount());
    try expectPattern(7, 30, layout.bytes(back, copied.payload_position.load(.seq_cst), copied.len.load(.seq_cst)));
}

test "returns: a returned message whose consumer ended moves to the scratch FIFO; with the ring full and its oldest held, then that holder's end too, both come back" {
    const queue = try Queue.create(4096, 1, 3);
    defer queue.destroy();
    var returns = try returnsOf(queue);
    defer returns.deinit(testing.allocator);
    const layout = &queue.layout;
    const back = segment.returnRing(layout);
    const cells = layout.cellCount();
    // The return ring full; its oldest message, 0, held in place by consumer 1; message 1 by consumer 2, which ends
    try fillReturnRing(&returns, 0, cells);
    const holder = try queue.consumer(1);
    var holder_ended = false;
    defer if (!holder_ended) holder.destroy();
    try expectPattern(0, 64, try holder.recvAcquire(0));
    try endHolding(queue, 2);
    try testing.expectEqual(ring_set.claimed(1, 2), layout.cell(back, 1).state.load(.seq_cst));
    // A copy within the full ring would wait for room its oldest message holds: the FIFO takes it, the original is done
    try testing.expectEqual(.free, returns.recover(2));
    try testing.expectEqual(ring_set.done(1, 2), layout.cell(back, 1).state.load(.seq_cst));
    try testing.expectEqual(@as(u64, 1), returns.scratch.count());
    try testing.expect(returns.pending());
    try testing.expectEqual(@as(u64, 0), returns.tick().mask); // no room: message 0 is held
    try testing.expectEqual(@as(u64, 1), returns.scratch.count());
    // The holder ends too: its message moves as well, and nothing waits on a live consumer any more
    holder.destroyHolding();
    holder_ended = true;
    try testing.expectEqual(.free, returns.recover(1));
    try testing.expectEqual(@as(u64, 2), returns.scratch.count());
    try testing.expect(returns.scratch.end - returns.scratch.start <= layout.capacity);
    try testing.expectEqual(@as(u64, 0), returns.tick().mask);
    try testing.expect(!returns.pending());
    try testing.expectEqual(cells + 2, layout.ring(back).tail.load(.seq_cst));
    // Every message once
    const live = try queue.consumer(0);
    defer live.destroy();
    var seen: [64]u8 = @splat(0);
    var buf: [64]u8 = undefined;
    for (0..cells) |_| {
        const msg = try live.recv(&buf, 0);
        const index = patternIndex(msg);
        try expectPattern(index, 64, msg);
        seen[@intCast(index)] += 1;
    }
    try testing.expectError(error.Timeout, live.recv(&buf, 0));
    for (seen) |times| try testing.expectEqual(@as(u8, 1), times);
}

test "settling: a producer's slot is busy while a message is unclaimed or held, settled after; a corrupt tail or head" {
    const queue = try Queue.create(4096, 2, 2);
    defer queue.destroy();
    const consumer = try queue.consumer(1);
    defer consumer.destroy();
    try testing.expectEqual(.settled, segment.Rings.ringSettled(&queue.layout, 0));
    {
        const producer = try queue.producer(0);
        defer producer.destroy();
        for (0..2) |index| try sendPattern(producer, index, 20, 0);
    }
    try testing.expectEqual(.busy, segment.Rings.ringSettled(&queue.layout, 0));
    var buf: [64]u8 = undefined;
    _ = try consumer.recv(&buf, 0);
    try testing.expectEqual(.busy, segment.Rings.ringSettled(&queue.layout, 0));
    _ = try consumer.recvAcquire(0);
    try testing.expectEqual(.busy, segment.Rings.ringSettled(&queue.layout, 0)); // held in place
    consumer.consumer.release();
    try testing.expectEqual(.settled, segment.Rings.ringSettled(&queue.layout, 0));
    try testing.expectEqual(.settled, segment.Rings.ringSettled(&queue.layout, 1));
    // A corrupt intent names no message of the ring; a corrupt tail retires the slot
    queue.layout.takerSlot(0).claim.store(1, .seq_cst);
    try testing.expectEqual(.settled, segment.Rings.ringSettled(&queue.layout, 0));
    queue.layout.ring(1).head.store(5, .seq_cst); // past its tail, 0
    try testing.expectEqual(.corrupt, segment.Rings.ringSettled(&queue.layout, 1));
    queue.layout.ring(1).head.store(0, .seq_cst);
    queue.layout.ring(1).tail.store(wire_protocol.queue_max_index, .seq_cst);
    try testing.expectEqual(.corrupt, segment.Rings.ringSettled(&queue.layout, 1));
}

// === A corrupt peer ===

/// The corrupt peer's writes: random words, and values each reader must check, through the owner's view. Without
/// `forges_cells` it leaves the cells and the payloads alone: every other word, the ring indices included, can't make
/// the library read a payload its producer may be writing.
const Scribbler = struct {
    layout: *const segment.Layout,
    stop: *Atomic(bool),
    forges_cells: bool,
    writes: u64 = 0,

    fn run(scribbler: *Scribbler) void {
        const layout = scribbler.layout;
        var random: u64 = 0x5c41bb1e;
        const cells = layout.cellCount();
        while (!scribbler.stop.load(.acquire)) : (scribbler.writes += 1) {
            random = random *% 6364136223846793005 +% 1442695040888963407;
            const word = random >> 11;
            const any_pick = (random >> 3) % 16;
            const pick = if (!scribbler.forges_cells and any_pick >= 8 and any_pick <= 11) 12 else any_pick;
            const ring_number: u32 = @intCast((random >> 40) % layout.max_senders);
            const ring = layout.ring(ring_number);
            const near = ring.head.load(.monotonic) +% (word % 5) -% 2;
            // The message a forged state or intent names: one a state word can name, below the index bound
            const index = near % wire_protocol.queue_max_index;
            switch (pick) {
                0 => layout.header.ended.store(@intFromBool(word % 8 == 0), .seq_cst),
                1 => layout.header.ready.store(word, .seq_cst),
                2 => layout.header.idle.store(word, .seq_cst),
                3 => layout.takerSlot(@intCast(word % layout.max_takers)).sleeps.store(@truncate(word), .seq_cst),
                4 => layout.takerSlot(@intCast(word % layout.max_takers)).claim.store(if (word % 2 == 0) word else ring_set.intent(ring_number, index), .seq_cst),
                5 => ring.tail.store(if (word % 4 == 0) word else near +% cells * (word % 3), .seq_cst),
                6 => ring.head.store(if (word % 4 == 0) word else near, .seq_cst),
                7 => ring.room_sleeps.store(@truncate(word), .seq_cst),
                8 => layout.cell(ring_number, near).state.store(switch (word % 4) {
                    0 => ring_set.published(index),
                    1 => ring_set.done(index, @intCast(word % 64)),
                    2 => ring_set.claimed(index, @intCast(word % 64)),
                    else => word,
                }, .seq_cst),
                9 => layout.cell(ring_number, near).payload_position.store(if (word % 2 == 0) word else word % (2 * layout.capacity), .seq_cst),
                10 => layout.cell(ring_number, near).len.store(@truncate(if (word % 2 == 0) word else word % (2 * layout.maxPiece())), .seq_cst),
                11 => layout.bytes(ring_number, word, 8)[0] = @truncate(word),
                else => {
                    // The queue goes on between the writes, mostly
                    layout.header.ended.store(0, .seq_cst);
                    for (0..64) |_| std.atomic.spinLoopHint();
                },
            }
            if (scribbler.writes % 64 == 0) std.Thread.yield() catch {};
        }
    }
};

/// A member's calls under the corrupt peer: each with a short timeout, each timed.
const Calling = struct {
    producer: ?*ProducerEnd = null,
    consumer: ?*ConsumerEnd = null,
    stop: *Atomic(bool),
    failure: *Failure,
    calls: u64 = 0,
    oks: u64 = 0,
    longest_ms: i64 = 0,
    thread: std.Thread = undefined,

    fn run(calling: *Calling) void {
        var random: u64 = @intFromPtr(calling) | 1;
        var buf: [1024]u8 = undefined;
        @memset(&buf, 0x5a);
        while (!calling.stop.load(.acquire)) : (calling.calls += 1) {
            random = random *% 6364136223846793005 +% 1442695040888963407;
            const timeout_ms: c_int = @intCast((random >> 40) % 6);
            const start_ms = nowMs();
            const ok = if (calling.producer) |end| ok: {
                const size = 1 + (random >> 20) % end.producer.layout.maxPiece();
                if (random >> 63 == 0) break :ok if (end.send(buf[0..size], timeout_ms)) true else |_| false;
                const room = end.producer.sendAcquire(end.endpoint, size, timeout_ms) catch break :ok false;
                @memset(room, 0xa5);
                break :ok if (end.producer.sendCommit(end.endpoint, size)) true else |_| false;
            } else ok: {
                const end = calling.consumer.?;
                const base = @intFromPtr(end.session.mapping.base);
                const msg: []const u8 = (if (random >> 63 == 0) end.recv(&buf, timeout_ms) else end.recvAcquire(timeout_ms)) catch break :ok false;
                // Whatever the segment says, a message lies within the mapping
                if (msg.ptr != &buf and (@intFromPtr(msg.ptr) < base or @intFromPtr(msg.ptr) + msg.len > base + end.session.mapping.size))
                    calling.failure.set("a message outside the segment");
                break :ok true;
            };
            if (ok) calling.oks += 1;
            const took = nowMs() - start_ms;
            calling.longest_ms = @max(calling.longest_ms, took);
            // Each call ends within its timeout, plus room for a descheduled thread
            if (took > timeout_ms + 2000) calling.failure.set("a call ran far past its timeout");
        }
    }
};

/// Producers and consumers calling with short timeouts while a corrupt peer scribbles the segment (`Scribbler`).
fn corruptPeer(forges_cells: bool) !void {
    const queue = try Queue.create(4096, 2, 3);
    defer queue.destroy();
    var stop: Atomic(bool) = .init(false);
    var failure: Failure = .{};
    var callings: [5]Calling = undefined;
    var made: usize = 0;
    defer for (callings[0..made]) |*calling| {
        if (calling.producer) |end| end.destroy();
        if (calling.consumer) |end| end.destroy();
    };
    for (&callings, 0..) |*calling, member_index| {
        calling.* = .{ .stop = &stop, .failure = &failure };
        if (member_index < 2) calling.producer = try queue.producer(@intCast(member_index)) else calling.consumer = try queue.consumer(@intCast(member_index - 2));
        made += 1;
    }
    var scribbler: Scribbler = .{ .layout = &queue.layout, .stop = &stop, .forges_cells = forges_cells };
    var started: usize = 0;
    defer {
        stop.store(true, .release);
        for (callings[0..started]) |*calling| calling.thread.join();
    }
    for (&callings) |*calling| {
        calling.thread = try std.Thread.spawn(.{}, Calling.run, .{calling});
        started += 1;
    }
    const scribbling = try std.Thread.spawn(.{}, Scribbler.run, .{&scribbler});
    testing.io.sleep(.fromMilliseconds(600), .awake) catch {};
    stop.store(true, .release);
    scribbling.join();
    for (callings[0..started]) |*calling| calling.thread.join();
    started = 0;
    try failure.check();
    var calls: u64 = 0;
    var oks: u64 = 0;
    var longest: i64 = 0;
    for (callings) |calling| {
        calls += calling.calls;
        oks += calling.oks;
        longest = @max(longest, calling.longest_ms);
        try testing.expect(calling.calls > 0);
    }
    std.debug.print("  corrupt peer (cells {s}): {d} writes; {d} calls, {d} OK, the longest {d} ms\n", .{
        if (forges_cells) "forged" else "left alone", scribbler.writes, calls, oks, longest,
    });
}

test "a corrupt peer scribbling the segment: every call returns within its timeout, nothing crashes or hangs" {
    // Forged states let a producer overwrite a payload a consumer still copies: under TSan, a race the corrupt peer
    // makes, which the sanitizer would report. The next test runs under TSan too
    if (builtin.sanitize_thread) return error.SkipZigTest;
    try corruptPeer(true);
}

test "a corrupt peer scribbling every word but the cells and payloads: calls return within their timeouts" {
    try corruptPeer(false);
}

// === Threads ===

/// The stress's consumer threads: each message must be whole and of a producer, and come after that producer's last
/// one this consumer took; each is counted once in `seen`.
const Taking = struct {
    end: *ConsumerEnd,
    seen: []const []Atomic(u8),
    failure: *Failure,
    received: u64 = 0,
    thread: std.Thread = undefined,

    fn run(taking: *Taking) void {
        var buf: [Message.max]u8 = undefined;
        var last: [wire_protocol.max_queue_producers]?u64 = @splat(null);
        var random: u64 = @as(u64, taking.end.consumer.taker.slot) *% 0x9e3779b97f4a7c15 +% 7;
        while (true) {
            random = random *% 6364136223846793005 +% 1442695040888963407;
            const in_place = random >> 62 == 0;
            const msg: []const u8 = (if (in_place) taking.end.recvAcquire(-1) else taking.end.recv(&buf, -1)) catch |err| switch (err) {
                error.Disconnected => return,
                else => return taking.failure.set("a receive failed"),
            };
            const id = Message.check(msg) orelse return taking.failure.set("a message was torn");
            if (id.producer >= taking.seen.len or id.seq >= taking.seen[id.producer].len) return taking.failure.set("a message of no producer");
            if (last[id.producer]) |previous| if (id.seq <= previous) return taking.failure.set("a producer's messages came out of order");
            last[id.producer] = id.seq;
            if (taking.seen[id.producer][id.seq].fetchAdd(1, .seq_cst) != 0) return taking.failure.set("a message was taken twice");
            if (in_place and random >> 58 & 0xf == 0) {
                // Held a while in place, it stays whole
                for (0..2000) |_| std.atomic.spinLoopHint();
                if (Message.check(msg) == null) return taking.failure.set("a message held in place was overwritten");
            }
            taking.received += 1;
        }
    }
};

const Giving = struct {
    end: *ProducerEnd,
    count: u64,
    budget_ms: i64,
    failure: *Failure,
    sent: u64 = 0,
    thread: std.Thread = undefined,

    fn run(giving: *Giving) void {
        var buf: [Message.max]u8 = undefined;
        const max_piece = giving.end.producer.layout.maxPiece();
        const start_ms = nowMs();
        while (giving.sent < giving.count and (giving.sent % 256 != 0 or nowMs() - start_ms < giving.budget_ms)) : (giving.sent += 1) {
            const size = Message.sizeOf(giving.sent, max_piece);
            Message.fill(buf[0..size], giving.end.producer.ring, giving.sent);
            giving.end.send(buf[0..size], 10_000) catch return giving.failure.set("a send failed");
        }
    }
};

/// `producers` producer and `consumers` consumer threads on a queue of `capacity` bytes per ring: each producer sends
/// up to `count` messages of varied sizes, for at most `budget_ms`; then the queue ends, and the consumers drain it.
/// Each message sent must have been taken exactly once.
fn stress(capacity: u32, producers: u32, consumers: u32, count: u64, budget_ms: i64) !void {
    const queue = try Queue.create(capacity, producers, consumers);
    defer queue.destroy();
    var failure: Failure = .{};
    const seen = try testing.allocator.alloc([]Atomic(u8), producers);
    defer testing.allocator.free(seen);
    var seen_made: usize = 0;
    defer for (seen[0..seen_made]) |row| testing.allocator.free(row);
    for (seen) |*row| {
        row.* = try testing.allocator.alloc(Atomic(u8), count);
        @memset(row.*, .init(0));
        seen_made += 1;
    }
    const givings = try testing.allocator.alloc(Giving, producers);
    defer testing.allocator.free(givings);
    const takings = try testing.allocator.alloc(Taking, consumers);
    defer testing.allocator.free(takings);
    var givers: usize = 0;
    defer for (givings[0..givers]) |*giving| giving.end.destroy();
    for (givings, 0..) |*giving, producer_index| {
        giving.* = .{ .end = try queue.producer(@intCast(producer_index)), .count = count, .budget_ms = budget_ms, .failure = &failure };
        givers += 1;
    }
    var takers: usize = 0;
    defer for (takings[0..takers]) |*taking| taking.end.destroy();
    for (takings, 0..) |*taking, consumer_index| {
        taking.* = .{ .end = try queue.consumer(@intCast(consumer_index)), .seen = seen, .failure = &failure };
        takers += 1;
    }
    var taking_started: usize = 0;
    defer for (takings[0..taking_started]) |*taking| taking.thread.join();
    for (takings) |*taking| {
        taking.thread = try std.Thread.spawn(.{}, Taking.run, .{taking});
        taking_started += 1;
    }
    var giving_started: usize = 0;
    defer for (givings[0..giving_started]) |*giving| giving.thread.join();
    // On an early return, the queue's end lets the consumers go
    defer if (taking_started != 0) {
        queue.markEnded();
        for (takings) |*taking| taking.end.connectionEnded();
    };
    for (givings) |*giving| {
        giving.thread = try std.Thread.spawn(.{}, Giving.run, .{giving});
        giving_started += 1;
    }
    for (givings) |*giving| giving.thread.join();
    giving_started = 0;
    // The queue's end: what was sent stays for the consumers, who drain it
    queue.markEnded();
    for (takings) |*taking| taking.end.connectionEnded();
    for (takings) |*taking| taking.thread.join();
    taking_started = 0;
    try failure.check();
    var sent: u64 = 0;
    var received: u64 = 0;
    for (givings, seen) |giving, row| {
        sent += giving.sent;
        for (row[0..giving.sent]) |*count_seen| try testing.expectEqual(@as(u8, 1), count_seen.load(.seq_cst));
        for (row[giving.sent..]) |*count_seen| try testing.expectEqual(@as(u8, 0), count_seen.load(.seq_cst));
    }
    for (takings) |taking| received += taking.received;
    try testing.expectEqual(sent, received);
    try testing.expect(sent > 0);
}

/// Producer and consumer threads, more than cores together (`cores + 4`): some producer or consumer is always
/// descheduled, mid-copy or holding a message. Producers have 63 slots at most and the threads are capped at 126, so a
/// machine of 126 cores or more is the one this can't oversubscribe. The fast tier's sends stop at their time budget,
/// however many threads.
fn oversubscribed() struct { producers: u32, consumers: u32 } {
    const threads: u32 = @intCast(@min((std.Thread.getCpuCount() catch 4) + 4, 2 * wire_protocol.max_queue_producers));
    return .{ .producers = @min(threads / 2, wire_protocol.max_queue_producers), .consumers = @min(threads - threads / 2, wire_protocol.max_queue_consumers) };
}

test "stress: producers and consumers in threads, every message taken once, each producer's in order" {
    try stress(64 * 1024, 1, 1, 50_000, 200);
    try stress(64 * 1024, 1, 4, 50_000, 200);
    try stress(64 * 1024, 4, 1, 20_000, 200);
    try stress(64 * 1024, 4, 4, 20_000, 200);
    try stress(64 * 1024, 8, 8, 10_000, 250);
    try stress(4096, 4, 4, 10_000, 250); // tiny rings: the producers wait for room all the time
}

test "stress: more producer and consumer threads than cores" {
    const threads = oversubscribed();
    try stress(16 * 1024, threads.producers, threads.consumers, 5_000, 400);
}

test {
    if (@import("build_options").slow_tests) _ = slow_tests;
}

const slow_tests = struct {
    test "slow: stress: producers and consumers in threads, for seconds, oversubscribed and with tiny rings" {
        // The slow tier runs each stress test on its own, beside the other slow tests (build/tests.zig)
        if (std.testing.environ.containsUnemptyConstant("FIPC_SKIP_STRESS_QUEUE")) return error.SkipZigTest;
        try stress(256 * 1024, 4, 4, 2_000_000, 2000);
        try stress(256 * 1024, 16, 16, 500_000, 2000);
        // Twice as many threads as cores, and the tiny rings oversubscribed
        const threads = oversubscribed();
        try stress(16 * 1024, @min(2 * threads.producers, wire_protocol.max_queue_producers), @min(2 * threads.consumers, wire_protocol.max_queue_consumers), 500_000, 3000);
        try stress(4096, threads.producers, threads.consumers, 500_000, 2000);
        try stress(4096, wire_protocol.max_queue_producers, wire_protocol.max_queue_consumers, 20_000, 2000);
    }
};
