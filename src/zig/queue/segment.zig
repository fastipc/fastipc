//! A work queue's segment: its owner creates it once for the queue (`create`) and every producer and consumer maps it
//! (`attach`). It holds a description (`wire_protocol.QueueInfo`, where its parts are) and the words every member writes (the
//! end mark, the `ready` and `idle` bitmaps), a slot per consumer, then a region per ring: its control block (`tail`,
//! `head`, the room flag), its cells and its data ring. Each producer has a ring, and after theirs comes the return
//! ring, ring `max_producers`, whose only producer is the owner: it holds the messages of consumers that ended holding
//! them, copied there for another consumer (returns.zig). Consumers treat it as any ring. The rings, the cells and the
//! bitmaps are a ring set (core/ring_set.zig, whose top comment says how messages and wakes work there): the queue's
//! is `Rings`, over its wire types, its producers the senders and its consumers the takers. `Layout` is where those
//! parts are in one process's mapping, which the producers (producer.zig), the consumers (consumer.zig) and the owner
//! work on.
//!
//! No part of the segment is mapped read-only: every member writes the cells of every ring. A corrupt peer can write
//! anything in it; whatever is read from it and indexes or sizes anything is checked where it is read.
//!
//! As every segment (core/shared_memory.zig), it is created and attached without a lock.

const std = @import("std");
const Io = std.Io;
const wire_protocol = @import("../wire_protocol.zig");
const frames = @import("frames.zig");
const platform = @import("../platform.zig");
const ring_set = @import("../core/ring_set.zig");
const shared_memory = @import("../core/shared_memory.zig");

const Session = shared_memory.Session;

/// The description of a queue segment for producer rings of `capacity` bytes, `max_producers` producers and
/// `max_consumers` consumers, its session id 0: where its parts are (core/ring_set.zig `regions`). The header (512
/// bytes) and a 128-byte slot per consumer, then from the next page each ring's region, the producers' and then the
/// return ring's: its control block (512 bytes), its cells (one per 64 bytes of data, 32 bytes each), and from the next
/// page its data ring. Every region is whole pages.
pub fn describe(capacity: u32, max_producers: u32, max_consumers: u32) wire_protocol.QueueInfo {
    const parts = Rings.regions(capacity, max_producers, max_consumers);
    return .{
        .magic = wire_protocol.queue_magic,
        .header_size = wire_protocol.queue_header_size,
        .capacity = capacity,
        .cell_count = parts.cell_count,
        .max_producers = max_producers,
        .max_consumers = max_consumers,
        .session_id = @splat(0),
        .consumers_offset = parts.takers_offset,
        .producers_offset = parts.rings_offset,
        .producer_stride = parts.ring_stride,
        .cells_offset = parts.cells_offset,
        .data_offset = parts.data_offset,
        .segment_size = parts.segment_size,
        .reserved = @splat(0),
    };
}

/// A queue's capacity, a power of two from 4 KiB to 2 GiB, its producers, 1 to 63, and its consumers, 1 to 64.
pub fn valid(capacity: u64, max_producers: u64, max_consumers: u64) bool {
    return capacity >= wire_protocol.queue_min_capacity and capacity <= 1 << 31 and std.math.isPowerOfTwo(capacity) and
        max_producers >= 1 and max_producers <= wire_protocol.max_queue_producers and
        max_consumers >= 1 and max_consumers <= wire_protocol.max_queue_consumers;
}

/// A queue session's description, at the start of its segment.
pub fn info(session: *const Session) *wire_protocol.QueueInfo {
    return @ptrCast(@alignCast(session.mapping.base));
}

/// A new queue's segment under a new random id (core/shared_memory.zig `createUnderRandomId`), and its description
/// written; everything else stays zero, which is an empty queue. `handle`: the segment's handle, where it travels with
/// OFFER (created and registered by the caller), else null.
pub fn create(
    io: Io,
    handle: ?platform.Handle,
    security: *const platform.Security,
    capacity: u32,
    max_producers: u32,
    max_consumers: u32,
) shared_memory.CreateError!Session {
    std.debug.assert(valid(capacity, max_producers, max_consumers));
    var description = describe(capacity, max_producers, max_consumers);
    const id, const mapping = try shared_memory.createUnderRandomId(io, handle, description.segment_size, security, .queue);
    const session: Session = .{ .id = id, .capacity = capacity, .server = true, .mapping = mapping };
    description.session_id = id;
    info(&session).* = description;
    return session;
}

/// A member's attach (docs/protocol.md §10.3): maps the offered queue segment, checks its description against the
/// offer (core/shared_memory.zig `attachDescribed`), and returns it as this side's session. The offer's sizes are the
/// peer's, so they are checked first. `received`: the segment's handle, where it travels with OFFER (the caller closes
/// it afterwards, whatever the result), else null.
pub fn attach(received: ?platform.Handle, offer: *const frames.Offer) shared_memory.AttachError!Session {
    if (!valid(offer.capacity, offer.max_producers, offer.max_consumers)) return shared_memory.rejected("the offered capacity, max_producers or max_consumers is out of range");
    var expected = describe(offer.capacity, offer.max_producers, offer.max_consumers);
    expected.session_id = offer.session_id;
    return shared_memory.attachDescribed(wire_protocol.QueueInfo, .queue, received, expected, offer.segment_size);
}

// === The ring set ===

/// The kind suffixes of a slot's wake (Windows: `Local\fastipc-<sid>-mq-producer-<p>`, `names.slotObject`).
pub const producer_wake_suffix = "-mq-producer-";
pub const consumer_wake_suffix = "-mq-consumer-";

/// The queue's ring set (core/ring_set.zig): the producers' rings, then the return ring.
pub const Rings = ring_set.RingSet(struct {
    pub const Header = wire_protocol.QueueHeader;
    pub const TakerSlot = wire_protocol.QueueConsumerSlot;
    pub const RingControl = wire_protocol.QueueRingControl;
    pub const Cell = wire_protocol.QueueCell;
    pub const Role = wire_protocol.QueueRole;
    pub const sender_role: wire_protocol.QueueRole = .producer;
    pub const taker_role: wire_protocol.QueueRole = .consumer;
    pub const max_senders = wire_protocol.max_queue_producers;
    pub const max_takers = wire_protocol.max_queue_consumers;
    /// The return ring.
    pub const extra_rings = 1;
    pub const piece_divisor: ?usize = wire_protocol.queue_piece_divisor;
    pub const replies = false;
    pub const cell_spread = wire_protocol.queue_cell_spread;
    pub const sender_wake_suffix = producer_wake_suffix;
    pub const taker_wake_suffix = consumer_wake_suffix;
    pub const room_wake_suffix: ?[]const u8 = null;
    pub const noun = "work queue";
    pub const published_step = .queue_published;
    pub const claimed_step = .queue_claimed;

    /// A publish's store of `flags`, always 0.
    pub inline fn clearExtra(cell: *Cell) void {
        cell.flags.store(0, .monotonic);
    }
});

comptime {
    std.debug.assert(@sizeOf(Rings.Header) == wire_protocol.queue_header_size and @sizeOf(Rings.RingControl) == wire_protocol.queue_ring_control_size);
}

/// A member's role, its wire value (`fipc_queue_join`'s, and JOIN's and WELCOME's).
pub const Role = Rings.Role;

/// A member's slot: its role, and its index among that role's slots (a producer's is its ring's number).
pub const Slot = Rings.Slot;

/// A set of slots of one role: bit s, slot s.
pub const Slots = Rings.Slots;

/// Where a queue segment's parts are in this process's mapping (core/ring_set.zig `Layout`: its senders are the
/// producers, its takers the consumers).
pub const Layout = Rings.Layout;

/// A member's handles to the other slots' wakes (Windows).
pub const Wakes = Rings.Wakes;

/// The return ring: the one past the producers'.
pub fn returnRing(layout: *const Layout) u32 {
    return layout.max_senders;
}

// === Tests (real OS objects, in this process; more in producer_consumer_test.zig and litmus_test.zig) ===

const testing = std.testing;
const page = 4096;

test "a queue segment's sizes: the header and the consumers' slots, then each ring's control, cells and data, page by page" {
    const small = describe(4096, 1, 1);
    try testing.expectEqual(@as(u32, 64), small.cell_count);
    try testing.expectEqual(@as(u64, 512), small.consumers_offset);
    try testing.expectEqual(@as(u64, 4096), small.producers_offset);
    try testing.expectEqual(@as(u64, 512), small.cells_offset);
    try testing.expectEqual(@as(u64, 4096), small.data_offset);
    try testing.expectEqual(@as(u64, 8192), small.producer_stride);
    try testing.expectEqual(@as(u64, 4096 + 2 * 8192), small.segment_size); // a producer's ring and the return ring

    const mid = describe(1 << 20, 8, 8);
    try testing.expectEqual(@as(u64, 4096), mid.producers_offset); // 512 + 8 slots of 128
    try testing.expectEqual(@as(u64, 528384), mid.data_offset); // 512 + 16384 cells of 32, rounded up to a page
    try testing.expectEqual(@as(u64, 4096 + 9 * (528384 + (1 << 20))), mid.segment_size);

    const big = describe(1 << 31, 63, 64);
    try testing.expectEqual(@as(u64, 12288), big.producers_offset); // 512 + 64 slots of 128, rounded up
    try testing.expectEqual(@as(u32, 1 << 25), big.cell_count);
    try testing.expectEqual(@as(u64, (1 << 30) + page), big.data_offset);
    try testing.expectEqual(@as(u64, 12288 + 64 * ((1 << 30) + page + (1 << 31))), big.segment_size);
    for ([_]wire_protocol.QueueInfo{ small, mid, big, describe(8192, 3, 33) }) |described| {
        try testing.expectEqual(@as(u64, 0), described.producers_offset % page);
        try testing.expectEqual(@as(u64, 0), described.data_offset % page);
        try testing.expectEqual(@as(u64, 0), described.producer_stride % page);
        try testing.expectEqual(@as(u64, 0), described.segment_size % page);
    }

    try testing.expect(valid(4096, 1, 1) and valid(1 << 31, 63, 64));
    try testing.expect(!valid(2048, 1, 1) and !valid(4097, 1, 1) and !valid(1 << 32, 1, 1));
    try testing.expect(!valid(4096, 0, 1) and !valid(4096, 64, 1) and !valid(4096, 1, 0) and !valid(4096, 1, 65));

    // The rings' bits of `ready`: one per producer and the return ring's, all 64 with 63 producers
    var header: wire_protocol.QueueHeader align(64) = undefined;
    const one: Layout = .init(@ptrCast(&header), 4096, 1, 1);
    try testing.expectEqual(@as(u32, 1), returnRing(&one));
    try testing.expectEqual(@as(u64, 0b11), one.ringMask());
    const most: Layout = .init(@ptrCast(&header), 4096, 63, 64);
    try testing.expectEqual(@as(u32, 64), most.ringCount());
    try testing.expectEqual(~@as(u64, 0), most.ringMask());
}

/// An owner's queue segment, and offers of it, in one process.
const QueuePair = struct {
    created: Session,
    handle: ?platform.Handle,
    security: platform.Security,
    max_producers: u32,
    max_consumers: u32,

    fn init(capacity: u32, max_producers: u32, max_consumers: u32) !QueuePair {
        var security = try platform.Security.init();
        errdefer security.deinit();
        const handle: ?platform.Handle = if (platform.passes_segment_handle) try platform.createSegmentHandle() else null;
        errdefer if (handle) |segment_handle| platform.close(segment_handle);
        return .{
            .created = try create(testing.io, handle, &security, capacity, max_producers, max_consumers),
            .handle = handle,
            .security = security,
            .max_producers = max_producers,
            .max_consumers = max_consumers,
        };
    }

    fn offer(pair: *const QueuePair) frames.Offer {
        return .{
            .capacity = pair.created.capacity,
            .segment_size = describe(pair.created.capacity, pair.max_producers, pair.max_consumers).segment_size,
            .session_id = pair.created.id,
            .max_producers = pair.max_producers,
            .max_consumers = pair.max_consumers,
        };
    }

    fn deinit(pair: *QueuePair) void {
        pair.created.unmap();
        pair.security.deinit();
        if (pair.handle) |segment_handle| platform.close(segment_handle);
    }
};

test "a new queue segment has its description, zeroes elsewhere, and a member's attach sees the same memory, writable" {
    var pair = try QueuePair.init(4096, 2, 3);
    defer pair.deinit();
    const description = info(&pair.created);
    try testing.expectEqualSlices(u8, "FIPCQUE1", std.mem.asBytes(&description.magic));
    try testing.expectEqual(@as(u32, 512), description.header_size);
    try testing.expectEqual(@as(u32, 4096), description.capacity);
    try testing.expectEqual(@as(u32, 2), description.max_producers);
    try testing.expectEqual(@as(u32, 3), description.max_consumers);
    try testing.expectEqual(@as(u64, 4096 + 3 * 8192), description.segment_size); // two producers' rings, the return ring
    try testing.expectEqual(pair.created.mapping.size, description.segment_size);
    try testing.expect(!std.mem.allEqual(u8, &description.session_id, 0));
    const base = pair.created.mapping.base;
    try testing.expect(std.mem.allEqual(u8, base[@sizeOf(wire_protocol.QueueInfo)..description.segment_size], 0));

    const offered = pair.offer();
    var attached = try attach(pair.handle, &offered);
    defer attached.unmap();
    try testing.expect(!attached.server and attached.capacity == 4096);
    const mine: Layout = .of(&pair.created, 2, 3);
    const theirs: Layout = .of(&attached, 2, 3);
    mine.ring(1).tail.store(7, .monotonic);
    try testing.expectEqual(@as(u64, 7), theirs.ring(1).tail.load(.monotonic));
    theirs.cell(1, 70).state.store(ring_set.published(70), .monotonic); // cell 6 of ring 1
    try testing.expectEqual(ring_set.published(70), mine.cell(1, 6).state.load(.monotonic));
    theirs.bytes(1, 4096 + 100, 3)[0] = 0xab;
    try testing.expectEqual(@as(u8, 0xab), mine.bytes(1, 100, 1)[0]);
    // Every part of a member's view is writable
    for ([_]u64{ 0, 512, 4096, 4096 + 512, 8192, description.segment_size - 1 }) |at| {
        try testing.expectEqual(platform.Protection.read_write, platform.protectionOf(attached.mapping.base + at));
    }
}

test "a member's attach refuses an offer out of range or inconsistent, a description that doesn't match it, and a broadcast segment" {
    var pair = try QueuePair.init(8192, 2, 2);
    defer pair.deinit();
    const description = info(&pair.created);
    var offered = pair.offer();
    offered.segment_size += page;
    try testing.expectError(error.Rejected, attach(pair.handle, &offered));
    offered = pair.offer();
    offered.capacity = 3000;
    try testing.expectError(error.Rejected, attach(pair.handle, &offered));
    offered = pair.offer();
    offered.max_producers = 0;
    try testing.expectError(error.Rejected, attach(pair.handle, &offered));
    offered = pair.offer();
    offered.max_consumers = 65;
    try testing.expectError(error.Rejected, attach(pair.handle, &offered));
    offered = pair.offer();
    offered.max_producers = 64;
    offered.segment_size = describe(offered.capacity, 64, offered.max_consumers).segment_size;
    try testing.expectError(error.Rejected, attach(pair.handle, &offered));
    offered = pair.offer();
    offered.capacity = std.math.maxInt(u32);
    try testing.expectError(error.Rejected, attach(pair.handle, &offered));
    offered = pair.offer();
    offered.max_consumers = 1; // the same size: the description tells
    try testing.expectError(error.Rejected, attach(pair.handle, &offered));
    offered = pair.offer();
    offered.session_id[3] +%= 1;
    try testing.expectError(if (platform.passes_segment_handle) error.Rejected else error.Gone, attach(pair.handle, &offered));

    // Each field of the description
    offered = pair.offer();
    const words = std.mem.bytesAsSlice(u32, std.mem.asBytes(description)[0..@offsetOf(wire_protocol.QueueInfo, "reserved")]);
    for (words) |*word| {
        const saved = word.*;
        word.* = saved ^ 0x10;
        try testing.expectError(error.Rejected, attach(pair.handle, &offered));
        word.* = saved;
    }
    // A broadcast channel's magic
    const saved = description.*;
    description.magic = wire_protocol.broadcast_magic;
    try testing.expectError(error.Rejected, attach(pair.handle, &offered));
    description.* = saved;
    // The reserved bytes are not checked
    description.reserved[31] = 0xff;
    var again = try attach(pair.handle, &offered);
    again.unmap();
}
