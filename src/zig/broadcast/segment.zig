//! A broadcast channel's segment (docs/protocol.md §9.4): its owner creates it once for the channel (`create`)
//! and every subscriber maps it (`attach`). It holds a description (`wire_protocol.BroadcastInfo`, where its parts are),
//! the header the writer publishes, the cells, the data ring, then what subscribers write: the shared block (the armed
//! and gated bitmaps, the sleep words) and a slot per subscriber. `Layout` is where those parts are in one process's
//! mapping, which the writer (writer.zig), the subscribers (reader.zig) and the owner (server.zig) work on.
//!
//! A subscriber's view has what only the writer writes (header, cells, data ring) read-only (`attach`):
//! nothing on a subscriber's side writes there. A corrupt peer can still write anything in the segment (a process of
//! the user can map it as it likes): whatever is read from it and indexes or sizes anything is checked where it is
//! read.
//!
//! As every segment (core/shared_memory.zig), it is created and attached without a lock.

const std = @import("std");
const builtin = @import("builtin");
const Io = std.Io;
const wire_protocol = @import("../wire_protocol.zig");
const frames = @import("frames.zig");
const names = @import("../core/names.zig");
const platform = @import("../platform.zig");
const shared_memory = @import("../core/shared_memory.zig");

const Atomic = std.atomic.Value;
const Session = shared_memory.Session;

/// The description of a broadcast segment for a data ring of `capacity` bytes and room for `max_subscribers`
/// subscribers (docs/protocol.md §9.4), its flags and session id 0: where its parts are. The header (512 bytes) and the
/// cells (one per 64 bytes of data ring, 32 bytes each), the data ring from the next 64 KiB boundary, then from the
/// next one after it the words subscribers write: the shared block (1024 bytes) and a 128-byte slot per subscriber; the
/// size is whole 4 KiB pages. What only the writer writes, `[0, shared_offset)`, is whole 64 KiB blocks, so that it can
/// be mapped read-only on its own on every supported system: 64 KiB is Windows' view granularity and a multiple of
/// every page size (4 KiB, 16 KiB).
pub fn describe(capacity: u32, max_subscribers: u32) wire_protocol.BroadcastInfo {
    const cell_count: u32 = @intCast(capacity / wire_protocol.broadcast_payload_align);
    const cells_offset: u64 = wire_protocol.broadcast_header_size;
    const data_offset = std.mem.alignForward(u64, cells_offset + @as(u64, cell_count) * @sizeOf(wire_protocol.BroadcastCell), block);
    const shared_offset = std.mem.alignForward(u64, data_offset + capacity, block);
    const slots_offset = shared_offset + @sizeOf(wire_protocol.BroadcastShared);
    return .{
        .magic = wire_protocol.broadcast_magic,
        .header_size = wire_protocol.broadcast_header_size,
        .capacity = capacity,
        .cell_count = cell_count,
        .max_subscribers = max_subscribers,
        .flags = 0,
        .session_id = @splat(0),
        .cells_offset = cells_offset,
        .data_offset = data_offset,
        .shared_offset = shared_offset,
        .slots_offset = slots_offset,
        .segment_size = std.mem.alignForward(u64, slots_offset + @as(u64, max_subscribers) * @sizeOf(wire_protocol.BroadcastSlot), page),
        .reserved = @splat(0),
    };
}

const page = 4096;
const block = 64 * 1024;

/// The kind suffix of a subscriber's slot wake (Windows: `Local\fastipc-<sid>-bc-wake-<slot>`, `names.slotObject`).
pub const slot_wake_suffix = "-bc-wake-";

comptime {
    std.debug.assert(slot_wake_suffix.len <= names.slot_suffix_max);
}

/// A channel's capacity, a power of two from 4 KiB to 2 GiB, and its subscribers, 1 to 1024.
pub fn valid(capacity: u64, max_subscribers: u64) bool {
    return capacity >= wire_protocol.broadcast_min_capacity and capacity <= 1 << 31 and std.math.isPowerOfTwo(capacity) and
        max_subscribers >= 1 and max_subscribers <= wire_protocol.max_channel_subscribers;
}

/// A channel session's description, at the start of its segment.
pub fn info(session: *const Session) *wire_protocol.BroadcastInfo {
    return @ptrCast(@alignCast(session.mapping.base));
}

/// A new broadcast channel's segment under a new random id (core/shared_memory.zig `createUnderRandomId`), and its
/// description written; everything else stays zero, which is an empty channel. `handle`: the segment's handle, where it
/// travels with OFFER (created and registered by the caller), else null.
pub fn create(
    io: Io,
    handle: ?platform.Handle,
    security: *const platform.Security,
    capacity: u32,
    max_subscribers: u32,
    gated_allowed: bool,
) shared_memory.CreateError!Session {
    std.debug.assert(valid(capacity, max_subscribers));
    var description = describe(capacity, max_subscribers);
    description.flags = if (gated_allowed) wire_protocol.offer_flag_gated else 0;
    const id, const mapping = try shared_memory.createUnderRandomId(io, handle, description.segment_size, security, .broadcast);
    const session: Session = .{ .id = id, .capacity = capacity, .server = true, .mapping = mapping };
    description.session_id = id;
    info(&session).* = description;
    return session;
}

/// A subscriber's attach (docs/protocol.md §9): maps the offered channel segment, checks its description against the
/// offer (core/shared_memory.zig `attachDescribed`), makes what only the writer writes (`[0, shared_offset)`: the
/// header, the cells, the data ring) read-only in this view, so that a stray write of this process faults instead of
/// corrupting every subscriber's messages, and returns it as this side's session. The offer's sizes and slot are the
/// peer's, so they are checked first. `received`: the segment's handle, where it travels with OFFER (the caller closes
/// it afterwards, whatever the result), else null.
pub fn attach(received: ?platform.Handle, offer: *const frames.Offer) shared_memory.AttachError!Session {
    if (!valid(offer.capacity, offer.max_subscribers)) return shared_memory.rejected("the offered capacity or max_subscribers is out of range");
    if (offer.slot >= offer.max_subscribers) return shared_memory.rejected("the offered slot is out of range");
    var expected = describe(offer.capacity, offer.max_subscribers);
    expected.flags = offer.flags;
    expected.session_id = offer.session_id;
    var session = try shared_memory.attachDescribed(wire_protocol.BroadcastInfo, .broadcast, received, expected, offer.segment_size);
    platform.protectReadOnly(&session.mapping, expected.shared_offset) catch {
        session.unmap();
        return error.Rejected; // logged where unexpected; a mapping count at its limit is retried as exhaustion is
    };
    return session;
}

// === The mapping ===

/// Where a channel segment's parts are in this process's mapping, and its sizes: this process's own copies, never read
/// again from the segment's description.
pub const Layout = struct {
    header: *wire_protocol.BroadcastHeader,
    cells: [*]wire_protocol.BroadcastCell,
    data: [*]u8,
    shared: *wire_protocol.BroadcastShared,
    slots: [*]wire_protocol.BroadcastSlot,
    /// Bytes of the data ring: a power of two.
    capacity: u64,
    max_subscribers: u32,
    /// What wakes the writer sleeping for room, besides its flag (Windows: the session's `broadcast_room` event).
    room_event: platform.RingEvent,

    /// The layout of a channel segment of `capacity` bytes and `max_subscribers` slots mapped at `base`.
    pub fn init(base: [*]u8, capacity: u32, max_subscribers: u32, room_event: platform.RingEvent) Layout {
        std.debug.assert(std.math.isPowerOfTwo(capacity));
        const sizes = describe(capacity, max_subscribers);
        return .{
            .header = @ptrCast(@alignCast(base)),
            .cells = @ptrCast(@alignCast(base + sizes.cells_offset)),
            .data = base + sizes.data_offset,
            .shared = @ptrCast(@alignCast(base + sizes.shared_offset)),
            .slots = @ptrCast(@alignCast(base + sizes.slots_offset)),
            .capacity = capacity,
            .max_subscribers = max_subscribers,
            .room_event = room_event,
        };
    }

    /// The layout of a channel's session, the owner's or a subscriber's.
    pub fn of(session: *const Session, max_subscribers: u32) Layout {
        return .init(session.mapping.base, session.capacity, max_subscribers, session.mapping.ringEvent(.broadcast_room));
    }

    pub fn cellCount(layout: *const Layout) u64 {
        return layout.capacity / wire_protocol.broadcast_payload_align;
    }

    /// The words of a slot bitmap that hold its `max_subscribers` slots.
    pub fn bitmapWords(layout: *const Layout) u32 {
        return (layout.max_subscribers + 63) / 64;
    }

    /// Message `index`'s cell.
    pub fn cell(layout: *const Layout, index: u64) *wire_protocol.BroadcastCell {
        return &layout.cells[@intCast(index & (layout.cellCount() - 1))];
    }

    pub fn maxPiece(layout: *const Layout) usize {
        return maxPieceOf(layout.capacity);
    }

    /// The offset in the data ring of position `payload_position`: the position modulo the capacity, a power of two.
    pub inline fn offset(layout: *const Layout, payload_position: u64) u64 {
        return payload_position & (layout.capacity - 1);
    }

    /// Whether a cell's payload fields describe a payload of the channel: 1 to `maxPiece` bytes, within the ring.
    pub fn validPiece(layout: *const Layout, payload_position: u64, len: u32) bool {
        return len != 0 and len <= layout.maxPiece() and layout.offset(payload_position) + len <= layout.capacity;
    }

    /// The `len` bytes of the data ring at position `payload_position` (checked by `validPiece`).
    pub fn bytes(layout: *const Layout, payload_position: u64, len: usize) []u8 {
        return layout.data[@intCast(layout.offset(payload_position))..][0..len];
    }

    /// Requests, for writing, the cache lines of the `len` bytes of the data ring at position `payload_position`
    /// (placed by the writer, within the ring).
    pub inline fn prefetchPayload(layout: *const Layout, payload_position: u64, len: usize) void {
        const start = layout.data + @as(usize, @intCast(layout.offset(payload_position)));
        var line: usize = 0;
        while (line < len) : (line += 64)
            @prefetch(start + line, .{ .rw = .write, .locality = 3, .cache = .data });
    }

    /// The writer's sleep flag while it waits for room.
    pub fn writerSleeps(layout: *const Layout) *Atomic(u32) {
        return &layout.shared.writer_sleeps;
    }
};

/// `fipc_max_piece` of the connections of a channel of `capacity` bytes: a quarter of the capacity.
pub fn maxPieceOf(capacity: u64) usize {
    return @intCast(capacity / wire_protocol.broadcast_piece_divisor);
}

/// Whether, after a message of `len` bytes, the writer requests the next message's lines ahead (writer.zig
/// `prefetchNext`): for payloads whose room is 256 bytes to
/// 4 KiB, as the stream's frames (stream/data_path.zig `prefetchNextFrame`), and on x86-64 only, as the stream's are.
pub inline fn prefetchesNext(len: usize) bool {
    if (builtin.cpu.arch != .x86_64) return false;
    const room = std.mem.alignForward(usize, len, wire_protocol.broadcast_payload_align);
    return room >= 256 and room <= 4096;
}

/// Bit `slot % 64`, of word `slot / 64` of a bitmap.
pub fn bitOf(slot: u32) u64 {
    return @as(u64, 1) << @intCast(slot % 64);
}

/// A slot bitmap: bit `slot % 64` of word `slot / 64` is slot `slot`'s. Each access takes the ordering its site needs.
pub const SlotBits = [wire_protocol.max_channel_subscribers / 64]Atomic(u64);

pub fn setBit(bits: *SlotBits, slot: u32, comptime order: std.lang.AtomicOrder) void {
    _ = bits[slot / 64].fetchOr(bitOf(slot), order);
}

pub fn clearBit(bits: *SlotBits, slot: u32, comptime order: std.lang.AtomicOrder) void {
    _ = bits[slot / 64].fetchAnd(~bitOf(slot), order);
}

pub fn hasBit(bits: *const SlotBits, slot: u32, comptime order: std.lang.AtomicOrder) bool {
    return bits[slot / 64].load(order) & bitOf(slot) != 0;
}

// === The sleep words ===

/// Wakes the writer sleeping for room, if it sleeps: whoever swaps its flag's 1 back to 0 wakes it.
pub fn wakeWriter(layout: *const Layout) void {
    const flag = layout.writerSleeps();
    if (flag.swap(0, .acq_rel) == 1) platform.ringWake(flag, layout.room_event);
}

/// Wakes every subscriber sleeping on `wake_gen` (Linux, macOS): the bump, then the wake of all its waiters.
pub fn wakeAll(shared: *wire_protocol.BroadcastShared) void {
    _ = shared.wake_gen.fetchAdd(1, .seq_cst);
    platform.broadcastWake(&shared.wake_gen);
}

// === Tests (real OS objects, in this process) ===

const testing = std.testing;

test "a broadcast segment's sizes: the header, the cells, the data ring and the shared block each from 64 KiB, then the slots" {
    const small = describe(4096, 1);
    try testing.expectEqual(@as(u32, 64), small.cell_count);
    try testing.expectEqual(@as(u64, 512), small.cells_offset);
    try testing.expectEqual(@as(u64, 65536), small.data_offset);
    try testing.expectEqual(@as(u64, 131072), small.shared_offset); // the data ring's 4 KiB, rounded up to 64 KiB
    try testing.expectEqual(@as(u64, 132096), small.slots_offset);
    try testing.expectEqual(@as(u64, 135168), small.segment_size);

    const mid = describe(64 * 1024, 1024);
    try testing.expectEqual(@as(u64, 65536), mid.data_offset); // 512 + 1024 cells of 32 bytes, rounded up to 64 KiB
    try testing.expectEqual(@as(u64, 131072), mid.shared_offset);
    try testing.expectEqual(@as(u64, 266240), mid.segment_size); // + 1024 + 128 KiB, rounded up to a page

    const big = describe(1 << 31, 1024);
    try testing.expectEqual(@as(u32, 1 << 25), big.cell_count);
    try testing.expectEqual(@as(u64, (1 << 30) + block), big.data_offset);
    try testing.expectEqual(big.data_offset + (1 << 31), big.shared_offset);
    try testing.expectEqual(big.shared_offset + 33 * page, big.segment_size);
    // The writer's part is whole 64 KiB blocks, the subscribers' starts on one
    for ([_]wire_protocol.BroadcastInfo{ small, mid, big }) |described| {
        try testing.expectEqual(@as(u64, 0), described.data_offset % block);
        try testing.expectEqual(@as(u64, 0), described.shared_offset % block);
        try testing.expectEqual(@as(u64, 0), described.segment_size % page);
    }

    try testing.expect(valid(4096, 1) and valid(1 << 31, 1024));
    try testing.expect(!valid(2048, 1) and !valid(4097, 1) and !valid(1 << 32, 1));
    try testing.expect(!valid(4096, 0) and !valid(4096, 1025));
}

/// An owner's channel segment and a subscriber's attach of it, in one process.
const BroadcastPair = struct {
    created: Session,
    handle: ?platform.Handle,
    security: platform.Security,
    max_subscribers: u32,

    fn init(capacity: u32, max_subscribers: u32) !BroadcastPair {
        var security = try platform.Security.init();
        errdefer security.deinit();
        const handle: ?platform.Handle = if (platform.passes_segment_handle) try platform.createSegmentHandle() else null;
        errdefer if (handle) |h| platform.close(h);
        return .{ .created = try create(testing.io, handle, &security, capacity, max_subscribers, true), .handle = handle, .security = security, .max_subscribers = max_subscribers };
    }

    fn offer(p: *const BroadcastPair) frames.Offer {
        return .{
            .flags = wire_protocol.offer_flag_gated,
            .capacity = p.created.capacity,
            .segment_size = describe(p.created.capacity, p.max_subscribers).segment_size,
            .session_id = p.created.id,
            .slot = p.max_subscribers - 1,
            .generation = 1,
            .max_subscribers = p.max_subscribers,
        };
    }

    fn deinit(p: *BroadcastPair) void {
        p.created.unmap();
        p.security.deinit();
        if (p.handle) |h| platform.close(h);
    }
};

test "a new channel segment has its description, zeroes elsewhere, and a subscriber's attach sees the same memory" {
    var pair = try BroadcastPair.init(4096, 3);
    defer pair.deinit();
    const description = info(&pair.created);
    try testing.expectEqualSlices(u8, "FIPCPUB1", std.mem.asBytes(&description.magic));
    try testing.expectEqual(@as(u32, 512), description.header_size);
    try testing.expectEqual(@as(u32, 4096), description.capacity);
    try testing.expectEqual(@as(u32, 64), description.cell_count);
    try testing.expectEqual(@as(u32, 3), description.max_subscribers);
    try testing.expectEqual(@as(u32, wire_protocol.offer_flag_gated), description.flags);
    try testing.expectEqual(@as(u64, 135168), description.segment_size);
    try testing.expectEqual(pair.created.mapping.size, description.segment_size);
    const base = pair.created.mapping.base;
    try testing.expect(std.mem.allEqual(u8, base[@sizeOf(wire_protocol.BroadcastInfo)..description.segment_size], 0));

    const o = pair.offer();
    var attached = try attach(pair.handle, &o);
    defer attached.unmap();
    try testing.expect(!attached.server and attached.capacity == 4096);
    const shared: *wire_protocol.BroadcastShared = @ptrCast(@alignCast(base + description.shared_offset));
    shared.wake_gen.store(5, .monotonic);
    const theirs: *wire_protocol.BroadcastShared = @ptrCast(@alignCast(attached.mapping.base + description.shared_offset));
    try testing.expectEqual(@as(u32, 5), theirs.wake_gen.load(.monotonic));
    // The subscriber's view: what only the writer writes is read-only, the rest read-write; the owner's all read-write
    const view = attached.mapping.base;
    for ([_]u64{ 0, description.cells_offset, description.data_offset, description.shared_offset - 1 }) |at| {
        try testing.expectEqual(platform.Protection.read_only, platform.protectionOf(view + at));
        try testing.expectEqual(platform.Protection.read_write, platform.protectionOf(base + at));
    }
    for ([_]u64{ description.shared_offset, description.slots_offset, description.segment_size - 1 }) |at| {
        try testing.expectEqual(platform.Protection.read_write, platform.protectionOf(view + at));
    }
}

test "a subscriber's attach refuses a description that doesn't match the offer, or a slot out of range" {
    var pair = try BroadcastPair.init(8192, 2);
    defer pair.deinit();
    const description = info(&pair.created);
    var o = pair.offer();
    o.slot = 2;
    try testing.expectError(error.Rejected, attach(pair.handle, &o));
    // Out of range or inconsistent offers, refused before any mapping
    o = pair.offer();
    o.segment_size += page;
    try testing.expectError(error.Rejected, attach(pair.handle, &o));
    o = pair.offer();
    o.capacity = 3000;
    try testing.expectError(error.Rejected, attach(pair.handle, &o));
    o = pair.offer();
    o.max_subscribers = 0;
    try testing.expectError(error.Rejected, attach(pair.handle, &o));
    o = pair.offer();
    o.max_subscribers = 1025;
    try testing.expectError(error.Rejected, attach(pair.handle, &o));
    o = pair.offer();
    o.flags = 0;
    try testing.expectError(error.Rejected, attach(pair.handle, &o));
    o = pair.offer();
    o.session_id[3] +%= 1;
    try testing.expectError(if (platform.passes_segment_handle) error.Rejected else error.Gone, attach(pair.handle, &o));

    // Each field of the description
    o = pair.offer();
    const words = std.mem.bytesAsSlice(u32, std.mem.asBytes(description)[0..@offsetOf(wire_protocol.BroadcastInfo, "reserved")]);
    for (words) |*word| {
        const saved = word.*;
        word.* = saved ^ 0x10;
        try testing.expectError(error.Rejected, attach(pair.handle, &o));
        word.* = saved;
    }
    // The reserved bytes are not checked
    description.reserved[39] = 0xff;
    var again = try attach(pair.handle, &o);
    again.unmap();
}
