//! A work queue's returns (docs/protocol.md §10.6): the owner's side of redelivery. When a consumer's member ends (its
//! process ended: it writes the segment no more) holding a message it never acknowledged, the owner copies that
//! message into the return ring (segment.zig: ring `max_producers`, whose only producer is the owner) and publishes it
//! there as a producer does, and only then marks the original done. Consumers take it from the return ring as from any
//! ring. The original's bytes stay valid until its done: a producer reclaims room only from done messages.
//!
//! No room. The owner never waits for room: a message of a producer's ring that doesn't fit stays claimed, pinning its
//! producer's ring, and its consumer's slot stays held (`waiting`), until a tick finds room (`tick`, every few
//! milliseconds on the owner's poller while anything waits).
//!
//! A message of the return ring itself. A dead consumer's claim there pins the return ring's own reclaim, so its copy
//! could wait for room it holds. So it moves at once into the scratch FIFO, private memory of up to `capacity` bytes,
//! and the original is done; each tick publishes the FIFO's messages into the return ring, oldest first, while there is
//! room, and copies a producer's message into the return ring only while the FIFO is empty. Then every returned byte is
//! in the return ring or the FIFO, never both, and their sum never exceeds `capacity` (each length rounded up to 64
//! bytes as the ring counts it): a move into the FIFO keeps the sum, a publish from it too, a consumer's done lowers
//! it, and new bytes come in only from a producer's ring while the FIFO is empty, if the ring has room. So a move into
//! the FIFO always fits, and what the return ring waits for is only live consumers' releases. The FIFO's memory comes
//! on demand, doubling up to `capacity`: a move whose memory can't be had leaves the claim as it is, its slot waiting,
//! for the next tick, which tries the moves first. Nothing is dropped for want of memory.
//!
//! Everything here runs under the queue's lock, on its poller (or in `close`, after the poller ended). A corrupt peer
//! can forge a claim: a forged move that doesn't fit is dropped and logged, and nothing indexes memory with a word
//! read from the segment before it is checked.

const std = @import("std");
const Allocator = std.mem.Allocator;
const wire_protocol = @import("../wire_protocol.zig");
const copy = @import("../core/copy.zig").copy;
const log = @import("../core/log.zig");
const platform = @import("../platform.zig");
const producer_module = @import("producer.zig");
const segment = @import("segment.zig");

const Claim = @import("../core/ring_set.zig").Claim;
const Layout = segment.Layout;
const Rings = segment.Rings;
const Slots = segment.Slots;

/// The owner's state of a queue's returns: the return ring's producer, the scratch FIFO, and the consumer slots whose
/// message waits for room.
pub const Returns = struct {
    layout: Layout,
    /// The return ring's producer, on the owner's mapping.
    producer: producer_module.Producer,
    /// The producer's payload record: `cell_count` entries.
    positions: []u32,
    scratch: Scratch,
    /// Bit c: consumer slot c ended holding a message of a producer's ring that waits for room in the return ring;
    /// the slot stays held until it went.
    waiting: Slots = .empty,

    /// The returns of the queue mapped by `layout`, created now: before any member, its return ring is empty. The
    /// producer's record is allocated now, the scratch FIFO's memory as it fills.
    pub fn init(gpa: Allocator, layout: Layout, session_id: [16]u8) error{OutOfMemory}!Returns {
        const positions = try gpa.alloc(u32, @intCast(layout.cellCount()));
        return .{
            .layout = layout,
            .producer = producer_module.Producer.init(layout, segment.returnRing(&layout), positions, platform.no_ring_event, session_id) catch unreachable,
            .positions = positions,
            .scratch = .{ .limit = @intCast(layout.capacity) },
        };
    }

    pub fn deinit(returns: *Returns, gpa: Allocator) void {
        returns.producer.deinit();
        gpa.free(returns.positions);
        returns.scratch.deinit();
        returns.* = undefined;
    }

    /// The owner's handles to the slots' wakes (Windows), which its returns and its rescans use.
    pub fn wakes(returns: *Returns) *segment.Wakes {
        return &returns.producer.wakes;
    }

    /// Whether a tick has work: a message in the FIFO, or a slot waiting.
    pub fn pending(returns: *const Returns) bool {
        return returns.waiting.count() != 0 or returns.scratch.count() != 0;
    }

    /// At consumer slot `consumer`'s end (it writes the segment no more): its held message, if any, is returned
    /// (the module's comment). `free`: its slot may be freed; `waiting`: the message waits for room, and the slot
    /// stays held until a tick frees it.
    pub fn recover(returns: *Returns, consumer: u32) enum { free, waiting } {
        if (returns.returnHeld(consumer) != .no_room) return .free;
        returns.waiting.set(consumer);
        return .waiting;
    }

    /// A tick: the waiting slots' moves into the FIFO (they need memory only), then the FIFO's messages published
    /// into the return ring, oldest first, while it has room, then the waiting slots' messages of producers' rings,
    /// once the FIFO is empty, while the return ring has room. The slots whose message went, to free.
    pub fn tick(returns: *Returns) Slots {
        var freed = returns.retryWaiting();
        while (returns.scratch.oldest()) |len| {
            const at = returns.producer.reserveNow(len) orelse break;
            returns.scratch.takeOldest(returns.layout.bytes(segment.returnRing(&returns.layout), at, len));
            returns.producer.publishMessage(at, len);
        }
        freed.setUnion(returns.retryWaiting());
        return freed;
    }

    /// Each waiting slot's message returned if it can be now: the slots whose message went.
    fn retryWaiting(returns: *Returns) Slots {
        var freed: Slots = .empty;
        var it = returns.waiting.iterator(.{});
        while (it.next()) |consumer| {
            if (returns.returnHeld(@intCast(consumer)) != .no_room) freed.set(consumer);
        }
        returns.waiting = returns.waiting.differenceWith(freed);
        return freed;
    }

    /// What is left at the queue's end, after its last tick: logged, and dropped with the owner's memory.
    pub fn reportLeft(returns: *const Returns) void {
        const left = returns.scratch.count() + returns.waiting.count();
        if (left != 0) log.warn(@src(), "{d} returned messages of the work queue are dropped: the queue ended before the return ring had room for them", .{left});
    }

    /// Returns the message consumer slot `consumer` holds, if any: `nothing` held (or a corrupt one, dropped),
    /// `returned`, or `no_room` (no room in the return ring, or no memory for the FIFO: the message stays as it is).
    fn returnHeld(returns: *Returns, consumer: u32) enum { nothing, returned, no_room } {
        const layout = &returns.layout;
        const claim = Rings.heldClaim(layout, consumer) orelse return .nothing;
        if (!layout.validPiece(claim.position, claim.len)) {
            log.warn(@src(), "the consumer of the work queue's slot {d} ended holding a message of ring {d} whose payload fields are a corrupt peer's: it is dropped", .{ consumer, claim.ring });
            returns.finish(consumer, claim);
            return .nothing;
        }
        const payload = layout.bytes(claim.ring, claim.position, claim.len);
        if (claim.ring == segment.returnRing(layout)) {
            returns.scratch.push(payload) catch |err| switch (err) {
                error.OutOfMemory => return .no_room,
                error.Full => log.warn(@src(), "the consumer of the work queue's slot {d} ended holding a returned message that doesn't fit the owner's scratch room: a corrupt peer's claim; it is dropped", .{consumer}),
            };
        } else {
            if (returns.scratch.count() != 0) return .no_room;
            const at = returns.producer.reserveNow(claim.len) orelse return .no_room;
            // The copy, then its publish, then the original's done (`finish`): the original's bytes are the producer's
            // again only after the done
            copy(layout.bytes(segment.returnRing(layout), at, claim.len), payload);
            returns.producer.publishMessage(at, claim.len);
        }
        returns.finish(consumer, claim);
        return .returned;
    }

    /// The original is done, and its producer woken if it sleeps for room.
    fn finish(returns: *Returns, consumer: u32, claim: Claim) void {
        if (!Rings.finishHeld(&returns.layout, consumer, claim)) {
            log.warn(@src(), "message {d} of the work queue's ring {d} changed under its return from the consumer of slot {d}: a corrupt peer's write; it may be taken twice", .{ claim.index, claim.ring, consumer });
        }
        Rings.wakeSender(&returns.layout, returns.wakes(), claim.ring);
    }
};

/// The scratch FIFO (the module's comment), in this process's memory:
///   - the payloads, up to `limit` bytes in all (the queue's capacity), each taking its length rounded up to 64 bytes
///     as a ring counts it (an entry may wrap, and is then copied in two parts);
///   - a ring of their lengths, one entry per 64 bytes of payload room.
/// The memory starts empty and doubles as the entries need it. It comes from the page allocator, which is thread-safe:
/// the owner's poller allocates here, whatever allocator the queue's creator gave.
pub const Scratch = struct {
    limit: usize,
    bytes: []u8 = &.{},
    lengths: []u32 = &.{},
    /// The entries taken so far: a running count that never wraps. The held entries are `first` to `next`, and an
    /// index into `lengths` is the count modulo its length.
    first: u64 = 0,
    /// The entries pushed so far.
    next: u64 = 0,
    /// The bytes the taken entries took: a running count. The held entries' bytes are `start` to `end`, and an index
    /// into `bytes` is the count modulo its length.
    start: u64 = 0,
    /// The bytes the pushed entries took.
    end: u64 = 0,

    const allocator = std.heap.page_allocator;

    pub fn deinit(scratch: *Scratch) void {
        allocator.free(scratch.bytes);
        allocator.free(scratch.lengths);
        scratch.* = undefined;
    }

    pub fn count(scratch: *const Scratch) u64 {
        return scratch.next - scratch.first;
    }

    /// Appends `payload`. Full past `limit`; OutOfMemory if the memory to grow can't be had (nothing changed).
    pub fn push(scratch: *Scratch, payload: []const u8) error{ Full, OutOfMemory }!void {
        const room = std.mem.alignForward(u64, payload.len, wire_protocol.queue_payload_align);
        const needed = scratch.end - scratch.start + room;
        if (needed > scratch.limit) return error.Full;
        if (needed > scratch.bytes.len or scratch.count() == scratch.lengths.len) try scratch.grow(needed);
        const at: usize = @intCast(scratch.end % scratch.bytes.len);
        const before_wrap = @min(payload.len, scratch.bytes.len - at);
        copy(scratch.bytes[at..][0..before_wrap], payload[0..before_wrap]);
        copy(scratch.bytes[0 .. payload.len - before_wrap], payload[before_wrap..]);
        scratch.lengths[@intCast(scratch.next % scratch.lengths.len)] = @intCast(payload.len);
        scratch.next += 1;
        scratch.end += room;
    }

    /// Room for `needed` bytes: twice the bytes (a page at first) until they fit, at most `limit`; the held entries
    /// move to the start of the new memory.
    fn grow(scratch: *Scratch, needed: u64) error{OutOfMemory}!void {
        var size: usize = @max(scratch.bytes.len * 2, std.heap.pageSize());
        while (size < needed) size *= 2;
        size = @min(size, scratch.limit);
        const bytes = try allocator.alloc(u8, size);
        errdefer allocator.free(bytes);
        const lengths = try allocator.alloc(u32, size / wire_protocol.queue_payload_align);
        const held: usize = @intCast(scratch.end - scratch.start);
        if (held != 0) {
            const at: usize = @intCast(scratch.start % scratch.bytes.len);
            const before_wrap = @min(held, scratch.bytes.len - at);
            copy(bytes[0..before_wrap], scratch.bytes[at..][0..before_wrap]);
            copy(bytes[before_wrap..held], scratch.bytes[0 .. held - before_wrap]);
        }
        const entries: usize = @intCast(scratch.count());
        for (lengths[0..entries], 0..) |*length, i| length.* = scratch.lengths[@intCast((scratch.first + i) % scratch.lengths.len)];
        allocator.free(scratch.bytes);
        allocator.free(scratch.lengths);
        scratch.* = .{ .limit = scratch.limit, .bytes = bytes, .lengths = lengths, .next = entries, .end = held };
    }

    /// The oldest entry's length, or null when empty.
    pub fn oldest(scratch: *const Scratch) ?u32 {
        if (scratch.count() == 0) return null;
        return scratch.lengths[@intCast(scratch.first % scratch.lengths.len)];
    }

    /// Copies the oldest entry into `into` (its length) and removes it.
    pub fn takeOldest(scratch: *Scratch, into: []u8) void {
        std.debug.assert(into.len == scratch.oldest().?);
        const at: usize = @intCast(scratch.start % scratch.bytes.len);
        const before_wrap = @min(into.len, scratch.bytes.len - at);
        copy(into[0..before_wrap], scratch.bytes[at..][0..before_wrap]);
        copy(into[before_wrap..], scratch.bytes[0 .. into.len - before_wrap]);
        scratch.first += 1;
        scratch.start += std.mem.alignForward(u64, into.len, wire_protocol.queue_payload_align);
    }
};

fn filled(comptime len: usize, byte: u8) [len]u8 {
    return @splat(byte);
}

/// Takes the oldest entry and checks it is `len` bytes of `byte`.
fn expectOldest(scratch: *Scratch, len: usize, byte: u8) !void {
    var out: [256]u8 = undefined;
    try std.testing.expectEqual(@as(?u32, @intCast(len)), scratch.oldest());
    scratch.takeOldest(out[0..len]);
    try std.testing.expect(std.mem.allEqual(u8, out[0..len], byte));
}

test "the scratch FIFO: entries in order, one across the end of its bytes in two parts; none past its limit" {
    const testing = std.testing;
    var scratch: Scratch = .{ .limit = 256 };
    defer scratch.deinit();
    try testing.expectEqual(@as(?u32, null), scratch.oldest());
    for ([_]u8{ 1, 2, 3 }) |fill| try scratch.push(&filled(64, fill));
    try testing.expectEqual(@as(usize, 256), scratch.bytes.len); // a page, cut to the limit
    try expectOldest(&scratch, 64, 1);
    try expectOldest(&scratch, 64, 2);
    // 100 bytes (room 128) at byte 192: 64 there, 36 from byte 0
    try scratch.push(&filled(100, 4));
    try scratch.push(&filled(64, 5));
    try testing.expectError(error.Full, scratch.push(&.{6}));
    try testing.expectEqual(@as(u64, 3), scratch.count());
    try expectOldest(&scratch, 64, 3);
    try expectOldest(&scratch, 100, 4);
    try expectOldest(&scratch, 64, 5);
    try testing.expectEqual(@as(?u32, null), scratch.oldest());
}

test "the scratch FIFO grows on demand: it starts empty, doubles up to its limit, and keeps its entries in order, a wrapped one too" {
    const testing = std.testing;
    const page = std.heap.pageSize();
    var scratch: Scratch = .{ .limit = 4 * page };
    defer scratch.deinit();
    try testing.expectEqual(@as(usize, 0), scratch.bytes.len);
    // A page of entries, the first two taken, then one that wraps to the start of the page
    var fill: u8 = 0;
    while (scratch.end + 64 <= page) : (fill +%= 1) try scratch.push(&filled(64, fill));
    try testing.expectEqual(page, scratch.bytes.len);
    try expectOldest(&scratch, 64, 0);
    try expectOldest(&scratch, 64, 1);
    try scratch.push(&filled(100, 0xaa)); // room 128 at the page's end: wraps
    try testing.expectEqual(page, scratch.bytes.len);
    // One more doesn't fit the page: twice the bytes, the entries moved to its start in order
    try scratch.push(&filled(200, 0xbb));
    try testing.expectEqual(2 * page, scratch.bytes.len);
    try testing.expectEqual(scratch.bytes.len / 64, scratch.lengths.len);
    var expected: u8 = 2;
    while (scratch.oldest()) |len| : (expected +%= 1) {
        if (len != 64) break;
        try expectOldest(&scratch, 64, expected);
    }
    try expectOldest(&scratch, 100, 0xaa);
    try expectOldest(&scratch, 200, 0xbb);
    try testing.expectEqual(@as(?u32, null), scratch.oldest());
    // Up to the limit, never past it
    while (scratch.end - scratch.start + 256 <= scratch.limit) try scratch.push(&filled(256, 7));
    try testing.expectEqual(scratch.limit, scratch.bytes.len);
    try testing.expectError(error.Full, scratch.push(&filled(256, 7)));
}
