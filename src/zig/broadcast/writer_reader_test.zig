//! The broadcast data path on a channel segment in this process, without the lifecycle: the owner's segment (real OS
//! objects: the mapping, and on Windows the room event and the slot wakes), its writer, and subscribers on endpoints of
//! their own, each with its own view of the segment as a subscriber attaches it (what only the writer writes
//! read-only), which tests drive from threads. Tests that play a corrupt peer write through the owner's view. The
//! publisher's end is what a watcher would publish: the subscribers' phase PEER_DEAD, then their local interrupt.
//!
//! Messages carry their index and a checksum (`Message`): every message a subscriber returns must be whole, and a gated
//! subscriber's must come one after the other.

const std = @import("std");
const builtin = @import("builtin");
const testing = std.testing;
const wire_protocol = @import("../wire_protocol.zig");
const platform = @import("../platform.zig");
const frames = @import("frames.zig");
const reader_module = @import("reader.zig");
const segment = @import("segment.zig");
const writer_module = @import("writer.zig");

const Endpoint = @import("../core/endpoint.zig").Endpoint;
const Hot = @import("../core/endpoint.zig").Hot;
const Phase = @import("../core/endpoint.zig").Phase;
const DataError = @import("../core/endpoint.zig").DataError;
const Roster = @import("roster.zig").Roster;
const Session = @import("../core/shared_memory.zig").Session;
const Atomic = std.atomic.Value;

// === The harness ===

fn newEndpoint() !*Endpoint {
    const endpoint = try testing.allocator.create(Endpoint);
    endpoint.* = .{ .hot = std.mem.zeroes(Hot), .io = testing.io, .gpa = testing.allocator, .io_ref = undefined, .handles = undefined };
    endpoint.hot.phase.store(.connected, .monotonic);
    return endpoint;
}

/// A channel as its owner sets it up, with its writer; subscribers come from `subscribe`.
pub const Channel = struct {
    security: platform.Security,
    handle: ?platform.Handle,
    session: Session,
    layout: segment.Layout,
    roster: Roster,
    payload_positions: []u32,
    writer_endpoint: *Endpoint,
    writer: writer_module.Writer,
    /// The writer's stall reports: how many, the last one's slot and generation.
    stalls: Atomic(u32) = .init(0),
    stalled_slot: Atomic(u32) = .init(std.math.maxInt(u32)),
    stalled_generation: Atomic(u32) = .init(0),
    /// A report drops the subscriber, as the owner does once it has made it let go.
    drop_on_stall: bool = true,

    pub fn create(capacity: u32, max_subscribers: u32, max_stall_ms: i32) !*Channel {
        var security = try platform.Security.init();
        errdefer security.deinit();
        const handle: ?platform.Handle = if (platform.passes_segment_handle) try platform.createSegmentHandle() else null;
        errdefer if (handle) |h| platform.close(h);
        var session = try segment.create(testing.io, handle, &security, capacity, max_subscribers, true);
        errdefer session.unmap();
        const layout: segment.Layout = .of(&session, max_subscribers);
        const payload_positions = try testing.allocator.alloc(u32, layout.cellCount());
        errdefer testing.allocator.free(payload_positions);
        @memset(payload_positions, 0); // tests that move `next` without sending find the empty ring's positions
        const writer_endpoint = try newEndpoint();
        errdefer testing.allocator.destroy(writer_endpoint);
        const channel = try testing.allocator.create(Channel);
        channel.* = .{
            .security = security,
            .handle = handle,
            .session = session,
            .layout = layout,
            .roster = .{},
            .payload_positions = payload_positions,
            .writer_endpoint = writer_endpoint,
            .writer = undefined,
        };
        channel.writer = .init(channel.layout, &channel.roster, channel.payload_positions, max_stall_ms, onStall, channel);
        return channel;
    }

    pub fn destroy(channel: *Channel) void {
        if (platform.wakes_each_slot) for (channel.roster.slots.wakes) |wake| if (wake) |handle| platform.closeSlotWake(handle);
        testing.allocator.destroy(channel.writer_endpoint);
        testing.allocator.free(channel.payload_positions);
        channel.session.unmap();
        if (channel.handle) |h| platform.close(h);
        channel.security.deinit();
        testing.allocator.destroy(channel);
    }

    fn onStall(context: *anyopaque, slot: u32, generation: u32) bool {
        const channel: *Channel = @ptrCast(@alignCast(context));
        channel.stalled_slot.store(slot, .monotonic);
        channel.stalled_generation.store(generation, .monotonic);
        _ = channel.stalls.fetchAdd(1, .seq_cst);
        if (channel.drop_on_stall) channel.drop(slot);
        return true;
    }

    /// The owner's drop of a slot's subscriber: its bit and its roster entry.
    fn drop(channel: *Channel, slot: u32) void {
        _ = channel.layout.shared.gated[slot / 64].fetchAnd(~segment.bitOf(slot), .seq_cst);
        channel.roster.free(slot);
    }

    /// A subscriber of slot `slot`, joined now: the owner's roster first, as the owner's admission does before WELCOME.
    pub fn subscribe(channel: *Channel, slot: u32, gated: bool) !*Subscriber {
        if (platform.wakes_each_slot and channel.roster.slots.wakes[slot] == null) {
            channel.roster.slots.wakes[slot] = try platform.createSlotWake(&channel.session.id, segment.slot_wake_suffix, slot, &channel.security);
        }
        channel.roster.hold(slot, channel.roster.slots.generations[slot].load(.monotonic) + 1);
        if (gated) channel.roster.holdGated(slot);
        const own: platform.SlotWake = if (platform.wakes_each_slot) channel.roster.slots.wakes[slot].? else {};
        const subscriber = try testing.allocator.create(Subscriber);
        errdefer testing.allocator.destroy(subscriber);
        const offer: frames.Offer = .{
            .flags = wire_protocol.offer_flag_gated,
            .capacity = channel.session.capacity,
            .segment_size = channel.session.mapping.size,
            .session_id = channel.session.id,
            .slot = slot,
            .generation = channel.roster.slots.generations[slot].load(.monotonic),
            .max_subscribers = channel.layout.max_subscribers,
        };
        subscriber.session = try segment.attach(channel.handle, &offer);
        errdefer subscriber.session.unmap();
        const view: segment.Layout = .of(&subscriber.session, channel.layout.max_subscribers);
        subscriber.endpoint = try newEndpoint();
        writer_module.startSlot(&channel.layout, slot, gated);
        subscriber.reader = try .init(view, slot, gated, own);
        return subscriber;
    }

    fn send(channel: *Channel, payload: []const u8, timeout_ms: c_int) DataError!void {
        return channel.writer.send(channel.writer_endpoint, payload, timeout_ms);
    }

    /// Message `seq` of `size` bytes (`Message`).
    fn sendMessage(channel: *Channel, seq: u64, size: usize, timeout_ms: c_int) DataError!void {
        var buf: [Message.max]u8 = undefined;
        Message.fill(buf[0..size], seq);
        return channel.send(buf[0..size], timeout_ms);
    }

    /// `fipc_cancel` on the writer: the request, then the writer's wake.
    fn cancelWriter(channel: *Channel) void {
        _ = channel.writer_endpoint.hot.requests.fetchOr(.{ .cancel = true }, .seq_cst);
        segment.wakeWriter(&channel.layout);
    }
};

/// A subscriber: its endpoint (status and requests) and its receive state.
pub const Subscriber = struct {
    endpoint: *Endpoint,
    reader: reader_module.Reader,
    /// The subscriber's own view of the segment.
    session: Session,

    pub fn destroy(subscriber: *Subscriber) void {
        subscriber.session.unmap();
        testing.allocator.destroy(subscriber.endpoint);
        testing.allocator.destroy(subscriber);
    }

    fn leave(subscriber: *Subscriber) void {
        subscriber.reader.leaveChannel(subscriber.endpoint);
    }

    fn recv(subscriber: *Subscriber, buf: []u8, timeout_ms: c_int) DataError![]u8 {
        var len: usize = undefined;
        try subscriber.reader.recv(subscriber.endpoint, buf, timeout_ms, &len);
        return buf[0..len];
    }

    fn recvAcquire(subscriber: *Subscriber, timeout_ms: c_int) DataError![]const u8 {
        return subscriber.reader.recvAcquire(subscriber.endpoint, timeout_ms);
    }

    fn release(subscriber: *Subscriber) void {
        subscriber.reader.release(subscriber.endpoint);
    }

    /// What the subscriber's watcher does at the publisher's end: PEER_DEAD, then the local interrupt.
    fn publisherEnded(subscriber: *Subscriber) void {
        _ = subscriber.reader.drop(subscriber.endpoint, .peer_dead);
        subscriber.reader.wakeOwn();
    }

    /// `fipc_cancel` on the subscriber.
    fn cancel(subscriber: *Subscriber) void {
        _ = subscriber.endpoint.hot.requests.fetchOr(.{ .cancel = true }, .seq_cst);
        subscriber.reader.wakeOwn();
    }
};

/// A test message: its index (8 bytes), its length (4), a checksum of the rest (4), then bytes made from the index.
const Message = struct {
    const min = 16;
    const max = 16 * 1024;

    fn fill(buf: []u8, seq: u64) void {
        std.debug.assert(buf.len >= min);
        std.mem.writeInt(u64, buf[0..8], seq, .little);
        std.mem.writeInt(u32, buf[8..12], @intCast(buf.len), .little);
        var state = seq *% 0x9e3779b97f4a7c15 +% 1;
        for (buf[min..]) |*byte| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            byte.* = @truncate(state);
        }
        std.mem.writeInt(u32, buf[12..16], sum(buf), .little);
    }

    fn sum(buf: []const u8) u32 {
        var hasher = std.hash.Wyhash.init(0x5eed);
        hasher.update(buf[0..12]);
        hasher.update(buf[min..]);
        return @truncate(hasher.final());
    }

    /// The message's index, if it is whole.
    fn check(buf: []const u8) ?u64 {
        if (buf.len < min) return null;
        if (std.mem.readInt(u32, buf[8..12], .little) != buf.len) return null;
        if (std.mem.readInt(u32, buf[12..16], .little) != sum(buf)) return null;
        return std.mem.readInt(u64, buf[0..8], .little);
    }

    /// Message `seq`'s size, from 16 to `max_piece`, most of them small.
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

fn sleepMs(ms: i64) void {
    testing.io.sleep(.fromMilliseconds(ms), .awake) catch {};
}

/// Until the writer has announced its sleep for room (`writer_sleeps`): from then on only a wake, or its slice's end,
/// lets it look again.
fn awaitWriterAsleep(channel: *const Channel) void {
    while (channel.layout.shared.writer_sleeps.load(.seq_cst) == 0) std.Thread.yield() catch {};
}

/// Until a receive of `subscriber` has begun a sleep since its count of sleeps was `since` (`Reader.sleeps`): it is
/// past every look, the ended mark's included, so only a wake ends that sleep before its time. (An armed bit tells
/// less: a receive arms before its last looks, and a bit no send took stays set.)
fn awaitAsleep(subscriber: *const Subscriber, since: u32) void {
    while (subscriber.reader.sleeps.load(.seq_cst) == since) std.Thread.yield() catch {};
}

/// The writer's waits for room sleep for a minute unless woken: a test of a wake then fails if it doesn't come.
fn longSlices(channel: *Channel) void {
    channel.writer.first_slice_ms = 60_000;
    channel.writer.longest_slice_ms = 60_000;
}

/// Byte `j` of the single-thread test's message `k`.
fn patternByte(k: u64, j: usize) u8 {
    return @truncate(k *% 31 +% j *% 7);
}

// === One thread ===

test "one message at a time, 1 byte to max_piece, copied and zero-copy, across many laps of cells and data" {
    const channel = try Channel.create(4096, 2, -1);
    defer channel.destroy();
    const lossy = try channel.subscribe(0, false);
    defer lossy.destroy();
    const gated = try channel.subscribe(1, true);
    defer gated.destroy();
    const max_piece = channel.layout.maxPiece();
    try testing.expectEqual(@as(usize, 1024), max_piece);
    // Each subscriber reads through a view whose writer-only part is read-only
    for ([_]*Subscriber{ lossy, gated }) |subscriber| {
        const view = subscriber.reader.layout;
        try testing.expect(view.cells != channel.layout.cells);
        try testing.expectEqual(platform.Protection.read_only, platform.protectionOf(view.header));
        try testing.expectEqual(platform.Protection.read_only, platform.protectionOf(view.data));
        try testing.expectEqual(platform.Protection.read_write, platform.protectionOf(view.shared));
    }
    const sizes = [_]usize{ 1, 2, 63, 64, 65, 127, 128, 500, 1000, 1023, 1024, 17, 3 };
    var buf: [1024]u8 = undefined;
    var k: u64 = 0;
    // 64 cells and 4 KiB of data: 1000 messages are over 15 laps of cells and many more of data
    while (k < 1000) : (k += 1) {
        const n = sizes[k % sizes.len];
        if (k % 3 == 0) {
            // Zero-copy: room for more than is committed
            const room = try channel.writer.sendAcquire(channel.writer_endpoint, @min(n + 100, max_piece), 0);
            try testing.expectEqual(@as(usize, 0), @intFromPtr(room.ptr) % 64);
            for (room[0..n], 0..) |*byte, j| byte.* = patternByte(k, j);
            try channel.writer.sendCommit(n);
        } else {
            for (buf[0..n], 0..) |*byte, j| byte.* = patternByte(k, j);
            try channel.send(buf[0..n], 0);
        }
        var got: [1024]u8 = undefined;
        const msg = try lossy.recv(&got, 0);
        try testing.expectEqual(n, msg.len);
        for (msg, 0..) |byte, j| try testing.expectEqual(patternByte(k, j), byte);
        const held = if (k % 2 == 0) try gated.recvAcquire(0) else try gated.recv(&got, 0);
        try testing.expectEqual(n, held.len);
        for (held, 0..) |byte, j| try testing.expectEqual(patternByte(k, j), byte);
        if (k % 4 == 0) gated.release(); // else the next receive lets it go
        try testing.expectError(error.Timeout, lossy.recv(&got, 0));
    }
    try testing.expectEqual(k, channel.layout.header.next.load(.monotonic));
    try testing.expectEqual(k, gated.reader.pos + @intFromBool(gated.reader.acquired));
}

test "the writer's calls refuse an empty message, one over max_piece, and a commit without its reservation" {
    const channel = try Channel.create(8192, 1, -1);
    defer channel.destroy();
    const subscriber = try channel.subscribe(0, false);
    defer subscriber.destroy();
    const max_piece = channel.layout.maxPiece();
    var big: [2049]u8 = @splat(1);
    try testing.expectError(error.Invalid, channel.send(big[0..0], 0));
    try testing.expectError(error.TooLarge, channel.send(big[0 .. max_piece + 1], 0));
    try testing.expectError(error.TooLarge, channel.writer.sendAcquire(channel.writer_endpoint, max_piece + 1, 0));
    try testing.expectError(error.Invalid, channel.writer.sendAcquire(channel.writer_endpoint, 0, 0));
    try testing.expectError(error.Invalid, channel.writer.sendCommit(1));
    _ = try channel.writer.sendAcquire(channel.writer_endpoint, 100, 0);
    try testing.expectError(error.Invalid, channel.writer.sendCommit(0));
    try testing.expectError(error.Invalid, channel.writer.sendCommit(101));
    try channel.writer.sendCommit(100); // the reservation stayed
    try testing.expectError(error.Invalid, channel.writer.sendCommit(100)); // and is gone
    // A send drops a reservation
    _ = try channel.writer.sendAcquire(channel.writer_endpoint, 10, 0);
    try channel.send(big[0..max_piece], 0);
    try testing.expectError(error.Invalid, channel.writer.sendCommit(10));
    var got: [2048]u8 = undefined;
    try testing.expectEqual(@as(usize, 100), (try subscriber.recv(&got, 0)).len);
    // A buffer too small: TooLarge with the length, and the message stays
    var len: usize = 0;
    try testing.expectError(error.TooLarge, subscriber.reader.recv(subscriber.endpoint, got[0..10], 0, &len));
    try testing.expectEqual(max_piece, len);
    try testing.expectEqual(max_piece, (try subscriber.recv(&got, 0)).len);
}

test "a lossy subscriber lapped gets Overrun once, then whole messages, in order, from a recent one" {
    const channel = try Channel.create(4096, 1, -1);
    defer channel.destroy();
    const subscriber = try channel.subscribe(0, false);
    defer subscriber.destroy();
    const cell_count = channel.layout.cellCount();
    const total = 3 * cell_count + 5;
    for (0..total) |k| try channel.sendMessage(k, Message.sizeOf(k, 48), 0);
    var buf: [Message.max]u8 = undefined;
    try testing.expectError(error.Overrun, subscriber.recv(&buf, 0));
    var prev: ?u64 = null;
    while (true) {
        const msg = subscriber.recv(&buf, 0) catch |err| {
            try testing.expectEqual(error.Timeout, err);
            break;
        };
        const seq = Message.check(msg) orelse return error.TornMessage;
        if (prev) |previous| try testing.expectEqual(previous + 1, seq) else try testing.expect(seq + cell_count >= total);
        prev = seq;
    }
    try testing.expectEqual(@as(?u64, total - 1), prev);
    // And on with the stream
    try channel.sendMessage(total, 40, 0);
    try testing.expectEqual(@as(?u64, total), Message.check(try subscriber.recv(&buf, 0)));
}

test "a data lap without a cell lap is an overrun too: the frontier tells" {
    // 256 cells, 4 KiB messages: the data ring laps every 4 messages
    const channel = try Channel.create(16 * 1024, 1, -1);
    defer channel.destroy();
    const subscriber = try channel.subscribe(0, false);
    defer subscriber.destroy();
    for (0..5) |k| try channel.sendMessage(k, 4096, 0);
    var buf: [Message.max]u8 = undefined;
    // Message 0's cell is still message 0's, its payload is message 4's
    try testing.expectEqual(@as(u64, 1), channel.layout.cell(0).seq.load(.monotonic));
    try testing.expectError(error.Overrun, subscriber.recv(&buf, 0));
    const seq = Message.check(try subscriber.recv(&buf, 0)) orelse return error.TornMessage;
    try testing.expect(seq >= 1 and seq <= 4);
}

test "corrupt cells and a corrupt next are Invalid; a lossy subscriber goes on, a gated one stays" {
    const channel = try Channel.create(4096, 2, -1);
    defer channel.destroy();
    const lossy = try channel.subscribe(0, false);
    defer lossy.destroy();
    const gated = try channel.subscribe(1, true);
    defer gated.destroy();
    var buf: [1024]u8 = undefined;
    const cell = channel.layout.cell(0);
    try channel.sendMessage(0, 100, 0);
    const good_payload_position = cell.payload_position.load(.monotonic);
    const bad = [_]struct { payload_position: u64, len: u32 }{
        .{ .payload_position = good_payload_position, .len = 0 },
        .{ .payload_position = good_payload_position, .len = 1025 },
        .{ .payload_position = 4096 - 64, .len = 100 }, // past the ring's end
        .{ .payload_position = std.math.maxInt(u64), .len = 16 },
    };
    for (bad) |piece| {
        cell.payload_position.store(piece.payload_position, .monotonic);
        cell.len.store(piece.len, .monotonic);
        try testing.expectError(error.Invalid, gated.recv(&buf, 0));
        try testing.expectError(error.Invalid, gated.recvAcquire(0));
        try testing.expectEqual(@as(u64, 0), gated.reader.pos);
    }
    try testing.expectError(error.Invalid, lossy.recv(&buf, 0));
    try testing.expectEqual(@as(u64, 1), lossy.reader.pos); // went on
    cell.payload_position.store(good_payload_position, .monotonic);
    cell.len.store(100, .monotonic);
    try testing.expectEqual(@as(?u64, 0), Message.check(try gated.recv(&buf, 0)));

    // A later message in a gated subscriber's cell, which the writer can't have written
    try channel.sendMessage(1, 100, 0);
    const next_cell = channel.layout.cell(1);
    next_cell.seq.store(2 + channel.layout.cellCount(), .monotonic);
    try testing.expectError(error.Invalid, gated.recv(&buf, 0));
    next_cell.seq.store(2, .monotonic);
    try testing.expectEqual(@as(?u64, 1), Message.check(try gated.recv(&buf, 0)));
    try testing.expectEqual(@as(?u64, 1), Message.check(try lossy.recv(&buf, 0)));

    // A published `next` past a message nobody wrote: a wait that looks again finds the cell empty
    channel.layout.header.next.store(10, .seq_cst);
    try testing.expectError(error.Invalid, gated.recv(&buf, 50));
    try testing.expectError(error.Invalid, lossy.recv(&buf, 50));
}

test "a corrupt next or next_mark fails a subscriber, never past the top of its indices, and a join on it fails" {
    const channel = try Channel.create(4096, 3, -1);
    defer channel.destroy();
    const lossy = try channel.subscribe(0, false);
    defer lossy.destroy();
    const gated = try channel.subscribe(1, true);
    defer gated.destroy();
    var buf: [1024]u8 = undefined;
    try channel.sendMessage(0, 100, 0);
    _ = try lossy.recv(&buf, 0);
    _ = try gated.recv(&buf, 0);
    const header = channel.layout.header;
    const cell_count = channel.layout.cellCount();
    for ([_]u64{ std.math.maxInt(u64), 1 << 63 }) |top| {
        header.next.store(top, .seq_cst);
        header.next_mark.store(top, .seq_cst);
        // A lap in the next cell: the resync would take `next_mark`, which it refuses
        const at = lossy.reader.pos;
        channel.layout.cell(at).seq.store(at + 1 + cell_count, .release);
        try testing.expectError(error.Invalid, lossy.recv(&buf, 0));
        try testing.expectEqual(at + 1, lossy.reader.pos);
        // `next` past a cell nobody wrote, again and again: Invalid each time, a message on each time
        for (0..3) |_| try testing.expectError(error.Invalid, lossy.recv(&buf, 20));
        try testing.expect(lossy.reader.pos < 1 << 63);
        channel.layout.cell(at).seq.store(0, .release);
        try testing.expectError(error.Invalid, gated.recv(&buf, 20));
        try testing.expectEqual(@as(u64, 1), gated.reader.pos);
        // Joins on it fail, and a gated one leaves no bit behind
        for ([_]bool{ false, true }) |role_gated| {
            writer_module.startSlot(&channel.layout, 2, role_gated);
            try testing.expectError(error.Invalid, reader_module.Reader.init(lossy.reader.layout, 2, role_gated, lossy.reader.own_wake));
        }
        try testing.expectEqual(@as(u64, 0), channel.layout.shared.gated[0].load(.seq_cst) & 0b100);
    }
    // next_mark alone at the top: the overrun is Invalid too
    header.next.store(channel.writer.next, .seq_cst);
    const at = lossy.reader.pos;
    channel.layout.cell(at).seq.store(at + 1 + cell_count, .release);
    try testing.expectError(error.Invalid, lossy.recv(&buf, 0));
    channel.layout.cell(at).seq.store(0, .release);
    header.next_mark.store(0, .seq_cst);
    // The writer goes on, and the gated subscriber with it
    try channel.sendMessage(1, 100, 0);
    try testing.expectEqual(@as(?u64, 1), Message.check(try gated.recv(&buf, 0)));
}

test "the sweep: no gated subscriber frees a lap at a time; a laggard holds the writer to what it needs" {
    const channel = try Channel.create(4096, 3, -1);
    defer channel.destroy();
    const cell_count = channel.layout.cellCount();
    const writer = &channel.writer;
    // Lossy only: a lap of 64-byte messages, then a sweep that frees the next lap
    const lossy = try channel.subscribe(0, false);
    defer lossy.destroy();
    for (0..cell_count) |k| try channel.sendMessage(k, 64, 0);
    try testing.expectEqual(@as(u64, 0), writer.free_index);
    try channel.sendMessage(cell_count, 64, 0);
    try testing.expectEqual(cell_count, writer.free_index);
    try testing.expectEqual(2 * cell_count, writer.cell_limit);

    // A gated subscriber joins at cell_count + 1 and reads nothing: a lap later the writer stops, exactly
    const gated = try channel.subscribe(1, true);
    defer gated.destroy();
    for (0..cell_count) |k| try channel.sendMessage(cell_count + 1 + k, 64, 0);
    try testing.expectError(error.Timeout, channel.sendMessage(0, 64, 0));
    try testing.expectEqual(@as(u32, 1), writer.scan.slot);
    var buf: [256]u8 = undefined;
    _ = try gated.recv(&buf, 0);
    try channel.sendMessage(0, 64, 0);
    try testing.expectEqual(gated.reader.pos, writer.free_index); // the one message it let go
    try testing.expectError(error.Timeout, channel.sendMessage(0, 64, 0));
}

test "the sweep resumes at the laggard, and starts again once the need passes a cursor it saw" {
    const channel = try Channel.create(4096, 130, -1);
    defer channel.destroy();
    var subscribers: [3]*Subscriber = undefined;
    // Slots 2, 64 and 129: three bitmap words
    for (&subscribers, [_]u32{ 2, 64, 129 }) |*subscriber, slot| subscriber.* = try channel.subscribe(slot, true);
    defer for (subscribers) |subscriber| subscriber.destroy();
    const writer = &channel.writer;
    writer.next = 40; // the sweep caps with it; nothing is sent
    const cursor = struct {
        fn of(owner: *Channel, slot: u32) *Atomic(u64) {
            return &owner.layout.slots[slot].cursor;
        }
    }.of;
    cursor(channel, 2).store(10, .monotonic);
    cursor(channel, 64).store(3, .monotonic);
    cursor(channel, 129).store(12, .monotonic);
    try testing.expectEqual(@as(?u64, 3), writer.sweep(5));
    try testing.expectEqual(@as(u32, 64), writer.scan.slot);
    try testing.expectEqual(@as(u64, 10), writer.scan.least_cursor);
    // The laggard moves: the sweep goes on from it, and frees up to the least cursor
    cursor(channel, 64).store(7, .monotonic);
    try testing.expectEqual(null, writer.sweep(5));
    try testing.expectEqual(@as(u64, 7), writer.free_index);
    try testing.expectEqual(@as(u64, 7 + 64), writer.cell_limit);
    // A need past a cursor seen before starts the sweep again from the first slot
    cursor(channel, 64).store(3, .monotonic);
    try testing.expectEqual(@as(?u64, 3), writer.sweep(5));
    try testing.expectEqual(@as(u32, 64), writer.scan.slot);
    try testing.expectEqual(@as(?u64, 10), writer.sweep(11)); // 10 at slot 2 is below it now
    try testing.expectEqual(@as(u32, 2), writer.scan.slot);
    // Cursors past `next` (a corrupt peer's, or ahead) say only "past": `next` caps free_index
    for ([_]u32{ 2, 64, 129 }) |slot| cursor(channel, slot).store(1000, .monotonic);
    try testing.expectEqual(null, writer.sweep(11));
    try testing.expectEqual(@as(u64, 40), writer.free_index);
    // A gated bit in the segment the roster doesn't hold, or past max_subscribers, is not waited for
    channel.roster.free(2);
    channel.layout.shared.gated[2].store(~@as(u64, 0), .seq_cst);
    cursor(channel, 2).store(0, .monotonic);
    writer.next = 50;
    try testing.expectEqual(null, writer.sweep(45));
    try testing.expectEqual(@as(u64, 50), writer.free_index);
}

// === Threads ===

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

test "a gated subscriber holding a message in place holds the writer until it lets go; a cancel ends the wait" {
    const channel = try Channel.create(4096, 2, -1);
    defer channel.destroy();
    const gated = try channel.subscribe(0, true);
    defer gated.destroy();
    const lossy = try channel.subscribe(1, false);
    defer lossy.destroy();
    const cell_count = channel.layout.cellCount();
    for (0..cell_count) |k| try channel.sendMessage(k, 64, 0);
    const held = try gated.recvAcquire(0);
    const before = Message.check(held);
    try testing.expectEqual(@as(?u64, 0), before);
    // Without a wait: a send that slept would leave the writer's sleep flag set for the waits below
    try testing.expectError(error.Timeout, channel.sendMessage(cell_count, 64, 0));
    try testing.expectEqual(before, Message.check(held)); // untouched

    const Sender = struct {
        fn run(owner: *Channel, timeout_ms: c_int, result: *?DataError, after: *i64) void {
            const t0 = nowMs();
            result.* = if (owner.sendMessage(owner.writer.next, 64, timeout_ms)) null else |err| err;
            after.* = nowMs() - t0;
        }
    };
    var result: ?DataError = error.Invalid;
    var after: i64 = 0;
    longSlices(channel);
    var thread = try std.Thread.spawn(.{}, Sender.run, .{ channel, -1, &result, &after });
    awaitWriterAsleep(channel);
    gated.release();
    thread.join();
    try testing.expectEqual(@as(?DataError, null), result);
    try testing.expect(after < 1000);

    // The writer's room wait ends with its cancel; calls that needn't wait still work
    thread = try std.Thread.spawn(.{}, Sender.run, .{ channel, -1, &result, &after });
    awaitWriterAsleep(channel);
    channel.cancelWriter();
    thread.join();
    try testing.expectEqual(@as(?DataError, error.Cancelled), result);
    try testing.expect(after < 1000);
    var buf: [256]u8 = undefined;
    _ = try gated.recv(&buf, 0);
    try channel.sendMessage(channel.writer.next, 64, 0);
}

test "a gated subscriber's leave lets a waiting writer go at once" {
    const channel = try Channel.create(4096, 1, -1);
    defer channel.destroy();
    const gated = try channel.subscribe(0, true);
    defer gated.destroy();
    for (0..channel.layout.cellCount()) |k| try channel.sendMessage(k, 64, 0);
    const Sender = struct {
        fn run(owner: *Channel, result: *?DataError, at: *i64) void {
            result.* = if (owner.sendMessage(0, 64, 5000)) null else |err| err;
            at.* = nowMs();
        }
    };
    var result: ?DataError = error.Invalid;
    var at: i64 = 0;
    longSlices(channel);
    const thread = try std.Thread.spawn(.{}, Sender.run, .{ channel, &result, &at });
    awaitWriterAsleep(channel);
    const left = nowMs();
    gated.leave();
    thread.join();
    try testing.expectEqual(@as(?DataError, null), result);
    // At once: the leave wakes the writer, which would otherwise only see it at the end of its minute's slice (a margin
    // for a loaded machine)
    try testing.expect(at >= left and at - left < 1000);
}

test "a subscriber's wait ends with its cancel, the publisher's end after the drain, or its drop" {
    const channel = try Channel.create(4096, 2, -1);
    defer channel.destroy();
    const Receiver = struct {
        fn run(subscriber: *Subscriber, result: *?DataError, got: *u32) void {
            var buf: [256]u8 = undefined;
            while (true) {
                _ = subscriber.recv(&buf, -1) catch |err| {
                    result.* = err;
                    return;
                };
                got.* += 1;
            }
        }
    };
    for ([_]bool{ false, true }) |gated| {
        const subscriber = try channel.subscribe(@intFromBool(gated), gated);
        defer subscriber.destroy();
        var result: ?DataError = null;
        var got: u32 = 0;
        var since = subscriber.reader.sleeps.load(.seq_cst);
        var thread = try std.Thread.spawn(.{}, Receiver.run, .{ subscriber, &result, &got });
        awaitAsleep(subscriber, since);
        subscriber.cancel();
        thread.join();
        try testing.expectEqual(@as(?DataError, error.Cancelled), result);
        // A call that needn't wait still works
        try channel.sendMessage(channel.writer.next, 40, 0);
        var buf: [256]u8 = undefined;
        _ = try subscriber.recv(&buf, 0);
        subscriber.endpoint.hot.requests.store(.{}, .seq_cst);

        // The publisher ends with two messages unread: both are received, then Disconnected
        got = 0;
        try channel.sendMessage(channel.writer.next, 40, 0);
        try channel.sendMessage(channel.writer.next, 40, 0);
        since = subscriber.reader.sleeps.load(.seq_cst);
        thread = try std.Thread.spawn(.{}, Receiver.run, .{ subscriber, &result, &got });
        awaitAsleep(subscriber, since); // for a third message
        subscriber.publisherEnded();
        thread.join();
        try testing.expectEqual(@as(?DataError, error.Disconnected), result);
        try testing.expectEqual(@as(u32, 2), got);
        if (gated) channel.drop(1);
    }

    // A dropped gated subscriber: every call says so and touches nothing, also one that waited
    const subscriber = try channel.subscribe(1, true);
    defer subscriber.destroy();
    try channel.sendMessage(channel.writer.next, 40, 0);
    _ = try subscriber.recvAcquire(0);
    subscriber.release(); // its next receive waits
    const cursor = channel.layout.slots[1].cursor.load(.monotonic);
    var result: ?DataError = null;
    var got: u32 = 0;
    const since = subscriber.reader.sleeps.load(.seq_cst);
    const thread = try std.Thread.spawn(.{}, Receiver.run, .{ subscriber, &result, &got });
    awaitAsleep(subscriber, since);
    try testing.expectEqual(@as(u32, 1), subscriber.reader.in_segment.load(.seq_cst)); // its sleep is inside the guard
    subscriber.endpoint.hot.phase.store(.unsubscribed, .seq_cst);
    subscriber.reader.wakeOwn();
    thread.join();
    try testing.expectEqual(@as(?DataError, error.Unsubscribed), result);
    var buf: [256]u8 = undefined;
    try testing.expectError(error.Unsubscribed, subscriber.recv(&buf, 0));
    try testing.expectError(error.Unsubscribed, subscriber.recvAcquire(0));
    subscriber.reader.acquired = true;
    subscriber.release(); // nothing: no cursor store
    try testing.expect(!subscriber.reader.acquired);
    try testing.expectEqual(cursor, channel.layout.slots[1].cursor.load(.monotonic));
    try testing.expectEqual(@as(u32, 0), subscriber.reader.in_segment.load(.seq_cst));
}

test "a gated subscription that ends without the writer's close: copied and checked drain, nothing in place, no slot write" {
    const channel = try Channel.create(4096, 2, -1);
    defer channel.destroy();
    const subscriber = try channel.subscribe(0, true);
    defer subscriber.destroy();
    for (0..3) |k| try channel.sendMessage(k, 40, 0);
    try testing.expectEqual(@as(?u64, 0), Message.check(try subscriber.recvAcquire(0)));
    // The owner's process ended, or the owner dropped it while the writer goes on: no mark
    subscriber.publisherEnded();
    const cursor = &channel.layout.slots[0].cursor;
    try testing.expectEqual(@as(u64, 0), cursor.load(.monotonic));
    // In place: nothing, though two messages are there; the held one is let go without a cursor store
    try testing.expectError(error.Disconnected, subscriber.recvAcquire(0));
    try testing.expectError(error.Disconnected, subscriber.recvAcquire(-1));
    var buf: [256]u8 = undefined;
    // Copied and checked: what was published up to the end
    try testing.expectEqual(@as(?u64, 1), Message.check(try subscriber.recv(&buf, 0)));
    var small: [8]u8 = undefined;
    try testing.expectError(error.TooLarge, subscriber.recv(&small, 0)); // keeps the message
    try testing.expectEqual(@as(?u64, 2), Message.check(try subscriber.recv(&buf, -1)));
    // What the writer publishes after the end isn't this subscriber's any more
    try channel.sendMessage(3, 40, 0);
    try testing.expectError(error.Disconnected, subscriber.recv(&buf, -1));
    try testing.expectError(error.Disconnected, subscriber.recv(&buf, 0));
    // It wrote nothing in its slot meanwhile, and its leave writes nothing either
    try testing.expectEqual(@as(u64, 0), cursor.load(.monotonic));
    subscriber.leave();
    try testing.expect(channel.layout.shared.gated[0].load(.monotonic) & 1 != 0);
    channel.drop(0);

    // One whose messages the writer overwrites once the owner dropped it: Disconnected, never a torn copy
    const lapped = try channel.subscribe(1, true);
    defer lapped.destroy();
    try channel.sendMessage(channel.writer.next, 40, 0);
    lapped.publisherEnded();
    channel.drop(1);
    for (0..2 * channel.layout.cellCount()) |_| try channel.sendMessage(channel.writer.next, 40, 0);
    try testing.expectError(error.Disconnected, lapped.recv(&buf, 0));
    try testing.expectError(error.Disconnected, lapped.recv(&buf, 0));
}

test "a gated subscription that ended before the writer's close drains no payload the writer overwrote meanwhile" {
    const channel = try Channel.create(4096, 1, -1);
    defer channel.destroy();
    const subscriber = try channel.subscribe(0, true);
    defer subscriber.destroy();
    try channel.sendMessage(0, 40, 0);
    // The owner drops it while the writer goes on: two laps of the data ring with messages of a quarter each, few
    // enough that message 0's cell is still its own
    subscriber.publisherEnded();
    channel.drop(0);
    for (1..9) |k| try channel.sendMessage(k, 1024, 0);
    try testing.expect(channel.writer.next < channel.layout.cellCount());
    // Then the writer's close marks the channel ended: too late to protect what was overwritten
    channel.writer.endChannel();
    var buf: [256]u8 = undefined;
    try testing.expectError(error.Disconnected, subscriber.recvAcquire(0));
    try testing.expectError(error.Disconnected, subscriber.recv(&buf, 0));
    try testing.expectError(error.Disconnected, subscriber.recv(&buf, -1));
}

test "a subscriber's own interrupt wakes the channel's sleepers only while it sleeps itself" {
    if (platform.wakes_each_slot) return error.SkipZigTest; // each subscriber has a wake of its own there
    const channel = try Channel.create(4096, 2, -1);
    defer channel.destroy();
    const subscriber = try channel.subscribe(0, false);
    defer subscriber.destroy();
    const generation = &channel.layout.shared.wake_gen;
    const before = generation.load(.seq_cst);
    subscriber.cancel(); // no receive: nobody to wake, and the other sleepers stay asleep
    try testing.expectEqual(before, generation.load(.seq_cst));
    subscriber.endpoint.hot.requests.store(.{}, .seq_cst);
    const Receiver = struct {
        fn run(receiver: *Subscriber, result: *?DataError) void {
            var buf: [64]u8 = undefined;
            _ = receiver.recv(&buf, -1) catch |err| {
                result.* = err;
            };
        }
    };
    subscriber.reader.longest_sleep_ms = 60_000;
    var result: ?DataError = null;
    const thread = try std.Thread.spawn(.{}, Receiver.run, .{ subscriber, &result });
    while (subscriber.reader.sleeping.load(.seq_cst) == 0) std.Thread.yield() catch {};
    subscriber.cancel();
    thread.join();
    try testing.expectEqual(@as(?DataError, error.Cancelled), result);
    try testing.expect(generation.load(.seq_cst) != before);
    try testing.expectEqual(@as(u32, 0), subscriber.reader.sleeping.load(.seq_cst));
}

test "the writer's room comes from its own record of the payloads, not from the cells a subscriber could rewrite" {
    const channel = try Channel.create(4096, 1, -1);
    defer channel.destroy();
    const subscriber = try channel.subscribe(0, true);
    defer subscriber.destroy();
    // Four messages of a quarter of the ring each fill the data ring the gated subscriber holds
    for (0..4) |k| try channel.sendMessage(k, 1024, 0);
    try testing.expectError(error.Timeout, channel.sendMessage(4, 1024, 0));
    // A corrupt peer rewrites every cell's payload position to the writer's last (what a clamp would keep)
    for (0..channel.layout.cellCount()) |i| channel.layout.cells[i].payload_position.store(channel.writer.payload_end, .monotonic);
    try testing.expectError(error.Timeout, channel.sendMessage(4, 1024, 0));
    try testing.expectEqual(@as(u64, 4), channel.writer.next);
    subscriber.leave();
}

test "a subscriber's spin: paced longer while its first look finds the message, looks after a hint once it doesn't" {
    const channel = try Channel.create(4096, 2, -1);
    defer channel.destroy();
    const stream_pace = @import("../core/wait.zig").stream_pace;
    for ([_]bool{ false, true }) |gated| {
        const subscriber = try channel.subscribe(@intFromBool(gated), gated);
        defer subscriber.destroy();
        const reader = &subscriber.reader;
        try testing.expectEqual(@as(u32, 1), reader.pace);
        // A message there at the first look: twice the pace each time, up to the stream's
        try channel.sendMessage(channel.writer.next, 40, 0);
        var want: u32 = 1;
        for (0..12) |_| {
            try testing.expect(reader.spin());
            want = @min(want * 2, stream_pace);
            try testing.expectEqual(want, reader.pace);
        }
        try testing.expectEqual(stream_pace, reader.pace);
        var buf: [256]u8 = undefined;
        _ = try subscriber.recv(&buf, 0);
        // Nothing there: the next spin looks after one hint
        reader.spin_iters = 64;
        try testing.expect(!reader.spin());
        try testing.expectEqual(@as(u32, 1), reader.pace);
        // A message that comes after the first look: one hint again. A spin that starts late under load may find
        // it at its first look (the pace stays the stream's): then that attempt tells nothing, and another is made.
        reader.spin_iters = std.math.maxInt(u32);
        const Late = struct {
            fn run(owner: *Channel) void {
                sleepMs(20);
                owner.sendMessage(owner.writer.next, 40, 0) catch |err| std.debug.panic("the late send failed: {t}", .{err});
            }
        };
        for (0..5) |_| {
            reader.pace = stream_pace;
            const thread = try std.Thread.spawn(.{}, Late.run, .{channel});
            try testing.expect(reader.spin());
            thread.join();
            _ = try subscriber.recv(&buf, 0);
            if (reader.pace == 1) break;
            try testing.expectEqual(stream_pace, reader.pace);
        }
        try testing.expectEqual(@as(u32, 1), reader.pace);
        reader.spin_iters = 0; // a spin of no hints looks at nothing
        try channel.sendMessage(channel.writer.next, 40, 0);
        try testing.expect(!reader.spin());
        _ = try subscriber.recv(&buf, 0);
        if (gated) subscriber.leave();
    }
}

test "the writer's close ends every subscriber on its own: each receives what is left, then Disconnected" {
    const channel = try Channel.create(4096, 2, -1);
    defer channel.destroy();
    const Receiver = struct {
        fn run(subscriber: *Subscriber, result: *?DataError, got: *u32) void {
            var buf: [256]u8 = undefined;
            while (true) {
                _ = subscriber.recv(&buf, 5000) catch |err| {
                    result.* = err;
                    return;
                };
                got.* += 1;
            }
        }
    };
    const subscribers = [2]*Subscriber{ try channel.subscribe(0, false), try channel.subscribe(1, true) };
    defer for (subscribers) |subscriber| subscriber.destroy();
    var results: [2]?DataError = .{ null, null };
    var got: [2]u32 = .{ 0, 0 };
    var threads: [2]std.Thread = undefined;
    try channel.sendMessage(0, 40, 0);
    try channel.sendMessage(1, 40, 0);
    // Sleeps as long as the receive's timeout: only a wake ends them early
    for (subscribers) |subscriber| subscriber.reader.longest_sleep_ms = 10_000;
    var since: [2]u32 = undefined;
    for (subscribers, &since) |subscriber, *n| n.* = subscriber.reader.sleeps.load(.seq_cst);
    for (&threads, 0..) |*thread, i| thread.* = try std.Thread.spawn(.{}, Receiver.run, .{ subscribers[i], &results[i], &got[i] });
    // Both received what was sent, and sleep for a third message, past their looks at the ended mark
    for (subscribers, since) |subscriber, n| awaitAsleep(subscriber, n);
    const begun = std.Io.Clock.awake.now(testing.io);
    channel.writer.endChannel();
    for (threads) |thread| thread.join();
    // Their watchers haven't seen the end: the mark and the writer's one wake end the waits
    try testing.expect(begun.durationTo(std.Io.Clock.awake.now(testing.io)).toMilliseconds() < 1000);
    for (subscribers, results, got) |subscriber, result, n| {
        try testing.expectEqual(@as(?DataError, error.Disconnected), result);
        try testing.expectEqual(@as(u32, 2), n);
        try testing.expectEqual(Phase.connected, subscriber.endpoint.hot.phase.load(.monotonic));
        var buf: [64]u8 = undefined;
        try testing.expectError(error.Disconnected, subscriber.recv(&buf, 0));
        subscriber.publisherEnded(); // the watcher's end comes later, and changes nothing
        try testing.expectError(error.Disconnected, subscriber.recv(&buf, -1));
    }
    channel.drop(1);
}

test "a gated subscriber held still past max_stall is reported, once, and the writer goes on once it is dropped" {
    const channel = try Channel.create(4096, 3, 100);
    defer channel.destroy();
    const lossy = try channel.subscribe(0, false);
    defer lossy.destroy();
    const keeps_up = try channel.subscribe(1, true);
    defer keeps_up.destroy();
    const laggard = try channel.subscribe(2, true);
    defer laggard.destroy();
    const cell_count = channel.layout.cellCount();
    var buf: [256]u8 = undefined;
    for (0..cell_count) |k| {
        try channel.sendMessage(k, 64, 0);
        _ = try keeps_up.recv(&buf, 0);
    }
    const t0 = nowMs();
    try channel.sendMessage(cell_count, 64, 2000);
    const took = nowMs() - t0;
    try testing.expect(took >= 90); // never before the stall (a loaded machine may take longer)
    try testing.expectEqual(@as(u32, 1), channel.stalls.load(.seq_cst));
    try testing.expectEqual(@as(u32, 2), channel.stalled_slot.load(.monotonic));
    try testing.expectEqual(channel.roster.slots.generations[2].load(.monotonic), channel.stalled_generation.load(.monotonic));

    // Sends that don't wait add the stall up across calls
    const again = try channel.subscribe(2, true);
    defer again.destroy();
    for (0..cell_count) |k| {
        if (channel.sendMessage(cell_count + 1 + k, 64, 0)) {} else |err| {
            try testing.expectEqual(error.Timeout, err);
            break;
        }
        _ = try keeps_up.recv(&buf, 0);
    }
    const t1 = nowMs();
    while (channel.stalls.load(.seq_cst) < 2 and nowMs() - t1 < 2000) {
        try testing.expectError(error.Timeout, channel.sendMessage(0, 64, 0));
        sleepMs(5);
    }
    try testing.expectEqual(@as(u32, 2), channel.stalls.load(.seq_cst));
    try testing.expect(nowMs() - t1 >= 90);
}

test "max_stall forever: a send waits for a laggard until its own timeout, and nobody is reported" {
    const channel = try Channel.create(4096, 1, -1);
    defer channel.destroy();
    const laggard = try channel.subscribe(0, true);
    defer laggard.destroy();
    for (0..channel.layout.cellCount()) |k| try channel.sendMessage(k, 64, 0);
    const t0 = nowMs();
    try testing.expectError(error.Timeout, channel.sendMessage(0, 64, 200));
    try testing.expect(nowMs() - t0 >= 190);
    try testing.expectEqual(@as(u32, 0), channel.stalls.load(.seq_cst));
}

/// The stress's subscriber threads: lossy ones check that each message is whole and later than the last, and later
/// by one but right after an overrun; gated ones that each is whole and the next.
const Reading = struct {
    subscriber: *Subscriber,
    failure: *Failure,
    received: u64 = 0,
    overruns: u64 = 0,
    /// Take messages in place (gated), every other one.
    in_place: bool,
    thread: std.Thread = undefined,

    fn run(reading: *Reading) void {
        var buf: [Message.max]u8 = undefined;
        var prev: ?u64 = null;
        var after_overrun = false;
        var k: u64 = 0;
        while (true) : (k += 1) {
            const zero_copy = reading.in_place and k % 2 == 0;
            const msg: []const u8 = (if (zero_copy) reading.subscriber.recvAcquire(-1) else reading.subscriber.recv(&buf, -1)) catch |err| switch (err) {
                error.Overrun => {
                    if (reading.subscriber.reader.gated) return reading.failure.set("a gated subscriber was overrun");
                    reading.overruns += 1;
                    after_overrun = true;
                    continue;
                },
                error.Disconnected => return,
                else => return reading.failure.set("a receive failed"),
            };
            const seq = Message.check(msg) orelse return reading.failure.set("a message was torn");
            if (prev) |previous| {
                if (seq <= previous) return reading.failure.set("a message came twice, or out of order");
                if (seq != previous + 1 and (reading.subscriber.reader.gated or !after_overrun)) return reading.failure.set("a message was skipped");
            }
            if (zero_copy) {
                // Held in place, it stays whole
                for (0..64) |_| std.atomic.spinLoopHint();
                if (Message.check(msg) != seq) return reading.failure.set("a message held in place was overwritten");
                if (k % 4 == 0) reading.subscriber.release();
            }
            prev = seq;
            after_overrun = false;
            reading.received += 1;
        }
    }
};

/// One writer and `lossy` + `gated` subscriber threads on a channel of `capacity` bytes: `count` messages of varied
/// sizes, at most `budget_ms` of sending, then the writer's close. Every subscriber's messages are checked as they
/// come; each gated one must have received every message.
fn stress(capacity: u32, lossy: u32, gated: u32, count: u64, budget_ms: i64) !void {
    const channel = try Channel.create(capacity, lossy + gated, -1);
    defer channel.destroy();
    var failure: Failure = .{};
    const readings = try testing.allocator.alloc(Reading, lossy + gated);
    defer testing.allocator.free(readings);
    var made: usize = 0;
    defer for (readings[0..made]) |*reading| reading.subscriber.destroy();
    for (readings, 0..) |*reading, i| {
        const is_gated = i >= lossy;
        reading.* = .{ .subscriber = try channel.subscribe(@intCast(i), is_gated), .failure = &failure, .in_place = is_gated and i % 2 == 0 };
        made += 1;
    }
    var started: usize = 0;
    defer for (readings[0..started]) |*reading| reading.thread.join();
    for (readings) |*reading| {
        reading.thread = try std.Thread.spawn(.{}, Reading.run, .{reading});
        started += 1;
    }
    const t0 = nowMs();
    var sent: u64 = 0;
    var buf: [Message.max]u8 = undefined;
    while (sent < count and (sent % 256 != 0 or nowMs() - t0 < budget_ms)) : (sent += 1) {
        const n = Message.sizeOf(sent, channel.layout.maxPiece());
        Message.fill(buf[0..n], sent);
        channel.send(buf[0..n], 10_000) catch {
            failure.set("a send failed");
            break;
        };
    }
    // The writer's close: what it published stays protected, in place too, until every subscriber has it
    channel.writer.endChannel();
    for (readings) |*reading| reading.subscriber.publisherEnded();
    for (readings[0..started]) |*reading| reading.thread.join();
    started = 0;
    try failure.check();
    var lossy_received: u64 = 0;
    for (readings) |*reading| {
        if (reading.subscriber.reader.gated) try testing.expectEqual(sent, reading.received) else lossy_received += reading.received;
    }
    try testing.expect(lossy == 0 or lossy_received > 0);
}

/// More threads than cores: some subscriber is always descheduled, mid-copy or holding a message.
fn oversubscribed() u32 {
    const cores = std.Thread.getCpuCount() catch 4;
    return @intCast(@min(cores + 2, 24));
}

test "stress: one writer, lossy and gated subscriber threads, more threads than cores, every message checked" {
    try stress(64 * 1024, oversubscribed(), 3, 20_000, 1000);
    try stress(4096, 3, 2, 5_000, 500); // small: laps all the time, the largest message a quarter of the ring
}

test "a gated subscriber that joins while the writer laps never reads a message overwritten" {
    // What it can catch, by chance of timing over its rounds, in a start (`writer_module.startSlot`, run on a thread
    // other than the writer's, as the owner's admission does):
    //   - a start without its second load of `next`: it would start at the first load, which a sweep that missed the
    //     bit may already have freed;
    //   - a start without its first store: a sweep that sees the bit would read the garbage cursor left before each
    //     join as "past everything".
    // Either shows as a held message whose checksum changes, or as a gap. It can't catch orderings that are too weak
    // only on other machines (x86 keeps them anyway): the litmus tests and an aarch64 run cover those.
    const channel = try Channel.create(4096, 2, -1);
    defer channel.destroy();
    const lossy = try channel.subscribe(1, false);
    defer lossy.destroy();
    const joiner = try channel.subscribe(0, true);
    defer joiner.destroy();
    joiner.leave(); // it joins and leaves again below
    const cursor = &channel.layout.slots[0].cursor;
    var failure: Failure = .{};
    var stop: Atomic(bool) = .init(false);
    const Writing = struct {
        fn run(owner: *Channel, stop_: *Atomic(bool), report: *Failure) void {
            var seq: u64 = owner.writer.next;
            while (!stop_.load(.acquire)) : (seq += 1) {
                owner.sendMessage(seq, Message.sizeOf(seq, 256), 10_000) catch return report.set("a send failed");
            }
        }
    };
    const thread = try std.Thread.spawn(.{}, Writing.run, .{ channel, &stop, &failure });
    var rounds: u32 = 0;
    const t0 = nowMs();
    var random: u64 = 0x1234;
    while (rounds < 2000 and nowMs() - t0 < 1500) : (rounds += 1) {
        random = random *% 6364136223846793005 +% 1442695040888963407;
        for (0..(random >> 33) % 2048) |_| std.atomic.spinLoopHint();
        // Garbage where the cursor was, which only the join's first store hides from a sweep that sees the bit
        cursor.store(std.math.maxInt(u64), .monotonic);
        writer_module.startSlot(&channel.layout, 0, true);
        joiner.reader = try .init(joiner.reader.layout, 0, true, joiner.reader.own_wake);
        var prev: ?u64 = null;
        for (0..4) |_| {
            const msg = joiner.recvAcquire(5000) catch {
                failure.set("a receive failed");
                break;
            };
            const seq = Message.check(msg) orelse {
                failure.set("a message was torn");
                break;
            };
            if (prev) |previous| if (seq != previous + 1) failure.set("a message was skipped");
            for (0..200) |_| std.atomic.spinLoopHint();
            if (Message.check(msg) != seq) failure.set("a message held in place was overwritten");
            prev = seq;
        }
        joiner.release();
        joiner.leave();
        if (failure.first.load(.monotonic) != null) break;
    }
    stop.store(true, .release);
    thread.join();
    try failure.check();
}

test "no wake is lost: lock-step rounds with subscribers that sleep for every message, and never time out" {
    // Each round the writer sends one message and waits until every subscriber has it, then pauses a little, so the
    // subscribers go to sleep. A lost wake would leave one asleep for a minute (its sleep's limit here): the round's
    // 5 s bound fails the test instead.
    const n_lossy = 3;
    const n_gated = 3;
    const channel = try Channel.create(4096, n_lossy + n_gated, -1);
    defer channel.destroy();
    var subscribers: [n_lossy + n_gated]*Subscriber = undefined;
    var made: usize = 0;
    defer for (subscribers[0..made]) |subscriber| subscriber.destroy();
    for (&subscribers, 0..) |*subscriber, i| {
        subscriber.* = try channel.subscribe(@intCast(i), i >= n_lossy);
        subscriber.*.reader.longest_sleep_ms = 60_000;
        if (i % 2 == 0) subscriber.*.reader.spin_iters = 0; // sleeps for every message
        made += 1;
    }
    var seen: [subscribers.len]Atomic(u64) = @splat(.init(0));
    var failure: Failure = .{};
    const Receiving = struct {
        fn run(subscriber: *Subscriber, last: *Atomic(u64), report: *Failure) void {
            var buf: [256]u8 = undefined;
            while (true) {
                const msg = subscriber.recv(&buf, -1) catch |err| {
                    if (err != error.Disconnected) report.set("a receive failed");
                    return;
                };
                const seq = Message.check(msg) orelse return report.set("a message was torn");
                last.store(seq + 1, .release);
            }
        }
    };
    var threads: [subscribers.len]std.Thread = undefined;
    var started: usize = 0;
    defer for (threads[0..started]) |thread| thread.join();
    defer for (subscribers) |subscriber| subscriber.publisherEnded();
    for (&threads, subscribers, &seen) |*thread, subscriber, *last| {
        thread.* = try std.Thread.spawn(.{}, Receiving.run, .{ subscriber, last, &failure });
        started += 1;
    }
    var random: u64 = 77;
    const t0 = nowMs();
    var round: u64 = 0;
    while (round < 1500 and nowMs() - t0 < 2000) : (round += 1) {
        try channel.sendMessage(round, 24, 1000);
        const start = nowMs();
        for (&seen) |*last| {
            while (last.load(.acquire) != round + 1) {
                if (nowMs() - start > 5000) {
                    std.debug.print("round {d}: a subscriber didn't wake\n", .{round});
                    return error.LostWake;
                }
                std.Thread.yield() catch {};
            }
        }
        random = random *% 6364136223846793005 +% 1442695040888963407;
        // Mostly long enough for the spinning subscribers to sleep too
        const pause: u64 = if (random >> 62 == 0) 0 else (random >> 33) % 400;
        const until = std.Io.Clock.awake.now(testing.io).addDuration(.fromMicroseconds(@intCast(pause)));
        while (std.Io.Clock.awake.now(testing.io).nanoseconds < until.nanoseconds) std.atomic.spinLoopHint();
    }
    try failure.check();
}

test "a writer waiting for a slow gated subscriber goes on as it reads; a lossy one beside it is never waited for" {
    const channel = try Channel.create(4096, 2, -1);
    defer channel.destroy();
    const slow = try channel.subscribe(0, true);
    defer slow.destroy();
    const lossy = try channel.subscribe(1, false);
    defer lossy.destroy();
    var failure: Failure = .{};
    const count = 3000;
    const Slow = struct {
        fn run(subscriber: *Subscriber, report: *Failure) void {
            var buf: [Message.max]u8 = undefined;
            for (0..count) |k| {
                const msg = subscriber.recv(&buf, 5000) catch return report.set("a receive failed");
                if (Message.check(msg) != k) return report.set("a message was torn or skipped");
                if (k % 200 == 0) sleepMs(2);
            }
        }
    };
    const thread = try std.Thread.spawn(.{}, Slow.run, .{ slow, &failure });
    for (0..count) |k| channel.sendMessage(k, Message.sizeOf(k, 1024), 5000) catch {
        failure.set("a send failed");
        break;
    };
    thread.join();
    try failure.check();
}

test "a writer lapping a lossy subscriber at full speed: the seqlock copy, TSan's one ignored race" {
    const channel = try Channel.create(8192, 1, -1);
    defer channel.destroy();
    const subscriber = try channel.subscribe(0, false);
    defer subscriber.destroy();
    var failure: Failure = .{};
    var reading: Reading = .{ .subscriber = subscriber, .failure = &failure, .in_place = false };
    reading.thread = try std.Thread.spawn(.{}, Reading.run, .{&reading});
    var buf: [Message.max]u8 = undefined;
    const t0 = nowMs();
    var k: u64 = 0;
    while (k < 50_000 and nowMs() - t0 < 1000) : (k += 1) {
        Message.fill(buf[0..2048], k);
        try channel.send(buf[0..2048], 0);
    }
    subscriber.publisherEnded();
    reading.thread.join();
    try failure.check();
}

test {
    if (@import("build_options").slow_tests) _ = slow_tests;
}

const slow_tests = struct {
    test "slow: stress: one writer, many lossy and gated subscriber threads, for seconds" {
        // The slow tier runs each stress test on its own, beside the other slow tests (build/tests.zig)
        if (std.testing.environ.containsUnemptyConstant("FIPC_SKIP_STRESS_BROADCAST")) return error.SkipZigTest;
        try stress(512 * 1024, 2 * oversubscribed(), 4, 2_000_000, 5000);
        try stress(4096, 6, 4, 500_000, 3000);
    }
};
