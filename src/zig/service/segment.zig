//! A service's segment: its owner creates it once for the service (`create`) and every client and server maps it
//! (`attach`). It holds a description (`wire_protocol.ServiceInfo`, where its parts are) and the words every member
//! writes (the end mark, the `ready` and `idle` bitmaps), a slot per server, then a region per client: its ring's
//! control block (`tail`, `head`, the room and reply flags), its cells and its data ring. Each client has a ring, which
//! holds its requests and the room of their replies; there is no ring past the clients'. The rings, the cells and the
//! bitmaps are a ring set (core/ring_set.zig, whose top comment says how messages, replies and wakes work there): the
//! service's is `Rings`, over its wire types, its clients the senders and its servers the takers. `Layout` is where
//! those parts are in one process's mapping, which the clients (caller.zig), the servers (responder.zig) and the owner
//! work on.
//!
//! No part of the segment is mapped read-only: every member writes the cells of every ring (a server its reply into a
//! client's data ring). A corrupt peer can write anything in it; whatever is read from it and indexes or sizes
//! anything is checked where it is read.
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

/// The description of a service segment for client rings of `capacity` bytes, `max_clients` clients, `max_servers`
/// servers and messages of up to `max_message` bytes, its session id 0: where its parts are (core/ring_set.zig
/// `regions`). The header (512 bytes) and a 128-byte slot per server, then from the next page each client's region: its
/// control block (512 bytes), its cells (one per 64 bytes of data, 32 bytes each), and from the next page its data
/// ring. Every region is whole pages.
pub fn describe(capacity: u32, max_clients: u32, max_servers: u32, max_message: u32) wire_protocol.ServiceInfo {
    const parts = Rings.regions(capacity, max_clients, max_servers);
    return .{
        .magic = wire_protocol.service_magic,
        .header_size = wire_protocol.service_header_size,
        .capacity = capacity,
        .cell_count = parts.cell_count,
        .max_clients = max_clients,
        .max_servers = max_servers,
        .max_message = max_message,
        .session_id = @splat(0),
        .servers_offset = parts.takers_offset,
        .clients_offset = parts.rings_offset,
        .client_stride = parts.ring_stride,
        .cells_offset = parts.cells_offset,
        .data_offset = parts.data_offset,
        .segment_size = parts.segment_size,
        .reserved = @splat(0),
    };
}

/// A service's capacity, a power of two from 4 KiB to 2 GiB, its `max_message`, 1 to a quarter of the capacity, and
/// its clients and servers, 1 to 64 each.
pub fn valid(capacity: u64, max_message: u64, max_clients: u64, max_servers: u64) bool {
    return capacity >= wire_protocol.service_min_capacity and capacity <= 1 << 31 and std.math.isPowerOfTwo(capacity) and
        max_message >= 1 and max_message <= capacity / wire_protocol.service_message_divisor and
        max_clients >= 1 and max_clients <= wire_protocol.max_service_clients and
        max_servers >= 1 and max_servers <= wire_protocol.max_service_servers;
}

/// A service session's description, at the start of its segment.
pub fn info(session: *const Session) *wire_protocol.ServiceInfo {
    return @ptrCast(@alignCast(session.mapping.base));
}

/// A new service's segment under a new random id (core/shared_memory.zig `createUnderRandomId`), and its description
/// written; everything else stays zero, which is a service with no request. `handle`: the segment's handle, where it
/// travels with OFFER (created and registered by the caller), else null.
pub fn create(
    io: Io,
    handle: ?platform.Handle,
    security: *const platform.Security,
    capacity: u32,
    max_clients: u32,
    max_servers: u32,
    max_message: u32,
) shared_memory.CreateError!Session {
    std.debug.assert(valid(capacity, max_message, max_clients, max_servers));
    var description = describe(capacity, max_clients, max_servers, max_message);
    const id, const mapping = try shared_memory.createUnderRandomId(io, handle, description.segment_size, security, .service);
    const session: Session = .{ .id = id, .capacity = capacity, .server = true, .mapping = mapping };
    description.session_id = id;
    info(&session).* = description;
    return session;
}

/// A member's attach (docs/protocol.md §11.3): maps the offered service segment, checks its description against the
/// offer (core/shared_memory.zig `attachDescribed`), and returns it as this side's session. The offer's sizes are the
/// peer's, so they are checked first. `received`: the segment's handle, where it travels with OFFER (the caller closes
/// it afterwards, whatever the result), else null.
pub fn attach(received: ?platform.Handle, offer: *const frames.Offer) shared_memory.AttachError!Session {
    if (!valid(offer.capacity, offer.max_message, offer.max_clients, offer.max_servers)) return shared_memory.rejected("the offered capacity, max_message, max_clients or max_servers is out of range");
    var expected = describe(offer.capacity, offer.max_clients, offer.max_servers, offer.max_message);
    expected.session_id = offer.session_id;
    return shared_memory.attachDescribed(wire_protocol.ServiceInfo, .service, received, expected, offer.segment_size);
}

/// The layout of a service session's segment, the owner's or a member's (`max_message`: the offer's, checked).
pub fn layoutOf(session: *const Session, max_clients: u32, max_servers: u32, max_message: u32) Layout {
    return .chosen(session.mapping.base, session.capacity, max_clients, max_servers, max_message);
}

// === The ring set ===

/// The kind suffixes of a slot's wakes (Windows: `Local\fastipc-<sid>-sv-reply-<c>`, `names.slotObject`): a client's
/// wait for a reply, which servers and the owner set, and its wait for room, which only its own receiving thread sets;
/// a server's park.
pub const client_reply_wake_suffix = "-sv-reply-";
pub const client_room_wake_suffix = "-sv-room-";
pub const server_wake_suffix = "-sv-server-";

/// The service's ring set (core/ring_set.zig): the clients' rings, whose messages carry replies.
pub const Rings = ring_set.RingSet(struct {
    pub const Header = wire_protocol.ServiceHeader;
    pub const TakerSlot = wire_protocol.ServiceServerSlot;
    pub const RingControl = wire_protocol.ServiceRingControl;
    pub const Cell = wire_protocol.ServiceCell;
    pub const Role = wire_protocol.ServiceRole;
    pub const sender_role: wire_protocol.ServiceRole = .client;
    pub const taker_role: wire_protocol.ServiceRole = .server;
    pub const max_senders = wire_protocol.max_service_clients;
    pub const max_takers = wire_protocol.max_service_servers;
    pub const extra_rings = 0;
    /// The owner's `max_message`.
    pub const piece_divisor: ?usize = null;
    pub const replies = true;
    pub const cell_spread = wire_protocol.service_cell_spread;
    pub const sender_wake_suffix = client_reply_wake_suffix;
    pub const taker_wake_suffix = server_wake_suffix;
    pub const room_wake_suffix: ?[]const u8 = client_room_wake_suffix;
    pub const noun = "service";
    pub const published_step = .service_published;
    pub const claimed_step = .service_claimed;
    pub const replying_step = .service_replying;
    pub const replied_step = .service_replied;

    /// The owner's, at each change of its counts: the clients and servers joined.
    pub inline fn showJoined(header: *Header, clients: u32, servers: u32) void {
        header.clients_joined.store(clients, .monotonic);
        header.servers_joined.store(servers, .monotonic);
    }

    /// The clients and servers joined, as the owner showed them last.
    pub inline fn joinedShown(header: *const Header) [2]u32 {
        return .{ header.clients_joined.load(.monotonic), header.servers_joined.load(.monotonic) };
    }

    /// A publish's store of the answer's fields: no reply yet.
    pub inline fn clearExtra(cell: *Cell) void {
        cell.reply_len.store(0, .monotonic);
        cell.outcome.store(@backingInt(wire_protocol.ServiceOutcome.none), .monotonic);
    }
});

comptime {
    std.debug.assert(@sizeOf(Rings.Header) == wire_protocol.service_header_size and @sizeOf(Rings.RingControl) == wire_protocol.service_ring_control_size);
}

/// A member's role, its wire value (`fipc_service_join`'s, and JOIN's and WELCOME's).
pub const Role = Rings.Role;

/// A member's slot: its role, and its index among that role's slots (a client's is its ring's number).
pub const Slot = Rings.Slot;

/// A set of slots of one role: bit s, slot s.
pub const Slots = Rings.Slots;

/// Where a service segment's parts are in this process's mapping (core/ring_set.zig `Layout`: its senders are the
/// clients, its takers the servers, its most bytes of a piece `max_message`).
pub const Layout = Rings.Layout;

/// A member's handles to the other slots' wakes (Windows): the clients' waits for replies, the servers' parks.
pub const Wakes = Rings.Wakes;

/// The kind suffix of the wake a member of `role` waits on for its data (Windows): a client's for its replies, a
/// server's for its park.
pub fn wakeSuffix(role: Role) []const u8 {
    return Rings.wakeSuffix(role);
}

// === Tests (real OS objects, in this process; more in caller_responder_test.zig and litmus_test.zig) ===

const testing = std.testing;
const page = 4096;

test "a service segment's sizes: the header and the servers' slots, then each client's control, cells and data, page by page" {
    const small = describe(4096, 1, 1, 1024);
    try testing.expectEqual(@as(u32, 64), small.cell_count);
    try testing.expectEqual(@as(u64, 512), small.servers_offset);
    try testing.expectEqual(@as(u64, 4096), small.clients_offset);
    try testing.expectEqual(@as(u64, 512), small.cells_offset);
    try testing.expectEqual(@as(u64, 4096), small.data_offset);
    try testing.expectEqual(@as(u64, 8192), small.client_stride);
    try testing.expectEqual(@as(u64, 12288), small.segment_size); // fipc.h's 12 KiB: no ring past the client's
    try testing.expectEqual(@as(u32, 1024), small.max_message);

    const mid = describe(1 << 16, 64, 8, 256);
    try testing.expectEqual(@as(u64, 4096), mid.clients_offset); // 512 + 8 slots of 128
    try testing.expectEqual(@as(u64, 36864), mid.data_offset); // 512 + 1024 cells of 32, rounded up to a page
    try testing.expectEqual(@as(u64, 4096 + 64 * (36864 + (1 << 16))), mid.segment_size); // fipc.h's "about 6.3 MiB"

    const big = describe(1 << 31, 64, 64, 1 << 29);
    try testing.expectEqual(@as(u64, 12288), big.clients_offset); // 512 + 64 slots of 128, rounded up
    try testing.expectEqual(@as(u32, 1 << 25), big.cell_count);
    try testing.expectEqual(@as(u64, (1 << 30) + page), big.data_offset);
    try testing.expectEqual(@as(u64, 12288 + 64 * ((1 << 30) + page + (1 << 31))), big.segment_size);
    for ([_]wire_protocol.ServiceInfo{ small, mid, big, describe(8192, 3, 33, 7) }) |described| {
        try testing.expectEqual(@as(u64, 0), described.clients_offset % page);
        try testing.expectEqual(@as(u64, 0), described.data_offset % page);
        try testing.expectEqual(@as(u64, 0), described.client_stride % page);
        try testing.expectEqual(@as(u64, 0), described.segment_size % page);
    }

    try testing.expect(valid(4096, 1, 1, 1) and valid(4096, 1024, 64, 64) and valid(1 << 31, 1 << 29, 64, 64));
    try testing.expect(!valid(2048, 1, 1, 1) and !valid(4097, 1, 1, 1) and !valid(1 << 32, 1, 1, 1));
    try testing.expect(!valid(4096, 0, 1, 1) and !valid(4096, 1025, 1, 1)); // max_message: 1 to capacity / 4
    try testing.expect(!valid(4096, 1, 0, 1) and !valid(4096, 1, 65, 1) and !valid(4096, 1, 1, 0) and !valid(4096, 1, 1, 65));

    // The rings' bits of `ready`: one per client, all 64 with 64 clients
    var header: wire_protocol.ServiceHeader align(64) = undefined;
    const one: Layout = .chosen(@ptrCast(&header), 4096, 1, 1, 1);
    try testing.expectEqual(@as(u64, 0b1), one.ringMask());
    const most: Layout = .chosen(@ptrCast(&header), 4096, 64, 64, 1024);
    try testing.expectEqual(@as(u32, 64), most.ringCount());
    try testing.expectEqual(~@as(u64, 0), most.ringMask());
    try testing.expectEqual(~@as(u64, 0), most.takerMask());
}

test "the reply's room: max_message rounded up to 64 before each request, which a piece must leave inside the ring" {
    var header: wire_protocol.ServiceHeader align(64) = undefined;
    for ([_]u32{ 1, 63, 64, 65, 1024 }) |max_message| {
        const layout: Layout = .chosen(@ptrCast(&header), 4096, 1, 1, max_message);
        const room = std.mem.alignForward(u64, max_message, 64);
        try testing.expectEqual(room, layout.replyRoom());
        try testing.expectEqual(@as(usize, max_message), layout.maxPiece());
        // A request at the room's end, of any length up to max_message, is a piece of the ring; one whose reply's room
        // would start before the data ring's isn't, whatever its lap
        try testing.expect(layout.validPiece(room, max_message) and layout.validPiece(4096 * 3 + room, 1));
        try testing.expect(!layout.validPiece(room - 1, 1) and !layout.validPiece(4096 * 3, 1) and !layout.validPiece(0, 1));
        try testing.expect(!layout.validPiece(room, 0) and !layout.validPiece(room, max_message + 1));
        try testing.expect(!layout.validPiece(4096 - 1, 2)); // past the ring's end
    }
}

/// An owner's service segment, and offers of it, in one process.
const ServicePair = struct {
    created: Session,
    handle: ?platform.Handle,
    security: platform.Security,
    max_clients: u32,
    max_servers: u32,
    max_message: u32,

    fn init(capacity: u32, max_clients: u32, max_servers: u32, max_message: u32) !ServicePair {
        var security = try platform.Security.init();
        errdefer security.deinit();
        const handle: ?platform.Handle = if (platform.passes_segment_handle) try platform.createSegmentHandle() else null;
        errdefer if (handle) |segment_handle| platform.close(segment_handle);
        return .{
            .created = try create(testing.io, handle, &security, capacity, max_clients, max_servers, max_message),
            .handle = handle,
            .security = security,
            .max_clients = max_clients,
            .max_servers = max_servers,
            .max_message = max_message,
        };
    }

    fn offer(pair: *const ServicePair) frames.Offer {
        return .{
            .capacity = pair.created.capacity,
            .segment_size = describe(pair.created.capacity, pair.max_clients, pair.max_servers, pair.max_message).segment_size,
            .session_id = pair.created.id,
            .max_clients = pair.max_clients,
            .max_servers = pair.max_servers,
            .max_message = pair.max_message,
        };
    }

    fn deinit(pair: *ServicePair) void {
        pair.created.unmap();
        pair.security.deinit();
        if (pair.handle) |segment_handle| platform.close(segment_handle);
    }
};

test "a new service segment has its description, zeroes elsewhere, and a member's attach sees the same memory, writable" {
    var pair = try ServicePair.init(4096, 2, 3, 100);
    defer pair.deinit();
    const description = info(&pair.created);
    try testing.expectEqualSlices(u8, "FIPCSVC1", std.mem.asBytes(&description.magic));
    try testing.expectEqual(@as(u32, 512), description.header_size);
    try testing.expectEqual(@as(u32, 4096), description.capacity);
    try testing.expectEqual(@as(u32, 2), description.max_clients);
    try testing.expectEqual(@as(u32, 3), description.max_servers);
    try testing.expectEqual(@as(u32, 100), description.max_message);
    try testing.expectEqual(@as(u64, 4096 + 2 * 8192), description.segment_size); // two clients' rings
    try testing.expectEqual(pair.created.mapping.size, description.segment_size);
    try testing.expect(!std.mem.allEqual(u8, &description.session_id, 0));
    const base = pair.created.mapping.base;
    try testing.expect(std.mem.allEqual(u8, base[@sizeOf(wire_protocol.ServiceInfo)..description.segment_size], 0));

    const offered = pair.offer();
    var attached = try attach(pair.handle, &offered);
    defer attached.unmap();
    try testing.expect(!attached.server and attached.capacity == 4096);
    const mine = layoutOf(&pair.created, 2, 3, 100);
    const theirs = layoutOf(&attached, 2, 3, 100);
    mine.ring(1).reply_sleeps.store(7, .monotonic);
    try testing.expectEqual(@as(u32, 7), theirs.ring(1).reply_sleeps.load(.monotonic));
    theirs.cell(1, 70).outcome.store(2, .monotonic); // cell 6 of ring 1
    try testing.expectEqual(@as(u32, 2), mine.cell(1, 6).outcome.load(.monotonic));
    theirs.bytes(1, 4096 + 100, 3)[0] = 0xab;
    try testing.expectEqual(@as(u8, 0xab), mine.bytes(1, 100, 1)[0]);
    // Every part of a member's view is writable
    for ([_]u64{ 0, 512, 4096, 4096 + 512, 8192, description.segment_size - 1 }) |at| {
        try testing.expectEqual(platform.Protection.read_write, platform.protectionOf(attached.mapping.base + at));
    }
}

test "a member's attach refuses an offer out of range or inconsistent, a description that doesn't match it, and a queue segment" {
    var pair = try ServicePair.init(8192, 2, 2, 64);
    defer pair.deinit();
    const description = info(&pair.created);
    var offered = pair.offer();
    offered.segment_size += page;
    try testing.expectError(error.Rejected, attach(pair.handle, &offered));
    offered = pair.offer();
    offered.capacity = 3000;
    try testing.expectError(error.Rejected, attach(pair.handle, &offered));
    for ([_]u32{ 0, 2049 }) |max_message| {
        offered = pair.offer();
        offered.max_message = max_message;
        try testing.expectError(error.Rejected, attach(pair.handle, &offered));
    }
    offered = pair.offer();
    offered.max_message = 65; // in range, the same size: the description tells
    try testing.expectError(error.Rejected, attach(pair.handle, &offered));
    offered = pair.offer();
    offered.max_clients = 0;
    try testing.expectError(error.Rejected, attach(pair.handle, &offered));
    offered = pair.offer();
    offered.max_servers = 65;
    try testing.expectError(error.Rejected, attach(pair.handle, &offered));
    offered = pair.offer();
    offered.max_clients = 65;
    offered.segment_size = describe(offered.capacity, 65, offered.max_servers, offered.max_message).segment_size;
    try testing.expectError(error.Rejected, attach(pair.handle, &offered));
    offered = pair.offer();
    offered.capacity = std.math.maxInt(u32);
    try testing.expectError(error.Rejected, attach(pair.handle, &offered));
    offered = pair.offer();
    offered.max_servers = 1; // the same size: the description tells
    try testing.expectError(error.Rejected, attach(pair.handle, &offered));
    offered = pair.offer();
    offered.session_id[3] +%= 1;
    try testing.expectError(if (platform.passes_segment_handle) error.Rejected else error.Gone, attach(pair.handle, &offered));

    // Each field of the description
    offered = pair.offer();
    const words = std.mem.bytesAsSlice(u32, std.mem.asBytes(description)[0..@offsetOf(wire_protocol.ServiceInfo, "reserved")]);
    for (words) |*word| {
        const saved = word.*;
        word.* = saved ^ 0x10;
        try testing.expectError(error.Rejected, attach(pair.handle, &offered));
        word.* = saved;
    }
    // A queue's magic
    const saved = description.*;
    description.magic = wire_protocol.queue_magic;
    try testing.expectError(error.Rejected, attach(pair.handle, &offered));
    description.* = saved;
    // The reserved bytes are not checked
    description.reserved[31] = 0xff;
    var again = try attach(pair.handle, &offered);
    again.unmap();
}
