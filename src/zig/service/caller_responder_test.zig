//! The service's data path on a service segment in this process, without the lifecycle: the owner's segment (real OS
//! objects: the mapping, and on Windows each slot's events), and clients and servers on endpoints of their own, each
//! with its own view of the segment as a member attaches it, which tests drive from threads. Tests that play a
//! corrupt or ended peer, or the owner's steps at a member's end, write through the owner's view. The service's end is
//! what each member's watcher would publish: the mark, the member's phase PEER_DEAD, then its local interrupt.
//!
//! The stress's requests carry their client, their sequence number and a checksum, and each reply is made from its
//! request: every client must get, in the order of its requests, the reply to each, exactly once.

const std = @import("std");
const testing = std.testing;
const wire_protocol = @import("../wire_protocol.zig");
const platform = @import("../platform.zig");
const caller_module = @import("caller.zig");
const responder_module = @import("responder.zig");
const frames = @import("frames.zig");
const ring_set = @import("../core/ring_set.zig");
const segment = @import("segment.zig");

const Atomic = std.atomic.Value;
const Endpoint = @import("../core/endpoint.zig").Endpoint;
const Hot = @import("../core/endpoint.zig").Hot;
const Session = @import("../core/shared_memory.zig").Session;
const DataError = @import("../core/endpoint.zig").DataError;
const Rings = segment.Rings;

// === The harness ===

fn newEndpoint() !*Endpoint {
    const endpoint = try testing.allocator.create(Endpoint);
    endpoint.* = .{ .hot = std.mem.zeroes(Hot), .io = testing.io, .gpa = testing.allocator, .io_ref = undefined, .handles = undefined };
    endpoint.hot.phase.store(.connected, .monotonic);
    return endpoint;
}

/// A service as its owner sets it up; clients and servers come from `client` and `server`.
pub const Service = struct {
    security: platform.Security,
    handle: ?platform.Handle,
    session: Session,
    layout: segment.Layout,
    /// Each slot's events (Windows), as the owner creates them at the slot's first claim and keeps them.
    reply_events: [wire_protocol.max_service_clients]?platform.SlotWake = @splat(null),
    room_events: [wire_protocol.max_service_clients]?platform.SlotWake = @splat(null),
    server_events: [wire_protocol.max_service_servers]?platform.SlotWake = @splat(null),
    /// The owner's wakes, for its abandons and `rescan`.
    wakes: segment.Wakes,

    pub fn create(capacity: u32, max_message: u32, max_clients: u32, max_servers: u32) !*Service {
        var security = try platform.Security.init();
        errdefer security.deinit();
        const handle: ?platform.Handle = if (platform.passes_segment_handle) try platform.createSegmentHandle() else null;
        errdefer if (handle) |segment_handle| platform.close(segment_handle);
        var session = try segment.create(testing.io, handle, &security, capacity, max_clients, max_servers, max_message);
        errdefer session.unmap();
        const service = try testing.allocator.create(Service);
        service.* = .{
            .security = security,
            .handle = handle,
            .session = session,
            .layout = segment.layoutOf(&session, max_clients, max_servers, max_message),
            .wakes = .init(session.id),
        };
        return service;
    }

    pub fn destroy(service: *Service) void {
        service.wakes.deinit();
        if (platform.wakes_each_slot) {
            for ([_][]?platform.SlotWake{ &service.reply_events, &service.room_events, &service.server_events }) |events| {
                for (events) |event| if (event) |handle| platform.closeSlotWake(handle);
            }
        }
        service.session.unmap();
        if (service.handle) |segment_handle| platform.close(segment_handle);
        service.security.deinit();
        testing.allocator.destroy(service);
    }

    fn offer(service: *const Service) frames.Offer {
        return .{
            .capacity = service.session.capacity,
            .segment_size = service.session.mapping.size,
            .session_id = service.session.id,
            .max_clients = service.layout.max_senders,
            .max_servers = service.layout.max_takers,
            .max_message = service.layout.max_piece,
        };
    }

    /// One of the member's own slot's events: created by the owner at the slot's first claim, opened by the member.
    fn ownWake(service: *Service, events: []?platform.SlotWake, suffix: []const u8, slot: u32) !?platform.SlotWake {
        if (!platform.wakes_each_slot) return null;
        if (events[slot] == null) events[slot] = try platform.createSlotWake(&service.session.id, suffix, slot, &service.security);
        return try platform.openSlotWake(&service.session.id, suffix, slot);
    }

    fn attachView(service: *const Service, session: *Session) !segment.Layout {
        const offered = service.offer();
        session.* = try segment.attach(service.handle, &offered);
        return segment.layoutOf(session, service.layout.max_senders, service.layout.max_takers, service.layout.max_piece);
    }

    /// The client of slot `slot`, joined now.
    pub fn client(service: *Service, slot: u32) !*ClientEnd {
        const end = try testing.allocator.create(ClientEnd);
        errdefer testing.allocator.destroy(end);
        end.reply_wake = try service.ownWake(&service.reply_events, segment.client_reply_wake_suffix, slot);
        end.room_wake = try service.ownWake(&service.room_events, segment.client_room_wake_suffix, slot);
        const control = service.layout.ring(slot);
        control.room_sleeps.store(0, .monotonic);
        control.reply_sleeps.store(0, .monotonic);
        const view = try service.attachView(&end.session);
        errdefer end.session.unmap();
        end.positions = try testing.allocator.alloc(u32, service.layout.cellCount());
        errdefer testing.allocator.free(end.positions);
        end.endpoint = try newEndpoint();
        errdefer testing.allocator.destroy(end.endpoint);
        try end.caller.init(view, slot, end.positions, platform.slotRingEvent(end.room_wake), platform.slotRingEvent(end.reply_wake), service.session.id);
        return end;
    }

    /// The server of slot `slot`, joined now.
    pub fn server(service: *Service, slot: u32) !*ServerEnd {
        const end = try testing.allocator.create(ServerEnd);
        errdefer testing.allocator.destroy(end);
        end.own_wake = try service.ownWake(&service.server_events, segment.server_wake_suffix, slot);
        const words = service.layout.takerSlot(slot);
        words.sleeps.store(0, .monotonic);
        words.claim.store(0, .monotonic);
        Rings.clearIdle(&service.layout, slot);
        const view = try service.attachView(&end.session);
        end.endpoint = try newEndpoint();
        end.responder = .init(view, slot, platform.slotRingEvent(end.own_wake), service.session.id);
        return end;
    }

    /// The service's end, as the owner's close (server.zig `close`) marks it: every member's watcher then reads its
    /// connection's end.
    pub fn markEnded(service: *Service) void {
        service.layout.header.ended.store(1, .seq_cst);
    }

    /// The owner's steps at the end of the server of slot `slot`: the request it held, abandoned, and the client its
    /// intent names, woken (server.zig `keepsSlot`). Whether there was one to abandon.
    pub fn serverEnded(service: *Service, slot: u32) bool {
        defer Rings.wakeIntended(&service.layout, &service.wakes, slot);
        const claim = Rings.heldClaim(&service.layout, slot) orelse return false;
        return Rings.abandonHeld(&service.layout, &service.wakes, slot, claim);
    }

    pub fn readyBits(service: *const Service) u64 {
        return service.layout.header.ready.load(.seq_cst);
    }
};

fn closeWake(wake: ?platform.SlotWake) void {
    if (platform.wakes_each_slot) if (wake) |handle| platform.closeSlotWake(handle);
}

/// A client: its endpoint, its own view of the segment, its payload record and its state.
pub const ClientEnd = struct {
    endpoint: *Endpoint,
    session: Session,
    positions: []u32,
    reply_wake: ?platform.SlotWake,
    room_wake: ?platform.SlotWake,
    caller: caller_module.Caller,

    /// Its close: a reply held in place is let go first.
    pub fn destroy(end: *ClientEnd) void {
        end.caller.replies.release();
        end.caller.deinit();
        closeWake(end.reply_wake);
        closeWake(end.room_wake);
        testing.allocator.destroy(end.endpoint);
        testing.allocator.free(end.positions);
        end.session.unmap();
        testing.allocator.destroy(end);
    }

    pub fn send(end: *ClientEnd, request: []const u8, timeout_ms: c_int) DataError!void {
        return end.caller.sender.send(end.endpoint, request, timeout_ms);
    }

    pub fn recv(end: *ClientEnd, buf: []u8, timeout_ms: c_int) DataError![]u8 {
        var len: usize = undefined;
        try end.caller.replies.recv(end.endpoint, buf, timeout_ms, &len);
        return buf[0..len];
    }

    pub fn recvAcquire(end: *ClientEnd, timeout_ms: c_int) DataError![]const u8 {
        return end.caller.replies.recvAcquire(end.endpoint, timeout_ms);
    }

    /// What its watcher does at the end of its control connection: PEER_DEAD, then the local interrupt of both waits
    /// (the service's end also has the mark, stored first).
    pub fn connectionEnded(end: *ClientEnd) void {
        end.endpoint.hot.phase.store(.peer_dead, .seq_cst);
        end.interrupt();
    }

    pub fn cancel(end: *ClientEnd) void {
        _ = end.endpoint.hot.requests.fetchOr(.{ .cancel = true }, .seq_cst);
        end.interrupt();
    }

    fn interrupt(end: *ClientEnd) void {
        end.caller.sender.interrupt();
        const flag = &end.caller.replies.layout.ring(end.caller.replies.ring).reply_sleeps;
        if (flag.swap(0, .seq_cst) == 1) platform.ringWake(flag, end.caller.replies.reply_event);
    }
};

/// A server: its endpoint, its own view of the segment and its state.
pub const ServerEnd = struct {
    endpoint: *Endpoint,
    session: Session,
    own_wake: ?platform.SlotWake,
    responder: responder_module.Responder,

    /// Its close: a request still held is abandoned first.
    pub fn destroy(end: *ServerEnd) void {
        end.responder.leave();
        end.destroyHolding();
    }

    /// A server whose process ended: nothing abandoned.
    fn destroyHolding(end: *ServerEnd) void {
        end.responder.deinit();
        closeWake(end.own_wake);
        testing.allocator.destroy(end.endpoint);
        end.session.unmap();
        testing.allocator.destroy(end);
    }

    pub fn recv(end: *ServerEnd, buf: []u8, timeout_ms: c_int) DataError![]u8 {
        var len: usize = undefined;
        try end.responder.recv(end.endpoint, buf, timeout_ms, &len);
        return buf[0..len];
    }

    pub fn recvAcquire(end: *ServerEnd, timeout_ms: c_int) DataError![]const u8 {
        return end.responder.recvAcquire(end.endpoint, timeout_ms);
    }

    pub fn reply(end: *ServerEnd, payload: []const u8) DataError!void {
        return end.responder.send(end.endpoint, payload);
    }

    pub fn connectionEnded(end: *ServerEnd) void {
        end.endpoint.hot.phase.store(.peer_dead, .seq_cst);
        end.responder.taker.interrupt();
    }
};

/// A stress request: its sequence number (8 bytes), its client (4), its length (4), a checksum of the rest (4), 4
/// bytes of 0, then bytes made from the sequence number. Its reply (`answerTo`): the same header with the bits of the
/// sequence number inverted, of a length made from it, and a checksum of its own.
const Request = struct {
    const min = 24;

    fn fill(buf: []u8, client: u32, seq: u64) void {
        std.debug.assert(buf.len >= min);
        std.mem.writeInt(u64, buf[0..8], seq, .little);
        std.mem.writeInt(u32, buf[8..12], client, .little);
        std.mem.writeInt(u32, buf[12..16], @intCast(buf.len), .little);
        std.mem.writeInt(u32, buf[20..24], 0, .little);
        var state = (seq ^ @as(u64, client) << 48) *% 0x9e3779b97f4a7c15 +% 1;
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

    const Id = struct { client: u32, seq: u64 };

    /// The message's client and sequence number, if it is whole.
    fn check(buf: []const u8) ?Id {
        if (buf.len < min) return null;
        if (std.mem.readInt(u32, buf[12..16], .little) != buf.len) return null;
        if (std.mem.readInt(u32, buf[16..20], .little) != sum(buf)) return null;
        return .{ .client = std.mem.readInt(u32, buf[8..12], .little), .seq = std.mem.readInt(u64, buf[0..8], .little) };
    }

    /// Request `seq`'s size, from 24 to `max_message`, most of them small.
    fn sizeOf(seq: u64, max_message: usize) usize {
        const bits = (seq *% 0x2545f4914f6cdd1d) >> 40;
        if (bits % 8 == 0) return min + @as(usize, @intCast(bits / 8 % (max_message - min + 1)));
        return min + @as(usize, @intCast(bits % @min(113, max_message - min + 1)));
    }

    /// The reply to request `id`, into `buf` (at least `max_message` bytes): its length.
    fn answerTo(buf: []u8, id: Id, max_message: usize) usize {
        const len = sizeOf(~id.seq, max_message);
        fill(buf[0..len], id.client, ~id.seq);
        return len;
    }
};

fn nowMs() i64 {
    return std.Io.Clock.awake.now(testing.io).toMilliseconds();
}

/// Until a receive of `end` has begun a sleep for an answer since its count of sleeps was `since`: it is past every
/// look, so only a wake ends that sleep before its time.
fn awaitAsleep(end: *const ClientEnd, since: u32) void {
    while (end.caller.replies.sleeps.load(.seq_cst) == since) std.Thread.yield() catch {};
}

/// Until a send of `end` has begun a sleep for room since its count was `since`.
fn awaitRoomSleep(end: *const ClientEnd, since: u32) void {
    while (end.caller.sender.sleeps.load(.seq_cst) == since) std.Thread.yield() catch {};
}

/// Sleeps for an answer and for room last a minute unless woken: a test of a wake then fails if it doesn't come.
fn longSleeps(end: *ClientEnd) void {
    end.caller.replies.longest_sleep_ms = 60_000;
    end.caller.replies.spin_iters = 0;
    end.caller.sender.first_slice_ms = 60_000;
    end.caller.sender.longest_slice_ms = 60_000;
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

/// Takes the next request on `server` and replies to it with "re:" and the request.
fn echo(server: *ServerEnd) !void {
    var buf: [1024]u8 = undefined;
    const request = try server.recv(&buf, 0);
    var answer: [1027]u8 = undefined;
    @memcpy(answer[0..3], "re:");
    @memcpy(answer[3..][0..request.len], request);
    try server.reply(answer[0 .. 3 + request.len]);
}

fn expectReply(client: *ClientEnd, expected: []const u8) !void {
    var buf: [1024]u8 = undefined;
    try testing.expectEqualStrings(expected, try client.recv(&buf, 0));
}

// === One thread ===

test "a request and its reply: the server takes it, replies into its room, and the client receives it; in order, many laps of cells and data" {
    const service = try Service.create(4096, 256, 1, 1);
    defer service.destroy();
    const client = try service.client(0);
    defer client.destroy();
    const server = try service.server(0);
    defer server.destroy();
    try testing.expectEqual(@as(u64, 256), service.layout.replyRoom());
    const ring = service.layout.ring(0);
    var request: [256]u8 = undefined;
    var buf: [256]u8 = undefined;
    // 64 cells and 4 KiB of data: 1000 requests are many laps of both
    for (0..1000) |index| {
        const size = 1 + index % 256;
        for (request[0..size], 0..) |*byte, offset| byte.* = @truncate(index *% 31 +% offset);
        try client.send(request[0..size], 0);
        try testing.expectEqual(@as(u64, index + 1), ring.tail.load(.seq_cst));
        const cell = service.layout.cell(0, index);
        // The request after its reply's room, which the record keeps as the reservation's start
        try testing.expectEqual(client.caller.sender.positions.lowOf(index) + 256, @as(u32, @truncate(cell.payload_position.load(.seq_cst))));
        try testing.expectEqualSlices(u8, request[0..size], try server.recv(&buf, 0));
        try testing.expectEqual(ring_set.claimed(index, 0), cell.state.load(.seq_cst));
        // The reply: the request reversed, as long as it
        var answer: [256]u8 = undefined;
        for (answer[0..size], 0..) |*byte, offset| byte.* = request[size - 1 - offset];
        try server.reply(answer[0..size]);
        try testing.expectEqual(ring_set.done(index, 0), cell.state.load(.seq_cst));
        try testing.expectEqual(@as(u32, @backingInt(wire_protocol.ServiceOutcome.replied)), cell.outcome.load(.seq_cst));
        try testing.expectEqualSlices(u8, answer[0..size], try client.recv(&buf, 0));
        try testing.expectError(error.Timeout, client.recv(&buf, 0));
        try testing.expectError(error.Timeout, server.recv(&buf, 0));
    }
}

test "a server's spin: alone, or in a pool larger than its clients, it looks after every hint; in a pool no larger, it paces it as a queue's consumers do where pools are paced" {
    const stream_pace = @import("../core/wait.zig").stream_pace;
    const pool_paced = @import("../core/taker.zig").pool_paced;
    const Case = struct { max_servers: u32, clients: u32, servers: u32, mode: enum { paced, alone, surplus } };
    for ([_]Case{
        .{ .max_servers = 1, .clients = 0, .servers = 0, .mode = .alone }, // one server slot: the counts aren't read
        .{ .max_servers = 2, .clients = 1, .servers = 1, .mode = .alone },
        .{ .max_servers = 2, .clients = 2, .servers = 2, .mode = if (pool_paced) .paced else .alone }, // x86 only: paced
        .{ .max_servers = 2, .clients = 1, .servers = 2, .mode = .surplus },
    }) |case| {
        const service = try Service.create(4096, 64, 2, case.max_servers);
        defer service.destroy();
        segment.Rings.showJoined(&service.layout, case.clients, case.servers); // as the owner shows them
        const client = try service.client(0);
        defer client.destroy();
        const server = try service.server(0);
        defer server.destroy();
        const taker = &server.responder.taker;
        try testing.expectEqualStrings(@tagName(case.mode), @tagName(taker.spinMode()));
        try testing.expectEqual(@as(u32, 1), taker.pace);
        // A request there at every first look: a paced server doubles its pace each time, up to the stream's
        try client.send("ping", 0);
        var want: u32 = 1;
        for (0..12) |_| {
            try testing.expect(taker.spin());
            if (case.mode == .paced) want = @min(want * 2, stream_pace);
            try testing.expectEqual(want, taker.pace);
        }
        var buf: [64]u8 = undefined;
        try testing.expectEqualStrings("ping", try server.recv(&buf, 0));
        try server.reply("PING");
        try testing.expectEqualStrings("PING", try client.recv(&buf, 0));
    }
}

test "several requests in flight: servers answer them in any order, and the client receives the replies in the order of its requests" {
    const service = try Service.create(8192, 64, 1, 2);
    defer service.destroy();
    const client = try service.client(0);
    defer client.destroy();
    const first = try service.server(0);
    defer first.destroy();
    const second = try service.server(1);
    defer second.destroy();
    for ([_][]const u8{ "a", "b", "c" }) |request| try client.send(request, 0);
    var buf: [64]u8 = undefined;
    try testing.expectEqualStrings("a", try first.recv(&buf, 0));
    try testing.expectEqualStrings("b", try second.recv(&buf, 0));
    // A server holds one request at a time
    try testing.expectError(error.Invalid, second.recv(&buf, 0));
    try second.reply("re:b");
    // The reply to b waits behind a's
    try testing.expectError(error.Timeout, client.recv(&buf, 0));
    try echo(second); // c
    try testing.expectError(error.Timeout, client.recv(&buf, 0));
    try first.reply("re:a");
    try expectReply(client, "re:a");
    try expectReply(client, "re:b");
    try expectReply(client, "re:c");
    try testing.expectError(error.Timeout, client.recv(&buf, 0));
}

test "the servers take each client's requests in the order it sent them, from every client" {
    const service = try Service.create(8192, 64, 3, 1);
    defer service.destroy();
    var clients: [3]*ClientEnd = undefined;
    for (&clients, 0..) |*client, slot| client.* = try service.client(@intCast(slot));
    defer for (clients) |client| client.destroy();
    const server = try service.server(0);
    defer server.destroy();
    for (0..10) |index| for (clients, 0..) |client, slot| try client.send(&.{ @intCast(slot), @intCast(index) }, 0);
    try testing.expectEqual(@as(u64, 0b111), service.readyBits());
    var next: [3]u8 = @splat(0);
    var buf: [64]u8 = undefined;
    for (0..30) |_| {
        const request = try server.recv(&buf, 0);
        try testing.expectEqual(next[request[0]], request[1]);
        next[request[0]] += 1;
        try server.reply(request);
    }
    try testing.expectEqual([3]u8{ 10, 10, 10 }, next);
    for (clients, 0..) |client, slot| for (0..10) |index| try testing.expectEqualSlices(u8, &.{ @intCast(slot), @intCast(index) }, try client.recv(&buf, 0));
}

test "a server's calls: a reply without a request, or one past max_message, an empty reply room, a receive while holding; a client's sends past max_message" {
    const service = try Service.create(4096, 100, 1, 1);
    defer service.destroy();
    const client = try service.client(0);
    defer client.destroy();
    const server = try service.server(0);
    defer server.destroy();
    const responder = &server.responder;
    var big: [101]u8 = @splat(7);
    var buf: [128]u8 = undefined;
    try testing.expectError(error.Invalid, server.reply("x"));
    try testing.expectError(error.Invalid, responder.sendAcquire(server.endpoint, 1));
    try testing.expectError(error.Invalid, responder.sendCommit(server.endpoint, 1));
    try testing.expectError(error.TooLarge, client.send(&big, 0));
    try testing.expectError(error.TooLarge, client.caller.sender.sendAcquire(client.endpoint, 101, 0));
    try client.send(big[0..100], 0);
    try testing.expectEqual(@as(usize, 100), (try server.recv(&buf, 0)).len);
    try testing.expectError(error.Invalid, server.recv(&buf, 0));
    try testing.expectError(error.Invalid, server.recvAcquire(0));
    try testing.expectError(error.TooLarge, server.reply(&big));
    try testing.expectError(error.TooLarge, responder.sendAcquire(server.endpoint, 101));
    try testing.expectError(error.Invalid, responder.sendAcquire(server.endpoint, 0));
    // The request is still held: a reply goes
    try server.reply(big[0..100]);
    try testing.expectError(error.Invalid, server.reply("x"));
    try testing.expectEqual(@as(usize, 100), (try client.recv(&buf, 0)).len);
}

test "TooLarge takes nothing: a request stays for any server, a reply for the client's next receive" {
    const service = try Service.create(4096, 200, 1, 2);
    defer service.destroy();
    const client = try service.client(0);
    defer client.destroy();
    const short = try service.server(0);
    defer short.destroy();
    const long = try service.server(1);
    defer long.destroy();
    try client.send(&@as([150]u8, @splat(1)), 0);
    var small: [100]u8 = undefined;
    var len: usize = 0;
    try testing.expectError(error.TooLarge, short.responder.recv(short.endpoint, &small, 0, &len));
    try testing.expectEqual(@as(usize, 150), len);
    var buf: [200]u8 = undefined;
    try testing.expectEqual(@as(usize, 150), (try long.recv(&buf, 0)).len);
    try long.reply(&@as([180]u8, @splat(2)));
    try testing.expectError(error.TooLarge, client.caller.replies.recv(client.endpoint, &small, 0, &len));
    try testing.expectEqual(@as(usize, 180), len);
    // An empty buffer asks for the length only
    var empty: [0]u8 = .{};
    try testing.expectError(error.TooLarge, client.caller.replies.recv(client.endpoint, &empty, 0, &len));
    try testing.expectEqual(@as(usize, 180), len);
    try testing.expect(std.mem.allEqual(u8, try client.recv(&buf, 0), 2));
}

test "in place both ways: a request held until the reply, a reply room in place, a reply held until the release or the next receive" {
    const service = try Service.create(4096, 128, 1, 1);
    defer service.destroy();
    const client = try service.client(0);
    defer client.destroy();
    const server = try service.server(0);
    defer server.destroy();
    for (0..3) |round| {
        const room = try client.caller.sender.sendAcquire(client.endpoint, 100, 0);
        try testing.expectEqual(@as(usize, 0), @intFromPtr(room.ptr) % 64);
        @memcpy(room[0..5], "ask-0");
        room[4] = '0' + @as(u8, @intCast(round));
        try client.caller.sender.sendCommit(client.endpoint, 5);
        const request = try server.recvAcquire(0);
        try testing.expectEqual(@as(usize, 0), @intFromPtr(request.ptr) % 64);
        try testing.expectEqualStrings("ask-", request[0..4]);
        // The reply's room is just before the request: writing it leaves the request whole
        const answer = try server.responder.sendAcquire(server.endpoint, 128);
        try testing.expectEqual(@intFromPtr(request.ptr) - 128, @intFromPtr(answer.ptr));
        @memset(answer, 0xee);
        @memcpy(answer[0..3], "re:");
        @memcpy(answer[3..8], request[0..5]);
        try testing.expectError(error.Invalid, server.responder.sendCommit(server.endpoint, 129));
        try testing.expectError(error.Invalid, server.responder.sendCommit(server.endpoint, 0));
        try server.responder.sendCommit(server.endpoint, 8); // the reservation stayed
        try testing.expectError(error.Invalid, server.responder.sendCommit(server.endpoint, 8));
        const got = try client.recvAcquire(0);
        try testing.expectEqual(@as(usize, 0), @intFromPtr(got.ptr) % 64);
        try testing.expectEqualStrings("re:ask-", got[0..7]);
        try testing.expectEqual('0' + @as(u8, @intCast(round)), got[7]);
        try testing.expectEqual(@as(u64, round), client.caller.replies.next.load(.seq_cst)); // held: not consumed
        if (round == 0) client.caller.replies.release();
        // Otherwise the next send's receive lets it go
    }
    client.caller.replies.release();
    try testing.expectEqual(@as(u64, 3), client.caller.replies.next.load(.seq_cst));
}

test "room: a request holds its reply's room and its own until its reply is consumed; a reply held in place holds it" {
    // max_message 1024: each request takes 1024 + 64 bytes of the 4 KiB ring, so 3 fit before the skip to its start
    const service = try Service.create(4096, 1024, 1, 1);
    defer service.destroy();
    const client = try service.client(0);
    defer client.destroy();
    const server = try service.server(0);
    defer server.destroy();
    for (0..3) |index| try client.send(&.{@intCast(index)}, 0);
    try testing.expectError(error.Timeout, client.send("x", 0));
    // Taken and answered, the requests still hold their room until the client consumed the answers
    for (0..3) |_| try echo(server);
    try testing.expectError(error.Timeout, client.send("x", 0));
    try testing.expectEqual(@as(u8, 0), (try client.recvAcquire(0))[3]);
    try testing.expectError(error.Timeout, client.send("x", 0)); // held in place
    client.caller.replies.release();
    try client.send(&.{3}, 0);
    try testing.expectError(error.Timeout, client.send("x", 0));
    try expectReply(client, &.{ 'r', 'e', ':', 1 });
    try expectReply(client, &.{ 'r', 'e', ':', 2 });
    try echo(server);
    try expectReply(client, &.{ 'r', 'e', ':', 3 });
    var buf: [64]u8 = undefined;
    try testing.expectError(error.Timeout, client.recv(&buf, 0));
}

test "a server never waits for a client: one that fills its ring and never receives leaves the servers serving the others" {
    const service = try Service.create(4096, 1024, 2, 1);
    defer service.destroy();
    const stuck = try service.client(0);
    defer stuck.destroy();
    const other = try service.client(1);
    defer other.destroy();
    const server = try service.server(0);
    defer server.destroy();
    for (0..3) |index| try stuck.send(&.{@intCast(index)}, 0);
    for (0..3) |_| try echo(server);
    for (0..20) |index| {
        try other.send(&.{@intCast(index)}, 0);
        try echo(server);
        try expectReply(other, &.{ 'r', 'e', ':', @intCast(index) });
    }
    try expectReply(stuck, &.{ 'r', 'e', ':', 0 });
}

test "room: a client asleep for room is woken by its own receive of a reply; a cancel or the service's end ends its wait" {
    const service = try Service.create(4096, 1024, 2, 1);
    defer service.destroy();
    const client = try service.client(0);
    defer client.destroy();
    const cancelled = try service.client(1);
    defer cancelled.destroy();
    const server = try service.server(0);
    defer server.destroy();
    // Each ring full of answered requests whose replies aren't consumed
    for ([_]*ClientEnd{ client, cancelled }) |end| {
        longSleeps(end);
        for (0..3) |index| try end.send(&.{@intCast(index)}, 0);
        for (0..3) |_| try echo(server);
    }
    const Sending = struct {
        fn run(end: *ClientEnd, result: *DataError!void) void {
            result.* = end.send("late", -1);
        }
    };
    for ([_]enum { reply, cancel, end }{ .reply, .cancel, .end }) |how| {
        const end = if (how == .cancel) cancelled else client;
        var sent: DataError!void = error.Timeout;
        const since = end.caller.sender.sleeps.load(.seq_cst);
        const thread = try std.Thread.spawn(.{}, Sending.run, .{ end, &sent });
        awaitRoomSleep(end, since);
        const begun = nowMs();
        switch (how) {
            .reply => try expectReply(end, &.{ 'r', 'e', ':', 0 }),
            .cancel => end.cancel(),
            .end => {
                service.markEnded();
                end.connectionEnded();
            },
        }
        thread.join();
        try testing.expect(nowMs() - begun < 30_000);
        switch (how) {
            .reply => try sent,
            .cancel => try testing.expectError(error.Cancelled, sent),
            .end => try testing.expectError(error.Disconnected, sent),
        }
    }
}

test "the owner abandons the request an ended server held: the client's receive is Abandoned, with no reply, and the next goes on" {
    const service = try Service.create(4096, 64, 1, 2);
    defer service.destroy();
    const client = try service.client(0);
    defer client.destroy();
    const dying = try service.server(0);
    const server = try service.server(1);
    defer server.destroy();
    for ([_][]const u8{ "a", "b" }) |request| try client.send(request, 0);
    var buf: [64]u8 = undefined;
    try testing.expectEqualStrings("a", try dying.recv(&buf, 0));
    // Its process ends holding it: the owner finds it by its intent and abandons it, once
    dying.destroyHolding();
    try testing.expect(service.serverEnded(0));
    try testing.expect(!service.serverEnded(0));
    var len: usize = 99;
    try testing.expectError(error.Abandoned, client.caller.replies.recv(client.endpoint, &buf, 0, &len));
    try testing.expectEqual(@as(usize, 0), len);
    try echo(server);
    try expectReply(client, "re:b");
    // A server that stored its done just before it ended: its reply stands, nothing is abandoned
    try client.send("c", 0);
    const answered = try service.server(0);
    try echo(answered);
    answered.destroyHolding();
    try testing.expect(!service.serverEnded(0));
    try expectReply(client, "re:c");
    // In place: Abandoned too, and nothing held
    try client.send("d", 0);
    const holder = try service.server(0);
    _ = try holder.recvAcquire(0);
    holder.destroyHolding();
    try testing.expect(service.serverEnded(0));
    try testing.expectError(error.Abandoned, client.recvAcquire(0));
    try testing.expect(!client.caller.replies.held);
    try testing.expectEqual(@as(u64, 4), client.caller.replies.next.load(.seq_cst));
}

test "a server's close abandons the request it holds; after the service's end it abandons nothing" {
    const service = try Service.create(4096, 64, 1, 1);
    defer service.destroy();
    const client = try service.client(0);
    defer client.destroy();
    try client.send("a", 0);
    const closing = try service.server(0);
    var buf: [64]u8 = undefined;
    _ = try closing.recv(&buf, 0);
    closing.destroy();
    try testing.expect(!service.serverEnded(0)); // nothing left for the owner
    try testing.expectError(error.Abandoned, client.recv(&buf, 0));
    // After the mark: the close leaves the request as it is, and the client's receive is Disconnected
    try client.send("b", 0);
    const late = try service.server(0);
    _ = try late.recv(&buf, 0);
    service.markEnded();
    try testing.expectError(error.Disconnected, late.reply("re:b"));
    late.destroy();
    try testing.expectEqual(ring_set.claimed(1, 0), service.layout.cell(0, 1).state.load(.seq_cst));
    try testing.expectError(error.Disconnected, client.recv(&buf, 0));
}

test "the owner drops an ended client's requests no server took; one a server holds is answered into its ring, then the slot settles" {
    const service = try Service.create(4096, 64, 1, 1);
    defer service.destroy();
    const client = try service.client(0);
    for ([_][]const u8{ "a", "b", "c" }) |request| try client.send(request, 0);
    const server = try service.server(0);
    defer server.destroy();
    var buf: [64]u8 = undefined;
    try testing.expectEqualStrings("a", try server.recv(&buf, 0));
    client.destroy();
    try testing.expectEqual(@as(u64, 2), Rings.dropUnclaimed(&service.layout, 0));
    try testing.expectEqual(@as(u64, 0), Rings.dropUnclaimed(&service.layout, 0));
    const ring = service.layout.ring(0);
    try testing.expectEqual(@as(u64, 3), ring.head.load(.seq_cst));
    for (1..3) |index| try testing.expectEqual(ring_set.done(index, 0), service.layout.cell(0, index).state.load(.seq_cst));
    try testing.expectEqual(.busy, Rings.ringSettled(&service.layout, 0));
    try server.reply("re:a"); // OK: its client has left
    try testing.expectEqual(.settled, Rings.ringSettled(&service.layout, 0));
    try testing.expectError(error.Timeout, server.recv(&buf, 0)); // nothing more for it
    // The slot's next client starts at its ring's tail, and its first request is answered as any
    const next = try service.client(0);
    defer next.destroy();
    try next.send("d", 0);
    try echo(server);
    try expectReply(next, "re:d");
}

test "a corrupt client's request (its reply's room outside the ring, past the ring's end, empty or too long) is answered as abandoned: Invalid, nothing written" {
    const service = try Service.create(4096, 128, 1, 1);
    defer service.destroy();
    const client = try service.client(0);
    defer client.destroy();
    const server = try service.server(0);
    defer server.destroy();
    const layout = &service.layout;
    var buf: [256]u8 = undefined;
    const cases = [_]struct { position: u64, len: u32 }{
        .{ .position = 0, .len = 10 }, // its reply's room before the data ring
        .{ .position = 64, .len = 10 },
        .{ .position = 4096 * 5 + 127, .len = 10 },
        .{ .position = 4096 - 8, .len = 10 }, // past the ring's end
        .{ .position = 128, .len = 0 },
        .{ .position = 128, .len = 129 },
    };
    // Every byte of the segment outside the cells, the control words and the header is a sentinel
    for (cases) |case| {
        const index = client.caller.sender.next;
        try client.send("x", 0);
        const cell = layout.cell(0, index);
        cell.payload_position.store(case.position, .seq_cst);
        cell.len.store(case.len, .seq_cst);
        @memset(layout.bytes(0, 0, 4096), 0x5a);
        try testing.expectError(error.Invalid, server.recv(&buf, 0));
        try testing.expect(std.mem.allEqual(u8, layout.bytes(0, 0, 4096), 0x5a));
        try testing.expectEqual(@as(u32, @backingInt(wire_protocol.ServiceOutcome.abandoned)), cell.outcome.load(.seq_cst));
        try testing.expectError(error.Abandoned, client.recv(&buf, 0));
        // The server holds nothing: it takes the next
        try client.send("ok", 0);
        try echo(server);
        try expectReply(client, "re:ok");
    }
}

test "a corrupt server's answer (no outcome, an unknown one, an empty or too long reply) is Invalid to the client, consumed, and the next goes on" {
    const service = try Service.create(4096, 128, 1, 1);
    defer service.destroy();
    const client = try service.client(0);
    defer client.destroy();
    const layout = &service.layout;
    const cases = [_]struct { outcome: u32, reply_len: u32 }{
        .{ .outcome = 0, .reply_len = 5 },
        .{ .outcome = 7, .reply_len = 5 },
        .{ .outcome = 1, .reply_len = 0 },
        .{ .outcome = 1, .reply_len = 129 },
        .{ .outcome = 1, .reply_len = 0xffff_ffff },
    };
    var buf: [256]u8 = undefined;
    for (cases, 0..) |case, index| {
        try client.send("x", 0);
        const cell = layout.cell(0, index);
        cell.outcome.store(case.outcome, .seq_cst);
        cell.reply_len.store(case.reply_len, .seq_cst);
        cell.state.store(ring_set.done(index, 3), .seq_cst);
        if (index % 2 == 0) try testing.expectError(error.Invalid, client.recv(&buf, 0)) else try testing.expectError(error.Invalid, client.recvAcquire(0));
        try testing.expectEqual(@as(u64, index + 1), client.caller.replies.next.load(.seq_cst));
    }
    // A done of another request's index answers nothing
    try client.send("y", 0);
    layout.cell(0, cases.len).state.store(ring_set.done(cases.len + 64, 0), .seq_cst);
    try testing.expectError(error.Timeout, client.recv(&buf, 0));
}

test "the service's end: a client receives the replies done before it, in order, then Disconnected; servers' receives and replies are Disconnected" {
    const service = try Service.create(4096, 64, 1, 2);
    defer service.destroy();
    const client = try service.client(0);
    defer client.destroy();
    const first = try service.server(0);
    defer first.destroy();
    const second = try service.server(1);
    defer second.destroy();
    for ([_][]const u8{ "a", "b", "c", "d" }) |request| try client.send(request, 0);
    try echo(first); // a
    var buf: [64]u8 = undefined;
    _ = try second.recv(&buf, 0); // b, never answered
    try echo(first); // c: answered, but after b's
    service.markEnded();
    for ([_]*ServerEnd{ first, second }) |server| server.connectionEnded();
    client.connectionEnded();
    try testing.expectError(error.Disconnected, first.recv(&buf, 0)); // d stays: no drain
    try testing.expectError(error.Disconnected, second.reply("re:b"));
    try testing.expectEqual(ring_set.claimed(1, 1), service.layout.cell(0, 1).state.load(.seq_cst)); // nothing written
    try testing.expectError(error.Disconnected, client.send("e", 0));
    try expectReply(client, "re:a");
    try testing.expectError(error.Disconnected, client.recv(&buf, 1000));
    try testing.expectError(error.Disconnected, client.recv(&buf, 0));
}

test "a client's own failure (its phase without the mark) is Disconnected at once, its replies done or not; a server's too" {
    const service = try Service.create(4096, 64, 1, 1);
    defer service.destroy();
    const client = try service.client(0);
    defer client.destroy();
    const server = try service.server(0);
    defer server.destroy();
    try client.send("a", 0);
    try echo(server);
    client.connectionEnded();
    var buf: [64]u8 = undefined;
    try testing.expectError(error.Disconnected, client.recv(&buf, 0));
    try testing.expectError(error.Disconnected, client.send("b", 0));
    server.connectionEnded();
    try testing.expectError(error.Disconnected, server.recv(&buf, 0));
}

test "a client's Disconnected at the end is final: a reply or an abandon whose server looked at the mark before it is never received" {
    const service = try Service.create(4096, 64, 1, 2);
    defer service.destroy();
    const client = try service.client(0);
    defer client.destroy();
    const replying = try service.server(0);
    defer replying.destroyHolding();
    const leaving = try service.server(1);
    defer leaving.destroyHolding();
    for ([_][]const u8{ "a", "b" }) |request| try client.send(request, 0);
    var buf: [64]u8 = undefined;
    _ = try replying.recv(&buf, 0); // a
    _ = try leaving.recv(&buf, 0); // b
    // Both servers passed their looks at the mark (a reply's `open`, a close's `leave`) before the end
    const a = replying.responder.held.request().?;
    const b = leaving.responder.held.request().?;
    service.markEnded();
    client.connectionEnded();
    try testing.expectError(error.Disconnected, client.recv(&buf, 0));
    // a's reply, copied meanwhile, then its done
    @memcpy(service.layout.bytes(0, a.position - service.layout.replyRoom(), 4), "re:a");
    Rings.answer(&service.layout, &service.wakes, 0, a, .replied, 4);
    try testing.expectError(error.Disconnected, client.recv(&buf, 0));
    try testing.expectError(error.Disconnected, client.recvAcquire(0));
    // b's close abandons it
    Rings.answer(&service.layout, &service.wakes, 1, b, .abandoned, 0);
    try testing.expectError(error.Disconnected, client.recv(&buf, 0));
    try testing.expectEqual(@as(u64, 0), client.caller.replies.next.load(.seq_cst)); // nothing consumed
}

test "a client's Disconnected at its own failure is final: a reply after it, and the mark after that, change nothing" {
    const service = try Service.create(4096, 64, 1, 1);
    defer service.destroy();
    const failing = try service.client(0);
    defer failing.destroy();
    const server = try service.server(0);
    defer server.destroy();
    var buf: [64]u8 = undefined;
    try failing.send("c", 0);
    _ = try server.recv(&buf, 0);
    failing.connectionEnded();
    try testing.expectError(error.Disconnected, failing.recv(&buf, 0));
    try server.reply("re:c");
    try testing.expectError(error.Disconnected, failing.recv(&buf, 0));
    service.markEnded(); // another member's watcher stores the mark later
    try testing.expectError(error.Disconnected, failing.recv(&buf, 0));
}

test "a reply wakes the client asleep in its receive; a cancel, and the end, wake it too" {
    const service = try Service.create(4096, 64, 2, 1);
    defer service.destroy();
    const client = try service.client(0);
    defer client.destroy();
    const cancelled = try service.client(1);
    defer cancelled.destroy();
    const server = try service.server(0);
    defer server.destroy();
    longSleeps(client);
    longSleeps(cancelled);
    const Receiving = struct {
        fn run(end: *ClientEnd, result: *DataError!void) void {
            var buf: [64]u8 = undefined;
            result.* = if (end.recv(&buf, -1)) |reply| (if (std.mem.eql(u8, reply, "re:a")) {} else error.Invalid) else |err| err;
        }
    };
    for ([_]enum { reply, cancel, end }{ .reply, .cancel, .end }) |how| {
        const end = if (how == .cancel) cancelled else client;
        if (how != .end) try end.send("a", 0);
        var received: DataError!void = error.Timeout;
        const since = end.caller.replies.sleeps.load(.seq_cst);
        const thread = try std.Thread.spawn(.{}, Receiving.run, .{ end, &received });
        awaitAsleep(end, since);
        const begun = nowMs();
        switch (how) {
            .reply => try echo(server),
            .cancel => end.cancel(),
            .end => {
                service.markEnded();
                end.connectionEnded();
            },
        }
        thread.join();
        try testing.expect(nowMs() - begun < 30_000);
        switch (how) {
            .reply => try received,
            .cancel => try testing.expectError(error.Cancelled, received),
            .end => try testing.expectError(error.Disconnected, received),
        }
    }
}

test "a client's receive with no request outstanding times out, though its cell holds the done of a request a lap before; then a send, and the receive returns its reply" {
    const service = try Service.create(4096, 64, 1, 1);
    defer service.destroy();
    const client = try service.client(0);
    defer client.destroy();
    const server = try service.server(0);
    defer server.destroy();
    // More than a lap of the 64 cells: the next request's cell holds request 6's done
    for (0..70) |_| {
        try client.send("q", 0);
        try echo(server);
        try expectReply(client, "re:q");
    }
    try testing.expectEqual(ring_set.done(6, 0), service.layout.cell(0, 70).state.load(.seq_cst));
    var buf: [64]u8 = undefined;
    for ([_]c_int{ 0, 20 }) |timeout_ms| try testing.expectError(error.Timeout, client.recv(&buf, timeout_ms));
    try testing.expectError(error.Timeout, client.recvAcquire(0));
    try testing.expectEqual(@as(u64, 70), client.caller.replies.next.load(.seq_cst)); // nothing consumed
    try client.send("late", 0);
    try echo(server);
    try expectReply(client, "re:late");
}

test "the owner drops an ended client's requests while servers take them: each is dropped or answered once, the ring settles, and the slot's next client gets its own replies" {
    const capacity = 65536; // 64 bytes of request and 64 of reply room each: 512 requests in the ring
    const count = 500;
    const servers = 4;
    const service = try Service.create(capacity, 64, 1, servers);
    defer service.destroy();
    const client = try service.client(0);
    for (0..count) |seq| {
        var request: [8]u8 = undefined;
        std.mem.writeInt(u64, &request, seq, .little);
        try client.send(&request, 0);
    }
    const Taking = struct {
        end: *ServerEnd,
        answered: *[count]Atomic(u32),
        stop: *const Atomic(bool),
        failure: *Failure,
        thread: std.Thread = undefined,

        fn run(taking: *@This()) void {
            var buf: [64]u8 = undefined;
            while (true) {
                const request = taking.end.recv(&buf, 0) catch |err| switch (err) {
                    error.Timeout => if (taking.stop.load(.seq_cst)) return else continue,
                    else => return taking.failure.set("a server's receive failed"),
                };
                if (request.len != 8) return taking.failure.set("a request was torn");
                const seq = std.mem.readInt(u64, request[0..8], .little);
                if (seq >= count) return taking.failure.set("a request was torn");
                _ = taking.answered[seq].fetchAdd(1, .seq_cst);
                taking.end.reply("re") catch return taking.failure.set("a reply failed");
            }
        }
    };
    var answered: [count]Atomic(u32) = @splat(.init(0));
    var stop: Atomic(bool) = .init(false);
    var failure: Failure = .{};
    var takers: [servers]Taking = undefined;
    for (&takers, 0..) |*taking, slot| {
        taking.* = .{ .end = try service.server(@intCast(slot)), .answered = &answered, .stop = &stop, .failure = &failure };
    }
    defer for (&takers) |*taking| taking.end.destroy();
    for (&takers) |*taking| taking.thread = try std.Thread.spawn(.{}, Taking.run, .{taking});
    // The client's process ends, and the owner drops what no server took meanwhile
    client.destroy();
    while (answered[0].load(.seq_cst) == 0) std.Thread.yield() catch {}; // the servers have begun
    const dropped = Rings.dropUnclaimed(&service.layout, 0);
    while (Rings.ringSettled(&service.layout, 0) != .settled) std.Thread.yield() catch {};
    stop.store(true, .seq_cst);
    for (&takers) |*taking| taking.thread.join();
    try failure.check();
    var answered_count: u64 = 0;
    for (&answered) |*times| {
        try testing.expect(times.load(.seq_cst) <= 1);
        answered_count += times.load(.seq_cst);
    }
    try testing.expectEqual(@as(u64, count), answered_count + dropped);
    for (0..count) |index| try testing.expectEqual(ring_set.Phase.done, ring_set.phaseOf(service.layout.cell(0, index).state.load(.seq_cst)));
    // The slot's next client starts at the ring's tail, and its first reply is its first request's
    const next = try service.client(0);
    defer next.destroy();
    try next.send("first", 0);
    try echo(takers[1].end);
    try expectReply(next, "re:first");
}

test "a client that leaves holding a reply in place, its later requests taken: the slot drains, and its next client's replies answer its own requests" {
    const service = try Service.create(4096, 64, 1, 3);
    defer service.destroy();
    const client = try service.client(0);
    const first = try service.server(0);
    defer first.destroy();
    const second = try service.server(1);
    defer second.destroy();
    const third = try service.server(2);
    defer third.destroy();
    for ([_][]const u8{ "a", "b", "c", "d" }) |request| try client.send(request, 0);
    try echo(first); // a
    var buf: [64]u8 = undefined;
    try testing.expectEqualStrings("b", try second.recv(&buf, 0));
    try testing.expectEqualStrings("c", try third.recv(&buf, 0));
    try testing.expectEqualStrings("re:a", try client.recvAcquire(0));
    // Its close lets the reply go; the owner drops d, which no server took
    client.destroy();
    try testing.expectEqual(@as(u64, 1), Rings.dropUnclaimed(&service.layout, 0));
    try testing.expectEqual(.busy, Rings.ringSettled(&service.layout, 0));
    try third.reply("re:c"); // the replies of a client that left
    try testing.expectEqual(.busy, Rings.ringSettled(&service.layout, 0));
    try second.reply("re:b");
    try testing.expectEqual(.settled, Rings.ringSettled(&service.layout, 0));
    const next = try service.client(0);
    defer next.destroy();
    try testing.expectEqual(@as(u64, 4), next.caller.replies.next.load(.seq_cst));
    for ([_][]const u8{ "x", "y" }) |request| try next.send(request, 0);
    try echo(second); // x
    try echo(first); // y
    try expectReply(next, "re:x");
    try expectReply(next, "re:y");
}

test "a server's wake that lands after its client's slot went to another client is spurious: that client looks, sleeps again, and gets its own reply" {
    const service = try Service.create(4096, 64, 1, 2);
    defer service.destroy();
    const old = try service.client(0);
    const late = try service.server(0);
    defer late.destroy();
    const server = try service.server(1);
    defer server.destroy();
    try old.send("a", 0);
    try echo(late);
    try expectReply(old, "re:a");
    old.destroy();
    try testing.expectEqual(.settled, Rings.ringSettled(&service.layout, 0));
    const client = try service.client(0);
    defer client.destroy();
    longSleeps(client);
    try client.send("b", 0);
    const Receiving = struct {
        fn run(end: *ClientEnd, result: *DataError!void) void {
            var reply: [64]u8 = undefined;
            result.* = if (end.recv(&reply, 30_000)) |got| (if (std.mem.eql(u8, got, "re:b")) {} else error.Invalid) else |err| err;
        }
    };
    var received: DataError!void = error.Timeout;
    const since = client.caller.replies.sleeps.load(.seq_cst);
    const thread = try std.Thread.spawn(.{}, Receiving.run, .{ client, &received });
    awaitAsleep(client, since);
    // The old server's wake for a's reply, late: the receive looks, finds b unanswered, and sleeps again
    Rings.wakeReply(&service.layout, &late.responder.replier.wakes, 0);
    awaitAsleep(client, since + 1);
    try testing.expectEqual(@as(u64, 1), client.caller.replies.next.load(.seq_cst));
    try echo(server);
    thread.join();
    try received;
}

// === Threads ===

const Asking = struct {
    end: *ClientEnd,
    /// The client's index, which its requests carry.
    index: u32,
    count: u64,
    /// The most requests in flight.
    depth: u64,
    budget_ms: i64,
    failure: *Failure,
    sent: u64 = 0,
    received: u64 = 0,
    thread: std.Thread = undefined,

    fn run(asking: *Asking) void {
        var buf: [4096]u8 = undefined;
        var expected: [4096]u8 = undefined;
        const max_message = asking.end.caller.sender.layout.maxPiece();
        const start_ms = nowMs();
        var random: u64 = @as(u64, asking.index) *% 0x9e3779b97f4a7c15 +% 3;
        while (true) {
            const more = asking.sent < asking.count and (asking.sent % 64 != 0 or nowMs() - start_ms < asking.budget_ms);
            if (more and asking.sent - asking.received < asking.depth) {
                const size = Request.sizeOf(asking.sent, max_message);
                Request.fill(buf[0..size], asking.index, asking.sent);
                asking.end.send(buf[0..size], 10_000) catch return asking.failure.set("a request failed");
                asking.sent += 1;
                continue;
            }
            if (asking.received == asking.sent) return;
            random = random *% 6364136223846793005 +% 1442695040888963407;
            const reply: []const u8 = (if (random >> 63 == 0) asking.end.recvAcquire(10_000) else asking.end.recv(&buf, 10_000)) catch
                return asking.failure.set("a receive failed");
            const len = Request.answerTo(&expected, .{ .client = asking.index, .seq = asking.received }, max_message);
            if (!std.mem.eql(u8, reply, expected[0..len])) return asking.failure.set("a reply came out of order, or torn");
            asking.received += 1;
        }
    }
};

const Answering = struct {
    end: *ServerEnd,
    failure: *Failure,
    answered: u64 = 0,
    thread: std.Thread = undefined,

    fn run(answering: *Answering) void {
        var buf: [4096]u8 = undefined;
        const max_message = answering.end.responder.taker.layout.maxPiece();
        var random: u64 = @as(u64, answering.end.responder.taker.slot) *% 0x2545f4914f6cdd1d +% 5;
        while (true) {
            random = random *% 6364136223846793005 +% 1442695040888963407;
            const request: []const u8 = (if (random >> 62 == 0) answering.end.recvAcquire(-1) else answering.end.recv(&buf, -1)) catch |err| switch (err) {
                error.Disconnected => return,
                else => return answering.failure.set("a server's receive failed"),
            };
            const id = Request.check(request) orelse return answering.failure.set("a request was torn");
            var answer: [4096]u8 = undefined;
            const len = Request.answerTo(&answer, id, max_message);
            if (random >> 58 & 0xf == 0) {
                // The reply in place, written over a while: the request stays whole meanwhile
                const room = answering.end.responder.sendAcquire(answering.end.endpoint, len) catch return answering.failure.set("a reply's room failed");
                @memcpy(room, answer[0..len]);
                for (0..2000) |_| std.atomic.spinLoopHint();
                if (Request.check(request) == null) return answering.failure.set("a request held in place was overwritten");
                answering.end.responder.sendCommit(answering.end.endpoint, len) catch |err| return answering.replyFailed(err);
            } else {
                answering.end.reply(answer[0..len]) catch |err| return answering.replyFailed(err);
            }
            answering.answered += 1;
        }
    }

    /// A reply's failure. Disconnected with the request no longer held is a reply that stored its done and then saw the
    /// service's end (responder.zig `answer`, the end's pair): its client may have received it, and here did, since the
    /// end comes only once every client received every reply. It counts as answered, and the server's work is over.
    /// Any other failure fails: no reply finds the end before its done, as its client still waits for it.
    fn replyFailed(answering: *Answering, err: DataError) void {
        if (err == error.Disconnected and answering.end.responder.held.request() == null) {
            answering.answered += 1;
            return;
        }
        answering.failure.set("a reply failed");
    }
};

/// `clients` client and `servers` server threads on a service of `capacity` bytes per ring: each client sends up to
/// `count` requests of varied sizes, at most `depth` in flight, for at most `budget_ms`, and checks each reply; then
/// the service ends. Each request must have been answered exactly once, and each client must have received every
/// answer.
fn stress(capacity: u32, max_message: u32, clients: u32, servers: u32, count: u64, depth: u64, budget_ms: i64) !void {
    // A client's one thread waits for room only while its servers owe it answers: its ring holds `depth` requests of
    // at most twice the reply's room each, the next one and a skip to the ring's start
    std.debug.assert(depth <= capacity / (2 * std.mem.alignForward(u64, max_message, 64)) - 2);
    const service = try Service.create(capacity, max_message, clients, servers);
    defer service.destroy();
    var failure: Failure = .{};
    const askings = try testing.allocator.alloc(Asking, clients);
    defer testing.allocator.free(askings);
    const answerings = try testing.allocator.alloc(Answering, servers);
    defer testing.allocator.free(answerings);
    var askers: usize = 0;
    defer for (askings[0..askers]) |*asking| asking.end.destroy();
    for (askings, 0..) |*asking, client_index| {
        asking.* = .{ .end = try service.client(@intCast(client_index)), .index = @intCast(client_index), .count = count, .depth = depth, .budget_ms = budget_ms, .failure = &failure };
        askers += 1;
    }
    var answerers: usize = 0;
    defer for (answerings[0..answerers]) |*answering| answering.end.destroy();
    for (answerings, 0..) |*answering, server_index| {
        answering.* = .{ .end = try service.server(@intCast(server_index)), .failure = &failure };
        answerers += 1;
    }
    var answering_started: usize = 0;
    defer for (answerings[0..answering_started]) |*answering| answering.thread.join();
    // On every way out, the service's end lets the servers go
    defer if (answering_started != 0) {
        service.markEnded();
        for (answerings) |*answering| answering.end.connectionEnded();
    };
    for (answerings) |*answering| {
        answering.thread = try std.Thread.spawn(.{}, Answering.run, .{answering});
        answering_started += 1;
    }
    var asking_started: usize = 0;
    defer for (askings[0..asking_started]) |*asking| asking.thread.join();
    for (askings) |*asking| {
        asking.thread = try std.Thread.spawn(.{}, Asking.run, .{asking});
        asking_started += 1;
    }
    for (askings) |*asking| asking.thread.join();
    asking_started = 0;
    service.markEnded();
    for (answerings) |*answering| answering.end.connectionEnded();
    for (answerings) |*answering| answering.thread.join();
    answering_started = 0;
    try failure.check();
    var sent: u64 = 0;
    var answered: u64 = 0;
    for (askings) |asking| {
        try testing.expectEqual(asking.sent, asking.received);
        sent += asking.sent;
    }
    for (answerings) |answering| answered += answering.answered;
    try testing.expectEqual(sent, answered);
    try testing.expect(sent > 0);
}

test "stress: clients and servers in threads, every request answered once, each client's replies in order" {
    try stress(64 * 1024, 1024, 1, 1, 20_000, 1, 200);
    try stress(64 * 1024, 1024, 1, 4, 20_000, 16, 200);
    try stress(64 * 1024, 512, 4, 1, 10_000, 8, 200);
    try stress(64 * 1024, 512, 4, 4, 10_000, 32, 200);
    try stress(64 * 1024, 256, 8, 8, 5_000, 64, 250);
    try stress(4096, 256, 4, 4, 5_000, 6, 250); // tiny rings: the clients wait for room all the time
}

test {
    if (@import("build_options").slow_tests) _ = slow_tests;
}

const slow_tests = struct {
    test "slow: stress: clients and servers in threads, for seconds, oversubscribed and with tiny rings" {
        // The slow tier runs each stress test on its own, beside the other slow tests (build/tests.zig)
        if (std.testing.environ.containsUnemptyConstant("FIPC_SKIP_STRESS_SERVICE")) return error.SkipZigTest;
        const threads: u32 = @intCast(@min((std.Thread.getCpuCount() catch 4) + 4, 2 * wire_protocol.max_service_clients));
        try stress(256 * 1024, 4096, 4, 4, 500_000, 16, 2000);
        try stress(256 * 1024, 1024, 16, 16, 200_000, 16, 2000);
        try stress(16 * 1024, 512, @min(threads, 64), @min(threads, 64), 200_000, 8, 3000);
        try stress(4096, 512, 64, 64, 20_000, 2, 2000);
    }
};
