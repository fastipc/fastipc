//! The service tests' shared helpers: a service's owner (its handle, and the members it joined), a member joined,
//! checked requests and replies (`support.Stamp`): a request's sequence number names its client, and its reply's is
//! the same with bit 63 set; and the threads that ask (a client, its requests in flight, its replies checked in order)
//! and answer (a server).

const std = @import("std");
const fastipc = @import("fastipc");
const support = @import("../support.zig");
const c = support.c;

pub const client = c.FIPC_CLIENT;
pub const server = c.FIPC_SERVER;

/// A service's owner in this process: its handle, and the members the owner joined on this thread (a client, a
/// server, or both).
pub const Service = struct {
    handle: support.Service = null,
    own_client: support.Conn = null,
    own_server: support.Conn = null,

    pub const Joins = struct { client: bool = false, server: bool = false };

    /// `fipc_service_create(name, ...)`, then the joins of `joins` (`fipc_service_join(name, role)`); false (nothing
    /// left open) on a failure.
    pub fn open(service: *Service, name: [*:0]const u8, capacity: usize, max_message: usize, max_clients: u32, max_servers: u32, joins: Joins) bool {
        service.* = .{};
        const result = c.fipc_service_create(name, capacity, max_message, max_clients, max_servers, &service.handle);
        if (result != c.FIPC_OK) {
            std.debug.print("  service_create on '{s}': {s}\n", .{ name, support.resultName(result) });
            return false;
        }
        for ([_]struct { bool, c_int, *support.Conn }{ .{ joins.client, client, &service.own_client }, .{ joins.server, server, &service.own_server } }) |join| {
            if (!join[0]) continue;
            const joined_own = c.fipc_service_join(name, join[1], join[2], 5000);
            if (joined_own == c.FIPC_OK) continue;
            std.debug.print("  the owner's service_join on '{s}': {s}\n", .{ name, support.resultName(joined_own) });
            service.close();
            return false;
        }
        return true;
    }

    /// Closes what is open: the owner's members, then the service (it ends).
    pub fn close(service: *Service) void {
        c.fipc_close(service.own_client);
        c.fipc_close(service.own_server);
        c.fipc_service_close(service.handle);
        service.* = .{};
    }
};

/// `Service.open` on the per-process name "<prefix>_<pid>" (into `name_buf`); ends the test if it fails.
pub fn openService(
    t: *support.Test,
    service: *Service,
    name_buf: []u8,
    comptime prefix: []const u8,
    capacity: usize,
    max_message: usize,
    max_clients: u32,
    max_servers: u32,
    joins: Service.Joins,
) ![:0]const u8 {
    const name = support.uniqueName(name_buf, prefix);
    try t.check(service.open(name, capacity, max_message, max_clients, max_servers, joins), "a service");
    return name;
}

/// A member of `role` joined to the service on `name`, or the test ends.
pub fn joined(t: *support.Test, name: [*:0]const u8, role: c_int) !support.Conn {
    var member: support.Conn = null;
    try t.check(support.joinNow(name, role, &member, 5000), "a member joins");
    return member;
}

/// Request `index` of client `client_index`: its stamp's sequence number.
pub fn seqOf(client_index: u64, index: u64) u64 {
    return client_index << 32 | index;
}

/// The sequence number of the reply to request `seq`.
pub fn replyOf(seq: u64) u64 {
    return seq | 1 << 63;
}

/// The length of request or reply `seq` in a service of `max_message`: at least `support.Stamp.min`, most small.
pub fn sizeOf(seq: u64, max_message: usize) usize {
    return support.Stamp.sizeOf(seq, @min(max_message, 4096));
}

/// Sends message `seq` (a request on a client, a reply on a server) of its length (`sizeOf`).
pub fn sendStamp(conn: support.Conn, seq: u64, max_message: usize, timeout_ms: c_int) c.fipc_result_t {
    var buf: [4096]u8 = undefined;
    const len = sizeOf(seq, max_message);
    support.Stamp.fill(buf[0..len], seq);
    return support.send(conn, buf[0..len], timeout_ms);
}

/// The same, in place (`fipc_send_acquire`, `fipc_send_commit`).
pub fn sendStampInPlace(conn: support.Conn, seq: u64, max_message: usize, timeout_ms: c_int) c.fipc_result_t {
    const len = sizeOf(seq, max_message);
    var room: ?*anyopaque = null;
    const acquired = c.fipc_send_acquire(conn, len, &room, timeout_ms);
    if (acquired != c.FIPC_OK) return acquired;
    support.Stamp.fill(@as([*]u8, @ptrCast(room.?))[0..len], seq);
    return c.fipc_send_commit(conn, len);
}

/// Receives the next message, copied or in place (released once checked, which on a server does nothing): its
/// sequence number; null on any other result (in `result`), or for a message that isn't whole (`result` OK).
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

/// A server's one answer: takes the next request (copied or in place) and replies to it (copied or in place): the
/// request's sequence number; null on any other result of the receive or the reply (in `result`), or for a request
/// that isn't whole (then answered with a reply of sequence 0).
pub fn answerOne(conn: support.Conn, max_message: usize, in_place: bool, timeout_ms: c_int, result: *c.fipc_result_t) ?u64 {
    var buf: [4096]u8 = undefined;
    var len: usize = 0;
    var request: []const u8 = undefined;
    if (in_place) {
        var at: ?*const anyopaque = null;
        result.* = c.fipc_recv_acquire(conn, &at, &len, timeout_ms);
        if (result.* != c.FIPC_OK) return null;
        request = @as([*]const u8, @ptrCast(at.?))[0..len];
    } else {
        result.* = support.recv(conn, &buf, &len, timeout_ms);
        if (result.* != c.FIPC_OK) return null;
        request = buf[0..len];
    }
    // The request held in place stays whole until the reply
    const seq = support.Stamp.check(request);
    result.* = if (in_place) sendStampInPlace(conn, replyOf(seq orelse 0), max_message, 0) else sendStamp(conn, replyOf(seq orelse 0), max_message, 0);
    if (result.* != c.FIPC_OK) return null;
    return seq;
}

/// A server answering on a thread of its own until a receive or a reply returns anything but OK (`end`).
pub const Answerer = struct {
    conn: support.Conn,
    max_message: usize,
    in_place: bool = false,
    timeout_ms: c_int = 5000,
    thread: std.Thread = undefined,
    end: c.fipc_result_t = 0,
    answered: std.atomic.Value(u64) = .init(0),
    bad: std.atomic.Value(u32) = .init(0),

    pub fn start(answerer: *Answerer) !void {
        answerer.thread = try std.Thread.spawn(.{}, run, .{answerer});
    }

    fn run(answerer: *Answerer) void {
        var round: u64 = 0;
        while (true) : (round += 1) {
            var result: c.fipc_result_t = 0;
            const in_place = answerer.in_place and round % 2 == 0;
            const seq = answerOne(answerer.conn, answerer.max_message, in_place, answerer.timeout_ms, &result);
            if (result != c.FIPC_OK) {
                answerer.end = result;
                return;
            }
            if (seq == null) _ = answerer.bad.fetchAdd(1, .monotonic);
            _ = answerer.answered.fetchAdd(1, .monotonic);
        }
    }

    pub fn join(answerer: *Answerer) c.fipc_result_t {
        answerer.thread.join();
        return answerer.end;
    }
};

/// A client's stream of `count` requests on a thread of its own, at most `depth` in flight: each reply must be its
/// request's, in order. `result`: the first call that failed, else OK; `wrong`: the replies that weren't.
pub const Asker = struct {
    conn: support.Conn,
    client_index: u64,
    count: u64,
    depth: u64 = 1,
    max_message: usize,
    in_place: bool = false,
    thread: std.Thread = undefined,
    result: c.fipc_result_t = c.FIPC_OK,
    sent: u64 = 0,
    received: u64 = 0,
    wrong: u64 = 0,

    pub fn start(asker: *Asker) !void {
        asker.thread = try std.Thread.spawn(.{}, run, .{asker});
    }

    fn run(asker: *Asker) void {
        while (asker.received < asker.count) {
            if (asker.sent < asker.count and asker.sent - asker.received < asker.depth) {
                const seq = seqOf(asker.client_index, asker.sent);
                const result = if (asker.in_place and asker.sent % 3 == 0) sendStampInPlace(asker.conn, seq, asker.max_message, 5000) else sendStamp(asker.conn, seq, asker.max_message, 5000);
                if (result != c.FIPC_OK) {
                    asker.result = result;
                    return;
                }
                asker.sent += 1;
                continue;
            }
            var result: c.fipc_result_t = 0;
            const seq = recvStamp(asker.conn, asker.in_place and asker.received % 2 == 0, 5000, &result);
            if (result != c.FIPC_OK) {
                asker.result = result;
                return;
            }
            if (seq != replyOf(seqOf(asker.client_index, asker.received))) asker.wrong += 1;
            asker.received += 1;
        }
    }

    pub fn join(asker: *Asker) c.fipc_result_t {
        asker.thread.join();
        return asker.result;
    }
};

/// The sleeps a member's receives began (a client's waits for a reply, a server's parks), for `awaitAsleep`.
pub fn sleepsOf(member: support.Conn) u32 {
    return fastipc.test_hooks.serviceSleeps(member.?);
}

/// Until a receive of `member` has begun a sleep since `sleepsOf(member)` was `since`.
pub fn awaitAsleep(member: support.Conn, since: u32) void {
    while (sleepsOf(member) == since) std.Thread.yield() catch {};
}

/// The sleeps for room a client's sends began.
pub fn roomSleepsOf(member: support.Conn) u32 {
    return fastipc.test_hooks.serviceRoomSleeps(member.?);
}

/// The service's segment, as `member` maps it.
pub fn layoutOf(member: support.Conn) *const fastipc.test_hooks.ServiceLayout {
    return fastipc.test_hooks.serviceLayout(member.?);
}

/// Until the owner counts `count` members of `role` (0: both), within `timeout_ms`; false if it doesn't.
pub fn awaitCount(service: support.Service, role: c_int, count: u32, timeout_ms: u64) bool {
    const begun = support.nowMs();
    var counted: u32 = 0;
    while (support.nowMs() - begun < timeout_ms) {
        _ = c.fipc_service_wait_for_members(service, role, 0, &counted, 0);
        if (counted == count) return true;
        support.sleepMs(1);
    }
    std.debug.print("  the owner counts {d}, not {d}\n", .{ counted, count });
    return false;
}
