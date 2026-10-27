//! A broadcast's, a queue's and a service's admission, which the owner's process runs on its poller (no thread of the owner's
//! application waits for a joiner): a joiner that sends nothing after its connect is dropped once its time to JOIN runs
//! out, and holds up no other joiner meanwhile; a joiner that sends something else than JOIN is dropped at once. The
//! misbehaving joiners are raw connections of this process to the name (the platform layer, `fastipc.internal`).

const std = @import("std");
const fastipc = @import("fastipc");
const support = @import("support.zig");
const c = support.c;

const platform = fastipc.internal.platform;

/// How long the owner gives a joiner to send JOIN (core/admission.zig `join_deadline_ms`).
const join_deadline_ms = 2000;

/// A raw connection to the owner of `name`, which plays a joiner that misbehaves.
const RawJoiner = struct {
    security: platform.Security,
    connection: platform.Handle,

    fn connect(name: [:0]const u8) !RawJoiner {
        var security = try platform.Security.init();
        errdefer security.deinit();
        const address = try platform.addressOf(name);
        const begun = support.nowMs();
        while (support.nowMs() - begun < 5000) {
            switch (try platform.connect(&address, &security)) {
                .connected => |connection| return .{ .security = security, .connection = connection },
                .no_listener, .busy => support.sleepMs(1),
                .foreign => return error.Foreign,
            }
        }
        return error.Timeout;
    }

    fn close(raw: *RawJoiner) void {
        platform.close(raw.connection);
        raw.security.deinit();
    }

    /// Reads what the owner sends within `timeout_ms`: its OFFER, then the end of the connection once it dropped this
    /// joiner (0). Null: nothing came.
    fn read(raw: *RawJoiner, timeout_ms: u32) ?usize {
        var buf: [64]u8 = undefined;
        var got: usize = 0;
        const begun = support.nowMs();
        while (true) {
            const left: u32 = @intCast(timeout_ms -| (support.nowMs() - begun));
            switch (platform.readWithin(raw.connection, buf[got..], null, left)) {
                .done => |n| {
                    if (n == 0) return if (got == 0) 0 else got;
                    got += n;
                    if (got == buf.len) return got;
                },
                .idle => if (left == 0) return if (got == 0) null else got,
                .cancelled => unreachable,
            }
        }
    }

    /// Waits for the end of the connection: the milliseconds it took, or null if it didn't come within `timeout_ms`.
    fn awaitDrop(raw: *RawJoiner, timeout_ms: u32) ?u64 {
        const begun = support.nowMs();
        while (support.nowMs() - begun < timeout_ms) {
            const got = raw.read(timeout_ms) orelse return null;
            if (got == 0) return support.nowMs() - begun;
        }
        return null;
    }
};

/// The kinds under test: the owner on `name`, and a member joined to it.
const Kind = enum {
    broadcast,
    queue,
    service,

    const Owner = union(enum) { publisher: support.Conn, queue: support.Queue, service: support.Service };

    /// An owner with room for two members of the role `join` takes.
    fn create(kind: Kind, name: [:0]const u8) !Owner {
        switch (kind) {
            .broadcast => {
                var publisher: support.Conn = null;
                if (c.fipc_broadcast_create(name, 4096, 2, c.FIPC_FOREVER, &publisher) != c.FIPC_OK) return error.Create;
                return .{ .publisher = publisher };
            },
            .queue => {
                var queue: support.Queue = null;
                if (c.fipc_queue_create(name, 4096, 1, 2, &queue) != c.FIPC_OK) return error.Create;
                return .{ .queue = queue };
            },
            .service => {
                var service: support.Service = null;
                if (c.fipc_service_create(name, 4096, 64, 1, 2, &service) != c.FIPC_OK) return error.Create;
                return .{ .service = service };
            },
        }
    }

    fn close(owner: Owner) void {
        switch (owner) {
            .publisher => |publisher| c.fipc_close(publisher),
            .queue => |queue| c.fipc_queue_close(queue),
            .service => |service| c.fipc_service_close(service),
        }
    }

    fn join(kind: Kind, name: [:0]const u8, conn: *support.Conn, timeout_ms: c_int) c.fipc_result_t {
        return switch (kind) {
            .broadcast => c.fipc_broadcast_join(name, c.FIPC_SUBSCRIBER_LOSSY, conn, timeout_ms),
            .queue => c.fipc_queue_join(name, c.FIPC_CONSUMER, conn, timeout_ms),
            .service => c.fipc_service_join(name, c.FIPC_SERVER, conn, timeout_ms),
        };
    }

    /// The prefix of a test's name for this kind.
    fn prefix(kind: Kind, comptime test_prefix: []const u8) []const u8 {
        return switch (kind) {
            .broadcast => test_prefix ++ "_bc",
            .queue => test_prefix ++ "_mq",
            .service => test_prefix ++ "_sv",
        };
    }
};

test "fast: a joiner silent after its connect holds up no other joiner, and is dropped once its time to JOIN runs out" {
    var t = try support.begin(@src(), 60);
    defer t.end();
    for ([_]Kind{ .broadcast, .queue, .service }) |kind| {
        var buf: [64]u8 = undefined;
        const name = support.uniqueName(&buf, kind.prefix("adm_silent"));
        const owner = try kind.create(name);
        defer Kind.close(owner);
        const connected_at = support.nowMs();
        var silent = try RawJoiner.connect(name);
        defer silent.close();
        try t.check(silent.read(5000) == 64, "the silent joiner gets its OFFER");
        // A joiner meanwhile gets in at once
        var member: support.Conn = null;
        const begun = support.nowMs();
        try t.checkEqual(kind.join(name, &member, 5000), c.FIPC_OK, "another joiner joins");
        _ = t.expect(support.nowMs() - begun < join_deadline_ms / 2, "without waiting for the silent one");
        c.fipc_close(member);
        // The silent one is dropped: within a while of its deadline, not before most of it
        _ = t.expect(silent.awaitDrop(join_deadline_ms + 3000) != null, "the silent joiner is dropped");
        _ = t.expect(support.nowMs() - connected_at >= join_deadline_ms - 200, "once its time runs out");
        // What it held is free again: two members join
        var members: [2]support.Conn = .{ null, null };
        for (&members) |*one| _ = t.expectEqual(kind.join(name, one, 5000), c.FIPC_OK, "a member joins");
        for (members) |one| c.fipc_close(one);
    }
    return t.done();
}

test "fast: a joiner that sends something else than JOIN is dropped at once, and the next joins" {
    var t = try support.begin(@src(), 60);
    defer t.end();
    for ([_]Kind{ .broadcast, .queue, .service }) |kind| {
        var buf: [64]u8 = undefined;
        const name = support.uniqueName(&buf, kind.prefix("adm_garbage"));
        const owner = try kind.create(name);
        defer Kind.close(owner);
        var garbage = try RawJoiner.connect(name);
        defer garbage.close();
        try t.check(garbage.read(5000) == 64, "the joiner gets its OFFER");
        const frame: [64]u8 = @splat(0xa5);
        try t.check(platform.sendFrame(garbage.connection, &frame, null), "it sends a frame that isn't JOIN");
        const dropped = garbage.awaitDrop(join_deadline_ms + 3000);
        _ = t.expect(dropped != null and dropped.? < join_deadline_ms / 2, "it is dropped at once");
        var members: [2]support.Conn = .{ null, null };
        for (&members) |*one| _ = t.expectEqual(kind.join(name, one, 5000), c.FIPC_OK, "a member joins");
        for (members) |one| c.fipc_close(one);
    }
    return t.done();
}

test "fast: one thread creates a broadcast and a queue and joins them: their processes admit without it" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    const name = support.uniqueName(&buf, "adm_one_thread");
    var publisher: support.Conn = null;
    try t.checkEqual(c.fipc_broadcast_create(name, 4096, 1, c.FIPC_FOREVER, &publisher), c.FIPC_OK, "a broadcast");
    var subscriber: support.Conn = null;
    try t.checkEqual(c.fipc_broadcast_join(name, c.FIPC_SUBSCRIBER_GATED, &subscriber, 5000), c.FIPC_OK, "its creator's thread joins it");
    _ = t.expectEqual(support.send(publisher, "one", 0), c.FIPC_OK, "a send");
    _ = t.expect(support.recvIs(subscriber, 1000, "one"), "received");
    c.fipc_close(publisher);
    var len: usize = 0;
    var receive_buffer: [8]u8 = undefined;
    _ = t.expectEqual(support.recv(subscriber, &receive_buffer, &len, 5000), c.FIPC_DISCONNECTED, "the publisher's close ends it");
    c.fipc_close(subscriber);

    var queue: support.Queue = null;
    try t.checkEqual(c.fipc_queue_create(name, 4096, 1, 1, &queue), c.FIPC_OK, "a queue on the freed name");
    var giver: support.Conn = null;
    var taker: support.Conn = null;
    try t.checkEqual(c.fipc_queue_join(name, c.FIPC_PRODUCER, &giver, 5000), c.FIPC_OK, "its creator's thread joins as a producer");
    try t.checkEqual(c.fipc_queue_join(name, c.FIPC_CONSUMER, &taker, 5000), c.FIPC_OK, "and as a consumer");
    _ = t.expectEqual(support.send(giver, "job", 0), c.FIPC_OK, "a job");
    _ = t.expect(support.recvIs(taker, 1000, "job"), "taken");
    c.fipc_queue_close(queue);
    _ = t.expectEqual(support.send(giver, "late", 0), c.FIPC_DISCONNECTED, "the queue's close ends it");
    c.fipc_close(giver);
    c.fipc_close(taker);
    return t.done();
}
