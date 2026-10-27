//! What every call of the C API promises, whatever it does: the names of the results and the checks of its arguments
//! (NULL pointers, NULL handles, sizes that overflow, capacities out of range).

const std = @import("std");
const support = @import("support.zig");
const c = support.c;

test "fast: every result code has a name" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    const names = [_][]const u8{ "FIPC_OK", "FIPC_TIMEOUT", "FIPC_DISCONNECTED", "FIPC_CANCELLED", "FIPC_TOO_LARGE", "FIPC_INVALID", "FIPC_NO_MEMORY", "FIPC_ADDR_IN_USE", "FIPC_OVERRUN", "FIPC_UNSUBSCRIBED", "FIPC_ABANDONED" };
    for (names, 0..) |name, code| _ = t.expect(std.mem.eql(u8, support.resultName(@intCast(code)), name), "each code's name");
    _ = t.expect(std.mem.eql(u8, support.resultName(11), "FIPC_UNKNOWN"), "an unknown code");
    _ = t.expect(std.mem.eql(u8, support.resultName(@bitCast(@as(c_int, -1))), "FIPC_UNKNOWN"), "a negative one");
    return t.done();
}

// `fipc_stream_create` and `fipc_stream_connect` refuse a capacity of 0, a NULL name and a NULL out-pointer, and create nothing.
test "fast: stream_create and stream_connect check their arguments" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var listener: support.Listener = null;
    var conn: support.Conn = null;
    if (c.fipc_stream_create("invalid_params", 0, &listener) != c.FIPC_INVALID) return t.fail("Should reject zero capacity");
    if (c.fipc_stream_create(null, 1024, &listener) != c.FIPC_INVALID) return t.fail("Should reject a NULL name");
    if (c.fipc_stream_connect(null, &conn, 0) != c.FIPC_INVALID) return t.fail("connect should reject a NULL name");
    if (c.fipc_stream_create("valid_name", 1024, null) != c.FIPC_INVALID) return t.fail("Should reject a NULL out-pointer");
    if (c.fipc_stream_connect("valid_name", null, 0) != c.FIPC_INVALID) return t.fail("connect should reject a NULL out-pointer");
    _ = t.expect(listener == null and conn == null, "nothing was created");
    return t.done();
}

// The broadcast's and the queue's creates and joins refuse a NULL name or out-pointer, a role that isn't the kind's,
// and a size out of range, and create nothing.
test "fast: the broadcast's and the queue's creates and joins check their arguments" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var publisher: support.Conn = null;
    var queue: support.Queue = null;
    var conn: support.Conn = null;
    _ = t.expectEqual(c.fipc_broadcast_create(null, 4096, 1, c.FIPC_FOREVER, &publisher), c.FIPC_INVALID, "broadcast_create: a NULL name");
    _ = t.expectEqual(c.fipc_broadcast_create("test_pub_args", 4096, 1, c.FIPC_FOREVER, null), c.FIPC_INVALID, "broadcast_create: a NULL out-pointer");
    _ = t.expectEqual(c.fipc_broadcast_create("test_pub_args", 2048, 1, c.FIPC_FOREVER, &publisher), c.FIPC_INVALID, "broadcast_create: a capacity below 4096");
    _ = t.expectEqual(c.fipc_broadcast_create("test_pub_args", 4096, 0, c.FIPC_FOREVER, &publisher), c.FIPC_INVALID, "broadcast_create: no subscriber");
    _ = t.expectEqual(c.fipc_broadcast_create("test_pub_args", 4096, 1025, c.FIPC_FOREVER, &publisher), c.FIPC_INVALID, "broadcast_create: over 1024 subscribers");
    _ = t.expectEqual(c.fipc_broadcast_join(null, c.FIPC_SUBSCRIBER_LOSSY, &conn, 0), c.FIPC_INVALID, "broadcast_join: a NULL name");
    _ = t.expectEqual(c.fipc_broadcast_join("test_pub_args", c.FIPC_SUBSCRIBER_LOSSY, null, 0), c.FIPC_INVALID, "broadcast_join: a NULL out-pointer");
    for ([_]c_int{ 0, c.FIPC_PRODUCER, c.FIPC_CONSUMER, 5, 6, -1 }) |role| {
        _ = t.expectEqual(c.fipc_broadcast_join("test_pub_args", role, &conn, 0), c.FIPC_INVALID, "broadcast_join: a role that isn't a subscriber's");
    }
    _ = t.expectEqual(c.fipc_queue_create(null, 4096, 1, 1, &queue), c.FIPC_INVALID, "queue_create: a NULL name");
    _ = t.expectEqual(c.fipc_queue_create("test_queue_args", 4096, 1, 1, null), c.FIPC_INVALID, "queue_create: a NULL out-pointer");
    _ = t.expectEqual(c.fipc_queue_create("test_queue_args", 2048, 1, 1, &queue), c.FIPC_INVALID, "queue_create: a capacity below 4096");
    _ = t.expectEqual(c.fipc_queue_create("test_queue_args", 4096, 0, 1, &queue), c.FIPC_INVALID, "queue_create: no producer");
    _ = t.expectEqual(c.fipc_queue_create("test_queue_args", 4096, 1, 65, &queue), c.FIPC_INVALID, "queue_create: over 64 consumers");
    _ = t.expectEqual(c.fipc_queue_create("test_queue_args", 4096, 64, 1, &queue), c.FIPC_INVALID, "queue_create: over 63 producers");
    _ = t.expectEqual(c.fipc_queue_join(null, c.FIPC_PRODUCER, &conn, 0), c.FIPC_INVALID, "queue_join: a NULL name");
    _ = t.expectEqual(c.fipc_queue_join("test_queue_args", c.FIPC_PRODUCER, null, 0), c.FIPC_INVALID, "queue_join: a NULL out-pointer");
    for ([_]c_int{ 0, c.FIPC_SUBSCRIBER_LOSSY, c.FIPC_SUBSCRIBER_GATED, 5, -1 }) |role| {
        _ = t.expectEqual(c.fipc_queue_join("test_queue_args", role, &conn, 0), c.FIPC_INVALID, "queue_join: a role that isn't a member's");
    }
    _ = t.expect(publisher == null and queue == null and conn == null, "nothing was created");
    return t.done();
}

// The service's create and join refuse a NULL name or out-pointer, a role that isn't the kind's, and a size out of
// range, and create nothing.
test "fast: the service's create and join check their arguments" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var service: support.Service = null;
    var conn: support.Conn = null;
    _ = t.expectEqual(c.fipc_service_create(null, 4096, 64, 1, 1, &service), c.FIPC_INVALID, "service_create: a NULL name");
    _ = t.expectEqual(c.fipc_service_create("test_service_args", 4096, 64, 1, 1, null), c.FIPC_INVALID, "service_create: a NULL out-pointer");
    _ = t.expectEqual(c.fipc_service_create("test_service_args", 2048, 64, 1, 1, &service), c.FIPC_INVALID, "service_create: a capacity below 4096");
    _ = t.expectEqual(c.fipc_service_create("test_service_args", 4096, 0, 1, 1, &service), c.FIPC_INVALID, "service_create: max_message 0");
    _ = t.expectEqual(c.fipc_service_create("test_service_args", 4096, 1025, 1, 1, &service), c.FIPC_INVALID, "service_create: max_message over a quarter of the capacity");
    _ = t.expectEqual(c.fipc_service_create("test_service_args", 4096, 64, 0, 1, &service), c.FIPC_INVALID, "service_create: no client");
    _ = t.expectEqual(c.fipc_service_create("test_service_args", 4096, 64, 1, 65, &service), c.FIPC_INVALID, "service_create: over 64 servers");
    _ = t.expectEqual(c.fipc_service_join(null, c.FIPC_CLIENT, &conn, 0), c.FIPC_INVALID, "service_join: a NULL name");
    _ = t.expectEqual(c.fipc_service_join("test_service_args", c.FIPC_SERVER, null, 0), c.FIPC_INVALID, "service_join: a NULL out-pointer");
    for ([_]c_int{ 0, c.FIPC_SUBSCRIBER_LOSSY, c.FIPC_PRODUCER, c.FIPC_CONSUMER, 7, -1 }) |role| {
        _ = t.expectEqual(c.fipc_service_join("test_service_args", role, &conn, 0), c.FIPC_INVALID, "service_join: a role that isn't a member's");
    }
    _ = t.expect(service == null and conn == null, "nothing was created");
    return t.done();
}

// A capacity of 4 GiB or more is refused (it would wrap to 0 in the segment's 32-bit field).
test "fast: a capacity above 4 GiB is refused" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var listener: support.Listener = null;
    const over_limit: usize = @as(usize, std.math.maxInt(u32)) + 1;
    _ = t.expectEqual(c.fipc_stream_create("test_over_limit", over_limit, &listener), c.FIPC_INVALID, "a capacity above UINT32_MAX is INVALID");
    return t.done();
}

// Every call on a NULL handle is INVALID (or does nothing), without touching memory: a closed handle is gone, so a
// second close is the caller's bug, and the NULL handle is what the calls check.
test "fast: every call on a NULL handle is INVALID or does nothing" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var len: usize = 0;
    var buf: [16]u8 = undefined;
    var out: ?*anyopaque = null;
    var in: ?*const anyopaque = null;
    var conn: support.Conn = null;
    _ = t.expectEqual(c.fipc_stream_accept(null, &conn, 0), c.FIPC_INVALID, "accept");
    _ = t.expectEqual(c.fipc_send(null, "x", 1, 100), c.FIPC_INVALID, "send");
    _ = t.expectEqual(c.fipc_recv(null, &buf, buf.len, &len, 100), c.FIPC_INVALID, "recv");
    _ = t.expectEqual(c.fipc_send_acquire(null, 16, &out, 100), c.FIPC_INVALID, "send_acquire");
    _ = t.expectEqual(c.fipc_send_commit(null, 16), c.FIPC_INVALID, "send_commit");
    _ = t.expectEqual(c.fipc_recv_acquire(null, &in, &len, 100), c.FIPC_INVALID, "recv_acquire");
    _ = t.expectEqual(c.fipc_max_piece(null), 0, "max_piece: 0");
    var count: u32 = 0;
    _ = t.expectEqual(c.fipc_broadcast_wait_for_members(null, 0, &count, 100), c.FIPC_INVALID, "broadcast_wait_for_members");
    _ = t.expectEqual(c.fipc_queue_wait_for_members(null, 0, 0, &count, 100), c.FIPC_INVALID, "queue_wait_for_members");
    _ = t.expectEqual(c.fipc_service_wait_for_members(null, 0, 0, &count, 100), c.FIPC_INVALID, "service_wait_for_members");
    c.fipc_recv_release(null);
    c.fipc_cancel(null);
    c.fipc_close(null);
    c.fipc_listener_cancel(null);
    c.fipc_listener_close(null);
    c.fipc_queue_cancel(null);
    c.fipc_queue_close(null);
    c.fipc_service_cancel(null);
    c.fipc_service_close(null);
    return t.done();
}

// NULL pointers and sizes that overflow are refused with a result code, never a panic, and a reservation of SIZE_MAX
// bytes doesn't wrap to a small one.
test "fast: NULL pointers and overflowing sizes are refused" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var pair: support.Pair = .{};
    try support.openPair(&t, &pair, "c_input", 4096);
    defer pair.close();
    _ = t.expectEqual(c.fipc_send_acquire(pair.server, 16, null, 0), c.FIPC_INVALID, "send_acquire with a NULL out-pointer: INVALID");
    _ = t.expectEqual(c.fipc_send_commit(pair.server, 16), c.FIPC_INVALID, "and nothing was reserved");
    var ptr: ?*anyopaque = null;
    _ = t.expectEqual(c.fipc_send_acquire(pair.server, std.math.maxInt(usize), &ptr, 0), c.FIPC_TOO_LARGE, "a reservation of SIZE_MAX bytes: TOO_LARGE");
    var len: usize = 0;
    _ = t.expectEqual(c.fipc_recv(null, null, 0, &len, 0), c.FIPC_INVALID, "recv on a NULL connection: INVALID");
    _ = t.expectEqual(c.fipc_recv(pair.client, null, 16, &len, 0), c.FIPC_INVALID, "recv into a NULL buffer with a length: INVALID");
    _ = t.expectEqual(c.fipc_send(pair.server, null, 16, 0), c.FIPC_INVALID, "send of a NULL buffer with a length: INVALID");
    _ = t.expectEqual(c.fipc_recv(pair.client, null, 0, null, 0), c.FIPC_INVALID, "recv with a NULL length: INVALID");
    var count: u32 = 0;
    _ = t.expectEqual(c.fipc_broadcast_wait_for_members(pair.server, 0, &count, 0), c.FIPC_INVALID, "broadcast_wait_for_members on a connection that isn't a publisher: INVALID");
    return t.done();
}
