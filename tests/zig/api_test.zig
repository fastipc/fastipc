//! What every call of the C API promises, whatever it does: the names of the results, the checks of its arguments
//! (NULL pointers, NULL handles, sizes that overflow, capacities out of range), and the layout of the one public
//! struct.

const std = @import("std");
const ts = @import("support.zig");
const c = ts.c;

test "fast: every result code has a name" {
    var t = ts.begin(@src(), 30);
    defer t.end();
    const names = [_][]const u8{ "FIPC_OK", "FIPC_TIMEOUT", "FIPC_DISCONNECTED", "FIPC_CANCELLED", "FIPC_TOO_LARGE", "FIPC_INVALID", "FIPC_NO_MEMORY", "FIPC_ADDR_IN_USE" };
    for (names, 0..) |name, code| _ = t.expect(std.mem.eql(u8, ts.resultName(@intCast(code)), name), "each code's name");
    _ = t.expect(std.mem.eql(u8, ts.resultName(8), "FIPC_UNKNOWN"), "an unknown code");
    _ = t.expect(std.mem.eql(u8, ts.resultName(@bitCast(@as(c_int, -1))), "FIPC_UNKNOWN"), "a negative one");
    return t.done();
}

// `fipc_listen` and `fipc_connect` refuse a capacity of 0, a NULL name and a NULL out-pointer, and create nothing.
test "fast: listen and connect check their arguments" {
    var t = ts.begin(@src(), 30);
    defer t.end();
    var listener: ts.Listener = null;
    var conn: ts.Conn = null;
    if (c.fipc_listen("invalid_params", 0, &listener) != c.FIPC_INVALID) return t.fail("Should reject zero capacity");
    if (c.fipc_listen(null, 1024, &listener) != c.FIPC_INVALID) return t.fail("Should reject a NULL name");
    if (c.fipc_connect(null, &conn, 0) != c.FIPC_INVALID) return t.fail("connect should reject a NULL name");
    if (c.fipc_listen("valid_name", 1024, null) != c.FIPC_INVALID) return t.fail("Should reject a NULL out-pointer");
    if (c.fipc_connect("valid_name", null, 0) != c.FIPC_INVALID) return t.fail("connect should reject a NULL out-pointer");
    _ = t.expect(listener == null and conn == null, "nothing was created");
    return t.done();
}

// A capacity of 4 GiB or more is refused (it would wrap to 0 in the segment's 32-bit field).
test "fast: a capacity above 4 GiB is refused" {
    var t = ts.begin(@src(), 30);
    defer t.end();
    var listener: ts.Listener = null;
    const over_limit: usize = @as(usize, std.math.maxInt(u32)) + 1;
    _ = t.expectEq(c.fipc_listen("test_over_limit", over_limit, &listener), c.FIPC_INVALID, "a capacity above UINT32_MAX is INVALID");
    return t.done();
}

// Every call on a NULL handle is INVALID (or does nothing), without touching memory: a closed handle is gone, so a
// second close is the caller's bug, and the NULL handle is what the calls check.
test "fast: every call on a NULL handle is INVALID or does nothing" {
    var t = ts.begin(@src(), 30);
    defer t.end();
    var len: usize = 0;
    var buf: [16]u8 = undefined;
    var out: ?*anyopaque = null;
    var in: ?*const anyopaque = null;
    var conn: ts.Conn = null;
    var msg: c.fipc_rpc_msg_t = undefined;
    var id: u64 = 0;
    _ = t.expectEq(c.fipc_accept(null, &conn, 0), c.FIPC_INVALID, "accept");
    _ = t.expectEq(c.fipc_send(null, "x", 1, 100), c.FIPC_INVALID, "send");
    _ = t.expectEq(c.fipc_recv(null, &buf, buf.len, &len, 100), c.FIPC_INVALID, "recv");
    _ = t.expectEq(c.fipc_send_acquire(null, 16, &out, 100), c.FIPC_INVALID, "send_acquire");
    _ = t.expectEq(c.fipc_send_commit(null, 16), c.FIPC_INVALID, "send_commit");
    _ = t.expectEq(c.fipc_recv_acquire(null, &in, &len, 100), c.FIPC_INVALID, "recv_acquire");
    _ = t.expectEq(c.fipc_rpc_submit(null, 1, null, 0, &id, 100), c.FIPC_INVALID, "rpc_submit");
    _ = t.expectEq(c.fipc_rpc_respond(null, 1, 1, 0, null, 0, 100), c.FIPC_INVALID, "rpc_respond");
    _ = t.expectEq(c.fipc_rpc_recv(null, null, 0, &msg, 100), c.FIPC_INVALID, "rpc_recv");
    _ = t.expectEq(c.fipc_max_piece(null), 0, "max_piece: 0");
    c.fipc_recv_release(null);
    c.fipc_cancel(null);
    c.fipc_close(null);
    c.fipc_listener_cancel(null);
    c.fipc_listener_close(null);
    return t.done();
}

// NULL pointers and sizes that overflow are refused with a result code, never a panic, and a reservation of SIZE_MAX
// bytes doesn't wrap to a small one.
test "fast: NULL pointers and overflowing sizes are refused" {
    var t = ts.begin(@src(), 30);
    defer t.end();
    var p: ts.Pair = .{};
    try ts.openPair(&t, &p, "c_input", 4096);
    defer p.close();
    _ = t.expectEq(c.fipc_send_acquire(p.server, 16, null, 0), c.FIPC_INVALID, "send_acquire with a NULL out-pointer: INVALID");
    _ = t.expectEq(c.fipc_send_commit(p.server, 16), c.FIPC_INVALID, "and nothing was reserved");
    var ptr: ?*anyopaque = null;
    _ = t.expectEq(c.fipc_send_acquire(p.server, std.math.maxInt(usize), &ptr, 0), c.FIPC_TOO_LARGE, "a reservation of SIZE_MAX bytes: TOO_LARGE");
    var len: usize = 0;
    _ = t.expectEq(c.fipc_recv(null, null, 0, &len, 0), c.FIPC_INVALID, "recv on a NULL connection: INVALID");
    _ = t.expectEq(c.fipc_recv(p.client, null, 16, &len, 0), c.FIPC_INVALID, "recv into a NULL buffer with a length: INVALID");
    _ = t.expectEq(c.fipc_send(p.server, null, 16, 0), c.FIPC_INVALID, "send of a NULL buffer with a length: INVALID");
    _ = t.expectEq(c.fipc_rpc_recv(p.client, null, 0, null, 0), c.FIPC_INVALID, "rpc_recv with a NULL message: INVALID");
    var id: u64 = 0;
    _ = t.expectEq(c.fipc_rpc_submit(p.server, 1, null, 16, &id, 0), c.FIPC_INVALID, "rpc_submit of a NULL payload with a length: INVALID");
    return t.done();
}

// The one public struct has the size and layout the bindings assume.
test "fast: fipc_rpc_msg_t has the layout the bindings assume" {
    var t = ts.begin(@src(), 30);
    defer t.end();
    _ = t.expect(@sizeOf(c.fipc_rpc_msg_t) == 32, "fipc_rpc_msg_t's size changed: update the bindings");
    _ = t.expect(@offsetOf(c.fipc_rpc_msg_t, "len") == 24, "fipc_rpc_msg_t's layout changed: update the bindings");
    return t.done();
}
