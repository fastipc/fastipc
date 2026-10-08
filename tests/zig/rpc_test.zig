//! The RPC layer: `fipc_rpc_submit` sends a request with a fresh id, `fipc_rpc_respond` answers it with that id, and
//! `fipc_rpc_recv` receives either, its 32-byte header into a `fipc_rpc_msg_t` and its payload into the caller's
//! buffer. (A blocked RPC receive and cancel: cancel_test.zig; and a peer's death: peer_death_test.zig.)

const std = @import("std");
const ts = @import("support.zig");
const c = ts.c;

// Either side submits and responds: ids are never 0 and grow per connection, and a response carries the request's id,
// the opcode and status the responder gave, and its payload.
test "fast: ids are never 0 and grow, and a response carries its request's id" {
    var t = ts.begin(@src(), 30);
    defer t.end();
    var p: ts.Pair = .{};
    try ts.openPair(&t, &p, "rpc_ids", 4096);
    defer p.close();
    for ([_][2]ts.Conn{ .{ p.client, p.server }, .{ p.server, p.client } }) |sides| {
        const asker, const answerer = sides;
        var last: u64 = 0;
        for (0..3) |i| {
            var id: u64 = 0;
            try t.checkEq(c.fipc_rpc_submit(asker, @intCast(i), "ask", 3, &id, 1000), c.FIPC_OK, "submit");
            _ = t.expect(id != 0 and id > last, "ids are never 0 and grow");
            last = id;
            var msg: c.fipc_rpc_msg_t = undefined;
            var body: [8]u8 = undefined;
            try t.checkEq(c.fipc_rpc_recv(answerer, &body, body.len, &msg, 1000), c.FIPC_OK, "the request arrives");
            _ = t.expect(msg.kind == c.FIPC_RPC_REQUEST and msg.id == id and msg.opcode == i and msg.status == 0 and msg.len == 3, "as sent");
            try t.checkEq(c.fipc_rpc_respond(answerer, msg.id, 77, -5, "answer", 6, 1000), c.FIPC_OK, "respond");
            try t.checkEq(c.fipc_rpc_recv(asker, &body, body.len, &msg, 1000), c.FIPC_OK, "the response arrives");
            _ = t.expect(msg.kind == c.FIPC_RPC_RESPONSE and msg.id == id and msg.opcode == 77 and msg.status == -5 and
                msg.len == 6 and std.mem.eql(u8, body[0..6], "answer"), "with the request's id, and the response's fields");
        }
    }
    return t.done();
}

// A submit that fails takes no id: a request that times out on a full ring changes nothing, and the next request
// that is sent gets the next id, with no gap.
test "fast: a submit that times out takes no id" {
    var t = ts.begin(@src(), 30);
    defer t.end();
    var p: ts.Pair = .{};
    try ts.openPair(&t, &p, "rpc_failed_id", 4096);
    defer p.close();
    var payload: [500]u8 = undefined;
    @memset(&payload, 0x3c);
    var sent: u64 = 0;
    var id: u64 = 0;
    while (true) : (sent += 1) {
        if (sent == 100) return t.fail("a 4 KiB ring never filled");
        const r = c.fipc_rpc_submit(p.client, 1, &payload, payload.len, &id, 0);
        if (r == c.FIPC_TIMEOUT) break;
        try t.checkEq(r, c.FIPC_OK, "submit until the ring is full");
        _ = t.expectEq(id, sent + 1, "ids from 1, one apart");
    }
    try t.check(sent > 0, "some requests fit");
    id = 12345;
    _ = t.expectEq(c.fipc_rpc_submit(p.client, 2, &payload, payload.len, &id, 20), c.FIPC_TIMEOUT, "a waiting submit times out too");
    _ = t.expectEq(id, 12345, "a failed submit sets no id");

    var msg: c.fipc_rpc_msg_t = undefined;
    var body: [500]u8 = undefined;
    for (0..sent) |i| {
        try t.checkEq(c.fipc_rpc_recv(p.server, &body, body.len, &msg, 1000), c.FIPC_OK, "the requests that were sent arrive");
        _ = t.expect(msg.id == i + 1 and msg.opcode == 1, "in order, with their ids");
    }
    _ = t.expectEq(c.fipc_rpc_recv(p.server, &body, body.len, &msg, 0), c.FIPC_TIMEOUT, "and nothing of the failed ones");

    try t.checkEq(c.fipc_rpc_submit(p.client, 3, "next", 4, &id, 1000), c.FIPC_OK, "a submit with room");
    _ = t.expectEq(id, sent + 1, "gets the next id: the failed submits took none");
    try t.checkEq(c.fipc_rpc_recv(p.server, &body, body.len, &msg, 1000), c.FIPC_OK, "it arrives");
    _ = t.expect(msg.id == sent + 1 and msg.opcode == 3 and msg.len == 4, "with that id");
    return t.done();
}

// RPC's receive into a buffer too small for the payload fills the message and returns TOO_LARGE, taking nothing; an
// empty payload needs no buffer.
test "fast: an RPC receive into a small buffer fills the message and is TOO_LARGE" {
    var t = ts.begin(@src(), 30);
    defer t.end();
    var p: ts.Pair = .{};
    try ts.openPair(&t, &p, "rpc_small_buffer", 4096);
    defer p.close();
    var payload: [100]u8 = undefined;
    @memset(&payload, 0x5c);
    var id: u64 = 0;
    try t.checkEq(c.fipc_rpc_submit(p.client, 9, &payload, payload.len, &id, 0), c.FIPC_OK, "a request of 100 bytes");
    var msg = std.mem.zeroes(c.fipc_rpc_msg_t);
    var small: [10]u8 = undefined;
    _ = t.expectEq(c.fipc_rpc_recv(p.server, &small, small.len, &msg, 100), c.FIPC_TOO_LARGE, "into 10 bytes: TOO_LARGE");
    _ = t.expect(msg.id == id and msg.len == 100 and msg.opcode == 9 and msg.kind == c.FIPC_RPC_REQUEST, "the message is filled");
    var got: [100]u8 = undefined;
    _ = t.expectEq(c.fipc_rpc_recv(p.server, &got, got.len, &msg, 0), c.FIPC_OK, "then with room: OK");
    _ = t.expect(std.mem.eql(u8, &got, &payload), "the payload");
    try t.checkEq(c.fipc_rpc_submit(p.client, 10, null, 0, &id, 0), c.FIPC_OK, "an empty request");
    _ = t.expectEq(c.fipc_rpc_recv(p.server, null, 0, &msg, 100), c.FIPC_OK, "needs no buffer");
    _ = t.expect(msg.id == id and msg.len == 0 and msg.opcode == 10, "its message");
    return t.done();
}

// A message that isn't a well-formed RPC message (a plain 5-byte message) is dropped by `fipc_rpc_recv` with one
// INVALID, and the next request arrives.
test "fast: an RPC receive drops a message that isn't RPC" {
    var t = ts.begin(@src(), 30);
    defer t.end();
    var p: ts.Pair = .{};
    try ts.openPair(&t, &p, "rpc_malformed", 4096);
    defer p.close();
    try t.checkEq(ts.send(p.client, "hello", 0), c.FIPC_OK, "a plain message");
    var id: u64 = 0;
    try t.checkEq(c.fipc_rpc_submit(p.client, 6, "ok", 2, &id, 0), c.FIPC_OK, "a request after it");
    var msg: c.fipc_rpc_msg_t = undefined;
    var body: [8]u8 = undefined;
    _ = t.expectEq(c.fipc_rpc_recv(p.server, &body, body.len, &msg, 1000), c.FIPC_INVALID, "the plain message: INVALID");
    _ = t.expectEq(c.fipc_rpc_recv(p.server, &body, body.len, &msg, 1000), c.FIPC_OK, "then the request");
    _ = t.expect(msg.id == id and msg.opcode == 6, "intact");
    return t.done();
}

// A request of 1 MiB, one piece of a 2 MiB ring behind its 32-byte header, arrives whole into the caller's buffer.
test "fast: a 1 MiB request arrives whole" {
    var t = ts.begin(@src(), 30);
    defer t.end();
    var p: ts.Pair = .{};
    try ts.openPair(&t, &p, "rpc_large", 2 * 1024 * 1024);
    defer p.close();
    const payload_size: usize = 1024 * 1024;

    const payload = try std.testing.allocator.alloc(u8, payload_size);
    defer std.testing.allocator.free(payload);
    @memset(payload, 0xBB);
    const received = try std.testing.allocator.alloc(u8, payload_size);
    defer std.testing.allocator.free(received);

    var id: u64 = 0;
    try t.checkEq(c.fipc_rpc_submit(p.client, 0xAA, payload.ptr, payload_size, &id, 0), c.FIPC_OK, "submit");
    var msg: c.fipc_rpc_msg_t = undefined;
    try t.checkEq(c.fipc_rpc_recv(p.server, received.ptr, received.len, &msg, 5000), c.FIPC_OK, "recv");
    try t.check(msg.id == id and msg.opcode == 0xAA and msg.kind == c.FIPC_RPC_REQUEST, "Header data corrupted");
    try t.check(msg.len == payload_size, "Received wrong length");
    for (received) |byte| {
        if (byte != 0xBB) return t.fail("Payload data corrupted");
    }
    return t.done();
}
