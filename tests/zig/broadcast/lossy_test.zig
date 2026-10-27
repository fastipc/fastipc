//! Lossy subscribers (`FIPC_SUBSCRIBER_LOSSY`): every message whole and in order, copied or written in place; lapped by
//! the publisher, OVERRUN once with the number of messages lost, then a recent message.

const support = @import("../support.zig");
const shared = @import("shared.zig");
const c = support.c;

const lossy = shared.lossy;
const joined = shared.joined;
const sendStamp = shared.sendStamp;
const recvStamp = shared.recvStamp;

test "fast: a lossy subscriber receives every message whole and in order, copied or written in place" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    var broadcast: support.Broadcast = .{};
    const name = try support.openBroadcast(&t, &broadcast, &buf, "bc_order", 65536, 4, c.FIPC_FOREVER);
    defer broadcast.close();
    const subscriber = try joined(&t, name, lossy);
    defer c.fipc_close(subscriber);

    var result: c.fipc_result_t = 0;
    for (0..200) |i| {
        try t.checkEqual(sendStamp(broadcast.publisher, i, support.Stamp.sizeOf(i, 4096), 0), c.FIPC_OK, "send");
        try t.checkEqual(recvStamp(subscriber, 1000, &result) orelse 999_999, i, "the message, whole");
    }
    // The writer's zero-copy send, committed shorter than reserved
    var out: ?*anyopaque = null;
    try t.checkEqual(c.fipc_send_acquire(broadcast.publisher, 1000, &out, 0), c.FIPC_OK, "send_acquire");
    const room: [*]u8 = @ptrCast(out.?);
    _ = t.expect(@intFromPtr(room) % 64 == 0, "the room is 64-byte aligned");
    support.Stamp.fill(room[0..100], 200);
    _ = t.expectEqual(c.fipc_send_commit(broadcast.publisher, 1001), c.FIPC_INVALID, "a commit past the reservation");
    try t.checkEqual(c.fipc_send_commit(broadcast.publisher, 100), c.FIPC_OK, "a shorter commit");
    _ = t.expectEqual(c.fipc_send_commit(broadcast.publisher, 100), c.FIPC_INVALID, "a second commit");
    _ = t.expectEqual(recvStamp(subscriber, 1000, &result) orelse 999_999, 200, "the zero-copy message");
    _ = t.expect(recvStamp(subscriber, 0, &result) == null and result == c.FIPC_TIMEOUT, "nothing more");
    return t.done();
}

test "fast: a lossy subscriber lapped by the writer gets OVERRUN once, then goes on from a recent message" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    var broadcast: support.Broadcast = .{};
    const name = try support.openBroadcast(&t, &broadcast, &buf, "bc_lap", 4096, 1, c.FIPC_FOREVER);
    defer broadcast.close();
    const subscriber = try joined(&t, name, lossy);
    defer c.fipc_close(subscriber);
    // Three laps of the 64 cells, never read: the writer never waits for a lossy subscriber
    const count = 3 * 64;
    for (0..count) |i| try t.checkEqual(sendStamp(broadcast.publisher, i, 48, 0), c.FIPC_OK, "send");
    var result: c.fipc_result_t = 0;
    _ = t.expect(recvStamp(subscriber, 0, &result) == null and result == c.FIPC_OVERRUN, "OVERRUN");
    var prev: ?u64 = null;
    while (recvStamp(subscriber, 0, &result)) |seq| {
        if (prev) |previous| _ = t.expectEqual(seq, previous + 1, "one after the other, once resynced") else _ = t.expect(seq >= count - 64, "a recent message");
        prev = seq;
    }
    _ = t.expectEqual(result, c.FIPC_TIMEOUT, "until there is nothing more");
    _ = t.expectEqual(prev orelse 0, count - 1, "up to the last message");
    // Data laps without cell laps: messages of 1 KiB lap the 4 KiB ring every 4 messages
    for (0..40) |i| try t.checkEqual(sendStamp(broadcast.publisher, count + i, 1024, 0), c.FIPC_OK, "send of 1 KiB");
    _ = t.expect(recvStamp(subscriber, 0, &result) == null and result == c.FIPC_OVERRUN, "OVERRUN on the data ring");
    prev = null;
    while (recvStamp(subscriber, 0, &result)) |seq| prev = seq;
    _ = t.expectEqual(result, c.FIPC_TIMEOUT, "until there is nothing more");
    _ = t.expectEqual(prev orelse 0, count + 39, "up to the last message");
    return t.done();
}

test "fast: OVERRUN stores in out_len the number of messages lost: to a cell lap, and to a copy the frontier tore" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    var broadcast: support.Broadcast = .{};
    const name = try support.openBroadcast(&t, &broadcast, &buf, "bc_lost", 4096, 1, c.FIPC_FOREVER);
    defer broadcast.close();
    const subscriber = try joined(&t, name, lossy);
    defer c.fipc_close(subscriber);
    var result: c.fipc_result_t = 0;
    for (0..4) |i| try t.checkEqual(sendStamp(broadcast.publisher, i, 48, 0), c.FIPC_OK, "send");
    for (0..4) |i| _ = t.expectEqual(recvStamp(subscriber, 0, &result) orelse 999, i, "the first messages");
    // At 4, lapped twice: 48-byte messages take 64 bytes, and the writer announces a frontier, with `next_mark`, every
    // fourth message (docs/protocol.md §9.6): 140 is the mark after message 143, so 4 to 139 are lost
    for (4..144) |i| try t.checkEqual(sendStamp(broadcast.publisher, i, 48, 0), c.FIPC_OK, "send");
    var receive_buffer: [1024]u8 = undefined;
    var len: usize = 0;
    _ = t.expectEqual(support.recv(subscriber, &receive_buffer, &len, 0), c.FIPC_OVERRUN, "OVERRUN after the laps");
    _ = t.expectEqual(len, 140 - 4, "the messages lost");
    for (140..144) |i| _ = t.expectEqual(recvStamp(subscriber, 0, &result) orelse 999, i, "from the mark on");
    _ = t.expect(recvStamp(subscriber, 0, &result) == null and result == c.FIPC_TIMEOUT, "nothing more: TIMEOUT");
    // Message 144's payload is at 9 KiB in the ring. Five 1 KiB messages move the frontier more than a ring past it
    // while its cell still holds it, so a copy of it is torn. Each of them announces a frontier, so the mark is 148,
    // the last one
    for (144..149) |i| try t.checkEqual(sendStamp(broadcast.publisher, i, 1024, 0), c.FIPC_OK, "send of 1 KiB");
    len = 0;
    _ = t.expectEqual(support.recv(subscriber, &receive_buffer, &len, 0), c.FIPC_OVERRUN, "OVERRUN for the torn copy");
    _ = t.expectEqual(len, 148 - 144, "the messages lost, the torn one included");
    _ = t.expectEqual(recvStamp(subscriber, 0, &result) orelse 999, 148, "the last message");
    // The writer gone: what it published was received, and DISCONNECTED stores nothing
    broadcast.close();
    len = 77;
    _ = t.expectEqual(support.recv(subscriber, &receive_buffer, &len, 1000), c.FIPC_DISCONNECTED, "DISCONNECTED");
    _ = t.expectEqual(len, 77, "out_len untouched");
    return t.done();
}

test "fast: a message a lossy subscriber left queued with TOO_LARGE is lost to a lap: OVERRUN, then a recent message" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    var broadcast: support.Broadcast = .{};
    const name = try support.openBroadcast(&t, &broadcast, &buf, "bc_lap_large", 4096, 1, c.FIPC_FOREVER);
    defer broadcast.close();
    const subscriber = try joined(&t, name, lossy);
    defer c.fipc_close(subscriber);
    try t.checkEqual(sendStamp(broadcast.publisher, 0, 100, 0), c.FIPC_OK, "send");
    var small: [8]u8 = undefined;
    var len: usize = 0;
    _ = t.expectEqual(support.recv(subscriber, &small, &len, 1000), c.FIPC_TOO_LARGE, "a buffer too small");
    _ = t.expectEqual(len, 100, "the message's length");
    // Three laps of the 64 cells while it stays queued
    const count = 3 * 64;
    for (1..count + 1) |i| try t.checkEqual(sendStamp(broadcast.publisher, i, 48, 0), c.FIPC_OK, "send");
    var result: c.fipc_result_t = 0;
    _ = t.expect(recvStamp(subscriber, 0, &result) == null and result == c.FIPC_OVERRUN, "OVERRUN instead of the queued message");
    var prev: ?u64 = null;
    while (recvStamp(subscriber, 0, &result)) |seq| {
        if (prev) |previous| _ = t.expectEqual(seq, previous + 1, "one after the other") else _ = t.expect(seq > count - 64, "a recent message");
        prev = seq;
    }
    _ = t.expectEqual(result, c.FIPC_TIMEOUT, "until there is nothing more");
    _ = t.expectEqual(prev orelse 0, count, "up to the last message");
    return t.done();
}
