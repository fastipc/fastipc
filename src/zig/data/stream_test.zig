//! The data path against a faulty or corrupt peer, and messages that arrive a piece at a time. A segment in plain
//! memory is viewed by two endpoints as a connection's two sides (no handshake): the server writes the s2c ring and
//! the client reads it. A test writes frames into the s2c ring by hand, as a faulty sender would (`Peer`), and checks
//! that the receiver returns a result: never a lost message it could deliver, an abort, a hang or a spin.

const std = @import("std");
const testing = std.testing;
const abi = @import("../abi.zig");
const endpoint = @import("../lifecycle/endpoint.zig");
const ring = @import("ring.zig");
const rpc = @import("rpc.zig");
const stream = @import("stream.zig");
const ring_wait = @import("ring_wait.zig");

const Endpoint = endpoint.Endpoint;
const Ring = ring.Ring;
const flag_start = abi.flag_start;
const flag_end = abi.flag_end;
const flag_pad = abi.flag_pad;

/// A connection's two sides over a segment in plain memory.
const Pair = struct {
    header: *abi.SegmentHeader,
    /// Both rings, and room after them for a header read off a frame's boundary (as a segment has).
    data: []align(16) u8,
    server: *Endpoint,
    client: *Endpoint,

    fn init(cap: u32) !Pair {
        const gpa = testing.allocator;
        const header = try gpa.create(abi.SegmentHeader);
        errdefer gpa.destroy(header);
        header.* = std.mem.zeroes(abi.SegmentHeader);
        const data = try gpa.alignedAlloc(u8, .@"16", 2 * @as(usize, cap) + 64);
        errdefer gpa.free(data);
        @memset(data, 0);
        const server = try side(header, data, cap, true);
        errdefer gpa.destroy(server);
        return .{ .header = header, .data = data, .server = server, .client = try side(header, data, cap, false) };
    }

    fn side(header: *abi.SegmentHeader, data: []u8, cap: u32, listener: bool) !*Endpoint {
        const ep = try testing.allocator.create(Endpoint);
        ep.* = .{ .hot = std.mem.zeroes(endpoint.Hot), .io = testing.io, .gpa = testing.allocator, .io_ref = null, .handles = undefined };
        ep.hot.view_header.store(header, .monotonic);
        ep.hot.view_data.store(data.ptr, .monotonic);
        ep.hot.view_cap.store(cap, .monotonic);
        ep.hot.view_server.store(@intFromBool(listener), .monotonic);
        ep.hot.status.store(endpoint.Status{ .phase = .connected }, .monotonic);
        ep.hot.view_number.store(1, .release);
        return ep;
    }

    fn deinit(p: *Pair) void {
        testing.allocator.destroy(p.client);
        testing.allocator.destroy(p.server);
        testing.allocator.free(p.data);
        testing.allocator.destroy(p.header);
    }

    /// The server's send ring, which the client receives from.
    fn ring(p: *const Pair) Ring {
        return Ring.forSend(&p.server.hot);
    }

    fn peer(p: *const Pair) Peer {
        return .{ .r = p.ring(), .hot = &p.server.hot };
    }

    /// The client's receive into `buf`, and the message's length.
    fn recv(p: *Pair, buf: []u8, timeout_ms: c_int) stream.Error!usize {
        var len: usize = undefined;
        try stream.recv(p.client, buf, timeout_ms, &len);
        return len;
    }

    /// Whether the client's next message is `expected`, within `timeout_ms`.
    fn recvIs(p: *Pair, timeout_ms: c_int, expected: []const u8) bool {
        var buf: [256]u8 = undefined;
        const len = p.recv(&buf, timeout_ms) catch return false;
        return std.mem.eql(u8, buf[0..len], expected);
    }
};

/// A faulty sender: writes a frame's header and bytes at the head, whatever they claim, and publishes the head a frame
/// further (the bytes written, padded as a frame is).
const Peer = struct {
    r: Ring,
    hot: *const endpoint.Hot,

    fn frame(p: Peer, flags: u32, frag_len: u32, total_len: u64, payload: []const u8) void {
        const head = p.r.head();
        const hdr: abi.FragHdr = .{ .flags = flags, .frag_len = frag_len, .total_len = total_len };
        p.put(head, std.mem.asBytes(&hdr));
        p.put(head +% ring.frag_hdr_len, payload);
        ring_wait.publishHead(p.hot, p.r, head +% ring.frameLen(payload.len));
    }

    /// A message of one piece.
    fn message(p: Peer, payload: []const u8) void {
        const len: u32 = @intCast(payload.len);
        p.frame(flag_start | flag_end, len, len, payload);
    }

    fn put(p: Peer, pos: u64, bytes: []const u8) void {
        for (bytes, 0..) |byte, i| p.r.data[@intCast((pos +% i) % p.r.cap)] = byte;
    }

    fn setHead(p: Peer, value: u64) void {
        p.r.ctrl.writer.head.store(value, .release);
    }

    fn setTail(p: Peer, value: u64) void {
        p.r.ctrl.reader.tail.store(value, .release);
    }

    /// After `ms`, the pieces of `msg` from byte `from` on, `piece` bytes each (the last with END), one every `ms`.
    fn later(p: Peer, msg: []const u8, from: usize, piece: usize, ms: u64) std.Thread {
        const Writer = struct {
            fn run(w: Peer, bytes: []const u8, start: usize, n: usize, every: u64) void {
                var at = start;
                while (at < bytes.len) {
                    sleepMs(every);
                    const len = @min(n, bytes.len - at);
                    w.frame(if (at + len == bytes.len) flag_end else 0, @intCast(len), 0, bytes[at..][0..len]);
                    at += len;
                }
            }
        };
        return std.Thread.spawn(.{}, Writer.run, .{ p, msg, from, piece, ms }) catch @panic("thread");
    }
};

fn sleepMs(ms: u64) void {
    testing.io.sleep(.fromMilliseconds(@intCast(ms)), .awake) catch {};
}

fn nowMs() i64 {
    return std.Io.Clock.awake.now(testing.io).toMilliseconds();
}

fn pattern(buf: []u8, seed: u8) void {
    for (buf, 0..) |*byte, i| byte.* = seed +% @as(u8, @truncate(i *% 7));
}

/// This side's end of a wait, as `fipc_cancel` or the peer's end publishes it: the change, then the local interrupt.
fn cancel(ep: *Endpoint) void {
    _ = ep.hot.requests.fetchOr(endpoint.Requests{ .cancel = true }, .seq_cst);
    ring_wait.wakeOwnWaiters(&ep.hot);
}

fn peerEnds(ep: *Endpoint) void {
    ep.hot.status.store(endpoint.Status{ .phase = .peer_dead }, .seq_cst);
    ring_wait.wakeOwnWaiters(&ep.hot);
}

/// A receive on another thread, whose result the test looks at once it has returned.
const Receive = struct {
    pair: *Pair,
    buf: []u8,
    timeout_ms: c_int,
    result: stream.Error!usize = undefined,
    done: std.atomic.Value(bool) = .init(false),
    thread: std.Thread = undefined,

    fn start(r: *Receive) void {
        r.thread = std.Thread.spawn(.{}, run, .{r}) catch @panic("thread");
    }

    fn run(r: *Receive) void {
        r.result = r.pair.recv(r.buf, r.timeout_ms);
        r.done.store(true, .release);
    }
};

// === A started message completes ===

// A message of two pieces whose second piece comes after the receive's timeout: the receive waits for it and returns
// the whole message. A zero-copy receive before it is TooLarge with the message's length, and takes nothing.
test "a message whose rest arrives after the receive's timeout still arrives whole" {
    var p: Pair = try .init(4096);
    defer p.deinit();
    var message: [200]u8 = undefined;
    pattern(&message, 3);
    p.peer().frame(flag_start, 100, message.len, message[0..100]);

    var data: [*]const u8 = undefined;
    var len: usize = 0;
    try testing.expectError(error.TooLarge, stream.recvAcquire(p.client, 0, &data, &len));
    try testing.expectEqual(@as(usize, message.len), len);
    try testing.expectEqual(@as(u64, 0), p.ring().tail()); // nothing taken

    const writer = p.peer().later(&message, 100, 100, 150);
    defer writer.join();
    var got: [256]u8 = undefined;
    const start = nowMs();
    const n = try p.recv(&got, 50);
    try testing.expect(nowMs() - start >= 100); // past its timeout, for the rest
    try testing.expectEqualSlices(u8, &message, got[0..n]);
    try testing.expectError(error.Timeout, p.recv(&got, 0));
}

// The timeout bounds the wait for the first piece; once it has come, the pieces that come every 100 ms after it are
// waited for, however long they take.
test "a receive's timeout bounds its wait for the first piece only" {
    var p: Pair = try .init(4096);
    defer p.deinit();
    var got: [600]u8 = undefined;
    var start = nowMs();
    try testing.expectError(error.Timeout, p.recv(&got, 150));
    try testing.expect(nowMs() - start < 1000);

    var message: [500]u8 = undefined;
    pattern(&message, 9);
    p.peer().frame(flag_start, 100, message.len, message[0..100]);
    const writer = p.peer().later(&message, 100, 100, 100);
    defer writer.join();
    start = nowMs();
    const n = try p.recv(&got, 150);
    try testing.expect(nowMs() - start >= 300);
    try testing.expectEqualSlices(u8, &message, got[0..n]);
}

// A consumer that polls: a request longer than a piece whose rest arrives after the poll found its first
// piece is taken whole by that poll.
test "an RPC poll that finds a request's first piece takes the whole request" {
    var p: Pair = try .init(1 << 20);
    defer p.deinit();
    var msg: abi.RpcMsg = undefined;
    var payload: [256]u8 = undefined;
    try testing.expectError(error.Timeout, rpc.recv(p.client, &payload, 0, &msg));

    var request: [@sizeOf(abi.RpcMsg) + 200]u8 = undefined;
    pattern(&request, 11);
    const header: abi.RpcMsg = .{ .id = 77, .kind = abi.rpc_request, .opcode = 42, .status = 0, .reserved = 0, .len = 200 };
    @memcpy(request[0..@sizeOf(abi.RpcMsg)], std.mem.asBytes(&header));
    p.peer().frame(flag_start, 132, request.len, request[0..132]);
    const writer = p.peer().later(&request, 132, 100, 100);
    defer writer.join();
    try rpc.recv(p.client, &payload, 0, &msg);
    try testing.expect(msg.id == 77 and msg.opcode == 42 and msg.kind == abi.rpc_request and msg.len == 200);
    try testing.expectEqualSlices(u8, request[@sizeOf(abi.RpcMsg)..], payload[0..200]);
}

// A submit that fails takes no id: the request that times out on a full ring leaves its id to the next request sent.
test "a submit that times out takes no id" {
    var p: Pair = try .init(4096);
    defer p.deinit();
    var payload: [1000]u8 = undefined;
    pattern(&payload, 3);
    var id: u64 = 0;
    var sent: u64 = 0;
    while (rpc.submit(p.server, 1, &payload, 0, &id)) : (sent += 1) {
        try testing.expectEqual(sent + 1, id);
    } else |err| try testing.expectEqual(error.Timeout, err);
    try testing.expect(sent > 0);
    try testing.expectError(error.Timeout, rpc.submit(p.server, 1, &payload, 0, &id));

    var msg: abi.RpcMsg = undefined;
    var got: [1000]u8 = undefined;
    try rpc.recv(p.client, &got, 0, &msg);
    try testing.expectEqual(1, msg.id);
    try rpc.submit(p.server, 2, "next", 0, &id);
    try testing.expectEqual(sent + 1, id);
}

// A receive in the middle of a message whose rest never comes waits past its timeout until cancel stops it. The
// message is dropped whole: its rest, if it comes, is skipped, and a receive that needn't wait still works.
test "cancel stops a receive in the middle of a message, which is dropped whole" {
    var p: Pair = try .init(4096);
    defer p.deinit();
    p.peer().frame(flag_start, 4, 8, &.{ 1, 2, 3, 4 });
    var got: [16]u8 = undefined;
    var receive: Receive = .{ .pair = &p, .buf = &got, .timeout_ms = 50 };
    receive.start();
    sleepMs(300);
    try testing.expect(!receive.done.load(.acquire)); // past its timeout
    cancel(p.client);
    receive.thread.join();
    try testing.expectError(error.Cancelled, receive.result);

    p.peer().frame(flag_end, 4, 0, &.{ 5, 6, 7, 8 });
    p.peer().message("next");
    try testing.expect(p.recvIs(0, "next"));
    try testing.expectError(error.Cancelled, p.recv(&got, 0)); // it would wait
}

// The peer's end stops a receive in the middle of a message: Disconnected, and the message is dropped. The messages
// the peer completed before are delivered first.
test "the peer's end stops a receive in the middle of a message: Disconnected" {
    var p: Pair = try .init(4096);
    defer p.deinit();
    p.peer().message("done");
    p.peer().frame(flag_start, 4, 8, &.{ 1, 2, 3, 4 });
    try testing.expect(p.recvIs(0, "done"));
    var got: [16]u8 = undefined;
    var receive: Receive = .{ .pair = &p, .buf = &got, .timeout_ms = 50 };
    receive.start();
    sleepMs(100);
    peerEnds(p.client);
    receive.thread.join();
    try testing.expectError(error.Disconnected, receive.result);
    try testing.expectError(error.Disconnected, p.recv(&got, 1000));
}

// A sender that gave its message up (cancel) begins its next message with a START: the receiver drops the unfinished
// one and takes the new one. Before its first piece is taken, the unfinished message is the next one, with its length.
test "a new message inside an unfinished one drops it and begins again" {
    var p: Pair = try .init(4096);
    defer p.deinit();
    p.peer().frame(flag_start, 100, 10240, &@as([100]u8, @splat(0x11)));
    p.peer().message("hello");
    var small: [64]u8 = undefined;
    try testing.expectError(error.TooLarge, p.recv(&small, 100));
    const big = try testing.allocator.alloc(u8, 10240);
    defer testing.allocator.free(big);
    const n = try p.recv(big, 100);
    try testing.expectEqualStrings("hello", big[0..n]);
}

// === Corrupt pieces ===

// A piece longer than what its message has left, and an END before the message's length, are corrupt: each is
// consumed, the message dropped (Invalid), and the next message arrives.
test "a piece that breaks its message is consumed, and the message dropped" {
    var p: Pair = try .init(4096);
    defer p.deinit();
    const peer = p.peer();
    var got: [4096]u8 = undefined;

    peer.frame(flag_start, 16, 20, &@as([16]u8, @splat(0x22)));
    peer.frame(flag_end, 16, 0, &@as([16]u8, @splat(0x23)));
    peer.message("first");
    try testing.expectError(error.Invalid, p.recv(&got, 100)); // 16 + 16 bytes of a 20-byte message
    try testing.expect(p.recvIs(100, "first"));

    peer.frame(flag_start, 16, 4000, &@as([16]u8, @splat(0x24)));
    peer.frame(flag_end, 16, 0, &@as([16]u8, @splat(0x25)));
    peer.message("second");
    try testing.expectError(error.Invalid, p.recv(&got, 100)); // an END at 32 bytes of a 4000-byte message
    try testing.expect(p.recvIs(100, "second"));
}

// A frame that doesn't fit the ring where it lies is Invalid, at a message's start and in its middle, at once; it
// stays, since its end is unknown.
test "a frame no ring can hold is Invalid, at once and again" {
    var got: [4096]u8 = undefined;
    {
        var p: Pair = try .init(4096);
        defer p.deinit();
        p.peer().frame(flag_start | flag_end, 4096, 4096, &.{});
        try testing.expectError(error.Invalid, p.recv(&got, 100));
        try testing.expectError(error.Invalid, p.recv(&got, 0));
    }
    {
        var p: Pair = try .init(4096);
        defer p.deinit();
        p.peer().frame(flag_start, 1000, 2000, &@as([1000]u8, @splat(0x33)));
        p.peer().frame(0, 0xFFFF_FFF0, 0, &.{});
        const start = nowMs();
        try testing.expectError(error.Invalid, p.recv(&got, 1000));
        try testing.expect(nowMs() - start < 500);
        try testing.expectError(error.Invalid, p.recv(&got, 0));
    }
    {
        var p: Pair = try .init(4096);
        defer p.deinit();
        p.peer().frame(flag_pad, std.math.maxInt(u32), 0, &.{});
        try testing.expectError(error.Invalid, p.recv(&got, 100));
    }
}

// The rest of a message nobody receives (its sender crashed in the middle) is skipped.
test "orphaned pieces are skipped" {
    var p: Pair = try .init(4096);
    defer p.deinit();
    p.peer().frame(flag_end, 4, 4, &.{ 0xDE, 0xAD, 0xBE, 0xEF });
    p.peer().message("hello");
    try testing.expect(p.recvIs(100, "hello"));
}

// A plain message holds at least a byte, but a faulty peer's zero-length message arrives as an empty message, and the
// one after it arrives too.
test "a zero-length message arrives empty, and the next one after it" {
    var p: Pair = try .init(4096);
    defer p.deinit();
    p.peer().message("");
    p.peer().message("after");
    var empty: [0]u8 = .{};
    try testing.expectEqual(@as(usize, 0), try p.recv(&empty, 100));
    try testing.expect(p.recvIs(100, "after"));
}

// The library allocates nothing by a length the peer wrote: a message claiming more than any buffer is TooLarge with
// that length, and stays.
test "a length no buffer holds is TooLarge, with that length" {
    var p: Pair = try .init(4096);
    defer p.deinit();
    p.peer().frame(flag_start, 0, std.math.maxInt(u64), &.{});
    var got: [16]u8 = undefined;
    var len: usize = 0;
    try testing.expectError(error.TooLarge, stream.recv(p.client, &got, 100, &len));
    try testing.expectEqual(@as(usize, std.math.maxInt(usize)), len);
    try testing.expectError(error.TooLarge, stream.recv(p.client, &got, 0, &len));
}

// === The zero-copy calls' guards ===

// A copy send after a reservation, and a reservation that stops after its PAD moved the head, drop the earlier
// reservation: its commit is Invalid, so it never publishes over room it no longer owns.
test "a send or a failed reservation drops a reservation" {
    var buffer: [*]u8 = undefined;
    {
        var p: Pair = try .init(1024);
        defer p.deinit();
        try stream.sendAcquire(p.server, 16, 0, &buffer);
        var bytes: [900]u8 = undefined;
        pattern(&bytes, 1);
        try stream.send(p.server, &.{}, &bytes, 0);
        try testing.expectError(error.Invalid, stream.sendCommit(p.server, 16));
        try testing.expect(p.ring().head() -% p.ring().tail() <= 1024);
    }
    {
        var p: Pair = try .init(1024);
        defer p.deinit();
        // The head at offset 912 with 800 bytes unread: 112 bytes to the ring's end, 224 free
        var a: [84]u8 = undefined;
        var b: [784]u8 = undefined;
        pattern(&a, 2);
        pattern(&b, 3);
        try stream.send(p.server, &.{}, &a, 0);
        try stream.send(p.server, &.{}, &b, 0);
        var got: [128]u8 = undefined;
        _ = try p.recv(&got, 100);
        try stream.sendAcquire(p.server, 50, 0, &buffer);
        try testing.expectError(error.Timeout, stream.sendAcquire(p.server, 184, 0, &buffer)); // a PAD, then no room
        try testing.expectError(error.Invalid, stream.sendCommit(p.server, 50));
        try testing.expect(p.ring().head() -% p.ring().tail() <= 1024);
    }
}

// A second release, and a receive after an acquire, never consume more than the acquired message: the tail never
// passes the head. A receive releases an acquired message first.
test "a release consumes the acquired message once" {
    var p: Pair = try .init(4096);
    defer p.deinit();
    var data: [*]const u8 = undefined;
    var len: usize = 0;
    try stream.send(p.server, &.{}, "hi", 0);
    try stream.recvAcquire(p.client, 100, &data, &len);
    stream.recvRelease(p.client);
    stream.recvRelease(p.client);
    try testing.expectEqual(p.ring().head(), p.ring().tail());
    var got: [16]u8 = undefined;
    try testing.expectError(error.Timeout, p.recv(&got, 100));

    try stream.send(p.server, &.{}, "one", 0);
    try stream.send(p.server, &.{}, "two", 0);
    try stream.recvAcquire(p.client, 100, &data, &len);
    try testing.expectEqualStrings("one", data[0..len]);
    try testing.expectEqual(@as(usize, 0), @intFromPtr(data) % 16); // aligned
    try testing.expect(p.recvIs(100, "two"));
    stream.recvRelease(p.client);
    try testing.expectEqual(p.ring().head(), p.ring().tail());
}

// A ring whose tail is past its head (a corrupt peer's indices) is Invalid for its reader, and for its writer once it
// loads the tail, when the room it saw runs out; and so is a head off a frame's boundary for its writer.
test "a tail past the head is Invalid for the reader and the writer" {
    var p: Pair = try .init(4096);
    defer p.deinit();
    // The writer saw the ring empty; 2080 bytes of that room are left
    try stream.send(p.server, &.{}, &@as([2000]u8, @splat('x')), 0);
    p.peer().setTail(p.ring().head() +% 64);
    var got: [16]u8 = undefined;
    try testing.expectError(error.Invalid, p.recv(&got, 100));
    var data: [*]const u8 = undefined;
    var len: usize = 0;
    try testing.expectError(error.Invalid, stream.recvAcquire(p.client, 0, &data, &len));
    try testing.expectError(error.Invalid, stream.send(p.server, &.{}, &@as([2100]u8, @splat('x')), 100));
    var buffer: [*]u8 = undefined;
    try testing.expectError(error.Invalid, stream.sendAcquire(p.server, 16, 0, &buffer));

    p.peer().setTail(8);
    p.peer().setHead(8);
    try testing.expectError(error.Invalid, stream.send(p.server, &.{}, "x", 100));
}

// The indices only ever wrap: a pair whose indices start 16 bytes below 2^64 passes messages across the wrap, copied
// and zero-copy.
test "the ring's indices wrap around 2^64" {
    var p: Pair = try .init(4096);
    defer p.deinit();
    const near_the_end: u64 = std.math.maxInt(u64) - 15;
    p.peer().setHead(near_the_end);
    p.peer().setTail(near_the_end);
    // Each side's copy of the other's index starts where the indices do, as in a segment whose indices start at 0
    p.client.recv.head_seen = near_the_end;
    p.server.send.tail_seen = near_the_end;

    var message: [100]u8 = undefined;
    pattern(&message, 5);
    try stream.send(p.server, &.{}, &message, 0);
    var got: [128]u8 = undefined;
    const n = try p.recv(&got, 100);
    try testing.expectEqualSlices(u8, &message, got[0..n]);

    var buffer: [*]u8 = undefined;
    try stream.sendAcquire(p.server, 16, 0, &buffer);
    @memcpy(buffer[0..16], "zero-copy, wraps");
    try stream.sendCommit(p.server, 16);
    var data: [*]const u8 = undefined;
    var len: usize = 0;
    try stream.recvAcquire(p.client, 100, &data, &len);
    try testing.expectEqualStrings("zero-copy, wraps", data[0..len]);
    stream.recvRelease(p.client);
    try testing.expectEqual(p.ring().head(), p.ring().tail());
}

// Every frame starts at a multiple of 16 bytes, so every piece's bytes are 16-byte aligned; a frame that wouldn't fit
// before the ring's end goes after a PAD at its start.
test "frames are aligned and never wrap" {
    var p: Pair = try .init(1024);
    defer p.deinit();
    var got: [1024]u8 = undefined;
    for (1..200) |size| {
        var message: [199]u8 = undefined;
        pattern(message[0..size], @truncate(size));
        var buffer: [*]u8 = undefined;
        try stream.sendAcquire(p.server, size, 0, &buffer);
        try testing.expectEqual(@as(usize, 0), @intFromPtr(buffer) % 16);
        try testing.expect(@intFromPtr(buffer) + size <= @intFromPtr(p.ring().data) + 1024);
        @memcpy(buffer[0..size], message[0..size]);
        try stream.sendCommit(p.server, size);
        try stream.send(p.server, &.{}, message[0..size], 0);
        for (0..2) |_| {
            const n = try p.recv(&got, 0);
            try testing.expectEqualSlices(u8, message[0..size], got[0..n]);
        }
    }
}
