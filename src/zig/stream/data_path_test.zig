//! The data path against a faulty or corrupt peer, and messages that arrive a piece at a time. A segment in plain
//! memory is viewed by two endpoints as a connection's two sides (no handshake): the server writes the s2c ring and
//! the client reads it. A test writes frames into the s2c ring by hand, as a faulty sender would (`Peer`), and checks
//! that the receiver returns a result: never a lost message it could deliver, an abort, a hang or a spin.

const std = @import("std");
const testing = std.testing;
const wire_protocol = @import("../wire_protocol.zig");
const ring = @import("ring.zig");
const calls = @import("../core/calls.zig");
const stream = @import("data_path.zig");
const ring_wait = @import("ring_wait.zig");
const wait = @import("../core/wait.zig");

const Endpoint = @import("../core/endpoint.zig").Endpoint;
const Hot = @import("../core/endpoint.zig").Hot;
const Requests = @import("../core/endpoint.zig").Requests;
const DataError = @import("../core/endpoint.zig").DataError;
const Ring = ring.Ring;
const first_piece: wire_protocol.FrameFlags = .{ .start = true };
const last_piece: wire_protocol.FrameFlags = .{ .end = true };
const whole_message: wire_protocol.FrameFlags = .{ .start = true, .end = true };
const middle_piece: wire_protocol.FrameFlags = .{};
const pad_frame: wire_protocol.FrameFlags = .{ .pad = true };

/// A connection's two sides over a segment in plain memory.
const Pair = struct {
    header: *wire_protocol.SegmentHeader,
    /// Both rings, and room after them for a header read off a frame's boundary (as a segment has).
    data: []align(16) u8,
    server: *Endpoint,
    client: *Endpoint,

    fn init(capacity: u32) !Pair {
        const gpa = testing.allocator;
        const header = try gpa.create(wire_protocol.SegmentHeader);
        errdefer gpa.destroy(header);
        header.* = std.mem.zeroes(wire_protocol.SegmentHeader);
        const data = try gpa.alignedAlloc(u8, .@"16", 2 * @as(usize, capacity) + 64);
        errdefer gpa.free(data);
        @memset(data, 0);
        const server = try side(header, data, capacity, true);
        errdefer gpa.destroy(server);
        return .{ .header = header, .data = data, .server = server, .client = try side(header, data, capacity, false) };
    }

    fn side(header: *wire_protocol.SegmentHeader, data: []u8, capacity: u32, listener: bool) !*Endpoint {
        const endpoint = try testing.allocator.create(Endpoint);
        endpoint.* = .{ .hot = std.mem.zeroes(Hot), .io = testing.io, .gpa = testing.allocator, .io_ref = undefined, .handles = undefined };
        endpoint.hot.stream.header.store(header, .monotonic);
        endpoint.hot.stream.data.store(data.ptr, .monotonic);
        endpoint.hot.view_capacity.store(capacity, .monotonic);
        endpoint.hot.view_server.store(listener, .monotonic);
        endpoint.hot.phase.store(.connected, .monotonic);
        endpoint.hot.view_published.store(true, .release);
        return endpoint;
    }

    fn deinit(pair: *Pair) void {
        testing.allocator.destroy(pair.client);
        testing.allocator.destroy(pair.server);
        testing.allocator.free(pair.data);
        testing.allocator.destroy(pair.header);
    }

    /// The server's send ring, which the client receives from.
    fn ring(pair: *const Pair) Ring {
        return Ring.forSend(&pair.server.hot);
    }

    fn peer(pair: *const Pair) Peer {
        return .{ .ring = pair.ring(), .hot = &pair.server.hot };
    }

    /// The client's receive into `buf`, and the message's length.
    fn recv(pair: *Pair, buf: []u8, timeout_ms: c_int) DataError!usize {
        var len: usize = undefined;
        try calls.recv(pair.client, buf, timeout_ms, &len);
        return len;
    }

    /// Moves the client's receive tail to `value`: the ring's, and its own copy (ring_wait.zig).
    fn setReaderTail(pair: *const Pair, value: u64) void {
        pair.peer().setTail(value);
        pair.client.recv.tail = value;
    }

    /// Whether the client's next message is `expected`, within `timeout_ms`.
    fn recvIs(pair: *Pair, timeout_ms: c_int, expected: []const u8) bool {
        var buf: [256]u8 = undefined;
        const len = pair.recv(&buf, timeout_ms) catch return false;
        return std.mem.eql(u8, buf[0..len], expected);
    }
};

/// A faulty sender: writes a frame's header and bytes at the head, whatever they claim, and publishes the head a frame
/// further (the bytes written, padded as a frame is).
const Peer = struct {
    ring: Ring,
    hot: *const Hot,

    fn frame(peer: Peer, flags: wire_protocol.FrameFlags, frag_len: u32, total_len: u64, payload: []const u8) void {
        const head = peer.ring.head();
        peer.ring.writeHeader(head, .{ .flags = flags, .frag_len = frag_len, .total_len = total_len });
        peer.put(head +% ring.frame_header_len, payload);
        ring_wait.publishHead(peer.hot, peer.ring, head +% ring.frameLen(payload.len));
    }

    /// A message of one piece at `pos`, stamped, as the sender writes one, without publishing the head.
    fn stamped(peer: Peer, pos: u64, payload: []const u8) void {
        peer.ring.writeUnstamped(pos, payload.len);
        peer.put(pos +% ring.frame_header_len, payload);
        peer.ring.stamp(pos);
    }

    /// A message of one piece.
    fn message(peer: Peer, payload: []const u8) void {
        const len: u32 = @intCast(payload.len);
        peer.frame(whole_message, len, len, payload);
    }

    fn put(peer: Peer, pos: u64, bytes: []const u8) void {
        for (bytes, 0..) |byte, i| peer.ring.data[@intCast((pos +% i) % peer.ring.capacity)] = byte;
    }

    fn setHead(peer: Peer, value: u64) void {
        peer.ring.indices.writer.head.store(value, .release);
    }

    fn setTail(peer: Peer, value: u64) void {
        peer.ring.indices.reader.tail.store(value, .release);
    }

    /// After `ms`, the pieces of `msg` from byte `from` on, `piece` bytes each (the last with END), one every `ms`.
    fn later(peer: Peer, msg: []const u8, from: usize, piece: usize, ms: u64) std.Thread {
        const Writer = struct {
            fn run(writer: Peer, bytes: []const u8, start: usize, n: usize, every: u64) void {
                var at = start;
                while (at < bytes.len) {
                    sleepMs(every);
                    const len = @min(n, bytes.len - at);
                    writer.frame(if (at + len == bytes.len) last_piece else middle_piece, @intCast(len), 0, bytes[at..][0..len]);
                    at += len;
                }
            }
        };
        return std.Thread.spawn(.{}, Writer.run, .{ peer, msg, from, piece, ms }) catch @panic("thread");
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
fn cancel(endpoint: *Endpoint) void {
    _ = endpoint.hot.requests.fetchOr(Requests{ .cancel = true }, .seq_cst);
    ring_wait.wakeOwnWaiters(&endpoint.hot);
}

fn peerEnds(endpoint: *Endpoint) void {
    endpoint.hot.phase.store(.peer_dead, .seq_cst);
    ring_wait.wakeOwnWaiters(&endpoint.hot);
}

/// A receive on another thread, whose result the test looks at once it has returned.
const Receive = struct {
    pair: *Pair,
    buf: []u8,
    timeout_ms: c_int,
    result: DataError!usize = undefined,
    done: std.atomic.Value(bool) = .init(false),
    thread: std.Thread = undefined,

    fn start(receive: *Receive) void {
        receive.thread = std.Thread.spawn(.{}, run, .{receive}) catch @panic("thread");
    }

    fn run(receive: *Receive) void {
        receive.result = receive.pair.recv(receive.buf, receive.timeout_ms);
        receive.done.store(true, .release);
    }
};

// === A started message completes ===

// A message of two pieces whose second piece comes after the receive's timeout: the receive waits for it and returns
// the whole message. A zero-copy receive before it is TooLarge with the message's length, and takes nothing.
test "a message whose rest arrives after the receive's timeout still arrives whole" {
    var pair: Pair = try .init(4096);
    defer pair.deinit();
    var message: [200]u8 = undefined;
    pattern(&message, 3);
    pair.peer().frame(first_piece, 100, message.len, message[0..100]);

    var data: [*]const u8 = undefined;
    var len: usize = 0;
    try testing.expectError(error.TooLarge, calls.recvAcquire(pair.client, 0, &data, &len));
    try testing.expectEqual(@as(usize, message.len), len);
    try testing.expectEqual(@as(u64, 0), pair.ring().tail()); // nothing taken

    const writer = pair.peer().later(&message, 100, 100, 150);
    defer writer.join();
    var got: [256]u8 = undefined;
    const start = nowMs();
    const n = try pair.recv(&got, 50);
    try testing.expect(nowMs() - start >= 100); // past its timeout, for the rest
    try testing.expectEqualSlices(u8, &message, got[0..n]);
    try testing.expectError(error.Timeout, pair.recv(&got, 0));
}

// The timeout bounds the wait for the first piece; once it has come, the pieces that come every 100 ms after it are
// waited for, however long they take.
test "a receive's timeout bounds its wait for the first piece only" {
    var pair: Pair = try .init(4096);
    defer pair.deinit();
    var got: [600]u8 = undefined;
    var start = nowMs();
    try testing.expectError(error.Timeout, pair.recv(&got, 150));
    try testing.expect(nowMs() - start < 1000);

    var message: [500]u8 = undefined;
    pattern(&message, 9);
    pair.peer().frame(first_piece, 100, message.len, message[0..100]);
    const writer = pair.peer().later(&message, 100, 100, 100);
    defer writer.join();
    start = nowMs();
    const n = try pair.recv(&got, 150);
    try testing.expect(nowMs() - start >= 300);
    try testing.expectEqualSlices(u8, &message, got[0..n]);
}

// A consumer that polls: a message longer than a piece whose rest arrives after the poll found its first piece is
// taken whole by that poll.
test "a poll that finds a message's first piece takes the whole message" {
    var pair: Pair = try .init(1 << 20);
    defer pair.deinit();
    var got: [256]u8 = undefined;
    try testing.expectError(error.Timeout, pair.recv(&got, 0));

    var message: [232]u8 = undefined;
    pattern(&message, 11);
    pair.peer().frame(first_piece, 132, message.len, message[0..132]);
    const writer = pair.peer().later(&message, 132, 100, 100);
    defer writer.join();
    const n = try pair.recv(&got, 0);
    try testing.expectEqualSlices(u8, &message, got[0..n]);
}

// A receive in the middle of a message whose rest never comes waits past its timeout until cancel stops it. The
// message is dropped whole: its rest, if it comes, is skipped, and a receive that needn't wait still works.
test "cancel stops a receive in the middle of a message, which is dropped whole" {
    var pair: Pair = try .init(4096);
    defer pair.deinit();
    pair.peer().frame(first_piece, 4, 8, &.{ 1, 2, 3, 4 });
    var got: [16]u8 = undefined;
    var receive: Receive = .{ .pair = &pair, .buf = &got, .timeout_ms = 50 };
    receive.start();
    sleepMs(300);
    try testing.expect(!receive.done.load(.acquire)); // past its timeout
    cancel(pair.client);
    receive.thread.join();
    try testing.expectError(error.Cancelled, receive.result);

    pair.peer().frame(last_piece, 4, 0, &.{ 5, 6, 7, 8 });
    pair.peer().message("next");
    try testing.expect(pair.recvIs(0, "next"));
    try testing.expectError(error.Cancelled, pair.recv(&got, 0)); // it would wait
}

// The peer's end stops a receive in the middle of a message: Disconnected, and the message is dropped. The messages
// the peer completed before are delivered first.
test "the peer's end stops a receive in the middle of a message: Disconnected" {
    var pair: Pair = try .init(4096);
    defer pair.deinit();
    pair.peer().message("done");
    pair.peer().frame(first_piece, 4, 8, &.{ 1, 2, 3, 4 });
    try testing.expect(pair.recvIs(0, "done"));
    var got: [16]u8 = undefined;
    var receive: Receive = .{ .pair = &pair, .buf = &got, .timeout_ms = 50 };
    receive.start();
    sleepMs(100);
    peerEnds(pair.client);
    receive.thread.join();
    try testing.expectError(error.Disconnected, receive.result);
    try testing.expectError(error.Disconnected, pair.recv(&got, 1000));
}

// A sender that gave its message up (cancel) begins its next message with a START: the receiver drops the unfinished
// one and takes the new one. Before its first piece is taken, the unfinished message is the next one, with its length.
test "a new message inside an unfinished one drops it and begins again" {
    var pair: Pair = try .init(4096);
    defer pair.deinit();
    pair.peer().frame(first_piece, 100, 10240, &@as([100]u8, @splat(0x11)));
    pair.peer().message("hello");
    var small: [64]u8 = undefined;
    try testing.expectError(error.TooLarge, pair.recv(&small, 100));
    const big = try testing.allocator.alloc(u8, 10240);
    defer testing.allocator.free(big);
    const n = try pair.recv(big, 100);
    try testing.expectEqualStrings("hello", big[0..n]);
}

// === Corrupt pieces ===

// A piece longer than what its message has left, and an END before the message's length, are corrupt: each is
// consumed, the message dropped (Invalid), and the next message arrives.
test "a piece that breaks its message is consumed, and the message dropped" {
    var pair: Pair = try .init(4096);
    defer pair.deinit();
    const peer = pair.peer();
    var got: [4096]u8 = undefined;

    peer.frame(first_piece, 16, 20, &@as([16]u8, @splat(0x22)));
    peer.frame(last_piece, 16, 0, &@as([16]u8, @splat(0x23)));
    peer.message("first");
    try testing.expectError(error.Invalid, pair.recv(&got, 100)); // 16 + 16 bytes of a 20-byte message
    try testing.expect(pair.recvIs(100, "first"));

    peer.frame(first_piece, 16, 4000, &@as([16]u8, @splat(0x24)));
    peer.frame(last_piece, 16, 0, &@as([16]u8, @splat(0x25)));
    peer.message("second");
    try testing.expectError(error.Invalid, pair.recv(&got, 100)); // an END at 32 bytes of a 4000-byte message
    try testing.expect(pair.recvIs(100, "second"));
}

// A frame that doesn't fit the ring where it lies is Invalid, at a message's start and in its middle, at once; it
// stays, since its end is unknown.
test "a frame no ring can hold is Invalid, at once and again" {
    var got: [4096]u8 = undefined;
    {
        var pair: Pair = try .init(4096);
        defer pair.deinit();
        pair.peer().frame(whole_message, 4096, 4096, &.{});
        try testing.expectError(error.Invalid, pair.recv(&got, 100));
        try testing.expectError(error.Invalid, pair.recv(&got, 0));
    }
    {
        var pair: Pair = try .init(4096);
        defer pair.deinit();
        pair.peer().frame(first_piece, 1000, 2000, &@as([1000]u8, @splat(0x33)));
        pair.peer().frame(middle_piece, 0xFFFF_FFF0, 0, &.{});
        const start = nowMs();
        try testing.expectError(error.Invalid, pair.recv(&got, 1000));
        try testing.expect(nowMs() - start < 500);
        try testing.expectError(error.Invalid, pair.recv(&got, 0));
    }
    {
        var pair: Pair = try .init(4096);
        defer pair.deinit();
        pair.peer().frame(pad_frame, std.math.maxInt(u32), 0, &.{});
        try testing.expectError(error.Invalid, pair.recv(&got, 100));
    }
}

// The rest of a message nobody receives (its sender crashed in the middle) is skipped.
test "orphaned pieces are skipped" {
    var pair: Pair = try .init(4096);
    defer pair.deinit();
    pair.peer().frame(last_piece, 4, 4, &.{ 0xDE, 0xAD, 0xBE, 0xEF });
    pair.peer().message("hello");
    try testing.expect(pair.recvIs(100, "hello"));
}

// A plain message holds at least a byte, but a faulty peer's zero-length message arrives as an empty message, and the
// one after it arrives too.
test "a zero-length message arrives empty, and the next one after it" {
    var pair: Pair = try .init(4096);
    defer pair.deinit();
    pair.peer().message("");
    pair.peer().message("after");
    var empty: [0]u8 = .{};
    try testing.expectEqual(@as(usize, 0), try pair.recv(&empty, 100));
    try testing.expect(pair.recvIs(100, "after"));
}

// The library allocates nothing by a length the peer wrote: a message claiming more than any buffer is TooLarge with
// that length, and stays.
test "a length no buffer holds is TooLarge, with that length" {
    var pair: Pair = try .init(4096);
    defer pair.deinit();
    pair.peer().frame(first_piece, 0, std.math.maxInt(u64), &.{});
    var got: [16]u8 = undefined;
    var len: usize = 0;
    try testing.expectError(error.TooLarge, calls.recv(pair.client, &got, 100, &len));
    try testing.expectEqual(@as(usize, std.math.maxInt(usize)), len);
    try testing.expectError(error.TooLarge, calls.recv(pair.client, &got, 0, &len));
}

// === The zero-copy calls' guards ===

// A copy send after a reservation, and a reservation that stops after its PAD moved the head, drop the earlier
// reservation: its commit is Invalid, so it never publishes over room it no longer owns.
test "a send or a failed reservation drops a reservation" {
    var buffer: [*]u8 = undefined;
    {
        var pair: Pair = try .init(1024);
        defer pair.deinit();
        try calls.sendAcquire(pair.server, 16, 0, &buffer);
        var bytes: [900]u8 = undefined;
        pattern(&bytes, 1);
        try calls.send(pair.server, &bytes, 0);
        try testing.expectError(error.Invalid, calls.sendCommit(pair.server, 16));
        try testing.expect(pair.ring().head() -% pair.ring().tail() <= 1024);
    }
    {
        var pair: Pair = try .init(1024);
        defer pair.deinit();
        // The head at offset 912 with 800 bytes unread: 112 bytes to the ring's end, 224 free
        var a: [84]u8 = undefined;
        var b: [784]u8 = undefined;
        pattern(&a, 2);
        pattern(&b, 3);
        try calls.send(pair.server, &a, 0);
        try calls.send(pair.server, &b, 0);
        var got: [128]u8 = undefined;
        _ = try pair.recv(&got, 100);
        try calls.sendAcquire(pair.server, 50, 0, &buffer);
        try testing.expectError(error.Timeout, calls.sendAcquire(pair.server, 184, 0, &buffer)); // a PAD, then no room
        try testing.expectError(error.Invalid, calls.sendCommit(pair.server, 50));
        try testing.expect(pair.ring().head() -% pair.ring().tail() <= 1024);
    }
}

// A second release, and a receive after an acquire, never consume more than the acquired message: the tail never
// passes the head. A receive releases an acquired message first.
test "a release consumes the acquired message once" {
    var pair: Pair = try .init(4096);
    defer pair.deinit();
    var data: [*]const u8 = undefined;
    var len: usize = 0;
    try calls.send(pair.server, "hi", 0);
    try calls.recvAcquire(pair.client, 100, &data, &len);
    calls.recvRelease(pair.client);
    calls.recvRelease(pair.client);
    try testing.expectEqual(pair.ring().head(), pair.ring().tail());
    var got: [16]u8 = undefined;
    try testing.expectError(error.Timeout, pair.recv(&got, 100));

    try calls.send(pair.server, "one", 0);
    try calls.send(pair.server, "two", 0);
    try calls.recvAcquire(pair.client, 100, &data, &len);
    try testing.expectEqualStrings("one", data[0..len]);
    try testing.expectEqual(@as(usize, 0), @intFromPtr(data) % 16); // aligned
    try testing.expect(pair.recvIs(100, "two"));
    calls.recvRelease(pair.client);
    try testing.expectEqual(pair.ring().head(), pair.ring().tail());
}

// A ring whose tail is past its head (a corrupt peer's indices) is Invalid for its writer once it loads the tail, when
// the room it saw runs out. For its reader, a tail less than a stamped frame past the head is a frame taken by its
// stamp before its head was stored (ring_wait.zig): a wait; more is Invalid. On aarch64 each side reads its own index
// from its endpoint, so a peer that moves it in the ring changes nothing that side reads; on x86-64 a head off a
// frame's boundary is Invalid for the writer.
test "a tail past the head is Invalid for the writer, and for the reader from a stamped frame past it" {
    var pair: Pair = try .init(4096);
    defer pair.deinit();
    // The writer saw the ring empty; 2080 bytes of that room are left
    try calls.send(pair.server, &@as([2000]u8, @splat('x')), 0);
    const head = pair.ring().head();
    var got: [16]u8 = undefined;
    var data: [*]const u8 = undefined;
    var len: usize = 0;
    pair.setReaderTail(head +% 64);
    try testing.expectError(error.Timeout, pair.recv(&got, 100));
    try testing.expectError(error.Timeout, calls.recvAcquire(pair.client, 0, &data, &len));
    pair.setReaderTail(head +% ring.stamped_frame_max);
    try testing.expectError(error.Invalid, pair.recv(&got, 100));
    try testing.expectError(error.Invalid, calls.recvAcquire(pair.client, 0, &data, &len));
    pair.peer().setTail(head +% 64);
    try testing.expectError(error.Invalid, calls.send(pair.server, &@as([2100]u8, @splat('x')), 100));
    var buffer: [*]u8 = undefined;
    try testing.expectError(error.Invalid, calls.sendAcquire(pair.server, 16, 0, &buffer));

    if (ring_wait.own_indices) {
        pair.peer().setTail(head);
        pair.peer().setHead(8); // the writer's own head, moved in the ring
        try calls.send(pair.server, "x", 100);
        try testing.expectEqual(head +% 32, pair.ring().head());
    } else {
        pair.peer().setTail(8);
        pair.peer().setHead(8);
        try testing.expectError(error.Invalid, calls.send(pair.server, "x", 100));
    }
}

// === A short message's stamp (ring_wait.zig) ===

// Copied and zero-copy: a message of one piece whose frame is shorter than `stamped_frame_max` carries its stamp once
// it is sent; a longer one carries its length.
test "a sender stamps a message of one piece in a frame shorter than 256 bytes, and no other" {
    var pair: Pair = try .init(4096);
    defer pair.deinit();
    var message: [1000]u8 = undefined;
    pattern(&message, 1);
    var got: [1000]u8 = undefined;
    for ([_]usize{ 1, 224, 225, 1000 }) |len| {
        const pos = pair.ring().head();
        try calls.send(pair.server, message[0..len], 0);
        try testing.expectEqual(ring.frameLen(len) < ring.stamped_frame_max, pair.ring().stamped(pos));
        try testing.expectEqual(len, try pair.recv(&got, 0));
        try testing.expectEqualSlices(u8, message[0..len], got[0..len]);
    }
    for ([_]usize{ 16, 300 }) |len| {
        const pos = pair.ring().head();
        var buffer: [*]u8 = undefined;
        try calls.sendAcquire(pair.server, len, 0, &buffer);
        @memcpy(buffer[0..len], message[0..len]);
        try calls.sendCommit(pair.server, len);
        try testing.expectEqual(len == 16, pair.ring().stamped(pos));
        try testing.expectEqual(len, try pair.recv(&got, 0));
    }
}

// A slot at the tail that already holds the tail's stamp as a wait begins is an earlier lap's bytes (or a frame that
// landed that instant): the wait doesn't take it, and waits for the head.
test "a stamp at the tail as the wait begins is not taken: the head decides" {
    var pair: Pair = try .init(4096);
    defer pair.deinit();
    pair.client.recv.poll_stamps = true; // as after a short frame
    pair.client.send.head_sent.store(32, .monotonic); // as if it had just sent: its wait is for an answer
    pair.peer().stamped(0, "stale"); // bytes that look like a stamped frame; the head stays at 0
    var got: [16]u8 = undefined;
    try testing.expectError(error.Timeout, pair.recv(&got, 50));
    try testing.expectEqual(@as(u64, 0), pair.ring().tail());
    pair.peer().message("fresh");
    try testing.expect(pair.recvIs(100, "fresh"));
}

// A frame stamped while a receive waits is taken by its stamp before its head is stored (its sender preempted between
// the two stores). The next receive finds its tail past the head: it waits, without Invalid, until the head's store and
// the next message.
test "a frame taken by its stamp ahead of its head: the next receive waits for the head" {
    var pair: Pair = try .init(4096);
    defer pair.deinit();
    pair.client.recv.poll_stamps = true;
    pair.client.send.head_sent.store(32, .monotonic); // as if it had just sent: its wait is for an answer
    var first_buf: [16]u8 = undefined;
    var first: Receive = .{ .pair = &pair, .buf = &first_buf, .timeout_ms = 2000 };
    first.start();
    sleepMs(20); // it waits
    pair.peer().stamped(0, "early");
    first.thread.join();
    try testing.expectEqualStrings("early", first_buf[0..try first.result]);
    try testing.expectEqual(@as(u64, 32), pair.ring().tail());
    try testing.expectEqual(@as(u64, 0), pair.ring().head());

    var got: [16]u8 = undefined;
    try testing.expectError(error.Timeout, pair.recv(&got, 50));
    var second: Receive = .{ .pair = &pair, .buf = &got, .timeout_ms = 2000 };
    second.start();
    sleepMs(20);
    ring_wait.publishHead(&pair.server.hot, pair.ring(), 32); // the first frame's head, at last
    pair.peer().message("late");
    second.thread.join();
    try testing.expectEqualStrings("late", got[0..try second.result]);
}

// A receiver that only receives (it hasn't sent since its last wait) waits for a stream, not an answer: it doesn't
// watch for stamps, so it leaves the line of the frame being written to the writer, and the head decides.
test "a receiver of a one-way stream doesn't watch for stamps" {
    if (wait.stream_pace == 1) return error.SkipZigTest; // an unpaced platform watches in every wait
    var pair: Pair = try .init(4096);
    defer pair.deinit();
    pair.client.recv.poll_stamps = true; // as after a short frame
    var got: [16]u8 = undefined;
    var receive: Receive = .{ .pair = &pair, .buf = &got, .timeout_ms = 100 };
    receive.start();
    sleepMs(20); // it waits
    pair.peer().stamped(0, "early"); // the head stays at 0
    receive.thread.join();
    try testing.expectError(error.Timeout, receive.result);
    try testing.expectEqual(@as(u64, 0), pair.ring().tail());
    ring_wait.publishHead(&pair.server.hot, pair.ring(), 32);
    try testing.expect(pair.recvIs(100, "early"));
}

// A poll (a receive that doesn't wait) takes a frame by its stamp only at a tail where an earlier poll found the slot
// without it (the guard across calls, ring_wait.zig): a stamp there at the first poll is an earlier lap's bytes or a
// frame that landed that instant, and the head decides.
test "a poll doesn't take a stamp that was at the tail when it first polled there" {
    var pair: Pair = try .init(4096);
    defer pair.deinit();
    pair.client.recv.poll_stamps = true; // as after a short frame
    try calls.send(pair.client, "ping", 0); // its polls are for an answer
    pair.peer().stamped(0, "stale"); // bytes that look like a stamped frame; the head stays at 0
    var got: [16]u8 = undefined;
    for (0..3) |_| try testing.expectError(error.Timeout, pair.recv(&got, 0));
    try testing.expectEqual(@as(u64, 0), pair.ring().tail());
    pair.peer().message("fresh");
    try testing.expect(pair.recvIs(0, "fresh"));
}

// A short frame the head shows turns the polls' watch on. A poll that found the slot at its tail clear takes the frame
// stamped there afterwards, before its head is stored; the next receive finds its tail past the head and waits, without
// Invalid, until the head's store and the next message.
test "a poll takes a frame by its stamp once an earlier poll found its slot clear, ahead of its head" {
    var pair: Pair = try .init(4096);
    defer pair.deinit();
    try calls.send(pair.client, "ping", 0);
    pair.peer().message("first"); // an answer: a poll that takes it notes its length
    try testing.expect(pair.recvIs(0, "first"));
    try testing.expect(pair.client.recv.poll_stamps);
    try calls.send(pair.client, "ping", 0); // its polls are for an answer
    var got: [16]u8 = undefined;
    try testing.expectError(error.Timeout, pair.recv(&got, 0)); // the slot at the tail is clear
    pair.peer().stamped(32, "early"); // the head stays at 32
    try testing.expectEqualStrings("early", got[0..try pair.recv(&got, 0)]);
    try testing.expectEqual(@as(u64, 64), pair.ring().tail());
    try testing.expectEqual(@as(u64, 32), pair.ring().head());

    try testing.expectError(error.Timeout, pair.recv(&got, 0));
    try testing.expectError(error.Timeout, pair.recv(&got, 50));
    ring_wait.publishHead(&pair.server.hot, pair.ring(), 64); // the frame's head, at last
    pair.peer().message("late");
    try testing.expect(pair.recvIs(0, "late"));
}

// A receiver that polls a one-way stream (it hasn't sent since its last message) doesn't look at its stamp slot, so it
// leaves the line of the frame being written to the writer, and the head decides.
test "a poll of a one-way stream doesn't take a frame by its stamp" {
    var pair: Pair = try .init(4096);
    defer pair.deinit();
    try calls.send(pair.client, "ping", 0);
    pair.peer().message("first"); // an answer, after which this side only receives
    try testing.expect(pair.recvIs(0, "first"));
    try testing.expect(pair.client.recv.poll_stamps);
    var got: [16]u8 = undefined;
    try testing.expectError(error.Timeout, pair.recv(&got, 0));
    try testing.expectEqual(@as(u64, 0), pair.client.recv.unstamped_tail); // the slot wasn't looked at
    pair.peer().stamped(32, "early"); // the head stays at 32
    try testing.expectError(error.Timeout, pair.recv(&got, 0));
    try testing.expectEqual(@as(u64, 32), pair.ring().tail());
    ring_wait.publishHead(&pair.server.hot, pair.ring(), 64);
    try testing.expect(pair.recvIs(0, "early"));
}

// A poll that finds nothing pauses and looks once more (ring_wait.zig `pollPaced`): one of a one-way stream that finds
// nothing then makes the next pause one hint; one for an answer (this side sent since the last frame it took) leaves
// the pace alone, so a polling round trip doesn't lose a stream's pace.
test "an empty poll resets its pause to one hint, unless it polls for an answer" {
    var pair: Pair = try .init(4096);
    defer pair.deinit();
    var got: [16]u8 = undefined;
    pair.client.recv.poll_pace = 16; // as after a steady stream
    try calls.send(pair.client, "ping", 0); // its polls are for an answer
    try testing.expectError(error.Timeout, pair.recv(&got, 0));
    try testing.expectEqual(@as(u32, 16), pair.client.recv.poll_pace);
    pair.peer().message("pong");
    try testing.expect(pair.recvIs(0, "pong"));
    try testing.expectError(error.Timeout, pair.recv(&got, 0)); // this side only receives now
    try testing.expectEqual(@as(u32, 1), pair.client.recv.poll_pace);
}

// A receiver watches for stamps after a frame short enough to carry one and stops after a longer one: messages arrive
// whole whatever their lengths' order.
test "a receiver watches for stamps after a short frame only" {
    var pair: Pair = try .init(4096);
    defer pair.deinit();
    var long: [1000]u8 = undefined;
    pattern(&long, 7);
    const Case = struct { message: []const u8, watches: bool };
    const cases = [_]Case{
        .{ .message = "short", .watches = true },
        .{ .message = &long, .watches = false },
        .{ .message = "short again", .watches = true },
        .{ .message = long[0..224], .watches = true }, // a 240-byte frame, the longest stamped one
        .{ .message = long[0..225], .watches = false }, // a 256-byte frame
        .{ .message = long[0..224], .watches = true },
        .{ .message = &long, .watches = false },
    };
    for (cases) |case| {
        var got: [1000]u8 = undefined;
        var receive: Receive = .{ .pair = &pair, .buf = &got, .timeout_ms = 2000 };
        receive.start();
        sleepMs(20); // it waits, so its spin finds the frame
        try calls.send(pair.server, case.message, 0);
        receive.thread.join();
        try testing.expectEqualSlices(u8, case.message, got[0..try receive.result]);
        try testing.expectEqual(case.watches, pair.client.recv.poll_stamps);
    }
}

// The indices only ever wrap: a pair whose indices start 16 bytes below 2^64 passes messages across the wrap, copied
// and zero-copy.
test "the ring's indices wrap around 2^64" {
    var pair: Pair = try .init(4096);
    defer pair.deinit();
    const near_the_end: u64 = std.math.maxInt(u64) - 15;
    pair.peer().setHead(near_the_end);
    pair.peer().setTail(near_the_end);
    // Each side's own index and its copy of the other's start where the indices do, as in a segment whose indices
    // start at 0
    pair.server.send.head = near_the_end;
    pair.client.recv.tail = near_the_end;
    pair.client.recv.head_seen = near_the_end;
    pair.server.send.tail_seen = near_the_end;

    var message: [100]u8 = undefined;
    pattern(&message, 5);
    try calls.send(pair.server, &message, 0);
    var got: [128]u8 = undefined;
    const n = try pair.recv(&got, 100);
    try testing.expectEqualSlices(u8, &message, got[0..n]);

    var buffer: [*]u8 = undefined;
    try calls.sendAcquire(pair.server, 16, 0, &buffer);
    @memcpy(buffer[0..16], "zero-copy, wraps");
    try calls.sendCommit(pair.server, 16);
    var data: [*]const u8 = undefined;
    var len: usize = 0;
    try calls.recvAcquire(pair.client, 100, &data, &len);
    try testing.expectEqualStrings("zero-copy, wraps", data[0..len]);
    calls.recvRelease(pair.client);
    try testing.expectEqual(pair.ring().head(), pair.ring().tail());
}

// Every frame starts at a multiple of 16 bytes, so every piece's bytes are 16-byte aligned; a frame that wouldn't fit
// before the ring's end goes after a PAD at its start.
test "frames are aligned and never wrap" {
    var pair: Pair = try .init(1024);
    defer pair.deinit();
    var got: [1024]u8 = undefined;
    for (1..200) |size| {
        var message: [199]u8 = undefined;
        pattern(message[0..size], @truncate(size));
        var buffer: [*]u8 = undefined;
        try calls.sendAcquire(pair.server, size, 0, &buffer);
        try testing.expectEqual(@as(usize, 0), @intFromPtr(buffer) % 16);
        try testing.expect(@intFromPtr(buffer) + size <= @intFromPtr(pair.ring().data) + 1024);
        @memcpy(buffer[0..size], message[0..size]);
        try calls.sendCommit(pair.server, size);
        try calls.send(pair.server, message[0..size], 0);
        for (0..2) |_| {
            const n = try pair.recv(&got, 0);
            try testing.expectEqualSlices(u8, message[0..size], got[0..n]);
        }
    }
}

// === Batches ===

/// The client's batched receive into `buf`: how many messages, their lengths in `lens`.
fn recvMany(pair: *Pair, buf: []u8, lens: []usize, timeout_ms: c_int) DataError!usize {
    var count: usize = undefined;
    try calls.recvMany(pair.client, buf, lens, timeout_ms, &count);
    return count;
}

// A batch's frames are a send's: a message of one piece in a frame shorter than `stamped_frame_max` stamped, a longer
// one with its length; the head past the last; a reader asleep woken by the batch's one swap.
test "a batch is written as sends write it, and wakes a sleeping reader" {
    var pair: Pair = try .init(4096);
    defer pair.deinit();
    const reader_sleeps = &pair.ring().indices.writer.reader_sleeps;
    reader_sleeps.store(1, .monotonic); // as if the reader slept
    var storage: [1000]u8 = undefined;
    pattern(&storage, 3);
    const messages = [_][]const u8{ storage[0..1], storage[0..224], storage[0..225], storage[0..1000] };
    var sent: usize = undefined;
    try calls.sendMany(pair.server, calls.SliceBatch.of(&messages), 0, &sent);
    try testing.expectEqual(messages.len, sent);
    try testing.expectEqual(@as(u32, 0), reader_sleeps.load(.monotonic));
    var pos: u64 = 0;
    for (messages) |message| {
        try testing.expectEqual(ring.frameLen(message.len) < ring.stamped_frame_max, pair.ring().stamped(pos));
        pos += ring.frameLen(message.len);
    }
    try testing.expectEqual(pos, pair.ring().head());
    var got: [2048]u8 = undefined;
    var lens: [8]usize = undefined;
    try testing.expectEqual(messages.len, try recvMany(&pair, &got, &lens, 0));
    var at: usize = 0;
    for (messages, lens[0..messages.len]) |message, len| {
        try testing.expectEqualSlices(u8, message, got[at..][0..len]);
        at += len;
    }
    try testing.expectEqual(pos, pair.ring().tail());
}

// After its first message a batched receive takes only whole messages of one piece, and stops, consuming nothing more,
// before anything else, which the next receive takes as its first: an orphaned piece (skipped), a message in pieces.
test "a batched receive stops before an orphan and a message in pieces, which the next receive takes" {
    var pair: Pair = try .init(4096);
    defer pair.deinit();
    const peer = pair.peer();
    peer.message("a");
    peer.message("bb");
    const orphan_at = pair.ring().head();
    peer.frame(middle_piece, 3, 0, "ccc");
    peer.message("d");
    var got: [64]u8 = undefined;
    var lens: [8]usize = undefined;
    try testing.expectEqual(2, try recvMany(&pair, &got, &lens, 0));
    try testing.expectEqualStrings("abb", got[0..3]);
    try testing.expectEqual(orphan_at, pair.ring().tail());
    try testing.expectEqual(1, try recvMany(&pair, &got, &lens, 0));
    try testing.expectEqualStrings("d", got[0..lens[0]]);

    peer.message("h");
    peer.frame(first_piece, 4, 8, "eeee");
    peer.frame(last_piece, 4, 0, "ffff");
    peer.message("g");
    try testing.expectEqual(1, try recvMany(&pair, &got, &lens, 0));
    try testing.expectEqualStrings("h", got[0..lens[0]]);
    try testing.expectEqual(2, try recvMany(&pair, &got, &lens, 0));
    try testing.expectEqualSlices(usize, &.{ 8, 1 }, lens[0..2]);
    try testing.expectEqualStrings("eeeeffffg", got[0..9]);
    try testing.expectEqual(pair.ring().head(), pair.ring().tail());
}

// A batched receive takes further messages only as far as the head it loads: a frame written and stamped past it
// waits for the next receive.
test "a batched receive takes nothing past the head" {
    var pair: Pair = try .init(4096);
    defer pair.deinit();
    pair.peer().message("a");
    const head = pair.ring().head();
    pair.peer().stamped(head, "unpublished"); // its head not stored yet
    var got: [64]u8 = undefined;
    var lens: [8]usize = undefined;
    try testing.expectEqual(1, try recvMany(&pair, &got, &lens, 0));
    try testing.expectEqualStrings("a", got[0..lens[0]]);
    try testing.expectEqual(head, pair.ring().tail());
    pair.peer().setHead(head + ring.frameLen("unpublished".len));
    try testing.expectEqual(1, try recvMany(&pair, &got, &lens, 0));
    try testing.expectEqualStrings("unpublished", got[0..lens[0]]);
}

// A receive publishes the room it frees at once, whatever it does next: a writer whose reader took the longest piece
// and then stopped gets that room again, in a send that doesn't wait. Three laps, so the later ones start with a PAD.
test "a writer gets the room its stopped reader freed, without waiting" {
    var pair: Pair = try .init(1024);
    defer pair.deinit();
    var message: [ring.maxPieceOf(1024)]u8 = undefined;
    var got: [1024]u8 = undefined;
    for (0..3) |lap| {
        pattern(&message, @intCast(lap));
        try calls.send(pair.server, &message, 0);
        try testing.expectEqualSlices(u8, &message, got[0..try pair.recv(&got, 100)]);
    }
    try testing.expectEqual(pair.ring().head(), pair.ring().tail());
    try calls.send(pair.server, &message, 0);
}

// The room's wake is inexact (ring_wait.zig): a receive that frees room takes the writer's flag when it sees it set,
// and a receive that waits for data takes it before it sleeps, whatever its looks saw, so a writer that missed a wake
// sleeps at most until its reader next waits.
test "a receive takes a waiting writer's flag when it frees room, and before it waits for data" {
    var pair: Pair = try .init(1024);
    defer pair.deinit();
    const writer_sleeps = &pair.ring().indices.reader.writer_sleeps;
    pair.peer().message("room");
    writer_sleeps.store(1, .monotonic);
    try testing.expect(pair.recvIs(0, "room"));
    try testing.expectEqual(@as(u32, 0), writer_sleeps.load(.monotonic));
    writer_sleeps.store(1, .monotonic); // as if the receive's look had missed it
    var buf: [16]u8 = undefined;
    try testing.expectError(error.Timeout, pair.recv(&buf, 0));
    try testing.expectEqual(@as(u32, 0), writer_sleeps.load(.monotonic));
}
