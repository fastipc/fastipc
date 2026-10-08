//! The native Zig API of FastIPC: the module "fastipc", a thin layer of listeners and connections (docs/protocol.md)
//! over the caller's `Io`. That `Io` must run concurrent tasks (`std.Io.Threaded` does), and the caller must not cancel
//! the library's tasks through it; errors are Zig error sets and timeouts `std.Io.Timeout`. The calls are the C API's
//! (`include/fipc.h`, c_api.zig, the shared library's root), over the same implementation: a server listens on a name
//! and accepts one client at a time; a client connects to the name. `accept` and `connect` set the connection up on the
//! calling thread, so a client and its server run on different threads (of one process or two). Their waits aren't
//! cancelation points of the `Io`: `Listener.cancel` ends an `accept`, and a timeout bounds a `connect`.

const std = @import("std");
const Io = std.Io;
const build_options = @import("build_options");
const abi = @import("abi.zig");
const control = @import("lifecycle/control.zig");
const endpoint = @import("lifecycle/endpoint.zig");
const listener = @import("lifecycle/listener.zig");
const rpc = @import("data/rpc.zig");
const stream = @import("data/stream.zig");

/// A connection's lifecycle errors: each is the `fipc_result_t` code of the same name.
pub const Error = endpoint.Error;

/// A listener's errors, as `Error`, and `AddrInUse`: another listener holds the name.
pub const ListenError = listener.Error;

/// The data calls' errors: each is the `fipc_result_t` code of the same name. A data call that the caller's `Io`
/// cancels while it waits returns Cancelled, and the cancelation stays pending for the task's next cancelation
/// point.
pub const DataError = stream.Error;

/// A server's listener: `listen`, then `accept` one client at a time.
pub const Listener = struct {
    inner: *listener.Listener,

    /// Claims `name` (`[A-Za-z0-9_.-]`, 1 to 245 characters, not starting with `.` or `-`) and listens on it, with
    /// rings of `capacity` bytes (a power of two from 1 KiB to 2 GiB). Doesn't wait. AddrInUse if another listener
    /// holds the name.
    pub fn listen(io: Io, gpa: std.mem.Allocator, name: []const u8, capacity: usize) ListenError!Listener {
        return .{ .inner = try listener.listen(.{ .caller = io }, gpa, name, capacity) };
    }

    /// Waits for a client, sets its connection up on the calling thread, and returns it. A call that times out in the
    /// middle of a client's setup keeps it for the next call, so a timeout of 0 polls. Invalid while the connection it
    /// returned last is open (one client at a time); Cancelled after `cancel`; Timeout.
    pub fn accept(l: Listener, timeout: Io.Timeout) ListenError!Conn {
        return .{ .ep = try listener.accept(l.inner, timeout) };
    }

    /// Every `accept` that waits, now or later, returns Cancelled; a client whose setup an `accept` began is dropped.
    /// Any thread.
    pub fn cancel(l: Listener) void {
        listener.cancel(l.inner);
    }

    /// Stops listening and frees the listener; the connections it accepted stay open. No other thread may be inside a
    /// call on the listener.
    pub fn close(l: Listener) void {
        listener.close(l.inner);
    }
};

/// An RPC request or response, as `Conn.rpcRecv` returns it: its header's fields and its payload.
pub const RpcMessage = struct {
    /// The request's id, which its response echoes.
    id: u64,
    kind: Kind,
    /// The application's: what the request asks for.
    opcode: u32,
    /// The application's: a response's outcome (0 in a request).
    status: i32,
    payload: []u8,

    pub const Kind = enum(u32) { request = abi.rpc_request, response = abi.rpc_response };

    fn init(header: abi.RpcMsg, payload: []u8) RpcMessage {
        // rpc.recv accepts the two kinds only
        return .{ .id = header.id, .kind = @fromBackingInt(header.kind), .opcode = header.opcode, .status = header.status, .payload = payload };
    }
};

/// One connection: two rings, one session.
pub const Conn = struct {
    ep: *endpoint.Endpoint,

    /// Connects to the server listening on `name` and sets the connection up on the calling thread, waiting up to
    /// `timeout` for the server to listen and to call `accept`; the server must run on another thread. Invalid: a bad
    /// name, or a server of another protocol version or user.
    pub fn connect(io: Io, gpa: std.mem.Allocator, name: []const u8, timeout: Io.Timeout) Error!Conn {
        return .{ .ep = try endpoint.connect(.{ .caller = io }, gpa, name, timeout) };
    }

    // One thread may send while another receives. None may overlap `close`.

    /// Sends `message` (at least 1 byte), in pieces if it is longer than one. The timeout bounds the wait for room for
    /// the first piece; a started message completes, unless `cancel` or the peer's end stops it (it is then dropped
    /// whole). Disconnected once the peer ended the session: nothing is written.
    pub fn send(c: Conn, message: []const u8, timeout: Io.Timeout) DataError!void {
        return stream.send(c.ep, &.{}, message, c.ms(timeout));
    }

    /// Receives the next message into `buf` and returns its length. TooLarge if it doesn't fit: it stays for the next
    /// receive (`recvAlloc` sizes the buffer). The timeout bounds the wait for the first piece, as for `send`. After
    /// the peer ended the session, the messages it completed are still delivered, then Disconnected.
    pub fn recv(c: Conn, buf: []u8, timeout: Io.Timeout) DataError!usize {
        var len: usize = undefined;
        try stream.recv(c.ep, buf, c.ms(timeout), &len);
        return len;
    }

    /// Receives the next message into memory from `gpa`, which the caller frees.
    pub fn recvAlloc(c: Conn, gpa: std.mem.Allocator, timeout: Io.Timeout) (DataError || std.mem.Allocator.Error)![]u8 {
        var len: usize = undefined;
        if (stream.recv(c.ep, &.{}, c.ms(timeout), &len)) |_| {
            return gpa.alloc(u8, 0); // an empty message
        } else |err| if (err != error.TooLarge) return err;
        const buf = try gpa.alloc(u8, len);
        errdefer gpa.free(buf);
        try stream.recv(c.ep, buf, 0, &len); // it is there already
        return buf;
    }

    /// Zero-copy send, step 1: room for a message of `len` bytes (one piece) in the send ring, waiting up to
    /// `timeout`; `commit` sends what was written there.
    pub fn acquire(c: Conn, len: usize, timeout: Io.Timeout) DataError![]u8 {
        var buffer: [*]u8 = undefined;
        try stream.sendAcquire(c.ep, len, c.ms(timeout), &buffer);
        return buffer[0..len];
    }

    /// Zero-copy send, step 2: sends the first `len` bytes (1 to the acquired length) of the last `acquire` as one
    /// message.
    pub fn commit(c: Conn, len: usize) DataError!void {
        return stream.sendCommit(c.ep, len);
    }

    /// Zero-copy receive, step 1: the next message in place, valid until `release`, the next receive or `close`.
    /// TooLarge for a message of several pieces (`recv` it).
    pub fn recvAcquire(c: Conn, timeout: Io.Timeout) DataError![]const u8 {
        var message: [*]const u8 = undefined;
        var len: usize = undefined;
        try stream.recvAcquire(c.ep, c.ms(timeout), &message, &len);
        return message[0..len];
    }

    /// Zero-copy receive, step 2: frees the acquired message's room.
    pub fn release(c: Conn) void {
        stream.recvRelease(c.ep);
    }

    // RPC: requests and responses, each a message with a 32-byte header in front of its payload (data/rpc.zig). Either
    // side may submit requests and respond to the requests it receives; the application pairs responses with requests
    // by id. A connection carries RPC or plain messages, not both.

    /// Sends a request, as `send` sends a message (`payload` may be empty), and returns its id: the connection's next,
    /// never 0.
    pub fn rpcSubmit(c: Conn, opcode: u32, payload: []const u8, timeout: Io.Timeout) DataError!u64 {
        var id: u64 = undefined;
        try rpc.submit(c.ep, opcode, payload, c.ms(timeout), &id);
        return id;
    }

    /// Sends the response to request `id`.
    pub fn rpcRespond(c: Conn, id: u64, opcode: u32, status: i32, payload: []const u8, timeout: Io.Timeout) DataError!void {
        return rpc.respond(c.ep, id, opcode, status, payload, c.ms(timeout));
    }

    /// Receives the next request or response, its payload into `buf`, as `recv` does. TooLarge if the payload doesn't
    /// fit: the message stays for the next receive (`rpcRecvAlloc` sizes the buffer). Invalid if the message isn't an
    /// RPC message: it is dropped.
    pub fn rpcRecv(c: Conn, buf: []u8, timeout: Io.Timeout) DataError!RpcMessage {
        var header: abi.RpcMsg = undefined;
        try rpc.recv(c.ep, buf, c.ms(timeout), &header);
        return .init(header, buf[0..header.len]);
    }

    /// Receives the next request or response, its payload into memory from `gpa`, which the caller frees.
    pub fn rpcRecvAlloc(c: Conn, gpa: std.mem.Allocator, timeout: Io.Timeout) (DataError || std.mem.Allocator.Error)!RpcMessage {
        var header: abi.RpcMsg = undefined;
        if (rpc.recv(c.ep, &.{}, c.ms(timeout), &header)) |_| {
            return .init(header, try gpa.alloc(u8, 0)); // an empty payload
        } else |err| if (err != error.TooLarge) return err;
        const buf = try gpa.alloc(u8, header.len);
        errdefer gpa.free(buf);
        try rpc.recv(c.ep, buf, 0, &header); // it is there already
        return .init(header, buf);
    }

    /// The longest message `acquire` and `recvAcquire` take: the ring's capacity less the framing's share.
    pub fn maxPiece(c: Conn) usize {
        return stream.maxPiece(c.ep);
    }

    /// From now on every call of this connection that would wait returns Cancelled; calls that needn't wait still work.
    pub fn cancel(c: Conn) void {
        endpoint.cancel(c.ep);
    }

    /// The peer ended the session (its calls report Disconnected once there's nothing left to receive).
    pub fn ended(c: Conn) bool {
        return endpoint.ended(c.ep);
    }

    /// Ends the connection and frees it; the peer's calls report Disconnected. No other thread may be inside a call on
    /// the connection.
    pub fn close(c: Conn) void {
        endpoint.close(c.ep);
    }

    /// An `Io.Timeout` as the data path's milliseconds (0: don't wait; negative: no timeout), rounded up to the next
    /// millisecond.
    fn ms(c: Conn, timeout: Io.Timeout) c_int {
        const left = timeout.toDurationFromNow(c.ep.io) orelse return -1;
        if (left.raw.nanoseconds <= 0) return 0;
        const max_ns = std.math.maxInt(c_int) * std.time.ns_per_ms;
        return @intCast(@divCeil(@min(left.raw.nanoseconds, max_ns), std.time.ns_per_ms));
    }
};

/// The tests' hooks, in the tests' build of the module only.
pub const test_hooks = if (build_options.test_hooks) control.test_hooks else struct {};

/// Internals the fuzz harness (tests/fuzz) needs to play a corrupt peer against a library connection: the ring layout,
/// the name derivation, the control frames, the segment attach and the OS rendezvous. test_hooks builds only, so the
/// shipped library never exposes them.
pub const internal = if (build_options.test_hooks) struct {
    pub const abi = @import("abi.zig");
    pub const names = @import("lifecycle/names.zig");
    pub const wire = @import("lifecycle/wire.zig");
    pub const segment = @import("session/segment.zig");
    pub const platform = @import("platform.zig");
} else struct {};

test {
    // No export calls the native API: the test build analyzes all of it, so it compiles on each OS the tests run on
    std.testing.refAllDecls(@This());
    std.testing.refAllDecls(Listener);
    std.testing.refAllDecls(Conn);
}

/// Connects to `name` on a thread of its own, while the caller accepts: `connect` returns once the server's `accept`
/// has offered the rings.
const Connector = struct {
    thread: std.Thread,
    result: Error!Conn = error.Timeout,

    fn start(c: *Connector, io: Io, name: []const u8, timeout: Io.Timeout) !void {
        c.thread = try std.Thread.spawn(.{}, run, .{ c, io, name, timeout });
    }

    fn run(c: *Connector, io: Io, name: []const u8, timeout: Io.Timeout) void {
        c.result = Conn.connect(io, std.testing.allocator, name, timeout);
    }

    fn join(c: *Connector) Error!Conn {
        c.thread.join();
        return c.result;
    }
};

test "a listener and a client in one process: messages both ways, in pieces, zero-copy, and the timeouts as Io.Timeout" {
    const testing = std.testing;
    var threaded: Io.Threaded = .init_single_threaded;
    threaded.allocator = std.heap.c_allocator;
    threaded.concurrent_limit = .unlimited;
    threaded.async_limit = .unlimited;
    defer @import("platform.zig").shutDownIo(&threaded); // its workers gone, as the tests after it count handles
    const io = threaded.io();
    const pid = @import("platform.zig").pid();
    var name_buf: [64]u8 = undefined;
    const name = try std.mem.print(&name_buf, "t-native-data-{d}", .{pid});
    const five_s: Io.Timeout = .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } };

    const l = try Listener.listen(io, testing.allocator, name, 4096);
    defer l.close();
    try testing.expectError(error.AddrInUse, Listener.listen(io, testing.allocator, name, 4096));
    var connector: Connector = undefined;
    try connector.start(io, name, five_s);
    const accepted = l.accept(five_s);
    const b = try connector.join();
    defer b.close();
    const a = try accepted;
    defer a.close();
    // One client at a time
    try testing.expectError(error.Invalid, l.accept(.none));

    var buf: [16]u8 = undefined;
    try a.send("hello", .none);
    try testing.expectEqualStrings("hello", buf[0..try b.recv(&buf, .none)]);
    try b.send("olleh", .none);
    try testing.expectError(error.TooLarge, a.recv(buf[0..2], .none)); // it stays
    try testing.expectEqualStrings("olleh", buf[0..try a.recv(&buf, .none)]);
    // A message longer than the ring goes in pieces, which a receiver on another thread takes in turn
    const long = try testing.allocator.alloc(u8, 10_000);
    defer testing.allocator.free(long);
    for (long, 0..) |*byte, i| byte.* = @truncate(i);
    const Receiver = struct {
        fn run(conn: Conn, expected: []const u8, ok: *bool) void {
            const got = conn.recvAlloc(testing.allocator, .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } }) catch return;
            defer testing.allocator.free(got);
            ok.* = std.mem.eql(u8, got, expected);
        }
    };
    var received = false;
    const receiver = try std.Thread.spawn(.{}, Receiver.run, .{ b, long, &received });
    const sent = a.send(long, five_s);
    receiver.join();
    try sent;
    try testing.expect(received);

    const buffer = try a.acquire(5, .none);
    @memcpy(buffer, "zc-ok");
    try a.commit(5);
    try testing.expectEqualStrings("zc-ok", try b.recvAcquire(.none));
    b.release();

    // Nothing more to receive: a zero duration looks once, a positive one waits, a deadline too
    try testing.expectError(error.Timeout, b.recv(&buf, .{ .duration = .{ .raw = .zero, .clock = .awake } }));
    try testing.expectError(error.Timeout, b.recv(&buf, .{ .duration = .{ .raw = .fromMilliseconds(20), .clock = .awake } }));
    const deadline = Io.Clock.Timestamp.fromNow(io, .{ .raw = .fromMilliseconds(20), .clock = .awake });
    try testing.expectError(error.Timeout, b.recvAcquire(.{ .deadline = deadline }));
}

test "RPC through the module: a request and its response, a payload too large for the buffer, a plain message" {
    const testing = std.testing;
    var threaded: Io.Threaded = .init_single_threaded;
    threaded.allocator = std.heap.c_allocator;
    threaded.concurrent_limit = .unlimited;
    threaded.async_limit = .unlimited;
    defer @import("platform.zig").shutDownIo(&threaded); // its workers gone, as the tests after it count handles
    const io = threaded.io();
    var name_buf: [64]u8 = undefined;
    const name = try std.mem.print(&name_buf, "t-native-rpc-{d}", .{@import("platform.zig").pid()});
    const five_s: Io.Timeout = .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } };

    const l = try Listener.listen(io, testing.allocator, name, 4096);
    defer l.close();
    var connector: Connector = undefined;
    try connector.start(io, name, five_s);
    const accepted = l.accept(five_s);
    const client = try connector.join();
    defer client.close();
    const server = try accepted;
    defer server.close();

    var buf: [16]u8 = undefined;
    const id = try client.rpcSubmit(1, "ping", .none);
    try testing.expect(id != 0);
    const request = try server.rpcRecv(&buf, .none);
    try testing.expectEqual(RpcMessage.Kind.request, request.kind);
    try testing.expectEqual(id, request.id);
    try testing.expectEqual(@as(u32, 1), request.opcode);
    try testing.expectEqualStrings("ping", request.payload);
    try server.rpcRespond(request.id, request.opcode, -7, "PING", .none);
    const response = try client.rpcRecv(&buf, .none);
    try testing.expectEqual(RpcMessage.Kind.response, response.kind);
    try testing.expectEqual(id, response.id);
    try testing.expectEqual(@as(i32, -7), response.status);
    try testing.expectEqualStrings("PING", response.payload);

    // A payload longer than the buffer stays queued; rpcRecvAlloc takes it at its length
    const second = try client.rpcSubmit(2, "a payload of 27 bytes total", .none);
    try testing.expect(second > id);
    try testing.expectError(error.TooLarge, server.rpcRecv(buf[0..4], .none));
    const large = try server.rpcRecvAlloc(testing.allocator, .none);
    defer testing.allocator.free(large.payload);
    try testing.expectEqual(second, large.id);
    try testing.expectEqualStrings("a payload of 27 bytes total", large.payload);
    // An empty payload too
    _ = try client.rpcSubmit(3, "", .none);
    const empty = try server.rpcRecvAlloc(testing.allocator, .none);
    defer testing.allocator.free(empty.payload);
    try testing.expectEqual(@as(usize, 0), empty.payload.len);

    // A plain message isn't an RPC message: dropped, and the next message is received
    try client.send("plain", .none);
    try testing.expectError(error.Invalid, server.rpcRecv(&buf, .none));
    _ = try client.rpcSubmit(4, "next", .none);
    try testing.expectEqualStrings("next", (try server.rpcRecv(&buf, .none)).payload);
    try testing.expectError(error.Timeout, server.rpcRecv(&buf, .{ .duration = .{ .raw = .zero, .clock = .awake } }));
}
