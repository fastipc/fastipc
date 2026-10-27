//! The test peer: the other process of the multi-process tests in tests/zig. Each scenario plays one role against the
//! test: a server that listens (the test connects), a client that connects (the test listens), a broadcast's
//! publisher or subscriber, a work queue's owner, producer or consumer, a service's owner, client or server, through
//! the C API, or through the native API for native_peer_death_test.zig and the broadcasts', the queues' and the
//! services' crash tests.
//!
//! Usage: `test_peer <scenario> <args...>`. A scenario reports progress as tokens on its stdout ("L" once it listens,
//! "A" once it accepted, "C" once it connected, ...), reads a byte from its stdin where it waits for the test, and ends
//! with its exit code; `exit` leaves without closing anything, as a crash would. The peer exits with 97 at once when its
//! stdin closes, which means the test process died.

const std = @import("std");
const builtin = @import("builtin");
const c = @import("c");
const fastipc = @import("fastipc");
const stamp = @import("stamp.zig");

const windows = builtin.os.tag == .windows;

var io: std.Io = undefined;
var stdin_event: std.Io.Event = .unset;

const Args = []const [:0]const u8;

const Scenario = struct { name: []const u8, run: *const fn (Args) noreturn, args: usize };

const scenarios = [_]Scenario{
    // A server: listens, and accepts one client
    .{ .name = "listen-hold", .run = listenHold, .args = 1 },
    .{ .name = "listen-exit", .run = listenExit, .args = 1 },
    .{ .name = "listen-close", .run = listenClose, .args = 1 },
    .{ .name = "serve", .run = serve, .args = 3 },
    .{ .name = "echo", .run = echo, .args = 1 },
    .{ .name = "partial-write", .run = partialWrite, .args = 1 },
    // A client: connects to the test's listener
    .{ .name = "connect-hold", .run = connectHold, .args = 1 },
    .{ .name = "connect-check", .run = connectCheck, .args = 1 },
    .{ .name = "connect-forever", .run = connectForever, .args = 1 },
    .{ .name = "send-exit", .run = sendExit, .args = 2 },
    .{ .name = "send-fresh", .run = sendFresh, .args = 1 },
    .{ .name = "send-long", .run = sendLong, .args = 1 },
    .{ .name = "send-batches", .run = sendBatches, .args = 3 },
    // Both, in this process (the same-process warning)
    .{ .name = "self-pair", .run = selfPair, .args = 1 },
    // Through the native API (native_peer_death_test.zig)
    .{ .name = "native-pair", .run = nativePair, .args = 2 },
    .{ .name = "native-crash", .run = nativeCrash, .args = 3 },
    // Broadcasts: a subscriber, a publisher (broadcast/)
    .{ .name = "broadcast-sub", .run = broadcastSub, .args = 3 },
    .{ .name = "broadcast-hold", .run = broadcastHold, .args = 2 },
    .{ .name = "broadcast-stall", .run = broadcastStall, .args = 2 },
    .{ .name = "broadcast-pub", .run = broadcastPub, .args = 3 },
    .{ .name = "native-broadcast-crash", .run = nativeBroadcastCrash, .args = 3 },
    // Work queues: a producer, a consumer, an owner (queue/)
    .{ .name = "queue-produce", .run = queueProduce, .args = 3 },
    .{ .name = "queue-produce-batches", .run = queueProduceBatches, .args = 4 },
    .{ .name = "queue-hold", .run = queueHold, .args = 2 },
    .{ .name = "queue-own", .run = queueOwn, .args = 2 },
    .{ .name = "native-queue-crash", .run = nativeQueueCrash, .args = 3 },
    // Services: a client, a server, an owner (service/)
    .{ .name = "service-ask", .run = serviceAsk, .args = 4 },
    .{ .name = "service-hold", .run = serviceHold, .args = 2 },
    .{ .name = "service-own", .run = serviceOwn, .args = 1 },
    .{ .name = "native-service-crash", .run = nativeServiceCrash, .args = 3 },
};

pub fn main(init: std.process.Init) void {
    io = init.io;
    const args = init.minimal.args.toSlice(init.arena.allocator()) catch exit(90);
    if (args.len < 2) usage();
    _ = std.Thread.spawn(.{}, watchStdin, .{}) catch exit(91);
    for (scenarios) |s| {
        if (std.mem.eql(u8, args[1], s.name)) {
            if (args.len - 2 != s.args) usage();
            s.run(args[2..]);
        }
    }
    usage();
}

fn usage() noreturn {
    std.debug.print("test_peer: usage: test_peer <scenario> <args...>; scenarios:", .{});
    for (scenarios) |s| std.debug.print(" {s}", .{s.name});
    std.debug.print("\n", .{});
    exit(89);
}

/// Reads stdin for the test's commands; its end means the test process is gone.
fn watchStdin() void {
    var buf: [64]u8 = undefined;
    while (true) {
        _ = std.Io.File.stdin().readStreaming(io, &.{&buf}) catch break;
        stdin_event.set(io);
    }
    exit(97);
}

// --- Steps ---

/// Leaves at once: `_exit` on POSIX (nothing is closed, as in a crash), `ExitProcess` on Windows.
fn exit(code: u8) noreturn {
    if (windows) std.process.exit(code) else std.c._exit(code);
}

/// A token on stdout, which the test reads (`Peer.waitFor`).
fn signal(comptime token: []const u8) void {
    std.Io.File.stdout().writeStreamingAll(io, token ++ "\n") catch {};
}

/// Blocks until the test wrote a byte to the peer's stdin.
fn waitForTest() void {
    stdin_event.waitUncancelable(io);
}

fn sleepMs(ms: i64) void {
    io.sleep(.fromMilliseconds(ms), .awake) catch {};
}

fn int(arg: []const u8) u32 {
    return std.fmt.parseInt(u32, arg, 10) catch exit(88);
}

/// Listens on `name` with rings of 4 KiB (exit 1 if it can't) and says "L".
fn listen(name: [:0]const u8) ?*c.fipc_listener_t {
    return listenWith(name, 4096);
}

fn listenWith(name: [:0]const u8, capacity: usize) ?*c.fipc_listener_t {
    var listener: ?*c.fipc_listener_t = null;
    if (c.fipc_stream_create(name, capacity, &listener) != c.FIPC_OK) exit(1);
    signal("L");
    return listener;
}

/// Accepts one client, without a timeout (exit 2 if it fails), and says "A".
fn accept(listener: ?*c.fipc_listener_t) ?*c.fipc_conn_t {
    var conn: ?*c.fipc_conn_t = null;
    if (c.fipc_stream_accept(listener, &conn, -1) != c.FIPC_OK) exit(2);
    signal("A");
    return conn;
}

/// Connects to `name` within 10 s (exit 2 if it can't) and says "C".
fn connect(name: [:0]const u8) ?*c.fipc_conn_t {
    var conn: ?*c.fipc_conn_t = null;
    if (c.fipc_stream_connect(name, &conn, 10000) != c.FIPC_OK) exit(2);
    signal("C");
    return conn;
}

/// Holds until the test's byte (then closes everything and exits 0) or until it is killed.
fn holdThenClose(conn: ?*c.fipc_conn_t, listener: ?*c.fipc_listener_t) noreturn {
    waitForTest();
    c.fipc_close(conn);
    c.fipc_listener_close(listener);
    exit(0);
}

// --- Servers ---

/// `<name>`: listens ("L"), accepts a client ("A"), and holds the connection until the test's byte or its death.
fn listenHold(args: Args) noreturn {
    const listener = listen(args[0]);
    const conn = accept(listener);
    holdThenClose(conn, listener);
}

/// `<name>`: listens ("L") and exits without closing anything: a server that dies before it accepts.
fn listenExit(args: Args) noreturn {
    _ = listen(args[0]);
    exit(0);
}

/// `<name>`: listens ("L"), closes the listener and exits 0.
fn listenClose(args: Args) noreturn {
    const listener = listen(args[0]);
    c.fipc_listener_close(listener);
    exit(0);
}

/// `<name> <count> <then>`: listens, accepts ("A"), and answers up to `count` messages (each waited for 5 s) with a
/// 1-byte message; then `hold` holds the connection until killed, and `close` closes everything and exits 0, or 3 if
/// no message came.
fn serve(args: Args) noreturn {
    const listener = listen(args[0]);
    const conn = accept(listener);
    const count = int(args[1]);
    var served: u32 = 0;
    var buf: [4096]u8 = undefined;
    while (served < count) : (served += 1) {
        var len: usize = 0;
        if (c.fipc_recv(conn, &buf, buf.len, &len, 5000) != c.FIPC_OK) break;
        if (c.fipc_send(conn, "k", 1, 5000) != c.FIPC_OK) break;
    }
    if (std.mem.eql(u8, args[2], "hold")) {
        sleepMs(120000); // until killed
        exit(0);
    }
    c.fipc_close(conn);
    c.fipc_listener_close(listener);
    exit(if (served > 0) 0 else 3);
}

/// `<name>`: listens with rings of 1 MiB, accepts ("A"), and sends each message back until the connection ends; then
/// closes and exits 0.
fn echo(args: Args) noreturn {
    const listener = listenWith(args[0], 1 << 20);
    const conn = accept(listener);
    const buf = std.heap.c_allocator.alloc(u8, 1 << 20) catch exit(4);
    while (true) {
        var len: usize = 0;
        if (c.fipc_recv(conn, buf.ptr, buf.len, &len, -1) != c.FIPC_OK) break;
        if (c.fipc_send(conn, buf.ptr, len, -1) != c.FIPC_OK) break;
    }
    c.fipc_close(conn);
    c.fipc_listener_close(listener);
    exit(0);
}

/// `<name>`: listens, accepts ("A"), reserves 128 bytes of its send ring, fills them, and dies before the commit.
fn partialWrite(args: Args) noreturn {
    const listener = listen(args[0]);
    const conn = accept(listener);
    var buf: ?*anyopaque = null;
    if (c.fipc_send_acquire(conn, 128, &buf, 0) == c.FIPC_OK) @memset(@as([*]u8, @ptrCast(buf.?))[0..128], 0xEE);
    exit(0); // without the commit
}

// --- Clients ---

/// `<name>`: connects ("C") and holds the connection until the test's byte or its death.
fn connectHold(args: Args) noreturn {
    const conn = connect(args[0]);
    holdThenClose(conn, null);
}

/// `<name>`: connects, sends "ping", expects "pong" back ("S" if it came), then holds until the test's byte; exits 0,
/// or 3 if the exchange failed.
fn connectCheck(args: Args) noreturn {
    const conn = connect(args[0]);
    var buf: [16]u8 = undefined;
    var len: usize = 0;
    const ok = c.fipc_send(conn, "ping", 4, 5000) == c.FIPC_OK and
        c.fipc_recv(conn, &buf, buf.len, &len, 5000) == c.FIPC_OK and std.mem.eql(u8, buf[0..len], "pong");
    if (ok) signal("S");
    waitForTest();
    c.fipc_close(conn);
    exit(if (ok) 0 else 3);
}

/// `<name>`: connects without a timeout (the test listens later), says "C", sends "ping", then holds until the test's
/// byte; exits 0, or 2 if the connect failed and 3 if the send did.
fn connectForever(args: Args) noreturn {
    var conn: ?*c.fipc_conn_t = null;
    if (c.fipc_stream_connect(args[0], &conn, c.FIPC_FOREVER) != c.FIPC_OK) exit(2);
    signal("C");
    if (c.fipc_send(conn, "ping", 4, 5000) != c.FIPC_OK) exit(3);
    waitForTest();
    c.fipc_close(conn);
    exit(0);
}

/// `<name> <count> <batch>`: connects ("C"), sends `count` checked messages (stamp.zig, up to 6000 bytes, so that some
/// go in pieces through rings of 4 KiB) with `fipc_send_many`, `batch` at a time, says "S", then holds until the test's
/// byte; exits 0, or 3 if a batch failed.
fn sendBatches(args: Args) noreturn {
    const conn = connect(args[0]);
    const count = int(args[1]);
    const batch = int(args[2]);
    if (batch == 0 or batch > 64) exit(87);
    const max_len = 6000;
    const storage = std.heap.page_allocator.alloc(u8, batch * max_len) catch exit(89);
    var datas: [64]?*const anyopaque = undefined;
    var lens: [64]usize = undefined;
    var next: u32 = 0;
    while (next < count) {
        const n = @min(batch, count - next);
        for (0..n) |i| {
            const seq = next + i;
            const len = stamp.sizeOf(seq, max_len);
            const message = storage[i * max_len ..][0..len];
            stamp.fill(message, seq);
            datas[i] = message.ptr;
            lens[i] = len;
        }
        var sent: usize = 0;
        const result = c.fipc_send_many(conn, &datas, &lens, n, &sent, 10000);
        if (result != c.FIPC_OK or sent != n) {
            std.debug.print("send-batches: {s}, {d} of {d} sent at message {d}\n", .{ c.fipc_result_str(result), sent, n, next });
            exit(3);
        }
        next += n;
    }
    signal("S");
    holdThenClose(conn, null);
}

/// `<name> <count>`: connects ("C"), sends `count` messages of 16 bytes (message i filled with the byte i), says "S"
/// and exits without closing anything. Exit 3: a send failed.
fn sendExit(args: Args) noreturn {
    const conn = connect(args[0]);
    var msg: [16]u8 = undefined;
    for (0..int(args[1])) |i| {
        @memset(&msg, @truncate(i));
        if (c.fipc_send(conn, &msg, msg.len, 5000) != c.FIPC_OK) exit(3);
    }
    signal("S");
    exit(0);
}

/// `<name>`: connects ("C"), sends 5 messages of 16 bytes (message i filled with 0xAA + i), waits for the test's byte
/// (it received them), then closes and exits 0. Exit 3: a send failed.
fn sendFresh(args: Args) noreturn {
    const conn = connect(args[0]);
    var msg: [16]u8 = undefined;
    for (0..5) |i| {
        @memset(&msg, 0xAA + @as(u8, @intCast(i)));
        if (c.fipc_send(conn, &msg, msg.len, 5000) != c.FIPC_OK) exit(3);
    }
    waitForTest();
    c.fipc_close(conn);
    exit(0);
}

/// `<name>`: connects ("C") and sends a message of 16 KiB, four pieces of the test's 4 KiB ring, waiting as long as it
/// takes: the test receives none of it and kills this process while the send waits for room for the second piece.
fn sendLong(args: Args) noreturn {
    const conn = connect(args[0]);
    var msg: [16384]u8 = undefined;
    @memset(&msg, 0x4C);
    _ = c.fipc_send(conn, &msg, msg.len, -1);
    exit(0);
}

// --- Both ---

/// `<name>`: a listener and a client of this process pair on the name (the client on a thread of its own: `connect`
/// waits for `accept`), with the library's log (stderr) sent to stdout, where the test reads the same-process warning;
/// then "DONE". Exit 1: the listen failed, 2: no connection.
fn selfPair(args: Args) noreturn {
    if (windows) {
        _ = SetStdHandle(STD_ERROR_HANDLE, GetStdHandle(STD_OUTPUT_HANDLE) orelse exit(3));
    } else {
        if (std.c.dup2(1, 2) == -1) exit(3);
    }
    var listener: ?*c.fipc_listener_t = null;
    if (c.fipc_stream_create(args[0], 4096, &listener) != c.FIPC_OK) exit(1);
    const Client = struct {
        fn run(name: [:0]const u8, conn: *?*c.fipc_conn_t, result: *c.fipc_result_t) void {
            result.* = c.fipc_stream_connect(name, conn, 5000);
        }
    };
    var client: ?*c.fipc_conn_t = null;
    var connect_result: c.fipc_result_t = c.FIPC_TIMEOUT;
    const thread = std.Thread.spawn(.{}, Client.run, .{ args[0], &client, &connect_result }) catch exit(4);
    var server: ?*c.fipc_conn_t = null;
    const accept_result = c.fipc_stream_accept(listener, &server, 5000);
    thread.join();
    const connected = connect_result == c.FIPC_OK and accept_result == c.FIPC_OK;
    c.fipc_close(client);
    c.fipc_close(server);
    c.fipc_listener_close(listener);
    if (!connected) exit(2);
    signal("DONE");
    exit(0);
}

const STD_OUTPUT_HANDLE: u32 = @bitCast(@as(i32, -11));
const STD_ERROR_HANDLE: u32 = @bitCast(@as(i32, -12));
extern "kernel32" fn GetStdHandle(which: u32) callconv(.winapi) ?std.os.windows.HANDLE;
extern "kernel32" fn SetStdHandle(which: u32, handle: std.os.windows.HANDLE) callconv(.winapi) c_int;

// --- Through the native API: the other process of native_peer_death_test.zig ---

const ten_s: std.Io.Timeout = .{ .duration = .{ .raw = .fromSeconds(10), .clock = .awake } };

/// The peer's connection: as `listen`, a listener that accepts one client (the listener stays open); as `connect`,
/// a client. Prints "OPEN" once it listens, or before it connects.
fn nativeOpen(role: []const u8, name: []const u8, timeout: std.Io.Timeout) fastipc.Conn {
    if (std.mem.eql(u8, role, "listen")) {
        const listener = fastipc.streamCreate(std.heap.c_allocator, name, 4096) catch exit(1);
        signal("OPEN");
        return listener.accept(timeout) catch exit(3);
    }
    if (!std.mem.eql(u8, role, "connect")) exit(5);
    signal("OPEN");
    return fastipc.streamConnect(std.heap.c_allocator, name, timeout) catch exit(3);
}

/// Receives until the session ends, then prints DEAD; returns at once on a cancel.
fn printDead(conn: fastipc.Conn) void {
    var buf: [64]u8 = undefined;
    while (true) {
        _ = conn.recv(&buf, .none) catch |err| {
            if (err == error.Disconnected) signal("DEAD");
            return;
        };
    }
}

/// `<listen|connect> <name>`: a connection with the test's: sends it a byte (its first receive returns at once) and
/// prints READY once connected, and DEAD when the test's side ends the session (a receiving thread sees it); closes and
/// exits 0 on the test's byte. Exit 6: the send failed.
fn nativePair(args: Args) noreturn {
    const conn = nativeOpen(args[0], args[1], ten_s);
    conn.send("h", .none) catch exit(6);
    const receiver = std.Thread.spawn(.{}, printDead, .{conn}) catch exit(2);
    signal("READY");
    waitForTest();
    conn.cancel();
    receiver.join();
    conn.close();
    exit(0);
}

/// `<listen|connect> <name> <step>`: a connection whose process ends as a crash would (exit code 99) when its handshake
/// or its watcher reaches `step` (fastipc.test_hooks.crash_at). Past it, it prints READY once connected, and waits for
/// the test's byte.
fn nativeCrash(args: Args) noreturn {
    fastipc.test_hooks.crash_at = std.meta.stringToEnum(fastipc.test_hooks.Step, args[2]) orelse exit(4);
    const conn = nativeOpen(args[0], args[1], .none);
    signal("READY");
    waitForTest();
    conn.close();
    exit(0);
}

// --- Broadcasts (broadcast/) ---

/// FIPC_SUBSCRIBER_LOSSY for "lossy", FIPC_SUBSCRIBER_GATED for "gated" and "gated-inplace" (which receives in place).
fn roleOf(arg: []const u8) c_int {
    if (std.mem.eql(u8, arg, "lossy")) return c.FIPC_SUBSCRIBER_LOSSY;
    if (std.mem.eql(u8, arg, "gated") or std.mem.eql(u8, arg, "gated-inplace")) return c.FIPC_SUBSCRIBER_GATED;
    exit(87);
}

/// Joins the broadcast (a subscriber's role), the queue (a producer's or a consumer's) or the service (a client's or a
/// server's) on `name` as `role` within 10 s (exit 2 if it can't) and says "J".
fn join(name: [:0]const u8, role: c_int) ?*c.fipc_conn_t {
    var conn: ?*c.fipc_conn_t = null;
    const joined = switch (role) {
        c.FIPC_PRODUCER, c.FIPC_CONSUMER => c.fipc_queue_join(name, role, &conn, 10000),
        c.FIPC_CLIENT, c.FIPC_SERVER => c.fipc_service_join(name, role, &conn, 10000),
        else => c.fipc_broadcast_join(name, role, &conn, 10000),
    };
    if (joined != c.FIPC_OK) exit(2);
    signal("J");
    return conn;
}

/// `<name> <lossy|gated|gated-inplace> <count>`: joins ("J") and receives the publisher's stream of `count` checked
/// messages (stamp.zig), copied or in place, until DISCONNECTED; says "OK" and exits 0 if it came whole (every message
/// of it for a gated subscriber, or one that never overran), 3 if not, 4 if a receive waited 10 s for nothing.
fn broadcastSub(args: Args) noreturn {
    const conn = join(args[0], roleOf(args[1]));
    const in_place = std.mem.eql(u8, args[1], "gated-inplace");
    const count = int(args[2]);
    var tally: stamp.Tally = .{};
    var buf: [65536]u8 = undefined;
    while (true) {
        var len: usize = 0;
        var at: ?*const anyopaque = null;
        const got = if (in_place) c.fipc_recv_acquire(conn, &at, &len, 10000) else c.fipc_recv(conn, &buf, buf.len, &len, 10000);
        switch (got) {
            c.FIPC_OK => {
                const msg: []const u8 = if (in_place) @as([*]const u8, @ptrCast(at.?))[0..len] else buf[0..len];
                tally.message(stamp.check(msg));
                if (in_place) c.fipc_recv_release(conn);
            },
            c.FIPC_OVERRUN => tally.overrun(),
            c.FIPC_DISCONNECTED => break,
            else => |result| {
                std.debug.print("broadcast-sub: {s} after {d} messages\n", .{ c.fipc_result_str(result), tally.received });
                exit(4);
            },
        }
    }
    c.fipc_close(conn);
    const gated_ok = roleOf(args[1]) != c.FIPC_SUBSCRIBER_GATED or (tally.overruns == 0 and tally.received == count);
    if (!tally.complete(count) or !gated_ok) {
        std.debug.print("broadcast-sub {s}: {d} received, {d} overruns, {d} bad, last {?d} of {d}\n", .{ args[1], tally.received, tally.overruns, tally.bad, tally.last, count });
        exit(3);
    }
    signal("OK");
    exit(0);
}

/// `<name> <lossy|gated>`: joins ("J") and holds the subscriber, receiving nothing, until killed.
fn broadcastHold(args: Args) noreturn {
    _ = join(args[0], roleOf(args[1]));
    sleepMs(120000);
    exit(0);
}

/// `<name> <copy|inplace|touch>`: joins as a gated subscriber ("J"), receives one message, copied or held in place
/// ("R"), then receives nothing more until the test's byte, by which time the publisher should have unsubscribed it.
/// copy, inplace: the release is nothing, and every receive is FIPC_UNSUBSCRIBED; then it closes, says "U" and exits 0
/// (3 if a call returned anything else). touch: says "TOUCH" and reads the message it still holds, which must fault:
/// "SURVIVED" and exit 0 only if it didn't. Exit 4: the first receive failed.
fn broadcastStall(args: Args) noreturn {
    const conn = join(args[0], c.FIPC_SUBSCRIBER_GATED);
    const mode = args[1];
    var buf: [4096]u8 = undefined;
    var len: usize = 0;
    var at: ?*const anyopaque = null;
    const first = if (std.mem.eql(u8, mode, "copy"))
        c.fipc_recv(conn, &buf, buf.len, &len, 10000)
    else
        c.fipc_recv_acquire(conn, &at, &len, 10000);
    if (first != c.FIPC_OK) exit(4);
    signal("R");
    waitForTest();
    if (std.mem.eql(u8, mode, "touch")) {
        signal("TOUCH");
        // POSIX: restore the default SIGSEGV action, so the fault ends the process at once. std's handler would load
        // debug info for a stack trace while the library's threads still run, a race ThreadSanitizer reports
        if (!windows) std.debug.updateSegfaultHandler(&.{
            .handler = .{ .handler = std.posix.SIG.DFL },
            .mask = std.posix.sigemptyset(),
            .flags = 0,
        });
        const held: *const volatile u8 = @ptrCast(at.?);
        _ = held.*;
        signal("SURVIVED");
        exit(0);
    }
    c.fipc_recv_release(conn);
    for (0..2) |_| {
        const result = c.fipc_recv(conn, &buf, buf.len, &len, 1000);
        const rc_acquire = c.fipc_recv_acquire(conn, &at, &len, 1000);
        if (result != c.FIPC_UNSUBSCRIBED or rc_acquire != c.FIPC_UNSUBSCRIBED) {
            std.debug.print("broadcast-stall: {s}, {s}\n", .{ c.fipc_result_str(result), c.fipc_result_str(rc_acquire) });
            exit(3);
        }
    }
    c.fipc_close(conn);
    signal("U");
    exit(0);
}

/// `<name> <subscribers> <count>`: claims a broadcast of 1 MiB for `subscribers` subscribers ("L"), waits until they
/// are joined ("ALL"), waits for the test's byte, sends `count` checked messages (stamp.zig, up to 1 KiB) and says
/// "SENT"; then holds until killed. Exit 1: the broadcast; 2: the subscribers didn't join; 3: a send.
fn broadcastPub(args: Args) noreturn {
    const subscribers = int(args[1]);
    var writer: ?*c.fipc_conn_t = null;
    if (c.fipc_broadcast_create(args[0], 1 << 20, subscribers, c.FIPC_FOREVER, &writer) != c.FIPC_OK) exit(1);
    signal("L");
    var joined: u32 = 0;
    if (c.fipc_broadcast_wait_for_members(writer, subscribers, &joined, @intCast(10000 * subscribers)) != c.FIPC_OK) exit(2);
    signal("ALL");
    waitForTest();
    var buf: [1024]u8 = undefined;
    for (0..int(args[2])) |i| {
        const len = stamp.sizeOf(i, buf.len);
        stamp.fill(buf[0..len], i);
        if (c.fipc_send(writer, &buf, len, 10000) != c.FIPC_OK) exit(3);
    }
    signal("SENT");
    sleepMs(120000);
    exit(0);
}

/// `<publish|join|join-gated> <name> <step>`, through the native API: a publisher (a broadcast of 64 KiB for one
/// subscriber, which holds until killed: its process admits the joiner) or a lossy or gated subscriber whose process
/// ends as a crash would (exit code 99) when its side of the join or its watcher reaches `step`
/// (fastipc.test_hooks.crash_at). Prints "OPEN" once it listens, or before it joins; a subscriber past `step` prints
/// READY once joined, and closes and exits 0 on the test's byte.
fn nativeBroadcastCrash(args: Args) noreturn {
    fastipc.test_hooks.crash_at = std.meta.stringToEnum(fastipc.test_hooks.Step, args[2]) orelse exit(4);
    if (std.mem.eql(u8, args[0], "publish")) {
        _ = fastipc.broadcastCreate(std.heap.c_allocator, args[1], 65536, 1, -1) catch exit(1);
        signal("OPEN");
        while (true) sleepMs(60_000);
    }
    const role: fastipc.SubscriberRole = if (std.mem.eql(u8, args[0], "join"))
        .lossy
    else if (std.mem.eql(u8, args[0], "join-gated"))
        .gated
    else
        exit(5);
    signal("OPEN");
    const conn = fastipc.broadcastJoin(std.heap.c_allocator, args[1], role, .none) catch exit(3);
    signal("READY");
    waitForTest();
    conn.close();
    exit(0);
}

// --- Work queues (queue/) ---

/// `<name> <producer_index> <count>`: joins the queue on `name` as a producer within 10 s ("J"; exit 2 if it can't),
/// sends `count` checked messages (stamp.zig, up to 256 bytes) whose sequence numbers name it (`producer_index` << 32 |
/// i), says "SENT", and closes and exits 0 on the test's byte. Exit 3: a send failed.
fn queueProduce(args: Args) noreturn {
    const conn = join(args[0], c.FIPC_PRODUCER);
    const producer_index: u64 = int(args[1]);
    var buf: [256]u8 = undefined;
    for (0..int(args[2])) |i| {
        const len = stamp.sizeOf(i, buf.len);
        stamp.fill(buf[0..len], producer_index << 32 | i);
        const result = c.fipc_send(conn, &buf, len, 10000);
        if (result != c.FIPC_OK) {
            std.debug.print("queue-produce: {s} at message {d}\n", .{ c.fipc_result_str(result), i });
            exit(3);
        }
    }
    signal("SENT");
    waitForTest();
    c.fipc_close(conn);
    exit(0);
}

/// `<name> <producer index> <count> <batch>`: joins the queue as a producer, sends `count` checked messages (stamp.zig,
/// up to 256 bytes) with `fipc_send_many`, `batch` at a time, each message's sequence number its index with the
/// producer's index above it, says "SENT", then holds until the test's byte; exits 0, or 3 if a batch failed.
fn queueProduceBatches(args: Args) noreturn {
    const conn = join(args[0], c.FIPC_PRODUCER);
    const producer_index: u64 = int(args[1]);
    const count = int(args[2]);
    const batch = int(args[3]);
    if (batch == 0 or batch > 64) exit(87);
    var storage: [64][256]u8 = undefined;
    var datas: [64]?*const anyopaque = undefined;
    var lens: [64]usize = undefined;
    var next: u32 = 0;
    while (next < count) {
        const n = @min(batch, count - next);
        for (0..n) |i| {
            const len = stamp.sizeOf(next + i, storage[i].len);
            stamp.fill(storage[i][0..len], producer_index << 32 | (next + i));
            datas[i] = &storage[i];
            lens[i] = len;
        }
        var sent: usize = 0;
        const result = c.fipc_send_many(conn, &datas, &lens, n, &sent, 10000);
        if (result != c.FIPC_OK or sent != n) {
            std.debug.print("queue-produce-batches: {s}, {d} of {d} sent at message {d}\n", .{ c.fipc_result_str(result), sent, n, next });
            exit(3);
        }
        next += n;
    }
    signal("SENT");
    waitForTest();
    c.fipc_close(conn);
    exit(0);
}

/// `<name> <producer|consumer|consumer-inplace|consumer-released>`: joins the queue as a producer or consumer ("J"); a
/// consumer-inplace takes one message in place ("R"; exit 4 if none came within 10 s) and holds it; a
/// consumer-released takes one in place and releases it at once, before its work ("R"). Then holds until killed.
fn queueHold(args: Args) noreturn {
    const in_place = std.mem.eql(u8, args[1], "consumer-inplace");
    const released = std.mem.eql(u8, args[1], "consumer-released");
    const role: c_int = if (std.mem.eql(u8, args[1], "producer")) c.FIPC_PRODUCER else if (in_place or released or std.mem.eql(u8, args[1], "consumer")) c.FIPC_CONSUMER else exit(87);
    const conn = join(args[0], role);
    if (in_place or released) {
        var at: ?*const anyopaque = null;
        var len: usize = 0;
        if (c.fipc_recv_acquire(conn, &at, &len, 10000) != c.FIPC_OK) exit(4);
        if (released) c.fipc_recv_release(conn);
        signal("R");
    }
    sleepMs(120000);
    exit(0);
}

/// `<name> <count>`: claims a work queue of 64 KiB rings, 4 producers and 4 consumers, and joins it as a producer on
/// the same thread ("L"; exit 1 if it can't), sends `count` checked messages (stamp.zig, up to 256 bytes, producer 0's),
/// says "SENT", then holds until killed (its process admits the members).
fn queueOwn(args: Args) noreturn {
    var queue: ?*c.fipc_queue_t = null;
    var own: ?*c.fipc_conn_t = null;
    if (c.fipc_queue_create(args[0], 65536, 4, 4, &queue) != c.FIPC_OK) exit(1);
    if (c.fipc_queue_join(args[0], c.FIPC_PRODUCER, &own, 10000) != c.FIPC_OK) exit(1);
    signal("L");
    var buf: [256]u8 = undefined;
    for (0..int(args[1])) |i| {
        const len = stamp.sizeOf(i, buf.len);
        stamp.fill(buf[0..len], i);
        if (c.fipc_send(own, &buf, len, 10000) != c.FIPC_OK) exit(3);
    }
    signal("SENT");
    while (true) sleepMs(60_000);
}

/// `<own|producer|consumer> <name> <step>`, through the native API: an owner (a queue of 4 KiB rings for one producer
/// and one consumer, which holds until killed: its process admits the members), or a producer or consumer whose process
/// ends as a crash would (exit code 99) when its side of the join, its watcher or its data path reaches `step`
/// (fastipc.test_hooks.crash_at). Prints "OPEN" once it listens, or before it joins; a member past its join's steps
/// prints READY, then a producer sends one message and a consumer receives one (within 10 s), and it closes and exits 0
/// on the test's byte.
fn nativeQueueCrash(args: Args) noreturn {
    fastipc.test_hooks.crash_at = std.meta.stringToEnum(fastipc.test_hooks.Step, args[2]) orelse exit(4);
    if (std.mem.eql(u8, args[0], "own")) {
        _ = fastipc.queueCreate(std.heap.c_allocator, args[1], 4096, 1, 1) catch exit(1);
        signal("OPEN");
        while (true) sleepMs(60_000);
    }
    const role: fastipc.QueueRole = if (std.mem.eql(u8, args[0], "producer"))
        .producer
    else if (std.mem.eql(u8, args[0], "consumer"))
        .consumer
    else
        exit(5);
    signal("OPEN");
    const conn = fastipc.queueJoin(std.heap.c_allocator, args[1], role, .none) catch exit(3);
    signal("READY");
    var buf: [64]u8 = undefined;
    switch (role) {
        .producer => conn.send("crash", ten_s) catch exit(6),
        .consumer => _ = conn.recv(&buf, ten_s) catch exit(6),
    }
    waitForTest();
    conn.close();
    exit(0);
}

// --- Services (service/) ---

/// Request `index` of client `client_index`, and its reply, as tests/zig/service/shared.zig numbers them.
fn requestSeq(client_index: u64, index: u64) u64 {
    return client_index << 32 | index;
}

fn replySeq(seq: u64) u64 {
    return seq | 1 << 63;
}

/// Sends checked message `seq` (stamp.zig, up to 256 bytes) within 10 s; exit 3 if it can't.
fn sendStamp(conn: ?*c.fipc_conn_t, seq: u64) void {
    var buf: [256]u8 = undefined;
    const len = stamp.sizeOf(seq, buf.len);
    stamp.fill(buf[0..len], seq);
    const result = c.fipc_send(conn, &buf, len, 10000);
    if (result == c.FIPC_OK) return;
    std.debug.print("service: {s} at the send of {d}\n", .{ c.fipc_result_str(result), seq });
    exit(3);
}

/// `<name> <client_index> <count> <depth>`: joins the service on `name` as a client within 10 s ("J"; exit 2 if it
/// can't), sends `count` checked requests (stamp.zig, up to 256 bytes) whose sequence numbers name it (`client_index` <<
/// 32 | i), at most `depth` in flight, and checks that each reply is its request's (the sequence number with bit 63
/// set), in order; says "DONE", and closes and exits 0 on the test's byte. Exit 3: a call failed, or a reply wasn't its
/// request's.
fn serviceAsk(args: Args) noreturn {
    const conn = join(args[0], c.FIPC_CLIENT);
    const client_index: u64 = int(args[1]);
    const count: u64 = int(args[2]);
    const depth: u64 = int(args[3]);
    var sent: u64 = 0;
    var received: u64 = 0;
    var buf: [256]u8 = undefined;
    while (received < count) {
        if (sent < count and sent - received < depth) {
            sendStamp(conn, requestSeq(client_index, sent));
            sent += 1;
            continue;
        }
        var len: usize = 0;
        const result = c.fipc_recv(conn, &buf, buf.len, &len, 10000);
        if (result != c.FIPC_OK or stamp.check(buf[0..len]) != replySeq(requestSeq(client_index, received))) {
            std.debug.print("service-ask: {s} at reply {d}\n", .{ c.fipc_result_str(result), received });
            exit(3);
        }
        received += 1;
    }
    signal("DONE");
    waitForTest();
    c.fipc_close(conn);
    exit(0);
}

/// `<name> <client|server|server-inplace|server-stray>`: joins the service as a client or a server ("J"); a client sends
/// three checked requests (sequence numbers 0 to 2: "SENT"); a server takes one request, copied or in place ("R"; exit
/// 4 if none came within 10 s), and holds it without replying; "server-stray" then sends a frame that doesn't belong to
/// the owner (fastipc.test_hooks.sendStray), which drops it. Then holds until killed.
fn serviceHold(args: Args) noreturn {
    const in_place = std.mem.eql(u8, args[1], "server-inplace");
    const stray = std.mem.eql(u8, args[1], "server-stray");
    const role: c_int = if (std.mem.eql(u8, args[1], "client")) c.FIPC_CLIENT else if (in_place or stray or std.mem.eql(u8, args[1], "server")) c.FIPC_SERVER else exit(87);
    const conn = join(args[0], role);
    if (role == c.FIPC_CLIENT) {
        for (0..3) |i| sendStamp(conn, i);
        signal("SENT");
    } else {
        var len: usize = 0;
        if (in_place) {
            var at: ?*const anyopaque = null;
            if (c.fipc_recv_acquire(conn, &at, &len, 10000) != c.FIPC_OK) exit(4);
        } else {
            var buf: [256]u8 = undefined;
            if (c.fipc_recv(conn, &buf, buf.len, &len, 10000) != c.FIPC_OK) exit(4);
        }
        if (stray) fastipc.test_hooks.sendStray(conn.?);
        signal("R");
    }
    sleepMs(120000);
    exit(0);
}

/// `<name>`: claims a service of 64 KiB rings, requests and replies of up to 256 bytes, 4 clients and 4 servers ("L";
/// exit 1 if it can't), then holds until killed (its process admits the members).
fn serviceOwn(args: Args) noreturn {
    var service: ?*c.fipc_service_t = null;
    if (c.fipc_service_create(args[0], 65536, 256, 4, 4, &service) != c.FIPC_OK) exit(1);
    signal("L");
    while (true) sleepMs(60_000);
}

/// `<own|client|server> <name> <step>`, through the native API: an owner (a service of 4 KiB rings, requests and replies
/// of up to 64 bytes, one client and one server, which holds until killed: its process admits the members), or a client
/// or server whose process ends as a crash would (exit code 99) when its side of the join, its watcher or its data path
/// reaches `step` (fastipc.test_hooks.crash_at). Prints "OPEN" once it listens, or before it joins; a member past its
/// join's steps prints READY, then a client sends one request, "crash", and receives its reply, and a server takes one
/// request ("R") and on the test's byte replies "crash" (each within 10 s), and it closes and exits 0 on the test's
/// byte.
fn nativeServiceCrash(args: Args) noreturn {
    fastipc.test_hooks.crash_at = std.meta.stringToEnum(fastipc.test_hooks.Step, args[2]) orelse exit(4);
    if (std.mem.eql(u8, args[0], "own")) {
        _ = fastipc.serviceCreate(std.heap.c_allocator, args[1], 4096, 64, 1, 1) catch exit(1);
        signal("OPEN");
        while (true) sleepMs(60_000);
    }
    const role: fastipc.ServiceRole = if (std.mem.eql(u8, args[0], "client"))
        .client
    else if (std.mem.eql(u8, args[0], "server"))
        .server
    else
        exit(5);
    signal("OPEN");
    const conn = fastipc.serviceJoin(std.heap.c_allocator, args[1], role, .none) catch exit(3);
    signal("READY");
    var buf: [64]u8 = undefined;
    switch (role) {
        .client => {
            conn.send("crash", ten_s) catch exit(6);
            _ = conn.recv(&buf, ten_s) catch exit(6);
        },
        .server => {
            _ = conn.recv(&buf, ten_s) catch exit(6);
            signal("R");
            waitForTest();
            conn.send("crash", ten_s) catch exit(6);
        },
    }
    waitForTest();
    conn.close();
    exit(0);
}
