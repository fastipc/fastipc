//! The test peer: the other process of the multi-process tests in tests/zig. Each scenario plays one role against the
//! test: a server that listens (the test connects), or a client that connects (the test listens), through the C API,
//! or through the native API for native_peer_death_test.zig.
//!
//! Usage: `test_peer <scenario> <args...>`. A scenario reports progress as tokens on its stdout ("L" once it listens,
//! "A" once it accepted, "C" once it connected, ...), reads a byte from its stdin where it waits for the test, and ends
//! with its exit code; `exit` leaves without closing anything, as a crash would. The peer exits with 97 at once when its
//! stdin closes, which means the test process died.

const std = @import("std");
const builtin = @import("builtin");
const c = @import("c");
const fastipc = @import("fastipc");

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
    // Both, in this process (the same-process warning)
    .{ .name = "self-pair", .run = selfPair, .args = 1 },
    // Through the native API (native_peer_death_test.zig)
    .{ .name = "native-pair", .run = nativePair, .args = 2 },
    .{ .name = "native-crash", .run = nativeCrash, .args = 3 },
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
    var l: ?*c.fipc_listener_t = null;
    if (c.fipc_listen(name, capacity, &l) != c.FIPC_OK) exit(1);
    signal("L");
    return l;
}

/// Accepts one client, without a timeout (exit 2 if it fails), and says "A".
fn accept(l: ?*c.fipc_listener_t) ?*c.fipc_conn_t {
    var conn: ?*c.fipc_conn_t = null;
    if (c.fipc_accept(l, &conn, -1) != c.FIPC_OK) exit(2);
    signal("A");
    return conn;
}

/// Connects to `name` within 10 s (exit 2 if it can't) and says "C".
fn connect(name: [:0]const u8) ?*c.fipc_conn_t {
    var conn: ?*c.fipc_conn_t = null;
    if (c.fipc_connect(name, &conn, 10000) != c.FIPC_OK) exit(2);
    signal("C");
    return conn;
}

/// Holds until the test's byte (then closes everything and exits 0) or until it is killed.
fn holdThenClose(conn: ?*c.fipc_conn_t, l: ?*c.fipc_listener_t) noreturn {
    waitForTest();
    c.fipc_close(conn);
    c.fipc_listener_close(l);
    exit(0);
}

// --- Servers ---

/// `<name>`: listens ("L"), accepts a client ("A"), and holds the connection until the test's byte or its death.
fn listenHold(args: Args) noreturn {
    const l = listen(args[0]);
    const conn = accept(l);
    holdThenClose(conn, l);
}

/// `<name>`: listens ("L") and exits without closing anything: a server that dies before it accepts.
fn listenExit(args: Args) noreturn {
    _ = listen(args[0]);
    exit(0);
}

/// `<name>`: listens ("L"), closes the listener and exits 0.
fn listenClose(args: Args) noreturn {
    const l = listen(args[0]);
    c.fipc_listener_close(l);
    exit(0);
}

/// `<name> <count> <then>`: listens, accepts ("A"), and responds with an empty payload to up to `count` requests (each
/// waited for 5 s); then `hold` holds the connection until killed, and `close` closes everything and exits 0, or 3 if
/// no request came.
fn serve(args: Args) noreturn {
    const l = listen(args[0]);
    const conn = accept(l);
    const count = int(args[1]);
    var served: u32 = 0;
    var buf: [4096]u8 = undefined;
    while (served < count) : (served += 1) {
        var msg: c.fipc_rpc_msg_t = undefined;
        if (c.fipc_rpc_recv(conn, &buf, buf.len, &msg, 5000) != c.FIPC_OK) break;
        if (c.fipc_rpc_respond(conn, msg.id, msg.opcode, 0, null, 0, 5000) != c.FIPC_OK) break;
    }
    if (std.mem.eql(u8, args[2], "hold")) {
        sleepMs(120000); // until killed
        exit(0);
    }
    c.fipc_close(conn);
    c.fipc_listener_close(l);
    exit(if (served > 0) 0 else 3);
}

/// `<name>`: listens with rings of 1 MiB, accepts ("A"), and echoes each request's payload back in its response until
/// the connection ends; then closes and exits 0.
fn echo(args: Args) noreturn {
    const l = listenWith(args[0], 1 << 20);
    const conn = accept(l);
    const buf = std.heap.c_allocator.alloc(u8, 1 << 20) catch exit(4);
    while (true) {
        var msg: c.fipc_rpc_msg_t = undefined;
        if (c.fipc_rpc_recv(conn, buf.ptr, buf.len, &msg, -1) != c.FIPC_OK) break;
        if (c.fipc_rpc_respond(conn, msg.id, msg.opcode, 0, buf.ptr, msg.len, -1) != c.FIPC_OK) break;
    }
    c.fipc_close(conn);
    c.fipc_listener_close(l);
    exit(0);
}

/// `<name>`: listens, accepts ("A"), reserves 128 bytes of its send ring, fills them, and dies before the commit.
fn partialWrite(args: Args) noreturn {
    const l = listen(args[0]);
    const conn = accept(l);
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
    if (c.fipc_connect(args[0], &conn, c.FIPC_FOREVER) != c.FIPC_OK) exit(2);
    signal("C");
    if (c.fipc_send(conn, "ping", 4, 5000) != c.FIPC_OK) exit(3);
    waitForTest();
    c.fipc_close(conn);
    exit(0);
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

/// `<name>`: connects ("C"), sends 5 messages of 16 bytes (message i filled with 0xAA + i), leaves the test 2 s to
/// receive them, then closes and exits 0. Exit 3: a send failed.
fn sendFresh(args: Args) noreturn {
    const conn = connect(args[0]);
    var msg: [16]u8 = undefined;
    for (0..5) |i| {
        @memset(&msg, 0xAA + @as(u8, @intCast(i)));
        if (c.fipc_send(conn, &msg, msg.len, 5000) != c.FIPC_OK) exit(3);
    }
    sleepMs(2000);
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
    var l: ?*c.fipc_listener_t = null;
    if (c.fipc_listen(args[0], 4096, &l) != c.FIPC_OK) exit(1);
    const Client = struct {
        fn run(name: [:0]const u8, conn: *?*c.fipc_conn_t, rc: *c.fipc_result_t) void {
            rc.* = c.fipc_connect(name, conn, 5000);
        }
    };
    var client: ?*c.fipc_conn_t = null;
    var connect_rc: c.fipc_result_t = c.FIPC_TIMEOUT;
    const thread = std.Thread.spawn(.{}, Client.run, .{ args[0], &client, &connect_rc }) catch exit(4);
    var server: ?*c.fipc_conn_t = null;
    const accept_rc = c.fipc_accept(l, &server, 5000);
    thread.join();
    const connected = connect_rc == c.FIPC_OK and accept_rc == c.FIPC_OK;
    c.fipc_close(client);
    c.fipc_close(server);
    c.fipc_listener_close(l);
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
        const l = fastipc.Listener.listen(io, std.heap.c_allocator, name, 4096) catch exit(1);
        signal("OPEN");
        return l.accept(timeout) catch exit(3);
    }
    if (!std.mem.eql(u8, role, "connect")) exit(5);
    signal("OPEN");
    return fastipc.Conn.connect(io, std.heap.c_allocator, name, timeout) catch exit(3);
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

/// `<listen|connect> <name>`: a connection with the test's: prints READY once connected, and DEAD when the test's side
/// ends the session (a receiving thread sees it); closes and exits 0 on the test's byte.
fn nativePair(args: Args) noreturn {
    const conn = nativeOpen(args[0], args[1], ten_s);
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
