//! FastIPC's Zig benchmark: one-way throughput through the native Zig API (the module "fastipc", src/zig/root.zig)
//! between this process, a server that receives and times, and a client process (this program again) that sends. The
//! same cases, output and checks as the C, C++, Python, C#, Java, Rust, Lua, JavaScript and Go benchmarks.
//!
//!   zig_bench copy|zerocopy|rpc
//!
//!   copy      Conn.send / Conn.recv into a buffer the server reuses
//!   zerocopy  Conn.acquire + Conn.commit / Conn.recvAcquire, copied out, + Conn.release (a message of several pieces
//!             through the copying calls)
//!   rpc       Conn.rpcSubmit / Conn.rpcRecv into a buffer the server reuses
//!
//! Each case first sends messages untimed for 0.2 s (the warm-up; at least its count), then a start marker, then its
//! count, and the server times from the last start marker to the end marker. Each case prints "Test i/n: name" and
//! "Throughput: n messages/sec" (devtool bench-compare reads them).

const std = @import("std");
const Io = std.Io;
const fastipc = @import("fastipc");

const connect_timeout: Io.Timeout = .{ .duration = .{ .raw = .fromSeconds(15), .clock = .awake } };

/// The opcodes of the RPC end and start markers; the plain markers are the messages "END" and "GO!"
const end_opcode: u32 = 0xFB000001;
const go_opcode: u32 = 0xFB000002;

/// The warm-up: the client sends messages untimed for at least this long (and at least the case's count), with a start
/// marker every `marker_every` of them, then the start marker the server times from
const warm_up_ns: u64 = 200_000_000;
const marker_every = 1000;

const Mode = enum { copy, zerocopy, rpc };

const Case = struct { count: usize, ring: usize, size: usize, name: []const u8 };

/// The cases of the other benchmarks
const cases = [_]Case{
    .{ .count = 2_000_000, .ring = 512 * 1024, .size = 16, .name = "Tiny messages (16B)" },
    .{ .count = 2_000_000, .ring = 512 * 1024, .size = 64, .name = "Small messages (64B)" },
    .{ .count = 1_000_000, .ring = 512 * 1024, .size = 256, .name = "Medium messages (256B)" },
    .{ .count = 1000, .ring = 512 * 1024, .size = 64 * 1024, .name = "Large messages (64KB)" },
    .{ .count = 1000, .ring = 2 * 1024 * 1024, .size = 512 * 1024, .name = "Large messages (512KB)" },
    .{ .count = 10, .ring = 512 * 1024, .size = 1024 * 1024, .name = "Exceeds buffer (1MB msg, 512KB buffer)" },
};

/// What the server's loop received, and how long it took
const Served = struct { messages: usize = 0, bytes: usize = 0, seconds: f64 = 0 };

fn now(io: Io) u64 {
    return @intCast(Io.Clock.awake.now(io).nanoseconds);
}

/// What a received message is: data, or one of the markers
const Kind = enum { data, end, go };

fn marker(data: []const u8) Kind {
    if (std.mem.eql(u8, data, "END")) return .end;
    return if (std.mem.eql(u8, data, "GO!")) .go else .data;
}

/// The server: receives on `conn` until the end marker, skipping the warm-up and timing from the last start marker to
/// the end marker, then closes `conn`.
fn serve(io: Io, gpa: std.mem.Allocator, conn: fastipc.Conn, mode: Mode, size: usize) !Served {
    defer conn.close();
    const cap = @max(size, 3);
    const buf = try gpa.alloc(u8, cap);
    defer gpa.free(buf);
    const sink = try gpa.alloc(u8, cap);
    defer gpa.free(sink);
    var served: Served = .{};
    var timing = false;
    var start = now(io);
    while (true) {
        var kind: Kind = .data;
        const len = switch (mode) {
            .rpc => blk: {
                const msg = try conn.rpcRecv(buf, .none);
                kind = if (msg.opcode == end_opcode) .end else if (msg.opcode == go_opcode) .go else .data;
                break :blk msg.payload.len;
            },
            .zerocopy => blk: {
                const data = conn.recvAcquire(.none) catch |err| switch (err) {
                    error.TooLarge => break :blk try conn.recv(buf, .none), // several pieces
                    else => return err,
                };
                kind = marker(data);
                if (kind == .data) {
                    @memcpy(sink[0..@min(data.len, cap)], data[0..@min(data.len, cap)]); // a consumer copies the message out
                    asm volatile ("" // and uses it: the compiler would drop a dead copy
                        :
                        : [sink] "r" (sink.ptr),
                        : .{ .memory = true });
                }
                conn.release();
                break :blk data.len;
            },
            .copy => blk: {
                const n = try conn.recv(buf, .none);
                kind = marker(buf[0..n]);
                break :blk n;
            },
        };
        switch (kind) {
            .end => break,
            .go => { // the last one starts the timed messages
                timing = true;
                served = .{};
                start = now(io);
            },
            .data => if (timing) {
                served.messages += 1;
                served.bytes += len;
            },
        }
    }
    served.seconds = @as(f64, @floatFromInt(now(io) - start)) / 1e9;
    return served;
}

fn sendMarker(conn: fastipc.Conn, mode: Mode, opcode: u32, text: []const u8) !void {
    if (mode == .rpc) _ = try conn.rpcSubmit(opcode, "", .none) else try conn.send(text, .none);
}

fn sendOne(conn: fastipc.Conn, mode: Mode, payload: []const u8, one_piece: bool) !void {
    switch (mode) {
        .rpc => _ = try conn.rpcSubmit(1, payload, .none),
        .zerocopy => if (one_piece) {
            const slot = try conn.acquire(payload.len, .none);
            @memcpy(slot[0..payload.len], payload);
            try conn.commit(payload.len);
        } else try conn.send(payload, .none),
        .copy => try conn.send(payload, .none),
    }
}

/// The client: sends the warm-up, the start marker, `count` messages of `size` bytes and the end marker on `conn`,
/// then closes it.
fn send(io: Io, gpa: std.mem.Allocator, conn: fastipc.Conn, mode: Mode, size: usize, count: usize) !void {
    defer conn.close();
    const payload = try gpa.alloc(u8, size);
    defer gpa.free(payload);
    @memset(payload, 'x');
    const one_piece = size <= conn.maxPiece();
    const warm = now(io) + warm_up_ns;
    var i: usize = 0;
    while (i < count or now(io) < warm) : (i += 1) {
        if (i % marker_every == 0) try sendMarker(conn, mode, go_opcode, "GO!");
        try sendOne(conn, mode, payload, one_piece);
    }
    try sendMarker(conn, mode, go_opcode, "GO!");
    for (0..count) |_| try sendOne(conn, mode, payload, one_piece);
    try sendMarker(conn, mode, end_opcode, "END");
}

/// Runs one case: listens, starts the client, serves it, checks what arrived and prints the rate.
fn runCase(io: Io, gpa: std.mem.Allocator, out: *Io.Writer, exe: []const u8, mode: Mode, c: Case, index: usize) !void {
    var name_buf: [64]u8 = undefined;
    const name = try std.fmt.bufPrint(&name_buf, "zigbench_{d}_{d}", .{ now(io), index });
    const listener = try fastipc.Listener.listen(io, gpa, name, c.ring);
    defer listener.close();
    var size_buf: [32]u8 = undefined;
    var count_buf: [32]u8 = undefined;
    var child = try std.process.spawn(io, .{
        .argv = &.{
            exe,                                               "client",                                            @tagName(mode), name,
            try std.fmt.bufPrint(&size_buf, "{d}", .{c.size}), try std.fmt.bufPrint(&count_buf, "{d}", .{c.count}),
        },
        .stdout = .ignore,
    });
    const served = served: {
        const conn = listener.accept(connect_timeout) catch |err| break :served err;
        break :served serve(io, gpa, conn, mode, c.size);
    } catch |err| {
        std.debug.print("server: {t}\n", .{err});
        child.kill(io);
        return error.CaseFailed;
    };
    switch (try child.wait(io)) {
        .exited => |code| if (code != 0) {
            std.debug.print("the client failed: exit code {d}\n", .{code});
            return error.CaseFailed;
        },
        else => |term| {
            std.debug.print("the client ended abnormally: {any}\n", .{term});
            return error.CaseFailed;
        },
    }
    if (served.messages != c.count or served.bytes != c.count * c.size) {
        std.debug.print("expected {d} messages of {d} B, got {d} ({d} B)\n", .{ c.count, c.size, served.messages, served.bytes });
        return error.CaseFailed;
    }
    const messages: f64 = @floatFromInt(served.messages);
    const bytes: f64 = @floatFromInt(served.bytes);
    try out.print("Messages: {d} | Ring: {d}KB | Size: {d}B\n", .{ c.count, c.ring / 1024, c.size });
    try out.print("Duration: {d:.3}s\n", .{served.seconds});
    try out.print("Throughput: {d:.0} messages/sec, {d:.1} MB/sec\n", .{ messages / served.seconds, bytes / served.seconds / (1024.0 * 1024.0) });
}

/// The client process: `client <mode> <name> <size> <count>`
fn client(io: Io, gpa: std.mem.Allocator, args: []const [:0]const u8) !u8 {
    if (args.len != 6) return 2;
    const mode = std.meta.stringToEnum(Mode, args[2]) orelse return 2;
    const size = try std.fmt.parseInt(usize, args[4], 10);
    const count = try std.fmt.parseInt(usize, args[5], 10);
    const conn = try fastipc.Conn.connect(io, gpa, args[3], connect_timeout);
    send(io, gpa, conn, mode, size, count) catch |err| {
        std.debug.print("client: {t}\n", .{err});
        return 1;
    };
    return 0;
}

pub fn main(init: std.process.Init) !u8 {
    const io = init.io;
    const gpa = init.gpa;
    const args = try init.minimal.args.toSlice(init.arena.allocator());
    if (args.len >= 2 and std.mem.eql(u8, args[1], "client")) return client(io, gpa, args);
    const mode: Mode = if (args.len == 1) .copy else if (args.len == 2) std.meta.stringToEnum(Mode, args[1]) orelse .copy else .copy;
    if (args.len > 2 or (args.len == 2 and std.meta.stringToEnum(Mode, args[1]) == null)) {
        std.debug.print("usage: {s} copy|zerocopy|rpc\n", .{args[0]});
        return 2;
    }
    const exe = try std.process.executablePathAlloc(io, init.arena.allocator());

    var stdout_buffer: [4096]u8 = undefined;
    var stdout = Io.File.stdout().writer(io, &stdout_buffer);
    const out = &stdout.interface;
    try out.print("FastIPC Zig benchmark: {t}\n\n", .{mode});
    var passed: usize = 0;
    for (cases, 0..) |c, i| {
        try out.print("Test {d}/{d}: {s}\n", .{ i + 1, cases.len, c.name });
        try out.flush();
        if (runCase(io, gpa, out, exe, mode, c, i)) {
            passed += 1;
        } else |_| try out.writeAll("FAILED\n");
        try out.writeAll("\n");
        try out.flush();
    }
    try out.print("Summary: {d}/{d} tests passed\n", .{ passed, cases.len });
    try out.flush();
    return if (passed == cases.len) 0 else 1;
}
