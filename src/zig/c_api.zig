//! The exported C API (`include/fipc.h`) and the root of the library, which is all Zig: build.zig compiles this file
//! into the shipped `fastipc` shared library, which the benchmarks load at run time, and the `fastipc_static` variant
//! that the C-API tests link; only the static variant carries the lifecycle's test hooks (`test_hooks`, Zig only).
//! All the `export fn`s of the shipped library (the `FIPC_API` functions of include/fipc.h) live here, as thin
//! wrappers over the Zig implementation: the listener (lifecycle/listener.zig), the connection's endpoint
//! (lifecycle/endpoint.zig), the data path (data/stream.zig) and RPC (data/rpc.zig), on the process-wide `Io`
//! (process_state.zig) and the C heap. They check the C arguments, call the implementation and turn its errors into
//! result codes. A comptime check keeps each export's parameters and result in line with its prototype in the header.
//! The native Zig API is root.zig (the module "fastipc").

const std = @import("std");
const builtin = @import("builtin");
const abi = @import("abi.zig");
const c = @import("c_headers");
const endpoint = @import("lifecycle/endpoint.zig");
const listener = @import("lifecycle/listener.zig");
const rpc = @import("data/rpc.zig");
const stream = @import("data/stream.zig");

const Result = abi.Result;
const Conn = endpoint.Endpoint;
const Listener = listener.Listener;

/// The C API's handles live on the C heap, on the process-wide `Io` (process_state.zig).
const gpa = std.heap.c_allocator;

/// How the zero-copy exports call the data path: inlined on Linux, where that makes 16-byte zero-copy messages 5-10%
/// faster, and called on Windows, where the same inlining makes them 7-9% slower. Nothing but speed depends on it.
const zero_copy_call: std.lang.CallModifier = if (builtin.os.tag == .linux) .always_inline else .auto;

/// Expected pipe and socket statuses print no stack trace: std's `unexpectedStatus` and `unexpectedErrno` print
/// one only while this is on, which Debug builds default to.
pub const std_options: std.Options = .{ .unexpected_error_tracing = false };

/// Keeps std's start code from exporting its own `_DllMainCRTStartup` from the Windows DLL: the
/// entry point stays the C runtime's (MinGW `DllMainCRTStartup`), and the DLL exports only the public API.
pub const _DllMainCRTStartup = {};

comptime {
    // Every build of the library runs the ABI checks.
    _ = @import("abi.zig");
}

comptime {
    for ([_][]const u8{
        "fipc_result_str",
        "fipc_listen",
        "fipc_accept",
        "fipc_listener_cancel",
        "fipc_listener_close",
        "fipc_connect",
        "fipc_max_piece",
        "fipc_cancel",
        "fipc_close",
        "fipc_send",
        "fipc_recv",
        "fipc_send_acquire",
        "fipc_send_commit",
        "fipc_recv_acquire",
        "fipc_recv_release",
        "fipc_rpc_submit",
        "fipc_rpc_respond",
        "fipc_rpc_recv",
    }) |name| {
        abi.expectSameSignature(name, @TypeOf(@field(@This(), name)), @TypeOf(@field(c, name)));
    }
}

// === Strings ===

export fn fipc_result_str(result: Result) callconv(.c) [*:0]const u8 {
    return switch (result) {
        .ok => "FIPC_OK",
        .timeout => "FIPC_TIMEOUT",
        .disconnected => "FIPC_DISCONNECTED",
        .cancelled => "FIPC_CANCELLED",
        .too_large => "FIPC_TOO_LARGE",
        .invalid => "FIPC_INVALID",
        .no_memory => "FIPC_NO_MEMORY",
        .addr_in_use => "FIPC_ADDR_IN_USE",
        _ => "FIPC_UNKNOWN",
    };
}

// === Connections ===

export fn fipc_listen(name: ?[*:0]const u8, capacity: usize, out_listener: ?*?*Listener) callconv(.c) Result {
    const n = name orelse return .invalid;
    const out = out_listener orelse return .invalid;
    out.* = listener.listen(.process, gpa, std.mem.span(n), capacity) catch |err| return lifecycleResult(err);
    return .ok;
}

export fn fipc_accept(l: ?*Listener, out_conn: ?*?*Conn, timeout_ms: c_int) callconv(.c) Result {
    const lis = l orelse return .invalid;
    const out = out_conn orelse return .invalid;
    out.* = listener.accept(lis, msTimeout(timeout_ms)) catch |err| return lifecycleResult(err);
    return .ok;
}

export fn fipc_listener_cancel(l: ?*Listener) callconv(.c) void {
    listener.cancel(l orelse return);
}

export fn fipc_listener_close(l: ?*Listener) callconv(.c) void {
    listener.close(l orelse return);
}

export fn fipc_connect(name: ?[*:0]const u8, out_conn: ?*?*Conn, timeout_ms: c_int) callconv(.c) Result {
    const n = name orelse return .invalid;
    const out = out_conn orelse return .invalid;
    out.* = endpoint.connect(.process, gpa, std.mem.span(n), msTimeout(timeout_ms)) catch |err| return lifecycleResult(err);
    return .ok;
}

export fn fipc_max_piece(conn: ?*const Conn) callconv(.c) usize {
    return stream.maxPiece(conn orelse return 0);
}

export fn fipc_cancel(conn: ?*Conn) callconv(.c) void {
    endpoint.cancel(conn orelse return);
}

export fn fipc_close(conn: ?*Conn) callconv(.c) void {
    endpoint.close(conn orelse return);
}

// === Messages ===

export fn fipc_send(conn: ?*Conn, data: ?*const anyopaque, len: usize, timeout_ms: c_int) callconv(.c) Result {
    const cn = conn orelse return .invalid;
    const payload = cBytes(data, len) orelse return .invalid;
    return dataCall(stream.send(cn, &.{}, payload, timeout_ms));
}

export fn fipc_recv(conn: ?*Conn, buf: ?*anyopaque, buf_len: usize, out_len: ?*usize, timeout_ms: c_int) callconv(.c) Result {
    const cn = conn orelse return .invalid;
    const len = out_len orelse return .invalid;
    const into = cBuffer(buf, buf_len) orelse return .invalid;
    return dataCall(stream.recv(cn, into, timeout_ms, len));
}

/// A NULL `out_buf` is refused before anything is reserved, and so is an empty reservation, which no commit
/// could use.
export fn fipc_send_acquire(conn: ?*Conn, len: usize, out_buf: ?*?*anyopaque, timeout_ms: c_int) callconv(.c) Result {
    const cn = conn orelse return .invalid;
    const out = out_buf orelse return .invalid;
    if (len == 0) return .invalid;
    var buffer: [*]u8 = undefined;
    @call(zero_copy_call, stream.sendAcquire, .{ cn, len, timeout_ms, &buffer }) catch |err| return dataResult(err);
    out.* = buffer;
    return .ok;
}

export fn fipc_send_commit(conn: ?*Conn, len: usize) callconv(.c) Result {
    return dataCall(stream.sendCommit(conn orelse return .invalid, len));
}

export fn fipc_recv_acquire(conn: ?*Conn, out_data: ?*?*const anyopaque, out_len: ?*usize, timeout_ms: c_int) callconv(.c) Result {
    const cn = conn orelse return .invalid;
    const out = out_data orelse return .invalid;
    const len = out_len orelse return .invalid;
    var data: [*]const u8 = undefined;
    @call(zero_copy_call, stream.recvAcquire, .{ cn, timeout_ms, &data, len }) catch |err| return dataResult(err);
    out.* = data;
    return .ok;
}

export fn fipc_recv_release(conn: ?*Conn) callconv(.c) void {
    stream.recvRelease(conn orelse return);
}

// === RPC ===

export fn fipc_rpc_submit(conn: ?*Conn, opcode: u32, data: ?*const anyopaque, len: usize, out_id: ?*u64, timeout_ms: c_int) callconv(.c) Result {
    const cn = conn orelse return .invalid;
    const id_out = out_id orelse return .invalid;
    const payload = cBytes(data, len) orelse return .invalid;
    var id: u64 = undefined;
    rpc.submit(cn, opcode, payload, timeout_ms, &id) catch |err| return dataResult(err);
    id_out.* = id;
    return .ok;
}

export fn fipc_rpc_respond(conn: ?*Conn, id: u64, opcode: u32, status: i32, data: ?*const anyopaque, len: usize, timeout_ms: c_int) callconv(.c) Result {
    const cn = conn orelse return .invalid;
    const payload = cBytes(data, len) orelse return .invalid;
    return dataCall(rpc.respond(cn, id, opcode, status, payload, timeout_ms));
}

export fn fipc_rpc_recv(conn: ?*Conn, buf: ?*anyopaque, buf_len: usize, msg: ?*abi.RpcMsg, timeout_ms: c_int) callconv(.c) Result {
    const cn = conn orelse return .invalid;
    const out = msg orelse return .invalid;
    const into = cBuffer(buf, buf_len) orelse return .invalid;
    return dataCall(rpc.recv(cn, into, timeout_ms, out));
}

// === C arguments ===

/// A C buffer as a slice: empty if `len` is 0, null if the pointer is NULL although `len` isn't.
fn cBytes(ptr: ?*const anyopaque, len: usize) ?[]const u8 {
    if (len == 0) return &.{};
    const bytes: [*]const u8 = @ptrCast(ptr orelse return null);
    return bytes[0..len];
}

/// A caller's C buffer to write into, as `cBytes`.
fn cBuffer(ptr: ?*anyopaque, len: usize) ?[]u8 {
    if (len == 0) return &.{};
    const bytes: [*]u8 = @ptrCast(ptr orelse return null);
    return bytes[0..len];
}

/// A C timeout in milliseconds as an `Io.Timeout`: negative waits for ever, 0 looks once, a positive value waits that
/// long.
fn msTimeout(timeout_ms: c_int) std.Io.Timeout {
    if (timeout_ms < 0) return .none;
    return .{ .duration = .{ .raw = .fromMilliseconds(timeout_ms), .clock = .awake } };
}

// === Errors as result codes ===

fn dataCall(result: stream.Error!void) Result {
    result catch |err| return dataResult(err);
    return .ok;
}

/// Out of line and cold: inlined, its switch becomes a jump table that every call goes through,
/// success included.
noinline fn dataResult(err: stream.Error) Result {
    @branchHint(.cold);
    return switch (err) {
        error.Invalid => .invalid,
        error.Timeout => .timeout,
        error.TooLarge => .too_large,
        error.Disconnected => .disconnected,
        error.Cancelled => .cancelled,
    };
}

/// The lifecycle's errors, a listener's and a connection's (a subset): each is the result code of its name.
noinline fn lifecycleResult(err: listener.Error) Result {
    @branchHint(.cold);
    return switch (err) {
        error.Invalid => .invalid,
        error.NoMemory => .no_memory,
        error.Timeout => .timeout,
        error.AddrInUse => .addr_in_use,
        error.Cancelled => .cancelled,
    };
}

test {
    // The unit tests of every module.
    _ = @import("platform.zig");
    _ = @import("process_state.zig");
    _ = @import("data/ring.zig");
    _ = @import("data/stream_test.zig");
    _ = @import("data/stream.zig");
    _ = @import("data/ring_wait.zig");
    _ = @import("lifecycle/control.zig");
    _ = @import("lifecycle/endpoint.zig");
    _ = @import("lifecycle/endpoint_test.zig");
    _ = @import("lifecycle/listener.zig");
    _ = @import("lifecycle/names.zig");
    _ = @import("lifecycle/wire.zig");
    _ = @import("log.zig");
    _ = @import("root.zig");
    _ = @import("data/rpc.zig");
    _ = @import("session/segment.zig");
}

test "each error becomes the result code of the same name" {
    inline for (@typeInfo(stream.Error).error_set.error_names.?) |e| {
        try std.testing.expectEqualStrings(comptime snakeCase(e), @tagName(dataResult(@field(stream.Error, e))));
    }
    inline for (@typeInfo(endpoint.Error).error_set.error_names.?) |e| {
        try std.testing.expectEqualStrings(comptime snakeCase(e), @tagName(lifecycleResult(@field(endpoint.Error, e))));
    }
    inline for (@typeInfo(listener.Error).error_set.error_names.?) |e| {
        try std.testing.expectEqualStrings(comptime snakeCase(e), @tagName(lifecycleResult(@field(listener.Error, e))));
    }
}

fn snakeCase(comptime name: []const u8) []const u8 {
    var out: []const u8 = "";
    for (name, 0..) |ch, i| {
        if (std.ascii.isUpper(ch) and i > 0) out = out ++ "_";
        out = out ++ [_]u8{std.ascii.toLower(ch)};
    }
    return out;
}

test "C timeouts: negative waits for ever, 0 looks once, positive waits that long" {
    try std.testing.expectEqual(std.Io.Timeout.none, msTimeout(-1));
    try std.testing.expectEqual(@as(i96, 0), msTimeout(0).duration.raw.nanoseconds);
    try std.testing.expectEqual(@as(i96, 1500 * std.time.ns_per_ms), msTimeout(1500).duration.raw.nanoseconds);
}

test {
    if (@import("build_options").slow_tests) _ = slow_tests;
}

/// The fork test (Linux, macOS): it starts a process, so it runs in the slow tier. Under TSan it is skipped, as the other fork
/// tests are: TSan ends a child that starts a thread after forking from a parent with threads.
const slow_tests = struct {
    const linux = if (builtin.os.tag == .macos) @import("platform/macos_test_sys.zig") else std.os.linux;
    const platform = @import("platform.zig");

    /// Comptime-known, so that the rest of the test isn't even analyzed where it can't run.
    const fork_unsupported = !platform.has_fork or builtin.sanitize_thread;

    test "slow: fork: calls on an inherited connection and listener return INVALID, and the first logs why, once" {
        if (fork_unsupported) return error.SkipZigTest;
        var name_buf: [64]u8 = undefined;
        const name = try std.mem.printSentinel(&name_buf, "test-c-api-fork-{d}", .{platform.pid()}, 0);
        var l: ?*Listener = null;
        try std.testing.expectEqual(Result.ok, fipc_listen(name, 4096, &l));
        defer fipc_listener_close(l);
        // The client connects on another thread: `fipc_connect` waits for the listener's `fipc_accept`
        const Client = struct {
            fn run(n: [*:0]const u8, conn: *?*Conn, rc: *Result) void {
                rc.* = fipc_connect(n, conn, 5000);
            }
        };
        var client: ?*Conn = null;
        var connected: Result = .timeout;
        const thread = try std.Thread.spawn(.{}, Client.run, .{ name, &client, &connected });
        var server: ?*Conn = null;
        const accepted = fipc_accept(l, &server, 5000);
        thread.join();
        defer fipc_close(client);
        defer fipc_close(server);
        try std.testing.expectEqual(Result.ok, connected);
        try std.testing.expectEqual(Result.ok, accepted);

        var stderr_pipe: [2]linux.fd_t = undefined;
        try std.testing.expectEqual(@as(usize, 0), linux.pipe2(&stderr_pipe, .{ .CLOEXEC = true }));
        const pid = std.c.fork();
        if (pid == 0) {
            // The child's stderr is the pipe; it leaves with `_exit`, never through the test runner
            _ = linux.dup2(stderr_pipe[1], 2);
            var taken: ?*Conn = null;
            const code: u8 = code: {
                if (fipc_send(client, "x", 1, 0) != .invalid) break :code 1;
                if (fipc_max_piece(client) != 0) break :code 2;
                if (fipc_accept(l, &taken, 0) != .invalid) break :code 3;
                if (fipc_send(server, "x", 1, 0) != .invalid) break :code 4;
                break :code 0;
            };
            fipc_close(client);
            fipc_close(server);
            fipc_listener_close(l);
            std.c._exit(code);
        }
        try std.testing.expect(pid > 0);
        _ = linux.close(stderr_pipe[1]);
        defer _ = linux.close(stderr_pipe[0]);

        // Everything the child wrote, until it exits (EOF)
        var output: [4096]u8 = undefined;
        var len: usize = 0;
        while (len < output.len) {
            var fds = [1]std.posix.pollfd{.{ .fd = stderr_pipe[0], .events = std.posix.POLL.IN, .revents = 0 }};
            if (try std.posix.poll(&fds, 5000) != 1) break;
            const n = linux.read(stderr_pipe[0], output[len..].ptr, output.len - len);
            if (linux.errno(n) != .SUCCESS or n == 0) break;
            len += n;
        }
        // Its exit within 5 s, or it hung: killed
        var status: c_int = 0;
        const exited = for (0..5000) |_| {
            if (std.c.waitpid(pid, &status, std.c.W.NOHANG) == pid) break true;
            const ms: linux.timespec = .{ .sec = 0, .nsec = std.time.ns_per_ms };
            _ = linux.nanosleep(&ms, null);
        } else false;
        if (!exited) {
            _ = std.c.kill(pid, .KILL);
            _ = std.c.waitpid(pid, &status, 0);
        }
        try std.testing.expect(exited);
        try std.testing.expectEqual(@as(c_int, 0), status);
        const written = output[0..len];
        try std.testing.expectEqual(@as(usize, 1), std.mem.count(u8, written, "inherited through fork()"));
        try std.testing.expect(std.mem.find(u8, written, "a call on a connection inherited through fork() failed") != null);
    }
};
