//! The FastIPC library under test, loaded at run time (`--lib`): one benchmark binary measures any build of the C
//! API of include/fipc.h - this tree's or another revision's - so an A/B run compares libraries, not benchmark
//! builds. The function types come from the header (module "c"), so every call through the table is checked as a call
//! to the header's declaration would be.
//!
//! The wrappers turn result codes into errors and out-parameters into return values. They are inline, so a benchmark
//! loop pays for the library call and nothing more.

const std = @import("std");
const builtin = @import("builtin");
const affinity = @import("affinity.zig");
pub const c = @import("c");

pub const Listener = c.fipc_listener_t;
pub const Conn = c.fipc_conn_t;
pub const RpcMsg = c.fipc_rpc_msg_t;

/// Millisecond timeouts.
pub const forever: c_int = c.FIPC_FOREVER;
pub const no_wait: c_int = c.FIPC_NO_WAIT;

/// The largest message the zero-copy calls take on `conn`.
pub fn maxPiece(conn: *Conn) usize {
    return api.fipc_max_piece(conn);
}

/// The entry points the benchmarks call, looked up by their C names.
const Api = struct {
    fipc_listen: *const @TypeOf(c.fipc_listen),
    fipc_accept: *const @TypeOf(c.fipc_accept),
    fipc_max_piece: *const @TypeOf(c.fipc_max_piece),
    fipc_listener_close: *const @TypeOf(c.fipc_listener_close),
    fipc_connect: *const @TypeOf(c.fipc_connect),
    fipc_close: *const @TypeOf(c.fipc_close),
    fipc_send: *const @TypeOf(c.fipc_send),
    fipc_recv: *const @TypeOf(c.fipc_recv),
    fipc_send_acquire: *const @TypeOf(c.fipc_send_acquire),
    fipc_send_commit: *const @TypeOf(c.fipc_send_commit),
    fipc_recv_acquire: *const @TypeOf(c.fipc_recv_acquire),
    fipc_recv_release: *const @TypeOf(c.fipc_recv_release),
    fipc_rpc_submit: *const @TypeOf(c.fipc_rpc_submit),
    fipc_rpc_respond: *const @TypeOf(c.fipc_rpc_respond),
    fipc_rpc_recv: *const @TypeOf(c.fipc_rpc_recv),
};

/// Set once by `load`, before any other thread starts.
var api: Api = undefined;

pub const LoadError = error{ LibraryNotLoaded, SymbolNotFound, OutOfMemory };

/// Loads the library at `path` and resolves every entry point. It stays loaded for the life of the process.
pub fn load(gpa: std.mem.Allocator, path: []const u8) LoadError!void {
    var library = try Library.open(gpa, path);
    const info = @typeInfo(Api).@"struct";
    inline for (info.field_names, info.field_types) |field_name, field_type| {
        @field(api, field_name) = library.lookup(field_type, field_name) orelse {
            std.log.err("{s} has no function {s} (it doesn't speak include/fipc.h)", .{ path, field_name });
            return error.SymbolNotFound;
        };
    }
}

const Library = if (builtin.os.tag == .windows) struct {
    module: std.os.windows.HMODULE,

    const load_with_altered_search_path: u32 = 0x0000_0008;
    extern "kernel32" fn LoadLibraryExW(name: [*:0]const u16, file: ?*anyopaque, flags: u32) callconv(.winapi) ?std.os.windows.HMODULE;
    extern "kernel32" fn GetProcAddress(module: std.os.windows.HMODULE, name: [*:0]const u8) callconv(.winapi) ?*anyopaque;
    extern "kernel32" fn GetLastError() callconv(.winapi) u32;

    fn open(gpa: std.mem.Allocator, path: []const u8) LoadError!@This() {
        const wide = std.unicode.wtf8ToWtf16LeAllocZ(gpa, path) catch |err| switch (err) {
            error.OutOfMemory => return error.OutOfMemory,
            error.InvalidWtf8 => return error.LibraryNotLoaded,
        };
        defer gpa.free(wide);
        const module = LoadLibraryExW(wide, null, load_with_altered_search_path) orelse {
            std.log.err("cannot load {s} (Windows error {d})", .{ path, GetLastError() });
            return error.LibraryNotLoaded;
        };
        return .{ .module = module };
    }

    fn lookup(library: *@This(), comptime T: type, name: [:0]const u8) ?T {
        return @ptrCast(GetProcAddress(library.module, name) orelse return null);
    }
} else struct {
    dynlib: std.DynLib,

    fn open(gpa: std.mem.Allocator, path: []const u8) LoadError!@This() {
        _ = gpa;
        const dynlib = std.DynLib.open(path) catch {
            const reason: [*:0]const u8 = std.c.dlerror() orelse "unknown error";
            std.log.err("cannot load {s}: {s}", .{ path, reason });
            return error.LibraryNotLoaded;
        };
        return .{ .dynlib = dynlib };
    }

    fn lookup(library: *@This(), comptime T: type, name: [:0]const u8) ?T {
        return library.dynlib.lookup(T, name);
    }
};

/// `fipc_result_t` codes other than OK.
pub const Error = error{ Timeout, Disconnected, Cancelled, TooLarge, Invalid, NoMemory, AddrInUse, UnknownResult };

inline fn check(code: c.fipc_result_t) Error!void {
    return switch (code) {
        c.FIPC_OK => {},
        c.FIPC_TIMEOUT => error.Timeout,
        c.FIPC_DISCONNECTED => error.Disconnected,
        c.FIPC_CANCELLED => error.Cancelled,
        c.FIPC_TOO_LARGE => error.TooLarge,
        c.FIPC_INVALID => error.Invalid,
        c.FIPC_NO_MEMORY => error.NoMemory,
        c.FIPC_ADDR_IN_USE => error.AddrInUse,
        else => error.UnknownResult,
    };
}

// === Connections ===

/// Server: claims `name`, with rings of `capacity` bytes.
pub fn listen(name: [:0]const u8, capacity: usize) Error!*Listener {
    var listener: ?*Listener = null;
    try check(api.fipc_listen(name.ptr, capacity, &listener));
    return listener.?;
}

/// Server: waits for a client. With `AUTO_AFFINITY=1` the calling thread then moves to the server's CPU of the
/// preferred pair (affinity.zig).
pub fn accept(listener: *Listener, timeout_ms: c_int) Error!*Conn {
    var conn: ?*Conn = null;
    try check(api.fipc_accept(listener, &conn, timeout_ms));
    affinity.pinSide(true);
    return conn.?;
}

pub fn closeListener(listener: *Listener) void {
    api.fipc_listener_close(listener);
}

/// Server: listens on `name`, accepts one client and stops listening (the connection stays open).
pub fn serve(name: [:0]const u8, capacity: usize, timeout_ms: c_int) Error!*Conn {
    const listener = try listen(name, capacity);
    defer closeListener(listener);
    return accept(listener, timeout_ms);
}

/// Client: connects to the server on `name`, which may start listening later. With `AUTO_AFFINITY=1` the calling
/// thread then moves to the client's CPU of the preferred pair.
pub fn connect(name: [:0]const u8, timeout_ms: c_int) Error!*Conn {
    var conn: ?*Conn = null;
    try check(api.fipc_connect(name.ptr, &conn, timeout_ms));
    affinity.pinSide(false);
    return conn.?;
}

pub fn close(conn: *Conn) void {
    api.fipc_close(conn);
}

// === Messages ===

pub inline fn send(conn: *Conn, bytes: []const u8, timeout_ms: c_int) Error!void {
    return check(api.fipc_send(conn, bytes.ptr, bytes.len, timeout_ms));
}

/// The next message, copied into `buf`: its bytes.
pub inline fn recv(conn: *Conn, buf: []u8, timeout_ms: c_int) Error![]u8 {
    var len: usize = 0;
    try check(api.fipc_recv(conn, buf.ptr, buf.len, &len, timeout_ms));
    return buf[0..len];
}

/// Room for a message of `len` bytes (one piece) in the send ring; `sendCommit` sends it.
pub inline fn sendAcquire(conn: *Conn, len: usize, timeout_ms: c_int) Error![]u8 {
    var ptr: ?*anyopaque = null;
    try check(api.fipc_send_acquire(conn, len, &ptr, timeout_ms));
    return @as([*]u8, @ptrCast(ptr.?))[0..len];
}

pub inline fn sendCommit(conn: *Conn, len: usize) Error!void {
    return check(api.fipc_send_commit(conn, len));
}

/// The next message in place (one piece; TooLarge otherwise), until `recvRelease`.
pub inline fn recvAcquire(conn: *Conn, timeout_ms: c_int) Error![]const u8 {
    var ptr: ?*const anyopaque = null;
    var len: usize = 0;
    try check(api.fipc_recv_acquire(conn, &ptr, &len, timeout_ms));
    return @as([*]const u8, @ptrCast(ptr.?))[0..len];
}

pub inline fn recvRelease(conn: *Conn) void {
    api.fipc_recv_release(conn);
}

// === RPC ===

/// Sends a request; returns its id.
pub inline fn submit(conn: *Conn, opcode: u32, payload: []const u8, timeout_ms: c_int) Error!u64 {
    var id: u64 = 0;
    try check(api.fipc_rpc_submit(conn, opcode, payload.ptr, payload.len, &id, timeout_ms));
    return id;
}

pub inline fn respond(conn: *Conn, id: u64, opcode: u32, payload: []const u8, timeout_ms: c_int) Error!void {
    return check(api.fipc_rpc_respond(conn, id, opcode, 0, payload.ptr, payload.len, timeout_ms));
}

/// The next request or response, its payload copied into `buf` (`msg.len` bytes).
pub inline fn rpcRecv(conn: *Conn, buf: []u8, timeout_ms: c_int) Error!RpcMsg {
    var msg: RpcMsg = undefined;
    try check(api.fipc_rpc_recv(conn, buf.ptr, buf.len, &msg, timeout_ms));
    return msg;
}
