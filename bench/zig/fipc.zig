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
pub const Queue = c.fipc_queue_t;
pub const Service = c.fipc_service_t;

/// Millisecond timeouts.
pub const forever: c_int = c.FIPC_FOREVER;
pub const no_wait: c_int = c.FIPC_NO_WAIT;

/// The largest message the zero-copy calls take on `conn`.
pub fn maxPiece(conn: *Conn) usize {
    return api.fipc_max_piece(conn);
}

/// The entry points the benchmarks call, looked up by their C names.
const Api = struct {
    fipc_stream_create: *const @TypeOf(c.fipc_stream_create),
    fipc_stream_accept: *const @TypeOf(c.fipc_stream_accept),
    fipc_max_piece: *const @TypeOf(c.fipc_max_piece),
    fipc_listener_close: *const @TypeOf(c.fipc_listener_close),
    fipc_stream_connect: *const @TypeOf(c.fipc_stream_connect),
    fipc_close: *const @TypeOf(c.fipc_close),
    fipc_send: *const @TypeOf(c.fipc_send),
    fipc_recv: *const @TypeOf(c.fipc_recv),
    fipc_send_acquire: *const @TypeOf(c.fipc_send_acquire),
    fipc_send_commit: *const @TypeOf(c.fipc_send_commit),
    fipc_recv_acquire: *const @TypeOf(c.fipc_recv_acquire),
    fipc_recv_release: *const @TypeOf(c.fipc_recv_release),
    fipc_broadcast_create: *const @TypeOf(c.fipc_broadcast_create),
    fipc_broadcast_join: *const @TypeOf(c.fipc_broadcast_join),
    fipc_broadcast_wait_for_members: *const @TypeOf(c.fipc_broadcast_wait_for_members),
    fipc_queue_create: *const @TypeOf(c.fipc_queue_create),
    fipc_queue_join: *const @TypeOf(c.fipc_queue_join),
    fipc_queue_wait_for_members: *const @TypeOf(c.fipc_queue_wait_for_members),
    fipc_queue_close: *const @TypeOf(c.fipc_queue_close),
};

/// The service's entry points, apart: a library from before the service has none, and measures every other scenario.
const ServiceApi = struct {
    fipc_service_create: *const @TypeOf(c.fipc_service_create),
    fipc_service_join: *const @TypeOf(c.fipc_service_join),
    fipc_service_wait_for_members: *const @TypeOf(c.fipc_service_wait_for_members),
    fipc_service_close: *const @TypeOf(c.fipc_service_close),
};

/// Set once by `load`, before any other thread starts.
var api: Api = undefined;
/// Null if the library has no service.
var service_api: ?ServiceApi = null;

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
    var service: ServiceApi = undefined;
    const service_info = @typeInfo(ServiceApi).@"struct";
    inline for (service_info.field_names, service_info.field_types) |field_name, field_type| {
        @field(service, field_name) = library.lookup(field_type, field_name) orelse return;
    }
    service_api = service;
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
pub const Error = error{ Timeout, Disconnected, Cancelled, TooLarge, Invalid, NoMemory, AddrInUse, Overrun, Unsubscribed, Abandoned, UnknownResult };

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
        c.FIPC_OVERRUN => error.Overrun,
        c.FIPC_UNSUBSCRIBED => error.Unsubscribed,
        c.FIPC_ABANDONED => error.Abandoned,
        else => error.UnknownResult,
    };
}

// === Connections ===

/// Server: claims `name` for a stream, with rings of `capacity` bytes.
pub fn streamCreate(name: [:0]const u8, capacity: usize) Error!*Listener {
    var listener: ?*Listener = null;
    try check(api.fipc_stream_create(name.ptr, capacity, &listener));
    return listener.?;
}

/// Server: waits for a client. With `AUTO_AFFINITY=1` the calling thread then moves to the server's CPU of the
/// preferred pair (affinity.zig).
pub fn accept(listener: *Listener, timeout_ms: c_int) Error!*Conn {
    var conn: ?*Conn = null;
    try check(api.fipc_stream_accept(listener, &conn, timeout_ms));
    affinity.pinSide(true);
    return conn.?;
}

pub fn closeListener(listener: *Listener) void {
    api.fipc_listener_close(listener);
}

/// Server: listens on `name`, accepts one client and stops listening (the connection stays open).
pub fn serve(name: [:0]const u8, capacity: usize, timeout_ms: c_int) Error!*Conn {
    const listener = try streamCreate(name, capacity);
    defer closeListener(listener);
    return accept(listener, timeout_ms);
}

/// Client: connects to the server on `name`, which may start listening later. With `AUTO_AFFINITY=1` the calling
/// thread then moves to the client's CPU of the preferred pair.
pub fn streamConnect(name: [:0]const u8, timeout_ms: c_int) Error!*Conn {
    var conn: ?*Conn = null;
    try check(api.fipc_stream_connect(name.ptr, &conn, timeout_ms));
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

// === Broadcasts ===

pub const subscriber_lossy: c_int = c.FIPC_SUBSCRIBER_LOSSY;
pub const subscriber_gated: c_int = c.FIPC_SUBSCRIBER_GATED;

/// Publisher: claims `name` for a broadcast of `capacity` bytes and `max_subscribers` subscribers; its connection.
pub fn broadcastCreate(name: [:0]const u8, capacity: usize, max_subscribers: u32, max_stall_ms: c_int) Error!*Conn {
    var publisher: ?*Conn = null;
    try check(api.fipc_broadcast_create(name.ptr, capacity, max_subscribers, max_stall_ms, &publisher));
    return publisher.?;
}

/// Publisher: waits until `at_least` subscribers are joined.
pub fn waitForSubscribers(publisher: *Conn, at_least: u32, timeout_ms: c_int) Error!void {
    var count: u32 = 0;
    return check(api.fipc_broadcast_wait_for_members(publisher, at_least, &count, timeout_ms));
}

/// Subscriber: joins the broadcast on `name` as `role` (`subscriber_lossy` or `subscriber_gated`).
pub fn broadcastJoin(name: [:0]const u8, role: c_int, timeout_ms: c_int) Error!*Conn {
    var conn: ?*Conn = null;
    try check(api.fipc_broadcast_join(name.ptr, role, &conn, timeout_ms));
    return conn.?;
}

// === Work queues ===

pub const producer: c_int = c.FIPC_PRODUCER;
pub const consumer: c_int = c.FIPC_CONSUMER;

/// Owner: claims `name` for a work queue of `max_producers` producers and `max_consumers` consumers, each producer with
/// a ring of `capacity` bytes; they join with `queueJoin`.
pub fn queueCreate(name: [:0]const u8, capacity: usize, max_producers: u32, max_consumers: u32) Error!*Queue {
    var queue: ?*Queue = null;
    try check(api.fipc_queue_create(name.ptr, capacity, max_producers, max_consumers, &queue));
    return queue.?;
}

/// Owner: waits until `at_least` members of either role are joined.
pub fn waitForMembers(queue: *Queue, at_least: u32, timeout_ms: c_int) Error!void {
    var count: u32 = 0;
    return check(api.fipc_queue_wait_for_members(queue, 0, at_least, &count, timeout_ms));
}

/// Owner: ends the queue.
pub fn closeQueue(queue: *Queue) void {
    api.fipc_queue_close(queue);
}

/// Producer or consumer: joins the queue on `name` as `role`.
pub fn queueJoin(name: [:0]const u8, role: c_int, timeout_ms: c_int) Error!*Conn {
    var conn: ?*Conn = null;
    try check(api.fipc_queue_join(name.ptr, role, &conn, timeout_ms));
    return conn.?;
}

// === Services ===

pub const client: c_int = c.FIPC_CLIENT;
pub const server: c_int = c.FIPC_SERVER;

/// The service's entry points; NoService if the library has none (a revision from before it).
fn serviceApi() error{NoService}!*const ServiceApi {
    if (service_api) |*service| return service;
    std.log.err("the library has no service (fipc_service_create): a revision from before it", .{});
    return error.NoService;
}

/// Owner: claims `name` for a service of `max_clients` clients, each with a ring of `capacity` bytes, and
/// `max_servers` servers, for requests and replies of up to `max_message` bytes; they join with `serviceJoin`.
pub fn serviceCreate(name: [:0]const u8, capacity: usize, max_message: usize, max_clients: u32, max_servers: u32) (Error || error{NoService})!*Service {
    var service: ?*Service = null;
    try check((try serviceApi()).fipc_service_create(name.ptr, capacity, max_message, max_clients, max_servers, &service));
    return service.?;
}

/// Owner: waits until `at_least` members of either role are joined.
pub fn waitForServiceMembers(service: *Service, at_least: u32, timeout_ms: c_int) (Error || error{NoService})!void {
    var count: u32 = 0;
    return check((try serviceApi()).fipc_service_wait_for_members(service, 0, at_least, &count, timeout_ms));
}

/// Owner: ends the service.
pub fn closeService(service: *Service) void {
    (serviceApi() catch unreachable).fipc_service_close(service); // serviceCreate made it
}

/// Client or server: joins the service on `name` as `role`.
pub fn serviceJoin(name: [:0]const u8, role: c_int, timeout_ms: c_int) (Error || error{NoService})!*Conn {
    var conn: ?*Conn = null;
    try check((try serviceApi()).fipc_service_join(name.ptr, role, &conn, timeout_ms));
    return conn.?;
}
