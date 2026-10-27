//! The exported C API (`include/fipc.h`) and the root of the shipped library. build.zig compiles this file into the
//! `fastipc` shared library and into `fastipc_static`, which the C-API tests link. Only the static variant carries the
//! lifecycle's test hooks (`test_hooks`, reachable from Zig only).
//!
//! Every `export fn` here is a thin wrapper. It checks the C arguments, calls the Zig implementation and turns its
//! errors into result codes. The implementation is in stream/, broadcast/, queue/ and service/ (each kind's creates and
//! joins),
//! core/endpoint.zig (a connection) and core/calls.zig (the data calls). The handles live on the C heap and use the
//! process-wide `Io` (core/process_state.zig).
//!
//! Comptime checks keep each export's signature, `Result` and the public constants in line with the header. The native
//! Zig API is root.zig (the module "fastipc").

const std = @import("std");
const builtin = @import("builtin");
const wire_protocol = @import("wire_protocol.zig");
// translate-c runs on include/fipc.h and converts it to a Zig module, "c_headers".
const c = @import("c_headers");
const broadcast_client = @import("broadcast/client.zig");
const broadcast_server = @import("broadcast/server.zig");
const calls = @import("core/calls.zig");
const endpoint_module = @import("core/endpoint.zig");
const queue_client = @import("queue/client.zig");
const queue_server = @import("queue/server.zig");
const service_client = @import("service/client.zig");
const service_server = @import("service/server.zig");
const stream_client = @import("stream/client.zig");
const stream_listener = @import("stream/listener.zig");
const stream_server = @import("stream/server.zig");

const Conn = endpoint_module.Endpoint;
const DataError = endpoint_module.DataError;
const Listener = stream_listener.Listener;
const Queue = queue_server.Queue;
const QueueRole = wire_protocol.QueueRole;
const Service = service_server.Service;
const ServiceRole = wire_protocol.ServiceRole;
const SubscriberRole = wire_protocol.SubscriberRole;

/// The C API's handles live on the C heap, on the process-wide `Io` (core/process_state.zig).
const gpa = std.heap.c_allocator;

/// How the zero-copy exports call the data path: inlined on Linux, where that makes 16-byte zero-copy messages 5-10%
/// faster, and called on Windows, where the same inlining makes them 7-9% slower. Nothing but speed depends on it.
const zero_copy_call: std.lang.CallModifier = if (builtin.os.tag == .linux) .always_inline else .auto;

/// std prints every `error.Unexpected` as a message and a stack trace in Debug builds.
/// The library handles the error itself, so it's better to suppress this to not pollute end user's stderr.
pub const std_options: std.Options = .{ .unexpected_error_tracing = false };

// === The result codes (non-exhaustive: C callers can pass any int) ===

/// `fipc_result_t`.
pub const Result = enum(c_int) {
    ok = 0,
    timeout = 1,
    disconnected = 2,
    cancelled = 3,
    too_large = 4,
    invalid = 5,
    no_memory = 6,
    addr_in_use = 7,
    overrun = 8,
    unsubscribed = 9,
    abandoned = 10,
    _,

    /// A data call's outcome as its result code: ok, or its error's code.
    fn of(outcome: DataError!void) Result {
        outcome catch |err| return .ofDataError(err);
        return .ok;
    }

    /// Out of line and cold: inlined, its switch becomes a jump table that every call goes through,
    /// success included.
    noinline fn ofDataError(err: DataError) Result {
        @branchHint(.cold);
        return switch (err) {
            error.Invalid => .invalid,
            error.Timeout => .timeout,
            error.TooLarge => .too_large,
            error.Disconnected => .disconnected,
            error.Cancelled => .cancelled,
            error.Overrun => .overrun,
            error.Unsubscribed => .unsubscribed,
            error.Abandoned => .abandoned,
        };
    }

    noinline fn ofLifecycleError(err: LifecycleError) Result {
        @branchHint(.cold);
        return switch (err) {
            error.Invalid => .invalid,
            error.NoMemory => .no_memory,
            error.Timeout => .timeout,
            error.AddrInUse => .addr_in_use,
            error.Cancelled => .cancelled,
            error.Disconnected => .disconnected,
        };
    }
};

/// The errors of every lifecycle call: the creates, `fipc_stream_accept`, the joins and the waits for members. Each
/// maps to the result code of the same name.
const LifecycleError = endpoint_module.CreateError || endpoint_module.Error || queue_server.WaitError || service_server.WaitError;

// === Compile-time checks against include/fipc.h, as compiled for this target ===

comptime {
    expectEnum(Result, c.fipc_result_t, .{
        .{ .ok, c.FIPC_OK },
        .{ .timeout, c.FIPC_TIMEOUT },
        .{ .disconnected, c.FIPC_DISCONNECTED },
        .{ .cancelled, c.FIPC_CANCELLED },
        .{ .too_large, c.FIPC_TOO_LARGE },
        .{ .invalid, c.FIPC_INVALID },
        .{ .no_memory, c.FIPC_NO_MEMORY },
        .{ .addr_in_use, c.FIPC_ADDR_IN_USE },
        .{ .overrun, c.FIPC_OVERRUN },
        .{ .unsubscribed, c.FIPC_UNSUBSCRIBED },
        .{ .abandoned, c.FIPC_ABANDONED },
    });
    // The data path's timeouts: 0 doesn't wait, a negative one waits for ever
    expectValue("FIPC_NO_WAIT", 0, c.FIPC_NO_WAIT);
    expectValue("FIPC_FOREVER", -1, c.FIPC_FOREVER);
    // The roles, which JOIN frames carry too
    expectValue("FIPC_SUBSCRIBER_LOSSY", @backingInt(SubscriberRole.lossy), c.FIPC_SUBSCRIBER_LOSSY);
    expectValue("FIPC_SUBSCRIBER_GATED", @backingInt(SubscriberRole.gated), c.FIPC_SUBSCRIBER_GATED);
    expectValue("FIPC_PRODUCER", @backingInt(QueueRole.producer), c.FIPC_PRODUCER);
    expectValue("FIPC_CONSUMER", @backingInt(QueueRole.consumer), c.FIPC_CONSUMER);
    expectValue("FIPC_CLIENT", @backingInt(ServiceRole.client), c.FIPC_CLIENT);
    expectValue("FIPC_SERVER", @backingInt(ServiceRole.server), c.FIPC_SERVER);
}

comptime {
    for ([_][]const u8{
        "fipc_result_str",
        "fipc_stream_create",
        "fipc_stream_accept",
        "fipc_listener_cancel",
        "fipc_listener_close",
        "fipc_stream_connect",
        "fipc_max_piece",
        "fipc_cancel",
        "fipc_close",
        "fipc_send",
        "fipc_recv",
        "fipc_send_many",
        "fipc_recv_many",
        "fipc_send_acquire",
        "fipc_send_commit",
        "fipc_recv_acquire",
        "fipc_recv_release",
        "fipc_broadcast_create",
        "fipc_broadcast_join",
        "fipc_broadcast_wait_for_members",
        "fipc_queue_create",
        "fipc_queue_join",
        "fipc_queue_wait_for_members",
        "fipc_queue_cancel",
        "fipc_queue_close",
        "fipc_service_create",
        "fipc_service_join",
        "fipc_service_wait_for_members",
        "fipc_service_cancel",
        "fipc_service_close",
    }) |name| {
        expectSameSignature(
            name,
            @TypeOf(@field(@This(), name)),
            @TypeOf(@field(c, name)),
        );
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
        .overrun => "FIPC_OVERRUN",
        .unsubscribed => "FIPC_UNSUBSCRIBED",
        .abandoned => "FIPC_ABANDONED",
        _ => "FIPC_UNKNOWN",
    };
}

// === Streams ===

export fn fipc_stream_create(name: ?[*:0]const u8, capacity: usize, out_listener: ?*?*Listener) callconv(.c) Result {
    const name_pointer = name orelse return .invalid;
    const out = out_listener orelse return .invalid;
    out.* = stream_server.create(
        gpa,
        std.mem.span(name_pointer),
        capacity,
    ) catch |err| return .ofLifecycleError(err);
    return .ok;
}

export fn fipc_stream_accept(listener: ?*Listener, out_conn: ?*?*Conn, timeout_ms: c_int) callconv(.c) Result {
    const listener_handle = listener orelse return .invalid;
    const out = out_conn orelse return .invalid;
    out.* = stream_server.accept(
        listener_handle,
        msTimeout(timeout_ms),
    ) catch |err| return .ofLifecycleError(err);
    return .ok;
}

export fn fipc_listener_cancel(listener: ?*Listener) callconv(.c) void {
    (listener orelse return).cancel();
}

export fn fipc_listener_close(listener: ?*Listener) callconv(.c) void {
    (listener orelse return).close();
}

export fn fipc_stream_connect(name: ?[*:0]const u8, out_conn: ?*?*Conn, timeout_ms: c_int) callconv(.c) Result {
    const name_pointer = name orelse return .invalid;
    const out = out_conn orelse return .invalid;
    out.* = stream_client.connect(
        gpa,
        std.mem.span(name_pointer),
        msTimeout(timeout_ms),
    ) catch |err| return .ofLifecycleError(err);
    return .ok;
}

// === Connections ===

export fn fipc_max_piece(conn: ?*const Conn) callconv(.c) usize {
    return calls.maxPiece(conn orelse return 0);
}

export fn fipc_cancel(conn: ?*Conn) callconv(.c) void {
    endpoint_module.cancel(conn orelse return);
}

export fn fipc_close(conn: ?*Conn) callconv(.c) void {
    endpoint_module.close(conn orelse return);
}

// === Messages ===

export fn fipc_send(conn: ?*Conn, data: ?*const anyopaque, len: usize, timeout_ms: c_int) callconv(.c) Result {
    const connection = conn orelse return .invalid;
    const message = cBytes(data, len) orelse return .invalid;
    return .of(calls.send(
        connection,
        message,
        timeout_ms,
    ));
}

export fn fipc_recv(conn: ?*Conn, buf: ?*anyopaque, buf_len: usize, out_len: ?*usize, timeout_ms: c_int) callconv(.c) Result {
    const connection = conn orelse return .invalid;
    const len = out_len orelse return .invalid;
    const into = cBuffer(buf, buf_len) orelse return .invalid;
    return .of(calls.recv(connection, into, timeout_ms, len));
}

/// `*out_sent` is set on every return, once the pointer is there; every check comes before anything is sent.
export fn fipc_send_many(
    conn: ?*Conn,
    datas: ?[*]const ?*const anyopaque,
    lens: ?[*]const usize,
    count: usize,
    out_sent: ?*usize,
    timeout_ms: c_int,
) callconv(.c) Result {
    const sent = out_sent orelse return .invalid;
    sent.* = 0;
    const connection = conn orelse return .invalid;
    const batch: calls.CBatch = .{ .datas = datas orelse return .invalid, .lens = lens orelse return .invalid, .count = count };
    return .of(calls.sendMany(connection, batch, timeout_ms, sent));
}

/// `*out_count` is set on every return, once the pointer is there.
export fn fipc_recv_many(
    conn: ?*Conn,
    buf: ?*anyopaque,
    buf_len: usize,
    lens: ?[*]usize,
    max_count: usize,
    out_count: ?*usize,
    timeout_ms: c_int,
) callconv(.c) Result {
    const count = out_count orelse return .invalid;
    count.* = 0;
    const connection = conn orelse return .invalid;
    const into = cBuffer(buf, buf_len) orelse return .invalid;
    const lengths = lens orelse return .invalid;
    return .of(calls.recvMany(connection, into, lengths[0..max_count], timeout_ms, count));
}

/// A NULL `out_buf` is refused before anything is reserved, and so is an empty reservation, which no commit
/// could use.
export fn fipc_send_acquire(conn: ?*Conn, len: usize, out_buf: ?*?*anyopaque, timeout_ms: c_int) callconv(.c) Result {
    const connection = conn orelse return .invalid;
    const out = out_buf orelse return .invalid;
    if (len == 0) return .invalid;
    var buffer: [*]u8 = undefined;
    @call(
        zero_copy_call,
        calls.sendAcquire,
        .{ connection, len, timeout_ms, &buffer },
    ) catch |err| return .ofDataError(err);
    out.* = buffer;
    return .ok;
}

export fn fipc_send_commit(conn: ?*Conn, len: usize) callconv(.c) Result {
    return .of(calls.sendCommit(conn orelse return .invalid, len));
}

export fn fipc_recv_acquire(conn: ?*Conn, out_data: ?*?*const anyopaque, out_len: ?*usize, timeout_ms: c_int) callconv(.c) Result {
    const connection = conn orelse return .invalid;
    const out = out_data orelse return .invalid;
    const len = out_len orelse return .invalid;
    var data: [*]const u8 = undefined;
    @call(
        zero_copy_call,
        calls.recvAcquire,
        .{ connection, timeout_ms, &data, len },
    ) catch |err| return .ofDataError(err);
    out.* = data;
    return .ok;
}

export fn fipc_recv_release(conn: ?*Conn) callconv(.c) void {
    calls.recvRelease(conn orelse return);
}

// === Broadcasts ===

export fn fipc_broadcast_create(
    name: ?[*:0]const u8,
    capacity: usize,
    max_subscribers: u32,
    max_stall_ms: c_int,
    out_publisher: ?*?*Conn,
) callconv(.c) Result {
    const name_pointer = name orelse return .invalid;
    const out = out_publisher orelse return .invalid;
    out.* = broadcast_server.create(
        gpa,
        std.mem.span(name_pointer),
        capacity,
        max_subscribers,
        max_stall_ms,
    ) catch |err|
        return .ofLifecycleError(err);
    return .ok;
}

export fn fipc_broadcast_join(name: ?[*:0]const u8, role: c_int, out_conn: ?*?*Conn, timeout_ms: c_int) callconv(.c) Result {
    const name_pointer = name orelse return .invalid;
    const out = out_conn orelse return .invalid;
    const subscriber_role = std.enums.fromInt(SubscriberRole, role) orelse return .invalid;
    out.* = broadcast_client.join(
        gpa,
        std.mem.span(name_pointer),
        subscriber_role,
        msTimeout(timeout_ms),
    ) catch |err| return .ofLifecycleError(err);
    return .ok;
}

export fn fipc_broadcast_wait_for_members(
    publisher: ?*Conn,
    at_least: u32,
    out_count: ?*u32,
    timeout_ms: c_int,
) callconv(.c) Result {
    const connection = publisher orelse return .invalid;
    const out = out_count orelse return .invalid;
    return .of(calls.waitForMembers(
        connection,
        at_least,
        timeout_ms,
        out,
    ));
}

// === Work queues ===

export fn fipc_queue_create(
    name: ?[*:0]const u8,
    capacity: usize,
    max_producers: u32,
    max_consumers: u32,
    out_queue: ?*?*Queue,
) callconv(.c) Result {
    const name_pointer = name orelse return .invalid;
    const out = out_queue orelse return .invalid;
    out.* = queue_server.create(
        gpa,
        std.mem.span(name_pointer),
        capacity,
        max_producers,
        max_consumers,
    ) catch |err|
        return .ofLifecycleError(err);
    return .ok;
}

export fn fipc_queue_join(name: ?[*:0]const u8, role: c_int, out_conn: ?*?*Conn, timeout_ms: c_int) callconv(.c) Result {
    const name_pointer = name orelse return .invalid;
    const out = out_conn orelse return .invalid;
    const member_role = std.enums.fromInt(QueueRole, role) orelse return .invalid;
    out.* = queue_client.join(
        gpa,
        std.mem.span(name_pointer),
        member_role,
        msTimeout(timeout_ms),
    ) catch |err| return .ofLifecycleError(err);
    return .ok;
}

/// `role` 0 counts both roles.
export fn fipc_queue_wait_for_members(queue: ?*Queue, role: c_int, at_least: u32, out_count: ?*u32, timeout_ms: c_int) callconv(.c) Result {
    const queue_handle = queue orelse return .invalid;
    const out = out_count orelse return .invalid;
    const counted_role: ?QueueRole = if (role == 0) null else std.enums.fromInt(QueueRole, role) orelse return .invalid;
    queue_server.waitForMembers(
        queue_handle,
        counted_role,
        at_least,
        timeout_ms,
        out,
    ) catch |err| return .ofLifecycleError(err);
    return .ok;
}

export fn fipc_queue_cancel(queue: ?*Queue) callconv(.c) void {
    queue_server.cancel(queue orelse return);
}

export fn fipc_queue_close(queue: ?*Queue) callconv(.c) void {
    queue_server.close(queue orelse return);
}

// === Services ===

export fn fipc_service_create(
    name: ?[*:0]const u8,
    capacity: usize,
    max_message: usize,
    max_clients: u32,
    max_servers: u32,
    out_service: ?*?*Service,
) callconv(.c) Result {
    const name_pointer = name orelse return .invalid;
    const out = out_service orelse return .invalid;
    out.* = service_server.create(
        gpa,
        std.mem.span(name_pointer),
        capacity,
        max_message,
        max_clients,
        max_servers,
    ) catch |err|
        return .ofLifecycleError(err);
    return .ok;
}

export fn fipc_service_join(name: ?[*:0]const u8, role: c_int, out_conn: ?*?*Conn, timeout_ms: c_int) callconv(.c) Result {
    const name_pointer = name orelse return .invalid;
    const out = out_conn orelse return .invalid;
    const member_role = std.enums.fromInt(ServiceRole, role) orelse return .invalid;
    out.* = service_client.join(
        gpa,
        std.mem.span(name_pointer),
        member_role,
        msTimeout(timeout_ms),
    ) catch |err| return .ofLifecycleError(err);
    return .ok;
}

/// `role` 0 counts both roles.
export fn fipc_service_wait_for_members(service: ?*Service, role: c_int, at_least: u32, out_count: ?*u32, timeout_ms: c_int) callconv(.c) Result {
    const service_handle = service orelse return .invalid;
    const out = out_count orelse return .invalid;
    const counted_role: ?ServiceRole = if (role == 0) null else std.enums.fromInt(ServiceRole, role) orelse return .invalid;
    service_server.waitForMembers(
        service_handle,
        counted_role,
        at_least,
        timeout_ms,
        out,
    ) catch |err| return .ofLifecycleError(err);
    return .ok;
}

export fn fipc_service_cancel(service: ?*Service) callconv(.c) void {
    service_server.cancel(service orelse return);
}

export fn fipc_service_close(service: ?*Service) callconv(.c) void {
    service_server.close(service orelse return);
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

// === The checks' helpers ===

/// An exported function has its C prototype's shape: C calling convention, the same number of
/// parameters, and each parameter and the result of the same size and kind (void, bool, integer
/// or enum, pointer — optional or not, since C pointers may be NULL — or float).
fn expectSameSignature(comptime name: []const u8, comptime Zig: type, comptime C: type) void {
    const zig_fn = @typeInfo(Zig).@"fn";
    const c_fn = @typeInfo(C).@"fn";
    if (!zig_fn.attrs.@"callconv".eql(.c)) fail("{s}: not callconv(.c)", .{name});
    if (zig_fn.param_types.len != c_fn.param_types.len)
        fail("{s}: {d} parameters, the C header has {d}", .{ name, zig_fn.param_types.len, c_fn.param_types.len });
    inline for (zig_fn.param_types, c_fn.param_types, 0..) |zig_param, c_param, i| {
        const zig_shape = shapeOf(zig_param.?);
        const c_shape = shapeOf(c_param.?);
        if (zig_shape.kind != c_shape.kind or zig_shape.size != c_shape.size)
            fail("{s}: parameter {d} is {s} ({d} bytes), the C header has {s} ({d} bytes)", .{
                name, i, @tagName(zig_shape.kind), zig_shape.size, @tagName(c_shape.kind), c_shape.size,
            });
    }
    const zig_result = shapeOf(zig_fn.return_type.?);
    const c_result = shapeOf(c_fn.return_type.?);
    if (zig_result.kind != c_result.kind or zig_result.size != c_result.size)
        fail("{s}: returns {s} ({d} bytes), the C header says {s} ({d} bytes)", .{
            name, @tagName(zig_result.kind), zig_result.size, @tagName(c_result.kind), c_result.size,
        });
}

const Shape = struct { kind: enum { void, bool, int, pointer, float }, size: usize };

fn shapeOf(comptime T: type) Shape {
    return switch (@typeInfo(T)) {
        .void => .{ .kind = .void, .size = 0 },
        .bool => .{ .kind = .bool, .size = 1 },
        .int, .@"enum" => .{ .kind = .int, .size = @sizeOf(T) },
        .float => .{ .kind = .float, .size = @sizeOf(T) },
        .pointer => .{ .kind = .pointer, .size = @sizeOf(T) },
        .optional => |o| switch (@typeInfo(o.child)) {
            .pointer => .{ .kind = .pointer, .size = @sizeOf(T) },
            else => fail("unsupported optional {s}", .{@typeName(T)}),
        },
        else => fail("unsupported parameter type {s}", .{@typeName(T)}),
    };
}

/// The C enum's size and alignment, and one C constant per named tag with the same value.
fn expectEnum(comptime E: type, comptime C: type, comptime pairs: anytype) void {
    const name = @typeName(E);
    if (@sizeOf(E) != @sizeOf(C) or @alignOf(E) != @alignOf(C))
        fail("{s}: size/alignment {d}/{d}, the C enum has {d}/{d}", .{ name, @sizeOf(E), @alignOf(E), @sizeOf(C), @alignOf(C) });
    if (pairs.len != @typeInfo(E).@"enum".field_names.len)
        fail("{s}: {d} named tags, but {d} C constants are listed", .{ name, @typeInfo(E).@"enum".field_names.len, pairs.len });
    inline for (pairs) |pair| {
        const tag: E = pair[0];
        if (@backingInt(tag) != pair[1])
            fail("{s}.{s} = {d}, the C constant is {d}", .{ name, @tagName(tag), @backingInt(tag), pair[1] });
    }
}

fn expectValue(comptime c_name: []const u8, comptime value: anytype, comptime c_value: anytype) void {
    if (value != c_value) fail("{s}: {d} in Zig, {d} in the C header", .{ c_name, value, c_value });
}

fn fail(comptime fmt: []const u8, comptime args: anytype) noreturn {
    @compileError(std.fmt.comptimePrint("ABI check: " ++ fmt, args));
}

test {
    // The unit tests of every module.
    _ = @import("platform.zig");
    _ = @import("root.zig");
    _ = @import("core/admission.zig");
    _ = @import("core/calls.zig");
    _ = @import("core/control.zig");
    _ = @import("core/copy.zig");
    _ = @import("core/endpoint.zig");
    _ = @import("core/endpoint_test.zig");
    _ = @import("core/litmus.zig");
    _ = @import("core/log.zig");
    _ = @import("core/members.zig");
    _ = @import("core/names.zig");
    _ = @import("core/payload_ring.zig");
    _ = @import("core/position_record.zig");
    _ = @import("core/ring_set.zig");
    _ = @import("core/roster.zig");
    _ = @import("core/sender.zig");
    _ = @import("core/process_state.zig");
    _ = @import("core/shared_memory.zig");
    _ = @import("core/taker.zig");
    _ = @import("core/wait.zig");
    _ = @import("core/wire.zig");
    _ = @import("stream/client.zig");
    _ = @import("stream/data_path.zig");
    _ = @import("stream/data_path_test.zig");
    _ = @import("stream/frames.zig");
    _ = @import("stream/handshake_test.zig");
    _ = @import("stream/listener.zig");
    _ = @import("stream/ring.zig");
    _ = @import("stream/ring_wait.zig");
    _ = @import("stream/segment.zig");
    _ = @import("stream/server.zig");
    _ = @import("broadcast/client.zig");
    _ = @import("broadcast/frames.zig");
    _ = @import("broadcast/reader.zig");
    _ = @import("broadcast/roster.zig");
    _ = @import("broadcast/segment.zig");
    _ = @import("broadcast/server.zig");
    _ = @import("broadcast/writer.zig");
    _ = @import("broadcast/writer_reader_test.zig");
    _ = @import("broadcast/litmus_test.zig");
    _ = @import("queue/client.zig");
    _ = @import("queue/consumer.zig");
    _ = @import("queue/frames.zig");
    _ = @import("queue/litmus_test.zig");
    _ = @import("queue/producer.zig");
    _ = @import("queue/producer_consumer_test.zig");
    _ = @import("queue/returns.zig");
    _ = @import("queue/segment.zig");
    _ = @import("queue/server.zig");
    _ = @import("service/caller.zig");
    _ = @import("service/caller_responder_test.zig");
    _ = @import("service/client.zig");
    _ = @import("service/frames.zig");
    _ = @import("service/litmus_test.zig");
    _ = @import("service/responder.zig");
    _ = @import("service/segment.zig");
    _ = @import("service/server.zig");
}

test "each error becomes the result code of the same name" {
    inline for (@typeInfo(DataError).error_set.error_names.?) |e| {
        try std.testing.expectEqualStrings(comptime snakeCase(e), @tagName(Result.ofDataError(@field(DataError, e))));
    }
    inline for (@typeInfo(endpoint_module.Error).error_set.error_names.?) |e| {
        try std.testing.expectEqualStrings(comptime snakeCase(e), @tagName(Result.ofLifecycleError(@field(endpoint_module.Error, e))));
    }
    inline for (@typeInfo(LifecycleError).error_set.error_names.?) |e| {
        try std.testing.expectEqualStrings(comptime snakeCase(e), @tagName(Result.ofLifecycleError(@field(LifecycleError, e))));
    }
}

fn snakeCase(comptime name: []const u8) []const u8 {
    var out: []const u8 = "";
    for (name, 0..) |char, i| {
        if (std.ascii.isUpper(char) and i > 0) out = out ++ "_";
        out = out ++ [_]u8{std.ascii.toLower(char)};
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
    const linux = if (builtin.os.tag == .macos) @import("platform/macos/test_sys.zig") else std.os.linux;
    const platform = @import("platform.zig");
    const waitChild = @import("core/process_state.zig").waitChild;

    /// Comptime-known, so that the rest of the test isn't even analyzed where it can't run.
    const fork_unsupported = !platform.has_fork or builtin.sanitize_thread;

    test "slow: fork: calls on an inherited connection and listener return INVALID, and the first logs why, once" {
        if (fork_unsupported) return error.SkipZigTest;
        var name_buf: [64]u8 = undefined;
        const name = try std.mem.printSentinel(&name_buf, "test-c-api-fork-{d}", .{platform.pid()}, 0);
        var listener: ?*Listener = null;
        try std.testing.expectEqual(Result.ok, fipc_stream_create(name, 4096, &listener));
        defer fipc_listener_close(listener);
        // The client connects on another thread: `fipc_stream_connect` waits for the listener's `fipc_stream_accept`
        const Client = struct {
            fn run(server_name: [*:0]const u8, conn: *?*Conn, result: *Result) void {
                result.* = fipc_stream_connect(server_name, conn, 5000);
            }
        };
        var client: ?*Conn = null;
        var connected: Result = .timeout;
        const thread = try std.Thread.spawn(.{}, Client.run, .{ name, &client, &connected });
        var server: ?*Conn = null;
        const accepted = fipc_stream_accept(listener, &server, 5000);
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
                if (fipc_stream_accept(listener, &taken, 0) != .invalid) break :code 3;
                if (fipc_send(server, "x", 1, 0) != .invalid) break :code 4;
                break :code 0;
            };
            fipc_close(client);
            fipc_close(server);
            fipc_listener_close(listener);
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
            const count = linux.read(stderr_pipe[0], output[len..].ptr, output.len - len);
            if (linux.errno(count) != .SUCCESS or count == 0) break;
            len += count;
        }
        // Its exit within 5 s, or it hung: killed
        try std.testing.expectEqual(@as(?u8, 0), waitChild(pid, 5000));
        const written = output[0..len];
        try std.testing.expectEqual(@as(usize, 1), std.mem.count(u8, written, "inherited through fork()"));
        try std.testing.expect(std.mem.find(u8, written, "a call on a connection inherited through fork() failed") != null);
    }

    test "slow: fork: calls on an inherited publisher and subscriber return INVALID; the parent's broadcast goes on" {
        if (fork_unsupported) return error.SkipZigTest;
        var name_buf: [64]u8 = undefined;
        const name = try std.mem.printSentinel(&name_buf, "test-c-api-broadcast-fork-{d}", .{platform.pid()}, 0);
        var writer: ?*Conn = null;
        try std.testing.expectEqual(Result.ok, fipc_broadcast_create(name, 4096, 2, -1, &writer));
        defer fipc_close(writer);
        var subscribers: [2]?*Conn = .{ null, null };
        defer for (subscribers) |subscriber| fipc_close(subscriber);
        for (&subscribers, [_]c_int{ c.FIPC_SUBSCRIBER_LOSSY, c.FIPC_SUBSCRIBER_GATED }) |*subscriber, role| {
            try std.testing.expectEqual(Result.ok, fipc_broadcast_join(name, role, subscriber, 5000));
        }
        const lossy, const gated = subscribers;

        const pid = std.c.fork();
        if (pid == 0) {
            var buf: [16]u8 = undefined;
            var len: usize = 0;
            var out: ?*anyopaque = null;
            var in: ?*const anyopaque = null;
            const code: u8 = code: {
                if (fipc_send(writer, "x", 1, 0) != .invalid) break :code 1;
                if (fipc_recv(lossy, &buf, buf.len, &len, 0) != .invalid) break :code 2;
                if (fipc_max_piece(writer) != 0 or fipc_max_piece(lossy) != 0) break :code 4;
                if (fipc_send_acquire(writer, 1, &out, 0) != .invalid) break :code 5;
                if (fipc_send_commit(writer, 1) != .invalid) break :code 6;
                if (fipc_recv_acquire(gated, &in, &len, 0) != .invalid) break :code 7;
                if (fipc_recv(gated, &buf, buf.len, &len, 0) != .invalid) break :code 8;
                var count: u32 = 0;
                if (fipc_broadcast_wait_for_members(writer, 0, &count, 0) != .invalid) break :code 9;
                fipc_recv_release(gated);
                fipc_cancel(lossy);
                fipc_cancel(gated);
                fipc_cancel(writer);
                break :code 0;
            };
            for (subscribers) |subscriber| fipc_close(subscriber);
            fipc_close(writer);
            std.c._exit(code);
        }
        try std.testing.expect(pid > 0);
        try std.testing.expectEqual(@as(?u8, 0), waitChild(pid, 5000));
        // The parent's broadcast saw nothing of the child's closes: the gated subscriber's bit holds the publisher still
        try std.testing.expectEqual(Result.ok, fipc_send(writer, "after", 5, 0));
        var buf: [16]u8 = undefined;
        var len: usize = 0;
        try std.testing.expectEqual(Result.ok, fipc_recv(lossy, &buf, buf.len, &len, 1000));
        try std.testing.expectEqualStrings("after", buf[0..len]);
        var in: ?*const anyopaque = null;
        try std.testing.expectEqual(Result.ok, fipc_recv_acquire(gated, &in, &len, 1000));
        try std.testing.expectEqualStrings("after", @as([*]const u8, @ptrCast(in.?))[0..len]);
        fipc_recv_release(gated);
        for (0..64) |_| try std.testing.expectEqual(Result.ok, fipc_send(writer, "x", 1, 0));
        try std.testing.expectEqual(Result.timeout, fipc_send(writer, "x", 1, 0));
    }

    test "slow: fork: calls on an inherited queue, producer and consumer return INVALID; the parent's queue goes on" {
        if (fork_unsupported) return error.SkipZigTest;
        var name_buf: [64]u8 = undefined;
        const name = try std.mem.printSentinel(&name_buf, "test-c-api-queue-fork-{d}", .{platform.pid()}, 0);
        var queue: ?*Queue = null;
        try std.testing.expectEqual(Result.ok, fipc_queue_create(name, 4096, 2, 2, &queue));
        defer fipc_queue_close(queue);
        var own: ?*Conn = null;
        try std.testing.expectEqual(Result.ok, fipc_queue_join(name, c.FIPC_PRODUCER, &own, 5000));
        defer fipc_close(own);
        var members: [2]?*Conn = .{ null, null };
        defer for (members) |member| fipc_close(member);
        for (&members, [_]c_int{ c.FIPC_PRODUCER, c.FIPC_CONSUMER }) |*member, role| {
            try std.testing.expectEqual(Result.ok, fipc_queue_join(name, role, member, 5000));
        }
        const joined_producer, const consumer = members;
        try std.testing.expectEqual(Result.ok, fipc_send(own, "before", 6, 0));

        const pid = std.c.fork();
        if (pid == 0) {
            var buf: [16]u8 = undefined;
            var len: usize = 0;
            var out: ?*anyopaque = null;
            var in: ?*const anyopaque = null;
            const code: u8 = code: {
                if (fipc_send(own, "x", 1, 0) != .invalid) break :code 1;
                if (fipc_send(joined_producer, "x", 1, 0) != .invalid) break :code 2;
                if (fipc_recv(consumer, &buf, buf.len, &len, 0) != .invalid) break :code 3;
                if (fipc_recv_acquire(consumer, &in, &len, 0) != .invalid) break :code 4;
                var count: u32 = 0;
                if (fipc_queue_wait_for_members(queue, 0, 0, &count, 0) != .invalid) break :code 5;
                if (fipc_max_piece(own) != 0 or fipc_max_piece(consumer) != 0) break :code 6;
                if (fipc_send_acquire(joined_producer, 1, &out, 0) != .invalid) break :code 7;
                if (fipc_send_commit(joined_producer, 1) != .invalid) break :code 8;
                fipc_recv_release(consumer);
                fipc_cancel(own);
                fipc_cancel(consumer);
                fipc_queue_cancel(queue);
                break :code 0;
            };
            for (members) |member| fipc_close(member);
            fipc_close(own);
            fipc_queue_close(queue);
            std.c._exit(code);
        }
        try std.testing.expect(pid > 0);
        try std.testing.expectEqual(@as(?u8, 0), waitChild(pid, 5000));
        // The parent's queue saw nothing of the child's closes: it goes on
        try std.testing.expectEqual(Result.ok, fipc_send(joined_producer, "after", 5, 0));
        var buf: [16]u8 = undefined;
        var len: usize = 0;
        try std.testing.expectEqual(Result.ok, fipc_recv(consumer, &buf, buf.len, &len, 1000));
        try std.testing.expectEqualStrings("before", buf[0..len]);
        try std.testing.expectEqual(Result.ok, fipc_recv(consumer, &buf, buf.len, &len, 1000));
        try std.testing.expectEqualStrings("after", buf[0..len]);
        // Its members are all still joined, and a wait for them still works
        var count: u32 = 0;
        try std.testing.expectEqual(Result.ok, fipc_queue_wait_for_members(queue, 0, 3, &count, 1000));
        try std.testing.expectEqual(3, count);
    }

    test "slow: fork: calls on an inherited service, client and server return INVALID; the parent's service goes on" {
        if (fork_unsupported) return error.SkipZigTest;
        var name_buf: [64]u8 = undefined;
        const name = try std.mem.printSentinel(&name_buf, "test-c-api-service-fork-{d}", .{platform.pid()}, 0);
        var service: ?*Service = null;
        try std.testing.expectEqual(Result.ok, fipc_service_create(name, 4096, 64, 1, 1, &service));
        defer fipc_service_close(service);
        var members: [2]?*Conn = .{ null, null };
        defer for (members) |member| fipc_close(member);
        for (&members, [_]c_int{ c.FIPC_CLIENT, c.FIPC_SERVER }) |*member, role| {
            try std.testing.expectEqual(Result.ok, fipc_service_join(name, role, member, 5000));
        }
        const asker, const answerer = members;
        try std.testing.expectEqual(Result.ok, fipc_send(asker, "before", 6, 0));

        const pid = std.c.fork();
        if (pid == 0) {
            var buf: [16]u8 = undefined;
            var len: usize = 0;
            var out: ?*anyopaque = null;
            var in: ?*const anyopaque = null;
            const code: u8 = code: {
                if (fipc_send(asker, "x", 1, 0) != .invalid) break :code 1;
                if (fipc_recv(asker, &buf, buf.len, &len, 0) != .invalid) break :code 2;
                if (fipc_recv(answerer, &buf, buf.len, &len, 0) != .invalid) break :code 3;
                if (fipc_recv_acquire(answerer, &in, &len, 0) != .invalid) break :code 4;
                if (fipc_send(answerer, "x", 1, 0) != .invalid) break :code 5;
                var count: u32 = 0;
                if (fipc_service_wait_for_members(service, 0, 0, &count, 0) != .invalid) break :code 6;
                if (fipc_max_piece(asker) != 0 or fipc_max_piece(answerer) != 0) break :code 7;
                if (fipc_send_acquire(asker, 1, &out, 0) != .invalid) break :code 8;
                fipc_recv_release(asker);
                fipc_cancel(asker);
                fipc_cancel(answerer);
                fipc_service_cancel(service);
                break :code 0;
            };
            for (members) |member| fipc_close(member);
            fipc_service_close(service);
            std.c._exit(code);
        }
        try std.testing.expect(pid > 0);
        try std.testing.expectEqual(@as(?u8, 0), waitChild(pid, 5000));
        // The parent's service saw nothing of the child's closes: it goes on
        var buf: [16]u8 = undefined;
        var len: usize = 0;
        try std.testing.expectEqual(Result.ok, fipc_recv(answerer, &buf, buf.len, &len, 1000));
        try std.testing.expectEqualStrings("before", buf[0..len]);
        try std.testing.expectEqual(Result.ok, fipc_send(answerer, "after", 5, 0));
        try std.testing.expectEqual(Result.ok, fipc_recv(asker, &buf, buf.len, &len, 1000));
        try std.testing.expectEqualStrings("after", buf[0..len]);
        var count: u32 = 0;
        try std.testing.expectEqual(Result.ok, fipc_service_wait_for_members(service, 0, 2, &count, 1000));
        try std.testing.expectEqual(2, count);
    }
};
