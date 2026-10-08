//! The C ABI of the shipped library (`include/fipc.h`) and the protocol's wire formats (docs/protocol.md), as Zig
//! `extern` types and constants.
//!
//! The comptime block at the end checks, in every build:
//!   1. the public mirrors (`fipc_rpc_msg_t`, `fipc_result_t` and the constants) against the header, translated by the
//!      build into the `c_headers` module;
//!   2. the wire formats, which two builds of this library share: the control frames, the segment's header, the
//!      rings' frame header and the RPC header. Their layouts are private, but a drift would corrupt a peer, so it
//!      fails the build. They are little-endian, the byte order of the one supported architecture (x86-64).

const std = @import("std");
const c = @import("c_headers");

const Atomic = std.atomic.Value;

// === The protocol's version (docs/protocol.md §8) ===

/// The version of the protocol: the control frames' `version` and the digit of the segment's magic. Two builds that
/// differ in any wire format differ in it, and refuse each other.
pub const protocol_version: u8 = 1;

/// The segment's magic, the bytes "FIPCSEG" and the version's digit, in `SegmentHeader.magic`.
pub const segment_magic: u64 = std.mem.readInt(u64, "FIPCSEG" ++ .{'0' + protocol_version}, .little);

// === The control connection (docs/protocol.md §3.1) ===

/// The layout of the control connection's two 64-byte frames, SEGMENT and READY (lifecycle/wire.zig). READY uses the
/// header (`magic` to `pid`) and leaves the rest 0. Reserved bytes are written as 0 and ignored on read.
pub const ControlFrame = extern struct {
    /// "FIPC", the same in every protocol version.
    magic: u32,
    /// 1 SEGMENT, 2 READY.
    frame_type: u8,
    /// `protocol_version`.
    version: u8,
    reserved0: [2]u8 = @splat(0),
    /// The sender's process id, for logs only.
    pid: u32,
    reserved1: [4]u8 = @splat(0),
    /// SEGMENT: the ring capacity the server chose.
    capacity: u64 = 0,
    /// SEGMENT: the segment's size in bytes.
    segment_size: u64 = 0,
    /// SEGMENT: the session id.
    session_id: [16]u8 = @splat(0),
    reserved2: [16]u8 = @splat(0),
};

// === Shared memory (docs/protocol.md §4, §5) ===

/// The writer's half of one ring's indices, on a cache line of its own: the index it publishes, and the sleep flag of
/// the reader that waits for it, so that a publish (store the index, swap the flag) touches one line.
pub const RingWriter = extern struct {
    /// The bytes the writer has published (a monotonic byte count).
    head: Atomic(u64),
    /// The reader's sleep flag: the reader sets it to 1 before it sleeps; whoever swaps the 1 back to 0 wakes it.
    reader_sleeps: Atomic(u32),
    pad: [52]u8,
};

/// The reader's half of one ring's indices, on a cache line of its own.
pub const RingReader = extern struct {
    /// The bytes the reader has consumed (a monotonic byte count).
    tail: Atomic(u64),
    /// The writer's sleep flag: the writer sets it to 1 before it sleeps; whoever swaps the 1 back to 0 wakes it.
    writer_sleeps: Atomic(u32),
    pad: [52]u8,
};

/// The indices of one ring.
pub const RingCtrl = extern struct {
    writer: RingWriter,
    reader: RingReader,
};

/// The first 384 bytes of a connection's segment: the magic, the two rings' indices, the ring capacity, the header's
/// size and the session id. The server writes it once, before it offers the segment; the client checks it before any
/// use and never writes it (session/segment.zig).
pub const SegmentHeader = extern struct {
    magic: u64,
    /// The server-to-client ring.
    s2c: RingCtrl,
    /// The client-to-server ring.
    c2s: RingCtrl,
    /// Bytes of each ring.
    capacity: u32,
    header_size: u32,
    /// Random and not all zero; names the connection's objects on Windows.
    session_id: [16]u8,
    reserved: [96]u8,
};

/// The header of every frame in a ring, in front of a piece of a message (data/ring.zig).
pub const FragHdr = extern struct {
    /// `flag_start`, `flag_end`, `flag_pad`.
    flags: u32,
    /// The piece's length.
    frag_len: u32,
    /// The whole message's length; set on the START piece only.
    total_len: u64,
};

/// The 32-byte header in front of every RPC payload (data/rpc.zig): private, with the layout of the public
/// `fipc_rpc_msg_t`, which `fipc_rpc_recv` fills from it.
pub const RpcMsg = extern struct {
    /// The request's id, echoed in its response.
    id: u64,
    /// `rpc_request` or `rpc_response`.
    kind: u32,
    opcode: u32,
    status: i32,
    reserved: u32,
    /// The payload's length.
    len: u64,
};

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
    _,
};

// === Constants ===

/// `FIPC_RPC_REQUEST` and `FIPC_RPC_RESPONSE`: values of `RpcMsg.kind`.
pub const rpc_request: u32 = 1;
pub const rpc_response: u32 = 2;
/// `fipc_max_piece` is the capacity minus this: the longest piece of a message (the framing takes the rest).
pub const piece_overhead: usize = 64;
/// The smallest ring capacity.
pub const min_capacity: usize = 1024;

/// A frame's length in the ring is a multiple of this (data/ring.zig).
pub const frame_align: usize = 16;
/// Bits of `FragHdr.flags`.
pub const flag_start: u32 = 0x1;
pub const flag_end: u32 = 0x2;
pub const flag_pad: u32 = 0x4;

// === Compile-time checks ===

comptime {
    // 1. The public mirrors match the header as compiled for this target.
    expectSameLayout(RpcMsg, c.fipc_rpc_msg_t);
    expectEnum(Result, c.fipc_result_t, .{
        .{ .ok, c.FIPC_OK },
        .{ .timeout, c.FIPC_TIMEOUT },
        .{ .disconnected, c.FIPC_DISCONNECTED },
        .{ .cancelled, c.FIPC_CANCELLED },
        .{ .too_large, c.FIPC_TOO_LARGE },
        .{ .invalid, c.FIPC_INVALID },
        .{ .no_memory, c.FIPC_NO_MEMORY },
        .{ .addr_in_use, c.FIPC_ADDR_IN_USE },
    });
    // The data path's timeouts: 0 doesn't wait, a negative one waits for ever
    expectValue("FIPC_NO_WAIT", 0, c.FIPC_NO_WAIT);
    expectValue("FIPC_FOREVER", -1, c.FIPC_FOREVER);
    expectValue("FIPC_RPC_REQUEST", rpc_request, c.FIPC_RPC_REQUEST);
    expectValue("FIPC_RPC_RESPONSE", rpc_response, c.FIPC_RPC_RESPONSE);

    // 2. The wire formats two builds of the library share (x86_64, the same on Linux and Windows).
    std.debug.assert(@import("builtin").target.cpu.arch.endian() == .little);
    std.debug.assert(std.mem.eql(u8, std.mem.asBytes(&segment_magic), "FIPCSEG1"));
    expectLayout(ControlFrame, 64, 8, .{
        .magic = 0,
        .frame_type = 4,
        .version = 5,
        .reserved0 = 6,
        .pid = 8,
        .reserved1 = 12,
        .capacity = 16,
        .segment_size = 24,
        .session_id = 32,
        .reserved2 = 48,
    });
    expectLayout(RingWriter, 64, 8, .{ .head = 0, .reader_sleeps = 8, .pad = 12 });
    expectLayout(RingReader, 64, 8, .{ .tail = 0, .writer_sleeps = 8, .pad = 12 });
    expectLayout(RingCtrl, 128, 8, .{ .writer = 0, .reader = 64 });
    expectLayout(SegmentHeader, 384, 8, .{
        .magic = 0,
        .s2c = 8,
        .c2s = 136,
        .capacity = 264,
        .header_size = 268,
        .session_id = 272,
        .reserved = 288,
    });
    expectLayout(FragHdr, 16, 8, .{ .flags = 0, .frag_len = 4, .total_len = 8 });
    expectLayout(RpcMsg, 32, 8, .{ .id = 0, .kind = 8, .opcode = 12, .status = 16, .reserved = 20, .len = 24 });
}

/// Same size, alignment and fields (names in the same order, each at the same offset with the
/// same size) as the translated C struct.
pub fn expectSameLayout(comptime Z: type, comptime C: type) void {
    const zig_info = @typeInfo(Z).@"struct";
    const c_info = @typeInfo(C).@"struct";
    const name = @typeName(Z);
    if (zig_info.field_names.len != c_info.field_names.len)
        fail("{s}: {d} fields, the C header has {d}", .{ name, zig_info.field_names.len, c_info.field_names.len });
    if (@sizeOf(Z) != @sizeOf(C))
        fail("{s}: size {d}, the C header says {d}", .{ name, @sizeOf(Z), @sizeOf(C) });
    if (@alignOf(Z) != @alignOf(C))
        fail("{s}: alignment {d}, the C header says {d}", .{ name, @alignOf(Z), @alignOf(C) });
    inline for (zig_info.field_names, zig_info.field_types, c_info.field_names, c_info.field_types) |zn, zt, cn, ct| {
        if (!std.mem.eql(u8, zn, cn))
            fail("{s}.{s}: the C header has field {s} at this position", .{ name, zn, cn });
        if (@offsetOf(Z, zn) != @offsetOf(C, cn))
            fail("{s}.{s}: offset {d}, the C header says {d}", .{ name, zn, @offsetOf(Z, zn), @offsetOf(C, cn) });
        if (@sizeOf(zt) != @sizeOf(ct))
            fail("{s}.{s}: size {d}, the C header says {d}", .{ name, zn, @sizeOf(zt), @sizeOf(ct) });
    }
}

/// An exported function has its C prototype's shape: C calling convention, the same number of
/// parameters, and each parameter and the result of the same size and kind (void, bool, integer
/// or enum, pointer — optional or not, since C pointers may be NULL — or float).
pub fn expectSameSignature(comptime name: []const u8, comptime Zig: type, comptime C: type) void {
    const zig_fn = @typeInfo(Zig).@"fn";
    const c_fn = @typeInfo(C).@"fn";
    if (!zig_fn.attrs.@"callconv".eql(.c)) fail("{s}: not callconv(.c)", .{name});
    if (zig_fn.param_types.len != c_fn.param_types.len)
        fail("{s}: {d} parameters, the C header has {d}", .{ name, zig_fn.param_types.len, c_fn.param_types.len });
    inline for (zig_fn.param_types, c_fn.param_types, 0..) |zp, cp, i| {
        const zs = shapeOf(zp.?);
        const cs = shapeOf(cp.?);
        if (zs.kind != cs.kind or zs.size != cs.size)
            fail("{s}: parameter {d} is {s} ({d} bytes), the C header has {s} ({d} bytes)", .{
                name, i, @tagName(zs.kind), zs.size, @tagName(cs.kind), cs.size,
            });
    }
    const zr = shapeOf(zig_fn.return_type.?);
    const cr = shapeOf(c_fn.return_type.?);
    if (zr.kind != cr.kind or zr.size != cr.size)
        fail("{s}: returns {s} ({d} bytes), the C header says {s} ({d} bytes)", .{
            name, @tagName(zr.kind), zr.size, @tagName(cr.kind), cr.size,
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

/// The given size, alignment and offset of every field.
fn expectLayout(comptime T: type, comptime size: usize, comptime alignment: usize, comptime offsets: anytype) void {
    const name = @typeName(T);
    if (@sizeOf(T) != size) fail("{s}: size {d}, the protocol needs {d}", .{ name, @sizeOf(T), size });
    if (@alignOf(T) != alignment) fail("{s}: alignment {d}, the protocol needs {d}", .{ name, @alignOf(T), alignment });
    const expected = @typeInfo(@TypeOf(offsets)).@"struct".field_names;
    const actual = @typeInfo(T).@"struct".field_names;
    if (expected.len != actual.len)
        fail("{s}: the offset table lists {d} of its {d} fields", .{ name, expected.len, actual.len });
    inline for (expected) |field_name| {
        const offset = @field(offsets, field_name);
        if (@offsetOf(T, field_name) != offset)
            fail("{s}.{s}: offset {d}, the protocol needs {d}", .{ name, field_name, @offsetOf(T, field_name), offset });
    }
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
