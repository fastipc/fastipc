//! RPC over a connection (`fipc_rpc_*`): request and response messages, each a plain message with a 32-byte header
//! in front of its payload. The header is private (`abi.RpcMsg`, the layout of the public `fipc_rpc_msg_t`, which
//! `recv` fills from it); RPC is copy-only. Either side may submit requests and respond to the requests it receives;
//! the application correlates responses by id.
//!
//! Failures are the data path's (stream.zig), which `c_api.zig` turns into `fipc_result_t` codes. As in
//! stream.zig, the per-message calls hand their result out through a parameter.

const std = @import("std");
const abi = @import("../abi.zig");
const endpoint = @import("../lifecycle/endpoint.zig");
const log = @import("../log.zig");
const Ring = @import("ring.zig").Ring;
const stream = @import("stream.zig");
const ring_wait = @import("ring_wait.zig");

const Endpoint = endpoint.Endpoint;
const RpcMsg = abi.RpcMsg;
const header_len = @sizeOf(RpcMsg);

pub const Error = stream.Error;

/// `fipc_rpc_submit`: sends a request, as `stream.send` sends a message, and sets `id` to its id: the connection's
/// next, never 0. Only a request that was sent takes its id: a submit that fails leaves it to the next one, as a
/// failed call changes nothing. The id is the sending thread's, as the send ring is (one thread at a time sends).
pub fn submit(ep: *Endpoint, opcode: u32, payload: []const u8, timeout_ms: c_int, id: *u64) Error!void {
    const req_id = ep.send.rpc_next_id;
    try sendMessage(ep, .{ .id = req_id, .kind = abi.rpc_request, .opcode = opcode, .status = 0, .reserved = 0, .len = payload.len }, payload, timeout_ms);
    ep.send.rpc_next_id = req_id + 1;
    id.* = req_id;
}

/// `fipc_rpc_respond`: sends the response to request `id`.
pub fn respond(ep: *Endpoint, id: u64, opcode: u32, status: i32, payload: []const u8, timeout_ms: c_int) Error!void {
    return sendMessage(ep, .{ .id = id, .kind = abi.rpc_response, .opcode = opcode, .status = status, .reserved = 0, .len = payload.len }, payload, timeout_ms);
}

fn sendMessage(ep: *Endpoint, header: RpcMsg, payload: []const u8, timeout_ms: c_int) Error!void {
    return stream.send(ep, std.mem.asBytes(&header), payload, timeout_ms);
}

/// `fipc_rpc_recv`: receives the next message (as `stream.recv`), checks that it is a well-formed RPC message (a whole
/// header in its first piece, of a known kind, and the payload's length the header says; else it is dropped: Invalid),
/// fills `msg`, and copies the payload into `buf`. TooLarge if it doesn't fit: `msg` is filled, and nothing is
/// consumed.
pub fn recv(ep: *Endpoint, buf: []u8, timeout_ms: c_int, msg: *RpcMsg) Error!void {
    const r = try stream.recvRing(ep);
    var deadline: ring_wait.Deadline = .init(timeout_ms);
    while (true) {
        var first: stream.First = undefined;
        try stream.nextMessage(ep, r, &deadline, &first);
        if (first.hdr.frag_len < header_len) return drop(ep, r, first, "shorter than an RPC header");
        var header: RpcMsg = undefined;
        @memcpy(std.mem.asBytes(&header), first.prefix(r, header_len));
        if (header.len != first.len - header_len) return drop(ep, r, first, "a payload length the message doesn't have");
        if (header.kind != abi.rpc_request and header.kind != abi.rpc_response) return drop(ep, r, first, "an unknown kind");
        msg.* = header;
        if (header.len > buf.len) return error.TooLarge;
        if (try stream.takeMessage(ep, r, first, header_len, buf[0..header.len])) return;
    }
}

fn drop(ep: *Endpoint, r: Ring, first: stream.First, comptime why: []const u8) Error {
    @branchHint(.cold);
    stream.dropMessage(ep, r, first);
    log.err(@src(), "fipc_rpc_recv: dropped a message that isn't an RPC message: " ++ why, .{});
    return error.Invalid;
}
