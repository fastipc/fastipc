//! The data path of a connection: the copying send and receive of a message of any size, and the zero-copy calls of a
//! message of one piece.
//!
//! Each direction is a ring written by one side and read by the other (ring.zig). A message is one or more pieces, each
//! in a frame of its own: the first carries START and the message's length, the last END. The one sender writes every
//! piece of a message, in order, before it starts the next message. A PAD frame fills the end of the ring when the next
//! frame doesn't fit before it. Waiting for a ring and waking its other side follow one protocol, in ring_wait.zig: every
//! wait is `ring_wait.until`, every index store `ring_wait.publishHead` or `ring_wait.publishTail`.
//!
//! **Receiving into the caller's buffer.** A receive waits for the first piece of the next message, reads its length,
//! and compares it with the caller's buffer before it consumes anything: TooLarge leaves the message where it is. Then
//! it copies each piece straight from the ring into the buffer and consumes it, since the sender needs the room. The
//! library holds no message of its own and allocates nothing.
//!
//! **A started message completes.** A call's timeout bounds its wait for room for the first piece (a send) or for the
//! first piece (a receive). Once a piece has moved, the call goes on until the whole message has, however long that
//! takes: a receive can't give back the pieces it consumed, and a send can't take back the pieces it published. Only
//! cancel or the peer's end stops it, and the message is then dropped whole:
//!   - a receive stopped in the middle leaves the message's other pieces in the ring, and the next receive skips them
//!     (orphans);
//!   - a send stopped in the middle leaves the receiver waiting for pieces that never come. The sender's next message
//!     (a send that needn't wait still works after cancel) begins with a START, which tells the receiver to drop the
//!     unfinished one and take the new one; the sender's close ends the connection.
//!
//! **The session.** Every call reaches the rings through the endpoint, whose first cache line holds the view of the
//! session (header, data, local capacity, role), `requests` and `status`. Each role's thread keeps its own records on
//! a line of its own: the sending thread's zero-copy reservation and the tail it saw last, the receiving thread's
//! acquired message and the head it saw last (ring_wait.zig). The view changes while a call runs only in a forked child,
//! whose calls find no session (Invalid).
//!   - Sends and zero-copy reservations check the phase on entry: nothing is written for a peer that is gone
//!     (Disconnected).
//!   - Receives check nothing on the way: what the peer completed before it died is delivered, and only a receive that
//!     would wait returns Disconnected (ring_wait.zig).
//!   - Cancel is checked only where a call would wait (ring_wait.zig).
//!
//! **The ring's content is the peer's**, so a receive trusts none of it: every frame's header is checked before its
//! frame is waited for or copied, the indices only ever wrap, and a ring whose tail is past its head is an error
//! (ring_wait.zig). Corrupt content ends a call with Invalid, never with an abort, a hang or an access outside the mapping.
//!
//! Failures are `Error` values; `c_api.zig` turns them into `fipc_result_t` codes. Timeouts are C's: milliseconds, 0
//! for a call that doesn't wait, negative for none. The per-message calls return `Error!void` and hand their result
//! out through a parameter: an error union with a pointer or slice payload is built in memory, and on these paths that
//! costs as much as the call itself.

const std = @import("std");
const abi = @import("../abi.zig");
const endpoint = @import("../lifecycle/endpoint.zig");
const log = @import("../log.zig");
const process_state = @import("../process_state.zig");
const ring = @import("ring.zig");
const ring_wait = @import("ring_wait.zig");

const Endpoint = endpoint.Endpoint;
const Ring = ring.Ring;
const FragHdr = ring.FragHdr;
const frag_hdr_len = ring.frag_hdr_len;
const frameLen = ring.frameLen;

const flag_start = abi.flag_start;
const flag_end = abi.flag_end;
const flag_pad = abi.flag_pad;

/// One per `fipc_result_t` code the data calls return.
pub const Error = error{ Invalid, Timeout, TooLarge, Disconnected, Cancelled };

// === Send ===

/// `fipc_send` (an empty header) and RPC's sends: sends `header ++ payload` as one message, in pieces of at most
/// `fipc_max_piece` bytes. The timeout bounds the wait for room for the first piece; the others follow however long
/// they take (a started message completes). Drops a zero-copy reservation that was never committed.
pub fn send(ep: *Endpoint, header: []const u8, payload: []const u8, timeout_ms: c_int) Error!void {
    // Two C lengths whose sum wraps describe no buffer
    const total_len = std.math.add(usize, header.len, payload.len) catch return error.Invalid;
    if (total_len == 0) {
        log.err(@src(), "an empty message", .{});
        return error.Invalid;
    }
    try inSession(ep);
    if (peerGone(ep)) return error.Disconnected;
    ep.send.reserved_len = 0;

    const r = Ring.forSend(&ep.hot);
    const max_piece = r.maxPiece();
    var deadline: ring_wait.Deadline = .init(timeout_ms);
    var sent: usize = 0;
    while (true) {
        const piece = Piece.at(sent, total_len, max_piece);
        const frame = frameLen(piece.len);
        var head: u64 = undefined;
        try reserve(ep, r, frame, &deadline, &head);
        r.writeHeader(head, .{
            .flags = piece.flags,
            .frag_len = @intCast(piece.len),
            .total_len = if (sent == 0) total_len else 0,
        });
        var write_pos = head +% frag_hdr_len;
        for (partsOf(header, payload, sent, sent + piece.len)) |part| {
            if (part.len == 0) continue;
            ring.copy(r.bytes(write_pos, part.len), part);
            write_pos +%= part.len;
        }
        ep.send.head_sent.store(head +% frame, .monotonic);
        ring_wait.publishHead(&ep.hot, r, head +% frame);
        sent += piece.len;
        if (sent == total_len) return;
        deadline = .init(-1); // started: the rest follows however long it takes
    }
}

/// The piece of a `total_len`-byte message that starts at byte `sent`: at most `max_len` bytes; the first carries
/// START, the last END.
const Piece = struct {
    len: usize,
    flags: u32,

    fn at(sent: usize, total_len: usize, max_len: usize) Piece {
        const len = @min(total_len - sent, max_len);
        return .{
            .len = len,
            .flags = (if (sent == 0) flag_start else 0) | (if (sent + len == total_len) flag_end else 0),
        };
    }
};

/// Bytes `begin..end` of `header ++ payload`, as the part in `header` and the part in `payload`.
fn partsOf(header: []const u8, payload: []const u8, begin: usize, end: usize) [2][]const u8 {
    return .{
        header[@min(begin, header.len)..@min(end, header.len)],
        payload[@max(begin, header.len) - header.len .. @max(end, header.len) - header.len],
    };
}

/// Waits until a frame of `frame` bytes fits at the head, before the ring's end, and sets `head` to it. The first look
/// is inline, with the tail this side saw last: there is usually room.
inline fn reserve(ep: *Endpoint, r: Ring, frame: u64, deadline: *ring_wait.Deadline, head: *u64) Error!void {
    const space = Space.of(r, ep.send.tail_seen);
    head.* = space.head;
    if (space.plan(r.cap, frame) != .reserve) return reserveSlow(ep, r, frame, deadline, head);
}

/// The rest of `reserve`: loads the tail, waits for room, and pads the end of the ring if the frame doesn't fit before
/// it.
noinline fn reserveSlow(ep: *Endpoint, r: Ring, frame: u64, deadline: *ring_wait.Deadline, head: *u64) Error!void {
    while (true) {
        ep.send.tail_seen = r.tail();
        const space = Space.of(r, ep.send.tail_seen);
        switch (space.plan(r.cap, frame)) {
            .reserve => {
                head.* = space.head;
                return;
            },
            .corrupt => return error.Invalid,
            .wait => try ring_wait.until(.space, ep, r, frame, deadline),
            .pad => {
                // A PAD frame to the end of the ring, published on its own, so the reader skips it without waiting for
                // the frame after it. The end is a whole number of frame units, at least one.
                try ring_wait.until(.space, ep, r, space.contiguous, deadline);
                r.writeHeader(space.head, .{ .flags = flag_pad, .frag_len = @intCast(space.contiguous - frag_hdr_len), .total_len = 0 });
                ring_wait.publishHead(&ep.hot, r, space.head +% space.contiguous);
            },
        }
    }
}

/// The send ring's free space, as far as the tail this side saw.
const Space = struct {
    head: u64,
    /// More than the ring (the tail past the head) on corrupt indices.
    free: u64,
    /// Bytes from the head to the end of the ring.
    contiguous: u64,

    fn of(r: Ring, tail: u64) Space {
        const head = r.head();
        return .{
            .head = head,
            .free = @as(u64, r.cap) -% (head -% tail),
            .contiguous = r.cap - r.offset(head),
        };
    }

    /// `pad`: fill the rest of the ring (`contiguous` bytes) with a PAD frame first, once that much is free;
    /// `wait`: until `frame` bytes are free.
    const Plan = enum { reserve, pad, wait, corrupt };

    /// What a writer does for a frame of `frame` bytes (at most the ring) in a ring of `cap` bytes: the ring holds
    /// more than `cap` bytes, or its head is off a frame's boundary, only if its indices are corrupt.
    inline fn plan(space: Space, cap: u32, frame: u64) Plan {
        if (space.free > cap or space.contiguous % abi.frame_align != 0) return .corrupt;
        if (space.contiguous < frame) return .pad;
        if (space.free < frame) return .wait;
        return .reserve;
    }
};

// === Receive ===

/// `fipc_recv`: receives the next message into `buf` and sets `len` to its length. TooLarge if it is longer than
/// `buf`: nothing is consumed. A zero-length message (only a faulty peer writes one) is a valid empty message.
pub fn recv(ep: *Endpoint, buf: []u8, timeout_ms: c_int, len: *usize) Error!void {
    const r = try recvRing(ep);
    var deadline: ring_wait.Deadline = .init(timeout_ms);
    while (true) {
        var first: First = undefined;
        try nextMessage(ep, r, &deadline, &first);
        len.* = first.len;
        if (first.len > buf.len) return error.TooLarge;
        if (try takeMessage(ep, r, first, 0, buf[0..first.len])) return;
    }
}

/// The start of every receive: the receive ring, once the message acquired before is released.
pub inline fn recvRing(ep: *Endpoint) Error!Ring {
    try inSession(ep);
    recvRelease(ep);
    return Ring.forRecv(&ep.hot);
}

/// The first piece of the next message, whole in the ring and not consumed yet.
pub const First = struct {
    /// Where its frame begins.
    tail: u64,
    hdr: FragHdr,
    /// The whole message's length.
    len: usize,

    /// The first `n` bytes of the message (`n` at most the first piece's length).
    pub fn prefix(first: First, r: Ring, n: usize) []const u8 {
        return r.bytes(first.tail +% frag_hdr_len, n);
    }
};

/// Waits up to the deadline for the next message's first piece, whole, and sets `first` to it: consumes the PAD frames
/// before it and the orphans, the other pieces of a message nobody is receiving (its receive was stopped, or its sender
/// died or gave it up). The first look is inline: the next frame is usually a message of one piece.
pub inline fn nextMessage(ep: *Endpoint, r: Ring, deadline: *ring_wait.Deadline, first: *First) Error!void {
    try ring_wait.until(.data, ep, r, frag_hdr_len, deadline);
    first.tail = r.tail();
    first.hdr = r.readHeader(first.tail);
    first.len = first.hdr.frag_len;
    if (first.hdr.flags & (flag_start | flag_end | flag_pad) == flag_start | flag_end and r.fits(first.tail, first.hdr))
        return ring_wait.until(.data, ep, r, frameLen(first.hdr.frag_len), deadline);
    return skipToMessage(ep, r, deadline, first);
}

/// The rest of `nextMessage`, out of line. Invalid for a frame that doesn't fit the ring where it lies, which is left
/// there, since its end is unknown; and for a START whose message is no longer than its first piece, which is consumed:
/// one message is lost rather than the ring wedged.
noinline fn skipToMessage(ep: *Endpoint, r: Ring, deadline: *ring_wait.Deadline, first: *First) Error!void {
    var tail = first.tail;
    var hdr = first.hdr;
    while (true) {
        if (!r.fits(tail, hdr)) return error.Invalid;
        const frame = frameLen(hdr.frag_len);
        if (hdr.flags & (flag_start | flag_pad) == flag_start) {
            const len: u64 = if (hdr.flags & flag_end != 0) hdr.frag_len else hdr.total_len;
            if (hdr.flags & flag_end == 0 and len <= hdr.frag_len) {
                try skipFrame(ep, r, tail, frame, deadline);
                return error.Invalid;
            }
            try ring_wait.until(.data, ep, r, frame, deadline);
            first.* = .{ .tail = tail, .hdr = hdr, .len = std.math.cast(usize, len) orelse std.math.maxInt(usize) };
            return;
        }
        try skipFrame(ep, r, tail, frame, deadline);
        try ring_wait.until(.data, ep, r, frag_hdr_len, deadline);
        tail = r.tail();
        hdr = r.readHeader(tail);
    }
}

/// Copies bytes `skip..` of the message `first` begins into `dst` (the message's length less `skip`; `skip` at most the
/// first piece's length) and consumes its pieces: true once the message is whole. False if a new message began before
/// this one ended (its sender gave it up): this one is dropped, and the new one is next. The pieces after the first
/// are waited for however long they take; cancel or the peer's end stops the wait, and the message is dropped.
pub inline fn takeMessage(ep: *Endpoint, r: Ring, first: First, skip: usize, dst: []u8) Error!bool {
    const first_len = first.hdr.frag_len - skip;
    ring.copy(dst[0..first_len], r.bytes(first.tail +% frag_hdr_len +% skip, first_len));
    ring_wait.publishTail(&ep.hot, r, first.tail +% frameLen(first.hdr.frag_len));
    if (first.hdr.flags & flag_end != 0) return true;
    return takePieces(ep, r, first.len, first.hdr.frag_len, dst[first_len..]);
}

/// The pieces after the first, out of line: messages of several pieces are the long ones. `have_before` bytes of the
/// `len`-byte message came before `dst`. Each frame is checked before it is waited for or copied: a piece that
/// doesn't continue the message is consumed, and the message dropped (Invalid; its other pieces are then orphans).
noinline fn takePieces(ep: *Endpoint, r: Ring, len: u64, have_before: u64, dst: []u8) Error!bool {
    var forever: ring_wait.Deadline = .init(-1);
    var have = have_before;
    var at: usize = 0;
    while (have < len) {
        try ring_wait.until(.data, ep, r, frag_hdr_len, &forever);
        const tail = r.tail();
        const hdr = r.readHeader(tail);
        if (!r.fits(tail, hdr)) return error.Invalid;
        const frame = frameLen(hdr.frag_len);
        if (hdr.flags & flag_pad != 0) {
            try skipFrame(ep, r, tail, frame, &forever);
            continue;
        }
        if (hdr.flags & flag_start != 0) return false;
        if (!continues(hdr, have, len)) {
            try skipFrame(ep, r, tail, frame, &forever);
            return error.Invalid;
        }
        try ring_wait.until(.data, ep, r, frame, &forever);
        ring.copy(dst[at..][0..hdr.frag_len], r.bytes(tail +% frag_hdr_len, hdr.frag_len));
        ring_wait.publishTail(&ep.hot, r, tail +% frame);
        have += hdr.frag_len;
        at += hdr.frag_len;
    }
    return true;
}

/// Whether a piece that isn't a START continues a message of `len` bytes, `have` of which came before it: it fits, and
/// it carries END exactly when it completes the message.
fn continues(hdr: FragHdr, have: u64, len: u64) bool {
    const after = have + hdr.frag_len;
    return after <= len and (hdr.flags & flag_end != 0) == (after == len);
}

/// Consumes the message `first` begins: its first piece; its other pieces are orphans, which the next receive skips.
pub fn dropMessage(ep: *Endpoint, r: Ring, first: First) void {
    ring_wait.publishTail(&ep.hot, r, first.tail +% frameLen(first.hdr.frag_len));
}

/// Consumes the `frame` bytes of the frame at `tail`, once they are all there.
fn skipFrame(ep: *Endpoint, r: Ring, tail: u64, frame: u64, deadline: *ring_wait.Deadline) Error!void {
    try ring_wait.until(.data, ep, r, frame, deadline);
    ring_wait.publishTail(&ep.hot, r, tail +% frame);
}

/// `fipc_recv_acquire`: releases a message acquired before, then sets `data` to the next message's bytes in the ring,
/// without consuming it, and records it for `recvRelease`; `len` is its length. TooLarge for a message of several
/// pieces, which stays for `recv`.
pub fn recvAcquire(ep: *Endpoint, timeout_ms: c_int, data: *[*]const u8, len: *usize) Error!void {
    const r = try recvRing(ep);
    var deadline: ring_wait.Deadline = .init(timeout_ms);
    var first: First = undefined;
    try nextMessage(ep, r, &deadline, &first);
    len.* = first.len;
    if (first.hdr.flags & flag_end == 0) return error.TooLarge;
    data.* = first.prefix(r, first.len).ptr;
    ep.recv.acquired_tail = first.tail;
    ep.recv.acquired_frame = frameLen(first.hdr.frag_len);
}

/// `fipc_recv_release`: consumes the message the last `recvAcquire` returned, once: nothing without one, or if the
/// message is no longer the next one (a corrupt peer moved the tail).
pub fn recvRelease(ep: *Endpoint) void {
    const frame = ep.recv.acquired_frame;
    if (frame == 0) return;
    ep.recv.acquired_frame = 0;
    if (ep.hot.view_number.load(.acquire) == 0) return;
    const r = Ring.forRecv(&ep.hot);
    const tail = ep.recv.acquired_tail;
    if (r.tail() != tail) return;
    ring_wait.publishTail(&ep.hot, r, tail +% frame);
}

// === Zero-copy send ===

/// `fipc_send_acquire`: reserves room for a message of `len` bytes (one piece) at the head, waiting up to the timeout
/// for it (after a PAD frame if the rest of the ring is too short), and sets `buffer` to where its bytes go. TooLarge
/// over one piece. Drops the reservation it replaces.
pub fn sendAcquire(ep: *Endpoint, len: usize, timeout_ms: c_int, buffer: *[*]u8) Error!void {
    try inSession(ep);
    if (peerGone(ep)) return error.Disconnected;
    ep.send.reserved_len = 0;
    const r = Ring.forSend(&ep.hot);
    if (len > r.maxPiece()) return error.TooLarge;
    var deadline: ring_wait.Deadline = .init(timeout_ms);
    var head: u64 = undefined;
    try reserve(ep, r, frameLen(len), &deadline, &head);
    ep.send.reserved_head = head;
    ep.send.reserved_len = len;
    buffer.* = r.bytes(head +% frag_hdr_len, len).ptr;
}

/// `fipc_send_commit`: publishes the first `len` bytes (1 to the reserved length) of the last `sendAcquire`'s
/// reservation as one message. A rejected length leaves the reservation armed; once the peer is gone it is used up,
/// and nothing is published (Disconnected). Only this thread moves the head, and every move drops the reservation, so
/// while it is armed its room is still at the head. Inline: it is a handful of stores.
pub inline fn sendCommit(ep: *Endpoint, len: usize) Error!void {
    if (len == 0 or len > ep.send.reserved_len) return error.Invalid;
    // An armed reservation means a session was in the view (a forked child's inherited one is not)
    if (ep.hot.view_number.load(.acquire) == 0) return noSession(ep);
    ep.send.reserved_len = 0;
    if (peerGone(ep)) return error.Disconnected;
    const r = Ring.forSend(&ep.hot);
    const head = ep.send.reserved_head;
    r.writeHeader(head, .{ .flags = flag_start | flag_end, .frag_len = @intCast(len), .total_len = len });
    // Before the publish, whose swap of the sleep flag a store after it waited for: on an M1 that cost zero-copy 16 B
    // some 10%. `send` does the same.
    ep.send.head_sent.store(head +% frameLen(len), .monotonic);
    ring_wait.publishHead(&ep.hot, r, head +% frameLen(len));
}

/// `fipc_max_piece`: the longest piece of the connection's messages, the limit of the zero-copy calls; 0 without a
/// session (a forked child's inherited connection).
pub fn maxPiece(ep: *const Endpoint) usize {
    if (ep.hot.view_number.load(.acquire) == 0) {
        if (ep.handles.forked) process_state.reportInherited("connection");
        return 0;
    }
    return ep.hot.view_cap.load(.monotonic) - abi.piece_overhead;
}

// === The session ===

/// A data call's session: in the view (a connection is handed to the application with its session); none in a forked
/// child: Invalid.
inline fn inSession(ep: *const Endpoint) Error!void {
    if (ep.hot.view_number.load(.acquire) == 0) return noSession(ep);
}

/// A call without a session: Invalid. Only a connection inherited through fork has none, and the first such call logs
/// why (process_state.zig `reportInherited`), which Invalid alone doesn't say. Out of line and cold, off the data
/// path's success path.
noinline fn noSession(ep: *const Endpoint) Error {
    @branchHint(.cold);
    if (ep.handles.forked) process_state.reportInherited("connection");
    return error.Invalid;
}

/// The session has ended: the peer is gone. The status word is on the view's line.
inline fn peerGone(ep: *const Endpoint) bool {
    return ep.hot.status.load(.monotonic).phase == .peer_dead;
}

test "a message splits into START, middle and END pieces" {
    try std.testing.expectEqual(Piece{ .len = 4, .flags = flag_start }, Piece.at(0, 10, 4));
    try std.testing.expectEqual(Piece{ .len = 4, .flags = 0 }, Piece.at(4, 10, 4));
    try std.testing.expectEqual(Piece{ .len = 2, .flags = flag_end }, Piece.at(8, 10, 4));
    try std.testing.expectEqual(Piece{ .len = 4, .flags = flag_start | flag_end }, Piece.at(0, 4, 4));
    try std.testing.expectEqual(Piece{ .len = 1, .flags = flag_start | flag_end }, Piece.at(0, 1, 4));
}

test "a piece's bytes come from the header, the payload or both" {
    const both = partsOf("HHHH", "pppppp", 2, 7);
    try std.testing.expectEqualStrings("HH", both[0]);
    try std.testing.expectEqualStrings("ppp", both[1]);

    const in_header = partsOf("HHHH", "pppppp", 0, 3);
    try std.testing.expectEqualStrings("HHH", in_header[0]);
    try std.testing.expectEqual(@as(usize, 0), in_header[1].len);

    const in_payload = partsOf("HHHH", "pppppp", 5, 10);
    try std.testing.expectEqual(@as(usize, 0), in_payload[0].len);
    try std.testing.expectEqualStrings("ppppp", in_payload[1]);

    const no_header = partsOf("", "pppppp", 0, 6);
    try std.testing.expectEqualStrings("pppppp", no_header[1]);
}

test "a writer reserves, pads, waits or finds the indices corrupt" {
    const cap: u32 = 256;
    const at = struct {
        fn space(head_offset: u32, free: u64) Space {
            return .{ .head = head_offset, .free = free, .contiguous = 256 - head_offset };
        }
    }.space;

    // Room in place, or not yet
    try std.testing.expectEqual(Space.Plan.reserve, at(0, 256).plan(cap, 128));
    try std.testing.expectEqual(Space.Plan.wait, at(0, 100).plan(cap, 128));
    // The end of the ring is too short: pad it
    try std.testing.expectEqual(Space.Plan.pad, at(192, 256).plan(cap, 128));
    try std.testing.expectEqual(Space.Plan.pad, at(240, 100).plan(cap, 128));
    // More free than the ring (the tail past the head), or a head off a frame's boundary
    try std.testing.expectEqual(Space.Plan.corrupt, at(0, 257).plan(cap, 128));
    try std.testing.expectEqual(Space.Plan.corrupt, at(248, 256).plan(cap, 128));
}

test "a piece that isn't a START must fit its message, and END must complete it" {
    const hdr = struct {
        fn of(flags: u32, frag_len: u32) FragHdr {
            return .{ .flags = flags, .frag_len = frag_len, .total_len = 0 };
        }
    }.of;
    try std.testing.expect(continues(hdr(0, 30), 48, 100));
    try std.testing.expect(continues(hdr(flag_end, 52), 48, 100));
    try std.testing.expect(!continues(hdr(flag_end, 30), 48, 100)); // an END too early
    try std.testing.expect(!continues(hdr(0, 52), 48, 100)); // complete, but no END
    try std.testing.expect(!continues(hdr(flag_end, 53), 48, 100)); // past the message
}
