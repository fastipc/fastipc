//! The data path of a connection: the copying send and receive of a message of any size, and the zero-copy calls of a
//! message of one piece.
//!
//! Each direction is a ring written by one side and read by the other (ring.zig). A message is one or more pieces, each
//! in a frame of its own: the first carries START and the message's length, the last END. The one sender writes every
//! piece of a message, in order, before it starts the next message. A PAD frame fills the end of the ring when the next
//! frame doesn't fit before it. Waiting for a ring and waking its other side follow one protocol, in ring_wait.zig: every
//! wait is `ring_wait.until`, every index store `ring_wait.publishHead` or `ring_wait.consume`.
//!
//! **A short message's stamp.** A message of one piece whose frame is shorter than `stamped_frame_max` (256 bytes)
//! carries, in place of its length, a stamp, stored after its bytes and before the head (ring.zig `stamp`): a receiver
//! that has caught up watches for it on the frame's own line, and learns of the message without the head's line first
//! (ring_wait.zig's comment says why that is sound). Its receive reads only `frag_len` of such a frame.
//!
//! **Receiving into the caller's buffer.** A receive waits for the first piece of the next message, reads its length,
//! and compares it with the caller's buffer before it consumes anything: TooLarge leaves the message where it is. Then
//! it copies each piece straight from the ring into the buffer and consumes it, since the sender needs the room. The
//! library holds no message of its own and allocates nothing.
//!
//! **A started message completes.** A call's timeout bounds only its first wait: for room for the first piece (a send),
//! or for the first piece to arrive (a receive). Once a piece has moved, the call goes on until the whole message has,
//! however long that takes: a receive can't give back the pieces it consumed, and a send can't take back the pieces it
//! published. Only cancel or the peer's end stops it, and the message is then dropped whole:
//!   - a receive stopped in the middle leaves the message's other pieces in the ring, and the next receive skips them
//!     (orphans);
//!   - a send stopped in the middle leaves the receiver waiting for pieces that never come. The sender's next message
//!     (a send that needn't wait still works after cancel) begins with a START, which tells the receiver to drop the
//!     unfinished one and take the new one; the sender's close ends the connection.
//!
//! **The session.** Every call reaches the rings through the endpoint, whose first cache line holds the view of the
//! session (header, data, local capacity, role), `requests` and `phase`. Each role's thread keeps its own records on
//! a line of its own: the sending thread's zero-copy reservation, its head and the tail it saw last, the receiving
//! thread's acquired message, its tail and the head it saw last (ring_wait.zig). The view changes while a call runs only in a forked child,
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
//! Failures are `DataError` values; `c_api.zig` turns them into `fipc_result_t` codes. Timeouts are C's: milliseconds,
//! 0 for a call that doesn't wait, negative for none. The per-message calls return `DataError!void` and hand their
//! result out through a parameter: an error union with a pointer or slice payload is built in memory, and on these
//! paths that costs as much as the call itself.

const builtin = @import("builtin");
const std = @import("std");
const wire_protocol = @import("../wire_protocol.zig");
const ring_wait = @import("ring_wait.zig");
const wait = @import("../core/wait.zig");

const Endpoint = @import("../core/endpoint.zig").Endpoint;
const Ring = @import("ring.zig").Ring;
const FrameHeader = @import("ring.zig").FrameHeader;
const FrameFlags = wire_protocol.FrameFlags;
const frame_header_len = @import("ring.zig").frame_header_len;
const frameLen = @import("ring.zig").frameLen;
const stamped_frame_max = @import("ring.zig").stamped_frame_max;
const copy = @import("../core/copy.zig").copy;

const DataError = @import("../core/endpoint.zig").DataError;

// === Send ===

/// `fipc_send`: sends `message` in pieces of at most `fipc_max_piece` bytes. The timeout bounds the wait for room for
/// the first piece; the others follow however long they take (a started message completes). Drops a zero-copy
/// reservation that was never committed. The caller (core/calls.zig) has checked the session, and that the message
/// isn't empty.
pub fn send(endpoint: *Endpoint, message: []const u8, timeout_ms: c_int) DataError!void {
    if (peerGone(endpoint)) return error.Disconnected;
    endpoint.send.reserved_len = 0;

    const ring = Ring.forSend(&endpoint.hot);
    const max_piece = ring.maxPiece();
    var deadline: wait.Deadline = .init(timeout_ms);
    var sent: usize = 0;
    while (true) {
        const piece = Piece.at(sent, message.len, max_piece);
        const frame = frameLen(piece.len);
        var head: u64 = undefined;
        try reserve(endpoint, ring, frame, &deadline, &head);
        prefetchAhead(ring, head, frame);
        if (piece.flags.start and piece.flags.end and frame < stamped_frame_max) {
            ring.writeUnstamped(head, piece.len);
            copy(ring.bytes(head +% frame_header_len, piece.len), message[sent..][0..piece.len]);
            ring.stamp(head);
        } else {
            ring.writeHeader(head, .{
                .flags = piece.flags,
                .frag_len = @intCast(piece.len),
                .total_len = if (sent == 0) message.len else 0,
            });
            copy(ring.bytes(head +% frame_header_len, piece.len), message[sent..][0..piece.len]);
        }
        prefetchNextFrame(endpoint, ring, head, frame);
        endpoint.send.head_sent.store(head +% frame, .monotonic);
        if (ring_wait.own_indices) endpoint.send.head = head +% frame;
        ring_wait.publishHead(&endpoint.hot, ring, head +% frame);
        sent += piece.len;
        if (sent == message.len) return;
        deadline = .init(-1); // started: the rest follows however long it takes
    }
}

/// `fipc_send_many` on a stream: sends the messages of `batch` (core/calls.zig checked them: at least 1 byte each) in
/// order, within one timeout for the whole call. Each message of one piece is written as `send` writes it, and the head
/// stored after its frame, but the reader is woken once (ring_wait.zig `storeHead`, `wakeReader`): after the last
/// frame, and before any wait for room, so that the reader frees what the batch wrote. A message longer than one piece
/// goes through `send`, after that wake, within what is left of the timeout. `sent` is how many went: the message that
/// stops the batch (a timeout, cancel, the peer's end) isn't counted, and every message before it is published.
pub fn sendMany(endpoint: *Endpoint, batch: anytype, timeout_ms: c_int, sent: *usize) DataError!void {
    endpoint.send.reserved_len = 0;
    const ring = Ring.forSend(&endpoint.hot);
    const max_piece = ring.maxPiece();
    var deadline: wait.Deadline = .init(timeout_ms);
    // Frames whose heads were stored since the reader's last wake
    var unwoken = false;
    defer if (unwoken) wakeAfterBatch(endpoint, ring);
    for (0..batch.count) |i| {
        if (peerGone(endpoint)) return error.Disconnected;
        const message = batch.at(i);
        if (message.len > max_piece) {
            if (unwoken) wakeAfterBatch(endpoint, ring);
            unwoken = false;
            try send(endpoint, message, wait.msLeftOf(endpoint.io, &deadline));
            sent.* = i + 1;
            continue;
        }
        const frame = frameLen(message.len);
        const space = Space.of(ring, ring_wait.writerHead(endpoint, ring), endpoint.send.tail_seen);
        var head = space.head;
        if (space.plan(ring.capacity, frame) != .reserve) {
            if (unwoken) wakeAfterBatch(endpoint, ring);
            unwoken = false;
            try reserveSlow(endpoint, ring, frame, &deadline, &head);
        }
        prefetchAhead(ring, head, frame);
        if (frame < stamped_frame_max) {
            ring.writeUnstamped(head, message.len);
            copy(ring.bytes(head +% frame_header_len, message.len), message);
            ring.stamp(head);
        } else {
            ring.writeHeader(head, .{ .flags = .{ .start = true, .end = true }, .frag_len = @intCast(message.len), .total_len = message.len });
            copy(ring.bytes(head +% frame_header_len, message.len), message);
        }
        prefetchNextFrame(endpoint, ring, head, frame);
        if (ring_wait.own_indices) endpoint.send.head = head +% frame;
        ring_wait.storeHead(ring, head +% frame);
        unwoken = true;
        sent.* = i + 1;
    }
}

/// The end of a run of a batch's frames: what this side sent (`SendLine.head_sent`, before the swap, as `send` stores
/// it), then the reader's wake.
inline fn wakeAfterBatch(endpoint: *Endpoint, ring: Ring) void {
    endpoint.send.head_sent.store(ring_wait.writerHead(endpoint, ring), .monotonic);
    ring_wait.wakeReader(&endpoint.hot, ring);
}

/// The piece of a `total_len`-byte message that starts at byte `sent`: at most `max_len` bytes; the first carries
/// START, the last END.
const Piece = struct {
    len: usize,
    flags: FrameFlags,

    fn at(sent: usize, total_len: usize, max_len: usize) Piece {
        const len = @min(total_len - sent, max_len);
        return .{ .len = len, .flags = .{ .start = sent == 0, .end = sent + len == total_len } };
    }
};

/// Waits until a frame of `frame` bytes fits at the head, before the ring's end, and sets `head` to it. The first look
/// is inline, with the tail this side saw last: there is usually room.
inline fn reserve(endpoint: *Endpoint, ring: Ring, frame: u64, deadline: *wait.Deadline, head: *u64) DataError!void {
    const space = Space.of(ring, ring_wait.writerHead(endpoint, ring), endpoint.send.tail_seen);
    head.* = space.head;
    if (space.plan(ring.capacity, frame) != .reserve) return reserveSlow(endpoint, ring, frame, deadline, head);
}

/// How far past the head `prefetchAhead` reaches: eight frames of a 16-byte message.
const prefetch_distance = 256;

/// A copying send's request, for writing, of the ring's cache line `prefetch_distance` bytes past the head (wrapping to
/// the ring's start), for frames shorter than that. The receiver read that line last, so it sits in the receiver's
/// cache, and the publish's swap (ring_wait.zig) waits for every store before it: without the request the sender waits
/// for each new line to come over. Longer frames and zero-copy sends don't gain from it. It pays only with the
/// receive's prefetch (`prefetchPublished`), which keeps the receiver up with the faster sender.
inline fn prefetchAhead(ring: Ring, head: u64, frame: u64) void {
    if (frame >= prefetch_distance) return;
    @prefetch(ring.data + ring.offset(head +% prefetch_distance), .{ .rw = .write, .locality = 3, .cache = .data });
}

/// The longest frame whose successor `prefetchNextFrame` requests: 64 lines.
const prefetch_frame_max = 4096;

/// A send's request, for writing, of the lines of the frame after this one, taken to be as long as this one: for frames
/// of `prefetch_distance` to `prefetch_frame_max` bytes, and only as far as the room this side saw (`tail_seen`), so
/// that no line the receiver hasn't read yet is taken from it. The receiver read those lines last, and the publish's
/// swap waits for every store before it: requested one frame early, they are this side's by the time the next frame is
/// written. Requested once this frame is written: requested before, they compete with its own lines (a zero-copy
/// 1 KiB round trip took about 15% longer on x86). On ARM64 only for a send that follows an answer: a receive wait saw
/// this side's last send (`RecvLine.send_head_seen`), as in a round trip, whose 1 KiB p50 it took about a third off on
/// Apple's M1; a one-way 1 KiB stream there ran 5-9% slower with it.
inline fn prefetchNextFrame(endpoint: *const Endpoint, ring: Ring, head: u64, frame: u64) void {
    if (frame < prefetch_distance or frame > prefetch_frame_max) return;
    if (builtin.cpu.arch != .x86_64 and
        endpoint.recv.send_head_seen.load(.monotonic) != endpoint.send.head_sent.load(.monotonic)) return;
    const next = head +% frame;
    const room = (endpoint.send.tail_seen +% ring.capacity) -% next;
    if (room > ring.capacity) return;
    const end = next +% @min(frame, room);
    var line = next & ~@as(u64, 63);
    while (line -% end > ring.capacity) : (line +%= 64)
        @prefetch(ring.data + ring.offset(line), .{ .rw = .write, .locality = 3, .cache = .data });
}

/// The rest of `reserve`: loads the tail, waits for room, and pads the end of the ring if the frame doesn't fit before
/// it.
noinline fn reserveSlow(endpoint: *Endpoint, ring: Ring, frame: u64, deadline: *wait.Deadline, head: *u64) DataError!void {
    var batched = false;
    while (true) {
        const tail_before = endpoint.send.tail_seen;
        endpoint.send.tail_seen = ring.tail();
        const space = Space.of(ring, ring_wait.writerHead(endpoint, ring), endpoint.send.tail_seen);
        switch (space.plan(ring.capacity, frame)) {
            .reserve => {
                // On aarch64, less than an eighth free while the reader moves: one spin for an eighth first
                // (ring_wait.zig `writer_spin_share`), never in a call that doesn't wait
                if (ring_wait.own_indices and !batched and deadline.timeout_ms != 0 and space.free < ring_wait.roomBatch(ring) and
                    endpoint.send.tail_seen != tail_before)
                {
                    batched = true;
                    ring_wait.spinForBatch(ring, space.head);
                    continue;
                }
                head.* = space.head;
                return;
            },
            .corrupt => return error.Invalid,
            .wait => try ring_wait.until(.space, endpoint, ring, frame, deadline),
            .pad => {
                // A PAD frame to the end of the ring, published on its own, so the reader skips it without waiting for
                // the frame after it. The end is a whole number of frame units, at least one.
                try ring_wait.until(.space, endpoint, ring, space.contiguous, deadline);
                ring.writeHeader(space.head, .{ .flags = .{ .pad = true }, .frag_len = @intCast(space.contiguous - frame_header_len), .total_len = 0 });
                if (ring_wait.own_indices) endpoint.send.head = space.head +% space.contiguous;
                ring_wait.publishHead(&endpoint.hot, ring, space.head +% space.contiguous);
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

    fn of(ring: Ring, head: u64, tail: u64) Space {
        return .{
            .head = head,
            .free = @as(u64, ring.capacity) -% (head -% tail),
            .contiguous = ring.capacity - ring.offset(head),
        };
    }

    /// `pad`: fill the rest of the ring (`contiguous` bytes) with a PAD frame first, once that much is free;
    /// `wait`: until `frame` bytes are free.
    const Plan = enum { reserve, pad, wait, corrupt };

    /// What a writer does for a frame of `frame` bytes (at most the ring) in a ring of `capacity` bytes: the ring holds
    /// more than `capacity` bytes, or its head is off a frame's boundary, only if its indices are corrupt.
    inline fn plan(space: Space, capacity: u32, frame: u64) Plan {
        if (space.free > capacity or space.contiguous % wire_protocol.frame_align != 0) return .corrupt;
        if (space.contiguous < frame) return .pad;
        if (space.free < frame) return .wait;
        return .reserve;
    }
};

// === Receive ===

/// What a receiver takes a frame for. PAD wins: a frame that carries it is padding, whatever else it carries.
const FrameKind = enum { pad, start, piece };

inline fn kindOf(flags: FrameFlags) FrameKind {
    if (flags.pad) return .pad;
    return if (flags.start) .start else .piece;
}

/// `fipc_recv`: receives the next message into `buf` and sets `len` to its length. TooLarge if it is longer than
/// `buf`: nothing is consumed. A zero-length message (only a faulty peer writes one) is a valid empty message.
pub fn recv(endpoint: *Endpoint, buf: []u8, timeout_ms: c_int, len: *usize) DataError!void {
    const ring = ringOf(endpoint);
    var deadline: wait.Deadline = .init(timeout_ms);
    while (true) {
        var first: FirstPiece = undefined;
        try nextMessage(endpoint, ring, &deadline, &first);
        prefetchPublished(endpoint, ring, first.tail);
        prefetchNextPublished(endpoint, ring, first.tail, frameLen(first.header.frag_len));
        len.* = first.len;
        if (first.len > buf.len) return error.TooLarge;
        if (try takeMessage(endpoint, ring, first, buf[0..first.len])) return;
    }
}

/// How far past the tail `prefetchPublished` reaches.
const receive_prefetch_distance = 128;

/// A copying receive's request, for reading, of the ring's cache line `receive_prefetch_distance` bytes past the tail,
/// once the writer has published all of it (up to the head this side saw last); a line the writer may still be
/// writing stays with it. The frames come over before they are read, so the receiver keeps up with a sender that
/// prefetches (`prefetchAhead`) instead of leaving the ring full, where the stream runs about a quarter slower. Only
/// the copying receive asks: in a zero-copy receive the request slows round trips.
inline fn prefetchPublished(endpoint: *const Endpoint, ring: Ring, tail: u64) void {
    const line = (tail +% receive_prefetch_distance) & ~@as(u64, 63);
    const published = endpoint.recv.head_seen -% line;
    if (published < 64 or published > ring.capacity) return;
    @prefetch(ring.data + ring.offset(line), .{ .rw = .read, .locality = 3, .cache = .data });
}

/// A copying receive's request, for reading, of the lines of the frame after this one, taken to be as long as this one:
/// for frames of `prefetch_distance` to `prefetch_frame_max` bytes, and only lines the writer has published (up to the
/// head this side saw last). It keeps the receiver up with the sender, which `prefetchNextFrame` makes faster. x86-64
/// only: on ARM64 `prefetchNextFrame` requests only after an answer, so no stream sender is faster with it.
inline fn prefetchNextPublished(endpoint: *const Endpoint, ring: Ring, tail: u64, frame: u64) void {
    if (builtin.cpu.arch != .x86_64) return;
    if (frame < prefetch_distance or frame > prefetch_frame_max) return;
    const next = tail +% frame;
    const published = endpoint.recv.head_seen -% next;
    if (published > ring.capacity) return;
    const end = (next +% @min(frame, published)) & ~@as(u64, 63); // whole lines only: the writer may be in the last
    var line = next & ~@as(u64, 63);
    while (line -% end > ring.capacity) : (line +%= 64)
        @prefetch(ring.data + ring.offset(line), .{ .rw = .read, .locality = 3, .cache = .data });
}

/// The receive ring of a connection in session, once the message acquired before is released.
inline fn ringOf(endpoint: *Endpoint) Ring {
    releaseHeld(endpoint);
    return Ring.forRecv(&endpoint.hot);
}

/// The first piece of the next message, whole in the ring and not consumed yet.
const FirstPiece = struct {
    /// Where its frame begins.
    tail: u64,
    header: FrameHeader,
    /// The whole message's length.
    len: usize,

    /// The first `n` bytes of the message (`n` at most the first piece's length).
    fn prefix(first: FirstPiece, ring: Ring, n: usize) []const u8 {
        return ring.bytes(first.tail +% frame_header_len, n);
    }
};

/// Waits up to the deadline for the next message's first piece, whole, and sets `first` to it: consumes the PAD frames
/// before it and the orphans, the other pieces of a message nobody is receiving (its receive was stopped, or its sender
/// died or gave it up). The first look is inline: the next frame is usually a message of one piece.
inline fn nextMessage(endpoint: *Endpoint, ring: Ring, deadline: *wait.Deadline, first: *FirstPiece) DataError!void {
    try ring_wait.until(.data, endpoint, ring, frame_header_len, deadline);
    first.tail = ring_wait.readerTail(endpoint, ring);
    first.header = ring.readHeader(first.tail);
    first.len = first.header.frag_len;
    if (kindOf(first.header.flags) == .start and first.header.flags.end and ring.fits(first.tail, first.header))
        return ring_wait.until(.data, endpoint, ring, frameLen(first.header.frag_len), deadline);
    return skipToMessage(endpoint, ring, deadline, first);
}

/// The rest of `nextMessage`, out of line. Invalid for a frame that doesn't fit the ring where it lies, which is left
/// there, since its end is unknown; and for a START whose message is no longer than its first piece, which is consumed:
/// one message is lost rather than the ring wedged.
noinline fn skipToMessage(endpoint: *Endpoint, ring: Ring, deadline: *wait.Deadline, first: *FirstPiece) DataError!void {
    if (!ring.fits(first.tail, first.header)) return error.Invalid;
    var next: Frame = .{ .tail = first.tail, .header = first.header };
    while (true) {
        const header = next.header;
        const frame = frameLen(header.frag_len);
        if (kindOf(header.flags) == .start) {
            const whole = header.flags.end;
            const len: u64 = if (whole) header.frag_len else header.total_len;
            if (!whole and len <= header.frag_len) {
                try skipFrame(endpoint, ring, next.tail, frame, deadline);
                return error.Invalid;
            }
            try ring_wait.until(.data, endpoint, ring, frame, deadline);
            first.* = .{ .tail = next.tail, .header = header, .len = len };
            return;
        }
        try skipFrame(endpoint, ring, next.tail, frame, deadline);
        next = try nextFrame(endpoint, ring, deadline);
    }
}

/// A frame at the receive ring's tail: where it begins, and its header.
const Frame = struct { tail: u64, header: FrameHeader };

/// Waits up to the deadline for the header of the frame at the tail, and reads it. Invalid for a frame that doesn't fit
/// the ring where it lies, which is left there, since its end is unknown.
noinline fn nextFrame(endpoint: *Endpoint, ring: Ring, deadline: *wait.Deadline) DataError!Frame {
    try ring_wait.until(.data, endpoint, ring, frame_header_len, deadline);
    const tail = ring_wait.readerTail(endpoint, ring);
    const header = ring.readHeader(tail);
    if (!ring.fits(tail, header)) return error.Invalid;
    return .{ .tail = tail, .header = header };
}

/// Copies the message `first` begins into `dst` (the message's length) and consumes its pieces: true once the message
/// is whole. False if a new message began before this one ended (its sender gave it up): this one is dropped, and the
/// new one is next. The pieces after the first are waited for however long they take; cancel or the peer's end stops
/// the wait, and the message is dropped.
inline fn takeMessage(endpoint: *Endpoint, ring: Ring, first: FirstPiece, dst: []u8) DataError!bool {
    const first_len = first.header.frag_len;
    copy(dst[0..first_len], first.prefix(ring, first_len));
    ring_wait.consume(endpoint, ring, first.tail +% frameLen(first_len));
    if (first.header.flags.end) return true;
    return takePieces(endpoint, ring, dst, first_len);
}

/// The pieces after the first, out of line: messages of several pieces are the long ones. `dst` is the whole message,
/// whose first `have_first` bytes are there. Each frame is checked before it is waited for or copied: a piece that
/// doesn't continue the message is consumed, and the message dropped (Invalid; its other pieces are then orphans).
noinline fn takePieces(endpoint: *Endpoint, ring: Ring, dst: []u8, have_first: usize) DataError!bool {
    var forever: wait.Deadline = .init(-1);
    var have = have_first;
    while (have < dst.len) {
        const next = try nextFrame(endpoint, ring, &forever);
        const header = next.header;
        const frame = frameLen(header.frag_len);
        switch (kindOf(header.flags)) {
            .pad => {
                try skipFrame(endpoint, ring, next.tail, frame, &forever);
                continue;
            },
            .start => return false,
            .piece => {},
        }
        if (!continues(header, have, dst.len)) {
            try skipFrame(endpoint, ring, next.tail, frame, &forever);
            return error.Invalid;
        }
        try ring_wait.until(.data, endpoint, ring, frame, &forever);
        copy(dst[have..][0..header.frag_len], ring.bytes(next.tail +% frame_header_len, header.frag_len));
        ring_wait.consume(endpoint, ring, next.tail +% frame);
        have += header.frag_len;
    }
    return true;
}

/// Whether a piece that isn't a START continues a message of `len` bytes, `have` of which came before it: it fits, and
/// it carries END exactly when it completes the message.
fn continues(header: FrameHeader, have: u64, len: u64) bool {
    const after = have + header.frag_len;
    return after <= len and header.flags.end == (after == len);
}

/// Consumes the `frame` bytes of the frame at `tail`, once they are all there.
fn skipFrame(endpoint: *Endpoint, ring: Ring, tail: u64, frame: u64, deadline: *wait.Deadline) DataError!void {
    try ring_wait.until(.data, endpoint, ring, frame, deadline);
    ring_wait.consume(endpoint, ring, tail +% frame);
}

/// `fipc_recv_many` on a stream: receives the next message as `recv` does (TooLarge leaves it, its length in `lens[0]`),
/// then every further message of one piece already published, whole, that fits the rest of `buf`, up to `lens.len`,
/// packed back to back, each one's length in `lens`; `count` is how many. It consumes them together, with one store
/// of the tail (a first message of several pieces is consumed piece by piece first). The further messages are taken
/// up to the head this side saw, which it loads once more if that runs out; it stops, consuming nothing more, at
/// anything else (an empty ring, a message in pieces, one too long, an orphan, a frame that doesn't fit), which the
/// next receive takes or reports. A PAD frame on the way is consumed.
pub fn recvMany(endpoint: *Endpoint, buf: []u8, lens: []usize, timeout_ms: c_int, count: *usize) DataError!void {
    const ring = ringOf(endpoint);
    var deadline: wait.Deadline = .init(timeout_ms);
    // Where the messages taken end, and how far the ring is consumed
    var tail: u64 = undefined;
    var consumed: u64 = undefined;
    while (true) {
        var first: FirstPiece = undefined;
        try nextMessage(endpoint, ring, &deadline, &first);
        prefetchPublished(endpoint, ring, first.tail);
        prefetchNextPublished(endpoint, ring, first.tail, frameLen(first.header.frag_len));
        lens[0] = first.len;
        if (first.len > buf.len) return error.TooLarge;
        if (first.header.flags.end) {
            copy(buf[0..first.len], first.prefix(ring, first.len));
            consumed = first.tail;
            tail = first.tail +% frameLen(first.len);
            break;
        }
        if (try takeMessage(endpoint, ring, first, buf[0..first.len])) {
            tail = ring_wait.readerTail(endpoint, ring);
            consumed = tail;
            break;
        }
    }
    var used = lens[0];
    var taken: usize = 1;
    var reloaded = false;
    while (taken < lens.len) {
        var published = endpoint.recv.head_seen -% tail;
        if (published > ring.capacity or published < frame_header_len) {
            if (reloaded) break;
            reloaded = true;
            endpoint.recv.head_seen = ring.head();
            published = endpoint.recv.head_seen -% tail;
            if (published > ring.capacity or published < frame_header_len) break;
        }
        const header = ring.readHeader(tail);
        if (!ring.fits(tail, header)) break;
        const frame = frameLen(header.frag_len);
        if (published < frame) break;
        switch (kindOf(header.flags)) {
            .pad => {
                tail +%= frame;
                continue;
            },
            .start => if (!header.flags.end) break,
            .piece => break,
        }
        const len: usize = header.frag_len;
        if (len > buf.len - used) break;
        prefetchPublished(endpoint, ring, tail);
        copy(buf[used..][0..len], ring.bytes(tail +% frame_header_len, len));
        lens[taken] = len;
        used += len;
        taken += 1;
        tail +%= frame;
    }
    if (tail != consumed) ring_wait.consume(endpoint, ring, tail);
    count.* = taken;
}

/// `fipc_recv_acquire`: releases a message acquired before, then sets `data` to the next message's bytes in the ring,
/// without consuming it, and records it for `recvRelease`; `len` is its length. TooLarge for a message of several
/// pieces, which stays for `recv`.
pub fn recvAcquire(endpoint: *Endpoint, timeout_ms: c_int, data: *[*]const u8, len: *usize) DataError!void {
    const ring = ringOf(endpoint);
    var deadline: wait.Deadline = .init(timeout_ms);
    var first: FirstPiece = undefined;
    try nextMessage(endpoint, ring, &deadline, &first);
    len.* = first.len;
    if (!first.header.flags.end) return error.TooLarge;
    data.* = first.prefix(ring, first.len).ptr;
    endpoint.recv.acquired_tail = first.tail;
    endpoint.recv.acquired_frame = frameLen(first.header.frag_len);
}

/// `fipc_recv_release` (core/calls.zig `recvRelease`), and the rings' release before a receive: consumes the message
/// the last `recvAcquire` returned, once: nothing without one, or if the message is no longer the next one (a corrupt
/// peer moved the tail).
pub fn releaseHeld(endpoint: *Endpoint) void {
    const frame = endpoint.recv.acquired_frame;
    if (frame == 0) return;
    endpoint.recv.acquired_frame = 0;
    if (!endpoint.hot.view_published.load(.acquire)) return;
    const ring = Ring.forRecv(&endpoint.hot);
    const tail = endpoint.recv.acquired_tail;
    if (ring_wait.readerTail(endpoint, ring) != tail) return;
    ring_wait.consume(endpoint, ring, tail +% frame);
}

// === Zero-copy send ===

/// `fipc_send_acquire`: reserves room for a message of `len` bytes (one piece) at the head, waiting up to the timeout
/// for it (after a PAD frame if the rest of the ring is too short), and sets `buffer` to where its bytes go. TooLarge
/// over one piece. Drops the reservation it replaces.
pub fn sendAcquire(endpoint: *Endpoint, len: usize, timeout_ms: c_int, buffer: *[*]u8) DataError!void {
    if (peerGone(endpoint)) return error.Disconnected;
    endpoint.send.reserved_len = 0;
    const ring = Ring.forSend(&endpoint.hot);
    if (len > ring.maxPiece()) return error.TooLarge;
    var deadline: wait.Deadline = .init(timeout_ms);
    var head: u64 = undefined;
    try reserve(endpoint, ring, frameLen(len), &deadline, &head);
    endpoint.send.reserved_head = head;
    endpoint.send.reserved_len = len;
    buffer.* = ring.bytes(head +% frame_header_len, len).ptr;
}

/// `fipc_send_commit`: publishes the first `len` bytes (1 to the reserved length, which the caller checked:
/// core/calls.zig) of the last `sendAcquire`'s reservation as one message. A rejected length leaves the reservation
/// armed; once the peer is gone it is used up, and nothing is published (Disconnected). Only this thread moves the
/// head, and every move drops the reservation, so while it is armed its room is still at the head. Inline: it is a
/// handful of stores and the next frame's request.
pub inline fn sendCommit(endpoint: *Endpoint, len: usize) DataError!void {
    endpoint.send.reserved_len = 0;
    if (peerGone(endpoint)) return error.Disconnected;
    const ring = Ring.forSend(&endpoint.hot);
    const head = endpoint.send.reserved_head;
    if (frameLen(len) < stamped_frame_max) {
        ring.writeUnstamped(head, len);
        ring.stamp(head);
    } else ring.writeHeader(head, .{ .flags = .{ .start = true, .end = true }, .frag_len = @intCast(len), .total_len = len });
    prefetchNextFrame(endpoint, ring, head, frameLen(len));
    // Stored before the publish: the publish swaps the sleep flag, and a store after that swap would wait for it (about
    // 10% of the zero-copy 16 B rate on an M1). `send` orders it the same way.
    endpoint.send.head_sent.store(head +% frameLen(len), .monotonic);
    if (ring_wait.own_indices) endpoint.send.head = head +% frameLen(len);
    ring_wait.publishHead(&endpoint.hot, ring, head +% frameLen(len));
}

/// The session has ended: the peer is gone. The phase is on the view's line.
inline fn peerGone(endpoint: *const Endpoint) bool {
    return endpoint.hot.phase.load(.monotonic) == .peer_dead;
}

test "a message splits into START, middle and END pieces" {
    try std.testing.expectEqual(Piece{ .len = 4, .flags = .{ .start = true } }, Piece.at(0, 10, 4));
    try std.testing.expectEqual(Piece{ .len = 4, .flags = .{} }, Piece.at(4, 10, 4));
    try std.testing.expectEqual(Piece{ .len = 2, .flags = .{ .end = true } }, Piece.at(8, 10, 4));
    try std.testing.expectEqual(Piece{ .len = 4, .flags = .{ .start = true, .end = true } }, Piece.at(0, 4, 4));
    try std.testing.expectEqual(Piece{ .len = 1, .flags = .{ .start = true, .end = true } }, Piece.at(0, 1, 4));
}

test "a writer reserves, pads, waits or finds the indices corrupt" {
    const capacity: u32 = 256;
    const at = struct {
        fn space(head_offset: u32, free: u64) Space {
            return .{ .head = head_offset, .free = free, .contiguous = 256 - head_offset };
        }
    }.space;

    // Room in place, or not yet
    try std.testing.expectEqual(Space.Plan.reserve, at(0, 256).plan(capacity, 128));
    try std.testing.expectEqual(Space.Plan.wait, at(0, 100).plan(capacity, 128));
    // The end of the ring is too short: pad it
    try std.testing.expectEqual(Space.Plan.pad, at(192, 256).plan(capacity, 128));
    try std.testing.expectEqual(Space.Plan.pad, at(240, 100).plan(capacity, 128));
    // More free than the ring (the tail past the head), or a head off a frame's boundary
    try std.testing.expectEqual(Space.Plan.corrupt, at(0, 257).plan(capacity, 128));
    try std.testing.expectEqual(Space.Plan.corrupt, at(248, 256).plan(capacity, 128));
}

test "a piece that isn't a START must fit its message, and END must complete it" {
    const header = struct {
        fn of(flags: FrameFlags, frag_len: u32) FrameHeader {
            return .{ .flags = flags, .frag_len = frag_len, .total_len = 0 };
        }
    }.of;
    try std.testing.expect(continues(header(.{}, 30), 48, 100));
    try std.testing.expect(continues(header(.{ .end = true }, 52), 48, 100));
    try std.testing.expect(!continues(header(.{ .end = true }, 30), 48, 100)); // an END too early
    try std.testing.expect(!continues(header(.{}, 52), 48, 100)); // complete, but no END
    try std.testing.expect(!continues(header(.{ .end = true }, 53), 48, 100)); // past the message
}
