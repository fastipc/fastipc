//! The calls on an endpoint whose behaviour depends on its kind (`ViewKind`, endpoint.zig). Each is a switch on the
//! kind:
//!   - the data calls (`fipc_send`, `fipc_recv`, their batches `fipc_send_many` and `fipc_recv_many`, the zero-copy
//!     calls, `fipc_max_piece`, `fipc_broadcast_wait_for_members`), which c_api.zig and root.zig call;
//!   - the steps of the lifecycle that differ by kind: the local interrupt, the leave at `close`, the watcher's read.
//! This is where core/ calls into the kinds: stream/data_path.zig, broadcast/server.zig and broadcast/client.zig,
//! queue/client.zig (with queue/producer.zig and queue/consumer.zig, over core/sender.zig and core/taker.zig),
//! service/client.zig (with service/caller.zig and service/responder.zig, over the same).
//!
//! The data calls check the session first. A connection reaches the application with its session, and only a
//! connection inherited through fork has none (Invalid). They then branch on the kind once. The kind is loaded after
//! the session check's `.acquire` load of `view_published`, from the same cache line, so it is the published view's
//! kind and costs no extra cache miss. A stream connection's call is inlined into its branch (`.always_inline`), so the
//! switch adds no call: c_api.zig reaches the rings through one call (its `zero_copy_call`).
//!
//! This file, endpoint.zig, control.zig and the kinds import each other: the endpoint's lifecycle and the watcher call
//! the kinds through here, and the kinds work on the endpoint and its control connection. The cycles are deliberate,
//! and Zig analyzes them lazily.

const std = @import("std");
const Allocator = std.mem.Allocator;
const wire_protocol = @import("../wire_protocol.zig");
const broadcast_client = @import("../broadcast/client.zig");
const broadcast_segment = @import("../broadcast/segment.zig");
const broadcast_server = @import("../broadcast/server.zig");
const control = @import("control.zig");
const endpoint_module = @import("endpoint.zig");
const log = @import("log.zig");
const platform = @import("../platform.zig");
const process_state = @import("process_state.zig");
const queue_client = @import("../queue/client.zig");
const queue_segment = @import("../queue/segment.zig");
const ring_wait = @import("../stream/ring_wait.zig");
const service_client = @import("../service/client.zig");
const stream_ring = @import("../stream/ring.zig");
const stream = @import("../stream/data_path.zig");

const Endpoint = endpoint_module.Endpoint;
const ViewKind = endpoint_module.ViewKind;

const DataError = endpoint_module.DataError;

// === The data calls ===

/// `fipc_send`: sends `message`, at least 1 byte (a service's client: a request; its server: the reply to the request
/// it holds); Invalid on a subscriber or a consumer.
pub fn send(endpoint: *Endpoint, message: []const u8, timeout_ms: c_int) DataError!void {
    if (message.len == 0) {
        log.err(@src(), "an empty message", .{});
        return error.Invalid;
    }
    try inSession(endpoint);
    switch (kindOf(endpoint)) {
        .stream => return @call(.always_inline, stream.send, .{ endpoint, message, timeout_ms }),
        .broadcast_publisher => {
            @branchHint(.unlikely);
            return broadcast_server.channelOf(endpoint).writer.send(endpoint, message, timeout_ms);
        },
        .queue_producer => {
            @branchHint(.unlikely);
            return queue_client.producerOf(endpoint).send(endpoint, message, timeout_ms);
        },
        .service_client => {
            @branchHint(.unlikely);
            return service_client.callerOf(endpoint).sender.send(endpoint, message, timeout_ms);
        },
        .service_server => {
            @branchHint(.unlikely);
            return service_client.responderOf(endpoint).send(endpoint, message);
        },
        .broadcast_subscriber, .queue_consumer => {
            @branchHint(.unlikely);
            return error.Invalid;
        },
    }
}

/// `fipc_recv`: the next message, copied into `buf`, its length in `len` (a service's client: the next reply; its
/// server: the next request).
pub fn recv(endpoint: *Endpoint, buf: []u8, timeout_ms: c_int, len: *usize) DataError!void {
    try inSession(endpoint);
    switch (kindOf(endpoint)) {
        .stream => return @call(.always_inline, stream.recv, .{ endpoint, buf, timeout_ms, len }),
        .broadcast_subscriber => {
            @branchHint(.unlikely);
            return broadcast_client.readerOf(endpoint).recv(endpoint, buf, timeout_ms, len);
        },
        .queue_consumer => {
            @branchHint(.unlikely);
            return queue_client.consumerOf(endpoint).recv(endpoint, buf, timeout_ms, len);
        },
        .service_client => {
            @branchHint(.unlikely);
            return service_client.callerOf(endpoint).replies.recv(endpoint, buf, timeout_ms, len);
        },
        .service_server => {
            @branchHint(.unlikely);
            return service_client.responderOf(endpoint).recv(endpoint, buf, timeout_ms, len);
        },
        .broadcast_publisher, .queue_producer => {
            @branchHint(.unlikely);
            return error.Invalid;
        },
    }
}

/// `fipc_send_many` (and the native module's `Conn.sendMany`): sends the messages of `batch` (a `CBatch` or a
/// `SliceBatch`, at least one) in order and publishes them together, the call's timeout bounding it whole; `sent` is
/// how many went, on every return. It checks every message first, before anything is sent: Invalid for an empty one,
/// on a connection that doesn't send batches (a subscriber, a consumer, a service's server, which replies to one held
/// request) and without a session; TooLarge over one piece where messages are of one piece (a broadcast's, a queue's,
/// a service's).
pub fn sendMany(endpoint: *Endpoint, batch: anytype, timeout_ms: c_int, sent: *usize) DataError!void {
    sent.* = 0;
    if (batch.count == 0) return error.Invalid;
    for (0..batch.count) |i| {
        if (batch.lenAt(i) == 0 or !batch.hasData(i)) {
            log.err(@src(), "message {d} of a batch is empty or has no data", .{i});
            return error.Invalid;
        }
    }
    try inSession(endpoint);
    switch (kindOf(endpoint)) {
        .stream => return stream.sendMany(endpoint, batch, timeout_ms, sent),
        .broadcast_publisher => {
            const writer = &broadcast_server.channelOf(endpoint).writer;
            try checkPieces(batch, writer.layout.maxPiece());
            return writer.sendMany(endpoint, batch, timeout_ms, sent);
        },
        .queue_producer => {
            const producer = queue_client.producerOf(endpoint);
            try checkPieces(batch, producer.layout.maxPiece());
            return producer.sendMany(endpoint, batch, timeout_ms, sent);
        },
        .service_client => {
            const sender = &service_client.callerOf(endpoint).sender;
            try checkPieces(batch, sender.layout.maxPiece());
            return sender.sendMany(endpoint, batch, timeout_ms, sent);
        },
        .service_server, .broadcast_subscriber, .queue_consumer => return error.Invalid,
    }
}

/// TooLarge if a message of `batch` is longer than `max_piece`, before any is sent.
fn checkPieces(batch: anytype, max_piece: usize) DataError!void {
    for (0..batch.count) |i| if (batch.lenAt(i) > max_piece) return error.TooLarge;
}

/// A batch of messages as C passes it (`fipc_send_many`): two arrays of `count` entries, each message's data and length.
pub const CBatch = struct {
    datas: [*]const ?*const anyopaque,
    lens: [*]const usize,
    count: usize,

    pub inline fn lenAt(batch: CBatch, i: usize) usize {
        return batch.lens[i];
    }

    pub inline fn hasData(batch: CBatch, i: usize) bool {
        return batch.datas[i] != null;
    }

    /// Message `i`, which `sendMany` checked: at least 1 byte, with its data.
    pub inline fn at(batch: CBatch, i: usize) []const u8 {
        const bytes: [*]const u8 = @ptrCast(batch.datas[i].?);
        return bytes[0..batch.lens[i]];
    }
};

/// A batch of messages as the native module passes it (`Conn.sendMany`).
pub const SliceBatch = struct {
    messages: []const []const u8,
    count: usize,

    pub fn of(messages: []const []const u8) SliceBatch {
        return .{ .messages = messages, .count = messages.len };
    }

    pub inline fn lenAt(batch: SliceBatch, i: usize) usize {
        return batch.messages[i].len;
    }

    pub inline fn hasData(_: SliceBatch, _: usize) bool {
        return true;
    }

    pub inline fn at(batch: SliceBatch, i: usize) []const u8 {
        return batch.messages[i];
    }
};

/// `fipc_recv_many` (and the native module's `Conn.recvMany`): waits within the timeout for the next message, as
/// `recv` does, then takes every further message already there that fits, up to `lens.len` (at least 1), packed back
/// to back into `buf`, each one's length in `lens`; `count` is how many it took, on every return. Every result but OK
/// is the first message's, as `recv` returns it, with `count` 0: TooLarge (its length in `lens[0]`, nothing taken),
/// Overrun (the number lost in `lens[0]`), Abandoned (`lens[0]` 0). After the first it stops, without waiting, before
/// anything else: a message not there yet, one too long for the rest of `buf`, one of several pieces, one that would
/// need another result. Invalid on a publisher, a producer and a service's server (which holds one request at a time).
pub fn recvMany(endpoint: *Endpoint, buf: []u8, lens: []usize, timeout_ms: c_int, count: *usize) DataError!void {
    count.* = 0;
    if (lens.len == 0) return error.Invalid;
    try inSession(endpoint);
    switch (kindOf(endpoint)) {
        .stream => return stream.recvMany(endpoint, buf, lens, timeout_ms, count),
        .broadcast_subscriber => return broadcast_client.readerOf(endpoint).recvMany(endpoint, buf, lens, timeout_ms, count),
        .queue_consumer => return queue_client.consumerOf(endpoint).recvMany(endpoint, buf, lens, timeout_ms, count),
        .service_client => return service_client.callerOf(endpoint).replies.recvMany(endpoint, buf, lens, timeout_ms, count),
        .broadcast_publisher, .queue_producer, .service_server => return error.Invalid,
    }
}

/// `fipc_recv_acquire`: the next message in place, `data` and `len`, until `recvRelease`.
pub fn recvAcquire(endpoint: *Endpoint, timeout_ms: c_int, data: *[*]const u8, len: *usize) DataError!void {
    try inSession(endpoint);
    switch (kindOf(endpoint)) {
        .stream => return @call(.always_inline, stream.recvAcquire, .{ endpoint, timeout_ms, data, len }),
        .broadcast_subscriber => {
            @branchHint(.unlikely);
            const piece = try broadcast_client.readerOf(endpoint).recvAcquire(endpoint, timeout_ms);
            data.* = piece.ptr;
            len.* = piece.len;
        },
        .queue_consumer => {
            @branchHint(.unlikely);
            const piece = try queue_client.consumerOf(endpoint).recvAcquire(endpoint, timeout_ms);
            data.* = piece.ptr;
            len.* = piece.len;
        },
        .service_client => {
            @branchHint(.unlikely);
            const piece = service_client.callerOf(endpoint).replies.recvAcquire(endpoint, timeout_ms) catch |err| {
                if (err == error.Abandoned) len.* = 0;
                return err;
            };
            data.* = piece.ptr;
            len.* = piece.len;
        },
        .service_server => {
            @branchHint(.unlikely);
            const piece = try service_client.responderOf(endpoint).recvAcquire(endpoint, timeout_ms);
            data.* = piece.ptr;
            len.* = piece.len;
        },
        .broadcast_publisher, .queue_producer => {
            @branchHint(.unlikely);
            return error.Invalid;
        },
    }
}

/// `fipc_recv_release`: consumes the message the last `recvAcquire` returned, once. Only a stream connection holds a
/// message of its rings (`acquired_frame` is not 0). Otherwise this releases a gated subscriber's or a queue consumer's
/// held message, or consumes a service client's held reply, if the connection is one (a service's server holds its
/// request until its reply: nothing).
pub fn recvRelease(endpoint: *Endpoint) void {
    if (endpoint.recv.acquired_frame == 0) return releaseOther(endpoint);
    stream.releaseHeld(endpoint);
}

/// `recvRelease` with no message of the rings held, out of line.
noinline fn releaseOther(endpoint: *Endpoint) void {
    if (!endpoint.hot.view_published.load(.acquire)) return;
    switch (kindOf(endpoint)) {
        .broadcast_subscriber => broadcast_client.readerOf(endpoint).release(endpoint),
        .queue_consumer => queue_client.consumerOf(endpoint).release(),
        .service_client => service_client.callerOf(endpoint).replies.release(),
        .stream, .broadcast_publisher, .queue_producer, .service_server => {},
    }
}

/// `fipc_send_acquire`: room for a message of `len` bytes, `buffer` where its bytes go, for `sendCommit`.
pub fn sendAcquire(endpoint: *Endpoint, len: usize, timeout_ms: c_int, buffer: *[*]u8) DataError!void {
    try inSession(endpoint);
    switch (kindOf(endpoint)) {
        .stream => return @call(.always_inline, stream.sendAcquire, .{ endpoint, len, timeout_ms, buffer }),
        .broadcast_publisher => {
            @branchHint(.unlikely);
            buffer.* = (try broadcast_server.channelOf(endpoint).writer.sendAcquire(endpoint, len, timeout_ms)).ptr;
        },
        .queue_producer => {
            @branchHint(.unlikely);
            buffer.* = (try queue_client.producerOf(endpoint).sendAcquire(endpoint, len, timeout_ms)).ptr;
        },
        .service_client => {
            @branchHint(.unlikely);
            buffer.* = (try service_client.callerOf(endpoint).sender.sendAcquire(endpoint, len, timeout_ms)).ptr;
        },
        .service_server => {
            @branchHint(.unlikely);
            buffer.* = (try service_client.responderOf(endpoint).sendAcquire(endpoint, len)).ptr;
        },
        .broadcast_subscriber, .queue_consumer => {
            @branchHint(.unlikely);
            return error.Invalid;
        },
    }
}

/// `fipc_send_commit`: publishes the first `len` bytes of the last `sendAcquire`'s reservation. A reservation of the
/// rings is armed only on a stream connection, so the rings' commit is the one that finds `len` within it; anything
/// else is a publisher's commit, or Invalid. Inline: the rings' commit is a handful of stores.
pub inline fn sendCommit(endpoint: *Endpoint, len: usize) DataError!void {
    if (len == 0 or len > endpoint.send.reserved_len) return commitOther(endpoint, len);
    // An armed reservation means a session was in the view (a forked child's inherited one is not)
    if (!endpoint.hot.view_published.load(.acquire)) return noSession(endpoint);
    return stream.sendCommit(endpoint, len);
}

/// `sendCommit` without a reservation of the rings, out of line: a publisher's, a queue producer's or a service
/// member's commit, if the connection is one; else Invalid.
noinline fn commitOther(endpoint: *Endpoint, len: usize) DataError!void {
    if (!endpoint.hot.view_published.load(.acquire)) return error.Invalid;
    return switch (kindOf(endpoint)) {
        .broadcast_publisher => broadcast_server.channelOf(endpoint).writer.sendCommit(len),
        .queue_producer => queue_client.producerOf(endpoint).sendCommit(endpoint, len),
        .service_client => service_client.callerOf(endpoint).sender.sendCommit(endpoint, len),
        .service_server => service_client.responderOf(endpoint).sendCommit(endpoint, len),
        .stream, .broadcast_subscriber, .queue_consumer => error.Invalid,
    };
}

/// `fipc_max_piece`: the longest piece of the connection's messages, the limit of the zero-copy calls (a broadcast's
/// and a queue's connections: a quarter of its capacity; a service's: its max_message); 0 without a session (a forked
/// child's inherited connection).
pub fn maxPiece(endpoint: *const Endpoint) usize {
    if (!endpoint.hot.view_published.load(.acquire)) {
        if (endpoint.handles.forked) process_state.reportInherited("connection");
        return 0;
    }
    const capacity = endpoint.hot.view_capacity.load(.monotonic);
    return switch (kindOf(endpoint)) {
        .stream => stream_ring.maxPieceOf(capacity),
        .broadcast_publisher, .broadcast_subscriber => broadcast_segment.maxPieceOf(capacity),
        .queue_producer, .queue_consumer => queue_segment.Rings.maxPieceOf(capacity),
        .service_client, .service_server => service_client.maxPiece(endpoint),
    };
}

/// `fipc_broadcast_wait_for_members`: a publisher's count of its joined subscribers, with a wait (broadcast/server.zig
/// `waitForMembers`); Invalid on any other connection.
pub fn waitForMembers(endpoint: *Endpoint, at_least: u32, timeout_ms: c_int, count: *u32) DataError!void {
    try inSession(endpoint);
    return switch (kindOf(endpoint)) {
        .broadcast_publisher => broadcast_server.waitForMembers(endpoint, at_least, timeout_ms, count),
        .stream, .broadcast_subscriber, .queue_producer, .queue_consumer, .service_client, .service_server => error.Invalid,
    };
}

/// The number of messages a lossy subscriber's last Overrun lost (the native module's `Conn.lostMessages`); 0 on any
/// other connection, and before the first Overrun.
pub fn lostMessages(endpoint: *const Endpoint) u64 {
    if (!endpoint.hot.view_published.load(.acquire)) return 0;
    return switch (kindOf(endpoint)) {
        .broadcast_subscriber => broadcast_client.readerOf(endpoint).lost,
        .stream, .broadcast_publisher, .queue_producer, .queue_consumer, .service_client, .service_server => 0,
    };
}

/// A data call's session: in the view (a connection is handed to the application with its session); none in a forked
/// child: Invalid.
inline fn inSession(endpoint: *const Endpoint) DataError!void {
    if (!endpoint.hot.view_published.load(.acquire)) return noSession(endpoint);
}

/// A call without a session: Invalid. Only a connection inherited through fork has none, and the first such call logs
/// why (core/process_state.zig `reportInherited`), which Invalid alone doesn't say. Out of line and cold, off the data
/// path's success path.
noinline fn noSession(endpoint: *const Endpoint) DataError {
    @branchHint(.cold);
    if (endpoint.handles.forked) process_state.reportInherited("connection");
    return error.Invalid;
}

/// The endpoint's kind: after the session check's load of `view_published` (.acquire), the published view's.
pub inline fn kindOf(endpoint: *const Endpoint) ViewKind {
    return endpoint.hot.view_kind.load(.monotonic);
}

// === The lifecycle ===

/// The local interrupt (endpoint.zig `interrupt`, under the endpoint mutex). It wakes this connection's own waits:
///   - a stream connection's ring waiters, while its session is mapped (stream/ring_wait.zig `wakeOwnWaiters`;
///     docs/protocol.md §6.2 states the wake rule);
///   - a publisher's wait for room and its member count's waiters;
///   - a subscriber's wait for a message;
///   - a queue producer's wait for room, and a consumer's wait for a message;
///   - a service client's waits for room and for a reply, and a server's wait for a request.
pub fn interrupt(endpoint: *Endpoint) void {
    switch (kindOf(endpoint)) {
        .stream => if (endpoint.hot.view_session.load(.monotonic) != null) ring_wait.wakeOwnWaiters(&endpoint.hot),
        .broadcast_publisher => broadcast_server.interrupt(endpoint),
        .broadcast_subscriber => broadcast_client.interrupt(endpoint),
        .queue_producer, .queue_consumer => queue_client.interrupt(endpoint),
        .service_client, .service_server => service_client.interrupt(endpoint),
    }
}

/// The first step of a connection's `close`, and of a start that failed: a gated subscriber leaves its broadcast (the
/// publisher stops waiting for it at once), a publisher ends the broadcast (broadcast/server.zig `closePublisher`), a
/// queue's consumer releases the message it holds in place (queue/client.zig `leave`), a service's server abandons the
/// request it holds (service/client.zig `leave`); nothing for a stream connection.
pub fn leave(endpoint: *Endpoint) void {
    switch (kindOf(endpoint)) {
        .stream => {},
        .broadcast_subscriber => broadcast_client.leave(endpoint),
        .broadcast_publisher => broadcast_server.closePublisher(endpoint),
        .queue_producer, .queue_consumer => queue_client.leave(endpoint),
        .service_client, .service_server => service_client.leave(endpoint),
    }
}

/// A subscriber's broadcast has ended (its publisher closed it), or a queue member's queue, or a service member's
/// service, as the native API reports it; false for any other connection. With the view published.
pub fn channelEnded(endpoint: *Endpoint) bool {
    return switch (kindOf(endpoint)) {
        .broadcast_subscriber => broadcast_client.subscriberEnded(endpoint),
        .queue_producer, .queue_consumer => queue_client.memberEnded(endpoint),
        .service_client, .service_server => service_client.memberEnded(endpoint),
        .stream, .broadcast_publisher => false,
    };
}

/// Frees the view's object of kind `kind` (`Hot.view_object`), once its session is unmapped: a subscriber's receive
/// state, a queue member's or a service member's state. A publisher's channel is its own (broadcast/server.zig
/// `closePublisher`).
pub fn freeObject(gpa: Allocator, kind: ViewKind, object: ?*anyopaque) void {
    switch (kind) {
        .broadcast_subscriber => broadcast_client.freeReader(gpa, object.?),
        .queue_producer => queue_client.freeMember(gpa, .producer, object.?),
        .queue_consumer => queue_client.freeMember(gpa, .consumer, object.?),
        .service_client => service_client.freeMember(gpa, .client, object.?),
        .service_server => service_client.freeMember(gpa, .server, object.?),
        .stream, .broadcast_publisher => {},
    }
}

/// The watcher's read of the control connection (control.zig `runWatcher`), until the peer's end. What arrives depends
/// on the kind:
///   - a stream connection: nothing more (`control.readUntilEnd`);
///   - a subscriber: the publisher's frames (broadcast/client.zig `readPublisherFrames`);
///   - a queue member: nothing, and any frame is this member's own failure (queue/client.zig `readOwnerFrames`); a
///     service member likewise (service/client.zig `readOwnerFrames`).
/// Returns true when `close` stopped the read. A publisher has no watcher.
pub fn readControl(endpoint: *Endpoint, connection: platform.Handle) bool {
    return switch (kindOf(endpoint)) {
        .stream => control.readUntilEnd(endpoint, connection),
        .broadcast_subscriber => broadcast_client.readPublisherFrames(endpoint, connection),
        .queue_producer, .queue_consumer => queue_client.readOwnerFrames(endpoint, connection),
        .service_client, .service_server => service_client.readOwnerFrames(endpoint, connection),
        .broadcast_publisher => unreachable,
    };
}

/// What the watcher still does at the peer's end once it has closed the control connection (`applyPeerEnd`).
pub const PeerEnd = enum {
    /// Nothing: the kind published the end before the close.
    published,
    /// PEER_DEAD, then the wake of this side's ring waiters (a stream connection's).
    publish_after_close,
};

/// The peer's end, as the watcher applies it before it closes the control connection (control.zig `runWatcher`):
///   - a subscriber ends its subscription with PEER_DEAD (broadcast/client.zig `endSubscription`), unless the publisher
///     unsubscribed it, which keeps that phase. It must end before the close, because the publisher takes the close as
///     a sign that this subscriber writes its slot no more;
///   - a queue member marks the queue's end, then sets the phase (queue/client.zig `endMembership`), and a service
///     member the service's (service/client.zig `endMembership`);
///   - a stream connection publishes PEER_DEAD after the close.
pub fn applyPeerEnd(endpoint: *Endpoint) PeerEnd {
    switch (kindOf(endpoint)) {
        .stream => return .publish_after_close,
        .broadcast_subscriber => {
            if (endpoint.loadPhase() != .unsubscribed) broadcast_client.endSubscription(endpoint, .peer_dead);
            return .published;
        },
        .queue_producer, .queue_consumer => {
            queue_client.endMembership(endpoint);
            return .published;
        },
        .service_client, .service_server => {
            service_client.endMembership(endpoint);
            return .published;
        },
        .broadcast_publisher => unreachable, // a publisher has no watcher
    }
}
