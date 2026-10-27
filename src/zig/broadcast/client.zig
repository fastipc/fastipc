//! A broadcast channel's subscriber (docs/protocol.md §9.3, §9.7): `join`, which sets it up on the caller's thread
//! (`attemptJoin`): it connects to the publisher's listener, waits for its OFFER, maps the channel's segment, answers
//! JOIN and waits for WELCOME. Then the subscriber's watcher applies the publisher's frames (`readPublisherFrames`),
//! and ends the subscription when the publisher ends or unsubscribes it (`endSubscription`). Its receives are
//! reader.zig's; the steps every kind's client takes are core/control.zig's; the publisher's side is server.zig.

const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;
const build_options = @import("build_options");
const wire_protocol = @import("../wire_protocol.zig");
const control = @import("../core/control.zig");
const endpoint_module = @import("../core/endpoint.zig");
const frames = @import("frames.zig");
const log = @import("../core/log.zig");
const platform = @import("../platform.zig");
const process_state = @import("../core/process_state.zig");
const segment = @import("segment.zig");
const test_hooks = @import("../core/test_hooks.zig");
const wire = @import("../core/wire.zig");

const Endpoint = endpoint_module.Endpoint;
const Error = endpoint_module.Error;
const Reader = @import("reader.zig").Reader;
const Role = wire_protocol.SubscriberRole;
const Session = @import("../core/shared_memory.zig").Session;

// === The join ===

/// `fipc_broadcast_join`: validates the name, creates the endpoint and joins the broadcast on `name` as `role` on the
/// caller's thread, within `timeout`. The join (`attemptJoin`) is retried like a stream's handshake while nobody
/// listens or the publisher's poller hasn't admitted it. Then it publishes the subscriber's view (the mapping and its
/// receive state) and starts the watcher.
///
/// The results are those of stream/client.zig `connect`, plus Invalid for:
///   - a name a queue or a service holds;
///   - a name a stream holds, once its server accepts (only `fipc_stream_accept` sends SEGMENT);
///   - a gated role on a broadcast that refuses gated subscribers.
pub fn join(gpa: Allocator, name: []const u8, role: Role, timeout: Io.Timeout) Error!*Endpoint {
    const endpoint = try endpoint_module.newClient(gpa, name);
    errdefer endpoint_module.free(endpoint);
    const joined = try control.retrying(Joined, endpoint, timeout.toTimestamp(endpoint.io), attemptJoin, .{role});
    endpoint.publishView(.{ .object = .{
        .kind = .broadcast_subscriber,
        .session = joined.session,
        .object = joined.reader,
        .capacity = joined.session.capacity,
    } });
    try endpoint_module.startWatching(endpoint);
    return endpoint;
}

/// A joined subscriber: its view of the channel's segment, and its receive state, from the start the publisher gave it
/// (a gated one's bit is set: it holds the writer back from there on).
const Joined = struct {
    session: *Session,
    reader: *Reader,
};

/// One attempt of the join (docs/protocol.md §9.3), as a stream connection's (stream/client.zig `attempt`): OFFER, the
/// attach, JOIN with `role`, WELCOME, then the start the publisher stored in its slot before WELCOME (writer.zig
/// `startSlot`). On failure the endpoint holds no handle.
fn attemptJoin(endpoint: *Endpoint, deadline: ?Io.Clock.Timestamp, role: Role) (Error || error{Retry})!Joined {
    errdefer {
        endpoint.handles.close(.received);
        endpoint.handles.close(.connected);
    }
    try control.connectOnce(endpoint, .member);
    if (build_options.test_hooks) test_hooks.crashIfAt(.connected);
    const offer_frame = try control.awaitOffer(endpoint, deadline, .broadcast);
    const offer = frames.decodeOffer(&offer_frame);
    switch (role) {
        .lossy => {},
        .gated => if (offer.flags & wire_protocol.offer_flag_gated == 0) {
            log.warn(@src(), "the broadcast refuses gated subscribers (its max_stall_ms is 0); the join fails", .{});
            return error.Invalid;
        },
    }
    if (build_options.test_hooks) test_hooks.crashIfAt(.offer_received);
    const session = try control.attach(endpoint, segment.attach, &offer);
    errdefer {
        session.unmap();
        endpoint.gpa.destroy(session);
    }
    const own_wake: platform.SlotWake = if (platform.wakes_each_slot)
        platform.openSlotWake(&offer.session_id, segment.slot_wake_suffix, offer.slot) catch |err| return switch (err) {
            error.OutOfMemory => error.NoMemory,
            error.AccessDenied => control.accessDenied(),
            else => error.Retry, // gone with its owner, or exhaustion
        }
    else {};
    errdefer if (platform.wakes_each_slot) platform.closeSlotWake(own_wake);
    const connection = endpoint.handles.get(.connected).?;
    const join_frame = frames.encodeJoin(platform.pid(), role, offer.slot, offer.generation);
    if (!platform.sendFrame(connection, &join_frame, null)) return error.Retry;
    if (build_options.test_hooks) test_hooks.crashIfAt(.join_sent);
    var welcome: wire.Frame = undefined;
    if (!try control.readFrameWithin(endpoint, connection, &welcome, deadline)) return error.Retry; // the publisher dropped it
    if (!frames.checkSlotFrame(&welcome, .welcome, offer.slot, offer.generation)) {
        log.warn(@src(), "protocol error: a frame other than WELCOME after JOIN", .{});
        return error.Retry;
    }
    if (build_options.test_hooks) test_hooks.crashIfAt(.welcomed);
    if (build_options.test_hooks) test_hooks.holdWelcomed();
    const reader = endpoint.gpa.create(Reader) catch return error.NoMemory;
    errdefer endpoint.gpa.destroy(reader);
    const layout: segment.Layout = .of(session, offer.max_subscribers);
    reader.* = Reader.init(layout, offer.slot, role == .gated, own_wake) catch
        return corruptStart();
    reader.generation = offer.generation;
    if (build_options.test_hooks) test_hooks.crashIfAt(.start_taken);
    return .{ .session = session, .reader = reader };
}

fn corruptStart() error{Invalid} {
    log.warn(@src(), "the start the publisher stored in the slot is corrupt; the join fails", .{});
    return error.Invalid;
}

/// A subscriber's watcher's read (core/calls.zig `readControl`): applies the publisher's frames (`subscriberFrame`: its
/// UNSUBSCRIBE) until the publisher's end or a frame that doesn't belong. True: `close` stopped the read, as in
/// core/control.zig `readUntilEnd`.
pub fn readPublisherFrames(endpoint: *Endpoint, connection: platform.Handle) bool {
    var frame: wire.Frame = undefined;
    while (true) {
        const whole = control.readFrame(endpoint.io, connection, &frame) catch |err| return err == error.Canceled;
        if (!whole or !subscriberFrame(endpoint, &frame)) return false;
    }
}

// === The subscription ===

/// A subscriber's receive state (its view's object).
pub fn readerOf(endpoint: *const Endpoint) *Reader {
    return @ptrCast(@alignCast(endpoint.hot.view_object.load(.monotonic).?));
}

/// A frame on a gated or lossy subscriber's control connection, which its watcher read (`readPublisherFrames`): the
/// publisher's UNSUBSCRIBE of this subscriber is applied (`unsubscribe`), and the watcher reads on; anything else ends
/// the connection (false), as the publisher's end does. A lossy subscriber is never unsubscribed, and holds the writer
/// back for nobody: an UNSUBSCRIBE for one is ignored.
fn subscriberFrame(endpoint: *Endpoint, frame: *const wire.Frame) bool {
    const reader = readerOf(endpoint);
    if (!frames.checkSlotFrame(frame, .unsubscribe, reader.slot, reader.generation)) {
        log.warn(@src(), "protocol error: the publisher sent a frame other than UNSUBSCRIBE; the subscription ends", .{});
        return false;
    }
    if (!reader.gated) {
        log.warn(@src(), "protocol error: the publisher unsubscribed a lossy subscriber; ignored", .{});
        return true;
    }
    if (endpoint.loadPhase() != .unsubscribed) unsubscribe(endpoint, reader);
    return true;
}

/// Ends a subscription on the subscriber's side (docs/protocol.md §9.7). The watcher runs it before it answers or
/// closes the control connection. In order:
///   1. it sets the phase to `phase`, for good;
///   2. it interrupts a sleeping receive (receives sleep inside the segment guard);
///   3. it waits, in 1 ms naps, until no call is inside the guard. A call that sees the new phase returns without
///      sleeping, but its thread may be descheduled.
/// After that no call of the subscriber writes the segment, because only a call that entered the guard while CONNECTED
/// does (reader.zig `Reader.enterSegment`). So the publisher may give the slot to another subscriber once it reads the
/// connection's end.
pub fn endSubscription(endpoint: *Endpoint, phase: endpoint_module.Phase) void {
    const reader = readerOf(endpoint);
    const outside = reader.drop(endpoint, phase);
    endpoint.interrupt();
    if (!outside) while (!reader.outside()) platform.sleep(1);
}

/// The publisher's UNSUBSCRIBE, applied by the subscriber's watcher (docs/protocol.md §9.7): the subscription's end
/// as `unsubscribed` (`endSubscription`), so that no call of the subscriber enters the segment again; then the mapping
/// made inaccessible to every thread of this process (`platform.revoke`), so that a pointer `fipc_recv_acquire` gave
/// the application faults instead of reading what the writer writes next; then UNSUBSCRIBED to the publisher, which
/// frees the slot. The mapping stays reserved until the subscriber's close unmaps it. The subscriber's close may stop
/// the watcher meanwhile: the answer's send never waits (`platform.sendFrame`), and the watcher's next read ends.
fn unsubscribe(endpoint: *Endpoint, reader: *Reader) void {
    endSubscription(endpoint, .unsubscribed);
    {
        endpoint.mutex.lockUncancelable(endpoint.io);
        defer endpoint.mutex.unlock(endpoint.io);
        platform.revoke(&endpoint.hot.view_session.load(.monotonic).?.mapping);
        reader.revoked = true;
    }
    log.warn(@src(), "this gated subscriber held its publisher back for max_stall_ms: the publisher unsubscribed it", .{});
    if (build_options.test_hooks) test_hooks.holdAnswer(endpoint);
    const answer = frames.encodeSlotFrame(.unsubscribed, platform.pid(), reader.slot, reader.generation);
    _ = platform.sendFrame(endpoint.handles.get(.connected).?, &answer, null);
}

// === The subscriber's lifecycle (core/calls.zig) ===

/// The local interrupt of a subscriber (core/endpoint.zig `interrupt`, under the endpoint mutex): its wait for a
/// message, while its mapping is there and accessible.
pub fn interrupt(endpoint: *Endpoint) void {
    if (endpoint.hot.view_session.load(.monotonic) == null) return;
    const reader = readerOf(endpoint);
    if (!reader.revoked) reader.wakeOwn();
}

/// Whether the subscriber's channel has ended, as the native API reports it: the writer's close marked it, though the
/// watcher may not have seen the end yet. False once the subscriber was unsubscribed. It reads under the endpoint
/// mutex, which is held while the mapping is revoked, so it never reads a revoked mapping (a receiving thread relies on
/// its own segment guard instead).
pub fn subscriberEnded(endpoint: *Endpoint) bool {
    const reader = readerOf(endpoint);
    endpoint.mutex.lockUncancelable(endpoint.io);
    defer endpoint.mutex.unlock(endpoint.io);
    return !reader.revoked and endpoint.loadPhase() == .connected and reader.channelEnded();
}

/// A subscriber's leave, at its close: a gated one's bit, and the writer's wake (reader.zig `Reader.leaveChannel`).
pub fn leave(endpoint: *Endpoint) void {
    readerOf(endpoint).leaveChannel(endpoint);
}

/// Frees a subscriber's receive state (`object`, the view's), with its slot wake.
pub fn freeReader(gpa: Allocator, object: *anyopaque) void {
    const reader: *Reader = @ptrCast(@alignCast(object));
    if (platform.wakes_each_slot) platform.closeSlotWake(reader.own_wake);
    gpa.destroy(reader);
}

/// A frame that doesn't belong (a second WELCOME) on the subscriber's control connection to its publisher, for the
/// tests (core/test_hooks.zig `sendStray`). Sent under the handle lock, which the watcher's close of that connection
/// takes too: the order between them otherwise goes through the socket only, which TSan doesn't see.
pub fn sendStray(endpoint: *Endpoint) void {
    const reader = readerOf(endpoint);
    const frame = frames.encodeSlotFrame(.welcome, platform.pid(), reader.slot, reader.generation);
    process_state.lockHandles();
    defer process_state.unlockHandles();
    if (endpoint.handles.get(.connected)) |h| _ = platform.sendFrame(h, &frame, null);
}

/// The sleeps the receives of the subscriber `endpoint` began, for the tests (core/test_hooks.zig
/// `subscriberSleeps`).
pub fn sleepsOf(endpoint: *Endpoint) u32 {
    return readerOf(endpoint).sleeps.load(.seq_cst);
}
