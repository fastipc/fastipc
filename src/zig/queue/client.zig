//! A work queue's producers and consumers, their side (docs/protocol.md §10.3, §10.7): `join`, which sets one up on the
//! caller's thread (`attemptJoin`): it connects to the name the owner listens on, waits for its OFFER, maps the queue's
//! segment, answers JOIN with its role and waits for WELCOME, which gives it a slot. Then its watcher reads the control
//! connection, on which the owner sends nothing after WELCOME (`readOwnerFrames`): the connection's end is the queue's
//! end (`endMembership`), and a frame on it is this member's own failure. The owner joins its own queue this way too:
//! its poller admits it as any joiner. The sends and receives are producer.zig's and consumer.zig's; the steps every
//! kind's client takes are core/control.zig's; the owner's side is server.zig.
//!
//! A member closes its control connection only at its close (the watcher's stop), or once it read the queue's end:
//! after its own failure the watcher keeps it open until the close, so that the owner frees the member's slot (and
//! drops a message it held) only once its process writes the segment no more.

const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;
const build_options = @import("build_options");
const control = @import("../core/control.zig");
const endpoint_module = @import("../core/endpoint.zig");
const frames = @import("frames.zig");
const log = @import("../core/log.zig");
const platform = @import("../platform.zig");
const segment = @import("segment.zig");
const test_hooks = @import("../core/test_hooks.zig");
const wire = @import("../core/wire.zig");

const Consumer = @import("consumer.zig").Consumer;
const Endpoint = endpoint_module.Endpoint;
const Error = endpoint_module.Error;
const Slot = segment.Slot;
const Producer = @import("producer.zig").Producer;
const Role = segment.Role;
const Session = @import("../core/shared_memory.zig").Session;

/// A producer's or consumer's state. All of it is set once, at the join, and read by any thread (the watcher's end, a
/// cancel).
pub const State = struct {
    slot: Slot,
    /// The member's view of the segment.
    layout: segment.Layout,
    generation: u32,
    /// Its own slot's wake (Windows), which it sleeps on; null where it was gone at the join (the queue had ended).
    own_wake: ?platform.SlotWake,
    /// A producer's payload position record: a sixteenth of the capacity.
    positions: []u32,
};

/// A member: its view's object, one allocation. `side`, its Producer or Consumer, is the sending or receiving thread's,
/// on cache lines of its own, so that no read of `state` touches what that thread writes; the data path reaches it from
/// the view's object with no further load. No union: another thread's access to a union field would read the whole
/// union in a safe build (its tag's check), the thread's writes included.
fn Member(comptime Side: type) type {
    return struct {
        state: State,
        side: Side align(64),
    };
}

pub fn viewKind(role: Role) endpoint_module.ViewKind {
    return switch (role) {
        .producer => .queue_producer,
        .consumer => .queue_consumer,
    };
}

/// A member (`Member`), on its view of the segment (`layout`): a producer starts at its ring's `tail` (Invalid: the
/// ring's indices are a corrupt peer's), a consumer at the ring its slot maps to. It owns `own_wake` once it is
/// returned. The view's object.
pub fn newMember(
    gpa: Allocator,
    layout: segment.Layout,
    slot: Slot,
    generation: u32,
    session_id: [16]u8,
    own_wake: ?platform.SlotWake,
) error{ NoMemory, Invalid }!*anyopaque {
    const event = platform.slotRingEvent(own_wake);
    const state: State = .{ .slot = slot, .layout = layout, .generation = generation, .own_wake = own_wake, .positions = &.{} };
    switch (slot.role) {
        .producer => {
            const member = gpa.create(Member(Producer)) catch return error.NoMemory;
            errdefer gpa.destroy(member);
            const positions = gpa.alloc(u32, @intCast(layout.cellCount())) catch return error.NoMemory;
            errdefer gpa.free(positions);
            member.* = .{ .state = state, .side = Producer.init(layout, slot.index, positions, event, session_id) catch return error.Invalid };
            member.state.positions = positions;
            return member;
        },
        .consumer => {
            const member = gpa.create(Member(Consumer)) catch return error.NoMemory;
            member.* = .{ .state = state, .side = .init(layout, slot.index, event, session_id) };
            return member;
        },
    }
}

/// Frees a member of `role` (`object`, the view's), with the wakes it opened.
pub fn freeMember(gpa: Allocator, role: Role, object: *anyopaque) void {
    switch (role) {
        .producer => {
            const member: *Member(Producer) = @ptrCast(@alignCast(object));
            member.side.deinit();
            freeState(gpa, &member.state);
            gpa.destroy(member);
        },
        .consumer => {
            const member: *Member(Consumer) = @ptrCast(@alignCast(object));
            member.side.deinit();
            freeState(gpa, &member.state);
            gpa.destroy(member);
        },
    }
}

fn freeState(gpa: Allocator, state: *State) void {
    gpa.free(state.positions);
    if (platform.wakes_each_slot) if (state.own_wake) |wake| platform.closeSlotWake(wake);
}

fn memberOf(comptime Side: type, endpoint: *const Endpoint) *Member(Side) {
    return @ptrCast(@alignCast(endpoint.hot.view_object.load(.monotonic).?));
}

pub fn stateOf(endpoint: *const Endpoint) *State {
    return switch (endpoint.hot.view_kind.load(.monotonic)) {
        .queue_producer => &memberOf(Producer, endpoint).state,
        .queue_consumer => &memberOf(Consumer, endpoint).state,
        .stream, .broadcast_publisher, .broadcast_subscriber, .service_client, .service_server => unreachable,
    };
}

pub fn producerOf(endpoint: *const Endpoint) *Producer {
    return &memberOf(Producer, endpoint).side;
}

pub fn consumerOf(endpoint: *const Endpoint) *Consumer {
    return &memberOf(Consumer, endpoint).side;
}

// === The join ===

/// `fipc_queue_join`: validates the name, creates the endpoint and joins the work queue on `name` as `role` on the
/// caller's thread, within `timeout` (`attemptJoin`, retried as a stream's handshake while nobody listens or no slot
/// of the role is free); then publishes the member's view and starts the watcher. Invalid also for a name a broadcast
/// holds, and a name a stream holds once its server accepts (SEGMENT comes only from `fipc_stream_accept`).
pub fn join(gpa: Allocator, name: []const u8, role: Role, timeout: Io.Timeout) Error!*Endpoint {
    const endpoint = try endpoint_module.newClient(gpa, name);
    errdefer endpoint_module.free(endpoint);
    const joined = try control.retrying(Joined, endpoint, timeout.toTimestamp(endpoint.io), attemptJoin, .{role});
    endpoint.publishView(.{ .object = .{
        .kind = viewKind(role),
        .session = joined.session,
        .object = joined.member,
        .capacity = joined.session.capacity,
    } });
    try endpoint_module.startWatching(endpoint);
    return endpoint;
}

/// A joined member: its view of the queue's segment, and its `Member`.
const Joined = struct {
    session: *Session,
    member: *anyopaque,
};

/// One attempt of the join (docs/protocol.md §10.3), as a stream connection's (stream/client.zig `attempt`). On failure
/// the endpoint holds no handle; a failure after WELCOME closes the connection, whose end the owner reads as this
/// member's (it wrote nothing in the segment).
fn attemptJoin(endpoint: *Endpoint, deadline: ?Io.Clock.Timestamp, role: Role) (Error || error{Retry})!Joined {
    errdefer {
        endpoint.handles.close(.received);
        endpoint.handles.close(.connected);
    }
    try control.connectOnce(endpoint, .member);
    if (build_options.test_hooks) test_hooks.crashIfAt(.connected);
    const offer_frame = try control.awaitOffer(endpoint, deadline, .queue);
    const offer = frames.decodeOffer(&offer_frame);
    if (build_options.test_hooks) test_hooks.crashIfAt(.offer_received);
    const session = try control.attach(endpoint, segment.attach, &offer);
    errdefer {
        session.unmap();
        endpoint.gpa.destroy(session);
    }
    const connection = endpoint.handles.get(.connected).?;
    const join_frame = frames.encodeJoin(platform.pid(), role);
    if (!platform.sendFrame(connection, &join_frame, null)) return error.Retry;
    if (build_options.test_hooks) test_hooks.crashIfAt(.join_sent);
    var frame: wire.Frame = undefined;
    // The end of the stream: the owner dropped it (no free slot of its role), or left
    if (!try control.readFrameWithin(endpoint, connection, &frame, deadline)) return error.Retry;
    const max_slots = switch (role) {
        .producer => offer.max_producers,
        .consumer => offer.max_consumers,
    };
    const welcome = frames.checkWelcome(&frame, role, max_slots) orelse {
        log.warn(@src(), "protocol error: a frame other than WELCOME after JOIN", .{});
        return error.Retry;
    };
    if (build_options.test_hooks) test_hooks.crashIfAt(.welcomed);
    // The owner created the slot's wake before WELCOME. Gone: the owner tore the queue down since, so the queue has
    // ended; the member goes on without it (its waits sleep in short slices)
    const own_wake: ?platform.SlotWake = if (platform.wakes_each_slot)
        platform.openSlotWake(&offer.session_id, segment.Rings.wakeSuffix(role), welcome.slot) catch |err| switch (err) {
            error.Gone => null,
            error.OutOfMemory => return error.NoMemory,
            error.AccessDenied => return control.accessDenied(),
            else => return error.Retry,
        }
    else {};
    const layout: segment.Layout = .of(session, offer.max_producers, offer.max_consumers);
    const member = newMember(endpoint.gpa, layout, .{ .role = role, .index = welcome.slot }, welcome.generation, offer.session_id, own_wake) catch |err| {
        if (platform.wakes_each_slot) if (own_wake) |wake| platform.closeSlotWake(wake);
        if (err == error.Invalid) log.warn(@src(), "the ring of the offered producer slot has corrupt indices; the join fails", .{});
        return err;
    };
    if (build_options.test_hooks) test_hooks.crashIfAt(.start_taken);
    return .{ .session = session, .member = member };
}

// === The watcher ===

/// A member's watcher's read (core/calls.zig `readControl`). The owner sends nothing after WELCOME and never closes a
/// live member's connection while the queue runs (server.zig), so the read returns only at an end:
///   - the end of the stream, or a failed read, is the queue's end: false, and core/control.zig `runWatcher` applies
///     it (`endMembership`);
///   - a whole frame is this member's own failure (the owner is alive, and buggy): PEER_DEAD without the end's mark,
///     so its calls return Disconnected at once and a consumer doesn't drain. The connection stays open until the
///     member's close stops the watcher (true), so that the owner frees the slot only once this process no longer
///     writes the segment;
///   - the close's stop (error.Canceled comes only from it): true.
pub fn readOwnerFrames(endpoint: *Endpoint, connection: platform.Handle) bool {
    var frame: wire.Frame = undefined;
    const whole = control.readFrame(endpoint.io, connection, &frame) catch |err| return err == error.Canceled;
    if (!whole) return false;
    log.warn(@src(), "protocol error: the work queue's owner sent a frame after WELCOME; this member stops", .{});
    endpoint.hot.phase.store(.peer_dead, .seq_cst);
    endpoint.interrupt();
    while (!endpoint.loadRequests().shutdown) platform.sleep(1);
    return true;
}

/// The queue's end, as a member's watcher applies it before it closes the connection: the end's mark in the segment
/// (sequentially consistent, before the phase: a consumer that sees the phase then loads the mark, and drains), then
/// PEER_DEAD, then the member's local interrupt.
pub fn endMembership(endpoint: *Endpoint) void {
    layoutOf(endpoint).header.ended.store(1, .seq_cst);
    endpoint.hot.phase.store(.peer_dead, .seq_cst);
    endpoint.interrupt();
}

// === The member's lifecycle (core/calls.zig) ===

/// The local interrupt of a member (core/endpoint.zig `interrupt`, under the endpoint mutex, on any thread): its wait
/// for room or for a message, after the caller published what it must see. It reads only what the join set, never the
/// sending or receiving thread's state (core/sender.zig, core/taker.zig `interrupt`).
pub fn interrupt(endpoint: *Endpoint) void {
    const state = stateOf(endpoint);
    const flag = sleepFlag(&state.layout, state.slot);
    if (flag.swap(0, .seq_cst) == 1) platform.ringWake(flag, platform.slotRingEvent(state.own_wake));
}

/// The flag a member of `slot` sleeps on: a consumer's `sleeps`, a producer's ring's `room_sleeps`.
pub fn sleepFlag(layout: *const segment.Layout, slot: Slot) *std.atomic.Value(u32) {
    return switch (slot.role) {
        .producer => &layout.ring(slot.index).room_sleeps,
        .consumer => &layout.takerSlot(slot.index).sleeps,
    };
}

/// A member's queue has ended, as the native API reports it: the end's mark is in the segment.
pub fn memberEnded(endpoint: *Endpoint) bool {
    return layoutOf(endpoint).header.ended.load(.acquire) != 0;
}

/// A member's leave, at its close: a consumer releases the message it holds in place. Its state is freed once the
/// watcher stopped and the mapping is gone (core/calls.zig `freeObject`); its slot is freed by the owner, which reads
/// the end of its connection.
pub fn leave(endpoint: *Endpoint) void {
    switch (stateOf(endpoint).slot.role) {
        .producer => {},
        .consumer => consumerOf(endpoint).release(),
    }
}

pub fn layoutOf(endpoint: *const Endpoint) *const segment.Layout {
    return &stateOf(endpoint).layout;
}

// === The tests' hooks (core/test_hooks.zig) ===

/// A frame that doesn't belong, on the member's connection to the owner (which retires its slot).
pub fn sendStray(endpoint: *Endpoint) void {
    const state = stateOf(endpoint);
    const frame = frames.encodeJoin(platform.pid(), state.slot.role);
    _ = platform.sendFrame(endpoint.handles.get(.connected).?, &frame, null);
}

/// The sleeps the waits of the member `endpoint` began: a consumer's parks, a producer's sleeps for room.
pub fn sleepsOf(endpoint: *Endpoint) u32 {
    return switch (stateOf(endpoint).slot.role) {
        .producer => producerOf(endpoint).sleeps.load(.seq_cst),
        .consumer => consumerOf(endpoint).taker.sleeps.load(.seq_cst),
    };
}
