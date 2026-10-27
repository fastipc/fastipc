//! A service's clients and servers, their side (docs/protocol.md §11.3, §11.7): `join`, which sets one up on the
//! caller's thread (`attemptJoin`): it connects to the name the owner listens on, waits for its OFFER, maps the
//! service's segment, answers JOIN with its role and waits for WELCOME, which gives it a slot. Then its watcher reads
//! the control connection, on which the owner sends nothing after WELCOME (`readOwnerFrames`): the connection's end is
//! the service's end (`endMembership`), and a frame on it is this member's own failure. The owner joins its own service
//! this way too: its poller admits it as any joiner. A client's requests and receives are caller.zig's, a server's
//! receives and replies responder.zig's; the steps every kind's client takes are core/control.zig's; the owner's side
//! is server.zig.
//!
//! A member closes its control connection only at its close (the watcher's stop), or once it read the service's end:
//! after its own failure the watcher keeps it open until the close, so that the owner frees the member's slot (and
//! abandons a request a server held, or drops a client's requests no server took) only once its process writes the
//! segment no more.

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

const Caller = @import("caller.zig").Caller;
const Endpoint = endpoint_module.Endpoint;
const Error = endpoint_module.Error;
const Responder = @import("responder.zig").Responder;
const Role = segment.Role;
const Session = @import("../core/shared_memory.zig").Session;
const Slot = segment.Slot;

/// A client's or server's state. All of it is set once, at the join, and read by any thread (the watcher's end, a
/// cancel).
pub const State = struct {
    slot: Slot,
    /// The member's view of the segment.
    layout: segment.Layout,
    generation: u32,
    /// Its own slot's wake (Windows), which it sleeps on for its data (a client's replies, a server's requests); null
    /// where it was gone at the join (the service had ended).
    own_wake: ?platform.SlotWake,
    /// A client's second wake (Windows), which its sending thread sleeps on for room; null for a server, and where it
    /// was gone.
    room_wake: ?platform.SlotWake,
    /// A client's payload position record: a sixteenth of the capacity.
    positions: []u32,
};

/// A member: its view's object, one allocation. `side`, its Caller or Responder, is the sending and receiving threads',
/// on cache lines of its own, so that no read of `state` touches what those threads write; the data path reaches it
/// from the view's object with no further load. No union: another thread's access to a union field would read the
/// whole union in a safe build (its tag's check), the threads' writes included.
fn Member(comptime Side: type) type {
    return struct {
        state: State,
        side: Side align(64),
    };
}

pub fn viewKind(role: Role) endpoint_module.ViewKind {
    return switch (role) {
        .client => .service_client,
        .server => .service_server,
    };
}

/// A member (`Member`), on its view of the segment (`layout`): a client starts at its ring's `tail` (Invalid: the
/// ring's indices are a corrupt peer's), a server at the ring its slot maps to. It owns `own_wake` and `room_wake` once
/// it is returned. The view's object.
pub fn newMember(
    gpa: Allocator,
    layout: segment.Layout,
    slot: Slot,
    generation: u32,
    session_id: [16]u8,
    own_wake: ?platform.SlotWake,
    room_wake: ?platform.SlotWake,
) error{ NoMemory, Invalid }!*anyopaque {
    const event = platform.slotRingEvent(own_wake);
    const state: State = .{ .slot = slot, .layout = layout, .generation = generation, .own_wake = own_wake, .room_wake = room_wake, .positions = &.{} };
    switch (slot.role) {
        .client => {
            const member = gpa.create(Member(Caller)) catch return error.NoMemory;
            errdefer gpa.destroy(member);
            const positions = gpa.alloc(u32, @intCast(layout.cellCount())) catch return error.NoMemory;
            errdefer gpa.free(positions);
            member.state = state;
            member.side.init(layout, slot.index, positions, platform.slotRingEvent(room_wake), event, session_id) catch return error.Invalid;
            member.state.positions = positions;
            return member;
        },
        .server => {
            const member = gpa.create(Member(Responder)) catch return error.NoMemory;
            member.* = .{ .state = state, .side = .init(layout, slot.index, event, session_id) };
            return member;
        },
    }
}

/// Frees a member of `role` (`object`, the view's), with the wakes it opened.
pub fn freeMember(gpa: Allocator, role: Role, object: *anyopaque) void {
    switch (role) {
        .client => {
            const member: *Member(Caller) = @ptrCast(@alignCast(object));
            member.side.deinit();
            freeState(gpa, &member.state);
            gpa.destroy(member);
        },
        .server => {
            const member: *Member(Responder) = @ptrCast(@alignCast(object));
            member.side.deinit();
            freeState(gpa, &member.state);
            gpa.destroy(member);
        },
    }
}

fn freeState(gpa: Allocator, state: *State) void {
    gpa.free(state.positions);
    if (platform.wakes_each_slot) {
        if (state.own_wake) |wake| platform.closeSlotWake(wake);
        if (state.room_wake) |wake| platform.closeSlotWake(wake);
    }
}

fn memberOf(comptime Side: type, endpoint: *const Endpoint) *Member(Side) {
    return @ptrCast(@alignCast(endpoint.hot.view_object.load(.monotonic).?));
}

pub fn stateOf(endpoint: *const Endpoint) *State {
    return switch (endpoint.hot.view_kind.load(.monotonic)) {
        .service_client => &memberOf(Caller, endpoint).state,
        .service_server => &memberOf(Responder, endpoint).state,
        .stream, .broadcast_publisher, .broadcast_subscriber, .queue_producer, .queue_consumer => unreachable,
    };
}

pub fn callerOf(endpoint: *const Endpoint) *Caller {
    return &memberOf(Caller, endpoint).side;
}

pub fn responderOf(endpoint: *const Endpoint) *Responder {
    return &memberOf(Responder, endpoint).side;
}

// === The join ===

/// `fipc_service_join`: validates the name, creates the endpoint and joins the service on `name` as `role` on the
/// caller's thread, within `timeout` (`attemptJoin`, retried as a stream's handshake while nobody listens or no slot of
/// the role is free); then publishes the member's view and starts the watcher. Invalid also for a name a broadcast or a
/// queue holds, and a name a stream holds once its server accepts (SEGMENT comes only from `fipc_stream_accept`).
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

/// A joined member: its view of the service's segment, and its `Member`.
const Joined = struct {
    session: *Session,
    member: *anyopaque,
};

/// One attempt of the join (docs/protocol.md §11.3), as a queue member's (queue/client.zig `attemptJoin`). On failure
/// the endpoint holds no handle; a failure after WELCOME closes the connection, whose end the owner reads as this
/// member's (it wrote nothing in the segment).
fn attemptJoin(endpoint: *Endpoint, deadline: ?Io.Clock.Timestamp, role: Role) (Error || error{Retry})!Joined {
    errdefer {
        endpoint.handles.close(.received);
        endpoint.handles.close(.connected);
    }
    try control.connectOnce(endpoint, .member);
    if (build_options.test_hooks) test_hooks.crashIfAt(.connected);
    const offer_frame = try control.awaitOffer(endpoint, deadline, .service);
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
        .client => offer.max_clients,
        .server => offer.max_servers,
    };
    const welcome = frames.checkWelcome(&frame, role, max_slots) orelse {
        log.warn(@src(), "protocol error: a frame other than WELCOME after JOIN", .{});
        return error.Retry;
    };
    if (build_options.test_hooks) test_hooks.crashIfAt(.welcomed);
    // The owner created the slot's wakes before WELCOME. Gone: the owner tore the service down since, so the service
    // has ended; the member goes on without them (its waits sleep in short slices)
    const own_wake = try openOwnWake(&offer.session_id, segment.wakeSuffix(role), welcome.slot);
    errdefer if (platform.wakes_each_slot) if (own_wake) |wake| platform.closeSlotWake(wake);
    const room_wake = if (role == .client) try openOwnWake(&offer.session_id, segment.client_room_wake_suffix, welcome.slot) else null;
    const layout = segment.layoutOf(session, offer.max_clients, offer.max_servers, offer.max_message);
    const member = newMember(endpoint.gpa, layout, .{ .role = role, .index = welcome.slot }, welcome.generation, offer.session_id, own_wake, room_wake) catch |err| {
        if (platform.wakes_each_slot) if (room_wake) |wake| platform.closeSlotWake(wake);
        if (err == error.Invalid) log.warn(@src(), "the ring of the offered client slot has corrupt indices; the join fails", .{});
        return err;
    };
    if (build_options.test_hooks) test_hooks.crashIfAt(.start_taken);
    return .{ .session = session, .member = member };
}

/// One of the member's own slot's wakes (Windows), which the owner created before WELCOME; null where it is gone (the
/// service has ended), and elsewhere.
fn openOwnWake(session_id: *const [16]u8, suffix: []const u8, slot: u32) (Error || error{Retry})!?platform.SlotWake {
    if (!platform.wakes_each_slot) return null;
    return platform.openSlotWake(session_id, suffix, slot) catch |err| switch (err) {
        error.Gone => null,
        error.OutOfMemory => error.NoMemory,
        error.AccessDenied => control.accessDenied(),
        else => error.Retry,
    };
}

// === The watcher ===

/// A member's watcher's read (core/calls.zig `readControl`). The owner sends nothing after WELCOME and never closes a
/// live member's connection while the service runs (server.zig), so the read returns only at an end:
///   - the end of the stream, or a failed read, is the service's end: false, and core/control.zig `runWatcher` applies
///     it (`endMembership`);
///   - a whole frame is this member's own failure (the owner is alive, and buggy): PEER_DEAD without the end's mark,
///     so its calls return Disconnected at once. The connection stays open until the member's close stops the watcher
///     (true), so that the owner frees the slot only once this process no longer writes the segment;
///   - the close's stop (error.Canceled comes only from it): true.
pub fn readOwnerFrames(endpoint: *Endpoint, connection: platform.Handle) bool {
    var frame: wire.Frame = undefined;
    const whole = control.readFrame(endpoint.io, connection, &frame) catch |err| return err == error.Canceled;
    if (!whole) return false;
    log.warn(@src(), "protocol error: the service's owner sent a frame after WELCOME; this member stops", .{});
    endpoint.hot.phase.store(.peer_dead, .seq_cst);
    endpoint.interrupt();
    while (!endpoint.loadRequests().shutdown) platform.sleep(1);
    return true;
}

/// The service's end, as a member's watcher applies it before it closes the connection: the end's mark in the segment
/// (sequentially consistent, before the phase: a client that sees the phase then loads the mark, and receives the
/// replies already done), then PEER_DEAD, then the member's local interrupt.
pub fn endMembership(endpoint: *Endpoint) void {
    layoutOf(endpoint).header.ended.store(1, .seq_cst);
    endpoint.hot.phase.store(.peer_dead, .seq_cst);
    endpoint.interrupt();
}

// === The member's lifecycle (core/calls.zig) ===

/// The local interrupt of a member (core/endpoint.zig `interrupt`, under the endpoint mutex, on any thread): a client's
/// waits for room and for a reply, a server's wait for a request, after the caller published what they must see. It
/// reads only what the join set, never the sending or receiving thread's state.
pub fn interrupt(endpoint: *Endpoint) void {
    const state = stateOf(endpoint);
    switch (state.slot.role) {
        .client => {
            const ring = state.layout.ring(state.slot.index);
            wakeOwn(&ring.room_sleeps, state.room_wake);
            wakeOwn(&ring.reply_sleeps, state.own_wake);
        },
        .server => wakeOwn(&state.layout.takerSlot(state.slot.index).sleeps, state.own_wake),
    }
}

fn wakeOwn(flag: *std.atomic.Value(u32), wake: ?platform.SlotWake) void {
    if (flag.swap(0, .seq_cst) == 1) platform.ringWake(flag, platform.slotRingEvent(wake));
}

/// A member's service has ended, as the native API reports it: the end's mark is in the segment.
pub fn memberEnded(endpoint: *Endpoint) bool {
    return layoutOf(endpoint).header.ended.load(.acquire) != 0;
}

/// A member's leave, at its close: a client lets a reply held in place go; a server abandons the request it holds
/// (responder.zig `leave`). Its state is freed once the watcher stopped and the mapping is gone (core/calls.zig
/// `freeObject`); its slot is freed by the owner, which reads the end of its connection.
pub fn leave(endpoint: *Endpoint) void {
    switch (stateOf(endpoint).slot.role) {
        .client => callerOf(endpoint).replies.release(),
        .server => responderOf(endpoint).leave(),
    }
}

/// `fipc_max_piece` on a member: the service's `max_message`, for requests and replies alike.
pub fn maxPiece(endpoint: *const Endpoint) usize {
    return stateOf(endpoint).layout.maxPiece();
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

/// The sleeps the receives of the member `endpoint` began: a client's waits for a reply, a server's parks.
pub fn sleepsOf(endpoint: *Endpoint) u32 {
    return switch (stateOf(endpoint).slot.role) {
        .client => callerOf(endpoint).replies.sleeps.load(.seq_cst),
        .server => responderOf(endpoint).taker.sleeps.load(.seq_cst),
    };
}

/// The sleeps for room a client's sends began.
pub fn roomSleepsOf(endpoint: *Endpoint) u32 {
    return callerOf(endpoint).sender.sleeps.load(.seq_cst);
}

/// The longest sleep of a client's receives for a reply (caller.zig `Replies.longest_sleep_ms`).
pub fn setReplySleepMs(endpoint: *Endpoint, ms: u32) void {
    callerOf(endpoint).replies.longest_sleep_ms = ms;
}
