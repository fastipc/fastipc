//! A broadcast's owner side (docs/protocol.md §9): `create`, which claims the name and creates the `Channel`, with the
//! broadcast's one segment and its publisher; the admission of each joining subscriber, on the channel's poller
//! (core/admission.zig), a `Member` of the channel once joined; the members' ends; and the publisher's close, which
//! ends the broadcast (`closePublisher`). The publisher's data path is writer.zig, and the subscriber's side
//! client.zig.
//!
//! Objects. The publisher's connection owns the channel: its close ends the broadcast and frees the channel. Each slot
//! has a `Member` record, allocated with the channel: the poller never allocates, so the allocator needn't be
//! thread-safe. A joined subscriber's record holds its control connection. One task of the channel's, its poller
//! (`pollMembers`, on the channel's `Io`), accepts the joiners and watches their connections and every member's
//! (`platform.Poller`): a channel takes one thread however many subscribers it has.
//!
//! A joiner (docs/protocol.md §9.3). The poller accepts it while a slot is free (core/admission.zig `accept`), claims a
//! slot for it and sends OFFER; at its JOIN it starts the member (`startMember`): WELCOME, its watch, the count. A
//! joiner that sends anything else, or nothing for core/admission.zig's `join_deadline_ms`, is dropped and its slot
//! freed.
//!
//! A member's end. The subscriber's process ends or closes the connection: the poller sees its end and, under the
//! owner's lock, frees the slot (the gated bit and the owner's roster too) and closes the connection; then it wakes
//! the publisher's sends, which may be waiting for that subscriber. A subscriber closes its connection only once it
//! writes its slot no more (client.zig `endSubscription`), so the slot may go to the next joiner. A member that ends any
//! other way (a read that fails, a frame that doesn't belong) may still be running and writing its slot: the slot is
//! retired instead, out of use until the channel's teardown, and the sends no longer wait for it. The publisher's close
//! stops and awaits the poller, then closes each member's connection itself: the subscribers then read the end of their
//! control connections and report Disconnected once they have received what was published.
//!
//! The count. `joined` counts the joined members the publisher hasn't unsubscribed, which `waitForMembers` reads on any
//! thread; each change of it and the publisher's cancel wake the threads waiting on it (core/owner.zig
//! `CountWaiters`).
//!
//! What every owner holds (the `Io`, the lock, the admission, the poller, the segment's handle) is the channel's
//! `owner` (core/owner.zig). Its lock guards the slot table, the members' records, `joined` and `closed`.
//!
//! Who runs where: the publisher's thread sends; the poller runs on the channel's `Io`, admits joiners and ends
//! members; any thread may count them (`waitForMembers`); the publisher's close ends it all.

const std = @import("std");
const Allocator = std.mem.Allocator;
const build_options = @import("build_options");
const wire_protocol = @import("../wire_protocol.zig");
const control = @import("../core/control.zig");
const endpoint_module = @import("../core/endpoint.zig");
const frames = @import("frames.zig");
const log = @import("../core/log.zig");
const names = @import("../core/names.zig");
const owner_module = @import("../core/owner.zig");
const platform = @import("../platform.zig");
const process_state = @import("../core/process_state.zig");
const roster_module = @import("roster.zig");
const segment = @import("segment.zig");
const test_hooks = @import("../core/test_hooks.zig");
const wire = @import("../core/wire.zig");
const writer_module = @import("writer.zig");

const Atomic = std.atomic.Value;
const Endpoint = endpoint_module.Endpoint;
const Joiner = @import("../core/admission.zig").Joiner;
const MemberEnd = owner_module.MemberEnd;
const Owner = owner_module.Owner;
const Roster = roster_module.Roster;
const Session = @import("../core/shared_memory.zig").Session;

const DataError = endpoint_module.DataError;

pub const Channel = struct {
    /// The `Io`, the lock (`owner.lock`), the admission, the poller task (`pollMembers`), the segment's handle, and
    /// `closed`: the publisher is closed, no more members start.
    owner: Owner,
    /// The owner's mapping of the segment (all of it writable).
    session: Session,
    layout: segment.Layout,
    max_stall_ms: i32,
    /// `max_stall_ms` isn't 0: gated subscribers may join.
    gated_allowed: bool,
    /// The slots, as the owner gave them out (roster.zig `Roster`): changed under the owner's lock, read by the
    /// publisher.
    roster: Roster,
    /// The publisher's own record of each cell's payload position (writer.zig `Writer.payload_positions`): a sixteenth
    /// of the capacity.
    payload_positions: []u32,
    /// One record per slot, `max_subscribers` of them: the slot's member while it is joined (under the owner's lock).
    members: []Member,
    /// How many joined members the publisher hasn't unsubscribed (`Member.state`): changed under the owner's lock, read
    /// anywhere (`waitForMembers`).
    joined: Atomic(u32) = .init(0),
    /// The publishing thread's.
    writer: writer_module.Writer align(128),
};

/// A slot's record, owner side: the subscriber that holds the slot while it is joined.
pub const Member = struct {
    channel: *Channel,
    slot: u32,
    generation: u32 = 0,
    /// `.connected`: the subscriber's control connection, while it is joined.
    handles: process_state.Handles = .{},
    /// The poller's record of the connection, from the member's start until its end.
    watched: platform.Watched = undefined,
    /// Under the owner's lock. `joined`: the subscriber is joined and counted (`Channel.joined`). `unsubscribing`:
    /// still joined, but the publisher sent it UNSUBSCRIBE (`onStall`), once, and stopped counting it; its UNSUBSCRIBED
    /// then ends the member as its end would.
    state: enum { free, joined, unsubscribing } = .free,
};

comptime {
    // fipc.h: the publisher's process keeps "up to 512 bytes per subscriber"
    std.debug.assert(@sizeOf(Member) <= 512);
}

// === The publisher's objects ===

/// `fipc_broadcast_create`: validates the arguments, claims the name and creates the channel: its segment (an empty
/// broadcast of `capacity` bytes, a power of two from 4 KiB to 2 GiB, and `max_subscribers` slots, 1 to 1024), the
/// members' records, its poller task, which admits the subscribers from now on, and the publisher's connection, which
/// is returned. Doesn't wait.
pub fn create(
    gpa: Allocator,
    name: []const u8,
    capacity: usize,
    max_subscribers: u32,
    max_stall_ms: i32,
) endpoint_module.CreateError!*Endpoint {
    if (!names.isValid(name) or !segment.valid(capacity, max_subscribers)) return error.Invalid;
    const channel = gpa.create(Channel) catch return error.NoMemory;
    errdefer gpa.destroy(channel);
    const payload_positions = gpa.alloc(u32, capacity / wire_protocol.broadcast_payload_align) catch return error.NoMemory;
    errdefer gpa.free(payload_positions);
    const members = gpa.alloc(Member, max_subscribers) catch return error.NoMemory;
    errdefer gpa.free(members);
    channel.* = .{
        .owner = undefined,
        .session = undefined,
        .layout = undefined,
        .max_stall_ms = max_stall_ms,
        .gated_allowed = max_stall_ms != 0,
        .roster = .{ .slots = .{ .slot_count = max_subscribers } },
        .payload_positions = payload_positions,
        .members = members,
        .writer = undefined,
    };
    try channel.owner.init(gpa, name);
    errdefer process_state.release(channel.owner.deinit());
    for (members, 0..) |*member, slot| {
        member.* = .{ .channel = channel, .slot = @intCast(slot) };
        member.handles.join();
    }
    errdefer for (members) |*member| member.handles.leave();
    const capacity_u32: u32 = @intCast(capacity);
    channel.session = segment.create(channel.owner.io, channel.owner.segmentHandle(), &channel.owner.security, capacity_u32, max_subscribers, channel.gated_allowed) catch
        return error.NoMemory;
    errdefer channel.session.unmap();
    channel.layout = .of(&channel.session, max_subscribers);
    channel.writer = .init(channel.layout, &channel.roster, channel.payload_positions, max_stall_ms, onStall, channel);
    const publisher = endpoint_module.create(gpa) catch return error.NoMemory;
    errdefer endpoint_module.free(publisher);
    try channel.owner.startPoller(Channel, pollMembers, channel);
    publisher.publishView(.{ .object = .{
        .kind = .broadcast_publisher,
        .session = null,
        .object = channel,
        .capacity = capacity_u32,
    } });
    publisher.hot.phase.store(.connected, .seq_cst);
    return publisher;
}

/// `fipc_close` of the publisher: the broadcast ends. The subscribers end at once (writer.zig `Writer.endChannel`); the
/// poller is stopped and awaited; the joiners' connections and the listening handle are closed (the name is free); then
/// every member's connection, so each subscriber reads the end and reports Disconnected once it has received what was
/// published; then the channel is freed. The caller frees the publisher's endpoint.
pub fn closePublisher(endpoint: *Endpoint) void {
    const channel = channelOf(endpoint);
    channel.writer.endChannel();
    channel.owner.stopPoller();
    const io_ref = channel.owner.deinit();
    for (channel.members) |*member| {
        member.handles.close(.connected);
        member.handles.leave();
    }
    channel.roster.slots.closeWakes();
    channel.session.unmap();
    const gpa = channel.owner.gpa;
    gpa.free(channel.members);
    gpa.free(channel.payload_positions);
    gpa.destroy(channel);
    process_state.release(io_ref);
}

// === The poller: admission and the members' ends ===

/// The channel's poller task: listens for joiners while a slot is free, accepts each one (`acceptJoiner`), waits for
/// the joiners' and the members' connections and handles each one that has something (`onJoinerReady`,
/// `onMemberReady`), and drops the joiners whose JOIN is overdue, until the publisher's close stops it; then `drain`s
/// the poller, so nothing of it is left on this thread.
fn pollMembers(channel: *Channel) void {
    const owner = &channel.owner;
    var ready: [64]*platform.Watched = undefined;
    while (true) {
        owner.admission.listenWhile(&owner.poller, hasFreeSlot(channel));
        const polled = owner.poller.wait(&ready, owner.admission.waitMs()) orelse break;
        if (polled.listening) acceptJoiner(channel);
        for (polled.ready) |watched| {
            if (owner.admission.joinerOf(watched)) |joiner| onJoinerReady(channel, joiner) else onMemberReady(channel, @fieldParentPtr("watched", watched));
        }
        while (owner.admission.overdue()) |joiner| dropJoiner(channel, joiner);
    }
    owner.poller.drain();
}

/// Whether a joiner can be accepted: the publisher is open and a slot is free.
fn hasFreeSlot(channel: *Channel) bool {
    channel.owner.lock();
    defer channel.owner.unlock();
    return !channel.owner.closed.load(.monotonic) and channel.roster.claim() != null;
}

/// A joiner on the listening handle (docs/protocol.md §9.3): its accept, a slot claimed for it, OFFER with the
/// segment's descriptor, and the watch of its connection for JOIN.
fn acceptJoiner(channel: *Channel) void {
    const owner = &channel.owner;
    const joiner = owner.admission.accept(&owner.poller) orelse return;
    const claim = claimSlot(channel) catch |err| {
        owner.admission.drop(&owner.poller, joiner);
        if (err == error.SystemResources) owner.admission.backOff();
        return;
    };
    joiner.setup.slot = claim;
    const frame = offerFrame(channel, claim);
    if (!owner.admission.offer(&owner.poller, joiner, &frame, owner.segmentHandle())) freeSlot(channel, claim.slot, claim.generation);
}

/// A joiner's connection has bytes or has ended: its JOIN, then the member's start. Its end, a read error, anything
/// but a JOIN for the offered slot, or a gated role the channel refuses drops the joiner (its slot is freed).
fn onJoinerReady(channel: *Channel, joiner: *Joiner) void {
    const owner = &channel.owner;
    const frame = switch (owner.admission.take(&owner.poller, joiner)) {
        .waiting => return,
        .gone => return dropJoiner(channel, joiner),
        .frame => |frame| frame,
    };
    const claim = joiner.setup.slot.?;
    const role = frames.checkJoin(frame, claim.slot, claim.generation) orelse {
        log.warn(@src(), "protocol error: a frame other than JOIN for the offered slot; the joiner is dropped", .{});
        return dropJoiner(channel, joiner);
    };
    switch (role) {
        .lossy => {},
        .gated => if (!channel.gated_allowed) return dropJoiner(channel, joiner),
    }
    if (build_options.test_hooks) test_hooks.crashIfAt(.join_read);
    const member = &channel.members[claim.slot];
    owner.admission.handOver(&owner.poller, joiner, &member.handles);
    startMember(channel, member, claim.generation, role);
}

/// A joiner's setup ends: its slot is freed, then its connection closed.
fn dropJoiner(channel: *Channel, joiner: *Joiner) void {
    const claim = joiner.setup.slot.?;
    freeSlot(channel, claim.slot, claim.generation);
    channel.owner.admission.drop(&channel.owner.poller, joiner);
}

/// Claims a slot for a joiner (docs/protocol.md §9.3), before its OFFER: its wake where each slot has one (created at
/// the slot's first claim, kept until the teardown), a zero cursor, and the roster's hold. Full: no slot is free, or
/// the publisher is closed.
fn claimSlot(channel: *Channel) error{ Full, SystemResources }!Roster.Claim {
    channel.owner.lock();
    defer channel.owner.unlock();
    if (channel.owner.closed.load(.monotonic)) return error.Full;
    const claim = channel.roster.claim() orelse return error.Full;
    if (platform.wakes_each_slot and channel.roster.slots.wakes[claim.slot] == null) {
        channel.roster.slots.wakes[claim.slot] = platform.createSlotWake(&channel.session.id, segment.slot_wake_suffix, claim.slot, &channel.owner.security) catch
            return error.SystemResources;
    }
    channel.layout.slots[claim.slot].cursor.store(0, .monotonic);
    channel.roster.hold(claim.slot, claim.generation);
    return claim;
}

/// Frees a slot held under `generation` (`endSlotLocked`), then wakes the publisher's sends, which may wait for the
/// slot's subscriber.
fn freeSlot(channel: *Channel, slot: u32, generation: u32) void {
    const freed = freed: {
        channel.owner.lock();
        defer channel.owner.unlock();
        break :freed endSlotLocked(channel, slot, generation, .release);
    };
    if (freed) segment.wakeWriter(&channel.layout);
}

/// A slot held under `generation` is free again (`.release`), or retired (`.retire`: its subscriber may still be
/// running and writing it, so no later subscriber gets it while the channel lasts): in the roster, so that the sends
/// no longer wait for it or wake it, and its gated bit in the segment. The caller wakes the publisher's sends, which
/// may wait for the slot's subscriber. Nothing (false) for a slot held by a later claim.
fn endSlotLocked(channel: *Channel, slot: u32, generation: u32, how: enum { release, retire }) bool {
    const ended = switch (how) {
        .release => channel.roster.release(slot, generation),
        .retire => channel.roster.retire(slot, generation),
    };
    if (ended) segment.clearBit(&channel.layout.shared.gated, slot, .seq_cst);
    return ended;
}

/// OFFER for a claimed slot.
fn offerFrame(channel: *const Channel, claim: Roster.Claim) wire.Frame {
    return frames.encodeOffer(platform.pid(), .{
        .flags = if (channel.gated_allowed) wire_protocol.offer_flag_gated else 0,
        .capacity = channel.session.capacity,
        .segment_size = segment.describe(channel.session.capacity, channel.layout.max_subscribers).segment_size,
        .session_id = channel.session.id,
        .slot = claim.slot,
        .generation = claim.generation,
        .max_subscribers = channel.layout.max_subscribers,
    });
}

/// The joiner's last step (docs/protocol.md §9.3), once its JOIN was read and its slot's record holds the connection.
/// Under the owner's lock, in order: the roster's gated mark, the subscriber's start (writer.zig `startSlot`: its
/// cursor, and a gated one's bit), the count (`joined`), WELCOME, the connection's watch, and the member's state.
///
/// The count includes the subscriber before its join can return, and every message sent once it does is at or after
/// the start. If the connection ended before its watch, the poller still reports it, later, on this thread, once the
/// member is joined. On failure:
///   - WELCOME failed, or the broadcast has ended (its publisher is closed): the member is uncounted, the connection
///     closed (the joiner reads its end) and the slot freed with its gated bit. This is safe because a joiner writes
///     nothing in the segment before it has read all of WELCOME, which a failed send never lets it do;
///   - the watch failed after WELCOME went out: the subscriber may be writing its slot, so it is uncounted and the
///     slot is retired.
fn startMember(channel: *Channel, member: *Member, generation: u32, role: wire_protocol.SubscriberRole) void {
    const owner = &channel.owner;
    const gated = role == .gated;
    const outcome: enum { joined, unwelcomed, unwatched, ended } = outcome: {
        owner.lock();
        defer owner.unlock();
        if (owner.closed.load(.monotonic)) break :outcome .ended;
        member.generation = generation;
        if (gated) channel.roster.holdGated(member.slot);
        writer_module.startSlot(&channel.layout, member.slot, gated);
        // `.release`: the start's stores before it, for a count that sees the subscriber (`waitForMembers`)
        _ = channel.joined.fetchAdd(1, .release);
        const connection = member.handles.get(.connected).?;
        const welcome = frames.encodeSlotFrame(.welcome, platform.pid(), member.slot, generation);
        if (!platform.sendFrame(connection, &welcome, null)) {
            _ = channel.joined.fetchSub(1, .monotonic);
            break :outcome .unwelcomed;
        }
        if (build_options.test_hooks) test_hooks.crashIfAt(.welcome_sent);
        owner.poller.watch(&member.watched, connection) catch {
            _ = channel.joined.fetchSub(1, .monotonic);
            _ = endSlotLocked(channel, member.slot, generation, .retire);
            break :outcome .unwatched;
        };
        member.state = .joined;
        break :outcome .joined;
    };
    switch (outcome) {
        .joined => {
            owner.wakeCountWaiters();
            if (build_options.test_hooks) test_hooks.crashIfAt(.member_watching);
        },
        .unwatched => {
            log.warn(@src(), "the subscriber of slot {d} is dropped (its control connection can't be watched): the slot stays out of use while the broadcast lasts", .{member.slot});
            member.handles.close(.connected);
            segment.wakeWriter(&channel.layout);
            owner.wakeCountWaiters();
        },
        .unwelcomed => {
            member.handles.close(.connected);
            freeSlot(channel, member.slot, generation);
            owner.wakeCountWaiters();
        },
        .ended => {
            member.handles.close(.connected);
            freeSlot(channel, member.slot, generation);
        },
    }
}

/// A member's connection has bytes or has ended (docs/protocol.md §9.7), on the poller's thread. The only frame
/// expected there is the subscriber's UNSUBSCRIBED after the publisher unsubscribed it, so the member ends at the
/// connection's end, at that answer, or at anything else. Under the owner's lock:
///   - the frame is taken;
///   - the slot is freed (the end, or UNSUBSCRIBED) or retired (anything else);
///   - the connection is unwatched and closed;
///   - the member is uncounted (unless its UNSUBSCRIBE already uncounted it) and its record freed.
/// Then the publisher's sends and the count's waiters are woken.
///
/// Only those two endings free the slot: a subscriber closes its end only once it no longer writes its slot, and
/// answers only once its mapping is gone. A failed read or a stray frame may leave it running with its slot mapped. A
/// failed send of UNSUBSCRIBE changes none of this: how the connection ends still decides.
fn onMemberReady(channel: *Channel, member: *Member) void {
    const owner = &channel.owner;
    const end: MemberEnd = end: {
        owner.lock();
        defer owner.unlock();
        const end = endOf(member, owner.poller.take(&member.watched)) orelse return;
        if (member.state == .joined) _ = channel.joined.fetchSub(1, .monotonic);
        member.state = .free;
        _ = endSlotLocked(channel, member.slot, member.generation, if (end == .left) .release else .retire);
        owner.poller.unwatch(&member.watched);
        // Last, after the slot's fate: a subscriber that reads its end finds the sends no longer waiting for it
        member.handles.close(.connected);
        break :end end;
    };
    if (end != .left) {
        log.warn(@src(), "the subscriber of slot {d} is dropped ({s}): the slot stays out of use while the broadcast lasts", .{ member.slot, end.reason() });
    }
    segment.wakeWriter(&channel.layout);
    owner.wakeCountWaiters();
}

/// How the member's connection ended, under the owner's lock (`Member.state`): the subscriber's end, or its
/// UNSUBSCRIBED once it is unsubscribing, is `.left`; any other frame is foreign. Null: only a part of a frame so far.
fn endOf(member: *const Member, taken: platform.Taken) ?MemberEnd {
    return switch (taken) {
        .partial => null,
        .ended => .left,
        .failed => .read_failed,
        .whole => if (member.state == .unsubscribing and
            frames.checkSlotFrame(&member.watched.frame, .unsubscribed, member.slot, member.generation)) .left else .foreign,
    };
}

/// The publisher's report of a gated subscriber held still for `max_stall_ms` while a send waits for it
/// (docs/protocol.md §9.7), on the publishing thread: UNSUBSCRIBE to that subscriber, once, under the owner's lock
/// (the poller closes a member's connection only under it, as it ends the member, so the frame never meets a closed
/// one); from then on it isn't counted (`joined`: the sends no longer deliver to it), and the count's waiters are
/// woken. The send goes on waiting within its timeout: the subscriber's UNSUBSCRIBED, or its end, frees the slot and
/// wakes it (`onMemberReady`). A subscriber whose whole process is stopped answers nothing until it runs again or
/// ends; nothing kills it.
fn onStall(context: *anyopaque, slot: u32, generation: u32) bool {
    const channel: *Channel = @ptrCast(@alignCast(context));
    const owner = &channel.owner;
    const outcome: enum { sent, failed, nothing } = outcome: {
        owner.lock();
        defer owner.unlock();
        if (slot >= channel.members.len) break :outcome .nothing;
        const member = &channel.members[slot];
        if (member.state != .joined or member.generation != generation) break :outcome .nothing;
        const frame = frames.encodeSlotFrame(.unsubscribe, platform.pid(), slot, generation);
        if (!platform.sendFrame(member.handles.get(.connected).?, &frame, null)) break :outcome .failed;
        member.state = .unsubscribing;
        _ = channel.joined.fetchSub(1, .monotonic);
        break :outcome .sent;
    };
    if (outcome == .sent) {
        owner.wakeCountWaiters();
        log.warn(@src(), "the gated subscriber of slot {d} held the broadcast's publisher back for max_stall_ms: it is unsubscribed", .{slot});
    }
    // A failed send is tried again at the publisher's next wait
    return outcome != .failed;
}

/// A frame that doesn't belong (a second WELCOME) on the publisher's control connections to each of its members, for
/// the tests (core/test_hooks.zig `sendStray`).
pub fn sendStray(endpoint: *Endpoint) void {
    const channel = channelOf(endpoint);
    channel.owner.lock();
    defer channel.owner.unlock();
    for (channel.members) |*member| {
        if (member.state == .free) continue;
        const frame = frames.encodeSlotFrame(.welcome, platform.pid(), member.slot, member.generation);
        if (member.handles.get(.connected)) |h| _ = platform.sendFrame(h, &frame, null);
    }
}

/// The segment's layout in the owner's mapping, of the channel of the publisher `endpoint`, for the tests
/// (core/test_hooks.zig `channelLayout`).
pub fn layoutOf(endpoint: *Endpoint) *const segment.Layout {
    return &channelOf(endpoint).layout;
}

// === The publisher's connection (core/calls.zig) ===

/// The publisher's channel (its view's object).
pub fn channelOf(endpoint: *const Endpoint) *Channel {
    return @ptrCast(@alignCast(endpoint.hot.view_object.load(.monotonic).?));
}

/// `fipc_broadcast_wait_for_members` (core/calls.zig `waitForMembers`): waits until at least `at_least` subscribers
/// are joined (`joined`), within `timeout_ms`, and stores how many are in `count` at every return but Invalid. Any
/// thread: it reads the channel's count only (core/owner.zig `CountWaiters`). Cancelled after the publisher's cancel,
/// or the caller's `Io`'s (the cancelation stays pending); Invalid for `at_least` over `max_subscribers`.
pub fn waitForMembers(endpoint: *Endpoint, at_least: u32, timeout_ms: c_int, count: *u32) DataError!void {
    const channel = channelOf(endpoint);
    if (at_least > channel.layout.max_subscribers) return error.Invalid;
    return channel.owner.count_waiters.waitUntil(endpoint.io, at_least, timeout_ms, count, endpoint, lookAtCount);
}

/// What `waitForMembers` finds at each look: the count (.acquire: each subscriber it counts has its start, at or before
/// the next message the caller sends, `startMember`), and the publisher's cancel.
fn lookAtCount(endpoint: *Endpoint) owner_module.CountWaiters.Look {
    return .{
        .count = channelOf(endpoint).joined.load(.acquire),
        .stop = if (endpoint.hot.requests.load(.monotonic).cancel) error.Cancelled else null,
    };
}

/// The local interrupt of the publisher (core/endpoint.zig `interrupt`, under the endpoint mutex): its wait for room
/// and its count's waiters.
pub fn interrupt(endpoint: *Endpoint) void {
    const channel = channelOf(endpoint);
    segment.wakeWriter(&channel.layout);
    channel.owner.wakeCountWaiters();
}
