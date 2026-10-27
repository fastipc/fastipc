//! A service's owner side (docs/protocol.md §11): `create`, which claims the name and creates the `Service`, with its
//! one segment; the admission of each joining client and server, on the service's poller (core/admission.zig), a
//! `Member` of the service once joined; the members' ends, on the same poller; the count of the joined members
//! (`waitForMembers`); and the service's end (`close`). The data path is caller.zig and responder.zig, the members'
//! side client.zig.
//!
//! Objects. The service handle (`fipc_service_t`) is the `Service`: its close ends the service and frees it. The owner
//! has no member of its own: it joins its service as any process does, through the name, and its poller admits it.
//! Each slot has a `Member` record, allocated with the service; the poller never allocates, so the creator's allocator
//! needn't be thread-safe. A joined member's record holds its control connection. One task of the service's, its
//! poller (`pollMembers`, on the service's `Io`), accepts the joiners and watches their connections and every
//! member's (`platform.Poller`): a service takes one thread however many members it has. The slots (one roster per
//! role, core/roster.zig), the records and the count are the service's `members` (core/members.zig), which it shares
//! with a work queue's owner.
//!
//! A joiner (docs/protocol.md §11.3), as a queue's: the poller accepts it while a slot of either role is free
//! (core/admission.zig `accept`) and sends OFFER; at its JOIN, which names its role, it claims a slot of that role
//! (none free: the joiner is dropped, and joins again within its own timeout) and starts the member (core/members.zig
//! `claimSlot`, `start`): WELCOME, its watch, the count. A joiner that sends anything else, or nothing for
//! core/admission.zig's `join_deadline_ms`, is dropped.
//!
//! A member's end. Nothing travels on a member's connection after WELCOME, and the owner never closes a live member's
//! connection while the service runs, nor a member its own before its close (client.zig): so the end of a member's
//! connection means that member writes the segment no more. The poller then, under the service's lock (`keepsSlot`):
//!   - for a server, answers the request it held, if any, as abandoned (core/ring_set.zig `abandonHeld`: its client's
//!     receive returns Abandoned, and nothing goes again), wakes the client of the request its intent names (one it
//!     answered just before it ended may not have been woken), and frees its slot;
//!   - for a client, drops its requests no server took (core/ring_set.zig `dropUnclaimed`: nobody waits for them), and
//!     makes its slot draining: free once its ring settles (core/ring_set.zig `ringSettled`: the servers that hold its
//!     requests answered them, into its ring), which the poller looks at whenever it would claim a slot, and every
//!     `settle_poll_ms` while it has room for nobody else.
//! A member whose connection fails otherwise, or which sends a frame, may still run: its slot is retired, out of use
//! while the service lasts, and its connection stays open (its close would read as the service's end to it) until the
//! end. A server retired for a frame is watched on: at its connection's end the request it holds then is abandoned, as
//! at any server's end, since its client receives no later answer before that one; its slot stays retired
//! (core/members.zig `watches_retired_takers`). Nothing of a retired client is dropped, nor of a server whose read
//! failed or whose connection can't be watched. After any member's end the poller rescans the rings (core/ring_set.zig
//! `rescan`). Once the service has ended (`ended` in the segment), the poller abandons, drops and frees nothing, and
//! admits nobody.
//!
//! The end. `close` stops and awaits the poller, marks the segment (`ended`, sequentially consistent), and closes the
//! joiners' connections, the listening handle (the name is free) and every member's connection, whose watchers then
//! wake their members: clients' sends and servers' receives return Disconnected, clients receive the replies already
//! done, then Disconnected. A member's watcher that reads the end of its connection marks the segment too, so the
//! owner's process ending ends the service the same way.
//!
//! The count. `members.joined` counts the joined members of each role, which `waitForMembers` reads on any thread;
//! each change of it, the cancel and the end wake the threads waiting on it (core/owner.zig `CountWaiters`).
//!
//! What every owner holds (the `Io`, the lock, the admission, the poller, the segment's handle) is the service's
//! `owner` (core/owner.zig). Its lock guards `members` (the rosters, `draining`, the records) and `closed`.

const std = @import("std");
const Allocator = std.mem.Allocator;
const build_options = @import("build_options");
const endpoint_module = @import("../core/endpoint.zig");
const frames = @import("frames.zig");
const log = @import("../core/log.zig");
const members_module = @import("../core/members.zig");
const names = @import("../core/names.zig");
const owner_module = @import("../core/owner.zig");
const platform = @import("../platform.zig");
const process_state = @import("../core/process_state.zig");
const segment = @import("segment.zig");
const test_hooks = @import("../core/test_hooks.zig");

const Atomic = std.atomic.Value;
const Joiner = @import("../core/admission.zig").Joiner;
const Owner = owner_module.Owner;
const Rings = segment.Rings;
const Role = segment.Role;
const Session = @import("../core/shared_memory.zig").Session;
const Slot = segment.Slot;

/// `fipc_service_wait_for_members`'s errors: each is the `fipc_result_t` code of the same name.
pub const WaitError = error{ Invalid, Timeout, Cancelled, Disconnected };

/// The service's members (core/members.zig): its clients' and servers' slots, records and count.
pub const Members = members_module.Members(Rings);

/// A slot's record, owner side (core/members.zig `Member`).
pub const Member = Members.Member;

pub const Service = struct {
    /// The `Io`, the lock (`owner.lock`), the admission, the poller task (`pollMembers`), the segment's handle, and
    /// `closed`: `close` ended the service, nothing more is admitted, abandoned, dropped or freed.
    owner: Owner,
    /// The owner's mapping of the segment.
    session: Session,
    layout: segment.Layout,
    /// The slots of each role, as the owner gave them out, the draining client slots, one record per slot, and the
    /// joined members of each role (under the owner's lock; the count is read anywhere, `waitForMembers`).
    members: Members,
    /// The owner's handles to the slots' wakes (Windows), for its abandons and rescans: the poller's, then the close's.
    wakes: segment.Wakes,
    /// Set by `cancel` (any thread), never cleared.
    cancelled: Atomic(bool) = .init(false),
    /// The poller saw the end's mark a member's watcher stored, and woke the count's waiters. Only the poller uses it.
    end_seen: bool = false,

    /// The service has ended: `close`, or a member's watcher, marked the segment.
    pub fn hasEnded(service: *const Service) bool {
        return Members.hasEnded(&service.owner, &service.layout);
    }
};

// === The service handle ===

/// `fipc_service_create`: validates the arguments, claims the name and creates the service: its segment, whole (rings
/// of `capacity` bytes, a power of two from 4 KiB to 2 GiB, for up to `max_clients` clients, 1 to 64, requests and
/// replies of up to `max_message` bytes, 1 to a quarter of the capacity, and up to `max_servers` servers, 1 to 64),
/// the members' records, and its poller task, which admits the members from now on. Doesn't wait.
pub fn create(gpa: Allocator, name: []const u8, capacity: usize, max_message: usize, max_clients: u32, max_servers: u32) endpoint_module.CreateError!*Service {
    if (!names.isValid(name) or !segment.valid(capacity, max_message, max_clients, max_servers)) return error.Invalid;
    const service = gpa.create(Service) catch return error.NoMemory;
    errdefer gpa.destroy(service);
    const records = gpa.alloc(Member, max_clients + max_servers) catch return error.NoMemory;
    errdefer gpa.free(records);
    service.* = .{
        .owner = undefined,
        .session = undefined,
        .layout = undefined,
        .members = .init(records, max_clients, max_servers),
        .wakes = undefined,
    };
    try service.owner.init(gpa, name);
    errdefer process_state.release(service.owner.deinit());
    service.members.joinRegistry();
    errdefer service.members.leaveRegistry();
    service.session = segment.create(service.owner.io, service.owner.segmentHandle(), &service.owner.security, @intCast(capacity), max_clients, max_servers, @intCast(max_message)) catch
        return error.NoMemory;
    errdefer service.session.unmap();
    service.layout = segment.layoutOf(&service.session, max_clients, max_servers, @intCast(max_message));
    service.wakes = .init(service.session.id);
    errdefer service.wakes.deinit();
    try service.owner.startPoller(Service, pollMembers, service);
    return service;
}

/// `fipc_service_close`: the service ends (the module's comment). The poller is stopped and awaited; then the mark,
/// sequentially consistent: a reply loads it after its done, a client's receive loads its request's state after it, so
/// a reply that returned OK is received (caller.zig). Then the joiners' connections and the listening handle are
/// closed (the name is free), then every member's connection; then the service is freed. A forked child's close of an
/// inherited service does nothing: the child has no poller, and the service is its parent's.
pub fn close(service: *Service) void {
    if (service.owner.handles.forked) return;
    service.owner.stopPoller();
    service.layout.header.ended.store(1, .seq_cst);
    const io_ref = service.owner.deinit();
    service.members.closeConnections();
    const gpa = service.owner.gpa;
    service.wakes.deinit();
    service.members.deinit(gpa);
    service.session.unmap();
    gpa.destroy(service);
    process_state.release(io_ref);
}

/// `fipc_service_cancel`: every `waitForMembers` that waits, now or later, returns Cancelled. Any thread.
pub fn cancel(service: *Service) void {
    if (service.owner.handles.forked) return;
    service.cancelled.store(true, .release);
    service.owner.wakeCountWaiters();
}

/// `fipc_service_wait_for_members`: waits until at least `at_least` members of `role` (null: of either role) are
/// joined, within `timeout_ms`, and stores how many are in `count` at every return but Invalid. Any thread: it reads
/// the service's count only (core/owner.zig `CountWaiters`). Disconnected once the service has ended; Cancelled after
/// `cancel`, or the caller's `Io`'s (the cancelation stays pending); Invalid for `at_least` over the role's maximum
/// (their sum for null), and on an inherited service.
pub fn waitForMembers(service: *Service, role: ?Role, at_least: u32, timeout_ms: c_int, count: *u32) WaitError!void {
    if (service.owner.handles.forked) {
        process_state.reportInherited("service");
        return error.Invalid;
    }
    const most = if (role) |r| service.members.roster(r).slot_count else service.members.senders.slot_count + service.members.takers.slot_count;
    if (at_least > most) return error.Invalid;
    const counted: CountedRole = .{ .service = service, .role = role };
    return service.owner.count_waiters.waitUntil(service.owner.io, at_least, timeout_ms, count, counted, CountedRole.look);
}

/// The members `waitForMembers` counts: those of `role`, or of either role (null).
const CountedRole = struct {
    service: *Service,
    role: ?Role,

    /// The count, and what ends the wait: the service's end, then the cancel.
    fn look(counted: CountedRole) owner_module.CountWaiters.Look {
        return .{
            .count = counted.service.members.joinedOf(counted.role),
            .stop = if (counted.service.hasEnded()) error.Disconnected else if (counted.service.cancelled.load(.acquire)) error.Cancelled else null,
        };
    }
};

// === The poller: admission and the members' ends ===

/// How often the poller looks at the draining client slots while it has room for no joiner.
const settle_poll_ms = 5;

/// The service's poller task: listens for joiners while a slot of either role is free, accepts each one
/// (`acceptJoiner`), waits for the joiners' and the members' connections and handles each one that has something
/// (`onJoinerReady`, `onMemberReady`), and drops the joiners whose JOIN is overdue, until `close` stops it; then
/// `drain`s the poller, so nothing of it is left on this thread. Once the service has ended it admits nobody.
fn pollMembers(service: *Service) void {
    const owner = &service.owner;
    var ready: [64]*platform.Watched = undefined;
    while (true) {
        const room = service.members.roomFor(owner, &service.layout);
        if (room.ended and !service.end_seen) {
            service.end_seen = true;
            owner.admission.dropAll(&owner.poller);
            owner.wakeCountWaiters();
        }
        owner.admission.listenWhile(&owner.poller, room.free);
        var timeout = owner.admission.waitMs();
        if (!room.free and room.draining) timeout = @min(timeout orelse settle_poll_ms, settle_poll_ms);
        const polled = owner.poller.wait(&ready, timeout) orelse break;
        if (polled.listening) acceptJoiner(service);
        for (polled.ready) |watched| {
            if (owner.admission.joinerOf(watched)) |joiner| onJoinerReady(service, joiner) else onMemberReady(service, @fieldParentPtr("watched", watched));
        }
        while (owner.admission.overdue()) |joiner| owner.admission.drop(&owner.poller, joiner);
    }
    owner.poller.drain();
}

/// A joiner on the listening handle (docs/protocol.md §11.3): its accept, OFFER with the segment's descriptor, and the
/// watch of its connection for JOIN.
fn acceptJoiner(service: *Service) void {
    const owner = &service.owner;
    const joiner = owner.admission.accept(&owner.poller) orelse return;
    const layout = &service.layout;
    const frame = frames.encodeOffer(platform.pid(), .{
        .capacity = service.session.capacity,
        .segment_size = segment.describe(service.session.capacity, layout.max_senders, layout.max_takers, layout.max_piece).segment_size,
        .session_id = service.session.id,
        .max_clients = layout.max_senders,
        .max_servers = layout.max_takers,
        .max_message = layout.max_piece,
    });
    _ = owner.admission.offer(&owner.poller, joiner, &frame, owner.segmentHandle());
}

/// A joiner's connection has bytes or has ended: its JOIN, then its slot (docs/protocol.md §11.3, core/members.zig
/// `claimSlot`) and the member's start (`start`). Its end, a read error or anything but a service's JOIN drops the
/// joiner, as does a role without a free slot: the joiner reads the end of its connection before WELCOME and joins
/// again, within its own timeout.
fn onJoinerReady(service: *Service, joiner: *Joiner) void {
    const owner = &service.owner;
    const frame = switch (owner.admission.take(&owner.poller, joiner)) {
        .waiting => return,
        .gone => return owner.admission.drop(&owner.poller, joiner),
        .frame => |frame| frame,
    };
    const role = frames.checkJoin(frame) orelse {
        log.warn(@src(), "protocol error: a frame other than a service's JOIN; the joiner is dropped", .{});
        return owner.admission.drop(&owner.poller, joiner);
    };
    if (build_options.test_hooks) test_hooks.crashIfAt(.join_read);
    const claim = service.members.claimSlot(owner, &service.layout, &service.session.id, role) catch |err| {
        owner.admission.drop(&owner.poller, joiner);
        if (err == error.SystemResources) owner.admission.backOff();
        return;
    };
    const member = service.members.memberOf(.{ .role = role, .index = claim.slot });
    owner.admission.handOver(&owner.poller, joiner, &member.handles);
    service.members.start(owner, &service.layout, member, claim.generation, frames.encodeWelcome);
}

/// A member's connection has bytes or has ended (docs/protocol.md §11.7), on the poller's thread (core/members.zig
/// `onMemberReady`): a server that left has the request it held abandoned, a client that left its requests no server
/// took dropped (`keepsSlot`), with the owner's wakes for the rescan.
fn onMemberReady(service: *Service, member: *Member) void {
    service.members.onMemberReady(&service.owner, &service.layout, &service.wakes, member, service, keepsSlot);
}

/// At the end of the member of `slot`, which left (it writes the segment no more; the module's comment): a server's
/// held request is answered as abandoned, and the client its intent names is woken (core/ring_set.zig `wakeIntended`:
/// a server that ended between its done and its wake left that client asleep); a client's requests no server took are
/// dropped. Neither keeps its slot held: a server's is free at once, a client's drains. Under the owner's lock.
fn keepsSlot(service: *Service, slot: Slot) bool {
    const layout = &service.layout;
    switch (slot.role) {
        .server => {
            if (Rings.heldClaim(layout, slot.index)) |claim| {
                if (!Rings.abandonHeld(layout, &service.wakes, slot.index, claim)) {
                    log.warn(@src(), "request {d} of the service's client ring {d} changed under its abandon from the server of slot {d}: a corrupt peer's write", .{ claim.index, claim.ring, slot.index });
                }
            }
            Rings.wakeIntended(layout, &service.wakes, slot.index);
        },
        .client => _ = Rings.dropUnclaimed(layout, slot.index),
    }
    return false;
}

// === The tests' hooks (core/test_hooks.zig) ===

/// A frame that doesn't belong (a second WELCOME) on the owner's control connections to each of its joined members:
/// each member's watcher then takes it for its own failure (client.zig `readOwnerFrames`).
pub fn sendStray(service: *Service) void {
    service.owner.lock();
    defer service.owner.unlock();
    for (service.members.records) |*member| {
        if (member.state != .joined) continue;
        const frame = frames.encodeWelcome(platform.pid(), member.slot.role, member.slot.index, member.generation);
        if (member.handles.get(.connected)) |h| _ = platform.sendFrame(h, &frame, null);
    }
}
