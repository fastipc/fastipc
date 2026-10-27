//! A work queue's owner side (docs/protocol.md §10): `create`, which claims the name and creates the `Queue`, with its
//! one segment; the admission of each joining producer and consumer, on the queue's poller (core/admission.zig), a
//! `Member` of the queue once joined; the members' ends, on the same poller; the count of the joined members
//! (`waitForMembers`); and the queue's end (`close`). The data path is producer.zig and consumer.zig, the members' side
//! client.zig.
//!
//! Objects. The queue handle (`fipc_queue_t`) is the `Queue`: its close ends the queue and frees it. The owner has no
//! member of its own: it joins its queue as any process does, through the name, and its poller admits it. Each slot has
//! a `Member` record, allocated with the queue. The poller never allocates from the creator's allocator (the returns'
//! scratch FIFO uses the page allocator), so that allocator needn't be thread-safe. A joined member's record holds its
//! control connection. One task of the queue's, its poller (`pollMembers`, on the queue's `Io`), accepts the joiners
//! and watches their connections and every member's (`platform.Poller`): a queue takes one thread however many members
//! it has. The slots (one roster per role, core/roster.zig), the records and the count are the queue's `members`
//! (core/members.zig), which the poller's steps below share with any kind of two roles over a ring set.
//!
//! A joiner (docs/protocol.md §10.3). The poller accepts it while a slot of either role is free (core/admission.zig
//! `accept`) and sends OFFER; at its JOIN, which names its role, it claims a slot of that role (none free: the joiner
//! is dropped, and joins again within its own timeout) and starts the member (core/members.zig `claimSlot`, `start`):
//! WELCOME, its watch, the count. A joiner that sends anything else, or nothing for core/admission.zig's
//! `join_deadline_ms`, is dropped.
//!
//! A member's end. Nothing travels on a member's connection after WELCOME, and the owner never closes a live member's
//! connection while the queue runs, nor a member its own before its close (client.zig): so the end of a member's
//! connection means that member writes the segment no more. The poller then, under the queue's lock, returns the
//! message a consumer held for another consumer (returns.zig) and frees its slot, or makes a producer's slot
//! draining: free once its ring settles (core/ring_set.zig `ringSettled`), which the poller looks at whenever it would
//! claim a slot, and every `settle_poll_ms` while it has room for nobody else. A consumer's message that waits for room
//! in the return ring keeps its slot held until it went, which the poller retries every `settle_poll_ms`. A member
//! whose connection fails otherwise, or which sends a frame, may still run: its slot is retired, out of use while the
//! queue lasts, and its connection stays open (its close would read as the queue's end to it) until the end. After any
//! member's end the poller rescans the rings (core/ring_set.zig `rescan`): a member that ended between two steps of a
//! handshake in the segment may have left a message without its ready bit. Once the queue has ended (`ended` in the
//! segment), the poller returns and frees nothing, and admits nobody.
//!
//! The end. `close` stops and awaits the poller, returns what it can without waiting, marks the segment (`ended`,
//! sequentially consistent), and closes the joiners' connections, the listening handle (the name is free) and every
//! member's connection, whose watchers then wake their members: producers return Disconnected, consumers take what is
//! left first. A member's watcher that reads the end of its connection marks the segment too, so the owner's process
//! ending ends the queue the same way, with nothing returned.
//!
//! The count. `members.joined` counts the joined members of each role, which `waitForMembers` reads on any thread;
//! each change of it, the cancel and the end wake the threads waiting on it (core/owner.zig `CountWaiters`).
//!
//! What every owner holds (the `Io`, the lock, the admission, the poller, the segment's handle) is the queue's `owner`
//! (core/owner.zig). Its lock guards `members` (the rosters, `draining`, the records), the returns and `closed`.

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
const returns_module = @import("returns.zig");
const segment = @import("segment.zig");
const test_hooks = @import("../core/test_hooks.zig");

const Atomic = std.atomic.Value;
const Joiner = @import("../core/admission.zig").Joiner;
const Owner = owner_module.Owner;
const Slot = segment.Slot;
const Role = segment.Role;
const Session = @import("../core/shared_memory.zig").Session;

/// `fipc_queue_wait_for_members`'s errors: each is the `fipc_result_t` code of the same name.
pub const WaitError = error{ Invalid, Timeout, Cancelled, Disconnected };

/// The queue's members (core/members.zig): its producers' and consumers' slots, records and count.
pub const Members = members_module.Members(segment.Rings);

/// A slot's record, owner side (core/members.zig `Member`).
pub const Member = Members.Member;

pub const Queue = struct {
    /// The `Io`, the lock (`owner.lock`), the admission, the poller task (`pollMembers`), the segment's handle, and
    /// `closed`: `close` ended the queue, nothing more is admitted, recovered or freed.
    owner: Owner,
    /// The owner's mapping of the segment.
    session: Session,
    layout: segment.Layout,
    /// The slots of each role, as the owner gave them out, the draining producer slots, one record per slot, and the
    /// joined members of each role (under the owner's lock; the count is read anywhere, `waitForMembers`).
    members: Members,
    /// Set by `cancel` (any thread), never cleared.
    cancelled: Atomic(bool) = .init(false),
    /// The poller saw the end's mark a member's watcher stored, and woke the count's waiters. Only the poller uses it.
    end_seen: bool = false,
    /// The messages of ended consumers, returned for others (under the owner's lock; its wakes are the poller's, then
    /// the close's).
    returns: returns_module.Returns,

    /// The queue has ended: `close`, or a member's watcher, marked the segment.
    pub fn hasEnded(queue: *const Queue) bool {
        return Members.hasEnded(&queue.owner, &queue.layout);
    }
};

// === The queue handle ===

/// `fipc_queue_create`: validates the arguments, claims the name and creates the queue: its segment, whole (an empty
/// queue of rings of `capacity` bytes, a power of two from 4 KiB to 2 GiB, for up to `max_producers` producers, 1 to
/// 63, and `max_consumers` consumers, 1 to 64), the members' records, the returns' memory, and its poller task, which
/// admits the members from now on. Doesn't wait.
pub fn create(gpa: Allocator, name: []const u8, capacity: usize, max_producers: u32, max_consumers: u32) endpoint_module.CreateError!*Queue {
    if (!names.isValid(name) or !segment.valid(capacity, max_producers, max_consumers)) return error.Invalid;
    const queue = gpa.create(Queue) catch return error.NoMemory;
    errdefer gpa.destroy(queue);
    const records = gpa.alloc(Member, max_producers + max_consumers) catch return error.NoMemory;
    errdefer gpa.free(records);
    queue.* = .{
        .owner = undefined,
        .session = undefined,
        .layout = undefined,
        .members = .init(records, max_producers, max_consumers),
        .returns = undefined,
    };
    try queue.owner.init(gpa, name);
    errdefer process_state.release(queue.owner.deinit());
    queue.members.joinRegistry();
    errdefer queue.members.leaveRegistry();
    queue.session = segment.create(queue.owner.io, queue.owner.segmentHandle(), &queue.owner.security, @intCast(capacity), max_producers, max_consumers) catch
        return error.NoMemory;
    errdefer queue.session.unmap();
    queue.layout = .of(&queue.session, max_producers, max_consumers);
    queue.returns = returns_module.Returns.init(gpa, queue.layout, queue.session.id) catch return error.NoMemory;
    errdefer queue.returns.deinit(gpa);
    try queue.owner.startPoller(Queue, pollMembers, queue);
    return queue;
}

/// `fipc_queue_close`: the queue ends (the module's comment). The poller is stopped and awaited, and one last tick of
/// the returns publishes what fits, without waiting (what doesn't is dropped and logged). Then the mark, sequentially
/// consistent: a publish loads it after its `tail` (the producers', and the owner's onto the return ring before it), a
/// consumer's drain loads `tail`s after it, so a send that returned OK, and a returned message, is drained
/// (producer.zig). Then the joiners' connections and the listening handle are closed (the name is free), then every
/// member's connection; then the queue is freed. A forked child's close of an inherited queue does nothing: the child
/// has no poller, and the queue is its parent's.
pub fn close(queue: *Queue) void {
    if (queue.owner.handles.forked) return;
    queue.owner.stopPoller();
    {
        queue.owner.lock();
        defer queue.owner.unlock();
        // Once a member's watcher (or a corrupt peer) stored the mark, consumers may have drained: nothing more is
        // published
        if (queue.layout.header.ended.load(.seq_cst) == 0) _ = queue.returns.tick();
        queue.returns.reportLeft();
    }
    queue.layout.header.ended.store(1, .seq_cst);
    const io_ref = queue.owner.deinit();
    queue.members.closeConnections();
    const gpa = queue.owner.gpa;
    queue.returns.deinit(gpa);
    queue.members.deinit(gpa);
    queue.session.unmap();
    gpa.destroy(queue);
    process_state.release(io_ref);
}

/// `fipc_queue_cancel`: every `waitForMembers` that waits, now or later, returns Cancelled. Any thread.
pub fn cancel(queue: *Queue) void {
    if (queue.owner.handles.forked) return;
    queue.cancelled.store(true, .release);
    queue.owner.wakeCountWaiters();
}

/// `fipc_queue_wait_for_members`: waits until at least `at_least` members of `role` (null: of either role) are joined,
/// within `timeout_ms`, and stores how many are in `count` at every return but Invalid. Any thread: it reads the
/// queue's count only (core/owner.zig `CountWaiters`). Disconnected once the queue has ended; Cancelled after `cancel`,
/// or the caller's `Io`'s (the cancelation stays pending); Invalid for `at_least` over the role's maximum (their sum
/// for null), and on an inherited queue.
pub fn waitForMembers(queue: *Queue, role: ?Role, at_least: u32, timeout_ms: c_int, count: *u32) WaitError!void {
    if (queue.owner.handles.forked) {
        process_state.reportInherited("queue");
        return error.Invalid;
    }
    const most = if (role) |r| queue.members.roster(r).slot_count else queue.members.senders.slot_count + queue.members.takers.slot_count;
    if (at_least > most) return error.Invalid;
    const counted: CountedRole = .{ .queue = queue, .role = role };
    return queue.owner.count_waiters.waitUntil(queue.owner.io, at_least, timeout_ms, count, counted, CountedRole.look);
}

/// The members `waitForMembers` counts: those of `role`, or of either role (null).
const CountedRole = struct {
    queue: *Queue,
    role: ?Role,

    /// The count, and what ends the wait: the queue's end, then the cancel.
    fn look(counted: CountedRole) owner_module.CountWaiters.Look {
        return .{
            .count = counted.queue.members.joinedOf(counted.role),
            .stop = if (counted.queue.hasEnded()) error.Disconnected else if (counted.queue.cancelled.load(.acquire)) error.Cancelled else null,
        };
    }
};

// === The poller: admission and the members' ends ===

/// How often the poller looks at the draining producer slots while it has room for no joiner, and retries the
/// returns while any waits.
const settle_poll_ms = 5;

/// The queue's poller task: listens for joiners while a slot of either role is free, accepts each one
/// (`acceptJoiner`), waits for the joiners' and the members' connections and handles each one that has something
/// (`onJoinerReady`, `onMemberReady`), ticks the returns, and drops the joiners whose JOIN is overdue, until `close`
/// stops it; then `drain`s the poller, so nothing of it is left on this thread. Once the queue has ended it admits
/// nobody.
fn pollMembers(queue: *Queue) void {
    const owner = &queue.owner;
    var ready: [64]*platform.Watched = undefined;
    while (true) {
        const returning = tickReturns(queue);
        const room = queue.members.roomFor(owner, &queue.layout);
        if (room.ended and !queue.end_seen) {
            queue.end_seen = true;
            owner.admission.dropAll(&owner.poller);
            owner.wakeCountWaiters();
        }
        owner.admission.listenWhile(&owner.poller, room.free);
        var timeout = owner.admission.waitMs();
        if ((!room.free and room.draining) or returning) timeout = @min(timeout orelse settle_poll_ms, settle_poll_ms);
        const polled = owner.poller.wait(&ready, timeout) orelse break;
        if (polled.listening) acceptJoiner(queue);
        for (polled.ready) |watched| {
            if (owner.admission.joinerOf(watched)) |joiner| onJoinerReady(queue, joiner) else onMemberReady(queue, @fieldParentPtr("watched", watched));
        }
        while (owner.admission.overdue()) |joiner| owner.admission.drop(&owner.poller, joiner);
    }
    owner.poller.drain();
}

/// The returns' tick (returns.zig), unless the queue has ended: the consumer slots whose message went are free.
/// Whether returns still wait.
fn tickReturns(queue: *Queue) bool {
    queue.owner.lock();
    defer queue.owner.unlock();
    if (queue.hasEnded() or !queue.returns.pending()) return false;
    var freed = queue.returns.tick().iterator(.{});
    while (freed.next()) |index| {
        const slot: Slot = .{ .role = .consumer, .index = @intCast(index) };
        queue.members.endSlotLocked(&queue.layout, slot, queue.members.memberOf(slot).generation);
    }
    return queue.returns.pending();
}

/// A joiner on the listening handle (docs/protocol.md §10.3): its accept, OFFER with the segment's descriptor, and the
/// watch of its connection for JOIN.
fn acceptJoiner(queue: *Queue) void {
    const owner = &queue.owner;
    const joiner = owner.admission.accept(&owner.poller) orelse return;
    const frame = frames.encodeOffer(platform.pid(), .{
        .capacity = queue.session.capacity,
        .segment_size = segment.describe(queue.session.capacity, queue.layout.max_senders, queue.layout.max_takers).segment_size,
        .session_id = queue.session.id,
        .max_producers = queue.layout.max_senders,
        .max_consumers = queue.layout.max_takers,
    });
    _ = owner.admission.offer(&owner.poller, joiner, &frame, owner.segmentHandle());
}

/// A joiner's connection has bytes or has ended: its JOIN, then its slot (docs/protocol.md §10.3, core/members.zig
/// `claimSlot`) and the member's start (`start`). Its end, a read error or anything but a queue's JOIN drops the
/// joiner, as does a role without a free slot: the joiner reads the end of its connection before WELCOME and joins
/// again, within its own timeout.
fn onJoinerReady(queue: *Queue, joiner: *Joiner) void {
    const owner = &queue.owner;
    const frame = switch (owner.admission.take(&owner.poller, joiner)) {
        .waiting => return,
        .gone => return owner.admission.drop(&owner.poller, joiner),
        .frame => |frame| frame,
    };
    const role = frames.checkJoin(frame) orelse {
        log.warn(@src(), "protocol error: a frame other than a work queue's JOIN; the joiner is dropped", .{});
        return owner.admission.drop(&owner.poller, joiner);
    };
    if (build_options.test_hooks) test_hooks.crashIfAt(.join_read);
    const claim = queue.members.claimSlot(owner, &queue.layout, &queue.session.id, role) catch |err| {
        owner.admission.drop(&owner.poller, joiner);
        if (err == error.SystemResources) owner.admission.backOff();
        return;
    };
    const member = queue.members.memberOf(.{ .role = role, .index = claim.slot });
    owner.admission.handOver(&owner.poller, joiner, &member.handles);
    queue.members.start(owner, &queue.layout, member, claim.generation, frames.encodeWelcome);
}

/// A member's connection has bytes or has ended (docs/protocol.md §10.7), on the poller's thread (core/members.zig
/// `onMemberReady`): a consumer that left has its held message returned (`keepsSlot`), with the owner's wakes for the
/// rescan.
fn onMemberReady(queue: *Queue, member: *Member) void {
    queue.members.onMemberReady(&queue.owner, &queue.layout, queue.returns.wakes(), member, queue, keepsSlot);
}

/// At the end of the member of `slot`, which left: a consumer's held message is returned (returns.zig), and its slot
/// kept held while the message waits for room (true). Under the owner's lock.
fn keepsSlot(queue: *Queue, slot: Slot) bool {
    return slot.role == .consumer and queue.returns.recover(slot.index) == .waiting;
}

// === The tests' hooks (core/test_hooks.zig) ===

/// A frame that doesn't belong (a second WELCOME) on the owner's control connections to each of its joined members:
/// each member's watcher then takes it for its own failure (client.zig `readOwnerFrames`).
pub fn sendStray(queue: *Queue) void {
    queue.owner.lock();
    defer queue.owner.unlock();
    for (queue.members.records) |*member| {
        if (member.state != .joined) continue;
        const frame = frames.encodeWelcome(platform.pid(), member.slot.role, member.slot.index, member.generation);
        if (member.handles.get(.connected)) |h| _ = platform.sendFrame(h, &frame, null);
    }
}
