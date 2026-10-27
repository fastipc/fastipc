//! An owner's record of the members of a kind of two roles over a ring set (core/ring_set.zig: its senders and its
//! takers), as a work queue's owner (queue/server.zig) and a service's (service/server.zig) keep it: the slots of each
//! role (core/roster.zig), a record per slot (`Member`), the joined members of each role, and the sender slots that
//! drain. It is process-local memory under the owner's lock (core/owner.zig), which the owner's poller works on; the
//! kind's admission steps call in here:
//!   - a joiner's slot, once its JOIN named its role (`claimSlot`), and the member's start (`start`): WELCOME, its
//!     watch, the count;
//!   - a member's end (`onMemberReady`). Nothing travels on a member's connection after WELCOME, so its end means that
//!     member writes the segment no more: a taker's slot is free, a sender's draining, once the kind did what the end
//!     leaves to it (a queue returns the message a consumer held; a service abandons the request a server held, and
//!     drops a client's requests no server took). A member whose connection fails otherwise, or which
//!     sends a frame, may still run: its slot is retired, out of use while the kind lasts, and its connection stays
//!     open (its close would read as the end to it) until the end; where messages carry replies, a taker retired for
//!     a frame is watched on, and its connection's end runs the kind's steps at a member's end (a service abandons the
//!     request it holds then), its slot staying retired. Then the rings are rescanned (core/ring_set.zig `rescan`);
//!   - a draining sender slot is free once its ring settles (core/ring_set.zig `ringSettled`), which the owner looks
//!     at whenever it would claim a slot, and while it has room for nobody else (`roomFor`).
//! Once the kind has ended (`ended` in the segment, or the owner's close), nothing is freed and nobody is admitted.

const std = @import("std");
const build_options = @import("build_options");
const core_roster = @import("roster.zig");
const log = @import("log.zig");
const owner_module = @import("owner.zig");
const platform = @import("../platform.zig");
const process_state = @import("process_state.zig");
const test_hooks = @import("test_hooks.zig");

const Atomic = std.atomic.Value;
const Frame = @import("wire.zig").Frame;
const MemberEnd = owner_module.MemberEnd;
const Owner = owner_module.Owner;

/// The members of the kind whose ring set is `Rings` (core/ring_set.zig `RingSet`).
pub fn Members(comptime Rings: type) type {
    return struct {
        const Self = @This();
        const Layout = Rings.Layout;
        const Role = Rings.Role;
        const Slot = Rings.Slot;

        /// The slots of one role (up to 64).
        pub const Roster = core_roster.Roster(64);

        /// A slot's record, owner side: the member that holds the slot while it is joined, or whose slot is retired.
        pub const Member = struct {
            slot: Slot,
            generation: u32 = 0,
            /// `.connected`: the member's control connection, while it is joined or retired.
            handles: process_state.Handles = .{},
            /// The poller's record of the connection, from the member's start until its end.
            watched: platform.Watched = undefined,
            /// `joined`: watched and counted; `retired`: its slot is retired and its connection open until the kind's
            /// end; `retired_watched`: retired, and its connection watched for its end (`watches_retired_takers`; under
            /// the owner's lock).
            state: enum { free, joined, retired, retired_watched } = .free,
        };

        comptime {
            // fipc.h: the owner's process keeps "up to 512 bytes per producer and consumer"
            std.debug.assert(@sizeOf(Member) <= 512);
        }

        /// The slots, as the owner gave them out (under the owner's lock).
        senders: Roster,
        takers: Roster,
        /// Bit s: sender slot s has no sender since its last one ended, and stays held until its ring settles
        /// (`settleDrainingLocked`; under the owner's lock).
        draining: Rings.Slots = .empty,
        /// One record per slot, the senders' then the takers': the slot's member while it is joined, or retired
        /// (under the owner's lock).
        records: []Member,
        /// The joined members of each role: changed under the owner's lock, read anywhere (`joinedOf`).
        joined: std.EnumArray(Role, Atomic(u32)) = .initFill(.init(0)),
        /// Where a sender slot's wait for room has a wake of its own (`Rings.room_wake_suffix`; where each slot has its
        /// wakes): each sender slot's second wake, created and kept as the roster keeps its first (under the owner's
        /// lock).
        room_wakes: if (Rings.room_wake_suffix != null) [64]?platform.SlotWake else void = if (Rings.room_wake_suffix != null) @splat(null) else {},

        /// The members of up to `max_senders` senders and `max_takers` takers, in `records` (one per slot, the
        /// caller's memory), whose slots it sets.
        pub fn init(records: []Member, max_senders: u32, max_takers: u32) Self {
            std.debug.assert(records.len == max_senders + max_takers);
            for (records, 0..) |*member, i| {
                const slot: Slot = if (i < max_senders) .{ .role = Rings.sender_role, .index = @intCast(i) } else .{ .role = Rings.taker_role, .index = @intCast(i - max_senders) };
                member.* = .{ .slot = slot };
            }
            return .{ .senders = .{ .slot_count = max_senders }, .takers = .{ .slot_count = max_takers }, .records = records };
        }

        /// Each record joins the handles' registry (core/process_state.zig), once the owner's has.
        pub fn joinRegistry(members: *Self) void {
            for (members.records) |*member| member.handles.join();
        }

        /// Each record leaves the handles' registry (no connection is open).
        pub fn leaveRegistry(members: *Self) void {
            for (members.records) |*member| member.handles.leave();
        }

        /// Closes every member's connection, and each record leaves the handles' registry.
        pub fn closeConnections(members: *Self) void {
            for (members.records) |*member| {
                member.handles.close(.connected);
                member.handles.leave();
            }
        }

        /// Closes the slots' wakes (core/roster.zig) and frees the records (`gpa`'s, `init`'s caller's).
        pub fn deinit(members: *Self, gpa: std.mem.Allocator) void {
            members.senders.closeWakes();
            members.takers.closeWakes();
            if (Rings.room_wake_suffix != null and platform.wakes_each_slot) {
                for (&members.room_wakes) |*wake| if (wake.*) |handle| platform.closeSlotWake(handle);
            }
            gpa.free(members.records);
        }

        pub fn roster(members: *Self, role: Role) *Roster {
            return if (role == Rings.sender_role) &members.senders else &members.takers;
        }

        pub fn memberOf(members: *Self, slot: Slot) *Member {
            return &members.records[if (slot.role == Rings.sender_role) slot.index else members.senders.slot_count + slot.index];
        }

        /// The joined members' counts, shown in the segment where the kind shows them (core/ring_set.zig
        /// `showJoined`). Under the owner's lock, after each change.
        fn showJoinedLocked(members: *const Self, layout: *const Layout) void {
            Rings.showJoined(layout, members.joinedOf(Rings.sender_role), members.joinedOf(Rings.taker_role));
        }

        /// The joined members of `role`, or of either role (null). Any thread.
        pub fn joinedOf(members: *const Self, role: ?Role) u32 {
            const joined = &members.joined;
            return if (role) |r|
                joined.getPtrConst(r).load(.acquire)
            else
                joined.getPtrConst(Rings.sender_role).load(.acquire) + joined.getPtrConst(Rings.taker_role).load(.acquire);
        }

        /// The kind has ended: the owner's close, or a member's watcher, marked the segment.
        pub fn hasEnded(owner: *const Owner, layout: *const Layout) bool {
            return layout.header.ended.load(.acquire) != 0 or owner.closed.load(.acquire);
        }

        /// Whether a slot of either role is free, once the draining sender slots that settled are free; whether some
        /// sender slot still drains; whether the kind has ended (no room then).
        pub fn roomFor(members: *Self, owner: *Owner, layout: *const Layout) struct { free: bool, draining: bool, ended: bool } {
            owner.lock();
            defer owner.unlock();
            if (hasEnded(owner, layout)) return .{ .free = false, .draining = false, .ended = true };
            members.settleDrainingLocked(layout);
            return .{
                .free = members.senders.claim() != null or members.takers.claim() != null,
                .draining = members.draining.count() != 0,
                .ended = false,
            };
        }

        /// Each draining sender slot whose ring settled is free (core/ring_set.zig `ringSettled`); one whose indices
        /// are a corrupt peer's is retired. Under the owner's lock.
        pub fn settleDrainingLocked(members: *Self, layout: *const Layout) void {
            var rest = members.draining.iterator(.{});
            while (rest.next()) |index| {
                const slot: u32 = @intCast(index);
                const generation = members.senders.generations[slot].load(.monotonic);
                switch (Rings.ringSettled(layout, slot)) {
                    .busy => continue,
                    .settled => _ = members.senders.release(slot, generation),
                    .corrupt => {
                        _ = members.senders.retire(slot, generation);
                        log.warn(@src(), "the ring of the " ++ Rings.noun ++ "'s " ++ @tagName(Rings.sender_role) ++ " slot {d} has corrupt indices: the slot stays out of use while the " ++ Rings.noun ++ " lasts", .{slot});
                    },
                }
                members.draining.unset(slot);
            }
        }

        /// Claims a slot of `role` for a joiner whose JOIN was read: its wakes where each slot has them (created at the
        /// slot's first claim, kept until the teardown), the slot's words in the segment reset, and the roster's hold.
        /// Full: no slot of the role is free, or the kind has ended.
        pub fn claimSlot(members: *Self, owner: *Owner, layout: *const Layout, session_id: *const [16]u8, role: Role) error{ Full, SystemResources }!core_roster.Claim {
            owner.lock();
            defer owner.unlock();
            if (hasEnded(owner, layout)) return error.Full;
            if (role == Rings.sender_role) members.settleDrainingLocked(layout);
            const slots = members.roster(role);
            const claim = slots.claim() orelse return error.Full;
            if (platform.wakes_each_slot and slots.wakes[claim.slot] == null) {
                slots.wakes[claim.slot] = platform.createSlotWake(session_id, Rings.wakeSuffix(role), claim.slot, &owner.security) catch
                    return error.SystemResources;
            }
            if (Rings.room_wake_suffix) |suffix| {
                if (platform.wakes_each_slot and role == Rings.sender_role and members.room_wakes[claim.slot] == null) {
                    members.room_wakes[claim.slot] = platform.createSlotWake(session_id, suffix, claim.slot, &owner.security) catch
                        return error.SystemResources;
                }
            }
            if (role == Rings.sender_role) {
                const control = layout.ring(claim.slot);
                control.room_sleeps.store(0, .monotonic);
                if (Rings.replies) control.reply_sleeps.store(0, .monotonic);
            } else {
                const words = layout.takerSlot(claim.slot);
                words.sleeps.store(0, .monotonic);
                words.claim.store(0, .monotonic);
                Rings.clearIdle(layout, claim.slot);
            }
            slots.hold(claim.slot, claim.generation);
            return claim;
        }

        /// Frees a slot a joiner never wrote (a WELCOME that failed, a kind that ended meanwhile).
        fn freeSlot(members: *Self, owner: *Owner, slot: Slot, generation: u32) void {
            owner.lock();
            defer owner.unlock();
            _ = members.roster(slot.role).release(slot.index, generation);
        }

        /// A member of `slot`, held under `generation`, writes the segment no more: a taker's slot is free (its idle
        /// bit cleared: a wake taken for it would be lost), a sender's draining. Under the owner's lock.
        pub fn endSlotLocked(members: *Self, layout: *const Layout, slot: Slot, generation: u32) void {
            if (slot.role == Rings.taker_role) {
                if (members.takers.release(slot.index, generation)) Rings.clearIdle(layout, slot.index);
            } else if (members.senders.holds(slot.index, generation)) {
                members.draining.set(slot.index);
            }
        }

        /// The joiner's last step, once its slot's record holds the connection. Under the owner's lock: the member is
        /// counted (before its join can return), WELCOME (`encodeWelcome`'s frame) is sent, the connection is watched,
        /// and the member is joined. If the connection ended before its watch, the poller still reports it, later, on
        /// this thread, once the member is joined. On failure:
        ///   - WELCOME failed, or the kind has ended: the member is uncounted, the connection closed and the slot
        ///     freed. This is safe because a joiner writes nothing in the segment before it has read all of WELCOME;
        ///   - the poller can't watch the connection: the member may already run, so it is uncounted, its slot
        ///     retired, and its connection kept open until the kind's end (closing it would read as the end to the
        ///     member).
        pub fn start(
            members: *Self,
            owner: *Owner,
            layout: *const Layout,
            member: *Member,
            generation: u32,
            comptime encodeWelcome: fn (u32, Role, u32, u32) Frame,
        ) void {
            const slot = member.slot;
            const outcome: enum { joined, unwelcomed, unwatched, ended } = outcome: {
                owner.lock();
                defer owner.unlock();
                if (hasEnded(owner, layout)) break :outcome .ended;
                member.generation = generation;
                const count = members.joined.getPtr(slot.role);
                _ = count.fetchAdd(1, .release);
                members.showJoinedLocked(layout);
                const connection = member.handles.get(.connected).?;
                const welcome = encodeWelcome(platform.pid(), slot.role, slot.index, generation);
                if (!platform.sendFrame(connection, &welcome, null)) {
                    _ = count.fetchSub(1, .monotonic);
                    members.showJoinedLocked(layout);
                    break :outcome .unwelcomed;
                }
                if (build_options.test_hooks) test_hooks.crashIfAt(.welcome_sent);
                owner.poller.watch(&member.watched, connection) catch {
                    _ = count.fetchSub(1, .monotonic);
                    members.showJoinedLocked(layout);
                    _ = members.roster(slot.role).retire(slot.index, generation);
                    member.state = .retired;
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
                    log.warn(@src(), "the {s} of slot {d} is dropped (its control connection can't be watched): the slot stays out of use while the " ++ Rings.noun ++ " lasts", .{ @tagName(slot.role), slot.index });
                    owner.wakeCountWaiters();
                },
                .unwelcomed => {
                    member.handles.close(.connected);
                    members.freeSlot(owner, slot, generation);
                    owner.wakeCountWaiters();
                },
                .ended => {
                    member.handles.close(.connected);
                    members.freeSlot(owner, slot, generation);
                },
            }
        }

        /// A member's connection has bytes or has ended, on the poller's thread. Nothing belongs there after WELCOME,
        /// so the member ends at once, under the owner's lock (the module's comment). It is unwatched and uncounted,
        /// then:
        ///   - if it left (it no longer writes the segment): `keepsSlot(context, slot)`, the kind's steps at its end
        ///     (a queue's consumer has its held message returned), then its slot is freed or made draining, unless
        ///     `keepsSlot` keeps it held for now; the connection is closed;
        ///   - after a failed read or a frame: its slot is retired and its connection kept open; a taker retired for a
        ///     frame, where messages carry replies, is watched on for its end (`watches_retired_takers`);
        ///   - once the kind has ended: only the connection is closed.
        /// Then the count's waiters are woken and the rings rescanned (with the owner's wakes, `wakes`). A retired
        /// taker still watched (`retired_watched`) is neither joined nor counted: a frame from it is dropped and the
        /// watch goes on, a failed read ends the watch, and its end runs `keepsSlot`, closes the connection and
        /// rescans the rings, its slot staying retired.
        pub fn onMemberReady(
            members: *Self,
            owner: *Owner,
            layout: *const Layout,
            wakes: *Rings.Wakes,
            member: *Member,
            context: anytype,
            comptime keepsSlot: fn (@TypeOf(context), Slot) bool,
        ) void {
            const outcome: union(enum) {
                member_ended: struct { end: MemberEnd, unwatched: bool = false },
                kind_ended,
                retired_ended,
                retired_unwatched,
                watching,
            } = outcome: {
                owner.lock();
                defer owner.unlock();
                const end: MemberEnd = switch (owner.poller.take(&member.watched)) {
                    .partial => return,
                    .ended => .left,
                    .failed => .read_failed,
                    .whole => .foreign,
                };
                owner.poller.unwatch(&member.watched);
                const retired = member.state == .retired_watched;
                if (!retired) {
                    _ = members.joined.getPtr(member.slot.role).fetchSub(1, .monotonic);
                    members.showJoinedLocked(layout);
                }
                if (layout.header.ended.load(.seq_cst) != 0) {
                    // A member's watcher stored the mark: nothing is recovered or freed
                    member.handles.close(.connected);
                    member.state = .free;
                    break :outcome .kind_ended;
                }
                if (retired) break :outcome switch (end) {
                    .foreign => if (members.watchRetiredLocked(owner, member)) .watching else .retired_unwatched,
                    .read_failed => unwatched: {
                        member.state = .retired;
                        break :unwatched .retired_unwatched;
                    },
                    .left => left: {
                        // It writes the segment no more: the kind's steps at its end, whatever they say of the slot,
                        // which stays retired
                        _ = keepsSlot(context, member.slot);
                        member.handles.close(.connected);
                        member.state = .free;
                        break :left .retired_ended;
                    },
                };
                if (end == .left) {
                    if (!keepsSlot(context, member.slot)) members.endSlotLocked(layout, member.slot, member.generation);
                    member.handles.close(.connected);
                    member.state = .free;
                    break :outcome .{ .member_ended = .{ .end = end } };
                }
                _ = members.roster(member.slot.role).retire(member.slot.index, member.generation);
                member.state = .retired;
                const watches = watches_retired_takers and end == .foreign and member.slot.role == Rings.taker_role;
                break :outcome .{ .member_ended = .{ .end = end, .unwatched = watches and !members.watchRetiredLocked(owner, member) } };
            };
            switch (outcome) {
                .kind_ended => owner.wakeCountWaiters(),
                .watching => {},
                .retired_unwatched => warnUnwatched(member.slot),
                .retired_ended => Rings.rescan(layout, wakes),
                .member_ended => |ended| {
                    owner.wakeCountWaiters();
                    if (ended.end != .left) {
                        log.warn(@src(), "the {s} of slot {d} is dropped ({s}): the slot stays out of use while the " ++ Rings.noun ++ " lasts", .{ @tagName(member.slot.role), member.slot.index, ended.end.reason() });
                    }
                    if (ended.unwatched) warnUnwatched(member.slot);
                    Rings.rescan(layout, wakes);
                },
            }
        }

        /// Where messages carry replies (a service), a taker retired for a frame stays watched until its connection's
        /// end: each sender receives its answers in the order of its messages, so a message that taker still holds
        /// when it ends must be answered (a service abandons it, `keepsSlot`), or its sender waits for good. It isn't
        /// answered at the retirement: the taker may still be running, and writing its reply (a taker's done is a
        /// store, not a compare-and-swap; its reply's bytes go into the sender's ring before it), which would land in
        /// room its sender reused once it received the abandon. The end of the connection means the taker writes the
        /// segment no more. A frame is one only a corrupt or foreign peer sends; a retired member whose read failed,
        /// or whose connection the poller can't watch, isn't watched: nothing it holds at its end is answered.
        const watches_retired_takers = Rings.replies;

        /// Watches a retired taker's connection for its end (`retired_watched`), from a new frame on; true unless the
        /// poller can't watch it, which leaves it `retired`. Under the owner's lock.
        fn watchRetiredLocked(members: *Self, owner: *Owner, member: *Member) bool {
            _ = members;
            owner.poller.watch(&member.watched, member.handles.get(.connected).?) catch {
                member.state = .retired;
                return false;
            };
            member.state = .retired_watched;
            return true;
        }

        fn warnUnwatched(slot: Slot) void {
            log.warn(@src(), "the dropped {s} of slot {d} can't be watched for its end: a message it holds then is never answered", .{ @tagName(slot.role), slot.index });
        }
    };
}
