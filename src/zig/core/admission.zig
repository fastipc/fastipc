//! The admission of a broadcast's, a queue's or a service's joiners (docs/protocol.md §9.3, §10.3, §11.3), on its
//! owner's poller task (broadcast/server.zig, queue/server.zig, service/server.zig `pollMembers`): no thread of the
//! owner's application waits for a joiner, so an owner may join its own broadcast, queue or service from any thread,
//! the one that created it included.
//!
//! A joiner's setup takes these steps, none of which waits:
//!   1. the poller reports a client on the listening handle (platform.zig `Poller.listen`);
//!   2. `accept` takes it into a free record (`Joiner`), after the fork check and the check of the peer's user;
//!   3. the kind claims what it needs before OFFER and sends it (`offer`); the poller then watches the joiner's
//!      connection as it watches the members';
//!   4. once that connection has a whole frame, the kind checks the JOIN (`take`), moves the connection to the
//!      member's record (`handOver`) and starts the member.
//!
//! A joiner that drops off, sends anything but a JOIN, or sends nothing for `join_deadline_ms` after its accept is
//! dropped: its connection is closed, and a real joiner reads the end and joins again within its own timeout. So a
//! stalled or garbage joiner holds up neither the other joiners nor any member's end, and it holds at most one of the
//! `max_pending` records plus the kind's claim.
//!
//! The kind decides when to listen (`listenWhile`): while it has room for a joiner, a record is free and no back-off
//! runs. Joiners beyond that wait in the listening handle's backlog (Linux, macOS) or for a free pipe instance
//! (Windows), within their own timeouts.
//!
//! Everything here runs on the poller's thread, but `init` and `deinit`, which run before the poller's task starts and
//! after it ended. Each record's connection, and the listening handle, are registered in the fork registry
//! (core/process_state.zig), so a forked child closes its copies.

const std = @import("std");
const Io = std.Io;
const build_options = @import("build_options");
const control = @import("control.zig");
const platform = @import("../platform.zig");
const process_state = @import("process_state.zig");
const roster = @import("roster.zig");
const test_hooks = @import("test_hooks.zig");
const wait = @import("wait.zig");
const wire = @import("wire.zig");

/// The most joiners set up at once.
pub const max_pending = 8;

/// How long a joiner has, from its accept, to send its JOIN.
pub const join_deadline_ms = 2000;

/// One joiner between its accept and its JOIN.
pub const Joiner = struct {
    /// `.connected`: the joiner's connection. It stays when the record is free again: a node of the fork registry,
    /// which other threads link to under the handle lock.
    handles: process_state.Handles = .{},
    /// The poller's record of the connection while it is watched (`setup.watching`).
    watched: platform.Watched = undefined,
    /// This joiner's setup, which a free record has at its defaults.
    setup: Setup = .{},

    pub const Setup = struct {
        /// The joiner's JOIN is due before this; null while the record is free.
        deadline: ?Io.Clock.Timestamp = null,
        /// The poller watches the connection.
        watching: bool = false,
        /// A broadcast's: the slot it claimed for the joiner before OFFER (a queue claims its slot after JOIN).
        slot: ?roster.Claim = null,
    };

    /// The record is free again.
    fn reset(joiner: *Joiner) void {
        joiner.setup = .{};
    }

    /// No joiner is being set up in the record, and no connection is left in it.
    fn isFree(joiner: *const Joiner) bool {
        return joiner.setup.deadline == null and joiner.handles.get(.connected) == null;
    }
};

pub const Admission = struct {
    io: Io,
    address: platform.Address,
    /// The kind's security, for the next listening handle (Windows).
    security: *const platform.Security,
    /// `.listening`: the name's listening handle.
    handles: process_state.Handles = .{},
    joiners: [max_pending]Joiner = @splat(.{}),
    /// No accept before this: a failed accept's back-off (null: none).
    resume_at: ?Io.Clock.Timestamp = null,
    backoff: control.Backoff = .{},

    /// Claims `name` (AddrInUse if another stream, broadcast, queue or service holds it) and joins the fork registry:
    /// the start of a broadcast's, a queue's or a service's create, which owns `admission` at a fixed address from then
    /// on.
    pub fn init(admission: *Admission, io: Io, name: []const u8, security: *const platform.Security) error{ NoMemory, AddrInUse }!void {
        admission.* = .{ .io = io, .address = platform.addressOf(name) catch return error.NoMemory, .security = security };
        admission.handles.join();
        for (&admission.joiners) |*joiner| joiner.handles.join();
        errdefer admission.leave();
        try control.claimName(&admission.handles, &admission.address, security);
    }

    /// Once the poller's task has ended and its handles are closed: every joiner's connection (it reads the end), then
    /// the listening handle, whose close frees the name.
    pub fn deinit(admission: *Admission) void {
        for (&admission.joiners) |*joiner| {
            joiner.handles.close(.connected);
            joiner.reset();
        }
        admission.handles.close(.listening);
        admission.leave();
    }

    fn leave(admission: *Admission) void {
        for (&admission.joiners) |*joiner| joiner.handles.leave();
        admission.handles.leave();
    }

    /// Lets the poller report joiners while `room` (the kind has room for one more), a record is free and no back-off
    /// runs; else stops it.
    pub fn listenWhile(admission: *Admission, poller: *platform.Poller, room: bool) void {
        const listening = admission.handles.get(.listening) orelse return poller.unlisten();
        if (!room or admission.freeJoiner() == null or admission.backingOff()) return poller.unlisten();
        poller.listen(listening) catch {
            poller.unlisten();
            admission.backOff();
        };
    }

    /// How long the poller may wait before the next timed step: a joiner's deadline, or the end of a back-off that
    /// still runs (one that ended is dropped, so it doesn't make every wait 0 while the kind has no room); null: none.
    pub fn waitMs(admission: *Admission) ?u32 {
        _ = admission.backingOff();
        var earliest = admission.resume_at;
        for (&admission.joiners) |*joiner| {
            const deadline = joiner.setup.deadline orelse continue;
            if (earliest == null or deadline.compare(.lt, earliest.?)) earliest = deadline;
        }
        return wait.msLeft(admission.io, earliest);
    }

    /// Accepts the client the poller reported on the listening handle, without waiting, into a free record whose
    /// deadline it sets (control.zig `acceptClient`: the fork check, the peer's user), then makes the next listening
    /// handle where the client took this one. Null: nobody after all, or the client was dropped (a failure backs off).
    pub fn accept(admission: *Admission, poller: *platform.Poller) ?*Joiner {
        const joiner = admission.freeJoiner() orelse return null;
        if (admission.handles.get(.listening) == null) return null;
        switch (control.acceptClient(&admission.handles, &joiner.handles, null, 0)) {
            .connection => {},
            .idle, .retry_now => return null,
            .back_off => {
                admission.backOff();
                return null;
            },
        }
        if (admission.handles.get(.listening) == null) {
            control.nextListening(&admission.handles, .listening, &admission.address, admission.security) catch {
                admission.drop(poller, joiner);
                admission.backOff();
                return null;
            };
        }
        joiner.setup.deadline = .fromNow(admission.io, .{ .raw = .fromMilliseconds(join_deadline_ms), .clock = .awake });
        admission.backoff = .{};
        return joiner;
    }

    /// Sends OFFER (`frame`, with the segment's handle `attached` where it travels with the frame), then watches the
    /// joiner's connection for its JOIN. False: the joiner is dropped (the caller frees what it claimed for it).
    pub fn offer(admission: *Admission, poller: *platform.Poller, joiner: *Joiner, frame: *const wire.Frame, attached: ?platform.Handle) bool {
        const connection = joiner.handles.get(.connected).?;
        if (!platform.sendFrame(connection, frame, attached)) {
            admission.drop(poller, joiner);
            return false;
        }
        if (build_options.test_hooks) test_hooks.crashIfAt(.offer_sent);
        poller.watch(&joiner.watched, connection) catch {
            admission.drop(poller, joiner);
            admission.backOff();
            return false;
        };
        joiner.setup.watching = true;
        return true;
    }

    /// The record whose connection `watched` is, if it is a joiner's (else a member's).
    pub fn joinerOf(admission: *Admission, watched: *platform.Watched) ?*Joiner {
        for (&admission.joiners) |*joiner| {
            if (&joiner.watched == watched) return joiner;
        }
        return null;
    }

    /// What the poller found on a joiner's connection: nothing whole yet (it waits on), its end or a failed read (the
    /// caller drops it), or its frame.
    pub const Taken = union(enum) { waiting, gone, frame: *const wire.Frame };

    pub fn take(_: *Admission, poller: *platform.Poller, joiner: *Joiner) Taken {
        return switch (poller.take(&joiner.watched)) {
            .partial => .waiting,
            .ended, .failed => .gone,
            .whole => .{ .frame = &joiner.watched.frame },
        };
    }

    /// The joiner's connection moves to the member's node `target` (under the handle lock), no longer watched as a
    /// joiner's; the record is free again.
    pub fn handOver(_: *Admission, poller: *platform.Poller, joiner: *Joiner, target: *process_state.Handles) void {
        if (joiner.setup.watching) poller.unwatch(&joiner.watched);
        process_state.lockHandles();
        defer process_state.unlockHandles();
        target.register(.connected, joiner.handles.unregister(.connected).?);
        joiner.reset();
    }

    /// The joiner's setup ends: its connection is closed (it reads the end; control.zig `dropClient`, which keeps the
    /// name held), and the record is free again.
    pub fn drop(admission: *Admission, poller: *platform.Poller, joiner: *Joiner) void {
        if (joiner.setup.watching) poller.unwatch(&joiner.watched);
        control.dropClient(&admission.handles, &joiner.handles, false);
        joiner.reset();
    }

    /// Drops every joiner being set up: the kind admits nobody any more.
    pub fn dropAll(admission: *Admission, poller: *platform.Poller) void {
        for (&admission.joiners) |*joiner| {
            if (joiner.setup.deadline != null) admission.drop(poller, joiner);
        }
    }

    /// A joiner whose JOIN is overdue, for the caller to drop; null: none.
    pub fn overdue(admission: *Admission) ?*Joiner {
        for (&admission.joiners) |*joiner| {
            const deadline = joiner.setup.deadline orelse continue;
            if (wait.passed(admission.io, deadline)) return joiner;
        }
        return null;
    }

    /// A failed accept, or a joiner that couldn't be set up for want of handles: no accept until the back-off ends
    /// (1 ms, doubled by each back-off up to 100 ms, and 1 ms again after an accept).
    pub fn backOff(admission: *Admission) void {
        admission.resume_at = .fromNow(admission.io, .{ .raw = .fromMilliseconds(admission.backoff.next(null)), .clock = .awake });
    }

    fn backingOff(admission: *Admission) bool {
        const at = admission.resume_at orelse return false;
        if (!wait.passed(admission.io, at)) return true;
        admission.resume_at = null;
        return false;
    }

    fn freeJoiner(admission: *Admission) ?*Joiner {
        for (&admission.joiners) |*joiner| {
            if (joiner.isFree()) return joiner;
        }
        return null;
    }
};

comptime {
    // fipc.h's "up to 512 bytes per subscriber" (and per producer and consumer) counts the owner's member records; the
    // joiners' records are of the same order
    std.debug.assert(@sizeOf(Joiner) <= 512);
}

test "an ended back-off sets no timer, while the kind has no room: the poller sleeps until something happens" {
    const testing = std.testing;
    var admission: Admission = .{ .io = testing.io, .address = undefined, .security = undefined };
    try testing.expectEqual(@as(?u32, null), admission.waitMs());
    admission.backOff();
    // The back-off (1 ms) runs: the timer is its end
    try testing.expect((admission.waitMs() orelse 99) <= 1);
    platform.sleep(5);
    try testing.expectEqual(@as(?u32, null), admission.waitMs());
    try testing.expectEqual(@as(?Io.Clock.Timestamp, null), admission.resume_at);
    // A joiner's deadline still sets one
    admission.joiners[0].setup.deadline = .fromNow(testing.io, .{ .raw = .fromMilliseconds(join_deadline_ms), .clock = .awake });
    try testing.expect((admission.waitMs() orelse 0) > join_deadline_ms / 2);
}
