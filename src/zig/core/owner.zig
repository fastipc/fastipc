//! What a broadcast's owner (broadcast/server.zig `Channel`) and a work queue's (queue/server.zig `Queue`) share: the
//! `Owner`, which each embeds, and the waits for its count of members (`CountWaiters`).
//!
//! The owner's life. `init` claims the name and makes what an owner holds besides its segment: the `Io`, the
//! security, the admission (core/admission.zig), the segment's handle where it travels with the frame, and the poller.
//! The kind then creates its segment and its members' records, and starts the poller task (`startPoller`), which admits
//! the joiners and watches the members until the kind's close stops it (`stopPoller`). After its own last steps, the
//! kind closes what the owner holds (`deinit`), then its members' connections, wakes and mapping, frees its memory, and
//! last releases the `Io` (`process_state.release`).
//!
//! Locks: `mutex` guards what the kind's poller and the kind's callers share (the kind's records, its counts and
//! `closed`); it is held only briefly, never on the data path, and before the handle lock where both are held
//! (core/process_state.zig). No endpoint mutex is held with it.

const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;
const Admission = @import("admission.zig").Admission;
const platform = @import("../platform.zig");
const process_state = @import("process_state.zig");
const wait = @import("wait.zig");

const Atomic = std.atomic.Value;

pub const Owner = struct {
    io: Io,
    gpa: Allocator,
    /// The process-wide `Io`'s reference, released last.
    io_ref: process_state.Ref,
    mutex: Io.Mutex = .init,
    /// Where segments are passed with the frame (Linux, macOS), `.segment`: the segment's descriptor, sent with every
    /// OFFER and kept for the owner's life; and the poller's handles (`.poller`, `.poller_signal`).
    handles: process_state.Handles = .{},
    /// The security of the listening handles, the segment and, on Windows, the members' wakes.
    security: platform.Security,
    /// The name's listening handle and the joiners being set up (the poller's).
    admission: Admission,
    /// Watches the listening handle, every joiner's and every member's control connection, on the task `poller_task`.
    poller: platform.Poller,
    poller_task: Io.Future(void),
    /// The kind's close has begun: no more members start. Set under `mutex`.
    closed: Atomic(bool) = .init(false),
    /// The threads that wait for the kind's count of its members.
    count_waiters: CountWaiters = .{},

    /// The start of a broadcast's, a queue's or a service's create, into the kind's object, which holds `owner` at a
    /// fixed address from then on: the process-wide `Io`, the security, the name claimed (AddrInUse if another stream,
    /// broadcast, queue or service holds it), the segment's handle where it travels with the frame, and the poller. On
    /// failure nothing is left.
    pub fn init(owner: *Owner, gpa: Allocator, name: []const u8) error{ NoMemory, AddrInUse }!void {
        const io_ref = process_state.acquire() catch return error.NoMemory;
        errdefer process_state.release(io_ref);
        const io = io_ref.io;
        owner.* = .{
            .io = io,
            .gpa = gpa,
            .io_ref = io_ref,
            .security = platform.Security.init() catch return error.NoMemory,
            .admission = undefined,
            .poller = undefined,
            .poller_task = undefined,
        };
        errdefer owner.security.deinit();
        try owner.admission.init(io, name, &owner.security);
        errdefer owner.admission.deinit();
        owner.handles.join();
        errdefer owner.handles.leave();
        if (platform.passes_segment_handle) {
            process_state.lockHandles();
            defer process_state.unlockHandles();
            owner.handles.register(.segment, platform.createSegmentHandle() catch return error.NoMemory);
        }
        errdefer owner.handles.close(.segment);
        process_state.lockHandles();
        defer process_state.unlockHandles();
        owner.poller = platform.Poller.init() catch return error.NoMemory;
        for (owner.poller.handles(), [_]process_state.Handles.Slot{ .poller, .poller_signal }) |handle, slot| {
            if (handle) |h| owner.handles.register(slot, h);
        }
    }

    /// The last step of the create: the poller task, `pollMembers(object)` on the owner's `Io`, which admits the
    /// joiners from now on.
    pub fn startPoller(owner: *Owner, comptime Object: type, comptime pollMembers: fn (*Object) void, object: *Object) error{NoMemory}!void {
        owner.poller_task = owner.io.concurrent(pollMembers, .{object}) catch return error.NoMemory;
    }

    /// The first step of the kind's close: `closed` set and the poller stopped, under `mutex` (no member starts after
    /// it); the count's waiters woken; the poller task awaited.
    pub fn stopPoller(owner: *Owner) void {
        {
            owner.lock();
            defer owner.unlock();
            owner.closed.store(true, .release);
            owner.poller.stop();
        }
        owner.wakeCountWaiters();
        // Awaited, never canceled through `Io`, which on Windows would alert the poller's thread in its alertable wait
        owner.poller_task.await(owner.io);
    }

    /// Closes what `init` made, once the poller task has ended (or never started): the poller's handles first, so the
    /// poller reports none of the connections closed after it, even one a forked child still holds open; then the
    /// joiners' connections and the listening handle (the name is free); the segment's handle, the registry entry and
    /// the security. The caller then closes its members' connections, frees the memory that holds `owner`, and
    /// releases the returned `Io` reference (`process_state.release`).
    pub fn deinit(owner: *Owner) process_state.Ref {
        owner.handles.close(.poller);
        owner.handles.close(.poller_signal);
        owner.admission.deinit();
        owner.handles.close(.segment);
        owner.handles.leave();
        owner.security.deinit();
        return owner.io_ref;
    }

    pub fn lock(owner: *Owner) void {
        owner.mutex.lockUncancelable(owner.io);
    }

    pub fn unlock(owner: *Owner) void {
        owner.mutex.unlock(owner.io);
    }

    /// The segment's handle, sent with every OFFER where it travels with the frame (else null).
    pub fn segmentHandle(owner: *const Owner) ?platform.Handle {
        return owner.handles.get(.segment);
    }

    /// Wakes every thread that waits for the count, after the caller published what it must see.
    pub fn wakeCountWaiters(owner: *Owner) void {
        owner.count_waiters.wake(owner.io);
    }
};

/// The threads that wait for an owner's count of its members (the kind's `waitForMembers`), on any thread. The count
/// is the kind's; each change of it, and anything else a waiter must see (a cancel, the end), bumps `changes`, a futex
/// in this process, and wakes the waiters (`wake`). A waiter loads `changes` before it looks, and sleeps only while
/// `changes` still holds that value, so no change between its look and its sleep goes unseen.
pub const CountWaiters = struct {
    changes: Atomic(u32) = .init(0),

    /// What a waiter finds at a look: the count, and what ends the wait if the count is short (null: nothing).
    pub const Look = struct { count: u32, stop: ?error{ Cancelled, Disconnected } = null };

    pub const WaitError = error{ Timeout, Cancelled, Disconnected };

    /// The bump (.release) orders the caller's stores before it, for a waiter that loads it (.acquire).
    pub fn wake(waiters: *CountWaiters, io: Io) void {
        _ = waiters.changes.fetchAdd(1, .release);
        io.futexWake(u32, &waiters.changes.raw, std.math.maxInt(u32));
    }

    /// Waits until `look(context)` counts at least `at_least`, within `timeout_ms` (C's: 0 looks once, negative has no
    /// limit), and stores the last look's count in `count` at every return. At each look, a short count returns the
    /// look's `stop` if it has one, then Timeout once the deadline has passed; Cancelled for the caller's `Io`'s cancel
    /// (the cancelation stays pending).
    pub fn waitUntil(
        waiters: *CountWaiters,
        io: Io,
        at_least: u32,
        timeout_ms: c_int,
        count: *u32,
        context: anytype,
        comptime look: fn (@TypeOf(context)) Look,
    ) WaitError!void {
        const deadline = wait.deadlineFromMs(io, timeout_ms);
        while (true) {
            const seen = waiters.changes.load(.acquire);
            const found = look(context);
            count.* = found.count;
            if (found.count >= at_least) return;
            if (found.stop) |err| return err;
            if (timeout_ms == 0 or wait.passed(io, deadline)) return error.Timeout;
            const timeout: Io.Timeout = if (deadline) |at| .{ .deadline = at } else .none;
            io.futexWaitTimeout(u32, &waiters.changes.raw, seen, timeout) catch {
                io.recancel();
                return error.Cancelled;
            };
        }
    }
};

/// How a member's control connection ended, as the owner's poller reads it.
pub const MemberEnd = enum {
    /// The member writes the segment no more: it closed its end of the connection, or a subscriber answered
    /// UNSUBSCRIBE. Its slot may go to another member.
    left,
    /// The read failed: the member may still be running and writing its slot.
    read_failed,
    /// A frame that doesn't belong: the member may still be running and writing its slot.
    foreign,

    /// Why the owner dropped a member whose slot it retires, for the log.
    pub fn reason(end: MemberEnd) []const u8 {
        return switch (end) {
            .left => "",
            .read_failed => "its control connection's read failed",
            .foreign => "protocol error: it sent a frame that doesn't belong",
        };
    }
};
