//! State shared by every listener and connection in the process. The C API hands out two kinds of object, and both
//! use what is here:
//!
//! - a listener, `fipc_listener_t*`: a `Listener` (lifecycle/listener.zig), a server's name that accepts clients;
//! - a connection, `fipc_conn_t*`: an `Endpoint` (lifecycle/endpoint.zig), one side of one connection, a client's or
//!   one a listener accepted.
//!
//! This file has three parts:
//!
//! 1. **The shared `Io`** (`acquire`, `release`). Every connection runs a background task, its watcher, which waits
//!    for its peer's end, and all of them run on one `Io.Threaded` (a pool of worker threads) for the whole process. It
//!    is reference counted, one reference per listener or connection: the first one created creates it, and the last
//!    one freed shuts it down and joins its threads, so no library thread outlives the last object.
//! 2. **Each object's OS handles** (`Handles`): the sockets (Linux, macOS) or pipes (Windows), the segment handles and the
//!    cancel signal a listener or connection holds, one per `Slot`.
//! 3. **Fork support** (Linux and macOS; Windows has no fork). A child made by `fork()` inherits a copy of every open
//!    descriptor but none of the parent's threads. So every `Handles` is linked into a registry, and a handler that
//!    runs in the child right after the fork closes every registered descriptor and marks each listener and
//!    connection as forked (docs/protocol.md §7). Calls on those then fail with Invalid, and the first one logs why
//!    (`reportInherited`). The child's first `acquire` abandons the inherited `Io` and builds a new one, so the objects
//!    it creates itself work normally.
//!
//! **Locks.** Three locks are involved. Whenever two are held at once, they are taken in this order:
//!
//! 1. The reference-count lock (`reference_lock`, here): guards the shared `Io` and its count.
//! 2. A connection's mutex (`Endpoint.mutex`, an `Io.Mutex`): guards the publication and unmapping of its session.
//! 3. The handle lock (`handle_lock`, here; Linux and macOS, a no-op on Windows): guards the registry and every `Handles`.
//!    A handle is opened and recorded in one step under it, so a fork never copies a descriptor the registry doesn't
//!    know of (the one exception, `accept`'s new connection, is covered by `forkCount`).
//!
//! Ordinary calls hold at most one of them at a time. The only nesting is at a fork: `prepareFork` takes lock 1, then
//! lock 3, so the child starts with a consistent registry and inherits no lock held by a thread it doesn't have (the
//! child's handler releases both).
//!
//! Since a `fork()` on any application thread waits for those two locks, neither is held across a call that can
//! block: `release` detaches the `Io` under the lock but joins its threads after releasing it, and a log line written
//! under the handle lock is buffered and written once the lock is released (log.zig `hold`).
//!
//! Both are plain OS locks (`platform.Lock`), not `Io.Mutex`es: an `Io.Mutex` needs an `Io`, which is what the first
//! lock guards the creation of, and the fork handlers take and release them without any `Io`.

const std = @import("std");
const builtin = @import("builtin");
const Io = std.Io;
const log = @import("log.zig");
const platform = @import("platform.zig");

// === The process-wide Io ===

/// A reference to the process-wide `Io`, from `acquire`; `release` gives it back.
pub const Ref = struct {
    io: Io,
    /// The instance it belongs to. A reference inherited through fork names the abandoned instance, and its
    /// release changes nothing.
    generation: u32,
};

var reference_lock: platform.Lock = .{};
// Guarded by the reference-count lock:
var instance: ?*Io.Threaded = null;
var references: u32 = 0;
var generation: u32 = 0;
/// Where the OS forks: the process that created `instance`. In any other process (a forked child) the instance is
/// inherited: its threads don't exist there.
var owner_pid: u32 = 0;

/// The fork handlers' registration, once per address space: `not_registered`, `registered`, or else the pid of the
/// process one of whose threads is registering them. It is never made under a lock: a fork while a thread held the
/// reference-count lock before the handlers existed would leave the child that lock held, with no handler to let go.
var fork_handlers = std.atomic.Value(u32).init(not_registered);
const not_registered: u32 = 0;
const registered: u32 = std.math.maxInt(u32);

/// Takes a reference to the process-wide `Io`, creating it at the first (0 → 1). In a forked child the inherited
/// instance is abandoned untouched (its threads don't exist there, and one of them may have held its internal
/// mutex at the fork) and a new one is created. Where the OS forks, the fork handlers are registered first, before
/// the lock (`registerForkHandlers`), once per address space: a child inherits the registration.
pub fn acquire() error{OutOfMemory}!Ref {
    if (platform.has_fork) try registerForkHandlers();
    reference_lock.lock();
    defer reference_lock.unlock();
    if (inherited()) {
        instance = null; // never touched again: its memory stays allocated, its references count for nothing
        references = 0;
    }
    if (instance == null) {
        const threaded = try std.heap.c_allocator.create(Io.Threaded);
        // `init_single_threaded` installs no signal handler, where `Threaded.init` would replace SIGIO's and
        // SIGPIPE's for the whole process. The library needs none: it never cancels a task through `Io` on POSIX,
        // which would send SIGIO. With these fields overridden, it runs concurrent tasks on worker threads.
        threaded.* = .init_single_threaded;
        threaded.allocator = std.heap.c_allocator;
        threaded.concurrent_limit = .unlimited;
        threaded.async_limit = .unlimited;
        threaded.stack_size = platform.task_stack_size;
        instance = threaded;
        generation +%= 1;
        if (platform.has_fork) owner_pid = platform.pid();
    }
    references += 1;
    return .{ .io = instance.?.io(), .generation = generation };
}

/// Gives a reference back. The last one (1 → 0) detaches the instance under the lock and shuts it down after
/// releasing it, so no library thread is left; the next `acquire` creates a new one. A reference of an abandoned
/// instance (inherited through fork) changes nothing. Never called on one of the instance's own workers, since
/// the shutdown joins them: only application threads free endpoints.
pub fn release(ref: Ref) void {
    const last: ?*Io.Threaded = last: {
        reference_lock.lock();
        defer reference_lock.unlock();
        if (inherited() or ref.generation != generation) break :last null;
        references -= 1;
        if (references > 0) break :last null;
        const threaded = instance.?;
        instance = null;
        break :last threaded;
    };
    if (last) |threaded| shutDown(threaded);
}

/// Registers the fork handlers, once per address space, holding no lock; returns once they are registered. A thread
/// that finds another of its process registering them waits for it. A registration that a fork interrupted, in the
/// child, is either complete (the child's handler then ran and recorded it) or never happened there: the child, whose
/// pid differs from the one recorded, registers them itself.
fn registerForkHandlers() error{OutOfMemory}!void {
    while (true) {
        const state = fork_handlers.load(.acquire);
        if (state == registered) return;
        if (state != not_registered and state == platform.pid()) {
            std.Thread.yield() catch {};
            continue;
        }
        if (fork_handlers.cmpxchgWeak(state, platform.pid(), .acquire, .acquire) != null) continue;
        platform.prepareForForks();
        if (std.c.pthread_atfork(prepareFork, afterForkInParent, afterForkInChild) != 0) {
            fork_handlers.store(not_registered, .release);
            return error.OutOfMemory;
        }
        fork_handlers.store(registered, .release);
        return;
    }
}

/// Whether `instance` was inherited through fork.
fn inherited() bool {
    return platform.has_fork and instance != null and owner_pid != platform.pid();
}

/// Joins the instance's workers (platform.zig `shutDownIo`) and frees it. At a count of 0 no task runs (every task
/// was awaited).
fn shutDown(threaded: *Io.Threaded) void {
    // The workers' own list (`Io/Threaded.zig:1774-1785`): never shut down from one of its own workers
    var worker = threaded.worker_threads.load(.acquire);
    while (worker) |w| : (worker = w.next) std.debug.assert(w.id != std.Thread.getCurrentId());
    platform.shutDownIo(threaded);
    std.heap.c_allocator.destroy(threaded);
}

// === Handles and fork ===

/// The handles a listener or connection owns, by slot, with its view number and its forked mark; where the OS forks,
/// also its entry in the fork registry. It joins the registry when its owner is allocated and leaves it when its
/// owner is freed.
pub const Handles = struct {
    prev: ?*Handles = null,
    next: ?*Handles = null,
    /// The handles by `Slot`: they are opened and registered, unregistered and closed only under the handle lock, so
    /// a fork copies none it doesn't know of.
    slots: std.EnumArray(Slot, ?platform.Handle) = .initFill(null),
    /// An endpoint's view number: a forked child's handler sets it to 0, so a data call finds no session. A listener
    /// has none.
    view_number: ?*std.atomic.Value(u64) = null,
    /// Set by a forked child's handler, while the child has no other thread; never cleared.
    forked: bool = false,

    pub const Slot = enum {
        /// A listener's: where it accepts its next client.
        listening,
        /// A listener's: the cancel signal its waits watch.
        cancel,
        /// The connection: a client's, or the one a listener accepted, until it hands it over.
        connected,
        /// A listener's next listening handle, where a client connects on the listening handle itself
        /// (platform.zig `nextListening`), until the hand-over.
        spare,
        /// A listener's: the segment's handle while it offers it (where `platform.passes_segment_handle`).
        segment,
        /// A client's: the offered segment's handle, until it is mapped (where `platform.passes_segment_handle`).
        received,
    };

    pub fn join(node: *Handles) void {
        if (!platform.has_fork) return;
        handle_lock.lock();
        defer handle_lock.unlock();
        node.prev = null;
        node.next = registry;
        if (registry) |head| head.prev = node;
        registry = node;
    }

    /// Leaves the registry; every handle was unregistered first.
    pub fn leave(node: *Handles) void {
        for (node.slots.values) |slot| std.debug.assert(slot == null);
        if (!platform.has_fork) return;
        handle_lock.lock();
        defer handle_lock.unlock();
        if (node.prev) |prev| prev.next = node.next else registry = node.next;
        if (node.next) |next| next.prev = node.prev;
        node.prev = null;
        node.next = null;
    }

    /// Records `handle` in `slot`. Under the handle lock, in the step that opened the handle.
    pub fn register(node: *Handles, slot: Slot, handle: platform.Handle) void {
        std.debug.assert(node.slots.getPtrConst(slot).* == null);
        node.slots.set(slot, handle);
    }

    /// Takes the handle out of `slot` (null if there is none). Under the handle lock, in the step that closes it.
    pub fn unregister(node: *Handles, slot: Slot) ?platform.Handle {
        defer node.slots.set(slot, null);
        return node.slots.getPtrConst(slot).*;
    }

    /// The handle in `slot`, if any. Reads that slot only (`EnumArray.get` copies the whole array), so a thread may
    /// read a slot that no other thread writes, such as a listener's cancel signal, while the owner changes the others.
    pub fn get(node: *const Handles, slot: Slot) ?platform.Handle {
        return node.slots.getPtrConst(slot).*;
    }

    /// Unregisters and closes the handle in `slot`, if there is one, under the handle lock.
    pub fn close(node: *Handles, slot: Slot) void {
        lockHandles();
        defer unlockHandles();
        if (node.unregister(slot)) |h| platform.close(h);
    }
};

var handle_lock: platform.Lock = .{};
/// The registered nodes, guarded by the handle lock.
var registry: ?*Handles = null;
var fork_count: std.atomic.Value(u32) = .init(0);

/// Takes the handle lock where the OS forks (elsewhere a no-op: no fork copies the handles). What the
/// thread logs while it holds the lock is written once it is released (log.zig `hold`): no step under it blocks.
pub fn lockHandles() void {
    if (!platform.has_fork) return;
    handle_lock.lock();
    log.hold();
}

pub fn unlockHandles() void {
    if (!platform.has_fork) return;
    handle_lock.unlock();
    log.release();
}

/// Logs why a call on a listener or connection inherited through fork failed, which its result (Invalid) doesn't say:
/// once per process, at the first such call.
pub noinline fn reportInherited(comptime object: []const u8) void {
    @branchHint(.cold);
    if (inherited_reported.swap(true, .monotonic)) return;
    log.err(@src(), "a call on a " ++ object ++ " inherited through fork() failed (FIPC_INVALID). A forked child " ++
        "can't use its parent's listeners and connections: every call on them fails, except the closes, which are " ++
        "safe. Open new ones in the child, or start it with exec (Python multiprocessing: the \"spawn\" or " ++
        "\"forkserver\" start method).", .{});
}

/// Whether this process has logged `reportInherited`'s line; a forked child starts over.
var inherited_reported = std.atomic.Value(bool).init(false);

/// The number of forks so far: `accept` returns the one handle that isn't born under the handle lock, so a listener
/// compares the count read before it with the count under the lock after it, and drops the connection if a fork came
/// between (the child may hold a copy).
pub fn forkCount() u32 {
    return fork_count.load(.monotonic);
}

/// Before a fork: take the reference-count lock, then the handle lock, so that the child sees one consistent set of
/// endpoints and handles and inherits neither lock held. No locked step blocks, so the fork waits only for
/// instant steps.
fn prepareFork() callconv(.c) void {
    reference_lock.lock();
    handle_lock.lock();
}

fn afterForkInParent() callconv(.c) void {
    _ = fork_count.fetchAdd(1, .monotonic);
    handle_lock.unlock();
    reference_lock.unlock();
}

/// In the child, which has one thread and both locks: close every registered handle (close only: a shutdown would
/// end the parent's connection too), set each endpoint's view number to 0 and mark it forked, let `reportInherited`
/// log again, then release the locks. No allocation and no `Io` call; the child's `Io` is rebuilt by its first
/// `acquire`.
fn afterForkInChild() callconv(.c) void {
    var node = registry;
    while (node) |n| : (node = n.next) {
        for (&n.slots.values) |*slot| {
            platform.close(slot.* orelse continue);
            slot.* = null;
        }
        if (n.view_number) |view| view.store(0, .monotonic);
        n.forked = true;
    }
    inherited_reported.store(false, .monotonic);
    // The handlers ran, so they are registered here, whether or not the parent's registering thread recorded it
    fork_handlers.store(registered, .release);
    handle_lock.unlock();
    reference_lock.unlock();
}

// === Tests ===

const testing = std.testing;
const build_options = @import("build_options");

fn threadedOf(ref: Ref) *Io.Threaded {
    return @ptrCast(@alignCast(ref.io.userdata));
}

test "one instance while it is referenced, a new one after the last release; stale references change nothing" {
    const first = try acquire();
    const second = try acquire();
    try testing.expectEqual(first.io.userdata, second.io.userdata);
    try testing.expectEqual(first.generation, second.generation);
    release(first);
    release(second);
    try testing.expectEqual(@as(?*Io.Threaded, null), instance);

    const third = try acquire();
    try testing.expect(third.generation != first.generation);
    release(first); // a reference of the instance that is gone
    try testing.expectEqual(@as(u32, 1), references);
    release(third);
    try testing.expectEqual(@as(u32, 0), references);
}

fn recordThread(id: *std.Thread.Id) void {
    id.* = std.Thread.getCurrentId();
}

/// POSIX signal dispositions (the handler address; 0 is SIG_DFL, 1 SIG_IGN).
fn disposition(signal: std.posix.SIG) usize {
    var action: std.posix.Sigaction = undefined;
    std.posix.sigaction(signal, null, &action);
    return @intFromPtr(action.handler.handler);
}

test "the instance runs concurrent tasks on its own threads, with the OS's stack size and no signal handler" {
    // Signal dispositions exist on POSIX only
    const posix = builtin.os.tag != .windows;
    const sigio_before = if (posix) disposition(.IO) else 0;
    const sigpipe_before = if (posix) disposition(.PIPE) else 0;
    const ref = try acquire();
    const threaded = threadedOf(ref);
    try testing.expectEqual(platform.task_stack_size, threaded.stack_size);
    try testing.expect(!threaded.have_signal_handler);

    var id: std.Thread.Id = std.Thread.getCurrentId();
    var task = try ref.io.concurrent(recordThread, .{&id});
    task.await(ref.io);
    try testing.expect(id != std.Thread.getCurrentId());
    if (posix) {
        try testing.expectEqual(sigio_before, disposition(.IO));
        try testing.expectEqual(sigpipe_before, disposition(.PIPE));
    }
    release(ref);
    if (posix) {
        try testing.expectEqual(sigio_before, disposition(.IO));
        try testing.expectEqual(sigpipe_before, disposition(.PIPE));
    }
}

/// A handle value for bookkeeping tests, never passed to the OS.
fn fakeHandle(n: usize) platform.Handle {
    return if (@typeInfo(platform.Handle) == .pointer) @ptrFromInt(n) else @intCast(n);
}

test "a node registers and unregisters handles by slot" {
    var view = std.atomic.Value(u64).init(5);
    var node: Handles = .{ .view_number = &view };
    node.join();
    lockHandles();
    node.register(.listening, fakeHandle(7));
    node.register(.segment, fakeHandle(9));
    unlockHandles();
    try testing.expectEqual(@as(?platform.Handle, fakeHandle(7)), node.get(.listening));
    lockHandles();
    try testing.expectEqual(@as(?platform.Handle, fakeHandle(7)), node.unregister(.listening));
    try testing.expectEqual(@as(?platform.Handle, null), node.unregister(.connected));
    try testing.expectEqual(@as(?platform.Handle, fakeHandle(9)), node.unregister(.segment));
    unlockHandles();
    node.leave();
    try testing.expect(!node.forked);
    try testing.expectEqual(@as(u64, 5), view.load(.monotonic));
}

test {
    if (build_options.slow_tests) _ = slow_tests;
}

/// The fork tests (where the OS forks): they start processes, so they run in the slow tier (`zig build test-slow`). A
/// forked child runs only its checks and leaves with `_exit`: it never returns into the test runner. Under TSan the
/// tests whose child forks from a parent with other threads are skipped: TSan ends a child that starts a thread after
/// such a fork, and reports the parent's threads as leaked when the child exits.
const slow_tests = struct {
    const linux = if (builtin.os.tag == .macos) @import("platform/macos_test_sys.zig") else std.os.linux;

    const no_security: platform.Security = .{};

    /// Waits up to `timeout_ms` for the child to exit, and returns its exit code; kills it at the deadline
    /// (null: it hung).
    fn waitChild(pid: std.c.pid_t, timeout_ms: u32) ?u8 {
        var status: c_int = 0;
        for (0..timeout_ms) |_| {
            const rc = std.c.waitpid(pid, &status, std.c.W.NOHANG);
            if (rc == pid) {
                const s: u32 = @bitCast(status);
                return if (s & 0x7f == 0) @truncate(s >> 8) else 128 + @as(u8, @truncate(s & 0x7f));
            }
            const ts: linux.timespec = .{ .sec = 0, .nsec = std.time.ns_per_ms };
            _ = linux.nanosleep(&ts, null);
        }
        _ = std.c.kill(pid, .KILL);
        _ = std.c.waitpid(pid, &status, 0);
        return null;
    }

    fn isOpen(fd: linux.fd_t) bool {
        return linux.errno(linux.fcntl(fd, linux.F.GETFD, 0)) == .SUCCESS;
    }

    fn setFlag(flag: *std.atomic.Value(u32)) void {
        flag.store(1, .release);
    }

    fn address(comptime tag: []const u8) platform.Address {
        var name_buf: [64]u8 = undefined;
        const name = std.mem.print(&name_buf, "test-fork-" ++ tag ++ "-{d}", .{platform.pid()}) catch unreachable;
        return platform.addressOf(name) catch unreachable;
    }

    /// The child's checks after a fork: the inherited node is marked and its handles are closed, and the child
    /// builds its own `Io`, whose tasks run. Returns the exit code: 0, or the number of the failed check.
    fn childChecks(node: *const Handles, view: *const std.atomic.Value(u64), listening: linux.fd_t, memfd: linux.fd_t, parent_generation: u32, parent_io: *anyopaque) u8 {
        if (!node.forked) return 1;
        if (view.load(.monotonic) != 0) return 2;
        for (node.slots.values) |slot| if (slot != null) return 3;
        if (isOpen(listening) or isOpen(memfd)) return 4;
        const ref = acquire() catch return 5;
        if (ref.generation == parent_generation or ref.io.userdata == parent_io) return 6;
        var ran = std.atomic.Value(u32).init(0);
        var task = ref.io.concurrent(setFlag, .{&ran}) catch return 7;
        task.await(ref.io);
        if (ran.load(.acquire) != 1) return 8;
        release(ref);
        return 0;
    }

    test "slow: a forked child abandons the inherited Io and builds its own; its copies of the handles are closed" {
        if (!platform.has_fork or builtin.sanitize_thread) return error.SkipZigTest;
        // The parent's Io is live and has an idle worker: the condition under which an inherited pool never runs a
        // task in the child
        const ref = try acquire();
        defer release(ref);
        var warmed = std.atomic.Value(u32).init(0);
        var warm = try ref.io.concurrent(setFlag, .{&warmed});
        warm.await(ref.io);

        const rendezvous = address("child");
        const listening = (try platform.claim(&rendezvous, &no_security)) orelse return error.TestUnexpectedResult;
        const memfd = try platform.createSegmentHandle();
        var view = std.atomic.Value(u64).init(7);
        var node: Handles = .{ .view_number = &view };
        node.join();
        defer node.leave();
        lockHandles();
        node.register(.listening, listening);
        node.register(.segment, memfd);
        unlockHandles();

        var to_child: [2]linux.fd_t = undefined;
        var from_child: [2]linux.fd_t = undefined;
        try testing.expectEqual(@as(usize, 0), linux.pipe2(&to_child, .{ .CLOEXEC = true }));
        try testing.expectEqual(@as(usize, 0), linux.pipe2(&from_child, .{ .CLOEXEC = true }));
        const forks_before = forkCount();
        const pid = std.c.fork();
        if (pid == 0) {
            const code = childChecks(&node, &view, listening, memfd, ref.generation, ref.io.userdata.?);
            _ = linux.write(from_child[1], "r", 1);
            var byte: [1]u8 = undefined;
            _ = linux.read(to_child[0], &byte, 1); // until the parent has checked the name
            std.c._exit(code);
        }
        try testing.expect(pid > 0);
        try testing.expectEqual(forks_before + 1, forkCount());
        var byte: [1]u8 = undefined;
        var poll_fds = [1]std.posix.pollfd{.{ .fd = from_child[0], .events = std.posix.POLL.IN, .revents = 0 }};
        const ready = try std.posix.poll(&poll_fds, 5000);
        if (ready == 1) _ = linux.read(from_child[0], &byte, 1);

        // The parent keeps everything; once it closes its listening socket the name is free at once, while the
        // child still runs: the child's copy was closed
        try testing.expect(!node.forked);
        try testing.expectEqual(@as(u64, 7), view.load(.monotonic));
        try testing.expect(isOpen(listening) and isOpen(memfd));
        node.close(.listening);
        node.close(.segment);
        const reclaimed = try platform.claim(&rendezvous, &no_security);
        if (reclaimed) |fd| platform.close(fd);
        _ = linux.write(to_child[1], "x", 1);
        const code = waitChild(pid, 5000);
        for ([_]linux.fd_t{ to_child[0], to_child[1], from_child[0], from_child[1] }) |fd| _ = linux.close(fd);
        try testing.expectEqual(@as(usize, 1), ready);
        try testing.expect(reclaimed != null);
        try testing.expectEqual(@as(?u8, 0), code);
    }

    test "slow: peer credentials name the process at the other end" {
        if (!platform.has_fork) return error.SkipZigTest;
        const ref = try acquire();
        defer release(ref);
        const rendezvous = address("cred");
        const listener = (try platform.claim(&rendezvous, &no_security)) orelse return error.TestUnexpectedResult;
        defer platform.close(listener);
        var to_child: [2]linux.fd_t = undefined;
        try testing.expectEqual(@as(usize, 0), linux.pipe2(&to_child, .{ .CLOEXEC = true }));
        defer for (to_child) |fd| platform.close(fd);
        const pid = std.c.fork();
        if (pid == 0) {
            // Connect, then stay until the parent has looked
            const outcome = platform.connect(&rendezvous, &no_security) catch std.c._exit(1);
            if (outcome != .connected) std.c._exit(2);
            var byte: [1]u8 = undefined;
            _ = linux.read(to_child[0], &byte, 1);
            std.c._exit(0);
        }
        try testing.expect(pid > 0);
        var poll_fds = [1]std.posix.pollfd{.{ .fd = listener, .events = std.posix.POLL.IN, .revents = 0 }};
        if (try std.posix.poll(&poll_fds, 5000) != 1) {
            _ = waitChild(pid, 0);
            return error.TestUnexpectedResult; // the child never connected
        }
        const accepted = switch (platform.acceptWithin(listener, null, 5000, null)) {
            .done => |outcome| switch (outcome) {
                .connection => |fd| fd,
                else => return error.TestUnexpectedResult,
            },
            else => return error.TestUnexpectedResult,
        };
        defer platform.close(accepted);
        const who = platform.peer(accepted, .listener) orelse return error.TestUnexpectedResult;
        _ = linux.write(to_child[1], "x", 1);
        try testing.expectEqual(@as(?u8, 0), waitChild(pid, 5000));
        try testing.expectEqual(@as(u32, @intCast(pid)), who.pid.?);
        try testing.expect(who.same_user);
    }

    fn holdLock(lock: *platform.Lock, held: *std.atomic.Value(u32)) void {
        lock.lock();
        held.store(1, .release);
        const ts: linux.timespec = .{ .sec = 0, .nsec = 100 * std.time.ns_per_ms };
        _ = linux.nanosleep(&ts, null);
        lock.unlock();
    }

    test "slow: a fork waits for the reference-count and handle locks, so the child inherits neither held" {
        if (!platform.has_fork or builtin.sanitize_thread) return error.SkipZigTest;
        release(try acquire()); // the fork handlers are registered
        for ([_]*platform.Lock{ &reference_lock, &handle_lock }) |lock| {
            var held = std.atomic.Value(u32).init(0);
            const holder = try std.Thread.spawn(.{}, holdLock, .{ lock, &held });
            while (held.load(.acquire) == 0) std.Thread.yield() catch {};
            // The prepare handler waits until the holder lets go; without it the child would inherit the lock held
            // and hang in its first `acquire` or handle step
            const pid = std.c.fork();
            if (pid == 0) {
                const ref = acquire() catch std.c._exit(1);
                release(ref);
                lockHandles();
                unlockHandles();
                std.c._exit(0);
            }
            holder.join();
            try testing.expect(pid > 0);
            try testing.expectEqual(@as(?u8, 0), waitChild(pid, 5000));
        }
    }
};
