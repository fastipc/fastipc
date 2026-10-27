//! macOS's side of the OS contract (platform.zig), on Apple Silicon and macOS 14.4 or later: peer credentials
//! (`getpeereid`), frames sent without SIGPIPE (`SO_NOSIGPIPE`), the POSIX shared memory segment passed by
//! `SCM_RIGHTS`, the cancel signal (a kqueue with a user event), the poller (a kqueue) and the ring waits (`os_sync`).
//! The rendezvous (a socket file in the user's private directory, claimed with a lock file held by `flock`; claim,
//! connect, accept) is in macos/rendezvous.zig. What macOS shares with Linux (the process, frames, the segment's
//! passing and mapping, the handshake's waits) is in platform/posix.zig. Both are re-exported here. Everything goes
//! through libSystem (std.c, and darwin.zig for what std lacks).
//!
//! Every call returns at once except four:
//!   - the handshake's waits on the caller's thread (`acceptWithin`, `readWithin`, `readableWithin`: a `poll` with a
//!     timeout, then a non-blocking call);
//!   - the watcher's `read`, which blocks through `Io`;
//!   - the poller's `wait`, on its own thread;
//!   - `ringWait`, the one wait `Io` can't express (an address shared with the peer; stream/ring_wait.zig slices it).
//!
//! Descriptors: as on Linux, every descriptor the library owns is opened, registered, unregistered and closed under
//! the handle lock (core/process_state.zig), which the callers of `claim`, `connect`, `createCancelSignal`,
//! `createSegmentHandle`, `recvSegment`, `Poller.init` and `close` hold. macOS has no `SOCK_CLOEXEC`, `accept4` or
//! `MSG_CMSG_CLOEXEC`, so a second call makes a new socket or a received descriptor close-on-exec. That call runs under
//! the same lock, so no fork comes between the two. `acceptWithin` waits, so it runs without the lock, and the fork
//! counter covers its new socket, as on Linux. A `posix_spawn` on another thread between `accept` and its `fcntl` could
//! still inherit that socket, because macOS has no call that does both at once.

const std = @import("std");
const c = std.c;
const darwin = @import("darwin.zig");
const platform = @import("../../platform.zig");
const posix = @import("../posix.zig");
const process_state = @import("../../core/process_state.zig");
const rendezvous = @import("rendezvous.zig");

const ResourceError = platform.ResourceError;
const UnexpectedError = platform.UnexpectedError;
const unexpected = posix.unexpected;
const warnUnexpected = posix.warnUnexpected;
const errnoNow = posix.errnoNow;

pub const has_fork = true;
pub const passes_segment_handle = true;

pub const Handle = posix.Handle;

// === The process ===

pub const Security = posix.Security;
pub const Lock = posix.Lock;
pub const registerForkHandlers = posix.registerForkHandlers;
pub const keepImageLoaded = posix.keepImageLoaded;
pub const pid = posix.pid;
pub const monotonicNs = posix.monotonicNs;

pub fn crash(code: u8) noreturn {
    c._exit(code);
}

pub const writeStderr = posix.writeStderr;
pub const task_stack_size = posix.task_stack_size;
pub const shutDownIo = posix.shutDownIo;

// === Names and connections ===

pub const Address = rendezvous.Address;
pub const addressOf = rendezvous.addressOf;
pub const prepareForForks = rendezvous.prepareForForks;
pub const claim = rendezvous.claim;

/// None: one listening socket serves every client. On Windows a client takes the pipe instance it connected on.
pub fn nextListening(_: *const Address, _: *const Security) (ResourceError || UnexpectedError)!?Handle {
    return null;
}

pub const connect = rendezvous.connect;

/// Polls the listening socket and the cancel signal, then accepts without waiting. The new socket inherits the
/// listening socket's `O_NONBLOCK` and `SO_NOSIGPIPE`. It is made blocking, as the watcher's `Io` reads need, and
/// close-on-exec. A client that left between the poll and the accept (EAGAIN) is nothing; one aborted in the
/// backlog (ECONNABORTED) is `closing`; descriptor exhaustion is `transient`.
pub fn acceptWithin(listening: Handle, cancel: ?Handle, timeout_ms: ?u32, forks: ?*u32) platform.Waited(platform.Accept) {
    switch (posix.pollFor(listening, cancel, timeout_ms)) {
        .ready => {},
        .idle => return .idle,
        .cancelled => return .cancelled,
    }
    if (forks) |count| count.* = process_state.forkCount();
    const fd = c.accept(listening, null, null);
    return .{ .done = switch (c.errno(fd)) {
        .SUCCESS => if (prepareAccepted(fd)) |_| .{ .connection = fd } else |_| other: {
            _ = c.close(fd);
            break :other .other;
        },
        .AGAIN, .INTR => return .idle,
        .CONNABORTED => .closing,
        .MFILE, .NFILE, .NOBUFS, .NOMEM => .transient,
        else => |e| other: {
            warnUnexpected(@src(), "accept", e);
            break :other .other;
        },
    } };
}

pub const dropClient = posix.dropClient;

/// The user and process at the other end, as the kernel recorded them at `connect` or `listen` (`getpeereid`,
/// `LOCAL_PEERPID`).
pub fn peer(connection: Handle, _: platform.Side) ?platform.Peer {
    var uid: c.uid_t = undefined;
    var gid: c.gid_t = undefined;
    if (darwin.getpeereid(connection, &uid, &gid) != 0) {
        warnUnexpected(@src(), "getpeereid", errnoNow());
        return null;
    }
    var peer_pid: c.pid_t = 0;
    var len: c.socklen_t = @sizeOf(c.pid_t);
    const known = c.getsockopt(connection, darwin.SOL_LOCAL, darwin.LOCAL_PEERPID, &peer_pid, &len) == 0;
    return .{ .same_user = uid == c.geteuid(), .pid = if (known) @intCast(peer_pid) else null };
}

pub const unblock = posix.unblock;
pub const endTask = posix.endTask;

/// Closes a descriptor; a listening socket's claim is given up first (`rendezvous.releaseClaim`).
pub fn close(fd: Handle) void {
    rendezvous.releaseClaim(fd);
    posix.close(fd);
}

fn prepareAccepted(fd: Handle) UnexpectedError!void {
    try posix.setCloseOnExec(fd);
    try posix.setNonblocking(fd, false);
}

// === Frames on a connection ===

pub const sendFrame = posix.sendFrame;
pub const read = posix.read;
pub const recvSegment = posix.recvSegment;

// === Waits on the caller's thread ===

pub const readWithin = posix.readWithin;
pub const readableWithin = posix.readableWithin;

/// The identifier of the cancel signal's user event in its kqueue.
const cancel_event = 1;

/// A kqueue with one user event, which `setCancelSignal` triggers: neither cleared nor one-shot, it stays triggered for
/// good, and the kqueue polls readable from then on (nothing collects the event). A forked child doesn't inherit a
/// kqueue: the child handler's close of its number fails, harmlessly.
pub fn createCancelSignal() (ResourceError || UnexpectedError)!Handle {
    const kq = c.kqueue();
    if (kq < 0) return switch (errnoNow()) {
        .MFILE, .NFILE, .NOMEM => error.SystemResources,
        else => |e| unexpected(@src(), "kqueue", e),
    };
    errdefer _ = c.close(kq);
    try posix.setCloseOnExec(kq);
    switch (applyChange(kq, .{ .ident = cancel_event, .filter = c.EVFILT.USER, .flags = c.EV.ADD })) {
        .SUCCESS => return kq,
        .NOMEM => return error.SystemResources,
        else => |e| return unexpected(@src(), "kevent(EV_ADD)", e),
    }
}

pub fn setCancelSignal(signal: Handle) void {
    _ = applyChange(signal, .{ .ident = cancel_event, .filter = c.EVFILT.USER, .flags = 0, .fflags = c.NOTE.TRIGGER });
}

pub const waitCancelSignal = posix.waitCancelSignal;
pub const sleep = posix.sleep;

/// One change to a kqueue's events (`kevent` with nothing to receive): an event's `ident` and `filter`, what to do
/// (`flags`, `fflags`), and the `udata` it is reported with.
const KqueueChange = struct { ident: usize, filter: i16, flags: u16, fflags: u32 = 0, udata: usize = 0 };

/// Applies `one` to the kqueue `kq`: the errno of its `kevent`.
fn applyChange(kq: Handle, one: KqueueChange) c.E {
    const changes = [1]c.Kevent{.{ .ident = one.ident, .filter = one.filter, .flags = one.flags, .fflags = one.fflags, .data = 0, .udata = one.udata }};
    var none: [0]c.Kevent = .{};
    return c.errno(c.kevent(kq, &changes, 1, &none, 0, null));
}

// === The poller ===

pub const Watched = posix.Watched;

/// A kqueue.
///   - Each connection's read filter is level-triggered (neither `EV_CLEAR` nor `EV_ONESHOT`), so a connection is
///     reported at every wait while it has bytes or has ended, even if it ended before its watch. The poller doesn't
///     act on `EV_EOF`, because `take` reads what is left first.
///   - The listening socket's read filter (`udata` 1, which no `Watched` has) is reported while a client waits in its
///     backlog.
///   - The kqueue is also a cancel signal (`createCancelSignal`): `stop` triggers its user event, which stays reported.
/// A forked child doesn't inherit a kqueue, so the child handler's close of its number fails, harmlessly.
pub const Poller = struct {
    kqueue: Handle,
    /// The listening socket whose read filter is in the kqueue (`listen`), if any.
    listening: ?Handle = null,

    /// The most connections one `wait` reports.
    const batch = 64;
    const listening_data = 1;

    pub fn init() (ResourceError || UnexpectedError)!Poller {
        return .{ .kqueue = try createCancelSignal() };
    }

    pub fn handles(poller: *const Poller) [2]?Handle {
        return .{ poller.kqueue, null };
    }

    pub fn watch(poller: *Poller, watched: *Watched, connection: Handle) (ResourceError || UnexpectedError)!void {
        watched.* = .{ .connection = connection };
        try add(poller.kqueue, connection, @intFromPtr(watched));
    }

    fn add(kq: Handle, fd: Handle, udata: usize) (ResourceError || UnexpectedError)!void {
        switch (applyChange(kq, .{ .ident = @intCast(fd), .filter = c.EVFILT.READ, .flags = c.EV.ADD, .udata = udata })) {
            .SUCCESS => {},
            .NOMEM => return error.SystemResources,
            else => |e| return unexpected(@src(), "kevent(EVFILT_READ, EV_ADD)", e),
        }
    }

    fn remove(kq: Handle, fd: Handle) void {
        switch (applyChange(kq, .{ .ident = @intCast(fd), .filter = c.EVFILT.READ, .flags = c.EV.DELETE })) {
            .SUCCESS => {},
            else => |e| warnUnexpected(@src(), "kevent(EVFILT_READ, EV_DELETE)", e),
        }
    }

    /// The connections readable or at their end (`ready.len` at most) and whether a client waits on the listening
    /// socket, within `timeout_ms` (null: no limit), or null once stopped. A signal ends the wait early, with nothing;
    /// a failure of the wait itself is logged and ends the polling (null).
    pub fn wait(poller: *Poller, ready: []*Watched, timeout_ms: ?u32) ?platform.Polled {
        var events: [batch]c.Kevent = undefined;
        const room: c_int = @intCast(@min(batch, ready.len));
        var none: [0]c.Kevent = .{};
        const timeout: ?c.timespec = if (timeout_ms) |ms| posix.timespecOfMs(ms) else null;
        const rc = c.kevent(poller.kqueue, &none, 0, &events, room, if (timeout) |*t| t else null);
        if (rc < 0) switch (errnoNow()) {
            .INTR => return .{ .ready = ready[0..0] },
            else => |e| {
                warnUnexpected(@src(), "kevent", e);
                return null;
            },
        };
        var polled: platform.Polled = .{ .ready = ready[0..0] };
        for (events[0..@intCast(rc)]) |event| {
            if (event.filter == c.EVFILT.USER) return null;
            if (event.udata == listening_data) {
                polled.listening = true;
                continue;
            }
            ready[polled.ready.len] = @ptrFromInt(event.udata);
            polled.ready = ready[0 .. polled.ready.len + 1];
        }
        return polled;
    }

    /// Reports a client in the listening socket's backlog from now on, until `unlisten`; a socket listened on already
    /// stays.
    pub fn listen(poller: *Poller, listening: Handle) (ResourceError || UnexpectedError)!void {
        if (poller.listening) |current| {
            if (current == listening) return;
            poller.unlisten();
        }
        try add(poller.kqueue, listening, listening_data);
        poller.listening = listening;
    }

    pub fn unlisten(poller: *Poller) void {
        const listening = poller.listening orelse return;
        poller.listening = null;
        remove(poller.kqueue, listening);
    }

    /// Receives the rest of the frame without waiting.
    pub fn take(_: *Poller, watched: *Watched) platform.Taken {
        return watched.take();
    }

    /// Before the connection's close.
    pub fn unwatch(poller: *Poller, watched: *Watched) void {
        remove(poller.kqueue, watched.connection);
    }

    pub fn stop(poller: *Poller) void {
        setCancelSignal(poller.kqueue);
    }

    /// Nothing is in flight between waits.
    pub fn drain(_: *Poller) void {}
};

// === Segments ===

/// A mapped POSIX shared memory object.
pub const Mapping = posix.Mapping;

/// How many random names `createSegmentHandle` tries before it gives up: a collision of 64 random bits is a sign of
/// something else.
const shm_name_attempts = 4;

/// A new segment descriptor: a POSIX shared memory object (`shm_open`, close-on-exec, read-write, this user's only)
/// under a random name, which is removed at once, so the object reaches only the processes it is sent to. Its memory
/// comes from `createSegment`, which the caller runs outside every lock.
pub fn createSegmentHandle() (ResourceError || UnexpectedError)!Handle {
    for (0..shm_name_attempts) |_| {
        var random: [8]u8 = undefined;
        c.arc4random_buf(&random, random.len);
        // "/fipc-" and 16 hex digits: 22 characters, within the shm name limit of 31
        var name_buf: ["/fipc-".len + 16 + 1]u8 = undefined;
        const name = std.mem.printSentinel(&name_buf, "/fipc-{s}", .{&std.fmt.bytesToHex(random, .lower)}, 0) catch unreachable;
        const flags: c_int = @bitCast(c.O{ .ACCMODE = .RDWR, .CREAT = true, .EXCL = true });
        const fd = c.shm_open(name, flags, @as(c_uint, 0o600));
        if (fd >= 0) {
            if (c.shm_unlink(name) != 0) warnUnexpected(@src(), "shm_unlink", errnoNow());
            return fd;
        }
        switch (errnoNow()) {
            .EXIST => {},
            .MFILE, .NFILE, .NOMEM, .NOSPC => return error.SystemResources,
            else => |e| return unexpected(@src(), "shm_open", e),
        }
    }
    return unexpected(@src(), "shm_open", .EXIST);
}

/// Gives the shared memory object `handle` its `size` bytes (`ftruncate`, which macOS allows once per object, so that
/// no holder can change the size later, and which rounds it up to whole pages), and maps it shared. macOS commits the
/// pages as they are first touched, and has no call that commits them up front: a lack of memory makes the system
/// compress or swap, not fault the mapping, so NO_MEMORY here means the object or the address space couldn't be had.
/// The session id and the kind name nothing here.
pub fn createSegment(handle: ?Handle, _: *const [16]u8, size: usize, _: *const Security, _: platform.SegmentKind) platform.CreateError!Mapping {
    const fd = handle.?;
    switch (c.errno(c.ftruncate(fd, @intCast(size)))) {
        .SUCCESS => {},
        .NOMEM, .INVAL, .FBIG, .NOSPC => return error.OutOfMemory,
        else => |e| return unexpected(@src(), "ftruncate", e),
    }
    return posix.map(fd, size);
}

/// Maps a received segment once it is sure that the segment can't change size and is exactly `size` bytes rounded up
/// to whole pages, so no access inside the mapping can fault. Its size is fixed if it is a POSIX shared memory object,
/// which `proc_pidfdinfo` checks: macOS allows one `ftruncate` per object, and the creator has already made it. The
/// caller then unregisters and closes the descriptor.
pub fn openSegment(received: ?Handle, _: *const [16]u8, size: usize, _: platform.SegmentKind) platform.OpenError!Mapping {
    const fd = received.?;
    var info: [darwin.pshm_fdinfo_size]u8 align(8) = undefined;
    const got = darwin.proc_pidfdinfo(c.getpid(), fd, darwin.PROC_PIDFDPSHMINFO, &info, info.len);
    if (got <= 0) return switch (errnoNow()) {
        .BADF => error.NotSealed, // not a shared memory object: a file could shrink under the mapping
        else => |e| unexpected(@src(), "proc_pidfdinfo(PROC_PIDFDPSHMINFO)", e),
    };
    if (got != info.len) return unexpected(@src(), "proc_pidfdinfo(PROC_PIDFDPSHMINFO)", .RANGE);
    var stat: c.Stat = undefined;
    if (c.fstat(fd, &stat) != 0) return unexpected(@src(), "fstat", errnoNow());
    if (stat.size < 0 or @as(u64, @intCast(stat.size)) != std.mem.alignForward(u64, size, std.heap.pageSize())) return error.WrongSize;
    return posix.map(fd, size);
}

pub const protectReadOnly = posix.protectReadOnly;
pub const revoke = posix.revoke;

// === Ring waits (stream/ring_wait.zig) ===

pub const RingEvent = posix.RingEvent;
pub const no_ring_event = posix.no_ring_event;

/// `os_sync_wait_on_address` with `SHARED`, since the word is in memory shared with the peer: the kernel finds the
/// waiters by the memory object, not the address. It returns early (EFAULT) while the flag's page isn't mapped into
/// this process yet; the waiter's swap of the flag just before has mapped it.
pub fn ringWait(flag: *const std.atomic.Value(u32), _: RingEvent, timeout_ms: u32) bool {
    const timeout_ns = @as(u64, timeout_ms) * std.time.ns_per_ms;
    return darwin.os_sync_wait_on_address_with_timeout(@constCast(&flag.raw), 1, @sizeOf(u32), darwin.OS_SYNC_WAIT_ON_ADDRESS_SHARED, darwin.OS_CLOCK_MACH_ABSOLUTE_TIME, timeout_ns) == -1 and errnoNow() == .TIMEDOUT;
}

/// `os_sync_wake_by_address_any` with `SHARED`: wakes one waiter (ENOENT when none waits).
pub fn ringWake(flag: *const std.atomic.Value(u32), _: RingEvent) void {
    _ = darwin.os_sync_wake_by_address_any(@constCast(&flag.raw), @sizeOf(u32), darwin.OS_SYNC_WAKE_BY_ADDRESS_SHARED);
}

// === Broadcast waits (a broadcast channel's subscribers) ===

pub const wakes_each_slot = posix.wakes_each_slot;
pub const SlotWake = posix.SlotWake;
pub const createSlotWake = posix.createSlotWake;
pub const openSlotWake = posix.openSlotWake;
pub const slotRingEvent = posix.slotRingEvent;
pub const closeSlotWake = posix.closeSlotWake;
pub const setSlotWake = posix.setSlotWake;

/// `os_sync_wait_on_address` with `SHARED` on the shared word, as `ringWait`. It fails with EFAULT in two cases: the
/// word's page isn't mapped into this process yet, or the word has been revoked. On EFAULT the wait checks the page
/// (`protectionOf`):
///   - if the page is readable, loading the word maps it, and the wait starts again once, unless the word has already
///     moved;
///   - if the page has been revoked, the wait returns at once, without a fault.
/// So no caller has to touch the word first. No caller revokes the word during a wait: broadcast/client.zig
/// `unsubscribe` waits until no call is inside the segment guard.
pub fn broadcastWait(word: *const std.atomic.Value(u32), expected: u32, _: SlotWake, timeout_ms: u32) bool {
    const timeout_ns = @as(u64, timeout_ms) * std.time.ns_per_ms;
    for (0..2) |_| {
        if (darwin.os_sync_wait_on_address_with_timeout(@constCast(&word.raw), expected, @sizeOf(u32), darwin.OS_SYNC_WAIT_ON_ADDRESS_SHARED, darwin.OS_CLOCK_MACH_ABSOLUTE_TIME, timeout_ns) != -1) return false;
        switch (errnoNow()) {
            .TIMEDOUT => return true,
            .FAULT => switch (protectionOf(word)) {
                .read_only, .read_write => if (word.load(.monotonic) != expected) return false,
                .none, .other => return false,
            },
            else => return false,
        }
    }
    return false;
}

/// `os_sync_wake_by_address_all` with `SHARED`: every waiter (ENOENT when none waits).
pub fn broadcastWake(word: *const std.atomic.Value(u32)) void {
    _ = darwin.os_sync_wake_by_address_all(@constCast(&word.raw), @sizeOf(u32), darwin.OS_SYNC_WAKE_BY_ADDRESS_SHARED);
}

// === For the tests' leak checks ===

pub const openHandleCount = posix.openHandleCount;

pub fn threadCount() usize {
    const task = c.mach_task_self();
    var list: c.mach_port_array_t = undefined;
    var count: c.mach_msg_type_number_t = 0;
    std.debug.assert(c.task_threads(task, &list, &count) == 0);
    for (list[0..count]) |thread| _ = c.mach_port_deallocate(task, thread);
    _ = c.vm_deallocate(task, @intFromPtr(list), count * @sizeOf(c.mach_port_t));
    return count;
}

/// The calling thread's id: it has exited once no thread of the process has that id.
pub const ThreadMark = struct {
    id: u64,

    pub fn ofCurrentThread() ThreadMark {
        var id: u64 = 0;
        _ = c.pthread_threadid_np(null, &id);
        return .{ .id = id };
    }

    pub fn exited(mark: ThreadMark) bool {
        var buf: [1024]u64 = undefined;
        return std.mem.findScalar(u64, buf[0..threadIds(&buf)], mark.id) == null;
    }

    pub fn deinit(_: ThreadMark) void {}
};

/// The ids of this process's threads, `buf.len` at most: how many it wrote.
fn threadIds(buf: []u64) usize {
    const task = c.mach_task_self();
    var list: c.mach_port_array_t = undefined;
    var count: c.mach_msg_type_number_t = 0;
    std.debug.assert(c.task_threads(task, &list, &count) == 0);
    defer _ = c.vm_deallocate(task, @intFromPtr(list), count * @sizeOf(c.mach_port_t));
    var n: usize = 0;
    for (list[0..count]) |thread| {
        defer _ = c.mach_port_deallocate(task, thread);
        var info: c.thread_identifier_info = undefined;
        var info_count: c.mach_msg_type_number_t = c.THREAD.IDENTIFIER.INFO_COUNT;
        if (c.thread_info(thread, c.THREAD.IDENTIFIER.INFO, @ptrCast(&info), &info_count) == 0 and n < buf.len) {
            buf[n] = info.thread_id;
            n += 1;
        }
    }
    return n;
}

/// The mappings of segments: the regions of this process's address space that are shared, readable and writable, and
/// carry no VM tag (the system's own shared regions are read-only, or tagged as malloc's).
pub fn segmentMappingCount() usize {
    const task = c.mach_task_self();
    var address: c.mach_vm_address_t = 0;
    var depth: c.natural_t = 0;
    var count: usize = 0;
    while (true) {
        var size: c.mach_vm_size_t = 0;
        var info: darwin.vm_region_submap_info_64 = undefined;
        var info_count = darwin.VM_REGION_SUBMAP_INFO_COUNT_64;
        // Fails past the last region (KERN_INVALID_ADDRESS)
        if (c.mach_vm_region_recurse(task, &address, &size, &depth, @ptrCast(&info), &info_count) != 0) break;
        if (info.is_submap != 0) {
            depth += 1;
            continue;
        }
        const shared = switch (info.share_mode) {
            darwin.SM_SHARED, darwin.SM_TRUESHARED, darwin.SM_SHARED_ALIASED => true,
            else => false,
        };
        if (shared and info.protection == read_write and info.user_tag == 0) count += 1;
        address += size;
    }
    return count;
}

const read_write: std.macho.vm_prot_t = .{ .READ = true, .WRITE = true };

/// From the region that holds the address (`mach_vm_region_recurse`, into submaps).
pub fn protectionOf(address: *const anyopaque) platform.Protection {
    const task = c.mach_task_self();
    const at: c.mach_vm_address_t = @intFromPtr(address);
    var region: c.mach_vm_address_t = at;
    var depth: c.natural_t = 0;
    while (true) {
        var size: c.mach_vm_size_t = 0;
        var info: darwin.vm_region_submap_info_64 = undefined;
        var info_count = darwin.VM_REGION_SUBMAP_INFO_COUNT_64;
        if (c.mach_vm_region_recurse(task, &region, &size, &depth, @ptrCast(&info), &info_count) != 0) return .none;
        if (info.is_submap != 0) {
            depth += 1;
            region = at;
            continue;
        }
        if (region > at) return .none; // the first region after it
        return .of(info.protection.READ, info.protection.WRITE);
    }
}

// === Tests of what only macOS does (real system calls; platform/contract_test.zig tests the contract) ===

const testing = std.testing;
const build_options = @import("build_options");

const contract_test = @import("../contract_test.zig");

const no_security: Security = .{};
const testAddress = contract_test.testAddress;
const Pair = contract_test.Pair;
const openFds = openHandleCount;

// The test helpers below are `pub` because rendezvous.zig's tests use them too.

pub fn closeOnExec(fd: Handle) bool {
    return c.fcntl(fd, c.F.GETFD) & c.FD_CLOEXEC != 0;
}

pub fn blocking(fd: Handle) bool {
    return c.fcntl(fd, c.F.GETFL) & posix.o_nonblock == 0;
}

pub fn noSigpipe(fd: Handle) bool {
    var on: c_int = 0;
    var len: c.socklen_t = @sizeOf(c_int);
    return c.getsockopt(fd, c.SOL.SOCKET, c.SO.NOSIGPIPE, &on, &len) == 0 and on != 0;
}

/// The lowest free descriptor number: with the descriptor limit lowered to it, no new descriptor can be opened.
fn lowestFree(any: Handle) !u64 {
    const fd = c.fcntl(any, c.F.DUPFD_CLOEXEC, @as(c_int, 0));
    try testing.expect(fd >= 0);
    _ = c.close(fd);
    return @intCast(fd);
}

test "descriptor exhaustion is SystemResources, which the caller retries after a back-off" {
    const address = testAddress("exhausted");
    const listener = (try claim(&address, &no_security)) orelse return error.TestUnexpectedResult;
    defer close(listener);
    // Lower the limit to the lowest free descriptor: no new one can be opened
    const other = testAddress("exhausted-claim");
    var saved: c.rlimit = undefined;
    try testing.expectEqual(@as(c_int, 0), c.getrlimit(.NOFILE, &saved));
    const limited: c.rlimit = .{ .cur = try lowestFree(listener), .max = saved.max };
    try testing.expectEqual(@as(c_int, 0), c.setrlimit(.NOFILE, &limited));
    const claimed = claim(&other, &no_security);
    const connected = connect(&address, &no_security);
    const shm = createSegmentHandle();
    const cancel = createCancelSignal();
    try testing.expectEqual(@as(c_int, 0), c.setrlimit(.NOFILE, &saved));
    try testing.expectError(error.SystemResources, claimed);
    try testing.expectError(error.SystemResources, connected);
    try testing.expectError(error.SystemResources, shm);
    try testing.expectError(error.SystemResources, cancel);
}

test "accept and peer credentials: both ends see this process and user; descriptors are close-on-exec (the cancel signal too), without SIGPIPE" {
    const ref = try process_state.acquire();
    defer process_state.release(ref);
    const fds = openFds();
    const address = testAddress("cred");
    const pair = try Pair.open(&address);
    defer pair.deinit();
    for ([_]Handle{ pair.listener, pair.accepted, pair.client }) |fd| {
        try testing.expect(closeOnExec(fd));
        try testing.expect(noSigpipe(fd));
    }
    // The accepted socket blocks (the watcher's reads need it), though the listening socket doesn't
    try testing.expect(blocking(pair.accepted) and blocking(pair.client) and !blocking(pair.listener));
    for ([_]Handle{ pair.accepted, pair.client }, [_]platform.Side{ .listener, .client }) |fd, side| {
        const who = peer(fd, side) orelse return error.TestUnexpectedResult;
        try testing.expectEqual(pid(), who.pid.?);
        try testing.expect(who.same_user);
    }
    try testing.expectEqual(fds + 4, openFds()); // the lock file too
    // The cancel signal is close-on-exec too
    const cancel = try createCancelSignal();
    defer close(cancel);
    try testing.expect(closeOnExec(cancel));
}

test "a segment goes across with SCM_RIGHTS: close-on-exec, a size neither side can change, the same memory on both sides" {
    const ref = try process_state.acquire();
    defer process_state.release(ref);
    const fds = openFds();
    const address = testAddress("segment");
    const pair = try Pair.open(&address);
    defer pair.deinit();

    const size = 64 * 1024;
    const id: [16]u8 = @splat(1);
    const shm = try createSegmentHandle();
    try testing.expect(closeOnExec(shm));
    var created = try createSegment(shm, &id, size, &no_security, .stream);
    defer created.unmap();
    const frame = @as([64]u8, @splat(0x5a));
    try posix.sendMsg(pair.accepted, &frame, shm);

    // The peek leaves the frame and the descriptor queued; the receive takes both
    try testing.expectEqual(platform.Waited(void){ .done = {} }, readableWithin(pair.client, 5000));
    var got: [64]u8 = undefined;
    const received = try recvSegment(pair.client, &got);
    try testing.expectEqualSlices(u8, &frame, &got);
    try testing.expect(closeOnExec(received));
    var other = try openSegment(received, &id, size, .stream);
    defer other.unmap();
    // Nobody can resize it: macOS gives a shared memory object its size once (so no access to the mapping can fault)
    try testing.expectEqual(c.E.INVAL, c.errno(c.ftruncate(received, size * 2)));
    close(received);
    created.base[100] = 42;
    try testing.expectEqual(@as(u8, 42), other.base[100]);
    other.base[size - 1] = 7;
    try testing.expectEqual(@as(u8, 7), created.base[size - 1]);
    try testing.expectEqual(c.E.INVAL, c.errno(c.ftruncate(shm, size / 2)));
    try testing.expectEqual(c.E.INVAL, c.errno(c.ftruncate(shm, size * 2)));
    try testing.expectEqual(c.E.INVAL, c.errno(c.ftruncate(shm, size)));
    close(shm);
    try testing.expectEqual(fds + 4, openFds());
}

/// Sends `frame` with the descriptors `fds` (any number) in one message.
fn sendWithFds(fd: Handle, frame: []const u8, comptime n: usize, fds: [n]Handle) !void {
    const iov = [1]std.posix.iovec_const{.{ .base = frame.ptr, .len = frame.len }};
    var control: posix.Rights(n) = .init(fds);
    const msg: c.msghdr_const = .{
        .name = null,
        .namelen = 0,
        .iov = &iov,
        .iovlen = 1,
        .control = &control,
        .controllen = @sizeOf(posix.Rights(n)),
        .flags = 0,
    };
    try testing.expectEqual(@as(isize, @intCast(frame.len)), c.sendmsg(fd, &msg, 0));
}

test "a SEGMENT with no descriptor, two, a short frame or at the descriptor limit is refused, leaking nothing" {
    const ref = try process_state.acquire();
    defer process_state.release(ref);
    const fds = openFds();
    const address = testAddress("refuse");
    const pair = try Pair.open(&address);
    defer pair.deinit();
    const frame = @as([64]u8, @splat(1));
    var got: [64]u8 = undefined;

    try posix.sendMsg(pair.accepted, &frame, null);
    try testing.expectError(error.DescriptorCount, recvSegment(pair.client, &got));

    const a = try createSegmentHandle();
    defer close(a);
    const b = try createSegmentHandle();
    defer close(b);
    try sendWithFds(pair.accepted, &frame, 2, .{ a, b });
    try testing.expectError(error.DescriptorCount, recvSegment(pair.client, &got));
    try testing.expectEqual(fds + 6, openFds()); // both received copies were closed

    try sendWithFds(pair.accepted, frame[0..10], 1, .{a});
    try testing.expectError(error.ShortFrame, recvSegment(pair.client, &got));
    try testing.expectEqual(fds + 6, openFds());

    // At the descriptor limit the kernel can't give this process the descriptor: macOS fails the receive (EMFILE)
    try sendWithFds(pair.accepted, &frame, 1, .{a});
    var saved: c.rlimit = undefined;
    try testing.expectEqual(@as(c_int, 0), c.getrlimit(.NOFILE, &saved));
    const limited: c.rlimit = .{ .cur = try lowestFree(pair.client), .max = saved.max };
    try testing.expectEqual(@as(c_int, 0), c.setrlimit(.NOFILE, &limited));
    const truncated = recvSegment(pair.client, &got);
    try testing.expectEqual(@as(c_int, 0), c.setrlimit(.NOFILE, &saved));
    try testing.expectError(error.ControlTruncated, truncated);
    try testing.expectEqual(fds + 6, openFds());
}

test "a received segment must be a POSIX shared memory object of exactly the announced size, in whole pages" {
    const size = 4096;
    const id: [16]u8 = @splat(2);
    // An object no one gave its size: empty
    const empty = try createSegmentHandle();
    defer close(empty);
    try testing.expectError(error.WrongSize, openSegment(empty, &id, size, .stream));

    // Not shared memory: a device, a socket
    const device = c.open("/dev/null", .{ .ACCMODE = .RDWR, .CLOEXEC = true });
    try testing.expect(device >= 0);
    defer close(device);
    try testing.expectError(error.NotSealed, openSegment(device, &id, size, .stream));
    const address = testAddress("not-shm");
    const listener = (try claim(&address, &no_security)) orelse return error.TestUnexpectedResult;
    defer close(listener);
    try testing.expectError(error.NotSealed, openSegment(listener, &id, size, .stream));

    // The object has whole pages: an announced size in the same pages maps, one past them doesn't
    const page = std.heap.pageSize();
    const sized = try createSegmentHandle();
    defer close(sized);
    var created = try createSegment(sized, &id, size, &no_security, .stream);
    defer created.unmap();
    try testing.expectError(error.WrongSize, openSegment(sized, &id, page + size, .stream));
    var again = try openSegment(sized, &id, size, .stream);
    again.unmap();
    var whole = try openSegment(sized, &id, page, .stream);
    whole.unmap();
}

test "a frame sent to a peer that is gone fails with BrokenPipe and no SIGPIPE, even at SIG_DFL" {
    const ref = try process_state.acquire();
    defer process_state.release(ref);
    var saved: std.posix.Sigaction = undefined;
    const default: std.posix.Sigaction = .{ .handler = .{ .handler = std.posix.SIG.DFL }, .mask = std.posix.sigemptyset(), .flags = 0 };
    std.posix.sigaction(.PIPE, &default, &saved);
    defer std.posix.sigaction(.PIPE, &saved, null);
    const frame = @as([64]u8, @splat(2));
    {
        const address = testAddress("sigpipe");
        const pair = try Pair.open(&address);
        defer {
            close(pair.client);
            close(pair.listener);
        }
        close(pair.accepted);
        // If SIGPIPE were raised, its default action would kill the test process here
        try testing.expectError(error.BrokenPipe, posix.sendMsg(pair.client, &frame, null));
        // The READY a client sends after the listener died: the same
        try testing.expect(!sendFrame(pair.client, &frame, null));
    }
    {
        // The accepted end, whose SO_NOSIGPIPE comes from the listening socket
        const address = testAddress("sigpipe-accepted");
        const pair = try Pair.open(&address);
        defer {
            close(pair.accepted);
            close(pair.listener);
        }
        close(pair.client);
        try testing.expectError(error.BrokenPipe, posix.sendMsg(pair.accepted, &frame, null));
    }
}

test {
    _ = rendezvous; // its own tests
    if (build_options.slow_tests) _ = slow_tests;
}

/// The tests that fork (the slow tier, `zig build test-slow`). A forked child runs only its checks and leaves with
/// `_exit`: it never returns into the test runner.
const slow_tests = struct {
    /// Milliseconds on the monotonic clock, without `Io`: a forked child makes no call on its parent's instances.
    fn nowMs() u64 {
        var ts: c.timespec = undefined;
        std.debug.assert(c.clock_gettime(.MONOTONIC, &ts) == 0);
        return @as(u64, @intCast(ts.sec)) * std.time.ms_per_s + @as(u64, @intCast(ts.nsec)) / std.time.ns_per_ms;
    }

    test "slow: a ring sleeper in another process sleeps on the shared flag until the publisher wakes it" {
        const id: [16]u8 = @splat(5);
        const shm = try createSegmentHandle();
        defer close(shm);
        var mapping = try createSegment(shm, &id, 4096, &no_security, .stream);
        defer mapping.unmap();
        const flag: *std.atomic.Value(u32) = @ptrCast(@alignCast(mapping.base));
        flag.store(0, .monotonic);

        const child = c.fork();
        if (child == 0) {
            // The waiter's side: announce the sleep, then sleep until woken (exit 0), at most 5 s (exit 1)
            const t0 = nowMs();
            _ = flag.swap(1, .acq_rel);
            while (flag.load(.acquire) == 1 and nowMs() - t0 < 5000) _ = ringWait(flag, no_ring_event, 5000);
            c._exit(if (nowMs() - t0 < 2000) 0 else 1);
        }
        try testing.expect(child > 0);
        const t0 = nowMs();
        while (flag.load(.acquire) == 0 and nowMs() - t0 < 5000) sleep(1);
        sleep(50); // the child is asleep by now, mostly
        if (flag.swap(0, .acq_rel) == 1) ringWake(flag, no_ring_event);
        try testing.expectEqual(@as(?u8, 0), process_state.waitChild(child, 5000));
    }
};
