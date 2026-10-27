//! Linux's side of the OS contract (platform.zig): the abstract `AF_UNIX` rendezvous (claim, connect, accept), peer
//! credentials (`SO_PEERCRED`), the sealed `memfd` segment, the cancel signal (an eventfd), the poller (an epoll
//! instance) and the ring waits (a futex). What Linux shares with macOS (the process, frames sent with `MSG_NOSIGNAL`,
//! the segment passed by `SCM_RIGHTS`, the handshake's waits, the mapping) is in platform/posix.zig, re-exported here.
//!
//! Every call returns at once except four:
//!   - the handshake's waits on the caller's thread (`acceptWithin`, `readWithin`, `readableWithin`: a `poll` with a
//!     timeout, then a non-blocking call);
//!   - the watcher's `read`, which blocks through `Io`;
//!   - the poller's `wait`, on its own thread;
//!   - `ringWait`, the one wait `Io` can't express (a futex shared with the peer; stream/ring_wait.zig slices it).
//!
//! Descriptors: every descriptor the library owns is opened close-on-exec, and it is opened, registered, unregistered
//! and closed under the handle lock (core/process_state.zig). The functions here don't take the lock. Their callers
//! hold it around the calls that open a descriptor (`claim`, `connect`, `createCancelSignal`, `createSegmentHandle`,
//! `recvSegment`, `Poller.init`) and around `close`. `acceptWithin` waits, so it runs without the lock, and the fork
//! counter covers that window.

const std = @import("std");
const linux = std.os.linux;
const names = @import("../../core/names.zig");
const platform = @import("../../platform.zig");
const posix = @import("../posix.zig");
const process_state = @import("../../core/process_state.zig");

const ResourceError = platform.ResourceError;
const UnexpectedError = platform.UnexpectedError;
const unexpected = posix.unexpected;
const warnUnexpected = posix.warnUnexpected;

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

/// `exit_group` itself, not libc's `_exit`, which ThreadSanitizer intercepts to wait `atexit_sleep_ms` first.
pub fn crash(code: u8) noreturn {
    linux.exit_group(code);
}

pub const writeStderr = posix.writeStderr;
pub const task_stack_size = posix.task_stack_size;
pub const shutDownIo = posix.shutDownIo;

// === Names and connections ===

/// An abstract socket address: `sun_path` holds a NUL byte, then the rendezvous name (names.linuxRendezvous),
/// with no trailing NUL; the length counts exactly those bytes. Abstract names have no file, and the kernel
/// frees one with its socket.
pub const Address = struct {
    sockaddr: linux.sockaddr.un,
    len: linux.socklen_t,

    pub fn init(name: []const u8) Address {
        var address: Address = .{
            .sockaddr = .{ .path = @splat(0) },
            .len = @intCast(@offsetOf(linux.sockaddr.un, "path") + 1 + name.len),
        };
        @memcpy(address.sockaddr.path[1..][0..name.len], name);
        return address;
    }
};

/// The name's abstract socket, which carries this user's id.
pub fn addressOf(name: []const u8) UnexpectedError!Address {
    var buf: [names.linux_max]u8 = undefined;
    return .init(names.linuxRendezvous(&buf, linux.geteuid(), name));
}

/// Claims the name: a new non-blocking socket (`acceptWithin` polls, then accepts without waiting), `bind`,
/// `listen(4)`. Null when another socket holds the name (EADDRINUSE).
pub fn claim(address: *const Address, _: *const Security) (ResourceError || UnexpectedError)!?Handle {
    const fd = try socket(linux.SOCK.STREAM | linux.SOCK.CLOEXEC | linux.SOCK.NONBLOCK);
    errdefer close(fd);
    switch (linux.errno(linux.bind(fd, @ptrCast(&address.sockaddr), address.len))) {
        .SUCCESS => {},
        .ADDRINUSE => {
            close(fd);
            return null;
        },
        .NOMEM, .NOBUFS => return error.SystemResources,
        else => |e| return unexpected(@src(), "bind", e),
    }
    switch (linux.errno(linux.listen(fd, 4))) {
        .SUCCESS => return fd,
        else => |e| return unexpected(@src(), "listen", e),
    }
}

/// None: one listening socket serves every client. On Windows a client takes the pipe instance it connected on.
pub fn nextListening(_: *const Address, _: *const Security) (ResourceError || UnexpectedError)!?Handle {
    return null;
}

/// Connects to the name without blocking (a new non-blocking socket, `connect`), then puts the socket in blocking
/// mode, as `Io`'s reads need. `no_listener`: ECONNREFUSED, ENOENT, or a holder between `bind` and `listen`. `busy`:
/// the listener's backlog is full (EAGAIN). Never `foreign`: `peer` tells the user.
pub fn connect(address: *const Address, _: *const Security) (ResourceError || UnexpectedError)!platform.Connect {
    const fd = try socket(linux.SOCK.STREAM | linux.SOCK.CLOEXEC | linux.SOCK.NONBLOCK);
    errdefer close(fd);
    const outcome: platform.Connect = switch (linux.errno(linux.connect(fd, &address.sockaddr, address.len))) {
        .SUCCESS => {
            try posix.setNonblocking(fd, false);
            return .{ .connected = fd };
        },
        .CONNREFUSED, .NOENT => .no_listener,
        .AGAIN => .busy,
        .NOMEM, .NOBUFS => return error.SystemResources,
        else => |e| return unexpected(@src(), "connect", e),
    };
    close(fd);
    return outcome;
}

/// Polls the listening socket and the cancel signal, then accepts without waiting: the new socket is close-on-exec and
/// blocking, as the watcher's `Io` reads need. A client that left between the poll and the accept (EAGAIN) is nothing;
/// one aborted in the backlog (ECONNABORTED) is `closing`; descriptor exhaustion is `transient`.
pub fn acceptWithin(listening: Handle, cancel: ?Handle, timeout_ms: ?u32, forks: ?*u32) platform.Waited(platform.Accept) {
    switch (posix.pollFor(listening, cancel, timeout_ms)) {
        .ready => {},
        .idle => return .idle,
        .cancelled => return .cancelled,
    }
    if (forks) |count| count.* = process_state.forkCount();
    const rc = linux.accept4(listening, null, null, linux.SOCK.CLOEXEC);
    return .{ .done = switch (linux.errno(rc)) {
        .SUCCESS => .{ .connection = @intCast(rc) },
        .AGAIN, .INTR => return .idle,
        .CONNABORTED => .closing,
        .MFILE, .NFILE, .NOBUFS, .NOMEM => .transient,
        else => |e| other: {
            warnUnexpected(@src(), "accept4", e);
            break :other .other;
        },
    } };
}

pub const dropClient = posix.dropClient;

/// The user and process at the other end, as the kernel recorded them at `connect` or `listen` (`SO_PEERCRED`).
pub fn peer(connection: Handle, _: platform.Side) ?platform.Peer {
    const credentials = peerCredentials(connection) catch return null;
    return .{ .same_user = credentials.uid == linux.geteuid(), .pid = @intCast(credentials.pid) };
}

const Credentials = struct { pid: linux.pid_t, uid: linux.uid_t };

fn peerCredentials(fd: Handle) UnexpectedError!Credentials {
    const Ucred = extern struct { pid: linux.pid_t, uid: linux.uid_t, gid: linux.gid_t };
    var credentials: Ucred = undefined;
    var len: linux.socklen_t = @sizeOf(Ucred);
    switch (linux.errno(linux.getsockopt(fd, linux.SOL.SOCKET, linux.SO.PEERCRED, @ptrCast(&credentials), &len))) {
        .SUCCESS => return .{ .pid = credentials.pid, .uid = credentials.uid },
        else => |e| return unexpected(@src(), "getsockopt(SO_PEERCRED)", e),
    }
}

pub const unblock = posix.unblock;
pub const endTask = posix.endTask;
pub const close = posix.close;

fn socket(flags: u32) (ResourceError || UnexpectedError)!Handle {
    const rc = linux.socket(linux.AF.UNIX, flags, 0);
    return switch (linux.errno(rc)) {
        .SUCCESS => @intCast(rc),
        .MFILE, .NFILE, .NOBUFS, .NOMEM => error.SystemResources,
        else => |e| unexpected(@src(), "socket", e),
    };
}

// === Frames on a connection ===

pub const sendFrame = posix.sendFrame;
pub const read = posix.read;
pub const recvSegment = posix.recvSegment;

// === Waits on the caller's thread ===

pub const readWithin = posix.readWithin;
pub const readableWithin = posix.readableWithin;

/// An eventfd: once set it stays readable for good (nothing reads it).
pub fn createCancelSignal() (ResourceError || UnexpectedError)!Handle {
    const rc = linux.eventfd(0, linux.EFD.CLOEXEC | linux.EFD.NONBLOCK);
    return switch (linux.errno(rc)) {
        .SUCCESS => @intCast(rc),
        .MFILE, .NFILE, .NODEV, .NOMEM => error.SystemResources,
        else => |e| unexpected(@src(), "eventfd", e),
    };
}

pub fn setCancelSignal(fd: Handle) void {
    const one: u64 = 1;
    _ = linux.write(fd, std.mem.asBytes(&one), @sizeOf(u64));
}

pub const waitCancelSignal = posix.waitCancelSignal;
pub const sleep = posix.sleep;

// === The poller ===

pub const Watched = posix.Watched;

/// An epoll instance, level-triggered: a connection with bytes or at its end is reported at every wait until it is
/// read or unwatched, also when it ended before its watch, and the listening socket while a client waits in its
/// backlog. Its stop is an eventfd (`createCancelSignal`), registered with no `Watched` (data 0); the listening socket
/// has data 1, which no `Watched` has.
pub const Poller = struct {
    epoll: Handle,
    stop_signal: Handle,
    /// The listening socket in the epoll instance (`listen`), if any.
    listening: ?Handle = null,

    /// The most connections one `wait` reports.
    const batch = 64;
    const stop_data = 0;
    const listening_data = 1;

    pub fn init() (ResourceError || UnexpectedError)!Poller {
        const rc = linux.epoll_create1(linux.EPOLL.CLOEXEC);
        const epoll: Handle = switch (linux.errno(rc)) {
            .SUCCESS => @intCast(rc),
            .MFILE, .NFILE, .NOMEM => return error.SystemResources,
            else => |e| return unexpected(@src(), "epoll_create1", e),
        };
        errdefer close(epoll);
        const stop_signal = try createCancelSignal();
        errdefer close(stop_signal);
        try add(epoll, stop_signal, stop_data);
        return .{ .epoll = epoll, .stop_signal = stop_signal };
    }

    pub fn handles(poller: *const Poller) [2]?Handle {
        return .{ poller.epoll, poller.stop_signal };
    }

    pub fn watch(poller: *Poller, watched: *Watched, connection: Handle) (ResourceError || UnexpectedError)!void {
        watched.* = .{ .connection = connection };
        try add(poller.epoll, connection, @intFromPtr(watched));
    }

    fn add(epoll: Handle, fd: Handle, data: usize) (ResourceError || UnexpectedError)!void {
        var event: linux.epoll_event = .{ .events = linux.EPOLL.IN | linux.EPOLL.RDHUP, .data = .{ .ptr = data } };
        switch (linux.errno(linux.epoll_ctl(epoll, linux.EPOLL.CTL_ADD, fd, &event))) {
            .SUCCESS => {},
            .NOMEM, .NOSPC => return error.SystemResources,
            else => |e| return unexpected(@src(), "epoll_ctl(EPOLL_CTL_ADD)", e),
        }
    }

    fn remove(epoll: Handle, fd: Handle) void {
        switch (linux.errno(linux.epoll_ctl(epoll, linux.EPOLL.CTL_DEL, fd, null))) {
            .SUCCESS => {},
            else => |e| warnUnexpected(@src(), "epoll_ctl(EPOLL_CTL_DEL)", e),
        }
    }

    /// The connections readable or at their end (`ready.len` at most) and whether a client waits on the listening
    /// socket, within `timeout_ms` (null: no limit), or null once stopped. A signal ends the wait early, with nothing;
    /// a failure of the wait itself is logged and ends the polling (null).
    pub fn wait(poller: *Poller, ready: []*Watched, timeout_ms: ?u32) ?platform.Polled {
        var events: [batch]linux.epoll_event = undefined;
        const room: u32 = @intCast(@min(batch, ready.len));
        const rc = linux.epoll_pwait(poller.epoll, &events, room, posix.pollTimeout(timeout_ms), null);
        switch (linux.errno(rc)) {
            .SUCCESS => {},
            .INTR => return .{ .ready = ready[0..0] },
            else => |e| {
                warnUnexpected(@src(), "epoll_pwait", e);
                return null;
            },
        }
        var polled: platform.Polled = .{ .ready = ready[0..0] };
        for (events[0..rc]) |event| switch (event.data.ptr) {
            stop_data => return null,
            listening_data => polled.listening = true,
            else => |data| {
                ready[polled.ready.len] = @ptrFromInt(data);
                polled.ready = ready[0 .. polled.ready.len + 1];
            },
        };
        return polled;
    }

    /// Reports a client in the listening socket's backlog from now on, until `unlisten`; a socket listened on already
    /// stays.
    pub fn listen(poller: *Poller, listening: Handle) (ResourceError || UnexpectedError)!void {
        if (poller.listening) |current| {
            if (current == listening) return;
            poller.unlisten();
        }
        try add(poller.epoll, listening, listening_data);
        poller.listening = listening;
    }

    pub fn unlisten(poller: *Poller) void {
        const listening = poller.listening orelse return;
        poller.listening = null;
        remove(poller.epoll, listening);
    }

    /// Receives the rest of the frame without waiting.
    pub fn take(_: *Poller, watched: *Watched) platform.Taken {
        return watched.take();
    }

    /// Before the connection's close: a forked child may still hold its open file description, which would keep it in
    /// the epoll instance, reported with a `Watched` that is gone.
    pub fn unwatch(poller: *Poller, watched: *Watched) void {
        remove(poller.epoll, watched.connection);
    }

    pub fn stop(poller: *Poller) void {
        setCancelSignal(poller.stop_signal);
    }

    /// Nothing is in flight between waits.
    pub fn drain(_: *Poller) void {}
};

// === Segments ===

/// A mapped memfd.
pub const Mapping = posix.Mapping;

/// `createSegment`'s unit of allocation: 2 MiB, a fraction of a millisecond of page clearing.
const fallocate_chunk = 2 << 20;

/// A new segment descriptor: an anonymous `memfd`, close-on-exec, that accepts seals. Its memory comes from
/// `createSegment`, which the caller runs outside every lock.
pub fn createSegmentHandle() (ResourceError || UnexpectedError)!Handle {
    const rc = linux.memfd_create("fastipc", linux.MFD.CLOEXEC | linux.MFD.ALLOW_SEALING);
    return switch (linux.errno(rc)) {
        .SUCCESS => @intCast(rc),
        .MFILE, .NFILE, .NOMEM => error.SystemResources,
        else => |e| unexpected(@src(), "memfd_create", e),
    };
}

/// Gives the memfd `handle` its `size` bytes up front (`ftruncate`, then `fallocate`: a full `/dev/shm` or memory
/// limit is NO_MEMORY now instead of a SIGBUS later), seals its size so no holder can shrink or grow it
/// (`F_SEAL_SHRINK`, `F_SEAL_GROW`, and `F_SEAL_SEAL` so no seal is added or removed later), and maps it shared.
/// The session id and the kind name nothing here.
///
/// A signal interrupts either call with EINTR, which `SA_RESTART` doesn't restart for `fallocate`: each is retried in
/// place rather than giving the claim up. A kernel that ends shmem's `fallocate` on any pending signal (5.x;
/// newer ones end it only on a fatal one) also frees what that call allocated, so the memory comes in
/// `fallocate_chunk`s: under a periodic signal a chunk still completes, and the chunks before it stay allocated.
pub fn createSegment(handle: ?Handle, _: *const [16]u8, size: usize, _: *const Security, _: platform.SegmentKind) platform.CreateError!Mapping {
    const fd = handle.?;
    while (true) switch (linux.errno(linux.ftruncate(fd, @intCast(size)))) {
        .SUCCESS => break,
        .INTR => {},
        .FBIG, .INVAL => return error.OutOfMemory,
        else => |e| return unexpected(@src(), "ftruncate", e),
    };
    var offset: usize = 0;
    while (offset < size) {
        const len = @min(fallocate_chunk, size - offset);
        switch (linux.errno(linux.fallocate(fd, 0, @intCast(offset), @intCast(len)))) {
            .SUCCESS => offset += len,
            .INTR => {},
            .NOSPC, .NOMEM, .FBIG => return error.OutOfMemory,
            else => |e| return unexpected(@src(), "fallocate", e),
        }
    }
    const seals = linux.F.SEAL_SHRINK | linux.F.SEAL_GROW | linux.F.SEAL_SEAL;
    switch (linux.errno(linux.fcntl(fd, linux.F.ADD_SEALS, seals))) {
        .SUCCESS => {},
        else => |e| return unexpected(@src(), "fcntl(F_ADD_SEALS)", e),
    }
    return posix.map(fd, size);
}

/// Maps a received segment once it can't change size (the seals of `createSegment`; a descriptor that isn't a
/// sealable `memfd` fails `F_GET_SEALS`) and is exactly `size` bytes, so no access inside the mapping can
/// fault. The caller then unregisters and closes the descriptor.
pub fn openSegment(received: ?Handle, _: *const [16]u8, size: usize, _: platform.SegmentKind) platform.OpenError!Mapping {
    const fd = received.?;
    const required = linux.F.SEAL_SHRINK | linux.F.SEAL_GROW | linux.F.SEAL_SEAL;
    const seals = linux.fcntl(fd, linux.F.GET_SEALS, 0);
    switch (linux.errno(seals)) {
        .SUCCESS => if (seals & required != required) return error.NotSealed,
        .INVAL => return error.NotSealed,
        else => |e| return unexpected(@src(), "fcntl(F_GET_SEALS)", e),
    }
    var stat: linux.Statx = undefined;
    switch (linux.errno(linux.statx(fd, "", linux.AT.EMPTY_PATH, .{ .SIZE = true }, &stat))) {
        .SUCCESS => if (stat.size != size) return error.WrongSize,
        else => |e| return unexpected(@src(), "statx", e),
    }
    return posix.map(fd, size);
}

pub const protectReadOnly = posix.protectReadOnly;
pub const revoke = posix.revoke;

// === Ring waits (stream/ring_wait.zig) ===

pub const RingEvent = posix.RingEvent;
pub const no_ring_event = posix.no_ring_event;

/// A non-private `FUTEX_WAIT` (`futexWait`), since the word is in memory shared with the peer (`Io`'s futex is
/// process-private).
pub fn ringWait(flag: *const std.atomic.Value(u32), _: RingEvent, timeout_ms: u32) bool {
    return futexWait(flag, 1, timeout_ms);
}

/// A non-private `FUTEX_WAKE` of one waiter.
pub fn ringWake(flag: *const std.atomic.Value(u32), _: RingEvent) void {
    futexWake(flag, 1);
}

/// Sleeps while `word`, in memory shared with other processes, holds `expected`, for at most `timeout_ms`: whether the
/// timeout ran out.
inline fn futexWait(word: *const std.atomic.Value(u32), expected: u32, timeout_ms: u32) bool {
    const timeout = posix.timespecOfMs(timeout_ms);
    return linux.errno(linux.futex_4arg(word, .{ .cmd = .WAIT, .private = false }, expected, &timeout)) == .TIMEDOUT;
}

/// Wakes up to `waiters` threads sleeping on `word` (`futexWait`), in any process.
inline fn futexWake(word: *const std.atomic.Value(u32), waiters: u32) void {
    _ = linux.futex_3arg(word, .{ .cmd = .WAKE, .private = false }, waiters);
}

// === Broadcast waits (a broadcast channel's subscribers) ===

pub const wakes_each_slot = posix.wakes_each_slot;
pub const SlotWake = posix.SlotWake;
pub const createSlotWake = posix.createSlotWake;
pub const openSlotWake = posix.openSlotWake;
pub const slotRingEvent = posix.slotRingEvent;
pub const closeSlotWake = posix.closeSlotWake;
pub const setSlotWake = posix.setSlotWake;

/// A non-private `FUTEX_WAIT` on the shared word, as `ringWait`.
pub fn broadcastWait(word: *const std.atomic.Value(u32), expected: u32, _: SlotWake, timeout_ms: u32) bool {
    return futexWait(word, expected, timeout_ms);
}

/// A non-private `FUTEX_WAKE` of every waiter: one system call, however many sleep.
pub fn broadcastWake(word: *const std.atomic.Value(u32)) void {
    futexWake(word, std.math.maxInt(i32));
}

// === For the tests' leak checks ===

pub const openHandleCount = posix.openHandleCount;

/// The number of threads of this process (from `/proc/self/status`).
pub fn threadCount() usize {
    const fd = linux.open("/proc/self/status", .{ .ACCMODE = .RDONLY, .CLOEXEC = true }, 0);
    std.debug.assert(linux.errno(fd) == .SUCCESS);
    defer _ = linux.close(@intCast(fd));
    var buf: [4096]u8 = undefined;
    const n = linux.read(@intCast(fd), &buf, buf.len);
    std.debug.assert(linux.errno(n) == .SUCCESS);
    const text = buf[0..n];
    const start = std.mem.find(u8, text, "\nThreads:").? + "\nThreads:".len;
    const end = std.mem.findScalarPos(u8, text, start, '\n').?;
    return std.fmt.parseInt(usize, std.mem.trim(u8, text[start..end], " \t"), 10) catch unreachable;
}

/// The calling thread's id: it has exited once `/proc/self/task/<id>` is gone.
pub const ThreadMark = struct {
    tid: linux.pid_t,

    pub fn ofCurrentThread() ThreadMark {
        return .{ .tid = linux.gettid() };
    }

    pub fn exited(mark: ThreadMark) bool {
        var path_buf: [64]u8 = undefined;
        const path = std.mem.printSentinel(&path_buf, "/proc/self/task/{d}", .{mark.tid}, 0) catch unreachable;
        const fd = linux.open(path, .{ .ACCMODE = .RDONLY, .CLOEXEC = true, .PATH = true }, 0);
        if (linux.errno(fd) != .SUCCESS) return true;
        _ = linux.close(@intCast(fd));
        return false;
    }

    pub fn deinit(_: ThreadMark) void {}
};

/// The mappings of segments: the lines of `/proc/self/maps` that name a FastIPC memfd (`/memfd:fastipc`).
pub fn segmentMappingCount() usize {
    var maps: MapsLines = .open();
    defer maps.close();
    var count: usize = 0;
    while (maps.next()) |line| {
        if (mapsLineNamesSegment(line)) count += 1;
    }
    return count;
}

/// Whether a maps line names a FastIPC memfd.
fn mapsLineNamesSegment(line: []const u8) bool {
    return std.mem.find(u8, line, "/memfd:fastipc") != null;
}

/// From the line of `/proc/self/maps` whose range holds the address.
pub fn protectionOf(address: *const anyopaque) platform.Protection {
    var maps: MapsLines = .open();
    defer maps.close();
    while (maps.next()) |line| {
        if (mapsLineProtection(line, @intFromPtr(address))) |protection| return protection;
    }
    return .none;
}

/// A maps line's protection if its range (`lo-hi perms ...`, hexadecimal) holds `at`.
fn mapsLineProtection(line: []const u8, at: usize) ?platform.Protection {
    var fields = std.mem.tokenizeScalar(u8, line, ' ');
    const range = fields.next() orelse return null;
    const perms = fields.next() orelse return null;
    const dash = std.mem.findScalar(u8, range, '-') orelse return null;
    const low = std.fmt.parseInt(usize, range[0..dash], 16) catch return null;
    const high = std.fmt.parseInt(usize, range[dash + 1 ..], 16) catch return null;
    if (at < low or at >= high or perms.len < 2) return null;
    return .of(perms[0] == 'r', perms[1] == 'w');
}

/// The lines of `/proc/self/maps`, one at a time, read in pieces into a buffer that holds any line.
const MapsLines = struct {
    fd: Handle,
    buf: [16384]u8 = undefined,
    /// The bytes read and not yet returned: `buf[start..end]`.
    start: usize = 0,
    end: usize = 0,

    fn open() MapsLines {
        const rc = linux.open("/proc/self/maps", .{ .ACCMODE = .RDONLY, .CLOEXEC = true }, 0);
        std.debug.assert(linux.errno(rc) == .SUCCESS);
        return .{ .fd = @intCast(rc) };
    }

    fn close(maps: *MapsLines) void {
        _ = linux.close(maps.fd);
    }

    /// The next line, without its newline; null at the end of the file.
    fn next(maps: *MapsLines) ?[]const u8 {
        while (true) {
            if (std.mem.findScalarPos(u8, maps.buf[0..maps.end], maps.start, '\n')) |newline| {
                const line = maps.buf[maps.start..newline];
                maps.start = newline + 1;
                return line;
            }
            // A line that straddles two reads: move its start to the front of the buffer and read the rest after it
            const kept = maps.end - maps.start;
            @memmove(maps.buf[0..kept], maps.buf[maps.start..maps.end]);
            maps.start = 0;
            maps.end = kept;
            if (maps.end == maps.buf.len) return null;
            const n = linux.read(maps.fd, maps.buf[maps.end..].ptr, maps.buf.len - maps.end);
            if (linux.errno(n) != .SUCCESS or n == 0) return null;
            maps.end += n;
        }
    }
};

// === Tests of what only Linux does (real system calls; platform/contract_test.zig tests the contract) ===

const testing = std.testing;

const no_security: Security = .{};

/// Each test's name is unique to its process (`contract_test.testAddress`).
const contract_test = @import("../contract_test.zig");
const testAddress = contract_test.testAddress;
const Pair = contract_test.Pair;

const openFds = openHandleCount;

test "descriptor exhaustion is SystemResources, which the caller retries after a back-off" {
    const address = testAddress("exhausted");
    const listener = (try claim(&address, &no_security)) orelse return error.TestUnexpectedResult;
    defer close(listener);
    // Lower the limit to the lowest free descriptor: no new one can be opened
    const lowest_free: u64 = lowest: {
        const rc = linux.fcntl(listener, linux.F.DUPFD_CLOEXEC, 0);
        try testing.expectEqual(linux.E.SUCCESS, linux.errno(rc));
        close(@intCast(rc));
        break :lowest rc;
    };
    var saved: linux.rlimit = undefined;
    try testing.expectEqual(linux.E.SUCCESS, linux.errno(linux.getrlimit(.NOFILE, &saved)));
    const limited: linux.rlimit = .{ .cur = lowest_free, .max = saved.max };
    try testing.expectEqual(linux.E.SUCCESS, linux.errno(linux.setrlimit(.NOFILE, &limited)));
    const claimed = claim(&testAddress("exhausted-claim"), &no_security);
    const connected = connect(&address, &no_security);
    const memfd = createSegmentHandle();
    try testing.expectEqual(linux.E.SUCCESS, linux.errno(linux.setrlimit(.NOFILE, &saved)));
    try testing.expectError(error.SystemResources, claimed);
    try testing.expectError(error.SystemResources, connected);
    try testing.expectError(error.SystemResources, memfd);
}

test "connect: no listener, a name bound but not listening yet, connected, and a full backlog" {
    const fds = openFds();
    const address = testAddress("connect");
    try testing.expectEqual(platform.Connect.no_listener, try connect(&address, &no_security));

    // Between the winner's bind and listen: refused, like no listener
    const bound = try socket(linux.SOCK.STREAM | linux.SOCK.CLOEXEC);
    try testing.expectEqual(linux.E.SUCCESS, linux.errno(linux.bind(bound, @ptrCast(&address.sockaddr), address.len)));
    try testing.expectEqual(platform.Connect.no_listener, try connect(&address, &no_security));
    close(bound);

    const listener = (try claim(&address, &no_security)) orelse return error.TestUnexpectedResult;
    defer close(listener);
    var queued: [16]Handle = undefined;
    var n: usize = 0;
    defer for (queued[0..n]) |fd| close(fd);
    // Nobody accepts: the backlog of 4 fills, then clients are told the name is busy
    const busy = while (n < queued.len) {
        switch (try connect(&address, &no_security)) {
            .connected => |fd| {
                queued[n] = fd;
                n += 1;
            },
            .busy => break true,
            .no_listener, .foreign => return error.TestUnexpectedResult,
        }
    } else false;
    try testing.expect(busy);
    try testing.expect(n >= 1);
    // A connected socket is in blocking mode
    const flags = linux.fcntl(queued[0], linux.F.GETFL, 0);
    try testing.expect(flags & linux.SOCK.NONBLOCK == 0);
    try testing.expectEqual(fds + 1 + n, openFds());
}

test "accept and peer credentials: both ends see this process and user; descriptors are close-on-exec, the accepted one blocks" {
    const ref = try process_state.acquire();
    defer process_state.release(ref);
    const fds = openFds();
    const address = testAddress("cred");
    const pair = try Pair.open(&address);
    defer pair.deinit();
    for ([_]Handle{ pair.listener, pair.accepted, pair.client }) |fd| {
        try testing.expect(linux.fcntl(fd, linux.F.GETFD, 0) & linux.FD_CLOEXEC != 0);
    }
    // The accepted socket blocks (the watcher's reads need it)
    try testing.expect(linux.fcntl(pair.accepted, linux.F.GETFL, 0) & linux.SOCK.NONBLOCK == 0);
    for ([_]Handle{ pair.accepted, pair.client }, [_]platform.Side{ .listener, .client }) |fd, side| {
        const who = peer(fd, side) orelse return error.TestUnexpectedResult;
        try testing.expectEqual(pid(), who.pid.?);
        try testing.expect(who.same_user);
    }
    try testing.expectEqual(fds + 3, openFds());
}

test "a segment goes across with SCM_RIGHTS: close-on-exec, sealed, the same memory on both sides" {
    const ref = try process_state.acquire();
    defer process_state.release(ref);
    const fds = openFds();
    const address = testAddress("segment");
    const pair = try Pair.open(&address);
    defer pair.deinit();

    const size = 64 * 1024;
    const id: [16]u8 = @splat(1);
    const memfd = try createSegmentHandle();
    var created = try createSegment(memfd, &id, size, &no_security, .stream);
    defer created.unmap();
    const frame = @as([64]u8, @splat(0x5a));
    try posix.sendMsg(pair.accepted, &frame, memfd);

    // The peek leaves the frame and the descriptor queued; the receive takes both
    try testing.expectEqual(platform.Waited(void){ .done = {} }, readableWithin(pair.client, 5000));
    var got: [64]u8 = undefined;
    const received = try recvSegment(pair.client, &got);
    try testing.expectEqualSlices(u8, &frame, &got);
    try testing.expect(linux.fcntl(received, linux.F.GETFD, 0) & linux.FD_CLOEXEC != 0);
    var other = try openSegment(received, &id, size, .stream);
    defer other.unmap();
    close(received);
    created.base[100] = 42;
    try testing.expectEqual(@as(u8, 42), other.base[100]);
    other.base[size - 1] = 7;
    try testing.expectEqual(@as(u8, 7), created.base[size - 1]);

    // Nobody can resize it or change its seals (so no access to the mapping can fault with SIGBUS)
    try testing.expectEqual(linux.E.PERM, linux.errno(linux.ftruncate(memfd, size / 2)));
    try testing.expectEqual(linux.E.PERM, linux.errno(linux.ftruncate(memfd, size * 2)));
    try testing.expectEqual(linux.E.PERM, linux.errno(linux.fcntl(memfd, linux.F.ADD_SEALS, linux.F.SEAL_WRITE)));
    close(memfd);
    try testing.expectEqual(fds + 3, openFds());
}

/// Sends `frame` with the descriptors `fds` (any number) in one message.
fn sendWithFds(fd: Handle, frame: []const u8, comptime n: usize, fds: [n]Handle) !void {
    const iov = [1]std.posix.iovec_const{.{ .base = frame.ptr, .len = frame.len }};
    var control: posix.Rights(n) = .init(fds);
    const msg: linux.msghdr_const = .{
        .name = null,
        .namelen = 0,
        .iov = &iov,
        .iovlen = 1,
        .control = &control,
        .controllen = @sizeOf(posix.Rights(n)),
        .flags = 0,
    };
    try testing.expectEqual(frame.len, linux.sendmsg(fd, &msg, linux.MSG.NOSIGNAL));
}

test "a SEGMENT with no descriptor, two, a short frame or dropped control data is refused, leaking nothing" {
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
    try testing.expectEqual(fds + 5, openFds()); // both received copies were closed

    try sendWithFds(pair.accepted, frame[0..10], 1, .{a});
    try testing.expectError(error.ShortFrame, recvSegment(pair.client, &got));
    try testing.expectEqual(fds + 5, openFds());

    // At the descriptor limit the kernel can't install the descriptor: MSG_CTRUNC
    try sendWithFds(pair.accepted, &frame, 1, .{a});
    const lowest_free: u64 = lowest: {
        const rc = linux.fcntl(pair.client, linux.F.DUPFD_CLOEXEC, 0);
        try testing.expectEqual(linux.E.SUCCESS, linux.errno(rc));
        close(@intCast(rc));
        break :lowest rc;
    };
    var saved: linux.rlimit = undefined;
    try testing.expectEqual(linux.E.SUCCESS, linux.errno(linux.getrlimit(.NOFILE, &saved)));
    const limited: linux.rlimit = .{ .cur = lowest_free, .max = saved.max };
    try testing.expectEqual(linux.E.SUCCESS, linux.errno(linux.setrlimit(.NOFILE, &limited)));
    const truncated = recvSegment(pair.client, &got);
    try testing.expectEqual(linux.E.SUCCESS, linux.errno(linux.setrlimit(.NOFILE, &saved)));
    try testing.expectError(error.ControlTruncated, truncated);
    try testing.expectEqual(fds + 5, openFds());
}

test "a received segment must be a sealed memfd of exactly the announced size" {
    const size = 4096;
    const id: [16]u8 = @splat(2);
    const unsealed = try createSegmentHandle();
    defer close(unsealed);
    try testing.expectEqual(linux.E.SUCCESS, linux.errno(linux.ftruncate(unsealed, size)));
    try testing.expectError(error.NotSealed, openSegment(unsealed, &id, size, .stream));

    const address = testAddress("not-memfd");
    const listener = (try claim(&address, &no_security)) orelse return error.TestUnexpectedResult;
    defer close(listener);
    try testing.expectError(error.NotSealed, openSegment(listener, &id, size, .stream));

    const sealed = try createSegmentHandle();
    defer close(sealed);
    var created = try createSegment(sealed, &id, size, &no_security, .stream);
    defer created.unmap();
    try testing.expectError(error.WrongSize, openSegment(sealed, &id, size * 2, .stream));
    var again = try openSegment(sealed, &id, size, .stream);
    again.unmap();
}

test "a frame sent to a peer that is gone fails with BrokenPipe and no SIGPIPE, even at SIG_DFL" {
    const ref = try process_state.acquire();
    defer process_state.release(ref);
    const address = testAddress("sigpipe");
    const pair = try Pair.open(&address);
    defer {
        close(pair.client);
        close(pair.listener);
    }
    close(pair.accepted);

    // If MSG_NOSIGNAL were missing, SIGPIPE's default action would kill the test process here
    var saved: std.posix.Sigaction = undefined;
    const default: std.posix.Sigaction = .{ .handler = .{ .handler = std.posix.SIG.DFL }, .mask = std.posix.sigemptyset(), .flags = 0 };
    std.posix.sigaction(.PIPE, &default, &saved);
    defer std.posix.sigaction(.PIPE, &saved, null);
    const frame = @as([64]u8, @splat(2));
    try testing.expectError(error.BrokenPipe, posix.sendMsg(pair.client, &frame, null));
    // The READY a client sends after the listener died: the same
    try testing.expect(!sendFrame(pair.client, &frame, null));
}
