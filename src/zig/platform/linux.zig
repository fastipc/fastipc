//! Linux's side of the OS contract (platform.zig): the abstract `AF_UNIX` rendezvous (claim, connect, accept), peer
//! credentials, frames sent with `MSG_NOSIGNAL`, the sealed `memfd` segment passed by `SCM_RIGHTS`, the cancel signal (an
//! eventfd), and the ring waits' futex calls. Every function is an instant system call, except the handshake's waits on
//! the caller's thread (`acceptWithin`, `readWithin`, `readableWithin`: a `poll` with a timeout, then a non-blocking
//! call), the watcher's `read`, which blocks through `Io`, and `ringWait`, the one wait `Io` can't express (a futex
//! shared with the peer; data/ring_wait.zig slices it).
//!
//! Descriptors: every one the library owns is opened and registered, unregistered and closed under the handle lock
//! (process_state.zig). The functions here don't take the lock; their callers hold it around the calls that open a
//! descriptor (`claim`, `connect`, `createCancelSignal`, `createSegmentHandle`, `recvSegment`) and around `close`.
//! `acceptWithin` waits, so it runs without the lock; the fork counter covers that window. Every descriptor is opened
//! close-on-exec.

const std = @import("std");
const linux = std.os.linux;
const Io = std.Io;
const log = @import("../log.zig");
const names = @import("../lifecycle/names.zig");
const platform = @import("../platform.zig");
const process_state = @import("../process_state.zig");

const ResourceError = platform.ResourceError;
const UnexpectedError = platform.UnexpectedError;

pub const has_fork = true;
pub const passes_segment_handle = true;

pub const Handle = linux.fd_t;

// === The process ===

/// Nothing to hold: an abstract socket name carries this user's id (names.linuxRendezvous), a peer's user is checked
/// from its credentials (`peer`), and a memfd reaches only the peer it is sent to.
pub const Security = struct {
    pub fn init() UnexpectedError!Security {
        return .{};
    }

    pub fn deinit(_: *Security) void {}
};

/// A `pthread_mutex_t`, which fork handlers can take.
pub const Lock = struct {
    raw: std.c.pthread_mutex_t = .{},

    pub fn lock(l: *Lock) void {
        std.debug.assert(std.c.pthread_mutex_lock(&l.raw) == .SUCCESS);
    }

    pub fn unlock(l: *Lock) void {
        std.debug.assert(std.c.pthread_mutex_unlock(&l.raw) == .SUCCESS);
    }
};

pub fn pid() u32 {
    return @intCast(linux.getpid());
}

pub fn crash(code: u8) noreturn {
    linux.exit_group(code);
}

pub fn writeStderr(bytes: []const u8) void {
    _ = std.c.write(2, bytes.ptr, bytes.len);
}

/// The std default: 16 MiB of address space, committed on demand.
pub const task_stack_size: usize = std.Thread.SpawnConfig.default_stack_size;

/// The join is enough: a shared object isn't unloaded under a thread that has left it.
pub fn shutDownIo(threaded: *Io.Threaded) void {
    threaded.deinit();
}

// === The rendezvous ===

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

/// The listening socket stays: none.
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
            const flags = linux.fcntl(fd, linux.F.GETFL, 0);
            if (linux.errno(flags) != .SUCCESS) return unexpected(@src(), "fcntl(F_GETFL)", linux.errno(flags));
            const nonblock: usize = linux.SOCK.NONBLOCK; // the O_NONBLOCK bit
            const rc = linux.fcntl(fd, linux.F.SETFL, flags & ~nonblock);
            if (linux.errno(rc) != .SUCCESS) return unexpected(@src(), "fcntl(F_SETFL)", linux.errno(rc));
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
    switch (pollFor(listening, cancel, timeout_ms)) {
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

/// Closes the accepted socket, shut down first when `forked` (closing only the parent's copy would leave the
/// connection up).
pub fn dropClient(connection: Handle, forked: bool) ?Handle {
    if (forked) unblock(connection);
    close(connection);
    return null;
}

/// The user and process at the other end, as the kernel recorded them at `connect` or `listen` (`SO_PEERCRED`).
pub fn peer(connection: Handle, _: platform.Side) ?platform.Peer {
    const cred = peerCred(connection) catch return null;
    return .{ .same_user = cred.uid == linux.geteuid(), .pid = @intCast(cred.pid) };
}

const Cred = struct { pid: linux.pid_t, uid: linux.uid_t };

fn peerCred(fd: Handle) UnexpectedError!Cred {
    const Ucred = extern struct { pid: linux.pid_t, uid: linux.uid_t, gid: linux.gid_t };
    var cred: Ucred = undefined;
    var len: linux.socklen_t = @sizeOf(Ucred);
    switch (linux.errno(linux.getsockopt(fd, linux.SOL.SOCKET, linux.SO.PEERCRED, @ptrCast(&cred), &len))) {
        .SUCCESS => return .{ .pid = cred.pid, .uid = cred.uid },
        else => |e| return unexpected(@src(), "getsockopt(SO_PEERCRED)", e),
    }
}

/// Shuts both directions of a socket down, which ends a blocked read on it at once. Never on a forked child's copy: it
/// would shut the parent's connection down too.
pub fn unblock(fd: Handle) void {
    _ = linux.shutdown(fd, linux.SHUT.RDWR);
}

/// `unblock` has ended the watcher's blocking read: POSIX never cancels a task through `Io`, which would send a signal.
pub fn endTask(io: Io, task: *Io.Future(void)) void {
    task.await(io);
}

pub fn close(fd: Handle) void {
    _ = linux.close(fd);
}

fn socket(flags: u32) (ResourceError || UnexpectedError)!Handle {
    const rc = linux.socket(linux.AF.UNIX, flags, 0);
    return switch (linux.errno(rc)) {
        .SUCCESS => @intCast(rc),
        .MFILE, .NFILE, .NOBUFS, .NOMEM => error.SystemResources,
        else => |e| unexpected(@src(), "socket", e),
    };
}

// === Frames ===

const SendError = error{
    /// The peer is gone (EPIPE, ECONNRESET), or the frame didn't fit its socket buffer at once (EAGAIN, a short
    /// send), which a connection carrying one frame each way never sees: either way the attempt ends.
    BrokenPipe,
} || UnexpectedError;

/// Sends one frame, with `attached` as an `SCM_RIGHTS` descriptor when given: instant, without `io`.
pub fn sendFrame(_: Io, connection: Handle, frame: []const u8, attached: ?Handle) Io.Cancelable!bool {
    sendMsg(connection, frame, attached) catch return false;
    return true;
}

/// One `sendmsg`: without blocking (`MSG_DONTWAIT`; a frame fits an empty socket buffer) and without SIGPIPE
/// (`MSG_NOSIGNAL`), whatever the host's disposition.
fn sendMsg(fd: Handle, frame: []const u8, segment: ?Handle) SendError!void {
    const iov = [1]std.posix.iovec_const{.{ .base = frame.ptr, .len = frame.len }};
    var control: Rights(1) = undefined;
    var msg: linux.msghdr_const = .{
        .name = null,
        .namelen = 0,
        .iov = &iov,
        .iovlen = 1,
        .control = null,
        .controllen = 0,
        .flags = 0,
    };
    if (segment) |segment_fd| {
        control = .init(.{segment_fd});
        msg.control = &control;
        msg.controllen = @sizeOf(Rights(1));
    }
    const rc = linux.sendmsg(fd, &msg, linux.MSG.NOSIGNAL | linux.MSG.DONTWAIT);
    switch (linux.errno(rc)) {
        .SUCCESS => if (rc != frame.len) return error.BrokenPipe,
        .PIPE, .CONNRESET, .AGAIN => return error.BrokenPipe,
        else => |e| return unexpected(@src(), "sendmsg", e),
    }
}

/// An `SCM_RIGHTS` control message carrying `n` descriptors (`CMSG_SPACE(n * sizeof(int))` bytes).
fn Rights(comptime n: usize) type {
    return extern struct {
        header: linux.cmsghdr,
        fds: [n]Handle,

        const Self = @This();

        fn init(fds: [n]Handle) Self {
            return .{
                .header = .{ .len = @sizeOf(linux.cmsghdr) + n * @sizeOf(Handle), .level = linux.SOL.SOCKET, .type = linux.SCM.RIGHTS },
                .fds = fds,
            };
        }
    };
}

/// Polls until the connection has data or has ended, leaving the frame and its descriptor queued for `recvSegment`.
pub fn readableWithin(fd: Handle, timeout_ms: ?u32) platform.Waited(void) {
    return switch (pollFor(fd, null, timeout_ms)) {
        .ready => .{ .done = {} },
        .idle => .idle,
        .cancelled => unreachable, // nothing to cancel it
    };
}

/// Polls the connection and the cancel signal, then receives what arrived without waiting. 0: the connection ended,
/// or the receive failed.
pub fn readWithin(fd: Handle, buf: []u8, cancel: ?Handle, timeout_ms: ?u32) platform.Waited(usize) {
    switch (pollFor(fd, cancel, timeout_ms)) {
        .ready => {},
        .idle => return .idle,
        .cancelled => return .cancelled,
    }
    const rc = linux.recvfrom(fd, buf.ptr, buf.len, linux.MSG.DONTWAIT, null, null);
    return switch (linux.errno(rc)) {
        .SUCCESS => .{ .done = rc },
        .AGAIN, .INTR => .idle,
        else => .{ .done = 0 },
    };
}

const Polled = enum { ready, idle, cancelled };

/// Waits up to `timeout_ms` (null: no limit) until `fd` is readable or has ended, or the cancel signal is set, which
/// wins over the descriptor. A signal ends the wait early: `idle`, and the caller waits again.
fn pollFor(fd: Handle, cancel: ?Handle, timeout_ms: ?u32) Polled {
    var fds = [2]linux.pollfd{
        .{ .fd = fd, .events = linux.POLL.IN, .revents = 0 },
        .{ .fd = cancel orelse -1, .events = linux.POLL.IN, .revents = 0 }, // a negative descriptor is ignored
    };
    const timeout: i32 = if (timeout_ms) |ms| @intCast(@min(ms, std.math.maxInt(i32))) else -1;
    const rc = linux.poll(&fds, fds.len, timeout);
    switch (linux.errno(rc)) {
        .SUCCESS => {},
        .INTR => return .idle,
        else => |e| {
            warnUnexpected(@src(), "poll", e);
            return .idle;
        },
    }
    if (fds[1].revents != 0) return .cancelled;
    if (fds[0].revents != 0) return .ready;
    return .idle;
}

// === The cancel signal ===

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

pub fn waitCancelSignal(fd: Handle, timeout_ms: u32) bool {
    var fds = [1]linux.pollfd{.{ .fd = fd, .events = linux.POLL.IN, .revents = 0 }};
    const rc = linux.poll(&fds, 1, @intCast(@min(timeout_ms, std.math.maxInt(i32))));
    return linux.errno(rc) == .SUCCESS and fds[0].revents != 0;
}

pub fn sleep(ms: u32) void {
    const ts: linux.timespec = .{ .sec = @intCast(ms / 1000), .nsec = @intCast(@as(u64, ms % 1000) * std.time.ns_per_ms) };
    _ = linux.nanosleep(&ts, null);
}

/// Receives the SEGMENT frame into `frame` and its descriptor, marked close-on-exec by the kernel
/// (`MSG_CMSG_CLOEXEC`: no fork+exec in the host can inherit it), without blocking: the caller waited with
/// `readableWithin`. Exactly `frame.len` bytes and exactly one descriptor, else every received descriptor is closed
/// and the error says what was wrong (ControlTruncated: the kernel dropped control data, `MSG_CTRUNC`).
pub fn recvSegment(fd: Handle, frame: []u8) platform.RecvError!Handle {
    var iov = [1]std.posix.iovec{.{ .base = frame.ptr, .len = frame.len }};
    // Room for more descriptors than the one expected, so that extra ones arrive (and are closed) rather than
    // being dropped as MSG_CTRUNC
    var control: Rights(4) = undefined;
    var msg: linux.msghdr = .{
        .name = null,
        .namelen = 0,
        .iov = &iov,
        .iovlen = 1,
        .control = &control,
        .controllen = @sizeOf(Rights(4)),
        .flags = 0,
    };
    const rc = linux.recvmsg(fd, &msg, linux.MSG.CMSG_CLOEXEC | linux.MSG.DONTWAIT);
    switch (linux.errno(rc)) {
        .SUCCESS => {},
        .CONNRESET => return error.EndOfStream,
        else => |e| return unexpected(@src(), "recvmsg", e),
    }
    // The descriptors that arrived, whatever else is wrong: every SCM_RIGHTS message in the control data
    var received: [4]Handle = undefined;
    var count: usize = 0;
    var offset: usize = 0;
    const bytes = std.mem.asBytes(&control)[0..msg.controllen];
    while (offset + @sizeOf(linux.cmsghdr) <= bytes.len) {
        const header: *align(1) const linux.cmsghdr = @ptrCast(bytes[offset..].ptr);
        if (header.len < @sizeOf(linux.cmsghdr) or offset + header.len > bytes.len) break;
        if (header.level == linux.SOL.SOCKET and header.type == linux.SCM.RIGHTS) {
            const payload = bytes[offset + @sizeOf(linux.cmsghdr) .. offset + header.len];
            var i: usize = 0;
            while (i + @sizeOf(Handle) <= payload.len) : (i += @sizeOf(Handle)) {
                received[count] = std.mem.readInt(Handle, payload[i..][0..@sizeOf(Handle)], .native);
                count += 1;
            }
        }
        offset += std.mem.alignForward(usize, header.len, @alignOf(linux.cmsghdr));
    }
    const failure: ?platform.RecvError = if (rc == 0)
        error.EndOfStream
    else if (rc != frame.len)
        error.ShortFrame
    else if (msg.flags & linux.MSG.CTRUNC != 0)
        error.ControlTruncated
    else if (count != 1)
        error.DescriptorCount
    else
        null;
    if (failure) |err| {
        for (received[0..count]) |extra| close(extra);
        return err;
    }
    return received[0];
}

/// Reads through `io` into `buf`, blocking until at least one byte arrives. 0: the connection ended, or the read
/// failed (after `unblock` of the socket too).
pub fn read(io: Io, fd: Handle, buf: []u8) Io.Cancelable!usize {
    const file: Io.File = .{ .handle = fd, .flags = .{ .nonblocking = false } };
    const result = try io.operate(.{ .file_read_streaming = .{ .file = file, .data = &.{buf} } });
    return result.file_read_streaming catch 0;
}

// === The segment ===

const page_align = std.heap.page_size_min;

/// A mapped memfd. Its ring sleepers need no event: the sleep flag is the futex word itself.
pub const Mapping = struct {
    base: [*]align(page_align) u8,
    size: usize,

    pub fn unmap(m: *Mapping) void {
        _ = std.c.munmap(m.base, m.size);
        m.* = undefined;
    }

    pub fn ringEvent(_: *const Mapping, _: names.Object) RingEvent {}
};

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
/// The session id names nothing here.
///
/// A signal interrupts either call with EINTR, which `SA_RESTART` doesn't restart for `fallocate`: each is retried in
/// place rather than giving the claim up. A kernel that ends shmem's `fallocate` on any pending signal (5.x;
/// newer ones end it only on a fatal one) also frees what that call allocated, so the memory comes in
/// `fallocate_chunk`s: under a periodic signal a chunk still completes, and the chunks before it stay allocated.
pub fn createSegment(handle: ?Handle, _: *const [16]u8, size: usize, _: *const Security) platform.CreateError!Mapping {
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
    return map(fd, size);
}

/// Maps a received segment once it can't change size (the seals of `createSegment`; a descriptor that isn't a
/// sealable `memfd` fails `F_GET_SEALS`) and is exactly `size` bytes, so no access inside the mapping can
/// fault. The caller then unregisters and closes the descriptor.
pub fn openSegment(received: ?Handle, _: *const [16]u8, size: usize) platform.OpenError!Mapping {
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
    return map(fd, size);
}

/// Through the C library, like `Mapping.unmap`, so that ThreadSanitizer sees the mapping: a new segment often gets the
/// address of one unmapped a moment before, and the kernel's ordering of the two is invisible to it otherwise.
fn map(fd: Handle, size: usize) (error{OutOfMemory} || UnexpectedError)!Mapping {
    const base = std.c.mmap(null, size, .{ .READ = true, .WRITE = true }, .{ .TYPE = .SHARED }, fd, 0);
    if (base == std.c.MAP_FAILED) return switch (@as(linux.E, @fromBackingInt(@intCast(std.c._errno().*)))) {
        .NOMEM => error.OutOfMemory,
        else => |e| unexpected(@src(), "mmap", e),
    };
    return .{ .base = @ptrCast(@alignCast(base)), .size = size };
}

/// Logs an errno the protocol doesn't expect, once per change of errno at that place on this thread: a handshake
/// retries most calls every few milliseconds (log.zig `warnOnChange`).
fn unexpected(comptime src: std.lang.SourceLocation, comptime call: []const u8, e: linux.E) UnexpectedError {
    warnUnexpected(src, call, e);
    return error.Unexpected;
}

fn warnUnexpected(comptime src: std.lang.SourceLocation, comptime call: []const u8, e: linux.E) void {
    log.warnOnChange(src, @backingInt(e), call ++ " failed: errno {d} ({s})", .{ @backingInt(e), std.enums.tagName(linux.E, e) orelse "unnamed" });
}

// === Ring waits (data/ring_wait.zig) ===

/// None: the sleep flag is the futex word itself.
pub const RingEvent = void;
pub const no_ring_event: RingEvent = {};

/// A non-private `FUTEX_WAIT`, since the word is in memory shared with the peer (`Io`'s futex is process-private).
pub fn ringWait(flag: *const std.atomic.Value(u32), _: RingEvent, timeout_ms: u32) bool {
    const ts: linux.timespec = .{ .sec = @intCast(timeout_ms / 1000), .nsec = @intCast(@as(u64, timeout_ms % 1000) * std.time.ns_per_ms) };
    return linux.errno(linux.futex_4arg(flag, .{ .cmd = .WAIT, .private = false }, 1, &ts)) == .TIMEDOUT;
}

/// A non-private `FUTEX_WAKE` of one waiter.
pub fn ringWake(flag: *const std.atomic.Value(u32), _: RingEvent) void {
    _ = linux.futex_3arg(flag, .{ .cmd = .WAKE, .private = false }, 1);
}

// === For the tests' leak checks ===

/// The open descriptors of this process (0 to 1023).
pub fn openHandleCount() usize {
    var count: usize = 0;
    for (0..1024) |fd| {
        if (linux.errno(linux.fcntl(@intCast(fd), linux.F.GETFD, 0)) == .SUCCESS) count += 1;
    }
    return count;
}

/// The mappings of segments (`/memfd:fastipc` in `/proc/self/maps`).
pub fn segmentMappingCount() usize {
    const rc = linux.open("/proc/self/maps", .{ .ACCMODE = .RDONLY, .CLOEXEC = true }, 0);
    std.debug.assert(linux.errno(rc) == .SUCCESS);
    const fd: Handle = @intCast(rc);
    defer _ = linux.close(fd);
    const needle = "/memfd:fastipc";
    var buf: [16384]u8 = undefined;
    var kept: usize = 0;
    var count: usize = 0;
    while (true) {
        const n = linux.read(fd, buf[kept..].ptr, buf.len - kept);
        if (linux.errno(n) != .SUCCESS or n == 0) break;
        const text = buf[0 .. kept + n];
        count += std.mem.count(u8, text, needle);
        kept = @min(needle.len - 1, text.len); // a match may straddle two reads
        @memmove(buf[0..kept], text[text.len - kept ..]);
    }
    return count;
}

// === Tests (real system calls; the name of each test's socket is unique to its process) ===

const testing = std.testing;

const no_security: Security = .{};

/// A rendezvous address unique to this process and call.
fn testAddress(comptime tag: []const u8) Address {
    const counter = struct {
        var next = std.atomic.Value(u32).init(0);
    };
    var name_buf: [64]u8 = undefined;
    const name = std.mem.print(&name_buf, "test-" ++ tag ++ "-{d}-{d}", .{ linux.getpid(), counter.next.fetchAdd(1, .monotonic) }) catch unreachable;
    return addressOf(name) catch unreachable;
}

const openFds = openHandleCount;

fn accepted(outcome: platform.Waited(platform.Accept)) !Handle {
    return switch (outcome) {
        .done => |accept| switch (accept) {
            .connection => |fd| fd,
            else => error.TestUnexpectedResult,
        },
        else => error.TestUnexpectedResult,
    };
}

/// A connected pair: the listener's accepted socket and the client's socket (`client`), plus the listening socket.
const Pair = struct {
    listener: Handle,
    accepted: Handle,
    client: Handle,

    fn open(address: *const Address) !Pair {
        const listener = (try claim(address, &no_security)) orelse return error.TestUnexpectedResult;
        errdefer close(listener);
        const client = switch (try connect(address, &no_security)) {
            .connected => |fd| fd,
            else => return error.TestUnexpectedResult,
        };
        errdefer close(client);
        return .{ .listener = listener, .accepted = try accepted(acceptWithin(listener, null, 5000, null)), .client = client };
    }

    fn deinit(pair: Pair) void {
        close(pair.accepted);
        close(pair.client);
        close(pair.listener);
    }
};

test "claim: one socket per name, which is free again at once when that socket closes" {
    const fds = openFds();
    const address = testAddress("claim");
    const first = (try claim(&address, &no_security)) orelse return error.TestUnexpectedResult;
    try testing.expectEqual(@as(?Handle, null), try claim(&address, &no_security));
    close(first);
    const again = (try claim(&address, &no_security)) orelse return error.TestUnexpectedResult;
    close(again);
    try testing.expectEqual(fds, openFds());
}

fn claimRacer(address: *const Address, start: *std.atomic.Value(u32), claimed: *?Handle) void {
    while (start.load(.acquire) == 0) std.atomic.spinLoopHint();
    claimed.* = claim(address, &no_security) catch null;
}

test "claim race: of several threads binding one name at once, exactly one gets it" {
    const fds = openFds();
    for (0..20) |_| {
        const address = testAddress("race");
        var start = std.atomic.Value(u32).init(0);
        var claimed: [8]?Handle = @splat(null);
        var threads: [8]std.Thread = undefined;
        for (&threads, &claimed) |*thread, *slot| thread.* = try std.Thread.spawn(.{}, claimRacer, .{ &address, &start, slot });
        start.store(1, .release);
        for (threads) |thread| thread.join();
        var winners: usize = 0;
        for (claimed) |slot| if (slot) |fd| {
            winners += 1;
            close(fd);
        };
        try testing.expectEqual(@as(usize, 1), winners);
    }
    try testing.expectEqual(fds, openFds());
}

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

test "accept and peer credentials: both ends see this process and user; descriptors are close-on-exec" {
    const ref = try process_state.acquire();
    defer process_state.release(ref);
    const fds = openFds();
    const address = testAddress("cred");
    const pair = try Pair.open(&address);
    defer pair.deinit();
    for ([_]Handle{ pair.listener, pair.accepted, pair.client }) |fd| {
        try testing.expect(linux.fcntl(fd, linux.F.GETFD, 0) & linux.FD_CLOEXEC != 0);
    }
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
    var created = try createSegment(memfd, &id, size, &no_security);
    defer created.unmap();
    const frame = @as([64]u8, @splat(0x5a));
    try sendMsg(pair.accepted, &frame, memfd);

    // The peek leaves the frame and the descriptor queued; the receive takes both
    try testing.expectEqual(platform.Waited(void){ .done = {} }, readableWithin(pair.client, 5000));
    var got: [64]u8 = undefined;
    const received = try recvSegment(pair.client, &got);
    try testing.expectEqualSlices(u8, &frame, &got);
    try testing.expect(linux.fcntl(received, linux.F.GETFD, 0) & linux.FD_CLOEXEC != 0);
    var other = try openSegment(received, &id, size);
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
    var control: Rights(n) = .init(fds);
    const msg: linux.msghdr_const = .{
        .name = null,
        .namelen = 0,
        .iov = &iov,
        .iovlen = 1,
        .control = &control,
        .controllen = @sizeOf(Rights(n)),
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

    try sendMsg(pair.accepted, &frame, null);
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
    try testing.expectError(error.NotSealed, openSegment(unsealed, &id, size));

    const address = testAddress("not-memfd");
    const listener = (try claim(&address, &no_security)) orelse return error.TestUnexpectedResult;
    defer close(listener);
    try testing.expectError(error.NotSealed, openSegment(listener, &id, size));

    const sealed = try createSegmentHandle();
    defer close(sealed);
    var created = try createSegment(sealed, &id, size, &no_security);
    defer created.unmap();
    try testing.expectError(error.WrongSize, openSegment(sealed, &id, size * 2));
    var again = try openSegment(sealed, &id, size);
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
    try testing.expectError(error.BrokenPipe, sendMsg(pair.client, &frame, null));
    // The READY a client sends after the listener died: the same
    try testing.expect(!try sendFrame(ref.io, pair.client, &frame, null));
}

test "unblock ends a blocked read (end of stream)" {
    const ref = try process_state.acquire();
    defer process_state.release(ref);
    const fds = openFds();
    {
        const address = testAddress("stop-read");
        const pair = try Pair.open(&address);
        defer pair.deinit();
        var buf: [64]u8 = undefined;
        var reading = try ref.io.concurrent(read, .{ ref.io, pair.client, @as([]u8, &buf) });
        letItBlock();
        unblock(pair.client);
        try testing.expectEqual(@as(usize, 0), try reading.await(ref.io));
    }
    try testing.expectEqual(fds, openFds());
}

test "a read or a peek after the peer closed is the end of the stream" {
    const ref = try process_state.acquire();
    defer process_state.release(ref);
    const address = testAddress("eof");
    const pair = try Pair.open(&address);
    defer {
        close(pair.client);
        close(pair.listener);
    }
    close(pair.accepted);
    var buf: [64]u8 = undefined;
    try testing.expectEqual(@as(usize, 0), try read(ref.io, pair.client, &buf));
    try testing.expectEqual(platform.Waited(void){ .done = {} }, readableWithin(pair.client, 0));
    try testing.expectEqual(platform.Waited(usize){ .done = 0 }, readWithin(pair.client, &buf, null, 0));
}

test "the waits on the caller's thread: nothing within the time, a client or bytes, the cancel signal, which wins and stays set" {
    const ref = try process_state.acquire();
    defer process_state.release(ref);
    const fds = openFds();
    const address = testAddress("within");
    const listener = (try claim(&address, &no_security)) orelse return error.TestUnexpectedResult;
    defer close(listener);
    const cancel = try createCancelSignal();
    defer close(cancel);
    // Nobody connected: 0 only looks, a timeout waits that long
    try testing.expectEqual(platform.Waited(platform.Accept).idle, acceptWithin(listener, cancel, 0, null));
    try testing.expectEqual(platform.Waited(platform.Accept).idle, acceptWithin(listener, cancel, 20, null));
    const client = switch (try connect(&address, &no_security)) {
        .connected => |fd| fd,
        else => return error.TestUnexpectedResult,
    };
    defer close(client);
    const server = try accepted(acceptWithin(listener, cancel, 0, null));
    defer close(server);
    // The accepted socket blocks (the watcher's reads need it)
    try testing.expect(linux.fcntl(server, linux.F.GETFL, 0) & linux.SOCK.NONBLOCK == 0);
    var buf: [64]u8 = undefined;
    try testing.expectEqual(platform.Waited(usize).idle, readWithin(server, &buf, cancel, 0));
    try testing.expectEqual(platform.Waited(void).idle, readableWithin(client, 10));
    try testing.expect(try sendFrame(ref.io, client, "abc", null));
    try testing.expectEqual(platform.Waited(usize){ .done = 3 }, readWithin(server, &buf, cancel, 5000));
    try testing.expectEqualStrings("abc", buf[0..3]);
    // The cancel signal: a back-off ends, and every wait that watches it returns cancelled, for good
    try testing.expect(!waitCancelSignal(cancel, 0));
    setCancelSignal(cancel);
    try testing.expect(waitCancelSignal(cancel, 5000));
    try testing.expect(try sendFrame(ref.io, client, "d", null));
    try testing.expectEqual(platform.Waited(usize).cancelled, readWithin(server, &buf, cancel, null));
    try testing.expectEqual(platform.Waited(platform.Accept).cancelled, acceptWithin(listener, cancel, null, null));
    try testing.expectEqual(platform.Waited(usize){ .done = 1 }, readWithin(server, &buf, null, 0));
    try testing.expectEqual(fds + 4, openFds());
}

/// Gives a task just started time to block in its system call: the stop rule must work whether it has or not,
/// and the test is only more telling when it has.
fn letItBlock() void {
    const ts: linux.timespec = .{ .sec = 0, .nsec = 20 * std.time.ns_per_ms };
    _ = linux.nanosleep(&ts, null);
}

/// The id of the calling thread, and whether that thread has exited since.
const Worker = struct {
    tid: linux.pid_t = 0,

    fn run(worker: *Worker, io: Io, gate: *std.atomic.Value(u32)) void {
        worker.tid = linux.gettid();
        while (gate.load(.acquire) == 0) io.futexWaitUncancelable(u32, &gate.raw, 0);
    }

    fn gone(worker: Worker) bool {
        var path_buf: [64]u8 = undefined;
        const path = std.mem.printSentinel(&path_buf, "/proc/self/task/{d}", .{worker.tid}, 0) catch unreachable;
        const fd = linux.open(path, .{ .ACCMODE = .RDONLY, .CLOEXEC = true, .PATH = true }, 0);
        if (linux.errno(fd) != .SUCCESS) return true;
        _ = linux.close(@intCast(fd));
        return false;
    }
};

/// The number of threads of this process (from `/proc/self/status`).
fn threadCount() usize {
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

fn recordThread(id: *std.Thread.Id) void {
    id.* = std.Thread.getCurrentId();
}

test "no library thread is left after the process-wide Io's last release, and a new instance works" {
    // Warm up, so that what the process creates once is in the baseline
    {
        const ref = try process_state.acquire();
        var id: std.Thread.Id = 0;
        var task = try ref.io.concurrent(recordThread, .{&id});
        task.await(ref.io);
        process_state.release(ref);
    }
    const threads_before = threadCount();

    const ref = try process_state.acquire();
    var gate = std.atomic.Value(u32).init(0);
    var workers: [3]Worker = @splat(.{});
    var tasks: [3]Io.Future(void) = undefined;
    for (&tasks, &workers) |*task, *worker| task.* = try ref.io.concurrent(Worker.run, .{ worker, ref.io, &gate });
    gate.store(1, .release);
    ref.io.futexWake(u32, &gate.raw, std.math.maxInt(u32));
    for (&tasks) |*task| task.await(ref.io);
    process_state.release(ref);

    // Joined by `release`; the kernel ends each thread a moment later
    const deadline = std.time.ns_per_s;
    var waited: u64 = 0;
    while (waited < deadline and !(workers[0].gone() and workers[1].gone() and workers[2].gone() and threadCount() <= threads_before)) : (waited += std.time.ns_per_ms) {
        const ts: linux.timespec = .{ .sec = 0, .nsec = std.time.ns_per_ms };
        _ = linux.nanosleep(&ts, null);
    }
    for (workers) |worker| try testing.expect(worker.gone());
    try testing.expect(threadCount() <= threads_before);

    const again = try process_state.acquire();
    var id: std.Thread.Id = std.Thread.getCurrentId();
    var task = try again.io.concurrent(recordThread, .{&id});
    task.await(again.io);
    try testing.expect(id != std.Thread.getCurrentId());
    process_state.release(again);
}
