//! What Linux and macOS share of the OS contract (platform.zig), written over libc (`std.c`; libc is linked on every
//! target): the process (the fork lock and handlers, keeping the image loaded, the clock), a connection's end,
//! frames sent and received over a local socket with `SCM_RIGHTS` descriptors, the handshake's waits on the caller's
//! thread (a `poll`, then a call without waiting), the watcher's read, a watched connection's frame, the segment's
//! mapping, and the ring event and slot wakes neither OS needs. platform/linux/linux.zig and platform/macos/macos.zig
//! re-export these and add their own: the rendezvous and peer credentials, the segment's descriptor, the cancel signal
//! and the poller, the futex or `os_sync` waits, and the leak checks' probes.
//!
//! Where the two differ inside one of these functions (a received descriptor's close-on-exec), a `switch` on the OS
//! says how.

const std = @import("std");
const builtin = @import("builtin");
const c = std.c;
const Io = std.Io;
const log = @import("../core/log.zig");
const names = @import("../core/names.zig");
const platform = @import("../platform.zig");
const wire = @import("../core/wire.zig");

const ResourceError = platform.ResourceError;
const UnexpectedError = platform.UnexpectedError;

pub const Handle = c.fd_t;

// === The process ===

/// Nothing to hold: a name is this user's alone (each OS's rendezvous), a peer's user is checked from its credentials
/// (`peer`), and a segment reaches only the peer it is sent to.
pub const Security = struct {
    pub fn init() UnexpectedError!Security {
        return .{};
    }

    pub fn deinit(_: *Security) void {}
};

/// A `pthread_mutex_t`, which fork handlers can take.
pub const Lock = struct {
    raw: c.pthread_mutex_t = .{},

    pub fn lock(mutex: *Lock) void {
        std.debug.assert(c.pthread_mutex_lock(&mutex.raw) == .SUCCESS);
    }

    pub fn unlock(mutex: *Lock) void {
        std.debug.assert(c.pthread_mutex_unlock(&mutex.raw) == .SUCCESS);
    }
};

pub fn registerForkHandlers(prepare: platform.ForkHandler, parent: platform.ForkHandler, child: platform.ForkHandler) bool {
    return c.pthread_atfork(prepare, parent, child) == 0;
}

/// `dladdr` finds the image that holds `anchor`, and `dlopen` of its path with `RTLD_NOLOAD | RTLD_NODELETE` marks it
/// never to be unloaded: nothing is loaded, and the handle is never closed.
pub fn keepImageLoaded(anchor: *const anyopaque) void {
    var info: DlInfo = undefined;
    if (dladdr(anchor, &info) == 0) {
        log.warn(@src(), "dladdr found no image holding the library: it is not kept loaded", .{});
        return;
    }
    if (c.dlopen(info.fname, .{ .NOW = true, .NOLOAD = true, .NODELETE = true }) == null) {
        const why: [*:0]const u8 = c.dlerror() orelse "no reason given";
        log.warn(@src(), "the library can't keep itself loaded (dlopen RTLD_NODELETE: {s})", .{std.mem.span(why)});
    }
}

/// `dladdr` (in glibc itself since 2.34, and in libSystem) and its result, `Dl_info`, laid out alike on both: std
/// declares them on macOS only.
const DlInfo = extern struct {
    fname: ?[*:0]const u8,
    fbase: ?*anyopaque,
    sname: ?[*:0]const u8,
    saddr: ?*anyopaque,
};
extern "c" fn dladdr(address: *const anyopaque, info: *DlInfo) c_int;

pub fn pid() u32 {
    return @intCast(c.getpid());
}

pub fn monotonicNs() u64 {
    var ts: c.timespec = undefined;
    _ = c.clock_gettime(.MONOTONIC, &ts);
    return @as(u64, @intCast(ts.sec)) * std.time.ns_per_s + @as(u64, @intCast(ts.nsec));
}

pub fn writeStderr(bytes: []const u8) void {
    _ = c.write(2, bytes.ptr, bytes.len);
}

/// The std default: 16 MiB of address space, committed on demand.
pub const task_stack_size: usize = std.Thread.SpawnConfig.default_stack_size;

/// Joins the pool (`deinit`), which is enough here. The join returns once every worker has counted itself out, but a
/// worker may still be running the library's last instructions. That is safe only because the library is never
/// unloaded (core/process_state.zig `keepLoaded`; on macOS, dyld also keeps loaded an image that has thread-local
/// variables, as core/log.zig does).
pub fn shutDownIo(threaded: *Io.Threaded) void {
    threaded.deinit();
}

pub fn timespecOfMs(ms: u32) c.timespec {
    return .{ .sec = @intCast(ms / std.time.ms_per_s), .nsec = @intCast(@as(u64, ms % std.time.ms_per_s) * std.time.ns_per_ms) };
}

/// The current thread's errno.
pub fn errnoNow() c.E {
    return @fromBackingInt(@intCast(c._errno().*));
}

/// Logs an errno the protocol doesn't expect, once per change of errno at that place on this thread: a handshake
/// retries most calls every few milliseconds (core/log.zig `warnOnChange`).
pub fn unexpected(comptime src: std.lang.SourceLocation, comptime call: []const u8, e: c.E) UnexpectedError {
    warnUnexpected(src, call, e);
    return error.Unexpected;
}

pub fn warnUnexpected(comptime src: std.lang.SourceLocation, comptime call: []const u8, e: c.E) void {
    log.warnOnChange(src, @backingInt(e), call ++ " failed: errno {d} ({s})", .{ @backingInt(e), std.enums.tagName(c.E, e) orelse "unnamed" });
}

// === Names and connections ===

/// Closes the accepted socket, shut down first when `forked` (closing only the parent's copy would leave the
/// connection up).
pub fn dropClient(connection: Handle, forked: bool) ?Handle {
    if (forked) unblock(connection);
    close(connection);
    return null;
}

/// Shuts both directions of a socket down, which ends a blocked read on it at once. Never on a forked child's copy: it
/// would shut the parent's connection down too.
pub fn unblock(fd: Handle) void {
    _ = c.shutdown(fd, c.SHUT.RDWR);
}

/// `unblock` has ended the watcher's blocking read: POSIX never cancels a task through `Io`, which would send a signal.
pub fn endTask(io: Io, task: *Io.Future(void)) void {
    task.await(io);
}

/// Closes a descriptor (macOS's `close` gives a claimed name up first, then calls this).
pub fn close(fd: Handle) void {
    _ = c.close(fd);
}

/// Makes `fd` close-on-exec, where the call that opened it couldn't (macOS has no `SOCK_CLOEXEC`, `accept4` or
/// `MSG_CMSG_CLOEXEC`).
pub fn setCloseOnExec(fd: Handle) UnexpectedError!void {
    switch (c.errno(c.fcntl(fd, c.F.SETFD, @as(c_int, c.FD_CLOEXEC)))) {
        .SUCCESS => {},
        else => |e| return unexpected(@src(), "fcntl(F_SETFD)", e),
    }
}

/// The `O_NONBLOCK` bit of the file status flags.
pub const o_nonblock: c_int = @bitCast(c.O{ .NONBLOCK = true });

/// Sets or clears `O_NONBLOCK`: a connection's socket blocks, as `Io`'s reads need, once its connect or accept
/// returned.
pub fn setNonblocking(fd: Handle, nonblocking: bool) UnexpectedError!void {
    const flags = c.fcntl(fd, c.F.GETFL);
    if (flags < 0) return unexpected(@src(), "fcntl(F_GETFL)", errnoNow());
    const wanted = if (nonblocking) flags | o_nonblock else flags & ~o_nonblock;
    switch (c.errno(c.fcntl(fd, c.F.SETFL, wanted))) {
        .SUCCESS => {},
        else => |e| return unexpected(@src(), "fcntl(F_SETFL)", e),
    }
}

// === Frames ===

const SendError = error{
    /// The peer is gone (EPIPE, ECONNRESET), or the frame didn't fit its socket buffer at once (EAGAIN, a short
    /// send), which a connection carrying one frame each way never sees: either way the attempt ends.
    BrokenPipe,
} || UnexpectedError;

/// Sends one frame, with `attached` as an `SCM_RIGHTS` descriptor when given: instant.
pub fn sendFrame(connection: Handle, frame: []const u8, attached: ?Handle) bool {
    sendMsg(connection, frame, attached) catch return false;
    return true;
}

/// One `sendmsg`: without blocking (`MSG_DONTWAIT`; a frame fits an empty socket buffer) and without SIGPIPE
/// (`MSG_NOSIGNAL`, and on macOS also `SO_NOSIGPIPE` on every socket of the library), whatever the host's disposition.
pub fn sendMsg(fd: Handle, frame: []const u8, segment: ?Handle) SendError!void {
    const iov = [1]c.iovec_const{.{ .base = frame.ptr, .len = frame.len }};
    var control: Rights(1) = undefined;
    var msg: c.msghdr_const = .{
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
    const rc = c.sendmsg(fd, &msg, c.MSG.NOSIGNAL | c.MSG.DONTWAIT);
    switch (c.errno(rc)) {
        .SUCCESS => if (rc != frame.len) return error.BrokenPipe,
        .PIPE, .CONNRESET, .AGAIN => return error.BrokenPipe,
        else => |e| return unexpected(@src(), "sendmsg", e),
    }
}

/// An `SCM_RIGHTS` control message carrying `n` descriptors, `CMSG_SPACE(n * sizeof(int))` bytes: the header (Linux's
/// 16 bytes, macOS's 12), then the descriptors, padded to the header's alignment.
pub fn Rights(comptime n: usize) type {
    return extern struct {
        header: c.cmsghdr,
        fds: [n]Handle,

        const Self = @This();

        comptime {
            const space = std.mem.alignForward(usize, @sizeOf(c.cmsghdr), c.cmsg_align) + std.mem.alignForward(usize, n * @sizeOf(Handle), c.cmsg_align);
            std.debug.assert(@sizeOf(Self) == space);
        }

        pub fn init(fds: [n]Handle) Self {
            return .{
                .header = .{ .len = @sizeOf(c.cmsghdr) + n * @sizeOf(Handle), .level = c.SOL.SOCKET, .type = c.SCM.RIGHTS },
                .fds = fds,
            };
        }
    };
}

/// The most descriptors `recvSegment` takes from one message: more than the one expected, so that extra ones arrive
/// (and are closed) rather than being dropped as `MSG_CTRUNC`.
const max_received = 4;

/// Linux marks a received descriptor close-on-exec as it arrives (`MSG_CMSG_CLOEXEC`: no fork+exec in the host can
/// inherit it); macOS has no such flag, and `recvSegment` marks it by a second call.
const receive_flags: u32 = switch (builtin.os.tag) {
    .linux => c.MSG.CMSG_CLOEXEC | c.MSG.DONTWAIT,
    else => c.MSG.DONTWAIT,
};

/// Receives the SEGMENT frame into `frame` and its descriptor, without blocking: the caller waited with
/// `readableWithin`, and holds the handle lock, so the descriptor is close-on-exec before any fork (`receive_flags`).
/// Exactly `frame.len` bytes and exactly one descriptor, else every received descriptor is closed and the error says
/// what was wrong (ControlTruncated: the kernel dropped control data, `MSG_CTRUNC`, or, on macOS, couldn't give this
/// process the descriptor, EMFILE).
pub fn recvSegment(fd: Handle, frame: []u8) platform.RecvError!Handle {
    var iov = [1]c.iovec{.{ .base = frame.ptr, .len = frame.len }};
    var control: Rights(max_received) = undefined;
    var msg: c.msghdr = .{
        .name = null,
        .namelen = 0,
        .iov = &iov,
        .iovlen = 1,
        .control = &control,
        .controllen = @sizeOf(Rights(max_received)),
        .flags = 0,
    };
    const rc = c.recvmsg(fd, &msg, receive_flags);
    switch (c.errno(rc)) {
        .SUCCESS => {},
        .CONNRESET => return error.EndOfStream,
        .MFILE, .NFILE => return error.ControlTruncated,
        else => |e| return unexpected(@src(), "recvmsg", e),
    }
    var received: [max_received]Handle = undefined;
    const count = takeDescriptors(std.mem.asBytes(&control)[0..msg.controllen], &received);
    var failure = receiveFailure(@intCast(rc), frame.len, msg.flags & c.MSG.CTRUNC != 0, count);
    if (failure == null and builtin.os.tag == .macos) setCloseOnExec(received[0]) catch |err| {
        failure = err;
    };
    if (failure) |err| {
        for (received[0..count]) |extra| close(extra);
        return err;
    }
    return received[0];
}

/// Copies into `received` the descriptors of every `SCM_RIGHTS` message in `control` (the control data a receive
/// returned) and gives their count: every descriptor that arrived, whatever else is wrong, so the caller can close
/// them. A header that is short or runs past the data ends the walk. `received` has room for every descriptor
/// `control` can hold.
fn takeDescriptors(control: []const u8, received: []Handle) usize {
    const header_size = @sizeOf(c.cmsghdr);
    const descriptor_size = @sizeOf(Handle);
    var count: usize = 0;
    var offset: usize = 0;
    while (offset + header_size <= control.len) {
        const header: *align(1) const c.cmsghdr = @ptrCast(control[offset..].ptr);
        if (header.len < header_size or offset + header.len > control.len) break;
        if (header.level == c.SOL.SOCKET and header.type == c.SCM.RIGHTS) {
            const payload = control[offset + header_size .. offset + header.len];
            var i: usize = 0;
            while (i + descriptor_size <= payload.len) : (i += descriptor_size) {
                received[count] = std.mem.readInt(Handle, payload[i..][0..descriptor_size], .native);
                count += 1;
            }
        }
        offset += std.mem.alignForward(usize, header.len, c.cmsg_align);
    }
    return count;
}

/// What is wrong with a received SEGMENT frame, checked in this order: the connection ended (`received_len` 0), the
/// frame isn't `frame_len` bytes, the kernel dropped control data (`MSG_CTRUNC`), or not exactly one descriptor came.
/// null: nothing.
fn receiveFailure(received_len: usize, frame_len: usize, control_truncated: bool, descriptor_count: usize) ?platform.RecvError {
    if (received_len == 0) return error.EndOfStream;
    if (received_len != frame_len) return error.ShortFrame;
    if (control_truncated) return error.ControlTruncated;
    if (descriptor_count != 1) return error.DescriptorCount;
    return null;
}

/// Reads through `io` into `buf`, blocking until at least one byte arrives. 0: the connection ended, or the read
/// failed (after `unblock` of the socket too).
pub fn read(io: Io, fd: Handle, buf: []u8) platform.ReadError!usize {
    const file: Io.File = .{ .handle = fd, .flags = .{ .nonblocking = false } };
    const result = try io.operate(.{ .file_read_streaming = .{ .file = file, .data = &.{buf} } });
    return platform.readResult(result.file_read_streaming);
}

// === Waits on the caller's thread ===

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
    const rc = c.recvfrom(fd, buf.ptr, buf.len, c.MSG.DONTWAIT, null, null);
    return switch (c.errno(rc)) {
        .SUCCESS => .{ .done = @intCast(rc) },
        .AGAIN, .INTR => .idle,
        else => .{ .done = 0 },
    };
}

const Polled = enum { ready, idle, cancelled };

/// Waits up to `timeout_ms` (null: no limit) until `fd` is readable or has ended, or the cancel signal is set, which
/// wins over the descriptor (Linux's eventfd polls readable once set, macOS's kqueue while it has an event). A signal
/// ends the wait early: `idle`, and the caller waits again.
pub fn pollFor(fd: Handle, cancel: ?Handle, timeout_ms: ?u32) Polled {
    var fds = [2]c.pollfd{
        .{ .fd = fd, .events = c.POLL.IN, .revents = 0 },
        .{ .fd = cancel orelse -1, .events = c.POLL.IN, .revents = 0 }, // a negative descriptor is ignored
    };
    switch (c.errno(c.poll(&fds, fds.len, pollTimeout(timeout_ms)))) {
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

/// A `poll` or `epoll_wait` timeout: `timeout_ms` capped to the largest a `c_int` holds, or -1 (no limit) for null.
pub fn pollTimeout(timeout_ms: ?u32) c_int {
    const ms = timeout_ms orelse return -1;
    return @intCast(@min(ms, std.math.maxInt(c_int)));
}

/// Whether the cancel signal is set, waiting up to `timeout_ms` for it: both OSes' signals poll readable once set.
pub fn waitCancelSignal(signal: Handle, timeout_ms: u32) bool {
    var fds = [1]c.pollfd{.{ .fd = signal, .events = c.POLL.IN, .revents = 0 }};
    return c.poll(&fds, 1, pollTimeout(timeout_ms)) > 0 and fds[0].revents != 0;
}

pub fn sleep(ms: u32) void {
    const ts = timespecOfMs(ms);
    _ = c.nanosleep(&ts, null);
}

// === The poller ===

/// A connection a poller watches (platform.zig `Watched`): its frame, read without waiting whenever the poller reports
/// the connection readable, which it is too at its end.
pub const Watched = struct {
    connection: Handle,
    frame: wire.Frame = undefined,
    got: usize = 0,

    /// Receives the rest of the frame without waiting (`Poller.take`): a receive of 0 bytes is the end of the stream,
    /// and the other side's reset (its end, with data left unread) is its end too.
    pub fn take(watched: *Watched) platform.Taken {
        const rest = watched.frame[watched.got..];
        const rc = c.recvfrom(watched.connection, rest.ptr, rest.len, c.MSG.DONTWAIT, null, null);
        switch (c.errno(rc)) {
            .SUCCESS => {},
            .AGAIN, .INTR => return .partial,
            .CONNRESET => return .ended,
            else => return .failed,
        }
        if (rc == 0) return .ended;
        watched.got += @intCast(rc);
        return if (watched.got == watched.frame.len) .whole else .partial;
    }
};

// === The segment ===

const page_align = std.heap.page_size_min;

/// A mapped segment. Its ring sleepers need no event: the sleep flag is the waited-on word itself.
pub const Mapping = struct {
    base: [*]align(page_align) u8,
    size: usize,

    pub fn unmap(mapping: *Mapping) void {
        _ = c.munmap(mapping.base, mapping.size);
        mapping.* = undefined;
    }

    pub fn ringEvent(_: *const Mapping, _: names.Object) RingEvent {}
};

/// Maps the segment's descriptor shared, through libc, like `Mapping.unmap`, so that ThreadSanitizer sees the
/// mapping: a new segment often gets the address of one unmapped a moment before, and the kernel's ordering of the two
/// is invisible to it otherwise.
pub fn map(fd: Handle, size: usize) (error{OutOfMemory} || UnexpectedError)!Mapping {
    const base = c.mmap(null, size, .{ .READ = true, .WRITE = true }, .{ .TYPE = .SHARED }, fd, 0);
    if (base == c.MAP_FAILED) return switch (errnoNow()) {
        .NOMEM => error.OutOfMemory,
        else => |e| unexpected(@src(), "mmap", e),
    };
    return .{ .base = @ptrCast(@alignCast(base)), .size = size };
}

/// `mprotect(PROT_READ)` of the first `len` bytes. ENOMEM: the kernel couldn't split the mapping (on Linux, the split
/// would pass the process's limit of mappings).
pub fn protectReadOnly(mapping: *Mapping, len: usize) (ResourceError || UnexpectedError)!void {
    if (c.mprotect(@ptrCast(mapping.base), len, .{ .READ = true }) == 0) return;
    const e = errnoNow();
    return if (e == .NOMEM) error.SystemResources else unexpected(@src(), "mprotect", e);
}

/// `mprotect(PROT_NONE)` of the whole mapping. It can't fail on a mapping of this process's own.
pub fn revoke(mapping: *Mapping) void {
    if (c.mprotect(@ptrCast(mapping.base), mapping.size, .{}) != 0) warnUnexpected(@src(), "mprotect", errnoNow());
}

// === Ring waits (stream/ring_wait.zig) ===

/// None: the sleep flag is the waited-on word itself.
pub const RingEvent = void;
pub const no_ring_event: RingEvent = {};

// === Broadcast waits ===

/// The writer wakes every sleeper at once on the shared word.
pub const wakes_each_slot = false;

/// None: a subscriber's interrupt bumps the wait word and wakes every sleeper, which each look again.
pub const SlotWake = void;

pub fn createSlotWake(_: *const [16]u8, _: []const u8, _: u32, _: *const Security) (ResourceError || UnexpectedError)!SlotWake {}

pub fn openSlotWake(_: *const [16]u8, _: []const u8, _: u32) platform.OpenError!SlotWake {}

/// The flag is the waited-on word itself.
pub fn slotRingEvent(_: ?SlotWake) RingEvent {
    return no_ring_event;
}

pub fn closeSlotWake(_: SlotWake) void {}

pub fn setSlotWake(_: SlotWake) void {}

// === For the tests' leak checks ===

/// The open descriptors of this process (0 to 1023).
pub fn openHandleCount() usize {
    var count: usize = 0;
    for (0..1024) |fd| {
        if (c.fcntl(@intCast(fd), c.F.GETFD) != -1) count += 1;
    }
    return count;
}
