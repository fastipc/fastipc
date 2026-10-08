//! macOS's side of the OS contract (platform.zig), on Apple Silicon and macOS 14.4 or later: the rendezvous (a socket
//! file in the user's private directory, claimed with a lock file held by `flock`; claim, connect, accept), peer
//! credentials (`getpeereid`), frames sent without SIGPIPE (`SO_NOSIGPIPE`), the POSIX shared memory segment passed by
//! `SCM_RIGHTS`, the cancel signal (a kqueue with a user event), and the ring waits' `os_sync` calls. Every function is
//! an instant system call, except the handshake's waits on the caller's thread (`acceptWithin`, `readWithin`,
//! `readableWithin`: a `poll` with a timeout, then a non-blocking call), the watcher's `read`, which blocks through
//! `Io`, and `ringWait`, the one wait `Io` can't express (an address shared with the peer; data/ring_wait.zig slices
//! it). Everything goes through libSystem (std.c, and darwin.zig for what std lacks).
//!
//! Descriptors: as on Linux, every one the library owns is opened and registered, unregistered and closed under the
//! handle lock (process_state.zig), which the callers of `claim`, `connect`, `createCancelSignal`,
//! `createSegmentHandle`, `recvSegment` and `close` hold. macOS has no `SOCK_CLOEXEC`, `accept4` or `MSG_CMSG_CLOEXEC`,
//! so a new socket or received descriptor is made close-on-exec by a second call, under that lock, so no fork comes
//! between. `acceptWithin` waits, so it runs without the lock; the fork counter covers its new socket, as on Linux. A
//! `posix_spawn` on another thread between `accept` and its `fcntl` could still inherit that socket: macOS has no call
//! that does both at once.
//!
//! The rendezvous (docs/protocol.md §2, §3.2): a name is the socket file `<dir>fastipc/<N>` (names.macosRendezvous), in
//! the user's directory from `confstr(_CS_DARWIN_USER_DIR)`, and its lock file `<N>.lock` beside it. The `fastipc`
//! directory is the user's alone (mode 0700), so no other user can reach the files; `peer` checks the user as well. A
//! listener holds the name by an `flock` on the lock file, which belongs to its open file description: two opens
//! exclude each other, also two threads of one process, and the lock goes with the last descriptor, also when the
//! process dies. Holding it, the listener removes a socket file a crashed listener left, binds and listens; it removes
//! both files when it closes, so the name is free again at once. A crash leaves the two files, which the name's next
//! listener clears. Deleting the lock file by hand while a listener runs lets a second listener claim the name.

const std = @import("std");
const builtin = @import("builtin");
const c = std.c;
const darwin = @import("darwin.zig");
const Io = std.Io;
const log = @import("../log.zig");
const names = @import("../lifecycle/names.zig");
const platform = @import("../platform.zig");
const process_state = @import("../process_state.zig");

const ResourceError = platform.ResourceError;
const UnexpectedError = platform.UnexpectedError;

pub const has_fork = true;
pub const passes_segment_handle = true;

pub const Handle = c.fd_t;

// === The process ===

/// Nothing to hold: the rendezvous directory is the user's alone, a peer's user is checked from its credentials
/// (`peer`), and a segment reaches only the peer it is sent to (its name is removed at once).
pub const Security = struct {
    pub fn init() UnexpectedError!Security {
        return .{};
    }

    pub fn deinit(_: *Security) void {}
};

/// A `pthread_mutex_t`, which fork handlers can take.
pub const Lock = struct {
    raw: c.pthread_mutex_t = .{},

    pub fn lock(l: *Lock) void {
        std.debug.assert(c.pthread_mutex_lock(&l.raw) == .SUCCESS);
    }

    pub fn unlock(l: *Lock) void {
        std.debug.assert(c.pthread_mutex_unlock(&l.raw) == .SUCCESS);
    }
};

pub fn pid() u32 {
    return @intCast(c.getpid());
}

pub fn crash(code: u8) noreturn {
    c._exit(code);
}

pub fn writeStderr(bytes: []const u8) void {
    _ = c.write(2, bytes.ptr, bytes.len);
}

/// The std default: 16 MiB of address space, committed on demand.
pub const task_stack_size: usize = std.Thread.SpawnConfig.default_stack_size;

/// The join is enough: the library is never unloaded (docs/platform-support.md), and dyld keeps an image with
/// thread-local variables (log.zig) loaded anyway.
pub fn shutDownIo(threaded: *Io.Threaded) void {
    threaded.deinit();
}

// === The rendezvous ===

/// A path socket address: `sun_path` holds the socket's absolute path (names.macosRendezvous) and a NUL.
pub const Address = struct {
    sockaddr: c.sockaddr.un,
    /// The length of the path's directory, `<dir>fastipc/`, before the socket's file name.
    dir_len: u8,

    /// The socket's path.
    fn path(address: *const Address) [*:0]const u8 {
        return @ptrCast(&address.sockaddr.path);
    }

    /// The socket's file name in the rendezvous directory.
    fn file(address: *const Address) [:0]const u8 {
        const all = std.mem.sliceTo(@as([*:0]const u8, @ptrCast(&address.sockaddr.path)), 0);
        return all[address.dir_len..];
    }
};

/// The name's socket path in this user's directory.
pub fn addressOf(name: []const u8) UnexpectedError!Address {
    var dir_buf: [c.PATH_MAX]u8 = undefined;
    const dir = try userDir(&dir_buf);
    var buf: [names.macos_max]u8 = undefined;
    const socket_path = names.macosRendezvous(&buf, dir, name) orelse {
        log.warn(@src(), "the user's directory {s} is too long for a socket path (104 bytes)", .{dir});
        return error.Unexpected;
    };
    var address: Address = .{ .sockaddr = .{ .path = @splat(0) }, .dir_len = @intCast(dir.len + names.macos_dir.len) };
    @memcpy(address.sockaddr.path[0..socket_path.len], socket_path);
    return address;
}

/// The user's private directory, `/var/folders/<xx>/<id>/0/`, ending with `/`.
fn userDir(buf: *[c.PATH_MAX]u8) UnexpectedError![]const u8 {
    const len = user_dir_len.load(.acquire);
    if (len != 0) {
        @memcpy(buf[0..len], user_dir[0..len]);
        return buf[0..len];
    }
    const dir = try lookUpUserDir(buf);
    if (!user_dir_claimed.swap(true, .acquire)) {
        @memcpy(user_dir[0..dir.len], dir);
        user_dir_len.store(dir.len, .release);
    }
    return dir;
}

/// The user's directory, kept by the first lookup (`userDir`) that finishes. A process's first
/// `confstr(_CS_DARWIN_USER_DIR)` sets libSystem's state up in a `dispatch_once`, and a fork in the middle of it leaves a
/// child whose own lookup crashes; the library makes that first lookup before its fork handlers exist
/// (`prepareForForks`), so a child forked after the process's first fipc call inherits the result. No lock: a lock
/// held across a fork would hang the child, and the fork handlers can't hold one across the lookup, which registers
/// libSystem's own fork handlers and so waits for a fork in progress.
var user_dir: [c.PATH_MAX]u8 = undefined;
var user_dir_len = std.atomic.Value(usize).init(0);
var user_dir_claimed = std.atomic.Value(bool).init(false);

/// Looks the user's directory up now (`userDir`), once, before the fork handlers are registered.
pub fn prepareForForks() void {
    var buf: [c.PATH_MAX]u8 = undefined;
    _ = userDir(&buf) catch {};
}

fn lookUpUserDir(buf: *[c.PATH_MAX]u8) UnexpectedError![]const u8 {
    const len = darwin.confstr(darwin._CS_DARWIN_USER_DIR, buf, buf.len);
    if (len == 0) return unexpected(@src(), "confstr(_CS_DARWIN_USER_DIR)", errnoNow());
    if (len > buf.len) return unexpected(@src(), "confstr(_CS_DARWIN_USER_DIR)", .NAMETOOLONG);
    const dir = buf[0 .. len - 1];
    if (dir.len > 0 and dir[dir.len - 1] == '/') return dir;
    buf[dir.len] = '/';
    return buf[0 .. dir.len + 1];
}

/// A claimed name: its listening socket, the lock file's descriptor, which holds the lock, the process that claimed it,
/// and the socket's path. Listed by `claim`, taken off the list by `close`.
const Claim = struct {
    next: ?*Claim,
    listening: Handle,
    lock: Handle,
    owner: c.pid_t,
    /// The socket's path, NUL-terminated; the lock file's is the same with `.lock`.
    path: [names.macos_max + 1]u8,
};

/// The claims of this process: in practice changed under the handle lock (`claim`, `close`), and guarded by a lock of
/// their own all the same, which nothing holds while it waits.
var claims: ?*Claim = null;
var claims_lock: Lock = .{};

/// How many times `claim` opens the lock file again when its holder removed it between the open and the `flock`, before
/// it calls the name held: a holder that let go has removed it, so a second try normally finds a new file.
const lock_attempts = 16;

/// How long `claim` waits while only shared holders (a sweep, `sweep`) hold the name's lock, before it calls the name
/// held: a sweep holds it for a few system calls, so only a sweeping process that is stopped (SIGSTOP, a debugger)
/// makes the wait run out, as a claimant of the name stopped mid-claim makes another claim fail at once.
const shared_wait_ms = 1000;

/// Claims the name: holds its lock file (`flock`; held elsewhere: null), removes a stale socket file, then a new
/// non-blocking socket (`acceptWithin` polls, then accepts without waiting), `bind`, `listen(4)`. Then clears what
/// crashed listeners of other names left (`sweep`), whose failures are only logged.
pub fn claim(address: *const Address, _: *const Security) (ResourceError || UnexpectedError)!?Handle {
    const listening = (try claimName(address)) orelse return null;
    sweep(address);
    return listening;
}

fn claimName(address: *const Address) (ResourceError || UnexpectedError)!?Handle {
    try checkDir(address);
    var lock_buf: LockPath = undefined;
    const lock_path = lockPathOf(address.path(), &lock_buf);
    const lock = (try holdLock(lock_path)) orelse return null;
    // From here the name is this call's: whatever fails gives it up again
    var listening: ?Handle = null;
    errdefer {
        if (listening) |fd| _ = c.close(fd);
        _ = c.unlink(lock_path);
        _ = c.close(lock);
    }
    const node = std.heap.c_allocator.create(Claim) catch return error.SystemResources;
    errdefer std.heap.c_allocator.destroy(node);
    // A socket file that a crashed listener left: no live listener holds it, since the lock was free
    switch (c.errno(c.unlink(address.path()))) {
        .SUCCESS, .NOENT => {},
        else => |e| return unexpected(@src(), "unlink(socket)", e),
    }
    listening = try socket();
    switch (c.errno(c.bind(listening.?, @ptrCast(&address.sockaddr), @sizeOf(c.sockaddr.un)))) {
        .SUCCESS => {},
        .NOMEM, .NOBUFS => return error.SystemResources,
        else => |e| return unexpected(@src(), "bind", e),
    }
    switch (c.errno(c.listen(listening.?, 4))) {
        .SUCCESS => {},
        else => |e| return unexpected(@src(), "listen", e),
    }
    node.* = .{ .next = null, .listening = listening.?, .lock = lock, .owner = c.getpid(), .path = address.sockaddr.path };
    claims_lock.lock();
    defer claims_lock.unlock();
    node.next = claims;
    claims = node;
    return listening.?;
}

/// The files of a name go by their absolute paths: a `*at` call relative to a descriptor of the directory fails now and
/// then with ENOENT while another thread creates the same file (`openat` with `O_CREAT`, macOS 26), which `open` by
/// path doesn't.
const LockPath = [names.macos_max + ".lock".len + 1]u8;

/// The lock file's path: the socket's and `.lock`.
fn lockPathOf(socket_path: [*:0]const u8, buf: *LockPath) [:0]const u8 {
    return std.mem.printSentinel(buf, "{s}.lock", .{std.mem.span(socket_path)}, 0) catch unreachable;
}

/// Makes sure of the rendezvous directory `<dir>fastipc`, created if missing: a directory of this user's that no one
/// else may enter (mode 0700), else Unexpected.
fn checkDir(address: *const Address) (ResourceError || UnexpectedError)!void {
    var buf: [names.macos_max + 1]u8 = undefined;
    const dir_path = std.mem.printSentinel(&buf, "{s}", .{address.sockaddr.path[0 .. address.dir_len - 1]}, 0) catch unreachable;
    switch (c.errno(c.mkdirat(c.AT.FDCWD, dir_path, 0o700))) {
        .SUCCESS, .EXIST => {},
        .NOSPC, .DQUOT => return error.SystemResources,
        else => |e| return unexpected(@src(), "mkdir(rendezvous directory)", e),
    }
    const fd = c.openat(c.AT.FDCWD, dir_path, .{ .ACCMODE = .RDONLY, .DIRECTORY = true, .NOFOLLOW = true, .CLOEXEC = true });
    if (fd < 0) return switch (errnoNow()) {
        .MFILE, .NFILE, .NOMEM => error.SystemResources,
        else => |e| unexpected(@src(), "open(rendezvous directory)", e),
    };
    defer _ = c.close(fd);
    var stat: c.Stat = undefined;
    if (c.fstat(fd, &stat) != 0) return unexpected(@src(), "fstat(rendezvous directory)", errnoNow());
    if (stat.uid != c.geteuid() or stat.mode & 0o077 != 0) {
        log.warn(@src(), "{s} must be this user's and mode 0700 (it is uid {d}, mode {o}): listening fails", .{ dir_path, stat.uid, stat.mode & 0o7777 });
        return error.Unexpected;
    }
}

/// Opens the lock file `path` and takes its `flock` without waiting: its descriptor, which holds the lock until it is
/// closed. Null when another open file holds it exclusively (a listener, or a claim in progress). A holder that lets go
/// removes the file first, so a lock taken on a file no longer under its name is let go, and the file of the name
/// opened again. A file held only shared is a sweep removing a crashed listener's files (`sweep`): the claim waits for
/// it (`shared_wait_ms`), then finds a new file.
fn holdLock(path: [*:0]const u8) (ResourceError || UnexpectedError)!?Handle {
    var attempts: usize = 0;
    const start_ms = nowMs();
    while (attempts < lock_attempts) {
        const fd = c.open(path, .{ .ACCMODE = .RDWR, .CREAT = true, .NOFOLLOW = true, .CLOEXEC = true }, @as(c_uint, 0o600));
        if (fd < 0) return switch (errnoNow()) {
            .MFILE, .NFILE, .NOMEM => error.SystemResources,
            .NOSPC, .DQUOT => error.SystemResources,
            else => |e| unexpected(@src(), "open(lock)", e),
        };
        switch (try lockFile(fd, std.posix.LOCK.EX)) {
            .held => {},
            .busy => switch (try lockFile(fd, std.posix.LOCK.SH)) {
                // Held exclusively: in use
                .busy => {
                    _ = c.close(fd);
                    return null;
                },
                // Held shared only: a sweep, which lets go at once
                .held => {
                    _ = c.close(fd);
                    const waited_ms = nowMs() - start_ms;
                    if (waited_ms >= shared_wait_ms) {
                        log.warn(@src(), "{s} has been held shared for {d} ms (a process stopped while it removes the files of a crashed listener): the name counts as in use", .{ path, waited_ms });
                        return null;
                    }
                    if (builtin.is_test) _ = test_shared_waits.fetchAdd(1, .monotonic);
                    sleep(1);
                    continue;
                },
            },
        }
        const named = namesFile(path, fd, null) catch |err| {
            _ = c.close(fd);
            return err;
        };
        if (named) return fd;
        _ = c.close(fd);
        attempts += 1;
    }
    return null;
}

const Locked = enum { held, busy };

/// Takes an `flock` (`LOCK_EX` or `LOCK_SH`) on `fd` without waiting.
fn lockFile(fd: Handle, operation: c_int) UnexpectedError!Locked {
    while (true) switch (c.errno(c.flock(fd, operation | std.posix.LOCK.NB))) {
        .SUCCESS => return .held,
        .INTR => {},
        .AGAIN => return .busy,
        else => |e| return unexpected(@src(), "flock", e),
    };
}

/// Whether `path` still names the file open as `fd` (a symbolic link is not followed); its `fstat` in `stat` if given.
fn namesFile(path: [*:0]const u8, fd: Handle, stat: ?*c.Stat) UnexpectedError!bool {
    var held: c.Stat = undefined;
    var named: c.Stat = undefined;
    if (c.fstat(fd, &held) != 0) return unexpected(@src(), "fstat(lock)", errnoNow());
    if (stat) |s| s.* = held;
    return switch (c.errno(c.fstatat(c.AT.FDCWD, path, &named, c.AT.SYMLINK_NOFOLLOW))) {
        .SUCCESS => named.dev == held.dev and named.ino == held.ino,
        .NOENT => false,
        else => |e| unexpected(@src(), "fstatat(lock)", e),
    };
}

/// How many times a claim waited for a shared holder (the tests).
var test_shared_waits: std.atomic.Value(u32) = .init(0);

// The sweep: what crashed listeners left
//
// A crashed listener leaves its socket file and lock file, which the name's next claim clears; a name never listened on
// again would keep them, so every claim also removes, in the rendezvous directory, the files of every name that no
// listener holds. The rules that make it safe, whatever runs at the same time:
//
// - One sweep at a time per user: a sweep holds an exclusive `flock` on the directory itself (held: another sweep runs,
//   and this claim waits for it up to `sweep_wait_ms`, then skips its own). Only sweeps lock the directory.
// - A sweep takes a name's lock shared, never exclusively, and without waiting. A listener holds it exclusively from
//   its claim until its close, also while stopped (SIGSTOP) and through a forked child's copy, so a sweep never gets a
//   live or suspended listener's lock, nor one of a claim in progress past its `flock`. Holding it shared, with the
//   name still naming that file (the claim's own re-check), no exclusive holder can come for that file, and the name
//   can't name another file until the sweep removes it: so no listener of the name exists, the socket file is stale,
//   and the sweep removes the socket file, then the lock file, then lets go, like a closing listener.
// - A claimant of the name that finds the lock held: held exclusively, the name is in use, as without sweeps; held
//   shared only, it is a sweep (or another claimant's look), so the claim waits for it and then finds the file gone
//   and makes a new one (`holdLock`). Only a sweeping process stopped within those few calls makes a claim wait longer;
//   past `shared_wait_ms` the name counts as in use, as when a claimant of it is stopped mid-claim. Path-based
//   removal can't avoid that wait: a claim that went ahead would have its new files removed when the sweep resumed.
// - Two sweeps never hold one file at once (the directory lock), so none removes a file another made after a
//   sweep's re-check. A claim that opened a lock file a sweep then removed fails its re-check and opens the name
//   again, as after a closing listener.
// - A sweep leaves lock files younger than `sweep_min_age_s` (by modification time, which no one changes after the
//   file is made): a claim's new file is never removed between its open and its `flock`, and a crash's files stay
//   for a moment. The age only ever spares files: safety rests on the locks.
// - A client connecting meanwhile sees the same as without the sweep: a stale socket file refuses it, a removed one is
//   missing; both are no listener.
// - The sweep runs inside `claim`, under the handle lock, so no fork copies its descriptors (close-on-exec besides),
//   and only in the process that listens: a forked child's close of an inherited listener only closes descriptors.
// - Its cost is bounded (`sweep_scan_max` entries read, `sweep_remove_max` names removed), and its failures are
//   logged, never the claim's.

/// A sweep reads at most this many directory entries, and removes at most this many names' files.
const sweep_scan_max = 4096;
const sweep_remove_max = 256;
/// A sweep leaves the files of a name whose lock file was made less than this many seconds ago.
const sweep_min_age_s = 2;
/// How long a claim waits for another sweep to end before it skips its own.
const sweep_wait_ms = 20;

/// Removes the files of the names in the rendezvous directory that no listener holds (the rules above).
fn sweep(address: *const Address) void {
    var dir_buf: [names.macos_max + 1]u8 = undefined;
    const dir_path = std.mem.printSentinel(&dir_buf, "{s}", .{address.sockaddr.path[0..address.dir_len]}, 0) catch unreachable;
    const fd = c.open(dir_path, .{ .ACCMODE = .RDONLY, .DIRECTORY = true, .NOFOLLOW = true, .CLOEXEC = true });
    if (fd < 0) return warnUnexpected(@src(), "open(rendezvous directory)", errnoNow());
    // Owns the descriptor from here: closing it lets the directory's lock go
    const dir = c.fdopendir(fd) orelse {
        warnUnexpected(@src(), "fdopendir", errnoNow());
        _ = c.close(fd);
        return;
    };
    defer _ = c.closedir(dir);
    // Another sweep runs: wait for it a moment (a sweep takes a few milliseconds), else leave the files to the next one
    var waited_ms: u32 = 0;
    while (lockFile(fd, std.posix.LOCK.EX) catch return == .busy) : (waited_ms += 1) {
        if (waited_ms == sweep_wait_ms) return;
        sleep(1);
    }
    var now: c.timespec = undefined;
    if (c.clock_gettime(.REALTIME, &now) != 0) return;
    var removed: usize = 0;
    for (0..sweep_scan_max) |_| {
        if (removed == sweep_remove_max) break;
        const entry = c.readdir(dir) orelse break;
        const file = entry.name[0..entry.namlen];
        if (!std.mem.endsWith(u8, file, ".lock") or file.len == ".lock".len) continue;
        var socket_buf: [names.macos_max + 1]u8 = undefined;
        const socket_path = std.mem.printSentinel(&socket_buf, "{s}{s}", .{ dir_path, file[0 .. file.len - ".lock".len] }, 0) catch continue;
        if (removeStale(socket_path, now)) removed += 1;
    }
}

/// Removes the name's socket file and lock file if no listener holds the lock and the lock file is old enough.
fn removeStale(socket_path: [*:0]const u8, now: c.timespec) bool {
    var lock_buf: LockPath = undefined;
    const lock_path = lockPathOf(socket_path, &lock_buf);
    const fd = c.open(lock_path, .{ .ACCMODE = .RDWR, .NOFOLLOW = true, .CLOEXEC = true });
    if (fd < 0) return false; // gone, or not a lock file of ours
    defer _ = c.close(fd);
    switch (lockFile(fd, std.posix.LOCK.SH) catch return false) {
        .held => {},
        .busy => return false, // a listener, or a claim in progress
    }
    var stat: c.Stat = undefined;
    if (!(namesFile(lock_path, fd, &stat) catch return false)) return false;
    if (now.sec - stat.mtimespec.sec < sweep_min_age_s) return false;
    switch (c.errno(c.unlink(socket_path))) {
        .SUCCESS, .NOENT => {},
        else => |e| {
            warnUnexpected(@src(), "unlink(stale socket)", e);
            return false;
        },
    }
    switch (c.errno(c.unlink(lock_path))) {
        .SUCCESS, .NOENT => {},
        else => |e| warnUnexpected(@src(), "unlink(stale lock)", e),
    }
    return true;
}

/// Gives a claim up: in the process that claimed the name, removes the socket file and the lock file while it still
/// holds the lock, then lets the lock go (the name is free at once); in a forked child (process_state.zig's child
/// handler) closes only its copy of the lock file's descriptor, for the parent still holds the name, and leaves the
/// entry allocated (no allocator call in a fork handler).
fn release(node: *Claim) void {
    if (node.owner == c.getpid()) {
        const socket_path: [*:0]const u8 = @ptrCast(&node.path);
        var lock_buf: LockPath = undefined;
        _ = c.unlink(socket_path);
        _ = c.unlink(lockPathOf(socket_path, &lock_buf));
        _ = c.close(node.lock);
        std.heap.c_allocator.destroy(node);
    } else {
        _ = c.close(node.lock);
    }
}

/// Takes the claim of listening socket `fd` off the list, if it is one.
fn takeClaim(fd: Handle) ?*Claim {
    claims_lock.lock();
    defer claims_lock.unlock();
    var link = &claims;
    while (link.*) |node| : (link = &node.next) {
        if (node.listening == fd) {
            link.* = node.next;
            return node;
        }
    }
    return null;
}

/// The listening socket stays: none.
pub fn nextListening(_: *const Address, _: *const Security) (ResourceError || UnexpectedError)!?Handle {
    return null;
}

/// Connects to the name without blocking (a new non-blocking socket, `connect`), then puts the socket in blocking
/// mode, as `Io`'s reads need. `no_listener`: no socket file (ENOENT), or a file nobody listens on: a crashed
/// listener's, a holder's between `bind` and `listen`, or a listener whose backlog is full (ECONNREFUSED; macOS doesn't
/// tell that apart). `busy`: EAGAIN. Never `foreign`: `peer` tells the user.
pub fn connect(address: *const Address, _: *const Security) (ResourceError || UnexpectedError)!platform.Connect {
    const fd = try socket();
    errdefer _ = c.close(fd);
    const outcome: platform.Connect = switch (c.errno(c.connect(fd, @ptrCast(&address.sockaddr), @sizeOf(c.sockaddr.un)))) {
        .SUCCESS => {
            try setNonblocking(fd, false);
            return .{ .connected = fd };
        },
        .CONNREFUSED, .NOENT => .no_listener,
        .AGAIN => .busy,
        .NOMEM, .NOBUFS => return error.SystemResources,
        else => |e| return unexpected(@src(), "connect", e),
    };
    _ = c.close(fd);
    return outcome;
}

/// Polls the listening socket and the cancel signal, then accepts without waiting. The new socket inherits the
/// listening socket's `O_NONBLOCK` and `SO_NOSIGPIPE` on macOS: it is made blocking, as the watcher's `Io` reads need,
/// and close-on-exec. A client that left between the poll and the accept (EAGAIN) is nothing; one aborted in the
/// backlog (ECONNABORTED) is `closing`; descriptor exhaustion is `transient`.
pub fn acceptWithin(listening: Handle, cancel: ?Handle, timeout_ms: ?u32, forks: ?*u32) platform.Waited(platform.Accept) {
    switch (pollFor(listening, cancel, timeout_ms)) {
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

/// Closes the accepted socket, shut down first when `forked` (closing only the parent's copy would leave the
/// connection up).
pub fn dropClient(connection: Handle, forked: bool) ?Handle {
    if (forked) unblock(connection);
    close(connection);
    return null;
}

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

/// Shuts both directions of a socket down, which ends a blocked read on it at once. Never on a forked child's copy: it
/// would shut the parent's connection down too.
pub fn unblock(fd: Handle) void {
    _ = c.shutdown(fd, c.SHUT.RDWR);
}

/// `unblock` has ended the watcher's blocking read: POSIX never cancels a task through `Io`, which would send a signal.
pub fn endTask(io: Io, task: *Io.Future(void)) void {
    task.await(io);
}

/// Closes a descriptor; a listening socket's claim is given up first (`release`).
pub fn close(fd: Handle) void {
    if (takeClaim(fd)) |node| release(node);
    _ = c.close(fd);
}

/// A new local stream socket: close-on-exec, non-blocking, and with `SO_NOSIGPIPE`, so that no send on it (or on a
/// socket accepted from it, which inherits it) raises SIGPIPE whatever the host's disposition.
fn socket() (ResourceError || UnexpectedError)!Handle {
    const fd = c.socket(c.AF.UNIX, c.SOCK.STREAM, 0);
    if (fd < 0) return switch (errnoNow()) {
        .MFILE, .NFILE, .NOBUFS, .NOMEM => error.SystemResources,
        else => |e| unexpected(@src(), "socket", e),
    };
    errdefer _ = c.close(fd);
    try setCloseOnExec(fd);
    try setNonblocking(fd, true);
    const on: c_int = 1;
    switch (c.errno(c.setsockopt(fd, c.SOL.SOCKET, c.SO.NOSIGPIPE, &on, @sizeOf(c_int)))) {
        .SUCCESS => {},
        else => |e| return unexpected(@src(), "setsockopt(SO_NOSIGPIPE)", e),
    }
    return fd;
}

/// Makes an accepted socket close-on-exec and blocking.
fn prepareAccepted(fd: Handle) UnexpectedError!void {
    try setCloseOnExec(fd);
    try setNonblocking(fd, false);
}

fn setCloseOnExec(fd: Handle) UnexpectedError!void {
    switch (c.errno(c.fcntl(fd, c.F.SETFD, @as(c_int, c.FD_CLOEXEC)))) {
        .SUCCESS => {},
        else => |e| return unexpected(@src(), "fcntl(F_SETFD)", e),
    }
}

const o_nonblock: c_int = @bitCast(c.O{ .NONBLOCK = true });

fn setNonblocking(fd: Handle, nonblocking: bool) UnexpectedError!void {
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

/// Sends one frame, with `attached` as an `SCM_RIGHTS` descriptor when given: instant, without `io`.
pub fn sendFrame(_: Io, connection: Handle, frame: []const u8, attached: ?Handle) Io.Cancelable!bool {
    sendMsg(connection, frame, attached) catch return false;
    return true;
}

/// One `sendmsg`: without blocking (`MSG_DONTWAIT`; a frame fits an empty socket buffer) and without SIGPIPE
/// (`SO_NOSIGPIPE` on every socket of the library, and `MSG_NOSIGNAL`), whatever the host's disposition.
fn sendMsg(fd: Handle, frame: []const u8, segment: ?Handle) SendError!void {
    const iov = [1]std.posix.iovec_const{.{ .base = frame.ptr, .len = frame.len }};
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

/// An `SCM_RIGHTS` control message carrying `n` descriptors: macOS's 12-byte header, aligned to 4 bytes, so the
/// descriptors follow it at once (`CMSG_SPACE(n * sizeof(int))` bytes).
fn Rights(comptime n: usize) type {
    return extern struct {
        header: c.cmsghdr,
        fds: [n]Handle,

        const Self = @This();

        comptime {
            std.debug.assert(@sizeOf(Self) == std.mem.alignForward(usize, @sizeOf(c.cmsghdr), c.cmsg_align) + n * @sizeOf(Handle));
        }

        fn init(fds: [n]Handle) Self {
            return .{
                .header = .{ .len = @sizeOf(c.cmsghdr) + n * @sizeOf(Handle), .level = c.SOL.SOCKET, .type = c.SCM.RIGHTS },
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
    const rc = c.recvfrom(fd, buf.ptr, buf.len, c.MSG.DONTWAIT, null, null);
    return switch (c.errno(rc)) {
        .SUCCESS => .{ .done = @intCast(rc) },
        .AGAIN, .INTR => .idle,
        else => .{ .done = 0 },
    };
}

const Polled = enum { ready, idle, cancelled };

/// Waits up to `timeout_ms` (null: no limit) until `fd` is readable or has ended, or the cancel signal is set, which
/// wins over the descriptor (a kqueue polls readable while it has an event). A signal ends the wait early: `idle`, and
/// the caller waits again.
fn pollFor(fd: Handle, cancel: ?Handle, timeout_ms: ?u32) Polled {
    var fds = [2]c.pollfd{
        .{ .fd = fd, .events = c.POLL.IN, .revents = 0 },
        .{ .fd = cancel orelse -1, .events = c.POLL.IN, .revents = 0 }, // a negative descriptor is ignored
    };
    const timeout: c_int = if (timeout_ms) |ms| @intCast(@min(ms, std.math.maxInt(c_int))) else -1;
    switch (c.errno(c.poll(&fds, fds.len, timeout))) {
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
    try setCloseOnExec(kq);
    const add = [1]c.Kevent{.{ .ident = cancel_event, .filter = c.EVFILT.USER, .flags = c.EV.ADD, .fflags = 0, .data = 0, .udata = 0 }};
    var none: [0]c.Kevent = .{};
    switch (c.errno(c.kevent(kq, &add, 1, &none, 0, null))) {
        .SUCCESS => return kq,
        .NOMEM => return error.SystemResources,
        else => |e| return unexpected(@src(), "kevent(EV_ADD)", e),
    }
}

pub fn setCancelSignal(kq: Handle) void {
    const trigger = [1]c.Kevent{.{ .ident = cancel_event, .filter = c.EVFILT.USER, .flags = 0, .fflags = c.NOTE.TRIGGER, .data = 0, .udata = 0 }};
    var none: [0]c.Kevent = .{};
    _ = c.kevent(kq, &trigger, 1, &none, 0, null);
}

pub fn waitCancelSignal(kq: Handle, timeout_ms: u32) bool {
    var fds = [1]c.pollfd{.{ .fd = kq, .events = c.POLL.IN, .revents = 0 }};
    const rc = c.poll(&fds, 1, @intCast(@min(timeout_ms, std.math.maxInt(c_int))));
    return rc > 0 and fds[0].revents != 0;
}

/// Milliseconds on the monotonic clock.
fn nowMs() u64 {
    var ts: c.timespec = undefined;
    std.debug.assert(c.clock_gettime(.MONOTONIC, &ts) == 0);
    return @as(u64, @intCast(ts.sec)) * std.time.ms_per_s + @as(u64, @intCast(ts.nsec)) / std.time.ns_per_ms;
}

pub fn sleep(ms: u32) void {
    const ts: c.timespec = .{ .sec = @intCast(ms / 1000), .nsec = @intCast(@as(u64, ms % 1000) * std.time.ns_per_ms) };
    _ = c.nanosleep(&ts, null);
}

/// Receives the SEGMENT frame into `frame` and its descriptor, without blocking: the caller waited with
/// `readableWithin`, and holds the handle lock, so the descriptor is made close-on-exec before any fork. Exactly
/// `frame.len` bytes and exactly one descriptor, else every received descriptor is closed and the error says what was
/// wrong (ControlTruncated: the kernel couldn't give this process the descriptor, EMFILE, or dropped control data,
/// `MSG_CTRUNC`).
pub fn recvSegment(fd: Handle, frame: []u8) platform.RecvError!Handle {
    var iov = [1]std.posix.iovec{.{ .base = frame.ptr, .len = frame.len }};
    // Room for more descriptors than the one expected, so that extra ones arrive (and are closed) rather than
    // being dropped as MSG_CTRUNC
    var control: Rights(4) = undefined;
    var msg: c.msghdr = .{
        .name = null,
        .namelen = 0,
        .iov = &iov,
        .iovlen = 1,
        .control = &control,
        .controllen = @sizeOf(Rights(4)),
        .flags = 0,
    };
    const rc = c.recvmsg(fd, &msg, c.MSG.DONTWAIT);
    switch (c.errno(rc)) {
        .SUCCESS => {},
        .CONNRESET => return error.EndOfStream,
        .MFILE, .NFILE => return error.ControlTruncated,
        else => |e| return unexpected(@src(), "recvmsg", e),
    }
    // The descriptors that arrived, whatever else is wrong: every SCM_RIGHTS message in the control data
    var received: [4]Handle = undefined;
    var count: usize = 0;
    var offset: usize = 0;
    const bytes = std.mem.asBytes(&control)[0..msg.controllen];
    while (offset + @sizeOf(c.cmsghdr) <= bytes.len) {
        const header: *align(1) const c.cmsghdr = @ptrCast(bytes[offset..].ptr);
        if (header.len < @sizeOf(c.cmsghdr) or offset + header.len > bytes.len) break;
        if (header.level == c.SOL.SOCKET and header.type == c.SCM.RIGHTS) {
            const payload = bytes[offset + @sizeOf(c.cmsghdr) .. offset + header.len];
            var i: usize = 0;
            while (i + @sizeOf(Handle) <= payload.len) : (i += @sizeOf(Handle)) {
                received[count] = std.mem.readInt(Handle, payload[i..][0..@sizeOf(Handle)], .native);
                count += 1;
            }
        }
        offset += std.mem.alignForward(usize, header.len, c.cmsg_align);
    }
    const failure: ?platform.RecvError = if (rc == 0)
        error.EndOfStream
    else if (rc != frame.len)
        error.ShortFrame
    else if (msg.flags & c.MSG.CTRUNC != 0)
        error.ControlTruncated
    else if (count != 1)
        error.DescriptorCount
    else if (setCloseOnExec(received[0])) |_|
        null
    else |err|
        err;
    if (failure) |err| {
        for (received[0..count]) |extra| _ = c.close(extra);
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

/// A mapped POSIX shared memory object. Its ring sleepers need no event: the sleep flag is the waited-on address
/// itself.
pub const Mapping = struct {
    base: [*]align(page_align) u8,
    size: usize,

    pub fn unmap(m: *Mapping) void {
        _ = c.munmap(m.base, m.size);
        m.* = undefined;
    }

    pub fn ringEvent(_: *const Mapping, _: names.Object) RingEvent {}
};

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
/// The session id names nothing here.
pub fn createSegment(handle: ?Handle, _: *const [16]u8, size: usize, _: *const Security) platform.CreateError!Mapping {
    const fd = handle.?;
    switch (c.errno(c.ftruncate(fd, @intCast(size)))) {
        .SUCCESS => {},
        .NOMEM, .INVAL, .FBIG, .NOSPC => return error.OutOfMemory,
        else => |e| return unexpected(@src(), "ftruncate", e),
    }
    return map(fd, size);
}

/// Maps a received segment once it can't change size (it is a POSIX shared memory object, which `proc_pidfdinfo`
/// recognizes and whose one `ftruncate` its creator has made) and is exactly `size` bytes rounded up to whole pages, so
/// no access inside the mapping can fault. The caller then unregisters and closes the descriptor.
pub fn openSegment(received: ?Handle, _: *const [16]u8, size: usize) platform.OpenError!Mapping {
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
    return map(fd, size);
}

/// Through the C library, like `Mapping.unmap`, so that ThreadSanitizer sees the mapping: a new segment often gets the
/// address of one unmapped a moment before, and the kernel's ordering of the two is invisible to it otherwise.
fn map(fd: Handle, size: usize) (error{OutOfMemory} || UnexpectedError)!Mapping {
    const base = c.mmap(null, size, .{ .READ = true, .WRITE = true }, .{ .TYPE = .SHARED }, fd, 0);
    if (base == c.MAP_FAILED) return switch (errnoNow()) {
        .NOMEM => error.OutOfMemory,
        else => |e| unexpected(@src(), "mmap", e),
    };
    return .{ .base = @ptrCast(@alignCast(base)), .size = size };
}

fn errnoNow() c.E {
    return @fromBackingInt(@intCast(c._errno().*));
}

/// Logs an errno the protocol doesn't expect, once per change of errno at that place on this thread: a handshake
/// retries most calls every few milliseconds (log.zig `warnOnChange`).
fn unexpected(comptime src: std.lang.SourceLocation, comptime call: []const u8, e: c.E) UnexpectedError {
    warnUnexpected(src, call, e);
    return error.Unexpected;
}

fn warnUnexpected(comptime src: std.lang.SourceLocation, comptime call: []const u8, e: c.E) void {
    log.warnOnChange(src, @backingInt(e), call ++ " failed: errno {d} ({s})", .{ @backingInt(e), std.enums.tagName(c.E, e) orelse "unnamed" });
}

// === Ring waits (data/ring_wait.zig) ===

/// None: the sleep flag is the waited-on address itself.
pub const RingEvent = void;
pub const no_ring_event: RingEvent = {};

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

// === For the tests' leak checks ===

/// The open descriptors of this process (0 to 1023).
pub fn openHandleCount() usize {
    var count: usize = 0;
    for (0..1024) |fd| {
        if (c.fcntl(@intCast(fd), c.F.GETFD) != -1) count += 1;
    }
    return count;
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

/// `VM_PROT_READ | VM_PROT_WRITE`.
const read_write = 0x1 | 0x2;

// === Tests (real system calls; the name of each test's socket is unique to its process) ===

const testing = std.testing;
const build_options = @import("build_options");

const no_security: Security = .{};

/// A rendezvous address unique to this process and call.
fn testAddress(comptime tag: []const u8) Address {
    const counter = struct {
        var next = std.atomic.Value(u32).init(0);
    };
    var name_buf: [64]u8 = undefined;
    const name = std.mem.print(&name_buf, "test-" ++ tag ++ "-{d}-{d}", .{ c.getpid(), counter.next.fetchAdd(1, .monotonic) }) catch unreachable;
    return addressOf(name) catch unreachable;
}

const openFds = openHandleCount;

/// Whether a file of that path exists (a symbolic link is not followed).
fn exists(path: [*:0]const u8) bool {
    var stat: c.Stat = undefined;
    return c.fstatat(c.AT.FDCWD, path, &stat, c.AT.SYMLINK_NOFOLLOW) == 0;
}

/// The name's lock file.
fn lockPath(address: *const Address, buf: *LockPath) [:0]const u8 {
    return lockPathOf(address.path(), buf);
}

fn closeOnExec(fd: Handle) bool {
    return c.fcntl(fd, c.F.GETFD) & c.FD_CLOEXEC != 0;
}

fn blocking(fd: Handle) bool {
    return c.fcntl(fd, c.F.GETFL) & o_nonblock == 0;
}

fn noSigpipe(fd: Handle) bool {
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

test "claim: one socket per name, which is free again at once when that socket closes, and leaves no file" {
    const fds = openFds();
    const address = testAddress("claim");
    var lock_buf: LockPath = undefined;
    const lock_path = lockPath(&address, &lock_buf);
    const first = (try claim(&address, &no_security)) orelse return error.TestUnexpectedResult;
    try testing.expect(exists(address.path()) and exists(lock_path));
    try testing.expectEqual(@as(?Handle, null), try claim(&address, &no_security));
    close(first);
    try testing.expect(!exists(address.path()) and !exists(lock_path));
    const again = (try claim(&address, &no_security)) orelse return error.TestUnexpectedResult;
    close(again);
    try testing.expect(!exists(address.path()) and !exists(lock_path));
    try testing.expectEqual(fds, openFds());
}

test "names: the rendezvous directory is this user's alone; a long name is hashed into the socket path, and works" {
    const fds = openFds();
    // The longest valid name, unique to this process
    var long: [names.max_len]u8 = @splat('x');
    _ = std.mem.print(&long, "test-long-{d}-", .{c.getpid()}) catch unreachable;
    const address = try addressOf(&long);
    try testing.expect(std.mem.span(address.path()).len <= names.macos_max);
    try testing.expect(std.mem.indexOfScalar(u8, address.file(), '~') != null);
    const pair = try Pair.open(&address);
    defer pair.deinit();
    var stat: c.Stat = undefined;
    var dir_buf: [names.macos_max + 1]u8 = undefined;
    const dir = std.mem.printSentinel(&dir_buf, "{s}", .{address.sockaddr.path[0 .. address.dir_len - 1]}, 0) catch unreachable;
    try testing.expectEqual(@as(c_int, 0), c.fstatat(c.AT.FDCWD, dir, &stat, c.AT.SYMLINK_NOFOLLOW));
    try testing.expectEqual(c.geteuid(), stat.uid);
    try testing.expectEqual(@as(u16, 0o700), stat.mode & 0o7777);
    try testing.expectEqual(fds + 4, openFds()); // the listening socket, the lock file, both ends
}

fn claimRacer(address: *const Address, start: *std.atomic.Value(u32), claimed: *?Handle) void {
    while (start.load(.acquire) == 0) std.atomic.spinLoopHint();
    claimed.* = claim(address, &no_security) catch null;
}

test "claim race: of several threads claiming one name at once, exactly one gets it" {
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

/// Leaves the files a crashed listener of the name leaves (a lock file nobody holds, a socket file nobody listens on),
/// made `age_s` seconds ago. The rendezvous directory must exist.
fn leaveStale(address: *const Address, age_s: i64) !void {
    var lock_buf: LockPath = undefined;
    const lock_path = lockPath(address, &lock_buf);
    const lock = c.open(lock_path, .{ .ACCMODE = .RDWR, .CREAT = true, .CLOEXEC = true }, @as(c_uint, 0o600));
    try testing.expect(lock >= 0);
    _ = c.close(lock);
    const bound = try socket();
    defer _ = c.close(bound);
    try testing.expectEqual(@as(c_int, 0), c.bind(bound, @ptrCast(&address.sockaddr), @sizeOf(c.sockaddr.un)));
    try age(lock_path, age_s);
}

/// Sets the file's modification time `age_s` seconds back.
fn age(path: [*:0]const u8, age_s: i64) !void {
    var now: c.timespec = undefined;
    try testing.expectEqual(@as(c_int, 0), c.clock_gettime(.REALTIME, &now));
    var times: [2]c.timeval = @splat(.{ .sec = @intCast(now.sec - age_s), .usec = 0 });
    try testing.expectEqual(@as(c_int, 0), c.utimes(path, &times));
}

/// Holds the lock file shared as a sweep does (`sweep`), for `ms`, then removes the name's files as a sweep does.
fn sweepLike(address: *const Address, held: Handle, ms: u32) void {
    sleep(ms);
    var lock_buf: LockPath = undefined;
    _ = c.unlink(address.path());
    _ = c.unlink(lockPath(address, &lock_buf));
    _ = c.close(held);
}

test "claim: a lock held shared only (a sweep removing a crashed listener's files) is waited for, then the name is claimed; one held exclusively is in use at once" {
    const fds = openFds();
    const address = testAddress("shared");
    var lock_buf: LockPath = undefined;
    const lock_path = lockPath(&address, &lock_buf);
    try checkDir(&address);
    // New, so that no other process's sweep removes it meanwhile (`sweep_min_age_s`)
    try leaveStale(&address, 0);
    const held = c.open(lock_path, .{ .ACCMODE = .RDWR, .CLOEXEC = true });
    try testing.expect(held >= 0);
    try testing.expectEqual(Locked.held, try lockFile(held, std.posix.LOCK.SH));
    const waits = test_shared_waits.load(.monotonic);
    const sweeper = try std.Thread.spawn(.{}, sweepLike, .{ &address, held, 50 });
    var start = nowMs();
    const claimed = (try claim(&address, &no_security)) orelse return error.TestUnexpectedResult;
    try testing.expect(nowMs() - start >= 40);
    sweeper.join();
    try testing.expect(test_shared_waits.load(.monotonic) > waits);
    // The claim's own files, made after the sweep's removal
    try testing.expect(exists(address.path()) and exists(lock_path));
    close(claimed);
    try testing.expect(!exists(address.path()) and !exists(lock_path));

    // Held exclusively, as a listener holds it: in use, at once
    const exclusive = c.open(lock_path, .{ .ACCMODE = .RDWR, .CREAT = true, .CLOEXEC = true }, @as(c_uint, 0o600));
    try testing.expect(exclusive >= 0);
    try testing.expectEqual(Locked.held, try lockFile(exclusive, std.posix.LOCK.EX));
    start = nowMs();
    try testing.expectEqual(@as(?Handle, null), try claim(&address, &no_security));
    try testing.expect(nowMs() - start < 20);
    _ = c.unlink(lock_path);
    _ = c.close(exclusive);
    try testing.expectEqual(fds, openFds());
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

test "connect: no listener, a socket file nobody listens on, connected, and a full backlog, which macOS refuses" {
    const fds = openFds();
    const address = testAddress("connect");
    try testing.expectEqual(platform.Connect.no_listener, try connect(&address, &no_security));

    // A socket file that nobody listens on, as a crashed listener leaves one: refused, like no listener; the next
    // claim removes it
    close((try claim(&address, &no_security)) orelse return error.TestUnexpectedResult); // the directory exists
    const bound = try socket();
    try testing.expectEqual(@as(c_int, 0), c.bind(bound, @ptrCast(&address.sockaddr), @sizeOf(c.sockaddr.un)));
    try testing.expectEqual(platform.Connect.no_listener, try connect(&address, &no_security));
    _ = c.close(bound);
    try testing.expect(exists(address.path()));

    const listener = (try claim(&address, &no_security)) orelse return error.TestUnexpectedResult;
    defer close(listener);
    var queued: [16]Handle = undefined;
    var n: usize = 0;
    defer for (queued[0..n]) |fd| close(fd);
    // Nobody accepts: the backlog of 4 fills, then macOS refuses the connection (ECONNREFUSED), as if nobody listened
    const full = while (n < queued.len) {
        switch (try connect(&address, &no_security)) {
            .connected => |fd| {
                queued[n] = fd;
                n += 1;
            },
            .no_listener => break true,
            .busy, .foreign => return error.TestUnexpectedResult,
        }
    } else false;
    try testing.expect(full);
    try testing.expect(n >= 1);
    // A connected socket is in blocking mode, close-on-exec, without SIGPIPE
    try testing.expect(blocking(queued[0]) and closeOnExec(queued[0]) and noSigpipe(queued[0]));
    try testing.expectEqual(fds + 2 + n, openFds()); // the listening socket and the lock file, the clients
}

test "accept and peer credentials: both ends see this process and user; descriptors are close-on-exec, without SIGPIPE" {
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
    var created = try createSegment(shm, &id, size, &no_security);
    defer created.unmap();
    const frame = @as([64]u8, @splat(0x5a));
    try sendMsg(pair.accepted, &frame, shm);

    // The peek leaves the frame and the descriptor queued; the receive takes both
    try testing.expectEqual(platform.Waited(void){ .done = {} }, readableWithin(pair.client, 5000));
    var got: [64]u8 = undefined;
    const received = try recvSegment(pair.client, &got);
    try testing.expectEqualSlices(u8, &frame, &got);
    try testing.expect(closeOnExec(received));
    var other = try openSegment(received, &id, size);
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
    var control: Rights(n) = .init(fds);
    const msg: c.msghdr_const = .{
        .name = null,
        .namelen = 0,
        .iov = &iov,
        .iovlen = 1,
        .control = &control,
        .controllen = @sizeOf(Rights(n)),
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

    try sendMsg(pair.accepted, &frame, null);
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
    try testing.expectError(error.WrongSize, openSegment(empty, &id, size));

    // Not shared memory: a device, a socket
    const device = c.open("/dev/null", .{ .ACCMODE = .RDWR, .CLOEXEC = true });
    try testing.expect(device >= 0);
    defer close(device);
    try testing.expectError(error.NotSealed, openSegment(device, &id, size));
    const address = testAddress("not-shm");
    const listener = (try claim(&address, &no_security)) orelse return error.TestUnexpectedResult;
    defer close(listener);
    try testing.expectError(error.NotSealed, openSegment(listener, &id, size));

    // The object has whole pages: an announced size in the same pages maps, one past them doesn't
    const page = std.heap.pageSize();
    const sized = try createSegmentHandle();
    defer close(sized);
    var created = try createSegment(sized, &id, size, &no_security);
    defer created.unmap();
    try testing.expectError(error.WrongSize, openSegment(sized, &id, page + size));
    var again = try openSegment(sized, &id, size);
    again.unmap();
    var whole = try openSegment(sized, &id, page);
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
        try testing.expectError(error.BrokenPipe, sendMsg(pair.client, &frame, null));
        // The READY a client sends after the listener died: the same
        try testing.expect(!try sendFrame(ref.io, pair.client, &frame, null));
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
        try testing.expectError(error.BrokenPipe, sendMsg(pair.accepted, &frame, null));
    }
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
    try testing.expect(closeOnExec(cancel));
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
    try testing.expect(blocking(server));
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
    try testing.expect(waitCancelSignal(cancel, 0));
    try testing.expect(try sendFrame(ref.io, client, "d", null));
    try testing.expectEqual(platform.Waited(usize).cancelled, readWithin(server, &buf, cancel, null));
    try testing.expectEqual(platform.Waited(platform.Accept).cancelled, acceptWithin(listener, cancel, null, null));
    try testing.expectEqual(platform.Waited(usize){ .done = 1 }, readWithin(server, &buf, null, 0));
    try testing.expectEqual(fds + 5, openFds()); // the lock file too
}

test "ring waits: a flag that isn't 1 returns at once, a timeout waits that long, a wake ends the wait" {
    const id: [16]u8 = @splat(3);
    const shm = try createSegmentHandle();
    defer close(shm);
    var mapping = try createSegment(shm, &id, 4096, &no_security);
    defer mapping.unmap();
    const flag: *std.atomic.Value(u32) = @ptrCast(@alignCast(mapping.base));

    flag.store(0, .monotonic);
    var start = nowMs();
    try testing.expect(!ringWait(flag, no_ring_event, 2000));
    try testing.expect(nowMs() - start < 1000);

    _ = flag.swap(1, .acq_rel);
    start = nowMs();
    try testing.expect(ringWait(flag, no_ring_event, 30));
    const waited = nowMs() - start;
    try testing.expect(waited >= 25 and waited < 1000);

    // A waiter on another thread: the publisher swaps the flag to 0, then wakes it
    const Waiter = struct {
        fn run(f: *std.atomic.Value(u32), woke_after: *u64) void {
            const t0 = nowMs();
            _ = f.swap(1, .acq_rel);
            while (f.load(.acquire) == 1 and nowMs() - t0 < 5000) _ = ringWait(f, no_ring_event, 5000);
            woke_after.* = nowMs() - t0;
        }
    };
    flag.store(0, .monotonic);
    var woke_after: u64 = std.math.maxInt(u64);
    const thread = try std.Thread.spawn(.{}, Waiter.run, .{ flag, &woke_after });
    while (flag.load(.acquire) == 0) std.Thread.yield() catch {};
    letItBlock();
    if (flag.swap(0, .acq_rel) == 1) ringWake(flag, no_ring_event);
    thread.join();
    try testing.expect(woke_after < 2000);
}

test "the segment mappings are counted: one more for each mapping, none left after the unmaps" {
    const id: [16]u8 = @splat(4);
    const before = segmentMappingCount();
    try testing.expectEqual(before, segmentMappingCount());
    const shm = try createSegmentHandle();
    defer close(shm);
    var first = try createSegment(shm, &id, 64 * 1024, &no_security);
    try testing.expectEqual(before + 1, segmentMappingCount());
    var second = try openSegment(shm, &id, 64 * 1024);
    try testing.expectEqual(before + 2, segmentMappingCount());
    second.unmap();
    try testing.expectEqual(before + 1, segmentMappingCount());
    first.unmap();
    try testing.expectEqual(before, segmentMappingCount());
}

/// Gives a task just started time to block in its system call: the stop rule must work whether it has or not,
/// and the test is only more telling when it has.
fn letItBlock() void {
    const ts: c.timespec = .{ .sec = 0, .nsec = 20 * std.time.ns_per_ms };
    _ = c.nanosleep(&ts, null);
}

/// The ids of this process's threads (`buf` long at most): the count is the number of threads.
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

fn threadCount() usize {
    var buf: [1024]u64 = undefined;
    return threadIds(&buf);
}

/// The id of the calling thread, and whether that thread has exited since.
const Worker = struct {
    tid: u64 = 0,

    fn run(worker: *Worker, io: Io, gate: *std.atomic.Value(u32)) void {
        _ = c.pthread_threadid_np(null, &worker.tid);
        while (gate.load(.acquire) == 0) io.futexWaitUncancelable(u32, &gate.raw, 0);
    }

    fn gone(worker: Worker) bool {
        var buf: [1024]u64 = undefined;
        return std.mem.indexOfScalar(u64, buf[0..threadIds(&buf)], worker.tid) == null;
    }
};

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
        const ts: c.timespec = .{ .sec = 0, .nsec = std.time.ns_per_ms };
        _ = c.nanosleep(&ts, null);
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

test {
    if (build_options.slow_tests) _ = slow_tests;
}

/// The tests that fork (the slow tier, `zig build test-slow`). A forked child runs only its checks and leaves with
/// `_exit`: it never returns into the test runner.
const slow_tests = struct {
    /// Waits up to `timeout_ms` for the child to exit, and returns its exit code; kills it at the deadline (null: it
    /// hung).
    fn waitChild(child: c.pid_t, timeout_ms: u32) ?u8 {
        var status: c_int = 0;
        for (0..timeout_ms) |_| {
            if (c.waitpid(child, &status, c.W.NOHANG) == child) {
                const s: u32 = @bitCast(status);
                return if (s & 0x7f == 0) @truncate(s >> 8) else 128 + @as(u8, @truncate(s & 0x7f));
            }
            sleep(1);
        }
        _ = c.kill(child, .KILL);
        _ = c.waitpid(child, &status, 0);
        return null;
    }

    /// A pipe whose ends are close-on-exec.
    fn pipe() ![2]Handle {
        var fds: [2]Handle = undefined;
        try testing.expectEqual(@as(c_int, 0), c.pipe(&fds));
        for (fds) |fd| try setCloseOnExec(fd);
        return fds;
    }

    fn readByte(fd: Handle) void {
        var byte: [1]u8 = undefined;
        _ = c.read(fd, &byte, 1);
    }

    test "slow: a ring sleeper in another process sleeps on the shared flag until the publisher wakes it" {
        const id: [16]u8 = @splat(5);
        const shm = try createSegmentHandle();
        defer close(shm);
        var mapping = try createSegment(shm, &id, 4096, &no_security);
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
        try testing.expectEqual(@as(?u8, 0), waitChild(child, 5000));
    }

    test "slow: a listener that crashed leaves its socket file, which the name's next claim clears" {
        const address = testAddress("crashed");
        const child = c.fork();
        if (child == 0) {
            // Claim, then end without closing anything, as a crash does
            const listening = (claim(&address, &no_security) catch c._exit(1)) orelse c._exit(2);
            _ = listening;
            c._exit(0);
        }
        try testing.expect(child > 0);
        try testing.expectEqual(@as(?u8, 0), waitChild(child, 5000));
        var lock_buf: LockPath = undefined;
        try testing.expect(exists(address.path()) and exists(lockPath(&address, &lock_buf)));
        try testing.expectEqual(platform.Connect.no_listener, try connect(&address, &no_security));

        const pair = try Pair.open(&address);
        pair.deinit();
        try testing.expect(!exists(address.path()) and !exists(lockPath(&address, &lock_buf)));
    }

    /// Claims names until one listen's sweep has removed the files of `address` (at most 5 listens: a listen whose
    /// sweep waited too long for another process's sweep skips its own). Whether they are gone.
    fn sweptBy(address: *const Address) !bool {
        var lock_buf: LockPath = undefined;
        for (0..5) |_| {
            const other = testAddress("sweeper");
            close((try claim(&other, &no_security)) orelse return error.TestUnexpectedResult);
            if (!exists(address.path()) and !exists(lockPath(address, &lock_buf))) return true;
        }
        return false;
    }

    /// A child that claims the name, makes its lock file an hour old, says so ("r"), then waits for a byte and leaves
    /// without closing anything (as a crash does). Its pid.
    fn listenerChild(address: *const Address, to_child: [2]Handle, from_child: [2]Handle) c.pid_t {
        const child = c.fork();
        if (child == 0) {
            _ = (claim(address, &no_security) catch c._exit(1)) orelse c._exit(2);
            var lock_buf: LockPath = undefined;
            age(lockPath(address, &lock_buf), 3600) catch c._exit(3);
            _ = c.write(from_child[1], "r", 1);
            readByte(to_child[0]);
            c._exit(0);
        }
        return child;
    }

    test "slow: the files a crashed listener left are removed when another name is listened on" {
        const address = testAddress("crashed-swept");
        const to_child = try pipe();
        const from_child = try pipe();
        defer for (to_child ++ from_child) |fd| {
            _ = c.close(fd);
        };
        const child = listenerChild(&address, to_child, from_child);
        try testing.expect(child > 0);
        readByte(from_child[0]);
        _ = c.write(to_child[1], "x", 1);
        try testing.expectEqual(@as(?u8, 0), waitChild(child, 5000));
        var lock_buf: LockPath = undefined;
        try testing.expect(exists(address.path()) and exists(lockPath(&address, &lock_buf)));
        try testing.expect(try sweptBy(&address));
    }

    /// Another listener's sweep leaves the files of a listener in another process, running or stopped, and a client
    /// gets in.
    fn survivesSweep(comptime stop: bool) !void {
        const address = testAddress(if (stop) "stopped" else "live");
        const to_child = try pipe();
        const from_child = try pipe();
        defer for (to_child ++ from_child) |fd| {
            _ = c.close(fd);
        };
        const child = listenerChild(&address, to_child, from_child);
        try testing.expect(child > 0);
        readByte(from_child[0]);
        if (stop) try testing.expectEqual(@as(c_int, 0), c.kill(child, .STOP));
        for (0..3) |_| {
            const other = testAddress("sweeper");
            close((try claim(&other, &no_security)) orelse return error.TestUnexpectedResult);
        }
        var lock_buf: LockPath = undefined;
        try testing.expect(exists(address.path()) and exists(lockPath(&address, &lock_buf)));
        try testing.expectEqual(@as(?Handle, null), try claim(&address, &no_security));
        switch (try connect(&address, &no_security)) {
            .connected => |fd| close(fd),
            else => return error.TestUnexpectedResult,
        }
        if (stop) try testing.expectEqual(@as(c_int, 0), c.kill(child, .CONT));
        _ = c.write(to_child[1], "x", 1);
        try testing.expectEqual(@as(?u8, 0), waitChild(child, 5000));
        // Gone with its process: the next sweep clears them
        try testing.expect(try sweptBy(&address));
    }

    test "slow: a running listener's files survive another listener's sweep" {
        try survivesSweep(false);
    }

    test "slow: a stopped (SIGSTOP) listener's files survive another listener's sweep" {
        try survivesSweep(true);
    }

    test "slow: a lock held shared that is never let go (a stopped sweep) makes the name in use after the wait" {
        const address = testAddress("stuck-sweep");
        var lock_buf: LockPath = undefined;
        const lock_path = lockPath(&address, &lock_buf);
        try checkDir(&address);
        // New, so that no other process's sweep removes it meanwhile (`sweep_min_age_s`)
        try leaveStale(&address, 0);
        const held = c.open(lock_path, .{ .ACCMODE = .RDWR, .CLOEXEC = true });
        try testing.expect(held >= 0);
        defer sweepLike(&address, held, 0);
        try testing.expectEqual(Locked.held, try lockFile(held, std.posix.LOCK.SH));
        const start = nowMs();
        try testing.expectEqual(@as(?Handle, null), try claim(&address, &no_security));
        try testing.expect(nowMs() - start >= shared_wait_ms);
    }

    /// The stress test's shared state (a shared anonymous mapping, made before the forks).
    const Shared = struct {
        round: std.atomic.Value(u32) = .init(0),
        claimed: std.atomic.Value(u32) = .init(0),
        finished: std.atomic.Value(u32) = .init(0),
        wins: std.atomic.Value(u32) = .init(0),
        stop: std.atomic.Value(u32) = .init(0),
        failures: std.atomic.Value(u32) = .init(0),
        shared_waits: std.atomic.Value(u32) = .init(0),
        sweeps: std.atomic.Value(u32) = .init(0),
    };

    const claimers = 3;
    const sweepers = 3;
    const rounds = 200;

    fn fail(shared: *Shared, comptime what: []const u8) void {
        writeStderr("stress: " ++ what ++ "\n");
        _ = shared.failures.fetchAdd(1, .monotonic);
    }

    /// Each round: claims the name once, as the other claimers do; the one that gets it checks its files and that a
    /// client gets in, once every claimer has tried, then closes it.
    fn claimer(shared: *Shared, name: *const Address) void {
        var round: u32 = 1;
        while (round <= rounds) : (round += 1) {
            while (shared.round.load(.acquire) < round) {
                if (shared.stop.load(.acquire) != 0) return;
                std.atomic.spinLoopHint();
            }
            const got = claim(name, &no_security) catch failed: {
                fail(shared, "a claim of the name failed");
                break :failed null;
            };
            _ = shared.claimed.fetchAdd(1, .acq_rel);
            while (shared.claimed.load(.acquire) < claimers) std.atomic.spinLoopHint();
            if (got) |listening| {
                _ = shared.wins.fetchAdd(1, .acq_rel);
                var lock_buf: LockPath = undefined;
                if (!exists(name.path()) or !exists(lockPath(name, &lock_buf))) fail(shared, "a listener's file is missing");
                switch (connect(name, &no_security) catch .busy) {
                    .connected => |fd| {
                        if (accepted(acceptWithin(listening, null, 5000, null))) |server| close(server) else |_| fail(shared, "no client accepted");
                        close(fd);
                    },
                    else => fail(shared, "a client didn't get in"),
                }
                close(listening);
            }
            _ = shared.finished.fetchAdd(1, .acq_rel);
        }
    }

    /// Until told to stop: leaves an hour-old crashed listener's files of a new name, and listens on another new name
    /// (each listen sweeps).
    fn sweeperLoop(shared: *Shared, index: usize) void {
        var k: u32 = 0;
        while (shared.stop.load(.acquire) == 0) : (k += 1) {
            var name_buf: [64]u8 = undefined;
            const stale = addressOf(std.mem.print(&name_buf, "test-stress-{d}-s{d}-{d}", .{ c.getppid(), index, k }) catch unreachable) catch return fail(shared, "addressOf");
            leaveStale(&stale, 3600) catch fail(shared, "leaveStale");
            const own = addressOf(std.mem.print(&name_buf, "test-stress-{d}-c{d}-{d}", .{ c.getppid(), index, k }) catch unreachable) catch return fail(shared, "addressOf");
            const got = claim(&own, &no_security) catch null;
            if (got) |fd| close(fd) else fail(shared, "a sweeper's claim of a new name failed");
            _ = shared.sweeps.fetchAdd(1, .monotonic);
        }
    }

    test "slow: stress: claims of one name racing in rounds over a crashed listener's files, while other processes listen and sweep: one winner per round, no file lost or left" {
        const mem = c.mmap(null, @sizeOf(Shared), .{ .READ = true, .WRITE = true }, .{ .TYPE = .SHARED, .ANONYMOUS = true }, -1, 0);
        try testing.expect(mem != c.MAP_FAILED);
        defer _ = c.munmap(@alignCast(mem), @sizeOf(Shared));
        const shared: *Shared = @ptrCast(@alignCast(mem));
        shared.* = .{};
        var name_buf: [64]u8 = undefined;
        const name = try addressOf(std.mem.print(&name_buf, "test-stress-{d}-n", .{c.getpid()}) catch unreachable);
        try checkDir(&name);

        var children: [claimers + sweepers]c.pid_t = undefined;
        for (&children, 0..) |*child, i| {
            child.* = c.fork();
            if (child.* == 0) {
                if (i < claimers) claimer(shared, &name) else sweeperLoop(shared, i);
                _ = shared.shared_waits.fetchAdd(test_shared_waits.load(.monotonic), .monotonic);
                c._exit(0);
            }
            try testing.expect(child.* > 0);
        }
        var bad_rounds: u32 = 0;
        var round: u32 = 1;
        while (round <= rounds) : (round += 1) {
            // The files of a crashed listener of the name, an hour old: a sweep may be removing them as the round starts
            const crashed = c.fork();
            if (crashed == 0) {
                _ = (claim(&name, &no_security) catch c._exit(1)) orelse c._exit(2);
                var lock_buf: LockPath = undefined;
                age(lockPath(&name, &lock_buf), 3600) catch c._exit(3);
                c._exit(0);
            }
            if (waitChild(crashed, 5000) != @as(?u8, 0)) bad_rounds += 1;
            shared.claimed.store(0, .monotonic);
            shared.finished.store(0, .monotonic);
            shared.wins.store(0, .monotonic);
            shared.round.store(round, .release);
            const t0 = nowMs();
            while (shared.finished.load(.acquire) < claimers and nowMs() - t0 < 10_000) sleep(1);
            if (shared.finished.load(.acquire) < claimers or shared.wins.load(.acquire) != 1) bad_rounds += 1;
        }
        shared.stop.store(1, .release);
        for (children) |child| try testing.expectEqual(@as(?u8, 0), waitChild(child, 10_000));
        std.debug.print("stress: {d} rounds, {d} sweeping listens, {d} claim waits for a sweep\n", .{ rounds, shared.sweeps.load(.monotonic), shared.shared_waits.load(.monotonic) });
        try testing.expectEqual(@as(u32, 0), bad_rounds);
        try testing.expectEqual(@as(u32, 0), shared.failures.load(.monotonic));

        // Nothing left: the name's last listener closed, and the next sweep clears the sweepers' crashed names
        var lock_buf: LockPath = undefined;
        try testing.expect(!exists(name.path()) and !exists(lockPath(&name, &lock_buf)));
        var prefix_buf: [64]u8 = undefined;
        const prefix = std.mem.print(&prefix_buf, "test-stress-{d}-", .{c.getpid()}) catch unreachable;
        var left: usize = 0;
        for (0..5) |_| {
            const other = testAddress("sweeper");
            close((try claim(&other, &no_security)) orelse return error.TestUnexpectedResult);
            left = filesStartingWith(&name, prefix);
            if (left == 0) break;
        }
        try testing.expectEqual(@as(usize, 0), left);
    }

    /// How many files in the rendezvous directory start with `prefix`.
    fn filesStartingWith(address: *const Address, prefix: []const u8) usize {
        var dir_buf: [names.macos_max + 1]u8 = undefined;
        const dir_path = std.mem.printSentinel(&dir_buf, "{s}", .{address.sockaddr.path[0..address.dir_len]}, 0) catch unreachable;
        const dir = c.opendir(dir_path) orelse return std.math.maxInt(usize);
        defer _ = c.closedir(dir);
        var count: usize = 0;
        while (c.readdir(dir)) |entry| {
            if (std.mem.startsWith(u8, entry.name[0..entry.namlen], prefix)) count += 1;
        }
        return count;
    }

    test "slow: a forked child's copy of a listening socket doesn't keep the name, nor remove its files" {
        if (builtin.sanitize_thread) return error.SkipZigTest; // the child of a parent with threads
        const ref = try process_state.acquire(); // the fork handlers are registered
        defer process_state.release(ref);
        const address = testAddress("forked");
        var lock_buf: LockPath = undefined;
        const listening = (try claim(&address, &no_security)) orelse return error.TestUnexpectedResult;
        var node: process_state.Handles = .{};
        node.join();
        defer node.leave();
        process_state.lockHandles();
        node.register(.listening, listening);
        process_state.unlockHandles();

        const to_child = try pipe();
        const from_child = try pipe();
        defer for (to_child ++ from_child) |fd| {
            _ = c.close(fd);
        };
        const child = c.fork();
        if (child == 0) {
            // The child handler closed the child's copies: it never claimed, so it removed nothing
            const code: u8 = if (node.forked and node.get(.listening) == null) 0 else 1;
            _ = c.write(from_child[1], "r", 1);
            readByte(to_child[0]); // until the parent has looked
            c._exit(code);
        }
        try testing.expect(child > 0);
        readByte(from_child[0]);
        // The parent's listener is whole: its files, its lock, and a client gets in
        try testing.expect(exists(address.path()) and exists(lockPath(&address, &lock_buf)));
        try testing.expectEqual(@as(?Handle, null), try claim(&address, &no_security));
        const client = switch (try connect(&address, &no_security)) {
            .connected => |fd| fd,
            else => return error.TestUnexpectedResult,
        };
        close(try accepted(acceptWithin(listening, null, 5000, null)));
        close(client);
        // Once the parent closes it, the name is free at once, while the child still runs
        node.close(.listening);
        try testing.expect(!exists(address.path()) and !exists(lockPath(&address, &lock_buf)));
        const reclaimed = try claim(&address, &no_security);
        if (reclaimed) |fd| close(fd);
        _ = c.write(to_child[1], "x", 1);
        try testing.expectEqual(@as(?u8, 0), waitChild(child, 5000));
        try testing.expect(reclaimed != null);
    }
};
