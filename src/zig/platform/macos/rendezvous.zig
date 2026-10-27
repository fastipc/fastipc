//! macOS's rendezvous (docs/protocol.md §2, §3.2), private to macos.zig: a name's address, its claim, and the connect
//! to it. A name is the socket file `<dir>fastipc/<N>` (names.macosRendezvous), in the user's directory from
//! `confstr(_CS_DARWIN_USER_DIR)`, and its lock file `<N>.lock` beside it. The `fastipc` directory is the user's alone
//! (mode 0700), so no other user can reach the files; `peer` checks the user as well. A listener holds the name by an
//! `flock` on the lock file, which belongs to its open file description: two opens exclude each other, also two
//! threads of one process, and the lock goes with the last descriptor, also when the process dies. Holding it, the
//! listener removes a socket file a crashed listener left, binds and listens; it removes both files when it closes
//! (`releaseClaim`), so the name is free again at once. A crash leaves the two files, which the name's next claim
//! clears. Deleting the lock file by hand while a listener runs lets a second listener claim the name.

const std = @import("std");
const c = std.c;
const darwin = @import("darwin.zig");
const log = @import("../../core/log.zig");
const names = @import("../../core/names.zig");
const platform = @import("../../platform.zig");
const posix = @import("../posix.zig");

const Handle = posix.Handle;
const ResourceError = platform.ResourceError;
const UnexpectedError = platform.UnexpectedError;
const unexpected = posix.unexpected;
const errnoNow = posix.errnoNow;

/// A path socket address: `sun_path` holds the socket's absolute path (names.macosRendezvous) and a NUL.
pub const Address = struct {
    sockaddr: c.sockaddr.un,
    /// The length of the path's directory, `<dir>fastipc/`, before the socket's file name.
    dir_len: u8,

    /// The socket's path.
    pub fn path(address: *const Address) [*:0]const u8 {
        return @ptrCast(&address.sockaddr.path);
    }

    /// The socket's file name in the rendezvous directory.
    pub fn file(address: *const Address) [:0]const u8 {
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

// === The claim ===

/// A claimed name: its listening socket, the lock file's descriptor, which holds the lock, the process that claimed it,
/// and the socket's path. Listed by `claim`, taken off the list by `releaseClaim`.
const Claim = struct {
    listening: Handle,
    lock: Handle,
    owner: c.pid_t,
    /// The socket's path, NUL-terminated; the lock file's is the same with `.lock`.
    path: [names.macos_max + 1]u8,
    /// In `claims`.
    node: std.SinglyLinkedList.Node = .{},
};

/// The claims of this process: in practice changed under the handle lock (`claim`, `close`), and guarded by a lock of
/// their own all the same, which nothing holds while it waits.
var claims: std.SinglyLinkedList = .{};
var claims_lock: posix.Lock = .{};

/// How many times `claim` opens the lock file again when its holder removed it between the open and the `flock`, before
/// it calls the name held: a holder that let go has removed it, so a second try normally finds a new file.
const lock_attempts = 16;

/// Claims the name: holds its lock file (`flock`; held elsewhere: null), removes a stale socket file, then a new
/// non-blocking socket (`acceptWithin` polls, then accepts without waiting), `bind`, `listen(4)`.
pub fn claim(address: *const Address, _: *const posix.Security) (ResourceError || UnexpectedError)!?Handle {
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
    const entry = std.heap.c_allocator.create(Claim) catch return error.SystemResources;
    errdefer std.heap.c_allocator.destroy(entry);
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
    entry.* = .{ .listening = listening.?, .lock = lock, .owner = c.getpid(), .path = address.sockaddr.path };
    claims_lock.lock();
    defer claims_lock.unlock();
    claims.prepend(&entry.node);
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

/// Opens the lock file `path` and takes its exclusive `flock` without waiting: its descriptor, which holds the lock
/// until it is closed. Null when another open file holds it (a listener, or a claim in progress). A holder removes the
/// file before it releases the lock. So a lock taken on a file that the path no longer names is stale: it is released
/// and the path opened again, up to `lock_attempts` times.
fn holdLock(path: [*:0]const u8) (ResourceError || UnexpectedError)!?Handle {
    for (0..lock_attempts) |_| {
        const fd = c.open(path, .{ .ACCMODE = .RDWR, .CREAT = true, .NOFOLLOW = true, .CLOEXEC = true }, @as(c_uint, 0o600));
        if (fd < 0) return switch (errnoNow()) {
            .MFILE, .NFILE, .NOMEM => error.SystemResources,
            .NOSPC, .DQUOT => error.SystemResources,
            else => |e| unexpected(@src(), "open(lock)", e),
        };
        errdefer _ = c.close(fd);
        // Held elsewhere: in use
        if (try lockFile(fd) == .busy) {
            _ = c.close(fd);
            return null;
        }
        if (try namesFile(path, fd)) return fd;
        // Removed by a holder that let go: the name's file is a new one
        _ = c.close(fd);
    }
    return null;
}

const Locked = enum { held, busy };

/// Takes an exclusive `flock` on `fd` without waiting.
fn lockFile(fd: Handle) UnexpectedError!Locked {
    while (true) switch (c.errno(c.flock(fd, std.posix.LOCK.EX | std.posix.LOCK.NB))) {
        .SUCCESS => return .held,
        .INTR => {},
        .AGAIN => return .busy,
        else => |e| return unexpected(@src(), "flock", e),
    };
}

/// Whether `path` still names the file open as `fd` (a symbolic link is not followed).
fn namesFile(path: [*:0]const u8, fd: Handle) UnexpectedError!bool {
    var held: c.Stat = undefined;
    var named: c.Stat = undefined;
    if (c.fstat(fd, &held) != 0) return unexpected(@src(), "fstat(lock)", errnoNow());
    return switch (c.errno(c.fstatat(c.AT.FDCWD, path, &named, c.AT.SYMLINK_NOFOLLOW))) {
        .SUCCESS => named.dev == held.dev and named.ino == held.ino,
        .NOENT => false,
        else => |e| unexpected(@src(), "fstatat(lock)", e),
    };
}

/// Gives up the claim of listening socket `fd`, if it has one. Call it before the socket is closed.
///   - In the process that claimed the name, it removes the socket file and the lock file while it still holds the
///     lock, then releases the lock, so the name is free at once.
///   - In a forked child (core/process_state.zig's child handler), the parent still holds the name: it closes only the
///     child's copy of the lock file's descriptor, and leaves the entry allocated, since a fork handler makes no
///     allocator call.
pub fn releaseClaim(fd: Handle) void {
    const entry = takeClaim(fd) orelse return;
    if (entry.owner == c.getpid()) {
        const socket_path: [*:0]const u8 = @ptrCast(&entry.path);
        var lock_buf: LockPath = undefined;
        _ = c.unlink(socket_path);
        _ = c.unlink(lockPathOf(socket_path, &lock_buf));
        _ = c.close(entry.lock);
        std.heap.c_allocator.destroy(entry);
    } else {
        _ = c.close(entry.lock);
    }
}

/// Takes the claim of listening socket `fd` off the list, if it is one.
fn takeClaim(fd: Handle) ?*Claim {
    claims_lock.lock();
    defer claims_lock.unlock();
    var it = claims.first;
    while (it) |node| : (it = node.next) {
        const entry: *Claim = @fieldParentPtr("node", node);
        if (entry.listening == fd) {
            claims.remove(node);
            return entry;
        }
    }
    return null;
}

// === The connect ===

/// Connects to the name without blocking (a new non-blocking socket, `connect`), then puts the socket in blocking
/// mode, as `Io`'s reads need. `no_listener`: no socket file (ENOENT), or a file nobody listens on: a crashed
/// listener's, a holder's between `bind` and `listen`, or a listener whose backlog is full (ECONNREFUSED; macOS doesn't
/// tell that apart). `busy`: EAGAIN. Never `foreign`: `peer` tells the user.
pub fn connect(address: *const Address, _: *const posix.Security) (ResourceError || UnexpectedError)!platform.Connect {
    const fd = try socket();
    errdefer _ = c.close(fd);
    const outcome: platform.Connect = switch (c.errno(c.connect(fd, @ptrCast(&address.sockaddr), @sizeOf(c.sockaddr.un)))) {
        .SUCCESS => {
            try posix.setNonblocking(fd, false);
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

/// A new local stream socket: close-on-exec, non-blocking, and with `SO_NOSIGPIPE`, so that no send on it (or on a
/// socket accepted from it, which inherits it) raises SIGPIPE whatever the host's disposition.
fn socket() (ResourceError || UnexpectedError)!Handle {
    const fd = c.socket(c.AF.UNIX, c.SOCK.STREAM, 0);
    if (fd < 0) return switch (errnoNow()) {
        .MFILE, .NFILE, .NOBUFS, .NOMEM => error.SystemResources,
        else => |e| unexpected(@src(), "socket", e),
    };
    errdefer _ = c.close(fd);
    try posix.setCloseOnExec(fd);
    try posix.setNonblocking(fd, true);
    const on: c_int = 1;
    switch (c.errno(c.setsockopt(fd, c.SOL.SOCKET, c.SO.NOSIGPIPE, &on, @sizeOf(c_int)))) {
        .SUCCESS => {},
        else => |e| return unexpected(@src(), "setsockopt(SO_NOSIGPIPE)", e),
    }
    return fd;
}

// === Tests (real system calls; platform/contract_test.zig tests the contract) ===

const testing = std.testing;
const build_options = @import("build_options");
const builtin = @import("builtin");
const contract_test = @import("../contract_test.zig");
const macos = @import("macos.zig");
const process_state = @import("../../core/process_state.zig");

const no_security: posix.Security = .{};
const testAddress = contract_test.testAddress;
const Pair = contract_test.Pair;
const openFds = posix.openHandleCount;

/// Whether a file of that path exists (a symbolic link is not followed).
fn exists(path: [*:0]const u8) bool {
    var stat: c.Stat = undefined;
    return c.fstatat(c.AT.FDCWD, path, &stat, c.AT.SYMLINK_NOFOLLOW) == 0;
}

/// The name's lock file.
fn lockPath(address: *const Address, buf: *LockPath) [:0]const u8 {
    return lockPathOf(address.path(), buf);
}

test "a claim's files: the socket file and the lock file are there while it holds the name, gone once it closes" {
    const fds = openFds();
    const address = testAddress("claim-files");
    var lock_buf: LockPath = undefined;
    const lock_path = lockPath(&address, &lock_buf);
    const first = (try claim(&address, &no_security)) orelse return error.TestUnexpectedResult;
    try testing.expect(exists(address.path()) and exists(lock_path));
    try testing.expectEqual(@as(?Handle, null), try claim(&address, &no_security));
    platform.close(first);
    try testing.expect(!exists(address.path()) and !exists(lock_path));
    const again = (try claim(&address, &no_security)) orelse return error.TestUnexpectedResult;
    platform.close(again);
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
    try testing.expect(std.mem.findScalar(u8, address.file(), '~') != null);
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

test "claim: a lock file held exclusively by another open file, as a listener holds it, is in use at once" {
    const fds = openFds();
    const address = testAddress("held");
    var lock_buf: LockPath = undefined;
    const lock_path = lockPath(&address, &lock_buf);
    try checkDir(&address);
    const exclusive = c.open(lock_path, .{ .ACCMODE = .RDWR, .CREAT = true, .CLOEXEC = true }, @as(c_uint, 0o600));
    try testing.expect(exclusive >= 0);
    try testing.expectEqual(Locked.held, try lockFile(exclusive));
    const start = contract_test.nowMs();
    try testing.expectEqual(@as(?Handle, null), try claim(&address, &no_security));
    try testing.expect(contract_test.nowMs() - start < 20);
    _ = c.unlink(lock_path);
    _ = c.close(exclusive);
    try testing.expectEqual(fds, openFds());
}

test "connect: no listener, a socket file nobody listens on, connected, and a full backlog, which macOS refuses" {
    const fds = openFds();
    const address = testAddress("connect");
    try testing.expectEqual(platform.Connect.no_listener, try connect(&address, &no_security));

    // A socket file that nobody listens on, as a crashed listener leaves one: refused, like no listener; the next
    // claim removes it
    platform.close((try claim(&address, &no_security)) orelse return error.TestUnexpectedResult); // the directory exists
    const bound = try socket();
    try testing.expectEqual(@as(c_int, 0), c.bind(bound, @ptrCast(&address.sockaddr), @sizeOf(c.sockaddr.un)));
    try testing.expectEqual(platform.Connect.no_listener, try connect(&address, &no_security));
    _ = c.close(bound);
    try testing.expect(exists(address.path()));

    const listener = (try claim(&address, &no_security)) orelse return error.TestUnexpectedResult;
    defer platform.close(listener);
    var queued: [16]Handle = undefined;
    var n: usize = 0;
    defer for (queued[0..n]) |fd| platform.close(fd);
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
    try testing.expect(macos.blocking(queued[0]) and macos.closeOnExec(queued[0]) and macos.noSigpipe(queued[0]));
    try testing.expectEqual(fds + 2 + n, openFds()); // the listening socket and the lock file, the clients
}

test {
    if (build_options.slow_tests) _ = slow_tests;
}

/// The tests that fork (the slow tier, `zig build test-slow`). A forked child runs only its checks and leaves with
/// `_exit`: it never returns into the test runner.
const slow_tests = struct {
    /// A pipe whose ends are close-on-exec.
    fn pipe() ![2]Handle {
        var fds: [2]Handle = undefined;
        try testing.expectEqual(@as(c_int, 0), c.pipe(&fds));
        for (fds) |fd| try posix.setCloseOnExec(fd);
        return fds;
    }

    fn readByte(fd: Handle) void {
        var byte: [1]u8 = undefined;
        _ = c.read(fd, &byte, 1);
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
        try testing.expectEqual(@as(?u8, 0), process_state.waitChild(child, 5000));
        var lock_buf: LockPath = undefined;
        try testing.expect(exists(address.path()) and exists(lockPath(&address, &lock_buf)));
        try testing.expectEqual(platform.Connect.no_listener, try connect(&address, &no_security));

        const pair = try Pair.open(&address);
        pair.deinit();
        try testing.expect(!exists(address.path()) and !exists(lockPath(&address, &lock_buf)));
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
        const client = try contract_test.connected(try connect(&address, &no_security));
        platform.close(try contract_test.accepted(platform.acceptWithin(listening, null, 5000, null)));
        platform.close(client);
        // Once the parent closes it, the name is free at once, while the child still runs
        node.close(.listening);
        try testing.expect(!exists(address.path()) and !exists(lockPath(&address, &lock_buf)));
        const reclaimed = try claim(&address, &no_security);
        if (reclaimed) |fd| platform.close(fd);
        _ = c.write(to_child[1], "x", 1);
        try testing.expectEqual(@as(?u8, 0), process_state.waitChild(child, 5000));
        try testing.expect(reclaimed != null);
    }
};
