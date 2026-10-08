//! The few system calls the fork tests make (process_state.zig, c_api.zig and lifecycle/endpoint_test.zig), for
//! macOS: the names, types and return convention of `std.os.linux`, built on libSystem (`std.c`), so the same tests run
//! unchanged on both OSes (`std.os.linux` makes Linux's raw system calls, which XNU doesn't have). As there, a call
//! returns a `usize` that holds `-errno` on failure, and `errno` reads it. Test code only.

const std = @import("std");
const c = std.c;

pub const fd_t = c.fd_t;
pub const timespec = c.timespec;
pub const F = c.F;
pub const E = c.E;

/// `std.os.linux.errno`: the error a call's result holds, or SUCCESS.
pub fn errno(r: usize) E {
    const signed: isize = @bitCast(r);
    return @fromBackingInt(@intCast(if (signed > -4096 and signed < 0) -signed else 0));
}

/// A libc result (-1 and `errno` on failure) in `std.os.linux`'s form.
fn raw(rc: anytype) usize {
    if (rc == -1) return 0 -% @as(usize, @backingInt(c.errno(rc)));
    return @intCast(rc);
}

pub fn read(fd: fd_t, buf: [*]u8, count: usize) usize {
    return raw(c.read(fd, buf, count));
}

pub fn write(fd: fd_t, buf: [*]const u8, count: usize) usize {
    return raw(c.write(fd, buf, count));
}

pub fn close(fd: fd_t) usize {
    return raw(c.close(fd));
}

pub fn dup2(old: fd_t, new: fd_t) usize {
    return raw(c.dup2(old, new));
}

pub fn fcntl(fd: fd_t, cmd: i32, arg: usize) usize {
    return raw(c.fcntl(fd, cmd, arg));
}

pub fn nanosleep(req: *const timespec, rem: ?*timespec) usize {
    return raw(c.nanosleep(req, rem));
}

pub fn clock_gettime(clock: c.clockid_t, tp: *timespec) usize {
    return raw(c.clock_gettime(clock, tp));
}

/// `pipe2`, which macOS lacks: `pipe`, then close-on-exec on both ends when asked. The two steps aren't atomic; the
/// tests that use it start no other process meanwhile.
pub fn pipe2(fds: *[2]fd_t, flags: struct { CLOEXEC: bool = false }) usize {
    const rc = raw(c.pipe(fds));
    if (rc != 0 or !flags.CLOEXEC) return rc;
    for (fds) |fd| {
        const set = raw(c.fcntl(fd, c.F.SETFD, @as(c_int, c.FD_CLOEXEC)));
        if (set != 0) {
            for (fds) |end| _ = c.close(end);
            return set;
        }
    }
    return 0;
}
