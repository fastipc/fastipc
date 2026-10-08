//! Raw macOS declarations that std doesn't have, private to `macos.zig`: libSystem functions and constants, each as the
//! macOS SDK declares it (the header is named beside each), and checks that the std types `macos.zig` passes to the
//! system have the SDK's layouts on aarch64-macos.

const std = @import("std");
const c = std.c;

// === sys/un.h: the peer of a local socket ===

/// `getsockopt` level of a local (`AF_UNIX`) socket's own options.
pub const SOL_LOCAL: i32 = 0;
/// `getsockopt(SOL_LOCAL)`: the process id of the peer, as recorded at `connect` or `listen` (a `pid_t`).
pub const LOCAL_PEERPID: u32 = 0x002;

/// unistd.h: the effective user and group of the process at the other end of a connected local socket.
pub extern "c" fn getpeereid(fd: c.fd_t, uid: *c.uid_t, gid: *c.gid_t) c_int;

// === unistd.h: the per-user directory ===

/// `confstr`: the user's private directory, `/var/folders/<xx>/<id>/0/`, owned by the user and kept while in use (not
/// the `T/` temporary directory beside it, whose files are cleaned when old).
pub const _CS_DARWIN_USER_DIR: c_int = 65536;

/// The value of the string variable `name` into `buf` (NUL-terminated, truncated to `len`): the length it needs with
/// its NUL, or 0 on failure (errno set).
pub extern "c" fn confstr(name: c_int, buf: ?[*]u8, len: usize) usize;

// === libproc.h, sys/proc_info.h ===

/// `proc_pidfdinfo` flavor: what a POSIX shared memory descriptor refers to (`struct pshm_fdinfo`); fails for any
/// other kind of descriptor.
pub const PROC_PIDFDPSHMINFO: c_int = 5;
/// `sizeof(struct pshm_fdinfo)` on aarch64-macos (`PROC_PIDFDPSHMINFO_SIZE`).
pub const pshm_fdinfo_size = 1192;

/// Fills `buffer` with `flavor`'s information about descriptor `fd` of process `pid`: the bytes written (the flavor's
/// size), or 0 on failure (errno set).
pub extern "c" fn proc_pidfdinfo(pid: c_int, fd: c_int, flavor: c_int, buffer: ?*anyopaque, buffersize: c_int) c_int;

// === os/os_sync_wait_on_address.h, os/clock.h (macOS 14.4) ===

/// `os_sync_wait_on_address_flags_t`: the address is in memory shared with other processes.
pub const OS_SYNC_WAIT_ON_ADDRESS_SHARED: u32 = 0x1;
/// `os_sync_wake_by_address_flags_t`: the address is in memory shared with other processes.
pub const OS_SYNC_WAKE_BY_ADDRESS_SHARED: u32 = 0x1;
/// `os_clockid_t`: the timeout counts `mach_absolute_time`, which doesn't advance while the system sleeps.
pub const OS_CLOCK_MACH_ABSOLUTE_TIME: u32 = 32;

/// Sleeps while the `size` (4 or 8) bytes at `addr` hold `value`, for at most `timeout_ns` (not 0): the number of
/// waiters left after a wake, or -1 with errno (ETIMEDOUT, EINTR; EFAULT while the page isn't mapped in yet).
pub extern "c" fn os_sync_wait_on_address_with_timeout(addr: *anyopaque, value: u64, size: usize, flags: u32, clockid: u32, timeout_ns: u64) c_int;

/// Wakes one thread waiting on `addr`: 0, or -1 with errno (ENOENT: nobody waits).
pub extern "c" fn os_sync_wake_by_address_any(addr: *anyopaque, size: usize, flags: u32) c_int;

// === mach/vm_region.h, mach/vm_statistics.h ===

/// `vm_region_submap_info_data_64_t`, which the SDK packs to 4 bytes (std's `vm_region_submap_info_64` is laid out
/// unpacked, so its later fields are at other offsets).
pub const vm_region_submap_info_64 = extern struct {
    protection: u32,
    max_protection: u32,
    inheritance: u32,
    offset: u64 align(4),
    user_tag: u32,
    pages_resident: u32,
    pages_shared_now_private: u32,
    pages_swapped_out: u32,
    pages_dirtied: u32,
    ref_count: u32,
    shadow_depth: u16,
    external_pager: u8,
    share_mode: u8,
    is_submap: c.boolean_t,
    behavior: i32,
    object_id: u32,
    user_wired_count: u16,
    pages_reusable: u32,
    object_id_full: u64 align(4),
};

/// `VM_REGION_SUBMAP_INFO_COUNT_64`: the struct's size in `natural_t`s.
pub const VM_REGION_SUBMAP_INFO_COUNT_64: c.mach_msg_type_number_t = @sizeOf(vm_region_submap_info_64) / @sizeOf(c.natural_t);

/// The share modes (`share_mode`) of a region whose memory other mappings may see.
pub const SM_SHARED: u8 = 4;
pub const SM_TRUESHARED: u8 = 5;
pub const SM_SHARED_ALIASED: u8 = 7;

comptime {
    // The layouts on aarch64-macos, as the SDK's headers give them (offsetof/sizeof in C)
    const assert = std.debug.assert;
    assert(@sizeOf(vm_region_submap_info_64) == 76);
    assert(@offsetOf(vm_region_submap_info_64, "user_tag") == 20);
    assert(@offsetOf(vm_region_submap_info_64, "share_mode") == 47);
    assert(@offsetOf(vm_region_submap_info_64, "is_submap") == 48);
    assert(@offsetOf(vm_region_submap_info_64, "object_id") == 56);
    assert(VM_REGION_SUBMAP_INFO_COUNT_64 == 19);

    assert(@sizeOf(c.cmsghdr) == 12 and c.cmsg_align == 4); // CMSG_LEN(4) == CMSG_SPACE(4) == 16
    assert(@sizeOf(c.msghdr) == 48 and @offsetOf(c.msghdr, "control") == 32 and @offsetOf(c.msghdr, "controllen") == 40 and @offsetOf(c.msghdr, "flags") == 44);
    assert(@sizeOf(c.sockaddr.un) == 106 and @offsetOf(c.sockaddr.un, "path") == 2);
    assert(@sizeOf(c.Stat) == 144 and @offsetOf(c.Stat, "size") == 96 and @offsetOf(c.Stat, "ino") == 8 and @offsetOf(c.Stat, "uid") == 16 and @offsetOf(c.Stat, "mode") == 4);
    assert(@sizeOf(c.Kevent) == 32);
    assert(@as(u32, @bitCast(c.O{ .DIRECTORY = true, .NOFOLLOW = true, .CLOEXEC = true })) == 0x100000 | 0x100 | 0x1000000);
    assert(@as(u32, @bitCast(c.O{ .ACCMODE = .RDWR, .CREAT = true, .EXCL = true, .NONBLOCK = true })) == 0x2 | 0x200 | 0x800 | 0x4);
}
