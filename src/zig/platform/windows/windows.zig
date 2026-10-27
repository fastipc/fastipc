//! Windows's side of the OS contract (platform.zig): the process, the user-only security of every object and an
//! object's owner, the cancel signal (a manual-reset event), a session's section and its events (named after the
//! session's random id), and the ring waits on those events.
//!   - The rendezvous is in windows/pipe.zig: the named pipe (claim, connect with the owner check, accept, disconnect,
//!     the frames on it, its peer process) and the Terminal Services session that scopes its name.
//!   - The poller (reads with completion routines) is in windows/poller.zig.
//! Both are re-exported here.
//!
//! Every call returns at once except four:
//!   - the handshake's waits on the caller's thread (pipe.zig);
//!   - the watcher's `read`, which blocks through `Io`;
//!   - the poller's `wait`, an alertable wait on its own thread;
//!   - `ringWait`, the one wait `Io` can't express (an event shared with the peer; stream/ring_wait.zig slices it).
//!
//! Every handle is created not inheritable, so a child process started with handle inheritance gets none.

const std = @import("std");
const windows = std.os.windows;
const Io = std.Io;
const log = @import("../../core/log.zig");
const names = @import("../../core/names.zig");
const platform = @import("../../platform.zig");
const named_pipe = @import("pipe.zig");
const poller = @import("poller.zig");
const win32 = @import("win32.zig");

const ResourceError = platform.ResourceError;
const UnexpectedError = platform.UnexpectedError;

pub const has_fork = false;
pub const passes_segment_handle = false;

pub const Handle = win32.HANDLE;

// === The process ===

/// This process's user SID and the security attributes of every object (docs/protocol.md §4.2): the user SID as the
/// owner (an elevated token's default owner would be the Administrators group) and as the only entry of a protected
/// DACL, `O:<SID>D:P(A;;GA;;;<SID>)`; not inheritable. An elevated and a non-elevated process of one user share the
/// SID, and an object created without a mandatory label counts as Medium integrity, which a High (elevated) process
/// may open too, so both directions of a mixed-elevation pair open each other's objects.
pub const Security = struct {
    sid: [win32.max_sid_size]u8 align(@alignOf(u32)),
    /// From `ConvertStringSecurityDescriptorToSecurityDescriptorW`; `deinit` frees it.
    descriptor: *anyopaque,

    pub fn init() UnexpectedError!Security {
        var security: Security = undefined;
        {
            var token: Handle = undefined;
            if (win32.OpenProcessToken(win32.GetCurrentProcess(), win32.TOKEN_QUERY, &token) == 0)
                return unexpected(@src(), "OpenProcessToken");
            defer _ = win32.CloseHandle(token);
            var buf: [@sizeOf(win32.TOKEN_USER) + win32.max_sid_size]u8 align(@alignOf(win32.TOKEN_USER)) = undefined;
            var len: win32.DWORD = 0;
            if (win32.GetTokenInformation(token, win32.TokenUser, &buf, buf.len, &len) == 0)
                return unexpected(@src(), "GetTokenInformation");
            const user: *const win32.TOKEN_USER = @ptrCast(&buf);
            const sid_len = win32.GetLengthSid(user.User.Sid);
            @memcpy(security.sid[0..sid_len], @as([*]const u8, @ptrCast(user.User.Sid))[0..sid_len]);
        }
        var sid_string: ?[*:0]u16 = null;
        if (win32.ConvertSidToStringSidW(security.sidPtr(), &sid_string) == 0) return unexpected(@src(), "ConvertSidToStringSidW");
        defer _ = win32.LocalFree(sid_string);
        const sid_text = std.mem.span(sid_string.?);
        // A SID string has at most 184 characters (15 sub-authorities)
        var sddl: Utf16Builder(2 * 184 + 20) = .{};
        sddl.ascii("O:");
        sddl.wide(sid_text);
        sddl.ascii("D:P(A;;GA;;;");
        sddl.wide(sid_text);
        sddl.ascii(")");
        var descriptor: ?*anyopaque = null;
        if (win32.ConvertStringSecurityDescriptorToSecurityDescriptorW(sddl.string(), win32.SDDL_REVISION_1, &descriptor, null) == 0)
            return unexpected(@src(), "ConvertStringSecurityDescriptorToSecurityDescriptorW");
        security.descriptor = descriptor.?;
        return security;
    }

    pub fn deinit(security: *Security) void {
        _ = win32.LocalFree(security.descriptor);
        security.* = undefined;
    }

    pub fn attributes(security: *const Security) win32.SECURITY_ATTRIBUTES {
        return .{
            .nLength = @sizeOf(win32.SECURITY_ATTRIBUTES),
            .lpSecurityDescriptor = security.descriptor,
            .bInheritHandle = win32.FALSE,
        };
    }

    /// The SID, for the calls that take one (they only read it).
    pub fn sidPtr(security: *const Security) *win32.SID {
        return @ptrCast(@constCast(&security.sid));
    }
};

/// An SRW lock.
pub const Lock = struct {
    raw: windows.SRWLOCK = .{},

    pub fn lock(mutex: *Lock) void {
        windows.ntdll.RtlAcquireSRWLockExclusive(&mutex.raw);
    }

    pub fn unlock(mutex: *Lock) void {
        windows.ntdll.RtlReleaseSRWLockExclusive(&mutex.raw);
    }
};

pub fn pid() u32 {
    return win32.GetCurrentProcessId();
}

/// The performance counter, in nanoseconds.
pub fn monotonicNs() u64 {
    var count: windows.LARGE_INTEGER = undefined;
    var frequency: windows.LARGE_INTEGER = undefined;
    _ = windows.ntdll.RtlQueryPerformanceCounter(&count);
    _ = windows.ntdll.RtlQueryPerformanceFrequency(&frequency);
    return @intCast(@as(u128, @intCast(count)) * std.time.ns_per_s / @as(u128, @intCast(frequency)));
}

pub fn crash(code: u8) noreturn {
    _ = win32.TerminateProcess(win32.GetCurrentProcess(), code);
    unreachable;
}

pub fn writeStderr(bytes: []const u8) void {
    const handle = win32.GetStdHandle(win32.STD_ERROR_HANDLE) orelse return;
    if (handle == win32.INVALID_HANDLE_VALUE) return;
    var written: win32.DWORD = 0;
    _ = win32.WriteFile(handle, bytes.ptr, @intCast(bytes.len), &written, null);
}

/// 2 MiB, because `std.Thread.spawn` passes the size as the thread's committed stack, not its reserve.
pub const task_stack_size: usize = 2 << 20;

/// Joins the workers, then waits until their threads have exited, including those still starting. The DLL may be
/// unloaded as soon as the last release returns, so no worker may still be running its code by then. A worker also
/// closes the handle to its own thread as it exits, after the join has returned, and that handle must be closed by
/// then too. No task is running (every one was awaited), so no new worker is spawned meanwhile.
pub fn shutDownIo(threaded: *Io.Threaded) void {
    var exits: std.ArrayList(Handle) = .empty;
    defer exits.deinit(std.heap.c_allocator);
    // The workers' own list (`Io/Threaded.zig`, `worker`): each worker links itself in when it starts, and its node
    // lives on its stack, which stays valid until the join. A worker spawned for a task that another worker took may
    // still be starting, so wait until the list holds every spawned worker. The pool's wait group (`WaitGroup`) counts
    // them: its state goes up by 2 per worker, and the low bit belongs to the join.
    const spawned = threaded.wait_group.state.load(.acquire) / 2;
    while (true) {
        var listed: usize = 0;
        var node = threaded.worker_threads.load(.acquire);
        while (node) |w| : (node = w.next) listed += 1;
        if (listed >= spawned) break;
        std.Thread.yield() catch {};
    }
    var worker = threaded.worker_threads.load(.acquire);
    while (worker) |w| : (worker = w.next) {
        const thread = win32.OpenThread(win32.SYNCHRONIZE, win32.FALSE, w.id) orelse continue;
        exits.append(std.heap.c_allocator, thread) catch {
            _ = win32.CloseHandle(thread);
        };
    }
    threaded.deinit();
    for (exits.items) |thread| {
        _ = win32.WaitForSingleObject(thread, win32.INFINITE);
        _ = win32.CloseHandle(thread);
    }
}

/// Who owns an object, as far as this process can tell.
pub const Owner = enum {
    /// This process's user.
    user,
    /// Another user, or an owner that can't be read for any reason but a disconnect: the check fails closed.
    foreign,
    /// A pipe whose server disconnected this client before its owner could be read.
    disconnected,
};

pub fn ownerOf(object: Handle, security: *const Security) Owner {
    var owner: ?*win32.SID = null;
    var descriptor: ?*anyopaque = null;
    const rc = win32.GetSecurityInfo(object, win32.SE_KERNEL_OBJECT, win32.OWNER_SECURITY_INFORMATION, &owner, null, null, null, &descriptor);
    switch (rc) {
        0 => {},
        win32.ERROR_PIPE_NOT_CONNECTED, win32.ERROR_BROKEN_PIPE, win32.ERROR_NO_DATA => return .disconnected,
        else => {
            log.warn(@src(), "GetSecurityInfo failed: error {d}", .{rc});
            return .foreign;
        },
    }
    defer _ = win32.LocalFree(descriptor);
    return if (win32.EqualSid(owner.?, security.sidPtr()) != 0) .user else .foreign;
}

// === Names and connections ===

pub const Address = named_pipe.Address;
pub const addressOf = named_pipe.addressOf;
pub const claim = named_pipe.claim;
pub const nextListening = named_pipe.nextListening;
pub const acceptWithin = named_pipe.acceptWithin;
pub const dropClient = named_pipe.dropClient;
pub const connect = named_pipe.connect;
pub const peer = named_pipe.peer;
pub const unblock = named_pipe.unblock;
pub const endTask = named_pipe.endTask;

pub fn close(handle: Handle) void {
    _ = win32.CloseHandle(handle);
}

// === Frames on a connection ===

pub const sendFrame = named_pipe.sendFrame;
pub const read = named_pipe.read;

// === Waits on the caller's thread ===

pub const readWithin = named_pipe.readWithin;

/// A manual-reset event: once set it stays set.
pub fn createCancelSignal() (ResourceError || UnexpectedError)!Handle {
    return win32.CreateEventW(null, win32.TRUE, win32.FALSE, null) orelse switch (win32.GetLastError()) {
        win32.ERROR_NO_SYSTEM_RESOURCES, win32.ERROR_NOT_ENOUGH_MEMORY, win32.ERROR_OUTOFMEMORY => error.SystemResources,
        else => unexpected(@src(), "CreateEventW"),
    };
}

pub fn setCancelSignal(event: Handle) void {
    _ = win32.SetEvent(event);
}

pub fn waitCancelSignal(event: Handle, timeout_ms: u32) bool {
    return win32.WaitForSingleObject(event, waitMs(timeout_ms)) == win32.WAIT_OBJECT_0;
}

pub fn sleep(ms: u32) void {
    win32.Sleep(waitMs(ms));
}

/// A wait's timeout in milliseconds: `timeout_ms`, short of `INFINITE`, or `INFINITE` (no limit) for null.
pub inline fn waitMs(timeout_ms: ?u32) win32.DWORD {
    const ms = timeout_ms orelse return win32.INFINITE;
    return @min(ms, win32.INFINITE - 1);
}

// === The poller ===

pub const Poller = poller.Poller;
pub const Watched = poller.Watched;

// === Segments ===

/// One session's objects: its section, mapped, and the auto-reset events of its kind (`kindObjects`), in that order: a
/// connection's four ring events (s2c data, s2c space, c2s data, c2s space), or a broadcast channel's event that wakes
/// its writer (`broadcast_room`; its subscribers' wakes are slot wakes, `createSlotWake`). The objects go away with
/// their last handle, in any process.
pub const Mapping = struct {
    section: Handle,
    /// The kind's objects first; the rest is unused.
    events: [4]Handle,
    kind: platform.SegmentKind,
    base: [*]u8,
    size: usize,

    pub fn unmap(mapping: *Mapping) void {
        _ = win32.UnmapViewOfFile(mapping.base);
        for (mapping.kindEvents()) |object| _ = win32.CloseHandle(object);
        _ = win32.CloseHandle(mapping.section);
        mapping.* = undefined;
    }

    pub fn kindEvents(mapping: *const Mapping) []const Handle {
        return mapping.events[0..kindObjects(mapping.kind).len];
    }

    /// The event of `object`, one of the kind's (`kindObjects`).
    pub fn ringEvent(mapping: *const Mapping, object: names.Object) RingEvent {
        return mapping.events[std.mem.findScalar(names.Object, kindObjects(mapping.kind), object).?];
    }
};

/// The events of a segment of `kind`, in `Mapping.events` order.
fn kindObjects(kind: platform.SegmentKind) []const names.Object {
    return switch (kind) {
        .stream => &.{ .s2c_data, .s2c_space, .c2s_data, .c2s_space },
        .broadcast => &.{.broadcast_room},
        .queue, .service => &.{},
    };
}

/// Creates session `id`'s objects with the user-only security: the section of `size` bytes, committed at creation
/// (so the commit limit is NO_MEMORY now, not a fault later), and the auto-reset events of its `kind`; then maps the
/// section. NameCollision: an object of the session's name exists. No handle is passed: `handle` is null.
pub fn createSegment(handle: ?Handle, id: *const [16]u8, size: usize, security: *const Security, kind: platform.SegmentKind) platform.CreateError!Mapping {
    std.debug.assert(handle == null);
    const attributes = security.attributes();
    var name: ObjectName = .init(id, .section);
    const section = win32.CreateFileMappingW(
        win32.INVALID_HANDLE_VALUE,
        &attributes,
        win32.PAGE_READWRITE,
        @truncate(size >> 32),
        @truncate(size),
        name.string(),
    ) orelse return switch (win32.GetLastError()) {
        win32.ERROR_COMMITMENT_LIMIT, win32.ERROR_NOT_ENOUGH_MEMORY, win32.ERROR_OUTOFMEMORY => error.OutOfMemory,
        win32.ERROR_NO_SYSTEM_RESOURCES => error.SystemResources,
        else => unexpected(@src(), "CreateFileMappingW"),
    };
    errdefer _ = win32.CloseHandle(section);
    if (win32.GetLastError() == win32.ERROR_ALREADY_EXISTS) return error.NameCollision;

    var events: [4]Handle = undefined;
    var created: usize = 0;
    errdefer for (events[0..created]) |event| {
        _ = win32.CloseHandle(event);
    };
    for (events[0..kindObjects(kind).len], kindObjects(kind)) |*event, object| {
        name = .init(id, object);
        event.* = win32.CreateEventW(&attributes, win32.FALSE, win32.FALSE, name.string()) orelse return switch (win32.GetLastError()) {
            win32.ERROR_NO_SYSTEM_RESOURCES, win32.ERROR_NOT_ENOUGH_MEMORY, win32.ERROR_OUTOFMEMORY => error.SystemResources,
            else => unexpected(@src(), "CreateEventW"),
        };
        created += 1;
        if (win32.GetLastError() == win32.ERROR_ALREADY_EXISTS) return error.NameCollision;
    }
    const base = win32.MapViewOfFile(section, win32.FILE_MAP_READ | win32.FILE_MAP_WRITE, 0, 0, size) orelse return switch (win32.GetLastError()) {
        win32.ERROR_NOT_ENOUGH_MEMORY, win32.ERROR_OUTOFMEMORY => error.OutOfMemory,
        else => unexpected(@src(), "MapViewOfFile"),
    };
    return .{ .section = section, .events = events, .kind = kind, .base = @ptrCast(base), .size = size };
}

/// Opens session `id`'s objects by name, as the client does with the id a SEGMENT frame names (a subscriber: an OFFER),
/// and maps `size` bytes of the section. Gone: an object no longer exists (the listener left after sending SEGMENT).
/// AccessDenied: the DACL of the section or an event refused this process. WrongSize: the section is smaller than
/// `size`, so the view is refused (ERROR_ACCESS_DENIED from `MapViewOfFile`, not from the open). No handle was
/// received: `received` is null.
pub fn openSegment(received: ?Handle, id: *const [16]u8, size: usize, kind: platform.SegmentKind) platform.OpenError!Mapping {
    std.debug.assert(received == null);
    var name: ObjectName = .init(id, .section);
    const section = win32.OpenFileMappingW(win32.FILE_MAP_READ | win32.FILE_MAP_WRITE, win32.FALSE, name.string()) orelse
        return openFailure(@src(), "OpenFileMappingW");
    errdefer _ = win32.CloseHandle(section);

    var events: [4]Handle = undefined;
    var opened: usize = 0;
    errdefer for (events[0..opened]) |event| {
        _ = win32.CloseHandle(event);
    };
    for (events[0..kindObjects(kind).len], kindObjects(kind)) |*event, object| {
        name = .init(id, object);
        event.* = win32.OpenEventW(win32.EVENT_MODIFY_STATE | win32.SYNCHRONIZE, win32.FALSE, name.string()) orelse
            return openFailure(@src(), "OpenEventW");
        opened += 1;
    }
    const base = win32.MapViewOfFile(section, win32.FILE_MAP_READ | win32.FILE_MAP_WRITE, 0, 0, size) orelse return switch (win32.GetLastError()) {
        win32.ERROR_ACCESS_DENIED => error.WrongSize,
        win32.ERROR_NOT_ENOUGH_MEMORY, win32.ERROR_OUTOFMEMORY => error.OutOfMemory,
        else => unexpected(@src(), "MapViewOfFile"),
    };
    return .{ .section = section, .events = events, .kind = kind, .base = @ptrCast(base), .size = size };
}

/// `VirtualProtect(PAGE_READONLY)` of the first `len` bytes of the view; out of resources, SystemResources.
pub fn protectReadOnly(mapping: *Mapping, len: usize) (ResourceError || UnexpectedError)!void {
    var old: win32.DWORD = undefined;
    if (win32.VirtualProtect(mapping.base, len, win32.PAGE_READONLY, &old) != 0) return;
    return switch (win32.GetLastError()) {
        win32.ERROR_NO_SYSTEM_RESOURCES, win32.ERROR_NOT_ENOUGH_MEMORY, win32.ERROR_OUTOFMEMORY => error.SystemResources,
        else => unexpected(@src(), "VirtualProtect"),
    };
}

/// `VirtualProtect(PAGE_NOACCESS)` of the whole view. It can't fail on a view of this process's own.
pub fn revoke(mapping: *Mapping) void {
    var old: win32.DWORD = undefined;
    if (win32.VirtualProtect(mapping.base, mapping.size, win32.PAGE_NOACCESS, &old) == 0) warnUnexpected(@src(), "VirtualProtect");
}

fn openFailure(comptime src: std.lang.SourceLocation, comptime call: []const u8) platform.OpenError {
    return switch (win32.GetLastError()) {
        win32.ERROR_FILE_NOT_FOUND => error.Gone,
        win32.ERROR_ACCESS_DENIED => error.AccessDenied,
        win32.ERROR_NO_SYSTEM_RESOURCES, win32.ERROR_NOT_ENOUGH_MEMORY, win32.ERROR_OUTOFMEMORY => error.SystemResources,
        else => unexpected(src, call),
    };
}

/// A session object's name (names.sessionObject) as a UTF-16 string.
pub const ObjectName = struct {
    builder: Utf16Builder(names.object_max) = .{},

    pub fn init(session_id: *const [16]u8, object: names.Object) ObjectName {
        var buf: [names.object_max]u8 = undefined;
        var name: ObjectName = .{};
        name.builder.ascii(names.sessionObject(&buf, session_id, object));
        return name;
    }

    fn initSlot(session_id: *const [16]u8, suffix: []const u8, slot: u32) ObjectName {
        var buf: [names.object_max]u8 = undefined;
        var name: ObjectName = .{};
        name.builder.ascii(names.slotObject(&buf, session_id, suffix, slot));
        return name;
    }

    pub fn string(name: *const ObjectName) [*:0]const u16 {
        return name.builder.string();
    }
};

/// A NUL-terminated UTF-16 string of at most `capacity` characters, built from ASCII (every name FastIPC makes) and
/// UTF-16 pieces.
pub fn Utf16Builder(comptime capacity: usize) type {
    return struct {
        buf: [capacity + 1]u16 = @splat(0),
        len: usize = 0,

        const Self = @This();

        pub fn ascii(builder: *Self, text: []const u8) void {
            for (text) |ch| builder.append(ch);
        }

        pub fn wide(builder: *Self, text: []const u16) void {
            for (text) |ch| builder.append(ch);
        }

        fn append(builder: *Self, ch: u16) void {
            builder.buf[builder.len] = ch; // callers stay within `capacity`
            builder.len += 1;
            builder.buf[builder.len] = 0;
        }

        pub fn string(builder: *const Self) [*:0]const u16 {
            return builder.buf[0..builder.len :0].ptr;
        }
    };
}

/// Logs an error the protocol doesn't expect, once per change of error at that place on this thread: a handshake
/// retries most calls every few milliseconds (core/log.zig `warnOnChange`).
pub fn unexpected(comptime src: std.lang.SourceLocation, comptime call: []const u8) UnexpectedError {
    warnUnexpected(src, call);
    return error.Unexpected;
}

pub fn warnUnexpected(comptime src: std.lang.SourceLocation, comptime call: []const u8) void {
    const code = win32.GetLastError();
    log.warnOnChange(src, code, call ++ " failed: error {d}", .{code});
}

// === Ring waits (stream/ring_wait.zig) ===

/// One of the session's four auto-reset events (`Mapping.events`); null without a session.
pub const RingEvent = ?Handle;
pub const no_ring_event: RingEvent = null;

/// Waits on the sleeper's event, which a publisher signals after it swapped the flag to 0 (docs/protocol.md §6.1); a
/// signal left from an earlier publish only wakes the next wait early. It looks at the flag `ring_spins` times first.
/// Without an event (a slot without its wake, or no session) it just sleeps, at most `eventless_sleep_ms`, and the
/// caller looks at the ring again.
pub fn ringWait(flag: *const std.atomic.Value(u32), event: RingEvent, timeout_ms: u32) bool {
    for (0..ring_spins) |_| {
        if (flag.load(.acquire) != 1) return false;
        std.atomic.spinLoopHint();
    }
    const event_handle = event orelse {
        win32.Sleep(@min(timeout_ms, eventless_sleep_ms));
        return timeout_ms <= eventless_sleep_ms;
    };
    return win32.WaitForSingleObject(event_handle, waitMs(timeout_ms)) == win32.WAIT_TIMEOUT;
}

/// How many times `ringWait` looks at the flag, with a spin-loop pause between looks, before it waits on the event. The
/// spin is far shorter than a wait and its wake, and it saves both when a publisher clears the flag meanwhile.
const ring_spins = 100;

/// The longest a wait without an event to wake it sleeps (`ringWait`, and `broadcastWait` after a failed wait): short,
/// since only the caller's next look at the ring or the word ends it early.
const eventless_sleep_ms = 10;

/// Signals the sleeper's event.
pub fn ringWake(flag: *const std.atomic.Value(u32), event: RingEvent) void {
    _ = flag;
    if (event) |e| _ = win32.SetEvent(e);
}

// === Broadcast waits (a broadcast channel's subscribers) ===

/// The writer sets the slot wake of each armed slot it took: a wake is never another subscriber's to take.
pub const wakes_each_slot = true;

/// A named auto-reset event of the slot (`names.slotObject`).
pub const SlotWake = Handle;

/// With the user-only security. A former subscriber of the slot may still hold its event open (ERROR_ALREADY_EXISTS):
/// it is taken as it is.
pub fn createSlotWake(id: *const [16]u8, suffix: []const u8, slot: u32, security: *const Security) (ResourceError || UnexpectedError)!SlotWake {
    const attributes = security.attributes();
    var name: ObjectName = .initSlot(id, suffix, slot);
    return win32.CreateEventW(&attributes, win32.FALSE, win32.FALSE, name.string()) orelse switch (win32.GetLastError()) {
        win32.ERROR_NO_SYSTEM_RESOURCES, win32.ERROR_NOT_ENOUGH_MEMORY, win32.ERROR_OUTOFMEMORY => error.SystemResources,
        else => unexpected(@src(), "CreateEventW"),
    };
}

/// To wait on it and to set it (the member's own interrupt, another slot's wake).
pub fn openSlotWake(id: *const [16]u8, suffix: []const u8, slot: u32) platform.OpenError!SlotWake {
    var name: ObjectName = .initSlot(id, suffix, slot);
    return win32.OpenEventW(win32.EVENT_MODIFY_STATE | win32.SYNCHRONIZE, win32.FALSE, name.string()) orelse
        openFailure(@src(), "OpenEventW");
}

/// The slot's event, which `ringWait` waits on and `ringWake` sets.
pub fn slotRingEvent(wake: ?SlotWake) RingEvent {
    return wake;
}

pub fn closeSlotWake(wake: SlotWake) void {
    _ = win32.CloseHandle(wake);
}

pub fn setSlotWake(wake: SlotWake) void {
    if (win32.SetEvent(wake) == 0) warnUnexpected(@src(), "SetEvent");
}

/// Waits on the slot's own event. The word isn't read: a revoke may have made the segment inaccessible since the
/// caller last looked at it, and a wake set since then is still there.
pub fn broadcastWait(word: *const std.atomic.Value(u32), expected: u32, own: SlotWake, timeout_ms: u32) bool {
    _ = .{ word, expected };
    return switch (win32.WaitForSingleObject(own, waitMs(timeout_ms))) {
        win32.WAIT_OBJECT_0 => false,
        win32.WAIT_TIMEOUT => true,
        else => {
            // WAIT_FAILED: a bad handle, the caller's bug; the caller looks again after a pause, not in a busy loop
            warnUnexpected(@src(), "WaitForSingleObject");
            win32.Sleep(@min(timeout_ms, eventless_sleep_ms));
            return false;
        },
    };
}

/// Nothing: the writer sets each armed slot's wake instead (`wakes_each_slot`).
pub fn broadcastWake(word: *const std.atomic.Value(u32)) void {
    _ = word;
}

// === For the tests' leak checks ===

pub fn openHandleCount() usize {
    var count: win32.DWORD = 0;
    std.debug.assert(win32.GetProcessHandleCount(win32.GetCurrentProcess(), &count) != 0);
    return count;
}

/// The threads whose owner is this process, in a snapshot of the system's (0 if the snapshot fails).
pub fn threadCount() usize {
    const snapshot = win32.CreateToolhelp32Snapshot(win32.TH32CS_SNAPTHREAD, 0);
    if (snapshot == win32.INVALID_HANDLE_VALUE) return 0;
    defer _ = win32.CloseHandle(snapshot);
    const self = win32.GetCurrentProcessId();
    var entry: win32.THREADENTRY32 = .{};
    var count: usize = 0;
    var more = win32.Thread32First(snapshot, &entry) != 0;
    while (more) : (more = win32.Thread32Next(snapshot, &entry) != 0) {
        if (entry.th32OwnerProcessID == self) count += 1;
    }
    return count;
}

/// A handle to the calling thread, which is signaled once the thread has exited.
pub const ThreadMark = struct {
    thread: ?Handle,

    pub fn ofCurrentThread() ThreadMark {
        return .{ .thread = win32.OpenThread(win32.SYNCHRONIZE, win32.FALSE, windows.GetCurrentThreadId()) };
    }

    pub fn exited(mark: ThreadMark) bool {
        return win32.WaitForSingleObject(mark.thread.?, 0) == win32.WAIT_OBJECT_0;
    }

    pub fn deinit(mark: ThreadMark) void {
        if (mark.thread) |thread| _ = win32.CloseHandle(thread);
    }
};

/// Every mapped view (`MEM_MAPPED`); the sections themselves are handles.
pub fn segmentMappingCount() usize {
    var count: usize = 0;
    var address: usize = 0;
    var info: win32.MEMORY_BASIC_INFORMATION = undefined;
    while (win32.VirtualQuery(@ptrFromInt(address), &info, @sizeOf(win32.MEMORY_BASIC_INFORMATION)) == @sizeOf(win32.MEMORY_BASIC_INFORMATION)) {
        if (info.Type == win32.MEM_MAPPED) count += 1;
        address = @intFromPtr(info.BaseAddress) + info.RegionSize;
    }
    return count;
}

pub fn protectionOf(address: *const anyopaque) platform.Protection {
    var info: win32.MEMORY_BASIC_INFORMATION = undefined;
    if (win32.VirtualQuery(address, &info, @sizeOf(win32.MEMORY_BASIC_INFORMATION)) != @sizeOf(win32.MEMORY_BASIC_INFORMATION)) return .none;
    if (info.State != win32.MEM_COMMIT) return .none;
    return switch (info.Protect) {
        win32.PAGE_NOACCESS => .none,
        win32.PAGE_READONLY => .read_only,
        win32.PAGE_READWRITE => .read_write,
        else => .other,
    };
}

// === Tests of what only Windows does (real system calls; platform/contract_test.zig tests the contract) ===

const testing = std.testing;
const contract_test = @import("../contract_test.zig");

test {
    _ = named_pipe; // their own tests
    _ = poller;
}

/// A session id unique to this process and to `fill`: the whole process id in its first four bytes, `fill` in the
/// rest. Each test passes a fill of its own.
pub fn testSessionId(fill: u8) [16]u8 {
    var session_id: [16]u8 = @splat(fill);
    std.mem.writeInt(u32, session_id[0..4], win32.GetCurrentProcessId(), .little);
    return session_id;
}

const handleCount = openHandleCount;

/// Checks an object's security: the owner is the user, the DACL holds one entry, allowing the user, and there is
/// no mandatory label (so the object counts as Medium integrity).
fn expectUserOnly(object: Handle, security: *const Security) !void {
    var owner: ?*win32.SID = null;
    var dacl: ?*win32.ACL = null;
    var descriptor: ?*anyopaque = null;
    try testing.expectEqual(@as(win32.DWORD, 0), win32.GetSecurityInfo(
        object,
        win32.SE_KERNEL_OBJECT,
        win32.OWNER_SECURITY_INFORMATION | win32.DACL_SECURITY_INFORMATION,
        &owner,
        null,
        &dacl,
        null,
        &descriptor,
    ));
    defer _ = win32.LocalFree(descriptor);
    try testing.expect(win32.EqualSid(owner.?, security.sidPtr()) != 0);
    var size: win32.ACL_SIZE_INFORMATION = undefined;
    try testing.expect(win32.GetAclInformation(dacl.?, &size, @sizeOf(win32.ACL_SIZE_INFORMATION), win32.AclSizeInformation) != 0);
    try testing.expectEqual(@as(win32.DWORD, 1), size.AceCount);
    var ace_ptr: ?*anyopaque = null;
    try testing.expect(win32.GetAce(dacl.?, 0, &ace_ptr) != 0);
    const ace: *win32.ACCESS_ALLOWED_ACE = @ptrCast(@alignCast(ace_ptr.?));
    try testing.expectEqual(win32.ACCESS_ALLOWED_ACE_TYPE, ace.AceType);
    try testing.expect(win32.EqualSid(@ptrCast(&ace.SidStart), security.sidPtr()) != 0);

    var sacl: ?*win32.ACL = null;
    var label_descriptor: ?*anyopaque = null;
    try testing.expectEqual(@as(win32.DWORD, 0), win32.GetSecurityInfo(object, win32.SE_KERNEL_OBJECT, win32.LABEL_SECURITY_INFORMATION, null, null, null, &sacl, &label_descriptor));
    defer _ = win32.LocalFree(label_descriptor);
    try testing.expectEqual(@as(?*win32.ACL, null), sacl);
}

fn expectNotInheritable(handle: Handle) !void {
    var flags: win32.DWORD = 0;
    try testing.expect(win32.GetHandleInformation(handle, &flags) != 0);
    try testing.expectEqual(@as(win32.DWORD, 0), flags & win32.HANDLE_FLAG_INHERIT);
}

test "every object gets the user-only security (owner and only DACL entry the user SID, no label); no handle is inheritable" {
    var security = try Security.init();
    defer security.deinit();
    const address = contract_test.testAddress("security");
    const pipe = (try claim(&address, &security)) orelse return error.TestUnexpectedResult;
    defer _ = win32.CloseHandle(pipe);
    try expectUserOnly(pipe, &security);
    try expectNotInheritable(pipe);
    const client = try contract_test.connected(try connect(&address, &security));
    defer _ = win32.CloseHandle(client);
    try expectNotInheritable(client);

    for ([_]platform.SegmentKind{ .stream, .broadcast, .queue, .service }, [_]u8{ 0x5e, 0x5f, 0x61, 0x62 }) |kind, fill| {
        const session_id = testSessionId(fill);
        var segment = try createSegment(null, &session_id, 64 * 1024, &security, kind);
        defer segment.unmap();
        try expectUserOnly(segment.section, &security);
        try expectNotInheritable(segment.section);
        for (segment.kindEvents()) |event| {
            try expectUserOnly(event, &security);
            try expectNotInheritable(event);
        }
        var opened = try openSegment(null, &session_id, 64 * 1024, kind);
        defer opened.unmap();
        try expectNotInheritable(opened.section);
        for (opened.kindEvents()) |event| try expectNotInheritable(event);
    }
    const session_id = testSessionId(0x60);
    const slot_wake = try createSlotWake(&session_id, "-bc-wake-", 1023, &security);
    defer closeSlotWake(slot_wake);
    try expectUserOnly(slot_wake, &security);
    try expectNotInheritable(slot_wake);
    const opened_wake = try openSlotWake(&session_id, "-bc-wake-", 1023);
    defer closeSlotWake(opened_wake);
    try expectNotInheritable(opened_wake);
}

test "a session's objects: created and opened by name, the same memory and events; collisions, gone, too small" {
    var security = try Security.init();
    defer security.deinit();
    const handles = handleCount();
    const session_id = testSessionId(0xc3);
    const size = 128 * 1024;
    {
        var created = try createSegment(null, &session_id, size, &security, .stream);
        defer created.unmap();
        var opened = try openSegment(null, &session_id, size, .stream);
        defer opened.unmap();
        created.base[1000] = 99;
        try testing.expectEqual(@as(u8, 99), opened.base[1000]);
        opened.base[size - 1] = 3;
        try testing.expectEqual(@as(u8, 3), created.base[size - 1]);
        for (created.kindEvents(), opened.kindEvents()) |mine, theirs| {
            try testing.expect(win32.SetEvent(mine) != 0);
            try testing.expectEqual(win32.WAIT_OBJECT_0, win32.WaitForSingleObject(theirs, 0));
            // Auto-reset: one wait consumed it
            try testing.expectEqual(win32.WAIT_TIMEOUT, win32.WaitForSingleObject(theirs, 0));
        }
        try testing.expectError(error.NameCollision, createSegment(null, &session_id, size, &security, .stream));
        try testing.expectError(error.WrongSize, openSegment(null, &session_id, 2 * size, .stream));
    }
    // The objects went away with their last handles
    try testing.expectError(error.Gone, openSegment(null, &session_id, size, .stream));
    try testing.expectEqual(handles, handleCount());
}
