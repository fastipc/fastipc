//! Windows's side of the OS contract (platform.zig): the named pipe that is the rendezvous (claim, connect with the
//! owner check, listen, disconnect), frames through `Io`, the pipe's peer process, the Terminal Services session, the
//! user-only security of every object, a session's section and four ring events, named after its random id, the cancel
//! signal (a manual-reset event), and the ring waits on those events. Every function is an instant call except the
//! handshake's waits on the caller's thread (`acceptWithin`, `readWithin`: an overlapped operation and a wait with a
//! timeout, cancelled and drained if it doesn't complete), the watcher's `read` and `sendFrame`, which block through
//! `Io`, so that `Future.cancel` ends them (`endTask`), and `ringWait`, the one wait `Io` can't express (an event shared
//! with the peer; data/ring_wait.zig slices it). Nothing here cancels or closes a handle another thread has an `Io`
//! operation on (that trips `Io.Threaded`).
//!
//! Every handle is created not inheritable, so a child process started with handle inheritance gets none.

const std = @import("std");
const windows = std.os.windows;
const Io = std.Io;
const log = @import("../log.zig");
const names = @import("../lifecycle/names.zig");
const platform = @import("../platform.zig");
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

    fn attributes(security: *const Security) win32.SECURITY_ATTRIBUTES {
        return .{
            .nLength = @sizeOf(win32.SECURITY_ATTRIBUTES),
            .lpSecurityDescriptor = security.descriptor,
            .bInheritHandle = win32.FALSE,
        };
    }

    /// The SID, for the calls that take one (they only read it).
    fn sidPtr(security: *const Security) *win32.SID {
        return @ptrCast(@constCast(&security.sid));
    }
};

/// An SRW lock.
pub const Lock = struct {
    raw: windows.SRWLOCK = .{},

    pub fn lock(l: *Lock) void {
        windows.ntdll.RtlAcquireSRWLockExclusive(&l.raw);
    }

    pub fn unlock(l: *Lock) void {
        windows.ntdll.RtlReleaseSRWLockExclusive(&l.raw);
    }
};

pub fn pid() u32 {
    return win32.GetCurrentProcessId();
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

/// Joins the workers, then waits until their threads have exited, those still starting included: the DLL may be
/// unloaded as soon as the last release returns, and each worker's handle to its own thread, which it closes as it
/// exits (after the join has returned), is closed by then. No task runs (every one was awaited), so no worker is
/// spawned meanwhile.
pub fn shutDownIo(threaded: *Io.Threaded) void {
    var exits: std.ArrayList(Handle) = .empty;
    defer exits.deinit(std.heap.c_allocator);
    // The workers' own list (`Io/Threaded.zig`, `worker`): each links itself in when it starts, and its node lives on
    // its stack, which stays valid until the join. A worker spawned for a task another worker took may still be
    // starting: wait until every worker spawned, the count of the pool's wait group (`WaitGroup`: its state counts
    // them in steps of 2, the low bit is the join's), is on the list.
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
const Owner = enum {
    /// This process's user.
    user,
    /// Another user, or an owner that can't be read for any reason but a disconnect: the check fails closed.
    foreign,
    /// A pipe whose server disconnected this client before its owner could be read.
    disconnected,
};

fn ownerOf(object: Handle, security: *const Security) Owner {
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

// === The rendezvous ===

/// A pipe path (names.windowsRendezvous).
pub const Address = struct {
    buf: [names.windows_max]u8,
    len: usize,

    pub fn path(a: *const Address) []const u8 {
        return a.buf[0..a.len];
    }
};

/// The name's pipe, in this process's Terminal Services session.
pub fn addressOf(name: []const u8) UnexpectedError!Address {
    var a: Address = undefined;
    a.len = names.windowsRendezvous(&a.buf, try sessionId(), name).len;
    return a;
}

/// This process's Terminal Services session: the scope of the rendezvous name.
fn sessionId() UnexpectedError!u32 {
    var session: win32.DWORD = 0;
    if (win32.ProcessIdToSessionId(win32.GetCurrentProcessId(), &session) == 0) return unexpected(@src(), "ProcessIdToSessionId");
    return session;
}

/// Claims the name: the pipe's first instance (`FILE_FLAG_FIRST_PIPE_INSTANCE`, byte mode, overlapped, remote clients
/// rejected), with the user-only security. Null when the name is held (ERROR_ACCESS_DENIED, which a second claim gets
/// while any instance exists; ERROR_PIPE_BUSY). A client connects on the instance itself, so a listener makes the next
/// (`nextListening`) before it hands a connection over: the name stays held throughout.
pub fn claim(address: *const Address, security: *const Security) (ResourceError || UnexpectedError)!?Handle {
    return createInstance(address, security, win32.FILE_FLAG_FIRST_PIPE_INSTANCE) catch |err| switch (err) {
        error.Held => null,
        else => |e| e,
    };
}

/// Another instance of a pipe the caller holds: the one a listener listens on next.
pub fn nextListening(address: *const Address, security: *const Security) (ResourceError || UnexpectedError)!?Handle {
    return createInstance(address, security, 0) catch |err| switch (err) {
        error.Held => error.SystemResources,
        else => |e| e,
    };
}

fn createInstance(address: *const Address, security: *const Security, first: win32.DWORD) (error{Held} || ResourceError || UnexpectedError)!Handle {
    var path: Utf16Builder(names.windows_max) = .{};
    path.ascii(address.path());
    const attributes = security.attributes();
    const pipe = win32.CreateNamedPipeW(
        path.string(),
        win32.PIPE_ACCESS_DUPLEX | first | win32.FILE_FLAG_OVERLAPPED,
        win32.PIPE_TYPE_BYTE_READMODE_BYTE_WAIT | win32.PIPE_REJECT_REMOTE_CLIENTS,
        pipe_unlimited_instances,
        4096,
        4096,
        0,
        &attributes,
    );
    if (pipe != win32.INVALID_HANDLE_VALUE) return pipe;
    return switch (win32.GetLastError()) {
        win32.ERROR_PIPE_BUSY, win32.ERROR_ACCESS_DENIED => error.Held,
        win32.ERROR_NO_SYSTEM_RESOURCES, win32.ERROR_NOT_ENOUGH_MEMORY, win32.ERROR_OUTOFMEMORY => error.SystemResources,
        else => unexpected(@src(), "CreateNamedPipeW"),
    };
}

/// `PIPE_UNLIMITED_INSTANCES`: a listener has two at a time, the accepted connection's and the next.
const pipe_unlimited_instances: win32.DWORD = 255;

/// Opens the pipe as its client (overlapped, not inheritable), then checks that its owner is this process's user:
/// a process of another user can make only a SID its token holds the owner (barring administrator privileges), so
/// it can't pass for the listener (`foreign`; also ERROR_ACCESS_DENIED, the pipe's DACL refusing this process).
/// `busy`: the one instance has a client or is between clients (ERROR_PIPE_BUSY), or the listener disconnected this
/// client before its owner could be checked, as one does with a connection it accepted and dropped at once. The
/// listener may identify the client, but not impersonate it (the default would let it).
pub fn connect(address: *const Address, security: *const Security) (ResourceError || UnexpectedError)!platform.Connect {
    var path: Utf16Builder(names.windows_max) = .{};
    path.ascii(address.path());
    const flags = win32.FILE_FLAG_OVERLAPPED | win32.SECURITY_SQOS_PRESENT | win32.SECURITY_IDENTIFICATION;
    const client = win32.CreateFileW(path.string(), win32.GENERIC_READ | win32.GENERIC_WRITE, 0, null, win32.OPEN_EXISTING, flags, null);
    if (client == win32.INVALID_HANDLE_VALUE) return switch (win32.GetLastError()) {
        win32.ERROR_FILE_NOT_FOUND => .no_listener,
        win32.ERROR_PIPE_BUSY => .busy,
        win32.ERROR_ACCESS_DENIED => .foreign,
        win32.ERROR_NO_SYSTEM_RESOURCES, win32.ERROR_NOT_ENOUGH_MEMORY, win32.ERROR_OUTOFMEMORY => error.SystemResources,
        else => unexpected(@src(), "CreateFileW"),
    };
    switch (ownerOf(client, security)) {
        .user => return .{ .connected = client },
        .foreign => {
            _ = win32.CloseHandle(client);
            return .foreign;
        },
        .disconnected => {
            _ = win32.CloseHandle(client);
            return .busy;
        },
    }
}

/// Waits for a client on the listening instance (`ConnectNamedPipe`), which becomes the connection. A client that came
/// and left before (ERROR_NO_DATA) and any other failure (logged) disconnect the instance, to listen on again.
pub fn acceptWithin(listening: Handle, cancel: ?Handle, timeout_ms: ?u32, _: ?*u32) platform.Waited(platform.Accept) {
    return switch (listenWithin(listening, cancel, timeout_ms)) {
        .idle => .idle,
        .cancelled => .cancelled,
        .done => |outcome| .{ .done = switch (outcome) {
            .connected => .{ .connection = listening },
            .closing => closing: {
                disconnect(listening);
                break :closing .closing;
            },
            .failed => failed: {
                disconnect(listening);
                break :failed .other;
            },
        } },
    };
}

/// Disconnects the accepted instance and keeps it, to listen on again: closing it could give up the name.
pub fn dropClient(connection: Handle, _: bool) ?Handle {
    disconnect(connection);
    return connection;
}

/// The pipe's security checked the user: its DACL admits only this user's clients, and `connect` checked its owner.
pub fn peer(connection: Handle, side: platform.Side) ?platform.Peer {
    return .{ .same_user = true, .pid = peerPid(connection, side) };
}

/// Nothing: a pipe handle can't be closed under an `Io` operation; `endTask` cancels the watcher instead.
pub fn unblock(_: Handle) void {}

/// Cancels the watcher, which ends its blocked pipe read (on a finished task it is the await).
pub fn endTask(io: Io, task: *Io.Future(void)) void {
    task.cancel(io);
}

pub fn close(handle: Handle) void {
    _ = win32.CloseHandle(handle);
}

const Listen = enum {
    /// A client is connected: it arrived during the listen, or before it (ERROR_PIPE_CONNECTED).
    connected,
    /// A client connected and left before the listen (ERROR_NO_DATA): disconnect, then listen again.
    closing,
    /// Any other failure (logged): drop the connection and back off.
    failed,
};

/// Waits for a client on the listener's instance (`ConnectNamedPipe`). A listen that is cancelled leaves the instance
/// listening: a client may connect to it meanwhile, and the next listen finds it (ERROR_PIPE_CONNECTED).
fn listenWithin(pipe: Handle, cancel: ?Handle, timeout_ms: ?u32) platform.Waited(Listen) {
    return switch (overlapped(pipe, cancel, timeout_ms, ConnectPipe{})) {
        .idle => .idle,
        .cancelled => .cancelled,
        .done => |completion| .{ .done = switch (completion) {
            .ok => .connected,
            .failed => |code| switch (code) {
                win32.ERROR_PIPE_CONNECTED => .connected,
                win32.ERROR_NO_DATA => .closing,
                else => failed: {
                    log.warn(@src(), "ConnectNamedPipe failed: error {d}", .{code});
                    break :failed .failed;
                },
            },
        } },
    };
}

const ConnectPipe = struct {
    fn start(_: ConnectPipe, pipe: Handle, ov: *win32.OVERLAPPED) win32.BOOL {
        return win32.ConnectNamedPipe(pipe, ov);
    }
};

const ReadPipe = struct {
    buf: []u8,

    fn start(r: ReadPipe, pipe: Handle, ov: *win32.OVERLAPPED) win32.BOOL {
        return win32.ReadFile(pipe, r.buf.ptr, @intCast(@min(r.buf.len, std.math.maxInt(win32.DWORD))), null, ov);
    }
};

/// How an overlapped operation completed: the bytes it moved, or its error.
const Completion = union(enum) { ok: win32.DWORD, failed: win32.DWORD };

/// Runs one overlapped operation (`op.start`) on `handle` and waits up to `timeout_ms` (null: no limit) for it to
/// complete, or for the cancel signal. If it hasn't completed by then, it is cancelled (`CancelIoEx`) and the
/// cancellation awaited, so no I/O is left in flight when this returns and the `OVERLAPPED` may live on this frame. An
/// operation that completed just as it was cancelled counts: a client that connected, or bytes that arrived, are
/// never lost.
fn overlapped(handle: Handle, cancel: ?Handle, timeout_ms: ?u32, op: anytype) platform.Waited(Completion) {
    const event = win32.CreateEventW(null, win32.TRUE, win32.FALSE, null) orelse
        return .{ .done = .{ .failed = win32.GetLastError() } };
    defer _ = win32.CloseHandle(event);
    var ov: win32.OVERLAPPED = .{ .hEvent = event };
    var n: win32.DWORD = 0;
    if (op.start(handle, &ov) != 0) {
        _ = win32.GetOverlappedResult(handle, &ov, &n, win32.TRUE);
        return .{ .done = .{ .ok = n } };
    }
    const started = win32.GetLastError();
    if (started != win32.ERROR_IO_PENDING) return .{ .done = .{ .failed = started } };
    const handles = [2]Handle{ event, cancel orelse event };
    const count: win32.DWORD = if (cancel != null) 2 else 1;
    const ms: win32.DWORD = if (timeout_ms) |t| @min(t, win32.INFINITE - 1) else win32.INFINITE;
    const waited = win32.WaitForMultipleObjects(count, &handles, win32.FALSE, ms);
    const cancelled = waited == win32.WAIT_OBJECT_0 + 1;
    if (waited != win32.WAIT_OBJECT_0) _ = win32.CancelIoEx(handle, &ov);
    if (win32.GetOverlappedResult(handle, &ov, &n, win32.TRUE) != 0) return .{ .done = .{ .ok = n } };
    const code = win32.GetLastError();
    if (code == win32.ERROR_OPERATION_ABORTED) return if (cancelled) .cancelled else .idle;
    return .{ .done = .{ .failed = code } };
}

/// Reads what arrives within `timeout_ms` (null: no limit), or until the cancel signal. 0: the connection ended (the
/// other end closed: ERROR_BROKEN_PIPE), or the read failed.
pub fn readWithin(pipe: Handle, buf: []u8, cancel: ?Handle, timeout_ms: ?u32) platform.Waited(usize) {
    return switch (overlapped(pipe, cancel, timeout_ms, ReadPipe{ .buf = buf })) {
        .idle => .idle,
        .cancelled => .cancelled,
        .done => |completion| .{ .done = switch (completion) {
            .ok => |n| n,
            .failed => 0,
        } },
    };
}

// === The cancel signal ===

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
    return win32.WaitForSingleObject(event, @min(timeout_ms, win32.INFINITE - 1)) == win32.WAIT_OBJECT_0;
}

pub fn sleep(ms: u32) void {
    win32.Sleep(@min(ms, win32.INFINITE - 1));
}

/// Disconnects the listener's instance from its client, keeping the instance: closing it would give up the name.
fn disconnect(pipe: Handle) void {
    if (win32.DisconnectNamedPipe(pipe) == 0) log.warn(@src(), "DisconnectNamedPipe failed: error {d}", .{win32.GetLastError()});
}

/// The process at the other end: the listener asks for its client, the client for its server. Null if the system
/// can't tell.
fn peerPid(pipe: Handle, side: platform.Side) ?u32 {
    var id: u32 = 0;
    const ok = switch (side) {
        .listener => win32.GetNamedPipeClientProcessId(pipe, &id),
        .client => win32.GetNamedPipeServerProcessId(pipe, &id),
    };
    return if (ok != 0) id else null;
}

// === Frames ===

/// Writes the frame through `io`. The segment is found by name: nothing is attached.
pub fn sendFrame(io: Io, connection: Handle, frame: []const u8, attached: ?Handle) Io.Cancelable!bool {
    std.debug.assert(attached == null);
    return write(io, connection, frame);
}

/// Reads through `io` into `buf`, blocking until at least one byte arrives. 0: the connection ended (the other end
/// closed: STATUS_PIPE_BROKEN) or the read failed, which the watcher treats alike.
pub fn read(io: Io, pipe: Handle, buf: []u8) Io.Cancelable!usize {
    const result = try io.operate(.{ .file_read_streaming = .{ .file = file(pipe), .data = &.{buf} } });
    return result.file_read_streaming catch 0;
}

/// Writes all of `bytes` through `io`. False: the connection is gone, or the write failed.
fn write(io: Io, pipe: Handle, bytes: []const u8) Io.Cancelable!bool {
    var rest = bytes;
    while (rest.len > 0) {
        const result = try io.operate(.{ .file_write_streaming = .{ .file = file(pipe), .data = &.{rest} } });
        const written = result.file_write_streaming catch return false;
        if (written == 0) return false;
        rest = rest[written..];
    }
    return true;
}

/// A pipe handle as an `Io.File`: opened overlapped, which `Io.Threaded` calls non-blocking.
fn file(pipe: Handle) Io.File {
    return .{ .handle = pipe, .flags = .{ .nonblocking = true } };
}

// === The segment ===

/// One session's objects: its section, mapped, and its four ring events, in `names.Object` order after the
/// section (s2c data, s2c space, c2s data, c2s space). The objects go away with their last handle, in any process.
pub const Mapping = struct {
    section: Handle,
    events: [4]Handle,
    base: [*]u8,
    size: usize,

    pub fn unmap(m: *Mapping) void {
        _ = win32.UnmapViewOfFile(m.base);
        for (m.events) |event| _ = win32.CloseHandle(event);
        _ = win32.CloseHandle(m.section);
        m.* = undefined;
    }

    pub fn ringEvent(m: *const Mapping, object: names.Object) RingEvent {
        return m.events[
            switch (object) {
                .s2c_data => 0,
                .s2c_space => 1,
                .c2s_data => 2,
                .c2s_space => 3,
                .section => unreachable,
            }
        ];
    }
};

const event_objects = [4]names.Object{ .s2c_data, .s2c_space, .c2s_data, .c2s_space };

/// Creates session `id`'s objects with the user-only security: the section of `size` bytes, committed at creation
/// (so the commit limit is NO_MEMORY now, not a fault later), and the four auto-reset events; then maps the
/// section. NameCollision: an object of the session's name exists. No handle is passed: `handle` is null.
pub fn createSegment(handle: ?Handle, id: *const [16]u8, size: usize, security: *const Security) platform.CreateError!Mapping {
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
    for (&events, event_objects) |*event, object| {
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
    return .{ .section = section, .events = events, .base = @ptrCast(base), .size = size };
}

/// Opens session `id`'s objects by name, as the client does with the id a SEGMENT frame names, and maps `size` bytes
/// of the section. Gone: an object no longer exists (the listener left after sending SEGMENT). AccessDenied: the DACL
/// of the section or an event refused this process. WrongSize: the section is smaller than `size`, so the view is
/// refused (ERROR_ACCESS_DENIED from `MapViewOfFile`, not from the open). No handle was received: `received` is null.
pub fn openSegment(received: ?Handle, id: *const [16]u8, size: usize) platform.OpenError!Mapping {
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
    for (&events, event_objects) |*event, object| {
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
    return .{ .section = section, .events = events, .base = @ptrCast(base), .size = size };
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
const ObjectName = struct {
    builder: Utf16Builder(names.object_max) = .{},

    fn init(sid: *const [16]u8, object: names.Object) ObjectName {
        var buf: [names.object_max]u8 = undefined;
        var name: ObjectName = .{};
        name.builder.ascii(names.sessionObject(&buf, sid, object));
        return name;
    }

    fn string(name: *const ObjectName) [*:0]const u16 {
        return name.builder.string();
    }
};

/// A NUL-terminated UTF-16 string of at most `capacity` characters, built from ASCII (every name FastIPC makes) and
/// UTF-16 pieces.
fn Utf16Builder(comptime capacity: usize) type {
    return struct {
        buf: [capacity + 1]u16 = @splat(0),
        len: usize = 0,

        const Self = @This();

        fn ascii(builder: *Self, s: []const u8) void {
            for (s) |ch| builder.append(ch);
        }

        fn wide(builder: *Self, s: []const u16) void {
            for (s) |ch| builder.append(ch);
        }

        fn append(builder: *Self, ch: u16) void {
            builder.buf[builder.len] = ch; // callers stay within `capacity`
            builder.len += 1;
            builder.buf[builder.len] = 0;
        }

        fn string(builder: *const Self) [*:0]const u16 {
            return builder.buf[0..builder.len :0].ptr;
        }
    };
}

/// Logs an error the protocol doesn't expect, once per change of error at that place on this thread: a handshake
/// retries most calls every few milliseconds (log.zig `warnOnChange`).
fn unexpected(comptime src: std.lang.SourceLocation, comptime call: []const u8) UnexpectedError {
    const code = win32.GetLastError();
    log.warnOnChange(src, code, call ++ " failed: error {d}", .{code});
    return error.Unexpected;
}

// === Ring waits (data/ring_wait.zig) ===

/// One of the session's four auto-reset events (`Mapping.events`); null without a session.
pub const RingEvent = ?Handle;
pub const no_ring_event: RingEvent = null;

/// Waits on the sleeper's event, which a publisher signals after it swapped the flag to 0 (docs/protocol.md §6.1); a
/// signal left from an earlier publish only wakes the next wait early. A few spins first catch a publisher that has
/// just cleared the flag. Without an event it just sleeps, at most 10 ms.
pub fn ringWait(flag: *const std.atomic.Value(u32), event: RingEvent, timeout_ms: u32) bool {
    for (0..100) |_| {
        if (flag.load(.acquire) != 1) return false;
        std.atomic.spinLoopHint();
    }
    const e = event orelse {
        win32.Sleep(@min(timeout_ms, 10));
        return timeout_ms <= 10;
    };
    return win32.WaitForSingleObject(e, timeout_ms) == win32.WAIT_TIMEOUT;
}

/// Signals the sleeper's event.
pub fn ringWake(flag: *const std.atomic.Value(u32), event: RingEvent) void {
    _ = flag;
    if (event) |e| _ = win32.SetEvent(e);
}

// === For the tests' leak checks ===

pub fn openHandleCount() usize {
    var count: win32.DWORD = 0;
    std.debug.assert(win32.GetProcessHandleCount(win32.GetCurrentProcess(), &count) != 0);
    return count;
}

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

// === Tests (real system calls; each test's names are unique to its process) ===

const testing = std.testing;
const process_state = @import("../process_state.zig");

/// A pipe address unique to this process and call.
fn testAddress(comptime tag: []const u8) Address {
    const counter = struct {
        var next = std.atomic.Value(u32).init(0);
    };
    var name_buf: [64]u8 = undefined;
    const name = std.mem.print(&name_buf, "test-" ++ tag ++ "-{d}-{d}", .{ win32.GetCurrentProcessId(), counter.next.fetchAdd(1, .monotonic) }) catch unreachable;
    return addressOf(name) catch unreachable;
}

/// A session id unique to this process and to `fill`: the whole process id in its first four bytes, `fill` in the
/// rest. Each test passes a fill of its own.
fn testSessionId(fill: u8) [16]u8 {
    var sid: [16]u8 = @splat(fill);
    std.mem.writeInt(u32, sid[0..4], win32.GetCurrentProcessId(), .little);
    return sid;
}

fn connected(outcome: platform.Connect) !Handle {
    return switch (outcome) {
        .connected => |client| client,
        else => error.TestUnexpectedResult,
    };
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
    const address = testAddress("security");
    const pipe = (try claim(&address, &security)) orelse return error.TestUnexpectedResult;
    defer _ = win32.CloseHandle(pipe);
    try expectUserOnly(pipe, &security);
    try expectNotInheritable(pipe);
    const client = try connected(try connect(&address, &security));
    defer _ = win32.CloseHandle(client);
    try expectNotInheritable(client);

    const sid = testSessionId(0x5e);
    var segment = try createSegment(null, &sid, 64 * 1024, &security);
    defer segment.unmap();
    try expectUserOnly(segment.section, &security);
    try expectNotInheritable(segment.section);
    for (segment.events) |event| {
        try expectUserOnly(event, &security);
        try expectNotInheritable(event);
    }
    var opened = try openSegment(null, &sid, 64 * 1024);
    defer opened.unmap();
    try expectNotInheritable(opened.section);
    for (opened.events) |event| try expectNotInheritable(event);
}

test "claim: one claim per name (a second claim is refused), free again at once when it closes" {
    var security = try Security.init();
    defer security.deinit();
    const handles = handleCount();
    const address = testAddress("claim");
    const first = (try claim(&address, &security)) orelse return error.TestUnexpectedResult;
    try testing.expectEqual(@as(?Handle, null), try claim(&address, &security));
    _ = win32.CloseHandle(first);
    const again = (try claim(&address, &security)) orelse return error.TestUnexpectedResult;
    _ = win32.CloseHandle(again);
    try testing.expectEqual(handles, handleCount());
}

fn claimRacer(address: *const Address, security: *const Security, start: *std.atomic.Value(u32), claimed: *?Handle) void {
    while (start.load(.acquire) == 0) std.atomic.spinLoopHint();
    claimed.* = claim(address, security) catch null;
}

test "claim race: of several threads creating one pipe name at once, exactly one gets the instance" {
    var security = try Security.init();
    defer security.deinit();
    const handles = handleCount();
    for (0..20) |_| {
        const address = testAddress("race");
        var start = std.atomic.Value(u32).init(0);
        var claimed: [8]?Handle = @splat(null);
        var threads: [8]std.Thread = undefined;
        for (&threads, &claimed) |*thread, *slot| thread.* = try std.Thread.spawn(.{}, claimRacer, .{ &address, &security, &start, slot });
        start.store(1, .release);
        for (threads) |thread| thread.join();
        var winners: usize = 0;
        for (claimed) |slot| if (slot) |pipe| {
            winners += 1;
            _ = win32.CloseHandle(pipe);
        };
        try testing.expectEqual(@as(usize, 1), winners);
    }
    try testing.expectEqual(handles, handleCount());
}

test "connect: no listener, connected past the owner check, busy while the instance has a client" {
    var security = try Security.init();
    defer security.deinit();
    const handles = handleCount();
    const address = testAddress("connect");
    try testing.expectEqual(platform.Connect.no_listener, try connect(&address, &security));
    const pipe = (try claim(&address, &security)) orelse return error.TestUnexpectedResult;
    const client = switch (try connect(&address, &security)) {
        .connected => |h| h,
        else => return error.TestUnexpectedResult,
    };
    try testing.expectEqual(platform.Connect.busy, try connect(&address, &security));
    try testing.expectEqual(win32.GetCurrentProcessId(), peerPid(pipe, .listener).?);
    try testing.expectEqual(win32.GetCurrentProcessId(), peerPid(client, .client).?);
    _ = win32.CloseHandle(client);
    _ = win32.CloseHandle(pipe);
    try testing.expectEqual(handles, handleCount());
}

const Listened = platform.Waited(Listen);

/// Connects to `address` again and again while the instance is busy (its listen hasn't begun yet), for up to 1 s.
fn connectWhileBusy(address: *const Address, security: *const Security, out: *?Handle) void {
    for (0..200) |_| switch (connect(address, security) catch return) {
        .connected => |h| {
            out.* = h;
            return;
        },
        .busy => win32.Sleep(5),
        else => return,
    };
}

test "listen: a client that came first, one that came and left (ERROR_NO_DATA), one during the listen" {
    var security = try Security.init();
    defer security.deinit();
    const address = testAddress("listen");

    // A client can open a new instance before its first listen (between the listener's claim and listen). If it
    // is still there, the listen finds it: ERROR_PIPE_CONNECTED
    {
        const pipe = (try claim(&address, &security)) orelse return error.TestUnexpectedResult;
        defer _ = win32.CloseHandle(pipe);
        const early = try connected(try connect(&address, &security));
        try testing.expectEqual(Listened{ .done = .connected }, listenWithin(pipe, null, 0));
        _ = win32.CloseHandle(early);
    }
    // If it has already left (killed, say): ERROR_NO_DATA, after which the listener disconnects and listens again.
    // Meanwhile a second client finds the name held (ERROR_PIPE_BUSY) and connects to the listen in progress. (After a
    // disconnect, the instance takes no client until the next listen begins.)
    const pipe = (try claim(&address, &security)) orelse return error.TestUnexpectedResult;
    defer _ = win32.CloseHandle(pipe);
    const gone = try connected(try connect(&address, &security));
    _ = win32.CloseHandle(gone);
    try testing.expectEqual(Listened{ .done = .closing }, listenWithin(pipe, null, 0));
    disconnect(pipe);
    try testing.expectEqual(platform.Connect.busy, try connect(&address, &security));

    var client: ?Handle = null;
    const thread = try std.Thread.spawn(.{}, connectWhileBusy, .{ &address, &security, &client });
    const listened = listenWithin(pipe, null, 5000);
    thread.join();
    try testing.expectEqual(@as(?Handle, null), try claim(&address, &security));
    try testing.expect(client != null);
    _ = win32.CloseHandle(client.?);
    try testing.expectEqual(Listened{ .done = .connected }, listened);
    disconnect(pipe);
}

test "a listen that times out leaves the instance listening: a client that connects meanwhile is found by the next one" {
    var security = try Security.init();
    defer security.deinit();
    const handles = handleCount();
    const address = testAddress("polled");
    const pipe = (try claim(&address, &security)) orelse return error.TestUnexpectedResult;
    // A served client disconnected: the instance takes no client until a listen begins
    {
        const first = try connected(try connect(&address, &security));
        try testing.expectEqual(Listened{ .done = .connected }, listenWithin(pipe, null, 0));
        _ = win32.CloseHandle(first);
        disconnect(pipe);
    }
    // A listen that only looks, then one that waits a moment: each is cancelled and drained, and leaves the instance
    // listening, so a polling listener (accept with timeout 0) lets a client in whenever it comes
    for (0..3) |round| {
        try testing.expectEqual(Listened.idle, listenWithin(pipe, null, 0));
        try testing.expectEqual(Listened.idle, listenWithin(pipe, null, 10));
        const client = try connected(try connect(&address, &security));
        try testing.expectEqual(Listened{ .done = .connected }, listenWithin(pipe, null, 0));
        // The connection works: bytes cross it both ways
        const frame = @as([64]u8, @splat(@intCast(round)));
        var buf: [64]u8 = undefined;
        try testing.expect(try sendFrame(testing.io, client, &frame, null));
        try testing.expectEqual(platform.Waited(usize){ .done = 64 }, readWithin(pipe, &buf, null, 5000));
        try testing.expectEqualSlices(u8, &frame, &buf);
        _ = win32.CloseHandle(client);
        disconnect(pipe);
    }
    _ = win32.CloseHandle(pipe);
    try testing.expectEqual(handles, handleCount());
}

test "the cancel signal ends a listen and a read, wins once set, and a read that times out loses no byte" {
    var security = try Security.init();
    defer security.deinit();
    const handles = handleCount();
    const address = testAddress("cancel");
    const pipe = (try claim(&address, &security)) orelse return error.TestUnexpectedResult;
    const cancel = try createCancelSignal();
    // Bytes that arrive after a read timed out wait for the next read
    const client = try connected(try connect(&address, &security));
    try testing.expectEqual(platform.Waited(platform.Accept){ .done = .{ .connection = pipe } }, acceptWithin(pipe, cancel, 0, null));
    var buf: [64]u8 = undefined;
    for (0..20) |i| {
        try testing.expectEqual(platform.Waited(usize).idle, readWithin(pipe, &buf, cancel, 0));
        const byte = [1]u8{@intCast(i)};
        try testing.expect(try sendFrame(testing.io, client, &byte, null));
        try testing.expectEqual(platform.Waited(usize){ .done = 1 }, readWithin(pipe, &buf, cancel, 5000));
        try testing.expectEqual(byte[0], buf[0]);
    }
    // The signal ends a read that waits for ever, and stays set
    const Setter = struct {
        fn run(signal: Handle) void {
            win32.Sleep(20);
            setCancelSignal(signal);
        }
    };
    const setter = try std.Thread.spawn(.{}, Setter.run, .{cancel});
    try testing.expectEqual(platform.Waited(usize).cancelled, readWithin(pipe, &buf, cancel, null));
    setter.join();
    try testing.expect(waitCancelSignal(cancel, 0));
    _ = win32.CloseHandle(client);
    disconnect(pipe);
    try testing.expectEqual(platform.Waited(platform.Accept).cancelled, acceptWithin(pipe, cancel, null, null));
    // The end of the stream
    const again = try connected(try connect(&address, &security));
    try testing.expectEqual(platform.Waited(platform.Accept){ .done = .{ .connection = pipe } }, acceptWithin(pipe, null, 0, null));
    _ = win32.CloseHandle(again);
    try testing.expectEqual(platform.Waited(usize){ .done = 0 }, readWithin(pipe, &buf, null, 5000));
    _ = win32.CloseHandle(cancel);
    _ = win32.CloseHandle(pipe);
    try testing.expectEqual(handles, handleCount());
}

test "a client the listener disconnects before its owner check is busy, and gets in at the next listen; a failed owner query fails closed" {
    var security = try Security.init();
    defer security.deinit();
    const address = testAddress("dropped");
    const pipe = (try claim(&address, &security)) orelse return error.TestUnexpectedResult;
    defer _ = win32.CloseHandle(pipe);
    // `connect`'s two steps, with the listener's accept and drop between them
    var path16: Utf16Builder(names.windows_max) = .{};
    path16.ascii(address.path());
    const client = win32.CreateFileW(path16.string(), win32.GENERIC_READ | win32.GENERIC_WRITE, 0, null, win32.OPEN_EXISTING, win32.FILE_FLAG_OVERLAPPED, null);
    try testing.expect(client != win32.INVALID_HANDLE_VALUE);
    try testing.expectEqual(Listened{ .done = .connected }, listenWithin(pipe, null, 0));
    disconnect(pipe);
    try testing.expectEqual(Owner.disconnected, ownerOf(client, &security));
    _ = win32.CloseHandle(client);

    // The client's next attempt reaches the listener's next listen
    var retried: ?Handle = null;
    const thread = try std.Thread.spawn(.{}, connectWhileBusy, .{ &address, &security, &retried });
    const listened = listenWithin(pipe, null, 5000);
    thread.join();
    try testing.expect(retried != null);
    try testing.expectEqual(Listened{ .done = .connected }, listened);
    _ = win32.CloseHandle(retried.?);
    disconnect(pipe);

    // Any other failed query is foreign: a handle without READ_CONTROL can't read the owner
    const sid = testSessionId(0x38);
    var name: ObjectName = .init(&sid, .s2c_data);
    const attributes = security.attributes();
    const event = win32.CreateEventW(&attributes, win32.FALSE, win32.FALSE, name.string()) orelse return error.TestUnexpectedResult;
    defer _ = win32.CloseHandle(event);
    const synchronize_only = win32.OpenEventW(win32.SYNCHRONIZE, win32.FALSE, name.string()) orelse return error.TestUnexpectedResult;
    defer _ = win32.CloseHandle(synchronize_only);
    try testing.expectEqual(Owner.foreign, ownerOf(synchronize_only, &security));
    try testing.expectEqual(Owner.user, ownerOf(event, &security));
}

test "frames go both ways through Io, and a read after the other end closed is the end of the stream" {
    const ref = try process_state.acquire();
    defer process_state.release(ref);
    var security = try Security.init();
    defer security.deinit();
    const address = testAddress("frames");
    const pipe = (try claim(&address, &security)) orelse return error.TestUnexpectedResult;
    defer _ = win32.CloseHandle(pipe);
    const client = try connected(try connect(&address, &security));
    try testing.expectEqual(Listened{ .done = .connected }, listenWithin(pipe, null, 0));

    const segment_frame = @as([64]u8, @splat(0x11));
    const ready_frame = @as([64]u8, @splat(0x22));
    var buf: [64]u8 = undefined;
    try testing.expect(try write(ref.io, pipe, &segment_frame));
    try testing.expectEqual(@as(usize, 64), try read(ref.io, client, &buf));
    try testing.expectEqualSlices(u8, &segment_frame, &buf);
    try testing.expect(try write(ref.io, client, &ready_frame));
    try testing.expectEqual(@as(usize, 64), try read(ref.io, pipe, &buf));
    try testing.expectEqualSlices(u8, &ready_frame, &buf);

    _ = win32.CloseHandle(client);
    try testing.expectEqual(@as(usize, 0), try read(ref.io, pipe, &buf));
}

test "the listener may identify its client but not impersonate it" {
    const ref = try process_state.acquire();
    defer process_state.release(ref);
    var security = try Security.init();
    defer security.deinit();
    const address = testAddress("identify");
    const pipe = (try claim(&address, &security)) orelse return error.TestUnexpectedResult;
    defer _ = win32.CloseHandle(pipe);
    const client = try connected(try connect(&address, &security));
    defer _ = win32.CloseHandle(client);
    try testing.expectEqual(Listened{ .done = .connected }, listenWithin(pipe, null, 0));
    // A server impersonates its client only after reading from the pipe
    const frame = @as([64]u8, @splat(0x22));
    var buf: [64]u8 = undefined;
    try testing.expect(try write(ref.io, client, &frame));
    try testing.expectEqual(@as(usize, 64), try read(ref.io, pipe, &buf));
    try testing.expect(win32.ImpersonateNamedPipeClient(pipe) != 0);
    defer _ = win32.RevertToSelf();
    var token: Handle = undefined;
    try testing.expect(win32.OpenThreadToken(win32.GetCurrentThread(), win32.TOKEN_QUERY, win32.TRUE, &token) != 0);
    defer _ = win32.CloseHandle(token);
    var level: u32 = 0;
    var len: win32.DWORD = 0;
    try testing.expect(win32.GetTokenInformation(token, win32.TokenImpersonationLevel, &level, @sizeOf(u32), &len) != 0);
    try testing.expectEqual(@as(u32, 1), level); // SecurityIdentification; the default is SecurityImpersonation (2)
}

test "a read in progress ends with error.Canceled on Future.cancel, on the process-wide Io (the watcher's stop rule)" {
    const ref = try process_state.acquire();
    defer process_state.release(ref);
    var security = try Security.init();
    defer security.deinit();
    const address = testAddress("stop");
    const pipe = (try claim(&address, &security)) orelse return error.TestUnexpectedResult;
    defer _ = win32.CloseHandle(pipe);
    const client = try connected(try connect(&address, &security));
    defer _ = win32.CloseHandle(client);
    var buf: [64]u8 = undefined;
    var reading = try ref.io.concurrent(read, .{ ref.io, client, @as([]u8, &buf) });
    try testing.io.sleep(.fromMilliseconds(20), .awake);
    try testing.expectError(error.Canceled, reading.cancel(ref.io));
}

test "a session's objects: created and opened by name, the same memory and events; collisions, gone, too small" {
    var security = try Security.init();
    defer security.deinit();
    const handles = handleCount();
    const sid = testSessionId(0xc3);
    const size = 128 * 1024;
    {
        var created = try createSegment(null, &sid, size, &security);
        defer created.unmap();
        var opened = try openSegment(null, &sid, size);
        defer opened.unmap();
        created.base[1000] = 99;
        try testing.expectEqual(@as(u8, 99), opened.base[1000]);
        opened.base[size - 1] = 3;
        try testing.expectEqual(@as(u8, 3), created.base[size - 1]);
        for (created.events, opened.events) |mine, theirs| {
            try testing.expect(win32.SetEvent(mine) != 0);
            try testing.expectEqual(win32.WAIT_OBJECT_0, win32.WaitForSingleObject(theirs, 0));
            // Auto-reset: one wait consumed it
            try testing.expectEqual(win32.WAIT_TIMEOUT, win32.WaitForSingleObject(theirs, 0));
        }
        try testing.expectError(error.NameCollision, createSegment(null, &sid, size, &security));
        try testing.expectError(error.WrongSize, openSegment(null, &sid, 2 * size));
    }
    // The objects went away with their last handles
    try testing.expectError(error.Gone, openSegment(null, &sid, size));
    try testing.expectEqual(handles, handleCount());
}

test "the pipe-owner check refuses a pipe that another owner created (needs an elevated process)" {
    // An elevated token may make the Administrators group an object's owner; a process of another user could make
    // only a SID of its own token the owner. Without elevation the test is skipped.
    var administrators: ?*win32.SID = null;
    try testing.expect(win32.ConvertStringSidToSidW(std.unicode.utf8ToUtf16LeStringLiteral("S-1-5-32-544"), &administrators) != 0);
    defer _ = win32.LocalFree(administrators);
    var elevated: win32.BOOL = 0;
    try testing.expect(win32.CheckTokenMembership(null, administrators.?, &elevated) != 0);
    if (elevated == 0) return error.SkipZigTest;

    var security = try Security.init();
    defer security.deinit();
    var sid_string: ?[*:0]u16 = null;
    try testing.expect(win32.ConvertSidToStringSidW(security.sidPtr(), &sid_string) != 0);
    defer _ = win32.LocalFree(sid_string);
    var sddl: Utf16Builder(2 * 184 + 20) = .{};
    sddl.ascii("O:BAD:P(A;;GA;;;");
    sddl.wide(std.mem.span(sid_string.?));
    sddl.ascii(")");
    var descriptor: ?*anyopaque = null;
    try testing.expect(win32.ConvertStringSecurityDescriptorToSecurityDescriptorW(sddl.string(), win32.SDDL_REVISION_1, &descriptor, null) != 0);
    defer _ = win32.LocalFree(descriptor);
    const foreign_attributes: win32.SECURITY_ATTRIBUTES = .{ .nLength = @sizeOf(win32.SECURITY_ATTRIBUTES), .lpSecurityDescriptor = descriptor, .bInheritHandle = win32.FALSE };

    const address = testAddress("foreign");
    var path16: Utf16Builder(names.windows_max) = .{};
    path16.ascii(address.path());
    const pipe = win32.CreateNamedPipeW(
        path16.string(),
        win32.PIPE_ACCESS_DUPLEX | win32.FILE_FLAG_FIRST_PIPE_INSTANCE | win32.FILE_FLAG_OVERLAPPED,
        win32.PIPE_REJECT_REMOTE_CLIENTS,
        1,
        4096,
        4096,
        0,
        &foreign_attributes,
    );
    try testing.expect(pipe != win32.INVALID_HANDLE_VALUE);
    defer _ = win32.CloseHandle(pipe);
    try testing.expectEqual(platform.Connect.foreign, try connect(&address, &security));
}

/// A task that holds a worker until `gate` opens, and records a handle that is signaled once its thread has exited.
const Worker = struct {
    exited: ?Handle = null,

    fn run(worker: *Worker, io: Io, gate: *std.atomic.Value(u32)) void {
        worker.exited = win32.OpenThread(win32.SYNCHRONIZE, win32.FALSE, windows.GetCurrentThreadId());
        while (gate.load(.acquire) == 0) io.futexWaitUncancelable(u32, &gate.raw, 0);
    }

    fn gone(worker: Worker) bool {
        return win32.WaitForSingleObject(worker.exited.?, 0) == win32.WAIT_OBJECT_0;
    }
};

fn recordThread(id: *std.Thread.Id) void {
    id.* = std.Thread.getCurrentId();
}

test "no library thread and no handle are left after the process-wide Io's last release, and a new instance works" {
    // Warm up, so that what the process creates once (thread-pool bookkeeping of the C runtime, loaded DLLs) is in
    // the baseline
    {
        const ref = try process_state.acquire();
        var id: std.Thread.Id = 0;
        var task = try ref.io.concurrent(recordThread, .{&id});
        task.await(ref.io);
        process_state.release(ref);
    }
    const handles_before = handleCount();

    const ref = try process_state.acquire();
    var gate = std.atomic.Value(u32).init(0);
    var workers: [3]Worker = @splat(.{});
    var tasks: [3]Io.Future(void) = undefined;
    for (&tasks, &workers) |*task, *worker| task.* = try ref.io.concurrent(Worker.run, .{ worker, ref.io, &gate });
    gate.store(1, .release);
    ref.io.futexWake(u32, &gate.raw, std.math.maxInt(u32));
    for (&tasks) |*task| task.await(ref.io);
    process_state.release(ref);

    // `release` waited for every worker to exit, each having closed its handle to itself. The count may still take a
    // moment to come back, or come back lower: parts of the process outside the library open or close a handle late.
    for (workers) |worker| try testing.expect(worker.gone());
    for (workers) |worker| _ = win32.CloseHandle(worker.exited.?);
    for (0..1000) |_| {
        if (handleCount() <= handles_before) break;
        win32.Sleep(1);
    }
    try testing.expect(handleCount() <= handles_before);

    const again = try process_state.acquire();
    var id: std.Thread.Id = std.Thread.getCurrentId();
    var task = try again.io.concurrent(recordThread, .{&id});
    task.await(again.io);
    try testing.expect(id != std.Thread.getCurrentId());
    process_state.release(again);
}
