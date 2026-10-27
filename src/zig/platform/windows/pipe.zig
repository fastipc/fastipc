//! The named pipe of a name, private to windows.zig: its claim (the pipe's instances), the connect with the owner
//! check, the accept and disconnect, the frames on it and its peer process. The handshake's waits on the caller's thread
//! (`acceptWithin`, `readWithin`) are an overlapped operation and a wait with a timeout, cancelled and drained if it
//! doesn't complete; `sendFrame` writes the pipe with an overlapped write of its own that completes at once (a frame
//! fits an empty pipe buffer); the watcher's `read` blocks through `Io`, so that `Future.cancel` ends it (`endTask`).
//! Nothing here cancels or closes a handle another thread has an `Io` operation on (that trips `Io.Threaded`).

const std = @import("std");
const Io = std.Io;
const log = @import("../../core/log.zig");
const names = @import("../../core/names.zig");
const platform = @import("../../platform.zig");
const win32 = @import("win32.zig");
const windows = @import("windows.zig");

const Handle = windows.Handle;
const Security = windows.Security;
const ResourceError = platform.ResourceError;
const UnexpectedError = platform.UnexpectedError;
const unexpected = windows.unexpected;

/// A pipe path (names.windowsRendezvous).
pub const Address = struct {
    buf: [names.windows_max]u8,
    len: usize,

    pub fn path(address: *const Address) []const u8 {
        return address.buf[0..address.len];
    }
};

/// The name's pipe, in this process's Terminal Services session.
pub fn addressOf(name: []const u8) UnexpectedError!Address {
    var address: Address = undefined;
    address.len = names.windowsRendezvous(&address.buf, try sessionId(), name).len;
    return address;
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
    var path: windows.Utf16Builder(names.windows_max) = .{};
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

/// `PIPE_UNLIMITED_INSTANCES`: a listener keeps one instance per connection it accepted, plus the next one it listens
/// on.
const pipe_unlimited_instances: win32.DWORD = 255;

/// Opens the pipe as its client (overlapped, not inheritable), then checks that the pipe's owner is this process's
/// user. Without administrator privileges, a process can make an object's owner only a SID that its own token holds,
/// so another user's process can't pass for the listener.
///   - `foreign`: the owner is another user's, or ERROR_ACCESS_DENIED (the pipe's DACL refuses this process).
///   - `busy`: the one instance has a client or is between clients (ERROR_PIPE_BUSY), or the listener disconnected this
///     client before its owner could be checked, as it does when it accepts a connection and drops it at once.
/// The listener may identify the client but not impersonate it (`SECURITY_IDENTIFICATION`; the default would let it).
pub fn connect(address: *const Address, security: *const Security) (ResourceError || UnexpectedError)!platform.Connect {
    var path: windows.Utf16Builder(names.windows_max) = .{};
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
    switch (windows.ownerOf(client, security)) {
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

/// Disconnects the listener's instance from its client, keeping the instance: closing it would give up the name.
pub fn disconnect(pipe: Handle) void {
    if (win32.DisconnectNamedPipe(pipe) == 0) log.warn(@src(), "DisconnectNamedPipe failed: error {d}", .{win32.GetLastError()});
}

/// Nothing: a pipe handle can't be closed under an `Io` operation; `endTask` cancels the watcher instead.
pub fn unblock(_: Handle) void {}

/// Cancels the watcher, which ends its blocked pipe read (on a finished task it is the await).
pub fn endTask(io: Io, task: *Io.Future(void)) void {
    task.cancel(io);
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
    return switch (overlapped(pipe, cancel, timeout_ms, .connect)) {
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

// === Frames, and the reads on the caller's thread ===

/// Writes the frame with an overlapped `WriteFile` of its own and a wait that isn't alertable: on a poller's thread an
/// alertable wait would run the poller's completion routines in the middle of the send. A frame fits an empty pipe
/// buffer, so the write completes at once. The segment is found by name: nothing is attached.
pub fn sendFrame(connection: Handle, frame: []const u8, attached: ?Handle) bool {
    std.debug.assert(attached == null);
    var rest = frame;
    while (rest.len > 0) {
        const written = switch (overlapped(connection, null, null, .{ .write = rest })) {
            .done => |completion| switch (completion) {
                .ok => |n| n,
                .failed => return false,
            },
            .idle, .cancelled => return false,
        };
        if (written == 0) return false;
        rest = rest[written..];
    }
    return true;
}

/// Reads what arrives within `timeout_ms` (null: no limit), or until the cancel signal. 0: the connection ended (the
/// other end closed: ERROR_BROKEN_PIPE), or the read failed.
pub fn readWithin(pipe: Handle, buf: []u8, cancel: ?Handle, timeout_ms: ?u32) platform.Waited(usize) {
    return switch (overlapped(pipe, cancel, timeout_ms, .{ .read = buf })) {
        .idle => .idle,
        .cancelled => .cancelled,
        .done => |completion| .{ .done = switch (completion) {
            .ok => |n| n,
            .failed => 0,
        } },
    };
}

/// Reads through `io` into `buf`, blocking until at least one byte arrives. 0: the connection ended (the other end
/// closed: STATUS_PIPE_BROKEN) or the read failed, which the watcher treats alike.
pub fn read(io: Io, pipe: Handle, buf: []u8) platform.ReadError!usize {
    const result = try io.operate(.{ .file_read_streaming = .{ .file = file(pipe), .data = &.{buf} } });
    return platform.readResult(result.file_read_streaming);
}

/// A pipe handle as an `Io.File`: opened overlapped, which `Io.Threaded` calls non-blocking.
fn file(pipe: Handle) Io.File {
    return .{ .handle = pipe, .flags = .{ .nonblocking = true } };
}

/// An overlapped operation on a pipe: wait for a client (`ConnectNamedPipe`), or read into or write from a buffer.
const Operation = union(enum) {
    connect,
    read: []u8,
    write: []const u8,

    /// Starts the operation: nonzero if it completed at once.
    fn start(operation: Operation, pipe: Handle, state: *win32.OVERLAPPED) win32.BOOL {
        return switch (operation) {
            .connect => win32.ConnectNamedPipe(pipe, state),
            .read => |buf| win32.ReadFile(pipe, buf.ptr, @intCast(@min(buf.len, std.math.maxInt(win32.DWORD))), null, state),
            .write => |bytes| win32.WriteFile(pipe, bytes.ptr, @intCast(@min(bytes.len, std.math.maxInt(win32.DWORD))), null, state),
        };
    }
};

/// How an overlapped operation completed: the bytes it moved, or its error.
const Completion = union(enum) { ok: win32.DWORD, failed: win32.DWORD };

/// Runs one overlapped operation on `pipe` and waits up to `timeout_ms` (null: no limit) for it to complete, or for the
/// cancel signal. If it hasn't completed by then, it is cancelled (`CancelIoEx`) and the cancellation awaited, so no
/// I/O is left in flight when this returns and the `OVERLAPPED` may live on this frame. An operation that completed
/// just as it was cancelled counts: a client that connected, or bytes that arrived, are never lost.
fn overlapped(pipe: Handle, cancel: ?Handle, timeout_ms: ?u32, operation: Operation) platform.Waited(Completion) {
    const event = win32.CreateEventW(null, win32.TRUE, win32.FALSE, null) orelse
        return .{ .done = .{ .failed = win32.GetLastError() } };
    defer _ = win32.CloseHandle(event);
    var state: win32.OVERLAPPED = .{ .hEvent = event };
    var n: win32.DWORD = 0;
    if (operation.start(pipe, &state) != 0) {
        _ = win32.GetOverlappedResult(pipe, &state, &n, win32.TRUE);
        return .{ .done = .{ .ok = n } };
    }
    const started = win32.GetLastError();
    if (started != win32.ERROR_IO_PENDING) return .{ .done = .{ .failed = started } };
    const handles = [2]Handle{ event, cancel orelse event };
    const count: win32.DWORD = if (cancel != null) 2 else 1;
    const waited = win32.WaitForMultipleObjects(count, &handles, win32.FALSE, windows.waitMs(timeout_ms));
    const cancelled = waited == win32.WAIT_OBJECT_0 + 1;
    if (waited != win32.WAIT_OBJECT_0) _ = win32.CancelIoEx(pipe, &state);
    if (win32.GetOverlappedResult(pipe, &state, &n, win32.TRUE) != 0) return .{ .done = .{ .ok = n } };
    const code = win32.GetLastError();
    if (code == win32.ERROR_OPERATION_ABORTED) return if (cancelled) .cancelled else .idle;
    return .{ .done = .{ .failed = code } };
}

// === Tests (real system calls; platform/contract_test.zig tests the contract) ===

const testing = std.testing;
const contract_test = @import("../contract_test.zig");
const process_state = @import("../../core/process_state.zig");

const testAddress = contract_test.testAddress;
const connected = contract_test.connected;
const handleCount = windows.openHandleCount;
const Listened = platform.Waited(Listen);

/// Writes all of `bytes` through `io`, as the peer of a watcher's `read`. False: the connection is gone, or the write
/// failed.
pub fn write(io: Io, pipe: Handle, bytes: []const u8) Io.Cancelable!bool {
    var rest = bytes;
    while (rest.len > 0) {
        const result = try io.operate(.{ .file_write_streaming = .{ .file = file(pipe), .data = &.{rest} } });
        const written = result.file_write_streaming catch return false;
        if (written == 0) return false;
        rest = rest[written..];
    }
    return true;
}

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
        try testing.expect(sendFrame(client, &frame, null));
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
    const cancel = try windows.createCancelSignal();
    // Bytes that arrive after a read timed out wait for the next read
    const client = try connected(try connect(&address, &security));
    try testing.expectEqual(platform.Waited(platform.Accept){ .done = .{ .connection = pipe } }, acceptWithin(pipe, cancel, 0, null));
    var buf: [64]u8 = undefined;
    for (0..20) |i| {
        try testing.expectEqual(platform.Waited(usize).idle, readWithin(pipe, &buf, cancel, 0));
        const byte = [1]u8{@intCast(i)};
        try testing.expect(sendFrame(client, &byte, null));
        try testing.expectEqual(platform.Waited(usize){ .done = 1 }, readWithin(pipe, &buf, cancel, 5000));
        try testing.expectEqual(byte[0], buf[0]);
    }
    // The signal ends a read that waits for ever, and stays set
    const Setter = struct {
        fn run(signal: Handle) void {
            win32.Sleep(20);
            windows.setCancelSignal(signal);
        }
    };
    const setter = try std.Thread.spawn(.{}, Setter.run, .{cancel});
    try testing.expectEqual(platform.Waited(usize).cancelled, readWithin(pipe, &buf, cancel, null));
    setter.join();
    try testing.expect(windows.waitCancelSignal(cancel, 0));
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
    var path16: windows.Utf16Builder(names.windows_max) = .{};
    path16.ascii(address.path());
    const client = win32.CreateFileW(path16.string(), win32.GENERIC_READ | win32.GENERIC_WRITE, 0, null, win32.OPEN_EXISTING, win32.FILE_FLAG_OVERLAPPED, null);
    try testing.expect(client != win32.INVALID_HANDLE_VALUE);
    try testing.expectEqual(Listened{ .done = .connected }, listenWithin(pipe, null, 0));
    disconnect(pipe);
    try testing.expectEqual(windows.Owner.disconnected, windows.ownerOf(client, &security));
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
    const session_id = windows.testSessionId(0x38);
    var name: windows.ObjectName = .init(&session_id, .s2c_data);
    const attributes = security.attributes();
    const event = win32.CreateEventW(&attributes, win32.FALSE, win32.FALSE, name.string()) orelse return error.TestUnexpectedResult;
    defer _ = win32.CloseHandle(event);
    const synchronize_only = win32.OpenEventW(win32.SYNCHRONIZE, win32.FALSE, name.string()) orelse return error.TestUnexpectedResult;
    defer _ = win32.CloseHandle(synchronize_only);
    try testing.expectEqual(windows.Owner.foreign, windows.ownerOf(synchronize_only, &security));
    try testing.expectEqual(windows.Owner.user, windows.ownerOf(event, &security));
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
    var sddl: windows.Utf16Builder(2 * 184 + 20) = .{};
    sddl.ascii("O:BAD:P(A;;GA;;;");
    sddl.wide(std.mem.span(sid_string.?));
    sddl.ascii(")");
    var descriptor: ?*anyopaque = null;
    try testing.expect(win32.ConvertStringSecurityDescriptorToSecurityDescriptorW(sddl.string(), win32.SDDL_REVISION_1, &descriptor, null) != 0);
    defer _ = win32.LocalFree(descriptor);
    const foreign_attributes: win32.SECURITY_ATTRIBUTES = .{ .nLength = @sizeOf(win32.SECURITY_ATTRIBUTES), .lpSecurityDescriptor = descriptor, .bInheritHandle = win32.FALSE };

    const address = testAddress("foreign");
    var path16: windows.Utf16Builder(names.windows_max) = .{};
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
