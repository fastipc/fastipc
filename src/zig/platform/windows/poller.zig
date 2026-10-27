//! Windows's poller (platform.zig `Poller`), private to windows.zig: reads that the polling thread issues itself, whose
//! completion routines run in its alertable waits, and the listening instance's connect.

const std = @import("std");
const log = @import("../../core/log.zig");
const named_pipe = @import("pipe.zig");
const platform = @import("../../platform.zig");
const win32 = @import("win32.zig");
const windows = @import("windows.zig");
const wire = @import("../../core/wire.zig");

const Handle = windows.Handle;
const ResourceError = platform.ResourceError;
const UnexpectedError = platform.UnexpectedError;

/// A pipe a `Poller` watches: the frame its reads fill, and which of the poller's lists holds it (`state`).
pub const Watched = struct {
    /// The read's `OVERLAPPED`. `onRead` finds the `Watched` from its address.
    overlapped: win32.OVERLAPPED = .{},
    connection: Handle,
    frame: wire.Frame = undefined,
    got: usize = 0,
    poller: *Poller,
    state: State = .starting,
    /// Set before `unwatch` or `drain` cancels the read: its completion then only ends it.
    cancelling: bool = false,
    /// What its read came to, once `ready`.
    outcome: platform.Taken = .partial,
    /// In the poller's list that `state` names.
    node: std.DoublyLinkedList.Node = .{},

    const State = enum {
        /// In `Poller.starting`: `watch` queued it, and the polling thread hasn't issued its first read
        starting,
        /// In `Poller.reading`: a read is in flight
        reading,
        /// In `Poller.ready`: its last read ended (`outcome`)
        ready,
        /// In no list: `wait` reported it, or its read is gone
        done,
    };
};

/// Reads that the polling thread issues itself (`ReadFileEx`), whose completion routines (`onRead`) run in its
/// alertable waits: a read is issued again until the frame is whole, the connection ends or a read fails. `watch`
/// queues a connection and sets `wake`, an auto-reset event, on which the polling thread waits alertably; `stop` sets
/// it too. The polling thread's own lists need no lock: completion routines run on it. Every read is drained (`drain`)
/// before the polling thread moves on, since a routine left queued would run in a later task's alertable wait.
///
/// The listening instance (`listen`) has an overlapped `ConnectNamedPipe` (which has no completion-routine form) whose
/// manual-reset event, `connect_event`, the polling thread's wait watches besides `wake` while it is pending. The
/// poller stays at one address while it runs, so the kernel writes its `OVERLAPPED` there; `unlisten` and `drain`
/// cancel a pending connect and await it before the instance or the poller can go.
pub const Poller = struct {
    wake: Handle,
    stopping: std.atomic.Value(bool) = .init(false),
    /// Guards `starting`, which `watch` fills from any thread.
    lock: windows.Lock = .{},
    starting: std.DoublyLinkedList = .{},
    reading: std.DoublyLinkedList = .{},
    ready: std.DoublyLinkedList = .{},
    /// The instance listened on (`listen`), if any.
    listening: ?Handle = null,
    connect_event: Handle,
    connect_overlapped: win32.OVERLAPPED = .{},
    /// `idle`: no connect issued since the last report; `pending`: one in flight; `connected`: a client is there (the
    /// connect completed, or found one), to report at the next wait.
    connect: enum { idle, pending, connected } = .idle,

    pub fn init() (ResourceError || UnexpectedError)!Poller {
        const wake = try createEvent(win32.FALSE);
        errdefer windows.close(wake);
        return .{ .wake = wake, .connect_event = try createEvent(win32.TRUE) };
    }

    fn createEvent(manual_reset: win32.BOOL) (ResourceError || UnexpectedError)!Handle {
        return win32.CreateEventW(null, manual_reset, win32.FALSE, null) orelse switch (win32.GetLastError()) {
            win32.ERROR_NO_SYSTEM_RESOURCES, win32.ERROR_NOT_ENOUGH_MEMORY, win32.ERROR_OUTOFMEMORY => error.SystemResources,
            else => windows.unexpected(@src(), "CreateEventW"),
        };
    }

    pub fn handles(poller: *const Poller) [2]?Handle {
        return .{ poller.wake, poller.connect_event };
    }

    pub fn watch(poller: *Poller, watched: *Watched, connection: Handle) (ResourceError || UnexpectedError)!void {
        watched.* = .{ .connection = connection, .poller = poller };
        {
            poller.lock.lock();
            defer poller.lock.unlock();
            poller.starting.append(&watched.node);
        }
        _ = win32.SetEvent(poller.wake);
    }

    /// Issues the reads `watch` queued, then reports the connections whose reads ended (`ready.len` at most) and a
    /// client on the listening instance; with nothing to report it waits alertably once, up to `timeout_ms` (null: no
    /// limit), which runs the routines of reads that completed, and reports what that brought (maybe nothing). Null
    /// once stopped.
    pub fn wait(poller: *Poller, ready: []*Watched, timeout_ms: ?u32) ?platform.Polled {
        var waited = false;
        while (true) {
            if (poller.stopping.load(.acquire)) return null;
            poller.startReads();
            var polled: platform.Polled = .{ .ready = ready[0..0], .listening = poller.connect == .connected };
            while (polled.ready.len < ready.len) {
                const watched: *Watched = @fieldParentPtr("node", poller.ready.popFirst() orelse break);
                watched.state = .done;
                ready[polled.ready.len] = watched;
                polled.ready = ready[0 .. polled.ready.len + 1];
            }
            if (polled.listening) poller.connect = .idle;
            if (polled.listening or polled.ready.len > 0 or waited) return polled;
            const objects = [2]Handle{ poller.wake, poller.connect_event };
            const count: win32.DWORD = if (poller.connect == .pending) 2 else 1;
            if (win32.WaitForMultipleObjectsEx(count, &objects, win32.FALSE, windows.waitMs(timeout_ms), win32.TRUE) == win32.WAIT_OBJECT_0 + 1) {
                // The connect completed: a client, or a failure that the accept then meets and reports
                var n: win32.DWORD = 0;
                _ = win32.GetOverlappedResult(poller.listening.?, &poller.connect_overlapped, &n, win32.FALSE);
                poller.connect = .connected;
            }
            waited = true;
        }
    }

    /// Reports a client on the listening instance from now on, until it is reported or `unlisten`: issues the
    /// instance's connect, unless one is in flight or found a client already.
    pub fn listen(poller: *Poller, listening: Handle) (ResourceError || UnexpectedError)!void {
        if (poller.listening) |current| {
            if (current != listening) poller.unlisten();
        }
        poller.listening = listening;
        if (poller.connect != .idle) return;
        while (true) {
            poller.connect_overlapped = .{ .hEvent = poller.connect_event };
            if (win32.ConnectNamedPipe(listening, &poller.connect_overlapped) != 0) {
                poller.connect = .connected;
                return;
            }
            switch (win32.GetLastError()) {
                win32.ERROR_IO_PENDING => poller.connect = .pending,
                // A client connected before the call: it is there
                win32.ERROR_PIPE_CONNECTED => poller.connect = .connected,
                // A client connected and left before the call: listen again
                win32.ERROR_NO_DATA => {
                    named_pipe.disconnect(listening);
                    continue;
                },
                else => |code| {
                    // Reported as a client: the accept meets the same failure, and the caller backs off
                    log.warn(@src(), "ConnectNamedPipe failed: error {d}", .{code});
                    poller.connect = .connected;
                },
            }
            return;
        }
    }

    /// Cancels a connect in flight and awaits it: the instance may go once this returns. A client that connected
    /// meanwhile stays connected, and the next `listen` finds it.
    pub fn unlisten(poller: *Poller) void {
        const listening = poller.listening orelse return;
        if (poller.connect == .pending) {
            if (win32.CancelIoEx(listening, &poller.connect_overlapped) == 0 and win32.GetLastError() != win32.ERROR_NOT_FOUND) {
                windows.warnUnexpected(@src(), "CancelIoEx");
            }
            var n: win32.DWORD = 0;
            _ = win32.GetOverlappedResult(listening, &poller.connect_overlapped, &n, win32.TRUE);
        }
        poller.connect = .idle;
        poller.listening = null;
    }

    /// What the reads came to: never `partial`, since a read is issued again until the frame is whole.
    pub fn take(_: *Poller, watched: *Watched) platform.Taken {
        return watched.outcome;
    }

    /// Takes the connection out of whichever list holds it; a read in flight is cancelled, and its routine awaited (an
    /// alertable sleep, which runs other reads' routines too), so the `Watched` may go once this returns.
    pub fn unwatch(poller: *Poller, watched: *Watched) void {
        switch (watched.state) {
            .starting => {
                poller.lock.lock();
                defer poller.lock.unlock();
                poller.starting.remove(&watched.node);
            },
            .reading => {
                cancel(watched);
                while (watched.state == .reading) _ = win32.SleepEx(win32.INFINITE, win32.TRUE);
            },
            .ready => poller.ready.remove(&watched.node),
            .done => {},
        }
        watched.state = .done;
    }

    pub fn stop(poller: *Poller) void {
        poller.stopping.store(true, .release);
        _ = win32.SetEvent(poller.wake);
    }

    /// The polling thread's last step: drops the connections `watch` queued and the ones not reported, cancels every
    /// read in flight and sleeps alertably until each one's routine has run, so that none is left queued to this
    /// thread.
    pub fn drain(poller: *Poller) void {
        poller.unlisten();
        {
            poller.lock.lock();
            defer poller.lock.unlock();
            while (poller.starting.popFirst()) |node| @as(*Watched, @fieldParentPtr("node", node)).state = .done;
        }
        while (poller.ready.popFirst()) |node| @as(*Watched, @fieldParentPtr("node", node)).state = .done;
        // No routine runs during this walk: they run only in alertable waits
        var node = poller.reading.first;
        while (node) |reading| : (node = reading.next) cancel(@fieldParentPtr("node", reading));
        while (poller.reading.first != null) _ = win32.SleepEx(win32.INFINITE, win32.TRUE);
    }

    /// Issues the first read of each connection `watch` queued.
    fn startReads(poller: *Poller) void {
        var queued = queued: {
            poller.lock.lock();
            defer poller.lock.unlock();
            const queued = poller.starting;
            poller.starting = .{};
            break :queued queued;
        };
        while (queued.popFirst()) |node| poller.read(@fieldParentPtr("node", node));
    }

    /// Reads the rest of the frame. Its routine runs in this thread's next alertable wait, also when the read completes
    /// at once; a read that can't start ends the connection's watch at once.
    fn read(poller: *Poller, watched: *Watched) void {
        const rest = watched.frame[watched.got..];
        watched.overlapped = .{};
        if (win32.ReadFileEx(watched.connection, rest.ptr, @intCast(rest.len), &watched.overlapped, onRead) != 0) {
            watched.state = .reading;
            poller.reading.append(&watched.node);
            return;
        }
        poller.finish(watched, endOf(win32.GetLastError()));
    }

    fn finish(poller: *Poller, watched: *Watched, outcome: platform.Taken) void {
        watched.outcome = outcome;
        watched.state = .ready;
        poller.ready.append(&watched.node);
    }

    /// A read's completion routine. If the read completed before its cancel took effect, it reports success with its
    /// bytes instead of ERROR_OPERATION_ABORTED. Either way, a read that is being cancelled (`cancelling`) ends here.
    fn onRead(code: win32.DWORD, bytes: win32.DWORD, overlapped_state: *win32.OVERLAPPED) callconv(.winapi) void {
        const watched: *Watched = @fieldParentPtr("overlapped", overlapped_state);
        const poller = watched.poller;
        poller.reading.remove(&watched.node);
        watched.state = .done;
        if (watched.cancelling) return;
        if (code != 0) return poller.finish(watched, endOf(code));
        watched.got += bytes;
        if (watched.got == watched.frame.len) return poller.finish(watched, .whole);
        poller.read(watched);
    }

    /// The other end closed the pipe, or its process ended: ended; any other error: failed.
    fn endOf(code: win32.DWORD) platform.Taken {
        return switch (code) {
            win32.ERROR_BROKEN_PIPE, win32.ERROR_NO_DATA, win32.ERROR_HANDLE_EOF, win32.ERROR_PIPE_NOT_CONNECTED => .ended,
            else => .failed,
        };
    }

    /// Cancels a read in flight. ERROR_NOT_FOUND: it completed, and its routine is queued.
    fn cancel(watched: *Watched) void {
        watched.cancelling = true;
        if (win32.CancelIoEx(watched.connection, &watched.overlapped) == 0 and win32.GetLastError() != win32.ERROR_NOT_FOUND) {
            windows.warnUnexpected(@src(), "CancelIoEx");
        }
    }
};

// === Tests (real system calls; platform/contract_test.zig tests the contract) ===

const testing = std.testing;
const contract_test = @import("../contract_test.zig");
const write = named_pipe.write;

/// Two connected pipes for the poller's tests: `watched` (the listener's ends, which the poller watches) and `peers`.
const PollerPipes = struct {
    security: windows.Security,
    listening: Handle,
    watched: [2]Handle,
    peers: [2]Handle,

    fn open() !PollerPipes {
        var security = try windows.Security.init();
        errdefer security.deinit();
        const address = contract_test.testAddress("poller");
        var pipes: PollerPipes = .{ .security = security, .listening = undefined, .watched = undefined, .peers = undefined };
        pipes.listening = (try windows.claim(&address, &pipes.security)) orelse return error.TestUnexpectedResult;
        for (&pipes.watched, &pipes.peers) |*watched, *other_end| {
            other_end.* = try contract_test.connected(try windows.connect(&address, &pipes.security));
            watched.* = try contract_test.accepted(windows.acceptWithin(pipes.listening, null, 0, null));
            pipes.listening = (try windows.nextListening(&address, &pipes.security)).?;
        }
        return pipes;
    }

    fn deinit(pipes: *PollerPipes) void {
        for (pipes.watched ++ pipes.peers) |handle| _ = win32.CloseHandle(handle);
        _ = win32.CloseHandle(pipes.listening);
        pipes.security.deinit();
    }
};

/// Whether a completion routine was queued to this thread: an alertable look runs it.
fn routineQueued() bool {
    return win32.SleepEx(0, win32.TRUE) == win32.WAIT_IO_COMPLETION;
}

test "poller: unwatch cancels a read in flight and awaits its routine; the pipe's bytes stay for another reader" {
    var pipes = try PollerPipes.open();
    defer pipes.deinit();
    var poller = try Poller.init();
    defer for (poller.handles()) |handle| windows.close(handle.?);
    var watched: [2]Watched = undefined;
    for (&watched, pipes.watched) |*one, pipe| try poller.watch(one, pipe);
    // The second's frame is reported; the first's read stays in flight
    const frame = @as([64]u8, @splat(0x5a));
    try testing.expect(try write(testing.io, pipes.peers[1], &frame));
    var ready: [2]*Watched = undefined;
    const reported = reported: while (true) {
        const polled = poller.wait(&ready, null).?;
        if (polled.ready.len > 0) break :reported polled.ready;
    };
    try testing.expectEqual(@as(usize, 1), reported.len);
    try testing.expectEqual(&watched[1], reported[0]);
    try testing.expectEqual(platform.Taken.whole, poller.take(reported[0]));
    try testing.expectEqual(Watched.State.reading, watched[0].state);
    poller.unwatch(&watched[0]);
    try testing.expectEqual(Watched.State.done, watched[0].state);
    poller.unwatch(&watched[1]);
    try testing.expect(!routineQueued());
    // Nothing of the pipe was taken: a frame sent now is read whole by another reader
    try testing.expect(try write(testing.io, pipes.peers[0], &frame));
    var buf: [64]u8 = undefined;
    try testing.expectEqual(platform.Waited(usize){ .done = 64 }, windows.readWithin(pipes.watched[0], &buf, null, 5000));
    poller.stop();
    poller.drain();
}

test "poller: drain cancels every read in flight and runs its routine, so none is left queued to the thread" {
    var pipes = try PollerPipes.open();
    defer pipes.deinit();
    var poller = try Poller.init();
    defer for (poller.handles()) |handle| windows.close(handle.?);
    var watched: [2]Watched = undefined;
    try poller.watch(&watched[0], pipes.watched[0]);
    // A stop from another thread while the reads are in flight
    const Stopper = struct {
        fn run(stopping: *Poller) void {
            win32.Sleep(50);
            stopping.stop();
        }
    };
    const stopper = try std.Thread.spawn(.{}, Stopper.run, .{&poller});
    var ready: [2]*Watched = undefined;
    while (poller.wait(&ready, null)) |polled| try testing.expectEqual(@as(usize, 0), polled.ready.len);
    stopper.join();
    try testing.expectEqual(Watched.State.reading, watched[0].state);
    // A watch the poller never started, and bytes that arrive for the read in flight as it is cancelled
    try poller.watch(&watched[1], pipes.watched[1]);
    const frame = @as([64]u8, @splat(0x33));
    try testing.expect(try write(testing.io, pipes.peers[0], &frame));
    poller.drain();
    for (watched) |one| try testing.expectEqual(Watched.State.done, one.state);
    try testing.expect(!routineQueued());
    try testing.expect(try write(testing.io, pipes.peers[1], &frame));
    win32.Sleep(20);
    try testing.expect(!routineQueued());
}
