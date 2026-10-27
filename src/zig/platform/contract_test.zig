//! The OS contract's tests (platform.zig): written once, against the contract alone, and run on every OS with real
//! system calls. Each backend keeps only the tests that need its own helpers (platform/linux/linux.zig,
//! platform/macos/, platform/windows/), and uses the helpers here (`testAddress`, `Pair`, `accepted`, `connected`).
//! The names each test claims are unique to its process.

const std = @import("std");
const builtin = @import("builtin");
const Io = std.Io;
const platform = @import("../platform.zig");
const process_state = @import("../core/process_state.zig");

const testing = std.testing;
const Handle = platform.Handle;
const Waited = platform.Waited;

// === Helpers, also for the backends' tests ===

/// A rendezvous address unique to this process and call.
pub fn testAddress(comptime tag: []const u8) platform.Address {
    const counter = struct {
        var next = std.atomic.Value(u32).init(0);
    };
    var name_buf: [64]u8 = undefined;
    const name = std.mem.print(&name_buf, "test-" ++ tag ++ "-{d}-{d}", .{ platform.pid(), counter.next.fetchAdd(1, .monotonic) }) catch unreachable;
    return platform.addressOf(name) catch unreachable;
}

/// The connection of an accept that must have got one.
pub fn accepted(outcome: Waited(platform.Accept)) !Handle {
    return switch (outcome) {
        .done => |accept| switch (accept) {
            .connection => |connection| connection,
            else => error.TestUnexpectedResult,
        },
        else => error.TestUnexpectedResult,
    };
}

/// The connection of a connect that must have got one.
pub fn connected(outcome: platform.Connect) !Handle {
    return switch (outcome) {
        .connected => |connection| connection,
        else => error.TestUnexpectedResult,
    };
}

/// A connected pair on a name: the listener's end of the connection (`accepted`), the client's (`client`), and the
/// listening handle (`listener`), which holds the name. Where the client connected on the listening handle itself
/// (Windows), `listener` is the next one (`nextListening`).
pub const Pair = struct {
    listener: Handle,
    accepted: Handle,
    client: Handle,

    pub fn open(address: *const platform.Address) !Pair {
        // The objects keep their security once created
        var security = try platform.Security.init();
        defer security.deinit();
        const listener = (try platform.claim(address, &security)) orelse return error.TestUnexpectedResult;
        errdefer platform.close(listener);
        const client = try connected(try platform.connect(address, &security));
        errdefer platform.close(client);
        const connection = try accepted(platform.acceptWithin(listener, null, 5000, null));
        const next = try platform.nextListening(address, &security);
        return .{ .listener = next orelse listener, .accepted = connection, .client = client };
    }

    pub fn deinit(pair: Pair) void {
        platform.close(pair.accepted);
        platform.close(pair.client);
        platform.close(pair.listener);
    }
};

/// Milliseconds on the awake clock.
pub fn nowMs() i64 {
    return Io.Clock.awake.now(testing.io).toMilliseconds();
}

/// Gives threads just started time to block in their waits: a test is only more telling when they have.
fn letThemBlock() void {
    testing.io.sleep(.fromMilliseconds(50), .awake) catch {};
}

// === The process ===

/// A task that holds a worker until `gate` opens, and marks its thread, to tell later whether it has exited.
const Worker = struct {
    mark: ?platform.ThreadMark = null,

    fn run(worker: *Worker, io: Io, gate: *std.atomic.Value(u32)) void {
        worker.mark = .ofCurrentThread();
        while (gate.load(.acquire) == 0) io.futexWaitUncancelable(u32, &gate.raw, 0);
    }

    fn gone(worker: Worker) bool {
        return worker.mark.?.exited();
    }
};

fn recordThread(id: *std.Thread.Id) void {
    id.* = std.Thread.getCurrentId();
}

test "no library thread is left after the process-wide Io's last release, and a new instance works" {
    // Warm up, so that what the process creates once (on Windows, the C runtime's thread-pool bookkeeping and loaded
    // DLLs) is in the baseline
    {
        const ref = try process_state.acquire();
        var id: std.Thread.Id = 0;
        var task = try ref.io.concurrent(recordThread, .{&id});
        task.await(ref.io);
        process_state.release(ref);
    }
    const threads_before = platform.threadCount();
    const handles_before = platform.openHandleCount();

    const ref = try process_state.acquire();
    var gate = std.atomic.Value(u32).init(0);
    var workers: [3]Worker = @splat(.{});
    var tasks: [3]Io.Future(void) = undefined;
    for (&tasks, &workers) |*task, *worker| task.* = try ref.io.concurrent(Worker.run, .{ worker, ref.io, &gate });
    gate.store(1, .release);
    ref.io.futexWake(u32, &gate.raw, std.math.maxInt(u32));
    for (&tasks) |*task| task.await(ref.io);
    process_state.release(ref);

    // Joined by `release`. Where the library can be unloaded at its last release (a Windows DLL), `shutDownIo` has
    // also waited for every worker's thread to exit; elsewhere the kernel ends each thread a moment later.
    const exits_at_release = builtin.os.tag == .windows;
    if (!exits_at_release) for (0..1000) |_| {
        if (workers[0].gone() and workers[1].gone() and workers[2].gone() and platform.threadCount() <= threads_before) break;
        platform.sleep(1);
    };
    for (workers) |worker| try testing.expect(worker.gone());
    for (workers) |worker| worker.mark.?.deinit();
    if (exits_at_release) {
        // Each worker closed its handle to itself as it exited. The count may still take a moment to come back, or
        // come back lower: parts of the process outside the library open or close a handle late.
        for (0..1000) |_| {
            if (platform.openHandleCount() <= handles_before) break;
            platform.sleep(1);
        }
        try testing.expect(platform.openHandleCount() <= handles_before);
    } else {
        try testing.expect(platform.threadCount() <= threads_before);
    }

    const again = try process_state.acquire();
    var id: std.Thread.Id = std.Thread.getCurrentId();
    var task = try again.io.concurrent(recordThread, .{&id});
    task.await(again.io);
    try testing.expect(id != std.Thread.getCurrentId());
    process_state.release(again);
}

// === Names and connections, frames ===

test "claim: one claim per name (a second one gets nothing), free again at once when it closes" {
    var security = try platform.Security.init();
    defer security.deinit();
    const handles = platform.openHandleCount();
    const address = testAddress("claim");
    const first = (try platform.claim(&address, &security)) orelse return error.TestUnexpectedResult;
    try testing.expectEqual(@as(?Handle, null), try platform.claim(&address, &security));
    platform.close(first);
    const again = (try platform.claim(&address, &security)) orelse return error.TestUnexpectedResult;
    platform.close(again);
    try testing.expectEqual(handles, platform.openHandleCount());
}

fn claimRacer(address: *const platform.Address, security: *const platform.Security, start: *std.atomic.Value(u32), claimed: *?Handle) void {
    while (start.load(.acquire) == 0) std.atomic.spinLoopHint();
    claimed.* = platform.claim(address, security) catch null;
}

test "claim race: of several threads claiming one name at once, exactly one gets it" {
    var security = try platform.Security.init();
    defer security.deinit();
    const handles = platform.openHandleCount();
    for (0..20) |_| {
        const address = testAddress("race");
        var start = std.atomic.Value(u32).init(0);
        var claimed: [8]?Handle = @splat(null);
        var threads: [8]std.Thread = undefined;
        for (&threads, &claimed) |*thread, *slot| thread.* = try std.Thread.spawn(.{}, claimRacer, .{ &address, &security, &start, slot });
        start.store(1, .release);
        for (threads) |thread| thread.join();
        var winners: usize = 0;
        for (claimed) |slot| if (slot) |handle| {
            winners += 1;
            platform.close(handle);
        };
        try testing.expectEqual(@as(usize, 1), winners);
    }
    try testing.expectEqual(handles, platform.openHandleCount());
}

/// A watcher's read, whose outcome the test looks at once `endTask` has ended it.
fn readInto(io: Io, connection: Handle, buf: []u8, outcome: *platform.ReadError!usize) void {
    outcome.* = platform.read(io, connection, buf);
}

test "a watcher's stop: unblock, then endTask, ends a blocked read (the end of the stream, or Canceled)" {
    const ref = try process_state.acquire();
    defer process_state.release(ref);
    const handles = platform.openHandleCount();
    {
        const pair = try Pair.open(&testAddress("stop-read"));
        defer pair.deinit();
        var buf: [64]u8 = undefined;
        var outcome: platform.ReadError!usize = error.ReadFailed;
        var reading = try ref.io.concurrent(readInto, .{ ref.io, pair.client, @as([]u8, &buf), &outcome });
        platform.sleep(20); // gives the read time to block: the stop must work whether it has or not
        platform.unblock(pair.client);
        platform.endTask(ref.io, &reading);
        // `unblock` ends the read where it can (a socket); elsewhere `endTask` cancels it
        const ended = if (outcome) |n| n == 0 else |err| err == error.Canceled;
        try testing.expect(ended);
    }
    // No descriptor is left (Windows counts handles, of which the `Io`'s worker threads hold their own)
    if (builtin.os.tag != .windows) try testing.expectEqual(handles, platform.openHandleCount());
}

test "a read or a peek after the peer closed is the end of the stream" {
    const ref = try process_state.acquire();
    defer process_state.release(ref);
    const pair = try Pair.open(&testAddress("eof"));
    defer {
        platform.close(pair.client);
        platform.close(pair.listener);
    }
    platform.close(pair.accepted);
    var buf: [64]u8 = undefined;
    try testing.expectEqual(@as(usize, 0), try platform.read(ref.io, pair.client, &buf));
    if (platform.passes_segment_handle) try testing.expectEqual(Waited(void){ .done = {} }, platform.readableWithin(pair.client, 0));
    try testing.expectEqual(Waited(usize){ .done = 0 }, platform.readWithin(pair.client, &buf, null, 0));
}

// === Waits on the caller's thread ===

test "the waits on the caller's thread: nothing within the time, a client or bytes, the cancel signal, which wins and stays set" {
    const ref = try process_state.acquire();
    defer process_state.release(ref);
    var security = try platform.Security.init();
    defer security.deinit();
    const handles = platform.openHandleCount();
    {
        const address = testAddress("within");
        var listening = (try platform.claim(&address, &security)) orelse return error.TestUnexpectedResult;
        defer platform.close(listening);
        const cancel = try platform.createCancelSignal();
        defer platform.close(cancel);
        // Nobody connected: 0 only looks, a timeout waits that long
        try testing.expectEqual(Waited(platform.Accept).idle, platform.acceptWithin(listening, cancel, 0, null));
        try testing.expectEqual(Waited(platform.Accept).idle, platform.acceptWithin(listening, cancel, 20, null));
        const client = try connected(try platform.connect(&address, &security));
        defer platform.close(client);
        const server = try accepted(platform.acceptWithin(listening, cancel, 0, null));
        defer platform.close(server);
        if (try platform.nextListening(&address, &security)) |next| listening = next;
        var buf: [64]u8 = undefined;
        try testing.expectEqual(Waited(usize).idle, platform.readWithin(server, &buf, cancel, 0));
        if (platform.passes_segment_handle) try testing.expectEqual(Waited(void).idle, platform.readableWithin(client, 10));
        try testing.expect(platform.sendFrame(client, "abc", null));
        try testing.expectEqual(Waited(usize){ .done = 3 }, platform.readWithin(server, &buf, cancel, 5000));
        try testing.expectEqualStrings("abc", buf[0..3]);
        // The cancel signal: a back-off ends, and every wait that watches it returns cancelled, for good
        try testing.expect(!platform.waitCancelSignal(cancel, 0));
        platform.setCancelSignal(cancel);
        try testing.expect(platform.waitCancelSignal(cancel, 5000));
        try testing.expect(platform.waitCancelSignal(cancel, 0));
        // Linux and macOS look at the signal before the connection: it wins over bytes already there. Windows reads
        // them at once, so they come after the cancelled read
        const polls_first = builtin.os.tag != .windows;
        if (polls_first) try testing.expect(platform.sendFrame(client, "d", null));
        try testing.expectEqual(Waited(usize).cancelled, platform.readWithin(server, &buf, cancel, null));
        try testing.expectEqual(Waited(platform.Accept).cancelled, platform.acceptWithin(listening, cancel, null, null));
        if (!polls_first) try testing.expect(platform.sendFrame(client, "d", null));
        try testing.expectEqual(Waited(usize){ .done = 1 }, platform.readWithin(server, &buf, null, 5000));
    }
    try testing.expectEqual(handles, platform.openHandleCount());
}

// === The poller ===

/// Connected pairs on one name: this process's two ends of each, the listener's (`accepted`, which the tests watch)
/// and the client's (`clients`, null once closed).
const TestPairs = struct {
    security: platform.Security,
    name_buf: [64]u8,
    name_len: usize,
    listening: Handle,
    accepted: []Handle,
    clients: []?Handle,

    fn open(count: usize) !TestPairs {
        const counter = struct {
            var next = std.atomic.Value(u32).init(0);
        };
        var pairs: TestPairs = .{ .security = undefined, .name_buf = undefined, .name_len = 0, .listening = undefined, .accepted = &.{}, .clients = &.{} };
        pairs.name_len = (try std.mem.print(&pairs.name_buf, "test-poller-{d}-{d}", .{ platform.pid(), counter.next.fetchAdd(1, .monotonic) })).len;
        const address = try platform.addressOf(pairs.name());
        pairs.security = try platform.Security.init();
        errdefer pairs.security.deinit();
        pairs.listening = (try platform.claim(&address, &pairs.security)) orelse return error.TestUnexpectedResult;
        errdefer platform.close(pairs.listening);
        pairs.accepted = try testing.allocator.alloc(Handle, count);
        errdefer testing.allocator.free(pairs.accepted);
        pairs.clients = try testing.allocator.alloc(?Handle, count);
        errdefer testing.allocator.free(pairs.clients);
        var made: usize = 0;
        errdefer for (pairs.accepted[0..made], pairs.clients[0..made]) |one, client| {
            platform.close(one);
            platform.close(client.?);
        };
        while (made < count) : (made += 1) {
            const client = try connected(try platform.connect(&address, &pairs.security));
            const connection = accepted(platform.acceptWithin(pairs.listening, null, 5000, null)) catch |err| {
                platform.close(client);
                return err;
            };
            pairs.accepted[made] = connection;
            pairs.clients[made] = client;
            // Where a client connects on the listening handle itself, the next one needs another
            if (try platform.nextListening(&address, &pairs.security)) |next| pairs.listening = next;
        }
        return pairs;
    }

    fn name(pairs: *const TestPairs) []const u8 {
        return pairs.name_buf[0..pairs.name_len];
    }

    fn closeClient(pairs: *TestPairs, i: usize) void {
        platform.close(pairs.clients[i].?);
        pairs.clients[i] = null;
    }

    fn deinit(pairs: *TestPairs) void {
        for (pairs.accepted) |one| platform.close(one);
        for (pairs.clients) |client| if (client) |handle| platform.close(handle);
        platform.close(pairs.listening);
        testing.allocator.free(pairs.accepted);
        testing.allocator.free(pairs.clients);
        pairs.security.deinit();
    }
};

/// A poller that a test runs on its own thread: stopped, drained and its handles closed at the end.
const TestPoller = struct {
    poller: platform.Poller,

    fn init() !TestPoller {
        return .{ .poller = try platform.Poller.init() };
    }

    fn deinit(test_poller: *TestPoller) void {
        test_poller.poller.stop();
        test_poller.poller.drain();
        for (test_poller.poller.handles()) |handle| if (handle) |h| platform.close(h);
    }

    /// The next connection the poller reports, which must be the only one.
    fn next(test_poller: *TestPoller) !*platform.Watched {
        var ready: [4]*platform.Watched = undefined;
        while (true) {
            const reported = test_poller.poller.wait(&ready, null) orelse return error.TestUnexpectedResult;
            try testing.expect(!reported.listening);
            if (reported.ready.len == 0) continue; // the OS ended the wait early
            try testing.expectEqual(@as(usize, 1), reported.ready.len);
            return reported.ready[0];
        }
    }
};

/// Where the OS limits a process's descriptors (Linux, macOS), raises the soft limit to `count`; false if the hard
/// limit is lower.
fn allowHandles(count: usize) bool {
    if (builtin.os.tag == .windows) return true;
    var limit = std.posix.getrlimit(.NOFILE) catch return false;
    if (limit.cur >= count) return true;
    if (limit.max < count) return false;
    limit.cur = count;
    std.posix.setrlimit(.NOFILE, limit) catch return false;
    return true;
}

fn sendBytes(connection: Handle, bytes: []const u8) !void {
    try testing.expect(platform.sendFrame(connection, bytes, null));
}

test "poller: of 1024 connections whose peers close in random order, on another thread, each is reported ended once" {
    const count = 1024;
    if (!allowHandles(2 * count + 256)) return error.SkipZigTest;
    var pairs = try TestPairs.open(count);
    defer pairs.deinit();
    var test_poller = try TestPoller.init();
    defer test_poller.deinit();
    const poller = &test_poller.poller;
    const watched = try testing.allocator.alloc(platform.Watched, count);
    defer testing.allocator.free(watched);
    for (watched, pairs.accepted) |*one, connection| try poller.watch(one, connection);

    const Closer = struct {
        fn run(closing: *TestPairs) void {
            var order: [count]usize = undefined;
            for (&order, 0..) |*index, i| index.* = i;
            var prng: std.Random.DefaultPrng = .init(0x5eed);
            prng.random().shuffle(usize, &order);
            for (order) |i| closing.closeClient(i);
        }
    };
    const closer = try std.Thread.spawn(.{}, Closer.run, .{&pairs});
    defer closer.join();
    var reported: [count]bool = @splat(false);
    var ended: usize = 0;
    var ready: [64]*platform.Watched = undefined;
    while (ended < count) {
        const batch = poller.wait(&ready, null) orelse return error.TestUnexpectedResult;
        for (batch.ready) |one| {
            const i = (@intFromPtr(one) - @intFromPtr(watched.ptr)) / @sizeOf(platform.Watched);
            try testing.expect(!reported[i]);
            try testing.expectEqual(platform.Taken.ended, poller.take(one));
            reported[i] = true;
            ended += 1;
            poller.unwatch(one);
        }
    }
}

test "poller: a part of a frame is kept until the rest makes it whole; a frame, then the end of the stream" {
    var pairs = try TestPairs.open(2);
    defer pairs.deinit();
    var test_poller = try TestPoller.init();
    defer test_poller.deinit();
    const poller = &test_poller.poller;
    var watched: [2]platform.Watched = undefined;
    for (&watched, pairs.accepted) |*one, connection| try poller.watch(one, connection);
    var frame: [64]u8 = undefined;
    for (&frame, 0..) |*byte, i| byte.* = @intCast(i);

    // A part, then the rest. Where the poller reports the part (Windows reads on by itself), it is kept
    try sendBytes(pairs.clients[0].?, frame[0..10]);
    if (builtin.os.tag != .windows) {
        const part = try test_poller.next();
        try testing.expectEqual(&watched[0], part);
        try testing.expectEqual(platform.Taken.partial, poller.take(part));
    }
    try sendBytes(pairs.clients[0].?, frame[10..]);
    const whole = try test_poller.next();
    try testing.expectEqual(&watched[0], whole);
    try testing.expectEqual(platform.Taken.whole, poller.take(whole));
    try testing.expectEqualSlices(u8, &frame, &whole.frame);
    poller.unwatch(whole);

    // A whole frame and the end at once: the frame first
    try sendBytes(pairs.clients[1].?, &frame);
    pairs.closeClient(1);
    const last = try test_poller.next();
    try testing.expectEqual(&watched[1], last);
    try testing.expectEqual(platform.Taken.whole, poller.take(last));
    try testing.expectEqualSlices(u8, &frame, &last.frame);
    if (builtin.os.tag != .windows) {
        // Level-triggered: reported again, at its end
        try testing.expectEqual(&watched[1], try test_poller.next());
        try testing.expectEqual(platform.Taken.ended, poller.take(last));
    }
    poller.unwatch(last);
}

test "poller: a connection that ended before its watch is reported at once" {
    var pairs = try TestPairs.open(1);
    defer pairs.deinit();
    var test_poller = try TestPoller.init();
    defer test_poller.deinit();
    pairs.closeClient(0);
    var watched: platform.Watched = undefined;
    try test_poller.poller.watch(&watched, pairs.accepted[0]);
    const ended = try test_poller.next();
    try testing.expectEqual(&watched, ended);
    try testing.expectEqual(platform.Taken.ended, test_poller.poller.take(ended));
    test_poller.poller.unwatch(ended);
}

test "poller: a stop from another thread ends a wait on quiet connections, for good" {
    var pairs = try TestPairs.open(4);
    defer pairs.deinit();
    var test_poller = try TestPoller.init();
    defer test_poller.deinit();
    var watched: [4]platform.Watched = undefined;
    for (&watched, pairs.accepted) |*one, connection| try test_poller.poller.watch(one, connection);
    const Stopper = struct {
        fn run(poller: *platform.Poller) void {
            letThemBlock();
            poller.stop();
        }
    };
    const stopper = try std.Thread.spawn(.{}, Stopper.run, .{&test_poller.poller});
    var ready: [4]*platform.Watched = undefined;
    const start = nowMs();
    while (test_poller.poller.wait(&ready, null)) |polled| try testing.expectEqual(@as(usize, 0), polled.ready.len);
    try testing.expect(nowMs() - start < 2000);
    stopper.join();
    try testing.expectEqual(@as(?platform.Polled, null), test_poller.poller.wait(&ready, null));
}

test "poller: a timeout ends a wait on quiet connections with nothing" {
    var pairs = try TestPairs.open(1);
    defer pairs.deinit();
    var test_poller = try TestPoller.init();
    defer test_poller.deinit();
    var watched: platform.Watched = undefined;
    try test_poller.poller.watch(&watched, pairs.accepted[0]);
    var ready: [4]*platform.Watched = undefined;
    const start = nowMs();
    while (nowMs() - start < 20) {
        const polled = test_poller.poller.wait(&ready, 20) orelse return error.TestUnexpectedResult;
        try testing.expectEqual(@as(usize, 0), polled.ready.len);
        try testing.expect(!polled.listening);
    }
    try testing.expect(nowMs() - start < 2000);
    test_poller.poller.unwatch(&watched);
}

test "poller: a client on the listening handle is reported while listening, and accepted without a wait" {
    var pairs = try TestPairs.open(0);
    defer pairs.deinit();
    var test_poller = try TestPoller.init();
    defer test_poller.deinit();
    const poller = &test_poller.poller;
    var ready: [4]*platform.Watched = undefined;
    try poller.listen(pairs.listening);
    // Nobody yet: a wait times out with nothing
    const quiet = poller.wait(&ready, 10) orelse return error.TestUnexpectedResult;
    try testing.expect(!quiet.listening);

    const address = try platform.addressOf(pairs.name());
    const client = try connected(try platform.connect(&address, &pairs.security));
    defer platform.close(client);
    const start = nowMs();
    while (true) {
        const polled = poller.wait(&ready, 1000) orelse return error.TestUnexpectedResult;
        if (polled.listening) break;
        try testing.expect(nowMs() - start < 5000);
    }
    const connection = try accepted(platform.acceptWithin(pairs.listening, null, 0, null));
    if (connection != pairs.listening) {
        platform.close(connection);
    } else {
        // Where the client took the listening handle itself, another one listens next
        pairs.listening = (try platform.nextListening(&address, &pairs.security)).?;
        _ = platform.dropClient(connection, false);
        platform.close(connection);
    }
    // Not listening: a second client isn't reported
    poller.unlisten();
    const second = try connected(try platform.connect(&address, &pairs.security));
    defer platform.close(second);
    const unreported = poller.wait(&ready, 20) orelse return error.TestUnexpectedResult;
    try testing.expect(!unreported.listening);
    // Listening again: it is
    try poller.listen(pairs.listening);
    const again = nowMs();
    while (true) {
        const polled = poller.wait(&ready, 1000) orelse return error.TestUnexpectedResult;
        if (polled.listening) break;
        try testing.expect(nowMs() - again < 5000);
    }
    poller.unlisten();
}

// === Segments and ring waits ===

/// A segment as its creator makes it, in this process: the mapping, its handle where passed, and its session id.
const TestSegment = struct {
    mapping: platform.Mapping,
    handle: ?Handle,
    security: platform.Security,
    id: [16]u8,

    fn init(size: usize, kind: platform.SegmentKind) !TestSegment {
        var security = try platform.Security.init();
        errdefer security.deinit();
        const handle: ?Handle = if (platform.passes_segment_handle) try platform.createSegmentHandle() else null;
        errdefer if (handle) |h| platform.close(h);
        var id: [16]u8 = undefined;
        testing.io.random(&id);
        const mapping = try platform.createSegment(handle, &id, size, &security, kind);
        return .{ .mapping = mapping, .handle = handle, .security = security, .id = id };
    }

    fn deinit(segment: *TestSegment) void {
        segment.mapping.unmap();
        if (segment.handle) |h| platform.close(h);
        segment.security.deinit();
    }

    /// A wait word in the mapping.
    fn word(segment: *TestSegment) *std.atomic.Value(u32) {
        return @ptrCast(@alignCast(segment.mapping.base + 1024));
    }
};

test "segment mappings are counted: one more for each mapping, none left after the unmaps" {
    var security = try platform.Security.init();
    defer security.deinit();
    const handle: ?Handle = if (platform.passes_segment_handle) try platform.createSegmentHandle() else null;
    defer if (handle) |h| platform.close(h);
    var id: [16]u8 = undefined;
    testing.io.random(&id);
    const size = 64 * 1024;
    const before = platform.segmentMappingCount();
    try testing.expectEqual(before, platform.segmentMappingCount());
    var first = try platform.createSegment(handle, &id, size, &security, .stream);
    try testing.expectEqual(before + 1, platform.segmentMappingCount());
    var second = try platform.openSegment(handle, &id, size, .stream);
    try testing.expectEqual(before + 2, platform.segmentMappingCount());
    second.unmap();
    try testing.expectEqual(before + 1, platform.segmentMappingCount());
    first.unmap();
    try testing.expectEqual(before, platform.segmentMappingCount());
}

test "ring waits: a flag that isn't 1 returns at once, a timeout waits that long, a wake ends the wait" {
    var segment = try TestSegment.init(64 * 1024, .stream);
    defer segment.deinit();
    const flag = segment.word();
    const event = segment.mapping.ringEvent(.s2c_data);

    flag.store(0, .monotonic);
    var start = nowMs();
    try testing.expect(!platform.ringWait(flag, event, 2000));
    try testing.expect(nowMs() - start < 1000);

    _ = flag.swap(1, .acq_rel);
    start = nowMs();
    try testing.expect(platform.ringWait(flag, event, 30));
    const waited = nowMs() - start;
    try testing.expect(waited >= 25 and waited < 1000);

    // A waiter on another thread: the publisher swaps the flag to 0, then wakes it
    const Waiter = struct {
        fn run(waiter_flag: *std.atomic.Value(u32), waiter_event: platform.RingEvent, woke_after: *i64) void {
            const t0 = nowMs();
            _ = waiter_flag.swap(1, .acq_rel);
            while (waiter_flag.load(.acquire) == 1 and nowMs() - t0 < 5000) _ = platform.ringWait(waiter_flag, waiter_event, 5000);
            woke_after.* = nowMs() - t0;
        }
    };
    flag.store(0, .monotonic);
    var woke_after: i64 = std.math.maxInt(i64);
    const thread = try std.Thread.spawn(.{}, Waiter.run, .{ flag, event, &woke_after });
    while (flag.load(.acquire) == 0) std.Thread.yield() catch {};
    platform.sleep(20);
    if (flag.swap(0, .acq_rel) == 1) platform.ringWake(flag, event);
    thread.join();
    try testing.expect(woke_after < 2000);
}

// === Broadcast waits, and the revoke ===

/// One subscriber's slot wake as both sides hold it: the owner's, which it creates, and the subscriber's, which it
/// opens.
const TestSlot = struct {
    owner: platform.SlotWake,
    own: platform.SlotWake,

    fn init(channel: *const TestSegment, slot: u32) !TestSlot {
        const owner = try platform.createSlotWake(&channel.id, "-bc-wake-", slot, &channel.security);
        errdefer platform.closeSlotWake(owner);
        return .{ .owner = owner, .own = try platform.openSlotWake(&channel.id, "-bc-wake-", slot) };
    }

    fn deinit(slot: TestSlot) void {
        platform.closeSlotWake(slot.own);
        platform.closeSlotWake(slot.owner);
    }
};

test "broadcast waits: the writer's wake ends the wait of every sleeper, once the word moved" {
    var channel = try TestSegment.init(4096, .broadcast);
    defer channel.deinit();
    const word = channel.word();
    const Sleeper = struct {
        fn run(sleep_word: *std.atomic.Value(u32), own: platform.SlotWake, started: *std.atomic.Value(u32), woke_after: *i64) void {
            const t0 = nowMs();
            _ = started.fetchAdd(1, .seq_cst);
            while (sleep_word.load(.acquire) == 0 and nowMs() - t0 < 10_000) _ = platform.broadcastWait(sleep_word, 0, own, 5000);
            woke_after.* = nowMs() - t0;
        }
    };
    const n = 8;
    var slots: [n]TestSlot = undefined;
    var made: usize = 0;
    defer for (slots[0..made]) |slot| slot.deinit();
    for (&slots, 0..) |*slot, i| {
        slot.* = try TestSlot.init(&channel, @intCast(i));
        made += 1;
    }
    var woke_after: [n]i64 = @splat(std.math.maxInt(i64));
    var started: std.atomic.Value(u32) = .init(0);
    var threads: [n]std.Thread = undefined;
    for (&threads, slots, &woke_after) |*thread, slot, *after| {
        thread.* = try std.Thread.spawn(.{}, Sleeper.run, .{ word, slot.own, &started, after });
    }
    while (started.load(.seq_cst) < n) std.Thread.yield() catch {};
    letThemBlock();
    word.store(1, .seq_cst);
    if (platform.wakes_each_slot) {
        for (slots) |slot| platform.setSlotWake(slot.owner);
    } else platform.broadcastWake(word);
    for (threads) |thread| thread.join();
    for (woke_after) |after| try testing.expect(after < 2000);
}

test "broadcast waits: a timeout waits that long, and a wait on a word that moved returns at once" {
    var channel = try TestSegment.init(4096, .broadcast);
    defer channel.deinit();
    const word = channel.word();
    const slot = try TestSlot.init(&channel, 0);
    defer slot.deinit();
    var start = nowMs();
    try testing.expect(platform.broadcastWait(word, 0, slot.own, 30));
    const waited = nowMs() - start;
    try testing.expect(waited >= 25 and waited < 1000);
    if (!platform.wakes_each_slot) {
        word.store(7, .seq_cst);
        start = nowMs();
        try testing.expect(!platform.broadcastWait(word, 6, slot.own, 2000));
        try testing.expect(nowMs() - start < 1000);
    }
}

test "slot wakes: each ends its own slot's wait only, or its next wait once; they go with their last handle" {
    if (!platform.wakes_each_slot) return error.SkipZigTest;
    const Sleeper = struct {
        fn run(sleep_word: *std.atomic.Value(u32), own: platform.SlotWake, ms: u32, timed_out: *bool, after: *i64) void {
            const t0 = nowMs();
            timed_out.* = platform.broadcastWait(sleep_word, 0, own, ms);
            after.* = nowMs() - t0;
        }
    };
    var channel = try TestSegment.init(4096, .broadcast);
    defer channel.deinit();
    const word = channel.word();
    {
        const woken = try TestSlot.init(&channel, 1023);
        defer woken.deinit();
        const left = try TestSlot.init(&channel, 7);
        defer left.deinit();
        var timed_out: [2]bool = .{ true, false };
        var after: [2]i64 = @splat(0);
        const woken_thread = try std.Thread.spawn(.{}, Sleeper.run, .{ word, woken.own, 5000, &timed_out[0], &after[0] });
        const left_thread = try std.Thread.spawn(.{}, Sleeper.run, .{ word, left.own, 300, &timed_out[1], &after[1] });
        letThemBlock();
        platform.setSlotWake(woken.owner);
        woken_thread.join();
        left_thread.join();
        try testing.expect(!timed_out[0] and after[0] < 250);
        try testing.expect(timed_out[1] and after[1] >= 250);

        // Set with no sleeper (also by the subscriber itself, its own interrupt): the next wait ends, once
        platform.setSlotWake(left.own);
        try testing.expect(!platform.broadcastWait(word, 0, left.own, 1000));
        try testing.expect(platform.broadcastWait(word, 0, left.own, 1));

        // The owner's create of a wake a former subscriber still holds takes it as it is
        const again = try platform.createSlotWake(&channel.id, "-bc-wake-", 7, &channel.security);
        defer platform.closeSlotWake(again);
        platform.setSlotWake(again);
        try testing.expect(!platform.broadcastWait(word, 0, left.own, 1000));
    }
    try testing.expectError(error.Gone, platform.openSlotWake(&channel.id, "-bc-wake-", 7));
}

test "protect, then revoke: read-only, then inaccessible; waits and wakes on a revoked word fail without a fault" {
    const mappings = platform.segmentMappingCount();
    var channel = try TestSegment.init(64 * 1024, .broadcast);
    const word = channel.word();
    word.store(3, .seq_cst);
    const slot = try TestSlot.init(&channel, 0);
    defer slot.deinit();
    try testing.expectEqual(platform.Protection.read_write, platform.protectionOf(word));
    try platform.protectReadOnly(&channel.mapping, 64 * 1024);
    try testing.expectEqual(platform.Protection.read_only, platform.protectionOf(word));
    platform.revoke(&channel.mapping);
    try testing.expectEqual(platform.Protection.none, platform.protectionOf(word));
    const start = nowMs();
    _ = platform.broadcastWait(word, 3, slot.own, 20);
    try testing.expect(nowMs() - start < 1000);
    platform.broadcastWake(word);
    channel.deinit();
    try testing.expectEqual(mappings, platform.segmentMappingCount());
}
