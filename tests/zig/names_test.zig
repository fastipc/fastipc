//! A listener's name: one listener holds it at a time, a closed listener frees it at once and leaves nothing behind,
//! and names that aren't valid are refused. A session's segment has no name of its own (Linux: a memfd; Windows: a
//! random one), so nothing named outlives its connections. (A crashed listener: stream/peer_death_test.zig.)

const std = @import("std");
const support = @import("support.zig");
const c = support.c;

const RING_BYTES = 4096;

// A name another listener holds is FIPC_ADDR_IN_USE, which creates nothing; once that listener is closed, the name is
// free.
test "fast: a name another listener holds is ADDR_IN_USE" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    const name = support.uniqueName(&buf, "in_use");
    var first: support.Listener = null;
    try t.checkEqual(c.fipc_stream_create(name, RING_BYTES, &first), c.FIPC_OK, "the first listener");
    var second: support.Listener = null;
    _ = t.expectEqual(c.fipc_stream_create(name, RING_BYTES, &second), c.FIPC_ADDR_IN_USE, "a second listener on the name: ADDR_IN_USE");
    _ = t.expect(second == null, "and nothing was created");
    c.fipc_listener_close(first);
    _ = t.expect(support.freeAtOnce(name), "free once the first is closed");
    return t.done();
}

/// Listeners made on one name at the same time, on threads.
const ListenRacer = struct {
    name: [*:0]const u8,
    start: *std.atomic.Value(i32),
    listener: support.Listener = null,
    result: i32 = -1,

    fn raceToListen(racer: *ListenRacer) void {
        while (racer.start.load(.acquire) == 0) {}
        racer.result = @intCast(c.fipc_stream_create(racer.name, RING_BYTES, &racer.listener));
    }
};

// Of two listeners made on one name at the same time, exactly one claims it (the kernel lets one claim a name); the
// other gets FIPC_ADDR_IN_USE. 50 rounds.
test "fast: of listeners racing for a name, exactly one claims it" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    const name = support.uniqueName(&buf, "listen_race");
    const rounds = 50;
    var double_listeners: usize = 0;
    var no_listener: usize = 0;
    var other_results: usize = 0;
    for (0..rounds) |_| {
        var start: std.atomic.Value(i32) = .init(0);
        var racers: [2]ListenRacer = undefined;
        var threads: [2]std.Thread = undefined;
        for (&racers, &threads) |*racer, *thread| {
            racer.* = .{ .name = name, .start = &start };
            thread.* = std.Thread.spawn(.{}, ListenRacer.raceToListen, .{racer}) catch return t.fail("Failed to start a racer thread");
        }
        start.store(1, .release);
        for (threads) |thread| thread.join();

        var listeners: usize = 0;
        for (racers) |racer| {
            if (racer.result == c.FIPC_OK) {
                listeners += 1;
            } else if (racer.result != c.FIPC_ADDR_IN_USE) {
                other_results += 1;
            }
        }
        if (listeners > 1) double_listeners += 1;
        if (listeners == 0) no_listener += 1;
        for (racers) |racer| c.fipc_listener_close(racer.listener);
    }

    std.debug.print("  {d} of {d} rounds had two listeners, {d} none, {d} other results\n", .{ double_listeners, rounds, no_listener, other_results });
    _ = t.expectEqual(other_results, 0, "Every listen claims the name or finds it in use");
    _ = t.expectEqual(double_listeners, 0, "Only one listener claims the name");
    _ = t.expectEqual(no_listener, 0, "One listener claims the name");
    return t.done();
}

// After the listener's close, a new listener claims the name at once, whether the first one was alone or served a
// client (Windows: once the connection it accepted is closed too, since that connection's pipe instance holds the
// name).
test "fast: a closed listener's name is free at once" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    const name = support.uniqueName(&buf, "name_after_close");

    var alone: support.Listener = null;
    try t.checkEqual(c.fipc_stream_create(name, RING_BYTES, &alone), c.FIPC_OK, "a first listener");
    c.fipc_listener_close(alone);
    _ = t.expect(support.freeAtOnce(name), "after its close, a new listener claims the name at once");

    var pair: support.Pair = .{};
    try t.check(pair.open(name, RING_BYTES), "a pair");
    c.fipc_listener_close(pair.listener);
    pair.listener = null;
    if (!support.windows) _ = t.expect(support.freeAtOnce(name), "after the listener's close, a new listener claims the name at once");
    c.fipc_close(pair.server);
    pair.server = null;
    _ = t.expect(support.freeAtOnce(name), "after the accepted connection's close too");
    pair.close();
    return t.done();
}

/// POSIX: counts the /dev/shm entries whose name contains `pattern`.
fn countDevShmWith(pattern: []const u8) usize {
    var dir = std.Io.Dir.openDirAbsolute(support.io(), "/dev/shm", .{ .iterate = true }) catch return 0;
    defer dir.close(support.io());
    var it = dir.iterate();
    var count: usize = 0;
    while (it.next(support.io()) catch null) |entry| {
        if (std.mem.find(u8, entry.name, pattern) != null) count += 1;
    }
    return count;
}

// Nothing is left after a close: over 3 cycles of a listener, a client and their close, the name is free at once after
// each; POSIX: no /dev/shm entry carries this process's PID suffix (the segments are memfds, which /dev/shm never
// lists).
test "fast: nothing is left after a close" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [128]u8 = undefined;
    const name = support.uniqueName(&buf, "leak_check");

    for (0..3) |_| {
        var pair: support.Pair = .{};
        try t.check(pair.open(name, RING_BYTES), "a cycle's pair");
        pair.close();
        if (!t.expect(support.freeAtOnce(name), "the name is free at once after the close")) return error.TestFailed;
    }
    if (support.windows) return t.done();

    var suffix_buf: [32]u8 = undefined;
    const pid_suffix = std.mem.print(&suffix_buf, "_{d}", .{support.pid()}) catch unreachable;
    _ = t.expectEqual(countDevShmWith(pid_suffix), 0, "no /dev/shm entry with our PID suffix");
    return t.done();
}

// Names that aren't valid are INVALID for listen and connect, and the longest valid name works. The client takes the
// capacity the listener chose: its largest piece is its ring's, 8 KiB less the framing.
test "fast: names that aren't valid are refused, and the client takes the listener's capacity" {
    var t = try support.begin(@src(), 30);
    defer t.end();
    var buf: [64]u8 = undefined;
    var pair: support.Pair = .{};
    try t.check(pair.open(support.uniqueName(&buf, "capacity"), 2 * RING_BYTES), "a pair on a ring of 8 KiB");
    defer pair.close();
    var out: ?*anyopaque = null;
    _ = t.expectEqual(c.fipc_send_acquire(pair.client, 2 * RING_BYTES - 64, &out, 0), c.FIPC_OK, "the client's ring has the listener's capacity");
    _ = t.expectEqual(c.fipc_send_acquire(pair.client, 2 * RING_BYTES - 63, &out, 0), c.FIPC_TOO_LARGE, "and no more (fipc_max_piece)");

    const max_len: usize = 245;
    const long = &@as([247]u8, @splat('n'));
    for ([_][]const u8{ "", ".hidden", "-dash", "has space", "a/b", "a\\b", "caf\xc3\xa9", long[0 .. max_len + 1] }) |bad| {
        var z: [256]u8 = undefined;
        const bad_z = std.mem.printSentinel(&z, "{s}", .{bad}, 0) catch unreachable;
        var listener: support.Listener = null;
        var conn: support.Conn = null;
        if (!t.expectEqual(c.fipc_stream_create(bad_z, RING_BYTES, &listener), c.FIPC_INVALID, "listen on a name that isn't valid: INVALID")) std.debug.print("  name: '{s}'\n", .{bad});
        if (!t.expectEqual(c.fipc_stream_connect(bad_z, &conn, 0), c.FIPC_INVALID, "connect to a name that isn't valid: INVALID")) std.debug.print("  name: '{s}'\n", .{bad});
        c.fipc_listener_close(listener);
    }
    var longest_z: [256]u8 = undefined;
    const longest = std.mem.printSentinel(&longest_z, "{s}", .{long[0..max_len]}, 0) catch unreachable;
    _ = t.expect(support.freeAtOnce(longest), "The longest valid name works");
    return t.done();
}
