//! The ring wake-ups (docs/protocol.md §6.1): a pinned ping-pong whose pings land as the receiver announces its sleep.
//! A lost wake-up leaves a receiver asleep for a whole 100 ms slice with its message waiting, so no message may wait
//! 90 ms or more for its receiver.

const std = @import("std");
const builtin = @import("builtin");
const support = @import("../support.zig");
const c = support.c;

const ring_bytes = 64 * 1024;
/// At most this many round trips, for at most `budget_ns` (a TSan build spins far slower).
const round_trips = 30_000;
const budget_ns = 30 * std.time.ns_per_s;
/// A message that waited this long slept through its wake-up: a slice is 100 ms, and a round takes well under 1 ms.
const lost_wakeup_ns = 90 * std.time.ns_per_ms;

/// One message each way: the round's number (0 ends the ping-pong), the sender's clock just before it sent the
/// message, and, in a reply, whether the ping's receive slept.
const Message = extern struct { round: u64, sent_ns: u64, slept: u64 };

/// Messages that waited `lost_wakeup_ns` or more from their send to the end of their receive.
const Stalls = struct {
    count: u32 = 0,
    longest_ns: u64 = 0,

    fn record(stalls: *Stalls, waited_ns: u64) void {
        if (waited_ns < lost_wakeup_ns) return;
        stalls.count += 1;
        stalls.longest_ns = @max(stalls.longest_ns, waited_ns);
    }
};

const Side = struct {
    stream: support.Conn,
    other: support.Conn,
    cpu: ?u32,
    stalls: Stalls = .{},
    failed: ?[]const u8 = null,

    fn start(side: *Side) void {
        if (side.cpu) |cpu| _ = support.pinThread(cpu);
    }

    fn send(side: *Side, round: u64, slept: bool) bool {
        const msg: Message = .{ .round = round, .sent_ns = support.nowNs(), .slept = @intFromBool(slept) };
        const result = c.fipc_send(side.stream, &msg, @sizeOf(Message), -1);
        if (result != c.FIPC_OK) return side.fail(support.resultName(result));
        return true;
    }

    /// Receives the message of round `round` (or the end, round 0) and records how long it waited; `receive_ns` is
    /// how long the receive took.
    fn receive(side: *Side, round: u64, msg: *Message, receive_ns: *u64) bool {
        var len: usize = 0;
        const start_ns = support.nowNs();
        const result = c.fipc_recv(side.stream, msg, @sizeOf(Message), &len, -1);
        const end_ns = support.nowNs();
        receive_ns.* = end_ns - start_ns;
        if (result != c.FIPC_OK) return side.fail(support.resultName(result));
        if (len != @sizeOf(Message)) return side.fail("a message of the wrong length");
        if (msg.round != round and msg.round != 0) return side.fail("a message of another round");
        side.stalls.record(end_ns -| msg.sent_ns);
        return true;
    }

    /// Records the failure and cancels both connections, so the other side's receive ends too; false.
    fn fail(side: *Side, what: []const u8) bool {
        side.failed = what;
        c.fipc_cancel(side.stream);
        c.fipc_cancel(side.other);
        return false;
    }
};

/// The receiving side under test: receives each ping and replies at once, saying whether the receive outlasted its
/// spin phase (it announced its sleep).
const Ponger = struct {
    side: Side,
    /// The receive's spin phase, measured on this side's CPU before the first round (0 until then).
    spin_ns: std.atomic.Value(u64) = .init(0),
    slept: u32 = 0,

    fn run(ponger: *Ponger) void {
        ponger.side.start();
        ponger.spin_ns.store(spinPhaseNs(ponger.side.stream), .release);
        var msg: Message = undefined;
        var receive_ns: u64 = 0;
        var round: u64 = 1;
        while (ponger.side.receive(round, &msg, &receive_ns) and msg.round != 0) : (round += 1) {
            const slept = receive_ns > ponger.spin_ns.raw;
            ponger.slept += @intFromBool(slept);
            if (!ponger.side.send(round, slept)) return;
        }
    }

    /// The median of 101 receives with timeout 0 on the empty ring: each spins through its whole spin phase, then
    /// times out.
    fn spinPhaseNs(stream: support.Conn) u64 {
        var samples: [101]u64 = undefined;
        for (&samples) |*sample| {
            var buf: Message = undefined;
            var len: usize = 0;
            const start_ns = support.nowNs();
            _ = c.fipc_recv(stream, &buf, @sizeOf(Message), &len, 0);
            sample.* = support.nowNs() - start_ns;
        }
        std.mem.sort(u64, &samples, {}, std.sort.asc(u64));
        return samples[samples.len / 2];
    }
};

/// The sending side: sends each ping a think time after the last reply arrived, when the ponger has spun for about
/// that long. The think time follows the ponger's reports: longer after a receive that slept, shorter after one that
/// caught the ping while spinning, so the pings keep landing where the receiver announces its sleep, the moment a
/// wake-up is easiest to lose.
const Pinger = struct {
    side: Side,
    ponger: *const Ponger,
    think_ns: u64 = 0,
    rounds: u32 = 0,

    fn run(pinger: *Pinger) void {
        pinger.side.start();
        while (pinger.ponger.spin_ns.load(.acquire) == 0) std.atomic.spinLoopHint();
        const spin_ns = pinger.ponger.spin_ns.raw;
        const step_ns = @max(spin_ns / 1000, 50);
        pinger.think_ns = spin_ns;
        var msg: Message = undefined;
        var receive_ns: u64 = 0;
        const end_ns = support.nowNs() + budget_ns;
        var reply_ns = support.nowNs();
        while (pinger.rounds < round_trips and reply_ns < end_ns) {
            const round = pinger.rounds + 1;
            support.spinUntil(reply_ns + pinger.think_ns);
            if (!pinger.side.send(round, false)) return;
            if (!pinger.side.receive(round, &msg, &receive_ns)) return;
            reply_ns = support.nowNs();
            pinger.rounds = round;
            pinger.think_ns = if (msg.slept != 0) @max(pinger.think_ns -| step_ns, spin_ns / 4) else @min(pinger.think_ns + step_ns, spin_ns * 4);
        }
        _ = pinger.side.send(0, false); // the end
    }
};

// A ping-pong between the two connections of one process's pair, each side on its own CPU (the two CPUs of
// `support.twoCpus`). A reader's flag store that still sat in its CPU's store buffer while it re-read `head` would let
// the writer miss the flag, and the reader would sleep a whole slice. No message may wait 90 ms.
test "slow: a pinned ping-pong loses no wake-up" {
    // Not under TSan on macOS: there GitHub's 3-vCPU runner stalls a thread for about 100 ms at times, between the
    // pinger's clock read and its publish, which this test can't tell from a lost wake-up. The macOS run without TSan
    // covers it.
    if (builtin.sanitize_thread and support.macos) return error.SkipZigTest;
    var t = try support.beginTimed(@src(), 120);
    defer t.end();
    var name_buf: [64]u8 = undefined;
    var pair: support.Pair = .{};
    if (!pair.open(support.uniqueName(&name_buf, "test_wake_pingpong"), ring_bytes)) return t.fail("Failed to open the pair");
    defer pair.close();

    const cpus: [2]?u32 = if (support.twoCpus()) |two| .{ two[0], two[1] } else .{ null, null };
    var ponger: Ponger = .{ .side = .{ .stream = pair.client, .other = pair.server, .cpu = cpus[1] } };
    var pinger: Pinger = .{ .side = .{ .stream = pair.server, .other = pair.client, .cpu = cpus[0] }, .ponger = &ponger };
    const start_ms = support.nowMs();
    const pong_thread = std.Thread.spawn(.{}, Ponger.run, .{&ponger}) catch return t.fail("Failed to start the ponger");
    const ping_thread = std.Thread.spawn(.{}, Pinger.run, .{&pinger}) catch {
        c.fipc_cancel(pair.client);
        pong_thread.join();
        return t.fail("Failed to start the pinger");
    };
    ping_thread.join();
    pong_thread.join();

    const stalls = ponger.side.stalls.count + pinger.side.stalls.count;
    std.debug.print("  {d} round trips in {d} ms on CPUs {?d} and {?d}: spin phase {d} us, think time {d} us at the end; " ++
        "the ponger's receive slept in {d} rounds; {d} messages waited 90 ms or more (longest {d} ms)\n", .{
        pinger.rounds,
        support.nowMs() - start_ms,
        cpus[0],
        cpus[1],
        ponger.spin_ns.raw / std.time.ns_per_us,
        pinger.think_ns / std.time.ns_per_us,
        ponger.slept,
        stalls,
        @max(ponger.side.stalls.longest_ns, pinger.side.stalls.longest_ns) / std.time.ns_per_ms,
    });
    if (ponger.side.failed) |what| return t.fail(what);
    if (pinger.side.failed) |what| return t.fail(what);
    _ = t.expectEqual(stalls, 0, "No message waits through a lost wake-up");
    return t.done();
}
