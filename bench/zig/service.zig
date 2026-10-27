//! A service: `clients` client threads send requests of one size and receive their replies, each keeping up to
//! `in_flight` requests awaiting their replies (one: each request a call, as a binding's `call` makes it), and
//! `servers` server threads take the requests of every client and reply to each with its own bytes. Each client and
//! server is a member joined through `fipc_service_join` (its own mapping of the service, as in another process), the
//! service owned by this thread (`fipc_service_create`, its max_message the case's size).
//!
//! The clients warm up (run 0, `warmup_ns`), then make `runs` runs of about `duration_ns` each (with `messages` set,
//! that many round trips between them), sending copied (`fipc_send`) or in place (`fipc_send_acquire`/`_commit`), and
//! receiving the replies with `fipc_recv`, or in place (`fipc_recv_acquire`/`_release`, each reply copied out unless
//! --no-touch). A server receives each request copied and replies with `fipc_send`, or receives it in place and copies
//! it into the reply's room (`fipc_send_acquire`/`_commit`). A run ends once every client has received the reply to
//! every request it sent in the run: its rate is the replies over the time from its start to then. A run's metrics:
//! that rate, and the rate per client.
//!
//! Each client counts its replies in a line of its own, so the counts add no shared write to the hot path.

const std = @import("std");
const fipc = @import("fipc.zig");
const scenarios = @import("scenarios.zig");
const throughput = @import("throughput.zig");
const timing = @import("timing.zig");

const Ctx = scenarios.Ctx;
const Results = scenarios.Results;
const Scenario = scenarios.Scenario;

const join_timeout_ms = throughput.connect_timeout_ms;
/// The most clients and servers a scenario starts.
const max_members = 8;

/// A client's count, alone in its cache line.
const Count = struct {
    value: std.atomic.Value(u64) align(std.atomic.cache_line) = .init(0),
};

/// What the owner's thread and the members share.
const Shared = struct {
    ctx: *const Ctx,
    zero_copy: bool,
    /// The requests a client keeps awaiting their replies.
    in_flight: u64,
    /// The run the clients make now: 0 the warm-up, then 1 to `runs`; `runs + 1` ends them.
    run: std.atomic.Value(u32) = .init(std.math.maxInt(u32)),
    /// The clients send no more requests this run (they receive the replies still to come).
    stop: std.atomic.Value(bool) = .init(false),
    /// Clients done with the run: every reply received.
    clients_done: std.atomic.Value(u32) = .init(0),
    /// Round trips per client per run, if counted (`messages`); else 0.
    per_client: u64,
    replies: [max_members]Count = @splat(.{}),
    /// A member's failure: the run stops, and the case fails.
    failed: std.atomic.Value(bool) = .init(false),

    fn total(counts: []const Count) u64 {
        var sum: u64 = 0;
        for (counts) |*count| sum += count.value.load(.acquire);
        return sum;
    }
};

/// The owner: claims the service, starts the members and waits until they are joined, times the runs, and ends the
/// service (its close), after which the servers leave.
pub fn run(ctx: *const Ctx, scenario: *const Scenario, results: *Results) !void {
    std.debug.assert(scenario.clients <= max_members and scenario.servers <= max_members);
    const io = ctx.io;
    const service = try fipc.serviceCreate(ctx.name, ctx.ring, ctx.size, scenario.clients, scenario.servers);
    var open = true;
    defer if (open) fipc.closeService(service);

    // A request holds its length and max_message, each rounded up to 64 bytes, of its client's ring until its reply
    // is received: no more in flight than the ring holds, or a client would wait for room only its replies free
    const per_request = 2 * std.mem.alignForward(usize, @max(ctx.size, 1), 64);
    var shared: Shared = .{
        .ctx = ctx,
        .zero_copy = scenario.zero_copy,
        .in_flight = @max(1, @min(scenario.in_flight, ctx.ring / per_request)),
        .per_client = if (ctx.messages > 0) std.math.divCeil(u64, ctx.messages, scenario.clients) catch unreachable else 0,
    };
    var threads: [2 * max_members]std.Thread = undefined;
    var started: usize = 0;
    defer for (threads[0..started]) |thread| thread.join();
    defer if (open) {
        shared.failed.store(true, .release);
        shared.run.store(ctx.runs + 1, .release);
        fipc.closeService(service);
        open = false;
    };
    for (0..scenario.servers) |k| {
        threads[started] = try std.Thread.spawn(.{}, server, .{ &shared, k });
        started += 1;
    }
    for (0..scenario.clients) |k| {
        threads[started] = try std.Thread.spawn(.{}, client, .{ &shared, k });
        started += 1;
    }
    try fipc.waitForServiceMembers(service, scenario.clients + scenario.servers, join_timeout_ms);

    const clients = shared.replies[0..scenario.clients];
    var r: u32 = if (ctx.warmup_ns > 0) 0 else 1;
    while (r <= ctx.runs) : (r += 1) {
        const budget_ns = if (r == 0) ctx.warmup_ns else ctx.duration_ns;
        const replies_before = Shared.total(clients);
        shared.stop.store(false, .release);
        shared.clients_done.store(0, .release);
        const start = timing.now(io);
        shared.run.store(r, .release);
        if (shared.per_client == 0 or r == 0) {
            timing.spinUntil(io, start + budget_ns);
            shared.stop.store(true, .release);
        }
        while (shared.clients_done.load(.acquire) < scenario.clients) {
            if (shared.failed.load(.acquire)) return error.MemberFailed;
            std.Thread.yield() catch {};
        }
        const elapsed = timing.now(io) - start;
        if (r == 0) continue;
        const rate = @as(f64, @floatFromInt(Shared.total(clients) - replies_before)) / timing.seconds(elapsed);
        try results.record(ctx.gpa, &.{ rate, rate / @as(f64, @floatFromInt(scenario.clients)) });
    }
    shared.run.store(ctx.runs + 1, .release);
    fipc.closeService(service); // the end: each server's receive returns Disconnected, and it leaves
    open = false;
}

fn client(shared: *Shared, k: usize) void {
    callRuns(shared, k) catch |err| {
        std.log.err("client {d}: {s}", .{ k, @errorName(err) });
        shared.failed.store(true, .release);
    };
}

fn callRuns(shared: *Shared, k: usize) !void {
    const ctx = shared.ctx;
    const conn = try fipc.serviceJoin(ctx.name, fipc.client, join_timeout_ms);
    defer fipc.close(conn);
    const payload = try throughput.payloadFor(ctx);
    defer ctx.gpa.free(payload);
    const buffer = try throughput.hotBuffer(ctx, ctx.size);
    defer ctx.gpa.free(buffer);
    var received: u64 = 0;
    var done: u32 = std.math.maxInt(u32);
    while (true) {
        const r = shared.run.load(.acquire);
        if (r == done or r == std.math.maxInt(u32)) {
            std.Thread.yield() catch {};
            continue;
        }
        if (r > ctx.runs) return;
        const count = if (r > 0) shared.per_client else 0;
        // Requests while the run lasts (its count, or until stop; stop is read every 64 requests), up to in_flight
        // awaiting their replies; then the replies still to come
        var sent: u64 = 0;
        var replies: u64 = 0;
        while (true) {
            const more = if (count > 0) sent < count else (sent % 64 != 0 or !shared.stop.load(.acquire));
            if (more and sent - replies < shared.in_flight) {
                try request(shared, conn, payload);
                sent += 1;
            } else if (replies < sent) {
                try reply(shared, conn, buffer);
                replies += 1;
            } else break;
        }
        received += replies;
        shared.replies[k].value.store(received, .release);
        done = r;
        _ = shared.clients_done.fetchAdd(1, .acq_rel);
    }
}

inline fn request(shared: *const Shared, conn: *fipc.Conn, payload: []const u8) !void {
    if (shared.zero_copy) {
        const room = try fipc.sendAcquire(conn, payload.len, fipc.forever);
        @memcpy(room, payload);
        try fipc.sendCommit(conn, payload.len);
    } else try fipc.send(conn, payload, fipc.forever);
}

/// The reply to the oldest request: its own bytes back (Abandoned, a server's end, fails the case).
inline fn reply(shared: *const Shared, conn: *fipc.Conn, buffer: []u8) !void {
    if (shared.zero_copy) {
        const bytes = try fipc.recvAcquire(conn, fipc.forever);
        defer fipc.recvRelease(conn);
        if (bytes.len != buffer.len) return error.UnexpectedMessage;
        if (shared.ctx.touch) @memcpy(buffer, bytes);
    } else {
        const bytes = try fipc.recv(conn, buffer, fipc.forever);
        if (bytes.len != buffer.len) return error.UnexpectedMessage;
    }
}

fn server(shared: *Shared, k: usize) void {
    serve(shared) catch |err| {
        std.log.err("server {d}: {s}", .{ k, @errorName(err) });
        shared.failed.store(true, .release);
    };
}

/// Replies to each request with its bytes until the service ends.
fn serve(shared: *Shared) !void {
    const ctx = shared.ctx;
    const conn = try fipc.serviceJoin(ctx.name, fipc.server, join_timeout_ms);
    defer fipc.close(conn);
    const buffer = try throughput.hotBuffer(ctx, ctx.size);
    defer ctx.gpa.free(buffer);
    while (true) {
        if (shared.zero_copy) {
            const bytes = fipc.recvAcquire(conn, fipc.forever) catch |err| switch (err) {
                error.Disconnected => return,
                else => return err,
            };
            // The request stays in place until the reply's commit
            const room = fipc.sendAcquire(conn, bytes.len, fipc.forever) catch |err| switch (err) {
                error.Disconnected => return,
                else => return err,
            };
            @memcpy(room, bytes);
            fipc.sendCommit(conn, bytes.len) catch |err| switch (err) {
                error.Disconnected => return,
                else => return err,
            };
        } else {
            const bytes = fipc.recv(conn, buffer, fipc.forever) catch |err| switch (err) {
                error.Disconnected => return,
                else => return err,
            };
            fipc.send(conn, bytes, fipc.forever) catch |err| switch (err) {
                error.Disconnected => return,
                else => return err,
            };
        }
    }
}
