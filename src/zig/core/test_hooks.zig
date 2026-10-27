//! The tests' hooks: compiled in only with `build_options.test_hooks` (the static library and the tests), and reached
//! from the Zig module as `fastipc.test_hooks` (root.zig). The library calls into them only under
//! `if (build_options.test_hooks)`, so the shipped library compiles none of this file. They are the crash steps of the
//! lifecycle (`crashIfAt`), the holds that pace a race the tests need, the strays that end a membership as a protocol
//! error would, and the reads of a connection's state a test looks at (and a client's longest sleep).

const std = @import("std");
const builtin = @import("builtin");
const Io = std.Io;
const broadcast_client = @import("../broadcast/client.zig");
const broadcast_segment = @import("../broadcast/segment.zig");
const broadcast_server = @import("../broadcast/server.zig");
const calls = @import("calls.zig");
const platform = @import("../platform.zig");
const queue_client = @import("../queue/client.zig");
const queue_segment = @import("../queue/segment.zig");
const queue_server = @import("../queue/server.zig");
const service_client = @import("../service/client.zig");
const service_segment = @import("../service/segment.zig");
const service_server = @import("../service/server.zig");

const Endpoint = @import("endpoint.zig").Endpoint;

// === Crash steps ===

/// A step of the handshake on either side, or the watcher's start: the point right after it.
pub const Step = enum {
    /// The listener accepted a client (its connection); a broadcast's, a queue's or a service's poller, a joiner.
    accepted,
    /// The listener created the client's segment and its connection.
    segment_created,
    /// The listener sent SEGMENT.
    segment_sent,
    /// The listener read READY, before it hands the connection over.
    ready_read,
    /// The client connected to the listener.
    connected,
    /// The client received SEGMENT.
    segment_received,
    /// The client mapped the offered segment.
    attached,
    /// The client sent READY, before `connect` returns.
    ready_sent,
    /// A connection's watcher, before its first read (either side; a channel subscriber's too).
    watching,
    /// A broadcast's, a queue's or a service's owner (its poller) sent OFFER to a joiner (after `accepted`).
    offer_sent,
    /// The owner read the joiner's JOIN.
    join_read,
    /// The owner sent WELCOME, before it watches the member's connection.
    welcome_sent,
    /// The owner's poller watches a new member's connection, and the member is joined.
    member_watching,
    /// The joiner received OFFER (after `connected`).
    offer_received,
    /// The joiner sent JOIN.
    join_sent,
    /// The joiner read WELCOME.
    welcomed,
    /// The joiner took the start the publisher stored in its slot, before its watcher starts (a work queue's
    /// joiner: its producer or consumer started).
    start_taken,
    /// A work queue's consumer claimed a message: after its claim's compare-and-swap, before it moves `head`.
    queue_claimed,
    /// A work queue's consumer copied a message out in `fipc_recv`, before its done: the message is the
    /// application's, not yet acknowledged.
    queue_copied,
    /// A work queue's producer published a message: after its `tail`, before its ready bit.
    queue_published,
    /// A service's client published a request: after its `tail`, before its ready bit.
    service_published,
    /// A service's server claimed a request: after its claim's compare-and-swap, before it moves `head`.
    service_claimed,
    /// A service's server wrote its reply (its bytes, its length and the outcome), before the request's done: the
    /// owner abandons the request.
    service_replying,
    /// A service's server stored its reply's done, before it wakes the client: the reply stands.
    service_replied,
};

/// When set, the process ends at once (exit code 99), as a crash would, when it reaches this step: nothing is
/// cleaned up, and the kernel closes what the process held (the crash tests, and the chaos harness's crash-at-step
/// scenarios). Set it before the process listens or connects, in Zig through this variable, or from any language
/// through the environment (`armFromEnv`).
pub var crash_at: ?Step = null;

pub fn crashIfAt(step: Step) void {
    armFromEnv();
    if (crash_at != step) return;
    platform.crash(99);
}

var env_checked = std.atomic.Value(bool).init(false);

/// Arms `crash_at` from `FASTIPC_TEST_CRASH_AT` (a harness that drives the library through the C API can't reach
/// `crash_at` directly). Read once per process, at the first step; an unset variable or an unknown step name
/// leaves `crash_at` unchanged. test_hooks only, so the shipped library never reads the environment for this.
pub fn armFromEnv() void {
    if (env_checked.swap(true, .monotonic)) return;
    const value = std.c.getenv("FASTIPC_TEST_CRASH_AT") orelse return;
    if (std.meta.stringToEnum(Step, std.mem.span(value))) |step| crash_at = step;
}

// === Holds ===

/// When set, a subscriber's watcher that applied UNSUBSCRIBE waits before its answer until the subscriber's close
/// asked it to stop, and 50 ms more (the close's `platform.endTask` meanwhile), so that the close meets the
/// answer's send, which it must not wait for (tests/zig/broadcast/unsubscribe_test.zig, "a subscriber's close right
/// after UNSUBSCRIBED doesn't wait for its answer to the publisher").
pub var hold_answer = std.atomic.Value(bool).init(false);

pub fn holdAnswer(endpoint: *const Endpoint) void {
    if (!hold_answer.load(.monotonic)) return;
    while (!endpoint.loadRequests().shutdown) platform.sleep(1);
    platform.sleep(50);
}

/// When set, a joiner that read WELCOME counts itself in `welcomed_held`, then waits until it is cleared before it
/// takes its start: the publisher counts the subscriber meanwhile, and may send (tests/zig/broadcast/
/// count_test.zig, "a subscriber the writer's count includes receives every message sent afterwards").
pub var hold_welcomed = std.atomic.Value(bool).init(false);
pub var welcomed_held = std.atomic.Value(u32).init(0);

pub fn holdWelcomed() void {
    if (!hold_welcomed.load(.acquire)) return;
    _ = welcomed_held.fetchAdd(1, .release);
    while (hold_welcomed.load(.acquire)) platform.sleep(1);
}

// === Strays ===

/// Sends a frame that doesn't belong (a second WELCOME, or JOIN) on a control connection: on a subscriber's (`conn`,
/// its `fipc_conn_t`) to its publisher, on the publisher's to each of its subscribers; on a queue member's or a service
/// member's to its owner. Each watcher that reads it ends the connection as for a protocol error, while both processes
/// run on (the tests of a membership that ends that way). Nothing on a stream connection.
pub fn sendStray(conn: *anyopaque) void {
    const endpoint = endpointOf(conn);
    switch (calls.kindOf(endpoint)) {
        .broadcast_publisher => broadcast_server.sendStray(endpoint),
        .broadcast_subscriber => broadcast_client.sendStray(endpoint),
        .queue_producer, .queue_consumer => queue_client.sendStray(endpoint),
        .service_client, .service_server => service_client.sendStray(endpoint),
        .stream => {},
    }
}

/// As `sendStray`, on a queue owner's control connections (`queue`, its `fipc_queue_t`) to each joined member.
pub fn sendQueueStray(queue: *anyopaque) void {
    queue_server.sendStray(@ptrCast(@alignCast(queue)));
}

/// As `sendStray`, on a service owner's control connections (`service`, its `fipc_service_t`) to each joined member.
pub fn sendServiceStray(service: *anyopaque) void {
    service_server.sendStray(@ptrCast(@alignCast(service)));
}

// === A connection's state ===

pub const ChannelLayout = broadcast_segment.Layout;

/// The segment of the broadcast whose publisher is `publisher` (a `fipc_conn_t`), as the publisher maps it: the
/// words a test looks at (a slot's cursor, the armed bits, the publisher's sleep flag).
pub fn channelLayout(publisher: *anyopaque) *const ChannelLayout {
    const endpoint = endpointOf(publisher);
    std.debug.assert(calls.kindOf(endpoint) == .broadcast_publisher);
    return broadcast_server.layoutOf(endpoint);
}

/// The sleeps the receives of `subscriber` (a `fipc_conn_t`) began (broadcast/reader.zig `Reader.sleeps`).
pub fn subscriberSleeps(subscriber: *anyopaque) u32 {
    const endpoint = endpointOf(subscriber);
    std.debug.assert(calls.kindOf(endpoint) == .broadcast_subscriber);
    return broadcast_client.sleepsOf(endpoint);
}

pub const QueueLayout = queue_segment.Layout;

/// The segment of the work queue of `member` (a producer's or consumer's `fipc_conn_t`), as the member maps it: the
/// words a test looks at, or writes as a corrupt or ended peer would.
pub fn queueLayout(member: *anyopaque) *const QueueLayout {
    const endpoint = endpointOf(member);
    std.debug.assert(isQueueMember(endpoint));
    return queue_client.layoutOf(endpoint);
}

/// The sleeps the waits of `member` (a work queue's producer or consumer) began: a consumer's parks, a producer's
/// sleeps for room.
pub fn queueSleeps(member: *anyopaque) u32 {
    const endpoint = endpointOf(member);
    std.debug.assert(isQueueMember(endpoint));
    return queue_client.sleepsOf(endpoint);
}

pub const ServiceLayout = service_segment.Layout;

/// The segment of the service of `member` (a client's or server's `fipc_conn_t`), as the member maps it: the words a
/// test looks at, or writes as a corrupt or ended peer would.
pub fn serviceLayout(member: *anyopaque) *const ServiceLayout {
    const endpoint = endpointOf(member);
    std.debug.assert(isServiceMember(endpoint));
    return service_client.layoutOf(endpoint);
}

/// The sleeps the receives of `member` (a service's client or server) began: a client's waits for a reply, a server's
/// parks.
pub fn serviceSleeps(member: *anyopaque) u32 {
    const endpoint = endpointOf(member);
    std.debug.assert(isServiceMember(endpoint));
    return service_client.sleepsOf(endpoint);
}

/// The sleeps for room the sends of `client` (a service's client) began.
pub fn serviceRoomSleeps(client: *anyopaque) u32 {
    const endpoint = endpointOf(client);
    std.debug.assert(calls.kindOf(endpoint) == .service_client);
    return service_client.roomSleepsOf(endpoint);
}

/// Makes each sleep of the receives of `client` (a service's client) for a reply last up to `ms` (a slice by default),
/// so that a test sees a wake end the sleep, not its slice. Before the receive, on its thread or one that started it.
pub fn setServiceReplySleepMs(client: *anyopaque, ms: u32) void {
    const endpoint = endpointOf(client);
    std.debug.assert(calls.kindOf(endpoint) == .service_client);
    service_client.setReplySleepMs(endpoint, ms);
}

/// The number of threads of this process (the test of a channel's one poller thread).
pub fn threadCount() usize {
    return platform.threadCount();
}

fn endpointOf(conn: *anyopaque) *Endpoint {
    return @ptrCast(@alignCast(conn));
}

fn isQueueMember(endpoint: *const Endpoint) bool {
    return switch (calls.kindOf(endpoint)) {
        .queue_producer, .queue_consumer => true,
        .stream, .broadcast_publisher, .broadcast_subscriber, .service_client, .service_server => false,
    };
}

fn isServiceMember(endpoint: *const Endpoint) bool {
    return switch (calls.kindOf(endpoint)) {
        .service_client, .service_server => true,
        .stream, .broadcast_publisher, .broadcast_subscriber, .queue_producer, .queue_consumer => false,
    };
}

// === Stack traces ===

/// std's Windows `Io` maps a read of a pipe that its server disconnected (STATUS_PIPE_DISCONNECTED, what a client
/// reads when a listener drops the connection) to an unexpected status, whose stack trace, in a Debug build that
/// keeps tracing on, loads the debug info and keeps its files open. A test that counts handles calls this first, so
/// that the process's first such trace isn't in its count. Nothing elsewhere.
pub fn primeStackTraces(io: Io) void {
    if (builtin.os.tag != .windows) return; // only std's Windows `Io` traces that status
    var security = platform.Security.init() catch return;
    defer security.deinit();
    var name_buf: [64]u8 = undefined;
    const name = std.mem.print(&name_buf, "t-prime-{d}", .{platform.pid()}) catch return;
    const address = platform.addressOf(name) catch return;
    const listening = (platform.claim(&address, &security) catch return) orelse return;
    defer platform.close(listening);
    const client = switch (platform.connect(&address, &security) catch return) {
        .connected => |client| client,
        else => return,
    };
    defer platform.close(client);
    switch (platform.acceptWithin(listening, null, 5000, null)) {
        .done => |outcome| switch (outcome) {
            .connection => |accepted| _ = platform.dropClient(accepted, false),
            else => return,
        },
        else => return,
    }
    std.debug.print("test_hooks.primeStackTraces: a PIPE_DISCONNECTED trace follows, on purpose\n", .{});
    var byte: [1]u8 = undefined;
    _ = platform.read(io, client, &byte) catch {};
}
