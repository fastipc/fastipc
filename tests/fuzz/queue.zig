//! The fuzz harness's work-queue cases (main.zig runs them): a corrupt queue peer against this library's owner,
//! producers and consumers, in one process.
//!
//! Control-plane cases, none of which may crash:
//!   - a forged owner (a raw listener) offers a library joiner a malformed OFFER (another kind or version, maximums or
//!     a capacity out of range, a wrong segment size, a frame cut short), or a valid OFFER and then a bad answer to
//!     JOIN (the end of the stream, WELCOME with another role or a place out of range, a producer place whose ring has
//!     corrupt indices): the join must fail (INVALID, or TIMEOUT while it retries);
//!   - a forged owner sends a frame after WELCOME: the join succeeds and the member's calls return DISCONNECTED;
//!   - a forged joiner sends a library owner a malformed JOIN (a broadcast role, an unknown role or kind, another
//!     version or type, a frame cut short, the end of the stream), or joins validly and then sends a frame (the owner
//!     retires its place) or leaves at once: each admission must return, and the queue must still serve a library
//!     consumer afterwards.
//!
//! Ring-content cases: a corrupt member joins a library owner's queue by hand, beside a library producer and consumer,
//! and writes the segment - random bytes everywhere, random values in every shared word, forged cell states and payload
//! fields, or all of that from a thread while the library works - then leaves, so that the owner recovers what its
//! consumer place names. Every call must return within its timeout, the owner's recovery and the closes included;
//! each trial runs in a child process of its own (`fuzz --queue-child <seed>` repeats one).

const std = @import("std");
const fuzz = @import("main.zig");
const fastipc = @import("fastipc");
const internal = fastipc.internal;
const wire_protocol = internal.wire_protocol;
const wire = internal.wire;
const queue_frames = internal.queue_frames;
const queue_segment = internal.queue_segment;
const Session = internal.shared_memory.Session;
const os = internal.platform;

const capacity = 4096; // each producer's ring: the smallest queue segments
const max_producers = 3;
const max_consumers = 3;
const join_timeout: std.Io.Timeout = .{ .duration = .{ .raw = .fromMilliseconds(500), .clock = .awake } };

fn ms(n: i64) std.Io.Timeout {
    return .{ .duration = .{ .raw = .fromMilliseconds(n), .clock = .awake } };
}

// === A forged owner against a library joiner ===

/// What the forged owner does wrong. Each round's joiner is a producer or a consumer in turn (`corrupt_ring`: a
/// producer).
pub const OwnerCorruption = enum {
    /// OFFER of a broadcast, and of a kind nobody knows: INVALID at once.
    kind_broadcast,
    kind_unknown,
    /// Another channel protocol version: INVALID at once.
    other_version,
    /// max_producers 0, max_consumers 65, a capacity that isn't a power of two, a segment size off the layout:
    /// malformed, retried until the join times out.
    no_producers,
    too_many_consumers,
    bad_capacity,
    wrong_size,
    /// A frame cut short, then the end of the stream.
    eof_midframe,
    /// A valid OFFER, then the end of the stream where WELCOME belongs.
    eof_before_welcome,
    /// WELCOME with the other queue role, or a place past the role's maximum.
    welcome_other_role,
    welcome_slot_out_of_range,
    /// A producer place whose ring's tail isn't its head: INVALID.
    corrupt_ring,
    /// A valid WELCOME, then another frame: the join succeeds, then the member's calls return DISCONNECTED.
    frame_after_welcome,

    fn needsSegment(corruption: OwnerCorruption) bool {
        return @backingInt(corruption) >= @backingInt(OwnerCorruption.eof_before_welcome);
    }
};

/// The forged owner listens on the name; a library joiner joins it. True when the join failed (or, for
/// `frame_after_welcome`, succeeded and its calls then returned DISCONNECTED) without a crash or hang.
pub fn ownerCase(corruption: OwnerCorruption, counter: u64) bool {
    var name_buf: [64]u8 = undefined;
    const name = fuzz.uniqueName(&name_buf, counter);
    var peer = fuzz.RawPeer.claim(name) orelse return false;
    defer peer.deinit();
    const role: fastipc.QueueRole = if (corruption == .corrupt_ring or counter % 2 == 0) .producer else .consumer;

    const Owner = struct {
        peer: *fuzz.RawPeer,
        corruption: OwnerCorruption,
        role: fastipc.QueueRole,
        done: std.atomic.Value(bool) = .init(false),

        fn run(owner: *@This()) void {
            if (!owner.peer.accept()) return;
            forgeOwner(owner.peer, owner.corruption, owner.role);
            // The connection stays open until the case is over: its end would be the queue's end to a member
            var waited: u32 = 0;
            while (!owner.done.load(.acquire) and waited < 5000) : (waited += 5) fuzz.sleep(5);
        }
    };
    var owner: Owner = .{ .peer = &peer, .corruption = corruption, .role = role };
    const thread = std.Thread.spawn(.{}, Owner.run, .{&owner}) catch return false;
    defer thread.join();
    defer owner.done.store(true, .release);

    // fipc_queue_join's documented refusals of a malformed owner: Invalid (another kind or version, a corrupt ring) or
    // Timeout (retried until the deadline); anything else is a failure
    const conn = fastipc.queueJoin(fuzz.gpa, name, role, join_timeout) catch |err| {
        const refused = corruption != .frame_after_welcome and (err == error.Invalid or err == error.Timeout);
        if (!refused) std.debug.print("fuzz:   queue owner {s}: the join returned {s}\n", .{ @tagName(corruption), @errorName(err) });
        return refused;
    };
    defer conn.close();
    if (corruption != .frame_after_welcome) {
        std.debug.print("fuzz:   queue owner {s}: the join succeeded\n", .{@tagName(corruption)});
        return false;
    }
    // The member's own failure: its calls return Disconnected, a producer's once its watcher read the frame
    var buf: [64]u8 = undefined;
    for (0..200) |_| {
        const result: fastipc.DataError!void = switch (role) {
            .producer => conn.send("fuzz", ms(10)),
            else => if (conn.recv(&buf, ms(10))) |_| {} else |err| err,
        };
        result catch |err| switch (err) {
            error.Disconnected => return true,
            error.Timeout => {},
            else => break,
        };
        fuzz.sleep(5);
    }
    std.debug.print("fuzz:   queue owner frame_after_welcome: the member's calls never returned Disconnected\n", .{});
    return false;
}

/// Sends the forged OFFER for `corruption` on `peer`'s connection, and for the cases past a valid OFFER reads JOIN and
/// answers it wrongly. A segment's handle travels with OFFER where it is passed: a throwaway one for the frame-level
/// cases, which the joiner refuses before it maps anything.
fn forgeOwner(peer: *fuzz.RawPeer, corruption: OwnerCorruption, role: fastipc.QueueRole) void {
    const offer: queue_frames.Offer = .{
        .capacity = capacity,
        .segment_size = queue_segment.describe(capacity, 1, 1).segment_size,
        .session_id = .{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 },
        .max_producers = 1,
        .max_consumers = 1,
    };
    if (!corruption.needsSegment()) {
        var frame = queue_frames.encodeOffer(4242, offer);
        const fields: *align(1) wire_protocol.ControlQueueOffer = @ptrCast(&frame);
        switch (corruption) {
            .kind_broadcast => fields.header.kind = .broadcast,
            .kind_unknown => fields.header.kind = @fromBackingInt(4),
            .other_version => fields.header.version = wire_protocol.channel_protocol_version + 1,
            .no_producers => fields.max_producers = 0,
            .too_many_consumers => fields.max_consumers = 65,
            .bad_capacity => fields.capacity = 3000,
            .wrong_size => fields.segment_size += 4096,
            .eof_midframe => {},
            else => unreachable,
        }
        const handle: ?os.Handle = if (os.passes_segment_handle) os.createSegmentHandle() catch return else null;
        defer if (handle) |h| os.close(h);
        peer.send(if (corruption == .eof_midframe) frame[0..10] else &frame, handle);
        return;
    }
    // A real queue segment of one producer and one consumer
    const handle: ?os.Handle = if (os.passes_segment_handle) os.createSegmentHandle() catch return else null;
    defer if (handle) |h| os.close(h);
    var session = queue_segment.create(fuzz.io, handle, &peer.security, capacity, 1, 1) catch return;
    defer session.unmap();
    const layout: queue_segment.Layout = .of(&session, 1, 1);
    if (corruption == .corrupt_ring) {
        layout.ring(0).tail.store(5, .seq_cst);
        layout.ring(0).head.store(3, .seq_cst);
    }
    var valid = offer;
    valid.session_id = session.id;
    peer.send(&queue_frames.encodeOffer(4242, valid), handle);
    var join: wire.Frame = undefined;
    if (!fuzz.readFull(peer.conn.?, &join)) return;
    const welcome = switch (corruption) {
        .eof_before_welcome => return,
        .welcome_other_role => queue_frames.encodeWelcome(4242, if (role == .producer) .consumer else .producer, 0, 1),
        .welcome_slot_out_of_range => queue_frames.encodeWelcome(4242, role, 1, 1),
        .corrupt_ring, .frame_after_welcome => queue_frames.encodeWelcome(4242, role, 0, 1),
        else => unreachable,
    };
    peer.send(&welcome, null);
    if (corruption == .frame_after_welcome) {
        fuzz.sleep(20);
        peer.send(&@as(wire.Frame, @splat(0xAB)), null);
    }
    // A named segment goes away with its last handle: keep it until the joiner has opened it
    if (!os.passes_segment_handle) fuzz.sleep(300);
}

// === A forged joiner against a library owner ===

/// What the forged joiner does wrong.
pub const JoinerCorruption = enum {
    /// JOIN with a broadcast's roles, or a role nobody knows: dropped.
    role_subscriber,
    role_gated,
    role_unknown,
    /// JOIN of a broadcast, of another version, or another frame type: dropped.
    kind_broadcast,
    other_version,
    bad_type,
    /// The end of the stream where JOIN belongs, and a JOIN cut short.
    eof_before_join,
    eof_midframe,
    /// A valid consumer that sends a frame after WELCOME: the owner retires its place.
    frame_after_welcome,
    /// A valid consumer that ends at once: the owner frees its place.
    end_after_welcome,
};

/// A library owner's process admits the forged joiner, then a library consumer, which must receive a message of the
/// owner's producer. True when the queue still served the consumer.
pub fn joinerCase(corruption: JoinerCorruption, counter: u64) bool {
    var name_buf: [64]u8 = undefined;
    const name = fuzz.uniqueName(&name_buf, counter);
    const queue = fastipc.queueCreate(fuzz.gpa, name, capacity, max_producers, max_consumers) catch return false;
    defer queue.close();
    const producer = joinNow(name, .producer) orelse return false;
    defer producer.close();

    const Joiner = struct {
        name: []const u8,
        corruption: JoinerCorruption,
        done: std.atomic.Value(bool) = .init(false),

        fn run(joiner: *@This()) void {
            var member = RawMember.connect(joiner.name) orelse return;
            defer member.deinit();
            forgeJoiner(&member, joiner.corruption);
            // The cases that end the stream return at once; the others keep their connection until the case is over
            switch (joiner.corruption) {
                .eof_before_join, .eof_midframe, .end_after_welcome => return,
                else => {},
            }
            var waited: u32 = 0;
            while (!joiner.done.load(.acquire) and waited < 5000) : (waited += 5) fuzz.sleep(5);
        }
    };
    var joiner: Joiner = .{ .name = name, .corruption = corruption };
    const thread = std.Thread.spawn(.{}, Joiner.run, .{&joiner}) catch return false;
    defer thread.join();
    defer joiner.done.store(true, .release);
    // Whatever the forged joiner does, the owner's poller admits the next joiner
    fuzz.sleep(50);
    const consumer = joinNow(name, .consumer) orelse {
        std.debug.print("fuzz:   queue joiner {s}: a library consumer couldn't join afterwards\n", .{@tagName(corruption)});
        return false;
    };
    defer consumer.close();
    producer.send("fuzz", ms(1000)) catch return false;
    var buf: [64]u8 = undefined;
    const len = consumer.recv(&buf, ms(2000)) catch |err| {
        std.debug.print("fuzz:   queue joiner {s}: the consumer's receive returned {s}\n", .{ @tagName(corruption), @errorName(err) });
        return false;
    };
    return std.mem.eql(u8, buf[0..len], "fuzz");
}

fn forgeJoiner(member: *RawMember, corruption: JoinerCorruption) void {
    var frame = queue_frames.encodeJoin(4243, .consumer);
    const fields: *align(1) wire_protocol.ControlQueueJoin = @ptrCast(&frame);
    switch (corruption) {
        .role_subscriber => fields.role = @backingInt(wire_protocol.SubscriberRole.lossy),
        .role_gated => fields.role = @backingInt(wire_protocol.SubscriberRole.gated),
        .role_unknown => fields.role = 5,
        .kind_broadcast => fields.header.kind = .broadcast,
        .other_version => fields.header.version = wire_protocol.channel_protocol_version + 1,
        .bad_type => fields.header.frame_type = @backingInt(wire_protocol.ChannelFrameType.welcome),
        .eof_before_join => return,
        .eof_midframe => return member.send(frame[0..10]),
        .frame_after_welcome, .end_after_welcome => {
            if (!member.join(.consumer)) return;
            if (corruption == .frame_after_welcome) member.send(&@as(wire.Frame, @splat(0xAB)));
            return;
        },
    }
    member.send(&frame);
}

/// Joins the queue on `name` as `role`, within 3 s: a joiner the owner dropped (no free place yet) comes again.
fn joinNow(name: []const u8, role: fastipc.QueueRole) ?fastipc.Conn {
    return fastipc.queueJoin(fuzz.gpa, name, role, ms(3000)) catch null;
}

/// A queue member played by hand: its control connection, and once it joined, its mapping of the segment.
const RawMember = struct {
    conn: os.Handle,
    offer: queue_frames.Offer,
    received: ?os.Handle,
    session: ?Session = null,
    layout: queue_segment.Layout = undefined,

    /// Connects to the owner on `name` and reads its OFFER (and the segment's handle, where it travels with it).
    fn connect(name: []const u8) ?RawMember {
        var security = os.Security.init() catch return null;
        defer security.deinit();
        const address = os.addressOf(name) catch return null;
        const conn = for (0..400) |_| {
            switch (os.connect(&address, &security) catch return null) {
                .connected => |h| break h,
                .busy, .no_listener => fuzz.sleep(5),
                .foreign => return null,
            }
        } else return null;
        var frame: wire.Frame = undefined;
        var received: ?os.Handle = null;
        if (os.passes_segment_handle) {
            if (os.readableWithin(conn, 5000) != .done) {
                os.close(conn);
                return null;
            }
            received = os.recvSegment(conn, &frame) catch {
                os.close(conn);
                return null;
            };
        } else if (!fuzz.readFull(conn, &frame)) {
            os.close(conn);
            return null;
        }
        if (wire.classifyOffer(&frame, .queue) != .offer) {
            if (received) |h| os.close(h);
            os.close(conn);
            return null;
        }
        const offer = queue_frames.decodeOffer(&frame);
        return .{ .conn = conn, .offer = offer, .received = received };
    }

    /// Maps the offered segment, sends a valid JOIN as `role` and reads WELCOME. False on any failure.
    fn join(member: *RawMember, role: fastipc.QueueRole) bool {
        var session = queue_segment.attach(member.received, &member.offer) catch return false;
        member.layout = .of(&session, member.offer.max_producers, member.offer.max_consumers);
        member.session = session;
        member.send(&queue_frames.encodeJoin(4243, role));
        var welcome: wire.Frame = undefined;
        if (!fuzz.readFull(member.conn, &welcome)) return false;
        return queue_frames.checkWelcome(&welcome, role, member.offer.max_consumers) != null;
    }

    fn send(member: *RawMember, bytes: []const u8) void {
        _ = os.sendFrame(member.conn, bytes, null);
    }

    /// Leaves: the owner reads the end of its connection. The mapping goes after it.
    fn deinit(member: *RawMember) void {
        os.close(member.conn);
        if (member.received) |h| os.close(h);
        if (member.session) |*session| session.unmap();
    }
};

// === Ring content: a corrupt member ===

/// How a ring-content trial's corrupt member writes the segment; the trial's seed picks the mode.
pub const RingMode = enum {
    /// Random bytes over the whole segment: the description, every shared word, the cells and the data.
    bytes,
    /// Random values in every shared word: `ended`, `ready`, `idle`, each consumer's `sleeps` and `claim`, each ring's
    /// `tail`, `head` and `room_sleeps`.
    words,
    /// Forged cell states (any phase, claimer and index, near the rings' indices or not) and payload fields.
    cells,
    /// The three above, again and again from a thread, while the library works.
    racing,
};

/// Runs one queue ring-content trial in a child process (`fuzz --queue-child <seed>`) and reports whether it returned
/// cleanly: exit code 0. An abort, a panic or a crash ends the child otherwise, and so does its watchdog if a call hangs.
pub fn ringCase(seed: u64) bool {
    var seed_buf: [24]u8 = undefined;
    const seed_str = std.mem.print(&seed_buf, "{d}", .{seed}) catch return false;
    var child = std.process.spawn(fuzz.io, .{
        .argv = &.{ fuzz.self_exe, "--queue-child", seed_str },
        .stdout = .ignore,
        .stderr = .ignore,
    }) catch return false;
    const term = child.wait(fuzz.io) catch return false;
    if (term == .exited and term.exited == 0) return true;
    std.debug.print("fuzz:   queue seed {d}: {any} (repeat it: fuzz --queue-child {d})\n", .{ seed, term, seed });
    return false;
}

/// One queue ring-content trial, in-process (the child): a library owner and the producer it joined, a joined library
/// producer and consumer, and the corrupt member, which writes the segment (`RingMode`) and then leaves. The library's
/// members make each kind of data call a few times with short timeouts, before and after the corrupt member leaves
/// (the owner then recovers what its consumer place names); then everything closes. 0 once every call returned.
pub fn ringTrial(seed: u64) u8 {
    _ = std.Thread.spawn(.{}, fuzz.watchdog, .{}) catch return 4;
    var name_buf: [64]u8 = undefined;
    const name = fuzz.uniqueName(&name_buf, seed);
    const queue = fastipc.queueCreate(fuzz.gpa, name, capacity, max_producers, max_consumers) catch return 0;
    defer queue.close();
    const own = joinNow(name, .producer) orelse return 0;
    defer own.close();
    const producer = joinNow(name, .producer) orelse return 0;
    defer producer.close();
    const consumer = joinNow(name, .consumer) orelse return 0;
    defer consumer.close();
    // Messages in the rings before the corrupt member writes them
    for (0..4) |_| {
        own.send("before", ms(50)) catch {};
        producer.send("before", ms(50)) catch {};
    }

    const Corrupt = struct {
        name: []const u8,
        member: ?RawMember = null,

        fn run(corrupt: *@This()) void {
            var member = RawMember.connect(corrupt.name) orelse return;
            if (!member.join(.consumer)) {
                member.deinit();
                return;
            }
            corrupt.member = member;
        }
    };
    var corrupt: Corrupt = .{ .name = name };
    {
        const thread = std.Thread.spawn(.{}, Corrupt.run, .{&corrupt}) catch return 4;
        thread.join();
    }
    var member = corrupt.member orelse return 0;
    var writer: Scribbler = .{ .layout = member.layout, .rng = .init(seed) };
    var stop = std.atomic.Value(bool).init(false);
    var racer: ?std.Thread = null;
    const modes = std.enums.values(RingMode);
    switch (modes[@intCast(seed % modes.len)]) {
        .bytes => writer.scribbleBytes(),
        .words => writer.scribbleWords(),
        .cells => writer.forgeCells(),
        .racing => racer = std.Thread.spawn(.{}, Scribbler.race, .{ &writer, &stop }) catch return 4,
    }

    // A call that started waiting is ended by the members' cancels, if the calls run that long
    var calls_done = std.atomic.Value(bool).init(false);
    const canceller = std.Thread.spawn(.{}, cancelAfter, .{ [_]fastipc.Conn{ own, producer, consumer }, &calls_done }) catch return 4;
    defer canceller.join();
    defer calls_done.store(true, .release);

    dataCalls(own, producer, consumer);
    if (racer) |thread| {
        stop.store(true, .release);
        thread.join();
    }
    // The corrupt member leaves: the owner's poller reads its end and recovers its place's claim, whatever it says
    member.deinit();
    fuzz.sleep(20);
    dataCalls(own, producer, consumer);
    return 0;
}

/// Each kind of data call a few times, with short timeouts: any result.
fn dataCalls(own: fastipc.Conn, producer: fastipc.Conn, consumer: fastipc.Conn) void {
    const quick = ms(30);
    var buf: [capacity]u8 = undefined;
    for (0..6) |_| {
        own.send("fuzz", quick) catch {};
        producer.send("fuzz", quick) catch {};
        if (producer.acquire(16, quick)) |buffer| {
            @memset(buffer, 0x5A);
            producer.commit(buffer.len) catch {};
        } else |_| {}
        _ = consumer.recv(&buf, quick) catch {};
        if (consumer.recvAcquire(quick)) |_| consumer.release() else |_| {}
    }
}

/// Cancels the members once the trial's calls have run for `cancel_after_ms`, unless they are done by then.
fn cancelAfter(conns: [3]fastipc.Conn, done: *const std.atomic.Value(bool)) void {
    var waited: u32 = 0;
    while (!done.load(.acquire)) : (waited += 10) {
        if (waited >= cancel_after_ms) {
            for (conns) |conn| conn.cancel();
            return;
        }
        fuzz.sleep(10);
    }
}

/// Longer than the trial's calls take when none waits past its timeout (two rounds of six times five 30 ms calls).
const cancel_after_ms = 4000;

/// The corrupt member's writes into the segment, through its own mapping.
const Scribbler = struct {
    layout: queue_segment.Layout,
    rng: std.Random.DefaultPrng,

    fn scribbleBytes(scribbler: *Scribbler) void {
        const size = queue_segment.describe(capacity, max_producers, max_consumers).segment_size;
        scribbler.rng.random().bytes(scribbler.layout.base[0..size]);
    }

    /// A value a reader must check: random, near a ring's indices, or at the index bound.
    fn value(scribbler: *Scribbler, near: u64) u64 {
        const random = scribbler.rng.random();
        return switch (random.uintLessThan(u8, 4)) {
            0 => random.int(u64),
            1 => near +% random.uintAtMost(u64, 2 * capacity) -% capacity,
            2 => wire_protocol.queue_max_index -% random.uintAtMost(u64, 2),
            else => random.uintAtMost(u64, 64),
        };
    }

    fn scribbleWords(scribbler: *Scribbler) void {
        const random = scribbler.rng.random();
        const layout = &scribbler.layout;
        layout.header.ended.store(random.uintLessThan(u32, 3) / 2, .seq_cst); // mostly 0: the queue goes on
        layout.header.ready.store(random.int(u64), .seq_cst);
        layout.header.idle.store(random.int(u64), .seq_cst);
        for (0..layout.max_takers) |c| {
            const slot = layout.takerSlot(@intCast(c));
            slot.sleeps.store(random.int(u32), .seq_cst);
            slot.claim.store(scribbler.value(0), .seq_cst);
        }
        for (0..layout.max_senders) |p| {
            const ring = layout.ring(@intCast(p));
            const tail = ring.tail.load(.seq_cst);
            ring.tail.store(scribbler.value(tail), .seq_cst);
            ring.head.store(scribbler.value(tail), .seq_cst);
            ring.room_sleeps.store(random.int(u32), .seq_cst);
        }
    }

    fn forgeCells(scribbler: *Scribbler) void {
        const random = scribbler.rng.random();
        const layout = &scribbler.layout;
        for (0..layout.max_senders) |p| {
            const ring = layout.ring(@intCast(p));
            const head = ring.head.load(.seq_cst);
            for (0..layout.cellCount()) |k| {
                const cell = layout.cell(@intCast(p), head +% k);
                const index = scribbler.value(head +% k);
                const phase: wire_protocol.QueuePhase = @fromBackingInt(@as(u2, @intCast(random.uintAtMost(u64, 3))));
                const claimer: u6 = @intCast(random.uintLessThan(u64, 64));
                const state: wire_protocol.QueueCellState = .{ .phase = phase, .claimer = claimer, .tag = @truncate(index +% 1) };
                cell.state.store(@bitCast(state), .release);
                cell.payload_position.store(scribbler.value(0), .release);
                cell.len.store(switch (random.uintLessThan(u8, 3)) {
                    0 => random.int(u32),
                    1 => random.uintAtMost(u32, capacity / 4 + 1),
                    else => 0,
                }, .release);
            }
            if (random.boolean()) ring.tail.store(head +% random.uintAtMost(u64, layout.cellCount() + 1), .seq_cst);
        }
    }

    fn race(scribbler: *Scribbler, stop: *std.atomic.Value(bool)) void {
        while (!stop.load(.acquire)) {
            switch (scribbler.rng.random().uintLessThan(u8, 3)) {
                0 => scribbler.scribbleWords(),
                1 => scribbler.forgeCells(),
                else => scribbler.scribbleBytes(),
            }
        }
    }
};
