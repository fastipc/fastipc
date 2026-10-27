//! The fuzz harness's service cases (main.zig runs them): a corrupt service peer against this library's owner, clients
//! and servers, in one process.
//!
//! Control-plane cases, none of which may crash:
//!   - a forged owner (a raw listener) offers a library joiner a malformed OFFER (another kind or version, a
//!     max_message of 0 or over a quarter of the capacity, client or server maximums out of range, a capacity that
//!     isn't a power of two, a wrong segment size, a frame cut short), or a valid OFFER and then a bad answer to JOIN
//!     (the end of the stream, WELCOME with the other service role or a queue's, a place out of range, a queue's
//!     WELCOME, a client place whose ring has corrupt indices): the join must fail (INVALID, or TIMEOUT while it
//!     retries);
//!   - a forged owner sends a frame after WELCOME: the join succeeds and the member's calls return DISCONNECTED;
//!   - a forged joiner sends a library owner a malformed JOIN (a queue's or a broadcast's role, an unknown role, a
//!     queue's kind, another version or type, a frame cut short, the end of the stream), or joins validly as a client
//!     or a server and then sends a stray frame (the owner retires its place) or leaves at once: each admission must
//!     return, and the service must still carry a library client's request to a library server and its reply back;
//!   - a forged server claims a library client's request by hand and then leaves, or sends a stray frame first (then
//!     its request waits for its end): the client receives ABANDONED for that request, exactly once, and the service
//!     goes on;
//!   - a forged client publishes requests by hand and leaves: the owner drops them, and its place, the only client
//!     place, goes to a library client, whose request is the first a library server receives.
//!
//! Ring-content cases: a corrupt client or server joins a library owner's service by hand, beside library clients and
//! servers, and writes the segment - random bytes everywhere, random values in every shared word (intents naming rings
//! that don't exist, `tail` and `head` garbage), forged cells (any state, outcome, request and reply length, a request
//! position below its reply's room or past the ring), or all of that from a thread while the library works - then
//! leaves, so that the owner abandons, drops or retires what its place names. Every call must return within its
//! timeout, the owner's recovery and the closes included; each trial runs in a child process of its own
//! (`fuzz --service-child <seed>` repeats one).

const std = @import("std");
const fuzz = @import("main.zig");
const fastipc = @import("fastipc");
const internal = fastipc.internal;
const wire_protocol = internal.wire_protocol;
const wire = internal.wire;
const service_frames = internal.service_frames;
const service_segment = internal.service_segment;
const Session = internal.shared_memory.Session;
const os = internal.platform;

const capacity = 4096; // each client's ring: the smallest service segments
const max_message = 256;
const max_clients = 3;
const max_servers = 3;

fn ms(n: i64) std.Io.Timeout {
    return .{ .duration = .{ .raw = .fromMilliseconds(n), .clock = .awake } };
}

const join_timeout = ms(500);

// === A forged owner against a library joiner ===

/// What the forged owner does wrong. Each round's joiner is a client or a server in turn (`corrupt_ring`: a client).
pub const OwnerCorruption = enum {
    /// OFFER of a queue, a broadcast, and of a kind nobody knows: INVALID at once.
    kind_queue,
    kind_broadcast,
    kind_unknown,
    /// Another channel protocol version: INVALID at once.
    other_version,
    /// max_message 0 or over a quarter of the capacity, no clients or servers, 65 of either, a capacity that isn't a
    /// power of two, a segment size off the layout: malformed, retried until the join times out.
    max_message_zero,
    max_message_over,
    no_clients,
    too_many_clients,
    no_servers,
    too_many_servers,
    bad_capacity,
    wrong_size,
    /// A frame cut short, then the end of the stream.
    eof_midframe,
    /// A valid OFFER, then the end of the stream where WELCOME belongs.
    eof_before_welcome,
    /// WELCOME with the other service role, with a queue's role, with a place past the role's maximum, or of a
    /// queue's kind.
    welcome_other_role,
    welcome_queue_role,
    welcome_slot_out_of_range,
    welcome_queue_kind,
    /// A client place whose ring's tail isn't its head: INVALID.
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
    const role: fastipc.ServiceRole = if (corruption == .corrupt_ring or counter % 2 == 0) .client else .server;

    const Owner = struct {
        peer: *fuzz.RawPeer,
        corruption: OwnerCorruption,
        role: fastipc.ServiceRole,
        done: std.atomic.Value(bool) = .init(false),

        fn run(owner: *@This()) void {
            if (!owner.peer.accept()) return;
            forgeOwner(owner.peer, owner.corruption, owner.role);
            // The connection stays open until the case is over: its end would be the service's end to a member
            var waited: u32 = 0;
            while (!owner.done.load(.acquire) and waited < 5000) : (waited += 5) fuzz.sleep(5);
        }
    };
    var owner: Owner = .{ .peer = &peer, .corruption = corruption, .role = role };
    const thread = std.Thread.spawn(.{}, Owner.run, .{&owner}) catch return false;
    defer thread.join();
    defer owner.done.store(true, .release);

    // fipc_service_join's documented refusals of a malformed owner: Invalid (another kind or version, a corrupt ring)
    // or Timeout (retried until the deadline); anything else is a failure
    const conn = fastipc.serviceJoin(fuzz.gpa, name, role, join_timeout) catch |err| {
        const refused = corruption != .frame_after_welcome and (err == error.Invalid or err == error.Timeout);
        if (!refused) std.debug.print("fuzz:   service owner {s}: the join returned {s}\n", .{ @tagName(corruption), @errorName(err) });
        return refused;
    };
    defer conn.close();
    if (corruption != .frame_after_welcome) {
        std.debug.print("fuzz:   service owner {s}: the join succeeded\n", .{@tagName(corruption)});
        return false;
    }
    // The member's own failure: its calls return Disconnected once its watcher read the frame
    var buf: [max_message]u8 = undefined;
    for (0..200) |_| {
        const result: fastipc.DataError!void = switch (role) {
            .client => conn.send("fuzz", ms(10)),
            .server => if (conn.recv(&buf, ms(10))) |_| {} else |err| err,
        };
        result catch |err| switch (err) {
            error.Disconnected => return true,
            error.Timeout => {},
            else => break,
        };
        fuzz.sleep(5);
    }
    std.debug.print("fuzz:   service owner frame_after_welcome: the member's calls never returned Disconnected\n", .{});
    return false;
}

fn validOffer() service_frames.Offer {
    return .{
        .capacity = capacity,
        .segment_size = service_segment.describe(capacity, 1, 1, max_message).segment_size,
        .session_id = .{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 },
        .max_clients = 1,
        .max_servers = 1,
        .max_message = max_message,
    };
}

/// Sends the forged OFFER for `corruption` on `peer`'s connection, and for the cases past a valid OFFER reads JOIN and
/// answers it wrongly. A segment's handle travels with OFFER where it is passed: a throwaway one for the frame-level
/// cases, which the joiner refuses before it maps anything.
fn forgeOwner(peer: *fuzz.RawPeer, corruption: OwnerCorruption, role: fastipc.ServiceRole) void {
    const offer = validOffer();
    if (!corruption.needsSegment()) {
        var frame = service_frames.encodeOffer(4242, offer);
        const fields: *align(1) wire_protocol.ControlServiceOffer = @ptrCast(&frame);
        switch (corruption) {
            .kind_queue => fields.header.kind = .queue,
            .kind_broadcast => fields.header.kind = .broadcast,
            .kind_unknown => fields.header.kind = @fromBackingInt(4),
            .other_version => fields.header.version = wire_protocol.channel_protocol_version + 1,
            .max_message_zero => fields.max_message = 0,
            .max_message_over => fields.max_message = capacity / 4 + 1,
            .no_clients => fields.max_clients = 0,
            .too_many_clients => {
                fields.max_clients = 65;
                fields.segment_size = service_segment.describe(capacity, 65, 1, max_message).segment_size;
            },
            .no_servers => fields.max_servers = 0,
            .too_many_servers => fields.max_servers = 65,
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
    // A real service segment of one client and one server
    const handle: ?os.Handle = if (os.passes_segment_handle) os.createSegmentHandle() catch return else null;
    defer if (handle) |h| os.close(h);
    var session = service_segment.create(fuzz.io, handle, &peer.security, capacity, 1, 1, max_message) catch return;
    defer session.unmap();
    const layout = service_segment.layoutOf(&session, 1, 1, max_message);
    if (corruption == .corrupt_ring) {
        layout.ring(0).tail.store(5, .seq_cst);
        layout.ring(0).head.store(3, .seq_cst);
    }
    var valid = offer;
    valid.session_id = session.id;
    peer.send(&service_frames.encodeOffer(4242, valid), handle);
    var join: wire.Frame = undefined;
    if (!fuzz.readFull(peer.conn.?, &join)) return;
    const other: fastipc.ServiceRole = if (role == .client) .server else .client;
    var welcome = switch (corruption) {
        .eof_before_welcome => return,
        .welcome_other_role => service_frames.encodeWelcome(4242, other, 0, 1),
        .welcome_queue_role, .welcome_queue_kind => service_frames.encodeWelcome(4242, role, 0, 1),
        .welcome_slot_out_of_range => service_frames.encodeWelcome(4242, role, 1, 1),
        .corrupt_ring, .frame_after_welcome => service_frames.encodeWelcome(4242, role, 0, 1),
        else => unreachable,
    };
    const fields: *align(1) wire_protocol.ControlServiceWelcome = @ptrCast(&welcome);
    if (corruption == .welcome_queue_role) fields.role = @backingInt(wire_protocol.QueueRole.consumer);
    if (corruption == .welcome_queue_kind) fields.header.kind = .queue;
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
    /// JOIN with a queue's roles, a broadcast's, a role nobody knows, or none: dropped.
    role_producer,
    role_consumer,
    role_subscriber,
    role_unknown,
    role_zero,
    /// JOIN of a queue's kind, of another version, or another frame type: dropped.
    kind_queue,
    other_version,
    bad_type,
    /// The end of the stream where JOIN belongs, and a JOIN cut short.
    eof_before_join,
    eof_midframe,
    /// A valid client or server that sends a stray frame after WELCOME: the owner retires its place.
    client_frame_after_welcome,
    server_frame_after_welcome,
    /// A valid client or server that ends at once: the owner frees its place.
    client_end_after_welcome,
    server_end_after_welcome,
    /// A valid server that claims a library client's request by hand and ends: the request is abandoned. With a stray
    /// frame first, it is abandoned only at the server's end (its client waits until then).
    server_holding_then_end,
    server_holding_then_frame,
    /// A valid client, the service's only one, that publishes requests by hand and ends: the owner drops them, and its
    /// place goes to the next client once its ring settled.
    client_pending_then_end,
};

/// A library owner's process admits the forged joiner; then a library client and server must carry a request and its
/// reply (`roundTrip`), and for the holding cases the client first receives ABANDONED for the request the forged
/// server claimed. True when the service still served them.
pub fn joinerCase(corruption: JoinerCorruption, counter: u64) bool {
    var name_buf: [64]u8 = undefined;
    const name = fuzz.uniqueName(&name_buf, counter);
    const clients: u32 = if (corruption == .client_pending_then_end) 1 else max_clients;
    const service = fastipc.serviceCreate(fuzz.gpa, name, capacity, max_message, clients, max_servers) catch return false;
    defer service.close();
    return switch (corruption) {
        .server_holding_then_end, .server_holding_then_frame => holdingCase(name, corruption == .server_holding_then_frame),
        .client_pending_then_end => pendingCase(name),
        else => forgedJoinerCase(name, corruption),
    };
}

/// A forged joiner does its wrong (keeping its connection until the case is over, unless it ends it), then a library
/// client and server join and carry a request and its reply.
fn forgedJoinerCase(name: []const u8, corruption: JoinerCorruption) bool {
    const Joiner = struct {
        name: []const u8,
        corruption: JoinerCorruption,
        done: std.atomic.Value(bool) = .init(false),

        fn run(joiner: *@This()) void {
            var member = RawMember.connect(joiner.name) orelse return;
            defer member.deinit();
            forgeJoiner(&member, joiner.corruption);
            switch (joiner.corruption) {
                .eof_before_join, .eof_midframe, .client_end_after_welcome, .server_end_after_welcome => return,
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
    // Whatever the forged joiner does, the owner's poller admits the next joiners
    fuzz.sleep(50);
    const client = joinNow(name, .client) orelse return report(corruption, "a library client couldn't join afterwards", .{});
    defer client.close();
    const server = joinNow(name, .server) orelse return report(corruption, "a library server couldn't join afterwards", .{});
    defer server.close();
    return roundTrip(client, server, corruption);
}

fn forgeJoiner(member: *RawMember, corruption: JoinerCorruption) void {
    var frame = service_frames.encodeJoin(4243, .client);
    const fields: *align(1) wire_protocol.ControlServiceJoin = @ptrCast(&frame);
    switch (corruption) {
        .role_producer => fields.role = @backingInt(wire_protocol.QueueRole.producer),
        .role_consumer => fields.role = @backingInt(wire_protocol.QueueRole.consumer),
        .role_subscriber => fields.role = @backingInt(wire_protocol.SubscriberRole.lossy),
        .role_unknown => fields.role = 7,
        .role_zero => fields.role = 0,
        .kind_queue => fields.header.kind = .queue,
        .other_version => fields.header.version = wire_protocol.channel_protocol_version + 1,
        .bad_type => fields.header.frame_type = @backingInt(wire_protocol.ChannelFrameType.welcome),
        .eof_before_join => return,
        .eof_midframe => return member.send(frame[0..10]),
        .client_frame_after_welcome, .server_frame_after_welcome, .client_end_after_welcome, .server_end_after_welcome => {
            const role: fastipc.ServiceRole = switch (corruption) {
                .client_frame_after_welcome, .client_end_after_welcome => .client,
                else => .server,
            };
            if (!member.join(role)) return;
            if (corruption == .client_frame_after_welcome or corruption == .server_frame_after_welcome) member.send(&@as(wire.Frame, @splat(0xAB)));
            return;
        },
        else => unreachable,
    }
    member.send(&frame);
}

/// A library client sends a request, which a forged server claims by hand (its intent, then the state's
/// compare-and-swap, then `head`, as a server's take does); the server then sends a stray frame (`frame_first`) and
/// ends, or ends at once. The client's receive returns Abandoned for that request (not before the server's end), then
/// a library server joins and the service carries the next request and its reply.
fn holdingCase(name: []const u8, frame_first: bool) bool {
    const corruption: JoinerCorruption = if (frame_first) .server_holding_then_frame else .server_holding_then_end;
    const client = joinNow(name, .client) orelse return report(corruption, "the library client couldn't join", .{});
    defer client.close();
    var member = joinedRawMember(name, .server) orelse return report(corruption, "the forged server couldn't join", .{});
    var member_open = true;
    defer if (member_open) member.deinit();
    client.send("held", ms(1000)) catch |err| return report(corruption, "the client's send returned {s}", .{@errorName(err)});
    if (!member.claimNext()) return report(corruption, "the forged server found no request to claim", .{});
    var buf: [max_message]u8 = undefined;
    if (frame_first) {
        member.send(&@as(wire.Frame, @splat(0xAB)));
        // Retired, the server is still watched: its request waits for its end
        if (client.recv(&buf, ms(200))) |_| {
            return report(corruption, "the client received a reply nobody sent", .{});
        } else |err| if (err != error.Timeout) return report(corruption, "the client's receive before the server's end returned {s}", .{@errorName(err)});
    }
    member.deinit();
    member_open = false;
    if (client.recv(&buf, ms(2000))) |_| {
        return report(corruption, "the client received a reply to the abandoned request", .{});
    } else |err| if (err != error.Abandoned) return report(corruption, "the client's receive returned {s}, not Abandoned", .{@errorName(err)});
    const server = joinNow(name, .server) orelse return report(corruption, "a library server couldn't join afterwards", .{});
    defer server.close();
    return roundTrip(client, server, corruption);
}

/// A forged client, the service's only one, publishes three requests by hand and ends; the owner drops them (the
/// ring's `head` reaches its `tail`, seen through the forged client's mapping, which stays until then), and its place
/// goes to a library client once it settled. A library server's first request is then the library client's.
fn pendingCase(name: []const u8) bool {
    const corruption: JoinerCorruption = .client_pending_then_end;
    var member = joinedRawMember(name, .client) orelse return report(corruption, "the forged client couldn't join", .{});
    var mapped = true;
    defer if (mapped) member.deinit();
    for (0..3) |_| member.publish("ghost");
    os.close(member.conn);
    member.conn_open = false;
    const ring = member.layout.ring(member.slot);
    var waited: u32 = 0;
    while (ring.head.load(.acquire) != ring.tail.load(.acquire)) : (waited += 5) {
        if (waited >= 2000) return report(corruption, "the owner didn't drop the ended client's requests", .{});
        fuzz.sleep(5);
    }
    member.deinit();
    mapped = false;
    const client = joinNow(name, .client) orelse return report(corruption, "a library client didn't get the ended client's place", .{});
    defer client.close();
    const server = joinNow(name, .server) orelse return report(corruption, "a library server couldn't join", .{});
    defer server.close();
    return roundTrip(client, server, corruption);
}

/// The library client sends a request, the library server receives it (that one, nothing else) and replies, and the
/// client receives that reply.
fn roundTrip(client: fastipc.Conn, server: fastipc.Conn, corruption: JoinerCorruption) bool {
    client.send("fuzz", ms(1000)) catch |err| return report(corruption, "the client's send returned {s}", .{@errorName(err)});
    var buf: [max_message]u8 = undefined;
    const len = server.recv(&buf, ms(2000)) catch |err| return report(corruption, "the server's receive returned {s}", .{@errorName(err)});
    if (!std.mem.eql(u8, buf[0..len], "fuzz")) return report(corruption, "the server received {d} bytes that aren't the client's request", .{len});
    server.send("zzuf", ms(0)) catch |err| return report(corruption, "the server's reply returned {s}", .{@errorName(err)});
    const got = client.recv(&buf, ms(2000)) catch |err| return report(corruption, "the client's receive returned {s}", .{@errorName(err)});
    if (!std.mem.eql(u8, buf[0..got], "zzuf")) return report(corruption, "the client received {d} bytes that aren't the server's reply", .{got});
    return true;
}

fn report(corruption: JoinerCorruption, comptime fmt: []const u8, args: anytype) bool {
    std.debug.print("fuzz:   service joiner {s}: ", .{@tagName(corruption)});
    std.debug.print(fmt ++ "\n", args);
    return false;
}

/// Joins the service on `name` as `role`, within 3 s: a joiner the owner dropped (no free place yet) comes again.
fn joinNow(name: []const u8, role: fastipc.ServiceRole) ?fastipc.Conn {
    return fastipc.serviceJoin(fuzz.gpa, name, role, ms(3000)) catch null;
}

/// A raw member joined as `role` on a thread of its own (the owner's admission runs on its poller either way).
fn joinedRawMember(name: []const u8, role: fastipc.ServiceRole) ?RawMember {
    const Join = struct {
        fn run(n: []const u8, r: fastipc.ServiceRole, out: *?RawMember) void {
            var member = RawMember.connect(n) orelse return;
            if (!member.join(r)) {
                member.deinit();
                return;
            }
            out.* = member;
        }
    };
    var joined: ?RawMember = null;
    const thread = std.Thread.spawn(.{}, Join.run, .{ name, role, &joined }) catch return null;
    thread.join();
    return joined;
}

/// A service member played by hand: its control connection, and once it joined, its mapping of the segment and its
/// place.
const RawMember = struct {
    conn: os.Handle,
    conn_open: bool = true,
    offer: service_frames.Offer,
    received: ?os.Handle,
    session: ?Session = null,
    layout: service_segment.Layout = undefined,
    slot: u32 = 0,

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
        if (wire.classifyOffer(&frame, .service) != .offer) {
            if (received) |h| os.close(h);
            os.close(conn);
            return null;
        }
        return .{ .conn = conn, .offer = service_frames.decodeOffer(&frame), .received = received };
    }

    /// Maps the offered segment, sends a valid JOIN as `role` and reads WELCOME. False on any failure.
    fn join(member: *RawMember, role: fastipc.ServiceRole) bool {
        var session = service_segment.attach(member.received, &member.offer) catch return false;
        member.layout = service_segment.layoutOf(&session, member.offer.max_clients, member.offer.max_servers, member.offer.max_message);
        member.session = session;
        member.send(&service_frames.encodeJoin(4243, role));
        var welcome: wire.Frame = undefined;
        if (!fuzz.readFull(member.conn, &welcome)) return false;
        const max_slots = if (role == .client) member.offer.max_clients else member.offer.max_servers;
        const welcomed = service_frames.checkWelcome(&welcome, role, max_slots) orelse return false;
        member.slot = welcomed.slot;
        return true;
    }

    /// As server `member.slot`: claims the next request of the first client ring that has one, as a server's take
    /// does (the intent, then the state's compare-and-swap, then `head`), within 2 s. False if none came, or the claim
    /// failed.
    fn claimNext(member: *RawMember) bool {
        const layout = &member.layout;
        var waited: u32 = 0;
        const ring: u32 = found: while (true) : (waited += 5) {
            for (0..layout.max_senders) |c| {
                const control = layout.ring(@intCast(c));
                if (control.head.load(.acquire) < control.tail.load(.acquire)) break :found @intCast(c);
            }
            if (waited >= 2000) return false;
            fuzz.sleep(5);
        };
        const control = layout.ring(ring);
        const head = control.head.load(.acquire);
        const tag: u56 = @intCast(head + 1);
        layout.takerSlot(member.slot).claim.store(@bitCast(wire_protocol.QueueIntent{ .ring_plus_one = @intCast(ring + 1), .tag = tag }), .release);
        const published: u64 = @bitCast(wire_protocol.QueueCellState{ .phase = .published, .claimer = 0, .tag = tag });
        const claimed: u64 = @bitCast(wire_protocol.QueueCellState{ .phase = .claimed, .claimer = @intCast(member.slot), .tag = tag });
        if (layout.cell(ring, head).state.cmpxchgStrong(published, claimed, .acq_rel, .monotonic) != null) return false;
        _ = control.head.cmpxchgStrong(head, head + 1, .release, .monotonic);
        return true;
    }

    /// As client `member.slot`: publishes `payload` as a request at its ring's `tail` as a client's send does (the reply's
    /// room, then the request, from the ring's start: the ring is new), and sets its ring's ready bit.
    fn publish(member: *RawMember, payload: []const u8) void {
        const layout = &member.layout;
        const control = layout.ring(member.slot);
        const index = control.tail.load(.acquire);
        const room = layout.replyRoom();
        const position = index * (room + 64) + room;
        @memcpy(layout.bytes(member.slot, position, payload.len), payload);
        const cell = layout.cell(member.slot, index);
        cell.payload_position.store(position, .monotonic);
        cell.len.store(@intCast(payload.len), .monotonic);
        cell.reply_len.store(0, .monotonic);
        cell.outcome.store(0, .monotonic);
        cell.state.store(@bitCast(wire_protocol.QueueCellState{ .phase = .published, .claimer = 0, .tag = @intCast(index + 1) }), .release);
        control.tail.store(index + 1, .seq_cst);
        _ = layout.header.ready.fetchOr(@as(u64, 1) << @intCast(member.slot), .seq_cst);
    }

    fn send(member: *RawMember, bytes: []const u8) void {
        _ = os.sendFrame(member.conn, bytes, null);
    }

    /// Leaves: the owner reads the end of its connection. The mapping goes after it.
    fn deinit(member: *RawMember) void {
        if (member.conn_open) os.close(member.conn);
        member.conn_open = false;
        if (member.received) |h| os.close(h);
        member.received = null;
        if (member.session) |*session| session.unmap();
        member.session = null;
    }
};

// === Ring content: a corrupt member ===

/// How a ring-content trial's corrupt member writes the segment; the trial's seed picks the mode.
pub const RingMode = enum {
    /// Random bytes over the whole segment: the description, every shared word, the cells and the data (the end's mark
    /// mostly 0 again).
    bytes,
    /// Random values in every shared word: `ended`, `ready`, `idle`, each server's `sleeps` and `claim` (intents of
    /// rings that exist and don't), each client ring's `tail`, `head`, `room_sleeps` and `reply_sleeps`.
    words,
    /// Forged cells: any phase, claimer and index, near the rings' indices or not; request positions below the reply's
    /// room, past the ring or anywhere; request and reply lengths of 0, over max_message or huge; any outcome.
    cells,
    /// The three above, again and again from a thread, while the library works.
    racing,
};

/// Runs one service ring-content trial in a child process (`fuzz --service-child <seed>`) and reports whether it
/// returned cleanly: exit code 0. An abort, a panic or a crash ends the child otherwise, and so does its watchdog if a
/// call hangs.
pub fn ringCase(seed: u64) bool {
    var seed_buf: [24]u8 = undefined;
    const seed_str = std.mem.print(&seed_buf, "{d}", .{seed}) catch return false;
    var child = std.process.spawn(fuzz.io, .{
        .argv = &.{ fuzz.self_exe, "--service-child", seed_str },
        .stdout = .ignore,
        .stderr = .ignore,
    }) catch return false;
    const term = child.wait(fuzz.io) catch return false;
    if (term == .exited and term.exited == 0) return true;
    std.debug.print("fuzz:   service seed {d}: {any} (repeat it: fuzz --service-child {d})\n", .{ seed, term, seed });
    return false;
}

/// The library's members of a ring-content trial.
const Members = struct {
    clients: [2]fastipc.Conn,
    servers: [2]fastipc.Conn,

    fn all(members: *const Members) [4]fastipc.Conn {
        return .{ members.clients[0], members.clients[1], members.servers[0], members.servers[1] };
    }
};

/// One service ring-content trial, in-process (the child): a library owner, two library clients and two library
/// servers (one copying, one in place), and the corrupt member (a client or a server, by the seed), which writes the
/// segment (`RingMode`) and then leaves. The library's members make each kind of data call a few times with short
/// timeouts, before and after the corrupt member leaves (the owner then abandons, drops or retires what its place
/// names); then everything closes. 0 once every call returned.
pub fn ringTrial(seed: u64) u8 {
    _ = std.Thread.spawn(.{}, fuzz.watchdog, .{}) catch return 4;
    var name_buf: [64]u8 = undefined;
    const name = fuzz.uniqueName(&name_buf, seed);
    const service = fastipc.serviceCreate(fuzz.gpa, name, capacity, max_message, max_clients, max_servers) catch return 0;
    defer service.close();
    var members: Members = undefined;
    for (&members.clients) |*client| client.* = joinNow(name, .client) orelse return 0;
    defer for (members.clients) |client| client.close();
    for (&members.servers) |*server| server.* = joinNow(name, .server) orelse return 0;
    defer for (members.servers) |server| server.close();
    // Requests in the rings, some answered, before the corrupt member writes them
    for (0..3) |_| {
        for (members.clients) |client| client.send("before", ms(50)) catch {};
    }
    serve(members.servers[0], false);

    const role: fastipc.ServiceRole = if ((seed / std.enums.values(RingMode).len) % 2 == 0) .client else .server;
    var member = joinedRawMember(name, role) orelse return 0;
    defer member.deinit();
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
    const canceller = std.Thread.spawn(.{}, cancelAfter, .{ members.all(), &calls_done }) catch return 4;
    defer canceller.join();
    defer calls_done.store(true, .release);

    dataCalls(&members);
    if (racer) |thread| {
        stop.store(true, .release);
        thread.join();
    }
    // The corrupt member leaves: the owner's poller reads its end and abandons, drops or retires, whatever its place's
    // words say
    member.deinit();
    fuzz.sleep(20);
    _ = service.waitForMembers(null, 0, ms(0)) catch 0;
    dataCalls(&members);
    return 0;
}

/// Each kind of data call a few times, with short timeouts: any result.
fn dataCalls(members: *const Members) void {
    const quick = ms(30);
    var buf: [max_message]u8 = undefined;
    for (0..5) |_| {
        for (members.clients) |client| {
            client.send("fuzz", quick) catch {};
            if (client.acquire(16, quick)) |buffer| {
                @memset(buffer, 0x5A);
                client.commit(buffer.len) catch {};
            } else |_| {}
        }
        serve(members.servers[0], false);
        serve(members.servers[1], true);
        for (members.clients) |client| {
            _ = client.recv(&buf, quick) catch {};
            if (client.recvAcquire(quick)) |_| client.release() else |_| {}
        }
    }
}

/// One request taken and replied to (`in_place`: received and replied to in place), any result. A server whose reply
/// failed still holds its request: its next receives return Invalid, which is a result too.
fn serve(server: fastipc.Conn, in_place: bool) void {
    const quick = ms(30);
    var buf: [max_message]u8 = undefined;
    if (in_place) {
        const request = server.recvAcquire(quick) catch return;
        const reply = server.acquire(@max(request.len, 1), quick) catch return;
        @memset(reply, 0xA5);
        server.commit(reply.len) catch {};
        server.release();
    } else {
        const len = server.recv(&buf, quick) catch return;
        server.send(buf[0..len], quick) catch {};
    }
}

/// Cancels the members once the trial's calls have run for `cancel_after_ms`, unless they are done by then.
fn cancelAfter(conns: [4]fastipc.Conn, done: *const std.atomic.Value(bool)) void {
    var waited: u32 = 0;
    while (!done.load(.acquire)) : (waited += 10) {
        if (waited >= cancel_after_ms) {
            for (conns) |conn| conn.cancel();
            return;
        }
        fuzz.sleep(10);
    }
}

/// Longer than the trial's calls take when none waits past its timeout (two rounds of five times about twelve 30 ms
/// calls).
const cancel_after_ms = 6000;

/// The corrupt member's writes into the segment, through its own mapping.
const Scribbler = struct {
    layout: service_segment.Layout,
    rng: std.Random.DefaultPrng,

    fn scribbleBytes(scribbler: *Scribbler) void {
        const size = service_segment.describe(capacity, max_clients, max_servers, max_message).segment_size;
        const random = scribbler.rng.random();
        random.bytes(scribbler.layout.base[0..size]);
        // Mostly not ended, so that the calls go on into the rest of the garbage
        if (random.uintLessThan(u8, 3) != 0) scribbler.layout.header.ended.store(0, .seq_cst);
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

    /// An intent word: of a ring that exists or doesn't (up to 255), an index near `near` or anywhere; or random.
    fn intent(scribbler: *Scribbler, near: u64) u64 {
        const random = scribbler.rng.random();
        if (random.uintLessThan(u8, 4) == 0) return random.int(u64);
        const ring_plus_one: u8 = if (random.boolean()) random.uintAtMost(u8, max_clients + 1) else random.int(u8);
        const tag: u56 = @truncate(scribbler.value(near) +% 1);
        return @bitCast(wire_protocol.QueueIntent{ .ring_plus_one = ring_plus_one, .tag = tag });
    }

    fn scribbleWords(scribbler: *Scribbler) void {
        const random = scribbler.rng.random();
        const layout = &scribbler.layout;
        layout.header.ended.store(random.uintLessThan(u32, 3) / 2, .seq_cst); // mostly 0: the service goes on
        layout.header.ready.store(random.int(u64), .seq_cst);
        layout.header.idle.store(random.int(u64), .seq_cst);
        for (0..layout.max_takers) |s| {
            const slot = layout.takerSlot(@intCast(s));
            slot.sleeps.store(random.int(u32), .seq_cst);
            slot.claim.store(scribbler.intent(layout.ring(0).head.load(.seq_cst)), .seq_cst);
        }
        for (0..layout.max_senders) |c| {
            const ring = layout.ring(@intCast(c));
            const tail = ring.tail.load(.seq_cst);
            ring.tail.store(scribbler.value(tail), .seq_cst);
            ring.head.store(scribbler.value(tail), .seq_cst);
            ring.room_sleeps.store(random.int(u32), .seq_cst);
            ring.reply_sleeps.store(random.int(u32), .seq_cst);
        }
    }

    /// A request position: random, below the reply's room, past the ring, at the room's edge, or plausible.
    fn position(scribbler: *Scribbler) u64 {
        const random = scribbler.rng.random();
        const room = scribbler.layout.replyRoom();
        const lap = random.uintAtMost(u64, 8) * capacity;
        return switch (random.uintLessThan(u8, 6)) {
            0 => random.int(u64),
            1 => lap + random.uintLessThan(u64, room), // below the reply's room
            2 => lap + capacity - random.uintAtMost(u64, 8), // the request past the ring's end
            3 => lap + room,
            4 => std.math.maxInt(u64) - random.uintAtMost(u64, capacity),
            else => lap + room + random.uintLessThan(u64, capacity - room),
        };
    }

    /// A length: 0, 1, max_message, just over it, huge, or random.
    fn length(scribbler: *Scribbler) u32 {
        const random = scribbler.rng.random();
        return switch (random.uintLessThan(u8, 6)) {
            0 => 0,
            1 => 1,
            2 => max_message,
            3 => max_message + 1,
            4 => std.math.maxInt(u32) - random.uintAtMost(u32, 3),
            else => random.uintAtMost(u32, capacity),
        };
    }

    fn forgeCells(scribbler: *Scribbler) void {
        const random = scribbler.rng.random();
        const layout = &scribbler.layout;
        for (0..layout.max_senders) |c| {
            const ring = layout.ring(@intCast(c));
            const head = ring.head.load(.seq_cst);
            for (0..layout.cellCount()) |k| {
                const cell = layout.cell(@intCast(c), head +% k);
                const index = if (random.boolean()) head +% k else scribbler.value(head +% k);
                const phase: wire_protocol.QueuePhase = @fromBackingInt(@as(u2, @intCast(random.uintAtMost(u64, 3))));
                const claimer: u6 = @intCast(random.uintLessThan(u64, 64));
                const state: wire_protocol.QueueCellState = .{ .phase = phase, .claimer = claimer, .tag = @truncate(index +% 1) };
                cell.payload_position.store(scribbler.position(), .release);
                cell.len.store(scribbler.length(), .release);
                cell.reply_len.store(scribbler.length(), .release);
                cell.outcome.store(switch (random.uintLessThan(u8, 5)) {
                    0, 1 => @backingInt(wire_protocol.ServiceOutcome.replied),
                    2 => @backingInt(wire_protocol.ServiceOutcome.abandoned),
                    3 => @backingInt(wire_protocol.ServiceOutcome.none),
                    else => random.int(u32),
                }, .release);
                cell.state.store(@bitCast(state), .release);
            }
            if (random.boolean()) ring.tail.store(head +% random.uintAtMost(u64, layout.cellCount() + 1), .seq_cst);
        }
        // The servers' intents: some name a forged claim of theirs, some no ring
        for (0..layout.max_takers) |s| {
            if (random.boolean()) layout.takerSlot(@intCast(s)).claim.store(scribbler.intent(layout.ring(0).head.load(.seq_cst)), .seq_cst);
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
