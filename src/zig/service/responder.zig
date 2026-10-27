//! A service's servers (docs/protocol.md §11.5): their receives, which take the next request of any client, and their
//! replies to the request they hold. A server's receiving thread is the service's ring set's taker (core/taker.zig: the
//! pass over the clients' rings, the claim, the ready bits, the park), which works on a mapped service segment
//! (segment.zig `Layout`) and the server's endpoint; the server's lifecycle sets it up. How clients send and receive is
//! caller.zig.
//!
//! A held request. A receive copies the request out (`recv`) or returns it in place (`recvAcquire`), and the server
//! then holds it (`Held`) until its reply: one at a time, so a receive while one is held is Invalid and changes
//! nothing. The reply goes into the request's reply room, which its client reserved just before the request
//! (core/ring_set.zig `Layout.replyRoom`), so a reply never waits; then the request is answered (core/ring_set.zig
//! `answer`: the length and the outcome, the done, the client's wake). The receiving thread publishes the held request
//! (`.release`), and the replying thread, which may be another one, loads it (`.acquire`) and clears it once it
//! answered (`.release`), so a server may receive on one thread and reply on another. A request held in place stays
//! valid until the reply: its room is its client's until the client consumed the answer. `fipc_recv_release` does
//! nothing; the close abandons a request still held (answered as abandoned, with no reply), and so does the owner for
//! a server that ended holding one (server.zig).
//!
//! The end. A service's servers don't drain (core/taker.zig, `drains` false): from the end's mark on, receives return
//! Disconnected, and so do replies, with nothing written. A reply that stores its done and then sees the mark
//! (sequentially consistent) returns Disconnected too, though its client may have received it; one that returned OK is
//! received by a client that receives (caller.zig, the end's pair). Nothing is abandoned after the mark: the clients
//! see it. Its own connection's failure is Disconnected at once, but its close still abandons a request it holds.

const std = @import("std");
const platform = @import("../platform.zig");
const copy = @import("../core/copy.zig").copy;
const payload_ring = @import("../core/payload_ring.zig");
const segment = @import("segment.zig");
const taker_module = @import("../core/taker.zig");

const Atomic = std.atomic.Value;
const Claim = @import("../core/ring_set.zig").Claim;
const Endpoint = @import("../core/endpoint.zig").Endpoint;
const Deadline = @import("../core/wait.zig").Deadline;
const Layout = segment.Layout;
const Rings = segment.Rings;

const DataError = @import("../core/endpoint.zig").DataError;

/// The service's taker: it doesn't drain at the service's end.
pub const Taker = taker_module.Taker(segment.Rings, false);

/// A server's state: `taker`, its receiving thread's; `held`, which both threads touch; `replier`, its replying
/// thread's; each on lines of its own.
pub const Responder = struct {
    taker: Taker,
    held: Held align(64) = .{},
    replier: Replier align(64),

    /// The server of slot `slot` (core/taker.zig `Taker.init`).
    pub fn init(layout: Layout, slot: u32, own_event: platform.RingEvent, session_id: [16]u8) Responder {
        return .{ .taker = .init(layout, slot, own_event, session_id), .replier = .{ .layout = layout, .slot = slot, .wakes = .init(session_id) } };
    }

    /// Closes the wakes it opened. The caller let go of a held request first (`leave`).
    pub fn deinit(responder: *Responder) void {
        responder.taker.deinit();
        responder.replier.wakes.deinit();
    }

    // === The receiving thread ===

    /// `fipc_recv` on a server: takes the next request of any client, copies it into `buf`, its length in `len`, and
    /// holds it until the reply. Invalid while one is held; TooLarge (and the length of the request found in `len`)
    /// for a `buf` too small: nothing is taken, the request stays for any server. Invalid for a corrupt request, which
    /// is answered as abandoned.
    pub fn recv(responder: *Responder, endpoint: *Endpoint, buf: []u8, timeout_ms: c_int, len: *usize) DataError!void {
        const taker = try responder.openToTake(endpoint);
        var deadline: Deadline = .init(timeout_ms);
        while (true) switch (try taker.take(buf.len)) {
            .claimed => |claim| {
                // Longer than the look before the claim said: a cell changed under its claim, a corrupt peer's
                if (claim.len > buf.len) {
                    taker.drop(claim);
                    return error.Invalid;
                }
                copy(buf[0..claim.len], taker.layout.bytes(claim.ring, claim.position, claim.len));
                len.* = claim.len;
                responder.held.hold(claim);
                return;
            },
            .too_large => |found| {
                len.* = found;
                return error.TooLarge;
            },
            .nothing => try taker.wait(endpoint, &deadline),
        };
    }

    /// `fipc_recv_acquire` on a server: takes the next request of any client in place (64-byte aligned in its client's
    /// data ring) and holds it until the reply. Invalid while one is held.
    pub fn recvAcquire(responder: *Responder, endpoint: *Endpoint, timeout_ms: c_int) DataError![]const u8 {
        const taker = try responder.openToTake(endpoint);
        var deadline: Deadline = .init(timeout_ms);
        while (true) switch (try taker.take(std.math.maxInt(usize))) {
            .claimed => |claim| {
                responder.held.hold(claim);
                return taker.layout.bytes(claim.ring, claim.position, claim.len);
            },
            .too_large => unreachable,
            .nothing => try taker.wait(endpoint, &deadline),
        };
    }

    /// A receive's start: Disconnected from the end's mark on, or after this server's own failure; then Invalid while a
    /// request is held. The taker, ready for its passes.
    fn openToTake(responder: *Responder, endpoint: *Endpoint) DataError!*Taker {
        const taker = &responder.taker;
        try taker.open(endpoint);
        if (responder.held.request() != null) return error.Invalid;
        taker.spun = false;
        return taker;
    }

    // === The replying thread ===

    /// `fipc_send` on a server: replies `payload` to the request it holds (the module's comment). Invalid with none
    /// held; TooLarge over `max_message` (the request stays held); Disconnected once the service ended or this server's
    /// connection did (nothing written; the request stays held), and when the done saw the end's mark. Drops a
    /// reservation.
    pub fn send(responder: *Responder, endpoint: *Endpoint, payload: []const u8) DataError!void {
        const replier = &responder.replier;
        replier.reservation.drop();
        const claim = responder.held.request() orelse return error.Invalid;
        if (payload.len > responder.view().maxPiece()) return error.TooLarge;
        try responder.open(endpoint);
        copy(responder.replyRoom(claim, payload.len), payload);
        return responder.answer(claim, payload.len);
    }

    /// `fipc_send_acquire` on a server: the reply's room for up to `len` bytes, in place in its client's data ring
    /// (64-byte aligned), for `sendCommit`. As `send`, it never waits; Invalid for a `len` of 0, which no commit could
    /// use. A later acquire or send drops the reservation.
    pub fn sendAcquire(responder: *Responder, endpoint: *Endpoint, len: usize) DataError![]u8 {
        const replier = &responder.replier;
        replier.reservation.drop();
        if (len == 0) return error.Invalid;
        const claim = responder.held.request() orelse return error.Invalid;
        if (len > responder.view().maxPiece()) return error.TooLarge;
        try responder.open(endpoint);
        replier.reservation.set(0, len);
        return responder.replyRoom(claim, len);
    }

    /// `fipc_send_commit` on a server: replies with the first `len` bytes of the reservation. Invalid without one, or
    /// for a `len` of 0 or past it (the reservation stays); Disconnected as `send`'s (the reservation is dropped).
    pub fn sendCommit(responder: *Responder, endpoint: *Endpoint, len: usize) DataError!void {
        _ = try responder.replier.reservation.commit(len);
        const claim = responder.held.request() orelse return error.Invalid;
        try responder.open(endpoint);
        return responder.answer(claim, len);
    }

    /// Disconnected once the service ended or this server's connection did: not exact (the exact pair is `answer`'s),
    /// it only saves a reply after the end.
    fn open(responder: *const Responder, endpoint: *const Endpoint) DataError!void {
        if (endpoint.hot.phase.load(.acquire) != .connected) return error.Disconnected;
        if (responder.view().header.ended.load(.acquire) != 0) return error.Disconnected;
    }

    /// The first `len` bytes of request `claim`'s reply room, just before the request (its position checked at the
    /// claim: core/ring_set.zig `validPiece`).
    fn replyRoom(responder: *const Responder, claim: Claim, len: usize) []u8 {
        const layout = responder.view();
        return layout.bytes(claim.ring, claim.position - layout.replyRoom(), len);
    }

    /// The reply of `len` bytes, in its room, answers the held request (core/ring_set.zig `answer`), which is held no
    /// more; then the load of the end's mark, the server's half of the end's pair.
    fn answer(responder: *Responder, claim: Claim, len: usize) DataError!void {
        const layout = responder.view();
        Rings.answer(layout, &responder.replier.wakes, responder.replier.slot, claim, .replied, @intCast(len));
        responder.held.clear();
        if (layout.header.ended.load(.seq_cst) != 0) return error.Disconnected;
    }

    // === The close ===

    /// The close's: a request still held is answered as abandoned (no reply), unless the service has ended (its client
    /// sees the mark). No other thread is inside a call on the server.
    pub fn leave(responder: *Responder) void {
        const claim = responder.held.request() orelse return;
        if (responder.view().header.ended.load(.acquire) == 0) {
            Rings.answer(responder.view(), &responder.replier.wakes, responder.replier.slot, claim, .abandoned, 0);
        }
        responder.held.clear();
    }

    /// The server's view of the segment: the replying thread's copy (`Replier.layout`).
    fn view(responder: *const Responder) *const Layout {
        return &responder.replier.layout;
    }
};

/// The request a server holds, which its receiving thread publishes and its replying thread clears.
pub const Held = struct {
    /// Set (`.release`) once `claim` is the held request; cleared (`.release`) once it is answered. Each thread loads
    /// it `.acquire` before it reads or writes `claim`.
    holding: Atomic(bool) = .init(false),
    claim: Claim = undefined,

    fn hold(held: *Held, claim: Claim) void {
        held.claim = claim;
        held.holding.store(true, .release);
    }

    /// The held request; null for none.
    pub fn request(held: *const Held) ?Claim {
        return if (held.holding.load(.acquire)) held.claim else null;
    }

    fn clear(held: *Held) void {
        held.holding.store(false, .release);
    }
};

/// A server's replying thread's state.
pub const Replier = struct {
    /// The server's view of the segment and its slot, as the join set them: copies of the taker's, whose lines the
    /// receiving thread writes.
    layout: Layout,
    slot: u32,
    /// The clients' wakes (Windows), for the replies' wakes of their receiving threads.
    wakes: segment.Wakes,
    /// The reservation of the last `fipc_send_acquire`: only its length counts.
    reservation: payload_ring.Reservation = .{},
};
