//! A work queue's consumers: their receives, which take each message of any ring for one consumer, and acknowledge it.
//! A consumer is the queue's ring set's taker (core/taker.zig: the pass over the rings, the claim, the ready bits, the
//! park), which works on a mapped queue segment (segment.zig `Layout`) and the consumer's endpoint; the consumer's
//! lifecycle sets it up. How producers publish is producer.zig.
//!
//! A receive copies the message it took out and marks it done before it returns (the acknowledgement), or holds it in
//! place until `release`, the next receive or the close. The owner returns the message of a consumer that ended
//! holding it for another consumer (returns.zig). The rings are the producers' and the return ring (segment.zig), which
//! a pass treats as any other: a returned message comes after its producer's later ones.
//!
//! The end. The queue's end lets a consumer take what is left, then Disconnected (its taker drains); its own
//! connection's failure is Disconnected at once.

const std = @import("std");
const build_options = @import("build_options");
const platform = @import("../platform.zig");
const test_hooks = @import("../core/test_hooks.zig");
const copy = @import("../core/copy.zig").copy;
const segment = @import("segment.zig");
const taker_module = @import("../core/taker.zig");

const Claim = @import("../core/ring_set.zig").Claim;
const Endpoint = @import("../core/endpoint.zig").Endpoint;
const Deadline = @import("../core/wait.zig").Deadline;
const Layout = segment.Layout;

const DataError = @import("../core/endpoint.zig").DataError;

/// The queue's taker: it drains at the queue's end.
pub const Taker = taker_module.Taker(segment.Rings, true);

/// A consumer's state: private to its receiving thread.
pub const Consumer = struct {
    taker: Taker,
    /// A message held in place (`recvAcquire`) until `release`, the next receive or the close.
    held: ?Claim = null,

    /// The consumer of slot `slot` (core/taker.zig `Taker.init`).
    pub fn init(layout: Layout, slot: u32, own_event: platform.RingEvent, session_id: [16]u8) Consumer {
        return .{ .taker = .init(layout, slot, own_event, session_id) };
    }

    /// Closes the wakes it opened. The caller released a held message first (`release`).
    pub fn deinit(consumer: *Consumer) void {
        consumer.taker.deinit();
    }

    /// `fipc_recv` on a consumer: lets a held message go, then takes the next message of any ring and copies it into
    /// `buf`, its length in `len`, and marks it done before it returns (the acknowledgement: a consumer that ends
    /// inside the call has its message returned, one that ends after it doesn't). TooLarge (and the length of the
    /// message found in `len`) for a `buf` too small: nothing is taken, the message stays for any consumer. Invalid for
    /// a corrupt message, which is dropped.
    pub fn recv(consumer: *Consumer, endpoint: *Endpoint, buf: []u8, timeout_ms: c_int, len: *usize) DataError!void {
        consumer.release();
        const taker = &consumer.taker;
        try taker.open(endpoint);
        taker.spun = false;
        var deadline: Deadline = .init(timeout_ms);
        while (true) switch (try taker.take(buf.len)) {
            .claimed => |claim| {
                defer taker.finish(claim);
                // Longer than the look before the claim said: a cell changed under its claim, a corrupt peer's
                if (claim.len > buf.len) return error.Invalid;
                copy(buf[0..claim.len], taker.layout.bytes(claim.ring, claim.position, claim.len));
                if (build_options.test_hooks) test_hooks.crashIfAt(.queue_copied);
                len.* = claim.len;
                return;
            },
            .too_large => |found| {
                len.* = found;
                return error.TooLarge;
            },
            .nothing => try taker.wait(endpoint, &deadline),
        };
    }

    /// `fipc_recv_many` on a consumer: the next message as `recv` takes it (its TooLarge too), then every further one a
    /// pass finds without waiting that fits the rest of `buf`, up to `lens.len`, packed back to back, each one's length
    /// in `lens`; `count` is how many. Each is acknowledged as `recv` acknowledges it, before the call returns. After
    /// the first it stops at anything else: nothing to take, a message too long for the rest of `buf` (it stays for any
    /// consumer), a corrupt one (dropped, as `recv` drops it).
    pub fn recvMany(consumer: *Consumer, endpoint: *Endpoint, buf: []u8, lens: []usize, timeout_ms: c_int, count: *usize) DataError!void {
        try consumer.recv(endpoint, buf, timeout_ms, &lens[0]);
        const taker = &consumer.taker;
        var used = lens[0];
        var taken: usize = 1;
        while (taken < lens.len and used < buf.len) {
            const rest = buf.len - used;
            const claim = switch (taker.take(rest) catch break) {
                .claimed => |claim| claim,
                .too_large, .nothing => break,
            };
            defer taker.finish(claim);
            // Longer than the look before the claim said: a cell changed under its claim, a corrupt peer's
            if (claim.len > rest) break;
            copy(buf[used..][0..claim.len], taker.layout.bytes(claim.ring, claim.position, claim.len));
            lens[taken] = claim.len;
            used += claim.len;
            taken += 1;
        }
        count.* = taken;
    }

    /// `fipc_recv_acquire` on a consumer: lets a held message go, then takes the next message in place (64-byte
    /// aligned in its ring's data ring), held until `release`, the next receive or the close: the acknowledgement.
    pub fn recvAcquire(consumer: *Consumer, endpoint: *Endpoint, timeout_ms: c_int) DataError![]const u8 {
        consumer.release();
        const taker = &consumer.taker;
        try taker.open(endpoint);
        taker.spun = false;
        var deadline: Deadline = .init(timeout_ms);
        while (true) switch (try taker.take(std.math.maxInt(usize))) {
            .claimed => |claim| {
                consumer.held = claim;
                return taker.layout.bytes(claim.ring, claim.position, claim.len);
            },
            .too_large => unreachable,
            .nothing => try taker.wait(endpoint, &deadline),
        };
    }

    /// `fipc_recv_release`, and the close's: the held message is done. Nothing without one.
    pub fn release(consumer: *Consumer) void {
        const claim = consumer.held orelse return;
        consumer.held = null;
        consumer.taker.finish(claim);
    }
};
