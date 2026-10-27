//! A service's clients (docs/protocol.md §11.5): their requests, sent into the client's own ring, and their receives,
//! which take the answers in the order of the requests. A client's sending thread is the service's ring set's sender
//! (core/sender.zig: the publish, the room it waits for), whose every request reserves the room of its reply just
//! before it (core/ring_set.zig `Layout.replyRoom`); its receiving thread reads its own ring's answers (`Replies`).
//! Both work on a mapped service segment (segment.zig `Layout`) and the client's endpoint; the client's lifecycle sets
//! them up. How servers take the requests and answer them is responder.zig.
//!
//! The answers. A client's n-th answer is its n-th request's: its receive waits for the done of request `next`, its
//! count of the answers it consumed, then reads the outcome and the reply's length the server stored before the done.
//! A reply is read in the client's own data ring, at the start of the request's room, which its own position record
//! gives (core/position_record.zig), never the cell: a corrupt peer can make it read only its own ring. `fipc_recv`
//! copies the reply out and consumes it, `fipc_recv_acquire` holds it in place until the release, the next receive or
//! the close; an abandoned request (its server ended before it replied) is consumed with Abandoned. Consuming moves
//! `next` on (`.release`, after the reads of the reply): the sender's reclaim frees a request's room only below it.
//!
//! The reply's wait is exact (§11.6): a server stores the done (`.seq_cst`), then loads the ring's `reply_sleeps` and,
//! if it read 1, swaps it to 0 and wakes the receiving thread if the swap read 1; the receiving thread swaps the flag
//! to 1, then loads the state (all `.seq_cst`), and sleeps only if it isn't done. Several servers may swap the flag
//! (they answer a client's requests in any order); a wake for a later request only makes the receive look and sleep
//! again. The room's wake is the receiving thread's, after it consumed an answer, and inexact (core/sender.zig).
//!
//! The end. Once the service's end is marked, a receive delivers the answers already done, in order, then
//! Disconnected at its first request without one: the server stores the done and then loads `ended`, the receive
//! loads `ended` and then the state, all sequentially consistent, so a reply whose server saw no mark is seen here. Its
//! own connection's failure (the phase without the mark) is Disconnected at once. Disconnected is final: a done stored
//! after it (a reply or an abandon whose server looked at the mark before it was stored, or any done after this
//! client's own failure) is never received (`disconnected`). A send returns Disconnected only before it publishes
//! (core/sender.zig): a request it published is sent, and its receive returns its answer or Disconnected.

const std = @import("std");
const build_options = @import("build_options");
const platform = @import("../platform.zig");
const copy = @import("../core/copy.zig").copy;
const ring_set = @import("../core/ring_set.zig");
const segment = @import("segment.zig");
const wait_module = @import("../core/wait.zig");
const wire_protocol = @import("../wire_protocol.zig");

const Atomic = std.atomic.Value;
const Endpoint = @import("../core/endpoint.zig").Endpoint;
const Deadline = wait_module.Deadline;
const Layout = segment.Layout;
const PositionRecord = @import("../core/position_record.zig").PositionRecord;

const DataError = @import("../core/endpoint.zig").DataError;

/// The service's sender: a client's sending thread.
pub const Sender = @import("../core/sender.zig").Sender(segment.Rings);

/// A client's state: `sender`, its sending thread's, and `replies`, its receiving thread's, on lines of their own.
pub const Caller = struct {
    sender: Sender,
    replies: Replies align(64),

    /// The client of slot `slot`, in place: its sender starts at its ring's `tail`, where the ring's last client left
    /// it (every request answered: the owner gives a slot out only then), and so does its count of answers.
    /// `positions`: `cell_count` entries, the caller's, zeroed here: the receiving thread reads an entry only for a
    /// request whose done it saw, which a corrupt peer can forge before the request is sent, and then finds the start
    /// of the ring. Invalid: the ring's indices are a corrupt peer's.
    pub fn init(
        caller: *Caller,
        layout: Layout,
        slot: u32,
        positions: []u32,
        room_event: platform.RingEvent,
        reply_event: platform.RingEvent,
        session_id: [16]u8,
    ) DataError!void {
        @memset(positions, 0);
        caller.* = .{
            .sender = try .init(layout, slot, positions, room_event, session_id),
            .replies = undefined,
        };
        caller.replies = .{
            .layout = layout,
            .ring = slot,
            .positions = caller.sender.positions,
            .room_event = room_event,
            .reply_event = reply_event,
            .next = .init(caller.sender.next),
        };
        caller.sender.consumed = &caller.replies.next;
    }

    /// Closes the wakes it opened.
    pub fn deinit(caller: *Caller) void {
        caller.sender.deinit();
    }
};

/// What a client's receive found for its next request.
const Answer = union(enum) { replied: u32, abandoned, corrupt };

/// A client's receiving thread's state: the answers of its own ring, in the order of its requests.
pub const Replies = struct {
    /// What the join set: the client's view, its ring, its sending thread's position record (whose entries the
    /// sending thread writes before each publish), and its own two wakes besides their flags (Windows: its slot's
    /// events).
    layout: Layout,
    ring: u32,
    positions: PositionRecord,
    room_event: platform.RingEvent,
    reply_event: platform.RingEvent,
    /// The index of the request whose answer comes next: every answer below it is consumed (`.release`, after the
    /// reads of its reply, for the sending thread's reclaim, which loads it `.acquire`).
    next: Atomic(u64),
    /// The reply of request `next` is held in place (`recvAcquire`) until `release`, the next receive or the close.
    held: bool = false,
    /// A receive returned Disconnected: so does every later one.
    disconnected: bool = false,
    /// The sleeps for an answer begun, counted in the tests' builds only: a test that sees it move knows the receive
    /// is past every look before its sleep.
    sleeps: Atomic(u32) = .init(0),
    /// The spin before a sleep, and the longest sleep: the defaults, which only tests change.
    spin_iters: u32 = wait_module.spin_iters,
    longest_sleep_ms: u32 = wait_module.slice_ms,

    /// `fipc_recv` on a client: lets a held reply go, then waits for the answer of its next request. A reply is copied
    /// into `buf`, its length in `len`, and consumed; TooLarge (its length in `len`) for a `buf` too small leaves it
    /// for the next receive. Abandoned (`len` 0): the server that took the request ended before it replied; the request
    /// counts as answered. Invalid for a corrupt peer's answer, which is consumed.
    pub fn recv(replies: *Replies, endpoint: *Endpoint, buf: []u8, timeout_ms: c_int, len: *usize) DataError!void {
        replies.release();
        switch (try replies.await(endpoint, timeout_ms)) {
            .replied => |reply_len| {
                len.* = reply_len;
                if (reply_len > buf.len) return error.TooLarge;
                copy(buf[0..reply_len], replies.reply(reply_len));
                replies.consume();
            },
            .abandoned => {
                len.* = 0;
                replies.consume();
                return error.Abandoned;
            },
            .corrupt => {
                replies.consume();
                return error.Invalid;
            },
        }
    }

    /// `fipc_recv_many` on a client: the next answer as `recv` takes it (its TooLarge, Abandoned and Invalid too), then
    /// the replies of the requests after it, in order, that are done already and fit the rest of `buf`, up to
    /// `lens.len`, packed back to back, each one's length in `lens`; `count` is how many. They are consumed together,
    /// with one store of `next`. It stops, consuming nothing more, at the first request without an answer, a reply too
    /// long for the rest of `buf`, an abandoned request or a corrupt answer (the next receive returns them).
    pub fn recvMany(replies: *Replies, endpoint: *Endpoint, buf: []u8, lens: []usize, timeout_ms: c_int, count: *usize) DataError!void {
        replies.release();
        const first_len = switch (try replies.await(endpoint, timeout_ms)) {
            .replied => |reply_len| reply_len,
            .abandoned => {
                lens[0] = 0;
                replies.consume();
                return error.Abandoned;
            },
            .corrupt => {
                replies.consume();
                return error.Invalid;
            },
        };
        lens[0] = first_len;
        if (first_len > buf.len) return error.TooLarge;
        copy(buf[0..first_len], replies.reply(first_len));
        var index = replies.next.load(.monotonic) + 1;
        var used: usize = first_len;
        var taken: usize = 1;
        while (taken < lens.len) : (index += 1) {
            if (!answered(replies.layout.cell(replies.ring, index).state.load(.acquire), index)) break;
            const reply_len = switch (replies.answerOf(index)) {
                .replied => |reply_len| reply_len,
                .abandoned, .corrupt => break,
            };
            if (reply_len > buf.len - used) break;
            copy(buf[used..][0..reply_len], replies.layout.bytes(replies.ring, replies.positions.lowOf(index), reply_len));
            lens[taken] = reply_len;
            used += reply_len;
            taken += 1;
        }
        replies.consumeTo(index);
        count.* = taken;
    }

    /// `fipc_recv_acquire` on a client: lets a held reply go, then waits for the answer of its next request, and
    /// returns a reply in place (64-byte aligned in the client's data ring), held until `release`, the next receive or
    /// the close. Abandoned and Invalid as `recv`'s.
    pub fn recvAcquire(replies: *Replies, endpoint: *Endpoint, timeout_ms: c_int) DataError![]const u8 {
        replies.release();
        switch (try replies.await(endpoint, timeout_ms)) {
            .replied => |reply_len| {
                replies.held = true;
                return replies.reply(reply_len);
            },
            .abandoned => {
                replies.consume();
                return error.Abandoned;
            },
            .corrupt => {
                replies.consume();
                return error.Invalid;
            },
        }
    }

    /// `fipc_recv_release`, and the close's: the reply held in place is consumed. Nothing without one.
    pub fn release(replies: *Replies) void {
        if (!replies.held) return;
        replies.consume();
    }

    /// The answer of request `next` is consumed: `next` moves on (`.release`), and the sending thread is woken if it
    /// sleeps for room, which the request held (inexact: a relaxed look, as a taker's done has at a queue).
    fn consume(replies: *Replies) void {
        replies.held = false;
        replies.consumeTo(replies.next.load(.monotonic) + 1);
    }

    /// The answers below request `index` are consumed: `next` moves to it (`.release`, after the reads of their
    /// replies), and the sending thread is woken as `consume` wakes it.
    fn consumeTo(replies: *Replies, index: u64) void {
        replies.next.store(index, .release);
        const flag = &replies.layout.ring(replies.ring).room_sleeps;
        if (flag.load(.monotonic) != 0 and flag.swap(0, .acq_rel) == 1) platform.ringWake(flag, replies.room_event);
    }

    /// The `len` bytes of request `next`'s reply: at the start of its room, which the sending thread's record gives.
    fn reply(replies: *const Replies, len: u32) []u8 {
        return replies.layout.bytes(replies.ring, replies.positions.lowOf(replies.next.load(.monotonic)), len);
    }

    /// Waits within `timeout_ms` for request `next`'s done, and reads its answer (the module's comment). Disconnected
    /// once a receive returned it.
    fn await(replies: *Replies, endpoint: *Endpoint, timeout_ms: c_int) DataError!Answer {
        if (replies.disconnected) return error.Disconnected;
        return replies.awaitDone(endpoint, timeout_ms) catch |err| {
            if (err == error.Disconnected) replies.disconnected = true;
            return err;
        };
    }

    fn awaitDone(replies: *Replies, endpoint: *Endpoint, timeout_ms: c_int) DataError!Answer {
        const index = replies.next.load(.monotonic);
        const state = &replies.layout.cell(replies.ring, index).state;
        if (endpoint.hot.phase.load(.acquire) != .connected and replies.layout.header.ended.load(.acquire) == 0) {
            return error.Disconnected; // its own failure
        }
        var deadline: Deadline = .init(timeout_ms);
        var spun = false;
        while (!answered(state.load(.acquire), index)) try replies.wait(endpoint, state, index, &deadline, &spun);
        return replies.answerOf(index);
    }

    /// The answer in request `index`'s cell, once it is done (its `.acquire` load read the server's done, after which
    /// the server's fields and reply bytes are visible). A reply is 1 to `max_message` bytes; anything else is a
    /// corrupt peer's.
    fn answerOf(replies: *const Replies, index: u64) Answer {
        const cell = replies.layout.cell(replies.ring, index);
        const outcome: wire_protocol.ServiceOutcome = @fromBackingInt(@intCast(cell.outcome.load(.monotonic)));
        return switch (outcome) {
            .replied => {
                const reply_len = cell.reply_len.load(.monotonic);
                if (reply_len == 0 or reply_len > replies.layout.maxPiece()) return .corrupt;
                return .{ .replied = reply_len };
            },
            .abandoned => .abandoned,
            .none, _ => .corrupt,
        };
    }

    /// Waits for request `index`'s done (`state`, its cell's), the module's comment. Each round, in this order: spin,
    /// unless the call doesn't wait (once per call); cancelled → Cancelled; the service's end → Disconnected, unless
    /// the state's sequentially consistent look after the mark finds it done; this client's own failure → Disconnected;
    /// the call's timeout 0 or past → Timeout; then announce the sleep, and sleep a slice at most.
    fn wait(replies: *Replies, endpoint: *Endpoint, state: *const Atomic(u64), index: u64, deadline: *Deadline, spun: *bool) DataError!void {
        if (deadline.timeout_ms != 0 and !spun.*) {
            spun.* = true;
            if (replies.spin(state, index)) return;
        }
        if (endpoint.hot.requests.load(.acquire).cancel) return error.Cancelled;
        try replies.open(endpoint, state, index);
        const sleep_ms = try wait_module.sleepMsWithin(endpoint.io, deadline, replies.longest_sleep_ms);
        const flag = &replies.layout.ring(replies.ring).reply_sleeps;
        // On every way out, a cancel through `Io` too; a server may have taken it already
        defer _ = flag.swap(0, .seq_cst);
        if (replies.announceSleep(endpoint, state, index) == .sleep) {
            if (build_options.test_hooks) _ = replies.sleeps.fetchAdd(1, .seq_cst);
            _ = platform.ringWait(flag, replies.reply_event, sleep_ms);
            try wait_module.backInIo(endpoint.io);
        }
    }

    /// Disconnected once the service has ended and request `index` has no answer (`.seq_cst`: the mark, then the
    /// state, the receive's half of the end's pair), and after this client's own failure (its phase without the mark,
    /// which its watcher stores before the phase).
    fn open(replies: *const Replies, endpoint: *const Endpoint, state: *const Atomic(u64), index: u64) DataError!void {
        if (replies.layout.header.ended.load(.seq_cst) != 0) {
            if (answered(state.load(.seq_cst), index)) return;
            return error.Disconnected;
        }
        if (endpoint.hot.phase.load(.acquire) != .connected) return error.Disconnected;
    }

    /// The receive's side of its sleep: swaps `reply_sleeps` to 1, then looks at the state, its requests, its phase
    /// and `ended`, all sequentially consistent. `.sleep` only if none changed; the caller swaps the flag back.
    pub fn announceSleep(replies: *const Replies, endpoint: *const Endpoint, state: *const Atomic(u64), index: u64) enum { sleep, look } {
        _ = replies.layout.ring(replies.ring).reply_sleeps.swap(1, .seq_cst);
        if (answered(state.load(.seq_cst), index) or endpoint.hot.requests.load(.seq_cst).cancel or
            endpoint.hot.phase.load(.seq_cst) != .connected or replies.layout.header.ended.load(.seq_cst) != 0) return .look;
        return .sleep;
    }

    /// The spin before a sleep: an answer is waited for, so it looks after every hint. True once request `index` is
    /// done.
    fn spin(replies: *const Replies, state: *const Atomic(u64), index: u64) bool {
        for (0..replies.spin_iters) |_| {
            if (answered(state.load(.monotonic), index)) return true;
            std.atomic.spinLoopHint();
        }
        return false;
    }
};

/// Whether `state` is request `index`'s done: its answer is in the cell.
inline fn answered(state: u64, index: u64) bool {
    return ring_set.ofMessage(state, index) and ring_set.phaseOf(state) == .done;
}
