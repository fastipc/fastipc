//! A ring set: the rings, the cells and the bitmaps of a segment that many senders publish into and many takers
//! claim from, each message by one taker (a work queue's, queue/segment.zig: its producers send, its consumers take;
//! a service's, service/segment.zig: its clients send requests, its servers take them). Each sender has a ring, and a
//! kind may have rings past the senders' (a queue's return ring, whose only sender is the owner); each taker has a
//! slot. `RingSet` makes the set's code for a kind's wire types (its `Shape`): `Layout`, where the parts are in one
//! process's mapping, which the senders (core/sender.zig), the takers (core/taker.zig) and the owner work on; the
//! wakes; and the owner's looks at the words of a member that ended.
//!
//! Messages. Message `i` of ring `r` has one cell of ring `r`, the same at every lap (`cellSlot`: cell
//! `i % cell_count`, or where the kind spreads its cells, consecutive messages on lines of their own), whose state word
//! names the message and its phase: published, claimed by one taker, done (`published`, `claimed`, `done`). The state
//! names the full index, which is unique for the ring's lifetime (`wire_protocol.queue_max_index`), so a
//! compare-and-swap that expects message `h`'s state can never succeed on another message's. A cell holds only words of
//! the library, never payload: a payload lies in the ring's data ring, at the position its cell names, and never
//! wraps.
//!
//! Wakes. A taker parks on its slot's `sleeps` and a sender waiting for room on its ring's `room_sleeps`: each word
//! has one sleeper, so waking one wakes the right process. On Linux and macOS the word is the futex or `os_sync` word
//! itself; Windows has no cross-process wait on an address, so each slot of each role has a named auto-reset event
//! (`names.slotObject`, the kind's suffixes), which the owner creates and a member opens: its own to wait on, the
//! others' to wake them (`Wakes`). `ready` (bit r: ring r may have a message) and `idle` (bit t: taker t parks) pair up
//! as two Dekker handshakes, every access sequentially consistent: a publish onto a ring that looked empty sets its bit
//! and wakes one idle taker (`wakeOneIdle`), and a taker parks only after it set its idle bit and then saw no ready bit
//! (core/taker.zig).
//!
//! Replies. Where the kind's messages carry replies (`Shape.replies`: a service's requests), each message's sender
//! reserves room for its reply in its own data ring, just before the payload (`Layout.replyRoom`: the most bytes of a
//! piece, rounded up to 64), so a taker never waits for room. The taker that claimed a message answers it (`answer`):
//! the reply's length and the outcome in the cell, then the done, then the wake of the sender's receiving thread,
//! which waits for the done on its ring's `reply_sleeps`. A sender reuses a message's room only once its own receiving
//! thread has consumed the answer (core/sender.zig `reclaim`).
//!
//! A corrupt peer can write anything in the set: every member writes the cells of every ring. Whatever is read from
//! it and indexes or sizes anything is checked where it is read.

const std = @import("std");
const build_options = @import("build_options");
const wire_protocol = @import("../wire_protocol.zig");
const names = @import("names.zig");
const platform = @import("../platform.zig");
const test_hooks = @import("test_hooks.zig");

const Session = @import("shared_memory.zig").Session;

const page = 4096;

/// A message a taker claimed, or an ended taker holds (`heldClaim`): its ring and index, and its payload's position
/// and length, read after the claim.
pub const Claim = struct { ring: u32, index: u64, position: u64, len: u32 };

/// The ring set of a kind whose segment holds the words of `Shape`, a namespace of:
///   - `Header` (with `ended`, `ready` and `idle`: wire_protocol.QueueHeader's), `TakerSlot` (`sleeps`, `claim`:
///     QueueConsumerSlot's), `RingControl` (`tail`, `head`, `room_sleeps`: QueueRingControl's; with `replies`, also
///     `reply_sleeps`) and `Cell` (`state`, `payload_position`, `len`: QueueCell's; with `replies`, also `reply_len`
///     and `outcome`), the kind's wire types; `clearExtra(cell)`, a publish's store of the cell's other fields;
///   - `Role`, `sender_role` and `taker_role`: the members' roles, as JOIN and WELCOME carry them;
///   - `max_senders` and `max_takers`, and `extra_rings`: the rings past the senders' (64 rings in all at most);
///   - `piece_divisor`: a payload is at most the capacity divided by it; null where the owner chooses the most
///     (`Layout.chosen`: a service's max_message);
///   - `replies`: whether each message carries its reply (the module's comment);
///   - `cell_spread`: how far apart consecutive messages' cells are (`cellSlot`);
///   - optionally `showJoined(header, senders, takers)` and `joinedShown(header)`, where the header shows the members
///     joined (a service's: its servers' spin reads them, core/taker.zig `spinMode`);
///   - `sender_wake_suffix` and `taker_wake_suffix`: the kind suffixes of the slots' wakes (Windows), the ones the
///     takers set; and `room_wake_suffix`, where a sender's wait for room has a wake of its own besides (a service's
///     client: the takers wake its wait for a reply, and its own receiving thread its wait for room), else null;
///   - `noun`, the kind's name in the log, and `published_step` and `claimed_step`, the crash steps right after a
///     publish's `tail` and a claim's compare-and-swap (core/test_hooks.zig).
pub fn RingSet(comptime Shape: type) type {
    comptime {
        std.debug.assert(Shape.max_senders + Shape.extra_rings <= 64 and Shape.max_takers <= 64);
        std.debug.assert(Shape.sender_wake_suffix.len <= names.slot_suffix_max and Shape.taker_wake_suffix.len <= names.slot_suffix_max);
        if (Shape.room_wake_suffix) |suffix| std.debug.assert(suffix.len <= names.slot_suffix_max);
        // Replies take room in the senders' rings, which the owner's rings past them don't give
        std.debug.assert(!Shape.replies or Shape.extra_rings == 0);
    }
    return struct {
        pub const Header = Shape.Header;
        pub const TakerSlot = Shape.TakerSlot;
        pub const RingControl = Shape.RingControl;
        pub const Cell = Shape.Cell;
        pub const clearExtra = Shape.clearExtra;
        pub const noun = Shape.noun;
        pub const published_step = Shape.published_step;
        pub const claimed_step = Shape.claimed_step;
        pub const replies = Shape.replies;
        pub const room_wake_suffix = Shape.room_wake_suffix;
        /// The owner chooses the most bytes of a payload (`Layout.chosen`).
        pub const chosen_piece = Shape.piece_divisor == null;

        /// The most rings: the senders', then those past them.
        pub const max_rings = Shape.max_senders + Shape.extra_rings;

        // === The parts ===

        /// The parts of a ring set's segment for rings of `capacity` bytes, `max_senders` senders and `max_takers`
        /// takers: the header and a slot per taker, then from the next page each ring's region, the senders' and then
        /// the rings past theirs: its control block, its cells (one per 64 bytes of data), and from the next page its
        /// data ring. Every region is whole pages.
        pub const Regions = struct {
            cell_count: u32,
            takers_offset: u64,
            rings_offset: u64,
            ring_stride: u64,
            /// Within a ring's region.
            cells_offset: u64,
            data_offset: u64,
            segment_size: u64,
        };

        pub fn regions(capacity: u32, max_senders: u32, max_takers: u32) Regions {
            const cell_count: u32 = @intCast(capacity / wire_protocol.queue_payload_align);
            const takers_offset: u64 = @sizeOf(Header);
            const rings_offset = std.mem.alignForward(u64, takers_offset + @as(u64, max_takers) * @sizeOf(TakerSlot), page);
            const cells_offset: u64 = @sizeOf(RingControl);
            const data_offset = std.mem.alignForward(u64, cells_offset + @as(u64, cell_count) * @sizeOf(Cell), page);
            const ring_stride = data_offset + capacity;
            return .{
                .cell_count = cell_count,
                .takers_offset = takers_offset,
                .rings_offset = rings_offset,
                .ring_stride = ring_stride,
                .cells_offset = cells_offset,
                .data_offset = data_offset,
                .segment_size = rings_offset + (@as(u64, max_senders) + Shape.extra_rings) * ring_stride,
            };
        }

        /// The most bytes of a payload in rings of `capacity` bytes: the capacity divided by `piece_divisor` (a
        /// queue's `fipc_max_piece`, a quarter of it).
        pub fn maxPieceOf(capacity: u64) usize {
            return @intCast(capacity / Shape.piece_divisor.?);
        }

        // === The slots ===

        /// A member's role, its wire value.
        pub const Role = Shape.Role;
        pub const sender_role: Role = Shape.sender_role;
        pub const taker_role: Role = Shape.taker_role;

        /// A member's slot: its role, and its index among that role's slots (a sender's is its ring's number).
        pub const Slot = struct { role: Role, index: u32 };

        /// A set of slots of one role: bit s, slot s.
        pub const Slots = std.bit_set.Integer(64);

        /// The kind suffix of the wake of a slot of `role` (Windows).
        pub fn wakeSuffix(role: Role) []const u8 {
            return if (role == sender_role) Shape.sender_wake_suffix else Shape.taker_wake_suffix;
        }

        // === The mapping ===

        /// Where a ring set's parts are in this process's mapping, and its sizes: this process's own copies, never
        /// read again from the segment's description. Ring and taker numbers passed in are below their maximum:
        /// callers check what they read from the segment.
        pub const Layout = struct {
            base: [*]u8,
            header: *Header,
            /// Bytes of each ring's data ring: a power of two.
            capacity: u64,
            max_senders: u32,
            max_takers: u32,
            rings_offset: u64,
            ring_stride: u64,
            data_offset: u64,
            /// Where the owner chooses it (`chosen_piece`): the most bytes of a payload.
            max_piece: if (chosen_piece) u32 else void,

            pub fn init(base: [*]u8, capacity: u32, max_senders: u32, max_takers: u32) Layout {
                comptime std.debug.assert(!chosen_piece);
                return initParts(base, capacity, max_senders, max_takers, {});
            }

            /// The layout of a set whose owner chose `max_piece`, the most bytes of a payload (`chosen_piece`).
            pub fn chosen(base: [*]u8, capacity: u32, max_senders: u32, max_takers: u32, max_piece: u32) Layout {
                comptime std.debug.assert(chosen_piece);
                return initParts(base, capacity, max_senders, max_takers, max_piece);
            }

            fn initParts(base: [*]u8, capacity: u32, max_senders: u32, max_takers: u32, max_piece: @FieldType(Layout, "max_piece")) Layout {
                std.debug.assert(std.math.isPowerOfTwo(capacity));
                const sizes = regions(capacity, max_senders, max_takers);
                return .{
                    .base = base,
                    .header = @ptrCast(@alignCast(base)),
                    .capacity = capacity,
                    .max_senders = max_senders,
                    .max_takers = max_takers,
                    .rings_offset = sizes.rings_offset,
                    .ring_stride = sizes.ring_stride,
                    .data_offset = sizes.data_offset,
                    .max_piece = max_piece,
                };
            }

            /// The layout of a session's ring set, the owner's or a member's.
            pub fn of(session: *const Session, max_senders: u32, max_takers: u32) Layout {
                return .init(session.mapping.base, session.capacity, max_senders, max_takers);
            }

            pub fn cellCount(layout: *const Layout) u64 {
                return layout.capacity / wire_protocol.queue_payload_align;
            }

            pub fn maxPiece(layout: *const Layout) usize {
                return if (chosen_piece) layout.max_piece else maxPieceOf(layout.capacity);
            }

            /// Where messages carry replies: the room each reserves for its reply, just before its payload, the most
            /// bytes of a piece rounded up to 64; 0 elsewhere.
            pub inline fn replyRoom(layout: *const Layout) u64 {
                if (!replies) return 0;
                return std.mem.alignForward(u64, layout.maxPiece(), wire_protocol.queue_payload_align);
            }

            /// The rings: the senders', then those past them.
            pub fn ringCount(layout: *const Layout) u32 {
                return layout.max_senders + Shape.extra_rings;
            }

            /// The bits of `ready` that name rings.
            pub fn ringMask(layout: *const Layout) u64 {
                return ~@as(u64, 0) >> @intCast(64 - layout.ringCount());
            }

            /// The bits of `idle` that name takers.
            pub fn takerMask(layout: *const Layout) u64 {
                return ~@as(u64, 0) >> @intCast(64 - layout.max_takers);
            }

            pub fn takerSlot(layout: *const Layout, taker: u32) *TakerSlot {
                std.debug.assert(taker < layout.max_takers);
                return @ptrCast(@alignCast(layout.base + @sizeOf(Header) + @as(u64, taker) * @sizeOf(TakerSlot)));
            }

            /// Ring `number`'s control block.
            pub fn ring(layout: *const Layout, number: u32) *RingControl {
                return @ptrCast(@alignCast(layout.region(number)));
            }

            /// Message `index`'s cell in ring `number`.
            pub fn cell(layout: *const Layout, number: u32, index: u64) *Cell {
                const cells: [*]Cell = @ptrCast(@alignCast(layout.region(number) + @sizeOf(RingControl)));
                return &cells[cellSlot(index, layout.cellCount(), Shape.cell_spread)];
            }

            /// Whether a cell's payload fields describe a payload of the set: 1 to `maxPiece` bytes, within the ring,
            /// and where messages carry replies, its reply's room before it within the ring too (so a taker's reply
            /// lands in the sender's own data ring, whatever a corrupt sender wrote).
            pub fn validPiece(layout: *const Layout, payload_position: u64, len: u32) bool {
                const at = layout.offset(payload_position);
                return len != 0 and len <= layout.maxPiece() and at + len <= layout.capacity and at >= layout.replyRoom();
            }

            /// The `len` bytes of ring `number`'s data ring at position `payload_position` (checked by `validPiece`).
            pub fn bytes(layout: *const Layout, number: u32, payload_position: u64, len: usize) []u8 {
                return (layout.region(number) + layout.data_offset)[@intCast(layout.offset(payload_position))..][0..len];
            }

            /// The offset in a data ring of position `payload_position`: the position modulo the capacity, a power of
            /// two.
            pub inline fn offset(layout: *const Layout, payload_position: u64) u64 {
                return payload_position & (layout.capacity - 1);
            }

            fn region(layout: *const Layout, number: u32) [*]u8 {
                std.debug.assert(number < layout.ringCount());
                return layout.base + layout.rings_offset + @as(u64, number) * layout.ring_stride;
            }
        };

        // === Wakes ===

        /// A member's handles to the other slots' wakes (Windows), opened at a slot's first wake and kept until the
        /// member's close: a cached handle keeps its event alive, so setting it is harmless even after the owner's
        /// teardown. Elsewhere it holds nothing: the flag is the word that is waited on. One thread uses it: the
        /// member's sending or receiving thread (the owner: its poller).
        pub const Wakes = struct {
            session_id: [16]u8,
            senders: [Shape.max_senders]?platform.SlotWake = @splat(null),
            takers: [Shape.max_takers]?platform.SlotWake = @splat(null),

            pub fn init(session_id: [16]u8) Wakes {
                return .{ .session_id = session_id };
            }

            pub fn deinit(wakes: *Wakes) void {
                if (!platform.wakes_each_slot) return;
                for (&wakes.senders) |*wake| if (wake.*) |handle| platform.closeSlotWake(handle);
                for (&wakes.takers) |*wake| if (wake.*) |handle| platform.closeSlotWake(handle);
                wakes.* = undefined;
            }

            pub fn sender(wakes: *Wakes, slot: u32) platform.RingEvent {
                return wakes.event(&wakes.senders[slot], Shape.sender_wake_suffix, slot);
            }

            pub fn taker(wakes: *Wakes, slot: u32) platform.RingEvent {
                return wakes.event(&wakes.takers[slot], Shape.taker_wake_suffix, slot);
            }

            /// The slot's event, opened at its first wake. One that can't be opened (the kind ended, or a corrupt idle
            /// bit named a slot never claimed) isn't cached: that wake is skipped, and its sleeper looks again within
            /// its slice.
            fn event(wakes: *Wakes, cached: *?platform.SlotWake, suffix: []const u8, slot: u32) platform.RingEvent {
                if (!platform.wakes_each_slot) return platform.no_ring_event;
                if (cached.* == null) cached.* = platform.openSlotWake(&wakes.session_id, suffix, slot) catch return platform.no_ring_event;
                return platform.slotRingEvent(cached.*);
            }
        };

        /// Wakes one parked taker, if any announced its park. It takes an idle bit, searching from the taker that
        /// `from_ring` maps to (so that the wakes of different rings spread over the takers), and wakes that taker. A
        /// bit another waker took first is skipped.
        pub fn wakeOneIdle(layout: *const Layout, wakes: *Wakes, from_ring: u32) void {
            const header = layout.header;
            var candidates = header.idle.load(.seq_cst) & layout.takerMask();
            if (candidates == 0) return; // the usual case on a busy set, which then pays no divide for `home`
            const home: u32 = @intCast(@as(u64, from_ring) * layout.max_takers / layout.ringCount());
            while (candidates != 0) {
                const taker = firstSetBitFrom(candidates, home);
                const bit = bitOf(taker);
                if (header.idle.fetchAnd(~bit, .seq_cst) & bit != 0) return wakeTaker(layout, wakes, taker);
                candidates &= ~bit;
            }
        }

        /// Wakes taker `taker` if it parks: whoever swaps its flag's 1 back to 0 wakes it; 0, it is awake, and looks
        /// again before any park.
        pub fn wakeTaker(layout: *const Layout, wakes: *Wakes, taker: u32) void {
            const flag = &layout.takerSlot(taker).sleeps;
            if (flag.swap(0, .seq_cst) == 1) platform.ringWake(flag, wakes.taker(taker));
        }

        /// Wakes the sender of ring `sender` if it sleeps for room. A ring past the senders' has none that sleeps on
        /// its flag (a queue's return ring: its sender, the owner, never waits): one set there is a corrupt peer's,
        /// and left. Where messages carry replies, only the sender's own receiving thread wakes its wait for room
        /// (service/caller.zig), and the takers' wake is the reply's (`answer`).
        pub fn wakeSender(layout: *const Layout, wakes: *Wakes, sender: u32) void {
            comptime std.debug.assert(!replies);
            if (sender >= layout.max_senders) return;
            const flag = &layout.ring(sender).room_sleeps;
            if (flag.swap(0, .acq_rel) == 1) platform.ringWake(flag, wakes.sender(sender));
        }

        /// The owner's, after any member's end: every ring with a message to take gets its ready bit, and a parked
        /// taker is woken if any ring has one (a member that ended between a store and its Dekker look may have left
        /// neither).
        pub fn rescan(layout: *const Layout, wakes: *Wakes) void {
            const header = layout.header;
            for (0..layout.ringCount()) |number| {
                const ring = layout.ring(@intCast(number));
                const tail = ring.tail.load(.seq_cst);
                if (claimable(ring.head.load(.seq_cst), tail, layout.cellCount())) _ = header.ready.fetchOr(bitOf(@intCast(number)), .seq_cst);
            }
            if (header.ready.load(.seq_cst) & layout.ringMask() != 0) wakeOneIdle(layout, wakes, 0);
        }

        /// The members joined, as the owner shows them in the header (`showJoined`).
        pub const Joined = struct { senders: u32, takers: u32 };

        /// The owner's, under its lock, at each change of its counts: the members joined of each role, in the header
        /// where the kind shows them. Nothing elsewhere.
        pub fn showJoined(layout: *const Layout, senders: u32, takers: u32) void {
            if (@hasDecl(Shape, "showJoined")) Shape.showJoined(layout.header, senders, takers);
        }

        /// The members joined, as the owner showed them last (relaxed: a hint, which a corrupt peer can write too).
        pub fn joinedShown(layout: *const Layout) Joined {
            const joined = Shape.joinedShown(layout.header);
            return .{ .senders = joined[0], .takers = joined[1] };
        }

        pub fn clearIdle(layout: *const Layout, taker: u32) void {
            _ = layout.header.idle.fetchAnd(~bitOf(taker), .seq_cst);
        }

        // === Replies (`replies`) ===

        /// Taker `taker` answers message `claim`, which it claimed: `outcome` and the reply's length `reply_len` (its
        /// bytes are in the reply room already) in the cell, then the done (`.seq_cst`: with its sender's sequentially
        /// consistent look at the state after its swap of `reply_sleeps` to 1, the reply's exact wake pair,
        /// `wakeReply`; with the caller's load of `ended` after it, the end's pair), then the wake of its sender's wait
        /// for the reply. After the done the taker touches nothing of the message: its sender may reuse the room once
        /// it consumed the answer.
        pub fn answer(layout: *const Layout, wakes: *Wakes, taker: u32, claim: Claim, outcome: wire_protocol.ServiceOutcome, reply_len: u32) void {
            comptime std.debug.assert(replies);
            const cell = layout.cell(claim.ring, claim.index);
            cell.reply_len.store(reply_len, .monotonic);
            cell.outcome.store(@backingInt(outcome), .monotonic);
            if (build_options.test_hooks and outcome == .replied) test_hooks.crashIfAt(Shape.replying_step);
            cell.state.store(done(claim.index, taker), .seq_cst);
            if (build_options.test_hooks and outcome == .replied) test_hooks.crashIfAt(Shape.replied_step);
            wakeReply(layout, wakes, claim.ring);
        }

        /// Wakes the receiving thread of ring `sender`'s sender if it waits for a reply: whoever swaps its flag's 1
        /// back to 0 wakes it; 0, it is awake, and looks at the state before any sleep. The swap follows only a load
        /// that read 1: a sequentially consistent load after the caller's sequentially consistent done is the waker's
        /// half of the store-buffering pair, as the swap is (either it reads the sleeper's 1, or the sleeper's look
        /// after its swap to 1 sees the done), and it leaves the flag's line shared while the client doesn't sleep.
        pub fn wakeReply(layout: *const Layout, wakes: *Wakes, sender: u32) void {
            comptime std.debug.assert(replies);
            const flag = &layout.ring(sender).reply_sleeps;
            if (flag.load(.seq_cst) == 1 and flag.swap(0, .seq_cst) == 1) platform.ringWake(flag, wakes.sender(sender));
        }

        /// The owner's, once taker slot `taker`'s member ended: wakes the receiving thread of the sender whose message its
        /// intent names, if it waits for a reply. A taker that ended between its done and its wake (`answer`) left that
        /// thread asleep, until its slice ended; a wake with nothing new only makes it look and sleep again.
        pub fn wakeIntended(layout: *const Layout, wakes: *Wakes, taker: u32) void {
            comptime std.debug.assert(replies);
            const named = intentOf(layout.takerSlot(taker).claim.load(.acquire), layout.ringCount()) orelse return;
            wakeReply(layout, wakes, named.ring);
        }

        /// The owner's, once taker slot `taker`'s member ended holding message `claim` (`heldClaim`) without answering
        /// it: the message is answered as abandoned (no reply; the fields before the compare-and-swap, which its
        /// sender's acquire of the done reads), `head` moved past it, and its sender woken. False if its cell no longer
        /// held the claim: the taker stored its done just before it ended, and its reply stands (or a corrupt peer
        /// wrote the cell). The fields go before a compare-and-swap that may fail: once `heldClaim` saw the claim, only
        /// a corrupt peer changes the cell (the ended taker writes nothing more, and no other taker claims a claimed
        /// message), and such a peer could have written the fields itself.
        pub fn abandonHeld(layout: *const Layout, wakes: *Wakes, taker: u32, claim: Claim) bool {
            comptime std.debug.assert(replies);
            const cell = layout.cell(claim.ring, claim.index);
            cell.reply_len.store(0, .monotonic);
            cell.outcome.store(@backingInt(wire_protocol.ServiceOutcome.abandoned), .monotonic);
            if (!finishHeld(layout, taker, claim)) return false;
            wakeReply(layout, wakes, claim.ring);
            return true;
        }

        /// The owner's, once sender slot `sender`'s member ended (it writes the segment no more, and waits for no
        /// answer): the messages of its ring that no taker claimed are dropped, each made done (as if by taker 0)
        /// before `head` moves past it, by the takers' own steps: a compare-and-swap from published, then one of
        /// `head`. A message found claimed or done is helped on, as a take does; a cell at `head` that is none of its
        /// message's states is a corrupt peer's and skipped. Taken messages go on: their takers answer them into the
        /// ring, and the slot drains until they did (`ringSettled`). A ring whose indices are a corrupt peer's is left
        /// to `ringSettled`, which retires its slot. The drops it made.
        pub fn dropUnclaimed(layout: *const Layout, sender: u32) u64 {
            comptime std.debug.assert(replies);
            const control = layout.ring(sender);
            var dropped: u64 = 0;
            // At most a ring of cells, and a round more for each one another taker moved meanwhile
            for (0..2 * layout.cellCount()) |_| {
                const tail = control.tail.load(.acquire);
                const head = control.head.load(.acquire);
                if (!claimable(head, tail, layout.cellCount())) break;
                const state = &layout.cell(sender, head).state;
                const seen = state.load(.acquire);
                if (seen == published(head)) {
                    if (state.cmpxchgStrong(published(head), done(head, 0), .acq_rel, .monotonic) == null) dropped += 1;
                    continue; // `head` moves on at the next look, as for a claimed one
                }
                _ = control.head.cmpxchgStrong(head, head + 1, .release, .monotonic);
            }
            return dropped;
        }

        // === The owner's looks at an ended member's words ===

        /// The owner's, once taker slot `taker`'s member ended (it writes nothing more): the message it holds, the one
        /// its intent names if that message's cell still holds its claim, with the payload's fields (unchecked); else
        /// null. Only a claim the ended taker made can match: the state names the claimer and the full index, so no
        /// live taker's claim is taken for its. The `.acquire` load of the state reads the claimer's compare-and-swap,
        /// which continues the release sequence of the sender's `published` store: the payload's fields and bytes are
        /// visible after it.
        pub fn heldClaim(layout: *const Layout, taker: u32) ?Claim {
            const named = intentOf(layout.takerSlot(taker).claim.load(.acquire), layout.ringCount()) orelse return null;
            const cell = layout.cell(named.ring, named.index);
            if (cell.state.load(.acquire) != claimed(named.index, taker)) return null;
            return .{ .ring = named.ring, .index = named.index, .position = cell.payload_position.load(.monotonic), .len = cell.len.load(.monotonic) };
        }

        /// The owner's, once an ended taker's held message (`heldClaim`) is copied out or dropped: the message is done
        /// (`.seq_cst`: the owner's reads of its payload come first), and `head` moved past it, in case the taker
        /// ended before it moved it. False if its cell no longer held the claim: a corrupt peer's write.
        pub fn finishHeld(layout: *const Layout, taker: u32, claim: Claim) bool {
            const state = &layout.cell(claim.ring, claim.index).state;
            if (state.cmpxchgStrong(claimed(claim.index, taker), done(claim.index, taker), .seq_cst, .monotonic) != null) return false;
            _ = layout.ring(claim.ring).head.cmpxchgStrong(claim.index, claim.index + 1, .seq_cst, .monotonic);
            return true;
        }

        /// Whether sender slot `sender`'s ring may be given to a new sender (the owner's, while no sender holds the
        /// slot): `settled` once every message published was claimed and no taker holds one of them; `busy` before;
        /// `corrupt` for a `tail` past the index bound or a `head` past `tail` (the slot is never given out again).
        ///
        /// Exact: with no sender, `head == tail` means every message below `tail` was claimed, and each claim happened
        /// before the `head` move past it, which this `.acquire` load reads; the claimer's intent was stored
        /// (`.release`) before its claim. A taker changes its intent only after the done of the message it named, so
        /// its intent (`.acquire`) shows either the claim of a message it still holds (its cell `claimed`) or a later
        /// intent, after which that message's done is visible.
        pub fn ringSettled(layout: *const Layout, sender: u32) enum { settled, busy, corrupt } {
            const control = layout.ring(sender);
            const head = control.head.load(.acquire);
            const tail = control.tail.load(.acquire);
            if (tail >= wire_protocol.queue_max_index or head > tail) return .corrupt;
            if (head != tail) return .busy;
            for (0..layout.max_takers) |taker| {
                const named = intentOf(layout.takerSlot(@intCast(taker)).claim.load(.acquire), layout.ringCount()) orelse continue;
                if (named.ring != sender) continue;
                if (layout.cell(sender, named.index).state.load(.acquire) == claimed(named.index, @intCast(taker))) return .busy;
            }
            return .settled;
        }
    };
}

// === The cell states ===

// The words live in `std.atomic.Value(u64)`s and are compared whole by compare-and-swap: these build and read them as
// integers (`wire_protocol.QueueCellState`, `wire_protocol.QueueIntent`).

/// Message `index`'s tag: index + 1, so that 0 is no message. Indices stay below `wire_protocol.queue_max_index`, so
/// the tag fits its 56 bits.
fn tag(index: u64) u56 {
    return @truncate(index + 1);
}

fn cellState(phase: Phase, index: u64, taker: u32) u64 {
    return @bitCast(wire_protocol.QueueCellState{ .phase = phase, .claimer = @intCast(taker), .tag = tag(index) });
}

pub fn published(index: u64) u64 {
    return cellState(.published, index, 0);
}

pub fn claimed(index: u64, taker: u32) u64 {
    return cellState(.claimed, index, taker);
}

pub fn done(index: u64, taker: u32) u64 {
    return cellState(.done, index, taker);
}

/// Whether `state` is a state of message `index`.
pub inline fn ofMessage(state: u64, index: u64) bool {
    return @as(wire_protocol.QueueCellState, @bitCast(state)).tag == index + 1;
}

pub const Phase = wire_protocol.QueuePhase;

pub inline fn phaseOf(state: u64) Phase {
    return @as(wire_protocol.QueueCellState, @bitCast(state)).phase;
}

/// A taker's intent word (its slot's `claim`): the message of ring `ring` it is about to claim.
pub inline fn intent(ring: u32, index: u64) u64 {
    return @bitCast(wire_protocol.QueueIntent{ .ring_plus_one = @intCast(ring + 1), .tag = tag(index) });
}

/// The message an intent word names.
pub const Intent = struct { ring: u32, index: u64 };

/// The message intent word `word` names; null for none (0), and for a ring at or past `ring_count`, which only a
/// corrupt peer writes.
pub fn intentOf(word: u64, ring_count: u32) ?Intent {
    const named: wire_protocol.QueueIntent = @bitCast(word);
    if (named.ring_plus_one == 0 or named.ring_plus_one > ring_count or named.tag == 0) return null;
    return .{ .ring = named.ring_plus_one - 1, .index = named.tag - 1 };
}

/// Whether a ring's `head` and `tail` could be a live ring's: below the index bound, and at most a ring of cells
/// apart. A reader that loads `tail` before `head` always sees a valid pair (core/taker.zig); any other is a corrupt
/// peer's.
pub fn validRing(head: u64, tail: u64, cell_count: u64) bool {
    return tail < wire_protocol.queue_max_index and head < wire_protocol.queue_max_index and (head >= tail or tail - head <= cell_count);
}

/// Whether a valid ring has a message not yet claimed.
pub fn claimable(head: u64, tail: u64, cell_count: u64) bool {
    return validRing(head, tail, cell_count) and head < tail;
}

/// The slot of message `index`'s cell among a ring's `count` cells (a power of two, at least 64): the index's low
/// `log2(count)` bits rotated left by `spread` (a kind's `Shape.cell_spread`), so message `i` is at slot
/// `(i << spread) % count + (i % count) / (count >> spread)`. 0 is `i % count`.
pub inline fn cellSlot(index: u64, count: u64, comptime spread: u6) usize {
    const low = index & (count - 1);
    if (spread == 0) return @intCast(low);
    const shift: u6 = @intCast(@ctz(count) - spread);
    return @intCast(((low << spread) & (count - 1)) | (low >> shift));
}

/// Bit `number` of `ready` (ring `number`) or of `idle` (taker `number`).
pub inline fn bitOf(number: u32) u64 {
    return @as(u64, 1) << @intCast(number);
}

/// The first set bit of `bits` (not 0) from bit `from` on, round to bit 0.
pub fn firstSetBitFrom(bits: u64, from: u32) u32 {
    const high = bits & (~@as(u64, 0) << @intCast(from));
    return if (high != 0) @ctz(high) else @ctz(bits);
}

// === Tests (more on a queue's segment: queue/segment.zig, queue/producer_consumer_test.zig, queue/litmus_test.zig) ===

const testing = std.testing;

test "cell slots: each lap's messages take every cell once; spread by 2, consecutive ones are 128 bytes apart" {
    for ([_]u64{ 64, 128, 1 << 25 }) |count| {
        const checked = @min(count, 4096);
        for (0..checked) |i| {
            try testing.expectEqual(@as(usize, @intCast(i)), cellSlot(i, count, 0));
            try testing.expectEqual(cellSlot(i, count, 2), cellSlot(i + count, count, 2)); // the same at every lap
            // 32-byte cells: slots 4 apart are in different pairs of 64-byte lines
            const here = cellSlot(i, count, 2);
            const next = cellSlot(i + 1, count, 2);
            if ((i + 1) % (count / 4) != 0) try testing.expectEqual(here + 4, next);
            try testing.expect(here / 4 != next / 4);
        }
        if (count > 4096) continue;
        var seen = try testing.allocator.alloc(bool, count);
        defer testing.allocator.free(seen);
        @memset(seen, false);
        for (0..count) |i| {
            const slot = cellSlot(i, count, 2);
            try testing.expect(!seen[slot]);
            seen[slot] = true;
        }
    }
    try testing.expectEqual(@as(usize, 1), cellSlot(16, 64, 2)); // a quarter of the ring on: the next cell of slot 0's line
}

test "cell states: each names its message and phase, and no other message's or phase's word equals it" {
    const indices = [_]u64{ 0, 1, 63, 64, 1 << 32, wire_protocol.queue_max_index - 1 };
    for (indices) |index| {
        try testing.expectEqual(Phase.published, phaseOf(published(index)));
        try testing.expectEqual(Phase.claimed, phaseOf(claimed(index, 63)));
        try testing.expectEqual(Phase.done, phaseOf(done(index, 0)));
        try testing.expect(ofMessage(published(index), index) and ofMessage(claimed(index, 5), index) and ofMessage(done(index, 63), index));
        try testing.expect(published(index) != 0);
        for (indices) |other| {
            if (other == index) continue;
            try testing.expect(!ofMessage(published(index), other));
            try testing.expect(published(index) != published(other) and published(index) != claimed(other, 0));
        }
        // The claimer is part of the word: a CAS from another taker's claim fails
        try testing.expect(claimed(index, 1) != claimed(index, 2) and done(index, 1) != done(index, 2));
        try testing.expect(published(index) != claimed(index, 0) and claimed(index, 0) != done(index, 0));
    }
    // The last index fills the 56 tag bits
    try testing.expectEqual(@as(u64, std.math.maxInt(u64) >> 8), published(wire_protocol.queue_max_index - 1) >> 8);
    try testing.expectEqual(Phase.none, phaseOf(0));
    try testing.expect(!ofMessage(0, 0));

    try testing.expect(validRing(5, 5, 64) and validRing(5, 69, 64) and !validRing(5, 70, 64));
    try testing.expect(validRing(9, 5, 64)); // head past tail reads as empty
    try testing.expect(!validRing(0, wire_protocol.queue_max_index, 64) and !validRing(wire_protocol.queue_max_index, 0, 64));
    try testing.expect(claimable(5, 6, 64) and !claimable(6, 6, 64) and !claimable(5, 70, 64));

    try testing.expectEqual(@as(u32, 3), firstSetBitFrom(0b1001000, 0));
    try testing.expectEqual(@as(u32, 6), firstSetBitFrom(0b1001000, 4));
    try testing.expectEqual(@as(u32, 3), firstSetBitFrom(0b1001000, 7));
    try testing.expectEqual(@as(u32, 63), firstSetBitFrom(1 << 63, 63));
}
