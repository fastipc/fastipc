//! The protocol's wire formats (docs/protocol.md) as Zig `extern` types and constants: the bytes two builds of this
//! library share, the control frames, the shared-memory segments' headers and the rings' frames, and the values they
//! carry (the versions, the magics, the roles).
//!
//! The comptime block at the end checks every layout, in every build: they are private, but a drift would corrupt a
//! peer, so it fails the build. They are little-endian, the byte order of both supported architectures (x86-64 and
//! aarch64). c_api.zig checks the values that are also public (the roles) against include/fipc.h.

const std = @import("std");

const Atomic = std.atomic.Value;

// === The protocol's version (docs/protocol.md §8) ===

/// The version of a stream's protocol: its control frames' `version` and the digit of its segment's magic. Two builds
/// that differ in any of a stream's wire formats differ in it, and refuse each other.
pub const protocol_version: u8 = 1;

/// The segment's magic, the bytes "FIPCSEG" and the version's digit, in `SegmentHeader.magic`.
pub const segment_magic: u64 = std.mem.readInt(u64, "FIPCSEG" ++ .{'0' + protocol_version}, .little);

// === The control connection (docs/protocol.md §3.1) ===

/// The first 12 bytes of every control frame, of every kind (core/wire.zig). The magic, the type, the version and the
/// kind never move, so that each side tells a peer of another kind or protocol version from a corrupt one.
pub const ControlHeader = extern struct {
    /// "FIPC", the same in every protocol version.
    magic: u32,
    /// The sender's process id, for logs only.
    pid: u32,
    /// The type of the frame, one of its kind's: `StreamFrameType` or `ChannelFrameType`.
    frame_type: u8,
    /// Stream: `protocol_version`; broadcast, queue and service: `channel_protocol_version`.
    version: u8,
    kind: Kind,
    reserved: u8 = 0,
};

/// `ControlHeader.kind`: whose frame it is. Non-exhaustive: a peer may send any byte.
pub const Kind = enum(u8) { stream = 0, broadcast = 1, queue = 2, service = 3, _ };

/// A stream's `ControlHeader.frame_type`.
pub const StreamFrameType = enum(u8) { segment = 1, ready = 2, _ };

/// SEGMENT: the server's offer of the client's segment (stream/frames.zig).
pub const ControlStreamSegment = extern struct {
    header: ControlHeader,
    /// Bytes of each ring.
    capacity: u32,
    segment_size: u64,
    session_id: [16]u8,
    reserved: [24]u8 = @splat(0),
};

/// READY: the client's answer to SEGMENT, once it has mapped the segment.
pub const ControlStreamReady = extern struct {
    header: ControlHeader,
    reserved: [52]u8 = @splat(0),
};

// === Shared memory (docs/protocol.md §4, §5) ===

/// The writer's half of one ring's indices, on 128 bytes of its own: the index it publishes, and the sleep flag of the
/// reader that waits for it, so that a publish (store the index, swap the flag) touches one line. 128 bytes: the cache
/// line of Apple's cores, and on x86-64 the pair of 64-byte lines its spatial prefetcher fetches together.
pub const RingWriter = extern struct {
    /// The bytes the writer has published (a monotonic byte count).
    head: Atomic(u64),
    /// The reader's sleep flag: the reader sets it to 1 before it sleeps; whoever swaps the 1 back to 0 wakes it.
    reader_sleeps: Atomic(u32),
    pad: [116]u8,
};

/// The reader's half of one ring's indices, on 128 bytes of its own.
pub const RingReader = extern struct {
    /// The bytes the reader has consumed (a monotonic byte count).
    tail: Atomic(u64),
    /// The writer's sleep flag: the writer sets it to 1 before it sleeps; whoever swaps the 1 back to 0 wakes it.
    writer_sleeps: Atomic(u32),
    pad: [116]u8,
};

pub const RingIndices = extern struct {
    writer: RingWriter,
    reader: RingReader,
};

/// The first 640 bytes of a connection's segment: the magic, the ring capacity, the header's size and the session id
/// on the first 128 bytes, then the two rings' indices. The server writes it once, before it offers the segment; the client checks it before any
/// use and never writes it (stream/segment.zig).
pub const SegmentHeader = extern struct {
    magic: u64,
    /// Bytes of each ring.
    capacity: u32,
    header_size: u32,
    /// Random and not all zero; names the connection's objects on Windows.
    session_id: [16]u8,
    reserved: [96]u8,
    /// The server-to-client ring.
    s2c: RingIndices,
    /// The client-to-server ring.
    c2s: RingIndices,
};

/// The header of every frame in a ring, in front of a piece of a message (stream/ring.zig).
pub const FrameHeader = extern struct {
    flags: FrameFlags,
    /// The piece's length.
    frag_len: u32,
    /// A START piece's: the whole message's length; any other piece's and a PAD's: 0. A message of one piece whose
    /// frame is shorter than `stamped_frame_max` carries its stamp here instead, its position in the ring complemented
    /// (`~pos`), stored after its bytes (docs/protocol.md §5.2, §6.5). A length is below 2^63, a stamp above it.
    total_len: u64,
};

/// The frames that carry a stamp in `FrameHeader.total_len` are shorter than this: those of a message of one piece of
/// at most 224 bytes.
pub const stamped_frame_max: u64 = 256;

// === Broadcast channels (docs/protocol.md §9) ===

/// The version of the channel protocol, a broadcast's, a queue's and a service's: their control frames' `version` and
/// the digit of their segments' magics. A change to a channel's wire formats, or a new kind of channel, bumps it, and
/// leaves `protocol_version` alone.
pub const channel_protocol_version: u8 = 1;
/// The broadcast segment's magic, the bytes "FIPCPUB" and the channel protocol's digit, in `BroadcastInfo.magic`.
pub const broadcast_magic: u64 = std.mem.readInt(u64, "FIPCPUB" ++ .{'0' + channel_protocol_version}, .little);
/// A broadcast's, a queue's and a service's `ControlHeader.frame_type` (a queue's and a service's never UNSUBSCRIBE or
/// UNSUBSCRIBED). OFFER has SEGMENT's type on purpose: the first frame of each kind passes the others' type check, and
/// its `kind` refuses the name at once.
pub const ChannelFrameType = enum(u8) { offer = 1, join = 3, welcome = 4, unsubscribe = 5, unsubscribed = 6, _ };
/// Bit of OFFER's `flags` and of `BroadcastInfo.flags`: the channel takes gated subscribers.
pub const offer_flag_gated: u32 = 0x1;
/// A subscriber's role, as `fipc_broadcast_join` takes it (`FIPC_SUBSCRIBER_LOSSY`, `FIPC_SUBSCRIBER_GATED`) and a
/// broadcast's JOIN carries it (`ControlBroadcastJoin.role`).
pub const SubscriberRole = enum(u32) { lossy = 1, gated = 2 };
/// The most subscribers a channel has: one bit each in the gated bitmap's 16 words.
pub const max_channel_subscribers: u32 = 1024;
/// The smallest capacity of a channel's data ring.
pub const broadcast_min_capacity: usize = 4096;
/// A payload's room in the data ring is a multiple of this; also the bytes of data ring per cell.
pub const broadcast_payload_align: usize = 64;
/// The writer announces the data ring a sixteenth at a time (`BroadcastHeader.frontier`).
pub const broadcast_chunks: u64 = 16;
/// A channel's `fipc_max_piece` is its capacity divided by this.
pub const broadcast_piece_divisor: usize = 4;
pub const broadcast_header_size: u32 = 512;
/// The bit of `BroadcastCell.seq` that marks a cell the writer is writing.
pub const broadcast_busy: u64 = 1 << 63;
/// Indices stay below this, the busy bit: a channel would need centuries of messages to reach it.
pub const broadcast_max_index: u64 = 1 << 63;

/// OFFER: the owner's offer of the broadcast's segment, and of the joiner's slot in it (broadcast/frames.zig).
pub const ControlBroadcastOffer = extern struct {
    header: ControlHeader,
    /// Bytes of the data ring.
    capacity: u32,
    segment_size: u64,
    session_id: [16]u8,
    slot: u32,
    slot_generation: u32,
    max_subscribers: u32,
    /// `offer_flag_gated`.
    flags: u32,
    reserved: [8]u8 = @splat(0),
};

/// JOIN: the joiner's answer to OFFER, with its role, and the offer's slot and generation echoed.
pub const ControlBroadcastJoin = extern struct {
    header: ControlHeader,
    /// A `SubscriberRole`'s value: a corrupt peer may send any.
    role: u32,
    slot: u32,
    slot_generation: u32,
    reserved: [40]u8 = @splat(0),
};

/// WELCOME, UNSUBSCRIBE and UNSUBSCRIBED, told apart by their type: a subscriber's slot and the slot's generation.
pub const ControlBroadcastSlot = extern struct {
    header: ControlHeader,
    slot: u32,
    slot_generation: u32,
    reserved: [44]u8 = @splat(0),
};

/// The broadcast segment's fixed description, at its start. The owner writes it once, before any OFFER; a joiner
/// checks it before any use and never writes it (broadcast/segment.zig).
pub const BroadcastInfo = extern struct {
    magic: u64,
    /// Random and not all zero; names the channel's objects on Windows.
    session_id: [16]u8,
    cells_offset: u64,
    data_offset: u64,
    shared_offset: u64,
    slots_offset: u64,
    segment_size: u64,
    header_size: u32,
    /// Bytes of the data ring.
    capacity: u32,
    cell_count: u32,
    max_subscribers: u32,
    /// Bit 0 (`offer_flag_gated`): gated subscribers allowed.
    flags: u32,
    reserved: [44]u8,
};

/// The first 512 bytes of a broadcast segment: the description, then the writer's words on lines of their own.
pub const BroadcastHeader = extern struct {
    info: BroadcastInfo,
    /// The end of the data chunk the writer may be writing: it moves a chunk at a time, before the writer writes there.
    frontier: Atomic(u64),
    /// The writer's next index when it announced that chunk: where a lapped subscriber goes on.
    next_mark: Atomic(u64),
    pad1: [112]u8,
    /// The published next index: every message below it is complete. Stored once per send (a batch's: once).
    next: Atomic(u64),
    pad2: [120]u8,
    /// 1 once the writer's close ended the channel, after its last message: a subscriber that sees it receives what is
    /// left, then ends, without waiting for its control connection's end.
    ended: Atomic(u32),
    reserved: [124]u8,
};

/// One cell of the cell ring: a message's sequence word and where its payload is.
pub const BroadcastCell = extern struct {
    /// The message's index + 1 once complete; (index + 1) | `broadcast_busy` while the writer writes it; 0 never
    /// written.
    seq: Atomic(u64),
    /// The payload's position in the data ring (a monotonic byte count).
    payload_position: Atomic(u64),
    len: Atomic(u32),
    /// Always 0 (reserved).
    flags: Atomic(u32),
    reserved: u64,
};

/// The words subscribers write too, after the data ring: the wake words, the armed bitmap and the gated bitmap.
pub const BroadcastShared = extern struct {
    /// Bit w: word w of `armed` may have bits set. A subscriber sets it after its own bit; the writer, per send, reads
    /// it and takes it whole when it isn't 0.
    armed_any: Atomic(u64),
    /// The subscribers' wait word (Linux, macOS): the writer bumps it after it took armed bits, then wakes every waiter.
    wake_gen: Atomic(u32),
    pad0: [116]u8,
    /// The writer's sleep flag, while it waits for gated subscribers: whoever swaps the 1 back to 0 wakes it.
    writer_sleeps: Atomic(u32),
    pad1: [124]u8,
    /// Bit s: slot s's subscriber is about to sleep, or sleeps, and wants the next send to wake it.
    armed: [16]Atomic(u64),
    /// Bit s: slot s is a gated subscriber the writer waits for.
    gated: [16]Atomic(u64),
    reserved: [512]u8,
};

/// One subscriber's slot, on a line of its own.
pub const BroadcastSlot = extern struct {
    /// Gated: the first index its subscriber hasn't consumed (the subscriber writes it).
    cursor: Atomic(u64),
    reserved: [120]u8,
};

// === Work queues (queue/segment.zig) ===

/// The queue segment's magic, the bytes "FIPCQUE" and the channel protocol's digit, in `QueueInfo.magic`.
pub const queue_magic: u64 = std.mem.readInt(u64, "FIPCQUE" ++ .{'0' + channel_protocol_version}, .little);
/// The most producers and consumers a queue has: one bit each in a 64-bit word (`QueueHeader.ready`, `idle`), where
/// `ready` has one more bit, the return ring's.
pub const max_queue_producers: u32 = 63;
pub const max_queue_consumers: u32 = 64;
/// A queue member's role, as `fipc_queue_join` and `fipc_queue_wait_for_members` take it (`FIPC_PRODUCER`,
/// `FIPC_CONSUMER`) and a queue's JOIN and WELCOME carry it (`ControlQueueJoin.role`, `ControlQueueWelcome.role`).
pub const QueueRole = enum(u32) { producer = 3, consumer = 4 };
/// The smallest capacity of a producer's data ring.
pub const queue_min_capacity: usize = 4096;
/// A payload's room in a data ring is a multiple of this; also the bytes of data ring per cell.
pub const queue_payload_align: usize = 64;
/// A queue's `fipc_max_piece` is its capacity divided by this.
pub const queue_piece_divisor: usize = 4;
pub const queue_header_size: u32 = 512;
/// The ring control block at the start of each producer's region.
pub const queue_ring_control_size: u32 = 512;
/// A cell's state word (`QueueCell.state`): 0 in a cell never written; else a message's phase, its claimer's consumer
/// slot (0 while published), and its tag, the message's index + 1.
pub const QueueCellState = packed struct(u64) {
    phase: QueuePhase,
    claimer: u6,
    tag: u56,
};
/// `none` only in a cell never written (or a corrupt peer's).
pub const QueuePhase = enum(u2) { none = 0, published = 1, claimed = 2, done = 3 };
/// A consumer's intent word (`QueueConsumerSlot.claim`): 0 for none; else the ring of the message it is about to claim,
/// plus one, and the message's tag, its index + 1.
pub const QueueIntent = packed struct(u64) {
    ring_plus_one: u8,
    tag: u56,
};
/// Indices stay below this: index + 1 fills the 56 tag bits of a state word and an intent word.
pub const queue_max_index: u64 = (1 << 56) - 1;

/// OFFER: the owner's offer of the queue's segment (queue/frames.zig).
pub const ControlQueueOffer = extern struct {
    header: ControlHeader,
    /// Bytes of each producer's data ring.
    capacity: u32,
    segment_size: u64,
    session_id: [16]u8,
    max_producers: u32,
    max_consumers: u32,
    reserved: [16]u8 = @splat(0),
};

/// JOIN: the joiner's answer to OFFER, with its role.
pub const ControlQueueJoin = extern struct {
    header: ControlHeader,
    /// A `QueueRole`'s value: a corrupt peer may send any.
    role: u32,
    reserved: [48]u8 = @splat(0),
};

/// WELCOME: the joiner's role, echoed, its slot among those of its role, and the slot's generation.
pub const ControlQueueWelcome = extern struct {
    header: ControlHeader,
    /// A `QueueRole`'s value.
    role: u32,
    slot: u32,
    slot_generation: u32,
    reserved: [40]u8 = @splat(0),
};

/// The queue segment's fixed description, at its start. The owner writes it once, before any OFFER; a joiner checks it
/// before any use and never writes it.
pub const QueueInfo = extern struct {
    magic: u64,
    /// Random and not all zero; names the queue's objects on Windows.
    session_id: [16]u8,
    /// Where the consumers' slots start: 512.
    consumers_offset: u64,
    /// Where producer 0's region starts; ring r's is `producer_stride` bytes further per ring: the producers' rings
    /// 0 to max_producers - 1, then the return ring, ring max_producers.
    producers_offset: u64,
    producer_stride: u64,
    /// Within a producer's region: its cells (512) and its data ring.
    cells_offset: u64,
    data_offset: u64,
    segment_size: u64,
    header_size: u32,
    /// Bytes of each producer's data ring.
    capacity: u32,
    /// Cells of each producer's ring: capacity / 64.
    cell_count: u32,
    max_producers: u32,
    max_consumers: u32,
    reserved: [36]u8,
};

/// The first 512 bytes of a queue segment: the description, then the words every member writes, each on a line of its
/// own.
pub const QueueHeader = extern struct {
    info: QueueInfo,
    /// 1 once the queue ended: producers fail, consumers take what is left and then end.
    ended: Atomic(u32),
    pad0: [124]u8,
    /// Bit r: ring r (a producer's, or the return ring, bit max_producers) may have a message to take.
    ready: Atomic(u64),
    pad1: [120]u8,
    /// Bit c: consumer c is parked, or about to park, and wants a wake.
    idle: Atomic(u64),
    reserved: [120]u8,
};

/// One consumer's slot, on a line of its own.
pub const QueueConsumerSlot = extern struct {
    /// The consumer's park flag (its futex word): 1 while it sleeps or is about to; whoever swaps the 1 back to 0
    /// wakes it.
    sleeps: Atomic(u32),
    pad0: u32,
    /// The consumer's last claim, stored before it: (index + 1) << 8 | (ring + 1); 0 none. The owner reads it when the
    /// consumer ended, to return a message it held.
    claim: Atomic(u64),
    reserved: [112]u8,
};

/// The control block at the start of each ring's region: its indices and its producer's sleep flag, each on a line of
/// its own.
pub const QueueRingControl = extern struct {
    /// The published count: every message below it is complete. Only the ring's producer writes it.
    tail: Atomic(u64),
    pad0: [120]u8,
    /// The first message not known to be claimed: consumers move it on by compare-and-swap.
    head: Atomic(u64),
    pad1: [120]u8,
    /// The producer's sleep flag while it waits for room: whoever swaps the 1 back to 0 wakes it.
    room_sleeps: Atomic(u32),
    pad2: [124]u8,
    reserved: [128]u8,
};

/// A queue ring's cells are in message order: message i at cell i % cell_count (`queue_cell_spread`, 0: two
/// consecutive messages share each line, which a consumer draining a backlog reads at once).
pub const queue_cell_spread: u6 = 0;

/// One cell of a producer's ring: message i at cell i % cell_count.
pub const QueueCell = extern struct {
    /// A `QueueCellState`.
    state: Atomic(u64),
    /// The payload's position in the producer's data ring (a monotonic byte count).
    payload_position: Atomic(u64),
    len: Atomic(u32),
    /// Always 0 (reserved).
    flags: Atomic(u32),
    reserved: u64,
};

// === Services (service/segment.zig) ===

/// The service segment's magic, the bytes "FIPCSVC" and the channel protocol's digit, in `ServiceInfo.magic`.
pub const service_magic: u64 = std.mem.readInt(u64, "FIPCSVC" ++ .{'0' + channel_protocol_version}, .little);
/// The most clients and servers a service has: one bit each in a 64-bit word (`ServiceHeader.ready`, `idle`).
pub const max_service_clients: u32 = 64;
pub const max_service_servers: u32 = 64;
/// A service member's role, as `fipc_service_join` and `fipc_service_wait_for_members` take it (`FIPC_CLIENT`,
/// `FIPC_SERVER`) and a service's JOIN and WELCOME carry it (`ControlServiceJoin.role`, `ControlServiceWelcome.role`).
/// The values go on from the queue's, so that a JOIN of another kind's role is refused.
pub const ServiceRole = enum(u32) { client = 5, server = 6 };
/// The smallest capacity of a client's data ring.
pub const service_min_capacity: usize = 4096;
/// A service's `max_message`, the most bytes of a request and of a reply, is at most its capacity divided by this.
pub const service_message_divisor: usize = 4;
pub const service_header_size: u32 = 512;
/// The ring control block at the start of each client's region.
pub const service_ring_control_size: u32 = 512;
/// A request's answer (`ServiceCell.outcome`), which its server writes before the request's done: `replied` (the reply
/// is in the request's reply room) or `abandoned` (the server that took it ended before it replied, or the request was
/// a corrupt peer's). `none` until then. Non-exhaustive: a peer may write any value.
pub const ServiceOutcome = enum(u32) { none = 0, replied = 1, abandoned = 2, _ };

/// OFFER: the owner's offer of the service's segment (service/frames.zig).
pub const ControlServiceOffer = extern struct {
    header: ControlHeader,
    /// Bytes of each client's data ring.
    capacity: u32,
    segment_size: u64,
    session_id: [16]u8,
    max_clients: u32,
    max_servers: u32,
    max_message: u32,
    reserved: [12]u8 = @splat(0),
};

/// JOIN: the joiner's answer to OFFER, with its role (a `ServiceRole`'s value): a queue's JOIN, of the service's kind.
pub const ControlServiceJoin = ControlQueueJoin;

/// WELCOME: the joiner's role, echoed, its slot among those of its role, and the slot's generation: a queue's WELCOME,
/// of the service's kind.
pub const ControlServiceWelcome = ControlQueueWelcome;

/// The service segment's fixed description, at its start. The owner writes it once, before any OFFER; a joiner checks
/// it before any use and never writes it.
pub const ServiceInfo = extern struct {
    magic: u64,
    /// Random and not all zero; names the service's objects on Windows.
    session_id: [16]u8,
    /// Where the servers' slots start: 512.
    servers_offset: u64,
    /// Where client 0's region starts; client c's is `client_stride` bytes further per client.
    clients_offset: u64,
    client_stride: u64,
    /// Within a client's region: its cells (512) and its data ring.
    cells_offset: u64,
    data_offset: u64,
    segment_size: u64,
    header_size: u32,
    /// Bytes of each client's data ring.
    capacity: u32,
    /// Cells of each client's ring: capacity / 64.
    cell_count: u32,
    max_clients: u32,
    max_servers: u32,
    /// The most bytes of a request and of a reply.
    max_message: u32,
    reserved: [32]u8,
};

/// The first 512 bytes of a service segment: the description, then the words every member writes, each on a line of
/// its own (a queue's header's shape).
pub const ServiceHeader = extern struct {
    info: ServiceInfo,
    /// 1 once the service ended: clients' sends and servers' receives fail, clients receive the replies already done.
    ended: Atomic(u32),
    /// The clients and the servers joined, which the owner stores at each change: a hint for the servers' spin.
    clients_joined: Atomic(u32),
    servers_joined: Atomic(u32),
    pad0: [116]u8,
    /// Bit c: client c's ring may have a request to take.
    ready: Atomic(u64),
    pad1: [120]u8,
    /// Bit s: server s is parked, or about to park, and wants a wake.
    idle: Atomic(u64),
    reserved: [120]u8,
};

/// One server's slot, on a line of its own: a queue consumer's (its park flag, and its intent, which the owner reads
/// when the server ended, to abandon the request it held).
pub const ServiceServerSlot = QueueConsumerSlot;

/// The control block at the start of each client's region: its ring's indices and its client's two sleep flags, each
/// on a line of its own.
pub const ServiceRingControl = extern struct {
    /// The published count: every request below it is complete. Only the ring's client writes it.
    tail: Atomic(u64),
    pad0: [120]u8,
    /// The first request not known to be claimed: servers move it on by compare-and-swap.
    head: Atomic(u64),
    pad1: [120]u8,
    /// The client's sending thread's sleep flag while it waits for room: whoever swaps the 1 back to 0 wakes it.
    room_sleeps: Atomic(u32),
    pad2: [124]u8,
    /// The client's receiving thread's sleep flag while it waits for a reply: whoever swaps the 1 back to 0 wakes it.
    reply_sleeps: Atomic(u32),
    reserved: [124]u8,
};

/// A service ring's cells are spread: request i's cell is i's low log2(cell_count) bits rotated left by 2,
/// (i << 2) % cell_count + (i % cell_count) / (cell_count / 4). Consecutive requests' cells are 128 bytes apart, each
/// on a line of its own and in another pair of lines (which x86's adjacent-line prefetch fetches together): a client
/// publishing request i + 1 doesn't take the line of request i from the server answering it, or from its own thread
/// waiting for that answer. The requests whose cells share a line are a quarter of the ring apart.
pub const service_cell_spread: u6 = 2;

/// One cell of a client's ring: request i at the cell `service_cell_spread` gives it. The first 24 bytes are a queue
/// cell's.
pub const ServiceCell = extern struct {
    /// A `QueueCellState`.
    state: Atomic(u64),
    /// The request's position in the client's data ring (a monotonic byte count). Its reply's room is the
    /// round64(max_message) bytes just before it.
    payload_position: Atomic(u64),
    /// The request's length.
    len: Atomic(u32),
    /// The reply's length, written by its server before the done (0 for an abandoned request).
    reply_len: Atomic(u32),
    /// A `ServiceOutcome`'s value, written before the done.
    outcome: Atomic(u32),
    reserved: u32,
};

// === A stream's rings (stream/ring.zig) ===

/// `fipc_max_piece` is the capacity minus this: the longest piece of a message (the framing takes the rest).
pub const piece_overhead: usize = 64;
/// The smallest ring capacity.
pub const min_capacity: usize = 1024;

/// A frame's length in the ring is a multiple of this (stream/ring.zig).
pub const frame_align: usize = 16;
/// `FrameHeader.flags`: START (bit 0) on a message's first piece, END (bit 1) on its last, PAD (bit 2) on a frame that
/// fills the end of the ring. The other bits are 0.
pub const FrameFlags = packed struct(u32) {
    start: bool = false,
    end: bool = false,
    pad: bool = false,
    reserved: u29 = 0,
};

// === Compile-time checks ===

comptime {
    // A stream's formats (the same on every supported target).
    std.debug.assert(@import("builtin").target.cpu.arch.endian() == .little);
    std.debug.assert(std.mem.eql(u8, std.mem.asBytes(&segment_magic), "FIPCSEG1"));
    expectLayout(ControlHeader, 12, 4, .{ .magic = 0, .pid = 4, .frame_type = 8, .version = 9, .kind = 10, .reserved = 11 });
    expectLayout(ControlStreamSegment, 64, 8, .{ .header = 0, .capacity = 12, .segment_size = 16, .session_id = 24, .reserved = 40 });
    expectLayout(ControlStreamReady, 64, 4, .{ .header = 0, .reserved = 12 });
    expectLayout(RingWriter, 128, 8, .{ .head = 0, .reader_sleeps = 8, .pad = 12 });
    expectLayout(RingReader, 128, 8, .{ .tail = 0, .writer_sleeps = 8, .pad = 12 });
    expectLayout(RingIndices, 256, 8, .{ .writer = 0, .reader = 128 });
    expectLayout(SegmentHeader, 640, 8, .{
        .magic = 0,
        .capacity = 8,
        .header_size = 12,
        .session_id = 16,
        .reserved = 32,
        .s2c = 128,
        .c2s = 384,
    });
    expectLayout(FrameHeader, 16, 8, .{ .flags = 0, .frag_len = 4, .total_len = 8 });
    expectBits(FrameFlags, .{ .start = true }, 0x1);
    expectBits(FrameFlags, .{ .end = true }, 0x2);
    expectBits(FrameFlags, .{ .pad = true }, 0x4);

    // Broadcast channels.
    std.debug.assert(std.mem.eql(u8, std.mem.asBytes(&broadcast_magic), "FIPCPUB1"));
    std.debug.assert(16 * 64 == max_channel_subscribers);
    std.debug.assert(broadcast_header_size == @sizeOf(BroadcastHeader));
    expectLayout(ControlBroadcastOffer, 64, 8, .{
        .header = 0,
        .capacity = 12,
        .segment_size = 16,
        .session_id = 24,
        .slot = 40,
        .slot_generation = 44,
        .max_subscribers = 48,
        .flags = 52,
        .reserved = 56,
    });
    expectLayout(ControlBroadcastJoin, 64, 4, .{ .header = 0, .role = 12, .slot = 16, .slot_generation = 20, .reserved = 24 });
    expectLayout(ControlBroadcastSlot, 64, 4, .{ .header = 0, .slot = 12, .slot_generation = 16, .reserved = 20 });
    expectLayout(BroadcastInfo, 128, 8, .{
        .magic = 0,
        .session_id = 8,
        .cells_offset = 24,
        .data_offset = 32,
        .shared_offset = 40,
        .slots_offset = 48,
        .segment_size = 56,
        .header_size = 64,
        .capacity = 68,
        .cell_count = 72,
        .max_subscribers = 76,
        .flags = 80,
        .reserved = 84,
    });
    expectLayout(BroadcastHeader, 512, 8, .{
        .info = 0,
        .frontier = 128,
        .next_mark = 136,
        .pad1 = 144,
        .next = 256,
        .pad2 = 264,
        .ended = 384,
        .reserved = 388,
    });
    expectLayout(BroadcastCell, 32, 8, .{ .seq = 0, .payload_position = 8, .len = 16, .flags = 20, .reserved = 24 });
    expectLayout(BroadcastShared, 1024, 8, .{
        .armed_any = 0,
        .wake_gen = 8,
        .pad0 = 12,
        .writer_sleeps = 128,
        .pad1 = 132,
        .armed = 256,
        .gated = 384,
        .reserved = 512,
    });
    expectLayout(BroadcastSlot, 128, 8, .{ .cursor = 0, .reserved = 8 });

    // Work queues.
    std.debug.assert(std.mem.eql(u8, std.mem.asBytes(&queue_magic), "FIPCQUE1"));
    std.debug.assert(max_queue_producers + 1 == @bitSizeOf(u64) and max_queue_consumers == @bitSizeOf(u64));
    std.debug.assert(max_queue_consumers <= 1 << @bitSizeOf(@FieldType(QueueCellState, "claimer")));
    std.debug.assert(max_queue_producers + 1 <= std.math.maxInt(@FieldType(QueueIntent, "ring_plus_one")));
    std.debug.assert(queue_max_index == std.math.maxInt(@FieldType(QueueCellState, "tag")));
    std.debug.assert(queue_max_index == std.math.maxInt(@FieldType(QueueIntent, "tag")));
    expectBits(QueueCellState, .{ .phase = .published, .claimer = 0, .tag = 1 }, 0x101);
    expectBits(QueueCellState, .{ .phase = .claimed, .claimer = 5, .tag = 7 }, 7 << 8 | 5 << 2 | 2);
    expectBits(QueueCellState, .{ .phase = .done, .claimer = 63, .tag = queue_max_index }, std.math.maxInt(u64));
    expectBits(QueueIntent, .{ .ring_plus_one = 3, .tag = 9 }, 9 << 8 | 3);
    expectBits(QueueIntent, .{ .ring_plus_one = 64, .tag = queue_max_index }, queue_max_index << 8 | 64);
    std.debug.assert(queue_header_size == @sizeOf(QueueHeader));
    std.debug.assert(queue_ring_control_size == @sizeOf(QueueRingControl));
    expectLayout(ControlQueueOffer, 64, 8, .{
        .header = 0,
        .capacity = 12,
        .segment_size = 16,
        .session_id = 24,
        .max_producers = 40,
        .max_consumers = 44,
        .reserved = 48,
    });
    expectLayout(ControlQueueJoin, 64, 4, .{ .header = 0, .role = 12, .reserved = 16 });
    expectLayout(ControlQueueWelcome, 64, 4, .{ .header = 0, .role = 12, .slot = 16, .slot_generation = 20, .reserved = 24 });
    expectLayout(QueueInfo, 128, 8, .{
        .magic = 0,
        .session_id = 8,
        .consumers_offset = 24,
        .producers_offset = 32,
        .producer_stride = 40,
        .cells_offset = 48,
        .data_offset = 56,
        .segment_size = 64,
        .header_size = 72,
        .capacity = 76,
        .cell_count = 80,
        .max_producers = 84,
        .max_consumers = 88,
        .reserved = 92,
    });
    expectLayout(QueueHeader, 512, 8, .{ .info = 0, .ended = 128, .pad0 = 132, .ready = 256, .pad1 = 264, .idle = 384, .reserved = 392 });
    expectLayout(QueueConsumerSlot, 128, 8, .{ .sleeps = 0, .pad0 = 4, .claim = 8, .reserved = 16 });
    expectLayout(QueueRingControl, 512, 8, .{ .tail = 0, .pad0 = 8, .head = 128, .pad1 = 136, .room_sleeps = 256, .pad2 = 260, .reserved = 384 });
    expectLayout(QueueCell, 32, 8, .{ .state = 0, .payload_position = 8, .len = 16, .flags = 20, .reserved = 24 });

    // Services.
    std.debug.assert(std.mem.eql(u8, std.mem.asBytes(&service_magic), "FIPCSVC1"));
    std.debug.assert(max_service_clients == @bitSizeOf(u64) and max_service_servers == @bitSizeOf(u64));
    std.debug.assert(max_service_servers <= 1 << @bitSizeOf(@FieldType(QueueCellState, "claimer")));
    std.debug.assert(max_service_clients <= std.math.maxInt(@FieldType(QueueIntent, "ring_plus_one")));
    std.debug.assert(@backingInt(ServiceRole.client) > @backingInt(QueueRole.consumer));
    std.debug.assert(service_header_size == @sizeOf(ServiceHeader));
    std.debug.assert(service_ring_control_size == @sizeOf(ServiceRingControl));
    expectLayout(ControlServiceOffer, 64, 8, .{
        .header = 0,
        .capacity = 12,
        .segment_size = 16,
        .session_id = 24,
        .max_clients = 40,
        .max_servers = 44,
        .max_message = 48,
        .reserved = 52,
    });
    expectLayout(ServiceInfo, 128, 8, .{
        .magic = 0,
        .session_id = 8,
        .servers_offset = 24,
        .clients_offset = 32,
        .client_stride = 40,
        .cells_offset = 48,
        .data_offset = 56,
        .segment_size = 64,
        .header_size = 72,
        .capacity = 76,
        .cell_count = 80,
        .max_clients = 84,
        .max_servers = 88,
        .max_message = 92,
        .reserved = 96,
    });
    expectLayout(ServiceHeader, 512, 8, .{
        .info = 0,
        .ended = 128,
        .clients_joined = 132,
        .servers_joined = 136,
        .pad0 = 140,
        .ready = 256,
        .pad1 = 264,
        .idle = 384,
        .reserved = 392,
    });
    expectLayout(ServiceRingControl, 512, 8, .{
        .tail = 0,
        .pad0 = 8,
        .head = 128,
        .pad1 = 136,
        .room_sleeps = 256,
        .pad2 = 260,
        .reply_sleeps = 384,
        .reserved = 388,
    });
    expectLayout(ServiceCell, 32, 8, .{ .state = 0, .payload_position = 8, .len = 16, .reply_len = 20, .outcome = 24, .reserved = 28 });
}

/// The given size, alignment and offset of every field.
fn expectLayout(comptime T: type, comptime size: usize, comptime alignment: usize, comptime offsets: anytype) void {
    const name = @typeName(T);
    if (@sizeOf(T) != size) fail("{s}: size {d}, the protocol needs {d}", .{ name, @sizeOf(T), size });
    if (@alignOf(T) != alignment) fail("{s}: alignment {d}, the protocol needs {d}", .{ name, @alignOf(T), alignment });
    const expected = @typeInfo(@TypeOf(offsets)).@"struct".field_names;
    const actual = @typeInfo(T).@"struct".field_names;
    if (expected.len != actual.len)
        fail("{s}: the offset table lists {d} of its {d} fields", .{ name, expected.len, actual.len });
    inline for (expected) |field_name| {
        const offset = @field(offsets, field_name);
        if (@offsetOf(T, field_name) != offset)
            fail("{s}.{s}: offset {d}, the protocol needs {d}", .{ name, field_name, @offsetOf(T, field_name), offset });
    }
}

/// A packed word's bits: `value` is the integer `bits`, and `bits` reads back as `value`, field by field.
fn expectBits(comptime T: type, comptime value: T, comptime bits: @typeInfo(T).@"struct".backing_integer.?) void {
    const name = @typeName(T);
    const as_bits: @TypeOf(bits) = @bitCast(value);
    if (as_bits != bits) fail("{s}: 0x{x}, the protocol needs 0x{x}", .{ name, as_bits, bits });
    const read_back: T = @bitCast(bits);
    inline for (@typeInfo(T).@"struct".field_names) |field_name| {
        if (@field(read_back, field_name) != @field(value, field_name))
            fail("{s}.{s}: 0x{x} reads back another value", .{ name, field_name, bits });
    }
}

fn fail(comptime fmt: []const u8, comptime args: anytype) noreturn {
    @compileError(std.fmt.comptimePrint("wire check: " ++ fmt, args));
}
