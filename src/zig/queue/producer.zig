//! A work queue's producer: the queue's ring set's sender (core/sender.zig: the sends into its own ring, the publish,
//! the room it waits for), on a mapped queue segment (segment.zig `Layout`) and the producer's endpoint; the
//! producer's lifecycle sets it up. How consumers take the messages is consumer.zig. The owner is the producer of the
//! return ring (returns.zig): it publishes there as a producer does, but never waits for room (`reserveNow`).
//!
//! The end. The queue's end's mark is stored before the consumers' drain (server.zig `close`, client.zig
//! `endMembership`), and a publish loads it after its `tail`: a send that returns OK is drained by a consumer that
//! receives until DISCONNECTED; one that returns DISCONNECTED may or may not have been.

const segment = @import("segment.zig");

/// A producer's state: private to its sending thread.
pub const Producer = @import("../core/sender.zig").Sender(segment.Rings);
