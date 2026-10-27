//! The native Zig API of FastIPC: the module "fastipc" (docs/protocol.md). Errors are Zig error sets and timeouts
//! `std.Io.Timeout`. The calls are the C API's (`include/fipc.h`, c_api.zig, the shared library's root), over the same
//! implementation and the same process-wide `Io` (core/process_state.zig), grouped by kind:
//!   - a stream: `streamCreate` claims a name and returns its `Listener`, whose `accept` sets one client at a time up on
//!     the calling thread; `streamConnect` connects to the name, also on the calling thread, so a client and its server
//!     run on different threads (of one process or two). Their waits aren't cancelation points of an `Io`:
//!     `Listener.cancel` ends an `accept`, and a timeout bounds a `streamConnect`;
//!   - a broadcast: `broadcastCreate` claims a name and returns the publisher's `Conn`, whose sends go to every
//!     subscriber; `broadcastJoin` joins as a `SubscriberRole`;
//!   - a work queue: `queueCreate` claims a name and returns its `Queue`, whose `close` ends it; `queueJoin` joins as a
//!     `QueueRole`, the owner too;
//!   - a service: `serviceCreate` claims a name and returns its `Service`, whose `close` ends it; `serviceJoin` joins
//!     as a `ServiceRole`, the owner too. A client's `send` is a request and its `recv` the next reply, in the order of
//!     the requests, and its `call` both; a server's `recv` is the next request of any client and its `send` the
//!     reply.
//! A broadcast's, a queue's and a service's owner admits its joiners on a task of its own, so a joiner may run on any
//! thread, the owner's included.

const std = @import("std");
const Io = std.Io;
const build_options = @import("build_options");
const wire_protocol = @import("wire_protocol.zig");
const broadcast_client = @import("broadcast/client.zig");
const broadcast_server = @import("broadcast/server.zig");
const calls = @import("core/calls.zig");
const endpoint_module = @import("core/endpoint.zig");
const queue_client = @import("queue/client.zig");
const queue_server = @import("queue/server.zig");
const service_client = @import("service/client.zig");
const service_server = @import("service/server.zig");
const stream_client = @import("stream/client.zig");
const stream_listener = @import("stream/listener.zig");
const stream_server = @import("stream/server.zig");
const wait = @import("core/wait.zig");

/// A connection's lifecycle errors (`streamConnect`, the joins): each is the `fipc_result_t` code of the same name.
pub const Error = endpoint_module.Error;

/// The creates' errors: Invalid (a bad name or size), NoMemory, AddrInUse (another stream, broadcast, queue or service
/// holds the name).
pub const CreateError = endpoint_module.CreateError;

/// `Listener.accept`'s errors, the same as `Error`'s: each is the `fipc_result_t` code of the same name.
pub const AcceptError = endpoint_module.Error;

/// `Queue.waitForMembers`' and `Service.waitForMembers`' errors: each is the `fipc_result_t` code of the same name.
pub const WaitError = queue_server.WaitError;

/// A broadcast subscriber's role (`broadcastJoin`): `lossy` never holds the publisher back and may lose messages
/// (`Overrun`); `gated` misses nothing (the publisher waits for it).
pub const SubscriberRole = wire_protocol.SubscriberRole;

/// A work queue member's role (`queueJoin`, `Queue.waitForMembers`): `producer` or `consumer`.
pub const QueueRole = wire_protocol.QueueRole;

/// A service member's role (`serviceJoin`, `Service.waitForMembers`): `client` or `server`.
pub const ServiceRole = wire_protocol.ServiceRole;

/// The data calls' errors: each is the `fipc_result_t` code of the same name. A data call whose task a
/// `std.Io.Threaded` cancels while it waits returns Cancelled, and the cancelation stays pending for the task's next
/// cancelation point. Overrun and Unsubscribed are a broadcast subscriber's; Abandoned a service client's (the server
/// that took the request ended before it replied).
pub const DataError = endpoint_module.DataError;

/// Server: claims `name` (`[A-Za-z0-9_.-]`, 1 to 245 characters, not starting with `.` or `-`) for a stream and listens
/// on it, with rings of `capacity` bytes (a power of two from 1 KiB to 2 GiB). Doesn't wait. AddrInUse if another
/// stream, broadcast, queue or service holds the name.
pub fn streamCreate(gpa: std.mem.Allocator, name: []const u8, capacity: usize) CreateError!Listener {
    return .{ .inner = try stream_server.create(gpa, name, capacity) };
}

/// Client: connects to the stream on `name` and sets the connection up on the calling thread, waiting up to `timeout`
/// for the server to create it and to call `accept`; the server must run on another thread. Invalid: a bad name, a
/// server of another protocol version or user, or a broadcast, a queue or a service on the name.
pub fn streamConnect(gpa: std.mem.Allocator, name: []const u8, timeout: Io.Timeout) Error!Conn {
    return .{ .endpoint = try stream_client.connect(gpa, name, timeout) };
}

/// Publisher: claims `name` for a broadcast of `capacity` bytes of messages (a power of two from 4 KiB to 2 GiB) and
/// room for `max_subscribers` subscribers (1 to 1024), and returns the publisher's connection, whose sends go to every
/// subscriber and whose `close` ends the broadcast. A gated subscriber held still for `max_stall_ms` from the first send
/// it held back (sends that timed out meanwhile count too) is unsubscribed; a negative `max_stall_ms` never, and 0
/// refuses gated subscribers. Doesn't wait: subscribers may join from now on. NoMemory: the broadcast's memory, which
/// is set up now, can't be had.
pub fn broadcastCreate(gpa: std.mem.Allocator, name: []const u8, capacity: usize, max_subscribers: u32, max_stall_ms: i32) CreateError!Conn {
    return .{ .endpoint = try broadcast_server.create(gpa, name, capacity, max_subscribers, max_stall_ms) };
}

/// Subscriber: joins the broadcast on `name` as `role` on the calling thread, waiting up to `timeout` for the publisher
/// to create it, to have a free slot and to admit it. A subscriber receives the messages sent from its admission on,
/// with `recv` (a gated one also `recvAcquire`); its sends are Invalid. Invalid: a bad name, a stream or a queue on the
/// name, a publisher of another version or user, a gated role on a broadcast that refuses gated subscribers.
pub fn broadcastJoin(gpa: std.mem.Allocator, name: []const u8, role: SubscriberRole, timeout: Io.Timeout) Error!Conn {
    return .{ .endpoint = try broadcast_client.join(gpa, name, role, timeout) };
}

/// Owner: claims `name` for a work queue of up to `max_producers` producers (1 to 63) and `max_consumers` consumers
/// (1 to 64), each producer with a ring of `capacity` bytes of messages (a power of two from 4 KiB to 2 GiB), and
/// returns it. Doesn't wait: members, the owner included, join with `queueJoin` from now on. NoMemory: the queue's
/// memory, which is set up now, can't be had.
pub fn queueCreate(gpa: std.mem.Allocator, name: []const u8, capacity: usize, max_producers: u32, max_consumers: u32) CreateError!Queue {
    return .{ .inner = try queue_server.create(gpa, name, capacity, max_producers, max_consumers) };
}

/// Producer or consumer: joins the queue on `name` as `role` on the calling thread, waiting up to `timeout` for the
/// owner to create it, for a free slot of the role and for the owner to admit it. A producer sends; a consumer
/// receives a message of any producer (no order is promised). Invalid: a bad name, a stream or a broadcast on the
/// name, an owner of another version or user.
pub fn queueJoin(gpa: std.mem.Allocator, name: []const u8, role: QueueRole, timeout: Io.Timeout) Error!Conn {
    return .{ .endpoint = try queue_client.join(gpa, name, role, timeout) };
}

/// Owner: claims `name` for a service of up to `max_clients` clients and `max_servers` servers (1 to 64 each), each
/// client with a ring of `capacity` bytes (a power of two from 4 KiB to 2 GiB) for its requests and their replies, of
/// 1 to `max_message` bytes each (up to a quarter of the capacity), and returns it. Doesn't wait: members, the owner
/// included, join with `serviceJoin` from now on. NoMemory: the service's memory, which is set up now, can't be had.
pub fn serviceCreate(gpa: std.mem.Allocator, name: []const u8, capacity: usize, max_message: usize, max_clients: u32, max_servers: u32) CreateError!Service {
    return .{ .inner = try service_server.create(gpa, name, capacity, max_message, max_clients, max_servers) };
}

/// Client or server: joins the service on `name` as `role` on the calling thread, waiting up to `timeout` for the owner
/// to create it, for a free slot of the role and for the owner to admit it. A client sends requests and receives their
/// replies in order (Abandoned for a request whose server ended); a server receives a request of any client and holds
/// it until its reply, which never waits, so it replies before it receives again (its receives are Invalid meanwhile),
/// and its `close` has a request it still holds answered Abandoned. Invalid: a bad name, a stream, a broadcast or a
/// queue on the name, an owner of another version or user.
pub fn serviceJoin(gpa: std.mem.Allocator, name: []const u8, role: ServiceRole, timeout: Io.Timeout) Error!Conn {
    return .{ .endpoint = try service_client.join(gpa, name, role, timeout) };
}

/// A stream's listener (`streamCreate`): `accept` one client at a time.
pub const Listener = struct {
    inner: *stream_listener.Listener,

    /// Waits for a client, sets its connection up on the calling thread, and returns it. A call that times out in the
    /// middle of a client's setup keeps it for the next call, so a timeout of 0 polls. Invalid while the connection it
    /// returned last is open (one client at a time); Cancelled after `cancel`; Timeout.
    pub fn accept(listener: Listener, timeout: Io.Timeout) AcceptError!Conn {
        return .{ .endpoint = try stream_server.accept(listener.inner, timeout) };
    }

    /// Every `accept` that waits, now or later, returns Cancelled; a client whose setup an `accept` began is dropped.
    /// Any thread.
    pub fn cancel(listener: Listener) void {
        listener.inner.cancel();
    }

    /// Stops listening and frees the listener; the connections it accepted stay open. No other thread may be inside a
    /// call on the listener.
    pub fn close(listener: Listener) void {
        listener.inner.close();
    }
};

/// A work queue (`queueCreate`), held by its owner: a queue lives until `close` or the end of the owner's process.
pub const Queue = struct {
    inner: *queue_server.Queue,

    /// Waits up to `timeout` until at least `at_least` members of `role` (null: of either role) are joined, and returns
    /// how many are; `at_least` 0 returns the count at once. Disconnected once the queue has ended; Cancelled after
    /// `cancel`; Invalid for `at_least` over the role's maximum (their sum for null). Any thread.
    pub fn waitForMembers(queue: Queue, role: ?QueueRole, at_least: u32, timeout: Io.Timeout) WaitError!u32 {
        var count: u32 = 0;
        try queue_server.waitForMembers(queue.inner, role, at_least, milliseconds(queue.inner.owner.io, timeout), &count);
        return count;
    }

    /// Every `waitForMembers` that waits, now or later, returns Cancelled; the queue goes on. Any thread.
    pub fn cancel(queue: Queue) void {
        queue_server.cancel(queue.inner);
    }

    /// Ends the queue (producers get Disconnected; consumers receive what is left, then Disconnected) and frees it; the
    /// members' connections, the owner's own included, stay open until their `close`. No other thread may be inside a
    /// call on the queue.
    pub fn close(queue: Queue) void {
        queue_server.close(queue.inner);
    }
};

/// A service (`serviceCreate`), held by its owner: a service lives until `close` or the end of the owner's process.
pub const Service = struct {
    inner: *service_server.Service,

    /// Waits up to `timeout` until at least `at_least` members of `role` (null: of either role) are joined, and returns
    /// how many are; `at_least` 0 returns the count at once. Disconnected once the service has ended; Cancelled after
    /// `cancel`; Invalid for `at_least` over the role's maximum (their sum for null). Any thread.
    pub fn waitForMembers(service: Service, role: ?ServiceRole, at_least: u32, timeout: Io.Timeout) WaitError!u32 {
        var count: u32 = 0;
        try service_server.waitForMembers(service.inner, role, at_least, milliseconds(service.inner.owner.io, timeout), &count);
        return count;
    }

    /// Every `waitForMembers` that waits, now or later, returns Cancelled; the service goes on. Any thread.
    pub fn cancel(service: Service) void {
        service_server.cancel(service.inner);
    }

    /// Ends the service (clients' sends and servers' receives get Disconnected; clients receive the replies already
    /// done, then Disconnected) and frees it; the members' connections, the owner's own included, stay open until their
    /// `close`. No other thread may be inside a call on the service.
    pub fn close(service: Service) void {
        service_server.close(service.inner);
    }
};

/// One connection: a stream's side, a broadcast's publisher or subscriber, a queue's producer or consumer, or a
/// service's client or server.
pub const Conn = struct {
    endpoint: *endpoint_module.Endpoint,

    // One thread may send while another receives; a service client's `call` does both, so no other thread sends or
    // receives while it runs. None may overlap `close`.

    /// Sends `message` (at least 1 byte), in pieces if it is longer than one. The timeout bounds the wait for room for
    /// the first piece; a started message completes, unless `cancel` or the peer's end stops it (it is then dropped
    /// whole). Disconnected once the peer ended the session: nothing is written. On a service, a client's send is a
    /// request (one piece: TooLarge over `maxPiece`), which waits for room in its ring, where its replies not yet
    /// received hold room too; a server's is the reply to the request it holds, which never waits: Invalid with none
    /// held, TooLarge over `maxPiece` (the request stays held), Disconnected once the service has ended.
    pub fn send(connection: Conn, message: []const u8, timeout: Io.Timeout) DataError!void {
        connection.countRequest();
        calls.send(connection.endpoint, message, connection.timeoutMs(timeout)) catch |err| {
            connection.uncountRequest();
            return err;
        };
    }

    /// Receives the next message into `buf` and returns its length. TooLarge if it doesn't fit: it stays for the next
    /// receive (`recvAlloc` sizes the buffer; on a queue's consumer or a service's server, another member may take it
    /// first, and a buffer of `maxPiece` bytes takes every message there). The timeout bounds the wait for the first
    /// piece, as for `send`. After the peer ended the session, the messages it completed are still delivered, then
    /// Disconnected. A lossy subscriber's Overrun: it lost messages (`lostMessages` says how many), and the next
    /// receive goes on from a recent one. On a service's client: the reply to its oldest request whose reply it hasn't
    /// received yet, waiting for it if it hasn't come, or Abandoned if the server that took that request ended before
    /// it replied (the request counts as answered); a receive that times out or is cancelled leaves its reply for the
    /// next one, and after a `call` that gave up, that call's late reply comes first. On a server: the next request of
    /// any client, which the server holds until its reply (`send` or `commit`): Invalid while it holds one. On both,
    /// Disconnected once the service has ended (a client's after the replies that came before).
    pub fn recv(connection: Conn, buf: []u8, timeout: Io.Timeout) DataError!usize {
        var len: usize = undefined;
        const received = calls.recv(connection.endpoint, buf, connection.timeoutMs(timeout), &len);
        connection.answered(received);
        try received;
        return len;
    }

    /// Sends `messages` (at least one, each of at least 1 byte) in order and publishes them together, with at most one
    /// wake of the receivers for the batch, and before any wait for room what it wrote (`fipc_send_many`). The timeout
    /// bounds the whole call (a duration of zero: only what fits now). `sent` is how many went, on every return: an
    /// error is the first message that didn't go, as `send` would return it, and every one before it went. Every
    /// message is checked first, before any goes: Invalid for an empty one or an empty batch, TooLarge over `maxPiece`
    /// on a broadcast, a queue or a service; on a stream a message longer than one piece goes in pieces, after the
    /// batch so far is published. A service's client sends a batch of requests, whose replies come in their order and
    /// are owed as `send`'s are; Invalid on a server. A producer whose publish meets the queue's end returns
    /// Disconnected, and `sent` leaves out that publish's messages (they may still reach a consumer). Batch both ends:
    /// a receiver that takes one message per call falls behind a batching sender, and the two run slower than without
    /// batches.
    pub fn sendMany(connection: Conn, messages: []const []const u8, timeout: Io.Timeout, sent: *usize) DataError!void {
        connection.countRequests(messages.len);
        const outcome = calls.sendMany(connection.endpoint, calls.SliceBatch.of(messages), connection.timeoutMs(timeout), sent);
        connection.uncountRequests(messages.len - sent.*);
        return outcome;
    }

    /// Waits up to `timeout` for the next message, as `recv`, then takes every further message already there that fits
    /// the rest of `buf`, up to `lens.len` (at least 1), packed back to back, each one's length in `lens`, and returns
    /// how many it took (`fipc_recv_many`). An error is the first message's, as `recv` returns it, and takes nothing
    /// but that message: TooLarge (its length in `lens[0]`), Overrun (the number lost in `lens[0]`), Abandoned. After
    /// the first it never waits, and stops before a message that isn't there yet, that doesn't fit, that is in pieces,
    /// or that would bring another result: the next receive returns it. A broadcast's subscriber takes, after its
    /// first, only the messages its publisher has moved past (all but those in the sixteenth of the capacity it writes
    /// now); the newest come in the next calls. Invalid on a publisher, a producer and a service's server.
    pub fn recvMany(connection: Conn, buf: []u8, lens: []usize, timeout: Io.Timeout) DataError!usize {
        var count: usize = 0;
        const received = calls.recvMany(connection.endpoint, buf, lens, connection.timeoutMs(timeout), &count);
        if (received) |_| {
            for (0..count) |_| connection.answered(received);
        } else |_| connection.answered(received);
        try received;
        return count;
    }

    /// Receives the next message into memory from `gpa`, which the caller frees. The timeout bounds the whole call: on
    /// a queue's consumer or a service's server, another member may take the message first, and the call then waits
    /// for the next one with what is left of it.
    pub fn recvAlloc(connection: Conn, gpa: std.mem.Allocator, timeout: Io.Timeout) (DataError || std.mem.Allocator.Error)![]u8 {
        // First ask for the length: a buffer of zero bytes fits only an empty message, which it receives; any other is
        // TooLarge, stays in the ring, and has its length in `len`. The next receive takes it, unless another consumer
        // or server took it first: it then finds the next message (a lossy subscriber's, Overrun if its copy was
        // overwritten meanwhile), maybe a longer one, so the buffer grows until a message fits
        const deadline = timeout.toDeadline(connection.endpoint.io);
        var buf: []u8 = &.{};
        errdefer gpa.free(buf);
        while (true) {
            var len: usize = undefined;
            const received = calls.recv(connection.endpoint, buf, connection.timeoutMs(deadline), &len);
            if (received) {
                connection.answered(received);
                return if (len == buf.len) buf else gpa.realloc(buf, len); // shorter: another took the one sized
            } else |err| switch (err) {
                error.TooLarge => {
                    gpa.free(buf);
                    buf = &.{};
                    buf = try gpa.alloc(u8, len);
                },
                else => {
                    connection.answered(received);
                    return err;
                },
            }
        }
    }

    /// Zero-copy send, step 1: room for a message of `len` bytes (one piece) in the send ring, waiting up to
    /// `timeout`; `commit` sends what was written there. On a service, a client's room is for a request; a server's is
    /// for the reply to the request it holds, and never waits (Invalid with none held).
    pub fn acquire(connection: Conn, len: usize, timeout: Io.Timeout) DataError![]u8 {
        var buffer: [*]u8 = undefined;
        try calls.sendAcquire(connection.endpoint, len, connection.timeoutMs(timeout), &buffer);
        return buffer[0..len];
    }

    /// Zero-copy send, step 2: sends the first `len` bytes (1 to the acquired length) of the last `acquire` as one
    /// message. On a service's server it is the reply: the request it held, in place too, is no longer valid.
    pub fn commit(connection: Conn, len: usize) DataError!void {
        connection.countRequest();
        calls.sendCommit(connection.endpoint, len) catch |err| {
            connection.uncountRequest();
            return err;
        };
    }

    /// Zero-copy receive, step 1: the next message in place, valid until `release`, the next receive or `close`.
    /// TooLarge for a message of several pieces (`recv` it). Invalid on a lossy subscriber. On a service's server: the
    /// request in place, which stays valid, and held, until its reply (`send` or `commit`), whatever `release` does.
    pub fn recvAcquire(connection: Conn, timeout: Io.Timeout) DataError![]const u8 {
        var message: [*]const u8 = undefined;
        var len: usize = undefined;
        const received = calls.recvAcquire(connection.endpoint, connection.timeoutMs(timeout), &message, &len);
        connection.answered(received);
        try received;
        return message[0..len];
    }

    /// Zero-copy receive, step 2: frees the acquired message's room. Does nothing on a service's server, whose
    /// request is held until its reply.
    pub fn release(connection: Conn) void {
        calls.recvRelease(connection.endpoint);
    }

    /// A lossy subscriber's count of the messages lost at its last receive that returned Overrun: at least 1, the
    /// message whose copy the publisher overwrote included; 0 before the first, and on any other connection. Read it on
    /// the thread that receives.
    pub fn lostMessages(connection: Conn) u64 {
        return calls.lostMessages(connection.endpoint);
    }

    /// A publisher's count of its joined subscribers (admitted, and not yet left, dropped or unsubscribed): waits up
    /// to `timeout` until at least `at_least` are joined, and returns how many are. `at_least` 0 returns the count at
    /// once, which is also how to read it after a Timeout or Cancelled. Invalid on any other connection, or for
    /// `at_least` over the broadcast's `max_subscribers`. Any thread.
    pub fn waitForMembers(connection: Conn, at_least: u32, timeout: Io.Timeout) DataError!u32 {
        var count: u32 = 0;
        try calls.waitForMembers(connection.endpoint, at_least, connection.timeoutMs(timeout), &count);
        return count;
    }

    /// The longest message `acquire` and `recvAcquire` take. For a stream: the ring's capacity less the framing's
    /// share. For a broadcast or a queue: a quarter of its capacity. For a service: its `max_message`.
    pub fn maxPiece(connection: Conn) usize {
        return calls.maxPiece(connection.endpoint);
    }

    /// From now on every call of this connection that would wait returns Cancelled; calls that needn't wait still work,
    /// but a service client's `call` returns Cancelled at once (after its request's checks) and sends nothing. Any
    /// thread.
    pub fn cancel(connection: Conn) void {
        endpoint_module.cancel(connection.endpoint);
    }

    /// The peer ended the session, or the publisher closed (a subscriber's), or the queue ended (a producer's or
    /// consumer's), or the service (a client's or server's): its calls report Disconnected once there's nothing left to
    /// receive.
    pub fn ended(connection: Conn) bool {
        return endpoint_module.ended(connection.endpoint);
    }

    /// Ends the connection and frees it; the peer's calls report Disconnected. A service's member leaves the service: a
    /// client's requests no server took are dropped, and the replies to those already taken are lost; a request a
    /// server still holds is answered Abandoned. No other thread may be inside a call on the connection.
    pub fn close(connection: Conn) void {
        endpoint_module.close(connection.endpoint);
    }

    /// A service's client: sends `request` and returns its reply, in memory from `gpa`, which the caller frees, waiting
    /// up to `timeout` in all; Abandoned if the server that took the request ended before it replied (the request
    /// counts as answered; it may or may not have been carried out, and it is never sent again). A call checks its
    /// request first: Invalid for an empty one, TooLarge over `maxPiece`, at once, even while a reply is owed. Then a
    /// call on a cancelled connection returns Cancelled at once, whatever its timeout, and sends nothing: it could
    /// never receive the reply.
    ///
    /// A call never returns the reply to another request: the connection keeps count of the replies still owed. A call
    /// that timed out or was cancelled waiting for its reply (or couldn't allocate it) leaves that reply owed, and the
    /// next call first receives and drops it (at most one is owed, since every call drops it first; an Abandoned in
    /// its place counts too), within its own timeout and before it sends. A call that had to drop an owed reply sends
    /// its request only if its timeout has time left: once a deadline or a positive duration has run out, it returns
    /// Timeout without sending, so no server sees that request; with a duration of zero no step waits, and it sends if
    /// there is room. After a call that gave up, a plain receive returns that call's late reply (or Abandoned) first:
    /// replies come in request order.
    ///
    /// Invalid while a request sent with `send` or `commit` still has its reply to be received (that reply is the
    /// caller's: receive it first), also after the service's end, and on any connection other than a service's client;
    /// Disconnected once the service has ended. A call sends and receives: no other thread may send or receive on the
    /// connection while it runs.
    pub fn call(connection: Conn, gpa: std.mem.Allocator, request: []const u8, timeout: Io.Timeout) (DataError || std.mem.Allocator.Error)![]u8 {
        const endpoint = connection.endpoint;
        if (!connection.isClient()) return error.Invalid;
        if (request.len == 0) return error.Invalid;
        if (request.len > connection.maxPiece()) return error.TooLarge;
        // Its reply could never be received: the request never goes
        if (endpoint.loadRequests().cancel) return error.Cancelled;
        // A reply to a request of `send`'s is the caller's, not the call's to drop
        if (endpoint.owed.sends.load(.acquire) != 0) return error.Invalid;
        const deadline = timeout.toDeadline(endpoint.io);
        var dropped = false;
        while (endpoint.owed.calls > 0) {
            // A reply owed to a call that gave up on it, dropped in place (a reply is one piece); the receive takes it
            // off the count
            if (connection.recvAcquire(deadline)) |_| {
                connection.release();
            } else |err| if (err != error.Abandoned) return err;
            dropped = true;
        }
        const no_wait = switch (timeout) {
            .duration => |duration| duration.raw.nanoseconds <= 0,
            else => false,
        };
        if (dropped and !no_wait) {
            if (deadline.toDurationFromNow(endpoint.io)) |left| {
                // Its time went on the drop: the request isn't sent, so no server sees it
                if (left.raw.nanoseconds <= 0) return error.Timeout;
            }
        }
        // Cancelled while it dropped: the request never goes
        if (endpoint.loadRequests().cancel) return error.Cancelled;
        endpoint.owed.calls = 1; // its reply is owed from before the send
        calls.send(endpoint, request, connection.timeoutMs(deadline)) catch |err| {
            endpoint.owed.calls = 0;
            return err;
        };
        // A reply or Abandoned takes it off the count; else it stays owed
        return connection.recvAlloc(gpa, deadline);
    }

    fn timeoutMs(connection: Conn, timeout: Io.Timeout) c_int {
        return milliseconds(connection.endpoint.io, timeout);
    }

    fn isClient(connection: Conn) bool {
        return connection.endpoint.hot.view_kind.load(.monotonic) == .service_client;
    }

    /// A service's client is about to send a request: its reply is owed from before the send.
    fn countRequest(connection: Conn) void {
        if (connection.isClient()) _ = connection.endpoint.owed.sends.fetchAdd(1, .acq_rel);
    }

    /// A service's client is about to send `count` requests: their replies are owed from before the send.
    fn countRequests(connection: Conn, count: usize) void {
        if (connection.isClient()) _ = connection.endpoint.owed.sends.fetchAdd(@intCast(@min(count, std.math.maxInt(u32))), .acq_rel);
    }

    /// `count` of the requests counted with `countRequests` weren't sent: nothing is owed to them.
    fn uncountRequests(connection: Conn, count: usize) void {
        if (count == 0 or !connection.isClient()) return;
        const less: u32 = @intCast(@min(count, std.math.maxInt(u32)));
        const sends = &connection.endpoint.owed.sends;
        var owed = sends.load(.acquire);
        while (sends.cmpxchgWeak(owed, owed -| less, .acq_rel, .acquire)) |seen| owed = seen;
    }

    /// The send of a request counted with `countRequest` failed: nothing is owed to it.
    fn uncountRequest(connection: Conn) void {
        if (connection.isClient()) connection.takeSend();
    }

    /// A service client's receive returned `received`: a reply or Abandoned answers the oldest request owed one.
    fn answered(connection: Conn, received: DataError!void) void {
        if (received) |_| {} else |err| if (err != error.Abandoned) return;
        if (!connection.isClient()) return;
        const owed = &connection.endpoint.owed;
        if (owed.calls > 0) owed.calls -= 1 else connection.takeSend();
    }

    fn takeSend(connection: Conn) void {
        const sends = &connection.endpoint.owed.sends;
        var owed = sends.load(.acquire);
        while (owed > 0) owed = sends.cmpxchgWeak(owed, owed - 1, .acq_rel, .acquire) orelse return;
    }
};

/// An `Io.Timeout` as the data path's milliseconds (0: don't wait; negative: no timeout), rounded up to the next
/// millisecond.
fn milliseconds(io: Io, timeout: Io.Timeout) c_int {
    const left = timeout.toDurationFromNow(io) orelse return -1;
    return @intCast(wait.msRoundedUp(left.raw.nanoseconds, std.math.maxInt(c_int)));
}

/// The tests' hooks, in the tests' build of the module only.
pub const test_hooks = if (build_options.test_hooks) @import("core/test_hooks.zig") else struct {};

/// Internals the fuzz harness (tests/fuzz) needs to play a corrupt peer against a library connection, work queue or
/// service: the ring layout, the name derivation, the control frames, the segment attach (a connection's, a queue's and
/// a service's) and the OS rendezvous. test_hooks builds only, so the shipped library never exposes them.
pub const internal = if (build_options.test_hooks) struct {
    pub const wire_protocol = @import("wire_protocol.zig");
    pub const names = @import("core/names.zig");
    pub const wire = @import("core/wire.zig");
    pub const frames = @import("stream/frames.zig");
    pub const segment = @import("stream/segment.zig");
    pub const queue_frames = @import("queue/frames.zig");
    pub const queue_segment = @import("queue/segment.zig");
    pub const service_frames = @import("service/frames.zig");
    pub const service_segment = @import("service/segment.zig");
    pub const shared_memory = @import("core/shared_memory.zig");
    pub const platform = @import("platform.zig");
} else struct {};

test {
    // No export calls the native API: the test build analyzes all of it, so it compiles on each OS the tests run on
    std.testing.refAllDecls(@This());
    std.testing.refAllDecls(Listener);
    std.testing.refAllDecls(Queue);
    std.testing.refAllDecls(Service);
    std.testing.refAllDecls(Conn);
}

/// Connects to the stream on `name` on a thread of its own, while the caller accepts: `streamConnect` returns once the
/// server's `accept` has offered the rings.
const Connector = struct {
    thread: std.Thread,
    result: Error!Conn = error.Timeout,

    fn start(connector: *Connector, name: []const u8, timeout: Io.Timeout) !void {
        connector.thread = try std.Thread.spawn(.{}, run, .{ connector, name, timeout });
    }

    fn run(connector: *Connector, name: []const u8, timeout: Io.Timeout) void {
        connector.result = streamConnect(std.testing.allocator, name, timeout);
    }

    fn join(connector: *Connector) Error!Conn {
        connector.thread.join();
        return connector.result;
    }
};

test "a listener and a client in one process: messages both ways, in pieces, zero-copy, and the timeouts as Io.Timeout" {
    const testing = std.testing;
    const pid = @import("platform.zig").pid();
    var name_buf: [64]u8 = undefined;
    const name = try std.mem.print(&name_buf, "t-native-data-{d}", .{pid});
    const five_s: Io.Timeout = .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } };

    const listener = try streamCreate(testing.allocator, name, 4096);
    defer listener.close();
    try testing.expectError(error.AddrInUse, streamCreate(testing.allocator, name, 4096));
    try testing.expectError(error.AddrInUse, queueCreate(testing.allocator, name, 4096, 1, 1));
    var connector: Connector = undefined;
    try connector.start(name, five_s);
    const accepted = listener.accept(five_s);
    const b = try connector.join();
    defer b.close();
    const a = try accepted;
    defer a.close();
    // One client at a time
    try testing.expectError(error.Invalid, listener.accept(.none));

    var buf: [16]u8 = undefined;
    try a.send("hello", .none);
    try testing.expectEqualStrings("hello", buf[0..try b.recv(&buf, .none)]);
    try b.send("olleh", .none);
    try testing.expectError(error.TooLarge, a.recv(buf[0..2], .none)); // it stays
    try testing.expectEqualStrings("olleh", buf[0..try a.recv(&buf, .none)]);
    // A message longer than the ring goes in pieces, which a receiver on another thread takes in turn
    const long = try testing.allocator.alloc(u8, 10_000);
    defer testing.allocator.free(long);
    for (long, 0..) |*byte, i| byte.* = @truncate(i);
    const Receiver = struct {
        fn run(conn: Conn, expected: []const u8, ok: *bool) void {
            const got = conn.recvAlloc(testing.allocator, .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } }) catch return;
            defer testing.allocator.free(got);
            ok.* = std.mem.eql(u8, got, expected);
        }
    };
    var received = false;
    const receiver = try std.Thread.spawn(.{}, Receiver.run, .{ b, long, &received });
    const sent = a.send(long, five_s);
    receiver.join();
    try sent;
    try testing.expect(received);

    const buffer = try a.acquire(5, .none);
    @memcpy(buffer, "zc-ok");
    try a.commit(5);
    try testing.expectEqualStrings("zc-ok", try b.recvAcquire(.none));
    b.release();

    // Nothing more to receive: a zero duration looks once, a positive one waits, a deadline too
    try testing.expectError(error.Timeout, b.recv(&buf, .{ .duration = .{ .raw = .zero, .clock = .awake } }));
    try testing.expectError(error.Timeout, b.recv(&buf, .{ .duration = .{ .raw = .fromMilliseconds(20), .clock = .awake } }));
    const deadline = Io.Clock.Timestamp.fromNow(testing.io, .{ .raw = .fromMilliseconds(20), .clock = .awake });
    try testing.expectError(error.Timeout, b.recvAcquire(.{ .deadline = deadline }));
}

fn recvOnce(conn: Conn) DataError!usize {
    var buf: [16]u8 = undefined;
    return conn.recv(&buf, .none);
}

test "a data call whose task another Io.Threaded cancels returns Cancelled, and the connection goes on" {
    const testing = std.testing;
    var name_buf: [64]u8 = undefined;
    const name = try std.mem.print(&name_buf, "t-native-io-cancel-{d}", .{@import("platform.zig").pid()});
    const five_s: Io.Timeout = .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } };

    const listener = try streamCreate(testing.allocator, name, 4096);
    defer listener.close();
    var connector: Connector = undefined;
    try connector.start(name, five_s);
    const accepted = listener.accept(five_s);
    const b = try connector.join();
    defer b.close();
    const a = try accepted;
    defer a.close();

    // The receive runs on a task of `testing.io`, not of the library's process-wide `Io`
    var receive = try testing.io.concurrent(recvOnce, .{b});
    try testing.io.sleep(.fromMilliseconds(20), .awake);
    try testing.expectError(error.Cancelled, receive.cancel(testing.io));
    try a.send("after", .none);
    var buf: [16]u8 = undefined;
    try testing.expectEqualStrings("after", buf[0..try b.recv(&buf, five_s)]);
}

test "a broadcast through the module: a lapped lossy subscriber gets Overrun, and lostMessages how many" {
    const testing = std.testing;
    var name_buf: [64]u8 = undefined;
    const name = try std.mem.print(&name_buf, "t-native-lost-{d}", .{@import("platform.zig").pid()});
    const five_s: Io.Timeout = .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } };

    const publisher = try broadcastCreate(testing.allocator, name, 4096, 1, -1);
    defer publisher.close();
    // The publisher's own thread joins: its process admits the subscriber on a task of the library's
    const subscriber = try broadcastJoin(testing.allocator, name, .lossy, five_s);
    defer subscriber.close();
    try testing.expectEqual(0, subscriber.lostMessages());

    // 144 messages of 48 bytes lap its 64 cells twice; the publisher marks every fourth (docs/protocol.md §9.6), so the
    // subscriber goes on from 140, and 0 to 139 are lost
    var message: [48]u8 = @splat(0);
    for (0..144) |i| {
        std.mem.writeInt(u64, message[0..8], i, .little);
        try publisher.send(&message, .none);
    }
    var buf: [64]u8 = undefined;
    try testing.expectError(error.Overrun, subscriber.recv(&buf, five_s));
    try testing.expectEqual(140, subscriber.lostMessages());
    try testing.expectEqual(48, try subscriber.recv(&buf, five_s));
    try testing.expectEqual(140, std.mem.readInt(u64, buf[0..8], .little));
    try testing.expectEqual(140, subscriber.lostMessages()); // until the next Overrun
    try testing.expectEqual(0, publisher.lostMessages());
}

fn waitForSubscribers(publisher: Conn, at_least: u32, result: *DataError!u32) void {
    result.* = publisher.waitForMembers(at_least, .none);
}

test "a broadcast through the module: the publisher counts its subscribers, and waits for them" {
    const testing = std.testing;
    var name_buf: [64]u8 = undefined;
    const name = try std.mem.print(&name_buf, "t-native-count-{d}", .{@import("platform.zig").pid()});
    const five_s: Io.Timeout = .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } };
    const no_wait: Io.Timeout = .{ .duration = .{ .raw = .zero, .clock = .awake } };

    const publisher = try broadcastCreate(testing.allocator, name, 4096, 2, -1);
    defer publisher.close();
    try testing.expectEqual(0, try publisher.waitForMembers(0, no_wait));
    try testing.expectError(error.Timeout, publisher.waitForMembers(1, no_wait));
    try testing.expectError(error.Invalid, publisher.waitForMembers(3, no_wait));

    // A wait on another thread, which the join ends
    var waited: DataError!u32 = error.Timeout;
    const waiter = try std.Thread.spawn(.{}, waitForSubscribers, .{ publisher, 1, &waited });
    const subscriber = try broadcastJoin(testing.allocator, name, .gated, five_s);
    waiter.join();
    try testing.expectEqual(1, try waited);
    try testing.expectError(error.Invalid, subscriber.waitForMembers(0, no_wait));

    // The leave: counted until the publisher's poller has seen it
    subscriber.close();
    const io = testing.io;
    const begun = Io.Clock.awake.now(io);
    while (try publisher.waitForMembers(0, no_wait) != 0) {
        try testing.expect(begun.durationTo(Io.Clock.awake.now(io)).toMilliseconds() < 5000);
        try io.sleep(.fromMilliseconds(1), .awake);
    }

    // The publisher's cancel ends a wait, and every later one; a count that needn't wait still works
    const canceled = try std.Thread.spawn(.{}, waitForSubscribers, .{ publisher, 2, &waited });
    try io.sleep(.fromMilliseconds(20), .awake);
    publisher.cancel();
    canceled.join();
    try testing.expectError(error.Cancelled, waited);
    try testing.expectError(error.Cancelled, publisher.waitForMembers(1, five_s));
    try testing.expectEqual(0, try publisher.waitForMembers(0, five_s));
}

test "a work queue through the module: the owner joins its own queue on its own thread, counts its members, and ends it" {
    const testing = std.testing;
    var name_buf: [64]u8 = undefined;
    const name = try std.mem.print(&name_buf, "t-native-queue-{d}", .{@import("platform.zig").pid()});
    const five_s: Io.Timeout = .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } };
    const no_wait: Io.Timeout = .{ .duration = .{ .raw = .zero, .clock = .awake } };

    try testing.expectError(error.Invalid, queueCreate(testing.allocator, name, 4096, 0, 1));
    const queue = try queueCreate(testing.allocator, name, 4096, 1, 2);
    var closed = false;
    defer if (!closed) queue.close();
    try testing.expectEqual(0, try queue.waitForMembers(null, 0, no_wait));
    try testing.expectError(error.Invalid, queue.waitForMembers(.producer, 2, no_wait));
    const producer = try queueJoin(testing.allocator, name, .producer, five_s);
    defer producer.close();
    try testing.expectEqual(1024, producer.maxPiece());
    // Jobs wait in the ring before any consumer joins
    try producer.send("job 1", .none);
    try producer.send("job 2", .none);
    const consumer = try queueJoin(testing.allocator, name, .consumer, five_s);
    defer consumer.close();
    try testing.expectEqual(1, try queue.waitForMembers(.consumer, 1, five_s));
    try testing.expectEqual(2, try queue.waitForMembers(null, 2, five_s));
    var buf: [16]u8 = undefined;
    try testing.expectEqualStrings("job 1", buf[0..try consumer.recv(&buf, five_s)]);
    try testing.expectEqualStrings("job 2", try consumer.recvAcquire(five_s));
    consumer.release();
    try testing.expectError(error.Invalid, consumer.send("x", .none));
    try testing.expectError(error.Invalid, producer.recv(&buf, .none));
    // The queue's cancel ends its waits for members, not the queue
    queue.cancel();
    try testing.expectError(error.Cancelled, queue.waitForMembers(.consumer, 2, five_s));
    // Its close ends it
    try producer.send("job 3", .none);
    queue.close();
    closed = true;
    try testing.expect(producer.ended());
    try testing.expectError(error.Disconnected, producer.send("late", .none));
    try testing.expectEqualStrings("job 3", buf[0..try consumer.recv(&buf, five_s)]);
    try testing.expectError(error.Disconnected, consumer.recv(&buf, five_s));
    try testing.expect(consumer.ended());
}

/// A service's server on a thread of its own that echoes every request, received with `recvAlloc`, until the service
/// ends; `failed` counts the receives and replies that returned anything else.
const Echoer = struct {
    server: Conn,
    thread: std.Thread = undefined,
    failed: std.atomic.Value(u32) = .init(0),

    fn start(echoer: *Echoer) !void {
        echoer.thread = try std.Thread.spawn(.{}, run, .{echoer});
    }

    fn run(echoer: *Echoer) void {
        const five_s: Io.Timeout = .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } };
        while (true) {
            const request = echoer.server.recvAlloc(std.testing.allocator, five_s) catch |err| {
                if (err != error.Disconnected) _ = echoer.failed.fetchAdd(1, .monotonic);
                return;
            };
            defer std.testing.allocator.free(request);
            echoer.server.send(request, .none) catch {
                _ = echoer.failed.fetchAdd(1, .monotonic);
                return;
            };
        }
    }
};

test "a service through the module: a pool of servers receives with recvAlloc, and a request another server took first leaves it waiting for the next" {
    const testing = std.testing;
    var name_buf: [64]u8 = undefined;
    const name = try std.mem.print(&name_buf, "t-native-pool-{d}", .{@import("platform.zig").pid()});
    const five_s: Io.Timeout = .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } };

    const service = try serviceCreate(testing.allocator, name, 16384, 256, 1, 2);
    var closed = false;
    defer if (!closed) service.close();
    const client = try serviceJoin(testing.allocator, name, .client, five_s);
    defer client.close();
    var echoers: [2]Echoer = undefined;
    for (&echoers) |*echoer| echoer.* = .{ .server = try serviceJoin(testing.allocator, name, .server, five_s) };
    defer for (&echoers) |*echoer| echoer.server.close();
    for (&echoers) |*echoer| try echoer.start();

    // Requests of many lengths in flight at once, which both servers wait for: a server whose request the other took
    // after it sized it waits for the next, of another length, within its timeout
    var request: [256]u8 = undefined;
    for (0..25) |round| {
        for (0..8) |k| {
            const i = round * 8 + k;
            const len = 1 + i * 37 % request.len;
            @memset(request[0..len], @truncate(i));
            try client.send(request[0..len], five_s);
        }
        for (0..8) |k| {
            const i = round * 8 + k;
            const reply = try client.recvAlloc(testing.allocator, five_s);
            defer testing.allocator.free(reply);
            try testing.expectEqual(1 + i * 37 % request.len, reply.len);
            for (reply) |byte| try testing.expectEqual(@as(u8, @truncate(i)), byte);
        }
    }
    service.close();
    closed = true;
    for (&echoers) |*echoer| {
        echoer.thread.join();
        try testing.expectEqual(0, echoer.failed.load(.monotonic));
    }
}

test "a service through the module: zero-copy requests and replies; a server holds its request until it replies" {
    const testing = std.testing;
    var name_buf: [64]u8 = undefined;
    const name = try std.mem.print(&name_buf, "t-native-service-zc-{d}", .{@import("platform.zig").pid()});
    const five_s: Io.Timeout = .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } };

    const service = try serviceCreate(testing.allocator, name, 4096, 128, 1, 1);
    defer service.close();
    const client = try serviceJoin(testing.allocator, name, .client, five_s);
    defer client.close();
    const server = try serviceJoin(testing.allocator, name, .server, five_s);
    defer server.close();
    try testing.expectEqual(128, server.maxPiece());
    // A server with no request held has nothing to reply to
    try testing.expectError(error.Invalid, server.acquire(4, .none));
    try testing.expectError(error.Invalid, server.send("none", .none));
    try testing.expectError(error.TooLarge, client.acquire(129, .none));

    const request = try client.acquire(4, five_s);
    @memcpy(request, "ping");
    try client.commit(4);
    const held = try server.recvAcquire(five_s);
    try testing.expectEqualStrings("ping", held);
    // The release does nothing: the request stays held, and valid in place, until the reply
    server.release();
    try testing.expectEqualStrings("ping", held);
    var buf: [16]u8 = undefined;
    try testing.expectError(error.Invalid, server.recv(&buf, .none));
    try testing.expectError(error.Invalid, server.recvAcquire(.none));
    // The reply never waits; over max_message it is TooLarge, and the request stays held
    try testing.expectError(error.TooLarge, server.acquire(129, .none));
    const reply = try server.acquire(4, .none);
    @memcpy(reply, "pong");
    try server.commit(4);
    try testing.expectError(error.Invalid, server.acquire(4, .none));
    try testing.expectEqualStrings("pong", try client.recvAcquire(five_s));
    client.release();

    // A copied request takes the zero-copy reply too, and a reply sent with `send` a zero-copy receive
    try client.send("ping 2", five_s);
    try testing.expectEqualStrings("ping 2", buf[0..try server.recv(&buf, five_s)]);
    const reply_2 = try server.acquire(6, .none);
    @memcpy(reply_2, "pong 2");
    try server.commit(6);
    try testing.expectEqualStrings("pong 2", buf[0..try client.recv(&buf, five_s)]);
}

test "a service through the module: a server that closes with a request held has its client receive Abandoned" {
    const testing = std.testing;
    var name_buf: [64]u8 = undefined;
    const name = try std.mem.print(&name_buf, "t-native-abandoned-{d}", .{@import("platform.zig").pid()});
    const five_s: Io.Timeout = .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } };

    const service = try serviceCreate(testing.allocator, name, 4096, 128, 1, 2);
    defer service.close();
    const client = try serviceJoin(testing.allocator, name, .client, five_s);
    defer client.close();
    const leaver = try serviceJoin(testing.allocator, name, .server, five_s);
    try client.send("first", five_s);
    var buf: [16]u8 = undefined;
    try testing.expectEqualStrings("first", buf[0..try leaver.recv(&buf, five_s)]);
    // The owner's process answers the request the server held when it left
    leaver.close();
    try testing.expectError(error.Abandoned, client.recv(&buf, five_s));
    try testing.expect(!client.ended());

    // The request counts as answered: the next one goes to a server still there, and its reply comes next
    const server = try serviceJoin(testing.allocator, name, .server, five_s);
    defer server.close();
    try client.send("second", five_s);
    try testing.expectEqualStrings("second", buf[0..try server.recv(&buf, five_s)]);
    try server.send("answer", .none);
    try testing.expectEqualStrings("answer", buf[0..try client.recv(&buf, five_s)]);
}

fn recvOnThread(conn: Conn, result: *DataError!usize) void {
    var buf: [16]u8 = undefined;
    result.* = conn.recv(&buf, .none);
}

test "a service through the module: a connection's cancel ends its waits, a client's reply stays for its next receive" {
    const testing = std.testing;
    const io = testing.io;
    var name_buf: [64]u8 = undefined;
    const name = try std.mem.print(&name_buf, "t-native-service-cancel-{d}", .{@import("platform.zig").pid()});
    const five_s: Io.Timeout = .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } };

    const service = try serviceCreate(testing.allocator, name, 4096, 128, 1, 1);
    defer service.close();
    const client = try serviceJoin(testing.allocator, name, .client, five_s);
    defer client.close();
    const server = try serviceJoin(testing.allocator, name, .server, five_s);
    defer server.close();

    // A server's receive waiting for a request, then every later wait
    var received: DataError!usize = 0;
    const waiter = try std.Thread.spawn(.{}, recvOnThread, .{ server, &received });
    try io.sleep(.fromMilliseconds(20), .awake);
    server.cancel();
    waiter.join();
    try testing.expectError(error.Cancelled, received);
    var buf: [16]u8 = undefined;
    try testing.expectError(error.Cancelled, server.recv(&buf, five_s));

    // A client's receive waiting for a reply; the reply comes later, for the next receive
    try client.send("question", five_s);
    const asker = try std.Thread.spawn(.{}, recvOnThread, .{ client, &received });
    try io.sleep(.fromMilliseconds(20), .awake);
    client.cancel();
    asker.join();
    try testing.expectError(error.Cancelled, received);
    // Calls that needn't wait still work: the cancelled server takes the request there and replies, and the cancelled
    // client receives the reply there
    try testing.expectEqualStrings("question", buf[0..try server.recv(&buf, five_s)]);
    try server.send("answer", .none);
    try testing.expectEqualStrings("answer", buf[0..try client.recv(&buf, five_s)]);
    try testing.expectError(error.Cancelled, client.recv(&buf, five_s));
}

fn waitForServiceMembers(service: Service, role: ?ServiceRole, at_least: u32, result: *WaitError!u32) void {
    result.* = service.waitForMembers(role, at_least, .none);
}

test "a service through the module: waitForMembers counts each role and both, waits for one, and the cancel ends it" {
    const testing = std.testing;
    const io = testing.io;
    var name_buf: [64]u8 = undefined;
    const name = try std.mem.print(&name_buf, "t-native-service-count-{d}", .{@import("platform.zig").pid()});
    const five_s: Io.Timeout = .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } };
    const no_wait: Io.Timeout = .{ .duration = .{ .raw = .zero, .clock = .awake } };

    const service = try serviceCreate(testing.allocator, name, 4096, 128, 2, 3);
    defer service.close();
    try testing.expectEqual(0, try service.waitForMembers(.client, 0, no_wait));
    try testing.expectError(error.Timeout, service.waitForMembers(.server, 1, no_wait));
    try testing.expectError(error.Invalid, service.waitForMembers(.client, 3, no_wait));
    try testing.expectError(error.Invalid, service.waitForMembers(.server, 4, no_wait));
    try testing.expectError(error.Invalid, service.waitForMembers(null, 6, no_wait));

    const server = try serviceJoin(testing.allocator, name, .server, five_s);
    defer server.close();
    try testing.expectEqual(1, try service.waitForMembers(.server, 1, five_s));
    try testing.expectEqual(0, try service.waitForMembers(.client, 0, no_wait));
    // A wait for a client on another thread, which the join ends
    var waited: WaitError!u32 = error.Timeout;
    const waiter = try std.Thread.spawn(.{}, waitForServiceMembers, .{ service, ServiceRole.client, 1, &waited });
    const client = try serviceJoin(testing.allocator, name, .client, five_s);
    waiter.join();
    try testing.expectEqual(1, try waited);
    try testing.expectEqual(2, try service.waitForMembers(null, 2, no_wait));
    try testing.expectEqual(1, try service.waitForMembers(.server, 0, no_wait));

    // The leave: counted until the owner's poller has seen it
    client.close();
    const begun = Io.Clock.awake.now(io);
    while (try service.waitForMembers(.client, 0, no_wait) != 0) {
        try testing.expect(begun.durationTo(Io.Clock.awake.now(io)).toMilliseconds() < 5000);
        try io.sleep(.fromMilliseconds(1), .awake);
    }
    try testing.expectEqual(1, try service.waitForMembers(null, 0, no_wait));

    // The service's cancel ends a wait, and every later one; a count that needn't wait still works
    const canceled = try std.Thread.spawn(.{}, waitForServiceMembers, .{ service, ServiceRole.server, 2, &waited });
    try io.sleep(.fromMilliseconds(20), .awake);
    service.cancel();
    canceled.join();
    try testing.expectError(error.Cancelled, waited);
    try testing.expectError(error.Cancelled, service.waitForMembers(.client, 1, five_s));
    try testing.expectEqual(1, try service.waitForMembers(.server, 0, five_s));
}

const CallResult = (DataError || std.mem.Allocator.Error)![]u8;

fn callOnThread(client: Conn, request: []const u8, timeout: Io.Timeout, result: *CallResult) void {
    result.* = client.call(std.testing.allocator, request, timeout);
}

/// Expects `result` to be the reply `expected`, and frees it.
fn expectReply(expected: []const u8, result: CallResult) !void {
    const reply = try result;
    defer std.testing.allocator.free(reply);
    try std.testing.expectEqualStrings(expected, reply);
}

/// A service's server's answer to one request, which should be `expected`: its uppercase. False if it received another
/// or none, or couldn't reply.
fn answerOne(server: Conn, expected: []const u8) bool {
    var buf: [64]u8 = undefined;
    const len = server.recv(&buf, .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } }) catch return false;
    if (!std.mem.eql(u8, expected, buf[0..len])) return false;
    for (buf[0..len]) |*byte| byte.* = std.ascii.toUpper(byte.*);
    server.send(buf[0..len], .none) catch return false;
    return true;
}

fn answerOnThread(server: Conn, expected: []const u8, ok: *bool) void {
    ok.* = answerOne(server, expected);
}

/// A service of one client and one server, its requests and replies of up to 64 bytes, for the tests of `call`.
const CallService = struct {
    service: Service,
    client: Conn,
    server: Conn,

    fn open(name: []const u8, max_clients: u32) !CallService {
        const five_s: Io.Timeout = .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } };
        const service = try serviceCreate(std.testing.allocator, name, 4096, 64, max_clients, 1);
        errdefer service.close();
        const client = try serviceJoin(std.testing.allocator, name, .client, five_s);
        errdefer client.close();
        return .{ .service = service, .client = client, .server = try serviceJoin(std.testing.allocator, name, .server, five_s) };
    }

    /// Answers the next request, which should be `expected`, on a thread of its own while `request` is called.
    fn callAnswered(s: CallService, request: []const u8, expected_reply: []const u8) !void {
        var answered = false;
        const answering = try std.Thread.spawn(.{}, answerOnThread, .{ s.server, request, &answered });
        const result = s.client.call(std.testing.allocator, request, .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
        answering.join();
        try std.testing.expect(answered);
        try expectReply(expected_reply, result);
    }

    fn close(s: CallService) void {
        s.server.close();
        s.client.close();
        s.service.close();
    }
};

const call_five_s: Io.Timeout = .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } };
const call_50_ms: Io.Timeout = .{ .duration = .{ .raw = .fromMilliseconds(50), .clock = .awake } };
const call_no_wait: Io.Timeout = .{ .duration = .{ .raw = .zero, .clock = .awake } };

test "a service through the module: a batch's requests are owed as sends' are, and batched receives answer them in order" {
    const testing = std.testing;
    var name_buf: [64]u8 = undefined;
    const name = try std.mem.print(&name_buf, "t-native-batch-{d}", .{@import("platform.zig").pid()});
    const s = try CallService.open(name, 1);
    defer s.close();

    var sent: usize = 99;
    try s.client.sendMany(&.{ "a", "b", "c" }, call_five_s, &sent);
    try testing.expectEqual(3, sent);
    // Their replies are the caller's: a call refuses to drop them
    try testing.expectError(error.Invalid, s.client.call(testing.allocator, "d", call_no_wait));
    for ([_][]const u8{ "a", "b", "c" }) |request| try testing.expect(answerOne(s.server, request));
    var buf: [64]u8 = undefined;
    var lens: [8]usize = undefined;
    try testing.expectEqual(3, try s.client.recvMany(&buf, &lens, call_five_s));
    try testing.expectEqualSlices(usize, &.{ 1, 1, 1 }, lens[0..3]);
    try testing.expectEqualStrings("ABC", buf[0..3]);
    // None owed now: a call goes
    try s.callAnswered("e", "E");

    // A batch whose message is too long sends nothing, and owes nothing
    const too_long: [65]u8 = @splat('x');
    try testing.expectError(error.TooLarge, s.client.sendMany(&.{ "f", &too_long }, call_five_s, &sent));
    try testing.expectEqual(0, sent);
    try s.callAnswered("g", "G");
    // A batch cut short owes only what went: 32 requests fill the client's ring (each holds 64 bytes and its reply's 64)
    var requests: [40][]const u8 = @splat("h");
    try testing.expectError(error.Timeout, s.client.sendMany(&requests, call_no_wait, &sent));
    try testing.expectEqual(32, sent);
    for (0..32) |_| try testing.expect(answerOne(s.server, "h"));
    var received: usize = 0;
    while (received < 32) received += try s.client.recvMany(&buf, &lens, call_five_s);
    try testing.expectEqual(32, received);
    try s.callAnswered("i", "I");
    requests[0] = "";
    try testing.expectError(error.Invalid, s.client.sendMany(&requests, call_no_wait, &sent));
    try testing.expectError(error.Invalid, s.server.sendMany(&.{"j"}, call_no_wait, &sent));
    try testing.expectError(error.Invalid, s.server.recvMany(&buf, &lens, call_no_wait));
}

test "a service through the module: call sends a request and returns its reply, on a service's client only, and checks its request first" {
    const testing = std.testing;
    var name_buf: [64]u8 = undefined;
    const name = try std.mem.print(&name_buf, "t-native-call-{d}", .{@import("platform.zig").pid()});
    const s = try CallService.open(name, 1);
    defer s.close();

    try s.callAnswered("ping", "PING");
    const longest: [64]u8 = @splat('a'); // maxPiece
    const longest_reply: [64]u8 = @splat('A');
    try s.callAnswered(&longest, &longest_reply);

    // The request's checks: nothing is sent, and nothing owed
    try testing.expectError(error.Invalid, s.client.call(testing.allocator, "", call_five_s));
    const too_long: [65]u8 = @splat('x');
    try testing.expectError(error.TooLarge, s.client.call(testing.allocator, &too_long, call_five_s));
    var buf: [64]u8 = undefined;
    try testing.expectError(error.Timeout, s.server.recv(&buf, call_50_ms));

    // Any other connection: a server, a work queue's producer
    try testing.expectError(error.Invalid, s.server.call(testing.allocator, "x", call_no_wait));
    var queue_name_buf: [64]u8 = undefined;
    const queue_name = try std.mem.print(&queue_name_buf, "t-native-call-queue-{d}", .{@import("platform.zig").pid()});
    const queue = try queueCreate(testing.allocator, queue_name, 4096, 1, 1);
    defer queue.close();
    const producer = try queueJoin(testing.allocator, queue_name, .producer, call_five_s);
    defer producer.close();
    try testing.expectError(error.Invalid, producer.call(testing.allocator, "x", call_no_wait));
}

test "a service through the module: a call that gave up leaves its reply owed, the next call drops it, and a plain receive takes it first" {
    const testing = std.testing;
    const gpa = testing.allocator;
    var name_buf: [64]u8 = undefined;
    const name = try std.mem.print(&name_buf, "t-native-call-owed-{d}", .{@import("platform.zig").pid()});
    const s = try CallService.open(name, 1);
    defer s.close();
    var buf: [64]u8 = undefined;

    try testing.expectError(error.Timeout, s.client.call(gpa, "late", call_50_ms)); // went: no reply in time
    try testing.expect(answerOne(s.server, "late")); // the late reply, owed to the call that gave up
    // The request's checks come first, even while a reply is owed: with no timeout, neither waits
    try testing.expectError(error.Invalid, s.client.call(gpa, "", .none));
    const too_long: [65]u8 = @splat('x');
    try testing.expectError(error.TooLarge, s.client.call(gpa, &too_long, .none));
    try s.callAnswered("next", "NEXT"); // not LATE
    try testing.expectError(error.Timeout, s.client.recv(&buf, call_no_wait)); // nothing more owed

    // A call whose owed reply hasn't come waits for it within its own timeout, and gives up before it sends
    try testing.expectError(error.Timeout, s.client.call(gpa, "a", call_no_wait)); // a went; its reply is owed
    try testing.expectError(error.Timeout, s.client.call(gpa, "b", call_50_ms)); // a's reply isn't there: no b
    try testing.expect(answerOne(s.server, "a"));
    try testing.expectError(error.Timeout, s.server.recv(&buf, call_no_wait)); // no b
    var called: CallResult = error.Invalid;
    const calling = try std.Thread.spawn(.{}, callOnThread, .{ s.client, "c", Io.Timeout{ .duration = .{ .raw = .fromMilliseconds(200), .clock = .awake } }, &called });
    const taken = s.server.recv(&buf, call_five_s); // drops A, sends c, gives up
    calling.join();
    try testing.expectError(error.Timeout, called);
    try testing.expectEqualStrings("c", buf[0..try taken]);
    try s.server.send("C", .none); // owed to the call that gave up
    try testing.expectEqualStrings("C", buf[0..try s.client.recv(&buf, call_five_s)]); // a plain receive, in order
    try testing.expectError(error.Timeout, s.client.recv(&buf, call_no_wait));

    // A request of send whose reply hasn't been received: call refuses, and leaves it for the caller's receive
    try s.client.send("mine", call_five_s);
    try testing.expectError(error.Invalid, s.client.call(gpa, "x", call_five_s));
    try testing.expect(answerOne(s.server, "mine"));
    const mine = try s.client.recvAlloc(gpa, call_five_s);
    defer gpa.free(mine);
    try testing.expectEqualStrings("MINE", mine);
    // ... and so does one of commit, received in place
    @memcpy(try s.client.acquire(4, call_five_s), "slot");
    try s.client.commit(4);
    try testing.expectError(error.Invalid, s.client.call(gpa, "x", call_five_s));
    try testing.expect(answerOne(s.server, "slot"));
    try testing.expectEqualStrings("SLOT", try s.client.recvAcquire(call_five_s));
    s.client.release();
    // A send that fails owes nothing
    try testing.expectError(error.Invalid, s.client.send("", call_no_wait));
    try s.callAnswered("x", "X");
    try testing.expectError(error.Timeout, s.client.recv(&buf, call_no_wait));
}

test "a service through the module: a call whose time runs out while it drops an owed reply never sends its request; a zero duration drops and sends" {
    const testing = std.testing;
    const gpa = testing.allocator;
    const io = testing.io;
    var name_buf: [64]u8 = undefined;
    const name = try std.mem.print(&name_buf, "t-native-call-deadline-{d}", .{@import("platform.zig").pid()});
    const s = try CallService.open(name, 1);
    defer s.close();
    var buf: [64]u8 = undefined;

    // A timeout of 1 ns: the call drops the owed reply (there, so its receive needn't wait), and by then the timeout
    // has run out, so its request isn't sent
    try testing.expectError(error.Timeout, s.client.call(gpa, "late", call_50_ms)); // went: no reply in time
    try testing.expect(answerOne(s.server, "late")); // the late reply has come once the send returns
    try testing.expectError(error.Timeout, s.client.call(gpa, "never", .{ .duration = .{ .raw = .fromNanoseconds(1), .clock = .awake } }));
    try testing.expectError(error.Timeout, s.server.recv(&buf, call_50_ms)); // the server never saw it
    try testing.expectError(error.Timeout, s.client.recv(&buf, call_no_wait)); // nothing is owed
    // A deadline that has passed by then, the same
    try testing.expectError(error.Timeout, s.client.call(gpa, "late", call_50_ms));
    try testing.expect(answerOne(s.server, "late"));
    const now = Io.Clock.Timestamp.fromNow(io, .{ .raw = .zero, .clock = .awake });
    try testing.expectError(error.Timeout, s.client.call(gpa, "never", .{ .deadline = now }));
    try testing.expectError(error.Timeout, s.server.recv(&buf, call_50_ms));
    try s.callAnswered("next", "NEXT");

    // A duration of zero waits at no step: with the owed reply there, the call drops it and sends
    try testing.expectError(error.Timeout, s.client.call(gpa, "a", call_no_wait)); // a went; its reply is owed
    try testing.expect(answerOne(s.server, "a"));
    try testing.expectError(error.Timeout, s.client.call(gpa, "b", call_no_wait)); // drops A, sends b
    try testing.expect(answerOne(s.server, "b"));
    try testing.expectEqualStrings("B", buf[0..try s.client.recv(&buf, call_five_s)]); // the late reply first
}

test "a service through the module: a call returns Abandoned, and an Abandoned owed to a call that gave up is dropped by the next" {
    const testing = std.testing;
    const gpa = testing.allocator;
    var name_buf: [64]u8 = undefined;
    const name = try std.mem.print(&name_buf, "t-native-call-abandoned-{d}", .{@import("platform.zig").pid()});
    const service = try serviceCreate(gpa, name, 4096, 64, 1, 2);
    defer service.close();
    const client = try serviceJoin(gpa, name, .client, call_five_s);
    defer client.close();
    var buf: [64]u8 = undefined;

    const quitter = try serviceJoin(gpa, name, .server, call_five_s);
    var called: CallResult = error.Invalid;
    const calling = try std.Thread.spawn(.{}, callOnThread, .{ client, "lost", call_five_s, &called });
    const taken = quitter.recv(&buf, call_five_s);
    quitter.close(); // holding it: the request is answered Abandoned
    calling.join();
    try testing.expectEqualStrings("lost", buf[0..try taken]);
    try testing.expectError(error.Abandoned, called);

    const leaver = try serviceJoin(gpa, name, .server, call_five_s);
    try testing.expectError(error.Timeout, client.call(gpa, "lost too", call_50_ms)); // went: no reply in time
    try testing.expectEqualStrings("lost too", buf[0..try leaver.recv(&buf, call_five_s)]);
    leaver.close(); // the Abandoned is owed to the call that gave up
    const server = try serviceJoin(gpa, name, .server, call_five_s);
    defer server.close();
    var answered = false;
    const answering = try std.Thread.spawn(.{}, answerOnThread, .{ server, "next", &answered });
    const result = client.call(gpa, "next", call_five_s); // drops the Abandoned, gets its own
    answering.join();
    try testing.expect(answered);
    try expectReply("NEXT", result);
    try testing.expectError(error.Timeout, client.recv(&buf, call_no_wait)); // nothing more owed
}

test "a service through the module: a call after the service's end" {
    const testing = std.testing;
    const gpa = testing.allocator;
    const pid = @import("platform.zig").pid();
    var name_buf: [64]u8 = undefined;
    var buf: [64]u8 = undefined;

    // Disconnected from the drop of an owed reply that never came
    {
        var s = try CallService.open(try std.mem.print(&name_buf, "t-native-call-end-owed-{d}", .{pid}), 1);
        defer s.close();
        try testing.expectError(error.Timeout, s.client.call(gpa, "held", call_50_ms)); // its reply is owed
        try testing.expectEqualStrings("held", buf[0..try s.server.recv(&buf, call_five_s)]);
        s.service.close();
        s.service = try serviceCreate(gpa, try std.mem.print(&name_buf, "t-native-call-end-next-{d}", .{pid}), 4096, 64, 1, 1);
        try testing.expectError(error.Disconnected, s.client.call(gpa, "x", call_five_s));
    }
    // Disconnected from the send, with nothing owed; the request is checked first
    {
        var s = try CallService.open(try std.mem.print(&name_buf, "t-native-call-end-send-{d}", .{pid}), 1);
        defer s.close();
        s.service.close();
        s.service = try serviceCreate(gpa, try std.mem.print(&name_buf, "t-native-call-end-next-{d}", .{pid}), 4096, 64, 1, 1);
        try testing.expectError(error.Disconnected, s.client.call(gpa, "x", call_five_s));
        try testing.expectError(error.Invalid, s.client.call(gpa, "", call_five_s));
    }
    // Invalid while a reply owed to a request of send's never comes
    {
        var s = try CallService.open(try std.mem.print(&name_buf, "t-native-call-end-sent-{d}", .{pid}), 1);
        defer s.close();
        try s.client.send("mine", call_five_s);
        s.service.close();
        s.service = try serviceCreate(gpa, try std.mem.print(&name_buf, "t-native-call-end-next-{d}", .{pid}), 4096, 64, 1, 1);
        try testing.expectError(error.Invalid, s.client.call(gpa, "x", call_five_s));
        try testing.expectError(error.Disconnected, s.client.recv(&buf, call_five_s)); // that reply never comes
    }
}

test "a service through the module: a cancel ends a call's wait and leaves its reply owed; a call on a cancelled connection sends nothing" {
    const testing = std.testing;
    const gpa = testing.allocator;
    const pid = @import("platform.zig").pid();
    var name_buf: [64]u8 = undefined;
    var buf: [64]u8 = undefined;

    const s = try CallService.open(try std.mem.print(&name_buf, "t-native-call-cancel-{d}", .{pid}), 1);
    defer s.close();
    var called: CallResult = error.Invalid;
    const calling = try std.Thread.spawn(.{}, callOnThread, .{ s.client, "cancelled", Io.Timeout.none, &called });
    const taken = s.server.recv(&buf, call_five_s);
    s.client.cancel(); // from another thread: the call's wait for its reply returns Cancelled
    calling.join();
    try testing.expectEqualStrings("cancelled", buf[0..try taken]);
    try testing.expectError(error.Cancelled, called);
    try s.server.send("CANCELLED", .none); // owed to the cancelled call
    try testing.expectEqualStrings("CANCELLED", buf[0..try s.client.recv(&buf, call_no_wait)]); // needn't wait
    try testing.expectError(error.Cancelled, s.client.recv(&buf, call_five_s)); // final
    try testing.expectError(error.Cancelled, s.client.call(gpa, "x", call_no_wait));

    const t = try CallService.open(try std.mem.print(&name_buf, "t-native-call-cancelled-{d}", .{pid}), 1);
    defer t.close();
    t.client.cancel();
    try testing.expectError(error.Invalid, t.client.call(gpa, "", call_five_s)); // the request's checks come first
    const too_long: [65]u8 = @splat('x');
    try testing.expectError(error.TooLarge, t.client.call(gpa, &too_long, call_five_s));
    for ([_]Io.Timeout{ .none, call_no_wait, call_five_s }) |timeout| {
        try testing.expectError(error.Cancelled, t.client.call(gpa, "never sent", timeout));
    }
    try testing.expectError(error.Timeout, t.server.recv(&buf, .{ .duration = .{ .raw = .fromMilliseconds(200), .clock = .awake } })); // no request went
}
