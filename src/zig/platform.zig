//! The OS contract: everything FastIPC needs from the operating system, in FastIPC's terms. The rest of the library
//! imports this file only. Each OS implements it in one file, in the order of the sections below:
//! `platform/linux/linux.zig`, `platform/macos/macos.zig` and `platform/windows/windows.zig`. The other files of a
//! folder are private to it, and the OS's file re-exports what the contract needs from them:
//!   - `platform/posix.zig`: what Linux and macOS share, over libc;
//!   - `macos/rendezvous.zig` (a name's files, its claim and connect), `windows/pipe.zig` (the named pipe) and
//!     `windows/poller.zig` (the poller);
//!   - `macos/darwin.zig` and `windows/win32.zig`: raw system declarations.
//! `platform/contract_test.zig` tests the contract on every OS. `platform/macos/test_sys.zig` is test code only: the
//! system calls of the fork tests on macOS.
//!
//! Dispatch is static: each declaration here is the building OS's own function or type, so a call costs what a
//! direct call does. Each function is declared with the contract's signature, so an OS file that doesn't match fails
//! to compile where the library uses it. A port to another OS implements every declaration below, except those
//! marked "only where `passes_segment_handle`" when it passes segments by name.
//!
//! The objects involved:
//!   - a **name** (names.zig) gives an `Address`: where a listener can be found, private to this user (and on
//!     Windows to this logon session);
//!   - a **listener** holds its name through a listening handle, and accepts one client at a time on it;
//!   - a **connection** is a local, reliable, bidirectional byte stream between two processes (the control
//!     connection). It carries a few 64-byte frames during the handshake (a stream's SEGMENT and READY; a
//!     broadcast's, a queue's or a service's OFFER, JOIN and WELCOME), and its end tells each side that the other's
//!     process closed it or ended;
//!   - a **segment** is shared memory: a stream connection's, created by the listener and mapped by the client, or a
//!     broadcast's, a queue's or a service's, created by its owner and mapped by every member;
//!   - a **ring event** wakes a thread sleeping on a ring's sleep flag, in the other process;
//!   - a broadcast's writer wakes its sleeping subscribers all at once on one shared word, or each on its slot's own
//!     **slot wake** (**broadcast waits**);
//!   - a **poller** watches, all on one thread, the control connections of a broadcast's subscribers or a queue's
//!     members, and the listening handle on which its joiners arrive.

const std = @import("std");
const builtin = @import("builtin");
const Io = std.Io;
const names = @import("core/names.zig");
const wire = @import("core/wire.zig");

const impl = switch (builtin.os.tag) {
    .linux => @import("platform/linux/linux.zig"),
    .windows => @import("platform/windows/windows.zig"),
    .macos => @import("platform/macos/macos.zig"),
    else => @compileError("FastIPC supports Linux, macOS and Windows"),
};

// === Capabilities: real differences in behavior ===

/// The process can fork with the library's state in it (Linux, macOS). Then every handle the library owns is
/// registered in the fork registry (core/process_state.zig), and a forked child closes its copies and marks its
/// connections inherited, so it can't disturb its parent's connections.
pub const has_fork: bool = impl.has_fork;

/// The listener sends the segment's handle with the SEGMENT frame (Linux: a sealed memfd, macOS: a POSIX shared memory
/// object, over `SCM_RIGHTS`), and the client maps what it received. Otherwise (Windows) the client opens the segment
/// by the name its session id gives.
pub const passes_segment_handle: bool = impl.passes_segment_handle;

// === Errors and outcomes ===

/// Out of handles or kernel memory: the caller backs off and retries.
pub const ResourceError = error{SystemResources};
/// A failure the protocol doesn't expect, logged where it happened.
pub const UnexpectedError = error{Unexpected};

/// What a listener's accept got.
pub const Accept = union(enum) {
    /// A client connected: its connection. Where a client connects on the listening handle itself (a Windows pipe
    /// instance), this is that handle, and the listener needs another (`nextListening`) before it accepts again.
    connection: Handle,
    /// A client connected and left before the accept: accept again at once.
    closing,
    /// Handles ran out: back off, then accept again.
    transient,
    /// Any other failure (logged): back off, then accept again.
    other,
};

/// What a client's connect got.
pub const Connect = union(enum) {
    /// Connected to the listener: the connection.
    connected: Handle,
    /// Nobody listens on the name (yet).
    no_listener,
    /// The listener has a client, or is between clients: try again later.
    busy,
    /// A process of another user holds the name: the connection can't be trusted.
    foreign,
};

/// Which end of a connection this process holds.
pub const Side = enum { listener, client };

/// The process at the other end of a connection.
pub const Peer = struct {
    /// It runs as this process's user.
    same_user: bool,
    /// Its process id, if the OS can tell.
    pid: ?u32,
};

/// Why a SEGMENT frame and its handle weren't received (only where `passes_segment_handle`).
pub const RecvError = error{
    /// The connection ended.
    EndOfStream,
    /// Fewer bytes than a frame.
    ShortFrame,
    /// The OS dropped the handle (this process is at its handle limit).
    ControlTruncated,
    /// Not exactly one handle.
    DescriptorCount,
} || UnexpectedError;

pub const CreateError = error{
    /// An object of the segment's name exists: the random session id collided; draw another.
    NameCollision,
    /// The segment can't get its memory, or address space to map it: NO_MEMORY.
    OutOfMemory,
} || ResourceError || UnexpectedError;

pub const OpenError = error{
    /// The segment no longer exists: the listener left after offering it.
    Gone,
    /// The segment's security refused this process: another user, or an incompatible elevation.
    AccessDenied,
    /// The segment isn't the size the offer announced: a protocol error.
    WrongSize,
    /// The received segment can still change size, so an access could fault: a protocol error.
    NotSealed,
    /// No address space for the mapping: NO_MEMORY.
    OutOfMemory,
} || ResourceError || UnexpectedError;

// === The process ===

/// A handle the library owns: a connection, a listening handle, a listener's cancel signal, or a segment's handle while
/// it is passed.
pub const Handle = impl.Handle;

/// The security of the objects this process creates: only its user may open them. `init` builds it once per
/// listener or client, `deinit` frees it.
pub const Security = impl.Security;

/// A process-wide lock that needs no `Io`, which a fork handler may take (`init` is `.{}`).
pub const Lock = impl.Lock;

/// Steps of the OS's own that a fork must not split, made once, before the fork handlers are registered
/// (core/process_state.zig), so that a child forked later inherits them done. Nothing where there are none.
pub const prepareForForks: fn () void = if (@hasDecl(impl, "prepareForForks")) impl.prepareForForks else struct {
    fn none() void {}
}.none;

/// A fork handler (`registerForkHandlers`).
pub const ForkHandler = *const fn () callconv(.c) void;

/// Only where `has_fork`: registers `prepare`, `parent` and `child` to run around every fork of the process
/// (`pthread_atfork`), once per address space (core/process_state.zig). False: no memory for them.
pub const registerForkHandlers: fn (prepare: ForkHandler, parent: ForkHandler, child: ForkHandler) bool = impl.registerForkHandlers;

/// Only where `has_fork`: keeps the shared library whose code holds `anchor` loaded for the life of the process, so an
/// application's unload of it does nothing (core/process_state.zig `keepLoaded` says why). A failure is logged and
/// changes nothing else.
pub const keepImageLoaded: fn (anchor: *const anyopaque) void = impl.keepImageLoaded;

pub const pid: fn () u32 = impl.pid;

/// The monotonic clock in nanoseconds, without `Io` (for the test hooks' wait reports, where none is at hand).
pub const monotonicNs: fn () u64 = impl.monotonicNs;

/// Ends this process at once with exit code `code`, as a crash would: nothing is cleaned up, and the OS closes what
/// it held (the test hooks' crash).
pub const crash: fn (code: u8) noreturn = impl.crash;

/// Writes `bytes` to the process's standard error in one write, ignoring errors.
pub const writeStderr: fn (bytes: []const u8) void = impl.writeStderr;

/// The stack of each worker thread of the process-wide `Io`, where the connections' watchers run.
pub const task_stack_size: usize = impl.task_stack_size;

/// Shuts the process-wide `Io` down once nothing runs on it: `deinit` joins its workers, and where the library can be
/// unloaded as soon as its last reference is released (a Windows DLL), this also waits until each worker's thread has
/// exited, since a worker runs a few instructions of the library after the join lets it go.
pub const shutDownIo: fn (threaded: *Io.Threaded) void = impl.shutDownIo;

// === Names and connections ===

/// Where a connection's listener is found: what both sides derive from the connection's name.
pub const Address = impl.Address;

/// The address of `name` (valid by names.isValid).
pub const addressOf: fn (name: []const u8) UnexpectedError!Address = impl.addressOf;

/// Claims the name: a listening handle, which holds it until closed. Null when another listener holds it.
pub const claim: fn (address: *const Address, security: *const Security) (ResourceError || UnexpectedError)!?Handle = impl.claim;

/// Where a client connects on the listening handle itself: another listening handle for the name, to accept the next
/// client on once this one's connection is handed over, so the name stays held throughout. Null where the listening
/// handle stays (a socket).
pub const nextListening: fn (address: *const Address, security: *const Security) (ResourceError || UnexpectedError)!?Handle = impl.nextListening;

/// Waits up to `timeout_ms` (null: no limit; 0: only looks) on the caller's thread for a client on the listening
/// handle, or for the cancel signal `cancel`, and accepts the client. No I/O is left in flight when it returns.
/// Where the OS forks, it sets `forks` (if given) to the fork count (core/process_state.zig) just before the accept
/// itself, after the wait: only a fork after that can have copied the new connection into a child.
pub const acceptWithin: fn (listening: Handle, cancel: ?Handle, timeout_ms: ?u32, forks: ?*u32) Waited(Accept) = impl.acceptWithin;

/// Ends an accepted client's connection. `forked`: a child forked while the accept ran may hold a copy of the handle,
/// so the connection is ended for every holder. Returns the handle when it can accept again (where a client connects
/// on the listening handle itself), else null: it was closed.
pub const dropClient: fn (connection: Handle, forked: bool) ?Handle = impl.dropClient;

/// Connects to the listener at `address` without waiting.
pub const connect: fn (address: *const Address, security: *const Security) (ResourceError || UnexpectedError)!Connect = impl.connect;

/// Who is at the other end of a connection. The user was checked when the connection was made where the OS does
/// that (Windows: the pipe's security and owner). Null if the OS can't tell: drop the connection and retry.
pub const peer: fn (connection: Handle, side: Side) ?Peer = impl.peer;

/// Makes a blocked read on the handle return at once, and every later one, in this process only. Nothing where the OS
/// can't (a Windows pipe; `endTask` cancels the watcher instead).
pub const unblock: fn (handle: Handle) void = impl.unblock;

/// Waits until a connection's watcher has exited, after its stop was requested and `unblock` was called on its
/// connection: where `unblock` can't end a blocked read, `Io` cancels it.
pub const endTask: fn (io: Io, task: *Io.Future(void)) void = impl.endTask;

pub const close: fn (handle: Handle) void = impl.close;

// === Frames on a connection ===

/// Sends one frame, without blocking (a frame fits an empty buffer) and without a signal if the peer is gone; with
/// the segment's handle `attached` (only where `passes_segment_handle`, else null). It runs on a poller's thread too,
/// so it never waits alertably (Windows). False: the peer is gone, or the send failed.
pub const sendFrame: fn (connection: Handle, frame: []const u8, attached: ?Handle) bool = impl.sendFrame;

/// Reads through `io` into `buf`, blocking until at least one byte arrives: the watcher's read. 0: the connection
/// ended (the other side closed it, or its process ended: the end of the stream, or a reset). ReadFailed: the read
/// failed otherwise, and the other side may still hold its end (a channel's owner tells the two apart:
/// docs/protocol.md §9.7). Canceled: the watcher's stop (`endTask`).
pub const read: fn (io: Io, connection: Handle, buf: []u8) ReadError!usize = impl.read;

pub const ReadError = Io.Cancelable || error{ReadFailed};

/// A watcher's read result as `read` reports it: the end of the stream, and a reset (an end with data left unread),
/// are the other side's end (0); any other failure is ReadFailed.
pub fn readResult(result: Io.Operation.FileReadStreaming.Error!usize) error{ReadFailed}!usize {
    return result catch |err| switch (err) {
        error.EndOfStream, error.ConnectionResetByPeer => 0,
        else => error.ReadFailed,
    };
}

/// Only where `passes_segment_handle`: takes a SEGMENT frame into `frame` and its segment handle, without waiting
/// (after `readableWithin`). Exactly `frame.len` bytes and exactly one handle, else every received handle is closed.
pub const recvSegment: fn (connection: Handle, frame: []u8) RecvError!Handle = impl.recvSegment;

// === Waits on the caller's thread ===

/// What a wait on the caller's thread got: its operation's outcome; nothing (the time ran out, or the OS ended the wait
/// early: the caller looks at its own deadline and waits again if it hasn't passed); or the cancel signal.
pub fn Waited(comptime T: type) type {
    return union(enum) { done: T, idle, cancelled };
}

/// Waits up to `timeout_ms` (null: no limit; 0: only looks) on the caller's thread for bytes on the connection, or
/// for the cancel signal `cancel`, and reads what arrived into `buf`: the handshake's reads. `done` with 0: the
/// connection ended, or the read failed. No I/O is left in flight when it returns.
pub const readWithin: fn (connection: Handle, buf: []u8, cancel: ?Handle, timeout_ms: ?u32) Waited(usize) = impl.readWithin;

/// Only where `passes_segment_handle`: waits up to `timeout_ms` (null: no limit) on the caller's thread until the
/// connection has data or has ended, leaving it queued for `recvSegment`.
pub const readableWithin: fn (connection: Handle, timeout_ms: ?u32) Waited(void) = impl.readableWithin;

/// A listener's cancel signal: a handle that `setCancelSignal` sets once and for good, and that the listener's waits
/// watch besides their own operation (Linux: an eventfd; macOS: a kqueue with a user event; Windows: a manual-reset
/// event).
pub const createCancelSignal: fn () (ResourceError || UnexpectedError)!Handle = impl.createCancelSignal;

/// Sets the cancel signal: every wait that watches it, now or later, returns `cancelled` (Linux and macOS: also when
/// its own operation is ready, which the signal wins over; Windows: when its operation would wait). Any thread.
pub const setCancelSignal: fn (signal: Handle) void = impl.setCancelSignal;

/// Sleeps up to `timeout_ms`, or until the cancel signal is set: true if it is (a listener's back-off).
pub const waitCancelSignal: fn (signal: Handle, timeout_ms: u32) bool = impl.waitCancelSignal;

/// Sleeps `ms` milliseconds on the caller's thread (a client's back-off).
pub const sleep: fn (ms: u32) void = impl.sleep;

// === The poller: one thread that watches many connections (a broadcast's, a queue's or a service's members and
// joiners) ===

/// One thread's watch over many connections, each of which carries at most one frame and then its end (a broadcast's
/// subscribers, a queue's members, and the joiners of either before their JOIN), and over one listening handle, on
/// which those joiners arrive (`listen`): the poller task of a broadcast (broadcast/server.zig) or a queue
/// (queue/server.zig). Linux: an epoll instance, level-triggered, with an eventfd that stops it. macOS: a kqueue, with
/// a user event that stops it. Windows: reads that the polling thread issues itself (`ReadFileEx`), whose completion
/// routines run in its alertable waits, an auto-reset event that wakes it, and an overlapped `ConnectNamedPipe` on the
/// listening instance with a manual-reset event of its own; not a completion port, since `Io` reads on the same pipes
/// with completion routines of its own.
///
/// `init` creates its handles, under the handle lock where `has_fork`: the caller registers them (`handles`) and
/// closes them once the polling thread is done. `watch` and `stop` may run on any thread; `wait`, `take`, `unwatch`,
/// `listen`, `unlisten` and `drain` only on the polling thread.
pub const Poller = impl.Poller;

/// What a `Poller.wait` got: the watched connections readable or at their end, and whether a client waits on the
/// listening handle (`Poller.listen`), to accept without waiting (`acceptWithin` with timeout 0). Neither, when the
/// timeout ran out or the OS ended the wait early.
pub const Polled = struct {
    ready: []*Watched,
    listening: bool = false,
};

/// What a `Poller` keeps of one watched connection: `frame`, the bytes read so far. It stays at one address from
/// `watch` until `unwatch` (or the `drain` after `stop`).
pub const Watched = impl.Watched;

/// What a watched connection had, as `Poller.take` reads it: a part of its frame (kept; the poller watches on), its
/// whole frame (in `Watched.frame`), its end (the other side closed it, or its process ended), or a read that failed
/// otherwise, which may leave the other side holding its end.
pub const Taken = enum { partial, whole, ended, failed };

// === Segments ===

/// What a segment holds, which decides its objects where segments are named: a connection's two rings (four ring
/// events), a broadcast channel (the writer's room event; each slot's wake is an object of its own), or a work queue
/// or a service (none: each slot's wakes are objects of their own).
pub const SegmentKind = enum { stream, broadcast, queue, service };

/// A segment mapped into this process: `base` and `size` bytes, and its ring events. `unmap` releases the mapping and
/// what it holds; `ringEvent` gives the event of one ring sleeper (`names.Object`).
pub const Mapping = impl.Mapping;

/// Only where `passes_segment_handle`: a new segment's handle, to give `createSegment` and pass with SEGMENT.
pub const createSegmentHandle: fn () (ResourceError || UnexpectedError)!Handle = impl.createSegmentHandle;

/// Creates a segment of `size` zeroed bytes for session `id`, with its memory committed now (so a lack of it is
/// NO_MEMORY here, not a fault later) and, where segments are named, the user-only security and the objects of its
/// `kind`; then maps it. `handle` is `createSegmentHandle`'s where `passes_segment_handle`, else null.
pub const createSegment: fn (handle: ?Handle, id: *const [16]u8, size: usize, security: *const Security, kind: SegmentKind) CreateError!Mapping = impl.createSegment;

/// Maps the segment a SEGMENT or OFFER frame offered, once it can't change size and is exactly `size` bytes: the
/// received handle where `passes_segment_handle` (the caller closes it afterwards), else the segment of session `id`
/// and the objects of its `kind`.
pub const openSegment: fn (received: ?Handle, id: *const [16]u8, size: usize, kind: SegmentKind) OpenError!Mapping = impl.openSegment;

/// Makes the mapping's first `len` bytes (a multiple of 64 KiB) read-only for this process: a later write there faults,
/// a read doesn't. A broadcast subscriber's view of what only the channel's writer writes (docs/protocol.md §9.4).
/// SystemResources: the kernel can't split the mapping (Linux: the process's mapping count is at its limit).
pub const protectReadOnly: fn (mapping: *Mapping, len: usize) (ResourceError || UnexpectedError)!void = impl.protectReadOnly;

/// Makes the whole mapping inaccessible, at once, for every thread of this process: a later access faults, a futex
/// or `os_sync` call on a word in it fails without one, and `Mapping.unmap` still releases it. Used on a broadcast
/// subscriber dropped from its channel, whose application may still hold pointers into it.
pub const revoke: fn (mapping: *Mapping) void = impl.revoke;

// === Ring waits (stream/ring_wait.zig) ===

/// What wakes a ring sleeper in the other process, besides its sleep flag: a `Mapping`'s `ringEvent`.
pub const RingEvent = impl.RingEvent;

/// The ring event of a connection without a session.
pub const no_ring_event: RingEvent = impl.no_ring_event;

/// Sleeps while the ring sleep flag `flag` holds 1, for at most `timeout_ms` (at least 1). It returns on a wake, a
/// change of the flag, a signal or the timeout: the caller looks at the ring again whatever the reason. The one
/// blocking wait outside `Io`: the flag is in memory shared with the peer, which `Io`'s waits don't cover. Returns
/// whether the timeout ran out (only the test hooks' diagnostics read it).
pub const ringWait: fn (flag: *const std.atomic.Value(u32), event: RingEvent, timeout_ms: u32) bool = impl.ringWait;

/// Wakes the sleeper of `flag`, after its 1 was swapped to 0.
pub const ringWake: fn (flag: *const std.atomic.Value(u32), event: RingEvent) void = impl.ringWake;

// === Broadcast waits (a broadcast channel's subscribers) ===

/// How the writer wakes the subscribers whose armed bits it took (docs/protocol.md §9.5): each on its slot's own wake
/// (`setSlotWake`; Windows), else all at once on the shared word, after bumping it (`broadcastWake`; Linux, macOS).
pub const wakes_each_slot: bool = impl.wakes_each_slot;

/// What wakes one slot's sleeper and no other: Windows, a named auto-reset event of the slot (`names.slotObject`),
/// which the channel's owner creates when it admits the slot's member and the member opens; elsewhere nothing
/// (`void`).
pub const SlotWake = impl.SlotWake;

/// The owner's: the wake of slot `slot` of channel session `id`, its name's kind suffix `suffix` (`names.slotObject`),
/// with the user-only security. One that a former member of the slot still holds open is taken as it is: it can only
/// wake the new member early.
pub const createSlotWake: fn (id: *const [16]u8, suffix: []const u8, slot: u32, security: *const Security) (ResourceError || UnexpectedError)!SlotWake = impl.createSlotWake;

/// The member's: its slot's wake, which the owner created, or another slot's, to wake its sleeper. Gone: it no longer
/// exists (the owner freed the slot, or never had a member there).
pub const openSlotWake: fn (id: *const [16]u8, suffix: []const u8, slot: u32) OpenError!SlotWake = impl.openSlotWake;

/// A slot's wake as the event of a ring wait (`ringWait`, `ringWake`) on a sleep flag of the slot's own: Windows, the
/// slot's event (none: `ringWait` sleeps in short slices); elsewhere the flag alone, `no_ring_event`.
pub const slotRingEvent: fn (wake: ?SlotWake) RingEvent = impl.slotRingEvent;

pub const closeSlotWake: fn (wake: SlotWake) void = impl.closeSlotWake;

/// Ends the slot's sleeper's wait, or its next one (it stays set until a wait takes it): the writer's wake of an armed
/// slot where `wakes_each_slot`, and a subscriber's interrupt of its own wait. Any thread.
pub const setSlotWake: fn (wake: SlotWake) void = impl.setSlotWake;

/// Sleeps for at most `timeout_ms` (at least 1): Linux and macOS, while the shared word `word` holds `expected` (a
/// non-private futex wait; `os_sync_wait_on_address` with `SHARED`); Windows, until the slot's wake `own` is set. It
/// returns on a wake, a change of the word, a signal, a wake set earlier, or the timeout: the caller looks again
/// whatever the reason. Returns whether the timeout ran out. It never faults on a mapping revoked (`revoke`) before the
/// call: the kernel's read of the word fails instead (Linux, macOS), or the word isn't read (Windows). The caller never
/// revokes the word during the call: macOS loads the word itself when the kernel can't read it yet.
pub const broadcastWait: fn (word: *const std.atomic.Value(u32), expected: u32, own: SlotWake, timeout_ms: u32) bool = impl.broadcastWait;

/// Where `wakes_each_slot` is false: wakes every waiter of `word`, after it was bumped (a non-private `FUTEX_WAKE` of
/// all, `os_sync_wake_by_address_all`). Elsewhere nothing.
pub const broadcastWake: fn (word: *const std.atomic.Value(u32)) void = impl.broadcastWake;

// === For the tests' leak checks ===

/// The number of handles this process has open.
pub const openHandleCount: fn () usize = impl.openHandleCount;

/// The number of segment mappings in this process (Windows: of every mapped view).
pub const segmentMappingCount: fn () usize = impl.segmentMappingCount;

/// The number of threads this process has.
pub const threadCount: fn () usize = impl.threadCount;

/// A thread of this process, marked by itself (`ofCurrentThread`), to tell later whether it has exited (`exited`).
/// `deinit` releases what the mark holds (Windows: a handle to the thread).
pub const ThreadMark = impl.ThreadMark;

/// What this process may do with the page at an address (`protectReadOnly`, `revoke`).
pub const Protection = enum {
    none,
    read_only,
    read_write,
    other,

    pub fn of(readable: bool, writable: bool) Protection {
        if (readable and writable) return .read_write;
        if (readable) return .read_only;
        return if (writable) .other else .none;
    }
};

/// The protection of the page at `address` in this process; `none` where nothing is mapped.
pub const protectionOf: fn (address: *const anyopaque) Protection = impl.protectionOf;

comptime {
    // The types' parts of the contract
    const init: fn () UnexpectedError!Security = Security.init;
    const deinit: fn (*Security) void = Security.deinit;
    const lock: fn (*Lock) void = Lock.lock;
    const unlock: fn (*Lock) void = Lock.unlock;
    const unmap: fn (*Mapping) void = Mapping.unmap;
    const ringEventOf: fn (*const Mapping, names.Object) RingEvent = Mapping.ringEvent;
    _ = .{ init, deinit, lock, unlock, unmap, ringEventOf };
    const poller_init: fn () (ResourceError || UnexpectedError)!Poller = Poller.init;
    const poller_handles: fn (*const Poller) [2]?Handle = Poller.handles;
    const watch: fn (*Poller, *Watched, Handle) (ResourceError || UnexpectedError)!void = Poller.watch;
    const wait: fn (*Poller, []*Watched, ?u32) ?Polled = Poller.wait;
    const listen: fn (*Poller, Handle) (ResourceError || UnexpectedError)!void = Poller.listen;
    const unlisten: fn (*Poller) void = Poller.unlisten;
    const take: fn (*Poller, *Watched) Taken = Poller.take;
    const unwatch: fn (*Poller, *Watched) void = Poller.unwatch;
    const stop: fn (*Poller) void = Poller.stop;
    const drain: fn (*Poller) void = Poller.drain;
    _ = .{ poller_init, poller_handles, watch, wait, listen, unlisten, take, unwatch, stop, drain };
    const mark_thread: fn () ThreadMark = ThreadMark.ofCurrentThread;
    const exited: fn (ThreadMark) bool = ThreadMark.exited;
    const forget: fn (ThreadMark) void = ThreadMark.deinit;
    _ = .{ mark_thread, exited, forget };
    // What only some OSes have, wherever the capability says so
    if (has_fork) _ = .{ registerForkHandlers, keepImageLoaded };
    if (passes_segment_handle) _ = .{ createSegmentHandle, readableWithin, recvSegment };
    std.debug.assert(@FieldType(Watched, "frame") == wire.Frame);
    std.debug.assert(@FieldType(Mapping, "size") == usize);
    std.debug.assert(@typeInfo(@FieldType(Mapping, "base")).pointer.size == .many);
}

test {
    _ = impl; // the building OS's own tests
    _ = @import("platform/contract_test.zig");
}
