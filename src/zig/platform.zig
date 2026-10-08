//! The OS contract: everything FastIPC needs from the operating system, in FastIPC's terms. The rest of the library
//! imports this file only; `platform/linux.zig`, `platform/macos.zig` and `platform/windows.zig` implement it, and
//! `platform/darwin.zig` and `platform/win32.zig` (raw macOS and Windows declarations) are private to `macos.zig` and
//! `windows.zig`. `platform/macos_test_sys.zig` is test code only: the system calls of the fork tests on macOS.
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
//!   - a **connection** is a local, reliable, bidirectional byte stream between the two processes (the control
//!     connection): it carries two 64-byte frames (SEGMENT, READY), then nothing, and its end tells each side that
//!     the other's process closed it or ended;
//!   - a **segment** is the shared memory of one connection, created by the listener and mapped by the client;
//!   - a **ring event** wakes a thread sleeping on a ring's sleep flag, in the other process.

const std = @import("std");
const builtin = @import("builtin");
const Io = std.Io;
const names = @import("lifecycle/names.zig");

const impl = switch (builtin.os.tag) {
    .linux => @import("platform/linux.zig"),
    .windows => @import("platform/windows.zig"),
    .macos => @import("platform/macos.zig"),
    else => @compileError("FastIPC supports Linux, macOS and Windows"),
};

// === Capabilities: real differences in behavior ===

/// The process can fork with the library's state in it (Linux, macOS). Then every handle the library owns is
/// registered in the fork registry (process_state.zig), and a forked child closes its copies and marks its connections
/// inherited, so it can't disturb its parent's connections.
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
/// (process_state.zig), so that a child forked later inherits them done. Nothing where there are none.
pub const prepareForForks: fn () void = if (@hasDecl(impl, "prepareForForks")) impl.prepareForForks else struct {
    fn none() void {}
}.none;

/// This process's id.
pub const pid: fn () u32 = impl.pid;

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
/// Where the OS forks, it sets `forks` (if given) to the fork count (process_state.zig) just before the accept itself,
/// after the wait: only a fork after that can have copied the new connection into a child.
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

/// Closes a handle.
pub const close: fn (handle: Handle) void = impl.close;

// === Frames on a connection ===

/// Sends one frame, without blocking (a frame fits an empty buffer) and without a signal if the peer is gone; with
/// the segment's handle `attached` (only where `passes_segment_handle`, else null). False: the peer is gone, or the
/// send failed.
pub const sendFrame: fn (io: Io, connection: Handle, frame: []const u8, attached: ?Handle) Io.Cancelable!bool = impl.sendFrame;

/// Reads through `io` into `buf`, blocking until at least one byte arrives: the watcher's read. 0: the connection
/// ended, or the read failed, which the watcher treats alike.
pub const read: fn (io: Io, connection: Handle, buf: []u8) Io.Cancelable!usize = impl.read;

/// Waits up to `timeout_ms` (null: no limit; 0: only looks) on the caller's thread for bytes on the connection, or
/// for the cancel signal `cancel`, and reads what arrived into `buf`: the handshake's reads. `done` with 0: the
/// connection ended, or the read failed. No I/O is left in flight when it returns.
pub const readWithin: fn (connection: Handle, buf: []u8, cancel: ?Handle, timeout_ms: ?u32) Waited(usize) = impl.readWithin;

/// Only where `passes_segment_handle`: waits up to `timeout_ms` (null: no limit) on the caller's thread until the
/// connection has data or has ended, leaving it queued for `recvSegment`.
pub const readableWithin: fn (connection: Handle, timeout_ms: ?u32) Waited(void) = impl.readableWithin;

/// Only where `passes_segment_handle`: takes a SEGMENT frame into `frame` and its segment handle, without waiting
/// (after `readableWithin`). Exactly `frame.len` bytes and exactly one handle, else every received handle is closed.
pub const recvSegment: fn (connection: Handle, frame: []u8) RecvError!Handle = impl.recvSegment;

// === Waits on the caller's thread ===

/// What a wait on the caller's thread got: its operation's outcome; nothing (the time ran out, or the OS ended the wait
/// early: the caller looks at its own deadline and waits again if it hasn't passed); or the cancel signal.
pub fn Waited(comptime T: type) type {
    return union(enum) { done: T, idle, cancelled };
}

/// A listener's cancel signal: a handle that `setCancelSignal` sets once and for good, and that the listener's waits
/// watch besides their own operation (Linux: an eventfd; macOS: a kqueue with a user event; Windows: a manual-reset
/// event).
pub const createCancelSignal: fn () (ResourceError || UnexpectedError)!Handle = impl.createCancelSignal;

/// Sets the cancel signal: every wait that watches it, now or later, returns `cancelled`. Any thread.
pub const setCancelSignal: fn (signal: Handle) void = impl.setCancelSignal;

/// Sleeps up to `timeout_ms`, or until the cancel signal is set: true if it is (a listener's back-off).
pub const waitCancelSignal: fn (signal: Handle, timeout_ms: u32) bool = impl.waitCancelSignal;

/// Sleeps `ms` milliseconds on the caller's thread (a client's back-off).
pub const sleep: fn (ms: u32) void = impl.sleep;

// === Segments ===

/// A segment mapped into this process: `base` and `size` bytes, and its ring events. `unmap` releases the mapping and
/// what it holds; `ringEvent` gives the event of one ring sleeper (`names.Object`).
pub const Mapping = impl.Mapping;

/// Only where `passes_segment_handle`: a new segment's handle, to give `createSegment` and pass with SEGMENT.
pub const createSegmentHandle: fn () (ResourceError || UnexpectedError)!Handle = impl.createSegmentHandle;

/// Creates a segment of `size` zeroed bytes for session `id`, with its memory committed now (so a lack of it is
/// NO_MEMORY here, not a fault later) and, where segments are named, the user-only security; then maps it. `handle`
/// is `createSegmentHandle`'s where `passes_segment_handle`, else null.
pub const createSegment: fn (handle: ?Handle, id: *const [16]u8, size: usize, security: *const Security) CreateError!Mapping = impl.createSegment;

/// Maps the segment a SEGMENT frame offered, once it can't change size and is exactly `size` bytes: the received
/// handle where `passes_segment_handle` (the caller closes it afterwards), else the segment of session `id`.
pub const openSegment: fn (received: ?Handle, id: *const [16]u8, size: usize) OpenError!Mapping = impl.openSegment;

// === Ring waits (data/ring_wait.zig) ===

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

// === For the tests' leak checks ===

/// The number of handles this process has open.
pub const openHandleCount: fn () usize = impl.openHandleCount;

/// The number of segment mappings in this process (Windows: of every mapped view).
pub const segmentMappingCount: fn () usize = impl.segmentMappingCount;

comptime {
    // The types' parts of the contract
    const init: fn () UnexpectedError!Security = Security.init;
    const deinit: fn (*Security) void = Security.deinit;
    const lock: fn (*Lock) void = Lock.lock;
    const unlock: fn (*Lock) void = Lock.unlock;
    const unmap: fn (*Mapping) void = Mapping.unmap;
    const ringEventOf: fn (*const Mapping, names.Object) RingEvent = Mapping.ringEvent;
    _ = .{ init, deinit, lock, unlock, unmap, ringEventOf };
    std.debug.assert(@FieldType(Mapping, "size") == usize);
    std.debug.assert(@typeInfo(@FieldType(Mapping, "base")).pointer.size == .many);
}

test {
    _ = impl; // the building OS's own tests
}
