//! The corrupt-peer fuzz harness (CONTRIBUTING.md, "Tests"): a heavy, opt-in check
//! that a corrupt peer can only make a library connection return an error, end the session or wait out its timeout -
//! never crash, abort, hang past a timeout, or touch memory outside its mapping. `python devtool.py test fuzz`
//! (`zig build fuzz`).
//!
//! The harness plays the peer by hand, through the library's own internals (fastipc.internal: the name derivation,
//! the control frames, the segment attach and the OS rendezvous), against a real library listener or client (the
//! native API) in the same process, on a fresh name each round.
//!
//! Control-plane cases: the corrupt peer listens on the name and a library client connects to
//! it, so the library parses a malformed SEGMENT - a bad magic, type or version, a READY where the SEGMENT belongs
//! (out of order), a capacity that doesn't match the segment size, a huge capacity, a wrong segment size, a frame cut
//! short, zero or (Linux, macOS) two descriptors / (Windows) a section id that names nothing, and a real, valid
//! segment whose header names another session (a wrong session id, caught at attach's last check). Each must make the
//! client's connect return an error (INVALID for another version; TIMEOUT while it retries a corrupt peer), with no
//! crash. A corrupt client that completes a valid handshake with a library listener then sends a frame after READY,
//! which must end the session cleanly (DISCONNECTED). Any failure fails the run.
//!
//! Ring-content cases: after a valid handshake the corrupt client writes the segment - random bytes everywhere,
//! random ring content under sane indices, plausible but random pieces, or all of that from a thread while the library
//! works - and the library's accepted connection receives (copying and zero-copy) and sends (copying and zero-copy).
//! Each call must return (any result) within its timeout, or, once it has started a message of several pieces (which
//! completes past the timeout), when the trial cancels the connection: the data path checks every piece and index
//! (docs/protocol.md §5.3), and an abort, a panic, a crash or a hang fails the round. Each round runs in a child
//! process of its own (`fuzz --ring-child <seed>` repeats one), so a failure is reported with its seed and the run
//! goes on.
//!
//! Usage: fuzz [--rounds N] [--seed S].

const std = @import("std");
const builtin = @import("builtin");
const Io = std.Io;
const fastipc = @import("fastipc");
const internal = fastipc.internal;
const wire = internal.wire;
const segment = internal.segment;
const os = internal.platform;

const gpa = std.heap.c_allocator;
const capacity = 4096; // a small ring: the smallest segments to attach
const short: Io.Timeout = .{ .duration = .{ .raw = .fromMilliseconds(500), .clock = .awake } };

var io: Io = undefined;
var self_exe: []const u8 = undefined; // this program, to re-spawn for an isolated ring-content trial

/// A malformed SEGMENT the corrupt peer (a raw listener) sends to a library client. Each must be refused: another
/// version fails the connect (INVALID), a corrupt frame or segment is retried until the connect times out; neither
/// may crash or hang. The cases: bad magic, type or version, frames out of order, huge capacities, wrong
/// session ids, zero or two descriptors, EOF mid-frame (and, below, frames after READY).
const Corruption = enum {
    bad_magic,
    bad_type,
    /// A READY frame where SEGMENT is expected.
    out_of_order,
    bad_version,
    /// A capacity that doesn't match the offered segment size.
    wrong_capacity,
    /// Capacity and segment size at the top of u64.
    huge_capacity,
    wrong_size,
    eof_midframe,
    /// Where the segment's handle travels with the frame: none with an otherwise-valid frame. Otherwise: a session id
    /// that names no segment.
    bad_handle,
    /// Two segment handles with the frame (both must be closed): only where the handle travels with it.
    two_descriptors,
    /// A real, valid segment whose header names another session than the frame: refused at attach's last check.
    wrong_session,

    fn applies(c: Corruption) bool {
        return !(c == .two_descriptors and !os.passes_segment_handle);
    }
};

pub fn main(init: std.process.Init) void {
    io = init.io;
    const args = init.minimal.args.toSlice(init.arena.allocator()) catch std.process.exit(90);
    self_exe = if (args.len > 0) args[0] else "fuzz";
    var rounds: u32 = 10;
    var seed: u64 = 1;
    var i: usize = 1;
    while (i < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], "--ring-child") and i + 1 < args.len) {
            // An isolated ring-content trial: run one in this child and exit, so an abort here is contained and the
            // parent can report it instead of dying.
            const s = std.fmt.parseInt(u64, args[i + 1], 10) catch std.process.exit(90);
            std.process.exit(ringTrial(s));
        } else if (std.mem.eql(u8, args[i], "--rounds") and i + 1 < args.len) {
            i += 1;
            rounds = std.fmt.parseInt(u32, args[i], 10) catch std.process.exit(90);
        } else if (std.mem.eql(u8, args[i], "--seed") and i + 1 < args.len) {
            i += 1;
            seed = std.fmt.parseInt(u64, args[i], 10) catch std.process.exit(90);
        } else {
            std.debug.print("fuzz: usage: fuzz [--rounds N] [--seed S]\n", .{});
            std.process.exit(90);
        }
    }
    std.process.exit(run(rounds, seed));
}

fn run(rounds: u32, seed: u64) u8 {
    std.debug.print("fuzz: {d} rounds/case, base seed {d}: control plane and ring content\n", .{ rounds, seed });
    var counter: u64 = seed;
    var failures: u32 = 0;

    for (std.enums.values(Corruption)) |corruption| {
        if (!corruption.applies()) {
            std.debug.print("  {s:<16} n/a on this OS\n", .{@tagName(corruption)});
            continue;
        }
        var pass: u32 = 0;
        for (0..rounds) |_| {
            counter +%= 1;
            if (clientCase(corruption, counter)) pass += 1;
        }
        std.debug.print("  {s:<16} {d}/{d} refused cleanly\n", .{ @tagName(corruption), pass, rounds });
        if (pass != rounds) failures += 1;
    }

    // A frame after READY ends the session, cleanly
    {
        var pass: u32 = 0;
        for (0..rounds) |_| {
            counter +%= 1;
            if (afterReadyCase(counter)) pass += 1;
        }
        std.debug.print("  {s:<16} {d}/{d} ended the session cleanly\n", .{ "frame-after-ready", pass, rounds });
        if (pass != rounds) failures += 1;
    }

    // Ring content: each mode `rounds` times; a trial's seed picks its mode
    const modes = std.enums.values(RingMode);
    for (modes) |mode| {
        var clean: u32 = 0;
        for (0..rounds) |_| {
            counter +%= 1;
            if (ringContentCase(counter * modes.len + @backingInt(mode))) clean += 1;
        }
        std.debug.print("  ring-{s:<11} {d}/{d} returned cleanly\n", .{ @tagName(mode), clean, rounds });
        if (clean != rounds) failures += 1;
    }

    if (failures != 0) {
        std.debug.print("fuzz: FAIL: {d} case(s) were not handled cleanly\n", .{failures});
        return 1;
    }
    std.debug.print("fuzz: PASS\n", .{});
    return 0;
}

// === The rendezvous, by hand (the corrupt peer's raw listener / client) ===

/// A raw listener claiming the caller's name, and the connection it accepts: what the corrupt peer holds.
const RawPeer = struct {
    listener: os.Handle,
    conn: ?os.Handle = null,
    security: os.Security,

    fn claim(name: []const u8) ?RawPeer {
        var security = os.Security.init() catch return null;
        const address = os.addressOf(name) catch {
            security.deinit();
            return null;
        };
        const listening = (os.claim(&address, &security) catch null) orelse {
            security.deinit();
            return null;
        };
        return .{ .listener = listening, .security = security };
    }

    /// Accepts the library client's connection, within 5 s.
    fn accept(peer: *RawPeer) bool {
        peer.conn = switch (os.acceptWithin(peer.listener, null, 5000, null)) {
            .done => |outcome| switch (outcome) {
                .connection => |h| h,
                else => return false,
            },
            else => return false,
        };
        return true;
    }

    /// Sends `bytes` on the connection, with `attached` as the segment's handle when given.
    fn send(peer: *RawPeer, bytes: []const u8, attached: ?os.Handle) void {
        _ = os.sendFrame(io, peer.conn.?, bytes, attached) catch {};
    }

    fn deinit(peer: *RawPeer) void {
        // A client that connected on the listening handle itself (a pipe instance) has no handle of its own
        if (peer.conn) |c| if (c != peer.listener) os.close(c);
        os.close(peer.listener);
        peer.security.deinit();
    }
};

fn uniqueName(buf: []u8, counter: u64) []const u8 {
    return std.mem.print(buf, "fz-{d}-{d}", .{ os.pid(), counter }) catch unreachable;
}

// === Control-plane cases ===

/// The corrupt peer listens on the name; a library client connects and must refuse the malformed SEGMENT. Returns true
/// when the connect returned an error (a clean refusal) without a crash or hang.
fn clientCase(corruption: Corruption, counter: u64) bool {
    var name_buf: [64]u8 = undefined;
    const name = uniqueName(&name_buf, counter);
    var peer = RawPeer.claim(name) orelse return false;
    defer peer.deinit();

    const Ctx = struct {
        peer: *RawPeer,
        corruption: Corruption,
        fn go(ctx: *@This()) void {
            if (!ctx.peer.accept()) return;
            sendCorrupt(ctx.peer, ctx.corruption);
        }
    };
    var ctx: Ctx = .{ .peer = &peer, .corruption = corruption };
    const thread = std.Thread.spawn(.{}, Ctx.go, .{&ctx}) catch return false;
    defer thread.join();

    // A malformed SEGMENT must never complete the handshake: the connect returns an error (Invalid for another
    // version, Timeout while the client backs off and retries a corrupt peer). A connection would mean the library
    // accepted it.
    const conn = fastipc.Conn.connect(io, gpa, name, short) catch return true;
    conn.close();
    std.debug.print("fuzz:   {s}: connect returned a connection for a malformed SEGMENT\n", .{@tagName(corruption)});
    return false;
}

fn offerFrame(session_id: [16]u8) wire.Frame {
    return wire.encodeSegment(.{
        .pid = 4242,
        .capacity = capacity,
        .segment_size = segment.size(capacity),
        .session_id = session_id,
    });
}

/// Builds and sends the malformed SEGMENT for `corruption` on `peer`'s connection.
fn sendCorrupt(peer: *RawPeer, corruption: Corruption) void {
    var frame = offerFrame(.{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 });
    switch (corruption) {
        .bad_magic => frame[0] = 'X',
        .bad_type => frame[4] = 9,
        .out_of_order => frame = wire.encodeReady(4242),
        .bad_version => frame[5] = 99,
        .wrong_capacity => std.mem.writeInt(u64, frame[16..24], capacity * 2, .little),
        .huge_capacity => {
            std.mem.writeInt(u64, frame[16..24], std.math.maxInt(u64), .little);
            std.mem.writeInt(u64, frame[24..32], std.math.maxInt(u64), .little);
        },
        .wrong_size => std.mem.writeInt(u64, frame[24..32], segment.size(capacity) + 4096, .little),
        .eof_midframe => {
            peer.send(frame[0..10], null); // a partial frame, then the connection is dropped
            return;
        },
        .bad_handle => {}, // the valid frame with no handle, or a session id that names nothing
        .two_descriptors => {
            if (os.passes_segment_handle) {
                const a = os.createSegmentHandle() catch return;
                defer os.close(a);
                const b = os.createSegmentHandle() catch return;
                defer os.close(b);
                sendWithFds(peer.conn.?, &frame, .{ a, b });
            }
            return;
        },
        .wrong_session => {
            sendWrongSession(peer);
            return;
        },
    }
    if (!os.passes_segment_handle or corruption == .bad_handle) {
        peer.send(&frame, null); // with a passed handle: none, and the client rejects it (DescriptorCount)
    } else {
        // The frame-level corruptions must reach the frame's checks, so the receive needs its one handle: a throwaway
        // segment handle the client never maps (it fails at the mismatch or the protocol error first).
        const handle = os.createSegmentHandle() catch return;
        defer os.close(handle);
        peer.send(&frame, handle);
    }
}

/// A real, valid segment (a sealed memfd / a committed section and its events) whose header's session id no longer
/// matches the frame's: the client maps it and must refuse it at the last attach check (§3 "Attach").
fn sendWrongSession(peer: *RawPeer) void {
    const handle: ?os.Handle = if (os.passes_segment_handle) os.createSegmentHandle() catch return else null;
    defer if (handle) |h| os.close(h);
    var session = segment.create(io, handle, &peer.security, capacity) catch return;
    defer session.unmap();
    const frame = offerFrame(session.id);
    session.header().session_id[0] ^= 0xFF; // a sealed memfd forbids resizing, not writing
    peer.send(&frame, handle); // a passed handle: the client gets its own reference
    // A named segment goes away with its last handle: keep it until the client has opened it, which it does as soon
    // as it reads the frame (it would otherwise find it gone: a clean refusal too, but not the one this case is for)
    if (!os.passes_segment_handle) sleep(300);
}

/// Sends `frame` with two descriptors in one SCM_RIGHTS message (the platform layer sends at most one), laid out as
/// platform/linux.zig's control message: a cmsghdr, then the descriptors. Only where the segment's handle travels
/// with the frame (Linux, macOS).
fn sendWithFds(conn: os.Handle, frame: []const u8, fds: [2]os.Handle) void {
    if (builtin.os.tag == .macos) return sendWithFdsMacos(conn, frame, fds);
    const linux = std.os.linux;
    const iov = [1]std.posix.iovec_const{.{ .base = frame.ptr, .len = frame.len }};
    var control: extern struct { header: linux.cmsghdr, fds: [2]os.Handle } = .{
        .header = .{ .len = @sizeOf(linux.cmsghdr) + 2 * @sizeOf(os.Handle), .level = linux.SOL.SOCKET, .type = linux.SCM.RIGHTS },
        .fds = fds,
    };
    const msg: linux.msghdr_const = .{
        .name = null,
        .namelen = 0,
        .iov = &iov,
        .iovlen = 1,
        .control = &control,
        .controllen = @sizeOf(@TypeOf(control)),
        .flags = 0,
    };
    _ = linux.sendmsg(conn, &msg, linux.MSG.NOSIGNAL | linux.MSG.DONTWAIT);
}

/// `sendWithFds` on macOS, laid out as platform/macos.zig's control message: macOS's 12-byte cmsghdr, aligned to 4
/// bytes, then the descriptors at once.
fn sendWithFdsMacos(conn: os.Handle, frame: []const u8, fds: [2]os.Handle) void {
    const c = std.c;
    const iov = [1]std.posix.iovec_const{.{ .base = frame.ptr, .len = frame.len }};
    var control: extern struct { header: c.cmsghdr, fds: [2]os.Handle } = .{
        .header = .{ .len = @sizeOf(c.cmsghdr) + 2 * @sizeOf(os.Handle), .level = c.SOL.SOCKET, .type = c.SCM.RIGHTS },
        .fds = fds,
    };
    const msg: c.msghdr_const = .{
        .name = null,
        .namelen = 0,
        .iov = &iov,
        .iovlen = 1,
        .control = &control,
        .controllen = @sizeOf(@TypeOf(control)),
        .flags = 0,
    };
    _ = c.sendmsg(conn, &msg, c.MSG.NOSIGNAL | c.MSG.DONTWAIT);
}

// === A valid corrupt handshake: the corrupt client plays it straight, then misbehaves ===

const Connected = struct {
    conn: os.Handle,
    session: segment.Session,
};

/// The corrupt peer as a client: connect, read the listener's SEGMENT, attach the real segment, send a valid READY.
/// Returns the mapped session, or null on any failure.
fn corruptConnect(name: []const u8) ?Connected {
    var security = os.Security.init() catch return null;
    defer security.deinit();
    const address = os.addressOf(name) catch return null;
    const conn = for (0..200) |_| {
        switch (os.connect(&address, &security) catch return null) {
            .connected => |h| break h,
            .busy, .no_listener => sleep(5),
            .foreign => return null,
        }
    } else return null;
    var frame: wire.Frame = undefined;
    var received: ?os.Handle = null;
    if (os.passes_segment_handle) {
        if (os.readableWithin(conn, 5000) != .done) return null;
        received = os.recvSegment(conn, &frame) catch return null;
    } else if (!readFull(conn, &frame)) return null;
    defer if (received) |h| os.close(h);
    const offer = switch (wire.checkSegment(&frame)) {
        .valid => |o| o,
        else => return null,
    };
    const session = segment.attach(received, &offer) catch return null;
    const ready = wire.encodeReady(4242);
    if (!(os.sendFrame(io, conn, &ready, null) catch return null)) return null;
    return .{ .conn = conn, .session = session };
}

fn connClose(c: Connected) void {
    var session = c.session;
    session.unmap();
    os.close(c.conn);
}

/// A library listener on a fresh name, and the connection it accepts from the corrupt client after a valid handshake.
const Accepted = struct {
    listener: fastipc.Listener,
    conn: fastipc.Conn,
    corrupt: Connected,

    fn open(name: []const u8) ?Accepted {
        const listener = fastipc.Listener.listen(io, gpa, name, capacity) catch return null;
        // The corrupt client on a thread of its own: its handshake goes on while the listener accepts
        const Client = struct {
            fn run(n: []const u8, out: *?Connected) void {
                out.* = corruptConnect(n);
            }
        };
        var connected: ?Connected = null;
        const thread = std.Thread.spawn(.{}, Client.run, .{ name, &connected }) catch {
            listener.close();
            return null;
        };
        // The valid READY must connect the listener's client
        const accepted = listener.accept(short);
        thread.join();
        const corrupt = connected orelse {
            if (accepted) |conn| conn.close() else |_| {}
            listener.close();
            return null;
        };
        const conn = accepted catch {
            connClose(corrupt);
            listener.close();
            return null;
        };
        return .{ .listener = listener, .conn = conn, .corrupt = corrupt };
    }

    fn close(a: Accepted) void {
        a.conn.close();
        a.listener.close();
    }
};

/// After a valid handshake the corrupt client sends a frame; the library's connection must end its session cleanly
/// (a receive returns Disconnected), never crash. Gated.
fn afterReadyCase(counter: u64) bool {
    var name_buf: [64]u8 = undefined;
    const name = uniqueName(&name_buf, counter);
    const accepted = Accepted.open(name) orelse return false;
    defer accepted.close();

    // A frame where none is expected (§5.5 Watching) ends the session.
    const garbage = @as([wire.frame_len]u8, @splat(0xAB));
    _ = os.sendFrame(io, accepted.corrupt.conn, &garbage, null) catch {};
    connClose(accepted.corrupt);
    // The library delivers anything real, then reports the end; a receive must return Disconnected, not hang or crash.
    var buf: [64]u8 = undefined;
    _ = accepted.conn.recv(&buf, .{ .duration = .{ .raw = .fromSeconds(2), .clock = .awake } }) catch |err| {
        return err == error.Disconnected;
    };
    return false; // recv returned data: the session did not end
}

/// How a ring-content trial's corrupt client writes the segment; the trial's seed picks the mode.
const RingMode = enum {
    /// Random bytes over the whole segment: header, indices, sleep flags and data.
    bytes,
    /// Random ring data under sane indices: the tail anywhere, the head at most a ring ahead.
    data,
    /// Plausible pieces, one after another as a writer publishes them, with random flags and lengths.
    fragments,
    /// The two above and random data, again and again from a thread, while the library works on the rings.
    racing,
};

/// Runs one ring-content trial in a child process (`fuzz --ring-child <seed>`) and reports whether it returned
/// cleanly: exit code 0. An abort, a panic or a crash ends the child otherwise, and so does its own watchdog if a call
/// hangs (exit code 3); a failure names its seed.
fn ringContentCase(seed: u64) bool {
    var seed_buf: [24]u8 = undefined;
    const seed_str = std.mem.print(&seed_buf, "{d}", .{seed}) catch return false;
    var child = std.process.spawn(io, .{
        .argv = &.{ self_exe, "--ring-child", seed_str },
        .stdout = .ignore,
        .stderr = .ignore,
    }) catch return false;
    const term = child.wait(io) catch return false;
    if (term == .exited and term.exited == 0) return true;
    std.debug.print("fuzz:   ring seed {d}: {any} (repeat it: fuzz --ring-child {d})\n", .{ seed, term, seed });
    return false;
}

/// A trial whose calls haven't all returned by then hangs: its watchdog ends the child with exit code 3.
const trial_limit_ms = 10_000;

/// One ring-content trial, in-process (the child): after a valid handshake the corrupt client writes the segment
/// (`RingMode`), and the library's accepted connection makes each kind of data call a few times, with short timeouts.
/// Any call may fail; each must return. 0 once they all have.
fn ringTrial(seed: u64) u8 {
    _ = std.Thread.spawn(.{}, watchdog, .{}) catch return 4;
    var name_buf: [64]u8 = undefined;
    const name = uniqueName(&name_buf, seed);
    const accepted = Accepted.open(name) orelse return 0;
    defer accepted.close();
    defer connClose(accepted.corrupt);

    const modes = std.enums.values(RingMode);
    var peer: CorruptWriter = .{ .session = accepted.corrupt.session, .rng = .init(seed) };
    var stop = std.atomic.Value(bool).init(false);
    var racer: ?std.Thread = null;
    switch (modes[@intCast(seed % modes.len)]) {
        .bytes => peer.scribbleAll(),
        .data => peer.scribbleData(),
        .fragments => peer.writeFragments(),
        .racing => racer = std.Thread.spawn(.{}, CorruptWriter.race, .{ &peer, &stop }) catch return 4,
    }
    defer if (racer) |thread| {
        stop.store(true, .release);
        thread.join();
    };

    const conn = accepted.conn;
    // A receive that started a message the corrupt client never completes waits for its rest past its timeout; the
    // connection's cancel ends that wait
    var calls_done = std.atomic.Value(bool).init(false);
    const canceller = std.Thread.spawn(.{}, cancelAfter, .{ conn, &calls_done }) catch return 4;
    defer canceller.join();
    defer calls_done.store(true, .release);

    const quick: Io.Timeout = .{ .duration = .{ .raw = .fromMilliseconds(50), .clock = .awake } };
    var buf: [2 * capacity]u8 = undefined;
    for (0..8) |_| {
        _ = conn.recv(&buf, quick) catch {};
        if (conn.recvAcquire(quick)) |_| conn.release() else |_| {}
        conn.send("fuzz", quick) catch {};
        if (conn.acquire(16, quick)) |buffer| {
            @memset(buffer, 0x5A);
            conn.commit(buffer.len) catch {};
        } else |_| {}
    }
    return 0;
}

/// Cancels `conn` once the trial's calls have run for `cancel_after_ms`, unless they are done by then.
fn cancelAfter(conn: fastipc.Conn, done: *const std.atomic.Value(bool)) void {
    var waited: u32 = 0;
    while (!done.load(.acquire)) : (waited += 10) {
        if (waited >= cancel_after_ms) return conn.cancel();
        sleep(10);
    }
}

/// Longer than the trial's calls take when none waits past its timeout (8 rounds of four 50 ms calls).
const cancel_after_ms = 3000;

fn watchdog() void {
    sleep(trial_limit_ms);
    std.process.exit(3);
}

/// The corrupt client's writes into the segment it shares with the library's connection. The library listens, so the
/// corrupt client writes the c2s ring, which the library reads.
const CorruptWriter = struct {
    session: segment.Session,
    rng: std.Random.DefaultPrng,

    fn ring(w: *CorruptWriter) *internal.abi.RingCtrl {
        return &w.session.header().c2s;
    }

    fn ringData(w: *CorruptWriter) []u8 {
        return (w.session.data() + capacity)[0..capacity];
    }

    fn scribbleAll(w: *CorruptWriter) void {
        const bytes: [*]u8 = @ptrCast(w.session.header());
        w.rng.random().bytes(bytes[0..segment.size(capacity)]);
    }

    fn scribbleData(w: *CorruptWriter) void {
        const random = w.rng.random();
        random.bytes(w.ringData());
        const tail = random.int(u64);
        w.ring().reader.tail.store(tail, .release);
        w.ring().writer.head.store(tail +% random.uintAtMost(u64, capacity), .release);
    }

    /// Pieces from the head on, as a writer publishes them (each frame padded to 16 bytes), until the ring is nearly
    /// full: flags of any mix, lengths mostly within a ring and sometimes not, total lengths small, huge or about a
    /// ring's.
    fn writeFragments(w: *CorruptWriter) void {
        const random = w.rng.random();
        const data = w.ringData();
        const tail = w.ring().reader.tail.load(.acquire);
        var head = w.ring().writer.head.load(.acquire);
        while (head -% tail <= capacity - 16 - 64) {
            const frag_len: u32 = switch (random.uintLessThan(u8, 8)) {
                0 => random.int(u32),
                1 => capacity - 16 + random.uintAtMost(u32, 32),
                else => random.uintAtMost(u32, 64),
            };
            const total_len: u64 = switch (random.uintLessThan(u8, 4)) {
                0 => random.int(u64),
                1 => @as(u64, capacity) + random.uintAtMost(u64, 128) -% 64,
                else => random.uintAtMost(u64, 512),
            };
            const hdr: internal.abi.FragHdr = .{ .flags = random.uintLessThan(u32, 8), .frag_len = frag_len, .total_len = total_len };
            const payload: u64 = @min(frag_len, capacity - 16 - (head -% tail));
            for (std.mem.asBytes(&hdr), 0..) |byte, k| data[@intCast((head +% k) % capacity)] = byte;
            for (0..payload) |k| data[@intCast((head +% 16 +% k) % capacity)] = random.int(u8);
            head +%= std.mem.alignForward(u64, 16 + payload, 16);
            w.ring().writer.head.store(head, .release);
        }
    }

    fn race(w: *CorruptWriter, stop: *std.atomic.Value(bool)) void {
        while (!stop.load(.acquire)) {
            switch (w.rng.random().uintLessThan(u8, 3)) {
                0 => w.scribbleData(),
                1 => w.writeFragments(),
                else => w.rng.random().bytes(w.ringData()),
            }
        }
    }
};

// === helpers ===

/// Reads exactly `frame.len` bytes; false on EOF.
fn readFull(conn: os.Handle, frame: []u8) bool {
    var got: usize = 0;
    while (got < frame.len) {
        const n = os.read(io, conn, frame[got..]) catch return false;
        if (n == 0) return false;
        got += n;
    }
    return true;
}

fn sleep(ms: i64) void {
    io.sleep(.fromMilliseconds(ms), .awake) catch {};
}
