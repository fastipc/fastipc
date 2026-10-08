//! The soak harness (CONTRIBUTING.md, "Tests"): a heavy, opt-in check that the
//! library's resource use stays flat over a long run. `python devtool.py test soak [--seconds N]` (`zig build soak`).
//!
//! One process pairs a listener and a client of its own on one name through the native API (`Listener`,
//! `Conn`; the client connects on a thread of its own, since `connect` waits for the listener's `accept`) and, for a
//! bounded time, runs two workloads and samples its own resources:
//!   1. churn: listen, connect, accept, exchange one message each way, close both connections and the listener - a
//!      connection's whole life;
//!   2. ping-pong: one long-lived connection, small messages bounced back and forth - the steady data path.
//! It samples RSS, thread count and open descriptors (Linux, macOS) / handles (Windows) after a warm-up, then again
//! throughout; a growth beyond a small slack fails the run (exit 1), which is how a leaked segment, descriptor or
//! worker thread would show. The smoke run is ~120 s (`--seconds`); the full run (>= 100,000 cycles and 10^7 round
//! trips) is heavy validation: `--seconds 1800` (about 30 min per OS).

const std = @import("std");
const builtin = @import("builtin");
const Io = std.Io;
const fastipc = @import("fastipc");

const is_windows = builtin.os.tag == .windows;
const is_macos = builtin.os.tag == .macos;
const linux = std.os.linux;
const Writer = std.Io.Writer;
const gpa = std.heap.c_allocator;
const capacity = 1 << 20; // a typical RPC ring: 1 MiB

/// A bounded timeout, so a bug fails the run rather than hanging it.
const bound: Io.Timeout = .{ .duration = .{ .raw = .fromSeconds(10), .clock = .awake } };

var io: Io = undefined;

pub fn main(init: std.process.Init) void {
    // An `Io` of its own over `c_allocator`, as the C API's: in Debug, `init.io` allocates through std's
    // `SafeAllocator`, whose stack trace per allocation grows memory on macOS (Zig 0.17.0's unwinder never frees
    // into its debug-info arena) and would read as a leak of the library's.
    var threaded: Io.Threaded = .init(gpa, .{});
    io = threaded.io();
    const args = init.minimal.args.toSlice(init.arena.allocator()) catch std.process.exit(90);
    var seconds: u64 = 120;
    var i: usize = 1;
    while (i < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], "--seconds") and i + 1 < args.len) {
            i += 1;
            seconds = std.fmt.parseInt(u64, args[i], 10) catch std.process.exit(90);
        } else {
            std.debug.print("soak: usage: soak [--seconds N]\n", .{});
            std.process.exit(90);
        }
    }
    std.process.exit(run(seconds));
}

fn run(seconds: u64) u8 {
    var name_buf: [64]u8 = undefined;
    const name = std.mem.print(&name_buf, "soak-{d}", .{pid()}) catch return 90;
    const half = @max(seconds / 2, 1);

    std.debug.print("soak: {d} s ({d} s churn, then {d} s ping-pong), 1 MiB rings\n", .{ seconds, half, seconds - half });

    // A warm-up connection settles the allocator and the Io's worker pool before the baseline is taken.
    if (!warmUp(name)) {
        std.debug.print("soak: FAIL: the warm-up pair could not connect\n", .{});
        return 1;
    }
    const base = sample();
    std.debug.print("soak: baseline {f}\n", .{base});

    var worst = base;
    const cycles = churn(name, half, base, &worst) orelse return 1;
    const trips = pingPong(name, seconds - half, base, &worst) orelse return 1;

    // The final resources must be back near the baseline: a leak never comes back down.
    const final = sample();
    std.debug.print("soak: {d} churn cycles, {d} round trips; final {f}; worst {f}\n", .{ cycles, trips, final, worst });
    if (final.leaksOver(base)) {
        std.debug.print("soak: FAIL: resources grew past the baseline (a leak)\n", .{});
        return 1;
    }
    std.debug.print("soak: PASS\n", .{});
    return 0;
}

/// A listener, its client and the connection it accepted: one pair of this process.
const Pair = struct {
    listener: fastipc.Listener,
    server: fastipc.Conn,
    client: fastipc.Conn,

    fn open(name: []const u8) ?Pair {
        const listener = fastipc.Listener.listen(io, gpa, name, capacity) catch return failed("listen");
        const Client = struct {
            fn run(n: []const u8, out: *fastipc.Error!fastipc.Conn) void {
                out.* = fastipc.Conn.connect(io, gpa, n, bound);
            }
        };
        var connected: fastipc.Error!fastipc.Conn = error.Timeout;
        const thread = std.Thread.spawn(.{}, Client.run, .{ name, &connected }) catch {
            listener.close();
            return failed("spawn");
        };
        const accepted = listener.accept(bound);
        thread.join();
        const client = connected catch {
            if (accepted) |server| server.close() else |_| {}
            listener.close();
            return failed("connect");
        };
        const server = accepted catch {
            client.close();
            listener.close();
            return failed("accept");
        };
        return .{ .listener = listener, .server = server, .client = client };
    }

    fn close(pair: Pair) void {
        pair.client.close();
        pair.server.close();
        pair.listener.close();
    }

    /// One message each way; false (reported) on a failure.
    fn exchange(pair: Pair, msg: []const u8, buf: []u8) bool {
        pair.server.send(msg, bound) catch return report("server send");
        _ = pair.client.recv(buf, bound) catch return report("client recv");
        pair.client.send(msg, bound) catch return report("client send");
        _ = pair.server.recv(buf, bound) catch return report("server recv");
        return true;
    }
};

/// One pair, connected, so the baseline includes a live session's steady state.
fn warmUp(name: []const u8) bool {
    const pair = Pair.open(name) orelse return false;
    pair.close();
    return true;
}

/// listen / connect / accept / exchange / close, over and over: the churn workload.
fn churn(name: []const u8, seconds: u64, base: Resources, worst: *Resources) ?u64 {
    const deadline = deadlineIn(seconds);
    var cycles: u64 = 0;
    var msg: [64]u8 = undefined;
    var buf: [64]u8 = undefined;
    while (nowMs() < deadline) {
        const pair = Pair.open(name) orelse return null;
        defer pair.close();
        if (!pair.exchange(&msg, &buf)) return null;
        cycles += 1;
        if (cycles % 256 == 0 and check(base, worst) == false) return null;
    }
    _ = check(base, worst);
    return cycles;
}

/// One long-lived pair, a small message bounced back and forth: the steady data path.
fn pingPong(name: []const u8, seconds: u64, base: Resources, worst: *Resources) ?u64 {
    const pair = Pair.open(name) orelse return null;
    defer pair.close();

    const deadline = deadlineIn(seconds);
    var trips: u64 = 0;
    var msg: [32]u8 = undefined;
    var buf: [32]u8 = undefined;
    while (nowMs() < deadline) {
        if (!pair.exchange(&msg, &buf)) return null;
        trips += 1;
        if (trips % 100_000 == 0 and check(base, worst) == false) return null;
    }
    return trips;
}

/// Reports a failed step; false for the caller to return.
fn report(what: []const u8) bool {
    std.debug.print("soak: FAIL: {s}\n", .{what});
    return false;
}

fn failed(what: []const u8) ?Pair {
    _ = report(what);
    return null;
}

/// Samples resources, tracks the worst seen, and returns false on a leak past the slack.
fn check(base: Resources, worst: *Resources) bool {
    const now = sample();
    worst.* = worst.max(now);
    if (now.leaksOver(base)) {
        std.debug.print("soak: FAIL: resources grew past the baseline mid-run: {f} vs baseline {f}\n", .{ now, base });
        return false;
    }
    return true;
}

fn deadlineIn(seconds: u64) i64 {
    return nowMs() + @as(i64, @intCast(seconds * 1000));
}

fn nowMs() i64 {
    return @intCast(Io.Clock.awake.now(io).toMilliseconds());
}

fn pid() u32 {
    if (is_macos) return @intCast(std.c.getpid());
    return if (is_windows) win.GetCurrentProcessId() else @intCast(std.os.linux.getpid());
}

// === Resource sampling ===

/// The resource counts the soak watches. RSS is allowed a generous slack (allocators keep freed pages); the
/// descriptor/handle and thread counts a much tighter one - a leaked segment or worker would push them up and keep
/// them up.
const Resources = struct {
    rss_kib: u64,
    handles: u64, // Windows handles / Linux and macOS open descriptors
    threads: u64, // Linux and macOS (0 on Windows)

    const rss_slack_kib = 64 * 1024; // 64 MiB: page churn, not a leak
    const handle_slack = 16;
    const thread_slack = 4;

    fn max(a: Resources, b: Resources) Resources {
        return .{ .rss_kib = @max(a.rss_kib, b.rss_kib), .handles = @max(a.handles, b.handles), .threads = @max(a.threads, b.threads) };
    }

    fn leaksOver(now: Resources, base: Resources) bool {
        return now.rss_kib > base.rss_kib + rss_slack_kib or
            now.handles > base.handles + handle_slack or
            now.threads > base.threads + thread_slack;
    }

    pub fn format(r: Resources, w: *Writer) Writer.Error!void {
        if (is_windows) {
            try w.print("RSS {d} MiB, handles {d}", .{ r.rss_kib / 1024, r.handles });
        } else {
            try w.print("RSS {d} MiB, fds {d}, threads {d}", .{ r.rss_kib / 1024, r.handles, r.threads });
        }
    }
};

fn sample() Resources {
    if (is_windows) {
        var counters: win.PROCESS_MEMORY_COUNTERS = undefined;
        counters.cb = @sizeOf(win.PROCESS_MEMORY_COUNTERS);
        const rss: u64 = if (win.K32GetProcessMemoryInfo(win.GetCurrentProcess(), &counters, counters.cb) != 0)
            counters.WorkingSetSize / 1024
        else
            0;
        var handles: win.DWORD = 0;
        _ = win.GetProcessHandleCount(win.GetCurrentProcess(), &handles);
        return .{ .rss_kib = rss, .handles = handles, .threads = 0 };
    }
    if (is_macos) {
        var info: mac.proc_taskinfo = undefined;
        const got = mac.proc_pidinfo(std.c.getpid(), mac.PROC_PIDTASKINFO, 0, &info, @sizeOf(mac.proc_taskinfo));
        if (got != @sizeOf(mac.proc_taskinfo)) return .{ .rss_kib = 0, .handles = fdCount(), .threads = 0 };
        return .{ .rss_kib = info.pti_resident_size / 1024, .handles = fdCount(), .threads = @intCast(info.pti_threadnum) };
    }
    return .{ .rss_kib = procField("VmRSS:") orelse 0, .handles = fdCount(), .threads = procField("Threads:") orelse 0 };
}

/// A numeric field of /proc/self/status (VmRSS in KiB, Threads as a count).
fn procField(name: []const u8) ?u64 {
    const fd = linux.open("/proc/self/status", .{ .ACCMODE = .RDONLY, .CLOEXEC = true }, 0);
    if (linux.errno(fd) != .SUCCESS) return null;
    defer _ = linux.close(@intCast(fd));
    var buf: [4096]u8 = undefined;
    const n = linux.read(@intCast(fd), &buf, buf.len);
    if (linux.errno(n) != .SUCCESS) return null;
    const text = buf[0..n];
    const at = std.mem.find(u8, text, name) orelse return null;
    var it = std.mem.tokenizeAny(u8, text[at + name.len ..], " \t\n");
    const token = it.next() orelse return null;
    return std.fmt.parseInt(u64, token, 10) catch null;
}

/// The open descriptors of this process (Linux, macOS), the way the endpoint tests count them: probe fd 0..1024.
fn fdCount() u64 {
    var count: u64 = 0;
    if (is_macos) {
        for (0..1024) |fd| {
            if (std.c.fcntl(@intCast(fd), std.c.F.GETFD) != -1) count += 1;
        }
        return count;
    }
    for (0..1024) |fd| {
        if (linux.errno(linux.fcntl(@intCast(fd), linux.F.GETFD, 0)) == .SUCCESS) count += 1;
    }
    return count;
}

/// What macOS's libproc offers for the sample (<libproc.h>, <sys/proc_info.h>): RSS and thread count in one call.
const mac = struct {
    const PROC_PIDTASKINFO = 4;
    const proc_taskinfo = extern struct {
        pti_virtual_size: u64,
        pti_resident_size: u64,
        pti_total_user: u64,
        pti_total_system: u64,
        pti_threads_user: u64,
        pti_threads_system: u64,
        pti_policy: i32,
        pti_faults: i32,
        pti_pageins: i32,
        pti_cow_faults: i32,
        pti_messages_sent: i32,
        pti_messages_received: i32,
        pti_syscalls_mach: i32,
        pti_syscalls_unix: i32,
        pti_csw: i32,
        pti_threadnum: i32,
        pti_numrunning: i32,
        pti_priority: i32,
    };
    comptime {
        std.debug.assert(@sizeOf(proc_taskinfo) == 96);
    }
    extern "c" fn proc_pidinfo(pid: c_int, flavor: c_int, arg: u64, buffer: ?*anyopaque, buffersize: c_int) c_int;
};

const win = struct {
    const DWORD = std.os.windows.DWORD;
    const HANDLE = std.os.windows.HANDLE;
    const SIZE_T = usize;
    const PROCESS_MEMORY_COUNTERS = extern struct {
        cb: DWORD,
        PageFaultCount: DWORD,
        PeakWorkingSetSize: SIZE_T,
        WorkingSetSize: SIZE_T,
        QuotaPeakPagedPoolUsage: SIZE_T,
        QuotaPagedPoolUsage: SIZE_T,
        QuotaPeakNonPagedPoolUsage: SIZE_T,
        QuotaNonPagedPoolUsage: SIZE_T,
        PagefileUsage: SIZE_T,
        PeakPagefileUsage: SIZE_T,
    };
    extern "kernel32" fn GetCurrentProcessId() callconv(.winapi) DWORD;
    extern "kernel32" fn GetCurrentProcess() callconv(.winapi) HANDLE;
    extern "kernel32" fn GetProcessHandleCount(process: HANDLE, count: *DWORD) callconv(.winapi) c_int;
    extern "kernel32" fn K32GetProcessMemoryInfo(process: HANDLE, counters: *PROCESS_MEMORY_COUNTERS, cb: DWORD) callconv(.winapi) c_int;
};
