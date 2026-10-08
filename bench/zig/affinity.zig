//! Optional CPU pinning (`AUTO_AFFINITY=1`), the benchmark's own (the library pins nothing): once a connection is
//! made (accept, connect), the calling thread moves to its side's CPU of a preferred pair, the server to one and the
//! client to the other.
//! The pair comes from the logical CPU count, each CPU's NUMA node and a P-/E-core split (Linux only, from the
//! cpufreq maxima). Detected once per process. Linux reads sysfs; Windows uses kernel32.

const std = @import("std");
const builtin = @import("builtin");

const max_cpus = 256;

const is_linux = builtin.os.tag == .linux;
const is_windows = builtin.os.tag == .windows;

/// Set once by main.zig from `AUTO_AFFINITY` (a value starting with "1"), before any connection is made.
pub var enabled: bool = false;

/// Pins the calling thread to its side's CPU of the preferred pair, if pinning is enabled and the machine has two
/// CPUs or more; otherwise does nothing.
pub fn pinSide(is_server: bool) void {
    if (!enabled) return;
    const pair = preferredPair() orelse return;
    setAffinity(if (is_server) pair.cpu_server else pair.cpu_client);
}

/// One logical CPU.
const CpuInfo = struct {
    numa_node: u32,
    /// 0 if unknown.
    max_freq_mhz: u32,
    /// P-core (true unless the core types differ and this is a slower one).
    is_performance: bool,
};

pub const CpuPair = struct {
    cpu_server: u32,
    cpu_client: u32,
    same_numa_node: bool,
    both_pcores: bool,
};

const Topology = struct {
    cpu_count: u32 = 0,
    numa_node_count: u32 = 1,
    cpus: [max_cpus]CpuInfo = @splat(.{ .numa_node = 0, .max_freq_mhz = 0, .is_performance = true }),
    has_heterogeneous: bool = false,
};

var topology: Topology = .{};

/// Detection runs once; concurrent callers wait for it.
var once_state = std.atomic.Value(u8).init(0); // 0: not started, 1: running, 2: done

fn detectOnce() void {
    if (once_state.load(.acquire) == 2) return;
    if (once_state.cmpxchgStrong(0, 1, .acquire, .acquire) == null) {
        detect();
        once_state.store(2, .release);
        return;
    }
    while (once_state.load(.acquire) != 2) std.Thread.yield() catch {};
}

/// Two CPUs for the server and the client (see `choosePair`); null with fewer than two CPUs, or more than 256.
pub fn preferredPair() ?CpuPair {
    detectOnce();
    if (topology.cpu_count < 2) return null;
    return choosePair(topology.cpus[0..topology.cpu_count], topology.has_heterogeneous, topology.numa_node_count);
}

/// Prefers two P-cores on one NUMA node, then any two P-cores (both only when the core types
/// differ), then two CPUs on one node (multi-node systems only), then CPUs 0 and 1. Among the
/// candidates, the first pair in CPU order. At least two CPUs.
fn choosePair(cpus: []const CpuInfo, heterogeneous: bool, numa_node_count: u32) CpuPair {
    const chosen: [2]usize = blk: {
        if (heterogeneous) {
            if (findPair(cpus, .pcores_same_node)) |pair| break :blk pair;
            if (findPair(cpus, .pcores)) |pair| break :blk pair;
        }
        if (numa_node_count > 1) {
            if (findPair(cpus, .same_node)) |pair| break :blk pair;
        }
        break :blk .{ 0, 1 };
    };
    const server = cpus[chosen[0]];
    const client = cpus[chosen[1]];
    return .{
        .cpu_server = @intCast(chosen[0]),
        .cpu_client = @intCast(chosen[1]),
        .same_numa_node = server.numa_node == client.numa_node,
        .both_pcores = server.is_performance and client.is_performance,
    };
}

const PairKind = enum { pcores_same_node, pcores, same_node };

/// The first CPUs `i < j` (in lexicographic order) that suit `kind`.
fn findPair(cpus: []const CpuInfo, kind: PairKind) ?[2]usize {
    for (cpus, 0..) |a, i| {
        for (cpus[i + 1 ..], i + 1..) |b, j| {
            const suits = switch (kind) {
                .pcores_same_node => a.is_performance and b.is_performance and a.numa_node == b.numa_node,
                .pcores => a.is_performance and b.is_performance,
                .same_node => a.numa_node == b.numa_node,
            };
            if (suits) return .{ i, j };
        }
    }
    return null;
}

extern "kernel32" fn GetCurrentThread() callconv(.winapi) *anyopaque;
extern "kernel32" fn SetThreadAffinityMask(thread: *anyopaque, mask: usize) callconv(.winapi) usize;
extern "kernel32" fn GetNumaHighestNodeNumber(highest: *u32) callconv(.winapi) c_int;
extern "kernel32" fn GetNumaProcessorNode(processor: u8, node: *u8) callconv(.winapi) c_int;

/// Pins the calling thread to `cpu_id`; a failure leaves it unpinned.
fn setAffinity(cpu_id: u32) void {
    if (is_linux) {
        var set = std.mem.zeroes(std.os.linux.cpu_set_t);
        if (cpu_id < set.len * @bitSizeOf(usize)) set[cpu_id / @bitSizeOf(usize)] |= @as(usize, 1) << @intCast(cpu_id % @bitSizeOf(usize));
        std.os.linux.sched_setaffinity(0, &set) catch {};
    } else if (is_windows) {
        _ = SetThreadAffinityMask(GetCurrentThread(), @as(usize, 1) << @truncate(cpu_id));
    }
}

fn detect() void {
    const count = std.Thread.getCpuCount() catch return;
    if (count > max_cpus) return;
    topology.cpu_count = @intCast(count);
    if (is_linux) {
        detectNumaLinux();
        detectFrequenciesLinux();
    } else if (is_windows) {
        detectNumaWindows();
    }
}

/// The node count is one past the highest `/sys/devices/system/node/nodeN` (N < 64); each CPU
/// belongs to the first node directory that lists it (node 0 otherwise).
fn detectNumaLinux() void {
    var max_node: u32 = 0;
    for (0..64) |node| {
        if (sysfsDirExists("/sys/devices/system/node/node{d}", .{node})) max_node = @intCast(node);
    }
    topology.numa_node_count = max_node + 1;
    for (topology.cpus[0..topology.cpu_count], 0..) |*cpu, index| {
        for (0..topology.numa_node_count) |node| {
            if (sysfsDirExists("/sys/devices/system/node/node{d}/cpu{d}", .{ node, index })) {
                cpu.numa_node = @intCast(node);
                break;
            }
        }
    }
}

/// Reads each CPU's `cpuinfo_max_freq`. If the fastest and slowest differ by more than 10%, the
/// system counts as heterogeneous and CPUs above the midpoint are P-cores; otherwise all are.
fn detectFrequenciesLinux() void {
    var min_freq: u32 = std.math.maxInt(u32);
    var max_freq: u32 = 0;
    for (topology.cpus[0..topology.cpu_count], 0..) |*cpu, index| {
        const freq_khz = readSysfsU32("/sys/devices/system/cpu/cpu{d}/cpufreq/cpuinfo_max_freq", .{index});
        if (freq_khz > 0) {
            cpu.max_freq_mhz = freq_khz / 1000;
            min_freq = @min(min_freq, freq_khz);
            max_freq = @max(max_freq, freq_khz);
        }
    }
    if (max_freq > 0 and min_freq > 0 and (max_freq -% min_freq) *% 100 / min_freq > 10) {
        topology.has_heterogeneous = true;
        const threshold = min_freq +% (max_freq -% min_freq) / 2;
        for (topology.cpus[0..topology.cpu_count]) |*cpu| {
            if (cpu.max_freq_mhz > 0) cpu.is_performance = cpu.max_freq_mhz *% 1000 > threshold;
        }
    }
}

fn detectNumaWindows() void {
    var highest_node: u32 = 0;
    topology.numa_node_count = if (GetNumaHighestNodeNumber(&highest_node) != 0) highest_node +% 1 else 1;
    for (topology.cpus[0..topology.cpu_count], 0..) |*cpu, index| {
        var node: u8 = 0;
        cpu.numa_node = if (GetNumaProcessorNode(@truncate(index), &node) != 0) node else 0;
    }
}

fn sysfsDirExists(comptime fmt: []const u8, args: anytype) bool {
    var buf: [256]u8 = undefined;
    const path = std.mem.printSentinel(&buf, fmt, args, 0) catch return false;
    const dir = std.c.opendir(path) orelse return false;
    _ = std.c.closedir(dir);
    return true;
}

/// The file's first number, or 0.
fn readSysfsU32(comptime fmt: []const u8, args: anytype) u32 {
    var buf: [256]u8 = undefined;
    const path = std.mem.printSentinel(&buf, fmt, args, 0) catch return 0;
    const file = std.c.fopen(path, "r") orelse return 0;
    defer _ = std.c.fclose(file);
    var text: [32]u8 = undefined;
    const n = std.c.fread(&text, 1, text.len, file);
    const digits = std.mem.trimEnd(u8, text[0..n], " \n\r\t");
    return std.fmt.parseInt(u32, digits, 10) catch 0;
}

test "a preferred pair is two distinct CPUs" {
    const pair = preferredPair() orelse return error.SkipZigTest; // fewer than two CPUs
    try std.testing.expect(pair.cpu_server != pair.cpu_client);
}

test "the pair prefers P-cores, then one NUMA node, then CPUs 0 and 1" {
    const cpu = struct {
        fn info(numa_node: u32, is_performance: bool) CpuInfo {
            return .{ .numa_node = numa_node, .max_freq_mhz = 0, .is_performance = is_performance };
        }
    }.info;

    // Hybrid: E-cores first, P-cores on two nodes; the first same-node P-core pair wins
    const hybrid = [_]CpuInfo{ cpu(0, false), cpu(1, true), cpu(0, true), cpu(1, true), cpu(0, true) };
    try std.testing.expectEqual(CpuPair{ .cpu_server = 1, .cpu_client = 3, .same_numa_node = true, .both_pcores = true }, choosePair(&hybrid, true, 2));
    // P-cores only on different nodes: still P-cores
    const split = [_]CpuInfo{ cpu(0, true), cpu(0, false), cpu(1, true) };
    try std.testing.expectEqual(CpuPair{ .cpu_server = 0, .cpu_client = 2, .same_numa_node = false, .both_pcores = true }, choosePair(&split, true, 2));
    // Hybrid on two nodes with a single P-core: the first same-node pair, E-cores included
    const lone_pcore = [_]CpuInfo{ cpu(0, true), cpu(1, false), cpu(1, false) };
    try std.testing.expectEqual(CpuPair{ .cpu_server = 1, .cpu_client = 2, .same_numa_node = true, .both_pcores = false }, choosePair(&lone_pcore, true, 2));
    // Uniform cores on two nodes: the first same-node pair
    const numa = [_]CpuInfo{ cpu(0, true), cpu(1, true), cpu(1, true) };
    try std.testing.expectEqual(CpuPair{ .cpu_server = 1, .cpu_client = 2, .same_numa_node = true, .both_pcores = true }, choosePair(&numa, false, 2));
    // One node, or nothing better: CPUs 0 and 1
    try std.testing.expectEqual(CpuPair{ .cpu_server = 0, .cpu_client = 1, .same_numa_node = false, .both_pcores = true }, choosePair(&numa, false, 1));
}
