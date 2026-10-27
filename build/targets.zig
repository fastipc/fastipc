//! The platform floors (docs/platform-support.md): the oldest CPU and OS every build targets unless the command line
//! names others, so the tests, the benchmarks and the packages run the code that ships, not code tuned to the build
//! machine (`-Dcpu=native`) or to a CPU FastIPC doesn't support (`-Dcpu=baseline`).

const std = @import("std");

/// The CPU every x86-64 build targets unless `-Dcpu` names another: x86-64-v3 (AVX2), the platform's minimum.
pub const cpu_floor = &std.Target.x86.cpu.x86_64_v3;
/// The x86-64 floor's one addition: PREFETCHW, a prefetch for writing, which the stream's sender uses
/// (stream/data_path.zig `prefetchAhead`). Without it a write prefetch becomes a read prefetch (`prefetcht0`), which
/// fetches the line shared and slows 16-byte sends. Intel from Broadwell on and every AMD Zen report it
/// (docs/platform-support.md).
pub const cpu_floor_features = std.Target.x86.featureSet(&.{.prfchw});

/// The CPU every ARM64 Linux build targets unless `-Dcpu` names another: generic ARMv8.0-A with NEON, the oldest
/// 64-bit ARM (docs/platform-support.md). A build on a newer core would otherwise use instructions older ones lack.
pub const linux_arm64_cpu_floor = &std.Target.aarch64.cpu.generic;

/// The CPU every macOS build targets unless `-Dcpu` names another: the Apple M1, the first Apple Silicon chip
/// (docs/platform-support.md). A build on a later chip would otherwise use instructions an M1 lacks.
pub const macos_cpu_floor = &std.Target.aarch64.cpu.apple_m1;
/// The oldest macOS every macOS build runs on, unless the target names another: 14.4, the first with
/// `os_sync_wait_on_address` (platform/macos/macos.zig). A build would otherwise require the build machine's macOS.
pub const macos_version_floor: std.SemanticVersion = .{ .major = 14, .minor = 4, .patch = 0 };

/// `query` with the CPU floor, if it names no CPU and its architecture is x86-64 (the host's, if it names none); for
/// macOS on aarch64, with the macOS floors (`withMacosFloors`); for Linux on aarch64, with the ARM64 Linux CPU floor if
/// it names no CPU.
pub fn withCpuFloor(query: std.Target.Query) std.Target.Query {
    var result = query;
    const arch = query.cpu_arch orelse @import("builtin").target.cpu.arch;
    const os = query.os_tag orelse @import("builtin").target.os.tag;
    if (query.cpu_model == .determined_by_arch_os and arch == .x86_64) {
        result.cpu_model = .{ .explicit = cpu_floor };
        result.cpu_features_add = cpu_floor_features;
    }
    if (arch == .aarch64 and os == .macos) result = withMacosFloors(result);
    if (query.cpu_model == .determined_by_arch_os and arch == .aarch64 and os == .linux) result.cpu_model = .{ .explicit = linux_arm64_cpu_floor };
    return result;
}

/// `query` (aarch64-macos) with the macOS CPU floor if it names no CPU, and the macOS version floor as its minimum if it
/// names none.
fn withMacosFloors(query: std.Target.Query) std.Target.Query {
    var result = query;
    if (query.cpu_model == .determined_by_arch_os) result.cpu_model = .{ .explicit = macos_cpu_floor };
    if (query.os_version_min == null) result.os_version_min = .{ .semver = macos_version_floor };
    return result;
}

/// The shipped library's target: Windows builds are always the GNU ABI.
pub fn sharedLibraryTarget(b: *std.Build, target: std.Build.ResolvedTarget) std.Build.ResolvedTarget {
    if (target.result.os.tag != .windows) return target;
    var query = target.query;
    query.os_tag = .windows;
    query.abi = .gnu;
    return b.resolveTargetQuery(query);
}
