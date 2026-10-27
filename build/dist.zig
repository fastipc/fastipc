//! `zig build dist`: the shared libraries the packages ship, buildable on any OS (Zig cross-compiles all four): the CPU
//! floors whatever `-Dcpu` says (x86-64-v3 with PREFETCHW, generic ARMv8.0-A for ARM64 Linux), glibc 2.34 as the Linux floor, and the
//! Apple M1 and macOS 14.4 as the macOS floors (docs/platform-support.md), ReleaseFast and stripped (no debug info, so
//! no build-machine paths either). Installed in the NuGet `runtimes/` layout under zig-out/dist, the Windows DLL with
//! its import library (`fastipc.lib`, for C and C++ programs that link it; the packages leave it out), each platform's
//! Node-API addon in zig-out/dist/node, and the C and C++ headers in zig-out/dist/include.

const std = @import("std");
const library = @import("library.zig");
const node_addon = @import("node_addon.zig");
const targets = @import("targets.zig");

/// The step `dist`.
pub fn add(b: *std.Build, config: library.Config) void {
    const dist = b.step("dist", "Build the packaged shared libraries (x86-64-v3, glibc 2.34; arm64 Linux glibc 2.34; arm64 macOS 14.4) into zig-out/dist");
    for ([_][]const u8{ "fipc.h", "fipc.hpp" }) |header| {
        const install = b.addInstallFileWithDir(b.path(b.fmt("include/{s}", .{header})), .{ .custom = "dist/include" }, header);
        dist.dependOn(&install.step);
    }
    // The floors of docs/platform-support.md
    const cpu: std.Target.Query.CpuModel = .{ .explicit = targets.cpu_floor };
    const platforms = [_]struct { rid: []const u8, query: std.Target.Query }{
        .{ .rid = "linux-x64", .query = .{
            .cpu_arch = .x86_64,
            .cpu_model = cpu,
            .cpu_features_add = targets.cpu_floor_features,
            .os_tag = .linux,
            .abi = .gnu,
            .glibc_version = .{ .major = 2, .minor = 34, .patch = 0 },
        } },
        .{ .rid = "win-x64", .query = .{
            .cpu_arch = .x86_64,
            .cpu_model = cpu,
            .cpu_features_add = targets.cpu_floor_features,
            .os_tag = .windows,
            .abi = .gnu,
        } },
        .{ .rid = "osx-arm64", .query = .{
            .cpu_arch = .aarch64,
            .cpu_model = .{ .explicit = targets.macos_cpu_floor },
            .os_tag = .macos,
            .os_version_min = .{ .semver = targets.macos_version_floor },
        } },
        .{ .rid = "linux-arm64", .query = .{
            .cpu_arch = .aarch64,
            .cpu_model = .{ .explicit = targets.linux_arm64_cpu_floor },
            .os_tag = .linux,
            .abi = .gnu,
            .glibc_version = .{ .major = 2, .minor = 34, .patch = 0 },
        } },
    };
    for (platforms) |platform| {
        const target = b.resolveTargetQuery(platform.query);
        const lib = b.addLibrary(.{
            .name = "fastipc",
            .linkage = .dynamic,
            // No .version: a package holds the one file libfastipc.so, not a symlink chain
            .root_module = library.libraryModule(b, config.releaseFast().withTarget(target), .{}),
        });
        lib.bundle_ubsan_rt = false;
        lib.root_module.strip = true;
        const dir: std.Build.InstallDir = .{ .custom = b.fmt("dist/runtimes/{s}/native", .{platform.rid}) };
        const install = b.addInstallArtifact(lib, .{
            .dest_dir = .{ .override = dir },
            .pdb_dir = .disabled,
            .implib_dir = if (platform.query.os_tag == .windows) .{ .override = dir } else .disabled,
        });
        dist.dependOn(&install.step);

        // The JavaScript binding's addon for the same platform (zig-out/dist/node/<rid>/fipc.node)
        const addon = node_addon.addLibrary(b, target, .fast);
        dist.dependOn(&b.addInstallArtifact(addon, .{
            .dest_dir = .{ .override = .{ .custom = b.fmt("dist/node/{s}", .{platform.rid}) } },
            .dest_sub_path = "fipc.node",
            .pdb_dir = .disabled,
            .implib_dir = .disabled,
        }).step);
    }
}
