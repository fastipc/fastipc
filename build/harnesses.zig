//! The heavy harnesses, driven by devtool and not part of the default build: `zig build harness` (the chaos peer),
//! `zig build soak` and `zig build fuzz`; and `zig build cdb`, the compilation database of the chaos peer, the one C
//! file the build compiles for them.

const std = @import("std");
const library = @import("library.zig");

/// The chaos harness peer (`zig build harness`, driven by `devtool.py test chaos`): one C program that uses the
/// library through its public header, include/fipc.h. Always optimized (`-O2`): it must spend its time in the library
/// under test, which keeps the build's mode, not in making and checking its own messages.
const chaos_peer_source = "tests/chaos/chaos.c";

/// The flags the chaos peer compiles with.
const chaos_peer_flags = [_][]const u8{ "-std=c99", "-Wall", "-Wextra", "-Werror", "-O2", "-g" };

/// The steps `harness`, `soak` and `fuzz`. `static_lib` is `fastipc_static`, which the chaos peer links.
pub fn add(b: *std.Build, config: library.Config, static_lib: *std.Build.Step.Compile) void {
    addChaosPeer(b, config, static_lib);
    addHarness(b, config, "soak", "tests/soak/main.zig");
    addHarness(b, config, "fuzz", "tests/fuzz/main.zig");
}

/// The `harness` step: the chaos harness peer (tests/chaos/chaos.c), built against this tree's fastipc_static (whose
/// test hook `FASTIPC_TEST_CRASH_AT` the crash-at-step scenario uses) and installed into zig-out/bin as `chaos_peer`.
fn addChaosPeer(b: *std.Build, config: library.Config, static_lib: *std.Build.Step.Compile) void {
    const harness_step = b.step("harness", "Build the chaos harness peer (tests/chaos)");
    const peer = b.addExecutable(.{
        .name = "chaos_peer",
        .root_module = b.createModule(.{ .target = config.target, .optimize = config.optimize }),
    });
    peer.root_module.addIncludePath(b.path("include"));
    peer.root_module.linkLibrary(static_lib);
    peer.root_module.addCSourceFile(.{
        .file = b.path(chaos_peer_source),
        .flags = &chaos_peer_flags,
    });
    library.linkSystemLibraries(peer.root_module, config);
    harness_step.dependOn(&b.addInstallArtifact(peer, .{}).step);
}

/// A Zig harness executable (`zig build <name>`) rooted at `src`, importing the native module "fastipc" (built with
/// its test hooks, so the fuzz harness reaches `fastipc.internal`: the names, wire, segment and platform helpers it
/// needs to be a corrupt peer). The library's Zig sources compile into the executable through that module; the
/// executable keeps the build's optimization mode, so its safety checks run. `python devtool.py test soak|fuzz`
/// builds and runs it.
fn addHarness(b: *std.Build, config: library.Config, name: []const u8, src: []const u8) void {
    const step = b.step(name, b.fmt("Build the {s} harness (tests/{s})", .{ name, name }));
    const native = library.privateModule(b, config, "src/zig/root.zig", .{ .test_hooks = true });
    const exe = b.addExecutable(.{
        .name = name,
        .root_module = b.createModule(.{
            .root_source_file = b.path(src),
            .target = config.target,
            .optimize = config.optimize,
            .imports = &.{.{ .name = "fastipc", .module = native }},
        }),
    });
    library.linkSystemLibraries(exe.root_module, config);
    step.dependOn(&b.addInstallArtifact(exe, .{}).step);
}

/// `zig build cdb`: compile_commands.json at the repository root, for CLion and clangd: the one C file the build
/// compiles (the chaos peer), with its include directories and flags. The library itself is Zig, which these tools
/// don't read.
pub fn addCompileCommands(b: *std.Build) void {
    var arguments: std.ArrayList([]const u8) = .empty;
    arguments.append(b.allocator, "clang") catch @panic("OOM");
    for (library.include_dirs) |dir| arguments.append(b.allocator, b.fmt("-I{s}", .{dir})) catch @panic("OOM");
    arguments.appendSlice(b.allocator, &chaos_peer_flags) catch @panic("OOM");
    arguments.appendSlice(b.allocator, &.{ "-c", chaos_peer_source }) catch @panic("OOM");
    const entries = [_]struct { directory: []const u8, file: []const u8, arguments: []const []const u8 }{.{
        .directory = b.root.toString(b.allocator) catch @panic("OOM"),
        .file = chaos_peer_source,
        .arguments = arguments.items,
    }};
    const json = std.json.Stringify.valueAlloc(b.allocator, entries, .{ .whitespace = .indent_2 }) catch @panic("OOM");
    const update = b.addUpdateSourceFiles();
    update.addBytesToSource(json, "compile_commands.json");
    b.step("cdb", "Write compile_commands.json for CLion/clangd").dependOn(&update.step);
}
