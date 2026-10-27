//! The benchmark suite (bench/zig): `fipc_bench` and the library it measures by default, this tree's shared library
//! built ReleaseFast whatever the build's mode, both installed in zig-out/bench. The suite loads the library at run
//! time (`--lib`), so the same binary also measures another revision's library that speaks include/fipc.h
//! (`devtool bench-compare --ab`). Beside them: the Node-API addon, the Zig benchmark (bench/zig-api) and the C and C++
//! benchmark (bench/c). Not built with TSan: nothing runs it there.

const std = @import("std");
const library = @import("library.zig");
const node_addon = @import("node_addon.zig");
const targets = @import("targets.zig");

/// Installs the benchmarks in zig-out/bench, part of the default build; the suite's unit tests run in `test_step`.
pub fn add(b: *std.Build, config: library.Config, test_step: *std.Build.Step) void {
    const release = config.releaseFast();
    const install_dir: std.Build.InstallDir = .{ .custom = "bench" };
    const c_api = library.translateHeader(b, library.public_header, config.target, .fast);
    const module = b.createModule(.{
        .root_source_file = b.path("bench/zig/main.zig"),
        .target = config.target,
        .optimize = .fast,
        .link_libc = true, // dlopen on Linux
        .imports = &.{.{ .name = "c", .module = c_api }},
    });

    const exe = b.addExecutable(.{ .name = "fipc_bench", .root_module = module });
    b.getInstallStep().dependOn(&b.addInstallArtifact(exe, .{
        .dest_dir = .{ .override = install_dir },
        .pdb_dir = .disabled,
    }).step);

    const lib = b.addLibrary(.{
        .name = "fastipc",
        .linkage = .dynamic,
        .root_module = library.libraryModule(b, release.withTarget(targets.sharedLibraryTarget(b, config.target)), .{}),
    });
    lib.bundle_ubsan_rt = false;
    b.getInstallStep().dependOn(&b.addInstallArtifact(lib, .{
        .dest_dir = .{ .override = install_dir },
        .pdb_dir = .disabled,
        .implib_dir = .disabled,
    }).step);

    // The JavaScript binding's addon beside it, ReleaseFast too: the language benchmarks load both from here
    // (FASTIPC_LIB_DIR), never the build's mode, which is Debug after a test run
    const addon = node_addon.addLibrary(b, targets.sharedLibraryTarget(b, config.target), .fast);
    b.getInstallStep().dependOn(&b.addInstallArtifact(addon, .{
        .dest_dir = .{ .override = install_dir },
        .dest_sub_path = "fipc.node",
        .pdb_dir = .disabled,
        .implib_dir = .disabled,
    }).step);

    const tests = b.addTest(.{ .root_module = module });
    test_step.dependOn(&b.addRunArtifact(tests).step);

    // The Zig benchmark (bench/zig-api): the same cases through the native module, built ReleaseFast
    const zig_bench = b.addExecutable(.{
        .name = "zig_bench",
        .root_module = b.createModule(.{
            .root_source_file = b.path("bench/zig-api/main.zig"),
            .target = config.target,
            .optimize = .fast,
            .imports = &.{.{ .name = "fastipc", .module = library.privateModule(b, release, "src/zig/root.zig", .{}) }},
        }),
    });
    b.getInstallStep().dependOn(&b.addInstallArtifact(zig_bench, .{
        .dest_dir = .{ .override = install_dir },
        .pdb_dir = .disabled,
    }).step);

    // The C and C++ benchmark (bench/c): one harness, the loops through the C API (c_bench) or the C++ wrapper
    // (cpp_bench), linked with the same ReleaseFast library and installed next to it
    for ([_]struct { name: []const u8, loops: []const u8, cpp: bool }{
        .{ .name = "c_bench", .loops = "bench/c/loops.c", .cpp = false },
        .{ .name = "cpp_bench", .loops = "bench/c/loops.cpp", .cpp = true },
    }) |program| {
        const bench_exe = b.addExecutable(.{
            .name = program.name,
            .root_module = b.createModule(.{
                .target = config.target,
                .optimize = .fast,
                .link_libc = true,
                .link_libcpp = program.cpp,
            }),
        });
        bench_exe.root_module.addIncludePath(b.path("include"));
        bench_exe.root_module.addCSourceFile(.{ .file = b.path("bench/c/main.c"), .flags = &c_flags });
        bench_exe.root_module.addCSourceFile(.{ .file = b.path(program.loops), .flags = if (program.cpp) &cpp_flags else &c_flags });
        bench_exe.root_module.linkLibrary(lib);
        if (config.target.result.os.tag == .macos) bench_exe.root_module.addRPathSpecial("@loader_path") else if (config.target.result.os.tag != .windows) bench_exe.root_module.addRPathSpecial("$ORIGIN");
        b.getInstallStep().dependOn(&b.addInstallArtifact(bench_exe, .{
            .dest_dir = .{ .override = install_dir },
            .pdb_dir = .disabled,
        }).step);
    }
}

/// The flags of the C and C++ benchmark (bench/c): C11 and C++20 (no exceptions or RTTI), every warning an error, the
/// library's functions declared dllimport on Windows.
const c_flags = [_][]const u8{ "-std=c11", "-Wall", "-Wextra", "-Wpedantic", "-Werror", "-DFASTIPC_SHARED" };
const cpp_flags = [_][]const u8{ "-std=c++20", "-fno-exceptions", "-fno-rtti", "-Wall", "-Wextra", "-Wpedantic", "-Werror", "-DFASTIPC_SHARED" };
