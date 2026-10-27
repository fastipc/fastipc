//! The build. `build` reads the command line, declares the shipped library and the native Zig API (what a project that
//! depends on this package gets), and then, for this repository's own builds only, the steps of build/: the tests
//! (tests.zig), the benchmarks (bench.zig), the harnesses (harnesses.zig), the packaged libraries (dist.zig) and the
//! JavaScript binding's addon (node_addon.zig). build/library.zig makes the modules of the library's sources, and
//! build/targets.zig holds the platform floors.

const std = @import("std");
const bench = @import("build/bench.zig");
const dist = @import("build/dist.zig");
const harnesses = @import("build/harnesses.zig");
const library = @import("build/library.zig");
const node_addon = @import("build/node_addon.zig");
const targets = @import("build/targets.zig");
const tests = @import("build/tests.zig");

pub fn build(b: *std.Build) void {
    const config: library.Config = .{
        .target = b.resolveTargetQuery(targets.withCpuFloor(b.standardTargetOptionsQueryOnly(.{}))),
        .optimize = b.standardOptimizeOption(.{}),
        .enable_tsan = b.option(bool, "enable_tsan", "Enable Thread Sanitizer (TSan) for race detection") orelse false,
        .stream_pace = b.option(u32, "stream_pace", "Override the platform's reader spin pace (core/wait.zig), for tuning"),
    };
    const build_fastipc_shared = b.option(bool, "build_fastipc_shared", "Build FastIPC shared library") orelse true;

    // === The shipped library: `fastipc` (dynamic), without the test hooks ===
    // Every build of the library also runs the comptime checks of src/zig/c_api.zig (against include/fipc.h) and
    // src/zig/wire_protocol.zig (the wire formats' layouts).
    var shared: ?*std.Build.Step.Compile = null;
    var install_shared: ?*std.Build.Step.InstallArtifact = null;
    if (build_fastipc_shared) {
        const shared_lib = b.addLibrary(.{
            .name = "fastipc",
            .linkage = .dynamic,
            // macOS: no version, so the library is the one file libfastipc.dylib (install name
            // @rpath/libfastipc.dylib), as the packages ship it, not a chain of symlinks
            .version = if (config.target.result.os.tag == .macos) null else .{ .major = 1, .minor = 0, .patch = 0 },
            .root_module = library.libraryModule(b, config.withTarget(targets.sharedLibraryTarget(b, config.target)), .{}),
            .use_llvm = library.tsanBackend(config.enable_tsan),
        });
        // The library has no C code to sanitize, and Zig's bundled UBSan runtime would export its
        // 34 __ubsan_handle_* functions from a Debug .so (check-exports --exact)
        shared_lib.bundle_ubsan_rt = false;
        shared_lib.installHeader(b.path("include/fipc.h"), "fipc.h");
        shared_lib.installHeader(b.path("include/fipc.hpp"), "fipc.hpp");
        install_shared = b.addInstallArtifact(shared_lib, .{});
        b.getInstallStep().dependOn(&install_shared.?.step);
        shared = shared_lib;
    }

    // === The native Zig API: the build's one public module ===
    // Public, so that Zig programs can build the library in (`b.dependency("fipc", ...).module("fastipc")`;
    // examples/zig). Every other module of the build is private: only this build uses it.
    _ = library.publicModule(b, config, "fastipc", "src/zig/root.zig");

    // A build that depends on this package gets the library and the module, nothing else: the artifact "fastipc" is
    // the shared library, with fipc.h and fipc.hpp (`b.dependency("fipc", ...).artifact("fastipc")`;
    // examples/c-cpp), and the package (build.zig.zon's `paths`) leaves out the tests, the benchmarks and the
    // rest that the steps below build.
    if (!b.isRoot()) return;

    // `fastipc_static`: the library with the test hooks, for the C-API tests and the chaos peer. Not installed.
    const static_lib = b.addLibrary(.{
        .name = "fastipc_static",
        .linkage = .static,
        .root_module = library.libraryModule(b, config, .{ .test_hooks = true }),
        .use_llvm = library.tsanBackend(config.enable_tsan),
    });

    // === Tests: `zig build test` (fast) and `zig build test-slow`, not part of the default build ===
    const test_steps = tests.add(b, config, static_lib, shared, install_shared);

    // === The benchmark suite (bench/zig), installed in zig-out/bench. Not under TSan: nothing runs it there ===
    if (!config.enable_tsan) bench.add(b, config, test_steps.fast);

    // === The heavy harnesses: `zig build harness|soak|fuzz`, not part of the default build; driven by devtool. Not
    // under TSan: nothing runs them there ===
    if (!config.enable_tsan) harnesses.add(b, config, static_lib);

    // === The packaged shared libraries: `zig build dist` ===
    dist.add(b, config);

    // === The downstream examples: `zig build examples`, and in `test-slow` ===
    if (!config.enable_tsan) tests.addExamples(b, test_steps.slow);
    tests.runTimedLast(test_steps); // after every other step of `test-slow` is added

    // === The JavaScript binding's Node-API addon (bindings/js), installed as zig-out/lib/fipc.node ===
    if (!config.enable_tsan) {
        const addon = node_addon.addLibrary(b, targets.sharedLibraryTarget(b, config.target), config.optimize);
        b.getInstallStep().dependOn(&b.addInstallArtifact(addon, .{
            .dest_dir = .{ .override = .lib },
            .dest_sub_path = "fipc.node",
            .pdb_dir = .disabled,
            .implib_dir = .disabled,
        }).step);
    }

    // === compile_commands.json for IDE support: `zig build cdb` ===
    harnesses.addCompileCommands(b);
}
