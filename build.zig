const std = @import("std");
const Translator = @import("translate_c").Translator;

// === Source lists ===
// The library is all Zig: the shared and static libraries are rooted at src/zig/c_api.zig (the C API), the
// module "fastipc" at src/zig/root.zig (the native Zig API). The tests (tests/zig), the benchmarks (bench/zig) and
// the soak and fuzz harnesses are Zig; the one C file the build compiles, the chaos peer, is listed below.

/// The chaos harness peer (`zig build harness`, driven by `devtool.py test chaos`): one C program that uses the
/// library through its public header, include/fipc.h. Always optimized (`-O2`): it must spend its time in the library
/// under test, which keeps the build's mode, not in making and checking its own messages.
const chaos_peer_source = "tests/chaos/chaos.c";

/// The headers' directory: the public header (include/fipc.h), which the shared library installs, and which is
/// translate-c input for the ABI checks. The tests (tests/zig/c_api.h) compile against it too.
const library_include_dirs = [_][]const u8{
    "include",
};

pub fn build(b: *std.Build) void {
    // Standard target and optimization options
    const target = b.resolveTargetQuery(withCpuFloor(b.standardTargetOptionsQueryOnly(.{})));
    const opt_build: std.lang.Optimize = b.standardOptimizeOption(.{});

    // Build options
    const build_fastipc_shared = b.option(bool, "build_fastipc_shared", "Build FastIPC shared library") orelse true;
    const enable_tsan = b.option(bool, "enable_tsan", "Enable Thread Sanitizer (TSan) for race detection") orelse false;
    stream_pace = b.option(u32, "stream_pace", "Override the platform's reader spin pace (ring_wait.zig), for tuning");

    // === STEP 1: Build targets ===
    var shared: ?*std.Build.Step.Compile = null;
    var install_shared: ?*std.Build.Step.InstallArtifact = null;

    // === STEP 2: The library ===
    // One module per variant, rooted at src/zig/c_api.zig. `fastipc` (dynamic) is the shipped
    // library; `fastipc_static`, with the test hooks, is linked by the C-API tests and the chaos peer.
    // Every build of the library also runs the comptime ABI checks of src/zig/abi.zig.
    if (build_fastipc_shared) {
        const shared_lib = b.addLibrary(.{
            .name = "fastipc",
            .linkage = .dynamic,
            // macOS: no version, so the library is the one file libfastipc.dylib (install name
            // @rpath/libfastipc.dylib), as the packages ship it, not a chain of symlinks
            .version = if (target.result.os.tag == .macos) null else .{ .major = 1, .minor = 0, .patch = 0 },
            .root_module = createLibraryModule(b, sharedLibraryTarget(b, target), opt_build, enable_tsan, .dynamic, false),
            .use_llvm = tsanBackend(enable_tsan),
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

    // The native Zig API, for other Zig builds: `b.dependency("fipc", ...).module("fastipc")` (examples/zig)
    _ = createModule(b, "fastipc", "src/zig/root.zig", target, opt_build, enable_tsan, false, false);

    // A build that depends on this package gets the library and the module, nothing else: the artifact "fastipc" is
    // the shared library, with fipc.h and fipc.hpp (`b.dependency("fipc", ...).artifact("fastipc")`;
    // examples/c-cpp), and the package (build.zig.zon's `paths`) leaves out the tests, the benchmarks and the
    // rest that the steps below build.
    if (!b.isRoot()) return;

    const static_lib = addStaticLibrary(b, target, opt_build, enable_tsan);

    // === STEP 3: Zig tests (`zig build test`, `zig build test-slow`; not part of the default build) ===
    // `test` is the fast tier: the test blocks of every Zig file the library imports, run in a test build of the
    // library module and the single-process C-API tests of tests/zig. `test-slow` runs the unit tests that start
    // processes (named "slow: ", compiled in only with the `slow_tests` option, so the fast tier doesn't have them)
    // and the multi-process C-API tests.
    const test_step = b.step("test", "Run the fast tests: Zig unit tests, single-process C-API tests (tests/zig)");
    const test_slow_step = b.step("test-slow", "Run the slow tests: unit and C-API tests that use other processes");
    const unit_tests = b.addTest(.{
        .root_module = createLibraryModule(b, target, opt_build, enable_tsan, .static, false),
        .use_llvm = tsanBackend(enable_tsan),
    });
    test_step.dependOn(&runTests(b, unit_tests, target, enable_tsan).step);
    const test_filters = b.option(
        []const []const u8,
        "test-filter",
        "C-API tests (tests/zig): run only the tests whose name contains this text, in either tier",
    );
    const slow_unit_tests = b.addTest(.{
        .name = "unit_tests_slow",
        .root_module = createLibraryModule(b, target, opt_build, enable_tsan, .static, true),
        .filters = test_filters orelse &.{"slow: "},
        .use_llvm = tsanBackend(enable_tsan),
    });
    test_slow_step.dependOn(&runTests(b, slow_unit_tests, target, enable_tsan).step);
    addApiTests(b, target, opt_build, enable_tsan, static_lib, shared, test_filters, test_step, test_slow_step);
    addCppTests(b, target, opt_build, enable_tsan, static_lib, install_shared, test_filters, test_step, test_slow_step);

    // === STEP 3b: The benchmark suite (bench/zig), installed in zig-out/bench ===
    if (!enable_tsan) addBench(b, target);

    // === STEP 3c: The heavy harnesses (`zig build harness|soak|fuzz`, not part of the default build; driven by
    // devtool). Not built under TSan: nothing runs them there.
    if (!enable_tsan) {
        addChaosPeer(b, target, opt_build, static_lib);
        addHarness(b, "soak", "tests/soak/main.zig", target, opt_build);
        addHarness(b, "fuzz", "tests/fuzz/main.zig", target, opt_build);
    }

    // === STEP 3d: The packaged shared libraries (`zig build dist`) ===
    addDistStep(b);

    // === STEP 3e: The downstream examples (`zig build examples`, and in `test-slow`) ===
    if (!enable_tsan) addExamples(b, test_slow_step);

    // === STEP 3f: The JavaScript binding's Node-API addon (bindings/js), installed as zig-out/lib/fipc.node ===
    if (!enable_tsan) {
        const addon = addNodeAddon(b, sharedLibraryTarget(b, target), opt_build);
        b.getInstallStep().dependOn(&b.addInstallArtifact(addon, .{
            .dest_dir = .{ .override = .lib },
            .dest_sub_path = "fipc.node",
            .pdb_dir = .disabled,
            .implib_dir = .disabled,
        }).step);
    }

    // === STEP 4: compile_commands.json for IDE support (`zig build cdb`) ===
    addCompileCommands(b);
}

/// The library module, rooted at src/zig/c_api.zig. The lifecycle's test hooks go into the static variant
/// only; `slow_tests` compiles in the unit tests that start processes (the slow tier's unit test build).
fn createLibraryModule(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.lang.Optimize,
    enable_tsan: bool,
    linkage: std.lang.LinkMode,
    slow_tests: bool,
) *std.Build.Module {
    return createModule(b, null, "src/zig/c_api.zig", target, optimize, enable_tsan, linkage == .static, slow_tests);
}

/// The `stream_pace` option, null unless given: every module of the library gets it as a build option.
var stream_pace: ?u32 = null;

/// A module of the library's sources rooted at `root`, public as `name` if given. It imports the public headers as
/// `c_headers` (translated with `library_include_dirs`) for its ABI checks, and the `build_options` `test_hooks`,
/// `slow_tests` and `stream_pace`.
fn createModule(
    b: *std.Build,
    name: ?[]const u8,
    root: []const u8,
    target: std.Build.ResolvedTarget,
    optimize: std.lang.Optimize,
    enable_tsan: bool,
    test_hooks: bool,
    slow_tests: bool,
) *std.Build.Module {
    const headers = translateHeader(b, "src/zig/abi_headers.h", target, optimize);

    const options = b.addOptions();
    options.addOption(bool, "test_hooks", test_hooks);
    options.addOption(bool, "slow_tests", slow_tests);
    options.addOption(?u32, "stream_pace", stream_pace);

    const module_options: std.Build.Module.CreateOptions = .{
        .root_source_file = b.path(root),
        .target = target,
        .optimize = optimize,
        // Frame pointers in Debug and TSan builds only. Zig keeps them
        // in every mode but ReleaseSmall by default, which costs the hot paths a register.
        .omit_frame_pointer = optimize != .debug and !enable_tsan,
        .imports = &.{
            .{ .name = "c_headers", .module = headers },
            .{ .name = "build_options", .module = options.createModule() },
        },
    };
    const module = if (name) |n| b.addModule(n, module_options) else b.createModule(module_options);
    linkSystemLibraries(module, target, enable_tsan);
    return module;
}

/// A C header translated to a Zig module by the official translate-c package (vendored: vendor/README.md), with
/// `library_include_dirs` on its include path.
fn translateHeader(b: *std.Build, header: []const u8, target: std.Build.ResolvedTarget, optimize: std.lang.Optimize) *std.Build.Module {
    const translator: Translator = .init(b.dependency("translate_c", .{}), .{
        .c_source_file = b.path(header),
        .target = target,
        .optimize = optimize,
    });
    for (library_include_dirs) |dir| translator.addIncludePath(b.path(dir));
    return translator.mod;
}

/// The C-API test suite (tests/zig): Zig tests that call the library through its exported C API, as translated from
/// tests/zig/c_api.h, linked with `fastipc_static`, and the native API's multi-process tests,
/// which import the module "fastipc" with its test hooks. One test executable per tier, chosen by the tier prefix of
/// the test names ("fast: ", "slow: ") unless `-Dtest-filter` gives other filters; the slow tier's other processes
/// are the test peer (tests/zig/peer.zig), whose path the tests get through the `test_options` module.
fn addApiTests(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.lang.Optimize,
    enable_tsan: bool,
    static_lib: *std.Build.Step.Compile,
    shared_lib: ?*std.Build.Step.Compile,
    filters: ?[]const []const u8,
    fast_step: *std.Build.Step,
    slow_step: *std.Build.Step,
) void {
    const c_module = translateHeader(b, "tests/zig/c_api.h", target, optimize);
    const native = createModule(b, null, "src/zig/root.zig", target, optimize, enable_tsan, true, false);

    const peer = b.addExecutable(.{
        .name = "test_peer",
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/zig/peer.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "c", .module = c_module },
                .{ .name = "fastipc", .module = native },
            },
        }),
        .use_llvm = tsanBackend(enable_tsan),
    });
    peer.root_module.linkLibrary(static_lib);
    linkSystemLibraries(peer.root_module, target, enable_tsan);

    // The fast tier has no multi-process tests, so it doesn't wait for the peer's build (unless filters pick tests).
    // The slow tier also gets the shipped library's path, which a test loads and unloads (lifecycle_test.zig).
    const with_peer = b.addOptions();
    with_peer.addOptionPath("peer_exe", peer.getEmittedBin());
    if (shared_lib) |lib| with_peer.addOptionPath("shared_lib", lib.getEmittedBin()) else with_peer.addOption([]const u8, "shared_lib", "");
    const without_peer = b.addOptions();
    without_peer.addOption([]const u8, "peer_exe", "");
    without_peer.addOption([]const u8, "shared_lib", "");

    const tiers = [_]struct { name: []const u8, filter: []const u8, step: *std.Build.Step, options: *std.Build.Step.Options }{
        .{ .name = "api_tests_fast", .filter = "fast: ", .step = fast_step, .options = if (filters == null) without_peer else with_peer },
        .{ .name = "api_tests_slow", .filter = "slow: ", .step = slow_step, .options = with_peer },
    };
    for (tiers) |tier| {
        const module = b.createModule(.{
            .root_source_file = b.path("tests/zig/all.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "c", .module = c_module },
                .{ .name = "fastipc", .module = native },
                .{ .name = "test_options", .module = tier.options.createModule() },
            },
        });
        module.linkLibrary(static_lib);
        linkSystemLibraries(module, target, enable_tsan);
        const tests = b.addTest(.{
            .name = tier.name,
            .root_module = module,
            .filters = filters orelse &.{tier.filter},
            .use_llvm = tsanBackend(enable_tsan),
        });
        tier.step.dependOn(&runTests(b, tests, target, enable_tsan).step);
    }
}

/// The C++ wrapper's tests (tests/cpp/fipc_hpp_test.cpp, include/fipc.hpp): one C++20 program, linked with
/// `fastipc_static` and built with `cpp_test_flags`, run once per tier (`fast` in `test`, `slow` in `test-slow`) with
/// `-Dtest-filter`'s filters. Its slow tier runs a Python peer (tests/cpp/peer.py) through the Python binding, which
/// loads the shared library from zig-out: that run installs it first. Not under TSan, whose library Python can't load.
fn addCppTests(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.lang.Optimize,
    enable_tsan: bool,
    static_lib: *std.Build.Step.Compile,
    install_shared: ?*std.Build.Step.InstallArtifact,
    filters: ?[]const []const u8,
    fast_step: *std.Build.Step,
    slow_step: *std.Build.Step,
) void {
    const exe = b.addExecutable(.{
        .name = "fipc_hpp_test",
        .root_module = b.createModule(.{ .target = target, .optimize = optimize, .link_libcpp = true }),
        .use_llvm = tsanBackend(enable_tsan),
    });
    exe.root_module.addIncludePath(b.path("include"));
    exe.root_module.addCSourceFile(.{ .file = b.path("tests/cpp/fipc_hpp_test.cpp"), .flags = &cpp_test_flags });
    exe.root_module.linkLibrary(static_lib);
    linkSystemLibraries(exe.root_module, target, enable_tsan);

    const tiers = [_]struct { name: []const u8, step: *std.Build.Step, install: ?*std.Build.Step.InstallArtifact }{
        .{ .name = "fast", .step = fast_step, .install = null },
        .{ .name = "slow", .step = slow_step, .install = if (enable_tsan) null else install_shared },
    };
    for (tiers) |tier| {
        const run = runTests(b, exe, target, enable_tsan);
        run.has_side_effects = true;
        run.addArg(tier.name);
        run.addArtifactArg(exe);
        run.addDirectoryArg(b.path("."));
        if (enable_tsan) run.addArg("--no-interop");
        for (filters orelse &.{}) |filter| run.addArg(filter);
        if (tier.install) |install| run.step.dependOn(&install.step);
        tier.step.dependOn(&run.step);
    }
}

/// The flags of the C++ wrapper's tests: C++20, no exceptions or RTTI, and every warning an error, the strict ones
/// included, so that include/fipc.hpp compiles cleanly in the strictest of its users' builds.
const cpp_test_flags = [_][]const u8{
    "-std=c++20",   "-fno-exceptions",   "-fno-rtti", "-Wall",            "-Wextra", "-Wpedantic",
    "-Wconversion", "-Wsign-conversion", "-Wshadow",  "-Wold-style-cast", "-Werror",
};

/// The CPU every build targets unless `-Dcpu` names another: x86-64-v3 (AVX2), the platform's minimum
/// (docs/platform-support.md). The tests, the benchmarks and the packages then run the code that ships, not code
/// tuned to the build machine (`-Dcpu=native`) or to a CPU FastIPC doesn't support (`-Dcpu=baseline`).
const cpu_floor = &std.Target.x86.cpu.x86_64_v3;

/// `query` with the CPU floor, if it names no CPU and its architecture is x86-64 (the host's, if it names none); for
/// macOS on aarch64, with the macOS floors (`withMacosFloors`); for Linux on aarch64, with the ARM64 Linux CPU floor if
/// it names no CPU.
fn withCpuFloor(query: std.Target.Query) std.Target.Query {
    var result = query;
    const arch = query.cpu_arch orelse @import("builtin").target.cpu.arch;
    const os = query.os_tag orelse @import("builtin").target.os.tag;
    if (query.cpu_model == .determined_by_arch_os and arch == .x86_64) result.cpu_model = .{ .explicit = cpu_floor };
    if (arch == .aarch64 and os == .macos) result = withMacosFloors(result);
    if (query.cpu_model == .determined_by_arch_os and arch == .aarch64 and os == .linux) result.cpu_model = .{ .explicit = linux_arm64_cpu_floor };
    return result;
}

/// The CPU every ARM64 Linux build targets unless `-Dcpu` names another: generic ARMv8.0-A with NEON, the oldest
/// 64-bit ARM (docs/platform-support.md). A build on a newer core would otherwise use instructions older ones lack.
const linux_arm64_cpu_floor = &std.Target.aarch64.cpu.generic;

/// The CPU every macOS build targets unless `-Dcpu` names another: the Apple M1, the first Apple Silicon chip
/// (docs/platform-support.md). A build on a later chip would otherwise use instructions an M1 lacks.
const macos_cpu_floor = &std.Target.aarch64.cpu.apple_m1;
/// The oldest macOS every macOS build runs on, unless the target names another: 14.4, the first with
/// `os_sync_wait_on_address` (platform/macos.zig). A build would otherwise require the build machine's macOS.
const macos_version_floor: std.SemanticVersion = .{ .major = 14, .minor = 4, .patch = 0 };

/// `query` (aarch64-macos) with the macOS CPU floor if it names no CPU, and the macOS version floor as its minimum if it
/// names none.
fn withMacosFloors(query: std.Target.Query) std.Target.Query {
    var result = query;
    if (query.cpu_model == .determined_by_arch_os) result.cpu_model = .{ .explicit = macos_cpu_floor };
    if (query.os_version_min == null) result.os_version_min = .{ .semver = macos_version_floor };
    return result;
}

/// The shipped library's target: Windows builds are always the GNU ABI.
fn sharedLibraryTarget(b: *std.Build, target: std.Build.ResolvedTarget) std.Build.ResolvedTarget {
    if (target.result.os.tag != .windows) return target;
    var query = target.query;
    query.os_tag = .windows;
    query.abi = .gnu;
    return b.resolveTargetQuery(query);
}

/// The benchmark suite (bench/zig): `fipc_bench` and the library it measures by default, this tree's shared library
/// built ReleaseFast whatever the build's mode, both installed in zig-out/bench. The suite loads the library at run
/// time (`--lib`), so the same binary also measures another revision's library that speaks include/fipc.h
/// (`devtool bench-compare --ab`). Its unit tests run in `zig build test`. Not built with TSan: nothing runs it there.
fn addBench(b: *std.Build, target: std.Build.ResolvedTarget) void {
    const install_dir: std.Build.InstallDir = .{ .custom = "bench" };
    const c_api = translateHeader(b, "bench/zig/c_api.h", target, .fast);
    const module = b.createModule(.{
        .root_source_file = b.path("bench/zig/main.zig"),
        .target = target,
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
        .root_module = createLibraryModule(b, sharedLibraryTarget(b, target), .fast, false, .dynamic, false),
    });
    lib.bundle_ubsan_rt = false;
    b.getInstallStep().dependOn(&b.addInstallArtifact(lib, .{
        .dest_dir = .{ .override = install_dir },
        .pdb_dir = .disabled,
        .implib_dir = .disabled,
    }).step);

    // The JavaScript binding's addon beside it, ReleaseFast too: the language benchmarks load both from here
    // (FASTIPC_LIB_DIR), never the build's mode, which is Debug after a test run
    const addon = addNodeAddon(b, sharedLibraryTarget(b, target), .fast);
    b.getInstallStep().dependOn(&b.addInstallArtifact(addon, .{
        .dest_dir = .{ .override = install_dir },
        .dest_sub_path = "fipc.node",
        .pdb_dir = .disabled,
        .implib_dir = .disabled,
    }).step);

    const tests = b.addTest(.{ .root_module = module });
    b.top_level_steps.get("test").?.step.dependOn(&b.addRunArtifact(tests).step);

    // The Zig benchmark (bench/zig-api): the same cases through the native module, built ReleaseFast
    const zig_bench = b.addExecutable(.{
        .name = "zig_bench",
        .root_module = b.createModule(.{
            .root_source_file = b.path("bench/zig-api/main.zig"),
            .target = target,
            .optimize = .fast,
            .imports = &.{.{ .name = "fastipc", .module = createModule(b, null, "src/zig/root.zig", target, .fast, false, false, false) }},
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
                .target = target,
                .optimize = .fast,
                .link_libc = true,
                .link_libcpp = program.cpp,
            }),
        });
        bench_exe.root_module.addIncludePath(b.path("include"));
        bench_exe.root_module.addCSourceFile(.{ .file = b.path("bench/c/main.c"), .flags = &bench_c_flags });
        bench_exe.root_module.addCSourceFile(.{ .file = b.path(program.loops), .flags = if (program.cpp) &bench_cpp_flags else &bench_c_flags });
        bench_exe.root_module.linkLibrary(lib);
        if (target.result.os.tag == .macos) bench_exe.root_module.addRPathSpecial("@loader_path") else if (target.result.os.tag != .windows) bench_exe.root_module.addRPathSpecial("$ORIGIN");
        b.getInstallStep().dependOn(&b.addInstallArtifact(bench_exe, .{
            .dest_dir = .{ .override = install_dir },
            .pdb_dir = .disabled,
        }).step);
    }
}

/// The flags of the C and C++ benchmark (bench/c): C11 and C++20 (no exceptions or RTTI), every warning an error, the
/// library's functions declared dllimport on Windows.
const bench_c_flags = [_][]const u8{ "-std=c11", "-Wall", "-Wextra", "-Wpedantic", "-Werror", "-DFASTIPC_SHARED" };
const bench_cpp_flags = [_][]const u8{ "-std=c++20", "-fno-exceptions", "-fno-rtti", "-Wall", "-Wextra", "-Wpedantic", "-Werror", "-DFASTIPC_SHARED" };

/// `zig build dist`: the shared libraries the NuGet and Python packages ship, buildable on any OS
/// (Zig cross-compiles all four): the CPU floors whatever `-Dcpu` says (x86-64-v3, generic ARMv8.0-A for ARM64
/// Linux), glibc 2.34 as the Linux floor, and the Apple M1 and macOS 14.4 as the macOS floors
/// (docs/platform-support.md), ReleaseFast and stripped (no debug info, so no build-machine paths either). Installed in the NuGet `runtimes/` layout under zig-out/dist, the Windows DLL with
/// its import library (`fastipc.lib`, for C and C++ programs that link it; the packages leave it out), and the C and
/// C++ headers in zig-out/dist/include.
fn addDistStep(b: *std.Build) void {
    const dist = b.step("dist", "Build the packaged shared libraries (x86-64-v3, glibc 2.34; arm64 Linux glibc 2.34; arm64 macOS 14.4) into zig-out/dist");
    for ([_][]const u8{ "fipc.h", "fipc.hpp" }) |header| {
        const install = b.addInstallFileWithDir(b.path(b.fmt("include/{s}", .{header})), .{ .custom = "dist/include" }, header);
        dist.dependOn(&install.step);
    }
    // The floors of docs/platform-support.md
    const cpu: std.Target.Query.CpuModel = .{ .explicit = cpu_floor };
    const targets = [_]struct { rid: []const u8, query: std.Target.Query }{
        .{ .rid = "linux-x64", .query = .{
            .cpu_arch = .x86_64,
            .cpu_model = cpu,
            .os_tag = .linux,
            .abi = .gnu,
            .glibc_version = .{ .major = 2, .minor = 34, .patch = 0 },
        } },
        .{ .rid = "win-x64", .query = .{
            .cpu_arch = .x86_64,
            .cpu_model = cpu,
            .os_tag = .windows,
            .abi = .gnu,
        } },
        .{ .rid = "osx-arm64", .query = .{
            .cpu_arch = .aarch64,
            .cpu_model = .{ .explicit = macos_cpu_floor },
            .os_tag = .macos,
            .os_version_min = .{ .semver = macos_version_floor },
        } },
        .{ .rid = "linux-arm64", .query = .{
            .cpu_arch = .aarch64,
            .cpu_model = .{ .explicit = linux_arm64_cpu_floor },
            .os_tag = .linux,
            .abi = .gnu,
            .glibc_version = .{ .major = 2, .minor = 34, .patch = 0 },
        } },
    };
    for (targets) |t| {
        const lib = b.addLibrary(.{
            .name = "fastipc",
            .linkage = .dynamic,
            // No .version: a package holds the one file libfastipc.so, not a symlink chain
            .root_module = createLibraryModule(b, b.resolveTargetQuery(t.query), .fast, false, .dynamic, false),
        });
        lib.bundle_ubsan_rt = false;
        lib.root_module.strip = true;
        const dir: std.Build.InstallDir = .{ .custom = b.fmt("dist/runtimes/{s}/native", .{t.rid}) };
        const install = b.addInstallArtifact(lib, .{
            .dest_dir = .{ .override = dir },
            .pdb_dir = .disabled,
            .implib_dir = if (t.query.os_tag == .windows) .{ .override = dir } else .disabled,
        });
        dist.dependOn(&install.step);

        // The JavaScript binding's addon for the same platform (zig-out/dist/node/<rid>/fipc.node)
        const addon = addNodeAddon(b, b.resolveTargetQuery(t.query), .fast);
        dist.dependOn(&b.addInstallArtifact(addon, .{
            .dest_dir = .{ .override = .{ .custom = b.fmt("dist/node/{s}", .{t.rid}) } },
            .dest_sub_path = "fipc.node",
            .pdb_dir = .disabled,
            .implib_dir = .disabled,
        }).step);
    }
}

/// The JavaScript binding's Node-API addon (bindings/js/src): one C file against the Node-API headers (vendored:
/// vendor/README.md) and include/fipc.h, which loads the library at run time. It links nothing of the JavaScript
/// runtime, so it builds for any target: on Linux and macOS its Node-API functions stay undefined until the host
/// process (Node.js, Bun, Deno) provides them; on Windows it looks them up in the host executable when it loads
/// (bindings/js/src/napi_windows.h). The one file serves all three runtimes; index.cjs loads it as `fipc.node`.
fn addNodeAddon(b: *std.Build, target: std.Build.ResolvedTarget, optimize: std.lang.Optimize) *std.Build.Step.Compile {
    const addon = b.addLibrary(.{
        .name = "fipc_node",
        .linkage = .dynamic,
        .root_module = b.createModule(.{ .target = target, .optimize = optimize, .link_libc = true }),
    });
    addon.root_module.addIncludePath(b.path("include"));
    addon.root_module.addIncludePath(b.path("vendor/node-api-headers/include"));
    addon.root_module.addCSourceFile(.{ .file = b.path("bindings/js/src/fipc_node.c"), .flags = &node_addon_flags });
    if (target.result.os.tag != .windows) addon.linker_allow_shlib_undefined = true;
    if (optimize != .debug) addon.root_module.strip = true;
    return addon;
}

/// The flags of the Node-API addon: C11, every warning an error.
const node_addon_flags = [_][]const u8{ "-std=c11", "-Wall", "-Wextra", "-Wpedantic", "-Werror" };

/// `zig build examples`: builds examples/c-cpp and examples/zig, projects of their own that depend on this one by path,
/// as a downstream user's build does: C and C++ servers and clients against the shared library, and Zig ones that
/// import the module. The slow tier runs it, so the consumer's view of the package (its artifact, its headers, its
/// module) stays working.
fn addExamples(b: *std.Build, slow_step: *std.Build.Step) void {
    const step = b.step("examples", "Build examples/c-cpp and examples/zig, downstream projects that depend on this one");
    for ([_][]const u8{ "examples/c-cpp", "examples/zig" }) |dir| {
        const build_example = b.addSystemCommand(&.{ b.graph.zig_exe, "build", "--summary", "none" });
        build_example.setCwd(b.path(dir));
        build_example.has_side_effects = true;
        step.dependOn(&build_example.step);
        slow_step.dependOn(&build_example.step);
    }
}

/// `fastipc_static`: the library for the C-API tests and the chaos peer, with the test hooks. Not installed.
fn addStaticLibrary(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.lang.Optimize,
    enable_tsan: bool,
) *std.Build.Step.Compile {
    return b.addLibrary(.{
        .name = "fastipc_static",
        .linkage = .static,
        .root_module = createLibraryModule(b, target, optimize, enable_tsan, .static, false),
        .use_llvm = tsanBackend(enable_tsan),
    });
}

/// The `harness` step: the chaos harness peer (tests/chaos/chaos.c), built against this tree's fastipc_static (whose
/// test hook `FASTIPC_TEST_CRASH_AT` the crash-at-step scenario uses) and installed into zig-out/bin as `chaos_peer`.
fn addChaosPeer(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.lang.Optimize,
    static_lib: *std.Build.Step.Compile,
) void {
    const harness_step = b.step("harness", "Build the chaos harness peer (tests/chaos)");
    const peer = b.addExecutable(.{
        .name = "chaos_peer",
        .root_module = b.createModule(.{ .target = target, .optimize = optimize }),
    });
    peer.root_module.addIncludePath(b.path("include"));
    peer.root_module.linkLibrary(static_lib);
    peer.root_module.addCSourceFile(.{
        .file = b.path(chaos_peer_source),
        .flags = &chaos_peer_flags,
    });
    linkSystemLibraries(peer.root_module, target, false);
    harness_step.dependOn(&b.addInstallArtifact(peer, .{}).step);
}

/// The flags `addChaosPeer` compiles the chaos peer with: the one C file the build compiles.
const chaos_peer_flags = [_][]const u8{ "-std=c99", "-Wall", "-Wextra", "-Werror", "-O2", "-g" };

/// `zig build cdb`: compile_commands.json at the repository root, for CLion and clangd: the one C file the build
/// compiles (the chaos peer), with its include directories and flags. The library itself is Zig, which these tools
/// don't read.
fn addCompileCommands(b: *std.Build) void {
    var arguments: std.ArrayList([]const u8) = .empty;
    arguments.append(b.allocator, "clang") catch @panic("OOM");
    for (library_include_dirs) |dir| arguments.append(b.allocator, b.fmt("-I{s}", .{dir})) catch @panic("OOM");
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

/// A Zig harness executable (`zig build <name>`) rooted at `src`, importing the native module "fastipc" (built with
/// its test hooks, so the fuzz harness reaches `fastipc.internal`: the names, wire, segment and platform helpers it
/// needs to be a corrupt peer). The library's Zig sources compile into the executable through that module; the
/// executable keeps the build's optimization mode, so its safety checks run. `python devtool.py test soak|fuzz`
/// builds and runs it.
fn addHarness(
    b: *std.Build,
    name: []const u8,
    src: []const u8,
    target: std.Build.ResolvedTarget,
    optimize: std.lang.Optimize,
) void {
    const step = b.step(name, b.fmt("Build the {s} harness (tests/{s})", .{ name, name }));
    const native = createModule(b, null, "src/zig/root.zig", target, optimize, false, true, false);
    const exe = b.addExecutable(.{
        .name = name,
        .root_module = b.createModule(.{
            .root_source_file = b.path(src),
            .target = target,
            .optimize = optimize,
            .imports = &.{.{ .name = "fastipc", .module = native }},
        }),
    });
    linkSystemLibraries(exe.root_module, target, false);
    step.dependOn(&b.addInstallArtifact(exe, .{}).step);
}

/// Runs a test program. Under TSan on macOS, `_exit` waits `atexit_sleep_ms` (1000 ms by default) before the process
/// ends, so a test peer that dies by `_exit` (platform/macos.zig's `crash`) would end a second late, past the tests'
/// windows for seeing a peer's death; the run sets `atexit_sleep_ms=0` ahead of any TSAN_OPTIONS already set, which
/// may override it. Every other build runs the program as `addRunArtifact` does.
fn runTests(b: *std.Build, exe: *std.Build.Step.Compile, target: std.Build.ResolvedTarget, enable_tsan: bool) *std.Build.Step.Run {
    const run = b.addRunArtifact(exe);
    if (enable_tsan and target.result.os.tag == .macos) {
        const options = if (b.graph.environ_map.get("TSAN_OPTIONS")) |given| b.fmt("atexit_sleep_ms=0:{s}", .{given}) else "atexit_sleep_ms=0";
        run.setEnvironmentVariable("TSAN_OPTIONS", options);
    }
    return run;
}

/// The backend of every compilation with Zig code in a TSan build: Zig's self-hosted x86_64 backend, the default for
/// Debug, ignores `sanitize_thread` without a word, so the Zig code would carry no TSan instrumentation; LLVM adds it.
fn tsanBackend(enable_tsan: bool) ?bool {
    return if (enable_tsan) true else null;
}

fn linkSystemLibraries(module: *std.Build.Module, target: std.Build.ResolvedTarget, enable_tsan: bool) void {
    if (enable_tsan) {
        module.sanitize_thread = true;
    }

    switch (target.result.os.tag) {
        .linux => {
            module.linkSystemLibrary("pthread", .{});
            module.linkSystemLibrary("rt", .{});
        },
        .windows => {
            module.linkSystemLibrary("kernel32", .{});
            module.linkSystemLibrary("user32", .{});
            module.linkSystemLibrary("advapi32", .{}); // the named objects' security (platform/windows.zig)
        },
        else => {},
    }
    module.link_libc = true;
}
