//! The test tiers, `zig build test` (fast) and `zig build test-slow`, and the downstream examples the slow tier builds.
//! `test` is the fast tier: the test blocks of every Zig file the library imports, run in a test build of the library
//! module, and the single-process C-API tests of tests/zig. `test-slow` runs the unit tests that start processes (named
//! "slow: ", compiled in only with the `slow_tests` build option, so the fast tier doesn't have them) and the
//! multi-process C-API tests. The slow tier's tests mostly wait for other processes, so it runs them in parallel: its
//! three stress tests each in a run of its own, and the C-API tests in shards, all at once, but for the runs with timed
//! bounds: the other slow unit tests and the C-API timed tests' shard, which run together once everything else in the
//! tier is done.

const std = @import("std");
const library = @import("library.zig");

const Config = library.Config;

/// The two tiers' steps, which other parts of the build add to, and the slow tier's two runs with timed bounds
/// (`runTimedLast`): the C-API timed tests' shard and the slow unit tests but for the stress tests.
pub const Steps = struct {
    fast: *std.Build.Step,
    slow: *std.Build.Step,
    timed: *std.Build.Step,
    unit_timed: *std.Build.Step,
};

/// The shards of the slow tier's C-API tests: tests/zig/support.zig's `begin` skips a test of another shard than its
/// run's `FIPC_TEST_SHARD`. Shard 1 has the timed tests (`beginTimed`); the others share out the rest.
const api_shards = 4;

/// The unit tests' stress tests (one per kind, seconds each), which the slow tier runs one per run, in parallel; each
/// skips itself when `FIPC_SKIP_STRESS_<kind>` is set.
const stress_kinds = [_][]const u8{ "QUEUE", "SERVICE", "BROADCAST" };

/// The steps `test` and `test-slow` with the unit tests, the C-API tests and the C++ wrapper's tests. `static_lib` is
/// `fastipc_static`; `shared_lib` and `install_shared` are the shipped library and its install step, if built.
pub fn add(
    b: *std.Build,
    config: Config,
    static_lib: *std.Build.Step.Compile,
    shared_lib: ?*std.Build.Step.Compile,
    install_shared: ?*std.Build.Step.InstallArtifact,
) Steps {
    var steps: Steps = .{
        .fast = b.step("test", "Run the fast tests: Zig unit tests, single-process C-API tests (tests/zig)"),
        .slow = b.step("test-slow", "Run the slow tests: unit and C-API tests that use other processes"),
        .timed = undefined,
        .unit_timed = undefined,
    };
    const unit_tests = b.addTest(.{
        .root_module = library.libraryModule(b, config, .{ .test_hooks = true }),
        .use_llvm = library.tsanBackend(config.enable_tsan),
    });
    steps.fast.dependOn(&runTests(b, unit_tests, config).step);
    const filters = b.option(
        []const []const u8,
        "test-filter",
        "C-API tests (tests/zig): run only the tests whose name contains this text, in either tier",
    );
    const slow_unit_module = library.libraryModule(b, config, .{ .test_hooks = true, .slow_tests = true });
    const slow_unit_tests = b.addTest(.{
        .name = "unit_tests_slow",
        .root_module = slow_unit_module,
        .filters = filters orelse &.{"slow: "},
        .use_llvm = library.tsanBackend(config.enable_tsan),
    });
    const slow_unit_run = runTests(b, slow_unit_tests, config);
    steps.slow.dependOn(&slow_unit_run.step);
    steps.unit_timed = &slow_unit_run.step;
    // The stress tests on their own (an executable of only them, run once per test), unless filters pick the tests
    if (filters == null) {
        const stress_tests = b.addTest(.{
            .name = "unit_tests_stress",
            .root_module = slow_unit_module,
            .filters = &.{"slow: stress: "},
            .use_llvm = library.tsanBackend(config.enable_tsan),
        });
        for (stress_kinds) |kind| {
            slow_unit_run.setEnvironmentVariable(b.fmt("FIPC_SKIP_STRESS_{s}", .{kind}), "1");
            const run = runTests(b, stress_tests, config);
            run.setName(b.fmt("run test unit_tests_stress {s}", .{kind}));
            for (stress_kinds) |other| {
                if (!std.mem.eql(u8, other, kind)) run.setEnvironmentVariable(b.fmt("FIPC_SKIP_STRESS_{s}", .{other}), "1");
            }
            steps.slow.dependOn(&run.step);
        }
    }
    steps.timed = addApiTests(b, config, static_lib, shared_lib, filters, steps);
    addCppTests(b, config, static_lib, install_shared, filters, steps);
    return steps;
}

/// Makes the slow tier's two runs with timed bounds (the slow unit tests, whose races bound a cancel's or a wake's
/// delay, and the C-API timed tests' shard) run after every other step of the tier, the two together: two test
/// processes at once, as the tier ran before it had shards. Called once the build has added all of them. (A failed step
/// before them leaves them not run: the build runner has no dependency that only orders.)
pub fn runTimedLast(steps: Steps) void {
    const others = steps.slow.dependencies.items;
    for (others) |other| {
        if (other == steps.timed or other == steps.unit_timed) continue;
        steps.timed.dependOn(other);
        steps.unit_timed.dependOn(other);
    }
}

/// The C-API test suite (tests/zig): Zig tests that call the library through its exported C API, the module "c"
/// translated from include/fipc.h, linked with `fastipc_static`, and the native API's multi-process tests, which
/// import the module "fastipc" with its test hooks. One test executable per tier, chosen by the tier prefix of the
/// test names ("fast: ", "slow: ") unless `-Dtest-filter` gives other filters; the slow tier's other processes
/// are the test peer (tests/zig/peer.zig), whose path the tests get through the `test_options` module. The slow tier
/// runs its executable once per shard (`api_shards`); returns the timed tests' run, shard 1.
fn addApiTests(
    b: *std.Build,
    config: Config,
    static_lib: *std.Build.Step.Compile,
    shared_lib: ?*std.Build.Step.Compile,
    filters: ?[]const []const u8,
    steps: Steps,
) *std.Build.Step {
    const c_module = library.translateHeader(b, library.public_header, config.target, config.optimize);
    const native = library.privateModule(b, config, "src/zig/root.zig", .{ .test_hooks = true });

    const peer = b.addExecutable(.{
        .name = "test_peer",
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/zig/peer.zig"),
            .target = config.target,
            .optimize = config.optimize,
            .imports = &.{
                .{ .name = "c", .module = c_module },
                .{ .name = "fastipc", .module = native },
            },
        }),
        .use_llvm = library.tsanBackend(config.enable_tsan),
    });
    peer.root_module.linkLibrary(static_lib);
    library.linkSystemLibraries(peer.root_module, config);

    // The fast tier has no multi-process tests, so it doesn't wait for the peer's build (unless filters pick tests).
    // The slow tier also gets the shipped library's path, which a test loads and unloads (stream/lifecycle_test.zig).
    const with_peer = b.addOptions();
    with_peer.addOptionPath("peer_exe", peer.getEmittedBin());
    if (shared_lib) |lib| with_peer.addOptionPath("shared_lib", lib.getEmittedBin()) else with_peer.addOption([]const u8, "shared_lib", "");
    const without_peer = b.addOptions();
    without_peer.addOption([]const u8, "peer_exe", "");
    without_peer.addOption([]const u8, "shared_lib", "");

    const tiers = [_]struct { name: []const u8, filter: []const u8, step: *std.Build.Step, options: *std.Build.Step.Options }{
        .{ .name = "api_tests_fast", .filter = "fast: ", .step = steps.fast, .options = if (filters == null) without_peer else with_peer },
        .{ .name = "api_tests_slow", .filter = "slow: ", .step = steps.slow, .options = with_peer },
    };
    var timed: ?*std.Build.Step = null;
    for (tiers) |tier| {
        const module = b.createModule(.{
            .root_source_file = b.path("tests/zig/all.zig"),
            .target = config.target,
            .optimize = config.optimize,
            .imports = &.{
                .{ .name = "c", .module = c_module },
                .{ .name = "fastipc", .module = native },
                .{ .name = "test_options", .module = tier.options.createModule() },
            },
        });
        module.linkLibrary(static_lib);
        library.linkSystemLibraries(module, config);
        const tests = b.addTest(.{
            .name = tier.name,
            .root_module = module,
            .filters = filters orelse &.{tier.filter},
            .use_llvm = library.tsanBackend(config.enable_tsan),
        });
        if (tier.step != steps.slow) {
            tier.step.dependOn(&runTests(b, tests, config).step);
            continue;
        }
        for (1..api_shards + 1) |shard| {
            const run = runTests(b, tests, config);
            run.setName(b.fmt("run test {s} shard {d}/{d}{s}", .{ tier.name, shard, api_shards, if (shard == 1) " (timed, last)" else "" }));
            run.setEnvironmentVariable("FIPC_TEST_SHARD", b.fmt("{d}/{d}", .{ shard, api_shards }));
            tier.step.dependOn(&run.step);
            if (shard == 1) timed = &run.step;
        }
    }
    return timed.?;
}

/// The C++ wrapper's tests (tests/cpp/fipc_hpp_test.cpp, include/fipc.hpp): one C++20 program, linked with
/// `fastipc_static` and built with `cpp_test_flags`, run once per tier (`fast` in `test`, `slow` in `test-slow`) with
/// `-Dtest-filter`'s filters. Its slow tier runs a Python peer (tests/cpp/peer.py) through the Python binding, which
/// loads the shared library from zig-out: that run installs it first. Not under TSan, whose library Python can't load.
fn addCppTests(
    b: *std.Build,
    config: Config,
    static_lib: *std.Build.Step.Compile,
    install_shared: ?*std.Build.Step.InstallArtifact,
    filters: ?[]const []const u8,
    steps: Steps,
) void {
    const exe = b.addExecutable(.{
        .name = "fipc_hpp_test",
        .root_module = b.createModule(.{ .target = config.target, .optimize = config.optimize, .link_libcpp = true }),
        .use_llvm = library.tsanBackend(config.enable_tsan),
    });
    exe.root_module.addIncludePath(b.path("include"));
    exe.root_module.addCSourceFile(.{ .file = b.path("tests/cpp/fipc_hpp_test.cpp"), .flags = &cpp_test_flags });
    exe.root_module.linkLibrary(static_lib);
    library.linkSystemLibraries(exe.root_module, config);

    const tiers = [_]struct { name: []const u8, step: *std.Build.Step, install: ?*std.Build.Step.InstallArtifact }{
        .{ .name = "fast", .step = steps.fast, .install = null },
        .{ .name = "slow", .step = steps.slow, .install = if (config.enable_tsan) null else install_shared },
    };
    for (tiers) |tier| {
        const run = runTests(b, exe, config);
        run.has_side_effects = true;
        run.addArg(tier.name);
        run.addArtifactArg(exe);
        run.addDirectoryArg(b.path("."));
        if (config.enable_tsan) run.addArg("--no-interop");
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

/// `zig build examples`: builds examples/c-cpp and examples/zig, projects of their own that depend on this one by path,
/// as a downstream user's build does: C and C++ servers and clients against the shared library, and Zig ones that
/// import the module. The slow tier runs it, so the consumer's view of the package (its artifact, its headers, its
/// module) stays working.
pub fn addExamples(b: *std.Build, slow_step: *std.Build.Step) void {
    const step = b.step("examples", "Build examples/c-cpp and examples/zig, downstream projects that depend on this one");
    for ([_][]const u8{ "examples/c-cpp", "examples/zig" }) |dir| {
        const build_example = b.addSystemCommand(&.{ b.graph.zig_exe, "build", "--summary", "none" });
        build_example.setCwd(b.path(dir));
        build_example.has_side_effects = true;
        step.dependOn(&build_example.step);
        slow_step.dependOn(&build_example.step);
    }
}

/// Runs a test program. Under TSan on macOS, `_exit` waits `atexit_sleep_ms` (1000 ms by default) before the process
/// ends, so a test peer that dies by `_exit` (platform/macos/macos.zig's `crash`) would end a second late, past the
/// tests' windows for seeing a peer's death; the run sets `atexit_sleep_ms=0` ahead of any TSAN_OPTIONS already set,
/// which may override it. Every other build runs the program as `addRunArtifact` does.
pub fn runTests(b: *std.Build, exe: *std.Build.Step.Compile, config: Config) *std.Build.Step.Run {
    const run = b.addRunArtifact(exe);
    if (config.enable_tsan and config.target.result.os.tag == .macos) {
        const options = if (b.graph.environ_map.get("TSAN_OPTIONS")) |given| b.fmt("atexit_sleep_ms=0:{s}", .{given}) else "atexit_sleep_ms=0";
        run.setEnvironmentVariable("TSAN_OPTIONS", options);
    }
    return run;
}
