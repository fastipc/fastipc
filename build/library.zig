//! The modules of the library's sources and what every compilation of them shares: the settings the command line gives
//! (`Config`), the module's visibility (public: other packages import it; private: only this build uses it), its
//! imports and the system libraries it links.

const std = @import("std");
const Translator = @import("translate_c").Translator;

/// The headers' directory: the public header, which the shared library installs.
pub const include_dirs = [_][]const u8{
    "include",
};

/// The public header, translate-c input (`translateHeader`) for the library's ABI checks, the C-API tests and the
/// benchmark suite: each sees the C API as a Zig module (the build-system form of @cImport).
pub const public_header = "include/fipc.h";

/// What the command line asks of every compilation: `build` reads it once and passes it to the helpers, which change
/// a field where a compilation needs another (`withTarget`, `releaseFast`).
pub const Config = struct {
    target: std.Build.ResolvedTarget,
    optimize: std.lang.Optimize,
    enable_tsan: bool,
    /// The `stream_pace` option, null unless given: every module of the library gets it as a build option.
    stream_pace: ?u32,

    /// This configuration for another target.
    pub fn withTarget(config: Config, target: std.Build.ResolvedTarget) Config {
        var result = config;
        result.target = target;
        return result;
    }

    /// This configuration ReleaseFast and without TSan: the benchmarks' and the packaged libraries'.
    pub fn releaseFast(config: Config) Config {
        var result = config;
        result.optimize = .fast;
        result.enable_tsan = false;
        return result;
    }
};

/// What a module compiles in besides the shipped code: its `build_options` `test_hooks` and `slow_tests`.
pub const BuildOptions = struct {
    /// The lifecycle's test hooks (`if (build_options.test_hooks)`): for the static library, the tests and the
    /// harnesses; never in the shipped library or the public module.
    test_hooks: bool = false,
    /// The unit tests that start processes: the slow tier's unit test build only.
    slow_tests: bool = false,
};

/// A public module: `b.addModule` registers it with this package under `name`, so a project that depends on the
/// package imports it with `b.dependency("fipc", ...).module(name)`. Without test hooks: it is what users build in.
pub fn publicModule(b: *std.Build, config: Config, name: []const u8, root: []const u8) *std.Build.Module {
    const module = b.addModule(name, moduleOptions(b, config, root));
    setUp(b, module, config, .{});
    return module;
}

/// A private module: `b.createModule` registers nothing, so only this build uses it (the libraries, the tests, the
/// benchmarks, the harnesses).
pub fn privateModule(b: *std.Build, config: Config, root: []const u8, build_options: BuildOptions) *std.Build.Module {
    const module = b.createModule(moduleOptions(b, config, root));
    setUp(b, module, config, build_options);
    return module;
}

/// The library's module, rooted at src/zig/c_api.zig (the C API), private: the shared and static libraries and the
/// unit tests are compiled from it.
pub fn libraryModule(b: *std.Build, config: Config, build_options: BuildOptions) *std.Build.Module {
    return privateModule(b, config, "src/zig/c_api.zig", build_options);
}

/// The options of a module of the library's sources rooted at `root`.
fn moduleOptions(b: *std.Build, config: Config, root: []const u8) std.Build.Module.CreateOptions {
    return .{
        .root_source_file = b.path(root),
        .target = config.target,
        .optimize = config.optimize,
        // Frame pointers in Debug and TSan builds only. Zig keeps them
        // in every mode but ReleaseSmall by default, which costs the hot paths a register.
        .omit_frame_pointer = config.optimize != .debug and !config.enable_tsan,
    };
}

/// Gives a module of the library's sources its imports, the public header as `c_headers` (translated with
/// `include_dirs`) for its ABI checks and the `build_options` `test_hooks`, `slow_tests` and `stream_pace`, and its
/// system libraries.
fn setUp(b: *std.Build, module: *std.Build.Module, config: Config, build_options: BuildOptions) void {
    module.addImport("c_headers", translateHeader(b, public_header, config.target, config.optimize));

    const options = b.addOptions();
    options.addOption(bool, "test_hooks", build_options.test_hooks);
    options.addOption(bool, "slow_tests", build_options.slow_tests);
    options.addOption(?u32, "stream_pace", config.stream_pace);
    module.addImport("build_options", options.createModule());

    linkSystemLibraries(module, config);
}

/// A C header translated to a Zig module by the official translate-c package (vendored: vendor/README.md), with
/// `include_dirs` on its include path.
pub fn translateHeader(b: *std.Build, header: []const u8, target: std.Build.ResolvedTarget, optimize: std.lang.Optimize) *std.Build.Module {
    const translator: Translator = .init(b.dependency("translate_c", .{}), .{
        .c_source_file = b.path(header),
        .target = target,
        .optimize = optimize,
    });
    for (include_dirs) |dir| translator.addIncludePath(b.path(dir));
    return translator.mod;
}

/// The backend of every compilation with Zig code in a TSan build: Zig's self-hosted x86_64 backend, the default for
/// Debug, ignores `sanitize_thread` without a word, so the Zig code would carry no TSan instrumentation; LLVM adds it.
pub fn tsanBackend(enable_tsan: bool) ?bool {
    return if (enable_tsan) true else null;
}

pub fn linkSystemLibraries(module: *std.Build.Module, config: Config) void {
    if (config.enable_tsan) {
        module.sanitize_thread = true;
    }

    switch (config.target.result.os.tag) {
        .linux => {
            module.linkSystemLibrary("pthread", .{});
            module.linkSystemLibrary("rt", .{});
        },
        .windows => {
            module.linkSystemLibrary("kernel32", .{});
            module.linkSystemLibrary("user32", .{});
            module.linkSystemLibrary("advapi32", .{}); // the named objects' security (platform/windows/windows.zig)
        },
        else => {},
    }
    module.link_libc = true;
}
