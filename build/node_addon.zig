//! The JavaScript binding's Node-API addon (bindings/js/src): one C file against the Node-API headers (vendored:
//! vendor/README.md) and include/fipc.h, which loads the library at run time. It links nothing of the JavaScript
//! runtime, so it builds for any target: on Linux and macOS its Node-API functions stay undefined until the host
//! process (Node.js, Bun, Deno) provides them; on Windows it looks them up in the host executable when it loads
//! (bindings/js/src/napi_windows.h). The one file serves all three runtimes; index.cjs loads it as `fipc.node`.

const std = @import("std");

/// The addon for `target`, built `optimize` (stripped unless Debug); the caller installs it as `fipc.node`.
pub fn addLibrary(b: *std.Build, target: std.Build.ResolvedTarget, optimize: std.lang.Optimize) *std.Build.Step.Compile {
    const addon = b.addLibrary(.{
        .name = "fipc_node",
        .linkage = .dynamic,
        .root_module = b.createModule(.{ .target = target, .optimize = optimize, .link_libc = true }),
    });
    addon.root_module.addIncludePath(b.path("include"));
    addon.root_module.addIncludePath(b.path("vendor/node-api-headers/include"));
    addon.root_module.addCSourceFile(.{ .file = b.path("bindings/js/src/fipc_node.c"), .flags = &flags });
    if (target.result.os.tag != .windows) addon.linker_allow_shlib_undefined = true;
    if (optimize != .debug) addon.root_module.strip = true;
    return addon;
}

/// The flags of the Node-API addon: C11, every warning an error.
const flags = [_][]const u8{ "-std=c11", "-Wall", "-Wextra", "-Wpedantic", "-Werror" };
