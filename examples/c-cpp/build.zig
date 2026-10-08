const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // The FastIPC shared library, built for this target: fastipc.dll (with fastipc.lib) or libfastipc.so, and the
    // headers fipc.h and fipc.hpp, which linking it puts on the include path
    const fastipc = b.dependency("fipc", .{ .target = target, .optimize = optimize }).artifact("fastipc");
    b.installArtifact(fastipc); // zig-out/bin/fastipc.dll, zig-out/lib/libfastipc.so

    const programs = [_]struct { name: []const u8, source: []const u8, cpp: bool }{
        .{ .name = "c_server", .source = "src/server.c", .cpp = false },
        .{ .name = "c_client", .source = "src/client.c", .cpp = false },
        .{ .name = "cpp_server", .source = "src/server.cpp", .cpp = true },
        .{ .name = "cpp_client", .source = "src/client.cpp", .cpp = true },
    };
    for (programs) |program| {
        const exe = b.addExecutable(.{
            .name = program.name,
            .root_module = b.createModule(.{ .target = target, .optimize = optimize, .link_libc = true, .link_libcpp = program.cpp }),
        });
        const flags: []const []const u8 = if (program.cpp) &.{"-std=c++20"} else &.{"-std=c11"};
        exe.root_module.addCSourceFile(.{ .file = b.path(program.source), .flags = flags });
        exe.root_module.addCMacro("FASTIPC_SHARED", "1"); // Windows: the functions are dllimport
        exe.root_module.linkLibrary(fastipc);
        // Linux: the installed program finds libfastipc.so in zig-out/lib (macOS: libfastipc.dylib)
        if (target.result.os.tag == .linux) exe.root_module.addRPathSpecial("$ORIGIN/../lib") else if (target.result.os.tag == .macos) exe.root_module.addRPathSpecial("@loader_path/../lib");
        b.installArtifact(exe);
    }
}
