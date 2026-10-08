const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // The module "fastipc", the library's own source built for this target: nothing to ship next to the programs
    const fastipc = b.dependency("fipc", .{ .target = target, .optimize = optimize }).module("fastipc");

    const programs = [_]struct { name: []const u8, source: []const u8 }{
        .{ .name = "zig_server", .source = "src/server.zig" },
        .{ .name = "zig_client", .source = "src/client.zig" },
    };
    for (programs) |program| {
        const exe = b.addExecutable(.{
            .name = program.name,
            .root_module = b.createModule(.{
                .root_source_file = b.path(program.source),
                .target = target,
                .optimize = optimize,
                .imports = &.{.{ .name = "fastipc", .module = fastipc }},
            }),
        });
        b.installArtifact(exe);
    }
}
