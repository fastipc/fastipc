const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // The module "fastipc", the library's own source built for this target: nothing to ship next to the programs
    const fastipc = b.dependency("fipc", .{ .target = target, .optimize = optimize }).module("fastipc");

    const programs = [_]struct { name: []const u8, source: []const u8 }{
        .{ .name = "zig_server", .source = "src/server.zig" },
        .{ .name = "zig_client", .source = "src/client.zig" },
        .{ .name = "zig_publisher", .source = "src/publisher.zig" },
        .{ .name = "zig_subscriber", .source = "src/subscriber.zig" },
        .{ .name = "zig_producer", .source = "src/producer.zig" },
        .{ .name = "zig_consumer", .source = "src/consumer.zig" },
        .{ .name = "zig_service_server", .source = "src/service_server.zig" },
        .{ .name = "zig_service_client", .source = "src/service_client.zig" },
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
