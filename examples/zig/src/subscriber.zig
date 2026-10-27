// subscriber.zig
const std = @import("std");
const fastipc = @import("fastipc");

pub fn main(init: std.process.Init) !void {
    const five_s: std.Io.Timeout = .{ .duration = .{
        .raw = .fromSeconds(5),
        .clock = .awake,
    } };
    // gated: misses nothing (the publisher waits for it)
    const subscriber = try fastipc.broadcastJoin(
        init.gpa,
        "my_broadcast",
        .gated,
        five_s,
    );
    defer subscriber.close();
    var buf: [256]u8 = undefined;
    while (true) {
        const len = subscriber.recv(&buf, .none) catch |err| switch (err) {
            error.Disconnected => return, // the publisher closed
            else => return err,
        };
        std.debug.print("{s}\n", .{buf[0..len]}); // tick 1, ...
    }
}
