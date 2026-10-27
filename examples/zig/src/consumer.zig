// consumer.zig
const std = @import("std");
const fastipc = @import("fastipc");

pub fn main(init: std.process.Init) !void {
    const five_s: std.Io.Timeout = .{ .duration = .{
        .raw = .fromSeconds(5),
        .clock = .awake,
    } };
    const consumer = try fastipc.queueJoin(
        init.gpa,
        "my_queue",
        .consumer,
        five_s,
    );
    defer consumer.close();
    var buf: [256]u8 = undefined;
    while (true) {
        const len = consumer.recv(&buf, .none) catch |err| switch (err) {
            error.Disconnected => return, // the queue ended
            else => return err,
        };
        std.debug.print("{s}\n", .{buf[0..len]}); // job 1, ...
    }
}
