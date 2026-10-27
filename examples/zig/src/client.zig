// client.zig
const std = @import("std");
const fastipc = @import("fastipc");

pub fn main(init: std.process.Init) !void {
    const five_s: std.Io.Timeout = .{ .duration = .{
        .raw = .fromSeconds(5),
        .clock = .awake,
    } };
    const conn = try fastipc.streamConnect(
        init.gpa,
        "my_channel",
        five_s,
    );
    defer conn.close();
    try conn.send("ping", .none);
    var buf: [256]u8 = undefined;
    const len = try conn.recv(&buf, .none);
    std.debug.print("{s}\n", .{buf[0..len]}); // PING
}
