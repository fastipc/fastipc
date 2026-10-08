// client.zig
const std = @import("std");
const fastipc = @import("fastipc");

pub fn main(init: std.process.Init) !void {
    const five_s: std.Io.Timeout = .{ .duration = .{
        .raw = .fromSeconds(5),
        .clock = .awake,
    } };
    const conn = try fastipc.Conn.connect(
        init.io,
        init.gpa,
        "my_channel",
        five_s,
    );
    defer conn.close();
    _ = try conn.rpcSubmit(1, "ping", .none); // opcode 1
    var buf: [256]u8 = undefined;
    const response = try conn.rpcRecv(&buf, .none);
    std.debug.print("{s}\n", .{response.payload}); // PING
}
