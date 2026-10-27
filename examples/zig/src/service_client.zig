// service_client.zig
const std = @import("std");
const fastipc = @import("fastipc");

pub fn main(init: std.process.Init) !void {
    const five_s: std.Io.Timeout = .{ .duration = .{
        .raw = .fromSeconds(5),
        .clock = .awake,
    } };
    const client = try fastipc.serviceJoin(
        init.gpa,
        "my_service",
        .client,
        five_s,
    );
    defer client.close();
    var buf: [16]u8 = undefined;
    for (1..4) |i| {
        const request = try std.mem.print(&buf, "ping {d}", .{i});
        // sends the request and returns its reply, which the caller
        // frees; error.Abandoned if the server ended before it replied
        const reply = try client.call(init.gpa, request, .none);
        defer init.gpa.free(reply);
        std.debug.print("{s}\n", .{reply}); // PING 1, ...
    }
}
