// service_server.zig
const std = @import("std");
const fastipc = @import("fastipc");

pub fn main(init: std.process.Init) !void {
    const five_s: std.Io.Timeout = .{ .duration = .{
        .raw = .fromSeconds(5),
        .clock = .awake,
    } };
    // 4 KiB per client, requests and replies of up to 256 bytes, 1 client
    // and 1 server: this process owns the service and is its server
    const service = try fastipc.serviceCreate(
        init.gpa,
        "my_service",
        4096,
        256,
        1,
        1,
    );
    defer service.close(); // ends the service
    const server = try fastipc.serviceJoin(
        init.gpa,
        "my_service",
        .server,
        five_s,
    );
    defer server.close();
    var buf: [256]u8 = undefined;
    for (0..3) |_| { // three requests, then the end
        const len = try server.recv(&buf, .none); // the next request of any client
        const reply = std.ascii.upperString(&buf, buf[0..len]);
        try server.send(reply, .none); // goes to its client, never waits
    }
}
