// server.zig
const std = @import("std");
const fastipc = @import("fastipc");

pub fn main(init: std.process.Init) !void {
    // rings of 1 MiB each way
    const listener = try fastipc.streamCreate(
        init.gpa,
        "my_channel",
        1 << 20,
    );
    defer listener.close();
    // waits for a client
    const conn = try listener.accept(.none);
    defer conn.close();

    var buf: [256]u8 = undefined;
    // .none: no timeout
    const len = try conn.recv(&buf, .none);
    const reply = std.ascii.upperString(&buf, buf[0..len]);
    try conn.send(reply, .none);
}
