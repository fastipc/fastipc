// server.zig
const std = @import("std");
const fastipc = @import("fastipc");

pub fn main(init: std.process.Init) !void {
    // rings of 1 MiB each way
    const listener = try fastipc.Listener.listen(
        init.io,
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
    const req = try conn.rpcRecv(&buf, .none);
    const reply = std.ascii.upperString(&buf, req.payload);
    try conn.rpcRespond(req.id, req.opcode, 0, reply, .none);
}
