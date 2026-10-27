// publisher.zig
const std = @import("std");
const fastipc = @import("fastipc");

pub fn main(init: std.process.Init) !void {
    // 64 KiB of messages, up to 8 subscribers;
    // one that holds the publisher back 5 s is dropped
    const publisher = try fastipc.broadcastCreate(
        init.gpa,
        "my_broadcast",
        1 << 16,
        8,
        5000,
    );
    defer publisher.close(); // ends the broadcast

    // waits for the first subscriber
    _ = try publisher.waitForMembers(1, .none);
    for (1..4) |i| {
        var buf: [16]u8 = undefined;
        const tick = try std.mem.print(&buf, "tick {d}", .{i});
        try publisher.send(tick, .none);
    }
}
