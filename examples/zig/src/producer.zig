// producer.zig
const std = @import("std");
const fastipc = @import("fastipc");

pub fn main(init: std.process.Init) !void {
    const five_s: std.Io.Timeout = .{ .duration = .{
        .raw = .fromSeconds(5),
        .clock = .awake,
    } };
    // 4 KiB of messages per producer, 1 producer and 1 consumer:
    // this process owns the queue and is its producer
    const queue = try fastipc.queueCreate(
        init.gpa,
        "my_queue",
        4096,
        1,
        1,
    );
    defer queue.close(); // ends the queue
    const producer = try fastipc.queueJoin(
        init.gpa,
        "my_queue",
        .producer,
        five_s,
    );
    defer producer.close();
    for (1..4) |i| {
        var buf: [16]u8 = undefined;
        const job = try std.mem.print(&buf, "job {d}", .{i});
        try producer.send(job, .none); // waits in the queue for a consumer
    }
    _ = try queue.waitForMembers(.consumer, 1, .none); // waits for the consumer to join
}
