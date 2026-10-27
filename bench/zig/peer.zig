//! The other process of a case: this executable again, as `fipc_bench child <scenario> ...` with the
//! case's settings. It leaves as soon as this process dies (its stdin closes), so an aborted run leaves
//! no peer spinning on a core.

const std = @import("std");
const Io = std.Io;

pub const Peer = struct {
    child: std.process.Child,

    pub fn spawn(io: Io, argv: []const []const u8) !Peer {
        return .{ .child = try std.process.spawn(io, .{ .argv = argv, .stdin = .pipe }) };
    }

    /// `spawn`, with the peer's stdout piped to this process (`readAll`).
    pub fn spawnReporting(io: Io, argv: []const []const u8) !Peer {
        return .{ .child = try std.process.spawn(io, .{ .argv = argv, .stdin = .pipe, .stdout = .pipe }) };
    }

    /// What a `spawnReporting` peer printed, up to its end (or `buffer.len` bytes).
    pub fn readAll(peer: *Peer, io: Io, buffer: []u8) ![]const u8 {
        var len: usize = 0;
        while (len < buffer.len) {
            len += peer.child.stdout.?.readStreaming(io, &.{buffer[len..]}) catch |err| switch (err) {
                error.EndOfStream => break,
                else => return err,
            };
        }
        return buffer[0..len];
    }

    /// Waits for the peer to finish; error.PeerFailed unless it exited with 0.
    pub fn finish(peer: *Peer, io: Io) !void {
        const term = try peer.child.wait(io);
        switch (term) {
            .exited => |code| if (code != 0) {
                std.log.err("the peer exited with {d}", .{code});
                return error.PeerFailed;
            },
            else => {
                std.log.err("the peer ended abnormally: {any}", .{term});
                return error.PeerFailed;
            },
        }
    }

    pub fn kill(peer: *Peer, io: Io) void {
        peer.child.kill(io);
    }
};

/// In the peer: exits the process when the parent's end of stdin closes.
pub fn exitWithParent(io: Io) !void {
    const watcher = try std.Thread.spawn(.{}, watchStdin, .{io});
    watcher.detach();
}

fn watchStdin(io: Io) void {
    var buffer: [64]u8 = undefined;
    while (true) {
        const n = std.Io.File.stdin().readStreaming(io, &.{&buffer}) catch break;
        if (n == 0) break;
    }
    std.process.exit(3);
}
