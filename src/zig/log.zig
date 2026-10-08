//! Library logging: `[LEVEL] file:line: message` lines on stderr, errors and warnings. `LOG_LEVEL=error` (any case)
//! keeps only the errors; the variable is read once, on the first message. Each line goes out in a single write, so
//! lines from different threads don't mix.

const std = @import("std");

pub const Level = enum(u8) {
    err = 0,
    warn = 1,

    fn name(level: Level) []const u8 {
        return switch (level) {
            .err => "ERROR",
            .warn => "WARN",
        };
    }
};

pub fn err(comptime src: std.lang.SourceLocation, comptime fmt: []const u8, args: anytype) void {
    emit(.err, src, fmt, args);
}

pub fn warn(comptime src: std.lang.SourceLocation, comptime fmt: []const u8, args: anytype) void {
    emit(.warn, src, fmt, args);
}

/// `warn`, unless this thread's previous `warnOnChange` had the same `code` at the same place: a failure that a control
/// task retries every few milliseconds is logged once per change of its error code, not once per retry.
pub fn warnOnChange(comptime src: std.lang.SourceLocation, code: u32, comptime fmt: []const u8, args: anytype) void {
    const key = @as(u64, src.line) << 32 | code;
    if (last_change == key) return;
    last_change = key;
    emit(.warn, src, fmt, args);
}

threadlocal var last_change: u64 = 0;

/// Holds this thread's log lines until `release`, while it holds the handle lock (process_state.zig): a write to stderr
/// may block (a pipe nobody drains), and no step under that lock may. A locked step logs one line at most; any later
/// one is dropped.
pub fn hold() void {
    holding = true;
}

/// Writes the line held since `hold`, if any.
pub fn release() void {
    holding = false;
    if (held_len == 0) return;
    writeStderr(held[0..held_len]);
    held_len = 0;
}

threadlocal var holding = false;
threadlocal var held_len: usize = 0;
threadlocal var held: [256]u8 = undefined;

/// The threshold from `LOG_LEVEL`, parsed on first use.
pub fn threshold() Level {
    const cached = cached_level.load(.monotonic);
    if (cached != unparsed) return @fromBackingInt(@intCast(cached));
    const level = parseLevel(if (std.c.getenv("LOG_LEVEL")) |v| std.mem.span(v) else null);
    cached_level.store(@backingInt(level), .monotonic);
    return level;
}

const unparsed = 0xff;
var cached_level = std.atomic.Value(u8).init(unparsed);

fn parseLevel(value: ?[]const u8) Level {
    const v = value orelse return .warn;
    return if (std.ascii.eqlIgnoreCase(v, "error")) .err else .warn;
}

/// Out of line and cold: inlined, the line buffer and the formatting
/// code would sit in every caller's frame, including the hot send/receive paths' error branches.
noinline fn emit(level: Level, comptime src: std.lang.SourceLocation, comptime fmt: []const u8, args: anytype) void {
    @branchHint(.cold);
    if (@backingInt(level) > @backingInt(threshold())) return;
    const file = comptime std.Io.Dir.path.basename(src.file);
    var buf: [1024]u8 = undefined;
    const line = std.mem.print(&buf, "[{s}] " ++ file ++ ":{d}: " ++ fmt ++ "\n", .{ level.name(), src.line } ++ args) catch blk: {
        // Too long: keep what fits, marked as cut
        const tail = "...\n";
        @memcpy(buf[buf.len - tail.len ..], tail);
        break :blk buf[0..];
    };
    if (holding) return keep(line);
    writeStderr(line);
}

const writeStderr = @import("platform.zig").writeStderr;

/// Keeps `line` for `release`, cut to fit, unless a line is kept already.
fn keep(line: []const u8) void {
    if (held_len != 0) return;
    if (line.len <= held.len) {
        @memcpy(held[0..line.len], line);
        held_len = line.len;
        return;
    }
    const tail = "...\n";
    @memcpy(held[0 .. held.len - tail.len], line[0 .. held.len - tail.len]);
    @memcpy(held[held.len - tail.len ..], tail);
    held_len = held.len;
}

test "a held line waits for release; a repeated warning is logged once per change of its code" {
    if (@backingInt(Level.warn) > @backingInt(threshold())) return error.SkipZigTest;
    hold();
    defer holding = false; // nothing is written: the test runner owns stderr
    warn(@src(), "held {d}", .{1});
    warn(@src(), "dropped", .{});
    try std.testing.expect(std.mem.endsWith(u8, held[0..held_len], "held 1\n"));
    held_len = 0;

    var logged: usize = 0;
    for ([_]u32{ 4, 4, 4, 5, 5, 4 }) |code| {
        warnOnChange(@src(), code, "retried", .{});
        if (held_len != 0) logged += 1;
        held_len = 0;
    }
    try std.testing.expectEqual(@as(usize, 3), logged); // 4, then 5, then 4 again
}

test "LOG_LEVEL=error, in any case, keeps only the errors; anything else logs warnings too" {
    try std.testing.expectEqual(Level.warn, parseLevel(null));
    try std.testing.expectEqual(Level.err, parseLevel("ERROR"));
    try std.testing.expectEqual(Level.err, parseLevel("error"));
    try std.testing.expectEqual(Level.warn, parseLevel("Warning"));
    try std.testing.expectEqual(Level.warn, parseLevel("debug"));
    try std.testing.expectEqual(Level.warn, parseLevel("errors"));
}
