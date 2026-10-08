//! The names (docs/protocol.md §2): which names are valid, the rendezvous name that both sides of a connection derive
//! from its name, and the names of a Windows session's objects. Pure functions; each derivation runs on every OS, so
//! each is tested on all of them.

const std = @import("std");

/// The longest valid name: 245 characters on both OSes (include/fipc.h).
pub const max_len: usize = 245;

/// Whether `name` is a valid stream name: 1 to `max_len` characters of `[A-Za-z0-9_.-]`, not starting with `.`
/// or `-`. Any other name is INVALID.
pub fn isValid(name: []const u8) bool {
    if (name.len == 0 or name.len > max_len) return false;
    if (name[0] == '.' or name[0] == '-') return false;
    for (name) |ch| switch (ch) {
        'A'...'Z', 'a'...'z', '0'...'9', '_', '.', '-' => {},
        else => return false,
    };
    return true;
}

/// The longest Linux rendezvous name: `sun_path` holds 108 bytes, and the first is the abstract namespace's NUL.
pub const linux_max = 107;
/// The longest Windows rendezvous name: a pipe path, `\\.\pipe\` included, holds at most 256 characters.
pub const windows_max = 256;

/// Linux: the abstract socket name `fastipc-<euid>-<N>` (the bytes after `sun_path`'s NUL), per user.
pub fn linuxRendezvous(buf: *[linux_max]u8, euid: u32, name: []const u8) []const u8 {
    return rendezvous(buf, "fastipc-{d}-", euid, name);
}

/// Windows: the pipe path `\\.\pipe\fastipc-<session>-<N>`, per Terminal Services session, which is the same
/// for an elevated and a non-elevated process of one desktop session (a logon LUID would not be).
pub fn windowsRendezvous(buf: *[windows_max]u8, session: u32, name: []const u8) []const u8 {
    return rendezvous(buf, "\\\\.\\pipe\\fastipc-{d}-", session, name);
}

/// The longest macOS rendezvous path: `sun_path` holds 104 bytes, the last a NUL (no abstract names, and no `bindat`).
pub const macos_max = 103;
/// The directory of the macOS rendezvous files, inside the user's directory.
pub const macos_dir = "fastipc/";

/// macOS: the socket path `<dir>fastipc/<N>`, where `dir` is the user's private directory (`confstr`'s
/// `_CS_DARWIN_USER_DIR`, ending with `/`), so the user is in the path. N is `name` if the path fits in 103 bytes, else
/// the name's first 8 characters, `~` and 32 hex digits of the first 16 bytes of SHA-256(name), or where even that
/// doesn't fit, `~` and the 32 hex digits alone. A valid name has no `~`, so a hashed N never equals a literal one.
/// Null if no N fits, which takes a `dir` over 62 bytes (confstr's is 49).
pub fn macosRendezvous(buf: *[macos_max]u8, dir: []const u8, name: []const u8) ?[]const u8 {
    const head = std.mem.print(buf, "{s}" ++ macos_dir, .{dir}) catch return null;
    const rest = buf[head.len..];
    if (name.len <= rest.len) {
        @memcpy(rest[0..name.len], name);
        return buf[0 .. head.len + name.len];
    }
    var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(name, &digest, .{});
    const hex = std.fmt.bytesToHex(digest[0..16].*, .lower);
    // Where the 8 characters and the hash fit, a name that doesn't fit is longer than 8 characters
    const tail = std.mem.print(rest, "{s}~{s}", .{ name[0..@min(8, name.len)], &hex }) catch
        std.mem.print(rest, "~{s}", .{&hex}) catch return null;
    return buf[0 .. head.len + tail.len];
}

/// `<prefix><N>`, where N is `name` if the whole string fits in `buf`, else the name's first 32 characters, `~`
/// and 32 hex digits of the first 16 bytes of SHA-256(name). A valid name has no `~`, so a hashed N never equals
/// a literal one, and a name too long to fit has more than 32 characters (the longest prefix leaves 87 bytes).
fn rendezvous(buf: []u8, comptime prefix: []const u8, scope: u32, name: []const u8) []const u8 {
    const head = std.mem.print(buf, prefix, .{scope}) catch unreachable;
    const rest = buf[head.len..];
    if (name.len <= rest.len) {
        @memcpy(rest[0..name.len], name);
        return buf[0 .. head.len + name.len];
    }
    var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(name, &digest, .{});
    const hex = std.fmt.bytesToHex(digest[0..16].*, .lower);
    const tail = std.mem.print(rest, "{s}~{s}", .{ name[0..32], &hex }) catch unreachable;
    return buf[0 .. head.len + tail.len];
}

/// The objects of one Windows session (docs/protocol.md §2): the section, and the ring events, each an
/// auto-reset event that the ring's two sides use to wake the reader for data or the writer for space.
pub const Object = enum { section, s2c_data, s2c_space, c2s_data, c2s_space };

pub const object_max = "Local\\fastipc-".len + 32 + "-s2c-space".len;

/// `Local\fastipc-<sid>` for the section, plus `-s2c-data`, `-s2c-space`, `-c2s-data` or `-c2s-space` for an
/// event, where `<sid>` is the session id in 32 lowercase hex digits.
pub fn sessionObject(buf: *[object_max]u8, sid: *const [16]u8, object: Object) []const u8 {
    const suffix = switch (object) {
        .section => "",
        .s2c_data => "-s2c-data",
        .s2c_space => "-s2c-space",
        .c2s_data => "-c2s-data",
        .c2s_space => "-c2s-space",
    };
    return std.mem.print(buf, "Local\\fastipc-{s}{s}", .{ &std.fmt.bytesToHex(sid.*, .lower), suffix }) catch unreachable;
}

const testing = std.testing;

test "valid names: [A-Za-z0-9_.-], not starting with . or -, 1 to max_len characters" {
    try testing.expect(isValid("game_cmd"));
    try testing.expect(isValid("A.b-C_9"));
    try testing.expect(isValid("x"));
    try testing.expect(isValid("9-lives."));
    try testing.expect(!isValid(""));
    try testing.expect(!isValid(".hidden"));
    try testing.expect(!isValid("-flag"));
    try testing.expect(!isValid("a/b"));
    try testing.expect(!isValid("a b"));
    try testing.expect(!isValid("a~b"));
    try testing.expect(!isValid("caf\xc3\xa9"));
    const long = &@as([max_len + 1]u8, @splat('n'));
    try testing.expect(isValid(long[0..max_len]));
    try testing.expect(!isValid(long));
}

test "a Linux rendezvous name is literal while it fits in 107 bytes, else hashed" {
    var buf: [linux_max]u8 = undefined;
    try testing.expectEqualStrings("fastipc-1000-game_cmd", linuxRendezvous(&buf, 1000, "game_cmd"));

    // "fastipc-1000-" is 13 bytes: a 94-byte name fills sun_path exactly, a 95-byte one doesn't fit
    const fits = &@as([94]u8, @splat('f'));
    try testing.expectEqualStrings("fastipc-1000-" ++ fits, linuxRendezvous(&buf, 1000, fits));
    const too_long = &@as([100]u8, @splat('a'));
    try testing.expectEqualStrings(
        "fastipc-1000-" ++ @as([32]u8, @splat('a')) ++ "~2816597888e4a0d3a36b82b83316ab32", // SHA-256("a" * 100), from Python's hashlib
        linuxRendezvous(&buf, 1000, too_long),
    );
    // The longest valid name, with the largest uid, still fits
    const longest = "game_cmd_" ++ @as([237]u8, @splat('x'));
    const derived = linuxRendezvous(&buf, std.math.maxInt(u32), longest);
    try testing.expectEqualStrings("fastipc-4294967295-game_cmd_" ++ @as([23]u8, @splat('x')) ++ "~195916f923be9f0fde822eaba0ea6f2c", derived);
    try testing.expect(derived.len <= linux_max);
}

test "a Windows rendezvous name is literal while the pipe path fits in 256 characters, else hashed" {
    var buf: [windows_max]u8 = undefined;
    try testing.expectEqualStrings("\\\\.\\pipe\\fastipc-1-game_cmd", windowsRendezvous(&buf, 1, "game_cmd"));

    // "\\.\pipe\fastipc-1-" is 19 characters: a 237-character name fills the path exactly
    const fits = &@as([237]u8, @splat('f'));
    try testing.expectEqualStrings("\\\\.\\pipe\\fastipc-1-" ++ fits, windowsRendezvous(&buf, 1, fits));
    const too_long = "game_cmd_" ++ @as([237]u8, @splat('x'));
    try testing.expectEqualStrings(
        "\\\\.\\pipe\\fastipc-1-game_cmd_" ++ @as([23]u8, @splat('x')) ++ "~195916f923be9f0fde822eaba0ea6f2c",
        windowsRendezvous(&buf, 1, too_long),
    );
}

test "a macOS rendezvous path is literal while it fits in 103 bytes, else hashed, else shorter, else none" {
    var buf: [macos_max]u8 = undefined;
    const dir = "/var/folders/jg/btn57xx55yv5wdnpl53jx5980000gp/0/"; // confstr's form: 49 bytes
    try testing.expectEqualStrings(dir ++ "fastipc/game_cmd", macosRendezvous(&buf, dir, "game_cmd").?);

    // "<dir>fastipc/" is 57 bytes: a 46-byte name fills the path exactly, a 47-byte one doesn't fit
    const fits = &@as([46]u8, @splat('f'));
    try testing.expectEqualStrings(dir ++ "fastipc/" ++ fits, macosRendezvous(&buf, dir, fits).?);
    const too_long = &@as([100]u8, @splat('a'));
    try testing.expectEqualStrings(
        dir ++ "fastipc/aaaaaaaa~2816597888e4a0d3a36b82b83316ab32", // SHA-256("a" * 100), from Python's hashlib
        macosRendezvous(&buf, dir, too_long).?,
    );
    const longest = "game_cmd_" ++ @as([237]u8, @splat('x'));
    try testing.expectEqualStrings(dir ++ "fastipc/game_cmd~195916f923be9f0fde822eaba0ea6f2c", macosRendezvous(&buf, dir, longest).?);

    // A longer directory: from 55 bytes the 8 characters don't fit beside the hash, from 63 nothing does
    const dir_62 = "/" ++ @as([60]u8, @splat('d')) ++ "/";
    try testing.expectEqualStrings(dir_62 ++ "fastipc/~2816597888e4a0d3a36b82b83316ab32", macosRendezvous(&buf, dir_62, too_long).?);
    try testing.expectEqualStrings(dir_62 ++ "fastipc/game_cmd", macosRendezvous(&buf, dir_62, "game_cmd").?);
    const dir_63 = "/" ++ @as([61]u8, @splat('d')) ++ "/";
    try testing.expectEqual(@as(?[]const u8, null), macosRendezvous(&buf, dir_63, too_long));
    const dir_200 = "/" ++ @as([198]u8, @splat('d')) ++ "/";
    try testing.expectEqual(@as(?[]const u8, null), macosRendezvous(&buf, dir_200, "game_cmd"));
}

test "a Windows session's objects are named after its id in lowercase hex" {
    var buf: [object_max]u8 = undefined;
    const sid = [16]u8{ 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff };
    const hex = "00112233445566778899aabbccddeeff";
    try testing.expectEqualStrings("Local\\fastipc-" ++ hex, sessionObject(&buf, &sid, .section));
    try testing.expectEqualStrings("Local\\fastipc-" ++ hex ++ "-s2c-data", sessionObject(&buf, &sid, .s2c_data));
    try testing.expectEqualStrings("Local\\fastipc-" ++ hex ++ "-s2c-space", sessionObject(&buf, &sid, .s2c_space));
    try testing.expectEqualStrings("Local\\fastipc-" ++ hex ++ "-c2s-data", sessionObject(&buf, &sid, .c2s_data));
    try testing.expectEqualStrings("Local\\fastipc-" ++ hex ++ "-c2s-space", sessionObject(&buf, &sid, .c2s_space));
}
