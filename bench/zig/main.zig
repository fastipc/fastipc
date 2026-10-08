//! fipc_bench: FastIPC's benchmark suite.
//!
//! Each case runs in two processes, this one (the server) and a peer (this executable again, the client),
//! connected through the library's public C API (include/fipc.h). The library is loaded at run time (`--lib`; by
//! default the build installed next to this executable), so one binary measures any build of it that speaks that
//! API: `devtool bench-compare --ab` compares this tree's library with another revision's that way.
//!
//! Method (docs/perf/baseline.md): a warm-up, then `--runs` runs of about `--duration` seconds; each
//! metric is reported as its median and range over the runs.

const std = @import("std");
const builtin = @import("builtin");
const affinity = @import("affinity.zig");
const Io = std.Io;

const fipc = @import("fipc.zig");
const latency = @import("latency.zig");
const os = @import("os.zig");
const peer = @import("peer.zig");
const report = @import("report.zig");
const scenarios = @import("scenarios.zig");
const throughput = @import("throughput.zig");
const timing = @import("timing.zig");

const Case = scenarios.Case;
const Ctx = scenarios.Ctx;
const Results = scenarios.Results;
const Scenario = scenarios.Scenario;

const usage =
    \\usage: fipc_bench <scenario> | all | gates | list [options]
    \\
    \\  <scenario>        one scenario, every case (`fipc_bench list` shows them)
    \\  all               every scenario
    \\  gates             the performance gates: the gate cases of every scenario (all --gates)
    \\
    \\options:
    \\  --lib <path>      the FastIPC shared library to measure (default: the one next to fipc_bench)
    \\  --runs <n>        measured runs per case (default 5)
    \\  --duration <s>    seconds per run (default 1)
    \\  --warmup <s>      seconds of warm-up per case (default 0.2)
    \\  --messages <n>    messages per run instead of --duration (throughput scenarios)
    \\  --sizes <list>    message sizes in bytes, comma-separated, in place of the scenario's
    \\  --ring <bytes>    ring capacity for every case
    \\  --think-us <us>   latency-sleep: the peer's think time (default 500)
    \\  --no-touch        zero-copy receivers don't copy messages out
    \\  --gates           only the cases of the performance gates
    \\  --quick           a smoke run: one run of 20 ms per case after 10 ms of warm-up
    \\  --json <path>     also write one JSON object per case to <path>
    \\  --revision <rev>  the library's revision, recorded in the JSON objects
    \\
;

/// latency-sleep's think time: a few times the library's spin phase (4096 pauses, 130-250 us on the
/// baseline machine; docs/perf/baseline.md), so this side is asleep when each reply is sent.
const default_think_ns = 500 * std.time.ns_per_us;

const Options = struct {
    command: []const u8,
    /// `child <scenario>`: run as the peer of a case.
    peer_of: ?[]const u8 = null,
    lib: ?[]const u8 = null,
    runs: ?u32 = null,
    duration_ns: ?u64 = null,
    warmup_ns: ?u64 = null,
    messages: u64 = 0,
    think_ns: u64 = default_think_ns,
    sizes: ?[]const u8 = null,
    ring: ?u32 = null,
    touch: bool = true,
    gates: bool = false,
    quick: bool = false,
    json: ?[]const u8 = null,
    revision: ?[]const u8 = null,
    /// The peer's case.
    name: ?[:0]const u8 = null,
    size: usize = 0,

    const Option = enum { no_touch, gates, quick, lib, runs, duration, warmup, messages, think_us, sizes, ring, json, revision, duration_ns, warmup_ns, think_ns, name, size };

    /// The command line's options; `--duration-ns`, `--warmup-ns`, `--think-ns`, `--name` and `--size` pass a
    /// case's exact settings to the peer.
    const option_names: std.StaticStringMap(Option) = .initComptime(.{
        .{ "--no-touch", .no_touch }, .{ "--gates", .gates },             .{ "--quick", .quick },
        .{ "--lib", .lib },           .{ "--runs", .runs },               .{ "--duration", .duration },
        .{ "--warmup", .warmup },     .{ "--messages", .messages },       .{ "--think-us", .think_us },
        .{ "--sizes", .sizes },       .{ "--ring", .ring },               .{ "--json", .json },
        .{ "--revision", .revision }, .{ "--duration-ns", .duration_ns }, .{ "--warmup-ns", .warmup_ns },
        .{ "--think-ns", .think_ns }, .{ "--name", .name },               .{ "--size", .size },
    });

    fn parse(args: []const [:0]const u8) !Options {
        if (args.len < 2) return error.MissingCommand;
        var options: Options = .{ .command = args[1] };
        var i: usize = 2;
        if (std.mem.eql(u8, options.command, "child")) {
            if (args.len < 3) return error.MissingScenario;
            options.peer_of = args[2];
            i = 3;
        }
        while (i < args.len) : (i += 1) {
            const option = option_names.get(args[i]) orelse {
                std.log.err("unknown option {s}", .{args[i]});
                return error.UnknownOption;
            };
            switch (option) {
                .no_touch => options.touch = false,
                .gates => options.gates = true,
                .quick => options.quick = true,
                else => {
                    i += 1;
                    if (i == args.len) return error.MissingValue;
                    const value = args[i];
                    switch (option) {
                        .no_touch, .gates, .quick => unreachable,
                        .lib => options.lib = value,
                        .runs => {
                            options.runs = try std.fmt.parseInt(u32, value, 10);
                            if (options.runs == 0) return error.InvalidRuns;
                        },
                        .duration => options.duration_ns = try parseSeconds(value),
                        .warmup => options.warmup_ns = try parseSeconds(value),
                        .messages => options.messages = try std.fmt.parseInt(u64, value, 10),
                        .think_us => options.think_ns = @intFromFloat(try std.fmt.parseFloat(f64, value) * std.time.ns_per_us),
                        .sizes => options.sizes = value,
                        .ring => options.ring = try std.fmt.parseInt(u32, value, 10),
                        .json => options.json = value,
                        .revision => options.revision = value,
                        .duration_ns => options.duration_ns = try std.fmt.parseInt(u64, value, 10),
                        .warmup_ns => options.warmup_ns = try std.fmt.parseInt(u64, value, 10),
                        .think_ns => options.think_ns = try std.fmt.parseInt(u64, value, 10),
                        .name => options.name = value,
                        .size => options.size = try std.fmt.parseInt(usize, value, 10),
                    }
                },
            }
        }
        return options;
    }

    fn parseSeconds(text: []const u8) !u64 {
        const s = try std.fmt.parseFloat(f64, text);
        if (!(s >= 0)) return error.InvalidDuration;
        return timing.nanoseconds(s);
    }

    /// Runs per case, seconds per run and seconds of warm-up, after the presets.
    fn method(options: Options) struct { runs: u32, duration_ns: u64, warmup_ns: u64 } {
        const quick = options.quick;
        return .{
            .runs = options.runs orelse if (quick) 1 else 5,
            .duration_ns = options.duration_ns orelse if (quick) 20 * std.time.ns_per_ms else std.time.ns_per_s,
            .warmup_ns = options.warmup_ns orelse if (quick) 10 * std.time.ns_per_ms else 200 * std.time.ns_per_ms,
        };
    }
};

pub fn main(init: std.process.Init) !u8 {
    const io = init.io;
    const gpa = init.gpa;
    const arena = init.arena.allocator();

    var stdout_buffer: [4096]u8 = undefined;
    var stdout = std.Io.File.stdout().writer(io, &stdout_buffer);
    const out = &stdout.interface;

    const args = try init.minimal.args.toSlice(arena);
    const options = Options.parse(args) catch |err| {
        std.log.err("{t}\n{s}", .{ err, usage });
        return 2;
    };
    if (std.mem.eql(u8, options.command, "help") or std.mem.eql(u8, options.command, "--help")) {
        try out.writeAll(usage);
        try out.flush();
        return 0;
    }
    if (std.mem.eql(u8, options.command, "list")) {
        try list(out);
        return 0;
    }

    const lib = options.lib orelse try defaultLibrary(io, arena);
    try fipc.load(gpa, lib);
    os.requestFullSpeed();
    // Both processes of a case pin their threads (the peer inherits the environment)
    affinity.enabled = if (init.environ_map.get("AUTO_AFFINITY")) |value| std.mem.startsWith(u8, value, "1") else false;

    if (options.peer_of) |name| {
        try peer.exitWithParent(io);
        const scenario = scenarios.find(name) orelse return error.UnknownScenario;
        const ctx = contextOf(io, gpa, options, options.name orelse return error.MissingName, .{ .size = options.size, .ring = options.ring orelse return error.MissingRing });
        try follow(scenario, &ctx);
        return 0;
    }

    const auto_affinity = affinity.enabled;
    const method = options.method();
    const setting: report.Setting = .{
        .lib = lib,
        .revision = options.revision,
        .method = if (options.messages > 0) "count" else "duration",
        .runs = method.runs,
        .run_seconds = timing.seconds(method.duration_ns),
        .warmup_seconds = timing.seconds(method.warmup_ns),
        .cpus = std.Thread.getCpuCount() catch 0,
        .auto_affinity = auto_affinity,
    };
    return runSuite(io, gpa, arena, options, setting, out);
}

/// The library installed next to this executable.
fn defaultLibrary(io: Io, arena: std.mem.Allocator) ![]const u8 {
    const dir = try std.process.executableDirPathAlloc(io, arena);
    const name = if (builtin.os.tag == .windows) "fastipc.dll" else if (builtin.os.tag == .macos) "libfastipc.dylib" else "libfastipc.so";
    return std.Io.Dir.path.join(arena, &.{ dir, name });
}

fn list(out: *std.Io.Writer) !void {
    var label: [32]u8 = undefined;
    for (&scenarios.all) |*scenario| {
        try out.print("{s:<30} {s}\n", .{ scenario.name, scenario.what });
        for (scenario.cases) |case| {
            var ring: [16]u8 = undefined;
            try out.print("    {s:<12} ring {s}{s}\n", .{ caseLabel(&label, scenario, case), report.sizeLabel(&ring, case.ring), if (case.gate) ", gate" else "" });
        }
    }
    try out.flush();
}

fn runSuite(io: Io, gpa: std.mem.Allocator, arena: std.mem.Allocator, options: Options, setting: report.Setting, out: *std.Io.Writer) !u8 {
    const selected: []const Scenario = if (std.mem.eql(u8, options.command, "all") or std.mem.eql(u8, options.command, "gates"))
        &scenarios.all
    else if (scenarios.find(options.command)) |scenario|
        scenario[0..1]
    else {
        std.log.err("unknown scenario {s}; `fipc_bench list` shows them", .{options.command});
        return 2;
    };
    const gates_only = options.gates or std.mem.eql(u8, options.command, "gates");

    var json_buffer: [4096]u8 = undefined;
    const json_file: ?std.Io.File = if (options.json) |path| try std.Io.Dir.cwd().createFile(io, path, .{}) else null;
    defer if (json_file) |file| file.close(io);
    var json = if (json_file) |file| file.writer(io, &json_buffer) else null;

    try report.header(out, setting);
    const exe = try std.process.executablePathAlloc(io, arena);
    var failures: usize = 0;
    for (selected) |*scenario| {
        const cases = try casesOf(arena, scenario, options, gates_only);
        if (cases.len == 0) continue;
        try report.scenarioTitle(out, scenario);
        try out.flush();
        for (cases) |case| {
            var label: [32]u8 = undefined;
            const case_label = caseLabel(&label, scenario, case);
            var results = runCase(io, gpa, exe, setting.lib, options, scenario, case) catch |err| {
                try out.print("  {s:<12}  FAILED: {t}\n", .{ case_label, err });
                try out.flush();
                failures += 1;
                continue;
            };
            defer results.deinit(gpa);
            try report.caseLine(out, gpa, case_label, &results);
            if (json) |*writer| try report.jsonLine(&writer.interface, gpa, setting, scenario, case_label, case.size, case.ring, &results);
        }
    }
    if (failures > 0) {
        try out.print("\n{d} case(s) failed\n", .{failures});
        try out.flush();
        return 1;
    }
    return 0;
}

/// The cases to run: the gates or all, with `--sizes` and `--ring` applied.
fn casesOf(arena: std.mem.Allocator, scenario: *const Scenario, options: Options, gates_only: bool) ![]const Case {
    const base = scenario.cases;
    var cases: std.ArrayList(Case) = .empty;
    if (options.sizes) |sizes| {
        if (scenario.kind == .setup or base.len == 0) return &.{};
        var it = std.mem.tokenizeScalar(u8, sizes, ',');
        while (it.next()) |text| {
            const size = try std.fmt.parseInt(usize, std.mem.trim(u8, text, " "), 10);
            const known = for (base) |case| {
                if (case.size == size) break case;
            } else Case{ .size = size, .ring = base[0].ring };
            try cases.append(arena, known);
        }
    } else {
        for (base) |case| {
            if (!gates_only or case.gate) try cases.append(arena, case);
        }
    }
    if (options.ring) |ring| {
        for (cases.items) |*case| case.ring = ring;
    }
    return cases.items;
}

fn caseLabel(buffer: []u8, scenario: *const Scenario, case: Case) []const u8 {
    if (scenario.kind != .setup) return report.sizeLabel(buffer, case.size);
    var ring: [16]u8 = undefined;
    return std.mem.print(buffer, "{s} ring", .{report.sizeLabel(&ring, case.ring)}) catch buffer;
}

/// What a case runs with; the peer rebuilds it from its command line.
fn contextOf(io: Io, gpa: std.mem.Allocator, options: Options, name: [:0]const u8, case: Case) Ctx {
    const method = options.method();
    return .{
        .io = io,
        .gpa = gpa,
        .name = name,
        .size = case.size,
        .ring = case.ring,
        .runs = method.runs,
        .duration_ns = method.duration_ns,
        .warmup_ns = method.warmup_ns,
        .messages = options.messages,
        .touch = options.touch,
        .think_ns = options.think_ns,
    };
}

/// Starts the peer, runs this side of the case and waits for the peer to finish.
fn runCase(io: Io, gpa: std.mem.Allocator, exe: []const u8, lib: []const u8, options: Options, scenario: *const Scenario, case: Case) !Results {
    var name_buffer: [96]u8 = undefined;
    const name = try std.mem.printSentinel(&name_buffer, "fipcbench_{s}_{d}_{d}", .{ scenario.name, case.size, case.ring }, 0);

    var ctx = contextOf(io, gpa, options, name, case);
    if (!scenario.thinks) ctx.think_ns = 0;
    if (ctx.duration_ns == 0 and ctx.messages == 0) return error.NoMessageCount;

    var arena_state: std.heap.ArenaAllocator = .init(gpa);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    var argv: std.ArrayList([]const u8) = .empty;
    try argv.appendSlice(arena, &.{ exe, "child", scenario.name, "--lib", lib, "--name", name });
    try argv.appendSlice(arena, &.{ "--size", try arena.print("{d}", .{ctx.size}) });
    try argv.appendSlice(arena, &.{ "--ring", try arena.print("{d}", .{ctx.ring}) });
    try argv.appendSlice(arena, &.{ "--runs", try arena.print("{d}", .{ctx.runs}) });
    try argv.appendSlice(arena, &.{ "--duration-ns", try arena.print("{d}", .{ctx.duration_ns}) });
    try argv.appendSlice(arena, &.{ "--warmup-ns", try arena.print("{d}", .{ctx.warmup_ns}) });
    try argv.appendSlice(arena, &.{ "--messages", try arena.print("{d}", .{ctx.messages}) });
    try argv.appendSlice(arena, &.{ "--think-ns", try arena.print("{d}", .{ctx.think_ns}) });
    if (!ctx.touch) try argv.append(arena, "--no-touch");

    var the_peer = try peer.Peer.spawn(io, argv.items);
    var results: Results = .init(scenario.kind);
    errdefer results.deinit(gpa);
    lead(scenario, &ctx, &results) catch |err| {
        the_peer.kill(io);
        return err;
    };
    try the_peer.finish(io);
    return results;
}

/// This process's side of a case, the server's: it measures.
fn lead(scenario: *const Scenario, ctx: *const Ctx, results: *Results) !void {
    switch (scenario.kind) {
        .stream => switch (scenario.zero_copy) {
            inline else => |zero_copy| try throughput.streamReceiver(ctx, zero_copy, results),
        },
        .rpc => try throughput.rpcReceiver(ctx, results),
        .ping_pong => try latency.pinger(ctx, results),
        .wakeup => switch (scenario.polls) {
            inline else => |polls| try latency.wakeupReceiver(ctx, polls, results),
        },
        .setup => try latency.setupLeader(ctx, results),
    }
}

/// The peer's side of a case, the client's.
fn follow(scenario: *const Scenario, ctx: *const Ctx) !void {
    switch (scenario.kind) {
        .stream => switch (scenario.zero_copy) {
            inline else => |zero_copy| try throughput.streamSender(ctx, zero_copy),
        },
        .rpc => try throughput.rpcSender(ctx),
        .ping_pong => try latency.ponger(ctx),
        .wakeup => try latency.wakeupSender(ctx),
        .setup => try latency.setupFollower(ctx),
    }
}

test {
    _ = affinity;
    _ = report;
    _ = throughput;
    _ = timing;
}
