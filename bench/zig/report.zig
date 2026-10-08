//! The suite's output: a readable table on stdout and, with `--json`, one JSON object per case.

const std = @import("std");
const builtin = @import("builtin");
const scenarios = @import("scenarios.zig");
const timing = @import("timing.zig");

const Writer = std.Io.Writer;
const Results = scenarios.Results;
const Scenario = scenarios.Scenario;

/// What every record repeats: how the suite ran.
pub const Setting = struct {
    lib: []const u8,
    revision: ?[]const u8,
    /// "duration" (warm-up, runs of about `run_seconds`) or "count" (`--messages`).
    method: []const u8,
    runs: u32,
    run_seconds: f64,
    warmup_seconds: f64,
    cpus: usize,
    auto_affinity: bool,
};

pub fn header(w: *Writer, setting: Setting) !void {
    try w.print("fipc_bench: FastIPC benchmarks\n", .{});
    try w.print("  library  {s}\n", .{setting.lib});
    try w.print("  system   {t} {t} ({s}), {d} CPUs, AUTO_AFFINITY {s}\n", .{
        builtin.os.tag, builtin.cpu.arch, builtin.cpu.model.name, setting.cpus, if (setting.auto_affinity) "set" else "unset",
    });
    try w.print("  method   {d} run(s) per case of ~{d:.2} s after a {d:.2} s warm-up; median [min - max]\n", .{
        setting.runs, setting.run_seconds, setting.warmup_seconds,
    });
}

pub fn scenarioTitle(w: *Writer, scenario: *const Scenario) !void {
    try w.print("\n{s}: {s}\n", .{ scenario.name, scenario.what });
}

/// One line per case: every metric's median and range.
pub fn caseLine(w: *Writer, gpa: std.mem.Allocator, case_label: []const u8, results: *const Results) !void {
    try w.print("  {s:<12}", .{case_label});
    for (results.metrics, results.values[0..results.metrics.len]) |metric, values| {
        const spread = try timing.Spread.of(gpa, values.items);
        if (std.mem.eql(u8, metric.unit, "ns")) {
            try w.print("  {s} {f} [{f} - {f}]", .{ quantileName(metric.name), Duration{ .ns = spread.median }, Duration{ .ns = spread.min }, Duration{ .ns = spread.max } });
        } else if (std.mem.eql(u8, metric.unit, "MiB/s")) {
            try w.print("  {d:.0} MiB/s", .{spread.median});
        } else {
            try w.print("  {f} {s} [{f} - {f}]", .{ Rate{ .value = spread.median }, metric.unit, Rate{ .value = spread.min }, Rate{ .value = spread.max } });
        }
    }
    if (results.samples > 0) try w.print("  ({f} per run)", .{Rate{ .value = @floatFromInt(results.samples) }});
    try w.print("\n", .{});
    try w.flush();
}

/// "p50_ns" and "cycle_p50_ns" read "p50", "p999_ns" "p99.9".
fn quantileName(metric: []const u8) []const u8 {
    const name = if (std.mem.findScalar(u8, metric, 'p')) |at| metric[at..] else metric;
    if (std.mem.startsWith(u8, name, "p50")) return "p50";
    if (std.mem.startsWith(u8, name, "p999")) return "p99.9";
    if (std.mem.startsWith(u8, name, "p99")) return "p99";
    return metric;
}

/// The case's record: the setting, the case and each metric's median, range and run values.
pub fn jsonLine(w: *Writer, gpa: std.mem.Allocator, setting: Setting, scenario: *const Scenario, case_label: []const u8, size: usize, ring: u32, results: *const Results) !void {
    try w.writeAll("{\"scenario\":");
    try std.json.Stringify.value(scenario.name, .{}, w);
    try w.writeAll(",\"case\":");
    try std.json.Stringify.value(case_label, .{}, w);
    try w.print(",\"size\":{d},\"ring\":{d},\"method\":\"{s}\",\"runs\":{d},\"run_seconds\":{d},\"warmup_seconds\":{d},\"samples\":{d}", .{
        size, ring, setting.method, results.values[0].items.len, setting.run_seconds, setting.warmup_seconds, results.samples,
    });
    try w.print(",\"os\":\"{t}\",\"arch\":\"{t}\",\"cpu\":\"{s}\",\"cpus\":{d},\"auto_affinity\":{},\"revision\":", .{
        builtin.os.tag, builtin.cpu.arch, builtin.cpu.model.name, setting.cpus, setting.auto_affinity,
    });
    try std.json.Stringify.value(setting.revision, .{}, w);
    try w.writeAll(",\"lib\":");
    try std.json.Stringify.value(setting.lib, .{}, w);
    try w.writeAll(",\"metrics\":{");
    for (results.metrics, results.values[0..results.metrics.len], 0..) |metric, values, i| {
        const spread = try timing.Spread.of(gpa, values.items);
        if (i > 0) try w.writeAll(",");
        try w.print("\"{s}\":{{\"unit\":\"{s}\",\"higher_is_better\":{},\"median\":{d},\"min\":{d},\"max\":{d},\"values\":", .{
            metric.name, metric.unit, metric.higher_is_better, spread.median, spread.min, spread.max,
        });
        try std.json.Stringify.value(values.items, .{}, w);
        try w.writeAll("}");
    }
    try w.writeAll("}}\n");
    try w.flush();
}

/// A rate with a K/M/G suffix: "12.34M", "456.7K", "812".
pub const Rate = struct {
    value: f64,

    pub fn format(rate: Rate, w: *Writer) Writer.Error!void {
        const v = rate.value;
        if (v >= 1e9) return w.print("{d:.2}G", .{v / 1e9});
        if (v >= 1e6) return w.print("{d:.2}M", .{v / 1e6});
        if (v >= 1e3) return w.print("{d:.1}K", .{v / 1e3});
        return w.print("{d:.0}", .{v});
    }
};

/// A duration in ns, us or ms.
pub const Duration = struct {
    ns: f64,

    pub fn format(d: Duration, w: *Writer) Writer.Error!void {
        if (d.ns >= 1e6) return w.print("{d:.2} ms", .{d.ns / 1e6});
        if (d.ns >= 1e3) return w.print("{d:.2} us", .{d.ns / 1e3});
        return w.print("{d:.0} ns", .{d.ns});
    }
};

/// "16 B", "1 KiB", "1.5 MiB".
pub fn sizeLabel(buffer: []u8, size: usize) []const u8 {
    const f: f64 = @floatFromInt(size);
    return (if (size >= 1024 * 1024)
        std.mem.print(buffer, "{d} MiB", .{f / (1024 * 1024)})
    else if (size >= 1024)
        std.mem.print(buffer, "{d} KiB", .{f / 1024})
    else
        std.mem.print(buffer, "{d} B", .{size})) catch buffer;
}

test "sizes read in binary units" {
    var buffer: [32]u8 = undefined;
    try std.testing.expectEqualStrings("16 B", sizeLabel(&buffer, 16));
    try std.testing.expectEqualStrings("64 KiB", sizeLabel(&buffer, 64 * 1024));
    try std.testing.expectEqualStrings("1.5 MiB", sizeLabel(&buffer, 3 * 512 * 1024));
}
