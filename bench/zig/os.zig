//! The little the suite needs from the OS beyond `std.Io`.

const std = @import("std");
const builtin = @import("builtin");

const is_windows = builtin.os.tag == .windows;
const is_macos = builtin.os.tag == .macos;

extern "kernel32" fn GetCurrentProcess() callconv(.winapi) *anyopaque;
extern "kernel32" fn SetProcessInformation(process: *anyopaque, class: c_int, information: *const anyopaque, size: u32) callconv(.winapi) c_int;
extern "c" fn setpriority(which: c_int, who: c_uint, prio: c_int) c_int;
extern "c" fn qos_class_self() c_uint;

/// Windows: asks for full speed even when this process has no foreground window. Windows 11 may run
/// such processes at low quality of service (EcoQoS: efficiency cores, lower clocks); started by a
/// background process, the suite measured 64 KiB stream messages at 210-280K/s with it and about 430K/s
/// without (docs/perf/baseline.md). An application's own process (a game) is in the foreground.
/// macOS: leaves the background state (`taskpolicy -b`, PRIO_DARWIN_BG), which runs a process and its
/// children on the efficiency cores (on an M1, copy 1 KiB at 7M/s against 27M/s); a QoS clamp of
/// background (`taskpolicy -c background`) does the same and can't be lifted from inside, so it is only
/// reported. Elsewhere a no-op.
pub fn requestFullSpeed() void {
    if (is_macos) {
        const prio_darwin_process = 4;
        _ = setpriority(prio_darwin_process, 0, 0);
        const qos_class_utility = 0x11;
        if (qos_class_self() < qos_class_utility) std.debug.print(
            "warning: this process runs at background QoS (a clamp it can't lift): on the efficiency cores, " ++
                "several times slower; start the suite from a foreground terminal\n",
            .{},
        );
        return;
    }
    if (!is_windows) return;
    const PowerThrottlingState = extern struct { version: u32, control_mask: u32, state_mask: u32 };
    const process_power_throttling = 4; // PROCESS_INFORMATION_CLASS.ProcessPowerThrottling
    const execution_speed = 0x1; // PROCESS_POWER_THROTTLING_EXECUTION_SPEED
    // The execution-speed policy under our control (control mask) and off (state mask)
    const state: PowerThrottlingState = .{ .version = 1, .control_mask = execution_speed, .state_mask = 0 };
    _ = SetProcessInformation(GetCurrentProcess(), process_power_throttling, &state, @sizeOf(PowerThrottlingState));
}
