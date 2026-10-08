"""
Windows power throttling, off for the benchmarks.

Started from a background process (a terminal without focus, an automated run), Windows 11 runs a process at low
quality of service: on efficiency cores, at lower clocks (docs/perf/baseline.md, "Windows power throttling"). The
state is per process and children don't inherit it, and a benchmark starts other processes (its peer, a JVM, a
build), so `unthrottle_descendants` puts devtool in a job object, which every process it starts joins (and theirs),
and turns throttling off for each one as the job reports it new (JOB_OBJECT_MSG_NEW_PROCESS on the job's completion
port), as the Zig suite does for itself.

macOS has two such states. A process in the background state (`taskpolicy -b`, PRIO_DARWIN_BG) runs, with the
processes it starts, on the efficiency cores: on an M1 copy 1 KiB at 7M/s against 27M/s, the round trip 550 ns
against 167 ns. devtool leaves it before it starts any, and they start outside it. A QoS clamp of background
(`taskpolicy -c background`) does the same, but no process can lift its own clamp: devtool reports it. And a Mac
left alone sleeps, between short dark wakes, with a benchmark in the middle of a case, whose clock goes on: a 16 B
case of 2,000,000 messages measured 2.1K/s across a 16-minute sleep. devtool holds the system awake (`caffeinate -i`)
while it runs. Elsewhere it does nothing.
"""
import ctypes
import sys
import threading

_PROCESS_POWER_THROTTLING = 4  # PROCESS_INFORMATION_CLASS.ProcessPowerThrottling
_EXECUTION_SPEED = 0x1  # PROCESS_POWER_THROTTLING_EXECUTION_SPEED
_PROCESS_SET_INFORMATION = 0x0200
_JOB_OBJECT_ASSOCIATE_COMPLETION_PORT = 7  # JOBOBJECTINFOCLASS.JobObjectAssociateCompletionPortInformation
_JOB_OBJECT_MSG_NEW_PROCESS = 6

_started = False


def unthrottle_descendants() -> None:
    """Turns Windows power throttling off for this process and every process it starts from now on (once)."""
    global _started
    if _started:
        return
    if sys.platform == "darwin":
        _started = True
        _leave_darwin_background()
        return
    if sys.platform != "win32":
        return
    _started = True
    from ctypes import wintypes as w

    k = ctypes.WinDLL("kernel32", use_last_error=True)
    k.GetCurrentProcess.restype = w.HANDLE
    k.CreateJobObjectW.restype = w.HANDLE
    k.CreateJobObjectW.argtypes = [ctypes.c_void_p, w.LPCWSTR]
    k.CreateIoCompletionPort.restype = w.HANDLE
    k.CreateIoCompletionPort.argtypes = [w.HANDLE, w.HANDLE, ctypes.c_size_t, w.DWORD]
    k.SetInformationJobObject.argtypes = [w.HANDLE, ctypes.c_int, ctypes.c_void_p, w.DWORD]
    k.AssignProcessToJobObject.argtypes = [w.HANDLE, w.HANDLE]
    k.OpenProcess.restype = w.HANDLE
    k.OpenProcess.argtypes = [w.DWORD, w.BOOL, w.DWORD]
    k.SetProcessInformation.argtypes = [w.HANDLE, ctypes.c_int, ctypes.c_void_p, w.DWORD]
    k.GetQueuedCompletionStatus.argtypes = [w.HANDLE, ctypes.POINTER(w.DWORD), ctypes.POINTER(ctypes.c_size_t),
                                            ctypes.POINTER(ctypes.c_void_p), w.DWORD]
    k.CloseHandle.argtypes = [w.HANDLE]

    class State(ctypes.Structure):  # PROCESS_POWER_THROTTLING_STATE
        _fields_ = [("version", w.ULONG), ("control_mask", w.ULONG), ("state_mask", w.ULONG)]

    class Port(ctypes.Structure):  # JOBOBJECT_ASSOCIATE_COMPLETION_PORT
        _fields_ = [("key", ctypes.c_void_p), ("port", w.HANDLE)]

    off = State(1, _EXECUTION_SPEED, 0)  # execution speed controlled, and not throttled

    def unthrottle(process) -> bool:
        return bool(k.SetProcessInformation(process, _PROCESS_POWER_THROTTLING, ctypes.byref(off), ctypes.sizeof(off)))

    unthrottle(k.GetCurrentProcess())
    job = k.CreateJobObjectW(None, None)
    port = k.CreateIoCompletionPort(w.HANDLE(-1), None, 0, 1)
    association = Port(None, port)
    if not (job and port
            and k.SetInformationJobObject(job, _JOB_OBJECT_ASSOCIATE_COMPLETION_PORT, ctypes.byref(association),
                                          ctypes.sizeof(association))
            and k.AssignProcessToJobObject(job, k.GetCurrentProcess())):
        print(f"Warning: can't turn power throttling off for the benchmarks' processes (error {ctypes.get_last_error()});"
              " started in the background, they may run slower")
        return

    def watch():
        message, key, overlapped = w.DWORD(), ctypes.c_size_t(), ctypes.c_void_p()
        while k.GetQueuedCompletionStatus(port, ctypes.byref(message), ctypes.byref(key), ctypes.byref(overlapped),
                                          0xFFFFFFFF):
            if message.value != _JOB_OBJECT_MSG_NEW_PROCESS:
                continue
            process = k.OpenProcess(_PROCESS_SET_INFORMATION, False, overlapped.value or 0)
            if process:
                unthrottle(process)
                k.CloseHandle(process)

    threading.Thread(target=watch, name="unthrottle", daemon=True).start()


def _leave_darwin_background() -> None:
    """macOS: keeps the system from idle sleep while devtool runs, leaves the background state, and reports a QoS
    clamp of background, which stays."""
    import ctypes.util
    import os
    import subprocess

    try:
        subprocess.Popen(["caffeinate", "-i", "-w", str(os.getpid())])
    except OSError as e:
        print(f"Warning: can't keep the Mac awake (caffeinate: {e}): a sleep in a case's middle counts in its time")

    libc = ctypes.CDLL(ctypes.util.find_library("c"), use_errno=True)
    prio_darwin_process = 4
    if libc.getpriority(prio_darwin_process, 0) != 0:
        if libc.setpriority(prio_darwin_process, 0, 0) == 0:
            print("Note: devtool was started in the background state (efficiency cores only); left it for the"
                  " benchmarks")
        else:
            print(f"Warning: can't leave the background state (errno {ctypes.get_errno()}): the benchmarks run on"
                  " the efficiency cores, several times slower")
    libc.qos_class_self.restype = ctypes.c_uint
    qos_class_utility = 0x11
    if libc.qos_class_self() < qos_class_utility:
        print("Warning: devtool runs at background QoS, a clamp no process can lift: the benchmarks run on the"
              " efficiency cores, several times slower. Start devtool from a foreground terminal")
