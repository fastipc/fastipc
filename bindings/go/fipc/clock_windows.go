package fipc

import (
	"syscall"
	"time"
	"unsafe"
)

// instant is a reading of a monotonic clock, in nanoseconds from an origin of its own.
//
// Windows' time.Now advances in steps of about half a millisecond, so a call that waited less than that would see
// none of its time go by: the instants come from QueryPerformanceCounter, precise to well under a microsecond.
type instant int64

var (
	kernel32                = syscall.NewLazyDLL("kernel32.dll")
	queryPerformanceCounter = kernel32.NewProc("QueryPerformanceCounter")
	performanceFrequency    = func() int64 { // ticks per second
		var frequency int64
		syscall.SyscallN(kernel32.NewProc("QueryPerformanceFrequency").Addr(), uintptr(unsafe.Pointer(&frequency)))
		return frequency
	}()
)

// now is the clock's reading.
func now() instant {
	var ticks int64
	syscall.SyscallN(queryPerformanceCounter.Addr(), uintptr(unsafe.Pointer(&ticks)))
	return instant(ticks/performanceFrequency*int64(time.Second) +
		ticks%performanceFrequency*int64(time.Second)/performanceFrequency)
}

// since is the time gone by from start until now.
func since(start instant) time.Duration { return time.Duration(now() - start) }
