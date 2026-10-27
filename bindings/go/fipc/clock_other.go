//go:build !windows

package fipc

import "time"

// instant is a reading of a monotonic clock, in nanoseconds from an origin of its own.
type instant int64

var origin = time.Now()

// now is the clock's reading.
func now() instant { return instant(time.Since(origin)) }

// since is the time gone by from start until now.
func since(start instant) time.Duration { return time.Duration(now() - start) }
