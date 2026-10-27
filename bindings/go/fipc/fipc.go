package fipc

import (
	"math"
	"strings"
	"time"
)

// Timeouts: every call that can wait takes one, last. A positive duration waits up to that long, counted in whole
// milliseconds, rounded up; durations from math.MaxInt32 milliseconds (24.8 days) on wait for ever.
const (
	// NoWait doesn't wait: the call does what it can at once, or returns ErrTimeout.
	NoWait time.Duration = 0
	// Forever waits as long as it takes; so does any negative duration.
	Forever time.Duration = -1
)

// millis is timeout as the C API's milliseconds.
func millis(timeout time.Duration) int32 {
	if timeout < 0 {
		return -1
	}
	ms := timeout / time.Millisecond
	if timeout%time.Millisecond != 0 {
		ms++
	}
	if ms >= math.MaxInt32 {
		return -1
	}
	return int32(ms)
}

// cName is name as a NUL-terminated C string; false for a name with a NUL character, which no name may hold.
func cName(name string) ([]byte, bool) {
	if strings.IndexByte(name, 0) >= 0 {
		return nil, false
	}
	c := make([]byte, len(name)+1)
	copy(c, name)
	return c, true
}
