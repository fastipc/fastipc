//go:build !windows

package fipc

import "github.com/ebitengine/purego"

// ffi calls the library's function fn with args, which live in a Conn (they hold no Go pointer the GC doesn't see
// through the Conn: see Conn.tx); only the low 32 bits of a C int result are the result.
//
// Passing a slice that already exists, rather than the arguments one by one, keeps purego.SyscallN from copying them
// into a new slice on the heap at every call.
func ffi(fn uintptr, args []uintptr) uintptr {
	r, _, _ := purego.SyscallN(fn, args...)
	return r
}
