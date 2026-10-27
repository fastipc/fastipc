package fipc

import "syscall"

// ffi calls the library's function fn with args, which live in a Conn (they hold no Go pointer the GC doesn't see
// through the Conn: see Conn.tx); only the low 32 bits of a C int result are the result.
//
// Windows' syscall.SyscallN takes the arguments without copying them to the heap; purego.SyscallN would.
func ffi(fn uintptr, args []uintptr) uintptr {
	r, _, _ := syscall.SyscallN(fn, args...)
	return r
}
