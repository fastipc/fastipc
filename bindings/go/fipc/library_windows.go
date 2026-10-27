package fipc

import "syscall"

// openLibrary loads the DLL at path (or, for a bare file name, from the DLL search order).
func openLibrary(path string) (uintptr, error) {
	handle, err := syscall.LoadLibrary(path)
	return uintptr(handle), err
}

// librarySymbol is the address of the library's function name.
func librarySymbol(handle uintptr, name string) (uintptr, error) {
	return syscall.GetProcAddress(syscall.Handle(handle), name)
}
