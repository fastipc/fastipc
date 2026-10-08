//go:build darwin || linux

package fipc

import "github.com/ebitengine/purego"

// openLibrary loads the shared library at path (or, for a bare file name, from the loader's search path).
func openLibrary(path string) (uintptr, error) {
	return purego.Dlopen(path, purego.RTLD_NOW|purego.RTLD_LOCAL)
}

// librarySymbol is the address of the library's function name.
func librarySymbol(handle uintptr, name string) (uintptr, error) {
	return purego.Dlsym(handle, name)
}
