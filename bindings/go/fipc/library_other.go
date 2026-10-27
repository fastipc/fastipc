//go:build !darwin && !linux && !windows

package fipc

import "errors"

var errUnsupported = errors.New("FastIPC doesn't support this operating system")

func openLibrary(string) (uintptr, error) { return 0, errUnsupported }

func librarySymbol(uintptr, string) (uintptr, error) { return 0, errUnsupported }
