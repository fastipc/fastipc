package fipc

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	"github.com/ebitengine/purego"
)

// ErrNoLibrary is the error of the calls that create, connect or join, and of [LibraryPath], when the FastIPC library
// can't be found or loaded; the error they return wraps it and says where the package looked.
var ErrNoLibrary = errors.New("fipc: the FastIPC library can't be loaded")

// The library's functions, as addresses for purego.SyscallN: those of include/fipc.h the package calls
// (fipc_result_str as a Go function, for the tests). They are set once, before any handle exists, and never change:
// the library is never unloaded.
var lib struct {
	path string

	resultStr               func(code int32) string // tests only: names a result as the library does
	streamCreate            uintptr
	streamAccept            uintptr
	streamConnect           uintptr
	listenerCancel          uintptr
	listenerClose           uintptr
	maxPiece                uintptr
	cancel                  uintptr
	close                   uintptr
	send                    uintptr
	recv                    uintptr
	sendMany                uintptr
	recvMany                uintptr
	sendAcquire             uintptr
	sendCommit              uintptr
	recvAcquire             uintptr
	recvRelease             uintptr
	broadcastCreate         uintptr
	broadcastJoin           uintptr
	broadcastWaitForMembers uintptr
	queueCreate             uintptr
	queueJoin               uintptr
	queueWaitForMembers     uintptr
	queueCancel             uintptr
	queueClose              uintptr
	serviceCreate           uintptr
	serviceJoin             uintptr
	serviceWaitForMembers   uintptr
	serviceCancel           uintptr
	serviceClose            uintptr
}

// load finds and loads the library on its first call; every call returns the first one's result.
var load = sync.OnceValue(func() error {
	path, handle, err := findLibrary()
	if err != nil {
		return err
	}
	symbols := []struct {
		name string
		addr *uintptr
	}{
		{"fipc_stream_create", &lib.streamCreate},
		{"fipc_stream_accept", &lib.streamAccept},
		{"fipc_stream_connect", &lib.streamConnect},
		{"fipc_listener_cancel", &lib.listenerCancel},
		{"fipc_listener_close", &lib.listenerClose},
		{"fipc_max_piece", &lib.maxPiece},
		{"fipc_cancel", &lib.cancel},
		{"fipc_close", &lib.close},
		{"fipc_send", &lib.send},
		{"fipc_recv", &lib.recv},
		{"fipc_send_many", &lib.sendMany},
		{"fipc_recv_many", &lib.recvMany},
		{"fipc_send_acquire", &lib.sendAcquire},
		{"fipc_send_commit", &lib.sendCommit},
		{"fipc_recv_acquire", &lib.recvAcquire},
		{"fipc_recv_release", &lib.recvRelease},
		{"fipc_broadcast_create", &lib.broadcastCreate},
		{"fipc_broadcast_join", &lib.broadcastJoin},
		{"fipc_broadcast_wait_for_members", &lib.broadcastWaitForMembers},
		{"fipc_queue_create", &lib.queueCreate},
		{"fipc_queue_join", &lib.queueJoin},
		{"fipc_queue_wait_for_members", &lib.queueWaitForMembers},
		{"fipc_queue_cancel", &lib.queueCancel},
		{"fipc_queue_close", &lib.queueClose},
		{"fipc_service_create", &lib.serviceCreate},
		{"fipc_service_join", &lib.serviceJoin},
		{"fipc_service_wait_for_members", &lib.serviceWaitForMembers},
		{"fipc_service_cancel", &lib.serviceCancel},
		{"fipc_service_close", &lib.serviceClose},
	}
	for _, s := range symbols {
		addr, err := librarySymbol(handle, s.name)
		if err != nil || addr == 0 {
			return fmt.Errorf("%w: %s has no %s (a FastIPC library older than this binding, or not FastIPC's?)",
				ErrNoLibrary, path, s.name)
		}
		*s.addr = addr
	}
	resultStr, err := librarySymbol(handle, "fipc_result_str")
	if err != nil || resultStr == 0 {
		return fmt.Errorf("%w: %s has no fipc_result_str (a FastIPC library older than this binding, or not "+
			"FastIPC's?)", ErrNoLibrary, path)
	}
	purego.RegisterFunc(&lib.resultStr, resultStr)
	lib.path = path
	return nil
})

// LibraryPath returns the path of the FastIPC library the package uses, loading it if no call has yet. The package
// looks for it, on the first call that needs it, in this order:
//
//  1. the folder the environment variable FASTIPC_LIB_DIR names (when it is set, nowhere else);
//  2. the folder of the running executable: the way to ship a program;
//  3. the copy the module carries, native/<platform>/ next to the package's source (win-x64, linux-x64,
//     linux-arm64, osx-arm64), which a program built from the module cache on this machine finds (not one built
//     with -trimpath, or run on another machine);
//  4. in a checkout of the FastIPC repository, its zig-out build;
//  5. the system's own search (the loader's library path; on Windows, the DLL search order).
//
// The library is never unloaded. The error wraps [ErrNoLibrary] and says where the package looked.
func LibraryPath() (string, error) {
	if err := load(); err != nil {
		return "", err
	}
	return lib.path, nil
}

// platform is this platform's folder in native/ and the library's file name; ok is false on a platform FastIPC
// doesn't support.
func platform() (rid, file string, ok bool) {
	switch runtime.GOOS + "/" + runtime.GOARCH {
	case "windows/amd64":
		return "win-x64", "fastipc.dll", true
	case "linux/amd64":
		return "linux-x64", "libfastipc.so", true
	case "linux/arm64":
		return "linux-arm64", "libfastipc.so", true
	case "darwin/arm64":
		return "osx-arm64", "libfastipc.dylib", true
	}
	return "", "", false
}

// findLibrary looks for the library in LibraryPath's order and loads the first one it finds.
func findLibrary() (path string, handle uintptr, err error) {
	rid, file, ok := platform()
	if !ok {
		return "", 0, fmt.Errorf("%w: FastIPC supports Windows and Linux on x86-64 and Linux and macOS on ARM64, not "+
			"%s/%s (https://github.com/fastipc/fastipc/blob/main/docs/platform-support.md)",
			ErrNoLibrary, runtime.GOOS, runtime.GOARCH)
	}

	if dir, set := os.LookupEnv("FASTIPC_LIB_DIR"); set {
		if path := libraryIn(dir, file); path != "" {
			return open(path)
		}
		return "", 0, fmt.Errorf("%w: no %s in %s, the folder FASTIPC_LIB_DIR names", ErrNoLibrary, file, dir)
	}

	var looked []string
	var dirs []string
	if exe, err := os.Executable(); err == nil {
		dirs = append(dirs, filepath.Dir(exe))
	}
	if source := sourceDir(); source != "" {
		dirs = append(dirs, filepath.Join(source, "native", rid))
		if root := repository(source); root != "" {
			build := "lib"
			if runtime.GOOS == "windows" {
				build = "bin"
			}
			dirs = append(dirs, filepath.Join(root, "zig-out", build))
		}
	}
	for _, dir := range dirs {
		if path := libraryIn(dir, file); path != "" {
			return open(path)
		}
		looked = append(looked, dir)
	}

	// The system's search, by the bare file name
	handle, err = openLibrary(file)
	if err == nil {
		return file, handle, nil
	}
	looked = append(looked, "the system's search ("+err.Error()+")")
	return "", 0, fmt.Errorf("%w: no %s in %s; set FASTIPC_LIB_DIR to the folder that holds it",
		ErrNoLibrary, file, strings.Join(looked, ", "))
}

// open loads the library at path; its error wraps ErrNoLibrary and names the path.
func open(path string) (string, uintptr, error) {
	handle, err := openLibrary(path)
	if err != nil {
		return "", 0, fmt.Errorf("%w: %s: %v", ErrNoLibrary, path, err)
	}
	return path, handle, nil
}

// libraryIn is the library's path in dir, or "" if it isn't there. On Linux a development build may hold only the
// file its soname names, libfastipc.so.1.
func libraryIn(dir, file string) string {
	names := []string{file}
	if runtime.GOOS == "linux" {
		names = append(names, file+".1")
	}
	for _, name := range names {
		path := filepath.Join(dir, name)
		if info, err := os.Stat(path); err == nil && info.Mode().IsRegular() {
			return path
		}
	}
	return ""
}

// sourceDir is the folder of this package's source when it was built, or "" if the build didn't record it as an
// absolute path (-trimpath).
func sourceDir() string {
	_, file, _, ok := runtime.Caller(0)
	if !ok || !filepath.IsAbs(file) {
		return ""
	}
	return filepath.Dir(file)
}

// repository is the root of the FastIPC repository whose bindings/go/fipc is source, or "" if source isn't in a
// checkout.
func repository(source string) string {
	root := filepath.Dir(filepath.Dir(filepath.Dir(source)))
	if filepath.Join(root, "bindings", "go", "fipc") != filepath.Clean(source) {
		return ""
	}
	for _, marker := range []string{"build.zig", filepath.Join("include", "fipc.h")} {
		if info, err := os.Stat(filepath.Join(root, marker)); err != nil || !info.Mode().IsRegular() {
			return ""
		}
	}
	return root
}
