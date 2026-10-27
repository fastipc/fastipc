# Platform support: modern systems only

FastIPC supports modern computers and modern hardware only, not older OS versions or CPUs. Without the baggage of old
systems, the code can use modern optimizations and patterns and stay simpler.

## What is supported

| | Supported | Checked by |
|---|---|---|
| CPU (Linux, Windows) | x86-64 with **x86-64-v3** (AVX2, BMI1/2, FMA, MOVBE) and PREFETCHW: Intel Haswell (2013) and later, AMD Zen (2017) and later | `zig build dist` targets it |
| CPU (Linux on ARM64) | **ARMv8.0-A** (aarch64, NEON): Raspberry Pi 3/4/5, AWS Graviton 1-4, Ampere, NVIDIA Jetson and later | every ARM64 Linux build targets `generic` unless `-Dcpu` names another CPU; `devtool dist` checks the `.so`'s ELF machine |
| CPU (macOS) | **Apple Silicon** (arm64): M1 and later | every macOS build targets `apple_m1` unless `-Dcpu` names another CPU |
| Linux | **glibc 2.34** or newer: Ubuntu 22.04, Debian 12, RHEL 9 and later (and their kernels, 5.14+), on x86-64 and ARM64 | `devtool dist` checks each `.so`'s glibc symbol versions |
| Windows | Windows 11, Windows Server 2022 and later, x64 | — |
| macOS | **macOS 14.4** (Sonoma) or newer, on Apple Silicon | every macOS build sets 14.4 as its minimum; `devtool dist` checks the `.dylib`'s `LC_BUILD_VERSION` |
| Toolchain | The current stable Zig (0.17.0), pinned in `build.zig.zon` | `minimum_zig_version` |
| JavaScript runtimes | **Node.js 22** or newer, Bun and Deno 2, through Node-API 8 | `engines` in `bindings/js/package.json`; devtool runs the tests and the package's smoke test on Node.js 22 or newer |
| Go | **Go 1.27** or newer, with or without cgo | `go 1.27` in `bindings/go/go.mod`; devtool runs Go with `GOTOOLCHAIN=local` |

The floors are the oldest releases in mainstream support: RHEL 9's glibc 2.34 is the oldest glibc in a supported LTS
distribution, and the first with `libpthread` and `librt` merged into `libc`; Windows 10 left support in October
2025. macOS 14.4 is the first with `os_sync_wait_on_address`, the futex-like wait the rings', the broadcasts', the
queues' and the services' wake-ups use. Node.js 22
is the oldest Node.js line still maintained (Node.js 20 left support in April 2026).

Not supported: 32-bit systems (32-bit ARM included), Windows on ARM, Intel Macs, musl-based Linux (Alpine), and
x86-64 CPUs without AVX2 (for example the Atom-based Celeron and
Pentium Silver parts up to 2021).

## What this means for the code

- **Use modern APIs and instructions directly.** No fallbacks, feature probes or compatibility shims for older
  kernels, glibc versions, Windows versions or CPUs.
- **Raise a floor when it buys something.** When a newer API would make the code simpler or faster, it is fine
  to require it. Record the new floor here and in the dist check.
- **Don't test on old systems.** An old-distribution smoke test is not needed; the glibc symbol check in
  `devtool dist` guards the declared floor.
- **Every build targets x86-64-v3 with PREFETCHW** (`build/targets.zig`; on macOS, `apple_m1` and macOS 14.4; on ARM64 Linux, generic ARMv8.0-A) unless `zig build -Dcpu=...` names another
  CPU: the tests, the benchmarks and the packages run the code that ships, not code tuned to the build machine.
- **PREFETCHW**, a prefetch for writing, is the floor's one addition to x86-64-v3: the stream's sender uses it.
  Intel processors report it from Broadwell (2014) on, and every AMD Zen does; on Haswell, which doesn't report it,
  Intel says it executes as a no-op, so the stream runs there without the hint.

## Loading and unloading the library

- **Linux (x86-64 and ARM64) and macOS: the library keeps itself loaded.** Its first create, join or connect reopens
  its own image with `RTLD_NOLOAD | RTLD_NODELETE` and never closes that handle, so a `dlclose` (CFFI's
  `ffi.dlclose`, `NativeLibrary.Free`) doesn't unload it: it stays loaded for the life of the process. It must,
  for two reasons. That first create, join or connect also registers `pthread_atfork` handlers, which glibc can't
  unregister for a Zig-built library (it drops a library's handlers only in `__cxa_finalize`, which such a library
  never runs): after an unload, the next `fork` anywhere in the process would jump into unmapped code (SIGSEGV in
  `__libc_fork`). And the last close joins the library's threads without waiting until they have left its code
  (Windows waits), so an unload right after it could crash one of them. The bindings keep it loaded too: the Python
  binding (it opens the library with `RTLD_NODELETE`, which CFFI would otherwise unload when the interpreter exits),
  P/Invoke, the Java binding (the library's symbols live in the global arena), the Rust binding (linked at build
  time), the Lua binding (it pins the library, which LuaJIT would otherwise unload when the Lua state closes), the
  Go and JavaScript bindings (they never close the library they opened). On macOS dyld wouldn't unload
  `libfastipc.dylib` anyway (it has thread-local variables, and dyld doesn't unload such an image). A C-API test
  closes the library's handle after its last close and checks that it is still loaded and that a `fork` still works.
- **Windows:** unloading (`FreeLibrary`, `NativeLibrary.Free`) is safe once every listener and connection is closed:
  the last close waits until every library thread has exited, those still starting included, so none is left (a
  C-API test unloads and loads it again).
- The Linux and Windows rules are in `include/fipc.h`'s conventions.

## macOS notes

- **Crash leftovers:** a Unix socket's name is a file on macOS, so a crashed owner (a stream's listener, a
  broadcast's publisher, a queue's or a service's owner) leaves its name's socket file and lock file in the user's private directory.
  They never stop a new owner or a connection, and the name's next create removes them (a name never created again
  keeps its two empty files). See [`protocol.md`](protocol.md) §3.2.
- **fork without exec:** a child forked after the process's first create, join or connect can use the library. A
  fork by another thread *during* that first call may leave a child whose own first create, join or connect crashes
  inside libSystem (its user-directory lookup is not fork-safe until it has run once in the parent): make the first
  call before forking, or `exec` in the child, as Apple recommends for any multithreaded process.
- **Signing:** the released `libfastipc.dylib` is ad hoc signed, not notarized. Packages installed by pip, NuGet,
  Maven, Cargo, LuaRocks, npm or Go aren't quarantined; the C/C++ archive, when a browser downloads it, is: clear it with
  `xattr -d com.apple.quarantine lib/libfastipc.dylib`. An app signed with the hardened runtime and library
  validation must sign the dylib it embeds with its own identity.

