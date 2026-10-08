# Contributing to FastIPC

Thanks for your interest. Bug reports, questions, documentation fixes and pull requests are welcome. This page says
how to build and test, what a change has to pass, and the few rules the code base keeps.

## Proposing a change

- **A bug:** open an issue with the OS, the version (or commit), the language binding, and the smallest program that
  shows it. A failing test is the best report.
- **A small fix** (a bug, the docs, the tooling): open a pull request directly.
- **A change to the API, the protocol or the platform floors:** open an issue first to discuss it. These are
  deliberate decisions (see "The contract" below), and agreeing on the design first saves rework.
- **A security issue:** don't open a public issue; see [`SECURITY.md`](SECURITY.md).

Pull requests go into `main`. Keep one change per pull request, say what it changes and why, and how you checked it.
Contributions are licensed under the project's [MIT license](LICENSE).

## Setup

- [Zig 0.17.0](https://ziglang.org/download/) (`build.zig.zon` sets the minimum). The build needs no network: its one
  dependency, translate-c (with Aro), is vendored in `vendor/` ([`vendor/README.md`](vendor/README.md)). The 0.16 and
  0.17 standard libraries changed a lot (`std.Io` above all): check an API against the installed std source (`zig env`
  shows `lib_dir`), not against older examples.
- Python 3.9+ for `devtool.py`, which creates its own `venv/` on first use (on macOS, a Python from python.org,
  Homebrew or uv rather than `/usr/bin/python3`, which is old and drops `DYLD_*` variables).
- For the C# binding's tests and benchmarks: the .NET 9 SDK.
- For the Rust binding: rustup; `bindings/rust/rust-toolchain.toml` selects the current stable Rust there (the crates
  build with Rust 1.85 or newer).
- For the Lua binding: LuaJIT 2.1 (`luajit` on `PATH`, or the environment variable `LUAJIT` naming it), and LuaRocks
  to package it (`LUAROCKS`, or `luarocks` on `PATH`).
- For the Java binding: a JDK 17 or newer to run its Gradle wrapper (`bindings/java/gradlew`); the build itself uses
  a JDK 25 toolchain, which Gradle downloads when none is installed.
- For the JavaScript binding: Node.js 22 or newer (`NODE`, or `node` on `PATH`), and Bun and Deno 2 to run its tests
  under them too (`BUN`, `DENO`, or on `PATH`).
- For the Go binding: Go 1.27 or newer (`GO`, or `go` on `PATH`); devtool runs it with `GOTOOLCHAIN=local`.
- For formatting C and headers: `clang-format-18` (on Windows, run it in WSL so its output matches CI's).
- On Linux on ARM64 (aarch64, glibc 2.34 or newer: a Raspberry Pi, a cloud ARM server or an ARM64 VM on an Apple
  Silicon Mac) the same tools as on x86-64 Linux; devtool, the tests and the packages work the same way there.
- On macOS (14.4 or newer, Apple Silicon) the same tools. The tests and tools find `zig-out/lib/libfastipc.dylib` by
  themselves; a program run by hand finds it through `DYLD_LIBRARY_PATH=$PWD/zig-out/lib` or an rpath.

## Build and test

```bash
python devtool.py build                 # Debug build (zig build); --release for ReleaseFast
python devtool.py test fast             # unit tests + single-process C-API tests
python devtool.py test slow             # the multi-process C-API tests
python devtool.py test all --filter <text>   # fast, then slow; only the tests whose name contains <text>
```

### Tests

| Tier | What | When |
|---|---|---|
| `test fast` | Zig unit tests next to the code, the single-process C-API tests (`tests/zig`), the C++ wrapper's tests (`tests/cpp`), and first a check that each language's server and client example is what the READMEs and the website show; under 30 s | every edit |
| `test slow` | the C-API tests with other processes (peers killed at each step, reconnects, fork), the C++ wrapper's tests with other processes (a Python peer among them), builds of `examples/c-cpp` and `examples/zig` (`zig build examples`); a few minutes | every pull request |
| TSan | both tiers under ThreadSanitizer, Linux and macOS: `zig build test test-slow -Denable_tsan=true` | every pull request (CI) |
| C# | `dotnet test tests/csharp/Fipc.IntegrationTests -c Release` (Linux: `LD_LIBRARY_PATH=$PWD/zig-out/lib`; Windows and macOS find the library next to the test assembly) | a change to the API or the C# binding |
| Java | `bindings/java/gradlew -p bindings/java test`: JUnit 5 tests of the binding against `zig-out`, other JVMs and a Python peer (the repository's `venv`) included | a change to the API or the Java binding; every pull request on Linux (CI) |
| Rust | `cargo test` in `bindings/rust`: unit, integration and doc tests against `zig-out`, peers in other processes, a Python peer (the repository's `venv`), and every Rust example of the docs and website run; with `cargo fmt --check` and `cargo clippy --all-targets -- -D warnings` | a change to the API or the Rust binding; every pull request on Linux (CI) |
| Lua | `luajit tests/run.lua` in `bindings/lua`: the binding's tests against `zig-out` (their own runner, nothing but LuaJIT), peers in other processes, a Python peer (the repository's `venv`), the examples in pairs, and every Lua example of the docs and website run; `python devtool.py package lua --smoke` | a change to the API or the Lua binding |
| JavaScript | `node --expose-gc --test` in `bindings/js` (and `bun test`, `deno test -A --no-check --v8-flags=--expose-gc tests/`): the binding's tests (`node:test`) against `zig-out`, peers in other processes, a Python peer (the repository's `venv`), worker threads and the garbage collector, the examples in pairs, and every JavaScript example of the docs and website run; `python devtool.py package js --smoke` | a change to the API or the JavaScript binding |
| Go | `go test ./...` in `bindings/go` (Go 1.27+): the binding's tests against `zig-out`, peers in other processes (the test binary run again), a Python peer (the repository's `venv`), goroutines and the garbage collector, the examples in pairs, the package's runnable examples, and every Go example of the docs and website run; `go test -race ./...` where cgo is available (Linux), `go vet ./...`, `gofmt -l .` (`format --check` runs it); `python devtool.py package go --smoke` | a change to the API or the Go binding |
| Interop | `test interop`: every language's server example against every language's client example (Zig, C, C++, Python, C#, Java, Rust, Lua, JavaScript, Go: 100 pairs, the server first, and ten with the client first), each in a process of its own; it builds what each language needs and needs every toolchain above (`--skip-missing` runs the others, `--server`/`--client` pick languages); under a minute | a change to the protocol, a binding or an example, and before releases, on Linux (x86-64; ARM64 before releases), Windows and macOS |
| Harnesses | `test chaos` (peers killed, paused and restarted), `test soak` (resources flat under churn), `test fuzz` (a corrupt peer); smoke runs by default (chaos: about 3 min), full runs with `--cycles 1000` (about 45 min per OS), `--seconds 1800`, `--rounds 1000` | risky changes to the lifecycle or the data path, and before releases |

- The C-API suite in `tests/zig` is the conformance suite: [`tests/zig/README.md`](tests/zig/README.md) says how to
  add a test, and lists the known flakes (none). A failure that isn't listed is a bug until shown otherwise: don't
  rerun a test until it passes.
- Never change a test's logic or expectations to make a change pass. A change to a test is a decision of its own:
  say so in the pull request.
- A test of a fix fails without the fix; its comment says what it guards.

### Benchmarks

A change that may affect performance runs a quick A/B against `main`:

```bash
python devtool.py build --release
python devtool.py bench-compare --ab    # this tree's library vs main's, alternating rounds (--ref REV for another)
```

It takes a few minutes per OS and gives rough numbers (about ±5%). [`docs/perf/baseline.md`](docs/perf/baseline.md)
says how to read the noise before acting on a result.

## Checks a pull request must pass

On Linux, Windows and macOS:

```bash
python devtool.py test fast
python devtool.py test slow
python devtool.py check-frozen          # include/fipc.h and include/fipc.hpp match their recorded hashes
python devtool.py check-exports --exact # the shared library exports exactly the header's FIPC_API functions
python devtool.py format --check        # clang-format-18 (C/H), zig fmt (Zig) and gofmt (Go)
```

`python devtool.py format` fixes the formatting. CI runs these on pull requests.

## The contract

- **The C API** is [`include/fipc.h`](include/fipc.h), the public header; its comments are the contract every
  binding and test relies on. [`include/fipc.hpp`](include/fipc.hpp), the header-only C++20 wrapper, is a layer over
  it that adds no ABI. Both are **frozen**: `check-frozen` compares them with recorded hashes
  (`devtool_lib/frozen.py`). An API change is agreed in an issue first, and its pull request records the new hash.
- **Exports:** the shipped library exports exactly the `FIPC_API` functions of the header; test hooks go only into
  the static library variant (`fastipc_static`) that the tests link.
- **Layouts:** `src/zig/abi.zig` mirrors the header's types and the wire formats, and checks them at compile time in
  every build.
- **The protocol** ([`docs/protocol.md`](docs/protocol.md)): two builds must interoperate bit for bit. A change to
  anything it specifies bumps `abi.protocol_version` and updates the spec.
- **Platforms** ([`docs/platform-support.md`](docs/platform-support.md)): modern systems only. Use modern APIs and
  instructions directly, without fallbacks, feature probes or shims for older systems.

## Style

- Zig is formatted by `zig fmt`, C, C++ and headers by `clang-format-18` (`.clang-format`).
- Comments and docs say what the code does and why, not how it came to be: the history belongs in commit messages
  and pull requests.
- The docs are part of the change: a change to behavior updates the header's comments, the protocol or the
  architecture document where they describe it.

## Releases (maintainers)

Pushing a tag `v<version>` runs [`.github/workflows/release.yml`](.github/workflows/release.yml): it builds the
libraries on Linux (x86-64 and ARM64), Windows and macOS, then every package from them with `devtool package` (the wheels, the NuGet
package, the Maven artifact in `zig-out/packages/maven`, the two crates with the workspace they were packaged from in
`zig-out/packages/rust`, the Lua rock, the npm package, the Go module's tree with every platform's library in
`zig-out/packages/go`), checks them and smoke-tests each on every OS. When all of them pass, the run waits for a maintainer's
approval (the `release` environment's required reviewer: the run's "Review deployments"); until then nothing is
published and the tag can still be moved. Once approved it publishes them: to PyPI, NuGet, Maven Central, crates.io, LuaRocks and npm; the Go module as the tag `bindings/go/v<version>`, on a commit that adds the libraries to
`bindings/go/fipc/native/` and is on no branch (the Go proxy and pkg.go.dev take it from there); and the header and the libraries (with `fastipc.lib` on Windows)
as archives per OS on the tag's GitHub release, whose notes are the version's section of the changelog. The registries are
separate, so a publish job can fail after another succeeded; re-run the failed job. Update [`CHANGELOG.md`](CHANGELOG.md) and the
versions first: `build.zig.zon`, the shared library's in `build.zig`, `__version__` in
`bindings/python/fipc/__init__.py`, `<Version>` in `bindings/csharp/Fipc.csproj` and `version` in
`bindings/java/gradle.properties`, and the `[workspace.package]` version in `bindings/rust/Cargo.toml` with the
`fipc-sys` version that `bindings/rust/fipc/Cargo.toml` requires, `M._VERSION` in `bindings/lua/fipc.lua` with the
rockspec's file name (`bindings/lua/fipc-<version>-1.rockspec`) and its `version`, and `version` in
`bindings/js/package.json` with `VERSION` in `bindings/js/index.cjs` (`devtool package` checks the packages
against each other and the tag; the Go module's version is its tag's).
