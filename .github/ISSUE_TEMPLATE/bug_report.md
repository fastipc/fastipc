---
name: Bug report
about: Something doesn't work as include/fipc.h or the docs say
labels: bug
---

**What happened**

What you did, what you expected (the call's contract in `include/fipc.h`, if it applies), and what happened instead:
the result code, the error, or the hang.

**How to reproduce**

The smallest program or test that shows it (a failing test in `tests/zig` is ideal), and how often it happens.

**Environment**

- FastIPC version or commit:
- Used from: C / Python (`fipc`) / C# (`Fipc`) / Java (`io.github.fastipc:fipc`) / Rust (`fipc`) / Lua (`fipc`) / JavaScript (`fipc`, Node.js / Bun / Deno) / Go (`github.com/fastipc/fastipc/bindings/go/fipc`) / Zig
- OS and version (for example Ubuntu 24.04, glibc 2.39; Windows 11 24H2; macOS 15.6 on an M2):
- CPU:
- Are the two processes the same user? Elevated (Windows)? In containers or namespaces (Linux)?

**Logs**

Anything the library wrote to stderr (its `[WARN]` and `[ERROR]` lines), and the output of the failing program.

For a security vulnerability, don't open an issue: see `SECURITY.md`.
