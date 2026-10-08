**What and why**

What this changes and why. Link the issue it resolves, if any.

**How it was checked**

- [ ] `python devtool.py test fast` and `test slow` (on which OSes?)
- [ ] `python devtool.py check-frozen`, `check-exports --exact`, `format --check`
- [ ] For a performance-sensitive change: `python devtool.py bench-compare --ab` against `main` (paste the summary)
- [ ] For a change to the API or a binding: the C# integration tests, the Java tests
  (`bindings/java/gradlew -p bindings/java test`), the Rust tests (`cargo test` in `bindings/rust`) and the Lua tests
  (`luajit tests/run.lua` in `bindings/lua`)

**Decisions**

Anything a reviewer should know: a changed test and why, a protocol version bump, a raised platform floor, a
trade-off.
