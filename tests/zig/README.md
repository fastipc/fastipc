# The C-API test suite

Zig tests of the library through its exported C API (`include/fipc.h`), and of the native Zig API against other
processes. Zig-internal logic, below the C API, has its Zig `test` blocks next to the code in `src/zig`.

| File | What it tests |
|---|---|
| [`api_test.zig`](api_test.zig) | What every call promises: result names, argument checks, NULL handles, the public struct's layout |
| [`lifecycle_test.zig`](lifecycle_test.zig) | Listen, connect, accept and close: one client at a time, a late server, an accept that polls, the listener's cancel and close, reconnects, pairs across processes and within one (on two threads), the library's unload (Windows) |
| [`names_test.zig`](names_test.zig) | A listener's name: held by one listener, free at once after a close, nothing left behind, invalid names |
| [`messages_test.zig`](messages_test.zig) | `fipc_send` and `fipc_recv`: messages whole, both ways, of any size through any ring; buffers too small; timeouts |
| [`wake_test.zig`](wake_test.zig) | The ring wake-ups: a pinned ping-pong that hunts lost wake-ups |
| [`zero_copy_test.zig`](zero_copy_test.zig) | The zero-copy calls: reservations, commits, pieces |
| [`rpc_test.zig`](rpc_test.zig) | The RPC layer: ids, responses, buffers too small, messages that aren't RPC, large requests |
| [`cancel_test.zig`](cancel_test.zig) | `fipc_cancel` and the waits it ends; a blocked wait doesn't spin (Windows) |
| [`session_end_test.zig`](session_end_test.zig) | A session's end as the side that stays sees it: the drain, unfinished messages, no message from an old session |
| [`peer_death_test.zig`](peer_death_test.zig) | A peer's process killed at each point of a connection; nothing left behind; many kill-and-reconnect cycles |
| [`native_peer_death_test.zig`](native_peer_death_test.zig) | The native API: a peer crashed at each step of its handshake and at its watcher's start, and a paused peer |

[`support.zig`](support.zig) has the shared helpers, [`peer.zig`](peer.zig) the other process of the multi-process
tests, and [`all.zig`](all.zig) is the root that imports every test file.

## Running

- `zig build test`: the fast tier (the everyday loop), the library's Zig unit tests and the single-process tests
  here. `zig build test-slow`: the slow tier, the tests that use other processes or take long.
- `python devtool.py test fast`, `test slow`, `test all` (fast, then slow), with a `--test-timeout` per tier.
- `-Dtest-filter=<text>` (devtool: `--filter <text>`) runs only the tests whose name contains the text, in either tier.
- Each tier's count includes the root's one unnamed `test` block, which only imports the files.

## Adding a test

- Put it in the file of its feature, or in a new `<feature>_test.zig` that `all.zig` imports.
- Name it by the behaviour it checks, `test "fast: <behaviour>"` or `test "slow: <behaviour>"`: the prefix picks the
  tier, and a test that starts another process, or takes seconds, is slow.
- Start and end it as the others do: `var t = ts.begin(@src(), 30); defer t.end(); ... return t.done();`, where 30 is
  its timeout in seconds. `t.expect(ok, "what")` records a failure and goes on (the test fails at its end), `t.check`
  and `t.fail` end the test, `t.expectEq` compares integers.
- Call the library through `c` (`c_api.h`, translated by the build): the public API, `include/fipc.h`. The tests that
  play a faulty peer, writing frames into a ring by hand, are the library's unit tests (`src/zig/data/stream_test.zig`).
- A listener and its two connections in this process: `var p: ts.Pair = .{}; try ts.openPair(&t, &p, "prefix",
  capacity); defer p.close();` (`p.server` is the accepted connection, which writes the s2c ring; `p.client` the
  client's, which connects on a thread of its own: `fipc_connect` waits for the listener's `fipc_accept`, so a client
  and its server in one process run on different threads; `ts.Connector` and `ts.connectAccept` do the same for a test
  of its own). `ts.recv`, `ts.recvIs` and `ts.send` wrap the message calls; `ts.freeAtOnce(name)` tells whether a new
  listener claims a name at once.
- To probe whether a session ended: `ts.ended`/`ts.waitForEnd` (`fipc_send_acquire` of a byte with timeout 0 returns
  DISCONNECTED; it touches nothing received). "Connected" is `fipc_connect`/`fipc_accept` returning OK.
- Allocate from `std.testing.allocator` (leaks fail the test) and print with `std.debug.print` (the build runner
  owns the test process's stdout).
- A test of a fix fails before the fix; its comment says what it guards, not the history of the bug.
- A Windows-only or POSIX-only test returns `error.SkipZigTest` on the other OS (`ts.windows`).

## Other processes

The other side of a multi-process test is `test_peer` ([`peer.zig`](peer.zig)), one executable with named scenarios.
To add a role, write a scenario function and list it in `scenarios`. In the test:
`const peer = try ts.Peer.spawn(&.{ "<scenario>", args... }); defer peer.deinit();`, then `peer.waitFor("A", ms)` for
a token the peer prints (null: no timeout, bounded by the test's), `peer.send(bytes)` to its stdin, `peer.wait()`,
`peer.waitTimeout(ms)`, `peer.kill()` (SIGKILL or TerminateProcess, then reaped), `peer.exitedWith(code)`. A peer
exits by itself when the test process dies.

`native_peer_death_test.zig` calls the library through the native Zig API (the module "fastipc", `Listener` and `Conn`,
imported with its test hooks) against the test peer's `native-pair` and `native-crash` scenarios, which use the same
module: a crash of the peer at each step of its handshake or at its watcher's start (`fastipc.test_hooks.crash_at`) and
a paused peer. The endpoint's and the listener's own tests, pairs within one process, the races and the fork tests, are
in `src/zig/lifecycle/endpoint_test.zig`.

The unit tests in `src/zig` that start a process (a `fork`) are named `test "slow: <name>"` and sit in a
`slow_tests` struct of their file, which only the slow tier's unit-test build compiles in (`build_options.slow_tests`;
`process_state.zig` has the pattern).

## Known flakes

None. Compare a failure with this list before anything else, and never rerun a test until it passes: a failure that
isn't listed here is a bug until shown otherwise. A flake that is understood and accepted goes here, with its test, its
OS and its symptom.
