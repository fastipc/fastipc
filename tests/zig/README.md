# The C-API test suite

Zig tests of the library through its exported C API (`include/fipc.h`), and of the native Zig API against other
processes. Zig-internal logic, below the C API, has its Zig `test` blocks next to the code in `src/zig`.

The files that apply to every kind are at the top; each kind's are in its folder, `stream/` (a connection between a
server and a client), `broadcast/` (a broadcast's publisher and its subscribers), `queue/` (a work queue's owner, its
producers and its consumers) and `service/` (a service's owner, its clients and its servers).

| File | What it tests |
|---|---|
| [`api_test.zig`](api_test.zig) | What every call promises: result names, argument checks (the streams', the broadcasts', the queues' and the services' creates and joins), NULL handles |
| [`names_test.zig`](names_test.zig) | A stream's name: held by one listener, free at once after a close, nothing left behind, invalid names |
| [`batch_test.zig`](batch_test.zig) | Batches (`fipc_send_many`, `fipc_recv_many`) on every kind: the argument checks, before anything is sent; order and packing; NO_WAIT, a timeout and cancel stopping a batch partway; TOO_LARGE, a lossy subscriber's OVERRUN and a service client's ABANDONED only as a receive's first; a gated subscriber's room, a queue's end, a service client's room and order; the roles that refuse; threads; batches from other processes (slow) |
| [`admission_test.zig`](admission_test.zig) | The admission on the owner's poller (a broadcast's, a queue's and a service's): a silent joiner dropped once its 2 s to JOIN run out, without holding up another joiner meanwhile; a joiner that sends something else than JOIN dropped at once; one thread that creates a broadcast and a queue and joins them both |
| [`native_peer_death_test.zig`](native_peer_death_test.zig) | The native API: a peer crashed at each step of its handshake and at its watcher's start, a paused peer, and a broadcast with a lossy and a gated subscriber and the publisher's close, nothing leaked |
| [`stream/lifecycle_test.zig`](stream/lifecycle_test.zig) | Create, connect, accept and close: one client at a time, a late server, an accept that polls, the listener's cancel and close, reconnects, pairs across processes and within one (on two threads), the library's unload (Windows) |
| [`stream/messages_test.zig`](stream/messages_test.zig) | `fipc_send` and `fipc_recv`: messages whole, both ways, of any size through any ring, a ping-pong across the stamped frames' edge; buffers too small; timeouts |
| [`stream/wake_test.zig`](stream/wake_test.zig) | The ring wake-ups: a pinned ping-pong that hunts lost wake-ups |
| [`stream/zero_copy_test.zig`](stream/zero_copy_test.zig) | The zero-copy calls: reservations, commits, pieces |
| [`stream/cancel_test.zig`](stream/cancel_test.zig) | `fipc_cancel` and the waits it ends; a blocked wait doesn't spin (Windows) |
| [`stream/session_end_test.zig`](stream/session_end_test.zig) | A session's end as the side that stays sees it: the drain, unfinished messages, no message from an old session |
| [`stream/peer_death_test.zig`](stream/peer_death_test.zig) | A peer's process killed at each point of a connection; nothing left behind; many kill-and-reconnect cycles |
| [`broadcast/broadcast_test.zig`](broadcast/broadcast_test.zig) | Broadcasts (`fipc_broadcast_create`, `fipc_broadcast_join`) in one process: each call's arguments and each role's calls, what a subscriber receives after it joined, the publisher's close (the name free at once; a waiting joiner joins the next broadcast), a full broadcast, cancel and the timeouts, `fipc_stream_connect` and `fipc_queue_join` to a broadcast and `fipc_broadcast_join` to a stream, nothing left after a close |
| [`broadcast/lossy_test.zig`](broadcast/lossy_test.zig) | Lossy subscribers: whole messages in order, OVERRUN once after a lap with the number lost, then a recent message |
| [`broadcast/gated_test.zig`](broadcast/gated_test.zig) | Gated subscribers: the publisher held back, in place and copied, the data ring's room, the publisher's cancel, NO_WAIT, TOO_LARGE, gated and lossy subscribers on threads |
| [`broadcast/unsubscribe_test.zig`](broadcast/unsubscribe_test.zig) | The unsubscribe of stalled gated subscribers (`max_stall_ms`, FOREVER, several at once, one that holds a message in place), nothing left behind; a subscription that ends while its process runs on (a stray frame either way, `fastipc.test_hooks.sendStray`): its slot's next holder is safe from it |
| [`broadcast/poller_test.zig`](broadcast/poller_test.zig) | The publisher's poller: a full broadcast's slots taken again, join-and-leave churn, the publisher's close racing its subscribers' leaves |
| [`broadcast/count_test.zig`](broadcast/count_test.zig) | The publisher's count of subscribers (`fipc_broadcast_wait_for_members`): after joins and leaves, a late joiner the count includes, a wait a join on another thread ends, its timeout and cancel |
| [`broadcast/processes_test.zig`](broadcast/processes_test.zig) | Subscriber processes (slow): the checked stream (also twice as many as CPUs), a stalled, a faulting and a paused gated subscriber process |
| [`broadcast/peer_death_test.zig`](broadcast/peer_death_test.zig) | A killed subscriber or publisher process, the count after a subscriber process's kill, a crash at each step of a join on either side (slow) |
| [`queue/queue_test.zig`](queue/queue_test.zig) | Work queues (`fipc_queue_create`, `fipc_queue_join`) in one process: the arguments, the kinds a joiner refuses, each role's calls, the owner's producer and consumer joined on the creating thread and closed before or after the queue, the shared example's exchange, `fipc_queue_wait_for_members` (each role and both, a poll, a wait a join ends), `fipc_queue_cancel`, a wait the queue's end ends, a joined producer's close, nothing left after a close |
| [`queue/delivery_test.zig`](queue/delivery_test.zig) | Delivery: producers and consumers on threads (each message to exactly one consumer; the test also checks each producer's order, which the implementation keeps and the API doesn't promise), TOO_LARGE, in place, a role full, a producer's place free once its ring settled, cancel |
| [`queue/end_test.zig`](queue/end_test.zig) | The queue's end (`fipc_queue_close`, the drain, the end's mark, after which no joiner gets in), a member's own failure, a member the owner drops, the timeouts |
| [`service/service_test.zig`](service/service_test.zig) | Services (`fipc_service_create`, `fipc_service_join`) in one process: the arguments, the name held against every kind, the kinds a joiner refuses both ways, each role's calls (INVALID, TOO_LARGE, a server's held request), the owner's client and server on the creating thread with a call round on one thread, the shared example's exchange, `fipc_service_wait_for_members` (each role and both, a poll, a wait a join ends), `fipc_service_cancel`, a wait the service's end ends, nothing left after a close |
| [`service/calls_test.zig`](service/calls_test.zig) | Calls: clients and servers on threads (each request answered once, each client's replies in order), a pool of servers, requests in flight and a client's room, a client that never receives while the others are served, a server that receives on one thread and replies on another, the places of each role (a client's drains while a server holds one of its requests), cancel |
| [`service/end_test.zig`](service/end_test.zig) | The service's end (`fipc_service_close`: the replies done, then DISCONNECTED; no drain for servers; the end's mark, after which no joiner gets in), a server's close holding a request (ABANDONED), a member's own failure, a member the owner drops (a server holding a request: its reply stands), the timeouts |
| [`service/processes_test.zig`](service/processes_test.zig) | Members in other processes (slow): client processes, a server process killed holding a request (ABANDONED once, then the next reply), also after the owner dropped it for a frame, a server crashed after its claim, inside its reply before or after its done (the client asleep, woken by the owner), a client process killed with requests pending or crashed after its `tail`, the owner killed, a crash at each step of an admission or a join |
| [`queue/processes_test.zig`](queue/processes_test.zig) | Members in other processes (slow): producer processes, a producer or consumer process killed, what goes again after a consumer's end by when it acknowledged (crashed inside `fipc_recv`, killed between acquire and release, killed after an early release, killed holding a message that already went again), a producer crashed after its `tail`, the owner killed, a crash at each step of an admission or a join |

[`support.zig`](support.zig) has the shared helpers, [`broadcast/shared.zig`](broadcast/shared.zig) those of the
broadcast tests, [`queue/shared.zig`](queue/shared.zig) those of the queue tests, [`service/shared.zig`](service/shared.zig)
those of the service tests, [`peer.zig`](peer.zig) the other process of the multi-process tests, [`stamp.zig`](stamp.zig) the
checked messages of a broadcast stream (shared by the tests and the peer), and [`all.zig`](all.zig) is the root that
imports every test file.

## Running

- `zig build test`: the fast tier (the everyday loop), the library's Zig unit tests and the single-process tests
  here. `zig build test-slow`: the slow tier, the tests that use other processes or take long.
- `python devtool.py test fast`, `test slow`, `test all` (fast, then slow), with a `--test-timeout` per tier.
- `-Dtest-filter=<text>` (devtool: `--filter <text>`) runs only the tests whose name contains the text, in either tier.
- Each tier's count includes the root's one unnamed `test` block, which only imports the files.
- The slow tier runs its test executable four times at once, one shard each (`FIPC_TEST_SHARD=<k>/4`, set by
  `build/tests.zig`): `support.begin` skips a test of another shard, so each run's summary counts the other shards'
  tests as skipped. Shard 1 has the timed tests (`support.beginTimed`) and runs after the tier's other runs, beside only
  the slow unit tests (whose races have timed bounds too; their stress tests run earlier, beside the shards); the
  others share out the rest by their place in the executable. Run directly, without the variable, the executable runs
  every test.

## Adding a test

- Put it in the file of its feature, or in a new `<feature>_test.zig` that `all.zig` imports: at the top if it applies
  to every kind, else in its kind's folder, where a file imports `../support.zig` (and a broadcast test
  `shared.zig`).
- Name it by the behaviour it checks, `test "fast: <behaviour>"` or `test "slow: <behaviour>"`: the prefix picks the
  tier, and a test that starts another process, or takes seconds, is slow.
- Start and end it as the others do: `var t = try support.begin(@src(), 30); defer t.end(); ... return t.done();`,
  where 30 is its timeout in seconds. A slow test whose checks measure time (a death seen within a bound, a wake-up,
  the CPU a wait spends) starts with `support.beginTimed` instead, which puts it in the shard that runs last. `t.expect(ok, "what")` records a failure and goes on (the test fails at its end),
  `t.check` and `t.fail` end the test, `t.expectEqual` compares integers.
- Call the library through `c`, the public API (`include/fipc.h`, translated by the build). The tests that play a
  faulty peer, writing frames into a ring by hand, are the library's unit tests (`src/zig/stream/data_path_test.zig`).
- Helpers per kind:
  - A stream: `var pair: support.Pair = .{}; try support.openPair(&t, &pair, "prefix", capacity); defer pair.close();`.
    `pair.server` is the accepted connection (it writes the s2c ring), `pair.client` the client's. The client connects
    on a thread of its own, because `fipc_stream_connect` waits for the listener's `fipc_stream_accept`;
    `support.Connector` and `support.connectAccept` do the same for a test that pairs by itself.
  - A broadcast: `var broadcast: support.Broadcast = .{}; const name = try support.openBroadcast(&t, &broadcast, &buf,
    "prefix", capacity, max_subscribers, max_stall_ms); defer broadcast.close();` (the publisher is
    `broadcast.publisher`).
  - A work queue: `queue/shared.zig`'s `Queue` and `openQueue`, which can also join the owner's own member of a role.
  - A service: `service/shared.zig`'s `Service` and `openService`, which can also join the owner's own client and
    server; `Answerer` and `Asker` answer and ask on threads of their own.
  - Joining: on the test's thread with `support.joinNow` or each kind's `shared.joined` (the owner's process admits the
    joiner by itself); `support.Joiner` joins on a thread of its own, for a test that does something else meanwhile.
  - Messages and names: `support.recv`, `support.recvIs` and `support.send` wrap the message calls;
    `support.freeAtOnce(name)` tells whether a new stream claims a name at once; `support.uniqueName(&buf, "prefix")`
    makes the per-process name "prefix_<pid>".
  - Handles: `support.handleCount()` counts the process's descriptors or handles, and `support.handlesBack(before)`
    checks that a test gave them all back.
- To probe whether a session ended: `support.ended`/`support.waitForEnd` (`fipc_send_acquire` of a byte with timeout 0
  returns DISCONNECTED; it touches nothing received). "Connected" is `fipc_stream_connect`/`fipc_stream_accept`
  returning OK.
- Allocate from `std.testing.allocator` (leaks fail the test) and print with `std.debug.print` (the build runner
  owns the test process's stdout).
- A test of a fix fails before the fix; its comment says what it guards, not the history of the bug.
- A Windows-only or POSIX-only test returns `error.SkipZigTest` on the other OS (`support.windows`).

## Other processes

The other side of a multi-process test is `test_peer` ([`peer.zig`](peer.zig)), one executable with named scenarios.
To add a role, write a scenario function and list it in `scenarios`. In the test:
`const peer = try support.Peer.spawn(&.{ "<scenario>", args... }); defer peer.deinit();`, then `peer.waitFor("A", ms)`
for a token the peer prints (null: no timeout, bounded by the test's), `peer.send(bytes)` to its stdin, `peer.wait()`,
`peer.waitTimeout(ms)`, `peer.kill()` (SIGKILL or TerminateProcess, then reaped), `peer.exitedWith(code)`. A peer
exits by itself when the test process dies.

## Tests elsewhere

- `native_peer_death_test.zig` uses the native Zig API (the module "fastipc" with its test hooks) against the peer's
  `native-pair` and `native-crash` scenarios: the peer crashes at each step of its handshake or at its watcher's start
  (`fastipc.test_hooks.crash_at`), or is paused.
- `src/zig/core/endpoint_test.zig`: the endpoint's and the listener's own tests, pairs within one process, the fork
  tests. `src/zig/stream/handshake_test.zig`: a stream's handshake against a client played by hand, and its races.
- Unit tests in `src/zig` that fork a process are named `test "slow: <name>"` and sit in a `slow_tests` struct of their
  file, which only the slow tier compiles in (`build_options.slow_tests`; `core/process_state.zig` shows the pattern).
- `src/zig/broadcast/writer_reader_test.zig`: the broadcast's data path (a many-thread stress test in the slow tier,
  which, like the queue's and the service's, the tier runs on its own, beside the other slow unit tests).
  `src/zig/service/caller_responder_test.zig`: the service's data path, its corrupt peers and the owner's steps at a
  member's end (a many-thread stress test in the slow tier too).
  `src/zig/broadcast/litmus_test.zig`: litmus tests that race each memory ordering against a weaker variant, and skip
  where the machine never shows the variant failing (the cell seqlock's and the frontier's need arm64).
- `tests/cpp` runs every kind through `include/fipc.hpp`.

## Known flakes

Compare a failure with this list before anything else, and never rerun a test until it passes: a failure that isn't
listed here is a bug until shown otherwise. A flake that is understood and accepted goes here, with its test, its OS and
its symptom.

- **Chaos `queue-pause-consumer`, Windows only** (`devtool test chaos`, not this suite): in about 5 of 1,000 cycles the
  consumer that was suspended receives nothing within 1,000 ms of its resume (`no-progress (victim)`), while the
  producers and the other consumers go on, nothing is lost and no leak shows. Linux and macOS pass every cycle. Not yet
  explained; accepted for 1.0.0 and open. It stays out of chaos's xfail list, so a full run still reports it.
