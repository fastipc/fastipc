# Architecture

How the library implements [`include/fipc.h`](../../include/fipc.h) and the protocol of
[`../protocol.md`](../protocol.md): its modules, its objects and threads, the lifecycle of a connection, the data
path, the OS contract, the process-wide state, and what checks it. The code tour
([`../guide/code-tour.md`](../guide/code-tour.md)) follows the same code call by call.

## 1. Principles

1. **One owner per piece of state.** The thread inside `fipc_accept` or `fipc_connect` owns a connection's setup;
   once it is set up, the connection's watcher owns its control connection; the application's threads own the data
   path; the kernel owns names, connections and their ends. Nothing about the lifecycle lives in shared memory: the
   segment holds the rings and nothing else.
2. **Setup on the caller's thread, simple rather than fast.** Only the data path needs to be fast. `fipc_accept` and
   `fipc_connect` run the handshake on the thread that calls them, with the call's timeout as a deadline: a listener
   runs nothing in the background, and the only race left in the lifecycle is a connection's end against its close.
3. **The waits.** The handshake's waits are the platform layer's, on the caller's thread: a `poll` (Linux, macOS) or an
   overlapped operation and `WaitForMultipleObjects` (Windows), with a timeout and the listener's cancel signal. The
   watcher and the process-local waits run on an `Io` (the C API's is one process-wide `Io.Threaded`; the native Zig
   API takes the caller's). The price is size: `Io.Threaded` is most of the library's code (about 217 KB of the
   stripped Linux library's `.text`). Speed is unaffected.
4. **The ring waits.** A ring's sleeper waits on a word in memory shared with the other process (a futex, an `os_sync`
   address wait, a named event), which `Io` can't express. That wait lives in one place (`data/ring_wait.zig`), in slices of at most 100 ms
   that return to `Io` between them.
5. **No signals.** The library installs no signal handler and never cancels a task through `Io` on Linux or macOS
   (which would send a signal): it stops a watcher by shutting its socket down.
6. **The data path allocates nothing and trusts nothing.** A message of any size goes through the caller's buffer, a
   piece at a time; every index and frame the peer wrote is checked before it is used.

## 2. Modules

```
include/fipc.h           the public API: 18 functions, one struct (fipc_rpc_msg_t), the result codes
src/zig/
  c_api.zig              the library's root: the exports, thin wrappers that check C arguments and map errors to
                         result codes; comptime checks of each export against its prototype
  root.zig               the native Zig API, module "fastipc": Listener, Conn and RpcMessage over the caller's Io
  abi.zig                the C mirrors (fipc_rpc_msg_t, fipc_result_t) and the wire layouts (segment header, frame
                         header, RPC header), checked at compile time; the protocol's version and magics
  data/
    stream.zig           the data calls: send, recv, the zero-copy acquire/commit/release, pieces, orphans
    ring.zig             one ring: its indices, frames (16-byte aligned, never wrapping), the long-piece copy
    ring_wait.zig        the ring waits and wake-ups: spin, announce, sleep in slices; publish head and tail
    rpc.zig              RPC: a 32-byte header in front of a plain message
  lifecycle/
    listener.zig         the listener handle and accept: set a client up on the caller's thread, resumably
    endpoint.zig         the connection handle (the endpoint): its hot lines, connect, cancel, close
    control.zig          the client's handshake, and a connection's watcher, which reads until the end
    wire.zig             the SEGMENT and READY frames
    names.zig            valid names, rendezvous addresses, Windows object names
  session/segment.zig    a connection's segment: create (server), attach and check (client)
  process_state.zig      the process-wide Io and its reference count; each object's handles; the fork registry
  platform.zig           the OS contract (§6), implemented by platform/linux.zig, platform/windows.zig and
                         platform/macos.zig
  platform/win32.zig     raw Windows declarations, private to windows.zig
  platform/darwin.zig    the macOS declarations std lacks, private to macos.zig
  log.zig                [LEVEL] file:line lines on stderr, filtered by LOG_LEVEL
```

`c_api.zig` and `root.zig` are two roots over the same modules. Two import cycles are deliberate, and Zig analyzes
them lazily: `lifecycle/endpoint.zig` with `data/ring_wait.zig` (the endpoint's interrupt runs the wake rule; the ring
waits read the endpoint's `Io` and hot line) and with `lifecycle/control.zig` (the endpoint runs the handshake and
starts the watcher, which work on it).

## 3. Objects and threads

**A listener** (`fipc_listener_t`, `listener.Listener`) holds its name through a listening handle, and a cancel signal
(an eventfd, a manual-reset event) that `fipc_listener_cancel` sets and every wait of `fipc_accept` watches. It runs
nothing in the background. It keeps a client's setup between two calls of `fipc_accept` (`pending`, §4.1), a
`cancelled` flag, a `failed` flag (NO_MEMORY for good) and `client_open`, set while the connection it returned last is
open. Its memory is reference-counted: the handle holds one reference and an accepted connection another, so a
connection may outlive its listener.

**A connection** (`fipc_conn_t`, `endpoint.Endpoint`) is one session: it is set up before the application gets it and
never set up again. Its first three cache lines are laid out by hand:

| Line | Written by | Holds |
|---|---|---|
| `Hot` | the lifecycle (published once; cleared at close or in a forked child) | the **view** of the session (header and data pointers, capacity, which side), `requests`, `status` |
| `RecvLine` | the receiving thread | the acquired message (its tail and frame length), the head this side last loaded, the head this side had sent at its last receive wait |
| `SendLine` | the sending thread | the zero-copy reservation (its head and length), the tail this side last loaded, the head its last message published |

Every data call reads `Hot` and the line of its own role, so a sending thread and a receiving thread never write the
same line. A receive that waits also reads `SendLine`'s published head once, to tell whether this side sent since its
last wait (a stream, whose reader paces its spin, `ring_wait.zig`): this process's own line, not the send ring's head in
the segment, on which the peer spins. The view is published once, before CONNECTED: its fields, then the session slot and `view_number`
(release); a data call loads `view_number` (acquire) and then the fields.

**Threads.** The application calls from its own threads, and `fipc_accept` and `fipc_connect` set connections up on
them. Each connection then has one **watcher**, a concurrent task of its `Io`, which reads the control connection
until the peer's end and is the only one that closes it. `requests` is written only by application threads
(fetch-or, never cleared); `status` by the thread that sets the session up, before the watcher starts, then only by
the watcher (PEER_DEAD).

## 4. The lifecycle

### 4.1 Listen and accept

`listener.listen` validates the name and capacity, acquires the process-wide `Io` (C API), creates the cancel signal
and claims the name (`platform.claim`; `FIPC_ADDR_IN_USE` if it is held); it starts nothing. `listener.accept` sets
one client up on the caller's thread, in a loop over two **waits**:

1. `platform.acceptWithin` waits for a client (and the cancel signal) and accepts it; then, at once: the fork check,
   the peer's user, the client's connection object and segment (on Windows also the next pipe instance), and SEGMENT.
   The setup is kept in `pending` (`awaiting_ready`: the endpoint, the session, READY's bytes so far).
2. `platform.readWithin` waits for READY's bytes (and the cancel signal), adding them to `pending`. Once READY is
   whole, the **hand-over** moves the connected handle into the connection's own handles (under the handle lock),
   gives it the session, publishes CONNECTED, starts its watcher, sets `client_open`, and `accept` returns it.

A timeout ends a call only inside a wait, and `pending` survives it, so the next call resumes: a polling server
(timeout 0) lets a client in within two calls. Every way out of `awaiting_ready` but the hand-over goes through
`dropPending` (the client, its segment, its endpoint): EOF or anything but READY, a cancel, a close. Failures drop the
client or back off (1 ms doubling to 100 ms, cut to the deadline, ended by the cancel signal); a segment that can't get
its memory fails the listener for good (every later accept returns NO_MEMORY). While `client_open` is set, accept is
INVALID at once; the connection's close tells the listener (`connectionClosed`), which clears it.

### 4.2 Connect

`endpoint.connect` validates the name, creates the endpoint and runs the client's handshake on the caller's thread
(`control.handshake`), up to its timeout: connect (nobody listens, or busy: back off and connect again), wait for
SEGMENT (`platform.readableWithin` then `recvSegment` on Linux, `platform.readWithin` on Windows), attach, send READY.
An attempt that ends early (EOF, a malformed frame, a vanished segment) backs off and starts over. Another protocol
version, another user or an incompatible elevation fails the connection (INVALID); no address space for the mapping
fails it with NO_MEMORY. A connect that fails or times out closes what it made. Then it publishes the view, CONNECTED,
and starts the watcher. A client and its server must run on different threads: `connect` waits for the server's
`accept`.

### 4.3 The end, cancel and close

- **The peer's end.** A connection's watcher (`control.runWatcher`) reads its control connection; whatever the read
  returns ends the session: the watcher closes the connection, publishes PEER_DEAD, interrupts this side's ring
  waiters (`Endpoint.interrupt` → `ring_wait.wakeOwnWaiters`, under the endpoint mutex) and exits.
- **`fipc_cancel`** sets `requests.cancel` and interrupts the ring waiters. Every call that would wait returns
  CANCELLED from then on; the session goes on.
- **`fipc_close`** (`endpoint.close` → `teardown`) follows the **stop rule**: set `requests.shutdown`, and under the
  handle lock `platform.unblock` the watcher's connection (Linux, macOS: `shutdown`, which ends a blocked read with
  end-of-stream); then `platform.endTask` (Linux, macOS: await; Windows: cancel through `Io`, which ends a pending pipe read).
  The watcher sees `shutdown`, closes its connection and exits without publishing an end. The view is then cleared
  and the segment unmapped under the endpoint mutex, the endpoint freed and the `Io` reference released. That no other
  thread is inside a call on the connection is the caller's precondition (fipc.h).
- **`fipc_listener_cancel`** sets `cancelled`, then the cancel signal: a waiting `accept` returns at once, drops a
  client whose setup began, and every later one returns CANCELLED.
- **`fipc_listener_close`** drops a client whose setup began, closes the listening handle and the cancel signal, and
  drops the handle's reference; no thread is inside the listener then (fipc.h). On Windows the accepted connections'
  pipe instances keep the name held until they close.

## 5. The data path

All in `data/`, on the connection's hot lines, without locks or allocation:

- **Send** (`stream.send`): checks the session (the view) and the peer (PEER_DEAD: DISCONNECTED), then for each piece
  of at most `capacity - 64` bytes reserves a frame at the head (`reserve`: a look with the kept tail first, else
  `reserveSlow`, which loads the tail, pads the ring's end or waits for room), writes the header, copies the piece
  (`ring.copy`) and publishes the head (`ring_wait.publishHead`). The timeout bounds only the wait for the first piece.
- **Receive** (`stream.recv`): releases an acquired message, waits for the next message's first piece
  (`nextMessage`, which skips PADs and orphans), compares its length with the caller's buffer (TOO_LARGE leaves it
  whole), then copies and consumes each piece (`takeMessage`, `takePieces`), publishing the tail after each. A START
  inside an unfinished message drops that message and starts over with the new one.
- **Zero-copy**: `sendAcquire` reserves one frame and records it on `SendLine`; `sendCommit` writes its header and
  publishes it. `recvAcquire` returns a one-piece message in place and records it on `RecvLine`; `recvRelease`
  consumes it, once, if it is still the next frame.
- **RPC** (`data/rpc.zig`) sends the 32-byte header and the payload as one message (`send` takes both parts), and receives
  by checking the header in the first piece before it copies the payload.
- **Waits** (`ring_wait.until`): a reader first looks at its kept head, then loads the shared one; otherwise `sleepUntil`
  spins (a writer until an eighth of the ring is free), checks for a corrupt ring, a cancel and the end of the
  session (a receive delivers what is there first), announces its sleep by swapping its flag to 1, looks again, and
  sleeps one slice in `platform.ringWait`. The argument is `../protocol.md` §6.
- **Copies**: pieces of 64 bytes and more go through `ring.copyLong` (32-byte vectors, aligned stores), shorter ones
  through `@memcpy`, so the copy's speed doesn't depend on which `memcpy` the library links.
- **Errors** are `stream.Error` values; `c_api.zig` maps them to result codes out of line (`dataResult`), so the
  success path carries no switch.

## 6. The OS contract: `platform.zig`

Everything the library needs from the OS, in FastIPC's terms, as typed constants bound at compile time to
`platform/linux.zig`, `platform/windows.zig` or `platform/macos.zig` (static dispatch: a direct call; an OS file whose signature differs
fails to compile). A port to another OS implements every declaration:

- **Capabilities**, the real differences in behavior: `has_fork` (Linux, macOS: the fork registry, the handle lock and
  the fork checks exist) and `passes_segment_handle` (Linux, macOS: the segment's descriptor travels with SEGMENT;
  Windows: the client opens the
  segment by its session id; `readableWithin`, `recvSegment` and `createSegmentHandle` exist only where it is true).
- **The process**: `Handle`, `Security` (only this user may open what the process creates), `Lock` (an OS lock that a
  fork handler may take), `pid`, `crash`, `writeStderr`, `task_stack_size`, `shutDownIo`.
- **Names and connections**: `Address`, `addressOf`, `claim`, `nextListening` (Windows: the next pipe instance),
  `acceptWithin`, `dropClient`, `connect`, `peer`, `unblock`, `endTask`, `close`, and the shared outcomes (`Accept`,
  `Connect`, `Peer`, `Side`).
- **Frames**: `sendFrame` (never blocks, never raises SIGPIPE), `readWithin` (the handshake's), `read` (the
  watcher's, through `Io`).
- **Waits on the caller's thread**: `Waited` (done, idle, cancelled), `createCancelSignal`, `setCancelSignal`,
  `waitCancelSignal`, `sleep`. No I/O is left in flight when a wait returns: on Windows an operation that didn't
  complete is cancelled and the cancellation awaited, and one that completed meanwhile counts.
- **Segments**: `Mapping`, `createSegment` (the memory committed now), `openSegment` (the size and seals checked).
- **Ring waits**: `RingEvent`, `ringWait`, `ringWake`: the data path's one blocking wait outside `Io`.
- **The tests' leak checks**: `openHandleCount`, `segmentMappingCount`.

Nothing outside `platform/` checks the OS, except where the behavior differs and no contract hides it: the zero-copy
exports are inlined on Linux only (`c_api.zig`: faster there, slower on Windows; called on macOS), and some tests.

## 7. The process: `process_state.zig`

- **The process-wide `Io`** of the C API: an `Io.Threaded` built from `init_single_threaded` with unlimited
  concurrency, so it runs tasks on worker threads but installs no signal handler, and the OS's task stack size
  (Linux, macOS: std's default, reserved; Windows: 2 MiB, which Windows commits up front). The first listener or connection
  creates it; the last one's release shuts it down (joining its workers; on Windows also waiting until their threads
  have exited, so that the DLL can be unloaded). A forked child abandons the inherited instance untouched and creates
  a new one.
- **Each object's handles** (`Handles`), in slots: `listening`, `cancel` (the listener's cancel signal), `connected`,
  `spare` (the next pipe instance),
  `segment` (a listener's memfd while it offers it) and `received` (a client's, until mapped). Where the OS forks,
  every handle is opened and registered, and unregistered and closed, under the process-wide **handle lock**, and
  every `Handles` is in the **fork registry**.
- **fork** (Linux, macOS): `pthread_atfork` handlers, registered once per address space. Before a fork: the reference-count
  lock, then the handle lock. In the child: close every registered handle (close only: a `shutdown` would end the
  parent's connections too), set each view number to 0, mark each object forked, and release the locks. A forked
  object's calls return INVALID; its close frees what is provably complete, takes no lock and makes no `Io` call. The
  one handle not born under the handle lock, the socket `accept` returns, is dropped if a fork came during the accept
  (`forkCount` before and after it).
- **The locks**, outermost first: the reference-count lock, an endpoint's mutex (an `Io.Mutex`, for the view, the
  unmap and the interrupt), the handle lock. No path takes an earlier one while holding a later one; only the fork
  handlers nest two. None is held across a blocking call, and nothing is written to stderr under the handle lock (a
  line logged meanwhile is held and written after it, `log.hold`).

## 8. The native Zig API: `root.zig`

`Listener` (listen, accept, cancel, close) and `Conn` (connect, send, recv, recvAlloc, acquire and commit, recvAcquire
and release, rpcSubmit, rpcRespond, rpcRecv and rpcRecvAlloc, which return an `RpcMessage`, maxPiece, cancel, ended,
close), over the same implementation, with the caller's `Io` and allocator,
`Io.Timeout` timeouts and Zig error sets. The caller's `Io` must run concurrent tasks (`Io.Threaded` does), and must
not cancel the library's tasks. A data call that the caller's `Io` cancels while it waits returns Cancelled, and the
cancelation stays pending for the task's next cancelation point. `accept` and `connect` wait on the caller's thread
outside `Io`: `Listener.cancel` ends an accept, and a timeout bounds a connect.

## 9. Invariants

| Invariant | Where it is kept | What checks it |
|---|---|---|
| Each ring index and each ring byte has one writer; the sleep flags change only by swaps | `ring_wait.publishHead` and `publishTail`, `announceSleep`, `wakeIfAsleep` | `abi.zig`'s layout checks; the lost-wake litmus test in `ring_wait.zig`; `wake_test` |
| A data call sees no session or a complete one | the view is published before CONNECTED (`publishSession`) and cleared only at close or in a forked child | the fork tests; the TSan runs |
| A kept index never says more than the shared one | it is loaded only from the shared index, and indices only grow | `data/stream_test.zig` (the wrap and corrupt-tail tests) |
| Nothing outside a ring is read or written, whatever the peer writes | `Ring.fits`, `Space.plan`, `ring_wait.corrupt`, `continues` | `data/stream_test.zig`'s faulty-peer tests; the fuzz harness |
| A receiver never sees part of a message | a message is compared with the buffer first; a stopped one is dropped whole | `datapath_test` and the "in the middle of a message" tests |
| A listener serves one client at a time; a connection is set up once | `client_open`, set by the hand-over and cleared by that connection's close; the watcher only ends a session | `lifecycle_test`, `endpoint_test` (the races) |
| A call of `accept` that times out drops no client; nothing is left in flight | the setup is kept in `pending` between calls; `dropPending` is the only other way out; Windows cancels and drains a pending operation | `endpoint_test` (a client paced at each step, READY a byte at a time, random short timeouts) |
| A segment is unmapped only when no call of this side can touch it | `close`'s precondition; the endpoint mutex around the unmap and the interrupt | the leak checks; the chaos and soak harnesses |
| No handle is left after a close, nothing named after a crash (macOS: a crashed listener's two rendezvous files, until the user's next listen) | the handle slots; unnamed (Linux, macOS) or randomly named (Windows) segments; macOS listens remove unheld names' files | `shm_cleanup_test`, the Windows handle counts, the chaos harness's leak checks |

## 10. Verification

- **The fast tier** (`devtool test fast`, seconds): the unit tests next to the code (among them the handshake paced
  step by step by a client played on the platform layer, to reach every state a resumed `accept` can be in) and the
  single-process C-API tests of `tests/zig`.
- **The slow tier** (`devtool test slow`, minutes): the tests with other processes (the test peer,
  `tests/zig/peer.zig`: crashes at every step of the handshake, kills, a peer that dies in the middle of a message) and
  the races (accepts with random short timeouts, cancels, fork). Both tiers run under TSan on Linux and macOS too.
- **The harnesses**, opt-in: chaos (peers killed, paused and restarted), soak (resources stay flat), fuzz (a corrupt
  peer on the control connection and in the rings).
- **The benchmarks** (`bench/zig`, and C#, Java, Python, Rust, Lua, JavaScript and Go): `devtool bench-compare --ab` compares this tree's library with
  another revision's, in alternating rounds (`../perf/baseline.md`).

## 11. Known limits

- A crash that a crash handler or a debugger holds is reported only when the process ends; a hung peer isn't detected.
- Peers are one user; on Linux they share a network namespace (abstract sockets are per namespace).
- Under an evented `Io` a ring wait blocks its carrier thread for up to a slice, and `accept` and `connect` block
  theirs for the whole setup (their waits are the OS's, on the calling thread).
- Linux, macOS: the library must never be unloaded (its fork handlers stay registered). A forked child keeps its inherited
  mappings until it closes them, execs or exits, and a host that forks continuously could starve a listener (each
  fork during an accept drops that client).
- A listener or connection that is never closed keeps its name and its peer's connection alive.
