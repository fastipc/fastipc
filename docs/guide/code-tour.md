# FastIPC code tour

A guide for reading the code: where to start, how each call flows through the functions, and a plan of sessions to
read it all. The contract is [`include/fipc.h`](../../include/fipc.h), the wire is
[`../protocol.md`](../protocol.md), the structure and the invariants are
[`../design/architecture.md`](../design/architecture.md). This tour names functions rather than lines.

## 1. Where to start

1. `include/fipc.h`: the API's promises. Everything below keeps one of them.
2. `src/zig/c_api.zig`: each export checks its C arguments and calls one function of the implementation; errors
   become result codes in `dataResult` and `lifecycleResult`.
3. `src/zig/root.zig`: the same calls as a Zig API (`Listener`, `Conn`); its tests are a small tour of the whole API.
4. Then follow the flows below.

## 2. Flows

### 2.1 A server listens and accepts

1. `fipc_listen` → `listener.listen`: `names.isValid`, `segment.validCapacity`; `process_state.acquire` (the
   process-wide `Io`); `platform.createCancelSignal` and `platform.addressOf`, `platform.claim` under
   `process_state.lockHandles`, registered in the `cancel` and `listening` slots. Nothing starts.
2. `fipc_accept` → `listener.accept`: `client_open` (INVALID) and `failed` (NO_MEMORY) first; then a loop that checks
   `cancelled` (→ `dropPending`, CANCELLED), computes the wait left (`control.waitMs`) and takes one step of the setup
   kept in `pending`:
   - `.none` → `acceptClient`: `platform.acceptWithin` (the first wait); under the handle lock, the handle moves to
     the `connected` slot and the fork count is compared; `platform.peer` checks the user; `newSetup`
     (`endpoint.create`, `newSegment`: `platform.createSegmentHandle` on Linux and macOS, `segment.create`; `nextListening`: the
     next pipe instance on Windows); `wire.encodeSegment`, `control.sendFrame` (with the segment's descriptor on Linux and macOS); `pending`
     becomes `.awaiting_ready`.
   - `.awaiting_ready` → `awaitReady`: `platform.readWithin` (the second wait) into the frame's remaining bytes;
     once whole, `wire.isReady`, then `handOver`: the connected handle moves to the connection's handles,
     `endpoint.startAccepted` publishes the session (`Endpoint.publishSession`), CONNECTED, and starts
     `control.runWatcher`; `client_open` is set and the connection returned.
   - A step that comes to nothing returns TIMEOUT once the deadline has passed (`control.passed`); a failed accept
     backs off on the cancel signal (`platform.waitCancelSignal`); EOF or garbage where READY belongs, `dropPending`.

### 2.2 A client connects

1. `fipc_connect` → `endpoint.connect`: `endpoint.create`, `platform.addressOf`, `platform.Security.init`, then
   `control.handshake` on the caller's thread, a loop of `attempt`s with a back-off (`platform.sleep`) until the
   deadline:
   - `connectOnce`: `platform.connect` under the handle lock; `platform.peer` checks the user (and `warnSameProcess`).
   - `awaitSegment`: Linux and macOS `platform.readableWithin` then `platform.recvSegment` (the frame and the
     segment's descriptor); Windows
     `platform.readWithin` until the frame is whole; `wire.checkSegment`; the capacity and segment size.
   - `attach`: `segment.attach` (`platform.openSegment`, then the header checks).
   - READY: `wire.encodeReady`, `control.sendFrame`.
2. `endpoint.start`: `Endpoint.publishSession`, CONNECTED, `io.concurrent(control.runWatcher)`.

### 2.3 A message goes through

- `fipc_send` → `stream.send(ep, &.{}, payload, timeout)`: `inSession`, `peerGone`; per piece `Piece.at`,
  `reserve` (inline, with `SendLine.tail_seen`) or `reserveSlow` (`Space.of`, `Space.plan`: reserve, pad, wait or
  corrupt; `ring_wait.until(.space, ...)`), `Ring.writeHeader`, `ring.copy` (`copyLong` from 64 bytes), `ring_wait.publishHead`.
- `fipc_recv` → `stream.recv`: `recvRing` (releases an acquired message), `nextMessage` (`ring_wait.until(.data, ...)`
  with `RecvLine.head_seen`; the common one-piece message inline, else `skipToMessage`), the length against the buffer,
  `takeMessage` → `ring.copy`, `ring_wait.publishTail`, and for a message of several pieces `takePieces` (`continues`
  checks each piece).
- `fipc_rpc_submit` / `fipc_rpc_respond` → `rpc.sendMessage` → `stream.send(ep, header, payload, ...)`;
  `fipc_rpc_recv` → `rpc.recv`: `nextMessage`, the header's checks (else `drop`), `takeMessage` after the header.
- Zero-copy: `stream.sendAcquire` / `sendCommit`, `stream.recvAcquire` / `recvRelease`.

### 2.4 A wait and a wake-up

`ring_wait.until(need, ...)`: the ready check inline; else `sleepUntil`: spin (`ready`, a writer for an eighth of the
ring; a reader that hasn't sent since its last wait, by `SendLine.head_sent`, makes no look for its first
`stream_pace` hints, `spinData`),
`stopped` (corrupt → Invalid, cancel → Cancelled, PEER_DEAD → Disconnected unless the ring is ready),
`nextSleepMs` (the deadline, taken on the first sleep), `announceSleep` (swap the flag to 1, look again),
`platform.ringWait`, and `io.checkCancel` between slices. The other side's `publishHead` / `publishTail` stores the
index and `wakeIfAsleep` swaps the flag to 0 and calls `platform.ringWake` if it was 1.

### 2.5 The peer ends, cancel, close

- The peer's process ends: this side's watcher (`control.runWatcher`) returns from `platform.read`, closes the
  connection, publishes PEER_DEAD and calls `Endpoint.interrupt` → `ring_wait.wakeOwnWaiters`. Blocked ring waits see
  PEER_DEAD in `stopped`.
- `fipc_cancel` → `endpoint.cancel`: `requests.cancel`, `interrupt`.
- `fipc_close` → `endpoint.close` → `teardown`: `requestStop` (`shutdown`, `platform.unblock` under the handle lock),
  `platform.endTask`, `dropSession` (under the endpoint mutex), `free` (`Handles.leave`, `process_state.release`), then
  `Listener.connectionClosed` for an accepted connection.
- `fipc_listener_cancel` → `listener.cancel`: `cancelled`, then `platform.setCancelSignal`.
- `fipc_listener_close` → `listener.close`: `dropPending`, the listening handle and the cancel signal closed,
  `release`.

### 2.6 fork (Linux, macOS)

`process_state.prepareFork` takes the reference-count lock, then the handle lock; `afterForkInChild` closes every
registered handle, sets each `view_number` to 0 and marks each `Handles` forked. In the child, `inSession` fails
(Invalid), `listener.accept` returns Invalid for a forked listener, and `endpoint.close` → `freeInherited`,
`listener.close` → `Handles.leave` only. The first call that fails on an inherited object logs why, once per process
(`process_state.reportInherited`, from `stream.noSession` and `listener.accept`). `process_state.acquire` abandons the
inherited `Io`.

### 2.7 Windows' differences

A client connects on the listener's pipe instance itself, so `platform.acceptWithin` returns that instance as the
connection, the listener makes the next instance in `newSetup` (`nextListening`, the `spare` slot), and the hand-over
makes it the listening one; `dropClient` disconnects an instance and keeps it. The handshake's waits are overlapped
operations (`platform/windows.zig` `overlapped`): one that hasn't completed when its wait ends is cancelled and
drained, and one that completed meanwhile counts. The segment is found by its session id (`passes_segment_handle` is
false), and each ring sleeper has a named event (`Mapping.ringEvent`). The watcher's blocked pipe read ends by `Io`'s
cancel (`platform.endTask`), since `unblock` can't end it.

### 2.8 macOS's differences

`platform/macos.zig` is shaped like `platform/linux.zig`; it differs where macOS lacks a Linux primitive. A name is a
socket file in the user's private directory, held by an `flock` on a lock file beside it (`claim`, `holdLock`,
`release`), and every listen sweeps the files crashed listeners left (`sweep`, `removeStale`; the comment block "The
sweep" says why it is safe). The segment is a POSIX shared memory object whose name is removed at once
(`createSegmentHandle`), sized once by `ftruncate`, which macOS doesn't allow twice (`openSegment` checks the type and
the size). The ring waits are `os_sync_wait_on_address` / `os_sync_wake_by_address_any` with the SHARED flags, and the
listener's cancel signal is a kqueue with a user event. Close-on-exec takes a second call after each call that makes a
descriptor, under the handle lock. `platform/darwin.zig` declares what std lacks; `platform/macos_test_sys.zig` is the
fork tests' stand-in for `std.os.linux`.

## 3. Walkthrough plan

Seven sessions of about 45 minutes, the lifecycle first (where correctness has to be read), then the data path. Keep
`../protocol.md` and `../design/architecture.md` open beside the code.

1. **The contract and the handles.** `include/fipc.h`, `c_api.zig`, `root.zig`, `abi.zig`, and `endpoint.zig` down
   to `Error` (`Hot`, `RecvLine`, `SendLine`, `Endpoint`). Ask: which thread writes each field of `Hot`? Why are the
   three lines `extern` and exactly 64 bytes?
2. **The handshake.** `lifecycle/listener.zig`, `lifecycle/control.zig`, `lifecycle/wire.zig`, `lifecycle/names.zig`,
   against `../protocol.md` §3.2 and §3.3, with `lifecycle/endpoint_test.zig`'s paced client. Ask: which handle does
   each step register, and what closes it on every path? Why may `accept` return TIMEOUT only from a wait, and what
   does `pending` hold then? Why must the view be published before CONNECTED?
3. **The watcher, cancel and close.** `control.runWatcher`, `endpoint.teardown`, `requestStop`, `listener.cancel`,
   `listener.close`. Ask: what tells the watcher's end from a close? Who closes the connection's handle in each case?
4. **The segment and the Linux platform.** `session/segment.zig`, `platform.zig`, `platform/linux.zig`. Ask: what
   is checked between a received memfd and the first ring access? Which calls wait, and on what?
5. **The Windows and macOS platforms.** `platform/windows.zig` (skim `win32.zig`), then `platform/macos.zig`'s
   rendezvous and sweep against `platform/linux.zig`. Ask: how does the owner check fail closed? How
   does a listener drop a client without losing its name? Why is no `OVERLAPPED` in use when `overlapped` returns?
6. **The process.** `process_state.zig`, `log.zig`'s `hold`, `endpoint.teardown`, `listener.close`. Ask: what runs
   under each lock? Who awaits each task? What does a forked child free, and what does it leave alone?
7. **The data path.** `data/ring.zig`, `data/ring_wait.zig` (with its litmus test), `data/stream.zig` and
   `data/stream_test.zig`, `data/rpc.zig`. Ask: why can't a wake-up be lost (`../protocol.md` §6.1)? What does a corrupt
   index or frame lead to on each path? Why does a kept index never let a side read unpublished bytes?

Then, as needed: the C-API tests (`tests/zig/`, `support.zig` and `peer.zig` first), and the harnesses
(`tests/chaos`, `tests/soak`, `tests/fuzz`).
