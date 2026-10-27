# The FastIPC protocol, version 1

What two processes that use this library share: the rendezvous names, the control connection's frames, the shared
segment's layout, the rings' frames, the wake-ups and the end of a connection. Two builds of the library interoperate
exactly when they speak the same version; a change to anything here bumps the version (§8), except a broadcast's, a
queue's and a service's formats (§9, §10, §11), which have a version of their own. The API on top is
[`include/fipc.h`](../include/fipc.h); how the library implements it is [the architecture page](https://fastipc.github.io/fastipc/architecture.html).

All integers are little-endian; offsets and sizes are in bytes. The platforms are x86-64 (Linux, Windows) and arm64
(Linux, macOS); the ring's argument (§6.1) relies on neither's memory model: it holds in the language's memory model. The
rendezvous is local, so a build only ever meets a build of the same OS.

## 1. Overview

A **server** listens on a name; a **client** connects to it. Each connection is set up on a **control connection**, a
local byte stream between the two processes, which carries two frames: the server's SEGMENT offers a new **segment**
of shared memory, and the client's READY accepts it. From then on the messages go through the segment's two
**rings**, one per direction, and the control connection carries nothing: its end is how each side learns that the
other closed the connection or its process ended.

| | Linux (glibc 2.34+) | Windows 11 / Server 2022+ | macOS 14.4+ (Apple Silicon) |
|---|---|---|---|
| Control connection | `AF_UNIX` `SOCK_STREAM` socket, abstract name (no file) | named pipe, byte mode | `AF_UNIX` `SOCK_STREAM` socket file in the user's private directory, claimed with a lock file (`flock`) |
| Who may connect | the same user (`SO_PEERCRED`, checked by both sides) | the same user (the objects' DACL and owner) | the same user (the directory's mode 0700, and `getpeereid`, checked by both sides) |
| Segment | a sealed `memfd` passed with SEGMENT (`SCM_RIGHTS`); no name | a pagefile-backed section named by a random session id | a POSIX shared memory object (`shm_open`), its name removed at once, passed with SEGMENT (`SCM_RIGHTS`) |
| Ring wake-ups | a futex on the ring's sleep flag, shared between processes | four auto-reset named events per connection | `os_sync_wait_on_address` / `os_sync_wake_by_address_any` on the ring's sleep flag, shared between processes |
| Left behind by a crash | nothing | nothing | the name's socket file and lock file, until the name's next create (§3.2) |

The **server** side of a connection writes the server-to-client ring (`s2c`) and reads the client-to-server ring
(`c2s`); the client does the opposite. Each ring has one writer and one reader, which are threads of different
processes.

The other kinds of a name, a broadcast (§9), a work queue (§10) and a service (§11), meet at the same rendezvous
address, with the same user checks, and set each member up on a control connection of its own, with frames and a
segment of their own: one segment for the whole kind, which every member maps.

## 2. Names

A connection's name is 1-245 characters of `[A-Za-z0-9_.-]`, not starting with `.` or `-`. Both sides derive the same
rendezvous address from it:

- **Linux:** an abstract `AF_UNIX` address: `sun_path` is a NUL byte followed by `fastipc-<euid>-<N>`, and the address
  length is `offsetof(sun_path) + 1 + len` (no trailing NUL). The effective uid gives each user a namespace of its own.
- **Windows:** the pipe `\\.\pipe\fastipc-<session>-<N>`, where `<session>` is the process's Terminal Services session
  (`ProcessIdToSessionId`): the same for an elevated and a non-elevated process of one desktop session.
- **macOS:** the socket file `<dir>fastipc/<N>`, and its lock file `<dir>fastipc/<N>.lock`, where `<dir>` is the
  user's private directory (`confstr(_CS_DARWIN_USER_DIR)`, such as `/var/folders/xx/<id>/0/`), so each user has a
  namespace of its own. The `fastipc` directory is created with mode 0700, and must be the effective user's and closed
  to everyone else.
- `<N>` is the name if the whole address fits (Linux: 107 bytes after the NUL; Windows: a path of 256 characters),
  else the name's first 32 characters, `~`, and 32 lowercase hex digits of the first 16 bytes of SHA-256(name). A valid
  name has no `~`, so a hashed `<N>` never equals a literal one. On macOS the socket's whole path must fit in 103
  bytes: `<N>` is the name if it fits, else the name's first 8 characters, `~` and the 32 hex digits, or where even
  that doesn't fit, `~` and the 32 hex digits alone.
- **A connection's objects on Windows**, named by its session id (§4) in 32 lowercase hex digits `<sid>`: the section
  `Local\fastipc-<sid>` and the events `Local\fastipc-<sid>-s2c-data`, `-s2c-space`, `-c2s-data` and `-c2s-space`.

## 3. The control connection

### 3.1 Frames

Every control frame, of every kind (a broadcast's, §9.2, a work queue's, §10.2, and a service's, §11.2, too), is 64
bytes, little-endian, on the byte stream, and begins with the same 12-byte **header**. A reader continues a short read;
the end of the stream in the middle of a frame is the end of the stream.

| Offset | Size | Field |
|---|---|---|
| 0 | 4 | `magic`: the bytes `FIPC` |
| 4 | 4 | `pid` of the sender (for logs only) |
| 8 | 1 | `type` |
| 9 | 1 | `version`: the protocol version of the frame's kind |
| 10 | 1 | `kind`: 0 stream, 1 broadcast, 2 work queue, 3 service |
| 11 | 1 | reserved |

The magic, the type, the version and the kind keep their places in every version, so that each side tells a peer of
another kind or another protocol version from a corrupt one.

A stream's two frames have `kind` 0 and `version` 1. **SEGMENT** (type 1), the server's:

| Offset | Size | Field |
|---|---|---|
| 0 | 12 | the header |
| 12 | 4 | `capacity`: bytes of each ring |
| 16 | 8 | `segment_size` |
| 24 | 16 | `session_id` |
| 40 | 24 | reserved |

**READY** (type 2), the client's: the header, then 52 reserved bytes.

Reserved bytes are written as 0 and ignored on read. On Linux and macOS the SEGMENT frame carries exactly one
descriptor, the segment's (Linux: its `memfd`; macOS: its shared memory object), as `SCM_RIGHTS` ancillary data.

### 3.2 The server

`fipc_stream_create` **claims the name** at once:
- Linux: `socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK)`, `bind` to the address, `listen(4)`.
  `EADDRINUSE`: the name is held (`FIPC_ADDR_IN_USE`).
- Windows: `CreateNamedPipeW` with `FILE_FLAG_FIRST_PIPE_INSTANCE`, `PIPE_ACCESS_DUPLEX | FILE_FLAG_OVERLAPPED`,
  `PIPE_TYPE_BYTE | PIPE_READMODE_BYTE | PIPE_WAIT | PIPE_REJECT_REMOTE_CLIENTS`, unlimited instances, and the
  user-only security of §4.2. `ERROR_ACCESS_DENIED` or `ERROR_PIPE_BUSY`: the name is held. Since the first instance
  can't be created while any instance of the name exists, the name stays held while a connection the listener
  accepted is open, even after the listener is closed.
- macOS: create the `fastipc` directory if missing (§2), open `<N>.lock` (`O_CREAT`) and take its `flock(LOCK_EX |
  LOCK_NB)`, which the listener holds until it is closed. Held exclusively by another open file: the name is held
  (`FIPC_ADDR_IN_USE`). Holding it, remove a socket file a crashed listener left, then a non-blocking, close-on-exec
  socket with `SO_NOSIGPIPE`, `bind` to the path, `listen(4)`. Closing the listener removes the socket file, then the
  lock file, then lets the lock go, so the name is free again at once (also while a connection it accepted is open).
  **Crash leftovers:** a crashed owner's process (a stream's listener, a broadcast's publisher, a queue's or a
  service's owner) can't remove its two files. They are never in the way: a client that finds a socket file nobody
  listens on sees no listener and tries again, and the name's next create (`fipc_stream_create`,
  `fipc_broadcast_create`, `fipc_queue_create`, `fipc_service_create`) removes them. A name never created again keeps
  them.

Nothing runs in the background: `fipc_stream_accept` sets clients up, **one at a time**, on the thread that calls it. A
client that connects while no `fipc_stream_accept` runs waits in the listen backlog on Linux, or on the listening pipe
instance on Windows (its `connect` succeeds, and its read of SEGMENT waits), within its own timeout. A call has two
**waits**, and everything between them runs at once:

1. **Wait for a client** (or the listener's cancel), then:
   - accept it: Linux `accept4(SOCK_CLOEXEC)` on the listening socket (non-blocking: the wait is a `poll`); macOS
     `accept`, then close-on-exec and blocking mode; Windows `ConnectNamedPipe` on the listening instance
     (overlapped), which becomes the connection;
   - check its user: Linux, the peer's uid (`SO_PEERCRED`) must be this process's; macOS, the same with
     `getpeereid`; Windows, the pipe's DACL admitted only this user;
   - create the client's segment (§4.2) with a new random session id, and on Windows the next pipe instance, which the
     listener listens on once this connection is set up;
   - send SEGMENT (Linux: with the `memfd`; macOS: with the shared memory object).
2. **Wait for READY** (or the cancel), and read what arrived; once READY of this version is complete, the connection
   (the connected socket or pipe instance, and the mapped segment) goes to a connection object of its own, which watches
   the control connection from then on (§7), and `fipc_stream_accept` returns it.

**The resume rule.** A call's timeout ends it only inside a wait. Before the second wait, the client's setup (its
connection, its segment, and the bytes of READY read so far) is kept in the listener, so a call that times out has
dropped nothing and the next call resumes there. A server that polls (`fipc_stream_accept` with timeout 0) lets a client
in within two calls: the call that sends SEGMENT also reads READY if it has already arrived, else a later call reads it.
The client's `fipc_stream_connect` returns once it has sent READY, which may be before the server's `fipc_stream_accept`
returns: messages sent meanwhile wait in the ring. On Windows a wait that ends while its overlapped operation is pending
cancels the operation and waits for the cancellation to complete; an operation that completed just as it was cancelled
counts (a client that connected, bytes that arrived), so no I/O is in flight when `fipc_stream_accept` returns and
nothing that arrived is lost.

What can go wrong, and what the call does:

| Situation | What the call does |
|---|---|
| Another user's client, or a client that came and left (Linux `ECONNABORTED`, Windows `ERROR_NO_DATA`) | drop it, wait for the next client |
| A fork while the accept ran (Linux, macOS) | shut the connection down and drop it (§7), wait for the next client |
| Descriptor or handle exhaustion, any other accept error | back off, wait for the next client |
| The segment's memory can't be committed | drop the client; the listener has **failed for good**: this and every later call returns `FIPC_NO_MEMORY` |
| Handles ran out creating the segment | drop the client, back off |
| SEGMENT can't be sent | drop the client and its segment, wait for the next client |
| The end of the stream, or anything but READY, where READY belongs | drop the client and its segment, wait for the next client |
| `fipc_listener_cancel` | drop a client whose setup began; this and every later call returns `FIPC_CANCELLED` |
| `fipc_listener_close` | drop a client whose setup began |
| The connection `fipc_stream_accept` returned last is open | `FIPC_INVALID` at once (one client at a time) |

A dropped client reads the end of the stream: it connects again (§3.3), or, if its `fipc_stream_connect` returned
already, sees `FIPC_DISCONNECTED`. The **back-off** waits 1 ms, doubled after each back-off up to 100 ms, and 1 ms again
after a client is served; never past the call's deadline, and a cancel ends it at once.

### 3.3 The client

`fipc_stream_connect` sets the connection up on the thread that calls it, until its timeout: it connects, and connects
again after a back-off (as the server's) while nobody listens or an attempt ends early. Its steps:

| Step | What it does | Next |
|---|---|---|
| connect | Linux: a non-blocking `socket(AF_UNIX, SOCK_STREAM \| SOCK_CLOEXEC)`, `connect`, then blocking mode; the peer's uid (`SO_PEERCRED`) must be this process's. Windows: `CreateFileW` on the pipe (overlapped, `SECURITY_SQOS_PRESENT \| SECURITY_IDENTIFICATION`: the server may identify the client but not impersonate it), then the pipe's owner (`GetSecurityInfo`) must be this process's user SID. macOS: as Linux, with a close-on-exec socket with `SO_NOSIGPIPE`, and `getpeereid` | connected: await SEGMENT; nobody listens (`ECONNREFUSED`, `ENOENT`, `ERROR_FILE_NOT_FOUND`; on macOS also a crashed listener's socket file, or a full backlog), the backlog is full or the instance busy (`EAGAIN`, `ERROR_PIPE_BUSY`, or the pipe disconnected before its owner was read): back off; another user's listener (or `ERROR_ACCESS_DENIED`): **fail** (`FIPC_INVALID`) |
| await SEGMENT | wait for one frame (Linux, macOS: with exactly one descriptor, and no `MSG_CTRUNC`): it comes once the server's `fipc_stream_accept` sets this client up | a SEGMENT of this version whose capacity is a power of two from 1024 to 2^31 and whose `segment_size` is the size it implies (§4.1): attach; another kind (a broadcast's, a work queue's or a service's OFFER, §9.1) or another `version`: **fail** (`FIPC_INVALID`); a wrong magic or type, a bad capacity or size, a wrong descriptor count, the end of the stream (the server dropped the attempt, or left): back off |
| attach | map the segment and check it (§4.3) | checked: send READY; not the offered segment: back off; gone (the server left): back off; `ERROR_ACCESS_DENIED` (another user or an incompatible elevation): **fail** (`FIPC_INVALID`); no address space: **fail** (`FIPC_NO_MEMORY`) |
| send READY | READY | sent: the connection is set up, and `fipc_stream_connect` returns it; failed: unmap, back off |

At its timeout the call closes what it opened and returns `FIPC_TIMEOUT`; it keeps nothing between calls. A client
and its server must run on different threads (of one process or two): `fipc_stream_connect` waits for the server's
`fipc_stream_accept`. A pair within one process is logged once, since it is also the symptom of a listener never closed,
which keeps its name.

After READY nothing more is sent on the control connection, by either side.

## 4. The segment

### 4.1 Layout

A segment is `align_up(640 + 2 * capacity, 4096)` bytes: a 640-byte header, then the `s2c` ring's data at offset 640,
then the `c2s` ring's data at `640 + capacity`. The capacity is a power of two from 1024 to 2^31, the same for both
rings; the server chooses it.

| Offset | Size | Field | Written by |
|---|---|---|---|
| 0 | 8 | `magic`: the bytes `FIPCSEG1` (the digit is the version) | the server, before SEGMENT |
| 8 | 4 | `capacity` | the server, before SEGMENT |
| 12 | 4 | `header_size`: 640 | the server, before SEGMENT |
| 16 | 16 | `session_id`: random, not all zero | the server, before SEGMENT |
| 32 | 96 | reserved, 0 | |
| 128 | 256 | `s2c`: the server-to-client ring's indices (§5.1) | §5.1 |
| 384 | 256 | `c2s`: the client-to-server ring's indices | §5.1 |

Neither side reads `capacity` again after the checks of §4.3: each takes the capacity from the frame it sent or
checked, so a peer can't change it under the other.

### 4.2 Creation (the server)

- **Linux:** `memfd_create("fastipc", MFD_CLOEXEC | MFD_ALLOW_SEALING)`, `ftruncate` to the size, `fallocate` of the
  whole size (in 2 MiB chunks, each retried on `EINTR`), so that a full `/dev/shm` or memory is `FIPC_NO_MEMORY` now
  rather than `SIGBUS` later; `F_ADD_SEALS(F_SEAL_SHRINK | F_SEAL_GROW | F_SEAL_SEAL)`, so no holder can change its
  size; `mmap(MAP_SHARED)`.
- **macOS:** `shm_open` of a random name (`/fipc-` and 16 lowercase hex digits, `O_RDWR | O_CREAT | O_EXCL`, mode
  0600), then `shm_unlink` of the name at once, so the object reaches only the processes its descriptor is sent to;
  `ftruncate` to the size, which macOS allows only once per object, so no holder can change its size (it rounds up to
  whole 16 KiB pages); `mmap(MAP_SHARED)`. macOS commits the pages as they are first touched.
- **Windows:** `CreateFileMappingW(INVALID_HANDLE_VALUE, PAGE_READWRITE)` named `Local\fastipc-<sid>`, whose memory is
  committed at creation, and the four auto-reset events of §2. Every object, and the pipe, has the same security: the
  process's user SID as owner and as the only SID its DACL admits (`O:<SID>D:P(A;;GA;;;<SID>)`), and no inheritance.
  An object of that name already existing means the random id collided: another id is drawn.
- The kernel zeroes the memory; the server writes the header's fields, and only then offers the segment.

### 4.3 Attach (the client)

Before any use, the client checks what it maps:
- Linux: the received `memfd` has the three seals (`F_GET_SEALS`) and exactly `segment_size` bytes (`fstat`);
- macOS: the received descriptor is a POSIX shared memory object (`proc_pidfdinfo`), and its size (`fstat`) is
  `segment_size` rounded up to whole pages;
- Windows: `OpenFileMappingW` and `OpenEventW` by the session id's names (`ERROR_FILE_NOT_FOUND`: the server left);
- the header: `magic`, `capacity` and `header_size` as expected, and `session_id` equal to the frame's.

A client writes nothing into a segment before it sends READY, so a segment offered to a client that failed is
untouched. After READY the segment belongs to that one connection; no segment serves two.

### 4.4 Lifetime

Linux and macOS: the object has no name; the server closes its descriptor once READY has set the connection up, the
client right after mapping it, and the memory is freed with the last mapping. Windows: the section and the events are freed with
their last handle. Both sides unmap at `fipc_close`. Nothing is left after a crash at any point.

## 5. The rings

### 5.1 Indices and who writes them

Each ring's indices take 256 bytes of the header, two halves of 128 bytes: the cache line of Apple's cores, and on
x86-64 the pair of 64-byte lines its spatial prefetcher fetches together, so neither side's stores take the other's
line.

| Offset in the 256 | Size | Field | Written by |
|---|---|---|---|
| 0 | 8 | `head`: the bytes the writer has published | the ring's writer |
| 8 | 4 | `reader_sleeps`: the reader's sleep flag | the reader sets it to 1; whoever takes the 1 clears it (§6) |
| 12 | 116 | padding | |
| 128 | 8 | `tail`: the bytes the reader has consumed | the ring's reader |
| 136 | 4 | `writer_sleeps`: the writer's sleep flag | the writer sets it to 1; whoever takes the 1 clears it |
| 140 | 116 | padding | |

`head` and `tail` are monotonic byte counts that wrap at 2^64; a position's offset in the ring's data is
`position mod capacity`. The ring holds `head - tail` bytes (mod 2^64). Each flag sits on the line of the index its
waiter waits for, so a publish (store the index, then swap the flag) touches one line. The flags are the only shared
words with two writers.

### 5.2 Frames

A ring holds **frames**: a 16-byte header, then a piece of a message.

| Offset | Size | Field |
|---|---|---|
| 0 | 4 | `flags`: START 1, END 2, PAD 4 |
| 4 | 4 | `frag_len`: the piece's length |
| 8 | 8 | `total_len`: the whole message's length, in a START frame; 0 otherwise; the stamp, in a stamped frame (below) |

- **A frame takes `align_up(16 + frag_len, 16)` bytes and never wraps.** A writer whose next frame doesn't fit between
  the head and the ring's end first fills the end with a **PAD** frame (`flags` = PAD, `frag_len` = the rest less
  16), which it publishes on its own. The ring's end is a multiple of 16 away from any frame's start, so a PAD always
  fits. So every header and every piece is contiguous, and every piece starts 16-byte aligned (the data starts at
  offset 640 of a page-aligned mapping): the zero-copy calls return pointers into the ring.
- **A piece is at most `capacity - 64` bytes** (`fipc_max_piece`). A message of `n` bytes is `ceil(n / max piece)`
  pieces, each in a frame of its own: the first carries START and `total_len`, the last END, one piece both. A
  message holds at least 1 byte (a writer never sends an empty one; a reader takes one as an empty message).
- **One message at a time.** The writer writes every piece of a message, in order, before it starts the next. A
  writer stopped in the middle (a cancel, or the end of the connection) leaves the rest unwritten: its next message
  begins with a START, which tells the reader to drop the unfinished one.
- **The reader consumes a piece as it copies it out**, since the writer needs the room; it compares the message's
  `total_len` with the destination before it takes the first piece, so a message that doesn't fit stays whole.
  Frames that no receive is taking are skipped: PADs, and **orphans**, the non-START pieces of a message whose
  receive was stopped or whose writer gave it up.
- **Zero-copy.** A reservation is a frame at the head (after a PAD if needed) that the writer fills in place and
  publishes as one START | END frame. A zero-copy receive returns a one-piece message's bytes in place and consumes
  its frame when it is released.
- **Stamps.** A one-piece message (START | END) whose frame is shorter than 256 bytes (a piece of at most 224 bytes)
  carries a stamp in place of `total_len`: the frame's position, bit-inverted (`~position`, a 64-bit store with release
  ordering), written after the frame's other header fields and its bytes and before `head` is published (§6.5). A
  reader of a one-piece message reads its `frag_len` only. Every other frame carries a value below 2^63 there (a
  length or 0), and a stamp is above it (a position reaches 2^63 after centuries). A writer may leave a frame
  unstamped, with its length there, as before.

### 5.3 Trusting nothing in the ring

A ring's content and the peer's index are written by the other process, so a reader and a writer check everything
before they use it. Corrupt content ends a call with `FIPC_INVALID`, never with a crash, a hang or an access outside
the mapping:
- a ring that holds more than `capacity` bytes (a tail past the head, or garbage) is corrupt for both sides, except a
  tail less than 256 bytes past the head for its reader, which may have taken a stamped frame before its head was
  published (§6.5): no wait is ready on it, so the reader waits;
- a head off a 16-byte boundary is corrupt for its writer;
- a frame that would end past the ring's end is corrupt for its reader and stays where it is, since its end is
  unknown (the reader reads its 16-byte header first: at most 15 bytes past the ring's end, which is still mapped);
- a non-START piece that overruns its message, or whose END doesn't match the message's end, is consumed and the
  message dropped; a START without END whose `total_len` is no longer than its piece is consumed;
- `total_len` is bounded by nothing but its 64 bits: a reader allocates nothing, and a length no buffer can hold is
  `FIPC_TOO_LARGE` with that length.

## 6. Wake-ups

A side waits for its ring only when it has nothing to do: a reader for data, a writer for room. It spins first
(4096 pauses), and sleeps only if the other side hasn't moved by then.

### 6.1 The sleep protocol

While a connection is set up, every change of a sleep flag is an atomic swap (acquire-release), so the swaps of one
flag are totally ordered (its modification order), and each reads the value of the swap just before it:

- **A publisher** (a writer publishing `head`, or a reader freeing room with `tail`): stores the index (release), then
  swaps the other side's sleep flag to 0, and wakes that side if it read 1. A reader freeing room first loads the
  writer's flag (relaxed) and swaps it only if it read 1: the room's wake is inexact (below). A writer may store `head`
  after each of several frames and swap the flag once, after the last and before any wait for room (a batch of messages,
  `fipc_send_many`): the swap covers every store before it, as it covers one, and a reader still never sees more than
  one stamped frame (§6.5) past `head`.
- **A waiter**: swaps its sleep flag to 1, then loads the index again (acquire), and sleeps only if the ring is still
  not ready (and, for its own process, no cancel or end of the connection was published: §6.2); otherwise it swaps the
  flag back to 0 and goes on.
- **No wake-up is lost.** If the publisher's swap comes after the waiter's, it reads the waiter's 1, or the 0 of
  whoever took that 1 first and so woke the waiter (or of the waiter itself, which then doesn't sleep), and it wakes.
  If it comes before, the waiter's swap reads from it or from a later swap, which continues its release sequence, so
  the publisher's swap synchronizes with the waiter's: the index store happens before the waiter's load, which sees
  it, and the waiter doesn't sleep. The kernel keeps the last step: `FUTEX_WAIT(flag, 1)` doesn't sleep once the flag
  is 0 (nor does `os_sync_wait_on_address`), and an auto-reset event stays signaled until a wait consumes it.
- **The primitives.** Linux: `FUTEX_WAIT` and `FUTEX_WAKE` (one waiter) on the flag, non-private (the word is shared
  between processes). macOS: `os_sync_wait_on_address` and `os_sync_wake_by_address_any` (one waiter) on the flag,
  with `OS_SYNC_WAIT_ON_ADDRESS_SHARED` and `OS_SYNC_WAKE_BY_ADDRESS_SHARED`. Windows: `WaitForSingleObject` and `SetEvent` on the flag's event: `-s2c-data` for the `s2c`
  reader, `-s2c-space` for its writer, likewise for `c2s`.
- **The room's wake is inexact.** A reader's look at the writer's flag can miss a 1 the writer is swapping in at that
  moment, so a writer waiting for room may sleep with room free. The reader closes that gap on its own slow path: once
  a spin for data has found none, and before it sleeps, it swaps the writer's flag to 0 (and wakes the writer if it
  read 1), the exact publish of everything it consumed before. So a writer sleeps at most until its reader next waits;
  a reader that stops receiving leaves it to the writer's slice. A writer's sleeps for room last 1 ms, then twice as
  long each time, up to 100 ms. The data's wake, which every receive's latency depends on, stays exact.
- **Slices.** A sleep lasts at most 100 ms; the waiter then looks at the ring again. Every wake-up the protocol needs
  is explicit, except the inexact room's: the slice is a backstop, and the point where a wait returns to its `std.Io`.
- **The cost** of a writer's publish is one plain store and one locked swap, on the line the writer has just written;
  of a reader's, one plain store and a plain load, on the line of `tail`, which the writer rarely reads. The waiters'
  swaps are on their slow paths. A locked swap for every message a reader consumes would cost x86 a quarter of its
  small-message rate.

### 6.2 Waking one's own waiters

When a side's own connection ends (§7) or is cancelled, its blocked calls must return. The same rule applies: publish
the change in the process-local connection state (release), then swap this side's two flags (the reader flag of its
inbound ring, the writer flag of its outbound ring) to 0 and wake a sleeper that set one. A waiter looks at that state
after its swap, so the argument of §6.1 holds unchanged. Only this side's own flags are touched: the peer's waiters
are never woken by it.

### 6.3 Each side keeps the other's index

A reader keeps the head it loaded last, and loads `head` again only once the bytes up to it are used up; a writer
keeps the tail it loaded last, and loads `tail` again only once the room up to it is used up. Indices only grow, so a
kept index only ever says less than the shared one: nothing is read that isn't published, and nothing written over
what isn't consumed. A new connection's kept indices start at 0, where its indices start. So each index's cache line
stays with its writer while the ring has data and room, instead of moving between the two cores for every message.

### 6.4 A writer on a full ring waits for an eighth of it

A writer that finds no room spins until an eighth of the ring or its frame, whichever is more, is free, then writes
into that room without loading the tail again. Resuming for every frame the reader frees would load the tail, and move
its line between the cores, for every message. Only the spin looks for the eighth: after it (asleep, then woken) and
for a call that doesn't wait, the frame's own room is enough, so no call waits longer for room it has than one spin.
A writer may also spin for an eighth once when its reload of the tail finds its frame's room but less than an eighth
free, with the tail moved since its last reload, and then write into the room it has: a reader that keeps freeing a
frame or two per reload would otherwise take the tail's line back for every message, and a reader that has stopped is
not waited for. This implementation's writer does on ARM64.

### 6.5 A reader watches for a short message's stamp

A reader that has caught up learns that the frame at its tail is written from the frame's stamp (§5.2), on the frame's
own cache line, which it must read anyway, instead of from `head`, on a line of its own, first: one transfer between
the cores per message instead of two. This implementation's reader watches for stamps only while spinning, or in a
receive that doesn't wait (a poll), in a wait for an answer (it has sent since its last wait: a reader that keeps up
with a one-way stream would take the line of the frame being written from its writer), after a frame shorter than 256
bytes arrived, and loads `head` too every 64 looks; any reader may ignore stamps and wait for `head` alone, and a writer
that stamps nothing is read as before.

- **The sleep protocol is unchanged** (§6.1): the writer publishes `head` and swaps the flag after the stamp, and a
  waiter's last look before it sleeps is at `head`. A stamp only ends a spin early.
- **A stamp the reader trusts is its frame's.** This lap's other frames carry a value below 2^63 in the slot. An
  earlier lap's bytes may hold anything, so a wait trusts stamps only if the slot at its tail holds no stamp as the
  wait begins: the reader waits at its tail P only once the `head` it loaded says the frame at P isn't written, and the
  slot changes only when the writer writes the frame at P, so a stamp that appears later is that frame's.
- **A poll keeps the guard across calls.** A poll doesn't spin: it looks at the stamp before `head`, and only at a tail
  P where an earlier poll found the frame unwritten and the slot without its stamp. Positions never repeat and only the
  reader moves its tail, so while its tail is P the slot changes only when the writer writes the frame at P, however
  many calls apart the two looks are. A poll notes the slot only in a wait for an answer: it has sent since it last
  took a frame or began a wait.
- **The reader may get ahead of `head`.** Having taken a frame by its stamp, the reader can find its tail up to one
  stamped frame (less than 256 bytes) past `head`, if the writer is between its two stores. That is a wait, not a
  corrupt ring (§5.3); the writer's store of `head` and its swap end it. The writer loads its own `head`, never behind
  its stores, so the room it sees is unaffected.

## 7. Liveness: the end of a connection

- **The end is the control connection's end.** After READY each side's watcher reads its control connection;
  the end of the stream, a read error or any byte ends the connection for that side. The kernel closes a process's
  end when the process exits for any reason, or when it closes the connection. There is no heartbeat, no shared clock
  and no timeout: a paused or hung peer is not reported, and a crashed one only once its process has ended (a crash
  handler, a debugger or a core dump that holds the process delays it).
- **What ends then:** the side publishes the end in its process and wakes its own waiters (§6.2). Its sends return
  `FIPC_DISCONNECTED` at once, writing nothing. Its receives go on delivering what the peer completed: a waiting
  receive looks at the ring once more after it has seen the end, so every message published before the peer's end
  is delivered before `FIPC_DISCONNECTED`. A message the peer left unfinished is dropped.
- **One session per connection.** A connection is never set up again: to talk again, the client connects anew and the
  server accepts it, on a new segment.
- **Both sides of one user.** Peers of the same user are trusted not to be malicious (a peer that connects and never
  sends READY holds up the listener's next client until it leaves), but not to be correct (§5.3). A process of another
  user can't connect, and one that squats a name makes the client fail with `FIPC_INVALID`.
- **fork (Linux, macOS).** A forked child closes its copies of every descriptor the library holds, without shutting the
  sockets down (on macOS, nor removing a listener's files), so the parent's connections and names are unaffected: a peer still sees the end when the parent's
  process ends. The child's inherited handles fail every call but their close, and the first such failure logs why
  on stderr.

## 8. Versions

The version is the SEGMENT and READY frames' `version` and the digit of the segment's magic. Two builds of different
versions meet at the same rendezvous address and refuse each other: a client that reads a SEGMENT of another version
fails with `FIPC_INVALID` and doesn't retry; a server that reads anything but a READY of its version drops that client
and goes on listening. Any change to the frames, the segment's header, the rings' frames, the names or the wake-up
protocol bumps the version.

A broadcast's, a work queue's and a service's wire formats (§9, §10, §11) have a version of their own, the **channel
protocol**'s (version 1), which changes alone; a new kind of channel changes it too. Two builds of different channel
protocol versions refuse each other's broadcasts, queues and services (§9.1). A control frame's `kind` (§3.1) tells
a stream's, a broadcast's, a queue's and a service's frames apart.

## 9. Broadcasts

A broadcast connects one publisher to up to 1024 subscribers. Its owner (the publisher's process) claims a name as a
server does (§2, §3.2: the same rendezvous address, the same user checks) and creates the broadcast's one segment.
Each subscriber has its own control connection to the owner, which sets it up and then reports either side's end, as
a connection's does (§7). The segment holds every message once; every subscriber maps it.

### 9.1 Versions

- The broadcast's frames have `kind` **1** and `version` **1**, the channel protocol's. The broadcast segment's magic is
  **`FIPCPUB1`**, the digit the channel protocol's version. A change to anything in §9, §10 or §11 bumps the channel
  protocol's version only.
- The owner's first frame, OFFER, has SEGMENT's type (1). A client of `fipc_stream_connect` reads its kind and fails
  with `FIPC_INVALID` at once, without retrying, logging that the name is a broadcast's, a queue's or a service's. The
  owner then reads the end of the stream where JOIN belongs and drops that client.
- A joiner that reads a SEGMENT (kind 0: a stream's server, of any version) fails with `FIPC_INVALID` and closes; the
  server reads the end of the stream where READY belongs and drops it. The server sends SEGMENT only inside
  `fipc_stream_accept`: while none runs, the joiner waits for it, and fails with `FIPC_TIMEOUT` at its deadline. An
  OFFER of another channel protocol version or of another kind (a queue's or a service's, to a broadcast's joiner) is
  `FIPC_INVALID` too.

### 9.2 Frames

64 bytes each, little-endian, on the control connection: the header (§3.1), of `kind` 1 and `version` 2, then the
frame's fields. **OFFER** (type 1), the owner's:

| Offset | Size | Field |
|---|---|---|
| 12 | 4 | `capacity`: the data ring's bytes |
| 16 | 8 | `segment_size` |
| 24 | 16 | `session_id` |
| 40 | 4 | `slot`: the joiner's |
| 44 | 4 | `slot_generation` |
| 48 | 4 | `max_subscribers` |
| 52 | 4 | `flags`: bit 0, gated subscribers allowed |
| 56 | 8 | reserved |

**JOIN** (type 3), the joiner's:

| Offset | Size | Field |
|---|---|---|
| 12 | 4 | `role`: 1 lossy, 2 gated |
| 16 | 4 | `slot`: OFFER's, echoed |
| 20 | 4 | `slot_generation`: OFFER's, echoed |
| 24 | 40 | reserved |

**WELCOME** (type 4), **UNSUBSCRIBE** (type 5) and **UNSUBSCRIBED** (type 6):

| Offset | Size | Field |
|---|---|---|
| 12 | 4 | `slot` |
| 16 | 4 | `slot_generation` |
| 20 | 44 | reserved |

WELCOME echoes OFFER's slot and generation, and UNSUBSCRIBED UNSUBSCRIBE's. Reserved bytes are 0 on write and ignored
on read. On Linux and macOS OFFER carries the segment's
descriptor as its one `SCM_RIGHTS` descriptor, as SEGMENT does; the owner keeps its own for the
broadcast's life. Anything else where a frame belongs (another type, version or magic, an echo of another slot or
generation, the end of the stream) ends the attempt on that side.

### 9.3 Handshake

The owner's process runs its side on a thread of the library's, the broadcast's poller, which also watches the
subscribers' control connections (§9.7); no thread of the application waits for a joiner. None of its steps waits: it
listens on the name while it has room for a joiner, and its one wait reports a client on the listening socket or pipe
instance, and each joiner's frame. At most **8 joiners** are in setup at once, each from its accept to its JOIN:

1. While every slot is taken, or 8 joiners are being set up, it doesn't accept: joiners wait in the backlog, or for a
   pipe instance, within their own timeouts.
2. It accepts a client and checks its user (§3.2), as a server does (a process of another user is dropped).
3. It claims a free slot `s`, the first after the slot it claimed last (round robin, so that a slot just freed is the
   last to be given out again), whose generation `g` is one more than its last, and writes the slot's `cursor` (0).
   On Windows it creates the slot's wake event (§9.4).
4. It sends OFFER, with the segment's descriptor where passed.
5. It reads JOIN and checks its type, version, kind and echo, a role of 1 or 2, and for 2 that the broadcast allows
   gated subscribers. Anything else (another frame, the end of the stream) frees the slot and drops the joiner at
   once, and so does **2 s** without JOIN from the accept on: a joiner that stalls or sends garbage holds up neither
   the other joiners nor any subscriber, and holds its slot for 2 s at most.
6. It starts the subscriber: it stores the published `next` as the slot's `cursor`, where the subscriber starts; for
   a gated one it first stores `next` there, sets the slot's bit in the gated bitmap, and loads `next` again for the
   start (§9.6). Then it counts the subscriber as joined, so every message the publisher sends once the count
   includes the subscriber is at or after its start, however late the subscriber reads it; and it counts it before
   WELCOME, so a joiner whose join returned is counted.
7. It sends WELCOME and watches the subscriber's control connection, so that the subscriber's end frees the slot
   (§9.7), an end that came before the watch too. A failed send uncounts and drops the subscriber and frees the slot
   (the joiner reads the end of the stream and starts again); a watch that fails uncounts and drops it too, and
   retires the slot (§9.7).

A joiner the owner drops reads the end of the stream where WELCOME belongs, and starts again within its own deadline.

The joiner, in `fipc_broadcast_join`, within one deadline, starting again after a back-off where noted:

1. It connects (§3.3; a server of another user is `FIPC_INVALID`).
2. It reads OFFER (and its descriptor) and checks it (§9.1). A bad capacity or `max_subscribers`, a slot out of range or
   a `segment_size` other than the layout's (§9.4) is a protocol error: again.
3. It maps the segment and checks its description (§9.4), and on Windows opens its slot's wake event: a mismatch,
   or a segment gone, again; another user's segment is `FIPC_INVALID`, no address space `FIPC_NO_MEMORY`.
4. It sends JOIN with its role and the offer's slot and generation (a failed send: again).
5. It reads WELCOME (the end of the stream: again).
6. It starts at its slot's `cursor` (§9.4), read after WELCOME (acquire): the start the owner stored there, at or
   before every message sent after the owner counted it. A gated subscriber's cursor and bit are set already, so the
   publisher overwrites nothing from its start on.

After WELCOME the only frames are UNSUBSCRIBE, from the owner to a gated subscriber that held the publisher back for the
broadcast's `max_stall_ms`, and its answer UNSUBSCRIBED, once the subscriber's process can no longer read the segment.

### 9.4 The segment

```
offset 0                written by the publisher only
  0       header, 512 bytes: the description (128 bytes), then `frontier` and `next_mark` on one line, `next` on the
          next, `ended` on the last
  512     cells: capacity / 64 cells of 32 bytes
  data_offset = align_up(512 + capacity / 2, 65536)
          the data ring: capacity bytes
shared_offset = align_up(data_offset + capacity, 65536)   written by subscribers too
  +0      the shared block, 1024 bytes: `armed_any` and `wake_gen`, `writer_sleeps` (a line each), then the armed
          bitmap and the gated bitmap (16 words of 64 bits each, a line each)
slots_offset = shared_offset + 1024
  +0      max_subscribers slots of 128 bytes: a gated subscriber's `cursor` (8 bytes), then 120 reserved
segment_size = align_up(slots_offset + 128 · max_subscribers, 4096)
```

A line is 128 bytes, at an offset that is a multiple of 128 (Apple Silicon's cache line; two of x86's, which its
adjacent-line prefetch fetches together), so that the words only the publisher writes, and each slot's cursor, share no
line with a word another side writes; the words both sides write have lines of their own. Every field is
little-endian and naturally aligned; the words after the description are accessed atomically.

The header (`wire_protocol.BroadcastHeader`), at offset 0:

| Offset | Size | Field |
|---|---|---|
| 0 | 128 | the description (below) |
| 128 | 8 | `frontier`: the end of the data chunk the publisher may be writing (§9.6) |
| 136 | 8 | `next_mark`: the publisher's next index when it announced that chunk |
| 144 | 112 | 0 |
| 256 | 8 | `next`: the published next index; every message below it is complete |
| 264 | 120 | 0 |
| 384 | 4 | `ended`: 1 once the publisher's close ended the broadcast (§9.7) |
| 388 | 124 | reserved, 0 |

The description (`wire_protocol.BroadcastInfo`), written once, at offset 0:

| Offset | Size | Field |
|---|---|---|
| 0 | 8 | `magic`: the bytes `FIPCPUB1` |
| 8 | 16 | `session_id`: random, not all zero |
| 24 | 8 | `cells_offset`: 512 |
| 32 | 8 | `data_offset` |
| 40 | 8 | `shared_offset` |
| 48 | 8 | `slots_offset` |
| 56 | 8 | `segment_size` |
| 64 | 4 | `header_size`: 512 |
| 68 | 4 | `capacity`: the data ring's bytes (at most 2^31) |
| 72 | 4 | `cell_count`: capacity / 64 |
| 76 | 4 | `max_subscribers` |
| 80 | 4 | `flags`: bit 0, gated subscribers allowed |
| 84 | 44 | reserved, 0 |

A cell (`wire_protocol.BroadcastCell`, 32 bytes), cell `k` at `512 + 32 · k`:

| Offset | Size | Field |
|---|---|---|
| 0 | 8 | `seq`: the message's index + 1 once complete; with bit 63 set while the publisher writes it; 0 never written |
| 8 | 8 | `payload_position`: the payload's position in the data ring (§9.6) |
| 16 | 4 | `len`: the payload's length |
| 20 | 4 | `flags`: 0 |
| 24 | 8 | reserved, 0 |

The shared block (`wire_protocol.BroadcastShared`, 1024 bytes), at `shared_offset`; slot `s`'s bit in a bitmap is bit `s % 64` of
word `s / 64`:

| Offset | Size | Field |
|---|---|---|
| 0 | 8 | `armed_any`: bit `w`, armed word `w` may have bits set (§9.5) |
| 8 | 4 | `wake_gen`: the subscribers' wait word on Linux and macOS (§9.5) |
| 12 | 116 | 0 |
| 128 | 4 | `writer_sleeps`: the publisher's sleep flag while it waits for gated subscribers (§9.6) |
| 132 | 124 | 0 |
| 256 | 128 | the armed bitmap: 16 words of 64 bits |
| 384 | 128 | the gated bitmap: 16 words of 64 bits |
| 512 | 512 | reserved, 0 |

A slot (`wire_protocol.BroadcastSlot`, 128 bytes), slot `s` at `slots_offset + 128 · s`:

| Offset | Size | Field |
|---|---|---|
| 0 | 8 | `cursor`: the joiner's start, which the owner stores before WELCOME (§9.3); then a gated subscriber's first message not consumed (§9.6) |
| 8 | 120 | reserved, 0 |

What only the publisher writes, `[0, shared_offset)`, is whole 64-KiB blocks (Windows' view granularity, a multiple of
every page size), and a subscriber's view makes it read-only once attached (`mprotect`, `VirtualProtect`): a stray write
of a subscriber's process there faults, instead of corrupting every subscriber's messages. The rest of the segment, from
`shared_offset`, stays read-write in every view; nothing a subscriber writes is below it. For example a 4 KiB broadcast
of one subscriber takes 135168 bytes, 64 KiB of 1024 subscribers 266240 bytes. The capacity is a power of two from 4096
to 2^31. A joiner checks OFFER's capacity, max_subscribers and segment_size against this layout, its slot against
max_subscribers, and every field of the description but the reserved bytes against OFFER and the layout. Linux checks
the memfd's seals and exact size, macOS that it is a POSIX shared memory object of the size rounded up to pages (§4.3).

A zeroed segment is an empty broadcast: a cell's `seq` is the message's index plus 1 once it is complete, so 0 means
never written, and its high bit marks a cell being written. The owner writes the description once, before any OFFER.

| Words | Written by | Read by |
|---|---|---|
| description | the owner, once | joiners, at attach |
| cells, data ring, `frontier`, `next_mark`, `next` | the publisher | subscribers; the owner |
| `ended` | the publisher's close, once (1) | subscribers |
| armed bitmap, `armed_any` | subscribers (set their bits), the publisher (takes them) | the publisher, per send |
| `wake_gen` | the publisher (on a wake); Linux and macOS subscribers (their own interrupts) | sleeping subscribers |
| `writer_sleeps` | the publisher, gated subscribers and the owner (whoever swaps the 1 back to 0 wakes it) | gated subscribers |
| gated bitmap | the owner (sets a gated joiner's bit, §9.3), a gated subscriber's close and the owner (clear it) | the publisher |
| slot `cursor` | the owner (the start, §9.3), then its gated subscriber | the publisher; the joiner, once |

Windows names the broadcast's objects after its session id (§2), with the user-only security: the section
`Local\fastipc-<sid>`, the auto-reset event `Local\fastipc-<sid>-bc-room`, which wakes the publisher waiting for gated
subscribers, and per slot `s` (in decimal) the auto-reset event `Local\fastipc-<sid>-bc-wake-<s>`, which wakes that
slot's subscriber. The owner creates a slot's event when it first admits a subscriber to the slot, before OFFER, and
keeps it for the slot's later subscribers until the broadcast ends; the subscriber opens it after the attach.
Subscribers open the events to wait on and set them.

### 9.5 Wake-ups

A subscriber about to sleep arms its slot `s`: it sets bit `s % 64` of armed word `w = s / 64` (an atomic OR), then
bit `w` of `armed_any`, then looks at `next` (and `ended`, §9.7) once more and sleeps only if neither moved. On Linux
and macOS it reads `wake_gen` before it arms, and waits while `wake_gen` holds that value (a non-private futex wait,
`os_sync` with `SHARED`); on Windows it waits on its slot's event. A subscriber arms again before each sleep; a bit
already set stays.

The publisher, after its store of `next` for a send, reads `armed_any`. If it isn't 0, the publisher takes it whole
(sets it to 0), and for each bit `w` of what it took takes armed word `w` whole. Then it wakes the slots it took: on
Linux and macOS it bumps `wake_gen` and wakes every waiter of it (a non-private `FUTEX_WAKE` of all,
`os_sync_wake_by_address_all`); on Windows it sets each taken slot's event, so a wake is never another subscriber's to
take.

No wake is lost. Every one of these accesses is sequentially consistent, so they fall in one total order. If the
publisher's read of `armed_any` comes before the subscriber's OR of it, then so does the publisher's store of `next`,
and the subscriber's next look sees the new `next` and doesn't sleep. Otherwise the publisher's read, and its take after
it, see bit `w` (only the publisher clears it, and the subscriber's OR came after any earlier take), and its take of
word `w` comes after the subscriber's OR of its own bit, so it takes that bit, unless an earlier take between the
subscriber's two ORs already did: either way a take of the subscriber's bit follows its OR, and the wake that follows
the take reaches it (on Linux and macOS `wake_gen` moves after the take, so a wait on the value read before arming ends
or doesn't begin; on Windows the slot's event stays set until the subscriber's next wait). A bit of `armed_any` left set
without bits beneath only costs the publisher a later empty look. A wake that finds its subscriber awake ends its next
wait early, which looks again.

A subscriber's own interrupts (cancel, the end of its subscription) set its own slot's event on Windows. On Linux and
macOS they bump `wake_gen` and wake every waiter (the others look again and sleep on), so only while the subscriber's
receive sleeps: the receive sets a flag of its process before it reads `wake_gen` and looks at what an interrupt
publishes (both sequentially consistent), and the interrupt publishes first, then reads the flag; either the look
sees what was published, or the interrupt sees the flag and its bump comes after the receive's read of `wake_gen`.
Without that, N subscribers whose watchers all see the publisher's end would wake every sleeper N times. The publisher
waiting for room sleeps on `writer_sleeps` as a ring's writer sleeps on its flag (§6.1).

### 9.6 Messages

Message `i` (counted from 0) has cell `i % (capacity / 64)` and a payload of 1 to `capacity / 4` bytes in the data ring
at its position `payload_position`, a byte count that only grows (the ring's offset is `payload_position % capacity`). A
payload takes its length rounded up to 64 bytes and never wraps: where it wouldn't fit before the ring's end, the
publisher skips to the start. A store fence orders the stores before it before the stores after it, a load fence the
loads before it before every access after it (aarch64: `dmb ishst`, `dmb ishld`).

The publisher, for message `i`:
1. If the payload ends past `frontier`, it stores `frontier` = the end rounded up to a sixteenth of the capacity and
   `next_mark` = `i`, then a store fence: the publisher writes data only below the frontier it announced.
2. It writes the payload, then the cell: `seq` = `(i + 1) | 2^63`, a store fence, `payload_position`, `len` and `flags`
   (0), then `seq` = `i + 1` (release).
3. It stores `next` = `i + 1` (sequentially consistent), then reads `armed_any` (§9.5).

A batch of messages (`fipc_send_many`) takes steps 1 and 2 for each message and step 3 once, after its last, and before
any wait for room (whose sweep caps what it frees with the `next` stored last): a subscriber may see a cell of the batch
before `next` covers it, as it may see one cell before its own `next` store.

A lossy subscriber at message `p` loads its cell's `seq` (acquire). `p + 1`: it loads `payload_position` and `len`, a
load fence, and `seq` again, which must still be `p + 1`; it checks `len` and that
`payload_position % capacity + len <= capacity`, copies the payload, and after a load fence accepts the copy only if
`frontier - payload_position <= capacity`: a byte of the payload can only be overwritten from a ring later, after a
frontier past that. A larger `seq` (its high bit aside), a `seq` that changed, or a frontier past: the subscriber was
lapped (`FIPC_OVERRUN`) and goes on at `next_mark`, clamped to the messages after `p` up to `next`. A `seq` below
`p + 1`, or `p + 1` with the high bit: not yet written.

A gated subscriber's slot `cursor` is the first message it hasn't consumed (stored with release, after its reads). The
publisher writes message `i`'s cell only once every cursor of a slot whose gated bit is set is at least
`i + 1 - capacity / 64`, and payload bytes only below `P` plus the capacity, where `c` is the least such cursor, taken
no further than the publisher's `next` (it reads the bitmap and the cursors sequentially consistently, after its last
store of `next`), and `P` is the `payload_position` the publisher gave message `c`, or, when `c` is that `next` (every
published message consumed), the end of the last payload it wrote. The publisher takes `P` from its own record, not
from the cell: a
subscriber's process can map the segment writable and change a cell; the record keeps each position's low 32 bits, a
sixteenth of the capacity in the owner's memory. The owner starts a gated subscriber before WELCOME (§9.3): it loads
`next` and stores it as the slot's cursor (a relaxed store: the sequentially consistent OR that sets the slot's gated
bit next releases it), loads `next` again (sequentially consistent) and stores that as the cursor (release), the
subscriber's start. A sweep that saw the bit read a cursor no later than the start; one that didn't loaded the bitmap
before the bit was set, after the publisher's store of the `next` it is capped with, so the start is at or past that
`next`. The subscriber leaves by clearing its bit. A
publisher waiting for a cursor sleeps on `writer_sleeps` as a ring's writer sleeps on its flag (§6.1), woken through its
`-bc-room` event on Windows; a subscriber that moved its cursor, or cleared its bit, and finds the flag set swaps it to
0 and wakes it. Only a subscriber about to sleep, or leaving, stores its cursor and then looks at the flag sequentially
consistently; on every other receive the store is a release and the look relaxed, so a publisher that goes to sleep just
then may miss the move: it sleeps 10 ms at most at a time, which bounds that delay. A publisher that finds no room spins
until every such cursor is past the first message it needs consumed by an eighth of the messages from that one to
`next`, then sends into that room without reading the cursors again, as a stream's writer waits for an eighth of its
ring (§6.4); after a spin that doesn't see it, and for a call that doesn't wait, the room the message needs is enough.

### 9.7 Liveness

The control connections report either side's end, as a connection's does (§7): nothing is sent on them after WELCOME
but the unsubscribe frames (§9.3), so a read returns only at the other side's end.

- The owner watches every member's connection (an implementation may watch them all on one thread). Its end (the end of
  the stream, or a reset: the subscriber closed it, or its process ended) frees the slot: the owner clears the slot's
  gated bit and wakes the publisher, which may be waiting for that subscriber. A gated subscriber's close clears its bit
  and wakes the publisher itself first. A subscriber closes its connection only once its subscription ended on its side
  (below), so the slot's next subscriber is safe from it. Any other end (a read that fails otherwise, or a frame that
  doesn't belong) may leave the subscriber running with the slot mapped: the owner retires the slot instead. It clears
  the slot's gated bit and its own record of it, so that the publisher neither waits for nor wakes that subscriber, and
  gives the slot to nobody until the broadcast ends (a broadcast whose slots are all retired admits nobody).
- A subscription ends on the subscriber's side when its watcher reads the end of the control connection, a read error
  or a frame that doesn't belong (PEER_DEAD), or UNSUBSCRIBE: the watcher sets the phase (sequentially consistent),
  wakes the subscriber's receive if it sleeps, waits until no call of the subscriber is inside the segment guard, and
  only then closes the connection, or for UNSUBSCRIBE makes the mapping inaccessible and answers. A gated subscriber's
  call enters the guard (it sets a word of its own, then reads the phase, both sequentially consistent) before it
  touches the segment, and leaves it when it returns; it holds the guard while it sleeps, so its mapping is there
  while it waits. A call that saw its subscription running may write its slot's cursor and its gated bit until it
  leaves; one that entered later writes nothing of the segment, and once unsubscribed touches nothing.
- The publisher's close first stores `ended` = 1 (sequentially consistent, after its last message) and wakes every
  sleeping subscriber once (Linux, macOS: it bumps `wake_gen` and wakes all its waiters; Windows: it sets the event of
  every slot held). A subscriber that sees the mark receives what is left, then reports `FIPC_DISCONNECTED`, without
  waiting for its control connection's end; one about to sleep looks at the mark after arming, as at `next` (§9.5).
  Then the close ends every member's connection; a subscriber whose watcher reads the end after the mark wakes
  nobody: the publisher did. Nothing writes the segment after the mark, so a gated subscriber whose subscription ends
  after it (its process reads the mark when it ends the subscription) drains in place.
- An end before the mark (the owner's process ended, or the owner dropped the subscriber, whose publisher may go on; a
  mark set later protects nothing the publisher overwrote meanwhile) ends each subscription as above. A lossy
  subscriber's receives return what the publisher published, then
  `FIPC_DISCONNECTED`. A gated subscriber's in-place reads are no longer protected: its receives return the messages
  published until its subscription ended (`next` then), each copied and checked as a lossy subscriber's (§9.6); a
  message there that fails the check, and every later one, is `FIPC_DISCONNECTED`, and `fipc_recv_acquire` returns
  `FIPC_DISCONNECTED` at once. A message it held in place when its subscription ended is no longer protected either.
- The publisher's close ends the broadcast and frees the name: the owner stops admitting, drops the joiners in setup,
  and closes the listening socket or pipe instance, then every member's connection. A joiner then waits, as for a
  publisher that isn't there yet, until its timeout or a new broadcast on the name.
- A gated subscriber whose cursor stays where it is for `max_stall_ms` (positive) from the first send it held back (the
  waits of the sends that timed out meanwhile add up, until a send finds room) is unsubscribed, each such subscriber on
  its own clock: the publisher sends UNSUBSCRIBE (its slot and generation) on that member's connection, once (again at
  its next wait if the send failed). The subscriber's process ends the subscription as above, as unsubscribed, so that
  none of its calls reads the segment again (it wakes a receive asleep, then waits until no call is inside the guard);
  makes its whole mapping inaccessible (`PROT_NONE`, `PAGE_NOACCESS`: a message the application still holds in place
  then faults instead of showing what the publisher writes there next); and answers UNSUBSCRIBED (the echo). Its
  receives return `FIPC_UNSUBSCRIBED` from then on, whatever comes later. The owner takes the answer as the member's
  end: it frees the slot, clears the gated bit, wakes the publisher and closes the connection. A process that doesn't
  run (stopped, suspended) answers nothing: the publisher's sends time out until it runs again or ends, and nothing else
  frees the slot. A negative `max_stall_ms` never unsubscribes; a lossy subscriber is never unsubscribed (one that
  receives UNSUBSCRIBE ignores it). Any other frame after WELCOME, on either side, ends the connection.
- A joiner that ends before its JOIN, whose JOIN is anything else, or that sends nothing for 2 s after its accept is
  dropped where JOIN belongs, and its slot freed; one that ends after it is a member whose end frees the slot. An
  owner that ends or drops the joiner before WELCOME leaves the joiner the end of the stream, and it starts again.

## 10. Work queues

A work queue takes messages from up to 63 producers and hands each to one of up to 64 consumers. Its owner (the
process that created it) claims a name as a server does (§2, §3.2: the same rendezvous address, the same user checks)
and creates the queue's one segment; each producer and consumer has its own control connection to the owner, which
sets it up and then reports either side's end. Each producer writes one ring of the segment, of its own; consumers
claim its messages, each message for one consumer. One more ring, the return ring, is the owner's: it copies there
the message a consumer held when it ended, for another consumer (§10.7). An owner that sends or receives joins its own
queue as a member, through the name, as any process does.

### 10.1 Versions

- The queue's frames have `kind` **2** and `version` **1**, the channel protocol's (§9.1). The queue segment's magic
  is **`FIPCQUE1`**. A change to anything in this section bumps the channel protocol's version, as one in §9 does.
- OFFER has SEGMENT's type (1): a client of `fipc_stream_connect` refuses it at once for its kind (§9.1). A broadcast's
  or a service's joiner reads kind 2 as another kind, and a queue's joiner kind 1 or 3 likewise: `FIPC_INVALID`,
  without retrying. A queue's
  joiner that reads a SEGMENT fails as a broadcast's does (§9.1).

### 10.2 Frames

64 bytes each, little-endian, on the control connection: the header (§3.1), of `kind` 2 and `version` 2, then the
frame's fields. **OFFER** (type 1), the owner's:

| Offset | Size | Field |
|---|---|---|
| 12 | 4 | `capacity`: each producer's data ring's bytes |
| 16 | 8 | `segment_size` |
| 24 | 16 | `session_id` |
| 40 | 4 | `max_producers` |
| 44 | 4 | `max_consumers` |
| 48 | 16 | reserved |

**JOIN** (type 3), the joiner's:

| Offset | Size | Field |
|---|---|---|
| 12 | 4 | `role`: 3 producer, 4 consumer |
| 16 | 48 | reserved |

**WELCOME** (type 4), the owner's:

| Offset | Size | Field |
|---|---|---|
| 12 | 4 | `role`: JOIN's, echoed |
| 16 | 4 | `slot`: the member's slot among those of its role |
| 20 | 4 | `slot_generation` |
| 24 | 40 | reserved |

Reserved bytes are 0 on write and ignored on read. On Linux and macOS OFFER carries the segment's
descriptor as its one `SCM_RIGHTS` descriptor; the owner keeps its own for the queue's life. The slot comes in WELCOME,
not in OFFER: the owner learns the joiner's role only from JOIN. Nothing travels on a member's control connection
after WELCOME, in either direction.

### 10.3 Handshake

The owner's process runs its side on the queue's poller, as a broadcast's owner does (§9.3): a thread of the
library's, which also watches the members' control connections (§10.7); none of its steps waits, and at most **8
joiners** are in setup at once, each from its accept to its JOIN:

1. Before it listens it frees the draining producer slots whose rings settled (§10.7). While neither role has a free
   slot, or 8 joiners are being set up, it doesn't accept: joiners wait in the backlog, or for a pipe instance, within
   their own timeouts. Once the queue has ended (`ended`, §10.7) it accepts nobody again, and drops the joiners in
   setup.
2. It accepts a client and checks its user (§3.2), as a server does (a process of another user is dropped).
3. It sends OFFER, with the segment's descriptor where passed.
4. It reads JOIN and checks its type, version, kind and a role of 3 or 4. Anything else (another frame, the end of the
   stream) drops the joiner at once, and so does **2 s** without JOIN from the accept on: a joiner that stalls or sends
   garbage holds up neither the other joiners nor any member.
5. It claims a free slot `p` of that role, the first after the slot of the role it claimed last (round robin), whose
   generation is one more than its last. None free: it drops the joiner, which reads the end of the stream before
   WELCOME and joins again. On Windows it creates the slot's wake event (§10.4) at the slot's first claim. It resets
   the slot's words: a consumer slot's `sleeps` and `claim` to 0 and its `idle` bit cleared, a producer slot's
   `room_sleeps` to 0; a producer ring's indices stay as its last producer left them.
6. It counts the member as joined, so a joiner whose join returned is counted, then sends WELCOME (the role, the slot,
   its generation) and watches the member's control connection. A failed send uncounts the member and frees the slot
   (the joiner read no WELCOME, so it wrote nothing in the segment); a watch that fails uncounts it and retires the
   slot (the joiner may already run), and the owner keeps the connection open until the queue ends (§10.7).

The owner joins its own queue this way too, from any thread of its process: its poller admits it like any joiner.

The joiner, in `fipc_queue_join`, within one deadline, starting again after a back-off where noted:

1. It connects (§3.3; a server of another user is `FIPC_INVALID`).
2. It reads OFFER (and its descriptor) and checks it (§10.1). `max_producers` outside 1 to 63 or `max_consumers`
   outside 1 to 64, a bad capacity, or a `segment_size` other than the layout's (§10.4): a protocol error, again.
3. It maps the segment and checks its description (§10.4): a mismatch, or a segment gone, again; another user's
   segment is `FIPC_INVALID`, no address space `FIPC_NO_MEMORY`.
4. It sends JOIN with its role (a failed send: again).
5. It reads WELCOME: its role echoed and a slot below its role's maximum. The end of the stream (no free slot, the
   owner dropped it, or the owner left): again.
6. On Windows it opens its slot's wake event. One gone means the owner has torn the queue down since WELCOME: the
   queue has ended, and the member goes on without it.
7. A producer starts at its ring's `tail`, which must be below the index bound and equal its `head` (the owner gives a
   producer slot out only once its ring settled); anything else is `FIPC_INVALID`. A consumer starts at once.

### 10.4 The segment

```
offset 0      header, 512 bytes: the description (128 bytes), then `ended`, `ready` and `idle`, a line each
512           max_consumers consumer slots of 128 bytes
producers_offset = align_up(512 + 128 · max_consumers, 4096)
ring r's region, at producers_offset + r · producer_stride (rings 0 to max_producers - 1 are the producers', ring
max_producers the return ring):
  +0          the ring control block, 512 bytes: `tail`, `head`, `room_sleeps`, a line each
  +512        cells: capacity / 64 cells of 32 bytes
  data_offset = align_up(512 + capacity / 2, 4096)
              the data ring: capacity bytes
producer_stride = data_offset + capacity
segment_size = producers_offset + (max_producers + 1) · producer_stride
```

The capacity is each ring's data ring's, a power of two from 4096 to 2^31; every region is whole pages. Lines are 128
bytes (§9.4): words written by different sides never share one, except a cell's, which the producer and the consumers
write in turn. Every field is little-endian and naturally aligned; the words after the description are accessed
atomically. No part of the segment is read-only in any view: every member writes the cells of every ring. For example
4096 bytes, one producer and one consumer take 20480 bytes; 1 MiB, 8 producers and 8 consumers 14196736.

The return ring is a ring as the producers' are, whose producer is the owner: consumers claim, release and help its
messages exactly as any ring's (§10.5, §10.6), through bit `max_producers` of `ready`. Its `room_sleeps` is never set:
the owner doesn't sleep for room. One bit of `ready` per ring is why a queue has at most 63 producers.

The header (`wire_protocol.QueueHeader`), at offset 0:

| Offset | Size | Field |
|---|---|---|
| 0 | 128 | the description (below) |
| 128 | 4 | `ended`: 1 once the queue ended (§10.7), never back to 0 |
| 132 | 124 | 0 |
| 256 | 8 | `ready`: bit `r`, ring `r` may have a message to take (§10.6); bit `max_producers` is the return ring's |
| 264 | 120 | 0 |
| 384 | 8 | `idle`: bit `c`, consumer `c` parks, or is about to (§10.6) |
| 392 | 120 | reserved, 0 |

The description (`wire_protocol.QueueInfo`), written once by the owner, before any OFFER:

| Offset | Size | Field |
|---|---|---|
| 0 | 8 | `magic`: the bytes `FIPCQUE1` |
| 8 | 16 | `session_id`: random, not all zero |
| 24 | 8 | `consumers_offset`: 512 |
| 32 | 8 | `producers_offset` |
| 40 | 8 | `producer_stride`: each ring's region |
| 48 | 8 | `cells_offset`: 512, within a ring's region |
| 56 | 8 | `data_offset`, within a ring's region |
| 64 | 8 | `segment_size` |
| 72 | 4 | `header_size`: 512 |
| 76 | 4 | `capacity`: each data ring's bytes (at most 2^31) |
| 80 | 4 | `cell_count`: capacity / 64 |
| 84 | 4 | `max_producers`: 1 to 63 |
| 88 | 4 | `max_consumers`: 1 to 64 |
| 92 | 36 | reserved, 0 |

A consumer slot (`wire_protocol.QueueConsumerSlot`, 128 bytes), consumer `c`'s at `512 + 128 · c`:

| Offset | Size | Field |
|---|---|---|
| 0 | 4 | `sleeps`: the consumer's park flag (§10.6) |
| 4 | 4 | 0 |
| 8 | 8 | `claim`: the consumer's intent, stored before each claim: (index + 1) << 8 \| (ring + 1), the return ring's too; 0 none |
| 16 | 112 | reserved, 0 |

A ring control block (`wire_protocol.QueueRingControl`, 512 bytes), at the start of ring `r`'s region:

| Offset | Size | Field |
|---|---|---|
| 0 | 8 | `tail`: the published count; every message below it is complete |
| 8 | 120 | 0 |
| 128 | 8 | `head`: the first message not known to be claimed |
| 136 | 120 | 0 |
| 256 | 4 | `room_sleeps`: the producer's sleep flag while it waits for room |
| 260 | 252 | reserved, 0 |

A cell (`wire_protocol.QueueCell`, 32 bytes), message `i` of ring `p` at cell `i % cell_count` of the ring:

| Offset | Size | Field |
|---|---|---|
| 0 | 8 | `state`: 0 never written; else the phase in bits 0-1 (1 published, 2 claimed, 3 done), the claimer's consumer in bits 2-7, the message's index + 1 in bits 8-63 |
| 8 | 8 | `payload_position`: the payload's position in the data ring (a byte count that only grows) |
| 16 | 4 | `len`: the payload's length |
| 20 | 4 | `flags`: 0 |
| 24 | 8 | reserved, 0 |

A zeroed segment is an empty queue. A joiner checks OFFER's capacity, maximums and segment_size against this layout,
and every field of the description but the reserved bytes against OFFER and the layout (§4.3's checks of the mapping
too).

| Words | Written by | Read by |
|---|---|---|
| description | the owner, once | joiners, at attach |
| `ended` | the queue's close; any member's watcher at the end of its control connection (1) | producers, consumers |
| `ready` | producers (set their bit), consumers (clear a bit they found empty, set it back), the owner (after a member's end) | producers, consumers |
| `idle` | consumers (their own bit), wakers (take a bit) | wakers |
| consumer `sleeps` | its consumer, wakers; the owner at the slot's claim | the kernel (futex word) |
| consumer `claim` | its consumer; the owner at the slot's claim | the owner |
| `tail` | the ring's producer (the return ring's: the owner) | consumers, the owner |
| `head` | consumers (compare-and-swap; a queue's one consumer slot: a store), the owner (after a return) | the producer, consumers, the owner |
| `room_sleeps` | its producer, consumers, the owner | the kernel |
| cell `state` | the producer (published), consumers (claimed, done), the owner (done, for a consumer that ended holding it) | everyone |
| cell `payload_position`, `len`, `flags`; data | the producer | the claimer; a look at `len` (§10.5); the owner (the copy of a returned message) |

Windows names the queue's objects after its session id (§2), with the user-only security: the section
`Local\fastipc-<sid>`, and per slot an auto-reset wake event, `Local\fastipc-<sid>-mq-producer-<p>` and
`Local\fastipc-<sid>-mq-consumer-<c>` (in decimal). The owner creates a slot's event at the slot's first claim,
before WELCOME, and keeps it for the slot's later members until the queue's last handle closes; a member opens its
own after WELCOME and the others' to wake them.

### 10.5 Messages

Message `i` of ring `p` (counted from 0 for the ring's whole life, across its producers) has a payload of 1 to
`capacity / 4` bytes in the ring's data, at its `payload_position`, which takes its length rounded up to 64 bytes and
never wraps: where it wouldn't fit before the ring's end, the producer skips to the start. Indices stay below
2^56 - 1, so a cell's state names a message's full index: a state, once left, never comes back, and no
compare-and-swap of it can succeed on another message's.

- **Publish.** The producer writes the payload, the cell's `payload_position`, `len` and `flags`, then `state` =
  published(i) (release), then `tail` = i + 1 (sequentially consistent). It then loads `ready` and, if its bit is
  clear, sets it and wakes one parked consumer (§10.6); last it loads `ended`, and reports `FIPC_DISCONNECTED` if it is
  set (the message may still be received: §10.7). A batch of messages (`fipc_send_many`) publishes the cells of a run of
  messages, each as above, then stores `tail` past the last and loads `ready` and `ended` once, after its last message
  and before any wait for room; consumers take only messages below `tail`.
- **Claim.** A consumer looks at a ring by loading `tail`, then `head` (so that `tail - head` is at most `cell_count`
  whatever happens between the loads: a larger gap is a corrupt peer's; a `tail` it loaded earlier does while `head` is
  below it, as every message below a `tail` stays published until it is claimed). With `head` = h < `tail`, it loads cell h's
  state: published(h) is claimed by storing its intent (`claim`, release) and a compare-and-swap to claimed(h, c)
  (acquire and release), which one consumer wins; the winner then moves `head` from h to h + 1 (compare-and-swap). In a
  queue of one consumer slot (`max_consumers` 1) its consumer may claim with a store of claimed(h, c) (release) and
  move `head` with a store of h + 1 (release) instead: no other consumer claims or moves `head` while it holds the slot,
  and the owner writes them only for a consumer that ended, before the slot is given out again (§10.7). A
  cell found claimed or done while `head` is h is helped: after a pause, if `head` is still h, it is moved on by
  compare-and-swap (its claimer usually moved it meanwhile). A cell at `head` that is
  none of message h's states is a corrupt peer's: `head` is moved past it and the receive returns `FIPC_INVALID`. Claims
  follow `head`, which passes a message only once it is claimed. The queue promises its users no order of messages.
- **The look before a claim.** A receive into a buffer shorter than `capacity / 4` first loads the cell's `len`, a
  load fence and the state again (still published(h); the one consumer of a queue of one consumer slot only loads
  `len`); a longer `len` returns `FIPC_TOO_LARGE` with that length, and
  the message stays for any consumer.
- **Done.** After its copy, or the release of a message it held in place, the consumer stores `state` = done(i, c)
  (release), and wakes the producer if `room_sleeps` is set. A claimed message whose `payload_position` and `len`
  don't describe a payload of the ring is a corrupt peer's: done at once, `FIPC_INVALID`.
- **Reclaim.** The producer frees the room of the run of done messages from the first it hasn't freed, as far as
  `head` (a done cell at or past `head` may be done before `head` passed it), with acquire loads; a message not done
  holds back that producer's room, not the consumers. It writes cells below its first message not freed plus
  `cell_count`, and data below that message's payload position plus the capacity, from its own record of each
  position (the low 32 bits, a sixteenth of the capacity), never from the cells.

### 10.6 Wake-ups

- **Ready bits.** A consumer that found a ring empty, or took its last message, clears the ring's `ready` bit, then
  loads `tail` and `head` again and sets the bit back if a message is there to take. With the producer's store of
  `tail` and load of `ready` this is a Dekker handshake, every access sequentially consistent: either the consumer's
  loads see the message, or the producer's load sees the bit clear and sets it. So a ring with a message always ends
  with its bit set by someone who then claims it or wakes a consumer: a consumer that sets a bit back and doesn't take
  that ring's message at once wakes one parked consumer itself, as does a receive that leaves a message (`FIPC_TOO_LARGE`)
  or the messages after a corrupt one.
- **Park.** A consumer with nothing to take swaps its `sleeps` to 1, sets its `idle` bit, then loads `ready`, its own
  requests, its phase and `ended` (all sequentially consistent), and sleeps only while none changed and no ready bit is
  set: on Linux a non-private futex wait on `sleeps` while it holds 1, on macOS `os_sync_wait_on_address` (shared), on
  Windows a wait on its slot's event; at most a slice (100 ms), after which it looks at every ring. It clears its
  `idle` bit and `sleeps` on every way out.
- **Waking one.** A waker loads `idle` and takes a bit with an atomic AND (from the consumer the waking ring maps to on);
  a bit another waker took first is skipped. It then swaps that consumer's `sleeps` to 0 and, if it read 1, wakes it
  (`FUTEX_WAKE` of one, `os_sync_wake_by_address_any`, or its event). No wake is lost: if the producer's load of `idle`
  comes before the consumer's OR, its OR of `ready` comes before the consumer's load, which sees the work; otherwise it
  sees the idle bit, and whoever takes it swaps `sleeps` after the consumer's swap to 1. A claim that leaves messages
  behind wakes one more parked consumer, if any (chaining), for parallelism only.
- **Room.** A producer waiting for room swaps `room_sleeps` to 1, reclaims again (sequentially consistent loads), looks
  at its requests, its phase and `ended`, and sleeps (as a consumer parks, on its slot's event on Windows) a slice of 1
  ms, doubled up to 10 ms. A consumer's done is a release and its look at `room_sleeps` relaxed, so a producer that
  announces its sleep just as a consumer marks done may miss it: its slice bounds the delay. A producer that finds no
  room spins, reclaiming every 64 hints, until an eighth of its cells and of its data ring is free past the room it
  needs, then sends into that room without reclaiming again, as a stream's writer waits for an eighth of its ring
  (§6.4); after a spin that doesn't see it, and for a call that doesn't wait, the room the message needs is enough.
- **Own interrupts.** A member's cancel or end is published (sequentially consistent) before it swaps its own flag
  (`sleeps`, `room_sleeps`) to 0 and wakes it if it read 1: the waiter's looks come after its swap to 1 (§6.2).

### 10.7 Liveness

- **A member's end.** The owner never closes a live member's connection while the queue runs, and a member closes its
  own only at its close or once it read the queue's end; so the end of a member's connection (the end of the stream, or
  a reset) means that member writes the segment no more. The owner (on one thread watching every member) then: for a
  consumer, returns the message it held, if any (below), clears its `idle` bit and frees its slot; for a producer,
  makes its slot draining. After either it rescans: every ring with a message to take gets its `ready` bit, and a
  parked consumer is woken if any does (a member that ended between a store and its look may have left neither).
- **Returns.** A consumer's message is acknowledged by its done; one it held when it ended goes again. The owner reads
  the consumer's `claim` (acquire) and cell `index` of ring `ring`'s state (acquire): only claimed(index, c) is held
  (the acquire reads the claimer's compare-and-swap, which continues the producer's release of published(index), so the
  payload is visible). A payload whose `payload_position` and `len` don't describe one of the ring is a corrupt peer's:
  the message is done and dropped. Of a producer's ring, the owner reserves room in the return ring without waiting
  (reclaiming the return ring's done run first, as a producer does), copies the payload there, publishes it as a
  producer publishes (§10.5: the cell, `tail`, the `ready` bit and a wake), and only then swaps the original's state to
  done(index, c) (sequentially consistent) and moves `head` past it if it is still there; the producer reclaims the
  original's room only after the done, so the copy reads valid bytes. Without room, the claim stays as it is (the
  message pins its producer's ring) and the consumer's slot stays held, uncounted, until a retry finds room, every 5
  ms. A held message of the return ring itself pins that ring's reclaim, so the owner copies it out at once into memory
  of its own (up to `capacity` bytes), swaps it to done and moves `head` on; it publishes those copies back into the return
  ring, oldest first, as room comes, and copies a producer ring's message only while it holds none. The returned bytes,
  in the return ring and the owner's memory together, then never exceed the capacity, and no copy waits on a consumer
  that ended. A returned message comes after messages its producer sent later.
- **Draining.** A producer slot is free again once its ring settled: `head` = `tail` (every message claimed), and no
  consumer's `claim` names a message of the ring whose cell is still claimed by that consumer. The next producer of the
  slot starts at `tail`. A `tail` at or past the index bound, or a `head` past `tail`, retires the slot.
- **Retired slots.** A member whose connection fails otherwise, or which sends a frame, may still run: the owner
  retires its slot (never given out again while the queue lasts) and keeps its connection open, unwatched, until the
  queue ends: closing it would read as the queue's end to that member. Nothing of a retired member is recovered.
- **A member's view.** The end of the stream, or a read that fails, on a member's connection is the queue's end: the
  watcher stores `ended` = 1 (sequentially consistent), then the phase, then wakes the member. A whole frame is the
  member's own failure: its phase without the mark, so its calls return `FIPC_DISCONNECTED` at once (a consumer doesn't
  drain), and the watcher keeps the connection open until the member closes, so that the owner frees the slot only once
  the member's process writes nothing more.
- **The queue's end.** The queue's close (`fipc_queue_close`) stops watching and admitting, publishes the returns that
  fit without waiting (the rest are dropped), stores `ended` = 1 (sequentially consistent), drops the joiners in setup,
  closes the listening socket or pipe instance (the name is free), and closes every member's connection; the owner's
  process ending closes them too, and each member's watcher stores the mark. From the mark on, the owner admits nobody
  and returns or frees nothing; producers return `FIPC_DISCONNECTED`; a consumer that sees the mark takes what is left,
  in passes over every ring whose loads of `tail` and `head` are sequentially consistent, then returns
  `FIPC_DISCONNECTED`. A send that returned `FIPC_OK`, and a message the owner returned before the mark, is received by
  a consumer that drains: the producer stores `tail`, then loads `ended`; a consumer loads `ended`, then `tail`; in the
  one order of these accesses either the consumer's load sees the message, or the producer's load sees the mark and
  the send returns `FIPC_DISCONNECTED`. A joiner then waits as for a name nobody listens on.
- A joiner that ends before its JOIN, whose JOIN is anything else, or that sends nothing for 2 s after its accept is
  dropped where JOIN belongs; one that ends after WELCOME is a member, whose end is the first case above. An owner that
  ends or drops the joiner before WELCOME leaves the joiner the end of the stream, and it starts again.

## 11. Services

A service takes requests from up to 64 clients and hands each to one of up to 64 servers; the server that took a
request replies to it, and the reply goes back to the client that sent it. Its owner (the process that created it)
claims a name as a server does (§2, §3.2: the same rendezvous address, the same user checks) and creates the service's
one segment; each client and server has its own control connection to the owner, which sets it up and then reports
either side's end. A service is a work queue's ring set (§10.4 to §10.6) with replies: each client writes one ring of
the segment, of its own, and each request reserves the room of its reply there, just before it, so that a server never
waits for a client. Servers claim requests as a queue's consumers claim messages. A client receives the answers in the
order of its requests: its n-th answer is its n-th request's, so a request carries no id. There is no return ring:
nothing is sent again. An owner that sends or receives joins its own service as a member, through the name, as any
process does.

### 11.1 Versions

- The service's frames have `kind` **3** and `version` **1**, the channel protocol's (§9.1). The service segment's
  magic is **`FIPCSVC1`**. A change to anything in this section bumps the channel protocol's version, as one in §9
  does.
- OFFER has SEGMENT's type (1): a client of `fipc_stream_connect` refuses it at once for its kind (§9.1). A
  broadcast's or a queue's joiner reads kind 3 as another kind, and a service's joiner kind 1 or 2 likewise:
  `FIPC_INVALID`, without retrying. A service's joiner that reads a SEGMENT fails as a broadcast's does (§9.1). The
  roles go on from the queue's (5 and 6), so that no kind's JOIN names another kind's role.

### 11.2 Frames

64 bytes each, little-endian, on the control connection: the header (§3.1), of `kind` 3 and `version` 2, then the
frame's fields. **OFFER** (type 1), the owner's:

| Offset | Size | Field |
|---|---|---|
| 12 | 4 | `capacity`: each client's data ring's bytes |
| 16 | 8 | `segment_size` |
| 24 | 16 | `session_id` |
| 40 | 4 | `max_clients` |
| 44 | 4 | `max_servers` |
| 48 | 4 | `max_message`: the most bytes of a request and of a reply |
| 52 | 12 | reserved |

**JOIN** (type 3), the joiner's, has a queue's JOIN's layout (§10.2), with `role` 5 client or 6 server. **WELCOME**
(type 4), the owner's, has a queue's WELCOME's layout: the role, echoed, the slot among those of its role, and the
slot's generation. Reserved bytes are 0 on write and ignored on read. On Linux and macOS OFFER carries the segment's
descriptor as its one `SCM_RIGHTS` descriptor; the owner keeps its own for the service's life. Nothing travels on a
member's control connection after WELCOME, in either direction.

### 11.3 Handshake

A queue's (§10.3), on the service's poller, with the service's frames and these differences:

- Step 1 frees the draining client slots whose rings settled (§11.7).
- Step 4 checks a role of 5 or 6.
- Step 5, the claim of a free slot of the role, creates on Windows the slot's wake events (§11.4): a client slot's two,
  a server slot's one, at the slot's first claim. It resets the slot's words: a server slot's `sleeps` and `claim` to 0
  and its `idle` bit cleared, a client slot's `room_sleeps` and `reply_sleeps` to 0; a client ring's indices stay as
  its last client left them.
- Step 6, and each change of its counts after it (a member's end, a WELCOME that fails): the owner stores the clients
  and the servers it counts as joined in the header's `clients_joined` and `servers_joined` (§11.4), before WELCOME.

The joiner, in `fipc_service_join`: as a queue's, with `max_clients` and `max_servers` 1 to 64, and `max_message` 1 to
`capacity / 4`, checked against OFFER. On Windows a client opens its slot's two wake events, a server its one. A
client starts at its ring's `tail`, which must be below the index bound and equal its `head`; its count of answers
starts there too. A server starts at once.

### 11.4 The segment

```
offset 0      header, 512 bytes: the description (128 bytes), then `ended` (with the joined counts), `ready` and
              `idle`, a line each
512           max_servers server slots of 128 bytes
clients_offset = align_up(512 + 128 · max_servers, 4096)
client c's region, at clients_offset + c · client_stride (c < max_clients; no ring past the clients'):
  +0          the ring control block, 512 bytes: `tail`, `head`, `room_sleeps`, `reply_sleeps`, a line each
  +512        cells: capacity / 64 cells of 32 bytes
  data_offset = align_up(512 + capacity / 2, 4096)
              the data ring: capacity bytes
client_stride = data_offset + capacity
segment_size = clients_offset + max_clients · client_stride
```

The capacity is each client's data ring's, a power of two from 4096 to 2^31; every region is whole pages; lines are 128
bytes (§9.4), as a queue's. Every field is little-endian and naturally aligned; the words after the description are
accessed atomically. No part of the segment is read-only in any view: every member writes the cells of every ring, and
a server writes its reply into its client's data ring. For example 4096 bytes, one client and one server take 12288
bytes; 64 KiB, 64 clients and 8 servers 6557696.

The header (`wire_protocol.ServiceHeader`) has a queue's header's layout (§10.4): `ended` at 128, `ready` at 256 (bit
`c`: client `c`'s ring may have a request to take), `idle` at 384 (bit `s`: server `s` parks, or is about to); and on
`ended`'s line `clients_joined` at 132 and `servers_joined` at 136 (4 bytes each): the members the owner counts as
joined (§11.3), which only the owner writes. A server may read them to choose how it spins for the next request
(§11.6); they are a hint, nothing else depends on them. The description (`wire_protocol.ServiceInfo`), written once by the owner, before any OFFER:

| Offset | Size | Field |
|---|---|---|
| 0 | 8 | `magic`: the bytes `FIPCSVC1` |
| 8 | 16 | `session_id`: random, not all zero |
| 24 | 8 | `servers_offset`: 512 |
| 32 | 8 | `clients_offset` |
| 40 | 8 | `client_stride`: each client's region |
| 48 | 8 | `cells_offset`: 512, within a client's region |
| 56 | 8 | `data_offset`, within a client's region |
| 64 | 8 | `segment_size` |
| 72 | 4 | `header_size`: 512 |
| 76 | 4 | `capacity`: each data ring's bytes (at most 2^31) |
| 80 | 4 | `cell_count`: capacity / 64 |
| 84 | 4 | `max_clients`: 1 to 64 |
| 88 | 4 | `max_servers`: 1 to 64 |
| 92 | 4 | `max_message`: 1 to capacity / 4 |
| 96 | 32 | reserved, 0 |

A server slot, server `s`'s at `512 + 128 · s`, is a queue's consumer slot (§10.4): `sleeps` at 0, its park flag;
`claim` at 8, its intent, (index + 1) << 8 \| (ring + 1). A ring control block (`wire_protocol.ServiceRingControl`, 512
bytes), at the start of client `c`'s region:

| Offset | Size | Field |
|---|---|---|
| 0 | 8 | `tail`: the published count; every request below it is complete |
| 8 | 120 | 0 |
| 128 | 8 | `head`: the first request not known to be claimed |
| 136 | 120 | 0 |
| 256 | 4 | `room_sleeps`: the client's sending thread's sleep flag while it waits for room |
| 260 | 124 | 0 |
| 384 | 4 | `reply_sleeps`: the client's receiving thread's sleep flag while it waits for an answer |
| 388 | 124 | reserved, 0 |

A cell (`wire_protocol.ServiceCell`, 32 bytes), request `i` of ring `c` at cell `(i << 2) % cell_count + (i %
cell_count) / (cell_count / 4)` of the ring: the low `log2(cell_count)` bits of `i` rotated left by 2, so that
consecutive requests' cells are 128 bytes apart, each on a line of its own and in another pair of lines (a client's
publish of request `i + 1` leaves the line of request `i` to the server answering it and the client's thread waiting
for that answer); the requests whose cells share a line are a quarter of the ring apart. Its first 24 bytes are a queue
cell's:

| Offset | Size | Field |
|---|---|---|
| 0 | 8 | `state`: a queue cell's state (§10.4): the phase in bits 0-1 (1 published, 2 claimed, 3 done), the claimer's server in bits 2-7, the request's index + 1 in bits 8-63 |
| 8 | 8 | `payload_position`: the request's position in the data ring (a byte count that only grows); its reply's room is the `R` bytes just before it |
| 16 | 4 | `len`: the request's length |
| 20 | 4 | `reply_len`: the reply's length, written by the server before the done (0 for an abandoned request) |
| 24 | 4 | `outcome`: 0 none, 1 replied, 2 abandoned; written before the done |
| 28 | 4 | reserved, 0 |

`R`, a request's reply room, is `max_message` rounded up to 64. A zeroed segment is a service with no request. A joiner
checks OFFER's capacity, maximums, `max_message` and `segment_size` against this layout, and every field of the
description but the reserved bytes against OFFER and the layout (§4.3's checks of the mapping too).

| Words | Written by | Read by |
|---|---|---|
| description | the owner, once | joiners, at attach |
| `ended` | the service's close; any member's watcher at the end of its control connection (1) | clients, servers |
| `ready` | clients (set their bit), servers (clear a bit they found empty, set it back), the owner (after a member's end) | clients, servers |
| `idle` | servers (their own bit), wakers (take a bit) | wakers |
| server `sleeps` | its server, wakers; the owner at the slot's claim | the kernel (futex word) |
| server `claim` | its server; the owner at the slot's claim | the owner |
| `tail` | the ring's client | servers, the owner |
| `head` | servers (compare-and-swap), the owner (when it drops an ended client's requests) | the client, servers, the owner |
| `room_sleeps` | its client's sending thread (1, then 0 if it doesn't sleep after all), its receiving thread (0), its cancel and end (0); the owner at the slot's claim | the kernel |
| `reply_sleeps` | its client's receiving thread (1, then 0 after each look or sleep), its cancel and end (0); servers and the owner (0); the owner at the slot's claim | the kernel |
| cell `state` | the client (published), servers (claimed, done), the owner (done: abandoned or dropped) | everyone |
| cell `payload_position`, `len`; request bytes | the client | the claimer; a look at `len` (§10.5) |
| cell `reply_len`, `outcome`; reply bytes | the claiming server (the owner, when it abandons: the fields only) | the client |

Windows names the service's objects after its session id (§2), with the user-only security: the section
`Local\fastipc-<sid>`, and per slot auto-reset wake events: a client's `Local\fastipc-<sid>-sv-reply-<c>` (its
receiving thread's, which servers and the owner set) and `Local\fastipc-<sid>-sv-room-<c>` (its sending thread's,
which only its own receiving thread sets), a server's `Local\fastipc-<sid>-sv-server-<s>` (in decimal). The owner
creates a slot's events at the slot's first claim, before WELCOME, and keeps them for the slot's later members until
the service's last handle closes; a member opens its own after WELCOME and the others' to wake them.

### 11.5 Requests and replies

Request `i` of ring `c` (counted from 0 for the ring's whole life, across its clients) has a payload of 1 to
`max_message` bytes in the ring's data, and a reply room of `R` bytes just before it. The client reserves both at once,
`R` + the length rounded up to 64 bytes, which never wraps: where it wouldn't fit before the ring's end, the client
skips to the start. A ring holds at most `cell_count` requests.

- **Publish.** As a queue's producer publishes (§10.5): the request at the reservation's start + `R`, the cell's
  `payload_position` (that position), `len`, `reply_len` = 0 and `outcome` = 0, then `state` = published(i)
  (release), then `tail` = i + 1 (sequentially consistent), the `ready` bit and a wake. No load of `ended` follows
  (servers don't drain): a published request returns `FIPC_OK`, and its receive returns its answer or, after the
  service's end, `FIPC_DISCONNECTED`. The client keeps the reservation's start in its own record (§10.5's reclaim),
  never reading it from the cell.
- **Claim.** As a queue's consumer claims a message (§10.5), the look before a claim included: the intent, the
  compare-and-swap to claimed(i, s), `head`. A claimed request whose `len` isn't 1 to `max_message`, or whose
  `payload_position` modulo the capacity is below `R` or leaves less than `len` bytes before the ring's end, is a
  corrupt peer's: it is answered as abandoned (below) and the receive returns `FIPC_INVALID`. So a server writes only
  in its client's own data ring. The server then holds the request until it replies: one at a time.
- **Reply.** The server writes its reply, 1 to `max_message` bytes, into the reply room at `payload_position` − `R`,
  then stores `reply_len` and `outcome` = 1 (replied), then `state` = done(i, s) (sequentially consistent), then wakes
  the client if it sleeps (§11.6), and last loads `ended` (sequentially consistent):
  set, the reply returns `FIPC_DISCONNECTED` (its client may still receive it). After the done the server touches
  nothing of the request. **Abandon** is the same with no reply: `reply_len` = 0 and `outcome` = 2 (abandoned).
- **Receive.** The client receives the answer of its request `next`, its count of the answers it consumed: once
  `state` = done(next, any server) (acquire), it reads `outcome` and `reply_len`. Replied with a `reply_len` of 1 to
  `max_message`: the reply is at the start of the request's reservation, which its own record gives. Abandoned: the
  receive returns `FIPC_ABANDONED`. Anything else is a corrupt peer's: `FIPC_INVALID`. Each of the three consumes the
  answer, by moving `next` on (with a release ordering after its reads of the reply); a reply received in place is
  consumed at its release. A receive that times out, or a buffer too short (`FIPC_TOO_LARGE`), consumes nothing.
- **Reclaim.** As a queue's producer reclaims (§10.5), but only as far as the smallest of `head` and `next`: a
  request's room, its reply's included, is free only once its client consumed its answer. The client's sending thread
  loads `next` with an acquire ordering, so the receiving thread's reads of a reply come before the room's reuse.

### 11.6 Wake-ups

- **Ready bits, park and waking one** are a queue's (§10.6), with the servers in the consumers' place.
- **A server's spin** before its park is its own choice: it never changes what the others see. This library's servers
  read the joined counts (§11.4): one server alone looks after every hint; a pool larger than its clients looks after
  every hint too, and a server that lost a request to another spins again rather than parking (no ready bit set, its
  call's deadline still ahead); a pool no larger than its clients paces its spin as a queue's consumers do.
- **The reply's wake** is exact. A server stores the done (sequentially consistent), then loads the client's
  `reply_sleeps` (sequentially consistent) and, if it read 1, swaps it to 0 and wakes the client if the swap read 1
  (`FUTEX_WAKE` of one, `os_sync_wake_by_address_any`, or its `-sv-reply-` event); so does the owner's abandon after
  its compare-and-swap. The client, with no answer, swaps `reply_sleeps` to 1, then loads its request's state, its
  requests, its phase and `ended` (all sequentially consistent), and sleeps a slice at most only if none changed.
  Either the client's load sees the done, or the server's load sees its 1. A server may swap the flag without the load
  first: the pair holds either way. Several servers may swap the flag: they answer a client's requests in any order,
  and a wake for a later request only makes the client look and sleep again.
- **Room.** A client waiting for room swaps `room_sleeps` to 1, reclaims again (sequentially consistent loads), and
  sleeps as a queue's producer does (§10.6), in slices of 1 ms doubled up to 10 ms. Only its own receiving thread
  wakes it, after it consumed an answer, with a relaxed look at the flag: the slices bound a missed wake.
- **Own interrupts.** A member's cancel or end is published (sequentially consistent) before it swaps its own flags
  (a client's `room_sleeps` and `reply_sleeps`, a server's `sleeps`) to 0 and wakes each that read 1.

### 11.7 Liveness

- **A member's end.** As a queue's (§10.7): the end of a member's connection means that member writes the segment no
  more. The owner then, for a server, abandons the request it held, if any: it reads the server's `claim` (acquire)
  and that cell's state (acquire), and only claimed(i, s) is held; it stores `reply_len` = 0 and `outcome` = 2, then
  swaps the state from claimed(i, s) to done(i, s) (sequentially consistent), moves `head` past it if it is still
  there, and wakes the client (§11.6). A server that stored its done just before it ended keeps its reply: the
  compare-and-swap fails. Whatever the server held, the owner then wakes the client of the ring its `claim` names if
  it sleeps (§11.6): a server that ended between its done and its wake left its client asleep.
  Then it clears the server's `idle` bit and frees its slot. For a client, it drops the
  requests no server took: while `head` is below `tail`, it swaps the state at `head` from published(head) to done(head,
  0) (acquire and release) and moves `head` on as a claim does (a cell claimed or done is helped on, a corrupt one
  skipped); nobody waits for their answers. The requests servers hold are still answered, into the ring. Its slot is
  draining. After either it rescans the rings (§10.7).
- **Draining.** A client slot is free again once its ring settled, as a queue's producer slot (§10.7): `head` = `tail`,
  and no server's `claim` names a request of the ring whose cell is still claimed by that server. The next client of
  the slot starts at `tail`.
- **A server's close** abandons the request it holds, as the owner would, unless the service has ended. Its
  `fipc_recv_release` does nothing.
- **Retired slots** are a queue's (§10.7), but a server retired for a frame stays watched: the owner drops each frame
  it sends, and at its connection's end abandons the request it holds then, wakes the client its `claim` names, and
  rescans, as at any server's end; its slot stays retired. Its client receives no later answer before that one, and
  the owner can't abandon it at the retirement: the server may still be running and replying (its done is a store, not
  a compare-and-swap, and its reply's bytes would land in room the client reused). Nothing of a retired client is
  dropped, nor of a server whose read failed or whose connection the owner can't watch: a client waiting for such a
  server's request waits until its own timeouts.
- **A member's view.** As a queue member's (§10.7): the end of the stream, or a read that fails, on a member's
  connection is the service's end (the mark, then the phase), and a whole frame the member's own failure, after which
  its calls return `FIPC_DISCONNECTED` at once.
- **The service's end.** The service's close (`fipc_service_close`) stops watching and admitting, stores `ended` = 1
  (sequentially consistent), drops the joiners in setup, closes the listening socket or pipe instance (the name is
  free), and closes every member's connection; the owner's process ending closes them too, and each member's watcher
  stores the mark. From the mark on, the owner admits nobody and abandons, drops or frees nothing; clients' sends,
  servers' receives and replies return `FIPC_DISCONNECTED` (servers don't drain); a client receives the answers done
  before the mark, in order, then `FIPC_DISCONNECTED` at its first request without one. A reply that returned
  `FIPC_OK` is received by a client that receives: the server stores its done, then loads `ended`; the client loads
  `ended`, then the state; in the one order of these accesses either the client's load sees the done, or the server's
  load sees the mark and the reply returns `FIPC_DISCONNECTED`. A client's `FIPC_DISCONNECTED`, at the end or at its
  own failure, is final: every later receive returns it, though a done may still come (a reply or an abandon whose
  server loaded `ended` before the mark). A joiner then waits as for a name nobody listens on.
- A joiner that ends before its JOIN, or whose JOIN is anything else, is a queue's joiner's case (§10.7).
