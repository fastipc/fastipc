# The FastIPC protocol, version 1

What two processes that use this library share: the rendezvous names, the control connection's frames, the shared
segment's layout, the rings' frames, the wake-ups and the end of a connection. Two builds of the library interoperate
exactly when they speak the same version; a change to anything here bumps the version (§8). The API on top is
[`include/fipc.h`](../include/fipc.h); how the library implements it is [`design/architecture.md`](design/architecture.md).

All integers are little-endian; offsets and sizes are in bytes. The platforms are x86-64 (Linux, Windows) and arm64
(macOS); the ring's argument (§6.1) relies on neither's memory model: it holds in the language's memory model. The
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
| Left behind by a crash | nothing | nothing | the name's socket file and lock file, until the next listen by the same user (§3.2) |

The **server** side of a connection writes the server-to-client ring (`s2c`) and reads the client-to-server ring
(`c2s`); the client does the opposite. Each ring has one writer and one reader, which are threads of different
processes.

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

Two frame types, each 64 bytes, on the byte stream. A reader continues a short read; the end of the stream in the
middle of a frame is the end of the stream.

| Offset | Size | Field | SEGMENT | READY |
|---|---|---|---|---|
| 0 | 4 | `magic`: the bytes `FIPC` | yes | yes |
| 4 | 1 | `type`: 1 SEGMENT, 2 READY | 1 | 2 |
| 5 | 1 | `version`: 1 | yes | yes |
| 6 | 2 | reserved | | |
| 8 | 4 | `pid` of the sender (for logs only) | yes | yes |
| 12 | 4 | reserved | | |
| 16 | 8 | `capacity`: bytes of each ring | yes | |
| 24 | 8 | `segment_size` | yes | |
| 32 | 16 | `session_id` | yes | |
| 48 | 16 | reserved | | |

Reserved bytes are written as 0 and ignored on read. On Linux and macOS the SEGMENT frame carries exactly one
descriptor, the segment's (Linux: its `memfd`; macOS: its shared memory object), as `SCM_RIGHTS` ancillary data.

### 3.2 The server

`fipc_listen` **claims the name** at once:
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
  **Crash leftovers:** a crashed listener's process can't remove its two files. They are never in the way: a client
  that finds a socket file nobody listens on sees no listener and tries again, and the name's next listener removes
  them. Nor do they accumulate: every listen also removes the files of the user's names that no listener holds (it
  takes each name's lock shared, without waiting, so it never touches a running or stopped listener's files), once
  they are two seconds old. A listen that meets such a removal of its own name waits the few microseconds it takes.

Nothing runs in the background: `fipc_accept` sets clients up, **one at a time**, on the thread that calls it. A
client that connects while no `fipc_accept` runs waits in the listen backlog on Linux, or on the listening pipe
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
   the control connection from then on (§7), and `fipc_accept` returns it.

**The resume rule.** A call's timeout ends it only inside a wait. Before the second wait, the client's setup (its
connection, its segment, and the bytes of READY read so far) is kept in the listener, so a call that times out has
dropped nothing and the next call resumes there. A server that polls (`fipc_accept` with timeout 0) lets a client in
within two calls: the call that sends SEGMENT also reads READY if it has already arrived, else a later call reads it.
The client's `fipc_connect` returns once it has sent READY, which may be before the server's `fipc_accept` returns:
messages sent meanwhile wait in the ring. On Windows a wait that ends while its overlapped operation is pending cancels
the operation and waits for the cancellation to complete; an operation that completed just as it was cancelled counts (a
client that connected, bytes that arrived), so no I/O is in flight when `fipc_accept` returns and nothing that arrived
is lost.

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
| The connection `fipc_accept` returned last is open | `FIPC_INVALID` at once (one client at a time) |

A dropped client reads the end of the stream: it connects again (§3.3), or, if its `fipc_connect` returned already,
sees `FIPC_DISCONNECTED`. The **back-off** waits 1 ms, doubled after each back-off up to 100 ms, and 1 ms again after a
client is served; never past the call's deadline, and a cancel ends it at once.

### 3.3 The client

`fipc_connect` sets the connection up on the thread that calls it, until its timeout: it connects, and connects again
after a back-off (as the server's) while nobody listens or an attempt ends early. Its steps:

| Step | What it does | Next |
|---|---|---|
| connect | Linux: a non-blocking `socket(AF_UNIX, SOCK_STREAM \| SOCK_CLOEXEC)`, `connect`, then blocking mode; the peer's uid (`SO_PEERCRED`) must be this process's. Windows: `CreateFileW` on the pipe (overlapped, `SECURITY_SQOS_PRESENT \| SECURITY_IDENTIFICATION`: the server may identify the client but not impersonate it), then the pipe's owner (`GetSecurityInfo`) must be this process's user SID. macOS: as Linux, with a close-on-exec socket with `SO_NOSIGPIPE`, and `getpeereid` | connected: await SEGMENT; nobody listens (`ECONNREFUSED`, `ENOENT`, `ERROR_FILE_NOT_FOUND`; on macOS also a crashed listener's socket file, or a full backlog), the backlog is full or the instance busy (`EAGAIN`, `ERROR_PIPE_BUSY`, or the pipe disconnected before its owner was read): back off; another user's listener (or `ERROR_ACCESS_DENIED`): **fail** (`FIPC_INVALID`) |
| await SEGMENT | wait for one frame (Linux, macOS: with exactly one descriptor, and no `MSG_CTRUNC`): it comes once the server's `fipc_accept` sets this client up | a SEGMENT of this version whose capacity is a power of two from 1024 to 2^31 and whose `segment_size` is the size it implies (§4.1): attach; another `version`: **fail** (`FIPC_INVALID`); a wrong magic or type, a bad capacity or size, a wrong descriptor count, the end of the stream (the server dropped the attempt, or left): back off |
| attach | map the segment and check it (§4.3) | checked: send READY; not the offered segment: back off; gone (the server left): back off; `ERROR_ACCESS_DENIED` (another user or an incompatible elevation): **fail** (`FIPC_INVALID`); no address space: **fail** (`FIPC_NO_MEMORY`) |
| send READY | READY | sent: the connection is set up, and `fipc_connect` returns it; failed: unmap, back off |

At its timeout the call closes what it opened and returns `FIPC_TIMEOUT`; it keeps nothing between calls. A client
and its server must run on different threads (of one process or two): `fipc_connect` waits for the server's
`fipc_accept`. A pair within one process is logged once, since it is also the symptom of a listener never closed,
which keeps its name.

After READY nothing more is sent on the control connection, by either side.

## 4. The segment

### 4.1 Layout

A segment is `align_up(384 + 2 * capacity, 4096)` bytes: a 384-byte header, then the `s2c` ring's data at offset 384,
then the `c2s` ring's data at `384 + capacity`. The capacity is a power of two from 1024 to 2^31, the same for both
rings; the server chooses it.

| Offset | Size | Field | Written by |
|---|---|---|---|
| 0 | 8 | `magic`: the bytes `FIPCSEG1` (the digit is the version) | the server, before SEGMENT |
| 8 | 128 | `s2c`: the server-to-client ring's indices (§5.1) | §5.1 |
| 136 | 128 | `c2s`: the client-to-server ring's indices | §5.1 |
| 264 | 4 | `capacity` | the server, before SEGMENT |
| 268 | 4 | `header_size`: 384 | the server, before SEGMENT |
| 272 | 16 | `session_id`: random, not all zero | the server, before SEGMENT |
| 288 | 96 | reserved, 0 | |

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

Each ring's indices take 128 bytes of the header, two cache lines:

| Offset in the 128 | Size | Field | Written by |
|---|---|---|---|
| 0 | 8 | `head`: the bytes the writer has published | the ring's writer |
| 8 | 4 | `reader_sleeps`: the reader's sleep flag | the reader sets it to 1; whoever takes the 1 clears it (§6) |
| 12 | 52 | padding | |
| 64 | 8 | `tail`: the bytes the reader has consumed | the ring's reader |
| 72 | 4 | `writer_sleeps`: the writer's sleep flag | the writer sets it to 1; whoever takes the 1 clears it |
| 76 | 52 | padding | |

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
| 8 | 8 | `total_len`: the whole message's length, in a START frame; 0 otherwise |

- **A frame takes `align_up(16 + frag_len, 16)` bytes and never wraps.** A writer whose next frame doesn't fit between
  the head and the ring's end first fills the end with a **PAD** frame (`flags` = PAD, `frag_len` = the rest less
  16), which it publishes on its own. The ring's end is a multiple of 16 away from any frame's start, so a PAD always
  fits. So every header and every piece is contiguous, and every piece starts 16-byte aligned (the data starts at
  offset 384 of a page-aligned mapping): the zero-copy calls return pointers into the ring.
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

### 5.3 Trusting nothing in the ring

A ring's content and the peer's index are written by the other process, so a reader and a writer check everything
before they use it. Corrupt content ends a call with `FIPC_INVALID`, never with a crash, a hang or an access outside
the mapping:
- a ring that holds more than `capacity` bytes (a tail past the head, or garbage) is corrupt for both sides;
- a head off a 16-byte boundary is corrupt for its writer;
- a frame that would end past the ring's end is corrupt for its reader and stays where it is, since its end is
  unknown (the reader reads its 16-byte header first: at most 15 bytes past the ring's end, which is still mapped);
- a non-START piece that overruns its message, or whose END doesn't match the message's end, is consumed and the
  message dropped; a START without END whose `total_len` is no longer than its piece is consumed;
- `total_len` is bounded by nothing but its 64 bits: a reader allocates nothing, and a length no buffer can hold is
  `FIPC_TOO_LARGE` with that length.

### 5.4 RPC messages

An RPC message is a plain message whose first 32 bytes, in its first piece, are this header, followed by the payload:

| Offset | Size | Field |
|---|---|---|
| 0 | 8 | `id`: the request's id, echoed in its response; a connection numbers its requests from 1 |
| 8 | 4 | `kind`: 1 request, 2 response |
| 12 | 4 | `opcode` |
| 16 | 4 | `status` (signed) |
| 20 | 4 | reserved, 0 |
| 24 | 8 | `len`: the payload's length |

A receiver drops a message whose first piece is shorter than 32 bytes, whose `len` isn't the message's length less
32, or whose `kind` is another (`FIPC_INVALID`). The layout is `fipc_rpc_msg_t`'s.

## 6. Wake-ups

A side waits for its ring only when it has nothing to do: a reader for data, a writer for room. It spins first
(4096 pauses), and sleeps only if the other side hasn't moved by then.

### 6.1 The sleep protocol

While a connection is set up, every change of a sleep flag is an atomic swap (acquire-release), so the swaps of one
flag are totally ordered (its modification order), and each reads the value of the swap just before it:

- **A publisher** (a writer publishing `head`, or a reader freeing room with `tail`): stores the index (release), then
  swaps the other side's sleep flag to 0, and wakes that side if it read 1.
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
- **Slices.** A sleep lasts at most 100 ms; the waiter then looks at the ring again. Every wake-up the protocol needs
  is explicit: the slice is a backstop, and the point where a wait returns to its `std.Io`.
- **The cost** of a publish is one plain store and one locked swap, on the line the publisher has just written; the
  waiter's swaps are on its slow path.

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
and goes on listening. Any change to the frames, the segment's header, the rings' frames, the RPC header, the names or
the wake-up protocol bumps the version.
