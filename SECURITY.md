# Security policy

## Reporting a vulnerability

Please report vulnerabilities privately, through GitHub's private vulnerability reporting:
[**Report a vulnerability**](https://github.com/fastipc/fastipc/security/advisories/new) (the repository's
Security tab). Don't open a public issue for one.

Include the version or commit, the OS, what an attacker needs (which user, which process) and what they gain, and a
proof of concept if you have one. You'll get an acknowledgement as soon as possible, normally within a week, and the
fix is released with an advisory that credits you unless you'd rather not be named.

## Supported versions

| Version | Supported |
|---|---|
| 1.0.x | yes |

Fixes go into the latest release.

## Threat model

FastIPC connects two processes **of the same user on one machine**; there is no network transport. Only that
user's processes can connect: on Linux both sides check the peer's uid (`SO_PEERCRED`) on an abstract `AF_UNIX`
socket; on macOS both sides check it (`getpeereid`) on a socket file in a directory only the user can enter; on
Windows the pipe, the shared section and the events admit only the creating user's SID, the client checks
the pipe's owner, and remote pipe clients are rejected. A segment is never reachable by another user (on Linux an
unnamed, sealed `memfd` passed over the socket; on macOS a POSIX shared memory object whose name is removed at once,
passed over the socket; on Windows a section named by a random 128-bit id with an owner-only
DACL), and nothing is left behind after a crash (on macOS, a crashed owner's socket and lock files, in the user's
private directory, until the name's next create). Within that boundary the peer is trusted not to be malicious but not
to be correct: every index and frame it writes into shared memory is checked before use, so a buggy or corrupt peer
makes a call fail with `FIPC_INVALID`, never read or write outside the mapping, crash or hang (a fuzz harness checks
this). The library does **not** defend against a malicious process of the same user, which can read and change the
messages, hold a name, keep a listener busy by connecting and never completing the handshake, or simply attach a
debugger; nor against an administrator or root; nor does it encrypt or authenticate message contents. A paused or
hung peer isn't detected (there is no heartbeat).

A broadcast's subscribers are such peers too, of the publisher and of each other: every subscriber's process
can write the segment's shared part, which holds every subscriber slot's cursor, the wake-up words and the bitmaps.
So a buggy subscriber can disturb the other subscribers' delivery (messages lost, or overwritten before a gated
subscriber read them; sends that stall or time out; wake-ups delayed or spurious; a gated subscriber unsubscribed
because its cursor was set back while it slept, so that the writer saw it stalled), but it can crash no one: the
writer and each subscriber check what they read there. The writer acts only on its own records and on the slots it
admitted, and a subscriber bounds every index and checks every cell before it copies: a bad value fails that
subscriber's call with `FIPC_INVALID`, never an access outside the segment.

A work queue's producers and consumers are such peers too, of the owner and of each other: every member's process can
write the whole of the queue's segment, the cells of every producer's ring included (a consumer claims a message
there). So a buggy member can disturb delivery (messages dropped or skipped, a producer's room held back so that its
sends time out, wake-ups delayed or spurious, the queue ended), but it can crash no one: every member and the owner
check what they read there. Bitmaps are masked to the queue's members, a ring's indices and a claimed message's
payload are checked before use, and a producer takes its payloads' positions from its own record: a bad value fails
that call with `FIPC_INVALID`, never an access outside the segment.

A service's clients and servers are such peers too, as a work queue's members are: every member's process can write
the whole of the service's segment, and a server writes its reply into its client's ring. So a buggy member can
disturb the others' requests and replies (requests dropped, answered early or wrongly, a client's room held back so
that its sends time out, wake-ups delayed or spurious, the service ended), but it can crash no one. A server checks a
request's length and position before it touches the request or its reply's room, so it writes only inside that
client's own ring, a client checks each answer's outcome and length and finds its replies through its own record of
its requests' positions, and a bad value fails that call with `FIPC_INVALID`. Details:
[`docs/protocol.md`](docs/protocol.md) §3-§7 and §9-§11, and [the architecture page](https://fastipc.github.io/fastipc/architecture.html) §5.2, §6.2, §7.2
and its [known limits](https://fastipc.github.io/fastipc/architecture.html#limits).

**In scope**, for example: a process of another user that can connect, claim or disturb a name, or read or write a
connection's memory; content a peer writes into shared memory, or frames it sends on the control connection, that
make the library access memory outside its mapping, corrupt memory, crash, or wait forever where its contract says
it returns; handles or memory leaked to a child process or another user.

**Out of scope:** anything that needs the same user's (or an administrator's) privileges to begin with, denial of
service between processes of the same user, and the documented limits above.
