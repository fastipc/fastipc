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
DACL), and nothing is left behind after a crash (on macOS, a crashed listener's socket and lock files, in the user's
private directory, until the user's next listen). Within that boundary the peer is trusted not to be malicious but not
to be correct: every index and frame it writes into shared memory is checked before use, so a buggy or corrupt peer
makes a call fail with `FIPC_INVALID`, never read or write outside the mapping, crash or hang (a fuzz harness checks
this). The library does **not** defend against a malicious process of the same user, which can read and change the
messages, hold a name, keep a listener busy by connecting and never completing the handshake, or simply attach a
debugger; nor against an administrator or root; nor does it encrypt or authenticate message contents. A paused or
hung peer isn't detected (there is no heartbeat). Details:
[`docs/protocol.md`](docs/protocol.md) §3-§7 and [`docs/design/architecture.md`](docs/design/architecture.md) §11.

**In scope**, for example: a process of another user that can connect, claim or disturb a name, or read or write a
connection's memory; content a peer writes into shared memory, or frames it sends on the control connection, that
make the library access memory outside its mapping, corrupt memory, crash, or wait forever where its contract says
it returns; handles or memory leaked to a child process or another user.

**Out of scope:** anything that needs the same user's (or an administrator's) privileges to begin with, denial of
service between processes of the same user, and the documented limits above.
