"""
The macOS rendezvous files of a name (docs/protocol.md section 2; src/zig/lifecycle/names.zig, macosRendezvous): the
socket file `<dir>fastipc/<N>` and its lock file `<N>.lock` beside it, where `<dir>` is the user's private directory,
`confstr(_CS_DARWIN_USER_DIR)`, as the library reads it. The harnesses use them to see whether a server listens
(interop.py) and what a pair left behind (chaos.py).
"""

import hashlib
import os
from typing import Optional

_CS_DARWIN_USER_DIR = 65536
MAX_PATH = 103  # sun_path's 104 bytes, the last a NUL
DIR = "fastipc/"


def macos_user_dir() -> str:
    """The user's private directory, `/var/folders/<xx>/<id>/0/`, ending with `/`."""
    path = os.confstr(_CS_DARWIN_USER_DIR)
    if not path:
        raise OSError("confstr(_CS_DARWIN_USER_DIR) gave no directory")
    return path if path.endswith("/") else path + "/"


def macos_socket_path(name: str, user_dir: Optional[str] = None) -> Optional[str]:
    """The name's socket path: N is the name if the path fits in 103 bytes, else the name's first 8 bytes, `~` and 32
    hex digits of the first 16 bytes of SHA-256(name), else `~` and the digits alone; None if no N fits."""
    head = os.fsencode((user_dir or macos_user_dir()) + DIR)
    raw = name.encode()
    room = MAX_PATH - len(head)
    if len(raw) <= room:
        return os.fsdecode(head + raw)
    digits = hashlib.sha256(raw).hexdigest()[:32].encode()
    for tail in (raw[:8] + b"~" + digits, b"~" + digits):
        if len(tail) <= room:
            return os.fsdecode(head + tail)
    return None


def macos_files(name: str) -> list:
    """The socket file and the lock file of `name`, whether or not they exist."""
    socket_path = macos_socket_path(name)
    return [] if socket_path is None else [socket_path, socket_path + ".lock"]
