"""
Frozen-file check: files that must not change without the user's explicit approval.

Each frozen file is recorded with its git blob hash (`git hash-object <path>`: the file's contents with the
repository's line endings, so the same on every OS and in a shallow clone). The check hashes the working tree's copy,
committed or not, and fails on any difference. To change a frozen file once the user approved it, record its new hash
here in the same commit.
"""

import subprocess
from pathlib import Path

# path (relative to the repo root) -> git blob hash of its frozen contents
FROZEN = {
    "include/fipc.h": "4f5cff29c9bd18d967fcb623eb3094ecc0d17f76",
    "include/fipc.hpp": "01b6fa1963e9deddb4bc1706051027298bd85a6d",
}


def check_frozen(repo_root: Path) -> bool:
    """Report frozen files whose contents differ from their recorded hash. True if every one matches."""
    changed = []
    for path, frozen_hash in FROZEN.items():
        result = subprocess.run(
            ["git", "hash-object", "--", path], cwd=repo_root, capture_output=True, text=True, check=False
        )
        if result.returncode != 0:
            print(f"❌ {path}: {result.stderr.strip() or 'git hash-object failed'}")
            return False
        if result.stdout.strip() != frozen_hash:
            changed.append(path)
    if changed:
        print(f"❌ {len(changed)} frozen file(s) changed: {', '.join(changed)}")
        print("A frozen file changes only as a deliberate API change (CONTRIBUTING.md); then record its new hash in "
              "devtool_lib/frozen.py.")
        return False
    print(f"✅ Frozen files unchanged: {', '.join(FROZEN)}")
    return True
