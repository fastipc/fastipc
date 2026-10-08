"""
Code formatting module for Zig files using `zig fmt`
"""

import subprocess
from pathlib import Path

# Zig sources owned by this repo. Fetched (zig-pkg/) and vendored (vendor/) packages are never formatted.
ZIG_PATHS = ["build.zig", "build.zig.zon", "src", "tests/zig", "tests/soak", "tests/fuzz", "bench/zig", "bench/zig-api",
             "examples/c-cpp/build.zig", "examples/c-cpp/build.zig.zon",
             "examples/zig/build.zig", "examples/zig/build.zig.zon", "examples/zig/src"]


def format_zig(repo_root: Path, check_only: bool = False) -> bool:
    """Format (or check) the repo's Zig files with `zig fmt`.

    Args:
        repo_root: Path to the repository root directory
        check_only: If True, only check formatting without modifying files

    Returns:
        True if formatting succeeded (or all files were already formatted)
    """
    paths = [p for p in ZIG_PATHS if (repo_root / p).exists()]
    cmd = ["zig", "fmt"] + (["--check"] if check_only else []) + paths
    print(f"🔍 Running: {' '.join(cmd)}")
    try:
        result = subprocess.run(cmd, cwd=repo_root, capture_output=True, text=True, check=False)
    except FileNotFoundError:
        print("❌ Error: zig not found on PATH")
        return False

    # zig fmt prints the files it changed (or, with --check, the files that need formatting)
    changed = [line for line in result.stdout.splitlines() if line.strip()]
    if result.returncode != 0:
        for line in changed:
            print(f"❌ File {line} is not properly formatted")
        if result.stderr.strip():
            print(result.stderr.strip())
        if check_only:
            print("\n💡 To fix Zig formatting issues, run:")
            print("  ./devtool.py format")
        return False

    if check_only:
        print("✅ All Zig code is properly formatted")
    else:
        for line in changed:
            print(f"  formatted {line}")
        print("✅ Zig formatting complete")
    return True
