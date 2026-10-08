"""
Code formatting module for Go files using `gofmt`
"""

import subprocess
from pathlib import Path

from devtool_lib import package

# Go sources owned by this repo: the binding's module and the benchmark's
GO_PATHS = ["bindings/go", "bench/go"]


def format_go(repo_root: Path, check_only: bool = False) -> bool:
    """Format (or check) the repo's Go files with `gofmt` (the one next to the go command devtool finds, else gofmt on
    PATH).

    Args:
        repo_root: Path to the repository root directory
        check_only: If True, only check formatting without modifying files

    Returns:
        True if formatting succeeded (or all files were already formatted)
    """
    gofmt = package.gofmt()
    if not gofmt:
        print("❌ Error: gofmt not found (install Go: set GO, put go on PATH, or install it in ~/go-sdk/go<version>)")
        return False
    paths = [p for p in GO_PATHS if (repo_root / p).exists()]
    cmd = [gofmt, "-l"] + ([] if check_only else ["-w"]) + paths
    print(f"🔍 Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=repo_root, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        print(f"❌ gofmt failed:\n{result.stderr.strip()}")
        return False

    # gofmt -l prints the files whose formatting differs (with -w, the ones it rewrote)
    listed = [line for line in result.stdout.splitlines() if line.strip()]
    if check_only:
        for line in listed:
            print(f"❌ File {line} is not properly formatted")
        if listed:
            print("\n💡 To fix Go formatting issues, run:")
            print("  ./devtool.py format")
            return False
        print("✅ All Go code is properly formatted")
    else:
        for line in listed:
            print(f"  formatted {line}")
        print("✅ Go formatting complete")
    return True
