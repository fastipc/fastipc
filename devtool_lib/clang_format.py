"""
Code formatting module for the C and C++ files (.c, .h, .cpp, .hpp) using clang-format, the version pinned in
devtool_lib/requirements.txt (installed into the venv)
"""

import subprocess
import sys
from pathlib import Path
from typing import List


def format_code(repo_root: Path, clang_format: Path, check_only: bool = False):
    """Format all C and C++ files with clang-format

    Args:
        repo_root: Path to the repository root directory
        clang_format: Path to the venv's clang-format executable
        check_only: If True, only check formatting without modifying files
    """
    try:
        result = subprocess.run(
            [str(clang_format), '--version'],
            capture_output=True,
            check=True,
            text=True
        )
        print(f"Using: {result.stdout.strip()}")
    except (subprocess.CalledProcessError, FileNotFoundError):
        print(f"❌ Error: {clang_format} not found")
        print("devtool installs it into the venv from devtool_lib/requirements.txt: delete venv/ to recreate it")
        sys.exit(1)

    # Find all C/H files, excluding build directories and generated files
    print("🔍 Finding C and C++ files to format...")

    exclude_patterns = [
        'build-debug',
        'build-release',
        'venv',
        '.venv',
        '/build/',
        '.build',
        '.git',
        'zig-pkg',  # Zig fetches dependencies here (third-party sources)
        'vendor',  # vendored third-party packages (vendor/README.md), kept as published
        'zig-out',  # build outputs: installed copies of the headers
        '.zig-cache',
    ]

    files: List[Path] = []
    for pattern in ['**/*.c', '**/*.h', '**/*.cpp', '**/*.hpp']:
        for file_path in repo_root.glob(pattern):
            path_str = file_path.relative_to(repo_root).as_posix()
            if any(exclude in path_str for exclude in exclude_patterns):
                continue
            files.append(file_path)

    files.sort()

    if not files:
        print("No C or C++ files found to format")
        return

    print(f"📝 Found {len(files)} files to {'check' if check_only else 'format'}")

    if check_only:
        failed_files = []
        for file_path in files:
            result = subprocess.run(
                [str(clang_format), '--dry-run', '--Werror', str(file_path)],
                capture_output=True,
                check=False
            )
            if result.returncode != 0:
                failed_files.append(file_path)
                print(f"❌ File {file_path.relative_to(repo_root)} is not properly formatted")

        if failed_files:
            print("\n💡 To fix all formatting issues, run:")
            print("  ./devtool.py format")
            sys.exit(1)
        else:
            print("✅ All code is properly formatted")
    else:
        result = subprocess.run(
            [str(clang_format), '-i'] + [str(f) for f in files],
            check=True
        )
        print("✅ Code formatting complete")
