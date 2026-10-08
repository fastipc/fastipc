"""
Code formatting module for the C and C++ files (.c, .h, .cpp, .hpp) using clang-format-18
"""

import subprocess
import sys
from pathlib import Path
from typing import List


def format_code(repo_root: Path, check_only: bool = False):
    """Format all C and C++ files with clang-format-18

    Args:
        repo_root: Path to the repository root directory
        check_only: If True, only check formatting without modifying files
    """
    # Check if clang-format-18 is available
    try:
        result = subprocess.run(
            ['clang-format-18', '--version'],
            capture_output=True,
            check=True,
            text=True
        )
        print(f"Using: {result.stdout.strip()}")
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("❌ Error: clang-format-18 not found")
        print("Install with: sudo apt-get install clang-format-18")
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
            # Check if file should be excluded
            path_str = str(file_path.relative_to(repo_root))
            if any(exclude in path_str for exclude in exclude_patterns):
                continue
            files.append(file_path)

    files.sort()  # Sort for consistent ordering

    if not files:
        print("No C or C++ files found to format")
        return

    print(f"📝 Found {len(files)} files to {'check' if check_only else 'format'}")

    if check_only:
        # Check formatting without modifying files
        failed_files = []
        for file_path in files:
            result = subprocess.run(
                ['clang-format-18', '--dry-run', '--Werror', str(file_path)],
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
        # Format files in place
        result = subprocess.run(
            ['clang-format-18', '-i'] + [str(f) for f in files],
            check=True
        )
        print("✅ Code formatting complete")
