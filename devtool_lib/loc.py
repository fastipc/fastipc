"""
Lines of code (`python devtool.py loc`): the library, the C++ wrapper, the tests, the harnesses and the benchmarks, in
Zig, C and C++.

One row per part, each split into blank, comment and code lines, then one per group:
- library: each file of src/zig up to its first test (a line starting with `test "`, `test {`, `// === Tests` or
  `const testing = `), which is the code the library runs, and the C headers of include and src (the public API,
  include/fipc.h; the static library's test-hook header and the ABI checks' translate-c input);
- wrapper: the header-only C++ wrapper, include/fipc.hpp (it compiles into its users' programs, not the library);
- tests: the rest of those files (the unit tests), the test-only files of src/zig (the endpoint's tests, the static
  library's test hooks), the C-API suite (tests/zig) and the C++ wrapper's tests (tests/cpp);
- harnesses: the chaos peer (tests/chaos), the soak and the fuzz harnesses (tests/soak, tests/fuzz);
- bench: the benchmark suite (bench/zig), and the C and C++ benchmark (bench/c).

A Zig line is a comment when it starts with `//` (Zig has no block comments); a C or C++ line when it lies within
`/* */` or starts with `//`.
"""

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Tuple

# The files of src/zig that only the tests compile (the static library's test hooks among them).
TEST_ONLY = ["src/zig/lifecycle/endpoint_test.zig"]

# Where a file's tests start (the code tour's rule for "the lines before its first test").
FIRST_TEST = re.compile(r'^(test ["{]|// === Tests|const testing = )')


@dataclass
class Count:
    files: int = 0
    blank: int = 0
    comment: int = 0
    code: int = 0

    @property
    def total(self) -> int:
        return self.blank + self.comment + self.code

    def add(self, other: "Count") -> None:
        self.files += other.files
        self.blank += other.blank
        self.comment += other.comment
        self.code += other.code


def _zig_counts(lines: Iterable[str]) -> Count:
    count = Count(files=1)
    for line in lines:
        stripped = line.strip()
        if not stripped:
            count.blank += 1
        elif stripped.startswith("//"):
            count.comment += 1
        else:
            count.code += 1
    return count


def _c_counts(lines: Iterable[str]) -> Count:
    count = Count(files=1)
    in_comment = False
    for line in lines:
        category, in_comment = _categorize_c_line(line, in_comment)
        setattr(count, category, getattr(count, category) + 1)
    return count


def _categorize_c_line(line: str, in_comment: bool) -> Tuple[str, bool]:
    """'blank', 'comment' or 'code', and whether a /* */ comment is still open after the line."""
    stripped = line.strip()
    if not stripped:
        return "blank", in_comment
    pos = 0
    has_code = False
    while pos < len(stripped):
        if in_comment:
            end = stripped.find("*/", pos)
            if end == -1:
                break
            in_comment = False
            pos = end + 2
        elif stripped.startswith("//", pos):
            break
        elif stripped.startswith("/*", pos):
            in_comment = True
            pos += 2
        else:
            has_code = True
            pos += 1
    return ("code" if has_code else "comment"), in_comment


def _read(path: Path) -> List[str]:
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        return f.read().splitlines()


def _files(repo_root: Path, directory: str, patterns: Iterable[str]) -> List[Path]:
    root = repo_root / directory
    return sorted({p for pattern in patterns for p in root.rglob(pattern)}) if root.exists() else []


def _is_test_only(repo_root: Path, path: Path) -> bool:
    rel = path.relative_to(repo_root).as_posix()
    return any(rel == entry or (entry.endswith("/") and rel.startswith(entry)) for entry in TEST_ONLY)


def _sum(counts: Iterable[Count]) -> Count:
    total = Count()
    for count in counts:
        total.add(count)
    return total


def _whole(repo_root: Path, directories: Iterable[str]) -> Count:
    """Every Zig, C and C++ file of `directories`, whole."""
    parts = []
    for directory in directories:
        parts += [_zig_counts(_read(p)) for p in _files(repo_root, directory, ["*.zig"])]
        parts += [_c_counts(_read(p)) for p in _files(repo_root, directory, ["*.c", "*.h", "*.cpp", "*.hpp"])]
    return _sum(parts)


def count_loc(repo_root: Path) -> None:
    """Prints the table the module docstring describes."""
    runtime: List[Count] = []
    unit_tests: List[Count] = []
    test_only: List[Count] = []
    for path in _files(repo_root, "src/zig", ["*.zig"]):
        lines = _read(path)
        if _is_test_only(repo_root, path):
            test_only.append(_zig_counts(lines))
            continue
        first_test = next((i for i, line in enumerate(lines) if FIRST_TEST.match(line)), len(lines))
        runtime.append(_zig_counts(lines[:first_test]))
        if first_test < len(lines):
            unit_tests.append(_zig_counts(lines[first_test:]))

    rows: List[Tuple[str, str, Count]] = [
        ("library", "src/zig up to each file's tests", _sum(runtime)),
        ("library", "C headers (include, src)",
         _sum(_c_counts(_read(p)) for d in ("include", "src") for p in _files(repo_root, d, ["*.h"]))),
        ("wrapper", "C++, include/fipc.hpp", _sum(_c_counts(_read(p)) for p in _files(repo_root, "include", ["*.hpp"]))),
        ("tests", "unit tests in src/zig", _sum(unit_tests)),
        ("tests", "test-only files of src/zig", _sum(test_only)),
        ("tests", "C-API suite, tests/zig", _whole(repo_root, ["tests/zig"])),
        ("tests", "C++ wrapper's tests, tests/cpp", _whole(repo_root, ["tests/cpp"])),
        ("harnesses", "tests/chaos, soak, fuzz", _whole(repo_root, ["tests/chaos", "tests/soak", "tests/fuzz"])),
        ("bench", "bench/zig", _whole(repo_root, ["bench/zig"])),
        ("bench", "bench/c, C and C++", _whole(repo_root, ["bench/c"])),
    ]
    groups = [(group, "", _sum(c for g, _, c in rows if g == group)) for group in ("library", "wrapper", "tests", "harnesses", "bench")]

    def label(group: str, part: str) -> str:
        return f"{group}: {part}" if part else group

    width = max(len(label(g, p)) for g, p, _ in rows + groups)
    separator = f"  {'-' * width}  {'-' * 5}  {'-' * 6}  {'-' * 6}  {'-' * 7}  {'-' * 6}"

    def line(name: str, count: Count) -> str:
        return (f"  {name.ljust(width)}  {count.files:>5}  {count.total:>6}  {count.blank:>6}  {count.comment:>7}"
                f"  {count.code:>6}")

    print("📊 Lines of code: Zig, C and C++ (comments: `//` lines, and C's /* */ blocks)")
    print()
    print(f"  {'Part'.ljust(width)}  {'Files':>5}  {'Total':>6}  {'Blank':>6}  {'Comment':>7}  {'Code':>6}")
    print(separator)
    for group, part, count in rows:
        print(line(label(group, part), count))
    print(separator)
    for group, part, count in groups:
        print(line(label(group, part), count))
    print(separator)
    print(line("TOTAL", _sum(c for _, _, c in groups)))
