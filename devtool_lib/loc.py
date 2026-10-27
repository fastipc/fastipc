"""
Lines of code (`python devtool.py loc`): the library, the C++ wrapper, the tests, the harnesses and the benchmarks, in
Zig, C and C++.

One row per part, each split into blank, comment and code lines, then one per group:
- library: each file of src/zig up to its first test (a line starting with `test "`, `test {`, `// === Tests` or
  `const testing = `), less its test support (below), which is the code the library runs, and the C headers of include
  and src (the public API, include/fipc.h; the static library's test-hook header and the ABI checks' translate-c
  input);
- wrapper: the header-only C++ wrapper, include/fipc.hpp (it compiles into its users' programs, not the library), and
  the native Zig module's API, src/zig/root.zig up to its tests (Zig programs build it in; it isn't behind fipc.h);
- tests: the rest of those files (the unit tests), the test support before a file's tests, the test-only files of
  src/zig (the endpoint's, the stream's and the work queue's tests, the litmus tests' harness, the ring waits' lost-wake
  diagnostics, the OS contract's tests, macOS's system calls for the fork tests), the C-API suite (tests/zig)
  and the C++ wrapper's tests (tests/cpp);
- harnesses: the chaos peer (tests/chaos), the soak and the fuzz harnesses (tests/soak, tests/fuzz);
- bench: the benchmark suite (bench/zig), and the C and C++ benchmark (bench/c).

Test support is the code before a file's tests that never reaches the shipped library: a section whose `// === `
header names tests or test hooks, or compile-time checks (up to the next header); a `comptime { }` block that exports
nothing (the layout and ABI checks); the `test_hooks` and `internal` declarations; a statement or block under
`if (build_options.test_hooks)` or `builtin.is_test`; a `var test_...`. Test-only code elsewhere counts as library.

A Zig line is a comment when it starts with `//` (Zig has no block comments); a C or C++ line when it lies within
`/* */` or starts with `//`.
"""

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Tuple

# The files of src/zig that only the tests compile (the static library's test hooks among them).
TEST_ONLY = [
    "src/zig/core/endpoint_test.zig",
    "src/zig/core/litmus.zig",
    "src/zig/core/test_hooks.zig",
    "src/zig/stream/data_path_test.zig",
    "src/zig/stream/wake_diagnostics.zig",
    "src/zig/queue/producer_consumer_test.zig",
    "src/zig/queue/litmus_test.zig",
    "src/zig/platform/contract_test.zig",
    "src/zig/platform/macos/test_sys.zig",
]

# The native Zig module's API (the wrapper group), not the library behind fipc.h.
ZIG_MODULE = "src/zig/root.zig"

# Where a file's tests start: the lines before its first test are its code.
FIRST_TEST = re.compile(r'^(test ["{]|// === Tests|const testing = )')

# Test support before a file's tests (the module docstring).
SECTION = re.compile(r'^// === (.*) ===')
TEST_SECTION = re.compile(r'test|compile-time check', re.IGNORECASE)
COMPTIME_BLOCK = re.compile(r'^\s*comptime \{')
TEST_HOOK_DECL = re.compile(r'^pub const (test_hooks|internal) = ')
TEST_HOOK_STATEMENT = re.compile(r'\bif \((build_options\.test_hooks|builtin\.is_test)\)')
TEST_VAR = re.compile(r'^var test_\w+')


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


def _code_part(line: str) -> str:
    """The line without its string literals and `//` comment, for counting braces."""
    if line.lstrip().startswith("\\\\"):
        return ""
    line = re.sub(r'"(\\.|[^"\\])*"', '""', line)
    line = re.sub(r"'(\\.|[^'\\])*'", "''", line)
    return line.split("//")[0]


def _block_end(lines: List[str], start: int) -> int:
    """The index of the line that closes the braces opened from `start` on (`start` itself for a one-line statement)."""
    depth = 0
    opened = False
    for i in range(start, len(lines)):
        code = _code_part(lines[i])
        depth += code.count("{") - code.count("}")
        opened = opened or "{" in code
        if (opened and depth <= 0) or (not opened and code.rstrip().endswith(";")):
            return i
    return len(lines) - 1


def _test_support(lines: List[str]) -> List[bool]:
    """For each line before a file's tests, whether it is test support (the module docstring)."""
    support = [False] * len(lines)
    in_test_section = False
    i = 0
    while i < len(lines):
        line = lines[i]
        section = SECTION.match(line)
        if section:
            in_test_section = bool(TEST_SECTION.search(section.group(1)))
        if in_test_section or TEST_VAR.match(line):
            support[i] = True
        elif COMPTIME_BLOCK.match(line) or TEST_HOOK_DECL.match(line) or TEST_HOOK_STATEMENT.search(line):
            end = _block_end(lines, i)
            if not (COMPTIME_BLOCK.match(line) and any("@export" in l for l in lines[i:end + 1])):
                # A block's `} else ...` line belongs to the code after it
                last = end if _code_part(lines[end]).strip() == "}" or end == i else end - 1
                for j in range(i, last + 1):
                    support[j] = True
                i = end + 1 if last == end else end
                continue
        i += 1
    return support


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
    zig_module: List[Count] = []
    support: List[Count] = []
    unit_tests: List[Count] = []
    test_only: List[Count] = []
    for path in _files(repo_root, "src/zig", ["*.zig"]):
        lines = _read(path)
        if _is_test_only(repo_root, path):
            test_only.append(_zig_counts(lines))
            continue
        first_test = next((i for i, line in enumerate(lines) if FIRST_TEST.match(line)), len(lines))
        if first_test < len(lines):
            unit_tests.append(_zig_counts(lines[first_test:]))
        before = lines[:first_test]
        if path.relative_to(repo_root).as_posix() == ZIG_MODULE:
            zig_module.append(_zig_counts(before))
            continue
        is_support = _test_support(before)
        runtime.append(_zig_counts(l for l, s in zip(before, is_support) if not s))
        support.append(_zig_counts(l for l, s in zip(before, is_support) if s))

    rows: List[Tuple[str, str, Count]] = [
        ("library", "src/zig up to each file's tests", _sum(runtime)),
        ("library", "C headers (include, src)",
         _sum(_c_counts(_read(p)) for d in ("include", "src") for p in _files(repo_root, d, ["*.h"]))),
        ("wrapper", "C++, include/fipc.hpp", _sum(_c_counts(_read(p)) for p in _files(repo_root, "include", ["*.hpp"]))),
        ("wrapper", "Zig module, src/zig/root.zig", _sum(zig_module)),
        ("tests", "unit tests in src/zig", _sum(unit_tests)),
        ("tests", "test support in src/zig", _sum(support)),
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
