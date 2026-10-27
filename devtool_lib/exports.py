"""
Export check: compare the dynamic exports of the shipped library with the functions the public header declares
(`FIPC_API` in include/fipc.h, the one source of truth for the API).

  --subset  every declared function is exported
  --exact   exactly the declared functions are exported

Runs in the devtool venv, which has pyelftools (ELF) and pefile (PE); Mach-O (macOS) is read here (`read_macho`), so
every host checks every library:
  python -m devtool_lib.exports <library> <header> --subset|--exact
"""

import argparse
import re
import struct
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional, Set, Tuple

_COMMENT = re.compile(r"/\*.*?\*/|//[^\n]*", re.DOTALL)
_DIRECTIVE = re.compile(r"^\s*#.*$", re.MULTILINE)
_DECLARATION = re.compile(r"\bFIPC_API\b[^;{}]*?\b(fipc_\w+)\s*\(")


def read_expected(header: Path) -> Set[str]:
    """The functions `header` declares with FIPC_API (comments and preprocessor lines aside)."""
    text = _DIRECTIVE.sub("", _COMMENT.sub(" ", header.read_text(encoding="utf-8")))
    return set(_DECLARATION.findall(text))


# Exported by every dylib Zig's Mach-O linker makes, beside the library's own functions, with no build option that removes
# them: the linker-synthesized `__mh_dylib_header` (the image's Mach-O header) and `___dso_handle`, data symbols that
# Apple's ld keeps private. Named here (one leading underscore stripped, as for every Mach-O name) so that the exact
# check lets these two through on a Mach-O library alone, and prints them; nothing else extra passes.
MACHO_LINKER_EXPORTS = frozenset({"_mh_dylib_header", "__dso_handle"})


def is_macho(library: Path) -> bool:
    with open(library, "rb") as f:
        return f.read(4) in (_MH_MAGIC_64, _FAT_MAGIC, _FAT_MAGIC_64)


def dynamic_exports(library: Path) -> Set[str]:
    """Names the library exports: defined global/weak .dynsym symbols (ELF), named exports (PE), or the export trie's
    names without their leading underscore (Mach-O, the arm64 slice of a universal file)."""
    with open(library, "rb") as f:
        magic = f.read(4)
    if magic == b"\x7fELF":
        return _elf_exports(library)
    if magic[:2] == b"MZ":
        return _pe_exports(library)
    if magic in (_MH_MAGIC_64, _FAT_MAGIC, _FAT_MAGIC_64):
        return read_macho(library).exports
    raise ValueError(f"{library}: neither ELF, PE nor Mach-O")


def _elf_exports(library: Path) -> Set[str]:
    from elftools.elf.elffile import ELFFile

    with open(library, "rb") as f:
        dynsym = ELFFile(f).get_section_by_name(".dynsym")
        if dynsym is None:
            return set()
        return {
            sym.name
            for sym in dynsym.iter_symbols()
            if sym.name
            and sym["st_shndx"] != "SHN_UNDEF"
            and sym["st_info"]["bind"] in ("STB_GLOBAL", "STB_WEAK")
        }


def _pe_exports(library: Path) -> Set[str]:
    import pefile

    pe = pefile.PE(str(library), fast_load=True)
    try:
        pe.parse_data_directories(directories=[pefile.DIRECTORY_ENTRY["IMAGE_DIRECTORY_ENTRY_EXPORT"]])
        export_dir = getattr(pe, "DIRECTORY_ENTRY_EXPORT", None)
        if export_dir is None:
            return set()
        return {sym.name.decode() for sym in export_dir.symbols if sym.name}
    finally:
        pe.close()


# ===== Mach-O =====

_MH_MAGIC_64 = b"\xcf\xfa\xed\xfe"  # MH_MAGIC_64, little-endian: a thin 64-bit file
_FAT_MAGIC = b"\xca\xfe\xba\xbe"  # a universal file (big-endian headers), 32-bit offsets
_FAT_MAGIC_64 = b"\xca\xfe\xba\xbf"  # the same with 64-bit offsets
CPU_TYPE_ARM64 = 0x0100000C
_CPU_NAMES = {CPU_TYPE_ARM64: "arm64", 0x01000007: "x86_64"}
MH_DYLIB = 6
PLATFORM_MACOS = 1
_LC_ID_DYLIB = 0xD
_LC_CODE_SIGNATURE = 0x1D
_LC_DYLD_INFO = 0x22
_LC_DYLD_INFO_ONLY = 0x80000022
_LC_BUILD_VERSION = 0x32
_LC_DYLD_EXPORTS_TRIE = 0x80000033
_EXPORT_SYMBOL_FLAGS_REEXPORT = 0x08
_EXPORT_SYMBOL_FLAGS_STUB_AND_RESOLVER = 0x10


@dataclass
class MachO:
    """What the checks need from a Mach-O library: its CPU, file type, install name (LC_ID_DYLIB), platform and
    minimum OS version (LC_BUILD_VERSION, as (major, minor, patch)), whether it carries a code signature, and its
    exported names (the export trie, one leading underscore stripped)."""
    cpu: str
    filetype: int
    install_name: Optional[str] = None
    platform: Optional[int] = None
    minos: Optional[Tuple[int, int, int]] = None
    signed: bool = False
    exports: Set[str] = field(default_factory=set)


def read_macho(library: Path) -> MachO:
    """Reads a thin 64-bit little-endian Mach-O file, or the arm64 slice of a universal one (a universal file without
    one is refused)."""
    data = library.read_bytes()
    base = 0
    if data[:4] in (_FAT_MAGIC, _FAT_MAGIC_64):
        wide = data[:4] == _FAT_MAGIC_64
        (count,) = struct.unpack_from(">I", data, 4)
        slices: List[Tuple[int, int]] = []
        for i in range(count):
            if wide:
                cputype, _, offset, _, _, _ = struct.unpack_from(">iiQQII", data, 8 + 32 * i)
            else:
                cputype, _, offset, _, _ = struct.unpack_from(">iiIII", data, 8 + 20 * i)
            slices.append((cputype & 0xFFFFFFFF, offset))
        arm64 = [offset for cputype, offset in slices if cputype == CPU_TYPE_ARM64]
        if not arm64:
            names = ", ".join(_CPU_NAMES.get(cputype, hex(cputype)) for cputype, _ in slices)
            raise ValueError(f"{library}: a universal file without an arm64 slice ({names})")
        base = arm64[0]
    if data[base:base + 4] != _MH_MAGIC_64:
        raise ValueError(f"{library}: not a 64-bit little-endian Mach-O file")
    _, cputype, _, filetype, ncmds, _, _, _ = struct.unpack_from("<IiiIIIII", data, base)
    macho = MachO(cpu=_CPU_NAMES.get(cputype & 0xFFFFFFFF, hex(cputype & 0xFFFFFFFF)), filetype=filetype)
    trie: Optional[Tuple[int, int]] = None
    at = base + 32
    for _ in range(ncmds):
        cmd, size = struct.unpack_from("<II", data, at)
        if cmd == _LC_ID_DYLIB:
            (name_offset,) = struct.unpack_from("<I", data, at + 8)
            macho.install_name = data[at + name_offset:at + size].split(b"\0", 1)[0].decode()
        elif cmd == _LC_BUILD_VERSION:
            macho.platform, minos = struct.unpack_from("<II", data, at + 8)
            macho.minos = (minos >> 16, (minos >> 8) & 0xFF, minos & 0xFF)
        elif cmd == _LC_CODE_SIGNATURE:
            macho.signed = True
        elif cmd in (_LC_DYLD_INFO, _LC_DYLD_INFO_ONLY):
            offset, length = struct.unpack_from("<II", data, at + 40)
            if length:
                trie = (offset, length)
        elif cmd == _LC_DYLD_EXPORTS_TRIE:
            trie = struct.unpack_from("<II", data, at + 8)
        at += size
    if trie is not None:
        start = base + trie[0]
        macho.exports = {name[1:] if name.startswith("_") else name
                         for name in _trie_names(data[start:start + trie[1]])}
    return macho


def _uleb(data: bytes, at: int) -> Tuple[int, int]:
    value = shift = 0
    while True:
        byte = data[at]
        at += 1
        value |= (byte & 0x7F) << shift
        shift += 7
        if byte < 0x80:
            return value, at


def _trie_names(trie: bytes) -> List[str]:
    """The names of an export trie (dyld's format): each node holds its terminal information (size, then flags and
    the symbol's address, resolver or re-export), then its edges (a label and the child's offset)."""
    names: List[str] = []
    pending = [(0, b"")]
    seen = set()
    while pending:
        node, prefix = pending.pop()
        if node in seen or node >= len(trie):
            raise ValueError(f"malformed export trie (node at {node})")
        seen.add(node)
        terminal, at = _uleb(trie, node)
        if terminal:
            names.append(prefix.decode())
        at += terminal
        children = trie[at]
        at += 1
        for _ in range(children):
            end = trie.index(b"\0", at)
            label = trie[at:end]
            child, at = _uleb(trie, end + 1)
            pending.append((child, prefix + label))
    return sorted(names)


def check(library: Path, header: Path, exact: bool) -> bool:
    expected = read_expected(header)
    if not expected:
        print(f"❌ {header} declares no FIPC_API function")
        return False
    actual = dynamic_exports(library)
    missing = sorted(expected - actual)
    extra = sorted(actual - expected)
    allowed = sorted(name for name in extra if name in MACHO_LINKER_EXPORTS) if is_macho(library) else []
    extra = [name for name in extra if name not in allowed]

    print(f"Library: {library} ({len(actual)} exported symbols)")
    print(f"Expected: the FIPC_API functions of {header} ({len(expected)})")
    for name in allowed:
        print(f"   also: {name} (synthesized by Zig's Mach-O linker, MACHO_LINKER_EXPORTS)")
    for name in missing:
        print(f"❌ missing: {name}")
    if exact:
        for name in extra:
            print(f"❌ unexpected: {name}")

    if missing or (exact and extra):
        return False
    if exact:
        print(f"✅ Exports are exactly the {len(expected)} expected functions")
    else:
        print(f"✅ All {len(expected)} expected functions are exported ({len(extra)} other symbols)")
    return True


def main(argv) -> int:
    sys.stdout.reconfigure(encoding="utf-8")  # the verdicts print emoji; redirected Windows output is cp1252
    parser = argparse.ArgumentParser(description="Compare a library's dynamic exports with the public header")
    parser.add_argument("library", type=Path)
    parser.add_argument("header", type=Path, help="the public header (include/fipc.h)")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--subset", action="store_true", help="every declared function is exported")
    mode.add_argument("--exact", action="store_true", help="exactly the declared functions are exported")
    args = parser.parse_args(argv)
    return 0 if check(args.library, args.header, exact=args.exact) else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
