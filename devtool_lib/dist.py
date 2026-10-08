"""
Checks of the packaged shared libraries (`zig build dist`, ReleaseFast and stripped): each exports exactly the public API (the FIPC_API
functions of include/fipc.h), each Linux library is an ELF file for its architecture (x86-64, AArch64) that needs
no glibc symbol newer than the floor (docs/platform-support.md), and the macOS library is an arm64 dylib for macOS 14.4
(LC_BUILD_VERSION's minos), named @rpath/libfastipc.dylib (its install name), with a code signature (`codesign -v` checks it on a Mac). Runs in the devtool
venv (pyelftools, pefile; Mach-O is read by devtool_lib/exports.py). The CPU floor (x86-64-v3: AVX2, no AVX-512) is the build's target;
check it with `objdump -d <lib> | grep -c '%zmm'` (expect 0); ARM64 Linux's (generic ARMv8.0-A) is the build's target too.
"""

import argparse
import subprocess
import sys
from pathlib import Path
from typing import Optional

from devtool_lib import exports

GLIBC_FLOOR = (2, 34)
# rid -> the ELF machine (e_machine, as pyelftools names it) of each Linux library
LINUX_MACHINES = {"linux-x64": "EM_X86_64", "linux-arm64": "EM_AARCH64"}
MACOS_FLOOR = (14, 4, 0)  # the minimum macOS (os_sync_wait_on_address), the dylib's LC_BUILD_VERSION minos
MACOS_INSTALL_NAME = "@rpath/libfastipc.dylib"


def glibc_versions(library: Path) -> list:
    """The GLIBC_x.y[.z] versions the library's dynamic symbols require, sorted."""
    from elftools.elf.elffile import ELFFile
    from elftools.elf.gnuversions import GNUVerNeedSection

    versions = set()
    with open(library, "rb") as f:
        elf = ELFFile(f)
        for section in elf.iter_sections():
            if not isinstance(section, GNUVerNeedSection):
                continue
            for _, auxiliaries in section.iter_versions():
                for aux in auxiliaries:
                    name = aux.name
                    if name.startswith("GLIBC_") and name[6:7].isdigit():
                        versions.add(tuple(int(part) for part in name[6:].split(".")))
    return sorted(versions)


def elf_machine(library: Path) -> str:
    """The ELF file's e_machine, as pyelftools names it (EM_X86_64, EM_AARCH64)."""
    from elftools.elf.elffile import ELFFile

    with open(library, "rb") as f:
        return ELFFile(f)["e_machine"]


def check(dist_dir: Path, header: Path) -> bool:
    linux = dist_dir / "runtimes" / "linux-x64" / "native" / "libfastipc.so"
    windows = dist_dir / "runtimes" / "win-x64" / "native" / "fastipc.dll"
    macos = dist_dir / "runtimes" / "osx-arm64" / "native" / "libfastipc.dylib"
    linux_arm64 = dist_dir / "runtimes" / "linux-arm64" / "native" / "libfastipc.so"
    return check_libraries(header, linux=linux, windows=windows, macos=macos, linux_arm64=linux_arm64)


def check_libraries(header: Path, linux: Optional[Path] = None, windows: Optional[Path] = None,
                    macos: Optional[Path] = None, linux_arm64: Optional[Path] = None) -> bool:
    """The checks of the given libraries (`devtool package` checks the libraries it is given this way)."""
    ok = True
    for library in filter(None, (linux, windows, macos, linux_arm64)):
        if not library.exists():
            print(f"❌ {library} not found; run: zig build dist")
            ok = False
            continue
        ok = exports.check(library, header, exact=True) and ok

    for rid, library in (("linux-x64", linux), ("linux-arm64", linux_arm64)):
        if library is not None and library.exists():
            ok = check_linux(rid, library) and ok

    if macos is not None and macos.exists():
        ok = check_macos(macos) and ok
    return ok


def check_linux(rid: str, library: Path) -> bool:
    """A Linux library: an ELF file for the rid's architecture (the two Linux libraries share a name, so one given for
    the other would otherwise pass), that needs no glibc newer than the floor."""
    ok = True
    machine = elf_machine(library)
    if machine != LINUX_MACHINES[rid]:
        print(f"❌ {library.name} ({rid}): machine {machine}, expected {LINUX_MACHINES[rid]}")
        ok = False
    versions = glibc_versions(library)
    newest = versions[-1] if versions else None
    shown = ", ".join("GLIBC_" + ".".join(map(str, v)) for v in versions)
    floor = ".".join(map(str, GLIBC_FLOOR))
    if newest is not None and newest[:2] > GLIBC_FLOOR:
        print(f"❌ {library.name} ({rid}) needs glibc newer than {floor}: {shown}")
        ok = False
    elif ok:
        print(f"✅ {library.name} ({rid}): {machine}, needs glibc ≤ {floor} ({shown or 'no versioned symbols'})")
    return ok


def _version(version: tuple) -> str:
    """14.4 for (14, 4, 0), 14.4.1 for (14, 4, 1)."""
    return ".".join(map(str, version if version[2] else version[:2]))


def check_macos(library: Path) -> bool:
    """The dylib's target and identity: arm64, a dylib for macOS whose minimum is the floor (a lower one would claim
    systems it can't load on; a higher one would refuse the floor), the install name, and a valid signature (Apple
    Silicon runs only signed code; Zig's linker signs ad hoc): its load command on every host, `codesign -v` on a Mac."""
    try:
        macho = exports.read_macho(library)
    except ValueError as e:
        print(f"❌ {e}")
        return False
    floor = _version(MACOS_FLOOR)
    minos = _version(macho.minos) if macho.minos else "none"
    checks = [
        (macho.cpu == "arm64", f"arch {macho.cpu}, expected arm64"),
        (macho.filetype == exports.MH_DYLIB, f"file type {macho.filetype}, expected a dylib ({exports.MH_DYLIB})"),
        (macho.platform == exports.PLATFORM_MACOS, f"LC_BUILD_VERSION platform {macho.platform}, expected macOS"),
        (macho.minos == MACOS_FLOOR, f"LC_BUILD_VERSION minos {minos}, expected {floor}"),
        (macho.install_name == MACOS_INSTALL_NAME, f"install name {macho.install_name}, expected {MACOS_INSTALL_NAME}"),
        (macho.signed, "no code signature (LC_CODE_SIGNATURE)"),
    ]
    failed = [why for good, why in checks if not good]
    for why in failed:
        print(f"❌ {library.name}: {why}")
    if failed:
        return False
    print(f"✅ {library.name}: arm64, macOS minos {floor}, {MACOS_INSTALL_NAME}, signed")
    if sys.platform != "darwin":
        print(f"   {library.name}: the signature's validity is checked on a Mac (codesign -v)")
        return True
    result = subprocess.run(["codesign", "-v", "--verbose", str(library)], capture_output=True, text=True)
    if result.returncode != 0:
        print(f"❌ {library.name}: codesign -v: {(result.stderr or result.stdout).strip()}")
        return False
    print(f"✅ {library.name}: codesign -v: {result.stderr.strip().splitlines()[0] if result.stderr.strip() else 'valid'}")
    return True


def main(argv) -> int:
    sys.stdout.reconfigure(encoding="utf-8")  # the verdicts print emoji; redirected Windows output is cp1252
    parser = argparse.ArgumentParser(description="Check the packaged shared libraries in zig-out/dist")
    parser.add_argument("dist_dir", type=Path)
    parser.add_argument("header", type=Path, help="the public header (include/fipc.h)")
    parser.add_argument("--linux", type=Path, help="check this Linux library instead of the dist folder's")
    parser.add_argument("--windows", type=Path, help="check this Windows library instead of the dist folder's")
    parser.add_argument("--macos", type=Path, help="check this macOS library instead of the dist folder's")
    parser.add_argument("--linux-arm64", type=Path, help="check this ARM64 Linux library instead of the dist folder's")
    args = parser.parse_args(argv)
    if args.linux or args.windows or args.macos or args.linux_arm64:
        return 0 if check_libraries(args.header, linux=args.linux, windows=args.windows, macos=args.macos,
                                    linux_arm64=args.linux_arm64) else 1
    return 0 if check(args.dist_dir, args.header) else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
