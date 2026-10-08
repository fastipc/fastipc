"""
The wheel's tag follows the native library bundled in fipc/_native/ (CFFI's ABI mode loads it with dlopen, so
the wheel fits every Python 3): fastipc.dll makes a py3-none-win_amd64 wheel, libfastipc.so a
py3-none-manylinux_2_34_x86_64 one, or a py3-none-manylinux_2_34_aarch64 one if it is an AArch64 ELF file (glibc 2.34,
docs/platform-support.md), libfastipc.dylib a
py3-none-macosx_14_0_arm64 one (a macOS wheel tag carries the major version only; the library needs macOS 14.4);
without a library the wheel is pure.
The metadata is in pyproject.toml.
"""

from pathlib import Path

from setuptools import setup
from setuptools.command.bdist_wheel import bdist_wheel
from setuptools.dist import Distribution

NATIVE = Path(__file__).parent / "fipc" / "_native"
PLATFORMS = {"fastipc.dll": "win_amd64", "libfastipc.so": "manylinux_2_34_x86_64",
             "libfastipc.dylib": "macosx_14_0_arm64"}
LINUX_ARM64 = "manylinux_2_34_aarch64"  # libfastipc.so whose ELF machine is EM_AARCH64 (183)


def platform_tag(name):
    """The platform tag of _native/<name>: an ELF file's machine tells the two Linux libraries apart."""
    if name == "libfastipc.so":
        with open(NATIVE / name, "rb") as f:
            if int.from_bytes(f.read(20)[18:20], "little") == 183:
                return LINUX_ARM64
    return PLATFORMS[name]


def bundled_platform():
    """The platform tag of the one library in _native/, or None without one."""
    found = [platform_tag(name) for name in PLATFORMS if (NATIVE / name).exists()]
    if len(found) > 1:
        raise SystemExit(f"{NATIVE} holds libraries of several platforms; a wheel bundles one")
    return found[0] if found else None


class NativeDistribution(Distribution):
    """With a library bundled, the package is platform-specific: it installs into platlib, its wheel isn't pure."""

    def has_ext_modules(self):
        return bundled_platform() is not None


class PlatformWheel(bdist_wheel):
    def get_tag(self):
        platform_tag = bundled_platform()
        return ("py3", "none", platform_tag) if platform_tag else super().get_tag()


setup(distclass=NativeDistribution, cmdclass={"bdist_wheel": PlatformWheel})
