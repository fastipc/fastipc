#!/usr/bin/env python3
"""
devtool - Development workflow tool for FastIPC

This tool manages the entire development workflow including:
- Building the library, its tests and harnesses (delegates to zig build)
- Running tests and benchmarks
- Installing dependencies for Python and C#
- Code formatting
- Contract checks (frozen files, exported symbols)

Usage:
  devtool build [--release] [--windows|--linux]
  devtool test fast|slow|all [--filter TEXT]
  devtool test chaos [--scenario S] [--cycles N] [--seed S] [--jobs N] [--ready-ms T] [--observe-ms T] [--trace]
  devtool test soak [--seconds N] [--pin]
  devtool test fuzz [--rounds N] [--seed S]
  devtool test interop [--server L] [--client L] [--client-first sample|all|none] [--skip-missing]
  devtool bench <benchmark-name> [--quick]
  devtool bench --regex <pattern>
  devtool install [language]
  devtool format
  devtool check-frozen
  devtool check-exports --subset|--exact
  devtool bench-compare [benchmark ...] [--runs N] [--ab [--ref REV]]
  devtool bench-pace [--paces 1,2,4,...] [--rounds N] [--runs N]
  devtool loc
  devtool dist
  devtool package [python|csharp|java|rust|lua|js|go|all] [--linux-lib PATH] [--windows-lib PATH] [--macos-lib PATH]
                  [--linux-arm64-lib PATH] [--smoke] [--expect-version V]
  devtool smoke <.whl|.nupkg|.jar|maven folder|.crate|.src.rock|.tgz|Go module folder> ...
"""
# PYTHON_ARGCOMPLETE_OK

import argparse
import io
import json
import os
import platform
import shutil
import statistics
import subprocess
import sys
import tarfile
import tempfile
import time
from pathlib import Path
from typing import Dict, List, Optional
from enum import Enum

from devtool_lib import loc, clang_format, zig_format, go_format, frozen, chaos, package, perfgate, interop, throttle

try:
    import argcomplete
    ARGCOMPLETE_AVAILABLE = True
except ImportError:
    ARGCOMPLETE_AVAILABLE = False


ELF_MACHINE_AARCH64 = 183  # EM_AARCH64


def _elf_machine(library: Path) -> int:
    """An ELF file's e_machine (little-endian; the header's bytes 18-19)."""
    with open(library, "rb") as f:
        return int.from_bytes(f.read(20)[18:20], "little")


class ExecutionMode(Enum):
    """Execution mode determining where code runs"""
    LINUX_NATIVE = "linux_native"           # Running on Linux/WSL for Linux
    WINDOWS_FROM_WSL = "windows_from_wsl"   # Running on WSL, targeting Windows
    WINDOWS_NATIVE = "windows_native"       # Running on native Windows
    MACOS_NATIVE = "macos_native"           # Running on macOS for macOS


class ExecutionContext:
    """Cached execution context with platform-specific paths"""

    def __init__(self, mode: ExecutionMode, repo_root: Path, config: Dict):
        self.mode = mode
        self.repo_root = repo_root
        self.config = config

        # Cache all paths based on execution mode
        self._setup_paths()

    def _setup_paths(self):
        """Setup all paths based on execution mode"""
        if self.mode == ExecutionMode.LINUX_NATIVE:
            self._setup_linux_native()
        elif self.mode == ExecutionMode.WINDOWS_FROM_WSL:
            self._setup_windows_from_wsl()
        elif self.mode == ExecutionMode.WINDOWS_NATIVE:
            self._setup_windows_native()
        elif self.mode == ExecutionMode.MACOS_NATIVE:
            self._setup_macos_native()

    def _setup_linux_native(self):
        """Setup paths for Linux native execution"""
        self.python_exe = self.repo_root / "venv" / "bin" / "python"
        self.pip_exe = self.repo_root / "venv" / "bin" / "pip"
        self.dotnet_exe = "dotnet"
        self.bin_dir = self.repo_root / "zig-out" / "bin"
        self.lib_dir = self.repo_root / "zig-out" / "lib"
        self.work_dir = self.repo_root
        self.bin_ext = ""
        self.path_sep = ":"
        self.lib_env_var = "LD_LIBRARY_PATH"
        self.script_paths_win = None  # Not needed for Linux

    def _setup_windows_from_wsl(self):
        """Setup paths for Windows execution from WSL"""
        # Windows host executables (accessed from WSL)
        self.python_exe = Path(
            self.config.get("windows_host", {}).get(
                "python_exe", "/mnt/c/Python313/python.exe"
            )
        )
        self.dotnet_exe = Path(
            self.config.get("windows_host", {}).get(
                "dotnet_exe", "/mnt/c/Program Files/dotnet/dotnet.exe"
            )
        )

        # Windows sandbox paths (WSL-accessible)
        sandbox_root = Path(
            self.config.get("windows_host", {}).get(
                "sandbox_path", "/mnt/c/WSL2WindowsHostSandbox/fastipc"
            )
        )
        self.bin_dir = sandbox_root / "zig-out" / "bin"
        self.lib_dir = sandbox_root / "zig-out" / "bin"  # Windows DLLs in bin/
        self.work_dir = sandbox_root
        self.bin_ext = ".exe"
        self.path_sep = ";"
        self.lib_env_var = "PATH"

        # Use absolute Windows path for PYTHONPATH to ensure proper module discovery
        sandbox_win_path = str(sandbox_root).replace('/mnt/c/', 'C:\\').replace('/', '\\')
        self.script_paths_win = {
            "pythonpath": f"{sandbox_win_path}\\bindings\\python",  # Absolute Windows path
        }

    def _setup_windows_native(self):
        """Setup paths for Windows native execution"""
        self.python_exe = self.repo_root / "venv" / "Scripts" / "python.exe"
        self.pip_exe = self.repo_root / "venv" / "Scripts" / "pip.exe"
        self.dotnet_exe = "dotnet"
        self.bin_dir = self.repo_root / "zig-out" / "bin"
        self.lib_dir = self.repo_root / "zig-out" / "bin"  # Windows DLLs in bin/
        self.work_dir = self.repo_root
        self.bin_ext = ".exe"
        self.path_sep = ";"
        self.lib_env_var = "PATH"
        self.script_paths_win = None  # Not needed - working directly

    def _setup_macos_native(self):
        """Setup paths for macOS native execution"""
        self.python_exe = self.repo_root / "venv" / "bin" / "python"
        self.pip_exe = self.repo_root / "venv" / "bin" / "pip"
        self.dotnet_exe = "dotnet"
        self.bin_dir = self.repo_root / "zig-out" / "bin"
        self.lib_dir = self.repo_root / "zig-out" / "lib"
        self.work_dir = self.repo_root
        self.bin_ext = ""
        self.path_sep = ":"
        # Stripped for the system's protected programs (/bin/sh, /usr/bin/*): it reaches only programs started directly
        self.lib_env_var = "DYLD_LIBRARY_PATH"
        self.script_paths_win = None

    def get_binary_path(self, base_name: str) -> Path:
        """Get path to a binary executable"""
        return self.bin_dir / f"{base_name}{self.bin_ext}"

    def get_library_env(self) -> Dict[str, str]:
        """Get environment variables for library loading"""
        if self.mode == ExecutionMode.WINDOWS_FROM_WSL:
            # A Windows process started from WSL doesn't see this process's PATH. The Python benches
            # find fastipc.dll in the sandbox binding's _native/ (staged by the sync), the C# benches
            # next to their build output.
            return {}
        current = os.environ.get(self.lib_env_var, "")
        lib_path = str(self.lib_dir)
        return {self.lib_env_var: f"{lib_path}{self.path_sep}{current}" if current else lib_path}


class DevTool:
    """Development workflow manager for FastIPC"""

    def __init__(self, target_windows: bool = False, verbose: bool = False, loglevel: Optional[str] = None):
        self.repo_root = Path(__file__).parent.absolute()
        self.zig_out = self.repo_root / "zig-out"
        self.venv_dir = self.repo_root / "venv"
        self.is_wsl = self._detect_wsl()
        self.host_os = {"Windows": "windows", "Darwin": "macos"}.get(platform.system(), "linux")
        self.verbose = verbose
        self.loglevel = loglevel

        # bench-compare captures command output (last_output) instead of streaming it
        self.capture_output = False
        self.last_output = ""

        # Load configuration
        self.config = self._load_config()

        # Determine execution mode and cache execution context
        self.mode = self._determine_execution_mode(target_windows)
        self.ctx = ExecutionContext(self.mode, self.repo_root, self.config)

        # Venv is created lazily when needed (not during __init__)

    def _determine_execution_mode(self, target_windows: bool = False) -> ExecutionMode:
        """Determine execution mode based on host OS and target platform"""
        if target_windows and self.is_wsl:
            return ExecutionMode.WINDOWS_FROM_WSL
        elif self.host_os == "windows":
            return ExecutionMode.WINDOWS_NATIVE
        elif self.host_os == "macos":
            return ExecutionMode.MACOS_NATIVE
        else:
            return ExecutionMode.LINUX_NATIVE

    def _detect_wsl(self) -> bool:
        """Detect if running under WSL"""
        try:
            with open("/proc/version", "r") as f:
                return "microsoft" in f.read().lower()
        except:
            return False

    def _load_config(self) -> Dict:
        """Load configuration from devtool_config.json (with optional local override)"""
        config_file = self.repo_root / "devtool_config.json"
        local_config_file = self.repo_root / "devtool_config.local.json"

        # Load default config
        if not config_file.exists():
            return {"windows_host": {}, "sync_patterns": {}}

        with open(config_file, "r") as f:
            config = json.load(f)

        # Merge local overrides if they exist
        if local_config_file.exists():
            with open(local_config_file, "r") as f:
                local_config = json.load(f)
                # Deep merge (simple version - just update top-level keys)
                for key, value in local_config.items():
                    if key in config and isinstance(config[key], dict):
                        config[key].update(value)
                    else:
                        config[key] = value

        return config

    def _ensure_venv(self):
        """Ensure Python venv exists and has required packages"""
        if not self.venv_dir.exists():
            print(f"📦 Creating Python virtual environment at {self.venv_dir}...")
            python_cmd = "python" if self.host_os == "windows" else "python3"
            subprocess.run(
                [python_cmd, "-m", "venv", str(self.venv_dir)],
                check=True,
                cwd=self.repo_root,
            )
            print("✅ Virtual environment created")

            # Install required packages from requirements.txt if it exists
            requirements = self.repo_root / "bindings/python/requirements.txt"
            if requirements.exists():
                print("📦 Installing Python requirements...")
                venv_pip = self._get_venv_pip()
                subprocess.run(
                    [str(venv_pip), "install", "-r", str(requirements)],
                    check=True,
                    cwd=self.repo_root,
                )
                print("✅ Python requirements installed")

            # Install test requirements
            test_requirements = self.repo_root / "bindings/python/requirements-tests.txt"
            if test_requirements.exists():
                print("📦 Installing Python test requirements...")
                venv_pip = self._get_venv_pip()
                subprocess.run(
                    [str(venv_pip), "install", "-r", str(test_requirements)],
                    check=True,
                    cwd=self.repo_root,
                )
                print("✅ Python test requirements installed")

    def _ensure_devtool_requirements(self):
        """Ensure the venv has devtool's own dependencies (devtool_lib/requirements.txt)"""
        self._ensure_venv()
        requirements = self.repo_root / "devtool_lib" / "requirements.txt"
        sys.stdout.flush()  # keep this process's output ahead of pip's in piped logs
        subprocess.run(
            [str(self._get_venv_pip()), "install", "--quiet", "--disable-pip-version-check", "-r", str(requirements)],
            check=True,
            cwd=self.repo_root,
        )

    def _get_venv_pip(self) -> Path:
        """Get the path to the venv's pip executable"""
        if self.host_os == "windows":
            return self.venv_dir / "Scripts" / "pip.exe"
        else:
            return self.venv_dir / "bin" / "pip"

    def _get_venv_python(self) -> Path:
        """Get the path to the venv's python executable"""
        if self.host_os == "windows":
            return self.venv_dir / "Scripts" / "python.exe"
        else:
            return self.venv_dir / "bin" / "python"

    def _update_csharp_runtimes(self):
        """Copy native libraries to C# bindings runtimes/ folder"""
        # Source libraries from zig-out (actual names from build.zig)
        linux_src = self.repo_root / "zig-out/lib/libfastipc.so"
        windows_src = self.repo_root / "zig-out/bin/fastipc.dll"
        macos_src = self.repo_root / "zig-out/lib/libfastipc.dylib"

        # Destination in runtimes/ folder (must match P/Invoke library name)
        windows_dst = self.repo_root / "bindings/csharp/runtimes/win-x64/native/fastipc.dll"

        # Copy Linux library if it exists, into the folder of its architecture (an ARM64 build's goes to linux-arm64)
        if linux_src.exists():
            linux_rid = "linux-arm64" if _elf_machine(linux_src) == ELF_MACHINE_AARCH64 else "linux-x64"
            linux_dst = self.repo_root / f"bindings/csharp/runtimes/{linux_rid}/native/libfastipc.so"
            linux_dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(linux_src, linux_dst)
            if self.verbose:
                print(f"📋 Copied {linux_src.name} to runtimes/{linux_rid}/native/")

        # Copy Windows library if it exists
        if windows_src.exists():
            windows_dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(windows_src, windows_dst)
            if self.verbose:
                print(f"📋 Copied {windows_src.name} to runtimes/win-x64/native/")

        # Copy the macOS library if it exists
        if macos_src.exists():
            macos_dst = self.repo_root / "bindings/csharp/runtimes/osx-arm64/native/libfastipc.dylib"
            macos_dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(macos_src, macos_dst)
            if self.verbose:
                print(f"📋 Copied {macos_src.name} to runtimes/osx-arm64/native/")

    def _ensure_windows_sync(self):
        """Ensure code is synced to Windows sandbox when running in Windows mode"""
        if self.mode == ExecutionMode.WINDOWS_FROM_WSL:
            print("\n🔄 Syncing to Windows sandbox for benchmark execution...")
            self._sync_to_windows_sandbox()
            print()

    def _sync_to_windows_sandbox(self):
        """Sync only necessary source files and binaries to Windows sandbox"""
        if not self.is_wsl:
            print("Warning: Not running in WSL, skipping Windows sandbox sync")
            return

        sandbox_path = Path(self.config.get("windows_host", {}).get("sandbox_path", "/mnt/c/WSL2WindowsHostSandbox/fastipc"))

        print(f"📦 Syncing to Windows sandbox: {sandbox_path}")

        # Create sandbox directory if it doesn't exist
        sandbox_path.mkdir(parents=True, exist_ok=True)

        # Only sync Python/C# bindings and benchmarks (not all source files)
        paths_to_sync = [
            "bindings/python",              # Unified Python bindings location
            "bindings/csharp",              # Unified C# bindings location
            "bench/python",                 # Python benchmarks
            "bench/csharp",                 # C# benchmarks
        ]

        for path_str in paths_to_sync:
            src_path = self.repo_root / path_str
            dst_path = sandbox_path / path_str

            if src_path.exists():
                print(f"  Syncing: {path_str}")
                # Remove destination if it exists
                if dst_path.exists():
                    shutil.rmtree(dst_path)

                # Create parent directories
                dst_path.parent.mkdir(parents=True, exist_ok=True)

                # Copy the directory
                shutil.copytree(
                    src_path,
                    dst_path,
                    ignore=shutil.ignore_patterns(
                        "__pycache__", "*.pyc", "bin", "obj", "*.cache", "*.suo", "*.user"
                    ),
                )

        # Sync zig-out binaries (.exe and .dll files)
        zigout_src = self.repo_root / "zig-out" / "bin"
        zigout_dst = sandbox_path / "zig-out" / "bin"

        if zigout_src.exists():
            print(f"  Syncing binaries to: zig-out/bin/")
            zigout_dst.mkdir(parents=True, exist_ok=True)

            # Copy all .exe and .dll files
            for file in zigout_src.glob("*.exe"):
                shutil.copy2(file, zigout_dst / file.name)
            for file in zigout_src.glob("*.dll"):
                shutil.copy2(file, zigout_dst / file.name)

            # PATH doesn't reach Windows processes started from WSL; the binding finds the sandbox's zig-out/bin, and
            # the DLL is also staged in the sandbox copy of the binding's _native/ (as a wheel bundles it).
            dll = zigout_src / "fastipc.dll"
            if dll.exists():
                native_dir = sandbox_path / "bindings" / "python" / "fipc" / "_native"
                native_dir.mkdir(parents=True, exist_ok=True)
                shutil.copy2(dll, native_dir / dll.name)

        print("✅ Sync complete")

    def _run_command(
        self,
        cmd: List[str],
        env: Optional[dict] = None,
        cwd: Optional[Path] = None,
        check: bool = True,
        log_timing: bool = False,
        timing_label: Optional[str] = None,
    ) -> subprocess.CompletedProcess:
        """Run a command and handle errors"""
        if cwd is None:
            cwd = self.repo_root

        if not self.capture_output:
            if len(cmd) > 0 and not "clang-format" in cmd[0]:
                print(f"Running: {' '.join(cmd)}")
            print(f"CWD: {cwd}", flush=True)

        # Merge environment variables
        full_env = os.environ.copy()

        # Set LOG_LEVEL if loglevel is specified
        if self.loglevel:
            full_env['LOG_LEVEL'] = self.loglevel.lower()

        if env:
            full_env.update(env)
            if not self.capture_output:
                print(f"Additional ENV: {env}")

        if self.mode == ExecutionMode.WINDOWS_FROM_WSL:
            # A Windows process started from WSL only sees the variables listed in WSLENV
            names = [name for name in ("LOG_LEVEL", *(env or {})) if name in full_env]
            full_env["WSLENV"] = ":".join(filter(None, [os.environ.get("WSLENV"), *names]))

        start_time = time.time()
        if self.capture_output:
            result = subprocess.run(
                cmd, cwd=cwd, env=full_env, capture_output=True, text=True, encoding="utf-8", errors="replace"
            )
            self.last_output = result.stdout
            if result.returncode != 0:
                print(f"❌ {' '.join(cmd)} exited with {result.returncode}:\n{result.stderr[-2000:]}")
        else:
            result = subprocess.run(cmd, cwd=cwd, env=full_env, check=check)
        elapsed = time.time() - start_time

        if log_timing:
            label = timing_label or ' '.join(cmd)
            print(f"⏱️  {label}: {elapsed:.2f}s", flush=True)

        return result


    # ===== Build Commands =====

    def build(
        self,
        release: bool = False,
        windows: bool = False,
        linux: bool = False,
        extra_args: Optional[List[str]] = None,
    ):
        """Build the library (zig build)"""
        build_start = time.time()
        print(f"\n{'='*60}", flush=True)
        print("🔨 Starting build process...", flush=True)
        print(f"{'='*60}\n", flush=True)

        cmd = ["zig", "build"]

        # Add optimization flag
        if release:
            cmd.extend(["-Doptimize=fast"])

        # Add target platform
        if windows:
            cmd.extend(["-Dtarget=x86_64-windows-gnu"])
        elif linux and self.host_os != "linux":
            # Explicit Linux target when not on Linux
            cmd.extend(["-Dtarget=x86_64-linux-gnu"])

        # Add any extra arguments
        if extra_args:
            cmd.extend(extra_args)

        self._run_command(cmd, log_timing=True, timing_label="zig build")

        # Copy native libraries to C# bindings runtimes/ folder for dev builds and packaging
        self._update_csharp_runtimes()

        # Sync to Windows sandbox after building for Windows
        if windows and self.is_wsl:
            sync_start = time.time()
            self._sync_to_windows_sandbox()
            sync_elapsed = time.time() - sync_start
            print(f"⏱️  Windows sandbox sync: {sync_elapsed:.2f}s", flush=True)

        build_total = time.time() - build_start
        print(f"\n{'='*60}", flush=True)
        print(f"✅ Build completed in {build_total:.2f}s", flush=True)
        print(f"{'='*60}\n", flush=True)

    # ===== Test Commands =====

    # The Zig test tiers (tests/zig/README.md): each test has its own timeout inside the test binary (30 s by
    # default); --test-timeout bounds every test of a tier from the build runner, above the longest of them.
    ZIG_TIERS = {
        "fast": ("test", "60s"),  # the Zig unit tests and the single-process C-API tests
        "slow": ("test-slow", "330s"),  # the multi-process C-API tests (the soak test has 300 s)
    }

    def test_zig_tiers(self, tiers: List[str], test_filter: Optional[str] = None):
        """Run Zig test tiers one after the other (zig build test / test-slow), each with its duration. The fast tier
        first checks that the examples are what the docs show (devtool_lib/interop.py, check_docs)."""
        if self.mode == ExecutionMode.WINDOWS_FROM_WSL:
            print("Error: the Zig tests run natively; run them on Windows itself.")
            sys.exit(1)
        if "fast" in tiers and not interop.report_docs(self.repo_root):
            sys.exit(1)
        durations = []
        for tier in tiers:
            step, timeout = self.ZIG_TIERS[tier]
            cmd = ["zig", "build", step, "--summary", "all", "--test-timeout", timeout]
            if test_filter:
                cmd.append(f"-Dtest-filter={test_filter}")
            start = time.time()
            self._run_command(cmd, log_timing=True, timing_label=f"zig build {step}")
            durations.append((tier, time.time() - start))
        if len(durations) > 1:
            print("⏱️  " + ", ".join(f"{tier} tier: {secs:.1f}s" for tier, secs in durations), flush=True)

    def _build_harness_peer(self, what: str):
        """zig build harness: the chaos harness peer (tests/chaos) against this tree's static library"""
        if self.mode == ExecutionMode.WINDOWS_FROM_WSL:
            print(f"Error: the {what} runs natively; run it on Windows itself.")
            sys.exit(1)
        self._run_command(["zig", "build", "harness"], log_timing=True, timing_label="zig build harness")
        if self.loglevel:
            os.environ["LOG_LEVEL"] = self.loglevel.lower()  # inherited by the peers

    def test_chaos(self, scenario: Optional[str], cycles: Optional[int], seed: Optional[int],
                   ready_ms: int, observe_ms: int, trace: bool, jobs: int):
        """The chaos (kill-and-restart) harness (devtool_lib/chaos.py) on the chaos peer"""
        if scenario and scenario not in chaos.SCENARIOS:
            print(f"Error: unknown chaos scenario '{scenario}' (choose from: {', '.join(chaos.SCENARIOS)})")
            sys.exit(1)
        if jobs < 1:
            print("Error: --jobs must be at least 1")
            sys.exit(1)
        self._build_harness_peer("chaos harness")
        scenarios = [scenario] if scenario else list(chaos.SCENARIOS)
        if not chaos.run(self.ctx.bin_dir, self.ctx.bin_ext, scenarios, cycles, seed, ready_ms, observe_ms, trace,
                         jobs):
            sys.exit(1)

    def _run_harness_exe(self, step: str, name: str, extra_args: List[str], what: str,
                         prefix: Optional[List[str]] = None):
        """Builds `zig build <step>` and runs zig-out/bin/<name> (after `prefix`, e.g. taskset); exits 1 if the
        harness does."""
        if self.mode == ExecutionMode.WINDOWS_FROM_WSL:
            print(f"Error: the {what} runs natively; run it on Windows itself.")
            sys.exit(1)
        self._run_command(["zig", "build", step], log_timing=True, timing_label=f"zig build {step}")
        exe = self.ctx.bin_dir / f"{name}{self.ctx.bin_ext}"
        self._run_command([*(prefix or []), str(exe), *extra_args], log_timing=True, timing_label=name)

    def test_soak(self, seconds: Optional[int], pin: bool = False):
        """The soak harness (tests/soak): connection churn (listen, connect, accept, close) and ping-pong round
        trips, resources flat. `pin` (Linux) runs it on CPU 0 alone (`taskset -c 0`), so both
        ends of the connection and the library's threads share one CPU."""
        args = ["--seconds", str(seconds)] if seconds is not None else []
        prefix = None
        if pin:
            if self.host_os != "linux":
                print("Error: --pin (taskset -c 0) is Linux-only.")
                sys.exit(1)
            prefix = ["taskset", "-c", "0"]
        self._run_harness_exe("soak", "soak", args, "soak harness", prefix)

    def test_fuzz(self, rounds: Optional[int], seed: Optional[int]):
        """The corrupt-peer fuzz harness (tests/fuzz): the control-plane and the ring-content cases, both gated."""
        args: List[str] = []
        if rounds is not None:
            args += ["--rounds", str(rounds)]
        if seed is not None:
            args += ["--seed", str(seed)]
        self._run_harness_exe("fuzz", "fuzz", args, "fuzz harness")

    def test_interop(self, servers: List[str], clients: List[str], client_first: str, skip_missing: bool):
        """The cross-language matrix (devtool_lib/interop.py): every language's server example against every
        language's client example, in two processes"""
        if self.mode == ExecutionMode.WINDOWS_FROM_WSL:
            print("Error: the interop matrix runs natively; run it on Windows itself.")
            sys.exit(1)
        if not interop.report_docs(self.repo_root):
            sys.exit(1)
        self._ensure_venv()
        tools = interop.Toolchains.find(self.repo_root, self._get_venv_python(), str(self.ctx.dotnet_exe))
        if not interop.run(tools, servers, clients, client_first, skip_missing, self._update_csharp_runtimes):
            sys.exit(1)

    # ===== Performance Gate =====

    def bench_compare(self, benchmarks: List[str], runs: int):
        """Run the C# and Python benchmarks `runs` times (round-robin) and compare each test's median
        with its reference median (docs/perf/bindings-baseline.md)"""
        if self.host_os not in perfgate.BASELINE_COLUMNS or platform.machine().lower() in ("aarch64", "arm64"):
            print(f"Error: docs/perf/bindings-baseline.md has no {self.host_os} {platform.machine()} column to compare "
                  "with; compare the library with another revision's instead (bench-compare --ab)")
            sys.exit(1)
        baseline = perfgate.load_baseline(self.repo_root / "docs" / "perf" / "bindings-baseline.md", self.host_os)
        available = benchmark_map(self)
        benches = benchmarks or [bench for bench in dict.fromkeys(bench for bench, _ in baseline) if bench in available]
        unknown = [bench for bench in benches if bench not in available]
        if unknown:
            print(f"Error: unknown benchmark(s): {', '.join(unknown)}")
            sys.exit(1)
        not_gated = [bench for bench in benches if bench.startswith("zig-")]
        if not_gated:
            print(f"Error: the Zig suite has no baseline table; compare it with --ab: {', '.join(not_gated)}")
            sys.exit(1)

        print(f"Perf gate: {runs} run(s) of {len(benches)} benchmark(s) on {self.host_os}, round-robin")
        samples: Dict = {}
        self.capture_output = True
        try:
            for run in range(1, runs + 1):
                for bench in benches:
                    start = time.time()
                    self.last_output = ""
                    available[bench]()
                    rates = perfgate.parse_output(self.last_output)
                    for test, rate in rates.items():
                        samples.setdefault((bench, test), []).append(rate)
                    print(f"  run {run}/{runs}  {bench:<32} {len(rates)} results  {time.time() - start:5.1f} s", flush=True)
        finally:
            self.capture_output = False
        if not perfgate.report(samples, baseline, benches):
            sys.exit(1)

    def bench_compare_ab(self, benchmarks: List[str], runs: int, ref: str, duration: float):
        """A/B perf gate: the Zig suite against this tree's library and against the library of git
        revision `ref` (one that speaks include/fipc.h, as the suite does), alternating in one session so that
        machine drift affects both sides alike. By default, the gate cases of every scenario, in a quick run:
        rough numbers (about ±5%) in a few minutes per OS (docs/perf/baseline.md, "Method")."""
        if self.mode == ExecutionMode.WINDOWS_FROM_WSL:
            print("Error: --ab runs natively; run it on Windows itself.")
            sys.exit(1)
        targets = []
        for bench in benchmarks or [f"zig-{scenario}" for scenario in perfgate.GATE_SCENARIOS]:
            scenario = bench.removeprefix("zig-")
            if scenario == "all":
                targets += [(name, False) for name in perfgate.ZIG_SCENARIOS]
            elif scenario == "gates":
                targets += [(name, True) for name in perfgate.GATE_SCENARIOS]
            elif scenario in perfgate.ZIG_SCENARIOS:
                targets.append((scenario, not benchmarks))
            else:
                print(f"Error: --ab compares the Zig suite's scenarios (zig-*), not {bench}")
                sys.exit(1)

        binary = self._zig_bench_binary()
        new_lib = binary.parent / self._library_name()
        if not new_lib.exists():
            print(f"Error: {new_lib} not found. Run 'devtool.py build' first.")
            sys.exit(1)
        ref_lib = self._library_at(ref)
        sides = {
            "new": (new_lib, self._describe_revision("HEAD") + " (this tree)"),
            "ref": (ref_lib, f"{ref} ({self._short_commit(ref)})"),
        }

        print(f"A/B perf gate: {runs} round(s) of {len(targets)} scenario(s), {duration:g} s per case, "
              f"this tree vs {ref}, on {self.host_os}")
        started = time.time()
        samples: Dict = {"new": {}, "ref": {}}
        order: List = []

        def measure(rounds: range, chosen: List, scratch: str) -> None:
            for run in rounds:
                sides_order = ["new", "ref"] if run % 2 else ["ref", "new"]
                for scenario, gates in chosen:
                    for side in sides_order:
                        json_path = Path(scratch) / f"{side}-{run}-{scenario}.jsonl"
                        # The two sides' command lines differ in one letter: the library's copy in the scratch
                        # directory, `new/` or `ref/`. Their lengths place the heap (dlopen and the peer's argv
                        # copy the path), and the heap's placement moves 1 KiB copies by 10%.
                        cmd = [str(binary), scenario, "--runs", "1", "--duration", f"{duration:g}", "--warmup", "0.1",
                               "--lib", str(Path(scratch) / side / new_lib.name), "--json", str(json_path)]
                        if gates:
                            cmd.append("--gates")
                        start = time.time()
                        result = subprocess.run(cmd, cwd=self.repo_root, capture_output=True, text=True,
                                                encoding="utf-8", errors="replace")
                        records = perfgate.load_records(json_path)
                        if result.returncode != 0:
                            print(f"❌ {' '.join(cmd)} exited with {result.returncode}:\n"
                                  f"{result.stdout[-2000:]}{result.stderr[-2000:]}")
                        perfgate.add_records(samples[side], order, records)
                        print(f"  round {run}/{rounds.stop - 1}  {side}  {scenario:<30} {len(records)} cases  "
                              f"{time.time() - start:5.1f} s", flush=True)

        with tempfile.TemporaryDirectory(prefix="fipc_ab_") as scratch:
            for side, (lib, _) in sides.items():
                (Path(scratch) / side).mkdir()
                shutil.copy2(lib, Path(scratch) / side / new_lib.name)
            measure(range(1, runs + 1), targets, scratch)
            print(f"\nreference: {sides['ref'][1]}, {ref_lib}\nthis tree: {sides['new'][1]}, {new_lib}")
            failed: set = set()
            ok = perfgate.report_zig_ab(samples["new"], samples["ref"], order, failed)
            if not ok:
                # A quick run's noise alone can fail a case (up to 9% at 16-64 B, docs/perf/baseline.md): re-check
                # the failing scenarios once, with as many rounds again, and judge them on all their rounds
                retry = [(scenario, gates) for scenario, gates in targets if scenario in failed]
                print(f"\nRe-checking once: {', '.join(scenario for scenario, _ in retry)}")
                measure(range(runs + 1, 2 * runs + 1), retry, scratch)
                ok = perfgate.report_zig_ab(samples["new"], samples["ref"], order)
        print(f"\nwall time: {time.time() - started:.0f} s")
        if not ok:
            sys.exit(1)

    def bench_pace(self, paces: List[int], rounds: int, runs: int):
        """Tunes the reader's spin pace (`stream_pace`, src/zig/data/ring_wait.zig): builds the library once per pace
        with the build option, then runs the Zig suite's copy, zero-copy and RPC streams at 64 B, 1 KiB and 1.5 MiB
        and latency-spin against each, in alternating rounds. Prints each case's median of all runs and the range of
        the rounds' medians, per pace; the pace that keeps 1.5 MiB and latency-spin and is best at 1 KiB goes into
        `stream_paces`. Every library is a release build, as `build --release` makes; leaves zig-out built so,
        without the override."""
        if self.mode == ExecutionMode.WINDOWS_FROM_WSL:
            print("Error: bench-pace runs natively; run it on Windows itself.")
            sys.exit(1)
        binary = self._zig_bench_binary()
        lib_name = self._library_name()
        # One name length for every pace's directory: the library's path places the heap (bench_compare_ab)
        width = len(str(max(paces)))
        labels = {pace: f"p{pace:0{width}d}" for pace in paces}
        scenarios = [("fastipc", "64,1024,1572864"), ("fastipc-zerocopy", "64,1024,1572864"),
                     ("rpc", "64,1024,1572864"), ("latency-spin", None)]
        samples: Dict = {}
        order: List = []
        started = time.time()
        try:
            with tempfile.TemporaryDirectory(prefix="fipc_pace_") as scratch:
                for pace in paces:
                    self._run_command(["zig", "build", "-Doptimize=fast", f"-Dstream_pace={pace}"], log_timing=True,
                                      timing_label=f"zig build -Doptimize=fast -Dstream_pace={pace}")
                    (Path(scratch) / labels[pace]).mkdir()
                    shutil.copy2(binary.parent / lib_name, Path(scratch) / labels[pace] / lib_name)
                print(f"Pace sweep: {rounds} round(s) of paces {', '.join(map(str, paces))}, {runs} run(s) per case, "
                      f"on {self.host_os}")
                for run in range(rounds):
                    turn = paces[run % len(paces):] + paces[:run % len(paces)]
                    for pace in (turn[::-1] if run % 2 else turn):
                        for scenario, sizes in scenarios:
                            json_path = Path(scratch) / "run.jsonl"
                            json_path.unlink(missing_ok=True)
                            cmd = [str(binary), scenario, "--runs", str(runs), "--lib",
                                   str(Path(scratch) / labels[pace] / lib_name), "--json", str(json_path)]
                            if sizes:
                                cmd += ["--sizes", sizes]
                            result = subprocess.run(cmd, cwd=self.repo_root, capture_output=True, text=True,
                                                    encoding="utf-8", errors="replace")
                            if result.returncode != 0:
                                print(f"❌ {' '.join(cmd)} exited with {result.returncode}:\n"
                                      f"{result.stdout[-2000:]}{result.stderr[-2000:]}")
                            for record in perfgate.load_records(json_path):
                                for metric in ("msgs_per_s", "p50_ns", "p99_ns"):
                                    if metric not in record["metrics"]:
                                        continue
                                    key = (scenario, record["case"], metric)
                                    if key not in order:
                                        order.append(key)
                                    samples.setdefault(key, {}).setdefault(pace, []).append(
                                        record["metrics"][metric]["values"])
                        print(f"  round {run + 1}/{rounds}  pace {pace:<5} {time.time() - started:6.0f} s", flush=True)
        finally:
            self._run_command(["zig", "build", "-Doptimize=fast"], log_timing=True,
                              timing_label="zig build -Doptimize=fast (no override)")

        def show(metric: str, value: float) -> str:
            return perfgate._format_value(metric, value)

        print(f"\n{'scenario':<18} {'case':<9} {'metric':<11}" + "".join(f"{f'pace {p}':>25}" for p in paces))
        for key in order:
            scenario, case, metric = key
            row = f"{scenario:<18} {case:<9} {metric:<11}"
            for pace in paces:
                per_round = samples[key].get(pace, [])
                if not per_round:
                    row += f"{'-':>25}"
                    continue
                medians = [statistics.median(values) for values in per_round]
                cell = (f"{show(metric, statistics.median([v for values in per_round for v in values]))} "
                        f"[{show(metric, min(medians))}-{show(metric, max(medians))}]")
                row += f"{cell:>25}"
            print(row)
        print(f"\nmedian of all runs [range of the rounds' medians]; wall time: {time.time() - started:.0f} s")

    def _library_name(self) -> str:
        return {"windows": "fastipc.dll", "macos": "libfastipc.dylib"}.get(self.host_os, "libfastipc.so")

    def _describe_revision(self, rev: str) -> str:
        cmd = ["git", "describe", "--always"] + (["--dirty"] if rev == "HEAD" else [rev])
        result = subprocess.run(cmd, cwd=self.repo_root, capture_output=True, text=True)
        return result.stdout.strip() or rev

    def _short_commit(self, rev: str) -> str:
        result = subprocess.run(["git", "rev-parse", "--short=12", f"{rev}^{{commit}}"], cwd=self.repo_root,
                                capture_output=True, text=True)
        return result.stdout.strip() or rev

    def _library_at(self, ref: str) -> Path:
        """The shared library of git revision `ref`, built ReleaseFast by that revision's own build.zig
        and cached by commit in .bench-libs/. The revision must speak the suite's API, include/fipc.h: an older
        one's library exports other functions."""
        probe = subprocess.run(["git", "rev-parse", "--verify", "--quiet", f"{ref}^{{commit}}"],
                               cwd=self.repo_root, capture_output=True, text=True)
        if probe.returncode != 0:
            print(f"Error: git revision '{ref}' not found; fetch it (a tag: git fetch origin tag {ref})")
            sys.exit(1)
        commit = probe.stdout.strip()
        header = subprocess.run(["git", "show", f"{commit}:include/fipc.h"], cwd=self.repo_root,
                                capture_output=True, text=True, encoding="utf-8", errors="replace")
        if header.returncode != 0 or "fipc_listen(" not in header.stdout:
            print(f"Error: {ref} ({commit[:12]}) doesn't speak include/fipc.h, the benchmarks' API; "
                  "compare with a later revision")
            sys.exit(1)
        cache = self.repo_root / ".bench-libs" / commit
        library = cache / self._library_name()
        if library.exists():
            return library

        source = self.repo_root / ".bench-libs" / f"{commit}-src"
        if source.exists():
            shutil.rmtree(source)
        print(f"Building the library of {ref} ({commit[:12]}) in {source} ...", flush=True)
        archive = subprocess.run(["git", "archive", "--format=tar", commit], cwd=self.repo_root,
                                 capture_output=True, check=True).stdout
        with tarfile.open(fileobj=io.BytesIO(archive)) as tar:
            tar.extractall(source, filter="data")
        self._run_command(["zig", "build", "-Doptimize=ReleaseFast"], cwd=source,
                          log_timing=True, timing_label=f"zig build ({ref})")
        built = source / "zig-out" / ("bin" if self.host_os == "windows" else "lib") / self._library_name()
        cache.mkdir(parents=True, exist_ok=True)
        shutil.copy2(built.resolve(), library)
        shutil.rmtree(source)
        return library

    # ===== Contract Checks =====

    def check_exports(self, exact: bool):
        """Compare the shared library's dynamic exports with include/fipc.h's FIPC_API functions"""
        library = self.ctx.lib_dir / self._library_name()
        if not library.exists():
            print(f"Error: {library} not found. Run 'devtool.py build' first.")
            sys.exit(1)

        # pyelftools and pefile live in the venv, so the comparison runs there
        self._ensure_devtool_requirements()
        result = subprocess.run(
            [
                str(self._get_venv_python()),
                "-m",
                "devtool_lib.exports",
                str(library),
                str(self.repo_root / "include" / "fipc.h"),
                "--exact" if exact else "--subset",
            ],
            cwd=self.repo_root,
        )
        if result.returncode != 0:
            sys.exit(result.returncode)

    def dist(self) -> Path:
        """Build the packaged shared libraries (`zig build dist`) and check them; returns zig-out/dist"""
        self._run_command(["zig", "build", "dist"], log_timing=True, timing_label="zig build dist")
        dist_dir = self.repo_root / "zig-out" / "dist"
        self._ensure_devtool_requirements()
        result = subprocess.run(
            [
                str(self._get_venv_python()),
                "-m",
                "devtool_lib.dist",
                str(dist_dir),
                str(self.repo_root / "include" / "fipc.h"),
            ],
            cwd=self.repo_root,
        )
        if result.returncode != 0:
            sys.exit(result.returncode)
        return dist_dir

    # ===== Benchmark Commands =====

    def _zig_bench_binary(self) -> Path:
        """zig-out/bench/fipc_bench, the Zig suite (built by every `zig build`)"""
        binary = self.repo_root / "zig-out" / "bench" / f"fipc_bench{self.ctx.bin_ext}"
        if self.mode == ExecutionMode.WINDOWS_FROM_WSL:
            print("Error: the Zig suite runs natively; run it on Windows itself.")
            sys.exit(1)
        if not binary.exists():
            print(f"Error: {binary} not found. Run 'devtool.py build' first.")
            sys.exit(1)
        return binary

    def bench_zig(self, target: str, quick: bool = False):
        """Run the Zig suite (bench/zig): one scenario, `all` or `gates`; `quick` is CI's smoke run"""
        self._run_command([str(self._zig_bench_binary()), target] + (["--quick"] if quick else []))

    def bench_c(self, program: str, mode: str):
        """The C or C++ benchmark (bench/c) in `mode`: copy, zerocopy or rpc. `program` is c_bench (the loops through
        the C API) or cpp_bench (through the C++ wrapper), which every `zig build` installs in zig-out/bench, ReleaseFast,
        next to the library it links"""
        if self.mode == ExecutionMode.WINDOWS_FROM_WSL:
            print("Error: the C and C++ benchmark runs natively; run it on Windows itself.")
            sys.exit(1)
        binary = self.repo_root / "zig-out" / "bench" / f"{program}{self.ctx.bin_ext}"
        if not binary.exists():
            print(f"Error: {binary} not found. Run 'devtool.py build' first.")
            sys.exit(1)
        self._run_command([str(binary), mode], check=False)

    def bench_zig_api(self, mode: str):
        """The Zig benchmark (bench/zig-api, through the native module) in `mode`: copy, zerocopy or rpc. Every `zig
        build` installs it in zig-out/bench, ReleaseFast."""
        if self.mode == ExecutionMode.WINDOWS_FROM_WSL:
            print("Error: the Zig benchmark runs natively; run it on Windows itself.")
            sys.exit(1)
        binary = self.repo_root / "zig-out" / "bench" / f"zig_bench{self.ctx.bin_ext}"
        if not binary.exists():
            print(f"Error: {binary} not found. Run 'devtool.py build' first.")
            sys.exit(1)
        self._run_command([str(binary), mode], check=False)

    def _bench_library_dir(self) -> Path:
        """zig-out/bench, the folder of the library every benchmark measures: this tree's, built ReleaseFast by every
        `zig build` whatever its mode (zig-out/bin and zig-out/lib hold the build's mode: Debug after a test run).
        The JavaScript addon beside it is ReleaseFast too. The bindings find it as FASTIPC_LIB_DIR."""
        folder = self.zig_out / "bench"
        if not (folder / self._library_name()).exists():
            print(f"Error: {folder / self._library_name()} not found. Run 'devtool.py build' first.")
            sys.exit(1)
        return folder

    def bench_python(self, mode: str):
        """The Python benchmark (bench/python/fipc_bench.py) in `mode`: copy, zerocopy or rpc, against
        zig-out/bench's library (FASTIPC_LIB_DIR)"""
        self._ensure_windows_sync()
        self._ensure_venv()
        env = self.ctx.get_library_env()
        env["PYTHONIOENCODING"] = "utf-8"
        if self.mode == ExecutionMode.WINDOWS_FROM_WSL:
            script = "bench\\python\\fipc_bench.py"
            env["PYTHONPATH"] = self.ctx.script_paths_win["pythonpath"]
        else:
            script = str(self.ctx.work_dir / "bench/python/fipc_bench.py")
            env["PYTHONPATH"] = str(self.ctx.work_dir / "bindings/python")
            env["FASTIPC_LIB_DIR"] = str(self._bench_library_dir())
        self._run_command(
            [str(self.ctx.python_exe), script, mode],
            env=env,
            cwd=self.ctx.work_dir if self.mode == ExecutionMode.WINDOWS_FROM_WSL else None,
        )

    def bench_csharp(self, mode: str):
        """The C# benchmark (bench/csharp/FastIpc.Bench) in `mode`: copy, zerocopy or rpc (builds it if needed), against
        zig-out/bench's library: a fresh copy in zig-out/bench/csharp/<rid>/native/ is the binding's FipcRuntimesDir,
        newer than the build output's copy, which MSBuild then replaces (PreserveNewest; Windows loads the one next to
        the executable), and zig-out/bench comes first on the library search path (Linux loads through it)"""
        self._ensure_windows_sync()
        env = self.ctx.get_library_env()
        properties = []
        if self.mode == ExecutionMode.WINDOWS_FROM_WSL:
            csproj = "bench\\csharp\\FastIpc.Bench\\FastIpc.Bench.csproj"
        else:
            csproj = str(self.ctx.work_dir / "bench/csharp/FastIpc.Bench/FastIpc.Bench.csproj")
            arm64 = platform.machine().lower() in ("aarch64", "arm64")
            rid = {"windows": "win-x64", "macos": "osx-arm64"}.get(self.host_os, "linux-arm64" if arm64 else "linux-x64")
            runtimes = self.zig_out / "bench" / "csharp"
            native = runtimes / rid / "native"
            native.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(self._bench_library_dir() / self._library_name(), native / self._library_name())
            properties = [f"--property:FipcRuntimesDir={runtimes}"]
            current = os.environ.get(self.ctx.lib_env_var, "")
            folder = str(self._bench_library_dir())
            env[self.ctx.lib_env_var] = f"{folder}{self.ctx.path_sep}{current}" if current else folder
        self._run_command(
            [str(self.ctx.dotnet_exe), "run", "--project", csproj, "--configuration", "Release", *properties, "--",
             mode],
            env=env,
            cwd=self.ctx.work_dir if self.mode == ExecutionMode.WINDOWS_FROM_WSL else None,
            check=False,
        )

    def bench_java(self, mode: str):
        """The Java benchmark (bench/java, through bindings/java) in `mode`: copy, zerocopy or rpc. Gradle builds it
        (the binding's wrapper; the toolchain JDK) and runs it against zig-out/bench's library (the system property
        fastipc.library.path)."""
        if self.mode == ExecutionMode.WINDOWS_FROM_WSL:
            print("Error: the Java benchmark runs natively; run it on Windows itself.")
            sys.exit(1)
        library = self._bench_library_dir() / self._library_name()
        self._run_command(
            [str(package.gradlew(self.repo_root)), "-p", str(self.repo_root / "bench" / "java"), "--no-daemon",
             "--console=plain", "--quiet", "run", f"-PfipcLibrary={library}", f"--args={mode}"],
            check=False,
        )

    def bench_rust(self, mode: str):
        """The Rust benchmark (bench/rust, through bindings/rust) in `mode`: copy, zerocopy or rpc. Cargo builds it
        (release, the current stable toolchain) against zig-out/bench's library (FASTIPC_LIB_DIR), and it runs with
        that folder first on the library search path, not through `cargo run`: fipc-sys's copy of the library is
        renewed only when the file's time is newer than its last build, and a library Zig takes from its cache keeps
        its old time."""
        if self.mode == ExecutionMode.WINDOWS_FROM_WSL:
            print("Error: the Rust benchmark runs natively; run it on Windows itself.")
            sys.exit(1)
        folder = self._bench_library_dir()
        manifest = self.repo_root / "bench" / "rust" / "Cargo.toml"
        self._run_command(
            ["cargo", "build", "--release", "--quiet", "--manifest-path", str(manifest)],
            env={"RUSTUP_TOOLCHAIN": os.environ.get("RUSTUP_TOOLCHAIN", "stable"), "FASTIPC_LIB_DIR": str(folder)},
        )
        target = Path(os.environ.get("CARGO_TARGET_DIR") or manifest.parent / "target")
        current = os.environ.get(self.ctx.lib_env_var, "")
        self._run_command(
            [str(target / "release" / f"fipc-bench{self.ctx.bin_ext}"), mode],
            env={self.ctx.lib_env_var: f"{folder}{self.ctx.path_sep}{current}" if current else str(folder)},
            check=False,
        )

    def bench_lua(self, mode: str):
        """The Lua benchmark (bench/lua/fipc_bench.lua, through bindings/lua) in `mode`: copy, zerocopy or rpc, on
        LuaJIT (package.luajit(): LUAJIT, PATH or ~/luajit) against zig-out/bench's library (FASTIPC_LIB_DIR)."""
        if self.mode == ExecutionMode.WINDOWS_FROM_WSL:
            print("Error: the Lua benchmark runs natively; run it on Windows itself.")
            sys.exit(1)
        self._run_command([package.luajit(), str(self.repo_root / "bench" / "lua" / "fipc_bench.lua"), mode],
                          env={"FASTIPC_LIB_DIR": str(self._bench_library_dir())}, check=False)

    def bench_js(self, mode: str):
        """The JavaScript benchmark (bench/js/fipc_bench.mjs, through bindings/js) in `mode`: copy, zerocopy or rpc,
        on Node.js (package.node(): NODE or PATH) against zig-out/bench's library and addon (FASTIPC_LIB_DIR). Bun and
        Deno run the same script by hand (bun bench/js/fipc_bench.mjs, deno run -A ...)."""
        if self.mode == ExecutionMode.WINDOWS_FROM_WSL:
            print("Error: the JavaScript benchmark runs natively; run it on Windows itself.")
            sys.exit(1)
        self._run_command([package.node(), str(self.repo_root / "bench" / "js" / "fipc_bench.mjs"), mode],
                          env={"FASTIPC_LIB_DIR": str(self._bench_library_dir())}, check=False)

    def bench_go(self, mode: str):
        """The Go benchmark (bench/go, through bindings/go) in `mode`: copy, zerocopy or rpc. The go command
        (package.go(): GO, PATH or ~/go-sdk) builds it into zig-out/go/bench, without cgo, and it runs against
        zig-out/bench's library (FASTIPC_LIB_DIR)."""
        if self.mode == ExecutionMode.WINDOWS_FROM_WSL:
            print("Error: the Go benchmark runs natively; run it on Windows itself.")
            sys.exit(1)
        binary = self.zig_out / "go" / "bench" / f"fipc_bench{self.ctx.bin_ext}"
        self._run_command([package.go(), "build", "-o", str(binary), "."], env=package.go_env(),
                          cwd=self.repo_root / "bench" / "go")
        self._run_command([str(binary), mode], env={"FASTIPC_LIB_DIR": str(self._bench_library_dir())}, check=False)

    # ===== Install Commands =====

    def install_python(self):
        """Install/reinstall Python dependencies"""
        # Ensure venv exists before installing Python dependencies
        self._ensure_venv()

        print("Reinstalling Python dependencies...")

        requirements = (
            self.repo_root
            / "bindings/python/requirements.txt"
        )
        if requirements.exists():
            print("Installing Python requirements...")
            venv_pip = self._get_venv_pip()
            self._run_command([str(venv_pip), "install", "-r", str(requirements)])

        test_requirements = (
            self.repo_root
            / "bindings/python/requirements-tests.txt"
        )
        if test_requirements.exists():
            print("Installing Python test requirements...")
            venv_pip = self._get_venv_pip()
            self._run_command([str(venv_pip), "install", "-r", str(test_requirements)])

        print("✅ Python dependencies installed successfully")

    def install_csharp(self):
        """Install C# dependencies: build the bindings and the benchmark (dotnet restores them)"""
        for csproj in ("bindings/csharp/Fipc.csproj", "bench/csharp/FastIpc.Bench/FastIpc.Bench.csproj"):
            print(f"Building {csproj}...")
            self._run_command([str(self.ctx.dotnet_exe), "build", "--configuration", "Release",
                               str(self.repo_root / csproj)])
        print("✅ C# dependencies installed successfully")

    def install_all(self):
        """Install all dependencies (Python + C#)"""
        print("Installing all dependencies...")
        self.install_python()
        self.install_csharp()
        print("✅ All dependencies installed successfully")

    def package(self, what: str, linux_lib: Optional[Path], windows_lib: Optional[Path], macos_lib: Optional[Path],
                linux_arm64_lib: Optional[Path], smoke: bool, expect_version: Optional[str]) -> None:
        """The packages in zig-out/packages, checked (devtool_lib/package.py): `python`, a wheel per library (this
        OS's alone when none is given), `csharp`, the NuGet package with every platform's library; `java`, the Maven
        artifact with every platform's, in zig-out/packages/maven (a folder in the Maven repository layout); `rust`,
        the two crates, fipc-sys with every platform's (and in zig-out/packages/rust the workspace they were
        packaged from); `lua`, the rock (its rockspec, its source rock and the archive in it, with every platform's);
        `js`, the npm package (every platform's library and Node-API addon, the addons from `zig build dist`); `go`,
        the Go module's tree as its release tag holds it (every platform's library in fipc/native/<rid>/), in
        zig-out/packages/go; `all`, every kind. Without --linux-lib/--windows-lib/--macos-lib/--linux-arm64-lib the libraries are `zig build
        dist`'s (cross-compiled); given ones are checked as the dist ones are (a platform no package carries is
        refused), and csharp, java, rust, lua, js and go need all four. `smoke` installs this OS's packages and makes a round trip through
        them."""
        version = package.check_versions(self.repo_root, expect_version)
        header = self.repo_root / "include" / "fipc.h"
        # rid -> the given library, and devtool_lib/dist.py's option that checks it
        options = {"linux-x64": "--linux", "win-x64": "--windows", "osx-arm64": "--macos",
                   "linux-arm64": "--linux-arm64"}
        given = {rid: lib for rid, lib in (("linux-x64", linux_lib), ("win-x64", windows_lib), ("osx-arm64", macos_lib),
                                           ("linux-arm64", linux_arm64_lib))
                 if lib is not None}
        unpackaged = [rid for rid in given if rid not in package.PLATFORMS]
        if unpackaged:
            print(f"❌ no package carries the {', '.join(unpackaged)} library")
            sys.exit(1)
        if given:
            self._ensure_devtool_requirements()
            check = [str(self._get_venv_python()), "-m", "devtool_lib.dist", "-", str(header)]
            for rid, lib in given.items():
                check += [options[rid], str(Path(lib).resolve())]
            if subprocess.run(check, cwd=self.repo_root).returncode != 0:
                sys.exit(1)
            libraries = {rid: Path(lib).resolve() for rid, lib in given.items()}
            wheel_rids = list(libraries)
        else:
            runtimes = self.dist() / "runtimes"
            libraries = {rid: runtimes / rid / "native" / name for rid, (name, _) in package.PLATFORMS.items()}
            wheel_rids = [package.host_rid()]

        missing = [rid for rid in package.PLATFORMS if rid not in libraries]
        if what in ("csharp", "java", "rust", "lua", "js", "go", "all") and missing:
            kind = {"csharp": "the NuGet package", "java": "the jar", "rust": "the fipc-sys crate",
                    "lua": "the rock", "js": "the npm package", "go": "the Go module"}.get(
                what, "the NuGet package, the jar, the fipc-sys crate, the rock, the npm package and the Go module")
            print(f"❌ {kind} carries every platform's library; give the {', '.join(missing)} one too")
            sys.exit(1)

        out_dir = self.zig_out / "packages"
        work = out_dir / "work"
        if out_dir.exists():
            shutil.rmtree(out_dir)
        work.mkdir(parents=True)
        ok = True

        if what in ("python", "all"):
            self._ensure_venv()
            license_text = (self.repo_root / "LICENSE").read_bytes()
            for rid in wheel_rids:
                wheel = package.build_wheel(self.repo_root, self._get_venv_python(), rid, libraries[rid], out_dir,
                                            work)
                ok = package.check_wheel(wheel, rid, version, license_text) and ok
                if smoke and ok and rid == package.host_rid():
                    package.smoke_wheel(wheel)

        if what in ("csharp", "all"):
            nupkg = package.build_nupkg(self.repo_root, str(self.ctx.dotnet_exe), libraries, out_dir, work)
            ok = package.check_nupkg(nupkg, version, libraries) and ok
            if smoke and ok:
                package.smoke_nupkg(nupkg, version, str(self.ctx.dotnet_exe))

        if what in ("java", "all"):
            license_text = (self.repo_root / "LICENSE").read_bytes()
            staging = package.build_maven(self.repo_root, libraries, out_dir, work, version)
            java_ok = package.check_maven(staging, version, libraries, license_text)
            ok = java_ok and ok
            if smoke and java_ok:
                package.smoke_maven(self.repo_root, staging, version)

        if what in ("rust", "all"):
            license_text = (self.repo_root / "LICENSE").read_bytes()
            crates = package.build_crates(self.repo_root, libraries, out_dir)
            rust_ok = package.check_crates(crates, version, libraries, license_text)
            ok = rust_ok and ok
            if smoke and rust_ok:
                package.smoke_crates(crates, version, libraries[package.host_rid()])

        if what in ("lua", "all"):
            license_text = (self.repo_root / "LICENSE").read_bytes()
            rock = package.build_rock(self.repo_root, libraries, out_dir, work, version)
            lua_ok = package.check_rock(rock, version, libraries, license_text)
            ok = lua_ok and ok
            if smoke and lua_ok:
                package.smoke_rock(rock["src_rock"], version, libraries[package.host_rid()])

        if what in ("js", "all"):
            # The addons are always `zig build dist`'s: they link nothing, so this host builds every platform's
            dist_dir = self.dist() if given else self.repo_root / "zig-out" / "dist"
            addons = {rid: dist_dir / "node" / rid / package.NPM_ADDON for rid in package.PLATFORMS}
            license_text = (self.repo_root / "LICENSE").read_bytes()
            tarball = package.build_npm(self.repo_root, libraries, addons, out_dir, work)
            js_ok = package.check_npm(tarball, version, libraries, addons, license_text)
            ok = js_ok and ok
            if smoke and js_ok:
                package.smoke_npm(tarball, libraries[package.host_rid()])

        if what in ("go", "all"):
            license_text = (self.repo_root / "LICENSE").read_bytes()
            tree = package.build_go(self.repo_root, libraries, out_dir)
            go_ok = package.check_go(self.repo_root, tree, libraries, license_text, work / "go")
            ok = go_ok and ok
            if smoke and go_ok:
                package.smoke_go(tree, version, libraries[package.host_rid()], work / "go")

        shutil.rmtree(work, ignore_errors=True)
        print(f"\n📦 {out_dir}:")
        for path in sorted(out_dir.iterdir()):
            if path.name == "rust":  # the workspace the crates were packaged from (for `cargo publish`)
                print("   rust/ (the workspace the crates were packaged from, for cargo publish)")
                continue
            if path.name == "go":  # the Go module's tree
                print(f"   go/ (the Go module's tree, for the commit the tag {package.GO_BINDING}/v{version} points to)")
                continue
            if path.is_dir():  # the Maven repository folder: its jars and POM
                for file in sorted(p for p in path.rglob("*") if p.suffix in (".jar", ".pom")):
                    print(f"   {file.relative_to(out_dir).as_posix()} ({file.stat().st_size / 1024:.1f} KB)")
                continue
            print(f"   {path.name} ({path.stat().st_size / 1024:.1f} KB)")
        if not ok:
            sys.exit(1)

    def smoke(self, files: List[Path]) -> None:
        """Installs built packages the way a user does and makes a round trip through each (devtool_lib/package.py):
        a wheel for this OS into a fresh venv, a .nupkg into a console project restored from its folder, the Maven
        artifact (a Maven repository folder, or the artifact's jar in one) into a Gradle project that resolves it
        from there, the crates (either .crate names both, side by side) into a Cargo project that takes them from a
        vendored folder, the Lua source rock (or its rockspec beside it) with LuaRocks into a fresh tree, the npm package
        with npm into a fresh project, the Go module's tree (zig-out/packages/go) into a Go project that requires it
        from a file proxy."""
        smoked_crates = set()
        for path in files:
            path = path.resolve()
            tree = package.go_tree_of(path)
            if tree is not None:
                version = package.check_versions(self.repo_root, None)
                work = self.zig_out / "go-smoke"
                shutil.rmtree(work, ignore_errors=True)
                package.smoke_go(tree, version, None, work)
                shutil.rmtree(work, ignore_errors=True)
                continue
            tarball = package.npm_tarball_of(path)
            if tarball is not None:
                package.smoke_npm(tarball, None)
                continue
            rock = package.rock_of(path)
            if rock is not None:
                version = rock.name.removeprefix(f"{package.LUA_ROCK}-").removesuffix(
                    f"-{package.LUA_REVISION}.src.rock")
                package.smoke_rock(rock, version, None)
                continue
            crates = package.crates_of(path)
            if crates is not None:
                if tuple(crates) not in smoked_crates:
                    smoked_crates.add(tuple(crates))
                    version = crates[0].name.removeprefix(f"{package.RUST_SYS}-").removesuffix(".crate")
                    package.smoke_crates(crates, version, None)
                continue
            repository = package.maven_repository_of(path)
            if repository is not None:
                versions = [path.parent.name] if path.suffix == ".jar" else package.maven_versions(repository)
                for version in versions:
                    package.smoke_maven(self.repo_root, repository, version)
                continue
            if path.suffix == ".whl":
                if not path.stem.endswith(package.PLATFORMS[package.host_rid()][1]):
                    print(f"⏭️  {path.name}: not this OS's wheel")
                    continue
                package.smoke_wheel(path)
            elif path.suffix == ".nupkg":
                package.smoke_nupkg(path, path.stem.removeprefix(f"{package.NUGET_ID}."), str(self.ctx.dotnet_exe))
            else:
                print(f"❌ {path}: not a .whl, a .nupkg, a Maven repository folder (or a jar in one) of "
                      f"{package.MAVEN_GROUP}:{package.MAVEN_ARTIFACT}, a .crate of {package.RUST_CRATE}, a "
                      f"source rock of {package.LUA_ROCK}, the npm package {package.NPM_PACKAGE}, nor the Go "
                      f"module's tree ({package.GO_MODULE})")
                sys.exit(1)


# The Zig suite's benchmarks: one per scenario, plus every scenario (zig-all) and the gate cases (zig-gates)
ZIG_BENCHMARKS = [f"zig-{name}" for name in (*perfgate.ZIG_SCENARIOS, "all", "gates")]
# The C, C++, Zig (the native module), C#, Java, Python, Rust, Lua, JavaScript and Go benchmarks, each in three modes
# (bench/c's c_bench and cpp_bench, bench/zig-api, bench/csharp/FastIpc.Bench, bench/java, bench/python/fipc_bench.py,
# bench/rust, bench/lua/fipc_bench.lua, bench/js/fipc_bench.mjs, bench/go)
OTHER_MODES = {"fastipc": "copy", "fastipc-zerocopy": "zerocopy", "rpc": "rpc"}
OTHER_BENCHMARKS = [f"{lang}-{name}" for lang in ("c", "cpp", "zigapi", "python", "csharp", "java", "rust", "lua", "js", "go")
                    for name in OTHER_MODES]
# The C# binding's raw layer against its object layer, ns per message in one process (not in the baseline table)
OTHER_BENCHMARKS.append("csharp-layers")


def benchmark_map(tool: DevTool, quick: bool = False) -> Dict:
    """Benchmark name -> function that runs it"""
    zig = {name: (lambda target=name.removeprefix("zig-"): tool.bench_zig(target, quick)) for name in ZIG_BENCHMARKS}
    zigapi = {f"zigapi-{name}": (lambda mode=mode: tool.bench_zig_api(mode)) for name, mode in OTHER_MODES.items()}
    python = {f"python-{name}": (lambda mode=mode: tool.bench_python(mode)) for name, mode in OTHER_MODES.items()}
    csharp = {f"csharp-{name}": (lambda mode=mode: tool.bench_csharp(mode)) for name, mode in OTHER_MODES.items()}
    csharp["csharp-layers"] = lambda: tool.bench_csharp("layers")
    java = {f"java-{name}": (lambda mode=mode: tool.bench_java(mode)) for name, mode in OTHER_MODES.items()}
    rust = {f"rust-{name}": (lambda mode=mode: tool.bench_rust(mode)) for name, mode in OTHER_MODES.items()}
    lua = {f"lua-{name}": (lambda mode=mode: tool.bench_lua(mode)) for name, mode in OTHER_MODES.items()}
    js = {f"js-{name}": (lambda mode=mode: tool.bench_js(mode)) for name, mode in OTHER_MODES.items()}
    go = {f"go-{name}": (lambda mode=mode: tool.bench_go(mode)) for name, mode in OTHER_MODES.items()}
    c = {f"{lang}-{name}": (lambda program=f"{lang}_bench", mode=mode: tool.bench_c(program, mode))
         for lang in ("c", "cpp") for name, mode in OTHER_MODES.items()}
    return zig | c | zigapi | python | csharp | java | rust | lua | js | go


def main():
    """Main entry point for devtool CLI"""
    # The messages print emoji; redirected output on Windows would default to cp1252 and fail on them
    for stream in (sys.stdout, sys.stderr):
        stream.reconfigure(encoding="utf-8")

    parser = argparse.ArgumentParser(
        description="Development workflow tool for FastIPC",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  devtool build                    # Build in debug mode for current platform
  devtool build --release          # Build in release mode
  devtool build --windows          # Cross-compile for Windows
  devtool --verbose build          # Build, saying which libraries it copies where
  devtool test fast                # The everyday loop: Zig unit tests + single-process C-API tests
  devtool test slow                # The multi-process C-API tests (the PR gate runs fast and slow)
  devtool test all                 # fast, then slow, with each tier's duration
  devtool test chaos               # Kill-and-restart harness, every scenario, 4 at once (--scenario, --cycles, --seed, --jobs)
  devtool test soak                # Soak harness: connection churn and ping-pong, resources flat
  devtool test fuzz                # Corrupt-peer harness: control-plane and ring-content cases, gated
  devtool test interop             # Every language's server example vs every client example (100 pairs)
  devtool test interop --server rust --client python,lua --client-first all   # some pairs, both orders
  devtool bench zig-fastipc        # The Zig suite's stream benchmark (zig-all: every scenario)
  devtool bench zig-all --quick    # Every scenario, one short run per case (CI's smoke run)
  devtool bench --regex "^zig-rpc"  # Run all benchmarks matching regex
  devtool bench python-fastipc     # Run FastIPC Python benchmark
  devtool bench csharp-layers      # The C# binding's raw layer vs its object layer, ns per message
  devtool bench java-rpc           # The Java benchmark (bench/java), RPC
  devtool bench rust-fastipc       # The Rust benchmark (bench/rust), copied messages
  devtool bench lua-rpc            # The Lua benchmark (bench/lua, LuaJIT), RPC
  devtool bench js-fastipc         # The JavaScript benchmark (bench/js, Node.js), copied messages
  devtool bench go-fastipc         # The Go benchmark (bench/go), copied messages
  devtool bench c-fastipc          # The C benchmark (bench/c, the C API); cpp-fastipc: the same through fipc.hpp
  devtool bench zigapi-fastipc     # The Zig benchmark (bench/zig-api, the native module), copied messages
  devtool bench-compare --ab       # A/B: the gate cases, this tree vs main (--ref REV)
  devtool bench-compare csharp-rpc # Perf gate: 5 runs, medians vs docs/perf/bindings-baseline.md
  devtool bench-pace               # Tune the reader's spin pace (ring_wait.zig stream_paces): paces 1-64, 3 rounds
  devtool install                  # Install all dependencies
  devtool install python           # Install only Python dependencies
  devtool format                   # Format C/H with clang-format-18, Zig with zig fmt and Go with gofmt
  devtool format --check           # Check formatting without modifying files (for CI)
  devtool check-frozen             # Frozen files (include/fipc.h, fipc.hpp) must match their recorded hashes
  devtool dist                     # Packaged libraries (x86-64-v3, glibc 2.34; arm64 Linux glibc 2.34; arm64 macOS 14.4)
                                   #   in zig-out/dist, checked
  devtool package                  # This OS's wheel and the NuGet package in zig-out/packages, checked
  devtool package all --smoke      # ... and installed (a venv, a console, Gradle and Cargo project, a LuaRocks
                                   #   tree) for a round trip
  devtool package java --smoke     # the Maven artifact (jar, sources, javadoc, POM) in zig-out/packages/maven, tried
  devtool package rust --smoke     # the crates fipc-sys and fipc in zig-out/packages, tried
  devtool package lua --smoke      # the rock (rockspec, source rock, archive) in zig-out/packages, tried
  devtool package js --smoke       # the npm package (fipc-<version>.tgz) in zig-out/packages, tried with Node.js,
                                   #   Bun and Deno where installed
  devtool package go --smoke       # the Go module's tree (zig-out/packages/go), tried from a file proxy
  devtool package csharp --linux-lib L --windows-lib W   # the NuGet package from given libraries (release.yml)
  devtool smoke zig-out/packages/*.nupkg   # install built packages and make a round trip through each
  devtool check-exports --subset   # The shared library exports the whole public API
        """,
    )

    # Add global --verbose flag
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Say which libraries a build copies where"
    )

    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # Build command
    build_parser = subparsers.add_parser("build", help="Build the library (zig build)")
    build_parser.add_argument(
        "--release", action="store_true", help="Build in release mode (ReleaseFast)"
    )
    build_group = build_parser.add_mutually_exclusive_group()
    build_group.add_argument(
        "--windows", action="store_true", help="Explicit Windows target"
    )
    build_group.add_argument(
        "--linux", action="store_true", help="Explicit Linux target"
    )

    # Test command
    test_parser = subparsers.add_parser("test", help="Run tests")
    test_parser.add_argument(
        "language",
        choices=["fast", "slow", "all", "zig", "chaos", "soak", "fuzz", "interop"],
        help="'fast': the everyday loop, the Zig unit tests and the single-process C-API tests (zig build test); "
        "'slow': the multi-process C-API tests (zig build test-slow); 'all': both; 'zig': the same as 'fast'; "
        "'chaos': the kill-and-restart harness (devtool_lib/chaos.py); "
        "'soak': the soak harness (connection churn and ping-pong, resources flat); "
        "'fuzz': the corrupt-peer harness (control-plane and ring-content cases, gated); "
        "'interop': every language's server example against every language's client example (devtool_lib/interop.py)",
    )
    test_parser.add_argument(
        "--filter",
        help="fast, slow, all: run only the C-API tests whose name contains this text (-Dtest-filter), in either tier",
    )
    test_parser.add_argument(
        "--scenario",
        help=f"chaos: run only this scenario ({', '.join(chaos.SCENARIOS)})",
    )
    test_parser.add_argument(
        "--cycles", type=int, help="chaos: cycles per scenario (default: 200 for kill-*, 50 for the others)"
    )
    test_parser.add_argument(
        "--seed", type=int, help="chaos/fuzz: base seed; cycle k uses seed + k (default: random, printed)"
    )
    test_parser.add_argument(
        "--jobs", type=int, default=chaos.DEFAULT_JOBS,
        help=f"chaos: scenarios run at once, each in a process of its own and printed as one block when it ends "
        f"(default: {chaos.DEFAULT_JOBS}; 1: one after another, each line as it comes)",
    )
    test_parser.add_argument(
        "--ready-ms", type=int, default=5000, help="chaos: the peers' accept and connect timeout (default: 5000)"
    )
    test_parser.add_argument(
        "--observe-ms", type=int, default=1000,
        help="chaos: how long a failing cycle is observed after its first problem, for evidence (default: 1000)",
    )
    test_parser.add_argument(
        "--seconds", type=int, help="soak: run for this long (default: a 120 s smoke run); the full run is heavy validation"
    )
    test_parser.add_argument(
        "--pin", action="store_true", help="soak (Linux): run pinned to CPU 0 with taskset -c 0"
    )
    test_parser.add_argument(
        "--rounds", type=int, help="fuzz: rounds per case (default: a short smoke run)"
    )
    test_parser.add_argument(
        "--trace", action="store_true", help="chaos: print every line the peers print, as it arrives"
    )
    test_parser.add_argument(
        "--server", action="append", metavar="L",
        help=f"interop: only these servers (repeat or comma-separate; {', '.join(interop.KEYS)})",
    )
    test_parser.add_argument(
        "--client", action="append", metavar="L",
        help=f"interop: only these clients (repeat or comma-separate; {', '.join(interop.KEYS)})",
    )
    test_parser.add_argument(
        "--client-first", choices=["sample", "all", "none"], default="sample",
        help="interop: also start the client first, one second ahead of the server, for a pair per server (sample, "
        "the default: each server with the next language's client), for every pair (all), or not at all (none)",
    )
    test_parser.add_argument(
        "--skip-missing", action="store_true",
        help="interop: leave out the languages whose toolchain is missing instead of failing",
    )
    test_parser.add_argument(
        "--windows", action="store_true", help="Run on Windows host (from WSL)"
    )
    test_parser.add_argument(
        "--loglevel",
        type=str,
        choices=["debug", "DEBUG", "info", "INFO", "warn", "WARN", "error", "ERROR"],
        help="Set logging level (DEBUG, INFO, WARN, ERROR)"
    )

    # Bench command
    bench_parser = subparsers.add_parser("bench", help="Run benchmarks")
    bench_parser.add_argument(
        "benchmark",
        nargs="?",
        choices=ZIG_BENCHMARKS + OTHER_BENCHMARKS,
        help="Benchmark to run",
    )
    bench_parser.add_argument(
        "--quick", action="store_true",
        help="zig-*: a smoke run, one run of 20 ms per case (CI)",
    )
    bench_parser.add_argument(
        "--regex",
        type=str,
        help="Run all benchmarks whose names match this regex"
    )
    bench_parser.add_argument(
        "--windows", action="store_true", help="Run on Windows host (from WSL)"
    )
    bench_parser.add_argument(
        "--loglevel",
        type=str,
        choices=["debug", "DEBUG", "info", "INFO", "warn", "WARN", "error", "ERROR"],
        help="Set logging level (DEBUG, INFO, WARN, ERROR)"
    )

    # Perf gate
    compare_parser = subparsers.add_parser(
        "bench-compare",
        help="Perf gates: the Zig suite vs another revision's library (--ab), or C#/Python medians vs "
        "docs/perf/bindings-baseline.md (build --release first)",
    )
    compare_parser.add_argument(
        "benchmarks", nargs="*",
        help="Benchmarks to run (default: --ab the gate cases, otherwise every benchmark in the baseline)"
    )
    compare_parser.add_argument(
        "--runs", type=int, help="Runs per benchmark (default: 5); --ab: alternating rounds (default: 3)"
    )
    compare_parser.add_argument(
        "--duration", type=float,
        help="--ab: seconds per case and run (default: 0.3, a quick run; a failing case is re-checked on its own, "
        "docs/perf/baseline.md)"
    )
    compare_parser.add_argument(
        "--ab", action="store_true",
        help="Zig suite: this tree's library vs the library of --ref, alternating in one session, "
        "instead of the baseline table",
    )
    compare_parser.add_argument(
        "--ref", default="main",
        help="--ab: the other library's git revision, one that speaks include/fipc.h (default: main)"
    )

    pace_parser = subparsers.add_parser(
        "bench-pace",
        help="Tune the reader's spin pace (stream_pace, src/zig/data/ring_wait.zig): the Zig suite per pace",
    )
    pace_parser.add_argument(
        "--paces", default="1,2,4,8,16,32,64",
        help="Comma-separated paces, each a build with -Dstream_pace (default: 1,2,4,8,16,32,64; 1 is unpaced)"
    )
    pace_parser.add_argument("--rounds", type=int, default=3, help="Alternating rounds (default: 3)")
    pace_parser.add_argument("--runs", type=int, default=5, help="fipc_bench runs per case and round (default: 5)")

    # Install command
    install_parser = subparsers.add_parser("install", help="Install dependencies")
    install_parser.add_argument(
        "language",
        nargs="?",
        choices=["python", "csharp"],
        help="Language to install (omit for all)",
    )
    install_parser.add_argument(
        "--windows", action="store_true", help="Install for Windows host (from WSL)"
    )

    # Format command
    format_parser = subparsers.add_parser("format", help="Format code with clang-format-18, zig fmt and gofmt")
    format_parser.add_argument(
        "--check", action="store_true", help="Check formatting without modifying files (for CI)"
    )

    # Contract checks
    subparsers.add_parser(
        "check-frozen", help="Check that frozen files (include/fipc.h, fipc.hpp) match their recorded hashes"
    )
    exports_parser = subparsers.add_parser(
        "check-exports", help="Compare the shared library's exports with the FIPC_API functions of include/fipc.h"
    )
    exports_mode = exports_parser.add_mutually_exclusive_group(required=True)
    exports_mode.add_argument(
        "--subset", action="store_true", help="Every declared function is exported (other symbols may be too)"
    )
    exports_mode.add_argument(
        "--exact", action="store_true", help="Exactly the declared functions are exported"
    )

    # LOC command
    subparsers.add_parser("loc", help="Count the lines of Zig and C: library, tests, harnesses, bench")

    # Dist command
    subparsers.add_parser(
        "dist", help="Build and check the packaged shared libraries (Linux and Windows x86-64-v3, glibc 2.34; Linux arm64, "
        "glibc 2.34; macOS arm64, 14.4) in zig-out/dist"
    )

    # Package command
    package_parser = subparsers.add_parser(
        "package",
        help="Build and check the Python wheel(s), the NuGet package, the Maven artifact, the Rust crates, the "
        "Lua rock, the npm package and the Go module's tree in zig-out/packages (never publishes)",
    )
    package_parser.add_argument(
        "what", nargs="?", default="all", choices=["python", "csharp", "java", "rust", "lua", "js", "go", "all"],
        help="python: a wheel per library (this OS's when none is given); csharp: the NuGet package with every "
        "platform's library; java: the Maven artifact with every platform's library (zig-out/packages/maven); rust: "
        "the crates fipc-sys (with every platform's library) and fipc; lua: the rockspec and the source "
        "rock (with every platform's library); js: the npm package (every platform's library and Node-API addon); "
        "go: the Go module's tree (with every platform's library, zig-out/packages/go); all (default): every kind",
    )
    package_parser.add_argument(
        "--linux-lib", type=Path, help="this libfastipc.so instead of `zig build dist`'s (checked the same way)"
    )
    package_parser.add_argument(
        "--windows-lib", type=Path, help="this fastipc.dll instead of `zig build dist`'s (checked the same way)"
    )
    package_parser.add_argument(
        "--macos-lib", type=Path, help="this libfastipc.dylib instead of `zig build dist`'s (checked the same way)"
    )
    package_parser.add_argument(
        "--linux-arm64-lib", type=Path,
        help="this ARM64 libfastipc.so instead of `zig build dist`'s (checked the same way)"
    )
    package_parser.add_argument(
        "--smoke", action="store_true",
        help="install this OS's wheel into a fresh venv, the NuGet package into a console project, the Maven "
        "artifact into a Gradle project, the crates into a Cargo project, the rock into a LuaRocks tree, the npm "
        "package into a project (Node.js, Bun and Deno where installed) and the Go module into a Go project (from a "
        "file proxy), and make a round trip through each",
    )
    package_parser.add_argument(
        "--expect-version", help="fail unless every package carries this version (the release tag's, without the v)"
    )

    smoke_parser = subparsers.add_parser(
        "smoke", help="Install built packages (.whl for this OS, .nupkg, the Maven artifact, the crates, the rock, the "
        "npm package, the Go module) and make a round trip through each"
    )
    smoke_parser.add_argument(
        "files", nargs="+", type=Path,
        help="the .whl and .nupkg files, Maven repository folders (zig-out/packages/maven) or the jars in them, "
        ".crate files (either crate names both, side by side), the Lua .src.rock (or the .rockspec beside it), "
        "the npm package's .tgz, and the Go module's tree (zig-out/packages/go)")

    # Enable tab completion if argcomplete is available
    if ARGCOMPLETE_AVAILABLE:
        argcomplete.autocomplete(parser)

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    # Determine if targeting Windows (only applicable for certain commands)
    target_windows = getattr(args, 'windows', False)

    # Get loglevel if available (only for commands that support it)
    loglevel = getattr(args, 'loglevel', None)

    # Create DevTool instance with execution mode and verbose flag
    tool = DevTool(target_windows=target_windows, verbose=args.verbose, loglevel=loglevel)

    # Dispatch to appropriate command
    match args.command:
        case "build":
            tool.build(release=args.release, windows=args.windows, linux=args.linux)
        case "test":
            if args.language in ("fast", "zig"):
                tool.test_zig_tiers(["fast"], args.filter)
            elif args.language == "slow":
                tool.test_zig_tiers(["slow"], args.filter)
            elif args.language == "all":
                tool.test_zig_tiers(["fast", "slow"], args.filter)
            elif args.language == "chaos":
                tool.test_chaos(scenario=args.scenario, cycles=args.cycles, seed=args.seed,
                                ready_ms=args.ready_ms, observe_ms=args.observe_ms, trace=args.trace,
                                jobs=args.jobs)
            elif args.language == "soak":
                tool.test_soak(seconds=args.seconds, pin=args.pin)
            elif args.language == "fuzz":
                tool.test_fuzz(rounds=args.rounds, seed=args.seed)
            elif args.language == "interop":
                def languages(given: Optional[List[str]]) -> List[str]:
                    names = [n.strip() for g in given or [] for n in g.split(",") if n.strip()]
                    unknown = [n for n in names if n not in interop.KEYS]
                    if unknown:
                        print(f"Error: unknown language {', '.join(unknown)} (choose from: {', '.join(interop.KEYS)})")
                        sys.exit(1)
                    return [k for k in interop.KEYS if k in names] if names else list(interop.KEYS)

                tool.test_interop(languages(args.server), languages(args.client), args.client_first,
                                  args.skip_missing)
        case "loc":
            loc.count_loc(tool.repo_root)
        case "bench":
            import re

            throttle.unthrottle_descendants()

            benchmarks = benchmark_map(tool, quick=args.quick)
            if args.regex:
                try:
                    pattern = re.compile(args.regex)
                except re.error as e:
                    print(f"Error: Invalid regex: {e}")
                    sys.exit(1)

                matched = [name for name in benchmarks.keys() if pattern.search(name)]
                if not matched:
                    print(f"No benchmarks match regex: {args.regex}")
                    sys.exit(1)

                print("Matched benchmarks:")
                for name in matched:
                    print(f"  - {name}")

                for name in matched:
                    benchmarks[name]()
                    time.sleep(1.0)
            else:
                if not args.benchmark:
                    print("Error: Specify a benchmark or use --regex")
                    sys.exit(1)
                benchmarks[args.benchmark]()
        case "bench-compare":
            throttle.unthrottle_descendants()
            if args.ab:
                tool.bench_compare_ab(args.benchmarks, args.runs or 3, args.ref, args.duration or 0.3)
            else:
                tool.bench_compare(args.benchmarks, args.runs or 5)
        case "bench-pace":
            throttle.unthrottle_descendants()
            tool.bench_pace([int(pace) for pace in args.paces.split(",")], args.rounds, args.runs)
        case "install":
            if args.language == "python":
                tool.install_python()
            elif args.language == "csharp":
                tool.install_csharp()
            else:
                tool.install_all()
        case "format":
            zig_ok = zig_format.format_zig(tool.repo_root, check_only=args.check)
            go_ok = go_format.format_go(tool.repo_root, check_only=args.check)
            clang_format.format_code(tool.repo_root, check_only=args.check)
            if not (zig_ok and go_ok):
                sys.exit(1)
        case "check-frozen":
            if not frozen.check_frozen(tool.repo_root):
                sys.exit(1)
        case "check-exports":
            tool.check_exports(exact=args.exact)
        case "dist":
            tool.dist()
        case "package":
            tool.package(args.what, args.linux_lib, args.windows_lib, args.macos_lib, args.linux_arm64_lib, args.smoke,
                         args.expect_version)
        case "smoke":
            tool.smoke(args.files)

if __name__ == "__main__":
    main()
