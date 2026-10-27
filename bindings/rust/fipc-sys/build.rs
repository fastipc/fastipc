//! Finds the FastIPC shared library and puts a copy in OUT_DIR, where the linker finds it and where `cargo run` and
//! `cargo test` find it at run time (Cargo puts link-search folders inside the target folder on `PATH` /
//! `LD_LIBRARY_PATH` / `DYLD_FALLBACK_LIBRARY_PATH`).
//!
//! The library comes from, in this order:
//! 1. the folder the environment variable `FASTIPC_LIB_DIR` names;
//! 2. in a checkout of the repository, its `zig-out` build (`zig-out/lib` on Linux and macOS, `zig-out/bin` on
//!    Windows);
//! 3. the copy this crate carries in `native/<platform>/` (the published crate has every platform's).
//!
//! On docs.rs (`DOCS_RS` set) nothing links, so nothing is looked for.

use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process;

fn main() {
    println!("cargo::rerun-if-changed=build.rs");
    println!("cargo::rerun-if-env-changed=FASTIPC_LIB_DIR");
    println!("cargo::rerun-if-env-changed=DOCS_RS");
    if env::var_os("DOCS_RS").is_some() {
        return;
    }

    let os = env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    let arch = env::var("CARGO_CFG_TARGET_ARCH").unwrap_or_default();
    let (platform, file) = match (os.as_str(), arch.as_str()) {
        ("linux", "x86_64") => ("linux-x64", "libfastipc.so"),
        ("windows", "x86_64") => ("win-x64", "fastipc.dll"),
        ("macos", "aarch64") => ("osx-arm64", "libfastipc.dylib"),
        ("linux", "aarch64") => ("linux-arm64", "libfastipc.so"),
        _ => fail(&format!(
            "FastIPC supports x86_64 Linux and Windows and aarch64 Linux and macOS; this target is {arch} {os} \
             (https://github.com/fastipc/fastipc/blob/main/docs/platform-support.md)"
        )),
    };

    let manifest = PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("Cargo sets CARGO_MANIFEST_DIR"));
    let (dir, origin) = if let Some(dir) = env::var_os("FASTIPC_LIB_DIR") {
        (PathBuf::from(dir), "the folder FASTIPC_LIB_DIR names")
    } else if let Some(dir) = repository_build(&manifest, &os) {
        (dir, "the repository's zig-out build (run `python devtool.py build` first)")
    } else {
        (manifest.join("native").join(platform), "the copy this crate carries")
    };
    let library = dir.join(file);
    if !library.is_file() {
        fail(&format!(
            "{file} not found in {}, {origin}; set FASTIPC_LIB_DIR to the folder that holds it",
            dir.display()
        ));
    }

    // The library itself, and on Linux the file named by its soname too (a development build's libfastipc.so.1),
    // which is the name the loader looks for at run time.
    let out = PathBuf::from(env::var("OUT_DIR").expect("Cargo sets OUT_DIR"));
    let entries = fs::read_dir(&dir).unwrap_or_else(|e| fail(&format!("{}: {e}", dir.display())));
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name == file || (os == "linux" && name.starts_with("libfastipc.so.")) {
            let from = entry.path();
            println!("cargo::rerun-if-changed={}", from.display());
            let to = out.join(&*name);
            let _ = fs::remove_file(&to);
            fs::copy(&from, &to)
                .unwrap_or_else(|e| fail(&format!("copying {} to {}: {e}", from.display(), to.display())));
        }
    }
    println!("cargo::rustc-link-search=native={}", out.display());
    // For the build scripts of crates that depend on this one: DEP_FASTIPC_LIB_DIR, the folder with the library
    println!("cargo::metadata=lib_dir={}", out.display());
}

/// In a checkout of the repository (this crate at bindings/rust/fipc-sys), its zig-out folder with the
/// library.
fn repository_build(manifest: &Path, os: &str) -> Option<PathBuf> {
    let root = manifest.parent()?.parent()?.parent()?;
    let checkout = manifest.ends_with(Path::new("bindings").join("rust").join("fipc-sys"))
        && root.join("build.zig").is_file()
        && root.join("include").join("fipc.h").is_file();
    checkout.then(|| root.join("zig-out").join(if os == "windows" { "bin" } else { "lib" }))
}

fn fail(message: &str) -> ! {
    eprintln!("fipc-sys: {message}");
    process::exit(1);
}
