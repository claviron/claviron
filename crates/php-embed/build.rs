use std::path::{Path, PathBuf};
use std::process::Command;

fn main() {
    println!("cargo:rerun-if-env-changed=PHP_CONFIG");

    let php_config = std::env::var_os("PHP_CONFIG")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("php-config"));

    emit_prefix_lib(&php_config);
    emit_ldflags(&php_config);
    maybe_use_mold();
}

/// Opt into the `mold` linker on Linux / FreeBSD / macOS when it's
/// available on PATH. mold links 5–15× faster than GNU ld; falls
/// back silently to the default linker if mold isn't installed.
///
/// macOS caveat: Apple's clang (Xcode toolchain) historically ignores
/// `-fuse-ld=mold`. If `cc -fuse-ld=mold` here is Apple clang, you
/// silently get the default linker. The flag takes effect with
/// brew's LLVM clang (`brew install llvm` + `CC=/opt/homebrew/opt/llvm/bin/clang`)
/// or once Apple ships proper `-fuse-ld` support. Either way no
/// breakage — worst case is "no speedup".
fn maybe_use_mold() {
    let target_os = std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    if !matches!(target_os.as_str(), "linux" | "freebsd") {
        return;
    }
    let available = Command::new("mold")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false);
    if available {
        println!("cargo:rustc-link-arg=-fuse-ld=mold");
    }
}

fn emit_prefix_lib(php_config: &PathBuf) {
    let Ok(output) = Command::new(php_config).arg("--prefix").output() else {
        return;
    };
    if !output.status.success() {
        return;
    }

    let prefix = String::from_utf8_lossy(&output.stdout);
    let lib = PathBuf::from(prefix.trim()).join("lib");
    if lib.exists() {
        emit_link_dir(&lib);
    }
}

fn emit_ldflags(php_config: &PathBuf) {
    let Ok(output) = Command::new(php_config).arg("--ldflags").output() else {
        return;
    };
    if !output.status.success() {
        return;
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    for flag in stdout.split_whitespace() {
        let Some(path) = flag.strip_prefix("-L") else {
            continue;
        };
        if !path.is_empty() {
            emit_link_dir(&PathBuf::from(path));
        }
    }
}

fn emit_link_dir(path: &Path) {
    println!("cargo:rustc-link-search=native={}", path.display());
}
