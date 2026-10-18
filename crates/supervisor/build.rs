//! Records the source revision this crate was built from, so a running master
//! can say which tree it is. The 2026-09-13 incident record had to infer that
//! from a renamed log target; the master now states it at boot and in every
//! worker snapshot.

use std::path::PathBuf;
use std::process::Command;

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=CLAVIRON_BUILD_REVISION");

    // An explicit revision wins: a packaging step that builds outside a
    // checkout knows better than `git` would.
    let revision = std::env::var("CLAVIRON_BUILD_REVISION")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(git_revision);
    println!("cargo:rustc-env=CLAVIRON_SUPERVISOR_BUILD_REVISION={revision}");
}

/// `<short hash>` or `<short hash>-dirty`, or `unknown` without a repository.
/// Reruns when the checked-out commit or the index changes; a never-staged
/// edit is the one drift this cannot see, and the `dirty` flag exists to make
/// the operator suspicious of exactly that build.
fn git_revision() -> String {
    let manifest_dir = PathBuf::from(
        std::env::var("CARGO_MANIFEST_DIR").expect("manifest dir"),
    );
    let git = |args: &[&str]| {
        Command::new("git")
            .arg("-c")
            .arg("core.fsmonitor=false")
            .args(args)
            .current_dir(&manifest_dir)
            .output()
            .ok()
            .filter(|output| output.status.success())
            .map(|output| {
                String::from_utf8_lossy(&output.stdout).trim().to_string()
            })
    };
    let Some(short) = git(&["rev-parse", "--short=8", "HEAD"]) else {
        return "unknown".to_string();
    };
    if let Some(git_dir) = git(&["rev-parse", "--absolute-git-dir"]) {
        println!("cargo:rerun-if-changed={git_dir}/HEAD");
        println!("cargo:rerun-if-changed={git_dir}/index");
        if let Some(branch) = git(&["symbolic-ref", "-q", "HEAD"]) {
            println!("cargo:rerun-if-changed={git_dir}/{branch}");
        }
    }
    let dirty = git(&["status", "--porcelain", "--untracked-files=no"])
        .is_some_and(|status| !status.is_empty());
    if dirty { format!("{short}-dirty") } else { short }
}
