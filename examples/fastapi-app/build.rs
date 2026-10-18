//! Where the runner finds its Python, written into the binary.
//!
//! On Linux and FreeBSD the runner links `libpython3.x.so.1.0` by its soname,
//! and which file that is gets decided at run time by the loader's search. The
//! distribution's library has the same soname as ours, so a runner started
//! without `LD_LIBRARY_PATH` — from a unit file, from cron, from a plain ssh
//! command — loads the distribution's instead. That is not a slower run: its
//! environment names our standard library, the two patch releases disagree,
//! and the process dies at start with a segmentation fault (seen on xd01,
//! 2026-09-25).
//!
//! So the directory of the library the runner links — gufi reports it — goes
//! into the binary as its rpath, and the runner loads that library wherever it
//! is started from. macOS needs nothing: there a library is linked by its full
//! path.
fn main() {
    let target = std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    if matches!(target.as_str(), "linux" | "freebsd")
        && let Ok(directory) = std::env::var("DEP_GUFI_PYTHON_LIB_DIR")
    {
        println!("cargo:rustc-link-arg-bins=-Wl,-rpath,{directory}");
    }
}
