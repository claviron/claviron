//! OS-level platform plumbing — small, native-API wrappers shared by
//! the rest of the workspace.
//!
//! The crate is the home for code that talks directly to the OS:
//! `setsockopt` socket options, process / kernel info queries
//! (`/proc`, `sysctl`, Mach `task_info`), file-descriptor limits, and
//! similar low-level helpers. None of it is "networking" or "runtime
//! orchestration"; it's the place where Rust meets the kernel.
//!
//! Consumers used to pull these helpers from a few different places
//! (`claviron-net`, `sysinfo`, hand-rolled `/proc` parsers in
//! per-feature crates). All converge here.

pub mod limits;
pub mod net;
pub mod process;
pub mod rss;
pub mod signal;

mod hostname;
pub use hostname::short_hostname;
