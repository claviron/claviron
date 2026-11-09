//! Supervised Link measurement; both process families boot through Claviron.
#[cfg(unix)]
#[path = "../../harness/mod.rs"]
mod harness;

#[cfg(unix)]
fn main() -> harness::Result<()> {
    harness::run(harness::Transport::Orbit)
}
#[cfg(not(unix))]
fn main() {
    panic!("the Link harness requires Unix process and socket APIs");
}
