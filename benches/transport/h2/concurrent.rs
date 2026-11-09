//! Production H2c upstream and Runner under the common supervised harness.
#[cfg(unix)]
#[path = "../../harness/mod.rs"]
mod harness;

#[cfg(unix)]
fn main() -> harness::Result<()> {
    harness::run(harness::Transport::H2c)
}
#[cfg(not(unix))]
fn main() {
    panic!("the Link harness requires Unix process and socket APIs");
}
