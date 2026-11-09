//! Production H2 over Unix sockets, with the same pools and supervised harness.
#[cfg(unix)]
#[path = "../../harness/mod.rs"]
mod harness;

#[cfg(unix)]
fn main() -> harness::Result<()> {
    harness::run(harness::Transport::UdsH2)
}
#[cfg(not(unix))]
fn main() {
    panic!("the Link harness requires Unix process and socket APIs");
}
