//! Concurrent HTTP/1 with a separate server process and process CPU accounting.

#[path = "../../support/mod.rs"]
pub mod support;

#[cfg(unix)]
#[path = "../../support/http1/concurrent.rs"]
mod concurrent;

#[cfg(unix)]
fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    concurrent::run()
}

#[cfg(not(unix))]
fn main() {
    panic!("HTTP/1 process CPU benchmarks require Unix getrusage");
}
