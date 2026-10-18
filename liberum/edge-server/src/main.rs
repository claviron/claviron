use clap::Parser;
use claviron::{Result, Server};

use crate::options::Options;
use tracing::error;

mod app;
mod options;

// Use mimalloc as the global allocator.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let options = Options::parse();
    let check_config = options.check_config;
    let server = Server::with_options(options.into()).register(app::bootstrap);

    let result = if check_config {
        server.check_config().await
    } else {
        server.serve().await
    };

    if let Err(e) = result {
        error!("failed to start services: {}", e);
        return Err(e);
    }

    Ok(())
}
