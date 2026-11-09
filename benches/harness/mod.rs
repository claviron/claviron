//! A supervised client family and a supervised Runner family, with no frontend.
mod backend;
mod boot;
mod config;
mod diagnostics;
mod fixtures;
mod process;
mod report;
mod service;
mod wire;

pub use config::{Args, Transport};
pub type Result<T> =
    std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
pub const PLAIN: &[u8] = b"ok\n";

pub fn run(default_transport: Transport) -> Result<()> {
    use clap::Parser;
    if let Some(launch) = boot::inherited()? {
        return boot::run(launch);
    }
    let args = Args::parse();
    let selected = args.transport.unwrap_or(default_transport);
    let runtime =
        tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    runtime.block_on(report::run(args, selected))
}
