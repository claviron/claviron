use clap::Parser;
use claviron::{FleetName, ServerName, ServerOptions};
use std::path::PathBuf;

/// The binary owns its user-facing flags and maps them into Claviron's typed
/// server options; what it serves is registered in `app::bootstrap`. Worker
/// identity is intentionally absent: the master passes that through
/// Claviron's private supervisor bootstrap protocol.
#[derive(Parser)]
#[command(
    name = "edge-server",
    author,
    version,
    about = "Example Application Server"
)]
pub struct Options {
    #[arg(
        short = 'c',
        long = "config-dir",
        visible_alias = "config",
        default_value = "runtime",
        help = "Directory containing Claviron config files"
    )]
    config_dir: PathBuf,

    #[arg(
        short = 'n',
        long = "name",
        default_value = env!("CARGO_PKG_NAME"),
        help = "Server instance name"
    )]
    name: ServerName,

    #[arg(
        short = 'f',
        long = "fleet",
        visible_alias = "fleet-name",
        default_value = "edge",
        help = "Orbit fleet name"
    )]
    fleet_name: FleetName,

    #[arg(
        long = "environment",
        default_value = "production",
        help = "Deployment environment for .env overlays"
    )]
    environment: String,

    #[arg(
        long = "worker-count",
        visible_alias = "workers",
        default_value_t = 0,
        help = "Worker child count; zero runs standalone"
    )]
    worker_count: u8,

    #[arg(
        short = 't',
        long = "check-config",
        help = "Validate registered providers and exit without serving"
    )]
    pub(crate) check_config: bool,
}

impl From<Options> for ServerOptions {
    fn from(options: Options) -> Self {
        Self::new()
            .config_dir(options.config_dir)
            .name(options.name)
            .fleet_name(options.fleet_name)
            .environment(options.environment)
            .workers(options.worker_count)
    }
}
