mod app;

use std::path::PathBuf;

use clap::Parser;
use claviron::{FleetName, Server, ServerName, ServerOptions};

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Parser)]
#[command(about = "Five applications embedded in one Claviron Edge")]
struct Args {
    #[arg(long, default_value = "config")]
    config: PathBuf,
    #[arg(long, default_value = "pentaglot", value_parser = str::parse::<ServerName>)]
    name: ServerName,
    #[arg(long, default_value = "penta", value_parser = str::parse::<FleetName>)]
    fleet: FleetName,
    #[arg(long, default_value_t = 0)]
    workers: u8,
    #[arg(long, conflicts_with = "production")]
    environment: Option<String>,
    #[arg(long)]
    production: bool,
    #[arg(long)]
    check_config: bool,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> claviron::Result<()> {
    let args = Args::parse();
    let check = args.check_config;
    let mut options = ServerOptions::new()
        .config_dir(args.config)
        .name(args.name)
        .fleet_name(args.fleet)
        .workers(args.workers);
    if args.production {
        options = options.production();
    } else if let Some(environment) = args.environment {
        options = options.environment(environment);
    }
    let server = Server::with_options(options).register(app::register);
    if check { server.check_config().await } else { server.serve().await }
}
