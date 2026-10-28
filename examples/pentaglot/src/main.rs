mod app;

use std::path::PathBuf;

use clap::Parser;
use claviron::{FleetName, Server, ServerName, ServerOptions};

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
    #[arg(long, default_value = "local")]
    environment: String,
    #[arg(long)]
    check_config: bool,
}

#[tokio::main]
async fn main() -> claviron::Result<()> {
    let args = Args::parse();
    let check = args.check_config;
    let options = ServerOptions::new()
        .config_dir(args.config)
        .name(args.name)
        .fleet_name(args.fleet)
        .workers(args.workers)
        .environment(args.environment);
    let server = Server::with_options(options).register(app::register);
    if check { server.check_config().await } else { server.serve().await }
}
