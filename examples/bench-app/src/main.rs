use std::path::PathBuf;

use clap::Parser;
use claviron::Register;
use claviron::axum::Router;
use claviron::runner::{self, Owner, RunnerOptions, launch};
use claviron::{FleetName, Server, ServerOptions, registry};

#[derive(Parser)]
#[command(
    about = "Native Rust benchmark application over Orbit Link and HTTP sockets"
)]
struct Options {
    #[arg(long, default_value = "bench-app.yaml")]
    config: PathBuf,
    #[arg(long)]
    check_config: bool,
    #[command(flatten)]
    runner: RunnerOptions,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let options = Options::parse();
    let written = serde_yaml::from_slice(&std::fs::read(&options.config)?)?;
    let runner = options.runner.over(written);
    runner.validate()?;
    let name = runner.name.clone().ok_or("name is required")?;
    let fleet =
        FleetName::try_new(runner.fleet_name.as_deref().unwrap_or(&name))?;
    let config_dir = options
        .config
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or(std::path::Path::new("."));
    let server = Server::with_options(
        ServerOptions::new()
            .name(name)
            .fleet_name(fleet)
            .config_dir(config_dir),
    )
    .register(move |state| -> registry::Result<()> {
        launch::register_routing(state, &runner, std::path::Path::new("."))?;
        runner::register_owner(state, Owner::new(Router::new()))?;
        claviron::bench::runner::BenchService::register(state)?;
        runner::ensure_application(state)?;
        launch::register_trust(state, &runner)?;
        launch::register_doors(state, &runner, "rust", None)
    });
    if options.check_config {
        server.check_config().await?;
    } else {
        server.serve().await?;
    }
    Ok(())
}
