use std::path::PathBuf;

use clap::Parser;
use claviron::Register;
use claviron::axum::Router;
use claviron::runner::{self, Owner, RunnerOptions, launch};
use claviron::{Server, ServerOptions, registry};

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
    let mut definition =
        launch::Launch { runner, own: serde_yaml::Mapping::new() };
    let prepared = definition.prepare(name)?;
    let definitions = definition.clone();
    let config_dir = options
        .config
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or(std::path::Path::new("."));
    let server = Server::with_options(
        ServerOptions::new()
            .name(prepared.name)
            .fleet_name(prepared.fleet_name)
            .workers(prepared.workers)
            .launch(prepared.envelope)
            .config_dir(config_dir),
    )
    .with_definitions(move |state| {
        let definitions = definitions.inherited(state)?;
        runner::supervisor::register_policy(
            state,
            definitions.runner.supervisor.unwrap_or_default(),
        )
    })
    .register(move |state| -> registry::Result<()> {
        let definition = definition.inherited(state)?;
        let runner = &definition.runner;
        launch::register_routing(state, runner, std::path::Path::new("."))?;
        runner::register_owner(state, Owner::new(Router::new()))?;
        claviron::bench::runner::BenchService::register(state)?;
        runner::ensure_application(state)?;
        launch::register_trust(state, runner)?;
        launch::register_doors(state, runner, "rust", None)
    });
    if options.check_config {
        server.check_config().await?;
    } else {
        server.serve().await?;
    }
    Ok(())
}
