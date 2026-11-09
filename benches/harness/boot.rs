//! Both sides use the public Claviron boot and worker re-exec contract.
use super::{
    Result,
    config::{Launch, Side},
};
use claviron::supervisor::ProcessEntry;

pub fn inherited() -> Result<Option<Launch>> {
    match ProcessEntry::detect()? {
        ProcessEntry::Worker(worker) => {
            Ok(Some(Launch::load(worker.config_dir())?))
        }
        ProcessEntry::Primary => {
            let mut args = std::env::args_os().skip(1);
            if args.next().as_deref()
                == Some(std::ffi::OsStr::new("--harness-config"))
            {
                let path =
                    args.next().ok_or("missing harness config directory")?;
                Ok(Some(Launch::load(std::path::Path::new(&path))?))
            } else {
                Ok(None)
            }
        }
    }
}

pub fn run(launch: Launch) -> Result<()> {
    let threads = match launch.side {
        Side::Client => launch.args.edge_threads,
        Side::Application => launch.args.application_threads,
    };
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads.get())
        .enable_all()
        .build()?
        .block_on(async move {
            match launch.side {
                Side::Application => {
                    let registration = launch.clone();
                    claviron::runner::Runner::new()
                        .with_config_path(launch.directory.join("runner.yaml"))
                        .with_router(|state| {
                            claviron::bench::routes_with_state(&state)
                        })
                        .with_bootstrap(move |state| {
                            super::service::register(state, registration)
                        })
                        .run()
                        .await?;
                }
                Side::Client => {
                    let registration = launch.clone();
                    claviron::Server::new()
                        .name(launch.name.parse::<claviron::ServerName>()?)
                        .fleet_name(
                            launch.fleet.parse::<claviron::FleetName>()?,
                        )
                        .config_dir(&launch.directory)
                        .workers(launch.args.client_workers)
                        .register(move |state| {
                            super::service::register(state, registration)
                        })
                        .serve()
                        .await?;
                }
            }
            Ok(())
        })
}
