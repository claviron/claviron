//! One application composed on a server: the launch every runner makes alike.
//!
//! A language runner says what its owner is and what belongs beside it; the
//! order is fixed here. The launch is checked and handed to the workers, each
//! process serves what its master resolved, the owner comes first, what stands
//! in front of it next, the application's own registrations in front of those,
//! and the doors last.

use std::path::{Path, PathBuf};

use claviron_runner::launch::{self, Launch};
use claviron_runner::{Owner, ensure_application, register_owner};
use claviron_runtime::{AppState, ServerName, registry};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::Server;

type Step<O> = Box<
    dyn FnOnce(&AppState, &Launch<O>) -> registry::Result<()> + Send + 'static,
>;
type OwnerStep<O> =
    Box<dyn FnOnce(&AppState, &Launch<O>) -> registry::Result<Owner> + Send>;
type Definitions<O> = Box<
    dyn FnOnce(&AppState, Launch<O>) -> registry::Result<()> + Send + 'static,
>;
type RootStep<O> = Box<dyn FnOnce(&Launch<O>) -> PathBuf + Send>;

/// A runner's launch on its way to a [`Server`].
pub struct Composition<O> {
    launch: Launch<O>,
    name: String,
    runtime: &'static str,
    default_in_flight: Option<usize>,
    config_dir: PathBuf,
    config_file: Option<PathBuf>,
    system_log: Option<Option<crate::logging::SystemLogConfig>>,
    definitions: Option<Definitions<O>>,
    owner: OwnerStep<O>,
    root: Option<RootStep<O>>,
    configure: Vec<Step<O>>,
    settled: Vec<Step<O>>,
}

impl<O> Composition<O>
where
    O: Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    /// `runtime` is what the Orbit lane says answers `name` (`php`, `asgi`).
    /// `owner` runs first in every process, with the launch that process
    /// serves; what it returns answers what nothing in front of it claims.
    pub fn new<F>(
        launch: Launch<O>,
        name: impl Into<String>,
        runtime: &'static str,
        owner: F,
    ) -> Self
    where
        F: FnOnce(&AppState, &Launch<O>) -> registry::Result<Owner>
            + Send
            + 'static,
    {
        Self {
            launch,
            name: name.into(),
            runtime,
            default_in_flight: None,
            config_dir: PathBuf::from("."),
            config_file: None,
            system_log: None,
            definitions: None,
            owner: Box::new(owner),
            root: None,
            configure: Vec::new(),
            settled: Vec::new(),
        }
    }

    /// The bound on requests in flight on the Orbit lane when the launch names
    /// none.
    pub fn default_in_flight(
        mut self,
        limit: Option<usize>,
    ) -> Self {
        self.default_in_flight = limit;
        self
    }

    /// Where the launch file is written: its neighbours resolve against this.
    pub fn config_path(
        mut self,
        path: &Path,
    ) -> Self {
        self.config_file = Some(path.to_owned());
        self.config_dir = path
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."))
            .to_owned();
        self
    }

    /// Say the system log's policy here rather than in `logging.yaml`; `None`
    /// is no file. Not calling this keeps reading it from the directory.
    pub fn system_log(
        mut self,
        log: Option<crate::logging::SystemLogConfig>,
    ) -> Self {
        self.system_log = Some(log);
        self
    }

    /// What the launch defines for the whole fleet, in the master too, after
    /// its supervisor policy.
    pub fn definitions<F>(
        mut self,
        define: F,
    ) -> Self
    where
        F: FnOnce(&AppState, Launch<O>) -> registry::Result<()>
            + Send
            + 'static,
    {
        self.definitions = Some(Box::new(define));
        self
    }

    /// The root static mounts are anchored at. The working directory unless
    /// the launch says where the application is.
    pub fn root<F>(
        mut self,
        root: F,
    ) -> Self
    where
        F: FnOnce(&Launch<O>) -> PathBuf + Send + 'static,
    {
        self.root = Some(Box::new(root));
        self
    }

    /// The application's own registrations, in order, in front of the owner.
    pub fn configure<F>(
        mut self,
        configure: F,
    ) -> Self
    where
        F: FnOnce(&AppState, &Launch<O>) -> registry::Result<()>
            + Send
            + 'static,
    {
        self.configure.push(Box::new(configure));
        self
    }

    /// Work that needs the composed application, before any door opens.
    pub fn settled<F>(
        mut self,
        settled: F,
    ) -> Self
    where
        F: FnOnce(&AppState, &Launch<O>) -> registry::Result<()>
            + Send
            + 'static,
    {
        self.settled.push(Box::new(settled));
        self
    }

    fn server(
        self
    ) -> crate::Result<
        Server<impl FnOnce(&AppState) -> registry::Result<()> + Send + 'static>,
    > {
        let Self {
            mut launch,
            name,
            runtime,
            default_in_flight,
            config_dir,
            config_file,
            system_log,
            definitions,
            owner,
            root,
            configure,
            settled,
        } = self;
        // What a lane is addressed by; a process with no lane has no such name.
        if launch.runner.link_enabled() {
            claviron_orbit::link::application::segment::check_application_name(
                &name,
            )
            .map_err(registry_error)?;
        }
        let server_name: ServerName = name.parse().map_err(registry_error)?;
        let production = launch.runner.production;
        let environment = launch.runner.environment.clone();
        let prepared = launch.prepare(name)?;
        let inherited = launch.clone();
        // Production only by `production`; otherwise `environment`, and without
        // either, `local`.
        let server = match (production, environment) {
            (true, _) => Server::new().production(),
            (false, Some(environment)) => {
                Server::new().environment(environment)
            }
            (false, None) => Server::new(),
        };
        #[cfg(feature = "supervisor")]
        let server = server.workers(prepared.workers);
        let server = match system_log {
            Some(log) => server.system_log(log),
            None => server,
        };
        Ok(server
            .name(server_name)
            .fleet_name(prepared.fleet_name)
            .launch(prepared.envelope)
            .config_dir(config_dir)
            .with_definitions(move |state| {
                let launch = inherited.inherited(state)?;
                #[cfg(feature = "runner-orbit")]
                claviron_orbit::config::OrbitConfiguration::runner_source(
                    state,
                    config_file
                        .clone()
                        .unwrap_or_else(|| PathBuf::from("runner.yaml")),
                    launch
                        .runner
                        .orbit_config()
                        .map_err(registry::Error::msg)?
                        .unwrap_or_default(),
                    default_in_flight,
                )?;
                #[cfg(not(feature = "runner-orbit"))]
                let _ = &config_file;
                #[cfg(feature = "supervisor")]
                claviron_runner::supervisor::register_policy(
                    state,
                    launch.runner.supervisor.clone().unwrap_or_default(),
                )?;
                match definitions {
                    Some(define) => define(state, launch),
                    None => Ok(()),
                }
            })
            .register(move |state| {
                // A worker serves what its master resolved.
                let launch = launch.inherited(state)?;
                let owner = owner(state, &launch)?;
                register_owner(state, owner)?;
                let root = match root {
                    Some(root) => root(&launch),
                    None => std::env::current_dir()?,
                };
                launch::register_routing(state, &launch.runner, &root)?;
                for configure in configure {
                    configure(state, &launch)?;
                }
                ensure_application(state)?;
                launch::register_trust(state, &launch.runner)?;
                for settled in settled {
                    settled(state, &launch)?;
                }
                launch::register_doors(
                    state,
                    &launch.runner,
                    runtime,
                    default_in_flight,
                )
            }))
    }

    pub async fn serve(self) -> crate::Result<()> {
        self.server()?.serve().await
    }

    /// Check the configuration without starting an application or a door.
    pub async fn check_config(self) -> crate::Result<()> {
        self.server()?.check_config().await
    }
}

fn registry_error(error: impl std::fmt::Display) -> registry::Error {
    registry::Error::msg(error.to_string())
}

/// The launch a worker was started with, for a runner reading its own command
/// line: a worker has none, its master resolved it. `None` in any other process.
#[cfg(feature = "supervisor")]
pub fn worker_launch<O: DeserializeOwned>() -> crate::Result<Option<Launch<O>>>
{
    match claviron_supervisor::ProcessEntry::detect()? {
        claviron_supervisor::ProcessEntry::Worker(worker) => {
            Ok(Some(serde_json::from_slice(worker.launch().as_bytes())?))
        }
        _ => Ok(None),
    }
}
