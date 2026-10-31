//! Process bootstrap and the Continuo lifecycle host.
//!
//! This is where the crates under `crates/` are composed into a running
//! process, and it is the only part of that composition the outside reaches:
//! `claviron::server`. It lived in a crate of its own until it turned out that
//! a crate buys nothing here and costs a second copy of every feature — each
//! one declared in the facade, forwarded, and declared again beside the same
//! optional dependency.

use std::path::PathBuf;
use std::sync::Arc;

use claviron_logging as logging;
use claviron_runtime::{
    AppState, DEFAULT_FLEET_NAME, FleetCapacity, FleetIncarnation, FleetName,
    FleetNodeId, LaunchBlob, RunMode, ServerName, WorkerCount,
};
#[cfg(feature = "supervisor")]
use claviron_supervisor::ProcessEntry;
use tracing::debug;

mod error;
pub use error::{Error, Result};

pub mod bootstrap;

mod instance;
#[cfg(not(feature = "supervisor"))]
mod signals;
#[cfg(test)]
#[path = "../../tests/server/options.rs"]
mod tests;
use claviron_runtime::registry;

type DefinitionRegistration =
    Box<dyn FnOnce(&AppState) -> registry::Result<()> + Send>;

fn chain_registration<S, R, N>(
    previous: R,
    next: N,
) -> impl FnOnce(&S) -> registry::Result<()>
where
    R: FnOnce(&S) -> registry::Result<()>,
    N: FnOnce(&S) -> registry::Result<()>,
{
    move |state| {
        previous(state)?;
        next(state)
    }
}

/// Typed process-family options for a Claviron server composition.
///
/// This type deliberately does not parse CLI arguments. Embedding
/// applications may fill it from their own CLI, config, installer defaults, or
/// code. Worker processes ignore these primary-entry values and use the
/// supervisor's validated internal bootstrap envelope instead.
/// Which `.env.<environment>` a primary process selects, and so whether it
/// is in production. A launch script or service manager supplies it.
#[derive(Clone, Debug, Default)]
enum EnvironmentChoice {
    /// [`claviron_runtime::env::LOCAL`]: nothing was selected.
    #[default]
    Local,
    /// A named environment. Never `production`.
    Named(String),
    /// [`claviron_runtime::env::PRODUCTION`], chosen only explicitly.
    Production,
}

#[derive(Clone, Debug)]
pub struct ServerOptions {
    config_dir: PathBuf,
    name: Option<ServerName>,
    fleet_name: FleetName,
    environment: EnvironmentChoice,
    #[cfg(feature = "supervisor")]
    worker_count: u8,
    launch: LaunchBlob,
    signals: claviron_sys::signal::ServerSignals,
    system_log: Option<Option<logging::SystemLogConfig>>,
}

impl Default for ServerOptions {
    fn default() -> Self {
        Self {
            config_dir: PathBuf::from("config"),
            name: None,
            fleet_name: FleetName::from(DEFAULT_FLEET_NAME),
            environment: EnvironmentChoice::Local,
            #[cfg(feature = "supervisor")]
            worker_count: 0,
            launch: LaunchBlob::default(),
            signals: claviron_sys::signal::ServerSignals::default(),
            system_log: None,
        }
    }
}

impl ServerOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn config_dir(
        mut self,
        config_dir: impl Into<PathBuf>,
    ) -> Self {
        self.config_dir = config_dir.into();
        self
    }

    pub fn name(
        mut self,
        name: impl Into<ServerName>,
    ) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Select the same-host Orbit sharing domain. Process instance names are
    /// independent: multiple Claviron compositions may intentionally join the
    /// same fleet when their node ids come from one allocator.
    pub fn fleet_name(
        mut self,
        fleet_name: impl Into<FleetName>,
    ) -> Self {
        self.fleet_name = fleet_name.into();
        self
    }

    /// Select `.env.<environment>` after `.env`. Without this or
    /// [`production`](Self::production) the environment is
    /// [`local`](claviron_runtime::env::LOCAL). `production` cannot be named
    /// here: production is only ever chosen explicitly, so a typo cannot
    /// select it. This immutable choice is carried to every worker.
    pub fn environment(
        mut self,
        environment: impl Into<String>,
    ) -> Self {
        self.environment = EnvironmentChoice::Named(environment.into());
        self
    }

    /// Run in production: select `.env.production` after `.env`, and make
    /// [`AppState::is_production`](claviron_runtime::AppState::is_production)
    /// true in this process and its workers. A deployment's service manager
    /// passes this; nothing infers it.
    pub fn production(mut self) -> Self {
        self.environment = EnvironmentChoice::Production;
        self
    }

    /// The environment name this primary process runs under.
    fn resolved_environment(&self) -> registry::Result<String> {
        use claviron_runtime::env::PRODUCTION;
        match &self.environment {
            EnvironmentChoice::Production => Ok(PRODUCTION.to_owned()),
            EnvironmentChoice::Named(name)
                if name.eq_ignore_ascii_case(PRODUCTION) =>
            {
                Err(registry::Error::msg(format!(
                    "the {PRODUCTION} environment is chosen explicitly (--production), never by name"
                )))
            }
            EnvironmentChoice::Named(name) => Ok(name.clone()),
            EnvironmentChoice::Local => {
                Ok(claviron_runtime::env::LOCAL.to_owned())
            }
        }
    }

    /// What this launch decided, for its workers to inherit.
    ///
    /// A worker is a re-exec, and the supervisor deliberately does not pass
    /// the command line along — an embedding binary may define any flags it
    /// likes, and the supervisor has no business parsing them. Whatever an
    /// application resolved before calling `serve` therefore has to travel in
    /// the bootstrap envelope, or every worker comes up on defaults nobody
    /// chose.
    ///
    /// It is carried and never read: encode it however suits the application
    /// and decode it from [`claviron::runtime::LaunchBlob`] in the worker.
    ///
    /// **A launch, not a secret.** The envelope travels in the environment,
    /// where any process of the same user can read it.
    pub fn launch(
        mut self,
        launch: impl Into<Vec<u8>>,
    ) -> Self {
        self.launch = LaunchBlob::new(launch);
        self
    }

    /// Configure the initial desired number of worker children. Zero means
    /// standalone. The fleet capacity is not an option: the build pins it
    /// (`CLAVIRON_FLEET_CAPACITY`) beside the ring geometry it must match.
    #[cfg(feature = "supervisor")]
    pub fn workers(
        mut self,
        worker_count: u8,
    ) -> Self {
        self.worker_count = worker_count;
        self
    }

    /// Which signal asks for a reload, a log reopen and a worker restart.
    ///
    /// The binary's default; the defaults are the common convention (HUP,
    /// USR1, USR2). With the supervisor, `supervisor.yaml`'s `signals`, when
    /// written, is the operator's and wins — read at start, before the
    /// listeners go in, so a change takes effect on a restart. Only `hup`,
    /// `usr1` and `usr2` may be chosen, each for one role. Workers do not need
    /// to know the choice: they ignore every operator signal, and a log reopen
    /// reaches them from the master as a fleet event.
    pub fn signals(
        mut self,
        signals: claviron_sys::signal::ServerSignals,
    ) -> Self {
        self.signals = signals;
        self
    }

    fn validate_primary(&self) -> registry::Result<()> {
        claviron_runtime::env::ConfigEnvironment::validate_environment(
            &self.resolved_environment()?,
        )
        .map_err(|error| registry::Error::msg(error.to_string()))?;
        self.signals.validate().map_err(registry::Error::msg)?;
        if self.config_dir.as_os_str().is_empty() {
            return Err(registry::Error::msg(
                "server config directory must not be empty",
            ));
        }
        Ok(())
    }

    /// Prepare a server builder. No configuration is read and no service runs.
    pub fn build(self) -> Server {
        Server::with_options(self)
    }
}

/// A Claviron process, from options to a running lifecycle.
pub struct Server<R = fn(&AppState) -> registry::Result<()>> {
    options: ServerOptions,
    register: R,
    definitions: Vec<DefinitionRegistration>,
    #[cfg(feature = "edge")]
    edge_extensions: Vec<
        Arc<
            dyn claviron_edge::pipeline::routing::RouteExtension<
                    claviron_edge::config::HostConfig,
                >,
        >,
    >,
}

impl Default for Server {
    fn default() -> Self {
        Self {
            options: ServerOptions::default(),
            register: |_| Ok(()),
            definitions: Vec::new(),
            #[cfg(feature = "edge")]
            edge_extensions: Vec::new(),
        }
    }
}

impl Server {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn with_options(options: ServerOptions) -> Self {
        Server::new().options(options)
    }
}

impl<R> Server<R>
where
    R: FnOnce(&AppState) -> registry::Result<()>,
{
    /// Supply system-log policy directly. `None` disables the file and avoids
    /// reading `logging.yaml`; omitting this method retains directory loading.
    pub fn system_log(
        mut self,
        config: Option<logging::SystemLogConfig>,
    ) -> Self {
        self.options.system_log = Some(config);
        self
    }

    /// Register explicitly configured definitions before built-in defaults.
    /// Built-in PHP, broadcast and log registrations retain those definitions.
    /// No provider is booted here; ordinary application registrations still run
    /// after the built-in composition.
    pub fn with_definitions<F>(
        mut self,
        register: F,
    ) -> Self
    where
        F: FnOnce(&AppState) -> registry::Result<()> + Send + 'static,
    {
        self.definitions.push(Box::new(register));
        self
    }
    pub fn options(
        mut self,
        options: ServerOptions,
    ) -> Self {
        self.options = options;
        self
    }

    /// Enable a typed route directive while preparing edge snapshots.
    /// Compiling an extension feature alone never registers its config option.
    #[cfg(feature = "edge")]
    pub fn with_edge_extension<E>(
        mut self,
        extension: E,
    ) -> Self
    where
        E: claviron_edge::pipeline::routing::RouteExtension<
                claviron_edge::config::HostConfig,
            > + 'static,
    {
        self.edge_extensions.push(Arc::new(extension));
        self
    }

    pub fn config_dir(
        mut self,
        config_dir: impl Into<PathBuf>,
    ) -> Self {
        self.options = self.options.config_dir(config_dir);
        self
    }

    pub fn environment(
        mut self,
        environment: impl Into<String>,
    ) -> Self {
        self.options = self.options.environment(environment);
        self
    }

    /// Run in production; see [`ServerOptions::production`].
    pub fn production(mut self) -> Self {
        self.options = self.options.production();
        self
    }

    pub fn name(
        mut self,
        name: impl Into<ServerName>,
    ) -> Self {
        self.options = self.options.name(name);
        self
    }

    pub fn fleet_name(
        mut self,
        fleet_name: impl Into<FleetName>,
    ) -> Self {
        self.options = self.options.fleet_name(fleet_name);
        self
    }

    /// See [`ServerOptions::workers`].
    #[cfg(feature = "supervisor")]
    pub fn workers(
        mut self,
        worker_count: u8,
    ) -> Self {
        self.options = self.options.workers(worker_count);
        self
    }

    /// See [`ServerOptions::signals`].
    pub fn signals(
        mut self,
        signals: claviron_sys::signal::ServerSignals,
    ) -> Self {
        self.options = self.options.signals(signals);
        self
    }

    /// See [`ServerOptions::launch`].
    pub fn launch(
        mut self,
        launch: impl Into<Vec<u8>>,
    ) -> Self {
        self.options = self.options.launch(launch);
        self
    }

    /// Append application-owned registrations before configuration and boot.
    ///
    /// Callbacks run in declaration order after built-in registration, in each
    /// process role. A failure prevents later callbacks from running; completed
    /// registrations are not rolled back. This does not start services.
    pub fn register<N>(
        self,
        register: N,
    ) -> Server<impl FnOnce(&AppState) -> registry::Result<()>>
    where
        N: FnOnce(&AppState) -> registry::Result<()>,
    {
        Server {
            options: self.options,
            register: chain_registration(self.register, register),
            definitions: self.definitions,
            #[cfg(feature = "edge")]
            edge_extensions: self.edge_extensions,
        }
    }

    /// Append statically typed service registrations, including tuples.
    ///
    /// Each type implements [`claviron_runtime::Register<AppState>`] and owns
    /// how its provider instances are constructed; `Default` is not required.
    /// Dependency ordering, configuration and execution remain lifecycle work.
    pub fn with_services<C>(
        self
    ) -> Server<impl FnOnce(&AppState) -> registry::Result<()>>
    where
        C: claviron_runtime::Register<AppState>,
    {
        self.register(C::register)
    }

    pub fn initialize(self) -> registry::Result<AppState> {
        self.initialize_as(false)
    }

    /// [`Self::initialize`] for `--check-config`: the same process, the same
    /// providers, but no log file is opened. The check may run as another
    /// user than the service — a deploy checks new configuration before
    /// installing it — and must neither create the service's files nor fail
    /// because it cannot write them; `logging.yaml` is read and checked.
    pub fn initialize_for_check(self) -> registry::Result<AppState> {
        self.initialize_as(true)
    }

    fn initialize_as(
        self,
        check: bool,
    ) -> registry::Result<AppState> {
        // A primary process's own choice; a worker takes its master's.
        let primary_environment = self.options.resolved_environment();
        #[cfg(feature = "supervisor")]
        let entry = ProcessEntry::detect()
            .map_err(|err| registry::Error::msg(err.to_string()))?;
        // A worker keeps the identity its master handed it: what it publishes
        // about itself, and what a drain request must name before it acts.
        #[cfg(feature = "supervisor")]
        let worker_identity = match &entry {
            ProcessEntry::Worker(worker) => Some(
                claviron_supervisor::WorkerIdentityProvider::from_bootstrap(
                    worker,
                )
                .map_err(|err| registry::Error::msg(err.to_string()))?,
            ),
            ProcessEntry::Primary => None,
        };
        // The master's count of this slot's replacements. Orbit stamps its
        // tables with it and matches death reports against it, so a process
        // must carry the number it was handed rather than derive one.
        #[cfg(feature = "supervisor")]
        let fleet_incarnation = match &entry {
            ProcessEntry::Worker(worker) => {
                FleetIncarnation::new(worker.incarnation())
            }
            ProcessEntry::Primary => FleetIncarnation::FIRST,
        };
        // A worker takes the launch its master resolved; the primary takes
        // the one it was given. Read here, once, while the environment is
        // still the environment — nothing looks at it again.
        #[cfg(feature = "supervisor")]
        let launch = match &entry {
            ProcessEntry::Worker(worker) => worker.launch().clone(),
            ProcessEntry::Primary => self.options.launch.clone(),
        };
        #[cfg(not(feature = "supervisor"))]
        let launch = self.options.launch.clone();
        #[cfg(feature = "supervisor")]
        let (
            run_mode,
            fleet_node_id,
            worker_count,
            fleet_capacity,
            instance_name,
            fleet_name,
            config_dir,
            environment,
        ) = match entry {
            ProcessEntry::Primary => {
                self.options.validate_primary()?;

                // A check never takes the master's role: it spawns and adopts
                // no workers, and must not read the live fleet's as its own.
                let worker_count = WorkerCount::new(if check {
                    0
                } else {
                    self.options.worker_count
                });
                // Lane geometry is the build's: `CLAVIRON_FLEET_CAPACITY`
                // beside the ring sizes, or the runtime's default.
                let fleet_capacity = FleetCapacity::BUILT;
                if !fleet_capacity.admits(worker_count) {
                    return Err(registry::Error::msg(format!(
                        "worker count {} exceeds fleet worker capacity {}",
                        worker_count.raw(),
                        fleet_capacity.worker_capacity()
                    )));
                }
                // Standalone mode is related ONLY with worker count, run mode can be shifted during in different runs, Standalone detection must be regardless of Fleet Capacity
                // When Fleet Capacity is changed, all shm paths must be re-created
                let run_mode = if worker_count.is_zero() {
                    RunMode::Standalone
                } else {
                    RunMode::Master
                };
                (
                    run_mode,
                    FleetNodeId::ZERO,
                    worker_count,
                    fleet_capacity,
                    self.options.name.map(Arc::from),
                    Arc::from(self.options.fleet_name),
                    self.options.config_dir,
                    primary_environment?,
                )
            }
            ProcessEntry::Worker(worker) => (
                worker.run_mode(),
                worker.fleet_node_id(),
                worker.worker_count(),
                worker.fleet_capacity(),
                Some(Arc::from(worker.instance_name())),
                Arc::from(worker.fleet_name()),
                worker.config_dir().to_path_buf(),
                worker.environment().to_owned(),
            ),
        };

        #[cfg(not(feature = "supervisor"))]
        let fleet_incarnation = FleetIncarnation::FIRST;
        #[cfg(not(feature = "supervisor"))]
        let (
            run_mode,
            fleet_node_id,
            worker_count,
            fleet_capacity,
            instance_name,
            fleet_name,
            config_dir,
            environment,
        ) = {
            self.options.validate_primary()?;
            (
                RunMode::Standalone,
                FleetNodeId::ZERO,
                WorkerCount::ZERO,
                FleetCapacity::BUILT,
                self.options.name.map(Arc::from),
                Arc::from(self.options.fleet_name),
                self.options.config_dir,
                primary_environment?,
            )
        };

        let hostname = claviron_sys::short_hostname();
        // Logger transport detection uses inherited process metadata, not a
        // dotenv-controlled JOURNAL_STREAM/INVOCATION_ID. File policy is the
        // same under systemd and a direct launch, with no process-env writes.
        let under_systemd = logging::is_running_under_systemd();
        let config_environment =
            claviron_runtime::env::ConfigEnvironment::load(
                &hostname,
                &environment,
                std::path::Path::new(".env"),
            )
            .map_err(|error| registry::Error::msg(error.to_string()))?;
        let system_log = |config_dir: &std::path::Path| {
            if let Some(config) = &self.options.system_log {
                let Some(config) = config else { return Ok(None) };
                let config = config
                    .resolve(|key| config_environment.snapshot().get(key))?;
                return if check {
                    config.validate().map(|()| None)
                } else {
                    logging::SystemLog::from_configuration(config, run_mode)
                        .map(Some)
                };
            }
            if check {
                logging::SystemLog::check_config_dir(config_dir, |key| {
                    config_environment.snapshot().get(key)
                })
                .map(|()| None)
            } else {
                logging::SystemLog::from_config_dir_for_startup(
                    config_dir,
                    |key| config_environment.snapshot().get(key),
                    run_mode,
                )
            }
        };
        let rust_log = config_environment.snapshot().get("RUST_LOG");
        if under_systemd {
            logging::journald_logger_with_filter(
                run_mode,
                system_log(&config_dir)?,
                rust_log.as_deref(),
            )?;
        } else {
            logging::console_logger_with_filter(
                run_mode,
                system_log(&config_dir)?,
                rust_log.as_deref(),
            )?;
        }

        let state = AppState::initialize_with_environment_in_fleet(
            run_mode,
            fleet_node_id,
            worker_count,
            fleet_capacity,
            instance_name,
            fleet_name,
            hostname,
            config_dir,
            config_environment,
        );

        #[cfg(feature = "supervisor")]
        if let Some(identity) = worker_identity {
            state.registry_ref().insert(Arc::new(identity))?;
        }
        if check {
            state
                .registry_ref()
                .insert(Arc::new(claviron_runtime::ConfigCheck))?;
        }
        state.registry_ref().insert(Arc::new(fleet_incarnation))?;
        state.registry_ref().insert(Arc::new(launch))?;

        debug!(
            role = %run_mode.as_process_label(),
            pid = std::process::id(),
            instance = state.name(),
            fleet = state.fleet_name(),
            config_dir = %state.config_dir().display(),
            environment = state.environment(),
            "🆔 Claviron process starting; registering providers"
        );

        for register in self.definitions {
            register(&state)?;
        }
        bootstrap::register(&state)?;
        #[cfg(feature = "edge")]
        {
            let registry = state
                .registry_ref()
                .resolve::<claviron_edge::HostAppRegistry>()
                .ok_or_else(|| {
                    registry::Error::msg("edge registry is not registered")
                })?;
            for extension in self.edge_extensions {
                registry.register_route_extension(extension)?;
            }
        }
        (self.register)(&state)?;

        Ok(state)
    }

    /// `--check-config`: read the configuration the way `serve` would and
    /// report whether it holds, running nothing.
    ///
    /// Every provider is registered and booted — booting is where most of
    /// them read their files — and then validated. Nothing is run: no
    /// runnable starts, no listener binds, no worker is spawned, and nothing
    /// is finalized, because nothing was taken that needs giving back. The
    /// process is a configuration check ([`claviron_runtime::ConfigCheck`]):
    /// its fleet lives in its own memory and it opens no shared memory, so it
    /// can run beside the live service, as its user, and leave it untouched.
    pub async fn check_config(self) -> crate::Result<()> {
        let state = self.initialize_for_check().map_err(Error::from)?;
        let registry = state.registry_ref();
        // Providers resolve the runtime's control handle while booting, as
        // they do under `serve`; the runtime itself never spawns anything here.
        let mut runtime = claviron_runtime::Runtime::default();
        registry.insert(runtime.manager()).map_err(Error::from)?;
        runtime = bootstrap::with_http_services(runtime);
        claviron_orbit::service::register(&state).map_err(Error::from)?;
        runtime.configure(&state).await.map_err(Error::from)?;
        registry.boot_all(&state).await.map_err(Error::from)?;
        let validation = registry.validate_all(&state).map_err(Error::from)?;
        for failure in validation.failures() {
            tracing::error!(
                component = failure.component(),
                error = %failure.error(),
                "provider '{}' validation failed: {}",
                failure.component(),
                failure.error()
            );
        }
        if validation.is_valid() {
            tracing::info!("configuration is valid");
            Ok(())
        } else {
            Err(registry::Error::msg(format!(
                "{} provider validation(s) failed",
                validation.failed_count()
            ))
            .into())
        }
    }

    pub async fn serve(self) -> crate::Result<()> {
        let signals = self.options.signals;
        let state = self.initialize().map_err(Error::from)?;
        bootstrap::start_services(state, signals).await?;
        Ok(())
    }
}
