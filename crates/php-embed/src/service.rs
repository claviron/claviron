use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::{Arc, Mutex as StdMutex, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use claviron_app::environment::EnvironmentContext;
use claviron_http::{HttpExchange, RouteBody, RouteResponse};
use claviron_php::PhpRuntime;
use claviron_runtime::AppState;
use claviron_runtime::registry::{self, Bootable, Component, ComponentOrder};
use http::Request;
use tokio::sync::mpsc;

use crate::config::{
    PhpApplication, PhpEventConfig, PhpLaunchConfig, PhpManagementConfig,
    RequestPool,
};
use crate::engine::{PhpSapiEngine, PhpSapiRequest, SapiThreadPoolSnapshot};
use crate::error::SapiGatewayError;
#[cfg(any(feature = "apcu-intercept", feature = "opcache-intercept"))]
use crate::invalidation::bus;

mod application;
#[cfg(feature = "tasks")]
pub(crate) mod invoke;
pub(crate) use application::ApplicationCommandAction;
pub use application::{
    PhpApplicationLifecycle, PhpApplicationService, PhpApplicationStatus,
};
mod script;

pub use script::PhpScriptService;

// PHP module state and host bridges cannot be reinitialized independently.
// Keep the identity alive even after a service is dropped: only a process
// restart can create a different embedded runtime safely.
static PROCESS_OWNER: OnceLock<Arc<()>> = OnceLock::new();

#[cfg(feature = "cache")]
type CacheDependency = Option<Arc<claviron_cache::CacheProvider>>;
#[cfg(not(feature = "cache"))]
type CacheDependency = ();
#[cfg(any(feature = "postgres", feature = "mysql"))]
type DatabaseDependency = Option<Arc<claviron_db::DbService>>;
#[cfg(not(any(feature = "postgres", feature = "mysql")))]
type DatabaseDependency = ();
#[cfg(feature = "tasks")]
type InvocationDependency = Arc<claviron_orbit::InvocationProvider>;
#[cfg(not(feature = "tasks"))]
type InvocationDependency = ();
#[cfg(any(feature = "apcu-intercept", feature = "opcache-intercept"))]
type InvalidationDependency = Option<Arc<bus::PhpInvalidationBusService>>;
#[cfg(not(any(feature = "apcu-intercept", feature = "opcache-intercept")))]
type InvalidationDependency = ();
#[cfg(feature = "log-pipeline")]
type LoggingDependency = Option<Arc<claviron_logging::pipeline::LogChannels>>;
#[cfg(not(feature = "log-pipeline"))]
type LoggingDependency = ();

/// Explicit PHP capabilities for one serving process. Constructing this builder
/// does not read configuration or start PHP. An empty builder has no PHP threads.
#[derive(Clone, Debug, Default)]
pub struct PhpEmbedRuntimeBuilder {
    request_pool: Option<RequestPool>,
    script_application: Option<(String, claviron_app::ApplicationDescription)>,
    applications: Vec<PhpApplication>,
    ini_path: Option<PathBuf>,
    ini_scan_dir: Option<PathBuf>,
    graceful_timeout: Option<Duration>,
    execution_timeout: Option<Duration>,
    interruptible_sleep: bool,
    cache_invalidation: Option<bool>,
    management: PhpManagementConfig,
    events: PhpEventConfig,
    launch: PhpLaunchConfig,
    #[cfg(feature = "metrics")]
    metrics: crate::metrics::PhpEmbedMetricsConfig,
}

impl PhpEmbedRuntimeBuilder {
    /// Add an application with an independent persistent pool in each worker.
    pub fn application(
        mut self,
        application: PhpApplication,
    ) -> Self {
        self.applications.push(application);
        self
    }
    pub fn request_pool(
        mut self,
        pool: RequestPool,
    ) -> Self {
        self.request_pool = Some(pool);
        self
    }

    /// Introduce a runner's sole script pool through application management.
    /// Unnamed edge request pools do not acquire an application identity.
    pub fn script_application(
        mut self,
        name: impl Into<String>,
        mut description: claviron_app::ApplicationDescription,
    ) -> Self {
        description.runtime = Some("php".to_owned());
        description.request_mode = Some("script".to_owned());
        self.script_application = Some((name.into(), description));
        self
    }

    /// Observe APCu/OPcache invalidations and propagate to peer workers.
    /// Defaults to enabled. This is shared SAPI policy for every PHP execution
    /// model, applied at process boot; pool restart cannot change it.
    pub fn cache_invalidation(
        mut self,
        enabled: bool,
    ) -> Self {
        self.cache_invalidation = Some(enabled);
        self
    }

    /// Process-wide PHP INI location; all programs share one SAPI module.
    pub fn ini_path(
        mut self,
        path: impl Into<PathBuf>,
    ) -> Self {
        self.ini_path = Some(path.into());
        self
    }

    /// Process-wide directory of additional PHP INI files.
    pub fn ini_scan_dir(
        mut self,
        path: impl Into<PathBuf>,
    ) -> Self {
        self.ini_scan_dir = Some(path.into());
        self
    }

    /// The bound on cooperative drain. It is not an execution timeout; see
    /// [`execution_timeout`](Self::execution_timeout) for that.
    ///
    /// `None` waits indefinitely and is never overridden — that is
    /// `GRACEFUL_OWNERSHIP: NO_IMPLICIT_TERMINATION` in
    /// `claviron-runtime/ARCHITECTURE.md`, and no default, fallback or helper
    /// may turn it into a duration.
    ///
    /// `Some(d)` authorises this owner to abandon **or force** the work it
    /// accepted once `d` has passed — the architecture's words. For embedded
    /// PHP, forcing means asking Zend to end the execution through its own
    /// timeout path, scoped to this service's generation and to threads
    /// actually executing. The drain then allows a short window for the VM to
    /// unwind before reporting an incomplete drain, and never detaches the
    /// handles.
    ///
    /// An earlier version of this doc said expiry "reports failure; it cannot
    /// safely kill an OS thread inside PHP". The first half is still true when
    /// the interrupt does not land — a script blocked in a native call never
    /// reaches a VM interrupt point — but the second half stopped being true
    /// when the interrupt landed, and the two had disagreed since.
    pub fn graceful_timeout(
        mut self,
        timeout: Option<Duration>,
    ) -> Self {
        self.graceful_timeout = timeout;
        self
    }

    /// How long one PHP execution may run before the runtime ends it.
    ///
    /// What counts as "one execution" differs by model, and the difference is
    /// part of the contract: a script request, an application request or event,
    /// and — for a task — the whole program, because a task's internal loop
    /// iteration is not something Rust can see start or end. `engine::interrupt`
    /// states all three, and why `set_time_limit()` does not move a bound the
    /// host imposed.
    ///
    /// Host-level because execution is host-owned: embedding PHP removed the
    /// per-process escape that `request_terminate_timeout` relies on, so the
    /// bound has to live where the threads do. `None` leaves executions
    /// unbounded, which is the historical behaviour and not a good default for
    /// anything serving requests.
    /// Let a cancelled program's `sleep()` return early, once.
    ///
    /// Off by default: this changes the behaviour of a PHP builtin for every
    /// program on the engine, and a library that leans on `sleep()` actually
    /// sleeping has no way to say otherwise. Worth it for a task that idles
    /// on a long sleep, which is the case it was written for.
    pub fn interruptible_sleep(
        mut self,
        enabled: bool,
    ) -> Self {
        self.interruptible_sleep = enabled;
        self
    }

    pub fn execution_timeout(
        mut self,
        timeout: Option<Duration>,
    ) -> Self {
        self.execution_timeout = timeout;
        self
    }

    /// Fleet publication and freshness policy for named applications.
    pub fn management(
        mut self,
        config: PhpManagementConfig,
    ) -> Self {
        self.management = config;
        self
    }

    pub fn launch(
        mut self,
        config: PhpLaunchConfig,
    ) -> Self {
        self.launch = config;
        self
    }

    /// Set the per-execution backlog bound shared by application and task subscriptions.
    pub fn events(
        mut self,
        config: PhpEventConfig,
    ) -> Self {
        self.events = config;
        self
    }

    #[cfg(feature = "metrics")]
    pub fn metrics(
        mut self,
        config: crate::metrics::PhpEmbedMetricsConfig,
    ) -> Self {
        self.metrics = config;
        self
    }

    /// Build without I/O. Registration validates these typed options.
    pub fn build(mut self) -> PhpEmbedRuntime {
        let definitions = ProgramDefinitions {
            applications: std::mem::take(&mut self.applications),
            frozen: false,
        };
        PhpEmbedRuntime {
            options: Arc::new(self),
            identity: Arc::new(()),
            engine: Arc::new(PhpSapiEngine::new()),
            booted: Arc::new(OnceLock::new()),
            environments: Arc::new(OnceLock::new()),
            definitions: Arc::new(StdMutex::new(definitions)),
        }
    }
}

/// One embedded PHP runtime with an optional request pool, PHP task executor,
/// and persistent application pools. The builder is configuration-free; [`crate::register`]
/// reads `php.yaml` and composes this same service internally.
/// It is not runnable: registration installs independent [`PhpSapiService`] and
/// [`PhpApplicationService`] runnables on workers and standalone processes, and
/// — with the `tasks` feature — the executor that runs `php:` tasks.
/// Downstream applications import these types through claviron::ephp.
/// One serving AppState/service owns PHP for the lifetime of the process; add
/// programs to that service as tasks, not as separate PHP engines.
///
/// ```no_run
/// use claviron_php_embed::{PhpApplication, PhpEmbedRuntime, RequestPool};
/// use claviron_runtime::AppState;
/// let state = AppState::standalone("example", ".");
/// let php = PhpEmbedRuntime::builder()
///     .request_pool(RequestPool::elastic().min_idle(2).max_size(32).max_idle(8))
///     .application(PhpApplication::new("web", "public/application.php"))
///     .build();
/// php.register(&state).unwrap();
/// ```
#[derive(Clone)]
pub struct PhpEmbedRuntime {
    options: Arc<PhpEmbedRuntimeBuilder>,
    identity: Arc<()>,
    engine: Arc<PhpSapiEngine>,
    booted: Arc<OnceLock<()>>,
    environments: Arc<OnceLock<EnvironmentContext>>,
    definitions: Arc<StdMutex<ProgramDefinitions>>,
}

struct ProgramDefinitions {
    applications: Vec<PhpApplication>,
    frozen: bool,
}

/// Typed lookup for application providers contributing PHP programs at boot.
/// Import this trait through `claviron::ephp::PhpEmbedStateExt` downstream.
pub trait PhpEmbedStateExt {
    fn php_embed(&self) -> registry::Result<Arc<PhpEmbedRuntime>>;
}

impl PhpEmbedStateExt for AppState {
    fn php_embed(&self) -> registry::Result<Arc<PhpEmbedRuntime>> {
        self.registry_ref().resolve::<PhpEmbedRuntime>().ok_or_else(|| {
            registry::Error::msg("PhpEmbedRuntime is not registered")
        })
    }
}

impl PhpEmbedRuntime {
    #[cfg(any(feature = "apcu-intercept", feature = "opcache-intercept"))]
    pub(crate) fn cache_invalidation_enabled(&self) -> bool {
        self.options
            .cache_invalidation
            .unwrap_or_else(crate::config::default_cache_invalidation)
    }

    /// Close registration so a later start cannot change the plan.
    fn freeze_definitions(&self) {
        self.definitions
            .lock()
            .expect("PHP program definitions lock poisoned")
            .frozen = true;
    }

    /// Launch policy, for the bridge that runs under this runtime.
    #[cfg(feature = "tasks")]
    pub(crate) fn launch_config(&self) -> &PhpLaunchConfig {
        &self.options.launch
    }

    pub(crate) fn ensure_booted(&self) -> registry::Result<()> {
        self.booted.get().map(|_| ()).ok_or_else(|| {
            registry::Error::msg(
                "PHP embed runtime must be booted before execution",
            )
        })
    }

    pub fn builder() -> PhpEmbedRuntimeBuilder {
        PhpEmbedRuntimeBuilder::default()
    }

    /// Whether a named persistent application has been registered.
    /// This reads definitions, not live pool readiness. Validate route targets
    /// after provider boot so boot-time application contributions are visible.
    pub fn has_application(
        &self,
        name: &str,
    ) -> bool {
        self.definitions
            .lock()
            .expect("PHP program definitions lock poisoned")
            .applications
            .iter()
            .any(|application| application.name == name)
    }

    /// Validate typed options without touching files or the provider registry.
    pub fn validate(&self) -> registry::Result<()> {
        if let Some(pool) = &self.options.request_pool {
            pool.validate().map_err(registry::Error::msg)?;
        }
        let definitions = self
            .definitions
            .lock()
            .expect("PHP program definitions lock poisoned");
        let mut applications = HashSet::new();
        for application in &definitions.applications {
            application.validate().map_err(registry::Error::msg)?;
            if !applications.insert(&application.name) {
                return Err(registry::Error::msg(format!(
                    "duplicate PHP application name: {}",
                    application.name
                )));
            }
        }
        if let Some((name, _)) = &self.options.script_application {
            if name.contains('\0') {
                return Err(registry::Error::msg(
                    "PHP application name cannot contain NUL bytes",
                ));
            }
            claviron_app::ApplicationName::new(name.clone())
                .map_err(|error| registry::Error::msg(error.to_string()))?;
            if self.options.request_pool.is_none() {
                return Err(registry::Error::msg(
                    "a named script application requires a request pool",
                ));
            }
            if applications.contains(name) {
                return Err(registry::Error::msg(
                    "script and persistent applications must have distinct names",
                ));
            }
        }
        for (name, path) in [
            ("ini_path", &self.options.ini_path),
            ("ini_scan_dir", &self.options.ini_scan_dir),
        ] {
            if let Some(path) = path
                && (path.as_os_str().is_empty()
                    || path.as_os_str().as_encoded_bytes().contains(&0))
            {
                return Err(registry::Error::msg(format!(
                    "PHP {name} must be a nonempty path without NUL bytes"
                )));
            }
        }
        #[cfg(feature = "metrics")]
        self.options.metrics.validate().map_err(registry::Error::msg)?;
        self.options.management.validate().map_err(registry::Error::msg)?;
        self.options.events.validate().map_err(registry::Error::msg)?;
        self.options.launch.validate().map_err(registry::Error::msg)?;
        Ok(())
    }

    /// Register an explicitly built service without file reads or starting PHP.
    /// For library-owned YAML and env-file loading use [`crate::register`].
    /// Installs the common provider everywhere, but execution services only on
    /// workers/standalone. Manage their stop/start independently by service name.
    pub fn register(
        self,
        state: &AppState,
    ) -> registry::Result<()> {
        self.validate()?;

        crate::engine::startup::declare(crate::engine::startup::PhpIni {
            path: self.options.ini_path.clone(),
            scan_dir: self.options.ini_scan_dir.clone(),
        })
        .map_err(registry::Error::msg)?;

        self.environments
            .set(
                EnvironmentContext::new(state)
                    .map_err(|error| registry::Error::msg(error.to_string()))?,
            )
            .map_err(|_| {
                registry::Error::msg("PHP environment context already prepared")
            })?;

        // Published here rather than in `build()`: a builder that is never
        // registered must not decide the process budget. See
        // `engine::interrupt::configure`.
        crate::engine::interrupt::configure(self.options.execution_timeout)
            .map_err(registry::Error::msg)?;
        crate::intercept::sleep::configure(self.options.interruptible_sleep);

        let registry = state.registry_ref();
        #[cfg(feature = "metrics")]
        let metrics = self.options.metrics;
        claviron_orbit::register_fleet(state)?;
        crate::functions::sapi_stats::register_shm(state)?;
        let runtime = Arc::new(self);
        registry.register(Arc::clone(&runtime))?;
        // Retain the reader handle, never a config copy. Preparation/reload
        // publishes through this same catalog before any hosted PHP reads it.
        crate::functions::config::configure_from_state(state);

        claviron_orbit::register_counter(state)?;
        claviron_orbit::register_cells(state)?;
        claviron_orbit::register_lock(state)?;
        claviron_orbit::register(state)?;
        #[cfg(any(feature = "apcu-intercept", feature = "opcache-intercept"))]
        if runtime.cache_invalidation_enabled() {
            bus::register(state)?;
        }
        #[cfg(feature = "tasks")]
        claviron_orbit::register_invoke(state)?;

        #[cfg(feature = "metrics")]
        crate::metrics::register(state, metrics)?;

        // Every member, the master included: see `tasks::register_bridge`.
        #[cfg(feature = "tasks")]
        crate::tasks::register_bridge(state, Arc::clone(&runtime))
            .map_err(registry::Error::msg)?;

        if !state.is_master() {
            crate::event::register(state)?;
            let script = Arc::new(PhpScriptService::new(Arc::clone(&runtime)));
            #[cfg(feature = "tasks")]
            let invocation =
                Arc::new(crate::service::invoke::PhpInvocationService::new(
                    Arc::clone(&runtime),
                ));
            #[cfg(feature = "tasks")]
            let runtime_for_tasks = Arc::clone(&runtime);
            let applications = Arc::new(PhpApplicationService::new(runtime));
            registry.register(Arc::clone(&script))?;
            #[cfg(feature = "tasks")]
            registry.register(invocation)?;
            registry.register(Arc::clone(&applications))?;
            crate::manager::register(state, applications, script)?;

            // Hand the task service an engine for its `php:` kind. Installing
            // also registers, so this is safe whichever of the two boots first.
            #[cfg(feature = "tasks")]
            crate::tasks::register(state, Arc::clone(&runtime_for_tasks))
                .map_err(registry::Error::msg)?;
        }

        Ok(())
    }

    /// Contribute a named persistent application during provider boot. No I/O.
    pub fn register_application(
        &self,
        application: PhpApplication,
    ) -> registry::Result<()> {
        application.validate().map_err(registry::Error::msg)?;
        if self
            .options
            .script_application
            .as_ref()
            .is_some_and(|(name, _)| name == &application.name)
        {
            return Err(registry::Error::msg(
                "script and persistent applications must have distinct names",
            ));
        }
        let mut definitions =
            self.definitions.lock().expect("PHP definitions lock poisoned");
        if definitions.frozen {
            return Err(registry::Error::msg(
                "PHP registrations are closed after the first service start",
            ));
        }
        if definitions
            .applications
            .iter()
            .any(|existing| existing.name == application.name)
        {
            return Err(registry::Error::msg(format!(
                "duplicate PHP application name: {}",
                application.name
            )));
        }
        definitions.applications.push(application);
        Ok(())
    }

    fn freeze_applications(&self) -> Vec<PhpApplication> {
        let applications = {
            let mut definitions =
                self.definitions.lock().expect("PHP definitions lock poisoned");
            definitions.frozen = true;
            definitions.applications.clone()
        };
        // All contributors have finished. Inspect only unspecified metadata
        // while preparing this generation, never while handling a request.
        applications
            .into_iter()
            .map(|mut application| {
                application.description.frameworks =
                    application.frameworks.clone().unwrap_or_else(|| {
                        crate::framework::detect(&application.script_path)
                    });
                application
            })
            .collect()
    }

    pub async fn execute(
        &self,
        runtime: Arc<PhpRuntime>,
        sapi_req: PhpSapiRequest,
        mut req: Request<RouteBody>,
        exchange_tx: Option<mpsc::Sender<HttpExchange>>,
    ) -> Result<RouteResponse, SapiGatewayError> {
        let context = self.environments.get().ok_or_else(|| {
            SapiGatewayError::ServiceUnavailable(
                "PHP environment context not registered".into(),
            )
        })?;
        if runtime.execution_environment.get().is_none() {
            let environment = context
                .observe(
                    &runtime.env_file,
                    runtime
                        .environment
                        .iter()
                        .map(|(key, value)| (key.clone(), value.clone())),
                )
                .map_err(|error| {
                    SapiGatewayError::ServiceUnavailable(error.to_string())
                })?;
            // Another request can win initialization; its identical recipe
            // remains the owner and this temporary registration is dropped.
            let _ = runtime.execution_environment.set(environment);
        }
        let snapshot = runtime
            .execution_environment
            .get()
            .expect("prepared request environment")
            .request_snapshot()
            .map_err(|error| {
                SapiGatewayError::ServiceUnavailable(error.to_string())
            })?;
        req.extensions_mut()
            .insert(crate::engine::context::RequestEnvironment(snapshot));
        self.engine.execute(runtime, sapi_req, req, exchange_tx).await
    }

    /// Request-pool telemetry only; task executions do not consume its slots.
    pub fn pool_snapshot(&self) -> Option<SapiThreadPoolSnapshot> {
        self.engine.pool_snapshot()
    }
}

impl Component<AppState> for PhpEmbedRuntime {
    type Deps = (
        Arc<claviron_orbit::FleetProvider>,
        Arc<claviron_orbit::CounterProvider>,
        Arc<claviron_orbit::CellsProvider>,
        CacheDependency,
        DatabaseDependency,
        InvocationDependency,
        InvalidationDependency,
        LoggingDependency,
    );

    fn name(&self) -> &'static str {
        "PhpEmbedRuntime"
    }

    fn order(&self) -> ComponentOrder {
        // The companion publishes the process-global stats table; the runtime
        // consumes that publication, not the companion object itself.
        ComponentOrder::new()
            .after::<crate::functions::sapi_stats::SapiStatsShm>()
    }

    fn as_bootable(&self) -> Option<&dyn Bootable<AppState, Self::Deps>> {
        Some(self)
    }
}

#[async_trait]
impl Bootable<AppState, <Self as Component<AppState>>::Deps>
    for PhpEmbedRuntime
{
    async fn boot(
        &self,
        (
            fleet,
            counter,
            cells,
            cache,
            database,
            invocation,
            _invalidation,
            logging,
        ): <Self as Component<AppState>>::Deps,
        state: &AppState,
    ) -> registry::Result<()> {
        self.validate()?;
        // Master keeps fleet metadata, but never boots application PHP or
        // requires a worker's dedicated environment files.
        if state.is_master() {
            return Ok(());
        }
        if self.booted.get().is_some() {
            return Ok(());
        }
        #[cfg(not(feature = "cache"))]
        let _ = cache;
        #[cfg(not(any(feature = "postgres", feature = "mysql")))]
        let _ = database;
        #[cfg(not(feature = "tasks"))]
        let _ = invocation;
        #[cfg(not(feature = "log-pipeline"))]
        let _ = logging;
        let owner = PROCESS_OWNER.get_or_init(|| Arc::clone(&self.identity));
        if !Arc::ptr_eq(owner, &self.identity) {
            return Err(registry::Error::msg(
                "one PhpEmbedRuntime may own PHP per process; install tasks on the existing runtime",
            ));
        }
        crate::functions::sapi_stats::configure_engine(Arc::clone(
            &self.engine,
        ));
        crate::sapi::init_server_scoped_vars(state);
        crate::functions::runtime::configure_from_state(state);
        #[cfg(feature = "auth")]
        crate::functions::auth::configure_from_state(state);
        crate::event::configure_from_state(state, self.options.events)
            .map_err(registry::Error::msg)?;
        #[cfg(feature = "tasks")]
        crate::functions::orbit::invoke::configure(invocation)?;
        #[cfg(feature = "cache")]
        crate::functions::cache::configure(cache)?;
        #[cfg(feature = "postgres")]
        crate::functions::postgres::configure(database.clone())?;
        #[cfg(feature = "mysql")]
        crate::functions::mysql::configure(database)?;
        crate::functions::orbit::counter::configure(counter);
        #[cfg(feature = "log-pipeline")]
        crate::functions::log::configure(logging);
        crate::functions::orbit::cell::configure(cells);
        crate::functions::orbit::lock::configure(fleet);
        crate::functions::runtime::mark_prepared();
        // Other providers can contribute programs later in the boot sequence.
        // Freeze that complete catalog only when Runnable starts.
        let _ = self.booted.set(());
        Ok(())
    }
}

#[cfg(test)]
#[path = "../tests/service.rs"]
mod tests;
