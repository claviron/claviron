use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use claviron_app::environment::{ApplicationEnvironments, EnvironmentContext};
use claviron_app::management::{
    ApplicationFailed, ApplicationStarted, ApplicationStopped,
};
use claviron_app::{ApplicationId, ApplicationName, ApplicationRuntime};
use claviron_http::{RouteBody, RouteResponse};
use claviron_runtime::registry::{self, Component, Runnable, priority};
use claviron_runtime::{AppState, RunContext, ServiceStartPolicy};
use http::Request;
use tokio::sync::Notify;
use tracing::{error, info};

use super::PhpEmbedRuntime;

/// How long an interrupted execution is given to unwind before the drain gives
/// up on it. The VM acts on the interrupt at its next check, which is immediate
/// for a script running bytecode; this is slack for the unwinding itself, not
/// for the script to notice. Matches `SapiThreadPool::INTERRUPT_GRACE` and
/// task-execution drain grace.
const INTERRUPT_UNWIND: std::time::Duration = std::time::Duration::from_secs(1);
use crate::SapiGatewayError;
use crate::config::PhpApplication;
use crate::engine::application::{ApplicationWorkers, PhpApplicationSnapshot};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PhpApplicationLifecycle {
    Running,
    Stopping,
    Stopped,
    Failed,
}

#[derive(Clone, Debug)]
pub struct PhpApplicationStatus {
    pub name: String,
    pub description: claviron_app::ApplicationDescription,
    pub enabled: bool,
    pub lifecycle: PhpApplicationLifecycle,
    pub generation: u64,
    pub pool: Option<PhpApplicationSnapshot>,
    pub last_error: Option<String>,
}

pub(crate) enum ApplicationCommandAction {
    Start,
    Stop,
    Restart,
}

struct ManagedApplication {
    definition: PhpApplication,
    enabled: bool,
    restart: bool,
    generation: u64,
    pool: Option<Arc<ApplicationWorkers>>,
    last_error: Option<String>,
}

/// Named persistent PHP application pools, local to every serving process.
/// This service owns the aggregate runnable generation; named application
/// lifecycle is exposed through the common `ApplicationManager` via
/// `PhpApplicationController`.
pub struct PhpApplicationService {
    runtime: Arc<PhpEmbedRuntime>,
    generation: tokio::sync::Mutex<()>,
    applications: Arc<Mutex<BTreeMap<String, ManagedApplication>>>,
    wake: Notify,
}

struct GenerationGuard(Arc<Mutex<BTreeMap<String, ManagedApplication>>>);

impl Drop for GenerationGuard {
    fn drop(&mut self) {
        for application in self.0.lock().unwrap().values() {
            if let Some(pool) = &application.pool {
                pool.stop();
            }
        }
    }
}

impl PhpApplicationService {
    pub(crate) fn new(runtime: Arc<PhpEmbedRuntime>) -> Self {
        Self {
            runtime,
            generation: tokio::sync::Mutex::new(()),
            applications: Arc::new(Mutex::new(BTreeMap::new())),
            wake: Notify::new(),
        }
    }

    /// Axum-free dispatch. Application selection belongs to the caller's router;
    /// PHP receives method/URI/headers/body explicitly, not synthetic superglobals.
    pub async fn execute(
        &self,
        name: &str,
        request: Request<RouteBody>,
    ) -> Result<RouteResponse, SapiGatewayError> {
        let pool = self
            .applications
            .lock()
            .unwrap()
            .get(name)
            .and_then(|application| application.pool.clone())
            .ok_or_else(|| {
                SapiGatewayError::ServiceUnavailable(format!(
                    "PHP application '{name}' is not running"
                ))
            })?;
        pool.execute(request).await
    }

    /// Compatibility telemetry for callers interested only in active pools.
    pub fn snapshot(&self) -> BTreeMap<String, PhpApplicationSnapshot> {
        self.applications
            .lock()
            .unwrap()
            .iter()
            .filter_map(|(name, application)| {
                application
                    .pool
                    .as_ref()
                    .map(|pool| (name.clone(), pool.snapshot()))
            })
            .collect()
    }

    pub fn statuses(&self) -> Vec<PhpApplicationStatus> {
        self.applications
            .lock()
            .unwrap()
            .iter()
            .map(|(name, application)| {
                let pool =
                    application.pool.as_ref().map(|pool| pool.snapshot());
                let lifecycle =
                    match (&pool, application.enabled, &application.last_error)
                    {
                        (Some(pool), _, _) if !pool.accepting => {
                            PhpApplicationLifecycle::Stopping
                        }
                        (Some(_), true, _) => PhpApplicationLifecycle::Running,
                        (None, true, Some(_)) => {
                            PhpApplicationLifecycle::Failed
                        }
                        (None, _, _) => PhpApplicationLifecycle::Stopped,
                        (Some(_), false, _) => {
                            PhpApplicationLifecycle::Stopping
                        }
                    };
                PhpApplicationStatus {
                    name: name.clone(),
                    description: application.definition.description.clone(),
                    enabled: application.enabled,
                    lifecycle,
                    generation: application.generation,
                    pool,
                    last_error: application.last_error.clone(),
                }
            })
            .collect()
    }

    pub(crate) fn apply_command(
        &self,
        name: &str,
        action: ApplicationCommandAction,
    ) -> Result<(), String> {
        let mut applications = self.applications.lock().unwrap();
        let application = applications.get_mut(name).ok_or_else(|| {
            format!("PHP application '{name}' is not configured")
        })?;
        match action {
            ApplicationCommandAction::Start => {
                application.enabled = true;
                application.restart = false;
                application.last_error = None;
            }
            ApplicationCommandAction::Stop => {
                application.enabled = false;
                application.restart = false;
                if let Some(pool) = &application.pool {
                    pool.stop();
                }
            }
            ApplicationCommandAction::Restart => {
                application.enabled = true;
                application.restart = true;
                application.last_error = None;
                if let Some(pool) = &application.pool {
                    pool.stop();
                }
            }
        }
        drop(applications);
        self.wake.notify_one();
        Ok(())
    }

    fn initialize(
        &self,
        definitions: Vec<PhpApplication>,
        environments: &ApplicationEnvironments,
    ) -> registry::Result<std::time::Duration> {
        let interval = definitions
            .iter()
            .map(|application| application.pool.maintenance_interval)
            .min()
            .ok_or_else(|| {
                registry::Error::msg("no PHP applications registered")
            })?;

        // Resolve every script before publishing or starting any application.
        let prepared = definitions
            .into_iter()
            .map(|mut definition| {
                let name = definition.name.clone();
                definition.environment = environments
                    .snapshot(&name)
                    .map_err(|error| registry::Error::msg(error.to_string()))?
                    .as_ref()
                    .iter()
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect();
                let pool = ApplicationWorkers::new(definition.clone())
                    .map_err(|error| {
                        registry::Error::msg(format!(
                            "PHP application '{name}': {error}"
                        ))
                    })?;
                Ok((name, definition, pool))
            })
            .collect::<registry::Result<Vec<_>>>()?;

        let mut applications = self.applications.lock().unwrap();
        if applications.values().any(|application| {
            application.pool.as_ref().is_some_and(|pool| !pool.drained())
        }) {
            return Err(registry::Error::msg(
                "previous PHP application generation is still draining",
            ));
        }
        for application in applications.values_mut() {
            application.pool = None;
        }
        for (name, definition, pool) in prepared {
            match applications.get_mut(&name) {
                Some(application) => {
                    application.definition = definition;
                    application.restart = false;
                    application.last_error = None;
                    if application.enabled {
                        application.pool = Some(pool);
                        application.generation =
                            application.generation.saturating_add(1);
                    }
                }
                None => {
                    applications.insert(
                        name,
                        ManagedApplication {
                            definition,
                            enabled: true,
                            restart: false,
                            generation: 1,
                            pool: Some(pool),
                            last_error: None,
                        },
                    );
                }
            }
        }
        Ok(interval)
    }

    fn maintain(
        &self,
        state: &AppState,
        allow_start: bool,
    ) {
        let mut applications = self.applications.lock().unwrap();
        for (name, application) in applications.iter_mut() {
            if let Some(pool) = &application.pool {
                if !allow_start || !application.enabled || application.restart {
                    pool.stop();
                } else {
                    pool.maintain();
                }
                if pool.drained() {
                    info!(
                        application = %name,
                        generation = application.generation,
                        restarting = application.restart,
                        "🔸 PHP application stopped"
                    );
                    state.events().emit(ApplicationStopped {
                        id: application_id(name),
                        generation: application.generation,
                    });
                    application.pool = None;
                    application.restart = false;
                }
            }

            if allow_start && application.pool.is_none() && application.enabled
            {
                match ApplicationWorkers::new(application.definition.clone()) {
                    Ok(pool) => {
                        pool.maintain();
                        application.pool = Some(pool);
                        application.generation =
                            application.generation.saturating_add(1);
                        application.last_error = None;
                        info!(application = %name, generation = application.generation, "🧩  PHP application started");
                        state.events().emit(ApplicationStarted {
                            id: application_id(name),
                            generation: application.generation,
                        });
                    }
                    Err(error) => {
                        application.last_error = Some(error.clone());
                        application.enabled = false;
                        error!(application = %name, %error, "❌ PHP application failed to start");
                        state.events().emit(ApplicationFailed {
                            id: application_id(name),
                            generation: application.generation,
                            error,
                        });
                    }
                }
            }
        }
    }

    async fn run_generation(
        &self,
        state: &AppState,
        context: RunContext,
    ) -> registry::Result<()> {
        let _generation = self.generation.try_lock().map_err(|_| {
            registry::Error::msg(
                "PHP application generation is already running",
            )
        })?;
        self.runtime.ensure_booted()?;
        let definitions = self.runtime.freeze_applications();
        let environment_context = EnvironmentContext::new(state)
            .map_err(|error| registry::Error::msg(error.to_string()))?;
        let mut environments = ApplicationEnvironments::default();
        for definition in &definitions {
            let environment = environment_context
                .observe(&definition.env_file, definition.environment.clone())
                .map_err(|error| {
                    registry::Error::msg(format!(
                        "PHP application '{}': {error}",
                        definition.name
                    ))
                })?;
            environments.insert(definition.name.clone(), environment);
        }
        let interval = self.initialize(definitions, &environments)?;
        let _stop_on_drop = GenerationGuard(Arc::clone(&self.applications));
        if !context.is_cancelled() {
            self.maintain(state, true);
            for application in self.statuses() {
                if application.lifecycle == PhpApplicationLifecycle::Running {
                    info!(
                        application = %application.name,
                        instances = application.pool.as_ref().map_or(0, |pool| pool.instances),
                        generation = application.generation,
                        "🧩 PHP application pool started"
                    );
                    state.events().emit(ApplicationStarted {
                        id: application_id(&application.name),
                        generation: application.generation,
                    });
                }
            }
        }

        loop {
            tokio::select! {
                biased;
                _ = context.cancelled() => break,
                (name, candidate) = environments.changed() => {
                    match candidate {
                        Ok(snapshot) => {
                            let mut applications = self.applications.lock().unwrap();
                            if let Some(application) = applications.get_mut(&name) {
                                let values = snapshot.as_ref().iter().map(|(key, value)| (key.clone(), value.clone())).collect();
                                if application.definition.environment != values {
                                    application.definition.environment = values;
                                    // Keep a stopped application stopped. A live pool uses the
                                    // existing joined replacement path, never a second pool.
                                    application.restart = application.pool.is_some();
                                    if let Some(pool) = &application.pool { pool.stop(); }
                                }
                            }
                            drop(applications);
                            self.maintain(state, true);
                        }
                        Err(error) => tracing::error!(application = %name, %error, "PHP application environment rejected; previous execution retained"),
                    }
                },
                _ = self.wake.notified() => self.maintain(state, true),
                _ = tokio::time::sleep(interval) => self.maintain(state, true)
            }
        }

        for application in self.applications.lock().unwrap().values() {
            if let Some(pool) = &application.pool {
                pool.stop();
            }
        }
        let drain = async {
            loop {
                self.maintain(state, false);
                if self
                    .applications
                    .lock()
                    .unwrap()
                    .values()
                    .all(|application| application.pool.is_none())
                {
                    break;
                }
                tokio::time::sleep(interval).await;
            }
        };
        let Some(grace) = self.runtime.options.graceful_timeout else {
            drain.await;

            return Ok(());
        };

        if tokio::time::timeout(grace, drain).await.is_ok() {
            return Ok(());
        }

        // The grace is the cooperative window, and an application that has not
        // returned from its handler by now is not going to. Interrupting is
        // what stops this becoming a stop that has to be a kill: a request
        // stuck in PHP keeps its instance, the instance keeps the worker, and
        // the master ends up waiting on a process that will never exit.
        //
        // One owner at a time, and only this service's. A process-wide signal
        // here used to end script requests and tasks that had nothing to do
        // with this drain.
        let signalled: usize = self
            .applications
            .lock()
            .unwrap()
            .values()
            .filter_map(|application| application.pool.as_ref())
            .map(|pool| {
                crate::engine::interrupt::interrupt_owned_by(pool.owner())
            })
            .sum();
        tracing::warn!(
            signalled,
            grace_ms = grace.as_millis() as u64,
            "🚨 php application drain expired; interrupting in-flight executions"
        );

        // Signalling is not completion. The VM acts at its next interrupt
        // point, which is immediate for running bytecode and never for a
        // blocking native call, so the drain watches its own generations for a
        // bounded window before it reports. The script pool and task execution
        // group do the same; this one used to return the moment it signalled,
        // which reported work as drained while it was still running.
        let unwind = async {
            loop {
                self.maintain(state, false);
                if self
                    .applications
                    .lock()
                    .unwrap()
                    .values()
                    .all(|application| application.pool.is_none())
                {
                    return true;
                }
                tokio::time::sleep(interval).await;
            }
        };
        if tokio::time::timeout(INTERRUPT_UNWIND, unwind).await.is_ok() {
            tracing::info!(
                "php application drain completed after interrupting in-flight work"
            );

            return Ok(());
        }

        Err(registry::Error::msg(
            "PHP application drain incomplete; in-flight executions interrupted",
        ))
    }
}

fn application_id(name: &str) -> ApplicationId {
    ApplicationId::new(
        ApplicationRuntime::new("php").expect("static PHP runtime name"),
        ApplicationName::new(name).expect("validated PHP application name"),
    )
}

impl Component<AppState> for PhpApplicationService {
    type Deps = Arc<PhpEmbedRuntime>;

    fn name(&self) -> &'static str {
        "PhpApplicationService"
    }
    fn run_priority(&self) -> Option<u8> {
        Some(priority::EARLY)
    }
    fn as_runnable(
        self: Arc<Self>
    ) -> Option<Arc<dyn Runnable<AppState, Self::Deps>>> {
        Some(self)
    }
}

#[async_trait]
impl Runnable<AppState, Arc<PhpEmbedRuntime>> for PhpApplicationService {
    fn start_policy(
        &self,
        state: &AppState,
    ) -> ServiceStartPolicy {
        if state.is_master()
            || self.runtime.definitions.lock().unwrap().applications.is_empty()
        {
            ServiceStartPolicy::Unavailable
        } else {
            ServiceStartPolicy::Automatic
        }
    }
    async fn run(
        self: Arc<Self>,
        _runtime: Arc<PhpEmbedRuntime>,
        state: AppState,
        context: RunContext,
    ) -> registry::Result<()> {
        self.run_generation(&state, context).await.map_err(|error| {
            error!(%error, "❌ PHP application generation failed");
            registry::Error::recoverable(error.to_string())
        })
    }
}
