use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use claviron_app::management::{
    ApplicationFailed, ApplicationStarted, ApplicationStopped,
};
use claviron_app::{
    ApplicationId, ApplicationLifecycle, ApplicationName,
    ApplicationPoolStatus, ApplicationRuntime, ApplicationSnapshot,
};
use claviron_http::{HttpExchange, RouteBody, RouteResponse};
use claviron_php::PhpRuntime;
use claviron_runtime::registry::{self, Component, Runnable, priority};
use claviron_runtime::{AppState, RunContext, ServiceStartPolicy};
use http::Request;
use tokio::sync::{Mutex, mpsc};
use tracing::info;

use super::PhpEmbedRuntime;
use crate::engine::{PhpSapiRequest, SapiPoolSettings, SapiThreadPoolSnapshot};
use crate::error::SapiGatewayError;

/// Independently managed HTTP PHP execution. Shares process SAPI setup with
/// task and application executions, but owns only request admission and pool threads.
pub struct PhpScriptService {
    runtime: Arc<PhpEmbedRuntime>,
    generation: Mutex<()>,
    application: RwLock<Option<ApplicationSnapshot>>,
}

impl PhpScriptService {
    pub(crate) fn new(runtime: Arc<PhpEmbedRuntime>) -> Self {
        let application = runtime.options.script_application.as_ref().map(
            |(name, description)| ApplicationSnapshot {
                id: ApplicationId::new(
                    ApplicationRuntime::new("php").expect("static PHP runtime"),
                    ApplicationName::new(name.clone())
                        .expect("validated script application name"),
                ),
                description: description.clone(),
                lifecycle: ApplicationLifecycle::Stopped,
                enabled: true,
                generation: 0,
                pool: None,
                last_error: None,
            },
        );
        Self {
            runtime,
            generation: Mutex::new(()),
            application: RwLock::new(application),
        }
    }

    /// Only a runner that names its sole script pool appears as an application.
    pub fn application_status(&self) -> Option<ApplicationSnapshot> {
        let mut status = self
            .application
            .read()
            .expect("PHP script status lock poisoned")
            .clone()?;
        status.pool = self.pool_snapshot().map(|pool| ApplicationPoolStatus {
            accepting: Some(pool.accepting),
            instances: u32::try_from(pool.threads_current)
                .expect("PHP pool capacity fits u32"),
            ready: Some(
                u32::try_from(pool.idle_threads)
                    .expect("PHP pool capacity fits u32"),
            ),
            busy: Some(
                u32::try_from(pool.busy_threads)
                    .expect("PHP pool capacity fits u32"),
            ),
            retiring: None,
            backing_off: None,
            exhausted: None,
        });
        Some(status)
    }

    fn transition(
        &self,
        lifecycle: ApplicationLifecycle,
        generation: u64,
        error: Option<String>,
    ) -> Option<ApplicationSnapshot> {
        let mut application =
            self.application.write().expect("PHP script status lock poisoned");
        let status = application.as_mut()?;
        status.lifecycle = lifecycle;
        status.generation = generation;
        status.last_error = error;
        Some(status.clone())
    }

    fn begin_generation(&self) -> u64 {
        let mut application =
            self.application.write().expect("PHP script status lock poisoned");
        let Some(status) = application.as_mut() else {
            return 0;
        };
        status.generation = status
            .generation
            .checked_add(1)
            .expect("PHP application generation exhausted");
        status.lifecycle = ApplicationLifecycle::Starting;
        status.last_error = None;
        status.generation
    }

    pub async fn execute(
        &self,
        runtime: Arc<PhpRuntime>,
        sapi_req: PhpSapiRequest,
        req: Request<RouteBody>,
        exchange_tx: Option<mpsc::Sender<HttpExchange>>,
    ) -> Result<RouteResponse, SapiGatewayError> {
        self.runtime.execute(runtime, sapi_req, req, exchange_tx).await
    }

    pub fn pool_snapshot(&self) -> Option<SapiThreadPoolSnapshot> {
        self.runtime.pool_snapshot()
    }

    async fn run_generation(
        &self,
        state: AppState,
        context: RunContext,
        generation: u64,
    ) -> registry::Result<()> {
        self.runtime.ensure_booted()?;
        self.runtime.engine.ensure_drained().map_err(registry::Error::msg)?;
        self.runtime.freeze_definitions();
        let pool =
            self.runtime.options.request_pool.as_ref().ok_or_else(|| {
                registry::Error::msg("PHP request pool is disabled")
            })?;
        let cancellation = context.cancellation_token().child_token();
        let mut errors = Vec::new();
        let mut maintenance = Vec::new();
        let mut publisher = None;
        if !context.is_cancelled() {
            let started =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    self.runtime.engine.start_pool(
                        SapiPoolSettings {
                            max_requests: pool.max_requests,
                            threads_min: pool.min_idle,
                            threads_max: pool.max_size,
                            threads_max_idle: pool.max_idle,
                            thread_idle_timeout: pool.idle_timeout,
                            thread_max_life: pool.max_life,
                        },
                        cancellation.clone(),
                    )
                }));
            match started {
                Ok(Ok(handles)) => {
                    maintenance = handles;
                    publisher = crate::functions::sapi_stats::start_publisher(
                        state.clone(),
                        Arc::clone(&self.runtime.engine),
                        cancellation.clone(),
                    );
                }
                Ok(Err(error)) => errors.push(error),
                Err(_) => {
                    errors.push("PHP request-pool startup panicked".to_owned())
                }
            }
        }
        if errors.is_empty() && !context.is_cancelled() {
            if let Some(status) =
                self.transition(ApplicationLifecycle::Running, generation, None)
            {
                state.events().emit(ApplicationStarted {
                    id: status.id,
                    generation: status.generation,
                });
            }
            info!("PHP request generation started");
            context.cancelled().await;
        }
        self.transition(ApplicationLifecycle::Stopping, generation, None);
        cancellation.cancel();
        for handle in maintenance {
            if let Err(error) = handle.await {
                errors.push(format!("PHP maintenance task failed: {error}"));
            }
        }
        if let Some(handle) = publisher
            && let Err(error) = handle.await
        {
            errors.push(format!("PHP stats publisher failed: {error}"));
        }
        if !self
            .runtime
            .engine
            .drain(self.runtime.options.graceful_timeout)
            .await
        {
            errors.push(
                "PHP request pool drain incomplete; retained work prevents restart".to_owned()
            );
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(registry::Error::msg(errors.join("; ")))
        }
    }
}

impl Component<AppState> for PhpScriptService {
    type Deps = Arc<PhpEmbedRuntime>;

    fn name(&self) -> &'static str {
        "PhpSapiService"
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
impl Runnable<AppState, Arc<PhpEmbedRuntime>> for PhpScriptService {
    fn start_policy(
        &self,
        state: &AppState,
    ) -> ServiceStartPolicy {
        if state.is_master() || self.runtime.options.request_pool.is_none() {
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
        let _generation = self.generation.try_lock().map_err(|_| {
            registry::Error::msg("PHP request generation is already running")
        })?;
        let generation = self.begin_generation();
        let result =
            self.run_generation(state.clone(), context, generation).await;
        match &result {
            Ok(()) => {
                if let Some(status) = self.transition(
                    ApplicationLifecycle::Stopped,
                    generation,
                    None,
                ) {
                    state
                        .events()
                        .emit(ApplicationStopped { id: status.id, generation });
                }
            }
            Err(error) => {
                let error = error.to_string();
                if let Some(status) = self.transition(
                    ApplicationLifecycle::Failed,
                    generation,
                    Some(error.clone()),
                ) {
                    state.events().emit(ApplicationFailed {
                        id: status.id,
                        generation,
                        error,
                    });
                }
            }
        }
        result.map_err(|error| registry::Error::recoverable(error.to_string()))
    }
}
