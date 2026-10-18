//! Named PHP application management.
//!
//! Aggregate PHP services remain Continuo runnables. These handles expose
//! domain lifecycle for configured children without pretending they are
//! providers themselves.

use std::sync::{Arc, Weak};

use async_trait::async_trait;
use claviron_app::{
    ApplicationControlResult, ApplicationController, ApplicationId,
    ApplicationLifecycle, ApplicationManager, ApplicationName,
    ApplicationPoolStatus, ApplicationRuntime, ApplicationSnapshot,
};
use claviron_runtime::registry::{self, Bootable, Component, Registry};
use claviron_runtime::{AppState, ServiceManager};

use crate::service::{
    ApplicationCommandAction, PhpApplicationService, PhpScriptService,
};

#[derive(Debug, thiserror::Error)]
pub enum PhpManagerError {
    #[error("PhpDaemonService is unavailable")]
    DaemonServiceUnavailable,
    #[error("PHP resource name cannot be empty")]
    EmptyName,
    #[error(
        "PHP resource name is too long for the Orbit command frame: {actual} bytes, max {max}"
    )]
    NameTooLong { actual: usize, max: usize },
    #[error("PHP daemon '{0}' is not configured")]
    DaemonNotFound(String),
    #[error("PHP management is unavailable during shutdown")]
    ShuttingDown,
    #[error("Orbit fleet is unavailable for PHP management snapshots")]
    FleetUnavailable,
    #[error("PHP management snapshot failed: {0}")]
    Snapshot(String),
}

pub type PhpManagerResult<T> = Result<T, PhpManagerError>;

/// Common application-management adapter for the embedded PHP runtime.
/// Clones share the execution services; they do not copy application state.
#[derive(Clone)]
pub struct PhpApplicationController {
    runtime: ApplicationRuntime,
    service: Arc<PhpApplicationService>,
    script: Option<(Arc<PhpScriptService>, Weak<Registry<AppState>>)>,
}

impl Component<AppState> for PhpApplicationController {
    type Deps = Arc<ApplicationManager>;

    fn name(&self) -> &'static str {
        "PhpApplicationController"
    }

    fn as_bootable(&self) -> Option<&dyn Bootable<AppState, Self::Deps>> {
        Some(self)
    }
}

#[async_trait]
impl Bootable<AppState, Arc<ApplicationManager>> for PhpApplicationController {
    async fn boot(
        &self,
        manager: Arc<ApplicationManager>,
        _state: &AppState,
    ) -> registry::Result<()> {
        manager
            .add(Arc::new(self.clone()))
            .map_err(|error| registry::Error::msg(error.to_string()))
    }
}

impl PhpApplicationController {
    pub fn new(service: Arc<PhpApplicationService>) -> Self {
        Self {
            runtime: ApplicationRuntime::new("php")
                .expect("static PHP runtime name"),
            service,
            script: None,
        }
    }

    fn script_status(
        &self,
        name: &ApplicationName,
    ) -> bool {
        self.script
            .as_ref()
            .and_then(|(script, _)| script.application_status())
            .is_some_and(|status| status.id.name() == name)
    }

    async fn control_script(
        &self,
        action: ApplicationCommandAction,
    ) -> ApplicationControlResult<()> {
        let manager = self
            .script
            .as_ref()
            .and_then(|(_, registry)| registry.upgrade())
            .and_then(|registry| registry.resolve::<ServiceManager>())
            .ok_or_else(|| {
                Box::new(PhpApplicationControlError(
                    "PHP service management is unavailable".into(),
                )) as Box<dyn std::error::Error + Send + Sync>
            })?;
        let result = match action {
            ApplicationCommandAction::Start => {
                manager.start("PhpSapiService").await
            }
            ApplicationCommandAction::Stop => {
                manager.stop("PhpSapiService").await
            }
            ApplicationCommandAction::Restart => {
                manager.restart("PhpSapiService").await
            }
        };
        result.map(|_| ()).map_err(|error| Box::new(error) as _)
    }

    fn snapshot(
        &self,
        status: crate::PhpApplicationStatus,
    ) -> ApplicationSnapshot {
        let name = ApplicationName::new(status.name)
            .expect("validated PHP application name");
        ApplicationSnapshot {
            id: ApplicationId::new(self.runtime.clone(), name),
            description: status.description,
            lifecycle: match status.lifecycle {
                crate::PhpApplicationLifecycle::Running => {
                    ApplicationLifecycle::Running
                }
                crate::PhpApplicationLifecycle::Stopping => {
                    ApplicationLifecycle::Stopping
                }
                crate::PhpApplicationLifecycle::Stopped => {
                    ApplicationLifecycle::Stopped
                }
                crate::PhpApplicationLifecycle::Failed => {
                    ApplicationLifecycle::Failed
                }
            },
            enabled: status.enabled,
            generation: status.generation,
            pool: status.pool.map(|pool| ApplicationPoolStatus {
                accepting: Some(pool.accepting),
                instances: pool_count(pool.instances),
                ready: Some(pool_count(pool.ready)),
                busy: Some(pool_count(pool.busy)),
                retiring: Some(pool_count(pool.retiring)),
                backing_off: Some(pool_count(pool.backing_off)),
                exhausted: Some(pool_count(pool.exhausted)),
            }),
            last_error: status.last_error,
        }
    }
}

#[async_trait]
impl ApplicationController for PhpApplicationController {
    fn runtime(&self) -> &ApplicationRuntime {
        &self.runtime
    }

    fn list(&self) -> Vec<ApplicationSnapshot> {
        let mut applications: Vec<_> = self
            .service
            .statuses()
            .into_iter()
            .map(|status| self.snapshot(status))
            .collect();
        if let Some(status) = self
            .script
            .as_ref()
            .and_then(|(script, _)| script.application_status())
        {
            applications.push(status);
        }
        applications
    }

    fn status(
        &self,
        name: &ApplicationName,
    ) -> Option<ApplicationSnapshot> {
        self.script
            .as_ref()
            .and_then(|(script, _)| script.application_status())
            .filter(|status| status.id.name() == name)
            .or_else(|| {
                self.service
                    .statuses()
                    .into_iter()
                    .find(|status| status.name == name.as_str())
                    .map(|status| self.snapshot(status))
            })
    }

    async fn start(
        &self,
        name: &ApplicationName,
    ) -> ApplicationControlResult<()> {
        if self.script_status(name) {
            return self.control_script(ApplicationCommandAction::Start).await;
        }
        apply_application_command(
            &self.service,
            name,
            ApplicationCommandAction::Start,
        )
    }

    async fn stop(
        &self,
        name: &ApplicationName,
    ) -> ApplicationControlResult<()> {
        if self.script_status(name) {
            return self.control_script(ApplicationCommandAction::Stop).await;
        }
        apply_application_command(
            &self.service,
            name,
            ApplicationCommandAction::Stop,
        )
    }

    async fn restart(
        &self,
        name: &ApplicationName,
    ) -> ApplicationControlResult<()> {
        if self.script_status(name) {
            return self
                .control_script(ApplicationCommandAction::Restart)
                .await;
        }
        apply_application_command(
            &self.service,
            name,
            ApplicationCommandAction::Restart,
        )
    }
}

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
struct PhpApplicationControlError(String);

fn apply_application_command(
    service: &PhpApplicationService,
    name: &ApplicationName,
    action: ApplicationCommandAction,
) -> ApplicationControlResult<()> {
    service
        .apply_command(name.as_str(), action)
        .map_err(|error| Box::new(PhpApplicationControlError(error)) as _)
}

fn pool_count(count: usize) -> u32 {
    u32::try_from(count).expect("validated PHP application pool count fits u32")
}

pub(crate) fn register(
    state: &AppState,
    applications: Arc<PhpApplicationService>,
    script: Arc<PhpScriptService>,
) -> claviron_runtime::registry::Result<()> {
    let mut controller = PhpApplicationController::new(applications);
    if script.application_status().is_some() {
        controller.script = Some((script, Arc::downgrade(&state.registry())));
    }
    state.registry_ref().register(Arc::new(controller))?;
    Ok(())
}

#[cfg(test)]
#[path = "../tests/manager.rs"]
mod tests;
