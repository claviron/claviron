//! The whole-server reload hook, separate from monitoring and policy loading.
use std::sync::Arc;

use async_trait::async_trait;
use claviron_orbit::placement::{Scope, Scoped};
use claviron_runtime::AppState;
use claviron_runtime::registry::{self, Component, ComponentOrder, Reloadable};
use tracing::{error, info, warn};

use crate::control::WorkerControl;
use crate::lifecycle::{OperationKind, OperationOrigin, OperationScope};
use crate::{Error, Result};

/// Master-only Reloadable hook that applies an accepted supervisor policy to
/// workers, separately from policy parsing and health monitoring.
///
/// It acquires a fleet operation, delegates recovery-epoch reset and worker
/// reload to the operation runner, and awaits that operation's result. Rejected
/// policy leaves worker recovery state untouched. This provider is not Runnable:
/// stopping or restarting the monitor is not a configuration reload.
pub(crate) struct WorkerConfigurationReload {
    control: Arc<WorkerControl>,
}
impl WorkerConfigurationReload {
    pub(crate) fn new(control: Arc<WorkerControl>) -> Self {
        Self { control }
    }
}
impl Scoped for WorkerConfigurationReload {
    fn scope(&self) -> Scope {
        Scope::Master
    }
}
impl Component<AppState> for WorkerConfigurationReload {
    type Deps = ();

    fn name(&self) -> &'static str {
        "WorkerConfigurationReload"
    }
    fn as_scoped(&self) -> Option<&dyn Scoped> {
        Some(self)
    }
    fn order(&self) -> ComponentOrder {
        ComponentOrder::new()
            .after::<crate::config::provider::SupervisorPolicy>()
            .after::<WorkerControl>()
    }
    fn as_reloadable(&self) -> Option<&dyn Reloadable<AppState>> {
        Some(self)
    }
}
#[async_trait]
impl Reloadable<AppState> for WorkerConfigurationReload {
    fn priority(&self) -> Option<u8> {
        Some(u8::MAX)
    }
    async fn reload(
        &self,
        state: &AppState,
    ) -> registry::Result<()> {
        // Worker processes configure locally but never command fleet reloads.
        if !state.is_master() {
            return Ok(());
        }
        let policy = state
            .registry_ref()
            .resolve::<crate::config::provider::SupervisorPolicy>()
            .ok_or_else(|| {
                registry::Error::msg("supervisor policy is unavailable")
            })?;
        if !policy.latest_load_accepted() {
            return Err(registry::Error::msg(
                "worker configuration reload skipped: supervisor policy was rejected",
            ));
        }
        let guard = super::submit::acquire(
            &self.control,
            OperationScope::Fleet,
            OperationKind::ConfigReload,
            OperationOrigin::Signal,
        )
        .map_err(|reason| {
            registry::Error::msg(format!(
                "worker configuration reload refused: {reason:?}"
            ))
        })?;
        let control = self.control.clone();
        let state = state.clone();
        let (completed, completion) = tokio::sync::oneshot::channel();
        super::submit::enqueue_operation(&self.control, guard, async move {
            let result = apply(&control, &state).await;
            let _ = completed.send(result);
            Ok(())
        })
        .map_err(|reason| {
            registry::Error::msg(format!(
                "worker configuration reload refused: {reason:?}"
            ))
        })?;
        completion.await.map_err(|_| {
            registry::Error::msg(
                "worker configuration reload lost its completion",
            )
        })??;
        Ok(())
    }
}

async fn apply(
    control: &WorkerControl,
    state: &AppState,
) -> Result<()> {
    let reset =
        crate::lifecycle::recovery::reset_worker_restart_budgets(control).await;
    if reset != 0 {
        info!(workers = reset, "reload opened a new worker recovery epoch");
    }
    let (_, dead) = crate::process::health::check_worker_health(control).await;
    for id in dead {
        if let Err(error) =
            crate::lifecycle::recovery::restart_worker(control, id, state).await
        {
            error!(worker_id = %id, %error, "worker recovery before configuration reload failed");
        }
    }
    let result = super::actions::reload::reload_workers(
        control,
        state,
        state.worker_count(),
    )
    .await
    .map_err(Error::service)?;
    match result {
        crate::reload::ReloadOutcome::Complete { reloaded } => {
            super::actions::reload::record_reload_revisions(control, &reloaded)
                .await;
            info!(
                workers = reloaded.len(),
                "workers completed configuration reload"
            );
        }
        crate::reload::ReloadOutcome::Partial {
            reloaded,
            timed_out,
            unavailable,
        } => {
            super::actions::reload::record_reload_revisions(control, &reloaded)
                .await;
            warn!(
                ?reloaded,
                ?timed_out,
                ?unavailable,
                "worker configuration reload is partial"
            );
        }
    }
    Ok(())
}
