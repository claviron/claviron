//! The runner owns worker counts; caller selection stays in the backend.
use crate::RunnerOptions;
use claviron_orbit::link::application::{
    LinkWorkerIdle, RetirementSource, RetirementTarget,
};
use claviron_runtime::{AppState, registry};
use claviron_supervisor::WorkerManager;
use std::sync::Arc;

struct OwnedCandidate {
    state: AppState,
    minimum: u16,
}
#[async_trait::async_trait]
impl RetirementTarget for OwnedCandidate {
    async fn candidate_pid(&self) -> Option<u32> {
        let count = self.state.worker_count();
        if u16::from(count) <= self.minimum {
            return None;
        }
        WorkerManager::new(&self.state)
            .ok()?
            .owned_worker_pid(count)
            .await
            .ok()
            .flatten()
    }
    async fn changed(
        &self,
        previous: Option<u32>,
    ) {
        match WorkerManager::new(&self.state) {
            Ok(manager) => {
                if manager
                    .wait_for_candidate_change(self.minimum, previous)
                    .await
                    .is_err()
                {
                    std::future::pending::<()>().await;
                }
            }
            Err(_) => std::future::pending::<()>().await,
        }
    }
}
pub(crate) fn register(
    state: &AppState,
    options: &RunnerOptions,
) -> registry::Result<()> {
    let Some(workers) = options
        .orbit_config()
        .map_err(registry::Error::msg)?
        .and_then(|config| config.link)
        .and_then(|link| link.workers)
    else {
        return Ok(());
    };
    if workers.retire_idle().timeout().is_none() {
        return Ok(());
    }
    let minimum = workers.initial_count();
    state.registry_ref().insert(Arc::new(RetirementSource(Arc::new(
        OwnedCandidate { state: state.clone(), minimum },
    ))))?;
    state.events().listen::<LinkWorkerIdle, _>(|state, event| async move {
        if !state.is_master()
            || u16::from(state.worker_count()) <= event.minimum
        {
            return Ok(());
        }
        let manager = WorkerManager::new(&state)
            .map_err(|error| registry::Error::msg(error.to_string()))?;
        if manager.owned_worker_pid(state.worker_count()).await.ok().flatten()
            != Some(event.pid)
        {
            return Ok(());
        }
        match manager.remove_worker() {
            Ok(plan) => tracing::info!(
                pid = event.pid,
                from = plan.previous_worker_count(),
                to = plan.target_worker_count(),
                "retiring idle Link worker"
            ),
            Err(error) => {
                tracing::warn!(%error, "idle Link worker could not be retired")
            }
        }
        Ok(())
    })?;
    Ok(())
}
