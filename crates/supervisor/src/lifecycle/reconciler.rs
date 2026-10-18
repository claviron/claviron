//! Initial convergence is independent of the restartable health monitor.
use std::sync::Arc;
use std::sync::atomic::Ordering;

use async_trait::async_trait;
use claviron_orbit::placement::{Scope, Scoped};
use claviron_runtime::registry::{Component, ComponentOrder, Runnable};
use claviron_runtime::{AppState, RunContext};

use crate::control::WorkerControl;
use crate::management::runner::WorkerOperationRunner;

/// Master-side Runnable that establishes the initial fleet and keeps resolving
/// slots whose process ownership could not yet be established.
///
/// Its initial phase spawns or reconciles configured slots and waits for exact
/// boot acknowledgements; subsequent passes revisit unclaimed ownership. This
/// keeps startup and adoption out of the health monitor. Targeted stop preserves
/// the process table, and restart resumes existing slots without duplicating
/// workers. Initial convergence failure, unlike targeted stop, explicitly runs
/// boot rollback before returning an error.
pub(crate) struct WorkerReconciler {
    control: Arc<WorkerControl>,
}
impl WorkerReconciler {
    pub(crate) fn new(control: Arc<WorkerControl>) -> Self {
        Self { control }
    }
}
impl Scoped for WorkerReconciler {
    fn scope(&self) -> Scope {
        Scope::Master
    }
}

impl Component<AppState> for WorkerReconciler {
    type Deps = ();

    fn name(&self) -> &'static str {
        "WorkerReconciler"
    }
    fn order(&self) -> ComponentOrder {
        ComponentOrder::new().after::<WorkerOperationRunner>()
    }
    fn as_runnable(
        self: Arc<Self>
    ) -> Option<Arc<dyn Runnable<AppState, Self::Deps>>> {
        Some(self)
    }

    fn as_scoped(&self) -> Option<&dyn Scoped> {
        Some(self)
    }
}
#[async_trait]
impl Runnable<AppState> for WorkerReconciler {
    async fn run(
        self: Arc<Self>,
        _deps: (),
        state: AppState,
        context: RunContext,
    ) -> claviron_runtime::registry::Result<()> {
        if context.is_cancelled() || self.control.is_shutting_down() {
            return Ok(());
        }
        // Idempotent per slot. Restarting this service resumes observation of
        // existing processes, never recreates the fleet or its ownership.
        if let Err(error) =
            super::reconcile::spawn_all_workers(&self.control, &state).await
        {
            super::drain::cleanup_failed_boot(&self.control, &state).await;
            return Err(error.into());
        }
        self.control.changed.notify_waiters();
        if !self.control.startup_completed.load(Ordering::Acquire) {
            let result = super::startup::wait_workers_started(
                &self.control,
                &state,
                &context.cancellation_token(),
                self.control.config().startup.timeout,
            )
            .await;
            if context.is_cancelled() {
                return Ok(());
            }
            if let Err(error) = result {
                // A failed initial convergence is a process boot failure,
                // not a targeted service stop. Drain before returning it.
                super::drain::cleanup_failed_boot(&self.control, &state).await;
                return Err(error.into());
            }
            self.control.startup_completed.store(true, Ordering::Release);
        }
        loop {
            tokio::select! {
                biased;
                _ = context.cancelled() => return Ok(()),
                _ = tokio::time::sleep(self.control.config().heartbeat.interval) => {},
            }
            if self.control.is_shutting_down() {
                return Ok(());
            }
            super::reconcile::reconcile_unclaimed_workers(
                &self.control,
                &state,
            )
            .await?;
        }
    }
}
