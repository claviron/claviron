//! Observation has a service lifetime; worker ownership does not.
use std::sync::Arc;

use async_trait::async_trait;
use claviron_orbit::placement::{Scope, Scoped};
use claviron_runtime::registry::{Component, ComponentOrder, Runnable};
use claviron_runtime::{AppState, RunContext};

use crate::control::WorkerControl;
use crate::management::runner::WorkerOperationRunner;

/// Master-side Runnable that evaluates process health, heartbeat freshness and
/// rotation policy after initial fleet convergence.
///
/// It submits recovery and rotation operations to the operation runner rather
/// than owning their execution. This separation lets monitoring stop or restart
/// without cancelling an accepted operation or shutting down workers. A new
/// generation resumes from the existing fleet table; it does not spawn the
/// initial fleet, reset ownership, or perform process-wide finalization.
pub(crate) struct WorkerMonitor {
    control: Arc<WorkerControl>,
}
impl WorkerMonitor {
    pub(crate) fn new(control: Arc<WorkerControl>) -> Self {
        Self { control }
    }
}
impl Scoped for WorkerMonitor {
    fn scope(&self) -> Scope {
        Scope::Master
    }
}

impl Component<AppState> for WorkerMonitor {
    type Deps = ();

    fn name(&self) -> &'static str {
        "WorkerMonitor"
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
impl Runnable<AppState> for WorkerMonitor {
    async fn run(
        self: Arc<Self>,
        _deps: (),
        state: AppState,
        context: RunContext,
    ) -> claviron_runtime::registry::Result<()> {
        super::evaluate::monitor_workers(
            &self.control,
            state,
            context.cancellation_token(),
        )
        .await
        .map_err(Into::into)
    }
}
