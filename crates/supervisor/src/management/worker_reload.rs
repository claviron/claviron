//! Worker-local configuration execution, never an event-dispatcher drain.
use std::sync::Arc;

use async_trait::async_trait;
use claviron_orbit::placement::{Scope, Scoped};
use claviron_runtime::registry::{Component, Runnable};
use claviron_runtime::{AppState, RunContext, WorkerId};
use tracing::error;

use super::runner::OperationQueue;
use crate::lifecycle::RejectionReason;
use crate::reload::{ServerReloading, WorkerReloaded};

/// Worker-side Runnable that serializes local provider reload cycles requested
/// by the master, keeping long reload work out of event callbacks.
///
/// A successful cycle emits its correlated acknowledgement; failed or interrupted
/// cycles do not claim completion. Targeted stop closes admission and drains
/// accepted requests. Process-wide shutdown is distinct: it may interrupt the
/// current reload and skip queued cycles as the worker itself exits. Restarting
/// this service does not replace the worker process or reset its identity.
#[derive(Default)]
pub(crate) struct WorkerReloadRunner {
    queue: OperationQueue,
    serial: Arc<tokio::sync::Mutex<()>>,
}
impl WorkerReloadRunner {
    pub(crate) fn submit(
        &self,
        state: AppState,
        request: ServerReloading,
        worker_id: WorkerId,
    ) -> Result<(), RejectionReason> {
        let serial = self.serial.clone();
        self.queue.submit(Box::pin(async move {
            let _reload = serial.lock().await;
            // Shutdown wins the race, and abandoning a half-applied reload is
            // the right answer here rather than a leak: this worker is on its
            // way out, so the configuration it did not finish adopting dies
            // with it. `reload_all` is not cancel-safe in general — see its
            // docs — but the state it would strand is state nobody will read.
            // Running it to completion instead is what turns a provider that
            // blocks into a shutdown that hangs.
            tokio::select! {
                biased;
                _ = state.on_shutdown() => (),
                result = state.registry_ref().reload_all(&state) => {
                    let completed = match result {
                        Ok(outcome) => {
                            for failure in outcome.failures() {
                                error!(
                                    generation = request.generation,
                                    %worker_id,
                                    component = failure.component(),
                                    error = %failure.error(),
                                    "worker provider reload failed"
                                );
                            }
                            outcome.is_complete()
                        }
                        Err(error) => {
                            error!(generation = request.generation, %worker_id, "worker reload cycle failed: {error}");
                            false
                        }
                    };

                    // Acknowledge only what happened. The master treats a
                    // `WorkerReloaded` as this worker's confirmation and
                    // reports `ReloadOutcome::Complete` once every worker has
                    // sent one — so acknowledging a cycle whose providers
                    // failed told the operator the fleet had reloaded when part
                    // of it had not. Staying silent leaves this worker in the
                    // master's unanswered set, which is accurate, and the
                    // errors above say why.
                    if completed {
                        state.events().emit(WorkerReloaded {
                            generation: request.generation,
                            worker_id: worker_id.raw()
                        });
                    }
                }
            }

        }))
    }
}
impl Scoped for WorkerReloadRunner {
    fn scope(&self) -> Scope {
        Scope::Worker
    }
}

impl Component<AppState> for WorkerReloadRunner {
    type Deps = ();

    fn name(&self) -> &'static str {
        "WorkerReloadRunner"
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
impl Runnable<AppState> for WorkerReloadRunner {
    async fn run(
        self: Arc<Self>,
        _deps: (),
        _: AppState,
        context: RunContext,
    ) -> claviron_runtime::registry::Result<()> {
        self.queue.drive(context).await
    }
}
