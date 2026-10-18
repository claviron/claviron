use std::sync::Arc;
#[cfg(feature = "metrics")]
use std::sync::atomic::AtomicU64;
#[cfg(feature = "metrics")]
use std::time::Instant;

use claviron_orbit::placement::{Scope, Scoped};
use claviron_runtime::AppState;
use claviron_runtime::registry::{Component, ComponentOrder};
use tokio::sync::Notify;

use crate::Result;
use crate::config::SupervisorConfig;
use crate::fleet::*;
#[cfg(test)]
use crate::lifecycle::WorkerIntent;
use crate::observer::HeartbeatObserver;

/// Master-side worker control API. This is not a runnable service: stopping a
/// monitor generation cannot destroy the process ownership held by the fleet.
///
/// Shares the fleet, policy, observations and operation handoff with named
/// services and actions; it does not run their loops or reload configuration.
/// Explicit bootstrap/shutdown compatibility methods delegate to those actions.
/// The legacy public name `WorkerSupervisor` aliases this capability, not a
/// combined boot/monitor/shutdown service.
pub struct WorkerControl {
    pub(crate) fleet: WorkerFleet,
    pub(crate) observer: Arc<HeartbeatObserver>,
    pub(crate) config: crate::config::provider::SharedPolicy,
    pub(crate) exited: Arc<Notify>,
    pub(crate) changed: Notify,
    pub(crate) operation_queue: crate::management::runner::OperationQueue,
    #[cfg(feature = "metrics")]
    pub(crate) metrics_started_at: Instant,
    #[cfg(feature = "metrics")]
    pub(crate) worker_restarts_total: AtomicU64,
    #[cfg(feature = "metrics")]
    pub(crate) worker_rotations_total: AtomicU64,
}
impl Scoped for WorkerControl {
    fn scope(&self) -> Scope {
        Scope::Master
    }
}
impl std::ops::Deref for WorkerControl {
    type Target = WorkerFleet;
    fn deref(&self) -> &WorkerFleet {
        &self.fleet
    }
}
pub(crate) use crate::fleet::{
    BlockedWorker, WorkerProcess, WorkerProcessSnapshot,
};
impl WorkerControl {
    pub(crate) fn new(observer: Arc<HeartbeatObserver>) -> Self {
        Self {
            observer,
            config: Arc::new(crate::config::provider::PolicySnapshot::default()),
            fleet: WorkerFleet::default(),
            operation_queue: crate::management::runner::OperationQueue::default(
            ),
            exited: Arc::new(Notify::new()),
            changed: Notify::new(),
            #[cfg(feature = "metrics")]
            metrics_started_at: Instant::now(),
            #[cfg(feature = "metrics")]
            worker_restarts_total: AtomicU64::new(0),
            #[cfg(feature = "metrics")]
            worker_rotations_total: AtomicU64::new(0),
        }
    }

    /// Returns the loaded supervisor policy. `configure()` must complete
    /// before any other lifecycle method dereferences this — runtime ordering
    /// guarantees that, so we expect the snapshot to be present.
    pub(crate) fn config(&self) -> Arc<SupervisorConfig> {
        self.config.load_full().expect(
            "WorkerControl::config accessed before supervisor configuration",
        )
    }

    /// Explicit low-level signalling compatibility API; normal lifecycle
    /// operations use typed drain requests through WorkerManager.
    pub async fn signal_alive_workers(
        &self,
        signal: nix::sys::signal::Signal,
    ) -> Vec<(claviron_runtime::WorkerId, u32)> {
        crate::process::health::signal_alive_workers(self, signal).await
    }

    /// Explicit initial reconciliation compatibility API. Ordinary runtime
    /// startup is driven by WorkerReconciler, never by monitor restart.
    pub async fn spawn_all_workers(
        &self,
        state: &AppState,
    ) -> Result<()> {
        crate::lifecycle::reconcile::spawn_all_workers(self, state).await?;
        self.changed.notify_waiters();
        Ok(())
    }

    /// Wait for the fleet's existing origin-aware shutdown policy to complete.
    pub async fn wait_workers(&self) {
        crate::lifecycle::drain::wait_workers(self).await
    }

    pub async fn shutdown_workers(
        &self,
        state: &AppState,
    ) {
        crate::lifecycle::drain::shutdown_workers(self, state).await
    }
}
/// Capability registration only; runtime jobs have their own named providers.
impl Component<AppState> for WorkerControl {
    type Deps = ();

    fn as_scoped(&self) -> Option<&dyn Scoped> {
        Some(self)
    }
    fn name(&self) -> &'static str {
        "WorkerControl"
    }

    fn order(&self) -> ComponentOrder {
        ComponentOrder::new()
            .after::<crate::lifecycle::master::MasterIdentityProvider>()
    }
}

#[cfg(test)]
#[path = "../../tests/management.rs"]
mod management_tests;
#[cfg(test)]
#[path = "../../tests/startup.rs"]
mod startup_tests;
#[cfg(test)]
use crate::lifecycle::reconcile::effective_worker_count_from_presence;

#[cfg(test)]
#[path = "../../tests/support.rs"]
mod test_support;
