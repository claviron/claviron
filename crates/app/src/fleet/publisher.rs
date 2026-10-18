use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use claviron_orbit::shm::{ManagedShmProvider, ReusableOrbitRing};
use claviron_orbit::transport::TransportSegment;
use claviron_orbit::{FleetEventRegistry, FleetProvider};
use claviron_runtime::registry::{
    self, Bootable, Component, ComponentOrder, Runnable,
};
use claviron_runtime::{AppState, RunContext};
use tracing::{debug, warn};

use super::snapshot;
use crate::ApplicationManager;

type ApplicationSnapshotShm =
    ManagedShmProvider<ReusableOrbitRing<snapshot::ApplicationSnapshotRecord>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ApplicationSnapshotPolicy {
    publish_interval: Duration,
    max_age: Duration,
}

impl ApplicationSnapshotPolicy {
    pub fn new(
        publish_interval: Duration,
        max_age: Duration,
    ) -> Self {
        Self { publish_interval, max_age }
    }

    pub fn publish_interval(self) -> Duration {
        self.publish_interval
    }

    pub fn max_age(self) -> Duration {
        self.max_age
    }

    pub(super) fn validate(self) -> registry::Result<()> {
        if self.publish_interval.is_zero() {
            return Err(registry::Error::msg(
                "application snapshot publish interval must be greater than zero",
            ));
        }
        if self.max_age.is_zero() {
            return Err(registry::Error::msg(
                "application snapshot max age must be greater than zero",
            ));
        }
        Ok(())
    }
}

impl Default for ApplicationSnapshotPolicy {
    fn default() -> Self {
        Self {
            publish_interval: Duration::from_secs(1),
            max_age: Duration::from_secs(5),
        }
    }
}

/// Owns application snapshot policy and master-side ring preparation at boot.
/// Only workers expose its publication loop as a managed runnable; the master
/// retains the provider for boot and policy lookup without a service placement.
pub(super) struct ApplicationSnapshotPublisher {
    policy: ApplicationSnapshotPolicy,
    publishes: bool,
}

impl ApplicationSnapshotPublisher {
    pub(super) fn new(
        state: &AppState,
        policy: ApplicationSnapshotPolicy,
    ) -> Self {
        Self { policy, publishes: state.is_worker() }
    }

    pub(super) fn policy(&self) -> ApplicationSnapshotPolicy {
        self.policy
    }
}

impl Component<AppState> for ApplicationSnapshotPublisher {
    /// What a publishing generation reads and writes. Boot only validates the
    /// policy, but it is handed the same dependencies.
    type Deps = (Arc<ApplicationManager>, Arc<FleetProvider>);

    fn name(&self) -> &'static str {
        "ApplicationSnapshotPublisher"
    }

    fn order(&self) -> ComponentOrder {
        ComponentOrder::new()
            .after::<FleetEventRegistry>()
            .after::<ApplicationSnapshotShm>()
    }

    fn as_runnable(
        self: Arc<Self>
    ) -> Option<Arc<dyn Runnable<AppState, Self::Deps>>> {
        if self.publishes { Some(self) } else { None }
    }

    fn as_bootable(&self) -> Option<&dyn Bootable<AppState, Self::Deps>> {
        Some(self)
    }
}

#[async_trait]
impl Bootable<AppState, (Arc<ApplicationManager>, Arc<FleetProvider>)>
    for ApplicationSnapshotPublisher
{
    async fn boot(
        &self,
        _deps: (Arc<ApplicationManager>, Arc<FleetProvider>),
        _state: &AppState,
    ) -> registry::Result<()> {
        self.policy.validate()?;
        Ok(())
    }
}

#[async_trait]
impl Runnable<AppState, (Arc<ApplicationManager>, Arc<FleetProvider>)>
    for ApplicationSnapshotPublisher
{
    async fn run(
        self: Arc<Self>,
        (manager, fleet_provider): (
            Arc<ApplicationManager>,
            Arc<FleetProvider>,
        ),
        state: AppState,
        context: RunContext,
    ) -> registry::Result<()> {
        let fleet = fleet_provider.handle().ok_or_else(|| {
            registry::Error::msg(
                "Fleet is unavailable for application snapshots",
            )
        })?;
        let member = super::local_member(&state);
        let mut interval = tokio::time::interval(self.policy.publish_interval);
        interval
            .set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                biased;
                _ = context.cancelled() => {
                    debug!("application snapshot publisher stopped");
                    return Ok(());
                }
                _ = interval.tick() => {}
            }
            let local = snapshot::local(&state, manager.list());
            if let Err(error) = snapshot::publish(&fleet, &local, member) {
                warn!(%error, "application snapshot publication failed");
            }
            match TransportSegment::existing_for_fleet(state.fleet_name()) {
                Ok(Some(segment)) => {
                    match snapshot::encode_local_rows(&local, member) {
                        Ok(rows) => {
                            if let Err(error) = segment
                                .publish_application_state(state.name(), &rows)
                            {
                                warn!(%error, "linked application snapshot publication failed");
                            }
                        }
                        Err(error) => {
                            warn!(%error, "linked application snapshot encoding failed")
                        }
                    }
                }
                Ok(None) => {}
                Err(error) => {
                    warn!(%error, "linked application transport is unavailable")
                }
            }
        }
    }
}

#[cfg(test)]
#[path = "../../tests/fleet/publisher.rs"]
mod tests;
