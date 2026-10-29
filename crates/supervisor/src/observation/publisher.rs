//! Master-side publication of the supervisor's authoritative process table.

use std::sync::{Arc, RwLock};
use std::time::Duration;

use async_trait::async_trait;
use claviron_orbit::placement::{Scope, Scoped};
use claviron_orbit::shm::{ManagedShmProvider, ReusableOrbitRing};
use claviron_orbit::{FleetEventRegistry, FleetProvider};
use claviron_runtime::congif::{
    self, ConfigFile, ConfigObserver, Configurable,
};
use claviron_runtime::registry::{
    self, Component, ComponentOrder, Reloadable, Runnable,
};
use claviron_runtime::{AppState, RunContext};
use tokio::sync::Notify;
use tracing::{debug, warn};

use crate::control::WorkerControl;

type WorkerSnapshotShm = ManagedShmProvider<
    ReusableOrbitRing<crate::snapshot::WorkerSnapshotRecord>,
>;

/// Master-side Runnable that publishes the fleet table as a coherent management
/// snapshot for CLI, GUI and service-membership readers.
///
/// This is a read-model publisher, not a source of lifecycle decisions. Its
/// managed storage reuses and preserves compatible snapshots; each Runnable
/// generation publishes at the configured cadence. Targeted stop pauses
/// publication without clearing the ring or changing workers. Restart publishes
/// current state again, preserving process identity and lifecycle clocks.
pub(crate) struct WorkerSnapshotPublisher {
    supervisor: Arc<WorkerControl>,
    source: crate::config::provider::PolicySource,
    policy: RwLock<Option<SnapshotPublication>>,
    changed: Notify,
}

/// Only the values needed to publish the management read model.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SnapshotPublication {
    interval: Duration,
    max_age: Duration,
}

impl From<crate::config::WorkerSnapshotConfig> for SnapshotPublication {
    fn from(config: crate::config::WorkerSnapshotConfig) -> Self {
        Self { interval: config.publish_interval, max_age: config.max_age }
    }
}

impl<'de> serde::Deserialize<'de> for SnapshotPublication {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D
    ) -> Result<Self, D::Error> {
        let policy =
            crate::config::SupervisorConfig::deserialize(deserializer)?;
        let policy = crate::config::validate(policy)
            .map_err(serde::de::Error::custom)?;
        Ok(policy.snapshot.into())
    }
}

impl WorkerSnapshotPublisher {
    #[cfg(test)]
    pub(crate) fn new(supervisor: Arc<WorkerControl>) -> Self {
        let policy = supervisor.config().snapshot.clone().into();
        Self {
            supervisor,
            source: crate::config::provider::PolicySource::fixture(),
            policy: RwLock::new(Some(policy)),
            changed: Notify::new(),
        }
    }

    pub(crate) fn for_state(
        supervisor: Arc<WorkerControl>,
        state: &AppState,
    ) -> registry::Result<Self> {
        let source = crate::config::provider::PolicySource::new(state);
        let policy = source
            .supplied()
            .map_err(|error| registry::Error::msg(error.to_string()))?
            .map(|config| config.snapshot.into());
        Ok(Self {
            supervisor,
            source,
            policy: RwLock::new(policy),
            changed: Notify::new(),
        })
    }

    fn policy(&self) -> SnapshotPublication {
        self.policy
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .expect("snapshot publication configured before execution")
    }

    fn apply(
        &self,
        policy: SnapshotPublication,
    ) {
        let previous = self
            .policy
            .write()
            .unwrap_or_else(|error| error.into_inner())
            .replace(policy);
        if previous.is_some() {
            self.changed.notify_one();
        }
    }
}

impl Scoped for WorkerSnapshotPublisher {
    fn scope(&self) -> Scope {
        Scope::Master
    }
}

impl Component<AppState> for WorkerSnapshotPublisher {
    type Deps = (Arc<FleetProvider>,);

    fn name(&self) -> &'static str {
        "WorkerSnapshotPublisher"
    }

    fn order(&self) -> ComponentOrder {
        ComponentOrder::new()
            .after::<FleetEventRegistry>()
            .after::<WorkerControl>()
            .after::<WorkerSnapshotShm>()
    }

    fn as_runnable(
        self: Arc<Self>
    ) -> Option<Arc<dyn Runnable<AppState, Self::Deps>>> {
        Some(self)
    }

    fn as_scoped(&self) -> Option<&dyn Scoped> {
        Some(self)
    }

    fn as_configurable(&self) -> Option<&dyn congif::ConfigRegistration> {
        self.source.custom().is_none().then_some(self)
    }

    fn as_reloadable(&self) -> Option<&dyn Reloadable<AppState>> {
        Some(self)
    }
}

#[async_trait]
impl Configurable for WorkerSnapshotPublisher {
    type Config = SnapshotPublication;

    fn source(&self) -> ConfigFile {
        self.source.file()
    }
    fn absent_source(&self) -> Option<Self::Config> {
        Some(crate::config::SupervisorConfig::default().snapshot.into())
    }

    fn as_config_observer(
        &self
    ) -> Option<&dyn ConfigObserver<Config = Self::Config>> {
        Some(self)
    }

    async fn configure(
        &self,
        config: &SnapshotPublication,
    ) -> congif::Result<()> {
        self.apply(*config);
        Ok(())
    }
}

#[async_trait]
impl ConfigObserver for WorkerSnapshotPublisher {
    async fn config_changed(
        &self,
        config: &SnapshotPublication,
    ) -> congif::Result<()> {
        self.configure(config).await
    }
}

#[async_trait]
impl Reloadable<AppState> for WorkerSnapshotPublisher {
    async fn reload(
        &self,
        state: &AppState,
    ) -> registry::Result<()> {
        let policy = if let Some(config) = self
            .source
            .supplied()
            .map_err(|error| registry::Error::msg(error.to_string()))?
        {
            config.snapshot.into()
        } else {
            *state
                .config_reader()
                .snapshot::<Self>()
                .map_err(|error| registry::Error::msg(error.to_string()))?
        };
        self.apply(policy);
        Ok(())
    }
}

#[async_trait]
impl Runnable<AppState, (Arc<FleetProvider>,)> for WorkerSnapshotPublisher {
    async fn run(
        self: Arc<Self>,
        (fleet_provider,): (Arc<FleetProvider>,),
        state: AppState,
        context: RunContext,
    ) -> claviron_runtime::registry::Result<()> {
        let fleet = fleet_provider.handle().ok_or_else(|| {
            claviron_runtime::registry::Error::msg(
                "Fleet is unavailable for worker snapshots",
            )
        })?;
        self.publish_until_stopped(state, context, &fleet).await
    }
}

impl WorkerSnapshotPublisher {
    async fn publish_until_stopped(
        &self,
        state: AppState,
        context: RunContext,
        fleet: &claviron_orbit::Fleet,
    ) -> claviron_runtime::registry::Result<()> {
        loop {
            if context.is_cancelled() {
                return Ok(());
            }
            let policy = self.policy();
            let snapshot = crate::observation::project::management_snapshot(
                &self.supervisor,
                &state,
            )
            .await;
            if let Err(error) =
                crate::snapshot::publish(fleet, &snapshot, policy.max_age)
            {
                warn!("worker snapshot publication failed: {error}");
            }
            tokio::select! {
                _ = context.cancelled() => {
                    debug!("worker snapshot publisher stopped");
                    return Ok(());
                }
                _ = self.changed.notified() => {}
                _ = tokio::time::sleep(policy.interval) => {}
            }
        }
    }
}

#[cfg(test)]
#[path = "../../tests/observation/publisher.rs"]
mod tests;
