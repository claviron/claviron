use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use claviron_runtime::congif::{self, ConfigFile, Configurable};
use claviron_runtime::registry::{
    self, Bootable, Component, ComponentOrder, Reloadable, Runnable,
};
use claviron_runtime::{AppState, Register, RunContext, ServiceManager};
use tokio::sync::watch;
use tracing::{debug, warn};

use super::{SNAPSHOT_PUBLISHER_NAME, snapshot};
use crate::config::{OrbitConfig, ServiceSnapshotPolicy};
use crate::placement::{Scope, Scoped};
use crate::shm::{ManagedShmProvider, ReusableOrbitRing};
use crate::{FleetEventRegistry, FleetProvider};

type ServiceSnapshotShm =
    ManagedShmProvider<ReusableOrbitRing<snapshot::ServiceSnapshotRecord>>;

/// Fleet service observations, configured through `orbit.yaml`.
///
/// `Register` installs the service command bridge and the supervised publisher.
/// Register Orbit's event bridge first. Continuo supplies `ServiceManager`
/// before boot; no manager or SHM handle is needed during registration.
/// Standalone installs only local command control, with no snapshot publisher.
pub struct ServiceSnapshotPublisher {
    config_path: PathBuf,
    absent_policy: ServiceSnapshotPolicy,
    policy: watch::Sender<ServiceSnapshotPolicy>,
}

impl ServiceSnapshotPublisher {
    pub(super) fn new(
        config_path: PathBuf,
        policy: ServiceSnapshotPolicy,
    ) -> Self {
        Self {
            config_path,
            absent_policy: policy,
            policy: watch::Sender::new(policy),
        }
    }

    pub fn policy(&self) -> ServiceSnapshotPolicy {
        *self.policy.borrow()
    }

    fn apply(
        &self,
        config: &OrbitConfig,
    ) -> registry::Result<()> {
        config.validate()?;
        self.policy.send_if_modified(|current| {
            if *current == config.service_snapshots {
                return false;
            }
            *current = config.service_snapshots;
            true
        });
        Ok(())
    }
}

impl Register<AppState> for ServiceSnapshotPublisher {
    fn register(state: &AppState) -> registry::Result<()> {
        super::register_with_policy(state, ServiceSnapshotPolicy::default())
    }
}

#[async_trait]
impl Configurable for ServiceSnapshotPublisher {
    type Config = OrbitConfig;

    fn source(&self) -> ConfigFile {
        ConfigFile::yaml(&self.config_path)
    }

    fn namespace(&self) -> String {
        "orbit".to_owned()
    }

    fn absent_source(&self) -> Option<OrbitConfig> {
        Some(OrbitConfig { service_snapshots: self.absent_policy })
    }

    fn validate_config(
        &self,
        config: &OrbitConfig,
    ) -> congif::Result<()> {
        config.validate().map_err(congif::Error::invalid)
    }

    async fn configure(
        &self,
        config: &OrbitConfig,
    ) -> congif::Result<()> {
        self.apply(config).map_err(congif::Error::invalid)
    }
}

#[async_trait]
impl Reloadable<AppState> for ServiceSnapshotPublisher {
    async fn reload(
        &self,
        state: &AppState,
    ) -> registry::Result<()> {
        let config = state.config_reader().snapshot::<Self>()?;
        self.apply(&config)
    }
}

impl Component<AppState> for ServiceSnapshotPublisher {
    type Deps = (Arc<ServiceManager>, Arc<FleetProvider>);

    fn name(&self) -> &'static str {
        SNAPSHOT_PUBLISHER_NAME
    }

    fn order(&self) -> ComponentOrder {
        ComponentOrder::new()
            .after::<FleetEventRegistry>()
            .after::<ServiceSnapshotShm>()
    }

    fn as_configurable(&self) -> Option<&dyn congif::ConfigRegistration> {
        Some(self)
    }

    fn as_reloadable(&self) -> Option<&dyn Reloadable<AppState>> {
        Some(self)
    }

    fn as_runnable(
        self: Arc<Self>
    ) -> Option<Arc<dyn Runnable<AppState, Self::Deps>>> {
        Some(self)
    }

    fn as_scoped(&self) -> Option<&dyn Scoped> {
        Some(self)
    }

    fn as_bootable(&self) -> Option<&dyn Bootable<AppState, Self::Deps>> {
        Some(self)
    }
}

#[async_trait]
impl Bootable<AppState, (Arc<ServiceManager>, Arc<FleetProvider>)>
    for ServiceSnapshotPublisher
{
    async fn boot(
        &self,
        _deps: (Arc<ServiceManager>, Arc<FleetProvider>),
        _state: &AppState,
    ) -> registry::Result<()> {
        self.policy().validate()?;
        Ok(())
    }
}

/// Every fleet member publishes its own service rows. A collector counts a
/// member whose lane is fresh, so a process without a running publisher is
/// reported as missing, and the fleet observation never reaches `complete`.
impl Scoped for ServiceSnapshotPublisher {
    fn scope(&self) -> Scope {
        Scope::Any
    }
}

#[async_trait]
impl Runnable<AppState, (Arc<ServiceManager>, Arc<FleetProvider>)>
    for ServiceSnapshotPublisher
{
    async fn run(
        self: Arc<Self>,
        (manager, fleet_provider): (Arc<ServiceManager>, Arc<FleetProvider>),
        state: AppState,
        context: RunContext,
    ) -> registry::Result<()> {
        let fleet = fleet_provider.handle().ok_or_else(|| {
            registry::Error::msg("Fleet is unavailable for service snapshots")
        })?;
        let mut policy = self.policy.subscribe();
        let mut cadence = policy.borrow_and_update().publish_interval();
        let mut interval = tokio::time::interval(cadence);
        interval
            .set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                biased;
                _ = context.cancelled() => {
                    debug!("service snapshot publisher stopped");
                    return Ok(());
                }
                // Reload wakes the existing generation directly. A freshness-only
                // update must not postpone publication; an unchanged policy emits
                // no notification. Cadence changes start one new declared period.
                changed = policy.changed() => {
                    changed.map_err(|error| registry::Error::msg(error.to_string()))?;
                    let next = policy.borrow_and_update().publish_interval();
                    if cadence != next {
                        cadence = next;
                        interval = tokio::time::interval_at(
                            tokio::time::Instant::now() + cadence, cadence,
                        );
                        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                    }
                    continue;
                }
                _ = interval.tick() => {}
            }

            let services = tokio::select! {
                biased;
                _ = context.cancelled() => {
                    debug!("service snapshot publisher stopped");
                    return Ok(());
                }
                result = manager.list() => result.map_err(|error| registry::Error::msg(error.to_string()))?,
            };
            let local = snapshot::local(&state, services);
            if let Err(error) = snapshot::publish(&fleet, &local) {
                warn!("service snapshot publication failed: {error}");
            }
        }
    }
}

#[cfg(test)]
#[path = "../../tests/service/publisher.rs"]
mod tests;
