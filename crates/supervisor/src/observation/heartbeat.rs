use std::sync::{Arc, RwLock};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use claviron_orbit::placement::{Scope, Scoped};
use claviron_orbit::{
    Fleet, FleetProvider, NetId64, NodeId, OrbitEpoch, OrbitTyped, RingSpec,
};
use claviron_runtime::congif::{
    self, ConfigFile, ConfigObserver, Configurable,
};
use claviron_runtime::registry::{
    self, Component, ComponentOrder, Reloadable, Runnable, priority,
};
use claviron_runtime::{AppState, RunContext};
use tokio::sync::Notify;
use tracing::debug;

use crate::lifecycle::WorkerIdentityProvider;
use crate::presence::publish_presence;

pub(crate) const HEARTBEAT_RING_KIND: u8 = 221;
pub(crate) const HEARTBEAT_RING_SPEC: RingSpec = RingSpec::per_node(1024, 0);
const HEARTBEAT_FRAME_KIND: u8 = 1;

/// Orbit type marker fixing the heartbeat ring's zero-payload lane geometry.
/// The pulse is carried by frame identity and capture time, not a metric payload.
#[derive(Clone, Debug)]
pub(crate) struct HeartbeatRecord;

impl OrbitTyped for HeartbeatRecord {
    const KIND: u8 = HEARTBEAT_RING_KIND;
    const RING_SPEC: RingSpec = HEARTBEAT_RING_SPEC;
}

/// Decoded lane pulse: node, monotonic record identity and capture time.
/// It proves only that the lane was written, not which process currently owns it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Heartbeat {
    pub(crate) id: NetId64,
    pub(crate) node_id: NodeId,
    pub(crate) captured_at: OrbitEpoch,
}

impl Heartbeat {
    pub(crate) fn counter(&self) -> u64 {
        self.id.counter()
    }

    pub(crate) fn age_at(
        &self,
        now: OrbitEpoch,
    ) -> Duration {
        self.captured_at.age_at(now)
    }

    fn from_frame(frame: claviron_orbit::Frame) -> Option<Self> {
        if frame.id.kind() != HEARTBEAT_RING_KIND
            || frame.kind != HEARTBEAT_FRAME_KIND
        {
            return None;
        }

        Some(Self {
            id: frame.id,
            node_id: NodeId::new(frame.id.node()),
            captured_at: OrbitEpoch::from_unix_ms(frame.ver),
        })
    }
}

pub(crate) fn publish_heartbeat(
    fleet: &Fleet,
    captured_at: OrbitEpoch,
) -> Heartbeat {
    let id = fleet.publish::<HeartbeatRecord>(
        HEARTBEAT_FRAME_KIND,
        captured_at.as_unix_ms(),
        Bytes::new(),
    );
    Heartbeat { id, node_id: fleet.node_id(), captured_at }
}

/// The most recent heartbeat published on one node's lane, whoever published
/// it.
///
/// Deliberately says nothing about *who*: the record carries no pid and no
/// state, so all a caller learns is that something was alive on this node id
/// as of some moment. That is enough to know the id is taken, and not enough
/// to know by what — see `REVIEW.md` §5f.
pub(crate) fn latest_for(
    fleet: &Fleet,
    node_id: NodeId,
) -> Option<Heartbeat> {
    let capacity = fleet.ring_capacity::<HeartbeatRecord>() as u64;
    let head = fleet.lane_head::<HeartbeatRecord>(node_id);
    let walk_count = head.min(capacity);

    (0..walk_count).find_map(|offset| {
        let counter = head - 1 - offset;
        let frame = fleet.read_lane_at::<HeartbeatRecord>(node_id, counter)?;
        let heartbeat = Heartbeat::from_frame(frame)?;
        (heartbeat.node_id == node_id).then_some(heartbeat)
    })
}

pub(crate) fn latest_heartbeats(fleet: &Fleet) -> Vec<Heartbeat> {
    (0..fleet.fleet_capacity())
        .filter_map(|node| latest_for(fleet, NodeId::new(node)))
        .collect()
}

/// Worker-side Runnable that publishes a liveness pulse and typed process
/// presence on the worker's own Orbit lanes at the heartbeat cadence.
///
/// It provides control evidence independently of optional metrics, using the
/// worker identity provider rather than inventing ownership. Targeted stop only
/// stops publication: it does not shut down the worker or clear its lanes.
/// Consequently the master may observe a stale heartbeat while it is stopped.
/// Restart resumes publication with the same process identity and runtime age.
pub(crate) struct HeartbeatProvider {
    source: crate::config::provider::PolicySource,
    interval: RwLock<Option<Duration>>,
    changed: Notify,
}

/// Decode the shared policy once, retaining only the heartbeat's runtime input.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct HeartbeatConfig(Duration);

impl<'de> serde::Deserialize<'de> for HeartbeatConfig {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D
    ) -> Result<Self, D::Error> {
        let policy =
            crate::config::SupervisorConfig::deserialize(deserializer)?;
        let policy = crate::config::validate(policy)
            .map_err(serde::de::Error::custom)?;
        Ok(Self(policy.heartbeat.interval))
    }
}

impl HeartbeatProvider {
    #[cfg(test)]
    pub(crate) fn new() -> Self {
        Self {
            source: crate::config::provider::PolicySource::fixture(),
            interval: RwLock::new(None),
            changed: Notify::new(),
        }
    }
    pub(crate) fn for_state(state: &AppState) -> registry::Result<Self> {
        let source = crate::config::provider::PolicySource::new(state);
        let interval = source
            .supplied()
            .map_err(|error| registry::Error::msg(error.to_string()))?
            .map(|config| config.heartbeat.interval);
        Ok(Self {
            source,
            interval: RwLock::new(interval),
            changed: Notify::new(),
        })
    }

    fn interval(&self) -> Duration {
        self.interval
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .expect("heartbeat interval applied before execution")
    }
}

impl Scoped for HeartbeatProvider {
    fn scope(&self) -> Scope {
        Scope::Worker
    }
}

impl Component<AppState> for HeartbeatProvider {
    type Deps = (Arc<FleetProvider>,);

    fn name(&self) -> &'static str {
        "HeartbeatProvider"
    }

    fn run_priority(&self) -> Option<u8> {
        Some(priority::EARLY)
    }

    fn order(&self) -> ComponentOrder {
        ComponentOrder::new()
            .after::<crate::config::provider::SupervisorPolicy>()
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
impl Configurable for HeartbeatProvider {
    type Config = HeartbeatConfig;
    fn source(&self) -> ConfigFile {
        self.source.file()
    }
    fn absent_source(&self) -> Option<Self::Config> {
        Some(HeartbeatConfig(
            crate::config::SupervisorConfig::default().heartbeat.interval,
        ))
    }
    fn as_config_observer(
        &self
    ) -> Option<&dyn ConfigObserver<Config = Self::Config>> {
        Some(self)
    }
    async fn configure(
        &self,
        config: &HeartbeatConfig,
    ) -> congif::Result<()> {
        *self.interval.write().unwrap_or_else(|error| error.into_inner()) =
            Some(config.0);
        self.changed.notify_one();
        Ok(())
    }
}

#[async_trait]
impl ConfigObserver for HeartbeatProvider {
    async fn config_changed(
        &self,
        config: &HeartbeatConfig,
    ) -> congif::Result<()> {
        self.configure(config).await
    }
}

#[async_trait]
impl Reloadable<AppState> for HeartbeatProvider {
    async fn reload(
        &self,
        state: &AppState,
    ) -> registry::Result<()> {
        let interval = if let Some(config) = self
            .source
            .supplied()
            .map_err(|error| registry::Error::msg(error.to_string()))?
        {
            config.heartbeat.interval
        } else {
            state
                .config_reader()
                .snapshot::<Self>()
                .map_err(|error| registry::Error::msg(error.to_string()))?
                .0
        };
        *self.interval.write().unwrap_or_else(|error| error.into_inner()) =
            Some(interval);
        self.changed.notify_one();
        Ok(())
    }
}

#[async_trait]
impl Runnable<AppState, (Arc<FleetProvider>,)> for HeartbeatProvider {
    async fn run(
        self: Arc<Self>,
        (fleet_provider,): (Arc<FleetProvider>,),
        state: AppState,
        context: RunContext,
    ) -> registry::Result<()> {
        let fleet = fleet_provider.handle().ok_or_else(|| {
            registry::Error::msg("Orbit fleet is unavailable for heartbeat")
        })?;
        // The pulse says something is alive on this lane; presence says who.
        // Both come from the worker itself, on the same tick.
        let me = state
            .registry_ref()
            .resolve::<WorkerIdentityProvider>()
            .ok_or_else(|| {
                registry::Error::msg(
                    "WorkerIdentityProvider is unavailable for worker presence",
                )
            })?;
        let mut ticker = tokio::time::interval(self.interval());
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = context.cancelled() => {
                    debug!("Orbit heartbeat provider shutting down");
                    return Ok(());
                }
                _ = self.changed.notified() => {
                    ticker = tokio::time::interval(self.interval());
                    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                }
                _ = ticker.tick() => {
                    let captured_at = OrbitEpoch::now();
                    let heartbeat = publish_heartbeat(fleet.as_ref(), captured_at);
                    let presence = publish_presence(
                        fleet.as_ref(),
                        me.presence(&state, captured_at)
                    )?;
                    tracing::trace!(
                        node_id = heartbeat.node_id.get(),
                        counter = heartbeat.counter(),
                        presence_counter = presence.counter(),
                        captured_at_unix_ms = heartbeat.captured_at.as_unix_ms(),
                        "Orbit worker heartbeat"
                    );
                }
            }
        }
    }
}

#[cfg(test)]
#[path = "../../tests/observation/heartbeat.rs"]
mod tests;
