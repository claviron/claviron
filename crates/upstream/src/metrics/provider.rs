//! Owns provider policy and runtime helpers.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use claviron_metrics::{MetricsFamily, MetricsProvider};
use claviron_orbit::{FleetProvider, OrbitStateExt};
use claviron_runtime::registry::{Component, Runnable, priority};
use claviron_runtime::{AppState, RunContext, registry};
use orbitive::metrics::{OrbitMetricCollector, OrbitMetricPublisher};
use tracing::{debug, error};

use super::orbit::UpstreamOrbitMetrics;
use super::{UPSTREAM_METRICS, UpstreamScalarMetricsSnapshot};
use crate::UpstreamProvider;

struct UpstreamMetricsFamily {
    provider: Arc<UpstreamProvider>,
}

impl MetricsFamily for UpstreamMetricsFamily {
    fn name(&self) -> &'static str {
        "claviron.upstream"
    }

    fn write_prometheus(
        &self,
        state: &AppState,
        output: &mut String,
    ) -> Result<(), String> {
        self.provider.write_resolution_metrics(output)?;
        if state.is_standalone() {
            UPSTREAM_METRICS.write_prometheus(output);
            return Ok(());
        }

        if state.is_worker() {
            let fleet = state.fleet().ok_or_else(|| {
                "Orbit fleet is unavailable for upstream metrics".to_string()
            })?;
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|err| {
                    format!("system clock is before Unix epoch: {err}")
                })?
                .as_secs();
            let max_age = self.provider.metrics_options().max_age.as_secs();
            let collector =
                OrbitMetricCollector::<UpstreamOrbitMetrics>::new(fleet);
            let mut aggregate = UpstreamScalarMetricsSnapshot::default();
            for sample in collector.fresh_by_node(now, max_age).into_values() {
                aggregate.merge(&sample.snapshot.into_snapshot());
            }
            aggregate.write_prometheus(output);
        }

        Ok(())
    }
}

struct UpstreamMetricsPublisher {
    provider: Arc<UpstreamProvider>,
}

impl Component<AppState> for UpstreamMetricsPublisher {
    type Deps = (Arc<FleetProvider>,);

    fn name(&self) -> &'static str {
        "UpstreamMetricsPublisher"
    }

    fn run_priority(&self) -> Option<u8> {
        Some(priority::EARLY)
    }

    fn as_runnable(
        self: Arc<Self>
    ) -> Option<Arc<dyn Runnable<AppState, Self::Deps>>> {
        Some(self)
    }
}

#[async_trait]
impl Runnable<AppState, (Arc<FleetProvider>,)> for UpstreamMetricsPublisher {
    async fn run(
        self: Arc<Self>,
        (fleet_provider,): (Arc<FleetProvider>,),
        state: AppState,
        context: RunContext,
    ) -> registry::Result<()> {
        let fleet = fleet_provider.handle().ok_or_else(|| {
            registry::Error::msg(
                "Orbit fleet is unavailable for upstream metrics",
            )
        })?;
        let publisher =
            OrbitMetricPublisher::<UpstreamOrbitMetrics>::new(fleet);
        let mut interval = self.provider.metrics_options().interval;
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = context.cancelled() => {
                    debug!("upstream metrics publisher shutting down");
                    return Ok(());
                }
                _ = ticker.tick() => {
                    let current_interval = self.provider.metrics_options().interval;
                    if current_interval != interval {
                        interval = current_interval;
                        ticker = tokio::time::interval(interval);
                        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                    }

                    let captured_at = match SystemTime::now().duration_since(UNIX_EPOCH) {
                        Ok(duration) => duration.as_secs(),
                        Err(err) => {
                            error!("cannot publish upstream metrics before Unix epoch: {err}");
                            continue;
                        }
                    };
                    let snapshot = UpstreamScalarMetricsSnapshot::capture();
                    let record = UpstreamOrbitMetrics::from_snapshot(
                        state.fleet_node_id().raw(),
                        captured_at,
                        &snapshot
                    );
                    if let Err(err) = publisher.publish(&record) {
                        error!("failed to publish upstream metrics to Orbit: {err}");
                    }
                }
            }
        }
    }
}

pub fn register(
    state: &AppState,
    _provider: Arc<UpstreamProvider>,
) -> registry::Result<()> {
    if state.registry_ref().resolve::<MetricsProvider>().is_none() {
        claviron_metrics::register(state)?;
    }
    state
        .registry_ref()
        .resolve::<MetricsProvider>()
        .ok_or_else(|| registry::Error::msg("MetricsProvider is unavailable"))?
        .register(Arc::new(UpstreamMetricsFamily {
            provider: _provider.clone(),
        }));

    if !state.is_standalone() {
        claviron_orbit::shm::register_reusable_ring::<UpstreamOrbitMetrics>(
            state,
            "UpstreamMetricsShm",
            "upstream-metrics-ring",
        )?;
    }

    if state.is_worker() {
        state.registry_ref().register(Arc::new(UpstreamMetricsPublisher {
            provider: _provider,
        }))?;
    }

    Ok(())
}
