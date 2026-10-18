use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use claviron_metrics::{MetricsFamily, MetricsProvider};
use claviron_orbit::shm::{ManagedShmProvider, ReusableOrbitRing};
use claviron_orbit::{FleetProvider, OrbitStateExt};
use claviron_runtime::registry::{Component, Runnable, priority};
use claviron_runtime::{AppState, RunContext, registry};
use orbitive::metrics::{OrbitMetricCollector, OrbitMetricPublisher};
use tracing::{debug, error};

use super::config::{PhpEmbedMetricsConfig, PhpEmbedMetricsConfigProvider};
use super::{PHP_EMBED_METRICS, PhpEmbedMetricsSnapshot, PhpEmbedOrbitMetrics};

struct PhpEmbedMetricsFamily {
    config: Arc<PhpEmbedMetricsConfigProvider>,
}

impl MetricsFamily for PhpEmbedMetricsFamily {
    fn name(&self) -> &'static str {
        "claviron.ephp"
    }

    fn write_prometheus(
        &self,
        state: &AppState,
        output: &mut String,
    ) -> Result<(), String> {
        let config = self.config.config();
        if !config.enabled {
            return Ok(());
        }
        if state.is_standalone() {
            PHP_EMBED_METRICS.write_prometheus(output);
            return Ok(());
        }

        if state.is_worker() {
            let fleet = state.fleet().ok_or_else(|| {
                "Orbit fleet is unavailable for PHP embed metrics".to_string()
            })?;
            let now = now_seconds()?;
            let collector =
                OrbitMetricCollector::<PhpEmbedOrbitMetrics>::new(fleet);
            let mut aggregate = PhpEmbedMetricsSnapshot::default();
            for sample in collector
                .fresh_by_node(now, config.max_age.as_secs())
                .into_values()
            {
                aggregate.merge(&sample.snapshot.into_snapshot());
            }
            aggregate.write_prometheus(output);
        }
        Ok(())
    }
}

struct PhpEmbedMetricsPublisher {
    config: Arc<PhpEmbedMetricsConfigProvider>,
}

impl Component<AppState> for PhpEmbedMetricsPublisher {
    type Deps = (
        Arc<FleetProvider>,
        Arc<ManagedShmProvider<ReusableOrbitRing<PhpEmbedOrbitMetrics>>>,
    );

    fn name(&self) -> &'static str {
        "PhpEmbedMetricsPublisher"
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
impl Runnable<AppState, <Self as Component<AppState>>::Deps>
    for PhpEmbedMetricsPublisher
{
    async fn run(
        self: Arc<Self>,
        (fleet, _ring): <Self as Component<AppState>>::Deps,
        state: AppState,
        context: RunContext,
    ) -> registry::Result<()> {
        let fleet = fleet.handle().ok_or_else(|| {
            registry::Error::msg(
                "Orbit fleet is unavailable for PHP embed metrics",
            )
        })?;
        let publisher =
            OrbitMetricPublisher::<PhpEmbedOrbitMetrics>::new(fleet);
        let config = self.config.config();
        let mut ticker = tokio::time::interval(config.publish_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = context.cancelled() => {
                    debug!("PHP embed metrics publisher shutting down");
                    return Ok(());
                }
                _ = ticker.tick() => {
                    if !config.enabled {
                        continue;
                    }
                    let captured_at = match now_seconds() {
                        Ok(now) => now,
                        Err(err) => {
                            error!("cannot publish PHP embed metrics: {err}");
                            continue;
                        }
                    };
                    let snapshot = PhpEmbedMetricsSnapshot::capture();
                    let record = PhpEmbedOrbitMetrics::from_snapshot(
                        state.fleet_node_id().raw(),
                        captured_at,
                        &snapshot
                    );
                    if let Err(err) = publisher.publish(&record) {
                        error!("failed to publish PHP embed metrics to Orbit: {err}");
                    }
                }
            }
        }
    }
}

pub fn register(
    state: &AppState,
    policy: PhpEmbedMetricsConfig,
) -> registry::Result<()> {
    policy.validate().map_err(registry::Error::msg)?;
    if state.registry_ref().resolve::<MetricsProvider>().is_none() {
        claviron_metrics::register(state)?;
    }
    let config = Arc::new(PhpEmbedMetricsConfigProvider::new(policy));
    state.registry_ref().insert(config.clone())?;
    state
        .registry_ref()
        .resolve::<MetricsProvider>()
        .ok_or_else(|| registry::Error::msg("MetricsProvider is unavailable"))?
        .register(Arc::new(PhpEmbedMetricsFamily { config: config.clone() }));

    if !state.is_standalone() {
        claviron_orbit::shm::register_reusable_ring::<PhpEmbedOrbitMetrics>(
            state,
            "PhpEmbedMetricsShm",
            "php-embed-metrics-ring",
        )?;
    }

    if state.is_worker() {
        state
            .registry_ref()
            .register(Arc::new(PhpEmbedMetricsPublisher { config }))?;
    }
    Ok(())
}

fn now_seconds() -> Result<u64, String> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|err| format!("system clock is before Unix epoch: {err}"))
}
