//! HTTP-owned metric families and their optional fleet transport.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use claviron_metrics::{MetricsFamily, MetricsProvider};
use claviron_orbit::{FleetProvider, OrbitStateExt};
use claviron_runtime::registry::{Component, Runnable, priority};
use claviron_runtime::{AppState, RunContext, registry};
use orbitive::metrics::{OrbitMetricCollector, OrbitMetricPublisher};
use tracing::{debug, error};

use super::orbit::{HttpMetricsRecord, ServerMetricsRecord};
use super::{
    HTTP_METRICS, HttpMetricsConfig, HttpMetricsSnapshot, SERVER_METRICS,
    ServerMetricsSnapshot,
};

/// The current metrics configuration, read afresh wherever it is used.
type ConfigReader = Arc<dyn Fn() -> Arc<HttpMetricsConfig> + Send + Sync>;

struct HttpMetricsFamily {
    config: ConfigReader,
}

impl MetricsFamily for HttpMetricsFamily {
    fn name(&self) -> &'static str {
        "claviron.http"
    }

    fn write_prometheus(
        &self,
        state: &AppState,
        output: &mut String,
    ) -> Result<(), String> {
        let config = (self.config)();
        if !config.enabled {
            return Ok(());
        }

        if state.is_standalone() {
            HTTP_METRICS.write_prometheus(output);
            return Ok(());
        }

        if state.is_worker() {
            let fleet = state.fleet().ok_or_else(|| {
                "Orbit fleet is unavailable for HTTP metrics".to_string()
            })?;
            let collector =
                OrbitMetricCollector::<HttpMetricsRecord>::new(fleet);
            let mut http = HttpMetricsSnapshot::default();
            for sample in collector
                .fresh_by_node(now_seconds()?, config.max_age.as_secs())
                .into_values()
            {
                http.merge(&sample.snapshot.snapshot);
            }
            http.write_prometheus(output);
        }

        Ok(())
    }
}

struct ServerMetricsFamily {
    config: ConfigReader,
}

impl MetricsFamily for ServerMetricsFamily {
    fn name(&self) -> &'static str {
        "claviron.server"
    }

    fn write_prometheus(
        &self,
        state: &AppState,
        output: &mut String,
    ) -> Result<(), String> {
        let config = (self.config)();
        if !config.enabled {
            return Ok(());
        }

        if state.is_standalone() {
            SERVER_METRICS.write_prometheus(output);
            return Ok(());
        }

        if state.is_worker() {
            let fleet = state.fleet().ok_or_else(|| {
                "Orbit fleet is unavailable for server metrics".to_string()
            })?;
            let collector =
                OrbitMetricCollector::<ServerMetricsRecord>::new(fleet);
            let mut server = ServerMetricsSnapshot::default();
            for sample in collector
                .fresh_by_node(now_seconds()?, config.max_age.as_secs())
                .into_values()
            {
                server.merge(&sample.snapshot.snapshot);
            }
            server.write_prometheus(output);
        }

        Ok(())
    }
}

struct HttpMetricsPublisher {
    config: ConfigReader,
}

impl Component<AppState> for HttpMetricsPublisher {
    type Deps = (Arc<FleetProvider>,);

    fn name(&self) -> &'static str {
        "HttpMetricsPublisher"
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
impl Runnable<AppState, (Arc<FleetProvider>,)> for HttpMetricsPublisher {
    async fn run(
        self: Arc<Self>,
        (fleet_provider,): (Arc<FleetProvider>,),
        state: AppState,
        context: RunContext,
    ) -> registry::Result<()> {
        let fleet = fleet_provider.handle().ok_or_else(|| {
            registry::Error::msg("Orbit fleet is unavailable for HTTP metrics")
        })?;
        let publisher = OrbitMetricPublisher::<HttpMetricsRecord>::new(fleet);
        let mut publish_interval =
            nonzero_interval((self.config)().publish_interval);
        let mut ticker = tokio::time::interval(publish_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = context.cancelled() => {
                    debug!("HTTP metrics publisher shutting down");
                    return Ok(());
                }
                _ = ticker.tick() => {
                    let config = (self.config)();
                    let current_interval = nonzero_interval(config.publish_interval);
                    if current_interval != publish_interval {
                        publish_interval = current_interval;
                        ticker = tokio::time::interval(publish_interval);
                        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                    }
                    if !config.enabled {
                        continue;
                    }

                    let captured_at = match now_seconds() {
                        Ok(now) => now,
                        Err(err) => {
                            error!("cannot publish HTTP metrics: {err}");
                            continue;
                        }
                    };
                    let record = HttpMetricsRecord::new(
                        state.fleet_node_id().raw(),
                        captured_at,
                        HttpMetricsSnapshot::capture()
                    );
                    if let Err(err) = publisher.publish(&record) {
                        error!("failed to publish HTTP metrics to Orbit: {err}");
                    }
                }
            }
        }
    }
}

struct ServerMetricsPublisher {
    config: ConfigReader,
}

impl Component<AppState> for ServerMetricsPublisher {
    type Deps = (Arc<FleetProvider>,);

    fn name(&self) -> &'static str {
        "ServerMetricsPublisher"
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
impl Runnable<AppState, (Arc<FleetProvider>,)> for ServerMetricsPublisher {
    async fn run(
        self: Arc<Self>,
        (fleet_provider,): (Arc<FleetProvider>,),
        state: AppState,
        context: RunContext,
    ) -> registry::Result<()> {
        let fleet = fleet_provider.handle().ok_or_else(|| {
            registry::Error::msg(
                "Orbit fleet is unavailable for server metrics",
            )
        })?;
        let publisher = OrbitMetricPublisher::<ServerMetricsRecord>::new(fleet);
        let mut publish_interval =
            nonzero_interval((self.config)().publish_interval);
        let mut ticker = tokio::time::interval(publish_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = context.cancelled() => {
                    debug!("server metrics publisher shutting down");
                    return Ok(());
                }
                _ = ticker.tick() => {
                    let config = (self.config)();
                    let current_interval = nonzero_interval(config.publish_interval);
                    if current_interval != publish_interval {
                        publish_interval = current_interval;
                        ticker = tokio::time::interval(publish_interval);
                        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                    }
                    if !config.enabled {
                        continue;
                    }

                    let captured_at = match now_seconds() {
                        Ok(now) => now,
                        Err(err) => {
                            error!("cannot publish server metrics: {err}");
                            continue;
                        }
                    };
                    let record = ServerMetricsRecord::new(
                        state.fleet_node_id().raw(),
                        captured_at,
                        ServerMetricsSnapshot::capture()
                    );
                    if let Err(err) = publisher.publish(&record) {
                        error!("failed to publish server metrics to Orbit: {err}");
                    }
                }
            }
        }
    }
}

pub(crate) fn register(
    state: &AppState,
    config: ConfigReader,
) -> registry::Result<()> {
    if state.registry_ref().resolve::<MetricsProvider>().is_none() {
        claviron_metrics::register(state)?;
    }
    let metrics =
        state.registry_ref().resolve::<MetricsProvider>().ok_or_else(|| {
            registry::Error::msg("MetricsProvider is unavailable")
        })?;
    metrics.register(Arc::new(HttpMetricsFamily { config: config.clone() }));
    metrics.register(Arc::new(ServerMetricsFamily { config: config.clone() }));

    if !state.is_standalone() {
        claviron_orbit::shm::register_reusable_ring::<HttpMetricsRecord>(
            state,
            "HttpMetricsShm",
            "http-metrics-ring",
        )?;
        claviron_orbit::shm::register_reusable_ring::<ServerMetricsRecord>(
            state,
            "ServerMetricsShm",
            "server-metrics-ring",
        )?;
    }

    if state.is_worker() {
        state.registry_ref().register(Arc::new(HttpMetricsPublisher {
            config: config.clone(),
        }))?;
        state
            .registry_ref()
            .register(Arc::new(ServerMetricsPublisher { config }))?;
    }
    Ok(())
}

fn nonzero_interval(interval: Duration) -> Duration {
    if interval.is_zero() { Duration::from_secs(1) } else { interval }
}

fn now_seconds() -> Result<u64, String> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|err| format!("system clock is before Unix epoch: {err}"))
}
