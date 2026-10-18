use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use claviron_orbit::OrbitStateExt;
use claviron_runtime::metrics::{MetricsFamily, MetricsProvider};
use claviron_runtime::{AppState, registry};
use orbitive::metrics::OrbitMetricCollector;

use super::fcgi::{
    FCGI_METRICS, FCGI_REQUEST_METRICS, FcgiMetricsProvider, FcgiOrbitMetrics,
    FcgiScalarMetricsSnapshot, freshness_bound,
};
use super::fpm::{FPM_METRICS, FpmMetricsSnapshot, FpmPoolOrbitMetrics};

struct FcgiMetricsFamily {
    source: Arc<FcgiMetricsProvider>,
}

impl MetricsFamily for FcgiMetricsFamily {
    fn name(&self) -> &'static str {
        "claviron.fcgi"
    }

    fn write_prometheus(
        &self,
        state: &AppState,
        output: &mut String,
    ) -> Result<(), String> {
        let Some(_plan) = self.source.metrics_plan() else { return Ok(()) };
        if state.is_standalone() {
            FCGI_REQUEST_METRICS.write_prometheus(output);
            FCGI_METRICS.write_prometheus(output);
            return Ok(());
        }

        if state.is_worker() {
            let plan = _plan;
            let fleet = state.fleet().ok_or_else(|| {
                "Orbit fleet is unavailable for FCGI metrics".to_string()
            })?;
            let bound = freshness_bound(plan.max_age, plan.publish_interval);
            let now = now_seconds()?;

            let collector =
                OrbitMetricCollector::<FcgiOrbitMetrics>::new(fleet.clone());
            let mut aggregate = FcgiScalarMetricsSnapshot::default();
            for sample in
                collector.fresh_by_node(now, bound.as_secs()).into_values()
            {
                aggregate.merge(&sample.snapshot.into_snapshot());
            }
            aggregate.write_prometheus(output);
        }
        Ok(())
    }
}

struct FpmMetricsFamily {
    source: Arc<FcgiMetricsProvider>,
}

impl MetricsFamily for FpmMetricsFamily {
    fn name(&self) -> &'static str {
        "claviron.fpm"
    }

    fn write_prometheus(
        &self,
        state: &AppState,
        output: &mut String,
    ) -> Result<(), String> {
        let Some(_plan) = self.source.metrics_plan() else { return Ok(()) };
        if state.is_standalone() {
            FPM_METRICS.write_prometheus(output);
            return Ok(());
        }

        if state.is_worker() {
            let plan = _plan;
            let fleet = state.fleet().ok_or_else(|| {
                "Orbit fleet is unavailable for FPM metrics".to_string()
            })?;
            // Against the scrape cadence, not the publication one: these
            // rows are written by the master every `metrics.interval`,
            // and a bound below that hides them for the rest of the
            // period.
            let bound = freshness_bound(plan.max_age, plan.scrape_interval);
            let samples =
                OrbitMetricCollector::<FpmPoolOrbitMetrics>::new(fleet)
                    .fresh_by_key(now_seconds()?, bound.as_secs());
            let mut scraped_at = 0;
            let mut pools = Vec::with_capacity(samples.len());
            for sample in samples.into_values() {
                scraped_at = scraped_at.max(sample.snapshot.scraped_at);
                pools.push(sample.snapshot.into_pool());
            }
            FpmMetricsSnapshot::new(scraped_at, pools).write_prometheus(output);
        }
        Ok(())
    }
}

pub fn register(
    state: &AppState,
    source: Arc<FcgiMetricsProvider>,
) -> registry::Result<()> {
    if state.registry_ref().resolve::<MetricsProvider>().is_none() {
        claviron_runtime::metrics::register(state)?;
    }
    let provider =
        state.registry_ref().resolve::<MetricsProvider>().ok_or_else(|| {
            registry::Error::msg("MetricsProvider is unavailable")
        })?;
    provider.register(Arc::new(FcgiMetricsFamily { source: source.clone() }));
    provider.register(Arc::new(FpmMetricsFamily { source }));

    if !state.is_standalone() {
        claviron_orbit::shm::register_reusable_ring::<FcgiOrbitMetrics>(
            state,
            "PhpFcgiMetricsShm",
            "php-fcgi-metrics-ring",
        )?;
        claviron_orbit::shm::register_reusable_ring::<FpmPoolOrbitMetrics>(
            state,
            "FpmPoolMetricsShm",
            "fpm-pool-metrics-ring",
        )?;
    }
    Ok(())
}

fn now_seconds() -> Result<u64, String> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|err| format!("system clock is before Unix epoch: {err}"))
}
