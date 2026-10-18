//! Worker-side FCGI client pool metrics provider.
//!
//! Mirrors `FpmMetricsProvider` in shape (Provider + Reloadable + Runnable
//! with an interval ticker) but inverts the master/worker gating: pool
//! state lives **inside each worker process**, so this provider runs on
//! workers and never on master. Capture cost is microseconds (atomic
//! loads + a brief read lock on the entries map), no I/O.
//!
//! Output is published via this crate's `FCGI_METRICS` store. With Orbit,
//! workers additionally publish the scalar capacity projection through kind
//! `215`; the owner-local `MetricsFamily` renders local or fleet state.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arc_swap::ArcSwapOption;
use async_trait::async_trait;
use claviron_orbit::OrbitStateExt;
use claviron_orbit::placement::{Scope, Scoped};
use claviron_runtime::congif::{
    self, ConfigFile, ConfigObserver, Configurable,
};
use claviron_runtime::registry::{Component, Reloadable, Runnable, priority};
use claviron_runtime::{AppState, RunContext};
use orbitive::metrics::OrbitMetricPublisher;
use tokio::time::{Instant, MissedTickBehavior, interval_at};
use tracing::{debug, info};

use crate::connection::FcgiEndpoint;
use crate::metrics::config::{FpmMetricsConfig, MetricsSection};
use crate::metrics::fcgi::{
    FCGI_METRICS, FCGI_REQUEST_METRICS, FcgiMetricsSnapshot, FcgiOrbitMetrics,
    FcgiPoolMetrics,
};
use crate::service::PhpFcgiService;

#[derive(Clone)]
pub struct FcgiMetricsProvider {
    config_path: PathBuf,
    plan: Arc<ArcSwapOption<FcgiMetricsPlan>>,
}

#[derive(Clone)]
pub(crate) struct FcgiMetricsPlan {
    /// How often a worker publishes the FCGI family (kind `215`).
    pub(crate) publish_interval: Duration,

    /// How often the master scrapes FPM and publishes the FPM family
    /// (kind `216`).
    ///
    /// Carried here although this provider does not scrape, because the
    /// family that *renders* those rows has to know how often they are
    /// written: a freshness bound below the write cadence hides the rows
    /// for the rest of every period.
    pub(crate) scrape_interval: Duration,

    /// The operator's floor on how stale a fleet sample may be. A floor,
    /// not a ceiling — see [`freshness_bound`].
    pub(crate) max_age: Duration,
}

/// How old a fleet sample may be before it is ignored.
///
/// A sample can be a whole publication interval old by construction — it
/// was written at the start of the period and the next one is not due
/// yet — so a bound below that cadence declares every sample stale for
/// most of every period, and the family renders empty. Measured against
/// a running fleet with `interval: 15s` and the default `max_age: 5s`:
/// the `claviron_fpm_*` rows were present for roughly six seconds in
/// every fifteen and absent for the rest, which a Prometheus scrape
/// landing in the gap reads as the series having ended.
///
/// `max_age` therefore sets the floor and the cadence sets the minimum.
/// Twice the cadence rather than exactly it, so a late publication does
/// not put the rows in and out on alternate scrapes.
pub(crate) fn freshness_bound(
    max_age: Duration,
    publication_cadence: Duration,
) -> Duration {
    max_age.max(publication_cadence * 2)
}

impl FcgiMetricsProvider {
    pub fn from_state(state: &AppState) -> Self {
        Self {
            config_path: state.config_dir().join("fpm.yaml"),
            plan: Arc::new(ArcSwapOption::empty()),
        }
    }

    /// Apply the prepared `metrics:` section: the plan the collector runs
    /// by, or none.
    fn apply(
        &self,
        parsed: Option<FpmMetricsConfig>,
    ) {
        let Some(metrics) = parsed.filter(|m| m.enabled) else {
            self.plan.store(None);
            FCGI_METRICS.replace(FcgiMetricsSnapshot::empty());
            debug!(
                "FCGI pool metrics disabled (no enabled `metrics:` block in fpm.yaml)"
            );
            return;
        };

        self.plan.store(Some(Arc::new(FcgiMetricsPlan {
            publish_interval: metrics.publish_interval,
            scrape_interval: metrics.interval,
            max_age: metrics.max_age,
        })));
        info!(
            "📊 FCGI pool metrics enabled (publish_interval={:?}, max_age={:?})",
            metrics.publish_interval, metrics.max_age
        );
    }

    async fn collect_once(
        &self,
        state: &AppState,
    ) {
        if self.plan.load_full().is_none() {
            return;
        }

        let Some(php) = state.registry_ref().resolve::<PhpFcgiService>() else {
            return;
        };

        let pooled = php.pooled_entries().await;
        let (pooled_count, oneshot_count) = php.entry_counts().await;

        let pools: Vec<FcgiPoolMetrics> = pooled
            .into_iter()
            .map(|(name, pool)| {
                let stats = pool.stats();
                let opts = pool.opts();
                let endpoint_kind = match pool.endpoint() {
                    FcgiEndpoint::Unix(_) => "unix",
                    FcgiEndpoint::Tcp(_) => "tcp",
                };
                FcgiPoolMetrics {
                    name,
                    endpoint_kind: endpoint_kind.to_string(),
                    live: stats.live as u64,
                    idle: stats.idle as u64,
                    replenishing: stats.replenishing,
                    min_live: opts.min_live as u64,
                    max_idle: opts.max_idle as u64,
                    max_live: opts.max_live as u64,
                    events: pool.events(),
                }
            })
            .collect();

        let snapshot = FcgiMetricsSnapshot {
            captured_at: now_seconds(),
            pools,
            pooled_count,
            oneshot_count,
        };

        if state.is_worker() {
            self.publish_orbit_snapshot(state, &snapshot);
        }
        FCGI_METRICS.replace(snapshot);
    }

    fn publish_orbit_snapshot(
        &self,
        state: &AppState,
        snapshot: &FcgiMetricsSnapshot,
    ) {
        let Some(fleet) = state.fleet() else { return };
        let node_id = state.fleet_node_id().raw();

        let publisher = OrbitMetricPublisher::<FcgiOrbitMetrics>::new(fleet);
        let row = FcgiOrbitMetrics::from_snapshot(
            node_id,
            &snapshot.scalar_totals(FCGI_REQUEST_METRICS.snapshot()),
        );
        if let Err(err) = publisher.publish(&row) {
            tracing::error!("failed to publish FCGI metrics to Orbit: {err}");
        }
    }

    async fn collect_loop(
        &self,
        state: AppState,
        context: RunContext,
    ) -> claviron_runtime::registry::Result<()> {
        self.collect_once(&state).await;
        let mut last_interval = self.current_interval();
        let mut ticker =
            interval_at(Instant::now() + last_interval, last_interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = context.cancelled() => {
                    debug!("FCGI pool metrics task shutting down");
                    return Ok(());
                }
                _ = ticker.tick() => {
                    let current_interval = self.current_interval();
                    if current_interval != last_interval {
                        last_interval = current_interval;
                        ticker = interval_at(Instant::now() + last_interval, last_interval);
                        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
                    }
                    self.collect_once(&state).await;
                }
            }
        }
    }

    fn current_interval(&self) -> Duration {
        self.plan
            .load_full()
            .map(|plan| plan.publish_interval)
            .unwrap_or_else(|| Duration::from_secs(60))
    }

    pub(crate) fn metrics_plan(&self) -> Option<Arc<FcgiMetricsPlan>> {
        self.plan.load_full()
    }
}

impl Component<AppState> for FcgiMetricsProvider {
    type Deps = ();

    fn name(&self) -> &'static str {
        "FcgiMetricsProvider"
    }

    fn as_configurable(&self) -> Option<&dyn congif::ConfigRegistration> {
        Some(self)
    }

    fn as_scoped(&self) -> Option<&dyn Scoped> {
        Some(self)
    }

    fn as_reloadable(&self) -> Option<&dyn Reloadable<AppState>> {
        Some(self)
    }

    fn as_runnable(
        self: std::sync::Arc<Self>
    ) -> Option<std::sync::Arc<dyn Runnable<AppState, Self::Deps>>> {
        Some(self)
    }
}

/// Pool statistics are per process and the pools live where requests are
/// served: in the workers, or in a standalone process.
impl Scoped for FcgiMetricsProvider {
    fn scope(&self) -> Scope {
        Scope::Worker
    }
}

#[async_trait]
impl Reloadable<AppState> for FcgiMetricsProvider {
    fn priority(&self) -> Option<u8> {
        Some(priority::LATE)
    }

    async fn reload(
        &self,
        state: &AppState,
    ) -> claviron_runtime::registry::Result<()> {
        let section = state.config_reader().snapshot::<Self>()?;
        self.apply(section.metrics.clone());
        Ok(())
    }
}

/// The `metrics:` section of `fpm.yaml`; an absent file turns metrics off.
#[async_trait]
impl Configurable for FcgiMetricsProvider {
    type Config = MetricsSection;

    fn source(&self) -> ConfigFile {
        ConfigFile::yaml(&self.config_path)
    }

    fn absent_source(&self) -> Option<MetricsSection> {
        Some(MetricsSection::default())
    }

    async fn configure(
        &self,
        config: &MetricsSection,
    ) -> congif::Result<()> {
        self.apply(config.metrics.clone());
        Ok(())
    }

    fn as_config_observer(
        &self
    ) -> Option<&dyn ConfigObserver<Config = Self::Config>> {
        Some(self)
    }
}

#[async_trait]
impl ConfigObserver for FcgiMetricsProvider {
    async fn config_changed(
        &self,
        config: &MetricsSection,
    ) -> congif::Result<()> {
        self.apply(config.metrics.clone());
        Ok(())
    }
}

#[async_trait]
impl Runnable<AppState> for FcgiMetricsProvider {
    async fn run(
        self: Arc<Self>,
        _deps: (),
        state: AppState,
        context: RunContext,
    ) -> claviron_runtime::registry::Result<()> {
        // Pool stats are per-worker; the scope keeps this off the master,
        // whose entries PhpFcgiService clears.
        self.collect_loop(state, context).await
    }
}

fn now_seconds() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()
}

#[cfg(test)]
#[path = "../../../tests/metrics/fcgi/provider.rs"]
mod tests;
