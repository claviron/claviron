use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arc_swap::ArcSwapOption;
use async_trait::async_trait;
use claviron_orbit::OrbitStateExt;
use claviron_orbit::placement::{
    Dispatch, DispatchPolicy, Dispatched, Scope, Scoped,
};
use claviron_runtime::congif::{
    self, ConfigFile, ConfigObserver, Configurable,
};
use claviron_runtime::registry::{Component, Reloadable, Runnable, priority};
use claviron_runtime::{AppState, RunContext, ServiceStartPolicy};
use fastcgi_client::Client;
use futures_util::future::join_all;
use orbitive::metrics::OrbitMetricPublisher;
use serde_json::Value;
use tokio::time::{Instant, MissedTickBehavior, interval_at, timeout};
use tracing::{debug, info, warn};

use crate::metrics::config::{FpmMetricsConfig, MetricsSection};
use crate::metrics::fpm::status::status_request;
use crate::metrics::fpm::{
    FPM_METRICS, FpmMetricsSnapshot, FpmPoolMetrics, FpmPoolOrbitMetrics,
};
use crate::service::{FpmStatusTarget, PhpFcgiService};

#[derive(Clone)]
pub struct FpmMetricsProvider {
    config_path: PathBuf,
    plan: Arc<ArcSwapOption<FpmMetricsPlan>>,
}

#[derive(Clone)]
struct FpmMetricsPlan {
    interval: Duration,
    dispatch_policy: Option<DispatchPolicy>,
}

impl FpmMetricsProvider {
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
            FPM_METRICS.replace(FpmMetricsSnapshot::empty());
            debug!(
                "FPM metrics disabled (no enabled `metrics:` block in fpm.yaml)"
            );
            return;
        };

        self.plan.store(Some(Arc::new(FpmMetricsPlan {
            interval: metrics.interval,
            dispatch_policy: metrics.dispatch_policy,
        })));
        info!("📊 FPM metrics enabled (interval={:?})", metrics.interval);
    }

    async fn collect_once(
        &self,
        state: &AppState,
    ) {
        if self.plan.load_full().is_none() {
            return;
        }

        let Some(php) = state.registry_ref().resolve::<PhpFcgiService>() else {
            warn!("FPM metrics skipped: PhpService not registered");
            return;
        };

        let targets = php.status_targets().await;
        if targets.is_empty() {
            // The one way this loop does nothing and says nothing: no
            // entry carries a `status_path`, or the catalog is not
            // loaded. Both leave the whole `claviron_fpm_*` family absent
            // from every worker, which reads as a broken scrape rather
            // than as an absent one.
            debug!(
                "FPM metrics: no entry carries a `status_path`; nothing to scrape"
            );
            return;
        }
        // A scrape that has not answered within one interval is already
        // superseded by the next one, so the cadence is its own budget and
        // no operator setting is needed for it.
        let budget = self.current_interval();
        let pools = join_all(
            targets.into_iter().map(|target| scrape_target(target, budget)),
        )
        .await;

        let scraped_at = now_seconds();
        let snapshot = FpmMetricsSnapshot::new(scraped_at, pools);

        if state.is_master() {
            self.publish_orbit_snapshot(state, &snapshot);
        }
        FPM_METRICS.replace(snapshot);
    }

    fn publish_orbit_snapshot(
        &self,
        state: &AppState,
        snapshot: &FpmMetricsSnapshot,
    ) {
        let Some(fleet) = state.fleet() else {
            return;
        };
        let publisher = OrbitMetricPublisher::<FpmPoolOrbitMetrics>::new(fleet);
        for pool in &snapshot.pools {
            let row = FpmPoolOrbitMetrics::from_pool(snapshot.scraped_at, pool);
            if let Err(err) = publisher.publish(&row) {
                warn!("failed to publish FPM pool metrics to Orbit: {}", err);
            }
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
                    debug!("FPM metrics task shutting down");
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
            .map(|plan| plan.interval)
            .unwrap_or_else(|| Duration::from_secs(60))
    }
}

impl Component<AppState> for FpmMetricsProvider {
    type Deps = ();

    fn name(&self) -> &'static str {
        "FpmMetricsProvider"
    }

    fn as_configurable(&self) -> Option<&dyn congif::ConfigRegistration> {
        Some(self)
    }

    fn as_scoped(&self) -> Option<&dyn Scoped> {
        Some(self)
    }

    fn as_dispatched(&self) -> Option<&dyn Dispatched> {
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

/// FPM `/status` has one source per fleet: the master scrapes it, or a
/// standalone process does.
impl Scoped for FpmMetricsProvider {
    fn scope(&self) -> Scope {
        Scope::Master
    }
}

/// A shared FPM status endpoint must have exactly one collector owner,
/// never one collector per eligible process or a pool of collectors.
impl Dispatched for FpmMetricsProvider {
    fn dispatch(&self) -> Dispatch {
        Dispatch::One
    }

    fn dispatch_policy(&self) -> Option<DispatchPolicy> {
        self.plan.load_full().and_then(|plan| plan.dispatch_policy)
    }
}

#[async_trait]
impl Reloadable<AppState> for FpmMetricsProvider {
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
impl Configurable for FpmMetricsProvider {
    type Config = MetricsSection;

    fn source(&self) -> ConfigFile {
        ConfigFile::yaml(&self.config_path)
    }

    fn absent_source(&self) -> Option<MetricsSection> {
        Some(MetricsSection::default())
    }

    fn validate_config(
        &self,
        config: &MetricsSection,
    ) -> congif::Result<()> {
        if let Some(metrics) =
            config.metrics.as_ref().filter(|metrics| metrics.enabled)
        {
            Dispatch::One
                .validate_policy(metrics.dispatch_policy)
                .map_err(congif::Error::invalid)?;
        }
        Ok(())
    }

    async fn configure(
        &self,
        config: &MetricsSection,
    ) -> congif::Result<()> {
        self.validate_config(config)?;
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
impl ConfigObserver for FpmMetricsProvider {
    async fn config_changed(
        &self,
        config: &MetricsSection,
    ) -> congif::Result<()> {
        self.validate_config(config)?;
        self.apply(config.metrics.clone());
        Ok(())
    }
}

#[async_trait]
impl Runnable<AppState> for FpmMetricsProvider {
    fn start_policy(
        &self,
        _state: &AppState,
    ) -> ServiceStartPolicy {
        if self.plan.load().is_some() {
            ServiceStartPolicy::Automatic
        } else {
            ServiceStartPolicy::Unavailable
        }
    }

    async fn run(
        self: std::sync::Arc<Self>,
        _deps: (),
        state: AppState,
        context: RunContext,
    ) -> claviron_runtime::registry::Result<()> {
        self.collect_loop(state, context).await
    }
}

/// Scrape one FPM's `/status`, or report it down.
///
/// Bounded end to end, and that bound is the point. `connect_timeout`
/// covers the dial, and the dial is the half that cannot hang: the kernel
/// accepts into the listen backlog whether or not a child is free to take
/// it. What can hang is everything after — FPM answers `/status` from a
/// child, so a pool whose children are all busy leaves the request
/// waiting with the socket wide open.
///
/// Unbounded, that stalls the caller, and the caller is a loop that
/// awaits this inline: one unanswered scrape and the whole family stops
/// being published for the life of the process, with nothing logged
/// because the failure path never runs. Observed on a deployment whose
/// FPM was `pm = dynamic` with four spare children and a keep-alive pool
/// holding them.
async fn scrape_target(
    target: FpmStatusTarget,
    budget: Duration,
) -> FpmPoolMetrics {
    let name = target.name.clone();
    match timeout(budget, scrape_once(target)).await {
        Ok(Ok(pool)) => pool,
        Ok(Err(e)) => {
            warn!("FPM metrics scrape failed for `{name}`: {e}");
            FpmPoolMetrics::down(name)
        }
        Err(_) => {
            // Down rather than absent: a pool that accepted the connection
            // and did not answer within a whole scrape interval is a fact
            // worth publishing, and `claviron_fpm_up 0` says it where
            // silence says nothing.
            warn!(
                "FPM metrics scrape for `{name}` exceeded {budget:?}; reporting it down"
            );
            FpmPoolMetrics::down(name)
        }
    }
}

async fn scrape_once(
    target: FpmStatusTarget
) -> Result<FpmPoolMetrics, std::io::Error> {
    let stream = target
        .endpoint
        .open(target.connect_timeout)
        .await
        .map_err(std::io::Error::other)?;
    let mut client = Client::new_keep_alive_tokio(stream);
    let json = status_request(&mut client, &target.status_path).await?;
    Ok(metrics_from_json(&target, &json))
}

fn metrics_from_json(
    target: &FpmStatusTarget,
    json: &Value,
) -> FpmPoolMetrics {
    FpmPoolMetrics {
        pool: target.name.clone(),
        fpm_pool: string_field(json, "pool")
            .unwrap_or_else(|| target.name.clone()),
        process_manager: string_field(json, "process manager")
            .unwrap_or_default(),
        up: true,
        start_time: u64_field(json, "start time"),
        start_since: u64_field(json, "start since"),
        accepted_conn: u64_field(json, "accepted conn"),
        listen_queue: u64_field(json, "listen queue"),
        max_listen_queue: u64_field(json, "max listen queue"),
        listen_queue_len: u64_field(json, "listen queue len"),
        idle_processes: u64_field(json, "idle processes"),
        active_processes: u64_field(json, "active processes"),
        total_processes: u64_field(json, "total processes"),
        max_active_processes: u64_field(json, "max active processes"),
        max_children_reached: u64_field(json, "max children reached"),
        slow_requests: u64_field(json, "slow requests"),
        memory_peak: u64_field(json, "memory peak"),
    }
}

fn string_field(
    json: &Value,
    key: &str,
) -> Option<String> {
    json.get(key)?.as_str().map(ToOwned::to_owned)
}

fn u64_field(
    json: &Value,
    key: &str,
) -> u64 {
    json.get(key).and_then(Value::as_u64).unwrap_or(0)
}

fn now_seconds() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()
}

#[cfg(test)]
#[path = "../../../tests/metrics/fpm/provider.rs"]
mod tests;
