use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use claviron_orbit::placement::{Scope, Scoped};
use claviron_orbit::{FleetProvider, OrbitStateExt};
use claviron_runtime::metrics::{MetricsFamily, MetricsProvider};
use claviron_runtime::registry::{
    self, Component, ComponentOrder, Runnable, priority,
};
use claviron_runtime::{AppState, RunContext};
use orbitive::metrics::{
    OrbitMetricCollector, OrbitMetricPublisher, OrbitMetricSnapshot,
    OrbitTyped, RingSpec,
};
use tracing::{debug, error};

use crate::config::SupervisorConfig;
use crate::control::WorkerControl;

const PROCESS_METRICS_RING_KIND: u8 = 223;
const PROCESS_METRICS_WIRE_LEN: usize = 49;
const PROCESS_METRICS_RING_SPEC: RingSpec = RingSpec::per_node(
    orbitive::metrics::METRIC_SNAPSHOT_RING_CAPACITY,
    PROCESS_METRICS_WIRE_LEN,
);

/// One supervisor-owned process snapshot.
///
/// Node zero is the master. Non-zero nodes are workers. Standalone snapshots
/// use the same local shape but are rendered directly and never enter Orbit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ProcessMetricsSnapshot {
    node_id: u16,
    worker_id: u8,
    pid: u32,
    captured_at: u64,
    rss_bytes: u64,
    uptime_seconds: u64,
    worker_restarts_total: u64,
    worker_rotations_total: u64,
    workers_configured: u8,
    workers_alive: u8,
}

impl ProcessMetricsSnapshot {
    pub(crate) fn master(
        started_at: Instant,
        worker_restarts_total: u64,
        worker_rotations_total: u64,
        workers_configured: u8,
        workers_alive: u8,
    ) -> Self {
        Self::capture_process(
            0,
            0,
            started_at,
            worker_restarts_total,
            worker_rotations_total,
        )
        .with_worker_counts(workers_configured, workers_alive)
    }

    fn worker(
        node_id: u16,
        worker_id: u8,
        started_at: Instant,
    ) -> Self {
        Self::capture_process(node_id, worker_id, started_at, 0, 0)
    }

    fn standalone(started_at: Instant) -> Self {
        Self::capture_process(0, 0, started_at, 0, 0)
    }

    fn capture_process(
        node_id: u16,
        worker_id: u8,
        started_at: Instant,
        worker_restarts_total: u64,
        worker_rotations_total: u64,
    ) -> Self {
        Self {
            node_id,
            worker_id,
            pid: std::process::id(),
            captured_at: unix_now(),
            rss_bytes: claviron_sys::rss::read_rss(None).unwrap_or(0),
            uptime_seconds: started_at.elapsed().as_secs(),
            worker_restarts_total,
            worker_rotations_total,
            workers_configured: 0,
            workers_alive: 0,
        }
    }

    fn with_worker_counts(
        mut self,
        configured: u8,
        alive: u8,
    ) -> Self {
        self.workers_configured = configured;
        self.workers_alive = alive;
        self
    }
}

impl OrbitTyped for ProcessMetricsSnapshot {
    const KIND: u8 = PROCESS_METRICS_RING_KIND;
    const RING_SPEC: RingSpec = PROCESS_METRICS_RING_SPEC;
}

impl OrbitMetricSnapshot for ProcessMetricsSnapshot {
    const FAMILY: &'static str = "claviron_supervisor_process";

    fn node_id(&self) -> u16 {
        self.node_id
    }

    fn captured_at_unix_secs(&self) -> u64 {
        self.captured_at
    }

    fn encode(&self) -> Result<Vec<u8>, String> {
        let mut out = Vec::with_capacity(PROCESS_METRICS_WIRE_LEN);
        out.extend_from_slice(&self.node_id.to_le_bytes());
        out.push(self.worker_id);
        out.extend_from_slice(&self.pid.to_le_bytes());
        for value in [
            self.captured_at,
            self.rss_bytes,
            self.uptime_seconds,
            self.worker_restarts_total,
            self.worker_rotations_total,
        ] {
            out.extend_from_slice(&value.to_le_bytes());
        }
        out.push(self.workers_configured);
        out.push(self.workers_alive);
        Ok(out)
    }

    fn decode(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() != PROCESS_METRICS_WIRE_LEN {
            return Err(format!(
                "bad supervisor process metrics payload len {}",
                bytes.len()
            ));
        }

        let node_id =
            u16::from_le_bytes(bytes[0..2].try_into().expect("node id bytes"));
        let worker_id = bytes[2];
        let pid =
            u32::from_le_bytes(bytes[3..7].try_into().expect("pid bytes"));
        let mut offset = 7;
        let mut next_u64 = || {
            let value = u64::from_le_bytes(
                bytes[offset..offset + 8].try_into().expect("u64 bytes"),
            );
            offset += 8;
            value
        };
        let captured_at = next_u64();
        let rss_bytes = next_u64();
        let uptime_seconds = next_u64();
        let worker_restarts_total = next_u64();
        let worker_rotations_total = next_u64();

        Ok(Self {
            node_id,
            worker_id,
            pid,
            captured_at,
            rss_bytes,
            uptime_seconds,
            worker_restarts_total,
            worker_rotations_total,
            workers_configured: bytes[offset],
            workers_alive: bytes[offset + 1],
        })
    }
}

fn unix_now() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()
}

type SharedConfig = crate::config::provider::SharedPolicy;

enum PublisherSource {
    Master(Arc<WorkerControl>),
    Worker { started_at: Instant },
}

/// Optional master/worker Runnable that publishes process RSS, uptime and
/// supervisor-owned counters independently of heartbeat and lifecycle control.
///
/// Exists so monitoring consumers can read telemetry without driving process
/// policy. The master resets its metrics ring during provider boot, not on each
/// Runnable start. Targeted stop pauses samples; restart keeps the process clock
/// and counters. Missing or stale samples never authorize recovery or termination.
struct SupervisorMetricsPublisher {
    source: PublisherSource,
    config: SharedConfig,
}

impl SupervisorMetricsPublisher {
    fn master(
        supervisor: Arc<WorkerControl>,
        config: SharedConfig,
    ) -> Self {
        Self { source: PublisherSource::Master(supervisor), config }
    }

    fn worker(config: SharedConfig) -> Self {
        Self {
            source: PublisherSource::Worker { started_at: Instant::now() },
            config,
        }
    }

    fn config(&self) -> Arc<SupervisorConfig> {
        self.config.load_full().expect(
            "SupervisorMetricsPublisher config accessed before configuration",
        )
    }

    async fn capture(
        &self,
        state: &AppState,
    ) -> ProcessMetricsSnapshot {
        match &self.source {
            PublisherSource::Master(supervisor) => {
                crate::observation::counters::capture_metrics(supervisor, state)
                    .await
            }
            PublisherSource::Worker { started_at } => {
                ProcessMetricsSnapshot::worker(
                    state.fleet_node_id().raw(),
                    state
                        .worker_id()
                        .expect("worker metrics require worker identity"),
                    *started_at,
                )
            }
        }
    }
}

impl Scoped for SupervisorMetricsPublisher {
    fn scope(&self) -> Scope {
        Scope::Any
    }
}

impl Component<AppState> for SupervisorMetricsPublisher {
    type Deps = (Arc<FleetProvider>,);

    fn name(&self) -> &'static str {
        "SupervisorMetricsPublisher"
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
}

#[async_trait]
impl Runnable<AppState, (Arc<FleetProvider>,)> for SupervisorMetricsPublisher {
    async fn run(
        self: Arc<Self>,
        (fleet_provider,): (Arc<FleetProvider>,),
        state: AppState,
        context: RunContext,
    ) -> registry::Result<()> {
        let fleet = fleet_provider.handle().ok_or_else(|| {
            registry::Error::msg(
                "Orbit fleet is unavailable for supervisor metrics",
            )
        })?;
        let publisher =
            OrbitMetricPublisher::<ProcessMetricsSnapshot>::new(fleet);
        self.publish_until_stopped(state, context, publisher).await
    }
}
impl SupervisorMetricsPublisher {
    async fn publish_until_stopped(
        &self,
        state: AppState,
        context: RunContext,
        publisher: OrbitMetricPublisher<ProcessMetricsSnapshot>,
    ) -> registry::Result<()> {
        let mut interval = self.config().metrics.publish_interval;
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                biased;
                _ = context.cancelled() => {
                    debug!("Supervisor metrics publisher shutting down");
                    return Ok(());
                }
                _ = ticker.tick() => {
                    let current_interval = self.config().metrics.publish_interval;
                    if current_interval != interval {
                        interval = current_interval;
                        ticker = tokio::time::interval(interval);
                        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                    }

                    let snapshot = self.capture(&state).await;
                    if let Err(err) = publisher.publish(&snapshot) {
                        error!("failed to publish supervisor metrics to Orbit: {err}");
                    }
                }
            }
        }
    }
}

/// On-demand renderer for supervisor process metrics, separate from publication.
///
/// Collects fresh fleet samples in supervised modes or captures local standalone
/// values. It owns neither a polling task nor worker lifecycle policy.
struct SupervisorMetricsFamily {
    standalone_started_at: Instant,
    config: Option<SharedConfig>,
}

impl SupervisorMetricsFamily {
    fn standalone() -> Self {
        Self { standalone_started_at: Instant::now(), config: None }
    }

    fn fleet(config: SharedConfig) -> Self {
        Self { standalone_started_at: Instant::now(), config: Some(config) }
    }

    fn collect_fleet(
        &self,
        state: &AppState,
    ) -> Result<BTreeMap<u16, ProcessMetricsSnapshot>, String> {
        let fleet = state.fleet().ok_or_else(|| {
            "Orbit fleet is unavailable for supervisor metrics".to_string()
        })?;
        let max_age = self
            .config
            .as_ref()
            .and_then(|config| config.load_full())
            .ok_or_else(|| {
                "Supervisor metrics config is unavailable".to_string()
            })?
            .metrics
            .max_age
            .as_secs()
            .max(1);

        Ok(OrbitMetricCollector::<ProcessMetricsSnapshot>::new(fleet)
            .fresh_by_node(unix_now(), max_age)
            .into_iter()
            .map(|(node_id, sample)| (node_id, sample.snapshot))
            .collect())
    }
}

impl MetricsFamily for SupervisorMetricsFamily {
    fn name(&self) -> &'static str {
        "claviron.supervisor"
    }

    fn write_prometheus(
        &self,
        state: &AppState,
        output: &mut String,
    ) -> Result<(), String> {
        if state.is_standalone() {
            write_process_metrics_header(output);
            write_process_metrics_sample(
                output,
                "standalone",
                &ProcessMetricsSnapshot::standalone(self.standalone_started_at),
            );
            return Ok(());
        }

        let snapshots = self.collect_fleet(state)?;
        write_process_metrics_header(output);
        for snapshot in snapshots.values() {
            let role =
                if snapshot.worker_id == 0 { "master" } else { "worker" };
            write_process_metrics_sample(output, role, snapshot);
        }
        if let Some(master) = snapshots.get(&0) {
            write_master_metrics(output, master);
        }
        Ok(())
    }
}

fn write_process_metrics_header(output: &mut String) {
    let _ = writeln!(
        output,
        "# HELP claviron_supervisor_process_rss_bytes Resident memory used by a Claviron process"
    );
    let _ =
        writeln!(output, "# TYPE claviron_supervisor_process_rss_bytes gauge");
    let _ = writeln!(
        output,
        "# HELP claviron_supervisor_process_uptime_seconds Uptime of a Claviron process"
    );
    let _ = writeln!(
        output,
        "# TYPE claviron_supervisor_process_uptime_seconds gauge"
    );
}

fn write_process_metrics_sample(
    output: &mut String,
    role: &str,
    snapshot: &ProcessMetricsSnapshot,
) {
    let _ = writeln!(
        output,
        "claviron_supervisor_process_rss_bytes{{role=\"{}\",node_id=\"{}\",worker_id=\"{}\",pid=\"{}\"}} {}",
        role,
        snapshot.node_id,
        snapshot.worker_id,
        snapshot.pid,
        snapshot.rss_bytes
    );
    let _ = writeln!(
        output,
        "claviron_supervisor_process_uptime_seconds{{role=\"{}\",node_id=\"{}\",worker_id=\"{}\",pid=\"{}\"}} {}",
        role,
        snapshot.node_id,
        snapshot.worker_id,
        snapshot.pid,
        snapshot.uptime_seconds
    );
}

fn write_master_metrics(
    output: &mut String,
    master: &ProcessMetricsSnapshot,
) {
    let _ = writeln!(
        output,
        "# HELP claviron_supervisor_worker_restarts_total Total successful worker restarts"
    );
    let _ = writeln!(
        output,
        "# TYPE claviron_supervisor_worker_restarts_total counter"
    );
    let _ = writeln!(
        output,
        "claviron_supervisor_worker_restarts_total {}",
        master.worker_restarts_total
    );
    let _ = writeln!(
        output,
        "# HELP claviron_supervisor_worker_rotations_total Total successful worker rotations"
    );
    let _ = writeln!(
        output,
        "# TYPE claviron_supervisor_worker_rotations_total counter"
    );
    let _ = writeln!(
        output,
        "claviron_supervisor_worker_rotations_total {}",
        master.worker_rotations_total
    );
    let _ = writeln!(
        output,
        "# HELP claviron_supervisor_workers_configured Configured worker process count"
    );
    let _ =
        writeln!(output, "# TYPE claviron_supervisor_workers_configured gauge");
    let _ = writeln!(
        output,
        "claviron_supervisor_workers_configured {}",
        master.workers_configured
    );
    let _ = writeln!(
        output,
        "# HELP claviron_supervisor_workers_alive Worker processes currently providing capacity"
    );
    let _ = writeln!(output, "# TYPE claviron_supervisor_workers_alive gauge");
    let _ = writeln!(
        output,
        "claviron_supervisor_workers_alive {}",
        master.workers_alive
    );
}

fn ensure_metrics_provider(
    state: &AppState
) -> registry::Result<Arc<MetricsProvider>> {
    if state.registry_ref().resolve::<MetricsProvider>().is_none() {
        claviron_runtime::metrics::register(state)?;
    }
    state.registry_ref().resolve::<MetricsProvider>().ok_or_else(|| {
        registry::Error::msg("MetricsProvider registration failed")
    })
}

pub(crate) fn register_standalone(state: &AppState) -> registry::Result<()> {
    ensure_metrics_provider(state)?
        .register(Arc::new(SupervisorMetricsFamily::standalone()));
    Ok(())
}

pub(crate) fn register_supervised(
    state: &AppState,
    supervisor: Arc<WorkerControl>,
) -> registry::Result<()> {
    claviron_orbit::shm::register_reusable_ring::<ProcessMetricsSnapshot>(
        state,
        "SupervisorMetricsShm",
        "supervisor-process-metrics-ring",
    )?;
    let config = supervisor.config.clone();
    let publisher = Arc::new(if state.is_master() {
        SupervisorMetricsPublisher::master(supervisor, config.clone())
    } else {
        SupervisorMetricsPublisher::worker(config.clone())
    });
    state.registry_ref().register(publisher)?;
    ensure_metrics_provider(state)?
        .register(Arc::new(SupervisorMetricsFamily::fleet(config)));
    Ok(())
}

#[cfg(test)]
#[path = "../../tests/observation/metrics.rs"]
mod tests;
