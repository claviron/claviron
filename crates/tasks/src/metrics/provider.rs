use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use claviron_metrics::{MetricsFamily, MetricsProvider};
use claviron_orbit::{FleetProvider, OrbitStateExt};
use claviron_runtime::registry::{
    Component, ComponentOrder, Runnable, priority,
};
use claviron_runtime::{AppState, RunContext, registry};
use orbitive::metrics::{OrbitMetricCollector, OrbitMetricPublisher};
use tracing::{debug, error};

use super::{
    ExternalProcessSampler, TASK_METRICS, TaskMetricsSnapshot,
    TaskOrbitMetrics, TaskPairOrbitMetrics, TaskServiceOrbitMetrics,
};
use crate::service::TaskService;

struct TaskMetricsFamily {
    service: Arc<TaskService>,
}

impl MetricsFamily for TaskMetricsFamily {
    fn name(&self) -> &'static str {
        "claviron.tasks"
    }

    fn write_prometheus(
        &self,
        state: &AppState,
        output: &mut String,
    ) -> Result<(), String> {
        let config = self.service.metrics_config();
        if !config.enabled {
            return Ok(());
        }
        if state.is_standalone() {
            TASK_METRICS.write_prometheus(output);
            return Ok(());
        }

        if state.is_worker() {
            let fleet = state.fleet().ok_or_else(|| {
                "Orbit fleet is unavailable for task metrics".to_string()
            })?;
            let now = now_seconds()?;
            let max_age = config.max_age.as_secs();
            let mut aggregate = TaskMetricsSnapshot::default();

            let service = OrbitMetricCollector::<TaskServiceOrbitMetrics>::new(
                fleet.clone(),
            );
            for sample in service.fresh_by_node(now, max_age).into_values() {
                sample.snapshot.merge_into(&mut aggregate);
            }
            let tasks =
                OrbitMetricCollector::<TaskOrbitMetrics>::new(fleet.clone());
            for sample in tasks.fresh_by_key(now, max_age).into_values() {
                sample.snapshot.merge_into(&mut aggregate);
            }
            let pairs =
                OrbitMetricCollector::<TaskPairOrbitMetrics>::new(fleet);
            for sample in pairs.fresh_by_key(now, max_age).into_values() {
                sample.snapshot.merge_into(&mut aggregate);
            }
            aggregate.write_prometheus(output);
        }
        Ok(())
    }
}

struct TaskMetricsPublisher {
    service: Arc<TaskService>,
}

impl Component<AppState> for TaskMetricsPublisher {
    type Deps = (Arc<FleetProvider>,);

    fn name(&self) -> &'static str {
        "TaskMetricsPublisher"
    }

    fn run_priority(&self) -> Option<u8> {
        Some(priority::LATE)
    }

    fn order(&self) -> ComponentOrder {
        ComponentOrder::new().after::<TaskService>()
    }

    fn as_runnable(
        self: Arc<Self>
    ) -> Option<Arc<dyn Runnable<AppState, Self::Deps>>> {
        Some(self)
    }
}

#[async_trait]
impl Runnable<AppState, (Arc<FleetProvider>,)> for TaskMetricsPublisher {
    async fn run(
        self: Arc<Self>,
        (fleet_provider,): (Arc<FleetProvider>,),
        _state: AppState,
        context: RunContext,
    ) -> registry::Result<()> {
        let fleet = fleet_provider.handle().ok_or_else(|| {
            registry::Error::msg("Orbit fleet is unavailable for task metrics")
        })?;
        let service_publisher =
            OrbitMetricPublisher::<TaskServiceOrbitMetrics>::new(fleet.clone());
        let task_publisher =
            OrbitMetricPublisher::<TaskOrbitMetrics>::new(fleet.clone());
        let pair_publisher =
            OrbitMetricPublisher::<TaskPairOrbitMetrics>::new(fleet.clone());
        let node_id = fleet.node_id().get();
        let mut interval = publish_interval(&self.service);
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = context.cancelled() => {
                    debug!("task metrics publisher shutting down");
                    return Ok(());
                }
                _ = ticker.tick() => {
                    let config = self.service.metrics_config();
                    let current_interval = nonzero_interval(config.publish_interval);
                    if current_interval != interval {
                        interval = current_interval;
                        ticker = tokio::time::interval(interval);
                        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                    }
                    if !config.enabled {
                        continue;
                    }
                    let captured_at = match now_seconds() {
                        Ok(now) => now,
                        Err(err) => {
                            error!("failed to capture task metrics time: {err}");
                            continue;
                        }
                    };
                    let snapshot = TASK_METRICS.snapshot();
                    publish_snapshot(
                        node_id,
                        captured_at,
                        &snapshot,
                        &service_publisher,
                        &task_publisher,
                        &pair_publisher
                    );
                }
            }
        }
    }
}

fn publish_snapshot(
    node_id: u16,
    captured_at: u64,
    snapshot: &TaskMetricsSnapshot,
    service_publisher: &OrbitMetricPublisher<TaskServiceOrbitMetrics>,
    task_publisher: &OrbitMetricPublisher<TaskOrbitMetrics>,
    pair_publisher: &OrbitMetricPublisher<TaskPairOrbitMetrics>,
) {
    let service =
        TaskServiceOrbitMetrics::from_snapshot(node_id, captured_at, snapshot);
    if let Err(err) = service_publisher.publish(&service) {
        error!("failed to publish task service metrics to Orbit: {err}");
    }
    for row in
        TaskOrbitMetrics::rows_from_snapshot(node_id, captured_at, snapshot)
    {
        if let Err(err) = task_publisher.publish(&row) {
            error!("failed to publish task metrics to Orbit: {err}");
        }
    }
    for row in
        TaskPairOrbitMetrics::rows_from_snapshot(node_id, captured_at, snapshot)
    {
        if let Err(err) = pair_publisher.publish(&row) {
            error!("failed to publish task pair metrics to Orbit: {err}");
        }
    }
}

pub fn register(
    state: &AppState,
    service: Arc<TaskService>,
) -> registry::Result<()> {
    if state.registry_ref().resolve::<MetricsProvider>().is_none() {
        claviron_metrics::register(state)?;
    }
    state
        .registry_ref()
        .resolve::<MetricsProvider>()
        .ok_or_else(|| registry::Error::msg("MetricsProvider is unavailable"))?
        .register(Arc::new(TaskMetricsFamily { service: service.clone() }));
    state.registry_ref().register(Arc::new(ExternalProcessSampler))?;

    if !state.is_standalone() {
        claviron_orbit::shm::register_reusable_ring::<TaskServiceOrbitMetrics>(
            state,
            "TaskServiceMetricsShm",
            "task-service-metrics-ring",
        )?;
        claviron_orbit::shm::register_reusable_ring::<TaskOrbitMetrics>(
            state,
            "TaskMetricsShm",
            "task-metrics-ring",
        )?;
        claviron_orbit::shm::register_reusable_ring::<TaskPairOrbitMetrics>(
            state,
            "TaskPairMetricsShm",
            "task-pair-metrics-ring",
        )?;
    }

    if !state.is_standalone() {
        state
            .registry_ref()
            .register(Arc::new(TaskMetricsPublisher { service }))?;
    }
    Ok(())
}

fn publish_interval(service: &TaskService) -> Duration {
    nonzero_interval(service.metrics_config().publish_interval)
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
