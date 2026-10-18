//! Head-of-line blocking watchdog: state, policy, and diagnostics.
//!
//! One of Claviron's oldest pieces, and deliberately not a metric. It reads
//! `claviron_runtime::activity` — what this process has in flight and how much
//! it has finished — and says something when the first stops moving while the
//! second does not. A build with no metrics still has it, because a process that
//! cannot notice its own wedge is worse than one that cannot be graphed.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use claviron_helpers::de::deserialize_duration;
use claviron_runtime::activity::ACTIVITY;
use claviron_runtime::registry::priority;
use claviron_runtime::{
    AppState, Component, RunContext, Runnable, ServiceStartPolicy,
};
use serde::Deserialize;
use tokio::runtime::Handle;
use tokio::time::{self, MissedTickBehavior};
use tracing::{debug, error, warn};

/// The current watchdog configuration: `None` while the yaml has no
/// `watchdog:` block. Asked at every check, so a reload is taken up at the
/// next one.
type ConfigReader = Box<dyn Fn() -> Option<Arc<WatchdogConfig>> + Send + Sync>;

pub fn register(
    state: &AppState,
    config: impl Fn() -> Option<Arc<WatchdogConfig>> + Send + Sync + 'static,
) -> claviron_runtime::registry::Result<()> {
    // Registration defines fleet membership; the master never serves HTTP.
    if state.is_master() {
        return Ok(());
    }
    state
        .registry_ref()
        .register(Arc::new(HttpWatchdogProvider::new(Box::new(config))))?;
    Ok(())
}

/// Watchdog configuration for HOL blocking detection
#[derive(Clone, Deserialize, PartialEq, Eq)]
pub struct WatchdogConfig {
    /// Check interval (default: 5s)
    #[serde(deserialize_with = "deserialize_duration")]
    pub check_interval: Duration,
    /// Max inflight requests before warning (default: 1000)
    pub warn_threshold: u64,
    /// Max inflight requests before panic (default: 5000, 0 = disabled)
    pub panic_threshold: u64,
    /// Max time without progress before warning (default: 90s)
    #[serde(deserialize_with = "deserialize_duration")]
    pub stall_timeout: Duration,
    /// Enable panic on severe HOL (default: false)
    pub panic_on_hol: bool,
}

impl Default for WatchdogConfig {
    fn default() -> Self {
        Self {
            check_interval: Duration::from_secs(5),
            warn_threshold: 1000,
            panic_threshold: 5000,
            stall_timeout: Duration::from_secs(90),
            panic_on_hol: false,
        }
    }
}

/// Watchdog state for tracking health across consecutive ticks.
struct WatchdogState {
    last_total_requests: AtomicU64,
    last_progress_time: std::sync::Mutex<Instant>,
    warnings_sent: AtomicU64,
    last_inflight: AtomicU64,
}

impl WatchdogState {
    fn new() -> Self {
        Self {
            last_total_requests: AtomicU64::new(0),
            last_inflight: AtomicU64::new(0),
            last_progress_time: std::sync::Mutex::new(Instant::now()),
            warnings_sent: AtomicU64::new(0),
        }
    }
}

fn check_hol_blocking(
    cfg: &WatchdogConfig,
    state: &WatchdogState,
    pid: u32,
    worker_id: Option<u8>,
) {
    let inflight = ACTIVITY.in_flight();
    let total = ACTIVITY.total();
    let last_total = state.last_total_requests.load(Ordering::Relaxed);
    let last_inflight = state.last_inflight.load(Ordering::Relaxed);

    // Progress if total increased (requests finished) OR in-flight decreased (backpressure releasing)
    let has_progress = total > last_total || inflight < last_inflight;

    // If no requests are in-flight, treat as healthy and reset timers/counters
    if inflight == 0 || has_progress {
        state.last_total_requests.store(total, Ordering::Relaxed);
        state.last_inflight.store(0, Ordering::Relaxed);
        *state.last_progress_time.lock().unwrap() = Instant::now();
        state.warnings_sent.store(0, Ordering::Relaxed);
        return;
    }

    // No progress - check if we're stalled
    let stall_duration = state.last_progress_time.lock().unwrap().elapsed();

    let role = if worker_id.is_some() { "worker" } else { "standalone" };

    // Warn threshold check
    if inflight > cfg.warn_threshold {
        let warnings = state.warnings_sent.fetch_add(1, Ordering::Relaxed);

        warn!(
            role,
            worker_id = ?worker_id,
            pid,
            warning = warnings + 1,
            inflight,
            threshold = cfg.warn_threshold,
            ?stall_duration,
            "HTTP HOL threshold exceeded"
        );

        // Log tokio runtime metrics if available
        if let Ok(handle) = Handle::try_current() {
            let metrics = handle.metrics();
            warn!(
                role,
                worker_id = ?worker_id,
                pid,
                runtime_workers = metrics.num_workers(),
                "Tokio runtime snapshot"
            );
        }
    }

    if stall_duration > cfg.stall_timeout {
        error!(
            role,
            worker_id = ?worker_id,
            pid,
            inflight,
            ?stall_duration,
            stall_timeout = ?cfg.stall_timeout,
            "HTTP requests are making no progress"
        );
    }

    // Panic threshold check (if enabled)
    if cfg.panic_on_hol
        && cfg.panic_threshold > 0
        && inflight > cfg.panic_threshold
    {
        error!(
            role,
            worker_id = ?worker_id,
            pid,
            inflight,
            threshold = cfg.panic_threshold,
            "HTTP HOL panic threshold exceeded"
        );
        dump_hol_diagnostics(inflight, total, stall_duration, pid, worker_id);
        panic!(
            "HOL blocking detected: {} requests in-flight, stalled for {:?}. \
             This panic prevents complete freeze. Check logs for diagnostics.",
            inflight, stall_duration
        );
    }

    // Update the last_inflight snapshot for the next tick
    state.last_inflight.store(inflight, Ordering::Relaxed);
}

fn dump_hol_diagnostics(
    inflight: u64,
    total: u64,
    stall_duration: Duration,
    pid: u32,
    worker_id: Option<u8>,
) {
    let role = if worker_id.is_some() { "worker" } else { "standalone" };
    error!(
        role,
        worker_id = ?worker_id,
        pid,
        inflight,
        total,
        ?stall_duration,
        captured_at = ?std::time::SystemTime::now(),
        "HTTP HOL diagnostics"
    );

    // Tokio runtime metrics
    if let Ok(handle) = Handle::try_current() {
        let metrics = handle.metrics();
        let runtime_workers = metrics.num_workers();
        error!(
            role,
            worker_id = ?worker_id,
            pid,
            runtime_workers,
            "Tokio runtime diagnostics"
        );
        for runtime_worker in 0..runtime_workers {
            error!(
                role,
                worker_id = ?worker_id,
                pid,
                runtime_worker,
                park_count = metrics.worker_park_count(runtime_worker),
                "Tokio worker diagnostics"
            );
        }
    }
}

/// Observes HTTP progress in a worker or standalone process, never the master.
/// Config controls start eligibility; stopping this runnable only stops its
/// diagnostic ticker, without affecting listeners or process lifecycle.
struct HttpWatchdogProvider {
    state: WatchdogState,
    config: ConfigReader,
}

impl HttpWatchdogProvider {
    fn new(config: ConfigReader) -> Self {
        Self { state: WatchdogState::new(), config }
    }
}

impl Component<AppState> for HttpWatchdogProvider {
    type Deps = ();

    fn name(&self) -> &'static str {
        "HttpWatchdogProvider"
    }

    fn run_priority(&self) -> Option<u8> {
        Some(priority::LATE)
    }

    fn as_runnable(
        self: std::sync::Arc<Self>
    ) -> Option<std::sync::Arc<dyn Runnable<AppState, Self::Deps>>> {
        Some(self)
    }
}

#[async_trait]
impl Runnable<AppState> for HttpWatchdogProvider {
    fn start_policy(
        &self,
        _state: &AppState,
    ) -> ServiceStartPolicy {
        if (self.config)().is_some() {
            ServiceStartPolicy::Automatic
        } else {
            ServiceStartPolicy::Unavailable
        }
    }

    async fn run(
        self: Arc<Self>,
        _deps: (),
        state: AppState,
        context: RunContext,
    ) -> claviron_runtime::registry::Result<()> {
        let Some(config) = (self.config)() else {
            // Config may change after the runtime checked start eligibility.
            return Ok(());
        };

        let pid = std::process::id();
        let worker_id = state.worker_id();

        debug!(
            role = if worker_id.is_some() { "worker" } else { "standalone" },
            worker_id = ?worker_id,
            pid,
            "HTTP HOL watchdog started"
        );

        let mut last_interval = config.check_interval;
        let mut ticker = time::interval(last_interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = context.cancelled() => {
                    debug!("HTTP HOL watchdog shutting down");
                    return Ok(());
                }
                _ = ticker.tick() => {
                    let Some(cfg) = (self.config)() else {
                        // Preserve this generation, but do no checks while disabled.
                        continue;
                    };
                    if cfg.check_interval != last_interval {
                        last_interval = cfg.check_interval;
                        ticker = time::interval(last_interval);
                        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
                        continue;
                    }
                    check_hol_blocking(&cfg, &self.state, pid, worker_id);
                }
            }
        }
    }
}
