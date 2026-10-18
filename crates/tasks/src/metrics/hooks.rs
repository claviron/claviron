//! Runtime hooks into the task-owned [`super::TASK_METRICS`].
//!
//! The type and Prometheus serialiser live beside these hooks. The increment /
//! decrement / observation calls live here, so all operational
//! mutations to task metrics flow through this module — single
//! place to grep when wiring a new hook point.
//!
//! Module gated entirely under `feature = "metrics"`. Callers in
//! `runner.rs` / `process/` / `service.rs` wrap their hook calls
//! in `#[cfg(feature = "metrics")]` blocks so a non-metrics build
//! is genuinely zero-cost (no atomic loads, no hashmap lookups).

// #![allow(dead_code)]
use std::collections::HashMap;
use std::sync::{LazyLock, RwLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use claviron_helpers::sync::Unpoisoned;
use claviron_runtime::registry::{Component, Runnable, priority};
use claviron_runtime::{AppState, RunContext};

pub use super::AttemptOutcome;
use super::TASK_METRICS;

// ---------------------------------------------------------------------------
// Service-level setters (TaskService::boot / reload / cancel_all)
// ---------------------------------------------------------------------------

#[inline]
pub fn set_registered(n: u64) {
    TASK_METRICS.set_registered(n);
}

#[inline]
pub fn inc_reload() {
    TASK_METRICS.inc_reload();
}

// ---------------------------------------------------------------------------
// Per-task counters (runner.rs hooks)
// ---------------------------------------------------------------------------

/// Top of `run_with_retries` — one increment per scheduled execution
/// regardless of how many retry attempts happen inside.
#[inline]
pub fn record_run(task: &str) {
    TASK_METRICS.record_run(task);
}

/// Per-attempt outcome — called once per attempt with its terminal
/// classification.
#[inline]
pub fn record_attempt(
    task: &str,
    outcome: AttemptOutcome,
) {
    TASK_METRICS.record_attempt(task, outcome);
}

/// One task occurrence was not executed because its schedule was missed,
/// a previous attempt overlapped it, or arbitration failed closed.
#[inline]
pub fn record_skipped(task: &str) {
    TASK_METRICS.record_skipped(task);
}

// ---------------------------------------------------------------------------
// Per-task in-flight gauge (`AttemptGuard`)
// ---------------------------------------------------------------------------

/// RAII guard for the `claviron_task_in_flight` gauge **and** the
/// `claviron_task_duration_seconds` histogram. Increment + start
/// timer on construct, decrement + observe duration + record outcome
/// on drop.
///
/// The outcome is set by the caller via [`AttemptGuard::finish`]
/// before the guard drops; if the guard drops without `finish`
/// (panic path), the outcome defaults to `Error` so panics still get
/// classified.
pub struct AttemptGuard {
    task: String,
    start: Instant,
    outcome: AttemptOutcome,
    finished: bool,
}

impl AttemptGuard {
    pub fn start(task: &str) -> Self {
        TASK_METRICS.inc_in_flight(task);
        Self {
            task: task.to_string(),
            start: Instant::now(),
            outcome: AttemptOutcome::Error,
            finished: false,
        }
    }

    /// Record the terminal outcome for this attempt. Idempotent —
    /// only the first call wins so an early `finish` followed by a
    /// panic keeps the operator-meaningful outcome instead of being
    /// overwritten to `Error`.
    pub fn finish(
        &mut self,
        outcome: AttemptOutcome,
    ) {
        if !self.finished {
            self.outcome = outcome;
            self.finished = true;
        }
    }
}

impl Drop for AttemptGuard {
    fn drop(&mut self) {
        TASK_METRICS.dec_in_flight(&self.task);
        let elapsed = self.start.elapsed();
        TASK_METRICS.record_duration(&self.task, elapsed.as_secs_f64());
        TASK_METRICS.record_attempt(&self.task, self.outcome);
    }
}

// ---------------------------------------------------------------------------
// Per-(task, role) setters
// ---------------------------------------------------------------------------

/// Compute the `role` label for the current process. The label
/// values are documented in this crate's task metric store and snapshot types:
/// `master` / `worker_<id>` / `standalone`.
pub fn role_label_from_run_mode(mode: claviron_runtime::RunMode) -> String {
    match mode {
        claviron_runtime::RunMode::Master => "master".to_string(),
        claviron_runtime::RunMode::Worker(id) => format!("worker_{}", id.raw()),
        claviron_runtime::RunMode::Standalone => "standalone".to_string(),
    }
}

#[inline]
pub fn set_workers_healthy(
    task: &str,
    role: &str,
    n: u64,
) {
    TASK_METRICS.set_workers_healthy(task, role, n);
}

#[inline]
pub fn set_workers_total(
    task: &str,
    role: &str,
    n: u64,
) {
    TASK_METRICS.set_workers_total(task, role, n);
}

// ---------------------------------------------------------------------------
// Self-info gauge (set once at registration / config load)
// ---------------------------------------------------------------------------

#[inline]
pub fn set_info(
    task: &str,
    kind: &str,
    schedule: &str,
) {
    TASK_METRICS.set_info(task, kind, schedule);
}

// ---------------------------------------------------------------------------
// External-process RSS sampler
// ---------------------------------------------------------------------------
//
// Lives here, not in `runner.rs` or `process/`, because it's its
// own small Provider+Runnable service — the user-facing decision in
// `ARCHITECTURE.md §7.2` (Provider vs Task): a stateful sampler with
// its own lifecycle, registry presence, and reload semantics is a
// Provider, not a Task.
//
// Platform support lives entirely in `claviron-sys`: Linux uses `/proc`,
// FreeBSD uses `sysctl`, macOS uses libproc, and unsupported platforms
// return `None` without leaking platform branches into this service.

/// Per-(task, slot) child PID registry, written by `process::run_command`
/// when it spawns / reaps an external process and read by the sampler
/// loop. Keys are (task name, concurrency slot 1..M); values are PIDs.
///
/// Slot detail is dropped at output time (sampler aggregates by task
/// name, sums RSS across slots before publishing) — kept here only so
/// we can sample the right process per slot.
pub static COMMAND_PIDS: LazyLock<RwLock<HashMap<(String, u8), u32>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

/// Called by `process::run_command` after `cmd.spawn()` succeeds.
pub fn register_command_pid(
    task: &str,
    slot: u8,
    pid: u32,
) {
    COMMAND_PIDS.write_unpoisoned().insert((task.to_string(), slot), pid);
}

/// Called by `process::run_command` once the child reaps.
pub fn unregister_command_pid(
    task: &str,
    slot: u8,
) {
    COMMAND_PIDS.write_unpoisoned().remove(&(task.to_string(), slot));
}

/// Provider+Runnable service that periodically reads RSS for every alive
/// `command:` task child through `claviron-sys` and publishes their summed RSS
/// to `claviron_task_process_rss_bytes{task,role}`.
///
/// Always registered when the `metrics` feature is on; the loop body
/// no-ops when [`COMMAND_PIDS`] is empty (no external tasks running).
/// Sampling cadence comes from
/// `tasks.yaml::metrics.rss_sample_interval`, resolved each tick via
/// `TaskService::metrics_interval()` — a SIGHUP that updates the
/// YAML cadence is picked up on the next loop iteration without a
/// restart.
#[derive(Default)]
pub struct ExternalProcessSampler;

impl Component<AppState> for ExternalProcessSampler {
    type Deps = ();

    fn name(&self) -> &'static str {
        "ExternalProcessSampler"
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
impl Runnable<AppState> for ExternalProcessSampler {
    async fn run(
        self: std::sync::Arc<Self>,
        _deps: (),
        state: AppState,
        context: RunContext,
    ) -> claviron_runtime::registry::Result<()> {
        run_sampler_loop(state, context).await
    }
}

/// Default cadence used when `TaskService` isn't yet registered (boot
/// race) or its config doesn't override it. Matches
/// `service::DEFAULT_METRICS_INTERVAL` — kept locally so the sampler
/// has a sane fallback even with TaskService missing entirely.
const SAMPLER_DEFAULT_INTERVAL: Duration = Duration::from_secs(5);

fn current_sample_interval(state: &AppState) -> Duration {
    use crate::service::TaskService;
    match state.registry_ref().resolve::<TaskService>() {
        Some(svc) => {
            let v = svc.metrics_interval();
            if v.is_zero() { SAMPLER_DEFAULT_INTERVAL } else { v }
        }
        None => SAMPLER_DEFAULT_INTERVAL,
    }
}

fn sampler_enabled(state: &AppState) -> bool {
    use crate::service::TaskService;
    state
        .registry_ref()
        .resolve::<TaskService>()
        .map(|service| {
            let config = service.metrics_config();
            config.enabled && config.rss_sampler
        })
        .unwrap_or(false)
}

async fn run_sampler_loop(
    state: AppState,
    context: RunContext,
) -> claviron_runtime::registry::Result<()> {
    let role = role_label_from_run_mode(state.run_mode());

    tracing::info!(
        "📡 RSS sampler started (initial interval={:?}, role={})",
        current_sample_interval(&state),
        role
    );

    // Track which task names we published last tick so we can
    // clear the gauge when a task no longer has any live children.
    let mut last_published: std::collections::HashSet<String> =
        std::collections::HashSet::new();

    loop {
        // Read the cadence freshly each tick so a config reload
        // (TaskService::reload swaps the inner ArcSwap) takes
        // effect on the very next iteration.
        let interval = current_sample_interval(&state);

        tokio::select! {
            _ = context.cancelled() => {
                tracing::debug!("🔸 RSS sampler shutting down");
                return Ok(());
            }
            _ = tokio::time::sleep(interval) => {
                if !sampler_enabled(&state) {
                    for stale in last_published.drain() {
                        TASK_METRICS.clear_rss_bytes(&stale, &role);
                    }
                    continue;
                }
                let observed = sample_once(&role);

                // Clear previously-published task gauges that are
                // no longer represented in this tick (children
                // exited between samples).
                for stale in last_published.difference(&observed) {
                    TASK_METRICS.clear_rss_bytes(stale, &role);
                }
                last_published = observed;
            }
        }
    }
}

fn sample_once(role: &str) -> std::collections::HashSet<String> {
    let pids: Vec<((String, u8), u32)> = {
        let map = COMMAND_PIDS.read_unpoisoned();
        map.iter().map(|(k, v)| (k.clone(), *v)).collect()
    };

    let mut per_task: HashMap<String, u64> = HashMap::new();
    for ((task, _slot), pid) in pids {
        if let Some(rss) = claviron_sys::rss::read_rss(Some(pid)) {
            *per_task.entry(task).or_insert(0) += rss;
        }
    }

    let mut published = std::collections::HashSet::new();
    for (task, rss) in per_task {
        TASK_METRICS.set_rss_bytes(&task, role, rss);
        published.insert(task);
    }
    published
}

#[cfg(test)]
#[path = "../../tests/metrics/hooks.rs"]
mod tests;
