//! Task-owned stores, snapshots, Orbit codecs, and Prometheus formatting.
//!
//! Mirrors the shape of `crate::http`. The struct + atomic fields
//! and the `write_prometheus` serialiser live here; the call sites
//! that increment / decrement these counters live in
//! `claviron-tasks/src/metrics/hooks.rs` and the surrounding runner code.
//!
//! Cardinality budget (see `claviron-tasks/ARCHITECTURE.md` once the
//! metrics doc lands): primary aggregation is by `{task}`. The
//! `role` label appears only on worker-health and external-process
//! RSS — concurrency-slot detail never reaches Prometheus, slots
//! are summed inside each forked worker.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{LazyLock, RwLock};

use claviron_helpers::sync::Unpoisoned;
use serde::{Deserialize, Serialize};

pub const TASK_SERVICE_ORBIT_METRICS_WIRE_LEN: usize = 26;

pub const TASK_SERVICE_ORBIT_METRICS_RING_SPEC: orbitive::metrics::RingSpec =
    orbitive::metrics::RingSpec::new(
        orbitive::metrics::METRIC_SNAPSHOT_RING_CAPACITY,
        TASK_SERVICE_ORBIT_METRICS_WIRE_LEN,
    );
/// Scan window for high-cardinality per-task and per-task-role snapshots.
pub const TASK_ORBIT_METRICS_RING_CAPACITY: usize = 16_384;

pub const TASK_ORBIT_METRICS_PAYLOAD_CAPACITY: usize = 256;

pub const TASK_ORBIT_METRICS_RING_SPEC: orbitive::metrics::RingSpec =
    orbitive::metrics::RingSpec::new(
        TASK_ORBIT_METRICS_RING_CAPACITY,
        TASK_ORBIT_METRICS_PAYLOAD_CAPACITY,
    );

pub const TASK_PAIR_ORBIT_METRICS_PAYLOAD_CAPACITY: usize = 256;

pub const TASK_PAIR_ORBIT_METRICS_RING_SPEC: orbitive::metrics::RingSpec =
    orbitive::metrics::RingSpec::new(
        TASK_ORBIT_METRICS_RING_CAPACITY,
        TASK_PAIR_ORBIT_METRICS_PAYLOAD_CAPACITY,
    );

/// Global task metrics instance. Each forked worker (and the master)
/// has its own; the supervisor aggregates snapshots from every fork
/// to produce the merged Prometheus output.
///
/// `LazyLock` because `HashMap::new()` isn't const-stable on the
/// MSRV we're targeting; the lock is initialised on first access
/// (typically at boot before any traffic).
pub static TASK_METRICS: LazyLock<TaskMetrics> =
    LazyLock::new(TaskMetrics::new);

/// Outcome of a single task attempt — labels the
/// `claviron_task_attempts_total` counter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttemptOutcome {
    Success,
    Error,
    Shutdown,
    GracefulTimeout,
    GaveUp,
}

impl AttemptOutcome {
    pub fn as_label(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Error => "error",
            Self::Shutdown => "shutdown",
            Self::GracefulTimeout => "graceful_timeout",
            Self::GaveUp => "gave_up",
        }
    }

    fn all() -> [Self; 5] {
        [
            Self::Success,
            Self::Error,
            Self::Shutdown,
            Self::GracefulTimeout,
            Self::GaveUp,
        ]
    }
}

/// Histogram bucket boundaries (seconds). Tail covers sub-millisecond
/// FCGI housekeep ticks up to ~10-minute ACME renewals. The +Inf
/// bucket is implicit (`count`).
const DURATION_BUCKETS: [f64; 9] =
    [0.001, 0.01, 0.1, 0.5, 1.0, 5.0, 30.0, 120.0, 600.0];

/// Per-task attempt counter set, one slot per outcome.
pub struct AttemptCounters {
    pub success: AtomicU64,
    pub error: AtomicU64,
    pub shutdown: AtomicU64,
    pub graceful_timeout: AtomicU64,
    pub gave_up: AtomicU64,
}

impl AttemptCounters {
    const fn new() -> Self {
        Self {
            success: AtomicU64::new(0),
            error: AtomicU64::new(0),
            shutdown: AtomicU64::new(0),
            graceful_timeout: AtomicU64::new(0),
            gave_up: AtomicU64::new(0),
        }
    }

    fn slot(
        &self,
        outcome: AttemptOutcome,
    ) -> &AtomicU64 {
        match outcome {
            AttemptOutcome::Success => &self.success,
            AttemptOutcome::Error => &self.error,
            AttemptOutcome::Shutdown => &self.shutdown,
            AttemptOutcome::GracefulTimeout => &self.graceful_timeout,
            AttemptOutcome::GaveUp => &self.gave_up,
        }
    }

    fn snapshot(&self) -> AttemptCountersSnapshot {
        AttemptCountersSnapshot {
            success: self.success.load(Ordering::Relaxed),
            error: self.error.load(Ordering::Relaxed),
            shutdown: self.shutdown.load(Ordering::Relaxed),
            graceful_timeout: self.graceful_timeout.load(Ordering::Relaxed),
            gave_up: self.gave_up.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttemptCountersSnapshot {
    pub success: u64,
    pub error: u64,
    pub shutdown: u64,
    pub graceful_timeout: u64,
    pub gave_up: u64,
}

impl AttemptCountersSnapshot {
    pub fn merge(
        &mut self,
        other: &Self,
    ) {
        self.success += other.success;
        self.error += other.error;
        self.shutdown += other.shutdown;
        self.graceful_timeout += other.graceful_timeout;
        self.gave_up += other.gave_up;
    }

    pub fn for_outcome(
        &self,
        outcome: AttemptOutcome,
    ) -> u64 {
        match outcome {
            AttemptOutcome::Success => self.success,
            AttemptOutcome::Error => self.error,
            AttemptOutcome::Shutdown => self.shutdown,
            AttemptOutcome::GracefulTimeout => self.graceful_timeout,
            AttemptOutcome::GaveUp => self.gave_up,
        }
    }
}

/// Per-task duration histogram. Cumulative buckets (Prometheus
/// convention: each bucket counts observations ≤ `le`).
pub struct DurationHistogram {
    pub buckets: [AtomicU64; 9],
    /// Sum of observed durations stored as microseconds (integer
    /// math); converted to seconds at serialisation.
    pub sum_us: AtomicU64,
    pub count: AtomicU64,
}

impl DurationHistogram {
    const fn new() -> Self {
        Self {
            buckets: [
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
            ],
            sum_us: AtomicU64::new(0),
            count: AtomicU64::new(0),
        }
    }

    pub fn observe(
        &self,
        seconds: f64,
    ) {
        if !seconds.is_finite() || seconds < 0.0 {
            return;
        }
        for (i, &le) in DURATION_BUCKETS.iter().enumerate() {
            if seconds <= le {
                self.buckets[i].fetch_add(1, Ordering::Relaxed);
            }
        }
        let us = (seconds * 1_000_000.0) as u64;
        self.sum_us.fetch_add(us, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self) -> DurationHistogramSnapshot {
        let mut buckets = [0u64; 9];
        for (i, b) in self.buckets.iter().enumerate() {
            buckets[i] = b.load(Ordering::Relaxed);
        }
        DurationHistogramSnapshot {
            buckets,
            sum_us: self.sum_us.load(Ordering::Relaxed),
            count: self.count.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DurationHistogramSnapshot {
    pub buckets: [u64; 9],
    pub sum_us: u64,
    pub count: u64,
}

/// Static metadata for the `claviron_task_info` self-describing
/// gauge. Set once at task registration and rarely changes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskInfo {
    /// `"builtin"` (registered via `#[task]`) or `"command"` (external
    /// process body via `tasks.yaml::command:`).
    pub kind: String,
    /// `"cron"` / `"interval"` / `"daemon"`.
    pub schedule: String,
}

/// Live (atomic) task metrics. Each forked process owns one of
/// these (`TASK_METRICS` static) and increments locally on the hot
/// path. Each process publishes its snapshot through Orbit; the master
/// merges those frames for the fleet-wide Prometheus output.
pub struct TaskMetrics {
    // Service-level — no labels.
    registered: AtomicU64,
    reload_total: AtomicU64,

    // Per-task counters / gauges. Map keys are task names.
    runs: RwLock<HashMap<String, AtomicU64>>,
    in_flight: RwLock<HashMap<String, AtomicU64>>,
    skipped: RwLock<HashMap<String, AtomicU64>>,
    attempts: RwLock<HashMap<String, AttemptCounters>>,
    durations: RwLock<HashMap<String, DurationHistogram>>,

    // Per-(task, role). Only used for worker-health and
    // external-process RSS. Concurrency-slot detail summed inside
    // each forked worker before being stored here.
    workers_healthy: RwLock<HashMap<(String, String), AtomicU64>>,
    workers_total: RwLock<HashMap<(String, String), AtomicU64>>,
    rss_bytes: RwLock<HashMap<(String, String), AtomicU64>>,

    // Self-describing info — static after registration.
    info: RwLock<HashMap<String, TaskInfo>>,
}

impl TaskMetrics {
    pub fn new() -> Self {
        Self {
            registered: AtomicU64::new(0),
            reload_total: AtomicU64::new(0),
            runs: RwLock::new(HashMap::new()),
            in_flight: RwLock::new(HashMap::new()),
            skipped: RwLock::new(HashMap::new()),
            attempts: RwLock::new(HashMap::new()),
            durations: RwLock::new(HashMap::new()),
            workers_healthy: RwLock::new(HashMap::new()),
            workers_total: RwLock::new(HashMap::new()),
            rss_bytes: RwLock::new(HashMap::new()),
            info: RwLock::new(HashMap::new()),
        }
    }

    // -- service-level setters / mutators -----------------------------------

    pub fn set_registered(
        &self,
        n: u64,
    ) {
        self.registered.store(n, Ordering::Relaxed);
    }

    pub fn inc_reload(&self) {
        self.reload_total.fetch_add(1, Ordering::Relaxed);
    }

    // -- per-task hooks -----------------------------------------------------

    pub fn record_run(
        &self,
        task: &str,
    ) {
        with_atomic(&self.runs, task, |c| {
            c.fetch_add(1, Ordering::Relaxed);
        });
    }

    pub fn record_attempt(
        &self,
        task: &str,
        outcome: AttemptOutcome,
    ) {
        let map_read = self.attempts.read_unpoisoned();
        if let Some(c) = map_read.get(task) {
            c.slot(outcome).fetch_add(1, Ordering::Relaxed);
            return;
        }
        drop(map_read);
        let mut map = self.attempts.write_unpoisoned();
        let counters =
            map.entry(task.to_string()).or_insert_with(AttemptCounters::new);
        counters.slot(outcome).fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_in_flight(
        &self,
        task: &str,
    ) {
        with_atomic(&self.in_flight, task, |c| {
            c.fetch_add(1, Ordering::Relaxed);
        });
    }

    pub fn dec_in_flight(
        &self,
        task: &str,
    ) {
        with_atomic(&self.in_flight, task, |c| {
            let _ = c.try_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                if v > 0 { Some(v - 1) } else { None }
            });
        });
    }

    pub fn record_skipped(
        &self,
        task: &str,
    ) {
        with_atomic(&self.skipped, task, |c| {
            c.fetch_add(1, Ordering::Relaxed);
        });
    }

    pub fn record_duration(
        &self,
        task: &str,
        seconds: f64,
    ) {
        let map_read = self.durations.read_unpoisoned();
        if let Some(h) = map_read.get(task) {
            h.observe(seconds);
            return;
        }
        drop(map_read);
        let mut map = self.durations.write_unpoisoned();
        let h =
            map.entry(task.to_string()).or_insert_with(DurationHistogram::new);
        h.observe(seconds);
    }

    // -- per-(task, role) setters ------------------------------------------

    pub fn set_workers_healthy(
        &self,
        task: &str,
        role: &str,
        n: u64,
    ) {
        with_pair_atomic(&self.workers_healthy, task, role, |c| {
            c.store(n, Ordering::Relaxed);
        });
    }

    pub fn set_workers_total(
        &self,
        task: &str,
        role: &str,
        n: u64,
    ) {
        with_pair_atomic(&self.workers_total, task, role, |c| {
            c.store(n, Ordering::Relaxed);
        });
    }

    pub fn set_rss_bytes(
        &self,
        task: &str,
        role: &str,
        bytes: u64,
    ) {
        with_pair_atomic(&self.rss_bytes, task, role, |c| {
            c.store(bytes, Ordering::Relaxed);
        });
    }

    pub fn clear_rss_bytes(
        &self,
        task: &str,
        role: &str,
    ) {
        let mut map = self.rss_bytes.write_unpoisoned();
        map.remove(&(task.to_string(), role.to_string()));
    }

    // -- info ---------------------------------------------------------------

    pub fn set_info(
        &self,
        task: &str,
        kind: &str,
        schedule: &str,
    ) {
        let mut map = self.info.write_unpoisoned();
        map.insert(
            task.to_string(),
            TaskInfo { kind: kind.to_string(), schedule: schedule.to_string() },
        );
    }

    // -- snapshot + emission ------------------------------------------------

    pub fn snapshot(&self) -> TaskMetricsSnapshot {
        TaskMetricsSnapshot {
            registered: self.registered.load(Ordering::Relaxed),
            reload_total: self.reload_total.load(Ordering::Relaxed),
            runs: snap_atomic_map(&self.runs),
            in_flight: snap_atomic_map(&self.in_flight),
            skipped: snap_atomic_map(&self.skipped),
            attempts: self
                .attempts
                .read()
                .unwrap()
                .iter()
                .map(|(k, v)| (k.clone(), v.snapshot()))
                .collect(),
            durations: self
                .durations
                .read()
                .unwrap()
                .iter()
                .map(|(k, v)| (k.clone(), v.snapshot()))
                .collect(),
            workers_healthy: snap_pair_atomic_map(&self.workers_healthy),
            workers_total: snap_pair_atomic_map(&self.workers_total),
            rss_bytes: snap_pair_atomic_map(&self.rss_bytes),
            info: self.info.read_unpoisoned().clone(),
        }
    }

    pub fn write_prometheus(
        &self,
        output: &mut String,
    ) {
        self.snapshot().write_prometheus(output);
    }
}

impl Default for TaskMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Serialisable point-in-time view of [`TaskMetrics`]. In standalone mode this
/// is formatted directly. In supervisor mode selected scalar/row projections
/// are published through dedicated Orbit metric families and merged back into
/// this shape before rendering `/metrics`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TaskMetricsSnapshot {
    pub registered: u64,
    pub reload_total: u64,

    pub runs: HashMap<String, u64>,
    pub in_flight: HashMap<String, u64>,
    pub skipped: HashMap<String, u64>,
    pub attempts: HashMap<String, AttemptCountersSnapshot>,
    pub durations: HashMap<String, DurationHistogramSnapshot>,

    #[serde(default, with = "pair_metric_map")]
    pub workers_healthy: HashMap<(String, String), u64>,
    #[serde(default, with = "pair_metric_map")]
    pub workers_total: HashMap<(String, String), u64>,
    #[serde(default, with = "pair_metric_map")]
    pub rss_bytes: HashMap<(String, String), u64>,

    pub info: HashMap<String, TaskInfo>,
}

impl TaskMetricsSnapshot {
    /// Append this snapshot in Prometheus text format to `output`.
    pub fn write_prometheus(
        &self,
        output: &mut String,
    ) {
        use std::fmt::Write as FmtWrite;

        // ---- service-level -------------------------------------------------
        let _ = writeln!(
            output,
            "# HELP claviron_tasks_registered Number of registered tasks"
        );
        let _ = writeln!(output, "# TYPE claviron_tasks_registered gauge");
        let _ =
            writeln!(output, "claviron_tasks_registered {}", self.registered);
        let _ = writeln!(output);

        let _ = writeln!(
            output,
            "# HELP claviron_tasks_running Distinct tasks with at least one in-flight attempt"
        );
        let _ = writeln!(output, "# TYPE claviron_tasks_running gauge");
        let running =
            self.in_flight.values().filter(|value| **value > 0).count();
        let _ = writeln!(output, "claviron_tasks_running {}", running);
        let _ = writeln!(output);

        let _ = writeln!(
            output,
            "# HELP claviron_tasks_reload_total Total task-service reloads observed"
        );
        let _ = writeln!(output, "# TYPE claviron_tasks_reload_total counter");
        let _ = writeln!(
            output,
            "claviron_tasks_reload_total {}",
            self.reload_total
        );
        let _ = writeln!(output);

        // ---- per-task counters / gauges -----------------------------------
        write_per_task_counter(
            output,
            "claviron_task_runs_total",
            "Scheduled task executions (excludes retries)",
            &self.runs,
        );
        write_per_task_gauge(
            output,
            "claviron_task_in_flight",
            "Currently in-flight task attempts",
            &self.in_flight,
        );
        write_per_task_counter(
            output,
            "claviron_task_skipped_total",
            "Occurrences skipped due to overlap, missed schedule, or fail-closed arbitration",
            &self.skipped,
        );

        // ---- attempts (task, outcome) -------------------------------------
        let _ = writeln!(
            output,
            "# HELP claviron_task_attempts_total Per-attempt outcome counters"
        );
        let _ = writeln!(output, "# TYPE claviron_task_attempts_total counter");
        let mut attempt_keys: Vec<&String> = self.attempts.keys().collect();
        attempt_keys.sort_unstable();
        for task in attempt_keys {
            let snap = &self.attempts[task];
            for outcome in AttemptOutcome::all() {
                let v = snap.for_outcome(outcome);
                let _ = writeln!(
                    output,
                    "claviron_task_attempts_total{{task=\"{}\",outcome=\"{}\"}} {}",
                    escape_label(task),
                    outcome.as_label(),
                    v
                );
            }
        }
        let _ = writeln!(output);

        // ---- duration histogram -------------------------------------------
        let _ = writeln!(
            output,
            "# HELP claviron_task_duration_seconds Wall-clock duration per attempt (seconds)"
        );
        let _ =
            writeln!(output, "# TYPE claviron_task_duration_seconds histogram");
        let mut dur_keys: Vec<&String> = self.durations.keys().collect();
        dur_keys.sort_unstable();
        for task in dur_keys {
            let h = &self.durations[task];
            for (i, le) in DURATION_BUCKETS.iter().enumerate() {
                let _ = writeln!(
                    output,
                    "claviron_task_duration_seconds_bucket{{task=\"{}\",le=\"{}\"}} {}",
                    escape_label(task),
                    le,
                    h.buckets[i]
                );
            }
            let _ = writeln!(
                output,
                "claviron_task_duration_seconds_bucket{{task=\"{}\",le=\"+Inf\"}} {}",
                escape_label(task),
                h.count
            );
            let _ = writeln!(
                output,
                "claviron_task_duration_seconds_sum{{task=\"{}\"}} {:.6}",
                escape_label(task),
                (h.sum_us as f64) / 1_000_000.0
            );
            let _ = writeln!(
                output,
                "claviron_task_duration_seconds_count{{task=\"{}\"}} {}",
                escape_label(task),
                h.count
            );
        }
        let _ = writeln!(output);

        // ---- worker health (task, role) -----------------------------------
        write_pair_gauge(
            output,
            "claviron_task_workers_healthy",
            "Healthy worker slots per (task, role)",
            &self.workers_healthy,
        );
        write_pair_gauge(
            output,
            "claviron_task_workers_total",
            "Configured worker slots per (task, role)",
            &self.workers_total,
        );

        // ---- RSS for external processes -----------------------------------
        write_pair_gauge(
            output,
            "claviron_task_process_rss_bytes",
            "Sum of RSS across concurrency slots of an external-process task per role",
            &self.rss_bytes,
        );

        // ---- info gauge ---------------------------------------------------
        let _ = writeln!(
            output,
            "# HELP claviron_task_info Self-describing metadata (always 1)"
        );
        let _ = writeln!(output, "# TYPE claviron_task_info gauge");
        let mut info_keys: Vec<&String> = self.info.keys().collect();
        info_keys.sort_unstable();
        for task in info_keys {
            let info = &self.info[task];
            let _ = writeln!(
                output,
                "claviron_task_info{{task=\"{}\",kind=\"{}\",schedule=\"{}\"}} 1",
                escape_label(task),
                escape_label(&info.kind),
                escape_label(&info.schedule)
            );
        }
        let _ = writeln!(output);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TaskServiceOrbitMetrics {
    pub node_id: u16,
    pub captured_at: u64,
    pub registered: u64,
    pub reload_total: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskOrbitMetrics {
    pub node_id: u16,
    pub captured_at: u64,
    pub task: String,
    pub runs: u64,
    pub in_flight: u64,
    pub skipped: u64,
    pub attempts: AttemptCountersSnapshot,
    pub info: Option<TaskInfo>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskPairMetricKind {
    WorkersHealthy,
    WorkersTotal,
    RssBytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskPairOrbitMetrics {
    pub node_id: u16,
    pub captured_at: u64,
    pub kind: TaskPairMetricKind,
    pub task: String,
    pub role: String,
    pub value: u64,
}

impl TaskServiceOrbitMetrics {
    pub fn from_snapshot(
        node_id: u16,
        captured_at: u64,
        snapshot: &TaskMetricsSnapshot,
    ) -> Self {
        Self {
            node_id,
            captured_at,
            registered: snapshot.registered,
            reload_total: snapshot.reload_total,
        }
    }

    pub fn merge_into(
        self,
        snapshot: &mut TaskMetricsSnapshot,
    ) {
        snapshot.registered = snapshot.registered.max(self.registered);
        snapshot.reload_total += self.reload_total;
    }
}

impl TaskOrbitMetrics {
    pub fn rows_from_snapshot(
        node_id: u16,
        captured_at: u64,
        snapshot: &TaskMetricsSnapshot,
    ) -> Vec<Self> {
        let mut names = std::collections::BTreeSet::new();
        names.extend(snapshot.runs.keys().cloned());
        names.extend(snapshot.in_flight.keys().cloned());
        names.extend(snapshot.skipped.keys().cloned());
        names.extend(snapshot.attempts.keys().cloned());
        names.extend(snapshot.info.keys().cloned());

        names
            .into_iter()
            .map(|task| Self {
                node_id,
                captured_at,
                runs: snapshot.runs.get(&task).copied().unwrap_or_default(),
                in_flight: snapshot
                    .in_flight
                    .get(&task)
                    .copied()
                    .unwrap_or_default(),
                skipped: snapshot
                    .skipped
                    .get(&task)
                    .copied()
                    .unwrap_or_default(),
                attempts: snapshot
                    .attempts
                    .get(&task)
                    .cloned()
                    .unwrap_or_default(),
                info: snapshot.info.get(&task).cloned(),
                task,
            })
            .collect()
    }

    pub fn merge_into(
        self,
        snapshot: &mut TaskMetricsSnapshot,
    ) {
        *snapshot.runs.entry(self.task.clone()).or_insert(0) += self.runs;
        *snapshot.in_flight.entry(self.task.clone()).or_insert(0) +=
            self.in_flight;
        *snapshot.skipped.entry(self.task.clone()).or_insert(0) += self.skipped;
        snapshot
            .attempts
            .entry(self.task.clone())
            .or_default()
            .merge(&self.attempts);
        if let Some(info) = self.info {
            snapshot.info.entry(self.task).or_insert(info);
        }
    }
}

impl TaskPairOrbitMetrics {
    pub fn rows_from_snapshot(
        node_id: u16,
        captured_at: u64,
        snapshot: &TaskMetricsSnapshot,
    ) -> Vec<Self> {
        let mut rows = Vec::new();
        rows.extend(snapshot.workers_healthy.iter().map(
            |((task, role), value)| Self {
                node_id,
                captured_at,
                kind: TaskPairMetricKind::WorkersHealthy,
                task: task.clone(),
                role: role.clone(),
                value: *value,
            },
        ));
        rows.extend(snapshot.workers_total.iter().map(
            |((task, role), value)| Self {
                node_id,
                captured_at,
                kind: TaskPairMetricKind::WorkersTotal,
                task: task.clone(),
                role: role.clone(),
                value: *value,
            },
        ));
        rows.extend(snapshot.rss_bytes.iter().map(|((task, role), value)| {
            Self {
                node_id,
                captured_at,
                kind: TaskPairMetricKind::RssBytes,
                task: task.clone(),
                role: role.clone(),
                value: *value,
            }
        }));
        rows
    }

    pub fn merge_into(
        self,
        snapshot: &mut TaskMetricsSnapshot,
    ) {
        let key = (self.task, self.role);
        match self.kind {
            TaskPairMetricKind::WorkersHealthy => {
                *snapshot.workers_healthy.entry(key).or_insert(0) += self.value;
            }
            TaskPairMetricKind::WorkersTotal => {
                *snapshot.workers_total.entry(key).or_insert(0) += self.value;
            }
            TaskPairMetricKind::RssBytes => {
                *snapshot.rss_bytes.entry(key).or_insert(0) += self.value;
            }
        }
    }
}

impl orbitive::metrics::OrbitTyped for TaskServiceOrbitMetrics {
    const KIND: u8 = 217;
    const RING_SPEC: orbitive::metrics::RingSpec =
        TASK_SERVICE_ORBIT_METRICS_RING_SPEC;
}

impl orbitive::metrics::OrbitMetricSnapshot for TaskServiceOrbitMetrics {
    const FAMILY: &'static str = "claviron_task_service_metrics";

    fn node_id(&self) -> u16 {
        self.node_id
    }

    fn captured_at_unix_secs(&self) -> u64 {
        self.captured_at
    }

    fn encode(&self) -> Result<Vec<u8>, String> {
        let mut out = Vec::with_capacity(TASK_SERVICE_ORBIT_METRICS_WIRE_LEN);
        out.extend_from_slice(&self.node_id.to_le_bytes());
        for value in [self.captured_at, self.registered, self.reload_total] {
            out.extend_from_slice(&value.to_le_bytes());
        }
        Ok(out)
    }

    fn decode(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() != TASK_SERVICE_ORBIT_METRICS_WIRE_LEN {
            return Err(format!(
                "bad task service metrics payload len {}",
                bytes.len()
            ));
        }
        let node_id =
            u16::from_le_bytes(bytes[0..2].try_into().expect("node bytes"));
        let mut offset = 2;
        let next_u64 = |offset: &mut usize| {
            let value = u64::from_le_bytes(
                bytes[*offset..*offset + 8].try_into().expect("u64 bytes"),
            );
            *offset += 8;
            value
        };
        Ok(Self {
            node_id,
            captured_at: next_u64(&mut offset),
            registered: next_u64(&mut offset),
            reload_total: next_u64(&mut offset),
        })
    }
}

impl orbitive::metrics::OrbitTyped for TaskOrbitMetrics {
    const KIND: u8 = 218;
    const RING_SPEC: orbitive::metrics::RingSpec = TASK_ORBIT_METRICS_RING_SPEC;
}

impl orbitive::metrics::OrbitMetricSnapshot for TaskOrbitMetrics {
    const FAMILY: &'static str = "claviron_task_metrics";

    fn node_id(&self) -> u16 {
        self.node_id
    }

    fn captured_at_unix_secs(&self) -> u64 {
        self.captured_at
    }

    fn encode(&self) -> Result<Vec<u8>, String> {
        let mut out = Vec::with_capacity(96);
        out.extend_from_slice(&self.node_id.to_le_bytes());
        out.extend_from_slice(&self.captured_at.to_le_bytes());
        write_string(&mut out, &self.task)?;
        write_task_info(&mut out, self.info.as_ref())?;
        for value in [
            self.runs,
            self.in_flight,
            self.skipped,
            self.attempts.success,
            self.attempts.error,
            self.attempts.shutdown,
            self.attempts.graceful_timeout,
            self.attempts.gave_up,
        ] {
            out.extend_from_slice(&value.to_le_bytes());
        }
        Ok(out)
    }

    fn decode(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() < 2 + 8 + 1 + 1 + 8 * 8 {
            return Err(format!(
                "bad task metrics payload len {}",
                bytes.len()
            ));
        }
        let node_id =
            u16::from_le_bytes(bytes[0..2].try_into().expect("node bytes"));
        let mut offset = 2;
        let next_u64 = |offset: &mut usize| {
            let value = u64::from_le_bytes(
                bytes[*offset..*offset + 8].try_into().expect("u64 bytes"),
            );
            *offset += 8;
            value
        };
        let captured_at = next_u64(&mut offset);
        let task = read_string(bytes, &mut offset)?;
        let info = read_task_info(bytes, &mut offset)?;
        let runs = next_u64(&mut offset);
        let in_flight = next_u64(&mut offset);
        let skipped = next_u64(&mut offset);
        let attempts = AttemptCountersSnapshot {
            success: next_u64(&mut offset),
            error: next_u64(&mut offset),
            shutdown: next_u64(&mut offset),
            graceful_timeout: next_u64(&mut offset),
            gave_up: next_u64(&mut offset),
        };
        if offset != bytes.len() {
            return Err(format!(
                "trailing task metrics bytes {}",
                bytes.len() - offset
            ));
        }
        Ok(Self {
            node_id,
            captured_at,
            task,
            runs,
            in_flight,
            skipped,
            attempts,
            info,
        })
    }
}

impl orbitive::metrics::OrbitMetricKeyedSnapshot for TaskOrbitMetrics {
    type Key = (u16, String);

    fn metric_key(&self) -> Self::Key {
        (self.node_id, self.task.clone())
    }
}

impl orbitive::metrics::OrbitTyped for TaskPairOrbitMetrics {
    const KIND: u8 = 219;
    const RING_SPEC: orbitive::metrics::RingSpec =
        TASK_PAIR_ORBIT_METRICS_RING_SPEC;
}

impl orbitive::metrics::OrbitMetricSnapshot for TaskPairOrbitMetrics {
    const FAMILY: &'static str = "claviron_task_pair_metrics";

    fn node_id(&self) -> u16 {
        self.node_id
    }

    fn captured_at_unix_secs(&self) -> u64 {
        self.captured_at
    }

    fn encode(&self) -> Result<Vec<u8>, String> {
        let mut out = Vec::with_capacity(80);
        out.extend_from_slice(&self.node_id.to_le_bytes());
        out.extend_from_slice(&self.captured_at.to_le_bytes());
        out.push(self.kind.as_u8());
        write_string(&mut out, &self.task)?;
        write_string(&mut out, &self.role)?;
        out.extend_from_slice(&self.value.to_le_bytes());
        Ok(out)
    }

    fn decode(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() < 2 + 8 + 1 + 2 + 8 {
            return Err(format!(
                "bad task pair metrics payload len {}",
                bytes.len()
            ));
        }
        let node_id =
            u16::from_le_bytes(bytes[0..2].try_into().expect("node bytes"));
        let mut offset = 2;
        let captured_at = u64::from_le_bytes(
            bytes[offset..offset + 8].try_into().expect("u64 bytes"),
        );
        offset += 8;
        let kind = TaskPairMetricKind::from_u8(bytes[offset])?;
        offset += 1;
        let task = read_string(bytes, &mut offset)?;
        let role = read_string(bytes, &mut offset)?;
        if bytes.len() != offset + 8 {
            return Err(format!("bad task pair numeric len {}", bytes.len()));
        }
        let value = u64::from_le_bytes(
            bytes[offset..offset + 8].try_into().expect("u64 bytes"),
        );
        Ok(Self { node_id, captured_at, kind, task, role, value })
    }
}

impl orbitive::metrics::OrbitMetricKeyedSnapshot for TaskPairOrbitMetrics {
    type Key = (u16, u8, String, String);

    fn metric_key(&self) -> Self::Key {
        (self.node_id, self.kind.as_u8(), self.task.clone(), self.role.clone())
    }
}

impl TaskPairMetricKind {
    fn as_u8(self) -> u8 {
        match self {
            Self::WorkersHealthy => 1,
            Self::WorkersTotal => 2,
            Self::RssBytes => 3,
        }
    }

    fn from_u8(value: u8) -> Result<Self, String> {
        match value {
            1 => Ok(Self::WorkersHealthy),
            2 => Ok(Self::WorkersTotal),
            3 => Ok(Self::RssBytes),
            _ => Err(format!("unknown task pair metric kind {}", value)),
        }
    }
}

fn write_task_info(
    out: &mut Vec<u8>,
    info: Option<&TaskInfo>,
) -> Result<(), String> {
    let Some(info) = info else {
        out.push(0);
        return Ok(());
    };
    // Both halves must stay exhaustive over `TaskExecutorKind::as_str` and
    // `TaskTiming::label`. They are matched as strings, so the compiler cannot
    // check that for us the way it checks the snapshot and proto mappings — a
    // missing arm is a publish-time failure instead of a build failure, and
    // `manual` was missing here from the start without any deployment that had
    // a manual task to find it.
    let kind = match info.kind.as_str() {
        "builtin" => 1,
        "command" => 2,
        "php" => 3,
        "js" => 4,
        value => return Err(format!("unknown task info kind {value}")),
    };
    let schedule = match info.schedule.as_str() {
        "cron" => 1,
        "interval" => 2,
        "daemon" => 3,
        "manual" => 4,
        value => return Err(format!("unknown task info schedule {value}")),
    };
    out.extend_from_slice(&[1, kind, schedule]);
    Ok(())
}

fn read_task_info(
    bytes: &[u8],
    offset: &mut usize,
) -> Result<Option<TaskInfo>, String> {
    let present = *bytes
        .get(*offset)
        .ok_or_else(|| "missing task info flag".to_string())?;
    *offset += 1;
    if present == 0 {
        return Ok(None);
    }
    if present != 1 || bytes.len() < *offset + 2 {
        return Err("bad task info encoding".to_string());
    }
    let kind = match bytes[*offset] {
        1 => "builtin",
        2 => "command",
        3 => "php",
        4 => "js",
        value => return Err(format!("unknown task info kind {value}")),
    };
    let schedule = match bytes[*offset + 1] {
        1 => "cron",
        2 => "interval",
        3 => "daemon",
        4 => "manual",
        value => return Err(format!("unknown task info schedule {value}")),
    };
    *offset += 2;
    Ok(Some(TaskInfo {
        kind: kind.to_string(),
        schedule: schedule.to_string(),
    }))
}

fn write_string(
    out: &mut Vec<u8>,
    value: &str,
) -> Result<(), String> {
    let len = u8::try_from(value.len()).map_err(|_| {
        format!("task metrics string too long: {}", value.len())
    })?;
    out.push(len);
    out.extend_from_slice(value.as_bytes());
    Ok(())
}

fn read_string(
    bytes: &[u8],
    offset: &mut usize,
) -> Result<String, String> {
    if *offset >= bytes.len() {
        return Err("missing task metrics string length".to_string());
    }
    let len = usize::from(bytes[*offset]);
    *offset += 1;
    if bytes.len() < *offset + len {
        return Err("truncated task metrics string".to_string());
    }
    let value = std::str::from_utf8(&bytes[*offset..*offset + len])
        .map_err(|e| e.to_string())?
        .to_owned();
    *offset += len;
    Ok(value)
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

fn with_atomic<F: FnOnce(&AtomicU64)>(
    map: &RwLock<HashMap<String, AtomicU64>>,
    key: &str,
    f: F,
) {
    if let Some(c) = map.read_unpoisoned().get(key) {
        f(c);
        return;
    }
    let mut w = map.write_unpoisoned();
    let c = w.entry(key.to_string()).or_insert_with(|| AtomicU64::new(0));
    f(c);
}

fn with_pair_atomic<F: FnOnce(&AtomicU64)>(
    map: &RwLock<HashMap<(String, String), AtomicU64>>,
    a: &str,
    b: &str,
    f: F,
) {
    let key = (a.to_string(), b.to_string());
    if let Some(c) = map.read_unpoisoned().get(&key) {
        f(c);
        return;
    }
    let mut w = map.write_unpoisoned();
    let c = w.entry(key).or_insert_with(|| AtomicU64::new(0));
    f(c);
}

fn snap_atomic_map(
    map: &RwLock<HashMap<String, AtomicU64>>
) -> HashMap<String, u64> {
    map.read_unpoisoned()
        .iter()
        .map(|(k, v)| (k.clone(), v.load(Ordering::Relaxed)))
        .collect()
}

fn snap_pair_atomic_map(
    map: &RwLock<HashMap<(String, String), AtomicU64>>
) -> HashMap<(String, String), u64> {
    map.read_unpoisoned()
        .iter()
        .map(|(k, v)| (k.clone(), v.load(Ordering::Relaxed)))
        .collect()
}

fn write_per_task_counter(
    output: &mut String,
    name: &str,
    help: &str,
    map: &HashMap<String, u64>,
) {
    use std::fmt::Write as FmtWrite;
    let _ = writeln!(output, "# HELP {} {}", name, help);
    let _ = writeln!(output, "# TYPE {} counter", name);
    let mut keys: Vec<&String> = map.keys().collect();
    keys.sort_unstable();
    for task in keys {
        let _ = writeln!(
            output,
            "{}{{task=\"{}\"}} {}",
            name,
            escape_label(task),
            map[task]
        );
    }
    let _ = writeln!(output);
}

fn write_per_task_gauge(
    output: &mut String,
    name: &str,
    help: &str,
    map: &HashMap<String, u64>,
) {
    use std::fmt::Write as FmtWrite;
    let _ = writeln!(output, "# HELP {} {}", name, help);
    let _ = writeln!(output, "# TYPE {} gauge", name);
    let mut keys: Vec<&String> = map.keys().collect();
    keys.sort_unstable();
    for task in keys {
        let _ = writeln!(
            output,
            "{}{{task=\"{}\"}} {}",
            name,
            escape_label(task),
            map[task]
        );
    }
    let _ = writeln!(output);
}

fn write_pair_gauge(
    output: &mut String,
    name: &str,
    help: &str,
    map: &HashMap<(String, String), u64>,
) {
    use std::fmt::Write as FmtWrite;
    let _ = writeln!(output, "# HELP {} {}", name, help);
    let _ = writeln!(output, "# TYPE {} gauge", name);
    let mut keys: Vec<&(String, String)> = map.keys().collect();
    keys.sort_unstable();
    for (task, role) in keys {
        let v = map[&(task.clone(), role.clone())];
        let _ = writeln!(
            output,
            "{}{{task=\"{}\",role=\"{}\"}} {}",
            name,
            escape_label(task),
            escape_label(role),
            v
        );
    }
    let _ = writeln!(output);
}

/// Escape a label value per the Prometheus exposition format.
fn escape_label(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for c in value.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            _ => out.push(c),
        }
    }
    out
}

mod pair_metric_map {
    use std::collections::HashMap;

    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    #[derive(Deserialize, Serialize)]
    struct PairMetric {
        task: String,
        role: String,
        value: u64,
    }

    pub fn serialize<S>(
        map: &HashMap<(String, String), u64>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut entries = map
            .iter()
            .map(|((task, role), value)| PairMetric {
                task: task.clone(),
                role: role.clone(),
                value: *value,
            })
            .collect::<Vec<_>>();
        entries.sort_unstable_by(|a, b| {
            a.task.cmp(&b.task).then_with(|| a.role.cmp(&b.role))
        });
        entries.serialize(serializer)
    }

    pub fn deserialize<'de, D>(
        deserializer: D
    ) -> Result<HashMap<(String, String), u64>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let entries = Vec::<PairMetric>::deserialize(deserializer)?;
        Ok(entries
            .into_iter()
            .map(|entry| ((entry.task, entry.role), entry.value))
            .collect())
    }
}
