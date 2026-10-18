// ---------------------------------------------------------------------------
// Claviron-side FCGI client pool metrics.
//
// Distinct from `fpm.rs` (which scrapes PHP-FPM's own `pm.status_path` over
// the wire, master-side): these gauges describe *our* keep-alive client
// pool state (live/idle/replenisher status) plus the configured caps as
// labels, captured locally by each worker. The owner-local Orbit family
// carries a scalar fleet projection; labeled
// pool rows remain available in standalone mode.
// ---------------------------------------------------------------------------

use std::sync::RwLock;

use serde::{Deserialize, Serialize};

use super::FcgiRequestMetricsSnapshot;
use crate::events::PoolEventCounts;

pub static FCGI_METRICS: FcgiMetricsStore = FcgiMetricsStore::new();

pub struct FcgiMetricsStore {
    snapshot: RwLock<FcgiMetricsSnapshot>,
}

impl FcgiMetricsStore {
    pub const fn new() -> Self {
        Self { snapshot: RwLock::new(FcgiMetricsSnapshot::empty()) }
    }

    pub fn replace(
        &self,
        snapshot: FcgiMetricsSnapshot,
    ) {
        *self.snapshot.write().expect("fcgi metrics lock poisoned") = snapshot;
    }

    pub fn snapshot(&self) -> FcgiMetricsSnapshot {
        self.snapshot.read().expect("fcgi metrics lock poisoned").clone()
    }

    pub fn write_prometheus(
        &self,
        output: &mut String,
    ) {
        self.snapshot().write_prometheus(output);
    }
}

impl Default for FcgiMetricsStore {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FcgiMetricsSnapshot {
    pub captured_at: u64,
    pub pools: Vec<FcgiPoolMetrics>,
    pub pooled_count: u64,
    pub oneshot_count: u64,
}

impl FcgiMetricsSnapshot {
    pub const fn empty() -> Self {
        Self {
            captured_at: 0,
            pools: Vec::new(),
            pooled_count: 0,
            oneshot_count: 0,
        }
    }

    pub fn scalar_totals(
        &self,
        requests: FcgiRequestMetricsSnapshot,
    ) -> FcgiScalarMetricsSnapshot {
        FcgiScalarMetricsSnapshot {
            captured_at: self.captured_at,
            live: self.pools.iter().map(|pool| pool.live).sum(),
            idle: self.pools.iter().map(|pool| pool.idle).sum(),
            // Summed across this worker's endpoints, which are different
            // FPMs and so different ceilings. Across *workers* it is not
            // summed — see `merge`.
            max_live: self.pools.iter().map(|pool| pool.max_live).sum(),
            requests,
        }
    }

    pub fn write_prometheus(
        &self,
        output: &mut String,
    ) {
        use std::fmt::Write as FmtWrite;

        let _ = writeln!(
            output,
            "# HELP claviron_fcgi_capture_timestamp_seconds Last FCGI pool snapshot timestamp"
        );
        let _ = writeln!(
            output,
            "# TYPE claviron_fcgi_capture_timestamp_seconds gauge"
        );
        let _ = writeln!(
            output,
            "claviron_fcgi_capture_timestamp_seconds {}",
            self.captured_at
        );

        let _ = writeln!(
            output,
            "# HELP claviron_fcgi_pools_total Number of keep-alive pooled entries"
        );
        let _ = writeln!(output, "# TYPE claviron_fcgi_pools_total gauge");
        let _ =
            writeln!(output, "claviron_fcgi_pools_total {}", self.pooled_count);

        let _ = writeln!(
            output,
            "# HELP claviron_fcgi_oneshot_total Number of one-shot (no pool) entries"
        );
        let _ = writeln!(output, "# TYPE claviron_fcgi_oneshot_total gauge");
        let _ = writeln!(
            output,
            "claviron_fcgi_oneshot_total {}",
            self.oneshot_count
        );

        if self.pools.is_empty() {
            return;
        }

        // Pool info gauge — one line per pool, value 1, labels carry the
        // endpoint kind for grouping in dashboards.
        let _ = writeln!(
            output,
            "# HELP claviron_fcgi_pool_info FCGI pool metadata"
        );
        let _ = writeln!(output, "# TYPE claviron_fcgi_pool_info gauge");
        for pool in &self.pools {
            let _ = writeln!(
                output,
                "claviron_fcgi_pool_info{{pool=\"{}\",endpoint_kind=\"{}\"}} 1",
                escape_label(&pool.name),
                escape_label(&pool.endpoint_kind)
            );
        }

        // Runtime gauges
        write_pool_gauge(
            output,
            "claviron_fcgi_live",
            "Currently live FCGI conns (idle + leased)",
            &self.pools,
            |p| p.live,
        );
        write_pool_gauge(
            output,
            "claviron_fcgi_idle",
            "Currently idle FCGI conns (queue depth)",
            &self.pools,
            |p| p.idle,
        );
        write_pool_gauge(
            output,
            "claviron_fcgi_replenishing",
            "Replenisher task currently active (0/1)",
            &self.pools,
            |p| u64::from(p.replenishing),
        );

        // Config gauges (constant across reloads, useful for headroom math)
        write_pool_gauge(
            output,
            "claviron_fcgi_min_live",
            "Configured min_live (replenisher floor)",
            &self.pools,
            |p| p.min_live,
        );
        write_pool_gauge(
            output,
            "claviron_fcgi_max_idle",
            "Configured max_idle (idle queue cap)",
            &self.pools,
            |p| p.max_idle,
        );
        write_pool_gauge(
            output,
            "claviron_fcgi_max_live",
            "Configured process-local max_live",
            &self.pools,
            |p| p.max_live,
        );

        // What the pool did. Same names as the fleet renders, carrying the
        // `pool` label that only survives where the rows do not have to
        // cross Orbit — one process renders one of the two shapes, never
        // both, so the name does not have to change with the label.
        for (series, help, value) in PoolEventCounts::SERIES {
            let name = format!("claviron_fcgi_{series}");
            let _ = writeln!(output, "# HELP {name} {help}");
            let _ = writeln!(output, "# TYPE {name} counter");
            for pool in &self.pools {
                let _ = writeln!(
                    output,
                    "{}{{pool=\"{}\"}} {}",
                    name,
                    escape_label(&pool.name),
                    value(&pool.events)
                );
            }
        }
    }
}

impl Default for FcgiMetricsSnapshot {
    fn default() -> Self {
        Self::empty()
    }
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct FcgiScalarMetricsSnapshot {
    pub captured_at: u64,
    pub live: u64,
    pub idle: u64,
    pub max_live: u64,
    pub requests: FcgiRequestMetricsSnapshot,
}

impl FcgiScalarMetricsSnapshot {
    pub fn merge(
        &mut self,
        other: &Self,
    ) {
        self.captured_at = self.captured_at.max(other.captured_at);
        self.live += other.live;
        self.idle += other.idle;
        // Every worker owns a process-local pool, so aggregate capacity is
        // the sum of the process-local ceilings.
        self.max_live += other.max_live;
        self.requests.merge(&other.requests);
    }

    pub fn write_prometheus(
        &self,
        output: &mut String,
    ) {
        use std::fmt::Write as FmtWrite;

        self.requests.write_prometheus(output);

        let _ = writeln!(
            output,
            "# HELP claviron_fcgi_capture_timestamp_aggregate_seconds Last FCGI pool snapshot timestamp across all workers"
        );
        let _ = writeln!(
            output,
            "# TYPE claviron_fcgi_capture_timestamp_aggregate_seconds gauge"
        );
        let _ = writeln!(
            output,
            "claviron_fcgi_capture_timestamp_aggregate_seconds {}",
            self.captured_at
        );

        // A sum of what each worker's process-local pool holds.
        write_aggregate_gauge(
            output,
            "claviron_fcgi_live_aggregate",
            "Live FCGI conns summed over each worker's process-local pool",
            self.live,
        );
        write_aggregate_gauge(
            output,
            "claviron_fcgi_idle_aggregate",
            "Idle FCGI conns summed over each worker's own census",
            self.idle,
        );
        write_aggregate_gauge(
            output,
            "claviron_fcgi_max_live_aggregate",
            "Configured FCGI process-local max_live summed over workers",
            self.max_live,
        );
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FcgiPoolMetrics {
    pub name: String,
    /// "unix" or "tcp"
    pub endpoint_kind: String,
    // Runtime — this worker's own census
    pub live: u64,
    pub idle: u64,
    pub replenishing: bool,
    // Config (snapshotted from PoolOpts at capture time)
    pub min_live: u64,
    pub max_idle: u64,
    /// Process-local connection ceiling.
    pub max_live: u64,
    /// What this pool has done since it was built, as against what it
    /// holds now. Per pool here; summed for the fleet, where the pool
    /// label does not survive.
    pub events: PoolEventCounts,
}

fn write_pool_gauge(
    output: &mut String,
    name: &str,
    help: &str,
    pools: &[FcgiPoolMetrics],
    value: impl Fn(&FcgiPoolMetrics) -> u64,
) {
    use std::fmt::Write as FmtWrite;

    let _ = writeln!(output, "# HELP {name} {help}");
    let _ = writeln!(output, "# TYPE {name} gauge");
    for pool in pools {
        let _ = writeln!(
            output,
            "{}{{pool=\"{}\"}} {}",
            name,
            escape_label(&pool.name),
            value(pool)
        );
    }
}

fn write_aggregate_gauge(
    output: &mut String,
    name: &str,
    help: &str,
    value: u64,
) {
    use std::fmt::Write as FmtWrite;

    let _ = writeln!(output, "# HELP {name} {help}");
    let _ = writeln!(output, "# TYPE {name} gauge");
    let _ = writeln!(output, "{name} {value}");
}

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

#[cfg(test)]
#[path = "../../../tests/metrics/fcgi/store.rs"]
mod tests;
