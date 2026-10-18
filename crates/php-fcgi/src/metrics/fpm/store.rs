// ---------------------------------------------------------------------------
// PHP-FPM status metrics.
//
// These are scraped from PHP-FPM's FastCGI `pm.status_path` endpoint and
// transported by the FCGI owner's keyed Orbit family.
// ---------------------------------------------------------------------------

use std::sync::RwLock;

use serde::{Deserialize, Serialize};

pub const FPM_POOL_ORBIT_METRICS_PAYLOAD_CAPACITY: usize = 256;

pub const FPM_POOL_ORBIT_METRICS_RING_SPEC: orbitive::metrics::RingSpec =
    orbitive::metrics::RingSpec::new(
        orbitive::metrics::KEYED_METRIC_RING_CAPACITY,
        FPM_POOL_ORBIT_METRICS_PAYLOAD_CAPACITY,
    );

pub static FPM_METRICS: FpmMetricsStore = FpmMetricsStore::new();

pub struct FpmMetricsStore {
    snapshot: RwLock<FpmMetricsSnapshot>,
}

impl FpmMetricsStore {
    pub const fn new() -> Self {
        Self { snapshot: RwLock::new(FpmMetricsSnapshot::empty()) }
    }

    pub fn replace(
        &self,
        snapshot: FpmMetricsSnapshot,
    ) {
        *self.snapshot.write().expect("fpm metrics lock poisoned") = snapshot;
    }

    pub fn snapshot(&self) -> FpmMetricsSnapshot {
        self.snapshot.read().expect("fpm metrics lock poisoned").clone()
    }

    pub fn write_prometheus(
        &self,
        output: &mut String,
    ) {
        self.snapshot().write_prometheus(output);
    }
}

impl Default for FpmMetricsStore {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FpmMetricsSnapshot {
    pub scraped_at: u64,
    pub pools: Vec<FpmPoolMetrics>,
}

impl FpmMetricsSnapshot {
    pub const fn empty() -> Self {
        Self { scraped_at: 0, pools: Vec::new() }
    }

    pub fn new(
        scraped_at: u64,
        pools: Vec<FpmPoolMetrics>,
    ) -> Self {
        Self { scraped_at, pools }
    }

    pub fn write_prometheus(
        &self,
        output: &mut String,
    ) {
        use std::fmt::Write as FmtWrite;

        if self.pools.is_empty() {
            return;
        }

        let _ = writeln!(
            output,
            "# HELP claviron_fpm_scrape_timestamp_seconds Last PHP-FPM status scrape timestamp"
        );
        let _ = writeln!(
            output,
            "# TYPE claviron_fpm_scrape_timestamp_seconds gauge"
        );
        let _ = writeln!(
            output,
            "claviron_fpm_scrape_timestamp_seconds {}",
            self.scraped_at
        );

        let _ = writeln!(
            output,
            "# HELP claviron_fpm_pool_info PHP-FPM pool metadata"
        );
        let _ = writeln!(output, "# TYPE claviron_fpm_pool_info gauge");
        for pool in self.pools.iter().filter(|pool| pool.up) {
            let _ = writeln!(
                output,
                "claviron_fpm_pool_info{{pool=\"{}\",fpm_pool=\"{}\",process_manager=\"{}\"}} 1",
                escape_label(&pool.pool),
                escape_label(&pool.fpm_pool),
                escape_label(&pool.process_manager)
            );
        }

        write_pool_gauge(
            output,
            "claviron_fpm_up",
            "Whether the PHP-FPM status endpoint was reachable",
            &self.pools,
            |m| u64::from(m.up),
        );
        write_pool_gauge(
            output,
            "claviron_fpm_start_time_seconds",
            "PHP-FPM pool start time",
            &self.pools,
            |m| m.start_time,
        );
        write_pool_gauge(
            output,
            "claviron_fpm_start_since_seconds",
            "Seconds since PHP-FPM pool start",
            &self.pools,
            |m| m.start_since,
        );
        write_pool_counter(
            output,
            "claviron_fpm_accepted_connections_total",
            "Accepted PHP-FPM connections",
            &self.pools,
            |m| m.accepted_conn,
        );
        write_pool_gauge(
            output,
            "claviron_fpm_listen_queue",
            "Current PHP-FPM listen queue depth",
            &self.pools,
            |m| m.listen_queue,
        );
        write_pool_gauge(
            output,
            "claviron_fpm_max_listen_queue",
            "Maximum observed PHP-FPM listen queue depth",
            &self.pools,
            |m| m.max_listen_queue,
        );
        write_pool_gauge(
            output,
            "claviron_fpm_listen_queue_len",
            "PHP-FPM listen queue capacity",
            &self.pools,
            |m| m.listen_queue_len,
        );
        write_pool_gauge(
            output,
            "claviron_fpm_idle_processes",
            "Idle PHP-FPM worker processes",
            &self.pools,
            |m| m.idle_processes,
        );
        write_pool_gauge(
            output,
            "claviron_fpm_active_processes",
            "Active PHP-FPM worker processes",
            &self.pools,
            |m| m.active_processes,
        );
        write_pool_gauge(
            output,
            "claviron_fpm_total_processes",
            "Total PHP-FPM worker processes",
            &self.pools,
            |m| m.total_processes,
        );
        write_pool_gauge(
            output,
            "claviron_fpm_max_active_processes",
            "Maximum observed active PHP-FPM worker processes",
            &self.pools,
            |m| m.max_active_processes,
        );
        write_pool_counter(
            output,
            "claviron_fpm_max_children_reached_total",
            "Times PHP-FPM reached max children",
            &self.pools,
            |m| m.max_children_reached,
        );
        write_pool_counter(
            output,
            "claviron_fpm_slow_requests_total",
            "PHP-FPM slow requests",
            &self.pools,
            |m| m.slow_requests,
        );
        write_pool_gauge(
            output,
            "claviron_fpm_memory_peak_bytes",
            "PHP-FPM reported memory peak",
            &self.pools,
            |m| m.memory_peak,
        );
    }
}

impl Default for FpmMetricsSnapshot {
    fn default() -> Self {
        Self::empty()
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FpmPoolMetrics {
    /// Claviron's configured FPM entry name.
    pub pool: String,
    /// Pool name reported by PHP-FPM's status response.
    pub fpm_pool: String,
    pub process_manager: String,
    pub up: bool,
    pub start_time: u64,
    pub start_since: u64,
    pub accepted_conn: u64,
    pub listen_queue: u64,
    pub max_listen_queue: u64,
    pub listen_queue_len: u64,
    pub idle_processes: u64,
    pub active_processes: u64,
    pub total_processes: u64,
    pub max_active_processes: u64,
    pub max_children_reached: u64,
    pub slow_requests: u64,
    pub memory_peak: u64,
}

impl FpmPoolMetrics {
    pub fn down(pool: impl Into<String>) -> Self {
        Self { pool: pool.into(), up: false, ..Self::default() }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FpmPoolOrbitMetrics {
    pub scraped_at: u64,
    pub pool: String,
    pub fpm_pool: String,
    pub process_manager: String,
    pub up: bool,
    pub start_time: u64,
    pub start_since: u64,
    pub accepted_conn: u64,
    pub listen_queue: u64,
    pub max_listen_queue: u64,
    pub listen_queue_len: u64,
    pub idle_processes: u64,
    pub active_processes: u64,
    pub total_processes: u64,
    pub max_active_processes: u64,
    pub max_children_reached: u64,
    pub slow_requests: u64,
    pub memory_peak: u64,
}

impl FpmPoolOrbitMetrics {
    pub fn from_pool(
        scraped_at: u64,
        pool: &FpmPoolMetrics,
    ) -> Self {
        Self {
            scraped_at,
            pool: pool.pool.clone(),
            fpm_pool: pool.fpm_pool.clone(),
            process_manager: pool.process_manager.clone(),
            up: pool.up,
            start_time: pool.start_time,
            start_since: pool.start_since,
            accepted_conn: pool.accepted_conn,
            listen_queue: pool.listen_queue,
            max_listen_queue: pool.max_listen_queue,
            listen_queue_len: pool.listen_queue_len,
            idle_processes: pool.idle_processes,
            active_processes: pool.active_processes,
            total_processes: pool.total_processes,
            max_active_processes: pool.max_active_processes,
            max_children_reached: pool.max_children_reached,
            slow_requests: pool.slow_requests,
            memory_peak: pool.memory_peak,
        }
    }

    pub fn into_pool(self) -> FpmPoolMetrics {
        FpmPoolMetrics {
            pool: self.pool,
            fpm_pool: self.fpm_pool,
            process_manager: self.process_manager,
            up: self.up,
            start_time: self.start_time,
            start_since: self.start_since,
            accepted_conn: self.accepted_conn,
            listen_queue: self.listen_queue,
            max_listen_queue: self.max_listen_queue,
            listen_queue_len: self.listen_queue_len,
            idle_processes: self.idle_processes,
            active_processes: self.active_processes,
            total_processes: self.total_processes,
            max_active_processes: self.max_active_processes,
            max_children_reached: self.max_children_reached,
            slow_requests: self.slow_requests,
            memory_peak: self.memory_peak,
        }
    }
}

impl orbitive::metrics::OrbitTyped for FpmPoolOrbitMetrics {
    const KIND: u8 = 216;
    const RING_SPEC: orbitive::metrics::RingSpec =
        FPM_POOL_ORBIT_METRICS_RING_SPEC;
}

impl orbitive::metrics::OrbitMetricSnapshot for FpmPoolOrbitMetrics {
    const FAMILY: &'static str = "claviron_fpm_pool_metrics";

    fn node_id(&self) -> u16 {
        0
    }

    fn captured_at_unix_secs(&self) -> u64 {
        self.scraped_at
    }

    fn encode(&self) -> Result<Vec<u8>, String> {
        let mut out = Vec::with_capacity(128);
        out.extend_from_slice(&self.scraped_at.to_le_bytes());
        write_string(&mut out, &self.pool)?;
        write_string(&mut out, &self.fpm_pool)?;
        write_string(&mut out, &self.process_manager)?;
        out.push(u8::from(self.up));
        for value in [
            self.start_time,
            self.start_since,
            self.accepted_conn,
            self.listen_queue,
            self.max_listen_queue,
            self.listen_queue_len,
            self.idle_processes,
            self.active_processes,
            self.total_processes,
            self.max_active_processes,
            self.max_children_reached,
            self.slow_requests,
            self.memory_peak,
        ] {
            out.extend_from_slice(&value.to_le_bytes());
        }
        Ok(out)
    }

    fn decode(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() < 8 + 3 + 1 + 13 * 8 {
            return Err(format!(
                "bad fpm pool metrics payload len {}",
                bytes.len()
            ));
        }
        let mut offset = 0;
        let next_u64 = |offset: &mut usize| {
            let value = u64::from_le_bytes(
                bytes[*offset..*offset + 8].try_into().expect("u64 bytes"),
            );
            *offset += 8;
            value
        };
        let scraped_at = next_u64(&mut offset);
        let pool = read_string(bytes, &mut offset)?;
        let fpm_pool = read_string(bytes, &mut offset)?;
        let process_manager = read_string(bytes, &mut offset)?;
        if offset >= bytes.len() {
            return Err("missing fpm up byte".to_string());
        }
        let up = bytes[offset] != 0;
        offset += 1;
        if bytes.len() != offset + 13 * 8 {
            return Err(format!(
                "bad fpm pool metrics numeric len {}",
                bytes.len()
            ));
        }
        Ok(Self {
            scraped_at,
            pool,
            fpm_pool,
            process_manager,
            up,
            start_time: next_u64(&mut offset),
            start_since: next_u64(&mut offset),
            accepted_conn: next_u64(&mut offset),
            listen_queue: next_u64(&mut offset),
            max_listen_queue: next_u64(&mut offset),
            listen_queue_len: next_u64(&mut offset),
            idle_processes: next_u64(&mut offset),
            active_processes: next_u64(&mut offset),
            total_processes: next_u64(&mut offset),
            max_active_processes: next_u64(&mut offset),
            max_children_reached: next_u64(&mut offset),
            slow_requests: next_u64(&mut offset),
            memory_peak: next_u64(&mut offset),
        })
    }
}

impl orbitive::metrics::OrbitMetricKeyedSnapshot for FpmPoolOrbitMetrics {
    type Key = String;

    fn metric_key(&self) -> Self::Key {
        self.pool.clone()
    }
}

fn write_string(
    out: &mut Vec<u8>,
    value: &str,
) -> Result<(), String> {
    let len = u8::try_from(value.len())
        .map_err(|_| format!("fpm metrics string too long: {}", value.len()))?;
    out.push(len);
    out.extend_from_slice(value.as_bytes());
    Ok(())
}

fn read_string(
    bytes: &[u8],
    offset: &mut usize,
) -> Result<String, String> {
    if *offset >= bytes.len() {
        return Err("missing fpm metrics string length".to_string());
    }
    let len = usize::from(bytes[*offset]);
    *offset += 1;
    if bytes.len() < *offset + len {
        return Err("truncated fpm metrics string".to_string());
    }
    let value = std::str::from_utf8(&bytes[*offset..*offset + len])
        .map_err(|e| e.to_string())?
        .to_owned();
    *offset += len;
    Ok(value)
}

fn write_pool_gauge(
    output: &mut String,
    name: &str,
    help: &str,
    pools: &[FpmPoolMetrics],
    value: impl Fn(&FpmPoolMetrics) -> u64,
) {
    write_pool_metric(output, name, help, "gauge", pools, value);
}

fn write_pool_counter(
    output: &mut String,
    name: &str,
    help: &str,
    pools: &[FpmPoolMetrics],
    value: impl Fn(&FpmPoolMetrics) -> u64,
) {
    write_pool_metric(output, name, help, "counter", pools, value);
}

fn write_pool_metric(
    output: &mut String,
    name: &str,
    help: &str,
    kind: &str,
    pools: &[FpmPoolMetrics],
    value: impl Fn(&FpmPoolMetrics) -> u64,
) {
    use std::fmt::Write as FmtWrite;

    let _ = writeln!(output, "# HELP {name} {help}");
    let _ = writeln!(output, "# TYPE {name} {kind}");
    for pool in pools {
        if !pool.up && name != "claviron_fpm_up" {
            continue;
        }

        let _ = writeln!(
            output,
            "{}{{pool=\"{}\",fpm_pool=\"{}\"}} {}",
            name,
            escape_label(&pool.pool),
            escape_label(&pool.fpm_pool),
            value(pool)
        );
    }
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
#[path = "../../../tests/metrics/fpm/store.rs"]
mod tests;
