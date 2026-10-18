use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

/// Global HTTP metrics instance
pub static HTTP_METRICS: HttpMetrics = HttpMetrics::new();

/// Global metrics for HTTP requests
pub struct HttpMetrics {
    pub cache_hit_plus_total: AtomicU64,
    pub cache_hits_total: AtomicU64,
    pub cache_misses_total: AtomicU64,
    pub memory_cache_entries: AtomicU64,
    pub memory_cache_bytes: AtomicU64,
}

impl HttpMetrics {
    pub const fn new() -> Self {
        Self {
            cache_hit_plus_total: AtomicU64::new(0),
            cache_hits_total: AtomicU64::new(0),
            cache_misses_total: AtomicU64::new(0),
            memory_cache_entries: AtomicU64::new(0),
            memory_cache_bytes: AtomicU64::new(0),
        }
    }

    /// Reported, not owned: the request counters are live state and live in
    /// [`claviron_runtime::activity`]. This layer reads them so a scrape sees the same
    /// numbers the watchdog acts on.
    #[inline]
    pub fn in_flight(&self) -> u64 {
        claviron_runtime::activity::ACTIVITY.in_flight()
    }

    #[inline]
    pub fn total(&self) -> u64 {
        claviron_runtime::activity::ACTIVITY.total()
    }

    #[inline]
    pub fn sessions(&self) -> u64 {
        claviron_runtime::activity::ACTIVITY.sessions()
    }

    #[inline]
    pub fn cache_hit_plus(&self) {
        self.cache_hit_plus_total.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn cache_hit(&self) {
        self.cache_hits_total.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn cache_miss(&self) {
        self.cache_misses_total.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn set_memory_cache(
        &self,
        entries: u64,
        bytes: u64,
    ) {
        self.memory_cache_entries.store(entries, Ordering::Relaxed);
        self.memory_cache_bytes.store(bytes, Ordering::Relaxed);
    }

    /// Capture a serializable snapshot of the current counters.
    pub fn snapshot(&self) -> HttpMetricsSnapshot {
        HttpMetricsSnapshot {
            requests_in_flight: self.in_flight(),
            requests_total: self.total(),
            sessions_open: self.sessions(),
            cache_hit_plus_total: self
                .cache_hit_plus_total
                .load(Ordering::Relaxed),
            cache_hits_total: self.cache_hits_total.load(Ordering::Relaxed),
            cache_misses_total: self.cache_misses_total.load(Ordering::Relaxed),
            memory_cache_entries: self
                .memory_cache_entries
                .load(Ordering::Relaxed),
            memory_cache_bytes: self.memory_cache_bytes.load(Ordering::Relaxed),
        }
    }

    /// Append the request-layer metrics in Prometheus text format to
    /// `output`. Convenience wrapper around `snapshot()`.
    pub fn write_prometheus(
        &self,
        output: &mut String,
    ) {
        self.snapshot().write_prometheus(output);
    }
}

/// Serializable point-in-time view of the HTTP counters.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize,
)]
pub struct HttpMetricsSnapshot {
    pub requests_in_flight: u64,
    pub requests_total: u64,
    /// WebSockets, CONNECT tunnels and WebTransport sessions open now. Not
    /// requests: nothing expects them to end.
    pub sessions_open: u64,
    pub cache_hit_plus_total: u64,
    pub cache_hits_total: u64,
    pub cache_misses_total: u64,
    pub memory_cache_entries: u64,
    pub memory_cache_bytes: u64,
}

impl HttpMetricsSnapshot {
    /// Capture the current globals into a snapshot.
    pub fn capture() -> Self {
        HTTP_METRICS.snapshot()
    }

    /// Field-wise add another snapshot's values into this one.
    pub fn merge(
        &mut self,
        other: &Self,
    ) {
        self.requests_in_flight += other.requests_in_flight;
        self.requests_total += other.requests_total;
        self.sessions_open += other.sessions_open;
        self.cache_hit_plus_total += other.cache_hit_plus_total;
        self.cache_hits_total += other.cache_hits_total;
        self.cache_misses_total += other.cache_misses_total;
        self.memory_cache_entries += other.memory_cache_entries;
        self.memory_cache_bytes += other.memory_cache_bytes;
    }

    /// Append this snapshot in Prometheus text format to `output`.
    pub fn write_prometheus(
        &self,
        output: &mut String,
    ) {
        use std::fmt::Write as FmtWrite;

        let _ = writeln!(
            output,
            "# HELP claviron_http_requests_inflight HTTP requests currently in flight"
        );
        let _ =
            writeln!(output, "# TYPE claviron_http_requests_inflight gauge");
        let _ = writeln!(
            output,
            "claviron_http_requests_inflight {}",
            self.requests_in_flight
        );
        let _ = writeln!(output);

        let _ = writeln!(
            output,
            "# HELP claviron_http_requests_total Total HTTP requests processed"
        );
        let _ = writeln!(output, "# TYPE claviron_http_requests_total counter");
        let _ = writeln!(
            output,
            "claviron_http_requests_total {}",
            self.requests_total
        );
        let _ = writeln!(output);

        let _ = writeln!(
            output,
            "# HELP claviron_http_sessions_open Long-lived sessions open now (WebSocket, CONNECT, WebTransport)"
        );
        let _ = writeln!(output, "# TYPE claviron_http_sessions_open gauge");
        let _ = writeln!(
            output,
            "claviron_http_sessions_open {}",
            self.sessions_open
        );
        let _ = writeln!(output);

        let _ = writeln!(
            output,
            "# HELP claviron_http_cache_hit_plus_total Static responses served by the metadata-free cache fast path"
        );
        let _ = writeln!(
            output,
            "# TYPE claviron_http_cache_hit_plus_total counter"
        );
        let _ = writeln!(
            output,
            "claviron_http_cache_hit_plus_total {}",
            self.cache_hit_plus_total
        );
        let _ = writeln!(output);

        let _ = writeln!(
            output,
            "# HELP claviron_http_cache_hits_total Static responses served from cache after file validation"
        );
        let _ =
            writeln!(output, "# TYPE claviron_http_cache_hits_total counter");
        let _ = writeln!(
            output,
            "claviron_http_cache_hits_total {}",
            self.cache_hits_total
        );
        let _ = writeln!(output);

        let _ = writeln!(
            output,
            "# HELP claviron_http_cache_misses_total Static responses not served from cache"
        );
        let _ =
            writeln!(output, "# TYPE claviron_http_cache_misses_total counter");
        let _ = writeln!(
            output,
            "claviron_http_cache_misses_total {}",
            self.cache_misses_total
        );
        let _ = writeln!(output);

        let _ = writeln!(
            output,
            "# HELP claviron_http_memory_cache_entries Static response entries held in memory"
        );
        let _ =
            writeln!(output, "# TYPE claviron_http_memory_cache_entries gauge");
        let _ = writeln!(
            output,
            "claviron_http_memory_cache_entries {}",
            self.memory_cache_entries
        );
        let _ = writeln!(output);

        let _ = writeln!(
            output,
            "# HELP claviron_http_memory_cache_bytes Bytes reserved by the static response memory cache"
        );
        let _ =
            writeln!(output, "# TYPE claviron_http_memory_cache_bytes gauge");
        let _ = writeln!(
            output,
            "claviron_http_memory_cache_bytes {}",
            self.memory_cache_bytes
        );
        let _ = writeln!(output);
    }
}

impl Default for HttpMetrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
#[path = "../../tests/metrics/http.rs"]
mod tests;
