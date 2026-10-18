// ---------------------------------------------------------------------------
// Process-wide counters for the in-process E-PHP SAPI runtime.
//
// `PHP_EMBED_METRICS` exposes atomic counters bumped from RAII guards in
// this module's RAII guards. `PhpEmbedMetricsSnapshot` is the POD encoded
// into the owner-local Orbit family for fleet-wide aggregation.
// ---------------------------------------------------------------------------

use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

pub static PHP_EMBED_METRICS: PhpEmbedMetrics = PhpEmbedMetrics::new();

pub struct PhpEmbedMetrics {
    /// Currently-running PHP embed requests (incremented at SAPI request
    /// startup, decremented at completion). Reflects "threads inside
    /// libphp.so right now"; under ZTS this is a meaningful concurrency
    /// signal.
    pub requests_in_flight: AtomicU64,
    /// Lifetime count of completed PHP embed requests. Counter — only
    /// goes up, never resets.
    pub requests_total: AtomicU64,
    /// Lifetime count of Zend bailouts (catastrophic execution failures
    /// — fatal PHP errors, longjmp escapes). Counter.
    pub bailouts_total: AtomicU64,
    /// Total request body bytes fed into PHP (sum of `Content-Length`
    /// across all served requests). Counter — useful for understanding
    /// embed throughput without per-pool/per-vhost slicing.
    pub request_bytes_total: AtomicU64,
    /// Total response body bytes emitted by PHP scripts. Counter.
    pub response_bytes_total: AtomicU64,
    /// Lifetime count of SAPI threads spawned in this worker. Counter
    /// (increments on each `SapiThreadPool::spawn` per thread). Steady
    /// in fixed-pool mode; jumps on rotation / dynamic scale events,
    /// which makes it a useful "did the pool just churn?" signal in
    /// dashboards even before explicit rotation telemetry lands.
    pub threads_spawned_total: AtomicU64,
    /// Currently-alive SAPI threads (gauge). Increments on spawn,
    /// decrements when a worker thread exits its `blocking_recv` loop.
    /// In a healthy fixed pool this matches `php.yaml::sapi.pool.min_idle`;
    /// during drain it walks down to zero.
    pub threads_active: AtomicU64,
    /// Lifetime count of engine-side errors that prevented a request
    /// from being served (pool busy → 502, drain rejection, body too
    /// large, request startup failure, lock poisoning, etc.).
    /// Distinct from `bailouts_total` (PHP-side fatals) and from PHP
    /// scripts returning 5xx (those still count as `requests_total`
    /// because the engine served them). Counter.
    pub engine_errors_total: AtomicU64,
}

impl PhpEmbedMetrics {
    pub const fn new() -> Self {
        Self {
            requests_in_flight: AtomicU64::new(0),
            requests_total: AtomicU64::new(0),
            bailouts_total: AtomicU64::new(0),
            request_bytes_total: AtomicU64::new(0),
            response_bytes_total: AtomicU64::new(0),
            threads_spawned_total: AtomicU64::new(0),
            threads_active: AtomicU64::new(0),
            engine_errors_total: AtomicU64::new(0),
        }
    }

    #[inline]
    pub fn request_start(&self) {
        self.requests_in_flight.fetch_add(1, Ordering::Relaxed);
        self.requests_total.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn request_end(&self) {
        self.requests_in_flight.fetch_sub(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn bailout(&self) {
        self.bailouts_total.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn add_request_bytes(
        &self,
        n: u64,
    ) {
        self.request_bytes_total.fetch_add(n, Ordering::Relaxed);
    }

    #[inline]
    pub fn add_response_bytes(
        &self,
        n: u64,
    ) {
        self.response_bytes_total.fetch_add(n, Ordering::Relaxed);
    }

    /// Bump `threads_spawned_total` and `threads_active` together;
    /// called from `SapiThreadPool::spawn` per worker thread fork.
    #[inline]
    pub fn thread_spawned(&self) {
        self.threads_spawned_total.fetch_add(1, Ordering::Relaxed);
        self.threads_active.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrement `threads_active`; called when a worker thread exits
    /// its `blocking_recv` loop (drain / panic / channel close).
    #[inline]
    pub fn thread_exited(&self) {
        self.threads_active.fetch_sub(1, Ordering::Relaxed);
    }

    /// Bump `engine_errors_total`; called whenever an engine-level
    /// failure prevents a request from being served (pool exhaustion,
    /// drain rejection, body too large, etc.).
    #[inline]
    pub fn engine_error(&self) {
        self.engine_errors_total.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn requests_in_flight(&self) -> u64 {
        self.requests_in_flight.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn requests_total(&self) -> u64 {
        self.requests_total.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn bailouts_total(&self) -> u64 {
        self.bailouts_total.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn request_bytes_total(&self) -> u64 {
        self.request_bytes_total.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn response_bytes_total(&self) -> u64 {
        self.response_bytes_total.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn threads_spawned_total(&self) -> u64 {
        self.threads_spawned_total.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn threads_active(&self) -> u64 {
        self.threads_active.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn engine_errors_total(&self) -> u64 {
        self.engine_errors_total.load(Ordering::Relaxed)
    }

    /// Capture a serializable snapshot of the current counters.
    pub fn snapshot(&self) -> PhpEmbedMetricsSnapshot {
        PhpEmbedMetricsSnapshot {
            requests_in_flight: self.requests_in_flight(),
            requests_total: self.requests_total(),
            bailouts_total: self.bailouts_total(),
            request_bytes_total: self.request_bytes_total(),
            response_bytes_total: self.response_bytes_total(),
            threads_spawned_total: self.threads_spawned_total(),
            threads_active: self.threads_active(),
            engine_errors_total: self.engine_errors_total(),
        }
    }

    pub fn write_prometheus(
        &self,
        output: &mut String,
    ) {
        self.snapshot().write_prometheus(output);
    }
}

impl Default for PhpEmbedMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Serializable point-in-time view of `PhpEmbedMetrics`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PhpEmbedMetricsSnapshot {
    pub requests_in_flight: u64,
    pub requests_total: u64,
    pub bailouts_total: u64,
    pub request_bytes_total: u64,
    pub response_bytes_total: u64,
    pub threads_spawned_total: u64,
    pub threads_active: u64,
    pub engine_errors_total: u64,
}

impl PhpEmbedMetricsSnapshot {
    /// Capture the current globals into a snapshot.
    pub fn capture() -> Self {
        PHP_EMBED_METRICS.snapshot()
    }

    /// Field-wise add another worker snapshot. Each worker has its own
    /// libphp.so, so fleet-wide counts are simple sums.
    pub fn merge(
        &mut self,
        other: &Self,
    ) {
        self.requests_in_flight += other.requests_in_flight;
        self.requests_total += other.requests_total;
        self.bailouts_total += other.bailouts_total;
        self.request_bytes_total += other.request_bytes_total;
        self.response_bytes_total += other.response_bytes_total;
        self.threads_spawned_total += other.threads_spawned_total;
        self.threads_active += other.threads_active;
        self.engine_errors_total += other.engine_errors_total;
    }

    /// Append this snapshot in Prometheus text format to `output`.
    pub fn write_prometheus(
        &self,
        output: &mut String,
    ) {
        use std::fmt::Write as _;

        let _ = writeln!(
            output,
            "# HELP claviron_ephp_requests_inflight Currently-running E-PHP requests"
        );
        let _ =
            writeln!(output, "# TYPE claviron_ephp_requests_inflight gauge");
        let _ = writeln!(
            output,
            "claviron_ephp_requests_inflight {}",
            self.requests_in_flight
        );

        let _ = writeln!(
            output,
            "# HELP claviron_ephp_requests_total Total E-PHP requests served"
        );
        let _ = writeln!(output, "# TYPE claviron_ephp_requests_total counter");
        let _ = writeln!(
            output,
            "claviron_ephp_requests_total {}",
            self.requests_total
        );

        let _ = writeln!(
            output,
            "# HELP claviron_ephp_bailouts_total Zend bailouts during E-PHP execution"
        );
        let _ = writeln!(output, "# TYPE claviron_ephp_bailouts_total counter");
        let _ = writeln!(
            output,
            "claviron_ephp_bailouts_total {}",
            self.bailouts_total
        );

        let _ = writeln!(
            output,
            "# HELP claviron_ephp_request_bytes_total Total request body bytes fed to E-PHP"
        );
        let _ = writeln!(
            output,
            "# TYPE claviron_ephp_request_bytes_total counter"
        );
        let _ = writeln!(
            output,
            "claviron_ephp_request_bytes_total {}",
            self.request_bytes_total
        );

        let _ = writeln!(
            output,
            "# HELP claviron_ephp_response_bytes_total Total response body bytes emitted by E-PHP"
        );
        let _ = writeln!(
            output,
            "# TYPE claviron_ephp_response_bytes_total counter"
        );
        let _ = writeln!(
            output,
            "claviron_ephp_response_bytes_total {}",
            self.response_bytes_total
        );

        let _ = writeln!(
            output,
            "# HELP claviron_ephp_threads_spawned_total Total E-PHP SAPI threads spawned (lifetime)"
        );
        let _ = writeln!(
            output,
            "# TYPE claviron_ephp_threads_spawned_total counter"
        );
        let _ = writeln!(
            output,
            "claviron_ephp_threads_spawned_total {}",
            self.threads_spawned_total
        );

        let _ = writeln!(
            output,
            "# HELP claviron_ephp_threads_active Currently-alive E-PHP SAPI threads"
        );
        let _ = writeln!(output, "# TYPE claviron_ephp_threads_active gauge");
        let _ = writeln!(
            output,
            "claviron_ephp_threads_active {}",
            self.threads_active
        );

        let _ = writeln!(
            output,
            "# HELP claviron_ephp_engine_errors_total E-PHP engine errors blocking request service"
        );
        let _ = writeln!(
            output,
            "# TYPE claviron_ephp_engine_errors_total counter"
        );
        let _ = writeln!(
            output,
            "claviron_ephp_engine_errors_total {}",
            self.engine_errors_total
        );
    }
}
