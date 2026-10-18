use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

/// RAII guard to ensure conn_close() is called even if connection is dropped/cancelled
pub struct MetricsGuard;

impl Drop for MetricsGuard {
    fn drop(&mut self) {
        SERVER_METRICS.conn_close();
    }
}

/// Global server metrics instance
pub static SERVER_METRICS: ServerMetrics = ServerMetrics::new();

/// Multi-layer server metrics tracking transport, connection, and handshake events.
///
/// **Transport Layer** (raw socket accepts):
/// - `tcp_accepts_total`: Total TCP socket accepts (before TLS handshake)
/// - `udp_accepts_total`: Total UDP/QUIC accepts (before QUIC handshake)
///
/// **Connection Layer** (post-handshake):
/// - `conn_in_flight`: Currently active connections (after successful handshake)
/// - `conn_total`: Cumulative successful connections (after successful handshake)
///
/// **Handshake Failures**:
/// - `tls_handshake_failures`: Failed TLS handshakes (timeouts, errors, invalid certs)
/// - `quic_handshake_failures`: Failed QUIC handshakes (timeouts, errors)
pub struct ServerMetrics {
    // Transport-level counters (raw accepts)
    pub tcp_accepts_total: AtomicU64,
    pub udp_accepts_total: AtomicU64,

    // Connection-level counters (post-handshake)
    pub conn_in_flight: AtomicU64,
    pub conn_total: AtomicU64,

    // Handshake failure counters
    pub tls_handshake_failures: AtomicU64,
    pub vhost_rejects: AtomicU64,
}

impl Default for ServerMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl ServerMetrics {
    pub const fn new() -> Self {
        Self {
            tcp_accepts_total: AtomicU64::new(0),
            udp_accepts_total: AtomicU64::new(0),
            conn_in_flight: AtomicU64::new(0),
            conn_total: AtomicU64::new(0),
            tls_handshake_failures: AtomicU64::new(0),
            vhost_rejects: AtomicU64::new(0),
        }
    }

    /// Record a TCP socket accept (before TLS handshake)
    #[inline]
    pub fn tcp_accept(&self) {
        self.tcp_accepts_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a UDP/QUIC socket accept (before QUIC handshake)
    #[inline]
    pub fn udp_accept(&self) {
        self.udp_accepts_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a TLS handshake failure
    #[inline]
    pub fn tls_failed(&self) {
        self.tls_handshake_failures.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a Vhost resolution failure
    #[inline]
    pub fn vhost_rejected(&self) {
        self.vhost_rejects.fetch_add(1, Ordering::Relaxed);
    }

    /// Increase active connections and total opened counters.
    /// Returns a guard that will decrement conn_in_flight when dropped.
    #[inline]
    pub fn conn_open(&self) -> MetricsGuard {
        self.conn_in_flight.fetch_add(1, Ordering::Relaxed);
        self.conn_total.fetch_add(1, Ordering::Relaxed);
        MetricsGuard
    }

    /// Decrease active connections counter.
    #[inline]
    pub fn conn_close(&self) {
        self.conn_in_flight.fetch_sub(1, Ordering::Relaxed);
    }

    /// Get number of active (in-flight) connections.
    #[inline]
    pub fn conn_active(&self) -> u64 {
        self.conn_in_flight.load(Ordering::Relaxed)
    }

    /// Get cumulative number of opened connections.
    #[inline]
    pub fn conn_total(&self) -> u64 {
        self.conn_total.load(Ordering::Relaxed)
    }

    /// Get total TCP accepts.
    #[inline]
    pub fn tcp_accepts(&self) -> u64 {
        self.tcp_accepts_total.load(Ordering::Relaxed)
    }

    /// Get total UDP accepts.
    #[inline]
    pub fn udp_accepts(&self) -> u64 {
        self.udp_accepts_total.load(Ordering::Relaxed)
    }

    /// Get TLS handshake failures.
    #[inline]
    pub fn tls_failures(&self) -> u64 {
        self.tls_handshake_failures.load(Ordering::Relaxed)
    }

    /// Capture a serializable snapshot of the current counters.
    pub fn snapshot(&self) -> ServerMetricsSnapshot {
        ServerMetricsSnapshot {
            tcp_accepts_total: self.tcp_accepts(),
            udp_accepts_total: self.udp_accepts(),
            conn_in_flight: self.conn_active(),
            conn_total: self.conn_total(),
            tls_handshake_failures: self.tls_failures(),
            vhost_rejects: self.vhost_rejects.load(Ordering::Relaxed),
        }
    }

    /// Append the transport/connection/handshake metrics in Prometheus
    /// text format to `output`. Convenience wrapper around `snapshot()`.
    pub fn write_prometheus(
        &self,
        output: &mut String,
    ) {
        self.snapshot().write_prometheus(output);
    }
}

/// Serializable point-in-time view of `ServerMetrics`. It is the canonical
/// source for local formatting and the HTTP-owned Orbit fleet snapshot.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize,
)]
pub struct ServerMetricsSnapshot {
    pub tcp_accepts_total: u64,
    pub udp_accepts_total: u64,
    pub conn_in_flight: u64,
    pub conn_total: u64,
    pub tls_handshake_failures: u64,
    pub vhost_rejects: u64,
}

impl ServerMetricsSnapshot {
    /// Capture the current globals into a snapshot.
    pub fn capture() -> Self {
        SERVER_METRICS.snapshot()
    }

    /// Field-wise add another snapshot's values into this one. Used by
    /// the HTTP Orbit family to aggregate worker snapshots.
    pub fn merge(
        &mut self,
        other: &Self,
    ) {
        self.tcp_accepts_total += other.tcp_accepts_total;
        self.udp_accepts_total += other.udp_accepts_total;
        self.conn_in_flight += other.conn_in_flight;
        self.conn_total += other.conn_total;
        self.tls_handshake_failures += other.tls_handshake_failures;
        self.vhost_rejects += other.vhost_rejects;
    }

    /// Append this snapshot in Prometheus text format to `output`.
    pub fn write_prometheus(
        &self,
        output: &mut String,
    ) {
        use std::fmt::Write as FmtWrite;

        let _ = writeln!(
            output,
            "# HELP claviron_tcp_accepts_total Total TCP socket accepts"
        );
        let _ = writeln!(output, "# TYPE claviron_tcp_accepts_total counter");
        let _ = writeln!(
            output,
            "claviron_tcp_accepts_total {}",
            self.tcp_accepts_total
        );
        let _ = writeln!(output);

        let _ = writeln!(
            output,
            "# HELP claviron_udp_accepts_total Total UDP/QUIC socket accepts"
        );
        let _ = writeln!(output, "# TYPE claviron_udp_accepts_total counter");
        let _ = writeln!(
            output,
            "claviron_udp_accepts_total {}",
            self.udp_accepts_total
        );
        let _ = writeln!(output);

        let _ = writeln!(
            output,
            "# HELP claviron_connections_inflight Active connections (post-handshake)"
        );
        let _ = writeln!(output, "# TYPE claviron_connections_inflight gauge");
        let _ = writeln!(
            output,
            "claviron_connections_inflight {}",
            self.conn_in_flight
        );
        let _ = writeln!(output);

        let _ = writeln!(
            output,
            "# HELP claviron_connections_total Total successful connections (post-handshake)"
        );
        let _ = writeln!(output, "# TYPE claviron_connections_total counter");
        let _ =
            writeln!(output, "claviron_connections_total {}", self.conn_total);
        let _ = writeln!(output);

        let _ = writeln!(
            output,
            "# HELP claviron_tls_handshake_failures_total Total TLS handshake failures"
        );
        let _ = writeln!(
            output,
            "# TYPE claviron_tls_handshake_failures_total counter"
        );
        let _ = writeln!(
            output,
            "claviron_tls_handshake_failures_total {}",
            self.tls_handshake_failures
        );
        let _ = writeln!(output);

        let _ = writeln!(
            output,
            "# HELP claviron_vhost_rejects_total Total vhost resolution failures (missing/invalid/unknown SNI)"
        );
        let _ = writeln!(output, "# TYPE claviron_vhost_rejects_total counter");
        let _ = writeln!(
            output,
            "claviron_vhost_rejects_total {}",
            self.vhost_rejects
        );
        let _ = writeln!(output);
    }
}
