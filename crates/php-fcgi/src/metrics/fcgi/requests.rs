use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

pub static FCGI_REQUEST_METRICS: FcgiRequestMetrics = FcgiRequestMetrics::new();

pub struct FcgiRequestMetrics {
    requests_in_flight: AtomicU64,
    requests_total: AtomicU64,
    streams_in_flight: AtomicU64,
    streams_total: AtomicU64,
    stream_bytes_total: AtomicU64,
}

impl FcgiRequestMetrics {
    pub const fn new() -> Self {
        Self {
            requests_in_flight: AtomicU64::new(0),
            requests_total: AtomicU64::new(0),
            streams_in_flight: AtomicU64::new(0),
            streams_total: AtomicU64::new(0),
            stream_bytes_total: AtomicU64::new(0),
        }
    }

    #[inline]
    fn request_start(&self) {
        self.requests_in_flight.fetch_add(1, Ordering::Relaxed);
        self.requests_total.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    fn request_end(&self) {
        self.requests_in_flight.fetch_sub(1, Ordering::Relaxed);
    }

    #[inline]
    fn stream_start(&self) {
        self.streams_in_flight.fetch_add(1, Ordering::Relaxed);
        self.streams_total.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    fn stream_end(&self) {
        self.streams_in_flight.fetch_sub(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn add_stream_bytes(
        &self,
        bytes: u64,
    ) {
        self.stream_bytes_total.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> FcgiRequestMetricsSnapshot {
        FcgiRequestMetricsSnapshot {
            requests_in_flight: self.requests_in_flight.load(Ordering::Relaxed),
            requests_total: self.requests_total.load(Ordering::Relaxed),
            streams_in_flight: self.streams_in_flight.load(Ordering::Relaxed),
            streams_total: self.streams_total.load(Ordering::Relaxed),
            stream_bytes_total: self.stream_bytes_total.load(Ordering::Relaxed),
        }
    }

    pub fn write_prometheus(
        &self,
        output: &mut String,
    ) {
        self.snapshot().write_prometheus(output);
    }
}

impl Default for FcgiRequestMetrics {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct FcgiRequestMetricsSnapshot {
    pub requests_in_flight: u64,
    pub requests_total: u64,
    pub streams_in_flight: u64,
    pub streams_total: u64,
    pub stream_bytes_total: u64,
}

impl FcgiRequestMetricsSnapshot {
    pub fn merge(
        &mut self,
        other: &Self,
    ) {
        self.requests_in_flight += other.requests_in_flight;
        self.requests_total += other.requests_total;
        self.streams_in_flight += other.streams_in_flight;
        self.streams_total += other.streams_total;
        self.stream_bytes_total += other.stream_bytes_total;
    }

    pub fn write_prometheus(
        &self,
        output: &mut String,
    ) {
        use std::fmt::Write as FmtWrite;

        write_metric(
            output,
            "claviron_fcgi_requests_inflight",
            "gauge",
            "Buffered FCGI requests currently in progress",
            self.requests_in_flight,
        );
        write_metric(
            output,
            "claviron_fcgi_requests_total",
            "counter",
            "Total buffered FCGI requests",
            self.requests_total,
        );
        write_metric(
            output,
            "claviron_fcgi_streams_inflight",
            "gauge",
            "Streaming FCGI bodies currently flowing",
            self.streams_in_flight,
        );
        write_metric(
            output,
            "claviron_fcgi_streams_total",
            "counter",
            "Total streaming FCGI bodies started",
            self.streams_total,
        );
        write_metric(
            output,
            "claviron_fcgi_stream_bytes_total",
            "counter",
            "Total bytes pumped through streaming FCGI bodies",
            self.stream_bytes_total,
        );

        fn write_metric(
            output: &mut String,
            name: &str,
            kind: &str,
            help: &str,
            value: u64,
        ) {
            let _ = writeln!(output, "# HELP {name} {help}");
            let _ = writeln!(output, "# TYPE {name} {kind}");
            let _ = writeln!(output, "{name} {value}");
        }
    }
}

/// Tracks one FCGI execution for the lifetime of its pump task. Every
/// execution streams its reply now, so it is counted as a request and as
/// a stream both; the two gauges were apart only while a buffered path
/// existed.
pub struct FcgiStreamGuard;

impl FcgiStreamGuard {
    pub fn enter() -> Self {
        FCGI_REQUEST_METRICS.request_start();
        FCGI_REQUEST_METRICS.stream_start();
        Self
    }
}

impl Drop for FcgiStreamGuard {
    fn drop(&mut self) {
        FCGI_REQUEST_METRICS.stream_end();
        FCGI_REQUEST_METRICS.request_end();
    }
}
