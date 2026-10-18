use super::PhpEmbedMetricsSnapshot;

pub const PHP_EMBED_ORBIT_METRICS_WIRE_LEN: usize = 74;
pub const PHP_EMBED_ORBIT_METRICS_RING_SPEC: orbitive::metrics::RingSpec =
    orbitive::metrics::RingSpec::new(
        orbitive::metrics::METRIC_SNAPSHOT_RING_CAPACITY,
        PHP_EMBED_ORBIT_METRICS_WIRE_LEN,
    );

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PhpEmbedOrbitMetrics {
    pub node_id: u16,
    pub captured_at: u64,
    pub requests_in_flight: u64,
    pub requests_total: u64,
    pub bailouts_total: u64,
    pub request_bytes_total: u64,
    pub response_bytes_total: u64,
    pub threads_spawned_total: u64,
    pub threads_active: u64,
    pub engine_errors_total: u64,
}

impl PhpEmbedOrbitMetrics {
    pub fn from_snapshot(
        node_id: u16,
        captured_at: u64,
        metrics: &PhpEmbedMetricsSnapshot,
    ) -> Self {
        Self {
            node_id,
            captured_at,
            requests_in_flight: metrics.requests_in_flight,
            requests_total: metrics.requests_total,
            bailouts_total: metrics.bailouts_total,
            request_bytes_total: metrics.request_bytes_total,
            response_bytes_total: metrics.response_bytes_total,
            threads_spawned_total: metrics.threads_spawned_total,
            threads_active: metrics.threads_active,
            engine_errors_total: metrics.engine_errors_total,
        }
    }

    pub fn into_snapshot(self) -> PhpEmbedMetricsSnapshot {
        PhpEmbedMetricsSnapshot {
            requests_in_flight: self.requests_in_flight,
            requests_total: self.requests_total,
            bailouts_total: self.bailouts_total,
            request_bytes_total: self.request_bytes_total,
            response_bytes_total: self.response_bytes_total,
            threads_spawned_total: self.threads_spawned_total,
            threads_active: self.threads_active,
            engine_errors_total: self.engine_errors_total,
        }
    }
}

impl orbitive::metrics::OrbitTyped for PhpEmbedOrbitMetrics {
    const KIND: u8 = 214;
    const RING_SPEC: orbitive::metrics::RingSpec =
        PHP_EMBED_ORBIT_METRICS_RING_SPEC;
}

impl orbitive::metrics::OrbitMetricSnapshot for PhpEmbedOrbitMetrics {
    const FAMILY: &'static str = "claviron_ephp_worker_metrics";

    fn node_id(&self) -> u16 {
        self.node_id
    }

    fn captured_at_unix_secs(&self) -> u64 {
        self.captured_at
    }

    fn encode(&self) -> Result<Vec<u8>, String> {
        let mut out = Vec::with_capacity(PHP_EMBED_ORBIT_METRICS_WIRE_LEN);
        out.extend_from_slice(&self.node_id.to_le_bytes());
        for value in [
            self.captured_at,
            self.requests_in_flight,
            self.requests_total,
            self.bailouts_total,
            self.request_bytes_total,
            self.response_bytes_total,
            self.threads_spawned_total,
            self.threads_active,
            self.engine_errors_total,
        ] {
            out.extend_from_slice(&value.to_le_bytes());
        }
        Ok(out)
    }

    fn decode(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() != PHP_EMBED_ORBIT_METRICS_WIRE_LEN {
            return Err(format!(
                "bad php embed worker metrics payload len {}",
                bytes.len()
            ));
        }
        let node_id =
            u16::from_le_bytes(bytes[0..2].try_into().expect("node id bytes"));
        let mut offset = 2;
        let mut next_u64 = || {
            let value = u64::from_le_bytes(
                bytes[offset..offset + 8].try_into().expect("u64 bytes"),
            );
            offset += 8;
            value
        };
        Ok(Self {
            node_id,
            captured_at: next_u64(),
            requests_in_flight: next_u64(),
            requests_total: next_u64(),
            bailouts_total: next_u64(),
            request_bytes_total: next_u64(),
            response_bytes_total: next_u64(),
            threads_spawned_total: next_u64(),
            threads_active: next_u64(),
            engine_errors_total: next_u64(),
        })
    }
}

#[cfg(test)]
#[path = "../../tests/metrics/orbit.rs"]
mod tests;
