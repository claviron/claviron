use super::FcgiScalarMetricsSnapshot;

pub const FCGI_ORBIT_METRICS_WIRE_LEN: usize = 74;
pub const FCGI_ORBIT_METRICS_RING_SPEC: orbitive::metrics::RingSpec =
    orbitive::metrics::RingSpec::new(
        orbitive::metrics::METRIC_SNAPSHOT_RING_CAPACITY,
        FCGI_ORBIT_METRICS_WIRE_LEN,
    );

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FcgiOrbitMetrics {
    pub node_id: u16,
    pub captured_at: u64,
    pub live: u64,
    pub idle: u64,
    pub max_live: u64,
    pub requests_in_flight: u64,
    pub requests_total: u64,
    pub streams_in_flight: u64,
    pub streams_total: u64,
    pub stream_bytes_total: u64,
}

impl FcgiOrbitMetrics {
    pub fn from_snapshot(
        node_id: u16,
        metrics: &FcgiScalarMetricsSnapshot,
    ) -> Self {
        Self {
            node_id,
            captured_at: metrics.captured_at,
            live: metrics.live,
            idle: metrics.idle,
            max_live: metrics.max_live,
            requests_in_flight: metrics.requests.requests_in_flight,
            requests_total: metrics.requests.requests_total,
            streams_in_flight: metrics.requests.streams_in_flight,
            streams_total: metrics.requests.streams_total,
            stream_bytes_total: metrics.requests.stream_bytes_total,
        }
    }

    pub fn into_snapshot(self) -> FcgiScalarMetricsSnapshot {
        FcgiScalarMetricsSnapshot {
            captured_at: self.captured_at,
            live: self.live,
            idle: self.idle,
            max_live: self.max_live,
            requests: super::FcgiRequestMetricsSnapshot {
                requests_in_flight: self.requests_in_flight,
                requests_total: self.requests_total,
                streams_in_flight: self.streams_in_flight,
                streams_total: self.streams_total,
                stream_bytes_total: self.stream_bytes_total,
            },
        }
    }
}

impl orbitive::metrics::OrbitTyped for FcgiOrbitMetrics {
    const KIND: u8 = 215;
    const RING_SPEC: orbitive::metrics::RingSpec = FCGI_ORBIT_METRICS_RING_SPEC;
}

impl orbitive::metrics::OrbitMetricSnapshot for FcgiOrbitMetrics {
    const FAMILY: &'static str = "claviron_fcgi_worker_metrics";

    fn node_id(&self) -> u16 {
        self.node_id
    }

    fn captured_at_unix_secs(&self) -> u64 {
        self.captured_at
    }

    fn encode(&self) -> Result<Vec<u8>, String> {
        let mut out = Vec::with_capacity(FCGI_ORBIT_METRICS_WIRE_LEN);
        out.extend_from_slice(&self.node_id.to_le_bytes());
        for value in [
            self.captured_at,
            self.live,
            self.idle,
            self.max_live,
            self.requests_in_flight,
            self.requests_total,
            self.streams_in_flight,
            self.streams_total,
            self.stream_bytes_total,
        ] {
            out.extend_from_slice(&value.to_le_bytes());
        }
        Ok(out)
    }

    fn decode(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() != FCGI_ORBIT_METRICS_WIRE_LEN {
            return Err(format!(
                "bad fcgi worker metrics payload len {}",
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
            live: next_u64(),
            idle: next_u64(),
            max_live: next_u64(),
            requests_in_flight: next_u64(),
            requests_total: next_u64(),
            streams_in_flight: next_u64(),
            streams_total: next_u64(),
            stream_bytes_total: next_u64(),
        })
    }
}

#[cfg(test)]
#[path = "../../../tests/metrics/fcgi/orbit.rs"]
mod tests;
