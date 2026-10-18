//! The upstream-owned fleet wire record: identity plus the shared Scalars body.
use super::Scalars;

// ABI: u16 node + u64 timestamp + eight u64 scalar fields, no padding.
// A different shape takes a new kind, not an operator-selected wire length.
pub const UPSTREAM_ORBIT_METRICS_WIRE_LEN: usize = 74;
pub const UPSTREAM_ORBIT_METRICS_RING_SPEC: orbitive::metrics::RingSpec =
    orbitive::metrics::RingSpec::new(
        orbitive::metrics::METRIC_SNAPSHOT_RING_CAPACITY,
        UPSTREAM_ORBIT_METRICS_WIRE_LEN,
    );

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UpstreamOrbitMetrics {
    pub node_id: u16,
    pub captured_at: u64,
    pub scalars: Scalars,
}
impl UpstreamOrbitMetrics {
    pub fn from_snapshot(
        node_id: u16,
        captured_at: u64,
        scalars: &Scalars,
    ) -> Self {
        Self { node_id, captured_at, scalars: *scalars }
    }
    pub fn into_snapshot(self) -> Scalars {
        self.scalars
    }
}
impl orbitive::metrics::OrbitTyped for UpstreamOrbitMetrics {
    const KIND: u8 = 213;
    const RING_SPEC: orbitive::metrics::RingSpec =
        UPSTREAM_ORBIT_METRICS_RING_SPEC;
}
impl orbitive::metrics::OrbitMetricSnapshot for UpstreamOrbitMetrics {
    const FAMILY: &'static str = "claviron_upstream_worker_metrics";
    fn node_id(&self) -> u16 {
        self.node_id
    }
    fn captured_at_unix_secs(&self) -> u64 {
        self.captured_at
    }
    fn encode(&self) -> Result<Vec<u8>, String> {
        let mut out = Vec::with_capacity(UPSTREAM_ORBIT_METRICS_WIRE_LEN);
        out.extend_from_slice(&self.node_id.to_le_bytes());
        for value in [
            self.captured_at,
            self.scalars.requests_total,
            self.scalars.request_errors_total,
            self.scalars.probes_total,
            self.scalars.probe_failures_total,
            self.scalars.state_transitions_total,
            self.scalars.configured_live_capacity,
            self.scalars.retries_total,
            self.scalars.ejections_total,
        ] {
            out.extend_from_slice(&value.to_le_bytes());
        }
        Ok(out)
    }
    fn decode(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() != UPSTREAM_ORBIT_METRICS_WIRE_LEN {
            return Err(format!(
                "bad upstream worker metrics payload len {}",
                bytes.len()
            ));
        }
        let node_id = u16::from_le_bytes(
            bytes[..2].try_into().expect("validated node bytes"),
        );
        let mut values = bytes[2..]
            .as_chunks::<8>()
            .0
            .iter()
            .map(|v| u64::from_le_bytes(*v));
        let mut next = || values.next().expect("validated scalar count");
        let captured_at = next();
        let scalars = Scalars {
            requests_total: next(),
            request_errors_total: next(),
            probes_total: next(),
            probe_failures_total: next(),
            state_transitions_total: next(),
            configured_live_capacity: next(),
            retries_total: next(),
            ejections_total: next(),
        };
        Ok(Self { node_id, captured_at, scalars })
    }
}
