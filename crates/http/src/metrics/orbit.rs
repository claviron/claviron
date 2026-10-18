use super::{HttpMetricsSnapshot, ServerMetricsSnapshot};

// node id (2) + captured_at + eight counters, little-endian. Growing this
// changes kind 224's segment geometry, so a build that changes it needs
// `orbit clear clv --kind 224` between stop and start.
pub const HTTP_METRICS_RECORD_WIRE_LEN: usize = 74;
pub const HTTP_METRICS_RING_SPEC: orbitive::metrics::RingSpec =
    orbitive::metrics::RingSpec::new(
        orbitive::metrics::METRIC_SNAPSHOT_RING_CAPACITY,
        HTTP_METRICS_RECORD_WIRE_LEN,
    );
pub const SERVER_METRICS_RECORD_WIRE_LEN: usize = 58;
pub const SERVER_METRICS_RING_SPEC: orbitive::metrics::RingSpec =
    orbitive::metrics::RingSpec::new(
        orbitive::metrics::METRIC_SNAPSHOT_RING_CAPACITY,
        SERVER_METRICS_RECORD_WIRE_LEN,
    );

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HttpMetricsRecord {
    pub node_id: u16,
    pub captured_at: u64,
    pub snapshot: HttpMetricsSnapshot,
}

impl HttpMetricsRecord {
    pub fn new(
        node_id: u16,
        captured_at: u64,
        snapshot: HttpMetricsSnapshot,
    ) -> Self {
        Self { node_id, captured_at, snapshot }
    }
}

impl orbitive::metrics::OrbitTyped for HttpMetricsRecord {
    const KIND: u8 = 224;
    const RING_SPEC: orbitive::metrics::RingSpec = HTTP_METRICS_RING_SPEC;
}

impl orbitive::metrics::OrbitMetricSnapshot for HttpMetricsRecord {
    const FAMILY: &'static str = "claviron_http_worker_metrics";

    fn node_id(&self) -> u16 {
        self.node_id
    }

    fn captured_at_unix_secs(&self) -> u64 {
        self.captured_at
    }

    fn encode(&self) -> Result<Vec<u8>, String> {
        let mut out = Vec::with_capacity(HTTP_METRICS_RECORD_WIRE_LEN);
        out.extend_from_slice(&self.node_id.to_le_bytes());
        for value in [
            self.captured_at,
            self.snapshot.requests_in_flight,
            self.snapshot.requests_total,
            self.snapshot.sessions_open,
            self.snapshot.cache_hit_plus_total,
            self.snapshot.cache_hits_total,
            self.snapshot.cache_misses_total,
            self.snapshot.memory_cache_entries,
            self.snapshot.memory_cache_bytes,
        ] {
            out.extend_from_slice(&value.to_le_bytes());
        }
        Ok(out)
    }

    fn decode(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() != HTTP_METRICS_RECORD_WIRE_LEN {
            return Err(format!(
                "bad HTTP worker metrics payload len {}",
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
            snapshot: HttpMetricsSnapshot {
                requests_in_flight: next_u64(),
                requests_total: next_u64(),
                sessions_open: next_u64(),
                cache_hit_plus_total: next_u64(),
                cache_hits_total: next_u64(),
                cache_misses_total: next_u64(),
                memory_cache_entries: next_u64(),
                memory_cache_bytes: next_u64(),
            },
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServerMetricsRecord {
    pub node_id: u16,
    pub captured_at: u64,
    pub snapshot: ServerMetricsSnapshot,
}

impl ServerMetricsRecord {
    pub fn new(
        node_id: u16,
        captured_at: u64,
        snapshot: ServerMetricsSnapshot,
    ) -> Self {
        Self { node_id, captured_at, snapshot }
    }
}

impl orbitive::metrics::OrbitTyped for ServerMetricsRecord {
    const KIND: u8 = 225;
    const RING_SPEC: orbitive::metrics::RingSpec = SERVER_METRICS_RING_SPEC;
}

impl orbitive::metrics::OrbitMetricSnapshot for ServerMetricsRecord {
    const FAMILY: &'static str = "claviron_server_worker_metrics";

    fn node_id(&self) -> u16 {
        self.node_id
    }

    fn captured_at_unix_secs(&self) -> u64 {
        self.captured_at
    }

    fn encode(&self) -> Result<Vec<u8>, String> {
        let mut out = Vec::with_capacity(SERVER_METRICS_RECORD_WIRE_LEN);
        out.extend_from_slice(&self.node_id.to_le_bytes());
        for value in [
            self.captured_at,
            self.snapshot.tcp_accepts_total,
            self.snapshot.udp_accepts_total,
            self.snapshot.conn_in_flight,
            self.snapshot.conn_total,
            self.snapshot.tls_handshake_failures,
            self.snapshot.vhost_rejects,
        ] {
            out.extend_from_slice(&value.to_le_bytes());
        }
        Ok(out)
    }

    fn decode(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() != SERVER_METRICS_RECORD_WIRE_LEN {
            return Err(format!(
                "bad server metrics payload len {}",
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
            snapshot: ServerMetricsSnapshot {
                tcp_accepts_total: next_u64(),
                udp_accepts_total: next_u64(),
                conn_in_flight: next_u64(),
                conn_total: next_u64(),
                tls_handshake_failures: next_u64(),
                vhost_rejects: next_u64(),
            },
        })
    }
}

#[cfg(test)]
#[path = "../../tests/metrics/orbit.rs"]
mod tests;
