//! Owns one H2 stream capacity token and in-flight accounting.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::sync::OwnedSemaphorePermit;

use crate::origin::PoolGeneration;

pub struct H2Permit {
    pub conn_id: u64,
    pub generation: PoolGeneration,
    _stream: OwnedSemaphorePermit,
    in_flight: Arc<AtomicUsize>,
    traffic: Option<crate::proxy::balance::least_connections::Traffic>,
}

impl H2Permit {
    pub(crate) fn with_traffic(
        mut self,
        traffic: crate::proxy::balance::least_connections::Traffic,
    ) -> Self {
        self.traffic = Some(traffic);
        self
    }

    pub fn new(
        conn_id: u64,
        generation: PoolGeneration,
        stream: OwnedSemaphorePermit,
        in_flight: Arc<AtomicUsize>,
    ) -> Self {
        in_flight.fetch_add(1, Ordering::AcqRel);
        Self { conn_id, generation, _stream: stream, in_flight, traffic: None }
    }
}

impl Drop for H2Permit {
    fn drop(&mut self) {
        self.in_flight.fetch_sub(1, Ordering::AcqRel);
    }
}

impl std::fmt::Debug for H2Permit {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        f.debug_struct("H2Permit")
            .field("conn_id", &self.conn_id)
            .field("generation", &self.generation)
            .finish_non_exhaustive()
    }
}
