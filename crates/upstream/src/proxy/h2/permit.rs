//! One stream reservation, retained through both response and request-body IO.
use std::sync::Arc;
use std::sync::atomic::Ordering;

use super::conn::H2Connection;
use crate::origin::PoolGeneration;

pub struct H2Permit {
    pub conn_id: u64,
    pub generation: PoolGeneration,
    owner: Option<Owner>,
}
enum Owner {
    Exclusive(StreamLease),
    Shared(Arc<StreamLease>),
}
struct StreamLease {
    connection: Arc<H2Connection>,
    traffic: Option<crate::proxy::balance::least_connections::Traffic>,
}
pub(crate) struct H2WriterLease {
    _lease: Arc<StreamLease>,
}
impl H2Permit {
    pub(super) fn new(connection: Arc<H2Connection>) -> Self {
        if let Some(pool) = connection.pool.upgrade() {
            pool.in_flight.fetch_add(1, Ordering::AcqRel);
        }
        Self {
            conn_id: connection.id,
            generation: connection.generation,
            owner: Some(Owner::Exclusive(StreamLease {
                connection,
                traffic: None,
            })),
        }
    }
    pub(crate) fn with_traffic(
        mut self,
        traffic: crate::proxy::balance::least_connections::Traffic,
    ) -> Self {
        let Some(Owner::Exclusive(lease)) = &mut self.owner else {
            unreachable!("traffic attaches before sharing a writer lease")
        };
        lease.traffic = Some(traffic);
        self
    }
    /// Only nonempty request bodies need a shared reservation. Empty requests
    /// retain the inline lease and add no per-stream allocation.
    pub(crate) fn retain_writer(&mut self) -> H2WriterLease {
        let lease = match self.owner.take().expect("live stream reservation") {
            Owner::Exclusive(lease) => Arc::new(lease),
            Owner::Shared(lease) => lease,
        };
        self.owner = Some(Owner::Shared(lease.clone()));
        H2WriterLease { _lease: lease }
    }
}
impl Drop for StreamLease {
    fn drop(&mut self) {
        let idle = self.connection.capacity.release();
        if let Some(pool) = self.connection.pool.upgrade() {
            pool.in_flight.fetch_sub(1, Ordering::AcqRel);
            if idle {
                pool.trim_idle();
            }
        }
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
