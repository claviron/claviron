//! Owns the protocol-specific pool handles retained by a service.

use std::sync::{Arc, Mutex};

use crate::proxy::h1::pool::TargetPool;
use crate::proxy::h2::pool::H2TargetPool;

#[derive(Clone, Debug)]
pub(crate) enum ProtocolPools {
    H1(Arc<[Arc<Mutex<TargetPool>>]>),
    H2(Arc<[Arc<H2TargetPool>]>),
}

#[derive(Clone)]
pub(crate) struct ServiceRuntime {
    pub(crate) state_table: Arc<crate::state::UpstreamStateTable>,
    // Compile the AppState role once: supervised master owns neither warm
    // serving sockets nor worker probe duties; detached/standalone does.
    pub(crate) serving_role: bool,
    /// Which edge worker this is, from zero; zero where there is only one
    /// process. `first` starts its order here, so each worker fills its own
    /// origin rather than all of them filling the same one.
    pub(crate) worker: usize,
    pub(crate) shutdown: tokio_util::sync::CancellationToken,
    /// None for direct profile consumers; managed providers own warm generations.
    pub(crate) warm_owner: Option<Arc<super::warm::Owner>>,
    pub(crate) events: Option<claviron_runtime::events::ProcessEventBus>,
    pub(crate) lock: Option<crate::health::lock::UpstreamLock>,
    pub(crate) changed: Arc<tokio::sync::Notify>,
    pub(crate) generations: Arc<std::sync::atomic::AtomicU64>,
}
impl ServiceRuntime {
    pub(crate) fn detached() -> Self {
        Self::with_table(crate::state::UpstreamStateTable::memory(
            crate::state::layout::CAPACITY,
        ))
    }
    pub(crate) fn with_table(
        state_table: Arc<crate::state::UpstreamStateTable>
    ) -> Self {
        Self {
            state_table,
            serving_role: true,
            worker: 0,
            shutdown: tokio_util::sync::CancellationToken::new(),
            warm_owner: None,
            events: None,
            lock: None,
            changed: Arc::new(tokio::sync::Notify::new()),
            generations: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }
}

impl ProtocolPools {
    pub(crate) fn in_flight(
        &self,
        id: crate::origin::OriginId,
    ) -> usize {
        match self {
            Self::H1(pools) => pools[id.index()]
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .in_flight(),
            Self::H2(pools) => pools[id.index()].in_flight(),
        }
    }
}
