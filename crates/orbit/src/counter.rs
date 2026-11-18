//! Keyed counters over the fleet's Orbitals table.
//!
//! A counter is a kept `i64` Orbital whose name is its key, in the table
//! [`OrbitalsProvider`] owns (kinds `174` and `170`). `orbit-counter` adds the
//! checked arithmetic and the lookup that installs nothing, and is
//! re-exported here unchanged. There is no counter segment and no lifecycle
//! of its own: the table's reset keeps counters because they are declared
//! kept, and its exit preserves them with every other value.
//!
//! [`CounterProvider`] is the capability: in each process it builds one
//! [`Counter`] over the Orbitals handle once that process has decided the
//! table, and hands it out as `Arc<Counter>`. A [`CounterHandle`] opened
//! from it belongs to the table's epoch, which a process never sees change
//! after that decision.

use std::sync::Arc;

use arc_swap::ArcSwapOption;
use async_trait::async_trait;
use claviron_runtime::AppState;
use claviron_runtime::registry::{
    self as runtime_registry, Bootable, Component,
};
pub use orbitive::counter::*;

use crate::OrbitalsProvider;

/// The process's [`Counter`], built at boot after [`OrbitalsProvider`].
///
/// `handle()` is `None` until it has booted. A worker boots its own registry
/// and builds its own `Counter`; every process reaches the same counters,
/// because they are values of the one fleet table.
pub struct CounterProvider {
    counter: ArcSwapOption<Counter>,
}

impl CounterProvider {
    pub(crate) fn new() -> Self {
        Self { counter: ArcSwapOption::const_empty() }
    }

    pub fn handle(&self) -> Option<Arc<Counter>> {
        self.counter.load_full()
    }
}

impl Component<AppState> for CounterProvider {
    type Deps = (Arc<OrbitalsProvider>,);

    fn name(&self) -> &'static str {
        "CounterProvider"
    }

    fn as_bootable(&self) -> Option<&dyn Bootable<AppState, Self::Deps>> {
        Some(self)
    }
}

#[async_trait]
impl Bootable<AppState, (Arc<OrbitalsProvider>,)> for CounterProvider {
    async fn boot(
        &self,
        (provider,): (Arc<OrbitalsProvider>,),
        _state: &AppState,
    ) -> runtime_registry::Result<()> {
        let orbitals = provider.handle().ok_or_else(|| {
            runtime_registry::Error::msg(
                "counters are unavailable: the Orbitals table is not open",
            )
        })?;
        let counter = Counter::new(orbitals.as_ref().clone());
        self.counter.store(Some(Arc::new(counter)));
        Ok(())
    }
}
