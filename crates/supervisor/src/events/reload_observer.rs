//! Registry-installed acknowledgement routing; requests own only their evidence.
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, Weak};

use claviron_runtime::WorkerId;
use tokio::sync::Notify;

use super::reload::WorkerReloaded;
use crate::MasterIdentity;

/// Master-local, non-Runnable acknowledgement router installed by the registry.
///
/// Associates incoming reload generations with live request observations so
/// operations need no late event registration. Weak references prevent this
/// provider from extending a completed or cancelled request's lifetime.
#[derive(Default)]
pub(crate) struct ReloadObserver {
    requests: Mutex<BTreeMap<u64, Weak<ReloadObservation>>>,
}
/// Request-owned evidence of which workers acknowledged one reload generation.
/// Wakes its waiter when evidence changes; it neither executes reloads nor
/// decides the request's expected worker set or timeout.
pub(crate) struct ReloadObservation {
    issuer: MasterIdentity,
    received: Mutex<BTreeSet<WorkerId>>,
    pub(crate) changed: Notify,
}
impl ReloadObservation {
    pub(crate) fn received(&self) -> BTreeSet<WorkerId> {
        self.received.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }
}
impl ReloadObserver {
    pub(crate) fn begin(
        &self,
        issuer: MasterIdentity,
        generation: u64,
    ) -> Arc<ReloadObservation> {
        let observation = Arc::new(ReloadObservation {
            issuer,
            received: Mutex::default(),
            changed: Notify::new(),
        });
        let mut requests =
            self.requests.lock().unwrap_or_else(|e| e.into_inner());
        requests.retain(|_, request| request.strong_count() != 0);
        requests.insert(generation, Arc::downgrade(&observation));
        observation
    }
    pub(crate) fn observe(
        &self,
        event: WorkerReloaded,
    ) {
        if event.worker_id == 0 {
            return;
        }
        let observation = self
            .requests
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&event.generation)
            .and_then(Weak::upgrade);
        if let Some(observation) = observation {
            if event.issuer != observation.issuer {
                return;
            }
            observation
                .received
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(WorkerId::new(event.worker_id));
            observation.changed.notify_one();
        }
    }
}

#[cfg(test)]
#[path = "../../tests/events/reload_observer.rs"]
mod tests;
