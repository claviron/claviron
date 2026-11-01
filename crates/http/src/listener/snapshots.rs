//! Coherent policy publication for named transport groups. This owns no
//! sockets or tasks; transport runnables own bind, admission and drain.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use arc_swap::ArcSwapOption;
use claviron_runtime::registry::{Error, Result};

use super::ListenerId;
pub trait ListenerSnapshot {
    fn listener_id(&self) -> &ListenerId;
    fn same_binding(
        &self,
        other: &Self,
    ) -> bool;
    /// Whether both snapshots would compete for the same non-ephemeral bind.
    fn binding_conflicts(
        &self,
        other: &Self,
    ) -> bool;
}

type Groups<S> = BTreeMap<ListenerId, Arc<S>>;

/// Reload can replace policies, but cannot change a running generation's
/// bind/gate geometry. Publication and generation start share one lock;
/// accepted connections only load the lock-free current snapshot.
pub struct ListenerSnapshots<S> {
    current: ArcSwapOption<Groups<S>>,
    active: Mutex<Groups<S>>,
}

impl<S> Default for ListenerSnapshots<S> {
    fn default() -> Self {
        Self {
            current: ArcSwapOption::empty(),
            active: Mutex::new(BTreeMap::new()),
        }
    }
}

impl<S: ListenerSnapshot> ListenerSnapshots<S> {
    pub fn get(
        &self,
        id: &ListenerId,
    ) -> Option<Arc<S>> {
        self.current.load().as_ref()?.get(id).cloned()
    }

    pub fn publish(
        &self,
        candidate: Groups<S>,
    ) -> Result<()> {
        validate_ids(&candidate)?;
        let active = self
            .active
            .lock()
            .map_err(|_| Error::msg("listener lifecycle lock poisoned"))?;
        for (id, previous) in active.iter() {
            for (next_id, next) in &candidate {
                if id == next_id && !previous.same_binding(next) {
                    return Err(Error::msg(format!(
                        "listener '{id}' bind/gate changed; service restart required"
                    )));
                }
                if id != next_id && previous.binding_conflicts(next) {
                    return Err(Error::msg(format!(
                        "listener '{next_id}' conflicts with running listener '{id}'; stop it before changing ownership"
                    )));
                }
            }
        }
        self.current.store(Some(Arc::new(candidate)));
        Ok(())
    }

    /// Capture a fresh generation from the provider's current desired config.
    /// Keep the guard until every bound socket and accepted task is drained.
    pub fn start(
        &self,
        candidate: Groups<S>,
    ) -> Result<ActiveListeners<'_, S>> {
        validate_ids(&candidate)?;
        let mut active = self
            .active
            .lock()
            .map_err(|_| Error::msg("listener lifecycle lock poisoned"))?;
        if !active.is_empty() {
            return Err(Error::msg("listener generation is already running"));
        }
        let groups = Arc::new(candidate);
        *active = groups.as_ref().clone();
        self.current.store(Some(groups.clone()));
        Ok(ActiveListeners { owner: self, groups })
    }

    /// Start one independently managed listener without replacing its siblings.
    /// The guard owns this listener's bind/gate generation through drain.
    pub fn start_listener(
        &self,
        snapshot: Arc<S>,
    ) -> Result<ActiveListeners<'_, S>> {
        let id = snapshot.listener_id().clone();
        let mut active = self
            .active
            .lock()
            .map_err(|_| Error::msg("listener lifecycle lock poisoned"))?;
        if active.contains_key(&id) {
            return Err(Error::msg(format!(
                "listener '{id}' is already running"
            )));
        }
        for (other_id, other) in active.iter() {
            if other.binding_conflicts(&snapshot) {
                return Err(Error::msg(format!(
                    "listener '{id}' conflicts with running listener '{other_id}'"
                )));
            }
        }
        let groups = Arc::new(BTreeMap::from([(id.clone(), snapshot.clone())]));
        let mut current = self
            .current
            .load_full()
            .map(|groups| groups.as_ref().clone())
            .unwrap_or_default();
        current.insert(id.clone(), snapshot.clone());
        active.insert(id, snapshot);
        self.current.store(Some(Arc::new(current)));
        Ok(ActiveListeners { owner: self, groups })
    }
}

pub struct ActiveListeners<'a, S> {
    owner: &'a ListenerSnapshots<S>,
    groups: Arc<Groups<S>>,
}

impl<S> ActiveListeners<'_, S> {
    pub fn groups(&self) -> &Groups<S> {
        &self.groups
    }
}

impl<S> Drop for ActiveListeners<'_, S> {
    fn drop(&mut self) {
        if let Ok(mut active) = self.owner.active.lock() {
            for (id, snapshot) in self.groups.iter() {
                if active
                    .get(id)
                    .is_some_and(|owned| Arc::ptr_eq(owned, snapshot))
                {
                    active.remove(id);
                }
            }
        }
    }
}

fn validate_ids<S: ListenerSnapshot>(groups: &Groups<S>) -> Result<()> {
    for (id, snapshot) in groups {
        if id != snapshot.listener_id() {
            return Err(Error::msg(format!(
                "listener key '{id}' does not match snapshot identity '{}'",
                snapshot.listener_id()
            )));
        }
    }
    Ok(())
}
