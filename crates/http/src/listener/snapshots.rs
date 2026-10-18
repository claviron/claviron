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
}

type Groups<S> = BTreeMap<ListenerId, Arc<S>>;

/// Reload can replace policies, but cannot change a running generation's
/// bind/gate geometry. Publication and generation start share one lock;
/// accepted connections only load the lock-free current snapshot.
pub struct ListenerSnapshots<S> {
    current: ArcSwapOption<Groups<S>>,
    active: Mutex<Option<Arc<Groups<S>>>>,
}

impl<S> Default for ListenerSnapshots<S> {
    fn default() -> Self {
        Self { current: ArcSwapOption::empty(), active: Mutex::new(None) }
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
        if let Some(active) = active.as_ref() {
            if active.len() != candidate.len() {
                return Err(Error::msg(
                    "listener groups changed; service restart required",
                ));
            }
            for (id, previous) in active.iter() {
                if !candidate
                    .get(id)
                    .is_some_and(|next| previous.same_binding(next))
                {
                    return Err(Error::msg(format!(
                        "listener '{id}' bind/gate changed; service restart required"
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
        if active.is_some() {
            return Err(Error::msg("listener generation is already running"));
        }
        let groups = Arc::new(candidate);
        *active = Some(groups.clone());
        self.current.store(Some(groups.clone()));
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
            *active = None;
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
