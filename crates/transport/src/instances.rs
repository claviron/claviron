use std::collections::BTreeMap;
use std::sync::{Arc, Weak};

use claviron_http::ListenerId;

/// Cold declarations retain operational identity without retaining retired work.
pub struct ListenerInstances<T> {
    declared: BTreeMap<ListenerId, Arc<T>>,
    retired: BTreeMap<ListenerId, Weak<T>>,
}

impl<T> Default for ListenerInstances<T> {
    fn default() -> Self {
        Self { declared: BTreeMap::new(), retired: BTreeMap::new() }
    }
}

impl<T> ListenerInstances<T> {
    pub fn reconcile<'a>(
        &mut self,
        ids: impl IntoIterator<Item = &'a ListenerId>,
        mut create: impl FnMut(&ListenerId) -> T,
    ) {
        let mut next = BTreeMap::new();
        for id in ids {
            let instance = self
                .declared
                .remove(id)
                .or_else(|| {
                    self.retired.remove(id).and_then(|old| old.upgrade())
                })
                .unwrap_or_else(|| Arc::new(create(id)));
            next.insert(id.clone(), instance);
        }
        for (id, instance) in std::mem::take(&mut self.declared) {
            self.retired.insert(id, Arc::downgrade(&instance));
        }
        self.retired.retain(|_, instance| instance.strong_count() != 0);
        self.declared = next;
    }

    pub fn iter(&self) -> impl Iterator<Item = &Arc<T>> {
        self.declared.values()
    }
}

#[cfg(test)]
#[path = "../tests/instances.rs"]
mod tests;
