//! Which routing keys this process can decode, for every link event bridge it
//! runs.
//!
//! A subscriber — a PHP task that subscribed to topics, a Rust consumer —
//! holds a guard for what it can decode, and the set is the union of the
//! guards alive. Each bridge publishes the set into its lane or watcher slot,
//! so the far side forwards only what someone here wants. Subscribing is the
//! declaration: there is no list to configure.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use tokio::sync::watch;

pub struct LinkInterest {
    keys: Mutex<BTreeMap<String, usize>>,
    changed: watch::Sender<Arc<[String]>>,
}

impl Default for LinkInterest {
    fn default() -> Self {
        Self {
            keys: Mutex::default(),
            changed: watch::channel(Arc::from(Vec::new())).0,
        }
    }
}

impl LinkInterest {
    /// Declare `keys` for as long as the guard lives.
    pub fn add<I, K>(
        self: &Arc<Self>,
        keys: I,
    ) -> InterestGuard
    where
        I: IntoIterator<Item = K>,
        K: Into<String>,
    {
        let keys: Vec<String> = keys.into_iter().map(Into::into).collect();
        let mut held = self.keys.lock().expect("link interest lock poisoned");
        let mut grew = false;
        for key in &keys {
            let count = held.entry(key.clone()).or_insert(0);
            grew |= *count == 0;
            *count += 1;
        }
        if grew {
            self.publish(&held);
        }
        InterestGuard { interest: Arc::clone(self), keys }
    }

    /// The current set, and every change to it.
    pub fn watch(&self) -> watch::Receiver<Arc<[String]>> {
        self.changed.subscribe()
    }

    fn release(
        &self,
        keys: &[String],
    ) {
        let mut held = self.keys.lock().expect("link interest lock poisoned");
        let mut shrank = false;
        for key in keys {
            if let Some(count) = held.get_mut(key) {
                *count -= 1;
                if *count == 0 {
                    held.remove(key);
                    shrank = true;
                }
            }
        }
        if shrank {
            self.publish(&held);
        }
    }

    fn publish(
        &self,
        held: &BTreeMap<String, usize>,
    ) {
        self.changed
            .send_replace(held.keys().cloned().collect::<Vec<_>>().into());
    }
}

/// Keeps keys declared; dropping it takes them back.
pub struct InterestGuard {
    interest: Arc<LinkInterest>,
    keys: Vec<String>,
}

impl Drop for InterestGuard {
    fn drop(&mut self) {
        self.interest.release(&self.keys);
    }
}

#[cfg(test)]
#[path = "../../tests/link/interest.rs"]
mod tests;
