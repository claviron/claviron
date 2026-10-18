use std::fmt::Display;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, RwLock, Weak};

mod event;
pub(crate) mod owner;

/// Fleet-wide channel membership crossed zero, not merely one socket joining.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OccupancyKind {
    Occupied,
    Vacated,
}

/// A decided broadcast transition. No Event trait or topic decoder is required.
#[derive(Clone, Debug, PartialEq)]
pub struct OccupancyChange {
    pub kind: OccupancyKind,
    /// Pusher application ID, not the configuration catalog's map key.
    pub app_id: String,
    pub channel: String,
    /// Owner's ISO-8601 timestamp, also carried by the application event.
    pub time: String,
    pub user_id: Option<String>,
    pub user_info: Option<serde_json::Value>,
}

type HookFuture = Pin<Box<dyn Future<Output = Result<(), String>> + Send>>;
type Callback = dyn Fn(OccupancyChange) -> HookFuture + Send + Sync;

/// Service-owned callback registry, shared with the local occupancy owner.
#[derive(Clone, Default)]
pub struct Hooks {
    occupancy: Arc<RwLock<Vec<Weak<Callback>>>>,
}

impl Hooks {
    /// Register before the occupancy owner runs and retain the returned handle.
    ///
    /// Register in the master or standalone process: worker registrations are
    /// local and are not transported to the master. Callbacks run serially in
    /// registration order under the existing owner future, without a task or
    /// polling loop. A slow callback delays later transitions; callbacks must
    /// cooperate with application shutdown and must not wait for a subsequent
    /// occupancy transition. Errors are logged without suppressing other hooks
    /// or the existing ApplicationEvent publication. There is no retry/replay.
    /// Capture an existing application DB pool when database work is needed.
    pub fn on_occupancy<F, Fut, E>(
        &self,
        callback: F,
    ) -> Subscription
    where
        F: Fn(OccupancyChange) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: Display,
    {
        let callback: Arc<Callback> = Arc::new(move |change| {
            let future = callback(change);
            Box::pin(
                async move { future.await.map_err(|error| error.to_string()) },
            )
        });
        let mut callbacks =
            self.occupancy.write().expect("Occupancy hook lock poisoned");
        callbacks.retain(|callback| callback.strong_count() != 0);
        callbacks.push(Arc::downgrade(&callback));
        Subscription { _callback: callback }
    }

    pub(crate) async fn dispatch(
        &self,
        change: OccupancyChange,
    ) {
        // Snapshot admission under the lock, but never hold it across user code.
        let callbacks = {
            let mut callbacks =
                self.occupancy.write().expect("Occupancy hook lock poisoned");
            callbacks.retain(|callback| callback.strong_count() != 0);
            callbacks.iter().filter_map(Weak::upgrade).collect::<Vec<_>>()
        };
        for callback in callbacks {
            if let Err(error) = callback(change.clone()).await {
                tracing::warn!(app_id = %change.app_id, channel = %change.channel, %error, "Broadcast occupancy hook failed");
            }
        }
    }
}

/// Dropping this handle unsubscribes from future dispatches.
///
/// Callbacks already admitted into a dispatch finish normally; dropping the
/// registration does not cancel an in-flight database operation. The registry
/// retains only weak references, so it cannot keep a captured pool/state alive
/// after the handle and any in-flight dispatch are released.
#[must_use = "retain the subscription for as long as the callback should run"]
pub struct Subscription {
    _callback: Arc<Callback>,
}

#[cfg(test)]
#[path = "../../tests/hooks/occupancy.rs"]
mod tests;
