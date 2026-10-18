//! What a server's requests are admitted under before an application sees
//! them: how many run at once, how long one waits for a place, and how long
//! it may take to its reply's head.
//!
//! These are a server's health, not a pipeline's: they hold on every path to
//! a host — a router, a bypass, a relay — and so they live here rather than in
//! the edge, for any server to apply at its own door. The edge applies them in
//! `HostService::dispatch`; a runner with a listener of its own, and no edge
//! in front, applies the same.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// How many requests one host serves at once.
///
/// One per host, shared by every connection to it. With no limit written,
/// nothing is kept and entering costs nothing.
#[derive(Clone, Debug, Default)]
pub struct Admission {
    places: Option<Arc<Semaphore>>,
}

/// A request's place among those running; held until its reply's head.
#[derive(Debug)]
pub struct Place(
    #[allow(dead_code, reason = "held for its drop")]
    Option<OwnedSemaphorePermit>,
);

impl Admission {
    pub fn limited(max_concurrent_requests: Option<usize>) -> Self {
        Self {
            places: max_concurrent_requests
                .map(|max| Arc::new(Semaphore::new(max))),
        }
    }

    /// A place for one request, waiting up to `queue_timeout` when all are
    /// taken — the request's own, from its policy — and for as long as it
    /// takes when none is written. `None` when the wait ran out: the answer is
    /// 503.
    pub async fn enter(
        &self,
        queue_timeout: Option<Duration>,
    ) -> Option<Place> {
        let Some(places) = self.places.as_ref() else {
            return Some(Place(None));
        };
        let place = Arc::clone(places).acquire_owned();
        let place = match queue_timeout {
            Some(wait) => tokio::time::timeout(wait, place).await.ok()?,
            None => place.await,
        };
        // The semaphore is never closed.
        Some(Place(place.ok()))
    }
}

/// The reply's head within `limit`, or `None` when it came too late: the
/// answer is 504, the server's lateness and not the client's. No limit, no
/// clock.
pub async fn head_within<F: Future>(
    limit: Option<Duration>,
    answering: F,
) -> Option<F::Output> {
    match limit {
        Some(limit) => tokio::time::timeout(limit, answering).await.ok(),
        None => Some(answering.await),
    }
}
