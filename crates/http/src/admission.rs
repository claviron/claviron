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
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use pin_project_lite::pin_project;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::coop;
use tokio::time::{Instant, Sleep};

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
/// clock. A configured deadline starts on the first poll, not construction.
/// The answering future is stored once and stays pinned until completion or
/// cancellation; timing adds neither an allocation nor a task.
pub fn head_within<F: Future>(
    limit: Option<Duration>,
    answering: F,
) -> impl Future<Output = Option<F::Output>> {
    HeadWithin { answering: Some(answering), delay: None, limit }
}

pin_project! {
    struct HeadWithin<F> {
        #[pin]
        answering: Option<F>,
        #[pin]
        delay: Option<Sleep>,
        limit: Option<Duration>,
    }
}

impl<F: Future> Future for HeadWithin<F> {
    type Output = Option<F::Output>;

    fn poll(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Self::Output> {
        let mut this = self.project();
        if let Some(limit) = this.limit.take()
            && let Some(deadline) = Instant::now().checked_add(limit)
        {
            this.delay.set(Some(tokio::time::sleep_until(deadline)));
        }
        let Some(delay) = this.delay.as_mut().as_pin_mut() else {
            let result = this
                .answering
                .as_mut()
                .as_pin_mut()
                .expect("head wait polled after completion")
                .poll(cx);
            if result.is_ready() {
                this.answering.set(None);
            }
            return result.map(Some);
        };

        // Match Tokio's timeout ordering and cooperative-budget behavior:
        // a ready reply wins, and budget exhaustion must not hide expiration.
        let had_budget = coop::has_budget_remaining();
        if let Poll::Ready(reply) = this
            .answering
            .as_mut()
            .as_pin_mut()
            .expect("head wait polled after completion")
            .poll(cx)
        {
            this.answering.set(None);
            this.delay.set(None);
            return Poll::Ready(Some(reply));
        }
        let expired = if had_budget && !coop::has_budget_remaining() {
            std::pin::pin!(coop::unconstrained(delay)).poll(cx)
        } else {
            delay.poll(cx)
        };
        if expired.is_ready() {
            this.answering.set(None);
            this.delay.set(None);
        }
        expired.map(|()| None)
    }
}

#[cfg(test)]
#[path = "../tests/admission.rs"]
mod tests;
