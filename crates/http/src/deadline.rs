//! Response-head deadlines and cancellation of the answering future.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use pin_project_lite::pin_project;
use tokio::task::coop;
use tokio::time::{Instant, Sleep};

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
#[path = "../tests/deadline.rs"]
mod tests;
