use super::*;
use std::future::Future;
use std::task::{Context, Poll, Waker};

fn poll<F: Future>(future: std::pin::Pin<&mut F>) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(Waker::noop()))
}

#[tokio::test]
async fn fifo_turn_cancellation_and_fast_path_state() {
    let admission = CreatorAdmission::default();
    assert!(!admission.contended());
    let first = admission.acquire().await;
    assert!(admission.contended());
    let mut cancelled = Box::pin(admission.acquire());
    let mut next = Box::pin(admission.acquire());
    let mut last = Box::pin(admission.acquire());
    assert!(poll(cancelled.as_mut()).is_pending());
    assert!(poll(next.as_mut()).is_pending());
    assert!(poll(last.as_mut()).is_pending());
    drop(cancelled);
    drop(first);
    assert!(admission.contended(), "queued callers prevent fast-path barging");
    assert!(poll(last.as_mut()).is_pending(), "FIFO ownership is preserved");
    let second = next.await;
    assert!(poll(last.as_mut()).is_pending());
    drop(second);
    drop(last.await);
    assert!(!admission.contended());
}

#[tokio::test]
async fn cancelling_all_queued_callers_clears_contention_after_the_owner() {
    let admission = CreatorAdmission::default();
    let owner = admission.acquire().await;
    let mut queued = Box::pin(admission.acquire());
    assert!(poll(queued.as_mut()).is_pending());
    drop(queued);
    assert!(admission.contended());
    drop(owner);
    assert!(!admission.contended());
}
