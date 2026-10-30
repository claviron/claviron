use std::marker::PhantomPinned;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::Waker;

use super::*;

struct LargeAnswer {
    _buffer: [u8; 4096],
    dropped: Arc<AtomicUsize>,
    address: AtomicUsize,
    ready: bool,
    _pin: PhantomPinned,
}

impl Future for LargeAnswer {
    type Output = ();

    fn poll(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<()> {
        let address = self.as_ref().get_ref() as *const Self as usize;
        let first = self.address.load(Ordering::Relaxed);
        if first == 0 {
            self.address.store(address, Ordering::Relaxed);
        } else {
            assert_eq!(
                address, first,
                "the answering future must remain pinned"
            );
        }
        if self.ready { Poll::Ready(()) } else { Poll::Pending }
    }
}

impl Drop for LargeAnswer {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::Relaxed);
    }
}

fn large_answer(dropped: Arc<AtomicUsize>) -> LargeAnswer {
    LargeAnswer {
        _buffer: [0; 4096],
        dropped,
        address: AtomicUsize::new(0),
        ready: false,
        _pin: PhantomPinned,
    }
}

#[test]
fn head_within_stores_one_large_future_and_cancels_without_a_runtime() {
    for limit in [None, Some(Duration::from_secs(1))] {
        let dropped = Arc::new(AtomicUsize::new(0));
        let answering = large_answer(dropped.clone());
        let size = std::mem::size_of_val(&answering);
        let head = head_within(limit, answering);
        assert!(std::mem::size_of_val(&head) < size * 2);
        // Even a timed head is lazy: constructing and dropping it needs no
        // runtime. An untimed head can also be polled without a clock.
        let mut head = Box::pin(head);
        if limit.is_none() {
            let mut context = Context::from_waker(Waker::noop());
            assert!(head.as_mut().poll(&mut context).is_pending());
            assert!(head.as_mut().poll(&mut context).is_pending());
        }
        assert_eq!(dropped.load(Ordering::Relaxed), 0);
        drop(head);
        assert_eq!(dropped.load(Ordering::Relaxed), 1);
    }
}

#[tokio::test(start_paused = true)]
async fn head_within_deadline_starts_on_first_poll_and_releases_answer() {
    let limit = Duration::from_secs(1);
    let dropped = Arc::new(AtomicUsize::new(0));
    let head = head_within(Some(limit), large_answer(dropped.clone()));
    tokio::time::advance(limit * 2).await;
    let mut head = Box::pin(head);
    assert!(futures_util::poll!(head.as_mut()).is_pending());
    tokio::time::advance(limit).await;
    assert_eq!(futures_util::poll!(head.as_mut()), Poll::Ready(None));
    assert_eq!(dropped.load(Ordering::Relaxed), 1);
    drop(head);
    assert_eq!(dropped.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn head_within_completion_releases_answer_before_the_wait_is_dropped() {
    for limit in [None, Some(Duration::from_secs(1))] {
        let dropped = Arc::new(AtomicUsize::new(0));
        let mut answering = large_answer(dropped.clone());
        answering.ready = true;
        let mut head = Box::pin(head_within(limit, answering));
        assert_eq!(futures_util::poll!(head.as_mut()), Poll::Ready(Some(())));
        assert_eq!(dropped.load(Ordering::Relaxed), 1);
        drop(head);
        assert_eq!(dropped.load(Ordering::Relaxed), 1);
    }
}

#[tokio::test(start_paused = true)]
async fn head_within_ready_answer_wins_an_expired_deadline() {
    let ready = Arc::new(AtomicBool::new(false));
    let flag = ready.clone();
    let answering = std::future::poll_fn(move |_| {
        if flag.load(Ordering::Relaxed) {
            Poll::Ready(42)
        } else {
            Poll::Pending
        }
    });
    let limit = Duration::from_secs(1);
    let mut head = Box::pin(head_within(Some(limit), answering));
    assert!(futures_util::poll!(head.as_mut()).is_pending());
    tokio::time::advance(limit).await;
    ready.store(true, Ordering::Relaxed);
    assert_eq!(head.await, Some(42));
    assert_eq!(
        head_within(Some(Duration::ZERO), std::future::ready(7)).await,
        Some(7)
    );
}

#[tokio::test(start_paused = true)]
async fn head_within_expiration_survives_answer_exhausting_cooperative_budget()
{
    let exhaust = Arc::new(AtomicBool::new(false));
    let flag = exhaust.clone();
    let answering = std::future::poll_fn(move |cx| {
        if flag.load(Ordering::Relaxed) {
            while coop::has_budget_remaining() {
                let _ = std::pin::pin!(tokio::task::consume_budget()).poll(cx);
            }
        }
        Poll::<()>::Pending
    });
    let limit = Duration::from_secs(1);
    let mut head = Box::pin(head_within(Some(limit), answering));
    assert!(futures_util::poll!(head.as_mut()).is_pending());
    tokio::time::advance(limit).await;
    exhaust.store(true, Ordering::Relaxed);
    // A spawned task has a cooperative budget. Poll exactly once so a
    // regression fails an assertion instead of spinning the test runtime.
    let result = tokio::spawn(async move {
        std::future::poll_fn(|cx| Poll::Ready(head.as_mut().poll(cx))).await
    })
    .await
    .unwrap();
    assert_eq!(result, Poll::Ready(None));
}

#[tokio::test(start_paused = true)]
async fn head_within_cancellation_releases_a_started_timed_answer() {
    let dropped = Arc::new(AtomicUsize::new(0));
    let mut head = Box::pin(head_within(
        Some(Duration::from_secs(1)),
        large_answer(dropped.clone()),
    ));
    assert!(futures_util::poll!(head.as_mut()).is_pending());
    assert!(futures_util::poll!(head.as_mut()).is_pending());
    drop(head);
    assert_eq!(dropped.load(Ordering::Relaxed), 1);
}
