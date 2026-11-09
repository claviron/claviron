//! Diagnostic counters belong to the benchmark, never to the production path.
//! Dispatch-waker calls are not kernel wakes or SHM doorbell sends.

use std::collections::BTreeMap;
use std::future::{Future, poll_fn};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::time::Instant;

use serde::{Deserialize, Serialize};

#[derive(Default)]
struct State {
    polls: AtomicU64,
    pending_polls: AtomicU64,
    wake_calls: AtomicU64,
    completions: AtomicU64,
    round_trip_ns: AtomicU64,
    max_round_trip_ns: AtomicU64,
    rejections: Mutex<BTreeMap<String, u64>>,
}

#[derive(Clone)]
pub struct Observations(Arc<State>);

#[derive(Deserialize, Serialize)]
pub struct Diagnostics {
    pub client_request_polls: u64,
    pub client_request_pending_polls: u64,
    pub client_request_waker_calls: u64,
    pub completions: u64,
    pub round_trip_ns: u64,
    /// This high-water mark includes warmup; unlike counters it cannot be subtracted.
    pub max_round_trip_ns_lifetime: u64,
    pub returned_errors: BTreeMap<String, u64>,
}

impl Diagnostics {
    pub fn since(
        self,
        before: Self,
    ) -> Self {
        let rejections = self
            .returned_errors
            .into_iter()
            .filter_map(|(reason, count)| {
                let count = count
                    - before.returned_errors.get(&reason).copied().unwrap_or(0);
                (count != 0).then_some((reason, count))
            })
            .collect();
        Self {
            client_request_polls: self.client_request_polls
                - before.client_request_polls,
            client_request_pending_polls: self.client_request_pending_polls
                - before.client_request_pending_polls,
            client_request_waker_calls: self.client_request_waker_calls
                - before.client_request_waker_calls,
            completions: self.completions - before.completions,
            round_trip_ns: self.round_trip_ns - before.round_trip_ns,
            max_round_trip_ns_lifetime: self.max_round_trip_ns_lifetime,
            returned_errors: rejections,
        }
    }
}

struct CountWake {
    target: Waker,
    state: Arc<State>,
}
impl Wake for CountWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.state.wake_calls.fetch_add(1, Ordering::Relaxed);
        self.target.wake_by_ref();
    }
}

impl Observations {
    pub fn new() -> Self {
        Self(Arc::new(State::default()))
    }
    pub fn rejected(
        &self,
        reason: &str,
    ) {
        let mut rejections =
            self.0.rejections.lock().expect("diagnostic rejection lock");
        *rejections.entry(reason.to_owned()).or_default() += 1;
    }
    pub fn snapshot(&self) -> Diagnostics {
        let get = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        Diagnostics {
            client_request_polls: get(&self.0.polls),
            client_request_pending_polls: get(&self.0.pending_polls),
            client_request_waker_calls: get(&self.0.wake_calls),
            completions: get(&self.0.completions),
            round_trip_ns: get(&self.0.round_trip_ns),
            max_round_trip_ns_lifetime: get(&self.0.max_round_trip_ns),
            returned_errors: self
                .0
                .rejections
                .lock()
                .expect("diagnostic rejection lock")
                .clone(),
        }
    }
    pub async fn track<F: Future>(
        &self,
        future: F,
    ) -> F::Output {
        let mut future = std::pin::pin!(future);
        let started = Instant::now();
        let mut cached: Option<(Waker, Waker)> = None;
        let result = poll_fn(|cx| {
            self.0.polls.fetch_add(1, Ordering::Relaxed);
            if cached
                .as_ref()
                .is_none_or(|(original, _)| !original.will_wake(cx.waker()))
            {
                let source = cx.waker().clone();
                let counted = Waker::from(Arc::new(CountWake {
                    target: source.clone(),
                    state: Arc::clone(&self.0),
                }));
                cached = Some((source, counted));
            }
            let counted = &cached.as_ref().expect("counted waker").1;
            let mut context = Context::from_waker(counted);
            match future.as_mut().poll(&mut context) {
                Poll::Pending => {
                    self.0.pending_polls.fetch_add(1, Ordering::Relaxed);
                    Poll::Pending
                }
                Poll::Ready(value) => Poll::Ready(value),
            }
        })
        .await;
        let elapsed = started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
        self.0.round_trip_ns.fetch_add(elapsed, Ordering::Relaxed);
        self.0.max_round_trip_ns.fetch_max(elapsed, Ordering::Relaxed);
        self.0.completions.fetch_add(1, Ordering::Relaxed);
        result
    }
}
