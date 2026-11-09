//! Diagnostic counters belong to the benchmark, never to the production path.
//! Dispatch-waker calls are not kernel wakes or SHM doorbell sends.

use std::collections::BTreeMap;
use std::future::{Future, poll_fn};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::time::Instant;

use serde::Serialize;

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

#[derive(Serialize)]
pub struct Diagnostics {
    pub edge_request_polls: u64,
    pub edge_request_pending_polls: u64,
    pub edge_request_waker_calls: u64,
    pub completions: u64,
    pub round_trip_ns: u64,
    /// This high-water mark includes warmup; unlike counters it cannot be subtracted.
    pub max_round_trip_ns_including_warmup: u64,
    pub link_rejections: BTreeMap<String, u64>,
}

impl Diagnostics {
    pub fn since(
        self,
        before: Self,
    ) -> Self {
        let rejections = self
            .link_rejections
            .into_iter()
            .filter_map(|(reason, count)| {
                let count = count
                    - before.link_rejections.get(&reason).copied().unwrap_or(0);
                (count != 0).then_some((reason, count))
            })
            .collect();
        Self {
            edge_request_polls: self.edge_request_polls
                - before.edge_request_polls,
            edge_request_pending_polls: self.edge_request_pending_polls
                - before.edge_request_pending_polls,
            edge_request_waker_calls: self.edge_request_waker_calls
                - before.edge_request_waker_calls,
            completions: self.completions - before.completions,
            round_trip_ns: self.round_trip_ns - before.round_trip_ns,
            max_round_trip_ns_including_warmup: self
                .max_round_trip_ns_including_warmup,
            link_rejections: rejections,
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
    pub fn install(&self) -> super::Result<()> {
        tracing::subscriber::set_global_default(self.clone())?;
        Ok(())
    }
    pub fn snapshot(&self) -> Diagnostics {
        let get = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        Diagnostics {
            edge_request_polls: get(&self.0.polls),
            edge_request_pending_polls: get(&self.0.pending_polls),
            edge_request_waker_calls: get(&self.0.wake_calls),
            completions: get(&self.0.completions),
            round_trip_ns: get(&self.0.round_trip_ns),
            max_round_trip_ns_including_warmup: get(&self.0.max_round_trip_ns),
            link_rejections: self
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

#[derive(Default)]
struct Message(String);
impl tracing::field::Visit for Message {
    fn record_debug(
        &mut self,
        field: &tracing::field::Field,
        value: &dyn std::fmt::Debug,
    ) {
        if field.name() == "message" {
            self.0 = format!("{value:?}");
        }
    }
}
impl tracing::Subscriber for Observations {
    fn enabled(
        &self,
        metadata: &tracing::Metadata<'_>,
    ) -> bool {
        metadata.target() == "claviron_pipeline::link"
            && *metadata.level() == tracing::Level::DEBUG
    }
    fn new_span(
        &self,
        _: &tracing::span::Attributes<'_>,
    ) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(
        &self,
        _: &tracing::span::Id,
        _: &tracing::span::Record<'_>,
    ) {
    }
    fn record_follows_from(
        &self,
        _: &tracing::span::Id,
        _: &tracing::span::Id,
    ) {
    }
    fn enter(
        &self,
        _: &tracing::span::Id,
    ) {
    }
    fn exit(
        &self,
        _: &tracing::span::Id,
    ) {
    }
    fn event(
        &self,
        event: &tracing::Event<'_>,
    ) {
        let mut message = Message::default();
        event.record(&mut message);
        // Count one canonical event, not both "exchange refused" and this one.
        if let Some(reason) =
            message.0.strip_prefix("Link dispatch unavailable: ")
        {
            let mut rejections =
                self.0.rejections.lock().expect("diagnostic rejection lock");
            *rejections.entry(reason.to_owned()).or_default() += 1;
        }
    }
}
