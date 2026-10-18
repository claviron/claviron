//! Synchronous, cancellation-safe admission; no queue and no health verdict.
use std::sync::{Arc, Mutex};

use crate::config::profile::breaker::BreakerConfig;
use crate::proxy::failure::RequestFailure;
#[derive(Debug, Default)]
struct Counters {
    connections: usize,
    pending: usize,
    requests: usize,
    retries: usize,
}
#[derive(Debug)]
pub(crate) struct State {
    policy: BreakerConfig,
    counters: Mutex<Counters>,
    changed: tokio::sync::Notify,
}
impl State {
    pub(crate) fn new(policy: &BreakerConfig) -> Option<Arc<Self>> {
        policy.enabled().then(|| {
            Arc::new(Self {
                policy: policy.clone(),
                counters: Mutex::new(Counters::default()),
                changed: tokio::sync::Notify::new(),
            })
        })
    }
    pub(crate) fn admit(
        self: &Arc<Self>,
        retry: bool,
    ) -> Result<Admission, RequestFailure> {
        let mut c = self.counters.lock().unwrap_or_else(|p| p.into_inner());
        if full(self.policy.max_pending, c.pending)
            || full(
                self.policy.max_requests,
                c.pending.saturating_add(c.requests),
            )
            || (retry && full(self.policy.max_retries, c.retries))
        {
            return Err(RequestFailure::BreakerOpen);
        }
        c.pending += 1;
        if retry {
            c.retries += 1;
        }
        Ok(Admission { state: self.clone(), pending: true, retry })
    }
    pub(crate) async fn connection_available(&self) {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if !full(
                self.policy.max_connections,
                self.counters
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .connections,
            ) {
                return;
            }
            changed.await;
        }
    }
    pub(crate) fn connection(
        self: &Arc<Self>
    ) -> Result<Connection, RequestFailure> {
        let mut c = self.counters.lock().unwrap_or_else(|p| p.into_inner());
        if full(self.policy.max_connections, c.connections) {
            return Err(RequestFailure::BreakerOpen);
        }
        c.connections += 1;
        Ok(Connection(self.clone()))
    }
}
fn full(
    limit: Option<usize>,
    used: usize,
) -> bool {
    limit.is_some_and(|limit| used >= limit)
}
#[derive(Debug)]
pub(crate) struct Connection(Arc<State>);
impl Drop for Connection {
    fn drop(&mut self) {
        self.0
            .counters
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .connections -= 1;
        self.0.changed.notify_waiters();
    }
}
#[derive(Debug)]
pub(crate) struct Admission {
    state: Arc<State>,
    pending: bool,
    retry: bool,
}
impl Admission {
    pub(crate) fn ready(&mut self) {
        if self.pending {
            let mut c =
                self.state.counters.lock().unwrap_or_else(|p| p.into_inner());
            c.pending -= 1;
            c.requests += 1;
            self.pending = false;
        }
    }
}
impl Drop for Admission {
    fn drop(&mut self) {
        let mut c =
            self.state.counters.lock().unwrap_or_else(|p| p.into_inner());
        if self.pending {
            c.pending -= 1;
        } else {
            c.requests -= 1;
        }
        if self.retry {
            c.retries -= 1;
        }
    }
}
