//! Optional lifecycle preconnect, sharing request acquisition's single-flight dial.
use std::sync::Arc;
use std::sync::atomic::Ordering;

use super::conn::H2DriverEndContext;
use super::pool::{H2AcquireError, H2TargetPool};
use crate::compile::profile::Capacity;
use crate::proxy::connect::AcquireConnectionError;
use crate::proxy::failure::RequestFailure;
use crate::proxy::service::UpstreamService;
use crate::proxy::service::lifecycle::ServiceEpoch;

struct WarmJob(Arc<H2TargetPool>);
impl WarmJob {
    fn claim(pool: &Arc<H2TargetPool>) -> Option<Self> {
        if !pool.needs_warmup()
            || pool
                .warming
                .compare_exchange(
                    false,
                    true,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_err()
        {
            return None;
        }
        Some(Self(pool.clone()))
    }
}

#[cfg(test)]
#[path = "../../../tests/proxy/h2/warm.rs"]
mod tests;
impl Drop for WarmJob {
    fn drop(&mut self) {
        self.0.warming.store(false, Ordering::Release);
    }
}

impl UpstreamService {
    pub(crate) fn warm_h2_pool(
        &self,
        index: usize,
        pool: &Arc<H2TargetPool>,
        epoch: &Arc<ServiceEpoch>,
    ) {
        if !self.inner.runtime.serving_role
            || epoch.background.is_cancelled()
            || self.inner.runtime.shutdown.is_cancelled()
            || !self.origin_eligible(index)
            || tokio::runtime::Handle::try_current().is_err()
        {
            return;
        }
        let Some(job) = WarmJob::claim(pool) else {
            return;
        };
        let service = self.clone();
        let captured = epoch.clone();
        let cancelled = epoch.background.clone();
        let shutdown = self.inner.runtime.shutdown.clone();
        self.spawn_warm_for(epoch, async move {
            tokio::select! {
                biased;
                _ = cancelled.cancelled() => {},
                _ = shutdown.cancelled() => {},
                _ = service.preconnect_h2(index, &job.0, &captured) => {},
            }
        });
    }

    async fn preconnect_h2(
        &self,
        index: usize,
        pool: &Arc<H2TargetPool>,
        epoch: &Arc<ServiceEpoch>,
    ) {
        let Capacity::H2 { replenish, .. } = &self.inner.profile.capacity
        else {
            return;
        };
        let mut backoff = replenish.initial_backoff;
        let origin = &self.inner.profile.origins[index];
        while self.origin_eligible(index) && pool.needs_warmup() {
            let connect = |id, generation| {
                debug_assert_eq!(
                    generation,
                    epoch.transport(origin.id).generation
                );
                self.connect_h2_connection(
                    Some(H2DriverEndContext { pool: Arc::downgrade(pool) }),
                    origin,
                    id,
                    epoch,
                )
            };
            let changed = pool.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            match pool.ensure_connection(&connect, false).await {
                Ok(None) => {
                    changed.await;
                    continue;
                }
                Ok(Some(connection)) => {
                    if !self.origin_eligible(index) {
                        pool.clear_connection(connection.id).await;
                        return;
                    }
                    let mut sender = connection.sender();
                    // Readiness belongs to this cancellable warm job. No request
                    // stream/traffic permit or invented readiness deadline.
                    match std::future::poll_fn(|cx| sender.poll_ready(cx)).await
                    {
                        Ok(()) => return,
                        Err(error) => {
                            tracing::debug!(source = %origin.source, %error,
                                "upstream h2 warm connection not ready");
                            pool.clear_connection(connection.id).await;
                        }
                    }
                }
                Err(H2AcquireError::Pool(RequestFailure::Closed)) => return,
                Err(H2AcquireError::Pool(RequestFailure::BreakerOpen)) => {
                    if let Some(breaker) = &epoch.breaker {
                        breaker.connection_available().await;
                    }
                    continue;
                }
                Err(error) => {
                    let error: H2AcquireError<AcquireConnectionError> = error;
                    tracing::debug!(source = %origin.source, ?error,
                        "upstream h2 warm connection failed");
                }
            }
            tokio::time::sleep(backoff).await;
            backoff = backoff.saturating_mul(2).min(replenish.max_backoff);
        }
    }
}
