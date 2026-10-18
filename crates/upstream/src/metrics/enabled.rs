//! Records directly into boot-bound counters; labels are export-only.
use std::sync::atomic::Ordering;

use super::UPSTREAM_METRICS;
use crate::health::probe::{ProbeKind, ProbeOutcome};
use crate::origin::OriginHealth;
use crate::proxy::service::UpstreamService;
impl UpstreamService {
    pub(crate) fn register_configured_capacity(&self) {
        let bank = UPSTREAM_METRICS.register(&self.inner.profile);
        for origin in self.inner.profile.origins.iter() {
            let _ = origin.metrics.bank.set(bank.clone());
            if let Some(counters) = origin.metrics.counters() {
                // Attachment projects current state; it is not a new transition.
                counters
                    .health
                    .store(origin.slot.get().health() as u8, Ordering::Relaxed);
                counters.serving.store(
                    origin.slot.get().serving() as u8,
                    Ordering::Relaxed,
                );
            }
        }
    }
    pub(crate) fn record_probe_result(
        &self,
        index: usize,
        source: ProbeKind,
        result: ProbeOutcome,
    ) {
        let s = source.index();
        let o = result.index();
        if let Some(c) = self
            .inner
            .profile
            .origins
            .get(index)
            .and_then(|o| o.metrics.counters())
        {
            c.probes[s][o].fetch_add(1, Ordering::Relaxed);
        }
    }
    pub(crate) fn record_health_transition(
        &self,
        index: usize,
        from: OriginHealth,
        to: OriginHealth,
    ) {
        if let Some(c) = self
            .inner
            .profile
            .origins
            .get(index)
            .and_then(|o| o.metrics.counters())
        {
            if from != to {
                c.health.store(to as u8, Ordering::Relaxed);
            }
            if from.is_healthy() != to.is_healthy() {
                c.transitions[usize::from(to.is_healthy())]
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
    }
    pub(crate) fn record_serving_transition(
        &self,
        index: usize,
        serving: crate::origin::Serving,
    ) {
        if let Some(counters) = self
            .inner
            .profile
            .origins
            .get(index)
            .and_then(|origin| origin.metrics.counters())
        {
            counters.serving.store(serving as u8, Ordering::Relaxed);
        }
    }
    pub(crate) fn record_l7_probe_result(
        &self,
        index: usize,
        result: ProbeOutcome,
    ) {
        if let Some(c) = self
            .inner
            .profile
            .origins
            .get(index)
            .and_then(|o| o.metrics.counters())
        {
            c.probes[super::slot::L7_PROBE_INDEX][result.index()]
                .fetch_add(1, Ordering::Relaxed);
        }
    }
    pub(crate) fn record_request_status(
        &self,
        index: usize,
        status: http::StatusCode,
    ) {
        if let Some(c) = self
            .inner
            .profile
            .origins
            .get(index)
            .and_then(|o| o.metrics.counters())
        {
            c.requests[if status.as_u16() < 600 {
                (status.as_u16() / 100 - 1) as usize
            } else {
                6
            }]
            .fetch_add(1, Ordering::Relaxed);
        }
    }
    pub(crate) fn record_retry(
        &self,
        index: usize,
        reason: crate::config::profile::retry::RetryCause,
    ) {
        if let Some(c) = self
            .inner
            .profile
            .origins
            .get(index)
            .and_then(|o| o.metrics.counters())
        {
            c.retries[reason.metric_index()].fetch_add(1, Ordering::Relaxed);
        }
    }
    pub(crate) fn record_outlier_ejection(
        &self,
        index: usize,
    ) {
        if let Some(c) = self
            .inner
            .profile
            .origins
            .get(index)
            .and_then(|o| o.metrics.counters())
        {
            c.ejections.fetch_add(1, Ordering::Relaxed);
        }
    }
    pub(crate) fn record_breaker_rejection(
        &self,
        index: usize,
    ) {
        self.record_request_error(index);
        if let Some(c) = self
            .inner
            .profile
            .origins
            .get(index)
            .and_then(|o| o.metrics.counters())
        {
            c.breaker_rejections.fetch_add(1, Ordering::Relaxed);
        }
    }
    pub(crate) fn record_request_error(
        &self,
        index: usize,
    ) {
        if let Some(c) = self
            .inner
            .profile
            .origins
            .get(index)
            .and_then(|o| o.metrics.counters())
        {
            c.requests[5].fetch_add(1, Ordering::Relaxed);
        }
    }
}
