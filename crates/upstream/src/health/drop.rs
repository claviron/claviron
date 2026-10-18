//! Connection-driver side signals.

use std::sync::Arc;

use crate::health::lock::{ProbeDuty, ProbeLock, UpstreamDownProbe};
use crate::health::probe::ProbeKind;
use crate::origin::Origin;
use crate::proxy::h2::pool::H2TargetPool;
use crate::proxy::service::UpstreamService;

impl UpstreamService {
    pub(crate) fn spawn_h1_driver_error_probe(
        &self,
        origin_index: usize,
        epoch: Arc<crate::proxy::service::lifecycle::ServiceEpoch>,
    ) {
        if self.is_shutting_down() {
            return;
        }

        let Ok(_handle) = tokio::runtime::Handle::try_current() else {
            return;
        };

        let service = self.clone();
        self.spawn_background_for(&epoch, async move {
            service.confirm_down_after_driver_error(origin_index).await;
        });
    }

    pub(crate) fn spawn_h2_driver_clean_end(
        &self,
        origin_index: usize,
        conn_id: u64,
        pool: Arc<H2TargetPool>,
        epoch: Arc<crate::proxy::service::lifecycle::ServiceEpoch>,
    ) {
        if self.is_shutting_down() {
            return;
        }

        let Ok(_handle) = tokio::runtime::Handle::try_current() else {
            return;
        };

        let service = self.clone();
        self.spawn_background_for(&epoch, async move {
            pool.clear_connection(conn_id).await;
            if let Some(origin) =
                service.inner.profile.origins.get(origin_index)
            {
                tracing::debug!(
                    profile = %service.inner.profile.name,
                    origin = origin.label.as_ref(),
                    "http upstream cleared h2 connection after clean driver end"
                );
            }
        });
    }

    pub(crate) fn spawn_h2_driver_error_probe(
        &self,
        origin_index: usize,
        conn_id: u64,
        pool: Arc<H2TargetPool>,
        epoch: Arc<crate::proxy::service::lifecycle::ServiceEpoch>,
    ) {
        if self.is_shutting_down() {
            return;
        }

        let Ok(_handle) = tokio::runtime::Handle::try_current() else {
            return;
        };

        let service = self.clone();
        self.spawn_background_for(&epoch, async move {
            service
                .probe_after_h2_driver_error(origin_index, conn_id, pool)
                .await;
        });
    }

    async fn probe_after_h2_driver_error(
        self,
        origin_index: usize,
        conn_id: u64,
        pool: Arc<H2TargetPool>,
    ) {
        if self.is_shutting_down() {
            return;
        }

        pool.clear_connection(conn_id).await;
        self.confirm_down_after_driver_error(origin_index).await;
    }

    async fn confirm_down_after_driver_error(
        self,
        origin_index: usize,
    ) {
        if self.is_shutting_down() {
            return;
        }

        // drop_probe is the operator gate: H2 defaults on (single session,
        // drop is a real signal), H1 defaults off (multi-conn pool, request
        // path catches outages with less noise). Operator can flip per profile.
        if !self.inner.profile.health.drop_probe.enabled {
            return;
        }

        if !self.origin_is_healthy(origin_index) {
            return;
        }

        let Some(origin) =
            self.inner.profile.origins.get(origin_index).cloned()
        else {
            return;
        };

        let Some(_assignment) = origin.slot.get().scope_guard() else {
            return;
        };
        let mut probe_lock =
            match ProbeDuty::<UpstreamDownProbe>::enter(&self, origin_index) {
                ProbeLock::Local => None,
                ProbeLock::Acquired(lock) => Some(lock),
                ProbeLock::YieldTo => return,
            };

        let result = tokio::select! { biased; _ = self.wait_shutdown() => return, result = self.run_driver_probe_attempt(&origin) => result };

        let _ = self.apply_outcome(
            origin_index,
            ProbeKind::Drop,
            result,
            &mut probe_lock,
        );
    }

    async fn run_driver_probe_attempt(
        &self,
        origin: &Origin,
    ) -> Result<
        Result<(), crate::health::probe::OriginReachabilityFailure>,
        tokio::time::error::Elapsed,
    > {
        tokio::time::timeout(
            self.inner.profile.health.drop_probe.probe_timeout,
            self.probe_origin_reachability(origin),
        )
        .await
    }
}
