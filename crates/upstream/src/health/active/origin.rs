//! One origin's active schedule, polled by the active Runnable generation.
use tokio::time::{self, MissedTickBehavior};

use crate::health::lock::{ProbeDuty, ProbeLock, UpstreamActiveProbe};
use crate::health::probe::ProbeKind;
use crate::proxy::service::UpstreamService;
impl UpstreamService {
    pub(crate) async fn run_active_origin_loop(
        self,
        index: usize,
    ) {
        let l4 = self.clone();
        let l7 = self.clone();
        let outlier = self.clone();
        tokio::join!(
            async move {
                if l4.inner.profile.health.active.enabled {
                    l4.run_active_l4_loop(index).await;
                }
            },
            async move {
                if l7.inner.profile.health.l7.enabled {
                    l7.run_l7_origin_loop(index).await;
                }
            },
            async move {
                if outlier.inner.profile.outlier.enabled() {
                    outlier.run_outlier_origin_loop(index).await;
                }
            }
        );
    }
    async fn run_active_l4_loop(
        self,
        origin_index: usize,
    ) {
        let active = self.inner.profile.health.active.clone();
        let Some(origin) =
            self.inner.profile.origins.get(origin_index).cloned()
        else {
            return;
        };

        let mut ticker = time::interval(active.interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        // Skip the first immediate tick: tokio::time::interval fires once
        // synchronously on first poll, but we want first probe to land at
        // `interval` after spawn rather than immediately at boot.
        ticker.tick().await;

        loop {
            tokio::select! { biased; _ = self.wait_shutdown() => return, _ = ticker.tick() => {} }

            if self.is_shutting_down() || !origin.slot.get().available() {
                return;
            }
            if !self.origin_is_healthy(origin_index) {
                continue;
            }

            let Some(_assignment) = origin.slot.get().scope_guard() else {
                return;
            };
            let mut probe_lock = match ProbeDuty::<UpstreamActiveProbe>::enter(
                &self,
                origin_index,
            ) {
                ProbeLock::Local => None,
                ProbeLock::Acquired(lock) => Some(lock),
                ProbeLock::YieldTo => continue,
            };

            let probe_timeout = active.probe_timeout;
            let result = tokio::select! { biased; _ = self.wait_shutdown() => return,
            result = time::timeout(probe_timeout, self.probe_origin_reachability(&origin)) => result };

            let _ = self.apply_outcome(
                origin_index,
                ProbeKind::Active,
                result,
                &mut probe_lock,
            );

            if probe_lock.is_some() {
                tokio::select! {
                    biased;
                    _ = self.wait_shutdown() => return,
                    _ = time::sleep(active.interval) => {}
                }
            }
        }
    }
}
