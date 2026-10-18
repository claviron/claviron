//! Slot-backed reachability and worker-local pool/metrics projections.
use std::sync::atomic::Ordering;

use crate::origin::OriginHealth;
use crate::proxy::failure::DirtyReason;
use crate::proxy::service::{ProtocolPools, UpstreamService};
impl UpstreamService {
    pub(crate) fn record_origin_reachability_failure(
        &self,
        index: usize,
    ) {
        if self.is_shutting_down() {
            return;
        }
        let Some(origin) = self.inner.profile.origins.get(index) else {
            return;
        };
        let Some((before, after)) = origin.slot.get().record_failure_change(
            self.inner.profile.health.passive.failure_threshold,
        ) else {
            return;
        };
        self.refresh_origin_health(index);
        self.publish_origin_health_event(index, before, after);
        self.ensure_origin_recovery(index);
    }
    /// This cache records only what this worker has already projected. Selection
    /// always reads the slot, including transitions whose wake-up event was missed.
    pub(crate) fn refresh_origin_health(
        &self,
        index: usize,
    ) {
        let Some(origin) = self.inner.profile.origins.get(index) else {
            return;
        };
        // Snapshot real verdicts while available. A closed assignment must
        // never turn fail-closed fallback values into a Down projection.
        let Some(serving) = self.origin_serving_if_available(index) else {
            return;
        };
        let Some(after) = origin.slot.get().read(|slot| slot.health()) else {
            return;
        };
        let observed_serving = &self.inner.observed_serving[index];
        let mut resumed = false;
        if observed_serving.load(Ordering::Acquire) != serving as u8
            && observed_serving.swap(serving as u8, Ordering::AcqRel)
                != serving as u8
        {
            self.record_serving_transition(index, serving);
            resumed = true;
        }
        let observed = &self.inner.observed_health[index];
        if observed.load(Ordering::Acquire) == after as u8 {
            if resumed {
                self.warm_origin(index);
            }
            return;
        }
        let before =
            OriginHealth::decode(observed.swap(after as u8, Ordering::AcqRel));
        self.record_health_transition(index, before, after);
        if !after.is_healthy() && before.is_healthy() {
            self.cleanup_origin_pool_after_down(index);
            self.ensure_origin_recovery(index);
        } else if after.is_healthy() {
            self.warm_origin(index);
        }
    }
    pub(crate) fn ensure_origin_recovery(
        &self,
        index: usize,
    ) {
        if self.is_shutting_down()
            || !self.inner.profile.health.recovery_probe.enabled
        {
            return;
        }
        let Some(origin) = self.inner.profile.origins.get(index) else {
            return;
        };
        let Some(health) = origin.slot.get().read(|slot| slot.health()) else {
            return;
        };
        if health.is_down() || matches!(health, OriginHealth::HalfOpen) {
            // Wake the existing recovery generation; the request path never
            // creates a task or claims worker-local ownership of fleet health.
            self.inner.runtime.changed.notify_waiters();
        }
    }
    pub(crate) fn cleanup_origin_pool_after_down(
        &self,
        origin_index: usize,
    ) {
        if self.is_shutting_down() {
            return;
        }

        let epoch = self.epoch();
        match &epoch.pools {
            ProtocolPools::H1(pools) => {
                if let Some(pool) = pools.get(origin_index) {
                    let dropped = pool
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .purge_idle(DirtyReason::OriginMarkedDown);
                    if dropped > 0
                        && let Some(origin) =
                            self.inner.profile.origins.get(origin_index)
                    {
                        tracing::debug!(
                            profile = %self.inner.profile.name,
                            origin = origin.label.as_ref(),
                            dropped,
                            "purged idle connections"
                        );
                    }
                }
            }
            ProtocolPools::H2(pools) => {
                let Some(pool) = pools.get(origin_index).cloned() else {
                    return;
                };
                let profile = self.inner.profile.name.clone();
                let origin =
                    self.inner.profile.origins.get(origin_index).cloned();
                if tokio::runtime::Handle::try_current().is_ok() {
                    self.spawn_background_for(&epoch, async move {
                        pool.clear_current_connection().await;
                        if let Some(origin) = origin {
                            tracing::debug!(
                                profile = %profile,
                                origin = origin.label.as_ref(),
                                "cleared h2 connection"
                            );
                        }
                    });
                }
            }
        }
    }

    pub(crate) fn record_origin_success(
        &self,
        index: usize,
    ) {
        if self.is_shutting_down() {
            return;
        }
        let Some(origin) = self.inner.profile.origins.get(index) else {
            return;
        };
        let Some((before, after)) = origin.slot.get().record_success_change()
        else {
            return;
        };
        self.refresh_origin_health(index);
        self.publish_origin_health_event(index, before, after);
    }
    pub(crate) fn reserve_validator_probe(
        &self,
        index: usize,
    ) -> bool {
        self.inner
            .profile
            .origins
            .get(index)
            .is_some_and(|origin| origin.slot.get().reserve_validator_probe())
    }
    pub(crate) fn origin_is_healthy(
        &self,
        index: usize,
    ) -> bool {
        self.inner
            .profile
            .origins
            .get(index)
            .is_some_and(|origin| origin.slot.get().health().is_healthy())
    }
    pub(crate) fn origin_is_selectable(
        &self,
        index: usize,
    ) -> bool {
        self.refresh_origin_health(index);
        self.origin_eligible(index)
    }
    pub(crate) fn origin_eligible(
        &self,
        index: usize,
    ) -> bool {
        if !self.origin_eligible_without_priority(index) {
            return false;
        }
        {
            let origin = &self.inner.profile.origins[index];
            if origin.priority > 0
                && let Some(crate::state::SlotRef::Resolution(member)) = origin.slot.0.as_ref()
                && self.inner.profile.origins.iter().enumerate().any(|(index, other)| {
                    other.priority < origin.priority
                        && matches!(other.slot.0.as_ref(), Some(crate::state::SlotRef::Resolution(other)) if member.same_binding(other))
                        && self.origin_eligible_without_priority(index)
                })
            {
                return false;
            }
        }
        true
    }
    fn origin_eligible_without_priority(
        &self,
        index: usize,
    ) -> bool {
        self.inner.profile.origins.get(index).is_some_and(|origin| {
            origin.slot.get().health().is_healthy()
                && self.origin_serving(index) == crate::origin::Serving::Yes
        })
    }
}
pub(crate) mod active;
mod drop;
pub(crate) mod event;
pub(crate) mod fleet;
pub(crate) mod lock;
pub(crate) mod outlier;
pub(crate) mod probe;
pub(crate) mod recovery;
pub(crate) mod tasks;
