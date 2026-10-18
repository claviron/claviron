//! Apply one verdict only while its exact probe duty is still held.
use std::time::Duration;

use super::{OriginReachabilityFailure, ProbeKind, ProbeOutcome};
use crate::health::lock::{
    FlagLock, ProbeDuty, UpstreamActiveProbe, UpstreamDownProbe,
    UpstreamRecoveryProbe,
};
use crate::proxy::service::UpstreamService;
impl ProbeKind {
    fn ttl(
        self,
        service: &UpstreamService,
    ) -> Duration {
        match self {
            Self::Drop => ProbeDuty::<UpstreamDownProbe>::ttl(service),
            Self::Recovery => ProbeDuty::<UpstreamRecoveryProbe>::ttl(service),
            Self::Active => ProbeDuty::<UpstreamActiveProbe>::ttl(service),
        }
    }
    fn timeout(
        self,
        service: &UpstreamService,
    ) -> Duration {
        let health = &service.inner.profile.health;
        match self {
            Self::Drop => health.drop_probe.probe_timeout,
            Self::Recovery => health.recovery_probe.probe_timeout,
            Self::Active => health.active.probe_timeout,
        }
    }
}
impl UpstreamService {
    pub(crate) fn apply_outcome(
        &self,
        index: usize,
        kind: ProbeKind,
        result: Result<
            Result<(), OriginReachabilityFailure>,
            tokio::time::error::Elapsed,
        >,
        lock: &mut Option<FlagLock>,
    ) -> Option<ProbeOutcome> {
        if self.is_shutting_down() {
            return None;
        }
        let origin = self.inner.profile.origins.get(index)?;
        if !origin.slot.get().available() {
            return None;
        }
        // All three probe paths fence the verdict, not just recovery. An
        // expired/finalized guard grants no authority to publish or count it.
        if lock.as_mut().is_some_and(|guard| !guard.renew(kind.ttl(self))) {
            return None;
        }
        let outcome = match &result {
            Ok(Ok(())) => ProbeOutcome::Ok,
            Ok(Err(_)) => ProbeOutcome::Fail,
            Err(_) => ProbeOutcome::Timeout,
        };
        self.record_probe_result(index, kind, outcome);
        match result {
            Ok(Ok(())) => {
                tracing::trace!(profile = %self.inner.profile.name, origin = %origin.label, source = %origin.source, probe = kind.as_str(), status = outcome.as_str(), "upstream_probe");
                // A drop check only confirms a suspicious driver end; it does
                // not claim another duty's ongoing recovery reservation.
                if kind != ProbeKind::Drop {
                    self.record_origin_success(index);
                }
            }
            Ok(Err(error)) => {
                if kind == ProbeKind::Drop {
                    tracing::warn!(profile = %self.inner.profile.name, origin = %origin.label, source = %origin.source, probe = kind.as_str(), status = outcome.as_str(), cause = error.cause(), %error, "upstream_probe");
                } else {
                    tracing::debug!(profile = %self.inner.profile.name, origin = %origin.label, source = %origin.source, probe = kind.as_str(), status = outcome.as_str(), cause = error.cause(), %error, "upstream_probe");
                }
                self.record_origin_reachability_failure(index);
            }
            Err(_) => {
                if kind == ProbeKind::Active {
                    tracing::debug!(profile = %self.inner.profile.name, origin = %origin.label, source = %origin.source, probe = kind.as_str(), status = outcome.as_str(), timeout_ms = kind.timeout(self).as_millis(), "upstream_probe");
                } else {
                    tracing::warn!(profile = %self.inner.profile.name, origin = %origin.label, source = %origin.source, probe = kind.as_str(), status = outcome.as_str(), timeout_ms = kind.timeout(self).as_millis(), "upstream_probe");
                }
                self.record_origin_reachability_failure(index);
            }
        }
        Some(outcome)
    }
}
