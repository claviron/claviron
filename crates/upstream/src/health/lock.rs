//! Orbit-backed locks for upstream background probes.

use std::sync::Arc;
use std::time::Duration;
mod held;
use claviron_orbit::lock::{LockAcquire, LockOwner, LockType, OrbitLock};
pub(crate) use held::FlagLock;
use held::{HeldLeases, Vacancy};

use crate::proxy::service::UpstreamService;

#[derive(Clone)]
pub(crate) struct UpstreamLock {
    locks: OrbitLock,
    owner: String,
    state: claviron_runtime::AppState,
    held: Arc<HeldLeases>,
}

impl std::fmt::Debug for UpstreamLock {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        f.debug_struct("UpstreamLock")
            .field("owner", &self.owner)
            .finish_non_exhaustive()
    }
}

impl UpstreamLock {
    pub(crate) fn new(
        locks: OrbitLock,
        owner: impl Into<String>,
        state: claviron_runtime::AppState,
    ) -> Self {
        Self {
            locks,
            owner: owner.into(),
            state,
            held: Arc::new(HeldLeases::default()),
        }
    }

    /// A policy ban is a bounded durable lease, not an executing probe duty.
    /// Retain the raw tenure until its configured TTL expires: process/profile
    /// stop must not release it and silently restore an ejected origin.
    pub(crate) fn begin_ejection(
        &self,
        key: &claviron_orbit::lock::LockKey,
        duration: Duration,
    ) -> orbitive::lock::Result<bool> {
        Ok(matches!(
            self.locks.try_acquire(
                key,
                &LockOwner::from(self.owner.as_str()),
                duration
            )?,
            LockAcquire::Acquired(_)
        ))
    }
    pub(crate) fn ejection_current(
        &self,
        key: &claviron_orbit::lock::LockKey,
    ) -> orbitive::lock::Result<bool> {
        Ok(self.locks.current(key)?.is_some())
    }
    pub(crate) fn release_held(&self) -> orbitive::lock::Result<()> {
        self.held.release_all()
    }
    pub(crate) fn owner(&self) -> &str {
        &self.owner
    }

    pub(crate) fn next_owner(&self) -> Option<String> {
        let members =
            claviron_orbit::service::current_members(&self.state).ok()?;
        next_owner(&self.owner, &members)
    }
}

pub(crate) struct UpstreamDownProbe;

impl LockType for UpstreamDownProbe {
    const NAMESPACE: &'static str = "claviron.upstream.down";
}

pub(crate) struct UpstreamRecoveryProbe;

impl LockType for UpstreamRecoveryProbe {
    const NAMESPACE: &'static str = "claviron.upstream.recovery";
}

pub(crate) struct UpstreamActiveProbe;

impl LockType for UpstreamActiveProbe {
    const NAMESPACE: &'static str = "claviron.upstream.active";
}

pub(crate) struct UpstreamOutlier;
impl LockType for UpstreamOutlier {
    const NAMESPACE: &'static str = "claviron.upstream.outlier";
}
// This ordinal indexes the boot-prepared policy key, not a limit or cadence.
pub(crate) const OUTLIER_LOCK_INDEX: usize = 4;

pub(crate) struct UpstreamL7Probe;
impl LockType for UpstreamL7Probe {
    const NAMESPACE: &'static str = "claviron.upstream.active.l7";
}
impl ProbeKind for UpstreamL7Probe {
    const INDEX: usize = 3;
    const NAME: &'static str = "l7";
    fn ttl(health: &crate::config::profile::health::HealthConfig) -> Duration {
        health.l7.interval.saturating_add(health.l7.timeout.saturating_mul(2))
    }
}
pub(crate) enum ProbeLock {
    Local,
    Acquired(FlagLock),
    YieldTo,
}

pub(crate) trait ProbeKind: LockType {
    const INDEX: usize;
    const NAME: &'static str;
    fn ttl(health: &crate::config::profile::health::HealthConfig) -> Duration;
}
impl ProbeKind for UpstreamDownProbe {
    const INDEX: usize = 0;
    const NAME: &'static str = "drop";
    fn ttl(health: &crate::config::profile::health::HealthConfig) -> Duration {
        health.drop_probe.probe_timeout.saturating_mul(2)
    }
}
impl ProbeKind for UpstreamRecoveryProbe {
    const INDEX: usize = 1;
    const NAME: &'static str = "recovery";
    fn ttl(health: &crate::config::profile::health::HealthConfig) -> Duration {
        health.recovery_probe.max_ttl
    }
}
impl ProbeKind for UpstreamActiveProbe {
    const INDEX: usize = 2;
    const NAME: &'static str = "active";
    fn ttl(health: &crate::config::profile::health::HealthConfig) -> Duration {
        health
            .active
            .interval
            .saturating_add(health.active.probe_timeout.saturating_mul(2))
    }
}
/// One acquisition path, indexed into the keys prepared at config compilation.
pub(crate) struct ProbeDuty<K>(std::marker::PhantomData<K>);
impl<K: ProbeKind> ProbeDuty<K> {
    pub(crate) fn ttl(service: &UpstreamService) -> Duration {
        K::ttl(&service.inner.profile.health)
    }
    pub(crate) fn enter(
        service: &UpstreamService,
        index: usize,
    ) -> ProbeLock {
        if !service.inner.runtime.serving_role || service.is_shutting_down() {
            return ProbeLock::YieldTo;
        }
        let Some(origin) = service.inner.profile.origins.get(index) else {
            return ProbeLock::YieldTo;
        };
        let Some(lock) = &service.inner.runtime.lock else {
            return ProbeLock::Local;
        };
        let ttl = Self::ttl(service);
        match lock.locks.try_acquire(
            &origin.lock[K::INDEX],
            &LockOwner::from(lock.owner.as_str()),
            ttl,
        ) {
            Ok(LockAcquire::Acquired(lease)) => {
                tracing::debug!(profile = %service.inner.profile.name, origin = %origin.label, probe = K::NAME, status = "acquired", "upstream_lock");
                let vacancy =
                    (K::INDEX == 1).then(|| Vacancy::new(service, index));
                ProbeLock::Acquired(lock.held.acquire(
                    lock.locks.clone(),
                    lease,
                    vacancy,
                ))
            }
            // Owner identity does not authorize cloning an exact-tenure guard.
            Ok(LockAcquire::Occupied(_)) => ProbeLock::YieldTo,
            Err(error) => {
                tracing::warn!(profile = %service.inner.profile.name, origin = %origin.label, probe = K::NAME, %error, "upstream probe lock failed");
                // A failed fleet capability is not permission to probe locally.
                ProbeLock::YieldTo
            }
        }
    }
}
/// Poll a bounded operation while renewing its exact recovery tenure. None
/// means the duty was lost; the caller discards the pending probe verdict.
pub(crate) async fn during_lease<T>(
    lock: &mut Option<FlagLock>,
    future: impl Future<Output = T>,
    probe_timeout: Duration,
    ttl: Duration,
) -> Option<T> {
    let Some(lock) = lock.as_mut() else {
        return Some(future.await);
    };
    if !lock.renew(ttl) {
        return None;
    }
    // Half the lease is a safety bound, not another cadence setting: renewing
    // no later than this keeps a short configured TTL alive during long probes.
    let cadence = probe_timeout.min(ttl / 2);
    let mut ticker = tokio::time::interval_at(
        tokio::time::Instant::now() + cadence,
        cadence,
    );
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    tokio::pin!(future);
    loop {
        tokio::select! {
            biased;
            _ = ticker.tick() => { if !lock.renew(ttl) { return None; } }
            output = &mut future => return lock.renew(ttl).then_some(output),
        }
    }
}

fn next_owner(
    owner: &str,
    members: &[claviron_orbit::service::FleetMember],
) -> Option<String> {
    let (prefix, id) = owner.rsplit_once(':')?;
    if prefix != "node" {
        return None;
    }
    let id = id.parse::<u16>().ok()?;
    let mut peers = members
        .iter()
        .filter(|member| {
            member.role == claviron_orbit::service::FleetMemberRole::Worker
                && member.node_id != id
        })
        .map(|member| member.node_id)
        .collect::<Vec<_>>();
    peers.sort_unstable();
    let next = peers
        .iter()
        .copied()
        .find(|peer| *peer > id)
        .or_else(|| peers.first().copied())?;
    Some(format!("{prefix}:{next}"))
}
