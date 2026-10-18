//! Worker-local origin recovery validator.

use std::collections::hash_map::DefaultHasher;
use std::time::Duration;

use tokio::time;

use crate::health::lock::{
    FlagLock, ProbeDuty, ProbeLock, UpstreamRecoveryProbe, during_lease,
};
use crate::health::probe::{ProbeKind, ProbeOutcome};
use crate::proxy::service::UpstreamService;

impl UpstreamService {
    pub(crate) async fn run_origin_validator(
        self,
        origin_index: usize,
    ) {
        let Some(origin) =
            self.inner.profile.origins.get(origin_index).cloned()
        else {
            return;
        };

        let validator = self.inner.profile.health.recovery_probe.clone();
        let max_backoff = validator.max_backoff;
        let mut backoff = max_backoff
            .map_or(validator.initial_backoff, |max| {
                validator.initial_backoff.min(max)
            });
        let mut attempt = 0_u64;

        let mut probe_lock: Option<FlagLock> = None;

        loop {
            if self.is_shutting_down() || !origin.slot.get().available() {
                return;
            }

            if self.origin_is_healthy(origin_index) {
                return;
            }

            let delay = add_jitter(
                backoff,
                validator.jitter,
                max_backoff,
                &self.inner.profile.name,
                &origin.source,
                attempt,
            );
            tokio::select! {
                biased;
                _ = self.wait_shutdown() => {
                    return;
                }
                kept = during_lease(&mut probe_lock, time::sleep(delay), validator.probe_timeout,
                    ProbeDuty::<UpstreamRecoveryProbe>::ttl(&self)) => {
                    if kept.is_none() { probe_lock = None; }
                }
            }

            if self.origin_is_healthy(origin_index)
                || !origin.slot.get().available()
            {
                return;
            }

            let ttl = ProbeDuty::<UpstreamRecoveryProbe>::ttl(&self);
            if probe_lock.as_mut().is_some_and(|lock| !lock.renew(ttl)) {
                // Lost tenures cannot publish health or abandon another owner's
                // reservation. Drop the exact old lease and retry acquisition.
                probe_lock = None;
            }
            if probe_lock.is_none() {
                match ProbeDuty::<UpstreamRecoveryProbe>::enter(
                    &self,
                    origin_index,
                ) {
                    ProbeLock::Local => {}
                    ProbeLock::Acquired(lock) => {
                        // An acquired successor tenure proves no earlier owner
                        // still owns recovery; reclaim its crash-left reservation.
                        origin.slot.get().cancel_validator_probe();
                        probe_lock = Some(lock);
                    }
                    ProbeLock::YieldTo => {
                        attempt = attempt.saturating_add(1);
                        continue;
                    }
                }
            }

            if !self.reserve_validator_probe(origin_index) {
                backoff = next_backoff(backoff, max_backoff);
                attempt = attempt.saturating_add(1);
                continue;
            }

            let Some(_assignment) = origin.slot.get().scope_guard() else {
                return;
            };
            let mut reservation = ProbeReservation {
                slot: origin.slot.get().clone(),
                lock: probe_lock.take(),
                ttl,
                completed: false,
            };
            let result = tokio::select! {
                biased;
                _ = self.wait_shutdown() => {
                    return;
                }
                result = during_lease(&mut reservation.lock, time::timeout(
                    validator.probe_timeout,
                    self.probe_origin_reachability(&origin)
                ), validator.probe_timeout, ttl) => result
            };

            let Some(result) = result else {
                drop(reservation);
                continue;
            };
            let Some(outcome) = self.apply_outcome(
                origin_index,
                ProbeKind::Recovery,
                result,
                &mut reservation.lock,
            ) else {
                drop(reservation);
                continue;
            };
            if outcome == ProbeOutcome::Ok {
                reservation.completed = true;
                tracing::info!(profile = %self.inner.profile.name, origin = %origin.label, attempt, status = "recovered", "upstream_probe");
                return;
            }
            backoff = next_backoff(backoff, max_backoff);

            // Health is now Healthy/Down. Drop the reservation while its exact
            // tenure is still held, then retain that lease for the next backoff.
            reservation.completed = true;
            probe_lock = reservation.lock.take();
            drop(reservation);
            attempt = attempt.saturating_add(1);
        }
    }
}

/// `None` is an explicit choice for no ceiling: the delay keeps doubling
/// (saturating) while the origin stays Down.
fn next_backoff(
    current: Duration,
    max_backoff: Option<Duration>,
) -> Duration {
    let doubled = current.saturating_mul(2);
    max_backoff.map_or(doubled, |max| doubled.min(max))
}

fn cap(
    delay: Duration,
    max_backoff: Option<Duration>,
) -> Duration {
    max_backoff.map_or(delay, |max| delay.min(max))
}

fn add_jitter(
    base: Duration,
    jitter: Duration,
    max_backoff: Option<Duration>,
    profile: &str,
    source: &str,
    attempt: u64,
) -> Duration {
    if jitter.is_zero() {
        return cap(base, max_backoff);
    }

    let jitter_ms = jitter.as_millis().min(u64::MAX as u128) as u64;
    if jitter_ms == 0 {
        return cap(base, max_backoff);
    }

    let mut hasher = DefaultHasher::new();
    profile.hash(&mut hasher);
    source.hash(&mut hasher);
    attempt.hash(&mut hasher);
    let offset =
        Duration::from_millis(hasher.finish() % jitter_ms.saturating_add(1));
    cap(base.saturating_add(offset), max_backoff)
}

use std::hash::{Hash, Hasher};

/// Cancellation abandons only this probe's reservation. Renewing the exact
/// lease fences an expired owner from touching its successor's HalfOpen state.
struct ProbeReservation {
    slot: crate::state::SlotRef,
    lock: Option<FlagLock>,
    ttl: Duration,
    completed: bool,
}
impl ProbeReservation {
    fn owns_duty(&mut self) -> bool {
        self.lock.as_mut().is_none_or(|lock| lock.renew(self.ttl))
    }
}
impl Drop for ProbeReservation {
    fn drop(&mut self) {
        if !self.completed && self.owns_duty() {
            self.slot.cancel_validator_probe();
        }
    }
}
