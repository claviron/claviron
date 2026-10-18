//! One active-generation L7 duty, retained and renewed across its schedule.
use crate::health::lock::{
    ProbeDuty, ProbeLock, UpstreamL7Probe, during_lease,
};
use crate::health::probe::ProbeOutcome;
use crate::origin::Serving;
use crate::proxy::service::UpstreamService;
#[derive(Default)]
pub(crate) struct Streak {
    successes: u32,
    failures: u32,
}
impl UpstreamService {
    pub(crate) async fn run_l7_origin_loop(
        self,
        index: usize,
    ) {
        let policy = &self.inner.profile.health.l7;
        let epoch = self.epoch();
        let mut ticker = tokio::time::interval(policy.interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        ticker.tick().await;
        let mut lock = None;
        let mut streak = Streak::default();
        let ttl = ProbeDuty::<UpstreamL7Probe>::ttl(&self);
        loop {
            tokio::select! { biased; _ = self.wait_shutdown() => return, _ = ticker.tick() => {} }
            if !self.inner.profile.origins[index].slot.get().available() {
                return;
            }
            if !self.origin_is_healthy(index) {
                continue;
            }
            let Some(_assignment) =
                self.inner.profile.origins[index].slot.get().scope_guard()
            else {
                return;
            };
            if lock.is_none() {
                match ProbeDuty::<UpstreamL7Probe>::enter(&self, index) {
                    ProbeLock::Local => {}
                    ProbeLock::Acquired(held) => lock = Some(held),
                    ProbeLock::YieldTo => continue,
                }
            }
            let verdict = tokio::select! { biased; _ = self.wait_shutdown() => return,
            result = during_lease(&mut lock, self.probe_l7(index, &epoch), policy.timeout, ttl) => result };
            let Some(verdict) = verdict else {
                lock.take();
                streak = Streak::default();
                continue;
            };
            self.apply_l7_verdict(index, verdict, &mut streak);
        }
    }
    pub(crate) fn apply_l7_verdict(
        &self,
        index: usize,
        verdict: ProbeOutcome,
        streak: &mut Streak,
    ) {
        if self.is_shutting_down() {
            return;
        }
        self.record_l7_probe_result(index, verdict);
        let policy = &self.inner.profile.health.l7;
        let next = if verdict == ProbeOutcome::Ok {
            streak.failures = 0;
            streak.successes = streak.successes.saturating_add(1);
            (streak.successes >= policy.success_threshold)
                .then_some(Serving::Yes)
        } else {
            streak.successes = 0;
            streak.failures = streak.failures.saturating_add(1);
            (streak.failures >= policy.failure_threshold).then_some(Serving::No)
        };
        if let Some(next) = next {
            let slot = self.inner.profile.origins[index].slot.get();
            let before = slot.serving();
            if !slot.set_l7_serving(next) {
                return;
            }
            self.refresh_origin_health(index);
            let after = slot.serving();
            self.publish_origin_serving_event(index, before, after);
            if before != after {
                tracing::debug!(profile = %self.inner.profile.name, origin = %self.inner.profile.origins[index].label, probe = "l7", status = verdict.as_str(), serving = ?after, "upstream serving changed");
            }
        }
    }
}
