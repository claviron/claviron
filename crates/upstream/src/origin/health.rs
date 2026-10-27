//! Reachability and serving are independent axes in the bound state slot.
use std::sync::atomic::Ordering;

use crate::state::layout::OriginSlot;
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    serde::Deserialize,
    serde::Serialize,
)]
#[repr(u8)]
pub enum OriginHealth {
    #[default]
    Healthy = 0,
    Down = 1,
    HalfOpen = 2,
}
impl OriginHealth {
    pub const fn is_healthy(self) -> bool {
        matches!(self, Self::Healthy)
    }
    pub const fn is_down(self) -> bool {
        matches!(self, Self::Down)
    }
    pub(crate) fn decode(value: u8) -> Self {
        match value {
            0 => Self::Healthy,
            2 => Self::HalfOpen,
            _ => Self::Down,
        }
    }
}
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    serde::Deserialize,
    serde::Serialize,
)]
#[repr(u8)]
pub(crate) enum Serving {
    No = 0,
    #[default]
    Yes = 1,
}
// Bit one is an independent fleet outlier deny cause in the existing serving
// byte. Original/older readers admit only raw 1, so both denied encodings remain
// fail-closed without changing the frozen kind-222 geometry. Not operator policy.
const OUTLIER_DENY: u8 = 2;
impl OriginSlot {
    pub(crate) fn outlier_ejected(&self) -> bool {
        self.verdict.load().serving() & OUTLIER_DENY != 0
    }
    pub(crate) fn set_outlier_ejected(
        &self,
        ejected: bool,
    ) {
        if ejected {
            self.verdict.serving_bits(OUTLIER_DENY, 0);
        } else {
            self.verdict.serving_bits(0, OUTLIER_DENY);
        }
    }

    pub(crate) fn health(&self) -> OriginHealth {
        OriginHealth::decode(self.verdict.load().health())
    }
    pub(crate) fn serving(&self) -> Serving {
        if self.verdict.load().serving() == Serving::Yes as u8 {
            Serving::Yes
        } else {
            Serving::No
        }
    }
    pub(crate) fn set_l7_serving(
        &self,
        next: Serving,
    ) {
        // Bit zero retains the original L7 Yes encoding. Preserve independent
        // deny bits so a successful L7 result cannot undo another serving cause.
        match next {
            Serving::Yes => {
                self.verdict.serving_bits(Serving::Yes as u8, 0);
            }
            Serving::No => {
                self.verdict.serving_bits(0, Serving::Yes as u8);
            }
        }
    }
    pub(crate) fn reserve_validator_probe(&self) -> bool {
        let mut before = self.verdict.load();
        loop {
            if before.health() != OriginHealth::Down as u8 {
                return false;
            }
            let after = before
                .with_health(OriginHealth::HalfOpen as u8, before.failures());
            match self.verdict.compare_exchange(before, after) {
                Ok(_) => return true,
                Err(current) => before = current,
            }
        }
    }
    pub(crate) fn cancel_validator_probe(&self) {
        let mut before = self.verdict.load();
        loop {
            if before.health() != OriginHealth::HalfOpen as u8 {
                return;
            }
            let after =
                before.with_health(OriginHealth::Down as u8, before.failures());
            match self.verdict.compare_exchange(before, after) {
                Ok(_) => return,
                Err(current) => before = current,
            }
        }
    }
    pub(crate) fn record_success(&self) -> (OriginHealth, OriginHealth) {
        let mut before = self.verdict.load();
        loop {
            let health = OriginHealth::decode(before.health());
            if health == OriginHealth::Healthy && before.failures() == 0 {
                return (health, health);
            }
            // Publish recovery time before the CAS makes this origin selectable.
            // An ordinary successful request never resets the slow-start clock.
            if health != OriginHealth::Healthy {
                self.healthy_since.store(
                    crate::state::layout::now_millis(),
                    Ordering::Relaxed,
                );
            }
            let after = before.with_health(OriginHealth::Healthy as u8, 0);
            match self.verdict.compare_exchange(before, after) {
                Ok(_) => return (health, OriginHealth::Healthy),
                Err(current) => before = current,
            }
        }
    }
    pub(crate) fn record_origin_reachability_failure(
        &self,
        threshold: u32,
    ) -> (OriginHealth, OriginHealth) {
        let mut before = self.verdict.load();
        loop {
            let health = OriginHealth::decode(before.health());
            let failures = before.failures().saturating_add(1);
            // A failed HalfOpen recovery always returns Down, even when a
            // peer's installed history is below this worker's passive threshold.
            let after_health = if (threshold > 0 && failures >= threshold)
                || health == OriginHealth::HalfOpen
            {
                OriginHealth::Down
            } else {
                health
            };
            let after = before.with_health(after_health as u8, failures);
            match self.verdict.compare_exchange(before, after) {
                Ok(_) => return (health, after_health),
                Err(current) => before = current,
            }
        }
    }
}

#[cfg(test)]
#[path = "../../tests/origin/health.rs"]
mod tests;
