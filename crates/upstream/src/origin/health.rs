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
        self.serving.load(Ordering::Acquire) & OUTLIER_DENY != 0
    }
    pub(crate) fn set_outlier_ejected(
        &self,
        ejected: bool,
    ) {
        if ejected {
            self.serving.fetch_or(OUTLIER_DENY, Ordering::AcqRel);
        } else {
            self.serving.fetch_and(!OUTLIER_DENY, Ordering::AcqRel);
        }
    }

    pub(crate) fn health(&self) -> OriginHealth {
        OriginHealth::decode(self.health.load(Ordering::Acquire))
    }
    pub(crate) fn serving(&self) -> Serving {
        if self.serving.load(Ordering::Acquire) == Serving::Yes as u8 {
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
                self.serving.fetch_or(Serving::Yes as u8, Ordering::AcqRel);
            }
            Serving::No => {
                self.serving.fetch_and(!(Serving::Yes as u8), Ordering::AcqRel);
            }
        }
    }
    pub(crate) fn reserve_validator_probe(&self) -> bool {
        self.health
            .compare_exchange(
                OriginHealth::Down as u8,
                OriginHealth::HalfOpen as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }
    pub(crate) fn record_success(&self) {
        self.failures.store(0, Ordering::Relaxed);
        // Publish the recovery timestamp before Healthy makes the origin selectable.
        // Ordinary successful requests must not restart the slow-start clock.
        let before = self.health.load(Ordering::Acquire);
        if before != OriginHealth::Healthy as u8 {
            self.healthy_since
                .store(crate::state::layout::now_millis(), Ordering::Relaxed);
        }
        let _ = self.health.compare_exchange(
            before,
            OriginHealth::Healthy as u8,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }
    pub(crate) fn mark_down(&self) {
        self.failures.fetch_max(1, Ordering::Relaxed);
        self.health.store(OriginHealth::Down as u8, Ordering::Release);
    }
    pub(crate) fn record_origin_reachability_failure(
        &self,
        threshold: u32,
    ) {
        let mut before = self.failures.load(Ordering::Relaxed);
        loop {
            match self.failures.compare_exchange_weak(
                before,
                before.saturating_add(1),
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(current) => before = current,
            }
        }
        // HalfOpen was already Down before reservation. A failed recovery
        // cannot make it Healthy merely because this worker's passive threshold
        // is higher than the failure history installed by a peer.
        if (threshold > 0 && before.saturating_add(1) >= threshold)
            || self.health() == OriginHealth::HalfOpen
        {
            self.mark_down();
        }
    }
}
