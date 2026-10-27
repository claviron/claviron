//! Semantic slot access. Serving code never dereferences an assignment into
//! retained raw atomics; compound writes stay inside one scoped operation.
use std::sync::atomic::Ordering;

use super::SlotRef;
use crate::origin::{OriginHealth, Serving};
use crate::proxy::failure::RequestFailure;

impl SlotRef {
    pub(crate) fn health(&self) -> OriginHealth {
        self.read(|slot| slot.health()).unwrap_or(OriginHealth::Down)
    }
    pub(crate) fn serving(&self) -> Serving {
        self.read(|slot| slot.serving()).unwrap_or(Serving::No)
    }
    pub(crate) fn outlier_ejected(&self) -> bool {
        self.read(|slot| slot.outlier_ejected()).unwrap_or(true)
    }
    pub(crate) fn set_outlier_ejected(
        &self,
        ejected: bool,
    ) -> bool {
        self.write(|slot| slot.set_outlier_ejected(ejected)).is_some()
    }
    pub(crate) fn set_l7_serving(
        &self,
        next: Serving,
    ) -> bool {
        self.write(|slot| slot.set_l7_serving(next)).is_some()
    }
    pub(crate) fn reserve_validator_probe(&self) -> bool {
        self.write(|slot| slot.reserve_validator_probe()).unwrap_or(false)
    }
    pub(crate) fn cancel_validator_probe(&self) {
        let _ = self.write(|slot| slot.cancel_validator_probe());
    }
    pub(crate) fn record_success_change(
        &self
    ) -> Option<(OriginHealth, OriginHealth)> {
        self.write(|slot| slot.record_success())
    }
    pub(crate) fn record_failure_change(
        &self,
        threshold: u32,
    ) -> Option<(OriginHealth, OriginHealth)> {
        self.write(|slot| slot.record_origin_reachability_failure(threshold))
    }
    pub(crate) fn healthy_since(&self) -> u64 {
        self.read(|slot| slot.healthy_since.load(Ordering::Acquire))
            .unwrap_or(0)
    }
    pub(crate) fn pressure(&self) -> u32 {
        if let Self::Resolution(member) = self {
            return member.pressure();
        }
        self.read(|slot| slot.in_flight.load(Ordering::Acquire))
            .unwrap_or(u32::MAX)
    }
    pub(crate) fn acquire_traffic(&self) -> Result<(), RequestFailure> {
        if matches!(self, Self::Resolution(_)) {
            return Err(RequestFailure::Closed);
        }
        self.write(|slot| {
            // Frozen u32 wire pressure cannot wrap and make a busy origin
            // appear idle. Reusable assignment validation covers the whole CAS.
            let mut before = slot.in_flight.load(Ordering::Acquire);
            loop {
                let after = before
                    .checked_add(1)
                    .ok_or(RequestFailure::CapacityFull)?;
                match slot.in_flight.compare_exchange_weak(
                    before,
                    after,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => return Ok(()),
                    Err(current) => before = current,
                }
            }
        })
        .unwrap_or(Err(RequestFailure::Closed))
    }
    pub(crate) fn release_traffic(&self) {
        if matches!(self, Self::Resolution(_)) {
            return;
        }
        let _ = self.write(|slot| {
            let before = slot.in_flight.fetch_sub(1, Ordering::AcqRel);
            debug_assert!(before > 0);
        });
    }
}
