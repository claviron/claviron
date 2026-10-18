//! Smooth weighted RR; effective weights share the fleet Healthy clock.
use std::sync::Mutex;

use super::Health;
use crate::config::profile::balance::SlowStartConfig;
use crate::origin::Origin;
// Percent scale retains sub-unit weights during slow start. The meaning of
// 100 percent is an algorithm invariant, not a capacity or timeout setting.
const FULL: u64 = 100;
pub(super) fn effective(
    origin: &Origin,
    slow: &SlowStartConfig,
    now: u64,
) -> u64 {
    let full = u64::from(origin.weight) * FULL;
    if slow.window.is_zero() {
        return full;
    }
    let elapsed = now.saturating_sub(origin.slot.get().healthy_since());
    let elapsed = u128::from(elapsed) * 1_000_000;
    let window = slow.window.as_nanos();
    if elapsed >= window {
        return full;
    }
    let initial = u64::from(origin.weight) * u64::from(slow.initial_percent);
    initial + ((u128::from(full - initial) * elapsed) / window) as u64
}
pub(super) struct Smooth(Mutex<Box<[i64]>>);
impl Smooth {
    pub(super) fn new(len: usize) -> Self {
        Self(Mutex::new(vec![0; len].into_boxed_slice()))
    }
    pub(super) fn reset(&self) {
        self.0.lock().unwrap_or_else(|p| p.into_inner()).fill(0);
    }
    pub(super) fn choose(
        &self,
        health: &Health<'_>,
        start: usize,
        slow: &SlowStartConfig,
        now: u64,
    ) -> Option<usize> {
        let origins = &health.service.inner.profile.origins;
        let mut current = self.0.lock().unwrap_or_else(|p| p.into_inner());
        let mut total = 0i64;
        let mut best = None;
        for offset in 0..origins.len() {
            let index = (start + offset) % origins.len();
            if !health.selectable(index) {
                current[index] = 0;
                continue;
            }
            let weight = effective(&origins[index], slow, now) as i64;
            total += weight;
            current[index] = current[index].saturating_add(weight);
            if best.is_none_or(|before| current[index] > current[before]) {
                best = Some(index);
            }
        }
        let best = best?;
        current[best] = current[best].saturating_sub(total);
        Some(best)
    }
}
