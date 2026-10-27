//! One native-endian CAS over the existing health/flags/failure byte region.
use std::sync::atomic::{AtomicU64, Ordering};

/// Bytes retain the kind-222 layout: health, serving, lifecycle, padding,
/// then a native-endian u32 failure count. Never alias this with narrower atomics.
#[repr(transparent)]
pub(crate) struct Verdict(AtomicU64);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Word(u64);
impl Word {
    pub(crate) fn health(self) -> u8 {
        self.0.to_ne_bytes()[0]
    }
    pub(crate) fn serving(self) -> u8 {
        self.0.to_ne_bytes()[1]
    }
    pub(crate) fn failures(self) -> u32 {
        let bytes = self.0.to_ne_bytes();
        u32::from_ne_bytes([bytes[4], bytes[5], bytes[6], bytes[7]])
    }
    pub(crate) fn with_health(
        self,
        health: u8,
        failures: u32,
    ) -> Self {
        let mut bytes = self.0.to_ne_bytes();
        bytes[0] = health;
        bytes[4..].copy_from_slice(&failures.to_ne_bytes());
        Self(u64::from_ne_bytes(bytes))
    }
}
impl Verdict {
    pub(crate) const fn new() -> Self {
        Self(AtomicU64::new(u64::from_ne_bytes([0, 1, 1, 0, 0, 0, 0, 0])))
    }
    pub(crate) fn load(&self) -> Word {
        Word(self.0.load(Ordering::Acquire))
    }
    pub(crate) fn compare_exchange(
        &self,
        before: Word,
        after: Word,
    ) -> Result<Word, Word> {
        self.0
            .compare_exchange(
                before.0,
                after.0,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .map(Word)
            .map_err(Word)
    }
    pub(crate) fn serving_bits(
        &self,
        set: u8,
        clear: u8,
    ) {
        let mut before = self.load();
        loop {
            let mut bytes = before.0.to_ne_bytes();
            bytes[1] = (bytes[1] | set) & !clear;
            let after = Word(u64::from_ne_bytes(bytes));
            if before == after {
                return;
            }
            match self.compare_exchange(before, after) {
                Ok(_) => return,
                Err(current) => before = current,
            }
        }
    }
    pub(super) fn install(&self) {
        let mut bytes = self.load().0.to_ne_bytes();
        bytes[1] = 1;
        bytes[2] = 1;
        self.0.store(u64::from_ne_bytes(bytes), Ordering::Relaxed);
    }
    pub(super) fn reset(&self) {
        self.0.store(Self::new().0.into_inner(), Ordering::Relaxed);
    }
}

#[cfg(test)]
#[path = "../../tests/state/verdict.rs"]
mod tests;
