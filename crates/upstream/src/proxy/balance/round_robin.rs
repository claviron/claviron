//! Worker-local round-robin ordering; configured origin indices never move.
use std::sync::atomic::{AtomicUsize, Ordering};
#[derive(Debug, Default)]
pub(crate) struct RoundRobinCursor {
    next: AtomicUsize,
}
impl RoundRobinCursor {
    pub(crate) fn next_index(
        &self,
        len: usize,
    ) -> Option<usize> {
        if len == 0 {
            return None;
        }
        Some(self.next.fetch_add(1, Ordering::Relaxed) % len)
    }
    pub(crate) fn reset(&self) {
        self.next.store(0, Ordering::Relaxed);
    }
}
