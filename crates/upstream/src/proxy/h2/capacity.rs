//! Per-connection stream admission and event-driven idle ownership.
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::Notify;
use tokio::time::Instant;

// The high bit fences admission atomically with the count, so idle eviction
// cannot close a stream that raced its zero-count check. Remaining bits are
// the counter's representable range, not a configurable concurrency policy.
const SEALED: usize = 1 << (usize::BITS - 1);

#[derive(Debug)]
pub(super) struct StreamCapacity {
    local: Option<usize>,
    peer: AtomicUsize,
    peer_known: AtomicBool,
    state: AtomicUsize,
    idle_since: Mutex<Option<Instant>>,
    idle_changed: Notify,
    changed: Arc<Notify>,
}
impl StreamCapacity {
    pub(super) fn new(
        local: Option<usize>,
        changed: Arc<Notify>,
    ) -> Self {
        Self {
            local,
            peer: AtomicUsize::new(0),
            peer_known: AtomicBool::new(false),
            state: AtomicUsize::new(0),
            idle_since: Mutex::new(Some(Instant::now())),
            idle_changed: Notify::new(),
            changed,
        }
    }
    pub(super) fn limit(&self) -> Option<usize> {
        if !self.peer_known.load(Ordering::Acquire) {
            return self.local;
        }
        let peer = self.peer.load(Ordering::Acquire);
        Some(self.local.unwrap_or(peer).min(peer))
    }
    pub(super) fn pending_peer(&self) -> bool {
        self.state.load(Ordering::Acquire) & SEALED == 0
            && !self.peer_known.load(Ordering::Acquire)
    }
    pub(super) fn update_peer(
        &self,
        peer: usize,
    ) {
        // Only this connection's driver publishes peer SETTINGS. Avoid an
        // RMW and a wake when an ordinary IO poll leaves the limit unchanged.
        if self.peer_known.load(Ordering::Acquire)
            && self.peer.load(Ordering::Relaxed) == peer
        {
            return;
        }
        self.peer.store(peer, Ordering::Release);
        self.peer_known.store(true, Ordering::Release);
        self.changed.notify_waiters();
    }
    pub(super) fn used(&self) -> usize {
        self.state.load(Ordering::Acquire) & !SEALED
    }
    pub(super) fn has_room(&self) -> bool {
        self.state.load(Ordering::Acquire) & SEALED == 0
            && self.peer_known.load(Ordering::Acquire)
            && self.used() < self.limit().unwrap_or(0)
    }
    pub(super) fn try_reserve(&self) -> bool {
        if !self.peer_known.load(Ordering::Acquire) {
            return false;
        }
        let limit = self.limit().unwrap_or(0).min(SEALED - 1);
        let Ok(before) = self.state.try_update(
            Ordering::AcqRel,
            Ordering::Acquire,
            |state| (state & SEALED == 0 && state < limit).then(|| state + 1),
        ) else {
            return false;
        };
        if before == 0 {
            *self.idle_since.lock().unwrap_or_else(|p| p.into_inner()) = None;
            self.idle_changed.notify_one();
        }
        true
    }
    /// The caller releases only after both response and request writer finish.
    pub(super) fn release(&self) -> bool {
        let idle = self.state.fetch_sub(1, Ordering::AcqRel) & !SEALED == 1;
        if idle {
            let mut since =
                self.idle_since.lock().unwrap_or_else(|p| p.into_inner());
            if self.used() == 0 {
                *since = Some(Instant::now());
            }
            self.idle_changed.notify_one();
        }
        self.changed.notify_one();
        idle
    }
    pub(super) fn seal(&self) {
        self.state.fetch_or(SEALED, Ordering::AcqRel);
        self.changed.notify_waiters();
    }
    pub(super) fn seal_if_idle(&self) -> bool {
        match self.state.compare_exchange(
            0,
            SEALED,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => true,
            Err(state) => state == SEALED,
        }
    }
    fn idle_deadline(
        &self,
        timeout: Duration,
    ) -> Option<Instant> {
        self.idle_since
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .and_then(|since| since.checked_add(timeout))
    }
    pub(super) async fn wait_idle(
        &self,
        timeout: Duration,
    ) {
        loop {
            let changed = self.idle_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if let Some(deadline) = self.idle_deadline(timeout) {
                tokio::select! {
                    _ = &mut changed => {},
                    _ = tokio::time::sleep_until(deadline) => {
                        let expired = {
                            let since = self.idle_since.lock().unwrap_or_else(|p| p.into_inner());
                            since.and_then(|since| since.checked_add(timeout)) == Some(deadline)
                                && self.seal_if_idle()
                        };
                        if expired { return; }
                        changed.await;
                    }
                }
            } else {
                changed.await;
            }
        }
    }
}

#[cfg(test)]
#[path = "../../../tests/proxy/h2/capacity.rs"]
mod tests;
