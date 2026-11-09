//! Process-local FIFO acquisition ownership under creator-slot pressure.

use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::sync::{Mutex, MutexGuard};

#[derive(Default)]
pub(super) struct CreatorAdmission {
    participants: AtomicUsize,
    turn: Mutex<()>,
}

impl CreatorAdmission {
    pub(super) fn contended(&self) -> bool {
        self.participants.load(Ordering::Acquire) != 0
    }

    pub(super) async fn acquire(&self) -> CreatorTurn<'_> {
        self.participants.fetch_add(1, Ordering::AcqRel);
        let registration = Registration(self);
        let guard = self.turn.lock().await;
        CreatorTurn { _guard: guard, _registration: registration }
    }
}

pub(super) struct CreatorTurn<'a> {
    // Unlock before removing the participant: zero must never advertise a
    // free fast path while an inspector still owns the turn.
    _guard: MutexGuard<'a, ()>,
    _registration: Registration<'a>,
}

struct Registration<'a>(&'a CreatorAdmission);

impl Drop for Registration<'_> {
    fn drop(&mut self) {
        self.0.participants.fetch_sub(1, Ordering::Release);
    }
}

#[cfg(test)]
#[path = "../../../tests/link/application/creator.rs"]
mod tests;
