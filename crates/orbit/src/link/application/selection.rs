//! Per-target selection state. Clones of a prepared target share its turn;
//! unrelated routes do not advance each other's round-robin cursor.

use std::sync::atomic::{AtomicUsize, Ordering};

use super::LinkPolicy;

pub(super) struct Selection {
    pub policy: LinkPolicy,
    turn: AtomicUsize,
}

impl Selection {
    pub fn new(
        policy: LinkPolicy,
        sender: usize,
    ) -> Self {
        Self { policy, turn: AtomicUsize::new(sender) }
    }

    /// One turn per selection, with a runnable pass before queueing. A failed
    /// reservation can race another sender, so continue through this snapshot;
    /// never retry an exchange that has already been published.
    pub fn round_robin<T, E>(
        &self,
        lanes: &[usize],
        mut runs_now: impl FnMut(usize) -> bool,
        mut reserve: impl FnMut(usize) -> Result<Option<T>, E>,
        pressure: impl FnOnce(),
    ) -> Result<Option<T>, E> {
        if lanes.is_empty() {
            return Ok(None);
        }
        let start = self.turn.fetch_add(1, Ordering::Relaxed) % lanes.len();
        let order = lanes[start..].iter().chain(lanes[..start].iter());
        for &lane in order.clone() {
            if runs_now(lane)
                && let Some(chosen) = reserve(lane)?
            {
                return Ok(Some(chosen));
            }
        }
        pressure();
        for &lane in order {
            if let Some(chosen) = reserve(lane)? {
                return Ok(Some(chosen));
            }
        }
        Ok(None)
    }
}

#[cfg(test)]
#[path = "../../../tests/link/application/selection.rs"]
mod tests;
