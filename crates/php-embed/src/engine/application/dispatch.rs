//! Published mailboxes borrow execution ownership without retaining a PHP thread.

use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use tokio::sync::{Notify, mpsc};
use tokio_util::sync::CancellationToken;

// Readiness is published only after PHP enters its receive loop.
const READY: u8 = 1;
// HTTP and event delivery must claim the same exclusive Zend context.
const BUSY: u8 = 2;
// Retirement permanently excludes this execution generation from selection.
const RETIRING: u8 = 4;

pub(super) struct Control {
    availability: AtomicU8,
    pub served: AtomicU64,
    epoch: Instant,
    idle_since_ns: AtomicU64,
    pub wake: Arc<Notify>,
    pub maintenance: Arc<Notify>,
    pub cancel: CancellationToken,
}

impl Control {
    pub fn new(
        wake: Arc<Notify>,
        maintenance: Arc<Notify>,
    ) -> Self {
        Self {
            availability: AtomicU8::new(0),
            served: AtomicU64::new(0),
            epoch: Instant::now(),
            idle_since_ns: AtomicU64::new(0),
            wake,
            maintenance,
            cancel: CancellationToken::new(),
        }
    }

    pub fn ready(&self) -> bool {
        self.availability.load(Ordering::Acquire) & (READY | RETIRING) == READY
    }

    pub fn busy(&self) -> bool {
        self.availability.load(Ordering::Acquire) & BUSY != 0
    }

    pub fn starting_or_idle(&self) -> bool {
        self.availability.load(Ordering::Acquire) & (BUSY | RETIRING) == 0
    }

    pub fn publish_ready(&self) {
        if self
            .availability
            .compare_exchange(0, READY, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.wake.notify_waiters();
        }
    }

    pub fn claim(&self) -> bool {
        self.availability
            .compare_exchange(
                READY,
                READY | BUSY,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    pub fn release_claim(&self) {
        self.availability.fetch_and(!BUSY, Ordering::Release);
        self.wake.notify_waiters();
    }

    pub fn complete(
        &self,
        served: bool,
    ) {
        if served {
            self.served.fetch_add(1, Ordering::Relaxed);
        }
        self.idle_since_ns.store(self.elapsed_ns(), Ordering::Release);
        self.release_claim();
    }

    pub fn idle_for(&self) -> Duration {
        Duration::from_nanos(
            self.elapsed_ns()
                .saturating_sub(self.idle_since_ns.load(Ordering::Acquire)),
        )
    }

    fn elapsed_ns(&self) -> u64 {
        self.epoch.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64
    }

    /// Idle retirement and request admission have one atomic winner.
    pub fn retire_idle(
        &self,
        minimum_idle: Option<Duration>,
    ) -> bool {
        if !self.claim() {
            return false;
        }
        // A handler may have completed after maintenance read its idle age.
        // Claim first, then validate the timestamp published by that completion.
        if minimum_idle.is_some_and(|minimum| self.idle_for() < minimum) {
            self.release_claim();
            return false;
        }
        self.availability.store(RETIRING, Ordering::Release);
        true
    }

    pub fn retire(&self) {
        self.availability.fetch_or(RETIRING, Ordering::AcqRel);
        self.cancel.cancel();
    }

    pub fn exited(&self) {
        self.availability.store(RETIRING, Ordering::Release);
        self.wake.notify_waiters();
        self.maintenance.notify_one();
    }
}

pub(super) struct Mailbox<T> {
    sender: mpsc::WeakSender<T>,
    pub control: Arc<Control>,
}

impl<T> Mailbox<T> {
    pub fn new(
        sender: &mpsc::Sender<T>,
        control: Arc<Control>,
    ) -> Self {
        Self { sender: sender.downgrade(), control }
    }

    /// No strong sender survives this call, including on a failed reservation.
    pub fn try_send(
        &self,
        value: &mut Option<T>,
    ) -> bool {
        if !self.control.claim() {
            return false;
        }
        if let Some(sender) = self.sender.upgrade() {
            match sender.try_send(value.take().expect("pending dispatch value"))
            {
                Ok(()) => return true,
                Err(error) => *value = Some(error.into_inner()),
            }
        }
        self.control.release_claim();
        false
    }
}

pub(super) fn send_available<T>(
    mailboxes: &[Mailbox<T>],
    next: &AtomicUsize,
    value: &mut Option<T>,
) -> bool {
    if mailboxes.is_empty() {
        return false;
    }
    let start = next.fetch_add(1, Ordering::Relaxed) % mailboxes.len();
    (0..mailboxes.len()).any(|offset| {
        mailboxes[(start + offset) % mailboxes.len()].try_send(value)
    })
}

/// Counts only dispatchers waiting for an instance, including cancelled callers.
pub(super) struct Waiting<'a>(&'a AtomicUsize);

impl<'a> Waiting<'a> {
    pub fn enter(count: &'a AtomicUsize) -> Self {
        count.fetch_add(1, Ordering::AcqRel);
        Self(count)
    }
}

impl Drop for Waiting<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

#[cfg(test)]
#[path = "../../../tests/application_dispatch.rs"]
mod tests;
