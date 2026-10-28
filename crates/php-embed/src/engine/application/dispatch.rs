//! Published mailboxes borrow execution ownership without retaining a PHP thread.

use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU64, AtomicUsize, Ordering};

use tokio::sync::{Notify, mpsc};
use tokio_util::sync::CancellationToken;

// Readiness is published only after PHP enters its receive loop.
const READY: u8 = 1;
// HTTP and event delivery must claim the same exclusive Zend context.
const BUSY: u8 = 2;
// Retirement permanently excludes this execution generation from selection.
const RETIRING: u8 = 4;
// Native execution has returned; lifecycle may begin its owned thread join.
const EXITED: u8 = 8;

pub(super) struct Control {
    availability: AtomicU8,
    pub served: AtomicU64,
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

    pub fn publish_ready(&self) {
        if self
            .availability
            .compare_exchange(0, READY, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.wake.notify_one();
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
        // One released instance admits one caller; avoid waking the whole queue.
        self.wake.notify_one();
    }

    pub fn complete(
        &self,
        served: bool,
    ) {
        if served {
            self.served.fetch_add(1, Ordering::Relaxed);
        }
        self.release_claim();
    }

    pub fn finished(&self) -> bool {
        self.availability.load(Ordering::Acquire) & EXITED != 0
    }

    pub fn retire(&self) {
        self.availability.fetch_or(RETIRING, Ordering::AcqRel);
        self.cancel.cancel();
    }

    pub fn exited(&self) {
        self.availability.store(RETIRING | EXITED, Ordering::Release);
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

#[cfg(test)]
#[path = "../../../tests/application_dispatch.rs"]
mod tests;
