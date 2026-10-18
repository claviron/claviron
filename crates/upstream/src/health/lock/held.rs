//! Exact-tenure guards share a release point with provider finalization.
//! Weak tracking covers every config generation without retaining idle guards.
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use claviron_orbit::lock::{Lock, LockLease, OrbitLock};
use orbitive::lock::Result;

use super::UpstreamLock;
use crate::proxy::service::UpstreamService;
#[derive(Default)]
pub(super) struct HeldLeases {
    guards: Mutex<Vec<Weak<HeldDuty>>>,
}
impl HeldLeases {
    pub(super) fn acquire(
        &self,
        locks: OrbitLock,
        lease: LockLease,
        vacancy: Option<Vacancy>,
    ) -> FlagLock {
        let duty = Arc::new(HeldDuty {
            lock: Mutex::new(Some(Lock::new(locks, lease))),
            vacancy,
        });
        let mut guards = self.guards.lock().unwrap_or_else(|p| p.into_inner());
        guards.retain(|guard| guard.strong_count() != 0);
        guards.push(Arc::downgrade(&duty));
        FlagLock { duty }
    }
    pub(super) fn release_all(&self) -> Result<()> {
        let guards = self
            .guards
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .filter_map(Weak::upgrade)
            .collect::<Vec<_>>();
        // Attempt every held tenure even when one backing operation fails.
        let mut failed = None;
        for guard in guards {
            if let Err(error) = guard.release() {
                failed.get_or_insert(error);
            }
        }
        match failed {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}
pub(crate) struct FlagLock {
    duty: Arc<HeldDuty>,
}
impl FlagLock {
    pub(crate) fn renew(
        &mut self,
        ttl: Duration,
    ) -> bool {
        let mut held = self.duty.lock.lock().unwrap_or_else(|p| p.into_inner());
        let Some(lock) = held.as_mut() else {
            return false;
        };
        match lock.renew(ttl) {
            Ok(owned) => owned,
            Err(error) => {
                tracing::warn!(%error, "upstream probe lease renewal failed");
                false
            }
        }
    }
}
struct HeldDuty {
    lock: Mutex<Option<Lock>>,
    vacancy: Option<Vacancy>,
}
impl HeldDuty {
    fn release(&self) -> Result<()> {
        let lock = self.lock.lock().unwrap_or_else(|p| p.into_inner()).take();
        if let Some(lock) = lock
            && lock.release()?
            && let Some(vacancy) = &self.vacancy
        {
            vacancy.publish();
        }
        Ok(())
    }
}
impl Drop for HeldDuty {
    fn drop(&mut self) {
        if let Err(error) = self.release() {
            tracing::warn!(%error, "upstream probe lease release failed");
        }
    }
}
/// Only exact, successful release may advertise a vacant recovery duty.
/// Events remain wake-ups; the table and next owner's lease decide authority.
pub(super) struct Vacancy {
    lock: UpstreamLock,
    events: Option<claviron_runtime::events::ProcessEventBus>,
    profile: String,
    origin: crate::origin::Origin,
}
impl Vacancy {
    pub(super) fn new(
        service: &UpstreamService,
        index: usize,
    ) -> Self {
        Self {
            lock: service
                .inner
                .runtime
                .lock
                .as_ref()
                .expect("fleet duty")
                .clone(),
            events: service.inner.runtime.events.clone(),
            profile: service.inner.profile.name.clone(),
            origin: service.inner.profile.origins[index].clone(),
        }
    }
    fn publish(&self) {
        if self.origin.slot.get().health().is_healthy() {
            return;
        }
        let Some(target_owner) = self.lock.next_owner() else {
            return;
        };
        let Some(events) = &self.events else {
            return;
        };
        events.emit(crate::health::event::RecoveryVacatedEvent {
            profile: self.profile.clone(),
            origin_name: Some(self.origin.label.to_string()),
            origin_source: self.origin.source.to_string(),
            previous_owner: self.lock.owner().to_owned(),
            target_owner,
        });
    }
}
