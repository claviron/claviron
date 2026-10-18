//! Live request pressure; idle/warm sockets are not traffic.

use super::Health;
use crate::config::profile::balance::Scope;
use crate::origin::OriginId;
use crate::proxy::failure::RequestFailure;
use crate::state::SlotRef;
pub(super) fn load(
    health: &Health<'_>,
    id: OriginId,
    scope: Scope,
) -> usize {
    match scope {
        Scope::Worker => health.pools.in_flight(id),
        Scope::Fleet => health.service.inner.profile.origins[id.index()]
            .slot
            .get()
            .pressure() as usize,
    }
}
/// Exactly one live request reservation. The slot outlives reload and peer misses.
#[derive(Debug)]
pub(crate) struct Traffic(Reservation);
#[derive(Debug)]
enum Reservation {
    Resident(SlotRef),
    Named(crate::resolve::table::membership::MemberTraffic),
}
impl Traffic {
    pub(crate) fn acquire(slot: &SlotRef) -> Result<Self, RequestFailure> {
        if let SlotRef::Resolution(member) = slot {
            return Ok(Self(Reservation::Named(member.traffic()?)));
        }
        slot.acquire_traffic()?;
        Ok(Self(Reservation::Resident(slot.clone())))
    }
}
impl Drop for Traffic {
    fn drop(&mut self) {
        match &self.0 {
            Reservation::Resident(slot) => slot.release_traffic(),
            Reservation::Named(traffic) => {
                let _ = traffic;
            }
        }
    }
}
