//! The same slot layout without a FleetProvider.
use super::layout::OriginSlot;
pub(super) fn slots(capacity: usize) -> Box<[OriginSlot]> {
    (0..capacity).map(|_| OriginSlot::empty()).collect()
}
