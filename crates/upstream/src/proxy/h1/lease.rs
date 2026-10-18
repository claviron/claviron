//! Owns the exclusive H1 lease state transitions.

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum H1LeaseState {
    LeasedDirty { reason: DirtyReason },
    BodyOwnedByResponseWrapper,
    ReusableAfterEof,
    Upgraded,
}

use crate::proxy::failure::DirtyReason;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct H1Lease {
    state: H1LeaseState,
}

impl H1Lease {
    pub fn new() -> Self {
        Self {
            state: H1LeaseState::LeasedDirty { reason: DirtyReason::Unknown },
        }
    }

    pub(crate) fn is_reusable(&self) -> bool {
        matches!(self.state, H1LeaseState::ReusableAfterEof)
    }

    pub fn mark_body_owned(&mut self) {
        self.state = H1LeaseState::BodyOwnedByResponseWrapper;
    }

    pub fn mark_reusable(&mut self) {
        self.state = H1LeaseState::ReusableAfterEof;
    }

    pub fn mark_dirty(
        &mut self,
        reason: DirtyReason,
    ) {
        self.state = H1LeaseState::LeasedDirty { reason };
    }

    pub fn into_upgrade(mut self) -> Self {
        self.state = H1LeaseState::Upgraded;
        self
    }
}
