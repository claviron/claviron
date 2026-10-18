//! Deterministic bounded slot transitions; the runtime supplies joined drain
//! evidence. No health table write, recovery probe or health event belongs here.
use std::time::Instant;

use claviron_resolver::{ResolveError, ResolveErrorKind};

use super::{
    Address, AddressAnswer, OnFailure, ReservedOriginId, ResolutionGeneration,
};

#[derive(
    Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize,
)]
#[serde(deny_unknown_fields)]
pub struct Assignment {
    pub id: ReservedOriginId,
    pub address: Address,
    /// Fences the lifetime of this concrete assignment, including retirement.
    pub installed: ResolutionGeneration,
}
/// A runtime may acknowledge this only after all accepted IO for the old
/// assignment has joined. A late receipt cannot release a successor's slot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Retirement(pub Assignment);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Change {
    pub generation: ResolutionGeneration,
    pub activated: Vec<Assignment>,
    pub updated: Vec<Assignment>,
    pub retired: Vec<Retirement>,
}
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize,
)]
#[serde(deny_unknown_fields)]
pub(super) struct Slot {
    pub(super) assignment: Assignment,
    pub(super) retiring: bool,
}

/// Owner-local membership model. Sharing this process-local object is not a
/// substitute for fleet election and an authoritative published answer table.
#[derive(Clone, Debug)]
pub struct ResolvedSet {
    pub(super) first: ReservedOriginId,
    pub(super) slots: Vec<Option<Slot>>,
    pub(super) desired: Vec<Address>,
    pub(super) generation: ResolutionGeneration,
    pub(super) valid_until: Option<Instant>,
    pub(super) stale: bool,
    pub(super) truncated: bool,
}
impl ResolvedSet {
    pub fn new(
        first: ReservedOriginId,
        capacity: usize,
    ) -> Result<Self, ResolveError> {
        if capacity == 0 || capacity > usize::from(u16::MAX) + 1 - first.index()
        {
            return Err(ResolveError::new(
                ResolveErrorKind::Config,
                "reserved slots exceed the nonempty ReservedOriginId domain",
            ));
        }
        Ok(Self {
            first,
            slots: vec![None; capacity],
            desired: Vec::new(),
            generation: ResolutionGeneration::default(),
            valid_until: None,
            stale: false,
            truncated: false,
        })
    }
    pub fn generation(&self) -> ResolutionGeneration {
        self.generation
    }
    pub fn valid_until(&self) -> Option<Instant> {
        self.valid_until
    }
    pub fn stale(&self) -> bool {
        self.stale
    }
    pub fn truncated(&self) -> bool {
        self.truncated
    }
    pub fn active(&self) -> impl Iterator<Item = Assignment> + '_ {
        self.slots
            .iter()
            .flatten()
            .filter(|slot| !slot.retiring)
            .map(|slot| slot.assignment)
    }
    pub fn retiring(&self) -> impl Iterator<Item = Retirement> + '_ {
        self.slots
            .iter()
            .flatten()
            .filter(|slot| slot.retiring)
            .map(|slot| Retirement(slot.assignment))
    }
    /// Addresses waiting for an old assignment's joined drain retain the same
    /// bounded desired set, instead of reusing an occupied ReservedOriginId early.
    pub fn pending(&self) -> impl Iterator<Item = Address> + '_ {
        self.desired.iter().copied().filter(|address| {
            !self.slots.iter().flatten().any(|slot| {
                !slot.retiring
                    && slot.assignment.address.socket == address.socket
            })
        })
    }

    pub fn apply(
        &mut self,
        answer: AddressAnswer,
    ) -> Result<Change, ResolveError> {
        // This public input can come from a custom publisher, not only lookup.
        if answer.addresses.iter().any(|address| address.socket.port() == 0) {
            return Err(ResolveError::new(
                ResolveErrorKind::Backend,
                "resolved endpoint port must be positive",
            ));
        }
        let normalized = if answer.addresses.is_empty() {
            answer
        } else {
            let mut prepared = super::normalize(
                answer.addresses,
                answer.valid_until,
                self.slots.len(),
            )?;
            prepared.truncated |= answer.truncated;
            prepared
        };
        let mut change = self.unchanged();
        if normalized.addresses != self.desired || self.valid_until.is_none() {
            self.advance()?;
            self.desired = normalized.addresses;
            for slot in
                self.slots.iter_mut().flatten().filter(|slot| !slot.retiring)
            {
                match self.desired.iter().find(|address| {
                    address.socket == slot.assignment.address.socket
                }) {
                    None => {
                        slot.retiring = true;
                        change.retired.push(Retirement(slot.assignment));
                    }
                    Some(address) if *address != slot.assignment.address => {
                        slot.assignment.address = *address;
                        change.updated.push(slot.assignment);
                    }
                    _ => {}
                }
            }
            self.activate(&mut change);
        }
        self.valid_until = Some(normalized.valid_until);
        self.truncated = normalized.truncated;
        self.stale = false;
        change.generation = self.generation;
        Ok(change)
    }

    /// Keep transport/backend failures regardless of negative-answer policy.
    /// Returns true only on entry to a stale episode, for one owner warning.
    pub fn fail(
        &mut self,
        kind: ResolveErrorKind,
        policy: OnFailure,
    ) -> Result<(Change, bool), ResolveError> {
        let warn = !self.stale;
        let change = if policy == OnFailure::Empty
            && matches!(
                kind,
                ResolveErrorKind::NxDomain | ResolveErrorKind::NoRecords
            ) {
            self.apply(AddressAnswer {
                addresses: vec![],
                valid_until: self.valid_until.unwrap_or_else(Instant::now),
                truncated: false,
            })?
        } else {
            self.unchanged()
        };
        self.stale = true;
        Ok((change, warn))
    }

    pub fn finish_retirement(
        &mut self,
        receipt: Retirement,
    ) -> Result<Change, ResolveError> {
        let Some(index) = receipt
            .0
            .id
            .index()
            .checked_sub(self.first.index())
            .filter(|index| *index < self.slots.len())
        else {
            return Ok(self.unchanged());
        };
        if !self.slots[index]
            .is_some_and(|slot| slot.retiring && slot.assignment == receipt.0)
        {
            return Ok(self.unchanged());
        }
        // Check before mutation so generation exhaustion leaves the old slot
        // reserved. Never wrap a revision into an older publication's identity.
        self.advance()?;
        self.slots[index] = None;
        let mut change = self.unchanged();
        self.activate(&mut change);
        Ok(change)
    }
    fn unchanged(&self) -> Change {
        Change {
            generation: self.generation,
            activated: vec![],
            updated: vec![],
            retired: vec![],
        }
    }
    fn advance(&mut self) -> Result<(), ResolveError> {
        self.generation.0 =
            self.generation.0.checked_add(1).ok_or_else(|| {
                ResolveError::new(
                    ResolveErrorKind::Backend,
                    "resolution generation exhausted",
                )
            })?;
        Ok(())
    }
    fn activate(
        &mut self,
        change: &mut Change,
    ) {
        for &address in &self.desired {
            if self
                .slots
                .iter()
                .flatten()
                .any(|slot| slot.assignment.address.socket == address.socket)
            {
                continue;
            }
            let Some(index) = self.slots.iter().position(Option::is_none)
            else {
                break;
            };
            let assignment = Assignment {
                id: ReservedOriginId(
                    self.first.0
                        + u16::try_from(index)
                            .expect("reserved ReservedOriginId domain"),
                ),
                address,
                installed: self.generation,
            };
            self.slots[index] = Some(Slot { assignment, retiring: false });
            change.activated.push(assignment);
        }
    }
}
