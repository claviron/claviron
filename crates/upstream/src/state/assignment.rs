//! Bounded reusable health for one local runtime. The scope gate protects a
//! physical reset; native owner receipts independently prove joined outbound IO.
//! This is not fleet registration or authoritative DNS snapshot publication.
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

use super::{OriginSlot, SlotRef, StateSlot, UpstreamStateTable};
use crate::origin::{Origin, ReservedOriginId};
use crate::resolve::{Assignment, Retirement};

// The high bit closes admission while the low bits count scopes. Separating
// closure from count prevents a reset racing an already admitted atomic write;
// this representation fixes the guard algorithm, not an operational limit.
const CLOSED: u64 = 1 << 63;

#[derive(Debug, thiserror::Error)]
pub enum MembershipStateError {
    #[error("local membership state requires a fleet-less memory runtime")]
    FleetUnavailable,
    #[error("invalid canonical membership state range")]
    Range,
    #[error("membership assignment does not match the concrete origin")]
    Address,
    #[error("membership assignment is stale, retiring or still owned")]
    Busy,
}
struct Cell {
    state: OriginSlot,
    stamp: AtomicU64,
    scopes: AtomicU64,
    owners: AtomicU32,
    assignment: Mutex<Option<Assignment>>,
}
/// Local named health has a fixed canonical reservation range. Retain this
/// capability across answer changes; constructing it does not start any work.
/// Resident literal kind-222 slots are never reset or reclaimed by this table.
#[derive(Clone)]
pub struct LocalMembershipState {
    inner: Arc<Storage>,
}
struct Storage {
    parent: Weak<UpstreamStateTable>,
    first: ReservedOriginId,
    cells: Box<[Cell]>,
}
impl std::fmt::Debug for LocalMembershipState {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        f.debug_struct("LocalMembershipState")
            .field("first", &self.inner.first)
            .field("capacity", &self.inner.cells.len())
            .finish_non_exhaustive()
    }
}
impl crate::proxy::service::UpstreamService {
    /// Reserve reusable named health for this fleet-less runtime. This does not
    /// enable named file configuration or replace fleet retirement authority.
    pub fn local_membership_state(
        &self,
        first: ReservedOriginId,
        capacity: usize,
    ) -> Result<LocalMembershipState, MembershipStateError> {
        if !self.inner.runtime.state_table.is_memory()
            || self.inner.runtime.lock.is_some()
        {
            return Err(MembershipStateError::FleetUnavailable);
        }
        if capacity == 0 || capacity > usize::from(u16::MAX) + 1 - first.index()
        {
            return Err(MembershipStateError::Range);
        }
        Ok(LocalMembershipState {
            inner: Arc::new(Storage {
                parent: Arc::downgrade(&self.inner.runtime.state_table),
                first,
                cells: (0..capacity)
                    .map(|_| Cell {
                        state: OriginSlot::empty(),
                        stamp: AtomicU64::new(0),
                        scopes: AtomicU64::new(CLOSED),
                        owners: AtomicU32::new(0),
                        assignment: Mutex::new(None),
                    })
                    .collect(),
            }),
        })
    }
}
impl LocalMembershipState {
    fn index(
        &self,
        id: ReservedOriginId,
    ) -> Result<usize, MembershipStateError> {
        id.index()
            .checked_sub(self.inner.first.index())
            .filter(|index| *index < self.inner.cells.len())
            .ok_or(MembershipStateError::Range)
    }
    /// Bind a prepared concrete origin. Reassignment needs a newer stamp, a
    /// retired slot, zero admitted scopes and every registered native owner
    /// joined. A failed bind leaves the old assignment and state unchanged.
    pub fn bind(
        &self,
        origin: &Origin,
        assignment: Assignment,
    ) -> Result<Origin, MembershipStateError> {
        let socket = match origin.connect {
            crate::origin::ConnectTarget::Tcp(socket) => socket,
            crate::origin::ConnectTarget::Tls(socket) => socket,
            _ => return Err(MembershipStateError::Address),
        };
        if socket != assignment.address.socket || assignment.installed.0 == 0 {
            return Err(MembershipStateError::Address);
        }
        let index = self.index(assignment.id)?;
        let cell = &self.inner.cells[index];
        let mut current =
            cell.assignment.lock().unwrap_or_else(|p| p.into_inner());
        if current.is_some_and(|old| {
            old.installed == assignment.installed
                && old.address.socket == assignment.address.socket
        }) {
            if cell.scopes.load(Ordering::Acquire) & CLOSED != 0 {
                return Err(MembershipStateError::Busy);
            }
        } else {
            if current.is_some_and(|old| old.installed >= assignment.installed)
                || cell.scopes.load(Ordering::Acquire) != CLOSED
                || cell.owners.load(Ordering::Acquire) != 0
            {
                return Err(MembershipStateError::Busy);
            }
            // The closed zero-count gate excludes physical readers/writers.
            // All native owners separately joined; no mutable alias is formed.
            cell.state.reset();
            cell.state.install(1, &[], assignment.installed.0);
            cell.stamp.store(assignment.installed.0, Ordering::Release);
            cell.scopes.store(0, Ordering::Release);
        }
        *current = Some(assignment);
        let mut origin = origin.clone();
        origin.slot = StateSlot(Some(SlotRef::Assignment(AssignmentRef {
            table: self.clone(),
            index,
            stamp: assignment.installed.0,
        })));
        Ok(origin)
    }
    /// Close assignment scopes immediately. This records no health failure,
    /// starts no recovery probe and is not evidence that outbound IO has ended.
    pub fn retire(
        &self,
        receipt: Retirement,
    ) -> Result<bool, MembershipStateError> {
        let cell = &self.inner.cells[self.index(receipt.0.id)?];
        let current = cell.assignment.lock().unwrap_or_else(|p| p.into_inner());
        if *current != Some(receipt.0) {
            return Ok(false);
        }
        cell.scopes.fetch_or(CLOSED, Ordering::AcqRel);
        Ok(true)
    }
    /// A local receipt is ready only after retirement, all admitted atomic
    /// scopes and native IO joins. Dropping a resource handle never counts as ACK.
    pub fn drained(
        &self,
        receipt: Retirement,
    ) -> Result<bool, MembershipStateError> {
        let cell = &self.inner.cells[self.index(receipt.0.id)?];
        let current = cell.assignment.lock().unwrap_or_else(|p| p.into_inner());
        Ok(*current == Some(receipt.0)
            && cell.scopes.load(Ordering::Acquire) == CLOSED
            && cell.owners.load(Ordering::Acquire) == 0)
    }
}
#[derive(Clone, Debug)]
pub(crate) struct AssignmentRef {
    table: LocalMembershipState,
    index: usize,
    stamp: u64,
}
impl AssignmentRef {
    fn cell(&self) -> &Cell {
        &self.table.inner.cells[self.index]
    }
    pub(crate) fn belongs_to(
        &self,
        parent: &Arc<UpstreamStateTable>,
    ) -> bool {
        self.table
            .inner
            .parent
            .upgrade()
            .is_some_and(|owner| Arc::ptr_eq(&owner, parent))
    }
    pub(crate) fn same(
        &self,
        other: &Self,
    ) -> bool {
        Arc::ptr_eq(&self.table.inner, &other.table.inner)
            && self.index == other.index
            && self.stamp == other.stamp
    }
    pub(super) fn guard(&self) -> Option<AssignmentGuard> {
        let cell = self.cell();
        let mut before = cell.scopes.load(Ordering::Acquire);
        loop {
            if before & CLOSED != 0 || before == CLOSED - 1 {
                return None;
            }
            match cell.scopes.compare_exchange_weak(
                before,
                before + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(current) => before = current,
            }
        }
        let guard = AssignmentGuard(self.clone());
        // Check AFTER reserving the scope. A cached gate value can CAS into a
        // newly opened generation; it must refund that new scope and reject.
        if cell.stamp.load(Ordering::Acquire) != self.stamp {
            return None;
        }
        Some(guard)
    }
    pub(crate) fn access<T>(
        &self,
        action: impl FnOnce(&OriginSlot) -> T,
    ) -> Option<T> {
        let _guard = self.guard()?;
        Some(action(&self.cell().state))
    }
    pub(crate) fn owner(&self) -> Option<AssignmentOwner> {
        let cell = self.cell();
        let _current =
            cell.assignment.lock().unwrap_or_else(|p| p.into_inner());
        if cell.stamp.load(Ordering::Acquire) != self.stamp
            || cell.scopes.load(Ordering::Acquire) & CLOSED != 0
        {
            return None;
        }
        let before = cell.owners.load(Ordering::Relaxed);
        cell.owners.store(before.checked_add(1)?, Ordering::Release);
        Some(AssignmentOwner {
            assignment: self.clone(),
            joined: AtomicBool::new(false),
        })
    }
}
pub(crate) struct AssignmentGuard(AssignmentRef);
impl Drop for AssignmentGuard {
    fn drop(&mut self) {
        let before = self.0.cell().scopes.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(before & !CLOSED > 0);
    }
}
pub(crate) struct AssignmentOwner {
    assignment: AssignmentRef,
    joined: AtomicBool,
}
impl AssignmentOwner {
    pub(crate) fn guard(&self) -> Option<AssignmentGuard> {
        if self.joined.load(Ordering::Acquire) {
            return None;
        }
        let guard = self.assignment.guard()?;
        if self.joined.load(Ordering::Acquire) {
            return None;
        }
        Some(guard)
    }
    /// Only OriginTransport calls this after force/close and native tracker
    /// join, or consumed unique inert construction with a closed empty tracker.
    /// There is deliberately no Drop acknowledgement or public ACK API.
    pub(crate) fn joined(&self) {
        let cell = self.assignment.cell();
        let _current =
            cell.assignment.lock().unwrap_or_else(|p| p.into_inner());
        if self.joined.swap(true, Ordering::AcqRel) {
            return;
        }
        debug_assert_eq!(
            cell.stamp.load(Ordering::Acquire),
            self.assignment.stamp
        );
        let before = cell.owners.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(before > 0);
    }
}
