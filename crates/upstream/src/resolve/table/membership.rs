//! Named health scopes and native IO receipts share the authoritative row.
//! No process heartbeat, handle Drop or elapsed time is a joined receipt.
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};

use arc_swap::ArcSwapOption;
use claviron_sys::process::{self, ProcessIdentity};

use super::layout::{
    ADDRESSES, CLOSED, Cell, GATE_MASK, GENERATION_SHIFT, Native, Row,
};
use super::{PublicationError, ResolutionEntry, read};
use crate::origin::{ConnectTarget, Origin};
use crate::proxy::failure::RequestFailure;
use crate::resolve::{Assignment, ResolutionSnapshot, Retirement};
use crate::state::{SlotRef, StateSlot, UpstreamStateTable};

// These phases are the native receipt protocol, not configurable service states.
const FREE: u64 = 0;
// A native owner remains registered even if its Rust handle is dropped.
const LIVE: u64 = 1;
// IO has joined; scopes already admitted before closure may still be exiting.
const JOINED: u64 = 2;
// One finalizer clears pressure before publishing FREE to cold registration.
const FINALIZING: u64 = 3;

#[derive(Clone)]
pub(crate) struct MemberRef {
    entry: ResolutionEntry,
    index: usize,
    stamp: u64,
    parent: Weak<UpstreamStateTable>,
    projection: Arc<ArcSwapOption<NativeRef>>,
}
impl std::fmt::Debug for MemberRef {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        f.debug_struct("MemberRef")
            .field("entry", &self.entry)
            .field("index", &self.index)
            .field("stamp", &self.stamp)
            .finish_non_exhaustive()
    }
}
#[derive(Debug)]
struct NativeRef {
    entry: ResolutionEntry,
    index: usize,
    stamp: u64,
    native: usize,
    generation: u64,
    pid: u32,
}
pub(crate) struct MemberGuard(Arc<NativeRef>);
pub(crate) struct MemberOwner {
    reference: Arc<NativeRef>,
    joined: AtomicBool,
}
#[derive(Debug)]
pub(crate) struct MemberTraffic(Arc<NativeRef>);

impl crate::proxy::service::UpstreamService {
    /// Bind a concrete, prepared origin to an exact current DNS publication.
    /// This cold capability starts no work and does not enable named config.
    /// The table/lock must belong to this runtime's fleet (or both be local).
    pub fn resolved_origin(
        &self,
        entry: &ResolutionEntry,
        revision: u64,
        origin: &Origin,
        assignment: Assignment,
    ) -> Result<Origin, PublicationError> {
        bind_origin(
            &self.inner.profile.name,
            &self.inner.runtime.state_table,
            self.inner.runtime.lock.is_some(),
            entry,
            revision,
            origin,
            assignment,
        )
    }
}
pub(crate) fn bind_origin(
    profile: &str,
    parent: &Arc<UpstreamStateTable>,
    fleet_authority: bool,
    entry: &ResolutionEntry,
    revision: u64,
    origin: &Origin,
    assignment: Assignment,
) -> Result<Origin, PublicationError> {
    if entry.duty.label() != profile.as_bytes() {
        return Err(PublicationError::Invalid(
            "membership belongs to another profile",
        ));
    }
    if entry.table.fleet_authority != fleet_authority {
        return Err(PublicationError::Invalid(
            "membership authority differs from runtime",
        ));
    }
    let socket = match origin.connect {
        ConnectTarget::Tcp(socket) => socket,
        ConnectTarget::Tls(socket) => socket,
        _ => {
            return Err(PublicationError::Invalid(
                "named origin requires TCP or TLS",
            ));
        }
    };
    if socket != assignment.address.socket {
        return Err(PublicationError::Invalid(
            "assignment differs from concrete socket",
        ));
    }
    entry.table.locked(|| {
        let current = read(entry.row())?
            .ok_or(PublicationError::Invalid("no published membership"))?;
        if current.revision != revision {
            return Err(PublicationError::Invalid(
                "membership publication was superseded",
            ));
        }
        let set = current.snapshot.restore()?;
        if !set.active().any(|member| member == assignment) {
            return Err(PublicationError::Invalid("assignment is not active"));
        }
        let index = assignment.id.index() - set.first.index();
        let mut origin = origin.clone();
        origin.weight = assignment.address.weight;
        origin.priority = assignment.address.priority;
        origin.slot = StateSlot(Some(SlotRef::Resolution(MemberRef {
            entry: entry.clone(),
            index,
            stamp: assignment.installed.0,
            parent: Arc::downgrade(parent),
            projection: Arc::new(ArcSwapOption::const_empty()),
        })));
        Ok(origin)
    })
}
impl MemberRef {
    pub(crate) fn same_binding(
        &self,
        other: &Self,
    ) -> bool {
        Arc::ptr_eq(&self.entry.table, &other.entry.table)
            && self.entry.index == other.entry.index
    }
    pub(crate) fn belongs_to(
        &self,
        parent: &Arc<UpstreamStateTable>,
    ) -> bool {
        self.parent.upgrade().is_some_and(|owner| Arc::ptr_eq(&owner, parent))
    }
    pub(crate) fn same(
        &self,
        other: &Self,
    ) -> bool {
        Arc::ptr_eq(&self.entry.table, &other.entry.table)
            && self.entry.index == other.entry.index
            && self.index == other.index
            && self.stamp == other.stamp
    }
    pub(crate) fn represented_by(
        &self,
        publications: &[(ResolutionEntry, u64)],
    ) -> bool {
        publications.iter().any(|(entry, _)| {
            Arc::ptr_eq(&self.entry.table, &entry.table)
                && self.entry.index == entry.index
        })
    }
    pub(super) fn matches_assignment(
        &self,
        entry: &ResolutionEntry,
        index: usize,
        stamp: u64,
    ) -> bool {
        Arc::ptr_eq(&self.entry.table, &entry.table)
            && self.entry.index == entry.index
            && self.index == index
            && self.stamp == stamp
    }
    pub(crate) fn guard(&self) -> Option<MemberGuard> {
        self.projection.load_full()?.guard(true)
    }
    pub(crate) fn access<T>(
        &self,
        action: impl FnOnce(&crate::state::layout::OriginSlot) -> T,
    ) -> Option<T> {
        let guard = self.guard()?;
        Some(action(&guard.0.cell().state))
    }
    pub(crate) fn pressure(&self) -> u32 {
        let Some(guard) = self.guard() else {
            return u32::MAX;
        };
        guard.0.cell().natives.iter().fold(0u32, |total, native| {
            total.saturating_add(native.pressure.load(Ordering::Acquire))
        })
    }
    pub(crate) fn traffic(&self) -> Result<MemberTraffic, RequestFailure> {
        let native =
            self.projection.load_full().ok_or(RequestFailure::Closed)?;
        let _guard = native.guard(true).ok_or(RequestFailure::Closed)?;
        let pressure = &native.slot().pressure;
        let mut before = pressure.load(Ordering::Acquire);
        loop {
            let after =
                before.checked_add(1).ok_or(RequestFailure::CapacityFull)?;
            match pressure.compare_exchange_weak(
                before,
                after,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(current) => before = current,
            }
        }
        Ok(MemberTraffic(native))
    }
    pub(crate) fn owner(&self) -> Option<MemberOwner> {
        // Only cold construction takes the process flock. A peer's snapshot
        // commit and registration cannot disagree on which assignment is active.
        self.entry
            .table
            .locked(|| {
                let row = self.entry.row();
                if !eligible(row, self.index, self.stamp) {
                    return Ok(None);
                }
                let cell = &row.cells[self.index];
                reconcile(cell);
                let Some(index) = cell.natives.iter().position(|native| {
                    reusable(native)
                        && native.generation.load(Ordering::Acquire)
                            < u64::MAX >> GENERATION_SHIFT
                }) else {
                    return Ok(None);
                };
                let native = &cell.natives[index];
                let Some(identity) = process::identity(std::process::id())
                else {
                    return Ok(None);
                };
                let Some(generation) =
                    native.generation.load(Ordering::Acquire).checked_add(1)
                else {
                    return Ok(None);
                };
                native.generation.store(generation, Ordering::Release);
                native.pid.store(identity.pid(), Ordering::Relaxed);
                native
                    .started_at
                    .store(identity.started_at(), Ordering::Relaxed);
                native.pressure.store(0, Ordering::Relaxed);
                native.phase.store(
                    (generation << GENERATION_SHIFT) | LIVE,
                    Ordering::Release,
                );
                native
                    .scopes
                    .store(generation << GENERATION_SHIFT, Ordering::Release);
                let reference = Arc::new(NativeRef {
                    entry: self.entry.clone(),
                    index: self.index,
                    stamp: self.stamp,
                    native: index,
                    generation,
                    pid: identity.pid(),
                });
                self.projection.store(Some(reference.clone()));
                Ok(Some(MemberOwner {
                    reference,
                    joined: AtomicBool::new(false),
                }))
            })
            .ok()
            .flatten()
    }
}
fn eligible(
    row: &Row,
    index: usize,
    stamp: u64,
) -> bool {
    let before = row.active.load(Ordering::Acquire);
    let installed = row.banks[(before & 1) as usize].eligible[index]
        .load(Ordering::Acquire);
    installed == stamp
        && row.cells[index].stamp.load(Ordering::Acquire) == stamp
        && row.active.load(Ordering::Acquire) == before
}
impl NativeRef {
    fn cell(&self) -> &Cell {
        &self.entry.row().cells[self.index]
    }
    fn slot(&self) -> &Native {
        &self.cell().natives[self.native]
    }
    fn guard(
        self: &Arc<Self>,
        active: bool,
    ) -> Option<MemberGuard> {
        // Forked/copied handles cannot reserve another process's scopes or ACK.
        // Only the recorded OS process can leave this native counter outstanding.
        if std::process::id() != self.pid {
            return None;
        }
        let native = self.slot();
        let mut before = native.scopes.load(Ordering::Acquire);
        loop {
            if before >> GENERATION_SHIFT != self.generation
                || before & CLOSED != 0
                || before & GATE_MASK == CLOSED - 1
            {
                return None;
            }
            match native.scopes.compare_exchange_weak(
                before,
                before + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(current) => before = current,
            }
        }
        let guard = MemberGuard(self.clone());
        // The CAS itself fenced native reuse. Retain the counted scope while
        // checking assignment eligibility, then through the entire operation.
        if native.generation.load(Ordering::Acquire) != self.generation
            || native.pid.load(Ordering::Acquire) != self.pid
            || self.cell().stamp.load(Ordering::Acquire) != self.stamp
            || (active && !eligible(self.entry.row(), self.index, self.stamp))
        {
            return None;
        }
        Some(guard)
    }
}
impl Drop for MemberGuard {
    fn drop(&mut self) {
        // A scope inherited across fork belongs to the original process only.
        if std::process::id() != self.0.pid {
            return;
        }
        let native = self.0.slot();
        let before = native.scopes.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(before & (CLOSED - 1) > 0);
        if before & GATE_MASK == CLOSED + 1 {
            finish_join(native, before >> GENERATION_SHIFT);
        }
    }
}
impl MemberOwner {
    pub(crate) fn guard(&self) -> Option<MemberGuard> {
        self.reference.guard(true)
    }
    /// Only OriginTransport's forced closed tracker join / unique inert abort.
    /// No public ACK API and deliberately no Drop implementation.
    pub(crate) fn joined(&self) {
        if self.joined.swap(true, Ordering::AcqRel) {
            return;
        }
        let native = self.reference.slot();
        if std::process::id() != self.reference.pid
            || native.generation.load(Ordering::Acquire)
                != self.reference.generation
        {
            return;
        }
        native.scopes.fetch_or(CLOSED, Ordering::AcqRel);
        let tag = self.reference.generation << GENERATION_SHIFT;
        if native
            .phase
            .compare_exchange(
                tag | LIVE,
                tag | JOINED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
        {
            finish_join(native, self.reference.generation);
        }
    }
}
fn finish_join(
    native: &Native,
    generation: u64,
) {
    let tag = generation << GENERATION_SHIFT;
    if native.scopes.load(Ordering::Acquire) == tag | CLOSED
        && native
            .phase
            .compare_exchange(
                tag | JOINED,
                tag | FINALIZING,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    {
        native.pressure.store(0, Ordering::Release);
        native.phase.store(tag | FREE, Ordering::Release);
    }
}

impl Drop for MemberTraffic {
    fn drop(&mut self) {
        // Retirement still allows an accepted body's release. Once native IO
        // joined, pressure was cleared and a frozen generation rejects late Drop,
        // even if the same assignment/socket has a newly registered native owner.
        let Some(_guard) = self.0.guard(false) else {
            return;
        };
        let before = self.0.slot().pressure.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(before > 0);
    }
}
fn reusable(native: &Native) -> bool {
    native.phase.load(Ordering::Acquire) & GATE_MASK == FREE
        && native.scopes.load(Ordering::Acquire) & GATE_MASK == CLOSED
}
fn drained(cell: &Cell) -> bool {
    cell.natives.iter().all(reusable)
}
fn reconcile(cell: &Cell) {
    for native in &cell.natives {
        if reusable(native) {
            continue;
        }
        let pid = native.pid.load(Ordering::Acquire);
        let identity = ProcessIdentity::new(
            pid,
            native.started_at.load(Ordering::Acquire),
        );
        if definitely_dead(identity) {
            // Only this OS process could hold scopes, and its incarnation is
            // proven gone. Heartbeats, master replacement and Unknown cannot do this.
            native.scopes.store(
                (native.generation.load(Ordering::Acquire) << GENERATION_SHIFT)
                    | CLOSED,
                Ordering::Release,
            );
            native.pressure.store(0, Ordering::Release);
            native.phase.store(
                (native.generation.load(Ordering::Acquire) << GENERATION_SHIFT)
                    | FREE,
                Ordering::Release,
            );
        }
    }
}
fn definitely_dead(identity: ProcessIdentity) -> bool {
    if identity.pid() == 0 || identity.pid() > i32::MAX as u32 {
        return false;
    }
    if let Some(current) = process::identity(identity.pid()) {
        return current != identity;
    }
    #[cfg(unix)]
    {
        // SAFETY: signal zero only queries existence; no process is signalled.
        let result = unsafe { libc::kill(identity.pid() as libc::pid_t, 0) };
        result == -1
            && std::io::Error::last_os_error().raw_os_error()
                == Some(libc::ESRCH)
    }
    #[cfg(not(unix))]
    {
        false
    }
}
impl ResolutionEntry {
    /// Exact published retirement plus every native IO join (or kernel death)
    /// and zero remaining scopes. Notification/Drop/time never count as receipt.
    pub fn retirement_ready(
        &self,
        receipt: Retirement,
    ) -> Result<bool, PublicationError> {
        self.table.locked(|| {
            let Some(current) = read(self.row())? else {
                return Ok(false);
            };
            let set = current.snapshot.restore()?;
            if !set.retiring().any(|retiring| retiring == receipt) {
                return Ok(false);
            }
            let cell =
                &self.row().cells[receipt.0.id.index() - set.first.index()];
            reconcile(cell);
            Ok(drained(cell))
        })
    }
}
/// Check all transitions before changing any physical health. Called under
/// snapshot publication flock; failed proof preserves the complete old bank.
pub(super) fn validate_transition(
    row: &Row,
    previous: Option<&ResolutionSnapshot>,
    next: &ResolutionSnapshot,
) -> Result<(), PublicationError> {
    let next_set = next.restore()?;
    if next_set.slots.len() > ADDRESSES {
        return Err(PublicationError::Invalid(
            "reserved range exceeds named health build geometry",
        ));
    }
    let Some(previous) = previous else {
        return Ok(());
    };
    let old_set = previous.restore()?;
    if old_set.first != next_set.first
        || old_set.slots.len() != next_set.slots.len()
    {
        return Err(PublicationError::Invalid(
            "published canonical range cannot change",
        ));
    }
    for (index, old) in old_set.slots.iter().enumerate() {
        let Some(old) = old else {
            if next_set.slots[index].is_some_and(|slot| {
                slot.assignment.installed <= old_set.generation
            }) {
                return Err(PublicationError::Invalid(
                    "new assignment must exceed previous membership generation",
                ));
            }
            continue;
        };
        let retained = next_set.slots[index].is_some_and(|next| {
            next.assignment.installed == old.assignment.installed
                && next.assignment.address.socket
                    == old.assignment.address.socket
        });
        if retained {
            if old.retiring
                && (!next_set.slots[index].unwrap().retiring
                    || old.assignment
                        != next_set.slots[index].unwrap().assignment)
            {
                return Err(PublicationError::Invalid(
                    "retiring assignment cannot be resurrected",
                ));
            }
        } else {
            if !old.retiring {
                return Err(PublicationError::Invalid(
                    "assignment must publish retirement before reuse",
                ));
            }
            let cell = &row.cells[index];
            reconcile(cell);
            if !drained(cell) {
                return Err(PublicationError::Invalid(
                    "native IO retirement is not joined",
                ));
            }
            if next_set.slots[index].is_some_and(|next| {
                next.assignment.installed <= old_set.generation
            }) {
                return Err(PublicationError::Invalid(
                    "reused assignment stamp must increase",
                ));
            }
        }
    }
    Ok(())
}
/// Inactive bank and unobservable/free cells only. The single selector release
/// in table.rs publishes snapshot bytes and new scope eligibility together.
pub(super) fn prepare(
    row: &Row,
    bank: usize,
    snapshot: &ResolutionSnapshot,
) -> Result<(), PublicationError> {
    let set = snapshot.restore()?;
    for (index, eligible) in row.banks[bank].eligible.iter().enumerate() {
        let assignment = set.slots.get(index).and_then(|slot| slot.as_ref());
        if let Some(slot) = assignment {
            let cell = &row.cells[index];
            // An unchanged answer also removes pressure from definitively dead
            // native owners; it never clears live/unknown owners or health.
            reconcile(cell);
            if cell.stamp.load(Ordering::Acquire) != slot.assignment.installed.0
            {
                if !drained(cell) {
                    return Err(PublicationError::Invalid(
                        "physical health slot remains owned",
                    ));
                }
                cell.state.reset();
                cell.state.install(1, &[], slot.assignment.installed.0);
                cell.stamp
                    .store(slot.assignment.installed.0, Ordering::Release);
            }
        }
        eligible.store(
            assignment
                .filter(|slot| !slot.retiring)
                .map_or(0, |slot| slot.assignment.installed.0),
            Ordering::Relaxed,
        );
    }
    Ok(())
}
