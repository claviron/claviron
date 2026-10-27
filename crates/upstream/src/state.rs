//! Current keyed origin state; slot addresses never move during a fleet lifetime.
mod access;
pub(crate) mod assignment;
pub(crate) mod layout;
mod memory;
pub(crate) mod provider;
pub(crate) mod verdict;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use layout::{Header, KEY_MAX, OriginSlot};
#[derive(Debug, thiserror::Error)]
pub(crate) enum StateError {
    #[error(
        "invalid upstream state key {key:?}: maximum {KEY_MAX} bytes, no NUL or empty key"
    )]
    Key { key: String },
    #[error("upstream state table is full: capacity={capacity} key={key:?}")]
    StateFull { capacity: usize, key: String },
    #[error(transparent)]
    Io(#[from] std::io::Error),
}
enum Backing {
    Memory(Box<[OriginSlot]>),
    #[cfg(unix)]
    Shm(orbitive::shm::ShmRegion),
}
pub struct UpstreamStateTable {
    backing: Backing,
    capacity: usize,
    structural: Mutex<()>,
}
impl std::fmt::Debug for UpstreamStateTable {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        f.debug_struct("UpstreamStateTable")
            .field("capacity", &self.capacity)
            .finish_non_exhaustive()
    }
}
impl UpstreamStateTable {
    pub(crate) fn memory(capacity: usize) -> Arc<Self> {
        assert!(capacity.is_power_of_two());
        Arc::new(Self {
            backing: Backing::Memory(memory::slots(capacity)),
            capacity,
            structural: Mutex::new(()),
        })
    }
    #[cfg(unix)]
    pub(super) fn open(
        name: &str,
        capacity: usize,
    ) -> Result<(Arc<Self>, bool), StateError> {
        assert!(capacity.is_power_of_two());
        let size = size_of::<Header>() + capacity * size_of::<OriginSlot>();
        let (region, _initialization) =
            orbitive::shm::ShmRegion::open_or_create_locked(name, size)?;
        let created = region.created();
        if created {
            // SAFETY: the new mapping has the validated size/alignment and its
            // initialization lock excludes other attachers until publication.
            unsafe {
                std::ptr::write(
                    region.as_ptr().cast::<Header>(),
                    Header::new(capacity),
                );
                let ptr = region
                    .as_ptr()
                    .add(size_of::<Header>())
                    .cast::<OriginSlot>();
                for i in 0..capacity {
                    std::ptr::write(ptr.add(i), OriginSlot::empty());
                }
            }
        } else {
            // SAFETY: open validates mapping size; immutable header was
            // published by the creator under the same initialization lock.
            unsafe { &*region.as_ptr().cast::<Header>() }.validate(capacity)?;
        }
        Ok((
            Arc::new(Self {
                backing: Backing::Shm(region),
                capacity,
                structural: Mutex::new(()),
            }),
            created,
        ))
    }
    fn slots(&self) -> &[OriginSlot] {
        match &self.backing {
            Backing::Memory(slots) => slots,
            #[cfg(unix)]
            Backing::Shm(region) => {
                // SAFETY: header geometry was checked before this capability
                // escaped; the region outlives every SlotRef and fields are atomic.
                unsafe {
                    std::slice::from_raw_parts(
                        region
                            .as_ptr()
                            .add(size_of::<Header>())
                            .cast::<OriginSlot>(),
                        self.capacity,
                    )
                }
            }
        }
    }
    pub(crate) fn install(
        self: &Arc<Self>,
        key: &str,
        generation: u64,
    ) -> Result<SlotRef, StateError> {
        if key.is_empty() || key.len() > KEY_MAX || key.contains('\0') {
            return Err(StateError::Key { key: key.into() });
        }
        // FNV-1a is a fixed cross-process key hash; collisions compare the full
        // padded key, so neither hashing nor truncation can alias two origins.
        let hash = key
            .bytes()
            .fold(0xcbf29ce484222325u64, |h, b| {
                (h ^ b as u64).wrapping_mul(0x100000001b3)
            })
            .max(1);
        let _local = self.structural.lock().unwrap_or_else(|p| p.into_inner());
        #[cfg(unix)]
        let _process = match &self.backing {
            Backing::Shm(region) => Some(region.lock_exclusive()?),
            Backing::Memory(_) => None,
        };
        for offset in 0..self.capacity {
            let index =
                (hash as usize).wrapping_add(offset) & (self.capacity - 1);
            let slot = &self.slots()[index];
            if slot.matches(hash, key.as_bytes()) {
                slot.config_generation.fetch_max(generation, Ordering::Relaxed);
                return Ok(SlotRef::Resident { table: self.clone(), index });
            }
            if slot.key_hash.load(Ordering::Acquire) == 0 {
                slot.install(hash, key.as_bytes(), generation);
                return Ok(SlotRef::Resident { table: self.clone(), index });
            }
        }
        Err(StateError::StateFull { capacity: self.capacity, key: key.into() })
    }
}
#[derive(Clone, Debug)]
pub(crate) enum SlotRef {
    Resident { table: Arc<UpstreamStateTable>, index: usize },
    Assignment(assignment::AssignmentRef),
    Resolution(crate::resolve::table::membership::MemberRef),
}
impl UpstreamStateTable {
    /// Serialize profile-wide policy publication with the same process lock as
    /// installation. The callback must never recursively install or lock this table.
    pub(crate) fn with_structural<T>(
        &self,
        action: impl FnOnce() -> T,
    ) -> Result<T, StateError> {
        let _local = self.structural.lock().unwrap_or_else(|p| p.into_inner());
        #[cfg(unix)]
        let _process = match &self.backing {
            Backing::Shm(region) => Some(region.lock_exclusive()?),
            Backing::Memory(_) => None,
        };
        Ok(action())
    }
}
impl SlotRef {
    /// The guard on a reusable assignment remains held through the entire
    /// callback. The callback cannot return a borrowed physical slot reference.
    pub(crate) fn read<T>(
        &self,
        action: impl FnOnce(&OriginSlot) -> T,
    ) -> Option<T> {
        match self {
            Self::Resident { table, index } => {
                Some(action(&table.slots()[*index]))
            }
            Self::Assignment(assignment) => assignment.access(action),
            Self::Resolution(member) => member.access(action),
        }
    }
    pub(crate) fn available(&self) -> bool {
        self.read(|_| ()).is_some()
    }
    pub(crate) fn scope_guard(&self) -> Option<Option<StateGuard>> {
        match self {
            Self::Resident { .. } => Some(None),
            Self::Assignment(assignment) => {
                assignment.guard().map(StateGuard::Local).map(Some)
            }
            Self::Resolution(member) => {
                member.guard().map(StateGuard::Fleet).map(Some)
            }
        }
    }
    pub(crate) fn write<T>(
        &self,
        action: impl FnOnce(&OriginSlot) -> T,
    ) -> Option<T> {
        self.read(action)
    }
    pub(crate) fn owner(&self) -> Option<StateOwner> {
        match self {
            Self::Resident { .. } => None,
            Self::Assignment(assignment) => {
                assignment.owner().map(StateOwner::Local)
            }
            Self::Resolution(member) => member.owner().map(StateOwner::Fleet),
        }
    }
    pub(crate) fn is_assignment(&self) -> bool {
        matches!(self, Self::Assignment(_) | Self::Resolution(_))
    }
    pub(crate) fn same_assignment(
        &self,
        other: &Self,
    ) -> bool {
        match (self, other) {
            (Self::Resident { .. }, Self::Resident { .. }) => true,
            (Self::Assignment(left), Self::Assignment(right)) => {
                left.same(right)
            }
            (Self::Resolution(left), Self::Resolution(right)) => {
                left.same(right)
            }
            _ => false,
        }
    }
}

/// Delimiters are escaped so valid profile/label/path names cannot alias keys.
/// This is prepared with the origin once, never hashed or built per request.
pub(crate) fn origin_key(
    profile: &str,
    label: &str,
    address: &str,
) -> Result<Arc<str>, StateError> {
    // The default label repeats the address. Encode that fact as `u` rather
    // than storing a long UDS/IPv6 address twice. Named labels use `n:`.
    let implied = label == address;
    let parts = [profile, if implied { "" } else { label }, address];
    let capacity = parts
        .iter()
        .map(|s| {
            s.len() + s.bytes().filter(|b| matches!(b, b'|' | b'\\')).count()
        })
        .sum::<usize>()
        + 2
        + if implied { 1 } else { 2 };
    let mut key = String::with_capacity(capacity);
    for (i, part) in parts.iter().enumerate() {
        if i > 0 {
            key.push('|');
        }
        if i == 1 {
            key.push_str(if implied { "u" } else { "n:" });
        }
        for ch in part.chars() {
            if matches!(ch, '|' | '\\') {
                key.push('\\');
            }
            key.push(ch);
        }
    }
    if key.len() > KEY_MAX || key.contains('\0') {
        return Err(StateError::Key { key });
    }
    Ok(Arc::from(key))
}

/// Boot-only binding. Equality compares desired config, never mapping identity.
#[derive(Clone, Debug, Default)]
pub(crate) struct StateSlot(pub(crate) Option<SlotRef>);
impl PartialEq for StateSlot {
    fn eq(
        &self,
        _other: &Self,
    ) -> bool {
        true
    }
}
impl Eq for StateSlot {}
impl StateSlot {
    pub(crate) fn get(&self) -> &SlotRef {
        self.0.as_ref().expect("origin state bound before service construction")
    }
    pub(crate) fn same_assignment(
        &self,
        other: &Self,
    ) -> bool {
        match (&self.0, &other.0) {
            (Some(left), Some(right)) => left.same_assignment(right),
            (Some(SlotRef::Assignment(_) | SlotRef::Resolution(_)), _)
            | (_, Some(SlotRef::Assignment(_) | SlotRef::Resolution(_))) => {
                false
            }
            _ => true,
        }
    }
    pub(crate) fn belongs_to(
        &self,
        table: &Arc<UpstreamStateTable>,
    ) -> bool {
        self.0.as_ref().is_some_and(|slot| match slot {
            SlotRef::Resident { table: owner, .. } => Arc::ptr_eq(owner, table),
            SlotRef::Assignment(assignment) => assignment.belongs_to(table),
            SlotRef::Resolution(member) => member.belongs_to(table),
        })
    }
}

impl UpstreamStateTable {
    pub(crate) fn is_memory(&self) -> bool {
        matches!(self.backing, Backing::Memory(_))
    }
}

pub(crate) enum StateGuard {
    Local(assignment::AssignmentGuard),
    Fleet(crate::resolve::table::membership::MemberGuard),
}
pub(crate) enum StateOwner {
    Local(assignment::AssignmentOwner),
    Fleet(crate::resolve::table::membership::MemberOwner),
}
impl StateOwner {
    pub(crate) fn guard(&self) -> Option<StateGuard> {
        match self {
            Self::Local(owner) => owner.guard().map(StateGuard::Local),
            Self::Fleet(owner) => owner.guard().map(StateGuard::Fleet),
        }
    }
    pub(crate) fn joined(&self) {
        match self {
            Self::Local(owner) => owner.joined(),
            Self::Fleet(owner) => owner.joined(),
        }
    }
}

impl Drop for StateGuard {
    fn drop(&mut self) {
        match self {
            Self::Local(guard) => {
                let _ = guard;
            }
            Self::Fleet(guard) => {
                let _ = guard;
            }
        }
    }
}
