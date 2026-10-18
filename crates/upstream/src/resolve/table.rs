//! Cold, transactional current truth. Events announce revisions; their payloads
//! never replace the table. Readers and writers serialize with the same lock.
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use claviron_orbit::lock::{Lock, LockKey, LockType};
use serde::{Deserialize, Serialize};

use super::ResolutionSnapshot;
pub(crate) mod layout;
pub(crate) mod membership;
pub(crate) mod provider;
use layout::{Header, PAYLOAD, Row};

#[derive(Debug, thiserror::Error)]
pub enum PublicationError {
    #[error("invalid upstream resolution publication: {0}")]
    Invalid(&'static str),
    #[error(
        "upstream resolution table is full: capacity={capacity} key={key:?}"
    )]
    Full { capacity: usize, key: String },
    #[error(
        "upstream resolution payload exceeds build geometry: bytes={bytes} capacity={capacity}"
    )]
    TooLarge { bytes: usize, capacity: usize },
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Lock(#[from] orbitive::lock::Error),
}
/// Contention is per profile; names within that profile share the same tenure.
pub struct ResolverDuty;
impl LockType for ResolverDuty {
    const NAMESPACE: &'static str = "claviron.upstream.resolve";
}

enum Backing {
    Memory(Box<[Row]>),
    #[cfg(unix)]
    Shm(orbitive::shm::ShmRegion),
}
pub struct ResolutionTable {
    backing: Backing,
    capacity: usize,
    fleet_authority: bool,
    structural: Mutex<()>,
}
impl std::fmt::Debug for ResolutionTable {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        f.debug_struct("ResolutionTable")
            .field("capacity", &self.capacity)
            .finish_non_exhaustive()
    }
}
#[derive(Clone, Debug)]
pub struct ResolutionEntry {
    table: Arc<ResolutionTable>,
    index: usize,
    duty: LockKey,
    identity: Arc<str>,
}
/// Resource-side tenure and optimistic revision fence. This owns no runnable,
/// lease renewal ticker or event loop; the caller owns all execution policy.
pub struct ResolutionWriter {
    entry: ResolutionEntry,
    fence: u64,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublishedResolution {
    pub revision: u64,
    pub snapshot: ResolutionSnapshot,
}
impl ResolutionTable {
    pub(crate) fn fleet_authority(&self) -> bool {
        self.fleet_authority
    }
    /// Physical canonical slots available per named binding, including retirees.
    pub fn address_capacity(&self) -> usize {
        layout::ADDRESSES
    }
    pub fn memory(capacity: usize) -> Arc<Self> {
        Self::memory_with_authority(capacity, false)
    }
    /// In-memory fleet fixtures still require an exact Orbit lease.
    pub fn memory_fleet(capacity: usize) -> Arc<Self> {
        Self::memory_with_authority(capacity, true)
    }
    fn memory_with_authority(
        capacity: usize,
        fleet_authority: bool,
    ) -> Arc<Self> {
        assert!(capacity > 0);
        Arc::new(Self {
            backing: Backing::Memory(
                (0..capacity).map(|_| Row::empty()).collect(),
            ),
            capacity,
            fleet_authority,
            structural: Mutex::new(()),
        })
    }
    #[cfg(unix)]
    fn open(
        name: &str,
        capacity: usize,
    ) -> Result<(Arc<Self>, bool), PublicationError> {
        let size = size_of::<Header>()
            .checked_add(
                capacity
                    .checked_mul(size_of::<Row>())
                    .ok_or(PublicationError::Invalid("table size overflow"))?,
            )
            .ok_or(PublicationError::Invalid("table size overflow"))?;
        if capacity == 0 || capacity > u32::MAX as usize {
            return Err(PublicationError::Invalid("invalid table capacity"));
        }
        let (region, _initialization) =
            orbitive::shm::ShmRegion::open_or_create_locked(name, size)?;
        let created = region.created();
        if created {
            // SAFETY: exact allocated geometry, page alignment and exclusive
            // initialization lock precede exposure to consumers.
            unsafe {
                std::ptr::write(
                    region.as_ptr().cast::<Header>(),
                    Header::new(capacity),
                );
                let rows =
                    region.as_ptr().add(size_of::<Header>()).cast::<Row>();
                for index in 0..capacity {
                    std::ptr::write(rows.add(index), Row::empty());
                }
            }
        } else {
            // SAFETY: mapping size checked; header published under initialization lock.
            unsafe { &*region.as_ptr().cast::<Header>() }.validate(capacity)?;
        }
        Ok((
            Arc::new(Self {
                backing: Backing::Shm(region),
                capacity,
                fleet_authority: true,
                structural: Mutex::new(()),
            }),
            created,
        ))
    }
    fn rows(&self) -> &[Row] {
        match &self.backing {
            Backing::Memory(rows) => rows,
            #[cfg(unix)]
            Backing::Shm(region) => {
                // SAFETY: validated geometry; mapping retained by every entry.
                unsafe {
                    std::slice::from_raw_parts(
                        region.as_ptr().add(size_of::<Header>()).cast::<Row>(),
                        self.capacity,
                    )
                }
            }
        }
    }
    fn locked<T>(
        &self,
        action: impl FnOnce() -> Result<T, PublicationError>,
    ) -> Result<T, PublicationError> {
        let _local =
            self.structural.lock().unwrap_or_else(|poison| poison.into_inner());
        #[cfg(unix)]
        let _process = match &self.backing {
            Backing::Memory(_) => None,
            Backing::Shm(region) => Some(region.lock_exclusive()?),
        };
        action()
    }
    /// Registry write -> lifecycle admission -> publication flock. The action
    /// must not register owners or recursively read/lock this table. All native
    /// construction precedes this operation; stale preparation is aborted outside.
    pub(crate) fn with_publications<T>(
        &self,
        publications: &[(ResolutionEntry, u64)],
        profile: &crate::compile::profile::Profile,
        action: impl FnOnce() -> T,
    ) -> Result<Option<T>, PublicationError> {
        self.locked(|| {
            Ok(self.validate_publications(publications, profile)?.then(action))
        })
    }
    /// One exclusion spans every changed profile at boot/reload. Candidates
    /// register native owners beforehand; the callback only exposes prepared IO.
    pub(crate) fn with_profiles<T>(
        &self,
        profiles: &[&super::MaterializedProfile],
        action: impl FnOnce() -> T,
    ) -> Result<Option<T>, PublicationError> {
        self.locked(|| {
            for candidate in profiles {
                if candidate.publications.is_empty() {
                    if candidate.profile.origins.iter().any(|origin| {
                        matches!(
                            origin.slot.0.as_ref(),
                            Some(crate::state::SlotRef::Resolution(_))
                        )
                    }) {
                        return Err(PublicationError::Invalid(
                            "named boot candidate has no approvals",
                        ));
                    }
                } else if !self.validate_publications(
                    &candidate.publications,
                    &candidate.profile,
                )? {
                    return Ok(None);
                }
            }
            Ok(Some(action()))
        })
    }
    fn validate_publications(
        &self,
        publications: &[(ResolutionEntry, u64)],
        profile: &crate::compile::profile::Profile,
    ) -> Result<bool, PublicationError> {
        if publications.is_empty() {
            return Err(PublicationError::Invalid(
                "resolved membership requires publications",
            ));
        }
        let mut approved = Vec::new();
        for (entry, revision) in publications {
            if !std::ptr::eq(self, entry.table.as_ref())
                || entry.duty.label() != profile.name.as_bytes()
            {
                return Err(PublicationError::Invalid(
                    "publication belongs to another table or profile",
                ));
            }
            if approved.iter().any(
                |(previous, _): &(&ResolutionEntry, super::ResolvedSet)| {
                    previous.index == entry.index
                },
            ) {
                return Err(PublicationError::Invalid(
                    "duplicate membership publication",
                ));
            }
            let Some(current) = read(entry.row())? else {
                return Ok(false);
            };
            if current.revision != *revision {
                return Ok(false);
            }
            approved.push((entry, current.snapshot.restore()?));
        }
        let matches = |origin: &crate::origin::Origin,
                       entry: &ResolutionEntry,
                       set: &super::ResolvedSet,
                       assignment: super::Assignment| {
            let Some(crate::state::SlotRef::Resolution(member)) =
                origin.slot.0.as_ref()
            else {
                return false;
            };
            let socket = match origin.connect {
                crate::origin::ConnectTarget::Tcp(socket) => socket,
                crate::origin::ConnectTarget::Tls(socket) => socket,
                _ => return false,
            };
            socket == assignment.address.socket
                && origin.weight == assignment.address.weight
                && origin.priority == assignment.address.priority
                && member.matches_assignment(
                    entry,
                    assignment.id.index() - set.first.index(),
                    assignment.installed.0,
                )
        };
        for (entry, set) in &approved {
            for assignment in set.active() {
                if profile
                    .origins
                    .iter()
                    .filter(|origin| matches(origin, entry, set, assignment))
                    .count()
                    != 1
                {
                    return Err(PublicationError::Invalid(
                        "candidate differs from complete active membership",
                    ));
                }
            }
        }
        for origin in profile.origins.iter() {
            match origin.slot.0.as_ref() {
                Some(crate::state::SlotRef::Resolution(_)) => {
                    if !approved.iter().any(|(entry, set)| {
                        set.active().any(|assignment| {
                            matches(origin, entry, set, assignment)
                        })
                    }) {
                        return Err(PublicationError::Invalid(
                            "candidate contains an unapproved assignment",
                        ));
                    }
                }
                Some(crate::state::SlotRef::Assignment(_)) => {
                    return Err(PublicationError::Invalid(
                        "local assignment cannot stand in for a published member",
                    ));
                }
                _ => {}
            }
        }
        Ok(true)
    }

    pub fn install(
        self: &Arc<Self>,
        profile: &str,
        name: &str,
    ) -> Result<ResolutionEntry, PublicationError> {
        if profile.is_empty() || name.is_empty() {
            return Err(PublicationError::Invalid(
                "empty profile or name identity",
            ));
        }
        let key =
            crate::state::origin_key(profile, name, "dns").map_err(|_| {
                PublicationError::Invalid(
                    "escaped resolution identity exceeds the key ABI",
                )
            })?;
        let hash = key
            .bytes()
            .fold(0xcbf29ce484222325u64, |hash, byte| {
                (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3)
            })
            .max(1);
        self.locked(|| {
            for offset in 0..self.capacity {
                let index = ((hash % self.capacity as u64) as usize + offset)
                    % self.capacity;
                let row = &self.rows()[index];
                let stored = row.hash.load(Ordering::Acquire);
                if stored == hash
                    && row.key.iter().enumerate().all(|(index, byte)| {
                        byte.load(Ordering::Relaxed)
                            == key.as_bytes().get(index).copied().unwrap_or(0)
                    })
                {
                    return Ok(ResolutionEntry {
                        table: self.clone(),
                        index,
                        duty: LockKey::new::<ResolverDuty>(
                            bytes::Bytes::copy_from_slice(profile.as_bytes()),
                        ),
                        identity: key.clone(),
                    });
                }
                if stored == 0 {
                    for (index, byte) in row.key.iter().enumerate() {
                        byte.store(
                            key.as_bytes().get(index).copied().unwrap_or(0),
                            Ordering::Relaxed,
                        );
                    }
                    // Recover crash-left uncommitted installation bytes. A row
                    // whose hash is zero has never escaped as an entry handle.
                    row.fence.store(0, Ordering::Relaxed);
                    row.active.store(0, Ordering::Relaxed);
                    for bank in &row.banks {
                        bank.len.store(0, Ordering::Relaxed);
                        for eligible in &bank.eligible {
                            eligible.store(0, Ordering::Relaxed);
                        }
                    }
                    row.hash.store(hash, Ordering::Release);
                    return Ok(ResolutionEntry {
                        table: self.clone(),
                        index,
                        duty: LockKey::new::<ResolverDuty>(
                            bytes::Bytes::copy_from_slice(profile.as_bytes()),
                        ),
                        identity: key.clone(),
                    });
                }
            }
            Err(PublicationError::Full {
                capacity: self.capacity,
                key: key.to_string(),
            })
        })
    }
}
impl ResolutionEntry {
    pub(crate) fn fleet_authority(&self) -> bool {
        self.table.fleet_authority
    }
    pub(crate) fn with_publications<T>(
        &self,
        publications: &[(ResolutionEntry, u64)],
        profile: &crate::compile::profile::Profile,
        action: impl FnOnce() -> T,
    ) -> Result<Option<T>, PublicationError> {
        self.table.with_publications(publications, profile, action)
    }
    pub(super) fn duty(&self) -> &LockKey {
        &self.duty
    }
    /// Construct after a successful table commit, then emit on the local bus.
    pub fn event(
        &self,
        publication: &PublishedResolution,
    ) -> super::ResolutionChanged {
        super::ResolutionChanged {
            profile: String::from_utf8(self.duty.label().to_vec())
                .expect("prepared UTF-8 profile"),
            identity: self.identity.to_string(),
            revision: publication.revision,
            generation: publication.snapshot.generation(),
        }
    }
    fn row(&self) -> &Row {
        &self.table.rows()[self.index]
    }
    pub fn read(
        &self
    ) -> Result<Option<PublishedResolution>, PublicationError> {
        self.table.locked(|| read(self.row()))
    }
    /// The lease must come from the same Fleet as this table. Composition owns
    /// that association: the public Orbit lease exposes its exact tenure and
    /// key, but does not expose the physical fleet identity for comparison.
    pub fn claim(
        &self,
        lease: &mut Lock,
        ttl: Duration,
    ) -> Result<Option<ResolutionWriter>, PublicationError> {
        if !self.table.fleet_authority {
            return Err(PublicationError::Invalid(
                "fleet claim requires a fleet table",
            ));
        }
        self.table.locked(|| {
            if lease.lease().key != self.duty || !lease.renew(ttl)? {
                return Ok(None);
            }
            let fence = lease.lease().fencing_token().get();
            let previous = self.row().fence.load(Ordering::Acquire);
            if fence < previous {
                return Ok(None);
            }
            self.row().fence.store(fence, Ordering::Release);
            Ok(Some(ResolutionWriter { entry: self.clone(), fence }))
        })
    }
    /// Fleet-less owner generations fence each other in their private table.
    pub fn claim_local(&self) -> Result<ResolutionWriter, PublicationError> {
        if self.table.fleet_authority {
            return Err(PublicationError::Invalid(
                "local ownership cannot claim an SHM table",
            ));
        }
        self.table.locked(|| {
            let fence =
                self.row().fence.load(Ordering::Acquire).checked_add(1).ok_or(
                    PublicationError::Invalid("local tenure exhausted"),
                )?;
            self.row().fence.store(fence, Ordering::Release);
            Ok(ResolutionWriter { entry: self.clone(), fence })
        })
    }
}
impl ResolutionWriter {
    pub fn publish(
        &self,
        expected_revision: u64,
        snapshot: ResolutionSnapshot,
        lease: &mut Lock,
        ttl: Duration,
    ) -> Result<Option<PublishedResolution>, PublicationError> {
        if !self.entry.table.fleet_authority {
            return Err(PublicationError::Invalid(
                "fleet publication requires a fleet table",
            ));
        }
        self.publish_with(expected_revision, snapshot, || {
            if lease.lease().key != self.entry.duty
                || lease.lease().fencing_token().get() != self.fence
            {
                return Ok(false);
            }
            Ok(lease.renew(ttl)?)
        })
    }
    pub fn publish_local(
        &self,
        expected_revision: u64,
        snapshot: ResolutionSnapshot,
    ) -> Result<Option<PublishedResolution>, PublicationError> {
        if self.entry.table.fleet_authority {
            return Err(PublicationError::Invalid(
                "local publication cannot write an SHM table",
            ));
        }
        self.publish_with(expected_revision, snapshot, || Ok(true))
    }
    fn publish_with(
        &self,
        expected_revision: u64,
        snapshot: ResolutionSnapshot,
        owned: impl FnOnce() -> Result<bool, PublicationError>,
    ) -> Result<Option<PublishedResolution>, PublicationError> {
        snapshot.validate()?;
        self.entry.table.locked(|| {
            let row = self.entry.row();
            if row.fence.load(Ordering::Acquire) != self.fence {
                return Ok(None);
            }
            let current = read(row)?;
            if current.as_ref().map_or(0, |current| current.revision)
                != expected_revision
            {
                return Ok(None);
            }
            if let Some(current) = &current {
                if snapshot.generation() < current.snapshot.generation()
                    || (snapshot.generation() == current.snapshot.generation()
                        && !snapshot.same_membership(&current.snapshot))
                {
                    return Err(PublicationError::Invalid(
                        "membership changed without a newer generation",
                    ));
                }
            }
            let publication = PublishedResolution {
                revision: expected_revision
                    .checked_add(1)
                    .filter(|revision| *revision <= u64::MAX >> 1)
                    .ok_or(PublicationError::Invalid(
                        "publication revision exhausted",
                    ))?,
                snapshot,
            };
            let payload = serde_json::to_vec(&publication)?;
            if payload.len() > PAYLOAD {
                return Err(PublicationError::TooLarge {
                    bytes: payload.len(),
                    capacity: PAYLOAD,
                });
            }
            membership::validate_transition(
                row,
                current.as_ref().map(|p| &p.snapshot),
                &publication.snapshot,
            )?;
            // Linearization proof while the publication mutex is held. No
            // successor can claim this resource until this transaction ends.
            // Never hold Orbit's own table lock across an entry operation.
            if !owned()? {
                return Ok(None);
            }
            let next = 1 - (row.active.load(Ordering::Acquire) & 1) as usize;
            membership::prepare(row, next, &publication.snapshot)?;
            let bank = &row.banks[next];
            for (byte, value) in bank.bytes.iter().zip(&payload) {
                byte.store(*value, Ordering::Relaxed);
            }
            bank.len.store(payload.len() as u32, Ordering::Relaxed);
            // CRASH_SAFE_PUBLICATION: old bank remains intact until this one
            // atomic selector publishes all of the completed replacement.
            row.active.store(
                (publication.revision << 1) | next as u64,
                Ordering::Release,
            );
            Ok(Some(publication))
        })
    }
}
fn read(row: &Row) -> Result<Option<PublishedResolution>, PublicationError> {
    let selector = row.active.load(Ordering::Acquire);
    let index = (selector & 1) as usize;
    let bank = row
        .banks
        .get(index)
        .ok_or(PublicationError::Invalid("invalid active bank"))?;
    let len = bank.len.load(Ordering::Acquire) as usize;
    if len == 0 {
        return if selector == 0 {
            Ok(None)
        } else {
            Err(PublicationError::Invalid("committed bank is empty"))
        };
    }
    if len > PAYLOAD {
        return Err(PublicationError::Invalid("invalid bank length"));
    }
    let payload: Vec<_> = bank.bytes[..len]
        .iter()
        .map(|byte| byte.load(Ordering::Relaxed))
        .collect();
    let publication: PublishedResolution = serde_json::from_slice(&payload)?;
    if publication.revision == 0 || publication.revision != selector >> 1 {
        return Err(PublicationError::Invalid(
            "published revision differs from commit selector",
        ));
    }
    publication.snapshot.validate()?;
    Ok(Some(publication))
}
