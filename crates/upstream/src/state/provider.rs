//! Physical owner: managed lifecycle decides reset authority, never master role.
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

use arc_swap::ArcSwapOption;
use async_trait::async_trait;
use claviron_orbit::OrbitStateExt;
use claviron_orbit::shm::{
    self, ActiveBootPolicy, ExistingBootPolicy, FinalizePolicy,
    ManagedShmHandle, ManagedShmProvider, ManagedShmResource,
    ManagedShmResources, PhysicalShmState, ResourceActivity, ShmCreateOutcome,
    ShmResourceDescriptor, ShmShape,
};
use claviron_runtime::{AppState, registry};

use super::UpstreamStateTable;
use super::layout::{CAPACITY, UPSTREAM_STATE_KIND};
pub struct UpstreamStateResources {
    table: ArcSwapOption<UpstreamStateTable>,
    observed: AtomicU8,
}
pub(crate) type UpstreamStateProvider =
    ManagedShmProvider<UpstreamStateResources>;
impl UpstreamStateResources {
    pub(crate) fn new() -> Self {
        Self { table: ArcSwapOption::const_empty(), observed: AtomicU8::new(2) }
    }
    fn fleet(
        &self,
        state: &AppState,
    ) -> Option<Arc<orbitive::Fleet>> {
        state.fleet()
    }
    fn open(
        &self,
        state: &AppState,
    ) -> registry::Result<(Arc<UpstreamStateTable>, bool)> {
        #[cfg(unix)]
        if let Some(fleet) = self.fleet(state)
            && fleet.is_shm()
        {
            let name = orbitive::shm::ring_segment_name(
                fleet.name(),
                UPSTREAM_STATE_KIND,
            );
            return Ok(UpstreamStateTable::open(&name, CAPACITY)?);
        }
        Ok((
            self.table
                .load_full()
                .unwrap_or_else(|| UpstreamStateTable::memory(CAPACITY)),
            false,
        ))
    }
}
impl ManagedShmHandle for UpstreamStateResources {
    type Handle = UpstreamStateTable;
    fn managed_handle(&self) -> Option<Arc<Self::Handle>> {
        self.table.load_full()
    }
}
#[async_trait]
impl ManagedShmResources for UpstreamStateResources {
    fn name(&self) -> &'static str {
        "UpstreamStateProvider"
    }
    async fn prepare(
        &self,
        state: &AppState,
    ) -> registry::Result<()> {
        // The supervisor authority already owns full-interval observation.
        // Unknown evidence preserves this resource; it never authorizes reset.
        let activity = if matches!(
            self.validate_physical(state)?,
            PhysicalShmState::Missing
        ) {
            // Creation needs initialization exclusion, not destructive authority.
            ResourceActivity::Unknown
        } else if state.is_worker() {
            ResourceActivity::Active
        } else {
            shm::activity(state, self.descriptor()).await?
        };
        self.observed.store(
            match activity {
                ResourceActivity::Active => 0,
                ResourceActivity::Quiescent => 1,
                ResourceActivity::Unknown => 2,
            },
            Ordering::Release,
        );
        Ok(())
    }
    fn shm_resources(&self) -> Vec<&dyn ManagedShmResource> {
        vec![self]
    }
}
#[async_trait]
impl ManagedShmResource for UpstreamStateResources {
    fn descriptor(&self) -> ShmResourceDescriptor {
        ShmResourceDescriptor {
            provider: "UpstreamStateProvider",
            resource: "upstream-state-table",
            shape: ShmShape::OrbitTable,
            orbit_kind: Some(UPSTREAM_STATE_KIND),
            existing_boot: ExistingBootPolicy::ResetWhenQuiescent {
                when_active: ActiveBootPolicy::Reuse,
            },
            finalize: FinalizePolicy::Preserve,
        }
    }
    fn validate_physical(
        &self,
        state: &AppState,
    ) -> registry::Result<PhysicalShmState> {
        #[cfg(unix)]
        if let Some(fleet) = self.fleet(state)
            && fleet.is_shm()
        {
            let name = orbitive::shm::ring_segment_name(
                fleet.name(),
                UPSTREAM_STATE_KIND,
            );
            let physical =
                orbitive::shm::ShmRegion::validate_existing(&name, 64)?;
            if matches!(physical, orbitive::shm::ShmValidation::Valid { .. }) {
                validate_header(&name)?;
            }
            return Ok(physical.into());
        }
        Ok(if self.table.load().is_some() {
            PhysicalShmState::Valid { actual_size: 64 + CAPACITY * 160 }
        } else {
            PhysicalShmState::Missing
        })
    }
    fn create_or_reuse(
        &self,
        state: &AppState,
    ) -> registry::Result<ShmCreateOutcome> {
        let (table, created) = self.open(state)?;
        self.table.store(Some(table));
        Ok(if created {
            ShmCreateOutcome::Created
        } else {
            ShmCreateOutcome::Reused
        })
    }
    fn reuse(
        &self,
        state: &AppState,
    ) -> registry::Result<()> {
        self.table.store(Some(self.open(state)?.0));
        Ok(())
    }
    fn destructive_boot_enabled(
        &self,
        _: &AppState,
    ) -> bool {
        self.observed.load(Ordering::Acquire) != 2
    }
    async fn activity(
        &self,
        state: &AppState,
    ) -> registry::Result<ResourceActivity> {
        let _ = state;
        Ok(match self.observed.load(Ordering::Acquire) {
            0 => ResourceActivity::Active,
            1 => ResourceActivity::Quiescent,
            _ => ResourceActivity::Unknown,
        })
    }
    fn reset(
        &self,
        state: &AppState,
    ) -> registry::Result<()> {
        // Open checks magic/version/geometry before any destructive operation;
        // the non-mutating validator has checked the same owning ABI header.
        let table = self.open(state)?.0;
        table.reset_quiescent()?;
        self.table.store(Some(table));
        Ok(())
    }
}
pub(crate) fn register(state: &AppState) -> registry::Result<()> {
    if state.registry_ref().resolve::<UpstreamStateProvider>().is_none() {
        shm::register(state, UpstreamStateResources::new())?;
    }
    Ok(())
}
// Destructive table operations stay inside the registered physical adapter.
impl UpstreamStateTable {
    fn reset_quiescent(&self) -> Result<(), super::StateError> {
        let _local = self.structural.lock().unwrap_or_else(|p| p.into_inner());
        #[cfg(unix)]
        let _process = match &self.backing {
            super::Backing::Shm(region) => Some(region.lock_exclusive()?),
            super::Backing::Memory(_) => None,
        };
        for slot in self.slots() {
            slot.reset();
        }
        Ok(())
    }
}

#[cfg(unix)]
fn validate_header(name: &str) -> registry::Result<()> {
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
    let name = std::ffi::CString::new(name)?;
    // SAFETY: valid C string; read-only open never creates or resizes an object.
    let raw = unsafe { libc::shm_open(name.as_ptr(), libc::O_RDONLY, 0o600) };
    if raw < 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    // SAFETY: shm_open returned a uniquely owned descriptor.
    let fd = unsafe { OwnedFd::from_raw_fd(raw) };
    let file = std::fs::File::from(fd);
    let actual = file.metadata()?.len();
    if actual < 64 {
        return Err(registry::Error::msg(format!(
            "upstream state header is truncated: actual={actual} expected=64"
        )));
    }
    // Orbitive does not expose a generic read-only raw table mapping. This
    // header-only mapping closes that gap without using an open-or-create API
    // during validation; POSIX SHM descriptor reads are not portable on macOS.
    // SAFETY: immutable object size is at least the frozen 64-byte header;
    // the mapping is read-only and the fd remains alive throughout validation.
    let ptr = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            64,
            libc::PROT_READ,
            libc::MAP_SHARED,
            file.as_raw_fd(),
            0,
        )
    };
    if ptr == libc::MAP_FAILED {
        return Err(std::io::Error::last_os_error().into());
    }
    // SAFETY: mmap is page-aligned and the validated header contains only
    // integer/byte fields; reading a malformed header does not create references
    // to its advertised slots. The layout is immutable after initialization.
    let result =
        unsafe { &*ptr.cast::<super::layout::Header>() }.validate(CAPACITY);
    // SAFETY: ptr is precisely this function's live 64-byte mapping.
    let unmapped = unsafe { libc::munmap(ptr, 64) };
    result?;
    if unmapped != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok(())
}
