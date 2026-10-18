//! Managed physical owner; DNS execution and membership adoption are separate.
use std::sync::Arc;

use arc_swap::ArcSwapOption;
use async_trait::async_trait;
use claviron_orbit::OrbitStateExt;
use claviron_orbit::shm::{
    self, ExistingBootPolicy, FinalizePolicy, ManagedShmHandle,
    ManagedShmProvider, ManagedShmResource, ManagedShmResources,
    PhysicalShmState, ShmCreateOutcome, ShmResourceDescriptor, ShmShape,
};
use claviron_runtime::{AppState, registry};

use super::{ResolutionTable, layout};

pub struct ResolutionResources {
    table: ArcSwapOption<ResolutionTable>,
}
pub(crate) type ResolutionProvider = ManagedShmProvider<ResolutionResources>;
impl ResolutionResources {
    pub(crate) fn new() -> Self {
        Self { table: ArcSwapOption::const_empty() }
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
    ) -> registry::Result<(Arc<ResolutionTable>, bool)> {
        #[cfg(unix)]
        if let Some(fleet) = self.fleet(state)
            && fleet.is_shm()
        {
            let name = orbitive::shm::ring_segment_name(
                fleet.name(),
                layout::RESOLUTION_KIND,
            );
            return Ok(ResolutionTable::open(&name, layout::CAPACITY)?);
        }
        let table = self.table.load_full().unwrap_or_else(|| {
            if self.fleet(state).is_some() {
                ResolutionTable::memory_fleet(layout::CAPACITY)
            } else {
                ResolutionTable::memory(layout::CAPACITY)
            }
        });
        Ok((table, false))
    }
}
impl ManagedShmHandle for ResolutionResources {
    type Handle = ResolutionTable;
    fn managed_handle(&self) -> Option<Arc<Self::Handle>> {
        self.table.load_full()
    }
}
impl ManagedShmResources for ResolutionResources {
    fn name(&self) -> &'static str {
        "UpstreamResolutionProvider"
    }
    fn shm_resources(&self) -> Vec<&dyn ManagedShmResource> {
        vec![self]
    }
}
#[async_trait]
impl ManagedShmResource for ResolutionResources {
    fn descriptor(&self) -> ShmResourceDescriptor {
        ShmResourceDescriptor {
            provider: "UpstreamResolutionProvider",
            resource: "upstream-resolution-table",
            shape: ShmShape::OrbitTable,
            orbit_kind: Some(layout::RESOLUTION_KIND),
            existing_boot: ExistingBootPolicy::Reuse,
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
                layout::RESOLUTION_KIND,
            );
            let physical = orbitive::shm::ShmRegion::validate_existing(
                &name,
                size_of::<layout::Header>()
                    + layout::CAPACITY * size_of::<layout::Row>(),
            )?;
            if matches!(physical, orbitive::shm::ShmValidation::Valid { .. }) {
                validate_header(&name)?;
            }
            return Ok(physical.into());
        }
        Ok(if self.table.load().is_some() {
            PhysicalShmState::Valid {
                actual_size: size_of::<layout::Header>()
                    + layout::CAPACITY * size_of::<layout::Row>(),
            }
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
}
pub(crate) fn register(state: &AppState) -> registry::Result<()> {
    if state.registry_ref().resolve::<ResolutionProvider>().is_none() {
        shm::register(state, ResolutionResources::new())?;
    }
    Ok(())
}

#[cfg(unix)]
fn validate_header(name: &str) -> registry::Result<()> {
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
    let name = std::ffi::CString::new(name)?;
    // SAFETY: valid C string; O_RDONLY does not create or resize any object.
    let raw = unsafe { libc::shm_open(name.as_ptr(), libc::O_RDONLY, 0o600) };
    if raw < 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    // SAFETY: shm_open returned a newly owned descriptor.
    let file = std::fs::File::from(unsafe { OwnedFd::from_raw_fd(raw) });
    if file.metadata()?.len() < size_of::<layout::Header>() as u64 {
        return Err(registry::Error::msg(
            "upstream resolution header is truncated",
        ));
    }
    // SAFETY: immutable header fits object, mapping is read-only/page-aligned.
    let ptr = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            size_of::<layout::Header>(),
            libc::PROT_READ,
            libc::MAP_SHARED,
            file.as_raw_fd(),
            0,
        )
    };
    if ptr == libc::MAP_FAILED {
        return Err(std::io::Error::last_os_error().into());
    }
    // SAFETY: header contains integer bytes only; no advertised rows are read.
    let validation =
        unsafe { &*ptr.cast::<layout::Header>() }.validate(layout::CAPACITY);
    // SAFETY: exactly this function's live mapping is released after inspection.
    let unmap = unsafe { libc::munmap(ptr, size_of::<layout::Header>()) };
    validation?;
    if unmap != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok(())
}
