//! Provider for the fleet's Orbitals table: `std::sync::atomic` between the
//! fleet's processes.
//!
//! One resource in two Orbit segments: the table, kind [`ATOMIC_KIND`], and
//! its name directory, kind [`ATOMIC_NAMES_KIND`], which lives and dies with
//! the table. Their life is the fleet's: the master resets the table when it
//! boots onto a quiescent fleet, which starts a new epoch, ends every earlier
//! id and forgets every name except those declared kept, whose values
//! survive. It reuses both unchanged while adopted workers keep running, so
//! their handles and ids stay valid. Both are kept on exit.
//!
//! A value is reached in one of two ways, and Claviron adds no handle type
//! over Orbitive's [`Orbital`] for either:
//!
//! - **By declaration.** [`orbital!`] gives a named value a type of its own,
//!   and [`Declare<T>`] registers it: every process opens it once at boot,
//!   after the table, and a handler takes it as `Provide<T>`. Every process
//!   that declares the same name holds the same value.
//! - **By id.** [`Orbitals::allocate`] in one process and [`Orbitals::open`]
//!   in another; the [`OrbitalId`] travels serialized or through an
//!   invocation.

use std::marker::PhantomData;
use std::sync::Arc;

use arc_swap::ArcSwapOption;
use async_trait::async_trait;
use claviron_runtime::registry::{
    self as runtime_registry, Bootable, Component,
};
use claviron_runtime::{AppState, Register};
pub use orbitive::atomic::*;

use crate::OrbitStateExt;
use crate::shm::{
    self, ActiveBootPolicy, ExistingBootPolicy, FinalizePolicy,
    ManagedShmHandle, ManagedShmProvider, ManagedShmResource,
    ManagedShmResources, PhysicalShmState, ResourceActivity, ShmCreateOutcome,
    ShmResourceDescriptor, ShmShape,
};

const PROVIDER_NAME: &str = "OrbitalsProvider";
const RESOURCE_NAME: &str = "orbit-atomic-table";
const NAMES_RESOURCE_NAME: &str = "orbit-atomic-names";

/// Domain resource set behind the mandatory SHM lifecycle wrapper.
pub struct OrbitalsResources {
    orbitals: ArcSwapOption<Orbitals>,
    names: OrbitalNames,
}

impl OrbitalsResources {
    fn new() -> Self {
        Self { orbitals: ArcSwapOption::const_empty(), names: OrbitalNames }
    }
}

/// Lifecycle owner and public capability for the fleet's Orbitals table.
pub type OrbitalsProvider = ManagedShmProvider<OrbitalsResources>;

impl ManagedShmHandle for OrbitalsResources {
    type Handle = Orbitals;

    fn managed_handle(&self) -> Option<Arc<Self::Handle>> {
        self.orbitals.load_full()
    }
}

impl ManagedShmResources for OrbitalsResources {
    fn name(&self) -> &'static str {
        PROVIDER_NAME
    }

    // The table first: its step decides the directory, which a reset
    // rewrites and a table created afresh removes, so the directory's
    // decision records what the table's step left.
    fn shm_resources(&self) -> Vec<&dyn ManagedShmResource> {
        vec![self, &self.names]
    }
}

#[async_trait]
impl ManagedShmResource for OrbitalsResources {
    fn descriptor(&self) -> ShmResourceDescriptor {
        ShmResourceDescriptor {
            provider: PROVIDER_NAME,
            resource: RESOURCE_NAME,
            shape: ShmShape::OrbitTable,
            orbit_kind: Some(ATOMIC_KIND),
            // Slots are never returned within an epoch, so a table nobody
            // holds is emptied rather than inherited; the reset keeps the
            // values declared kept, and the directory with them. Adopted
            // workers hold handles into it, and a reset does not invalidate a
            // handle: it is reused, same epoch, while any of them runs.
            existing_boot: ExistingBootPolicy::ResetWhenQuiescent {
                when_active: ActiveBootPolicy::Reuse,
            },
            finalize: FinalizePolicy::Preserve,
        }
    }

    fn validate_physical(
        &self,
        state: &AppState,
    ) -> runtime_registry::Result<PhysicalShmState> {
        validate_segment(state, ATOMIC_KIND)
    }

    fn create_or_reuse(
        &self,
        state: &AppState,
    ) -> runtime_registry::Result<ShmCreateOutcome> {
        self.orbitals.store(Some(Arc::new(open(state)?)));
        Ok(ShmCreateOutcome::Opened)
    }

    fn reuse(
        &self,
        state: &AppState,
    ) -> runtime_registry::Result<()> {
        self.orbitals.store(Some(Arc::new(open(state)?)));
        Ok(())
    }

    async fn activity(
        &self,
        state: &AppState,
    ) -> runtime_registry::Result<ResourceActivity> {
        if state.is_worker() {
            return Ok(ResourceActivity::Active);
        }
        shm::activity(state, self.descriptor()).await
    }

    fn reset(
        &self,
        state: &AppState,
    ) -> runtime_registry::Result<()> {
        let orbitals = open(state)?;
        orbitals.reset_all().map_err(|error| {
            runtime_registry::Error::msg(format!(
                "Orbitals table reset failed: {error}"
            ))
        })?;
        self.orbitals.store(Some(Arc::new(orbitals)));
        Ok(())
    }
}

/// The table's name directory, kind [`ATOMIC_NAMES_KIND`]: listed and
/// validated as a segment of its own, decided by the table.
///
/// Its own steps change nothing. The table's reset rewrites it, keeping the
/// kept names; a table created afresh removes a directory left over from an
/// earlier one; [`Orbitals::unlink`] removes both. Orbitive attaches it
/// through the table's handle only, and checks its header and geometry when
/// it does: at that reset, or at the first declaration. A fleet that
/// declares no name never creates it.
struct OrbitalNames;

#[async_trait]
impl ManagedShmResource for OrbitalNames {
    fn descriptor(&self) -> ShmResourceDescriptor {
        ShmResourceDescriptor {
            provider: PROVIDER_NAME,
            resource: NAMES_RESOURCE_NAME,
            shape: ShmShape::OrbitTable,
            orbit_kind: Some(ATOMIC_NAMES_KIND),
            // Whatever the table's step left: reset, removed or kept with it.
            // Clearing the directory alone would forget kept names whose
            // values the table still holds.
            existing_boot: ExistingBootPolicy::Reuse,
            finalize: FinalizePolicy::Preserve,
        }
    }

    fn validate_physical(
        &self,
        state: &AppState,
    ) -> runtime_registry::Result<PhysicalShmState> {
        validate_segment(state, ATOMIC_NAMES_KIND)
    }

    /// The first declaration creates it, after this decision.
    fn create_or_reuse(
        &self,
        _state: &AppState,
    ) -> runtime_registry::Result<ShmCreateOutcome> {
        Ok(ShmCreateOutcome::Deferred)
    }

    fn reuse(
        &self,
        _state: &AppState,
    ) -> runtime_registry::Result<()> {
        Ok(())
    }
}

/// Registers the value `T` declares, for a `Register` entry point such as a
/// runner's `with_services::<Declare<Hits>>()`.
///
/// Each process opens `T` once at boot, after [`OrbitalsProvider`] has decided
/// the table in that process, and publishes the open handle as a plain value:
/// `Provide<T>` hands it out, and the name is not looked up again. A worker
/// boots its own registry and opens its own handle; the first process to
/// declare the name in the table's epoch installs `T::INITIAL`, every other
/// one finds the same value. A component that reads `T` in its own boot
/// orders itself after `Declare<T>`.
///
/// `T` is a type of the application's own, from [`orbital!`]; `Declare<T>`
/// stands in for it because `Register` and `T` both belong to other crates.
pub struct Declare<T>(PhantomData<fn() -> T>);

impl<T: Declared> Register<AppState> for Declare<T> {
    fn register(state: &AppState) -> runtime_registry::Result<()> {
        crate::register_orbitals(state)?;
        let registry = state.registry_ref();
        if registry.resolve::<Self>().is_none() {
            registry.register(Arc::new(Self(PhantomData)))?;
        }
        Ok(())
    }
}

impl<T: Declared> Component<AppState> for Declare<T> {
    type Deps = (Arc<OrbitalsProvider>,);

    fn name(&self) -> &str {
        std::any::type_name::<T>()
    }

    fn as_bootable(&self) -> Option<&dyn Bootable<AppState, Self::Deps>> {
        Some(self)
    }
}

#[async_trait]
impl<T: Declared> Bootable<AppState, (Arc<OrbitalsProvider>,)> for Declare<T> {
    async fn boot(
        &self,
        (provider,): (Arc<OrbitalsProvider>,),
        state: &AppState,
    ) -> runtime_registry::Result<()> {
        let orbitals = provider.handle().ok_or_else(|| {
            runtime_registry::Error::msg(format!(
                "Orbital '{}' cannot be declared: the Orbitals table is not open",
                T::NAME
            ))
        })?;
        let value = T::open(&orbitals).map_err(|error| {
            runtime_registry::Error::msg(format!(
                "Orbital '{}' declaration failed: {error}",
                T::NAME
            ))
        })?;
        state.registry_ref().insert(Arc::new(value))?;
        Ok(())
    }
}

pub(crate) fn register(state: &AppState) -> runtime_registry::Result<()> {
    shm::register(state, OrbitalsResources::new())
}

fn fleet(state: &AppState) -> runtime_registry::Result<Arc<orbitive::Fleet>> {
    state.fleet().ok_or_else(|| {
        runtime_registry::Error::msg("Orbit fleet is unavailable")
    })
}

fn validate_segment(
    state: &AppState,
    kind: u8,
) -> runtime_registry::Result<PhysicalShmState> {
    let name = orbitive::shm::ring_segment_name(fleet(state)?.name(), kind);
    Ok(orbitive::shm::ShmRegion::validate_existing(&name, 64)?.into())
}

fn open(state: &AppState) -> runtime_registry::Result<Orbitals> {
    Orbitals::new(fleet(state)?).map_err(|error| {
        runtime_registry::Error::msg(format!(
            "Orbitals table boot failed: {error}"
        ))
    })
}

#[cfg(test)]
#[path = "../tests/atomic.rs"]
mod tests;
