//! Claviron lifecycle adapter for Orbit-backed rustls server sessions.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use arc_swap::ArcSwapOption;
use async_trait::async_trait;
use claviron_orbit::OrbitStateExt;
use claviron_orbit::shm::{
    self, ActiveBootPolicy, ExistingBootPolicy, FinalizePolicy,
    ManagedShmHandle, ManagedShmProvider, ManagedShmResource,
    ManagedShmResources, PhysicalShmState, ResourceActivity, ShmCreateOutcome,
    ShmResourceDescriptor, ShmShape,
};
use claviron_runtime::AppState;
use claviron_runtime::registry::{self as runtime_registry};
pub use orbitive::rustls::{
    DEFAULT_SESSION_TTL, FleetServerSessions, MAX_SESSION_TTL, SessionDomain,
};

const PROVIDER_NAME: &str = "FleetServerSessionsProvider";
const RESOURCE_NAME: &str = "fleet-tls-session-table";
// `orbit-rustls` owns this physical id but does not currently export it.
const TLS_SESSION_STATE_KIND: u8 = 231;

/// Domain resource set behind the mandatory SHM lifecycle wrapper.
pub struct FleetServerSessionsResources {
    sessions: ArcSwapOption<FleetServerSessions>,
    cleanup_on_exit: AtomicBool,
    ttl: Mutex<std::time::Duration>,
}

impl FleetServerSessionsResources {
    fn new() -> Self {
        Self {
            sessions: ArcSwapOption::const_empty(),
            cleanup_on_exit: AtomicBool::new(true),
            ttl: Mutex::new(DEFAULT_SESSION_TTL),
        }
    }

    fn ttl(&self) -> std::time::Duration {
        *self.ttl.lock().expect("TLS session TTL lock poisoned")
    }
}

/// Managed lifecycle owner and public TLS session-storage capability.
pub type FleetServerSessionsProvider =
    ManagedShmProvider<FleetServerSessionsResources>;

#[async_trait]
impl ManagedShmResources for FleetServerSessionsResources {
    fn name(&self) -> &'static str {
        PROVIDER_NAME
    }

    async fn prepare(
        &self,
        state: &AppState,
    ) -> runtime_registry::Result<()> {
        let config =
            crate::provider::load_tls_config(state).await.map_err(|error| {
                runtime_registry::Error::msg(format!(
                    "fleet TLS session config failed: {error}"
                ))
            })?;
        *self.ttl.lock().expect("TLS session TTL lock poisoned") =
            config.sessions().ttl();
        self.cleanup_on_exit
            .store(config.sessions().cleanup_on_exit(), Ordering::Release);
        Ok(())
    }

    fn reloadable(&self) -> bool {
        true
    }

    async fn reload(
        &self,
        state: &AppState,
    ) -> runtime_registry::Result<()> {
        let sessions = self.sessions.load_full().ok_or_else(|| {
            runtime_registry::Error::msg(
                "fleet TLS session storage is not booted",
            )
        })?;
        let config =
            crate::provider::load_tls_config(state).await.map_err(|error| {
                runtime_registry::Error::msg(format!(
                    "fleet TLS session config reload failed: {error}"
                ))
            })?;
        let ttl = config.sessions().ttl();
        sessions.set_ttl(ttl).map_err(|error| {
            runtime_registry::Error::msg(format!(
                "fleet TLS session TTL reload failed: {error}"
            ))
        })?;
        *self.ttl.lock().expect("TLS session TTL lock poisoned") = ttl;
        Ok(())
    }

    fn shm_resources(&self) -> Vec<&dyn ManagedShmResource> {
        vec![self]
    }
}

#[async_trait]
impl ManagedShmResource for FleetServerSessionsResources {
    fn descriptor(&self) -> ShmResourceDescriptor {
        ShmResourceDescriptor {
            provider: PROVIDER_NAME,
            resource: RESOURCE_NAME,
            shape: ShmShape::OrbitTable,
            orbit_kind: Some(TLS_SESSION_STATE_KIND),
            existing_boot: ExistingBootPolicy::ResetWhenQuiescent {
                when_active: ActiveBootPolicy::Reuse,
            },
            finalize: FinalizePolicy::UnlinkWhenQuiescent,
        }
    }

    fn validate_physical(
        &self,
        state: &AppState,
    ) -> runtime_registry::Result<PhysicalShmState> {
        let fleet = fleet(state)?;
        let name = orbitive::shm::ring_segment_name(
            fleet.name(),
            TLS_SESSION_STATE_KIND,
        );
        // The session primitive validates its complete private ABI on attach.
        Ok(orbitive::shm::ShmRegion::validate_existing(&name, 64)?.into())
    }

    fn create_or_reuse(
        &self,
        state: &AppState,
    ) -> runtime_registry::Result<ShmCreateOutcome> {
        self.sessions.store(Some(Arc::new(open(state, self.ttl())?)));
        Ok(ShmCreateOutcome::Opened)
    }

    fn reuse(
        &self,
        state: &AppState,
    ) -> runtime_registry::Result<()> {
        self.sessions.store(Some(Arc::new(open(state, self.ttl())?)));
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

    fn destructive_boot_enabled(
        &self,
        _state: &AppState,
    ) -> bool {
        self.cleanup_on_exit.load(Ordering::Acquire)
    }

    fn finalize_cleanup_enabled(
        &self,
        _state: &AppState,
    ) -> bool {
        self.cleanup_on_exit.load(Ordering::Acquire)
    }

    fn reset(
        &self,
        state: &AppState,
    ) -> runtime_registry::Result<()> {
        let sessions = open(state, self.ttl())?;
        sessions.reset().map_err(|error| {
            runtime_registry::Error::msg(format!(
                "fleet TLS session storage reset failed: {error}"
            ))
        })?;
        self.sessions.store(Some(Arc::new(sessions)));
        Ok(())
    }

    fn unlink(
        &self,
        state: &AppState,
    ) -> runtime_registry::Result<()> {
        let sessions = self
            .sessions
            .load_full()
            .map(Ok)
            .unwrap_or_else(|| open(state, self.ttl()).map(Arc::new))?;
        sessions.unlink().map_err(|error| {
            runtime_registry::Error::msg(format!(
                "fleet TLS session storage cleanup failed: {error}"
            ))
        })
    }
}

impl ManagedShmHandle for FleetServerSessionsResources {
    type Handle = FleetServerSessions;

    fn managed_handle(&self) -> Option<Arc<Self::Handle>> {
        self.sessions.load_full()
    }
}

/// Register the optional fleet-wide rustls session-storage capability.
pub fn register(state: &AppState) -> runtime_registry::Result<()> {
    claviron_orbit::register_fleet(state)?;
    let registry = state.registry_ref();
    if registry.resolve::<FleetServerSessionsProvider>().is_none() {
        shm::register(state, FleetServerSessionsResources::new())?;
    }
    Ok(())
}

fn fleet(state: &AppState) -> runtime_registry::Result<Arc<orbitive::Fleet>> {
    state.fleet().ok_or_else(|| {
        runtime_registry::Error::msg("Orbit fleet is unavailable")
    })
}

fn open(
    state: &AppState,
    ttl: std::time::Duration,
) -> runtime_registry::Result<FleetServerSessions> {
    FleetServerSessions::with_ttl(fleet(state)?, ttl).map_err(|error| {
        runtime_registry::Error::msg(format!(
            "fleet TLS session storage boot failed: {error}"
        ))
    })
}

#[cfg(test)]
#[path = "../tests/orbit.rs"]
mod tests;
