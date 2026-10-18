use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use claviron_db::DbService;
use claviron_runtime::AppState;
use claviron_runtime::registry::{Bootable, Component, Error, Result};
use tracing::info;

use crate::HostingStore;

/// Sole process-local owner of the resolved `hosting.servers.id`.
pub struct HostingService {
    server_id: OnceLock<i32>,
    db: OnceLock<Arc<DbService>>,
    bus: OnceLock<claviron_runtime::events::ProcessEventBus>,
}

impl HostingService {
    pub fn new() -> Self {
        Self {
            server_id: OnceLock::new(),
            db: OnceLock::new(),
            bus: OnceLock::new(),
        }
    }

    pub fn server_id(&self) -> Option<i32> {
        self.server_id.get().copied()
    }

    /// The hosting store on the connection `db.yaml`'s `hosting:` names,
    /// looked up now, so a reloaded `db.yaml` is what it opens. Where the
    /// database does not announce its own changes (SQLite), the store
    /// announces its committed writes as [`HostingChanged`] fleet events.
    ///
    /// [`HostingChanged`]: crate::HostingChanged
    pub fn store(
        &self
    ) -> std::result::Result<HostingStore, crate::HostingError> {
        let db = self.db.get().ok_or(crate::HostingError::NotInitialized)?;
        let store = HostingStore::open(db)?;
        match self.bus.get() {
            Some(bus) if !store.is_postgres() => {
                let bus = bus.clone();
                Ok(store.announcing(Arc::new(move |what| bus.emit(what))))
            }
            _ => Ok(store),
        }
    }

    pub fn require_server_id(&self) -> Result<i32> {
        self.server_id()
            .ok_or_else(|| Error::msg("HostingService is not initialized"))
    }

    async fn resolve_server_id(
        &self,
        db: Arc<DbService>,
        state: &AppState,
    ) -> Result<()> {
        let _ = self.db.set(Arc::clone(&db));
        let _ = self.bus.set(state.events().bus().clone());
        if let Some(events) =
            state.registry_ref().resolve::<claviron_orbit::FleetEventRegistry>()
        {
            events.register_bridge::<crate::HostingChanged>(state)?;
        }
        // Boot reads the declaration; it does not make one. A host is
        // registered in hosting as a setup step (`claviron-hosting
        // register-server`), so a name nothing declared is an error here,
        // not a row.
        let name = state.name();
        let server = HostingStore::open(&db)
            .map_err(|error| Error::msg(error.to_string()))?
            .server_by_name(name)
            .await
            .map_err(|error| Error::msg(format!("finding server: {error}")))?
            .ok_or_else(|| {
                Error::msg(format!(
                    "server '{name}' is not registered in hosting; run `claviron-hosting --server-name {name} register-server` first"
                ))
            })?;

        self.server_id
            .set(server.id)
            .map_err(|_| Error::msg("HostingService booted more than once"))?;
        info!(server_id = server.id, name = %server.name, "HostingService bootstrapped");
        Ok(())
    }
}

impl Default for HostingService {
    fn default() -> Self {
        Self::new()
    }
}

impl Component<AppState> for HostingService {
    /// The server id is resolved through the pool `DbService` publishes.
    type Deps = (Arc<DbService>,);

    fn name(&self) -> &'static str {
        "HostingService"
    }

    fn as_bootable(&self) -> Option<&dyn Bootable<AppState, Self::Deps>> {
        Some(self)
    }
}

#[async_trait]
impl Bootable<AppState, (Arc<DbService>,)> for HostingService {
    async fn boot(
        &self,
        (db,): (Arc<DbService>,),
        state: &AppState,
    ) -> Result<()> {
        self.resolve_server_id(db, state).await
    }
}

pub fn register(state: &AppState) -> Result<()> {
    state.registry_ref().register(Arc::new(HostingService::new()))?;
    Ok(())
}
