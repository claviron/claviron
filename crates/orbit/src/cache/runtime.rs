use std::collections::HashMap;
use std::error::Error as StdError;
use std::fmt;
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::sync::{Arc, OnceLock, RwLock};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use claviron_runtime::registry::{
    self, Bootable, Component, ComponentOrder, Runnable, priority,
};
use claviron_runtime::{AppState, RunContext};
use orbitive::cache::{
    Cache, CacheLayout, CachePoll, CacheRead, DefaultCacheLayout, Store,
};
use orbitive::{Fleet, OrbitTyped, RingSpec};
use tokio::runtime::Handle;
use tracing::warn;

use crate::shm::{
    ExistingBootPolicy, FinalizePolicy, ManagedShmProvider, ManagedShmResource,
    ManagedShmResources, OrbitRingResource,
};
use crate::{FleetProvider, shm};

#[cfg(not(any(target_os = "linux", target_os = "freebsd")))]
const FALLBACK_POLL_INTERVAL: Duration = Duration::from_millis(10);

struct ManagedCacheMutationRecord<L>(PhantomData<L>);

impl<L> Clone for ManagedCacheMutationRecord<L> {
    fn clone(&self) -> Self {
        Self(PhantomData)
    }
}

impl<L: CacheLayout> OrbitTyped for ManagedCacheMutationRecord<L> {
    const KIND: u8 = L::MUTATION_RING_KIND;
    const RING_SPEC: RingSpec = RingSpec::per_node(
        L::MUTATION_RING_SPEC.capacity,
        L::MUTATION_RING_SPEC.payload_capacity,
    );
}

pub(crate) struct CacheShmResources {
    mutation: OrbitRingResource<ManagedCacheMutationRecord<DefaultCacheLayout>>,
}

impl CacheShmResources {
    fn new() -> Self {
        Self {
            mutation: OrbitRingResource::new(
                "OrbitCacheShmProvider",
                "cache-mutation-ring",
                ExistingBootPolicy::Reuse,
                FinalizePolicy::Preserve,
            ),
        }
    }
}

impl ManagedShmResources for CacheShmResources {
    fn name(&self) -> &'static str {
        "OrbitCacheShmProvider"
    }

    fn boot_priority(&self) -> Option<u8> {
        Some(priority::EARLY)
    }

    fn shm_resources(&self) -> Vec<&dyn ManagedShmResource> {
        vec![&self.mutation]
    }
}

pub(crate) fn register_shm(state: &AppState) -> registry::Result<()> {
    crate::arena::register(state, crate::arena::ArenaBootPolicy::Preserve)?;
    shm::register(state, CacheShmResources::new())
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    FleetUnavailable,
    NotBooted,
    Primitive(orbitive::cache::Error),
    Orbit(orbitive::Error),
    Io(std::io::Error),
    Backing { operation: &'static str, message: String },
    Rejected(String),
}

impl fmt::Display for Error {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        match self {
            Self::FleetUnavailable => {
                write!(
                    f,
                    "Orbit fleet is not booted; enable and register claviron-orbit"
                )
            }
            Self::NotBooted => write!(f, "Orbit cache service is not booted"),
            Self::Primitive(error) => error.fmt(f),
            Self::Orbit(error) => error.fmt(f),
            Self::Io(error) => error.fmt(f),
            Self::Backing { operation, message } => {
                write!(f, "Orbit cache backing {operation} failed: {message}")
            }
            Self::Rejected(message) => {
                write!(f, "Orbit cache rejected: {message}")
            }
        }
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::Primitive(error) => Some(error),
            Self::Orbit(error) => Some(error),
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<orbitive::cache::Error> for Error {
    fn from(value: orbitive::cache::Error) -> Self {
        Self::Primitive(value)
    }
}

impl From<orbitive::Error> for Error {
    fn from(value: orbitive::Error) -> Self {
        Self::Orbit(value)
    }
}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

/// Backing storage used by one logical Orbit store after an ordinary L1 miss.
///
/// Concrete File and Redis adapters live in `claviron-cache`; this contract
/// keeps their types and configuration out of the Orbit lifecycle binding.
#[async_trait]
pub trait CacheBacking: Send + Sync + 'static {
    fn store_name(&self) -> &str;
    fn supports_ttl(&self) -> bool;

    async fn get(
        &self,
        key: &str,
    ) -> std::result::Result<Option<Bytes>, String>;

    async fn put(
        &self,
        key: &str,
        value: &[u8],
        ttl: Option<Duration>,
    ) -> std::result::Result<(), String>;

    async fn forget(
        &self,
        key: &str,
    ) -> std::result::Result<(), String>;

    async fn clear(&self) -> std::result::Result<(), String>;
}

struct StoreState {
    name: String,
    l1_capacity: NonZeroUsize,
    primitive: OnceLock<Store<DefaultCacheLayout>>,
    backing: RwLock<Option<Arc<dyn CacheBacking>>>,
}

impl StoreState {
    fn primitive(&self) -> Result<&Store<DefaultCacheLayout>> {
        self.primitive.get().ok_or(Error::NotBooted)
    }

    fn backing(&self) -> Option<Arc<dyn CacheBacking>> {
        self.backing.read().expect("Orbit cache backing lock poisoned").clone()
    }
}

/// Lifecycle owner for one physical Orbit cache connection.
///
/// The service owns one mutation cursor, one notification listener, and the
/// registry of logical stores. `claviron-cache` registers named stores before
/// boot; this service opens their independent L1s after `FleetProvider` joins.
pub struct CacheService {
    cache: OnceLock<Cache<DefaultCacheLayout>>,
    stores: RwLock<HashMap<String, Arc<StoreState>>>,
    runtime: OnceLock<Handle>,
}

impl CacheService {
    pub fn new() -> Self {
        Self {
            cache: OnceLock::new(),
            stores: RwLock::new(HashMap::new()),
            runtime: OnceLock::new(),
        }
    }

    /// Construct an already connected service for direct library use or tests.
    pub fn from_fleet(fleet: Arc<Fleet>) -> Result<Arc<Self>> {
        let service = Arc::new(Self::new());
        service.install_connection(fleet)?;
        if let Ok(handle) = Handle::try_current() {
            let _ = service.runtime.set(handle);
        }
        Ok(service)
    }

    /// Register one named logical store on the shared cache connection.
    pub fn register_store(
        self: &Arc<Self>,
        name: impl Into<String>,
        l1_capacity: NonZeroUsize,
    ) -> Result<CacheRuntime> {
        self.open_store_inner(name.into(), l1_capacity, false)
    }

    /// Open a logical store, reusing a prior registration with the same
    /// capacity. This supports facade generations that temporarily hide and
    /// later re-enable a store without creating another cache listener.
    pub fn open_store(
        self: &Arc<Self>,
        name: impl Into<String>,
        l1_capacity: NonZeroUsize,
    ) -> Result<CacheRuntime> {
        self.open_store_inner(name.into(), l1_capacity, true)
    }

    fn open_store_inner(
        self: &Arc<Self>,
        name: String,
        l1_capacity: NonZeroUsize,
        reuse: bool,
    ) -> Result<CacheRuntime> {
        let mut stores = self
            .stores
            .write()
            .expect("Orbit cache store registry lock poisoned");
        if let Some(state) = stores.get(&name) {
            if !reuse {
                return Err(Error::Rejected(format!(
                    "Orbit cache store '{name}' is already registered"
                )));
            }
            if state.l1_capacity != l1_capacity {
                return Err(Error::Rejected(format!(
                    "Orbit cache store '{name}' is already registered with L1 capacity {}, requested {}",
                    state.l1_capacity, l1_capacity
                )));
            }
            return Ok(CacheRuntime {
                service: self.clone(),
                state: state.clone(),
            });
        }

        let state = Arc::new(StoreState {
            name: name.clone(),
            l1_capacity,
            primitive: OnceLock::new(),
            backing: RwLock::new(None),
        });

        if let Some(cache) = self.cache.get() {
            let primitive = cache.open_store(name.as_bytes(), l1_capacity)?;
            state.primitive.set(primitive).map_err(|_| {
                Error::Rejected(format!(
                    "Orbit cache store '{name}' is initialized"
                ))
            })?;
        }
        stores.insert(name, state.clone());

        Ok(CacheRuntime { service: self.clone(), state })
    }

    pub fn is_booted(&self) -> bool {
        self.cache.get().is_some()
    }

    pub fn has_stores(&self) -> bool {
        !self
            .stores
            .read()
            .expect("Orbit cache store registry lock poisoned")
            .is_empty()
    }

    fn install_connection(
        &self,
        fleet: Arc<Fleet>,
    ) -> Result<()> {
        let stores = self
            .stores
            .write()
            .expect("Orbit cache store registry lock poisoned");
        if self.cache.get().is_some() {
            return Ok(());
        }

        let cache = Cache::new(fleet)?;
        for state in stores.values() {
            let primitive =
                cache.open_store(state.name.as_bytes(), state.l1_capacity)?;
            state.primitive.set(primitive).map_err(|_| {
                Error::Rejected(format!(
                    "Orbit cache store '{}' is initialized",
                    state.name
                ))
            })?;
        }
        self.cache.set(cache).map_err(|_| {
            Error::Rejected("Orbit cache connection is initialized".to_string())
        })?;
        Ok(())
    }

    fn cache(&self) -> Result<&Cache<DefaultCacheLayout>> {
        self.cache.get().ok_or(Error::NotBooted)
    }

    fn runtime_handle(&self) -> Result<&Handle> {
        if Handle::try_current().is_ok() {
            return Err(Error::Rejected(
                "synchronous Orbit cache access must run outside a Tokio runtime thread"
                    .to_string()
            ));
        }
        self.runtime.get().ok_or(Error::NotBooted)
    }

    /// Drive inbound mutations for every registered logical store.
    async fn drive(
        &self,
        context: RunContext,
    ) -> registry::Result<()> {
        #[cfg(any(target_os = "linux", target_os = "freebsd"))]
        {
            let event_fd =
                self.cache().map_err(runtime_error)?.event_fd().map_err(
                    |error| {
                        registry::Error::msg(format!(
                            "Orbit cache eventfd creation failed: {error}"
                        ))
                    },
                )?;
            return self.drive_readiness(context, event_fd).await;
        }

        #[cfg(target_os = "macos")]
        {
            let event_fd = match self.cache().map_err(runtime_error)?.event_fd()
            {
                Ok(event_fd) => event_fd,
                Err(orbitive::cache::Error::Io(error))
                    if error.kind() == std::io::ErrorKind::Unsupported =>
                {
                    warn!(
                        "Orbit cache native readiness unavailable; using timer polling"
                    );
                    return self.drive_polling(context).await;
                }
                Err(error) => {
                    return Err(registry::Error::msg(format!(
                        "Orbit cache eventfd creation failed: {error}"
                    )));
                }
            };
            return self.drive_readiness(context, event_fd).await;
        }

        #[cfg(not(any(
            target_os = "linux",
            target_os = "freebsd",
            target_os = "macos"
        )))]
        self.drive_polling(context).await
    }

    #[cfg(any(
        target_os = "linux",
        target_os = "freebsd",
        target_os = "macos"
    ))]
    async fn drive_readiness(
        &self,
        context: RunContext,
        event_fd: orbitive::core::RingEventFd,
    ) -> registry::Result<()> {
        let event_fd =
            // SAFETY: RingEventFd owns its stable descriptor until drop. This
            // registration only borrows it to drain; it never replaces it.
            unsafe { tokio::io::unix::AsyncFd::register(event_fd) }.map_err(|error| {
                registry::Error::msg(format!(
                    "Orbit cache eventfd registration failed: {error}"
                ))
            })?;

        // Prepare readiness before the first poll so a mutation committed
        // between startup and the wait cannot be stranded without a wake.
        self.poll_and_recover().map_err(runtime_error)?;

        loop {
            tokio::select! {
                _ = context.cancelled() => return Ok(()),
                ready = event_fd.readable() => {
                    let mut ready = ready.map_err(|error| {
                        registry::Error::msg(format!(
                            "Orbit cache eventfd readiness failed: {error}"
                        ))
                    })?;
                    event_fd.get_ref().drain().map_err(|error| {
                        registry::Error::msg(format!(
                            "Orbit cache eventfd drain failed: {error}"
                        ))
                    })?;
                    ready.clear_ready();
                    self.poll_and_recover().map_err(runtime_error)?;
                }
            }
        }
    }

    #[cfg(not(any(target_os = "linux", target_os = "freebsd")))]
    async fn drive_polling(
        &self,
        context: RunContext,
    ) -> registry::Result<()> {
        self.poll_and_recover().map_err(runtime_error)?;

        let mut interval = tokio::time::interval(FALLBACK_POLL_INTERVAL);
        interval
            .set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = context.cancelled() => return Ok(()),
                _ = interval.tick() => {
                    self.poll_and_recover().map_err(runtime_error)?;
                }
            }
        }
    }

    fn poll_and_recover(&self) -> Result<CachePoll> {
        let cache = self.cache()?;
        let poll = cache.poll();
        if poll.loss.total() > 0 || poll.malformed > 0 {
            warn!(
                overwritten = poll.loss.overwritten,
                unavailable = poll.loss.unavailable,
                malformed = poll.malformed,
                "Orbit cache mutation history was incomplete"
            );
        }
        if !poll.payload_unavailable.is_empty() {
            warn!(
                entries = poll.payload_unavailable.len(),
                "Orbit cache payloads left the retained window; reads will fall through to L2"
            );
        }
        if !poll.unknown_stores.is_empty() {
            warn!(
                stores = poll.unknown_stores.len(),
                "Orbit cache received mutations for unregistered logical stores"
            );
        }
        if poll.resync_required {
            let missing = self
                .stores
                .read()
                .expect("Orbit cache store registry lock poisoned")
                .values()
                .filter(|state| state.backing().is_none())
                .map(|state| state.name.clone())
                .collect::<Vec<_>>();
            if !missing.is_empty() {
                return Err(Error::Rejected(format!(
                    "mutation history was lost and stores without backing cannot recover: {}",
                    missing.join(", ")
                )));
            }
            cache.recover_from_backing();
            warn!("Orbit cache stores recovered as empty L2-backed views");
        }
        Ok(poll)
    }
}

impl Default for CacheService {
    fn default() -> Self {
        Self::new()
    }
}

impl Component<AppState> for CacheService {
    // Optional: a runtime without an Orbit store boots without the fleet.
    type Deps = (Option<Arc<FleetProvider>>,);

    fn name(&self) -> &'static str {
        "OrbitCacheService"
    }

    fn boot_priority(&self) -> Option<u8> {
        Some(priority::EARLY)
    }

    fn run_priority(&self) -> Option<u8> {
        Some(priority::EARLY)
    }

    fn order(&self) -> ComponentOrder {
        ComponentOrder::new().after::<ManagedShmProvider<CacheShmResources>>()
    }

    fn as_runnable(
        self: Arc<Self>
    ) -> Option<Arc<dyn Runnable<AppState, Self::Deps>>> {
        Some(self)
    }

    fn as_bootable(&self) -> Option<&dyn Bootable<AppState, Self::Deps>> {
        Some(self)
    }
}

#[async_trait]
impl Bootable<AppState, (Option<Arc<FleetProvider>>,)> for CacheService {
    async fn boot(
        &self,
        (fleet_provider,): (Option<Arc<FleetProvider>>,),
        _state: &AppState,
    ) -> registry::Result<()> {
        let fleet = fleet_provider.and_then(|provider| provider.handle());
        let Some(fleet) = fleet else {
            // A process-local runtime with no Orbit store has nothing to
            // connect; one that declares a store cannot do without the fleet.
            if self.has_stores() {
                return Err(registry::Error::msg(
                    Error::FleetUnavailable.to_string(),
                ));
            }
            return Ok(());
        };
        self.install_connection(fleet).map_err(runtime_error)?;
        let _ = self.runtime.set(Handle::current());
        Ok(())
    }
}

#[async_trait]
impl Runnable<AppState, (Option<Arc<FleetProvider>>,)> for CacheService {
    async fn run(
        self: Arc<Self>,
        _deps: (Option<Arc<FleetProvider>>,),
        _state: AppState,
        context: RunContext,
    ) -> registry::Result<()> {
        if self.cache.get().is_none() {
            // Nothing to drive without a connection; see `boot`.
            context.cancelled().await;
            return Ok(());
        }
        self.drive(context).await
    }
}

/// One logical Orbit cache store and its optional L2 policy.
///
/// Clones share only this store's bounded L1/backing state while the parent
/// [`CacheService`] shares the physical connection and inbound driver with all
/// sibling stores.
#[derive(Clone)]
pub struct CacheRuntime {
    service: Arc<CacheService>,
    state: Arc<StoreState>,
}

impl CacheRuntime {
    pub fn store_name(&self) -> &str {
        &self.state.name
    }

    pub fn attach_backing(
        &self,
        backing: Arc<dyn CacheBacking>,
    ) -> Result<()> {
        let mut current = self
            .state
            .backing
            .write()
            .expect("Orbit cache backing lock poisoned");
        if let Some(existing) = current.as_ref()
            && existing.store_name() != backing.store_name()
        {
            return Err(Error::Rejected(format!(
                "Orbit cache store '{}' already uses backing store '{}', requested '{}'",
                self.state.name,
                existing.store_name(),
                backing.store_name()
            )));
        }
        *current = Some(backing);
        Ok(())
    }

    pub fn has_backing(&self) -> bool {
        self.state.backing().is_some()
    }

    pub fn supports_ttl(&self) -> bool {
        self.state.backing().is_none_or(|backing| backing.supports_ttl())
    }

    /// Inspect only this logical store's process-local L1.
    pub fn read_local(
        &self,
        key: &[u8],
    ) -> Result<CacheRead> {
        Ok(self.state.primitive()?.read(key))
    }

    pub async fn get(
        &self,
        key: &str,
    ) -> Result<Option<Bytes>> {
        if let Some(value) = self.local_get(key)? {
            return Ok(Some(value));
        }
        let Some(backing) = self.state.backing() else {
            return Ok(None);
        };
        let value = backing
            .get(key)
            .await
            .map_err(|message| backing_error("get", message))?;

        // A concurrent mutation may have arrived while the backing read was in
        // flight. Prefer that newer local value over the backing result.
        if let Some(local) = self.local_get(key)? {
            return Ok(Some(local));
        }
        Ok(value)
    }

    pub async fn put(
        &self,
        key: &str,
        value: &[u8],
        ttl: Option<Duration>,
    ) -> Result<()> {
        let store = self.state.primitive()?;
        store.validate_put(key.as_bytes(), value)?;
        let backing = self.state.backing();
        if ttl.is_some()
            && let Some(backing) = backing.as_deref()
            && !backing.supports_ttl()
        {
            return Err(Error::Rejected(format!(
                "backing store '{}' does not preserve TTL for key '{key}'",
                backing.store_name()
            )));
        }
        if let Some(backing) = backing {
            backing
                .put(key, value, ttl)
                .await
                .map_err(|message| backing_error("put", message))?;
        }
        store.put(key.as_bytes(), value, ttl)?;
        Ok(())
    }

    pub async fn forget(
        &self,
        key: &str,
    ) -> Result<()> {
        let store = self.state.primitive()?;
        store.validate_key(key.as_bytes())?;
        if let Some(backing) = self.state.backing() {
            backing
                .forget(key)
                .await
                .map_err(|message| backing_error("forget", message))?;
        }
        store.delete(key.as_bytes())?;
        Ok(())
    }

    pub async fn clear(&self) -> Result<()> {
        if let Some(backing) = self.state.backing() {
            backing
                .clear()
                .await
                .map_err(|message| backing_error("clear", message))?;
        }
        self.state.primitive()?.reset()?;
        Ok(())
    }

    pub fn get_sync(
        &self,
        key: &str,
    ) -> Result<Option<Bytes>> {
        self.service.runtime_handle()?.block_on(self.get(key))
    }

    pub fn put_sync(
        &self,
        key: &str,
        value: &[u8],
        ttl: Option<Duration>,
    ) -> Result<()> {
        self.service.runtime_handle()?.block_on(self.put(key, value, ttl))
    }

    pub fn forget_sync(
        &self,
        key: &str,
    ) -> Result<()> {
        self.service.runtime_handle()?.block_on(self.forget(key))
    }

    pub fn clear_sync(&self) -> Result<()> {
        self.service.runtime_handle()?.block_on(self.clear())
    }

    fn local_get(
        &self,
        key: &str,
    ) -> Result<Option<Bytes>> {
        self.service.poll_and_recover()?;
        match self.state.primitive()?.read(key.as_bytes()) {
            CacheRead::Hit(entry) => Ok(Some(entry.value)),
            CacheRead::Miss => Ok(None),
            CacheRead::ResyncRequired => Err(Error::Rejected(format!(
                "Orbit cache store '{}' requires backing-store recovery",
                self.state.name
            ))),
        }
    }
}

fn backing_error(
    operation: &'static str,
    message: String,
) -> Error {
    Error::Backing { operation, message }
}

fn runtime_error(error: Error) -> registry::Error {
    registry::Error::msg(error.to_string())
}

#[cfg(test)]
#[path = "../../tests/cache/runtime.rs"]
mod tests;
