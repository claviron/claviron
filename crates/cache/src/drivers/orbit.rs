use std::any::Any;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use claviron_orbit::cache::{
    CacheBacking, CacheMode, CacheRuntime, CacheService, DEFAULT_L1_CAPACITY,
};
use serde::Deserialize;

use crate::config::CacheStoreConfig;
use crate::drivers::memory::MemoryCacheStore;
use crate::error::{Error, Result};
use crate::{CacheDriver, CacheStore, CacheStoreResolver, CacheValue};

/// Configured Orbit driver factory.
///
/// All stores created by this driver register with one `CacheService`, which
/// owns the physical rings and inbound lifecycle task in `claviron-orbit`.
pub struct OrbitCacheDriver {
    service: Arc<CacheService>,
}

impl OrbitCacheDriver {
    pub fn new() -> Self {
        Self { service: Arc::new(CacheService::new()) }
    }

    pub fn service(&self) -> Arc<CacheService> {
        self.service.clone()
    }

    pub fn has_stores(&self) -> bool {
        self.service.has_stores()
    }
}

impl Default for OrbitCacheDriver {
    fn default() -> Self {
        Self::new()
    }
}

struct CacheStoreBacking {
    store: Arc<dyn CacheStore>,
}

impl CacheStoreBacking {
    fn new(store: Arc<dyn CacheStore>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl CacheBacking for CacheStoreBacking {
    fn store_name(&self) -> &str {
        self.store.store_name()
    }

    fn supports_ttl(&self) -> bool {
        self.store.supports_ttl()
    }

    async fn get(
        &self,
        key: &str,
    ) -> std::result::Result<Option<Bytes>, String> {
        self.store.get(key).await.map_err(|error| error.to_string())
    }

    async fn put(
        &self,
        key: &str,
        value: &[u8],
        ttl: Option<Duration>,
    ) -> std::result::Result<(), String> {
        self.store.put(key, value, ttl).await.map_err(|error| error.to_string())
    }

    async fn forget(
        &self,
        key: &str,
    ) -> std::result::Result<(), String> {
        self.store.forget(key).await.map_err(|error| error.to_string())
    }

    async fn clear(&self) -> std::result::Result<(), String> {
        self.store.clear().await.map_err(|error| error.to_string())
    }
}

/// Driver-neutral facade adapter for one logical Orbit cache store.
///
/// Named-store configuration and concrete L2 adaptation live here. The
/// physical connection, this store's L1/L2 ordering, resynchronization, and
/// notification mechanics live in `claviron-orbit`.
#[derive(Clone)]
pub struct OrbitCacheStore {
    store_name: String,
    l2_name: Option<String>,
    cache: CacheRuntime,
}

impl OrbitCacheStore {
    pub fn new(
        store_name: impl Into<String>,
        service: Arc<CacheService>,
        l1_capacity: usize,
        l2_name: Option<String>,
    ) -> Result<Self> {
        Self::with_config(
            store_name,
            service,
            OrbitCacheStoreConfig {
                mode: OrbitCacheMode::Replicated,
                l1_capacity: Some(l1_capacity),
                l2: l2_name,
            },
        )
    }

    fn with_config(
        store_name: impl Into<String>,
        service: Arc<CacheService>,
        config: OrbitCacheStoreConfig,
    ) -> Result<Self> {
        let store_name = store_name.into();
        let mode = match config.mode {
            OrbitCacheMode::Replicated => {
                let capacity =
                    config.l1_capacity.unwrap_or(DEFAULT_L1_CAPACITY);
                let l1_capacity =
                    NonZeroUsize::new(capacity).ok_or_else(|| {
                        Error::Rejected(
                            "orbit cache l1_capacity must be greater than zero"
                                .to_string(),
                        )
                    })?;
                CacheMode::Replicated { l1_capacity }
            }
            OrbitCacheMode::Shm => {
                if config.l1_capacity.is_some() {
                    return Err(Error::Rejected(
                        "orbit cache mode shm does not accept l1_capacity"
                            .to_string(),
                    ));
                }
                CacheMode::Shm
            }
        };
        let l2_name = config.l2;
        let cache = service
            .open_store_with_mode(store_name.clone(), mode)
            .map_err(Error::from)?;
        if l2_name.is_none() && cache.has_backing() {
            return Err(Error::Rejected(format!(
                "Orbit cache store '{store_name}' already has an L2; removing it requires restart"
            )));
        }
        Ok(Self { store_name, l2_name, cache })
    }

    pub fn l2_name(&self) -> Option<&str> {
        self.l2_name.as_deref()
    }

    pub fn cache(&self) -> &CacheRuntime {
        &self.cache
    }

    pub(crate) fn attach_l2(
        &self,
        store: Arc<dyn CacheStore>,
    ) -> Result<()> {
        if store.store_name() == self.store_name
            || store.driver_name() == "orbit"
        {
            return Err(Error::Rejected(format!(
                "orbit cache store '{}' cannot use another Orbit store as L2",
                self.store_name
            )));
        }
        if store.as_any().is::<MemoryCacheStore>() {
            return Err(Error::Rejected(format!(
                "orbit cache store '{}' requires a backing shared by its workers",
                self.store_name
            )));
        }
        self.cache
            .attach_backing(Arc::new(CacheStoreBacking::new(store)))
            .map_err(Error::from)
    }

    pub fn get_sync(
        &self,
        key: &str,
    ) -> Result<Option<Bytes>> {
        self.cache.get_sync(key).map_err(Error::from)
    }

    pub fn put_sync(
        &self,
        key: &str,
        value: &[u8],
        ttl: Option<Duration>,
    ) -> Result<()> {
        self.cache.put_sync(key, value, ttl).map_err(Error::from)
    }

    pub fn get_value_sync(
        &self,
        key: &str,
    ) -> Result<Option<CacheValue>> {
        self.get_sync(key)?
            .map(|bytes| CacheValue::decode(&bytes).map_err(Error::from))
            .transpose()
    }

    pub fn put_value_sync(
        &self,
        key: &str,
        value: CacheValue,
        ttl: Option<Duration>,
    ) -> Result<()> {
        self.put_sync(key, &value.encode().map_err(Error::from)?, ttl)
    }

    pub fn forget_sync(
        &self,
        key: &str,
    ) -> Result<()> {
        self.cache.forget_sync(key).map_err(Error::from)
    }

    pub fn reset_sync(&self) -> Result<()> {
        self.cache.clear_sync().map_err(Error::from)
    }
}

#[async_trait]
impl CacheStore for OrbitCacheStore {
    fn store_name(&self) -> &str {
        &self.store_name
    }

    fn driver_name(&self) -> &str {
        "orbit"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn supports_ttl(&self) -> bool {
        self.cache.supports_ttl()
    }

    async fn get(
        &self,
        key: &str,
    ) -> Result<Option<Bytes>> {
        self.cache.get(key).await.map_err(Error::from)
    }

    async fn put(
        &self,
        key: &str,
        value: &[u8],
        ttl: Option<Duration>,
    ) -> Result<()> {
        self.cache.put(key, value, ttl).await.map_err(Error::from)
    }

    async fn forget(
        &self,
        key: &str,
    ) -> Result<()> {
        self.cache.forget(key).await.map_err(Error::from)
    }

    async fn clear(&self) -> Result<()> {
        self.cache.clear().await.map_err(Error::from)
    }
}

impl CacheDriver for OrbitCacheDriver {
    fn driver_name(&self) -> &'static str {
        "orbit"
    }

    fn create_store(
        self: Arc<Self>,
        store_name: &str,
        store: &CacheStoreConfig,
    ) -> Result<Arc<dyn CacheStore>> {
        let config: OrbitCacheStoreConfig = store.deserialize_options()?;
        Ok(Arc::new(OrbitCacheStore::with_config(
            store_name,
            self.service.clone(),
            config,
        )?))
    }

    fn create_reload_store(
        self: Arc<Self>,
        store_name: &str,
        store: &CacheStoreConfig,
    ) -> Result<Arc<dyn CacheStore>> {
        if !self.service.is_booted() {
            return Err(Error::Rejected(format!(
                "Orbit cache store '{store_name}' cannot be enabled because OrbitCacheService was not active at boot; restart is required"
            )));
        }
        self.create_store(store_name, store)
    }

    fn finish_store(
        &self,
        store: &Arc<dyn CacheStore>,
        stores: &dyn CacheStoreResolver,
    ) -> Result<()> {
        let Some(orbit) = store.as_any().downcast_ref::<OrbitCacheStore>()
        else {
            return Err(Error::Rejected(format!(
                "orbit driver built incompatible cache store '{}'",
                store.store_name()
            )));
        };
        let Some(l2_name) = orbit.l2_name() else {
            return Ok(());
        };
        let l2 = stores.store(l2_name).map_err(|_| {
            Error::Missing(format!(
                "orbit cache store '{}' references unknown L2 store '{l2_name}'",
                orbit.store_name()
            ))
        })?;
        orbit.attach_l2(l2)
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OrbitCacheMode {
    #[default]
    Replicated,
    Shm,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OrbitCacheStoreConfig {
    #[serde(default)]
    pub mode: OrbitCacheMode,
    pub l1_capacity: Option<usize>,
    #[serde(default)]
    pub l2: Option<String>,
}

#[cfg(test)]
#[path = "../../tests/drivers/orbit.rs"]
mod tests;
