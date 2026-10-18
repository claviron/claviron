use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;

use crate::config::CacheStoreConfig;
use crate::{CacheValue, Error};

/// Raw storage contract implemented by cache backends.
///
/// Stores know how to persist bytes. Selection, value encoding, configuration,
/// and default-store behavior live above this boundary.
#[async_trait]
pub trait CacheStore: Any + Send + Sync + 'static {
    fn store_name(&self) -> &str;
    fn driver_name(&self) -> &str;
    fn as_any(&self) -> &dyn Any;
    fn supports_ttl(&self) -> bool;
    /// Return an immutable owning buffer, valid independently of this store.
    /// Memory backends share the allocation; eviction must not invalidate it.
    async fn get(
        &self,
        key: &str,
    ) -> crate::Result<Option<Bytes>>;
    async fn put(
        &self,
        key: &str,
        value: &[u8],
        ttl: Option<Duration>,
    ) -> crate::Result<()>;
    async fn forget(
        &self,
        key: &str,
    ) -> crate::Result<()>;

    async fn clear(&self) -> crate::Result<()> {
        Err(Error::Rejected(format!(
            "cache store '{}' with driver '{}' does not support clearing its namespace",
            self.store_name(),
            self.driver_name()
        )))
    }
}

/// Read-only view of the complete candidate store graph during composition.
///
/// Drivers use this only to resolve cross-store dependencies after every
/// store has been constructed. The view may represent the boot generation or
/// a not-yet-published reload generation.
pub trait CacheStoreResolver {
    fn store(
        &self,
        name: &str,
    ) -> crate::Result<Arc<dyn CacheStore>>;
}

impl CacheStoreResolver for HashMap<String, Arc<dyn CacheStore>> {
    fn store(
        &self,
        name: &str,
    ) -> crate::Result<Arc<dyn CacheStore>> {
        self.get(name).cloned().ok_or_else(|| {
            Error::Missing(format!("cache store '{name}' is not registered"))
        })
    }
}

/// Build-time factory for one cache backend family.
///
/// Third-party drivers register this trait with
/// [`CacheRegistry`](crate::CacheRegistry). They may deserialize the common
/// `cache.yaml` store options, ignore them, or use configuration owned by their
/// own provider.
pub trait CacheDriver: Any + Send + Sync + 'static {
    fn driver_name(&self) -> &'static str;

    fn create_store(
        self: Arc<Self>,
        store_name: &str,
        config: &CacheStoreConfig,
    ) -> crate::Result<Arc<dyn CacheStore>>;

    /// Build a store for a reload candidate.
    ///
    /// Most drivers use ordinary construction. Drivers backed by a
    /// process-lifetime service may reject a first runtime registration or
    /// reuse a logical store already owned by that service.
    fn create_reload_store(
        self: Arc<Self>,
        store_name: &str,
        config: &CacheStoreConfig,
    ) -> crate::Result<Arc<dyn CacheStore>> {
        self.create_store(store_name, config)
    }

    /// Complete links that require all named stores to exist.
    fn finish_store(
        &self,
        _store: &Arc<dyn CacheStore>,
        _stores: &dyn CacheStoreResolver,
    ) -> crate::Result<()> {
        Ok(())
    }
}

/// Driver-neutral operations for one named cache store.
///
/// The repository owns common value encoding while the underlying store stays
/// byte-oriented.
pub struct CacheRepository {
    store: Arc<dyn CacheStore>,
}

impl CacheRepository {
    pub(crate) fn new(store: Arc<dyn CacheStore>) -> Self {
        Self { store }
    }

    pub fn store_name(&self) -> &str {
        self.store.store_name()
    }

    pub fn driver_name(&self) -> &str {
        self.store.driver_name()
    }

    pub fn supports_ttl(&self) -> bool {
        self.store.supports_ttl()
    }

    pub fn as_any(&self) -> &dyn Any {
        self.store.as_any()
    }

    pub fn store(&self) -> Arc<dyn CacheStore> {
        self.store.clone()
    }

    /// Read bytes without copying a memory backend's allocation.
    /// Keep this owner alive while borrowing with [`CacheValue::view`].
    pub async fn get(
        &self,
        key: &str,
    ) -> crate::Result<Option<Bytes>> {
        self.store.get(key).await
    }

    pub async fn put(
        &self,
        key: &str,
        value: &[u8],
        ttl: Option<Duration>,
    ) -> crate::Result<()> {
        self.store.put(key, value, ttl).await
    }

    pub async fn get_value(
        &self,
        key: &str,
    ) -> crate::Result<Option<CacheValue>> {
        self.get(key)
            .await?
            .map(|bytes| CacheValue::decode(&bytes).map_err(Error::from))
            .transpose()
    }

    pub async fn put_value(
        &self,
        key: &str,
        value: CacheValue,
        ttl: Option<Duration>,
    ) -> crate::Result<()> {
        let encoded = value.encode().map_err(Error::from)?;
        self.put(key, &encoded, ttl).await
    }

    pub async fn forget(
        &self,
        key: &str,
    ) -> crate::Result<()> {
        self.store.forget(key).await
    }

    pub async fn clear(&self) -> crate::Result<()> {
        self.store.clear().await
    }
}
