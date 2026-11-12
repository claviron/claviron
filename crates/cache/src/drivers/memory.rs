use std::any::Any;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use moka::Expiry;
use serde::Deserialize;

use crate::config::CacheStoreConfig;
use crate::memory::{MAX_TTL, MemoryCache};
use crate::{CacheDriver, CacheStore, Error, Result};

pub(crate) const NAME: &str = "memory";
pub(crate) const DEFAULT_CAPACITY: u64 = 10_000;

pub(crate) struct MemoryCacheDriver;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MemoryCacheStoreConfig {
    #[serde(default = "default_capacity")]
    capacity: u64,
}

fn default_capacity() -> u64 {
    DEFAULT_CAPACITY
}

pub(crate) struct MemoryCacheStore {
    name: String,
    cache: MemoryCache<String, Entry>,
}

impl MemoryCacheStore {
    pub(crate) fn new(
        name: &str,
        capacity: u64,
    ) -> Self {
        let cache = MemoryCache::builder()
            .max_capacity(capacity)
            .expire_after(EntryExpiry)
            .build();
        Self { name: name.to_owned(), cache }
    }
}

#[derive(Clone)]
struct Entry {
    bytes: Bytes,
    ttl: Option<Duration>,
}

struct EntryExpiry;

impl Expiry<String, Entry> for EntryExpiry {
    fn expire_after_create(
        &self,
        _key: &String,
        value: &Entry,
        _created_at: Instant,
    ) -> Option<Duration> {
        value.ttl
    }

    fn expire_after_update(
        &self,
        _key: &String,
        value: &Entry,
        _updated_at: Instant,
        _remaining: Option<Duration>,
    ) -> Option<Duration> {
        value.ttl
    }
}

impl CacheDriver for MemoryCacheDriver {
    fn driver_name(&self) -> &'static str {
        NAME
    }

    fn create_store(
        self: Arc<Self>,
        store_name: &str,
        config: &CacheStoreConfig,
    ) -> Result<Arc<dyn CacheStore>> {
        let config: MemoryCacheStoreConfig = config.deserialize_options()?;
        Ok(Arc::new(MemoryCacheStore::new(store_name, config.capacity)))
    }
}

#[async_trait]
impl CacheStore for MemoryCacheStore {
    fn store_name(&self) -> &str {
        &self.name
    }

    fn driver_name(&self) -> &str {
        NAME
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn supports_ttl(&self) -> bool {
        true
    }

    async fn get(
        &self,
        key: &str,
    ) -> Result<Option<Bytes>> {
        Ok(self.cache.get(key).map(|entry| entry.bytes))
    }

    async fn put(
        &self,
        key: &str,
        value: &[u8],
        ttl: Option<Duration>,
    ) -> Result<()> {
        if ttl.is_some_and(|ttl| ttl > MAX_TTL) {
            return Err(Error::Rejected(
                "memory cache TTL exceeds Moka's 1000-year limit".into(),
            ));
        }
        if ttl == Some(Duration::ZERO) {
            self.cache.invalidate(key);
        } else {
            self.cache.insert(
                key.to_owned(),
                Entry { bytes: Bytes::copy_from_slice(value), ttl },
            );
        }
        Ok(())
    }

    async fn forget(
        &self,
        key: &str,
    ) -> Result<()> {
        self.cache.invalidate(key);
        Ok(())
    }

    async fn clear(&self) -> Result<()> {
        self.cache.invalidate_all();
        Ok(())
    }
}

#[cfg(test)]
#[path = "../../tests/drivers/memory.rs"]
mod tests;
