use std::any::Any;
use std::borrow::Cow;
use std::sync::{Arc, PoisonError, RwLock};
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use async_trait::async_trait;
use bytes::Bytes;
use moka::Expiry;
use serde::{Deserialize, Deserializer};

use crate::config::CacheStoreConfig;
use crate::memory::MemoryCache;
use crate::{CacheDriver, CacheStore, Error, Result};

pub(crate) const NAME: &str = "memory";
const DEFAULT_CAPACITY: u64 = 10_000;

pub(crate) struct MemoryCacheDriver;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MemoryCacheStoreConfig {
    #[serde(default)]
    capacity: Option<u64>,
    #[serde(default, deserialize_with = "deserialize_byte_size")]
    max_bytes: Option<u64>,
}

/// What bounds a memory store's residency.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MemoryLimit {
    /// Resident entry count.
    Entries(u64),
    /// Resident key and value bytes; per-entry bookkeeping is not counted.
    Bytes(u64),
}

impl MemoryLimit {
    pub(crate) const DEFAULT: Self = Self::Entries(DEFAULT_CAPACITY);

    pub(crate) fn from_config(
        store_name: &str,
        config: &CacheStoreConfig,
    ) -> Result<Self> {
        let options: MemoryCacheStoreConfig = config.deserialize_options()?;
        match (options.capacity, options.max_bytes) {
            (Some(_), Some(_)) => Err(Error::Rejected(format!(
                "memory cache store '{store_name}' sets both capacity and max_bytes; choose one bound"
            ))),
            (Some(entries), None) => Ok(Self::Entries(entries)),
            (None, Some(bytes)) => Ok(Self::Bytes(bytes)),
            (None, None) => Ok(Self::DEFAULT),
        }
    }
}

/// Process-local byte store over Moka.
///
/// The store outlives catalog generations: reload keeps it, and a changed
/// bound moves the resident entries into a cache built with the new bound.
pub(crate) struct MemoryCacheStore {
    name: Box<str>,
    generation: ArcSwap<Generation>,
    /// Mutations hold this shared; a bound change holds it exclusively while
    /// it moves the entries, so no write lands in the cache being replaced.
    mutations: RwLock<()>,
}

struct Generation {
    limit: MemoryLimit,
    cache: MemoryCache<String, Entry>,
}

impl Generation {
    fn new(limit: MemoryLimit) -> Self {
        let builder = MemoryCache::builder().expire_after(EntryExpiry);
        let cache = match limit {
            MemoryLimit::Entries(entries) => {
                builder.max_capacity(entries).build()
            }
            MemoryLimit::Bytes(bytes) => builder
                .max_capacity(bytes)
                .weigher(|key: &String, entry: &Entry| {
                    u32::try_from(key.len() + entry.bytes.len())
                        .unwrap_or(u32::MAX)
                })
                .build(),
        };
        Self { limit, cache }
    }
}

impl MemoryCacheStore {
    pub(crate) fn new(
        name: &str,
        limit: MemoryLimit,
    ) -> Self {
        Self {
            name: name.into(),
            generation: ArcSwap::from_pointee(Generation::new(limit)),
            mutations: RwLock::new(()),
        }
    }

    /// Apply a new residency bound without dropping what the store holds.
    ///
    /// Moka fixes the bound when a cache is built, so a changed bound builds
    /// a new cache and moves every live entry into it with its remaining
    /// TTL. A lower bound then evicts the excess under the usual policy.
    /// Bound changes are serialized by the caller; the provider's reload lock
    /// does that.
    pub(crate) fn set_limit(
        &self,
        limit: MemoryLimit,
    ) {
        // An unchanged bound, the usual reload, does not hold writes off.
        if self.generation.load().limit == limit {
            return;
        }
        let _mutations =
            self.mutations.write().unwrap_or_else(PoisonError::into_inner);
        let current = self.generation.load_full();
        let next = Generation::new(limit);
        for (key, entry) in current.cache.iter() {
            next.cache.insert(String::clone(&key), entry);
        }
        self.generation.store(Arc::new(next));
    }

    fn mutate<T>(
        &self,
        mutation: impl FnOnce(&MemoryCache<String, Entry>) -> T,
    ) -> T {
        let _mutations =
            self.mutations.read().unwrap_or_else(PoisonError::into_inner);
        mutation(&self.generation.load().cache)
    }
}

#[derive(Clone)]
struct Entry {
    bytes: Bytes,
    lifetime: Option<Lifetime>,
}

#[derive(Clone, Copy)]
struct Lifetime {
    ttl: Duration,
    written_at: Instant,
}

impl Lifetime {
    /// What is left of the TTL at `at`. A fresh write gets its whole TTL; an
    /// entry moved by [`MemoryCacheStore::set_limit`] keeps only the rest.
    fn remaining(
        self,
        at: Instant,
    ) -> Duration {
        self.ttl.saturating_sub(at.saturating_duration_since(self.written_at))
    }
}

struct EntryExpiry;

impl Expiry<String, Entry> for EntryExpiry {
    fn expire_after_create(
        &self,
        _key: &String,
        value: &Entry,
        created_at: Instant,
    ) -> Option<Duration> {
        value.lifetime.map(|lifetime| lifetime.remaining(created_at))
    }

    fn expire_after_update(
        &self,
        _key: &String,
        value: &Entry,
        updated_at: Instant,
        _remaining: Option<Duration>,
    ) -> Option<Duration> {
        value.lifetime.map(|lifetime| lifetime.remaining(updated_at))
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
        let limit = MemoryLimit::from_config(store_name, config)?;
        Ok(Arc::new(MemoryCacheStore::new(store_name, limit)))
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
        Ok(self.generation.load().cache.get(key).map(|entry| entry.bytes))
    }

    async fn put(
        &self,
        key: &str,
        value: Cow<'_, [u8]>,
        ttl: Option<Duration>,
    ) -> Result<()> {
        if ttl == Some(Duration::ZERO) {
            self.mutate(|cache| cache.invalidate(key));
            return Ok(());
        }
        // Moka saturates a per-entry expiry beyond its clock range, so a TTL
        // of any length is kept rather than rejected.
        let entry = Entry {
            bytes: Bytes::from(value.into_owned()),
            lifetime: ttl
                .map(|ttl| Lifetime { ttl, written_at: Instant::now() }),
        };
        self.mutate(|cache| cache.insert(key.to_owned(), entry));
        Ok(())
    }

    async fn forget(
        &self,
        key: &str,
    ) -> Result<()> {
        self.mutate(|cache| cache.invalidate(key));
        Ok(())
    }

    async fn clear(&self) -> Result<()> {
        self.mutate(MemoryCache::invalidate_all);
        Ok(())
    }
}

/// A byte count or a size such as `64 MiB`.
fn deserialize_byte_size<'de, D>(
    deserializer: D
) -> std::result::Result<Option<u64>, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Size {
        Bytes(u64),
        Human(
            #[serde(
                deserialize_with = "claviron_helpers::de::deserialize_byte_size"
            )]
            u64,
        ),
    }

    Ok(Option::<Size>::deserialize(deserializer)?.map(|size| match size {
        Size::Bytes(bytes) | Size::Human(bytes) => bytes,
    }))
}

#[cfg(test)]
#[path = "../../tests/drivers/memory.rs"]
mod tests;
