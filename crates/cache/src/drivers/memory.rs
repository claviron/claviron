use std::any::Any;
use std::borrow::Cow;
use std::sync::{Arc, OnceLock, PoisonError, RwLock};
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use async_trait::async_trait;
use bytes::Bytes;
use moka::Expiry;
use serde::{Deserialize, Deserializer};

use crate::config::CacheStoreConfig;
use crate::memory::MemoryCache;
use crate::{
    CacheDriver, CacheEntry, CacheExpiry, CacheStore, CacheStoreResolver,
    Error, Result,
};

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
    #[serde(default)]
    l2: Option<Box<str>>,
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
}

impl MemoryCacheStoreConfig {
    fn limit(
        &self,
        store_name: &str,
    ) -> Result<MemoryLimit> {
        match (self.capacity, self.max_bytes) {
            (Some(_), Some(_)) => Err(Error::Rejected(format!(
                "memory cache store '{store_name}' sets both capacity and max_bytes; choose one bound"
            ))),
            (Some(entries), None) => Ok(MemoryLimit::Entries(entries)),
            (None, Some(bytes)) => Ok(MemoryLimit::Bytes(bytes)),
            (None, None) => Ok(MemoryLimit::DEFAULT),
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
    l2_name: Option<Box<str>>,
    backing: OnceLock<Backing>,
}

struct Backing {
    store: Arc<dyn CacheStore>,
    operations: tokio::sync::RwLock<()>,
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
            l2_name: None,
            backing: OnceLock::new(),
        }
    }

    pub(crate) fn reload_limit(
        &self,
        config: &CacheStoreConfig,
    ) -> Result<MemoryLimit> {
        let options: MemoryCacheStoreConfig = config.deserialize_options()?;
        if self.l2_name != options.l2 {
            return Err(Error::Rejected(format!(
                "memory cache store '{}' cannot change its L2 during reload; restart is required",
                self.name
            )));
        }
        options.limit(&self.name)
    }

    pub(crate) fn l2_name(&self) -> Option<&str> {
        self.l2_name.as_deref()
    }

    fn local_entry(
        &self,
        key: &str,
    ) -> Option<CacheEntry> {
        self.generation.load().cache.get(key).map(Entry::into_cache_entry)
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
    lifetime: Option<(Instant, Duration)>,
}

impl Entry {
    fn remaining(
        &self,
        at: Instant,
    ) -> Option<Duration> {
        self.lifetime.map(|(started, ttl)| {
            ttl.saturating_sub(at.saturating_duration_since(started))
        })
    }

    fn into_cache_entry(self) -> CacheEntry {
        let expiry =
            self.lifetime.map_or(CacheExpiry::Never, |(started, ttl)| {
                started
                    .checked_add(ttl)
                    .map_or(CacheExpiry::Unknown, CacheExpiry::At)
            });
        CacheEntry { bytes: self.bytes, expiry }
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
        value.remaining(created_at)
    }

    fn expire_after_update(
        &self,
        _key: &String,
        value: &Entry,
        updated_at: Instant,
        _remaining: Option<Duration>,
    ) -> Option<Duration> {
        value.remaining(updated_at)
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
        let options: MemoryCacheStoreConfig = config.deserialize_options()?;
        let mut store =
            MemoryCacheStore::new(store_name, options.limit(store_name)?);
        store.l2_name = options.l2;
        Ok(Arc::new(store))
    }

    fn finish_store(
        &self,
        store: &Arc<dyn CacheStore>,
        stores: &dyn CacheStoreResolver,
    ) -> Result<()> {
        let memory = store
            .as_any()
            .downcast_ref::<MemoryCacheStore>()
            .expect("memory driver creates a memory store");
        let Some(name) = memory.l2_name() else {
            return Ok(());
        };
        let backing = stores.store(name)?;
        if Arc::ptr_eq(store, &backing)
            || backing.driver_name() == "orbit"
            || backing
                .as_any()
                .downcast_ref::<MemoryCacheStore>()
                .is_some_and(|store| store.l2_name().is_some())
        {
            return Err(Error::Rejected(format!(
                "memory cache store '{}' requires a single-tier L2, got '{name}'",
                memory.name
            )));
        }
        memory
            .backing
            .set(Backing {
                store: backing,
                operations: tokio::sync::RwLock::new(()),
            })
            .map_err(|_| {
                Error::Rejected(format!(
                    "memory cache store '{}' already has an L2",
                    memory.name
                ))
            })?;
        Ok(())
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
        self.backing.get().is_none_or(|backing| backing.store.supports_ttl())
    }

    async fn get(
        &self,
        key: &str,
    ) -> Result<Option<Bytes>> {
        if let Some(entry) = self.generation.load().cache.get(key) {
            return Ok(Some(entry.bytes));
        }
        Ok(self.read_backing(key).await?.map(|entry| entry.bytes))
    }

    async fn get_entry(
        &self,
        key: &str,
    ) -> Result<Option<CacheEntry>> {
        if let Some(entry) = self.local_entry(key) {
            return Ok(Some(entry));
        }
        self.read_backing(key).await
    }

    async fn put(
        &self,
        key: &str,
        value: Cow<'_, [u8]>,
        ttl: Option<Duration>,
    ) -> Result<()> {
        if ttl == Some(Duration::ZERO) {
            return self.forget(key).await;
        }
        let backing = self.backing.get();
        let _operation = if let Some(backing) = backing {
            if ttl.is_some() && !backing.store.supports_ttl() {
                return Err(Error::Rejected(format!(
                    "backing store '{}' does not preserve TTL for key '{key}'",
                    backing.store.store_name()
                )));
            }
            Some(backing.operations.write().await)
        } else {
            None
        };
        let lifetime = ttl.map(|ttl| (Instant::now(), ttl));
        if let Some(backing) = backing {
            self.mutate(|cache| cache.invalidate(key));
            backing.store.put(key, Cow::Borrowed(value.as_ref()), ttl).await?;
        }
        let entry = Entry { bytes: Bytes::from(value.into_owned()), lifetime };
        self.mutate(|cache| cache.insert(key.to_owned(), entry));
        Ok(())
    }

    async fn forget(
        &self,
        key: &str,
    ) -> Result<()> {
        if let Some(backing) = self.backing.get() {
            let _operation = backing.operations.write().await;
            self.mutate(|cache| cache.invalidate(key));
            return backing.store.forget(key).await;
        }
        self.mutate(|cache| cache.invalidate(key));
        Ok(())
    }

    async fn clear(&self) -> Result<()> {
        if let Some(backing) = self.backing.get() {
            let _operation = backing.operations.write().await;
            self.mutate(MemoryCache::invalidate_all);
            return backing.store.clear().await;
        }
        self.mutate(MemoryCache::invalidate_all);
        Ok(())
    }
}

impl MemoryCacheStore {
    async fn read_backing(
        &self,
        key: &str,
    ) -> Result<Option<CacheEntry>> {
        let Some(backing) = self.backing.get() else {
            return Ok(None);
        };
        let _operation = backing.operations.read().await;
        if let Some(entry) = self.local_entry(key) {
            return Ok(Some(entry));
        }
        let Some(entry) = backing.store.get_entry(key).await? else {
            return Ok(None);
        };
        let lifetime = match entry.expiry {
            CacheExpiry::At(deadline) => {
                let now = Instant::now();
                let ttl = deadline.saturating_duration_since(now);
                if ttl.is_zero() {
                    return Ok(None);
                }
                Some((now, ttl))
            }
            CacheExpiry::Unknown => return Ok(Some(entry)),
            CacheExpiry::Never => None,
        };
        let cached = self.mutate(|cache| {
            // Another miss can fill the entry while this read is in flight.
            cache.get_with(key.to_owned(), || Entry {
                bytes: entry.bytes,
                lifetime,
            })
        });
        Ok(Some(cached.into_cache_entry()))
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
