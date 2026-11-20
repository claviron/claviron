use std::any::Any;
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;

use crate::config::CacheStoreConfig;
use crate::{CacheValue, Error};

/// Expiry observed with a backing value in the same read.
#[derive(Clone, Copy, Debug)]
pub enum CacheExpiry {
    Unknown,
    Never,
    At(Instant),
}

pub struct CacheEntry {
    pub bytes: Bytes,
    pub expiry: CacheExpiry,
}

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

    /// Read a value and its expiry together for a cache that can retain it.
    /// Backends without this information can still serve the current miss.
    async fn get_entry(
        &self,
        key: &str,
    ) -> crate::Result<Option<CacheEntry>> {
        Ok(self
            .get(key)
            .await?
            .map(|bytes| CacheEntry { bytes, expiry: CacheExpiry::Unknown }))
    }
    /// Store `value` under `key`. An owned buffer lets a backend that retains
    /// bytes in process keep the allocation; others borrow it.
    async fn put(
        &self,
        key: &str,
        value: Cow<'_, [u8]>,
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

    /// This store's tagged operations, or `None` when it cannot keep tags.
    ///
    /// The answer belongs to the built store, not to its driver: one driver
    /// can build stores that differ, as an Orbit store does with and without
    /// a backing store. A store answers `Some` only when an invalidation
    /// reaches every value it can return afterwards, including what a miss
    /// reads from a lower tier. Untagged operations do not depend on it.
    fn tagged(&self) -> Option<&dyn TaggedCacheStore> {
        None
    }
}

/// A position on a store's tag-invalidation clock, read before a backing
/// read and given back to [`TaggedCacheStore::fill_tagged`] with its result.
///
/// Opaque to everyone but the store that issued it: `source` names the
/// clock, `clock` the position on it. A store refuses a token from another
/// clock as [`Fill::Invalidated`]. 16 bytes, `Copy`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct FillToken {
    source: u64,
    clock: u64,
}

impl FillToken {
    /// For a store implementing [`TaggedCacheStore::fill_token`].
    pub const fn new(
        source: u64,
        clock: u64,
    ) -> Self {
        Self { source, clock }
    }

    pub const fn source(self) -> u64 {
        self.source
    }

    pub const fn clock(self) -> u64 {
        self.clock
    }
}

/// What [`TaggedCacheStore::fill_tagged`] did with its value. Every variant
/// but [`Fill::Kept`] leaves the store as it was, and none is an error: the
/// value is still the right answer for the read that produced it.
#[must_use = "a refused fill is not an error, but nothing was stored"]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Fill {
    /// Stored; reads may return it until a newer write of its key, an
    /// invalidation of one of its tags newer than its token, its expiry, or
    /// eviction.
    Kept,
    /// Refused by the fence: an invalidation of one of its tags may be newer
    /// than the token, or the token is from another clock.
    Invalidated,
    /// Refused by its key: the store holds or has recorded a write of this
    /// key newer than the token.
    Superseded,
    /// Not kept, for a reason that says nothing about staleness, such as an
    /// already expired value or no room.
    Skipped,
}

impl Fill {
    pub const fn is_kept(self) -> bool {
        matches!(self, Self::Kept)
    }
}

/// Tagged writes, fenced fills and invalidation by tag for a store that
/// supports them.
///
/// Reached through [`CacheStore::tagged`]. These are separate entry points
/// rather than an optional argument to `put`, so a store without tags refuses
/// the tagged call instead of silently dropping its tags. Reads are the same
/// for tagged and untagged entries. A store with tags has the fenced fill as
/// well: without it, a value read from a source while one of its tags was
/// invalidated could be cached with nothing left to remove it.
#[async_trait]
pub trait TaggedCacheStore: Send + Sync {
    /// Store `value` under `key` so that [`Self::invalidate_tags`] of any of
    /// `tags` makes it a miss. An empty set is an untagged put. A set larger
    /// than the store keeps is [`Error::TooManyTags`]; nothing is truncated.
    async fn put_tagged(
        &self,
        key: &str,
        value: Cow<'_, [u8]>,
        ttl: Option<Duration>,
        tags: &[&str],
    ) -> crate::Result<()>;

    /// This store's position on its tag-invalidation clock. Take it
    /// immediately before reading the authoritative source, and before any
    /// snapshot that read uses.
    async fn fill_token(&self) -> crate::Result<FillToken>;

    /// Store `value`, read from the authoritative source after `token` was
    /// taken, under `tags`, unless an invalidation of one of them is newer
    /// than the token: an invalidation whose clock position is at or below
    /// the token ran before the read began and is in what it returned; one
    /// above it refuses the fill ([`Fill::Invalidated`]), whether it reaches
    /// the store before the fill or after. Arguments the store refuses are
    /// errors, as for [`Self::put_tagged`]; an empty set is an untagged fill.
    async fn fill_tagged(
        &self,
        token: FillToken,
        key: &str,
        value: Cow<'_, [u8]>,
        ttl: Option<Duration>,
        tags: &[&str],
    ) -> crate::Result<Fill>;

    /// Make every entry carrying any of `tags`, written before this call, a
    /// miss. An empty set is [`Error::TagsEmpty`]. Change the source first,
    /// then invalidate: a fill whose token was read after this call returns
    /// is kept.
    async fn invalidate_tags(
        &self,
        tags: &[&str],
    ) -> crate::Result<()>;
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

    /// Whether this store accepts [`Self::put_tagged`] and
    /// [`Self::invalidate_tags`]. Fixed for the store's life; a reload that
    /// changes it builds another store, resolved again through the provider.
    pub fn supports_tags(&self) -> bool {
        self.store.tagged().is_some()
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

    pub async fn put<'v>(
        &self,
        key: &str,
        value: impl Into<Cow<'v, [u8]>>,
        ttl: Option<Duration>,
    ) -> crate::Result<()> {
        self.store.put(key, value.into(), ttl).await
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
        self.put(key, encoded, ttl).await
    }

    /// Store bytes that an invalidation of any of `tags` makes a miss.
    /// Refused with [`Error::TagsUnsupported`] when [`Self::supports_tags`]
    /// is false, before anything is written.
    pub async fn put_tagged<'v>(
        &self,
        key: &str,
        value: impl Into<Cow<'v, [u8]>>,
        ttl: Option<Duration>,
        tags: &[&str],
    ) -> crate::Result<()> {
        self.tagged()?.put_tagged(key, value.into(), ttl, tags).await
    }

    pub async fn put_value_tagged(
        &self,
        key: &str,
        value: CacheValue,
        ttl: Option<Duration>,
        tags: &[&str],
    ) -> crate::Result<()> {
        let tagged = self.tagged()?;
        let encoded = value.encode().map_err(Error::from)?;
        tagged.put_tagged(key, encoded.into(), ttl, tags).await
    }

    /// The token to take before a backing read whose result goes to
    /// [`Self::fill_tagged`]. Refused like [`Self::put_tagged`].
    pub async fn fill_token(&self) -> crate::Result<FillToken> {
        self.tagged()?.fill_token().await
    }

    /// Store bytes read from an authoritative source after `token`, unless
    /// an invalidation of one of `tags` is newer than the token; see
    /// [`TaggedCacheStore::fill_tagged`]. Refused like [`Self::put_tagged`].
    pub async fn fill_tagged<'v>(
        &self,
        token: FillToken,
        key: &str,
        value: impl Into<Cow<'v, [u8]>>,
        ttl: Option<Duration>,
        tags: &[&str],
    ) -> crate::Result<Fill> {
        self.tagged()?.fill_tagged(token, key, value.into(), ttl, tags).await
    }

    /// Make every entry carrying any of `tags` a miss. Refused like
    /// [`Self::put_tagged`].
    pub async fn invalidate_tags(
        &self,
        tags: &[&str],
    ) -> crate::Result<()> {
        self.tagged()?.invalidate_tags(tags).await
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

    fn tagged(&self) -> crate::Result<&dyn TaggedCacheStore> {
        self.store.tagged().ok_or_else(|| Error::TagsUnsupported {
            store: self.store_name().into(),
            driver: self.driver_name().into(),
        })
    }
}
