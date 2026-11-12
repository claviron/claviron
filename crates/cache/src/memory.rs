//! Typed process-local caching for synchronous callers.

use std::hash::Hash;
use std::time::Duration;

pub use moka::sync::Cache as MemoryCache;

/// Maximum TTL accepted by the memory cache.
pub const MAX_TTL: Duration = Duration::from_secs(1_000 * 365 * 24 * 3600);

/// Construct a bounded cache with a fixed time to live.
///
/// # Panics
///
/// Panics if `ttl` exceeds [`MAX_TTL`].
pub fn new<K, V>(
    capacity: u64,
    ttl: Duration,
) -> MemoryCache<K, V>
where
    K: Eq + Hash + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    MemoryCache::builder().max_capacity(capacity).time_to_live(ttl).build()
}
