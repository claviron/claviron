//! Claviron's Orbit cache binding.
//!
//! The framework-neutral ring and L1 mechanics remain in `orbitive::cache`. This
//! module adds one shared Claviron cache service, named logical-store
//! bindings, an abstract backing-store contract, L1/L2 ordering, the platform
//! notification driver.

mod runtime;

pub use orbitive::cache::{CachePoll, CacheRead, DEFAULT_L1_CAPACITY};
pub use runtime::{CacheBacking, CacheRuntime, CacheService, Error, Result};

/// Register the existing arena and managed mutation SHM companion before inserting a
/// [`CacheService`]. Repeated calls are idempotent.
pub fn register_shm(
    state: &claviron_runtime::AppState
) -> claviron_runtime::registry::Result<()> {
    runtime::register_shm(state)
}
