use std::sync::Arc;

use claviron_runtime::AppState;
use tracing::info;

#[cfg(feature = "file")]
use crate::FileCacheDriver;
use crate::drivers::orbit::OrbitCacheDriver;
#[cfg(feature = "redis")]
use crate::drivers::redis::RedisCacheDriver;
use crate::{CacheConfig, CacheProvider, CacheRegistry};

/// Compose the cache: the registry, the provider that owns `cache.yaml`, and
/// the built-in drivers. Congif prepares the file before boot; the sealing
/// `configure` builds the configured stores from the registered drivers. Without the
/// file there is no store until a reload declares one, and the Orbit cache's
/// rings and service stand ready for that, as the ring of every fleet-wide
/// service does.
pub fn register(state: &AppState) -> claviron_runtime::registry::Result<()> {
    if state.registry_ref().resolve::<CacheProvider>().is_some() {
        return Ok(());
    }
    compose(state, None)
}

/// Supply the existing cache schema from an enclosing launch document.
/// Driver construction and sealing remain lifecycle work, after all application
/// registrations. This immutable boot intent does not read or watch cache.yaml.
pub fn register_with_config(
    state: &AppState,
    config: CacheConfig,
) -> claviron_runtime::registry::Result<()> {
    crate::registry::validate_config(&config)?;
    if state.registry_ref().resolve::<CacheProvider>().is_some() {
        return Err(claviron_runtime::registry::Error::msg(
            "cache provider already registered",
        ));
    }
    compose(state, Some(config))
}

fn compose(
    state: &AppState,
    config: Option<CacheConfig>,
) -> claviron_runtime::registry::Result<()> {
    let from_file = config.is_none();
    #[cfg(feature = "redis")]
    let redis = crate::drivers::redis::register(state)?;

    let cache_path = state.config_dir().join("cache.yaml");
    let provider = if let Some(config) = config {
        CacheProvider::from_config(config)
    } else {
        state.bind_config_environment::<CacheProvider>()?;
        CacheProvider::new(cache_path.clone())
    };
    let provider = Arc::new(provider);
    let cache_registry = Arc::new(CacheRegistry::new(provider.clone()));
    provider.attach_registry(Arc::downgrade(&cache_registry))?;
    #[cfg(feature = "file")]
    cache_registry.register_driver(Arc::new(FileCacheDriver))?;

    #[cfg(feature = "redis")]
    cache_registry
        .register_driver(Arc::new(RedisCacheDriver::new(redis.clone())))?;

    let orbit = Arc::new(OrbitCacheDriver::new());
    cache_registry.register_driver(orbit.clone())?;

    let registry = state.registry_ref();
    registry.insert(cache_registry)?.register(provider)?;

    claviron_orbit::cache::register_shm(state)?;
    registry.register(orbit.service())?;

    if from_file {
        info!(path = %cache_path.display(), "📦 cache registry composed; stores follow the prepared configuration");
    } else {
        info!("📦 cache registry composed from supplied configuration");
    }
    Ok(())
}
