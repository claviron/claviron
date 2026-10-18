use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

use arc_swap::ArcSwapOption;

use crate::config::CacheConfig;
use crate::provider::CacheCatalog;
use crate::{CacheDriver, CacheProvider, CacheStore, Error};

/// Registration-time extension point for cache drivers and stores.
///
/// Drivers may be added by downstream application registration after
/// `claviron-cache` registers its built-ins; `cache.yaml` arrives afterwards,
/// prepared by Congif. During boot the registry
/// builds every configured store from the registered drivers, completes
/// cross-store links, publishes the final [`CacheProvider`] catalog, and
/// seals itself against runtime mutation.
pub struct CacheRegistry {
    config: ArcSwapOption<CacheConfig>,
    drivers: RwLock<HashMap<String, Arc<dyn CacheDriver>>>,
    stores: RwLock<HashMap<String, Arc<dyn CacheStore>>>,
    ready_stores: RwLock<HashMap<String, Arc<dyn CacheStore>>>,
    provider: Arc<CacheProvider>,
    sealed: AtomicBool,
}

impl CacheRegistry {
    pub(crate) fn new(provider: Arc<CacheProvider>) -> Self {
        Self {
            config: ArcSwapOption::empty(),
            drivers: RwLock::new(HashMap::new()),
            stores: RwLock::new(HashMap::new()),
            ready_stores: RwLock::new(HashMap::new()),
            provider,
            sealed: AtomicBool::new(false),
        }
    }

    /// The prepared `cache.yaml` the sealing boot builds from. Until sealed
    /// a later preparation replaces the earlier one; after it, the provider's
    /// reload path takes over.
    pub(crate) fn configure(
        &self,
        config: CacheConfig,
    ) -> crate::Result<()> {
        self.ensure_open()?;
        validate_config(&config)?;
        self.config.store(Some(Arc::new(config)));
        Ok(())
    }

    fn config(&self) -> crate::Result<Arc<CacheConfig>> {
        self.config.load_full().ok_or_else(|| {
            Error::Missing(
                "cache configuration has not been applied".to_string(),
            )
        })
    }

    /// Register a driver. Its configured stores are built when the registry
    /// seals, from the configuration prepared by then.
    pub fn register_driver(
        &self,
        driver: Arc<dyn CacheDriver>,
    ) -> crate::Result<&Self> {
        self.ensure_open()?;
        let name = normalize_driver_name(driver.driver_name())?;
        let mut drivers =
            self.drivers.write().expect("cache drivers lock poisoned");
        if drivers.contains_key(&name) {
            return Err(Error::Rejected(format!(
                "cache driver '{name}' is already registered"
            )));
        }
        drivers.insert(name, driver);
        Ok(self)
    }

    /// Register a ready store built from driver-owned configuration.
    pub fn register_store(
        &self,
        store: Arc<dyn CacheStore>,
    ) -> crate::Result<&Self> {
        self.ensure_open()?;
        let name = validate_store_name(store.store_name())?;
        let mut stores =
            self.stores.write().expect("cache stores lock poisoned");
        if stores.contains_key(&name) {
            return Err(Error::Rejected(format!(
                "cache store '{name}' is already registered"
            )));
        }
        stores.insert(name.clone(), store.clone());
        self.ready_stores
            .write()
            .expect("cache ready stores lock poisoned")
            .insert(name, store);
        Ok(self)
    }

    pub fn store(
        &self,
        name: &str,
    ) -> crate::Result<Arc<dyn CacheStore>> {
        if self.is_sealed() {
            return self.provider.raw_store(name);
        }
        self.stores
            .read()
            .expect("cache stores lock poisoned")
            .get(name)
            .cloned()
            .ok_or_else(|| {
                Error::Missing(format!(
                    "cache store '{name}' is not registered"
                ))
            })
    }

    pub fn is_sealed(&self) -> bool {
        self.sealed.load(Ordering::Acquire)
    }

    fn ensure_open(&self) -> crate::Result<()> {
        if self.is_sealed() {
            return Err(Error::Rejected(
                "cache registry is already sealed".to_string(),
            ));
        }
        Ok(())
    }

    /// Build every configured store from the registered drivers, finish
    /// their links and publish the catalog. An empty configuration seals an
    /// empty catalog: no cache, until a reload declares one.
    pub(crate) fn seal(&self) -> crate::Result<()> {
        if self.is_sealed() {
            return Ok(());
        }
        let config = self.config()?;

        let drivers =
            self.drivers.read().expect("cache drivers lock poisoned").clone();
        let mut stores =
            self.stores.read().expect("cache stores lock poisoned").clone();
        let mut configured = config.stores.iter().collect::<Vec<_>>();
        configured.sort_unstable_by_key(|(name, _)| *name);
        for (store_name, store_config) in configured {
            if stores.contains_key(store_name) {
                return Err(Error::Rejected(format!(
                    "cache store '{store_name}' is already registered"
                )));
            }
            let Some(driver) = drivers.get(&store_config.driver_key()) else {
                continue;
            };
            let store =
                driver.clone().create_store(store_name, store_config)?;
            validate_built_store(store_name, &store)?;
            stores.insert(store_name.clone(), store);
        }
        validate_catalog(&config, &stores)?;

        for (store_name, store_config) in &config.stores {
            let Some(driver) = drivers.get(&store_config.driver_key()) else {
                continue;
            };
            let store = stores
                .get(store_name)
                .expect("configured cache store was checked above");
            driver.finish_store(store, &stores)?;
        }

        *self.stores.write().expect("cache stores lock poisoned") =
            stores.clone();
        self.provider.install(CacheCatalog::new((*config).clone(), stores))?;
        self.sealed.store(true, Ordering::Release);
        Ok(())
    }

    pub(crate) fn build_reload_catalog(
        &self,
        config: CacheConfig,
    ) -> crate::Result<CacheCatalog> {
        if !self.is_sealed() {
            return Err(Error::Rejected(
                "cache registry is not sealed".to_string(),
            ));
        }
        validate_config(&config)?;

        let current = self.provider.catalog()?;
        validate_orbit_reload(&current.config, &config)?;

        let drivers =
            self.drivers.read().expect("cache drivers lock poisoned").clone();
        let mut stores = self
            .ready_stores
            .read()
            .expect("cache ready stores lock poisoned")
            .clone();
        let mut unfinished = Vec::new();

        let mut configured = config.stores.iter().collect::<Vec<_>>();
        configured.sort_unstable_by_key(|(name, _)| *name);
        for (store_name, store_config) in configured {
            if stores.contains_key(store_name) {
                continue;
            }

            if store_config.driver_key() == "orbit"
                && current.config.stores.get(store_name) == Some(store_config)
            {
                stores.insert(store_name.clone(), current.store(store_name)?);
                continue;
            }

            let driver_name = store_config.driver_key();
            let driver = drivers.get(&driver_name).cloned().ok_or_else(|| {
                Error::Missing(format!(
                    "cache store '{store_name}' uses unregistered driver '{}'",
                    store_config.driver
                ))
            })?;
            let store =
                driver.clone().create_reload_store(store_name, store_config)?;
            validate_built_store(store_name, &store)?;
            stores.insert(store_name.clone(), store.clone());
            unfinished.push((driver, store));
        }

        validate_catalog(&config, &stores)?;
        for (driver, store) in unfinished {
            driver.finish_store(&store, &stores)?;
        }

        Ok(CacheCatalog::new(config, stores))
    }
}

/// A document that declares stores must name its default; one that declares
/// none composes no cache and needs none.
pub(crate) fn validate_config(config: &CacheConfig) -> crate::Result<()> {
    if config.is_empty() {
        return Ok(());
    }
    if config.default.trim().is_empty() {
        return Err(Error::Missing(
            "cache default store is not configured".to_string(),
        ));
    }
    Ok(())
}

fn validate_catalog(
    config: &CacheConfig,
    stores: &HashMap<String, Arc<dyn CacheStore>>,
) -> crate::Result<()> {
    for (store_name, store_config) in &config.stores {
        if !stores.contains_key(store_name) {
            return Err(Error::Missing(format!(
                "cache store '{store_name}' uses unregistered driver '{}'",
                store_config.driver
            )));
        }
    }
    if config.is_empty() && stores.is_empty() {
        return Ok(());
    }
    if !stores.contains_key(&config.default) {
        return Err(Error::Missing(format!(
            "cache default store '{}' is not registered",
            config.default
        )));
    }
    Ok(())
}

fn validate_built_store(
    expected_name: &str,
    store: &Arc<dyn CacheStore>,
) -> crate::Result<()> {
    let actual_name = validate_store_name(store.store_name())?;
    if actual_name != expected_name {
        return Err(Error::Rejected(format!(
            "cache driver built store '{actual_name}' for configured name '{expected_name}'"
        )));
    }
    Ok(())
}

fn validate_orbit_reload(
    current: &CacheConfig,
    candidate: &CacheConfig,
) -> crate::Result<()> {
    for (name, old) in current
        .stores
        .iter()
        .filter(|(_, config)| config.driver_key() == "orbit")
    {
        let Some(new) = candidate.stores.get(name) else {
            continue;
        };
        if old != new {
            return Err(Error::Rejected(format!(
                "Orbit cache store '{name}' cannot be reconfigured during reload; disable it or restart"
            )));
        }

        {
            let orbit: crate::OrbitCacheStoreConfig =
                old.deserialize_options()?;
            if let Some(l2_name) = orbit.l2
                && current.stores.get(&l2_name)
                    != candidate.stores.get(&l2_name)
            {
                return Err(Error::Rejected(format!(
                    "cache store '{l2_name}' backs Orbit store '{name}' and cannot be reconfigured during reload; restart is required"
                )));
            }
        }
    }
    Ok(())
}

fn normalize_driver_name(name: &str) -> crate::Result<String> {
    let name = name.trim().to_ascii_lowercase();
    if name.is_empty() {
        return Err(Error::Rejected(
            "cache driver name must not be empty".to_string(),
        ));
    }
    Ok(name)
}

fn validate_store_name(name: &str) -> crate::Result<String> {
    if name.is_empty() || name.trim() != name {
        return Err(Error::Rejected(
            "cache store name must not be empty or contain surrounding whitespace".to_string()
        ));
    }
    Ok(name.to_string())
}

#[cfg(test)]
#[path = "../tests/registry.rs"]
mod tests;
