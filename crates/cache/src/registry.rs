use std::any::Any;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

use arc_swap::ArcSwapOption;

use crate::config::CacheConfig;
use crate::drivers::memory::{self, MemoryCacheStore, MemoryLimit};
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
    /// their links and publish the catalog with its default store.
    pub(crate) fn seal(&self) -> crate::Result<()> {
        if self.is_sealed() {
            return Ok(());
        }
        let mut config = (*self.config()?).clone();

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
        complete_catalog(&mut config, &mut stores)?;

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
        self.provider.install(CacheCatalog::new(config, stores))?;
        self.sealed.store(true, Ordering::Release);
        Ok(())
    }

    pub(crate) fn build_reload_catalog(
        &self,
        mut config: CacheConfig,
    ) -> crate::Result<ReloadCatalog> {
        if !self.is_sealed() {
            return Err(Error::Rejected(
                "cache registry is not sealed".to_string(),
            ));
        }
        validate_config(&config)?;

        let current = self.provider.catalog()?;
        #[cfg(feature = "orbit")]
        validate_orbit_reload(&current.config, &config)?;

        let drivers =
            self.drivers.read().expect("cache drivers lock poisoned").clone();
        let mut stores = self
            .ready_stores
            .read()
            .expect("cache ready stores lock poisoned")
            .clone();
        let mut unfinished = Vec::new();
        let mut memory_limits = Vec::new();

        // Keep the exact backing instances used by retained memory stores
        // visible through the new catalog as well.
        for (store_name, store_config) in &config.stores {
            if store_config.driver_key() == memory::NAME
                && let Some(store) = current_memory_store(&current, store_name)
                && let Some(l2_name) = store.l2_name()
            {
                if current.config.stores.get(l2_name)
                    != config.stores.get(l2_name)
                {
                    return Err(Error::Rejected(format!(
                        "cache store '{l2_name}' backs memory store '{store_name}' and cannot be reconfigured during reload; restart is required"
                    )));
                }
                if l2_name == memory::NAME
                    && !config.stores.contains_key(l2_name)
                    && !selects_implicit_memory(&config)
                    && !stores.contains_key(l2_name)
                {
                    return Err(Error::Rejected(format!(
                        "cannot remove the implicit memory store while it backs '{store_name}'"
                    )));
                }
                stores.insert(l2_name.to_owned(), current.store(l2_name)?);
            }
        }

        let mut configured = config.stores.iter().collect::<Vec<_>>();
        configured.sort_unstable_by_key(|(name, _)| *name);
        for (store_name, store_config) in configured {
            if stores.contains_key(store_name) {
                continue;
            }

            #[cfg(feature = "orbit")]
            if store_config.driver_key() == "orbit"
                && current.config.stores.get(store_name) == Some(store_config)
            {
                stores.insert(store_name.clone(), current.store(store_name)?);
                continue;
            }

            // A memory store's contents are process state, not configuration:
            // keep the store and stage its bound for commit.
            if store_config.driver_key() == memory::NAME
                && let Some(store) = current_memory_store(&current, store_name)
            {
                let limit = store.reload_limit(store_config)?;
                stores.insert(store_name.clone(), store.clone());
                memory_limits.push((store, limit));
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

        if selects_implicit_memory(&config)
            && !stores.contains_key(memory::NAME)
            && let Some(store) = current_memory_store(&current, memory::NAME)
        {
            if store.l2_name().is_some() {
                return Err(Error::Rejected(
                    "the default memory store cannot remove its L2 during reload; restart is required".into(),
                ));
            }
            stores.insert(memory::NAME.to_owned(), store.clone());
            memory_limits.push((store, MemoryLimit::DEFAULT));
        }

        complete_catalog(&mut config, &mut stores)?;
        for (driver, store) in unfinished {
            driver.finish_store(&store, &stores)?;
        }

        Ok(ReloadCatalog {
            catalog: CacheCatalog::new(config, stores),
            memory_limits,
        })
    }
}

/// A validated reload candidate.
///
/// Memory stores carried over from the current generation keep their
/// contents; their new bounds apply only once the whole candidate has been
/// built, so a rejected reload leaves every live store as it was.
pub(crate) struct ReloadCatalog {
    catalog: CacheCatalog,
    memory_limits: Vec<(Arc<MemoryCacheStore>, MemoryLimit)>,
}

impl ReloadCatalog {
    /// Apply the staged bounds and hand over the catalog to publish.
    pub(crate) fn commit(self) -> CacheCatalog {
        for (store, limit) in self.memory_limits {
            store.set_limit(limit);
        }
        self.catalog
    }
}

fn current_memory_store(
    current: &CacheCatalog,
    name: &str,
) -> Option<Arc<MemoryCacheStore>> {
    let store: Arc<dyn Any + Send + Sync> = current.store(name).ok()?;
    store.downcast::<MemoryCacheStore>().ok()
}

/// Whether the catalog will supply the built-in `memory` default itself.
fn selects_implicit_memory(config: &CacheConfig) -> bool {
    (config.default.trim().is_empty() || config.default == memory::NAME)
        && !config.stores.contains_key(memory::NAME)
}

/// Validate an explicit default name. An omitted name selects memory.
pub(crate) fn validate_config(config: &CacheConfig) -> crate::Result<()> {
    if !config.default.trim().is_empty() {
        validate_store_name(&config.default)?;
    }
    Ok(())
}

fn complete_catalog(
    config: &mut CacheConfig,
    stores: &mut HashMap<String, Arc<dyn CacheStore>>,
) -> crate::Result<()> {
    for (store_name, store_config) in &config.stores {
        if !stores.contains_key(store_name) {
            return Err(Error::Missing(format!(
                "cache store '{store_name}' uses unregistered driver '{}'",
                store_config.driver
            )));
        }
    }
    if config.default.trim().is_empty() {
        if stores
            .get(memory::NAME)
            .is_some_and(|store| store.driver_name() != memory::NAME)
        {
            return Err(Error::Rejected(
                "cache store 'memory' uses another driver; choose a default explicitly".into(),
            ));
        }
        config.default = memory::NAME.to_owned();
    }
    if config.default == memory::NAME {
        stores.entry(config.default.clone()).or_insert_with(|| {
            Arc::new(MemoryCacheStore::new(memory::NAME, MemoryLimit::DEFAULT))
        });
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

#[cfg(feature = "orbit")]
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
