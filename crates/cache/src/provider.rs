use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, OnceLock, Weak};
use std::time::Duration;

use arc_swap::ArcSwapOption;
use async_trait::async_trait;
use claviron_runtime::congif::{
    self, ConfigFile, ConfigObserver, Configurable,
};
use claviron_runtime::registry::{Bootable, Component, Reloadable};
use claviron_runtime::{AppState, Register};
use tokio::sync::Mutex;
use tracing::info;

use crate::config::CacheConfig;
use crate::store::CacheRepository;
use crate::{CacheRegistry, CacheStore, CacheValue, Error};

pub(crate) struct CacheCatalog {
    pub(crate) config: CacheConfig,
    default: String,
    stores: HashMap<String, Arc<dyn CacheStore>>,
    repositories: HashMap<String, Arc<CacheRepository>>,
}

impl CacheCatalog {
    pub(crate) fn new(
        config: CacheConfig,
        stores: HashMap<String, Arc<dyn CacheStore>>,
    ) -> Self {
        let repositories = stores
            .iter()
            .map(|(name, store)| {
                (name.clone(), Arc::new(CacheRepository::new(store.clone())))
            })
            .collect();
        Self { default: config.default.clone(), config, stores, repositories }
    }

    pub(crate) fn store(
        &self,
        name: &str,
    ) -> crate::Result<Arc<dyn CacheStore>> {
        self.stores.get(name).cloned().ok_or_else(|| {
            Error::Missing(format!("cache store '{name}' is not registered"))
        })
    }
}

/// Reloadable runtime cache facade, and the owner of `cache.yaml`.
///
/// [`CacheRegistry`] owns driver discovery and candidate construction. This
/// provider owns the active catalog generation: Congif prepares `cache.yaml`
/// before boot and in the reload batch; `configure` hands the registry its
/// composition before the sealing boot, and a reload or a prepared source
/// change asks the sealed registry to build and validate a complete
/// replacement, then publishes it atomically. Handles resolved before
/// publication keep their old repository alive; later resolutions see the new
/// generation. An absent file composes no cache until one is declared.
pub struct CacheProvider {
    config_path: PathBuf,
    initial_config: Option<CacheConfig>,
    catalog: ArcSwapOption<CacheCatalog>,
    registry: OnceLock<Weak<CacheRegistry>>,
    reload_lock: Mutex<()>,
}

impl Register<AppState> for CacheProvider {
    fn register(state: &AppState) -> claviron_runtime::Result<()> {
        crate::register(state)
    }
}

impl CacheProvider {
    pub(crate) fn new(config_path: PathBuf) -> Self {
        Self {
            config_path,
            initial_config: None,
            catalog: ArcSwapOption::empty(),
            registry: OnceLock::new(),
            reload_lock: Mutex::new(()),
        }
    }

    pub(crate) fn from_config(config: CacheConfig) -> Self {
        Self { initial_config: Some(config), ..Self::new(PathBuf::new()) }
    }

    pub(crate) fn is_file_configured(&self) -> bool {
        self.initial_config.is_none()
    }

    pub(crate) fn attach_registry(
        &self,
        registry: Weak<CacheRegistry>,
    ) -> crate::Result<()> {
        self.registry.set(registry).map_err(|_| {
            Error::Rejected("cache registry is already attached".to_string())
        })
    }

    pub(crate) fn install(
        &self,
        catalog: CacheCatalog,
    ) -> crate::Result<()> {
        if self.catalog.load().is_some() {
            return Err(Error::Rejected(
                "cache provider is already built".to_string(),
            ));
        }
        self.publish(catalog);
        Ok(())
    }

    pub(crate) fn publish(
        &self,
        catalog: CacheCatalog,
    ) {
        self.catalog.store(Some(Arc::new(catalog)));
    }

    pub(crate) fn catalog(&self) -> crate::Result<Arc<CacheCatalog>> {
        self.catalog.load_full().ok_or_else(|| {
            Error::Missing("cache provider is not built yet".to_string())
        })
    }

    pub(crate) fn raw_store(
        &self,
        name: &str,
    ) -> crate::Result<Arc<dyn CacheStore>> {
        self.catalog()?.store(name)
    }

    pub fn store(
        &self,
        name: &str,
    ) -> crate::Result<Arc<CacheRepository>> {
        self.catalog()?.repositories.get(name).cloned().ok_or_else(|| {
            Error::Missing(format!("cache store '{name}' is not registered"))
        })
    }

    pub fn default_store_name(&self) -> crate::Result<String> {
        Ok(self.catalog()?.default.clone())
    }

    pub fn default_store(&self) -> crate::Result<Arc<CacheRepository>> {
        let catalog = self.catalog()?;
        if catalog.default.is_empty() {
            return Err(Error::Missing(
                "no cache store is configured".to_string(),
            ));
        }
        catalog.repositories.get(&catalog.default).cloned().ok_or_else(|| {
            Error::Missing(format!(
                "cache default store '{}' is missing",
                catalog.default
            ))
        })
    }

    fn registry(
        &self
    ) -> claviron_runtime::registry::Result<Arc<CacheRegistry>> {
        self.registry.get().and_then(Weak::upgrade).ok_or_else(|| {
            claviron_runtime::registry::Error::msg(
                "CacheRegistry is unavailable",
            )
        })
    }

    /// Publish `config` as the next catalog generation of a sealed registry.
    fn republish(
        &self,
        registry: &CacheRegistry,
        config: CacheConfig,
    ) -> claviron_runtime::registry::Result<()> {
        let catalog =
            registry.build_reload_catalog(config).map_err(|error| {
                claviron_runtime::registry::Error::msg(format!(
                    "cache catalog reload failed: {error}"
                ))
            })?;
        self.publish(catalog);
        Ok(())
    }

    pub async fn get(
        &self,
        key: &str,
    ) -> crate::Result<Option<bytes::Bytes>> {
        self.default_store()?.get(key).await
    }

    pub async fn put(
        &self,
        key: &str,
        value: &[u8],
        ttl: Option<Duration>,
    ) -> crate::Result<()> {
        self.default_store()?.put(key, value, ttl).await
    }

    pub async fn get_value(
        &self,
        key: &str,
    ) -> crate::Result<Option<CacheValue>> {
        self.default_store()?.get_value(key).await
    }

    pub async fn put_value(
        &self,
        key: &str,
        value: CacheValue,
        ttl: Option<Duration>,
    ) -> crate::Result<()> {
        self.default_store()?.put_value(key, value, ttl).await
    }

    pub async fn forget(
        &self,
        key: &str,
    ) -> crate::Result<()> {
        self.default_store()?.forget(key).await
    }

    pub async fn clear(&self) -> crate::Result<()> {
        self.default_store()?.clear().await
    }
}

impl Component<AppState> for CacheProvider {
    type Deps = ();

    fn name(&self) -> &'static str {
        "CacheProvider"
    }

    fn validate(
        &self,
        _state: &AppState,
    ) -> claviron_runtime::registry::Result<()> {
        self.catalog().map(|_| ()).map_err(|error| {
            claviron_runtime::registry::Error::msg(error.to_string())
        })
    }

    fn as_reloadable(&self) -> Option<&dyn Reloadable<AppState>> {
        self.is_file_configured().then_some(self)
    }

    fn as_configurable(&self) -> Option<&dyn congif::ConfigRegistration> {
        self.is_file_configured().then_some(self)
    }

    fn as_bootable(&self) -> Option<&dyn Bootable<AppState, Self::Deps>> {
        Some(self)
    }
}

#[async_trait]
impl Bootable<AppState> for CacheProvider {
    async fn boot(
        &self,
        _deps: (),
        _state: &AppState,
    ) -> claviron_runtime::registry::Result<()> {
        if let Some(config) = &self.initial_config {
            self.configure(config).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl Configurable for CacheProvider {
    type Config = CacheConfig;

    fn source(&self) -> ConfigFile {
        ConfigFile::yaml(&self.config_path)
    }

    /// No file, no cache: the registry seals an empty catalog.
    fn absent_source(&self) -> Option<CacheConfig> {
        Some(CacheConfig::default())
    }

    fn validate_config(
        &self,
        config: &CacheConfig,
    ) -> congif::Result<()> {
        crate::registry::validate_config(config)
            .map_err(congif::Error::validation)
    }

    async fn configure(
        &self,
        config: &CacheConfig,
    ) -> congif::Result<()> {
        let registry = self.registry().map_err(congif::Error::msg)?;
        registry
            .configure(config.clone())
            .map_err(congif::Error::validation)?;
        // The drivers were registered with the registry; the declared stores
        // are built from them here, so nothing is left for boot.
        registry.seal().map_err(congif::Error::validation)
    }

    fn as_config_observer(
        &self
    ) -> Option<&dyn ConfigObserver<Config = Self::Config>> {
        Some(self)
    }
}

#[async_trait]
impl ConfigObserver for CacheProvider {
    /// Before the registry seals, the newest preparation is what boot builds;
    /// after it, a changed file is a new catalog generation.
    async fn config_changed(
        &self,
        config: &CacheConfig,
    ) -> congif::Result<()> {
        let _guard = self.reload_lock.lock().await;
        let registry = self.registry().map_err(congif::Error::msg)?;
        if !registry.is_sealed() {
            return registry
                .configure(config.clone())
                .map_err(congif::Error::validation);
        }
        self.republish(&registry, config.clone())
            .map_err(congif::Error::msg)?;
        info!(path = %self.config_path.display(), "🔁 CacheProvider reconfigured");
        Ok(())
    }
}

#[async_trait]
impl Reloadable<AppState> for CacheProvider {
    async fn reload(
        &self,
        state: &AppState,
    ) -> claviron_runtime::registry::Result<()> {
        let _guard = self.reload_lock.lock().await;
        let registry = self.registry()?;
        let config = state.config_reader().snapshot::<Self>()?;
        self.republish(&registry, (*config).clone())?;
        info!(path = %self.config_path.display(), "🔁 CacheProvider reloaded");
        Ok(())
    }
}
