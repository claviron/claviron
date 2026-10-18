//! One validated policy snapshot per process; publication is atomic.
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock, Weak};

use arc_swap::ArcSwapOption;
use async_trait::async_trait;
use claviron_orbit::placement::{Scope, Scoped};
use claviron_runtime::AppState;
use claviron_runtime::congif::{
    self, ConfigFile, ConfigObserver, ConfigReader, Configurable, Format,
};
use claviron_runtime::registry::{Component, Reloadable, priority};

use crate::config::{self, SupervisorConfig};

/// A handle to Congif's policy storage, not a second active snapshot.
#[derive(Default)]
pub(crate) struct PolicySnapshot {
    reader: OnceLock<Arc<ConfigReader>>,
    supplied: ArcSwapOption<SupervisorConfig>,
    #[cfg(test)]
    fixture: ArcSwapOption<SupervisorConfig>,
}
impl PolicySnapshot {
    pub(crate) fn load_full(&self) -> Option<Arc<SupervisorConfig>> {
        let snapshot = self.supplied.load_full().or_else(|| {
            self.reader
                .get()
                .and_then(|reader| reader.snapshot::<SupervisorPolicy>().ok())
        });
        #[cfg(test)]
        let snapshot = snapshot.or_else(|| self.fixture.load_full());
        snapshot
    }
    #[cfg(test)]
    pub(crate) fn store(
        &self,
        config: Option<Arc<SupervisorConfig>>,
    ) {
        self.fixture.store(config);
    }
}
pub(crate) type SharedPolicy = Arc<PolicySnapshot>;

/// Shared source description; each consumer prepares its own typed projection.
pub(crate) struct PolicySource {
    path: PathBuf,
    registry: Weak<claviron_runtime::registry::Registry<AppState>>,
}
impl PolicySource {
    pub(crate) fn new(state: &AppState) -> Self {
        Self {
            path: config::config_path(state),
            registry: Arc::downgrade(&state.registry()),
        }
    }
    #[cfg(test)]
    pub(crate) fn fixture() -> Self {
        Self { path: PathBuf::new(), registry: Weak::new() }
    }
    pub(crate) fn custom(
        &self
    ) -> Option<Arc<dyn config::SupervisorConfigSource>> {
        self.registry.upgrade().and_then(|registry| {
            registry.resolve_dyn::<dyn config::SupervisorConfigSource>()
        })
    }
    pub(crate) fn file(&self) -> ConfigFile {
        ConfigFile { path: self.path.clone(), format: Format::Yaml }
    }
    pub(crate) fn supplied(&self) -> congif::Result<Option<SupervisorConfig>> {
        self.custom()
            .map(|source| {
                let candidate =
                    source.supervisor_config().map_err(|error| {
                        congif::Error::validation(crate::Error::service(error))
                    })?;
                config::validate(candidate).map_err(congif::Error::validation)
            })
            .transpose()
    }
}
/// Per-process source of the validated supervisor configuration.
///
/// Congif prepares the complete policy before runtime reload hooks apply it.
/// This hook accepts the prepared snapshot for the effectful fleet reload hook;
/// it never opens or parses sources again. A failed preparation retains the old
/// snapshot. This
/// non-Runnable provider owns no worker operations, tickers or process cleanup.
pub(crate) struct SupervisorPolicy {
    accepted: AtomicBool,
    source: PolicySource,
    current: SharedPolicy,
}
impl SupervisorPolicy {
    pub(crate) fn new(
        current: SharedPolicy,
        state: &AppState,
    ) -> claviron_runtime::registry::Result<Self> {
        current.reader.get_or_init(|| state.config_reader_handle());
        let source = PolicySource::new(state);
        if let Some(config) = source.supplied().map_err(|error| {
            claviron_runtime::registry::Error::msg(error.to_string())
        })? {
            current.supplied.store(Some(Arc::new(config)));
        }
        Ok(Self { accepted: AtomicBool::new(false), source, current })
    }
    pub(crate) fn latest_load_accepted(&self) -> bool {
        self.accepted.load(Ordering::Acquire)
    }
    async fn load(
        &self,
        state: &AppState,
    ) -> claviron_runtime::registry::Result<()> {
        // reload_all continues other providers after an error. The effectful
        // fleet hook must not mistake the retained policy for a new acceptance.
        self.accepted.store(false, Ordering::Release);
        if let Some(config) = self.source.supplied().map_err(|error| {
            claviron_runtime::registry::Error::msg(error.to_string())
        })? {
            self.current.supplied.store(Some(Arc::new(config)));
            self.accepted.store(true, Ordering::Release);
            return Ok(());
        }
        state.config_reader().snapshot::<Self>().map_err(|error| {
            claviron_runtime::registry::Error::msg(error.to_string())
        })?;
        self.accepted.store(true, Ordering::Release);
        Ok(())
    }
}
impl Component<AppState> for SupervisorPolicy {
    type Deps = ();

    fn as_scoped(&self) -> Option<&dyn Scoped> {
        Some(self)
    }
    fn name(&self) -> &'static str {
        "SupervisorPolicy"
    }
    fn as_configurable(&self) -> Option<&dyn congif::ConfigRegistration> {
        self.source.custom().is_none().then_some(self)
    }
    fn as_reloadable(&self) -> Option<&dyn Reloadable<AppState>> {
        Some(self)
    }
}
impl Scoped for SupervisorPolicy {
    fn scope(&self) -> Scope {
        Scope::Any
    }
}
#[async_trait]
impl Configurable for SupervisorPolicy {
    type Config = SupervisorConfig;
    fn namespace(&self) -> String {
        "supervisor".into()
    }
    fn source(&self) -> ConfigFile {
        self.source.file()
    }
    fn as_config_observer(
        &self
    ) -> Option<&dyn ConfigObserver<Config = Self::Config>> {
        Some(self)
    }
    async fn configure(
        &self,
        _config: &SupervisorConfig,
    ) -> congif::Result<()> {
        Ok(())
    }
    fn validate_config(
        &self,
        config: &SupervisorConfig,
    ) -> congif::Result<()> {
        config::validate(config.clone())
            .map(|_| ())
            .map_err(congif::Error::validation)
    }
}
#[async_trait]
impl ConfigObserver for SupervisorPolicy {
    async fn config_changed(
        &self,
        config: &SupervisorConfig,
    ) -> congif::Result<()> {
        self.configure(config).await
    }
}
#[async_trait]
impl Reloadable<AppState> for SupervisorPolicy {
    fn priority(&self) -> Option<u8> {
        Some(priority::EARLY)
    }
    async fn reload(
        &self,
        state: &AppState,
    ) -> claviron_runtime::registry::Result<()> {
        self.load(state).await
    }
}
