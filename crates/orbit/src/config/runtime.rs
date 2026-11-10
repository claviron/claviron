//! One configuration owner for Link geometry and live receiver admission.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use claviron_runtime::congif::{
    self, ConfigFile, ConfigObserver, ConfigReader, Configurable,
};
use claviron_runtime::registry::Reloadable;
use claviron_runtime::{AppState, Component, registry};

use super::{LinkCapacity, LinkGeometry, OrbitConfig};

struct Source {
    path: PathBuf,
    nested: bool,
    absent: OrbitConfig,
    fallback: Option<usize>,
    resolved: Option<OrbitConfig>,
    programmatic: bool,
}

#[derive(Default)]
struct Applied {
    startup: Option<OrbitConfig>,
    warned: Option<(Option<LinkGeometry>, bool)>,
}

/// Geometry is frozen for the process lifetime. Only a positive receiver
/// bound changes live. Reading a snapshot never applies or logs a change.
pub struct OrbitConfiguration {
    source: Mutex<Source>,
    applied: Mutex<Applied>,
    capacity: Arc<LinkCapacity>,
    report_changes: bool,
}

impl OrbitConfiguration {
    pub fn register(state: &AppState) -> registry::Result<()> {
        if state.registry_ref().resolve::<Self>().is_some() {
            return Ok(());
        }
        state.bind_config_environment::<Self>()?;
        state.registry_ref().register(Arc::new(Self {
            source: Mutex::new(Source {
                path: state.config_dir().join("orbit.yaml"),
                nested: false,
                absent: OrbitConfig::default(),
                fallback: None,
                resolved: None,
                programmatic: false,
            }),
            applied: Mutex::new(Applied::default()),
            capacity: Arc::new(LinkCapacity::new(None, None)),
            report_changes: !state.is_worker(),
        }))?;
        Ok(())
    }

    /// Select the runner's `orbit` section before configuration preparation.
    pub fn runner_source(
        state: &AppState,
        path: PathBuf,
        initial: OrbitConfig,
        fallback: Option<usize>,
    ) -> registry::Result<()> {
        Self::register(state)?;
        let this = state.registry_ref().resolve::<Self>().ok_or_else(|| {
            registry::Error::msg("Orbit configuration missing")
        })?;
        if this
            .applied
            .lock()
            .map_err(|_| {
                registry::Error::msg("Orbit configuration lock poisoned")
            })?
            .startup
            .is_some()
        {
            return Err(registry::Error::msg(
                "Orbit source must be selected before configuration",
            ));
        }
        *this.source.lock().map_err(|_| {
            registry::Error::msg("Orbit source lock poisoned")
        })? = Source {
            path,
            nested: true,
            absent: initial.clone(),
            fallback,
            resolved: Some(initial),
            programmatic: false,
        };
        Ok(())
    }

    pub fn capacity(&self) -> Arc<LinkCapacity> {
        Arc::clone(&self.capacity)
    }

    fn apply_change(
        &self,
        config: &OrbitConfig,
        watched: bool,
    ) -> registry::Result<()> {
        config.validate()?;
        let mut applied = self.applied.lock().map_err(|_| {
            registry::Error::msg("Orbit configuration lock poisoned")
        })?;
        let startup = applied.startup.as_ref().ok_or_else(|| {
            registry::Error::msg("Orbit configuration not prepared")
        })?;
        let frozen = startup.link.as_ref().map(|link| link.geometry);
        let candidate = config.link.as_ref().map(|link| link.geometry);
        let fallback = self
            .source
            .lock()
            .map_err(|_| registry::Error::msg("Orbit source lock poisoned"))?
            .fallback;
        let limit = config
            .link
            .as_ref()
            .and_then(|link| link.max_in_flight.or(fallback));
        let active_limit = self.capacity.limit()?;
        let mode_changed = active_limit.is_some() != limit.is_some();
        let pending = (candidate, limit.is_some());
        if frozen != candidate || mode_changed {
            if watched && applied.warned != Some(pending) {
                if self.report_changes {
                    tracing::warn!(
                        "Orbit Link geometry, enablement or bounded/unbounded admission mode changed; current resources are retained; new settings will apply on the next restart"
                    );
                }
                applied.warned = Some(pending);
            } else if !watched {
                tracing::debug!(
                    "Orbit Link resources and admission mode are retained; new settings will apply on the next restart"
                );
            }
        } else {
            applied.warned = None;
        }
        if !mode_changed && let Some(limit) = limit {
            // A candidate's physical expansion is pending restart, so a limit
            // that does not fit the active shape must remain pending too.
            let admitted = self.capacity.max_waiting()?.unwrap_or(limit);
            if active_limit != Some(limit)
                && frozen
                    .is_some_and(|geometry| geometry.admits(limit, admitted))
            {
                self.capacity.set_max_in_flight(limit)?;
            }
        }
        Ok(())
    }
}

#[async_trait]
impl Configurable for OrbitConfiguration {
    type Config = OrbitConfig;
    fn source(&self) -> ConfigFile {
        ConfigFile::yaml(&self.source.lock().expect("Orbit source lock").path)
    }
    fn namespace(&self) -> String {
        "orbit".to_owned()
    }
    fn as_config_observer(
        &self
    ) -> Option<&dyn ConfigObserver<Config = OrbitConfig>> {
        Some(self)
    }
    async fn read_config(
        &self,
        reader: &ConfigReader,
    ) -> congif::Result<OrbitConfig> {
        let (nested, absent) = {
            let source = self.source.lock().map_err(|_| {
                congif::Error::msg("Orbit source lock poisoned")
            })?;
            (source.nested, source.absent.clone())
        };
        let value: serde_yaml::Value =
            match reader.read_include(self, &self.source()).await {
                Ok(value) => value,
                Err(congif::Error::Read { source, .. })
                    if source.kind() == std::io::ErrorKind::NotFound =>
                {
                    return Ok(absent);
                }
                Err(error) => return Err(error),
            };
        let selected = if nested {
            match value.get("orbit") {
                Some(value) => value.clone(),
                None => return Ok(absent),
            }
        } else {
            value
        };
        let mut candidate: OrbitConfig =
            serde_yaml::from_value(selected).map_err(congif::Error::invalid)?;
        if nested {
            let startup = self
                .applied
                .lock()
                .map_err(|_| {
                    congif::Error::msg("Orbit configuration lock poisoned")
                })?
                .startup
                .is_none();
            let mut source = self.source.lock().map_err(|_| {
                congif::Error::msg("Orbit source lock poisoned")
            })?;
            if let Some(initial) = source.resolved.clone() {
                if startup {
                    // The launch's explicit options win over its file. Preserve
                    // that whole-object override on subsequent source changes.
                    source.programmatic = initial != candidate;
                    return Ok(initial);
                }
                if source.programmatic {
                    candidate = initial;
                }
            }
        }
        Ok(candidate)
    }
    fn validate_config(
        &self,
        config: &OrbitConfig,
    ) -> congif::Result<()> {
        config.validate().map_err(congif::Error::invalid)
    }
    async fn configure(
        &self,
        config: &OrbitConfig,
    ) -> congif::Result<()> {
        config.validate().map_err(congif::Error::invalid)?;
        let mut applied = self.applied.lock().map_err(|_| {
            congif::Error::msg("Orbit configuration lock poisoned")
        })?;
        if applied.startup.is_some() {
            return Err(congif::Error::msg(
                "Orbit configuration is already prepared",
            ));
        }
        let fallback = self
            .source
            .lock()
            .map_err(|_| congif::Error::msg("Orbit source lock poisoned"))?
            .fallback;
        if let Some(link) = &config.link {
            self.capacity
                .prepare(link, fallback)
                .map_err(congif::Error::invalid)?;
        }
        applied.startup = Some(config.clone());
        Ok(())
    }
}

#[async_trait]
impl ConfigObserver for OrbitConfiguration {
    async fn config_changed(
        &self,
        config: &OrbitConfig,
    ) -> congif::Result<()> {
        self.apply_change(config, true).map_err(congif::Error::invalid)
    }
}

#[async_trait]
impl Reloadable<AppState> for OrbitConfiguration {
    async fn reload(
        &self,
        state: &AppState,
    ) -> registry::Result<()> {
        tracing::debug!(
            "reloading Orbit live admission policy; physical resources are retained"
        );
        self.apply_change(
            state.config_reader().snapshot::<Self>()?.as_ref(),
            false,
        )
    }
}

impl Component<AppState> for OrbitConfiguration {
    type Deps = ();
    fn name(&self) -> &'static str {
        "OrbitConfiguration"
    }
    fn as_configurable(&self) -> Option<&dyn congif::ConfigRegistration> {
        Some(self)
    }
    fn as_reloadable(&self) -> Option<&dyn Reloadable<AppState>> {
        Some(self)
    }
}

#[cfg(test)]
#[path = "../../tests/config/runtime.rs"]
mod tests;
