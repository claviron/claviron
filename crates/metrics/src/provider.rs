use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use claviron_runtime::AppState;

/// One independently owned Prometheus metric family.
///
/// Implementations live with the subsystem that owns the counters and decide
/// whether the current process should render local state or a fleet aggregate.
/// The registry only composes their output; it does not know Orbit, IPC, or any
/// application metric type.
pub trait MetricsFamily: Send + Sync + 'static {
    /// Stable registration key and diagnostic label.
    fn name(&self) -> &'static str;

    /// Append this family's current Prometheus exposition to `output`.
    fn write_prometheus(
        &self,
        state: &AppState,
        output: &mut String,
    ) -> Result<(), String>;
}

/// Process-local registry and composition point for optional metrics.
///
/// This is the only lifecycle provider. Subsystems register ordinary metric
/// families through [`MetricsProvider::register`]; they are not themselves
/// registered as dynamic lifecycle providers.
pub struct MetricsProvider {
    families: RwLock<BTreeMap<&'static str, Arc<dyn MetricsFamily>>>,
}

impl MetricsProvider {
    pub fn new() -> Self {
        Self { families: RwLock::new(BTreeMap::new()) }
    }

    /// Register or replace a family by its stable name.
    ///
    /// Replacement makes repeated subsystem registration idempotent while
    /// keeping Prometheus output order deterministic.
    pub fn register<F>(
        &self,
        family: Arc<F>,
    ) where
        F: MetricsFamily,
    {
        self.families
            .write()
            .expect("metrics family registry lock poisoned")
            .insert(family.name(), family);
    }

    pub fn render_prometheus(
        &self,
        state: &AppState,
    ) -> Result<String, String> {
        let families = self
            .families
            .read()
            .expect("metrics family registry lock poisoned")
            .values()
            .cloned()
            .collect::<Vec<_>>();

        let mut output = String::with_capacity(4096);
        for family in families {
            family.write_prometheus(state, &mut output).map_err(|err| {
                format!(
                    "metrics family `{}` failed to render: {err}",
                    family.name()
                )
            })?;
        }
        Ok(output)
    }

    pub fn family_count(&self) -> usize {
        self.families
            .read()
            .expect("metrics family registry lock poisoned")
            .len()
    }
}

impl Default for MetricsProvider {
    fn default() -> Self {
        Self::new()
    }
}

/// Insert the metrics composition provider.
pub fn register(state: &AppState) -> claviron_runtime::registry::Result<()> {
    state.registry_ref().insert(Arc::new(MetricsProvider::new()))?;
    Ok(())
}

#[cfg(test)]
#[path = "../tests/provider.rs"]
mod tests;
