//! HTTP metric families and their export — all of it behind the `metrics`
//! feature.
//!
//! What a process has in flight is not here: that is
//! `claviron_runtime::activity`, which the families below read and the
//! watchdog uses for progress checks.

mod config;
mod http;
mod orbit;
mod provider;
mod router;
mod server;

use std::sync::Arc;

pub use config::HttpMetricsConfig;
pub use http::*;
pub use router::routes;
pub use server::*;

pub(crate) type ConfigReader =
    Arc<dyn Fn() -> Option<Arc<HttpMetricsConfig>> + Send + Sync>;

/// Register the metric families and their export.
///
/// `config` reads the current configuration — whatever the last reload
/// published — and is asked again at every publication, so a reload that
/// changes the interval is taken up without a restart.
pub fn register(
    state: &claviron_runtime::AppState,
    config: impl Fn() -> Option<Arc<HttpMetricsConfig>> + Send + Sync + 'static,
) -> claviron_runtime::registry::Result<()> {
    let config: ConfigReader = Arc::new(config);
    provider::register(state, config.clone())?;
    crate::watchdog::register(state, config)
}
