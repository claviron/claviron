use claviron_runtime::AppState;

// Axum middleware adapter for the "intercept-after-rust-routes" mode.
// The upstream middleware (vhost or other) is responsible for picking the
// matching `PhpRuntime` and inserting it as `Extension<Arc<PhpRuntime>>`
// before this handler runs — this crate stays HostConfig-free.
pub mod adapter;
mod balancer;
#[cfg(feature = "axum")]
mod bindings;
pub mod config;
pub(crate) mod connection;
pub(crate) mod error;
pub(crate) mod events;
#[cfg(feature = "metrics")]
pub(crate) mod metrics;
pub(crate) mod ping;
pub(crate) mod pool;
pub(crate) mod proxy;
pub mod service;
#[cfg(feature = "tasks")]
mod tasks;

pub use adapter::PhpFpmAdapter;
#[cfg(feature = "axum")]
pub use bindings::build_fcgi_router;
pub use service::PhpFcgiService;

pub fn register(state: &AppState) -> claviron_runtime::registry::Result<()> {
    let registry = state.registry_ref();
    use std::sync::Arc;
    #[cfg(feature = "metrics")]
    {
        use metrics::fcgi::FcgiMetricsProvider;
        use metrics::fpm::provider::FpmMetricsProvider;
        state.bind_config_environment::<FcgiMetricsProvider>()?;
        state.bind_config_environment::<FpmMetricsProvider>()?;
        let fcgi_metrics = Arc::new(FcgiMetricsProvider::from_state(state));
        metrics::register(state, fcgi_metrics.clone())?;
        // Both register everywhere and declare where they run: the FPM
        // scraper master-side (one source), the pool stats worker-side.
        registry.register(Arc::new(FpmMetricsProvider::from_state(state)))?;
        registry.register(fcgi_metrics)?;
    }
    use crate::service::PhpFcgiService;
    // `fpm.yaml` is prepared by Congif before boot; the service applies the
    // catalog in boot and, with the `tasks` feature, installs the sweep's
    // task entry from the prepared cadence in `configure`.
    state.bind_config_environment::<PhpFcgiService>()?;
    registry.register(Arc::new(PhpFcgiService::from_state(state.clone())))?;
    Ok(())
}
