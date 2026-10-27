use claviron_runtime::AppState;

#[cfg(feature = "auth")]
pub mod auth;
mod config;
pub mod console;
mod engine;
mod error;
pub mod evaluate;
mod event;
pub mod extension;
pub mod framework;
mod functions;
mod intercept;
mod invalidation;
mod manager;
#[cfg(feature = "metrics")]
mod metrics;
pub mod observer;
#[cfg(feature = "postgres")]
pub mod postgres;
mod routing;
mod sapi;
mod service;
#[cfg(feature = "tasks")]
pub(crate) mod tasks;
mod warmup;

pub use config::{
    ApplicationPool, PhpApplication, PhpEventConfig, PhpLaunchConfig,
    PhpManagementConfig, RequestPool,
};
pub use engine::application::PhpApplicationSnapshot;
pub use engine::{
    PhpSapiEngine, PhpSapiRequest, SapiThreadPoolSnapshot,
    SapiThreadSlotSnapshot,
};
pub use error::SapiGatewayError;
pub use invalidation::ipc::{
    PhpCacheInvalidationKind, PhpCacheInvalidationObserved,
};
#[cfg(any(feature = "apcu-intercept", feature = "opcache-intercept"))]
use invalidation::{bus, ipc};
pub use manager::{
    PhpApplicationController, PhpManagerError, PhpManagerResult,
};
#[cfg(feature = "metrics")]
pub use metrics::PhpEmbedMetricsConfig;
pub use routing::adapter::application::PhpApplicationAdapter;
pub use routing::adapter::script::PhpScriptAdapter;
#[cfg(feature = "axum")]
pub use routing::bindings::{build_application_router, build_script_router};
pub use service::{
    PhpApplicationLifecycle, PhpApplicationService, PhpApplicationStatus,
    PhpEmbedRuntime, PhpEmbedRuntimeBuilder, PhpEmbedStateExt,
    PhpScriptService,
};

/// Register PHP from `php.yaml` in the runtime configuration directory.
/// The library owns parsing, environment-file resolution, and validation.
/// A missing file preserves the default one-thread request pool.
/// Use [`PhpEmbedRuntime::builder`] for explicit, configuration-free composition.
/// An existing runtime keeps its settings; repeated boot-time registration
/// completes optional task bindings after TaskService becomes available.
pub fn register(state: &AppState) -> claviron_runtime::registry::Result<()> {
    if let Some(runtime) = state.registry_ref().resolve::<PhpEmbedRuntime>() {
        // A custom runtime may precede TaskService during server definitions.
        // Complete optional wiring here without replacing its PHP settings.
        #[cfg(feature = "tasks")]
        tasks::register(state, runtime)
            .map_err(claviron_runtime::registry::Error::msg)?;
        #[cfg(not(feature = "tasks"))]
        let _ = runtime;
        return Ok(());
    }
    config::file::load_service(state)?.register(state)
}

/// Host one application using the embed schema from an enclosing launch file.
/// The host supplies its name; `application` is the usual per-application
/// definition, beside `engine`, `lifecycle` and the other embed policies.
pub fn register_application_from_yaml(
    state: &AppState,
    name: &str,
    yaml: &str,
    description: claviron_app::ApplicationDescription,
) -> claviron_runtime::registry::Result<()> {
    config::file::load_application(state, name, yaml, description)?
        .register(state)
}

/// Host one script-mode application using the existing embed engine and SAPI
/// pool schema from an enclosing launch document. The request route is owned
/// by the caller, not this engine configuration.
pub fn register_script_from_yaml(
    state: &AppState,
    name: &str,
    yaml: &str,
    description: claviron_app::ApplicationDescription,
) -> claviron_runtime::registry::Result<()> {
    config::file::load_script(state, name, yaml, description)?.register(state)
}

#[cfg(test)]
#[path = "../tests/support.rs"]
mod test_support;
