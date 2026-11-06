//! Axum application registration; the adapter is `claviron_pipeline::axum`.

use std::sync::Arc;

use axum::Router;
use claviron_runtime::AppState;

/// Cold Axum router factory for a selected host.
pub type Factory = dyn Fn(AppState) -> Router + Send + Sync + 'static;

impl super::HostAppRegistry {
    /// Register a complete Axum router for this host.
    ///
    /// The factory runs at boot/reload. The router enters the same host scope
    /// compiler as configured backends, after the scope's active policy layers.
    /// Its response, including a 404, ends request dispatch.
    pub fn register_router<F>(
        &self,
        vhost_key: &'static str,
        factory: F,
    ) where
        F: Fn(AppState) -> Router + Send + Sync + 'static,
    {
        let router: Arc<Factory> = Arc::new(factory);

        self.inner
            .write()
            .expect("host app registry lock poisoned")
            .insert(vhost_key.to_string(), super::Binding::Axum(router));
    }
}
