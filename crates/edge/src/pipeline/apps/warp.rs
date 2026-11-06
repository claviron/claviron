//! Warp application registration; the adapter is `claviron_pipeline::warp`.

use std::sync::Arc;

use claviron_http::warp::Router;
use claviron_runtime::AppState;

/// Cold Warp router factory for a selected host.
pub type Factory = dyn Fn(AppState) -> Router + Send + Sync + 'static;

impl super::HostAppRegistry {
    /// Prepare this Warp router for a canonical host at boot/reload.
    pub fn register_warp<F>(
        &self,
        vhost_key: &'static str,
        factory: F,
    ) where
        F: Fn(AppState) -> Router + Send + Sync + 'static,
    {
        self.inner.write().expect("host app registry lock poisoned").insert(
            vhost_key.to_owned(),
            super::Binding::Warp(Arc::new(factory)),
        );
    }
}

#[cfg(test)]
#[path = "../../../tests/pipeline/apps/warp.rs"]
mod tests;
