//! The shared host-free Rust handler contract.

use crate::config::HostConfig;
use claviron_runtime::{AppState, registry::Result};
use claviron_types::Host;
use std::sync::Arc;

pub use claviron_http::{OtherHandler, OtherService};

/// Cold handler factory for a selected host.
pub type Factory = dyn Fn(AppState, Host, Arc<HostConfig>) -> Result<OtherService>
    + Send
    + Sync
    + 'static;

impl super::HostAppRegistry {
    /// Register another Rust request owner for this host.
    ///
    /// The factory receives the canonical host and config at boot/reload.
    /// The Rust handler/router enters the same host scope compiler as Axum.
    /// Its response ends dispatch; router selection does not skip host layers.
    /// Factory errors prevent host publication.
    pub fn register_other<F>(
        &self,
        vhost_key: &'static str,
        factory: F,
    ) where
        F: Fn(AppState, Host, Arc<HostConfig>) -> Result<OtherService>
            + Send
            + Sync
            + 'static,
    {
        let f: Arc<Factory> = Arc::new(factory);

        self.inner
            .write()
            .expect("host app registry lock poisoned")
            .insert(vhost_key.to_string(), super::Binding::Other(f));
    }
}
