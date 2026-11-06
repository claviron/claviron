//! Cold listener preparation. Accepted connections retain the resulting view.

use std::sync::Arc;

use claviron_http::ListenerId;
use claviron_http::config::HttpSnapshot;
use claviron_runtime::AppState;
use claviron_runtime::registry::{Error, Result};

use super::{HostAppRegistry, VhostResolver, VhostTable};

pub type PreparedListener = Box<dyn FnOnce() + Send>;

/// One named instance prepares against the same declaration publication.
/// This contract runs only at start/reload or a host-source publication.
pub trait ListenerPublication: Send + Sync {
    fn listener_id(&self) -> &ListenerId;

    #[cfg(all(feature = "tcp", feature = "tls"))]
    fn mtls_h2_port(
        &self,
        _name: &claviron_types::Host,
        _canonical: &claviron_types::Host,
    ) -> Option<u16> {
        None
    }

    fn prepare(
        &self,
        hosts: &VhostTable,
        refresh_factories: bool,
    ) -> crate::Result<PreparedListener>;
}

/// Preserve owner-aware environment expansion and validate the entire source;
/// only the addressed listener and its inherited defaults become its input.
pub async fn read_listener(
    state: &AppState,
    id: &ListenerId,
) -> Result<HttpSnapshot> {
    let provider = state
        .registry_ref()
        .resolve::<crate::HttpConfigProvider>()
        .ok_or_else(|| Error::msg("HTTP configuration provider unavailable"))?;
    let reader = state.config_reader();
    reader.prepare_owners(&[provider.as_ref()]).await?;
    let input = reader.snapshot::<crate::HttpConfigProvider>()?;
    // Only this instance applies the candidate; do not acknowledge the whole
    // shared HTTP source on behalf of sibling listeners or the provider.
    select_listener(&input, id)
}

pub fn select_listener(
    input: &HttpSnapshot,
    id: &ListenerId,
) -> Result<HttpSnapshot> {
    let listener = input.listeners.get(id).cloned().ok_or_else(|| {
        Error::msg(format!("listener '{id}' is not configured"))
    })?;
    let mut selected = input.clone();
    selected.listeners = [(id.clone(), listener)].into();
    Ok(selected)
}

pub fn prepare_hosts(
    state: &AppState,
    hosts: &VhostTable,
    input: &HttpSnapshot,
    previous: Option<&VhostTable>,
) -> Result<VhostTable> {
    let apps = state
        .registry_ref()
        .resolve::<HostAppRegistry>()
        .ok_or_else(|| Error::msg("host application registry unavailable"))?;
    apps.prepare_listener(state, hosts, input, previous)
}

pub fn resolver(state: &AppState) -> Result<Arc<VhostResolver>> {
    state
        .registry_ref()
        .resolve::<VhostResolver>()
        .ok_or_else(|| Error::msg("vhost resolver unavailable"))
}

/// Bind and admission geometry, shared across transport factories. Prepared
/// request/TLS snapshots belong to the listener instances, not this catalog.
pub struct ListenerBinding(pub Arc<claviron_http::config::ListenerConfig>);
impl claviron_http::listener::ListenerSnapshot for ListenerBinding {
    fn listener_id(&self) -> &ListenerId {
        &self.0.id
    }
    fn same_binding(
        &self,
        other: &Self,
    ) -> bool {
        self.0.same_binding(&other.0)
    }
    fn binding_conflicts(
        &self,
        other: &Self,
    ) -> bool {
        self.0.binding_conflicts(&other.0)
    }
}
