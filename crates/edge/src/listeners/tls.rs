use std::sync::Arc;

use claviron_http::config::ListenerConfig;
use claviron_runtime::registry::{Error, Result};
use claviron_tls::auth::{ClientAuth, create_client_auth_config};
use claviron_tls::revocation::Revocations;
use claviron_tls::{CertAuthority, CertResolver};
use claviron_types::Host;
use rustls::ServerConfig;

use crate::config::HostConfig;
use crate::transport::VhostTable;

/// A listener that serves TLS needs the certificate source `tls.yaml`
/// declares, and without the file this process has none. `listeners` are the
/// ones one TLS transport declares; with none there is nothing to require.
pub(crate) fn require_certificates<'a>(
    state: &claviron_runtime::AppState,
    mut listeners: impl Iterator<Item = &'a claviron_http::ListenerId>,
) -> Result<()> {
    let Some(listener) = listeners.next() else {
        return Ok(());
    };
    if claviron_tls::declared(state)? {
        return Ok(());
    }
    Err(Error::msg(format!(
        "listener '{listener}' serves TLS, but tls.yaml does not exist"
    )))
}

/// Read the store CA each of `listeners` selects for its own client policy,
/// before its declaration is prepared. Every path that hands a listener a new
/// declaration runs this first; host publications then only look it up.
pub(crate) async fn prepare_listener_authorities(
    state: &claviron_runtime::AppState,
    mut listeners: Vec<Arc<ListenerConfig>>,
) -> crate::Result<()> {
    listeners.retain(|listener| {
        listener.client_auth().and_then(ClientAuth::store_authority).is_some()
    });
    if listeners.is_empty() {
        return Ok(());
    }
    let resolver =
        state.registry_ref().resolve::<crate::VhostResolver>().ok_or_else(
            || crate::Error::other("VhostResolver is not registered"),
        )?;
    resolver.listener_authorities().prepare(state, &listeners).await
}

/// A compiled handshake policy and the declaration used for cold reuse.
#[derive(Clone)]
pub(crate) struct TlsHost {
    pub declaration: Arc<HostConfig>,
    pub config: Arc<ServerConfig>,
    /// The client certificate this handshake requires: the listener's
    /// policy or the host's own.
    pub client_auth: Option<ClientAuth>,
    pub authority: Option<Arc<CertAuthority>>,
}

impl TlsHost {
    pub fn matches(
        &self,
        host: &HostConfig,
        client_auth: Option<&ClientAuth>,
        authority: Option<&Arc<CertAuthority>>,
    ) -> bool {
        self.declaration.same_tls_policy(host)
            && self.client_auth.as_ref() == client_auth
            && match (&self.authority, authority) {
                (None, None) => true,
                (Some(old), Some(new)) => Arc::ptr_eq(old, new),
                _ => false,
            }
    }
}

/// The client certificate a host's handshake on `listener` requires. A
/// listener that states a policy requires it of every connection, for every
/// host on it: such a host declares none, or the same one, which is then
/// redundant. A host that declares a different one is refused; there is no
/// merging and no precedence. Without a listener policy each host's own
/// applies.
pub(crate) fn client_auth<'a>(
    listener: &'a ListenerConfig,
    canonical: &Host,
    host: &'a HostConfig,
) -> Result<Option<&'a ClientAuth>> {
    match (listener.client_auth(), host.tls.client_auth.as_ref()) {
        (Some(required), Some(declared)) if declared != required => {
            Err(Error::msg(format!(
                "TLS host {canonical}: listener '{}' requires client certificates with its own tls.client_auth, and the host declares a different one; declare the policy in one place: on the listener, or on its hosts",
                listener.id
            )))
        }
        (Some(required), _) => Ok(Some(required)),
        (None, declared) => Ok(declared),
    }
}

/// The store CA a listener's own policy selects, prepared with the
/// listener's declaration. Read once per listener preparation, not per host.
pub(crate) fn listener_authority(
    resolver: &crate::VhostResolver,
    listener: &ListenerConfig,
) -> Result<Option<Arc<CertAuthority>>> {
    let Some(source) =
        listener.client_auth().and_then(ClientAuth::store_authority)
    else {
        return Ok(None);
    };
    resolver
        .listener_authorities()
        .get(&listener.id, source)
        .map(Some)
        .ok_or_else(|| {
            Error::msg(format!(
                "listener '{}': client authority was not prepared",
                listener.id
            ))
        })
}

/// The store CA a host's handshake on `listener` verifies against: the
/// listener's, when the listener states the policy, otherwise the one the
/// host's own declaration selects.
pub(crate) fn host_authority<'a>(
    listener: &ListenerConfig,
    prepared: Option<&'a Arc<CertAuthority>>,
    table: &'a VhostTable,
    host: &Host,
    declaration: &HostConfig,
) -> Result<Option<&'a Arc<CertAuthority>>> {
    if listener.client_auth().is_some() {
        return Ok(prepared);
    }
    client_authority(table, host, declaration)
}

fn client_authority<'a>(
    table: &'a VhostTable,
    host: &Host,
    declaration: &HostConfig,
) -> Result<Option<&'a Arc<CertAuthority>>> {
    let Some(source) = declaration
        .tls
        .client_auth
        .as_ref()
        .and_then(ClientAuth::store_authority)
    else {
        return Ok(None);
    };
    table.client_authority(source).map(Some).ok_or_else(|| {
        Error::msg(format!(
            "TLS host {host}: client authority was not prepared"
        ))
    })
}

/// The handshake of a host that requires a client certificate. `authority`
/// is the store CA its declaration selects, prepared with this publication.
pub(crate) fn create_host_auth_config(
    host: &Host,
    auth: &ClientAuth,
    authority: Option<&CertAuthority>,
    resolver: Arc<CertResolver>,
    alpns: Vec<Vec<u8>>,
    revocations: Arc<Revocations>,
) -> Result<ServerConfig> {
    create_client_auth_config(auth, authority, resolver, alpns, revocations)
        .map_err(|error| Error::msg(format!("TLS host {host}: {error}")))
}

#[cfg(test)]
#[path = "../../tests/listeners/tls.rs"]
mod tests;
