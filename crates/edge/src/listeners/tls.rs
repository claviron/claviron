use std::sync::Arc;

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

/// A compiled handshake policy and the declaration used for cold reuse.
#[derive(Clone)]
pub(crate) struct TlsHost {
    pub declaration: Arc<HostConfig>,
    pub config: Arc<ServerConfig>,
    pub authority: Option<Arc<CertAuthority>>,
}

impl TlsHost {
    pub fn matches(
        &self,
        host: &HostConfig,
        authority: Option<&Arc<CertAuthority>>,
    ) -> bool {
        self.declaration.same_tls_policy(host)
            && match (&self.authority, authority) {
                (None, None) => true,
                (Some(old), Some(new)) => Arc::ptr_eq(old, new),
                _ => false,
            }
    }
}

pub(crate) fn client_authority<'a>(
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
