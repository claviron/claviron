use std::collections::BTreeMap;
use std::collections::HashMap;
use std::io;
use std::sync::Arc;

use crate::transport::{VhostBindings, VhostCtx, VhostResolver, VhostTable};
use claviron_http::ListenerId;
use claviron_http::config::{ListenerConfig, TcpListenerConfig};
use claviron_runtime::registry::Error;
use claviron_tls::Proto::{H1, H2};
use claviron_tls::auth::create_auth_config_with_revocations;
use claviron_tls::auth::verifier::PolicyConfig;
use claviron_tls::create_tls_config;
use claviron_tls::orbit::SessionDomain;
use claviron_types::Host;
use tokio::net::TcpStream;
use tokio_rustls::LazyConfigAcceptor;
use tokio_rustls::rustls::{self, ServerConfig};
use tokio_rustls::server::TlsStream;

/// SNI selects both host authentication and the listener/host ALPN intersection.
/// Host and TLS policy are captured together; a reload cannot switch auth policy
/// underneath an already accepted connection.
#[derive(Clone)]
pub(crate) struct TlsAccept(Arc<TlsConfigResolver>);

pub(super) struct VhostTlsPolicies(pub BTreeMap<ListenerId, Arc<TlsPolicy>>);

pub(super) struct TlsPolicy {
    table: Arc<VhostTable>,
    by_host: HashMap<Host, Arc<ServerConfig>>,
}

pub(crate) struct AcceptedTls {
    pub stream: TlsStream<crate::tcp::io::TcpIo>,
    pub host: VhostCtx,
}

impl TlsAccept {
    pub(super) fn prepared_policy(&self) -> Arc<TlsPolicy> {
        Arc::new(TlsPolicy {
            table: self.0.table.clone(),
            by_host: self.0.by_host.clone(),
        })
    }
    pub(crate) fn build(
        deps: &super::server::Deps,
        listener: &ListenerConfig,
        tcp: &TcpListenerConfig,
    ) -> Result<Self, Error> {
        let table = deps.vhost_resolver.snapshot();
        let policy = Self::prepare_policy(deps, listener, tcp, &table, None)?;
        Ok(Self(Arc::new(TlsConfigResolver {
            table: policy.table,
            by_host: policy.by_host,
            live: deps.vhost_resolver.clone(),
            listener: Some(listener.id.clone()),
        })))
    }

    pub(super) fn prepare_policy(
        deps: &super::server::Deps,
        listener: &ListenerConfig,
        tcp: &TcpListenerConfig,
        table: &VhostTable,
        previous: Option<&TlsPolicy>,
    ) -> Result<TlsPolicy, Error> {
        let mut by_host = HashMap::new();
        let domain = SessionDomain::new(format!("tcp-public:{}", listener.id))
            .map_err(|error| {
                Error::msg(format!("TLS listener '{}': {error}", listener.id))
            })?;
        for (canonical, record) in table.canonical_entries() {
            let host = &record.config;
            if !host.allows_listener(&listener.id) {
                continue;
            }
            let protocols = allowed_protocols(tcp, host);
            if protocols.is_empty() {
                continue;
            }
            if let Some(previous) = previous
                && previous
                    .table
                    .canonical_config(canonical)
                    .is_some_and(|old| old.same_tls_policy(host))
                && let Some(config) = previous.by_host.get(canonical)
            {
                by_host.insert(canonical.clone(), config.clone());
                continue;
            }
            let auth = &host.tls.client_auth;
            let config = if auth.is_required() {
                let ca = auth.ca_bundle.as_deref().ok_or_else(|| {
                    Error::msg(format!(
                        "TLS host {canonical}: required client auth needs ca_bundle"
                    ))
                })?;
                let policy = subject_policy_for_host(
                    &canonical.to_string(),
                    auth.organization.as_deref(),
                    auth.unit.as_deref()
                )?;
                let alpns = protocols
                    .iter()
                    .flat_map(|protocol| protocol.alpn_versions().iter().map(|alpn| alpn.to_vec()))
                    .collect();
                // mTLS never shares public resumption storage.
                create_auth_config_with_revocations(ca, deps.cert_resolver.clone(), alpns, policy.as_ref(), deps.revocations.clone())
            } else {
                create_tls_config(&protocols, deps.cert_resolver.clone()).map(|mut config| {
                    config.session_storage = deps.tls_sessions.storage(domain.clone());
                    config
                })
            }
            .map_err(|error| {
                Error::msg(format!("TLS listener '{}' host {canonical}: {error}", listener.id))
            })?;
            by_host.insert(canonical.clone(), Arc::new(config));
        }
        Ok(TlsPolicy { table: Arc::new(table.declarations_only()), by_host })
    }

    pub(crate) async fn accept(
        &self,
        stream: TcpStream,
    ) -> io::Result<AcceptedTls> {
        let names = self.0.live.snapshot();
        let policies = names.prepared::<VhostTlsPolicies>();
        let current = self
            .0
            .listener
            .as_ref()
            .and_then(|listener| policies.as_ref()?.0.get(listener));
        let (table, by_host) = match current {
            Some(policy) => (&policy.table, &policy.by_host),
            None => (&self.0.table, &self.0.by_host),
        };
        let hosts = VhostBindings::new(table.clone(), names);
        let start = LazyConfigAcceptor::new(
            rustls::server::Acceptor::default(),
            crate::tcp::io::TcpIo::new(stream),
        )
        .await?;
        let sni = start.client_hello().server_name().map(str::to_owned);
        tracing::debug!(
            listener = ?self.0.listener,
            server_name = sni.as_deref().unwrap_or("<absent>"),
            offered_alpn = ?start.client_hello().alpn().map(|protocols| {
                protocols.map(String::from_utf8_lossy).collect::<Vec<_>>()
            }),
            "TLS ClientHello ALPN offered",
        );
        let host = hosts.resolve_sni(sni.as_deref()).map_err(|error| {
            io::Error::new(io::ErrorKind::PermissionDenied, error.to_string())
        })?;
        // Named, so a refusal says which host was asked for: the name is the
        // only thing that tells a misrouted client from a missing binding.
        let config = by_host
            .get(host.vhost_key.0.as_ref())
            .cloned()
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    format!(
                        "host {} (SNI {}) is not enabled on this listener",
                        host.vhost_key.0,
                        sni.as_deref().unwrap_or("none")
                    ),
                )
            })?;
        Ok(AcceptedTls { stream: start.into_stream(config).await?, host })
    }
}

fn allowed_protocols(
    tcp: &TcpListenerConfig,
    host: &crate::transport::HostConfig,
) -> Vec<claviron_tls::Proto> {
    // The listener bounds capabilities, but an explicit host list owns ALPN
    // preference. Preserve omission separately from an explicit empty list.
    host.transport
        .protocols
        .as_deref()
        .unwrap_or(&tcp.protocols)
        .iter()
        .copied()
        .filter(|protocol| {
            matches!(protocol, H1 | H2) && tcp.protocols.contains(protocol)
        })
        .collect()
}

struct TlsConfigResolver {
    table: Arc<VhostTable>,
    live: Arc<VhostResolver>,
    by_host: HashMap<Host, Arc<ServerConfig>>,
    listener: Option<ListenerId>,
}
fn subject_policy_for_host(
    host: &str,
    organization: Option<&str>,
    unit: Option<&str>,
) -> Result<Option<PolicyConfig>, Error> {
    match (organization, unit) {
        (Some(organization), Some(unit)) => Ok(Some(PolicyConfig::new(
            organization.to_owned(),
            unit.to_owned(),
        ))),
        (None, None) => Ok(None),
        _ => Err(Error::msg(format!(
            "HTTPS TCP TLS policy for {host}: client_auth organization and unit must be configured together"
        ))),
    }
}

#[cfg(test)]
#[path = "../../../tests/tcp/unit/tls.rs"]
mod tests;
