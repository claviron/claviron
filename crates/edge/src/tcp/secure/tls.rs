use std::collections::HashMap;
use std::io;
use std::sync::Arc;

use crate::tcp::io::{TcpErrors, TcpIo};
use crate::transport::tls::{
    TlsHost, client_authority, create_host_auth_config,
};
use crate::transport::{VhostCtx, VhostTable, resolve_sni_for_listener};
use claviron_http::ListenerId;
use claviron_http::config::{ListenerConfig, TcpListenerConfig};
use claviron_runtime::registry::Error;
use claviron_tls::Proto::{H1, H2};
use claviron_tls::create_tls_config;
use claviron_tls::orbit::SessionDomain;
use claviron_types::Host;
use tokio::net::TcpStream;
use tokio_rustls::LazyConfigAcceptor;
use tokio_rustls::rustls;
use tokio_rustls::server::TlsStream;

/// SNI selects both host authentication and the listener/host ALPN intersection.
/// Host and TLS policy are captured together; a reload cannot switch auth policy
/// underneath an already accepted connection.
#[derive(Clone)]
pub(crate) struct TlsAccept {
    hosts: Arc<VhostTable>,
    policy: Arc<TlsPolicy>,
    listener: ListenerId,
}

pub(super) struct TlsPolicy {
    by_host: HashMap<Host, TlsHost>,
    protocols: Vec<claviron_tls::Proto>,
}

pub(crate) struct AcceptedTls {
    pub stream: TlsStream<TcpIo>,
    pub errors: TcpErrors,
    pub host: VhostCtx,
}

impl TlsAccept {
    pub(crate) fn new(
        deps: &super::server::Deps,
        listener: &ListenerConfig,
        tcp: &TcpListenerConfig,
        hosts: Arc<VhostTable>,
        previous: Option<&Self>,
    ) -> Result<Self, Error> {
        let previous = previous
            .map(|old| old.policy.as_ref())
            .filter(|old| old.protocols == tcp.protocols);
        let policy = Arc::new(Self::prepare_policy(
            deps, listener, tcp, &hosts, previous,
        )?);
        Ok(Self { hosts, policy, listener: listener.id.clone() })
    }

    pub(super) fn hosts(&self) -> &VhostTable {
        &self.hosts
    }

    pub(super) fn serves_mtls_h2(
        &self,
        name: &Host,
        canonical: &Host,
    ) -> bool {
        self.hosts.record(name).is_some_and(|record| {
            &record.canonical == canonical
                && record.config.tls.client_auth.is_required()
                && self.policy.by_host.get(canonical).is_some_and(|host| {
                    host.config.alpn_protocols.iter().any(|alpn| alpn == b"h2")
                })
        })
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
        for (canonical, record) in table.listener_entries(&listener.id) {
            let host = &record.config;
            let protocols = allowed_protocols(tcp, host);
            if protocols.is_empty() {
                continue;
            }
            let auth = &host.tls.client_auth;
            let authority = client_authority(table, canonical, host)?;
            let retained = previous
                .and_then(|previous| previous.by_host.get(canonical))
                .filter(|old| old.matches(host, authority));
            let config = if let Some(old) = retained {
                old.config.clone()
            } else {
                let config = if auth.is_required() {
                    let alpns = protocols
                        .iter()
                        .flat_map(|protocol| {
                            protocol
                                .alpn_versions()
                                .iter()
                                .map(|alpn| alpn.to_vec())
                        })
                        .collect();
                    // mTLS never shares public resumption storage.
                    create_host_auth_config(
                        canonical,
                        host,
                        authority.map(Arc::as_ref),
                        deps.cert_resolver.clone(),
                        alpns,
                        deps.revocations.clone(),
                    )
                } else {
                    create_tls_config(&protocols, deps.cert_resolver.clone())
                        .map(|mut config| {
                            config.session_storage =
                                deps.tls_sessions.storage(domain.clone());
                            config
                        })
                        .map_err(|error| {
                            Error::msg(format!("TLS host {canonical}: {error}"))
                        })
                }?;
                Arc::new(config)
            };
            by_host.insert(
                canonical.clone(),
                TlsHost {
                    declaration: host.clone(),
                    config,
                    authority: authority.cloned(),
                },
            );
        }
        Ok(TlsPolicy { by_host, protocols: tcp.protocols.clone() })
    }

    pub(crate) async fn accept(
        &self,
        stream: TcpStream,
    ) -> io::Result<AcceptedTls> {
        // Both host routing and TLS policy belong to this captured publication.
        let hosts = &self.hosts;
        let policy = &self.policy;
        let (stream, errors) = TcpIo::split(stream);
        let start = LazyConfigAcceptor::new(
            rustls::server::Acceptor::default(),
            stream,
        )
        .await?;
        let sni = start.client_hello().server_name().map(str::to_owned);
        tracing::debug!(
            listener = ?self.listener,
            server_name = sni.as_deref().unwrap_or("<absent>"),
            offered_alpn = ?start.client_hello().alpn().map(|protocols| {
                protocols.map(String::from_utf8_lossy).collect::<Vec<_>>()
            }),
            "TLS ClientHello ALPN offered",
        );
        let host =
            resolve_sni_for_listener(hosts, sni.as_deref(), &self.listener)
                .map_err(|error| {
                    io::Error::new(
                        io::ErrorKind::PermissionDenied,
                        error.to_string(),
                    )
                })?;
        // Named, so a refusal says which host was asked for: the name is the
        // only thing that tells a misrouted client from a missing binding.
        let config = policy
            .by_host
            .get(host.vhost_key.0.as_ref())
            .map(|host| host.config.clone())
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
        Ok(AcceptedTls {
            stream: start.into_stream(config).await?,
            errors,
            host,
        })
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

#[cfg(test)]
#[path = "../../../tests/tcp/unit/tls.rs"]
mod tests;
