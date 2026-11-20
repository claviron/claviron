use std::collections::HashMap;
use std::io;
use std::sync::Arc;

use crate::host::SessionPlan;
use crate::listeners::tls::{
    TlsHost, client_auth, create_host_auth_config, host_authority,
    listener_authority,
};
use crate::transport::{VhostCtx, VhostTable, resolve_sni_for_listener};
use claviron_http::ListenerId;
use claviron_http::config::{HttpSnapshot, ListenerConfig, TcpListenerConfig};
use claviron_pipeline::plan::HostExecution;
use claviron_runtime::AppState;
use claviron_runtime::registry::Error;
use claviron_tls::CertAuthority;
use claviron_tls::Proto::{H1, H2};
use claviron_tls::SessionWire;
use claviron_tls::create_tls_config;
use claviron_tls::orbit::SessionDomain;
use claviron_types::Host;
use tokio_rustls::rustls;

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
    /// The session profiles each host enabled, prepared with it.
    sessions: HashMap<Host, Vec<HostSession>>,
}

/// A session profile a host enabled by ALPN: its own handshake, which asks
/// for no client certificate unless the listener requires one of every
/// connection, and the only execution its connections reach.
struct HostSession {
    protocol: &'static [u8],
    wire: SessionWire,
    config: Arc<rustls::ServerConfig>,
    execution: HostExecution,
}

/// What a ClientHello selects: the handshake, the host, and the HTTP version
/// of a session profile when one was selected.
pub(super) struct Selected {
    pub(super) config: Arc<rustls::ServerConfig>,
    pub(super) host: VhostCtx,
    pub(super) session: Option<SessionWire>,
}

impl TlsAccept {
    pub(crate) fn new(
        deps: &super::server::Deps,
        listener: &ListenerConfig,
        tcp: &TcpListenerConfig,
        hosts: Arc<VhostTable>,
        previous: Option<&Self>,
        state: &AppState,
        http: &HttpSnapshot,
    ) -> Result<Self, Error> {
        let previous = previous
            .map(|old| old.policy.as_ref())
            .filter(|old| old.protocols == tcp.protocols);
        // The listener's own policy and its CA are the same for every host.
        let authority = listener_authority(&deps.vhost_resolver, listener)?;
        let mut policy = Self::prepare_policy(
            deps,
            listener,
            tcp,
            &hosts,
            previous,
            authority.as_ref(),
        )?;
        policy.sessions = prepare_sessions(
            deps,
            listener,
            &hosts,
            state,
            http,
            authority.as_ref(),
        )?;
        let policy = Arc::new(policy);
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
                && self.policy.by_host.get(canonical).is_some_and(|host| {
                    host.client_auth.is_some()
                        && host
                            .config
                            .alpn_protocols
                            .iter()
                            .any(|alpn| alpn == b"h2")
                })
        })
    }

    pub(super) fn prepare_policy(
        deps: &super::server::Deps,
        listener: &ListenerConfig,
        tcp: &TcpListenerConfig,
        table: &VhostTable,
        previous: Option<&TlsPolicy>,
        listener_authority: Option<&Arc<CertAuthority>>,
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
            let required = client_auth(listener, canonical, host)?;
            let authority = host_authority(
                listener,
                listener_authority,
                table,
                canonical,
                host,
            )?;
            let retained = previous
                .and_then(|previous| previous.by_host.get(canonical))
                .filter(|old| old.matches(host, required, authority));
            let config = if let Some(old) = retained {
                old.config.clone()
            } else {
                let config = if let Some(auth) = required {
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
                        auth,
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
                    client_auth: required.cloned(),
                    authority: authority.cloned(),
                },
            );
        }
        Ok(TlsPolicy {
            by_host,
            protocols: tcp.protocols.clone(),
            sessions: HashMap::new(),
        })
    }

    /// The TLS configuration and host a ClientHello's server name selects.
    /// Both belong to this captured publication.
    pub(super) fn select<'a>(
        &self,
        sni: Option<&str>,
        offered: Option<impl Iterator<Item = &'a [u8]>>,
    ) -> io::Result<Selected> {
        let host = resolve_sni_for_listener(&self.hosts, sni, &self.listener)
            .map_err(|error| {
            io::Error::new(io::ErrorKind::PermissionDenied, error.to_string())
        })?;
        // Named, so a refusal says which host was asked for: the name is the
        // only thing that tells a misrouted client from a missing binding.
        let config = self
            .policy
            .by_host
            .get(host.vhost_key.0.as_ref())
            .map(|host| host.config.clone())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    format!(
                        "host {} (SNI {}) is not enabled on this listener",
                        host.vhost_key.0,
                        sni.unwrap_or("none")
                    ),
                )
            })?;
        // A session the host enabled for a protocol the client offered, in
        // the client's order, replaces the host's handshake and execution.
        let sessions = self.policy.sessions.get(host.vhost_key.0.as_ref());
        if let (Some(sessions), Some(offered)) = (sessions, offered) {
            for protocol in offered {
                if let Some(session) =
                    sessions.iter().find(|session| session.protocol == protocol)
                {
                    let mut host = host;
                    host.application = Some(session.execution.clone());
                    return Ok(Selected {
                        config: session.config.clone(),
                        host,
                        session: Some(session.wire),
                    });
                }
            }
        }
        Ok(Selected { config, host, session: None })
    }
}

/// Prepare the session profiles each host on `listener` enables. An id no
/// profile registered refuses the publication: a host that names a session
/// must get it. A listener that requires a client certificate requires it of
/// these handshakes too.
fn prepare_sessions(
    deps: &super::server::Deps,
    listener: &ListenerConfig,
    table: &VhostTable,
    state: &AppState,
    http: &HttpSnapshot,
    listener_authority: Option<&Arc<CertAuthority>>,
) -> Result<HashMap<Host, Vec<HostSession>>, Error> {
    let mut sessions = HashMap::new();
    let profiles = state.registry_ref().resolve::<crate::SessionProfiles>();
    for (canonical, record) in table.listener_entries(&listener.id) {
        let host = &record.config;
        if host.tls.alpn.is_empty() {
            continue;
        }
        let mut prepared = Vec::with_capacity(host.tls.alpn.len());
        for (id, params) in &host.tls.alpn {
            let profile = profiles
                .as_ref()
                .and_then(|profiles| profiles.get(id.as_bytes()))
                .ok_or_else(|| {
                    Error::msg(format!(
                        "TLS host {canonical}: ALPN '{id}' has no registered session profile"
                    ))
                })?;
            let wire = profile.wire();
            // Its own handshake: the profile's id alone, and rustls' own
            // resumption store rather than the listener's shared one. It asks
            // for a client certificate only when the listener requires one;
            // that handshake keeps no resumption store at all.
            let config = match listener.client_auth() {
                Some(auth) => create_host_auth_config(
                    canonical,
                    auth,
                    listener_authority.map(Arc::as_ref),
                    deps.cert_resolver.clone(),
                    vec![profile.protocol().to_vec()],
                    deps.revocations.clone(),
                )?,
                None => {
                    let mut config = create_tls_config(
                        wire.protocols(),
                        deps.cert_resolver.clone(),
                    )
                    .map_err(|error| {
                        Error::msg(format!("TLS host {canonical}: {error}"))
                    })?;
                    config.alpn_protocols = vec![profile.protocol().to_vec()];
                    config
                }
            };
            prepared.push(HostSession {
                protocol: profile.protocol(),
                wire,
                config: Arc::new(config),
                execution: profile.execution(SessionPlan::new(
                    state,
                    canonical,
                    host,
                    listener,
                    Some(http),
                    params,
                    profile.inherits(),
                ))?,
            });
        }
        sessions.insert(canonical.clone(), prepared);
    }
    Ok(sessions)
}

fn allowed_protocols(
    tcp: &TcpListenerConfig,
    host: &crate::config::HostConfig,
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
#[path = "../../../../tests/listeners/tcp/unit/tls.rs"]
mod tests;
