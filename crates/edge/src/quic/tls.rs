use std::collections::BTreeMap;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use crate::transport::{VhostBindings, VhostResolver, VhostTable};
use arc_swap::ArcSwap;
use claviron_http::ListenerId;
use claviron_http::config::QuicConfig;
use claviron_runtime::registry::{Error, Result};
use claviron_tls::Proto::H3;
use claviron_tls::auth::create_auth_config_with_revocations;
use claviron_tls::auth::verifier::PolicyConfig;
use claviron_tls::create_tls_config;
use claviron_tls::orbit::{FleetServerSessions, SessionDomain};
use claviron_tls::resolver::CertResolver;
use claviron_tls::revocation::Revocations;
use claviron_types::Host;
use quinn::{Connection, Incoming, ServerConfig};
use quinn_deferred_config::{ClientHelloView, DeferredServerConfig};
use rustls::ServerConfig as RustlsServerConfig;

fn h3_alpns() -> Vec<Vec<u8>> {
    H3.alpn_versions().iter().map(|p| p.to_vec()).collect()
}

#[cfg(test)]
#[path = "../../tests/quic/tls.rs"]
mod alias;

#[derive(Clone)]
pub(crate) struct H3TlsAccept {
    policy: Arc<H3TlsPolicy>,
    live: Arc<VhostResolver>,
    current: Arc<ArcSwap<H3Handshake>>,
    listener: Option<ListenerId>,
}

pub(crate) struct VhostTlsPolicies(pub BTreeMap<ListenerId, Arc<H3TlsPolicy>>);

pub(crate) struct H3TlsPolicy {
    base: Arc<RustlsServerConfig>,
    rejected: Arc<RustlsServerConfig>,
    by_host: Arc<HashMap<Host, Arc<RustlsServerConfig>>>,
    table: Arc<VhostTable>,
    config: QuicConfig,
    revocations: Arc<Revocations>,
}

struct H3Handshake {
    config: ServerConfig,
    names: Arc<VhostTable>,
    hosts: VhostBindings,
}

pub(crate) struct AcceptedQuic {
    pub connection: Connection,
    pub hosts: VhostBindings,
}
/// QUIC derives its Initial packet keys from AES-128-GCM (RFC 9001 §5.2), so a
/// cipher policy that filters that suite out leaves QUIC with no way to start a
/// connection. rustls reports it as `NoInitialCipherSuite`, which does not say
/// where to look.
const QUIC_INITIAL_SUITE_MISSING: &str = concat!(
    "H3 has no QUIC initial cipher suite: tls.policy.cipher_suites excludes ",
    "TLS13_AES_128_GCM_SHA256, which QUIC needs for its Initial packets. ",
    "Add it, or drop the h3 listener."
);

impl H3TlsAccept {
    pub(crate) fn build(
        cert_resolver: Arc<CertResolver>,
        revocations: Arc<Revocations>,
        vhost_resolver: Arc<VhostResolver>,
        cfg: &QuicConfig,
        sessions: &FleetServerSessions,
        listener: &ListenerId,
    ) -> Result<Self> {
        let mut base_tls = create_tls_config(&[H3], cert_resolver.clone())
            .map_err(|error| Error::msg(format!("H3 TLS config: {error}")))?;
        let domain = SessionDomain::new(format!("quic-public:{listener}"))
            .map_err(|error| {
                Error::msg(format!("H3 TLS session domain: {error}"))
            })?;
        base_tls.session_storage = sessions.storage(domain);
        let base_tls = prepare_rustls_server_config(
            base_tls,
            EarlyDataTransport::from_enabled(cfg.enable_early_data),
        );
        let table = vhost_resolver.snapshot();
        let mut by_host = HashMap::new();
        for (canonical, record) in table.canonical_entries() {
            let host = &record.config;
            if !host.allows_transport(listener, H3) {
                continue;
            }
            let auth = &host.tls.client_auth;
            let config = if auth.is_required() {
                build_auth_config_for_host(
                    &canonical.to_string(),
                    auth.ca_bundle.as_deref(),
                    auth.organization.as_deref(),
                    auth.unit.as_deref(),
                    cert_resolver.clone(),
                    cfg,
                    revocations.clone(),
                )?
            } else {
                base_tls.clone()
            };
            by_host.insert(canonical.clone(), config);
        }
        // The deferred selector cannot return Result. Its reject branch must
        // forbid resumption as well as certificate selection.
        let mut rejected = base_tls.as_ref().clone();
        rejected.cert_resolver = Arc::new(RejectCertificates);
        rejected.session_storage =
            Arc::new(rustls::server::NoServerSessionStorage {});
        rejected.max_early_data_size = 0;
        let rejected = Arc::new(rejected);
        let policy = Arc::new(H3TlsPolicy {
            base: base_tls,
            rejected,
            by_host: Arc::new(by_host),
            table: Arc::new(table.declarations_only()),
            config: cfg.clone(),
            revocations,
        });
        let current = Arc::new(ArcSwap::from_pointee(policy.handshake(table)?));
        Ok(Self {
            policy,
            live: vhost_resolver,
            current,
            listener: Some(listener.clone()),
        })
    }
    pub(crate) fn prepared_policy(&self) -> Arc<H3TlsPolicy> {
        self.policy.clone()
    }

    pub(crate) fn prepare_policy(
        template: &H3TlsPolicy,
        table: &VhostTable,
        previous: Option<&H3TlsPolicy>,
        certificates: Arc<CertResolver>,
        listener: &ListenerId,
    ) -> Result<H3TlsPolicy> {
        let mut by_host = HashMap::new();
        for (canonical, record) in table.canonical_entries() {
            let host = &record.config;
            if !host.allows_transport(listener, H3) {
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
                build_auth_config_for_host(
                    &canonical.to_string(),
                    auth.ca_bundle.as_deref(),
                    auth.organization.as_deref(),
                    auth.unit.as_deref(),
                    certificates.clone(),
                    &template.config,
                    template.revocations.clone(),
                )?
            } else {
                template.base.clone()
            };
            by_host.insert(canonical.clone(), config);
        }
        let policy = H3TlsPolicy {
            base: template.base.clone(),
            rejected: template.rejected.clone(),
            by_host: Arc::new(by_host),
            table: Arc::new(table.declarations_only()),
            config: template.config.clone(),
            revocations: template.revocations.clone(),
        };
        // Validate the deferred selector before the table becomes visible.
        policy.handshake(Arc::new(table.declarations_only()))?;
        Ok(policy)
    }
    pub(crate) fn base_config(&self) -> ServerConfig {
        self.current.load().config.clone()
    }
    pub(crate) async fn accept(
        &self,
        incoming: Incoming,
    ) -> Result<AcceptedQuic> {
        let names = self.live.snapshot();
        let policies = names.prepared::<VhostTlsPolicies>();
        let policy = self
            .listener
            .as_ref()
            .and_then(|id| policies.as_ref()?.0.get(id))
            .unwrap_or(&self.policy);
        let cached = self.current.load_full();
        let captured = if Arc::ptr_eq(&names, &cached.names) {
            cached
        } else {
            // Rebuild only the cheap selector/QUIC envelope after name-view
            // publication, never re-read CA files or rebuild host TLS configs.
            // Competing accepts may cache different immutable views; each
            // connection still retains exactly the one used by its selector.
            let next = Arc::new(policy.handshake(names)?);
            self.current.store(next.clone());
            next
        };
        let connection = incoming
            .accept_with(Arc::new(captured.config.clone()))
            .map_err(|error| Error::msg(format!("H3 accept: {error}")))?
            .await
            .map_err(|error| Error::msg(format!("H3 handshake: {error}")))?;
        Ok(AcceptedQuic { connection, hosts: captured.hosts.clone() })
    }
}

impl H3TlsPolicy {
    fn handshake(
        &self,
        names: Arc<VhostTable>,
    ) -> Result<H3Handshake> {
        let hosts = VhostBindings::new(self.table.clone(), names.clone());
        let lookup = hosts.clone();
        let by_host = self.by_host.clone();
        let rejected = self.rejected.clone();
        let selector: quinn_deferred_config::Selector =
            Arc::new(move |hello: &ClientHelloView<'_>| {
                // Only the canonical key: it picks the host's TLS config.
                // Routing (VhostCtx, app, ingress) comes from the single
                // `resolve_sni` after the handshake, on this same view.
                lookup
                    .handshake_key(hello.server_name)
                    .and_then(|key| by_host.get(key).cloned())
                    .unwrap_or_else(|| rejected.clone())
            });
        let crypto = DeferredServerConfig::new(self.base.clone(), selector)
            .map_err(|error| {
                // QUIC derives its Initial packet keys from AES-128-GCM (RFC 9001
                // §5.2), so a cipher policy that filters that suite out leaves QUIC
                // with nothing to start a connection with. rustls reports this as
                // `NoInitialCipherSuite`, which does not say where to look.
                if matches!(
                    error,
                    quinn::crypto::rustls::NoInitialCipherSuite { .. }
                ) {
                    return Error::msg(QUIC_INITIAL_SUITE_MISSING);
                }

                Error::msg(format!("H3 deferred config: {error:?}"))
            })?;
        let config = build_quinn_server_config_with_crypto(
            Arc::new(crypto),
            &self.config,
        )?;
        Ok(H3Handshake { config, names, hosts })
    }
}
#[derive(Debug)]
struct RejectCertificates;
impl rustls::server::ResolvesServerCert for RejectCertificates {
    fn resolve(
        &self,
        _hello: rustls::server::ClientHello<'_>,
    ) -> Option<Arc<rustls::sign::CertifiedKey>> {
        None
    }
}
fn build_auth_config_for_host(
    host: &str,
    ca_bundle: Option<&Path>,
    organization: Option<&str>,
    unit: Option<&str>,
    cert_resolver: Arc<CertResolver>,
    _cfg: &QuicConfig,
    revocations: Arc<Revocations>,
) -> Result<Arc<RustlsServerConfig>> {
    let ca_bundle = ca_bundle.ok_or_else(|| {
        Error::msg(format!(
            "H3 TLS policy for {host}: client_auth.required needs ca_bundle"
        ))
    })?;
    let policy = subject_policy_for_host(host, organization, unit)?;

    // Client-auth handshakes must not accept replayable early data, and
    // must not share resumption state with public unauthenticated hosts.
    let tls_cfg = create_auth_config_with_revocations(
        ca_bundle,
        cert_resolver,
        h3_alpns(),
        policy.as_ref(),
        revocations,
    )
    .map_err(|e| Error::msg(format!("H3 TLS auth config for {host}: {e}")))?;
    Ok(prepare_rustls_server_config(tls_cfg, EarlyDataTransport::Disabled))
}

fn subject_policy_for_host(
    host: &str,
    organization: Option<&str>,
    unit: Option<&str>,
) -> Result<Option<PolicyConfig>> {
    match (organization, unit) {
        (Some(organization), Some(unit)) => Ok(Some(PolicyConfig::new(
            organization.to_owned(),
            unit.to_owned(),
        ))),
        (None, None) => Ok(None),
        _ => Err(Error::msg(format!(
            "H3 TLS policy for {host}: client_auth organization and unit must be configured together"
        ))),
    }
}

fn prepare_rustls_server_config(
    mut tls_cfg: RustlsServerConfig,
    early_data: EarlyDataTransport,
) -> Arc<RustlsServerConfig> {
    // QUIC requires `max_early_data_size` to be either 0 or u32::MAX.
    // 0-RTT is replayable; per-route gating happens in `tower.rs`.
    tls_cfg.max_early_data_size = early_data.max_early_data_size();
    Arc::new(tls_cfg)
}

#[derive(Clone, Copy)]
enum EarlyDataTransport {
    Enabled,
    Disabled,
}

impl EarlyDataTransport {
    fn from_enabled(enabled: bool) -> Self {
        if enabled { Self::Enabled } else { Self::Disabled }
    }

    fn max_early_data_size(self) -> u32 {
        match self {
            Self::Enabled => u32::MAX,
            Self::Disabled => 0,
        }
    }
}

fn build_quinn_server_config_with_crypto(
    crypto: Arc<dyn quinn::crypto::ServerConfig>,
    cfg: &QuicConfig,
) -> Result<ServerConfig> {
    let mut server_cfg = ServerConfig::with_crypto(crypto);
    let transport =
        Arc::get_mut(&mut server_cfg.transport).ok_or_else(|| {
            Error::msg("H3Server: failed to mutate QUIC transport config")
        })?;
    transport
        .max_concurrent_bidi_streams(cfg.max_concurrent_bidi_streams.into())
        .max_concurrent_uni_streams(cfg.max_concurrent_uni_streams.into())
        .max_idle_timeout(Some(
            cfg.idle_timeout.try_into().map_err(|e| {
                Error::msg(format!("H3Server: idle_timeout: {e}"))
            })?,
        ))
        .keep_alive_interval(cfg.keep_alive_interval);

    Ok(server_cfg)
}
