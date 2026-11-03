use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use crate::transport::VhostTable;
use crate::transport::tls::TlsHost;
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

#[derive(Clone)]
pub(crate) struct H3TlsAccept {
    template: Arc<H3TlsTemplate>,
    hosts: Arc<VhostTable>,
    policy: Arc<H3TlsPolicy>,
}

pub(crate) struct H3TlsTemplate {
    base: Arc<RustlsServerConfig>,
    rejected: Arc<RustlsServerConfig>,
    endpoint: ServerConfig,
    config: QuicConfig,
    revocations: Arc<Revocations>,
}

pub(crate) struct H3TlsPolicy {
    by_host: Arc<HashMap<Host, TlsHost>>,
    config: Arc<ServerConfig>,
}

pub(crate) struct AcceptedQuic {
    pub connection: Connection,
    pub hosts: Arc<VhostTable>,
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
        deps: &crate::quic::server::Deps,
        cfg: &QuicConfig,
        listener: &ListenerId,
        hosts: Arc<VhostTable>,
        previous: Option<&Self>,
    ) -> Result<Self> {
        let template = match previous.filter(|old| old.template.config == *cfg)
        {
            Some(old) => old.template.clone(),
            None => Self::build_template(
                deps.cert_resolver.clone(),
                deps.revocations.clone(),
                cfg,
                &deps.tls_sessions,
                listener,
            )?,
        };
        let previous =
            previous.filter(|old| Arc::ptr_eq(&old.template, &template));
        let policy = Self::prepare_policy(
            &template,
            &hosts,
            previous.map(|old| old.policy.as_ref()),
            deps.cert_resolver.clone(),
            listener,
        )?;
        Ok(Self { template, hosts, policy: Arc::new(policy) })
    }

    pub(super) fn hosts(&self) -> &VhostTable {
        &self.hosts
    }

    fn build_template(
        cert_resolver: Arc<CertResolver>,
        revocations: Arc<Revocations>,
        cfg: &QuicConfig,
        sessions: &FleetServerSessions,
        listener: &ListenerId,
    ) -> Result<Arc<H3TlsTemplate>> {
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
        // The deferred selector cannot return Result. Its reject branch must
        // forbid resumption as well as certificate selection.
        let mut rejected = base_tls.as_ref().clone();
        rejected.cert_resolver = Arc::new(RejectCertificates);
        rejected.session_storage =
            Arc::new(rustls::server::NoServerSessionStorage {});
        rejected.max_early_data_size = 0;
        let rejected = Arc::new(rejected);
        // The endpoint needs transport parameters before an Incoming exists.
        // Its rejecting selector cannot admit a connection without a published policy.
        let refusal = rejected.clone();
        let endpoint = handshake_config(
            base_tls.clone(),
            Arc::new(move |_| refusal.clone()),
            cfg,
        )?;
        let template = Arc::new(H3TlsTemplate {
            base: base_tls,
            rejected,
            endpoint,
            config: cfg.clone(),
            revocations,
        });
        Ok(template)
    }

    pub(crate) fn prepare_policy(
        template: &H3TlsTemplate,
        table: &Arc<VhostTable>,
        previous: Option<&H3TlsPolicy>,
        certificates: Arc<CertResolver>,
        listener: &ListenerId,
    ) -> Result<H3TlsPolicy> {
        let mut by_host = HashMap::new();
        for (canonical, record) in table.listener_entries(listener) {
            let host = &record.config;
            let retained = previous
                .and_then(|previous| previous.by_host.get(canonical))
                .filter(|old| old.declaration.same_tls_policy(host));
            let auth = &host.tls.client_auth;
            let config = if let Some(old) = retained {
                old.config.clone()
            } else if auth.is_required() {
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
            by_host.insert(
                canonical.clone(),
                TlsHost { declaration: host.clone(), config },
            );
        }
        let by_host = Arc::new(by_host);
        let lookup = by_host.clone();
        let names = table.clone();
        let listener = listener.clone();
        let rejected = template.rejected.clone();
        let selector: quinn_deferred_config::Selector =
            Arc::new(move |hello: &ClientHelloView<'_>| {
                let selected = hello
                    .server_name
                    .and_then(|name| name.parse::<Host>().ok())
                    .and_then(|name| {
                        let (record, _) = names
                            .resolve_application_on(&name, Some(&listener))?;
                        lookup.get(&record.canonical).map(|host| &host.config)
                    });
                selected.cloned().unwrap_or_else(|| rejected.clone())
            });
        let config = handshake_config(
            template.base.clone(),
            selector,
            &template.config,
        )?;
        Ok(H3TlsPolicy { by_host, config: Arc::new(config) })
    }
    pub(crate) fn base_config(&self) -> ServerConfig {
        self.template.endpoint.clone()
    }
    pub(crate) async fn accept(
        &self,
        incoming: Incoming,
    ) -> Result<AcceptedQuic> {
        // Move this owner through the handshake; routing cannot load a newer view.
        let hosts = self.hosts.clone();
        let policy = &self.policy;
        let connection = incoming
            .accept_with(policy.config.clone())
            .map_err(|error| Error::msg(format!("H3 accept: {error}")))?
            .await
            .map_err(|error| Error::msg(format!("H3 handshake: {error}")))?;
        Ok(AcceptedQuic { connection, hosts })
    }
}

fn handshake_config(
    base: Arc<RustlsServerConfig>,
    selector: quinn_deferred_config::Selector,
    config: &QuicConfig,
) -> Result<ServerConfig> {
    let crypto =
        DeferredServerConfig::new(base, selector).map_err(|error| {
            if matches!(
                error,
                quinn::crypto::rustls::NoInitialCipherSuite { .. }
            ) {
                return Error::msg(QUIC_INITIAL_SUITE_MISSING);
            }
            Error::msg(format!("H3 deferred config: {error:?}"))
        })?;
    build_quinn_server_config_with_crypto(Arc::new(crypto), config)
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
