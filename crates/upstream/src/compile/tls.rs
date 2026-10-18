//! Cold outbound TLS policy/material compilation; no request-time file or trust IO.
use std::sync::Arc;

use claviron_runtime::AppState;
use claviron_tls::auth::ClientIdentity;
use claviron_tls::{CertAuthority, CertResolver};
use claviron_types::KeyString;
use claviron_types::ssl::certstore::CertEntry;
use rustls::pki_types::{CertificateDer, ServerName};
use rustls::{ClientConfig, RootCertStore};

use crate::config::error::ConfigError;
use crate::config::profile::UpstreamProtocol;
use crate::config::profile::tls::{TlsConfig, Verification};
use crate::origin::{ConnectTarget, Origin};
mod insecure;

/// What the process certificate manager lends one cold compile: the resolver
/// that answers a certificate name, and the authority this deployment issues
/// its own certificates from. Both are slices of the material `tls.yaml`
/// already loaded — this crate opens no certificate directory. A process that
/// registered no certificate manager lends nothing, and a profile that asks
/// for either fails to compile rather than inventing trust.
#[derive(Clone, Debug, Default)]
pub struct TlsMaterial {
    certs: Option<Arc<CertResolver>>,
    authority: Option<Arc<CertAuthority>>,
}
impl TlsMaterial {
    pub fn from_state(state: &AppState) -> Self {
        let registry = state.registry_ref();
        Self {
            certs: registry.resolve::<CertResolver>(),
            authority: registry.resolve::<CertAuthority>(),
        }
    }
    /// Name material the caller already holds; the registry is the ordinary
    /// source and this is how a test or an embedder supplies its own.
    pub fn new(
        certs: Option<Arc<CertResolver>>,
        authority: Option<Arc<CertAuthority>>,
    ) -> Self {
        Self { certs, authority }
    }
}

#[derive(Debug)]
pub(crate) struct Prepared {
    pub(crate) client: Arc<ClientConfig>,
    // Desired equality uses full certificate bytes, not fresh Arc identity or
    // a collision-prone digest. Rotation at the same path replaces the epoch.
    roots: Vec<CertificateDer<'static>>,
    identity: Option<Identity>,
}

/// How this leg proves who it is, and therefore what a change to it means.
#[derive(Debug, PartialEq, Eq)]
enum Identity {
    /// A name the resolver answers. Renewing the certificate behind it is not
    /// a profile change: the next handshake simply gets the new one.
    Named(String),
    /// Somebody else's material, read from their files. Rotating it at the
    /// same path is a profile change, exactly as a trust store rotation is.
    Loaded(Vec<CertificateDer<'static>>),
}
impl PartialEq for Prepared {
    fn eq(
        &self,
        other: &Self,
    ) -> bool {
        self.roots == other.roots && self.identity == other.identity
    }
}
impl Eq for Prepared {}
pub(crate) fn compile(
    name: &str,
    policy: &TlsConfig,
    protocol: UpstreamProtocol,
    origins: &mut [Origin],
    material: &TlsMaterial,
) -> Result<Option<Arc<Prepared>>, ConfigError> {
    if !origins.iter().any(|o| matches!(o.connect, ConnectTarget::Tls(_))) {
        return Ok(None);
    }
    for origin in origins
        .iter_mut()
        .filter(|o| matches!(o.connect, ConnectTarget::Tls(_)))
    {
        origin.tls_name = Some(server_name(name, policy, &origin.authority)?);
    }
    prepare(name, policy, protocol, material).map(Some)
}
pub(crate) fn server_name(
    name: &str,
    policy: &TlsConfig,
    authority: &http::uri::Authority,
) -> Result<ServerName<'static>, ConfigError> {
    let host = policy
        .server_name
        .as_deref()
        .unwrap_or(authority.host())
        .trim_start_matches('[')
        .trim_end_matches(']');
    ServerName::try_from(host.to_owned()).map_err(|e| ConfigError::InvalidTls {
        profile: name.into(),
        detail: format!("server_name: {e}"),
    })
}
/// Cold material preparation also serves TLS names before any address exists.
pub(crate) fn prepare(
    name: &str,
    policy: &TlsConfig,
    protocol: UpstreamProtocol,
    material: &TlsMaterial,
) -> Result<Arc<Prepared>, ConfigError> {
    let invalid = |detail: String| ConfigError::InvalidTls {
        profile: name.into(),
        detail,
    };
    if policy.handshake_timeout.is_zero() {
        return Err(invalid("handshake_timeout must be positive".into()));
    }
    let expected = match protocol {
        UpstreamProtocol::H1 => "http/1.1",
        UpstreamProtocol::H2 => "h2",
    };
    if policy.alpn.as_ref().is_some_and(|list| {
        list.is_empty()
            || list.iter().any(|value| value.is_empty() || value.len() > 255)
    }) {
        // RFC 7301 encodes each nonempty protocol name in one length byte.
        return Err(invalid(
            "alpn must contain nonempty names of at most 255 bytes".into(),
        ));
    }
    let mut roots = RootCertStore::empty();
    let mut trusted = match policy.verify {
        Verification::Ca => {
            let path =
                policy.ca.as_ref().ok_or_else(|| invalid("verify: ca requires ca path".into()))?;
            claviron_tls::pem::load_der_certs(path).map_err(|e| invalid(e.to_string()))?
        }
        Verification::Authority => material
            .authority
            .as_ref()
            .ok_or_else(|| {
                invalid(
                    "verify: authority needs the certificate authority tls.yaml declares; \
                     this process loaded none"
                        .into()
                )
            })?
            .roots()
            .to_vec(),
        Verification::Native if policy.insecure_skip_verify => Vec::new(),
        Verification::Native => {
            let loaded = rustls_native_certs::load_native_certs();
            if !loaded.errors.is_empty() {
                return Err(invalid(format!("native trust load errors: {:?}", loaded.errors)));
            }
            loaded.certs
        }
    };
    if trusted.is_empty() && !policy.insecure_skip_verify {
        return Err(invalid("trust store contains no certificates".into()));
    }
    trusted.sort_by(|a, b| a.as_ref().cmp(b.as_ref()));
    for cert in &trusted {
        roots.add(cert.clone()).map_err(|e| invalid(e.to_string()))?;
    }
    let provider = Arc::new(claviron_tls::policy::provider());
    let builder = ClientConfig::builder_with_provider(provider.clone())
        .with_protocol_versions(&claviron_tls::policy::versions())
        .map_err(|e| invalid(e.to_string()))?;
    let builder = if policy.insecure_skip_verify {
        builder.dangerous().with_custom_certificate_verifier(Arc::new(
            insecure::Verifier(provider),
        ))
    } else {
        builder.with_root_certificates(roots)
    };
    let mut identity = None;
    let mut client = match &policy.client {
        None => builder.with_no_client_auth(),
        Some(CertEntry::Name(host)) => {
            let name = host.key_string();
            let certs = material.certs.as_ref().ok_or_else(|| {
                invalid(format!(
                    "client identity `{name}` needs the process certificate resolver; \
                     this process loaded none"
                ))
            })?;
            // Presence is proved once, here, so a misspelled name fails the
            // boot instead of an unauthenticated handshake much later.
            if certs.get_cert(&name).is_none() {
                return Err(invalid(format!(
                    "client identity `{name}` is not one of the certificates tls.yaml loaded"
                )));
            }
            identity = Some(Identity::Named(name.clone()));
            builder.with_client_cert_resolver(Arc::new(ClientIdentity::new(
                certs.clone(),
                name,
            )))
        }
        Some(CertEntry::Paths(paths)) => {
            if paths.ca_path().is_some() {
                return Err(invalid(
                    "client.ca_path: a peer's trust store belongs in `ca` with `verify: ca`".into()
                ));
            }
            let cert = paths.cert_path().ok_or_else(|| {
                invalid("client paths require cert_path".into())
            })?;
            let key = paths.key_path().ok_or_else(|| {
                invalid("client paths require key_path".into())
            })?;
            let chain = claviron_tls::pem::load_der_certs(cert)
                .map_err(|e| invalid(e.to_string()))?;
            let key = claviron_tls::pem::load_private_key(key)
                .map_err(|e| invalid(e.to_string()))?;
            identity = Some(Identity::Loaded(chain.clone()));
            builder
                .with_client_auth_cert(chain, key)
                .map_err(|e| invalid(e.to_string()))?
        }
    };
    client.alpn_protocols = policy
        .alpn
        .as_ref()
        .map(|list| list.iter().map(|v| v.as_bytes().to_vec()).collect())
        .unwrap_or_else(|| vec![expected.as_bytes().to_vec()]);
    // Connections authenticate independently; no insecure result can leak into
    // another profile through a process-global session cache.
    Ok(Arc::new(Prepared {
        client: Arc::new(client),
        roots: trusted,
        identity,
    }))
}
