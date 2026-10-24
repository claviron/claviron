use std::path::Path;
use std::sync::Arc;

use rustls::crypto::CryptoProvider;
use rustls::server::WebPkiClientVerifier;
use rustls::server::danger::ClientCertVerifier;
use rustls::{RootCertStore, ServerConfig};

use crate::auth::verifier::{PolicyConfig, PolicyVerifier};
use crate::error::{Error, Error as TlsError};
use crate::pem::load_der_certs;
use crate::resolver::CertResolver;
use crate::revocation::Revocations;

mod client;
mod policy;
pub mod verifier;

pub use client::ClientIdentity;
pub use policy::{ClientAuthMode, ClientAuthPolicy};

pub fn create_client_cert_verifier(
    ca_path: impl AsRef<Path>,
    cfg: Option<&PolicyConfig>,
) -> Result<Arc<dyn ClientCertVerifier>, TlsError> {
    create_client_cert_verifier_with_provider(
        ca_path,
        cfg,
        &crate::policy::provider(),
    )
}

pub fn create_client_cert_verifier_with_provider(
    ca_path: impl AsRef<Path>,
    cfg: Option<&PolicyConfig>,
    provider: &CryptoProvider,
) -> Result<Arc<dyn ClientCertVerifier>, TlsError> {
    let mut roots = RootCertStore::empty();
    for der in load_der_certs(ca_path)? {
        roots.add(der)?;
    }
    // Base mTLS verifier: chain/time/EKU
    let verifier: Arc<dyn ClientCertVerifier> =
        WebPkiClientVerifier::builder_with_provider(
            Arc::new(roots),
            Arc::new(provider.clone()),
        )
        .build()
        .map_err(TlsError::ServerVerifier)?;

    Ok(match cfg {
        Some(policy_cfg) => {
            // Policy wrapper enforcing O/OU
            Arc::new(PolicyVerifier::from_config(verifier.clone(), policy_cfg))
        }
        None => verifier,
    })
}

pub fn create_auth_config(
    ca_path: impl AsRef<Path>,
    resolver: Arc<CertResolver>,
    alpns: Vec<Vec<u8>>,
    cfg: Option<&PolicyConfig>,
) -> Result<ServerConfig, TlsError> {
    create_auth_config_with_provider(
        ca_path,
        resolver,
        alpns,
        cfg,
        crate::policy::provider(),
    )
}

pub fn create_tcp_auth_config(
    ca_path: impl AsRef<Path>,
    resolver: Arc<CertResolver>,
    alpns: Vec<Vec<u8>>,
    cfg: Option<&PolicyConfig>,
) -> Result<ServerConfig, TlsError> {
    create_auth_config_with_provider(
        ca_path,
        resolver,
        alpns,
        cfg,
        crate::policy::provider(),
    )
}

/// Client-auth TLS with a shared, prepared revocation snapshot. Resumption is
/// disabled so every new connection reaches the current certificate verifier.
pub fn create_auth_config_with_revocations(
    ca_path: impl AsRef<Path>,
    resolver: Arc<CertResolver>,
    alpns: Vec<Vec<u8>>,
    cfg: Option<&PolicyConfig>,
    revocations: Arc<Revocations>,
) -> Result<ServerConfig, TlsError> {
    let provider = crate::policy::provider();
    let verifier =
        create_client_cert_verifier_with_provider(ca_path, cfg, &provider)?;
    let verifier = crate::revocation::verifier(verifier, revocations);
    let mut config = ServerConfig::builder_with_provider(Arc::new(provider))
        .with_protocol_versions(&crate::policy::versions())?
        .with_client_cert_verifier(verifier)
        .with_cert_resolver(resolver);
    config.alpn_protocols = alpns;
    config.session_storage =
        Arc::new(rustls::server::NoServerSessionStorage {});
    config.send_tls13_tickets = 0;
    config.max_early_data_size = 0;
    Ok(config)
}

fn create_auth_config_with_provider(
    ca_path: impl AsRef<Path>,
    resolver: Arc<CertResolver>,
    alpns: Vec<Vec<u8>>,
    cfg: Option<&PolicyConfig>,
    provider: CryptoProvider,
) -> Result<ServerConfig, TlsError> {
    let verifier =
        create_client_cert_verifier_with_provider(ca_path, cfg, &provider)?;
    let mut sc = ServerConfig::builder_with_provider(Arc::new(provider))
        .with_protocol_versions(&crate::policy::versions())?
        .with_client_cert_verifier(verifier)
        .with_cert_resolver(resolver);
    sc.alpn_protocols = alpns;
    Ok(sc)
}

/// Build CustomCertResolver for a service from a shared resolver
/// which already has all certs loaded from PEM files and DB
pub fn derive_resolver_for_hosts(
    cert_names: &[String],
    shared_resolver: &Arc<CertResolver>,
) -> Result<Arc<CertResolver>, Error> {
    // Get the cert from the shared resolver
    let cert_map =
        shared_resolver.get_any(cert_names.iter().map(|s| s.as_str()));
    if cert_map.is_empty() {
        return Err(Error::Other(
            "No hostname found in shared host".to_string(),
        )); // or a new Error::NoCertFound variant
    }
    // Use the same default cert as the shared resolver
    let resolver =
        Arc::new(CertResolver::new(cert_map, shared_resolver.get_default()));
    tracing::trace!(
        resolver = %format_args!("{:p}", Arc::as_ptr(&resolver)),
        parent_resolver = %format_args!("{:p}", Arc::as_ptr(shared_resolver)),
        process_id = std::process::id(),
        ?cert_names,
        cert_count = resolver.count(),
        "Scoped TLS resolver derived"
    );
    Ok(resolver)
}
