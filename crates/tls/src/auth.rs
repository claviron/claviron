use std::sync::Arc;

use rustls::crypto::CryptoProvider;
use rustls::server::WebPkiClientVerifier;
use rustls::server::danger::ClientCertVerifier;
use rustls::{RootCertStore, ServerConfig};

use crate::CertAuthority;
use crate::auth::verifier::{PolicyConfig, PolicyVerifier};
use crate::error::{Error, Error as TlsError};
use crate::pem::load_der_certs;
use crate::resolver::CertResolver;
use crate::revocation::Revocations;

mod client;
mod policy;
pub mod verifier;

pub use client::ClientIdentity;
pub use policy::{ClientAuth, ClientTrust};

/// The verifier a client-authentication declaration asks for. Every server
/// that requires client certificates builds its verifier here.
///
/// `authority` is the store CA that `auth` selects, resolved by its holder
/// with `CertificateStore::trust`; a CA bundle is read here and takes none.
pub fn client_verifier(
    auth: &ClientAuth,
    authority: Option<&CertAuthority>,
    provider: &CryptoProvider,
) -> Result<Arc<dyn ClientCertVerifier>, TlsError> {
    let mut roots = RootCertStore::empty();
    match (auth.trust(), authority) {
        (ClientTrust::Bundle(path), None) => {
            for der in load_der_certs(path)? {
                roots.add(der)?;
            }
        }
        (ClientTrust::Authority(_), Some(authority)) => {
            let ca = authority.roots().first().ok_or_else(|| {
                Error::Other("authority CA chain is empty".into())
            })?;
            roots.add(ca.clone())?;
        }
        (ClientTrust::Authority(_), None) => {
            return Err(Error::Other(
                "client_auth.authority was not resolved from the certificate store".into(),
            ));
        }
        (ClientTrust::Bundle(_), Some(_)) => {
            return Err(Error::Other(
                "client_auth selects a ca_bundle; a store authority does not apply".into(),
            ));
        }
    }
    verifier_from_roots(roots, auth.subject(), provider)
}

fn verifier_from_roots(
    roots: RootCertStore,
    cfg: Option<&PolicyConfig>,
    provider: &CryptoProvider,
) -> Result<Arc<dyn ClientCertVerifier>, TlsError> {
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

/// Client-auth TLS with a shared, prepared revocation snapshot. Resumption is
/// disabled so every new connection reaches the current certificate verifier.
pub fn create_client_auth_config(
    auth: &ClientAuth,
    authority: Option<&CertAuthority>,
    resolver: Arc<CertResolver>,
    alpns: Vec<Vec<u8>>,
    revocations: Arc<Revocations>,
) -> Result<ServerConfig, TlsError> {
    let provider = crate::policy::provider();
    let verifier = client_verifier(auth, authority, &provider)?;
    auth_config_with_revocations(
        verifier,
        resolver,
        alpns,
        provider,
        revocations,
    )
}

fn auth_config_with_revocations(
    verifier: Arc<dyn ClientCertVerifier>,
    resolver: Arc<CertResolver>,
    alpns: Vec<Vec<u8>>,
    provider: CryptoProvider,
    revocations: Arc<Revocations>,
) -> Result<ServerConfig, TlsError> {
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
    let resolver = Arc::new(CertResolver::new_owned(
        cert_map,
        shared_resolver.get_default(),
        Default::default(),
        shared_resolver.pid,
    ));
    tracing::trace!(
        resolver = %format_args!("{:p}", Arc::as_ptr(&resolver)),
        parent_resolver = %format_args!("{:p}", Arc::as_ptr(shared_resolver)),
        process_id = shared_resolver.pid,
        ?cert_names,
        cert_count = resolver.count(),
        "Scoped TLS resolver derived"
    );
    Ok(resolver)
}
