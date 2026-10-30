//! Ephemeral benchmark identity. Generate once during setup, never per request.

use std::sync::Arc;

use rcgen::{CertificateParams, DnType, ExtendedKeyUsagePurpose};
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use rustls::sign::CertifiedKey;

pub struct SelfSigned {
    pub certificate: CertificateDer<'static>,
    pub key: Arc<CertifiedKey>,
}

pub fn generate(hostname: &str) -> SelfSigned {
    // Use Claviron's existing ECDSA P-256 key generator.
    let key = claviron::ssl::generators::generate_private_key()
        .expect("generate benchmark signing key");
    let mut params = CertificateParams::new(vec![hostname.to_owned()])
        .expect("valid benchmark certificate name");
    params.distinguished_name.push(DnType::CommonName, hostname);
    params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    let certificate = params
        .self_signed(&key)
        .expect("generate self-signed benchmark certificate")
        .der()
        .clone();
    let key = CertifiedKey::from_der(
        vec![certificate.clone()],
        PrivatePkcs8KeyDer::from(key.serialize_der()).into(),
        &rustls::crypto::aws_lc_rs::default_provider(),
    )
    .expect("load benchmark certificate and signing key");
    SelfSigned { certificate, key: Arc::new(key) }
}
