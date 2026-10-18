use std::path::{Path, PathBuf};

#[cfg(feature = "db")]
use claviron_hosting::Certificate;
use rustls::crypto::aws_lc_rs;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::sign::CertifiedKey;

use super::error::Error;

pub fn load_private_key(
    key: impl AsRef<Path>
) -> Result<PrivateKeyDer<'static>, Error> {
    let key_ref = key.as_ref();
    let key = PrivateKeyDer::from_pem_file(key_ref)
        .map_err(|e| Error::Key { path: key_ref.to_path_buf(), source: e })?;
    Ok(key)
}

pub fn load_der_certs(
    path: impl AsRef<Path>
) -> Result<Vec<CertificateDer<'static>>, Error> {
    let path_ref = path.as_ref();
    let certs = CertificateDer::pem_file_iter(path_ref)
        .map_err(|e| Error::Certificate {
            path: path_ref.to_path_buf(),
            source: e,
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| Error::Certificate {
            path: path_ref.to_path_buf(),
            source: e,
        })?;

    // debug!("Parsed {} certificate(s) from {}", certs.len(), path_ref.display());
    Ok(certs)
}
// Note: pem::load_cert_and_key must be implemented to cover both PEM and PKCS#12 inputs.
pub fn certified_key_from_path_pair(
    cert_path: &PathBuf,
    key_path: &PathBuf,
) -> Result<CertifiedKey, Error> {
    let certs = load_der_certs(cert_path)?;
    let key_der = load_private_key(key_path)?;
    CertifiedKey::from_der(certs, key_der, &aws_lc_rs::default_provider())
        .map_err(Into::into)
}

#[cfg(feature = "db")]
/// Build a `CertifiedKey` from a DB row.
///
/// DB layout:
/// - `certs_der`: concatenated DER chain **without** the root (leaf || intermediates).
/// - `key_der`: PKCS#8 DER private key.
/// - `cn`: used for logging/errors.
///
/// We split `certs_der` into individual certs (no reordering) and pair it with
/// `key_der` to produce a `CertifiedKey`. If chain ordering ever varies, add an
/// AKI/SKI (or Issuer/Subject) ordering step before building the config.
pub fn db_cert_to_certified_key(
    cert: &Certificate
) -> Result<CertifiedKey, Error> {
    let key_der = PrivateKeyDer::Pkcs8(cert.key_der.clone().into());

    let certs = parse_cert_chain_from_bytes(&cert.cn, &cert.certs_der)?;

    CertifiedKey::from_der(certs, key_der, &aws_lc_rs::default_provider())
        .map_err(Into::into)
}
#[cfg(feature = "db")]
/// Split one or more X.509 certificates from a concatenated DER blob.
/// Note: perform chain ordering/verification separately (leaf first, then intermediates),
/// and validate key ↔ leaf before swapping configs.
/// #CERT_CHAIN_SAVING, uses X509CertificateParser. DER is TLV (Tag-Length-Value) with definite lengths.
fn parse_cert_chain_from_bytes(
    cn: &str,
    data: &[u8],
) -> Result<Vec<CertificateDer<'static>>, Error> {
    use x509_parser::prelude::*;
    // Parse possibly concatenated DER
    let mut data = data;
    let mut certs = Vec::new();
    while !data.is_empty() {
        match X509Certificate::from_der(data) {
            Ok((rem, _)) => {
                let used = data.len() - rem.len();
                certs.push(CertificateDer::from(data[..used].to_vec()));
                data = rem;
            }
            Err(e) => {
                return Err(Error::DbCerts {
                    cn: cn.to_string(),
                    source: e.into(),
                });
            }
        }
    }
    Ok(certs)
}
