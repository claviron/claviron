//! Decode and verify CRLs only during snapshot preparation.
use std::sync::Arc;

use claviron_types::ssl::RevocationId;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{
    CertificateDer, CertificateRevocationListDer, UnixTime,
};
use x509_parser::prelude::{FromDer, X509Certificate};
use x509_parser::revocation_list::CertificateRevocationList;

use super::Snapshot;
use crate::TlsError;

pub(super) fn index(
    bytes: &[u8],
    authorities: &[CertificateDer<'_>],
    now: UnixTime,
) -> Result<Snapshot, TlsError> {
    let input = bytes.trim_ascii_start();
    let crls = if input.starts_with(b"-----BEGIN") {
        CertificateRevocationListDer::pem_slice_iter(input)
            .map(|entry| {
                entry.map_err(|error| {
                    TlsError::Other(format!("invalid CRL PEM: {error}"))
                })
            })
            .collect::<Result<Vec<_>, _>>()?
    } else {
        vec![CertificateRevocationListDer::from(bytes.to_vec())]
    };
    if crls.is_empty() {
        return Err(TlsError::Other(
            "CRL file contains no X509 CRL entries".into(),
        ));
    }

    // Reuse rustls' rejection of unsupported CRL versions, delta/indirect CRLs
    // and critical extensions before compiling the local identity index.
    let mut roots = rustls::RootCertStore::empty();
    for authority in authorities {
        roots.add(authority.clone().into_owned())?;
    }
    let _validated =
        rustls::server::WebPkiClientVerifier::builder_with_provider(
            Arc::new(roots),
            Arc::new(crate::policy::provider()),
        )
        .with_crls(crls.clone())
        .build()
        .map_err(TlsError::ServerVerifier)?;

    let mut snapshot = Snapshot::default();
    for der in &crls {
        let (remainder, crl) =
            CertificateRevocationList::from_der(der.as_ref())
                .map_err(|_| TlsError::Other("invalid CRL DER".into()))?;
        if !remainder.is_empty() {
            return Err(TlsError::Other("trailing bytes after CRL DER".into()));
        }
        verify_signer(&crl, authorities)?;
        let next = crl.next_update().ok_or_else(|| {
            TlsError::Other("CRL is missing nextUpdate".into())
        })?;
        let next = u64::try_from(next.timestamp())
            .map_err(|_| TlsError::Other("invalid CRL nextUpdate".into()))?;
        if crl.last_update().timestamp() < 0
            || u64::try_from(crl.last_update().timestamp())
                .is_ok_and(|start| start > now.as_secs() || start >= next)
        {
            return Err(TlsError::Other(
                "CRL validity period is invalid or not yet current".into(),
            ));
        }
        let next =
            UnixTime::since_unix_epoch(std::time::Duration::from_secs(next));
        snapshot.next_update =
            Some(snapshot.next_update.map_or(next, |old| old.min(next)));
        for revoked in crl.iter_revoked_certificates() {
            snapshot.ids.insert(RevocationId::new(
                crl.issuer().as_raw(),
                revoked.raw_serial(),
            )?);
        }
    }
    snapshot.validate(now)?;
    Ok(snapshot)
}

fn verify_signer(
    crl: &CertificateRevocationList<'_>,
    authorities: &[CertificateDer<'_>],
) -> Result<(), TlsError> {
    for der in authorities {
        let (_, issuer) =
            X509Certificate::from_der(der.as_ref()).map_err(|_| {
                TlsError::Other("invalid CRL authority certificate".into())
            })?;
        if issuer.subject().as_raw() != crl.issuer().as_raw() || !issuer.is_ca()
        {
            continue;
        }
        let usage = issuer.key_usage().map_err(|_| {
            TlsError::Other("invalid CRL signer key usage".into())
        })?;
        if usage.is_some_and(|usage| !usage.value.crl_sign()) {
            continue;
        }
        if crl.verify_signature(issuer.public_key()).is_ok() {
            return Ok(());
        }
    }
    Err(TlsError::Other("CRL signature has no matching authorized issuer in certstore.authority".into()))
}
