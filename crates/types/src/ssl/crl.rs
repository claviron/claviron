//! Signed local revocation-list updates, independent of storage and TLS admission.
use std::collections::BTreeMap;

use rcgen::{
    CertificateRevocationListParams, Issuer, KeyIdMethod, KeyPair,
    PublicKeyData, RevokedCertParams, SerialNumber,
};
use rustls::pki_types::{CertificateRevocationListDer, pem::PemObject};
use time::OffsetDateTime;
use x509_parser::extensions::ParsedExtension;
use x509_parser::prelude::FromDer;
use x509_parser::revocation_list::CertificateRevocationList;

use super::parsers::{sanitize_pem_content_to_der, take_first_cert_from_der};
use super::{Error, RevocationId};

/// Add one certificate to a full CRL, preserving prior entries and other issuers.
/// `nextUpdate` is the signer's certificate expiry; revocation itself is permanent.
/// No files are read or written. Callers serialize concurrent updates themselves.
pub fn revoke(
    certificate_pem: &[u8],
    issuer_pem: &[u8],
    issuer_key_pem: &[u8],
    previous: &[u8],
    now: OffsetDateTime,
) -> Result<String, Error> {
    let certificate_der = sanitize_pem_content_to_der(certificate_pem);
    let certificate = take_first_cert_from_der(&certificate_der)?;
    let issuer_der = sanitize_pem_content_to_der(issuer_pem);
    let authority = take_first_cert_from_der(&issuer_der)?;
    let id = RevocationId::from_certificate_der(&certificate_der)?;
    if id.issuer() != authority.subject().as_raw()
        || certificate.verify_signature(Some(authority.public_key())).is_err()
    {
        return Err(Error::Other(
            "certificate does not belong to the selected issuer".into(),
        ));
    }
    if certificate.is_ca() {
        return Err(Error::Other("CA revocation requires a separate trust-chain policy; select a client or server certificate".into()));
    }
    if !authority.is_ca()
        || authority.key_usage()?.is_some_and(|usage| !usage.value.crl_sign())
    {
        return Err(Error::Other(
            "authority is not permitted to sign CRLs".into(),
        ));
    }
    let next_update = authority.validity().not_after.to_datetime();
    if now < authority.validity().not_before.to_datetime() || now >= next_update
    {
        return Err(Error::Other(
            "CRL issuer is outside its validity period".into(),
        ));
    }
    let key = KeyPair::from_pem(std::str::from_utf8(issuer_key_pem)?)?;
    if authority.public_key().raw != key.subject_public_key_info() {
        return Err(Error::Other(
            "authority certificate and private key do not match".into(),
        ));
    }
    let issuer =
        Issuer::from_ca_cert_pem(std::str::from_utf8(issuer_pem)?, key)?;
    let mut preserved = String::new();
    let mut entries = BTreeMap::new();
    let mut number = vec![0];
    let input = previous.trim_ascii_start();
    let crls = if input.is_empty() {
        Vec::new()
    } else if input.starts_with(b"-----BEGIN") {
        let lists = CertificateRevocationListDer::pem_slice_iter(input)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| {
                Error::Other(format!("invalid existing CRL PEM: {error}"))
            })?;
        if lists.is_empty() {
            return Err(Error::Other(
                "existing CRL file contains no CRLs".into(),
            ));
        }
        lists
    } else {
        vec![CertificateRevocationListDer::from(previous.to_vec())]
    };
    for der in crls {
        let (rest, crl) = CertificateRevocationList::from_der(der.as_ref())
            .map_err(|_| Error::Other("invalid existing CRL DER".into()))?;
        if !rest.is_empty() {
            return Err(Error::Other(
                "trailing bytes after existing CRL DER".into(),
            ));
        }
        if crl.issuer().as_raw() != authority.subject().as_raw() {
            super::formatters::write_pem_block(
                &mut preserved,
                "X509 CRL",
                der.as_ref(),
            );
            continue;
        }
        crl.verify_signature(authority.public_key()).map_err(|_| {
            Error::Other(
                "existing CRL signature does not match the issuer".into(),
            )
        })?;
        if crl.last_update().to_datetime() > now {
            return Err(Error::Other(
                "existing CRL was issued in the future".into(),
            ));
        }
        for extension in crl.extensions() {
            if !matches!(
                extension.parsed_extension(),
                ParsedExtension::AuthorityKeyIdentifier(_)
                    | ParsedExtension::CRLNumber(_)
            ) {
                return Err(Error::Other(
                    "updating scoped, delta or extended CRLs is not supported"
                        .into(),
                ));
            }
        }
        let old = crl
            .crl_number()
            .ok_or_else(|| {
                Error::Other("existing CRL has no CRL number".into())
            })?
            .to_bytes_be();
        if old.len() > number.len() || old.len() == number.len() && old > number
        {
            number = old;
        }
        for revoked in crl.iter_revoked_certificates() {
            // Do not silently discard entry semantics from an imported list.
            if !revoked.extensions().is_empty() {
                return Err(Error::Other(
                    "updating CRLs with entry extensions is not supported"
                        .into(),
                ));
            }
            let old_id =
                RevocationId::new(crl.issuer().as_raw(), revoked.raw_serial())?;
            let revoked_at = revoked.revocation_date.to_datetime();
            entries
                .entry(old_id.serial().to_vec())
                .and_modify(|date: &mut OffsetDateTime| {
                    *date = (*date).min(revoked_at)
                })
                .or_insert(revoked_at);
        }
    }
    entries.entry(id.serial().to_vec()).or_insert(now);
    let mut carry = true;
    for byte in number.iter_mut().rev() {
        if !carry {
            break;
        }
        (*byte, carry) = byte.overflowing_add(1);
    }
    if carry {
        number.insert(0, 1);
    }
    if number.len() > 20 {
        return Err(Error::Other(
            "CRL number exhausted the X.509 20-octet limit".into(),
        ));
    }
    let list = CertificateRevocationListParams {
        this_update: now,
        next_update,
        crl_number: SerialNumber::from_slice(&number),
        issuing_distribution_point: None,
        revoked_certs: entries
            .into_iter()
            .map(|(serial, revocation_time)| RevokedCertParams {
                serial_number: SerialNumber::from_slice(&serial),
                revocation_time,
                reason_code: None,
                invalidity_date: None,
            })
            .collect(),
        key_identifier_method: authority
            .extensions()
            .iter()
            .find_map(|extension| match extension.parsed_extension() {
                ParsedExtension::SubjectKeyIdentifier(identifier) => {
                    Some(KeyIdMethod::PreSpecified(identifier.0.to_vec()))
                }
                _ => None,
            })
            .unwrap_or(KeyIdMethod::Sha256),
    }
    .signed_by(&issuer)?;
    let (_, parsed) = CertificateRevocationList::from_der(list.der().as_ref())
        .map_err(|_| {
            Error::Other("generated CRL could not be parsed".into())
        })?;
    if parsed.issuer().as_raw() != authority.subject().as_raw() {
        return Err(Error::Other(
            "issuer name encoding cannot be preserved by the CRL signer".into(),
        ));
    }
    preserved.push_str(&list.pem()?);
    Ok(preserved)
}

#[cfg(test)]
#[path = "../../tests/ssl/crl.rs"]
mod tests;
