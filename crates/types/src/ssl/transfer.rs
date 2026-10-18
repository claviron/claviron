//! File-independent import planning. Supplied chains are evidence, not new trust.
use rcgen::PublicKeyData;
use rustls::pki_types::CertificateDer;
use rustls::pki_types::pem::PemObject;
use time::OffsetDateTime;
use x509_parser::certificate::X509Certificate;
use x509_parser::extensions::ParsedExtension;
use x509_parser::parse_x509_certificate;

use super::{Aliases, CertificateKind, Error, Host, KeyString, generators};

/// Public material from an enabled, explicitly registered local DB issuer.
pub struct IssuerCandidate {
    pub id: i32,
    pub certs_der: Vec<u8>,
}

/// Validated material bound to a registered local issuer, or a self-signed root.
/// Deliberately not Debug: this value owns a private key.
pub struct ImportPlan {
    pub common_name: String,
    pub aliases: Vec<String>,
    pub kind: CertificateKind,
    pub issuer_id: Option<i32>,
    pub certs_der: Vec<u8>,
    pub key_der: Vec<u8>,
    pub issued_at: OffsetDateTime,
    pub expires_at: OffsetDateTime,
}

fn parse(der: &[u8]) -> Result<X509Certificate<'_>, Error> {
    let (rest, cert) = parse_x509_certificate(der)
        .map_err(|_| Error::Other("invalid X.509 certificate".into()))?;
    if !rest.is_empty() {
        return Err("trailing data after certificate".into());
    }
    Ok(cert)
}

fn first_der(chain: &[u8]) -> Result<&[u8], Error> {
    let (rest, _) = parse_x509_certificate(chain)
        .map_err(|_| Error::Other("invalid DB issuer certificate".into()))?;
    Ok(&chain[..chain.len() - rest.len()])
}

fn ca(cert: &X509Certificate<'_>) -> Result<bool, Error> {
    Ok(cert.basic_constraints()?.is_some_and(|ext| ext.value.ca))
}

fn usable_ca(cert: &X509Certificate<'_>) -> Result<bool, Error> {
    Ok(ca(cert)?
        && cert.validity().is_valid()
        && cert.key_usage()?.is_none_or(|ext| ext.value.key_cert_sign()))
}

fn signs(
    child: &X509Certificate<'_>,
    parent: &X509Certificate<'_>,
) -> Result<bool, Error> {
    if child.issuer() != parent.subject() || !usable_ca(parent)? {
        return Ok(false);
    }
    // AKI/SKI narrow candidates. The signature is always the final proof.
    let aki = child.extensions().iter().find_map(|ext| {
        match ext.parsed_extension() {
            ParsedExtension::AuthorityKeyIdentifier(value) => Some(value),
            _ => None,
        }
    });
    let ski = parent.extensions().iter().find_map(|ext| {
        match ext.parsed_extension() {
            ParsedExtension::SubjectKeyIdentifier(value) => Some(&value.0),
            _ => None,
        }
    });
    if let Some(aki) = aki {
        if aki.key_identifier.as_ref().zip(ski).is_some_and(|(a, s)| a.0 != *s)
            || aki
                .authority_cert_serial
                .is_some_and(|serial| serial != parent.raw_serial())
        {
            return Ok(false);
        }
    }
    Ok(child.verify_signature(Some(parent.public_key())).is_ok())
}

/// Inspect a PEM fullchain in any order and bind only to a verified DB issuer.
/// A root is recognized by CA constraints AND a valid self-signature. A leaf's
/// serverAuth permission takes precedence over clientAuth, matching generation.
pub fn plan_import(
    certificate_pem: &[u8],
    private_key_pem: &[u8],
    issuers: &[IssuerCandidate],
) -> Result<ImportPlan, Error> {
    let key = generators::read_private_key_from_pem(private_key_pem)?;
    let public_key = key.subject_public_key_info();
    let mut supplied = Vec::<Vec<u8>>::new();
    for cert in CertificateDer::pem_slice_iter(certificate_pem) {
        let der = cert
            .map_err(|_| Error::Other("invalid certificate PEM".into()))?
            .to_vec();
        if !supplied.contains(&der) {
            supplied.push(der);
        }
    }
    if supplied.is_empty() {
        return Err("certificate PEM contains no certificates".into());
    }
    let mut targets = Vec::new();
    for (index, der) in supplied.iter().enumerate() {
        if parse(der)?.public_key().raw == public_key {
            targets.push(index);
        }
    }
    if targets.len() != 1 {
        return Err(
            "private key must match exactly one certificate in the fullchain"
                .into(),
        );
    }
    let target = supplied[targets[0]].clone();
    let cert = parse(&target)?;
    if !cert.validity().is_valid() {
        return Err("target certificate is expired or not yet valid".into());
    }
    let kind = if ca(&cert)? {
        if !usable_ca(&cert)? {
            return Err(
                "CA certificate does not permit certificate signing".into()
            );
        }
        CertificateKind::Ca
    } else {
        let usage = cert.extended_key_usage()?.ok_or_else(|| {
            Error::Other(
                "cannot infer server/client role without extended key usage"
                    .into(),
            )
        })?;
        if usage.value.server_auth {
            CertificateKind::Server
        } else if usage.value.client_auth {
            CertificateKind::Client
        } else {
            return Err(
                "certificate has neither serverAuth nor clientAuth usage"
                    .into(),
            );
        }
    };
    let (common_name, aliases) = if kind == CertificateKind::Ca {
        // A CA's subject is an authority identity, never a routing hostname.
        let name = cert
            .subject()
            .iter_common_name()
            .next()
            .ok_or_else(|| Error::Other("missing subject CN".into()))?
            .as_str()?;
        let name = super::CommonName::try_from(name.to_owned())
            .map_err(Error::Other)?;
        (name.into_string(), Vec::new())
    } else {
        let name = Host::try_from(&cert)?;
        let common_name = name.key_string();
        let mut names = Aliases::try_from(&cert)?;
        names.push_unique(name);
        (common_name, names.iter().map(KeyString::key_string).collect())
    };
    let self_signed = kind == CertificateKind::Ca
        && cert.subject() == cert.issuer()
        && cert.verify_signature(None).is_ok();
    let mut matches = Vec::new();
    if !self_signed {
        for candidate in issuers {
            let der = first_der(&candidate.certs_der)?;
            if der != target && signs(&cert, &parse(der)?)? {
                matches.push(candidate);
            }
        }
        if matches.len() != 1 {
            return Err(if matches.is_empty() {
                "no enabled local DB issuer verifies this certificate; provision its local CA through the hosting CLI; ACME import is not supported here"
            } else {
                "more than one DB issuer verifies this certificate; issuer relationship is ambiguous"
            }.into());
        }
    }
    let issuer_id = matches.first().map(|candidate| candidate.id);
    let mut available = supplied.clone();
    if let Some(candidate) = matches.first() {
        let mut remaining = candidate.certs_der.as_slice();
        while !remaining.is_empty() {
            let der = first_der(remaining)?.to_vec();
            remaining = &remaining[der.len()..];
            if !available.contains(&der) {
                available.push(der);
            }
        }
    }
    let mut chain = vec![target.clone()];
    loop {
        let current = parse(chain.last().expect("target exists"))?;
        if current.subject() == current.issuer()
            && current.verify_signature(None).is_ok()
        {
            break;
        }
        let mut parents = Vec::new();
        for der in &available {
            if der.as_slice() != chain.last().unwrap().as_slice()
                && signs(&current, &parse(der)?)?
            {
                parents.push(der);
            }
        }
        if parents.is_empty() {
            // A registered local intermediate can omit its root.
            // This does not establish application trust.
            break;
        }
        if parents.len() != 1 || chain.contains(parents[0]) {
            return Err("certificate chain is ambiguous or cyclic".into());
        }
        let parent = parse(parents[0])?;
        if let Some(limit) = parent
            .basic_constraints()?
            .and_then(|ext| ext.value.path_len_constraint)
        {
            let ca_below = chain.iter().try_fold(0u32, |n, der| {
                let cert = parse(der)?;
                Ok::<_, Error>(
                    n + u32::from(
                        ca(&cert)? && cert.subject() != cert.issuer(),
                    ),
                )
            })?;
            if ca_below > limit {
                return Err("certificate chain exceeds CA path length".into());
            }
        }
        chain.push(parents[0].clone());
    }
    if supplied.iter().any(|der| !chain.contains(der)) {
        return Err("fullchain contains unrelated certificates".into());
    }
    Ok(ImportPlan {
        common_name,
        aliases,
        kind,
        issuer_id,
        certs_der: chain.concat(),
        key_der: key.serialize_der(),
        issued_at: cert.validity().not_before.to_datetime(),
        expires_at: cert.validity().not_after.to_datetime(),
    })
}

/// Export keys in standard, padded PKCS#8 PEM regardless of input encoding.
pub fn private_key_to_pem(der: &[u8]) -> Result<String, Error> {
    Ok(generators::read_private_key_from_der(der)?.serialize_pem())
}

#[cfg(test)]
#[path = "../../tests/ssl/transfer.rs"]
mod tests;
