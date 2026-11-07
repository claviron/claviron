/// The key type every generator takes, named here so a caller holds one
/// without depending on the certificate library itself.
pub use rcgen::KeyPair;
use rcgen::{
    BasicConstraints, CertificateParams, CertificateSigningRequest,
    CertificateSigningRequestParams, DistinguishedName, DnType,
    ExtendedKeyUsagePurpose, IsCa, Issuer, KeyUsagePurpose, PublicKeyData,
};
use rustls::pki_types::CertificateDer;
use time::{Duration as TimeDuration, OffsetDateTime};

use super::error::Error;
use super::{CommonName, Organization, SubjectUri};
use crate::{Aliases, CertificateKind};

// Certificates and keys cross this module in the form their holder already
// has: a key as the `KeyPair` that was generated or loaded, a certificate as
// DER. PEM is a file format, produced and read where a file is.

/// The subject of a certificate this module issues: its common name and the
/// organization attributes its issuer states. Built here and nowhere else.
fn subject(
    common_name: &CommonName,
    organization: Option<&Organization>,
) -> DistinguishedName {
    let mut subject = DistinguishedName::new();
    subject.push(DnType::CommonName, common_name);
    if let Some(organization) = organization {
        organization.write(&mut subject);
    }
    subject
}

fn generate_keypair_ecdsa_p256() -> Result<KeyPair, Error> {
    Ok(KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256)?)
}

/// CA keys are typically stronger; default to 4096 bits
pub fn generate_ca_key() -> Result<KeyPair, Error> {
    generate_keypair_ecdsa_p256()
}

/// Server/end-entity keys; default to 2048 bits
pub fn generate_private_key() -> Result<KeyPair, Error> {
    generate_keypair_ecdsa_p256()
}

pub fn read_private_key_from_pem(key_content: &[u8]) -> Result<KeyPair, Error> {
    let s = std::str::from_utf8(key_content)?;
    let kp = KeyPair::from_pem(s)?;
    Ok(kp)
}

/// Restore a persisted PKCS#8 private key without rotating its identity.
pub fn read_private_key_from_der(key_content: &[u8]) -> Result<KeyPair, Error> {
    Ok(KeyPair::try_from(key_content)?)
}

pub fn generate_server_csr(
    private_key: &KeyPair,
    cname: &CommonName,
    aliases: impl Into<Vec<String>>,
    org: &Organization,
) -> Result<CertificateSigningRequest, Error> {
    // Build params with SANs from CSR
    // `CertificateParams::new` accepts `impl Into<Vec<String>>`.
    // We have `impl From<&Aliases> for Vec<String>`, so `&Aliases` implements `Into<Vec<String>>` automatically.

    // let aliases: Vec<String> = aliases.into();
    // debug!(?aliases, "creating server csr from aliases");
    let mut params = CertificateParams::new(aliases)?;

    params.distinguished_name = subject(cname, Some(org));

    // Create CSR using provided keypair
    let csr = params.serialize_request(private_key)?;

    // Return DER-encoded CSR bytes (use pem::encode if you need PEM)
    // Ok(csr.der().as_ref().to_vec())
    Ok(csr)
}

/// Generate a server certificate for the names of a request, signed by the
/// given CA certificate (DER) and its key.
pub fn generate_server_cert(
    csr: &CertificateSigningRequest,
    subject_key: &KeyPair,
    ca_cert: &[u8],
    ca_key: &KeyPair,
    validity_days: u32,
) -> Result<CertificateDer<'static>, Error> {
    let aliases = Aliases::from_csr_der(csr.der().as_ref())?;
    let cname =
        CommonName::from(aliases.first().ok_or(Error::NoDomainsProvided)?);
    generate_leaf_cert(
        CertificateKind::Server,
        &cname,
        subject_key,
        ca_cert,
        ca_key,
        validity_days,
        &Vec::<String>::from(&aliases),
    )
}

/// Generate a client certificate (for mutual TLS / Postgres clientcert) signed by the given CA.
/// Modern profile (ECDSA P-256):
///   - basicConstraints = CA:FALSE
///   - keyUsage        = digitalSignature
///   - extendedKeyUsage= clientAuth
///   - subjectAltName  = CN + any extra SAN entries (DNS/IP/email/URI strings are auto-parsed)
pub fn generate_client_cert(
    client_name: &CommonName,
    client_key: &KeyPair,
    ca_cert: &[u8],
    ca_key: &KeyPair,
    validity_days: u32,
    extra_sans: &[impl Into<String> + Clone],
) -> Result<CertificateDer<'static>, Error> {
    generate_leaf_cert(
        CertificateKind::Client,
        client_name,
        client_key,
        ca_cert,
        ca_key,
        validity_days,
        extra_sans,
    )
}

/// Generate a typed leaf identity. Server certificates retain Claviron's
/// serverAuth + clientAuth profile; client certificates have clientAuth only.
pub fn generate_leaf_cert(
    kind: CertificateKind,
    client_name: &CommonName,
    key: &KeyPair,
    ca_cert: &[u8],
    ca_key: &KeyPair,
    validity_days: u32,
    extra_sans: &[impl Into<String> + Clone],
) -> Result<CertificateDer<'static>, Error> {
    let mut sans = vec![String::from(client_name)];
    for name in extra_sans {
        let name = name.clone().into();
        if !sans.contains(&name) {
            sans.push(name);
        }
    }
    let mut params = leaf_params(kind, client_name, sans)?;
    let (issuer, _) =
        validated_issuer(&mut params, ca_cert, ca_key, validity_days)?;
    Ok(params.signed_by(key, &issuer)?.into())
}

/// Verify a PKCS#10 request and sign only its public key with issuer-owned claims.
pub fn sign_client_csr(
    common_name: &CommonName,
    csr_der: &[u8],
    ca_cert: &[u8],
    ca_key: &KeyPair,
    validity_days: u32,
    aliases: Vec<String>,
) -> Result<CertificateDer<'static>, Error> {
    let csr = CertificateSigningRequestParams::from_der(&csr_der.into())?;
    let mut params =
        leaf_params(CertificateKind::Client, common_name, aliases)?;
    let mut serial = [0; 16];
    rustls::crypto::aws_lc_rs::default_provider()
        .secure_random
        .fill(&mut serial)
        .map_err(|_| {
            Error::Other("could not generate a certificate serial".into())
        })?;
    // Keep each serial positive and independent of a reused public key.
    serial[0] = (serial[0] & 0x7f) | 0x40;
    params.serial_number = Some(rcgen::SerialNumber::from_slice(&serial));
    let (issuer, _) =
        validated_issuer(&mut params, ca_cert, ca_key, validity_days)?;
    Ok(params.signed_by(&csr.public_key, &issuer)?.into())
}

fn leaf_params(
    kind: CertificateKind,
    client_name: &CommonName,
    sans: Vec<String>,
) -> Result<CertificateParams, Error> {
    if !matches!(kind, CertificateKind::Server | CertificateKind::Client) {
        return Err(Error::Other(
            "leaf issuance requires server or client".into(),
        ));
    }
    // rcgen detects DNS/IP only. URI identities must use the URI GeneralName.
    let (uris, names): (Vec<_>, Vec<_>) = sans.into_iter().partition(|value| {
        value.contains(':') && value.parse::<std::net::IpAddr>().is_err()
    });
    let mut params = CertificateParams::new(names)?;
    for uri in uris {
        let uri = uri.parse::<SubjectUri>()?;
        params.subject_alt_names.push((&uri).try_into()?);
    }

    params.distinguished_name = subject(client_name, None);

    // Leaf cert
    params.is_ca = IsCa::NoCa;

    // Key Usage: ECDSA client certificate only needs DigitalSignature
    params.key_usages = vec![KeyUsagePurpose::DigitalSignature];

    // EKU: clientAuth
    params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
    if kind == CertificateKind::Server {
        params.extended_key_usages.push(ExtendedKeyUsagePurpose::ServerAuth);
    }

    Ok(params)
}

/// The issuer a certificate is signed by, once it is known that the authority
/// may sign, that the key is its own and that the requested validity lies
/// inside the authority's. Sets that validity on `params` and returns the
/// authority's own delegation limit beside the issuer.
fn validated_issuer<'key>(
    params: &mut CertificateParams,
    ca_cert: &[u8],
    ca_key: &'key KeyPair,
    validity_days: u32,
) -> Result<(Issuer<'static, &'key KeyPair>, Option<u32>), Error> {
    if validity_days == 0 {
        return Err(Error::Other("validity_days must be positive".into()));
    }
    let now = OffsetDateTime::now_utc();
    let expires_at = now
        .checked_add(TimeDuration::days(i64::from(validity_days)))
        .ok_or_else(|| Error::Other("validity_days is out of range".into()))?;
    let ca = super::parsers::take_first_cert_from_der(ca_cert)?;
    if !ca.is_ca()
        || ca.key_usage()?.is_some_and(|usage| !usage.value.key_cert_sign())
    {
        return Err(Error::Other(
            "authority is not permitted to sign certificates".into(),
        ));
    }
    if ca.public_key().raw != ca_key.subject_public_key_info() {
        return Err(Error::Other(
            "authority certificate and private key do not match".into(),
        ));
    }
    if now.unix_timestamp() < ca.validity().not_before.timestamp()
        || expires_at.unix_timestamp() > ca.validity().not_after.timestamp()
    {
        return Err(Error::Other(
            "requested validity is outside the authority validity period"
                .into(),
        ));
    }
    // Strict verifiers refuse a chain without it (Python's default since 3.13).
    params.use_authority_key_identifier_extension = true;
    params.not_before = now;
    params.not_after = expires_at;
    let path_limit =
        ca.basic_constraints()?.and_then(|ext| ext.value.path_len_constraint);
    let issuer = Issuer::from_ca_cert_der(&ca_cert.into(), ca_key)?;
    Ok((issuer, path_limit))
}

/// Generate a self-signed **CA certificate** with the provided CA key.
/// The subject carries the organization attributes that are stated.
pub fn generate_ca_cert(
    ca_name: &CommonName,
    ca_key: &KeyPair,
    validity_days: u32,
    org: &Organization,
) -> Result<CertificateDer<'static>, Error> {
    // Start with empty SANs for a CA cert
    let mut params = CertificateParams::new(Vec::<String>::new())?;

    // Mark as CA with unconstrained path length
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);

    // Key usages appropriate for a CA
    params.key_usages =
        vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];

    params.distinguished_name = subject(ca_name, Some(org));

    if validity_days == 0 {
        return Err(Error::Other("validity_days must be positive".into()));
    }
    let now = OffsetDateTime::now_utc();
    params.not_before = now;
    params.not_after = now
        .checked_add(TimeDuration::days(i64::from(validity_days)))
        .ok_or_else(|| Error::Other("validity_days is out of range".into()))?;

    Ok(params.self_signed(ca_key)?.into())
}

/// Generate an **intermediate CA certificate** signed by an existing CA (issuer).
///
/// Profile:
///   - basicConstraints = CA:TRUE (optionally constrained by `path_len`)
///   - keyUsage        = keyCertSign, cRLSign
///   - extendedKeyUsage: none (not needed for CA)
///   - subjectAltName  = empty (a CA certificate needs none)
///
/// The subject carries the organization attributes that are stated. The
/// issuer certificate is DER.
pub fn generate_intermediate_ca_cert(
    int_key: &KeyPair,
    issuer_ca_cert: &[u8],
    issuer_ca_key: &KeyPair,
    common_name: &CommonName,
    validity_days: u32,
    org: &Organization,
    path_len: Option<u8>,
) -> Result<CertificateDer<'static>, Error> {
    // Start with an empty SAN list for a CA certificate
    let mut params = CertificateParams::new(Vec::<String>::new())?;

    // Mark as CA (optional path length constraint)
    params.is_ca = match path_len {
        Some(n) => IsCa::Ca(BasicConstraints::Constrained(n)),
        None => IsCa::Ca(BasicConstraints::Unconstrained),
    };

    // Key usages appropriate for a CA
    params.key_usages =
        vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];

    params.distinguished_name = subject(common_name, Some(org));

    let (issuer, path_limit) = validated_issuer(
        &mut params,
        issuer_ca_cert,
        issuer_ca_key,
        validity_days,
    )?;
    if let Some(limit) = path_limit
        && (limit == 0
            || path_len.is_none_or(|requested| u32::from(requested) >= limit))
    {
        return Err(Error::Other(
            "requested CA path length exceeds the issuer delegation limit"
                .into(),
        ));
    }

    Ok(params.signed_by(int_key, &issuer)?.into())
}

const ALPHANUM: &[u8] =
    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
pub fn random_alphanum(capacity: usize) -> String {
    let mut out = String::with_capacity(capacity);
    for _ in 0..capacity {
        let idx = fastrand::usize(..ALPHANUM.len());
        out.push(ALPHANUM[idx] as char);
    }
    out
}

#[cfg(test)]
#[path = "../../tests/ssl/generators.rs"]
mod tests;
