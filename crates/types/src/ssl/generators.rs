use rcgen::{
    BasicConstraints, CertificateParams, CertificateSigningRequest,
    DistinguishedName, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose, PublicKeyData,
};
use time::{Duration as TimeDuration, OffsetDateTime};

use super::Organization;
use super::error::Error;
use crate::{Aliases, CertificateKind, KeyString};

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
    cname: &str,
    aliases: impl Into<Vec<String>>,
    org: &Organization,
) -> Result<CertificateSigningRequest, Error> {
    // Build params with SANs from CSR
    // `CertificateParams::new` accepts `impl Into<Vec<String>>`.
    // We have `impl From<&Aliases> for Vec<String>`, so `&Aliases` implements `Into<Vec<String>>` automatically.

    // let aliases: Vec<String> = aliases.into();
    // debug!(?aliases, "creating server csr from aliases");
    let mut params = CertificateParams::new(aliases)?;

    // Subject DN: set CN to the first extracted domain
    let mut dn = DistinguishedName::new();
    dn.push(DnType::CommonName, cname.to_string());

    if !org.name.is_empty() {
        dn.push(DnType::OrganizationName, org.name.to_string());
    }
    if !org.country.is_empty() {
        dn.push(DnType::CountryName, org.country.to_string());
    }
    params.distinguished_name = dn;

    // Create CSR using provided keypair
    let csr = params.serialize_request(private_key)?;

    // Return DER-encoded CSR bytes (use pem::encode if you need PEM)
    // Ok(csr.der().as_ref().to_vec())
    Ok(csr)
}

/// Generate a server certificate signed by the given CA (PEM), using rcgen.
/// Returns the certificate in PEM format. The end-entity key is provided as PEM bytes.
pub fn generate_server_cert(
    csr: &CertificateSigningRequest,
    subject_key_pem: &[u8],
    ca_cert_pem: &[u8],
    ca_key_pem: &[u8],
    validity_days: u32,
) -> Result<Vec<u8>, Error> {
    let aliases = Aliases::from_csr_der(csr.der().as_ref())?;
    let cname = aliases.first().ok_or(Error::NoDomainsProvided)?.key_string();
    generate_leaf_cert(
        CertificateKind::Server,
        &cname,
        subject_key_pem,
        ca_cert_pem,
        ca_key_pem,
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
///     Returns client certificates data in PEM format.
pub fn generate_client_cert(
    client_name: &str,
    client_key_pem: &[u8],
    ca_cert_pem: &[u8],
    ca_key_pem: &[u8],
    validity_days: u32,
    extra_sans: &[impl Into<String> + Clone],
) -> Result<Vec<u8>, Error> {
    generate_leaf_cert(
        CertificateKind::Client,
        client_name,
        client_key_pem,
        ca_cert_pem,
        ca_key_pem,
        validity_days,
        extra_sans,
    )
}

/// Generate a typed leaf identity. Server certificates retain Claviron's
/// serverAuth + clientAuth profile; client certificates have clientAuth only.
pub fn generate_leaf_cert(
    kind: CertificateKind,
    client_name: &str,
    client_key_pem: &[u8],
    ca_cert_pem: &[u8],
    ca_key_pem: &[u8],
    validity_days: u32,
    extra_sans: &[impl Into<String> + Clone],
) -> Result<Vec<u8>, Error> {
    if !matches!(kind, CertificateKind::Server | CertificateKind::Client) {
        return Err(Error::Other(
            "leaf issuance requires server or client".into(),
        ));
    }
    // Build SANs: CN first, then extras (dedup preserving order)
    let mut sans: Vec<String> = Vec::with_capacity(1 + extra_sans.len());
    sans.push(client_name.to_string());
    for s in extra_sans {
        let s_str = s.clone().into();
        if s_str != client_name && !sans.iter().any(|x| x == &s_str) {
            sans.push(s_str);
        }
    }

    // rcgen detects DNS/IP only. URI identities must use the URI GeneralName.
    let (uris, names): (Vec<_>, Vec<_>) = sans.into_iter().partition(|value| {
        value.contains(':') && value.parse::<std::net::IpAddr>().is_err()
    });
    let mut params = CertificateParams::new(names)?;
    for uri in uris {
        let uri = uri.parse::<super::SubjectUri>()?;
        params.subject_alt_names.push(rcgen::SanType::URI(
            uri.as_str().try_into().map_err(|_| {
                Error::Other("subject URI must be ASCII".into())
            })?,
        ));
    }

    // Subject DN (CN)
    let mut dn = DistinguishedName::new();
    dn.push(DnType::CommonName, client_name);
    params.distinguished_name = dn;

    // Leaf cert
    params.is_ca = IsCa::NoCa;

    // Key Usage: ECDSA client certificate only needs DigitalSignature
    params.key_usages = vec![KeyUsagePurpose::DigitalSignature];

    // EKU: clientAuth
    params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
    if kind == CertificateKind::Server {
        params.extended_key_usages.push(ExtendedKeyUsagePurpose::ServerAuth);
    }

    let ca_issuer =
        validated_issuer(&mut params, ca_cert_pem, ca_key_pem, validity_days)?;

    // Prepare client keypair from PEM bytes
    let client_key = KeyPair::from_pem(std::str::from_utf8(client_key_pem)?)?;

    // Generate end-entity keypair and issue cert
    let cert = params.signed_by(&client_key, &ca_issuer)?;

    Ok(cert.pem().into_bytes())
}

fn validated_issuer(
    params: &mut CertificateParams,
    ca_cert_pem: &[u8],
    ca_key_pem: &[u8],
    validity_days: u32,
) -> Result<Issuer<'static, KeyPair>, Error> {
    if validity_days == 0 {
        return Err(Error::Other("validity_days must be positive".into()));
    }
    let now = OffsetDateTime::now_utc();
    let expires_at = now
        .checked_add(TimeDuration::days(i64::from(validity_days)))
        .ok_or_else(|| Error::Other("validity_days is out of range".into()))?;
    let ca_key = KeyPair::from_pem(std::str::from_utf8(ca_key_pem)?)?;
    let (_, ca_pem) = x509_parser::pem::parse_x509_pem(ca_cert_pem)
        .map_err(|_| Error::Other("invalid CA certificate PEM".into()))?;
    let ca = super::parsers::take_first_cert_from_der(&ca_pem.contents)?;
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
    params.not_before = now;
    params.not_after = expires_at;
    let ca_issuer =
        Issuer::from_ca_cert_pem(std::str::from_utf8(ca_cert_pem)?, ca_key)?;

    Ok(ca_issuer)
}

/// Generate a self-signed **CA certificate** using the provided CA key PEM bytes.
/// Accepts optional organization name and country code for DN fields.
/// Returns the CA certificate PEM bytes.
pub fn generate_ca_cert(
    ca_name: &str,
    ca_key_pem: &[u8],
    validity_days: u32,
    org_name: Option<&str>,
    country: Option<&str>,
) -> Result<Vec<u8>, Error> {
    // Start with empty SANs for a CA cert
    let mut params = CertificateParams::new(Vec::<String>::new())?;

    // Mark as CA with unconstrained path length
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);

    // Key usages appropriate for a CA
    params.key_usages =
        vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];

    // Subject DN
    let mut dn = DistinguishedName::new();
    dn.push(DnType::CommonName, ca_name);

    if let Some(name) = org_name
        && !name.is_empty()
    {
        dn.push(DnType::OrganizationName, name.to_string());
    }
    if let Some(cc) = country
        && !cc.is_empty()
    {
        dn.push(DnType::CountryName, cc.to_string());
    }

    params.distinguished_name = dn;

    if validity_days == 0 {
        return Err(Error::Other("validity_days must be positive".into()));
    }
    let now = OffsetDateTime::now_utc();
    params.not_before = now;
    params.not_after = now
        .checked_add(TimeDuration::days(i64::from(validity_days)))
        .ok_or_else(|| Error::Other("validity_days is out of range".into()))?;

    // Generate CA key from PEM and self-sign
    let ca_key = KeyPair::from_pem(std::str::from_utf8(ca_key_pem)?)?;
    let ca_cert = params.self_signed(&ca_key)?;

    // PEM outputs
    let ca_cert_pem = ca_cert.pem().into_bytes();

    Ok(ca_cert_pem)
}

/// Generate an **intermediate CA certificate** signed by an existing CA (issuer).
///
/// Profile:
///   - basicConstraints = CA:TRUE (optionally constrained by `path_len`)
///   - keyUsage        = keyCertSign, cRLSign
///   - extendedKeyUsage: none (not needed for CA)
///   - subjectAltName  = empty (CA certlerde SAN zorunlu değildir)
///
/// Accepts optional organization name and country code for DN fields.
/// Returns the intermediate certificate PEM bytes.
pub fn generate_intermediate_ca_cert(
    int_key_pem: &[u8],
    issuer_ca_cert_pem: &[u8],
    issuer_ca_key_pem: &[u8],
    common_name: &str,
    validity_days: u32,
    org: &Organization,
    path_len: Option<u8>,
) -> Result<Vec<u8>, Error> {
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

    // Subject DN
    let mut dn = DistinguishedName::new();
    dn.push(DnType::CommonName, common_name);
    if !org.name.is_empty() {
        dn.push(DnType::OrganizationName, org.name.to_string());
    }
    if !org.country.is_empty() {
        dn.push(DnType::CountryName, org.country.to_string());
    }
    params.distinguished_name = dn;

    let issuer = validated_issuer(
        &mut params,
        issuer_ca_cert_pem,
        issuer_ca_key_pem,
        validity_days,
    )?;
    let (_, pem) = x509_parser::pem::parse_x509_pem(issuer_ca_cert_pem)
        .map_err(|_| Error::Other("invalid CA certificate PEM".into()))?;
    let ca = super::parsers::take_first_cert_from_der(&pem.contents)?;
    if let Some(limit) =
        ca.basic_constraints()?.and_then(|ext| ext.value.path_len_constraint)
    {
        if limit == 0
            || path_len.is_none_or(|requested| u32::from(requested) >= limit)
        {
            return Err(Error::Other(
                "requested CA path length exceeds the issuer delegation limit"
                    .into(),
            ));
        }
    }

    // Build intermediate CA key pair from PEM
    let int_key = KeyPair::from_pem(std::str::from_utf8(int_key_pem)?)?;

    // Sign using the provided intermediate CA key
    let int_cert = params.signed_by(&int_key, &issuer)?;

    Ok(int_cert.pem().into_bytes())
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
