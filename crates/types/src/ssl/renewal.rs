//! Same-key renewal: retain the encoded subject, issuer, SPKI and every
//! extension verbatim. Only validity, serial and signature are replaced.
use rcgen::PublicKeyData;
use rustls::SignatureScheme;
use rustls::crypto::aws_lc_rs;
use rustls::pki_types::PrivateKeyDer;
use time::{Duration, OffsetDateTime};

use super::{SSLError as Error, generators, parsers};

/// Re-sign one DER certificate with its existing key and original issuer.
/// A missing issuer means self-signed CA renewal, never a self-signed leaf.
/// Unknown signature algorithms fail closed; unknown extensions are copied.
pub fn renew_certificate(
    certificate: &[u8],
    subject_key: &[u8],
    issuer: Option<(&[u8], &[u8])>,
    validity_days: u32,
) -> Result<Vec<u8>, Error> {
    let old = parsers::take_first_cert_from_der(certificate)?;
    let key = generators::read_private_key_from_der(subject_key)?;
    if old.public_key().raw != key.subject_public_key_info() {
        return Err(Error::Other(
            "certificate and private key do not match".into(),
        ));
    }
    let now = OffsetDateTime::now_utc();
    let expires = now
        .checked_add(Duration::days(i64::from(validity_days)))
        .filter(|_| validity_days > 0)
        .ok_or_else(|| {
            Error::Other("validity_days must be positive and in range".into())
        })?;
    let signing_key = match issuer {
        Some((chain, issuer_key)) => {
            let ca = parsers::take_first_cert_from_der(chain)?;
            let key = generators::read_private_key_from_der(issuer_key)?;
            if !ca.is_ca()
                || ca
                    .key_usage()?
                    .is_some_and(|usage| !usage.value.key_cert_sign())
                || ca.public_key().raw != key.subject_public_key_info()
                || old.issuer().as_raw() != ca.subject().as_raw()
            {
                return Err(Error::Other(
                    "renewal issuer does not match the original signing CA"
                        .into(),
                ));
            }
            old.verify_signature(Some(ca.public_key()))?;
            if now.unix_timestamp() < ca.validity().not_before.timestamp()
                || expires.unix_timestamp()
                    > ca.validity().not_after.timestamp()
            {
                return Err(Error::Other("requested validity is outside the authority validity period".into()));
            }
            issuer_key
        }
        None => {
            if !old.is_ca() || old.subject().as_raw() != old.issuer().as_raw() {
                return Err(Error::Other(
                    "only a self-signed CA can renew without an issuer".into(),
                ));
            }
            old.verify_signature(None)?;
            subject_key
        }
    };
    let scheme = match old.signature_algorithm.algorithm.to_id_string().as_str()
    {
        "1.2.840.10045.4.3.2" => SignatureScheme::ECDSA_NISTP256_SHA256,
        "1.2.840.10045.4.3.3" => SignatureScheme::ECDSA_NISTP384_SHA384,
        "1.2.840.113549.1.1.11" => SignatureScheme::RSA_PKCS1_SHA256,
        "1.2.840.113549.1.1.12" => SignatureScheme::RSA_PKCS1_SHA384,
        "1.2.840.113549.1.1.13" => SignatureScheme::RSA_PKCS1_SHA512,
        "1.3.101.112" => SignatureScheme::ED25519,
        _ => {
            return Err(Error::Other(
                "unsupported renewal signature algorithm".into(),
            ));
        }
    };
    let provider = aws_lc_rs::default_provider();
    let key = provider
        .key_provider
        .load_private_key(PrivateKeyDer::Pkcs8(signing_key.to_vec().into()))
        .map_err(|error| Error::Other(error.to_string()))?;
    let signer = key.choose_scheme(&[scheme]).ok_or_else(|| {
        Error::Other(
            "issuer key cannot retain the certificate signature algorithm"
                .into(),
        )
    })?;
    let (outer, rest) = element(certificate)?;
    if !rest.is_empty() || outer.tag != 0x30 {
        return Err(Error::Other("renewal expects one DER certificate".into()));
    }
    let fields = elements(outer.content)?;
    if fields.len() != 3 || fields[0].tag != 0x30 || fields[2].tag != 0x03 {
        return Err(Error::Other("invalid certificate DER shape".into()));
    }
    let tbs = elements(fields[0].content)?;
    let serial_index =
        usize::from(tbs.first().is_some_and(|field| field.tag == 0xa0));
    let validity_index = serial_index + 3;
    if tbs.len() < validity_index + 3
        || tbs[serial_index].tag != 0x02
        || tbs[validity_index].tag != 0x30
        || tbs[serial_index + 1].raw != fields[1].raw
    {
        return Err(Error::Other("invalid certificate TBS shape".into()));
    }
    // RFC 5280 serials are positive and at most 20 octets. Use the crypto
    // provider's RNG, not a deterministic key-derived serial on each renewal.
    let mut serial = [0u8; 20];
    provider.secure_random.fill(&mut serial).map_err(|_| {
        Error::Other("cryptographic random source failed".into())
    })?;
    serial[0] = (serial[0] & 0x7f) | 1;
    let serial = encode(0x02, &serial);
    let validity = encode(0x30, &[date(now)?, date(expires)?].concat());
    let body = tbs
        .iter()
        .enumerate()
        .flat_map(|(index, field)| {
            if index == serial_index {
                serial.as_slice()
            } else if index == validity_index {
                validity.as_slice()
            } else {
                field.raw
            }
        })
        .copied()
        .collect::<Vec<_>>();
    let body = encode(0x30, &body);
    let signature =
        signer.sign(&body).map_err(|error| Error::Other(error.to_string()))?;
    let mut bits = vec![0]; // DER BIT STRING: no unused bits in the signature.
    bits.extend(signature);
    Ok(encode(
        0x30,
        &[body.as_slice(), fields[1].raw, &encode(0x03, &bits)].concat(),
    ))
}

struct Element<'a> {
    tag: u8,
    raw: &'a [u8],
    content: &'a [u8],
}

fn element(bytes: &[u8]) -> Result<(Element<'_>, &[u8]), Error> {
    let invalid = || Error::Other("invalid DER length".into());
    let (&tag, rest) = bytes.split_first().ok_or_else(invalid)?;
    let (&length, _) = rest.split_first().ok_or_else(invalid)?;
    let (header, length) = if length < 128 {
        (2usize, usize::from(length))
    } else {
        let count = usize::from(length & 0x7f);
        if count == 0 || count > size_of::<usize>() {
            return Err(invalid());
        }
        let raw = bytes.get(2..2 + count).ok_or_else(invalid)?;
        if raw[0] == 0 {
            return Err(invalid());
        }
        let length = raw
            .iter()
            .try_fold(0usize, |length, byte| {
                length.checked_mul(256)?.checked_add(usize::from(*byte))
            })
            .ok_or_else(invalid)?;
        if length < 128 {
            return Err(invalid());
        }
        (2 + count, length)
    };
    let end = header.checked_add(length).ok_or_else(invalid)?;
    let raw = bytes.get(..end).ok_or_else(invalid)?;
    Ok((Element { tag, raw, content: &raw[header..] }, &bytes[end..]))
}

fn elements(mut bytes: &[u8]) -> Result<Vec<Element<'_>>, Error> {
    let mut result = Vec::new();
    while !bytes.is_empty() {
        let (field, rest) = element(bytes)?;
        result.push(field);
        bytes = rest;
    }
    Ok(result)
}

fn encode(
    tag: u8,
    content: &[u8],
) -> Vec<u8> {
    let mut output = vec![tag];
    if content.len() < 128 {
        output.push(content.len() as u8);
    } else {
        let length = content.len().to_be_bytes();
        let start = length
            .iter()
            .position(|byte| *byte != 0)
            .unwrap_or(length.len() - 1);
        output.push(0x80 | (length.len() - start) as u8);
        output.extend_from_slice(&length[start..]);
    }
    output.extend_from_slice(content);
    output
}

fn date(date: OffsetDateTime) -> Result<Vec<u8>, Error> {
    let year = date.year();
    if !(1950..=9999).contains(&year) {
        return Err(Error::Other(
            "validity date cannot be encoded in X.509".into(),
        ));
    }
    let prefix = if year < 2050 {
        format!("{:02}", year % 100)
    } else {
        format!("{year:04}")
    };
    let value = format!(
        "{prefix}{:02}{:02}{:02}{:02}{:02}Z",
        u8::from(date.month()),
        date.day(),
        date.hour(),
        date.minute(),
        date.second()
    );
    Ok(encode(if year < 2050 { 0x17 } else { 0x18 }, value.as_bytes()))
}

#[cfg(test)]
#[path = "../../tests/ssl/renewal.rs"]
mod tests;
