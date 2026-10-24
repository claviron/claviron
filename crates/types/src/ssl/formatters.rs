use std::fmt::Write as _;

use base64ct::{Base64, Encoding};
use x509_parser::extensions::{GeneralName, ParsedExtension};
use x509_parser::public_key::PublicKey;
use x509_parser::{oid_registry, pem};

use super::error::Error;

/// Convert the hosting store's concatenated DER representation into a public
/// PEM fullchain. Private-key material is not accepted by this API.
pub fn certificate_chain_to_pem(chain: &[u8]) -> Result<String, Error> {
    if chain.is_empty() {
        return Err(Error::Other("certificate chain is empty".to_string()));
    }

    let mut output = String::new();
    let mut remaining = chain;
    while !remaining.is_empty() {
        let (rest, _) = x509_parser::parse_x509_certificate(remaining)
            .map_err(|error| {
                Error::Other(format!("invalid certificate DER: {error}"))
            })?;
        let used = remaining.len() - rest.len();
        write_pem_block(&mut output, "CERTIFICATE", &remaining[..used]);
        remaining = rest;
    }
    Ok(output)
}

pub(crate) fn write_pem_block(
    output: &mut String,
    label: &str,
    der: &[u8],
) {
    let encoded = Base64::encode_string(der);
    let _ = writeln!(output, "-----BEGIN {label}-----");
    for chunk in encoded.as_bytes().chunks(64) {
        output.push_str(
            std::str::from_utf8(chunk).expect("base64 output is ASCII"),
        );
        output.push('\n');
    }
    let _ = writeln!(output, "-----END {label}-----");
}

fn oid_name(oid: &oid_registry::Oid) -> String {
    let registry = oid_registry::OidRegistry::default().with_crypto();
    if let Some(entry) = registry.get(oid) {
        entry.sn().to_string()
    } else {
        oid.to_id_string()
    }
}

/// Format a certificate (PEM or DER) into human-readable text similar to `openssl x509 -text -noout`.
pub fn parse_text_from_pem(cert_pem: &[u8]) -> Result<String, Error> {
    // Detect PEM vs DER
    let der = match pem::parse_x509_pem(cert_pem) {
        Ok((_, p)) => p.contents,
        Err(_) => cert_pem.to_vec(),
    };

    let (_, cert) = x509_parser::parse_x509_certificate(&der)
        .map_err(|e| Error::Other(format!("X509 parse error: {e:?}")))?;

    let mut out = String::new();
    writeln!(&mut out, "Certificate:")?;
    writeln!(&mut out, "    Data:")?;
    writeln!(
        &mut out,
        "        Version: {} (0x{:x})",
        cert.tbs_certificate.version(),
        cert.tbs_certificate.version().0
    )?;
    writeln!(
        &mut out,
        "        Serial Number: {}",
        cert.tbs_certificate.raw_serial_as_string()
    )?;
    writeln!(
        &mut out,
        "        Signature Algorithm: {}",
        oid_name(cert.signature_algorithm.oid())
    )?;
    writeln!(&mut out, "        Issuer: {}", cert.issuer())?;
    let validity = cert.validity();
    writeln!(&mut out, "        Validity")?;
    writeln!(&mut out, "            Not Before: {}", validity.not_before)?;
    writeln!(&mut out, "            Not After : {}", validity.not_after)?;
    writeln!(&mut out, "        Subject: {}", cert.subject())?;
    if let Some(cn) = cert.subject().iter_common_name().next()
        && let Ok(cn_str) = cn.as_str()
    {
        writeln!(&mut out, "            Common Name: {}", cn_str)?;
    }

    // Subject Public Key Info
    writeln!(&mut out, "        Subject Public Key Info:")?;
    writeln!(
        &mut out,
        "            Public Key Algorithm: {}",
        oid_name(&cert.tbs_certificate.subject_pki.algorithm.algorithm)
    )?;
    match cert.tbs_certificate.subject_pki.parsed() {
        Ok(PublicKey::EC(point)) => {
            // For EC keys, show the raw point length (uncompressed point size in bytes)
            writeln!(
                &mut out,
                "                Public-Key: {} bytes",
                point.data().len()
            )?;
        }
        _ => {
            // Fallback: show the raw SubjectPublicKey bitstring length in bytes
            let spk = &cert.tbs_certificate.subject_pki.subject_public_key;
            writeln!(
                &mut out,
                "                Public-Key: {} bytes",
                spk.data.len()
            )?;
        }
    }

    // Extensions
    writeln!(&mut out, "        X509v3 extensions:")?;
    for ext in cert.extensions() {
        match ext.parsed_extension() {
            ParsedExtension::SubjectAlternativeName(san) => {
                let mut parts: Vec<String> = Vec::new();
                for gn in &san.general_names {
                    let s = match gn {
                        GeneralName::DNSName(name) => format!("DNS:{}", name),
                        GeneralName::RFC822Name(email) => {
                            format!("email:{}", email)
                        }
                        GeneralName::URI(uri) => format!("URI:{}", uri),
                        GeneralName::DirectoryName(name) => {
                            format!("DirName:{}", name)
                        }
                        GeneralName::RegisteredID(oid) => {
                            format!("RID:{}", oid)
                        }
                        GeneralName::IPAddress(bytes) => {
                            if bytes.len() == 4 {
                                format!(
                                    "IP:{}.{}.{}.{}",
                                    bytes[0], bytes[1], bytes[2], bytes[3]
                                )
                            } else if bytes.len() == 16 {
                                let mut a = [0u8; 16];
                                a.copy_from_slice(bytes);
                                let ip = std::net::Ipv6Addr::from(a);
                                format!("IP:{}", ip)
                            } else {
                                // Fallback: show raw bytes length
                                format!("IP:[{} bytes]", bytes.len())
                            }
                        }
                        GeneralName::OtherName(..) => "OtherName".to_string(),
                        _ => format!("{:?}", gn),
                    };
                    parts.push(s);
                }
                writeln!(
                    &mut out,
                    "            X509v3 Subject Alternative Name:"
                )?;
                writeln!(&mut out, "                {}", parts.join(", "))?;
            }
            ParsedExtension::KeyUsage(ku) => {
                writeln!(
                    &mut out,
                    "            X509v3 Key Usage: critical={}",
                    ext.critical
                )?;
                writeln!(&mut out, "                {:?}", ku)?;
            }
            ParsedExtension::ExtendedKeyUsage(eku) => {
                writeln!(&mut out, "            X509v3 Extended Key Usage:")?;
                writeln!(&mut out, "                {:?}", eku)?;
            }
            _ => {}
        }
    }

    writeln!(
        &mut out,
        "    Signature Algorithm: {}",
        oid_name(cert.signature_algorithm.oid())
    )?;

    // Signature (first bytes)
    let sig = &cert.signature_value.data;
    let shown = sig
        .iter()
        .take(16)
        .map(|b| format!("{:02x}", b))
        .collect::<Vec<_>>()
        .join(":");
    writeln!(&mut out, "    Signature: {}…", shown)?;

    Ok(out)
}

#[cfg(test)]
#[path = "../../tests/ssl/formatters.rs"]
mod tests;
