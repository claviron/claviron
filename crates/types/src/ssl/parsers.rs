use std::str::FromStr;

use rustls::pki_types::CertificateDer;
use rustls::pki_types::pem::PemObject;
use x509_parser::asn1_rs::FromDer;
use x509_parser::certificate::X509Certificate;
use x509_parser::certification_request::X509CertificationRequest;
use x509_parser::extensions::{GeneralName, ParsedExtension};
use x509_parser::pem::parse_x509_pem;
use x509_parser::prelude::X509Error;

use super::error::Error;
use super::{Issuer, Organization};
use crate::{Aliases, Host};

/// PEM files include BEGIN/END headers and base64 armor; this helper strips those and
/// returns the raw DER bytes. If parsing as PEM fails, it returns the original bytes unchanged.
pub fn sanitize_pem_content_to_der(content: &[u8]) -> Vec<u8> {
    match parse_x509_pem(content) {
        Ok((_, pem)) => pem.contents,
        Err(_) => content.to_vec(),
    }
}

pub fn take_first_cert_from_der(
    der: &[u8]
) -> Result<X509Certificate<'_>, Error> {
    let (_, cert) = x509_parser::parse_x509_certificate(der)
        .map_err(|e| Error::X509Parser(X509Error::from(e)))?;
    Ok(cert)
}

/// #CERT_CHAIN_SAVING
/// Parses a PEM-encoded certificate chain and converts it into a Vec<CertificateDer>.
/// Used when importing new or renewed certificates before storing or serving them.
pub fn concat_chain_bytes_from_pem(pem: &[u8]) -> Result<Vec<u8>, Error> {
    let mut out = Vec::new();
    for cert in CertificateDer::pem_slice_iter(pem) {
        let cert = cert.map_err(|e| {
            Error::from(format!("Failed to parse certificate PEM: {e}"))
        })?;
        out.extend_from_slice(cert.as_ref()); // append each DER
    }
    Ok(out)
}

impl<'a> TryFrom<&'a X509Certificate<'a>> for Organization {
    type Error = Error;

    fn try_from(cert: &'a X509Certificate<'a>) -> Result<Self, Self::Error> {
        let mut org = Organization::default();
        if let Some(attr) = cert.subject().iter_organization().next() {
            org.name = attr.as_str()?.to_owned();
        }
        if let Some(attr) = cert.subject().iter_email().next() {
            org.email = attr.as_str()?.to_owned();
        }

        if let Some(attr) = cert.subject().iter_country().next() {
            org.country = attr.as_str()?.to_owned();
        }
        if let Some(attr) = cert.subject().iter_locality().next() {
            org.city = attr.as_str()?.to_owned();
        }
        if let Some(attr) = cert.subject().iter_organizational_unit().next() {
            org.unit = attr.as_str()?.to_owned();
        }
        Ok(org)
    }
}

impl<'a> TryFrom<&'a X509Certificate<'a>> for Issuer {
    type Error = Error;

    fn try_from(cert: &'a X509Certificate<'a>) -> Result<Self, Self::Error> {
        let mut iss = Issuer::default();
        // use issuer() instead of subject() if you want the signing CA info,
        // or subject() if you want the end-entity info:

        let issuer_name = cert.issuer();
        if let Some(attr) = issuer_name.iter_common_name().next() {
            iss.cn = attr.as_str()?.to_owned();
        }

        if let Some(attr) = issuer_name.iter_country().next() {
            iss.country = attr.as_str()?.to_owned();
        }
        if let Some(attr) = issuer_name.iter_organization().next() {
            iss.organization = attr.as_str()?.to_owned();
        }
        if let Some(attr) = issuer_name.iter_organizational_unit().next() {
            iss.unit = attr.as_str()?.to_owned();
        }
        Ok(iss)
    }
}

impl<'a> TryFrom<&'a X509Certificate<'a>> for Host {
    type Error = Error;

    fn try_from(cert: &'a X509Certificate<'a>) -> Result<Self, Self::Error> {
        let str = cert
            .subject()
            .iter_common_name()
            .next()
            .ok_or_else(|| Error::Other("missing subject CN".into()))?
            .as_str()?;
        Ok(Self::from_str(str)?)
    }
}
impl<'a> TryFrom<&'a X509Certificate<'a>> for Aliases {
    type Error = Error;

    fn try_from(cert: &'a X509Certificate<'a>) -> Result<Self, Self::Error> {
        // Collect parsed extensions with the correct lifetime
        let parsed_exts: Vec<ParsedExtension<'a>> = cert
            .extensions()
            .iter()
            .map(|ext| ext.parsed_extension().clone())
            .collect();

        Aliases::from_extensions(&parsed_exts)
    }
}

impl<'a> TryFrom<&'a X509CertificationRequest<'a>> for Aliases {
    type Error = Error;

    fn try_from(
        csr: &'a X509CertificationRequest<'a>
    ) -> Result<Self, Self::Error> {
        // Collect parsed extensions with the correct lifetime
        let mut parsed_exts: Vec<ParsedExtension> = Vec::new();
        if let Some(iter) = csr.requested_extensions() {
            for ext in iter {
                parsed_exts.push(ext.clone());
            }
        }
        let mut aliases = Self::from_extensions(&parsed_exts)?;
        for attr in csr.certification_request_info.subject.iter_common_name() {
            let name = attr.as_str()?;
            let host = Host::from_str(name).map_err(Error::InvalidHostname)?;
            aliases.push_unique(host);
        }
        Ok(aliases)
    }
}

impl Aliases {
    pub fn from_cert_bytes(cert_data: &[u8]) -> Result<Aliases, Error> {
        // Accept both PEM and DER; try PEM first regardless of header, then fall back to DER
        let der_data = sanitize_pem_content_to_der(cert_data);
        let cert = take_first_cert_from_der(&der_data)?;
        Self::try_from(&cert)
    }

    pub fn from_csr_der(data: &[u8]) -> Result<Aliases, Error> {
        let vec = data.to_vec();
        let (_, csr) = X509CertificationRequest::from_der(&vec)
            .map_err(|e| Error::Other(format!("DER parse error: {e}")))?;
        Self::try_from(&csr)
    }

    pub fn from_csr_content(content: &[u8]) -> Result<Aliases, Error> {
        let der = sanitize_pem_content_to_der(content);
        Self::from_csr_der(&der)
    }
}

impl Aliases {
    /// Build alias list from a single parsed extension (typically a SAN).
    pub fn from_extensions(
        extensions: &[ParsedExtension]
    ) -> Result<Aliases, Error> {
        let mut aliases = Aliases::new();
        for extension in extensions {
            if let ParsedExtension::SubjectAlternativeName(san) = extension {
                for name in &san.general_names {
                    // debug!("Found subject alternative name: {:?}", name);
                    match name {
                        GeneralName::DNSName(dns_name) => {
                            let host = Host::from_str(dns_name)
                                .map_err(Error::InvalidHostname)?;
                            aliases.push_unique(host);
                        }
                        GeneralName::IPAddress(raw_ip) => {
                            let host = Host::try_from(*raw_ip)
                                .map_err(Error::InvalidHostname)?;
                            aliases.push_unique(host);
                        }
                        GeneralName::OtherName(_, _) => {}
                        GeneralName::RFC822Name(_) => {}
                        GeneralName::X400Address(_) => {}
                        GeneralName::DirectoryName(_) => {}
                        GeneralName::EDIPartyName(_) => {}
                        GeneralName::URI(_) => {}
                        GeneralName::RegisteredID(_) => {}
                        GeneralName::Invalid(_, _) => {}
                    }
                }
            }
        }
        Ok(aliases)
    }
}
