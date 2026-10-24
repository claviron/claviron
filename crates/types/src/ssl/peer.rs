//! Extract certificate facts; the caller owns chain, validity and EKU checks.
use x509_parser::extensions::GeneralName;

use super::{Error, parsers::take_first_cert_from_der};
use crate::PeerIdentity;

pub fn peer_identity(der: &[u8]) -> Result<PeerIdentity, Error> {
    let cert = take_first_cert_from_der(der)?;
    let common_names = cert
        .subject()
        .iter_common_name()
        .map(|name| name.as_str().map(str::to_owned))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| Error::Other("invalid certificate common name".into()))?;
    let mut subject_uris = Vec::new();
    if let Some(san) = cert.subject_alternative_name().map_err(|_| {
        Error::Other("invalid certificate subject alternative names".into())
    })? {
        for name in &san.value.general_names {
            if let GeneralName::URI(uri) = name {
                subject_uris.push((*uri).to_owned());
            }
        }
    }
    Ok(PeerIdentity {
        certificate: der.into(),
        subject_dn: cert.subject().to_string(),
        issuer_dn: cert.issuer().to_string(),
        common_names,
        subject_uris,
    })
}

#[cfg(test)]
#[path = "../../tests/peer.rs"]
mod tests;
