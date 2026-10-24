use x509_parser::prelude::{FromDer, X509Certificate};

/// X.509 issuer Name DER and the normalized unsigned certificate serial.
/// This identity is independent of database row IDs and certificate storage.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct RevocationId {
    issuer: Box<[u8]>,
    serial: Box<[u8]>,
}

impl RevocationId {
    /// The issuer must be a parsed X.509 Name's DER, never a display string.
    pub fn new(
        issuer: &[u8],
        serial: &[u8],
    ) -> Result<Self, super::Error> {
        let first =
            serial.iter().position(|byte| *byte != 0).ok_or_else(|| {
                super::Error::Other(
                    "certificate serial must be positive".into(),
                )
            })?;
        if issuer.is_empty() {
            return Err(super::Error::Other(
                "certificate issuer must not be empty".into(),
            ));
        }
        Ok(Self { issuer: issuer.into(), serial: serial[first..].into() })
    }

    /// Reads the first certificate, including a leaf-first stored DER chain.
    pub fn from_certificate_der(der: &[u8]) -> Result<Self, super::Error> {
        let (_, certificate) =
            X509Certificate::from_der(der).map_err(|_| {
                super::Error::Other("invalid certificate DER".into())
            })?;
        Self::new(certificate.issuer().as_raw(), certificate.raw_serial())
    }

    pub fn issuer(&self) -> &[u8] {
        &self.issuer
    }
    pub fn serial(&self) -> &[u8] {
        &self.serial
    }
}
