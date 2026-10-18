use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::CertificateKind;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Server {
    pub id: i32,
    pub name: String,
    pub created_at: OffsetDateTime,
    pub enabled: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Vhost {
    pub id: i32,
    pub name: String,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
    pub enabled: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VhostAlias {
    pub id: i32,
    pub vhost_id: i32,
    pub hostname: String,
    pub created_at: OffsetDateTime,
}

/// Storage-neutral import request used by config loaders and tools.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VhostImport {
    pub name: String,
    pub aliases: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostedVhost {
    pub vhost: Vhost,
    pub aliases: Vec<VhostAlias>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Certificate {
    pub id: i32,
    pub kind: CertificateKind,
    pub issuer_id: Option<i32>,
    pub cn: String,
    pub certs_der: Vec<u8>,
    pub key_der: Vec<u8>,
    pub expires_at: OffsetDateTime,
    pub issued_at: OffsetDateTime,
    pub updated_at: Option<OffsetDateTime>,
    pub enabled: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CertificateAlias {
    pub id: i32,
    pub certificate_id: i32,
    pub hostname: String,
    pub created_at: OffsetDateTime,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostedCertificate {
    pub certificate: Certificate,
    pub aliases: Vec<CertificateAlias>,
}

/// Certificate material produced by an issuer before it is persisted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewCertificate {
    pub kind: CertificateKind,
    pub issuer_id: Option<i32>,
    pub cn: String,
    pub certs_der: Vec<u8>,
    pub key_der: Vec<u8>,
    pub expires_at: OffsetDateTime,
    pub issued_at: OffsetDateTime,
    pub enabled: bool,
}

/// How an issuer obtains signed certificates, independent of certificate usage.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IssuerKind {
    Acme,
    LocalCa,
}

impl TryFrom<String> for IssuerKind {
    type Error = crate::HostingError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.as_str() {
            "acme" => Ok(Self::Acme),
            "local_ca" => Ok(Self::LocalCa),
            _ => Err(crate::HostingError::InvalidInput(format!(
                "unknown issuer kind: {value}"
            ))),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CertificateIssuer {
    pub id: i32,
    pub name: String,
    pub kind: IssuerKind,
    pub acme_account_id: Option<i32>,
    pub ca_certificate_id: Option<i32>,
}
