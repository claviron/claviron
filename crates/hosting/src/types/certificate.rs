use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::CertificateKind;

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
    /// Irreversible revocation, independent of the operational enabled flag.
    pub revoked_at: Option<OffsetDateTime>,
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

/// Public projection; the query never reads or decodes the private key.
/// Consumers choose trust, independently of the persisted usage kind.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicCertificate {
    pub id: i32,
    pub kind: CertificateKind,
    pub issuer_id: Option<i32>,
    pub cn: String,
    pub certs_der: Vec<u8>,
    pub expires_at: OffsetDateTime,
    pub issued_at: OffsetDateTime,
    pub updated_at: Option<OffsetDateTime>,
    pub revoked_at: Option<OffsetDateTime>,
    pub enabled: bool,
}
