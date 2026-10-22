use serde::{Deserialize, Serialize};

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
