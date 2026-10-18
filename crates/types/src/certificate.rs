use serde::{Deserialize, Serialize};

/// Intended TLS role. A server identity may also authenticate outbound mTLS;
/// a client identity must never be installed as an inbound server certificate.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CertificateKind {
    Server,
    Client,
    Ca,
}

impl CertificateKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Server => "server",
            Self::Client => "client",
            Self::Ca => "ca",
        }
    }
    pub fn is_server(self) -> bool {
        matches!(self, Self::Server)
    }
}

#[derive(Debug)]
pub struct InvalidCertificateKind(String);
impl std::fmt::Display for InvalidCertificateKind {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        write!(f, "unknown certificate kind: {}", self.0)
    }
}
impl std::error::Error for InvalidCertificateKind {}
impl TryFrom<String> for CertificateKind {
    type Error = InvalidCertificateKind;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.as_str() {
            "server" => Ok(Self::Server),
            "client" => Ok(Self::Client),
            "ca" => Ok(Self::Ca),
            _ => Err(InvalidCertificateKind(value)),
        }
    }
}
