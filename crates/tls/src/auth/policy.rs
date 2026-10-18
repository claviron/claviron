//! Whether a server asks the client for a certificate, and which one it
//! accepts. A virtual host borrows this for its own `tls:` block; nothing here
//! knows about hosts.

use std::path::PathBuf;

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ClientAuthMode {
    #[default]
    Off,
    Required,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct ClientAuthPolicy {
    #[serde(default)]
    pub mode: ClientAuthMode,
    /// Optional host-level client-auth CA bundle override. When unset, a
    /// future global TLS client-auth CA may be used. Today `required`
    /// hosts must provide this explicitly.
    #[serde(default)]
    pub ca_bundle: Option<PathBuf>,
    /// Optional client certificate subject Organization (O) policy.
    /// When set together with `unit`, the TLS ServerConfig rejects
    /// client certificates that do not match before request handling.
    #[serde(default)]
    pub organization: Option<String>,
    /// Optional client certificate subject Organizational Unit (OU)
    /// policy. Accepted aliases keep existing config vocabulary loose.
    #[serde(
        default,
        alias = "ou",
        alias = "org_unit",
        alias = "organizational_unit"
    )]
    pub unit: Option<String>,
}

impl ClientAuthPolicy {
    pub fn is_required(&self) -> bool {
        self.mode == ClientAuthMode::Required
    }
}
