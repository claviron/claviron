//! Whether a server asks the client for a certificate, and which one it
//! accepts. A virtual host borrows this for its own `tls:` block; nothing here
//! knows about hosts.

use std::path::PathBuf;

use claviron_types::ssl::certstore::AuthorityEntry;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ClientAuthMode {
    #[default]
    Off,
    Required,
}

#[derive(Debug, Clone, Deserialize, Default, PartialEq, Eq)]
#[serde(try_from = "ClientAuthDeclaration")]
pub struct ClientAuthPolicy {
    pub mode: ClientAuthMode,
    pub authority: Option<AuthorityEntry>,
    pub ca_bundle: Option<PathBuf>,
    /// Optional client certificate subject Organization (O) policy.
    /// When set together with `unit`, the TLS ServerConfig rejects
    /// client certificates that do not match before request handling.
    pub organization: Option<String>,
    /// Optional client certificate subject Organizational Unit (OU)
    /// policy.
    pub unit: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ClientAuthDeclaration {
    #[serde(default)]
    mode: ClientAuthMode,
    authority: Option<AuthorityEntry>,
    ca_bundle: Option<PathBuf>,
    organization: Option<String>,
    #[serde(alias = "ou", alias = "org_unit", alias = "organizational_unit")]
    unit: Option<String>,
}

impl TryFrom<ClientAuthDeclaration> for ClientAuthPolicy {
    type Error = &'static str;

    fn try_from(raw: ClientAuthDeclaration) -> Result<Self, Self::Error> {
        if raw.authority.is_some() && raw.ca_bundle.is_some() {
            return Err(
                "client_auth must select either authority or ca_bundle",
            );
        }
        if raw.authority.as_ref().is_some_and(|source| {
            !matches!(source, AuthorityEntry::Local(_) | AuthorityEntry::Cn(_))
        }) {
            return Err("client_auth.authority must select local or cn");
        }
        Ok(Self {
            mode: raw.mode,
            authority: raw.authority,
            ca_bundle: raw.ca_bundle,
            organization: raw.organization,
            unit: raw.unit,
        })
    }
}

impl ClientAuthPolicy {
    pub fn is_required(&self) -> bool {
        self.mode == ClientAuthMode::Required
    }
}

#[cfg(test)]
#[path = "../../tests/auth_policy.rs"]
mod tests;
