//! Which client certificate a server requires. A host borrows this for its own
//! `tls:` block and a single-owner listener for its own; nothing here knows
//! about either.
//!
//! The declaration is present or absent: absent asks for no client
//! certificate, present requires one. Requiring a certificate needs something
//! to verify it against, so a declaration always carries exactly one source
//! of trust.

use std::path::{Path, PathBuf};

use claviron_types::ssl::certstore::AuthorityEntry;
use serde::Deserialize;

use crate::auth::verifier::PolicyConfig;

/// A required client certificate: what verifies it, and optionally the
/// subject it must carry.
///
/// ```yaml
/// client_auth:
///   authority: {local: example}   # or: ca_bundle: path/to/ca.pem
///   organization: Example         # optional, together with unit
///   unit: Operations
/// ```
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(try_from = "ClientAuthDeclaration")]
pub struct ClientAuth {
    trust: ClientTrust,
    subject: Option<PolicyConfig>,
}

/// What a client certificate chain must lead to.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ClientTrust {
    /// A CA of the certificate store, selected by `local` basename or `cn`.
    /// The holder resolves it with `CertificateStore::trust` before a
    /// verifier is built.
    Authority(AuthorityEntry),
    /// A PEM file of CA certificates, cwd-relative.
    Bundle(Box<Path>),
}

impl ClientAuth {
    /// Require a client certificate signed by a CA in this PEM bundle.
    pub fn ca_bundle(path: impl Into<PathBuf>) -> Self {
        Self {
            trust: ClientTrust::Bundle(path.into().into_boxed_path()),
            subject: None,
        }
    }

    pub fn trust(&self) -> &ClientTrust {
        &self.trust
    }

    /// The store authority this declaration selects, when it selects one.
    pub fn store_authority(&self) -> Option<&AuthorityEntry> {
        match &self.trust {
            ClientTrust::Authority(source) => Some(source),
            ClientTrust::Bundle(_) => None,
        }
    }

    /// The subject Organization and Organizational Unit the leaf must carry.
    pub fn subject(&self) -> Option<&PolicyConfig> {
        self.subject.as_ref()
    }
}

#[derive(Deserialize)]
#[serde(
    deny_unknown_fields,
    expecting = "a client_auth mapping that selects authority or ca_bundle"
)]
struct ClientAuthDeclaration {
    authority: Option<AuthorityEntry>,
    ca_bundle: Option<PathBuf>,
    organization: Option<Box<str>>,
    #[serde(alias = "ou", alias = "org_unit", alias = "organizational_unit")]
    unit: Option<Box<str>>,
}

impl TryFrom<ClientAuthDeclaration> for ClientAuth {
    type Error = &'static str;

    fn try_from(raw: ClientAuthDeclaration) -> Result<Self, Self::Error> {
        let trust = match (raw.authority, raw.ca_bundle) {
            (Some(_), Some(_)) => {
                return Err(
                    "client_auth selects both authority and ca_bundle; keep one",
                );
            }
            (None, None) => {
                return Err(
                    "client_auth needs a verifier: select authority or ca_bundle",
                );
            }
            (Some(source), None) => match source {
                AuthorityEntry::Local(_) | AuthorityEntry::Cn(_) => {
                    ClientTrust::Authority(source)
                }
                _ => {
                    return Err(
                        "client_auth.authority must select local or cn",
                    );
                }
            },
            (None, Some(path)) if path.as_os_str().is_empty() => {
                return Err("client_auth.ca_bundle is empty");
            }
            (None, Some(path)) => ClientTrust::Bundle(path.into_boxed_path()),
        };
        let subject = match (raw.organization, raw.unit) {
            (Some(organization), Some(unit)) => {
                Some(PolicyConfig::new(organization, unit))
            }
            (None, None) => None,
            _ => {
                return Err(
                    "client_auth organization and unit must be configured together",
                );
            }
        };
        Ok(Self { trust, subject })
    }
}

#[cfg(test)]
#[path = "../../tests/auth_policy.rs"]
mod tests;
