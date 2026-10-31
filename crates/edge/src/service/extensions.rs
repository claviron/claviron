use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

use claviron_http::ext::{HostKey, HostName};
use claviron_types::Host;
use tracing::debug;

use crate::config::HostConfig;
use crate::loader::table::VhostTable;

/// Bundle of resolved vhost extensions to be injected into each request
/// This avoids parameter bloat when passing to h1/h2/h3 serve functions
#[derive(Clone)]
pub struct VhostCtx {
    /// Requested hostname: SNI for TLS, validated authority for cleartext.
    pub hostname: HostName,
    /// The canonical vhost key (resolved)
    pub vhost_key: HostKey,
    /// The vhost configuration
    pub vhost_config: Arc<HostConfig>,
    /// Routing prepared with this host publication, pinned for the connection.
    pub application: Option<crate::pipeline::HostApp>,
}

/// Resolve against the same host snapshot used to select TLS authentication.
pub fn resolve_sni_from_table(
    table: &VhostTable,
    server_name: Option<&str>,
) -> Result<VhostCtx, HostRejection> {
    let sni = server_name.ok_or(HostRejection::Missing)?;
    let host = Host::from_str(sni)
        .map_err(|error| HostRejection::Invalid(sni.to_owned(), error))?;
    match table.resolve_with_application(&host) {
        Some((record, application)) => {
            let key = &record.canonical;
            debug!("resolved SNI '{}' => canonical key '{:?}'", sni, key);
            Ok(VhostCtx {
                hostname: HostName(Arc::new(host)),
                vhost_key: HostKey(Arc::new(key.clone())),
                vhost_config: record.config.clone(),
                application: application.cloned(),
            })
        }
        None => Err(HostRejection::NotHosted(sni.to_string())),
    }
}

/// Result of SNI/vhost resolution at connection time
#[derive(Clone, Debug)]
pub enum HostRejection {
    /// No SNI provided (on HTTPS this should drop connection)
    Missing,

    /// SNI provided but invalid format
    Invalid(String, String),

    /// SNI provided but no matching vhost found
    NotHosted(String),
}

impl fmt::Display for HostRejection {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        match self {
            HostRejection::Missing => {
                write!(f, "No SNI provided")
            }
            HostRejection::Invalid(sni, err) => {
                write!(f, "Invalid SNI format '{}' err: {}", sni, err)
            }
            HostRejection::NotHosted(sni) => {
                write!(f, "Unknown SNI '{}'", sni)
            }
        }
    }
}

#[cfg(test)]
#[path = "../../tests/service/extensions.rs"]
mod tests;
