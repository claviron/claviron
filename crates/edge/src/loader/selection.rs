//! Selecting a published host for a server name or a request's authority.

use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

use claviron_http::authority::RequestAuthority;
use claviron_http::ext::{HostKey, HostName};
use claviron_types::Host;
use tracing::debug;

use crate::config::HostConfig;
use crate::loader::table::VhostTable;

/// A host selected for a connection or a request.
#[derive(Clone)]
pub struct VhostCtx {
    /// Requested hostname: SNI for TLS, validated authority for cleartext.
    pub hostname: HostName,
    /// The canonical vhost key (resolved)
    pub vhost_key: HostKey,
    /// The vhost configuration
    pub vhost_config: Arc<HostConfig>,
    /// Routing prepared with this host publication, pinned for the connection.
    pub application: Option<claviron_pipeline::plan::HostExecution>,
}

/// Resolve against the same host snapshot used to select TLS authentication.
pub fn resolve_sni_from_table(
    table: &VhostTable,
    server_name: Option<&str>,
) -> Result<VhostCtx, HostRejection> {
    resolve_sni(table, server_name, None)
}

/// Select the ready plan for an accepted listener from the same publication.
pub fn resolve_sni_for_listener(
    table: &VhostTable,
    server_name: Option<&str>,
    listener: &claviron_http::ListenerId,
) -> Result<VhostCtx, HostRejection> {
    resolve_sni(table, server_name, Some(listener))
}

fn resolve_sni(
    table: &VhostTable,
    server_name: Option<&str>,
    listener: Option<&claviron_http::ListenerId>,
) -> Result<VhostCtx, HostRejection> {
    let sni = server_name.ok_or(HostRejection::Missing)?;
    let host = Host::from_str(sni)
        .map_err(|error| HostRejection::Invalid(sni.to_owned(), error))?;
    match table.resolve_application_on(&host, listener) {
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

/// Resolve an already validated wire authority against the configured table.
/// Preserve the requested host separately from its canonical binding.
pub fn resolve_request_authority(
    authority: &RequestAuthority,
    table: &VhostTable,
) -> Option<VhostCtx> {
    let (record, application) =
        table.resolve_application_on(authority.host(), None)?;
    Some(VhostCtx {
        hostname: HostName(Arc::new(authority.host().clone())),
        vhost_key: HostKey(Arc::new(record.canonical.clone())),
        vhost_config: record.config.clone(),
        application: application.cloned(),
    })
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
