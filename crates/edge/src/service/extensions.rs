use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

use claviron_http::config::ProxyConfig;
use claviron_http::ext::{ConnCtx, HostKey, HostName, RealIpExt};
use claviron_http::resolvers::resolve_real_ip_from_headers;
use claviron_types::Host;
use http::Request;
use tracing::debug;

use super::RequestAuthority;
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
    match table.resolve(&host) {
        Some((key, cfg)) => {
            debug!("resolved SNI '{}' => canonical key '{:?}'", sni, key);
            Ok(VhostCtx {
                hostname: HostName(Arc::new(host)),
                vhost_key: HostKey(Arc::new(key.clone())),
                vhost_config: cfg,
                application: table
                    .prepared::<crate::pipeline::HostAppSnapshot>()
                    .and_then(|apps| apps.resolve(&key)),
            })
        }
        None => Err(HostRejection::NotHosted(sni.to_string())),
    }
}

/// Inject vhost extensions into request
/// This replaces what the resolver middleware was doing
pub fn inject_vhost_ctx<B>(
    req: &mut Request<B>,
    extensions: &VhostCtx,
) {
    req.extensions_mut().insert(extensions.hostname.clone());
    req.extensions_mut().insert(extensions.vhost_key.clone());
    req.extensions_mut().insert(extensions.vhost_config.clone());
}

/// Inject connection context, vhost extensions, and resolve real IP per request
///
/// Real IP is resolved per request because upstream proxies (nginx, Cloudflare, etc.)
/// can multiplex many clients over a single TCP connection, each with different
/// forwarded headers.
pub(crate) fn inject_context_and_real_ip<B>(
    req: &mut Request<B>,
    ctx: &Arc<ConnCtx>,
    vctx: &VhostCtx,
    proxy_cache: Option<&ProxyConfig>,
) -> Arc<ConnCtx> {
    let (conn_ctx, real_ip) = match ctx.real_ip {
        // Direct connection: the identity was fixed when it was accepted, so
        // every request shares the connection's context as it is.
        Some(ip) => (Arc::clone(ctx), ip),
        // Behind a proxy one connection carries many clients: resolve this
        // request's real IP from its headers (not cached) into its own copy.
        None => {
            let real_ip = match proxy_cache {
                Some(proxy) => resolve_real_ip_from_headers(
                    req.headers(),
                    ctx.remote.ip(),
                    &proxy.trusted,
                    &proxy.headers,
                ),
                None => ctx.remote.ip(),
            };
            let mut conn_ctx = ctx.as_ref().clone();
            conn_ctx.real_ip = Some(real_ip);
            (Arc::new(conn_ctx), real_ip)
        }
    };

    // Inject connection context and vhost metadata.
    req.extensions_mut().insert(conn_ctx.clone());
    req.extensions_mut().insert(ctx.listener.clone());
    req.extensions_mut().insert(ctx.scheme());
    if let Some(protocol) = ctx.proto {
        req.extensions_mut().insert(protocol);
    }
    inject_vhost_ctx(req, vctx);
    req.extensions_mut().insert(RealIpExt(real_ip));
    conn_ctx
}

/// Compare a validated host identity with the accepted SNI binding. Ports do
/// not alter this host-only policy. Invalid messages must be rejected before
/// this check; `None` is only the legal HTTP/1.0 missing-authority case.
pub fn authority_matches_sni(
    authority: Option<&RequestAuthority>,
    vctx: &VhostCtx,
) -> bool {
    authority
        .is_none_or(|authority| authority.host() == vctx.hostname.0.as_ref())
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
